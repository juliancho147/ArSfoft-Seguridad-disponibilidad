"""
modificar_pedido (bitacora-reto2.md 8.7)

Punto de entrada del flujo de Tampering. Aplica el UPDATE primero (no bloquea
la escritura -- Resistir esta fuera de alcance), registra el intento en
registro_auditoria, y delega toda la verificacion a verificador_integridad_hash.
`patron_simulado` es un campo de prueba que solo existe para alimentar el mock
de detector_de_patrones_de_acceso -- no es parte del contrato real.
"""
import json
import os
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, Histogram, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

DATABASE_URL = os.environ["DATABASE_URL"]
VERIFICADOR_INTEGRIDAD_HASH_URL = os.environ["VERIFICADOR_INTEGRIDAD_HASH_URL"]

tampering_total_seconds = Histogram(
    "tampering_total_seconds",
    "Tiempo total end-to-end de modificar_pedido (UPDATE + auditoria + verificacion/reaccion)",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    app.state.http = httpx.AsyncClient(timeout=5.0)
    yield
    await app.state.http.aclose()
    await app.state.pool.close()


app = FastAPI(title="modificar_pedido", lifespan=lifespan)


class ModificarRequest(BaseModel):
    pedido_id: str
    precio_nuevo: float
    cantidad_nuevo: int
    actor_id: str
    patron_simulado: str  # "normal" | "anomalo" -- solo para el mock


class ModificarResponse(BaseModel):
    pedido_id: str
    estado: str
    rama: str


@app.post("/modificar", response_model=ModificarResponse)
async def modificar(req: ModificarRequest) -> ModificarResponse:
    with tampering_total_seconds.time():
        async with app.state.pool.acquire() as conn:
            pedido_actual = await conn.fetchrow(
                "SELECT precio, cantidad FROM pedido WHERE id = $1", req.pedido_id
            )
            if pedido_actual is None:
                raise HTTPException(status_code=404, detail="pedido no encontrado")

            valor_anterior = {"precio": float(pedido_actual["precio"]), "cantidad": pedido_actual["cantidad"]}
            valor_nuevo = {"precio": req.precio_nuevo, "cantidad": req.cantidad_nuevo}

            await conn.execute(
                "UPDATE pedido SET precio = $1, cantidad = $2 WHERE id = $3",
                req.precio_nuevo,
                req.cantidad_nuevo,
                req.pedido_id,
            )
            await conn.execute(
                """
                INSERT INTO registro_auditoria (pedido_id, actor_id, valor_anterior, valor_nuevo, tipo)
                VALUES ($1, $2, $3::jsonb, $4::jsonb, 'intento_modificacion')
                """,
                req.pedido_id,
                req.actor_id,
                json.dumps(valor_anterior),
                json.dumps(valor_nuevo),
            )

        verificar_resp = await app.state.http.post(
            f"{VERIFICADOR_INTEGRIDAD_HASH_URL}/verificar",
            json={"pedido_id": req.pedido_id, "patron_simulado": req.patron_simulado},
        )
        verificar_resp.raise_for_status()
        resultado = verificar_resp.json()

    return ModificarResponse(
        pedido_id=req.pedido_id, estado=resultado["estado"], rama=resultado["rama"]
    )


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
