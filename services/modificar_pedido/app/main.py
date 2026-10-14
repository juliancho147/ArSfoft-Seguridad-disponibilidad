"""
modificar_pedido (bitacora-reto2.md 8.7)

Punto de entrada del flujo de Tampering. Registra el intento en
registro_auditoria, aplica el UPDATE (no bloquea la escritura -- Resistir esta
fuera de alcance) y delega toda la verificacion a verificador_integridad_hash.
`patron_simulado` es un campo de prueba que solo existe para alimentar el mock
de detector_de_patrones_de_acceso -- no es parte del contrato real.

Lo unico que si se rechaza de entrada es una cuenta ya bloqueada por una
reaccion anterior: sin eso el bloqueo de gestor_bloqueo_cuentas no tendria efecto.
"""
import json
import os
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

DATABASE_URL = os.environ["DATABASE_URL"]
VERIFICADOR_INTEGRIDAD_HASH_URL = os.environ["VERIFICADOR_INTEGRIDAD_HASH_URL"]

# Fronteras explicitas en los umbrales de los ASR (150ms, 200ms, 350ms).
BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.35, 0.5, 1.0, 2.5, 5.0)

tampering_total_seconds = Histogram(
    "tampering_total_seconds",
    "Tiempo total end-to-end de modificar_pedido (auditoria + UPDATE + verificacion/reaccion)",
    buckets=BUCKETS,
)
intentos_cuenta_bloqueada_total = Counter(
    "intentos_cuenta_bloqueada_total", "Modificaciones rechazadas porque la cuenta del actor esta bloqueada"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=20)
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
            bloqueada = await conn.fetchval(
                "SELECT bloqueada FROM cuenta WHERE actor_id = $1", req.actor_id
            )
            if bloqueada:
                intentos_cuenta_bloqueada_total.inc()
                raise HTTPException(status_code=403, detail="cuenta bloqueada")

            # Serializa modificacion + verificacion + reaccion por pedido: sin esto,
            # dos modificaciones simultaneas al mismo pedido se cruzan y la
            # verificacion de una termina juzgando (o legitimando) el cambio de la otra.
            await conn.execute("SELECT pg_advisory_lock(hashtext($1))", req.pedido_id)
            try:
                async with conn.transaction():
                    pedido_actual = await conn.fetchrow(
                        "SELECT precio, cantidad FROM pedido WHERE id = $1 FOR UPDATE", req.pedido_id
                    )
                    if pedido_actual is None:
                        raise HTTPException(status_code=404, detail="pedido no encontrado")

                    valor_anterior = {
                        "precio": float(pedido_actual["precio"]),
                        "cantidad": pedido_actual["cantidad"],
                    }
                    valor_nuevo = {"precio": req.precio_nuevo, "cantidad": req.cantidad_nuevo}

                    auditoria_id = await conn.fetchval(
                        """
                        INSERT INTO registro_auditoria (pedido_id, actor_id, valor_anterior, valor_nuevo, tipo)
                        VALUES ($1, $2, $3::jsonb, $4::jsonb, 'intento_modificacion')
                        RETURNING id
                        """,
                        req.pedido_id,
                        req.actor_id,
                        json.dumps(valor_anterior),
                        json.dumps(valor_nuevo),
                    )
                    await conn.execute(
                        "UPDATE pedido SET precio = $1, cantidad = $2 WHERE id = $3",
                        req.precio_nuevo,
                        req.cantidad_nuevo,
                        req.pedido_id,
                    )

                verificar_resp = await app.state.http.post(
                    f"{VERIFICADOR_INTEGRIDAD_HASH_URL}/verificar",
                    json={
                        "pedido_id": req.pedido_id,
                        "auditoria_id": auditoria_id,
                        "patron_simulado": req.patron_simulado,
                    },
                )
                verificar_resp.raise_for_status()
                resultado = verificar_resp.json()
            finally:
                await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", req.pedido_id)

    return ModificarResponse(
        pedido_id=req.pedido_id, estado=resultado["estado"], rama=resultado["rama"]
    )


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
