"""
generar_pedido (bitacora-reto2.md 8.4)

Se invoca desde reservar_producto tras una verificacion de sanidad exitosa.
Lee el precio vigente del producto, pide el hash a calculador_hash_integridad
y hace el INSERT de pedido+hash. Estado final: 'procesado' (misma convencion
que usan los pedidos semilla del Experimento 2).
"""
import os
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

DATABASE_URL = os.environ["DATABASE_URL"]
CALCULADOR_HASH_URL = os.environ["CALCULADOR_HASH_URL"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    app.state.http = httpx.AsyncClient(timeout=5.0)
    yield
    await app.state.http.aclose()
    await app.state.pool.close()


app = FastAPI(title="generar_pedido", lifespan=lifespan)


class GenerarRequest(BaseModel):
    pedido_id: str
    producto_id: str
    cantidad: int
    tendero_id: str


class GenerarResponse(BaseModel):
    pedido_id: str
    hash_integridad: str


@app.post("/generar", response_model=GenerarResponse)
async def generar(req: GenerarRequest) -> GenerarResponse:
    async with app.state.pool.acquire() as conn:
        producto = await conn.fetchrow(
            "SELECT precio FROM producto WHERE producto_id = $1", req.producto_id
        )
        if producto is None:
            raise HTTPException(status_code=404, detail="producto no encontrado")
        precio = float(producto["precio"])
        estado = "procesado"

        hash_resp = await app.state.http.post(
            f"{CALCULADOR_HASH_URL}/calcular",
            json={"precio": precio, "cantidad": req.cantidad, "estado": estado},
        )
        hash_resp.raise_for_status()
        hash_integridad = hash_resp.json()["hash"]

        await conn.execute(
            """
            INSERT INTO pedido (id, tendero_id, producto_id, cantidad, precio, estado, hash_integridad)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            req.pedido_id,
            req.tendero_id,
            req.producto_id,
            req.cantidad,
            precio,
            estado,
            hash_integridad,
        )

    return GenerarResponse(pedido_id=req.pedido_id, hash_integridad=hash_integridad)


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
