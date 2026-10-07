"""
componente_restauracion (bitacora-reto2.md 8.9)

Revierte puntualmente un pedido a su valor original (precio/cantidad) cuando
verificador_integridad_hash detecta tampering. Recibe el pedido_id y el
valor_original (obtenidos del audit trail por quien lo invoca).
"""
import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

DATABASE_URL = os.environ["DATABASE_URL"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    yield
    await app.state.pool.close()


app = FastAPI(title="componente_restauracion", lifespan=lifespan)


class RestaurarRequest(BaseModel):
    pedido_id: str
    precio: float
    cantidad: int


class RestaurarResponse(BaseModel):
    pedido_id: str
    estado: str


@app.post("/restaurar", response_model=RestaurarResponse)
async def restaurar(req: RestaurarRequest) -> RestaurarResponse:
    async with app.state.pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE pedido SET precio = $1, cantidad = $2 WHERE id = $3",
            req.precio,
            req.cantidad,
            req.pedido_id,
        )
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="pedido no encontrado")
    return RestaurarResponse(pedido_id=req.pedido_id, estado="restaurado")


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
