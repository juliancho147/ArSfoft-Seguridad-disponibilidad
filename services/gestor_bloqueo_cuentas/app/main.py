"""
gestor_bloqueo_cuentas (bitacora-reto2.md 8.1, 4.5.2)

Bloquea la cuenta del actor identificado por el audit trail como responsable
de una modificacion no autorizada sobre un pedido ya procesado.
"""
import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

DATABASE_URL = os.environ["DATABASE_URL"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    yield
    await app.state.pool.close()


app = FastAPI(title="gestor_bloqueo_cuentas", lifespan=lifespan)


class BloquearRequest(BaseModel):
    actor_id: str


class BloquearResponse(BaseModel):
    actor_id: str
    bloqueada: bool


@app.post("/bloquear", response_model=BloquearResponse)
async def bloquear(req: BloquearRequest) -> BloquearResponse:
    async with app.state.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO cuenta (actor_id, bloqueada) VALUES ($1, true)
            ON CONFLICT (actor_id) DO UPDATE SET bloqueada = true
            """,
            req.actor_id,
        )
    return BloquearResponse(actor_id=req.actor_id, bloqueada=True)


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
