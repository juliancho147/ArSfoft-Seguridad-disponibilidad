"""
verificacion_sanidad (bitacora-reto2.md 8.3, ASR 3.1 - deteccion)

Compara la cantidad reservada calculada contra el stock real, antes de confirmar
el pedido. Mecanica de deteccion de la condicion de carrera (no documentada
explicitamente en el plan original, definida al implementar):

  UPDATE producto SET stock = stock - :cantidad WHERE producto_id = :pid RETURNING stock

Es un decremento atomico SIN guarda (no "WHERE stock >= cantidad") -- eso es lo que
permite que la condicion de carrera ocurra de forma natural bajo concurrencia real
(bitacora 3.1: "no se disenio un componente dedicado a controlar condiciones de
carrera"). Postgres serializa las escrituras concurrentes sobre la misma fila, asi
que el stock resultante siempre refleja la suma exacta de todos los decrementos
aplicados -- si esa suma supera el stock original, el resultado queda negativo y
esa es la senial de inconsistencia que este componente reporta. No hay perdida de
escrituras: lo que hay es sobreventa real, detectada aqui.
"""
import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, Histogram, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

DATABASE_URL = os.environ["DATABASE_URL"]

sanidad_deteccion_seconds = Histogram(
    "sanidad_deteccion_seconds",
    "Tiempo de verificacion de sanidad (decremento atomico + comparacion contra stock)",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    yield
    await app.state.pool.close()


app = FastAPI(title="verificacion_sanidad", lifespan=lifespan)


class VerificarRequest(BaseModel):
    producto_id: str
    cantidad: int


class VerificarResponse(BaseModel):
    consistente: bool
    stock_resultante: int


@app.post("/verificar", response_model=VerificarResponse)
async def verificar(req: VerificarRequest) -> VerificarResponse:
    with sanidad_deteccion_seconds.time():
        async with app.state.pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE producto SET stock = stock - $1 WHERE producto_id = $2 RETURNING stock",
                req.cantidad,
                req.producto_id,
            )

    if row is None:
        raise HTTPException(status_code=404, detail="producto no encontrado")

    stock_resultante = row["stock"]
    return VerificarResponse(consistente=stock_resultante >= 0, stock_resultante=stock_resultante)


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
