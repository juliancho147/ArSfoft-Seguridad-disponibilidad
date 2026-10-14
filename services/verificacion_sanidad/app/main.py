"""
verificacion_sanidad (bitacora-reto2.md 8.3, ASR 3.1 - deteccion)

Compara la cantidad reservada que reservar_producto calculo y persistio contra
el stock real del producto, antes de que el pedido sea confirmado. Solo lee de
base_de_datos (reserva.reservado_acumulado y producto.stock) y retorna el
resultado de la comparacion; si la reserva es inconsistente la marca como
'erronea' (respuesta del ASR de deteccion).

La inconsistencia la produce reservar_producto, que calcula la reserva con un
check-then-act sin control de concurrencia: dos solicitudes simultaneas sobre el
mismo producto leen el mismo disponible y ambas reservan, dejando la cantidad
reservada acumulada por encima del stock real (condicion de carrera real).
"""
import os
import time
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

DATABASE_URL = os.environ["DATABASE_URL"]

# Fronteras explicitas en los umbrales de los ASR (150ms, 200ms, 350ms) para que
# histogram_quantile no tenga que interpolar justo donde se decide el experimento.
BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.35, 0.5, 1.0, 2.5, 5.0)

sanidad_deteccion_seconds = Histogram(
    "sanidad_deteccion_seconds",
    "Tiempo de verificacion de sanidad (lectura reservado vs stock + comparacion + marca de erronea)",
    ["resultado"],  # "consistente" | "erronea"
    buckets=BUCKETS,
)
reservas_erroneas_detectadas_total = Counter(
    "reservas_erroneas_detectadas_total",
    "Reservas cuya cantidad reservada acumulada supera el stock real",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=20)
    yield
    await app.state.pool.close()


app = FastAPI(title="verificacion_sanidad", lifespan=lifespan)


class VerificarRequest(BaseModel):
    reserva_id: str


class VerificarResponse(BaseModel):
    consistente: bool
    reservado_acumulado: int
    stock_real: int


@app.post("/verificar", response_model=VerificarResponse)
async def verificar(req: VerificarRequest) -> VerificarResponse:
    inicio = time.perf_counter()
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT r.reservado_acumulado, p.stock
            FROM reserva r JOIN producto p ON p.producto_id = r.producto_id
            WHERE r.id = $1
            """,
            req.reserva_id,
        )
        if row is None:
            raise HTTPException(status_code=404, detail="reserva no encontrada")

        consistente = row["reservado_acumulado"] <= row["stock"]
        if not consistente:
            await conn.execute(
                """
                UPDATE reserva SET estado = 'erronea', detectada_en = clock_timestamp()
                WHERE id = $1 AND estado = 'pendiente'
                """,
                req.reserva_id,
            )
            reservas_erroneas_detectadas_total.inc()

    sanidad_deteccion_seconds.labels("consistente" if consistente else "erronea").observe(
        time.perf_counter() - inicio
    )
    return VerificarResponse(
        consistente=consistente,
        reservado_acumulado=row["reservado_acumulado"],
        stock_real=row["stock"],
    )


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
