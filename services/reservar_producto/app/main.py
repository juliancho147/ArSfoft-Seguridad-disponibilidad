"""
reservar_producto + cola_de_reserva_producto (bitacora-reto2.md 8.2, plan seccion 2)

Estos dos componentes logicos del diagrama corren en el MISMO proceso/contenedor:
la cola elegida es in-memory (asyncio.Queue), que no se puede compartir entre
contenedores distintos sin un broker externo (Redis, SQS...). El endpoint POST
/reservas cumple el rol de cola_de_reserva_producto (recibe y encola); la
corutina `worker` cumple el rol de reservar_producto (consume, verifica, decide).

Flujo por item:
  1. Llama a verificacion_sanidad (decrementa stock atomicamente y compara).
  2. Si es inconsistente -> rollback (revierte el decremento) y responde rechazo.
  3. Si es consistente -> llama a generar_pedido para confirmar.
"""
import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

DATABASE_URL = os.environ["DATABASE_URL"]
VERIFICACION_SANIDAD_URL = os.environ["VERIFICACION_SANIDAD_URL"]
GENERAR_PEDIDO_URL = os.environ["GENERAR_PEDIDO_URL"]

rollback_seconds = Histogram(
    "rollback_seconds", "Tiempo de rollback de una reserva marcada como inconsistente"
)
reserva_total_seconds = Histogram(
    "reserva_total_seconds", "Tiempo total end-to-end de una reserva (sanidad + generar_pedido|rollback)"
)
pedidos_rechazados_total = Counter(
    "pedidos_rechazados_total", "Reservas rechazadas por inconsistencia de stock"
)
pedidos_confirmados_total = Counter("pedidos_confirmados_total", "Reservas confirmadas")


@dataclass
class ItemCola:
    producto_id: str
    cantidad: int
    tendero_id: str
    future: "asyncio.Future"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    app.state.http = httpx.AsyncClient(timeout=5.0)
    app.state.cola: asyncio.Queue[ItemCola] = asyncio.Queue()
    app.state.worker_task = asyncio.create_task(worker(app))
    yield
    app.state.worker_task.cancel()
    await app.state.http.aclose()
    await app.state.pool.close()


app = FastAPI(title="reservar_producto", lifespan=lifespan)


class ReservaRequest(BaseModel):
    producto_id: str
    cantidad: int
    tendero_id: str


class ReservaResponse(BaseModel):
    pedido_id: str | None
    estado: str
    mensaje: str


async def worker(app: FastAPI):
    cola: "asyncio.Queue[ItemCola]" = app.state.cola
    while True:
        item = await cola.get()
        try:
            resultado = await procesar(app, item)
            if not item.future.done():
                item.future.set_result(resultado)
        except Exception as exc:  # noqa: BLE001
            if not item.future.done():
                item.future.set_exception(exc)
        finally:
            cola.task_done()


async def procesar(app: FastAPI, item: ItemCola) -> ReservaResponse:
    with reserva_total_seconds.time():
        verificar_resp = await app.state.http.post(
            f"{VERIFICACION_SANIDAD_URL}/verificar",
            json={"producto_id": item.producto_id, "cantidad": item.cantidad},
        )
        verificar_resp.raise_for_status()
        verificacion = verificar_resp.json()

        if not verificacion["consistente"]:
            with rollback_seconds.time():
                async with app.state.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE producto SET stock = stock + $1 WHERE producto_id = $2",
                        item.cantidad,
                        item.producto_id,
                    )
            pedidos_rechazados_total.inc()
            return ReservaResponse(
                pedido_id=None,
                estado="rechazado",
                mensaje="Stock insuficiente: la reserva no se pudo completar con la cantidad solicitada",
            )

        pedido_id = f"ped-{uuid.uuid4().hex[:12]}"
        generar_resp = await app.state.http.post(
            f"{GENERAR_PEDIDO_URL}/generar",
            json={
                "pedido_id": pedido_id,
                "producto_id": item.producto_id,
                "cantidad": item.cantidad,
                "tendero_id": item.tendero_id,
            },
        )
        generar_resp.raise_for_status()
        pedidos_confirmados_total.inc()
        return ReservaResponse(pedido_id=pedido_id, estado="procesado", mensaje="Reserva confirmada")


@app.post("/reservas", response_model=ReservaResponse)
async def crear_reserva(req: ReservaRequest) -> ReservaResponse:
    future: asyncio.Future = asyncio.get_event_loop().create_future()
    await app.state.cola.put(
        ItemCola(producto_id=req.producto_id, cantidad=req.cantidad, tendero_id=req.tendero_id, future=future)
    )
    try:
        return await future
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Error llamando a un servicio dependiente: {exc}")


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
