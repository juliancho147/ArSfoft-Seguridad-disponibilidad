"""
reservar_producto + cola_de_reserva_producto (bitacora-reto2.md 8.2, plan seccion 2)

Estos dos componentes logicos del diagrama corren en el MISMO proceso/contenedor:
la cola elegida es in-memory (asyncio.Queue), que no se puede compartir entre
contenedores distintos sin un broker externo (Kafka, Redis, SQS...). El endpoint
POST /reservas cumple el rol de cola_de_reserva_producto (recibe y encola); las
corutinas `worker` (N_WORKERS en paralelo) cumplen el rol de reservar_producto.

Flujo por item (igual al diagrama de secuencia de disponibilidad):
  1. get_stock: lee stock real y cantidad ya reservada del producto.
  2. calcular_reserva: si lo disponible no alcanza, rechaza por falta de stock.
  3. save_reserva: persiste la reserva y acumula la cantidad reservada.
  4. verificacion_sanidad: compara lo reservado contra el stock real.
  5. Si es inconsistente -> rollback (revierte la reserva) y responde rechazo.
     Si es consistente -> generar_pedido confirma el pedido.

Los pasos 1-3 son un check-then-act SIN control de concurrencia (intencional:
bitacora 3.1, "no se disenio un componente dedicado a controlar condiciones de
carrera"). Con varios workers, dos solicitudes simultaneas sobre el mismo
producto leen el mismo disponible y ambas reservan: esa es la falla que
verificacion_sanidad detecta y que el rollback recupera.
"""
import asyncio
import os
import time
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
N_WORKERS = int(os.environ.get("N_WORKERS", "8"))
ENABLE_TEST_ENDPOINTS = os.environ.get("ENABLE_TEST_ENDPOINTS", "false").lower() == "true"

# Fronteras explicitas en los umbrales de los ASR (150ms, 200ms, 350ms).
BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.35, 0.5, 1.0, 2.5, 5.0)

rollback_seconds = Histogram(
    "rollback_seconds",
    "Tiempo de rollback de una reserva marcada como erronea",
    buckets=BUCKETS,
)
reserva_total_seconds = Histogram(
    "reserva_total_seconds",
    "Tiempo total end-to-end de una reserva, incluida la espera en cola",
    buckets=BUCKETS,
)
pedidos_confirmados_total = Counter("pedidos_confirmados_total", "Reservas confirmadas")
pedidos_rechazados_total = Counter(
    "pedidos_rechazados_total", "Reservas rechazadas (sin stock + erroneas revertidas)"
)
reservas_sin_stock_total = Counter(
    "reservas_sin_stock_total", "Rechazos normales: lo disponible no alcanzaba al calcular la reserva"
)
reservas_revertidas_total = Counter(
    "reservas_revertidas_total", "Reservas erroneas revertidas por el rollback"
)


@dataclass
class ItemCola:
    producto_id: str
    cantidad: int
    tendero_id: str
    future: "asyncio.Future"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=20)
    app.state.http = httpx.AsyncClient(timeout=5.0)
    app.state.cola: asyncio.Queue[ItemCola] = asyncio.Queue()
    app.state.worker_tasks = [asyncio.create_task(worker(app)) for _ in range(N_WORKERS)]
    yield
    for task in app.state.worker_tasks:
        task.cancel()
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


async def revertir_reserva(app: FastAPI, reserva_id: str, producto_id: str, cantidad: int) -> None:
    """Devuelve el producto y la reserva al estado anterior, en una sola transaccion."""
    async with app.state.pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "UPDATE producto SET reservado = reservado - $1 WHERE producto_id = $2",
                cantidad,
                producto_id,
            )
            await conn.execute(
                "UPDATE reserva SET estado = 'revertida', revertida_en = clock_timestamp() WHERE id = $1",
                reserva_id,
            )


async def procesar(app: FastAPI, item: ItemCola) -> ReservaResponse:
    reserva_id = f"res-{uuid.uuid4().hex[:12]}"

    async with app.state.pool.acquire() as conn:
        # get_stock
        producto = await conn.fetchrow(
            "SELECT stock, reservado FROM producto WHERE producto_id = $1", item.producto_id
        )
        if producto is None:
            raise HTTPException(status_code=404, detail="producto no encontrado")

        # calcular_reserva
        disponible = producto["stock"] - producto["reservado"]
        if item.cantidad > disponible:
            reservas_sin_stock_total.inc()
            pedidos_rechazados_total.inc()
            return ReservaResponse(
                pedido_id=None,
                estado="rechazado",
                mensaje="Stock insuficiente: no hay unidades disponibles para la cantidad solicitada",
            )

        # save_reserva
        async with conn.transaction():
            reservado_acumulado = await conn.fetchval(
                "UPDATE producto SET reservado = reservado + $1 WHERE producto_id = $2 RETURNING reservado",
                item.cantidad,
                item.producto_id,
            )
            await conn.execute(
                """
                INSERT INTO reserva (id, producto_id, tendero_id, cantidad, reservado_acumulado, estado)
                VALUES ($1, $2, $3, $4, $5, 'pendiente')
                """,
                reserva_id,
                item.producto_id,
                item.tendero_id,
                item.cantidad,
                reservado_acumulado,
            )

    try:
        verificar_resp = await app.state.http.post(
            f"{VERIFICACION_SANIDAD_URL}/verificar", json={"reserva_id": reserva_id}
        )
        verificar_resp.raise_for_status()
        consistente = verificar_resp.json()["consistente"]
    except httpx.HTTPError:
        # Sin veredicto de sanidad no se confirma: se libera lo reservado.
        await revertir_reserva(app, reserva_id, item.producto_id, item.cantidad)
        raise

    if not consistente:
        with rollback_seconds.time():
            await revertir_reserva(app, reserva_id, item.producto_id, item.cantidad)
        reservas_revertidas_total.inc()
        pedidos_rechazados_total.inc()
        return ReservaResponse(
            pedido_id=None,
            estado="rechazado",
            mensaje="No se puede completar el pedido con la cantidad solicitada: la reserva fue revertida",
        )

    pedido_id = f"ped-{uuid.uuid4().hex[:12]}"
    try:
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
    except httpx.HTTPError:
        # El pedido no se creo: no puede quedar stock comprometido sin pedido.
        await revertir_reserva(app, reserva_id, item.producto_id, item.cantidad)
        raise

    async with app.state.pool.acquire() as conn:
        await conn.execute(
            "UPDATE reserva SET estado = 'confirmada', pedido_id = $1 WHERE id = $2",
            pedido_id,
            reserva_id,
        )
    pedidos_confirmados_total.inc()
    return ReservaResponse(pedido_id=pedido_id, estado="procesado", mensaje="Reserva confirmada")


@app.post("/reservas", response_model=ReservaResponse)
async def crear_reserva(req: ReservaRequest) -> ReservaResponse:
    inicio = time.perf_counter()
    future: asyncio.Future = asyncio.get_running_loop().create_future()
    await app.state.cola.put(
        ItemCola(producto_id=req.producto_id, cantidad=req.cantidad, tendero_id=req.tendero_id, future=future)
    )
    try:
        return await future
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Error llamando a un servicio dependiente: {exc}")
    finally:
        reserva_total_seconds.observe(time.perf_counter() - inicio)


class ReponerRequest(BaseModel):
    prefijo: str = "prod-bajo-"
    minimo: int = 5
    maximo: int = 10


if ENABLE_TEST_ENDPOINTS:

    @app.post("/_test/reponer")
    async def reponer(req: ReponerRequest):
        """
        Soporte de prueba (no es parte del contrato real): simula la reposicion de
        bodega de los productos de bajo stock, dejando entre `minimo` y `maximo`
        unidades disponibles. Sin esto los 4 productos se agotan en los primeros
        segundos y no vuelve a haber contencion durante el resto de la corrida.
        El stock solo sube, asi que el invariante reservado <= stock no se rompe.
        """
        async with app.state.pool.acquire() as conn:
            resultado = await conn.execute(
                """
                UPDATE producto
                SET stock = GREATEST(stock, reservado + $2 + floor(random() * ($3 - $2 + 1))::int)
                WHERE producto_id LIKE $1 || '%'
                """,
                req.prefijo,
                req.minimo,
                req.maximo,
            )
        return {"resultado": resultado}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
