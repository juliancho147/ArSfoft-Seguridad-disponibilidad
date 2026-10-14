"""
verificador_integridad_hash (bitacora-reto2.md 8.8, ASR 4.5.1 corregido 2026-09-30)

Orquesta las dos senales de deteccion (hash + patron de acceso) y decide entre
3 ramas:
  1. Hash coincide -> confirma (caso casi teorico dado el alcance del Estimulo).
  2. Hash no coincide Y patron normal -> correccion legitima: recalcula y
     actualiza el hash (nueva linea base), confirma sin alerta.
  3. Hash no coincide Y patron anomalo -> tampering: genera la alerta de
     seguridad y dispara la reaccion completa (restauracion + bloqueo de
     cuenta) usando el registro de auditoria de ESA solicitud (auditoria_id),
     no el ultimo del pedido.
"""
import json
import logging
import os
import time
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

DATABASE_URL = os.environ["DATABASE_URL"]
CALCULADOR_HASH_URL = os.environ["CALCULADOR_HASH_URL"]
DETECTOR_PATRONES_URL = os.environ["DETECTOR_PATRONES_URL"]
COMPONENTE_RESTAURACION_URL = os.environ["COMPONENTE_RESTAURACION_URL"]
GESTOR_BLOQUEO_URL = os.environ["GESTOR_BLOQUEO_URL"]

logger = logging.getLogger("uvicorn.error")

# Fronteras explicitas en los umbrales de los ASR (150ms, 200ms, 350ms).
BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.35, 0.5, 1.0, 2.5, 5.0)

tampering_deteccion_seconds = Histogram(
    "tampering_deteccion_seconds",
    "Tiempo de deteccion completo: lectura del pedido + hash + patron de acceso (+ alerta si aplica)",
    ["rama"],  # "hash_coincide" | "correccion_legitima" | "tampering"
    buckets=BUCKETS,
)
hash_deteccion_seconds = Histogram(
    "hash_deteccion_seconds", "Tiempo de recalculo y comparacion del hash de integridad", buckets=BUCKETS
)
patron_deteccion_seconds = Histogram(
    "patron_deteccion_seconds", "Tiempo de validacion del patron de acceso (mock)", buckets=BUCKETS
)
reaccion_seconds = Histogram(
    "reaccion_seconds", "Tiempo de reaccion desde la alerta (restauracion + bloqueo)", buckets=BUCKETS
)
hash_coincide_total = Counter("hash_coincide_total", "Verificaciones donde el hash coincidio (rama 1)")
correcciones_legitimas_total = Counter(
    "correcciones_legitimas_total", "Hash no coincidia pero el patron era normal (rama 2)"
)
alertas_generadas_total = Counter(
    "alertas_generadas_total", "Hash no coincidia y el patron fue anomalo: tampering (rama 3)"
)
reacciones_completadas_total = Counter(
    "reacciones_completadas_total", "Tampering con pedido restaurado y cuenta bloqueada"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=20)
    app.state.http = httpx.AsyncClient(timeout=5.0)
    yield
    await app.state.http.aclose()
    await app.state.pool.close()


app = FastAPI(title="verificador_integridad_hash", lifespan=lifespan)


class VerificarRequest(BaseModel):
    pedido_id: str
    auditoria_id: int  # registro 'intento_modificacion' de esta solicitud
    patron_simulado: str  # "normal" | "anomalo"


class VerificarResponse(BaseModel):
    pedido_id: str
    estado: str  # "confirmado" | "revertido"
    rama: str  # "hash_coincide" | "correccion_legitima" | "tampering"


@app.post("/verificar", response_model=VerificarResponse)
async def verificar(req: VerificarRequest) -> VerificarResponse:
    inicio_deteccion = time.perf_counter()
    async with app.state.pool.acquire() as conn:
        pedido = await conn.fetchrow(
            "SELECT precio, cantidad, estado, hash_integridad FROM pedido WHERE id = $1",
            req.pedido_id,
        )
    if pedido is None:
        raise HTTPException(status_code=404, detail="pedido no encontrado")

    with hash_deteccion_seconds.time():
        hash_resp = await app.state.http.post(
            f"{CALCULADOR_HASH_URL}/calcular",
            json={
                "precio": float(pedido["precio"]),
                "cantidad": pedido["cantidad"],
                "estado": pedido["estado"],
            },
        )
        hash_resp.raise_for_status()
        hash_calculado = hash_resp.json()["hash"]

    hash_coincide = hash_calculado == pedido["hash_integridad"]

    if hash_coincide:
        hash_coincide_total.inc()
        tampering_deteccion_seconds.labels("hash_coincide").observe(time.perf_counter() - inicio_deteccion)
        return VerificarResponse(pedido_id=req.pedido_id, estado="confirmado", rama="hash_coincide")

    with patron_deteccion_seconds.time():
        patron_resp = await app.state.http.post(
            f"{DETECTOR_PATRONES_URL}/validar",
            json={"patron_simulado": req.patron_simulado},
        )
        patron_resp.raise_for_status()
        patron = patron_resp.json()["patron"]

    if patron == "normal":
        async with app.state.pool.acquire() as conn:
            await conn.execute(
                "UPDATE pedido SET hash_integridad = $1 WHERE id = $2",
                hash_calculado,
                req.pedido_id,
            )
        correcciones_legitimas_total.inc()
        tampering_deteccion_seconds.labels("correccion_legitima").observe(
            time.perf_counter() - inicio_deteccion
        )
        return VerificarResponse(pedido_id=req.pedido_id, estado="confirmado", rama="correccion_legitima")

    # patron == "anomalo": tampering -> alerta de seguridad (cierra la deteccion)
    async with app.state.pool.acquire() as conn:
        intento = await conn.fetchrow(
            """
            SELECT actor_id, valor_anterior, valor_nuevo FROM registro_auditoria
            WHERE id = $1 AND pedido_id = $2 AND tipo = 'intento_modificacion'
            """,
            req.auditoria_id,
            req.pedido_id,
        )
        if intento is None:
            raise HTTPException(status_code=409, detail="no hay registro de auditoria para esta solicitud")

        actor_id = intento["actor_id"]
        valor_anterior = json.loads(intento["valor_anterior"])
        await conn.execute(
            """
            INSERT INTO registro_auditoria (pedido_id, actor_id, valor_anterior, valor_nuevo, tipo)
            VALUES ($1, $2, $3::jsonb, $4::jsonb, 'alerta_tampering')
            """,
            req.pedido_id,
            actor_id,
            intento["valor_anterior"],
            intento["valor_nuevo"],
        )
    alertas_generadas_total.inc()
    logger.warning(
        "ALERTA_SEGURIDAD tampering pedido=%s actor=%s auditoria_id=%s",
        req.pedido_id,
        actor_id,
        req.auditoria_id,
    )
    tampering_deteccion_seconds.labels("tampering").observe(time.perf_counter() - inicio_deteccion)

    # reaccion: restauracion puntual + bloqueo de cuenta
    with reaccion_seconds.time():
        restaurar_resp = await app.state.http.post(
            f"{COMPONENTE_RESTAURACION_URL}/restaurar",
            json={
                "pedido_id": req.pedido_id,
                "precio": valor_anterior["precio"],
                "cantidad": valor_anterior["cantidad"],
            },
        )
        restaurar_resp.raise_for_status()

        bloquear_resp = await app.state.http.post(
            f"{GESTOR_BLOQUEO_URL}/bloquear", json={"actor_id": actor_id}
        )
        bloquear_resp.raise_for_status()

        async with app.state.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO registro_auditoria (pedido_id, actor_id, valor_anterior, valor_nuevo, tipo)
                VALUES ($1, $2, $3::jsonb, $4::jsonb, 'reaccion_tampering')
                """,
                req.pedido_id,
                actor_id,
                intento["valor_nuevo"],
                intento["valor_anterior"],
            )

    reacciones_completadas_total.inc()
    return VerificarResponse(pedido_id=req.pedido_id, estado="revertido", rama="tampering")


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
