"""
detector_de_patrones_de_acceso (bitacora-reto2.md 8.6) -- MOCK

No implementa un algoritmo real de anomalias (fuera de alcance de este reto).
Recibe la etiqueta de prueba `patron_simulado` que vino en la solicitud original
(propagada por modificar_pedido -> verificador_integridad_hash) y simplemente la
retorna. El experimento valida la arquitectura de decision de 3 ramas en
verificador_integridad_hash, no la calidad de un algoritmo de deteccion real.
"""
from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

app = FastAPI(title="detector_de_patrones_de_acceso (mock)")


class ValidarRequest(BaseModel):
    patron_simulado: str  # "normal" | "anomalo"


class ValidarResponse(BaseModel):
    patron: str


@app.post("/validar", response_model=ValidarResponse)
def validar(req: ValidarRequest) -> ValidarResponse:
    return ValidarResponse(patron=req.patron_simulado)


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health():
    return {"status": "ok"}
