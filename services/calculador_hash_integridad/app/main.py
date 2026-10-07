"""
calculador_hash_integridad (bitacora-reto2.md 8.5)

Componente compartido entre Exp. 1 (generar_pedido) y Exp. 2 (verificador_integridad_hash).
Calcula sha256(precio|cantidad|estado) sin persistencia propia: quien lo invoca decide
qué hacer con el resultado (guardar o comparar).
"""
import hashlib

from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

app = FastAPI(title="calculador_hash_integridad")


class CalcularRequest(BaseModel):
    precio: float
    cantidad: int
    estado: str


class CalcularResponse(BaseModel):
    hash: str


def calcular_hash(precio: float, cantidad: int, estado: str) -> str:
    return hashlib.sha256(f"{precio}|{cantidad}|{estado}".encode()).hexdigest()


@app.post("/calcular", response_model=CalcularResponse)
def calcular(req: CalcularRequest) -> CalcularResponse:
    return CalcularResponse(hash=calcular_hash(req.precio, req.cantidad, req.estado))


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health():
    return {"status": "ok"}
