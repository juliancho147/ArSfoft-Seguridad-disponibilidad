/*
 * Experimento 1 - Disponibilidad (bitacora-reto2.md 7.2)
 *
 * 30 TPS constante contra POST /reservas, distribuido uniformemente entre los
 * 34 productos del seed (30 con stock=500, 4 con stock bajo 5-10). La cantidad
 * solicitada es aleatoria entre 1 y 5. Con suficiente concurrencia sobre los
 * productos de bajo stock, la suma de reservas los excede de forma natural
 * (condicion de carrera real, sin control de concurrencia dedicado).
 *
 * Uso:
 *   Corrida corta de validacion (1 min):
 *     k6 run --env DURATION=1m --env RATE=30 k6/exp1_reserva.js
 *   Corrida real (10 min):
 *     k6 run --env DURATION=10m --env RATE=30 --summary-export=k6/results/exp1_real.json k6/exp1_reserva.js
 *
 * Nota (bitacora/plan): los umbrales de deteccion <150ms, rollback <200ms y
 * total <350ms se validan contra las metricas de negocio en Prometheus
 * (sanidad_deteccion_seconds, rollback_seconds, reserva_total_seconds), no
 * contra el tiempo de red que mide k6 aqui. Los checks de este script solo
 * verifican que el servicio responda de forma valida bajo carga.
 */
import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8004";
const DURATION = __ENV.DURATION || "10m";
const RATE = parseInt(__ENV.RATE || "30", 10);

const PRODUCTOS = [];
for (let i = 1; i <= 30; i++) {
  PRODUCTOS.push(`prod-normal-${String(i).padStart(2, "0")}`);
}
for (let i = 1; i <= 4; i++) {
  PRODUCTOS.push(`prod-bajo-${String(i).padStart(2, "0")}`);
}

export const options = {
  scenarios: {
    carga_constante: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: DURATION,
      preAllocatedVUs: Math.max(50, RATE * 2),
      maxVUs: Math.max(200, RATE * 10),
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
  },
};

export default function () {
  const producto_id = PRODUCTOS[Math.floor(Math.random() * PRODUCTOS.length)];
  const cantidad = Math.floor(Math.random() * 5) + 1;
  const tendero_id = `tendero-k6-${__VU}-${__ITER}`;

  const res = http.post(
    `${BASE_URL}/reservas`,
    JSON.stringify({ producto_id, cantidad, tendero_id }),
    { headers: { "Content-Type": "application/json" } }
  );

  check(res, {
    "status 200": (r) => r.status === 200,
    "estado valido": (r) => {
      if (r.status !== 200) return false;
      const body = r.json();
      return body.estado === "procesado" || body.estado === "rechazado";
    },
  });
}
