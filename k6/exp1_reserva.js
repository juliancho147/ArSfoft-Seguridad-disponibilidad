/*
 * Experimento 1 - Disponibilidad (bitacora-reto2.md 7.2)
 *
 * 30 TPS constantes contra POST /reservas, distribuidos uniformemente entre los
 * 34 productos del seed (30 con stock alto, 4 con stock bajo 5-10). La cantidad
 * solicitada es aleatoria entre 1 y 5.
 *
 * Para que exista concurrencia real, cada iteracion envia una rafaga de BURST
 * solicitudes simultaneas sobre el mismo producto (RATE/BURST iteraciones por
 * segundo, asi que el total sigue siendo RATE TPS). Con llegadas espaciadas
 * uniformemente (1 cada 33ms) cada reserva termina antes de que llegue la
 * siguiente y la condicion de carrera nunca ocurre.
 *
 * Un segundo escenario repone los productos de bajo stock cada REPONER_CADA
 * segundos (POST /_test/reponer). Sin reposicion los 4 productos se agotan en
 * los primeros segundos y no hay mas contencion en el resto de la corrida.
 *
 * Uso:
 *   Corrida corta de validacion (1 min):
 *     k6 run --env DURATION=1m k6/exp1_reserva.js
 *   Corrida real (10 min):
 *     k6 run --env DURATION=10m --summary-export=k6/results/exp1_real.json k6/exp1_reserva.js
 *
 * Los umbrales de deteccion <150ms, rollback <200ms y total <350ms se validan
 * contra las metricas de negocio en Prometheus (sanidad_deteccion_seconds,
 * rollback_seconds, reserva_total_seconds) y contra db/verificar_exp1.sql, no
 * contra el tiempo de red que mide k6.
 */
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8004";
const DURATION = __ENV.DURATION || "10m";
const RATE = parseInt(__ENV.RATE || "30", 10);
const BURST = parseInt(__ENV.BURST || "2", 10);
const REPONER_CADA = parseInt(__ENV.REPONER_CADA || "5", 10);

const confirmadas = new Counter("reservas_confirmadas");
const rechazadas = new Counter("reservas_rechazadas");

const PRODUCTOS = [];
for (let i = 1; i <= 30; i++) {
  PRODUCTOS.push(`prod-normal-${String(i).padStart(2, "0")}`);
}
for (let i = 1; i <= 4; i++) {
  PRODUCTOS.push(`prod-bajo-${String(i).padStart(2, "0")}`);
}

const ITERACIONES_POR_SEG = Math.max(1, Math.round(RATE / BURST));
const HEADERS = { headers: { "Content-Type": "application/json" } };

export const options = {
  scenarios: {
    carga_constante: {
      executor: "constant-arrival-rate",
      exec: "reservar",
      rate: ITERACIONES_POR_SEG,
      timeUnit: "1s",
      duration: DURATION,
      preAllocatedVUs: Math.max(50, RATE * 2),
      maxVUs: Math.max(200, RATE * 10),
    },
    reposicion: {
      executor: "constant-arrival-rate",
      exec: "reponer",
      rate: 1,
      timeUnit: `${REPONER_CADA}s`,
      duration: DURATION,
      preAllocatedVUs: 1,
      maxVUs: 2,
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
  },
};

export function reservar() {
  const producto_id = PRODUCTOS[Math.floor(Math.random() * PRODUCTOS.length)];

  const solicitudes = [];
  for (let i = 0; i < BURST; i++) {
    const cantidad = Math.floor(Math.random() * 5) + 1;
    const tendero_id = `tendero-k6-${__VU}-${__ITER}-${i}`;
    solicitudes.push([
      "POST",
      `${BASE_URL}/reservas`,
      JSON.stringify({ producto_id, cantidad, tendero_id }),
      HEADERS,
    ]);
  }

  for (const res of http.batch(solicitudes)) {
    check(res, {
      "status 200": (r) => r.status === 200,
      "estado valido": (r) => {
        if (r.status !== 200) return false;
        const estado = r.json().estado;
        return estado === "procesado" || estado === "rechazado";
      },
    });
    if (res.status === 200) {
      (res.json().estado === "procesado" ? confirmadas : rechazadas).add(1);
    }
  }
}

export function reponer() {
  const res = http.post(`${BASE_URL}/_test/reponer`, JSON.stringify({}), HEADERS);
  check(res, { "reposicion 200": (r) => r.status === 200 });
}
