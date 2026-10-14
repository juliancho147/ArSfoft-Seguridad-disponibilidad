/*
 * Experimento 2 - Seguridad (bitacora-reto2.md 7.3)
 *
 * 30 TPS constantes contra POST /modificar sobre los pedidos semilla (ya
 * procesados). Toda solicitud cambia precio/cantidad, asi que el hash siempre
 * difiere; quien discrimina es la etiqueta de prueba `patron_simulado`:
 *   - 80% "normal"  -> correccion legitima hecha por el duenio del pedido.
 *   - 20% "anomalo" -> tampering; debe revertirse y bloquear la cuenta.
 *
 * Cada solicitud anomala usa una cuenta distinta (una cuenta comprometida por
 * intento). Si se reutilizaran las 300 cuentas semilla, a 6 bloqueos/seg todas
 * quedarian bloqueadas en el primer minuto y el resto de la corrida serian 403.
 * Un REINTENTO_PCT de las anomalas reintenta con la misma cuenta para comprobar
 * que el bloqueo tiene efecto (debe responder 403).
 *
 * Uso:
 *   k6 run --env DURATION=1m k6/exp2_tampering.js
 *   k6 run --env DURATION=10m --summary-export=k6/results/exp2_real.json k6/exp2_tampering.js
 *
 * Los umbrales (deteccion <150ms, reaccion <200ms, total <350ms) se validan en
 * Prometheus (tampering_deteccion_seconds, reaccion_seconds,
 * tampering_total_seconds) y con db/verificar_exp2.sql.
 */
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8009";
const DURATION = __ENV.DURATION || "10m";
const RATE = parseInt(__ENV.RATE || "30", 10);
const PCT_ANOMALO = parseFloat(__ENV.PCT_ANOMALO || "0.2");
const REINTENTO_PCT = parseFloat(__ENV.REINTENTO_PCT || "0.1");
const N_PEDIDOS = parseInt(__ENV.N_PEDIDOS || "300", 10);

const HEADERS = { headers: { "Content-Type": "application/json" } };
const CORRIDA = Date.now();

const falsosPositivos = new Counter("falsos_positivos");
const tamperingRevertido = new Counter("tampering_revertido");
const tamperingNoRevertido = new Counter("tampering_no_revertido");
const bloqueoEfectivo = new Counter("bloqueo_efectivo");
const bloqueoSinEfecto = new Counter("bloqueo_sin_efecto");

// 403 es una respuesta esperada (cuenta bloqueada), no un fallo HTTP.
http.setResponseCallback(http.expectedStatuses(200, 403));

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
    falsos_positivos: ["count==0"],
    tampering_no_revertido: ["count==0"],
    bloqueo_sin_efecto: ["count==0"],
  },
};

function modificar(sufijo, actor_id, patron_simulado) {
  return http.post(
    `${BASE_URL}/modificar`,
    JSON.stringify({
      pedido_id: `ped-seed-${sufijo}`,
      precio_nuevo: Math.round((10000 + Math.random() * 90000) * 100) / 100,
      cantidad_nuevo: Math.floor(Math.random() * 20) + 1,
      actor_id,
      patron_simulado,
    }),
    HEADERS
  );
}

export default function () {
  const sufijo = String(Math.floor(Math.random() * N_PEDIDOS) + 1).padStart(4, "0");

  if (Math.random() >= PCT_ANOMALO) {
    const res = modificar(sufijo, `tendero-seed-${sufijo}`, "normal");
    const ok = check(res, {
      "normal: 200 confirmado sin alerta": (r) =>
        r.status === 200 && r.json().estado === "confirmado" && r.json().rama !== "tampering",
    });
    if (!ok) falsosPositivos.add(1);
    return;
  }

  const actor = `tendero-anomalo-${CORRIDA}-${__VU}-${__ITER}`;
  const res = modificar(sufijo, actor, "anomalo");
  const ok = check(res, {
    "anomalo: 200 revertido": (r) =>
      r.status === 200 && r.json().estado === "revertido" && r.json().rama === "tampering",
  });
  (ok ? tamperingRevertido : tamperingNoRevertido).add(1);

  if (ok && Math.random() < REINTENTO_PCT) {
    const reintento = modificar(sufijo, actor, "anomalo");
    const bloqueado = check(reintento, {
      "cuenta bloqueada: 403": (r) => r.status === 403,
    });
    (bloqueado ? bloqueoEfectivo : bloqueoSinEfecto).add(1);
  }
}
