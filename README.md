# Reto 2 — Validación de ASRs: Disponibilidad y Seguridad

Implementación de los dos experimentos de validación arquitectónica definidos en la
bitácora del Reto 2 (ARTI4109): uno para el atributo de calidad **Disponibilidad**
(`reservar_producto`) y otro para **Seguridad** (`modificar_pedido`, escenario de
tampering). El análisis completo (ASRs, hipótesis, justificación de tácticas,
diagramas) vive en `../bitacora-reto2.md` y `../plan-implementacion-experimentos-reto2.md`;
este documento cubre **cómo está construido y cómo correrlo**.

Stack: Python (FastAPI) para todos los servicios, Postgres, Prometheus + Grafana
para métricas, k6 para generar carga. Todo corre en Docker Compose para desarrollo
local; la sección de nube más abajo explica qué falta para EKS.

---

## 1. Qué se está validando

### Experimento 1 — Disponibilidad (`reservar_producto`)

**Hipótesis**: si el sistema aplica verificación de sanidad antes de confirmar una
reserva y rollback ante inconsistencia, entonces ≥99.9% de las discrepancias entre
cantidad reservada y stock real se detectan en <150ms, y el 100% de las reservas
erróneas se revierten en <200ms sin dejar stock comprometido — incluso cuando la
inconsistencia se origina en una condición de carrera real.

**Tácticas**: Verificación de sanidad (detección) + Rollback (recuperación).

**Cómo se produce la condición de carrera**: el catálogo semilla tiene 30 productos
con stock alto (5000 unidades) y 4 con stock bajo (5-10 unidades). Bajo carga
concurrente sobre los productos de bajo stock, `verificacion_sanidad` hace un
decremento atómico **sin guarda** (`UPDATE producto SET stock = stock - :cantidad
... RETURNING stock`, sin `WHERE stock >= cantidad`). Esto deja que la sobreventa
ocurra de forma natural bajo concurrencia real (tal como pide la bitácora: no se
diseñó un mecanismo de control de concurrencia dedicado). Si el stock resultante
queda negativo, `reservar_producto` hace rollback (revierte el decremento) y
rechaza la reserva; si no, confirma el pedido.

### Experimento 2 — Seguridad (`modificar_pedido`, Tampering-Tendero)

**Hipótesis**: si el sistema aplica verificación de integridad por hash + detección
de patrones de acceso + reacción vía audit trail, entonces el 100% de las
modificaciones no autorizadas sobre pedidos procesados se detectan en <150ms y se
revierten/bloquean en <200ms desde la alerta, sin falsos positivos sobre
correcciones legítimas con patrón de acceso normal.

**Tácticas**: Verificación de integridad por hash (detección) + Detección de
patrones de acceso (detección) + Audit trail: restauración puntual + bloqueo de
cuenta (reacción).

**Lógica de decisión (3 ramas)**, en `verificador_integridad_hash`:

1. **Hash coincide** → confirma (caso casi teórico: dentro de este flujo, toda
   modificación cambia precio/cantidad, así que el hash casi siempre difiere).
2. **Hash no coincide Y patrón normal** → corrección legítima: se recalcula y
   actualiza el hash (nueva línea base), se confirma sin alerta.
3. **Hash no coincide Y patrón anómalo** → tampering: se dispara la reacción
   completa (restaura el pedido al valor original + bloquea la cuenta).

**El mock**: `detector_de_patrones_de_acceso` no implementa un algoritmo real de
anomalías (fuera de alcance del reto). Recibe una etiqueta de prueba
`patron_simulado: "normal"|"anomalo"` en la solicitud y simplemente la devuelve.
El experimento valida la arquitectura de decisión de 3 ramas, no la calidad de un
algoritmo de detección real.

---

## 2. Arquitectura de la implementación

### Servicios

| Servicio | Puerto host | Responsabilidad | Experimento |
|---|---|---|---|
| `reservar_producto` | 8004 | Ingreso (`POST /reservas`) + cola in-memory + orquestación (sanidad → generar_pedido \| rollback) | 1 |
| `verificacion_sanidad` | 8002 | Decremento atómico de stock + comparación (detección) | 1 |
| `generar_pedido` | 8003 | Crea el pedido confirmado, pide el hash, hace el INSERT | 1 |
| `modificar_pedido` | 8009 | Ingreso (`POST /modificar`): aplica el UPDATE, registra auditoría, delega la verificación | 2 |
| `verificador_integridad_hash` | 8008 | Orquesta hash + patrón de acceso, decide las 3 ramas, dispara la reacción | 2 |
| `detector_de_patrones_de_acceso` | 8005 | Mock: devuelve la etiqueta `patron_simulado` recibida | 2 |
| `componente_restauracion` | 8006 | Revierte un pedido a su valor original | 2 |
| `gestor_bloqueo_cuentas` | 8007 | Bloquea la cuenta del actor | 2 |
| `calculador_hash_integridad` | 8001 | `sha256(precio\|cantidad\|estado)` — compartido por los 2 experimentos | 1 y 2 |
| `postgres` | 5432 | Base de datos única (producto, pedido, registro_auditoria, cuenta) | — |
| `prometheus` | 9090 | Scraping de métricas de negocio de cada servicio | — |
| `grafana` | 3000 | Dashboards (usuario anónimo habilitado, rol Viewer) | — |

**Decisión no obvia**: `cola_de_reserva_producto` (el componente de cola del
diagrama de arquitectura) y `reservar_producto` corren en el **mismo contenedor**.
La cola elegida es `asyncio.Queue` in-memory, que no se puede compartir entre
procesos distintos sin un broker externo (Redis, SQS...); el endpoint `POST
/reservas` hace de cola de ingreso y una corrutina interna hace de worker.

### Modelo de datos (una sola base Postgres — ver `db/init.sql`)

- `producto(producto_id, stock, precio)` — catálogo.
- `pedido(id, tendero_id, producto_id, cantidad, precio, estado, hash_integridad, fecha_creacion)`.
- `registro_auditoria(id, pedido_id, actor_id, ts, valor_anterior, valor_nuevo, tipo)` —
  usado por `verificador_integridad_hash` para saber a qué valor restaurar.
- `cuenta(actor_id, bloqueada)`.

`precio` usa `NUMERIC(12,2)` a propósito (no `NUMERIC` sin precisión): evita que
`asyncpg` introduzca ruido binario de IEEE754 al escribir un `float` de Python
(se detectó al validar el flujo de restauración — un `float` restaurado llegaba
como `59245.2600000000020...` en vez de `59245.26`).

**Nota sobre `registro_auditoria`**: corre en el mismo motor Postgres que todo lo
demás (es solo otra tabla), no en un motor separado. Esto es intencional (bitácora
7.2/7.3: "no Aurora/QLDB, para no adaptar configuración/driver"). La arquitectura
AWS-nativa (fuera de alcance de este experimento) propone Amazon QLDB o DynamoDB
para este componente — ver bitácora sección 6, pestaña "Despliegue UML - Version AWS".

### Métricas Prometheus expuestas

| Métrica | Tipo | Servicio | Umbral del ASR |
|---|---|---|---|
| `sanidad_deteccion_seconds` | Histogram | `verificacion_sanidad` | p99 < 150ms |
| `rollback_seconds` | Histogram | `reservar_producto` | p99 < 200ms |
| `reserva_total_seconds` | Histogram | `reservar_producto` | p99 < 350ms |
| `pedidos_confirmados_total` / `pedidos_rechazados_total` | Counter | `reservar_producto` | — (conteo) |
| `hash_deteccion_seconds` | Histogram | `verificador_integridad_hash` | p99 < 150ms |
| `patron_deteccion_seconds` | Histogram | `verificador_integridad_hash` | p99 < 150ms (parte de detección) |
| `reaccion_seconds` | Histogram | `verificador_integridad_hash` | p99 < 200ms desde la alerta |
| `tampering_total_seconds` | Histogram | `modificar_pedido` | p99 < 350ms |
| `hash_coincide_total` / `correcciones_legitimas_total` / `alertas_generadas_total` | Counter | `verificador_integridad_hash` | — (conteo por rama) |

Los umbrales se validan contra **estas métricas de negocio en Prometheus**, no
contra el tiempo de red que mide k6 — k6 solo confirma que el servicio responde
bajo carga sostenida.

---

## 3. Requisitos previos

- Docker y Docker Compose (Docker Desktop en Mac/Windows, o Docker Engine en Linux).
- [k6](https://k6.io/) para generar carga desde el host: `brew install k6` (Mac) o
  ver [instrucciones oficiales](https://grafana.com/docs/k6/latest/set-up/install-k6/)
  para Linux/Windows.
- Opcional: cliente `psql` para inspeccionar la base de datos directamente
  (también se puede usar `docker exec implementacion-postgres-1 psql -U reto2 -d reto2`).

---

## 4. Cómo correr todo en local

### 4.1 Levantar el stack

```bash
cd implementacion
docker compose up -d --build
```

Esto levanta Postgres, siembra el catálogo (30 productos stock=5000,
4 productos stock 5-10, 300 pedidos semilla en `procesado`) y arranca los 9
servicios FastAPI + Prometheus + Grafana. Verificar que todo quedó sano:

```bash
docker compose ps
docker compose logs seed --no-log-prefix   # debe terminar en "Seed completo."
```

### 4.2 Probar manualmente con curl

**Experimento 1 — reserva normal:**
```bash
curl -s -X POST http://localhost:8004/reservas -H 'Content-Type: application/json' \
  -d '{"producto_id":"prod-normal-01","cantidad":3,"tendero_id":"tendero-test-01"}'
```

**Experimento 2 — corrección legítima (patrón normal):**
```bash
curl -s -X POST http://localhost:8009/modificar -H 'Content-Type: application/json' \
  -d '{"pedido_id":"ped-seed-0001","precio_nuevo":40000,"cantidad_nuevo":7,"actor_id":"tendero-seed-0001","patron_simulado":"normal"}'
# -> {"estado":"confirmado","rama":"correccion_legitima"}
```

**Experimento 2 — tampering (patrón anómalo):**
```bash
curl -s -X POST http://localhost:8009/modificar -H 'Content-Type: application/json' \
  -d '{"pedido_id":"ped-seed-0002","precio_nuevo":1,"cantidad_nuevo":999,"actor_id":"tendero-seed-0002","patron_simulado":"anomalo"}'
# -> {"estado":"revertido","rama":"tampering"} y la cuenta queda bloqueada
```

(Usa cualquier `pedido_id`/`tendero_id` de la tabla `pedido` semilla —
`ped-seed-0001` .. `ped-seed-0300`.)

### 4.3 Generar carga con k6 (Experimento 1)

```bash
# Corrida corta de validación (1 min) — confirma que el pipeline responde
k6 run --env DURATION=1m --env RATE=30 k6/exp1_reserva.js

# Corrida oficial (10 min, 30 TPS, igual que el ASR). En Mac, usa caffeinate
# para que el sistema no entre en reposo a mitad de la prueba:
caffeinate -dis k6 run --env DURATION=10m --env RATE=30 \
  --summary-export=k6/results/mi_corrida.json k6/exp1_reserva.js
```

> El Experimento 2 todavía no tiene script k6 propio (ver sección 6 "Qué falta").
> Hasta entonces, su carga se puede probar manualmente con los `curl` de arriba.

### 4.4 Ver los dashboards en vivo

Abrir **mientras hay tráfico corriendo** (los paneles usan `rate()`, así que si
no hay tráfico reciente dentro de la ventana de tiempo se ven en blanco):

```
http://localhost:3000/d/exp1-disponibilidad/experimento-1-disponibilidad-reservar-producto
```

Usuario anónimo habilitado (no pide login). El dashboard trae: TPS real,
latencias p99 combinadas con líneas de umbral, distribución p50/p95/p99 por
métrica, y una barra en vivo de pedidos totales/procesados/rechazados.

Prometheus crudo (para correr cualquier PromQL propio) está en
`http://localhost:9090`.

### 4.5 Resetear los datos a la semilla limpia

```bash
docker compose down -v && docker compose up -d
```

El `-v` borra el volumen de Postgres (fuerza a que `seed` vuelva a correr) y de
paso resetea Prometheus (tampoco tiene volumen persistente, así que sus
contadores también quedan en cero).

---

## 5. Resultado de referencia (última corrida oficial validada)

Experimento 1, 10 minutos, 30 TPS, 18.000 requests, 0% fallos HTTP:

| Métrica | Umbral | Resultado (p99) |
|---|---|---|
| Detección | <150ms | 4.95 ms |
| Rollback | <200ms | 4.95 ms |
| Total end-to-end | <350ms | 15.41 ms |
| Reservas erróneas revertidas | 100% | 100% (0 productos con stock negativo) |

El Experimento 2 está validado funcionalmente (las 3 ramas se comportan como
describe la bitácora) pero todavía no tiene una corrida de carga oficial de 10 min
— ver sección 6.

---

## 6. Qué falta (estado actual)

- [x] Experimento 1: servicios, instrumentación, dashboard Grafana, script k6,
      corrida oficial de 10 min validada.
- [x] Experimento 2: servicios, instrumentación, 3 ramas validadas manualmente.
- [ ] Experimento 2: script k6 (carga con etiqueta `patron_simulado`, 88%
      normal / 12% anómalo, según bitácora 7.3) y dashboard Grafana propio.
- [ ] Experimento 2: corrida oficial de 10 min.
- [ ] Despliegue en nube (EKS) — ver sección 7, decisión explícita de dejarlo
      para después de validar todo en local.

---

## 7. Despliegue en la nube (AWS / Kubernetes gestionado)

**Estado: no implementado todavía** (decisión explícita: validar primero todo en
local). Esta sección documenta el plan para cuando se retome, tal como está
descrito en la bitácora (secciones 7.2/7.3):

### Qué sí valida este ambiente de nube

Que **las mismas imágenes Docker** que corren en Compose funcionan igual en un
Kubernetes gestionado (Amazon EKS). No es una migración de arquitectura, es una
prueba de portabilidad de contenedores.

### Qué NO valida

La arquitectura AWS-nativa completa descrita en `bitacora-reto2.md` sección 6
(pestaña "Despliegue UML - Version AWS": API Gateway, Cognito, SQS, Aurora, QLDB,
EventBridge/SNS). Esa arquitectura es la referencia para una implementación
productiva futura, no el objeto de este experimento.

### Pasos previstos

1. Build de las mismas imágenes de `services/*` y push a Amazon ECR.
2. Manifiestos Kubernetes: un `Deployment` + `Service` por cada uno de los 9
   servicios FastAPI, un `Deployment` de Postgres con `PersistentVolumeClaim`
   (mismo Postgres que en local — no Aurora, para no adaptar configuración/driver).
3. Prometheus + Grafana vía Helm chart (`kube-prometheus-stack`).
4. k6 ejecutado desde fuera del cluster, contra el `LoadBalancer`/`Ingress`
   expuesto.
5. Repetir las corridas de 10 min de ambos experimentos y comparar contra los
   resultados locales (sección 5).

### Qué necesitarías para ejecutarlo

- Cuenta de AWS con permisos para EKS, ECR, IAM, VPC.
- `aws-cli`, `eksctl` o Terraform, y `kubectl` configurados.
- Tener presente el costo: un cluster EKS + nodos corriendo 24/7 tiene costo
  continuo, a diferencia de Docker Compose local.

---

## 8. Decisiones de diseño que no son obvias leyendo el código

Para que cualquiera que retome esto no se sorprenda:

- **Cola in-memory, no Redis**: `cola_de_reserva_producto` y `reservar_producto`
  comparten proceso por esto (ver sección 2). Si se cambia a un broker real
  (Redis/SQS), se pueden separar en dos contenedores.
- **Decremento de stock sin guarda de concurrencia**: es intencional (así se
  produce la condición de carrera real que exige el Experimento 1 — no es un bug).
- **`stock` de productos "normales" en 5000, no 500**: el valor original de la
  bitácora (500) no alcanzaba a sostener 30 TPS/10min sin que *todo* el catálogo
  se agotara, no solo los 4 productos de bajo stock. Se subió a 5000 (margen ~3x
  sobre la demanda esperada) para que el comportamiento observado corresponda al
  que describe la bitácora 7.2.
- **300 pedidos semilla en vez de 50**: el valor original (50) significaba que,
  a 30 TPS/10min, cada pedido se modificaría en promedio ~360 veces, con riesgo
  de contención sobre la misma fila. Se subió a 300 para diluir esa reutilización.
- **`NUMERIC(12,2)` en vez de `NUMERIC`**: evita ruido de punto flotante al
  restaurar un pedido (ver sección 2).
- **En corridas largas de k6 en Mac, usar `caffeinate -dis`**: sin esto, si la
  pantalla se apaga a mitad de una corrida de 10 minutos, macOS puede suspender
  recursos de la VM de Docker y producir un atasco artificial que no tiene nada
  que ver con el sistema bajo prueba (nos pasó una vez; el log de energía de
  macOS lo confirmó).
- **Prometheus y Postgres no tienen volumen persistente entre `down -v` y
  `up`**: cualquier reset de datos también resetea las métricas acumuladas.

---

## 9. Referencias

- `../bitacora-reto2.md` — ASRs completos (fuente, estímulo, respuesta, medida de
  respuesta), justificación de cada táctica, histórico de decisiones y
  correcciones.
- `../plan-implementacion-experimentos-reto2.md` — plan original de
  implementación de ambos experimentos.
- `../componentes-reto2.drawio` — diagramas UML (componentes, secuencia,
  clases, despliegue agnóstico y despliegue AWS-nativo).
