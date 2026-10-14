# Reto 2 — Validación de ASRs: Disponibilidad y Seguridad

Implementación de los dos experimentos de validación arquitectónica definidos en la
bitácora del Reto 2 (ARTI4109): uno para el atributo de calidad **Disponibilidad**
(`reservar_producto`) y otro para **Seguridad** (`modificar_pedido`, escenario de
tampering). El análisis completo (ASRs, hipótesis, justificación de tácticas,
diagramas) vive en el documento de arquitectura del Reto 2 (la bitácora, que no está
en este repositorio); este documento cubre **cómo está construido y cómo correrlo**.

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

**Cómo se produce la condición de carrera**: `reservar_producto` sigue el diagrama
de secuencia tal cual — `get_stock` (lee stock real y cantidad ya reservada),
`calcular_reserva` (si lo disponible no alcanza, rechaza por falta de stock) y
`save_reserva` (persiste la reserva y acumula `producto.reservado`). Ese
check-then-act **no tiene control de concurrencia** (intencional: la bitácora no
diseña un componente para eso) y hay 8 workers consumiendo la cola, así que dos
solicitudes simultáneas sobre el mismo producto leen el mismo disponible y ambas
reservan: la cantidad reservada acumulada queda por encima del stock real.

**Detección**: `verificacion_sanidad` solo lee de la base (`reserva.reservado_acumulado`
vs `producto.stock`), compara, y si no cuadra marca la reserva como `erronea`.

**Recuperación**: `reservar_producto` revierte la reserva en una sola transacción
(devuelve las unidades y deja la reserva en `revertida`) y responde el rechazo.
Lo mismo ocurre si `generar_pedido` falla después de reservar: no queda stock
comprometido sin pedido.

Hay entonces dos tipos de rechazo: el **normal** (no había unidades al calcular;
no es una falla) y el de **reserva errónea** (falla por carrera, detectada y
revertida). Solo el segundo es el objeto del experimento.

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
3. **Hash no coincide Y patrón anómalo** → tampering: se genera la alerta de
   seguridad (registro `alerta_tampering` en el audit trail + log
   `ALERTA_SEGURIDAD`) y se dispara la reacción completa (restaura el pedido al
   valor original + bloquea la cuenta), que queda registrada como
   `reaccion_tampering`.

La restauración usa el registro de auditoría **de esa solicitud** (`auditoria_id`),
no "el último del pedido", y `modificar_pedido` serializa por pedido todo el ciclo
modificación → verificación → reacción (advisory lock de Postgres), para que dos
modificaciones simultáneas al mismo pedido no se crucen. Una cuenta bloqueada
recibe `403` en su siguiente intento.

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
| `reservar_producto` | 8004 | Ingreso (`POST /reservas`) + cola in-memory + 8 workers: get_stock → calcular_reserva → save_reserva → sanidad → generar_pedido \| rollback | 1 |
| `verificacion_sanidad` | 8002 | Lee reservado vs stock real, compara y marca la reserva errónea (detección) | 1 |
| `generar_pedido` | 8003 | Crea el pedido confirmado, pide el hash, hace el INSERT | 1 |
| `modificar_pedido` | 8009 | Ingreso (`POST /modificar`): rechaza cuentas bloqueadas, registra auditoría, aplica el UPDATE, delega la verificación | 2 |
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
La cola elegida es `asyncio.Queue` in-memory (el diagrama de despliegue propone
Kafka), que no se puede compartir entre procesos distintos sin un broker externo;
el endpoint `POST /reservas` hace de cola de ingreso y `N_WORKERS` corrutinas
(8 por defecto) hacen de workers concurrentes.

### Modelo de datos (una sola base Postgres — ver `db/init.sql`)

- `producto(producto_id, stock, reservado, precio)` — catálogo. `stock` es el stock
  real; `reservado` la cantidad reservada acumulada. Invariante: `reservado <= stock`.
- `reserva(id, producto_id, tendero_id, cantidad, reservado_acumulado, estado, pedido_id,
  creada_en, detectada_en, revertida_en)` — estados `pendiente → confirmada` o
  `pendiente → erronea → revertida`. Los timestamps permiten auditar el experimento
  directamente en la base.
- `pedido(id, tendero_id, producto_id, cantidad, precio, estado, hash_integridad, fecha_creacion)`.
- `registro_auditoria(id, pedido_id, actor_id, ts, valor_anterior, valor_nuevo, tipo)` —
  tipos `intento_modificacion`, `alerta_tampering`, `reaccion_tampering`. Es
  inmutable: un trigger rechaza `UPDATE` y `DELETE`.
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
| `sanidad_deteccion_seconds{resultado}` | Histogram | `verificacion_sanidad` | p99 < 150ms (`resultado="erronea"`) |
| `rollback_seconds` | Histogram | `reservar_producto` | p99 < 200ms |
| `reserva_total_seconds` | Histogram | `reservar_producto` | p99 < 350ms (incluye espera en cola) |
| `reservas_erroneas_detectadas_total` | Counter | `verificacion_sanidad` | debe igualar a las revertidas |
| `reservas_revertidas_total` / `reservas_sin_stock_total` | Counter | `reservar_producto` | — |
| `pedidos_confirmados_total` / `pedidos_rechazados_total` | Counter | `reservar_producto` | — (conteo) |
| `tampering_deteccion_seconds{rama}` | Histogram | `verificador_integridad_hash` | p99 < 150ms (`rama="tampering"`) |
| `hash_deteccion_seconds` / `patron_deteccion_seconds` | Histogram | `verificador_integridad_hash` | — (desglose de la detección) |
| `reaccion_seconds` | Histogram | `verificador_integridad_hash` | p99 < 200ms desde la alerta |
| `tampering_total_seconds` | Histogram | `modificar_pedido` | p99 < 350ms |
| `hash_coincide_total` / `correcciones_legitimas_total` / `alertas_generadas_total` / `reacciones_completadas_total` | Counter | `verificador_integridad_hash` | — (conteo por rama) |
| `intentos_cuenta_bloqueada_total` | Counter | `modificar_pedido` | — |

Todos los histogramas tienen fronteras de bucket exactamente en 150, 200 y 350 ms
(con los buckets por defecto de `prometheus_client` no hay frontera en esos valores
y `histogram_quantile` interpola justo donde se decide el experimento).

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
  (también se puede usar `docker compose exec postgres psql -U reto2 -d reto2`).

---

## 4. Cómo correr todo en local

### 4.1 Levantar el stack

```bash
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
# -> {"estado":"revertido","rama":"tampering"} y la cuenta queda bloqueada:
#    repetir el mismo curl responde 403 "cuenta bloqueada"
```

(Usa cualquier `pedido_id`/`tendero_id` de la tabla `pedido` semilla —
`ped-seed-0001` .. `ped-seed-0300`.)

### 4.3 Generar carga con k6

```bash
# Corrida corta de validación (1 min)
k6 run --env DURATION=1m k6/exp1_reserva.js
k6 run --env DURATION=1m k6/exp2_tampering.js

# Corridas oficiales (10 min, 30 TPS, igual que los ASR). En Mac, usa caffeinate
# para que el sistema no entre en reposo a mitad de la prueba:
caffeinate -dis k6 run --env DURATION=10m --summary-export=k6/results/exp1.json k6/exp1_reserva.js
caffeinate -dis k6 run --env DURATION=10m --summary-export=k6/results/exp2.json k6/exp2_tampering.js
```

Resetear los datos (4.5) entre un experimento y otro.

**Experimento 1**: cada iteración envía una ráfaga de 2 solicitudes simultáneas
sobre el mismo producto (15 iteraciones/s = 30 TPS). Con llegadas espaciadas
uniformemente cada reserva termina antes de que llegue la siguiente y la carrera
nunca ocurre. Un segundo escenario llama cada 5 s a `POST /_test/reponer`, que
repone los 4 productos de bajo stock a 5-10 unidades disponibles; sin eso se
agotan en los primeros segundos y no hay más contención en el resto de la corrida.

**Experimento 2**: 80% `normal` (el dueño del pedido) / 20% `anomalo`. Cada
solicitud anómala usa una cuenta distinta, y un 10% reintenta con la misma cuenta
para comprobar que el bloqueo responde `403`. El script falla si hay algún falso
positivo, algún tampering sin revertir o algún bloqueo sin efecto.

Al terminar cada corrida, verificar contra la base:

```bash
docker compose exec -T postgres psql -U reto2 -d reto2 < db/verificar_exp1.sql
docker compose exec -T postgres psql -U reto2 -d reto2 < db/verificar_exp2.sql
```

### 4.4 Ver los dashboards en vivo

Abrir **mientras hay tráfico corriendo** (los paneles usan `rate()`, así que si
no hay tráfico reciente dentro de la ventana de tiempo se ven en blanco):

```
http://localhost:3000/d/exp1-disponibilidad
http://localhost:3000/d/exp2-seguridad
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

## 5. Resultados

### Experimento 1 — corrida oficial (11 de octubre de 2026, 10 min, 30 TPS, local)

| Métrica | Objetivo | Resultado |
|---|---|---|
| Detección p99 (reservas erróneas) | <150ms | 4.6 ms en el servicio; 8.3 ms medido en la base (máx. 30.8 ms) |
| Rollback p99 | <200ms | 12.4 ms en el servicio; 17.0 ms medido en la base (máx. 20.8 ms) |
| Total end-to-end p99 (incluye cola) | <350ms | 37.2 ms |
| Reservas erróneas revertidas | 100% | 168 de 168 |
| Pedidos confirmados con reserva inválida | 0 | 0 |

18.000 reservas a 30.2 TPS reales: 16.931 confirmadas, 901 rechazos normales por
falta de stock y 168 reservas erróneas por condición de carrera. 37 solicitudes
(0.2%) superaron los 350 ms; por ventanas de 1 minuto el p99 total tuvo dos picos
(~280 ms hacia el minuto 2 y 470-590 ms en el cierre), de causa no identificada.
Una de las 120 llamadas a `/_test/reponer` falló; ninguna reserva falló.

Ambiente: MacBook Air M2 (8 núcleos, 16 GB), Docker Desktop con 8 CPU y 8.2 GB,
k6 2.2.0 en la misma máquina. Los resultados anteriores de este README (p99 de
4.95 ms) correspondían a una versión que procesaba las reservas en serie, sin
condición de carrera, y se retiraron.

### Experimento 2 — pendiente de corrida oficial

Prueba de humo de 60 s a 30 TPS (no reemplaza la corrida oficial): 1.800
modificaciones, 1.466 correcciones legítimas sin alerta (0 falsos positivos), 334
tampering con 334 alertas y 334 reacciones; 97/97 reintentos con cuenta bloqueada
respondieron 403. Detección p99 ≈ 9 ms, reacción p99 ≈ 10 ms, total p99 ≈ 24 ms.

---

## 6. Qué falta (estado actual)

- [x] Experimento 1: servicios, instrumentación, dashboard Grafana, script k6.
- [x] Experimento 2: servicios, instrumentación, dashboard Grafana, script k6.
- [x] Experimento 1: corrida oficial de 10 min (ver sección 5).
- [ ] Experimento 2: corrida oficial de 10 min.
- [ ] Despliegue en nube (EKS) — ver sección 7.
- [ ] Fuera de este experimento pero presentes en la arquitectura: API Gateway,
      Kafka como cola, el Lambda verificador de hashes y TLS/mTLS entre servicios
      (hoy todo el tráfico interno es HTTP plano).

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
- **`calcular_reserva` sin control de concurrencia**: es intencional (así se
  produce la condición de carrera real que exige el Experimento 1 — no es un bug).
- **La detección es conservadora**: una reserva se marca errónea si el acumulado
  que ella misma persistió supera el stock, aunque otra reserva del mismo choque
  esté por revertirse. Nunca se confirma de más; a veces se rechaza una reserva
  que habría cabido tras el rollback de la otra.
- **`POST /_test/reponer` y `patron_simulado`** son soporte de prueba, no parte
  del contrato real. El endpoint solo existe con `ENABLE_TEST_ENDPOINTS=true`.
- **`actor_id` llega en el cuerpo de la solicitud**: no hay autenticación ni se
  valida que el actor sea dueño del pedido (Resistir está fuera de alcance).
- **`stock` de productos "normales" en 5000, no 500**: el valor de la
  bitácora (500, **pendiente de actualizar allá**) no alcanzaba a sostener 30 TPS/10min sin que *todo* el catálogo
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

- Documento de arquitectura "Reto 2: Disponibilidad y Seguridad — Sistema CCP"
  (bitácora): ASRs, vistas, decisiones y planeación de los experimentos. No está
  versionado en este repositorio.
