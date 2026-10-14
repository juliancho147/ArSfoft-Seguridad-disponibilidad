-- Verificacion del Experimento 2 contra la base de datos (correr al terminar la carga).
--   docker compose exec -T postgres psql -U reto2 -d reto2 < db/verificar_exp2.sql

\echo '== Registros de auditoria por tipo (alertas y reacciones deben ser iguales) =='
SELECT tipo, count(*) FROM registro_auditoria GROUP BY tipo ORDER BY tipo;

\echo '== Actores con alerta de tampering cuya cuenta NO quedo bloqueada (debe ser 0) =='
SELECT count(DISTINCT a.actor_id) AS actores_sin_bloquear
FROM registro_auditoria a
LEFT JOIN cuenta c ON c.actor_id = a.actor_id
WHERE a.tipo = 'alerta_tampering' AND coalesce(c.bloqueada, false) = false;

\echo '== Falsos positivos: cuentas semilla (solo hacen correcciones legitimas) bloqueadas (debe ser 0) =='
SELECT count(*) AS cuentas_legitimas_bloqueadas
FROM cuenta WHERE actor_id LIKE 'tendero-seed-%' AND bloqueada;

\echo '== Latencia de reaccion medida en la base (ms): alerta -> reaccion registrada =='
WITH pares AS (
    SELECT a.id,
           (SELECT min(r.ts) FROM registro_auditoria r
             WHERE r.tipo = 'reaccion_tampering' AND r.pedido_id = a.pedido_id
               AND r.actor_id = a.actor_id AND r.id > a.id) - a.ts AS delta
    FROM registro_auditoria a WHERE a.tipo = 'alerta_tampering'
)
SELECT count(*) AS alertas,
       count(delta) AS con_reaccion,
       round((percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM delta)) * 1000)::numeric, 2) AS reaccion_p50,
       round((percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM delta)) * 1000)::numeric, 2) AS reaccion_p99,
       round((max(extract(epoch FROM delta)) * 1000)::numeric, 2)                                          AS reaccion_max
FROM pares;
