-- Verificacion del Experimento 1 contra la base de datos (correr al terminar la carga).
--   docker compose exec -T postgres psql -U reto2 -d reto2 < db/verificar_exp1.sql

\echo '== Reservas por estado (no debe quedar ninguna pendiente ni erronea) =='
SELECT estado, count(*) FROM reserva GROUP BY estado ORDER BY estado;

\echo '== Reservas erroneas detectadas vs revertidas (debe ser 100%) =='
SELECT count(*) FILTER (WHERE detectada_en IS NOT NULL)                          AS erroneas_detectadas,
       count(*) FILTER (WHERE detectada_en IS NOT NULL AND estado = 'revertida') AS revertidas,
       count(*) FILTER (WHERE estado = 'erronea')                                AS sin_revertir
FROM reserva;

\echo '== Productos con stock comprometido de mas (debe ser 0 filas) =='
SELECT producto_id, stock, reservado FROM producto WHERE reservado > stock;

\echo '== Pedidos confirmados con reserva invalida (debe ser 0 filas) =='
-- lo confirmado por producto no puede superar el stock real, y lo reservado
-- en producto debe coincidir exactamente con la suma de reservas confirmadas
SELECT p.producto_id, p.stock, p.reservado, coalesce(sum(r.cantidad), 0) AS confirmado
FROM producto p
LEFT JOIN reserva r ON r.producto_id = p.producto_id AND r.estado = 'confirmada'
GROUP BY p.producto_id, p.stock, p.reservado
HAVING coalesce(sum(r.cantidad), 0) > p.stock OR coalesce(sum(r.cantidad), 0) <> p.reservado;

SELECT count(*) AS pedidos_de_reservas_no_confirmadas
FROM reserva WHERE pedido_id IS NOT NULL AND estado <> 'confirmada';

\echo '== Latencias medidas en la base (ms): deteccion = creada->detectada, rollback = detectada->revertida =='
SELECT count(*) AS n,
       round((percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM detectada_en - creada_en)) * 1000)::numeric, 2)    AS deteccion_p50,
       round((percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM detectada_en - creada_en)) * 1000)::numeric, 2)    AS deteccion_p99,
       round((max(extract(epoch FROM detectada_en - creada_en)) * 1000)::numeric, 2)                                             AS deteccion_max,
       round((percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM revertida_en - detectada_en)) * 1000)::numeric, 2) AS rollback_p50,
       round((percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM revertida_en - detectada_en)) * 1000)::numeric, 2) AS rollback_p99,
       round((max(extract(epoch FROM revertida_en - detectada_en)) * 1000)::numeric, 2)                                          AS rollback_max
FROM reserva WHERE detectada_en IS NOT NULL AND revertida_en IS NOT NULL;
