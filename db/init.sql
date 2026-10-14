-- Esquema para los experimentos de Disponibilidad (7.2) y Seguridad (7.3)
-- Fuente: bitacora-reto2.md secciones 4.5, 7.2, 7.3, 8 + plan-implementacion-experimentos-reto2.md

CREATE TABLE producto (
    producto_id   TEXT PRIMARY KEY,
    stock         INT NOT NULL,              -- stock real en bodega
    -- cantidad reservada acumulada (reservas pendientes + confirmadas).
    -- Invariante que vigila verificacion_sanidad: reservado <= stock.
    reservado     INT NOT NULL DEFAULT 0,
    precio        NUMERIC(12,2) NOT NULL
);

CREATE TABLE pedido (
    id                TEXT PRIMARY KEY,
    tendero_id        TEXT NOT NULL,
    producto_id       TEXT NOT NULL REFERENCES producto(producto_id),
    cantidad          INT NOT NULL,
    -- precision fija (2 decimales): evita que asyncpg introduzca ruido binario
    -- de IEEE754 al escribir un float de Python (ej. componente_restauracion)
    precio            NUMERIC(12,2) NOT NULL,
    estado            TEXT NOT NULL,          -- 'reservado' | 'procesado' | 'rechazado'
    hash_integridad   TEXT,
    fecha_creacion    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reserva calculada y persistida por reservar_producto (Exp. 1).
-- reservado_acumulado es la cantidad reservada del producto que quedo persistida
-- al guardar esta reserva; verificacion_sanidad la compara contra el stock real.
CREATE TABLE reserva (
    id                   TEXT PRIMARY KEY,
    producto_id          TEXT NOT NULL REFERENCES producto(producto_id),
    tendero_id           TEXT NOT NULL,
    cantidad             INT NOT NULL,
    reservado_acumulado  INT NOT NULL,
    estado               TEXT NOT NULL,       -- 'pendiente' | 'confirmada' | 'erronea' | 'revertida'
    pedido_id            TEXT REFERENCES pedido(id),
    creada_en            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    detectada_en         TIMESTAMPTZ,         -- cuando sanidad la marco como erronea
    revertida_en         TIMESTAMPTZ          -- cuando el rollback la revirtio
);

CREATE INDEX idx_reserva_producto_estado ON reserva(producto_id, estado);

CREATE TABLE registro_auditoria (
    id             SERIAL PRIMARY KEY,
    pedido_id      TEXT NOT NULL REFERENCES pedido(id),
    actor_id       TEXT NOT NULL,
    ts             TIMESTAMPTZ NOT NULL DEFAULT now(),
    valor_anterior JSONB,            -- {precio, cantidad}
    valor_nuevo    JSONB,
    -- 'intento_modificacion' | 'alerta_tampering' | 'reaccion_tampering'
    tipo           TEXT NOT NULL
);

-- Historial inmutable: solo se permite INSERT sobre el audit trail.
CREATE FUNCTION registro_auditoria_inmutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'registro_auditoria es inmutable (solo INSERT)';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_registro_auditoria_inmutable
    BEFORE UPDATE OR DELETE ON registro_auditoria
    FOR EACH ROW EXECUTE FUNCTION registro_auditoria_inmutable();

CREATE TABLE cuenta (
    actor_id    TEXT PRIMARY KEY,
    bloqueada   BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_registro_auditoria_pedido_id ON registro_auditoria(pedido_id);
