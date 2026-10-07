-- Esquema para los experimentos de Disponibilidad (7.2) y Seguridad (7.3)
-- Fuente: bitacora-reto2.md secciones 4.5, 7.2, 7.3, 8 + plan-implementacion-experimentos-reto2.md

CREATE TABLE producto (
    producto_id   TEXT PRIMARY KEY,
    stock         INT NOT NULL,
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

CREATE TABLE registro_auditoria (
    id             SERIAL PRIMARY KEY,
    pedido_id      TEXT NOT NULL REFERENCES pedido(id),
    actor_id       TEXT NOT NULL,
    ts             TIMESTAMPTZ NOT NULL DEFAULT now(),
    valor_anterior JSONB NOT NULL,   -- {precio, cantidad}
    valor_nuevo    JSONB NOT NULL,
    tipo           TEXT NOT NULL     -- 'intento_modificacion'
);

CREATE TABLE cuenta (
    actor_id    TEXT PRIMARY KEY,
    bloqueada   BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_registro_auditoria_pedido_id ON registro_auditoria(pedido_id);
