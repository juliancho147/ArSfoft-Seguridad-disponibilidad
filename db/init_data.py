"""
Seed de datos para los experimentos 7.2 (Disponibilidad) y 7.3 (Seguridad).

Exp. 1 (reservar_producto):
  - 30 productos stock=5000 (prod-normal-01..30; ver STOCK_NORMAL)
  - 4 productos stock aleatorio 5-10 (prod-bajo-01..04)

Exp. 2 (modificar_pedido):
  - 300 pedidos en estado 'procesado', cada uno con su hash_integridad ya
    calculado y su propio tendero_id/cuenta (evita que el bloqueo de una
    cuenta por un hallazgo de tampering afecte a otros pedidos semilla).
    Subido de 50 a 300 frente al plan original para reducir la reutilización
    del mismo pedido bajo 30 TPS/10 min (18.000 requests).
"""
import hashlib
import os
import random
import time

import psycopg2

DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "reto2")
DB_USER = os.environ.get("POSTGRES_USER", "reto2")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "reto2")

N_PRODUCTOS_NORMALES = 30
N_PRODUCTOS_BAJOS = 4
# 500 (valor original de la bitacora 7.2) no alcanza a sostener 30 TPS/10 min:
# ~18.000 requests / 34 productos * ~3 unidades promedio ~= 1.590 unidades de
# demanda esperada por producto. Se sube a 5.000 (margen ~3x) para que solo los
# 4 productos "bajo" generen la condicion de carrera, como describe la bitacora,
# en vez de que se agote el catalogo completo.
STOCK_NORMAL = 5000
STOCK_BAJO_RANGO = (5, 10)
N_PEDIDOS_SEMILLA = 300


def calcular_hash(precio, cantidad, estado) -> str:
    # Misma fórmula que calculador_hash_integridad: sha256(precio|cantidad|estado)
    return hashlib.sha256(f"{precio}|{cantidad}|{estado}".encode()).hexdigest()


def esperar_postgres(conn_kwargs, intentos=30, espera_s=2):
    for intento in range(1, intentos + 1):
        try:
            conn = psycopg2.connect(**conn_kwargs)
            conn.close()
            return
        except psycopg2.OperationalError:
            print(f"Postgres no disponible aún (intento {intento}/{intentos})...")
            time.sleep(espera_s)
    raise RuntimeError("Postgres no respondió a tiempo")


def seed():
    conn_kwargs = dict(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    esperar_postgres(conn_kwargs)

    conn = psycopg2.connect(**conn_kwargs)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM producto")
    if cur.fetchone()[0] > 0:
        print("La base ya tiene datos, se omite el seed.")
        cur.close()
        conn.close()
        return

    productos = []
    for i in range(1, N_PRODUCTOS_NORMALES + 1):
        pid = f"prod-normal-{i:02d}"
        precio = round(random.uniform(10_000, 100_000), 2)
        productos.append((pid, STOCK_NORMAL, precio))

    for i in range(1, N_PRODUCTOS_BAJOS + 1):
        pid = f"prod-bajo-{i:02d}"
        stock = random.randint(*STOCK_BAJO_RANGO)
        precio = round(random.uniform(10_000, 100_000), 2)
        productos.append((pid, stock, precio))

    cur.executemany(
        "INSERT INTO producto (producto_id, stock, precio) VALUES (%s, %s, %s)",
        productos,
    )
    print(f"Insertados {len(productos)} productos.")

    producto_ids = [p[0] for p in productos]
    precios_por_producto = {p[0]: p[2] for p in productos}

    pedidos = []
    cuentas = []
    for i in range(1, N_PEDIDOS_SEMILLA + 1):
        pedido_id = f"ped-seed-{i:04d}"
        tendero_id = f"tendero-seed-{i:04d}"
        producto_id = random.choice(producto_ids)
        cantidad = random.randint(1, 5)
        precio = precios_por_producto[producto_id]
        estado = "procesado"
        hash_integridad = calcular_hash(precio, cantidad, estado)

        pedidos.append(
            (pedido_id, tendero_id, producto_id, cantidad, precio, estado, hash_integridad)
        )
        cuentas.append((tendero_id,))

    cur.executemany(
        """
        INSERT INTO pedido (id, tendero_id, producto_id, cantidad, precio, estado, hash_integridad)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        pedidos,
    )
    cur.executemany(
        "INSERT INTO cuenta (actor_id, bloqueada) VALUES (%s, false)",
        cuentas,
    )
    print(f"Insertados {len(pedidos)} pedidos semilla (estado procesado) + cuentas.")

    conn.commit()
    cur.close()
    conn.close()
    print("Seed completo.")


if __name__ == "__main__":
    seed()
