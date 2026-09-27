"""
ALETHEIA — Conexión a Base de Datos (Turso / libSQL y SQLite local)
Este archivo SOLO gestiona la conexión. Nunca agregar rutas aquí.
"""

import os
import sqlite3
import threading

# Intenta importar libsql para soporte de Turso en la nube
try:
    import libsql
except ImportError:
    libsql = None

# Credenciales de Turso (por defecto la DB en la nube proporcionada)
TURSO_DATABASE_URL = os.environ.get(
    'TURSO_DATABASE_URL',
    'libsql://aletheia-laperradeadrelees.aws-us-east-1.turso.io'
)
TURSO_AUTH_TOKEN = os.environ.get(
    'TURSO_AUTH_TOKEN',
    'eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3OTAzNzA4MTgsImlkIjoiMDFhMGRhNjctMzUwMS03Y2Q4LWIzZmMtZDdjZGNkOTA1YjU4Iiwia2lkIjoiZUl5NHJsYll5Qk9CNHpUbDRpekNNbWRiRkI2a09Uc2dhTmFMQ1lrRDB5ZyIsInJpZCI6IjU5NDJlMmExLWY2YWMtNDYyMy1hNmJiLTg0OTUwNTRiMTczNiJ9.UZ0qDE1MQi2qbQHHMhqMl33r0mx0VRZJ26sBObcI0iF-6g7sbcy4UubNUtgOmHsBoTGgJO7D5mFBB619htWmAw'
)

# Ruta local de reserva (fallback)
LOCAL_DB_PATH = os.environ.get(
    'DATABASE_PATH',
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'aletheia.db')
)


def get_conn():
    """
    Abre una conexión a Turso (libSQL) si la URL es remota (libsql:// o https://),
    o a SQLite local si no hay librería libsql o se especifica un archivo local.
    """
    url = TURSO_DATABASE_URL.strip() if TURSO_DATABASE_URL else ""
    token = TURSO_AUTH_TOKEN.strip() if TURSO_AUTH_TOKEN else ""

    is_remote = url.startswith("libsql://") or url.startswith("https://") or url.startswith("http://")

    if is_remote and libsql is not None:
        conn = libsql.connect(database=url, auth_token=token)
    else:
        conn = sqlite3.connect(LOCAL_DB_PATH)

    if hasattr(conn, 'row_factory'):
        try:
            conn.row_factory = sqlite3.Row
        except Exception:
            pass

    # Aplicar PRAGMAs de forma segura
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        pass

    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except Exception:
        pass

    return conn


def release_conn(conn):
    """Cierra la conexión adecuadamente."""
    if conn:
        try:
            conn.close()
        except Exception:
            pass


# ─── RUTA RÁPIDA PARA LECTURAS ───────────────────────────────────────────────
# Abrir una conexión a Turso cuesta ~0.7 s; una consulta sobre una conexión ya
# abierta ~0.2 s. Para las lecturas del sitio (que no mutan nada) se reutiliza
# UNA conexión por hilo con reintento único si Turso la cerró por inactividad.
_local = threading.local()


def _conn_lectura():
    conn = getattr(_local, 'conn', None)
    if conn is None:
        conn = get_conn()
        _local.conn = conn
    return conn


def reset_conn():
    """Descarta la conexión de lectura del hilo (se reabre en la próxima consulta)."""
    conn = getattr(_local, 'conn', None)
    _local.conn = None
    if conn:
        try:
            conn.close()
        except Exception:
            pass


def fetch_all(sql, params=None):
    """Ejecuta un SELECT y devuelve filas como dicts, reutilizando la conexión
    del hilo y reintentando una vez si la conexión quedó muerta."""
    ultimo_error = None
    for _ in range(2):
        conn = _conn_lectura()
        try:
            cur = conn.cursor()
            cur.execute(sql, list(params or []))
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            cur.close()
            return rows
        except Exception as e:      # conexión caída o error transitorio
            ultimo_error = e
            reset_conn()
    raise ultimo_error