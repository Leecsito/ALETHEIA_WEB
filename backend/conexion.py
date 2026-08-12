"""
ALETHEIA — Conexión a Base de Datos (Turso / libSQL y SQLite local)
Este archivo SOLO gestiona la conexión. Nunca agregar rutas aquí.
"""

import os
import sqlite3

# Intenta importar libsql para soporte de Turso en la nube
try:
    import libsql
except ImportError:
    libsql = None

# Credenciales de Turso (por defecto la DB en la nube proporcionada)
TURSO_DATABASE_URL = os.environ.get(
    'TURSO_DATABASE_URL',
    'libsql://aletheia-leecsito.aws-us-east-1.turso.io'
)
TURSO_AUTH_TOKEN = os.environ.get(
    'TURSO_AUTH_TOKEN',
    'eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3ODY1NTk2ODMsImlkIjoiMDE5ZmY3MjItZDAwMS03OWQwLWJkNWQtM2Y1NGRmMDQ2ZjU3Iiwia2lkIjoiYzhBUjBCOG9VRlF6ekFULWFFV210X3NnS1o2YlNvRHRxdHFGeWtjN2d2cyIsInJpZCI6ImY4ZjYxNWE4LWFhNmEtNGUyMy04ZGJjLTBmNTg0ZDlmNWVhMCJ9.fS0i12h8SYsyP2YpY1fbf-ktNdxQ9FCJdEkmlUqG69n5VGJqhsZ9lKUyAoP-Vho4r9pJwSA8B485gOkgUITIDw'
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