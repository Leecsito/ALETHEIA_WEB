"""
ALETHEIA — Conexión a SQLite
Este archivo SOLO gestiona la conexión. Nunca agregar rutas aquí.

El archivo aletheia.db vive dentro de la carpeta backend/.
Sin servidor, sin pool, sin límite de conexiones.
"""

import sqlite3
import os

# Ruta al archivo .db — mismo directorio que este archivo (backend/)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'aletheia.db')


def get_conn():
    """Abre una conexión SQLite con soporte de foreign keys y row_factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row           # permite acceder columnas por nombre
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL") # mejor rendimiento concurrente
    return conn


def release_conn(conn):
    """Cierra la conexión (mantiene la misma firma que la versión PostgreSQL)."""
    if conn:
        conn.close()