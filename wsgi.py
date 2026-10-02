"""
ALETHEIA — WSGI entry point (ejecutar desde la raíz del proyecto)

Uso local:
    gunicorn wsgi:app --bind 0.0.0.0:5000 --worker-class gthread --threads 4 --timeout 300

Uso en Render (Start Command):
    gunicorn wsgi:app --worker-class gthread --workers 1 --threads 4 --timeout 300
"""

import os
import sys

# Asegurar que la raíz del proyecto esté en sys.path
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _cargar_env():
    """Carga `.env` (raíz) en os.environ antes de importar la app.

    Sin dependencias extra (mismas reglas que `aletheia/aletheia.py`): así
    `backend/conexion.py` ve `TURSO_AUTH_TOKEN`/`TURSO_DATABASE_URL` en local.
    Solo define variables que aún no estén en el entorno (en Render mandan las
    del panel). El `.env` es local y está gitignored.
    """
    ruta = os.path.join(ROOT, '.env')
    if not os.path.isfile(ruta):
        return
    try:
        with open(ruta, 'r', encoding='utf-8') as fh:
            for linea in fh:
                linea = linea.strip()
                if not linea or linea.startswith('#') or '=' not in linea:
                    continue
                clave, _, valor = linea.partition('=')
                clave = clave.strip()
                valor = valor.strip().strip('"').strip("'")
                if clave and clave not in os.environ:
                    os.environ[clave] = valor
    except OSError:
        pass


_cargar_env()

# Importar la app de Flask desde backend/app.py
from backend.app import app  # noqa: F401, E402

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
