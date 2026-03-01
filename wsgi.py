"""
ALETHEIA — WSGI entry point (ejecutar desde la raíz del proyecto)

Uso local:
    gunicorn wsgi:app --bind 0.0.0.0:5000

Uso en Render (Start Command):
    gunicorn wsgi:app
"""

import os
import sys

# Asegurar que la raíz del proyecto esté en sys.path
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Importar la app de Flask desde backend/app.py
from backend.app import app  # noqa: F401, E402

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
