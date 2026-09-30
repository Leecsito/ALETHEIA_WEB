"""
ALETHEIA — App principal
Para agregar un nuevo componente:
    1. Crea su_carpeta/su_nombre.py con un Blueprint
    2. Agrega las 2 líneas de sys.path + register_blueprint aquí

Ejecutar desde la raíz:
    gunicorn wsgi:app
    o bien: python wsgi.py
"""

import sys
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Asegurar que ROOT esté en sys.path para imports de paquetes (inicio, tablas, etc.)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from flask import Flask, send_from_directory, redirect, request
from flask_cors import CORS

try:
    from flask_compress import Compress
except ImportError:
    Compress = None

from inicio    import inicio_bp
from tablas    import tablas_bp
from visualizar import visualizar_bp
from aletheia import aletheia_bp
from partidos  import partidos_bp
from equipos   import equipos_bp
from jugadores import jugadores_bp
from eventos   import eventos_bp
from media     import media_bp

FRONTEND_FOLDERS = ['inicio', 'tablas', 'visualizar', 'aletheia', 'aletheia_preparar', 'header',
                    'partidos', 'equipos', 'jugadores', 'eventos']

# static_folder=ROOT sirve automáticamente CSS/JS/imágenes desde la raíz del proyecto
app = Flask(__name__, static_folder=ROOT, static_url_path='')
CORS(app)

# CSS/JS/imágenes se cachean en el navegador 5 min (antes iban con no-cache y se
# revalidaban en cada visita). El HTML se sirve aparte con max_age=0.
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 300

# Carpetas públicas. El resto del proyecto (backend/, .git/, .env, scripts,
# DB...) queda bloqueado: con static_folder=ROOT se podía descargar el código
# fuente y hasta /.git/config.
STATIC_PREFIXES = set(FRONTEND_FOLDERS) | {'comun', 'multimedia'}


@app.before_request
def _solo_archivos_publicos():
    if request.path.startswith('/api/'):
        return None
    first = request.path.lstrip('/').split('/', 1)[0]
    if not first or first in STATIC_PREFIXES:
        return None
    return 'Not Found', 404

# gzip/brotli para JSON, HTML, CSS y JS (menos bytes = carga más rápida)
if Compress is not None:
    app.config['COMPRESS_MIN_SIZE'] = 500
    Compress(app)

app.register_blueprint(inicio_bp)
app.register_blueprint(tablas_bp)
app.register_blueprint(visualizar_bp)
app.register_blueprint(aletheia_bp)
app.register_blueprint(partidos_bp)
app.register_blueprint(equipos_bp)
app.register_blueprint(jugadores_bp)
app.register_blueprint(eventos_bp)
app.register_blueprint(media_bp)

# -- RUTAS PARA SERVIR LAS PÁGINAS HTML --
@app.route('/')
def home():
    return redirect('/aletheia/')

@app.route('/<folder>/')
@app.route('/<folder>/index.html')
def serve_index(folder):
    if folder in FRONTEND_FOLDERS:
        return send_from_directory(os.path.join(ROOT, folder), 'index.html', max_age=0)
    return "Not Found", 404

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)