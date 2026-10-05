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

from flask import Flask, send_from_directory, request
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

# Caché de estáticos (F7): el CSS/JS se sirve con `?v=<hash>` (nombres
# versionados) y 1 año `immutable`; las imágenes (avif/svg/png) no llevan
# fingerprint, así que se cachean 30 días sin `immutable`. El HTML se sirve
# aparte con max_age=0 (no-cache) para que un deploy nuevo se vea al instante.
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 30 * 24 * 3600

# Extensiones con caché larga por ser estáticos inmutables a corto plazo.
_EXT_JS_CSS = {'.css', '.js'}
_EXT_IMAGENES = {'.avif', '.svg', '.png', '.jpg', '.jpeg', '.webp', '.ico',
                 '.woff', '.woff2'}

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


@app.after_request
def _cache_estaticos(resp):
    """Cabeceras de caché de estáticos (F7).

    - CSS/JS con `?v=` (fingerprint): 1 año `immutable`.
    - CSS/JS sin fingerprint (por si algún enlace viejo lo omite): 5 min.
    - Imágenes/fuentes: 30 días (URLs sin hash, sin `immutable`).
    - HTML y API: intactos (el HTML sale con no-cache de `serve_index`).
    """
    if request.path.startswith('/api/') or resp.status_code not in (200, 304):
        return resp
    ext = os.path.splitext(request.path)[1].lower()
    if ext in _EXT_JS_CSS:
        if request.args.get('v'):
            resp.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
        else:
            resp.headers['Cache-Control'] = 'public, max-age=300'
    elif ext in _EXT_IMAGENES:
        resp.headers['Cache-Control'] = f'public, max-age={30 * 24 * 3600}'
    return resp

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
# F13: `/` sirve EN VIVO directo (200) en vez de redirigir a `/aletheia/`
# (se ahorra un viaje de red); `/aletheia` sin barra final también responde
# directo gracias a `strict_slashes=False` (sin 308 de Werkzeug).
@app.route('/')
def home():
    return send_from_directory(os.path.join(ROOT, 'aletheia'), 'index.html', max_age=0)


@app.route('/<folder>/', strict_slashes=False)
@app.route('/<folder>/index.html')
def serve_index(folder):
    if folder in FRONTEND_FOLDERS:
        return send_from_directory(os.path.join(ROOT, folder), 'index.html', max_age=0)
    return "Not Found", 404

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)