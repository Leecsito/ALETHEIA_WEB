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

from flask import Flask, send_from_directory, redirect
from flask_cors import CORS

from inicio    import inicio_bp
from tablas    import tablas_bp
from visualizar import visualizar_bp
from predecir  import predecir_bp

FRONTEND_FOLDERS = ['inicio', 'tablas', 'visualizar', 'predecir']

# static_folder=ROOT sirve automáticamente CSS/JS/imágenes desde la raíz del proyecto
app = Flask(__name__, static_folder=ROOT, static_url_path='')
CORS(app)

app.register_blueprint(inicio_bp)
app.register_blueprint(tablas_bp)
app.register_blueprint(visualizar_bp)
app.register_blueprint(predecir_bp)

# -- RUTAS PARA SERVIR LAS PÁGINAS HTML --
@app.route('/')
def home():
    return redirect('/inicio/')

@app.route('/<folder>/')
@app.route('/<folder>/index.html')
def serve_index(folder):
    if folder in FRONTEND_FOLDERS:
        return send_from_directory(os.path.join(ROOT, folder), 'index.html')
    return "Not Found", 404

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)