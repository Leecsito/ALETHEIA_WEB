"""
ALETHEIA — App principal
Para agregar un nuevo componente:
    1. Crea su_carpeta/su_nombre.py con un Blueprint
    2. Agrega las 2 líneas de sys.path + register_blueprint aquí

Ejecutar:
    python backend/app.py
"""

import sys
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, 'backend'))    # conexion.py accesible globalmente
sys.path.insert(0, os.path.join(ROOT, 'inicio'))
sys.path.insert(0, os.path.join(ROOT, 'tablas'))
sys.path.insert(0, os.path.join(ROOT, 'visualizar'))
sys.path.insert(0, os.path.join(ROOT, 'predecir'))
sys.path.insert(0, os.path.join(ROOT, 'predecir'))

from predecir import predecir_bp
from flask import Flask
from flask_cors import CORS

from inicio    import inicio_bp
from tablas    import tablas_bp
from visualizar import visualizar_bp
from predecir  import predecir_bp

app = Flask(__name__)
CORS(app)

app.register_blueprint(inicio_bp)
app.register_blueprint(tablas_bp)
app.register_blueprint(visualizar_bp)
app.register_blueprint(predecir_bp)

if __name__ == '__main__':
    app.run(debug=True, port=5000, use_reloader=False)