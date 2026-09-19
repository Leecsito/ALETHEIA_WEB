"""
ALETHEIA — Predictor Avanzado (proxy HTTP)

Este módulo NO contiene lógica de predicción. Actúa como proxy hacia el
servicio externo ALETHEIA_PREDICT (motor Glicko-2 + regresión logística +
Monte Carlo), cuya URL se toma de la variable de entorno ALETHEIA_PREDICT_URL.

Endpoints expuestos (proxy):
    GET  /api/aletheia/equipos   -> GET  {BASE}/api/equipos
    GET  /api/aletheia/mapas     -> GET  {BASE}/api/mapas
    POST /api/aletheia/predecir  -> POST {BASE}/api/predecir  (reenvía el body)

Decisiones de diseño:
    · El servicio externo devuelve solo NOMBRES de equipo. Para no romper el
      grid del frontend, /equipos adapta la respuesta a
      {"ok": true, "teams": [{"name", "abbrev": name, "maps_played": 0,
      "avg_rating": 0}]} (abbrev = nombre; el servicio no aporta abreviaturas
      ni estadísticas de mapa/rating).
    · Si el servicio no responde se devuelve 502 {"ok": false, "error": ...};
      si tarda más de TIMEOUT segundos, 504.
"""

import os
from flask import Blueprint, jsonify, request
import requests

aletheia_bp = Blueprint('aletheia', __name__)


# ─── CONFIGURACIÓN / ENTORNO ──────────────────────────────────────────────────
def _load_dotenv():
    """Carga un archivo .env de la raíz del proyecto sin dependencias extra.

    Solo define variables que aún no estén presentes en os.environ.
    Formato soportado: líneas `CLAVE=valor`, comentarios con `#`.
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(root, '.env')
    if not os.path.isfile(env_path):
        return
    try:
        with open(env_path, 'r', encoding='utf-8') as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, _, value = line.partition('=')
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        pass


_load_dotenv()

# Local dev por defecto: servicio ALETHEIA_PREDICT en el puerto 8000.
BASE_URL = os.environ.get('ALETHEIA_PREDICT_URL', 'http://localhost:8000').rstrip('/')
TIMEOUT = 120  # segundos


# ─── HELPERS DE PROXY ─────────────────────────────────────────────────────────
def _request_service(method, path, payload=None):
    """Llama al servicio externo y devuelve (respuesta, None) o (None, error).

    `error` es un dict {'error': mensaje, 'status': código}. El llamador lo
    convierte en respuesta JSON con `_error_response`.
    """
    url = f"{BASE_URL}{path}"
    kwargs = {'timeout': TIMEOUT}
    if payload is not None:
        kwargs['json'] = payload
    try:
        resp = requests.request(method, url, **kwargs)
    except requests.exceptions.Timeout:
        return None, {
            'error': f'Tiempo de espera agotado ({TIMEOUT}s) contactando ALETHEIA_PREDICT.',
            'status': 504,
        }
    except requests.exceptions.RequestException as exc:
        return None, {
            'error': f'Servicio de predicción no disponible: {exc}',
            'status': 502,
        }
    return resp, None


def _json_or_error(resp):
    try:
        return resp.json(), None
    except ValueError:
        return None, {
            'error': 'El servicio de predicción devolvió una respuesta no válida (JSON inválido).',
            'status': 502,
        }


def _error_response(err):
    return jsonify({'ok': False, 'error': err['error']}), err['status']


# ─── RUTAS (PROXY) ────────────────────────────────────────────────────────────
@aletheia_bp.route('/api/aletheia/equipos', methods=['GET'])
def equipos():
    """Proxy GET /api/equipos, adaptado al formato del grid del frontend."""
    resp, err = _request_service('GET', '/api/equipos')
    if err:
        return _error_response(err)
    if resp.status_code >= 400:
        data, _ = _json_or_error(resp)
        return jsonify(data if isinstance(data, dict) else {'ok': False, 'error': f'Error {resp.status_code} del servicio.'}), resp.status_code

    data, err = _json_or_error(resp)
    if err:
        return _error_response(err)

    nombres = data.get('equipos', []) if isinstance(data, dict) else []
    teams = [
        {'name': n, 'abbrev': n, 'maps_played': 0, 'avg_rating': 0}
        for n in nombres if n
    ]
    return jsonify({'ok': True, 'teams': teams})


@aletheia_bp.route('/api/aletheia/mapas', methods=['GET'])
def mapas():
    """Proxy GET /api/mapas (devuelve la lista tal cual, incluido 'Summit')."""
    resp, err = _request_service('GET', '/api/mapas')
    if err:
        return _error_response(err)
    data, err = _json_or_error(resp)
    if err:
        return _error_response(err)
    return jsonify(data), resp.status_code


@aletheia_bp.route('/api/aletheia/predecir', methods=['POST'])
def predecir():
    """Proxy POST /api/predecir. Reenvía el body y devuelve la respuesta tal cual."""
    body = request.get_json(silent=True)
    if body is None:
        return jsonify({'ok': False, 'error': 'Body JSON inválido o vacío.'}), 400

    resp, err = _request_service('POST', '/api/predecir', body)
    if err:
        return _error_response(err)
    data, err = _json_or_error(resp)
    if err:
        return _error_response(err)
    return jsonify(data), resp.status_code
