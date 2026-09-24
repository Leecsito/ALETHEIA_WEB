"""
ALETHEIA — Predictor Avanzado (proxy HTTP)

Este módulo NO contiene lógica de predicción. Actúa como proxy hacia el
servicio externo ALETHEIA_PREDICT (motor Glicko-2 + regresión logística +
Monte Carlo), cuya URL se toma de la variable de entorno ALETHEIA_PREDICT_URL.

Endpoints expuestos (proxy):
    GET  /api/aletheia/equipos         -> GET  {BASE}/api/equipos
    GET  /api/aletheia/mapas           -> GET  {BASE}/api/mapas
    POST /api/aletheia/predecir        -> POST {BASE}/api/predecir
    GET  /api/aletheia/modelo_version  -> GET  {BASE}/api/modelo_version
    POST /api/aletheia/precalcular     -> POST {BASE}/api/precalcular
    POST /api/aletheia/asociar         -> POST {BASE}/api/asociar
    GET  /api/aletheia/prediccion      -> GET  {BASE}/api/prediccion
    GET  /api/aletheia/predicciones    -> GET  {BASE}/api/predicciones
    GET  /api/aletheia/comparacion     -> GET  {BASE}/api/comparacion
    GET  /api/aletheia/scorecard       -> GET  {BASE}/api/scorecard
    GET  /api/aletheia/scorecard_agregado -> GET {BASE}/api/scorecard_agregado
    GET  /api/aletheia/dataset         -> GET  {BASE}/api/dataset
    GET  /api/aletheia/simulaciones    -> GET  {BASE}/api/simulaciones
    POST /api/aletheia/serie           -> POST {BASE}/api/serie
    POST /api/aletheia/borrar          -> POST {BASE}/api/borrar

Decisiones de diseño:
    · El servicio externo devuelve solo NOMBRES de equipo. Para no romper el
      grid del frontend, /equipos adapta la respuesta a
      {"ok": true, "teams": [{"name", "abbrev": name, "maps_played": 0,
      "avg_rating": 0}]} (abbrev = nombre; el servicio no aporta abreviaturas
      ni estadísticas de mapa/rating).
    · Los endpoints de lectura reenvían los query params recibidos (match_id,
      map_name, lado_inicial_a, equipo_a, equipo_b, limite, ...).
    · Si el servicio no responde se devuelve 502 {"ok": false, "error": ...};
      si tarda más de TIMEOUT segundos, 504.

NOTA: las corridas largas (precalcular / predecir con 25K-50K) se piden
DIRECTO desde el frontend a PREDICT_DIRECTO (ngrok), no por este proxy, para
no chocar con el timeout de gunicorn.
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
def _request_service(method, path, payload=None, params=None):
    """Llama al servicio externo y devuelve (respuesta, None) o (None, error).

    `error` es un dict {'error': mensaje, 'status': código}. El llamador lo
    convierte en respuesta JSON con `_error_response`.
    """
    url = f"{BASE_URL}{path}"
    # El header de ngrok es inocuo para URLs locales y evita la página de aviso
    # si ALETHEIA_PREDICT_URL apunta al túnel de ngrok en producción.
    kwargs = {'timeout': TIMEOUT, 'headers': {'ngrok-skip-browser-warning': '1'}}
    if payload is not None:
        kwargs['json'] = payload
    if params:
        kwargs['params'] = params
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


def _passthrough_get(service_path):
    """GET al servicio reenviando los query params; devuelve la respuesta tal cual."""
    resp, err = _request_service('GET', service_path, params=request.args.to_dict())
    if err:
        return _error_response(err)
    data, err = _json_or_error(resp)
    if err:
        return _error_response(err)
    return jsonify(data), resp.status_code


def _passthrough_post(service_path):
    """POST al servicio reenviando el body JSON; devuelve la respuesta tal cual."""
    body = request.get_json(silent=True)
    if body is None:
        return jsonify({'ok': False, 'error': 'Body JSON inválido o vacío.'}), 400
    resp, err = _request_service('POST', service_path, payload=body)
    if err:
        return _error_response(err)
    data, err = _json_or_error(resp)
    if err:
        return _error_response(err)
    return jsonify(data), resp.status_code


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
    return _passthrough_get('/api/mapas')


@aletheia_bp.route('/api/aletheia/predecir', methods=['POST'])
def predecir():
    """Proxy POST /api/predecir. Reenvía el body y devuelve la respuesta tal cual."""
    return _passthrough_post('/api/predecir')


@aletheia_bp.route('/api/aletheia/modelo_version', methods=['GET'])
def modelo_version():
    """Proxy GET /api/modelo_version (hash + fecha del modelo vigente)."""
    return _passthrough_get('/api/modelo_version')


@aletheia_bp.route('/api/aletheia/precalcular', methods=['POST'])
def precalcular():
    """Proxy POST /api/precalcular (precomputa 13 mapas x 2 lados = 26 filas)."""
    return _passthrough_post('/api/precalcular')


@aletheia_bp.route('/api/aletheia/asociar', methods=['POST'])
def asociar():
    """Proxy POST /api/asociar (reasigna el match_id de las predicciones)."""
    return _passthrough_post('/api/asociar')


@aletheia_bp.route('/api/aletheia/prediccion', methods=['GET'])
def prediccion():
    """Proxy GET /api/prediccion (lee una fila cacheada; instantáneo)."""
    return _passthrough_get('/api/prediccion')


@aletheia_bp.route('/api/aletheia/predicciones', methods=['GET'])
def predicciones():
    """Proxy GET /api/predicciones (todas las filas de un partido/equipos)."""
    return _passthrough_get('/api/predicciones')


@aletheia_bp.route('/api/aletheia/comparacion', methods=['GET'])
def comparacion():
    """Proxy GET /api/comparacion (predicho vs. resultado real)."""
    return _passthrough_get('/api/comparacion')


@aletheia_bp.route('/api/aletheia/scorecard', methods=['GET'])
def scorecard():
    """Proxy GET /api/scorecard (micro-eventos: economía, OT, marcador vs real)."""
    return _passthrough_get('/api/scorecard')


@aletheia_bp.route('/api/aletheia/scorecard_agregado', methods=['GET'])
def scorecard_agregado():
    """Proxy GET /api/scorecard_agregado (scorecard sumando todos los partidos)."""
    return _passthrough_get('/api/scorecard_agregado')


@aletheia_bp.route('/api/aletheia/dataset', methods=['GET'])
def dataset():
    """Proxy GET /api/dataset (dataset predicción↔resultado para reentrenar)."""
    return _passthrough_get('/api/dataset')


@aletheia_bp.route('/api/aletheia/simulaciones', methods=['GET'])
def simulaciones():
    """Proxy GET /api/simulaciones (enfrentamientos ya preparados)."""
    return _passthrough_get('/api/simulaciones')


@aletheia_bp.route('/api/aletheia/serie', methods=['POST'])
def serie():
    """Proxy POST /api/serie (probabilidad de serie desde caché, sin Monte Carlo)."""
    return _passthrough_post('/api/serie')


@aletheia_bp.route('/api/aletheia/borrar', methods=['POST'])
def borrar():
    """Proxy POST /api/borrar (borra las predicciones de un enfrentamiento)."""
    return _passthrough_post('/api/borrar')
