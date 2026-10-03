"""
ALETHEIA — Predictor Avanzado (proxy HTTP)

Este módulo NO contiene lógica de predicción. Actúa como proxy hacia el
servicio externo ALETHEIA_PREDICT (motor Glicko-2 + regresión logística +
Monte Carlo). Hay DOS servicios:

    · READ_URL  = ALETHEIA_PREDICT_READ_URL (Render, siempre disponible). Es
      cache-first: responde de Turso sin cargar el motor. Si no se define la
      variable, se usa BASE_URL (comportamiento anterior).
    · BASE_URL  = ALETHEIA_PREDICT_URL (PC/ngrok). Cómputo pesado y mutaciones.

Enrutado:
    Lecturas (READ_URL, con reintento por cold start y caída a BASE_URL):
    GET  /api/aletheia/equipos         -> GET  {READ}/api/equipos
    GET  /api/aletheia/mapas           -> GET  {READ}/api/mapas
    GET  /api/aletheia/modelo_version  -> GET  {READ}/api/modelo_version
    GET  /api/aletheia/prediccion      -> GET  {READ}/api/prediccion
    GET  /api/aletheia/predicciones    -> GET  {READ}/api/predicciones
    GET  /api/aletheia/comparacion     -> GET  {READ}/api/comparacion
    GET  /api/aletheia/scorecard       -> GET  {READ}/api/scorecard
    GET  /api/aletheia/scorecard_agregado -> GET {READ}/api/scorecard_agregado
    GET  /api/aletheia/simulaciones    -> GET  {READ}/api/simulaciones
    GET  /api/aletheia/dataset         -> GET  {READ}/api/dataset (sin guardar)
    POST /api/aletheia/predecir        -> POST {READ}/api/predecir (sin forzar)
    POST /api/aletheia/serie           -> POST {READ}/api/serie (sin forzar)

    Cómputo/mutaciones (BASE_URL/ngrok, sin fallback):
    POST /api/aletheia/precalcular     -> POST {BASE}/api/precalcular
    GET  /api/aletheia/precalcular/estado -> GET {BASE}/api/precalcular/estado
    POST /api/aletheia/asociar         -> POST {BASE}/api/asociar
    POST /api/aletheia/borrar          -> POST {BASE}/api/borrar
    GET  /api/aletheia/dataset?guardar=1 -> GET {BASE}/api/dataset?guardar=1
    POST /api/aletheia/predecir|serie con forzar:true -> {BASE}

Decisiones de diseño:
    · El servicio externo devuelve solo NOMBRES de equipo. Para no romper el
      grid del frontend, /equipos adapta la respuesta a
      {"ok": true, "teams": [{"name", "abbrev": name, "maps_played": 0,
      "avg_rating": 0}]} (abbrev = nombre; el servicio no aporta abreviaturas
      ni estadísticas de mapa/rating).
    · Los endpoints de lectura reenvían los query params recibidos (match_id,
      map_name, lado_inicial_a, equipo_a, equipo_b, limite, ...).
    · Si el servicio no responde se devuelve 502 {"ok": false, "error": ...};
      si se agota el timeout del intento, 504.

Clave API: si `ALETHEIA_API_KEY` está configurada en el entorno, este proxy
añade **server-side** el header `X-API-Key` a todas las llamadas al servicio
(los endpoints públicos la ignoran). La clave NUNCA viaja al navegador.
`POST /api/precalcular` responde 202 al instante (async con `job_id`), así que
también puede pedirse por el proxy sin chocar con su timeout.
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
BASE_URL = os.environ.get('ALETHEIA_PREDICT_URL', 'http://localhost:8000').strip().rstrip('/')
# Servicio de lectura en Render (siempre disponible, cache-first). Si no se
# define, las lecturas siguen yendo a BASE_URL (comportamiento anterior).
READ_URL = os.environ.get('ALETHEIA_PREDICT_READ_URL', '').strip().rstrip('/') or BASE_URL

TIMEOUT = 120             # segundos: ngrok/PC y operaciones de cómputo
READ_TIMEOUT = 60         # primer intento de lectura (en caliente responde <5 s)
READ_RETRY_TIMEOUT = 180  # reintento único: cubre el cold start de Render free


# ─── HELPERS DE PROXY ─────────────────────────────────────────────────────────
def _es_forzar(payload):
    """True si el body pide forzar el recálculo (bool o string tipo 'true')."""
    valor = (payload or {}).get('forzar')
    if isinstance(valor, str):
        return valor.strip().lower() in ('1', 'true', 'si', 'sí', 'yes', 'on')
    return bool(valor)


def _guardar_dataset():
    """True si /api/dataset?guardar=1 (persiste en el PC; no en Render)."""
    valor = request.args.get('guardar')
    if valor is None:
        return False
    return str(valor).strip().lower() in ('1', 'true', 'si', 'sí', 'yes', 'on')


def _intentos_servicio(prefer):
    """Parejas (url, timeout) a probar, en orden, según el tipo de llamada.

    'read'  -> READ_URL (Render); reintenta una vez por cold start y, como
               último recurso, cae a BASE_URL (PC/ngrok) si está definido.
    'ngrok' -> BASE_URL (cómputo pesado y mutaciones); sin fallback.
    """
    if prefer != 'read' or READ_URL == BASE_URL:
        return [(BASE_URL, TIMEOUT)]
    return [
        (READ_URL, READ_TIMEOUT),
        (READ_URL, READ_RETRY_TIMEOUT),
        (BASE_URL, TIMEOUT),
    ]


def _request_service(method, path, payload=None, params=None, prefer='read'):
    """Llama al servicio externo y devuelve (respuesta, None) o (None, error).

    `prefer` decide el servicio: 'read' usa el servicio de lectura (Render,
    cache-first) con reintento por cold start y caída a ngrok/PC; 'ngrok' va
    directo al PC (cómputo pesado y mutaciones).

    `error` es un dict {'error': mensaje, 'status': código}. El llamador lo
    convierte en respuesta JSON con `_error_response`.
    """
    # El header de ngrok es inocuo para URLs locales y evita la página de aviso
    # si la URL apunta al túnel de ngrok. La clave API se añade server-side
    # (nunca sale al navegador); los endpoints públicos del servicio la ignoran.
    headers = {'ngrok-skip-browser-warning': '1'}
    api_key = os.environ.get('ALETHEIA_API_KEY', '').strip()
    if api_key:
        headers['X-API-Key'] = api_key

    intentos = _intentos_servicio(prefer)
    ultimo_error = {'error': 'Servicio de predicción no disponible.', 'status': 502}
    for indice, (url, timeout) in enumerate(intentos):
        kwargs = {'timeout': timeout, 'headers': headers}
        if payload is not None:
            kwargs['json'] = payload
        if params:
            kwargs['params'] = params
        try:
            resp = requests.request(method, f"{url}{path}", **kwargs)
        except requests.exceptions.Timeout:
            ultimo_error = {
                'error': f'Tiempo de espera agotado ({timeout}s) contactando ALETHEIA_PREDICT.',
                'status': 504,
            }
            continue
        except requests.exceptions.RequestException as exc:
            ultimo_error = {
                'error': f'Servicio de predicción no disponible: {exc}',
                'status': 502,
            }
            continue
        # Un 5xx del servicio de lectura (p. ej. cold start roto) no descarta
        # al PC: se prueba el siguiente candidato antes de rendirse.
        if resp.status_code >= 500 and indice < len(intentos) - 1:
            ultimo_error = {
                'error': f'El servicio de lectura devolvió HTTP {resp.status_code}.',
                'status': resp.status_code,
            }
            continue
        return resp, None
    return None, ultimo_error


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


def _passthrough_get(service_path, prefer='read'):
    """GET al servicio reenviando los query params; devuelve la respuesta tal cual."""
    resp, err = _request_service('GET', service_path, params=request.args.to_dict(), prefer=prefer)
    if err:
        return _error_response(err)
    data, err = _json_or_error(resp)
    if err:
        return _error_response(err)
    return jsonify(data), resp.status_code


def _passthrough_post(service_path, prefer='read'):
    """POST al servicio reenviando el body JSON; devuelve la respuesta tal cual."""
    body = request.get_json(silent=True)
    if body is None:
        return jsonify({'ok': False, 'error': 'Body JSON inválido o vacío.'}), 400
    resp, err = _request_service('POST', service_path, payload=body, prefer=prefer)
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
    """Proxy POST /api/predecir (cache-aware en READ_URL; forzar:true en ngrok)."""
    body = request.get_json(silent=True) or {}
    prefer = 'ngrok' if _es_forzar(body) else 'read'
    return _passthrough_post('/api/predecir', prefer=prefer)


@aletheia_bp.route('/api/aletheia/modelo_version', methods=['GET'])
def modelo_version():
    """Proxy GET /api/modelo_version (hash + fecha del modelo vigente)."""
    return _passthrough_get('/api/modelo_version')


@aletheia_bp.route('/api/aletheia/precalcular', methods=['POST'])
def precalcular():
    """Proxy POST /api/precalcular (precomputa 13 mapas x 2 lados = 26 filas).

    Cómputo pesado: va siempre al PC/ngrok (BASE_URL), no al servicio de
    lectura de Render.
    """
    return _passthrough_post('/api/precalcular', prefer='ngrok')


@aletheia_bp.route('/api/aletheia/precalcular/estado', methods=['GET'])
def precalcular_estado():
    """Proxy GET /api/precalcular/estado (progreso de un job de precálculo).

    El job vive en el PC que lo lanzó, así que el polling también va a
    BASE_URL. El POST ya responde 202 al instante y cada sondeo es rápido.
    """
    return _passthrough_get('/api/precalcular/estado', prefer='ngrok')


@aletheia_bp.route('/api/aletheia/asociar', methods=['POST'])
def asociar():
    """Proxy POST /api/asociar (reasigna el match_id de las predicciones)."""
    return _passthrough_post('/api/asociar', prefer='ngrok')


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
    """Proxy GET /api/dataset (dataset predicción↔resultado para reentrenar).

    Sin `guardar` es una lectura más (READ_URL); con `guardar=1` persiste el
    CSV en el PC del servicio, así que va a BASE_URL.
    """
    prefer = 'ngrok' if _guardar_dataset() else 'read'
    return _passthrough_get('/api/dataset', prefer=prefer)


@aletheia_bp.route('/api/aletheia/simulaciones', methods=['GET'])
def simulaciones():
    """Proxy GET /api/simulaciones (enfrentamientos ya preparados)."""
    return _passthrough_get('/api/simulaciones')


@aletheia_bp.route('/api/aletheia/serie', methods=['POST'])
def serie():
    """Proxy POST /api/serie (probabilidad de serie cache-aware).

    Reutiliza las filas de caché vigentes y **calcula y persiste** en la DB lo
    que falte (mapa y serie); una segunda llamada idéntica sale de caché
    (`fuente` por mapa). Devuelve además `confianza_serie`, `modelo_version`,
    `n_sim`, `resultados_serie`, `caminos_serie` y `prob_intervalo` por mapa.

    Sin `forzar` va al servicio de lectura de Render (cache-first); con
    `forzar:true` va al PC/ngrok para recalcular.
    """
    body = request.get_json(silent=True) or {}
    prefer = 'ngrok' if _es_forzar(body) else 'read'
    return _passthrough_post('/api/serie', prefer=prefer)


@aletheia_bp.route('/api/aletheia/borrar', methods=['POST'])
def borrar():
    """Proxy POST /api/borrar (borra las predicciones de un enfrentamiento)."""
    return _passthrough_post('/api/borrar', prefer='ngrok')
