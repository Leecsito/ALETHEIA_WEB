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
    Lecturas cache-first: 1º Turso (`predicciones_mapa`/`predicciones_serie`); si
    no hay fila, READ_URL con reintento por cold start y caída a BASE_URL:
    GET  /api/aletheia/mapas           -> Turso | GET {READ}/api/mapas
    GET  /api/aletheia/modelo_version  -> Turso | GET {READ}/api/modelo_version
    GET  /api/aletheia/prediccion      -> Turso | GET {READ}/api/prediccion
    GET  /api/aletheia/predicciones    -> Turso | GET {READ}/api/predicciones
    GET  /api/aletheia/simulaciones    -> Turso | GET {READ}/api/simulaciones
    POST /api/aletheia/serie           -> Turso (si la fila coincide) | {READ}/api/serie

    Lecturas que el servicio deriva (siempre servicio, READ_URL → BASE_URL):
    GET  /api/aletheia/equipos         -> GET  {READ}/api/equipos
    GET  /api/aletheia/comparacion     -> GET  {READ}/api/comparacion
    GET  /api/aletheia/scorecard       -> GET  {READ}/api/scorecard
    GET  /api/aletheia/scorecard_agregado -> GET {READ}/api/scorecard_agregado
    GET  /api/aletheia/dataset         -> GET  {READ}/api/dataset (sin guardar)
    POST /api/aletheia/predecir        -> POST {READ}/api/predecir (sin forzar)

    Cómputo/mutaciones (BASE_URL/ngrok, sin fallback):
    POST /api/aletheia/precalcular     -> POST {BASE}/api/precalcular
    GET  /api/aletheia/precalcular/estado -> GET {BASE}/api/precalcular/estado
    POST /api/aletheia/asociar         -> POST {BASE}/api/asociar
    POST /api/aletheia/borrar          -> POST {BASE}/api/borrar
    GET  /api/aletheia/dataset?guardar=1 -> GET {BASE}/api/dataset?guardar=1
    POST /api/aletheia/predecir|serie con forzar:true -> {BASE}

    Capas L2/L3 (cargan el motor completo; BASE_URL/ngrok, sin fallback):
    POST /api/aletheia/recomendar      -> POST {BASE}/api/recomendar (L2: mapa/ban/lado)
    GET  /api/aletheia/perfil          -> GET  {BASE}/api/perfil (L3: micro-económico PIT)

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

Lectura directa de Turso (sin servidor): las predicciones ya preparadas se
sirven **cache-first** desde `predicciones_mapa`/`predicciones_serie` con
`backend.conexion.fetch_all`. Aplica a `mapas`, `modelo_version`,
`simulaciones`, `prediccion`, `predicciones` y `serie` (si hay una fila
cacheada con los mismos mapas/lados y el veto está completo: 1/3/5 mapas; con
2/4 se cae al proxy). El servicio solo aporta lo no persistido
(`prob_intervalo`, `comparacion`/`scorecard`, `prob_motor_*`) y el cómputo
(`precalcular`/`forzar`/`equipos`).

Contrato vigente de `predicciones_mapa` (2026-10): `prob_victoria_a` es la
capa **ESC por (mapa, lado)** — la predicción que se sirve y se cachea — y
`esc_peso` (0–1) es su peso/confianza (`n_min/(n_min+10)`; `null` si la capa
está apagada). La P del motor Glicko **no se persiste**: se expone una sola
vez por enfrentamiento en la raíz de `POST /predecir` y `POST /serie`
(`prob_motor_a`/`prob_motor_b`) y decide la serie (`prob_serie_a/b`). En modo
DB `prob_intervalo` y `escenario_mapa` quedan `null` (el escenario se
recalcula en el servicio; la UI usa `prob_victoria_a` + `esc_peso` y oculta
la confianza sin romper).

Campos aditivos del motor por capas (2026-10-07, F1–F4): `mapas[].capa`
(`"esc"`|`"motor"`) y `capa_serie` (`"motor_glicko"`) en las respuestas del
servicio; `predicciones_serie.recomendacion_json` (recomendación L2
persistida, best-effort). En modo DB `capa` se deriva de `esc_peso` y
`capa_serie` es siempre `"motor_glicko"` (la P del motor no se persiste);
`recomendacion` se expone parseada si la columna existe. Los endpoints L2/L3
(`POST /api/aletheia/recomendar` y `GET /api/aletheia/perfil`) se enrutan
siempre a BASE_URL: cargan el motor completo (1–2 min en frío) y no son
cache-first ni tienen lectura directa a Turso.

Clave API: si `ALETHEIA_API_KEY` está configurada en el entorno, este proxy
añade **server-side** el header `X-API-Key` a todas las llamadas al servicio
(los endpoints públicos la ignoran). La clave NUNCA viaja al navegador.
`POST /api/precalcular` responde 202 al instante (async con `job_id`), así que
también puede pedirse por el proxy sin chocar con su timeout; si el motor está
en frío, el proxy espera hasta 180 s (`TIMEOUT_PRECALCULAR`) para no cortar el
arranque. El 429 "ya hay un precálculo en curso" se reenvía con su cuerpo.
"""

import json
import math
import os
import time

from flask import Blueprint, jsonify, request
import requests

from backend.cache import ttl_cache
from backend.conexion import fetch_all

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
TIMEOUT_PRECALCULAR = 180  # el POST de precalcular puede tardar (motor en frío)
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


def _intentos_servicio(prefer, timeout=None):
    """Parejas (url, timeout) a probar, en orden, según el tipo de llamada.

    'read'  -> READ_URL (Render); reintenta una vez por cold start y, como
               último recurso, cae a BASE_URL (PC/ngrok) si está definido.
    'ngrok' -> BASE_URL (cómputo pesado y mutaciones); sin fallback.
    `timeout` sustituye al TIMEOUT por defecto en las llamadas a BASE_URL
    (p. ej. el POST de precalcular, que puede tardar en cargar el motor).
    """
    if prefer != 'read' or READ_URL == BASE_URL:
        return [(BASE_URL, timeout or TIMEOUT)]
    return [
        (READ_URL, READ_TIMEOUT),
        (READ_URL, READ_RETRY_TIMEOUT),
        (BASE_URL, TIMEOUT),
    ]


def _request_service(method, path, payload=None, params=None, prefer='read',
                     timeout=None):
    """Llama al servicio externo y devuelve (respuesta, None) o (None, error).

    `prefer` decide el servicio: 'read' usa el servicio de lectura (Render,
    cache-first) con reintento por cold start y caída a ngrok/PC; 'ngrok' va
    directo al PC (cómputo pesado y mutaciones). `timeout` solo aplica a las
    llamadas directas a BASE_URL (precalcular usa 180 s por el arranque del
    motor; sobre el servicio de lectura se conservan los tiempos estándar).

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

    intentos = _intentos_servicio(prefer, timeout)
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
        # Un 5xx no JSON suele ser la página HTML del túnel caído o del proxy
        # del hosting; se traduce a un mensaje claro para la UI.
        if resp.status_code >= 500:
            return None, {
                'error': f'El servicio de predicción no está disponible (HTTP {resp.status_code}).',
                'status': 502,
            }
        return None, {
            'error': 'El servicio de predicción devolvió una respuesta no válida (JSON inválido).',
            'status': 502,
        }


def _error_response(err):
    return jsonify({'ok': False, 'error': err['error']}), err['status']


# ─── LECTURA DIRECTA DE LA CACHÉ (TURSO, SIN SERVIDOR) ───────────────────────
# Réplica de la parte de lectura del servicio sobre las tablas compartidas. La
# P(mapa) y todo lo persistido salen tal cual; lo derivado (`confianza`,
# `analisis_mapa`) se recalcula aquí con las mismas reglas. El IC95%
# (`prob_intervalo`) usa los RD locales del servicio: en modo DB va `None`.
_BANDA_ALTA = 0.62
_BANDA_MEDIA = 0.55
# El JOIN de `_tabla_mapas_equipo` (2.821 filas, ~0.7 s) solo cambia con un ETL:
# TTL alto (30 min) para no pagarlo en cada worker/visita.
_TABLA_MAPAS_TTL = 1800
_tabla_mapas = {'ts': 0.0, 'data': {}}


def _banda_confianza(prob):
    """Misma banda que `core.calibracion.banda_confianza` (sin n/rd)."""
    p = max(float(prob), 1.0 - float(prob))
    if p >= _BANDA_ALTA:
        return 'alta'
    if p >= _BANDA_MEDIA:
        return 'media'
    return 'baja'


def _formato_de_serie(n_mapas):
    """Bo1/Bo3/Bo5 del **veto completo** (1/3/5); `(None, None)` si no aplica.

    Alineado con el contrato de `/api/serie` (Plan C): 2/4 mapas no son un
    veto completo, no tienen formato válido y el servicio responde 400.
    """
    if n_mapas == 1:
        return 'bo1', 1
    if n_mapas == 3:
        return 'bo3', 2
    if n_mapas == 5:
        return 'bo5', 3
    return None, None


def _parse_json(txt):
    if txt is None:
        return None
    try:
        return json.loads(txt)
    except (TypeError, ValueError):
        return None


def _logit_recortado(p, eps=1e-4):
    p = min(max(float(p), eps), 1.0 - eps)
    return math.log(p / (1.0 - p))


def _p_mapa_analitica(p_modelo, wr_a, wr_b, peso=0.5):
    """Réplica de `core.analisis.probabilidad_mapa_analitica` (análisis)."""
    edge = max(-2.0, min(2.0, _logit_recortado(wr_a) - _logit_recortado(wr_b)))
    z = _logit_recortado(p_modelo) + peso * edge
    return 1.0 / (1.0 + math.exp(-z))


def _tabla_mapas_equipo():
    """{equipo: {mapa: {n, winrate}}} desde `maps`+`matches`+`teams` (Turso).

    Réplica de `core.analisis.tabla_mapas_equipo` (shrinkage k=10) para el
    `analisis_mapa`; se cachea en memoria 30 min (`_TABLA_MAPAS_TTL`). Ante un
    fallo devuelve la copia previa (o `{}`): el análisis es decorativo, no
    tumba la lectura.
    """
    ahora = time.time()
    if _tabla_mapas['data'] and (ahora - _tabla_mapas['ts']) < _TABLA_MAPAS_TTL:
        return _tabla_mapas['data']
    try:
        filas = fetch_all(
            """
            SELECT ta.team_name AS team_a, tb.team_name AS team_b, m.map_name,
                   (m.score_a_attack + m.score_a_defense) AS sa,
                   (m.score_b_attack + m.score_b_defense) AS sb
            FROM maps m
            JOIN matches ma ON m.match_id = ma.match_id
            JOIN teams ta ON ma.team_a_id = ta.team_id
            JOIN teams tb ON ma.team_b_id = tb.team_id
            """
        )
    except Exception:  # noqa: BLE001 - la lectura cache-first no depende de esto
        return _tabla_mapas['data']
    k = 10.0
    acumulado = {}
    for fila in filas:
        sa = fila.get('sa') or 0
        sb = fila.get('sb') or 0
        if sa == sb:
            continue
        for equipo, gano in ((fila.get('team_a'), sa > sb),
                             (fila.get('team_b'), sb > sa)):
            if not equipo:
                continue
            clave = (equipo, fila.get('map_name'))
            wins, n = acumulado.get(clave, (0, 0))
            acumulado[clave] = (wins + (1 if gano else 0), n + 1)
    tabla = {}
    for (equipo, mapa), (wins, n) in acumulado.items():
        tabla.setdefault(equipo, {})[mapa] = {
            'n': n,
            'winrate': (wins + k * 0.5) / (n + k),
        }
    _tabla_mapas['data'] = tabla
    _tabla_mapas['ts'] = ahora
    return tabla


def _analisis_mapa_local(tabla, equipo_a, equipo_b, map_name, p_modelo):
    da = (tabla.get(equipo_a) or {}).get(map_name) or {'n': 0, 'winrate': 0.5}
    db = (tabla.get(equipo_b) or {}).get(map_name) or {'n': 0, 'winrate': 0.5}
    p = p_modelo if p_modelo is not None else 0.5
    return {
        'equipo_a': {'n': da['n'], 'winrate': round(da['winrate'], 4)},
        'equipo_b': {'n': db['n'], 'winrate': round(db['winrate'], 4)},
        'p_mapa_a': round(_p_mapa_analitica(p, da['winrate'], db['winrate']), 4),
        'nota': 'análisis histórico por mapa · no es la predicción del motor',
    }


def _derivar_fila(fila, tabla):
    """Completa una fila de `predicciones_mapa` como lo hace el servicio.

    Contrato vigente: `prob_victoria_a` es la capa **ESC por (mapa, lado)** — la
    predicción que se sirve — y `esc_peso` su peso/confianza (`null` si la capa
    está apagada). En modo DB `prob_intervalo` y `escenario_mapa` quedan `null`
    (la capa de escenarios se recalcula en el servicio); la UI usa
    `prob_victoria_a` + `esc_peso` y oculta la confianza sin romper. La P del
    motor no se persiste (solo viaja en la raíz de `POST /predecir` y
    `POST /serie`). `analisis_mapa` es análisis histórico, no la predicción.
    """
    fila = dict(fila)
    fila['marcadores'] = _parse_json(fila.get('marcadores_json'))
    fila['economia'] = _parse_json(fila.get('economia_json'))
    p = fila.get('prob_victoria_a')
    fila['confianza'] = _banda_confianza(p) if p is not None else None
    fila['prob_intervalo'] = None  # el IC95% usa los RD locales del servicio
    esc = fila.get('escenario_mapa')
    if isinstance(esc, str):
        esc = _parse_json(esc)
    fila['escenario_mapa'] = esc if isinstance(esc, dict) else None
    peso = fila.get('esc_peso')
    if peso is None and isinstance(fila['escenario_mapa'], dict):
        peso = fila['escenario_mapa'].get('peso')
    fila['esc_peso'] = peso
    # F1 (aditivo): capa que sirve `prob_victoria_a`. En modo DB no se persiste:
    # se deriva de `esc_peso` (null = capa ESC apagada, se sirve la P motor).
    fila['capa'] = 'esc' if peso is not None else 'motor'
    fila['analisis_mapa'] = _analisis_mapa_local(
        tabla, fila.get('equipo_a'), fila.get('equipo_b'), fila.get('map_name'), p)
    return fila


def _map_pool_db():
    """Nombres de mapa activos del pool (`map_pool.en_pool=1`), o None.

    La tabla `map_pool` la gestiona la web (no el servicio). Si la tabla no
    existe (DB vieja) se devuelve `None` para degradar sin romper: el llamador
    mantiene el comportamiento anterior (todos los mapas con predicción).
    """
    try:
        existe = fetch_all(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'map_pool' LIMIT 1")
    except Exception:  # noqa: BLE001 - sin tabla se sigue con todos los mapas
        return None
    if not existe:
        return None
    try:
        filas = fetch_all(
            'SELECT map_name FROM map_pool WHERE en_pool = 1 ORDER BY map_name')
    except Exception:  # noqa: BLE001 - pool ilegible = sin filtro
        return None
    return [str(f.get('map_name')).strip() for f in filas if f.get('map_name')]


@ttl_cache(30)
def _version_vigente_db():
    """(modelo_version, fecha) más reciente de la caché, o (None, None).

    Memo de 30 s (con single-flight de `ttl_cache`): es una consulta de 1-2
    queries por request y solo cambia si entran predicciones nuevas.
    """
    filas = fetch_all(
        'SELECT modelo_version, MAX(updated_at) AS fecha FROM predicciones_mapa '
        'GROUP BY modelo_version ORDER BY fecha DESC LIMIT 1'
    )
    if filas:
        return filas[0].get('modelo_version'), filas[0].get('fecha')
    filas = fetch_all(
        'SELECT modelo_version, MAX(created_at) AS fecha FROM predicciones_serie '
        'GROUP BY modelo_version ORDER BY fecha DESC LIMIT 1'
    )
    if filas:
        return filas[0].get('modelo_version'), filas[0].get('fecha')
    return None, None


# Columnas de `predicciones_mapa` que la web realmente usa (se proyectan en
# explícito en vez de `SELECT *`). `marcadores_json`/`economia_json` SÍ se
# necesitan: alimentan los bloques DISTRIBUCIÓN DE MARCADOR y ECONOMÍA; si el
# servicio añade columnas, estas no viajan sin querer.
_COLS_PREDICCIONES = (
    'id, match_id, equipo_a, equipo_b, equipo_a_id, equipo_b_id, map_name, '
    'lado_inicial_a, prob_victoria_a, prob_victoria_b, prob_overtime, n_sim, '
    'con_datos, modelo_version, marcadores_json, economia_json, esc_peso, '
    'created_at, updated_at'
)


def _predicciones_db(match_id=None, equipo_a=None, equipo_b=None,
                     map_name=None, lado=None):
    """Filas de `predicciones_mapa` con los mismos filtros que el servicio."""
    condiciones, params = [], []
    if match_id is not None:
        condiciones.append('match_id = ?')
        params.append(int(match_id))
    if equipo_a:
        condiciones.append('equipo_a = ?')
        params.append(equipo_a)
    if equipo_b:
        condiciones.append('equipo_b = ?')
        params.append(equipo_b)
    if map_name:
        condiciones.append('lower(map_name) = lower(?)')
        params.append(map_name)
    if lado:
        condiciones.append('lado_inicial_a = ?')
        params.append(lado)
    where = ('WHERE ' + ' AND '.join(condiciones)) if condiciones else ''
    return fetch_all(
        f'SELECT {_COLS_PREDICCIONES} FROM predicciones_mapa {where} '
        'ORDER BY map_name, lado_inicial_a',
        params,
    )


def _serie_desde_db(data):
    """Respuesta de `/api/serie` desde `predicciones_serie`, o None si no hay.

    Solo sirve si existe una fila vigente cuyo `mapas_json` coincide (mapas y
    lados, en orden) con lo pedido: la P de serie depende de esa lista. Si no
    coincide (p. ej. lados elegidos a mano en ARMAR SERIE), la calcula el
    servicio. `forzar:true` nunca sale de aquí.

    Los mapas salen tal cual del `mapas_json` persistido (capa ESC + `esc_peso`);
    `prob_motor_a/b` va `null` porque la P del motor no se persiste (la UI lo
    oculta y usa `prob_serie_a/b` para la serie).
    """
    if _es_forzar(data):
        return None
    mapas = data.get('mapas')
    if not isinstance(mapas, list) or not mapas:
        return None
    # Plan C: la API exige el veto completo (1/3/5). Con 2/4 no hay fila de
    # `predicciones_serie` válida que devolver: se cae al proxy (que a su vez
    # responderá 400 si el servicio está caído).
    if len(mapas) not in (1, 3, 5):
        return None
    equipo_a = str(data.get('equipo_a') or '').strip()
    equipo_b = str(data.get('equipo_b') or '').strip()
    if not (equipo_a and equipo_b):
        return None
    try:
        match_id = int(data.get('match_id') or 0)
    except (TypeError, ValueError):
        match_id = 0
    pedido = []
    for item in mapas:
        if isinstance(item, dict):
            nombre = str(item.get('map_name') or '').strip().lower()
            lado = str(item.get('lado_inicial_a') or 'attack').strip().lower()
        else:
            nombre, lado = str(item or '').strip().lower(), 'attack'
        if not nombre:
            return None
        pedido.append((nombre, lado))

    if match_id:
        condicion, params = 'match_id = ?', [match_id]
    else:
        condicion, params = 'equipo_a = ? AND equipo_b = ?', [equipo_a, equipo_b]
    cols = ('match_id, equipo_a, equipo_b, formato, mapas_json, prob_serie_a, '
            'prob_serie_b, n_sim, modelo_version, created_at')
    try:
        # F4 (aditivo): la recomendación L2 persistida (columna nueva). Si la DB
        # es vieja/local y la columna no existe, se relee sin ella sin romper.
        filas = fetch_all(
            f'SELECT {cols}, recomendacion_json FROM predicciones_serie '
            f'WHERE {condicion} ORDER BY created_at DESC',
            params,
        )
    except Exception:  # noqa: BLE001 - columna ausente en DB vieja o local
        filas = fetch_all(
            f'SELECT {cols} FROM predicciones_serie '
            f'WHERE {condicion} ORDER BY created_at DESC',
            params,
        )
    version, _ = _version_vigente_db()
    for fila in filas:
        if version and str(fila.get('modelo_version')) != version:
            continue
        if str(fila.get('equipo_a') or '').strip().lower() != equipo_a.lower():
            continue
        if str(fila.get('equipo_b') or '').strip().lower() != equipo_b.lower():
            continue
        detalle = _parse_json(fila.get('mapas_json'))
        if not isinstance(detalle, list) or len(detalle) != len(pedido):
            continue
        coincide = all(
            str((d or {}).get('map_name') or '').strip().lower() == nombre
            and str((d or {}).get('lado_inicial_a') or '').strip().lower() == lado
            for d, (nombre, lado) in zip(detalle, pedido)
        )
        if not coincide:
            continue
        # La capa ESC (`prob_victoria_a`) y `esc_peso` ya vienen persistidos en
        # `mapas_json`; `escenario_mapa` puede ser `null` (la UI usa entonces
        # ESC + esc_peso y oculta la confianza). La P del motor no se persiste:
        # la raíz la expone `null` y la UI no la pinta.
        formato, objetivo = _formato_de_serie(len(pedido))
        p_a = float(fila['prob_serie_a'])
        # F1 (aditivo): `capa` por mapa derivada de `esc_peso` (el servicio no
        # persiste `capa`); la raíz siempre decide con el motor Glicko.
        for mapa in detalle:
            if isinstance(mapa, dict):
                mapa.setdefault(
                    'capa', 'esc' if mapa.get('esc_peso') is not None else 'motor')
        return {
            'ok': True,
            'equipo_a': fila.get('equipo_a') or equipo_a,
            'equipo_b': fila.get('equipo_b') or equipo_b,
            'match_id': int(fila.get('match_id') or match_id or 0),
            'n_sim': fila.get('n_sim'),
            'modelo_version': fila.get('modelo_version'),
            'formato': fila.get('formato') or formato,
            'mapas_para_ganar': objetivo,
            'mapas': detalle,
            'capa_serie': 'motor_glicko',
            'recomendacion': _parse_json(fila.get('recomendacion_json')),
            'prob_motor_a': None,
            'prob_motor_b': None,
            'prob_serie_a': round(p_a, 4),
            'prob_serie_b': round(1.0 - p_a, 4),
            'confianza_serie': _banda_confianza(p_a),
        }
    return None


def _passthrough_get(service_path, prefer='read'):
    """GET al servicio reenviando los query params; devuelve la respuesta tal cual."""
    resp, err = _request_service('GET', service_path, params=request.args.to_dict(), prefer=prefer)
    if err:
        return _error_response(err)
    data, err = _json_or_error(resp)
    if err:
        return _error_response(err)
    return jsonify(data), resp.status_code


def _passthrough_post(service_path, prefer='read', timeout=None):
    """POST al servicio reenviando el body JSON; devuelve la respuesta tal cual."""
    body = request.get_json(silent=True)
    if body is None:
        return jsonify({'ok': False, 'error': 'Body JSON inválido o vacío.'}), 400
    resp, err = _request_service('POST', service_path, payload=body,
                                 prefer=prefer, timeout=timeout)
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
    """GET /api/mapas: directo de Turso (los mapas con predicción); si no hay, proxy.

    Con `?pool=1` (EN VIVO) se limita a los mapas del pool activo
    (`map_pool.en_pool=1`); si la tabla `map_pool` no existe, se devuelven todos
    (degradación sin romper). Así el selector/armador de serie nunca ofrece
    mapas fuera del pool.
    """
    solo_pool = str(request.args.get('pool') or '').strip().lower() in (
        '1', 'true', 'si', 'sí', 'yes', 'on')
    try:
        filas = fetch_all(
            'SELECT DISTINCT map_name FROM predicciones_mapa ORDER BY map_name')
        nombres = [f.get('map_name') for f in filas if f.get('map_name')]
        if solo_pool:
            pool = _map_pool_db()
            if pool is not None:
                activos = {m.lower() for m in pool}
                nombres = [m for m in nombres if str(m).strip().lower() in activos]
        if nombres or solo_pool:
            return jsonify({'ok': True, 'mapas': nombres})
    except Exception:  # noqa: BLE001 - cae al servicio
        pass
    return _passthrough_get('/api/mapas')


@aletheia_bp.route('/api/aletheia/predecir', methods=['POST'])
def predecir():
    """Proxy POST /api/predecir (cache-aware en READ_URL; forzar:true en ngrok)."""
    body = request.get_json(silent=True) or {}
    prefer = 'ngrok' if _es_forzar(body) else 'read'
    return _passthrough_post('/api/predecir', prefer=prefer)


@aletheia_bp.route('/api/aletheia/modelo_version', methods=['GET'])
def modelo_version():
    """GET /api/modelo_version: versión vigente de la caché; si no hay, proxy.

    En modo DB, `desactualizado` siempre es `false`: sin servicio no hay una
    versión "en disco" con la que comparar; la vigencia se decide por la
    versión más reciente presente en `predicciones_mapa`.
    """
    try:
        version, fecha = _version_vigente_db()
        if version:
            return jsonify({
                'ok': True,
                'modelo_version': version,
                'fecha': fecha,
                'en_disco': version,
                'desactualizado': False,
            })
    except Exception:  # noqa: BLE001 - cae al servicio
        pass
    return _passthrough_get('/api/modelo_version')


@aletheia_bp.route('/api/aletheia/precalcular', methods=['POST'])
def precalcular():
    """Proxy POST /api/precalcular (precomputa 13 mapas x 2 lados = 26 filas).

    Cómputo pesado: va siempre al PC/ngrok (BASE_URL), no al servicio de
    lectura de Render. El timeout es largo (180 s) porque en frío el servicio
    puede tardar en cargar el motor antes de responder el 202; el cómputo en
    sí es asíncrono (`job_id` + polling de `/precalcular/estado`). Un 429 ("ya
    hay un precálculo en curso") se reenvía tal cual, con su `job_id`/`estado`
    si la API los incluye.
    """
    return _passthrough_post('/api/precalcular', prefer='ngrok',
                             timeout=TIMEOUT_PRECALCULAR)


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
    """GET /api/prediccion: fila cacheada directo de Turso; si no hay, proxy."""
    match_id = (request.args.get('match_id') or '').strip()
    match_id = int(match_id) if match_id.isdigit() else None
    equipo_a = (request.args.get('equipo_a') or '').strip() or None
    equipo_b = (request.args.get('equipo_b') or '').strip() or None
    map_name = (request.args.get('map_name') or '').strip()
    lado = (request.args.get('lado_inicial_a') or '').strip().lower()
    if map_name and lado in ('attack', 'defense') and (
            match_id is not None or (equipo_a and equipo_b)):
        try:
            filas = _predicciones_db(match_id, equipo_a, equipo_b, map_name, lado)
            if filas:
                version, _ = _version_vigente_db()
                fila = _derivar_fila(filas[0], _tabla_mapas_equipo())
                return jsonify({
                    'ok': True,
                    'prediccion': fila,
                    'modelo_version': fila.get('modelo_version'),
                    'vigente': (not version)
                    or str(fila.get('modelo_version')) == str(version),
                })
        except Exception:  # noqa: BLE001 - cae al servicio
            pass
    return _passthrough_get('/api/prediccion')


@aletheia_bp.route('/api/aletheia/predicciones', methods=['GET'])
def predicciones():
    """GET /api/predicciones: filas cacheadas directo de Turso; si no hay, proxy."""
    match_id = (request.args.get('match_id') or '').strip()
    match_id = int(match_id) if match_id.isdigit() else None
    equipo_a = (request.args.get('equipo_a') or '').strip() or None
    equipo_b = (request.args.get('equipo_b') or '').strip() or None
    if match_id is not None or (equipo_a and equipo_b):
        try:
            filas = _predicciones_db(match_id, equipo_a, equipo_b)
            if filas:
                version, _ = _version_vigente_db()
                tabla = _tabla_mapas_equipo()
                return jsonify({
                    'ok': True,
                    'modelo_version': version,
                    'predicciones': [_derivar_fila(f, tabla) for f in filas],
                })
        except Exception:  # noqa: BLE001 - cae al servicio
            pass
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
    """GET /api/simulaciones: enfrentamientos preparados directo de Turso.

    Es una lectura pura de `predicciones_mapa` (la misma que hace el servicio),
    así que no necesita servidor; si la consulta falla, cae al proxy.
    """
    equipo = (request.args.get('equipo') or '').strip() or None
    match_id = (request.args.get('match_id') or '').strip()
    match_id = int(match_id) if match_id.isdigit() else None
    try:
        limite = max(1, min(int(request.args.get('limite', 200)), 1000))
    except (TypeError, ValueError):
        limite = 200
    try:
        condiciones, params = [], []
        if match_id is not None:
            condiciones.append('match_id = ?')
            params.append(match_id)
        if equipo:
            condiciones.append('(equipo_a = ? OR equipo_b = ?)')
            params.extend([equipo, equipo])
        where = ('WHERE ' + ' AND '.join(condiciones)) if condiciones else ''
        filas = fetch_all(
            f"""SELECT match_id, equipo_a, equipo_b,
                       COUNT(*) AS filas,
                       COUNT(DISTINCT map_name) AS mapas,
                       MAX(n_sim) AS n_sim,
                       MAX(modelo_version) AS modelo_version,
                       MAX(created_at) AS created_at,
                       MAX(updated_at) AS updated_at
                FROM predicciones_mapa {where}
                GROUP BY match_id, equipo_a, equipo_b
                ORDER BY updated_at DESC
                LIMIT {int(limite)}""",
            params,
        )
        version, _ = _version_vigente_db()
        lista = []
        for f in filas:
            n_sim = f.get('n_sim')
            lista.append({
                'match_id': int(f['match_id']) if f.get('match_id') is not None else 0,
                'equipo_a': f.get('equipo_a'),
                'equipo_b': f.get('equipo_b'),
                'mapas': int(f.get('mapas') or 0),
                'filas': int(f.get('filas') or 0),
                'n_sim': int(n_sim) if n_sim is not None else None,
                'modelo_version': f.get('modelo_version'),
                'vigente': str(f.get('modelo_version')) == str(version),
                'created_at': f.get('created_at'),
                'updated_at': f.get('updated_at'),
            })
        return jsonify({
            'ok': True,
            'modelo_version': version,
            'simulaciones': lista,
        })
    except Exception:  # noqa: BLE001 - cae al servicio
        return _passthrough_get('/api/simulaciones')


@aletheia_bp.route('/api/aletheia/serie', methods=['POST'])
def serie():
    """Proxy POST /api/serie (probabilidad de serie cache-aware).

    Reutiliza las filas de caché vigentes y **calcula y persiste** en la DB lo
    que falte (mapa y serie); una segunda llamada idéntica sale de caché
    (`fuente` por mapa). Devuelve además `confianza_serie`, `modelo_version`,
    `n_sim`, `resultados_serie`, `caminos_serie` y `prob_intervalo` por mapa.

    Si hay una fila en `predicciones_serie` con los mismos mapas/lados se
    responde directo de Turso (sin servidor); si no, va al servicio de lectura
    de Render (cache-first) y, con `forzar:true`, al PC/ngrok.
    """
    body = request.get_json(silent=True) or {}
    try:
        respuesta = _serie_desde_db(body)
        if respuesta is not None:
            return jsonify(respuesta)
    except Exception:  # noqa: BLE001 - cae al servicio
        pass
    prefer = 'ngrok' if _es_forzar(body) else 'read'
    return _passthrough_post('/api/serie', prefer=prefer)


@aletheia_bp.route('/api/aletheia/borrar', methods=['POST'])
def borrar():
    """Proxy POST /api/borrar (borra las predicciones de un enfrentamiento)."""
    return _passthrough_post('/api/borrar', prefer='ngrok')


@aletheia_bp.route('/api/aletheia/recomendar', methods=['POST'])
def recomendar():
    """L2: tier-list del pool + mejor mapa/ban/lado (capa ESC anclada al motor).

    Carga el motor completo la primera vez (1–2 min en frío), así que va
    siempre a BASE_URL/ngrok (`prefer='ngrok'`), sin fallback a READ_URL (en
    Render aún no está el código L2 y su cold start free tarda igual). El body
    es `{"equipo_a", "equipo_b", "mapas": [...]?}`; `mapas` es opcional (sin él
    usa el pool vigente) y admite máx. 5. La P(serie) no la mueve esta capa.
    """
    return _passthrough_post('/api/recomendar', prefer='ngrok')


@aletheia_bp.route('/api/aletheia/perfil', methods=['GET'])
def perfil():
    """L3: perfil económico PIT por equipo (`equipo_a`) o cruce (+`equipo_b`).

    Carga el motor completo → BASE_URL/ngrok (`prefer='ngrok'`). Reenvía los
    query params (`equipo_a`/`equipo_b`/`equipo`) tal cual. Los empates ~50/50
    (pistols R1/R13, `full vs full`) son esperados, no señal fuerte.
    """
    return _passthrough_get('/api/perfil', prefer='ngrok')
