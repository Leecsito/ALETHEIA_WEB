"""
ALETHEIA — Media Blueprint (enlaces a logos de equipos, fotos de jugadores y logos de eventos)
Rutas: /api/media/equipo/<team_id>, /api/media/jugador/<player_id>,
       /api/media/evento/<event_id>, /api/media/estado

NO descarga ni guarda imágenes. Resuelve el enlace directo desde vlr.gg
(misma fuente que los datos) y redirige al navegador para que cargue la imagen
desde el CDN (owcdn.net). Solo se cachea el ENLACE en un JSON diminuto
(`media/urls_cache.json`): ~80 bytes por equipo/jugador, regenerable.

Reglas:
- Resolución serializada de a 2 como máximo y con 0.3 s entre requests (cortesía).
- Los "sin imagen" (404 real o sin logo/foto) se marcan y no se reintentan por 24 h.
- Los timeouts/errores de red NO se marcan: se reintenta en la próxima visita.
"""

import os
import re
import json
import time
import threading
import requests
from flask import Blueprint, redirect, jsonify

media_bp = Blueprint('media', __name__)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URLS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'urls_cache.json')

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}

MISS_TTL = 24 * 3600          # reintentar "sin imagen" pasado 1 día
MIN_INTERVAL = 0.3            # segundos entre requests a vlr.gg
PAGE_TIMEOUT = 20
REDIRECT_MAX_AGE = 7 * 24 * 3600

SEM = threading.BoundedSemaphore(2)
RATE_LOCK = threading.Lock()
CACHE_LOCK = threading.Lock()
_last_request = [0.0]


def _cargar_cache():
    try:
        with open(URLS_PATH, encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


_cache = _cargar_cache()


def _guardar_cache():
    try:
        tmp = URLS_PATH + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(_cache, f, ensure_ascii=False, indent=0)
        os.replace(tmp, URLS_PATH)
    except OSError:
        pass


# ─── RUTAS ───────────────────────────────────────────────────────────────────
@media_bp.route('/api/media/equipo/<int:team_id>', methods=['GET'])
def media_equipo(team_id):
    return _servir('equipos', team_id, f'https://www.vlr.gg/team/{team_id}')


@media_bp.route('/api/media/jugador/<int:player_id>', methods=['GET'])
def media_jugador(player_id):
    return _servir('jugadores', player_id, f'https://www.vlr.gg/player/{player_id}')


@media_bp.route('/api/media/evento/<int:event_id>', methods=['GET'])
def media_evento(event_id):
    return _servir('eventos', event_id, f'https://www.vlr.gg/event/{event_id}')


@media_bp.route('/api/media/estado', methods=['GET'])
def media_estado():
    resumen = {}
    for kind in ('equipos', 'jugadores', 'eventos'):
        entradas = (_cache.get(kind) or {})
        resumen[kind] = {
            'resueltas': sum(1 for v in entradas.values() if isinstance(v, dict) and v.get('u')),
            'sin_imagen': sum(1 for v in entradas.values() if isinstance(v, dict) and not v.get('u')),
        }
    tamano = os.path.getsize(URLS_PATH) if os.path.exists(URLS_PATH) else 0
    return jsonify({"ok": True, "urls_cache": URLS_PATH, "bytes": tamano, "estado": resumen})


def _servir(kind, eid, page_url):
    estado, url = ensure(kind, eid, page_url)
    if url:
        resp = redirect(url, code=302)
        resp.headers['Cache-Control'] = f'public, max-age={REDIRECT_MAX_AGE}'
        return resp
    return jsonify({"ok": False, "error": "Sin imagen disponible.", "estado": estado}), 404


# ─── LÓGICA DE RESOLUCIÓN ────────────────────────────────────────────────────
def _entrada(kind, eid):
    with CACHE_LOCK:
        return (_cache.get(kind) or {}).get(str(eid))


def _guardar(kind, eid, url):
    with CACHE_LOCK:
        _cache.setdefault(kind, {})[str(eid)] = {'u': url, 't': int(time.time())}
        _guardar_cache()


def _miss_vigente(entrada):
    return bool(entrada) and not entrada.get('u') and \
        (time.time() - int(entrada.get('t') or 0)) < MISS_TTL


def _throttle():
    with RATE_LOCK:
        espera = MIN_INTERVAL - (time.time() - _last_request[0])
        if espera > 0:
            time.sleep(espera)
        _last_request[0] = time.time()


CONTENEDORES = {
    'equipos': 'team-header-logo',
    'jugadores': 'player-header',
    'eventos': 'event-header',
}


def _extraer_imagen(html, kind):
    """Busca la imagen del header (logo/foto) y cae a og:image si no aparece."""
    contenedor = CONTENEDORES.get(kind, 'team-header-logo')
    m = re.search(contenedor + r'[\s\S]{0,800}?<img[^>]+src=["\']([^"\']+)', html, re.I)
    if not m:
        m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)', html, re.I)
    if not m:
        m = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', html, re.I)
    if not m:
        return None
    url = m.group(1).strip()
    if url.startswith('//'):
        url = 'https:' + url
    return url if url.startswith('http') else None


def ensure(kind, eid, page_url):
    """Resuelve el enlace de la imagen (sin descargarla). Devuelve (estado, url|None).

    Estados: cache | ok | miss | busy | error
    """
    entrada = _entrada(kind, eid)
    if entrada and entrada.get('u'):
        return 'cache', entrada['u']
    if _miss_vigente(entrada):
        return 'miss', None

    if not SEM.acquire(timeout=10):
        return 'busy', None

    try:
        entrada = _entrada(kind, eid)
        if entrada and entrada.get('u'):
            return 'cache', entrada['u']

        _throttle()
        try:
            r = requests.get(page_url, headers=HEADERS, timeout=PAGE_TIMEOUT)
        except requests.RequestException:
            return 'error', None
        if r.status_code == 404:
            _guardar(kind, eid, None)
            return 'miss', None
        if r.status_code != 200:
            return 'error', None

        url = _extraer_imagen(r.text, kind)
        if not url:
            _guardar(kind, eid, None)
            return 'miss', None

        _guardar(kind, eid, url)
        return 'ok', url
    except Exception:
        return 'error', None
    finally:
        SEM.release()
