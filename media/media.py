"""
ALETHEIA — Media Blueprint (enlaces a logos de equipos, fotos de jugadores y logos de eventos)
Rutas: /api/media/equipo/<team_id>, /api/media/jugador/<player_id>,
       /api/media/evento/<event_id>, /api/media/evento?nombre=...,
       /api/media/colores, /api/media/estado

NO guarda imágenes. Resuelve el enlace directo desde vlr.gg y redirige al
navegador para que cargue la imagen desde el CDN (owcdn.net). Solo se cachea
metadatos diminutos en `media/urls_cache.json`: el enlace, el color medio del
logo (`c`) y si es oscuro (`d`).

Para el color medio se lee la imagen UNA vez EN MEMORIA (nunca se escribe a
disco) y se descarta; solo se guarda el hex resultante (~7 bytes).

Los eventos sin `event_id` se resuelven por nombre con el buscador de vlr.gg
(`/api/media/evento?nombre=Valorant Champions 2026`).

Reglas:
- Resolución serializada de a 2 como máximo y con 0.3 s entre requests (cortesía).
- Los "sin imagen" (404 real o sin logo/foto) se marcan y no se reintentan por 24 h.
- Los timeouts/errores de red NO se marcan: se reintenta en la próxima visita.
"""

import os
import io
import re
import json
import time
import threading
from urllib.parse import quote
import requests
from flask import Blueprint, redirect, jsonify, request

try:
    from PIL import Image
except ImportError:
    Image = None

media_bp = Blueprint('media', __name__)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URLS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'urls_cache.json')

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}
IMG_HEADERS = {**HEADERS, "Referer": "https://www.vlr.gg/"}

MISS_TTL = 24 * 3600          # reintentar "sin imagen" pasado 1 día
MIN_INTERVAL = 0.3            # segundos entre requests a vlr.gg
PAGE_TIMEOUT = 20
IMG_TIMEOUT = 25
MAX_BYTES = 8 * 1024 * 1024   # 8 MB de tope
REDIRECT_MAX_AGE = 7 * 24 * 3600
COLOR_DARK_LUM = 0.5          # debajo de esto el logo es oscuro -> fondo claro (contraste)

SEM = threading.BoundedSemaphore(2)
RATE_LOCK = threading.Lock()
CACHE_LOCK = threading.Lock()
COLOR_LOCK = threading.Lock()
_colores_en_proceso = set()
_last_request = [0.0]


def _cargar_cache():
    try:
        with open(URLS_PATH, encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


_cache = _cargar_cache()
_cache_mtime = [os.path.getmtime(URLS_PATH) if os.path.exists(URLS_PATH) else 0.0]


def _guardar_cache():
    try:
        tmp = URLS_PATH + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(_cache, f, ensure_ascii=False, indent=0)
        os.replace(tmp, URLS_PATH)
        _cache_mtime[0] = os.path.getmtime(URLS_PATH)
    except OSError:
        pass


def _recargar_si_cambio():
    """Si otro proceso (script de precarga u otro worker) escribió el JSON, recargar."""
    try:
        m = os.path.getmtime(URLS_PATH)
    except OSError:
        return
    if m <= _cache_mtime[0]:
        return
    with CACHE_LOCK:
        if m <= _cache_mtime[0]:
            return
        try:
            with open(URLS_PATH, encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                _cache.clear()
                _cache.update(data)
        except Exception:
            pass
        _cache_mtime[0] = m


def _slug(texto):
    return re.sub(r'[^a-z0-9]+', '-', str(texto or '').lower()).strip('-')


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


@media_bp.route('/api/media/evento', methods=['GET'])
def media_evento_nombre():
    nombre = (request.args.get('nombre') or '').strip()
    if not nombre:
        return jsonify({"ok": False, "error": "Falta ?nombre=."}), 400
    estado, url = ensure_evento_nombre(nombre)
    if url:
        resp = redirect(url, code=302)
        resp.headers['Cache-Control'] = f'public, max-age={REDIRECT_MAX_AGE}'
        return resp
    return jsonify({"ok": False, "error": "Sin imagen disponible.", "estado": estado}), 404


@media_bp.route('/api/media/meta', methods=['GET'])
def media_meta():
    """URLs resueltas + color medio, sin disparar descargas.

    Con esto el frontend apunta los <img> directo al CDN (cero requests a este
    backend por imagen) y pinta colores/watermarks. Lo no resuelto se pide por
    el endpoint de redirect (`/api/media/...`) que sí resuelve bajo demanda.
    """
    def parse_ids(raw):
        return [x.strip() for x in (raw or '').split(',') if x.strip()]

    def info(kind, eid):
        e = _entrada(kind, eid)
        if e and e.get('u'):
            # Enlace resuelto pero sin color: calcularlo en segundo plano.
            if kind in ('equipos', 'eventos') and not e.get('c') and not e.get('cc'):
                _lanzar_color(kind, eid, e['u'])
            return {'u': e['u'], 'c': e.get('c'), 'd': bool(e.get('d'))}
        return None

    def collect(kind, ids):
        out = {}
        for i in ids:
            v = info(kind, i)
            if v:
                out[i] = v
        return out

    equipos = collect('equipos', parse_ids(request.args.get('equipos')))
    jugadores = collect('jugadores', parse_ids(request.args.get('jugadores')))
    eventos = collect('eventos', parse_ids(request.args.get('eventos')))
    nombres = {}
    for nombre in [n for n in (request.args.get('nombres') or '').split('|') if n.strip()]:
        v = info('eventos', 'n:' + _slug(nombre))
        if v:
            nombres[_slug(nombre)] = v
    return jsonify({
        "ok": True,
        "equipos": equipos, "jugadores": jugadores,
        "eventos": eventos, "nombres": nombres,
    })


@media_bp.route('/api/media/estado', methods=['GET'])
def media_estado():
    resumen = {}
    for kind in ('equipos', 'jugadores', 'eventos'):
        entradas = (_cache.get(kind) or {})
        resumen[kind] = {
            'resueltas': sum(1 for v in entradas.values() if isinstance(v, dict) and v.get('u')),
            'sin_imagen': sum(1 for v in entradas.values() if isinstance(v, dict) and not v.get('u')),
            'con_color': sum(1 for v in entradas.values() if isinstance(v, dict) and v.get('c')),
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
    _recargar_si_cambio()
    with CACHE_LOCK:
        return (_cache.get(kind) or {}).get(str(eid))


def _guardar(kind, eid, url, color=None, dark=None):
    with CACHE_LOCK:
        entrada = {'u': url, 't': int(time.time())}
        if color:
            entrada['c'] = color
            entrada['d'] = bool(dark)
        _cache.setdefault(kind, {})[str(eid)] = entrada
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


def _color_medio(data):
    """Color medio y si el logo es oscuro. Se calcula EN MEMORIA, no se guarda."""
    if Image is None or not data:
        return None, False
    try:
        im = Image.open(io.BytesIO(data)).convert('RGBA')
        im.thumbnail((48, 48))
        r = g = b = n = 0
        for pr, pg, pb, pa in im.getdata():
            if pa < 40:                     # ignorar píxeles transparentes
                continue
            r += pr; g += pg; b += pb; n += 1
        if not n:
            return None, True
        r //= n; g //= n; b //= n
        lum = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255
        return f'#{r:02x}{g:02x}{b:02x}', lum < COLOR_DARK_LUM
    except Exception:
        return None, False


def _color_de_url(img_url):
    """Lee la imagen una vez (en memoria) para sacar su color medio."""
    try:
        r = requests.get(img_url, headers=IMG_HEADERS, timeout=IMG_TIMEOUT)
        if r.status_code == 200 and len(r.content) <= MAX_BYTES:
            return _color_medio(r.content)
    except requests.RequestException:
        pass
    return None, False


def _resolver_evento_nombre(nombre):
    """Busca un evento por nombre en vlr.gg. Devuelve (event_id|None, img_url|None)."""
    url = 'https://www.vlr.gg/search/?q=' + quote(nombre)
    r = requests.get(url, headers=HEADERS, timeout=PAGE_TIMEOUT)
    if r.status_code != 200:
        return None, None
    m = re.search(r'/search/r/event/(\d+)/idx[\s\S]{0,1600}?<img[^>]+src=["\']([^"\']+)', r.text, re.I)
    if m:
        img = m.group(2)
        return m.group(1), ('https:' + img if img.startswith('//') else img)
    m = re.search(r'search-item-thumb[\s\S]{0,500}?<img[^>]+src=["\']([^"\']+)', r.text, re.I)
    if m:
        img = m.group(1)
        return None, ('https:' + img if img.startswith('//') else img)
    return None, None


def _completar_color(kind, eid, url):
    """Calcula el color medio en segundo plano (no bloquea la respuesta)."""
    try:
        if not SEM.acquire(timeout=8):
            return
        try:
            _throttle()
            color, dark = _color_de_url(url)
            with CACHE_LOCK:
                entrada = (_cache.get(kind) or {}).get(str(eid))
                if entrada and entrada.get('u') == url:
                    entrada['c'] = color
                    entrada['d'] = bool(dark)
                    entrada['cc'] = True
                    _guardar_cache()
        finally:
            SEM.release()
    finally:
        with COLOR_LOCK:
            _colores_en_proceso.discard((kind, str(eid)))


def _lanzar_color(kind, eid, url):
    clave = (kind, str(eid))
    with COLOR_LOCK:
        if clave in _colores_en_proceso:
            return
        _colores_en_proceso.add(clave)
    threading.Thread(target=_completar_color, args=(kind, eid, url), daemon=True).start()


def ensure(kind, eid, page_url):
    """Resuelve el enlace de la imagen (sin guardarla). Devuelve (estado, url|None).

    Estados: cache | ok | miss | busy | error
    """
    entrada = _entrada(kind, eid)
    if entrada and entrada.get('u'):
        if kind in ('equipos', 'eventos') and not entrada.get('c') and not entrada.get('cc'):
            _lanzar_color(kind, eid, entrada['u'])
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

        color, dark = (None, False)
        if kind in ('equipos', 'eventos'):
            color, dark = _color_de_url(url)
        _guardar(kind, eid, url, color, dark)
        return 'ok', url
    except Exception:
        return 'error', None
    finally:
        SEM.release()


def ensure_evento_nombre(nombre):
    """Resuelve un evento por nombre (para torneos sin event_id)."""
    key = 'n:' + _slug(nombre)
    entrada = _entrada('eventos', key)
    if entrada and entrada.get('u'):
        if not entrada.get('c') and not entrada.get('cc'):
            _lanzar_color('eventos', key, entrada['u'])
        return 'cache', entrada['u']
    if _miss_vigente(entrada):
        return 'miss', None

    if not SEM.acquire(timeout=10):
        return 'busy', None

    try:
        entrada = _entrada('eventos', key)
        if entrada and entrada.get('u'):
            return 'cache', entrada['u']

        _throttle()
        try:
            eid, img = _resolver_evento_nombre(nombre)
        except requests.RequestException:
            return 'error', None
        if not img:
            _guardar('eventos', key, None)
            return 'miss', None

        color, dark = _color_de_url(img)
        _guardar('eventos', key, img, color, dark)
        if eid:                                     # alias numérico para futuras visitas
            _guardar('eventos', eid, img, color, dark)
        return 'ok', img
    except Exception:
        return 'error', None
    finally:
        SEM.release()
