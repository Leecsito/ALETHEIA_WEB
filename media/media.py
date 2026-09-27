"""
ALETHEIA — Media Blueprint (logos de equipos y fotos de jugadores)
Rutas: /api/media/equipo/<team_id>, /api/media/jugador/<player_id>, /api/media/estado

Descarga BAJO DEMANDA desde vlr.gg (misma fuente que los datos) y cachea en
`multimedia/cache/equipos/` y `multimedia/cache/jugadores/`. No es una página:
solo sirve imágenes. Si no hay imagen (o vlr.gg falla), responde 404 y el
frontend muestra el fallback (lozenge con siglas / avatar con iniciales).

Reglas:
- Descarga serializada de a 2 como máximo y con 0.3 s entre requests (cortesía).
- Los "miss" (sin imagen / 404 real) se marcan y no se reintentan por 24 h.
- Los timeouts/errores de red NO se marcan: se reintenta en la próxima visita.
"""

import os
import re
import time
import threading
import requests
from flask import Blueprint, send_file, jsonify

media_bp = Blueprint('media', __name__)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, 'multimedia', 'cache')

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}
IMG_HEADERS = {**HEADERS, "Referer": "https://www.vlr.gg/"}

EXTS = ('png', 'jpg', 'jpeg', 'webp', 'svg', 'gif')
MISS_TTL = 24 * 3600          # reintentar "sin imagen" pasado 1 día
MAX_BYTES = 8 * 1024 * 1024   # 8 MB de tope
MIN_INTERVAL = 0.3            # segundos entre requests a vlr.gg
PAGE_TIMEOUT = 20
IMG_TIMEOUT = 25

SEM = threading.BoundedSemaphore(2)
RATE_LOCK = threading.Lock()
_last_request = [0.0]


# ─── RUTAS ───────────────────────────────────────────────────────────────────
@media_bp.route('/api/media/equipo/<int:team_id>', methods=['GET'])
def media_equipo(team_id):
    return _servir('equipos', team_id, f'https://www.vlr.gg/team/{team_id}')


@media_bp.route('/api/media/jugador/<int:player_id>', methods=['GET'])
def media_jugador(player_id):
    return _servir('jugadores', player_id, f'https://www.vlr.gg/player/{player_id}')


@media_bp.route('/api/media/estado', methods=['GET'])
def media_estado():
    resumen = {}
    for kind in ('equipos', 'jugadores'):
        carpeta = os.path.join(CACHE_DIR, kind)
        imagenes, misses = 0, 0
        if os.path.isdir(carpeta):
            for f in os.listdir(carpeta):
                if f.endswith('.miss'):
                    misses += 1
                elif f.rsplit('.', 1)[-1].lower() in EXTS:
                    imagenes += 1
        resumen[kind] = {'cacheadas': imagenes, 'sin_imagen': misses}
    return jsonify({"ok": True, "cache_dir": CACHE_DIR, "estado": resumen})


def _servir(kind, eid, page_url):
    estado, path = ensure(kind, eid, page_url)
    if path:
        return send_file(path, max_age=7 * 24 * 3600)
    return jsonify({"ok": False, "error": "Sin imagen disponible.", "estado": estado}), 404


# ─── LÓGICA DE CACHÉ ─────────────────────────────────────────────────────────
def _carpeta(kind):
    return os.path.join(CACHE_DIR, kind)


def _buscar_cache(kind, eid):
    for ext in EXTS:
        p = os.path.join(_carpeta(kind), f'{eid}.{ext}')
        if os.path.exists(p) and os.path.getsize(p) > 0:
            return p
    return None


def _miss_path(kind, eid):
    return os.path.join(_carpeta(kind), f'{eid}.miss')


def _tocar_miss(kind, eid):
    os.makedirs(_carpeta(kind), exist_ok=True)
    try:
        with open(_miss_path(kind, eid), 'w', encoding='utf-8') as f:
            f.write(time.strftime('%Y-%m-%d %H:%M:%S'))
    except OSError:
        pass


def _miss_vigente(kind, eid):
    p = _miss_path(kind, eid)
    try:
        return os.path.exists(p) and (time.time() - os.path.getmtime(p)) < MISS_TTL
    except OSError:
        return False


def _throttle():
    with RATE_LOCK:
        espera = MIN_INTERVAL - (time.time() - _last_request[0])
        if espera > 0:
            time.sleep(espera)
        _last_request[0] = time.time()


def _extraer_imagen(html, kind):
    """Busca la imagen del header (logo/foto) y cae a og:image si no aparece."""
    contenedor = 'team-header-logo' if kind == 'equipos' else 'player-header'
    m = re.search(contenedor + r'[\s\S]{0,600}?<img[^>]+src=["\']([^"\']+)', html, re.I)
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


def _extension(url, content_type):
    ctype = (content_type or '').split(';')[0].strip().lower()
    mapa = {
        'image/png': 'png', 'image/jpeg': 'jpg', 'image/jpg': 'jpg',
        'image/webp': 'webp', 'image/svg+xml': 'svg', 'image/gif': 'gif',
    }
    if ctype in mapa:
        return mapa[ctype]
    ext = url.rsplit('.', 1)[-1].lower()
    return ext if ext in EXTS else 'png'


def ensure(kind, eid, page_url):
    """Garantiza la imagen en caché. Devuelve (estado, ruta|None).

    Estados: cache | ok | miss | busy | error
    """
    path = _buscar_cache(kind, eid)
    if path:
        return 'cache', path

    if _miss_vigente(kind, eid):
        return 'miss', None

    if not SEM.acquire(timeout=10):
        return 'busy', None

    try:
        path = _buscar_cache(kind, eid)
        if path:
            return 'cache', path

        _throttle()
        try:
            r = requests.get(page_url, headers=HEADERS, timeout=PAGE_TIMEOUT)
        except requests.RequestException:
            return 'error', None
        if r.status_code == 404:
            _tocar_miss(kind, eid)
            return 'miss', None
        if r.status_code != 200:
            return 'error', None

        img_url = _extraer_imagen(r.text, kind)
        if not img_url:
            _tocar_miss(kind, eid)
            return 'miss', None

        _throttle()
        try:
            ir = requests.get(img_url, headers=IMG_HEADERS, timeout=IMG_TIMEOUT)
        except requests.RequestException:
            return 'error', None
        if ir.status_code != 200 or not ir.content:
            _tocar_miss(kind, eid)
            return 'miss', None
        if len(ir.content) > MAX_BYTES:
            _tocar_miss(kind, eid)
            return 'miss', None
        if not (ir.headers.get('Content-Type') or '').lower().startswith('image/'):
            _tocar_miss(kind, eid)
            return 'miss', None

        ext = _extension(img_url, ir.headers.get('Content-Type'))
        os.makedirs(_carpeta(kind), exist_ok=True)
        final = os.path.join(_carpeta(kind), f'{eid}.{ext}')
        tmp = final + '.tmp'
        with open(tmp, 'wb') as f:
            f.write(ir.content)
        os.replace(tmp, final)
        try:
            os.remove(_miss_path(kind, eid))
        except OSError:
            pass
        return 'ok', final
    except Exception:
        return 'error', None
    finally:
        SEM.release()
