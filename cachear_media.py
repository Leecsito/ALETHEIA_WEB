"""
ALETHEIA — Precarga de ENLACES de imágenes (logos de equipos y fotos de jugadores)

NO descarga imágenes: solo resuelve el enlace directo desde vlr.gg y lo guarda
en `media/urls_cache.json` (~80 bytes por entidad). El sitio web resuelve enlaces
bajo demanda igualmente; este script sirve para "calentar" la caché de golpe de
forma educada (1 request/segundo por defecto).

Uso:
    python cachear_media.py --equipos              # equipos con partidos jugados
    python cachear_media.py --jugadores            # jugadores con nickname
    python cachear_media.py --eventos              # eventos con event_id de vlr.gg
    python cachear_media.py --todo                 # los tres
    python cachear_media.py --todo --limite 50     # solo los primeros 50 de cada grupo
    python cachear_media.py --todo --delay 0.5     # más rápido (menos cortés)
"""

import os
import sys
import time
import argparse

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

from backend.conexion import get_conn, release_conn
from media.media import ensure


def query(sql):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        return rows
    finally:
        release_conn(conn)


def equipos(limite=None):
    sql = """
        SELECT t.team_id, t.team_name
        FROM teams t
        WHERE EXISTS (
            SELECT 1 FROM matches m
            WHERE m.team_a_id = t.team_id OR m.team_b_id = t.team_id
        )
        ORDER BY t.team_id
    """
    rows = query(sql)
    return rows[:limite] if limite else rows


def jugadores(limite=None):
    sql = """
        SELECT player_id, nickname
        FROM players
        WHERE nickname IS NOT NULL AND nickname != ''
        ORDER BY player_id
    """
    rows = query(sql)
    return rows[:limite] if limite else rows


def eventos(limite=None):
    sql = """
        SELECT event_id, event_name
        FROM events
        ORDER BY event_id
    """
    rows = query(sql)
    return rows[:limite] if limite else rows


ID_KEY = {'equipos': 'team_id', 'jugadores': 'player_id', 'eventos': 'event_id'}
NAME_KEY = {'equipos': 'team_name', 'jugadores': 'nickname', 'eventos': 'event_name'}
URL_PATH = {'equipos': 'team', 'jugadores': 'player', 'eventos': 'event'}


def procesar(kind, items, delay):
    total = len(items)
    resumen = {'cache': 0, 'ok': 0, 'miss': 0, 'error': 0, 'busy': 0}
    print(f"\n=== {kind.upper()} ({total}) ===")
    for i, item in enumerate(items, 1):
        eid = item[ID_KEY[kind]]
        nombre = item.get(NAME_KEY[kind]) or str(eid)
        estado, url = ensure(kind, eid, f"https://www.vlr.gg/{URL_PATH[kind]}/{eid}")
        resumen[estado] = resumen.get(estado, 0) + 1
        detalle = estado if estado != 'ok' else (url or '')
        print(f"  [{i:>4}/{total}] {eid:<7} {nombre[:32]:<32} -> {detalle}")
        if estado in ('ok', 'error') and delay > 0:
            time.sleep(delay)
    print(f"  resumen {kind}: " + "  ".join(f"{k}={v}" for k, v in resumen.items()))
    return resumen


def main():
    ap = argparse.ArgumentParser(description='Resuelve enlaces de logos/fotos desde vlr.gg a media/urls_cache.json.')
    ap.add_argument('--equipos', action='store_true', help='resolver logos de equipos con partidos')
    ap.add_argument('--jugadores', action='store_true', help='resolver fotos de jugadores con nickname')
    ap.add_argument('--eventos', action='store_true', help='resolver logos de eventos con event_id')
    ap.add_argument('--todo', action='store_true', help='equivale a --equipos --jugadores --eventos')
    ap.add_argument('--limite', type=int, default=0, help='máximo de items por grupo (0 = todos)')
    ap.add_argument('--delay', type=float, default=1.0, help='segundos de espera entre resoluciones (default 1.0)')
    args = ap.parse_args()

    if not (args.equipos or args.jugadores or args.eventos or args.todo):
        ap.print_help()
        return

    limite = args.limite or None
    t0 = time.time()
    if args.equipos or args.todo:
        procesar('equipos', equipos(limite), args.delay)
    if args.jugadores or args.todo:
        procesar('jugadores', jugadores(limite), args.delay)
    if args.eventos or args.todo:
        procesar('eventos', eventos(limite), args.delay)
    print(f"\nListo en {time.time() - t0:.1f}s. Enlaces en media/urls_cache.json")


if __name__ == '__main__':
    main()
