"""
ALETHEIA — Cargador masivo de torneos (ETL por lotes)

Recorre una carpeta con subcarpetas por torneo (cada una con sus .xlsx),
y ejecuta el ETL de cada torneo directamente contra la base de datos.

Uso:
    # Contra Turso (usa TURSO_DATABASE_URL / TURSO_AUTH_TOKEN del entorno, o los por defecto)
    python cargar_todo.py --data "D:\\PROYECTOS\\ALETHEIA\\output_data"

    # Contra SQLite local (backend/aletheia.db)
    python cargar_todo.py --data "...\\output_data" --local

    # Solo algunos torneos y limpiando la BD antes
    python cargar_todo.py --data "..." --only champions --reset

Antes de usarlo:  pip install -r requirements.txt
"""

import os
import sys
import time
import argparse

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Consola de Windows suele ser cp1252 y rompe con símbolos/acentos.
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

# Archivos que el ETL entiende, en el orden en que los lee
DATA_FILES = [
    'vct_partidos', 'vlr_mapas', 'vlr_rondas', 'vlr_economia_rondas',
    'vlr_stats_players_sides', 'vlr_economia_resumen',
    'vlr_enfrentamientos', 'vlr_multikills_clutches',
]
GLOBAL_FILES = ['vct_equipos', 'vct_jugadores']

# Borrado en este orden respeta las FKs (hijos primero)
DELETE_ORDER = [
    'rounds', 'duels', 'multikills_clutches', 'player_stats',
    'economy_summary', 'match_veto', 'maps', 'matches', 'players', 'teams',
]


def read_file(path):
    if not os.path.exists(path):
        return None
    with open(path, 'rb') as fh:
        return fh.read()


def build_raw_files(folder, common):
    raw = dict(common)
    for name in DATA_FILES:
        blob = read_file(os.path.join(folder, name + '.xlsx'))
        if blob is not None:
            raw[name] = blob
    return raw


def torneo_cargado(conn, folder):
    """True si TODOS los match_id de vct_partidos ya están en la tabla matches."""
    import pandas as pd
    path = os.path.join(folder, 'vct_partidos.xlsx')
    if not os.path.exists(path):
        return False
    try:
        df = pd.read_excel(path)
    except Exception:
        return False
    mids = sorted({int(x) for x in df['match_id'].dropna()})
    if not mids:
        return False
    cur = conn.cursor()
    try:
        q = ','.join('?' * len(mids))
        cur.execute(f'SELECT COUNT(DISTINCT match_id) FROM matches WHERE match_id IN ({q})', mids)
        n = cur.fetchone()[0]
    except Exception:
        return False
    finally:
        cur.close()
    return n >= len(mids)


def main():
    parser = argparse.ArgumentParser(description='Carga masiva de torneos ALETHEIA (ETL).')
    parser.add_argument('--data', default='output_data',
                        help='Carpeta raíz con las subcarpetas de torneos (default: output_data)')
    parser.add_argument('--only', default=None,
                        help='Procesar solo torneos cuyo nombre contenga este texto (ej: champions)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Procesar como máximo N torneos')
    parser.add_argument('--reset', action='store_true',
                        help='Vaciar todas las tablas antes de cargar (¡cuidado!)')
    parser.add_argument('--skip-existing', action='store_true',
                        help='Omitir torneos cuyos partidos ya estén cargados (reanudable)')
    parser.add_argument('--retries', type=int, default=3,
                        help='Reintentos por torneo ante fallos de red (default: 3)')
    parser.add_argument('--local', action='store_true',
                        help='Usar SQLite local en vez de Turso')
    parser.add_argument('--yes', action='store_true',
                        help='No pedir confirmación')
    args = parser.parse_args()

    base = os.path.abspath(args.data)
    if not os.path.isdir(base):
        print(f'✕ No existe la carpeta: {base}')
        return 1

    # La conexión lee las variables de entorno al importarse: hay que fijarlas antes.
    if args.local:
        os.environ['TURSO_DATABASE_URL'] = ''

    try:
        from backend import conexion as cx
        import inicio.inicio as etl
    except ImportError as e:
        print(f'✕ Falta una dependencia: {e}')
        print('  Instalá con:  pip install -r requirements.txt')
        return 1

    remote = cx.TURSO_DATABASE_URL.strip().startswith(('libsql://', 'http://', 'https://'))
    if remote and cx.libsql is not None:
        destino = f'Turso remoto → {cx.TURSO_DATABASE_URL}'
    elif remote and cx.libsql is None:
        destino = f'SQLite local ({cx.LOCAL_DB_PATH})  ⚠ libsql NO instalado, se ignora Turso'
    else:
        destino = f'SQLite local ({cx.LOCAL_DB_PATH})'

    torneos = sorted(d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d)))
    if args.only:
        torneos = [t for t in torneos if args.only.lower() in t.lower()]
    if args.limit:
        torneos = torneos[:args.limit]

    if not torneos:
        print('✕ No se encontraron subcarpetas de torneos.')
        return 1

    print('=' * 70)
    print(f'  ALETHEIA — Carga masiva ETL')
    print(f'  Origen : {base}')
    print(f'  Destino: {destino}')
    print(f'  Torneos: {len(torneos)}')
    print(f'  Reset  : {"SÍ (se vaciarán las tablas)" if args.reset else "no"}')
    print('=' * 70)

    if args.reset and not args.yes:
        resp = input('⚠ --reset borrará TODOS los datos de la BD. ¿Continuar? [s/N]: ').strip().lower()
        if resp not in ('s', 'si', 'sí', 'y', 'yes'):
            print('Cancelado.')
            return 0

    # ── Preparar esquema ──
    conn = etl.get_conn()
    try:
        cur = conn.cursor()
        for sql in etl.CREATE_TABLES_SQL:
            cur.execute(sql)
        etl.run_migrations(conn)
        if args.reset:
            for t in DELETE_ORDER:
                try:
                    cur.execute(f'DELETE FROM {t}')
                except Exception:
                    pass
        conn.commit()
        cur.close()
    finally:
        etl.release_conn(conn)

    # ── Archivos globales (equipos / jugadores) ──
    common = {}
    for name in GLOBAL_FILES:
        blob = read_file(os.path.join(base, name + '.xlsx'))
        if blob is not None:
            common[name] = blob
    if common:
        print(f'Globales: {", ".join(common)}')
    else:
        print('Globales: (no encontrados) — se derivarán de partidos/stats')

    # ── Procesar torneo por torneo ──
    check_conn = None
    if args.skip_existing:
        try:
            check_conn = etl.get_conn()
        except Exception as e:
            print(f'⚠ No se pudo abrir conexión de comprobación: {e}')

    totals, ok, fail, omitidos = {}, 0, 0, 0
    t_inicio = time.time()
    for i, nombre in enumerate(torneos, 1):
        carpeta = os.path.join(base, nombre)
        if check_conn is not None and torneo_cargado(check_conn, carpeta):
            print(f'[{i:>2}/{len(torneos)}] = {nombre}: ya cargado, se omite')
            omitidos += 1
            continue

        raw = build_raw_files(carpeta, common)
        if 'vct_partidos' not in raw:
            print(f'[{i:>2}/{len(torneos)}] ⚠ {nombre}: sin vct_partidos.xlsx, se omite')
            continue

        print(f'[{i:>2}/{len(torneos)}] ▶ {nombre} ... ', end='', flush=True)
        t0 = time.time()
        res = None
        for intento in range(1, max(1, args.retries) + 1):
            try:
                res = etl._process_etl(raw, {})
                break
            except Exception as e:
                if intento < max(1, args.retries):
                    print(f'↻ reintento {intento}/{args.retries - 1} ({e}) ... ', end='', flush=True)
                    time.sleep(3 * intento)
                else:
                    dt = time.time() - t0
                    print(f'✕ {dt:.1f}s  {e}')
                    fail += 1
        if res is None:
            continue
        dt = time.time() - t0
        for k, v in res.items():
            totals[k] = totals.get(k, 0) + v
        print(f'✓ {dt:.1f}s  (' + ', '.join(f'{k}={v}' for k, v in res.items()) + ')')
        ok += 1

    if check_conn is not None:
        etl.release_conn(check_conn)

    # ── Resumen ──
    print('=' * 70)
    extra = f', {omitidos} omitidos (ya cargados)' if omitidos else ''
    print(f'  Listo en {time.time() - t_inicio:.1f}s — {ok} torneos OK, {fail} con error{extra}')
    print('  Totales insertados:')
    for k in sorted(totals):
        print(f'    {k:<18} {totals[k]}')
    print('=' * 70)
    return 0 if fail == 0 else 2


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print('\nInterrumpido por el usuario.')
        sys.exit(130)
