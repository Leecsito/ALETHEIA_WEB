"""
ALETHEIA — Limpieza y actualización de equipos / jugadores

Problema: la tabla `teams` tiene muchísimos equipos basura (tier 3) y `players`
quedó con datos viejos. NO se pueden borrar todos con un DELETE simple porque
`matches`, `player_stats`, `duels`, etc. los referencian por FK.

Este script:
  1. Agrega las columnas nuevas (teams.tag, teams.country, players.country).
  2. Actualiza (upsert) los equipos/jugadores buenos desde los Excel.
  3. BORRA solo los equipos/jugadores que:
       - NO están referenciados por ninguna tabla, Y
       - NO vienen en los Excel nuevos.
     Es decir, borra la basura sin tocar nada anidado.

Uso:
    python limpiar_equipos_jugadores.py                       # usa vct_equipos.xlsx / vct_jugadores.xlsx de la raíz
    python limpiar_equipos_jugadores.py --dry-run             # solo muestra qué borraría
    python limpiar_equipos_jugadores.py --local               # contra SQLite local
"""

import os
import sys
import argparse

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

# Columnas que referencian a teams / players (para saber qué NO se puede borrar)
REF_TEAMS_SQL = """
    SELECT team_a_id AS id FROM matches WHERE team_a_id IS NOT NULL
    UNION SELECT team_b_id FROM matches WHERE team_b_id IS NOT NULL
    UNION SELECT team_id FROM match_veto WHERE team_id IS NOT NULL
    UNION SELECT picker_id FROM maps WHERE picker_id IS NOT NULL
    UNION SELECT team_id FROM player_stats WHERE team_id IS NOT NULL
    UNION SELECT team_id FROM economy_summary WHERE team_id IS NOT NULL
    UNION SELECT team_id FROM players WHERE team_id IS NOT NULL
"""
REF_PLAYERS_SQL = """
    SELECT player_id AS id FROM player_stats WHERE player_id IS NOT NULL
    UNION SELECT player_a_id FROM duels WHERE player_a_id IS NOT NULL
    UNION SELECT player_b_id FROM duels WHERE player_b_id IS NOT NULL
    UNION SELECT player_id FROM multikills_clutches WHERE player_id IS NOT NULL
"""


def main():
    ap = argparse.ArgumentParser(description='Limpieza segura de equipos y jugadores (ALETHEIA).')
    ap.add_argument('--equipos', default='vct_equipos.xlsx')
    ap.add_argument('--jugadores', default='vct_jugadores.xlsx')
    ap.add_argument('--local', action='store_true', help='Usar SQLite local en vez de Turso')
    ap.add_argument('--dry-run', action='store_true', help='Solo mostrar qué se borraría')
    ap.add_argument('--yes', action='store_true', help='No pedir confirmación')
    args = ap.parse_args()

    if args.local:
        os.environ['TURSO_DATABASE_URL'] = ''

    try:
        import pandas as pd
        from backend import conexion as cx
        import inicio.inicio as etl
    except ImportError as e:
        print(f'✕ Falta dependencia: {e}\n  Instalá con: pip install -r requirements.txt')
        return 1

    remote = cx.TURSO_DATABASE_URL.strip().startswith(('libsql://', 'http://', 'https://'))
    destino = f'Turso → {cx.TURSO_DATABASE_URL}' if (remote and cx.libsql) else f'SQLite local ({cx.LOCAL_DB_PATH})'
    print('=' * 70)
    print('  LIMPIEZA DE EQUIPOS / JUGADORES')
    print(f'  Destino: {destino}')
    print('=' * 70)

    eq_path = os.path.join(ROOT, args.equipos)
    ju_path = os.path.join(ROOT, args.jugadores)
    if not os.path.exists(eq_path) or not os.path.exists(ju_path):
        print(f'✕ No encuentro {args.equipos} / {args.jugadores} en {ROOT}')
        return 1

    equipos = pd.read_excel(eq_path)
    jugadores = pd.read_excel(ju_path)
    ids_equipos = sorted({etl.ii(x) for x in equipos['team_id'].dropna()} - {None})
    ids_jugadores = sorted({etl.ii(x) for x in jugadores['player_id'].dropna()} - {None})

    conn = etl.get_conn()
    try:
        cur = conn.cursor()

        # 1) Esquema + columnas nuevas
        for sql in etl.CREATE_TABLES_SQL:
            cur.execute(sql)
        etl.run_migrations(conn)

        def count(sql):
            cur.execute(sql)
            return cur.fetchone()[0]

        print(f'  Antes → teams: {count("SELECT COUNT(*) FROM teams")} · players: {count("SELECT COUNT(*) FROM players")}')

        # 2) IDs protegidos: referenciados por la BD ∪ los del Excel nuevo
        cur.execute(f'SELECT id FROM ({REF_TEAMS_SQL})')
        ref_equipos = {x[0] for x in cur.fetchall()}
        cur.execute(f'SELECT id FROM ({REF_PLAYERS_SQL})')
        ref_jugadores = {x[0] for x in cur.fetchall()}

        ph_e = ','.join('?' * len(ids_equipos)) if ids_equipos else 'NULL'
        ph_j = ','.join('?' * len(ids_jugadores)) if ids_jugadores else 'NULL'

        sel_e = f'SELECT team_id, team_name FROM teams WHERE team_id NOT IN ({REF_TEAMS_SQL}) AND team_id NOT IN ({ph_e})'
        sel_j = f'SELECT player_id, nickname FROM players WHERE player_id NOT IN ({REF_PLAYERS_SQL}) AND player_id NOT IN ({ph_j})'

        cur.execute(sel_e, ids_equipos)
        borrar_eq = cur.fetchall()
        cur.execute(sel_j, ids_jugadores)
        borrar_j = cur.fetchall()

        print(f'  A borrar → teams: {len(borrar_eq)} · players: {len(borrar_j)}')
        print(f'  Protegidos (referenciados) → teams: {len(ref_equipos)} · players: {len(ref_jugadores)}')

        if args.dry_run:
            print('  (dry-run: no se borra nada)')
            preview = [str(r[1]) for r in borrar_eq[:15]]
            print('  Ej. de equipos a borrar:', ', '.join(preview))
            conn.commit()
            cur.close()
            return 0

        if not args.yes:
            resp = input('¿Borrar los no referenciados? [s/N]: ').strip().lower()
            if resp not in ('s', 'si', 'sí', 'y', 'yes'):
                print('Cancelado.')
                cur.close()
                return 0

        # 3) Upsert equipos/jugadores buenos (y placeholders para team_id que falten)
        etl.etl_teams(equipos, cur)
        etl.exec_batch(cur,
            "INSERT OR IGNORE INTO teams (team_id,team_name) VALUES ",
            [(etl.ii(r.get('team_id')), etl.ss(r.get('team_name')))
             for _, r in jugadores.iterrows() if etl.ii(r.get('team_id')) is not None], 2)
        etl.etl_players(jugadores, cur)

        # 4) Borrar solo lo no referenciado y no curado
        before_e = count('SELECT COUNT(*) FROM teams')
        before_j = count('SELECT COUNT(*) FROM players')
        cur.execute(f'DELETE FROM teams WHERE team_id NOT IN ({REF_TEAMS_SQL}) AND team_id NOT IN ({ph_e})', ids_equipos)
        cur.execute(f'DELETE FROM players WHERE player_id NOT IN ({REF_PLAYERS_SQL}) AND player_id NOT IN ({ph_j})', ids_jugadores)
        conn.commit()
        del_eq = before_e - count('SELECT COUNT(*) FROM teams')
        del_pj = before_j - count('SELECT COUNT(*) FROM players')

        print(f'  ✓ Borrados → teams: {del_eq} · players: {del_pj}')
        print(f'  Después → teams: {count("SELECT COUNT(*) FROM teams")} · players: {count("SELECT COUNT(*) FROM players")}')

        cur.execute('PRAGMA foreign_key_check')
        fk = cur.fetchall()
        print(f'  FK violations: {len(fk)}' + ('' if not fk else f'  {fk[:5]}'))

        cur.close()
        print('=' * 70)
        return 0
    finally:
        etl.release_conn(conn)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print('\nInterrumpido.')
        sys.exit(130)
