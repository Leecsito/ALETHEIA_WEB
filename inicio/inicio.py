"""
ALETHEIA — ETL Blueprint
Rutas: /api/init-db, /api/etl, /api/status
Para agregar más rutas ETL, editá solo este archivo.
"""

from flask import Blueprint, request, jsonify
import pandas as pd
import os
import re
import io
import threading
import uuid
import time
import traceback
from datetime import datetime
try:
    from backend.conexion import get_conn, release_conn
except ImportError:
    from conexion import get_conn, release_conn

inicio_bp = Blueprint('inicio', __name__)

# ─── JOBS ETL EN SEGUNDO PLANO ────────────────────────────────────────────────
# El ETL puede tardar más que el timeout del worker (30 s por defecto en Gunicorn).
# Por eso se ejecuta en un hilo y el cliente consulta el estado con /api/etl-status.
ETL_JOBS = {}
ETL_JOBS_LOCK = threading.Lock()
ETL_JOB_TTL = 3600  # segundos que se conserva un job finalizado

def _prune_jobs():
    now = time.time()
    with ETL_JOBS_LOCK:
        expired = [jid for jid, j in ETL_JOBS.items()
                   if j.get('finished') and now - j['finished'] > ETL_JOB_TTL]
        for jid in expired:
            ETL_JOBS.pop(jid, None)

def _job_set(job, **kw):
    if job is not None:
        job.update(kw)

# ─── SQL: CREAR TABLAS ────────────────────────────────────────────────────────
CREATE_TABLES_SQL = [
    """CREATE TABLE IF NOT EXISTS matches (
        match_id    INTEGER PRIMARY KEY,
        tournament  TEXT,
        phase       TEXT,
        match_date  TEXT,
        team_a      TEXT,
        team_b      TEXT,
        score_a     INTEGER,
        score_b     INTEGER,
        winner      TEXT,
        patch       TEXT,
        team_a_id   INTEGER REFERENCES teams(team_id),
        team_b_id   INTEGER REFERENCES teams(team_id)
    )""",
    """CREATE TABLE IF NOT EXISTS match_veto (
        veto_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id    INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
        action      TEXT,
        team        TEXT,
        map_name    TEXT,
        veto_order  INTEGER,
        team_id     INTEGER REFERENCES teams(team_id)
    )""",
    """CREATE TABLE IF NOT EXISTS maps (
        map_id          TEXT PRIMARY KEY,
        match_id        INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
        map_name        TEXT,
        map_number      INTEGER,
        picker          TEXT,
        side_chosen     TEXT,
        side_top_start  TEXT,
        score_a_attack  INTEGER,
        score_a_defense INTEGER,
        score_b_attack  INTEGER,
        score_b_defense INTEGER,
        duration        TEXT,
        picker_id       INTEGER REFERENCES teams(team_id)
    )""",
    """CREATE TABLE IF NOT EXISTS rounds (
        map_id          TEXT REFERENCES maps(map_id) ON DELETE CASCADE,
        round_num       INTEGER,
        winner          TEXT,
        result_type     TEXT,
        winning_side    TEXT,
        team_top        TEXT,
        bank_top        INTEGER,
        spend_top       INTEGER,
        category_top    TEXT,
        team_bot        TEXT,
        bank_bot        INTEGER,
        spend_bot       INTEGER,
        category_bot    TEXT,
        PRIMARY KEY (map_id, round_num)
    )""",
    """CREATE TABLE IF NOT EXISTS player_stats (
        stat_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id    INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
        map_id      TEXT REFERENCES maps(map_id) ON DELETE CASCADE,
        player_name TEXT,
        team_name   TEXT,
        side        TEXT,
        agent       TEXT,
        rating      REAL,
        acs         INTEGER,
        kills       INTEGER,
        deaths      INTEGER,
        assists     INTEGER,
        kast        REAL,
        adr         REAL,
        hs_percent  REAL,
        fk          INTEGER,
        fd          INTEGER,
        player_id   INTEGER REFERENCES players(player_id),
        team_id     INTEGER REFERENCES teams(team_id)
    )""",
    """CREATE TABLE IF NOT EXISTS economy_summary (
        econ_id         INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id        INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
        map_id          TEXT REFERENCES maps(map_id) ON DELETE CASCADE,
        team            TEXT,
        pistol_won      INTEGER,
        eco_played      INTEGER,
        eco_won         INTEGER,
        semi_eco_played INTEGER,
        semi_eco_won    INTEGER,
        semi_buy_played INTEGER,
        semi_buy_won    INTEGER,
        full_buy_played INTEGER,
        full_buy_won    INTEGER,
        team_id         INTEGER REFERENCES teams(team_id)
    )""",
    """CREATE TABLE IF NOT EXISTS duels (
        duel_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id    INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
        map_id      TEXT REFERENCES maps(map_id) ON DELETE CASCADE,
        duel_type   TEXT,
        player_a    TEXT,
        player_b    TEXT,
        kills_a     INTEGER,
        kills_b     INTEGER,
        player_a_id INTEGER REFERENCES players(player_id),
        player_b_id INTEGER REFERENCES players(player_id)
    )""",
    """CREATE TABLE IF NOT EXISTS multikills_clutches (
        mk_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id    INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
        map_id      TEXT REFERENCES maps(map_id) ON DELETE CASCADE,
        player_name TEXT,
        agent       TEXT,
        k2 INTEGER, k3 INTEGER, k4 INTEGER, k5 INTEGER,
        v1 INTEGER, v2 INTEGER, v3 INTEGER, v4 INTEGER, v5 INTEGER,
        econ_rating INTEGER,
        plants      INTEGER,
        defuses     INTEGER,
        player_id   INTEGER REFERENCES players(player_id)
    )""",
    """CREATE TABLE IF NOT EXISTS teams (
        team_id     INTEGER PRIMARY KEY,
        team_name   TEXT,
        region      TEXT,
        url         TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS players (
        player_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        nickname    TEXT,
        real_name   TEXT,
        team_id     INTEGER REFERENCES teams(team_id) ON DELETE SET NULL,
        team_name   TEXT
    )""",
]

# ─── MIGRACIONES ─────────────────────────────────────────────────────────────
# Columnas *_id agregadas en la migración a modelo relacional (FK por id).
ID_COLUMNS = [
    ('matches',              'team_a_id',   'INTEGER REFERENCES teams(team_id)'),
    ('matches',              'team_b_id',   'INTEGER REFERENCES teams(team_id)'),
    ('match_veto',           'team_id',     'INTEGER REFERENCES teams(team_id)'),
    ('maps',                 'picker_id',   'INTEGER REFERENCES teams(team_id)'),
    ('player_stats',         'player_id',   'INTEGER REFERENCES players(player_id)'),
    ('player_stats',         'team_id',     'INTEGER REFERENCES teams(team_id)'),
    ('economy_summary',      'team_id',     'INTEGER REFERENCES teams(team_id)'),
    ('duels',                'player_a_id', 'INTEGER REFERENCES players(player_id)'),
    ('duels',                'player_b_id', 'INTEGER REFERENCES players(player_id)'),
    ('multikills_clutches',  'player_id',   'INTEGER REFERENCES players(player_id)'),
]

def table_columns(cur, table):
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}

def ensure_column(cur, table, column, decl):
    if column not in table_columns(cur, table):
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
        except Exception:
            pass

def run_migrations(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(maps)")
    map_cols = {row[1] for row in cur.fetchall()}

    if 'score_a_h1' in map_cols:
        try:
            cur.execute("ALTER TABLE maps RENAME COLUMN score_a_h1 TO score_a_attack")
            cur.execute("ALTER TABLE maps RENAME COLUMN score_a_h2 TO score_a_defense")
            cur.execute("ALTER TABLE maps RENAME COLUMN score_b_h1 TO score_b_attack")
            cur.execute("ALTER TABLE maps RENAME COLUMN score_b_h2 TO score_b_defense")
        except Exception:
            pass

    if 'side_top_start' not in map_cols:
        try:
            cur.execute("ALTER TABLE maps ADD COLUMN side_top_start TEXT")
        except Exception:
            pass

    cur.execute("PRAGMA table_info(rounds)")
    round_cols = {row[1] for row in cur.fetchall()}
    if 'is_pistol' in round_cols:
        try:
            cur.execute("ALTER TABLE rounds DROP COLUMN is_pistol")
        except Exception:
            pass

    # Columnas relacionales por id
    for table, column, decl in ID_COLUMNS:
        ensure_column(cur, table, column, decl)

    cur.close()

# ─── HELPERS ETL ──────────────────────────────────────────────────────────────
MAP_NAMES = {'abyss','bind','breeze','corrode','haven','pearl','split','lotus','icebox','fracture','sunset','ascent','summit'}
SIDES     = {'attack','defense'}

ABBREV_MAP = {
    'EG':'Evil Geniuses','C9':'Cloud9','LEV':'LEVIATAN','100T':'100 Thieves',
    'KRU':'KRU Esports','FUR':'FURIA','NRG':'NRG','MIBR':'MIBR','LOUD':'LOUD',
    'ENVY':'ENVY','SEN':'Sentinels','G2':'G2 Esports','M8':'Gentle Mates',
    'FNC':'Fnatic','BBL':'BBL Esports','EDG':'EDward Gaming','BLG':'Bilibili Gaming',
    'PRX':'Paper Rex','DRX':'DRX','T1':'T1','GEN':'Gen.G','ZETA':'ZETA DIVISION',
}

def parse_score_match(s):
    try:
        a, b = str(s).split('-'); return int(a), int(b)
    except: return 0, 0

def parse_score_half(s):
    try:
        a, b = str(s).split('/'); return int(a), int(b)
    except: return 0, 0

def parse_eco(s):
    m = re.match(r'(\d+)\((\d+)\)', str(s))
    return (int(m.group(1)), int(m.group(2))) if m else (0, 0)

def si(v, d=0):
    """Safe int — convierte a int manejando NaN, None y strings vacíos."""
    try:
        if v is None: return d
        if isinstance(v, float) and pd.isna(v): return d
        return int(v)
    except (ValueError, TypeError):
        return d

def sf(v, d=0.0):
    """Safe float — convierte a float manejando NaN, None y strings vacíos."""
    try:
        if v is None: return d
        if isinstance(v, float) and pd.isna(v): return d
        return float(v)
    except (ValueError, TypeError):
        return d

def ii(v):
    """Safe int-or-None — como si() pero preserva NULL para columnas id/FK."""
    if v is None: return None
    try:
        if pd.isna(v): return None
    except Exception:
        pass
    try:
        return int(v)
    except (ValueError, TypeError, OverflowError):
        return None

def ss(v):
    """Safe string-or-None — evita guardar el literal 'nan'."""
    if v is None: return None
    try:
        if pd.isna(v): return None
    except Exception:
        pass
    s = str(v).strip()
    return s if s and s.lower() != 'nan' else None

def exec_batch(cur, sql_prefix, rows, cols, suffix=''):
    """
    Inserción por lotes multi-fila: '(?,?,...),(?,?,...)'.
    Reduce drásticamente los round-trips contra Turso remoto frente a executemany.
    """
    if not rows:
        return
    chunk = max(1, 900 // max(1, cols))  # ~900 parámetros por sentencia (límite seguro)
    for i in range(0, len(rows), chunk):
        part = rows[i:i + chunk]
        placeholders = ",".join(["(" + ",".join(["?"] * cols) + ")"] * len(part))
        cur.execute(sql_prefix + placeholders + suffix, [v for r_ in part for v in r_])

def parse_kills(s):
    try:
        a, b = str(s).split('/'); return int(a), int(b)
    except: return 0, 0

def parse_date(s, fallback_year=None):
    """
    Detecta el año dentro del string si existe ('July 16, 2025' → 2025).
    Si no hay año ('Saturday, August 23'), usa fallback_year.
    """
    if not s or str(s).strip() in ('nan', 'None', ''):
        return None
    raw = str(s).strip()
    year_match = re.search(r'\b(20\d{2})\b', raw)
    year = int(year_match.group(1)) if year_match else fallback_year
    if not year:
        return None
    # Quitar día de semana del inicio: "Saturday, August 23" → "August 23"
    clean = re.sub(r'^[A-Za-z]+,\s*', '', raw).strip()
    # Quitar año si ya estaba en el string para no duplicarlo
    clean_no_year = re.sub(r',?\s*20\d{2}', '', clean).strip()
    for fmt in ("%B %d", "%B %d,"):
        try:
            return datetime.strptime(f"{clean_no_year} {year}", f"{fmt} %Y").strftime("%Y-%m-%d")
        except:
            continue
    return None

def clean_map_id(s):
    m = re.match(r'(\d+_[a-z]+?)(?=pick|\d|$|-)', str(s).lower())
    return m.group(1) if m else str(s)

def normalize_winner(name):
    if not name: return name
    upper = str(name).upper().strip()
    return ABBREV_MAP.get(upper, str(name).strip())

def resolve_map_row(pick_a, pick_b):
    a_raw, b_raw = ss(pick_a), ss(pick_b)
    a = a_raw.lower() if a_raw else ''
    b = b_raw.lower() if b_raw else ''
    if a == 'decider': return 'decider', None, None
    if a in MAP_NAMES: return 'a', a_raw.capitalize(), (b if b in SIDES else None)
    if a in SIDES:     return 'b', (b_raw.capitalize() if b_raw else None), (a if a in SIDES else None)
    return 'unknown', a_raw, (b if b in SIDES else None)

def safe_nan(v):
    if v is None: return None
    try:
        if pd.isna(v): return None
    except Exception: pass
    return v

# ─── ETL FUNCTIONS ────────────────────────────────────────────────────────────
def etl_matches(df, cur):
    rows, veto_rows = [], []

    # Inferir año de fallback: del nombre del torneo o de las fechas con año
    fallback_year = None
    torneo_year = re.search(r'\b(20\d{2})\b', str(df['torneo'].iloc[0]) if len(df) > 0 else '')
    if torneo_year:
        fallback_year = int(torneo_year.group(1))
    else:
        for f in df['fecha'].dropna():
            ym = re.search(r'\b(20\d{2})\b', str(f))
            if ym:
                fallback_year = int(ym.group(1))
                break

    for _, r in df.iterrows():
        sa, sb = parse_score_match(r['score'])
        winner = r['equipo_a'] if sa > sb else r['equipo_b']
        patch  = str(r['patch']).replace('Patch ', '').strip() if pd.notna(r.get('patch')) else None
        a_id   = ii(r.get('equipo_a_id'))
        b_id   = ii(r.get('equipo_b_id'))
        rows.append((int(r['match_id']), str(r['torneo']), str(r['fase']),
                     parse_date(r['fecha'], fallback_year), str(r['equipo_a']), str(r['equipo_b']),
                     sa, sb, winner, patch, a_id, b_id))
        mid   = int(r['match_id'])
        order = [1]
        def add_veto(val, action, team, team_id):
            if pd.isna(val) or str(val).strip() == '': return
            for mn in str(val).split(','):
                mn = mn.strip()
                if mn: veto_rows.append((mid, action, team, mn, order[0], team_id)); order[0] += 1
        add_veto(r.get('pick_a'),'pick','a', a_id); add_veto(r.get('pick_b'),'pick','b', b_id)
        add_veto(r.get('ban_a'),'ban','a', a_id);   add_veto(r.get('ban_b'),'ban','b', b_id)
        add_veto(r.get('decider'),'decider',None,None)
    exec_batch(cur,
        "INSERT OR IGNORE INTO matches (match_id,tournament,phase,match_date,team_a,team_b,score_a,score_b,winner,patch,team_a_id,team_b_id) VALUES ",
        rows, 12)
    exec_batch(cur,
        "INSERT INTO match_veto (match_id,action,team,map_name,veto_order,team_id) VALUES ",
        veto_rows, 6)
    return len(rows)


def etl_maps(df, cur, match_teams):
    rows = []
    for map_num, ((mid, round_id), group) in enumerate(df.groupby(['match_id','round_id'], sort=False), 1):
        r = group.iloc[0]
        picker, map_name, side_chosen = resolve_map_row(r['pick_a'], r['pick_b'])
        ah1, ah2 = parse_score_half(r['score_a'])
        bh1, bh2 = parse_score_half(r['score_b'])
        # El round_id (map_id) SIEMPRE contiene el nombre real del mapa (ej: 542200_corrode).
        sufijo = str(round_id).split('_', 1)[1] if '_' in str(round_id) else ''
        m = re.match(r'[A-Za-z]+', sufijo)
        if m:
            map_name = m.group(0).capitalize()
        elif not map_name:
            map_name = None
        raw_side = safe_nan(r.get('side_top_start', None))
        side_top_start = str(raw_side).strip() or None if raw_side is not None else None
        a_id, b_id = match_teams.get(int(mid), (None, None))
        picker_id = a_id if picker == 'a' else (b_id if picker == 'b' else None)
        rows.append((str(round_id), int(mid), map_name, map_num,
                     picker, side_chosen, side_top_start, ah1, ah2, bh1, bh2,
                     str(r.get('time','')), picker_id))
    exec_batch(cur,
        """INSERT OR IGNORE INTO maps
           (map_id,match_id,map_name,map_number,picker,side_chosen,side_top_start,
            score_a_attack,score_a_defense,score_b_attack,score_b_defense,duration,picker_id) VALUES """,
        rows, 13)
    return len(rows)


def etl_rounds(df_rondas, df_eco, cur, team_names):
    # df_eco puede ser None si no se subió el archivo de economía
    eco_lookup = {}
    if df_eco is not None and not df_eco.empty:
        eco_lookup = {(str(r['map_id']), int(r['round'])): r for _, r in df_eco.iterrows()}
    rows = []
    for _, r in df_rondas.iterrows():
        mid = str(r['round_id'])
        num = int(r['num'])
        eco = eco_lookup.get((mid, num), {})
        def eg(field, default=None):
            v = eco.get(field, default) if hasattr(eco, 'get') else default
            return None if (v is None or (hasattr(v, '__float__') and pd.isna(v))) else v
        win_id = ii(r.get('win'))
        if win_id is not None and win_id in team_names:
            winner_full = team_names[win_id]
        else:
            winner_full = normalize_winner(eg('winner')) or str(r['win']).strip()
        rows.append((mid, num, winner_full,
                     str(r.get('result','')), str(r.get('band','')),
                     str(eg('team_top','') or ''), int(eg('bank_top',0) or 0),
                     int(eg('spend_top',0) or 0), str(eg('category_top','') or ''),
                     str(eg('team_bot','') or ''), int(eg('bank_bot',0) or 0),
                     int(eg('spend_bot',0) or 0), str(eg('category_bot','') or '')))
    exec_batch(cur,
        """INSERT OR IGNORE INTO rounds
           (map_id,round_num,winner,result_type,winning_side,
            team_top,bank_top,spend_top,category_top,
            team_bot,bank_bot,spend_bot,category_bot) VALUES """,
        rows, 13)
    return len(rows)


def etl_stats(df, cur):
    rows = [(si(r['match_id']), clean_map_id(r['map_id']),
             ss(r.get('player_name')), ss(r.get('team_name')),
             str(r['side']).lower() if pd.notna(r.get('side')) else None, ss(r.get('agent')),
             sf(r['rating']), si(r['acs']), si(r['kills']),
             si(r['deaths']), si(r['assists']), sf(r['kast']),
             sf(r['adr']), sf(r['hs_percent']), si(r['fk']), si(r['fd']),
             ii(r.get('player_id')), ii(r.get('team_id')))
            for _, r in df.iterrows()]
    exec_batch(cur,
        "INSERT INTO player_stats (match_id,map_id,player_name,team_name,side,agent,rating,acs,kills,deaths,assists,kast,adr,hs_percent,fk,fd,player_id,team_id) VALUES ",
        rows, 18)
    return len(rows)


def etl_economy_summary(df, cur):
    rows = []
    for _, r in df.iterrows():
        ep,ew=parse_eco(r['eco']); sep,sew=parse_eco(r['semi_eco'])
        sbp,sbw=parse_eco(r['semi_buy']); fbp,fbw=parse_eco(r['full_buy'])
        rows.append((si(r['match_id']), str(r['map_id']), str(r['team']),
                     si(r['pistol_won']), ep,ew,sep,sew,sbp,sbw,fbp,fbw,
                     ii(r.get('team_id'))))
    exec_batch(cur,
        "INSERT INTO economy_summary (match_id,map_id,team,pistol_won,eco_played,eco_won,semi_eco_played,semi_eco_won,semi_buy_played,semi_buy_won,full_buy_played,full_buy_won,team_id) VALUES ",
        rows, 13)
    return len(rows)


def etl_duels(df, cur, nick_to_pid):
    rows = []
    for _, r in df.iterrows():
        ka, kb = parse_kills(r['kills'])
        rows.append((int(r['match_id']), str(r['map_id']), str(r['tipo_kill']),
                     str(r['player_a']), str(r['player_b']), ka, kb,
                     nick_to_pid.get(str(r['player_a'])), nick_to_pid.get(str(r['player_b']))))
    exec_batch(cur,
        "INSERT INTO duels (match_id,map_id,duel_type,player_a,player_b,kills_a,kills_b,player_a_id,player_b_id) VALUES ",
        rows, 9)
    return len(rows)


def etl_multikills(df, cur, nick_to_pid):
    rows = []
    for _, r in df.iterrows():
        rows.append((si(r['match_id']), str(r['map_id']), str(r['player_name']), str(r['agent']),
                     si(r.get('k2')),si(r.get('k3')),si(r.get('k4')),si(r.get('k5')),
                     si(r.get('v1')),si(r.get('v2')),si(r.get('v3')),si(r.get('v4')),si(r.get('v5')),
                     si(r.get('econ')),si(r.get('pl')),si(r.get('de')),
                     nick_to_pid.get(str(r['player_name']))))
    exec_batch(cur,
        "INSERT INTO multikills_clutches (match_id,map_id,player_name,agent,k2,k3,k4,k5,v1,v2,v3,v4,v5,econ_rating,plants,defuses,player_id) VALUES ",
        rows, 17)
    return len(rows)


def etl_teams(df, cur):
    rows = [(ii(r['team_id']), ss(r.get('team_name')), ss(r.get('region')), ss(r.get('url')))
            for _, r in df.iterrows() if ii(r.get('team_id')) is not None]
    exec_batch(cur,
        "INSERT INTO teams (team_id,team_name,region,url) VALUES ",
        rows, 4,
        suffix=""" ON CONFLICT(team_id) DO UPDATE SET
               team_name = COALESCE(excluded.team_name, teams.team_name),
               region    = COALESCE(excluded.region, teams.region),
               url       = COALESCE(excluded.url, teams.url)""")
    return len(rows)


def etl_players(df, cur):
    rows = [(ii(r['player_id']), ss(r.get('nickname')), ss(r.get('real_name')),
             ii(r.get('team_id')), ss(r.get('team_name')))
            for _, r in df.iterrows() if ii(r.get('player_id')) is not None]
    exec_batch(cur,
        "INSERT INTO players (player_id,nickname,real_name,team_id,team_name) VALUES ",
        rows, 5,
        suffix=""" ON CONFLICT(player_id) DO UPDATE SET
               nickname  = COALESCE(excluded.nickname, players.nickname),
               real_name = COALESCE(excluded.real_name, players.real_name),
               team_id   = COALESCE(excluded.team_id, players.team_id),
               team_name = COALESCE(excluded.team_name, players.team_name)""")
    return len(rows)


# ─── RUTAS ────────────────────────────────────────────────────────────────────
@inicio_bp.route('/api/init-db', methods=['POST'])
def init_db():
    try:
        conn = get_conn()
        try:
            cur = conn.cursor()
            for sql in CREATE_TABLES_SQL:
                cur.execute(sql)
            run_migrations(conn)
            conn.commit()
            cur.close()
        finally:
            release_conn(conn)
        return jsonify({"ok": True, "message": "Tablas creadas correctamente."})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _process_etl(raw_files, job=None):
    """Procesa los Excel (bytes) e inserta en la BD. Pensado para correr en un hilo."""
    files = {}
    for name, blob in raw_files.items():
        files[name] = pd.read_excel(io.BytesIO(blob))

    conn = get_conn()
    try:
        cur = conn.cursor()
        results = {}

        # ── 1) TEAMS (primero, para satisfacer FKs de matches/maps/economy) ──
        _job_set(job, step='equipos')
        team_names = {}   # team_id -> team_name
        if 'vct_equipos' in files:
            results['teams'] = etl_teams(files['vct_equipos'], cur)
            for _, r in files['vct_equipos'].iterrows():
                tid = ii(r.get('team_id'))
                if tid is not None:
                    team_names[tid] = ss(r.get('team_name'))

        # Fallback: equipos referenciados por partidos y jugadores (garantiza FKs)
        for _, r in files['vct_partidos'].iterrows():
            for id_col, name_col in (('equipo_a_id','equipo_a'), ('equipo_b_id','equipo_b')):
                tid = ii(r.get(id_col))
                if tid is not None and tid not in team_names:
                    team_names[tid] = ss(r.get(name_col))
        if 'vct_jugadores' in files:
            for _, r in files['vct_jugadores'].iterrows():
                tid = ii(r.get('team_id'))
                if tid is not None and tid not in team_names:
                    team_names[tid] = ss(r.get('team_name'))
        exec_batch(cur,
            "INSERT OR IGNORE INTO teams (team_id,team_name) VALUES ",
            [(tid, name) for tid, name in team_names.items() if tid is not None], 2)

        # ── 2) PLAYERS (antes de player_stats/duels/multikills) ──
        _job_set(job, step='jugadores')
        nick_to_pid = {}  # nickname -> player_id
        if 'vct_jugadores' in files:
            results['players'] = etl_players(files['vct_jugadores'], cur)

        # Derivar/rellenar jugadores desde stats (cubre los ausentes en vct_jugadores)
        if 'vlr_stats_players_sides' in files:
            df_st = files['vlr_stats_players_sides']
            player_rows = []
            seen = set()
            for _, r in df_st.iterrows():
                pid  = ii(r.get('player_id'))
                nick = ss(r.get('player_name'))
                if pid is None or nick is None or pid in seen:
                    continue
                seen.add(pid)
                player_rows.append((pid, nick, ii(r.get('team_id')), ss(r.get('team_name'))))
            exec_batch(cur,
                "INSERT OR IGNORE INTO players (player_id,nickname,team_id,team_name) VALUES ",
                player_rows, 4)

        # Mapa nickname -> player_id (fuente principal: stats; refuerzo: vct_jugadores)
        if 'vlr_stats_players_sides' in files:
            for _, r in files['vlr_stats_players_sides'].iterrows():
                pid, nick = ii(r.get('player_id')), ss(r.get('player_name'))
                if pid is not None and nick:
                    nick_to_pid[nick] = pid
        if 'vct_jugadores' in files:
            for _, r in files['vct_jugadores'].iterrows():
                pid, nick = ii(r.get('player_id')), ss(r.get('nickname'))
                if pid is not None and nick:
                    nick_to_pid.setdefault(nick, pid)

        # match_id -> (team_a_id, team_b_id) para picker_id / veto / etc.
        match_teams = {}
        for _, r in files['vct_partidos'].iterrows():
            match_teams[int(r['match_id'])] = (ii(r.get('equipo_a_id')), ii(r.get('equipo_b_id')))

        # Partidos referenciados por otros archivos pero ausentes en vct_partidos:
        # se crea un partido mínimo para no romper las FKs y no perder los datos.
        referenciados = set()
        for key in ('vlr_mapas','vlr_stats_players_sides','vlr_economia_resumen',
                    'vlr_economia_rondas','vlr_enfrentamientos','vlr_multikills_clutches'):
            df = files.get(key)
            if df is not None and 'match_id' in df.columns:
                for v in df['match_id'].dropna():
                    mid = ii(v)
                    if mid is not None:
                        referenciados.add(mid)
        huerfanos = sorted(referenciados - set(match_teams.keys()))
        if huerfanos:
            torneo = ss(files['vct_partidos']['torneo'].iloc[0]) if len(files['vct_partidos']) else None
            exec_batch(cur,
                "INSERT OR IGNORE INTO matches (match_id,tournament) VALUES ",
                [(mid, torneo) for mid in huerfanos], 2)
            for mid in huerfanos:
                match_teams.setdefault(mid, (None, None))
        results['orphan_matches'] = len(huerfanos)

        # ── 3) RESTO DE TABLAS ──
        _job_set(job, step='partidos')
        results['matches'] = etl_matches(files['vct_partidos'], cur)

        if 'vlr_mapas' in files:
            _job_set(job, step='mapas')
            results['maps'] = etl_maps(files['vlr_mapas'], cur, match_teams)

        if 'vlr_rondas' in files:
            _job_set(job, step='rondas')
            df_eco = files.get('vlr_economia_rondas')  # puede ser None
            results['rounds'] = etl_rounds(files['vlr_rondas'], df_eco, cur, team_names)

        if 'vlr_stats_players_sides' in files:
            _job_set(job, step='stats de jugadores')
            results['player_stats'] = etl_stats(files['vlr_stats_players_sides'], cur)

        if 'vlr_economia_resumen' in files:
            _job_set(job, step='economía')
            results['economy_summary'] = etl_economy_summary(files['vlr_economia_resumen'], cur)

        if 'vlr_enfrentamientos' in files:
            _job_set(job, step='enfrentamientos')
            results['duels'] = etl_duels(files['vlr_enfrentamientos'], cur, nick_to_pid)

        if 'vlr_multikills_clutches' in files:
            _job_set(job, step='multikills y clutches')
            results['multikills'] = etl_multikills(files['vlr_multikills_clutches'], cur, nick_to_pid)

        conn.commit()
        cur.close()
        return results
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        release_conn(conn)


@inicio_bp.route('/api/etl', methods=['POST'])
def run_etl():
    # Solo vct_partidos es obligatorio (es la "espina dorsal" de match_id)
    # El resto son opcionales — útil para torneos con datos incompletos (ej. China)
    all_files = [
        'vct_partidos', 'vlr_mapas', 'vlr_rondas', 'vlr_economia_rondas',
        'vlr_stats_players_sides', 'vlr_economia_resumen',
        'vlr_enfrentamientos', 'vlr_multikills_clutches',
        'vct_equipos', 'vct_jugadores'
    ]

    if not request.files.get('vct_partidos'):
        return jsonify({"ok": False, "error": "vct_partidos.xlsx es obligatorio."}), 400

    # Leer bytes (rápido) y delegar el parseo + ETL al hilo en segundo plano,
    # para que la petición HTTP nunca supere el timeout del worker.
    raw_files = {}
    for name in all_files:
        f = request.files.get(name)
        if f:
            raw_files[name] = f.read()

    _prune_jobs()
    job_id = uuid.uuid4().hex
    job = {"id": job_id, "status": "running", "step": "subiendo",
           "results": None, "error": None, "trace": None,
           "started": time.time(), "finished": None}
    with ETL_JOBS_LOCK:
        ETL_JOBS[job_id] = job

    def _worker():
        try:
            results = _process_etl(raw_files, job)
            _job_set(job, status='done', results=results, step='completado', finished=time.time())
        except Exception as e:
            _job_set(job, status='error', error=str(e),
                     trace=traceback.format_exc(), finished=time.time())

    threading.Thread(target=_worker, daemon=True).start()
    return jsonify({"ok": True, "async": True, "job_id": job_id}), 202


@inicio_bp.route('/api/etl-status/<job_id>', methods=['GET'])
def etl_status(job_id):
    job = ETL_JOBS.get(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Trabajo no encontrado o expirado."}), 404
    return jsonify({
        "ok": True,
        "status": job.get("status"),
        "step": job.get("step"),
        "progress": job.get("progress"),
        "inserted": job.get("results"),
        "error": job.get("error"),
        "trace": job.get("trace"),
    })


@inicio_bp.route('/api/etl-batch', methods=['POST'])
def run_etl_batch():
    """
    Recibe archivos con su ruta relativa (subida de carpetas vía webkitdirectory),
    los agrupa por carpeta de torneo y ejecuta el ETL de cada uno.
    Los archivos .xlsx en la raíz son globales (equipos / jugadores).
    """
    uploaded = request.files.getlist('files')
    if not uploaded:
        return jsonify({"ok": False, "error": "No se recibieron archivos."}), 400

    globales = {}
    carpetas = {}
    for f in uploaded:
        rel = (f.filename or '').replace('\\', '/').strip()
        if not rel.lower().endswith('.xlsx'):
            continue
        parts = [p for p in rel.split('/') if p not in ('', '.', '..')]
        if not parts:
            continue
        base = os.path.splitext(parts[-1])[0].lower()
        if len(parts) <= 2:
            # Archivo directamente bajo la carpeta raíz seleccionada -> global
            globales[base] = f.read()
        else:
            # Archivo dentro de una subcarpeta (torneo) -> su carpeta inmediata
            carpetas.setdefault(parts[-2], {})[base] = f.read()

    # Si el usuario seleccionó una sola carpeta de torneo (archivos en la raíz)
    if not carpetas and 'vct_partidos' in globales:
        carpetas['torneo'] = globales
        globales = {}

    if not carpetas:
        return jsonify({"ok": False, "error": "No se detectaron subcarpetas de torneos."}), 400

    _prune_jobs()
    job_id = uuid.uuid4().hex
    job = {"id": job_id, "status": "running", "step": "preparando",
           "progress": {"done": 0, "total": len(carpetas)},
           "results": None, "error": None, "trace": None,
           "started": time.time(), "finished": None}
    with ETL_JOBS_LOCK:
        ETL_JOBS[job_id] = job

    def _worker():
        try:
            total = len(carpetas)
            out = {}
            for i, (nombre, archivos) in enumerate(sorted(carpetas.items()), 1):
                raw = dict(globales)
                raw.update(archivos)
                _job_set(job, step=f'torneo {i}/{total}: {nombre}',
                         progress={"done": i - 1, "total": total})
                if 'vct_partidos' not in raw:
                    out[nombre] = {"skipped": "sin vct_partidos.xlsx"}
                    continue
                try:
                    out[nombre] = _process_etl(raw, None)
                except Exception as e:
                    out[nombre] = {"error": str(e)}
            _job_set(job, status='done', results=out, step='completado',
                     progress={"done": total, "total": total}, finished=time.time())
        except Exception as e:
            _job_set(job, status='error', error=str(e),
                     trace=traceback.format_exc(), finished=time.time())

    threading.Thread(target=_worker, daemon=True).start()
    return jsonify({"ok": True, "async": True, "job_id": job_id, "torneos": len(carpetas)}), 202


@inicio_bp.route('/api/status', methods=['GET'])
def status():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        tables = ['matches','match_veto','maps','rounds','player_stats',
                  'economy_summary','duels','multikills_clutches','teams','players']
        counts = {}
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                counts[t] = cur.fetchone()[0]
            except Exception:
                counts[t] = 0
        cur.close()
        release_conn(conn)
        return jsonify({"ok": True, "tables": counts})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500