"""
ALETHEIA — ETL Blueprint
Rutas: /api/init-db, /api/etl, /api/status
Para agregar más rutas ETL, editá solo este archivo.
"""

from flask import Blueprint, request, jsonify
import pandas as pd
import re
import io
from datetime import datetime
try:
    from backend.conexion import get_conn, release_conn
except ImportError:
    from conexion import get_conn, release_conn

inicio_bp = Blueprint('inicio', __name__)

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
        patch       TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS match_veto (
        veto_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id    INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
        action      TEXT,
        team        TEXT,
        map_name    TEXT,
        veto_order  INTEGER
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
        duration        TEXT
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
        fd          INTEGER
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
        full_buy_won    INTEGER
    )""",
    """CREATE TABLE IF NOT EXISTS duels (
        duel_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id    INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
        map_id      TEXT REFERENCES maps(map_id) ON DELETE CASCADE,
        duel_type   TEXT,
        player_a    TEXT,
        player_b    TEXT,
        kills_a     INTEGER,
        kills_b     INTEGER
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
        defuses     INTEGER
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

    cur.close()

# ─── HELPERS ETL ──────────────────────────────────────────────────────────────
MAP_NAMES = {'abyss','bind','breeze','corrode','haven','pearl','split','lotus','icebox','fracture','sunset','ascent'}
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
    a = str(pick_a).lower().strip()
    b = str(pick_b).lower().strip()
    if a == 'decider': return 'decider', None, None
    if a in MAP_NAMES: return 'a', pick_a.capitalize(), b if b in SIDES else None
    if a in SIDES:     return 'b', pick_b.capitalize(), a if a in SIDES else None
    return 'unknown', pick_a, pick_b

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
        rows.append((int(r['match_id']), str(r['torneo']), str(r['fase']),
                     parse_date(r['fecha'], fallback_year), str(r['equipo_a']), str(r['equipo_b']),
                     sa, sb, winner, patch))
        mid   = int(r['match_id'])
        order = [1]
        def add_veto(val, action, team):
            if pd.isna(val) or str(val).strip() == '': return
            for mn in str(val).split(','):
                mn = mn.strip()
                if mn: veto_rows.append((mid, action, team, mn, order[0])); order[0] += 1
        add_veto(r.get('pick_a'),'pick','a'); add_veto(r.get('pick_b'),'pick','b')
        add_veto(r.get('ban_a'),'ban','a');   add_veto(r.get('ban_b'),'ban','b')
        add_veto(r.get('decider'),'decider',None)
    cur.executemany(
        "INSERT OR IGNORE INTO matches (match_id,tournament,phase,match_date,team_a,team_b,score_a,score_b,winner,patch) VALUES (?,?,?,?,?,?,?,?,?,?)",
        rows)
    cur.executemany(
        "INSERT INTO match_veto (match_id,action,team,map_name,veto_order) VALUES (?,?,?,?,?)",
        veto_rows)
    return len(rows)


def etl_maps(df, cur):
    rows = []
    for map_num, ((mid, round_id), group) in enumerate(df.groupby(['match_id','round_id'], sort=False), 1):
        r = group.iloc[0]
        picker, map_name, side_chosen = resolve_map_row(r['pick_a'], r['pick_b'])
        ah1, ah2 = parse_score_half(r['score_a'])
        bh1, bh2 = parse_score_half(r['score_b'])
        if not map_name:
            map_name = str(round_id).split('_')[1].capitalize() if '_' in str(round_id) else None
        raw_side = safe_nan(r.get('side_top_start', None))
        side_top_start = str(raw_side).strip() or None if raw_side is not None else None
        rows.append((str(round_id), int(mid), map_name, map_num,
                     picker, side_chosen, side_top_start, ah1, ah2, bh1, bh2,
                     str(r.get('time',''))))
    cur.executemany(
        """INSERT OR IGNORE INTO maps
           (map_id,match_id,map_name,map_number,picker,side_chosen,side_top_start,
            score_a_attack,score_a_defense,score_b_attack,score_b_defense,duration)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows)
    return len(rows)


def etl_rounds(df_rondas, df_eco, cur):
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
        winner_full = normalize_winner(eg('winner')) or str(r['win']).strip()
        rows.append((mid, num, winner_full,
                     str(r.get('result','')), str(r.get('band','')),
                     str(eg('team_top','') or ''), int(eg('bank_top',0) or 0),
                     int(eg('spend_top',0) or 0), str(eg('category_top','') or ''),
                     str(eg('team_bot','') or ''), int(eg('bank_bot',0) or 0),
                     int(eg('spend_bot',0) or 0), str(eg('category_bot','') or '')))
    cur.executemany(
        """INSERT OR IGNORE INTO rounds
           (map_id,round_num,winner,result_type,winning_side,
            team_top,bank_top,spend_top,category_top,
            team_bot,bank_bot,spend_bot,category_bot)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows)
    return len(rows)


def etl_stats(df, cur):
    rows = [(si(r['match_id']), clean_map_id(r['map_id']),
             str(r['player_name']), str(r['team_name']), str(r['side']).lower(), str(r['agent']),
             sf(r['rating']), si(r['acs']), si(r['kills']),
             si(r['deaths']), si(r['assists']), sf(r['kast']),
             sf(r['adr']), sf(r['hs_percent']), si(r['fk']), si(r['fd']))
            for _, r in df.iterrows()]
    cur.executemany(
        "INSERT INTO player_stats (match_id,map_id,player_name,team_name,side,agent,rating,acs,kills,deaths,assists,kast,adr,hs_percent,fk,fd) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows)
    return len(rows)


def etl_economy_summary(df, cur):
    rows = []
    for _, r in df.iterrows():
        ep,ew=parse_eco(r['eco']); sep,sew=parse_eco(r['semi_eco'])
        sbp,sbw=parse_eco(r['semi_buy']); fbp,fbw=parse_eco(r['full_buy'])
        rows.append((si(r['match_id']), str(r['map_id']), str(r['team']),
                     si(r['pistol_won']), ep,ew,sep,sew,sbp,sbw,fbp,fbw))
    cur.executemany(
        "INSERT INTO economy_summary (match_id,map_id,team,pistol_won,eco_played,eco_won,semi_eco_played,semi_eco_won,semi_buy_played,semi_buy_won,full_buy_played,full_buy_won) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        rows)
    return len(rows)


def etl_duels(df, cur):
    rows = []
    for _, r in df.iterrows():
        ka, kb = parse_kills(r['kills'])
        rows.append((int(r['match_id']), str(r['map_id']), str(r['tipo_kill']),
                     str(r['player_a']), str(r['player_b']), ka, kb))
    cur.executemany(
        "INSERT INTO duels (match_id,map_id,duel_type,player_a,player_b,kills_a,kills_b) VALUES (?,?,?,?,?,?,?)",
        rows)
    return len(rows)


def etl_multikills(df, cur):
    rows = []
    for _, r in df.iterrows():
        rows.append((si(r['match_id']), str(r['map_id']), str(r['player_name']), str(r['agent']),
                     si(r.get('k2')),si(r.get('k3')),si(r.get('k4')),si(r.get('k5')),
                     si(r.get('v1')),si(r.get('v2')),si(r.get('v3')),si(r.get('v4')),si(r.get('v5')),
                     si(r.get('econ')),si(r.get('pl')),si(r.get('de'))))
    cur.executemany(
        "INSERT INTO multikills_clutches (match_id,map_id,player_name,agent,k2,k3,k4,k5,v1,v2,v3,v4,v5,econ_rating,plants,defuses) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows)
    return len(rows)


def etl_teams(df, cur):
    rows = [(int(r['team_id']), str(r['team_name']), str(r['region']), str(r['url']))
            for _, r in df.iterrows()]
    cur.executemany(
        """INSERT INTO teams (team_id,team_name,region,url) VALUES (?,?,?,?)
           ON CONFLICT(team_id) DO UPDATE SET
               team_name = excluded.team_name,
               region    = excluded.region""",
        rows)
    return len(rows)


def etl_players(df, cur):
    rows = [(str(r['nickname']), str(r['real_name']), int(r['team_id']), str(r['team_name']))
            for _, r in df.iterrows()]
    cur.executemany(
        "INSERT INTO players (nickname,real_name,team_id,team_name) VALUES (?,?,?,?)",
        rows)
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

    files = {}
    for name in all_files:
        f = request.files.get(name)
        if f:
            files[name] = pd.read_excel(io.BytesIO(f.read()))

    try:
        conn = get_conn()
        cur  = conn.cursor()
        results = {}

        results['matches'] = etl_matches(files['vct_partidos'], cur)

        if 'vlr_mapas' in files:
            results['maps'] = etl_maps(files['vlr_mapas'], cur)

        if 'vlr_rondas' in files:
            df_eco = files.get('vlr_economia_rondas')  # puede ser None
            results['rounds'] = etl_rounds(files['vlr_rondas'], df_eco, cur)

        if 'vlr_stats_players_sides' in files:
            results['player_stats'] = etl_stats(files['vlr_stats_players_sides'], cur)

        if 'vlr_economia_resumen' in files:
            results['economy_summary'] = etl_economy_summary(files['vlr_economia_resumen'], cur)

        if 'vlr_enfrentamientos' in files:
            results['duels'] = etl_duels(files['vlr_enfrentamientos'], cur)

        if 'vlr_multikills_clutches' in files:
            results['multikills'] = etl_multikills(files['vlr_multikills_clutches'], cur)

        if 'vct_equipos' in files:
            results['teams'] = etl_teams(files['vct_equipos'], cur)

        if 'vct_jugadores' in files:
            results['players'] = etl_players(files['vct_jugadores'], cur)

        conn.commit()
        cur.close()
        release_conn(conn)
        return jsonify({"ok": True, "inserted": results})
    except Exception as e:
        if 'conn' in locals(): conn.rollback(); release_conn(conn)
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500


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