"""
ALETHEIA — Predictor Avanzado v3
Extiende el modelo Monte Carlo con:
  · Punto 1: Simulación de halftime con economía real de Valorant (12 rondas)
  · Punto 2: Análisis de overtime — factor de cierre por equipo + OT win prob

Base heredada de predecir.py v2 (decay temporal, fuerza rival, 5 señales).
"""

from flask import Blueprint, jsonify, request
import numpy as np
import math
from datetime import date as date_type, datetime

try:
    from backend.conexion import get_conn, release_conn
except ImportError:
    from conexion import get_conn, release_conn

aletheia_bp = Blueprint('aletheia', __name__)

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
MAPS = ['Abyss', 'Ascent', 'Bind', 'Breeze', 'Corrode',
        'Fracture', 'Haven', 'Icebox', 'Lotus', 'Pearl', 'Split', 'Sunset']

DECAY_HALF_LIFE = 75

MAP_META_ATK = {
    'Abyss': 0.566, 'Ascent': 0.455, 'Bind': 0.507, 'Breeze': 0.510,
    'Corrode': 0.476, 'Fracture': 0.502, 'Haven': 0.512, 'Icebox': 0.533,
    'Lotus': 0.500, 'Pearl': 0.501, 'Split': 0.504, 'Sunset': 0.517,
}

TEAM_ABBREV = {
    '100 Thieves': '100T', '2Game Esports': '2G', 'Apeks': 'APK',
    'BBL Esports': 'BBL', 'BOOM Esports': 'BME', 'Cloud9': 'C9',
    'DRX': 'DRX', 'DetonatioN FocusMe': 'DFM', 'Evil Geniuses': 'EG',
    'FNATIC': 'FNC', 'FURIA': 'FUR', 'FUT Esports': 'FUT',
    'G2 Esports': 'G2', 'GIANTX': 'GX', 'Gen.G': 'GEN',
    'Gentle Mates': 'M8', 'Global Esports': 'GE', 'KRÜ Esports': 'KRÜ',
    'Karmine Corp': 'KC', 'LEVIATÁN': 'LEV', 'LOUD': 'LOUD',
    'MIBR': 'MIBR', 'Movistar KOI(KOI)': 'MKOI', 'NRG': 'NRG',
    'Natus Vincere': 'NAVI', 'Nongshim RedForce': 'NS', 'Paper Rex': 'PRX',
    'Rex Regum Qeon': 'RRQ', 'Sentinels': 'SEN', 'T1': 'T1',
    'TALON': 'TLN', 'Team Heretics': 'TH', 'Team Liquid': 'TL',
    'Team Secret': 'TS', 'Team Vitality': 'VIT', 'ZETA DIVISION': 'ZETA',
}

# Impacto de la Operator por mapa (0-1): qué tan dominante es el arma en ese mapa
OPERATOR_MAP_WEIGHT = {
    'Breeze': 0.90, 'Icebox': 0.80, 'Ascent': 0.75, 'Haven': 0.70,
    'Lotus': 0.65, 'Corrode': 0.55, 'Sunset': 0.50, 'Pearl': 0.50,
    'Split': 0.40, 'Bind': 0.35, 'Abyss': 0.45, 'Fracture': 0.30,
}
OPERATOR_AGENTS = {'Jett', 'Chamber'}

# ─── ECONOMÍA DE VALORANT ─────────────────────────────────────────────────────
# P(attacker wins the round) según categoría de compra de cada equipo.
# Valores calibrados con datos reales de VCT + conocimiento del meta.
# full_buy vs full_buy = 0.494 (dato real de la DB, 2008 rondas).
ECON_WIN_RATE = {
    ('full_buy',  'full_buy'):  0.494,
    ('full_buy',  'semi_buy'):  0.545,
    ('full_buy',  'semi_eco'):  0.660,
    ('full_buy',  'eco'):       0.730,
    ('semi_buy',  'full_buy'):  0.445,
    ('semi_buy',  'semi_buy'):  0.490,
    ('semi_buy',  'semi_eco'):  0.590,
    ('semi_buy',  'eco'):       0.650,
    ('semi_eco',  'full_buy'):  0.375,
    ('semi_eco',  'semi_buy'):  0.420,
    ('semi_eco',  'semi_eco'):  0.480,
    ('semi_eco',  'eco'):       0.580,
    ('eco',       'full_buy'):  0.195,
    ('eco',       'semi_buy'):  0.270,
    ('eco',       'semi_eco'):  0.370,
    ('eco',       'eco'):       0.490,
}

# Bonus por rondas perdidas consecutivas: índice = streak (0-based), max en idx 4
LOSS_BONUS = [1900, 2400, 2900, 3400, 3400]

# Umbrales de banco para categoría de compra (créditos disponibles al inicio de ronda)
def bank_to_category(bank):
    if bank >= 4900: return 'full_buy'
    if bank >= 3000: return 'semi_buy'
    if bank >= 1800: return 'semi_eco'
    return 'eco'

def spend_for_category(cat):
    """Créditos que gasta un equipo según su categoría."""
    return {'full_buy': 4100, 'semi_buy': 3200, 'semi_eco': 1800, 'eco': 700}[cat]


# ─── DB HELPER ────────────────────────────────────────────────────────────────
def query(sql, params=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        release_conn(conn)

def safe_div(a, b, default=0.5):
    return float(a) / float(b) if b and b > 0 else default


# ─── DECAIMIENTO TEMPORAL ─────────────────────────────────────────────────────
def decay_weight(match_date_str, ref_date=None):
    if not match_date_str:
        return 0.5
    if ref_date is None:
        ref_date = date_type.today()
    try:
        md = datetime.strptime(str(match_date_str)[:10], "%Y-%m-%d").date()
        days_ago = max(0, (ref_date - md).days)
        return math.exp(-math.log(2) * days_ago / DECAY_HALF_LIFE)
    except Exception:
        return 0.5

_strength_cache = None

def get_strength_index():
    global _strength_cache
    if _strength_cache is not None:
        return _strength_cache
    try:
        rows = query("""
            SELECT team, SUM(won) AS won, SUM(played) AS played FROM (
                SELECT mt.team_a AS team,
                       SUM(CASE WHEN mt.winner = mt.team_a THEN 1 ELSE 0 END) AS won,
                       COUNT(*) AS played
                FROM maps mp JOIN matches mt ON mp.match_id = mt.match_id GROUP BY mt.team_a
                UNION ALL
                SELECT mt.team_b,
                       SUM(CASE WHEN mt.winner = mt.team_b THEN 1 ELSE 0 END),
                       COUNT(*)
                FROM maps mp JOIN matches mt ON mp.match_id = mt.match_id GROUP BY mt.team_b
            ) GROUP BY team
        """)
        _strength_cache = {r['team']: safe_div(r['won'] or 0, r['played'] or 0) for r in rows if r['team']}
    except Exception:
        _strength_cache = {}
    return _strength_cache

def combined_weight(match_date_str, opponent_name, ref_date=None):
    dw = decay_weight(match_date_str, ref_date)
    si = get_strength_index()
    opp_wr = si.get(opponent_name, 0.5)
    return dw * (1.0 + 0.40 * (opp_wr - 0.5))


# ─── PERFIL DE EQUIPO (idéntico a predecir.py v2) ────────────────────────────
def get_team_profile(full_name):
    abbrev = TEAM_ABBREV.get(full_name, full_name)
    ref_date = date_type.today()

    round_rows = query("""
        SELECT mp.map_name, mt.match_date, mt.team_b AS opponent,
               mp.score_a_attack AS atk_won,
               (mp.score_a_attack + mp.score_b_defense) AS atk_total,
               mp.score_a_defense AS def_won,
               (mp.score_a_defense + mp.score_b_attack) AS def_total,
               CASE WHEN mt.winner = mt.team_a THEN 1 ELSE 0 END AS map_won
        FROM maps mp JOIN matches mt ON mp.match_id = mt.match_id WHERE mt.team_a = ?
        UNION ALL
        SELECT mp.map_name, mt.match_date, mt.team_a AS opponent,
               mp.score_b_attack, (mp.score_b_attack + mp.score_a_defense),
               mp.score_b_defense, (mp.score_b_defense + mp.score_a_attack),
               CASE WHEN mt.winner = mt.team_b THEN 1 ELSE 0 END
        FROM maps mp JOIN matches mt ON mp.match_id = mt.match_id WHERE mt.team_b = ?
    """, [full_name, full_name])

    player_rows = query("""
        SELECT mp.map_name, ps.side, mt.match_date,
               ps.rating, ps.acs, ps.adr, ps.kast, ps.fk, ps.fd
        FROM player_stats ps JOIN maps mp ON ps.map_id = mp.map_id
        JOIN matches mt ON mp.match_id = mt.match_id WHERE ps.team_name = ?
    """, [abbrev])

    econ_rows = query("""
        SELECT mp.map_name, mt.match_date,
               es.pistol_won, 2 AS pistol_total,
               es.full_buy_played AS fb_played, es.full_buy_won AS fb_won,
               es.eco_played, es.eco_won,
               es.semi_buy_played AS sb_played, es.semi_buy_won AS sb_won
        FROM economy_summary es JOIN maps mp ON es.map_id = mp.map_id
        JOIN matches mt ON mp.match_id = mt.match_id WHERE es.team = ?
    """, [abbrev])

    clutch_rows = query("""
        SELECT mp.map_name,
               SUM(mc.v1 + mc.v2*2 + mc.v3*3 + mc.v4*4 + mc.v5*5) AS clutch_score,
               SUM(mc.k2 + mc.k3*2 + mc.k4*3 + mc.k5*4) AS mk_score,
               COUNT(DISTINCT mc.map_id) AS maps_count
        FROM multikills_clutches mc JOIN maps mp ON mc.map_id = mp.map_id
        JOIN matches mt ON mp.match_id = mt.match_id
        WHERE mc.player_name IN (SELECT DISTINCT player_name FROM player_stats WHERE team_name = ?)
        GROUP BY mp.map_name
    """, [abbrev])

    duel_rows = query("""
        SELECT mp.map_name,
               SUM(CASE WHEN d.player_a IN (SELECT player_name FROM player_stats WHERE team_name = ?) THEN d.kills_a ELSE d.kills_b END) AS kills_for,
               SUM(CASE WHEN d.player_a IN (SELECT player_name FROM player_stats WHERE team_name = ?) THEN d.kills_b ELSE d.kills_a END) AS kills_against
        FROM duels d JOIN maps mp ON d.map_id = mp.map_id
        JOIN matches mt ON mp.match_id = mt.match_id
        AND (d.player_a IN (SELECT player_name FROM player_stats WHERE team_name = ?)
          OR d.player_b IN (SELECT player_name FROM player_stats WHERE team_name = ?))
        GROUP BY mp.map_name
    """, [abbrev, abbrev, abbrev, abbrev])

    veto_rows = query("""
        SELECT mv.map_name,
               SUM(CASE WHEN mv.action = 'pick' THEN 1 ELSE 0 END) AS picks,
               SUM(CASE WHEN mv.action = 'ban'  THEN 1 ELSE 0 END) AS bans
        FROM match_veto mv JOIN matches mt ON mv.match_id = mt.match_id
        WHERE ((mv.team = 'a' AND mt.team_a = ?) OR (mv.team = 'b' AND mt.team_b = ?))
        GROUP BY mv.map_name
    """, [full_name, full_name])

    profile = {'team': full_name, 'abbrev': abbrev, 'by_map': {}, 'global': {}}
    by_map_rnd = {}
    g_aw = g_at = g_dw = g_dt = g_mw = g_wt = 0.0

    for r in round_rows:
        mn = r['map_name']
        if not mn:
            continue
        w = combined_weight(r['match_date'], r['opponent'], ref_date)
        if mn not in by_map_rnd:
            by_map_rnd[mn] = {'aw': 0.0, 'at': 0.0, 'dw_': 0.0, 'dt': 0.0,
                               'mw': 0.0, 'wt': 0.0, 'cnt': 0, 'decay_sum': 0.0}
        bm = by_map_rnd[mn]
        bm['aw'] += (r['atk_won'] or 0) * w
        bm['at'] += (r['atk_total'] or 0) * w
        bm['dw_'] += (r['def_won'] or 0) * w
        bm['dt'] += (r['def_total'] or 0) * w
        bm['mw'] += (r['map_won'] or 0) * w
        bm['wt'] += w
        bm['cnt'] += 1
        bm['decay_sum'] += decay_weight(r['match_date'], ref_date)
        g_aw += (r['atk_won'] or 0) * w; g_at += (r['atk_total'] or 0) * w
        g_dw += (r['def_won'] or 0) * w; g_dt += (r['def_total'] or 0) * w
        g_mw += (r['map_won'] or 0) * w; g_wt += w

    for mn, bm in by_map_rnd.items():
        d = profile['by_map'].setdefault(mn, {})
        d.update({'atk_won': bm['aw'], 'atk_total': bm['at'], 'def_won': bm['dw_'],
                  'def_total': bm['dt'], 'maps_won': bm['mw'], 'maps_played': bm['cnt'],
                  'recency': bm['decay_sum'] / bm['cnt'] if bm['cnt'] else 0.3})

    profile['global'].update({'atk_wr': safe_div(g_aw, g_at), 'def_wr': safe_div(g_dw, g_dt),
                               'map_wr': safe_div(g_mw, g_wt),
                               'maps_played': sum(bm['cnt'] for bm in by_map_rnd.values())})

    by_ms = {}
    all_rw = all_rwt = 0.0
    for r in player_rows:
        mn = r['map_name']; side = r['side']
        if not mn or not side:
            continue
        dw = decay_weight(r['match_date'], ref_date)
        key = (mn, side)
        if key not in by_ms:
            by_ms[key] = {'rw': 0.0, 'aw': 0.0, 'adw': 0.0, 'kw': 0.0, 'fk': 0, 'fd': 0, 'wt': 0.0}
        s = by_ms[key]
        s['rw'] += (r['rating'] or 1.0) * dw; s['aw'] += (r['acs'] or 200) * dw
        s['adw'] += (r['adr'] or 130) * dw; s['kw'] += (r['kast'] or 70) * dw
        s['fk'] += r['fk'] or 0; s['fd'] += r['fd'] or 0; s['wt'] += dw
        all_rw += (r['rating'] or 1.0) * dw; all_rwt += dw

    for (mn, side), s in by_ms.items():
        d = profile['by_map'].setdefault(mn, {})
        wt = max(s['wt'], 1e-9)
        d[f'{side}_rating'] = s['rw'] / wt; d[f'{side}_acs'] = s['aw'] / wt
        d[f'{side}_adr'] = s['adw'] / wt; d[f'{side}_kast'] = s['kw'] / wt
        d['fk_fd'] = safe_div(s['fk'], s['fk'] + s['fd'])

    profile['global']['avg_rating'] = safe_div(all_rw, all_rwt, 1.0)

    by_me = {}
    g_pw = g_pt = g_fbw = g_fbp = 0.0
    for r in econ_rows:
        mn = r['map_name']
        if not mn:
            continue
        dw = decay_weight(r['match_date'], ref_date)
        if mn not in by_me:
            by_me[mn] = {'pw': 0.0, 'pt': 0.0, 'fbw': 0.0, 'fbp': 0.0,
                         'ecow': 0.0, 'ecop': 0.0, 'sbw': 0.0, 'sbp': 0.0}
        e = by_me[mn]
        e['pw'] += (r['pistol_won'] or 0) * dw; e['pt'] += (r['pistol_total'] or 2) * dw
        e['fbw'] += (r['fb_won'] or 0) * dw; e['fbp'] += (r['fb_played'] or 0) * dw
        e['ecow'] += (r['eco_won'] or 0) * dw; e['ecop'] += (r['eco_played'] or 0) * dw
        e['sbw'] += (r['sb_won'] or 0) * dw; e['sbp'] += (r['sb_played'] or 0) * dw
        g_pw += (r['pistol_won'] or 0) * dw; g_pt += (r['pistol_total'] or 2) * dw
        g_fbw += (r['fb_won'] or 0) * dw; g_fbp += (r['fb_played'] or 0) * dw

    for mn, e in by_me.items():
        d = profile['by_map'].setdefault(mn, {})
        d['pistol_wr'] = safe_div(e['pw'], e['pt'])
        d['full_buy_wr'] = safe_div(e['fbw'], e['fbp'])
        d['eco_wr'] = safe_div(e['ecow'], e['ecop'])
        d['semi_buy_wr'] = safe_div(e['sbw'], e['sbp'])

    profile['global']['pistol_wr'] = safe_div(g_pw, g_pt)
    profile['global']['full_buy_wr'] = safe_div(g_fbw, g_fbp)

    total_cs = total_ms = total_mc = 0
    for r in clutch_rows:
        mn = r['map_name']
        if not mn:
            continue
        d = profile['by_map'].setdefault(mn, {})
        mc = max(r['maps_count'] or 1, 1)
        d['clutch_per_map'] = (r['clutch_score'] or 0) / mc
        d['mk_per_map'] = (r['mk_score'] or 0) / mc
        total_cs += r['clutch_score'] or 0; total_ms += r['mk_score'] or 0; total_mc += mc

    profile['global']['clutch_per_map'] = total_cs / max(total_mc, 1)
    profile['global']['mk_per_map'] = total_ms / max(total_mc, 1)

    total_df = total_da = 0
    for r in duel_rows:
        mn = r['map_name']
        if not mn:
            continue
        d = profile['by_map'].setdefault(mn, {})
        kf = r['kills_for'] or 0; ka = r['kills_against'] or 0
        d['duel_wr'] = safe_div(kf, kf + ka)
        total_df += kf; total_da += ka

    profile['global']['duel_wr'] = safe_div(total_df, total_df + total_da)

    for r in veto_rows:
        mn = r['map_name']
        if not mn:
            continue
        d = profile['by_map'].setdefault(mn, {})
        d['picks'] = r['picks'] or 0; d['bans'] = r['bans'] or 0

    return profile


def get_h2h(team_a, team_b):
    rows = query("""
        SELECT winner, COUNT(*) AS cnt FROM matches
        WHERE ((team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?)) GROUP BY winner
    """, [team_a, team_b, team_b, team_a])
    a_w = sum(r['cnt'] for r in rows if r['winner'] == team_a)
    b_w = sum(r['cnt'] for r in rows if r['winner'] == team_b)
    total = a_w + b_w
    return {'a_wins': a_w, 'b_wins': b_w, 'total': total, 'a_wr': safe_div(a_w, total)}


# ─── PROBABILIDAD POR RONDA (heredada de predecir.py) ────────────────────────
def get_side_stats(profile, map_name, side):
    PRIOR_ROUNDS = 20
    meta_atk = MAP_META_ATK.get(map_name, 0.5)
    meta_def = 1 - meta_atk
    gl = profile['global']
    mp = profile['by_map'].get(map_name, {})

    if side == 'attack':
        obs_won = mp.get('atk_won', 0); obs_tot = mp.get('atk_total', 0)
        prior_wr = meta_atk * (gl['atk_wr'] / max(gl['atk_wr'] + gl['def_wr'], 0.01))
        rating = mp.get('attack_rating', gl.get('avg_rating', 1.0))
        acs = mp.get('attack_acs', 200); adr = mp.get('attack_adr', 130)
    else:
        obs_won = mp.get('def_won', 0); obs_tot = mp.get('def_total', 0)
        prior_wr = meta_def * (gl['def_wr'] / max(gl['atk_wr'] + gl['def_wr'], 0.01))
        rating = mp.get('defense_rating', gl.get('avg_rating', 1.0))
        acs = mp.get('defense_acs', 200); adr = mp.get('defense_adr', 130)

    prior_won = prior_wr * PRIOR_ROUNDS
    wr = (obs_won + prior_won) / (obs_tot + PRIOR_ROUNDS)

    return {
        'wr': wr,
        'rating': max(rating or 0.5, 0.5),
        'acs': acs or 200, 'adr': adr or 130,
        'fk_fd': mp.get('fk_fd', gl.get('duel_wr', 0.5)),
        'duel_wr': mp.get('duel_wr', gl.get('duel_wr', 0.5)),
        'pistol_wr': mp.get('pistol_wr', gl.get('pistol_wr', 0.5)),
        'full_buy_wr': mp.get('full_buy_wr', gl.get('full_buy_wr', 0.5)),
        'eco_wr': mp.get('eco_wr', 0.25),
        'clutch_mk': mp.get('clutch_per_map', gl.get('clutch_per_map', 3.0)) + mp.get('mk_per_map', gl.get('mk_per_map', 2.0)) * 0.5,
        'veto_signal': 0.52 if mp.get('picks', 0) > 0 else (0.48 if mp.get('bans', 0) > 1 else 0.5),
        'sample': obs_tot, 'maps': mp.get('maps_played', 0), 'recency': mp.get('recency', 0.3),
    }


def compute_round_prob(prof_a, prof_b, map_name, a_is_atk, h2h):
    side_a = 'attack' if a_is_atk else 'defense'
    side_b = 'defense' if a_is_atk else 'attack'
    sa = get_side_stats(prof_a, map_name, side_a)
    sb = get_side_stats(prof_b, map_name, side_b)

    wr_signal = sa['wr'] / (sa['wr'] + sb['wr']) if (sa['wr'] + sb['wr']) > 0 else 0.5
    a_skill = sa['rating'] * 0.50 + sa['acs'] / 280.0 * 0.20 + sa['adr'] / 170.0 * 0.15 + sa['fk_fd'] * 0.15
    b_skill = sb['rating'] * 0.50 + sb['acs'] / 280.0 * 0.20 + sb['adr'] / 170.0 * 0.15 + sb['fk_fd'] * 0.15
    skill_signal = safe_div(a_skill, a_skill + b_skill)

    a_econ = sa['full_buy_wr'] * 0.55 + sa['pistol_wr'] * 0.30 + sa['eco_wr'] * 0.15
    b_econ = sb['full_buy_wr'] * 0.55 + sb['pistol_wr'] * 0.30 + sb['eco_wr'] * 0.15
    econ_signal = safe_div(a_econ, a_econ + b_econ)

    a_cm = sa['clutch_mk']; b_cm = sb['clutch_mk']
    clutch_signal = safe_div(a_cm, a_cm + b_cm) if (a_cm + b_cm) > 0 else 0.5

    if h2h['total'] >= 2:
        h2h_comp = h2h['a_wr'] * 0.7 + sa['veto_signal'] * 0.3
    else:
        h2h_comp = sa['veto_signal']
    meta_signal = 0.5 * h2h_comp + 0.5 * (1 - sb['veto_signal'])

    p = (0.40 * wr_signal + 0.28 * skill_signal + 0.18 * econ_signal +
         0.09 * clutch_signal + 0.05 * meta_signal)
    return float(max(0.28, min(0.72, p)))


def map_confidence(a_maps, b_maps, a_recency, b_recency):
    min_m = min(a_maps, b_maps)
    avg_rec = (a_recency + b_recency) / 2
    base = 'high' if min_m >= 4 else ('medium' if min_m >= 2 else 'low')
    if avg_rec < 0.35:
        return {'high': 'medium', 'medium': 'low'}.get(base, base)
    elif avg_rec < 0.55 and base == 'high':
        return 'medium'
    return base


# ─── PUNTO 1: SIMULACIÓN DE HALFTIME CON ECONOMÍA ───────────────────────────
def simulate_halftime(p_atk_base, p_def_base, prof_a, prof_b, map_name, a_starts_atk, n=10000):
    """
    Simula las 12 primeras rondas de un mapa con máquina de estados de economía real.

    Cómo funciona:
    - Cada simulación tiene un banco de créditos por equipo
    - La categoría de compra (eco/semi_eco/semi_buy/full_buy) se determina del banco
    - La probabilidad de ganar la ronda se ajusta según la tabla ECON_WIN_RATE
    - El equipo ganador mantiene su equipamiento (bank += 3000)
    - El equipo perdedor recibe el bonus de derrota según su racha

    Retorna distribución de scores al halftime + predicción del marcador.
    """
    # Escalar p_base por el ratio de habilidad relativa del equipo en ese mapa
    mp_a = prof_a['by_map'].get(map_name, {})
    mp_b = prof_b['by_map'].get(map_name, {})
    gl_a = prof_a['global']; gl_b = prof_b['global']

    # Factor de economía personal (qué tan bien maneja cada equipo sus rondas eco)
    eco_factor_a = mp_a.get('eco_wr', gl_a.get('pistol_wr', 0.35)) / 0.30  # normalizado
    eco_factor_b = mp_b.get('eco_wr', gl_b.get('pistol_wr', 0.35)) / 0.30
    pistol_a = mp_a.get('pistol_wr', gl_a.get('pistol_wr', 0.50))
    pistol_b = mp_b.get('pistol_wr', gl_b.get('pistol_wr', 0.50))
    fb_a = mp_a.get('full_buy_wr', gl_a.get('full_buy_wr', 0.50))
    fb_b = mp_b.get('full_buy_wr', gl_b.get('full_buy_wr', 0.50))

    rng = np.random.default_rng()
    scores_a = np.zeros(n, dtype=np.int32)
    scores_b = np.zeros(n, dtype=np.int32)
    a_is_atk = np.full(n, a_starts_atk, dtype=bool)

    # Bancos iniciales: ronda de pistola → 800 créditos
    bank_a = np.full(n, 800.0)
    bank_b = np.full(n, 800.0)
    streak_a = np.zeros(n, dtype=np.int32)  # racha de derrotas consecutivas
    streak_b = np.zeros(n, dtype=np.int32)

    for rnd in range(12):
        # Determinar categorías de compra de cada equipo
        # El atacante de esta ronda puede ser A o B
        bank_atk = np.where(a_is_atk, bank_a, bank_b)
        bank_def = np.where(a_is_atk, bank_b, bank_a)

        # Función vectorizada de banco → categoría → índice 0-3
        cat_atk = np.where(bank_atk >= 4900, 3,
                  np.where(bank_atk >= 3000, 2,
                  np.where(bank_atk >= 1800, 1, 0)))
        cat_def = np.where(bank_def >= 4900, 3,
                  np.where(bank_def >= 3000, 2,
                  np.where(bank_def >= 1800, 1, 0)))

        # Tabla 4x4 de tasas de victoria del atacante
        ECON_TABLE = np.array([
            [0.490, 0.370, 0.270, 0.195],  # atk=eco
            [0.580, 0.480, 0.420, 0.375],  # atk=semi_eco
            [0.650, 0.590, 0.490, 0.445],  # atk=semi_buy
            [0.730, 0.660, 0.545, 0.494],  # atk=full_buy
        ])

        # Tasa base del matchup de economía
        econ_rate = ECON_TABLE[cat_atk, cat_def]  # shape (n,)

        # Ajustar con habilidades relativas del equipo:
        # Cuando A ataca: su ventaja de habilidad (p_atk_base vs 0.50) escala el econ_rate
        # Cuando B ataca: usamos p_def_base (que ya refleja A como defensor)
        skill_adj_atk = (p_atk_base - 0.50) * 0.6   # cuánto mejora A en ataque vs neutro
        skill_adj_def = (p_def_base - 0.50) * 0.6    # cuánto mejora A en defensa vs neutro

        p_a_wins = np.where(
            a_is_atk,
            np.clip(econ_rate + skill_adj_atk, 0.08, 0.92),    # A ataca
            np.clip(1.0 - econ_rate - skill_adj_def, 0.08, 0.92)  # B ataca, A defiende
        )

        # Ronda 1 es siempre de pistola: ajustar con pistol_wr
        if rnd == 0:
            pistol_base = safe_div(pistol_a, pistol_a + pistol_b)
            p_a_wins = np.full(n, np.clip(pistol_base, 0.25, 0.75))

        a_wins = rng.random(n) < p_a_wins
        scores_a += a_wins.astype(np.int32)
        scores_b += (~a_wins).astype(np.int32)

        # ── Actualizar bancos ──
        # Ganadores: mantienen su arma + ganan 3000 (no pierden lo gastado)
        # Perdedores: pierden su arma + reciben bonus de derrota según racha

        # Calcular gastos de esta ronda
        spend_atk = np.where(bank_atk >= 4900, 4100,
                    np.where(bank_atk >= 3000, 3200,
                    np.where(bank_atk >= 1800, 1800, 700))).astype(float)
        spend_def = np.where(bank_def >= 4900, 4100,
                    np.where(bank_def >= 3000, 3200,
                    np.where(bank_def >= 1800, 1800, 700))).astype(float)

        # Bonus de derrota según racha (capado en índice 4)
        # streak_a y streak_b representan la racha del equipo A y B respectivamente
        loss_arr_a = np.array([LOSS_BONUS[min(s, 4)] for s in streak_a.tolist()], dtype=float)
        loss_arr_b = np.array([LOSS_BONUS[min(s, 4)] for s in streak_b.tolist()], dtype=float)

        # A gana ronda
        new_bank_a_win = np.minimum(bank_a + 3000, 9000.0)
        new_bank_a_lose = np.maximum(bank_a - spend_atk if a_is_atk.all() else bank_a - spend_def, 0) + loss_arr_a

        bank_a_next = np.where(a_wins, new_bank_a_win, np.where(
            a_is_atk,
            np.maximum(bank_a - spend_atk, 0) + loss_arr_a,
            np.maximum(bank_a - spend_def, 0) + loss_arr_a
        ))

        # B gana ronda (cuando a_wins es False)
        bank_b_next = np.where(~a_wins, np.minimum(bank_b + 3000, 9000.0), np.where(
            a_is_atk,
            np.maximum(bank_b - spend_def, 0) + loss_arr_b,
            np.maximum(bank_b - spend_atk, 0) + loss_arr_b
        ))

        bank_a = np.minimum(bank_a_next, 9000.0)
        bank_b = np.minimum(bank_b_next, 9000.0)

        streak_a = np.where(a_wins, 0, streak_a + 1)
        streak_b = np.where(~a_wins, 0, streak_b + 1)

    # ── Calcular estadísticas del halftime ────────────────────────────────────
    from collections import Counter
    raw = Counter(zip(scores_a.tolist(), scores_b.tolist()))
    top8 = sorted(raw.items(), key=lambda x: -x[1])[:8]
    score_dist = {f"{k[0]}-{k[1]}": round(v / n, 3) for k, v in top8}

    avg_a = float(scores_a.mean())
    avg_b = float(scores_b.mean())

    # Probabilidades de liderato
    a_leads = float((scores_a > scores_b).mean())
    tied = float((scores_a == scores_b).mean())
    b_leads = float((scores_b > scores_a).mean())

    # Ventaja grande (≥3 rondas de diferencia → difícil de remontar)
    big_lead_a = float(((scores_a - scores_b) >= 3).mean())
    big_lead_b = float(((scores_b - scores_a) >= 3).mean())

    # Score modal (más frecuente)
    modal = top8[0][0] if top8 else (6, 6)

    # Nota económica: detectar si hay asimetría económica notable
    eco_note = None
    diff = abs(avg_a - avg_b)
    if diff >= 3.0:
        leader = prof_a['abbrev'] if avg_a > avg_b else prof_b['abbrev']
        eco_note = f"{leader} domina la primera mitad ({round(max(avg_a,avg_b),1)}-{round(min(avg_a,avg_b),1)}). Ventaja de ≥3 rondas es muy difícil de remontar."
    elif big_lead_a > 0.25 or big_lead_b > 0.25:
        leader = prof_a['abbrev'] if big_lead_a > big_lead_b else prof_b['abbrev']
        eco_note = f"{leader} tiene {round(max(big_lead_a,big_lead_b)*100)}% prob. de ventaja ≥3 rondas al medio tiempo."

    return {
        'avg_a': round(avg_a, 1),
        'avg_b': round(avg_b, 1),
        'pred_a': str(round(avg_a, 1)),
        'pred_b': str(round(avg_b, 1)),
        'modal_score': f"{modal[0]}-{modal[1]}",
        'prob_a_leads': round(a_leads, 3),
        'prob_tied': round(tied, 3),
        'prob_b_leads': round(b_leads, 3),
        'big_lead_a': round(big_lead_a, 3),
        'big_lead_b': round(big_lead_b, 3),
        'score_dist': score_dist,
        'eco_note': eco_note,
    }


# ─── PUNTO 2: PERFIL DE OVERTIME ─────────────────────────────────────────────
def get_team_ot_profile(full_name):
    """
    Analiza la capacidad del equipo para cerrar mapas y ganar overtimes.

    Métricas calculadas:
    - ot_rate: % de sus mapas que fueron a OT
    - ot_win_rate: % de OTs ganados (de los que llegaron a OT)
    - close_rate: % de mapas ganados sin necesitar OT (cierres limpios)
    - failed_close: mapas donde el equipo llegó a 12 rondas ganadas y igual fue OT
      (identificado cuando el equipo ganó pero el total de rondas > 25)

    Lógica: si un equipo tiene alta close_rate, es bueno cerrando.
    Si tiene baja close_rate pero alta ot_win_rate, domina en OT pero llega a él mucho.
    """
    rows = query("""
        SELECT
            mt.team_a, mt.team_b, mt.winner,
            (mp.score_a_attack + mp.score_a_defense) AS score_a,
            (mp.score_b_attack + mp.score_b_defense) AS score_b,
            mt.match_date
        FROM maps mp
        JOIN matches mt ON mp.match_id = mt.match_id
        WHERE mt.team_a = ? OR mt.team_b = ?
    """, [full_name, full_name])

    if not rows:
        return {
            'total_maps': 0, 'ot_maps': 0, 'ot_rate': 0.0,
            'ot_win_rate': 0.5, 'close_rate': 0.5,
            'ot_wins': 0, 'ot_losses': 0, 'grade': 'N/A',
        }

    total = len(rows)
    ot_maps = [r for r in rows if (r['score_a'] + r['score_b']) > 25]
    normal_maps = [r for r in rows if (r['score_a'] + r['score_b']) <= 25]

    ot_wins = sum(1 for r in ot_maps if r['winner'] == full_name)
    ot_losses = len(ot_maps) - ot_wins

    total_wins = sum(1 for r in rows if r['winner'] == full_name)
    # Victorias limpias (sin OT)
    clean_wins = sum(1 for r in normal_maps if r['winner'] == full_name)
    # De todas las victorias, cuántas fueron sin OT
    close_rate = safe_div(clean_wins, total_wins)

    ot_rate = safe_div(len(ot_maps), total)
    ot_win_rate = safe_div(ot_wins, len(ot_maps)) if ot_maps else 0.5

    # "Fallar el cierre": victorias que necesitaron OT (tenían ventaja pero no cerraron)
    # Proxy: de los OT maps ganados, cuántos tenían score ≥ 14-12 (fueron limpios en OT)
    # vs. 15-13, 16-14, etc. (necesitaron más rondas)
    ot_wins_close = sum(1 for r in ot_maps
                       if r['winner'] == full_name and abs(r['score_a'] - r['score_b']) == 2)
    ot_wins_extended = ot_wins - ot_wins_close  # necesitaron más de 2 rondas de OT

    # Grade de cierre: A (>85% limpios), B (70-85%), C (55-70%), D (<55%)
    if close_rate >= 0.85:
        grade = 'A'
    elif close_rate >= 0.70:
        grade = 'B'
    elif close_rate >= 0.55:
        grade = 'C'
    else:
        grade = 'D'

    return {
        'total_maps': total,
        'ot_maps': len(ot_maps),
        'ot_rate': round(ot_rate, 3),
        'ot_win_rate': round(ot_win_rate, 3),
        'close_rate': round(close_rate, 3),
        'ot_wins': ot_wins,
        'ot_losses': ot_losses,
        'ot_wins_close': ot_wins_close,
        'ot_wins_extended': ot_wins_extended,
        'grade': grade,
    }


def compute_ot_analysis(ot_prof_a, ot_prof_b, sim_ot_pct, p_atk, p_def, abbrev_a, abbrev_b):
    """
    Combina la probabilidad de OT simulada con los factores históricos.

    - sim_ot_pct: % de las simulaciones que llegaron a OT (de monte_carlo)
    - Ajuste histórico: si ambos equipos tienden a ir a OT, aumentar la prob
    - OT win prob: basado en ot_win_rate histórico de cada equipo,
      con corrección por la probabilidad de ganar rondas en OT (p_atk/p_def)
    """
    # Probabilidad ajustada de OT: 60% simulación + 40% historial
    hist_ot_avg = (ot_prof_a['ot_rate'] + ot_prof_b['ot_rate']) / 2
    # Si ambos equipos van mucho a OT, subir un poco la prob
    ot_tendency_boost = max(0, hist_ot_avg - 0.15) * 0.5  # boost si >15% van a OT
    final_ot_prob = min(0.60 * sim_ot_pct + 0.40 * hist_ot_avg + ot_tendency_boost, 0.95)

    # P(A gana el OT): combinar historial con habilidad de ronda
    # En OT ambos equipos tienen rounds alternados de ATK/DEF
    # La habilidad base (promedio de p_atk y p_def) importa
    round_skill_a = (p_atk + p_def) / 2  # habilidad promedio de A en ese mapa

    hist_a = ot_prof_a['ot_win_rate']
    hist_b = ot_prof_b['ot_win_rate']

    # Normalizar historial para que sume 1
    hist_sum = hist_a + hist_b
    hist_a_norm = safe_div(hist_a, hist_sum)

    # Combinar: 55% habilidad de ronda + 45% historial de OT
    ot_win_a = 0.55 * round_skill_a + 0.45 * hist_a_norm
    ot_win_a = float(max(0.25, min(0.75, ot_win_a)))

    # Nota explicativa
    def _close_note(prof, abbrev):
        grade = prof['grade']
        if prof['total_maps'] < 3:
            return f"{abbrev}: datos insuficientes"
        if grade == 'A':
            return f"{abbrev}: excelente al cerrar ({prof['close_rate']*100:.0f}% limpios)"
        elif grade == 'B':
            return f"{abbrev}: bueno al cerrar ({prof['close_rate']*100:.0f}% limpios)"
        elif grade == 'C':
            return f"{abbrev}: irregular al cerrar ({prof['close_rate']*100:.0f}% limpios)"
        else:
            return f"{abbrev}: dificultad para cerrar ({prof['close_rate']*100:.0f}% limpios)"

    note_a = _close_note(ot_prof_a, abbrev_a)
    note_b = _close_note(ot_prof_b, abbrev_b)

    return {
        'ot_prob': round(final_ot_prob, 3),
        'ot_win_a': round(ot_win_a, 3),
        'ot_win_b': round(1 - ot_win_a, 3),
        'sim_ot_pct': round(sim_ot_pct, 3),
        'hist_ot_avg': round(hist_ot_avg, 3),
        'close_note_a': note_a,
        'close_note_b': note_b,
        'grade_a': ot_prof_a['grade'],
        'grade_b': ot_prof_b['grade'],
        'ot_rate_a': ot_prof_a['ot_rate'],
        'ot_rate_b': ot_prof_b['ot_rate'],
        'ot_wr_a': ot_prof_a['ot_win_rate'],
        'ot_wr_b': ot_prof_b['ot_win_rate'],
    }


# ─── PUNTO 3: IMPACTO DE LA OPERATOR ─────────────────────────────────────────
def get_operator_analysis(abbrev_a, abbrev_b):
    """
    Por cada mapa, analiza:
    - Qué tanto impacta la Operator en ese mapa (OPERATOR_MAP_WEIGHT)
    - Si algún jugador de cada equipo usa agentes de Operator (Jett/Chamber)
    - Rating promedio de esos jugadores en ese mapa
    - Si el rival tiene historial de contrarrestar Operators
    """
    result = {}
    for mn in MAPS:
        map_w = OPERATOR_MAP_WEIGHT.get(mn, 0.5)
        op_players = []
        for ab in [abbrev_a, abbrev_b]:
            rows = query("""
                SELECT ps.player_name, ps.agent,
                       AVG(ps.rating) AS avg_r, AVG(ps.acs) AS avg_acs,
                       SUM(ps.fk) AS total_fk, COUNT(*) AS games
                FROM player_stats ps
                JOIN maps mp ON ps.map_id = mp.map_id
                WHERE ps.team_name = ? AND mp.map_name = ? AND ps.agent IN ('Jett','Chamber')
                GROUP BY ps.player_name, ps.agent
                HAVING COUNT(*) >= 1
            """, [ab, mn])
            for r in rows:
                op_players.append({
                    'name': r['player_name'], 'agent': r['agent'],
                    'team': ab, 'rating': round(r['avg_r'] or 1.0, 2),
                    'acs': round(r['avg_acs'] or 200, 0),
                    'fk_per_game': round((r['total_fk'] or 0) / max(r['games'], 1), 1),
                    'games': r['games'],
                })
        result[mn] = {
            'map_weight': map_w,
            'op_players': op_players,
            'has_op_users': len(op_players) > 0,
        }
    return result


# ─── PUNTO 4: EVALUACIÓN DEL JUGADOR ESTRELLA ────────────────────────────────
def get_star_player_analysis(abbrev, opponent_abbrev=None):
    """
    Identifica al jugador estrella (mayor rating ponderado por decay) y evalúa:
    - Dependencia: ratio star_rating / team_avg_rating
    - Consistencia del star (std dev)
    - Riesgo de contra-estrategia del rival
    """
    ref_date = date_type.today()
    rows = query("""
        SELECT ps.player_name, ps.rating, mt.match_date
        FROM player_stats ps
        JOIN maps mp ON ps.map_id = mp.map_id
        JOIN matches mt ON mp.match_id = mt.match_id
        WHERE ps.team_name = ?
    """, [abbrev])

    if not rows:
        return None

    # Agrupar por jugador con decay
    by_player = {}
    for r in rows:
        pn = r['player_name']
        if not pn:
            continue
        dw = decay_weight(r['match_date'], ref_date)
        rating = r['rating'] or 1.0
        if pn not in by_player:
            by_player[pn] = {'ratings': [], 'dw_sum': 0.0, 'dw_rating_sum': 0.0}
        by_player[pn]['ratings'].append(rating)
        by_player[pn]['dw_sum'] += dw
        by_player[pn]['dw_rating_sum'] += rating * dw

    if not by_player:
        return None

    # Calcular rating decay-ponderado por jugador
    player_stats_list = []
    for pn, data in by_player.items():
        if data['dw_sum'] < 0.01:
            continue
        avg_r = data['dw_rating_sum'] / data['dw_sum']
        std_r = float(np.std(data['ratings'])) if len(data['ratings']) >= 2 else 0.0
        player_stats_list.append({
            'name': pn, 'avg_rating': avg_r, 'std_dev': std_r, 'games': len(data['ratings']),
        })

    if not player_stats_list:
        return None

    player_stats_list.sort(key=lambda x: -x['avg_rating'])
    star = player_stats_list[0]

    # Team average (excluir star para medir dependencia real)
    others = [p['avg_rating'] for p in player_stats_list[1:]] if len(player_stats_list) > 1 else [1.0]
    team_avg = sum(p['avg_rating'] for p in player_stats_list) / len(player_stats_list)
    team_avg_no_star = sum(others) / len(others)
    dependency = star['avg_rating'] / max(team_avg, 0.5)

    # Riesgo de contra-estrategia: si el oponente tiene buen historial contra los teams
    # que dependen mucho de un star player (proxy: si rival tiene high FK vs teams con stars)
    counter_risk = False
    if opponent_abbrev and dependency >= 1.15:
        try:
            rival_fk = query("""
                SELECT AVG(ps.fk) AS avg_fk FROM player_stats ps
                JOIN maps mp ON ps.map_id = mp.map_id
                WHERE ps.team_name = ?
            """, [opponent_abbrev])
            if rival_fk and rival_fk[0].get('avg_fk', 0) and rival_fk[0]['avg_fk'] > 2.5:
                counter_risk = True
        except Exception:
            pass

    return {
        'star_name': star['name'],
        'star_rating': round(star['avg_rating'], 2),
        'star_std': round(star['std_dev'], 2),
        'star_games': star['games'],
        'team_avg_rating': round(team_avg, 2),
        'team_avg_no_star': round(team_avg_no_star, 2),
        'dependency': round(dependency, 2),
        'counter_risk': counter_risk,
    }


# ─── PUNTO 5: CONSISTENCIA ESTADÍSTICA ───────────────────────────────────────
def get_player_consistency(abbrev):
    """
    Calcula la desviación estándar del rating de cada jugador del equipo.
    Grade: A (σ < 0.10), B (σ < 0.15), C (σ < 0.20), D (σ >= 0.20)
    Prioriza consistencia sobre números absolutos.
    """
    rows = query("""
        SELECT ps.player_name, ps.rating
        FROM player_stats ps
        JOIN maps mp ON ps.map_id = mp.map_id
        JOIN matches mt ON mp.match_id = mt.match_id
        WHERE ps.team_name = ?
    """, [abbrev])

    by_player = {}
    for r in rows:
        pn = r['player_name']
        if not pn:
            continue
        by_player.setdefault(pn, []).append(r['rating'] or 1.0)

    result = []
    for pn, ratings in by_player.items():
        if len(ratings) < 2:
            continue
        avg_r = sum(ratings) / len(ratings)
        std_r = float(np.std(ratings))
        if std_r < 0.10:
            grade = 'A'
        elif std_r < 0.15:
            grade = 'B'
        elif std_r < 0.20:
            grade = 'C'
        else:
            grade = 'D'
        result.append({
            'name': pn, 'avg_rating': round(avg_r, 2),
            'std_dev': round(std_r, 2), 'grade': grade, 'games': len(ratings),
        })

    result.sort(key=lambda x: -x['avg_rating'])
    return result


# ─── SIMULACIÓN MONTE CARLO (igual que predecir.py) ──────────────────────────
def monte_carlo(p_a_atk, p_a_def, a_starts_atk, n=10000, return_details=False):
    rng = np.random.default_rng()
    scores_a = np.zeros(n, dtype=np.int32)
    scores_b = np.zeros(n, dtype=np.int32)
    a_atk = np.full(n, a_starts_atk, dtype=bool)
    rnds = np.zeros(n, dtype=np.int32)
    active = np.ones(n, dtype=bool)

    for _ in range(90):
        if not active.any():
            break
        idx = np.where(active)[0]
        p = np.where(a_atk[idx], p_a_atk, p_a_def)
        won = rng.random(len(idx)) < p
        scores_a[idx] += won.astype(np.int32)
        scores_b[idx] += (~won).astype(np.int32)
        rnds[idx] += 1
        switch12 = active & (rnds == 12)
        a_atk[switch12] = ~a_atk[switch12]
        ot_switch = active & (rnds > 24) & ((rnds - 24) % 2 == 0)
        a_atk[ot_switch] = ~a_atk[ot_switch]
        normal_win = active & (rnds <= 24) & ((scores_a >= 13) | (scores_b >= 13))
        ot_win = (active & (rnds > 24) & ((scores_a >= 13) | (scores_b >= 13)) &
                  (np.abs(scores_a - scores_b) >= 2))
        active &= ~(normal_win | ot_win)

    win_a = float((scores_a > scores_b).mean())
    if not return_details:
        return win_a

    from collections import Counter
    total_rnds = scores_a + scores_b
    ot_pct = float((total_rnds > 24).mean())
    avg_rounds = float(total_rnds.mean())
    raw = Counter(zip(scores_a.tolist(), scores_b.tolist()))
    top5 = sorted(raw.items(), key=lambda x: -x[1])[:5]
    score_freq = {f"{k[0]}-{k[1]}": round(v / n, 3) for k, v in top5}
    modal = top5[0][0] if top5 else (13, 0)

    return {
        'win_prob': win_a,
        'avg_score_a': round(float(scores_a.mean()), 1),
        'avg_score_b': round(float(scores_b.mean()), 1),
        'avg_rounds': round(avg_rounds, 1),
        'ot_pct': round(ot_pct, 3),
        'score_freq': score_freq,
        'modal_score': f"{modal[0]}-{modal[1]}",
    }


def monte_carlo_series_sequential(map_probs, n=10000):
    """
    Simula N series completas respetando el orden secuencial de los mapas.
    Punto 6 (prep): no se puede ganar 2-0 si no se gana el mapa 1.
    """
    total = len(map_probs)
    need = (total + 1) // 2
    rng = np.random.default_rng()
    wins_a = np.zeros(n, dtype=np.int32)
    wins_b = np.zeros(n, dtype=np.int32)

    for p_win in map_probs:
        still = (wins_a < need) & (wins_b < need)
        if not still.any():
            break
        won_a = still & (rng.random(n) < p_win)
        wins_a += won_a.astype(np.int32)
        wins_b += (still & ~won_a).astype(np.int32)

    series_win_a = float((wins_a >= need).mean())
    from collections import Counter
    raw = Counter(zip(wins_a.tolist(), wins_b.tolist()))
    score_dist = {f"{k[0]}-{k[1]}": round(v / n, 4)
                  for k, v in sorted(raw.items(), reverse=True)}

    return series_win_a, score_dist


def best_maps(profile):
    scores = []
    for mn in MAPS:
        mp = profile['by_map'].get(mn, {})
        if mp.get('maps_played', 0) == 0:
            scores.append((mn, None)); continue
        at = safe_div(mp.get('atk_won', 0), mp.get('atk_total', 1))
        df = safe_div(mp.get('def_won', 0), mp.get('def_total', 1))
        wr = safe_div(mp.get('maps_won', 0), mp.get('maps_played', 1))
        scores.append((mn, round(0.40 * wr + 0.30 * at + 0.30 * df, 3)))
    scores.sort(key=lambda x: (x[1] is None, -(x[1] or 0)))
    return [{'map': m, 'score': s} for m, s in scores]


# ─── HELPER: mapas con datos para un equipo ─────────────────────────────────
def _get_maps_with_data(full_name):
    """Retorna set de map_names que tienen datos en la DB para este equipo."""
    rows = query("""
        SELECT DISTINCT mp.map_name
        FROM maps mp
        JOIN matches mt ON mp.match_id = mt.match_id
        WHERE mt.team_a = ? OR mt.team_b = ?
    """, [full_name, full_name])
    return {r['map_name'] for r in rows if r['map_name']}


# ─── RUTAS ────────────────────────────────────────────────────────────────────
@aletheia_bp.route('/api/aletheia/equipos', methods=['GET'])
def get_teams():
    result = []
    try:
        teams_from_matches = query("""
            SELECT DISTINCT team_a AS name FROM matches
            UNION SELECT DISTINCT team_b FROM matches ORDER BY name
        """)
        stats_by_abbrev = {r['team_name']: r for r in query("""
            SELECT ps.team_name, COUNT(DISTINCT ps.map_id) AS maps_played,
                   ROUND(AVG(ps.rating), 2) AS avg_rating
            FROM player_stats ps JOIN maps mp ON ps.map_id = mp.map_id
            JOIN matches mt ON mp.match_id = mt.match_id GROUP BY ps.team_name
        """)}
        for t in teams_from_matches:
            nm = t['name'] if t['name'] else ''
            if not nm: continue
            ab = TEAM_ABBREV.get(nm, nm)
            st = stats_by_abbrev.get(ab, {})
            result.append({
                'name': nm, 'abbrev': ab,
                'maps_played': int(st.get('maps_played') or 0),
                'avg_rating': float(st.get('avg_rating') or 0),
            })
        return jsonify({'ok': True, 'teams': result})
    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500


@aletheia_bp.route('/api/aletheia/mapas_disponibles', methods=['POST'])
def mapas_disponibles():
    """Retorna la lista de mapas con datos en la DB para ambos equipos."""
    data = request.get_json()
    team_a = data.get('team_a', '').strip()
    team_b = data.get('team_b', '').strip()
    if not team_a or not team_b:
        return jsonify({'ok': False, 'error': 'Se requieren dos equipos.'}), 400
    try:
        maps_a = _get_maps_with_data(team_a)
        maps_b = _get_maps_with_data(team_b)
        # Un mapa es "disponible" si AL MENOS uno de los equipos tiene datos
        available = sorted(maps_a | maps_b)
        # Filtrar solo mapas del pool conocido
        available = [m for m in available if m in MAPS]
        return jsonify({'ok': True, 'maps': available})
    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500


@aletheia_bp.route('/api/aletheia/jugadores', methods=['GET'])
def get_jugadores():
    """Retorna los jugadores activos de un equipo con sus agentes históricos.
    Parámetro opcional map_name para filtrar agentes usados en un mapa específico."""
    team = request.args.get('team', '').strip()
    map_name = request.args.get('map_name', '').strip() or None
    if not team:
        return jsonify({'ok': False, 'error': 'Se requiere parámetro team'}), 400
    try:
        # Si se especifica mapa, filtrar por ese mapa
        if map_name:
            rows = query("""
                SELECT ps.player_name,
                       GROUP_CONCAT(DISTINCT ps.agent) AS agents,
                       COUNT(DISTINCT ps.map_id) AS games,
                       ROUND(AVG(ps.rating), 2) AS avg_rating,
                       MAX(mt.match_date) AS last_played
                FROM player_stats ps
                JOIN maps mp ON ps.map_id = mp.map_id
                JOIN matches mt ON mp.match_id = mt.match_id
                WHERE ps.team_name = ? AND mp.map_name = ?
                GROUP BY ps.player_name
                ORDER BY MAX(mt.match_date) DESC, COUNT(DISTINCT ps.map_id) DESC
                LIMIT 7
            """, [team, map_name])
        else:
            rows = query("""
                SELECT ps.player_name,
                       GROUP_CONCAT(DISTINCT ps.agent) AS agents,
                       COUNT(DISTINCT ps.map_id) AS games,
                       ROUND(AVG(ps.rating), 2) AS avg_rating,
                       MAX(mt.match_date) AS last_played
                FROM player_stats ps
                JOIN maps mp ON ps.map_id = mp.map_id
                JOIN matches mt ON mp.match_id = mt.match_id
                WHERE ps.team_name = ?
                GROUP BY ps.player_name
                ORDER BY MAX(mt.match_date) DESC, COUNT(DISTINCT ps.map_id) DESC
                LIMIT 7
            """, [team])
        players = []
        for r in rows:
            agents_list = (r['agents'] or '').split(',')
            # Agente más frecuente primero (filtrado por mapa si aplica)
            if map_name:
                agent_freq = query("""
                    SELECT ps.agent, COUNT(*) AS cnt FROM player_stats ps
                    JOIN maps mp ON ps.map_id = mp.map_id
                    WHERE ps.player_name = ? AND ps.team_name = ? AND mp.map_name = ?
                    GROUP BY ps.agent ORDER BY cnt DESC
                """, [r['player_name'], team, map_name])
            else:
                agent_freq = query("""
                    SELECT agent, COUNT(*) AS cnt FROM player_stats
                    WHERE player_name = ? AND team_name = ?
                    GROUP BY agent ORDER BY cnt DESC
                """, [r['player_name'], team])
            sorted_agents = [a['agent'] for a in agent_freq if a['agent']]
            players.append({
                'name': r['player_name'],
                'agents': sorted_agents,
                'default_agent': sorted_agents[0] if sorted_agents else 'Unknown',
                'games': r['games'],
                'avg_rating': float(r['avg_rating'] or 1.0),
            })
        return jsonify({'ok': True, 'players': players[:5], 'team': team})
    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500


@aletheia_bp.route('/api/aletheia/analizar', methods=['POST'])
def analizar():
    """
    Explorador de mapas avanzado.
    Añade a cada mapa: halftime prediction (punto 1) + OT analysis (punto 2).
    """
    data = request.get_json()
    team_a = data.get('team_a', '').strip()
    team_b = data.get('team_b', '').strip()
    n_sim = max(1000, min(int(data.get('simulations', 10000)), 50000))

    if not team_a or not team_b:
        return jsonify({'ok': False, 'error': 'Se requieren dos equipos.'}), 400
    if team_a == team_b:
        return jsonify({'ok': False, 'error': 'Los equipos deben ser distintos.'}), 400

    try:
        prof_a = get_team_profile(team_a)
        prof_b = get_team_profile(team_b)
        h2h = get_h2h(team_a, team_b)
        ot_prof_a = get_team_ot_profile(team_a)
        ot_prof_b = get_team_ot_profile(team_b)

        # Filtrar solo mapas con datos para al menos uno de los equipos
        maps_a = _get_maps_with_data(team_a)
        maps_b = _get_maps_with_data(team_b)
        available_maps = [m for m in MAPS if m in (maps_a | maps_b)]

        results = []
        for map_name in available_maps:
            for starting_side in ['attack', 'defense']:
                a_starts_atk = (starting_side == 'attack')

                p_a_atk = compute_round_prob(prof_a, prof_b, map_name, True, h2h)
                p_a_def = compute_round_prob(prof_a, prof_b, map_name, False, h2h)
                det = monte_carlo(p_a_atk, p_a_def, a_starts_atk, n_sim, return_details=True)

                # PUNTO 1: Halftime con economía
                ht = simulate_halftime(p_a_atk, p_a_def, prof_a, prof_b,
                                       map_name, a_starts_atk, n=max(n_sim // 2, 3000))

                # PUNTO 2: Análisis de OT
                ot = compute_ot_analysis(
                    ot_prof_a, ot_prof_b, det['ot_pct'],
                    p_a_atk, p_a_def,
                    prof_a['abbrev'], prof_b['abbrev']
                )

                a_maps = prof_a['by_map'].get(map_name, {}).get('maps_played', 0)
                b_maps = prof_b['by_map'].get(map_name, {}).get('maps_played', 0)
                a_recency = prof_a['by_map'].get(map_name, {}).get('recency', 0.3)
                b_recency = prof_b['by_map'].get(map_name, {}).get('recency', 0.3)

                results.append({
                    'map': map_name, 'start': starting_side,
                    'win_a': round(det['win_prob'], 4),
                    'win_b': round(1.0 - det['win_prob'], 4),
                    'p_round_atk': round(p_a_atk, 4),
                    'p_round_def': round(p_a_def, 4),
                    'confidence': map_confidence(a_maps, b_maps, a_recency, b_recency),
                    'a_maps': a_maps, 'b_maps': b_maps,
                    'a_recency': round(a_recency, 2), 'b_recency': round(b_recency, 2),
                    'avg_score_a': det['avg_score_a'], 'avg_score_b': det['avg_score_b'],
                    'avg_rounds': det['avg_rounds'], 'ot_pct': det['ot_pct'],
                    'score_freq': det['score_freq'], 'modal_score': det['modal_score'],
                    'halftime': ht,
                    'ot_analysis': ot,
                    'ot_closer_a': ot.get('ot_win_a', 0.5),
                })

        gl_a = prof_a['global']; gl_b = prof_b['global']
        summary = {
            'team_a': {
                'name': team_a, 'abbrev': prof_a['abbrev'],
                'maps_played': gl_a.get('maps_played', 0),
                'avg_rating': round(gl_a.get('avg_rating', 1.0), 2),
                'atk_wr': round(gl_a.get('atk_wr', 0.5) * 100, 1),
                'def_wr': round(gl_a.get('def_wr', 0.5) * 100, 1),
                'map_wr': round(gl_a.get('map_wr', 0.5) * 100, 1),
                'pistol_wr': round(gl_a.get('pistol_wr', 0.5) * 100, 1),
                'full_buy_wr': round(gl_a.get('full_buy_wr', 0.5) * 100, 1),
                'clutch_pm': round(gl_a.get('clutch_per_map', 0), 1),
                'duel_wr': round(gl_a.get('duel_wr', 0.5) * 100, 1),
                'best_maps': best_maps(prof_a),
                'ot_profile': ot_prof_a,
            },
            'team_b': {
                'name': team_b, 'abbrev': prof_b['abbrev'],
                'maps_played': gl_b.get('maps_played', 0),
                'avg_rating': round(gl_b.get('avg_rating', 1.0), 2),
                'atk_wr': round(gl_b.get('atk_wr', 0.5) * 100, 1),
                'def_wr': round(gl_b.get('def_wr', 0.5) * 100, 1),
                'map_wr': round(gl_b.get('map_wr', 0.5) * 100, 1),
                'pistol_wr': round(gl_b.get('pistol_wr', 0.5) * 100, 1),
                'full_buy_wr': round(gl_b.get('full_buy_wr', 0.5) * 100, 1),
                'clutch_pm': round(gl_b.get('clutch_per_map', 0), 1),
                'duel_wr': round(gl_b.get('duel_wr', 0.5) * 100, 1),
                'best_maps': best_maps(prof_b),
                'ot_profile': ot_prof_b,
            },
            'h2h': h2h,
            'operator_analysis': get_operator_analysis(prof_a['abbrev'], prof_b['abbrev']),
            'star_player_a': get_star_player_analysis(prof_a['abbrev'], prof_b['abbrev']),
            'star_player_b': get_star_player_analysis(prof_b['abbrev'], prof_a['abbrev']),
            'consistency_a': get_player_consistency(prof_a['abbrev']),
            'consistency_b': get_player_consistency(prof_b['abbrev']),
        }

        return jsonify({
            'ok': True, 'results': results, 'summary': summary, 'simulations': n_sim,
        })

    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500


@aletheia_bp.route('/api/aletheia/partido', methods=['POST'])
def partido():
    """
    Simula un partido con mapas y lados definidos.
    Incluye halftime + OT por mapa, y serie secuencial (punto 6 prep).
    """
    data = request.get_json()
    team_a = data.get('team_a', '').strip()
    team_b = data.get('team_b', '').strip()
    maps_config = data.get('maps', [])
    n_sim = max(1000, min(int(data.get('simulations', 10000)), 50000))
    agent_overrides = data.get('agent_overrides', None)

    if not team_a or not team_b:
        return jsonify({'ok': False, 'error': 'Se requieren dos equipos.'}), 400
    if team_a == team_b:
        return jsonify({'ok': False, 'error': 'Los equipos deben ser distintos.'}), 400
    if not maps_config or len(maps_config) > 5:
        return jsonify({'ok': False, 'error': 'Entre 1 y 5 mapas.'}), 400

    bad = [c.get('map_name', '') for c in maps_config if c.get('map_name') not in MAPS]
    if bad:
        return jsonify({'ok': False, 'error': f'Mapas no válidos: {", ".join(bad)}'}), 400

    # Función para calcular ajuste Operator basado en agentes seleccionados
    def operator_adjustment(mn, overrides):
        if not overrides:
            return 0.0
        map_w = OPERATOR_MAP_WEIGHT.get(mn, 0.5)
        a_agents = [p.get('agent', '') for p in overrides.get('team_a', [])]
        b_agents = [p.get('agent', '') for p in overrides.get('team_b', [])]
        a_has_op = any(ag in OPERATOR_AGENTS for ag in a_agents)
        b_has_op = any(ag in OPERATOR_AGENTS for ag in b_agents)
        # Si A tiene Operator y B no → bonus para A proporcional al mapa
        # Si B tiene y A no → penalty para A
        # Si ambos o ninguno → neutral
        if a_has_op and not b_has_op:
            return map_w * 0.025  # max +2.25% en Breeze
        elif b_has_op and not a_has_op:
            return -map_w * 0.025
        return 0.0

    try:
        prof_a = get_team_profile(team_a)
        prof_b = get_team_profile(team_b)
        h2h = get_h2h(team_a, team_b)
        ot_prof_a = get_team_ot_profile(team_a)
        ot_prof_b = get_team_ot_profile(team_b)

        map_results = []
        map_probs = []

        for cfg in maps_config:
            mn = cfg['map_name']
            a_starts_atk = bool(cfg.get('a_starts_atk', True))

            p_a_atk = compute_round_prob(prof_a, prof_b, mn, True, h2h)
            p_a_def = compute_round_prob(prof_a, prof_b, mn, False, h2h)

            # Ajuste Operator basado en selección de agentes
            op_adj = operator_adjustment(mn, agent_overrides)
            p_a_atk = max(0.05, min(0.95, p_a_atk + op_adj))
            p_a_def = max(0.05, min(0.95, p_a_def + op_adj * 0.6))

            det = monte_carlo(p_a_atk, p_a_def, a_starts_atk, n_sim, return_details=True)

            ht = simulate_halftime(p_a_atk, p_a_def, prof_a, prof_b,
                                   mn, a_starts_atk, n=max(n_sim // 2, 3000))
            ot = compute_ot_analysis(
                ot_prof_a, ot_prof_b, det['ot_pct'],
                p_a_atk, p_a_def,
                prof_a['abbrev'], prof_b['abbrev']
            )

            a_maps = prof_a['by_map'].get(mn, {}).get('maps_played', 0)
            b_maps = prof_b['by_map'].get(mn, {}).get('maps_played', 0)
            a_recency = prof_a['by_map'].get(mn, {}).get('recency', 0.3)
            b_recency = prof_b['by_map'].get(mn, {}).get('recency', 0.3)

            map_results.append({
                'map': mn, 'a_starts_atk': a_starts_atk,
                'win_a': round(det['win_prob'], 4),
                'win_b': round(1.0 - det['win_prob'], 4),
                'p_round_atk': round(p_a_atk, 4),
                'p_round_def': round(p_a_def, 4),
                'confidence': map_confidence(a_maps, b_maps, a_recency, b_recency),
                'a_maps': a_maps, 'b_maps': b_maps,
                'avg_score_a': det['avg_score_a'], 'avg_score_b': det['avg_score_b'],
                'avg_rounds': det['avg_rounds'], 'ot_pct': det['ot_pct'],
                'score_freq': det['score_freq'], 'modal_score': det['modal_score'],
                'halftime': ht,
                'ot_analysis': ot,
                'ot_closer_a': ot.get('ot_win_a', 0.5),
            })
            map_probs.append(det['win_prob'])

        # SERIE SECUENCIAL (punto 6)
        series_win_a, score_dist = monte_carlo_series_sequential(map_probs, n_sim)
        total_maps = len(maps_config)
        maps_to_win = (total_maps + 1) // 2

        gl_a = prof_a['global']; gl_b = prof_b['global']

        # PUNTO 6: Advertencia de lógica secuencial
        sequential_warning = None
        if total_maps >= 3 and map_probs:
            fav_series = prof_a['abbrev'] if series_win_a >= 0.5 else prof_b['abbrev']
            fav_map1 = prof_a['abbrev'] if map_probs[0] >= 0.5 else prof_b['abbrev']
            if fav_series != fav_map1 and abs(map_probs[0] - 0.5) > 0.05:
                sequential_warning = (
                    f"El favorito de la serie ({fav_series}) no es favorito en el mapa 1. "
                    f"Un resultado {maps_to_win}-0 es poco probable porque debe ganar "
                    f"el primer mapa donde {fav_map1} tiene ventaja."
                )

        return jsonify({
            'ok': True,
            'map_results': map_results,
            'series': {
                'win_a': round(series_win_a, 4),
                'win_b': round(1.0 - series_win_a, 4),
                'score_dist': score_dist,
                'maps_to_win': maps_to_win,
                'total_maps': total_maps,
                'format': f'Bo{total_maps}',
            },
            'summary': {
                'team_a': {'name': team_a, 'abbrev': prof_a['abbrev'],
                           'atk_wr': round(gl_a.get('atk_wr', 0.5) * 100, 1),
                           'def_wr': round(gl_a.get('def_wr', 0.5) * 100, 1)},
                'team_b': {'name': team_b, 'abbrev': prof_b['abbrev'],
                           'atk_wr': round(gl_b.get('atk_wr', 0.5) * 100, 1),
                           'def_wr': round(gl_b.get('def_wr', 0.5) * 100, 1)},
                'h2h': h2h,
            },
            'sequential_warning': sequential_warning,
            'simulations': n_sim,
        })

    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500


@aletheia_bp.route('/api/aletheia/recalcular_mapa', methods=['POST'])
def recalcular_mapa():
    """
    Re-simula un solo mapa con agent overrides específicos.
    Usado por la selección de agentes por mapa en el frontend.
    """
    data = request.get_json()
    team_a = data.get('team_a', '').strip()
    team_b = data.get('team_b', '').strip()
    map_name = data.get('map_name', '').strip()
    a_starts_atk = bool(data.get('a_starts_atk', True))
    n_sim = max(1000, min(int(data.get('simulations', 10000)), 50000))
    agent_overrides = data.get('agent_overrides', None)

    if not team_a or not team_b:
        return jsonify({'ok': False, 'error': 'Se requieren dos equipos.'}), 400
    if map_name not in MAPS:
        return jsonify({'ok': False, 'error': f'Mapa no válido: {map_name}'}), 400

    try:
        prof_a = get_team_profile(team_a)
        prof_b = get_team_profile(team_b)
        h2h = get_h2h(team_a, team_b)
        ot_prof_a = get_team_ot_profile(team_a)
        ot_prof_b = get_team_ot_profile(team_b)

        p_a_atk = compute_round_prob(prof_a, prof_b, map_name, True, h2h)
        p_a_def = compute_round_prob(prof_a, prof_b, map_name, False, h2h)

        # Ajuste Operator basado en agentes seleccionados
        if agent_overrides:
            map_w = OPERATOR_MAP_WEIGHT.get(map_name, 0.5)
            a_agents = [p.get('agent', '') for p in agent_overrides.get('team_a', [])]
            b_agents = [p.get('agent', '') for p in agent_overrides.get('team_b', [])]
            a_has_op = any(ag in OPERATOR_AGENTS for ag in a_agents)
            b_has_op = any(ag in OPERATOR_AGENTS for ag in b_agents)
            if a_has_op and not b_has_op:
                op_adj = map_w * 0.025
            elif b_has_op and not a_has_op:
                op_adj = -map_w * 0.025
            else:
                op_adj = 0.0
            p_a_atk = max(0.05, min(0.95, p_a_atk + op_adj))
            p_a_def = max(0.05, min(0.95, p_a_def + op_adj * 0.6))

        det = monte_carlo(p_a_atk, p_a_def, a_starts_atk, n_sim, return_details=True)

        ht = simulate_halftime(p_a_atk, p_a_def, prof_a, prof_b,
                               map_name, a_starts_atk, n=max(n_sim // 2, 3000))
        ot = compute_ot_analysis(
            ot_prof_a, ot_prof_b, det['ot_pct'],
            p_a_atk, p_a_def,
            prof_a['abbrev'], prof_b['abbrev']
        )

        a_maps = prof_a['by_map'].get(map_name, {}).get('maps_played', 0)
        b_maps = prof_b['by_map'].get(map_name, {}).get('maps_played', 0)
        a_recency = prof_a['by_map'].get(map_name, {}).get('recency', 0.3)
        b_recency = prof_b['by_map'].get(map_name, {}).get('recency', 0.3)

        result = {
            'map': map_name, 'a_starts_atk': a_starts_atk,
            'win_a': round(det['win_prob'], 4),
            'win_b': round(1.0 - det['win_prob'], 4),
            'p_round_atk': round(p_a_atk, 4),
            'p_round_def': round(p_a_def, 4),
            'confidence': map_confidence(a_maps, b_maps, a_recency, b_recency),
            'a_maps': a_maps, 'b_maps': b_maps,
            'avg_score_a': det['avg_score_a'], 'avg_score_b': det['avg_score_b'],
            'avg_rounds': det['avg_rounds'], 'ot_pct': det['ot_pct'],
            'score_freq': det['score_freq'], 'modal_score': det['modal_score'],
            'halftime': ht,
            'ot_analysis': ot,
            'ot_closer_a': ot.get('ot_win_a', 0.5),
        }

        return jsonify({'ok': True, 'result': result})

    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500


@aletheia_bp.route('/api/aletheia/recalcular_serie', methods=['POST'])
def recalcular_serie():
    """
    Recalcula la probabilidad de serie con probabilidades de mapa actualizadas.
    """
    data = request.get_json()
    map_probs = data.get('map_probs', [])
    n_sim = max(1000, min(int(data.get('simulations', 10000)), 50000))

    if not map_probs or len(map_probs) > 5:
        return jsonify({'ok': False, 'error': 'Entre 1 y 5 mapas.'}), 400

    try:
        series_win_a, score_dist = monte_carlo_series_sequential(map_probs, n_sim)
        total_maps = len(map_probs)
        maps_to_win = (total_maps + 1) // 2

        return jsonify({
            'ok': True,
            'series': {
                'win_a': round(series_win_a, 4),
                'win_b': round(1.0 - series_win_a, 4),
                'score_dist': score_dist,
                'maps_to_win': maps_to_win,
                'total_maps': total_maps,
                'format': f'Bo{total_maps}',
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500