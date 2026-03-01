"""
ALETHEIA — Predecir Blueprint  v2
Simulación Monte Carlo para predecir resultados de partidos.

Mejoras respecto a v1:
  · YEAR_FILTER  — solo datos del año en curso (mismos jugadores, mismo meta)
  · Decaimiento temporal exponencial — datos recientes pesan más
  · Ajuste por fuerza del rival — ganar contra tops vale más que contra débiles
  · Confianza incluye recencia de datos (no solo tamaño de muestra)

Señales ponderadas para P(ronda):
  40% — Histórico ATK/DEF en ese mapa (suavizado Bayesiano, decay-weighted)
  28% — Diferencial de habilidad (rating × ACS × ADR × FK/FD)
  18% — Eficiencia económica (pistol, full-buy, eco WR)
   9% — Factor clutch y multikills
   5% — H2H del año + tendencias de veto
"""

from flask import Blueprint, jsonify, request
import numpy as np
import math
from datetime import date as date_type, datetime
try:
    from backend.conexion import get_conn, release_conn
except ImportError:
    from conexion import get_conn, release_conn

predecir_bp = Blueprint('predecir', __name__)

# ─── CONFIGURACIÓN DEL MODELO ────────────────────────────────────────────────
MAPS            = ['Abyss', 'Ascent', 'Bind', 'Corrode', 'Fracture', 'Haven', 'Icebox', 'Lotus', 'Pearl', 'Split', 'Sunset']
YEAR_FILTER     = '2026'   # ← cambiar aquí cuando llegue 2026
DECAY_HALF_LIFE = 75       # días: datos de hace 75 días valen ~50% vs datos recientes

# Meta global ATK por mapa (% de rondas que gana el atacante)
MAP_META_ATK = {
    'Abyss':    0.566,  # datos reales del dataset 2026
    'Ascent':   0.455,  # DEF-heavy
    'Bind':     0.507,
    'Corrode':  0.476,  # DEF-heavy
    'Fracture': 0.502,
    'Haven':    0.512,
    'Icebox':   0.533,  # ATK-heavy
    'Lotus':    0.500,
    'Pearl':    0.501,
    'Split':    0.504,
    'Sunset':   0.517,
}

TEAM_ABBREV = {
    '100 Thieves':        '100T',
    '2Game Esports':      '2G',
    'Apeks':              'APK',
    'BBL Esports':        'BBL',
    'BOOM Esports':       'BME',
    'Cloud9':             'C9',
    'DRX':                'DRX',
    'DetonatioN FocusMe': 'DFM',
    'Evil Geniuses':      'EG',
    'FNATIC':             'FNC',
    'FURIA':              'FUR',
    'FUT Esports':        'FUT',
    'G2 Esports':         'G2',
    'GIANTX':             'GX',
    'Gen.G':              'GEN',
    'Gentle Mates':       'M8',
    'Global Esports':     'GE',
    'KRÜ Esports':        'KRÜ',
    'Karmine Corp':       'KC',
    'LEVIATÁN':           'LEV',
    'LOUD':               'LOUD',
    'MIBR':               'MIBR',
    'Movistar KOI(KOI)':  'MKOI',
    'NRG':                'NRG',
    'Natus Vincere':      'NAVI',
    'Nongshim RedForce':  'NS',
    'Paper Rex':          'PRX',
    'Rex Regum Qeon':     'RRQ',
    'Sentinels':          'SEN',
    'T1':                 'T1',
    'TALON':              'TLN',
    'Team Heretics':      'TH',
    'Team Liquid':        'TL',
    'Team Secret':        'TS',
    'Team Vitality':      'VIT',
    'ZETA DIVISION':      'ZETA',
}

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
    """
    Decaimiento exponencial respecto a ref_date (hoy por defecto).
    Un partido de hace DECAY_HALF_LIFE días vale 0.5x vs uno de hoy.
    Kickoff (ene-feb) → ~0.25-0.40 · Champions (ago) → ~0.80-1.00
    """
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


# ─── ÍNDICE DE FUERZA DE EQUIPOS ──────────────────────────────────────────────
_strength_cache = None

def get_strength_index():
    """
    Calcula el map win rate de cada equipo en el año actual.
    Usado para ponderar resultados: ganar contra un top-team vale más.
    Cacheado en memoria durante la sesión del servidor.
    """
    global _strength_cache
    if _strength_cache is not None:
        return _strength_cache

    try:
        rows = query(f"""
            SELECT team, SUM(won) AS won, SUM(played) AS played
            FROM (
                SELECT mt.team_a AS team,
                       SUM(CASE WHEN mt.winner = mt.team_a THEN 1 ELSE 0 END) AS won,
                       COUNT(*) AS played
                FROM maps mp
                JOIN matches mt ON mp.match_id = mt.match_id
                GROUP BY mt.team_a
                UNION ALL
                SELECT mt.team_b,
                       SUM(CASE WHEN mt.winner = mt.team_b THEN 1 ELSE 0 END),
                       COUNT(*)
                FROM maps mp
                JOIN matches mt ON mp.match_id = mt.match_id
                GROUP BY mt.team_b
            )
            GROUP BY team
        """)
        _strength_cache = {
            r['team']: safe_div(r['won'] or 0, r['played'] or 0)
            for r in rows if r['team']
        }
    except Exception:
        _strength_cache = {}

    return _strength_cache


def combined_weight(match_date_str, opponent_name, ref_date=None):
    """
    Peso combinado = decay_temporal × ajuste_fuerza_rival.
    
    Ajuste de fuerza: rival con 70% WR da peso 1.10x, rival con 30% WR da 0.90x.
    Rango: ~0.85x (rival muy débil reciente) — 1.10x (rival top reciente).
    """
    dw  = decay_weight(match_date_str, ref_date)
    si  = get_strength_index()
    opp_wr = si.get(opponent_name, 0.5)
    # Ajuste centrado en 0: ±0.20 según fuerza del rival
    strength_adj = 1.0 + 0.40 * (opp_wr - 0.5)
    return dw * strength_adj


# ─── PERFIL DE EQUIPO ─────────────────────────────────────────────────────────
def get_team_profile(full_name):
    """
    Construye el perfil estadístico completo de un equipo.

    Cambios v2:
      - Filtra por YEAR_FILTER en todas las queries
      - Agrega con peso = decay × opp_strength (en lugar de conteos crudos)
      - Guarda 'recency' por mapa para la métrica de confianza
    """
    abbrev   = TEAM_ABBREV.get(full_name, full_name)
    ref_date = date_type.today()

    # ── 1. Rondas por mapa — una fila por mapa jugado, con fecha y rival ──────
    round_rows = query(f"""
        SELECT mp.map_name,
               mt.match_date,
               mt.team_b                                    AS opponent,
               mp.score_a_attack                            AS atk_won,
               (mp.score_a_attack + mp.score_b_defense)    AS atk_total,
               mp.score_a_defense                           AS def_won,
               (mp.score_a_defense + mp.score_b_attack)    AS def_total,
               CASE WHEN mt.winner = mt.team_a THEN 1 ELSE 0 END AS map_won
        FROM maps mp
        JOIN matches mt ON mp.match_id = mt.match_id
        WHERE mt.team_a = ?         UNION ALL
        SELECT mp.map_name,
               mt.match_date,
               mt.team_a                                    AS opponent,
               mp.score_b_attack,
               (mp.score_b_attack + mp.score_a_defense),
               mp.score_b_defense,
               (mp.score_b_defense + mp.score_a_attack),
               CASE WHEN mt.winner = mt.team_b THEN 1 ELSE 0 END
        FROM maps mp
        JOIN matches mt ON mp.match_id = mt.match_id
        WHERE mt.team_b = ?     """, [full_name, full_name])

    # ── 2. Stats de jugadores por mapa+lado con fecha ─────────────────────────
    player_rows = query(f"""
        SELECT mp.map_name, ps.side, mt.match_date,
               ps.rating, ps.acs, ps.adr, ps.kast, ps.fk, ps.fd
        FROM player_stats ps
        JOIN maps mp ON ps.map_id = mp.map_id
        JOIN matches mt ON mp.match_id = mt.match_id
        WHERE ps.team_name = ?     """, [abbrev])

    # ── 3. Economía por mapa con fecha ────────────────────────────────────────
    econ_rows = query(f"""
        SELECT mp.map_name, mt.match_date,
               es.pistol_won, 2 AS pistol_total,
               es.full_buy_played AS fb_played, es.full_buy_won AS fb_won,
               es.eco_played, es.eco_won,
               es.semi_buy_played AS sb_played, es.semi_buy_won AS sb_won
        FROM economy_summary es
        JOIN maps mp ON es.map_id = mp.map_id
        JOIN matches mt ON mp.match_id = mt.match_id
        WHERE es.team = ?     """, [abbrev])

    # ── 4. Clutch/multikills ──────────────────────────────────────────────────
    clutch_rows = query(f"""
        SELECT mp.map_name,
               SUM(mc.v1 + mc.v2*2 + mc.v3*3 + mc.v4*4 + mc.v5*5) AS clutch_score,
               SUM(mc.k2 + mc.k3*2 + mc.k4*3 + mc.k5*4)            AS mk_score,
               COUNT(DISTINCT mc.map_id)                             AS maps_count
        FROM multikills_clutches mc
        JOIN maps mp ON mc.map_id = mp.map_id
        JOIN matches mt ON mp.match_id = mt.match_id
        WHERE mc.player_name IN (
            SELECT DISTINCT player_name FROM player_stats WHERE team_name = ?
        )
                GROUP BY mp.map_name
    """, [abbrev])

    # ── 5. Duelos ─────────────────────────────────────────────────────────────
    duel_rows = query(f"""
        SELECT mp.map_name,
               SUM(CASE WHEN d.player_a IN (
                   SELECT player_name FROM player_stats WHERE team_name = ?
               ) THEN d.kills_a ELSE d.kills_b END) AS kills_for,
               SUM(CASE WHEN d.player_a IN (
                   SELECT player_name FROM player_stats WHERE team_name = ?
               ) THEN d.kills_b ELSE d.kills_a END) AS kills_against
        FROM duels d
        JOIN maps mp ON d.map_id = mp.map_id
        JOIN matches mt ON mp.match_id = mt.match_id
        AND (d.player_a IN (SELECT player_name FROM player_stats WHERE team_name = ?)
          OR d.player_b IN (SELECT player_name FROM player_stats WHERE team_name = ?))
        GROUP BY mp.map_name
    """, [abbrev, abbrev, abbrev, abbrev])

    # ── 6. Veto ───────────────────────────────────────────────────────────────
    veto_rows = query(f"""
        SELECT mv.map_name,
               SUM(CASE WHEN mv.action = 'pick' THEN 1 ELSE 0 END) AS picks,
               SUM(CASE WHEN mv.action = 'ban'  THEN 1 ELSE 0 END) AS bans
        FROM match_veto mv
        JOIN matches mt ON mv.match_id = mt.match_id
        WHERE ((mv.team = 'a' AND mt.team_a = ?) OR (mv.team = 'b' AND mt.team_b = ?))
                GROUP BY mv.map_name
    """, [full_name, full_name])

    # ─── Ensamblar perfil con ponderación decay × opp_strength ───────────────
    profile = {'team': full_name, 'abbrev': abbrev, 'by_map': {}, 'global': {}}

    # — Rondas
    by_map_rnd = {}
    g_aw = g_at = g_dw = g_dt = g_mw = g_wt = 0.0

    for r in round_rows:
        mn = r['map_name']
        if not mn:
            continue
        w = combined_weight(r['match_date'], r['opponent'], ref_date)

        if mn not in by_map_rnd:
            by_map_rnd[mn] = {
                'aw': 0.0, 'at': 0.0, 'dw_': 0.0, 'dt': 0.0,
                'mw': 0.0, 'wt': 0.0, 'cnt': 0, 'decay_sum': 0.0,
            }
        bm = by_map_rnd[mn]
        bm['aw']  += (r['atk_won']  or 0) * w
        bm['at']  += (r['atk_total'] or 0) * w
        bm['dw_'] += (r['def_won']  or 0) * w
        bm['dt']  += (r['def_total'] or 0) * w
        bm['mw']  += (r['map_won']  or 0) * w
        bm['wt']  += w
        bm['cnt'] += 1
        bm['decay_sum'] += decay_weight(r['match_date'], ref_date)

        g_aw += (r['atk_won']  or 0) * w
        g_at += (r['atk_total'] or 0) * w
        g_dw += (r['def_won']  or 0) * w
        g_dt += (r['def_total'] or 0) * w
        g_mw += (r['map_won']  or 0) * w
        g_wt += w

    for mn, bm in by_map_rnd.items():
        d = profile['by_map'].setdefault(mn, {})
        d['atk_won']    = bm['aw']
        d['atk_total']  = bm['at']
        d['def_won']    = bm['dw_']
        d['def_total']  = bm['dt']
        d['maps_won']   = bm['mw']
        d['maps_played'] = bm['cnt']
        # recency: promedio de decay weights individuales (1.0 = todo reciente)
        d['recency']    = bm['decay_sum'] / bm['cnt'] if bm['cnt'] else 0.3

    profile['global'].update({
        'atk_wr':      safe_div(g_aw, g_at),
        'def_wr':      safe_div(g_dw, g_dt),
        'map_wr':      safe_div(g_mw, g_wt),
        'maps_played': sum(bm['cnt'] for bm in by_map_rnd.values()),
    })

    # — Player stats
    by_ms = {}
    all_rw = all_rwt = 0.0

    for r in player_rows:
        mn = r['map_name']; side = r['side']
        if not mn or not side:
            continue
        dw = decay_weight(r['match_date'], ref_date)
        key = (mn, side)
        if key not in by_ms:
            by_ms[key] = {
                'rw': 0.0, 'aw': 0.0, 'adw': 0.0, 'kw': 0.0,
                'fk': 0, 'fd': 0, 'wt': 0.0,
            }
        s = by_ms[key]
        s['rw']  += (r['rating'] or 1.0) * dw
        s['aw']  += (r['acs']    or 200)  * dw
        s['adw'] += (r['adr']    or 130)  * dw
        s['kw']  += (r['kast']   or 70)   * dw
        s['fk']  += r['fk'] or 0
        s['fd']  += r['fd'] or 0
        s['wt']  += dw
        all_rw  += (r['rating'] or 1.0) * dw
        all_rwt += dw

    for (mn, side), s in by_ms.items():
        d = profile['by_map'].setdefault(mn, {})
        wt = max(s['wt'], 1e-9)
        d[f'{side}_rating'] = s['rw']  / wt
        d[f'{side}_acs']    = s['aw']  / wt
        d[f'{side}_adr']    = s['adw'] / wt
        d[f'{side}_kast']   = s['kw']  / wt
        d['fk_fd'] = safe_div(s['fk'], s['fk'] + s['fd'])

    profile['global']['avg_rating'] = safe_div(all_rw, all_rwt, 1.0)

    # — Economía
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
        e['pw']   += (r['pistol_won']    or 0) * dw
        e['pt']   += (r['pistol_total']  or 2) * dw
        e['fbw']  += (r['fb_won']        or 0) * dw
        e['fbp']  += (r['fb_played']     or 0) * dw
        e['ecow'] += (r['eco_won']       or 0) * dw
        e['ecop'] += (r['eco_played']    or 0) * dw
        e['sbw']  += (r['sb_won']        or 0) * dw
        e['sbp']  += (r['sb_played']     or 0) * dw
        g_pw  += (r['pistol_won']  or 0) * dw
        g_pt  += (r['pistol_total'] or 2) * dw
        g_fbw += (r['fb_won']      or 0) * dw
        g_fbp += (r['fb_played']   or 0) * dw

    for mn, e in by_me.items():
        d = profile['by_map'].setdefault(mn, {})
        d['pistol_wr']   = safe_div(e['pw'],   e['pt'])
        d['full_buy_wr'] = safe_div(e['fbw'],  e['fbp'])
        d['eco_wr']      = safe_div(e['ecow'], e['ecop'])
        d['semi_buy_wr'] = safe_div(e['sbw'],  e['sbp'])

    profile['global']['pistol_wr']   = safe_div(g_pw,  g_pt)
    profile['global']['full_buy_wr'] = safe_div(g_fbw, g_fbp)

    # — Clutch
    total_cs = total_ms = total_mc = 0
    for r in clutch_rows:
        mn = r['map_name']
        if not mn:
            continue
        d = profile['by_map'].setdefault(mn, {})
        mc = max(r['maps_count'] or 1, 1)
        d['clutch_per_map'] = (r['clutch_score'] or 0) / mc
        d['mk_per_map']     = (r['mk_score']     or 0) / mc
        total_cs += r['clutch_score'] or 0
        total_ms += r['mk_score']     or 0
        total_mc += mc

    profile['global']['clutch_per_map'] = total_cs / max(total_mc, 1)
    profile['global']['mk_per_map']     = total_ms / max(total_mc, 1)

    # — Duelos
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

    # — Veto
    for r in veto_rows:
        mn = r['map_name']
        if not mn:
            continue
        d = profile['by_map'].setdefault(mn, {})
        d['picks'] = r['picks'] or 0
        d['bans']  = r['bans']  or 0

    return profile


def get_h2h(team_a, team_b):
    """H2H filtrado por año — enfrentamientos con el mismo roster."""
    rows = query(f"""
        SELECT winner, COUNT(*) AS cnt
        FROM matches
        WHERE ((team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?))
        GROUP BY winner
    """, [team_a, team_b, team_b, team_a])
    a_w   = sum(r['cnt'] for r in rows if r['winner'] == team_a)
    b_w   = sum(r['cnt'] for r in rows if r['winner'] == team_b)
    total = a_w + b_w
    return {'a_wins': a_w, 'b_wins': b_w, 'total': total, 'a_wr': safe_div(a_w, total)}


# ─── PROBABILIDAD POR RONDA ───────────────────────────────────────────────────
def get_side_stats(profile, map_name, side):
    """
    Estadísticas de equipo en mapa+lado.
    Suavizado Bayesiano con prior = meta del mapa × eficiencia relativa del equipo.
    """
    PRIOR_ROUNDS = 20
    meta_atk = MAP_META_ATK.get(map_name, 0.5)
    meta_def = 1 - meta_atk

    gl = profile['global']
    mp = profile['by_map'].get(map_name, {})

    if side == 'attack':
        obs_won = mp.get('atk_won', 0)
        obs_tot = mp.get('atk_total', 0)
        prior_wr = meta_atk * (gl['atk_wr'] / max(gl['atk_wr'] + gl['def_wr'], 0.01))
        rating   = mp.get('attack_rating',  gl.get('avg_rating', 1.0))
        acs      = mp.get('attack_acs',  200)
        adr      = mp.get('attack_adr',  130)
    else:
        obs_won  = mp.get('def_won', 0)
        obs_tot  = mp.get('def_total', 0)
        prior_wr = meta_def * (gl['def_wr'] / max(gl['atk_wr'] + gl['def_wr'], 0.01))
        rating   = mp.get('defense_rating', gl.get('avg_rating', 1.0))
        acs      = mp.get('defense_acs', 200)
        adr      = mp.get('defense_adr', 130)

    prior_won = prior_wr * PRIOR_ROUNDS
    wr = (obs_won + prior_won) / (obs_tot + PRIOR_ROUNDS)

    fk_fd    = mp.get('fk_fd',        gl.get('duel_wr',       0.5))
    duel_wr  = mp.get('duel_wr',      gl.get('duel_wr',       0.5))
    pistol   = mp.get('pistol_wr',    gl.get('pistol_wr',     0.5))
    full_buy = mp.get('full_buy_wr',  gl.get('full_buy_wr',   0.5))
    eco_wr   = mp.get('eco_wr',       0.25)
    clutch   = mp.get('clutch_per_map', gl.get('clutch_per_map', 3.0))
    mk       = mp.get('mk_per_map',     gl.get('mk_per_map',     2.0))
    veto_picks = mp.get('picks', 0)
    veto_bans  = mp.get('bans',  0)

    return {
        'wr':          wr,
        'rating':      max(rating or 0.5, 0.5),
        'acs':         acs or 200,
        'adr':         adr or 130,
        'fk_fd':       fk_fd,
        'duel_wr':     duel_wr,
        'pistol_wr':   pistol,
        'full_buy_wr': full_buy,
        'eco_wr':      eco_wr,
        'clutch_mk':   clutch + mk * 0.5,
        'veto_signal': 0.52 if veto_picks > 0 else (0.48 if veto_bans > 1 else 0.5),
        'sample':      obs_tot,
        'maps':        mp.get('maps_played', 0),
        'recency':     mp.get('recency', 0.3),
    }


def compute_round_prob(prof_a, prof_b, map_name, a_is_atk, h2h):
    """
    P(Team A gana esta ronda) — señales ponderadas:
      40% histórico ATK/DEF · 28% habilidad · 18% economía · 9% clutch · 5% H2H/veto
    """
    side_a = 'attack' if a_is_atk else 'defense'
    side_b = 'defense' if a_is_atk else 'attack'

    sa = get_side_stats(prof_a, map_name, side_a)
    sb = get_side_stats(prof_b, map_name, side_b)

    # Señal 1: WR histórico
    wr_signal = sa['wr'] / (sa['wr'] + sb['wr']) if (sa['wr'] + sb['wr']) > 0 else 0.5

    # Señal 2: Habilidad compuesta
    a_skill = (sa['rating']        * 0.50 +
               sa['acs'] / 280.0   * 0.20 +
               sa['adr'] / 170.0   * 0.15 +
               sa['fk_fd']         * 0.15)
    b_skill = (sb['rating']        * 0.50 +
               sb['acs'] / 280.0   * 0.20 +
               sb['adr'] / 170.0   * 0.15 +
               sb['fk_fd']         * 0.15)
    skill_signal = safe_div(a_skill, a_skill + b_skill)

    # Señal 3: Economía
    a_econ = sa['full_buy_wr'] * 0.55 + sa['pistol_wr'] * 0.30 + sa['eco_wr'] * 0.15
    b_econ = sb['full_buy_wr'] * 0.55 + sb['pistol_wr'] * 0.30 + sb['eco_wr'] * 0.15
    econ_signal = safe_div(a_econ, a_econ + b_econ)

    # Señal 4: Clutch/multikills
    a_cm = sa['clutch_mk']; b_cm = sb['clutch_mk']
    clutch_signal = safe_div(a_cm, a_cm + b_cm) if (a_cm + b_cm) > 0 else 0.5

    # Señal 5: H2H del año + veto
    if h2h['total'] >= 2:
        h2h_comp = h2h['a_wr'] * 0.7 + sa['veto_signal'] * 0.3
    else:
        h2h_comp = sa['veto_signal']
    meta_signal = 0.5 * h2h_comp + 0.5 * (1 - sb['veto_signal'])

    # Blend final
    p = (0.40 * wr_signal +
         0.28 * skill_signal +
         0.18 * econ_signal +
         0.09 * clutch_signal +
         0.05 * meta_signal)

    # Clamp conservador: nunca más de 70% / 30% por ronda
    return float(max(0.28, min(0.72, p)))


def map_confidence(a_maps, b_maps, a_recency, b_recency):
    """
    Confianza basada en tamaño de muestra Y recencia de los datos.
    Un equipo con 5 mapas del Kickoff tendrá confianza 'medium', no 'high'.
    """
    min_m      = min(a_maps, b_maps)
    avg_rec    = (a_recency + b_recency) / 2

    # Base: tamaño de muestra
    if min_m >= 4:   base = 'high'
    elif min_m >= 2: base = 'medium'
    else:            base = 'low'

    # Penalizar si los datos son muy viejos (recency < 0.35 → datos de hace 2+ half-lives)
    if avg_rec < 0.35:
        if base == 'high':   return 'medium'
        if base == 'medium': return 'low'
    elif avg_rec < 0.55 and base == 'high':
        return 'medium'  # datos algo viejos, no dar alta confianza

    return base


# ─── SIMULACIÓN MONTE CARLO (vectorizada) ────────────────────────────────────
def monte_carlo(p_a_atk, p_a_def, a_starts_atk, n=10000, return_details=False):
    """
    Simula N partidas en paralelo con NumPy.
    Reglas: primero a 13 · cambio de lados en r12 · OT cada 2 rondas, primero a +2.
    """
    rng      = np.random.default_rng()
    scores_a = np.zeros(n, dtype=np.int32)
    scores_b = np.zeros(n, dtype=np.int32)
    a_atk    = np.full(n, a_starts_atk, dtype=bool)
    rnds     = np.zeros(n, dtype=np.int32)
    active   = np.ones(n, dtype=bool)

    for _ in range(90):
        if not active.any():
            break
        idx = np.where(active)[0]
        p   = np.where(a_atk[idx], p_a_atk, p_a_def)
        won = rng.random(len(idx)) < p
        scores_a[idx] += won.astype(np.int32)
        scores_b[idx] += (~won).astype(np.int32)
        rnds[idx] += 1

        switch12  = active & (rnds == 12)
        a_atk[switch12] = ~a_atk[switch12]
        ot_switch = active & (rnds > 24) & ((rnds - 24) % 2 == 0)
        a_atk[ot_switch] = ~a_atk[ot_switch]

        normal_win = active & (rnds <= 24) & ((scores_a >= 13) | (scores_b >= 13))
        ot_win     = (active & (rnds > 24) &
                      ((scores_a >= 13) | (scores_b >= 13)) &
                      (np.abs(scores_a - scores_b) >= 2))
        active &= ~(normal_win | ot_win)

    win_a = float((scores_a > scores_b).mean())

    if not return_details:
        return win_a

    from collections import Counter
    total_rnds = scores_a + scores_b
    ot_pct     = float((total_rnds > 24).mean())
    avg_rounds = float(total_rnds.mean())
    raw        = Counter(zip(scores_a.tolist(), scores_b.tolist()))
    top5       = sorted(raw.items(), key=lambda x: -x[1])[:5]
    score_freq = {f"{k[0]}-{k[1]}": round(v / n, 3) for k, v in top5}
    modal      = top5[0][0] if top5 else (13, 0)

    return {
        'win_prob':    win_a,
        'avg_score_a': round(float(scores_a.mean()), 1),
        'avg_score_b': round(float(scores_b.mean()), 1),
        'avg_rounds':  round(avg_rounds, 1),
        'ot_pct':      round(ot_pct, 3),
        'score_freq':  score_freq,
        'modal_score': f"{modal[0]}-{modal[1]}",
    }


def monte_carlo_series(map_probs, n=10000):
    """
    Simula N series completas.
    map_probs: [P(Team A gana mapa 1), P(mapa 2), ...]
    """
    total  = len(map_probs)
    need   = (total + 1) // 2
    rng    = np.random.default_rng()
    wins_a = np.zeros(n, dtype=np.int32)
    wins_b = np.zeros(n, dtype=np.int32)

    for p_win in map_probs:
        still  = (wins_a < need) & (wins_b < need)
        if not still.any():
            break
        won_a  = still & (rng.random(n) < p_win)
        wins_a += won_a.astype(np.int32)
        wins_b += (still & ~won_a).astype(np.int32)

    series_win_a = float((wins_a >= need).mean())

    from collections import Counter
    raw = Counter(zip(wins_a.tolist(), wins_b.tolist()))
    score_dist = {
        f"{k[0]}-{k[1]}": round(v / n, 4)
        for k, v in sorted(raw.items(), reverse=True)
    }

    return series_win_a, score_dist


# ─── HELPERS ─────────────────────────────────────────────────────────────────
def best_maps(profile):
    """Top mapas del equipo por puntuación compuesta."""
    scores = []
    for mn in MAPS:
        mp = profile['by_map'].get(mn, {})
        if mp.get('maps_played', 0) == 0:
            scores.append((mn, None))
            continue
        at  = safe_div(mp.get('atk_won', 0),  mp.get('atk_total', 1))
        df  = safe_div(mp.get('def_won', 0),  mp.get('def_total', 1))
        wr  = safe_div(mp.get('maps_won', 0), mp.get('maps_played', 1))
        sc  = 0.40 * wr + 0.30 * at + 0.30 * df
        scores.append((mn, round(sc, 3)))
    scores.sort(key=lambda x: (x[1] is None, -(x[1] or 0)))
    return [{'map': m, 'score': s} for m, s in scores]


# ─── RUTAS ────────────────────────────────────────────────────────────────────
@predecir_bp.route('/api/equipos-pred', methods=['GET'])
def get_teams():
    result = []
    try:
        # Sin filtro de año — usamos todos los datos disponibles
        teams_from_matches = query("""
            SELECT DISTINCT team_a AS name FROM matches
            UNION
            SELECT DISTINCT team_b FROM matches
            ORDER BY name
        """)

        stats_by_abbrev = {r['team_name']: r for r in query("""
            SELECT ps.team_name,
                   COUNT(DISTINCT ps.map_id) AS maps_played,
                   ROUND(AVG(ps.rating), 2)  AS avg_rating
            FROM player_stats ps
            JOIN maps mp ON ps.map_id = mp.map_id
            JOIN matches mt ON mp.match_id = mt.match_id
            GROUP BY ps.team_name
        """)}

        for t in teams_from_matches:
            try:
                nm = t['name'] if t['name'] else ''
                if not nm:
                    continue
                ab = TEAM_ABBREV.get(nm, nm)
                st = stats_by_abbrev.get(ab, {})
                result.append({
                    'name':        nm,
                    'abbrev':      ab,
                    'maps_played': int(st.get('maps_played') or 0),
                    'avg_rating':  float(st.get('avg_rating') or 0),
                })
            except Exception:
                continue

        return jsonify({'ok': True, 'teams': result})

    except Exception as e:
        import traceback
        try:
            return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500
        except Exception:
            return jsonify({'ok': False, 'error': 'Error interno'}), 500


@predecir_bp.route('/api/predecir', methods=['POST'])
def predict():
    data   = request.get_json()
    team_a = data.get('team_a', '').strip()
    team_b = data.get('team_b', '').strip()
    n_sim  = max(1000, min(int(data.get('simulations', 10000)), 50000))

    if not team_a or not team_b:
        return jsonify({'ok': False, 'error': 'Se requieren dos equipos.'}), 400
    if team_a == team_b:
        return jsonify({'ok': False, 'error': 'Los equipos deben ser distintos.'}), 400

    try:
        prof_a = get_team_profile(team_a)
        prof_b = get_team_profile(team_b)
        h2h    = get_h2h(team_a, team_b)

        results = []
        for map_name in MAPS:
            for starting_side in ['attack', 'defense']:
                a_starts_atk = (starting_side == 'attack')

                p_a_atk = compute_round_prob(prof_a, prof_b, map_name, True,  h2h)
                p_a_def = compute_round_prob(prof_a, prof_b, map_name, False, h2h)
                det     = monte_carlo(p_a_atk, p_a_def, a_starts_atk, n_sim, return_details=True)

                a_maps    = prof_a['by_map'].get(map_name, {}).get('maps_played', 0)
                b_maps    = prof_b['by_map'].get(map_name, {}).get('maps_played', 0)
                a_recency = prof_a['by_map'].get(map_name, {}).get('recency', 0.3)
                b_recency = prof_b['by_map'].get(map_name, {}).get('recency', 0.3)
                conf      = map_confidence(a_maps, b_maps, a_recency, b_recency)

                results.append({
                    'map':          map_name,
                    'start':        starting_side,
                    'win_a':        round(det['win_prob'], 4),
                    'win_b':        round(1.0 - det['win_prob'], 4),
                    'p_round_atk':  round(p_a_atk, 4),
                    'p_round_def':  round(p_a_def, 4),
                    'confidence':   conf,
                    'a_maps':       a_maps,
                    'b_maps':       b_maps,
                    'a_recency':    round(a_recency, 2),
                    'b_recency':    round(b_recency, 2),
                    'avg_score_a':  det['avg_score_a'],
                    'avg_score_b':  det['avg_score_b'],
                    'avg_rounds':   det['avg_rounds'],
                    'ot_pct':       det['ot_pct'],
                    'score_freq':   det['score_freq'],
                    'modal_score':  det['modal_score'],
                })

        gl_a = prof_a['global']
        gl_b = prof_b['global']
        summary = {
            'team_a': {
                'name':        team_a,
                'abbrev':      prof_a['abbrev'],
                'maps_played': gl_a.get('maps_played', 0),
                'avg_rating':  round(gl_a.get('avg_rating', 1.0), 2),
                'atk_wr':      round(gl_a.get('atk_wr', 0.5) * 100, 1),
                'def_wr':      round(gl_a.get('def_wr', 0.5) * 100, 1),
                'map_wr':      round(gl_a.get('map_wr', 0.5) * 100, 1),
                'pistol_wr':   round(gl_a.get('pistol_wr', 0.5) * 100, 1),
                'full_buy_wr': round(gl_a.get('full_buy_wr', 0.5) * 100, 1),
                'clutch_pm':   round(gl_a.get('clutch_per_map', 0), 1),
                'duel_wr':     round(gl_a.get('duel_wr', 0.5) * 100, 1),
                'best_maps':   best_maps(prof_a),
            },
            'team_b': {
                'name':        team_b,
                'abbrev':      prof_b['abbrev'],
                'maps_played': gl_b.get('maps_played', 0),
                'avg_rating':  round(gl_b.get('avg_rating', 1.0), 2),
                'atk_wr':      round(gl_b.get('atk_wr', 0.5) * 100, 1),
                'def_wr':      round(gl_b.get('def_wr', 0.5) * 100, 1),
                'map_wr':      round(gl_b.get('map_wr', 0.5) * 100, 1),
                'pistol_wr':   round(gl_b.get('pistol_wr', 0.5) * 100, 1),
                'full_buy_wr': round(gl_b.get('full_buy_wr', 0.5) * 100, 1),
                'clutch_pm':   round(gl_b.get('clutch_per_map', 0), 1),
                'duel_wr':     round(gl_b.get('duel_wr', 0.5) * 100, 1),
                'best_maps':   best_maps(prof_b),
            },
            'h2h':        h2h,
        }

        return jsonify({
            'ok':          True,
            'results':     results,
            'summary':     summary,
            'simulations': n_sim,
        })

    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500


@predecir_bp.route('/api/predecir-partido', methods=['POST'])
def predict_match():
    """
    Simula un partido con mapas y lados definidos por el usuario.
    Body: { team_a, team_b, maps: [{map_name, a_starts_atk}, ...], simulations }
    """
    data        = request.get_json()
    team_a      = data.get('team_a', '').strip()
    team_b      = data.get('team_b', '').strip()
    maps_config = data.get('maps', [])
    n_sim       = max(1000, min(int(data.get('simulations', 10000)), 50000))

    if not team_a or not team_b:
        return jsonify({'ok': False, 'error': 'Se requieren dos equipos.'}), 400
    if team_a == team_b:
        return jsonify({'ok': False, 'error': 'Los equipos deben ser distintos.'}), 400
    if not maps_config:
        return jsonify({'ok': False, 'error': 'Selecciona al menos un mapa.'}), 400
    if len(maps_config) > 5:
        return jsonify({'ok': False, 'error': 'Máximo 5 mapas por partido.'}), 400

    bad = [c.get('map_name', '') for c in maps_config if c.get('map_name') not in MAPS]
    if bad:
        return jsonify({'ok': False, 'error': f'Mapas no válidos: {", ".join(bad)}'}), 400

    try:
        prof_a = get_team_profile(team_a)
        prof_b = get_team_profile(team_b)
        h2h    = get_h2h(team_a, team_b)

        map_results = []
        map_probs   = []

        for cfg in maps_config:
            mn           = cfg['map_name']
            a_starts_atk = bool(cfg.get('a_starts_atk', True))

            p_a_atk = compute_round_prob(prof_a, prof_b, mn, True,  h2h)
            p_a_def = compute_round_prob(prof_a, prof_b, mn, False, h2h)
            det     = monte_carlo(p_a_atk, p_a_def, a_starts_atk, n_sim, return_details=True)

            a_maps    = prof_a['by_map'].get(mn, {}).get('maps_played', 0)
            b_maps    = prof_b['by_map'].get(mn, {}).get('maps_played', 0)
            a_recency = prof_a['by_map'].get(mn, {}).get('recency', 0.3)
            b_recency = prof_b['by_map'].get(mn, {}).get('recency', 0.3)
            conf      = map_confidence(a_maps, b_maps, a_recency, b_recency)

            map_results.append({
                'map':          mn,
                'a_starts_atk': a_starts_atk,
                'win_a':        round(det['win_prob'], 4),
                'win_b':        round(1.0 - det['win_prob'], 4),
                'p_round_atk':  round(p_a_atk, 4),
                'p_round_def':  round(p_a_def, 4),
                'confidence':   conf,
                'a_maps':       a_maps,
                'b_maps':       b_maps,
                'a_recency':    round(a_recency, 2),
                'b_recency':    round(b_recency, 2),
                'avg_score_a':  det['avg_score_a'],
                'avg_score_b':  det['avg_score_b'],
                'avg_rounds':   det['avg_rounds'],
                'ot_pct':       det['ot_pct'],
                'score_freq':   det['score_freq'],
                'modal_score':  det['modal_score'],
            })
            map_probs.append(det['win_prob'])

        series_win_a, score_dist = monte_carlo_series(map_probs, n_sim)
        total_maps  = len(maps_config)
        maps_to_win = (total_maps + 1) // 2

        gl_a = prof_a['global']
        gl_b = prof_b['global']

        return jsonify({
            'ok':          True,
            'map_results': map_results,
            'series': {
                'win_a':       round(series_win_a, 4),
                'win_b':       round(1.0 - series_win_a, 4),
                'score_dist':  score_dist,
                'maps_to_win': maps_to_win,
                'total_maps':  total_maps,
                'format':      f'Bo{total_maps}',
            },
            'summary': {
                'team_a': {
                    'name':   team_a, 'abbrev': prof_a['abbrev'],
                    'atk_wr': round(gl_a.get('atk_wr', 0.5) * 100, 1),
                    'def_wr': round(gl_a.get('def_wr', 0.5) * 100, 1),
                },
                'team_b': {
                    'name':   team_b, 'abbrev': prof_b['abbrev'],
                    'atk_wr': round(gl_b.get('atk_wr', 0.5) * 100, 1),
                    'def_wr': round(gl_b.get('def_wr', 0.5) * 100, 1),
                },
                'h2h':         h2h,
                },
            'simulations': n_sim,
        })

    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500