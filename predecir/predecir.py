"""
ALETHEIA — Predecir Blueprint
Simulación Monte Carlo para predecir resultados de partidos.

Usa TODAS las tablas disponibles:
  maps + matches      → tasas de victoria ATK/DEF por mapa
  player_stats        → rendimiento por jugador/lado/mapa (rating, ACS, ADR, KAST, FK/FD)
  economy_summary     → eficiencia económica (pistol, full-buy, eco WR)
  multikills_clutches → factor clutch y multikills
  duels               → dominancia en enfrentamientos directos
  match_veto          → tendencias de picks/bans (señal de fortaleza/debilidad)
  matches             → historial H2H entre equipos

Señales ponderadas para probabilidad de ronda:
  40% — Histórico ATK/DEF en ese mapa específico (suavizado Bayesiano)
  28% — Diferencial de habilidad (rating × ACS × ADR × FK/FD)
  18% — Eficiencia económica (pistol_wr × full_buy_wr)
  9%  — Factor clutch y multikills
  5%  — Historial H2H + tendencias veto
"""

from flask import Blueprint, jsonify, request
import numpy as np
from conexion import get_conn, release_conn

predecir_bp = Blueprint('predecir', __name__)

MAPS = ['Abyss', 'Bind', 'Fracture', 'Haven', 'Lotus', 'Pearl', 'Split']

# Sesgo meta global de cada mapa (% de rondas que gana el atacante)
# Calculado del dataset completo: rounds ATK_wins / total_rounds por mapa
MAP_META_ATK = {
    'Abyss':   0.587,   # ATK-heavy
    'Bind':    0.548,
    'Fracture': 0.509,
    'Haven':   0.474,   # DEF-heavy
    'Lotus':   0.504,
    'Pearl':   0.545,
    'Split':   0.495,
}

# Mapeo nombre completo (matches) → abreviatura (player_stats, economy_summary, etc.)
TEAM_ABBREV = {
    '100 Thieves':      '100T',
    '2Game Esports':    '2G',
    'Apeks':            'APK',
    'BBL Esports':      'BBL',
    'BOOM Esports':     'BME',
    'Cloud9':           'C9',
    'DRX':              'DRX',
    'DetonatioN FocusMe': 'DFM',
    'Evil Geniuses':    'EG',
    'FNATIC':           'FNC',
    'FURIA':            'FUR',
    'FUT Esports':      'FUT',
    'G2 Esports':       'G2',
    'GIANTX':           'GX',
    'Gen.G':            'GEN',
    'Gentle Mates':     'M8',
    'Global Esports':   'GE',
    'KRÜ Esports':      'KRÜ',
    'Karmine Corp':     'KC',
    'LEVIATÁN':         'LEV',
    'LOUD':             'LOUD',
    'MIBR':             'MIBR',
    'Movistar KOI(KOI)': 'MKOI',
    'NRG':              'NRG',
    'Natus Vincere':    'NAVI',
    'Nongshim RedForce': 'NS',
    'Paper Rex':        'PRX',
    'Rex Regum Qeon':   'RRQ',
    'Sentinels':        'SEN',
    'T1':               'T1',
    'TALON':            'TLN',
    'Team Heretics':    'TH',
    'Team Liquid':      'TL',
    'Team Secret':      'TS',
    'Team Vitality':    'VIT',
    'ZETA DIVISION':    'ZETA',
}

# ─── DB HELPER ───────────────────────────────────────────────────────────────

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

# ─── EXTRACCIÓN DE PERFIL ─────────────────────────────────────────────────────

def get_team_profile(full_name):
    """
    Construye el perfil estadístico completo de un equipo.
    Combina datos de TODAS las tablas.
    """
    abbrev = TEAM_ABBREV.get(full_name, full_name)

    # ── 1. Tasas ATK/DEF por ronda y resultado de mapas ──────────────────────
    round_rows = query("""
        SELECT map_name,
               SUM(atk_won)    AS atk_won,
               SUM(atk_total)  AS atk_total,
               SUM(def_won)    AS def_won,
               SUM(def_total)  AS def_total,
               SUM(maps_won)   AS maps_won,
               COUNT(*)        AS maps_played
        FROM (
            SELECT mp.map_name,
                   mp.score_a_attack                        AS atk_won,
                   (mp.score_a_attack + mp.score_b_defense) AS atk_total,
                   mp.score_a_defense                       AS def_won,
                   (mp.score_a_defense + mp.score_b_attack) AS def_total,
                   CASE WHEN mt.winner = mt.team_a THEN 1 ELSE 0 END AS maps_won
            FROM maps mp JOIN matches mt ON mp.match_id = mt.match_id
            WHERE mt.team_a = ?
            UNION ALL
            SELECT mp.map_name,
                   mp.score_b_attack,
                   (mp.score_b_attack + mp.score_a_defense),
                   mp.score_b_defense,
                   (mp.score_b_defense + mp.score_a_attack),
                   CASE WHEN mt.winner = mt.team_b THEN 1 ELSE 0 END
            FROM maps mp JOIN matches mt ON mp.match_id = mt.match_id
            WHERE mt.team_b = ?
        )
        WHERE map_name IS NOT NULL
        GROUP BY map_name
    """, [full_name, full_name])

    # ── 2. Rendimiento de jugadores por lado y mapa ───────────────────────────
    player_rows = query("""
        SELECT mp.map_name, ps.side,
               AVG(ps.rating)     AS avg_rating,
               AVG(ps.acs)        AS avg_acs,
               AVG(ps.adr)        AS avg_adr,
               AVG(ps.kast)       AS avg_kast,
               SUM(ps.fk)         AS total_fk,
               SUM(ps.fd)         AS total_fd
        FROM player_stats ps
        JOIN maps mp ON ps.map_id = mp.map_id
        WHERE ps.team_name = ?
        GROUP BY mp.map_name, ps.side
    """, [abbrev])

    # ── 3. Eficiencia económica por mapa ─────────────────────────────────────
    econ_rows = query("""
        SELECT mp.map_name,
               SUM(es.pistol_won)          AS pistol_won,
               COUNT(*) * 2                AS pistol_total,
               SUM(es.full_buy_played)     AS fb_played,
               SUM(es.full_buy_won)        AS fb_won,
               SUM(es.eco_played)          AS eco_played,
               SUM(es.eco_won)             AS eco_won,
               SUM(es.semi_buy_played)     AS sb_played,
               SUM(es.semi_buy_won)        AS sb_won
        FROM economy_summary es
        JOIN maps mp ON es.map_id = mp.map_id
        WHERE es.team = ?
        GROUP BY mp.map_name
    """, [abbrev])

    # ── 4. Factor clutch y multikills ─────────────────────────────────────────
    clutch_rows = query("""
        SELECT mp.map_name,
               SUM(mc.v1 + mc.v2*2 + mc.v3*3 + mc.v4*4 + mc.v5*5) AS clutch_score,
               SUM(mc.k2 + mc.k3*2 + mc.k4*3 + mc.k5*4)            AS mk_score,
               COUNT(DISTINCT mc.map_id)                             AS maps_count
        FROM multikills_clutches mc
        JOIN maps mp ON mc.map_id = mp.map_id
        WHERE mc.player_name IN (
            SELECT DISTINCT player_name FROM player_stats WHERE team_name = ?
        )
        GROUP BY mp.map_name
    """, [abbrev])

    # ── 5. Dominancia en duelos ───────────────────────────────────────────────
    duel_rows = query("""
        SELECT mp.map_name,
               SUM(CASE WHEN d.player_a IN (SELECT player_name FROM player_stats WHERE team_name = ?)
                        THEN d.kills_a ELSE d.kills_b END) AS kills_for,
               SUM(CASE WHEN d.player_a IN (SELECT player_name FROM player_stats WHERE team_name = ?)
                        THEN d.kills_b ELSE d.kills_a END) AS kills_against
        FROM duels d
        JOIN maps mp ON d.map_id = mp.map_id
        WHERE d.player_a IN (SELECT player_name FROM player_stats WHERE team_name = ?)
           OR d.player_b IN (SELECT player_name FROM player_stats WHERE team_name = ?)
        GROUP BY mp.map_name
    """, [abbrev, abbrev, abbrev, abbrev])

    # ── 6. Tendencias de veto ─────────────────────────────────────────────────
    veto_rows = query("""
        SELECT mv.map_name,
               SUM(CASE WHEN mv.action = 'pick' THEN 1 ELSE 0 END)  AS picks,
               SUM(CASE WHEN mv.action = 'ban'  THEN 1 ELSE 0 END)  AS bans
        FROM match_veto mv
        JOIN matches mt ON mv.match_id = mt.match_id
        WHERE (mv.team = 'a' AND mt.team_a = ?)
           OR (mv.team = 'b' AND mt.team_b = ?)
        GROUP BY mv.map_name
    """, [full_name, full_name])

    # ─── Ensamblar perfil ─────────────────────────────────────────────────────
    profile = {'team': full_name, 'abbrev': abbrev, 'by_map': {}, 'global': {}}

    # Rondas
    total_atk_won = total_atk_tot = total_def_won = total_def_tot = 0
    total_maps_won = total_maps = 0
    for r in round_rows:
        mn = r['map_name']
        if not mn: continue
        d = profile['by_map'].setdefault(mn, {})
        d.update({
            'atk_won': r['atk_won'] or 0, 'atk_total': r['atk_total'] or 0,
            'def_won': r['def_won'] or 0, 'def_total': r['def_total'] or 0,
            'maps_won': r['maps_won'] or 0, 'maps_played': r['maps_played'] or 0,
        })
        total_atk_won += r['atk_won'] or 0; total_atk_tot += r['atk_total'] or 0
        total_def_won += r['def_won'] or 0; total_def_tot += r['def_total'] or 0
        total_maps_won += r['maps_won'] or 0; total_maps += r['maps_played'] or 0

    profile['global'].update({
        'atk_wr':    safe_div(total_atk_won, total_atk_tot),
        'def_wr':    safe_div(total_def_won, total_def_tot),
        'map_wr':    safe_div(total_maps_won, total_maps),
        'maps_played': total_maps,
    })

    # Player stats
    all_ratings = []
    for r in player_rows:
        mn = r['map_name']; side = r['side']
        if not mn: continue
        d = profile['by_map'].setdefault(mn, {})
        d[f'{side}_rating'] = r['avg_rating'] or 1.0
        d[f'{side}_acs']    = r['avg_acs'] or 200
        d[f'{side}_adr']    = r['avg_adr'] or 130
        d[f'{side}_kast']   = r['avg_kast'] or 70
        fk = r['total_fk'] or 0; fd = r['total_fd'] or 0
        d['fk_fd'] = safe_div(fk, fk + fd)
        all_ratings.append(r['avg_rating'] or 1.0)

    profile['global']['avg_rating'] = sum(all_ratings) / len(all_ratings) if all_ratings else 1.0

    # Economía
    total_pw = total_pt = total_fb_w = total_fb_p = 0
    for r in econ_rows:
        mn = r['map_name']
        if not mn: continue
        d = profile['by_map'].setdefault(mn, {})
        d['pistol_wr']   = safe_div(r['pistol_won'] or 0, r['pistol_total'] or 1)
        d['full_buy_wr'] = safe_div(r['fb_won'] or 0, r['fb_played'] or 1)
        d['eco_wr']      = safe_div(r['eco_won'] or 0, r['eco_played'] or 1)
        d['semi_buy_wr'] = safe_div(r['sb_won'] or 0, r['sb_played'] or 1)
        total_pw += r['pistol_won'] or 0; total_pt += r['pistol_total'] or 0
        total_fb_w += r['fb_won'] or 0; total_fb_p += r['fb_played'] or 0

    profile['global']['pistol_wr']   = safe_div(total_pw, total_pt)
    profile['global']['full_buy_wr'] = safe_div(total_fb_w, total_fb_p)

    # Clutch
    total_cs = total_ms = total_mc = 0
    for r in clutch_rows:
        mn = r['map_name']
        if not mn: continue
        d = profile['by_map'].setdefault(mn, {})
        mc = max(r['maps_count'] or 1, 1)
        d['clutch_per_map'] = (r['clutch_score'] or 0) / mc
        d['mk_per_map']     = (r['mk_score'] or 0) / mc
        total_cs += r['clutch_score'] or 0; total_ms += r['mk_score'] or 0
        total_mc += r['maps_count'] or 1

    profile['global']['clutch_per_map'] = total_cs / max(total_mc, 1)
    profile['global']['mk_per_map']     = total_ms / max(total_mc, 1)

    # Duelos
    total_df = total_da = 0
    for r in duel_rows:
        mn = r['map_name']
        if not mn: continue
        d = profile['by_map'].setdefault(mn, {})
        kf = r['kills_for'] or 0; ka = r['kills_against'] or 0
        d['duel_wr'] = safe_div(kf, kf + ka)
        total_df += kf; total_da += ka

    profile['global']['duel_wr'] = safe_div(total_df, total_df + total_da)

    # Veto
    for r in veto_rows:
        mn = r['map_name']
        if not mn: continue
        d = profile['by_map'].setdefault(mn, {})
        d['picks'] = r['picks'] or 0
        d['bans']  = r['bans'] or 0

    return profile


def get_h2h(team_a, team_b):
    rows = query("""
        SELECT winner, COUNT(*) AS cnt
        FROM matches
        WHERE (team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?)
        GROUP BY winner
    """, [team_a, team_b, team_b, team_a])
    a_w = sum(r['cnt'] for r in rows if r['winner'] == team_a)
    b_w = sum(r['cnt'] for r in rows if r['winner'] == team_b)
    total = a_w + b_w
    return {'a_wins': a_w, 'b_wins': b_w, 'total': total, 'a_wr': safe_div(a_w, total)}


# ─── PROBABILIDAD POR RONDA ──────────────────────────────────────────────────

def get_side_stats(profile, map_name, side):
    """
    Devuelve estadísticas específicas de equipo+mapa+lado.
    Aplica suavizado Bayesiano con prior = meta global del mapa.
    """
    PRIOR_ROUNDS = 20   # equivalente a 20 rondas de prior
    meta_atk = MAP_META_ATK.get(map_name, 0.5)
    meta_def = 1 - meta_atk

    gl = profile['global']
    mp = profile['by_map'].get(map_name, {})

    if side == 'attack':
        obs_won = mp.get('atk_won', 0)
        obs_tot = mp.get('atk_total', 0)
        prior_wr = meta_atk * gl['atk_wr'] / 0.5 if gl['atk_wr'] != 0.5 else meta_atk
        # prior ponderado: meta del mapa × eficiencia relativa del equipo en ATK
        prior_wr = meta_atk * (gl['atk_wr'] / max(gl['atk_wr'] + gl['def_wr'], 0.01))
        rating = mp.get('attack_rating', gl.get('avg_rating', 1.0))
        acs    = mp.get('attack_acs', 200)
        adr    = mp.get('attack_adr', 130)
    else:
        obs_won = mp.get('def_won', 0)
        obs_tot = mp.get('def_total', 0)
        prior_wr = meta_def * (gl['def_wr'] / max(gl['atk_wr'] + gl['def_wr'], 0.01))
        rating = mp.get('defense_rating', gl.get('avg_rating', 1.0))
        acs    = mp.get('defense_acs', 200)
        adr    = mp.get('defense_adr', 130)

    # Bayesian blend: observaciones + prior
    prior_won = prior_wr * PRIOR_ROUNDS
    wr = (obs_won + prior_won) / (obs_tot + PRIOR_ROUNDS)

    # Factor FK/FD — quien controla los primeros contactos domina la ronda
    fk_fd    = mp.get('fk_fd', gl.get('duel_wr', 0.5))
    duel_wr  = mp.get('duel_wr', gl.get('duel_wr', 0.5))
    pistol   = mp.get('pistol_wr', gl.get('pistol_wr', 0.5))
    full_buy = mp.get('full_buy_wr', gl.get('full_buy_wr', 0.5))
    eco_wr   = mp.get('eco_wr', 0.25)
    clutch   = mp.get('clutch_per_map', gl.get('clutch_per_map', 3.0))
    mk       = mp.get('mk_per_map', gl.get('mk_per_map', 2.0))
    veto_picks = mp.get('picks', 0)
    veto_bans  = mp.get('bans', 0)

    return {
        'wr':        wr,
        'rating':    max(rating or 0.5, 0.5),
        'acs':       acs or 200,
        'adr':       adr or 130,
        'fk_fd':     fk_fd,
        'duel_wr':   duel_wr,
        'pistol_wr': pistol,
        'full_buy_wr': full_buy,
        'eco_wr':    eco_wr,
        'clutch_mk': clutch + mk * 0.5,
        'veto_signal': 0.52 if veto_picks > 0 else (0.48 if veto_bans > 1 else 0.5),
        'sample':    obs_tot,
        'maps':      mp.get('maps_played', 0),
    }


def compute_round_prob(prof_a, prof_b, map_name, a_is_atk, h2h):
    """
    Calcula P(Team A gana esta ronda).

    Señales ponderadas:
      40% — tasa histórica de rondas ATK/DEF en este mapa
      28% — diferencial de habilidad (rating, ACS, ADR, FK/FD, duelos)
      18% — eficiencia económica
       9% — factor clutch y multikills
       5% — H2H + tendencias veto
    """
    side_a = 'attack' if a_is_atk else 'defense'
    side_b = 'defense' if a_is_atk else 'attack'

    sa = get_side_stats(prof_a, map_name, side_a)
    sb = get_side_stats(prof_b, map_name, side_b)

    # ── Señal 1: Histórico WR ─────────────────────────────────────────────────
    wr_signal = sa['wr'] / (sa['wr'] + sb['wr']) if (sa['wr'] + sb['wr']) > 0 else 0.5

    # ── Señal 2: Habilidad compuesta ──────────────────────────────────────────
    # rating(50%) + ACS/normalizado(20%) + ADR/normalizado(15%) + FK/FD(15%)
    a_skill = (sa['rating']          * 0.50 +
               sa['acs']   / 280.0   * 0.20 +
               sa['adr']   / 170.0   * 0.15 +
               sa['fk_fd']           * 0.15)
    b_skill = (sb['rating']          * 0.50 +
               sb['acs']   / 280.0   * 0.20 +
               sb['adr']   / 170.0   * 0.15 +
               sb['fk_fd']           * 0.15)
    skill_signal = safe_div(a_skill, a_skill + b_skill)

    # ── Señal 3: Economía ─────────────────────────────────────────────────────
    # Los pistol rounds afectan rachas de 3-4 rondas → peso mayor
    a_econ = sa['full_buy_wr'] * 0.55 + sa['pistol_wr'] * 0.30 + sa['eco_wr'] * 0.15
    b_econ = sb['full_buy_wr'] * 0.55 + sb['pistol_wr'] * 0.30 + sb['eco_wr'] * 0.15
    econ_signal = safe_div(a_econ, a_econ + b_econ)

    # ── Señal 4: Clutch/multikill ─────────────────────────────────────────────
    a_cm = sa['clutch_mk']; b_cm = sb['clutch_mk']
    clutch_signal = safe_div(a_cm, a_cm + b_cm) if (a_cm + b_cm) > 0 else 0.5

    # ── Señal 5: H2H + veto ───────────────────────────────────────────────────
    h2h_w = 0.7  # peso del H2H directo
    veto_w = 0.3
    if h2h['total'] >= 2:
        h2h_comp = h2h['a_wr'] * h2h_w + sa['veto_signal'] * veto_w
    else:
        h2h_comp = sa['veto_signal']
    meta_signal = 0.5 * h2h_comp + 0.5 * (1 - sb['veto_signal'])

    # ── Blend final ───────────────────────────────────────────────────────────
    p = (0.40 * wr_signal +
         0.28 * skill_signal +
         0.18 * econ_signal +
         0.09 * clutch_signal +
         0.05 * meta_signal)

    # Clamp conservador: nunca más de 70% / 30% por ronda
    return float(max(0.28, min(0.72, p)))


# ─── SIMULACIÓN MONTE CARLO (vectorizada) ────────────────────────────────────

def monte_carlo(p_a_atk, p_a_def, a_starts_atk, n=10000, return_details=False):
    """
    Simula N partidas en paralelo usando NumPy.
    Retorna P(Team A gana el mapa) o dict detallado si return_details=True.

    Reglas:
      - Primero en llegar a 13 rondas gana
      - Cambio de lados tras 12 rondas
      - Tiempo extra (OT): alternancia de lados cada 2 rondas, primero a 15
    """
    rng = np.random.default_rng()
    scores_a  = np.zeros(n, dtype=np.int32)
    scores_b  = np.zeros(n, dtype=np.int32)
    a_atk     = np.full(n, a_starts_atk, dtype=bool)
    rnds      = np.zeros(n, dtype=np.int32)
    active    = np.ones(n, dtype=bool)

    for _ in range(90):    # límite de seguridad: máx 90 rondas
        if not active.any():
            break
        idx = np.where(active)[0]
        p   = np.where(a_atk[idx], p_a_atk, p_a_def)
        won = rng.random(len(idx)) < p
        scores_a[idx] += won.astype(np.int32)
        scores_b[idx] += (~won).astype(np.int32)
        rnds[idx] += 1

        # Cambio de lados en ronda 12 y luego cada 2 rondas en OT
        switch12  = active & (rnds == 12)
        a_atk[switch12] = ~a_atk[switch12]
        ot_switch = active & (rnds > 24) & ((rnds - 24) % 2 == 0)
        a_atk[ot_switch] = ~a_atk[ot_switch]

        # Condición de victoria: primero a 13 (o 15, 16... en OT)
        # En OT: ganar cuando tienes 2 más que el rival y ambos ≥ 13
        normal_win = active & (rnds <= 24) & ((scores_a >= 13) | (scores_b >= 13))
        ot_win     = active & (rnds > 24)  & ((scores_a >= 13) | (scores_b >= 13)) & \
                     (np.abs(scores_a - scores_b) >= 2)
        done = normal_win | ot_win
        active &= ~done

    win_a = float((scores_a > scores_b).mean())

    if not return_details:
        return win_a

    # ─── Estadísticas detalladas ───────────────────────────────────────────────
    from collections import Counter
    total_rnds = scores_a + scores_b
    ot_mask    = total_rnds > 24
    ot_pct     = float(ot_mask.mean())
    avg_rounds = float(total_rnds.mean())
    avg_sa     = float(scores_a.mean())
    avg_sb     = float(scores_b.mean())

    # Distribución de scores: top 5 resultados más frecuentes
    raw  = Counter(zip(scores_a.tolist(), scores_b.tolist()))
    top5 = sorted(raw.items(), key=lambda x: -x[1])[:5]
    score_freq = {f"{k[0]}-{k[1]}": round(v / n, 3) for k, v in top5}

    # Score más probable (moda)
    modal_score = top5[0][0] if top5 else (13, 0)

    return {
        'win_prob':    win_a,
        'avg_score_a': round(avg_sa, 1),
        'avg_score_b': round(avg_sb, 1),
        'avg_rounds':  round(avg_rounds, 1),
        'ot_pct':      round(ot_pct, 3),
        'score_freq':  score_freq,
        'modal_score': f"{modal_score[0]}-{modal_score[1]}",
    }


# ─── SIMULACIÓN DE SERIE (Bo1 / Bo3 / Bo5) ───────────────────────────────────

def monte_carlo_series(map_probs, n=10000):
    """
    Simula N series completas.
    map_probs: lista de P(Team A gana ese mapa), en el orden del partido.
    Retorna (P(Team A gana serie), distribución de scores).

    Lógica: quien llega primero a ceil(total/2) victorias gana.
    Si el oponente ya ganó la serie, los mapas restantes no se juegan.
    """
    total = len(map_probs)
    need  = (total + 1) // 2   # 1→1, 2→2, 3→2, 4→3, 5→3
    rng   = np.random.default_rng()

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

    # Distribución de resultados (ej: "2-0", "2-1", "1-2", "0-2")
    from collections import Counter
    raw = Counter(zip(wins_a.tolist(), wins_b.tolist()))
    score_dist = {f"{k[0]}-{k[1]}": round(v / n, 4) for k, v in sorted(raw.items(), reverse=True)}

    return series_win_a, score_dist


# ─── HELPERS PARA RESUMEN ────────────────────────────────────────────────────

def best_maps(profile):
    """Top 3 mapas del equipo basado en win rate combinado."""
    scores = []
    for mn in MAPS:
        mp = profile['by_map'].get(mn, {})
        if mp.get('maps_played', 0) == 0:
            scores.append((mn, None))
            continue
        atk = safe_div(mp.get('atk_won', 0), mp.get('atk_total', 1))
        dff = safe_div(mp.get('def_won', 0), mp.get('def_total', 1))
        wr  = safe_div(mp.get('maps_won', 0), mp.get('maps_played', 1))
        # Puntuación compuesta: 40% map WR + 30% ATK + 30% DEF
        score = 0.40 * wr + 0.30 * atk + 0.30 * dff
        scores.append((mn, round(score, 3)))
    scores.sort(key=lambda x: (x[1] is None, -(x[1] or 0)))
    return [{'map': m, 'score': s} for m, s in scores]


# ─── RUTAS ───────────────────────────────────────────────────────────────────

@predecir_bp.route('/api/equipos-pred', methods=['GET'])
def get_teams():
    try:
        rows = query("""
            SELECT 
                CASE WHEN mt.team_a = sub.team THEN mt.team_a
                     ELSE mt.team_b END              AS full_name,
                sub.team                             AS abbrev,
                sub.maps_played,
                sub.avg_rating
            FROM (
                SELECT team_name AS team,
                       COUNT(DISTINCT map_id)      AS maps_played,
                       ROUND(AVG(rating), 2)        AS avg_rating
                FROM player_stats
                GROUP BY team_name
            ) sub
            JOIN matches mt ON (mt.team_a = sub.team OR mt.team_b = sub.team
                               OR EXISTS (
                   SELECT 1 FROM player_stats ps2
                   JOIN maps mp2 ON ps2.map_id = mp2.map_id
                   JOIN matches mt2 ON mp2.match_id = mt2.match_id
                   WHERE ps2.team_name = sub.team
                   AND (mt2.team_a = mt.team_a OR mt2.team_b = mt.team_b)
                   LIMIT 1
               ))
            LIMIT 200
        """)
        # Query simplificada: usa el mapeo directo
        teams_from_matches = query("""
            SELECT DISTINCT team_a AS name FROM matches
            UNION
            SELECT DISTINCT team_b FROM matches
            ORDER BY name
        """)
        
        result = []
        stats_by_abbrev = {r['team_name']: r for r in query("""
            SELECT team_name,
                   COUNT(DISTINCT map_id) AS maps_played,
                   ROUND(AVG(rating), 2)  AS avg_rating
            FROM player_stats GROUP BY team_name
        """)}
        
        for t in teams_from_matches:
            nm = t['name']
            ab = TEAM_ABBREV.get(nm, nm)
            st = stats_by_abbrev.get(ab, {})
            result.append({
                'name':        nm,
                'abbrev':      ab,
                'maps_played': st.get('maps_played', 0),
                'avg_rating':  st.get('avg_rating', 0),
            })
        
        return jsonify({'ok': True, 'teams': result})
    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500


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

                # Probabilidades por ronda
                p_a_atk = compute_round_prob(prof_a, prof_b, map_name, True,  h2h)
                p_a_def = compute_round_prob(prof_a, prof_b, map_name, False, h2h)

                # Simulación detallada
                det      = monte_carlo(p_a_atk, p_a_def, a_starts_atk, n_sim, return_details=True)
                win_prob = det['win_prob']

                # Confianza basada en muestra
                a_maps = prof_a['by_map'].get(map_name, {}).get('maps_played', 0)
                b_maps = prof_b['by_map'].get(map_name, {}).get('maps_played', 0)
                min_m  = min(a_maps, b_maps)
                conf   = 'high' if min_m >= 4 else ('medium' if min_m >= 2 else 'low')

                results.append({
                    'map':          map_name,
                    'start':        starting_side,
                    'win_a':        round(win_prob, 4),
                    'win_b':        round(1.0 - win_prob, 4),
                    'p_round_atk':  round(p_a_atk, 4),
                    'p_round_def':  round(p_a_def, 4),
                    'confidence':   conf,
                    'a_maps':       a_maps,
                    'b_maps':       b_maps,
                    # ─── Detalle de simulación ────────────────────────────────
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
                'name':         team_a,
                'abbrev':       prof_a['abbrev'],
                'maps_played':  gl_a.get('maps_played', 0),
                'avg_rating':   round(gl_a.get('avg_rating', 1.0), 2),
                'atk_wr':       round(gl_a.get('atk_wr', 0.5) * 100, 1),
                'def_wr':       round(gl_a.get('def_wr', 0.5) * 100, 1),
                'map_wr':       round(gl_a.get('map_wr', 0.5) * 100, 1),
                'pistol_wr':    round(gl_a.get('pistol_wr', 0.5) * 100, 1),
                'full_buy_wr':  round(gl_a.get('full_buy_wr', 0.5) * 100, 1),
                'clutch_pm':    round(gl_a.get('clutch_per_map', 0), 1),
                'duel_wr':      round(gl_a.get('duel_wr', 0.5) * 100, 1),
                'best_maps':    best_maps(prof_a),
            },
            'team_b': {
                'name':         team_b,
                'abbrev':       prof_b['abbrev'],
                'maps_played':  gl_b.get('maps_played', 0),
                'avg_rating':   round(gl_b.get('avg_rating', 1.0), 2),
                'atk_wr':       round(gl_b.get('atk_wr', 0.5) * 100, 1),
                'def_wr':       round(gl_b.get('def_wr', 0.5) * 100, 1),
                'map_wr':       round(gl_b.get('map_wr', 0.5) * 100, 1),
                'pistol_wr':    round(gl_b.get('pistol_wr', 0.5) * 100, 1),
                'full_buy_wr':  round(gl_b.get('full_buy_wr', 0.5) * 100, 1),
                'clutch_pm':    round(gl_b.get('clutch_per_map', 0), 1),
                'duel_wr':      round(gl_b.get('duel_wr', 0.5) * 100, 1),
                'best_maps':    best_maps(prof_b),
            },
            'h2h': h2h,
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


# ─── RUTA: SIMULAR PARTIDO ESPECÍFICO ────────────────────────────────────────

@predecir_bp.route('/api/predecir-partido', methods=['POST'])
def predict_match():
    """
    Simula un partido con mapas y lados definidos por el usuario.
    Body: {
        team_a: str,
        team_b: str,
        maps: [{map_name: str, a_starts_atk: bool}, ...],  (1-5 mapas)
        simulations: int
    }
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

    bad = [c.get('map_name','') for c in maps_config if c.get('map_name') not in MAPS]
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

            p_a_atk  = compute_round_prob(prof_a, prof_b, mn, True,  h2h)
            p_a_def  = compute_round_prob(prof_a, prof_b, mn, False, h2h)
            det      = monte_carlo(p_a_atk, p_a_def, a_starts_atk, n_sim, return_details=True)
            win_prob = det['win_prob']

            a_maps = prof_a['by_map'].get(mn, {}).get('maps_played', 0)
            b_maps = prof_b['by_map'].get(mn, {}).get('maps_played', 0)
            min_m  = min(a_maps, b_maps)
            conf   = 'high' if min_m >= 4 else ('medium' if min_m >= 2 else 'low')

            map_results.append({
                'map':          mn,
                'a_starts_atk': a_starts_atk,
                'win_a':        round(win_prob, 4),
                'win_b':        round(1.0 - win_prob, 4),
                'p_round_atk':  round(p_a_atk, 4),
                'p_round_def':  round(p_a_def, 4),
                'confidence':   conf,
                'a_maps':       a_maps,
                'b_maps':       b_maps,
                # ─── Detalle de simulación ────────────────────────────────────
                'avg_score_a':  det['avg_score_a'],
                'avg_score_b':  det['avg_score_b'],
                'avg_rounds':   det['avg_rounds'],
                'ot_pct':       det['ot_pct'],
                'score_freq':   det['score_freq'],
                'modal_score':  det['modal_score'],
            })
            map_probs.append(win_prob)

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
                    'name':   team_a,   'abbrev': prof_a['abbrev'],
                    'atk_wr': round(gl_a.get('atk_wr', 0.5) * 100, 1),
                    'def_wr': round(gl_a.get('def_wr', 0.5) * 100, 1),
                },
                'team_b': {
                    'name':   team_b,   'abbrev': prof_b['abbrev'],
                    'atk_wr': round(gl_b.get('atk_wr', 0.5) * 100, 1),
                    'def_wr': round(gl_b.get('def_wr', 0.5) * 100, 1),
                },
                'h2h': h2h,
            },
            'simulations': n_sim,
        })

    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}), 500