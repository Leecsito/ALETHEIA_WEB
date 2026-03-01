"""
ALETHEIA — Visualizar Blueprint
Rutas: /api/matches, /api/player-stats, /api/maps-stats,
       /api/rounds-stats, /api/economy, /api/agents
"""

from flask import Blueprint, jsonify
try:
    from backend.conexion import get_conn, release_conn
except ImportError:
    from conexion import get_conn, release_conn

visualizar_bp = Blueprint('visualizar', __name__)

# ─── HELPER ──────────────────────────────────────────────────────────────────
def query(sql, params=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        return rows
    finally:
        release_conn(conn)

# ─── PARTIDOS ────────────────────────────────────────────────────────────────
@visualizar_bp.route('/api/matches', methods=['GET'])
def get_matches():
    try:
        data = query("""
            SELECT
                m.match_id,
                m.tournament,
                m.phase,
                m.match_date,
                m.team_a,
                m.team_b,
                m.score_a,
                m.score_b,
                m.winner,
                m.patch,
                COUNT(mp.map_id) AS maps_played
            FROM matches m
            LEFT JOIN maps mp ON mp.match_id = m.match_id
            GROUP BY m.match_id
            ORDER BY
                CASE WHEN m.match_date IS NULL THEN 1 ELSE 0 END,
                m.match_date DESC,
                m.match_id DESC
        """)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ─── JUGADORES ───────────────────────────────────────────────────────────────
@visualizar_bp.route('/api/player-stats', methods=['GET'])
def get_player_stats():
    try:
        data = query("""
            SELECT
                player_name,
                team_name,
                COUNT(DISTINCT match_id)                        AS matches,
                ROUND(AVG(rating), 2)                           AS avg_rating,
                ROUND(AVG(acs), 0)                              AS avg_acs,
                SUM(kills)                                      AS total_kills,
                SUM(deaths)                                     AS total_deaths,
                SUM(assists)                                    AS total_assists,
                ROUND(AVG(hs_percent), 1)                       AS avg_hs,
                ROUND(AVG(adr), 1)                              AS avg_adr,
                ROUND(AVG(kast), 1)                             AS avg_kast,
                SUM(fk)                                         AS total_fk,
                SUM(fd)                                         AS total_fd
            FROM player_stats
            GROUP BY player_name, team_name
            ORDER BY avg_rating DESC
        """)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ─── MAPAS ───────────────────────────────────────────────────────────────────
@visualizar_bp.route('/api/maps-stats', methods=['GET'])
def get_maps_stats():
    try:
        data = query("""
            SELECT
                map_name,
                COUNT(*)                                                    AS times_played,
                SUM(CASE WHEN picker = 'a' THEN 1 ELSE 0 END)              AS picked_by_a,
                SUM(CASE WHEN picker = 'b' THEN 1 ELSE 0 END)              AS picked_by_b,
                SUM(CASE WHEN picker = 'decider' THEN 1 ELSE 0 END)        AS as_decider,
                SUM(CASE WHEN side_chosen = 'attack'  THEN 1 ELSE 0 END)   AS attack_chosen,
                SUM(CASE WHEN side_chosen = 'defense' THEN 1 ELSE 0 END)   AS defense_chosen,
                ROUND(AVG(
                    score_a_attack + score_a_defense +
                    score_b_attack + score_b_defense
                ), 1)                                                       AS avg_rounds
            FROM maps
            WHERE map_name IS NOT NULL
            GROUP BY map_name
            ORDER BY times_played DESC
        """)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ─── RONDAS ──────────────────────────────────────────────────────────────────
@visualizar_bp.route('/api/rounds-stats', methods=['GET'])
def get_rounds_stats():
    try:
        data = query("""
            SELECT
                result_type,
                winning_side,
                COUNT(*) AS total
            FROM rounds
            WHERE result_type IS NOT NULL AND result_type != ''
            GROUP BY result_type, winning_side
            ORDER BY result_type, winning_side
        """)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ─── ECONOMIA ────────────────────────────────────────────────────────────────
@visualizar_bp.route('/api/economy', methods=['GET'])
def get_economy():
    try:
        data = query("""
            SELECT
                team,
                COUNT(DISTINCT map_id)                              AS maps,
                SUM(pistol_won)                                     AS pistol_won,
                SUM(eco_played)                                     AS eco_played,
                SUM(eco_won)                                        AS eco_won,
                SUM(semi_eco_played)                                AS semi_eco_p,
                SUM(semi_eco_won)                                   AS semi_eco_w,
                SUM(semi_buy_played)                                AS semi_buy_p,
                SUM(semi_buy_won)                                   AS semi_buy_w,
                SUM(full_buy_played)                                AS full_buy_p,
                SUM(full_buy_won)                                   AS full_buy_w,
                CASE WHEN SUM(eco_played) > 0
                     THEN ROUND(SUM(eco_won) * 100.0 / SUM(eco_played), 1)
                     ELSE NULL END                                  AS eco_wr,
                CASE WHEN SUM(semi_eco_played) > 0
                     THEN ROUND(SUM(semi_eco_won) * 100.0 / SUM(semi_eco_played), 1)
                     ELSE NULL END                                  AS semi_eco_wr,
                CASE WHEN SUM(semi_buy_played) > 0
                     THEN ROUND(SUM(semi_buy_won) * 100.0 / SUM(semi_buy_played), 1)
                     ELSE NULL END                                  AS semi_buy_wr,
                CASE WHEN SUM(full_buy_played) > 0
                     THEN ROUND(SUM(full_buy_won) * 100.0 / SUM(full_buy_played), 1)
                     ELSE NULL END                                  AS full_buy_wr
            FROM economy_summary
            GROUP BY team
            ORDER BY
                CASE WHEN SUM(full_buy_played) = 0 THEN 1 ELSE 0 END,
                ROUND(SUM(full_buy_won) * 100.0 / MAX(SUM(full_buy_played), 1), 1) DESC
        """)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ─── AGENTES ─────────────────────────────────────────────────────────────────
@visualizar_bp.route('/api/agents', methods=['GET'])
def get_agents():
    try:
        data = query("""
            SELECT
                agent,
                COUNT(*)                    AS picks,
                ROUND(AVG(rating), 2)       AS avg_rating,
                ROUND(AVG(acs), 0)          AS avg_acs,
                ROUND(AVG(hs_percent), 1)   AS avg_hs,
                ROUND(AVG(kast), 1)         AS avg_kast
            FROM player_stats
            WHERE agent IS NOT NULL AND agent != '' AND agent != 'Unknown'
            GROUP BY agent
            ORDER BY picks DESC
        """)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500