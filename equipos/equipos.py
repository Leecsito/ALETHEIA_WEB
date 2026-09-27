"""
ALETHEIA — Equipos Blueprint
Rutas: /api/equipos, /api/equipo/<team_id>
Lista de equipos con récord (V-D, winrate) y detalle con roster, historial
de transacciones, partidos, promedios de jugadores y rendimiento por mapa.
"""

from flask import Blueprint, request, jsonify
try:
    from backend.conexion import fetch_all
    from backend.cache import ttl_cache
except ImportError:
    from conexion import fetch_all
    from cache import ttl_cache

equipos_bp = Blueprint('equipos', __name__)


@ttl_cache(300)
def query(sql, params=None):
    return fetch_all(sql, params)


@equipos_bp.route('/api/equipos', methods=['GET'])
def list_equipos():
    try:
        q      = request.args.get('q', '').strip()
        region = request.args.get('region', '').strip()

        conds, params = [], []
        if q:
            like = f"%{q}%"
            conds.append("(t.team_name LIKE ? OR t.tag LIKE ?)")
            params += [like, like]
        if region:
            conds.append("t.region = ?")
            params.append(region)
        where = ("WHERE " + " AND ".join(conds)) if conds else ""

        data = query(f"""
            SELECT
                t.team_id, t.team_name, t.tag, t.region, t.country, t.url,
                COUNT(m.match_id)                                      AS matches,
                SUM(CASE WHEN m.winner_id = t.team_id THEN 1 ELSE 0 END) AS wins,
                MIN(m.match_date)                                      AS first_date,
                MAX(m.match_date)                                      AS last_date
            FROM teams t
            JOIN matches m ON m.team_a_id = t.team_id OR m.team_b_id = t.team_id
            {where}
            GROUP BY t.team_id
            ORDER BY matches DESC, t.team_name
        """, params)

        regiones = query("""
            SELECT region, COUNT(*) AS n FROM teams
            WHERE region IS NOT NULL AND region != ''
            GROUP BY region ORDER BY region
        """)

        return jsonify({"ok": True, "data": data, "regiones": regiones})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@equipos_bp.route('/api/equipo/<int:team_id>', methods=['GET'])
def detalle_equipo(team_id):
    try:
        info = query("SELECT * FROM teams WHERE team_id = ?", [team_id])
        if not info:
            return jsonify({"ok": False, "error": "Equipo no encontrado."}), 404
        info = info[0]

        record = query("""
            SELECT
                COUNT(*) AS matches,
                SUM(CASE WHEN winner_id = ? THEN 1 ELSE 0 END) AS wins,
                MIN(match_date) AS first_date,
                MAX(match_date) AS last_date
            FROM matches
            WHERE team_a_id = ? OR team_b_id = ?
        """, [team_id, team_id, team_id])[0]

        # Roster actual = jugadores cuyo ÚLTIMO movimiento (global) es JOIN y fue
        # a este equipo. Si el equipo no tiene transacciones cargadas, se cae al
        # criterio viejo (players.team_id) para no mostrar rosters vacíos.
        roster = query("""
            SELECT p.player_id, p.nickname, p.real_name, p.country
            FROM players p
            JOIN roster_transactions rt ON rt.player_id = p.player_id
            WHERE rt.team_id = ?
              AND p.nickname IS NOT NULL AND p.nickname != ''
              AND rt.transaction_id = (
                  SELECT rt2.transaction_id FROM roster_transactions rt2
                  WHERE rt2.player_id = p.player_id
                  ORDER BY rt2.transaction_date DESC, rt2.transaction_id DESC
                  LIMIT 1
              )
              AND rt.action = 'JOIN'
            ORDER BY p.nickname
        """, [team_id])

        if not roster:
            tiene_tx = query(
                "SELECT COUNT(*) AS n FROM roster_transactions WHERE team_id = ?",
                [team_id]
            )[0]['n']
            if not tiene_tx:
                roster = query("""
                    SELECT player_id, nickname, real_name, country
                    FROM players
                    WHERE team_id = ? AND nickname IS NOT NULL AND nickname != ''
                    ORDER BY nickname
                """, [team_id])

        transacciones = query("""
            SELECT rt.action, rt.transaction_date, rt.reference_url,
                   rt.player_id, p.nickname, p.real_name
            FROM roster_transactions rt
            LEFT JOIN players p ON p.player_id = rt.player_id
            WHERE rt.team_id = ?
            ORDER BY rt.transaction_date DESC, rt.transaction_id DESC
            LIMIT 40
        """, [team_id])

        partidos = query("""
            SELECT
                v.match_id, v.tournament, v.phase, v.match_date,
                v.score_a, v.score_b, v.patch, v.winner_id,
                v.team_a_id, ta.team_name AS team_a, ta.tag AS team_a_tag,
                v.team_b_id, tb.team_name AS team_b, tb.tag AS team_b_tag,
                e.event_id, e.event_name,
                (SELECT COUNT(*) FROM maps mp WHERE mp.match_id = v.match_id) AS maps_played
            FROM v_matches_events v
            LEFT JOIN teams  ta ON ta.team_id = v.team_a_id
            LEFT JOIN teams  tb ON tb.team_id = v.team_b_id
            LEFT JOIN events e  ON e.event_id  = v.event_id
            WHERE v.team_a_id = ? OR v.team_b_id = ?
            ORDER BY v.match_date DESC, v.match_id DESC
            LIMIT 60
        """, [team_id, team_id])

        jugadores = query("""
            SELECT
                p.player_id, p.nickname, p.real_name, p.country,
                COUNT(DISTINCT ps.match_id)     AS matches,
                ROUND(AVG(ps.rating), 2)        AS rating,
                ROUND(AVG(ps.acs), 0)           AS acs,
                SUM(ps.kills)                   AS kills,
                SUM(ps.deaths)                  AS deaths,
                SUM(ps.assists)                 AS assists,
                ROUND(AVG(ps.kast), 1)          AS kast,
                ROUND(AVG(ps.adr), 1)           AS adr,
                ROUND(AVG(ps.hs_percent), 1)    AS hs_percent,
                SUM(ps.fk)                      AS fk,
                SUM(ps.fd)                      AS fd
            FROM player_stats ps
            JOIN players p ON p.player_id = ps.player_id
            WHERE ps.team_id = ?
            GROUP BY ps.player_id
            ORDER BY rating DESC
        """, [team_id])

        mapas = query("""
            SELECT
                mp.map_name,
                COUNT(*) AS played,
                SUM(CASE
                    WHEN m.team_a_id = ?
                         AND (mp.score_a_attack + mp.score_a_defense) >
                             (mp.score_b_attack + mp.score_b_defense) THEN 1
                    WHEN m.team_b_id = ?
                         AND (mp.score_b_attack + mp.score_b_defense) >
                             (mp.score_a_attack + mp.score_a_defense) THEN 1
                    ELSE 0 END) AS wins,
                ROUND(AVG(
                    mp.score_a_attack + mp.score_a_defense +
                    mp.score_b_attack + mp.score_b_defense
                ), 1) AS avg_rounds
            FROM maps mp
            JOIN matches m ON m.match_id = mp.match_id
            WHERE m.team_a_id = ? OR m.team_b_id = ?
            GROUP BY mp.map_name
            ORDER BY played DESC
        """, [team_id, team_id, team_id, team_id])

        eventos = query("""
            SELECT
                v.tournament,
                MAX(v.event_id) AS event_id,
                COUNT(*) AS matches,
                SUM(CASE WHEN v.winner_id = ? THEN 1 ELSE 0 END) AS wins,
                MIN(v.match_date) AS start_date,
                MAX(v.match_date) AS end_date
            FROM v_matches_events v
            WHERE v.team_a_id = ? OR v.team_b_id = ?
            GROUP BY v.tournament
            ORDER BY MAX(v.match_date) DESC
        """, [team_id, team_id, team_id])

        return jsonify({
            "ok": True,
            "equipo": info,
            "record": record,
            "roster": roster,
            "transacciones": transacciones,
            "partidos": partidos,
            "jugadores": jugadores,
            "mapas": mapas,
            "eventos": eventos,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
