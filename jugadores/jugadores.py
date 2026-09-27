"""
ALETHEIA — Jugadores Blueprint
Rutas: /api/jugadores, /api/jugador/<player_id>
Lista de jugadores con promedios y detalle con agentes, partidos recientes,
estadísticas por mapa e historial de equipos.
"""

from datetime import date
from flask import Blueprint, request, jsonify
try:
    from backend.conexion import fetch_all
    from backend.cache import ttl_cache
except ImportError:
    from conexion import fetch_all
    from cache import ttl_cache

jugadores_bp = Blueprint('jugadores', __name__)

ORDENES = {
    'rating': 'rating DESC',
    'acs':    'acs DESC',
    'kd':     '(1.0 * SUM(ps.kills)) / (SUM(ps.deaths) + 0.0001) DESC',
    'matches': 'matches DESC',
    'nombre': 'p.nickname COLLATE NOCASE',
}


@ttl_cache(300)
def query(sql, params=None):
    return fetch_all(sql, params)


@jugadores_bp.route('/api/jugadores', methods=['GET'])
def list_jugadores():
    try:
        page  = max(1, int(request.args.get('page', 1)))
        limit = min(200, max(1, int(request.args.get('limit', 60))))
        q     = request.args.get('q', '').strip()
        orden = request.args.get('orden', 'rating').strip().lower()
        order_sql = ORDENES.get(orden, ORDENES['rating'])

        conds, params = ["p.nickname IS NOT NULL", "p.nickname != ''"], []
        if q:
            like = f"%{q}%"
            conds.append("(p.nickname LIKE ? OR p.real_name LIKE ? OR t.team_name LIKE ? OR t.tag LIKE ?)")
            params += [like] * 4
        where = "WHERE " + " AND ".join(conds)

        base_from = """
            FROM players p
            JOIN player_stats ps ON ps.player_id = p.player_id
            LEFT JOIN teams t ON t.team_id = p.team_id
            {where}
            GROUP BY p.player_id
        """.format(where=where)

        total = query(f"SELECT COUNT(*) AS n FROM (SELECT p.player_id {base_from})", params)[0]['n']

        data = query(f"""
            SELECT
                p.player_id, p.nickname, p.real_name, p.country,
                t.team_id, t.team_name, t.tag, t.region,
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
            {base_from}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
        """, params + [limit, (page - 1) * limit])

        return jsonify({
            "ok": True, "total": total, "page": page, "limit": limit,
            "pages": max(1, -(-total // limit)), "data": data,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _span_days(row):
    """Anchura de la ventana temporal de una fila de player_agent_stats."""
    try:
        d0 = date.fromisoformat(str(row.get('date_start'))[:10])
        d1 = date.fromisoformat(str(row.get('date_end'))[:10])
        return (d1 - d0).days
    except Exception:
        return -1


@jugadores_bp.route('/api/jugador/<int:player_id>', methods=['GET'])
def detalle_jugador(player_id):
    try:
        info = query("""
            SELECT p.*, t.team_name, t.tag, t.region, t.country AS team_country
            FROM players p
            LEFT JOIN teams t ON t.team_id = p.team_id
            WHERE p.player_id = ?
        """, [player_id])
        if not info:
            return jsonify({"ok": False, "error": "Jugador no encontrado."}), 404
        info = info[0]

        totales = query("""
            SELECT
                COUNT(DISTINCT ps.match_id)     AS matches,
                COUNT(DISTINCT ps.map_id)       AS maps,
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
            WHERE ps.player_id = ?
        """, [player_id])[0]

        multikills = query("""
            SELECT
                COALESCE(SUM(k2), 0) AS k2, COALESCE(SUM(k3), 0) AS k3,
                COALESCE(SUM(k4), 0) AS k4, COALESCE(SUM(k5), 0) AS k5,
                COALESCE(SUM(v1), 0) AS v1, COALESCE(SUM(v2), 0) AS v2,
                COALESCE(SUM(v3), 0) AS v3, COALESCE(SUM(v4), 0) AS v4,
                COALESCE(SUM(v5), 0) AS v5,
                COALESCE(SUM(plants), 0)  AS plants,
                COALESCE(SUM(defuses), 0) AS defuses
            FROM multikills_clutches
            WHERE player_id = ?
        """, [player_id])[0]

        agentes_raw = query("""
            SELECT agent, date_start, date_end, use_count, rnd, rating, acs, kd,
                   kast, adr, kpr, apr, fk_fd, k, d, a, fk, fd
            FROM player_agent_stats
            WHERE player_id = ?
        """, [player_id])

        roles = {r['agent_name']: r['role'] for r in query("SELECT agent_name, role FROM agents")}

        partidos = query("""
            SELECT
                ps.match_id, m.match_date, m.tournament, m.phase,
                m.score_a, m.score_b,
                m.team_a_id, ta.team_name AS team_a, ta.tag AS team_a_tag,
                m.team_b_id, tb.team_name AS team_b, tb.tag AS team_b_tag,
                m.winner_id,
                MAX(ps.team_id)                 AS player_team_id,
                COUNT(DISTINCT ps.map_id)       AS maps,
                ROUND(AVG(ps.rating), 2)        AS rating,
                ROUND(AVG(ps.acs), 0)           AS acs,
                SUM(ps.kills)                   AS kills,
                SUM(ps.deaths)                  AS deaths,
                SUM(ps.assists)                 AS assists,
                GROUP_CONCAT(DISTINCT ps.agent) AS agents,
                e.event_id, e.event_name
            FROM player_stats ps
            JOIN matches m ON m.match_id = ps.match_id
            LEFT JOIN v_matches_events v ON v.match_id = m.match_id
            LEFT JOIN events e ON e.event_id = v.event_id
            LEFT JOIN teams ta ON ta.team_id = m.team_a_id
            LEFT JOIN teams tb ON tb.team_id = m.team_b_id
            WHERE ps.player_id = ?
            GROUP BY ps.match_id
            ORDER BY m.match_date DESC, m.match_id DESC
            LIMIT 30
        """, [player_id])

        mapas = query("""
            SELECT
                mp.map_name,
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
            JOIN maps mp ON mp.map_id = ps.map_id
            WHERE ps.player_id = ?
            GROUP BY mp.map_name
            ORDER BY matches DESC
        """, [player_id])

        equipos = query("""
            SELECT rt.action, rt.transaction_date, rt.reference_url,
                   t.team_id, t.team_name, t.tag, t.region
            FROM roster_transactions rt
            JOIN teams t ON t.team_id = rt.team_id
            WHERE rt.player_id = ?
            ORDER BY rt.transaction_date DESC, rt.transaction_id DESC
        """, [player_id])

        # Agentes: nos quedamos con la ventana más amplia por agente (equivalente a "All").
        mejores = {}
        for row in agentes_raw:
            prev = mejores.get(row['agent'])
            if prev is None or _span_days(row) > _span_days(prev):
                mejores[row['agent']] = row
        agentes = sorted(mejores.values(), key=lambda r: (r.get('use_count') or 0), reverse=True)
        for a in agentes:
            a['role'] = roles.get((a.get('agent') or '').lower())

        return jsonify({
            "ok": True,
            "jugador": info,
            "totales": totales,
            "multikills": multikills,
            "agentes": agentes,
            "partidos": partidos,
            "mapas": mapas,
            "equipos": equipos,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
