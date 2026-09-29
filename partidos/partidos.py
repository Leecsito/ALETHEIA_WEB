"""
ALETHEIA — Partidos Blueprint
Rutas: /api/partidos, /api/partidos/filtros, /api/partidos/resultados,
       /api/partido/<match_id>
Listado estilo vlr.gg (con evento resuelto y filtros) y detalle completo
de un partido (veto, mapas, rondas, economía y scoreboard por mapa).
"""

from flask import Blueprint, request, jsonify
try:
    from backend.conexion import fetch_all
    from backend.cache import ttl_cache
except ImportError:
    from conexion import fetch_all
    from cache import ttl_cache

partidos_bp = Blueprint('partidos', __name__)


@ttl_cache(120)
def query(sql, params=None):
    return fetch_all(sql, params)


MATCH_SELECT = """
    SELECT
        v.match_id,
        v.tournament,
        v.phase,
        v.match_date,
        v.score_a,
        v.score_b,
        v.patch,
        v.team_a_id,
        ta.team_name AS team_a,
        ta.tag       AS team_a_tag,
        ta.country   AS team_a_country,
        v.team_b_id,
        tb.team_name AS team_b,
        tb.tag       AS team_b_tag,
        tb.country   AS team_b_country,
        v.winner_id,
        e.event_id,
        e.event_name,
        (SELECT COUNT(*) FROM maps mp WHERE mp.match_id = v.match_id) AS maps_played
    FROM v_matches_events v
    LEFT JOIN teams  ta ON ta.team_id = v.team_a_id
    LEFT JOIN teams  tb ON tb.team_id = v.team_b_id
    LEFT JOIN events e  ON e.event_id  = v.event_id
"""


# ─── LISTA DE PARTIDOS ───────────────────────────────────────────────────────
@partidos_bp.route('/api/partidos', methods=['GET'])
def list_partidos():
    try:
        page   = max(1, int(request.args.get('page', 1)))
        limit  = min(200, max(1, int(request.args.get('limit', 60))))
        q      = request.args.get('q', '').strip()
        torneo = request.args.get('torneo', '').strip()
        year   = request.args.get('year', '').strip()
        evento = request.args.get('event_id', '').strip()
        orden  = request.args.get('orden', 'recientes').strip()

        conds, params = [], []
        if q:
            like = f"%{q}%"
            conds.append("""(
                ta.team_name LIKE ? OR tb.team_name LIKE ? OR
                ta.tag LIKE ? OR tb.tag LIKE ? OR
                v.tournament LIKE ? OR v.phase LIKE ?
            )""")
            params += [like] * 6
        if torneo:
            conds.append("v.tournament = ?")
            params.append(torneo)
        if year:
            conds.append("substr(v.match_date, 1, 4) = ?")
            params.append(year)
        if evento:
            try:
                evento = int(evento)
            except ValueError:
                evento = None
            if evento is not None:
                conds.append("v.event_id = ?")
                params.append(evento)

        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        direction = "ASC" if orden == 'antiguos' else "DESC"

        total = query(f"SELECT COUNT(*) AS n FROM v_matches_events v "
                      f"LEFT JOIN teams ta ON ta.team_id = v.team_a_id "
                      f"LEFT JOIN teams tb ON tb.team_id = v.team_b_id {where}", params)[0]['n']

        data = query(
            MATCH_SELECT + f"""
            {where}
            ORDER BY
                CASE WHEN v.match_date IS NULL OR v.match_date = '' THEN 1 ELSE 0 END,
                v.match_date {direction},
                v.match_id {direction}
            LIMIT ? OFFSET ?
            """,
            params + [limit, (page - 1) * limit]
        )

        return jsonify({
            "ok": True, "total": total, "page": page, "limit": limit,
            "pages": max(1, -(-total // limit)), "data": data,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ─── FILTROS (torneos y años) ────────────────────────────────────────────────
@partidos_bp.route('/api/partidos/filtros', methods=['GET'])
def filtros_partidos():
    try:
        torneos = query("""
            SELECT
                v.tournament,
                MAX(v.event_id) AS event_id,
                COUNT(*)        AS n,
                MIN(v.match_date) AS start_date,
                MAX(v.match_date) AS end_date
            FROM v_matches_events v
            WHERE v.tournament IS NOT NULL AND v.tournament != ''
            GROUP BY v.tournament
            ORDER BY MAX(v.match_date) DESC
        """)
        years = query("""
            SELECT substr(match_date, 1, 4) AS year, COUNT(*) AS n
            FROM matches
            WHERE match_date IS NOT NULL AND match_date != ''
            GROUP BY year ORDER BY year DESC
        """)
        return jsonify({"ok": True, "torneos": torneos, "years": years})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ─── RESULTADO REAL (para separar predicciones pendientes de las jugadas) ────
@partidos_bp.route('/api/partidos/resultados', methods=['GET'])
def resultados_partidos():
    """Dado `match_ids` (lista separada por comas) devuelve cuáles ya tienen
    resultado real en la DB (al menos un mapa jugado). Lo usa EN VIVO para
    filtrar las simulaciones pendientes de las ya jugadas."""
    try:
        crudos = request.args.get('match_ids', '')
        ids = []
        for parte in crudos.split(','):
            parte = parte.strip()
            if not parte:
                continue
            try:
                n = int(parte)
            except ValueError:
                continue
            if n > 0 and n not in ids:
                ids.append(n)
        ids = ids[:500]
        if not ids:
            return jsonify({"ok": True, "con_resultado": []})
        marks = ",".join(["?"] * len(ids))
        filas = query(f"SELECT DISTINCT match_id FROM maps WHERE match_id IN ({marks})", ids)
        return jsonify({"ok": True, "con_resultado": [f['match_id'] for f in filas]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ─── DETALLE DE PARTIDO ──────────────────────────────────────────────────────
@partidos_bp.route('/api/partido/<int:match_id>', methods=['GET'])
def detalle_partido(match_id):
    try:
        partido = query(MATCH_SELECT + " WHERE v.match_id = ?", [match_id])
        if not partido:
            return jsonify({"ok": False, "error": "Partido no encontrado."}), 404
        partido = partido[0]

        veto = query("""
            SELECT action, team, map_name, veto_order, team_id
            FROM match_veto WHERE match_id = ?
            ORDER BY veto_order
        """, [match_id])

        mapas = query("""
            SELECT map_id, map_name, map_number, picker, side_chosen, side_top_start,
                   score_a_attack, score_a_defense, score_b_attack, score_b_defense,
                   duration, picker_id
            FROM maps WHERE match_id = ?
            ORDER BY map_number
        """, [match_id])

        stats = query("""
            SELECT
                ps.map_id, ps.player_id, ps.team_id,
                p.nickname, p.real_name, p.country,
                MAX(ps.agent)               AS agent,
                SUM(ps.kills)               AS kills,
                SUM(ps.deaths)              AS deaths,
                SUM(ps.assists)             AS assists,
                ROUND(AVG(ps.rating), 2)    AS rating,
                ROUND(AVG(ps.acs), 0)       AS acs,
                ROUND(AVG(ps.kast), 1)      AS kast,
                ROUND(AVG(ps.adr), 1)       AS adr,
                ROUND(AVG(ps.hs_percent), 1) AS hs_percent,
                SUM(ps.fk)                  AS fk,
                SUM(ps.fd)                  AS fd
            FROM player_stats ps
            LEFT JOIN players p ON p.player_id = ps.player_id
            WHERE ps.match_id = ?
            GROUP BY ps.map_id, ps.player_id, ps.team_id
        """, [match_id])

        economy = query("""
            SELECT map_id, team_id, pistol_won,
                   eco_played, eco_won, semi_eco_played, semi_eco_won,
                   semi_buy_played, semi_buy_won, full_buy_played, full_buy_won
            FROM economy_summary WHERE match_id = ?
        """, [match_id])

        rounds = []
        map_ids = [m['map_id'] for m in mapas if m.get('map_id')]
        if map_ids:
            marks = ",".join(["?"] * len(map_ids))
            rounds = query(f"""
                SELECT map_id, round_num, winner_id, winning_side, result_type
                FROM rounds WHERE map_id IN ({marks})
                ORDER BY map_id, round_num
            """, map_ids)

        # Repartir las piezas por mapa
        by_map = {}
        for m in mapas:
            m['rounds'] = []
            m['players'] = []
            m['economy'] = []
            by_map[m['map_id']] = m
        for r in rounds:
            if r['map_id'] in by_map:
                by_map[r['map_id']]['rounds'].append(r)
        for s in stats:
            if s['map_id'] in by_map:
                by_map[s['map_id']]['players'].append(s)
        for e in economy:
            if e['map_id'] in by_map:
                by_map[e['map_id']]['economy'].append(e)

        return jsonify({"ok": True, "partido": partido, "veto": veto, "maps": mapas})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
