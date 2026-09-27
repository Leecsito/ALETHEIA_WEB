"""
ALETHEIA — Eventos Blueprint
Rutas: /api/eventos, /api/evento?event_id= | ?torneo=
Lista de eventos/torneos con rango de fechas y detalle con partidos, récord
por equipo, stats de mapas (meta del evento si existe) y pickrate de agentes.
"""

from flask import Blueprint, request, jsonify
try:
    from backend.conexion import get_conn, release_conn
except ImportError:
    from conexion import get_conn, release_conn

eventos_bp = Blueprint('eventos', __name__)


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


@eventos_bp.route('/api/eventos', methods=['GET'])
def list_eventos():
    try:
        q = request.args.get('q', '').strip()

        torneos = query("""
            SELECT tournament,
                   COUNT(*)          AS matches,
                   MIN(match_date)   AS start_date,
                   MAX(match_date)   AS end_date
            FROM matches
            WHERE tournament IS NOT NULL AND tournament != ''
            GROUP BY tournament
        """)

        equipos_n = query("""
            SELECT tournament, COUNT(DISTINCT team_id) AS teams FROM (
                SELECT tournament, team_a_id AS team_id FROM matches
                UNION ALL
                SELECT tournament, team_b_id FROM matches
            )
            GROUP BY tournament
        """)
        equipos_map = {r['tournament']: r['teams'] for r in equipos_n}

        resueltos = query("""
            SELECT tournament, MAX(event_id) AS event_id
            FROM v_matches_events GROUP BY tournament
        """)
        evento_map = {r['tournament']: r['event_id'] for r in resueltos}

        nombres = {r['event_id']: r['event_name'] for r in query("SELECT event_id, event_name FROM events")}

        data = []
        for t in torneos:
            name = t['tournament']
            if q and q.lower() not in name.lower() and q.lower() not in (nombres.get(evento_map.get(name)) or '').lower():
                continue
            eid = evento_map.get(name)
            data.append({
                **t,
                'teams': equipos_map.get(name, 0),
                'event_id': eid,
                'event_name': nombres.get(eid),
            })
        data.sort(key=lambda r: r.get('end_date') or '', reverse=True)

        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@eventos_bp.route('/api/evento', methods=['GET'])
def detalle_evento():
    try:
        event_id = request.args.get('event_id', '').strip()
        torneo   = request.args.get('torneo', '').strip()
        if not event_id and not torneo:
            return jsonify({"ok": False, "error": "Falta event_id o torneo."}), 400

        evento = None
        torneos = []

        if event_id:
            evento = query("SELECT event_id, event_name FROM events WHERE event_id = ?", [event_id])
            if not evento:
                return jsonify({"ok": False, "error": "Evento no encontrado."}), 404
            evento = evento[0]
            torneos = [r['tournament_name'] for r in query(
                "SELECT tournament_name FROM tournament_aliases WHERE event_id = ?", [event_id])]
            if evento['event_name'] not in torneos:
                torneos.append(evento['event_name'])
        else:
            torneos = [torneo]
            eid = query("SELECT MAX(event_id) AS event_id FROM v_matches_events WHERE tournament = ?", [torneo])
            if eid and eid[0]['event_id']:
                evento = query("SELECT event_id, event_name FROM events WHERE event_id = ?", [eid[0]['event_id']])
                evento = evento[0] if evento else None
                if evento:
                    alias = [r['tournament_name'] for r in query(
                        "SELECT tournament_name FROM tournament_aliases WHERE event_id = ?", [evento['event_id']])]
                    torneos = list(dict.fromkeys(torneos + alias + [evento['event_name']]))

        marks = ",".join(["?"] * len(torneos))

        resumen = query(f"""
            SELECT COUNT(*) AS matches,
                   MIN(match_date) AS start_date, MAX(match_date) AS end_date
            FROM matches WHERE tournament IN ({marks})
        """, torneos)[0]

        equipos_n = query(f"""
            SELECT COUNT(DISTINCT team_id) AS teams FROM (
                SELECT team_a_id AS team_id FROM matches WHERE tournament IN ({marks})
                UNION ALL
                SELECT team_b_id FROM matches WHERE tournament IN ({marks})
            )
        """, torneos + torneos)[0]['teams']

        partidos = query(f"""
            SELECT
                v.match_id, v.tournament, v.phase, v.match_date,
                v.score_a, v.score_b, v.patch, v.winner_id,
                v.team_a_id, ta.team_name AS team_a, ta.tag AS team_a_tag,
                v.team_b_id, tb.team_name AS team_b, tb.tag AS team_b_tag,
                (SELECT COUNT(*) FROM maps mp WHERE mp.match_id = v.match_id) AS maps_played
            FROM v_matches_events v
            LEFT JOIN teams ta ON ta.team_id = v.team_a_id
            LEFT JOIN teams tb ON tb.team_id = v.team_b_id
            WHERE v.tournament IN ({marks})
            ORDER BY v.match_date DESC, v.match_id DESC
        """, torneos)

        equipos_raw = query(f"""
            SELECT team_id, COUNT(*) AS matches, SUM(won) AS wins FROM (
                SELECT team_a_id AS team_id,
                       CASE WHEN winner_id = team_a_id THEN 1 ELSE 0 END AS won
                FROM matches WHERE tournament IN ({marks})
                UNION ALL
                SELECT team_b_id,
                       CASE WHEN winner_id = team_b_id THEN 1 ELSE 0 END
                FROM matches WHERE tournament IN ({marks})
            )
            GROUP BY team_id
        """, torneos + torneos)

        mapas_equipo = query(f"""
            SELECT team_id, COUNT(*) AS maps, SUM(won) AS map_wins FROM (
                SELECT m.team_a_id AS team_id,
                       CASE WHEN (mp.score_a_attack + mp.score_a_defense) >
                                 (mp.score_b_attack + mp.score_b_defense)
                            THEN 1 ELSE 0 END AS won
                FROM maps mp JOIN matches m ON m.match_id = mp.match_id
                WHERE m.tournament IN ({marks})
                UNION ALL
                SELECT m.team_b_id,
                       CASE WHEN (mp.score_b_attack + mp.score_b_defense) >
                                 (mp.score_a_attack + mp.score_a_defense)
                            THEN 1 ELSE 0 END
                FROM maps mp JOIN matches m ON m.match_id = mp.match_id
                WHERE m.tournament IN ({marks})
            )
            GROUP BY team_id
        """, torneos + torneos)
        mapas_equipo_map = {r['team_id']: r for r in mapas_equipo}

        ids = [r['team_id'] for r in equipos_raw if r['team_id'] is not None]
        nombres_equipos = {}
        if ids:
            tmarks = ",".join(["?"] * len(ids))
            nombres_equipos = {r['team_id']: r for r in query(
                f"SELECT team_id, team_name, tag, region, country FROM teams WHERE team_id IN ({tmarks})", ids)}

        equipos = []
        for r in equipos_raw:
            tid = r['team_id']
            if tid is None or tid not in nombres_equipos:
                continue
            info = nombres_equipos[tid]
            mstats = mapas_equipo_map.get(tid, {})
            equipos.append({
                'team_id': tid,
                'team_name': info['team_name'],
                'tag': info['tag'],
                'region': info['region'],
                'country': info['country'],
                'matches': r['matches'],
                'wins': r['wins'] or 0,
                'maps': mstats.get('maps') or 0,
                'map_wins': mstats.get('map_wins') or 0,
            })
        equipos.sort(key=lambda r: (-(r['wins'] or 0), -(r['maps'] or 0), r['team_name'] or ''))

        mapas = query(f"""
            SELECT
                mp.map_name,
                COUNT(*) AS played,
                SUM(CASE WHEN mp.picker = 'a' THEN 1 ELSE 0 END)       AS picked_a,
                SUM(CASE WHEN mp.picker = 'b' THEN 1 ELSE 0 END)       AS picked_b,
                SUM(CASE WHEN mp.picker = 'decider' THEN 1 ELSE 0 END) AS deciders
            FROM maps mp
            JOIN matches m ON m.match_id = mp.match_id
            WHERE m.tournament IN ({marks})
            GROUP BY mp.map_name
            ORDER BY played DESC
        """, torneos)

        bans = query(f"""
            SELECT mv.map_name, COUNT(*) AS bans
            FROM match_veto mv
            JOIN matches m ON m.match_id = mv.match_id
            WHERE mv.action = 'ban' AND m.tournament IN ({marks})
            GROUP BY mv.map_name
        """, torneos)
        bans_map = {r['map_name']: r['bans'] for r in bans}

        meta_map = {}
        if evento:
            meta_map = {r['map_name']: r for r in query("""
                SELECT map_name, matches_played, atk_win_pct, def_win_pct
                FROM event_map_stats WHERE event_id = ?
            """, [evento['event_id']])}
        for m in mapas:
            m['bans'] = bans_map.get(m['map_name'], 0)
            meta = meta_map.get(m['map_name'], {})
            m['atk_win_pct'] = meta.get('atk_win_pct')
            m['def_win_pct'] = meta.get('def_win_pct')

        agentes = []
        if evento:
            agentes = query("""
                SELECT p.map_name, p.agent_name, p.pick_pct, a.role
                FROM event_agent_pickrate p
                LEFT JOIN agents a ON LOWER(a.agent_name) = LOWER(p.agent_name)
                WHERE p.event_id = ?
                ORDER BY p.map_name, p.pick_pct DESC
            """, [evento['event_id']])
        else:
            totales = {r['map_name']: r['n'] for r in query(f"""
                SELECT mp.map_name, COUNT(*) AS n
                FROM maps mp JOIN matches m ON m.match_id = mp.match_id
                WHERE m.tournament IN ({marks})
                GROUP BY mp.map_name
            """, torneos)}
            filas = query(f"""
                SELECT mp.map_name, ps.agent AS agent_name, COUNT(DISTINCT ps.map_id) AS maps
                FROM player_stats ps
                JOIN maps mp ON mp.map_id = ps.map_id
                JOIN matches m ON m.match_id = mp.match_id
                WHERE m.tournament IN ({marks})
                      AND ps.agent IS NOT NULL AND ps.agent != '' AND ps.agent != 'Unknown'
                GROUP BY mp.map_name, ps.agent
            """, torneos)
            for r in filas:
                n = totales.get(r['map_name']) or 1
                agentes.append({
                    'map_name': r['map_name'],
                    'agent_name': r['agent_name'],
                    'pick_pct': round((r['maps'] or 0) * 100.0 / n),
                    'role': None,
                })
            roles = {r['agent_name'].lower(): r['role'] for r in query("SELECT agent_name, role FROM agents")}
            for a in agentes:
                a['role'] = roles.get((a['agent_name'] or '').lower())
            agentes.sort(key=lambda r: (r['map_name'], -(r['pick_pct'] or 0)))

        nombre = evento['event_name'] if evento else torneo
        return jsonify({
            "ok": True,
            "evento": {
                'event_id': evento['event_id'] if evento else None,
                'nombre': nombre,
                'torneos': torneos,
                **resumen,
                'teams': equipos_n,
            },
            "partidos": partidos,
            "equipos": equipos,
            "mapas": mapas,
            "agentes": agentes,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
