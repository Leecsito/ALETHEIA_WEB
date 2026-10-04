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
def _total_score(ataque, defensa):
    """Suma las rondas de un lado del mapa; None si la fila no trae marcador."""
    try:
        return int(ataque) + int(defensa)
    except (TypeError, ValueError):
        return None


# Categorías de compra del detalle (mismas que `economy_summary`).
CAT_ECO = ('eco', 'semi_eco', 'semi_buy', 'full_buy')


def _categoria(valor):
    """Normaliza la categoría de compra ('semi-buy' -> 'semi_buy')."""
    return str(valor or '').strip().lower().replace('-', '_')


def _economia_desde_rounds(rounds, mapas, partido, economy_summary):
    """Economía real por (mapa, equipo) calculada desde `rounds` (H5).

    Misma fuente que el scorecard de ALETHEIA_PREDICT: excluye las rondas
    pistol (R1/R13) de las categorías, resuelve la categoría propia con
    `team_top_id`/`team_bot_id` y el ganador con `winner_id`. Devuelve filas
    con la forma de `economy_summary` (`*_played`/`*_won`) y
    `fuente='rounds'`. Los mapas sin `rounds` conservan su fila de
    `economy_summary`, etiquetada `fuente='economy_summary'` (benchmark no
    fiable en eco: sus columnas no son consistentes con `rounds`).
    """
    por_mapa = {}
    for r in rounds:
        por_mapa.setdefault(r.get('map_id'), []).append(r)
    filas = []
    con_rounds = set()
    for m in mapas:
        mid = m.get('map_id')
        if not mid:
            continue
        equipos = [t for t in (partido.get('team_a_id'), partido.get('team_b_id')) if t]
        if not equipos:
            for r in por_mapa.get(mid, []):
                for t in (r.get('team_top_id'), r.get('team_bot_id')):
                    if t and t not in equipos:
                        equipos.append(t)
        for team_id in equipos:
            datos = {c: [0, 0] for c in CAT_ECO}   # [played, won]
            pistol_won = 0
            visto = False
            for r in por_mapa.get(mid, []):
                if r.get('team_top_id') == team_id:
                    cat = _categoria(r.get('category_top'))
                elif r.get('team_bot_id') == team_id:
                    cat = _categoria(r.get('category_bot'))
                else:
                    continue
                visto = True
                try:
                    rn = int(r.get('round_num'))
                except (TypeError, ValueError):
                    continue
                gano = 1 if r.get('winner_id') == team_id else 0
                if rn in (1, 13):
                    pistol_won += gano
                    continue
                if cat in datos:
                    datos[cat][0] += 1
                    datos[cat][1] += gano
            if not visto:
                continue
            fila = {'map_id': mid, 'team_id': team_id, 'fuente': 'rounds',
                    'pistol_won': pistol_won}
            for c in CAT_ECO:
                fila[f'{c}_played'] = datos[c][0]
                fila[f'{c}_won'] = datos[c][1]
            filas.append(fila)
            con_rounds.add(mid)
    # Fallback etiquetado para mapas sin `rounds` (no se compara con el motor).
    for e in economy_summary or []:
        if e.get('map_id') not in con_rounds:
            fila = dict(e)
            fila['fuente'] = 'economy_summary'
            filas.append(fila)
    return filas


@partidos_bp.route('/api/partidos/resultados', methods=['GET'])
def resultados_partidos():
    """Dado `match_ids` (lista separada por comas) devuelve cuáles ya tienen
    resultado real en la DB (al menos un mapa jugado). Lo usa EN VIVO para
    filtrar las simulaciones pendientes de las ya jugadas.

    Además devuelve `partidos` (por match_id) con la identidad de los equipos
    (ids/tags para los logos) y un resumen **predicción vs realidad** en la
    orientación de la predicción (`equipo_a`/`equipo_b` del motor): marcador
    real de la serie, P del motor por mapa (`p_a`), P media que el motor dio a
    los ganadores reales (`p_real_media`) y aciertos del favorito
    (`favoritos_ok`/`n_mapas`), más el detalle por mapa (`mapas[]`). Si las
    tablas del servicio ALETHEIA_PREDICT no existen (DB local vieja) se degrada
    a solo la identidad y el resultado real.
    """
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
            return jsonify({"ok": True, "con_resultado": [], "partidos": {}})
        marks = ",".join(["?"] * len(ids))
        filas = query(f"SELECT DISTINCT match_id FROM maps WHERE match_id IN ({marks})", ids)
        con_resultado = [f['match_id'] for f in filas]

        # Predicciones del motor para cualquier id (aunque no tenga resultado):
        # dan la identidad (nombres/ids) y la P por mapa (constante en el partido).
        preds = {}
        try:
            filas_pred = query(
                f"SELECT match_id, equipo_a, equipo_b, equipo_a_id, equipo_b_id, prob_victoria_a "
                f"FROM predicciones_mapa WHERE match_id IN ({marks})", ids)
            for r in filas_pred:
                p = preds.setdefault(r['match_id'], {
                    'nombre_a': r.get('equipo_a'), 'nombre_b': r.get('equipo_b'),
                    'a_id': r.get('equipo_a_id'), 'b_id': r.get('equipo_b_id'), 'p_a': None,
                })
                p['nombre_a'] = p['nombre_a'] or r.get('equipo_a')
                p['nombre_b'] = p['nombre_b'] or r.get('equipo_b')
                p['a_id'] = p['a_id'] or r.get('equipo_a_id')
                p['b_id'] = p['b_id'] or r.get('equipo_b_id')
                if p['p_a'] is None:
                    p['p_a'] = r.get('prob_victoria_a')
        except Exception:
            preds = {}

        # Info real de los partidos jugados (equipos, marcador de serie).
        info = {}
        if con_resultado:
            marcas_res = ",".join(["?"] * len(con_resultado))
            for p in query(MATCH_SELECT + f" WHERE v.match_id IN ({marcas_res})", con_resultado):
                info[p['match_id']] = p

        # Mapas reales (marcador por mapa) para medir la predicción mapa a mapa.
        mapas = {}
        if con_resultado:
            marcas_res = ",".join(["?"] * len(con_resultado))
            try:
                for m in query(
                        f"SELECT match_id, map_name, map_number, "
                        f"score_a_attack, score_a_defense, score_b_attack, score_b_defense "
                        f"FROM maps WHERE match_id IN ({marcas_res}) ORDER BY map_number", con_resultado):
                    mapas.setdefault(m['match_id'], []).append(m)
            except Exception:
                mapas = {}

        # Pool completo del veto (picks + decider, en orden): es la lista que se
        # usa para calcular la P de serie. Pasar solo los mapas jugados rompe el
        # caso bo3 terminado 2-0 (el endpoint /serie necesita los 3 del pool).
        serie_mapas = {}
        if con_resultado:
            marcas_res = ",".join(["?"] * len(con_resultado))
            try:
                for v in query(
                        f"SELECT match_id, map_name, veto_order, action FROM match_veto "
                        f"WHERE match_id IN ({marcas_res}) ORDER BY veto_order", con_resultado):
                    accion = str(v.get('action') or '').strip().lower()
                    mapa = (v.get('map_name') or '').strip()
                    if mapa and accion in ('pick', 'decider'):
                        lista = serie_mapas.setdefault(v['match_id'], [])
                        if mapa not in lista:
                            lista.append(mapa)
            except Exception:
                serie_mapas = {}

        # Tags de equipos (la tabla `teams` es la fuente de verdad).
        tags = {}
        ids_equipo = set()
        for p in list(info.values()) + list(preds.values()):
            for k in ('team_a_id', 'team_b_id', 'a_id', 'b_id'):
                if p.get(k):
                    ids_equipo.add(p[k])
        if ids_equipo:
            lista_ids = sorted(ids_equipo)
            marcas_teams = ",".join(["?"] * len(lista_ids))
            try:
                for t in query(f"SELECT team_id, tag FROM teams WHERE team_id IN ({marcas_teams})", lista_ids):
                    tags[t['team_id']] = t['tag']
            except Exception:
                tags = {}

        partidos = {}
        for mid in ids:
            pr = preds.get(mid) or {}
            pa = info.get(mid) or {}
            a_id = pr.get('a_id') or pa.get('team_a_id')
            b_id = pr.get('b_id') or pa.get('team_b_id')
            if not (a_id or b_id or pa):
                continue
            # ¿La "equipo_a" de la predicción es la team_a de `matches`?
            # (si no hay ids, se intenta por nombre; sin datos, no se invierte).
            if pr.get('a_id') and pa.get('team_a_id'):
                mismo_lado = pr['a_id'] == pa['team_a_id']
            elif pr.get('nombre_a') and pa.get('team_a'):
                mismo_lado = pr['nombre_a'].strip().lower() == pa['team_a'].strip().lower()
            else:
                mismo_lado = True

            # Normalizar a la orientación de la predicción (equipo_a del motor).
            if mismo_lado:
                nombre_a = pa.get('team_a')
                nombre_b = pa.get('team_b')
                score_a, score_b = pa.get('score_a'), pa.get('score_b')
            else:
                nombre_a = pa.get('team_b')
                nombre_b = pa.get('team_a')
                score_a, score_b = pa.get('score_b'), pa.get('score_a')
            nombre_a = pr.get('nombre_a') or nombre_a
            nombre_b = pr.get('nombre_b') or nombre_b
            p_a = pr.get('p_a')

            detalle_mapas, p_reales, favoritos_ok = [], [], 0
            for m in mapas.get(mid, []):
                sa = _total_score(m.get('score_a_attack'), m.get('score_a_defense'))
                sb = _total_score(m.get('score_b_attack'), m.get('score_b_defense'))
                if sa is None or sb is None or sa == sb:
                    continue
                gano_a = (sa > sb) if mismo_lado else (sb > sa)
                fila = {'map_name': m.get('map_name'), 'gano_a': 1 if gano_a else 0}
                if p_a is not None:
                    p_ganador = p_a if gano_a else 1 - p_a
                    fila['p_ganador'] = round(p_ganador, 4)
                    p_reales.append(p_ganador)
                    if p_ganador >= 0.5:
                        favoritos_ok += 1
                detalle_mapas.append(fila)

            partidos[str(mid)] = {
                'match_id': mid,
                'team_a_id': a_id, 'team_b_id': b_id,
                'team_a': nombre_a, 'team_b': nombre_b,
                'team_a_tag': tags.get(a_id) if a_id else None,
                'team_b_tag': tags.get(b_id) if b_id else None,
                'score_a': score_a, 'score_b': score_b,
                'winner_id': pa.get('winner_id'),
                'match_date': pa.get('match_date'),
                'p_a': round(p_a, 4) if p_a is not None else None,
                'p_real_media': round(sum(p_reales) / len(p_reales), 4) if p_reales else None,
                'favoritos_ok': favoritos_ok,
                'n_mapas': len(detalle_mapas),
                'mapas': detalle_mapas,
                'serie_mapas': serie_mapas.get(mid) or [m.get('map_name') for m in mapas.get(mid, []) if m.get('map_name')],
            }
        return jsonify({"ok": True, "con_resultado": con_resultado, "partidos": partidos})
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

        economy_summary = query("""
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
                SELECT map_id, round_num, winner_id, winning_side, result_type,
                       team_top_id, team_bot_id, category_top, category_bot
                FROM rounds WHERE map_id IN ({marks})
                ORDER BY map_id, round_num
            """, map_ids)

        # H5: la economía "real" del panel se calcula desde `rounds`
        # (excluyendo R1/R13); `economy_summary` queda solo como fallback
        # etiquetado (benchmark no fiable en eco).
        economy = _economia_desde_rounds(rounds, mapas, partido, economy_summary)

        # Repartir las piezas por mapa (el timeline solo conserva sus campos).
        by_map = {}
        for m in mapas:
            m['rounds'] = []
            m['players'] = []
            m['economy'] = []
            by_map[m['map_id']] = m
        for r in rounds:
            if r['map_id'] in by_map:
                by_map[r['map_id']]['rounds'].append({
                    'map_id': r['map_id'], 'round_num': r['round_num'],
                    'winner_id': r['winner_id'], 'winning_side': r['winning_side'],
                    'result_type': r['result_type'],
                })
        for s in stats:
            if s['map_id'] in by_map:
                by_map[s['map_id']]['players'].append(s)
        for e in economy:
            if e['map_id'] in by_map:
                by_map[e['map_id']]['economy'].append(e)

        return jsonify({"ok": True, "partido": partido, "veto": veto, "maps": mapas})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
