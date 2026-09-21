"""
ALETHEIA — Tablas API Blueprint
Rutas: /api/tabla/<nombre>, /api/tablas, /api/tablas/reporte
Permite ver el contenido raw de cualquier tabla de la BD con paginacion,
y generar un reporte de calidad de datos por partido.
"""

from collections import defaultdict
from flask import Blueprint, request, jsonify
try:
    from backend.conexion import get_conn, release_conn
except ImportError:
    from conexion import get_conn, release_conn

tablas_bp = Blueprint('tablas', __name__)

TABLAS_PERMITIDAS = [
    'matches', 'match_veto', 'maps', 'rounds',
    'player_stats', 'economy_summary', 'duels', 'multikills_clutches',
    'teams', 'players', 'roster_transactions', 'agents', 'player_agent_stats',
    'events', 'event_map_stats', 'event_agent_pickrate',
    # Tablas del servicio ALETHEIA_PREDICT (solo consulta/muestra).
    'predicciones_mapa', 'predicciones_serie',
]

@tablas_bp.route('/api/tabla/<nombre>', methods=['GET'])
def get_tabla(nombre):
    if nombre not in TABLAS_PERMITIDAS:
        return jsonify({"ok": False, "error": f"Tabla '{nombre}' no permitida."}), 403

    page   = max(1, int(request.args.get('page', 1)))
    limit  = min(200, max(1, int(request.args.get('limit', 50))))
    search = request.args.get('search', '').strip()
    offset = (page - 1) * limit

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Obtener columnas via PRAGMA (equivalente a information_schema en SQLite)
        cur.execute(f"PRAGMA table_info({nombre})")
        cols_info = cur.fetchall()
        # cols_info: (cid, name, type, notnull, dflt_value, pk)
        columnas  = [c[1] for c in cols_info]
        text_cols = [c[1] for c in cols_info
                     if any(t in c[2].upper() for t in ('TEXT', 'CHAR', 'CLOB', 'VARCHAR'))]

        # WHERE para busqueda — SQLite usa LIKE (case-insensitive para ASCII)
        where_clause = ""
        params = []
        if search and text_cols:
            conditions = [f"CAST({col} AS TEXT) LIKE ?" for col in text_cols]
            where_clause = "WHERE " + " OR ".join(conditions)
            params = [f"%{search}%"] * len(text_cols)

        # Total de filas
        cur.execute(f"SELECT COUNT(*) FROM {nombre} {where_clause}", params)
        total = cur.fetchone()[0]

        # Filas paginadas
        cur.execute(
            f"SELECT * FROM {nombre} {where_clause} LIMIT ? OFFSET ?",
            params + [limit, offset]
        )
        raw_rows = cur.fetchall()
        rows = []
        for row in raw_rows:
            clean = {}
            for i, col in enumerate(columnas):
                v = row[i]
                clean[col] = v  # SQLite ya devuelve tipos nativos Python
            rows.append(clean)

        cur.close()
        release_conn(conn)

        return jsonify({
            "ok":      True,
            "tabla":   nombre,
            "columns": columnas,
            "total":   total,
            "page":    page,
            "limit":   limit,
            "pages":   max(1, -(-total // limit)),
            "data":    rows,
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@tablas_bp.route('/api/tablas', methods=['GET'])
def list_tablas():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        result = []
        for t in TABLAS_PERMITIDAS:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                count = cur.fetchone()[0]
            except Exception:
                count = 0
            result.append({"tabla": t, "filas": count})
        cur.close()
        release_conn(conn)
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
#  REPORTE DE CALIDAD DE DATOS
#  Cada chequeo es (tabla, alias, etiqueta legible, condicion SQL).
#  Los de REPORT_CHECKS tienen match_id (se agrupan por partido).
#  Los de GLOBAL_CHECKS son conteos sueltos de toda la tabla.
# ═══════════════════════════════════════════════════════════════════════════════
REPORT_CHECKS = [
    # ── matches ──
    ('matches', 'sin_equipo_id', 'Equipos sin id (team_a_id/team_b_id)', 'team_a_id IS NULL OR team_b_id IS NULL'),
    ('matches', 'sin_ganador_id', 'Partido sin winner_id', 'winner_id IS NULL'),
    ('matches', 'sin_torneo', 'Torneo vacío', "tournament IS NULL OR tournament = ''"),
    ('matches', 'sin_fase', 'Fase vacía', "phase IS NULL OR phase = ''"),
    ('matches', 'sin_fecha', 'Fecha vacía', "match_date IS NULL OR match_date = ''"),
    ('matches', 'sin_patch', 'Patch vacío', "patch IS NULL OR patch = ''"),
    ('matches', 'ganador_invalido', 'Ganador no coincide con los equipos', 'winner_id IS NOT NULL AND winner_id != team_a_id AND winner_id != team_b_id'),
    ('matches', 'huerfano', 'Partido huérfano (sin datos de equipos)', 'team_a_id IS NULL AND team_b_id IS NULL'),
    # ── match_veto ──
    ('match_veto', 'equipo_sin_id', 'Veto de un equipo sin team_id', "team IN ('a','b') AND team_id IS NULL"),
    ('match_veto', 'mapa_vacio', 'Veto sin nombre de mapa', "map_name IS NULL OR map_name = ''"),
    ('match_veto', 'accion_vacia', 'Veto sin acción', "action IS NULL OR action = ''"),
    ('match_veto', 'orden_vacio', 'Veto sin orden', 'veto_order IS NULL OR veto_order = 0'),
    # ── maps ──
    ('maps', 'picker_desconocido', 'Mapa sin picker (unknown)', "picker = 'unknown'"),
    ('maps', 'mapa_sin_nombre', 'Mapa sin nombre', "map_name IS NULL OR map_name = '' OR map_name = 'Unknown'"),
    ('maps', 'lado_sin_elegir', 'Mapa elegido sin lado (side_chosen)', "picker IN ('a','b') AND (side_chosen IS NULL OR side_chosen = '')"),
    ('maps', 'picker_sin_id', 'Picker sin team_id', "picker IN ('a','b') AND picker_id IS NULL"),
    ('maps', 'inicio_lado_vacio', 'side_top_start vacío', "side_top_start IS NULL OR side_top_start = ''"),
    ('maps', 'duracion_vacia', 'Duración vacía', "duration IS NULL OR duration = ''"),
    # NOTA: un marcador/valor en 0 NO es un error (puede ser legítimo: pocas rondas, sin clutch, etc.)
    # ── rounds ──
    ('rounds', 'ganador_vacio', 'Ronda sin ganador', 'winner_id IS NULL'),
    ('rounds', 'tipo_vacio', 'Ronda sin tipo de resultado', "result_type IS NULL OR result_type = ''"),
    ('rounds', 'lado_vacio', 'Ronda sin bando ganador', "winning_side IS NULL OR winning_side = ''"),
    # NOTA: category_top/category_bot vacíos NO se marcan como error:
    # hay torneos (ej. China) que no traen archivos de economía, así que es esperado.
    # ── player_stats ──
    ('player_stats', 'jugador_sin_id', 'Jugador sin player_id', 'player_id IS NULL'),
    ('player_stats', 'equipo_sin_id', 'Jugador sin team_id', 'team_id IS NULL'),
    ('player_stats', 'agente_vacio', 'Jugador sin agente', "agent IS NULL OR agent = ''"),
    ('player_stats', 'lado_invalido', 'Lado inválido (no attack/defense)', "side IS NULL OR side = '' OR side NOT IN ('attack','defense')"),
    ('player_stats', 'stats_nulos', 'KAST/ADR/HS nulos', 'kast IS NULL OR adr IS NULL OR hs_percent IS NULL'),
    # NOTA: rating o ACS en 0 NO es un error.
    # ── economy_summary ──
    ('economy_summary', 'equipo_sin_id', 'Economía sin team_id', 'team_id IS NULL'),
    # ── duels ──
    ('duels', 'jugador_a_sin_id', 'Duelo: jugador A sin id', 'player_a_id IS NULL'),
    ('duels', 'jugador_b_sin_id', 'Duelo: jugador B sin id', 'player_b_id IS NULL'),
    ('duels', 'kills_nulos', 'Duelo sin kills', 'kills_a IS NULL OR kills_b IS NULL'),
    # ── multikills_clutches ──
    ('multikills_clutches', 'jugador_sin_id', 'Multikill sin player_id', 'player_id IS NULL'),
    ('multikills_clutches', 'agente_vacio', 'Multikill sin agente', "agent IS NULL OR agent = ''"),
    # NOTA: no haber hecho 2k/clutch/plant/defuse NO es un error.
]

GLOBAL_CHECKS = [
    ('players', 'sin_team_id', 'Jugador sin team_id', 'team_id IS NULL'),
    ('players', 'sin_real_name', 'Jugador sin nombre real', "real_name IS NULL OR real_name = ''"),
    ('teams', 'sin_region', 'Equipo sin región', "region IS NULL OR region = ''"),
    ('teams', 'sin_url', 'Equipo sin URL', "url IS NULL OR url = ''"),
]


def build_reporte(conn):
    cur = conn.cursor()

    incidencias = defaultdict(list)          # match_id -> [ {tabla, campo, etiqueta, filas} ]
    total_campo = defaultdict(int)           # "tabla.campo" -> filas
    afectados_por_tabla = defaultdict(set)   # tabla -> {match_id}

    # ── Chequeos agrupados por partido (una sola query por tabla) ──
    tablas_orden = []
    for t, *_ in REPORT_CHECKS:
        if t not in tablas_orden:
            tablas_orden.append(t)

    for tabla in tablas_orden:
        checks = [(a, l, c) for (tt, a, l, c) in REPORT_CHECKS if tt == tabla]
        sums = ", ".join([f"SUM(CASE WHEN {c} THEN 1 ELSE 0 END) AS c{i}" for i, (a, l, c) in enumerate(checks)])
        # rounds no tiene match_id: se obtiene vía maps
        if tabla == 'rounds':
            from_clause, mid_expr = 'rounds JOIN maps ON rounds.map_id = maps.map_id', 'maps.match_id'
        else:
            from_clause, mid_expr = tabla, 'match_id'
        cur.execute(f"SELECT {mid_expr} AS mid, {sums} FROM {from_clause} GROUP BY {mid_expr}")
        for row in cur.fetchall():
            mid = row[0]
            for i, (alias, etiqueta, cond) in enumerate(checks):
                cnt = row[i + 1] or 0
                if cnt:
                    incidencias[mid].append({'tabla': tabla, 'campo': alias, 'etiqueta': etiqueta, 'filas': cnt})
                    total_campo[f'{tabla}.{alias}'] += cnt
                    afectados_por_tabla[tabla].add(mid)

    # ── Info de partidos ──
    cur.execute("""SELECT m.match_id, m.tournament, ta.team_name, tb.team_name
                   FROM matches m
                   LEFT JOIN teams ta ON ta.team_id = m.team_a_id
                   LEFT JOIN teams tb ON tb.team_id = m.team_b_id""")
    partidos = {r[0]: {'tournament': r[1], 'team_a': r[2], 'team_b': r[3]} for r in cur.fetchall()}
    cur.execute('SELECT COUNT(*) FROM matches')
    total_partidos = cur.fetchone()[0]

    # ── Conteos globales ──
    globales = []
    for tabla, alias, etiqueta, cond in GLOBAL_CHECKS:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {tabla} WHERE {cond}")
            n = cur.fetchone()[0]
        except Exception:
            n = 0
        if n:
            globales.append({'tabla': tabla, 'campo': alias, 'etiqueta': etiqueta, 'filas': n})

    # ── Conteo de filas por tabla ──
    tablas_conteo = {}
    for t in TABLAS_PERMITIDAS:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            tablas_conteo[t] = cur.fetchone()[0]
        except Exception:
            tablas_conteo[t] = 0

    cur.close()

    # ── Agrupar por partido + texto ──
    por_partido = []
    lineas = []
    for mid in sorted(incidencias):
        info = partidos.get(mid, {})
        incs = incidencias[mid]
        por_partido.append({
            'match_id': mid,
            'url': f'https://www.vlr.gg/{mid}',
            'tournament': info.get('tournament'),
            'team_a': info.get('team_a'),
            'team_b': info.get('team_b'),
            'incidencias': incs,
        })
        lineas.append(f'vlr.gg/{mid}')
        for inc in incs:
            lineas.append(f"  - [{inc['tabla']}] {inc['etiqueta']} ({inc['filas']})")

    # ── Resumen por tipo de chequeo (magnitud) ──
    agregado = defaultdict(lambda: {'filas': 0, 'partidos': 0})
    for p in por_partido:
        for inc in p['incidencias']:
            key = (inc['tabla'], inc['campo'], inc['etiqueta'])
            agregado[key]['filas'] += inc['filas']
            agregado[key]['partidos'] += 1
    por_chequeo = [
        {'tabla': k[0], 'campo': k[1], 'etiqueta': k[2], 'filas': v['filas'], 'partidos': v['partidos']}
        for k, v in agregado.items()
    ]
    por_chequeo.sort(key=lambda x: -x['filas'])

    return {
        'total_partidos': total_partidos,
        'partidos_con_problemas': len(por_partido),
        'total_incidencias': sum(total_campo.values()) + sum(g['filas'] for g in globales),
        'tablas': tablas_conteo,
        'global': globales,
        'por_chequeo': por_chequeo,
        'por_partido': por_partido,
        'texto': "\n".join(lineas),
    }


@tablas_bp.route('/api/tablas/reporte', methods=['GET'])
def reporte_tablas():
    try:
        conn = get_conn()
        try:
            data = build_reporte(conn)
        finally:
            release_conn(conn)
        return jsonify({'ok': True, 'reporte': data})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500