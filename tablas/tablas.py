"""
ALETHEIA — Tablas API Blueprint
Ruta: /api/tabla/<nombre>
Permite ver el contenido raw de cualquier tabla de la BD con paginacion.
"""

from flask import Blueprint, request, jsonify
try:
    from backend.conexion import get_conn, release_conn
except ImportError:
    from conexion import get_conn, release_conn

tablas_bp = Blueprint('tablas', __name__)

TABLAS_PERMITIDAS = [
    'matches', 'match_veto', 'maps', 'rounds',
    'player_stats', 'economy_summary', 'duels', 'multikills_clutches',
    'teams', 'players'
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