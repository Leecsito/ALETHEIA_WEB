"""
ALETHEIA — Exportar Blueprint
Rutas: /api/export/tables, /api/export/csv/<nombre>, /api/export/excel/<nombre>,
       /api/export/json/<nombre>, /api/export/zip
Permite descargar las tablas de la BD en diferentes formatos.
"""

import io
import zipfile
import pandas as pd
from flask import Blueprint, jsonify, Response, send_file
try:
    from backend.conexion import get_conn, release_conn
except ImportError:
    from conexion import get_conn, release_conn

exportar_bp = Blueprint('exportar', __name__)

TABLAS_PERMITIDAS = [
    'matches', 'match_veto', 'maps', 'rounds',
    'player_stats', 'economy_summary', 'duels', 'multikills_clutches',
    'teams', 'players'
]


def fetch_table_df(nombre):
    """Obtiene los datos de una tabla como DataFrame de pandas."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {nombre}")
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return pd.DataFrame(rows)
    finally:
        release_conn(conn)


@exportar_bp.route('/api/export/tables', methods=['GET'])
def list_tables_info():
    """Retorna información de conteo y columnas de todas las tablas."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        data = []
        for t in TABLAS_PERMITIDAS:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                count = cur.fetchone()[0]
                cur.execute(f"PRAGMA table_info({t})")
                cols = [row[1] for row in cur.fetchall()]
            except Exception:
                count = 0
                cols = []
            data.append({"tabla": t, "filas": count, "columnas": cols})
        cur.close()
        release_conn(conn)
        return jsonify({"ok": True, "tablas": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@exportar_bp.route('/api/export/csv/<nombre>', methods=['GET'])
def export_csv(nombre):
    if nombre not in TABLAS_PERMITIDAS:
        return jsonify({"ok": False, "error": f"Tabla '{nombre}' no permitida."}), 403
    try:
        df = fetch_table_df(nombre)
        csv_data = df.to_csv(index=False)
        return Response(
            csv_data,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={nombre}.csv"}
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@exportar_bp.route('/api/export/excel/<nombre>', methods=['GET'])
def export_excel(nombre):
    if nombre not in TABLAS_PERMITIDAS:
        return jsonify({"ok": False, "error": f"Tabla '{nombre}' no permitida."}), 403
    try:
        df = fetch_table_df(nombre)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=nombre, index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"{nombre}.xlsx"
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@exportar_bp.route('/api/export/json/<nombre>', methods=['GET'])
def export_json(nombre):
    if nombre not in TABLAS_PERMITIDAS:
        return jsonify({"ok": False, "error": f"Tabla '{nombre}' no permitida."}), 403
    try:
        df = fetch_table_df(nombre)
        json_data = df.to_json(orient='records', indent=2)
        return Response(
            json_data,
            mimetype="application/json",
            headers={"Content-Disposition": f"attachment; filename={nombre}.json"}
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@exportar_bp.route('/api/export/zip', methods=['GET'])
def export_zip():
    try:
        memory_file = io.BytesIO()
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            for t in TABLAS_PERMITIDAS:
                try:
                    df = fetch_table_df(t)
                    csv_bytes = df.to_csv(index=False).encode('utf-8')
                    zf.writestr(f"{t}.csv", csv_bytes)
                except Exception:
                    continue
        memory_file.seek(0)
        return send_file(
            memory_file,
            mimetype="application/zip",
            as_attachment=True,
            download_name="aletheia_tablas_csv.zip"
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
