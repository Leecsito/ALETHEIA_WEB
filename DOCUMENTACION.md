# DOCUMENTACIÓN TÉCNICA Y ARQUITECTURA DEL PROYECTO ALETHEIA

> **Nota para Asistentes de IA y Desarrolladores:**  
> Este documento contiene la arquitectura completa, esquema de base de datos, catálogo de APIs, estructura de archivos y reglas de negocio del proyecto **ALETHEIA**. Consúltalo como fuente de verdad para realizar modificaciones, agregar rutas o ajustar lógica sin necesidad de escanear repetidamente todo el código fuente del proyecto.

---

## 1. Visión General del Proyecto

**ALETHEIA** es una plataforma web integral de analítica, procesamiento ETL, visualización y predicción de partidos para deportes electrónicos (específicamente Valorant VCT).

### Stack Tecnológico:
- **Backend:** Python 3 (Flask, Gunicorn, Pandas, NumPy, OpenPyXL, libSQL client / SQLite3).
- **Base de Datos:** Turso (libSQL en la nube) con fallback a SQLite3 local (`aletheia.db`).
- **Frontend:** Vanilla HTML5, Vanilla CSS3 (Variables CSS, Estética Cyberpunk/Dark Mode), JavaScript ES6+ (Fetch API, origen dinámico `window.location.origin`).
- **Despliegue:** Render / Gunicorn (`render.yaml` y `requirements.txt`).

---

## 2. Estructura de Directorios

```
ALETHEIA/
├── backend/                  # Núcleo del servidor Flask y gestión de conexión
│   ├── __init__.py
│   ├── app.py                # Punto de entrada de Flask, registro de Blueprints y rutas de páginas HTML
│   ├── conexion.py           # Gestión centralizada de la base de datos (Turso / SQLite)
│   ├── aletheia.db           # Base de datos SQLite local (fallback)
│   └── aletheia_2025.db      # Base de datos SQLite de respaldo
├── inicio/                   # Componente ETL (Carga de Excel e inicialización)
│   ├── __init__.py
│   ├── inicio.py             # Blueprint Flask (/api/init-db, /api/etl, /api/status)
│   ├── index.html            # UI de carga masiva de Excel
│   ├── style.css
│   └── script.js
├── tablas/                   # Componente Explorador de Tablas (Raw Data)
│   ├── __init__.py
│   ├── tablas.py             # Blueprint Flask (/api/tablas, /api/tabla/<nombre>)
│   ├── index.html
│   ├── style.css
│   └── script.js
├── visualizar/               # Componente de Visualización y Métricas VCT
│   ├── __init__.py
│   ├── visualizar.py         # Blueprint Flask (partidos, jugadores, mapas, rondas, economía, agentes)
│   ├── index.html
│   ├── style.css
│   └── script.js
├── predecir/                 # Componente Predictor Monte Carlo Clásico (v2)
│   ├── __init__.py
│   ├── predecir.py           # Blueprint Flask (Simulación Monte Carlo con 5 señales)
│   ├── index.html
│   ├── style.css
│   └── script.js
├── aletheia/                 # Componente Predictor Avanzado Monte Carlo (v3)
│   ├── __init__.py
│   ├── aletheia.py           # Blueprint Flask (Simulación Halftime, Overtime, Operator, Star Player)
│   ├── index.html
│   ├── style.css
│   └── script.js
├── exportar/                 # Componente de Exportación de Datos
│   ├── __init__.py
│   ├── exportar.py           # Blueprint Flask (Descarga CSV, Excel, JSON y paquete ZIP)
│   ├── index.html
│   ├── style.css
│   └── script.js
├── multimedia/               # Archivos multimedia / imágenes
├── wsgi.py                   # Punto de entrada WSGI para Gunicorn
├── render.yaml               # Configuración de despliegue en Render
├── requirements.txt          # Dependencias de Python
└── DOCUMENTACION.md          # Este documento de arquitectura
```

---

## 3. Base de Datos y Capa de Conexión (`backend/conexion.py`)

La conexión a la base de datos se gestiona de forma centralizada a través de las funciones `get_conn()` y `release_conn(conn)` definidas en `backend/conexion.py`.

### Variables de Entorno Soporta:
- `TURSO_DATABASE_URL`: URL remota de la base de datos libSQL (por defecto: `libsql://aletheia-leecsito.aws-us-east-1.turso.io`).
- `TURSO_AUTH_TOKEN`: Token JWT de autenticación para Turso.
- `DATABASE_PATH`: Ruta al archivo SQLite local de respaldo (por defecto: `backend/aletheia.db`).

### Esquema de las 10 Tablas de la Base de Datos:

1. **`matches`**: Partidos jugados.
   - `match_id` (INTEGER, PK), `tournament` (TEXT), `phase` (TEXT), `match_date` (TEXT), `team_a` (TEXT), `team_b` (TEXT), `score_a` (INTEGER), `score_b` (INTEGER), `winner` (TEXT), `patch` (TEXT).
2. **`match_veto`**: Picks y bans de mapas por partido.
   - `veto_id` (INTEGER, PK AUTO), `match_id` (FK matches), `action` (TEXT: pick/ban/decider), `team` (TEXT: a/b), `map_name` (TEXT), `veto_order` (INTEGER).
3. **`maps`**: Mapas disputados en los partidos.
   - `map_id` (TEXT, PK), `match_id` (FK matches), `map_name` (TEXT), `map_number` (INTEGER), `picker` (TEXT), `side_chosen` (TEXT), `side_top_start` (TEXT), `score_a_attack` (INTEGER), `score_a_defense` (INTEGER), `score_b_attack` (INTEGER), `score_b_defense` (INTEGER), `duration` (TEXT).
4. **`rounds`**: Detalle ronda a ronda.
   - `map_id` (TEXT, FK maps), `round_num` (INTEGER), `winner` (TEXT), `result_type` (TEXT), `winning_side` (TEXT), `team_top` (TEXT), `bank_top` (INTEGER), `spend_top` (INTEGER), `category_top` (TEXT), `team_bot` (TEXT), `bank_bot` (INTEGER), `spend_bot` (INTEGER), `category_bot` (TEXT). PK: `(map_id, round_num)`.
5. **`player_stats`**: Rendimiento individual por mapa y lado.
   - `stat_id` (INTEGER, PK AUTO), `match_id` (FK matches), `map_id` (FK maps), `player_name` (TEXT), `team_name` (TEXT), `side` (TEXT), `agent` (TEXT), `rating` (REAL), `acs` (INTEGER), `kills` (INTEGER), `deaths` (INTEGER), `assists` (INTEGER), `kast` (REAL), `adr` (REAL), `hs_percent` (REAL), `fk` (INTEGER), `fd` (INTEGER).
6. **`economy_summary`**: Resumen económico por equipo y mapa.
   - `econ_id` (INTEGER, PK AUTO), `match_id` (FK matches), `map_id` (FK maps), `team` (TEXT), `pistol_won` (INTEGER), `eco_played` (INTEGER), `eco_won` (INTEGER), `semi_eco_played` (INTEGER), `semi_eco_won` (INTEGER), `semi_buy_played` (INTEGER), `semi_buy_won` (INTEGER), `full_buy_played` (INTEGER), `full_buy_won` (INTEGER).
7. **`duels`**: Enfrentamientos y duelos 1v1 entre jugadores.
   - `duel_id` (INTEGER, PK AUTO), `match_id` (FK matches), `map_id` (FK maps), `duel_type` (TEXT), `player_a` (TEXT), `player_b` (TEXT), `kills_a` (INTEGER), `kills_b` (INTEGER).
8. **`multikills_clutches`**: Bajas múltiples y situaciones límite.
   - `mk_id` (INTEGER, PK AUTO), `match_id` (FK matches), `map_id` (FK maps), `player_name` (TEXT), `agent` (TEXT), `k2`..`k5` (INTEGER), `v1`..`v5` (INTEGER), `econ_rating` (INTEGER), `plants` (INTEGER), `defuses` (INTEGER).
9. **`teams`**: Información de equipos.
   - `team_id` (INTEGER, PK), `team_name` (TEXT), `region` (TEXT), `url` (TEXT).
10. **`players`**: Registro de jugadores.
    - `player_id` (INTEGER, PK AUTO), `nickname` (TEXT), `real_name` (TEXT), `team_id` (FK teams), `team_name` (TEXT).

---

## 4. Catálogo de Rutas API (Backend)

### 4.1. Módulo ETL / Inicio (`inicio_bp`)
- `POST /api/init-db`: Crea las tablas de la base de datos si no existen y ejecuta migraciones.
- `POST /api/etl`: Recibe archivos Excel (`vct_partidos`, `vlr_mapas`, `vlr_rondas`, etc.) y procesa la inserción masiva de datos en la BD.
- `GET /api/status`: Retorna el conteo de filas de cada una de las 10 tablas.

### 4.2. Módulo Tablas (`tablas_bp`)
- `GET /api/tablas`: Lista el nombre de las tablas permitidas y su total de filas.
- `GET /api/tabla/<nombre>`: Retorna los datos paginados de la tabla solicitada (acepta query params `page`, `limit`, `search`).

### 4.3. Módulo Visualizar (`visualizar_bp`)
- `GET /api/matches`: Métricas agregadas de partidos jugados.
- `GET /api/player-stats`: Estadísticas promedio de jugadores (rating, acs, kills, deaths, adr, kast, fk, fd).
- `GET /api/maps-stats`: Métricas de mapas (veces jugado, selecciones por lado, promedio de rondas).
- `GET /api/rounds-stats`: Distribución de tipos de victoria por ronda y bando ganador.
- `GET /api/economy`: Win rate por categoría económica (Pistol, Eco, Semi-Eco, Semi-Buy, Full-Buy).
- `GET /api/agents`: Estadísticas de selección e impacto por agente.

### 4.4. Módulo Predecir Clásico v2 (`predecir_bp`)
- `GET /api/equipos-pred`: Lista los equipos disponibles en la base de datos con su número de mapas jugados y rating promedio.
- `POST /api/predecir`: Ejecuta la simulación Monte Carlo (por defecto 10,000 iteraciones) utilizando 5 señales de rendimiento (WR histórico, habilidad, economía, clutch, H2H/veto) y decaimiento exponencial temporal.

### 4.5. Módulo Predictor Avanzado v3 (`aletheia_bp`)
- `GET /api/aletheia/equipos`: Lista de equipos para el módulo avanzado.
- `POST /api/aletheia/predecir`: Simulación avanzada Monte Carlo que incluye:
  - Simulación de Halftime con máquina de estados de economía real de Valorant.
  - Perfil y tasa de victoria en Overtime / capacidad de cierre.
  - Análisis de impacto del arma Operator por mapa.
  - Evaluación del Jugador Estrella y riesgo de contra-estrategia.
- `POST /api/aletheia/recalcular_mapa`: Re-simula un solo mapa especificando overrides de agentes seleccionados.
- `POST /api/aletheia/recalcular_serie`: Recalcula la probabilidad global de serie (Bo1, Bo3, Bo5) con probabilidades de mapa actualizadas.

### 4.6. Módulo Exportar (`exportar_bp`)
- `GET /api/export/tables`: Retorna metadatos de las 10 tablas (filas y lista de columnas).
- `GET /api/export/csv/<nombre>`: Descarga la tabla seleccionada en formato `.csv`.
- `GET /api/export/excel/<nombre>`: Descarga la tabla seleccionada en formato `.xlsx`.
- `GET /api/export/json/<nombre>`: Descarga la tabla seleccionada en formato `.json`.
- `GET /api/export/zip`: Genera y descarga un archivo `.zip` comprimido con todos los `.csv` de la base de datos.

---

## 5. Estructura y Reglas del Frontend

1. **Rutas Estáticas de Navegación (`backend/app.py`):**
   Las subcarpetas registradas en `FRONTEND_FOLDERS = ['inicio', 'tablas', 'visualizar', 'predecir', 'aletheia', 'exportar']` se sirven automáticamente en la raíz HTTP:
   - `/inicio/` o `/inicio/index.html`
   - `/tablas/` o `/tablas/index.html`
   - `/visualizar/` o `/visualizar/index.html`
   - `/predecir/` o `/predecir/index.html`
   - `/aletheia/` o `/aletheia/index.html`
   - `/exportar/` o `/exportar/index.html`

2. **Configuración de Host API Dinámico:**
   En todos los archivos JavaScript del frontend (`script.js`), la variable `API` está configurada como:
   ```javascript
   const API = `${window.location.origin}/api`;
   ```
   Esto garantiza que las peticiones se dirijan correctamente al mismo host tanto en entornos locales (`http://localhost:5000/api`) como en producción en Render (`https://tu-app.onrender.com/api`).

3. **Sistema de Diseño Visual:**
   - Estética oscura / Cyberpunk (`--bg-color: #0b0e14`, paneles con fondo translúcido y bordes luminosos).
   - Tipografías principales desde Google Fonts:
     - Titulares y Badges: `'Bebas Neue', sans-serif`
     - Textos, Tablas y Métricas: `'DM Mono', monospace`
   - Navegación superior consistente en todos los componentes mediante la clase `.btn-nav`.

---

## 6. Configuración de Despliegue (Render & Gunicorn)

- **Entrypoint:** `wsgi.py` carga la instancia `app` de Flask desde `backend/app.py`.
- **Servidor WSGI:** `gunicorn wsgi:app`
- **Comando de Build:** `pip install -r requirements.txt`
- **Archivo de Configuración:** `render.yaml` declara el servicio web Python con las variables de entorno necesarias para la conexión remota a Turso.

---

## 7. Instrucciones para la Asistencia de IA

Al recibir una nueva tarea o solicitud de cambio:
1. **Revisa este documento** para ubicar el archivo, blueprint o tabla involucrada.
2. **Realiza modificaciones quirúrgicas** enfocadas únicamente en los archivos relevantes.
3. **Mantén las firmas de API**, la estructura dinámica de `window.location.origin` y la compatibilidad con el esquema de base de datos descrito arriba.
