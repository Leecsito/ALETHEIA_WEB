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
├── aletheia/                 # Componente Predictor Avanzado (proxy hacia ALETHEIA_PREDICT)
│   ├── __init__.py
│   ├── aletheia.py           # Blueprint Flask — proxy HTTP a ALETHEIA_PREDICT_URL (equipos, mapas, predecir)
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

### Esquema de las Tablas de la Base de Datos

**10 tablas del ETL** (creadas/gestionadas por ALETHEIA):

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
   - `team_id` (INTEGER, PK), `team_name` (TEXT), `region` (TEXT), `url` (TEXT), `tag` (TEXT), `country` (TEXT).
10. **`players`**: Registro de jugadores.
    - `player_id` (INTEGER, PK AUTO), `nickname` (TEXT), `real_name` (TEXT), `team_id` (FK teams), `team_name` (TEXT), `country` (TEXT).

**2 tablas del servicio ALETHEIA_PREDICT** (ALETHEIA **solo las consulta/muestra**; las crea y escribe el servicio externo). `match_id` es el id del partido de **vlr.gg** (ej. `753455`) y es el mismo para todo el partido.

11. **`predicciones_mapa`**: predicción cacheada por mapa y lado.
    - `id`, `match_id`, `equipo_a`, `equipo_b`, `map_name`, `lado_inicial_a`, `prob_victoria_a`, `prob_victoria_b`, `prob_overtime`, `n_sim`, `con_datos`, `modelo_version`, `created_at`, `updated_at`.
    - `UNIQUE(match_id, equipo_a, equipo_b, map_name, lado_inicial_a)`.
12. **`predicciones_serie`**: predicción cacheada de la serie.
    - `id`, `match_id`, `equipo_a`, `equipo_b`, `formato`, `mapas_json`, `prob_serie_a`, `prob_serie_b`, `n_sim`, `modelo_version`, `created_at`.

---

## 4. Catálogo de Rutas API (Backend)

### 4.1. Módulo ETL / Inicio (`inicio_bp`)
- `POST /api/init-db`: Crea las tablas de la base de datos si no existen y ejecuta migraciones.
- `POST /api/etl`: Recibe archivos Excel (`vct_partidos`, `vlr_mapas`, `vlr_rondas`, etc.) y procesa la inserción masiva. Responde `202` con un `job_id`; el ETL corre en segundo plano.
- `POST /api/etl-batch`: Recibe múltiples archivos con su ruta relativa (subida de carpetas por torneo) y ejecuta el ETL de cada torneo. Responde `202` con `job_id`.
- `GET /api/etl-status/<job_id>`: Consulta el estado de un ETL asíncrono (`status`, `step`, `progress`, `inserted`, `error`).
- `GET /api/status`: Retorna el conteo de filas de cada una de las 10 tablas.

> **Reimportación idempotente:** al cargar un torneo, el ETL **borra y reinserta** los datos de sus `match_id` (hijos primero por FK: `rounds`, `player_stats`, `economy_summary`, `duels`, `multikills_clutches`, `match_veto`, `maps`, `matches`). Esto permite **re-subir un torneo para corregir datos sin duplicar filas**. Los jugadores se actualizan con `UPSERT` (rellena `team_id`/`team_name` si faltaban).

### 4.2. Módulo Tablas (`tablas_bp`)
- `GET /api/tablas`: Lista el nombre de las tablas permitidas y su total de filas. La allowlist (`TABLAS_PERMITIDAS`) incluye las 10 tablas del ETL **más** `predicciones_mapa` y `predicciones_serie` (estas dos las escribe el servicio ALETHEIA_PREDICT; aquí solo se consultan/muestran).
- `GET /api/tabla/<nombre>`: Retorna los datos paginados de la tabla solicitada (acepta query params `page`, `limit`, `search`).
- `GET /api/tablas/reporte`: Genera un reporte de calidad de datos: incidencias por partido (agrupadas por `match_id`, con URL `vlr.gg/<match_id>`), problemas globales por tabla y un texto plano listo para copiar.

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

### 4.5. Módulo Predictor Avanzado (`aletheia_bp`)

Este módulo **no simula partidos**: es un **proxy HTTP** hacia el servicio externo
**ALETHEIA_PREDICT** (repositorio independiente), cuyo motor es **Glicko-2 + regresión
logística sobre `rating_diff` (P(mapa)) + Monte Carlo re-escalado** (marcador/economía)
para estimar overtime. La URL base se lee de la variable de entorno
`ALETHEIA_PREDICT_URL` (por defecto `http://localhost:8000`).

Endpoints expuestos por ALETHEIA (todos reenvían al servicio externo):

- `GET /api/aletheia/equipos` → proxy de `GET {BASE}/api/equipos`.
  Adapta la respuesta para el grid del frontend:
  `{"ok": true, "teams": [{"name", "abbrev", "maps_played": 0, "avg_rating": 0}]}`.
  Como el servicio solo devuelve nombres, `abbrev = name` (decisión de diseño) y
  las métricas `maps_played`/`avg_rating` quedan en 0 porque el servicio no las aporta.
- `GET /api/aletheia/mapas` → proxy de `GET {BASE}/api/mapas`.
  Devuelve `{"ok": true, "mapas": [...]}` (13 mapas, incluye `Summit`).
- `POST /api/aletheia/predecir` → proxy de `POST {BASE}/api/predecir`.
  Reenvía el body tal cual y devuelve la respuesta del servicio sin transformar.

**Body de `/api/aletheia/predecir`:**
```json
{
  "equipo_a": "Team Liquid",
  "equipo_b": "Paper Rex",
  "mapas": [
    {"map_name": "Split", "lado_inicial_a": "attack"},
    {"map_name": "Ascent", "lado_inicial_a": "defense"}
  ],
  "n_sim": 10000
}
```
Reglas: `lado_inicial_a` se define **por mapa** (`"attack"` | `"defense"`); el
formato se **infiere por cantidad** (1→bo1, 2-3→bo3, 4-5→bo5); `n_sim` se normaliza
a `[1000, 50000]`.

**Respuesta del servicio (proxy sin cambios):**
```json
{
  "ok": true,
  "equipo_a": "Team Liquid",
  "equipo_b": "Paper Rex",
  "n_sim": 10000,
  "formato": "bo3",
  "mapas_para_ganar": 2,
  "mapas": [
    {"map_name": "Split", "lado_inicial_a": "attack",
     "prob_victoria_a": 0.4412, "prob_victoria_b": 0.5588, "prob_overtime": 0.164}
  ],
  "prob_serie_a": 0.6333,
  "prob_serie_b": 0.3667
}
```

Manejo de errores: timeout de 120 s (504 si expira) y 502 `{"ok": false, "error": "..."}`
si el servicio no responde. Este módulo **no importa** `numpy`, `pandas` ni
`backend.conexion`.

#### Ciclo precomputar → asociar → leer → comparar

`match_id` es el id del partido de **vlr.gg** (ej. `753455`) y es el mismo para
todo el partido. La DB Turso es compartida con el servicio (ALETHEIA no crea
estas tablas). Endpoints adicionales del proxy:

- `GET /api/aletheia/modelo_version` → proxy de `GET {BASE}/api/modelo_version`.
  Devuelve `{"ok": true, "modelo_version": "<hash>", "fecha": "<iso>"}`.
- `POST /api/aletheia/precalcular` → proxy de `POST {BASE}/api/precalcular`.
  Body: `{"equipo_a", "equipo_b", "n_sim": 50000, "match_id": 753455, "mapas": [...]?}`.
  Calcula los 13 mapas × 2 lados (26 filas) y hace UPSERT en `predicciones_mapa`
  con ese `match_id`. Responde `{"ok": true, ..., "total": 26, "computados": X, "desde_cache": Y, "tiempo_s": Z}`.
- `POST /api/aletheia/asociar` → proxy de `POST {BASE}/api/asociar`.
  Body: `{"equipo_a", "equipo_b", "match_id": 753455, "desde_match_id": 0}`.
  Reasigna el `match_id` de predicciones ya calculadas. Responde
  `{"ok": true, "filas_actualizadas": N, "match_id": 753455}`.
- `GET /api/aletheia/prediccion?match_id=753455&map_name=Split&lado_inicial_a=attack`
  (o `?equipo_a=&equipo_b=`) → proxy de `GET {BASE}/api/prediccion`. Lee la fila
  cacheada (instantáneo). Responde `{"ok": true, "prediccion": {...}, "modelo_version": "<hash>", "vigente": true}`
  o `404 {"ok": false, "error": "Sin predicción cacheada."}`.
- `GET /api/aletheia/predicciones?match_id=753455` (o `?equipo_a=&equipo_b=`) →
  proxy de `GET {BASE}/api/predicciones`.
- `GET /api/aletheia/comparacion?match_id=753455&limite=100` (o `?equipo_a=&equipo_b=`) →
  proxy de `GET {BASE}/api/comparacion`. Compara lo predicho (`predicciones_mapa`)
  con el resultado real (`matches` + `maps`) del mismo `match_id`:
  ```json
  {
    "ok": true,
    "resumen": {"n": 5, "accuracy": 0.8, "brier": 0.13, "log_loss": 0.42,
                "favoritos_ok": 4, "upsets": 1, "inciertos": 0},
    "detalle": [{"equipo_a": "Team Liquid", "equipo_b": "Paper Rex", "map_name": "Split",
                 "lado_inicial_a": "attack", "prob_victoria_a": 0.4412,
                 "prob_victoria_b": 0.5588, "prob_overtime": 0.164,
                 "gano_a_real": 1, "resultado": "acierto", "tipo": "favorito_gano",
                 "match_id": 753455, "created_at": "..."}]
  }
  ```
  Clasificación: favorito = A si `p_a >= 0.5`; `max(p_a,p_b) < 0.55` → `incierto`;
  si ganó el favorito → `favorito_gano`; si no → `upset`.

**Flujo del ciclo (frontend → `PREDICT_DIRECTO` ngrok):**
1. **PREPARAR PARTIDO**: `POST {PREDICT_DIRECTO}/api/precalcular` con el id de
   vlr.gg (llamada larga, directa al servicio para no chocar con el timeout de
   gunicorn). Se guarda el `modelo_version` con el que se preparó.
2. **SELECCIÓN EN VIVO**: al cambiar mapa/lado, `GET /api/prediccion` lee la fila
   cacheada (sin simular).
3. **ASOCIAR ID**: si se preparó con id `0` o equivocado, `POST /api/asociar`
   reasigna el `match_id` (usando `desde_match_id`).
4. **COMPARACIÓN**: `GET /api/comparacion` contrasta lo predicho con el resultado
   real del mismo `match_id`.
5. Si `/api/modelo_version` cambia respecto al guardado, la web marca **RE-PREPARAR**.

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

3. **Servicio ALETHEIA_PREDICT (ngrok) y ciclo de partido (`aletheia/script.js`):**
   - El predictor externo corre en el PC del autor y se expone con ngrok en la
     constante `PREDICT_DIRECTO` (`https://snugly-encore-sweep.ngrok-free.dev`).
     Todas las llamadas a ese host llevan el header `ngrok-skip-browser-warning: 1`.
   - Las corridas **largas** (`/api/precalcular` y `/api/predecir`) se piden
     **directo** a `PREDICT_DIRECTO` (no por el proxy de la web) para no chocar con
     el timeout de gunicorn/Render. Equipos y mapas sí van por el proxy (son rápidos).
   - **Campo de ID de partido:** input donde se pega la URL de vlr.gg o el número; se
     parsea el primer grupo de dígitos (`https://www.vlr.gg/753455/...` → `753455`) y
     se muestra como `PARTIDO #753455`. Puede quedar vacío (`0`) y asociarse luego.
   - **ARMADOR DE SERIE (BO1/BO3/BO5):** el usuario elige el formato y se muestran
     N slots (1/3/5) que se llenan tocando los mapas en orden (el último es el
     DECIDER) y define ATK/DEF por mapa. Si el partido ya está en caché, cada slot
     muestra la predicción inline (P(A)/P(B)/OT) sin re-preparar; `SIMULAR PARTIDO`
     solo se habilita con todos los slots llenos.
   - **DETECCIÓN DE CACHÉ:** al elegir equipos o `match_id` se leen las filas
     existentes (`/api/predicciones`); un badge indica "ya predicho (N filas)" y el
     botón pasa a "RE-PREPARAR", de modo que un partido ya predecido se usa sin
     re-ejecutar el servicio.
   - **PREPARAR PARTIDO:** dispara `POST /api/precalcular` con
     `{equipo_a, equipo_b, n_sim, match_id}` (avisa "no cierres la pestaña" y muestra
     el tiempo transcurrido). Al **no** enviar `mapas`, el servicio calcula los **13
     mapas × 2 lados = 26 filas**; el usuario puede preparar sin elegir ningún mapa.
     Al terminar guarda el `modelo_version`.
   - **SELECCIÓN EN VIVO:** rejilla visual de los 13 mapas (mismo estilo de tiles que el
     armador de serie) + toggle ATK/DEF de A. Al entrar o cambiar de lado se leen todas
     las filas cacheadas con `GET /api/predicciones?match_id=..` y cada tile muestra la
     P(A) y el OT del lado elegido; al tocar un mapa se pide el detalle exacto con
     `GET /api/prediccion?match_id=..&map_name=..&lado_inicial_a=..` (prob_victoria_a,
     prob_victoria_b, prob_overtime, fuente y `vigente`). Si no hay caché: "Aún no
     precomputado"; si `vigente=false`: "vuelve a preparar".
   - **ASOCIAR ID:** `POST /api/asociar` con
     `{equipo_a, equipo_b, match_id, desde_match_id}` para reasignar el id; refresca la vista.
   - **PESTAÑA COMPARACIÓN:** `GET /api/comparacion?match_id=..` muestra tarjetas resumen
     (`n`, accuracy, brier, log-loss, favoritos_ok, upsets, inciertos) y una tabla de
     detalle coloreada (verde = favorito ganó, rojo = upset, ámbar = incierto). Si el
     partido aún no está en la DB: "sin resultado real todavía".
   - Se muestra el `modelo_version`; si el del servicio cambia, la web marca **RE-PREPARAR**.

4. **Sistema de Diseño Visual:**
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
- **Variables de Entorno:**
  - `TURSO_DATABASE_URL` / `TURSO_AUTH_TOKEN`: conexión a la base de datos Turso.
  - `ALETHEIA_PREDICT_URL`: URL base del servicio externo **ALETHEIA_PREDICT**
    (motor de predicción). En local se define en el archivo `.env`
    (`http://localhost:8000`); en Render se declara en `render.yaml`.
    El módulo `aletheia/aletheia.py` actúa como proxy hacia esta URL.

---

## 7. Instrucciones para la Asistencia de IA

Al recibir una nueva tarea o solicitud de cambio:
1. **Revisa este documento** para ubicar el archivo, blueprint o tabla involucrada.
2. **Realiza modificaciones quirúrgicas** enfocadas únicamente en los archivos relevantes.
3. **Mantén las firmas de API**, la estructura dinámica de `window.location.origin` y la compatibilidad con el esquema de base de datos descrito arriba.
