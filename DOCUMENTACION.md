# DOCUMENTACIÓN TÉCNICA Y ARQUITECTURA DEL PROYECTO ALETHEIA

> **Nota para Asistentes de IA y Desarrolladores:**  
> Este documento contiene la arquitectura completa, esquema de base de datos, catálogo de APIs, estructura de archivos y reglas de negocio del proyecto **ALETHEIA**. Consúltalo como fuente de verdad para realizar modificaciones, agregar rutas o ajustar lógica sin necesidad de escanear repetidamente todo el código fuente del proyecto.

> **⭐ PRINCIPIO RECTOR — prioridad máxima:** lo más importante de ALETHEIA es que su **tasa de acierto de predicciones sea alta**. El motor, las features, el cache y toda decisión de diseño se subordinan a **maximizar la precisión** (medida con `accuracy`, Brier y log-loss en `/api/comparacion`). La velocidad, la estética y la comodidad importan, pero **nunca por encima de acertar**: cualquier cambio que empeore la tasa de acierto debe rechazarse o revertirse.

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
├── inicio/                   # Componente CARGAR DATOS (ETL: carga de Excel) — /inicio/
│   ├── __init__.py
│   ├── inicio.py             # Blueprint Flask (/api/init-db, /api/etl, /api/etl-batch,
│   │                         #   /api/etl-status/<job_id>, /api/status) — backend ETL
│   ├── index.html            # UI "CARGAR DATOS": subir Excel, INIT DB, log, progreso
│   ├── style.css
│   └── script.js
├── tablas/                   # Componente Explorador de Tablas (Raw Data)
│   ├── __init__.py
│   ├── tablas.py             # Blueprint Flask (/api/tablas, /api/tabla/<nombre>, /api/tablas/reporte)
│   ├── index.html
│   ├── style.css
│   └── script.js
├── visualizar/               # Componente de Visualización y Métricas VCT
│   ├── __init__.py
│   ├── visualizar.py         # Blueprint Flask (partidos, jugadores, mapas, rondas, economía, agentes)
│   ├── index.html
│   ├── style.css
│   └── script.js
├── aletheia/                 # Componente EN VIVO (predictor; proxy hacia ALETHEIA_PREDICT)
│   ├── __init__.py
│   ├── aletheia.py           # Blueprint Flask — proxy HTTP a ALETHEIA_PREDICT_URL
│   ├── index.html            # Página EN VIVO: lista de simulaciones + mapa/bando + armador de serie
│   ├── style.css
│   └── script.js
├── aletheia_preparar/        # Componente PREPARAR PARTIDO (equipos + id + preparar/asociar)
│   ├── index.html
│   ├── style.css
│   └── script.js
├── header/                   # Componente HEADER reutilizable (no es una página)
│   ├── index.html            # Demo/preview del componente
│   ├── header.css            # Estilos del header (clases `ae-*`)
│   └── header.js             # Inyecta el header en `#aeHeaderMount` y marca el nav activo
├── multimedia/               # Archivos multimedia / imágenes (maps/, agents/)
├── wsgi.py                   # Punto de entrada WSGI para Gunicorn
├── render.yaml               # Configuración de despliegue en Render
├── requirements.txt          # Dependencias de Python
└── DOCUMENTACION.md          # Este documento de arquitectura
```

> **Módulos eliminados:** `predecir/` (Predictor Monte Carlo clásico) y `exportar/`
> (exportación CSV/Excel/JSON/ZIP) fueron **borrados** junto con sus blueprints
> (`predecir_bp`, `exportar_bp`) y sus rutas `/api/equipos-pred`,
> `/api/predecir-partido`, `/api/export/*`. El predictor vigente es el proxy a
> ALETHEIA_PREDICT (módulo `aletheia/`).

---

## 3. Base de Datos y Capa de Conexión (`backend/conexion.py`)

La conexión a la base de datos se gestiona de forma centralizada a través de las funciones `get_conn()` y `release_conn(conn)` definidas en `backend/conexion.py`.

### Variables de Entorno Soporta:
- `TURSO_DATABASE_URL`: URL remota de la base de datos libSQL (por defecto: `libsql://aletheia-leecsito.aws-us-east-1.turso.io`).
- `TURSO_AUTH_TOKEN`: Token JWT de autenticación para Turso.
- `DATABASE_PATH`: Ruta al archivo SQLite local de respaldo (por defecto: `backend/aletheia.db`).

### Esquema de las Tablas de la Base de Datos

**13 tablas del ETL** (creadas/gestionadas por ALETHEIA):

> **Modelo normalizado (solo IDs):** las tablas de hechos **no guardan nombres ni
> siglas** de equipos/jugadores. La identidad vive únicamente en `teams` (`team_name`,
> `tag`) y `players` (`nickname`, `real_name`) y se referencia por FK (`*_id`). Para
> mostrar nombres hay que hacer `JOIN` con `teams`/`players`.
> El ETL migra esquemas viejos automáticamente: rellena las FKs desde los textos y
> luego **elimina** las columnas de texto redundantes (`team_a`, `team_b`, `winner`,
> `team_top`, `team_bot`, `player_name`, `team_name`, `team`, `player_a`, `player_b`).

1. **`matches`**: Partidos jugados.
   - `match_id` (INTEGER, PK), `tournament` (TEXT), `phase` (TEXT), `match_date` (TEXT), `score_a` (INTEGER), `score_b` (INTEGER), `patch` (TEXT), `team_a_id` (FK teams), `team_b_id` (FK teams), `winner_id` (FK teams).
2. **`match_veto`**: Picks y bans de mapas por partido.
   - `veto_id` (INTEGER, PK AUTO), `match_id` (FK matches), `action` (TEXT: pick/ban/decider), `team` (TEXT: a/b), `map_name` (TEXT), `veto_order` (INTEGER), `team_id` (FK teams).
3. **`maps`**: Mapas disputados en los partidos.
   - `map_id` (TEXT, PK), `match_id` (FK matches), `map_name` (TEXT), `map_number` (INTEGER), `picker` (TEXT: a/b/decider), `side_chosen` (TEXT), `side_top_start` (TEXT), `score_a_attack` (INTEGER), `score_a_defense` (INTEGER), `score_b_attack` (INTEGER), `score_b_defense` (INTEGER), `duration` (TEXT), `picker_id` (FK teams).
4. **`rounds`**: Detalle ronda a ronda.
   - `map_id` (TEXT, FK maps), `round_num` (INTEGER), `winner_id` (FK teams), `result_type` (TEXT), `winning_side` (TEXT), `team_top_id` (FK teams), `bank_top` (INTEGER), `spend_top` (INTEGER), `category_top` (TEXT), `team_bot_id` (FK teams), `bank_bot` (INTEGER), `spend_bot` (INTEGER), `category_bot` (TEXT). PK: `(map_id, round_num)`.
5. **`player_stats`**: Rendimiento individual por mapa y lado.
   - `stat_id` (INTEGER, PK AUTO), `match_id` (FK matches), `map_id` (FK maps), `player_id` (FK players), `team_id` (FK teams), `side` (TEXT), `agent` (TEXT), `rating` (REAL), `acs` (INTEGER), `kills` (INTEGER), `deaths` (INTEGER), `assists` (INTEGER), `kast` (REAL), `adr` (REAL), `hs_percent` (REAL), `fk` (INTEGER), `fd` (INTEGER).
6. **`economy_summary`**: Resumen económico por equipo y mapa.
   - `econ_id` (INTEGER, PK AUTO), `match_id` (FK matches), `map_id` (FK maps), `team_id` (FK teams), `pistol_won` (INTEGER), `eco_played` (INTEGER), `eco_won` (INTEGER), `semi_eco_played` (INTEGER), `semi_eco_won` (INTEGER), `semi_buy_played` (INTEGER), `semi_buy_won` (INTEGER), `full_buy_played` (INTEGER), `full_buy_won` (INTEGER).
7. **`duels`**: Enfrentamientos y duelos 1v1 entre jugadores.
   - `duel_id` (INTEGER, PK AUTO), `match_id` (FK matches), `map_id` (FK maps), `duel_type` (TEXT), `player_a_id` (FK players), `player_b_id` (FK players), `kills_a` (INTEGER), `kills_b` (INTEGER).
8. **`multikills_clutches`**: Bajas múltiples y situaciones límite.
   - `mk_id` (INTEGER, PK AUTO), `match_id` (FK matches), `map_id` (FK maps), `player_id` (FK players), `agent` (TEXT), `k2`..`k5` (INTEGER), `v1`..`v5` (INTEGER), `econ_rating` (INTEGER), `plants` (INTEGER), `defuses` (INTEGER).
9. **`teams`**: Información de equipos (fuente de verdad de nombres/siglas).
   - `team_id` (INTEGER, PK), `team_name` (TEXT), `region` (TEXT), `url` (TEXT), `tag` (TEXT), `country` (TEXT).
10. **`players`**: Registro de jugadores.
    - `player_id` (INTEGER, PK AUTO), `nickname` (TEXT), `real_name` (TEXT), `team_id` (FK teams), `country` (TEXT).
11. **`roster_transactions`**: Historial de movimientos de roster (JOIN/LEAVE/INACTIVE).
    - `transaction_id` (INTEGER, PK AUTO), `team_id` (FK teams), `player_id` (FK players), `action` (TEXT: JOIN/LEAVE/INACTIVE), `transaction_date` (TEXT), `reference_url` (TEXT).
    - Fuente: `vct_transacciones.xlsx` (global). El ETL crea `teams`/`players` stub si el id no existe y hace UPSERT por `transaction_id`.
12. **`agents`**: Catálogo de agentes y su rol (fuente de verdad de nombres de agente).
    - `agent_id` (INTEGER, PK AUTO), `agent_name` (TEXT UNIQUE: jett, raze, sova...), `role` (TEXT: Duelist/Controller/Initiator/Sentinel).
    - Se **siembra** automáticamente con 29 agentes (constante `AGENTS` en `inicio/inicio.py`); no proviene de ningún Excel.
13. **`player_agent_stats`**: Rendimiento agregado por jugador y agente (ventana temporal).
    - `id` (INTEGER, PK AUTO), `player_id` (FK players), `agent` (TEXT), `date_start`/`date_end` (TEXT `YYYY-MM-DD`), `use_count`, `rnd`, `rating`, `acs`, `kd`, `kast`, `adr`, `kpr`, `apr`, `fk_fd`, `k`, `d`, `a`, `fk`, `fd`. `UNIQUE(player_id, agent, date_start, date_end)`.
    - Fuente: `vct_stats_agentes.xlsx` (global). El ETL hace UPSERT por `(player_id, agent, date_start, date_end)` (idempotente) y crea `players` stub si el id no existe.

**2 tablas del servicio ALETHEIA_PREDICT** (ALETHEIA **solo las consulta/muestra**; las crea y escribe el servicio externo). `match_id` es el id del partido de **vlr.gg** (ej. `753455`) y es el mismo para todo el partido.

11. **`predicciones_mapa`**: predicción cacheada por mapa y lado.
    - `id`, `match_id`, `equipo_a`, `equipo_b`, `map_name`, `lado_inicial_a`, `prob_victoria_a`, `prob_victoria_b`, `prob_overtime`, `n_sim`, `con_datos`, `modelo_version`, `created_at`, `updated_at`.
    - `UNIQUE(match_id, equipo_a, equipo_b, map_name, lado_inicial_a)`.
12. **`predicciones_serie`**: predicción cacheada de la serie.
    - `id`, `match_id`, `equipo_a`, `equipo_b`, `formato`, `mapas_json`, `prob_serie_a`, `prob_serie_b`, `n_sim`, `modelo_version`, `created_at`.

---

## 4. Catálogo de Rutas API (Backend)

### 4.1. Módulo CARGAR DATOS / ETL (`inicio_bp`)
- `POST /api/init-db`: Crea las tablas de la base de datos si no existen y ejecuta migraciones.
- `POST /api/etl`: Recibe archivos Excel (`vct_partidos`, `vlr_mapas`, `vlr_rondas`, etc.) y procesa la inserción masiva. Responde `202` con un `job_id`; el ETL corre en segundo plano. Archivos globales: `vct_equipos`, `vct_jugadores`, `vct_transacciones`, `vct_stats_agentes`.
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

### 4.4. Módulo Predictor Avanzado (`aletheia_bp`)

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
a `[100, MAX_SIM]` (default `10000`). **En hosting free no usar 25K/50K.**

> **P(mapa) es independiente del lado y constante entre mapas** del mismo
> enfrentamiento: la fija la regresión logística calibrada (Platt) sobre el rating
> Glicko-2; el Monte Carlo solo re-escala y aporta `prob_overtime`. No esperes
> probabilidades distintas por mapa. La P(serie) se agrega asumiendo mapas
> independientes desde la lista ordenada de mapas que realmente se juegan (el veto
> se conoce antes del partido): **no** se predice ni se enumeran permutaciones.

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
     "prob_victoria_a": 0.4412, "prob_victoria_b": 0.5588, "prob_overtime": 0.164,
     "confianza": "media", "fuente": "cache",
     "marcadores": [
       {"marcador_a": 11, "marcador_b": 13, "prob": 0.09},
       {"marcador_a": 13, "marcador_b": 11, "prob": 0.07},
       {"marcador_a": 10, "marcador_b": 13, "prob": 0.06}
     ],
     "marcador_mas_probable": {"marcador_a": 11, "marcador_b": 13, "prob": 0.09}}
  ],
  "prob_serie_a": 0.6333,
  "prob_serie_b": 0.3667,
  "confianza_serie": "media",
  "modelo_version": "<hash>",
  "n_sim": 10000
}
```
`prob_victoria_a` viene **calibrada**. `confianza`/`confianza_serie`
(`alta|media|baja`) son para la UI; `fuente` es `cache|calculado`.
`marcadores[]` viene ordenado por `prob` desc; `marcador_a` son los goles de
`equipo_a`. **El marcador más probable ronda 8-12%**, no es dominante: la web lo
etiqueta siempre como *estimación, no resultado seguro*.

**Detalle añadido (2026-09):** por mapa, `economia` (micro-eventos de economía/
ronda: `equipo_a`/`equipo_b` con `{n, p_gana_ronda}` por categoría, los 16 cruces
`cat_a_vs_cat_b` con `{n, p_gana_a}` y `pistol`) y `analisis_mapa` (winrate
histórico de cada equipo **en ese mapa** + `p_mapa_a`, una P **por mapa** que
difiere; es análisis, no la predicción calibrada del motor); a nivel serie,
`resultados_serie` (`{"2-0","2-1","1-2","0-2"}` en Bo3, `3-x` en Bo5) y
`caminos_serie` (secuencia mapa a mapa: `V` gana A, `D` gana B). Todo también en
las lecturas crudas `/api/predicciones` y `/api/prediccion`.

**Semántica de la banda de confianza** (la fija el backend, conservadora):
se calcula sobre `max(p, 1-p)` → `>=0.62` **alta**, `>=0.55` **media**, si no
**baja**. La web la muestra con color (alta=verde, media=ámbar, baja=gris) y, si
el dato no viene, lo **oculta o deriva** con la misma regla (no rompe).

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
  Body: `{"equipo_a", "equipo_b", "n_sim": 10000, "match_id": 753455, "mapas": [...]?,
  "forzar": false}`. Calcula los 13 mapas × 2 lados (26 filas) y hace UPSERT en
  `predicciones_mapa` con ese `match_id`.
  **Por defecto ASÍNCRONO:** responde `202` con
  `{"ok", "job_id", "total": 26, "modelo_version", "progreso", "mapas_hechos"}`.
  Progreso: `GET {BASE}/api/precalcular/estado?job_id=...` →
  `{"ok", "job": {"estado", "progreso", "mapas_hechos", "computados", "desde_cache", "error"}}`.
  Con `sync:true` corre inline y responde `{"ok", "total", "computados", "desde_cache", "tiempo_s"}`.
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
- `GET /api/aletheia/simulaciones[?equipo=&match_id=&limite=]` → proxy de
  `GET {BASE}/api/simulaciones`. Lista los enfrentamientos ya preparados:
  ```json
  {
    "ok": true, "modelo_version": "<hash>",
    "simulaciones": [{"match_id": 753455, "equipo_a": "Team Liquid", "equipo_b": "Paper Rex",
                      "mapas": 13, "filas": 26, "n_sim": 50000, "modelo_version": "<hash>",
                      "vigente": true, "created_at": "...", "updated_at": "..."}]
  }
  ```
  `match_id=0` = "sin id" (para leer sus filas usar `equipo_a`/`equipo_b`).
- `POST /api/aletheia/serie` → proxy de `POST {BASE}/api/serie`. Probabilidad de
  serie desde caché (sin Monte Carlo; no escribe en la DB). Body:
  `{"match_id":753455,"equipo_a":...,"equipo_b":...,"mapas":[{map_name,lado_inicial_a},...]}`.
  Respuesta: `{"ok":true,"formato":"bo3","mapas_para_ganar":2,"mapas":[{...,"fuente":"cache"}],"prob_serie_a":...,"prob_serie_b":...}`.
- `POST /api/aletheia/borrar` → proxy de `POST {BASE}/api/borrar`. Body:
  `{"equipo_a", "equipo_b", "match_id"}`. Borra las filas de `predicciones_mapa`
  y `predicciones_serie` de ese enfrentamiento (útil para duplicados o
  preparaciones erróneas). Responde `{"ok": true, "filas_borradas": N, "match_id": ...}`.
  Nota: el endpoint vive en ALETHEIA_PREDICT; ALETHEIA solo lo invoca por proxy
  (no borra directamente en Turso).

**Flujo del ciclo (frontend → `PREDICT_DIRECTO` ngrok solo para lo largo):**
1. **PREPARAR** (`/aletheia_preparar/`): `POST {PREDICT_DIRECTO}/api/precalcular`
   con el id de vlr.gg (llamada larga, directa al servicio; async con `job_id`).
   El polling `GET {PREDICT_DIRECTO}/api/precalcular/estado?job_id=...` también es
   directo. El resto (`equipos`, `modelo_version`, `predicciones`, `asociar`) va
   por el **proxy** de la web (`/api/aletheia/...`). Se guarda el `modelo_version`
   (viene en el `202`); se puede **ASOCIAR ID** y **re-preparar** con
   `forzar:true` si el modelo cambió.
2. **LISTAR/LEER** (`/aletheia/`, EN VIVO): `GET /api/aletheia/simulaciones` lista
   lo preparado (por defecto solo `vigente:true`); al elegir una se leen sus filas
   UNA vez con `GET /api/aletheia/predicciones` (caché). EN VIVO **nunca** llama a
   ngrok directo ni simula.
3. **MAPA/BANDO**: elegir mapa+lado muestra `prob_victoria_a/b`, `prob_overtime` y
   `confianza` desde la caché local (solo consulta `/api/aletheia/prediccion` si
   falta el dato).
4. **ARMAR SERIE**: `POST /api/aletheia/serie` da `prob_serie_a/b`,
   `confianza_serie` y `mapas_para_ganar` al instante (desde caché, sin Monte Carlo).
5. **COMPARACIÓN** (EN VIVO): `GET /api/aletheia/comparacion` contrasta lo predicho
   con el resultado real del mismo `match_id`.
6. **GESTIÓN** (EN VIVO): por enfrentamiento, **asignar/corregir ID**
   (`POST /api/aletheia/asociar`) y **borrar** duplicados o preparaciones erróneas
   (`POST /api/aletheia/borrar`).
7. **INVALIDACIÓN:** la web compara el `modelo_version` de las filas cacheadas con
   `GET /api/aletheia/modelo_version`; si difiere, esas filas quedan
   `vigente:false` y la web marca **RE-PREPARAR** (que envía `forzar:true`).

---

## 5. Estructura y Reglas del Frontend

1. **Rutas Estáticas de Navegación (`backend/app.py`):**
   Las subcarpetas registradas en `FRONTEND_FOLDERS = ['inicio', 'tablas', 'visualizar', 'aletheia', 'aletheia_preparar', 'header']` se sirven automáticamente en la raíz HTTP:
   - `/` → **redirige a `/aletheia/`**
   - `/inicio/` o `/inicio/index.html` → **CARGAR DATOS** (ETL: subir Excel, INIT DB, log)
   - `/tablas/` o `/tablas/index.html`
   - `/visualizar/` o `/visualizar/index.html`
   - `/aletheia/` o `/aletheia/index.html` (**EN VIVO**)
   - `/aletheia_preparar/` o `/aletheia_preparar/index.html` (**PREPARAR**)
   - `/header/` o `/header/index.html` (demo del componente header)
   - `header/header.css` y `header/header.js` se sirven como estáticos desde la raíz
     (`static_url_path=''`).
   > Los módulos `predecir/` y `exportar/` **ya no existen**.

2. **Configuración de Host API Dinámico:**
   En todos los archivos JavaScript del frontend (`script.js`), la variable `API` está configurada como:
   ```javascript
   const API = `${window.location.origin}/api`;
   ```
   Esto garantiza que las peticiones se dirijan correctamente al mismo host tanto en entornos locales (`http://localhost:5000/api`) como en producción en Render (`https://tu-app.onrender.com/api`).

3. **Servicio ALETHEIA_PREDICT (ngrok) y las DOS páginas (`aletheia/` y `aletheia_preparar/`):**
   - El predictor externo corre en el PC del autor y se expone con ngrok en la
     constante `PREDICT_DIRECTO` (`https://snugly-encore-sweep.ngrok-free.dev`).
     Todas las llamadas a ese host llevan el header `ngrok-skip-browser-warning: 1`
     (helper `predictFetch`).
   - Las corridas **largas** (`POST /api/precalcular` y su polling
     `GET /api/precalcular/estado`) se piden **directo** a `PREDICT_DIRECTO` (no por
     el proxy de la web) para no chocar con el timeout de gunicorn/Render.
     `equipos`, `mapas`, `modelo_version`, `predicciones`, `prediccion`, `serie`,
     `comparacion`, `simulaciones`, `asociar` y `borrar` van por el **proxy**
     `/api/aletheia/...` (helper `proxyFetch`; son rápidos).
   - **EN VIVO** (`/aletheia/`) no importa `PREDICT_DIRECTO`: todas sus lecturas
     salen de la caché a través del proxy.
   - **`/aletheia_preparar/` — PREPARAR:** selección de equipos (search+grids),
     selector de simulaciones (1K/5K/10K; **sin 25K/50K en hosting free**), campo de ID vlr.gg (parsea URL o
     número), **PREPARAR PARTIDO** (async: `POST /api/precalcular` → `job_id` → poll
     `/api/precalcular/estado`, con % y tiempo transcurrido), **ASOCIAR ID** y badges
     de caché ("ya predicho") y de `modelo_version` ("si cambia el modelo" marca
     RE-PREPARAR). Botón **"IR A EN VIVO →"**.
     - El **poll es resiliente**: ante cortes del túnel/PC (p. ej.
       `ERR_PROXY_CONNECTION_FAILED`) reintenta con backoff en vez de abortar, y
       ofrece **"cancelar espera"**. Se usa un favicon inline para evitar el 404 de
       `/favicon.ico`.
   - **`/aletheia/` — EN VIVO (nunca simula):**
     - Al cargar, `GET /api/aletheia/simulaciones` pinta la lista de preparadas
       (`EQUIPO_A vs EQUIPO_B · #match_id · N mapas · n_sim · [vigente]`); por
       defecto solo `vigente:true`, con toggle "mostrar no vigentes" (marcadas
       "re-preparar").
     - **Gestión por enfrentamiento:** **✎ ID** reasigna el `match_id`
       (`POST /api/aletheia/asociar`; sirve si se preparó sin id) y **🗑 BORRAR**
       elimina el enfrentamiento (`POST /api/aletheia/borrar`; sirve para
       duplicados o preparaciones erróneas).
     - Al elegir una se leen sus filas **UNA vez** (`GET /api/aletheia/predicciones`)
       y se guardan en `liveBulk` (`map|side`).
      - **MAPA / SERIE (tab único):** los tabs de la página son `MAPA / SERIE` y
        `COMPARACIÓN`. **ARMAR SERIE dejó de ser un tab** y ahora es una **sección
        dentro de `MAPA / SERIE`**. Layout en **dos columnas**: **izquierda** el
        análisis del **mapa** seleccionado, **derecha** la **serie**. Arriba: selector
        único de los 13 mapas (agrega mapas a la serie; clic = añadir + ver análisis),
        los **slots en orden** con bando por mapa, y el botón **ARMAR SERIE**. Al
        pulsar el botón se calcula la serie; cambiar formato/mapas/lado **invalida** el
        banner (hay que volver a pulsar). Clic en un mapa del grid o en un slot muestra
        su análisis a la izquierda, sin subir/bajar.
      - **Explorador de mapas:** rejilla de los 13 mapas con la **P(A) por mapa**
        (`analisis_mapa.p_mapa_a`, **difiere por mapa**) y OT del bando elegido
        (leídas de `liveBulk`; no llama al servicio en cada clic). Al tocar un mapa
        muestra las tarjetas A/B **por mapa** (`p_mapa_a`), `prob_overtime`,
        **confianza del motor** (derivada si falta: `max(p,1-p)` ≥0.62 alta, ≥0.55
        media, resto baja), `n_sim` y una **nota de análisis** con la P del motor
        (igual en todos los mapas) y el **historial de cada equipo en ese mapa**
        (`analisis_mapa.equipo_a/b`: `winrate`, `n`).
      - **DISTRIBUCIÓN DE MARCADOR:** bajo las tarjetas se muestra el **marcador
        más probable** (etiquetado como *estimación, no resultado seguro*) y el
        **top-3** con su % (`marcadores[]` viene del backend ordenado por prob desc;
        `marcador_a` = goles de `equipo_a`). Si `marcadores` falta (fila de caché
        anterior al cambio), el bloque se **oculta** y se marca el enfrentamiento
        para **RE-PRECALCULAR**.
      - **ECONOMÍA / RONDAS (micro-eventos):** bloque `economia-block` bajo el
        marcador. Por mapa y bando muestra: win rate por categoría
        (eco/semi-eco/semi-buy/full-buy) de **cada equipo** con barra, **pistol**
        (P de que gane `equipo_a`), y la **mini-matriz de cruces** 4×4
        (`cat_a_vs_cat_b`, prob. de que `equipo_a` gane la ronda; filas = `equipo_a`,
        columnas = `equipo_b`), destacando `semi_buy_vs_full_buy` y
        `eco_vs_full_buy`. Etiquetado como *estimación condicionada a la P del mapa*.
        Si `economia` falta, el bloque se oculta y se marca **RE-PRECALCULAR**
        junto con `marcadores` (helper `detalleFaltante`).
      - **ARMAR SERIE (BO1/BO3/BO5)** *(sección dentro de `MAPA / SERIE`):* slots en
        orden (el último = DECIDER) con bando por mapa; cada cambio hace
        `POST /api/aletheia/serie` y muestra el banner (`prob_serie_a/b`,
        `confianza_serie` con color, formato, `mapas_para_ganar`, `n_sim`) al
        instante. La tabla de mapas de la serie muestra la **confianza por mapa** y
        el **marcador más probable** de cada mapa. El banner incluye además la
        **distribución de la serie** (`resultados_serie`: 2-0/2-1/1-2/0-2, ordenada
        por prob desc) y los **caminos de la serie** (`caminos_serie`: la secuencia
        mapa a mapa, p. ej. `V-D-D` vs `D-V-D` para un 1-2; ✓ gana A, ✗ gana B).
     - **VIGENCIA Y RE-PRECALCULAR:** la web compara `modelo_version` de cada
       enfrentamiento con `GET /api/aletheia/modelo_version` (y usa `vigente` de
       `/api/simulaciones`). Si difiere, o si las filas no traen `marcadores`,
       marca el enfrentamiento como **RE-PRECALCULAR** y ofrece **↻ RE-PRECALCULAR**,
       que hace `POST /api/precalcular` con `forzar:true` **directo a
       `PREDICT_DIRECTO`** (async: `job_id` + polling de `/api/precalcular/estado`),
       refresca la lista y vuelve a leer la caché.
     - **COMPARACIÓN:** `GET /api/aletheia/comparacion?match_id=..` muestra tarjetas resumen
       (accuracy, brier, log-loss, favoritos_ok, upsets, inciertos) y una tabla de
       detalle coloreada (verde = favorito ganó, rojo = upset, ámbar = incierto);
       si el partido no está en la DB: "sin resultado real todavía".
     - Botón **"← PREPARAR PARTIDO"**.
   - Servicio apagado: cada llamada se maneja con avisos, sin romper la página.

4. **Sistema de Diseño Visual:**
   - Estética oscura / Cyberpunk (`--bg-color: #0b0e14`, paneles con fondo translúcido y bordes luminosos).
   - Tipografías principales desde Google Fonts:
     - Titulares y Badges: `'Bebas Neue', sans-serif`
     - Textos, Tablas y Métricas: `'DM Mono', monospace`

5. **Componente HEADER reutilizable (`header/`):**
   La cabecera de navegación ya **no se duplica** en cada `index.html`: vive en
   `header/` y se inyecta en las páginas reales.

   - **Inclusión** en el `<head>` de la página:
     ```html
     <link rel="stylesheet" href="../header/header.css" />
     ```
   - **Marcado** (un solo montaje dentro del `<body>`):
     ```html
     <div id="aeHeaderMount"></div>
     ```
   - **Script** (antes de cerrar `</body>`, antes del `script.js` de la página):
     ```html
     <script>window.AE_HEADER = { title: 'EN VIVO', badge: 'PREDICTOR' };</script>
     <script src="../header/header.js"></script>
     ```
   - `header.js` construye el nav (`EN VIVO`, `PREPARAR PARTIDO`, `TABLAS`,
     `VISUALIZAR`, `CARGAR DATOS`), resuelve las rutas relativas a la raíz y
     **marca activa** la página actual según `window.location.pathname`.
   - Config opcional `window.AE_HEADER`:
     - `title` / `badge`: título central (p. ej. `EN VIVO` · `PREDICTOR`).
     - `hidden`: array de ids (`'aletheia'`, `'preparar'`, `'tablas'`,
       `'visualizar'`, `'datos'`) para ocultar entradas concretas.
   - Todas las clases del componente usan prefijo `ae-` (`.ae-header`, `.ae-nav`,
     `.ae-btn`, `.ae-logo`, `.ae-page-title`…) para no colisionar con los estilos
     propios de cada componente.
   - `header/index.html` es solo una **demo/preview** del componente.
   - **Páginas que lo usan:** `aletheia/`, `aletheia_preparar/`, `tablas/`,
     `visualizar/`, `inicio/` (CARGAR DATOS).
   - El componente `inicio/` añade su propia barra `.db-bar` bajo el header con
     el estado de la DB y el botón **INIT DB** (son específicos de CARGAR DATOS,
     no del header compartido).

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
4. **No dupliques el header**: usa el componente `header/` (`#aeHeaderMount` + `header.js`).
5. **Recuerda los módulos eliminados**: `predecir/` y `exportar/` no existen; no los referencies.
6. **Prioriza siempre la tasa de acierto** (principio rector, §1): ningún cambio debe degradar la precisión de las predicciones. Si un cambio la empeora, descártalo o revíerte.
