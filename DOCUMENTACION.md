# DOCUMENTACIÓN TÉCNICA Y ARQUITECTURA DEL PROYECTO ALETHEIA

> **Nota para Asistentes de IA y Desarrolladores:**  
> Este documento contiene la arquitectura completa, esquema de base de datos, catálogo de APIs, estructura de archivos y reglas de negocio del proyecto **ALETHEIA**. Consúltalo como fuente de verdad para realizar modificaciones, agregar rutas o ajustar lógica sin necesidad de escanear repetidamente todo el código fuente del proyecto.

> **⭐ PRINCIPIO RECTOR — prioridad máxima:** lo más importante de ALETHEIA es que su **tasa de acierto de predicciones sea alta**. El motor, las features, el cache y toda decisión de diseño se subordinan a **maximizar la precisión** (medida con `accuracy`, Brier y log-loss en `/api/comparacion`). La velocidad, la estética y la comodidad importan, pero **nunca por encima de acertar**: cualquier cambio que empeore la tasa de acierto debe rechazarse o revertirse.

---

## 1. Visión General del Proyecto

**ALETHEIA** es una plataforma web integral de analítica, procesamiento ETL, visualización y predicción de partidos para deportes electrónicos (específicamente Valorant VCT).

### Stack Tecnológico:
- **Backend:** Python 3 (Flask, Gunicorn, Pandas, NumPy, OpenPyXL, Pillow, flask-compress, libSQL client / SQLite3).
- **Base de Datos:** Turso (libSQL en la nube) con fallback a SQLite3 local (`aletheia.db`).
- **Frontend:** Vanilla HTML5, Vanilla CSS3 (Variables CSS, Estética Cyberpunk/Dark Mode), JavaScript ES6+ (Fetch API, origen dinámico `window.location.origin`).
- **Despliegue:** Render / Gunicorn (`render.yaml` y `requirements.txt`).

---

## 2. Estructura de Directorios

```
ALETHEIA/
├── backend/                  # Núcleo del servidor Flask y gestión de conexión
│   ├── __init__.py
│   ├── app.py                # Punto de entrada de Flask, registro de Blueprints, gzip y rutas HTML
│   ├── conexion.py           # Gestión centralizada de la base de datos (Turso / SQLite) + fetch_all (pool)
│   ├── cache.py              # Caché TTL en memoria para consultas SQL (acelera visitas repetidas)
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
│   ├── aletheia.py           # Blueprint Flask — proxy HTTP: lecturas al servicio
│   │                         #   READ_URL (Render) y cómputo/mutaciones a ALETHEIA_PREDICT_URL
│   ├── index.html            # Página EN VIVO: lista de simulaciones + mapa/bando + armador de serie
│   ├── style.css
│   └── script.js
├── aletheia_preparar/        # Componente PREPARAR PARTIDO (equipos + id + preparar/asociar)
│   ├── index.html
│   ├── style.css
│   └── script.js
├── header/                   # Componente HEADER reutilizable (no es una página)
│   ├── index.html            # Demo/preview del componente
│   ├── header.css            # Estilos del header (clases `ae-*`): fondo lima, logo y nav
│   ├── header.js             # Inyecta el header en `#aeHeaderMount` y marca el nav activo
│   └── header-nodes.js       # Canvas de nodos: efecto del header + fondo global `.ae-nodes-bg`
├── comun/                    # Core visual compartido (no es una página)
│   ├── theme.css             # Tema global: paleta única en `:root`, base y tokens de diseño
│   ├── ALETHEIA_ico.svg      # Copia servible del ico/logo (la ruta raíz responde 308 → 404)
│   ├── vct.css               # Design system de los componentes VCT (clases `v-*`)
│   └── vct.js                # Helpers globales `VCT` (API, formato, filas de partido, tabs)
├── partidos/                 # Componente PARTIDOS estilo vlr.gg (lista + detalle)
│   ├── partidos.py           # Blueprint (/api/partidos, /api/partidos/filtros, /api/partidos/resultados, /api/partido/<id>)
│   ├── index.html, style.css, script.js
├── equipos/                  # Componente EQUIPOS (grid + detalle con roster/tabs)
│   ├── equipos.py            # Blueprint (/api/equipos, /api/equipo/<id>)
│   ├── index.html, style.css, script.js
├── jugadores/                # Componente JUGADORES (lista + detalle con agentes)
│   ├── jugadores.py          # Blueprint (/api/jugadores, /api/jugador/<id>)
│   ├── index.html, style.css, script.js
├── eventos/                  # Componente EVENTOS (lista + detalle con partidos/mapas/agentes)
│   ├── eventos.py            # Blueprint (/api/eventos, /api/evento)
│   ├── index.html, style.css, script.js
├── media/                    # Enlaces a logos/fotos de vlr.gg (no es una página)
│   ├── media.py              # Blueprint (/api/media/equipo/<id>, /api/media/jugador/<id>, /api/media/evento/<id>, /api/media/estado)
│   └── urls_cache.json       # Caché de enlaces + color medio (solo texto, VERSIONADA desde F5
│                             #   para sobrevivir a los deploys; solo se ignora el .tmp)
├── multimedia/               # Archivos multimedia / imágenes
│   ├── agents/               # 28 retratos de agentes (.avif, locales, usados en scoreboards)
│   └── maps/                 # 13 imágenes de mapas (.avif, locales, usadas en tabs de mapa)
├── tools/                    # Utilidades de build/mantenimiento (no se sirven)
│   └── minificar_assets.py   # F12: genera los *.min.css/*.min.js y reescribe los
│                             #   HTML con ?v=<hash> (los fuentes se conservan)
├── ALETHEIA_ico.svg          # Logo/ico original del proyecto (raíz; se copia a comun/ para servir)
├── wsgi.py                   # Punto de entrada WSGI para Gunicorn
├── render.yaml               # Configuración de despliegue en Render
├── requirements.txt          # Dependencias de Python
└── DOCUMENTACION.md          # Este documento de arquitectura
```

> **Assets minificados/versionados (F12/F7):** las páginas enlazan las versiones
> `*.min.css`/`*.min.js` con `?v=<hash8>` (`comun/theme.min.css`, `comun/vct.min.*`,
> `header/header*.min.*`, `aletheia/style.min.css`, `aletheia/script.min.js`). Las
> fuentes siguen en el repo y el desarrollo se hace sobre ellas; para regenerar
> las versiones minificadas y los hashes de los HTML: `py tools/minificar_assets.py`.

> **Módulos eliminados:** `predecir/` (Predictor Monte Carlo clásico) y `exportar/`
> (exportación CSV/Excel/JSON/ZIP) fueron **borrados** junto con sus blueprints
> (`predecir_bp`, `exportar_bp`) y sus rutas `/api/equipos-pred`,
> `/api/predecir-partido`, `/api/export/*`. El predictor vigente es el proxy a
> ALETHEIA_PREDICT (módulo `aletheia/`).

---

## 3. Base de Datos y Capa de Conexión (`backend/conexion.py`)

La conexión a la base de datos se gestiona de forma centralizada a través de las funciones `get_conn()` y `release_conn(conn)` definidas en `backend/conexion.py`.

### Lecturas rápidas: `fetch_all()` (pool por hilo) y caché TTL
Abrir una conexión a Turso cuesta ~0.7 s y una consulta sobre conexión ya abierta ~0.2 s.
Por eso los **blueprints de lectura** (`partidos`, `equipos`, `jugadores`, `eventos`) usan:

- `fetch_all(sql, params)` (`backend/conexion.py`): reutiliza **una conexión por hilo**
  (no la cierra al terminar), con **reintento único** si Turso la cerró por inactividad
  (`reset_conn()`). No afecta al ETL ni a `tablas/`/`visualizar/`, que siguen con
  `get_conn()`/`release_conn()`.
- `@ttl_cache(120)` (`backend/cache.py`): cachea en memoria el resultado de cada SQL
  (clave = SQL + parámetros) por **120 s**. Los datos solo cambian al correr un ETL.
  Efecto medido: detalle de equipo pasó de ~6.5 s a ~2.5 s en frío y ~0.2 s en caliente.
  Desde **F9** incluye **single-flight**: 8 peticiones concurrentes de la misma clave
  ejecutan **una** consulta y las demás esperan su resultado (verificado con test).
- **F9 (módulo `aletheia`)**: `_tabla_mapas_equipo` (JOIN de ~0,7 s) con TTL
  **1800 s**; `_version_vigente_db` memoizada 30 s (`@ttl_cache`, single-flight); y
  `_predicciones_db` proyecta columnas explícitas en vez de `SELECT *` (los JSON
  `marcadores_json`/`economia_json` se conservan porque alimentan la UI).
- **gzip**: `flask-compress` comprime JSON/HTML/CSS/JS (un JSON de 24 KB baja a ~3 KB).

### Variables de Entorno Soporta:
- `TURSO_DATABASE_URL`: URL remota de la base de datos libSQL (por defecto: `libsql://aletheia-laperradeadrelees.aws-us-east-1.turso.io`).
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

**4 tablas + 1 vista de EVENTOS/META** (las escribe el ETL; las usan los componentes `eventos/` y `partidos/`):

14. **`events`**: catálogo de eventos (`event_id` de vlr.gg, `event_name`). Fuente: `vct_evento_map_stats` / `vct_evento_agent_pickrate`.
15. **`event_map_stats`**: meta de mapas por evento. PK `(event_id, map_name)`; `matches_played`, `atk_win_pct`, `def_win_pct`.
16. **`event_agent_pickrate`**: pickrate de agentes por evento y mapa. PK `(event_id, map_name, agent_name)`; `pick_pct`.
17. **`tournament_aliases`**: alias `tournament_name` → `event_id` (para cruzar `matches.tournament` con `events`).
- **Vista `v_matches_events`**: `matches` + `event_id` resuelto por alias o por nombre exacto (LOWER/TRIM). Es la base de los listados de `partidos/` y `eventos/`. **Ojo:** solo 588/1098 partidos resuelven `event_id` (los torneos 2025 y Champions/Masters no tienen evento cargado); `event_name` puede venir `NULL`.

**2 tablas del servicio ALETHEIA_PREDICT** (ALETHEIA **solo las consulta/muestra**; las crea y escribe el servicio externo). `match_id` es el id del partido de **vlr.gg** (ej. `753455`) y es el mismo para todo el partido.

11. **`predicciones_mapa`**: predicción cacheada por mapa y lado.
    - `id`, `match_id`, `equipo_a`, `equipo_b` (+ `equipo_a_id`/`equipo_b_id`),
      `map_name`, `lado_inicial_a`.
    - `prob_victoria_a` / `prob_victoria_b` (REAL): **capa ESC por (mapa, lado)**
      — la predicción que se sirve y se cachea; varía por mapa y por lado. NO es
      la P del motor.
    - `esc_peso` (REAL, nullable): peso/confianza del ESC en ese mapa/lado
      (`n_min/(n_min+10)`); `null` si la capa está apagada. Más alto = más
      historial lo respalda.
    - `prob_overtime`, `n_sim`, `con_datos`, `modelo_version`, `marcadores_json`,
      `economia_json`, `created_at`, `updated_at`.
    - La **P del motor Glicko no se persiste** (una sola por enfrentamiento en
      la raíz de `POST /predecir` y `POST /serie`).
    - `UNIQUE(match_id, equipo_a, equipo_b, map_name, lado_inicial_a)`.
12. **`predicciones_serie`**: predicción cacheada de la serie.
    - `id`, `match_id`, `equipo_a`, `equipo_b`, `formato`, `mapas_json`, `prob_serie_a`, `prob_serie_b`, `n_sim`, `modelo_version`, `created_at`.

**1 tabla de la web** (la gestiona la propia web; el motor no la escribe):

- **`map_pool`**: pool de mapas activos. `map_name` (TEXT, PK), `en_pool` (INTEGER 0/1, default 1), `updated_at` (TEXT). EN VIVO (`/aletheia/`) solo ofrece los mapas con `en_pool=1` en el selector y el armador de serie; si la tabla no existe, se degrada a todos los mapas con predicción. El tope del pool en el motor (`precalcular`/`serie`) lo aplica ALETHEIA_PREDICT (fuera de este repo).

---

## 4. Catálogo de Rutas API (Backend)

### 4.1. Módulo CARGAR DATOS / ETL (`inicio_bp`)
- `POST /api/init-db`: Crea las tablas de la base de datos si no existen y ejecuta migraciones.
- `POST /api/etl`: Recibe archivos Excel (`vct_partidos`, `vlr_mapas`, `vlr_rondas`, etc.) y procesa la inserción masiva. Responde `202` con un `job_id`; el ETL corre en segundo plano. Archivos globales: `vct_equipos`, `vct_jugadores`, `vct_transacciones`, `vct_stats_agentes`. Para cargar un torneo se requiere `vct_partidos`; los archivos globales **pueden subirse solos** (sin torneo): el ETL ejecuta solo las etapas cuyos Excel están presentes (útil p. ej. para actualizar solo `vct_transacciones`).
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

**Lectura directa de Turso (sin servidor):** las lecturas de caché (`mapas`,
`modelo_version`, `simulaciones`, `prediccion`/`predicciones` y `serie` si hay
una fila en `predicciones_serie` con los mismos mapas/lados) se sirven **primero
desde Turso** (`backend.conexion.fetch_all`): la web lista y abre las
predicciones preparadas con el PC y el servicio apagados. La capa ESC y su peso
(`esc_peso`) vienen **persistidos** en `predicciones_mapa`; solo se recalculan
`confianza` y `analisis_mapa` (análisis histórico). En modo DB
`prob_intervalo` y `escenario_mapa` quedan `null` (el escenario se recalcula en
el servicio; la UI usa `prob_victoria_a` + `esc_peso` y oculta la confianza sin
romper) y la raíz de `/api/serie` trae `prob_motor_a/b: null` (la P del motor no
se persiste). El
`/api/serie` de modo DB solo responde de `predicciones_serie` si la lista
pedida es el **veto completo** (1/3/5 mapas); con 2/4 se cae al proxy.

Solo lo que el servicio deriva va a **`ALETHEIA_PREDICT_READ_URL`** (Render,
cache-first, con reintento por cold start y caída a `ALETHEIA_PREDICT_URL`):
`equipos`, `comparacion`, `scorecard(_agregado)`, `dataset` sin guardar,
`predecir` y `serie` no cacheada. El **cómputo pesado y las mutaciones**
(`precalcular`, `precalcular/estado`, `asociar`, `borrar`, `dataset?guardar=1`
y `predecir`/`serie` con `forzar:true`) van a `ALETHEIA_PREDICT_URL`
(PC/ngrok). Si `ALETHEIA_PREDICT_READ_URL` no está definida, las lecturas no
cacheadas van a `ALETHEIA_PREDICT_URL` (comportamiento anterior).

La clave `ALETHEIA_API_KEY` (si está configurada en el entorno de la web) viaja
**solo server-side**: `_request_service` la añade como header `X-API-Key` a
todas las llamadas que reenvía; el navegador nunca la ve. Sin clave en el
servicio de predicción, los endpoints admin responden **401** (fail-closed).

Endpoints expuestos por ALETHEIA (todos reenvían al servicio externo):

- `GET /api/aletheia/equipos` → proxy de `GET {BASE}/api/equipos`.
  Adapta la respuesta para el grid del frontend:
  `{"ok": true, "teams": [{"name", "abbrev", "maps_played": 0, "avg_rating": 0}]}`.
  Como el servicio solo devuelve nombres, `abbrev = name` (decisión de diseño) y
  las métricas `maps_played`/`avg_rating` quedan en 0 porque el servicio no las aporta.
- `GET /api/aletheia/mapas` → proxy de `GET {BASE}/api/mapas`.
  Devuelve `{"ok": true, "mapas": [...]}` (13 mapas, incluye `Summit`).
  Con `?pool=1` (EN VIVO) se limita a los mapas de `map_pool` con `en_pool=1`
  (intersección con los que tienen predicción); si la tabla no existe, los 13.
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
veto debe estar **completo**: `mapas` exige **1/3/5** entradas (1→bo1, 3→bo3,
5→bo5) y 2/4 responden **400** (`'mapas' debe traer el veto completo…`); `n_sim`
se normaliza a `[100, MAX_SIM]` (default `10000`). **En hosting free no usar 25K/50K.**

> **Contrato 2026-10 (importante):** `mapas[].prob_victoria_a` es la capa
> **ESC por (mapa, lado)** — la predicción que se sirve y se cachea — y varía
> por mapa y por lado; **no promediar lados**. La **P del motor Glicko no se
> repite por mapa**: se expone UNA sola vez por enfrentamiento en la **raíz**
> como `prob_motor_a`/`prob_motor_b` (plana, puede ser `null`) y es la que
> decide la serie (`prob_serie_a`/`prob_serie_b`, motor + temperatura). Es
> esperado que un mapa muestre favorito al rival y la serie al otro: **manda el
> global**. Además, cada mapa trae `esc_peso` (peso del ESC: `n_min/(n_min+10)`;
> `null` si la capa está apagada) y `escenario_mapa` (o `null`); cuando
> `escenario_mapa` viene `null`, se usa `prob_victoria_a` + `esc_peso` y se
> oculta la confianza (no es error).

**Respuesta del servicio (proxy sin cambios):**
```json
{
  "ok": true,
  "equipo_a": "NRG",
  "equipo_b": "T1",
  "n_sim": 50000,
  "formato": "bo3",
  "mapas_para_ganar": 2,
  "prob_motor_a": 0.5343,
  "prob_motor_b": 0.4657,
  "mapas": [
    {"map_name": "Abyss", "lado_inicial_a": "attack",
     "prob_victoria_a": 0.6228, "prob_victoria_b": 0.3772, "prob_overtime": 0.164,
     "esc_peso": 0.8214, "confianza": "media", "fuente": "cache",
     "escenario_mapa": {"p_mapa": 0.6228, "delta_logit": 0.08, "n_a": 24, "n_b": 31,
                        "peso": 0.8214, "lambda": 0.4, "k": 10.0,
                        "p_lo": 0.55, "p_hi": 0.69},
     "marcadores": [
       {"marcador_a": 11, "marcador_b": 13, "prob": 0.09},
       {"marcador_a": 13, "marcador_b": 11, "prob": 0.07},
       {"marcador_a": 10, "marcador_b": 13, "prob": 0.06}
     ],
     "marcador_mas_probable": {"marcador_a": 11, "marcador_b": 13, "prob": 0.09}}
  ],
  "prob_serie_a": 0.5412,
  "prob_serie_b": 0.4588,
  "confianza_serie": "media",
  "modelo_version": "<hash>",
  "n_sim": 50000
}
```
`prob_victoria_a` (ESC) y `prob_motor_a` vienen **calibradas**.
`prob_motor_a/b` es plana (mismo valor en todos los mapas/lados) y puede ser
`null`; `prob_serie_a/b` es la única que decide la serie. `confianza`/
`confianza_serie` (`alta|media|baja`) son para la UI; `fuente` es
`cache|calculado`.
`marcadores[]` viene ordenado por `prob` desc; `marcador_a` son los goles de
`equipo_a`. **El marcador más probable ronda 8-12%**, no es dominante: la web lo
etiqueta siempre como *estimación, no resultado seguro*.

**Detalle añadido (2026-09):** por mapa, `economia` (micro-eventos de economía/
ronda: `equipo_a`/`equipo_b` con `{n, p_gana_ronda}` por categoría, los 16 cruces
`cat_a_vs_cat_b` con `{n, p_gana_a}` y `pistol`) y `analisis_mapa` (winrate
histórico de cada equipo **en ese mapa** + `p_mapa_a`, una P **por mapa** que
difiere; es análisis, no la predicción servida); a nivel serie,
`resultados_serie` (`{"2-0","2-1","1-2","0-2"}` en Bo3, `3-x` en Bo5) y
`caminos_serie` (secuencia mapa a mapa: `V` gana A, `D` gana B). Todo también en
las lecturas crudas `/api/predicciones` y `/api/prediccion`, que además
recalculan `prob_intervalo` (IC95% aditivo del RD, A6) sin persistirlo. La P
puntual y `modelo_version` no cambian por este campo.

**Semántica nueva de la economía (A1, 2026-10):** `eco`/`semi_eco`/`semi_buy`/
`full_buy` y los cruces **excluyen los pistols R1/R13** (tienen bloque `pistol`
aparte); `n` es el conteo crudo de rondas simuladas de esa categoría y
`p_gana_*` se pondera por `Σw` (cambio de medida). El backend publica
`economia.semantica` y `economia.pistol_semantica`, que la UI muestra en la nota
del bloque (con fallback si faltan).

**Capa de escenarios (B1, 2026-10):** cada mapa incluye `escenario_mapa`
(`p_mapa`, `delta_logit`, `n_a`, `n_b`, `peso`, `lambda`, `k`, `p_lo`, `p_hi`) o
`null` (lecturas 100% cacheadas con el motor en frío). Desde el contrato
2026-10, `p_mapa` **coincide con `prob_victoria_a`** cuando está presente (el
ESC ya es la P servida) y `esc_peso` es `peso` a nivel de fila; usar ambos en
lugar de asumir que el escenario está siempre. La web muestra `esc_peso` con
`p_lo–p_hi`/`n` en la tarjeta CONF. ESC y el `ESC` del selector/armador; si
`escenario_mapa`/`esc_peso` vienen `null` se oculta la confianza sin romper.

**Semántica de la banda de confianza** (la fija el backend, conservadora):
se calcula sobre `max(p, 1-p)` → `>=0.62` **alta**, `>=0.55` **media**, si no
**baja**. La web la muestra con color (alta=verde, media=ámbar, baja=gris) y, si
el dato no viene, lo **oculta o deriva** con la misma regla (no rompe).

Manejo de errores: timeout de 120 s (504 si expira) y 502 `{"ok": false, "error": "..."}`
si el servicio no responde. Este módulo **no importa** `numpy` ni `pandas`;
usa `backend.conexion.fetch_all` solo para la lectura directa de Turso
(cache-first), de modo que ver las predicciones no depende de ningún servicio.

#### Ciclo precomputar → asociar → leer → comparar

`match_id` es el id del partido de **vlr.gg** (ej. `753455`) y es el mismo para
todo el partido. La DB Turso es compartida con el servicio (ALETHEIA no crea
estas tablas). Endpoints adicionales del proxy:

- `GET /api/aletheia/modelo_version` → proxy de `GET {BASE}/api/modelo_version`.
  Devuelve `{"ok": true, "modelo_version": "<hash>", "fecha": "<iso>",
  "en_disco": "<hash>", "desactualizado": <bool>}`. `desactualizado:true` avisa
  de un reentreno/cambio pendiente de reiniciar el servicio; **EN VIVO** lo
  muestra como aviso ("reinicia ALETHEIA_PREDICT") sin bloquear la UI.
- `POST /api/aletheia/precalcular` → proxy de `POST {BASE}/api/precalcular`.
  Body: `{"equipo_a", "equipo_b", "n_sim": 10000, "match_id": 753455, "mapas": [...]?,
  "forzar": false}`. Calcula los 13 mapas × 2 lados (26 filas) y hace UPSERT en
  `predicciones_mapa` con ese `match_id`. Requiere `X-API-Key` (la añade el
  proxy).
  **Por defecto ASÍNCRONO:** responde `202` al instante con
  `{"ok", "job_id", "total": 26, "modelo_version", "progreso", "mapas_hechos"}`.
  El proxy espera hasta **180 s** (`TIMEOUT_PRECALCULAR`) a que el servicio
  responda, porque en frío el motor puede tardar 1-2 min en arrancar antes del
  `202`; el cómputo en sí no bloquea. Un **429** ("Ya hay un precálculo en
  curso") se reenvía tal cual (puede traer el `job_id` y `estado` del job
  activo).
  Progreso: `GET /api/aletheia/precalcular/estado?job_id=...` → proxy de
  `GET {BASE}/api/precalcular/estado` →
  `{"ok", "job": {"estado", "progreso", "mapas_hechos", "computados", "desde_cache", "error"}}`.
  El servicio lee del dict en memoria y, si el worker se recicló, cae a la
  copia persistida en la tabla **`precalculo_jobs`** (L16).
  Con `sync:true` corre inline y responde `{"ok", "total", "computados", "desde_cache", "tiempo_s"}`.
- `POST /api/aletheia/asociar` → proxy de `POST {BASE}/api/asociar`.
  Body: `{"equipo_a", "equipo_b", "match_id": 753455, "desde_match_id": 0}`.
  Reasigna el `match_id` de predicciones ya calculadas. Responde
  `{"ok": true, "filas_actualizadas": N, "match_id": 753455}`.
- `GET /api/aletheia/prediccion?match_id=753455&map_name=Split&lado_inicial_a=attack`
  (o `?equipo_a=&equipo_b=`) → proxy de `GET {BASE}/api/prediccion`. Lee la fila
  cacheada (instantáneo) con `prob_victoria_a/b` (**ESC por mapa/lado**),
  `esc_peso`, `marcadores`, `economia`, `analisis_mapa` y, si el servicio la
  aporta, `escenario_mapa`/`prob_intervalo`; en modo DB `escenario_mapa` y
  `prob_intervalo` van `null` (la UI usa ESC + `esc_peso`).
  Responde `{"ok": true, "prediccion": {...}, "modelo_version": "<hash>", "vigente": true}`
  o `404 {"ok": false, "error": "Sin predicción cacheada."}`.
- `GET /api/aletheia/predicciones?match_id=753455` (o `?equipo_a=&equipo_b=`) →
  proxy de `GET {BASE}/api/predicciones` (cada fila trae los mismos campos,
  incluidos `esc_peso` e `prob_intervalo`). La P del motor no está por fila: solo
  en la raíz de `/predecir` y `/serie`.
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
- `GET /api/aletheia/scorecard?match_id=...` → proxy de `GET {BASE}/api/scorecard`.
  **Scorecard de micro-eventos**: por mapa compara lo predicho con lo real →
  ganador (`map_accuracy`/`map_brier`/`map_log_loss`), overtime (`ot_brier`),
  marcador (`scoreline_prob_media`, `scoreline_top1_hit`) y economía (`eco_mae`,
  `cruce_mae`); `detalle[]` trae por mapa `economia` (por categoría/equipo) y
  `cruces` (por `cat_a_vs_cat_b`, con `n`). Une `/api/comparacion` y el scorecard
  en la sección **SCORECARD** del tab COMPARACIÓN.
- `GET /api/aletheia/scorecard_agregado` → proxy de `GET {BASE}/api/scorecard_agregado`.
  Scorecard **sumando todos los partidos jugados con predicción**: `resumen` +
  `por_categoria` y `por_cruce` (`n`, `pred_media`, `real_media`, `mae`).
- `GET /api/aletheia/dataset[?guardar=1]` → proxy de `GET {BASE}/api/dataset`.
  Dataset predicción↔resultado por mapa; `guardar=1` lo persiste en
  `data/dataset_entrenamiento.csv` (memoria de datos para reentrenar).
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
  serie **cache-aware**: reutiliza las filas de caché vigentes y **calcula y
  persiste** (mapa y serie) lo que falte; la segunda llamada idéntica sale de
  caché. Body:
  `{"match_id":753455,"equipo_a":...,"equipo_b":...,"mapas":[{map_name,lado_inicial_a},...]}`.
  `mapas` debe traer el **veto completo** (1/3/5); 2/4 → **400**. Respuesta: `{"ok":true,"formato":"bo3","mapas_para_ganar":2,"prob_motor_a":0.5343,"prob_motor_b":0.4657,"mapas":[{...,"prob_victoria_a/b":ESC por mapa/lado,"esc_peso":0.82,"fuente":"cache|calculado","economia":...,"analisis_mapa":...,"escenario_mapa":{...}|null,"prob_intervalo":[lo,hi]|null}],"prob_serie_a":...,"prob_serie_b":...,"confianza_serie":"...","modelo_version":"<hash>","n_sim":10000,"resultados_serie":{...},"caminos_serie":{...}}`.
  La **P(serie) sale de `prob_serie_a/b`** (motor + temperatura), nunca de un
  promedio/suma de los ESC por mapa. En modo DB `prob_motor_a/b` va `null` (no se
  persiste) y la UI lo oculta.
- `POST /api/aletheia/borrar` → proxy de `POST {BASE}/api/borrar`. Body:
  `{"equipo_a", "equipo_b", "match_id"}`. Borra las filas de `predicciones_mapa`
  y `predicciones_serie` de ese enfrentamiento (útil para duplicados o
  preparaciones erróneas). Responde `{"ok": true, "filas_borradas": N, "match_id": ...}`.
  Nota: el endpoint vive en ALETHEIA_PREDICT; ALETHEIA solo lo invoca por proxy
  (no borra directamente en Turso).

**Flujo del ciclo (frontend → proxy `/api/aletheia/...`; el navegador nunca
llama directo a ngrok y la clave API la añade el proxy server-side):**
1. **PREPARAR** (`/aletheia_preparar/`): `POST /api/aletheia/precalcular`
   con el id de vlr.gg (async con `job_id`; el POST responde `202` al instante).
   El polling `GET /api/aletheia/precalcular/estado?job_id=...` también va por el
   proxy. El resto (`equipos`, `modelo_version`, `predicciones`, `asociar`) usa
   el mismo proxy. Se guarda el `modelo_version`
   (viene en el `202`); se puede **ASOCIAR ID** y **re-preparar** con
   `forzar:true` si el modelo cambió.
2. **LISTAR/LEER** (`/aletheia/`, EN VIVO): `GET /api/aletheia/simulaciones` lista
   lo preparado (por defecto solo `vigente:true`); al elegir una se leen sus filas
   UNA vez con `GET /api/aletheia/predicciones` (caché). El navegador **nunca**
   llama a ngrok directo.
3. **MAPA/BANDO**: elegir mapa+lado muestra `prob_victoria_a/b`, `prob_overtime`,
   `confianza` y `prob_intervalo` (IC95%, si el rating trae RD) desde la caché
   local (solo consulta `/api/aletheia/prediccion` si falta el dato).
4. **ARMAR SERIE**: `POST /api/aletheia/serie` da `prob_serie_a/b`,
   `confianza_serie` y `mapas_para_ganar` al instante (cache-aware: reutiliza la
   caché y calcula/persiste lo que falte).
5. **COMPARACIÓN** (EN VIVO): `GET /api/aletheia/comparacion` contrasta lo predicho
   con el resultado real del mismo `match_id`.
6. **GESTIÓN** (EN VIVO): por enfrentamiento, **asignar/corregir ID**
   (`POST /api/aletheia/asociar`) y **borrar** duplicados o preparaciones erróneas
   (`POST /api/aletheia/borrar`).
7. **INVALIDACIÓN:** la web compara el `modelo_version` de las filas cacheadas con
   `GET /api/aletheia/modelo_version`; si difiere, esas filas quedan
   `vigente:false` y la web marca **RE-PREPARAR** (que envía `forzar:true`).

### 4.5. Módulo Partidos (`partidos_bp`) — componente `/partidos/`
- `GET /api/partidos`: lista paginada estilo vlr.gg. Query: `page`, `limit` (máx 200, def 60), `q` (equipo/sigla/torneo/fase), `torneo` (nombre exacto), `year`, `event_id`, `orden` (`recientes|antiguos`). Cada fila trae equipos con tag/país, `event_id`/`event_name` resueltos y `maps_played`.
- `GET /api/partidos/filtros`: `torneos` (30, con `event_id`, `n`, fechas) y `years` (2025/2026) para los selects.
- `GET /api/partidos/resultados?match_ids=753456,753461`: dado un lote de ids (máx 500, separados por comas) responde `{"ok": true, "con_resultado": [...]}` con los que ya tienen **resultado real** (al menos un mapa jugado en `maps`). Lo usa EN VIVO para separar predicciones pendientes de las ya comparables. Además devuelve `partidos` (por `match_id`, en la orientación de la predicción: `team_a`/`team_b` = `equipo_a`/`equipo_b` del motor) con la identidad de los equipos (ids/tags para los logos), el marcador real de la serie y el resumen **predicción vs realidad** usando la **P ESC por mapa/lado** (no hay `p_a` del motor): `p_real_media` (P ESC media que se dio a los ganadores reales), `favoritos_ok`/`n_mapas`, `mapas[]` (`map_name`, `gano_a`, `p_ganador`; el ESC se elige con el lado en que `equipo_a` empezó el mapa real, `side_top_start`/`team_top_id` de `rounds`) y `serie_mapas` (pool del veto en orden: picks + decider; es la lista correcta para `POST /serie` — con solo los mapas jugados, un bo3 terminado 2-0 le da al servicio una lista de 2 y devuelve una P incoherente). Funciona también para simulaciones **sin resultado** (solo identidad, leída de `predicciones_mapa`); si las tablas del servicio no existen (DB local vieja) se degrada a solo identidad/resultado.
- `GET /api/partido/<match_id>`: detalle completo → `partido` (header), `veto` (ordenado), `maps[]` y, anidado por mapa, `rounds[]` (timeline de rondas), `players[]` (scoreboard agregado de los 2 lados: K/D/A, rating, ACS, KAST, ADR, HS%, FK/FD) y `economy[]` (pistol/eco/semi-eco/semi-buy/full-buy). La `economy[]` se calcula **desde `rounds`** (excluye los pistols R1/R13 y resuelve el equipo con `team_top_id`/`team_bot_id`), con `fuente:'rounds'`; si un mapa no tiene rondas se conserva `economy_summary` etiquetado `fuente:'economy_summary'` (benchmark no fiable en eco, no comparable con el motor). `404` si no existe.

### 4.6. Módulo Equipos (`equipos_bp`) — componente `/equipos/`
- `GET /api/equipos`: equipos con `matches`, `wins`, fechas y `regiones` (para chips). Query: `q`, `region`. Solo equipos con partidos jugados.
- `GET /api/equipo/<team_id>`: `equipo` (info), `record` (V-D), `roster` (players con `team_id`), `transacciones` (roster_transactions, últimas 80), `partidos` (últimos 120 con evento), `jugadores` (promedios por jugador del equipo), `mapas` (jugados/ganados por mapa + avg rondas) y `eventos` (torneos jugados con récord).

### 4.7. Módulo Jugadores (`jugadores_bp`) — componente `/jugadores/`
- `GET /api/jugadores`: jugadores con stats (JOIN `player_stats`), nickname obligatorio y **paginado** (`page`, `limit` máx 200 def 60; responde `total`/`pages`). Query: `q` (nick/real/equipo), `orden` (`rating|acs|kd|matches|nombre`, allowlist). El frontend carga por lotes de 60 con botón **CARGAR MÁS**.
- `GET /api/jugador/<player_id>`: `jugador` (info + equipo actual), `totales`, `multikills` (k2..k5, v1..v5, plants, defuses), `agentes` (**una fila por agente con la ventana temporal más amplia** de `player_agent_stats` + `role` de `agents`), `partidos` (últimos 30 agrupados por partido con rival/resultado), `mapas` (rendimiento por mapa) y `equipos` (historial de `roster_transactions`).

### 4.8. Módulo Eventos (`eventos_bp`) — componente `/eventos/`
- `GET /api/eventos`: lista de **torneos** (no solo los 14 `events`) con `matches`, `teams`, fechas, `event_id`/`event_name` cuando existe alias. Query: `q`.
- `GET /api/evento?event_id=<id>` o `?torneo=<nombre>`: detalle. Devuelve `evento` (nombre, torneos/aliases, fechas, partidos, equipos), `partidos`, `equipos` (récord y mapas V-D), `mapas` (jugados, picks, bans, deciders; `atk_win_pct`/`def_win_pct` solo si hay `event_map_stats`) y `agentes` (pickrate por mapa desde `event_agent_pickrate`; si el torneo no tiene meta, se **calcula** la presencia % desde `player_stats`).

### 4.9. Módulo Media (`media_bp`) — logos y fotos por enlace (no es una página)
- **No guarda imágenes.** Resuelve el enlace directo desde vlr.gg y hace `302 redirect` para que el navegador cargue la imagen desde el CDN (`owcdn.net`). Solo se cachean **metadatos** en `media/urls_cache.json` (~100 bytes por entidad, regenerable): enlace (`u`), color medio (`c`), si es oscuro (`d`), si es **negro-sin-croma** (`bl`) y marca de color calculado (`cc`).
- **Color medio sin guardar la imagen:** para equipos y eventos se lee la imagen UNA vez **en memoria** (Pillow, máx 48×48, ignorando transparencia), se calcula el color medio, la luminancia y el ratio de píxeles negros, y se descarta. `d:true` (luminancia < **0.5**) marca logo oscuro; `bl:true` (ratio de píxeles con `max(R,G,B) < 70` y chroma `< 35` ≥ **85%**) marca logo **negro de verdad** → el frontend **invierte el logo** (`on-light` + `filter: invert(1) hue-rotate(180deg)`: el negro pasa a blanco y los tonos se conservan) para que se lea sobre el fondo oscuro, sin cambiar su tamaño. El watermark del banner también se invierte (`wm-inv-*`). No aplica a jugadores. Los colores guardan `cv` (`COLOR_VERSION`, hoy **3**): al subir la versión se recalculan solos en 2º plano (evita quedar con umbrales viejos).
- `GET /api/media/equipo/<team_id>`: si no hay enlace resuelto, baja **bajo demanda** la página `vlr.gg/team/<id>` (mismo id que `teams.team_id`), extrae la imagen de `team-header-logo` (fallback `og:image`) y redirige. `404` con `{"ok":false,"estado":"miss|busy|error"}` si no hay imagen.
- `GET /api/media/jugador/<player_id>`: igual para la foto (`player-header`, fallback `og:image`).
- `GET /api/media/evento/<event_id>`: igual para el logo del evento (`event-header`, fallback `og:image`).
- `GET /api/media/evento?nombre=<torneo>`: para torneos **sin `event_id`** (p. ej. *Valorant Champions 2026*). Busca el evento en `vlr.gg/search/?q=...`, extrae el primer resultado `/search/r/event/<id>/idx` + su thumbnail, y lo cachea por nombre (`n:<slug>`) y por id.
- `GET /api/media/meta?equipos=1,2&jugadores=4&eventos=2766&nombres=A|B`: devuelve **enlaces ya resueltos** (`u`) + color (`c`) + flag oscuro (`d`) + flag **negro-sin-croma** (`bl`, usado para la tarjeta clara de logos negros), sin bloquear la carga. El frontend apunta los `<img>` **directo al CDN** con esto (cero requests de imagen a este backend) y pinta colores/watermarks. Si la entidad no tiene imagen en origen devuelve `{"miss": true}` y el frontend se queda con siglas/iniciales **sin pedir la imagen** (nada de iconos rotos). **F5:** si la entidad no tiene entrada, responde `{"pending": true}` y la resuelve en un **hilo de fondo** (mismo semáforo 2 + throttle; `_resoluciones_en_proceso` evita duplicados): la primera carga no paga el scrape de 1,6 s; el frontend **reintenta solo las pendientes** (F6) y recibe el enlace o `miss`. Los enlaces cacheados sin color lo calculan también en un hilo de fondo (`cc` evita reintentos infinitos).
- **Sin imagen real:** vlr.gg usa rutas relativas para placeholders (`/img/base/ph/sil.png` en jugadores, `/img/vlr/tmp/vlr.png` en equipos) y `og:image` genérica (`vlr/card.png`); `_extraer_imagen` las descarta (solo acepta `http(s)`), así que se marcan como miss y el fallback es el monograma/iniciales. Verificado con precarga completa: equipos 70/79 (9 sin logo real), eventos 16/16, jugadores 524/688 (164 sin foto), **0 errores**.
- `GET /api/media/estado`: conteo de resueltas / sin imagen / con color por tipo y tamaño del JSON.
- **Reglas:** máximo 2 resoluciones simultáneas y 0.3 s entre requests a vlr.gg; los "sin imagen"/404 reales se marcan y no se reintentan por 24 h; los errores de red **no** se marcan (se reintenta en la próxima visita); el redirect se cachea 7 días en el navegador. Al guardar `urls_cache.json` se hace **merge por `t`** con lo que haya en disco, para que el servidor y `cachear_media.py` (u otro worker) nunca se pisen. **F5:** `urls_cache.json` se versiona (deja de estar gitignored) para que los enlaces sobrevivan a los deploys de Render; solo se ignora `urls_cache.json.tmp`.
- **Precarga opcional de enlaces y colores:** `python cachear_media.py --equipos|--jugadores|--eventos|--todo [--limite N] [--delay S]` (1 s entre resoluciones por defecto; también calcula el color medio de equipos/eventos). No es necesario: el sitio resuelve enlaces solo al mostrar cada imagen.
- El frontend usa el fallback si la imagen falla: **lozenge con siglas** del equipo (colores por hash del nombre) y **avatar con iniciales** del jugador. `VCT.imgError` reintenta una vez a los 3 s y luego quita la imagen.

---

## 5. Estructura y Reglas del Frontend

1. **Rutas Estáticas de Navegación (`backend/app.py`):**
   Las subcarpetas registradas en `FRONTEND_FOLDERS = ['inicio', 'tablas', 'visualizar', 'aletheia', 'aletheia_preparar', 'header', 'partidos', 'equipos', 'jugadores', 'eventos']` se sirven automáticamente en la raíz HTTP:
   - `/` → **sirve EN VIVO directo** (200, sin redirección desde F13; es la misma página que `/aletheia/`)
   - `/inicio/` o `/inicio/index.html` → **CARGAR DATOS** (ETL: subir Excel, INIT DB, log)
   - `/tablas/` o `/tablas/index.html`
   - `/visualizar/` o `/visualizar/index.html`
   - `/aletheia/` o `/aletheia/index.html` (**EN VIVO**)
   - `/aletheia_preparar/` o `/aletheia_preparar/index.html` (**PREPARAR**)
   - `/partidos/`, `/equipos/`, `/jugadores/`, `/eventos/` → **componentes VCT** (lista + detalle en la misma página vía query param: `?match=`, `?team=`, `?player=`, `?event=`/`?torneo=`)
   - `/header/` o `/header/index.html` (demo del componente header)
   - `comun/theme.css`, `header/header.css`, `header/header.js`, `header/header-nodes.js`,
     `comun/vct.css` y `comun/vct.js` se sirven como estáticos desde la raíz (`static_url_path=''`).
     Desde F7/F12 los HTML enlazan sus versiones `.min.*` con `?v=<hash>` (ver arriba).
   - **Caché de estáticos (F7, `_cache_estaticos`):** CSS/JS con `?v=` → `max-age=31536000,
     immutable`; CSS/JS sin `?v=` → 300 s; imágenes/fuentes (`.avif`, `.svg`, `.png`…) →
     30 días sin `immutable`; el HTML (`<folder>/` y `/`) sale `no-cache, max-age=0`.
   - **Sin 308 de barra final (F13):** la regla `/<folder>/` usa `strict_slashes=False`, así
     `/aletheia`, `/partidos`, etc. responden 200 directo (antes Werkzeug hacía 308).
   - **Ojo con los archivos sueltos en la raíz:** un archivo de un solo segmento
     (`/ALETHEIA_ico.svg`) puede caer en la ruta de carpeta/404. Por eso el ico/logo se
     sirve desde `comun/ALETHEIA_ico.svg` y todos los `<link rel="icon">`/`<img>` apuntan ahí.
   > Los módulos `predecir/` y `exportar/` **ya no existen**.
   > `comun/` **no es una página**: es el core visual compartido (CSS `v-*` + objeto JS global `VCT`); no está en `FRONTEND_FOLDERS` y no debe registrarse blueprint.

   > **Regla de enlaces (estética de URL):** todos los enlaces de navegación entre
   > páginas deben apuntar a **`/componente/`** (relativo `../componente/` o `./`
   > para la misma página), **nunca** a `/componente/index.html`. El `index.html`
   > existe solo para que Flask sirva la carpeta; no debe verse en la barra del
   > navegador. Ejemplos: `../equipos/?team=120`, `../partidos/?match=753444`,
   > `?player=3885` (misma página), `./` (volver a la lista). Los `<link>`/`<script>`
   > a CSS/JS sí usan la ruta de archivo normal (`../comun/vct.js`).

2. **Configuración de Host API Dinámico:**
   En todos los archivos JavaScript del frontend (`script.js`), la variable `API` está configurada como:
   ```javascript
   const API = `${window.location.origin}/api`;
   ```
   Esto garantiza que las peticiones se dirijan correctamente al mismo host tanto en entornos locales (`http://localhost:5000/api`) como en producción en Render (`https://tu-app.onrender.com/api`).

3. **Servicio ALETHEIA_PREDICT (ngrok) y las DOS páginas (`aletheia/` y `aletheia_preparar/`):**
   - El predictor externo corre en el PC del autor y se expone con ngrok; la URL
     llega al proxy por `ALETHEIA_PREDICT_URL`. **Todas** las llamadas del
     navegador van por el proxy `/api/aletheia/...` (helper `proxyFetch`). El
     header `ngrok-skip-browser-warning` y la clave `X-API-Key` los añade
     `aletheia/aletheia.py` **server-side** (`_request_service`); la clave nunca
     llega al HTML/JS.
   - **Lecturas sin PC ni servicio:** el proxy sirve `mapas`, `modelo_version`,
     `simulaciones`, `prediccion`/`predicciones` y `serie` (si la fila cacheada
     coincide) **directo de Turso** (`fetch_all`); EN VIVO lista y abre las
     predicciones preparadas con el PC apagado. Solo lo no cacheado/derivado
     (`equipos`, `comparacion`, `scorecard`, `dataset`, `predecir`, serie con
     otros mapas) va a `ALETHEIA_PREDICT_READ_URL` (Render, con reintento por
     cold start y caída a ngrok); `forzar`/`precalcular` siguen pidiendo el PC.
   - `POST /api/aletheia/precalcular` (async: responde `202` al instante) y su
     polling `GET /api/aletheia/precalcular/estado` van por el proxy igual que
     `equipos`, `comparacion`, `scorecard`, `simulaciones`, `asociar` y
     `borrar`; así no chocan con el timeout de gunicorn/Render.
   - **EN VIVO** (`/aletheia/`) lee todo de la caché a través del proxy (Turso
     directo; el servicio solo si falta el dato).
    - **`/aletheia_preparar/` — PREPARAR:** selección de equipos (search+grids),
      selector de simulaciones (1K/5K/10K; **sin 25K/50K en hosting free**), campo de ID vlr.gg (parsea URL o
      número), **PREPARAR PARTIDO** (async: `POST /api/aletheia/precalcular` → `job_id`
      → poll `/api/aletheia/precalcular/estado`, con % y tiempo transcurrido),
      **ASOCIAR ID** y badges de caché ("ya predicho") y de `modelo_version` ("si
      cambia el modelo" marca RE-PREPARAR; si el servicio trae `desactualizado:true`
      avisa de reiniciarlo). Botón **"IR A EN VIVO →"**.
      - El **poll es resiliente**: ante cortes del túnel/PC (p. ej.
        `ERR_PROXY_CONNECTION_FAILED`) reintenta con backoff en vez de abortar, y
        ofrece **"cancelar espera"**. Se usa un favicon inline para evitar el 404 de
        `/favicon.ico`. Sondea cada **2-3 s** y la **barra de progreso usa
        `job.progreso` (0..1)**; NO `mapas_hechos/total` (las filas son mapas × 2).
      - **Anti-doble-envío y reanudación:** el botón se deshabilita mientras el
        POST está en vuelo (timeout de 190 s; el motor en frío tarda 1-2 min) y el
        job se **persiste por enfrentamiento** (`match_id`+equipos) en
        `localStorage` (`ae_precalcular_jobs`, TTL 24 h). Al **recargar** la
        página se restaura el enfrentamiento y se **reanuda el polling** de ese
        `job_id` sin volver a POSTear; una segunda pestaña hace lo mismo. Si un
        timeout deja dudas, se hace **un único reintento** (solo si no hay job
        conocido) para descubrir/adoptar el job activo.
      - **429 "ya hay un precálculo en curso" nunca es error fatal:** si la
        respuesta trae `job_id` se adopta y se pollea; si no, se sigue el job
        guardado; si no hay ninguno, se informa y se ofrece **reintentar** sin
        perder la selección de la UI.
      - **Caché y `forzar`:** por defecto se envía `forzar:false` para reutilizar
        caché válida (solo `RE-PREPARAR` fuerza). El contador "en caché" cuenta
        **solo filas vigentes** (`modelo_version` actual y `n_sim` >= el
        solicitado), para que coincida con lo que el job reutiliza.
      - Al terminar (`listo`) se recargan las predicciones y se habilita el
        botón; si `estado=error` se muestra el campo `error` y se permite
        reintentar.
   - **`/aletheia/` — EN VIVO (nunca simula):**
      - Al cargar, `GET /api/aletheia/simulaciones` pinta la lista de preparadas
        (`EQUIPO_A vs EQUIPO_B · #match_id · N mapas · n_sim · [vigente]`); por
        defecto solo `vigente:true`, con toggle "mostrar no vigentes" (marcadas
        "re-preparar").
      - **Filtro por resultado real:** chips `TODAS / SIN RESULTADO / CON
        RESULTADO` (con conteo) y badge por fila (`PENDIENTE` ámbar / `CON
        RESULTADO` verde). El estado y la identidad de los equipos (ids/tags) y
        el resumen predicción↔realidad salen de
        `GET /api/partidos/resultados?match_ids=...` (DB propia); la elección se
        recuerda en `localStorage` (`ae_sim_filtro`). Si el endpoint falla, el
        filtro avisa y no oculta nada (todo cuenta como pendiente).
      - **Logos e identidad:** la página carga el core `comun/` (`vct.css` +
        `vct.js`) y pinta **logo + nombre** de cada equipo con `VCT.lozenge` en
        la lista y en la cabecera del enfrentamiento seleccionado
        (`VCT.aplicarMedia` apunta las imágenes al CDN/`/api/media/...`). Si el
        partido no tiene id/identidad resuelta, cae a las siglas (sin imagen
        rota).
      - **Escala verde/naranja/rojo (predicción vs realidad):** por enfrentamiento
        con resultado real, la fila muestra, en este orden: `MOTOR p(A)/p(B)` (P
        del **motor Glicko**, plana; se lee de la **raíz de `POST /serie`**, no de
        la caché por mapa, y se actualiza en 2º plano), el marcador real `x-y`
        (ganador resaltado), la
        píldora **`SERIE P%` + ✓/✕** (P del motor al ganador real de la serie,
        calculada en 2º plano con `POST /serie` usando `serie_mapas`; si no hay
        pool completo 1/3/5 se pinta **`SERIE sin pool`** y no se llama al
        servicio; **verde ≥62%, naranja ≥55%, rojo <55%**), la píldora
        **`MAPAS x/y (%)`**
        (acierto del favorito **ESC** por mapa: **verde ≥67%, naranja ≥50%, rojo <50%**),
        `P(MAPA)` (P media **ESC** al ganador real por mapa, color por banda; es
        calibración, **no** la tasa de acierto) y puntos coloreados por mapa
        (`title` = mapa + P real + favorito/upset). **Cola de `/serie` (F1):** la P
        de serie se pide una vez por `match_id` y solo para las filas **visibles**
        (IntersectionObserver con 200 px de margen) o la **seleccionada**, nunca
        para `stale`/sin resultado; como máximo **2 peticiones en vuelo**
        (`SERIE_CONCURRENCY`), el resultado queda cacheado por sesión
        (`simSerie` + `serieIntentados`) y **no se relanza en cada
        `renderSimList`**. Al hacer scroll se encolan las nuevas visibles. Antes
        se lanzaban hasta 12 POST secuenciales en cada render. La píldora se
        actualiza en sitio. `prob_victoria_a` (ESC) ya **no** se usa como MOTOR;
        los `match_id` sin resultado no piden serie y no muestran MOTOR. El acento
        está en el **acierto** (✓/✕ y `MAPAS`): en
        Valorant la P ronda 50-60% incluso acertando, así que `P(MAPA)`
        y `SERIE` bajos no implican mal motor.
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
         único de los mapas del pool (`map_pool.en_pool=1`; si la tabla no existe,
         los 13) (agrega mapas a la serie; clic = añadir + ver análisis),
        los **slots en orden** con bando por mapa, y el botón **ARMAR SERIE**. Al
        pulsar el botón se calcula la serie; cambiar formato/mapas/lado **invalida** el
        banner (hay que volver a pulsar). Clic en un mapa del grid o en un slot muestra
        su análisis a la izquierda, sin subir/bajar.
      - **Leyenda:** `ESC` = predicción por mapa/lado (la que se sirve; puede
        variar por mapa y lado) · `MOTOR` = P del motor Glicko (una sola por
        enfrentamiento, plana; decide la serie) · `hist` = análisis histórico del
        mapa.
      - **Explorador de mapas:** rejilla de los mapas del pool activo
        (`map_pool.en_pool=1`; si la tabla no existe, los 13) con el **ESC por
        mapa/lado** (`prob_victoria_a`, varía por mapa y lado), `conf` (`esc_peso`)
        y OT del bando elegido (leídas de `liveBulk`; no llama al servicio en cada
        clic). Al tocar un mapa muestra las tarjetas **ESC A/B** (número grande =
        `prob_victoria_a/b` del lado elegido; **no se promedian lados**), el
        **MOTOR plano** como sub-línea (raíz de `/serie`, idéntico en todas las
        tarjetas; oculto si aún no se armó la serie), `prob_overtime`, `n_sim`, la
        tarjeta **CONF. ESC** (`esc_peso` con `p_lo–p_hi`/`n_a`/`n_b`/Δlogit si
        `escenario_mapa` está presente; se oculta si no hay ni peso ni escenario) y
        una **nota de análisis** con el **historial de cada equipo en ese mapa**
        (`analisis_mapa.equipo_a/b`: `winrate`, `n`).
      - **escenario_mapa/esc_peso null:** no es error. Se usa `prob_victoria_a`
        como ESC y se oculta la confianza; el escenario y el IC solo se pintan si
        vienen.
      - **DISTRIBUCIÓN DE MARCADOR:** bajo las tarjetas se muestra el **marcador
        más probable** (etiquetado como *estimación, no resultado seguro*) y el
        **top-3** con su % (`marcadores[]` viene del backend ordenado por prob desc;
        `marcador_a` = goles de `equipo_a`). Si `marcadores` falta (fila de caché
        anterior al cambio), el bloque se **oculta** y se marca el enfrentamiento
        para **RE-PRECALCULAR**.
      - **ECONOMÍA / RONDAS (micro-eventos):** bloque `economia-block` bajo el
        marcador. Por mapa y bando muestra: win rate por categoría
        (eco/semi-eco/semi-buy/full-buy) de **cada equipo** con barra y `n`
        (`n` = rondas simuladas de esa categoría), **pistol** (P de que gane
        `equipo_a`), y la **mini-matriz de cruces** 4×4 (`cat_a_vs_cat_b`, prob.
        de que `equipo_a` gane la ronda; filas = `equipo_a`, columnas =
        `equipo_b`), destacando `semi_buy_vs_full_buy` y `eco_vs_full_buy` (con
        `n` visible en las celdas destacadas). La nota del bloque usa
        `economia.semantica` (con fallback): **sin pistols R1/R13**; el bloque
        PISTOL va aparte (`pistol_semantica`). Si `economia` falta, el bloque se
        oculta y se marca **RE-PRECALCULAR** junto con `marcadores` (helper
        `detalleFaltante`).
      - **ARMAR SERIE (BO1/BO3/BO5)** *(sección dentro de `MAPA / SERIE`):* slots en
        orden (el último = DECIDER) con bando por mapa (cada slot muestra su
        **ESC A/B** y `conf` = `esc_peso`); al pulsar **ARMAR SERIE**
        se hace `POST /api/aletheia/serie` y se muestra el banner (`prob_serie_a/b`,
        `prob_motor_a/b` —el MOTOR plano—, `confianza_serie` con color, formato,
        `mapas_para_ganar`, `n_sim`) al
        instante; cambiar formato/mapas/lado **invalida** el banner ("Cambió la
        serie · pulsa ARMAR SERIE"). **Veto completo:** con 2/4 mapas no hay POST: el botón se
        deshabilita, el formato se muestra como incompleto y la nota dice cuántos
        mapas faltan (la API exige 1/3/5). La tabla de mapas de la serie muestra
        el **ESC por mapa/lado** (`prob_victoria_a/b`), `conf` (`esc_peso`), el
        **marcador más probable** y, si viene, el IC de `escenario_mapa`
        (`p_lo–p_hi`). El banner incluye además
        la **distribución de la serie** (`resultados_serie`: 2-0/2-1/1-2/0-2,
        ordenada por prob desc) y los **caminos de la serie** (`caminos_serie`:
        la secuencia mapa a mapa, p. ej. `V-D-D` vs `D-V-D` para un 1-2; ✓ gana A,
        ✗ gana B). Es esperado que un mapa favorezca al rival y la serie al otro:
        el global manda el MOTOR.
      - **DESCARGAR ANÁLISIS (.md):** botón que genera y descarga un `.md` con
        **(1) el prompt general** para un LLM, **(2) los MERCADOS** precalculados
        (ganador de serie, MOTOR plano, total de mapas Más/Menos, marcador exacto
        de serie, total de rondas por mapa derivado de la distribución de
        marcadores, y pistol), **(3) TODOS los datos** en JSON (por mapa×lado:
        `esc_p_a`/`esc_p_b` (ESC), `esc_peso`, OT, marcador, `total_rondas`,
        `pistol`, economía por categoría y cruce, `escenario_mapa` y
        `analisis_mapa`; y la serie: `motor_p_a/b`, `prob_serie_a/b`,
        `resultados_serie`, `caminos_serie`) y **(4) un apartado de notas** para
        contexto de los equipos. Se arma con el `liveBulk` + la última serie
        (conviene pulsar ARMAR SERIE antes).
       - **VIGENCIA Y RE-PRECALCULAR:** la web compara `modelo_version` de cada
         enfrentamiento con `GET /api/aletheia/modelo_version` (y usa `vigente` de
         `/api/simulaciones`); si el servicio trae `desactualizado:true` muestra el
         aviso de reinicio. Si el modelo difiere, o si las filas no traen
         `marcadores`/`economia`, marca el enfrentamiento como **RE-PRECALCULAR** y
         ofrece **↻ RE-PRECALCULAR**, que hace `POST /api/aletheia/precalcular` con
         `forzar:true` **por el proxy** (async: `job_id` + polling de
         `/api/aletheia/precalcular/estado`), refresca la lista y vuelve a leer la caché.
         Comparte el job persistido por enfrentamiento con PREPARAR: un **429**
         ("ya hay un precálculo en curso") se adopta como job activo (o se
         reanuda el guardado) en vez de mostrarse como error fatal, y el POST
         tiene timeout largo (190 s) para no duplicar peticiones.
      - **COMPARACIÓN:** `GET /api/aletheia/comparacion?match_id=..` muestra
        tarjetas resumen (accuracy, brier, log-loss, favoritos_ok, upsets,
        inciertos) — `ACCURACY` con la escala verde/naranja/rojo (≥67/≥50/<50) —
        y una tabla de detalle coloreada por fila (verde = favorito
        ganó, rojo = upset, ámbar = incierto) con columnas `MARCADOR` (rondas
        reales del mapa) y `P(REAL)` = P que el motor dio al ganador real del
        mapa, con la **escala verde (≥62%) / naranja (≥55%) / rojo (<55%)** y su
        leyenda; si el partido no está en la DB: "sin resultado real todavía".
        Sobre la tabla, el banner **SERIE · PREDICHO VS REAL** (`POST
        /api/aletheia/serie` con el **pool del veto**, `serie_mapas`) muestra la
        P de serie del motor para ambos equipos, el marcador real `x-y` y
        `P(REAL)` coloreada (✓ favorito / ✕ upset); si no hay pool completo
        (1/3/5) no llama al servicio y lo dice. Debajo se agrega el
        **SCORECARD**
        (`GET /api/aletheia/scorecard`): tarjetas de micro-eventos (MAP
        ACCURACY/BRIER, OT BRIER, MARCADOR TOP-1, ECO MAE, CRUCE MAE) y una
        tabla por mapa con marcador real, `P(A)`, ganador, OT (pred/real),
        probabilidad del marcador real y MAE de economía/cruces.
        Además, botones **SCORECARD AGREGADO** (`GET /api/aletheia/scorecard_agregado`:
        suma todos los partidos → tablas `por_categoria` y `por_cruce` con
        pred/real/MAE) y **EXPORTAR DATASET** (`GET /api/aletheia/dataset?guardar=1`:
        guarda `data/dataset_entrenamiento.csv` en el servidor de predicción).
     - Botón **"← PREPARAR PARTIDO"**.
   - Servicio apagado: cada llamada se maneja con avisos, sin romper la página.

4. **Sistema de Diseño Visual (`comun/theme.css`):**
   La **única** definición de la paleta vive en `comun/theme.css` (`:root`); ese
   archivo se enlaza en el `<head>` de **todas** las páginas antes que cualquier
   otro CSS. El resto de CSS usa sus variables (o los alias heredados) y **no**
   colores literales.

   - **Paleta:** `--bg #0A0A0C`, `--panel #222228`, `--panel-v #1C2412`
     (verde muy oscuro, uso puntual), verdes `--g1 #4C5C2D` / `--g2 #788428` /
     `--g3 #B0C138`, acento `--accent #E8FF47`, textos `--txt-2 #B8BCA8`,
     `--txt #D4D4D8`, títulos `--txt-1 #F4F2E6`.
   - **Contenido neutro:** superficies y bordes son negros/grises derivados
     (`--surface`, `--surface2`, `--surface3`, `--panel-hover`, `--border`,
     `--border-soft`). El verde **no** se usa en bordes ni chrome.
   - **Escala semántica de datos:** `--ok` (verde), `--mid` (naranja `#FB923C`),
     `--bad` (rojo `#FF4757`), con `--warn` = naranja, para porcentajes de
     predicción, acierto, victoria/derrota, upset y errores. Naranja y rojo son
     la excepción acordada a la paleta (solo datos).
   - **Header lima:** `.ae-header` en `--accent` plano, 80px de alto
     (`--ae-header-h`, compartida con `.vct-tabs` y el layout de `tablas/`),
     logo con copia agrandada casi transparente detrás (marca de agua,
     `opacity: .13`, sin blur) y texto oscuro.
   - **Animación de nodos:** `header/header-nodes.js` (ver §5.5).
   - Tipografías principales desde Google Fonts (se mantienen):
     - Titulares y Badges: `'Bebas Neue', sans-serif`
     - Textos, Tablas y Métricas: `'DM Mono', monospace`
   - **Favicon:** todas las páginas enlazan `<link rel="icon" type="image/svg+xml"
     href="../comun/ALETHEIA_ico.svg" />`.

5. **Componente HEADER reutilizable (`header/`):**
   La cabecera de navegación ya **no se duplica** en cada `index.html`: vive en
   `header/` y se inyecta en las páginas reales.

   - **Inclusión** en el `<head>` de la página:
     ```html
     <link rel="stylesheet" href="../comun/theme.css" />
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
     <script src="../header/header-nodes.js"></script>
     ```
   - **Aspecto:** fondo `--accent` plano (amarillo lima), 80px de alto
     (`--ae-header-h`), logo `comun/ALETHEIA_ico.svg` a 56px con una copia
     agrandada casi transparente detrás (marca de agua, sin difuminar) y el
     wordmark en segundo plano. Todos los controles del header usan texto
     oscuro sobre el lima.
   - **Botones del nav:** chips **angulares** (esquina superior derecha cortada
     con `clip-path`), mono en mayúsculas con tracking amplio, padding amplio
     (`11px 24px`) y tinte oscuro sutil en reposo. En hover/activo el chip se
     vuelve oscuro y se enciende el neón: **barra de acento** a la izquierda,
     `box-shadow: inset` (brillo interior + rim superior + línea inferior en el
     activo), `text-shadow` lima en dos capas y un **barrido de luz diagonal**
     (`::after` con `skewX`) al pasar el cursor. El glow exterior usa
     `filter: drop-shadow` (un `box-shadow` exterior quedaría recortado por el
     `clip-path`); el interior sí usa `inset`, que se ve.
   - **`header-nodes.js` (efecto de nodos reutilizable):**
      - Monta un `<canvas>` detrás del contenido del header (nodos verde oscuro
        que se funden a negro cerca del cursor) y un canvas fijo de fondo en
        toda la página (`body > .ae-nodes-bg`), visible (opacidad .9), con nodos
        verdes `--g2`/`--g3` (halo suave para que se lean sobre el fondo negro),
        líneas tenues y encendido verde al pasar el cursor.
      - Nodos 44–105 en el header y **36–140 en el fondo** (`W/14` y `W/12`)
        según el ancho; **rebotan en los bordes reflejando también la deriva
        base** (nunca se quedan pegados a la pared); líneas solo entre nodos
        cercanos con opacidad decreciente; repulsión suave con easing en el
        header y en el fondo. **F2:** el fondo usa `dprMax 1.25` (menos píxeles
        por frame) y los enlaces se calculan con **rejilla espacial** (no O(n²));
        `.ae-nodes-bg` lleva `contain: strict`. **Neón:** cada nodo tiene un
        **pulso de tamaño muy leve** (±10–14 %, fase propia) y, en el fondo, un
        glow sutil con composición `lighter`, halo doble y núcleo casi blanco
        (sin `shadowBlur`, para no encarecer el frame).
     - `requestAnimationFrame`, `devicePixelRatio`, `ResizeObserver`, pausa
       con la pestaña oculta y `prefers-reduced-motion` (nodos estáticos).
     - Se puede desactivar el fondo por página con `window.AE_NODES_BG = false;`
       antes de cargar el script. Sin librerías; los colores se leen de las
       variables de `comun/theme.css`.
   - `header.js` construye el nav (`EN VIVO`, `PREPARAR PARTIDO`, `VCT`, `TABLAS`,
     `VISUALIZAR`, `CARGAR DATOS`), resuelve las rutas relativas a la raíz y
     **marca activa** la página actual según `window.location.pathname`.
     La entrada `VCT` apunta a `/partidos/` y queda activa en los 4 componentes
     VCT (`partidos`, `equipos`, `jugadores`, `eventos`).
   - Config opcional `window.AE_HEADER`:
     - `hidden`: array de ids (`'aletheia'`, `'preparar'`, `'vct'`, `'tablas'`,
       `'visualizar'`, `'datos'`) para ocultar entradas concretas del nav.
     - El antiguo título/badge central se **eliminó**: el header solo muestra el
       logo y el nav (el estado activo ya marca la página).
   - Todas las clases del componente usan prefijo `ae-` (`.ae-header`, `.ae-nav`,
     `.ae-btn`, `.ae-logo`, `.ae-page-title`…) para no colisionar con los estilos
     propios de cada componente.
   - `header/index.html` es solo una **demo/preview** del componente.
   - **Páginas que lo usan:** `aletheia/`, `aletheia_preparar/`, `tablas/`,
     `visualizar/`, `inicio/` (CARGAR DATOS), `partidos/`, `equipos/`,
     `jugadores/` y `eventos/`.
   - El componente `inicio/` añade su propia barra `.db-bar` bajo el header con
     el estado de la DB y el botón **INIT DB** (son específicos de CARGAR DATOS,
     no del header compartido).

6. **Componentes VCT (`partidos/`, `equipos/`, `jugadores/`, `eventos/`):**
   Navegación visual estilo vlr.gg (densa, oscura, sin depender de tablas raw).
   - Cada página incluye, bajo el header, una **sub-navegación propia**
     `.vct-tabs` (PARTIDOS · EQUIPOS · JUGADORES · EVENTOS) con enlaces
     relativos entre componentes.
   - **Lista + detalle en la misma página** mediante query param
     (`?match=753444`, `?team=120`, `?player=3885`, `?event=2977` o
     `?torneo=Valorant%20Champions%202026`). El detalle se pinta en
     `#vistaDetalle` y oculta `#vistaLista`; "← VOLVER" es un enlace normal
     (sin history API) para que el botón atrás del navegador funcione.
   - **Core compartido `comun/`:** `vct.css` define tokens y clases `v-*`
     (filas de partido, banners, tabs internas, scoreboards, barras, chips);
     `vct.js` expone el global `VCT` (`VCT.api`, `VCT.matchRow`,
     `VCT.renderMatchList`, `VCT.lozenge`, `VCT.flag`, `VCT.tabs`,
     `VCT.bindLinks`, formateadores…). Las páginas cargan
     `../comun/vct.css` + `../comun/vct.js` **antes** de su `script.js`.
   - `partidos/`: lista con filtros (búsqueda, torneo, año, orden) + paginación;
     detalle con veto, tabs por mapa, timeline de rondas (color = equipo,
     tooltip = tipo), economía por categoría y scoreboard por equipo.
   - `equipos/`: grid con récord/winrate; detalle con tabs RESUMEN / ROSTER /
     PARTIDOS / ESTADÍSTICAS.
   - `jugadores/`: tabla ordenable (server-side) y detalle con tabs AGENTES /
     PARTIDOS / MAPAS / EQUIPOS (la tabla de agentes usa la ventana temporal
     más amplia de `player_agent_stats`).
   - `eventos/`: lista de torneos (con `event_id` cuando existe) y detalle con
     tabs PARTIDOS / EQUIPOS / MAPAS / AGENTES.
   - **Imágenes (PNG transparentes, sin caja):** los lozenges de equipo,
     avatares y logos de evento usan `VCT.lozenge(name, tag, cls, teamId, prioridad)`,
     `VCT.avatar(playerId, nickname, cls, prioridad)` y
     `VCT.eventLogo(eventId, name, cls, prioridad)`. Desde **F8** cada uno pinta
     **una sola** `<img>` (`*-fg`) con `object-fit: contain` (nunca recorta;
     antes eran dos `<img>` fg+bg con la misma URL: doble decodificación/pintura).
     **Sin glow**: el logo se muestra tal cual (los halos amplificaban los
     logos brillantes y hacían "caja" en los oscuros). La clase `on-light`
     (flag `bl` del backend) **invierte** (`filter: invert(1)
     hue-rotate(180deg)`, conservando los tonos) solo los logos
     mayoritariamente **negros sin croma**, para que se lean sobre el fondo
     oscuro.
     `prioridad=true` (primera fila de EN VIVO) usa `loading="eager"` +
     `fetchpriority="high"` para el LCP. Fallback: siglas/iniciales/monograma
     (`VCT.imgError`). Los agentes (`VCT.agentIcon`) y mapas (`VCT.mapIcon`)
     usan los `.avif` locales de `multimedia/agents/` y `multimedia/maps/`.
     Tamaños: lozenge 42px (`md` 60, `big` 104), avatar 56px (`sm` 36, `big` 148),
     elogo 64px (`big` 116), agente 34×44px, mapa 40×23px (`big` 96×54).
   - **Carga de imágenes (1 solo request por render):** los `<img>` se pintan sin
     `src` con `data-media="equipo:120|jugador:4|evento:2766|nombre:<torneo>"`
     (y `data-fallback="/api/media/..."`). `VCT.aplicarMedia(root)` pide
     `/api/media/meta` una vez por render y:
     1. apunta los `<img>` **directo al CDN** (`u`) — cero requests de imagen al backend;
     2. si el backend responde `pending` (F5) **no** dispara el fallback: espera
        el resultado del hilo de fondo y reintenta solo esa entidad;
     3. setea `--c` (color medio), `--c2` (rival, para el gradiente VS), `on-light`
        cuando el logo es oscuro, y `--wm-a`/`--wm-b` (watermarks);
     4. reintenta hasta 3 veces (3.5 s) **solo las entidades pendientes** (F6),
        nunca el lote completo.
     Nunca se usan colores aleatorios. Los `matchRow` llevan
     `data-c-equipo`/`data-c-equipo2` para el degradado A→B de cada VS.
   - **`tablas/` no se toca**: sigue siendo el explorador raw; los componentes
     VCT son la vista "bonita" sobre los mismos datos.

---

## 6. Configuración de Despliegue (Render & Gunicorn)

- **Entrypoint:** `wsgi.py` carga la instancia `app` de Flask desde `backend/app.py`.
- **Servidor WSGI:** `gunicorn wsgi:app`
- **Comando de Build:** `pip install -r requirements.txt`
- **Archivo de Configuración:** `render.yaml` declara el servicio web Python con las variables de entorno necesarias para la conexión remota a Turso.
- **Assets del deploy (F12):** los `*.min.css`/`*.min.js` se **versionan** para que
  Render los sirva con el HTML ya enlazado; si se editan los fuentes, correr
  `py tools/minificar_assets.py` **antes** de commitear (regenera min + `?v=` de
  todos los HTML). No hace falta en el build de Render (no hay paso de build JS).
- **Caché de media persistente (F5):** `media/urls_cache.json` se versiona (ya no
  está en `.gitignore`): así el FS efímero de Render arranca con enlaces resueltos
  y sin scraping de vlr.gg en la primera visita.
- **Variables de Entorno:**
  - `TURSO_DATABASE_URL` / `TURSO_AUTH_TOKEN`: conexión a la base de datos Turso.
    El **token no se versiona**: `render.yaml` lo declara con `sync: false`, se
    fija en el panel de Render (mismo patrón que el `render.yaml` de Predict) y
    `backend/conexion.py` no tiene valor por defecto (lo lee del entorno; en
    local `wsgi.py` carga `.env`, que está gitignored). Si se filtró alguna vez,
    hay que **rotarlo** en Turso y actualizar el panel.
  - `ALETHEIA_PREDICT_URL`: URL base del servicio externo **ALETHEIA_PREDICT**
    (PC/ngrok: cómputo pesado y mutaciones). En local se define en el archivo
    `.env` (`http://localhost:8000`); en Render se declara en `render.yaml`.
    El módulo `aletheia/aletheia.py` actúa como proxy hacia esta URL.
  - `ALETHEIA_PREDICT_READ_URL`: URL del **servicio de lectura** cache-first en
    Render (siempre disponible). El proxy manda ahí lo que **no** puede servir
    de Turso (`equipos`, `comparacion`, `scorecard`, `dataset`, `predecir` y
    serie no cacheada) y solo cae a `ALETHEIA_PREDICT_URL` si no responde. Si
    no se define, esas llamadas van a `ALETHEIA_PREDICT_URL` (comportamiento
    anterior). En `render.yaml` de la web apunta a
    `https://aletheia-predict.onrender.com`.
  - `ALETHEIA_API_KEY`: clave compartida con ALETHEIA_PREDICT para los endpoints
    admin/mutantes. Se define en el `.env` local (web y Predict con la **misma**
    clave) y en el panel de Render (secreto, `sync: false`). El proxy la añade
    server-side; nunca se expone al navegador.
  - `ALETHEIA_ESCENARIO_MAPA`: **obsoleta en la web desde el contrato 2026-10**
    (la capa ESC ya viene persistida en `prob_victoria_a`/`esc_peso`; el proxy
    ya no replica `escenario_mapa` en modo DB). El flag sigue siendo del
    servicio ALETHEIA_PREDICT para devolver o no `escenario_mapa`.

---

## 7. Instrucciones para la Asistencia de IA

Al recibir una nueva tarea o solicitud de cambio:
1. **Revisa este documento** para ubicar el archivo, blueprint o tabla involucrada.
2. **Realiza modificaciones quirúrgicas** enfocadas únicamente en los archivos relevantes.
3. **Mantén las firmas de API**, la estructura dinámica de `window.location.origin` y la compatibilidad con el esquema de base de datos descrito arriba.
4. **No dupliques el header**: usa el componente `header/` (`#aeHeaderMount` + `header.js`).
5. **Recuerda los módulos eliminados**: `predecir/` y `exportar/` no existen; no los referencies.
6. **Prioriza siempre la tasa de acierto** (principio rector, §1): ningún cambio debe degradar la precisión de las predicciones. Si un cambio la empeora, descártalo o revíerte.
7. **Para vistas nuevas del estilo VCT**: reutiliza el core `comun/` (`vct.css` + `vct.js`) en lugar de duplicar estilos o helpers; agrega los endpoints en el blueprint del componente correspondiente y registra la carpeta en `FRONTEND_FOLDERS` si es una página nueva. No modifiques `tablas/` para esto.
8. **Imágenes de equipos/jugadores/eventos**: usa siempre `/api/media/meta` (enlaces+color, 1 request por render) y los endpoints `/api/media/...` como fallback (resuelven y redirigen al CDN; **no se descargan ni guardan imágenes**). `meta` marca `pending` y resuelve en 2º plano: **no** fuerces el fallback ni scrapees en la ruta crítica. No scrapees Google Images ni guardes archivos de imagen; la caché es solo de enlaces/color (`media/urls_cache.json`, **versionado**: no lo vuelvas a ignorar). Si necesitas precargar enlaces, usa `cachear_media.py` con `--delay`.
9. **Rendimiento**: para blueprints de solo lectura usa `fetch_all` (`backend.conexion`) + `@ttl_cache(120)` (`backend.cache`, con single-flight); no abras conexiones nuevas por consulta ni paralelices consultas a Turso (el cliente serializa). Mantén gzip (`flask-compress`) y paginación en listados grandes. Para cambiar CSS/JS propios, edita los fuentes y corre `py tools/minificar_assets.py` (regenera `.min` + `?v=`); el HTML propio va `no-cache` y los estáticos con hash van `immutable`.
10. **Enlaces internos**: navega siempre con `/componente/` (o relativo `../componente/`, `./`), **nunca** `/componente/index.html` (regla de estética de URL, §5.1). Al añadir una vista dentro de una página, usa query params (`?team=`, `?match=`…), no nuevas carpetas con `index.html` en el enlace.
11. **Diseño y colores**: la paleta y los tokens viven SOLO en `comun/theme.css`; no introduzcas colores literales en HTML/CSS (usa variables). El contenido/chrome va en negros, grises y blancos neutros: el verde no se usa en bordes ni superficies, solo en la escala semántica de datos (verde/naranja/amarillo/rojo) y en la animación de nodos. Toda página nueva debe enlazar `comun/theme.css` antes de sus CSS y `header/header-nodes.js` después de `header.js`. El ico/logo se referencia desde `comun/ALETHEIA_ico.svg` (los archivos sueltos de la raíz dan 308/404 en Flask).

---

## 8. Registro de Cambios

- **2026-10-05 — Nodos neón + rediseño de los botones del header.**
  - `header/header-nodes.js`: más densidad (header 44–105, fondo 36–140),
    **pulso de tamaño muy leve** (±10–14 % por nodo, con fase propia) y, en el
    fondo, un **glow neón sutil** con composición `lighter`, halo doble y
    núcleo casi blanco (sin `shadowBlur`). Se mantienen el tope de DPR 1.25 y
    la rejilla espacial de F2.
  - `header/header.css`: los botones del nav pasan a **chips angulares**
    (esquina superior derecha cortada con `clip-path`), más grandes
    (`11px 24px`, 12,5px, tracking 2,5px), mono en mayúsculas y tinte oscuro
    sutil en reposo. Hover/activo con relleno oscuro, **barra de acento**
    creciente, brillo interior (`box-shadow: inset`), rim y línea inferior,
    `text-shadow` lima en dos capas y **barrido de luz** diagonal (`::after`).
    Verificado con capturas headless (reposo, hover y activo).
  - `tools/minificar_assets.py`: escritura de archivos con **reintentos** ante
    bloqueos transitorios de Windows (`OSError: [Errno 22]` al reescribir un
    HTML que otro proceso tiene abierto un instante); ya no deja los hashes del
    HTML a medias.
- **2026-10-05 — Fix visual del detalle de PARTIDOS (post-auditoría).**
  - `partidos/script.js`: el banner del partido pintaba el logo de `team_a` en
    grande (`'big'`) junto al título del evento, mientras que el bloque VS pinta
    ambos equipos a tamaño normal (de ahí la sensación de "logos desiguales").
    Ahora el logo grande es el del **evento** (coherente con el título); si el
    partido no trae evento/torneo se conserva el logo grande de `team_a`.
  - **Degradado y watermarks del banner** (`comun/vct.css`, `comun/vct.js`,
    `partidos/script.js`): izquierda = evento (`--ce`/`--wm-e`), centro = equipo
    A (`--c`/`--wm-a`) y derecha = equipo B (`--c2`/`--wm-b`). `aplicarMedia`
    entiende ahora `data-ce` (color del evento) y `data-wm3` (watermark del
    evento) además de `data-wm`/`data-wm2`.
  - **Logos de equipos más grandes en el bloque VS** (`'big'`, 104px; 80px en
    móvil) y **nombre secundario** (`v-team-name` 28 → 15px, 13px en móvil):
    el protagonismo es del logo, no del nombre.
  - **Logos sin glow (F8 bis, decisión final):** se eliminaron los halos
    (`drop-shadow`) de `.v-lozenge-fg`/`.v-elogo-fg`. El glow del color del
    equipo amplificaba los logos ya brillantes (XI LAI cian, VCT
    naranja/rojo/púrpura) y el halo blanco se recortaba como "caja" en logos
    oscuros. Ahora los logos se muestran **tal cual**, sin filtros.
    Verificado con capturas headless (equipos, eventos y partido).
  - **Inversión de logos negros (`bl`):** `media.py` mide el ratio de píxeles
    **negros sin croma** (`max(R,G,B) < 70` y chroma `< 35`; así el
    rojo/azul/púrpura saturados no cuentan) y expone `bl` en `/media/meta`
    con un umbral del **85%**; si un logo es negro, `.on-light` lo **invierte**
    con `filter: invert(1) hue-rotate(180deg)` (mismo tamaño; el negro pasa a
    blanco y los tonos se conservan: la estrella roja de FUT sigue roja). El
    **watermark** del banner también se invierte (`wm-inv-a/b/e` sobre
    `.v-banner::after`, sin tocar el degradado). `COLOR_VERSION = 3` recalcula
    la caché de colores en 2º plano. Quedan invertidos 18 equipos (p. ej.
    Paper Rex, FUT) y 9 eventos (VCT EMEA); T1, DRX o los VCT naranja/púrpura
    quedan igual. Verificado con capturas headless (equipos, eventos y partido
    FUT vs T1: logo claro con estrella roja y ambos watermarks visibles).
  - **Watermark del banner:** la regla genérica vuelve a 2 capas (`--wm-a`/`--wm-b`)
    para no mover el watermark de equipos/jugadores/eventos; el orden evento/A/B
    vive solo en `.v-banner.vs::after` (partidos). Verificado con capturas
    headless (equipos, evento/EMEA, partido y EN VIVO).
- **2026-10-05 — Auditoría de rendimiento web (F1–F14, sin F10).** Correcciones de
  carga/estabilidad sin tocar lógica de predicción, modelos, ratings ni resultados.
  - **F1 (`aletheia/script.js`) cola de `/serie`:** `cargarSeriesLista()` (hasta 12
    POST secuenciales por render) se sustituye por `encolarSerie()` +
    `bombearSerie()` + `procesarSerie()`: solo filas **visibles**
    (`IntersectionObserver`, 200 px de margen) o la **seleccionada**, nunca
    `stale`/sin resultado; **concurrencia 2** (`SERIE_CONCURRENCY`); resultado
    cacheado por sesión (`simSerie` + `serieIntentados`) y sin relanzar en cada
    `renderSimList`; generación (`serieGen`) descarta respuestas de una carga
    vieja. Verificado en Node con las funciones reales extraídas del fuente
    (10 filas ⇒ 10 llamadas, máx. 2 en vuelo, 0 repetidas en 2 renders).
  - **F2 (`header/header-nodes.js`, `comun/theme.css`):** fondo con 32–110 nodos
    (antes 60–300), `dprMax 1.25` (antes 2,5) y enlaces por **rejilla espacial**
    (fin del O(n²) por frame); `contain: strict` en `.ae-nodes-bg`. Se conserva
    el aspecto y `prefers-reduced-motion`; el header mantiene su densidad.
  - **F3 (CLS):** skeleton de 6 tarjetas en `#simList` + `min-height` en
    `.sim-list`; `#simListStatus` con altura reservada; conteo de chips en
    `<span class="sim-filter-count">` de ancho fijo (ya no reescribe el texto
    del botón).
  - **F4 (LCP):** logos de la primera fila con `loading="eager"`
    `fetchpriority="high"` (`VCT.lozenge(..., prioridad)`); el resto lazy con
    `width`/`height`; `aplicarMedia` ya no espera a fijar `src` para el fallback.
  - **F5 (`media/media.py`, `.gitignore`):** `media/urls_cache.json` se
    **versiona** (solo se ignora el `.tmp`) para sobrevivir a los deploys;
    `/api/media/meta` responde `{"pending": true}` y resuelve en hilo de fondo
    (semáforo/throttle, `_resoluciones_en_proceso`), sin scrape en la ruta
    crítica.
  - **F6 (`comun/vct.js`):** `aplicarMedia` reintenta **solo las entidades
    pendientes** (no el lote completo) y no dispara el fallback cuando el
    backend marca `pending`.
  - **F7 (`backend/app.py`, HTML):** CSS/JS con `?v=<hash>` (generado por
    `tools/minificar_assets.py`) → `max-age=31536000, immutable`; imágenes/fuentes
    → 30 días; HTML `no-cache`.
  - **F8 (`comun/vct.js`, `comun/vct.css`):** **una sola `<img>`** por logo/foto
    en vez de fg+bg (sin glow al final: el logo se muestra tal cual); se evita
    la doble decodificación/pintura. El CDN de owcdn no ofrece redimensionado
    verificado: no se inventaron parámetros de tamaño.
  - **F9 (`backend/cache.py`, `aletheia/aletheia.py`):** single-flight en
    `@ttl_cache` (8 hilos concurrentes ⇒ 1 query); `_TABLA_MAPAS_TTL` 300→1800 s;
    `_version_vigente_db` memoizada 30 s; `_predicciones_db` proyecta columnas
    explícitas (los JSON de marcadores/economía sí se usan y se conservan).
  - **F11 (HTML):** Google Fonts con `rel="preload"` + `media="print"
    onload="this.media='all'"` y fallback `<noscript>` en las 10 páginas.
  - **F12 (`tools/minificar_assets.py`, assets `.min`):** versiones minificadas
    de los CSS/JS propios (fuentes intactas) y versionado automático de los HTML.
  - **F13 (`backend/app.py`, `header/header.js`):** `/` sirve EN VIVO directo
    (sin 302) y `strict_slashes=False` elimina los 308 de `/aletheia`,
    `/partidos`…; el nav marca EN VIVO en `/`.
  - **F14 (`.gitignore`, `inicio/inicio.py`):** pandas/numpy con import perezoso
    (`_LazyPandas`; al arrancar la app ya no se cargan); `.gitignore` ordenado
    (`AUDITORIA_*.md`, `*.db-journal`, `.tmp` de media).
  - **F10 fuera de alcance** (workers/plan de Render y Cloudflare): no se tocó
    `render.yaml` ni configuración de borde; queda reportado.
  - **Medición local (`py wsgi.py`):** `/predicciones?match_id=754732`
    2,17/1,29 s → 1,58/1,04 s; `/prediccion` 1,06 → 0,84 s; `/modelo_version`
    caliente 1,24 → 0,003 s; `/` 302→200 y `/aletheia` 308→200; CSS/JS con
    `?v=` e `immutable`; `.min` + gzip: `style.css` 10,5 → 8,8 KB, `script.js`
    27,8 → 27,4 KB. No se pudieron medir CLS/LCP/TBT reales ni el conteo de
    `/serie` en navegador (sin headless disponible). Turso añade varianza
    (±1–2 s en conexión fría por hilo): los endpoints calientes son los fiables.
- **2026-10-05 — Contrato ESC por (mapa, lado) + MOTOR en la raíz (adaptación web).**
  - **Backend ALETHEIA_PREDICT (no tocado aquí):** `mapas[].prob_victoria_a` pasó
    a ser la capa **ESC por (mapa, lado)** (anclada al Glicko) y varía por mapa y
    lado; la **P del motor** ya no se repite por mapa y se expone una sola vez
    por enfrentamiento en la raíz como `prob_motor_a`/`prob_motor_b` (puede ser
    `null`). `prob_serie_a/b` (motor + temperatura) sigue siendo la que decide
    la serie. `predicciones_mapa` añade `esc_peso` (y ya tenía
    `marcadores_json`/`economia_json`); `escenario_mapa` puede venir `null` en
    lecturas cacheadas con el motor en frío.
  - **`aletheia/aletheia.py` (modo DB):** `_derivar_fila` expone `esc_peso`
    (fallback a `escenario_mapa.peso`) y ya **no replica `escenario_mapa`** desde
    `rounds` (era la fórmula anclada al motor; al ser `prob_victoria_a` el ESC,
    replicarla lo duplicaba). Se eliminan `_tabla_mapa_lado`, `_calcular_escenario`,
    `_escenario_local`, `_escenario_activo` y el flag web
    `ALETHEIA_ESCENARIO_MAPA`. `_serie_desde_db` pasa los mapas de `mapas_json`
    tal cual y devuelve `prob_motor_a/b: null` (no se persiste).
  - **`aletheia/script.js` (EN VIVO):** el número grande de las tarjetas de mapa
    es el **ESC** (`prob_victoria_a/b` del lado elegido; sin promediar lados) y
    el **MOTOR** se lee de `prob_motor_a/b` (raíz de `/serie`): sub-línea de las
    tarjetas, banner de serie y MOTOR de la lista (se pinta cuando la cola de
    `/serie` responde). Se añade **CONF. ESC** con `esc_peso` e
    `p_lo–p_hi`/`n_a/n_b`/Δlogit si `escenario_mapa` está presente; si
    `escenario_mapa`/`esc_peso` son `null` se oculta la confianza sin romper. El
    banner y la tabla de la serie muestran el ESC por mapa/lado + `conf`; el
    informe `.md` usa `esc_p_a/b`, `esc_peso` y `motor_p_a/b`, y el prompt del
    LLM se reescribe con la nueva nomenclatura. `fetchSerieReal` (COMPARACIÓN)
    separa `MOTOR:` (plano) de `SERIE:`.
  - **`partidos/partidos.py`:** `/api/partidos/resultados` deja de exponer `p_a`
    (P plana del motor) y calcula `p_ganador` con el **ESC del (mapa, lado)** en
    que `equipo_a` empezó el mapa real (`side_top_start` + `team_top_id` de
    `rounds`); `p_real_media`/`favoritos_ok`/puntos de la lista EN VIVO pasan a
    ser ESC.
  - **Criterio:** el MOTOR mostrado es idéntico y coincide con `prob_motor_a`;
    el ESC varía y coincide con `prob_victoria_a`; la P(serie)/ganador global
    usan solo `prob_serie_a/b`; `null` en `escenario_mapa`/`esc_peso` no rompe.
  - Verificado contra Turso (modo DB): `/api/aletheia/predicciones`
    de 754732 devuelve ESC + `esc_peso` (Abyss attack 0.6228/0.8214) con
    `escenario_mapa: null`; `/api/partidos/resultados` de 753460 calcula ESC
    por mapa sin `p_a`; `_serie_desde_db` pasa `mapas_json` con motor `null`.
- **2026-10-05 — Pool de mapas (`map_pool`) en EN VIVO.**
  - `aletheia/aletheia.py`: nuevo helper `_map_pool_db()` (detecta `map_pool`
    vía `sqlite_master` y devuelve los `map_name` con `en_pool=1`; `None` si la
    tabla no existe). `GET /api/aletheia/mapas` acepta `?pool=1` y devuelve la
    intersección con `predicciones_mapa`; con la tabla ausente degrada a todos.
  - `aletheia/script.js`: `loadAvailableMaps()` pide `?pool=1` y purga de la
    serie los mapas que salgan del pool; el selector/explorador y el armador de
    serie quedan limitados al pool. PREPARAR no cambia (no usa `/mapas`).
  - El tope del pool en el motor (`precalcular`/`serie`) es responsabilidad de
    ALETHEIA_PREDICT (repo aparte); la web solo restringe la UI.
  - Verificado contra Turso: `?pool=1` → 7 mapas (Abyss, Ascent, Haven, Lotus,
    Split, Summit, Sunset); sin `pool` → 13.
- **2026-10-04 — POST /api/precalcular: anti-doble-envío, 429 y reanudación del job.**
  - **PREPARAR (`aletheia_preparar/script.js`):** el job se persiste por
    enfrentamiento (`match_id`+equipos) en `localStorage`
    (`ae_precalcular_jobs`, TTL 24 h). Al recargar se restaura el
    enfrentamiento y se **reanuda el polling** sin re-POSTear; una segunda
    pestaña muestra el progreso. El **429** ("ya hay un precálculo en curso")
    deja de ser error fatal: se adopta el `job_id` si viene en la respuesta, se
    reanuda el job guardado o se informa con botón **reintentar** sin perder la
    UI. El POST se lanza con `AbortController` (190 s) y, si expira/no responde,
    se hace **un único reintento** (solo sin job conocido) para descubrir el job
    que pudo crearse; el botón queda deshabilitado durante todo el vuelo. La
    **barra de progreso** usa `job.progreso` (0..1), no `mapas_hechos/total`.
    Se envía `forzar:false` salvo RE-PREPARAR. El contador "en caché" cuenta
    **solo filas vigentes** (`modelo_version` actual y `n_sim` >= solicitado).
    Al terminar se recarga `/predicciones`; si `estado=error` se muestra el
    error y se permite reintentar.
  - **EN VIVO (`aletheia/script.js`):** RE-PRECALCULAR comparte el job
    persistido con PREPARAR, adopta el job del 429 o el guardado en vez de
    fallar, usa timeout de POST largo (190 s) y reintenta el polling ante 5xx
    del túnel.
  - **Proxy (`aletheia/aletheia.py`):** `POST /api/precalcular` espera hasta
    **180 s** (`TIMEOUT_PRECALCULAR`) la respuesta del servicio (motor en frío);
    el 429 se reenvía con su cuerpo. `_request_service`/`_passthrough_post`
    aceptan `timeout` opcional (solo afecta a las llamadas directas a BASE_URL).
  - Contexto: FUT Esports vs T1 #753448 mostró "Error: Ya hay un precálculo en
    curso" mientras un job real terminaba bien (26/26, ~117 s), por doble
    petición y por tratar el 429 como fatal.
- **2026-10-03 — Nodos: rebote real y fondo visible.**
  - `header/header-nodes.js`: el rebote ahora **refleja la deriva base**
    (`bvx`/`bvy`, con patada mínima hacia dentro) además de la velocidad; antes
    el easing devolvía el nodo contra la pared y, con el tiempo, **todos los
    nodos terminaban pegados a los bordes** (sin líneas cercanas, casi
    invisibles).
  - Modo fondo (`bg`) más perceptible sobre `--bg`: nodos `--g2`/`--g3`, alpha
    base 0.72, halo suave, mayor radio, `linkDist` 115 y líneas/cursor más
    intensos; `.ae-nodes-bg` sube de `opacity: .6` a `.9` (`comun/theme.css`).
    El header (nodos oscuros sobre lima) conserva su aspecto.
  - Densidad del fondo muy superior (`W/8`, **60–300 nodos**, antes 34–90);
    tope de 5 líneas por nodo para que la red densa no se sature ni pierda
    rendimiento.
- **2026-10-03 — ESC en modo DB (capa de escenarios sin servidor).**
  - `aletheia/aletheia.py` replica `escenario_mapa` (B1) en modo DB desde
    `rounds`: tabla `(equipo, mapa, lado) -> (w, n)` con el swap de regulación
    (r13) y la alternancia de overtime (r25+), agregada en una sola consulta
    (las dos ramas se unen antes de agrupar) y cacheada 5 min con fallback a la
    copia previa; misma fórmula que `core/escenario_mapa.py` (`λ=0.4`, `K=10`,
    Wilson 95%).
  - `_derivar_fila` (`/prediccion`, `/predicciones`) y `_serie_desde_db`
    (`/serie` cacheada) exponen la capa; la UI ya pinta el pill `ESC`, la
    tarjeta ESCENARIO, la tabla de la serie y el informe `.md`.
  - Flag `ALETHEIA_ESCENARIO_MAPA` (default `1`): `0/false/no/off` ⇒
    `escenario_mapa=null`. `prob_victoria_a` y `modelo_version` no cambian.
  - Paridad verificada contra el motor real: `tabla_mapa_lado` idéntica (1472
    claves) y para 753455 `max|Δp_mapa| = 0.0` en los 26 pares (mismos `n`).
- **2026-10-03 — Alineación web ↔ Planes A/B/C de ALETHEIA_PREDICT (H1-H8).**
  - **H1/H2 (veto completo):** ARMAR SERIE exige **1/3/5** mapas antes del POST;
    con 2/4 el botón se deshabilita, el estado dice "faltan N" y **no hay
    llamada** (la API responde 400). La P de serie en 2º plano de la lista y el
    banner de COMPARACIÓN tampoco llaman con listas 2/4: la píldora muestra
    **`SERIE sin pool`** y el armador avisa.
  - **H3 (escenario):** la UI consume `escenario_mapa` (capa B1) en la tarjeta
    **ESCENARIO** del mapa, el pill `ESC` del selector/armador, la tabla de la
    serie y el informe `.md`; si viene `null` (flag apagado) se oculta
    sin romper. `prob_victoria_a` **no** cambia.
  - **H4 (economía):** la nota del bloque usa `economia.semantica` ("sin pistols
    R1/R13") y `n` por categoría/celda destacada; la matriz 4×4 y el bloque
    PISTOL se mantienen.
  - **H5 (partidos):** `GET /api/partido/<id>` calcula la economía real desde
    `rounds` (excluye R1/R13, `fuente:'rounds'`); `economy_summary` queda solo
    como fallback etiquetado (benchmark no fiable en eco).
  - **H6 (modo DB):** `_serie_desde_db` rechaza listas 2/4, `_formato_de_serie`
    queda alineado con 1/3/5 y `_derivar_fila` documenta `prob_intervalo` como
    `null` (la UI lo oculta; el `escenario_mapa` se replica en modo DB desde la
    entrada siguiente).
  - **H8 (ETL):** `inicio/inicio.py` etiqueta R1/R13 como `pistol` en
    `category_top`/`category_bot` (coherente con la economía sin pistols).
  - Referencia: `modelo_version` **`5232151ff388`** (caché 520/520). No se toca
    `core/` de Predict ni la P(mapa)/`modelo_version`.
- **2026-10-02 — Lectura directa de Turso: ver predicciones sin servidor.**
  - `aletheia/aletheia.py` sirve **cache-first desde Turso** (`fetch_all`):
    `mapas`, `modelo_version`, `simulaciones`, `prediccion`, `predicciones` y
    `serie` si hay una fila de `predicciones_serie` con los mismos mapas/lados.
    Recalcula `confianza` y `analisis_mapa` con las mismas reglas que `core/`
    (réplica de `tabla_mapas_equipo` + `probabilidad_mapa_analitica`, caché
    5 min); `prob_intervalo` queda `null` (usa los RD locales del servicio).
    Si no hay fila o la DB falla, cae al servicio.
  - Con esto, ver las predicciones preparadas **no necesita el PC ni Render**:
    el servidor solo se usa para preparar/forzar (`precalcular`,
    `precalcular/estado`, `asociar`, `borrar`, `dataset?guardar=1`,
    `forzar:true`) y para lo derivado no persistido (`equipos`, `comparacion`,
    `scorecard(_agregado)`, `dataset` sin guardar, `predecir`, serie con otros
    mapas). El módulo pasa a importar `backend.conexion.fetch_all` (sigue sin
    `numpy`/`pandas`).
- **2026-10-02 — Fase 2 web sin servidor local (lecturas en Render).**
  - `aletheia/aletheia.py` enruta las **lecturas** a
    `ALETHEIA_PREDICT_READ_URL` (servicio cache-first en Render, siempre
    disponible) con un reintento por cold start y caída a
    `ALETHEIA_PREDICT_URL`; el cómputo/mutaciones (`precalcular`,
    `precalcular/estado`, `asociar`, `borrar`, `dataset?guardar=1`,
    `predecir`/`serie` con `forzar:true`) siguen en ngrok/PC. Sin
    `ALETHEIA_PREDICT_READ_URL`, todo va a `ALETHEIA_PREDICT_URL`.
  - `render.yaml` fija `ALETHEIA_PREDICT_READ_URL=https://aletheia-predict.onrender.com`.
  - EN VIVO sube el corte de espera de la cola `POST /serie` a 60 s (cold
    start de Render); los comentarios de enrutado de los dos `script.js`
    quedan al día.
  - Un 5xx no JSON del servicio (túnel/hosting caído) ya no se reporta como
    "JSON inválido": el proxy responde `El servicio de predicción no está
    disponible (HTTP 5xx)`, más claro para la UI.
- **2026-10-02 — Alineación web ↔ ALETHEIA_PREDICT (H1-H6).**
  - **H1 (clave S1):** `aletheia/aletheia.py` añade `X-API-Key` **server-side**
    cuando `ALETHEIA_API_KEY` está configurada y expone
    `GET /api/aletheia/precalcular/estado`; PREPARAR, RE-PRECALCULAR y su polling
    pasan al proxy (`proxyFetch`); se eliminan `PREDICT_DIRECTO`,
    `NGROK_HEADER` y `predictFetch` de los dos `script.js`. El navegador ya no
    llama a ngrok.
  - **H2:** `/api/serie` documentado como **cache-aware** (reutiliza la caché y
    calcula/persiste lo que falte), en docstring, nota de la UI y esta doc.
  - **H3:** la web muestra `prob_intervalo` (IC95% aditivo, A6) en la tarjeta de
    mapa y en el informe `.md`; Predict lo **recalcula en la lectura de caché**
    (`api/app.py`, aditivo: no cambia la P puntual ni `modelo_version`).
  - **H4:** la web avisa cuando `GET /api/modelo_version` trae
    `desactualizado:true` ("reinicia ALETHEIA_PREDICT") en EN VIVO y en PREPARAR.
  - **H5:** §4.4/§5/§6 y los comentarios/docstrings alineados con el contrato
    vigente.
  - **H6:** `render.yaml` sin el valor de `TURSO_AUTH_TOKEN` (`sync: false`) y
    con `ALETHEIA_API_KEY`; `backend/conexion.py` deja de llevar el JWT como
    valor por defecto (lo lee del entorno/`.env`) y `wsgi.py` carga `.env`
    antes de importar la app. La **rotación del token** en Turso y su carga en
    el panel de Render es una operación manual pendiente de confirmar.
