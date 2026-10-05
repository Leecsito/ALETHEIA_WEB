const API = `${window.location.origin}/api`;

// EN VIVO lee de la caché por el proxy (/api/aletheia/...). El proxy sirve las
// lecturas directo de Turso (sin servidor); solo la serie no cacheada y las
// vistas derivadas van al servicio de Render (y caen a ngrok/PC si no responde).
// RE-PRECALCULAR (forzar:true) va directo al PC (async: el POST responde 202 y
// su polling es rápido). La clave API la añade el proxy server-side; nunca
// llega al navegador.
let availableMaps = [];    // mapas del pool activo (proxy /api/aletheia/mapas?pool=1)
let mapsLoading = false;
let sims = [];             // enfrentamientos ya preparados (/api/simulaciones)
let current = null;        // simulación seleccionada
let ultimaSerie = null;    // último resultado de POST /api/serie (para el informe)
let liveBulk = null;       // 'map|side' -> fila cacheada (se lee UNA vez)
let liveSide = 'attack';   // bando inicial de A
let liveMap = null;        // mapa seleccionado en el panel mapa/bando
let matchMaps = [];        // [{ map_name, lado_inicial_a }] para la serie
let maxMapsSel = 3;        // slots del formato (1/3/5)
let showStale = false;     // mostrar simulaciones no vigentes
let filtroResultado = 'todas';   // todas | pendiente | resultado
let simsConResultado = null;     // Set de match_id con resultado real (null = sin dato)
let simsInfo = {};               // match_id -> identidad + resumen predicción↔realidad
let simSerie = {};               // match_id -> {p_a,p_b,p_real} | {error:true} (P de serie)
let resultadosError = null;      // error al leer /api/partidos/resultados (null = ok)
let serviceModelVersion = null;  // modelo vigente (GET /modelo_version)
let serviceModeloDesactualizado = false;  // true = reentreno/cambio sin reiniciar
let recalculating = false;       // evita doble RE-PRECALCULAR
let recalcStopped = false;       // cancelar la espera del job de re-precalculo
let liveBulkError = null;        // error al leer /predicciones (null = ok)
let mapsError = null;            // error al cargar /mapas (null = ok)
let modelVersionError = null;    // error al leer /modelo_version (null = ok)

// Cota del job de RE-PRECALCULAR en EN VIVO (evita poll infinito si se cuelga).
const RECALC_POLL_MS = 2000;
const RECALC_TIMEOUT_MS = 30 * 60 * 1000;
const RECALC_POST_TIMEOUT_MS = 190000;  // el POST puede tardar (motor en frío)

// Job de precálculo persistido por enfrentamiento (mismo esquema que PREPARAR):
// permite reanudar el polling tras recargar/otra pestaña y adoptar el job
// activo cuando el POST responde 429 ("ya hay un precálculo en curso").
const JOBS_KEY = 'ae_precalcular_jobs';
const JOB_TTL_MS = 24 * 60 * 60 * 1000;

function claveEnfrentamiento(a, b, mid) {
    const id = parseInt(mid, 10) || 0;
    if (id > 0) return `match:${id}`;
    return `eq:${String(a || '').trim().toLowerCase()}|${String(b || '').trim().toLowerCase()}`;
}

function jobVigente(job) {
    return !!(job && job.job_id && (Date.now() - (job.started_at || 0)) < JOB_TTL_MS);
}

function leerJobs() {
    try {
        const raw = localStorage.getItem(JOBS_KEY);
        const obj = raw ? JSON.parse(raw) : {};
        return obj && typeof obj === 'object' ? obj : {};
    } catch (e) {
        return {};
    }
}

function escribirJobs(jobs) {
    Object.keys(jobs).forEach(k => { if (!jobVigente(jobs[k])) delete jobs[k]; });
    try { localStorage.setItem(JOBS_KEY, JSON.stringify(jobs)); } catch (e) { /* sin localStorage */ }
}

function guardarJob(job) {
    const jobs = leerJobs();
    jobs[job.clave] = job;
    escribirJobs(jobs);
}

function quitarJob(clave) {
    const jobs = leerJobs();
    if (jobs[clave]) { delete jobs[clave]; escribirJobs(jobs); }
}

function jobGuardado(a, b, mid) {
    const jobs = leerJobs();
    const id = parseInt(mid, 10) || 0;
    let job = jobs[claveEnfrentamiento(a, b, id)];
    if (!job && id > 0) job = jobs[claveEnfrentamiento(a, b, 0)];
    if (!job) job = Object.values(jobs).find(j => j.equipo_a === a && j.equipo_b === b && jobVigente(j));
    return jobVigente(job) ? job : null;
}

// Cola de POST /serie de la lista (F1): solo para las filas VISIBLES (viewport,
// con margen) y la seleccionada; nunca stale. Concurrencia acotada, resultado
// cacheado por sesión (`simSerie` + `serieIntentados`) y sin relanzar lotes en
// cada `renderSimList`. 60 s cubre el cold start del servicio de lectura en
// Render (en caliente responde en segundos); el proxy reintenta y cae a ngrok.
const SERIE_CONCURRENCY = 2;     // peticiones /serie en vuelo a la vez
const SERIE_TIMEOUT_MS = 60000;
let serieCola = [];              // [{s, gen}] pendientes de procesar
let serieActivos = 0;            // peticiones /serie en vuelo
let serieIntentados = new Set(); // match_id ya encolados en esta carga (no repetir)
let serieGen = 0;                // generación: invalida resultados de una carga vieja
let simObserver = null;          // IntersectionObserver de las filas visibles

// Filtro de resultado persistido (si el navegador lo permite).
const FILTRO_SIMS_KEY = 'ae_sim_filtro';
try {
    const filtroGuardado = localStorage.getItem(FILTRO_SIMS_KEY);
    if (['todas', 'pendiente', 'resultado'].includes(filtroGuardado)) filtroResultado = filtroGuardado;
} catch (e) { /* sin localStorage: se usa el default */ }

// ─── DOM ──────────────────────────────────────────────────────────────────────
const simList = document.getElementById('simList');
const simListStatus = document.getElementById('simListStatus');
const btnRefreshSims = document.getElementById('btnRefreshSims');
const chkShowStale = document.getElementById('chkShowStale');
const simFilter = document.getElementById('simFilter');
const simFilterBtns = simFilter ? Array.from(simFilter.querySelectorAll('.sim-filter-btn')) : [];

simFilterBtns.forEach(btn => btn.addEventListener('click', () => {
    filtroResultado = btn.dataset.filtro || 'todas';
    try { localStorage.setItem(FILTRO_SIMS_KEY, filtroResultado); } catch (e) { /* sin localStorage */ }
    renderSimList();
}));

const liveSection = document.getElementById('liveSection');
const selSimHead = document.getElementById('selSimHead');
const panelMapa = document.getElementById('panelMapa');

const liveMapPicker = document.getElementById('liveMapPicker');
const liveDetailTitle = document.getElementById('liveDetailTitle');
const liveTeamALabel = document.getElementById('liveTeamALabel');
const liveSideAtk = document.getElementById('liveSideAtk');
const liveSideDef = document.getElementById('liveSideDef');
const liveCards = document.getElementById('liveCards');
const liveScoreboard = document.getElementById('liveScoreboard');
const liveEconomia = document.getElementById('liveEconomia');
const liveStatus = document.getElementById('liveStatus');

const serieSlots = document.getElementById('serieSlots');
const seriesBanner = document.getElementById('seriesBanner');
const serieNote = document.getElementById('serieNote');
const mbFormat = document.getElementById('mbFormat');

const panelComparacion = document.getElementById('panelComparacion');
const cmpSerie = document.getElementById('cmpSerie');
const cmpSummary = document.getElementById('cmpSummary');
const cmpTableWrap = document.getElementById('cmpTableWrap');
const cmpStatus = document.getElementById('cmpStatus');
const scorecardWrap = document.getElementById('scorecardWrap');
const scorecardAgregadoWrap = document.getElementById('scorecardAgregadoWrap');
const btnScorecardAgregado = document.getElementById('btnScorecardAgregado');
const btnExportDataset = document.getElementById('btnExportDataset');
const cmpToolsStatus = document.getElementById('cmpToolsStatus');

// ─── HELPERS ──────────────────────────────────────────────────────────────────
// Devuelve el % redondeado o '—' si el dato falta (no lo convierte en 0%).
function pct(v) {
    if (v == null || v === '') return '—';
    const n = Number(v);
    if (isNaN(n)) return '—';
    return `${Math.round(n * 100)}%`;
}

function proxyFetch(path, options = {}) {
    return fetch(`${API}/aletheia/${String(path).replace(/^\/+/, '')}`, options);
}

function escapeHtml(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

// Parsea la URL o el número de vlr.gg -> primer grupo de dígitos.
function parseMatchId(raw) {
    const m = String(raw || '').match(/(\d+)/);
    return m ? parseInt(m[1], 10) : 0;
}

function fmtRatio(v) {
    if (v == null || isNaN(v)) return '—';
    const n = Number(v);
    return `${(n <= 1 ? n * 100 : n).toFixed(1)}%`;
}

function fmtNum(v, d = 4) {
    if (v == null || isNaN(v)) return '—';
    return Number(v).toFixed(d);
}

// Formatos válidos por veto completo (contrato /api/serie y /api/predecir):
// la API exige 1, 3 o 5 mapas; 2/4 responden 400.
const VETO_COMPLETO = [1, 3, 5];

function inferFormat(n) {
    if (n <= 0) return '—';
    if (n === 1) return 'Bo1';
    if (n === 3) return 'Bo3';
    if (n === 5) return 'Bo5';
    return 'incompleto';
}

function sameSim(a, b) {
    if (!a || !b) return false;
    if (a.match_id && b.match_id) return a.match_id === b.match_id;
    return a.equipo_a === b.equipo_a && a.equipo_b === b.equipo_b;
}

// Peso/confianza del ESC de una fila (`esc_peso`; si falta, `escenario_mapa.peso`).
// Devuelve un número 0-1 o null; null no es error (la UI oculta la confianza).
function escPeso(p) {
    if (!p) return null;
    const raw = (p.esc_peso != null) ? p.esc_peso
        : ((p.escenario_mapa && p.escenario_mapa.peso != null) ? p.escenario_mapa.peso : null);
    if (raw == null || raw === '' || isNaN(Number(raw))) return null;
    return Number(raw);
}

// Escala verde / naranja / rojo para la probabilidad del resultado real
// (misma banda conservadora del motor: >=0.62 alta, >=0.55 media, resto baja).
function probBandClass(p) {
    if (p == null || p === '' || isNaN(Number(p))) return '';
    const n = Number(p);
    if (n >= 0.62) return 'p-alta';
    if (n >= 0.55) return 'p-media';
    return 'p-baja';
}

// Escala verde / naranja / rojo para la TASA DE ACIERTO (fracción 0-1):
// >=2/3 verde, >=1/2 naranja, resto rojo. Es el indicador de calidad, no P(REAL).
function probAccClass(acc) {
    if (acc == null || acc === '' || isNaN(Number(acc))) return '';
    const n = Number(acc);
    if (n >= 2 / 3) return 'p-alta';
    if (n >= 0.5) return 'p-media';
    return 'p-baja';
}

// Logo + nombre de equipo (usa el core VCT; sin id cae a las siglas/iniciales).
// `prioridad` = logos de la primera fila (LCP): eager + fetchpriority=high.
function teamLogo(name, tag, teamId, cls = '', prioridad = false) {
    return `<span class="team-inline">${VCT.lozenge(name, tag, cls, teamId || null, prioridad)}` +
        `<span class="team-inline-name">${escapeHtml(name || '—')}</span></span>`;
}

// ─── MAPAS (proxy) ────────────────────────────────────────────────────────────
// Solo el pool activo (`map_pool.en_pool=1`, vía `?pool=1`); si la tabla no
// existe, el proxy devuelve todos los mapas con predicción.
async function loadAvailableMaps() {
    mapsLoading = true;
    mapsError = null;
    try {
        const res = await fetch(`${API}/aletheia/mapas?pool=1`);
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || `HTTP ${res.status}`);
        availableMaps = data.mapas || [];
        // La serie no conserva mapas que salieron del pool.
        matchMaps = matchMaps.filter(m => availableMaps.includes(m.map_name));
    } catch (e) {
        mapsError = e.message || 'servicio no disponible';
    }
    mapsLoading = false;
    renderLiveMapPicker();
    syncSerieBuilder();
}

// ─── LISTA DE SIMULACIONES PREPARADAS ─────────────────────────────────────────
async function loadSimulaciones() {
    simListStatus.className = 'live-status';
    simListStatus.textContent = 'Cargando simulaciones...';
    try {
        const res = await proxyFetch('/simulaciones?limite=100');
        const data = await res.json();
        if (!data.ok) {
            simListStatus.className = 'live-status err';
            simListStatus.textContent = `Error: ${data.error || 'no se pudo leer /api/simulaciones'}`;
            return;
        }
        if (data.modelo_version) {
            serviceModelVersion = data.modelo_version;
            modelVersionError = null;
        }
        sims = data.simulaciones || [];
        // Nueva carga = nueva generación: descarta cola/resultados de la anterior.
        serieGen++;
        simSerie = {};
        serieIntentados = new Set();
        serieCola = [];
        // Marca no vigentes también por comparación de modelo_version (aunque el
        // backend no lo hubiera marcado), para ofrecer RE-PRECALCULAR.
        sims.forEach(s => {
            if (s.modelo_version && serviceModelVersion && s.modelo_version !== serviceModelVersion) {
                s.vigente = false;
            }
        });
        await loadResultadosSims();
        renderSimList();
        const vigentes = sims.filter(s => s.vigente !== false).length;
        const pendientes = simsConResultado
            ? sims.filter(s => s.vigente !== false && tieneResultado(s) === false).length
            : null;
        simListStatus.className = (modelVersionError || resultadosError) ? 'live-status warn' : 'live-status ok';
        simListStatus.textContent = `${vigentes} vigentes · ${sims.length} totales`
            + (pendientes != null ? ` · ${pendientes} sin resultado` : '')
            + ` · modelo ${data.modelo_version || '—'}`
            + (serviceModeloDesactualizado
                ? ' · ⚠ servicio desactualizado: reinicia ALETHEIA_PREDICT; las filas nuevas quedarán viejas al reiniciar'
                : '')
            + (modelVersionError
                ? ` · ⚠ no se pudo leer /modelo_version (${modelVersionError}); vigencia según el servicio`
                : '')
            + (resultadosError
                ? ` · ⚠ filtro por resultado no disponible (${resultadosError})`
                : '');
    } catch (e) {
        simListStatus.className = 'live-status err';
        simListStatus.textContent = `Servicio de predicción no disponible: ${e.message}`;
    }
}

// ¿El partido de la simulación ya tiene resultado real?
// true = jugado, false = pendiente, null = sin dato (sin id o endpoint caído).
function tieneResultado(s) {
    const mid = Number(s && s.match_id) || 0;
    if (!mid || !simsConResultado) return null;
    return simsConResultado.has(mid);
}

// Lee de la DB propia (GET /api/partidos/resultados) qué match_id ya están
// jugados; así EN VIVO separa las predicciones pendientes de las comparables.
async function loadResultadosSims() {
    resultadosError = null;
    const ids = sims.map(s => Number(s.match_id) || 0).filter(id => id > 0);
    if (!ids.length) {
        simsConResultado = new Set();
        simsInfo = {};
        return;
    }
    try {
        const res = await fetch(`${API}/partidos/resultados?match_ids=${ids.join(',')}`);
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || `HTTP ${res.status}`);
        simsConResultado = new Set((data.con_resultado || []).map(Number));
        simsInfo = (data.partidos && typeof data.partidos === 'object') ? data.partidos : {};
    } catch (e) {
        simsConResultado = null;
        simsInfo = {};
        resultadosError = e.message || 'servicio no disponible';
    }
}

// Refresca los chips del filtro con el conteo de la lista base (la que ya
// aplica "mostrar no vigentes").
function actualizarFiltroSims(base) {
    if (!simFilterBtns.length) return;
    const cuenta = {
        todas: base.length,
        pendiente: base.filter(s => tieneResultado(s) !== true).length,
        resultado: base.filter(s => tieneResultado(s) === true).length,
    };
    simFilterBtns.forEach(btn => {
        const f = btn.dataset.filtro || 'todas';
        btn.classList.toggle('active', f === filtroResultado);
        // El conteo va en un span de ancho fijo: el texto del chip no cambia
        // de tamaño al pasar de (0) a (24) y no desplaza el resto del header (CLS).
        let cnt = btn.querySelector('.sim-filter-count');
        if (!cnt) {
            btn.textContent = `${btn.dataset.label || f.toUpperCase()} `;
            cnt = document.createElement('span');
            cnt.className = 'sim-filter-count';
            btn.appendChild(cnt);
        }
        cnt.textContent = `(${cuenta[f] || 0})`;
    });
}

// Píldora de la SERIE: P del motor al ganador real de la serie (verde/naranja/
// rojo por banda) + ✓/✕ si era su favorito. Se rellena en 2º plano con /serie.
function seriePillHtml(mid, info) {
    if (!info || info.score_a == null || info.score_b == null) return '';
    const s = simSerie[mid];
    if (s && s.sin_pool) {
        return `<span class="si-pred-pill dim" data-serie="${mid}" title="Sin pool de veto completo (1/3/5 mapas): no se calcula la P de serie.">SERIE sin pool</span>`;
    }
    if (s && s.p_real != null) {
        const marca = Number(s.p_real) >= 0.5 ? '✓' : '✕';
        const tt = `Serie: P del motor (Glicko + temperatura) al ganador real = ${pct(s.p_real)} (motor ${pct(s.p_a)} / ${pct(s.p_b)}). ${marca === '✓' ? 'Era su favorito' : 'Upset de serie'}.`;
        return `<span class="si-pred-pill ${probBandClass(s.p_real)}" data-serie="${mid}" title="${tt}">SERIE ${pct(s.p_real)} ${marca}</span>`;
    }
    if (s && s.error) return '';
    return `<span class="si-pred-pill dim" data-serie="${mid}" title="Calculando P(serie) del motor (motor + temperatura)…">SERIE …</span>`;
}

// MOTOR del enfrentamiento (P del motor Glicko, plana entre mapas y lados).
// No se persiste por mapa: se obtiene de la raíz de POST /serie y por eso solo
// se pinta cuando el /serie de la fila ya respondió. NUNCA se usa
// `prob_victoria_a` (que es el ESC por mapa/lado) como MOTOR.
function motorItemHtml(info, mid) {
    const s = simSerie[mid];
    if (s && s.p_motor_a != null) {
        return `<span class="si-pred-item" data-motor="${mid}" title="P del MOTOR Glicko de ${escapeHtml((info && info.team_a) || 'A')} / ${escapeHtml((info && info.team_b) || 'B')}: una sola por enfrentamiento, plana entre mapas y lados; decide la serie.">MOTOR ${pct(s.p_motor_a)}/${pct(s.p_motor_b)}</span>`;
    }
    // Ya respondió /serie (sin motor, p. ej. caché DB) o falló: se oculta.
    if (s && !s.cargando) return '';
    // Partido pendiente: la lista no pide /serie (solo se calcula con pool y
    // resultado), así que no se inventa un MOTOR.
    if (info && info.score_a == null) return '';
    return `<span class="si-pred-item dim" data-motor="${mid}" title="La P del motor no se persiste por mapa; se lee de la raíz de /serie.">MOTOR …</span>`;
}

// Resumen compacto "predicción (ESC) vs realidad" de una fila de la lista.
// Orden: MOTOR (P Glicko del enfrentamiento, de la raíz de /serie; plana) ·
// REAL (marcador de serie) · SERIE (P del ganador real, color por banda) ·
// MAPAS (acierto del favorito ESC, color por tasa) · P(MAPA) (calibración
// media del ESC) · puntos por mapa.
function simResumenHtml(info, res, mid) {
    if (!info) return '';
    const motor = motorItemHtml(info, mid);
    if (res !== true || info.score_a == null || info.score_b == null) {
        return `<div class="si-pred">${motor}</div>`;
    }
    const ganaA = Number(info.score_a) > Number(info.score_b);
    const ganaB = Number(info.score_b) > Number(info.score_a);
    const dots = (info.mapas || []).map(m => {
        const p = m.p_ganador != null ? `P(real) ${pct(m.p_ganador)} · ${m.p_ganador >= 0.5 ? 'favorito ESC ✓' : 'upset ✕'}` : 'sin predicción cacheada';
        return `<span class="si-dot ${probBandClass(m.p_ganador)}" title="${escapeHtml(m.map_name || '')}: ${p}"></span>`;
    }).join('');
    const n = Number(info.n_mapas) || 0;
    const acc = n ? (Number(info.favoritos_ok) || 0) / n : null;
    return `<div class="si-pred">
        ${motor}
        <span class="si-pred-item">REAL <b class="${ganaA ? 'gana' : ''}">${info.score_a}</b>-<b class="${ganaB ? 'gana' : ''}">${info.score_b}</b></span>
        ${seriePillHtml(mid, info)}
        <span class="si-pred-pill ${probAccClass(acc)}" title="ACIERTO del favorito ESC por mapa (${info.favoritos_ok || 0}/${n}). Verde ≥67% · naranja ≥50% · rojo <50%.">MAPAS ${info.favoritos_ok || 0}/${n} (${pct(acc)})</span>
        <span class="si-pred-item ${probBandClass(info.p_real_media)}" title="P(MAPA): probabilidad media que la predicción ESC (por mapa/lado) dio al ganador real de cada mapa (calibración; no es la tasa de acierto).">P(MAPA) ${pct(info.p_real_media)}</span>
        ${dots ? `<span class="si-dots">${dots}</span>` : ''}
      </div>`;
}

// Mapas para calcular la P de serie: pool del veto (picks + decider, en orden).
// Es clave pasar el pool completo: con solo los mapas jugados, un bo3 terminado
// 2-0 le da al endpoint /serie una lista de 2 (la API responde 400). Si la lista
// no es 1/3/5 se devuelve [] y NO se llama al servicio.
function mapasSerieDe(info) {
    const nombres = (info && Array.isArray(info.serie_mapas) && info.serie_mapas.length)
        ? info.serie_mapas
        : ((info && Array.isArray(info.mapas)) ? info.mapas.map(m => m.map_name) : []);
    const limpios = nombres.filter(Boolean);
    if (!VETO_COMPLETO.includes(limpios.length)) return [];
    return limpios.map(m => ({ map_name: m, lado_inicial_a: 'attack' }));
}

// Encola la P de serie (POST /serie, desde la caché) de UNA fila. Solo se
// calcula para filas visibles/seleccionadas con resultado y vigentes; el
// resultado queda cacheado por sesión (`simSerie`) y no se repite.
function encolarSerie(s, gen = serieGen) {
    if (!s || gen !== serieGen) return;
    const mid = Number(s && s.match_id) || 0;
    if (!mid || simSerie[mid] || serieIntentados.has(mid)) return;
    if (simIsStale(s) || tieneResultado(s) !== true) return;
    const info = simsInfo[mid];
    if (!info) return;
    if (!mapasSerieDe(info).length) {
        // Sin pool de veto completo (1/3/5): no se llama al servicio.
        simSerie[mid] = { sin_pool: true };
        actualizarSeriesLista();
        return;
    }
    serieIntentados.add(mid);
    serieCola.push({ s, gen });
    bombearSerie();
}

// Cola con concurrencia acotada (2): las peticiones visibles no bloquean la
// lista y no se martilla el worker con 12 POST seguidos.
function bombearSerie() {
    while (serieActivos < SERIE_CONCURRENCY && serieCola.length) {
        const { s, gen } = serieCola.shift();
        serieActivos++;
        procesarSerie(s, gen).finally(() => {
            serieActivos--;
            bombearSerie();
        });
    }
}

async function procesarSerie(s, gen) {
    if (gen !== serieGen) return;
    const mid = Number(s.match_id) || 0;
    const info = simsInfo[mid];
    if (!info || simSerie[mid]) return;
    const mapas = mapasSerieDe(info);
    if (!mapas.length) {
        simSerie[mid] = { sin_pool: true };
        actualizarSeriesLista();
        return;
    }
    simSerie[mid] = { cargando: true };
    const ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null;
    const timer = ctrl ? setTimeout(() => ctrl.abort(), SERIE_TIMEOUT_MS) : null;
    try {
        const res = await proxyFetch('/serie', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                match_id: mid,
                equipo_a: s.equipo_a,
                equipo_b: s.equipo_b,
                mapas,
            }),
            signal: ctrl ? ctrl.signal : undefined,
        });
        const d = await res.json();
        if (gen !== serieGen) return;
        if (!d.ok) throw new Error(d.error || `HTTP ${res.status}`);
        const ganaA = Number(info.score_a) > Number(info.score_b);
        const num = v => (v == null || v === '' || isNaN(Number(v))) ? null : Number(v);
        const pA = num(d.prob_serie_a);
        const pB = num(d.prob_serie_b);
        // El MOTOR (prob_motor_a/b) vive en la raíz de /serie (no se
        // persiste por mapa); el ESC es prob_victoria_a de cada mapa.
        simSerie[mid] = {
            p_a: pA, p_b: pB, p_real: (pA != null) ? (ganaA ? pA : pB) : null,
            p_motor_a: num(d.prob_motor_a),
            p_motor_b: num(d.prob_motor_b),
        };
    } catch (e) {
        if (gen === serieGen) simSerie[mid] = { error: true };
    } finally {
        if (timer) clearTimeout(timer);
        if (gen === serieGen) actualizarSeriesLista();
    }
}

// Observa las tarjetas renderizadas y encola su /serie recién cuando entran en
// el viewport (con margen): al cargar solo se piden las visibles, y el resto
// se completan al hacer scroll sin volver a lanzar lotes en cada render.
function observarSeriesVisibles() {
    if (simObserver) { simObserver.disconnect(); simObserver = null; }
    const items = [...simList.querySelectorAll('.sim-item')].filter(el => el.__sim);
    if (!items.length) return;
    if (typeof IntersectionObserver === 'undefined') {
        items.forEach(el => encolarSerie(el.__sim));
        return;
    }
    const gen = serieGen;
    simObserver = new IntersectionObserver(entradas => {
        entradas.forEach(en => {
            if (!en.isIntersecting) return;
            const sim = en.target.__sim;
            simObserver.unobserve(en.target);
            if (sim) encolarSerie(sim, gen);
        });
    }, { rootMargin: '200px 0px' });
    items.forEach(el => simObserver.observe(el));
}

// Refresca en sitio las píldoras SERIE y los MOTOR ya pintados (sin re-render
// de la lista). El MOTOR llega en 2º plano (raíz de /serie), no de la caché por
// mapa.
function actualizarSeriesLista() {
    document.querySelectorAll('[data-serie]').forEach(el => {
        const mid = Number(el.dataset.serie);
        const info = simsInfo[mid];
        const html = seriePillHtml(mid, info);
        if (!html) { el.remove(); return; }
        el.outerHTML = html;
    });
    document.querySelectorAll('[data-motor]').forEach(el => {
        const mid = Number(el.dataset.motor);
        const info = simsInfo[mid] || {};
        const html = motorItemHtml(info, mid);
        if (!html) { el.remove(); return; }
        el.outerHTML = html;
    });
}

function renderSimList() {
    const base = showStale ? sims : sims.filter(s => s.vigente !== false);
    const list = base.filter(s => {
        if (filtroResultado === 'pendiente') return tieneResultado(s) !== true;
        if (filtroResultado === 'resultado') return tieneResultado(s) === true;
        return true;
    });
    actualizarFiltroSims(base);
    simList.innerHTML = '';
    if (!list.length) {
        const msg = !base.length
            ? 'No hay simulaciones preparadas. Ve a <strong>PREPARAR PARTIDO</strong>.'
            : filtroResultado === 'resultado'
                ? 'No hay simulaciones con resultado real con este filtro.'
                : filtroResultado === 'pendiente'
                    ? 'No hay simulaciones pendientes de resultado con este filtro.'
                    : 'No hay simulaciones que mostrar.';
        simList.innerHTML = `<div class="live-hint" style="padding:14px">${msg}</div>`;
        simList.setAttribute('aria-busy', 'false');
        return;
    }
    list.forEach((s, idx) => {
        const vigente = s.vigente !== false;
        const res = tieneResultado(s);
        const info = simsInfo[s.match_id] || null;
        // La primera fila carga sus logos en prioridad alta (LCP); el resto lazy.
        const prio = idx < 2;
        const resBadge = res === true
            ? '<span class="si-res ok" title="Ya jugado: hay resultado real en la DB">CON RESULTADO</span>'
            : res === false
                ? '<span class="si-res pend" title="Pendiente: el partido todavía no tiene resultado real">PENDIENTE</span>'
                : '';
        const item = document.createElement('div');
        item.className = 'sim-item' + (sameSim(current, s) ? ' selected' : '') + (vigente ? '' : ' stale');
        item.__sim = s;
        const mid = s.match_id ? `#${s.match_id}` : 'sin id';
        const aName = (info && info.team_a) || s.equipo_a;
        const bName = (info && info.team_b) || s.equipo_b;
        item.innerHTML = `
      <div class="si-top">
        <div class="si-teams">
          ${teamLogo(aName, info && info.team_a_tag, info && info.team_a_id, '', prio)}
          <span class="si-vs">VS</span>
          ${teamLogo(bName, info && info.team_b_tag, info && info.team_b_id, '', prio)}
        </div>
        ${resBadge}
      </div>
      <div class="si-meta">${mid} · ${s.mapas != null ? s.mapas : '?'} mapas · ${(s.n_sim || 0).toLocaleString()} sims${vigente ? '' : ' · ⚠ re-preparar'}</div>
      ${simResumenHtml(info, res, s.match_id)}
      <div class="si-actions">
        <button class="si-btn" data-act="id" title="Asignar/corregir el ID de vlr.gg">✎ ID</button>
        <button class="si-btn danger" data-act="del" title="Borrar estas predicciones">🗑 BORRAR</button>
      </div>`;
        item.addEventListener('click', () => selectSim(s));
        item.querySelector('[data-act="id"]').addEventListener('click', e => { e.stopPropagation(); asignarId(s); });
        item.querySelector('[data-act="del"]').addEventListener('click', e => { e.stopPropagation(); borrarSim(s); });
        simList.appendChild(item);
    });
    simList.setAttribute('aria-busy', 'false');
    VCT.aplicarMedia(simList);
    observarSeriesVisibles();
}

// Asigna/corrige el match_id (id de vlr.gg) de un enfrentamiento ya preparado.
async function asignarId(sim) {
    const actual = sim.match_id || 0;
    const raw = window.prompt(
        `ID de vlr.gg para ${sim.equipo_a} vs ${sim.equipo_b}\n(puedes pegar la URL o el número):`,
        actual ? String(actual) : ''
    );
    if (raw == null) return;
    const nuevo = parseMatchId(raw);
    if (!nuevo) { window.alert('ID inválido.'); return; }
    try {
        const res = await proxyFetch('/asociar', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                equipo_a: sim.equipo_a, equipo_b: sim.equipo_b,
                match_id: nuevo, desde_match_id: actual,
            }),
        });
        const data = await res.json();
        if (!data.ok) { window.alert(`Error: ${data.error || 'no se pudo asociar'}`); return; }
        window.alert(`✓ ${data.filas_actualizadas != null ? data.filas_actualizadas : 0} filas reasignadas a #${nuevo}.`);
        await loadSimulaciones();
    } catch (e) {
        window.alert(`Sin conexión con ALETHEIA_PREDICT: ${e.message}`);
    }
}

// Borra las predicciones (mapa + serie) de un enfrentamiento.
async function borrarSim(sim) {
    const mid = sim.match_id || 0;
    const etiqueta = `${sim.equipo_a} vs ${sim.equipo_b} (${mid ? '#' + mid : 'sin id'})`;
    if (!window.confirm(`¿Borrar las predicciones de ${etiqueta}? Esta acción no se puede deshacer.`)) return;
    try {
        const res = await proxyFetch('/borrar', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ equipo_a: sim.equipo_a, equipo_b: sim.equipo_b, match_id: mid }),
        });
        const data = await res.json();
        if (!data.ok) { window.alert(`Error: ${data.error || 'no se pudo borrar'}`); return; }
        window.alert(`✓ ${data.filas_borradas != null ? data.filas_borradas : 0} filas borradas.`);
        if (sameSim(current, sim)) {
            current = null;
            liveSection.style.display = 'none';
        }
        await loadSimulaciones();
    } catch (e) {
        window.alert(`Sin conexión con ALETHEIA_PREDICT: ${e.message}`);
    }
}

// Modelo vigente del servicio (GET /modelo_version, vía proxy). `desactualizado`
// avisa de un reentreno/cambio de core/ pendiente de reiniciar el servicio.
async function loadModeloVersion() {
    modelVersionError = null;
    try {
        const res = await proxyFetch('/modelo_version');
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || `HTTP ${res.status}`);
        serviceModelVersion = data.modelo_version;
        serviceModeloDesactualizado = data.desactualizado === true;
    } catch (e) {
        modelVersionError = e.message || 'servicio no disponible';
    }
    refrescarAvisoModelo();
}

// Refleja el aviso de servicio desactualizado sin repetirlo si ya está pintado.
function refrescarAvisoModelo() {
    if (!serviceModeloDesactualizado || !simListStatus) return;
    const aviso = '⚠ servicio desactualizado: reinicia ALETHEIA_PREDICT; las filas nuevas quedarán viejas al reiniciar';
    if (!simListStatus.textContent.includes('servicio desactualizado')) {
        simListStatus.className = 'live-status warn';
        simListStatus.textContent = simListStatus.textContent
            ? `${simListStatus.textContent} · ${aviso}`
            : aviso;
    }
}

// ¿La simulación seleccionada está desactualizada? (su modelo_version difiere
// del vigente, o el servicio ya la marca vigente:false).
function simIsStale(s) {
    if (!s) return false;
    if (s.vigente === false) return true;
    if (s.modelo_version && serviceModelVersion) return s.modelo_version !== serviceModelVersion;
    return false;
}

// ¿Qué detalle falta en la caché del enfrentamiento actual? (filas anteriores al
// cambio, sin `marcadores` o sin `economia`). Sirve para ofrecer RE-PRECALCULAR.
function detalleFaltante() {
    const nada = { marcadores: false, economia: false };
    if (!liveBulk) return nada;
    const filas = Object.values(liveBulk).filter(Boolean);
    if (!filas.length) return nada;
    return {
        marcadores: filas.every(p => !Array.isArray(p.marcadores) || !p.marcadores.length),
        economia: filas.every(p => !p.economia || typeof p.economia !== 'object'),
    };
}

// Cabecera del enfrentamiento seleccionado + botón RE-PRECALCULAR si aplica.
function renderSimHead(s, needsRepre) {
    const staleModel = simIsStale(s);
    const falta = detalleFaltante();
    const motivos = [];
    if (staleModel) motivos.push('modelo cambió');
    if (falta.marcadores) motivos.push('sin marcadores');
    if (falta.economia) motivos.push('sin economía');
    const info = simsInfo[s.match_id] || null;
    const res = tieneResultado(s);
    const aName = (info && info.team_a) || s.equipo_a;
    const bName = (info && info.team_b) || s.equipo_b;
    selSimHead.innerHTML = `
    <div class="ss-head-teams">
      ${teamLogo(aName, info && info.team_a_tag, info && info.team_a_id, 'md')}
      <span class="si-vs">VS</span>
      ${teamLogo(bName, info && info.team_b_tag, info && info.team_b_id, 'md')}
    </div>
    <div class="ss-head-meta">${s.match_id ? 'PARTIDO #' + s.match_id : 'sin id'} · ${s.n_sim ? Number(s.n_sim).toLocaleString() + ' sims' : ''} · modelo ${s.modelo_version || '—'}${needsRepre ? ` · <span style="color:var(--orange)">⚠ RE-PRECALCULAR${motivos.length ? ' (' + motivos.join(', ') + ')' : ''}</span>` : ''}</div>
    ${res === true ? simResumenHtml(info, true, s.match_id) : ''}
    <div class="ss-head-actions">
      <button class="btn-nav" id="btnReprecalcular"${needsRepre ? '' : ' style="display:none"'}>↻ RE-PRECALCULAR</button>
    </div>`;
    const btnRepre = document.getElementById('btnReprecalcular');
    if (btnRepre) btnRepre.addEventListener('click', () => reprecalcular(s));
    VCT.aplicarMedia(selSimHead);
}

// ─── SELECCIÓN DE SIMULACIÓN ──────────────────────────────────────────────────
async function selectSim(s) {
    current = s;
    matchMaps = [];
    liveMap = null;
    liveBulk = null;
    ultimaSerie = null;   // el MOTOR/la serie se recalculan para este enfrentamiento
    liveSection.style.display = 'block';
    encolarSerie(s);      // la fila seleccionada siempre cuenta, esté o no a la vista

    liveTeamALabel.textContent = s.equipo_a;
    renderSimHead(s, simIsStale(s));

    await loadLiveBulkForCurrent();
    // Tras leer la caché, puede que las filas sean antiguas (sin marcadores) o
    // de otro modelo: recomputa el motivo de re-precalcular con la info real.
    const falta = detalleFaltante();
    renderSimHead(s, simIsStale(s) || falta.marcadores || falta.economia);
    renderLiveMapPicker();
    renderLiveDetail();
    syncSerieBuilder();
    marcarSeriePendiente();
    if (panelComparacion && panelComparacion.style.display !== 'none') {
        fetchComparacion();
        fetchScorecard();
    }
    renderSimList();
}

// Re-precalcula el enfrentamiento actual con forzar:true (por el proxy; el POST
// responde 202 al instante) y refresca la lista. Un 429 ("ya hay un precálculo
// en curso") se adopta como job activo, nunca como error fatal.
async function reprecalcular(s) {
    if (!s || recalculating) return;
    const clave = claveEnfrentamiento(s.equipo_a, s.equipo_b, s.match_id);

    // Job persistido de ese enfrentamiento: reanudar el polling en vez de
    // volver a POSTear (sobrevive a recargas y se comparte entre pestañas).
    const guardado = jobGuardado(s.equipo_a, s.equipo_b, s.match_id);
    if (guardado) { await atenderRecalcGuardado(guardado, s); return; }

    recalculating = true;
    recalcStopped = false;
    const btn = document.getElementById('btnReprecalcular');
    if (btn) { btn.disabled = true; btn.textContent = '↻ RECALCULANDO…'; }
    simListStatus.className = 'live-status warn';
    simListStatus.textContent = `Re-precalculando ${s.equipo_a} vs ${s.equipo_b}… no cierres la pestaña.`;
    const t0 = Date.now();
    const ctrl = new AbortController();
    const corte = setTimeout(() => ctrl.abort(), RECALC_POST_TIMEOUT_MS);
    try {
        const res = await proxyFetch('/precalcular', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                equipo_a: s.equipo_a,
                equipo_b: s.equipo_b,
                match_id: s.match_id || 0,
                n_sim: s.n_sim || 10000,
                forzar: true,
            }),
            signal: ctrl.signal,
        });
        let data = null;
        try { data = await res.json(); } catch (e) { data = null; }

        // 429 = ya hay un precálculo en curso: no es error fatal. Si trae
        // `job_id` (API nueva) se adopta; si no, se reanuda el job guardado.
        const enCurso = res.status === 429
            || (data && data.ok === false && /en curso/i.test(String(data.error || '')));
        if (enCurso) {
            let job = jobGuardado(s.equipo_a, s.equipo_b, s.match_id);
            if (!job && data && data.job_id) {
                job = {
                    clave, job_id: data.job_id, equipo_a: s.equipo_a, equipo_b: s.equipo_b,
                    match_id: s.match_id || 0, n_sim: s.n_sim || 10000,
                    total: data.total || 26, modelo_version: data.modelo_version || null,
                    estado: data.estado || 'en_proceso', started_at: Date.now(),
                };
                guardarJob(job);
            }
            if (job) {
                recalculating = false;
                await atenderRecalcGuardado(job, s);
                return;
            }
            simListStatus.className = 'live-status warn';
            simListStatus.textContent = '⚠ Ya hay un precálculo en curso (otra pestaña o usuario). Espera a que termine y pulsa RE-PRECALCULAR para reintentar.';
            return;
        }

        if (res.status >= 500 || !data) {
            simListStatus.className = 'live-status err';
            simListStatus.textContent = 'El servicio de predicción no respondió; el job puede seguir en curso. Pulsa RE-PRECALCULAR para reintentar.';
            return;
        }
        if (!data.ok) {
            simListStatus.className = 'live-status err';
            simListStatus.textContent = `Error: ${data.error || 'no se pudo re-precalcular'}`;
            return;
        }
        // Async (202): poll del job; sync: ya terminó.
        if (data.job_id) {
            guardarJob({
                clave, job_id: data.job_id, equipo_a: s.equipo_a, equipo_b: s.equipo_b,
                match_id: s.match_id || 0, n_sim: s.n_sim || 10000,
                total: data.total || 26, modelo_version: data.modelo_version || null,
                estado: 'en_proceso', started_at: t0,
            });
            const ok = await pollPrecalcular(data.job_id, t0);
            quitarJob(clave);
            if (!ok) {
                // Restaura el botón para poder reintentar sin recargar la página.
                renderSimHead(s, simIsStale(s) || detalleFaltante().marcadores || detalleFaltante().economia);
                return;
            }
        }
        await finalizarRecalc(s);
    } catch (e) {
        if (e && e.name === 'AbortError') {
            simListStatus.className = 'live-status warn';
            simListStatus.textContent = 'El POST tardó más de lo esperado; el job puede seguir en curso. Pulsa RE-PRECALCULAR para reintentar.';
        } else {
            simListStatus.className = 'live-status err';
            simListStatus.textContent = `Servicio de predicción no disponible: ${e.message}`;
        }
    } finally {
        clearTimeout(corte);
        recalculating = false;
    }
}

// Atiende un job ya persistido/adoptado (resume tras recarga, 429 con job_id…).
async function atenderRecalcGuardado(job, s) {
    recalculating = true;
    recalcStopped = false;
    const btn = document.getElementById('btnReprecalcular');
    if (btn) { btn.disabled = true; btn.textContent = '↻ RECALCULANDO…'; }
    simListStatus.className = 'live-status warn';
    simListStatus.textContent = `Re-precalculando ${s.equipo_a} vs ${s.equipo_b}… no cierres la pestaña.`;
    try {
        const ok = await pollPrecalcular(job.job_id, job.started_at || Date.now());
        quitarJob(job.clave);
        if (!ok) {
            renderSimHead(s, simIsStale(s) || detalleFaltante().marcadores || detalleFaltante().economia);
            return;
        }
        await finalizarRecalc(s);
    } finally {
        recalculating = false;
    }
}

async function finalizarRecalc(s) {
    simListStatus.className = 'live-status ok';
    simListStatus.textContent = '✓ Re-precalculo listo.';
    await loadModeloVersion();
    await loadSimulaciones();
    if (current && sameSim(current, s)) await selectSim(current);
}

// Poll del job de precalculo con cota de tiempo y cancelación (mismo contrato
// que PREPARAR). t0 marca el inicio del POST para medir el total transcurrido.
// Un 5xx del proxy/túnel se reintenta (no aborta el poll).
async function pollPrecalcular(jobId, t0) {
    let fails = 0;
    while (!recalcStopped) {
        const elapsed = Math.floor((Date.now() - t0) / 1000);
        if (Date.now() - t0 > RECALC_TIMEOUT_MS) {
            simListStatus.className = 'live-status err';
            simListStatus.textContent = `⚠ El re-precalculo superó el límite de ${Math.round(RECALC_TIMEOUT_MS / 60000)} min (${elapsed}s). Pulsa RE-PRECALCULAR para reintentar.`;
            return false;
        }
        let r, jd;
        try {
            r = await proxyFetch(`/precalcular/estado?job_id=${encodeURIComponent(jobId)}`);
            if (r.status === 404) {
                simListStatus.className = 'live-status err';
                simListStatus.textContent = 'El servicio perdió el job. Reintenta RE-PRECALCULAR.';
                return false;
            }
            jd = await r.json();
            if (r.status >= 500) {
                fails++;
                simListStatus.className = 'live-status warn';
                simListStatus.innerHTML = `⚠ El servicio no responde (HTTP ${r.status}). Reintento #${fails} · ${elapsed}s · <button class="link-cancel" id="btnCancelRecalc">cancelar espera</button>`;
                await new Promise(r2 => setTimeout(r2, Math.min(RECALC_POLL_MS + fails * 500, 10000)));
                continue;
            }
            fails = 0;
        } catch {
            fails++;
            simListStatus.className = 'live-status warn';
            simListStatus.innerHTML = `⚠ Sin conexión (reintento #${fails}) · ${elapsed}s · <button class="link-cancel" id="btnCancelRecalc">cancelar espera</button>`;
            await new Promise(r2 => setTimeout(r2, Math.min(RECALC_POLL_MS + fails * 500, 10000)));
            continue;
        }
        const job = jd && jd.job;
        if (!jd || !jd.ok || !job) {
            simListStatus.className = 'live-status err';
            simListStatus.textContent = `Error: ${(jd && jd.error) || 'job no encontrado'}`;
            return false;
        }
        const prog = Math.round((job.progreso || 0) * 100);
        simListStatus.className = 'live-status warn';
        simListStatus.innerHTML = `↻ ${prog}% · mapa ${job.mapas_hechos || 0}/${Math.ceil((job.total || 26) / 2)} · ${elapsed}s · <button class="link-cancel" id="btnCancelRecalc">cancelar espera</button>`;
        if (job.estado === 'listo') return true;
        if (job.estado === 'error') {
            simListStatus.className = 'live-status err';
            simListStatus.textContent = `Error: ${job.error || 'no se pudo re-precalcular'}`;
            return false;
        }
        await new Promise(r2 => setTimeout(r2, RECALC_POLL_MS));
    }
    simListStatus.className = 'live-status warn';
    simListStatus.textContent = 'Espera cancelada. Pulsa RE-PRECALCULAR para reintentar.';
    return false;
}

async function loadLiveBulkForCurrent() {
    if (!current) return;
    liveBulk = {};
    liveBulkError = null;
    const params = new URLSearchParams();
    if (current.match_id > 0) {
        params.set('match_id', current.match_id);
    } else {
        params.set('equipo_a', current.equipo_a);
        params.set('equipo_b', current.equipo_b);
    }
    try {
        const res = await proxyFetch(`/predicciones?${params.toString()}`);
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || `HTTP ${res.status}`);
        if (Array.isArray(data.predicciones)) {
            data.predicciones.forEach(p => {
                if (p && p.map_name) liveBulk[`${p.map_name}|${p.lado_inicial_a}`] = p;
            });
        }
        // `data.modelo_version` es la versión VIGENTE del servicio, no la de las
        // filas. La versión real de la cache de este enfrentamiento está en cada
        // fila (`p.modelo_version`), y es la que determina la vigencia.
        if (data.modelo_version) {
            serviceModelVersion = data.modelo_version;
            modelVersionError = null;
        }
        const filas = Object.values(liveBulk);
        const fila = filas.find(p => p && p.modelo_version);
        if (fila) current.modelo_version = fila.modelo_version;
    } catch (e) {
        liveBulk = {};
        liveBulkError = e.message || 'servicio no disponible';
    }
}

// ─── PANEL MAPA / BANDO (lee de liveBulk, no llama al servicio en cada clic) ──
function renderLiveMapPicker() {
    if (mapsLoading) {
        liveMapPicker.innerHTML = '<div class="live-hint" style="padding:12px">Cargando mapas...</div>';
        return;
    }
    if (mapsError) {
        liveMapPicker.innerHTML = `<div class="live-hint" style="padding:12px;color:var(--red)">No se pudieron cargar los mapas (${escapeHtml(mapsError)}). <button class="link-cancel" id="btnRetryMaps">reintentar</button></div>`;
        const retry = document.getElementById('btnRetryMaps');
        if (retry) retry.addEventListener('click', loadAvailableMaps);
        return;
    }
    if (!availableMaps.length) {
        liveMapPicker.innerHTML = '<div class="live-hint" style="padding:12px">Sin mapas disponibles.</div>';
        return;
    }
    if (!liveMap || !availableMaps.includes(liveMap)) liveMap = availableMaps[0];

    liveMapPicker.innerHTML = '';
    availableMaps.forEach(m => {
        const row = liveBulk ? liveBulk[`${m}|${liveSide}`] : null;
        // ESC = predicción servida para ESTE mapa y ESTE lado (varía por mapa y
        // lado); hist = análisis histórico del mapa (contrato: no es la
        // predicción). El MOTOR es plano y solo se muestra en la serie.
        const escP = row ? row.prob_victoria_a : null;
        const peso = escPeso(row);
        const am = row && row.analisis_mapa ? row.analisis_mapa : null;
        const histP = (am && am.p_mapa_a != null) ? Number(am.p_mapa_a) : null;
        const ot = row ? row.prob_overtime : null;
        const meta = row
            ? `<span class="mqp-prob" title="ESC: predicción por mapa/lado (la que se sirve; puede variar por mapa y lado). El MOTOR plano decide la serie.">ESC ${pct(escP)}</span>`
            + (peso != null ? `<span class="mqp-esc" title="conf: peso/confianza del ESC en este mapa/lado (n_min/(n_min+10)); más alto = más historial lo respalda.">conf ${pct(peso)}</span>` : '')
            + `<span class="mqp-ot" title="P(overtime) del Monte Carlo de este mapa">OT ${pct(ot)}</span>`
            + ((histP != null && !isNaN(histP))
                ? `<span class="mqp-hist" title="Análisis histórico del mapa (no es la predicción)">hist ${pct(histP)}</span>`
                : '')
            : `<span class="mqp-prob">—</span>`;
        const tile = document.createElement('button');
        const enSerie = matchMaps.some(mm => mm.map_name === m);
        tile.className = 'mqp-tile' + (m === liveMap ? ' mqp-selected' : '') + (enSerie ? ' mqp-in-serie' : '');
        tile.innerHTML = `
      <img class="mqp-img" src="../multimedia/maps/${m.toUpperCase()}.avif" alt="${m}" onerror="this.style.display='none'">
      <span class="mqp-name">${m.toUpperCase()}</span>
      ${meta}`;
        tile.addEventListener('click', () => {
            liveMap = m;
            if (!enSerie && matchMaps.length < maxMapsSel) {
                matchMaps.push({ map_name: m, lado_inicial_a: liveSide });
                syncSerieBuilder();
                marcarSeriePendiente();
            }
            renderLiveMapPicker();
            renderLiveDetail();
        });
        liveMapPicker.appendChild(tile);
    });
}

function renderLiveDetail() {
    if (!current || !liveMap) return;
    if (liveBulkError) {
        liveCards.innerHTML = '';
        if (liveScoreboard) { liveScoreboard.className = 'scoreboard-block'; liveScoreboard.innerHTML = ''; }
        if (liveEconomia) { liveEconomia.className = 'economia-block'; liveEconomia.innerHTML = ''; }
        liveStatus.className = 'live-status err';
        liveStatus.textContent = `No se pudieron leer las predicciones cacheadas: ${liveBulkError}. Reintenta con ACTUALIZAR o RE-PRECALCULAR.`;
        return;
    }
    const key = `${liveMap}|${liveSide}`;
    const row = liveBulk ? liveBulk[key] : null;
    if (row) {
        paintLiveDetail(row);
        return;
    }
    // Solo si falta el dato, se consulta al servicio (una vez).
    fetchPrediccion(liveMap, liveSide);
}

async function fetchPrediccion(map, side) {
    liveStatus.className = 'live-status';
    liveStatus.textContent = 'Consultando...';
    liveCards.innerHTML = '';
    if (liveScoreboard) { liveScoreboard.className = 'scoreboard-block'; liveScoreboard.innerHTML = ''; }
    const params = new URLSearchParams();
    if (current.match_id > 0) {
        params.set('match_id', current.match_id);
    } else {
        params.set('equipo_a', current.equipo_a);
        params.set('equipo_b', current.equipo_b);
    }
    params.set('map_name', map);
    params.set('lado_inicial_a', side);
    try {
        const res = await proxyFetch(`/prediccion?${params.toString()}`);
        const data = await res.json();
        if (res.status === 404 || !data.ok) {
            liveStatus.className = 'live-status warn';
            liveStatus.textContent = 'Sin predicción cacheada para este mapa.';
            return;
        }
        const p = data.prediccion || {};
        if (liveBulk) liveBulk[`${map}|${side}`] = p;
        paintLiveDetail(p, data.modelo_version, data.vigente);
    } catch (e) {
        liveStatus.className = 'live-status err';
        liveStatus.textContent = `Servicio de predicción no disponible: ${e.message}`;
    }
}

// Top de marcadores por mapa. `marcadores` viene del backend ordenado por prob
// desc (puede faltar en filas de cache anteriores: se oculta sin romper).
function renderScoreboard(p) {
    if (!liveScoreboard) return;
    const lista = Array.isArray(p && p.marcadores) ? p.marcadores.filter(m => m) : [];
    if (!lista.length) {
        liveScoreboard.className = 'scoreboard-block';
        liveScoreboard.innerHTML = '';
        return;
    }

    const equipoA = current ? current.equipo_a : 'A';
    const equipoB = current ? current.equipo_b : 'B';
    const masProb = (p && p.marcador_mas_probable) || lista[0];
    const top = lista.slice(0, 3);
    const maxProb = Math.max(...top.map(m => Number(m.prob) || 0), 0.0001);

    const filas = top.map((m, i) => {
        const prob = Number(m.prob) || 0;
        const ancho = Math.max(4, Math.round((prob / maxProb) * 100));
        return `
      <div class="scoreboard-row">
        <span class="scoreboard-score">${m.marcador_a}-${m.marcador_b}</span>
        <span class="scoreboard-bar"><span style="width:${ancho}%"></span></span>
        <span class="scoreboard-pct">${(prob * 100).toFixed(1)}%</span>
      </div>`;
    }).join('');

    liveScoreboard.className = 'scoreboard-block has-data';
    liveScoreboard.innerHTML = `
    <div class="scoreboard-head">
      <div class="scoreboard-title">DISTRIBUCIÓN DE MARCADOR
        <span class="scoreboard-note">${escapeHtml(equipoA)}-${escapeHtml(equipoB)} · estimación, no resultado seguro</span>
      </div>
      <div class="scoreboard-top">
        <div>
          <div class="scoreboard-mostprob-label">MÁS PROBABLE (${((Number(masProb.prob) || 0) * 100).toFixed(1)}%)</div>
          <div class="scoreboard-mostprob">${masProb.marcador_a}-${masProb.marcador_b}</div>
        </div>
      </div>
    </div>
    <div class="scoreboard-list">${filas}</div>`;
}

// Categorías de economía en orden (eco -> full-buy).
const CAT_ECO = [
    ['eco', 'ECO'],
    ['semi_eco', 'SEMI-ECO'],
    ['semi_buy', 'SEMI-BUY'],
    ['full_buy', 'FULL-BUY'],
];

// Bloque de economía/ronda por mapa (micro-eventos). `economia` viene del backend
// (por equipo: win rate por categoría; 16 cruces `cat_a_vs_cat_b`; pistol). Si
// falta (fila de caché vieja), se oculta sin romper.
function renderEconomia(p) {
    if (!liveEconomia) return;
    const eco = p && p.economia;
    if (!eco || typeof eco !== 'object' || !eco.cruce) {
        liveEconomia.className = 'economia-block';
        liveEconomia.innerHTML = '';
        return;
    }
    const eqA = (current && current.equipo_a) || 'A';
    const eqB = (current && current.equipo_b) || 'B';
    const pctOr = v => (v == null || isNaN(Number(v))) ? '—' : `${Math.round(Number(v) * 100)}%`;
    const nDe = d => (d && d.n != null && d.n !== '' && !isNaN(Number(d.n))) ? Number(d.n) : null;
    // A1: el backend publica la semántica nueva (eco sin pistols R1/R13 y
    // n = rondas simuladas de la categoría). Si no viene, se usa el texto previo.
    const semantica = (typeof eco.semantica === 'string' && eco.semantica.trim())
        ? eco.semantica.trim()
        : 'estimación condicionada a la P del mapa · no resultado seguro';
    const pistolSemantica = (typeof eco.pistol_semantica === 'string' && eco.pistol_semantica.trim())
        ? eco.pistol_semantica.trim() : '';

    const catCols = datos => CAT_ECO.map(([key, label]) => {
        const d = (datos && datos[key]) || {};
        const pv = Number(d.p_gana_ronda);
        const n = nDe(d);
        const ancho = isNaN(pv) ? 0 : Math.round(pv * 100);
        return `<div class="eco-cat">
            <span class="eco-cat-label">${label}</span>
            <span class="eco-cat-bar"><span style="width:${ancho}%"></span></span>
            <span class="eco-cat-val">${pctOr(pv)}${n != null ? ` <span class="eco-n">n=${n}</span>` : ''}</span>
        </div>`;
    }).join('');

    const destacados = new Set(['semi_buy_vs_full_buy', 'eco_vs_full_buy']);
    const colsB = CAT_ECO.map(([, l]) => `<span class="eco-mcol">${l}</span>`).join('');
    const filasM = CAT_ECO.map(([ra, la]) => {
        const celdas = CAT_ECO.map(([cb]) => {
            const d = (eco.cruce || {})[`${ra}_vs_${cb}`] || {};
            const pv = Number(d.p_gana_a);
            const n = nDe(d);
            const txt = isNaN(pv) ? '—' : `${Math.round(pv * 100)}%`;
            const tono = isNaN(pv) ? '' : (pv >= 0.5 ? 'eco-hi' : 'eco-lo');
            const dest = destacados.has(`${ra}_vs_${cb}`) ? ' eco-dest' : '';
            const nTxt = (dest && n != null) ? `<span class="eco-cell-n">${n}</span>` : '';
            return `<span class="eco-cell ${tono}${dest}"${n != null ? ` title="n=${n} rondas simuladas"` : ''}>${txt}${nTxt}</span>`;
        }).join('');
        return `<div class="eco-mrow"><span class="eco-mrow-label">${la}</span>${celdas}</div>`;
    }).join('');

    const pistol = eco.pistol || {};
    const pvPis = Number(pistol.p_gana_a);
    const nPis = nDe(pistol);
    const anchoPis = isNaN(pvPis) ? 0 : Math.round(pvPis * 100);

    liveEconomia.className = 'economia-block has-data';
    liveEconomia.innerHTML = `
    <div class="eco-head">
      <div class="eco-title">ECONOMÍA / RONDAS
        <span class="eco-note"${pistolSemantica ? ` title="${escapeHtml(pistolSemantica)}"` : ''}>${escapeHtml(semantica)}</span>
      </div>
    </div>
    <div class="eco-cols">
      <div class="eco-team">
        <div class="eco-team-name eco-a">${escapeHtml(eqA)}</div>
        ${catCols(eco.equipo_a)}
      </div>
      <div class="eco-team">
        <div class="eco-team-name eco-b">${escapeHtml(eqB)}</div>
        ${catCols(eco.equipo_b)}
      </div>
    </div>
    <div class="eco-pistol"${pistolSemantica ? ` title="${escapeHtml(pistolSemantica)}"` : ''}>
      <span class="eco-cat-label">PISTOL</span>
      <span class="eco-cat-bar"><span style="width:${anchoPis}%"></span></span>
      <span class="eco-cat-val">${escapeHtml(eqA)} ${pctOr(pvPis)}${nPis != null ? ` <span class="eco-n">n=${nPis}</span>` : ''}</span>
    </div>
    <div class="eco-cruce-wrap">
      <div class="eco-cruce-title">CRUCES · prob. de que <b>${escapeHtml(eqA)}</b> gane la ronda
        <span class="eco-note">filas = ${escapeHtml(eqA)} · columnas = ${escapeHtml(eqB)}</span>
      </div>
      <div class="eco-matrix">
        <div class="eco-mrow eco-mrow-head"><span class="eco-mrow-label"></span>${colsB}</div>
        ${filasM}
      </div>
    </div>`;
}

// MOTOR del enfrentamiento actual: P(A) Glicko plana, solo conocida si ya se
// armó la serie (raíz de /serie). `null` si no aplica (esperando ARMAR SERIE,
// otra simulación, o serie servida de la caché DB sin `prob_motor_a`).
function motorActualA() {
    if (!ultimaSerie || !current) return null;
    const midOk = (ultimaSerie.match_id && current.match_id)
        ? Number(ultimaSerie.match_id) === Number(current.match_id)
        : true;
    const eqOk = String(ultimaSerie.equipo_a || '').trim().toLowerCase()
        === String(current.equipo_a || '').trim().toLowerCase()
        && String(ultimaSerie.equipo_b || '').trim().toLowerCase()
        === String(current.equipo_b || '').trim().toLowerCase();
    if (!midOk || !eqOk) return null;
    const v = ultimaSerie.prob_motor_a;
    if (v == null || v === '' || isNaN(Number(v))) return null;
    return Number(v);
}

function paintLiveDetail(p, modelVersion, vigente) {
    liveDetailTitle.textContent = `${(liveMap || '').toUpperCase()} · ${liveSide === 'attack' ? 'ATK' : 'DEF'}`;
    const am = p && p.analisis_mapa ? p.analisis_mapa : null;
    // ESC: predicción por (mapa, lado) de la fila elegida (ya es el lado que se
    // muestra). No se promedian lados: el otro lado es su complementario en la
    // MISMA fila. Historic: p_mapa_a del analisis_mapa (análisis, no predicción).
    const escA = (p && p.prob_victoria_a != null) ? Number(p.prob_victoria_a) : null;
    const escB = (p && p.prob_victoria_b != null)
        ? Number(p.prob_victoria_b)
        : ((escA != null && !isNaN(escA)) ? 1 - escA : null);
    const histP = (am && am.p_mapa_a != null) ? Number(am.p_mapa_a) : null;
    const wrA = (am && am.equipo_a) ? am.equipo_a : null;
    const wrB = (am && am.equipo_b) ? am.equipo_b : null;
    // conf = esc_peso (n_min/(n_min+10)). `escenario_mapa` puede venir `null`
    // en lecturas 100% cacheadas con el motor en frío: se usa la ESC y se
    // oculta la confianza sin romper.
    const peso = escPeso(p);
    const esc = (p && p.escenario_mapa && typeof p.escenario_mapa === 'object') ? p.escenario_mapa : null;
    // MOTOR plano del enfrentamiento (raíz de /serie): idéntico en todas las
    // tarjetas y en el banner; nunca se lee de `prob_victoria_a` (que es ESC).
    const motorA = motorActualA();
    const motorB = (motorA != null) ? 1 - motorA : null;
    renderScoreboard(p);
    renderEconomia(p);
    liveCards.innerHTML = `
    <div class="live-card">
      <div class="live-card-label" style="color:var(--accent)">ESC · ${escapeHtml(current.equipo_a)} GANA EL MAPA</div>
      <div class="live-card-val live-a">${pct(escA)}</div>
      ${motorA != null ? `<div class="live-card-sub" title="MOTOR Glicko plano (misma P en todos los mapas y lados); decide la serie.">MOTOR ${pct(motorA)}</div>` : ''}
    </div>
    <div class="live-card">
      <div class="live-card-label" style="color:var(--blue)">ESC · ${escapeHtml(current.equipo_b)} GANA EL MAPA</div>
      <div class="live-card-val live-b">${pct(escB)}</div>
      ${motorB != null ? `<div class="live-card-sub" title="MOTOR Glicko plano (misma P en todos los mapas y lados); decide la serie.">MOTOR ${pct(motorB)}</div>` : ''}
    </div>
    <div class="live-card">
      <div class="live-card-label">OVERTIME</div>
      <div class="live-card-val live-ot">${pct(p && p.prob_overtime)}</div>
    </div>
    ${(peso != null || esc) ? `<div class="live-card" title="conf = peso del ESC en este mapa/lado (esc_peso = n_min/(n_min+10)); el IC es de la capa de escenarios (Wilson 95%).">
      <div class="live-card-label">CONF. ESC (MAPA/LADO)</div>
      <div class="live-card-val">${peso != null ? pct(peso) : '—'}</div>
      <div class="live-card-sub">${esc
        ? `${pct(esc.p_lo)}–${pct(esc.p_hi)}${esc.n_a != null ? ` · n ${esc.n_a}/${esc.n_b != null ? esc.n_b : '—'}` : ''} · Δlogit ${fmtNum(esc.delta_logit, 3)}`
        : 'escenario no disponible en caché'}</div>
    </div>` : ''}
    <div class="live-card">
      <div class="live-card-label">MUESTRAS</div>
      <div class="live-card-val">${p.n_sim ? Number(p.n_sim).toLocaleString() : '—'}</div>
    </div>
    <div class="analisis-nota analisis-nota-hist">
      <b>ANÁLISIS HISTÓRICO</b> (no es la predicción) ·
      ${histP != null && !isNaN(histP) ? `p_mapa_a: <b>${pct(histP)}</b>` : 'p_mapa_a: —'} ·
      historial en <b>${(liveMap || '').toUpperCase()}</b>:
      ${escapeHtml(current.equipo_a)} ${wrA ? pct(wrA.winrate) + ' <span style="color:var(--dim)">(n=' + wrA.n + ')</span>' : '—'} ·
      ${escapeHtml(current.equipo_b)} ${wrB ? pct(wrB.winrate) + ' <span style="color:var(--dim)">(n=' + wrB.n + ')</span>' : '—'}
      <br><span style="color:var(--dim)">El <b>ESC</b> es la predicción por mapa/lado (puede variar); el <b>MOTOR</b> es plano y decide la serie; el histórico es descriptivo.</span>
    </div>`;
    const stale = vigente === false || current.vigente === false;
    liveStatus.className = 'live-status ' + (stale ? 'warn' : 'ok');
    liveStatus.textContent = stale
        ? '⚠ Predicciones desactualizadas; usa RE-PRECALCULAR.'
        : `✓ desde caché${peso != null ? ' · conf. ESC ' + pct(peso) : ''} · modelo ${modelVersion || current.modelo_version || '—'}`;
}

function setLiveSide(side) {
    liveSide = side;
    liveSideAtk.classList.toggle('qi-atk-active', side === 'attack');
    liveSideDef.classList.toggle('qi-def-active', side === 'defense');
    renderLiveMapPicker();
    renderLiveDetail();
}
liveSideAtk.addEventListener('click', () => setLiveSide('attack'));
liveSideDef.addEventListener('click', () => setLiveSide('defense'));

// ─── SERIE (armador + POST /api/serie, con botón ARMAR SERIE) ────────────────
document.querySelectorAll('.fmt-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.fmt-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        maxMapsSel = parseInt(btn.dataset.fmt);
        if (matchMaps.length > maxMapsSel) matchMaps = matchMaps.slice(0, maxMapsSel);
        syncSerieBuilder();
        marcarSeriePendiente();
    });
});

// Marca la serie como "hay que recalcular" (cambió algo). Con un veto
// incompleto (n ∉ {1,3,5}) avisa de cuántos mapas faltan y no hay POST.
function marcarSeriePendiente() {
    seriesBanner.innerHTML = '';
    const n = matchMaps.length;
    if (!n) {
        serieNote.textContent = 'Elige los mapas y pulsa ARMAR SERIE.';
        return;
    }
    if (!VETO_COMPLETO.includes(n)) {
        const falta = Math.max(0, maxMapsSel - n);
        serieNote.textContent = `Veto incompleto (${n}/${maxMapsSel}) · faltan ${falta || 1} mapa(s): la API exige el veto completo (Bo1=1, Bo3=3, Bo5=5).`;
        return;
    }
    serieNote.textContent = 'Cambió la serie · pulsa ARMAR SERIE.';
}

function syncSerieBuilder() {
    if (!serieSlots) return;
    serieSlots.innerHTML = '';
    if (!current) return;
    if (mapsLoading) {
        serieSlots.innerHTML = '<div style="padding:20px;text-align:center;font-size:10px;color:var(--dim);letter-spacing:2px">⏳ CARGANDO MAPAS...</div>';
        return;
    }
    if (mapsError) {
        serieSlots.innerHTML = `<div style="padding:20px;text-align:center;font-size:10px;color:var(--red);letter-spacing:2px">MAPAS NO DISPONIBLES: ${escapeHtml(mapsError)}</div>`;
        return;
    }

    const queueDiv = document.createElement('div');
    queueDiv.className = 'map-queue';
    const abbrevA = current.equipo_a;
    for (let i = 0; i < maxMapsSel; i++) {
        const cfg = matchMaps[i];
        const isDecider = (i === maxMapsSel - 1) && maxMapsSel >= 2;
        const item = document.createElement('div');

        if (!cfg) {
            item.className = `map-queue-item qi-empty${isDecider ? ' qi-decider-row' : ''}`;
            item.innerHTML = `
        <div class="qi-left">
          <span class="qi-num">0${i + 1}</span>
          <span class="qi-empty-txt">Selecciona un mapa arriba</span>
          ${isDecider ? '<span class="qi-decider-badge">DECIDER</span>' : ''}
        </div>`;
            queueDiv.appendChild(item);
            continue;
        }

        const row = liveBulk ? liveBulk[`${cfg.map_name}|${cfg.lado_inicial_a}`] : null;
        const am = row && row.analisis_mapa ? row.analisis_mapa : null;
        // ESC del slot: predicción del (mapa, lado) configurado (A/B misma fila,
        // sin promediar lados). El MOTOR plano vive en el banner de serie.
        const escA = row ? Number(row.prob_victoria_a) : null;
        const escB = (row && row.prob_victoria_b != null)
            ? Number(row.prob_victoria_b)
            : ((escA != null && !isNaN(escA)) ? 1 - escA : null);
        const histP = (am && am.p_mapa_a != null) ? Number(am.p_mapa_a) : null;
        const peso = escPeso(row);
        const pred = row
            ? `<div class="qi-pred">
                 <span class="qi-pred-tag" title="ESC: predicción por mapa/lado (la que se sirve; puede variar por mapa y lado)">ESC</span>
                 <span class="qi-pred-a">${pct(escA)}</span>
                 <span class="qi-pred-b">${pct(escB)}</span>
                 <span class="qi-pred-ot" title="P(overtime) del Monte Carlo de este mapa">OT ${pct(row.prob_overtime)}</span>
                 ${peso != null ? `<span class="qi-pred-esc" title="conf: peso/confianza del ESC en este mapa/lado (más alto = más historial)">conf ${pct(peso)}</span>` : ''}
                 ${histP != null && !isNaN(histP) ? `<span class="qi-pred-hist" title="Análisis histórico del mapa (no es la predicción)">hist ${pct(histP)}</span>` : ''}
               </div>`
            : `<span class="qi-pred-none">sin caché</span>`;

        item.className = `map-queue-item${isDecider ? ' qi-decider-row' : ''}${cfg.map_name === liveMap ? ' qi-selected' : ''}`;
        item.innerHTML = `
        <div class="qi-left" data-idx="${i}" title="Ver análisis de este mapa">
          <span class="qi-num">0${i + 1}</span>
          <img class="qi-map-img" src="../multimedia/maps/${cfg.map_name.toUpperCase()}.avif" onerror="this.style.display='none'">
          <span class="qi-mapname">${cfg.map_name.toUpperCase()}</span>
          ${isDecider ? '<span class="qi-decider-badge">DECIDER</span>' : ''}
          ${pred}
        </div>
        <div class="qi-side-group">
          <span class="qi-side-label">${abbrevA} empieza:</span>
          <button class="qi-side-btn${cfg.lado_inicial_a === 'attack' ? ' qi-atk-active' : ''}" data-idx="${i}" data-side="atk">⚔ ATK</button>
          <button class="qi-side-btn${cfg.lado_inicial_a === 'defense' ? ' qi-def-active' : ''}" data-idx="${i}" data-side="def">🛡 DEF</button>
        </div>
        <button class="qi-remove" data-idx="${i}" title="Quitar">✕</button>`;
        queueDiv.appendChild(item);
    }
    serieSlots.appendChild(queueDiv);

    serieSlots.querySelectorAll('.qi-left[data-idx]').forEach(el => {
        el.addEventListener('click', e => {
            const cfg = matchMaps[parseInt(e.currentTarget.dataset.idx)];
            if (!cfg) return;
            liveMap = cfg.map_name;
            liveSide = cfg.lado_inicial_a;
            liveSideAtk.classList.toggle('qi-atk-active', liveSide === 'attack');
            liveSideDef.classList.toggle('qi-def-active', liveSide === 'defense');
            serieSlots.querySelectorAll('.map-queue-item').forEach(it => it.classList.remove('qi-selected'));
            e.currentTarget.parentElement.classList.add('qi-selected');
            renderLiveMapPicker();
            renderLiveDetail();
        });
    });
    serieSlots.querySelectorAll('.qi-side-btn').forEach(btn => {
        btn.addEventListener('click', e => {
            const idx = parseInt(e.currentTarget.dataset.idx);
            matchMaps[idx].lado_inicial_a = (e.currentTarget.dataset.side === 'atk') ? 'attack' : 'defense';
            if (matchMaps[idx].map_name === liveMap) liveSide = matchMaps[idx].lado_inicial_a;
            syncSerieBuilder();
            marcarSeriePendiente();
        });
    });
    serieSlots.querySelectorAll('.qi-remove').forEach(btn => {
        btn.addEventListener('click', e => {
            matchMaps.splice(parseInt(e.currentTarget.dataset.idx), 1);
            syncSerieBuilder();
            marcarSeriePendiente();
        });
    });
    updateSerieState();
}

// Botón ARMAR SERIE (recalcula la serie con los mapas elegidos).
const btnArmarSerie = document.getElementById('btnArmarSerie');
if (btnArmarSerie) btnArmarSerie.addEventListener('click', () => updateSerie());

function updateSerieState() {
    const n = matchMaps.length;
    const valido = VETO_COMPLETO.includes(n);
    const falta = valido ? 0 : Math.max(0, maxMapsSel - n);
    mbFormat.textContent = valido
        ? `${inferFormat(n)} · ${n}/${maxMapsSel}`
        : `${n}/${maxMapsSel}${falta ? ` · faltan ${falta}` : ' · veto incompleto'}`;
    // Deshabilitado mientras el veto no sea 1/3/5 (con 0 mapas se mantiene
    // activo para mostrar la ayuda al pulsar).
    if (btnArmarSerie) btnArmarSerie.disabled = !valido && n > 0;
}

async function updateSerie() {
    if (!current || !matchMaps.length) {
        seriesBanner.innerHTML = '';
        serieNote.textContent = current ? 'Toca los mapas para armar la serie.' : '';
        return;
    }
    const n = matchMaps.length;
    if (!VETO_COMPLETO.includes(n)) {
        seriesBanner.innerHTML = '';
        const falta = Math.max(0, maxMapsSel - n);
        serieNote.textContent = `Faltan ${falta || 1} mapa(s): arma el veto completo (1 para Bo1, 3 para Bo3 o 5 para Bo5); recibí ${n}.`;
        return;
    }
    serieNote.textContent = 'Calculando serie...';
    const body = {
        match_id: current.match_id || 0,
        equipo_a: current.equipo_a,
        equipo_b: current.equipo_b,
        mapas: matchMaps.map(m => ({ map_name: m.map_name, lado_inicial_a: m.lado_inicial_a })),
    };
    try {
        const res = await proxyFetch('/serie', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const data = await res.json();
        if (!data.ok) {
            seriesBanner.innerHTML = '';
            serieNote.textContent = `Error: ${data.error || 'no se pudo calcular la serie'}`;
            return;
        }
        ultimaSerie = data;
        renderSerieBanner(data);
        // El MOTOR (raíz de /serie) se refleja también en las tarjetas del mapa.
        renderLiveDetail();
    } catch (e) {
        seriesBanner.innerHTML = '';
        serieNote.textContent = `Servicio de predicción no disponible: ${e.message}`;
    }
}

function renderSerieBanner(data) {
    const pa = data.prob_serie_a || 0;
    const pb = data.prob_serie_b || 0;
    const favA = pa * 100 >= 55 ? 'winner-side' : '';
    const favB = pb * 100 >= 55 ? 'winner-side-b' : '';
    const mid = current.match_id ? `PARTIDO #${current.match_id}` : 'sin id';

    const mapRows = (data.mapas || []).map(m => {
        const peso = escPeso(m);
        const mp = m.marcador_mas_probable || (Array.isArray(m.marcadores) && m.marcadores.length ? m.marcadores[0] : null);
        const marcadorTxt = mp ? `<b>${mp.marcador_a}-${mp.marcador_b}</b> (${((Number(mp.prob) || 0) * 100).toFixed(1)}%)` : '';
        // conf = esc_peso del ESC de ese (mapa, lado); IC de la capa de
        // escenarios (Wilson 95%) si el backend la trae.
        const esc = (m.escenario_mapa && typeof m.escenario_mapa === 'object') ? m.escenario_mapa : null;
        const escTxt = peso != null
            ? `<span class="smr-esc" title="conf: peso/confianza del ESC en este mapa/lado (esc_peso = n_min/(n_min+10))">conf ${pct(peso)}</span>` : '';
        const icTxt = esc ? `<span class="smr-conf" title="IC95% (Wilson) de la capa de escenarios">IC ${pct(esc.p_lo)}–${pct(esc.p_hi)}</span>` : '';
        return `
    <div class="serie-map-row">
      <span class="smr-name">${m.map_name.toUpperCase()}</span>
      <span class="smr-side">${m.lado_inicial_a === 'attack' ? 'ATK' : 'DEF'}</span>
      <span class="smr-a">${pct(m.prob_victoria_a)}</span>
      <span class="smr-b">${pct(m.prob_victoria_b)}</span>
      <span class="smr-ot">OT ${pct(m.prob_overtime)}</span>
      ${escTxt || '<span class="smr-esc"></span>'}
      <span class="smr-marcador">${marcadorTxt}</span>
      ${icTxt || '<span class="smr-conf"></span>'}
      <span class="smr-fuente">${m.fuente || 'cache'}</span>
    </div>`;
    }).join('');

    // Distribución del marcador de la serie (2-0/2-1/1-2/0-2; o 3-x en Bo5).
    const dist = (data.resultados_serie && typeof data.resultados_serie === 'object')
        ? Object.entries(data.resultados_serie).sort((a, b) => b[1] - a[1]) : [];
    const maxDist = dist.length ? Math.max(...dist.map(d => Number(d[1]) || 0), 0.0001) : 1;
    const distRows = dist.map(([score, prob], i) => {
        const pv = Number(prob) || 0;
        const ancho = Math.max(4, Math.round((pv / maxDist) * 100));
        const [x, y] = score.split('-').map(Number);
        const colorA = x > y ? 'sd-a' : 'sd-b';
        return `<div class="sd-row${i === 0 ? ' sd-top' : ''}">
            <span class="sd-score ${colorA}">${score}</span>
            <span class="sd-bar"><span class="${colorA}" style="width:${ancho}%"></span></span>
            <span class="sd-pct">${(pv * 100).toFixed(1)}%</span>
        </div>`;
    }).join('');
    const distBlock = dist.length
        ? `<div class="serie-dist">
             <div class="sd-title">DISTRIBUCIÓN DE LA SERIE
               <span class="eco-note">marcador final más probable primero</span></div>
             ${distRows}
           </div>`
        : '';

    // Caminos de la serie: secuencia mapa a mapa (V = gana A, D = gana B).
    const caminos = Array.isArray(data.caminos_serie) ? data.caminos_serie : [];
    const nombresMapas = matchMaps.map(m => m.map_name);
    const caminosRows = caminos.map(c => {
        const pasos = (c.camino || []).map((v, i) => {
            const ganaA = v === 'V';
            const lbl = (nombresMapas[i] || `M${i + 1}`).toUpperCase();
            return `<span class="cam-step ${ganaA ? 'cam-v' : 'cam-d'}">${lbl} ${ganaA ? '✓' : '✗'}</span>`;
        }).join('<span class="cam-arrow">›</span>');
        return `<div class="cam-row">
            <span class="cam-mark ${c.equipo === 'a' ? 'cam-a' : 'cam-b'}">${c.marcador}</span>
            <span class="cam-seq">${pasos}</span>
            <span class="cam-prob">${(Number(c.prob) * 100).toFixed(1)}%</span>
        </div>`;
    }).join('');
    const caminosBlock = caminos.length
        ? `<div class="serie-caminos">
             <div class="sd-title">CAMINOS DE LA SERIE
               <span class="eco-note">✓ gana ${escapeHtml(current.equipo_a)} · ✗ gana ${escapeHtml(current.equipo_b)}</span></div>
             ${caminosRows}
           </div>`
        : '';

    // MOTOR plano del enfrentamiento (raíz de la respuesta de /serie). No se
    // persiste por mapa: si la serie vino de la caché DB puede faltar y se
    // oculta sin romper.
    const numOrNull = v => (v == null || v === '' || isNaN(Number(v))) ? null : Number(v);
    const motorA = numOrNull(data.prob_motor_a);
    const motorB = (numOrNull(data.prob_motor_b) != null)
        ? numOrNull(data.prob_motor_b)
        : ((motorA != null) ? 1 - motorA : null);

    seriesBanner.innerHTML = `
    <div class="sb-team ${favA}">
      <div class="sb-name">EQUIPO A</div>
      <div class="sb-abbrev">${escapeHtml(current.equipo_a)}</div>
      <div class="sb-pct">${pct(pa)}</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
    <div class="sb-center">
      <div class="sb-format">${(data.formato || '').toUpperCase()}</div>
      <div class="sb-sims">${(data.n_sim || current.n_sim || 0).toLocaleString()}<br>SIMULACIONES</div>
      ${motorA != null ? `<div style="font-size:10px;color:var(--txt-2);letter-spacing:1px" title="P del MOTOR Glicko del enfrentamiento: una sola, plana entre mapas y lados; decide la serie (motor + temperatura).">MOTOR ${pct(motorA)} · ${pct(motorB)}</div>` : ''}
      <div style="font-size:10px;color:var(--dim);letter-spacing:1px;margin-top:4px">GANAR ${data.mapas_para_ganar}</div>
      ${data.confianza_serie ? `<div class="sb-conf ${String(data.confianza_serie).toLowerCase()}"><span class="conf-dot"></span>CONFIANZA ${escapeHtml(String(data.confianza_serie).toUpperCase())}</div>` : ''}
      <div style="font-size:9px;color:var(--dim);letter-spacing:1px;margin-top:4px">${mid}</div>
    </div>
    <div class="sb-team ${favB}" style="text-align:right;align-items:flex-end">
      <div class="sb-name">EQUIPO B</div>
      <div class="sb-abbrev">${escapeHtml(current.equipo_b)}</div>
      <div class="sb-pct">${pct(pb)}</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
    ${distBlock}
    ${caminosBlock}
    ${mapRows ? `<div class="serie-maps"><div class="sd-title">MAPAS · ESC (PREDICCIÓN POR MAPA/LADO) <span class="eco-note">ESC = predicción servida por mapa/lado (puede variar por mapa y lado) · motor plano decide la serie · conf = esc_peso</span></div>${mapRows}</div>` : ''}`;

    const fuentes = [...new Set((data.mapas || []).map(m => m.fuente).filter(Boolean))];
    serieNote.innerHTML = `<strong>Serie cache-aware</strong> (reutiliza la caché y calcula/persiste lo que falte` +
        `${fuentes.length ? `; fuente por mapa: <strong>${escapeHtml(fuentes.join('/'))}</strong>` : ''}). ` +
        `Formato <strong>${(data.formato || '').toUpperCase()}</strong> — necesario ganar <strong>${data.mapas_para_ganar}</strong> mapa(s).`;
}

// ─── INFORME PARA EL LLM (prompt + todos los datos + notas) ─────────────────
const PROMPT_ANALISTA = `Eres un analista de Valorant. Recibes el JSON de abajo con las predicciones y el análisis de un enfrentamiento.

NOMENCLATURA (NO confundir):
- esc_p_a / esc_p_b = P del ESC para ESE MAPA y ESE LADO: es la predicción que se sirve y puede variar por mapa y lado. NO es la P de la serie. Compara el "analítico" (p_mapa_a) SIEMPRE contra esc_p_a, nunca contra prob_serie_a/prob_serie_b.
- esc_peso (0–1) = peso/confianza del ESC en ese mapa/lado (n_min/(n_min+10)); más alto = más historial lo respalda. null = capa apagada (no es error).
- motor_p_a / motor_p_b = P del MOTOR Glicko del enfrentamiento: UNA sola, plana entre mapas y lados; decide la serie. Solo está en la raíz (resumen_serie).
- escenario_mapa = capa de escenarios por mapa/lado (delta_logit, n_a/n_b, IC p_lo–p_hi); su p_mapa coincide con esc_p_a cuando está presente. Puede venir null (lectura cacheada con el motor en frío): en ese caso usa esc_p_a + esc_peso.
- prob_serie_a / prob_serie_b = P de GANAR LA SERIE (motor + temperatura); úsalas SOLO para la serie.
- Nunca menciones un "n" que no venga explícito en el bloque. Si el bloque no trae n (p. ej. total_rondas), NO lo menciones (ni "n alto"): di "sin n reportado" o no lo cites.
- ot (por mapa) = P(overtime) del Monte Carlo de ESE mapa. total_rondas.mas_24_5 = P(rondas totales > 24.5) deducida de la distribución de marcadores: NO es el campo "ot"; no los mezcles.
- Certeza (de la RECOMENDACIÓN, no del resultado; mismos umbrales que la banda del motor sobre el favorito): **baja** si el pick < 55% (cerca de coinflip); **media** si 55%–<62%; **alta** si >= 62%. EXCEPCIÓN: "marcador exacto" y "pistol" son mercados dispersos → NUNCA "alta" (máximo "media"), aunque la probabilidad sea alta. "certeza alta" = el estimado es estable, NO significa que el resultado vaya a pasar.

FORMATO DE SALIDA (respetar el orden):
1) RESUMEN (directo, sin relleno). Una línea por mercado, con el pick y su %:
   - Ganador de serie: <equipo> — <X%>
   - Total de mapas (línea 2.5 en Bo3 / 3.5 en Bo5): Más|Menos — <X%>   (aclara: Menos = 2-0/0-2; Más = 2-1/1-2)
   - Marcador exacto: <p. ej. 2-1 (PRX)> — <X%>
   - Pistol por mapa: <MAPA (LADO)>: <equipo> — <X%>
   - Total de rondas por mapa: <MAPA (LADO)>: Más|Menos de 21.5 — <X%>
   Al final de cada línea, la certeza entre paréntesis: (alta|media|baja).
2) ANÁLISIS BREVE: 1 línea por mapa (analítico vs esc_p_a) y 1 línea de serie. Máximo 120 palabras.
   - El mapa MÁS propenso a upset es el de p_mapa_a MÁS CERCANA a 0.50 (el más parejo). Si p_mapa_a se aleja del modelo HACIA el favorito, ese mapa es MENOS propenso a upset (NO es "valor" para el no-favorito).
3) Si un mercado no es estimable con los datos, escríbelo: "no estimable: <motivo>".

REGLAS:
- Razona SOLO con los números del JSON. NO inventes cambios de roster, parches ni contexto externo. Si NOTAS trae contexto, úsalo.
- Con n<10 no afirmes nada fuerte.
- Habla en probabilidades; nunca prometas resultados.`;

function _slug(t) {
    return String(t || '').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
}

function _descargarArchivo(nombre, texto) {
    const blob = new Blob([texto], { type: 'text/markdown;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = nombre;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// Mercados: total de rondas por mapa desde la distribución de marcadores.
function _totalRondas(marcadores) {
    let exp = 0, mas215 = 0, mas245 = 0, n = 0;
    for (const m of (marcadores || [])) {
        const t = (Number(m.marcador_a) || 0) + (Number(m.marcador_b) || 0);
        const p = Number(m.prob) || 0;
        exp += t * p; n++;
        if (t > 21.5) mas215 += p;
        if (t > 24.5) mas245 += p;
    }
    if (!n) return null;
    return {
        esperado: Number(exp.toFixed(2)),
        mas_21_5: Number(mas215.toFixed(4)),
        menos_21_5: Number((1 - mas215).toFixed(4)),
        mas_24_5: Number(mas245.toFixed(4)),
    };
}

function _totalMapas(serie) {
    const dist = (serie && serie.resultados_serie) || {};
    const fmt = String((serie && serie.formato) || '').toLowerCase();
    const linea = fmt === 'bo5' ? 3.5 : (fmt === 'bo1' ? 1.5 : 2.5);
    let menos = 0, mas = 0;
    for (const [k, v] of Object.entries(dist)) {
        const total = k.split('-').map(Number).reduce((a, b) => a + b, 0);
        if (total <= Math.floor(linea)) menos += v; else mas += v;
    }
    return { linea, menos: Number(menos.toFixed(4)), mas: Number(mas.toFixed(4)) };
}

function _mercadosTexto(payload) {
    const m = payload.mercados || {};
    if (!m.ganador_serie) {
        return '(Sin serie armada: pulsa ARMAR SERIE antes de descargar para incluir ganador, total de mapas y marcador exacto.)';
    }
    const f = v => (v == null ? '—' : `${(Number(v) * 100).toFixed(1)}%`);
    const g = m.ganador_serie, tm = m.total_mapas || {};
    const out = [];
    out.push(`Ganador de serie: ${payload.equipo_a} ${f(g.equipo_a)} · ${payload.equipo_b} ${f(g.equipo_b)}`);
    if (m.motor) {
        out.push(`MOTOR (plano, decide la serie): ${payload.equipo_a} ${f(m.motor.p_a)} · ${payload.equipo_b} ${f(m.motor.p_b)}`);
    }
    out.push(`Total de mapas (línea ${tm.linea}): Menos ${f(tm.menos)} · Más ${f(tm.mas)}`);
    if (m.marcador_exacto_serie) {
        out.push('Marcador exacto de serie: ' + Object.entries(m.marcador_exacto_serie)
            .map(([k, v]) => `${k}: ${f(v)}`).join(' · '));
    }
    out.push('Total de rondas y pistol por mapa:');
    for (const mp of (m.por_mapa || [])) {
        const tr = mp.total_rondas || {}, pis = mp.pistol || {}, esc = mp.escenario_mapa || {};
        const ic = (esc.p_lo != null && esc.p_hi != null)
            ? ` · IC ${f(esc.p_lo)}–${f(esc.p_hi)}` : '';
        const escTxt = mp.esc_p_a != null
            ? ` · ESC ${f(mp.esc_p_a)}${mp.esc_peso != null ? ` (conf ${f(mp.esc_peso)})` : ''}`
            : '';
        out.push(`  - ${mp.map} (${mp.lado}): rondas≈${tr.esperado != null ? tr.esperado : '—'}`
            + ` · >21.5 ${f(tr.mas_21_5)} · rondas>24.5 ${f(tr.mas_24_5)}`
            + ` · pistol ${payload.equipo_a} ${f(pis.p_a)} (n=${pis.n != null ? pis.n : '—'})`
            + escTxt
            + ic);
    }
    return out.join('\n');
}

function descargarAnalisis() {
    if (!current || !liveBulk) {
        window.alert('Selecciona un enfrentamiento preparado primero.');
        return;
    }
    const filas = Object.values(liveBulk).filter(Boolean);
    const mapas = filas.map(p => ({
        map: p.map_name,
        lado: p.lado_inicial_a,
        esc_p_a: p.prob_victoria_a,
        esc_p_b: p.prob_victoria_b,
        esc_peso: escPeso(p),
        ot: p.prob_overtime,
        escenario_mapa: p.escenario_mapa || null,
        analisis_mapa: p.analisis_mapa || null,
        total_rondas: _totalRondas(p.marcadores),
        pistol: (p.economia && p.economia.pistol)
            ? { p_a: p.economia.pistol.p_gana_a, n: p.economia.pistol.n } : null,
        marcador_top5: (p.marcadores || []).slice(0, 5),
        economia: p.economia || null,
        n_sim: p.n_sim,
    })).sort((a, b) => String(a.map).localeCompare(String(b.map))
        || String(a.lado).localeCompare(String(b.lado)));

    const numOrNull = v => (v == null || v === '' || isNaN(Number(v))) ? null : Number(v);
    const motorA = ultimaSerie ? numOrNull(ultimaSerie.prob_motor_a) : null;
    const motorB = ultimaSerie ? numOrNull(ultimaSerie.prob_motor_b) : null;

    const mercados = {
        ganador_serie: ultimaSerie
            ? { equipo_a: ultimaSerie.prob_serie_a, equipo_b: ultimaSerie.prob_serie_b } : null,
        motor: (motorA != null) ? { p_a: motorA, p_b: (motorB != null ? motorB : 1 - motorA) } : null,
        total_mapas: ultimaSerie ? _totalMapas(ultimaSerie) : null,
        marcador_exacto_serie: ultimaSerie ? ultimaSerie.resultados_serie : null,
        por_mapa: mapas.map(m => ({
            map: m.map, lado: m.lado, esc_p_a: m.esc_p_a, esc_p_b: m.esc_p_b,
            esc_peso: m.esc_peso,
            total_rondas: m.total_rondas, pistol: m.pistol,
            marcador_top5: m.marcador_top5,
            escenario_mapa: m.escenario_mapa,
        })),
    };

    const payload = {
        equipo_a: current.equipo_a,
        equipo_b: current.equipo_b,
        match_id: current.match_id || 0,
        modelo_version: current.modelo_version || serviceModelVersion || null,
        resumen_serie: ultimaSerie ? {
            formato: ultimaSerie.formato,
            mapa_seleccionados: (ultimaSerie.mapas || []).map(m => `${m.map_name}|${m.lado_inicial_a}`),
            motor_p_a: motorA,
            motor_p_b: (motorB != null ? motorB : ((motorA != null) ? 1 - motorA : null)),
            prob_serie_a: ultimaSerie.prob_serie_a,
            prob_serie_b: ultimaSerie.prob_serie_b,
            confianza_serie: ultimaSerie.confianza_serie,
            resultados_serie: ultimaSerie.resultados_serie,
            caminos_serie: ultimaSerie.caminos_serie,
        } : null,
        mercados,
        mapas,
    };

    const notas = `Equipo A (${current.equipo_a}):
- (roster, cambios recientes, forma, parche, motivación...)

Equipo B (${current.equipo_b}):
- (roster, cambios recientes, forma, parche, motivación...)

Contexto del torneo / del partido:
- (fase, formato, descanso, map pool, etc.)`;

    const informe = `# ALETHEIA · Informe para análisis con LLM

Partido: ${current.equipo_a} vs ${current.equipo_b} (#${current.match_id || 0}) · modelo ${payload.modelo_version || '—'}
Generado: ${new Date().toISOString()}

## 1) PROMPT (general — úsalo tal cual)

${PROMPT_ANALISTA}

## 2) MERCADOS (estimaciones precalculadas)

${_mercadosTexto(payload)}

## 3) DATOS (JSON)

\`\`\`json
${JSON.stringify(payload, null, 2)}
\`\`\`

## 4) NOTAS DE CONTEXTO (rellenar si aplica)

${notas}
`;

    _descargarArchivo(
        `aletheia_${_slug(current.equipo_a)}_vs_${_slug(current.equipo_b)}_${current.match_id || 0}.md`,
        informe
    );
    serieNote.textContent = '✓ Informe descargado (prompt + mercados + datos + notas).';
}

const btnDescargarAnalisis = document.getElementById('btnDescargarAnalisis');
if (btnDescargarAnalisis) btnDescargarAnalisis.addEventListener('click', descargarAnalisis);

// ─── COMPARACIÓN (predicho vs. real) ─────────────────────────────────────────
async function fetchComparacion() {
    if (!current) return;
    cmpStatus.className = 'live-status';
    cmpStatus.textContent = 'Consultando comparación...';
    cmpSummary.innerHTML = '';
    cmpTableWrap.innerHTML = '';
    if (cmpSerie) cmpSerie.innerHTML = '';

    const params = new URLSearchParams();
    if (current.match_id > 0) {
        params.set('match_id', current.match_id);
    } else {
        params.set('equipo_a', current.equipo_a);
        params.set('equipo_b', current.equipo_b);
    }
    params.set('limite', '100');

    try {
        const res = await proxyFetch(`/comparacion?${params.toString()}`);
        const data = await res.json();
        if (!data.ok || !data.resumen || !data.resumen.n) {
            cmpStatus.className = 'live-status warn';
            cmpStatus.textContent = 'Sin resultado real todavía (el partido no está en la DB).';
            return;
        }
        renderComparison(data);
        fetchSerieReal(data.detalle);
    } catch (e) {
        cmpStatus.className = 'live-status err';
        cmpStatus.textContent = `Servicio de predicción no disponible: ${e.message}`;
    }
}

function renderComparison(data) {
    const r = data.resumen || {};
    const cards = [
        { label: 'N', val: r.n },
        { label: 'ACCURACY', val: fmtRatio(r.accuracy), cls: probAccClass(r.accuracy) },
        { label: 'BRIER', val: fmtNum(r.brier) },
        { label: 'LOG-LOSS', val: fmtNum(r.log_loss) },
        { label: 'FAVORITOS OK', val: r.favoritos_ok },
        { label: 'UPSETS', val: r.upsets },
        { label: 'INCIERTOS', val: r.inciertos },
    ];
    cmpSummary.innerHTML = cards.map(c => `
    <div class="cmp-card">
      <div class="cmp-card-label">${c.label}</div>
      <div class="cmp-card-val ${c.cls || ''}">${c.val == null ? '—' : c.val}</div>
    </div>`).join('');

    const tipoClass = t => t === 'favorito_gano' ? 'cmp-fav' : t === 'upset' ? 'cmp-upset' : 'cmp-unc';
    const tipoLabel = t => t === 'favorito_gano' ? 'favorito ganó' : t === 'upset' ? 'UPSET' : 'incierto';

    const rows = (data.detalle || []).map(d => {
        const ganador = d.gano_a_real ? (d.equipo_a || 'A') : (d.equipo_b || 'B');
        const pReal = d.gano_a_real ? d.prob_victoria_a : d.prob_victoria_b;
        return `
      <tr class="cmp-row ${tipoClass(d.tipo)}">
        <td>${escapeHtml(d.map_name)}</td>
        <td>${d.lado_inicial_a === 'attack' ? 'ATK' : 'DEF'}</td>
        <td class="cmp-a">${pct(d.prob_victoria_a)}</td>
        <td class="cmp-b">${pct(d.prob_victoria_b)}</td>
        <td>${pct(d.prob_overtime)}</td>
        <td class="cmp-score"><b class="${d.gano_a_real ? 'gana' : ''}">${d.score_a}</b>-<b class="${d.gano_a_real ? '' : 'gana'}">${d.score_b}</b></td>
        <td>${escapeHtml(ganador)}</td>
        <td class="cmp-preal ${probBandClass(pReal)}" title="Probabilidad que el ESC (mapa/lado) dio al ganador real de este mapa">${pct(pReal)}</td>
        <td>${tipoLabel(d.tipo)}</td>
        <td>${d.resultado === 'acierto' ? '✓' : '✕'} ${escapeHtml(d.resultado)}</td>
      </tr>`;
    }).join('');

    cmpTableWrap.innerHTML = `
    <table class="cmp-table">
      <thead>
        <tr>
          <th>MAPA</th><th>LADO</th><th>ESC A</th><th>ESC B</th><th>OT</th>
          <th>MARCADOR</th><th>GANÓ (REAL)</th><th>P(REAL)</th><th>TIPO</th><th>RESULTADO</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="cmp-legend">
      <span class="cmp-legend-item"><span class="legend-dot p-alta"></span>P(REAL) ≥62%: acierto esperado</span>
      <span class="cmp-legend-item"><span class="legend-dot p-media"></span>≥55%: ajustado</span>
      <span class="cmp-legend-item"><span class="legend-dot p-baja"></span>&lt;55%: sorpresa/upset</span>
      <span class="cmp-legend-item"><span class="legend-line cmp-fav"></span>favorito ganó</span>
      <span class="cmp-legend-item"><span class="legend-line cmp-upset"></span>upset</span>
      <span class="cmp-legend-item"><span class="legend-line cmp-unc"></span>incierto</span>
    </div>`;

    cmpStatus.className = 'live-status ok';
    cmpStatus.textContent = `✓ comparación con modelo ${data.modelo_version || '—'}`;
}

// Banner de la serie: probabilidad predicha (POST /serie, desde la caché) vs
// resultado real (marcador de mapas del detalle de comparación).
async function fetchSerieReal(detalle) {
    if (!cmpSerie) return;
    cmpSerie.innerHTML = '';
    const filas = (detalle || []).slice();
    if (!current || !filas.length) return;

    // Orden real de mapas (si el resumen de la lista lo trae) para armar la serie.
    const info = simsInfo[current.match_id] || null;
    if (info && Array.isArray(info.mapas) && info.mapas.length) {
        const orden = new Map(info.mapas.map((m, i) => [m.map_name, i]));
        filas.sort((a, b) => (orden.has(a.map_name) ? orden.get(a.map_name) : 99) -
            (orden.has(b.map_name) ? orden.get(b.map_name) : 99));
    }
    let winA = 0, winB = 0;
    filas.forEach(d => { if (d.gano_a_real) winA++; else winB++; });
    if (winA === winB) return;
    const ganaA = winA > winB;
    const nombreA = (filas[0] && filas[0].equipo_a) || current.equipo_a;
    const nombreB = (filas[0] && filas[0].equipo_b) || current.equipo_b;
    const marcadorReal = `<b class="${ganaA ? 'gana' : ''}">${winA}</b>-<b class="${ganaA ? '' : 'gana'}">${winB}</b>`;
    // P de serie: mejor con el pool del veto; con listas de 2/4 la API responde
    // 400, así que si no hay pool completo (1/3/5) no se llama al servicio.
    let mapasSerie = mapasSerieDe(info);
    if (!mapasSerie.length) {
        const jugados = filas.map(d => ({ map_name: d.map_name, lado_inicial_a: d.lado_inicial_a }));
        mapasSerie = VETO_COMPLETO.includes(jugados.length) ? jugados : [];
    }

    const render = extra => {
        cmpSerie.innerHTML = `
        <div class="cmp-serie">
          <span class="cmp-serie-title">SERIE · PREDICHO VS REAL</span>
          <span class="cmp-serie-item">REAL: <b>${escapeHtml(ganaA ? nombreA : nombreB)}</b> ${marcadorReal}</span>
          ${extra}
        </div>`;
    };
    if (!mapasSerie.length) {
        render('<span class="cmp-serie-item cmp-serie-err">sin pool de veto completo (1/3/5 mapas); no se calcula la P de serie</span>');
        return;
    }
    render('<span class="cmp-serie-item cmp-serie-cargando">calculando P(serie) del motor…</span>');
    try {
        const res = await proxyFetch('/serie', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                match_id: current.match_id || 0,
                equipo_a: current.equipo_a,
                equipo_b: current.equipo_b,
                mapas: mapasSerie,
            }),
        });
        const s = await res.json();
        if (!s.ok) throw new Error(s.error || `HTTP ${res.status}`);
        const pReal = ganaA ? Number(s.prob_serie_a) : Number(s.prob_serie_b);
        const marca = pReal >= 0.5 ? '✓ favorito' : '✕ upset';
        // MOTOR = prob_motor_a/b (raíz, plano; puede faltar en caché DB).
        // SERIE = prob_serie_a/b (motor + temperatura): es la que decide el global.
        const mtrA = (s.prob_motor_a == null || isNaN(Number(s.prob_motor_a))) ? null : Number(s.prob_motor_a);
        const mtrB = (s.prob_motor_b == null || isNaN(Number(s.prob_motor_b)))
            ? ((mtrA != null) ? 1 - mtrA : null) : Number(s.prob_motor_b);
        render(`
          ${mtrA != null ? `<span class="cmp-serie-item" title="P del MOTOR Glicko (una sola, plana entre mapas y lados).">MOTOR: ${escapeHtml(nombreA)} ${pct(mtrA)} · ${escapeHtml(nombreB)} ${pct(mtrB)}</span>` : ''}
          <span class="cmp-serie-item" title="P de ganar la serie (motor + temperatura).">SERIE: ${escapeHtml(nombreA)} ${pct(s.prob_serie_a)} · ${escapeHtml(nombreB)} ${pct(s.prob_serie_b)}</span>
          <span class="cmp-serie-pill ${probBandClass(pReal)}" title="Probabilidad que el motor (serie) dio al ganador real de la serie">P(REAL) ${pct(pReal)} ${marca}</span>`);
    } catch (e) {
        render(`<span class="cmp-serie-item cmp-serie-err">sin P(serie) del motor (${escapeHtml(e.message || 'servicio no disponible')})</span>`);
    }
}

// ─── SCORECARD (micro-eventos predichos vs reales) ──────────────────────────
async function fetchScorecard() {
    if (!current || !scorecardWrap) return;
    scorecardWrap.innerHTML = '';
    const params = new URLSearchParams();
    if (current.match_id > 0) params.set('match_id', current.match_id);
    else { params.set('equipo_a', current.equipo_a); params.set('equipo_b', current.equipo_b); }
    // Defaults documentados del endpoint de micro-eventos.
    params.set('cruces', '1');
    params.set('tol_cruce', '6');
    try {
        const res = await proxyFetch(`/scorecard?${params.toString()}`);
        const data = await res.json();
        if (!data.ok) {
            scorecardWrap.innerHTML = `<div class="live-status err">No se pudo cargar el SCORECARD: ${escapeHtml(data.error || `HTTP ${res.status}`)}</div>`;
            return;
        }
        if (!data.resumen || !data.resumen.n_mapas) return;
        renderScorecard(data);
    } catch (e) {
        scorecardWrap.innerHTML = `<div class="live-status err">No se pudo cargar el SCORECARD: ${escapeHtml(e.message || 'servicio no disponible')}</div>`;
    }
}

function renderScorecard(data) {
    const r = data.resumen || {};
    const sinCruces = r.cruce_mae == null;
    const cards = [
        { label: 'N MAPAS', val: r.n_mapas },
        { label: 'MAP ACCURACY', val: fmtRatio(r.map_accuracy), cls: probAccClass(r.map_accuracy) },
        { label: 'MAP BRIER', val: fmtNum(r.map_brier) },
        { label: 'OT BRIER', val: fmtNum(r.ot_brier) },
        { label: 'MARCADOR TOP-1', val: fmtRatio(r.scoreline_top1_hit) },
        { label: 'ECO MAE', val: r.eco_mae == null ? '—' : `${fmtNum(r.eco_mae)} (n=${r.eco_n})` },
        { label: 'CRUCE MAE', val: sinCruces ? '— (sin cruces)' : `${fmtNum(r.cruce_mae)} (n=${r.cruce_n})` },
    ];
    const media = arr => (arr && arr.length ? arr.reduce((a, x) => a + x.erro, 0) / arr.length : null);
    const rows = (data.detalle || []).map(m => {
        const ecoErr = media(m.economia);
        const cruceErr = media(m.cruces);
        const otOk = (m.ot_pred >= 0.5 ? 1 : 0) === m.ot_real ? '✓' : '✕';
        return `<tr>
            <td>${escapeHtml(m.map_name)}</td>
            <td>${m.score_a}-${m.score_b}</td>
            <td>${pct(m.prob_victoria_a)}</td>
            <td>${m.gano_a ? 'A' : 'B'}</td>
            <td>${pct(m.ot_pred)} / ${m.ot_real ? 'sí' : 'no'} ${otOk}</td>
            <td>${pct(m.scoreline_prob)}</td>
            <td>${ecoErr == null ? '—' : ecoErr.toFixed(3)}</td>
            <td>${cruceErr == null ? '—' : cruceErr.toFixed(3)}</td>
        </tr>`;
    }).join('');
    scorecardWrap.innerHTML = `
    <div class="scorecard-title">SCORECARD · MICRO-EVENTOS
      <span class="eco-note">predicho vs real · MAE menor = mejor (0-1)${sinCruces ? ' · sin datos de cruces (cruces=1)' : ''}</span></div>
    <div class="cmp-summary scorecard-cards">${cards.map(c =>
        `<div class="cmp-card"><div class="cmp-card-label">${c.label}</div><div class="cmp-card-val ${c.cls || ''}">${c.val == null ? '—' : c.val}</div></div>`).join('')}</div>
    <div class="cmp-table-wrap"><table class="cmp-table">
        <thead><tr><th>MAPA</th><th>MARCADOR</th><th>ESC A</th><th>GANÓ</th><th>OT ≥50% (PRED/REAL)</th><th>P(MARCADOR REAL)</th><th>ECO MAE</th><th>CRUCE MAE</th></tr></thead>
        <tbody>${rows}</tbody>
    </table></div>`;
}

// ─── SCORECARD AGREGADO (todos los partidos) + DATASET ──────────────────────
async function fetchScorecardAgregado() {
    if (!scorecardAgregadoWrap) return;
    scorecardAgregadoWrap.innerHTML = '';
    if (cmpToolsStatus) { cmpToolsStatus.className = 'live-status warn'; cmpToolsStatus.textContent = 'Calculando agregado (puede tardar)...'; }
    try {
        const res = await proxyFetch('/scorecard_agregado?cruces=1&tol_cruce=6');
        const data = await res.json();
        if (!data.ok) {
            if (cmpToolsStatus) { cmpToolsStatus.className = 'live-status err'; cmpToolsStatus.textContent = `Error: ${data.error || `HTTP ${res.status}`}`; }
            return;
        }
        if (!data.resumen || !data.resumen.n_mapas) {
            if (cmpToolsStatus) { cmpToolsStatus.className = 'live-status warn'; cmpToolsStatus.textContent = 'Sin partidos jugados con predicción.'; }
            return;
        }
        renderScorecardAgregado(data);
        const r = data.resumen;
        const nPart = r.n_partidos != null ? r.n_partidos : r.n;
        if (cmpToolsStatus) {
            cmpToolsStatus.className = 'live-status ok';
            cmpToolsStatus.textContent = `✓ ${nPart != null ? nPart + ' partidos / ' : ''}${r.n_mapas} mapas`;
        }
    } catch (e) {
        if (cmpToolsStatus) { cmpToolsStatus.className = 'live-status err'; cmpToolsStatus.textContent = `Error: ${e.message}`; }
    }
}

function renderScorecardAgregado(data) {
    const r = data.resumen || {};
    const sinCruces = r.cruce_mae == null;
    const cards = [
        { label: 'PARTIDOS', val: r.n_partidos != null ? r.n_partidos : r.n },
        { label: 'MAPAS', val: r.n_mapas },
        { label: 'MAP ACCURACY', val: fmtRatio(r.map_accuracy), cls: probAccClass(r.map_accuracy) },
        { label: 'MAP BRIER', val: fmtNum(r.map_brier) },
        { label: 'OT BRIER', val: fmtNum(r.ot_brier) },
        { label: 'ECO MAE', val: r.eco_mae == null ? '—' : `${fmtNum(r.eco_mae)} (n=${r.eco_n})` },
        { label: 'CRUCE MAE', val: sinCruces ? '— (sin cruces)' : `${fmtNum(r.cruce_mae)} (n=${r.cruce_n})` },
    ];
    const tabla = (titulo, filas) => `
    <div class="scorecard-subtitle">${titulo}</div>
    <div class="cmp-table-wrap"><table class="cmp-table">
      <thead><tr><th>GRUPO</th><th>N</th><th>PRED MEDIA</th><th>REAL MEDIA</th><th>MAE</th></tr></thead>
      <tbody>${(filas || []).map(f => `<tr>
        <td>${escapeHtml(f.grupo)}</td><td>${f.n}</td>
        <td>${pct(f.pred_media)}</td><td>${pct(f.real_media)}</td>
        <td>${(Number(f.mae) * 100).toFixed(1)}%</td></tr>`).join('')}</tbody>
    </table></div>`;
    scorecardAgregadoWrap.innerHTML = `
    <div class="scorecard-title">SCORECARD AGREGADO
      <span class="eco-note">predicho vs real, sumando partidos · MAE menor = mejor${sinCruces ? ' · sin datos de cruces (cruces=1)' : ''}</span></div>
    <div class="cmp-summary scorecard-cards">${cards.map(c =>
        `<div class="cmp-card"><div class="cmp-card-label">${c.label}</div><div class="cmp-card-val ${c.cls || ''}">${c.val == null ? '—' : c.val}</div></div>`).join('')}</div>
    ${tabla('POR CATEGORÍA', data.por_categoria)}
    ${tabla('POR CRUCE DE COMPRA', data.por_cruce)}`;
}

async function exportarDataset() {
    if (cmpToolsStatus) { cmpToolsStatus.className = 'live-status warn'; cmpToolsStatus.textContent = 'Exportando dataset...'; }
    try {
        const res = await proxyFetch('/dataset?guardar=1');
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || 'no se pudo exportar');
        if (cmpToolsStatus) { cmpToolsStatus.className = 'live-status ok'; cmpToolsStatus.textContent = `✓ ${data.n} filas guardadas en ${data.guardado || 'data/dataset_entrenamiento.csv'}`; }
    } catch (e) {
        if (cmpToolsStatus) { cmpToolsStatus.className = 'live-status err'; cmpToolsStatus.textContent = `Error: ${e.message}`; }
    }
}

if (btnScorecardAgregado) btnScorecardAgregado.addEventListener('click', fetchScorecardAgregado);
if (btnExportDataset) btnExportDataset.addEventListener('click', exportarDataset);

// ─── PESTAÑAS ─────────────────────────────────────────────────────────────────
document.querySelectorAll('.live-tab').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.live-tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        const tab = btn.dataset.tab;
        panelMapa.style.display = tab === 'mapa' ? 'block' : 'none';
        panelComparacion.style.display = tab === 'comparacion' ? 'block' : 'none';
        if (tab === 'mapa') {
            renderLiveMapPicker();
            renderLiveDetail();
            syncSerieBuilder();
        } else {
            fetchComparacion();
            fetchScorecard();
        }
    });
});

// ─── EVENTOS LISTA ────────────────────────────────────────────────────────────
btnRefreshSims.addEventListener('click', loadSimulaciones);
chkShowStale.addEventListener('change', () => {
    showStale = chkShowStale.checked;
    renderSimList();
});
// Cancelar la espera del job de re-precalculo (botón inyectado en el estado).
simListStatus.addEventListener('click', e => {
    if (e.target && e.target.id === 'btnCancelRecalc') recalcStopped = true;
});

// ─── INIT ─────────────────────────────────────────────────────────────────────
// En paralelo: /simulaciones ya devuelve `modelo_version`, así que no hace
// falta esperar a /modelo_version (antes se encadenaba y sumaba ~1.5 s).
loadAvailableMaps();
loadModeloVersion();
loadSimulaciones();
