const API = `${window.location.origin}/api`;

// Todas las llamadas al servicio de predicción van por el proxy de la web
// (/api/aletheia/...): las lecturas de caché (modelo_version/predicciones)
// salen directo de Turso (sin servidor), `equipos` del servicio (Render) y el
// cómputo/mutaciones (precalcular + su polling de estado, asociar) del PC/ngrok.
// El POST de precalcular es async (202 al instante). La clave API la añade el
// proxy server-side; nunca llega al navegador.
function proxyFetch(path, options = {}) {
    return fetch(`${API}/aletheia/${String(path).replace(/^\/+/, '')}`, options);
}

let teams = [];
let selectedA = null;
let selectedB = null;
let nSim = 10000;
const TEAM_ABBREV_CACHE = {};
let cacheRows = {};        // 'map|side' -> fila (solo para el badge de caché)
let totalCombos = 26;      // fallback; se ajusta al pool vigente (map_pool)

// ─── JOB PERSISTENTE / TIMEOUTS ───────────────────────────────────────────────
// El job de precálculo se guarda por enfrentamiento (match_id+equipos) en
// localStorage: sobrevive a recargas (reanuda el polling) y se comparte entre
// pestañas. La API lo persiste en Turso con TTL 24 h.
const JOBS_KEY = 'ae_precalcular_jobs';
const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const POST_TIMEOUT_MS = 190000;  // el POST puede tardar (carga del motor en frío)
const POLL_MS = 2500;            // sondeo de /precalcular/estado (2-3 s)
const POLL_TIMEOUT_MS = 30000;   // corte por request de sondeo
const RETRY_VUELO_MS = 1500;     // espera antes del único reintento del POST

let matchId = 0;                 // id de vlr.gg parseado del input
let preparedMatchId = 0;         // id usado en el último PREPARAR (para desde_match_id)
let preparedModelVersion = null; // hash del modelo con el que se preparó
let serviceModelVersion = null;  // hash del modelo vigente en el servicio
let serviceModeloDesactualizado = false;  // true = reentreno/cambio sin reiniciar
let prepareBusy = false;
let jobStopped = false;          // permite cancelar la espera del job
let cacheError = null;           // error al leer la caché (null = ok)
let modelVersionError = null;    // error al leer /modelo_version (null = ok)

// ─── DOM ──────────────────────────────────────────────────────────────────────
const gridA = document.getElementById('teamGridA');
const gridB = document.getElementById('teamGridB');
const selA = document.getElementById('selectedA');
const selB = document.getElementById('selectedB');
const searchA = document.getElementById('searchA');
const searchB = document.getElementById('searchB');

const preparePanel = document.getElementById('preparePanel');
const matchIdInput = document.getElementById('matchIdInput');
const matchIdBadge = document.getElementById('matchIdBadge');
const modelBadge = document.getElementById('modelBadge');
const cacheBadge = document.getElementById('cacheBadge');
const btnAsociar = document.getElementById('btnAsociar');
const btnPreparar = document.getElementById('btnPreparar');
const prepareTimer = document.getElementById('prepareTimer');
const prepareStatus = document.getElementById('prepareStatus');
const combosHint = document.getElementById('combosHint');

// ─── HELPERS ──────────────────────────────────────────────────────────────────
const pct = v => Math.round((v || 0) * 100);
const sleep = ms => new Promise(r => setTimeout(r, ms));

function parseMatchId(raw) {
    const m = String(raw || '').match(/(\d+)/);
    return m ? parseInt(m[1], 10) : 0;
}

// ─── JOBS PERSISTIDOS (localStorage, por enfrentamiento) ─────────────────────
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
    try { localStorage.setItem(JOBS_KEY, JSON.stringify(jobs)); } catch (e) { /* modo privado */ }
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

// match_id con el que se preparó cada enfrentamiento: el job se borra al
// terminar, así que para el `desde_match_id` de ASOCIAR (incluso tras recargar
// la página) se guarda aparte, indexado por la pareja de equipos.
const PREPARED_KEY = 'ae_prepared_match_ids';

function claveEquipos(a, b) {
    return `eq:${String(a || '').trim().toLowerCase()}|${String(b || '').trim().toLowerCase()}`;
}

function leerPreparados() {
    try {
        const raw = localStorage.getItem(PREPARED_KEY);
        const obj = raw ? JSON.parse(raw) : {};
        return obj && typeof obj === 'object' ? obj : {};
    } catch (e) {
        return {};
    }
}

function guardarPreparado(a, b, mid) {
    if (!a || !b) return;
    const obj = leerPreparados();
    obj[claveEquipos(a, b)] = parseInt(mid, 10) || 0;
    try { localStorage.setItem(PREPARED_KEY, JSON.stringify(obj)); } catch (e) { /* modo privado */ }
}

function preparadoGuardado(a, b) {
    if (!a || !b) return 0;
    return parseInt(leerPreparados()[claveEquipos(a, b)], 10) || 0;
}

// Job guardado del enfrentamiento; tolera que se preparara sin id y que ahora
// se haya escrito el match_id (misma pareja de equipos).
function jobGuardado(a, b, mid) {
    if (!a || !b) return null;
    const jobs = leerJobs();
    const id = parseInt(mid, 10) || 0;
    let job = jobs[claveEnfrentamiento(a, b, id)];
    if (!job && id > 0) job = jobs[claveEnfrentamiento(a, b, 0)];
    if (!job) job = Object.values(jobs).find(j => j.equipo_a === a && j.equipo_b === b && jobVigente(j));
    return jobVigente(job) ? job : null;
}

function jobMasReciente() {
    return Object.values(leerJobs())
        .filter(jobVigente)
        .sort((x, y) => (y.started_at || 0) - (x.started_at || 0))[0] || null;
}

// ─── CARGAR EQUIPOS ───────────────────────────────────────────────────────────
async function loadTeams() {
    try {
        const res = await fetch(`${API}/aletheia/equipos`);
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || 'Error al cargar equipos');
        teams = data.teams || [];
        renderTeamGrids(teams);
    } catch (e) {
        const err = `<div style="color:var(--red);padding:12px;font-size:11px">No se pudo conectar al servicio de predicción</div>`;
        gridA.innerHTML = err;
        gridB.innerHTML = err;
    }
}

// Nº real de combinaciones = mapas del pool vigente (`map_pool`, en_pool=1) × 2
// lados. Antes estaba fijo en 26 (13 mapas); con el pool actual son 7×2 = 14.
async function loadPoolCombos() {
    try {
        const res = await fetch(`${API}/aletheia/mapas?pool=1`);
        const data = await res.json();
        const mapas = data && Array.isArray(data.mapas) ? data.mapas : [];
        if (data && data.ok && mapas.length) {
            totalCombos = mapas.length * 2;
            if (combosHint) {
                combosHint.textContent =
                    `${mapas.length} mapas × 2 lados = ${totalCombos} combinaciones`;
            }
        }
    } catch (e) { /* se mantiene el fallback */ }
    if (selectedA && selectedB) updateCacheBadge();
}

function renderTeamGrids(list) {
    renderGrid(gridA, list, 'a');
    renderGrid(gridB, list, 'b');
}

function renderGrid(container, list, side) {
    container.innerHTML = '';
    list.forEach(t => {
        const card = document.createElement('div');
        card.className = 'team-card';
        const isSelA = t.name === selectedA;
        const isSelB = t.name === selectedB;
        if (isSelA) card.classList.add('selected-a');
        if (isSelB) card.classList.add('selected-b');
        if ((side === 'a' && isSelB) || (side === 'b' && isSelA)) card.classList.add('disabled');
        card.innerHTML = `<div class="tc-abbrev">${t.abbrev || t.name}</div>`;
        card.addEventListener('click', () => selectTeam(t, side));
        container.appendChild(card);
    });
}

// ─── SELECCIÓN ───────────────────────────────────────────────────────────────
function selectTeam(team, side) {
    TEAM_ABBREV_CACHE[team.name] = team.abbrev || team.name;
    if (side === 'a') {
        selectedA = team.name;
        selA.innerHTML = `<span>${team.name}</span>`;
        selA.classList.add('has-team');
        selA.title = team.name;
    } else {
        selectedB = team.name;
        selB.innerHTML = `<span>${team.name}</span>`;
        selB.classList.add('has-team');
        selB.title = team.name;
    }
    renderTeamGrids(filterTeams('', side));
    showPrepare();
}

function showPrepare() {
    const ready = !!(selectedA && selectedB);
    preparePanel.style.display = ready ? 'block' : 'none';
    if (ready) preparedMatchId = preparadoGuardado(selectedA, selectedB);
    updateActionState();
    if (ready) {
        loadCacheSummary();
    }
}

function filterTeams(q, side) {
    const query = (q || '').toLowerCase().trim();
    return query ? teams.filter(t =>
        t.name.toLowerCase().includes(query) || (t.abbrev || '').toLowerCase().includes(query)
    ) : teams;
}

searchA.addEventListener('input', () => renderGrid(gridA, filterTeams(searchA.value, 'a'), 'a'));
searchB.addEventListener('input', () => renderGrid(gridB, filterTeams(searchB.value, 'b'), 'b'));

// ─── SIMULACIONES ─────────────────────────────────────────────────────────────
document.querySelectorAll('.sim-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.sim-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        nSim = parseInt(btn.dataset.n);
        updateActionState();
        updateCacheBadge();
    });
});

// ─── ID DE PARTIDO (vlr.gg) ───────────────────────────────────────────────────
matchIdInput.addEventListener('input', () => {
    matchId = parseMatchId(matchIdInput.value);
    matchIdBadge.textContent = matchId > 0 ? `PARTIDO #${matchId}` : 'SIN ID';
    matchIdBadge.classList.toggle('has-id', matchId > 0);
    updateAssociarState();
});

matchIdInput.addEventListener('change', () => {
    loadCacheSummary();
});

// ─── ESTADO / BOTONES ─────────────────────────────────────────────────────────
function updateActionState() {
    const ready = !!(selectedA && selectedB);
    btnPreparar.disabled = !ready || prepareBusy;
    updateAssociarState();
}

function updateAssociarState() {
    btnAsociar.disabled = !(matchId > 0 && selectedA && selectedB) || prepareBusy;
}

// Lee las predicciones existentes para el badge de caché (sin preparar).
async function loadCacheSummary() {
    if (!selectedA || !selectedB) return;
    cacheError = null;
    const params = new URLSearchParams();
    if (matchId > 0) {
        params.set('match_id', matchId);
    } else {
        params.set('equipo_a', selectedA);
        params.set('equipo_b', selectedB);
    }
    try {
        const res = await proxyFetch(`/predicciones?${params.toString()}`);
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || `HTTP ${res.status}`);
        cacheRows = {};
        if (Array.isArray(data.predicciones)) {
            data.predicciones.forEach(p => {
                if (p && p.map_name) cacheRows[`${p.map_name}|${p.lado_inicial_a}`] = p;
            });
        }
    } catch (e) {
        cacheRows = {};
        cacheError = e.message || 'servicio no disponible';
    }
    updateCacheBadge();
}

// ¿Hay que re-precalcular forzando? Solo si el modelo del servicio cambió:
// con la caché completa y vigente, forzar:true recomputaría 26 filas sin motivo
// (el backend las sirve desde caché con forzar:false).
function needsReprepare() {
    const rows = Object.values(cacheRows);
    return !!serviceModelVersion && rows.some(r => r.modelo_version && r.modelo_version !== serviceModelVersion);
}

// Filas de caché realmente vigentes: del modelo actual y con `n_sim` >= el
// solicitado. El contador "8/26 en caché" contaba filas viejas que el job
// recalcularía; así el número coincide con lo que el motor reutiliza.
function filasVigentes() {
    return Object.values(cacheRows).filter(r => {
        if (!r || !r.map_name) return false;
        if (serviceModelVersion && String(r.modelo_version || '') !== String(serviceModelVersion)) return false;
        const n = Number(r.n_sim);
        if (!isFinite(n)) return false;
        return n >= nSim;
    });
}

function updateCacheBadge() {
    const bpText = btnPreparar.querySelector('.bp-text');
    if (!selectedA || !selectedB) {
        cacheBadge.className = 'cache-badge';
        cacheBadge.textContent = '';
        return;
    }
    if (cacheError) {
        cacheBadge.className = 'cache-badge warn';
        cacheBadge.textContent = `⚠ no se pudo leer la caché: ${cacheError}`;
        if (bpText) bpText.textContent = 'PREPARAR PARTIDO';
        return;
    }
    const rows = Object.values(cacheRows);
    const n = filasVigentes().length;
    const staleModel = !!serviceModelVersion && rows.some(r => r.modelo_version && r.modelo_version !== serviceModelVersion);
    if (rows.length === 0) {
        cacheBadge.className = 'cache-badge warn';
        cacheBadge.textContent = '⚠ sin predicciones en caché';
    } else if (n >= totalCombos) {
        cacheBadge.className = 'cache-badge ok';
        cacheBadge.textContent = `✓ ya predicho (${n} filas vigentes)`;
    } else if (staleModel) {
        const sample = (rows.find(r => r.modelo_version) || {}).modelo_version || '?';
        cacheBadge.className = 'cache-badge warn';
        cacheBadge.textContent = `⚠ ${n}/${totalCombos} filas vigentes (modelo ${sample}) — RE-PREPARAR`;
    } else {
        cacheBadge.className = 'cache-badge partial';
        cacheBadge.textContent = `${n}/${totalCombos} filas vigentes en caché`;
    }
    if (bpText) bpText.textContent = needsReprepare() ? 'RE-PREPARAR' : 'PREPARAR PARTIDO';
}

// ─── MODELO / VERSIÓN ─────────────────────────────────────────────────────────
async function refreshModelVersion() {
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
    updateModelBadge();
    if (selectedA && selectedB) updateCacheBadge();
}

function updateModelBadge() {
    if (modelVersionError) {
        modelBadge.className = 'model-badge stale';
        modelBadge.textContent = `⚠ no se pudo leer modelo_version (${modelVersionError})`;
        return;
    }
    if (!serviceModelVersion) { modelBadge.textContent = ''; modelBadge.className = 'model-badge'; return; }
    const stale = !!(preparedModelVersion && preparedModelVersion !== serviceModelVersion);
    const desactualizado = serviceModeloDesactualizado;
    const aviso = desactualizado
        ? ' · ⚠ servicio desactualizado: reinicia ALETHEIA_PREDICT; las filas nuevas quedarán viejas al reiniciar'
        : '';
    modelBadge.className = 'model-badge' + (stale || desactualizado ? ' stale' : '');
    if (stale) {
        modelBadge.textContent = `⚠ modelo ${serviceModelVersion} — RE-PREPARAR${aviso}`;
    } else if (preparedModelVersion) {
        modelBadge.textContent = `modelo ${serviceModelVersion} · preparado${aviso}`;
    } else {
        modelBadge.textContent = `modelo ${serviceModelVersion}${aviso}`;
    }
}

// ─── PREPARAR PARTIDO (precalcular async, por el proxy) ─────────────────────
btnPreparar.addEventListener('click', prepararPartido);

// Delegación en el estado: "cancelar espera" deja de pollear (el job sigue
// vivo y persistido; se puede reanudar) y "reintentar" relanza el flujo.
prepareStatus.addEventListener('click', e => {
    const t = e.target;
    if (!t) return;
    if (t.id === 'btnCancelPrepare') jobStopped = true;
    if (t.id === 'btnRetryPrepare' && !prepareBusy) prepararPartido();
});

function mostrarEstadoPreparar(html, clase) {
    prepareStatus.style.display = 'block';
    prepareStatus.className = `prepare-status ${clase || 'running'}`;
    prepareStatus.innerHTML = html;
}

// POST con corte de tiempo: el motor en frío puede tardar 1-2 min. La petición
// no se duplica (máx. un reintento, y solo sin job previo).
function fetchConTimeout(path, options = {}, ms = POST_TIMEOUT_MS) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), ms);
    return proxyFetch(path, { ...options, signal: ctrl.signal })
        .finally(() => clearTimeout(timer));
}

// Convierte una respuesta con job (202 o 429 con el job activo) en job local.
function adoptarJobData(data) {
    if (!data || !data.job_id) return null;
    const job = {
        clave: claveEnfrentamiento(selectedA, selectedB, matchId),
        job_id: data.job_id,
        equipo_a: selectedA,
        equipo_b: selectedB,
        match_id: matchId,
        n_sim: data.n_sim || nSim,
        total: data.total || totalCombos,
        modelo_version: data.modelo_version || null,
        estado: data.estado || 'en_proceso',
        started_at: Date.now(),
    };
    guardarJob(job);
    return job;
}

async function prepararPartido() {
    if (!selectedA || !selectedB || prepareBusy) return;

    // 2) Job ya persistido para este enfrentamiento: reanudar el polling en vez
    // de volver a POSTear (sobrevive a recargas y se comparte entre pestañas).
    const guardado = jobGuardado(selectedA, selectedB, matchId);
    if (guardado) { await atenderJob(guardado); return; }

    prepareBusy = true;
    btnPreparar.disabled = true;
    btnPreparar.classList.add('running');
    updateAssociarState();

    const t0 = Date.now();
    const timer = setInterval(() => {
        prepareTimer.textContent = `${Math.floor((Date.now() - t0) / 1000)}s`;
    }, 250);
    const finish = () => {
        clearInterval(timer);
        prepareTimer.textContent = '';
        prepareBusy = false;
        btnPreparar.classList.remove('running');
        updateActionState();
    };

    mostrarEstadoPreparar(`⏳ Precomputando ${Math.ceil(totalCombos / 2)} mapas × 2 lados (${nSim.toLocaleString()} sims) con match_id <strong>#${matchId || 0}</strong>. <strong>No cierres esta pestaña.</strong>`);

    let res = null;
    let data = null;
    const MAX_INTENTOS = 2;   // 1 POST + un único reintento (solo sin job previo)
    for (let intento = 1; intento <= MAX_INTENTOS; intento++) {
        try {
            res = await fetchConTimeout('/precalcular', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    equipo_a: selectedA,
                    equipo_b: selectedB,
                    n_sim: nSim,
                    match_id: matchId,
                    forzar: needsReprepare(),
                }),
            }, POST_TIMEOUT_MS);
            try { data = await res.json(); } catch (e) { data = null; }
        } catch (e) {
            const agotado = e && e.name === 'AbortError';
            // No se relanza el job: el reintento único sirve para descubrir por
            // el 429 (con `job_id`) el job que pudo crearse pese al timeout.
            if (intento < MAX_INTENTOS && !jobGuardado(selectedA, selectedB, matchId)) {
                mostrarEstadoPreparar(`⚠ Sin respuesta del servicio (${agotado ? 'tiempo agotado' : 'túnel/PC'}). Comprobando si el precálculo ya está en curso…`);
                await sleep(RETRY_VUELO_MS);
                continue;
            }
            const activo = jobGuardado(selectedA, selectedB, matchId);
            finish();
            if (activo) { await atenderJob(activo); return; }
            mostrarEstadoPreparar(
                `Sin respuesta de ALETHEIA_PREDICT (${agotado ? 'tiempo agotado' : 'túnel/PC'}). ` +
                `El job puede seguir en curso; <button id="btnRetryPrepare" class="link-cancel">reintentar</button>`, 'err');
            return;
        }

        // 3) 429 = "ya hay un precálculo en curso": nunca es error fatal.
        const enCurso = res.status === 429
            || (data && data.ok === false && /en curso/i.test(String(data.error || '')));
        if (enCurso) {
            const activo = adoptarJobData(data) || jobGuardado(selectedA, selectedB, matchId);
            finish();
            if (activo) { await atenderJob(activo); return; }
            mostrarEstadoPreparar(
                `⚠ Ya hay un precálculo en curso (otra pestaña o usuario). Espera a que termine y ` +
                `<button id="btnRetryPrepare" class="link-cancel">reintentar</button>; tu selección no se pierde.`, 'warn');
            return;
        }

        // 502/504/5xx: el túnel o el PC no responden. Un único reintento.
        if (res.status >= 500) {
            if (intento < MAX_INTENTOS) {
                mostrarEstadoPreparar('⚠ El servicio no respondió; comprobando si el precálculo ya está en curso…');
                await sleep(RETRY_VUELO_MS);
                continue;
            }
            const activo = jobGuardado(selectedA, selectedB, matchId);
            finish();
            if (activo) { await atenderJob(activo); return; }
            mostrarEstadoPreparar('Servicio no disponible, reintenta.', 'err');
            return;
        }
        break;
    }

    // Fallback: respuesta síncrona antigua (sin job_id).
    if (!data || !data.job_id) {
        if (!data || !data.ok) {
            const status = res ? res.status : '—';
            finish();
            mostrarEstadoPreparar(`Error: ${(data && data.error) || `HTTP ${status}`}`, 'err');
            return;
        }
        const secs = data.tiempo_s != null ? data.tiempo_s : ((Date.now() - t0) / 1000).toFixed(1);
        preparedMatchId = matchId;
        guardarPreparado(selectedA, selectedB, preparedMatchId);
        if (data.modelo_version) preparedModelVersion = data.modelo_version;
        await refreshModelVersion();
        if (!preparedModelVersion) preparedModelVersion = serviceModelVersion;
        updateModelBadge();
        mostrarEstadoPreparar(
            `✓ ${data.total || totalCombos} combinaciones listas en <strong>${secs}s</strong>` +
            ` · ${data.computados != null ? data.computados + ' computadas, ' : ''}` +
            `${data.desde_cache != null ? data.desde_cache + ' desde caché' : ''}` +
            ` · modelo ${preparedModelVersion || '—'}`, 'ok');
        loadCacheSummary();
        finish();
        return;
    }

    // Asíncrono (202): persistir el job y pollear su estado.
    const job = adoptarJobData(data);
    finish();
    if (job) await atenderJob(job);
}

// Atiende un job ya creado (nuevo, adoptado de un 429 o persistido tras una
// recarga): pollea su estado con barra de progreso y, al terminar, refresca la
// caché y limpia el job persistido.
async function atenderJob(job) {
    if (!job || prepareBusy) return;
    prepareBusy = true;
    btnPreparar.disabled = true;
    btnPreparar.classList.add('running');
    updateAssociarState();

    const t0 = job.started_at || Date.now();
    const timer = setInterval(() => {
        prepareTimer.textContent = `${Math.floor((Date.now() - t0) / 1000)}s`;
    }, 250);
    const finish = () => {
        clearInterval(timer);
        prepareTimer.textContent = '';
        prepareBusy = false;
        btnPreparar.classList.remove('running');
        updateActionState();
    };

    const result = await pollPrecalcularJob(job.job_id, t0, job.total || totalCombos)
        .catch(e => ({ status: 'error', error: (e && e.message) || 'fallo inesperado' }));

    if (result.status === 'listo') {
        quitarJob(job.clave);
        preparedMatchId = job.match_id || matchId;
        guardarPreparado(job.equipo_a || selectedA, job.equipo_b || selectedB,
                         preparedMatchId);
        // El modelo vigente viene en el 202 (data.modelo_version); el job de
        // estado puede no incluirlo.
        preparedModelVersion = (result.job && result.job.modelo_version)
            || job.modelo_version || serviceModelVersion || null;
        await refreshModelVersion();
        updateModelBadge();
        const j = result.job || {};
        const secs = j.tiempo_s != null ? j.tiempo_s : result.elapsed;
        mostrarEstadoPreparar(
            `✓ ${result.total} combinaciones listas en ${secs}s · ` +
            `${j.computados != null ? j.computados : 0} computadas · ` +
            `${j.desde_cache != null ? j.desde_cache : 0} desde caché · ` +
            `modelo ${preparedModelVersion || j.modelo_version || '—'}`, 'ok');
        loadCacheSummary();
    } else if (result.status === 'error') {
        quitarJob(job.clave);
        mostrarEstadoPreparar(
            `Error: ${result.error || 'no se pudo precomputar'}. ` +
            `<button id="btnRetryPrepare" class="link-cancel">reintentar</button>`, 'err');
    } else if (result.status === 'lost') {
        quitarJob(job.clave);
        mostrarEstadoPreparar('El servicio se reinició y perdió el job. Vuelve a PREPARAR PARTIDO.', 'err');
    } else {
        // Cancelada: el job sigue en Turso y queda persistido para reanudar.
        mostrarEstadoPreparar('Espera cancelada. El job sigue registrado: pulsa PREPARAR PARTIDO para reanudar.', 'warn');
    }
    finish();
}

// Poll con reintentos: NUNCA aborta por un fallo de red transitorio
// (ERR_PROXY_CONNECTION_FAILED, PC dormido, pestaña en segundo plano...).
// La barra usa `job.progreso` (0..1), no `mapas_hechos/total` (las filas son
// mapas × 2). Sondea cada 2-3 s hasta `listo` o `error`.
async function pollPrecalcularJob(jobId, t0, totalCombinaciones) {
    jobStopped = false;
    let fails = 0;
    while (!jobStopped) {
        const elapsed = Math.floor((Date.now() - t0) / 1000);
        let r, jd;
        try {
            r = await fetchConTimeout(`/precalcular/estado?job_id=${encodeURIComponent(jobId)}`, {}, POLL_TIMEOUT_MS);
            try { jd = await r.json(); } catch (e) { jd = null; }
        } catch (e) {
            fails++;
            prepareStatus.className = 'prepare-status running';
            prepareStatus.innerHTML = `⚠ Sin conexión con ALETHEIA_PREDICT (túnel/PC caído). Reintentando #${fails} · ${elapsed}s. ` +
                `Puedes dejarlo abierto. <button id="btnCancelPrepare" class="link-cancel">cancelar espera</button>`;
            await sleep(Math.min(POLL_MS + fails * 500, 10000));
            continue;
        }

        if (r.status === 404) {
            return { status: 'lost' };
        }

        // 5xx/ok:false del proxy puede ser transitorio (túnel): reintentar.
        if (r.status >= 500 || !jd || !jd.job) {
            if (!jd || r.status >= 500) {
                fails++;
                prepareStatus.className = 'prepare-status running';
                prepareStatus.innerHTML = `⚠ El servicio no responde (HTTP ${r.status}). Reintentando #${fails} · ${elapsed}s. ` +
                    `Puedes dejarlo abierto. <button id="btnCancelPrepare" class="link-cancel">cancelar espera</button>`;
                await sleep(Math.min(POLL_MS + fails * 500, 10000));
                continue;
            }
            return { status: 'error', error: (jd && jd.error) || 'job no encontrado' };
        }

        fails = 0;
        const job = jd.job;
        const total = job.total || totalCombinaciones;

        if (job.estado === 'en_proceso') {
            const progreso = Math.max(0, Math.min(1, Number(job.progreso) || 0));
            const progPct = Math.round(progreso * 100);
            const mapas = job.mapas_hechos != null ? job.mapas_hechos : 0;
            prepareStatus.className = 'prepare-status running';
            prepareStatus.innerHTML =
                `<div class="prep-bar"><div class="prep-bar-fill" style="width:${progPct}%"></div></div>` +
                `⏳ en proceso · <strong>${progPct}%</strong> · mapa ${mapas}/${Math.ceil(total / 2)} · ${elapsed}s. ` +
                `<strong>No cierres esta pestaña.</strong> ` +
                `<button id="btnCancelPrepare" class="link-cancel">cancelar espera</button>`;
            await sleep(POLL_MS);
            continue;
        }

        if (job.estado === 'listo') {
            return { status: 'listo', job, total, elapsed };
        }

        if (job.estado === 'error') {
            return { status: 'error', error: job.error };
        }

        await sleep(POLL_MS);
    }
    return { status: 'cancelled' };
}

// ─── ASOCIAR ID ───────────────────────────────────────────────────────────────
btnAsociar.addEventListener('click', asociarId);

async function asociarId() {
    if (!selectedA || !selectedB || matchId <= 0 || prepareBusy) return;
    btnAsociar.disabled = true;
    btnAsociar.classList.add('running');
    prepareStatus.style.display = 'block';
    prepareStatus.className = 'prepare-status running';
    prepareStatus.textContent = `Asociando predicciones a #${matchId}...`;

    try {
        const res = await proxyFetch('/asociar', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                equipo_a: selectedA,
                equipo_b: selectedB,
                match_id: matchId,
                desde_match_id: preparedMatchId || 0,
            }),
        });
        const data = await res.json();
        if (!data.ok) {
            prepareStatus.className = 'prepare-status err';
            prepareStatus.textContent = `Error: ${data.error || 'no se pudo asociar'}`;
            return;
        }
        preparedMatchId = matchId;
        guardarPreparado(selectedA, selectedB, preparedMatchId);
        prepareStatus.className = 'prepare-status ok';
        prepareStatus.textContent = `✓ ${data.filas_actualizadas != null ? data.filas_actualizadas : 0} filas asociadas a #${matchId}.`;
        loadCacheSummary();
    } catch (e) {
        prepareStatus.className = 'prepare-status err';
        prepareStatus.textContent = `Servicio de predicción no disponible: ${e.message}`;
    } finally {
        btnAsociar.classList.remove('running');
        updateAssociarState();
    }
}

// ─── INIT ─────────────────────────────────────────────────────────────────────
// 2) Al recargar a mitad del precálculo: restaura el enfrentamiento del job
// persistido y reanuda el polling sin volver a POSTear.
function reanudarJobGuardado() {
    const job = jobMasReciente();
    if (!job) return;
    if (job.equipo_a) {
        selectedA = job.equipo_a;
        selA.innerHTML = `<span>${job.equipo_a}</span>`;
        selA.classList.add('has-team');
        selA.title = job.equipo_a;
    }
    if (job.equipo_b) {
        selectedB = job.equipo_b;
        selB.innerHTML = `<span>${job.equipo_b}</span>`;
        selB.classList.add('has-team');
        selB.title = job.equipo_b;
    }
    if (job.match_id > 0) {
        matchId = job.match_id;
        matchIdInput.value = job.match_id;
        matchIdBadge.textContent = `PARTIDO #${matchId}`;
        matchIdBadge.classList.add('has-id');
    }
    if (job.n_sim) {
        nSim = job.n_sim;
        document.querySelectorAll('.sim-btn').forEach(b => {
            b.classList.toggle('active', parseInt(b.dataset.n, 10) === job.n_sim);
        });
    }
    renderTeamGrids(teams);
    showPrepare();
    atenderJob(job);
}

async function init() {
    await loadTeams();
    await loadPoolCombos();
    refreshModelVersion();
    reanudarJobGuardado();
}

init();
