const API = `${window.location.origin}/api`;

// Las llamadas normales (equipos, caché, modelo_version, asociar) van por el
// proxy de la web (/api/aletheia/...). Solo la corrida larga (precalcular) y su
// polling de estado se piden DIRECTO a ngrok para no chocar con el timeout de
// gunicorn/Render. Toda petición a ngrok lleva el header anti-warning.
const PREDICT_DIRECTO = 'https://snugly-encore-sweep.ngrok-free.dev';
const NGROK_HEADER = { 'ngrok-skip-browser-warning': '1' };

function proxyFetch(path, options = {}) {
    return fetch(`${API}/aletheia/${String(path).replace(/^\/+/, '')}`, options);
}

let teams = [];
let selectedA = null;
let selectedB = null;
let nSim = 10000;
const TEAM_ABBREV_CACHE = {};
let cacheRows = {};        // 'map|side' -> fila (solo para el badge de caché)
const TOTAL_COMBOS = 26;   // 13 mapas × 2 lados

let matchId = 0;                 // id de vlr.gg parseado del input
let preparedMatchId = 0;         // id usado en el último PREPARAR (para desde_match_id)
let preparedModelVersion = null; // hash del modelo con el que se preparó
let serviceModelVersion = null;  // hash del modelo vigente en el servicio
let prepareBusy = false;
let jobStopped = false;          // permite cancelar la espera del job

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

// ─── HELPERS ──────────────────────────────────────────────────────────────────
const pct = v => Math.round((v || 0) * 100);
const sleep = ms => new Promise(r => setTimeout(r, ms));

function predictFetch(path, options = {}) {
    const headers = Object.assign({}, NGROK_HEADER, options.headers || {});
    return fetch(`${PREDICT_DIRECTO}${path}`, Object.assign({}, options, { headers }));
}

function parseMatchId(raw) {
    const m = String(raw || '').match(/(\d+)/);
    return m ? parseInt(m[1], 10) : 0;
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
        cacheRows = {};
        if (data.ok && Array.isArray(data.predicciones)) {
            data.predicciones.forEach(p => {
                if (p && p.map_name) cacheRows[`${p.map_name}|${p.lado_inicial_a}`] = p;
            });
        }
    } catch {
        cacheRows = {};
    }
    updateCacheBadge();
}

// ¿Hay que re-precalcular? Sí si ya está completo o si el modelo del servicio
// cambió (las filas cacheadas quedan con modelo_version viejo => vigente:false).
function needsReprepare() {
    const rows = Object.values(cacheRows);
    const staleModel = !!serviceModelVersion && rows.some(r => r.modelo_version && r.modelo_version !== serviceModelVersion);
    return staleModel || rows.length >= TOTAL_COMBOS;
}

function updateCacheBadge() {
    const bpText = btnPreparar.querySelector('.bp-text');
    if (!selectedA || !selectedB) {
        cacheBadge.className = 'cache-badge';
        cacheBadge.textContent = '';
        return;
    }
    const rows = Object.values(cacheRows);
    const n = rows.length;
    const staleModel = !!serviceModelVersion && rows.some(r => r.modelo_version && r.modelo_version !== serviceModelVersion);
    if (n === 0) {
        cacheBadge.className = 'cache-badge warn';
        cacheBadge.textContent = '⚠ sin predicciones en caché';
    } else if (staleModel) {
        const sample = (rows.find(r => r.modelo_version) || {}).modelo_version || '?';
        cacheBadge.className = 'cache-badge warn';
        cacheBadge.textContent = `⚠ ${n} filas con modelo ${sample} — RE-PREPARAR`;
    } else if (n >= TOTAL_COMBOS) {
        cacheBadge.className = 'cache-badge ok';
        cacheBadge.textContent = `✓ ya predicho (${n} filas en caché)`;
    } else {
        cacheBadge.className = 'cache-badge partial';
        cacheBadge.textContent = `${n}/${TOTAL_COMBOS} en caché`;
    }
    if (bpText) bpText.textContent = needsReprepare() ? 'RE-PREPARAR' : 'PREPARAR PARTIDO';
}

// ─── MODELO / VERSIÓN ─────────────────────────────────────────────────────────
async function refreshModelVersion() {
    try {
        const res = await proxyFetch('/modelo_version');
        const data = await res.json();
        if (data.ok) serviceModelVersion = data.modelo_version;
    } catch { }
    updateModelBadge();
    if (selectedA && selectedB) updateCacheBadge();
}

function updateModelBadge() {
    if (!serviceModelVersion) { modelBadge.textContent = ''; modelBadge.className = 'model-badge'; return; }
    const stale = !!(preparedModelVersion && preparedModelVersion !== serviceModelVersion);
    modelBadge.className = 'model-badge' + (stale ? ' stale' : '');
    if (stale) {
        modelBadge.textContent = `⚠ modelo ${serviceModelVersion} — RE-PREPARAR`;
    } else if (preparedModelVersion) {
        modelBadge.textContent = `modelo ${serviceModelVersion} · preparado`;
    } else {
        modelBadge.textContent = `modelo ${serviceModelVersion}`;
    }
}

// ─── PREPARAR PARTIDO (precalcular async, DIRECTO) ───────────────────────────
btnPreparar.addEventListener('click', prepararPartido);

// Botón "cancelar espera" que aparece si el túnel/PC no responde.
prepareStatus.addEventListener('click', e => {
    if (e.target && e.target.id === 'btnCancelPrepare') jobStopped = true;
});

async function prepararPartido() {
    if (!selectedA || !selectedB || prepareBusy) return;
    prepareBusy = true;
    btnPreparar.disabled = true;
    btnPreparar.classList.add('running');
    updateAssociarState();

    const t0 = Date.now();
    prepareStatus.style.display = 'block';
    prepareStatus.className = 'prepare-status running';
    prepareStatus.innerHTML = `⏳ Precomputando 13 mapas × 2 lados (${nSim.toLocaleString()} sims) con match_id <strong>#${matchId || 0}</strong>. <strong>No cierres esta pestaña.</strong>`;

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

    const failDisponible = () => {
        prepareStatus.className = 'prepare-status err';
        prepareStatus.textContent = 'Servicio no disponible, reintenta.';
        finish();
    };

    let data;
    try {
        const res = await predictFetch('/api/precalcular', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                equipo_a: selectedA,
                equipo_b: selectedB,
                n_sim: nSim,
                match_id: matchId,
                forzar: needsReprepare(),
            }),
        });
        if (res.status === 502 || res.status === 504) {
            failDisponible();
            return;
        }
        data = await res.json();
    } catch (e) {
        prepareStatus.className = 'prepare-status err';
        prepareStatus.textContent = `Sin conexión con ALETHEIA_PREDICT (túnel/PC): ${e.message}. Reintenta.`;
        finish();
        return;
    }

    // Fallback: respuesta síncrona antigua (sin job_id).
    if (!data.job_id) {
        if (!data.ok) {
            prepareStatus.className = 'prepare-status err';
            prepareStatus.textContent = `Error: ${data.error || 'no se pudo precomputar'}`;
            finish();
            return;
        }
        const secs = data.tiempo_s != null ? data.tiempo_s : ((Date.now() - t0) / 1000).toFixed(1);
        preparedMatchId = matchId;
        if (data.modelo_version) preparedModelVersion = data.modelo_version;
        await refreshModelVersion();
        if (!preparedModelVersion) preparedModelVersion = serviceModelVersion;
        updateModelBadge();
        prepareStatus.className = 'prepare-status ok';
        prepareStatus.innerHTML = `✓ ${data.total || 26} combinaciones listas en <strong>${secs}s</strong>` +
            ` · ${data.computados != null ? data.computados + ' computadas, ' : ''}` +
            `${data.desde_cache != null ? data.desde_cache + ' desde caché' : ''}` +
            ` · modelo ${preparedModelVersion || '—'}`;
        loadCacheSummary();
        finish();
        return;
    }

    // Asíncrono: el POST devolvió {job_id}. Poll resiliente: sobrevive a caídas
    // temporales del túnel/PC y a que el navegador esté en segundo plano.
    const jobId = data.job_id;
    const totalCombinaciones = data.total || 26;
    const initProg = Math.round((data.progreso || 0) * 100);
    prepareStatus.innerHTML = `⏳ en_proceso · ${initProg}% · mapa ${data.mapas_hechos || 0}/${Math.ceil(totalCombinaciones / 2)}. <strong>No cierres esta pestaña.</strong>`;

    const result = await pollPrecalcularJob(jobId, t0, totalCombinaciones);

    if (result.status === 'listo') {
        const secs = result.job.tiempo_s != null ? result.job.tiempo_s : result.elapsed;
        preparedMatchId = matchId;
        // El modelo vigente viene en el 202 (data.modelo_version); el job de
        // estado puede no incluirlo.
        preparedModelVersion = data.modelo_version || result.job.modelo_version || serviceModelVersion || null;
        await refreshModelVersion();
        updateModelBadge();
        prepareStatus.className = 'prepare-status ok';
        prepareStatus.textContent = `✓ ${result.total} combinaciones listas en ${secs}s · ${result.job.computados != null ? result.job.computados : 0} computadas · ${result.job.desde_cache != null ? result.job.desde_cache : 0} desde caché · modelo ${preparedModelVersion || result.job.modelo_version || '—'}`;
        loadCacheSummary();
    } else if (result.status === 'lost') {
        prepareStatus.className = 'prepare-status err';
        prepareStatus.textContent = 'El servicio se reinició y perdió el job. Vuelve a PREPARAR PARTIDO.';
    } else if (result.status === 'error') {
        prepareStatus.className = 'prepare-status err';
        prepareStatus.textContent = `Error: ${result.error || 'no se pudo precomputar'}`;
    } else {
        prepareStatus.className = 'prepare-status err';
        prepareStatus.textContent = 'Espera cancelada.';
    }
    finish();
}

// Poll con reintentos: NUNCA aborta por un fallo de red transitorio
// (ERR_PROXY_CONNECTION_FAILED, PC dormido, pestaña en segundo plano...).
async function pollPrecalcularJob(jobId, t0, totalCombinaciones) {
    jobStopped = false;
    let fails = 0;
    while (!jobStopped) {
        let r;
        try {
            r = await predictFetch(`/api/precalcular/estado?job_id=${encodeURIComponent(jobId)}`);
        } catch (e) {
            fails++;
            const elapsed = Math.floor((Date.now() - t0) / 1000);
            prepareStatus.className = 'prepare-status running';
            prepareStatus.innerHTML = `⚠ Sin conexión con ALETHEIA_PREDICT (túnel/PC caído). Reintentando #${fails} · ${elapsed}s. ` +
                `Puedes dejarlo abierto. <button id="btnCancelPrepare" class="link-cancel">cancelar espera</button>`;
            await sleep(Math.min(2000 + fails * 500, 10000));
            continue;
        }

        if (r.status === 404) {
            return { status: 'lost' };
        }

        let jd;
        try {
            jd = await r.json();
        } catch {
            fails++;
            await sleep(Math.min(2000 + fails * 500, 10000));
            continue;
        }

        if (!jd.ok || !jd.job) {
            return { status: 'error', error: (jd && jd.error) || 'job no encontrado' };
        }

        fails = 0;
        const job = jd.job;
        const total = job.total || totalCombinaciones;
        const elapsed = Math.floor((Date.now() - t0) / 1000);

        if (job.estado === 'en_proceso') {
            const progreso = Math.round((job.progreso || 0) * 100);
            prepareStatus.className = 'prepare-status running';
            prepareStatus.innerHTML = `⏳ ${progreso}% · mapa ${job.mapas_hechos || 0}/${total / 2} · ${elapsed}s. <strong>No cierres esta pestaña.</strong>`;
            await sleep(2000);
            continue;
        }

        if (job.estado === 'listo') {
            return { status: 'listo', job, total, elapsed };
        }

        if (job.estado === 'error') {
            return { status: 'error', error: job.error };
        }

        await sleep(2000);
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
loadTeams();
refreshModelVersion();
