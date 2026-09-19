const API = `${window.location.origin}/api`;

// ALETHEIA_PREDICT corre en el PC del usuario y se expone con ngrok.
// Las corridas largas (precalcular) se piden DIRECTO al servicio para que no
// las corte el timeout de gunicorn/Render. Equipos van por el proxy (rápidos).
const PREDICT_DIRECTO = 'https://snugly-encore-sweep.ngrok-free.dev';
const NGROK_HEADER = { 'ngrok-skip-browser-warning': '1' };

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

const comparisonPanel = document.getElementById('comparisonPanel');
const cmpSummary = document.getElementById('cmpSummary');
const cmpTableWrap = document.getElementById('cmpTableWrap');
const cmpStatus = document.getElementById('cmpStatus');

// ─── HELPERS ──────────────────────────────────────────────────────────────────
const pct = v => Math.round((v || 0) * 100);

function predictFetch(path, options = {}) {
    const headers = Object.assign({}, NGROK_HEADER, options.headers || {});
    return fetch(`${PREDICT_DIRECTO}${path}`, Object.assign({}, options, { headers }));
}

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

function escapeHtml(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
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
    comparisonPanel.style.display = ready ? 'block' : 'none';
    updateActionState();
    if (ready) {
        loadCacheSummary();
        fetchComparacion();
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
    fetchComparacion();
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
        const res = await predictFetch(`/api/predicciones?${params.toString()}`);
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

function updateCacheBadge() {
    const bpText = btnPreparar.querySelector('.bp-text');
    if (!selectedA || !selectedB) {
        cacheBadge.className = 'cache-badge';
        cacheBadge.textContent = '';
        return;
    }
    const n = Object.keys(cacheRows).length;
    if (n === 0) {
        cacheBadge.className = 'cache-badge warn';
        cacheBadge.textContent = '⚠ sin predicciones en caché';
    } else if (n >= TOTAL_COMBOS) {
        cacheBadge.className = 'cache-badge ok';
        cacheBadge.textContent = `✓ ya predicho (${n} filas en caché)`;
    } else {
        cacheBadge.className = 'cache-badge partial';
        cacheBadge.textContent = `${n}/${TOTAL_COMBOS} en caché`;
    }
    if (bpText) bpText.textContent = (n >= TOTAL_COMBOS) ? 'RE-PREPARAR' : 'PREPARAR PARTIDO';
}

// ─── MODELO / VERSIÓN ─────────────────────────────────────────────────────────
async function refreshModelVersion() {
    try {
        const res = await predictFetch('/api/modelo_version');
        const data = await res.json();
        if (data.ok) serviceModelVersion = data.modelo_version;
    } catch { }
    updateModelBadge();
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

// ─── COMPARACIÓN ──────────────────────────────────────────────────────────────
async function fetchComparacion() {
    if (!selectedA || !selectedB) return;
    cmpStatus.className = 'live-status';
    cmpStatus.textContent = 'Consultando comparación...';
    cmpSummary.innerHTML = '';
    cmpTableWrap.innerHTML = '';

    const params = new URLSearchParams();
    if (matchId > 0) {
        params.set('match_id', matchId);
    } else {
        params.set('equipo_a', selectedA);
        params.set('equipo_b', selectedB);
    }
    params.set('limite', '100');

    try {
        const res = await predictFetch(`/api/comparacion?${params.toString()}`);
        const data = await res.json();
        if (!data.ok || !data.resumen || !data.resumen.n) {
            cmpStatus.className = 'live-status warn';
            cmpStatus.textContent = 'Sin resultado real todavía (el partido no está en la DB).';
            return;
        }
        renderComparison(data);
    } catch (e) {
        cmpStatus.className = 'live-status err';
        cmpStatus.textContent = `Servicio de predicción no disponible: ${e.message}`;
    }
}

function renderComparison(data) {
    const r = data.resumen || {};
    const cards = [
        { label: 'N', val: r.n },
        { label: 'ACCURACY', val: fmtRatio(r.accuracy) },
        { label: 'BRIER', val: fmtNum(r.brier) },
        { label: 'LOG-LOSS', val: fmtNum(r.log_loss) },
        { label: 'FAVORITOS OK', val: r.favoritos_ok },
        { label: 'UPSETS', val: r.upsets },
        { label: 'INCIERTOS', val: r.inciertos },
    ];
    cmpSummary.innerHTML = cards.map(c => `
    <div class="cmp-card">
      <div class="cmp-card-label">${c.label}</div>
      <div class="cmp-card-val">${c.val == null ? '—' : c.val}</div>
    </div>`).join('');

    const tipoClass = t => t === 'favorito_gano' ? 'cmp-fav' : t === 'upset' ? 'cmp-upset' : 'cmp-unc';
    const tipoLabel = t => t === 'favorito_gano' ? 'favorito ganó' : t === 'upset' ? 'UPSET' : 'incierto';

    const rows = (data.detalle || []).map(d => {
        const ganador = d.gano_a_real ? (d.equipo_a || 'A') : (d.equipo_b || 'B');
        return `
      <tr class="cmp-row ${tipoClass(d.tipo)}">
        <td>${escapeHtml(d.map_name)}</td>
        <td>${d.lado_inicial_a === 'attack' ? 'ATK' : 'DEF'}</td>
        <td class="cmp-a">${pct(d.prob_victoria_a)}%</td>
        <td class="cmp-b">${pct(d.prob_victoria_b)}%</td>
        <td>${pct(d.prob_overtime)}%</td>
        <td>${escapeHtml(ganador)}</td>
        <td>${tipoLabel(d.tipo)}</td>
        <td>${d.resultado === 'acierto' ? '✓' : '✕'} ${escapeHtml(d.resultado)}</td>
      </tr>`;
    }).join('');

    cmpTableWrap.innerHTML = `
    <table class="cmp-table">
      <thead>
        <tr>
          <th>MAPA</th><th>LADO</th><th>P(A)</th><th>P(B)</th><th>OT</th>
          <th>GANÓ (REAL)</th><th>TIPO</th><th>RESULTADO</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;

    cmpStatus.className = 'live-status ok';
    cmpStatus.textContent = `✓ comparación con modelo ${data.modelo_version || '—'}`;
}

// ─── PREPARAR PARTIDO (precalcular async, DIRECTO) ───────────────────────────
btnPreparar.addEventListener('click', prepararPartido);

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
            }),
        });
        if (res.status === 502 || res.status === 504) {
            failDisponible();
            return;
        }
        data = await res.json();
    } catch (e) {
        prepareStatus.className = 'prepare-status err';
        prepareStatus.textContent = `Servicio de predicción no disponible: ${e.message}. Reintenta.`;
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

    // Asíncrono: poll cada 2 s.
    const jobId = data.job_id;
    const totalCombinaciones = data.total || 26;
    prepareStatus.innerHTML = `⏳ ${data.estado || 'en_proceso'} · 0% · mapa 0/${totalCombinaciones / 2}. <strong>No cierres esta pestaña.</strong>`;

    const poll = setInterval(async () => {
        let jd;
        try {
            const r = await predictFetch(`/api/precalcular/estado?job_id=${encodeURIComponent(jobId)}`);
            if (r.status === 404) {
                clearInterval(poll);
                failDisponible();
                return;
            }
            jd = await r.json();
        } catch (e) {
            clearInterval(poll);
            prepareStatus.className = 'prepare-status err';
            prepareStatus.textContent = `Servicio de predicción no disponible: ${e.message}. Reintenta.`;
            finish();
            return;
        }

        if (!jd.ok || !jd.job) {
            clearInterval(poll);
            prepareStatus.className = 'prepare-status err';
            prepareStatus.textContent = `Error: ${(jd && jd.error) || 'job no encontrado'}.`;
            finish();
            return;
        }

        const job = jd.job;
        const total = job.total || totalCombinaciones;
        const elapsed = Math.floor((Date.now() - t0) / 1000);

        if (job.estado === 'en_proceso') {
            const progreso = Math.round((job.progreso || 0) * 100);
            prepareStatus.className = 'prepare-status running';
            prepareStatus.innerHTML = `⏳ ${progreso}% · mapa ${job.mapas_hechos || 0}/${total / 2} · ${elapsed}s. <strong>No cierres esta pestaña.</strong>`;
            return;
        }

        if (job.estado === 'listo') {
            clearInterval(poll);
            const secs = job.tiempo_s != null ? job.tiempo_s : elapsed;
            preparedMatchId = matchId;
            preparedModelVersion = job.modelo_version;
            await refreshModelVersion();
            updateModelBadge();
            prepareStatus.className = 'prepare-status ok';
            prepareStatus.textContent = `✓ ${total} combinaciones listas en ${secs}s · ${job.computados != null ? job.computados : 0} computadas · ${job.desde_cache != null ? job.desde_cache : 0} desde caché · modelo ${job.modelo_version}`;
            loadCacheSummary();
            finish();
            return;
        }

        if (job.estado === 'error') {
            clearInterval(poll);
            prepareStatus.className = 'prepare-status err';
            prepareStatus.textContent = `Error: ${job.error || 'no se pudo precomputar'}`;
            finish();
            return;
        }
    }, 2000);
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
        const res = await predictFetch('/api/asociar', {
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
        fetchComparacion();
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
