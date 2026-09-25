const API = `${window.location.origin}/api`;

// EN VIVO lee de la caché por el proxy (/api/aletheia/...). La única corrida
// larga que puede lanzar es RE-PRECALCULAR (forzar:true), que va DIRECTO a
// ngrok para no chocar con el timeout de gunicorn/Render.
const PREDICT_DIRECTO = 'https://snugly-encore-sweep.ngrok-free.dev';
const NGROK_HEADER = { 'ngrok-skip-browser-warning': '1' };

function predictFetch(path, options = {}) {
    const headers = Object.assign({}, NGROK_HEADER, options.headers || {});
    return fetch(`${PREDICT_DIRECTO}${path}`, Object.assign({}, options, { headers }));
}

let availableMaps = [];    // 13 mapas del servicio (proxy /api/aletheia/mapas)
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
let serviceModelVersion = null;  // modelo vigente (GET /modelo_version)
let recalculating = false;       // evita doble RE-PRECALCULAR

// ─── DOM ──────────────────────────────────────────────────────────────────────
const simList = document.getElementById('simList');
const simListStatus = document.getElementById('simListStatus');
const btnRefreshSims = document.getElementById('btnRefreshSims');
const chkShowStale = document.getElementById('chkShowStale');

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
const cmpSummary = document.getElementById('cmpSummary');
const cmpTableWrap = document.getElementById('cmpTableWrap');
const cmpStatus = document.getElementById('cmpStatus');
const scorecardWrap = document.getElementById('scorecardWrap');
const scorecardAgregadoWrap = document.getElementById('scorecardAgregadoWrap');
const btnScorecardAgregado = document.getElementById('btnScorecardAgregado');
const btnExportDataset = document.getElementById('btnExportDataset');
const cmpToolsStatus = document.getElementById('cmpToolsStatus');

// ─── HELPERS ──────────────────────────────────────────────────────────────────
const pct = v => Math.round((v || 0) * 100);

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

function inferFormat(n) {
    if (n <= 0) return '—';
    if (n === 1) return 'Bo1';
    if (n <= 3) return 'Bo3';
    if (n <= 5) return 'Bo5';
    return `Bo${n}`;
}

function sameSim(a, b) {
    if (!a || !b) return false;
    if (a.match_id && b.match_id) return a.match_id === b.match_id;
    return a.equipo_a === b.equipo_a && a.equipo_b === b.equipo_b;
}

// Banda de confianza. Usa la que manda el backend (`confianza`) y, si no viene
// (p. ej. las filas crudas de /predicciones no la incluyen), la deriva con la
// misma regla conservadora que el motor: max(p, 1-p) >=0.62 alta, >=0.55 media.
function confBand(p) {
    if (!p) return null;
    if (p.confianza) return String(p.confianza).toLowerCase();
    const pa = Number(p.prob_victoria_a);
    const pb = Number(p.prob_victoria_b);
    if (isNaN(pa) && isNaN(pb)) return null;
    const pmax = Math.max(isNaN(pa) ? 0 : pa, isNaN(pb) ? 0 : pb);
    if (pmax >= 0.62) return 'alta';
    if (pmax >= 0.55) return 'media';
    return 'baja';
}

function confBadge(conf) {
    if (!conf) return '';
    return `<span class="conf-badge ${conf}"><span class="conf-dot"></span>${escapeHtml(conf)}</span>`;
}

// ─── MAPAS (proxy) ────────────────────────────────────────────────────────────
async function loadAvailableMaps() {
    mapsLoading = true;
    try {
        const res = await fetch(`${API}/aletheia/mapas`);
        const data = await res.json();
        if (data.ok) availableMaps = data.mapas || [];
    } catch { }
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
        if (data.modelo_version) serviceModelVersion = data.modelo_version;
        sims = data.simulaciones || [];
        // Marca no vigentes también por comparación de modelo_version (aunque el
        // backend no lo hubiera marcado), para ofrecer RE-PRECALCULAR.
        sims.forEach(s => {
            if (s.modelo_version && serviceModelVersion && s.modelo_version !== serviceModelVersion) {
                s.vigente = false;
            }
        });
        renderSimList();
        const vigentes = sims.filter(s => s.vigente !== false).length;
        simListStatus.className = 'live-status ok';
        simListStatus.textContent = `${vigentes} vigentes · ${sims.length} totales · modelo ${data.modelo_version || '—'}`;
    } catch (e) {
        simListStatus.className = 'live-status err';
        simListStatus.textContent = `Servicio de predicción no disponible: ${e.message}`;
    }
}

function renderSimList() {
    const list = showStale ? sims : sims.filter(s => s.vigente !== false);
    simList.innerHTML = '';
    if (!list.length) {
        simList.innerHTML = '<div class="live-hint" style="padding:14px">No hay simulaciones preparadas. Ve a <strong>PREPARAR PARTIDO</strong>.</div>';
        return;
    }
    list.forEach(s => {
        const vigente = s.vigente !== false;
        const item = document.createElement('div');
        item.className = 'sim-item' + (sameSim(current, s) ? ' selected' : '') + (vigente ? '' : ' stale');
        const mid = s.match_id ? `#${s.match_id}` : 'sin id';
        item.innerHTML = `
      <div class="si-teams">${escapeHtml(s.equipo_a)} <span class="si-vs">vs</span> ${escapeHtml(s.equipo_b)}</div>
      <div class="si-meta">${mid} · ${s.mapas != null ? s.mapas : '?'} mapas · ${(s.n_sim || 0).toLocaleString()} sims${vigente ? '' : ' · ⚠ re-preparar'}</div>
      <div class="si-actions">
        <button class="si-btn" data-act="id" title="Asignar/corregir el ID de vlr.gg">✎ ID</button>
        <button class="si-btn danger" data-act="del" title="Borrar estas predicciones">🗑 BORRAR</button>
      </div>`;
        item.addEventListener('click', () => selectSim(s));
        item.querySelector('[data-act="id"]').addEventListener('click', e => { e.stopPropagation(); asignarId(s); });
        item.querySelector('[data-act="del"]').addEventListener('click', e => { e.stopPropagation(); borrarSim(s); });
        simList.appendChild(item);
    });
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

// Modelo vigente del servicio (GET /modelo_version, vía proxy).
async function loadModeloVersion() {
    try {
        const res = await proxyFetch('/modelo_version');
        const data = await res.json();
        if (data.ok) serviceModelVersion = data.modelo_version;
    } catch { }
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
    selSimHead.innerHTML = `
    <div class="ss-head-teams">${escapeHtml(s.equipo_a)} <span class="si-vs">vs</span> ${escapeHtml(s.equipo_b)}</div>
    <div class="ss-head-meta">${s.match_id ? 'PARTIDO #' + s.match_id : 'sin id'} · ${s.n_sim ? Number(s.n_sim).toLocaleString() + ' sims' : ''} · modelo ${s.modelo_version || '—'}${needsRepre ? ` · <span style="color:var(--orange)">⚠ RE-PRECALCULAR${motivos.length ? ' (' + motivos.join(', ') + ')' : ''}</span>` : ''}</div>
    <div class="ss-head-actions">
      <button class="btn-nav" id="btnReprecalcular"${needsRepre ? '' : ' style="display:none"'}>↻ RE-PRECALCULAR</button>
    </div>`;
    const btnRepre = document.getElementById('btnReprecalcular');
    if (btnRepre) btnRepre.addEventListener('click', () => reprecalcular(s));
}

// ─── SELECCIÓN DE SIMULACIÓN ──────────────────────────────────────────────────
async function selectSim(s) {
    current = s;
    matchMaps = [];
    liveMap = null;
    liveBulk = null;
    liveSection.style.display = 'block';

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

// Re-precalcula el enfrentamiento actual con forzar:true (directo a ngrok) y
// refresca la lista. Es la única corrida larga que lanza EN VIVO.
async function reprecalcular(s) {
    if (!s || recalculating) return;
    recalculating = true;
    const btn = document.getElementById('btnReprecalcular');
    if (btn) { btn.disabled = true; btn.textContent = '↻ RECALCULANDO…'; }
    simListStatus.className = 'live-status warn';
    simListStatus.textContent = `Re-precalculando ${s.equipo_a} vs ${s.equipo_b}… no cierres la pestaña.`;
    try {
        const res = await predictFetch('/api/precalcular', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                equipo_a: s.equipo_a,
                equipo_b: s.equipo_b,
                match_id: s.match_id || 0,
                n_sim: s.n_sim || 10000,
                forzar: true,
            }),
        });
        const data = await res.json();
        if (!data.ok) {
            simListStatus.className = 'live-status err';
            simListStatus.textContent = `Error: ${data.error || 'no se pudo re-precalcular'}`;
            return;
        }
        // Async (202): poll del job; sync: ya terminó.
        if (data.job_id) {
            const ok = await pollPrecalcular(data.job_id, s);
            if (!ok) return;
        }
        simListStatus.className = 'live-status ok';
        simListStatus.textContent = '✓ Re-precalculo listo.';
        await loadModeloVersion();
        await loadSimulaciones();
        if (current && sameSim(current, s)) await selectSim(current);
    } catch (e) {
        simListStatus.className = 'live-status err';
        simListStatus.textContent = `Servicio de predicción no disponible: ${e.message}`;
    } finally {
        recalculating = false;
    }
}

// Poll simple del job de precalculo (mismo contrato que PREPARAR).
async function pollPrecalcular(jobId, s) {
    let fails = 0;
    while (true) {
        let r, jd;
        try {
            r = await predictFetch(`/api/precalcular/estado?job_id=${encodeURIComponent(jobId)}`);
            if (r.status === 404) {
                simListStatus.className = 'live-status err';
                simListStatus.textContent = 'El servicio perdió el job. Reintenta RE-PRECALCULAR.';
                return false;
            }
            jd = await r.json();
            fails = 0;
        } catch {
            fails++;
            simListStatus.className = 'live-status warn';
            simListStatus.textContent = `⚠ Sin conexión (reintento #${fails})… puedes dejarlo abierto.`;
            await new Promise(r2 => setTimeout(r2, Math.min(2000 + fails * 500, 10000)));
            continue;
        }
        const job = jd && jd.job;
        if (!jd || !jd.ok || !job) {
            simListStatus.className = 'live-status err';
            simListStatus.textContent = `Error: ${(jd && jd.error) || 'job no encontrado'}`;
            return false;
        }
        const pctProg = Math.round((job.progreso || 0) * 100);
        simListStatus.className = 'live-status warn';
        simListStatus.textContent = `↻ ${pctProg}% · mapa ${job.mapas_hechos || 0}/${Math.ceil((job.total || 26) / 2)} · no cierres la pestaña.`;
        if (job.estado === 'listo') return true;
        if (job.estado === 'error') {
            simListStatus.className = 'live-status err';
            simListStatus.textContent = `Error: ${job.error || 'no se pudo re-precalcular'}`;
            return false;
        }
        await new Promise(r2 => setTimeout(r2, 2000));
    }
}

async function loadLiveBulkForCurrent() {
    if (!current) return;
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
        liveBulk = {};
        if (data.ok && Array.isArray(data.predicciones)) {
            data.predicciones.forEach(p => {
                if (p && p.map_name) liveBulk[`${p.map_name}|${p.lado_inicial_a}`] = p;
            });
        }
        // `data.modelo_version` es la versión VIGENTE del servicio, no la de las
        // filas. La versión real de la cache de este enfrentamiento está en cada
        // fila (`p.modelo_version`), y es la que determina la vigencia.
        if (data.modelo_version) serviceModelVersion = data.modelo_version;
        const filas = Object.values(liveBulk);
        const fila = filas.find(p => p && p.modelo_version);
        if (fila) current.modelo_version = fila.modelo_version;
    } catch {
        liveBulk = {};
    }
}

// ─── PANEL MAPA / BANDO (lee de liveBulk, no llama al servicio en cada clic) ──
function renderLiveMapPicker() {
    if (!availableMaps.length) {
        liveMapPicker.innerHTML = '<div class="live-hint" style="padding:12px">Cargando mapas...</div>';
        return;
    }
    if (!liveMap || !availableMaps.includes(liveMap)) liveMap = availableMaps[0];

    liveMapPicker.innerHTML = '';
    availableMaps.forEach(m => {
        const row = liveBulk ? liveBulk[`${m}|${liveSide}`] : null;
        const am = row && row.analisis_mapa ? row.analisis_mapa : null;
        const paMap = (am && am.p_mapa_a != null) ? Number(am.p_mapa_a) : null;
        const ot = row ? row.prob_overtime : null;
        const meta = (paMap != null && !isNaN(paMap))
            ? `<span class="mqp-prob">${pct(paMap)}%</span><span class="mqp-ot">OT ${pct(ot)}%</span>`
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

    const catCols = datos => CAT_ECO.map(([key, label]) => {
        const d = (datos && datos[key]) || {};
        const pv = Number(d.p_gana_ronda);
        const ancho = isNaN(pv) ? 0 : Math.round(pv * 100);
        return `<div class="eco-cat">
            <span class="eco-cat-label">${label}</span>
            <span class="eco-cat-bar"><span style="width:${ancho}%"></span></span>
            <span class="eco-cat-val">${pctOr(pv)}</span>
        </div>`;
    }).join('');

    const destacados = new Set(['semi_buy_vs_full_buy', 'eco_vs_full_buy']);
    const colsB = CAT_ECO.map(([, l]) => `<span class="eco-mcol">${l}</span>`).join('');
    const filasM = CAT_ECO.map(([ra, la]) => {
        const celdas = CAT_ECO.map(([cb]) => {
            const d = (eco.cruce || {})[`${ra}_vs_${cb}`] || {};
            const pv = Number(d.p_gana_a);
            const txt = isNaN(pv) ? '—' : `${Math.round(pv * 100)}%`;
            const tono = isNaN(pv) ? '' : (pv >= 0.5 ? 'eco-hi' : 'eco-lo');
            const dest = destacados.has(`${ra}_vs_${cb}`) ? ' eco-dest' : '';
            return `<span class="eco-cell ${tono}${dest}">${txt}</span>`;
        }).join('');
        return `<div class="eco-mrow"><span class="eco-mrow-label">${la}</span>${celdas}</div>`;
    }).join('');

    const pistol = eco.pistol || {};
    const pvPis = Number(pistol.p_gana_a);
    const anchoPis = isNaN(pvPis) ? 0 : Math.round(pvPis * 100);

    liveEconomia.className = 'economia-block has-data';
    liveEconomia.innerHTML = `
    <div class="eco-head">
      <div class="eco-title">ECONOMÍA / RONDAS
        <span class="eco-note">estimación condicionada a la P del mapa · no resultado seguro</span>
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
    <div class="eco-pistol">
      <span class="eco-cat-label">PISTOL</span>
      <span class="eco-cat-bar"><span style="width:${anchoPis}%"></span></span>
      <span class="eco-cat-val">${escapeHtml(eqA)} ${pctOr(pvPis)}</span>
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

function paintLiveDetail(p, modelVersion, vigente) {
    liveDetailTitle.textContent = `${(liveMap || '').toUpperCase()} · ${liveSide === 'attack' ? 'ATK' : 'DEF'}`;
    const conf = confBand(p);
    const am = p && p.analisis_mapa ? p.analisis_mapa : null;
    const pMapA = (am && am.p_mapa_a != null) ? Number(am.p_mapa_a) : Number(p.prob_victoria_a);
    const wrA = (am && am.equipo_a) ? am.equipo_a : null;
    const wrB = (am && am.equipo_b) ? am.equipo_b : null;
    renderScoreboard(p);
    renderEconomia(p);
    liveCards.innerHTML = `
    <div class="live-card">
      <div class="live-card-label" style="color:var(--accent)">${escapeHtml(current.equipo_a)} · ESTE MAPA</div>
      <div class="live-card-val live-a">${pct(pMapA)}%</div>
    </div>
    <div class="live-card">
      <div class="live-card-label" style="color:var(--blue)">${escapeHtml(current.equipo_b)} · ESTE MAPA</div>
      <div class="live-card-val live-b">${pct(1 - pMapA)}%</div>
    </div>
    <div class="live-card">
      <div class="live-card-label">OVERTIME</div>
      <div class="live-card-val live-ot">${pct(p.prob_overtime)}%</div>
    </div>
    <div class="live-card">
      <div class="live-card-label">CONFIANZA (MOTOR)</div>
      <div class="live-card-val">${confBadge(conf) || '<span class="live-card-val">—</span>'}</div>
    </div>
    <div class="live-card">
      <div class="live-card-label">MUESTRAS</div>
      <div class="live-card-val">${p.n_sim ? Number(p.n_sim).toLocaleString() : '—'}</div>
    </div>
    <div class="analisis-nota">
      <b>Análisis por mapa</b> (histórico) · motor: <b>${pct(p.prob_victoria_a)}%</b> (igual en todos los mapas) ·
      historial en <b>${(liveMap || '').toUpperCase()}</b>:
      ${escapeHtml(current.equipo_a)} ${wrA ? pct(wrA.winrate) + '% <span style="color:var(--dim)">(n=' + wrA.n + ')</span>' : '—'} ·
      ${escapeHtml(current.equipo_b)} ${wrB ? pct(wrB.winrate) + '% <span style="color:var(--dim)">(n=' + wrB.n + ')</span>' : '—'}
    </div>`;
    const stale = vigente === false || current.vigente === false;
    liveStatus.className = 'live-status ' + (stale ? 'warn' : 'ok');
    liveStatus.textContent = stale
        ? '⚠ Predicciones desactualizadas; usa RE-PRECALCULAR.'
        : `✓ desde caché${conf ? ' · confianza ' + conf : ''} · modelo ${modelVersion || current.modelo_version || '—'}`;
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

// Marca la serie como "hay que recalcular" (cambió algo).
function marcarSeriePendiente() {
    seriesBanner.innerHTML = '';
    serieNote.textContent = matchMaps.length
        ? 'Cambió la serie · pulsa ARMAR SERIE.'
        : 'Elige los mapas y pulsa ARMAR SERIE.';
}

function syncSerieBuilder() {
    if (!serieSlots) return;
    serieSlots.innerHTML = '';
    if (!current) return;
    if (mapsLoading) {
        serieSlots.innerHTML = '<div style="padding:20px;text-align:center;font-size:10px;color:var(--dim);letter-spacing:2px">⏳ CARGANDO MAPAS...</div>';
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
        const paMap = (am && am.p_mapa_a != null) ? Number(am.p_mapa_a) : null;
        const pred = (paMap != null && !isNaN(paMap))
            ? `<div class="qi-pred">
                 <span class="qi-pred-a">${pct(paMap)}%</span>
                 <span class="qi-pred-b">${pct(1 - paMap)}%</span>
                 <span class="qi-pred-ot">OT ${pct(row.prob_overtime)}%</span>
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
    mbFormat.textContent = `${n > 0 ? inferFormat(n) : '—'} · ${n}/${maxMapsSel}`;
}

async function updateSerie() {
    if (!current || !matchMaps.length) {
        seriesBanner.innerHTML = '';
        serieNote.textContent = current ? 'Toca los mapas para armar la serie.' : '';
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
        const conf = confBand(m);
        const mp = m.marcador_mas_probable || (Array.isArray(m.marcadores) && m.marcadores.length ? m.marcadores[0] : null);
        const marcadorTxt = mp ? `<b>${mp.marcador_a}-${mp.marcador_b}</b> (${((Number(mp.prob) || 0) * 100).toFixed(1)}%)` : '';
        return `
    <div class="serie-map-row">
      <span class="smr-name">${m.map_name.toUpperCase()}</span>
      <span class="smr-side">${m.lado_inicial_a === 'attack' ? 'ATK' : 'DEF'}</span>
      <span class="smr-a">${pct(m.prob_victoria_a)}%</span>
      <span class="smr-b">${pct(m.prob_victoria_b)}%</span>
      <span class="smr-ot">OT ${pct(m.prob_overtime)}%</span>
      <span class="smr-marcador">${marcadorTxt}</span>
      <span class="smr-conf ${conf || ''}">${conf ? `<span class="conf-dot"></span>${conf}` : ''}</span>
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

    seriesBanner.innerHTML = `
    <div class="sb-team ${favA}">
      <div class="sb-name">EQUIPO A</div>
      <div class="sb-abbrev">${escapeHtml(current.equipo_a)}</div>
      <div class="sb-pct">${pct(pa)}%</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
    <div class="sb-center">
      <div class="sb-format">${(data.formato || '').toUpperCase()}</div>
      <div class="sb-sims">${(data.n_sim || current.n_sim || 0).toLocaleString()}<br>SIMULACIONES</div>
      <div style="font-size:10px;color:var(--dim);letter-spacing:1px;margin-top:4px">GANAR ${data.mapas_para_ganar}</div>
      ${data.confianza_serie ? `<div class="sb-conf ${String(data.confianza_serie).toLowerCase()}"><span class="conf-dot"></span>CONFIANZA ${escapeHtml(String(data.confianza_serie).toUpperCase())}</div>` : ''}
      <div style="font-size:9px;color:var(--dim);letter-spacing:1px;margin-top:4px">${mid}</div>
    </div>
    <div class="sb-team ${favB}" style="text-align:right;align-items:flex-end">
      <div class="sb-name">EQUIPO B</div>
      <div class="sb-abbrev">${escapeHtml(current.equipo_b)}</div>
      <div class="sb-pct">${pct(pb)}%</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
    ${distBlock}
    ${caminosBlock}
    ${mapRows ? `<div class="serie-maps">${mapRows}</div>` : ''}`;

    serieNote.innerHTML = `<strong>Serie desde caché</strong> (sin Monte Carlo). Formato <strong>${(data.formato || '').toUpperCase()}</strong> — necesario ganar <strong>${data.mapas_para_ganar}</strong> mapa(s).`;
}

// ─── INFORME PARA EL LLM (prompt + todos los datos + notas) ─────────────────
const PROMPT_ANALISTA = `Eres un analista de Valorant. Recibes el JSON de abajo con las predicciones y el análisis de un enfrentamiento:
- P del modelo por mapa (IGUAL en todos: es el motor de rating), P de overtime y confianza.
- Winrate histórico de cada equipo EN ESE MAPA (con su n) y una P analítica por mapa (difiere por mapa).
- Marcador más probable (distribución de marcadores) y economía/rondas por categoría y por cruce de compra.
- Distribución y caminos de la serie.

REGLAS ESTRICTAS:
- Razona SOLO con los números del JSON. NO inventes cambios de roster, parches ni contexto externo; si falta un dato, dilo. Si en NOTAS hay contexto, úsalo.
- Usa n (muestra) para juzgar fiabilidad: con n<10 no afirmes nada fuerte.
- Identifica: (a) mapas "coinflip" (p≈50% o n bajo); (b) mapas con ventaja real (brecha + n decente); (c) el mapa más propenso a upset; (d) dónde tu lectura difiere del modelo.
- Salida BREVE (≤150 palabras): 1 línea por mapa y 1 línea de serie. Cita n.
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

function descargarAnalisis() {
    if (!current || !liveBulk) {
        window.alert('Selecciona un enfrentamiento preparado primero.');
        return;
    }
    const filas = Object.values(liveBulk).filter(Boolean);
    const mapas = filas.map(p => ({
        map: p.map_name,
        lado: p.lado_inicial_a,
        model_p_a: p.prob_victoria_a,
        model_p_b: p.prob_victoria_b,
        confianza: confBand(p),
        ot: p.prob_overtime,
        analisis_mapa: p.analisis_mapa || null,
        marcador_top5: (p.marcadores || []).slice(0, 5),
        economia: p.economia || null,
        n_sim: p.n_sim,
    })).sort((a, b) => String(a.map).localeCompare(String(b.map))
        || String(a.lado).localeCompare(String(b.lado)));

    const payload = {
        equipo_a: current.equipo_a,
        equipo_b: current.equipo_b,
        match_id: current.match_id || 0,
        modelo_version: current.modelo_version || serviceModelVersion || null,
        resumen_serie: ultimaSerie ? {
            formato: ultimaSerie.formato,
            mapa_seleccionados: (ultimaSerie.mapas || []).map(m => `${m.map_name}|${m.lado_inicial_a}`),
            prob_serie_a: ultimaSerie.prob_serie_a,
            prob_serie_b: ultimaSerie.prob_serie_b,
            confianza_serie: ultimaSerie.confianza_serie,
            resultados_serie: ultimaSerie.resultados_serie,
            caminos_serie: ultimaSerie.caminos_serie,
        } : null,
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

## 2) DATOS (JSON)

\`\`\`json
${JSON.stringify(payload, null, 2)}
\`\`\`

## 3) NOTAS DE CONTEXTO (rellenar si aplica)

${notas}
`;

    _descargarArchivo(
        `aletheia_${_slug(current.equipo_a)}_vs_${_slug(current.equipo_b)}_${current.match_id || 0}.md`,
        informe
    );
    serieNote.textContent = '✓ Informe descargado (prompt + datos + notas).';
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

// ─── SCORECARD (micro-eventos predichos vs reales) ──────────────────────────
async function fetchScorecard() {
    if (!current || !scorecardWrap) return;
    scorecardWrap.innerHTML = '';
    const params = new URLSearchParams();
    if (current.match_id > 0) params.set('match_id', current.match_id);
    else { params.set('equipo_a', current.equipo_a); params.set('equipo_b', current.equipo_b); }
    try {
        const res = await proxyFetch(`/scorecard?${params.toString()}`);
        const data = await res.json();
        if (!data.ok || !data.resumen || !data.resumen.n_mapas) return;
        renderScorecard(data);
    } catch { }
}

function renderScorecard(data) {
    const r = data.resumen || {};
    const cards = [
        { label: 'N MAPAS', val: r.n_mapas },
        { label: 'MAP ACCURACY', val: fmtRatio(r.map_accuracy) },
        { label: 'MAP BRIER', val: fmtNum(r.map_brier) },
        { label: 'OT BRIER', val: fmtNum(r.ot_brier) },
        { label: 'MARCADOR TOP-1', val: fmtRatio(r.scoreline_top1_hit) },
        { label: 'ECO MAE', val: r.eco_mae == null ? '—' : `${fmtNum(r.eco_mae)} (n=${r.eco_n})` },
        { label: 'CRUCE MAE', val: r.cruce_mae == null ? '—' : `${fmtNum(r.cruce_mae)} (n=${r.cruce_n})` },
    ];
    const media = arr => (arr && arr.length ? arr.reduce((a, x) => a + x.erro, 0) / arr.length : null);
    const rows = (data.detalle || []).map(m => {
        const ecoErr = media(m.economia);
        const cruceErr = media(m.cruces);
        const otOk = (m.ot_pred >= 0.5 ? 1 : 0) === m.ot_real ? '✓' : '✕';
        return `<tr>
            <td>${escapeHtml(m.map_name)}</td>
            <td>${m.score_a}-${m.score_b}</td>
            <td>${pct(m.prob_victoria_a)}%</td>
            <td>${m.gano_a ? 'A' : 'B'}</td>
            <td>${pct(m.ot_pred)}% / ${m.ot_real ? 'sí' : 'no'} ${otOk}</td>
            <td>${pct(m.scoreline_prob)}%</td>
            <td>${ecoErr == null ? '—' : ecoErr.toFixed(3)}</td>
            <td>${cruceErr == null ? '—' : cruceErr.toFixed(3)}</td>
        </tr>`;
    }).join('');
    scorecardWrap.innerHTML = `
    <div class="scorecard-title">SCORECARD · MICRO-EVENTOS
      <span class="eco-note">predicho vs real · MAE menor = mejor (0-1)</span></div>
    <div class="cmp-summary scorecard-cards">${cards.map(c =>
        `<div class="cmp-card"><div class="cmp-card-label">${c.label}</div><div class="cmp-card-val">${c.val == null ? '—' : c.val}</div></div>`).join('')}</div>
    <div class="cmp-table-wrap"><table class="cmp-table">
        <thead><tr><th>MAPA</th><th>MARCADOR</th><th>P(A)</th><th>GANÓ</th><th>OT (PRED/REAL)</th><th>P(MARCADOR REAL)</th><th>ECO MAE</th><th>CRUCE MAE</th></tr></thead>
        <tbody>${rows}</tbody>
    </table></div>`;
}

// ─── SCORECARD AGREGADO (todos los partidos) + DATASET ──────────────────────
async function fetchScorecardAgregado() {
    if (!scorecardAgregadoWrap) return;
    scorecardAgregadoWrap.innerHTML = '';
    if (cmpToolsStatus) { cmpToolsStatus.className = 'live-status warn'; cmpToolsStatus.textContent = 'Calculando agregado (puede tardar)...'; }
    try {
        const res = await proxyFetch('/scorecard_agregado');
        const data = await res.json();
        if (!data.ok || !data.resumen || !data.resumen.n_mapas) {
            if (cmpToolsStatus) { cmpToolsStatus.className = 'live-status warn'; cmpToolsStatus.textContent = 'Sin partidos jugados con predicción.'; }
            return;
        }
        renderScorecardAgregado(data);
        if (cmpToolsStatus) { cmpToolsStatus.className = 'live-status ok'; cmpToolsStatus.textContent = `✓ ${data.resumen.n_partidos} partidos / ${data.resumen.n_mapas} mapas`; }
    } catch (e) {
        if (cmpToolsStatus) { cmpToolsStatus.className = 'live-status err'; cmpToolsStatus.textContent = `Error: ${e.message}`; }
    }
}

function renderScorecardAgregado(data) {
    const r = data.resumen || {};
    const cards = [
        { label: 'PARTIDOS', val: r.n_partidos },
        { label: 'MAPAS', val: r.n_mapas },
        { label: 'MAP ACCURACY', val: fmtRatio(r.map_accuracy) },
        { label: 'MAP BRIER', val: fmtNum(r.map_brier) },
        { label: 'OT BRIER', val: fmtNum(r.ot_brier) },
        { label: 'ECO MAE', val: r.eco_mae == null ? '—' : `${fmtNum(r.eco_mae)} (n=${r.eco_n})` },
        { label: 'CRUCE MAE', val: r.cruce_mae == null ? '—' : `${fmtNum(r.cruce_mae)} (n=${r.cruce_n})` },
    ];
    const tabla = (titulo, filas) => `
    <div class="scorecard-subtitle">${titulo}</div>
    <div class="cmp-table-wrap"><table class="cmp-table">
      <thead><tr><th>GRUPO</th><th>N</th><th>PRED MEDIA</th><th>REAL MEDIA</th><th>MAE</th></tr></thead>
      <tbody>${(filas || []).map(f => `<tr>
        <td>${escapeHtml(f.grupo)}</td><td>${f.n}</td>
        <td>${pct(f.pred_media)}%</td><td>${pct(f.real_media)}%</td>
        <td>${(Number(f.mae) * 100).toFixed(1)}%</td></tr>`).join('')}</tbody>
    </table></div>`;
    scorecardAgregadoWrap.innerHTML = `
    <div class="scorecard-title">SCORECARD AGREGADO
      <span class="eco-note">predicho vs real, sumando partidos · MAE menor = mejor</span></div>
    <div class="cmp-summary scorecard-cards">${cards.map(c =>
        `<div class="cmp-card"><div class="cmp-card-label">${c.label}</div><div class="cmp-card-val">${c.val == null ? '—' : c.val}</div></div>`).join('')}</div>
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

// ─── INIT ─────────────────────────────────────────────────────────────────────
loadAvailableMaps();
loadModeloVersion().then(loadSimulaciones);
