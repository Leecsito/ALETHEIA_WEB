const API = `${window.location.origin}/api`;

// EN VIVO nunca simula: todo sale de la caché (predicciones/serie/comparación).
// Las lecturas van por el proxy de la web (/api/aletheia/...), que a su vez
// contacta ALETHEIA_PREDICT (ALETHEIA_PREDICT_URL). Aquí no hay corridas largas.

let availableMaps = [];    // 13 mapas del servicio (proxy /api/aletheia/mapas)
let mapsLoading = false;
let sims = [];             // enfrentamientos ya preparados (/api/simulaciones)
let current = null;        // simulación seleccionada
let liveBulk = null;       // 'map|side' -> fila cacheada (se lee UNA vez)
let liveSide = 'attack';   // bando inicial de A
let liveMap = null;        // mapa seleccionado en el panel mapa/bando
let matchMaps = [];        // [{ map_name, lado_inicial_a }] para la serie
let maxMapsSel = 3;        // slots del formato (1/3/5)
let showStale = false;     // mostrar simulaciones no vigentes

// ─── DOM ──────────────────────────────────────────────────────────────────────
const simList = document.getElementById('simList');
const simListStatus = document.getElementById('simListStatus');
const btnRefreshSims = document.getElementById('btnRefreshSims');
const chkShowStale = document.getElementById('chkShowStale');

const liveSection = document.getElementById('liveSection');
const selSimHead = document.getElementById('selSimHead');
const panelMapa = document.getElementById('panelMapa');
const panelSerie = document.getElementById('panelSerie');

const liveMapPicker = document.getElementById('liveMapPicker');
const liveDetailTitle = document.getElementById('liveDetailTitle');
const liveTeamALabel = document.getElementById('liveTeamALabel');
const liveSideAtk = document.getElementById('liveSideAtk');
const liveSideDef = document.getElementById('liveSideDef');
const liveCards = document.getElementById('liveCards');
const liveStatus = document.getElementById('liveStatus');

const serieSlots = document.getElementById('serieSlots');
const seriesBanner = document.getElementById('seriesBanner');
const serieNote = document.getElementById('serieNote');
const mbFormat = document.getElementById('mbFormat');

const panelComparacion = document.getElementById('panelComparacion');
const cmpSummary = document.getElementById('cmpSummary');
const cmpTableWrap = document.getElementById('cmpTableWrap');
const cmpStatus = document.getElementById('cmpStatus');

// ─── HELPERS ──────────────────────────────────────────────────────────────────
const pct = v => Math.round((v || 0) * 100);

function proxyFetch(path, options = {}) {
    return fetch(`${API}/aletheia${path}`, options);
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
        const res = await proxyFetch('/api/simulaciones?limite=100');
        const data = await res.json();
        if (!data.ok) {
            simListStatus.className = 'live-status err';
            simListStatus.textContent = `Error: ${data.error || 'no se pudo leer /api/simulaciones'}`;
            return;
        }
        sims = data.simulaciones || [];
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
        const res = await proxyFetch('/api/asociar', {
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
        const res = await proxyFetch('/api/borrar', {
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

// ─── SELECCIÓN DE SIMULACIÓN ──────────────────────────────────────────────────
async function selectSim(s) {
    current = s;
    matchMaps = [];
    liveMap = null;
    liveBulk = null;
    liveSection.style.display = 'block';

    const vigente = s.vigente !== false;
    selSimHead.innerHTML = `
    <div class="ss-head-teams">${escapeHtml(s.equipo_a)} <span class="si-vs">vs</span> ${escapeHtml(s.equipo_b)}</div>
    <div class="ss-head-meta">${s.match_id ? 'PARTIDO #' + s.match_id : 'sin id'} · ${s.n_sim ? Number(s.n_sim).toLocaleString() + ' sims' : ''} · modelo ${s.modelo_version || '—'}${vigente ? '' : ' · <span style="color:var(--orange)">⚠ no vigente, re-preparar</span>'}</div>`;
    liveTeamALabel.textContent = s.equipo_a;

    await loadLiveBulkForCurrent();
    renderLiveMapPicker();
    renderLiveDetail();
    syncSerieBuilder();
    updateSerie();
    if (panelComparacion && panelComparacion.style.display !== 'none') fetchComparacion();
    renderSimList();
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
        const res = await proxyFetch(`/api/predicciones?${params.toString()}`);
        const data = await res.json();
        liveBulk = {};
        if (data.ok && Array.isArray(data.predicciones)) {
            data.predicciones.forEach(p => {
                if (p && p.map_name) liveBulk[`${p.map_name}|${p.lado_inicial_a}`] = p;
            });
        }
        if (data.modelo_version) current.modelo_version = data.modelo_version;
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
        const pa = row ? row.prob_victoria_a : null;
        const ot = row ? row.prob_overtime : null;
        const meta = pa != null
            ? `<span class="mqp-prob">${pct(pa)}%</span><span class="mqp-ot">OT ${pct(ot)}%</span>`
            : `<span class="mqp-prob">—</span>`;
        const tile = document.createElement('button');
        tile.className = 'mqp-tile' + (m === liveMap ? ' mqp-selected' : '');
        tile.innerHTML = `
      <img class="mqp-img" src="../multimedia/maps/${m.toUpperCase()}.avif" alt="${m}" onerror="this.style.display='none'">
      <span class="mqp-name">${m.toUpperCase()}</span>
      ${meta}`;
        tile.addEventListener('click', () => {
            liveMap = m;
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
        const res = await proxyFetch(`/api/prediccion?${params.toString()}`);
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

function paintLiveDetail(p, modelVersion, vigente) {
    liveDetailTitle.textContent = `${(liveMap || '').toUpperCase()} · ${liveSide === 'attack' ? 'ATK' : 'DEF'}`;
    liveCards.innerHTML = `
    <div class="live-card">
      <div class="live-card-label" style="color:var(--accent)">${escapeHtml(current.equipo_a)}</div>
      <div class="live-card-val live-a">${pct(p.prob_victoria_a)}%</div>
    </div>
    <div class="live-card">
      <div class="live-card-label" style="color:var(--blue)">${escapeHtml(current.equipo_b)}</div>
      <div class="live-card-val live-b">${pct(p.prob_victoria_b)}%</div>
    </div>
    <div class="live-card">
      <div class="live-card-label">OVERTIME</div>
      <div class="live-card-val live-ot">${pct(p.prob_overtime)}%</div>
    </div>
    <div class="live-card">
      <div class="live-card-label">MUESTRAS</div>
      <div class="live-card-val">${p.n_sim ? Number(p.n_sim).toLocaleString() : '—'}</div>
    </div>`;
    const stale = vigente === false || current.vigente === false;
    const conf = p.confianza ? ` · confianza ${p.confianza}` : '';
    liveStatus.className = 'live-status ' + (stale ? 'warn' : 'ok');
    liveStatus.textContent = stale
        ? '⚠ Predicciones desactualizadas; vuelve a PREPARAR PARTIDO.'
        : `✓ desde caché${conf} · modelo ${modelVersion || current.modelo_version || '—'}`;
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

// ─── SERIE (armador + POST /api/serie, instantáneo) ──────────────────────────
document.querySelectorAll('.fmt-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.fmt-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        maxMapsSel = parseInt(btn.dataset.fmt);
        if (matchMaps.length > maxMapsSel) matchMaps = matchMaps.slice(0, maxMapsSel);
        syncSerieBuilder();
        updateSerie();
    });
});

function syncSerieBuilder() {
    if (!serieSlots) return;
    serieSlots.innerHTML = '';
    if (!current) return;
    if (mapsLoading) {
        serieSlots.innerHTML = '<div style="padding:20px;text-align:center;font-size:10px;color:var(--dim);letter-spacing:2px">⏳ CARGANDO MAPAS...</div>';
        return;
    }

    const pickerDiv = document.createElement('div');
    pickerDiv.className = 'map-quick-picker';
    availableMaps.forEach(m => {
        const used = matchMaps.some(mm => mm.map_name === m);
        const full = matchMaps.length >= maxMapsSel;
        const tile = document.createElement('button');
        tile.className = `mqp-tile${used ? ' mqp-used' : ''}${(!used && full) ? ' mqp-full' : ''}`;
        tile.innerHTML = `
      <img class="mqp-img" src="../multimedia/maps/${m.toUpperCase()}.avif" alt="${m}" onerror="this.style.display='none'">
      <span class="mqp-name">${m.toUpperCase()}</span>`;
        tile.disabled = used || full;
        if (!used && !full) tile.addEventListener('click', () => {
            matchMaps.push({ map_name: m, lado_inicial_a: 'attack' });
            syncSerieBuilder();
            updateSerie();
        });
        pickerDiv.appendChild(tile);
    });
    serieSlots.appendChild(pickerDiv);

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
        const pred = row
            ? `<div class="qi-pred">
                 <span class="qi-pred-a">${pct(row.prob_victoria_a)}%</span>
                 <span class="qi-pred-b">${pct(row.prob_victoria_b)}%</span>
                 <span class="qi-pred-ot">OT ${pct(row.prob_overtime)}%</span>
               </div>`
            : `<span class="qi-pred-none">sin caché</span>`;

        item.className = `map-queue-item${isDecider ? ' qi-decider-row' : ''}`;
        item.innerHTML = `
        <div class="qi-left">
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

    serieSlots.querySelectorAll('.qi-side-btn').forEach(btn => {
        btn.addEventListener('click', e => {
            const idx = parseInt(e.currentTarget.dataset.idx);
            matchMaps[idx].lado_inicial_a = (e.currentTarget.dataset.side === 'atk') ? 'attack' : 'defense';
            syncSerieBuilder();
            updateSerie();
        });
    });
    serieSlots.querySelectorAll('.qi-remove').forEach(btn => {
        btn.addEventListener('click', e => {
            matchMaps.splice(parseInt(e.currentTarget.dataset.idx), 1);
            syncSerieBuilder();
            updateSerie();
        });
    });
    updateSerieState();
}

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
        const res = await proxyFetch('/api/serie', {
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

    const mapRows = (data.mapas || []).map(m => `
    <div class="serie-map-row">
      <span class="smr-name">${m.map_name.toUpperCase()}</span>
      <span class="smr-side">${m.lado_inicial_a === 'attack' ? 'ATK' : 'DEF'}</span>
      <span class="smr-a">${pct(m.prob_victoria_a)}%</span>
      <span class="smr-b">${pct(m.prob_victoria_b)}%</span>
      <span class="smr-ot">OT ${pct(m.prob_overtime)}%</span>
      <span class="smr-fuente">${m.fuente || 'cache'}${m.confianza ? ' · ' + m.confianza : ''}</span>
    </div>`).join('');

    seriesBanner.innerHTML = `
    <div class="sb-team ${favA}">
      <div class="sb-name">EQUIPO A</div>
      <div class="sb-abbrev">${escapeHtml(current.equipo_a)}</div>
      <div class="sb-pct">${pct(pa)}%</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
    <div class="sb-center">
      <div class="sb-format">${(data.formato || '').toUpperCase()}</div>
      <div class="sb-sims">${(current.n_sim || 0).toLocaleString()}<br>SIMULACIONES</div>
      <div style="font-size:10px;color:var(--dim);letter-spacing:1px;margin-top:4px">GANAR ${data.mapas_para_ganar}</div>
      ${data.confianza_serie ? `<div style="font-size:9px;color:var(--dim);letter-spacing:1px;margin-top:4px">CONFIANZA ${escapeHtml(String(data.confianza_serie).toUpperCase())}</div>` : ''}
      <div style="font-size:9px;color:var(--dim);letter-spacing:1px;margin-top:4px">${mid}</div>
    </div>
    <div class="sb-team ${favB}" style="text-align:right;align-items:flex-end">
      <div class="sb-name">EQUIPO B</div>
      <div class="sb-abbrev">${escapeHtml(current.equipo_b)}</div>
      <div class="sb-pct">${pct(pb)}%</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
    ${mapRows ? `<div class="serie-maps">${mapRows}</div>` : ''}`;

    serieNote.innerHTML = `<strong>Serie desde caché</strong> (sin Monte Carlo). Formato <strong>${(data.formato || '').toUpperCase()}</strong> — necesario ganar <strong>${data.mapas_para_ganar}</strong> mapa(s).`;
}

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
        const res = await proxyFetch(`/api/comparacion?${params.toString()}`);
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

// ─── PESTAÑAS ─────────────────────────────────────────────────────────────────
document.querySelectorAll('.live-tab').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.live-tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        const tab = btn.dataset.tab;
        panelMapa.style.display = tab === 'mapa' ? 'block' : 'none';
        panelSerie.style.display = tab === 'serie' ? 'block' : 'none';
        panelComparacion.style.display = tab === 'comparacion' ? 'block' : 'none';
        if (tab === 'mapa') {
            renderLiveMapPicker();
            renderLiveDetail();
        } else if (tab === 'serie') {
            syncSerieBuilder();
            updateSerie();
        } else {
            fetchComparacion();
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
loadSimulaciones();
