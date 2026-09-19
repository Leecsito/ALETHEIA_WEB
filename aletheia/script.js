const API = `${window.location.origin}/api`;

// ALETHEIA_PREDICT corre en el PC del usuario y se expone con ngrok.
// Las predicciones se piden DIRECTO a este servicio (no via el proxy de la
// web) para que las corridas largas (25K/50K) no las corte el timeout de
// gunicorn/Render. Equipos y mapas sí van por el proxy (son rápidos).
const PREDICT_DIRECTO = 'https://snugly-encore-sweep.ngrok-free.dev';
const NGROK_HEADER = { 'ngrok-skip-browser-warning': '1' };

let teams = [];
let selectedA = null;
let selectedB = null;
let nSim = 10000;
const TEAM_ABBREV_CACHE = {};
let availableMaps = [];    // mapas ofrecidos por el servicio ALETHEIA_PREDICT
let mapsLoading = false;   // true mientras carga la lista de mapas
let matchMaps = [];        // [{ map_name, lado_inicial_a }]

// ─── ESTADO DEL CICLO PREPARAR → LEER → ASOCIAR → COMPARAR ────────────────────
let matchId = 0;                 // id de vlr.gg parseado del input
let preparedMatchId = 0;         // id usado en el último PREPARAR (para desde_match_id)
let preparedModelVersion = null; // hash del modelo con el que se preparó
let serviceModelVersion = null;  // hash del modelo vigente en el servicio
let liveSide = 'attack';         // lado inicial de A en el panel en vivo
let liveMap = null;              // mapa seleccionado en el panel en vivo
let liveBulk = null;             // cache de todas las filas del match: 'Split|attack' -> fila
let prepareBusy = false;
let cmpVisible = false;          // true si la pestaña comparación está activa

// ─── DOM ──────────────────────────────────────────────────────────────────────
const gridA = document.getElementById('teamGridA');
const gridB = document.getElementById('teamGridB');
const selA = document.getElementById('selectedA');
const selB = document.getElementById('selectedB');
const searchA = document.getElementById('searchA');
const searchB = document.getElementById('searchB');
const simProgress = document.getElementById('simProgress');
const progressBar = document.getElementById('simProgressBar');
const progressLbl = document.getElementById('simProgressLabel');
const simParticles = document.getElementById('simParticles');
const matchBuilder = document.getElementById('matchBuilder');
const partidoResults = document.getElementById('partidoResults');
const mapSlots = document.getElementById('mapSlots');
const btnAddMap = document.getElementById('btnAddMap');
const btnSimPart = document.getElementById('btnSimPartido');
const bspCount = document.getElementById('bspCount');
const mbFormat = document.getElementById('mbFormat');
const hintTeamA = document.getElementById('hintTeamA');

const matchIdInput = document.getElementById('matchIdInput');
const matchIdBadge = document.getElementById('matchIdBadge');
const modelBadge = document.getElementById('modelBadge');
const btnAsociar = document.getElementById('btnAsociar');
const btnPreparar = document.getElementById('btnPreparar');
const prepareTimer = document.getElementById('prepareTimer');
const prepareStatus = document.getElementById('prepareStatus');

const liveSection = document.getElementById('liveSection');
const panelVivo = document.getElementById('panelVivo');
const panelComparacion = document.getElementById('panelComparacion');
const liveMapPicker = document.getElementById('liveMapPicker');
const liveDetailTitle = document.getElementById('liveDetailTitle');
const liveTeamALabel = document.getElementById('liveTeamALabel');
const liveSideAtk = document.getElementById('liveSideAtk');
const liveSideDef = document.getElementById('liveSideDef');
const liveCards = document.getElementById('liveCards');
const liveStatus = document.getElementById('liveStatus');
const cmpSummary = document.getElementById('cmpSummary');
const cmpTableWrap = document.getElementById('cmpTableWrap');
const cmpStatus = document.getElementById('cmpStatus');

// ─── PARTÍCULAS LOADING ───────────────────────────────────────────────────────
for (let i = 0; i < 5; i++) {
    const p = document.createElement('div');
    p.className = 'particle';
    simParticles.appendChild(p);
}

// ─── HELPERS ──────────────────────────────────────────────────────────────────
const pct = v => Math.round((v || 0) * 100);

// fetch directo al servicio (siempre con el header de ngrok)
function predictFetch(path, options = {}) {
    const headers = Object.assign({}, NGROK_HEADER, options.headers || {});
    return fetch(`${PREDICT_DIRECTO}${path}`, Object.assign({}, options, { headers }));
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
        card.innerHTML = `
      <div class="tc-abbrev">${t.abbrev || t.name}</div>
    `;
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
    updateHintTeam();
    if (selectedA && selectedB) {
        availableMaps = [];
        mapsLoading = true;
    }
    showBuilder();
    if (selectedA && selectedB) {
        loadAvailableMaps();
    }
}

function showBuilder() {
    const ready = !!(selectedA && selectedB);
    matchBuilder.style.display = ready ? 'block' : 'none';
    liveSection.style.display = ready ? 'block' : 'none';
    if (ready) {
        updateLiveTeamLabel();
        loadLiveBulk();
    }
}

async function loadAvailableMaps() {
    if (!selectedA || !selectedB) return;
    mapsLoading = true;
    syncMatchBuilder();
    try {
        const res = await fetch(`${API}/aletheia/mapas`);
        const data = await res.json();
        if (data.ok) availableMaps = data.mapas || [];
    } catch { }
    mapsLoading = false;
    syncMatchBuilder();
    loadLiveBulk();
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

// ─── FORMATO (inferido igual que el servicio: 1→Bo1, 2-3→Bo3, 4-5→Bo5) ───────
function inferFormat(n) {
    if (n <= 0) return '—';
    if (n === 1) return 'Bo1';
    if (n <= 3) return 'Bo3';
    if (n <= 5) return 'Bo5';
    return `Bo${n}`;
}

// ─── MATCH BUILDER ────────────────────────────────────────────────────────────
function updateHintTeam() {
    if (selectedA) hintTeamA.textContent = TEAM_ABBREV_CACHE[selectedA] || selectedA;
}

function syncMatchBuilder() {
    mapSlots.innerHTML = '';

    if (mapsLoading) {
        mapSlots.innerHTML = '<div style="padding:20px;text-align:center;font-size:10px;color:var(--dim);letter-spacing:2px">⏳ CARGANDO MAPAS DISPONIBLES...</div>';
        btnSimPart.disabled = true;
        return;
    }

    const pickerDiv = document.createElement('div');
    pickerDiv.className = 'map-quick-picker';
    if (!availableMaps.length) {
        pickerDiv.innerHTML = '<div style="padding:12px;font-size:10px;color:var(--dim);letter-spacing:1px">No se pudieron cargar los mapas del servicio.</div>';
    }
    availableMaps.forEach(m => {
        const used = matchMaps.some(mm => mm.map_name === m);
        const full = matchMaps.length >= 5;
        const tile = document.createElement('button');
        tile.className = `mqp-tile${used ? ' mqp-used' : ''}${(!used && full) ? ' mqp-full' : ''}`;
        tile.innerHTML = `
      <img class="mqp-img" src="../multimedia/maps/${m.toUpperCase()}.avif" alt="${m}" onerror="this.style.display='none'">
      <span class="mqp-name">${m.toUpperCase()}</span>`;
        tile.disabled = used || full;
        if (!used && !full) tile.addEventListener('click', () => {
            matchMaps.push({ map_name: m, lado_inicial_a: 'attack' });
            syncMatchBuilder();
        });
        pickerDiv.appendChild(tile);
    });
    mapSlots.appendChild(pickerDiv);

    if (matchMaps.length > 0) {
        const queueDiv = document.createElement('div');
        queueDiv.className = 'map-queue';
        const abbrevA = selectedA ? (TEAM_ABBREV_CACHE[selectedA] || selectedA) : 'A';
        matchMaps.forEach((cfg, i) => {
            const isDecider = i === matchMaps.length - 1 && matchMaps.length >= 2;
            const item = document.createElement('div');
            item.className = `map-queue-item${isDecider ? ' qi-decider-row' : ''}`;
            item.innerHTML = `
        <div class="qi-left">
          <span class="qi-num">0${i + 1}</span>
          <img class="qi-map-img" src="../multimedia/maps/${cfg.map_name.toUpperCase()}.avif" onerror="this.style.display='none'">
          <span class="qi-mapname">${cfg.map_name.toUpperCase()}</span>
          ${isDecider ? '<span class="qi-decider-badge">DECIDER</span>' : ''}
        </div>
        <div class="qi-side-group">
          <span class="qi-side-label">${abbrevA} empieza:</span>
          <button class="qi-side-btn${cfg.lado_inicial_a === 'attack' ? ' qi-atk-active' : ''}" data-idx="${i}" data-side="atk">⚔ ATK</button>
          <button class="qi-side-btn${cfg.lado_inicial_a === 'defense' ? ' qi-def-active' : ''}" data-idx="${i}" data-side="def">🛡 DEF</button>
        </div>
        <button class="qi-remove" data-idx="${i}" title="Quitar">✕</button>`;
            queueDiv.appendChild(item);
        });
        mapSlots.appendChild(queueDiv);
    }

    mapSlots.querySelectorAll('.qi-side-btn').forEach(btn => {
        btn.addEventListener('click', e => {
            const idx = parseInt(e.currentTarget.dataset.idx);
            matchMaps[idx].lado_inicial_a = (e.currentTarget.dataset.side === 'atk') ? 'attack' : 'defense';
            syncMatchBuilder();
        });
    });
    mapSlots.querySelectorAll('.qi-remove').forEach(btn => {
        btn.addEventListener('click', e => {
            matchMaps.splice(parseInt(e.currentTarget.dataset.idx), 1);
            syncMatchBuilder();
        });
    });
    updateBuilderState();
}

function updateBuilderState() {
    const n = matchMaps.length;
    mbFormat.textContent = n > 0 ? inferFormat(n) : '—';
    bspCount.textContent = `${n} mapa${n !== 1 ? 's' : ''}`;
    const ready = n > 0;
    btnSimPart.classList.toggle('ready', ready);
    btnSimPart.disabled = !ready;
    updateActionState();
}

// Habilita/deshabilita PREPARAR y ASOCIAR según equipos + id.
function updateActionState() {
    const ready = !!(selectedA && selectedB);
    btnPreparar.disabled = !ready || prepareBusy;
    updateAssociarState();
}

function updateAssociarState() {
    btnAsociar.disabled = !(matchId > 0 && selectedA && selectedB) || prepareBusy;
}

btnAddMap.addEventListener('click', () => { matchMaps = []; syncMatchBuilder(); });

// ─── ID DE PARTIDO (vlr.gg) ───────────────────────────────────────────────────
matchIdInput.addEventListener('input', () => {
    matchId = parseMatchId(matchIdInput.value);
    matchIdBadge.textContent = matchId > 0 ? `PARTIDO #${matchId}` : 'SIN ID';
    matchIdBadge.classList.toggle('has-id', matchId > 0);
    updateAssociarState();
});

// Al confirmar el id (blur/enter) refrescar las vistas que dependen de él.
matchIdInput.addEventListener('change', () => {
    loadLiveBulk();
    if (cmpVisible) fetchComparacion();
});

// ─── PANEL EN VIVO ────────────────────────────────────────────────────────────
function updateLiveTeamLabel() {
    liveTeamALabel.textContent = selectedA || '';
}

// Carga TODAS las filas cacheadas del partido y dibuja el selector visual de mapas.
async function loadLiveBulk() {
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
        liveBulk = {};
        if (data.ok && Array.isArray(data.predicciones)) {
            data.predicciones.forEach(p => {
                if (p && p.map_name) liveBulk[`${p.map_name}|${p.lado_inicial_a}`] = p;
            });
        }
    } catch {
        liveBulk = {};
    }
    renderLiveMapPicker();
    if (liveMap) fetchLive();
}

// Selector visual de mapas (mismo estilo de tiles que el armador de serie).
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
            fetchLive();
        });
        liveMapPicker.appendChild(tile);
    });
}

function setLiveSide(side) {
    liveSide = side;
    liveSideAtk.classList.toggle('qi-atk-active', side === 'attack');
    liveSideDef.classList.toggle('qi-def-active', side === 'defense');
    renderLiveMapPicker();
    fetchLive();
}
liveSideAtk.addEventListener('click', () => setLiveSide('attack'));
liveSideDef.addEventListener('click', () => setLiveSide('defense'));

async function fetchLive() {
    if (!selectedA || !selectedB || !liveMap) return;
    liveStatus.className = 'live-status';
    liveStatus.textContent = 'Consultando...';
    liveCards.innerHTML = '';

    const params = new URLSearchParams();
    if (matchId > 0) {
        params.set('match_id', matchId);
    } else {
        params.set('equipo_a', selectedA);
        params.set('equipo_b', selectedB);
    }
    params.set('map_name', liveMap);
    params.set('lado_inicial_a', liveSide);

    try {
        const res = await predictFetch(`/api/prediccion?${params.toString()}`);
        const data = await res.json();
        if (res.status === 404 || !data.ok) {
            liveStatus.className = 'live-status warn';
            liveStatus.textContent = 'Aún no precomputado (usa PREPARAR PARTIDO).';
            return;
        }
        renderLive(data);
    } catch (e) {
        liveStatus.className = 'live-status err';
        liveStatus.textContent = `Servicio de predicción no disponible: ${e.message}`;
    }
}

function renderLive(data) {
    const p = data.prediccion || {};
    const vigente = data.vigente !== false;
    const fuente = p.fuente || data.fuente || 'cache';

    liveDetailTitle.textContent = liveMap
        ? `${liveMap.toUpperCase()} · ${liveSide === 'attack' ? 'ATK' : 'DEF'}`
        : '';

    liveCards.innerHTML = `
    <div class="live-card">
      <div class="live-card-label" style="color:var(--accent)">${escapeHtml(p.equipo_a || selectedA)}</div>
      <div class="live-card-val live-a">${pct(p.prob_victoria_a)}%</div>
    </div>
    <div class="live-card">
      <div class="live-card-label" style="color:var(--blue)">${escapeHtml(p.equipo_b || selectedB)}</div>
      <div class="live-card-val live-b">${pct(p.prob_victoria_b)}%</div>
    </div>
    <div class="live-card">
      <div class="live-card-label">OVERTIME</div>
      <div class="live-card-val live-ot">${pct(p.prob_overtime)}%</div>
    </div>
    <div class="live-card">
      <div class="live-card-label">MUESTRAS</div>
      <div class="live-card-val">${p.n_sim ? Number(p.n_sim).toLocaleString() : '—'}</div>
    </div>
  `;

    if (!vigente) {
        liveStatus.className = 'live-status warn';
        liveStatus.textContent = `⚠ Predicciones desactualizadas (modelo ${data.modelo_version || '—'}); vuelve a PREPARAR PARTIDO.`;
    } else {
        const sinDatos = Number(p.con_datos) === 0;
        liveStatus.className = 'live-status ok';
        liveStatus.textContent = `✓ fuente: ${fuente} · vigente · modelo ${data.modelo_version || '—'}${sinDatos ? ' · sin datos históricos para este mapa' : ''}`;
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

// ─── PESTAÑAS ─────────────────────────────────────────────────────────────────
document.querySelectorAll('.live-tab').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.live-tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        const tab = btn.dataset.tab;
        cmpVisible = tab === 'comparacion';
        panelVivo.style.display = tab === 'vivo' ? 'block' : 'none';
        panelComparacion.style.display = tab === 'comparacion' ? 'block' : 'none';
        refreshModelVersion();
        if (cmpVisible) fetchComparacion();
        else fetchLive();
    });
});

// ─── PREPARAR PARTIDO (precalcular, DIRECTO y largo) ─────────────────────────
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
        loadLiveBulk();
        finish();
        return;
    }

    // Asíncrono: el POST devolvió {job_id, total}. Poll cada 2 s.
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
            loadLiveBulk();
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
        loadLiveBulk();
        if (cmpVisible) fetchComparacion();
    } catch (e) {
        prepareStatus.className = 'prepare-status err';
        prepareStatus.textContent = `Servicio de predicción no disponible: ${e.message}`;
    } finally {
        btnAsociar.classList.remove('running');
        updateAssociarState();
    }
}

// ─── SIMULAR PARTIDO (serie) ──────────────────────────────────────────────────
btnSimPart.addEventListener('click', runPartido);

async function runPartido() {
    if (!btnSimPart.classList.contains('ready') || btnSimPart.classList.contains('running')) return;
    btnSimPart.classList.remove('ready');
    btnSimPart.classList.add('running');
    btnSimPart.querySelector('.bsp-text').textContent = 'SIMULANDO...';

    simProgress.style.display = 'block';
    progressLbl.style.color = '';
    partidoResults.style.display = 'none';

    const labels = [
        'Enviando configuración al motor de predicción...',
        `Ejecutando ${nSim.toLocaleString()} simulaciones...`,
        'Calculando probabilidades por mapa...',
        'Resolviendo probabilidad de serie...',
        'Consolidando resultados...',
    ];
    let li = 0;
    progressLbl.textContent = labels[0];
    const lInterval = setInterval(() => {
        if (li < labels.length - 1) progressLbl.textContent = labels[++li];
    }, 700);

    try {
        const body = {
            equipo_a: selectedA,
            equipo_b: selectedB,
            mapas: matchMaps.map(m => ({ map_name: m.map_name, lado_inicial_a: m.lado_inicial_a })),
            n_sim: nSim,
        };
        if (matchId > 0) body.match_id = matchId;

        const res = await predictFetch('/api/predecir', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        clearInterval(lInterval);
        const data = await res.json();

        if (!data.ok) {
            progressLbl.textContent = `Error: ${data.error}`;
            progressLbl.style.color = 'var(--red)';
            return;
        }

        if (data.modelo_version) { serviceModelVersion = data.modelo_version; updateModelBadge(); }
        simProgress.style.display = 'none';
        renderPartidoResults(data);

    } catch (e) {
        clearInterval(lInterval);
        progressLbl.textContent = `Error de conexión: ${e.message}`;
        progressLbl.style.color = 'var(--red)';
    } finally {
        btnSimPart.classList.remove('running');
        btnSimPart.classList.add('ready');
        btnSimPart.querySelector('.bsp-text').textContent = 'SIMULAR PARTIDO';
    }
}

// ─── RENDER RESULTADOS (serie) ────────────────────────────────────────────────
function renderSeriesBanner(data) {
    const pa = data.prob_serie_a || 0;
    const pb = data.prob_serie_b || 0;
    const favA = pa * 100 >= 55 ? 'winner-side' : '';
    const favB = pb * 100 >= 55 ? 'winner-side-b' : '';

    document.getElementById('seriesBanner').innerHTML = `
    <div class="sb-team ${favA}">
      <div class="sb-name">EQUIPO A</div>
      <div class="sb-abbrev">${data.equipo_a}</div>
      <div class="sb-pct">${pct(pa)}%</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
    <div class="sb-center">
      <div class="sb-format">${(data.formato || '').toUpperCase()}</div>
      <div class="sb-sims">${(data.n_sim || nSim).toLocaleString()}<br>SIMULACIONES</div>
      <div style="font-size:10px;color:var(--dim);letter-spacing:1px;margin-top:4px">GANAR ${data.mapas_para_ganar}</div>
      ${data.match_id ? `<div style="font-size:9px;color:var(--dim);letter-spacing:1px;margin-top:4px">PARTIDO #${data.match_id}</div>` : ''}
    </div>
    <div class="sb-team ${favB}" style="text-align:right;align-items:flex-end">
      <div class="sb-name">EQUIPO B</div>
      <div class="sb-abbrev">${data.equipo_b}</div>
      <div class="sb-pct">${pct(pb)}%</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
  `;
}

function buildMapRowHtml(m, i, data) {
    const winA = (m.prob_victoria_a || 0) * 100;
    const barColor = winA >= 60 ? 'pm-bar-green' : winA >= 40 ? 'pm-bar-yellow' : 'pm-bar-red';
    const pctClass = winA >= 55 ? 'pm-pct-a' : winA <= 45 ? 'pm-pct-b' : 'pm-pct-even';
    const isAtk = m.lado_inicial_a === 'attack';
    const sideLabel = isAtk
        ? `<span class="pm-start-atk">⚔ ${data.equipo_a} EMPIEZA ATK</span>`
        : `<span class="pm-start-def">🛡 ${data.equipo_a} EMPIEZA DEF</span>`;
    const otPct = pct(m.prob_overtime);
    const otHtml = otPct > 0
        ? `<div class="pm-score-expect">
             <span class="pse-label">OVERTIME</span>
             <span class="pse-ot ${otPct >= 20 ? 'pse-ot-high' : ''}">OT ${otPct}%</span>
           </div>`
        : '';
    const fuente = m.fuente
        ? `<span class="pm-fuente">${m.fuente}</span>`
        : '';

    return `
    <div class="pm-num">0${i + 1}</div>
    <div class="pm-map-info">
      <div class="pm-map-name">${m.map_name.toUpperCase()} ${fuente}</div>
      <div class="pm-start-side">${sideLabel}</div>
      ${otHtml}
    </div>
    <div class="pm-prob-cell">
      <div class="pm-teams-row">
        <div class="pm-team-pct">
          <span class="pm-abbrev" style="color:var(--accent)">${data.equipo_a}</span>
          <span class="pm-pct-val ${pctClass}">${Math.round(winA)}%</span>
        </div>
        <div class="pm-team-pct" style="text-align:right">
          <span class="pm-abbrev" style="color:var(--blue)">${data.equipo_b}</span>
          <span class="pm-pct-val pm-pct-b">${pct(m.prob_victoria_b)}%</span>
        </div>
      </div>
      <div class="pm-bar-track"><div class="pm-bar-fill ${barColor}" style="width:${Math.round(winA)}%"></div></div>
    </div>`;
}

function renderPartidoResults(data) {
    renderSeriesBanner(data);

    document.getElementById('pmTeamAName').textContent = data.equipo_a;
    const list = document.getElementById('partidoMapsList');
    list.innerHTML = '';

    (data.mapas || []).forEach((m, i) => {
        const row = document.createElement('div');
        row.className = 'pm-row';
        row.style.animationDelay = `${i * 0.06}s`;
        row.innerHTML = buildMapRowHtml(m, i, data);
        list.appendChild(row);
    });

    document.getElementById('partidoMethodNote').innerHTML = `
    <strong>Motor:</strong> ALETHEIA_PREDICT (Glicko-2 + regresión logística + Monte Carlo).
    Formato <strong>${(data.formato || '').toUpperCase()}</strong> — necesario ganar
    <strong>${data.mapas_para_ganar}</strong> mapa(s) · ${(data.n_sim || nSim).toLocaleString()} simulaciones.
    El lado inicial de ${data.equipo_a} se define por mapa; la probabilidad de overtime se estima vía remontada
    de marcador/economía.
  `;

    partidoResults.style.display = 'block';
    partidoResults.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ─── INIT ─────────────────────────────────────────────────────────────────────
loadTeams();
refreshModelVersion();
