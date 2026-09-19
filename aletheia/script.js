const API = `${window.location.origin}/api`;

// ALETHEIA_PREDICT corre en el PC del usuario y se expone con ngrok.
// Las predicciones se piden DIRECTO a este servicio (no via el proxy de la
// web) para que las corridas largas (25K/50K) no las corte el timeout de
// gunicorn/Render. Equipos y mapas sí van por el proxy (son rápidos).
const PREDICT_DIRECTO = 'https://snugly-encore-sweep.ngrok-free.dev';

let teams = [];
let selectedA = null;
let selectedB = null;
let nSim = 10000;
const TEAM_ABBREV_CACHE = {};
let availableMaps = [];    // mapas ofrecidos por el servicio ALETHEIA_PREDICT
let mapsLoading = false;   // true mientras carga la lista de mapas
let matchMaps = [];        // [{ map_name, lado_inicial_a }]

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

// ─── PARTÍCULAS LOADING ───────────────────────────────────────────────────────
for (let i = 0; i < 5; i++) {
    const p = document.createElement('div');
    p.className = 'particle';
    simParticles.appendChild(p);
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
    showBuilder();
    if (selectedA && selectedB) {
        availableMaps = [];
        mapsLoading = true;
        syncMatchBuilder();
        loadAvailableMaps();
    }
}

function showBuilder() {
    matchBuilder.style.display = (selectedA && selectedB) ? 'block' : 'none';
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
}

btnAddMap.addEventListener('click', () => { matchMaps = []; syncMatchBuilder(); });

// ─── SIMULAR PARTIDO ──────────────────────────────────────────────────────────
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
        const res = await fetch(`${PREDICT_DIRECTO}/api/predecir`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'ngrok-skip-browser-warning': '1',
            },
            body: JSON.stringify({
                equipo_a: selectedA,
                equipo_b: selectedB,
                mapas: matchMaps.map(m => ({ map_name: m.map_name, lado_inicial_a: m.lado_inicial_a })),
                n_sim: nSim,
            })
        });
        clearInterval(lInterval);
        const data = await res.json();

        if (!data.ok) {
            progressLbl.textContent = `Error: ${data.error}`;
            progressLbl.style.color = 'var(--red)';
            return;
        }

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

// ─── RENDER RESULTADOS ────────────────────────────────────────────────────────
const pct = v => Math.round((v || 0) * 100);

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

    return `
    <div class="pm-num">0${i + 1}</div>
    <div class="pm-map-info">
      <div class="pm-map-name">${m.map_name.toUpperCase()}</div>
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
