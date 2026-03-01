const API = window.location.hostname.includes('localhost') ? 'http://localhost:5000/api' : 'https://aletheia-backend.onrender.com/api';

let teams = [];
let selectedA = null;
let selectedB = null;
let nSim = 10000;

// ─── DOM ─────────────────────────────────────────────────────────────────────
const gridA = document.getElementById('teamGridA');
const gridB = document.getElementById('teamGridB');
const selA = document.getElementById('selectedA');
const selB = document.getElementById('selectedB');
const searchA = document.getElementById('searchA');
const searchB = document.getElementById('searchB');
const btnPredict = document.getElementById('btnPredict');
const h2hBadge = document.getElementById('h2hBadge');
const simProgress = document.getElementById('simProgress');
const progressBar = document.getElementById('simProgressBar');
const progressLbl = document.getElementById('simProgressLabel');
const simParticles = document.getElementById('simParticles');
const resultsSection = document.getElementById('resultsSection');
const statsCompare = document.getElementById('statsCompare');
const mapsGrid = document.getElementById('mapsGrid');
const methodNote = document.getElementById('methodNote');

// ─── PARTÍCULAS LOADING ───────────────────────────────────────────────────────
for (let i = 0; i < 5; i++) {
  const p = document.createElement('div');
  p.className = 'particle';
  simParticles.appendChild(p);
}

// ─── CARGAR EQUIPOS ───────────────────────────────────────────────────────────
async function loadTeams() {
  try {
    const res = await fetch(`${API}/equipos-pred`);
    const data = await res.json();
    if (!data.ok) return;
    teams = data.teams;
    renderTeamGrids(teams);
  } catch (e) {
    gridA.innerHTML = `<div style="color:var(--red);padding:12px;font-size:11px">No se pudo conectar al backend</div>`;
    gridB.innerHTML = gridA.innerHTML;
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
    card.dataset.name = t.name;
    card.dataset.side = side;
    const isSelA = t.name === selectedA;
    const isSelB = t.name === selectedB;
    if (isSelA) card.classList.add('selected-a');
    if (isSelB) card.classList.add('selected-b');
    if ((side === 'a' && isSelB) || (side === 'b' && isSelA)) card.classList.add('disabled');

    card.innerHTML = `
      <div class="tc-abbrev">${t.abbrev}</div>
      <div class="tc-maps">${t.maps_played}m · ${t.avg_rating}r</div>
    `;
    card.addEventListener('click', () => selectTeam(t, side));
    container.appendChild(card);
  });
}

// ─── SELECCIÓN ────────────────────────────────────────────────────────────────
function selectTeam(team, side) {
  if (side === 'a') {
    selectedA = team.name;
    TEAM_ABBREV_CACHE[team.name] = team.abbrev;
    selA.innerHTML = `<span>${team.abbrev}</span>`;
    selA.classList.add('has-team');
    selA.title = team.name;
  } else {
    selectedB = team.name;
    TEAM_ABBREV_CACHE[team.name] = team.abbrev;
    selB.innerHTML = `<span>${team.abbrev}</span>`;
    selB.classList.add('has-team');
    selB.title = team.name;
  }
  renderTeamGrids(filterTeams(side === 'a' ? searchA.value : searchB.value, side));
  updatePredictBtn();
  showModeToggle();
  updateHintTeam();
  if (selectedA && selectedB) fetchH2H();
}

function filterTeams(query, side) {
  const q = (query || '').toLowerCase().trim();
  return q ? teams.filter(t =>
    t.name.toLowerCase().includes(q) || t.abbrev.toLowerCase().includes(q)
  ) : teams;
}

// ─── BÚSQUEDA ─────────────────────────────────────────────────────────────────
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

// ─── BOTÓN ────────────────────────────────────────────────────────────────────
function updatePredictBtn() {
  if (selectedA && selectedB) {
    btnPredict.classList.add('ready');
    btnPredict.disabled = false;
  } else {
    btnPredict.classList.remove('ready');
    btnPredict.disabled = true;
  }
}

// ─── H2H ─────────────────────────────────────────────────────────────────────
async function fetchH2H() {
  try {
    const res = await fetch(`${API}/predecir`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ team_a: selectedA, team_b: selectedB, simulations: 1000 })
    });
    const data = await res.json();
    if (!data.ok) return;
    const h2h = data.summary.h2h;
    if (h2h.total > 0) {
      const abbA = data.summary.team_a.abbrev;
      const abbB = data.summary.team_b.abbrev;
      h2hBadge.style.display = 'block';
      h2hBadge.innerHTML =
        `H2H<br>` +
        `<span style="color:var(--accent)">${abbA} ${h2h.a_wins}</span> — ` +
        `<span style="color:var(--blue)">${h2h.b_wins} ${abbB}</span>`;
    } else {
      h2hBadge.style.display = 'none';
    }
  } catch { }
}

// ─── PREDECIR ─────────────────────────────────────────────────────────────────
btnPredict.addEventListener('click', runPrediction);

async function runPrediction() {
  if (!selectedA || !selectedB || btnPredict.classList.contains('running')) return;

  btnPredict.classList.remove('ready');
  btnPredict.classList.add('running');
  btnPredict.innerHTML = `<span class="btn-predict-icon">◈</span>SIMULANDO...`;

  simProgress.style.display = 'block';
  resultsSection.style.display = 'none';
  progressLbl.textContent = `Ejecutando ${nSim.toLocaleString()} simulaciones por escenario...`;

  const labels = [
    'Procesando historial de rondas...',
    'Calculando perfiles por mapa...',
    'Midiendo eficiencia económica...',
    'Evaluando factor clutch...',
    'Analizando tendencias de veto...',
    'Ejecutando Monte Carlo...',
    'Consolidando resultados...',
  ];
  let li = 0;
  const lInterval = setInterval(() => {
    if (li < labels.length) {
      progressLbl.textContent = labels[li++];
    }
  }, 600);

  try {
    const res = await fetch(`${API}/predecir`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ team_a: selectedA, team_b: selectedB, simulations: nSim })
    });
    clearInterval(lInterval);
    const data = await res.json();

    if (!data.ok) {
      progressLbl.textContent = `Error: ${data.error}`;
      progressLbl.style.color = 'var(--red)';
      return;
    }

    simProgress.style.display = 'none';
    renderResults(data);

  } catch (e) {
    clearInterval(lInterval);
    progressLbl.textContent = `Error de conexión: ${e.message}`;
    progressLbl.style.color = 'var(--red)';
  } finally {
    btnPredict.classList.remove('running');
    btnPredict.classList.add('ready');
    btnPredict.innerHTML = `<span class="btn-predict-icon">◈</span>SIMULAR`;
  }
}

// ─── RENDER RESULTADOS ────────────────────────────────────────────────────────
function pct(v) { return Math.round(v * 100); }
function pctStr(v) { return `${pct(v)}%`; }

function colorClass(winA) {
  const p = winA * 100;
  if (p >= 60) return 'color-green';
  if (p >= 40) return 'color-yellow';
  return 'color-red';
}

function favorClass(winA) {
  const p = winA * 100;
  if (p >= 55) return 'favored-a';
  if (p <= 45) return 'favored-b';
  return 'even';
}

function renderStats(summary) {
  const ta = summary.team_a;
  const tb = summary.team_b;
  const h2h = summary.h2h;

  const statRows = [
    { label: 'RATING', va: ta.avg_rating, vb: tb.avg_rating, maxV: 1.5, fmt: v => v.toFixed(2) },
    { label: 'ATK WR', va: ta.atk_wr, vb: tb.atk_wr, maxV: 70, fmt: v => v.toFixed(1) + '%' },
    { label: 'DEF WR', va: ta.def_wr, vb: tb.def_wr, maxV: 70, fmt: v => v.toFixed(1) + '%' },
    { label: 'PISTOL WR', va: ta.pistol_wr, vb: tb.pistol_wr, maxV: 80, fmt: v => v.toFixed(1) + '%' },
    { label: 'FULL BUY', va: ta.full_buy_wr, vb: tb.full_buy_wr, maxV: 80, fmt: v => v.toFixed(1) + '%' },
    { label: 'CLUTCH/M', va: ta.clutch_pm, vb: tb.clutch_pm, maxV: 20, fmt: v => v.toFixed(1) },
  ];

  let rowsA = '', rowsB = '';
  statRows.forEach(s => {
    const pctA = Math.min(100, (s.va / s.maxV) * 100);
    const pctB = Math.min(100, (s.vb / s.maxV) * 100);
    rowsA += `
      <div class="stat-row">
        <span class="stat-label">${s.label}</span>
        <div class="stat-bar-wrap">
          <div class="stat-bar-fill" style="width:${pctA}%"></div>
        </div>
        <span class="stat-val">${s.fmt(s.va)}</span>
      </div>`;
    rowsB += `
      <div class="stat-row">
        <span class="stat-label">${s.label}</span>
        <div class="stat-bar-wrap">
          <div class="stat-bar-fill" style="width:${pctB}%"></div>
        </div>
        <span class="stat-val">${s.fmt(s.vb)}</span>
      </div>`;
  });

  const h2hTxt = h2h.total > 0
    ? `${ta.abbrev} ${h2h.a_wins} — ${h2h.b_wins} ${tb.abbrev}`
    : 'Sin encuentros previos';

  statsCompare.innerHTML = `
    <div class="stats-team">
      <div class="stats-team-name">${ta.name}</div>
      ${rowsA}
    </div>
    <div class="stats-divider">
      <span class="stats-vs-label">VS</span>
      <div class="stat-divider-item">${ta.maps_played}<br>mapas</div>
      <div class="stat-divider-item" style="color:var(--accent);font-size:9px;border-top:1px solid var(--border);padding-top:8px;width:100%;text-align:center">H2H</div>
      <div class="stat-divider-item">${h2hTxt}</div>
      <div class="stat-divider-item">${tb.maps_played}<br>mapas</div>
    </div>
    <div class="stats-team team-b-stats">
      <div class="stats-team-name">${tb.name}</div>
      ${rowsB}
    </div>
  `;
}

function renderMaps(results, summary) {
  const ta = summary.team_a;
  const tb = summary.team_b;

  // Agrupar por mapa
  const byMap = {};
  results.forEach(r => {
    if (!byMap[r.map]) byMap[r.map] = {};
    byMap[r.map][r.start] = r;
  });

  mapsGrid.innerHTML = '';

  Object.entries(byMap).forEach(([mapName, sides]) => {
    const atk = sides['attack'];
    const def = sides['defense'];
    const row = document.createElement('div');
    row.className = 'map-row';

    // Generar celda de predicción
    function cell(data, sideClass) {
      const winA = data.win_a;
      const clr = colorClass(winA);
      const fav = favorClass(winA);
      const barW = Math.round(winA * 100);

      // Puntos de confianza (3 = high, 2 = medium, 1 = low)
      const confLevels = { high: 3, medium: 2, low: 1 };
      const confN = confLevels[data.confidence] || 1;
      let dots = '';
      for (let i = 0; i < 3; i++) {
        const cls = i < confN ? data.confidence : '';
        dots += `<span class="conf-dot ${cls}"></span>`;
      }

      // Score esperado y OT
      const otPct = data.ot_pct != null ? Math.round(data.ot_pct * 100) : 0;
      const scoreA = data.avg_score_a != null ? data.avg_score_a.toFixed(1) : '—';
      const scoreB = data.avg_score_b != null ? data.avg_score_b.toFixed(1) : '—';
      const modal = data.modal_score || '—';
      const otHtml = otPct > 0
        ? `<span class="pc-ot ${otPct >= 20 ? 'pc-ot-high' : ''}">OT ${otPct}%</span>`
        : '';

      return `
        <div class="prediction-cell ${sideClass}">
          <div class="pc-teams">
            <div>
              <div class="pc-team-name" style="color:var(--accent)">${ta.abbrev}</div>
              <div class="pc-pct ${fav}">${pctStr(winA)}</div>
            </div>
            <div style="text-align:right">
              <div class="pc-team-name" style="color:var(--blue)">${tb.abbrev}</div>
              <div class="pc-pct" style="color:var(--blue)">${pctStr(data.win_b)}</div>
            </div>
          </div>
          <div class="pc-bar-track">
            <div class="pc-bar-a ${clr}" style="width:${barW}%"></div>
          </div>
          <div class="pc-score-line">
            <span class="pc-score-lbl">≈</span>
            <span class="pc-score-a">${scoreA}</span>
            <span class="pc-score-sep">—</span>
            <span class="pc-score-b">${scoreB}</span>
            <span class="pc-modal">(${modal})</span>
            ${otHtml}
          </div>
          <div class="pc-meta">
            <span class="pc-round-prob">
              rnd ATK ${pct(data.p_round_atk)}% · DEF ${pct(data.p_round_def)}%
            </span>
            <div class="pc-conf" title="Confianza: ${data.confidence} (${data.a_maps} vs ${data.b_maps} mapas)">${dots}</div>
          </div>
        </div>
      `;
    }

    const mapImg = `../multimedia/maps/${mapName.toUpperCase()}.avif`;
    row.innerHTML = `
      <div class="map-label">
        <img class="map-thumb" src="${mapImg}" alt="${mapName}" onerror="this.style.display='none'">
        <span class="map-name-text">${mapName.toUpperCase()}</span>
      </div>
      ${atk ? cell(atk, 'atk-cell') : '<div class="prediction-cell">—</div>'}
      ${def ? cell(def, 'def-cell') : '<div class="prediction-cell">—</div>'}
    `;
    mapsGrid.appendChild(row);
  });
}

function renderMethodology(summary, nSim) {
  const ta = summary.team_a;
  const tb = summary.team_b;
  const bestA = ta.best_maps.filter(m => m.score !== null).slice(0, 3).map(m => m.map).join(', ') || '—';
  const bestB = tb.best_maps.filter(m => m.score !== null).slice(0, 3).map(m => m.map).join(', ') || '—';

  methodNote.innerHTML = `
    <strong>Metodología:</strong> ${nSim.toLocaleString()} simulaciones Monte Carlo por escenario (14 total).
    Señales usadas: historial ATK/DEF por ronda en cada mapa (40%), rendimiento de jugadores por lado — rating, ACS, ADR, FK/FD (28%),
    eficiencia económica — pistol WR + full-buy WR (18%), factor clutch y multikills (9%), H2H + tendencias de veto (5%).
    La probabilidad por ronda usa suavizado Bayesiano con prior del meta global del mapa.
    &nbsp;·&nbsp; <strong>Mejores mapas ${ta.abbrev}:</strong> ${bestA}
    &nbsp;·&nbsp; <strong>Mejores mapas ${tb.abbrev}:</strong> ${bestB}
  `;
}

function renderResults(data) {
  renderStats(data.summary);
  renderMaps(data.results, data.summary);
  renderMethodology(data.summary, data.simulations);
  resultsSection.style.display = 'block';
  resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ─── MODO TOGGLE ─────────────────────────────────────────────────────────────
let currentMode = 'explorer';

const modeToggle = document.getElementById('modeToggle');
const matchBuilder = document.getElementById('matchBuilder');
const partidoResults = document.getElementById('partidoResults');

document.querySelectorAll('.mode-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const mode = btn.dataset.mode;
    if (mode === currentMode) return;
    currentMode = mode;
    document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');

    if (mode === 'partido') {
      matchBuilder.style.display = 'block';
      resultsSection.style.display = 'none';
      partidoResults.style.display = 'none';
      syncMatchBuilder();  // render map tiles immediately
    } else {
      matchBuilder.style.display = 'none';
      partidoResults.style.display = 'none';
    }
  });
});

function showModeToggle() {
  if (selectedA && selectedB) modeToggle.style.display = 'grid';
  else modeToggle.style.display = 'none';
}

// ─── MATCH BUILDER ────────────────────────────────────────────────────────────
const MAPS_LIST = ['Abyss', 'Ascent', 'Bind', 'Corrode', 'Fracture', 'Haven', 'Icebox', 'Lotus', 'Pearl', 'Split', 'Sunset'];
const mapSlots = document.getElementById('mapSlots');
const btnAddMap = document.getElementById('btnAddMap');
const btnSimPart = document.getElementById('btnSimPartido');
const bspCount = document.getElementById('bspCount');
const mbFormat = document.getElementById('mbFormat');
const hintTeamA = document.getElementById('hintTeamA');

let matchMaps = [];   // [{map_name, a_starts_atk}]

function updateHintTeam() {
  if (selectedA) hintTeamA.textContent = TEAM_ABBREV_CACHE[selectedA] || selectedA.split(' ')[0];
}
const TEAM_ABBREV_CACHE = {};  // se llena al cargar equipos

function syncMatchBuilder() {
  mapSlots.innerHTML = '';

  // ── 1. Mapa tiles (picker rápido) ─────────────────────────────────────────
  const pickerDiv = document.createElement('div');
  pickerDiv.className = 'map-quick-picker';

  MAPS_LIST.forEach(m => {
    const used = matchMaps.some(mm => mm.map_name === m);
    const full = matchMaps.length >= 5;
    const tile = document.createElement('button');
    tile.className = `mqp-tile${used ? ' mqp-used' : ''}${(!used && full) ? ' mqp-full' : ''}`;
    const tImg = `../multimedia/maps/${m.toUpperCase()}.avif`;
    tile.innerHTML = `
      <img class="mqp-img" src="${tImg}" alt="${m}" onerror="this.style.display='none'">
      <span class="mqp-name">${m.toUpperCase()}</span>
    `;
    tile.disabled = used || full;
    if (!used && !full) {
      tile.addEventListener('click', () => {
        matchMaps.push({ map_name: m, a_starts_atk: true });
        syncMatchBuilder();
        updateBuilderState();
      });
    }
    pickerDiv.appendChild(tile);
  });
  mapSlots.appendChild(pickerDiv);

  // ── 2. Cola de mapas seleccionados ────────────────────────────────────────
  if (matchMaps.length > 0) {
    const queueDiv = document.createElement('div');
    queueDiv.className = 'map-queue';

    matchMaps.forEach((cfg, i) => {
      const isLast = i === matchMaps.length - 1;
      const isDecider = isLast && matchMaps.length >= 2;
      const abbrevA = selectedA ? (TEAM_ABBREV_CACHE[selectedA] || '?') : 'A';

      const item = document.createElement('div');
      item.className = `map-queue-item${isDecider ? ' qi-decider-row' : ''}`;
      item.innerHTML = `
        <div class="qi-left">
          <span class="qi-num">0${i + 1}</span>
          <img class="qi-map-img" src="../multimedia/maps/${cfg.map_name.toUpperCase()}.avif" alt="${cfg.map_name}" onerror="this.style.display='none'">
          <span class="qi-mapname">${cfg.map_name.toUpperCase()}</span>
          ${isDecider ? '<span class="qi-decider-badge">DECIDER</span>' : ''}
        </div>
        <div class="qi-side-group">
          <span class="qi-side-label">${abbrevA} empieza:</span>
          <button class="qi-side-btn${cfg.a_starts_atk ? ' qi-atk-active' : ''}" data-idx="${i}" data-side="atk">
            ⚔ ATK
          </button>
          <button class="qi-side-btn${!cfg.a_starts_atk ? ' qi-def-active' : ''}" data-idx="${i}" data-side="def">
            🛡 DEF
          </button>
        </div>
        <button class="qi-remove" data-idx="${i}" title="Quitar">✕</button>
      `;
      queueDiv.appendChild(item);
    });

    mapSlots.appendChild(queueDiv);
  }

  // ── 3. Bind events ────────────────────────────────────────────────────────
  mapSlots.querySelectorAll('.qi-side-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      const idx = parseInt(e.currentTarget.dataset.idx);
      const side = e.currentTarget.dataset.side;
      matchMaps[idx].a_starts_atk = (side === 'atk');
      syncMatchBuilder();
      updateBuilderState();
    });
  });

  mapSlots.querySelectorAll('.qi-remove').forEach(btn => {
    btn.addEventListener('click', e => {
      const idx = parseInt(e.currentTarget.dataset.idx);
      matchMaps.splice(idx, 1);
      syncMatchBuilder();
      updateBuilderState();
    });
  });

  updateBuilderState();
}

function updateBuilderState() {
  const n = matchMaps.length;
  const filled = matchMaps.filter(m => m.map_name).length;
  const ready = filled === n && n > 0;
  const formats = ['', 'Bo1', 'Bo2', 'Bo3', 'Bo4', 'Bo5'];

  mbFormat.textContent = n > 0 ? formats[n] || `Bo${n}` : '—';
  bspCount.textContent = `${filled} mapa${filled !== 1 ? 's' : ''}`;

  btnSimPart.classList.toggle('ready', ready);
  btnSimPart.disabled = !ready;
}

// Limpiar todos los mapas
btnAddMap.addEventListener('click', () => {
  matchMaps = [];
  syncMatchBuilder();
  updateBuilderState();
});

// ─── SIMULAR PARTIDO ─────────────────────────────────────────────────────────
btnSimPart.addEventListener('click', runPartido);

async function runPartido() {
  if (!btnSimPart.classList.contains('ready') || btnSimPart.classList.contains('running')) return;

  btnSimPart.classList.remove('ready');
  btnSimPart.classList.add('running');
  btnSimPart.querySelector('.bsp-text').textContent = 'SIMULANDO...';

  simProgress.style.display = 'block';
  partidoResults.style.display = 'none';

  const labels = [
    'Construyendo perfiles de equipo...',
    'Analizando cada mapa configurado...',
    'Calculando probabilidades por ronda...',
    `Ejecutando ${nSim.toLocaleString()} simulaciones de partido...`,
    'Calculando distribución de resultados...',
  ];
  let li = 0;
  simProgressLabel.textContent = labels[0];
  const lInterval = setInterval(() => {
    if (li < labels.length - 1) simProgressLabel.textContent = labels[++li];
  }, 700);

  try {
    const res = await fetch(`${API}/predecir-partido`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        team_a: selectedA,
        team_b: selectedB,
        maps: matchMaps.filter(m => m.map_name),
        simulations: nSim,
      })
    });
    clearInterval(lInterval);
    const data = await res.json();

    if (!data.ok) {
      simProgressLabel.textContent = `Error: ${data.error}`;
      simProgressLabel.style.color = 'var(--red)';
      return;
    }

    simProgress.style.display = 'none';
    renderPartidoResults(data);

  } catch (e) {
    clearInterval(lInterval);
    simProgressLabel.textContent = `Error de conexión: ${e.message}`;
    simProgressLabel.style.color = 'var(--red)';
  } finally {
    btnSimPart.classList.remove('running');
    btnSimPart.classList.add('ready');
    btnSimPart.querySelector('.bsp-text').textContent = 'SIMULAR PARTIDO';
  }
}

// ─── RENDER PARTIDO RESULTS ───────────────────────────────────────────────────
function renderPartidoResults(data) {
  const { series, map_results, summary, simulations } = data;
  const ta = summary.team_a;
  const tb = summary.team_b;

  // ── 1. Series banner ──────────────────────────────────────────────────────
  const winnerIsA = series.win_a > series.win_b;
  const banner = document.getElementById('seriesBanner');
  const favClass = series.win_a * 100 >= 55 ? 'winner-side' : (series.win_b * 100 >= 55 ? '' : '');
  const favClassB = series.win_b * 100 >= 55 ? 'winner-side-b' : '';

  banner.innerHTML = `
    <div class="sb-team ${favClass}">
      <div class="sb-name">${ta.name.toUpperCase()}</div>
      <div class="sb-abbrev">${ta.abbrev}</div>
      <div class="sb-pct">${Math.round(series.win_a * 100)}%</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
    <div class="sb-center">
      <div class="sb-format">${series.format}</div>
      <div class="sb-sims">${simulations.toLocaleString()}<br>SIMULACIONES</div>
      <div style="font-size:10px;color:var(--dim);letter-spacing:1px;margin-top:4px">GANAR ${series.maps_to_win}</div>
    </div>
    <div class="sb-team ${favClassB}" style="text-align:right">
      <div class="sb-name">${tb.name.toUpperCase()}</div>
      <div class="sb-abbrev">${tb.abbrev}</div>
      <div class="sb-pct">${Math.round(series.win_b * 100)}%</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
  `;

  // ── 2. Score distribution ─────────────────────────────────────────────────
  const distWrap = document.getElementById('scoreDistWrap');
  const distItems = Object.entries(series.score_dist).sort((a, b) => b[1] - a[1]);
  const maxPct = Math.max(...distItems.map(([, v]) => v));

  let barsHtml = distItems.map(([score, prob]) => {
    const [wa, wb] = score.split('-').map(Number);
    const isA = wa > wb;
    const isB = wb > wa;
    const cls = isA ? 'win-a' : (isB ? 'win-b' : 'draw');
    const barH = Math.round((prob / maxPct) * 44);
    return `
      <div class="sd-item">
        <div class="sd-bar-wrap">
          <div class="sd-bar ${cls}" style="height:${barH}px"></div>
        </div>
        <div class="sd-score">${score}</div>
        <div class="sd-pct">${Math.round(prob * 100)}%</div>
      </div>
    `;
  }).join('');

  distWrap.innerHTML = `
    <div class="sd-title">DISTRIBUCIÓN DE RESULTADOS</div>
    <div class="sd-bars">${barsHtml}</div>
  `;

  // ── 3. Per-map breakdown ──────────────────────────────────────────────────
  document.getElementById('pmTeamAName').textContent = ta.abbrev;
  const mapsList = document.getElementById('partidoMapsList');
  mapsList.innerHTML = '';

  map_results.forEach((r, i) => {
    const winA = r.win_a * 100;
    const barColor = winA >= 60 ? 'pm-bar-green' : (winA >= 40 ? 'pm-bar-yellow' : 'pm-bar-red');
    const pctClass = winA >= 55 ? 'pm-pct-a' : (winA <= 45 ? 'pm-pct-b' : 'pm-pct-even');
    const confN = { high: 3, medium: 2, low: 1 }[r.confidence] || 1;
    let dots = '';
    for (let d = 0; d < 3; d++) {
      dots += `<span class="conf-dot ${d < confN ? r.confidence : ''}"></span>`;
    }
    const sideLabel = r.a_starts_atk
      ? `<span class="pm-start-atk">⚔ ${ta.abbrev} EMPIEZA ATK</span>`
      : `<span class="pm-start-def">🛡 ${ta.abbrev} EMPIEZA DEF</span>`;

    // ── Score esperado y OT ────────────────────────────────────────────────
    const modalParts = (r.modal_score || '0-0').split('-').map(Number);
    const modalWinnerIsA = modalParts[0] > modalParts[1];
    const otPct = Math.round((r.ot_pct || 0) * 100);
    const scoreA = r.avg_score_a != null ? r.avg_score_a.toFixed(1) : '—';
    const scoreB = r.avg_score_b != null ? r.avg_score_b.toFixed(1) : '—';

    // Top scores (minibar)
    let scoreFreqHtml = '';
    if (r.score_freq) {
      const freqEntries = Object.entries(r.score_freq).slice(0, 4);
      const maxF = Math.max(...freqEntries.map(([, v]) => v));
      scoreFreqHtml = freqEntries.map(([score, prob]) => {
        const [sa, sb] = score.split('-').map(Number);
        const isAWin = sa > sb;
        const barW = Math.round((prob / maxF) * 100);
        const cls = isAWin ? 'sf-bar-a' : 'sf-bar-b';
        return `
          <div class="sf-item">
            <span class="sf-score">${score}</span>
            <div class="sf-bar-track"><div class="sf-bar ${cls}" style="width:${barW}%"></div></div>
            <span class="sf-pct">${Math.round(prob * 100)}%</span>
          </div>`;
      }).join('');
    }

    const row = document.createElement('div');
    row.className = 'pm-row';
    row.style.animationDelay = `${i * 0.06}s`;
    row.innerHTML = `
      <div class="pm-num">0${i + 1}</div>
      <div class="pm-map-info">
        <div class="pm-map-name">${r.map.toUpperCase()}</div>
        <div class="pm-start-side">${sideLabel}</div>
        <div class="pm-score-expect">
          <span class="pse-label">SCORE ESPERADO</span>
          <span class="pse-val" style="color:var(--accent)">${scoreA}</span>
          <span class="pse-sep">—</span>
          <span class="pse-val" style="color:var(--blue)">${scoreB}</span>
          ${otPct > 0 ? `<span class="pse-ot ${otPct >= 20 ? 'pse-ot-high' : ''}">OT ${otPct}%</span>` : ''}
        </div>
      </div>
      <div class="pm-prob-cell">
        <div class="pm-teams-row">
          <div class="pm-team-pct">
            <span class="pm-abbrev" style="color:var(--accent)">${ta.abbrev}</span>
            <span class="pm-pct-val ${pctClass}">${Math.round(winA)}%</span>
          </div>
          <div class="pm-team-pct">
            <span class="pm-abbrev" style="color:var(--blue)">${tb.abbrev}</span>
            <span class="pm-pct-val pm-pct-b">${Math.round(r.win_b * 100)}%</span>
          </div>
        </div>
        <div class="pm-bar-track">
          <div class="pm-bar-fill ${barColor}" style="width:${Math.round(winA)}%"></div>
        </div>
        <div class="pm-score-freq">${scoreFreqHtml}</div>
        <div class="pm-meta">
          <span class="pm-round-info">rnd ATK ${Math.round(r.p_round_atk * 100)}% · DEF ${Math.round(r.p_round_def * 100)}%</span>
          <div class="pm-conf-dots" title="Confianza: ${r.confidence} (${r.a_maps} vs ${r.b_maps} mapas)">${dots}</div>
        </div>
      </div>
    `;
    mapsList.appendChild(row);
  });

  // ── 4. Nota metodológica ──────────────────────────────────────────────────
  document.getElementById('partidoMethodNote').innerHTML = `
    <strong>Metodología:</strong> ${simulations.toLocaleString()} simulaciones Monte Carlo.
    Formato <strong>${series.format}</strong> — necesario ganar <strong>${series.maps_to_win}</strong> mapa(s).
    Las probabilidades de cada mapa son independientes y se simulan en el orden configurado.
    En Bo3/Bo5 la serie se detiene cuando un equipo alcanza el número de victorias necesario.
  `;

  partidoResults.style.display = 'block';
  partidoResults.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ─── INIT ─────────────────────────────────────────────────────────────────────
loadTeams();