const isLocal = ['localhost', '127.0.0.1'].includes(window.location.hostname);
const API = isLocal ? `${window.location.origin}/api` : 'https://aletheia-web.onrender.com/api';

let teams = [];
let selectedA = null;
let selectedB = null;
let nSim = 10000;
const TEAM_ABBREV_CACHE = {};
let cachedAnalysis = null;
let availableMaps = null;  // mapas con datos en la DB para los equipos seleccionados
let mapsLoading = false;   // true mientras carga mapas disponibles
let lastPartidoData = null; // almacena los resultados del último partido simulado
const VALORANT_AGENTS = ['Astra', 'Breach', 'Brimstone', 'Chamber', 'Clove', 'Cypher', 'Deadlock', 'Fade', 'Gekko', 'Harbor', 'Iso', 'Jett', 'KAY/O', 'Killjoy', 'Neon', 'Omen', 'Phoenix', 'Raze', 'Reyna', 'Sage', 'Skye', 'Sova', 'Tejo', 'Viper', 'Vyse', 'Waylay', 'Yoru'];
const OPERATOR_AGENTS = new Set(['Jett', 'Chamber']);

// ─── DOM ──────────────────────────────────────────────────────────────────────
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
const modeToggle = document.getElementById('modeToggle');
const matchBuilder = document.getElementById('matchBuilder');
const partidoResults = document.getElementById('partidoResults');

// ─── PARTÍCULAS LOADING ──────────────────────────────────────────────────────
for (let i = 0; i < 5; i++) {
    const p = document.createElement('div');
    p.className = 'particle';
    simParticles.appendChild(p);
}

// ─── CARGAR EQUIPOS ──────────────────────────────────────────────────────────
async function loadTeams() {
    try {
        const res = await fetch(`${API}/aletheia/equipos`);
        const data = await res.json();
        if (!data.ok) return;
        teams = data.teams;
        renderTeamGrids(teams);
    } catch (e) {
        const err = `<div style="color:var(--red);padding:12px;font-size:11px">No se pudo conectar al backend</div>`;
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
      <div class="tc-abbrev">${t.abbrev}</div>
      <div class="tc-maps">${t.maps_played}m · ${t.avg_rating}r</div>
    `;
        card.addEventListener('click', () => selectTeam(t, side));
        container.appendChild(card);
    });
}

// ─── SELECCIÓN ───────────────────────────────────────────────────────────────
function selectTeam(team, side) {
    TEAM_ABBREV_CACHE[team.name] = team.abbrev;
    if (side === 'a') {
        selectedA = team.name;
        selA.innerHTML = `<span>${team.abbrev}</span>`;
        selA.classList.add('has-team');
        selA.title = team.name;
    } else {
        selectedB = team.name;
        selB.innerHTML = `<span>${team.abbrev}</span>`;
        selB.classList.add('has-team');
        selB.title = team.name;
    }
    renderTeamGrids(filterTeams('', side));
    updatePredictBtn();
    showModeToggle();
    updateHintTeam();
    if (selectedA && selectedB) {
        // Reset available maps so the builder shows a loading state, not stale data
        availableMaps = null;
        mapsLoading = true;
        if (currentMode === 'partido') syncMatchBuilder();
        fetchH2H();
        loadAvailableMaps();
    }
}

async function loadAvailableMaps() {
    if (!selectedA || !selectedB) return;
    mapsLoading = true;
    try {
        const res = await fetch(`${API}/aletheia/mapas_disponibles`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ team_a: selectedA, team_b: selectedB })
        });
        const data = await res.json();
        if (data.ok) {
            availableMaps = data.maps;
        }
    } catch { }
    mapsLoading = false;
    // Siempre actualizar el match builder al terminar
    if (currentMode === 'partido') syncMatchBuilder();
}

function filterTeams(q, side) {
    const query = (q || '').toLowerCase().trim();
    return query ? teams.filter(t =>
        t.name.toLowerCase().includes(query) || t.abbrev.toLowerCase().includes(query)
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

// ─── BOTÓN ────────────────────────────────────────────────────────────────────
function updatePredictBtn() {
    const ready = !!(selectedA && selectedB);
    btnPredict.classList.toggle('ready', ready);
    btnPredict.disabled = !ready;
}

// ─── H2H ──────────────────────────────────────────────────────────────────────
async function fetchH2H() {
    try {
        const res = await fetch(`${API}/aletheia/analizar`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ team_a: selectedA, team_b: selectedB, simulations: 1000 })
        });
        const data = await res.json();
        if (!data.ok) return;
        const h2h = data.summary.h2h;
        const abbA = data.summary.team_a.abbrev;
        const abbB = data.summary.team_b.abbrev;
        if (h2h.total > 0) {
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

// ─── PREDECIR (EXPLORADOR) ───────────────────────────────────────────────────
btnPredict.addEventListener('click', runPrediction);

async function runPrediction() {
    if (!selectedA || !selectedB || btnPredict.classList.contains('running')) return;
    btnPredict.classList.remove('ready');
    btnPredict.classList.add('running');
    btnPredict.innerHTML = `<span class="btn-predict-icon">◈</span>SIMULANDO...`;

    simProgress.style.display = 'block';
    resultsSection.style.display = 'none';

    const labels = [
        'Procesando historial de rondas...',
        'Calculando perfiles por mapa...',
        'Simulando economía round a round...',
        'Evaluando jugadores estrella...',
        'Analizando impacto de la Operator...',
        'Calculando consistencia de jugadores...',
        'Ejecutando Monte Carlo...',
        'Consolidando resultados...',
    ];
    let li = 0;
    progressLbl.textContent = labels[0];
    const lInterval = setInterval(() => {
        if (li < labels.length - 1) progressLbl.textContent = labels[++li];
    }, 600);

    try {
        const res = await fetch(`${API}/aletheia/analizar`, {
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
        cachedAnalysis = data;
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

// ─── HELPERS ─────────────────────────────────────────────────────────────────
const pct = v => Math.round(v * 100);
const pctStr = v => `${pct(v)}%`;

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

// ─── RENDER STATS ─────────────────────────────────────────────────────────────
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
        const pA = Math.min(100, (s.va / s.maxV) * 100);
        const pB = Math.min(100, (s.vb / s.maxV) * 100);
        rowsA += `<div class="stat-row">
      <span class="stat-label">${s.label}</span>
      <div class="stat-bar-wrap"><div class="stat-bar-fill" style="width:${pA}%"></div></div>
      <span class="stat-val">${s.fmt(s.va)}</span>
    </div>`;
        rowsB += `<div class="stat-row">
      <span class="stat-label">${s.label}</span>
      <div class="stat-bar-wrap"><div class="stat-bar-fill" style="width:${pB}%"></div></div>
      <span class="stat-val">${s.fmt(s.vb)}</span>
    </div>`;
    });

    const h2hTxt = h2h.total > 0
        ? `${ta.abbrev} ${h2h.a_wins} — ${h2h.b_wins} ${tb.abbrev}`
        : 'Sin encuentros previos';

    statsCompare.innerHTML = `
    <div class="stats-team">
      <div class="stats-team-name">${ta.name}</div>${rowsA}
    </div>
    <div class="stats-divider">
      <span class="stats-vs-label">VS</span>
      <div class="stat-divider-item">${ta.maps_played}<br>mapas</div>
      <div class="stat-divider-item" style="color:var(--accent);font-size:9px;border-top:1px solid var(--border);padding-top:8px;width:100%;text-align:center">H2H</div>
      <div class="stat-divider-item">${h2hTxt}</div>
      <div class="stat-divider-item">${tb.maps_played}<br>mapas</div>
    </div>
    <div class="stats-team team-b-stats">
      <div class="stats-team-name">${tb.name}</div>${rowsB}
    </div>
  `;
}

// ─── RENDER MAPS ──────────────────────────────────────────────────────────────
function renderMaps(results, summary) {
    const ta = summary.team_a;
    const tb = summary.team_b;
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

        function cell(data, sideClass) {
            const winA = data.win_a;
            const clr = colorClass(winA);
            const fav = favorClass(winA);
            const barW = Math.round(winA * 100);
            const confN = { high: 3, medium: 2, low: 1 }[data.confidence] || 1;
            let dots = '';
            for (let i = 0; i < 3; i++) dots += `<span class="conf-dot ${i < confN ? data.confidence : ''}"></span>`;
            const otPct = data.ot_pct != null ? Math.round(data.ot_pct * 100) : 0;
            const scoreA = data.avg_score_a != null ? data.avg_score_a.toFixed(1) : '—';
            const scoreB = data.avg_score_b != null ? data.avg_score_b.toFixed(1) : '—';
            const modal = data.modal_score || '—';
            const otHtml = otPct > 0
                ? `<span class="pc-ot ${otPct >= 20 ? 'pc-ot-high' : ''}">OT ${otPct}%</span>` : '';
            const htA = data.halftime ? data.halftime.pred_a : '—';
            const htB = data.halftime ? data.halftime.pred_b : '—';
            const htNote = data.halftime && data.halftime.eco_note
                ? `<div class="ht-eco-note">⚠ ${data.halftime.eco_note}</div>` : '';

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
          <div class="pc-bar-track"><div class="pc-bar-a ${clr}" style="width:${barW}%"></div></div>
          <div class="pc-score-line">
            <span class="pc-score-lbl">≈</span>
            <span class="pc-score-a">${scoreA}</span>
            <span class="pc-score-sep">—</span>
            <span class="pc-score-b">${scoreB}</span>
            <span class="pc-modal">(${modal})</span>
            ${otHtml}
          </div>
          <div style="font-size:9px;color:var(--dim);margin-bottom:4px">
            MEDIO TIEMPO ≈ <span style="color:var(--accent)">${htA}</span> — <span style="color:var(--blue)">${htB}</span>
          </div>
          ${htNote}
          <div class="pc-meta">
            <span class="pc-round-prob">rnd ATK ${pct(data.p_round_atk)}% · DEF ${pct(data.p_round_def)}%</span>
            <div class="pc-conf" title="Confianza: ${data.confidence}">${dots}</div>
          </div>
        </div>`;
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

// ─── RENDER PANELES AVANZADOS ─────────────────────────────────────────────────
function renderAdvancedPanels(data) {
    const { summary, results } = data;
    const ta = summary.team_a;
    const tb = summary.team_b;

    const container = document.getElementById('advancedPanels');
    if (!container) return;

    // ── OT Panel ──────────────────────────────────────────────────────────────
    const otRows = results.filter(r => r.start === 'attack').map(r => {
        const otP = Math.round((r.ot_pct || 0) * 100);
        const grade = otP >= 25 ? ['BAD', 'closer-bad'] : otP >= 12 ? ['OK', 'closer-ok'] : ['GOOD', 'closer-good'];
        const barCls = otP >= 25 ? 'ot-bar-high' : otP >= 12 ? 'ot-bar-mid' : 'ot-bar-low';
        const closerA = r.ot_closer_a != null ? Math.round(r.ot_closer_a * 100) : 50;
        const closerB = 100 - closerA;
        return `
      <div class="ot-map-row">
        <span class="ot-map-nm">${r.map.toUpperCase()}</span>
        <span class="ot-map-pct">${otP}% OT</span>
        <div class="ot-bar-track"><div class="ot-bar-fill ${barCls}" style="width:${Math.min(otP * 2, 100)}%"></div></div>
        <span class="ot-closer-grade ${grade[1]}">${grade[0]}</span>
        <span style="font-size:9px;color:var(--dim);margin-left:8px">${ta.abbrev} OT: ${closerA}% · ${tb.abbrev}: ${closerB}%</span>
      </div>`;
    }).join('');

    // ── Operator Panel ────────────────────────────────────────────────────────
    const opData = summary.operator_analysis || {};
    const opRows = Object.entries(opData).map(([mapName, op]) => {
        if (!op) return '';
        const w = (op.map_weight || 0) * 100;
        const badge = w >= 80 ? ['DOMINANTE', 'op-dominant'] : w >= 65 ? ['ALTO', 'op-high'] : w >= 45 ? ['MEDIO', 'op-medium'] : ['BAJO', 'op-low'];
        const playersHtml = (op.op_players || []).map(p =>
            `<div class="op-player-chip">
        <span class="op-player-name">${p.name}</span>
        <span class="op-player-agent">${p.agent}</span>
        <span style="color:var(--dim);font-size:8px">r:${(p.rating || 0).toFixed(2)}</span>
      </div>`
        ).join('');
        return `
      <div class="op-map-row">
        <img class="op-map-thumb" src="../multimedia/maps/${mapName.toUpperCase()}.avif" onerror="this.style.display='none'">
        <span class="op-map-nm">${mapName.toUpperCase()}</span>
        <span class="op-weight-badge ${badge[1]}">${badge[0]}</span>
        <div class="op-impact-bar"><div class="op-impact-fill" style="width:${w}%"></div></div>
        <div class="op-players">${playersHtml}</div>
      </div>`;
    }).join('');

    // ── Star Player Panel ─────────────────────────────────────────────────────
    function starBlock(starData, teamAbbrev, colorCls) {
        if (!starData || !starData.star_name) return `<div class="star-team-block"><div class="star-team-header"><span class="star-team-label">EQUIPO</span><span class="star-team-abbrev ${colorCls}">${teamAbbrev}</span></div><div style="font-size:10px;color:var(--dim);padding:8px 0">Sin datos suficientes</div></div>`;
        const dep = starData.dependency || 1;
        const depGrade = dep >= 1.25 ? ['ALTA', 'dep-high'] : dep >= 1.10 ? ['MEDIA', 'dep-medium'] : ['BAJA', 'dep-low'];
        const counterTxt = starData.counter_risk
            ? `<div class="counter-meta">⚠ <span>Riesgo de contra-estrategia detectado</span> — el rival tiene historial de neutralizar jugadores dependientes del Operator o con alta carga individual.</div>`
            : '';
        return `
      <div class="star-team-block">
        <div class="star-team-header">
          <span class="star-team-label">EQUIPO</span>
          <span class="star-team-abbrev ${colorCls}">${teamAbbrev}</span>
        </div>
        <div class="star-player-row">
          <span class="star-crown">★</span>
          <span class="star-name">${starData.star_name}</span>
          <div class="star-rating">Rating: <span>${(starData.star_rating || 0).toFixed(2)}</span></div>
        </div>
        <div class="star-metrics">
          <div class="star-metric">
            <span class="sm-label">RATING EQUIPO</span>
            <div class="sm-bar-track"><div class="sm-bar-fill" style="width:${Math.min(100, (starData.team_avg_rating || 1) / 1.5 * 100)}%"></div></div>
            <span class="sm-val">${(starData.team_avg_rating || 0).toFixed(2)}</span>
          </div>
          <div class="star-metric">
            <span class="sm-label">CONSISTENCIA</span>
            <div class="sm-bar-track"><div class="sm-bar-fill" style="width:${Math.min(100, 100 - (starData.star_std || 0) * 400)}%"></div></div>
            <span class="sm-val">σ ${(starData.star_std || 0).toFixed(2)}</span>
          </div>
        </div>
        <div class="star-dependency">
          <span class="dep-label">DEPENDENCIA DEL EQUIPO</span>
          <span class="dep-grade ${depGrade[1]}">${depGrade[0]} (×${dep.toFixed(2)})</span>
        </div>
        ${counterTxt}
      </div>`;
    }

    const starA = summary.star_player_a || {};
    const starB = summary.star_player_b || {};

    // ── Consistency Panel ─────────────────────────────────────────────────────
    function consBlock(players, teamAbbrev, colorCls) {
        if (!players || players.length === 0) return '';
        const rows = players.map(p => {
            const gc = { A: 'grade-A', B: 'grade-B', C: 'grade-C', D: 'grade-D' }[p.grade] || 'grade-D';
            return `<div class="cons-row">
        <span class="cons-player">${p.name}</span>
        <span class="cons-val">${(p.avg_rating || 0).toFixed(2)}</span>
        <span class="cons-std">σ${(p.std_dev || 0).toFixed(2)}</span>
        <span class="cons-grade"><span class="grade-badge ${gc}">${p.grade}</span></span>
      </div>`;
        }).join('');
        return `
      <div class="cons-block">
        <div class="cons-team-label" style="color:var(--${colorCls === 'star-a' ? 'accent' : 'blue'})">${teamAbbrev}</div>
        <div class="cons-table">
          <div class="cons-header">
            <span>JUGADOR</span><span style="text-align:right">RATING</span>
            <span style="text-align:right">STD DEV</span><span style="text-align:right">GRADE</span>
          </div>
          ${rows}
        </div>
      </div>`;
    }

    container.innerHTML = `
    <!-- OT + Operator row -->
    <div class="advanced-panels">
      <div class="advanced-panel">
        <div class="ap-title"><span class="ap-icon">⏱</span> ANÁLISIS DE OVERTIME</div>
        <div class="ot-section">
          <div class="ot-map-rows">${otRows || '<div class="op-no-data">Sin datos de OT</div>'}</div>
        </div>
      </div>
      <div class="advanced-panel">
        <div class="ap-title"><span class="ap-icon">🔭</span> IMPACTO DE LA OPERATOR</div>
        <div class="op-section">
          ${opRows || '<div class="op-no-data">Sin datos de Operator para estos mapas</div>'}
        </div>
      </div>
    </div>

    <!-- Star player row -->
    <div class="advanced-panels">
      <div class="advanced-panel">
        <div class="ap-title"><span class="ap-icon">★</span> JUGADOR ESTRELLA</div>
        <div class="star-section">
          ${starBlock(starA, ta.abbrev, 'star-a')}
          ${starBlock(starB, tb.abbrev, 'star-b')}
        </div>
      </div>
      <div class="advanced-panel">
        <div class="ap-title"><span class="ap-icon">📊</span> CONSISTENCIA ESTADÍSTICA</div>
        <div class="consistency-section">
          ${consBlock(summary.consistency_a, ta.abbrev, 'star-a')}
          ${consBlock(summary.consistency_b, tb.abbrev, 'star-b')}
        </div>
      </div>
    </div>
  `;
}

// ─── RENDER METHODOLOGY ───────────────────────────────────────────────────────
function renderMethodology(summary, nSimUsed) {
    const ta = summary.team_a;
    const tb = summary.team_b;
    const bA = (ta.best_maps || []).filter(m => m.score !== null).slice(0, 3).map(m => m.map).join(', ') || '—';
    const bB = (tb.best_maps || []).filter(m => m.score !== null).slice(0, 3).map(m => m.map).join(', ') || '—';
    methodNote.innerHTML = `
    <strong>Metodología Aletheia v3:</strong> ${nSimUsed.toLocaleString()} simulaciones Monte Carlo.
    Señales: historial ATK/DEF (40%), habilidad jugadores (28%), economía real (18%), clutch (9%), H2H/veto (5%).
    Incluye simulación de economía ronda a ronda para predicción del medio tiempo.
    El análisis de OT evalúa la capacidad de cerrar mapas. La Operator se pondera por mapa y por agente del jugador.
    &nbsp;·&nbsp; <strong>Mejores mapas ${ta.abbrev}:</strong> ${bA}
    &nbsp;·&nbsp; <strong>Mejores mapas ${tb.abbrev}:</strong> ${bB}
  `;
}

function renderResults(data) {
    renderStats(data.summary);
    renderMaps(data.results, data.summary);
    renderAdvancedPanels(data);
    renderMethodology(data.summary, data.simulations);
    resultsSection.style.display = 'block';
    resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ─── MODO TOGGLE ─────────────────────────────────────────────────────────────
let currentMode = 'explorer';

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
            // If maps haven't been fetched yet for current team pair, trigger load
            if (!availableMaps && !mapsLoading && selectedA && selectedB) {
                availableMaps = null;
                mapsLoading = true;
                syncMatchBuilder(); // shows loading spinner immediately
                loadAvailableMaps();
            } else {
                syncMatchBuilder();
            }
        } else {
            matchBuilder.style.display = 'none';
            partidoResults.style.display = 'none';
        }
    });
});

function showModeToggle() {
    modeToggle.style.display = (selectedA && selectedB) ? 'grid' : 'none';
}

// ─── MATCH BUILDER ────────────────────────────────────────────────────────────
const MAPS_LIST = ['Abyss', 'Ascent', 'Bind', 'Breeze', 'Corrode', 'Fracture', 'Haven', 'Icebox', 'Lotus', 'Pearl', 'Split', 'Sunset'];
const mapSlots = document.getElementById('mapSlots');
const btnAddMap = document.getElementById('btnAddMap');
const btnSimPart = document.getElementById('btnSimPartido');
const bspCount = document.getElementById('bspCount');
const mbFormat = document.getElementById('mbFormat');
const hintTeamA = document.getElementById('hintTeamA');

let matchMaps = [];

function updateHintTeam() {
    if (selectedA) hintTeamA.textContent = TEAM_ABBREV_CACHE[selectedA] || selectedA.split(' ')[0];
}

function syncMatchBuilder() {
    mapSlots.innerHTML = '';

    // While loading, show a spinner and lock the builder
    if (mapsLoading) {
        mapSlots.innerHTML = '<div style="padding:20px;text-align:center;font-size:10px;color:var(--dim);letter-spacing:2px">⏳ CARGANDO MAPAS DISPONIBLES...</div>';
        btnSimPart.disabled = true;
        return;
    }

    const pickerDiv = document.createElement('div');
    pickerDiv.className = 'map-quick-picker';
    // Usar solo mapas con datos en la DB; si por alguna razón no cargaron, no mostrar nada
    const mapsToShow = availableMaps
        ? MAPS_LIST.filter(m => availableMaps.includes(m))
        : [];
    mapsToShow.forEach(m => {
        const used = matchMaps.some(mm => mm.map_name === m);
        const full = matchMaps.length >= 5;
        const tile = document.createElement('button');
        tile.className = `mqp-tile${used ? ' mqp-used' : ''}${(!used && full) ? ' mqp-full' : ''}`;
        tile.innerHTML = `
      <img class="mqp-img" src="../multimedia/maps/${m.toUpperCase()}.avif" alt="${m}" onerror="this.style.display='none'">
      <span class="mqp-name">${m.toUpperCase()}</span>`;
        tile.disabled = used || full;
        if (!used && !full) tile.addEventListener('click', () => {
            matchMaps.push({ map_name: m, a_starts_atk: true });
            syncMatchBuilder();
            updateBuilderState();
        });
        pickerDiv.appendChild(tile);
    });
    mapSlots.appendChild(pickerDiv);

    if (matchMaps.length > 0) {
        const queueDiv = document.createElement('div');
        queueDiv.className = 'map-queue';
        matchMaps.forEach((cfg, i) => {
            const isDecider = i === matchMaps.length - 1 && matchMaps.length >= 2;
            const abbrevA = selectedA ? (TEAM_ABBREV_CACHE[selectedA] || '?') : 'A';
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
          <button class="qi-side-btn${cfg.a_starts_atk ? ' qi-atk-active' : ''}" data-idx="${i}" data-side="atk">⚔ ATK</button>
          <button class="qi-side-btn${!cfg.a_starts_atk ? ' qi-def-active' : ''}" data-idx="${i}" data-side="def">🛡 DEF</button>
        </div>
        <button class="qi-remove" data-idx="${i}" title="Quitar">✕</button>`;
            queueDiv.appendChild(item);
        });
        mapSlots.appendChild(queueDiv);
    }

    mapSlots.querySelectorAll('.qi-side-btn').forEach(btn => {
        btn.addEventListener('click', e => {
            const idx = parseInt(e.currentTarget.dataset.idx);
            matchMaps[idx].a_starts_atk = (e.currentTarget.dataset.side === 'atk');
            syncMatchBuilder(); updateBuilderState();
        });
    });
    mapSlots.querySelectorAll('.qi-remove').forEach(btn => {
        btn.addEventListener('click', e => {
            matchMaps.splice(parseInt(e.currentTarget.dataset.idx), 1);
            syncMatchBuilder(); updateBuilderState();
        });
    });
    updateBuilderState();
}

function updateBuilderState() {
    const n = matchMaps.length;
    const formats = ['', 'Bo1', 'Bo2', 'Bo3', 'Bo4', 'Bo5'];
    mbFormat.textContent = n > 0 ? (formats[n] || `Bo${n}`) : '—';
    bspCount.textContent = `${n} mapa${n !== 1 ? 's' : ''}`;
    const ready = n > 0;
    btnSimPart.classList.toggle('ready', ready);
    btnSimPart.disabled = !ready;
}

btnAddMap.addEventListener('click', () => { matchMaps = []; syncMatchBuilder(); updateBuilderState(); });

// ─── SIMULAR PARTIDO ──────────────────────────────────────────────────────────
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
        'Analizando mapas configurados...',
        'Verificando lógica secuencial de serie...',
        `Ejecutando ${nSim.toLocaleString()} simulaciones de partido...`,
        'Calculando distribución de resultados...',
    ];
    let li = 0;
    document.getElementById('simProgressLabel').textContent = labels[0];
    const lInterval = setInterval(() => {
        if (li < labels.length - 1) document.getElementById('simProgressLabel').textContent = labels[++li];
    }, 700);

    try {
        const res = await fetch(`${API}/aletheia/partido`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                team_a: selectedA, team_b: selectedB,
                maps: matchMaps.filter(m => m.map_name),
                simulations: nSim,
            })
        });
        clearInterval(lInterval);
        const data = await res.json();

        if (!data.ok) {
            document.getElementById('simProgressLabel').textContent = `Error: ${data.error}`;
            document.getElementById('simProgressLabel').style.color = 'var(--red)';
            return;
        }

        simProgress.style.display = 'none';
        lastPartidoData = data;
        renderPartidoResults(data);

    } catch (e) {
        clearInterval(lInterval);
        document.getElementById('simProgressLabel').textContent = `Error: ${e.message}`;
        document.getElementById('simProgressLabel').style.color = 'var(--red)';
    } finally {
        btnSimPart.classList.remove('running');
        btnSimPart.classList.add('ready');
        btnSimPart.querySelector('.bsp-text').textContent = 'SIMULAR PARTIDO';
    }
}

// ─── RENDER PARTIDO RESULTS ───────────────────────────────────────────────────
function renderSeriesBanner(data) {
    const { series, summary, simulations } = data;
    const ta = summary.team_a;
    const tb = summary.team_b;
    const favA = series.win_a * 100 >= 55 ? 'winner-side' : '';
    const favB = series.win_b * 100 >= 55 ? 'winner-side-b' : '';
    const seqWarning = data.sequential_warning
        ? `<div style="grid-column:1/-1;text-align:center;font-size:9px;color:var(--orange);border:1px solid var(--orange);padding:6px 14px;background:rgba(251,146,60,.05);letter-spacing:1px">
        ⚠ ${data.sequential_warning}
      </div>` : '';

    document.getElementById('seriesBanner').innerHTML = `
    <div class="sb-team ${favA}">
      <div class="sb-name">${ta.name.toUpperCase()}</div>
      <div class="sb-abbrev">${ta.abbrev}</div>
      <div class="sb-pct">${Math.round(series.win_a * 100)}%</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
    <div class="sb-center">
      <div class="sb-format">${series.format}</div>
      <div class="sb-sims">${(simulations || nSim).toLocaleString()}<br>SIMULACIONES</div>
      <div style="font-size:10px;color:var(--dim);letter-spacing:1px;margin-top:4px">GANAR ${series.maps_to_win}</div>
    </div>
    <div class="sb-team ${favB}" style="text-align:right;align-items:flex-end">
      <div class="sb-name">${tb.name.toUpperCase()}</div>
      <div class="sb-abbrev">${tb.abbrev}</div>
      <div class="sb-pct">${Math.round(series.win_b * 100)}%</div>
      <div class="sb-label">PROB. GANAR SERIE</div>
    </div>
    ${seqWarning}
  `;
}

function renderScoreDist(series) {
    const distItems = Object.entries(series.score_dist).sort((a, b) => b[1] - a[1]);
    const maxPct = Math.max(...distItems.map(([, v]) => v));
    const barsHtml = distItems.map(([score, prob]) => {
        const [wa, wb] = score.split('-').map(Number);
        const cls = wa > wb ? 'win-a' : wb > wa ? 'win-b' : 'draw';
        const barH = Math.round((prob / maxPct) * 44);
        return `<div class="sd-item">
      <div class="sd-bar-wrap"><div class="sd-bar ${cls}" style="height:${barH}px"></div></div>
      <div class="sd-score">${score}</div>
      <div class="sd-pct">${Math.round(prob * 100)}%</div>
    </div>`;
    }).join('');

    document.getElementById('scoreDistWrap').innerHTML = `
    <div class="sd-title">DISTRIBUCIÓN DE RESULTADOS</div>
    <div class="sd-bars">${barsHtml}</div>
  `;
}

function buildMapRowHtml(r, i, ta, tb) {
    const winA = r.win_a * 100;
    const barColor = winA >= 60 ? 'pm-bar-green' : winA >= 40 ? 'pm-bar-yellow' : 'pm-bar-red';
    const pctClass = winA >= 55 ? 'pm-pct-a' : winA <= 45 ? 'pm-pct-b' : 'pm-pct-even';
    const confN = { high: 3, medium: 2, low: 1 }[r.confidence] || 1;
    let dots = '';
    for (let d = 0; d < 3; d++) dots += `<span class="conf-dot ${d < confN ? r.confidence : ''}"></span>`;
    const sideLabel = r.a_starts_atk
        ? `<span class="pm-start-atk">⚔ ${ta.abbrev} EMPIEZA ATK</span>`
        : `<span class="pm-start-def">🛡 ${ta.abbrev} EMPIEZA DEF</span>`;
    const otPct = Math.round((r.ot_pct || 0) * 100);
    const scoreA = r.avg_score_a != null ? r.avg_score_a.toFixed(1) : '—';
    const scoreB = r.avg_score_b != null ? r.avg_score_b.toFixed(1) : '—';
    const htA = r.halftime ? r.halftime.pred_a : '—';
    const htB = r.halftime ? r.halftime.pred_b : '—';
    const htEco = r.halftime && r.halftime.eco_note ? `<span style="color:var(--orange);font-size:9px;margin-left:6px">⚠ ${r.halftime.eco_note}</span>` : '';

    let scoreFreqHtml = '';
    if (r.score_freq) {
        const freqEntries = Object.entries(r.score_freq).slice(0, 4);
        const maxF = Math.max(...freqEntries.map(([, v]) => v));
        scoreFreqHtml = freqEntries.map(([score, prob]) => {
            const [sa, sb] = score.split('-').map(Number);
            const cls = sa > sb ? 'sf-bar-a' : 'sf-bar-b';
            const barW = Math.round((prob / maxF) * 100);
            return `<div class="sf-item">
        <span class="sf-score">${score}</span>
        <div class="sf-bar-track"><div class="sf-bar ${cls}" style="width:${barW}%"></div></div>
        <span class="sf-pct">${Math.round(prob * 100)}%</span>
      </div>`;
        }).join('');
    }

    return `
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
      <div style="font-size:9px;color:var(--dim);margin-top:2px">
        MEDIO TIEMPO ≈ <span style="color:var(--accent)">${htA}</span> — <span style="color:var(--blue)">${htB}</span>${htEco}
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
      <div class="pm-bar-track"><div class="pm-bar-fill ${barColor}" style="width:${Math.round(winA)}%"></div></div>
      <div class="pm-score-freq">${scoreFreqHtml}</div>
      <div class="pm-meta">
        <span class="pm-round-info">rnd ATK ${Math.round(r.p_round_atk * 100)}% · DEF ${Math.round(r.p_round_def * 100)}%</span>
        <div class="pm-conf-dots" title="Confianza: ${r.confidence}">${dots}</div>
      </div>
    </div>
    <div class="pm-agents-toggle-wrap">
      <button class="pm-agents-toggle" data-map-idx="${i}">
        <span class="pm-agents-icon">⚙</span> AGENTES
      </button>
    </div>`;
}

function renderPartidoResults(data) {
    const { series, map_results, summary, simulations } = data;
    const ta = summary.team_a;
    const tb = summary.team_b;

    renderSeriesBanner(data);
    renderScoreDist(series);

    document.getElementById('pmTeamAName').textContent = ta.abbrev;
    const mapsList = document.getElementById('partidoMapsList');
    mapsList.innerHTML = '';

    map_results.forEach((r, i) => {
        const row = document.createElement('div');
        row.className = 'pm-row';
        row.id = `pm-row-${i}`;
        row.style.animationDelay = `${i * 0.06}s`;
        row.innerHTML = buildMapRowHtml(r, i, ta, tb);

        const agentPanel = document.createElement('div');
        agentPanel.className = 'pm-agent-panel';
        agentPanel.id = `pm-agent-panel-${i}`;
        agentPanel.style.display = 'none';
        row.appendChild(agentPanel);

        mapsList.appendChild(row);
    });

    mapsList.querySelectorAll('.pm-agents-toggle').forEach(btn => {
        btn.addEventListener('click', e => {
            const idx = parseInt(e.currentTarget.dataset.mapIdx);
            toggleMapAgentPanel(idx, data);
        });
    });

    document.getElementById('partidoMethodNote').innerHTML = `
    <strong>Metodología:</strong> ${simulations.toLocaleString()} simulaciones Monte Carlo.
    Formato <strong>${series.format}</strong> — necesario ganar <strong>${series.maps_to_win}</strong> mapa(s).
    La lógica de serie es <strong>secuencial</strong>: para predecir 2-0, el equipo debe ganar mapa 1.
    <strong>⚙ Agentes</strong>: selecciona la composición por mapa y recalcula.
  `;

    partidoResults.style.display = 'block';
    partidoResults.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ─── PER-MAP AGENT SELECTION ──────────────────────────────────────────────────
async function toggleMapAgentPanel(mapIdx, data) {
    const panel = document.getElementById(`pm-agent-panel-${mapIdx}`);
    if (!panel) return;

    if (panel.style.display === 'block') {
        panel.style.display = 'none';
        return;
    }
    panel.style.display = 'block';

    const mapResult = data.map_results[mapIdx];
    const mapName = mapResult.map;
    const ta = data.summary.team_a;
    const tb = data.summary.team_b;

    panel.innerHTML = `<div class="pma-loading">Cargando jugadores de ${mapName.toUpperCase()}...</div>`;

    try {
        const [resA, resB] = await Promise.all([
            fetch(`${API}/aletheia/jugadores?team=${encodeURIComponent(ta.abbrev)}&map_name=${encodeURIComponent(mapName)}`),
            fetch(`${API}/aletheia/jugadores?team=${encodeURIComponent(tb.abbrev)}&map_name=${encodeURIComponent(mapName)}`),
        ]);
        const dA = await resA.json();
        const dB = await resB.json();
        const playersA = dA.ok ? dA.players : [];
        const playersB = dB.ok ? dB.players : [];

        renderMapAgentPanel(panel, mapIdx, mapName, playersA, playersB, ta.abbrev, tb.abbrev, data);
    } catch (err) {
        panel.innerHTML = `<div class="pma-loading" style="color:var(--red)">Error: ${err.message}</div>`;
    }
}

function renderMapAgentPanel(panel, mapIdx, mapName, playersA, playersB, abbA, abbB, data) {
    function playerCol(players, teamAbbrev, colorVar) {
        if (!players.length) return `<div class="pma-team-col"><div class="pma-team-hdr" style="color:var(--${colorVar})">${teamAbbrev}</div><div class="pma-no-data">Sin datos en ${mapName.toUpperCase()}</div></div>`;
        const rows = players.map((p, i) => {
            const agentOptions = (p.agents && p.agents.length ? p.agents : VALORANT_AGENTS)
                .map(a => `<option value="${a}" ${a === p.default_agent ? 'selected' : ''}>${a}${OPERATOR_AGENTS.has(a) ? ' ⚡OP' : ''}</option>`).join('');
            const isOp = OPERATOR_AGENTS.has(p.default_agent);
            return `<div class="pma-player-row ${isOp ? 'pma-has-op' : ''}" data-idx="${i}">
        <div class="pma-player-info">
          <span class="pma-name">${p.name}</span>
          <span class="pma-rating">${p.avg_rating.toFixed(2)}</span>
          ${isOp ? '<span class="pma-op-badge">OP</span>' : ''}
        </div>
        <select class="pma-agent-select ${isOp ? 'pma-op-agent' : ''}" data-team="${teamAbbrev}" data-idx="${i}">
          ${agentOptions}
        </select>
      </div>`;
        }).join('');
        return `<div class="pma-team-col"><div class="pma-team-hdr" style="color:var(--${colorVar})">${teamAbbrev} · ${mapName.toUpperCase()}</div>${rows}</div>`;
    }

    panel.innerHTML = `
    <div class="pma-header">
      <span class="pma-title">⚙ AGENTES · ${mapName.toUpperCase()}</span>
      <span class="pma-hint">Jett/Chamber ajustan impacto Operator</span>
    </div>
    <div class="pma-columns">
      ${playerCol(playersA, abbA, 'accent')}
      ${playerCol(playersB, abbB, 'blue')}
    </div>
    <div class="pma-actions">
      <button class="pma-recalc-btn" data-map-idx="${mapIdx}">
        <span class="pma-recalc-icon">🔄</span> RECALCULAR ${mapName.toUpperCase()}
      </button>
    </div>
  `;

    panel.querySelectorAll('.pma-agent-select').forEach(sel => {
        sel.addEventListener('change', e => {
            const row = e.target.closest('.pma-player-row');
            const isOp = OPERATOR_AGENTS.has(e.target.value);
            row.classList.toggle('pma-has-op', isOp);
            e.target.classList.toggle('pma-op-agent', isOp);
            const badge = row.querySelector('.pma-op-badge');
            if (isOp && !badge) row.querySelector('.pma-player-info').insertAdjacentHTML('beforeend', '<span class="pma-op-badge">OP</span>');
            else if (!isOp && badge) badge.remove();
        });
    });

    panel.querySelector('.pma-recalc-btn').addEventListener('click', () => {
        recalcMapWithAgents(mapIdx, panel, playersA, playersB, abbA, abbB, data);
    });
}

async function recalcMapWithAgents(mapIdx, panel, playersA, playersB, abbA, abbB, data) {
    const mapResult = data.map_results[mapIdx];
    const btn = panel.querySelector('.pma-recalc-btn');
    btn.disabled = true;
    btn.innerHTML = `<span class="pma-recalc-icon">⏳</span> RECALCULANDO...`;

    const overrides = { team_a: [], team_b: [] };
    panel.querySelectorAll('.pma-agent-select').forEach(sel => {
        const team = sel.dataset.team;
        const idx = parseInt(sel.dataset.idx);
        const roster = team === abbA ? playersA : playersB;
        const player = roster[idx];
        if (player) {
            const side = team === abbA ? 'team_a' : 'team_b';
            overrides[side].push({ player: player.name, agent: sel.value });
        }
    });

    try {
        const res = await fetch(`${API}/aletheia/recalcular_mapa`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                team_a: selectedA, team_b: selectedB,
                map_name: mapResult.map,
                a_starts_atk: mapResult.a_starts_atk,
                simulations: nSim,
                agent_overrides: overrides,
            })
        });
        const resData = await res.json();

        if (!resData.ok) {
            btn.innerHTML = `<span class="pma-recalc-icon">❌</span> Error`;
            btn.disabled = false;
            return;
        }

        // Update stored data
        data.map_results[mapIdx] = resData.result;

        // Re-render this map row content (preserve the agent panel)
        const row = document.getElementById(`pm-row-${mapIdx}`);
        const ta = data.summary.team_a;
        const tb = data.summary.team_b;

        // Remove everything except agent panel, then re-add
        const savedPanel = panel.cloneNode(true);
        row.innerHTML = buildMapRowHtml(resData.result, mapIdx, ta, tb);
        row.appendChild(savedPanel);

        // Re-wire the new toggle button
        row.querySelector('.pm-agents-toggle').addEventListener('click', () => {
            toggleMapAgentPanel(mapIdx, data);
        });

        // Re-wire restored panel events
        const restoredPanel = row.querySelector('.pm-agent-panel');
        restoredPanel.style.display = 'block';
        restoredPanel.querySelectorAll('.pma-agent-select').forEach(sel => {
            sel.addEventListener('change', e => {
                const prow = e.target.closest('.pma-player-row');
                const isOp = OPERATOR_AGENTS.has(e.target.value);
                prow.classList.toggle('pma-has-op', isOp);
                e.target.classList.toggle('pma-op-agent', isOp);
                const badge = prow.querySelector('.pma-op-badge');
                if (isOp && !badge) prow.querySelector('.pma-player-info').insertAdjacentHTML('beforeend', '<span class="pma-op-badge">OP</span>');
                else if (!isOp && badge) badge.remove();
            });
        });
        restoredPanel.querySelector('.pma-recalc-btn').addEventListener('click', () => {
            recalcMapWithAgents(mapIdx, restoredPanel, playersA, playersB, abbA, abbB, data);
        });

        // Flash effect
        row.classList.add('pm-row-updated');
        setTimeout(() => row.classList.remove('pm-row-updated'), 1500);

        const recalcBtn = restoredPanel.querySelector('.pma-recalc-btn');
        recalcBtn.innerHTML = `<span class="pma-recalc-icon">✅</span> ACTUALIZADO`;
        recalcBtn.disabled = false;
        setTimeout(() => {
            recalcBtn.innerHTML = `<span class="pma-recalc-icon">🔄</span> RECALCULAR ${mapResult.map.toUpperCase()}`;
        }, 1200);

        // Recalculate series
        await recalcSeriesFromData(data);

    } catch (e) {
        btn.innerHTML = `<span class="pma-recalc-icon">❌</span> Error`;
        btn.disabled = false;
    }
}

async function recalcSeriesFromData(data) {
    const mapProbs = data.map_results.map(r => r.win_a);
    try {
        const res = await fetch(`${API}/aletheia/recalcular_serie`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ map_probs: mapProbs, simulations: nSim })
        });
        const resData = await res.json();
        if (resData.ok) {
            data.series = resData.series;
            data.simulations = nSim;
            renderSeriesBanner(data);
            renderScoreDist(resData.series);
        }
    } catch { }
}

// ─── INIT ─────────────────────────────────────────────────────────────────────
loadTeams();
