const API = 'http://localhost:5000/api';

// ─── CACHE ───────────────────────────────────────────────────────────────────
const cache = {};

// ─── LOADING ─────────────────────────────────────────────────────────────────
const overlay = document.getElementById('loadingOverlay');
const showLoad = () => overlay.classList.remove('hidden');
const hideLoad = () => overlay.classList.add('hidden');

// ─── TABS ─────────────────────────────────────────────────────────────────────
document.querySelectorAll('.nav-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
        btn.classList.add('active');
        const id = btn.dataset.tab;
        document.getElementById(`tab-${id}`).classList.add('active');
        loadTab(id);
    });
});

async function loadTab(tab) {
    if (cache[tab]) return; // ya cargado
    showLoad();
    try {
        switch (tab) {
            case 'partidos': await loadPartidos(); break;
            case 'jugadores': await loadJugadores(); break;
            case 'mapas': await loadMapas(); break;
            case 'economia': await loadEconomia(); break;
            case 'agentes': await loadAgentes(); break;
        }
        cache[tab] = true;
    } catch (e) {
        console.error(e);
    } finally {
        hideLoad();
    }
}

// ─── HELPERS ─────────────────────────────────────────────────────────────────
async function fetchAPI(endpoint) {
    const res = await fetch(`${API}/${endpoint}`);
    const data = await res.json();
    if (!data.ok) throw new Error(data.error);
    return data.data;
}

function colorRating(v) {
    v = parseFloat(v);
    if (v >= 1.3) return 'val-high';
    if (v >= 1.0) return 'val-mid';
    return 'val-low';
}
function colorWR(v) {
    v = parseFloat(v);
    if (v >= 60) return 'val-high';
    if (v >= 45) return 'val-mid';
    return 'val-low';
}

function searchFilter(inputId, rows, keys) {
    document.getElementById(inputId).addEventListener('input', function () {
        const q = this.value.toLowerCase();
        rows.forEach(tr => {
            const text = keys.map(k => tr.dataset[k] || '').join(' ').toLowerCase();
            tr.style.display = text.includes(q) ? '' : 'none';
        });
    });
}

// Ordenamiento de tabla
function makeSortable(tableId, data, renderFn) {
    const table = document.getElementById(tableId);
    let sortCol = null, sortDir = 1;

    table.querySelectorAll('th[data-sort]').forEach(th => {
        th.addEventListener('click', () => {
            const col = th.dataset.sort;
            if (sortCol === col) sortDir *= -1;
            else { sortCol = col; sortDir = -1; }

            table.querySelectorAll('th').forEach(t => t.classList.remove('sorted-asc', 'sorted-desc'));
            th.classList.add(sortDir === 1 ? 'sorted-asc' : 'sorted-desc');

            const sorted = [...data].sort((a, b) => {
                const av = parseFloat(a[col]) || 0;
                const bv = parseFloat(b[col]) || 0;
                return (av - bv) * sortDir;
            });
            renderFn(sorted);
        });
    });
}

// ─── PARTIDOS ────────────────────────────────────────────────────────────────
async function loadPartidos() {
    const data = await fetchAPI('matches');
    document.getElementById('count-partidos').textContent = `(${data.length})`;
    renderPartidos(data);
    makeSortable('tbl-partidos', data, renderPartidos);

    const rows = document.querySelectorAll('#tbl-partidos tbody tr');
    searchFilter('search-partidos', rows, ['search']);
}

function renderPartidos(data) {
    const tbody = document.querySelector('#tbl-partidos tbody');
    tbody.innerHTML = '';
    data.forEach(r => {
        const scoreA = parseInt(r.score_a);
        const scoreB = parseInt(r.score_b);
        const tr = document.createElement('tr');
        tr.dataset.search = `${r.team_a} ${r.team_b} ${r.phase} ${r.winner}`.toLowerCase();
        tr.innerHTML = `
      <td class="val-accent">${r.match_id}</td>
      <td>${r.match_date || '—'}</td>
      <td style="color:var(--dim);font-size:11px">${r.phase}</td>
      <td><span class="team-tag">${r.team_a}</span></td>
      <td class="score-cell">${scoreA} : ${scoreB}</td>
      <td><span class="team-tag">${r.team_b}</span></td>
      <td><span class="winner-tag">${r.winner}</span></td>
      <td style="color:var(--dim)">${r.maps_played}</td>
      <td style="color:var(--dim);font-size:11px">${r.patch || '—'}</td>
    `;
        tbody.appendChild(tr);
    });
}

// ─── JUGADORES ───────────────────────────────────────────────────────────────
async function loadJugadores() {
    const data = await fetchAPI('player-stats');
    document.getElementById('count-jugadores').textContent = `(${data.length})`;
    renderJugadores(data);
    makeSortable('tbl-jugadores', data, renderJugadores);

    setTimeout(() => {
        const rows = document.querySelectorAll('#tbl-jugadores tbody tr');
        searchFilter('search-jugadores', rows, ['search']);
    }, 50);
}

function renderJugadores(data) {
    const tbody = document.querySelector('#tbl-jugadores tbody');
    tbody.innerHTML = '';
    data.forEach(r => {
        const tr = document.createElement('tr');
        tr.dataset.search = `${r.player_name} ${r.team_name}`.toLowerCase();
        const kd = r.total_deaths > 0 ? (r.total_kills / r.total_deaths).toFixed(2) : r.total_kills;
        tr.innerHTML = `
      <td class="val-accent">${r.player_name}</td>
      <td><span class="team-tag">${r.team_name}</span></td>
      <td style="color:var(--dim)">${r.matches}</td>
      <td class="${colorRating(r.avg_rating)}">${r.avg_rating}</td>
      <td>${r.avg_acs}</td>
      <td class="val-high">${r.total_kills}</td>
      <td class="val-low">${r.total_deaths}</td>
      <td style="color:var(--dim)">${r.total_assists}</td>
      <td>${r.avg_hs}%</td>
      <td>${r.avg_adr}</td>
      <td>${r.avg_kast}%</td>
      <td style="color:var(--green)">${r.total_fk}</td>
      <td style="color:var(--accent2)">${r.total_fd}</td>
    `;
        tbody.appendChild(tr);
    });
}

// ─── MAPAS ───────────────────────────────────────────────────────────────────
async function loadMapas() {
    const [maps, rounds] = await Promise.all([
        fetchAPI('maps-stats'),
        fetchAPI('rounds-stats'),
    ]);

    const maxPlayed = Math.max(...maps.map(m => m.times_played));
    const container = document.getElementById('cards-mapas');
    container.innerHTML = '';

    maps.forEach(m => {
        const pct = Math.round((m.times_played / maxPlayed) * 100);
        const atkPct = m.attack_chosen + m.defense_chosen > 0
            ? Math.round(m.attack_chosen / (m.attack_chosen + m.defense_chosen) * 100)
            : 50;

        const card = document.createElement('div');
        card.className = 'map-card';
        card.innerHTML = `
      <div class="map-card-name">${m.map_name || '—'}</div>
      <div class="map-card-stat"><span class="label">JUGADO</span><span class="value">${m.times_played}x</span></div>
      <div class="map-card-stat"><span class="label">PICK A / B</span><span class="value">${m.picked_by_a} / ${m.picked_by_b}</span></div>
      <div class="map-card-stat"><span class="label">DECIDER</span><span class="value">${m.as_decider}</span></div>
      <div class="map-card-stat"><span class="label">ATK ELEGIDO</span><span class="value" style="color:var(--orange)">${m.attack_chosen}</span></div>
      <div class="map-card-stat"><span class="label">DEF ELEGIDO</span><span class="value" style="color:var(--blue)">${m.defense_chosen}</span></div>
      <div class="map-card-stat"><span class="label">AVG RONDAS</span><span class="value">${m.avg_rounds}</span></div>
      <div class="map-card-bar">
        <div class="map-card-bar-fill" style="width:${pct}%"></div>
      </div>
    `;
        container.appendChild(card);
    });

    // Rounds stats cards
    const rContainer = document.getElementById('cards-rounds');
    rContainer.innerHTML = '';

    // Agrupar por resultado
    const byResult = {};
    rounds.forEach(r => {
        const key = r.result_type;
        if (!byResult[key]) byResult[key] = { total: 0, attack: 0, defense: 0 };
        byResult[key].total += parseInt(r.total);
        if (r.winning_side === 'attack') byResult[key].attack += parseInt(r.total);
        if (r.winning_side === 'defense') byResult[key].defense += parseInt(r.total);
    });

    const colors = { elim: 'var(--accent2)', defuse: 'var(--blue)', detonation: 'var(--orange)', time: 'var(--purple)' };
    const totalRounds = Object.values(byResult).reduce((s, v) => s + v.total, 0);

    Object.entries(byResult).sort((a, b) => b[1].total - a[1].total).forEach(([type, vals]) => {
        const pct = ((vals.total / totalRounds) * 100).toFixed(1);
        const atkPct = vals.total > 0 ? ((vals.attack / vals.total) * 100).toFixed(0) : 0;
        const card = document.createElement('div');
        card.className = 'round-card';
        card.innerHTML = `
      <div class="round-card-label">${type.toUpperCase()}</div>
      <div class="round-card-value" style="color:${colors[type] || 'var(--text)'}">
        ${vals.total.toLocaleString()}
      </div>
      <div class="round-card-pct">${pct}% del total</div>
      <div class="round-card-pct" style="margin-top:6px">
        <span style="color:var(--orange)">ATK ${atkPct}%</span>
        &nbsp;·&nbsp;
        <span style="color:var(--blue)">DEF ${100 - parseInt(atkPct)}%</span>
      </div>
    `;
        rContainer.appendChild(card);
    });
}

// ─── ECONOMÍA ────────────────────────────────────────────────────────────────
async function loadEconomia() {
    const data = await fetchAPI('economy');
    document.getElementById('count-economia').textContent = `(${data.length})`;
    renderEconomia(data);

    const rows = document.querySelectorAll('#tbl-economia tbody tr');
    searchFilter('search-economia', rows, ['search']);
}

function renderEconomia(data) {
    const tbody = document.querySelector('#tbl-economia tbody');
    tbody.innerHTML = '';
    data.forEach(r => {
        const tr = document.createElement('tr');
        tr.dataset.search = r.team.toLowerCase();
        tr.innerHTML = `
      <td><span class="team-tag">${r.team}</span></td>
      <td style="color:var(--dim)">${r.maps}</td>
      <td style="color:var(--accent)">${r.pistol_won}</td>
      <td class="${colorWR(r.eco_wr)}">${r.eco_wr ?? '—'}%</td>
      <td class="${colorWR(r.semi_eco_wr)}">${r.semi_eco_wr ?? '—'}%</td>
      <td class="${colorWR(r.semi_buy_wr)}">${r.semi_buy_wr ?? '—'}%</td>
      <td class="${colorWR(r.full_buy_wr)}">${r.full_buy_wr ?? '—'}%</td>
      <td style="color:var(--dim)">${r.eco_played}/${r.eco_won}</td>
      <td style="color:var(--dim)">${r.semi_eco_p}/${r.semi_eco_w}</td>
      <td style="color:var(--dim)">${r.semi_buy_p}/${r.semi_buy_w}</td>
      <td style="color:var(--dim)">${r.full_buy_p}/${r.full_buy_w}</td>
    `;
        tbody.appendChild(tr);
    });
}

// ─── AGENTES ─────────────────────────────────────────────────────────────────
async function loadAgentes() {
    const data = await fetchAPI('agents');
    const maxPicks = Math.max(...data.map(a => a.picks));
    const container = document.getElementById('agents-grid');
    container.innerHTML = '';

    data.forEach(a => {
        const barW = Math.round((a.picks / maxPicks) * 100);
        const card = document.createElement('div');
        card.className = 'agent-card';
        card.innerHTML = `
      <div class="picks-bar" style="width:${barW}%"></div>
      <div class="agent-name">${a.agent}</div>
      <div class="agent-stat"><span class="label">RATING</span><span class="${colorRating(a.avg_rating)}">${a.avg_rating}</span></div>
      <div class="agent-stat"><span class="label">ACS</span><span>${a.avg_acs}</span></div>
      <div class="agent-stat"><span class="label">HS%</span><span>${a.avg_hs}%</span></div>
      <div class="agent-stat"><span class="label">KAST</span><span>${a.avg_kast}%</span></div>
      <div class="agent-picks">${a.picks} picks</div>
    `;
        container.appendChild(card);
    });
}

// ─── INIT ─────────────────────────────────────────────────────────────────────
loadTab('partidos');