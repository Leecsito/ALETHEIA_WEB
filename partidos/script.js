/* ALETHEIA — PARTIDOS: listado estilo vlr.gg + detalle del partido */

const estado = { page: 1, q: '', torneo: '', year: '', orden: 'recientes' };

const listaEl = document.getElementById('lista');
const pagerEl = document.getElementById('pager');
const countEl = document.getElementById('count');

/* ── LISTA ──────────────────────────────────────────────────────────────── */
async function cargarFiltros() {
    try {
        const d = await VCT.api('/partidos/filtros');
        document.getElementById('f-torneo').innerHTML =
            '<option value="">TODOS LOS TORNEOS</option>' +
            d.torneos.map(t => `<option value="${VCT.esc(t.tournament)}">${VCT.esc(t.tournament)} (${t.n})</option>`).join('');
        document.getElementById('f-year').innerHTML =
            '<option value="">TODOS LOS AÑOS</option>' +
            d.years.map(y => `<option value="${VCT.esc(y.year)}">${VCT.esc(y.year)} (${y.n})</option>`).join('');
    } catch (e) {
        console.error(e);
    }
}

async function cargarLista() {
    VCT.loading(true);
    const qs = new URLSearchParams({ page: estado.page, limit: 60, orden: estado.orden });
    if (estado.q) qs.set('q', estado.q);
    if (estado.torneo) qs.set('torneo', estado.torneo);
    if (estado.year) qs.set('year', estado.year);
    try {
        const d = await VCT.api(`/partidos?${qs}`);
        countEl.textContent = `${d.total} PARTIDOS`;
        VCT.renderMatchList(listaEl, d.data);
        pintarPager(d);
    } catch (e) {
        VCT.showError(listaEl, e);
        pagerEl.innerHTML = '';
    } finally {
        VCT.loading(false);
    }
}

function pintarPager(d) {
    let html = '';
    if (d.page > 1) html += `<button class="v-btn small" data-p="${d.page - 1}">← ANTERIOR</button>`;
    html += `<span class="v-page-info">PÁGINA ${d.page} DE ${d.pages}</span>`;
    if (d.page < d.pages) html += `<button class="v-btn small" data-p="${d.page + 1}">SIGUIENTE →</button>`;
    pagerEl.innerHTML = html;
    pagerEl.querySelectorAll('[data-p]').forEach(b => b.addEventListener('click', () => {
        estado.page = parseInt(b.dataset.p);
        cargarLista();
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }));
}

/* ── DETALLE ────────────────────────────────────────────────────────────── */
function toggleVistas(detalle) {
    document.getElementById('vistaLista').hidden = detalle;
    document.getElementById('vistaDetalle').hidden = !detalle;
}

async function cargarDetalle(mid) {
    toggleVistas(true);
    VCT.loading(true);
    try {
        const d = await VCT.api(`/partido/${mid}`);
        pintarDetalle(d);
    } catch (e) {
        VCT.showError(document.getElementById('vistaDetalle'), e);
    } finally {
        VCT.loading(false);
    }
}

function vetoItem(v) {
    const action = (v.action || '').toLowerCase();
    let cls = 'ban';
    if (action === 'decider') cls = 'decider';
    else if (action === 'pick') cls = v.team === 'b' ? 'pick-b' : 'pick-a';
    const who = action === 'decider' ? 'DECIDER' : `${action.toUpperCase()} ${(v.team || '').toUpperCase()}`;
    return `<div class="v-veto-item ${cls}">
        <span class="v-veto-map">${VCT.esc(v.map_name || '—')}</span>
        <small>${VCT.esc(who)}</small>
    </div>`;
}

function pintarDetalle(d) {
    const p = d.partido;
    const winA = p.winner_id && p.winner_id === p.team_a_id;
    const winB = p.winner_id && p.winner_id === p.team_b_id;
    const titulo = p.event_name || p.tournament || 'PARTIDO';

    const el = document.getElementById('vistaDetalle');
    el.innerHTML = `
        <a class="v-back" href="index.html">← VOLVER A PARTIDOS</a>

        <div class="v-banner">
            ${VCT.lozenge(p.team_a, p.team_a_tag, 'big')}
            <div class="v-banner-main">
                <h1>${VCT.esc(titulo)}</h1>
                <div class="v-meta">
                    <span>${VCT.esc(p.phase || '')}</span>
                    <span>${VCT.fmtDate(p.match_date)}</span>
                    ${p.patch ? `<span>PATCH ${VCT.esc(p.patch)}</span>` : ''}
                    <a href="${VCT.eventHref(p.event_id, p.tournament)}">VER EVENTO →</a>
                    <a href="https://www.vlr.gg/${p.match_id}" target="_blank" rel="noopener">VLR.GG ↗</a>
                </div>
            </div>
            <div class="v-vs">
                <a class="v-vs-team ${winA ? '' : 'lost'}" href="${VCT.teamHref(p.team_a_id)}">
                    ${VCT.lozenge(p.team_a, p.team_a_tag)}
                    <span class="v-team-name">${VCT.esc(p.team_a || 'TBD')}</span>
                </a>
                <span class="v-vs-score">${p.score_a ?? '-'}<small> : </small>${p.score_b ?? '-'}</span>
                <a class="v-vs-team away ${winB ? '' : 'lost'}" href="${VCT.teamHref(p.team_b_id)}">
                    ${VCT.lozenge(p.team_b, p.team_b_tag)}
                    <span class="v-team-name">${VCT.esc(p.team_b || 'TBD')}</span>
                </a>
            </div>
        </div>

        ${d.veto.length ? `
            <div class="v-panel" style="margin-top:2px">
                <div class="v-panel-title">VETO <span class="v-hint">ORDEN DE PICKS Y BANS</span></div>
                <div class="v-veto">${d.veto.map(vetoItem).join('')}</div>
            </div>` : ''}

        ${d.maps.length ? `
            <div style="margin-top:20px" class="v-map-tabs">
                ${d.maps.map((m, i) => {
                    const sa = (m.score_a_attack || 0) + (m.score_a_defense || 0);
                    const sb = (m.score_b_attack || 0) + (m.score_b_defense || 0);
                    return `<button class="v-map-btn" data-i="${i}">${VCT.esc(m.map_name)} <span class="v-map-score">${sa}:${sb}</span></button>`;
                }).join('')}
            </div>
            <div id="map-panel"></div>` : VCT.empty('Sin mapas registrados para este partido.')}
    `;

    const btns = el.querySelectorAll('.v-map-btn');
    btns.forEach((b, i) => b.addEventListener('click', () => {
        btns.forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        pintarMapa(d, i);
    }));
    if (d.maps.length) {
        btns[0].classList.add('active');
        pintarMapa(d, 0);
    }
}

function roundsStrip(m, p) {
    const rounds = m.rounds || [];
    if (!rounds.length) return '';
    const items = rounds.map(r => {
        const cls = r.winner_id === p.team_a_id ? 'a' : (r.winner_id === p.team_b_id ? 'b' : '');
        const ot = r.round_num > 24 ? ' ot' : '';
        const tipo = VCT.esc((r.result_type || '').toUpperCase());
        const side = VCT.esc((r.winning_side || '').toUpperCase());
        return `<span class="v-round ${cls}${ot}" title="Ronda ${r.round_num} · ${tipo} · ${side}">${r.round_num}</span>`;
    }).join('');
    return `
        <div class="v-rounds">${items}</div>
        <div class="v-round-legend">
            <span><i class="a"></i>${VCT.esc(p.team_a)}</span>
            <span><i class="b"></i>${VCT.esc(p.team_b)}</span>
            <span>PASA EL CURSOR SOBRE UNA RONDA PARA VER EL TIPO (ELIM / DEFUSE / DETONATION / TIME)</span>
        </div>`;
}

function econBlock(m, p) {
    const econ = m.economy || [];
    if (!econ.length) return '';
    const fila = (label, won, played, cls) => {
        const wr = played ? Math.round((won || 0) * 100 / played) : null;
        return `<div class="v-econ-row">
            <span class="label">${label}</span>
            ${VCT.bar(wr ?? 0, cls)}
            <span class="val">${won || 0}/${played || 0}${wr !== null ? ` · ${wr}%` : ''}</span>
        </div>`;
    };
    const bloque = (teamId, name) => {
        const e = econ.find(x => x.team_id === teamId);
        if (!e) return '';
        return `<div class="v-econ-team">
            <div class="v-econ-name">${VCT.esc(name)}</div>
            <div class="v-econ-row">
                <span class="label">PISTOL</span>
                ${VCT.bar((e.pistol_won || 0) * 50, 'purple')}
                <span class="val">${e.pistol_won || 0}</span>
            </div>
            ${fila('ECO', e.eco_won, e.eco_played, 'green')}
            ${fila('SEMI-ECO', e.semi_eco_won, e.semi_eco_played, 'green')}
            ${fila('SEMI-BUY', e.semi_buy_won, e.semi_buy_played, 'blue')}
            ${fila('FULL BUY', e.full_buy_won, e.full_buy_played, 'blue')}
        </div>`;
    };
    return `<div class="v-econ" style="margin-top:2px">${bloque(p.team_a_id, p.team_a)}${bloque(p.team_b_id, p.team_b)}</div>`;
}

function board(teamId, name, tag, score, players) {
    if (!players.length) return '';
    const rows = players.map(s => {
        const diff = (s.kills || 0) - (s.deaths || 0);
        return `<tr>
            <td>${s.agent ? `<span class="v-agent-chip">${VCT.esc(s.agent)}</span>` : '<span class="muted">—</span>'}</td>
            <td>${VCT.playerCell(s.player_id, s.nickname)}</td>
            <td class="num ${VCT.ratingClass(s.rating)}">${VCT.fmt(s.rating)}</td>
            <td class="num">${s.acs ?? '—'}</td>
            <td class="num">${s.kills ?? '—'}</td>
            <td class="num">${s.deaths ?? '—'}</td>
            <td class="num">${s.assists ?? '—'}</td>
            <td class="num ${VCT.kdColor(s.kills, s.deaths)}">${diff > 0 ? '+' : ''}${diff}</td>
            <td class="num">${VCT.pct(s.kast, 0)}</td>
            <td class="num">${VCT.fmt(s.adr, 1)}</td>
            <td class="num">${VCT.pct(s.hs_percent, 0)}</td>
            <td class="num">${s.fk ?? 0}</td>
            <td class="num">${s.fd ?? 0}</td>
        </tr>`;
    }).join('');

    return `<div class="v-board">
        <div class="v-board-head">
            <a href="${VCT.teamHref(teamId)}" style="text-decoration:none">${VCT.lozenge(name, tag)}</a>
            <a class="v-board-team" href="${VCT.teamHref(teamId)}" style="color:inherit;text-decoration:none">${VCT.esc(name)}</a>
            <span class="v-board-score">${score}</span>
        </div>
        <table class="v-table">
            <thead>
                <tr>
                    <th>AGENTE</th><th>JUGADOR</th>
                    <th class="num">R</th><th class="num">ACS</th><th class="num">K</th>
                    <th class="num">D</th><th class="num">A</th><th class="num">+/-</th>
                    <th class="num">KAST</th><th class="num">ADR</th><th class="num">HS%</th>
                    <th class="num">FK</th><th class="num">FD</th>
                </tr>
            </thead>
            <tbody>${rows}</tbody>
        </table>
    </div>`;
}

function pintarMapa(d, i) {
    const p = d.partido;
    const m = d.maps[i];
    const sa = (m.score_a_attack || 0) + (m.score_a_defense || 0);
    const sb = (m.score_b_attack || 0) + (m.score_b_defense || 0);
    const picker = m.picker === 'a' ? p.team_a : (m.picker === 'b' ? p.team_b : 'DECIDER');

    let stats = (m.players || []).slice();
    let teamA = stats.filter(s => s.team_id === p.team_a_id);
    let teamB = stats.filter(s => s.team_id === p.team_b_id);
    if (!teamA.length && !teamB.length && stats.length) {
        const half = Math.ceil(stats.length / 2);
        teamA = stats.slice(0, half);
        teamB = stats.slice(half);
    }
    const byRating = (a, b) => (b.rating || 0) - (a.rating || 0);
    teamA.sort(byRating);
    teamB.sort(byRating);

    document.getElementById('map-panel').innerHTML = `
        <div class="v-map-head">
            <h2>${VCT.esc(m.map_name)}</h2>
            <div class="v-info">
                <span class="v-map-picker">PICK <b>${VCT.esc(picker)}</b></span>
                ${m.side_chosen ? `<span>LADO ELEGIDO <b>${VCT.esc(m.side_chosen.toUpperCase())}</b></span>` : ''}
                ${m.side_top_start ? `<span>INICIO TOP <b>${VCT.esc(m.side_top_start.toUpperCase())}</b></span>` : ''}
                ${m.duration ? `<span>DURACIÓN <b>${VCT.esc(m.duration)}</b></span>` : ''}
            </div>
            <span class="v-vs-score" style="margin-left:auto">${sa}<small> : </small>${sb}</span>
        </div>
        ${roundsStrip(m, p)}
        ${econBlock(m, p)}
        <div class="v-map-panel">
            ${(teamA.length || teamB.length)
                ? `<div class="v-scoreboard-wrap">
                        ${board(p.team_a_id, p.team_a, p.team_a_tag, sa, teamA)}
                        ${board(p.team_b_id, p.team_b, p.team_b_tag, sb, teamB)}
                   </div>`
                : VCT.empty('Sin scoreboard para este mapa.')}
        </div>`;
}

/* ── INIT ───────────────────────────────────────────────────────────────── */
(async function init() {
    VCT.bindLinks(document);
    const mid = VCT.param('match');
    if (mid) {
        await cargarDetalle(mid);
        return;
    }
    await cargarFiltros();
    await cargarLista();

    document.getElementById('q').addEventListener('input', VCT.debounce(() => {
        estado.q = document.getElementById('q').value.trim();
        estado.page = 1;
        cargarLista();
    }));
    document.getElementById('f-torneo').addEventListener('change', function () {
        estado.torneo = this.value;
        estado.page = 1;
        cargarLista();
    });
    document.getElementById('f-year').addEventListener('change', function () {
        estado.year = this.value;
        estado.page = 1;
        cargarLista();
    });
    document.getElementById('f-orden').addEventListener('change', function () {
        estado.orden = this.value;
        estado.page = 1;
        cargarLista();
    });
})();
