/* ALETHEIA — EQUIPOS: grid de equipos + detalle con tabs */

const estado = { q: '', region: '' };

const gridEl = document.getElementById('grid');
const countEl = document.getElementById('count');

/* ── LISTA ──────────────────────────────────────────────────────────────── */
async function cargarLista() {
    VCT.loading(true);
    const qs = new URLSearchParams();
    if (estado.q) qs.set('q', estado.q);
    if (estado.region) qs.set('region', estado.region);
    try {
        const d = await VCT.api(`/equipos?${qs}`);
        countEl.textContent = `${d.data.length} EQUIPOS`;
        pintarChips(d.regiones);
        pintarGrid(d.data);
    } catch (e) {
        VCT.showError(gridEl, e);
    } finally {
        VCT.loading(false);
    }
}

function pintarChips(regiones) {
    const cont = document.getElementById('chips-region');
    cont.innerHTML = `<button class="v-chip ${estado.region ? '' : 'active'}" data-r="">TODAS</button>` +
        regiones.map(r => `<button class="v-chip ${estado.region === r.region ? 'active' : ''}" data-r="${VCT.esc(r.region)}">${VCT.esc(r.region)}</button>`).join('');
    cont.querySelectorAll('.v-chip').forEach(c => c.addEventListener('click', () => {
        estado.region = c.dataset.r;
        cargarLista();
    }));
}

function pintarGrid(equipos) {
    if (!equipos.length) {
        gridEl.innerHTML = VCT.empty('Sin equipos.');
        return;
    }
    gridEl.innerHTML = equipos.map(t => {
        const wr = t.matches ? Math.round(t.wins * 100 / t.matches) : 0;
        const losses = t.matches - t.wins;
        return `<a class="v-card v-team-grid-card" href="index.html?team=${t.team_id}" style="--c:${VCT.teamColor(t.team_name)}">
            <div class="v-card-head">
                ${VCT.lozenge(t.team_name, t.tag)}
                <div style="min-width:0">
                    <h3>${VCT.esc(t.team_name)}</h3>
                    <div class="v-card-sub">${t.region ? VCT.esc(t.region) : '—'} ${VCT.flagHtml(t.country)}</div>
                </div>
            </div>
            <div class="v-card-row"><span class="label">RÉCORD</span><span class="record">${t.wins}<small> - </small>${losses}</span></div>
            <div class="v-card-row"><span class="label">WINRATE</span><span class="${VCT.wrClass(wr)}">${wr}%</span></div>
            <div class="v-card-row"><span class="label">PARTIDOS</span><span>${t.matches}</span></div>
            <div class="v-card-row"><span class="label">ÚLTIMO</span><span>${VCT.fmtDate(t.last_date)}</span></div>
            ${VCT.bar(wr, wr >= 55 ? 'green' : (wr >= 45 ? '' : 'red'))}
        </a>`;
    }).join('');
}

/* ── DETALLE ────────────────────────────────────────────────────────────── */
function toggleVistas(detalle) {
    document.getElementById('vistaLista').hidden = detalle;
    document.getElementById('vistaDetalle').hidden = !detalle;
}

async function cargarDetalle(tid) {
    toggleVistas(true);
    VCT.loading(true);
    try {
        const d = await VCT.api(`/equipo/${tid}`);
        pintarDetalle(d);
    } catch (e) {
        VCT.showError(document.getElementById('vistaDetalle'), e);
    } finally {
        VCT.loading(false);
    }
}

function rosterCards(roster) {
    if (!roster.length) return VCT.empty('Sin roster registrado.');
    return `<div class="v-roster">${roster.map(p => `
        <a class="v-player-card" href="${VCT.playerHref(p.player_id)}">
            <span class="v-avatar">${VCT.esc(VCT.initials(p.nickname))}</span>
            <span style="min-width:0">
                <span class="nick">${VCT.flagHtml(p.country)} ${VCT.esc(p.nickname)}</span><br />
                <span class="real">${VCT.esc(p.real_name || '')}</span>
            </span>
        </a>`).join('')}</div>`;
}

function transTable(tx) {
    if (!tx.length) return VCT.empty('Sin transacciones registradas.');
    return `<div class="v-table-wrap" style="overflow-x:auto"><table class="v-table">
        <thead><tr><th>MOVIMIENTO</th><th>JUGADOR</th><th>FECHA</th></tr></thead>
        <tbody>${tx.map(t => `
            <tr>
                <td><span class="v-tx ${VCT.esc(t.action)}">${VCT.esc(t.action)}</span></td>
                <td>${VCT.playerCell(t.player_id, t.nickname) || VCT.esc(t.real_name || '—')}</td>
                <td class="muted">${VCT.fmtDate(t.transaction_date)}</td>
            </tr>`).join('')}
        </tbody>
    </table></div>`;
}

function jugadoresTable(rows) {
    if (!rows.length) return VCT.empty('Sin estadísticas de jugadores.');
    return `<div style="overflow-x:auto"><table class="v-table">
        <thead><tr>
            <th>JUGADOR</th><th class="num">MAPS</th><th class="num">R</th><th class="num">ACS</th>
            <th class="num">K/D/A</th><th class="num">KAST</th><th class="num">ADR</th>
            <th class="num">HS%</th><th class="num">FK/FD</th>
        </tr></thead>
        <tbody>${rows.map(j => `
            <tr>
                <td>${VCT.flagHtml(j.country)} ${VCT.playerCell(j.player_id, j.nickname)}</td>
                <td class="num muted">${j.matches}</td>
                <td class="num ${VCT.ratingClass(j.rating)}">${VCT.fmt(j.rating)}</td>
                <td class="num">${j.acs ?? '—'}</td>
                <td class="num"><span class="v-val-high">${j.kills}</span> / <span class="v-val-low">${j.deaths}</span> / ${j.assists}</td>
                <td class="num">${VCT.pct(j.kast, 0)}</td>
                <td class="num">${VCT.fmt(j.adr, 1)}</td>
                <td class="num">${VCT.pct(j.hs_percent, 0)}</td>
                <td class="num">${j.fk}/${j.fd}</td>
            </tr>`).join('')}
        </tbody>
    </table></div>`;
}

function mapasTable(rows) {
    if (!rows.length) return VCT.empty('Sin mapas jugados.');
    return `<div style="overflow-x:auto"><table class="v-table">
        <thead><tr><th>MAPA</th><th class="num">JUGADOS</th><th class="num">V</th><th class="num">D</th><th class="num">WR</th><th style="width:140px"></th><th class="num">AVG RONDAS</th></tr></thead>
        <tbody>${rows.map(m => {
            const wr = m.played ? Math.round(m.wins * 100 / m.played) : 0;
            return `<tr>
                <td>${VCT.esc(m.map_name || '—')}</td>
                <td class="num muted">${m.played}</td>
                <td class="num v-val-high">${m.wins}</td>
                <td class="num v-val-low">${m.played - m.wins}</td>
                <td class="num ${VCT.wrClass(wr)}">${wr}%</td>
                <td>${VCT.bar(wr, wr >= 55 ? 'green' : (wr >= 45 ? '' : 'red'))}</td>
                <td class="num muted">${VCT.fmt(m.avg_rounds, 1)}</td>
            </tr>`;
        }).join('')}
        </tbody>
    </table></div>`;
}

function eventosTable(rows) {
    if (!rows.length) return VCT.empty('Sin eventos jugados.');
    return `<div style="overflow-x:auto"><table class="v-table">
        <thead><tr><th>EVENTO</th><th class="num">PARTIDOS</th><th class="num">V</th><th class="num">WR</th><th>FECHAS</th></tr></thead>
        <tbody>${rows.map(ev => {
            const wr = ev.matches ? Math.round(ev.wins * 100 / ev.matches) : 0;
            return `<tr>
                <td><a class="v-event-link" href="${VCT.eventHref(ev.event_id, ev.tournament)}">${VCT.esc(ev.tournament)}</a></td>
                <td class="num muted">${ev.matches}</td>
                <td class="num v-val-high">${ev.wins}</td>
                <td class="num ${VCT.wrClass(wr)}">${wr}%</td>
                <td class="muted">${VCT.fmtDate(ev.start_date)} → ${VCT.fmtDate(ev.end_date)}</td>
            </tr>`;
        }).join('')}
        </tbody>
    </table></div>`;
}

function pintarDetalle(d) {
    const t = d.equipo;
    const r = d.record || {};
    const losses = (r.matches || 0) - (r.wins || 0);
    const wr = r.matches ? Math.round(r.wins * 100 / r.matches) : 0;

    const el = document.getElementById('vistaDetalle');
    el.innerHTML = `
        <a class="v-back" href="index.html">← VOLVER A EQUIPOS</a>

        <div class="v-banner" style="--c:${VCT.teamColor(t.team_name)}">
            ${VCT.lozenge(t.team_name, t.tag, 'big')}
            <div class="v-banner-main">
                <h1>${VCT.esc(t.team_name)}</h1>
                <div class="v-meta">
                    ${t.tag ? `<span>${VCT.esc(t.tag)}</span>` : ''}
                    <span>${VCT.flagHtml(t.country)} ${VCT.esc(t.country || '—')}</span>
                    <span>${VCT.esc(t.region || '—')}</span>
                    ${t.url ? `<a href="${VCT.esc(t.url)}" target="_blank" rel="noopener">VLR.GG ↗</a>` : ''}
                </div>
            </div>
            <div class="v-record">
                <div class="v-vs-score">${r.wins ?? 0}<small> - </small>${losses}</div>
                <div class="v-sub">${wr}% WR · ${r.matches ?? 0} PARTIDOS</div>
            </div>
        </div>

        <div class="v-tabs" style="margin-top:18px">
            <button data-tab="resumen" class="active">RESUMEN</button>
            <button data-tab="roster">ROSTER</button>
            <button data-tab="partidos">PARTIDOS</button>
            <button data-tab="stats">ESTADÍSTICAS</button>
        </div>

        <div class="v-tab-panel active" id="tab-resumen">
            <div class="v-panel">
                <div class="v-panel-title">ÚLTIMOS PARTIDOS <a class="v-hint" href="#partidos" data-goto="partidos" style="color:var(--blue);text-decoration:none">VER TODOS →</a></div>
                <div id="eq-recientes"></div>
            </div>
            <div class="v-panel">
                <div class="v-panel-title">ROSTER ACTUAL</div>
                ${rosterCards(d.roster)}
            </div>
            <div class="v-panel">
                <div class="v-panel-title">EVENTOS JUGADOS</div>
                ${eventosTable(d.eventos)}
            </div>
        </div>

        <div class="v-tab-panel" id="tab-roster">
            <div class="v-panel">
                <div class="v-panel-title">ROSTER ACTUAL <span class="v-hint">${d.roster.length} JUGADORES</span></div>
                ${rosterCards(d.roster)}
            </div>
            <div class="v-panel">
                <div class="v-panel-title">HISTORIAL DE MOVIMIENTOS <span class="v-hint">JOIN / LEAVE / INACTIVE</span></div>
                ${transTable(d.transacciones)}
            </div>
        </div>

        <div class="v-tab-panel" id="tab-partidos">
            <div class="v-panel">
                <div class="v-panel-title">PARTIDOS <span class="v-hint">${d.partidos.length} REGISTRADOS (MÁS RECIENTES PRIMERO)</span></div>
                <div id="eq-todos"></div>
            </div>
        </div>

        <div class="v-tab-panel" id="tab-stats">
            <div class="v-panel">
                <div class="v-panel-title">PROMEDIOS DE JUGADORES <span class="v-hint">TODAS LAS FASES</span></div>
                ${jugadoresTable(d.jugadores)}
            </div>
            <div class="v-panel">
                <div class="v-panel-title">RENDIMIENTO POR MAPA</div>
                ${mapasTable(d.mapas)}
            </div>
        </div>
    `;

    VCT.renderMatchList(document.getElementById('eq-recientes'), d.partidos.slice(0, 8));
    VCT.renderMatchList(document.getElementById('eq-todos'), d.partidos);
    VCT.tabs(el, tab => { if (tab === 'partidos') document.getElementById('eq-todos').scrollIntoView({ block: 'nearest' }); });
    el.querySelector('[data-goto]').addEventListener('click', e => {
        e.preventDefault();
        VCT.activateTab(e.target.dataset.goto);
    });
}

/* ── INIT ───────────────────────────────────────────────────────────────── */
(async function init() {
    VCT.bindLinks(document);
    const tid = VCT.param('team');
    if (tid) {
        await cargarDetalle(tid);
        return;
    }
    await cargarLista();
    document.getElementById('q').addEventListener('input', VCT.debounce(() => {
        estado.q = document.getElementById('q').value.trim();
        cargarLista();
    }));
})();
