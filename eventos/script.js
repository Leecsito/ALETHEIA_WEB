/* ALETHEIA — EVENTOS: lista de eventos/torneos + detalle con partidos, equipos, mapas y agentes */

const gridEl = document.getElementById('grid');
const countEl = document.getElementById('count');

/* ── LISTA ──────────────────────────────────────────────────────────────── */
async function cargarLista() {
    VCT.loading(true);
    try {
        const d = await VCT.api('/eventos');
        const q = (document.getElementById('q').value || '').trim().toLowerCase();
        const rows = q
            ? d.data.filter(e => (e.tournament || '').toLowerCase().includes(q) || (e.event_name || '').toLowerCase().includes(q))
            : d.data;
        countEl.textContent = `${rows.length} EVENTOS`;
        pintarGrid(rows);
        VCT.aplicarColores(gridEl);
    } catch (e) {
        VCT.showError(gridEl, e);
    } finally {
        VCT.loading(false);
    }
}

function estadoEvento(e) {
    const hoy = VCT.todayIso();
    if (e.end_date && e.end_date >= hoy && (!e.start_date || e.start_date <= hoy)) return { txt: 'EN CURSO', cls: 'live' };
    if (e.start_date && e.start_date > hoy) return { txt: 'PRÓXIMO', cls: 'next' };
    return { txt: 'FINALIZADO', cls: 'done' };
}

function linkEvento(e) {
    return e.event_id
        ? `index.html?event=${e.event_id}`
        : `index.html?torneo=${encodeURIComponent(e.tournament)}`;
}

function pintarGrid(rows) {
    if (!rows.length) {
        gridEl.innerHTML = VCT.empty('Sin eventos.');
        return;
    }
    gridEl.innerHTML = rows.map(e => {
        const st = estadoEvento(e);
        const cAttr = e.event_id
            ? `data-c-evento="${e.event_id}"`
            : `data-c-nombre="${VCT.esc(e.event_name || e.tournament)}"`;
        return `<a class="v-card v-event-card" href="${linkEvento(e)}" ${cAttr}>
            <div class="v-event-head">
                ${VCT.eventLogo(e.event_id, e.event_name || e.tournament)}
                <div class="v-event-info">
                    <h3>${VCT.esc(e.event_name || e.tournament)}</h3>
                    <div class="v-card-sub">${e.event_name ? VCT.esc(e.tournament) : 'TORNEO'}</div>
                    <span class="v-pill ${st.cls}">${st.txt}</span>
                </div>
            </div>
            <div class="v-event-dates">${VCT.fmtDate(e.start_date)} <small style="color:var(--dim)">→</small> ${VCT.fmtDate(e.end_date)}</div>
            <div class="v-event-stats">
                <span><b>${e.matches}</b> PARTIDOS</span>
                <span><b>${e.teams}</b> EQUIPOS</span>
            </div>
        </a>`;
    }).join('');
}

/* ── DETALLE ────────────────────────────────────────────────────────────── */
function toggleVistas(detalle) {
    document.getElementById('vistaLista').hidden = detalle;
    document.getElementById('vistaDetalle').hidden = !detalle;
}

async function cargarDetalle() {
    toggleVistas(true);
    VCT.loading(true);
    const qs = new URLSearchParams();
    const ev = VCT.param('event');
    const tor = VCT.param('torneo');
    if (ev) qs.set('event_id', ev);
    else if (tor) qs.set('torneo', tor);
    try {
        const d = await VCT.api(`/evento?${qs}`);
        pintarDetalle(d);
    } catch (e) {
        VCT.showError(document.getElementById('vistaDetalle'), e);
    } finally {
        VCT.loading(false);
    }
}

function equiposTable(equipos) {
    if (!equipos.length) return VCT.empty('Sin equipos.');
    return `<div style="overflow-x:auto"><table class="v-table">
        <thead>
            <tr>
                <th>EQUIPO</th><th class="num">PJ</th><th class="num">V-D</th><th class="num">WR</th>
                <th style="width:120px"></th><th class="num">MAPAS V-D</th><th class="num">MAP WR</th>
            </tr>
        </thead>
        <tbody>${equipos.map(t => {
            const wr = t.matches ? Math.round(t.wins * 100 / t.matches) : 0;
            const mapWr = t.maps ? Math.round(t.map_wins * 100 / t.maps) : 0;
            return `<tr>
                <td>${VCT.lozenge(t.team_name, t.tag, '', t.team_id)} <a href="${VCT.teamHref(t.team_id)}">${VCT.esc(t.team_name)}</a></td>
                <td class="num muted">${t.matches}</td>
                <td class="num"><span class="v-val-high">${t.wins}</span> - <span class="v-val-low">${t.matches - t.wins}</span></td>
                <td class="num ${VCT.wrClass(wr)}">${wr}%</td>
                <td>${VCT.bar(wr, wr >= 55 ? 'green' : (wr >= 45 ? '' : 'red'))}</td>
                <td class="num">${t.map_wins} - ${t.maps - t.map_wins}</td>
                <td class="num ${VCT.wrClass(mapWr)}">${mapWr}%</td>
            </tr>`;
        }).join('')}
        </tbody>
    </table></div>`;
}

function mapasTable(mapas) {
    if (!mapas.length) return VCT.empty('Sin mapas jugados.');
    return `<div style="overflow-x:auto"><table class="v-table">
        <thead>
            <tr>
                <th>MAPA</th><th class="num">JUGADOS</th><th class="num">PICKS</th><th class="num">PICK%</th>
                <th class="num">BANS</th><th class="num">DECIDER</th><th class="num">ATK%</th><th class="num">DEF%</th>
                <th style="width:160px">ATK / DEF</th>
            </tr>
        </thead>
        <tbody>${mapas.map(m => {
            const picks = (m.picked_a || 0) + (m.picked_b || 0);
            const pickPct = m.played ? Math.round(picks * 100 / m.played) : 0;
            return `<tr>
                <td>${VCT.esc(m.map_name || '—')}</td>
                <td class="num muted">${m.played}</td>
                <td class="num">${picks}</td>
                <td class="num">${pickPct}%</td>
                <td class="num">${m.bans}</td>
                <td class="num muted">${m.deciders}</td>
                <td class="num">${m.atk_win_pct != null ? m.atk_win_pct + '%' : '<span class="muted">—</span>'}</td>
                <td class="num">${m.def_win_pct != null ? m.def_win_pct + '%' : '<span class="muted">—</span>'}</td>
                <td>
                    <div style="display:flex;gap:6px;align-items:center">
                        ${VCT.bar(m.atk_win_pct ?? 0, 'green')}
                        ${VCT.bar(m.def_win_pct ?? 0, 'blue')}
                    </div>
                </td>
            </tr>`;
        }).join('')}
        </tbody>
    </table></div>`;
}

function agentesPanel(agentes, mapas) {
    if (!agentes.length) return VCT.empty('Sin datos de agentes para este evento.');
    const orden = mapas.map(m => m.map_name);
    const mapNames = [...new Set(agentes.map(a => a.map_name))]
        .sort((a, b) => orden.indexOf(a) - orden.indexOf(b));

    const chips = mapNames.map((mn, i) => `<button class="v-chip ${i === 0 ? 'active' : ''}" data-map="${VCT.esc(mn)}">${VCT.esc(mn)}</button>`).join('');
    return `
        <div class="v-chips" id="ag-chips">${chips}</div>
        <div id="ag-lista"></div>`;
}

function pintarAgentesMapa(agentes, mapName) {
    const rows = agentes.filter(a => a.map_name === mapName).sort((a, b) => (b.pick_pct || 0) - (a.pick_pct || 0));
    const cont = document.getElementById('ag-lista');
    if (!rows.length) {
        cont.innerHTML = VCT.empty('Sin agentes para este mapa.');
        return;
    }
    cont.innerHTML = `<div class="v-agent-grid">${rows.map(a => `
        <div class="v-agent-row">
            <span class="name v-agent-cell">${VCT.agentIcon(a.agent_name)}${VCT.esc(a.agent_name)} <span class="role">${VCT.esc(a.role || '')}</span></span>
            ${VCT.bar(a.pick_pct || 0, 'purple')}
            <span class="pct">${a.pick_pct ?? '—'}%</span>
        </div>`).join('')}</div>`;
}

function pintarDetalle(d) {
    const e = d.evento;
    const st = estadoEvento({ start_date: e.start_date, end_date: e.end_date });

    const el = document.getElementById('vistaDetalle');
    el.innerHTML = `
        <a class="v-back" href="index.html">← VOLVER A EVENTOS</a>

        <div class="v-banner" style="${e.event_id ? `--wm-a:url('/api/media/evento/${e.event_id}')` : ''}" ${e.event_id ? `data-c-evento="${e.event_id}"` : `data-c-nombre="${VCT.esc(e.nombre)}"`}>
            ${VCT.eventLogo(e.event_id, e.nombre, 'big')}
            <div class="v-banner-main">
                <h1>${VCT.esc(e.nombre)}</h1>
                <div class="v-meta">
                    <span class="v-pill ${st.cls}">${st.txt}</span>
                    <span>${VCT.fmtDate(e.start_date)} → ${VCT.fmtDate(e.end_date)}</span>
                    <span>${e.matches} PARTIDOS</span>
                    <span>${e.teams} EQUIPOS</span>
                    ${e.event_id ? `<a href="https://www.vlr.gg/event/${e.event_id}" target="_blank" rel="noopener">VLR.GG ↗</a>` : ''}
                </div>
                ${e.torneos && e.torneos.length > 1 ? `<div class="v-meta">${e.torneos.map(t => `<span>${VCT.esc(t)}</span>`).join('')}</div>` : ''}
            </div>
        </div>

        <div class="v-tabs" style="margin-top:18px">
            <button data-tab="partidos" class="active">PARTIDOS</button>
            <button data-tab="equipos">EQUIPOS</button>
            <button data-tab="mapas">MAPAS</button>
            <button data-tab="agentes">AGENTES</button>
        </div>

        <div class="v-tab-panel active" id="tab-partidos">
            <div id="ev-partidos"></div>
        </div>

        <div class="v-tab-panel" id="tab-equipos">
            <div class="v-panel">
                <div class="v-panel-title">RÉCORD POR EQUIPO <span class="v-hint">PARTIDOS Y MAPAS</span></div>
                ${equiposTable(d.equipos)}
            </div>
        </div>

        <div class="v-tab-panel" id="tab-mapas">
            <div class="v-panel">
                <div class="v-panel-title">ESTADÍSTICAS DE MAPAS <span class="v-hint">${e.event_id ? 'META DEL EVENTO (ATK/DEF)' : 'ATK/DEF NO DISPONIBLE PARA ESTE TORNEO'}</span></div>
                ${mapasTable(d.mapas)}
            </div>
        </div>

        <div class="v-tab-panel" id="tab-agentes">
            <div class="v-panel">
                <div class="v-panel-title">PICKRATE DE AGENTES <span class="v-hint">% DE MAPAS DONDE EL AGENTE FUE USADO</span></div>
                ${agentesPanel(d.agentes, d.mapas)}
            </div>
        </div>
    `;

    VCT.renderMatchList(document.getElementById('ev-partidos'), d.partidos);
    VCT.tabs(el);

    const chips = document.getElementById('ag-chips');
    if (chips) {
        const buttons = chips.querySelectorAll('.v-chip');
        buttons.forEach(b => b.addEventListener('click', () => {
            buttons.forEach(x => x.classList.remove('active'));
            b.classList.add('active');
            pintarAgentesMapa(d.agentes, b.dataset.map);
        }));
        if (buttons.length) pintarAgentesMapa(d.agentes, buttons[0].dataset.map);
    }
    VCT.aplicarColores(el);
}

/* ── INIT ───────────────────────────────────────────────────────────────── */
(async function init() {
    VCT.bindLinks(document);
    if (VCT.param('event') || VCT.param('torneo')) {
        await cargarDetalle();
        return;
    }
    await cargarLista();
    document.getElementById('q').addEventListener('input', VCT.debounce(cargarLista));
})();
