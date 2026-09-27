/* ALETHEIA — JUGADORES: lista + detalle con agentes, partidos y equipos */

const estado = { q: '', orden: 'rating' };

const listaEl = document.getElementById('lista');
const countEl = document.getElementById('count');

/* ── LISTA ──────────────────────────────────────────────────────────────── */
async function cargarLista() {
    VCT.loading(true);
    const qs = new URLSearchParams({ orden: estado.orden });
    if (estado.q) qs.set('q', estado.q);
    try {
        const d = await VCT.api(`/jugadores?${qs}`);
        countEl.textContent = `${d.data.length} JUGADORES`;
        pintarLista(d.data);
    } catch (e) {
        VCT.showError(listaEl, e);
    } finally {
        VCT.loading(false);
    }
}

function pintarLista(rows) {
    if (!rows.length) {
        listaEl.innerHTML = VCT.empty('Sin jugadores.');
        return;
    }
    listaEl.innerHTML = `<table class="v-table" style="background:var(--surface)">
        <thead>
            <tr>
                <th>JUGADOR</th><th>EQUIPO</th><th class="num">MAPS</th><th class="num">R</th>
                <th class="num">ACS</th><th class="num">K/D</th><th class="num">KAST</th>
                <th class="num">ADR</th><th class="num">HS%</th><th class="num">FK</th><th class="num">FD</th>
            </tr>
        </thead>
        <tbody>${rows.map(j => {
            const kd = j.deaths ? (j.kills / j.deaths).toFixed(2) : '—';
            return `<tr data-href="index.html?player=${j.player_id}" style="cursor:pointer">
                <td><span class="v-pl">${VCT.avatar(j.player_id, j.nickname, 'sm')}<span>${VCT.flagHtml(j.country)} <b>${VCT.esc(j.nickname)}</b> <span class="muted">${VCT.esc(j.real_name || '')}</span></span></span></td>
                <td>${j.team_id ? `<a href="${VCT.teamHref(j.team_id)}">${VCT.esc(j.tag || j.team_name)}</a>` : '<span class="muted">—</span>'}</td>
                <td class="num muted">${j.matches}</td>
                <td class="num ${VCT.ratingClass(j.rating)}">${VCT.fmt(j.rating)}</td>
                <td class="num">${j.acs ?? '—'}</td>
                <td class="num ${VCT.kdColor(j.kills, j.deaths)}">${kd}</td>
                <td class="num">${VCT.pct(j.kast, 0)}</td>
                <td class="num">${VCT.fmt(j.adr, 1)}</td>
                <td class="num">${VCT.pct(j.hs_percent, 0)}</td>
                <td class="num v-val-high">${j.fk}</td>
                <td class="num v-val-low">${j.fd}</td>
            </tr>`;
        }).join('')}
        </tbody>
    </table>`;
}

/* ── DETALLE ────────────────────────────────────────────────────────────── */
function toggleVistas(detalle) {
    document.getElementById('vistaLista').hidden = detalle;
    document.getElementById('vistaDetalle').hidden = !detalle;
}

async function cargarDetalle(pid) {
    toggleVistas(true);
    VCT.loading(true);
    try {
        const d = await VCT.api(`/jugador/${pid}`);
        pintarDetalle(d);
    } catch (e) {
        VCT.showError(document.getElementById('vistaDetalle'), e);
    } finally {
        VCT.loading(false);
    }
}

function agentesTable(agentes) {
    if (!agentes.length) return VCT.empty('Sin estadísticas por agente.');
    const maxUse = Math.max(...agentes.map(a => a.use_count || 0), 1);
    return `<div style="overflow-x:auto"><table class="v-table">
        <thead>
            <tr>
                <th>AGENTE</th><th>USO</th><th class="num">RND</th><th class="num">R</th><th class="num">ACS</th>
                <th class="num">K/D</th><th class="num">KAST</th><th class="num">ADR</th>
                <th class="num">KPR</th><th class="num">APR</th><th class="num">FK/FD</th>
                <th class="num">K</th><th class="num">D</th><th class="num">A</th>
            </tr>
        </thead>
        <tbody>${agentes.map(a => {
            const use = Math.round((a.use_count || 0) * 100 / maxUse);
            return `<tr>
                <td><span class="v-agent-cell" style="text-transform:capitalize">${VCT.agentIcon(a.agent)}${VCT.esc(a.agent)}${a.role ? `<span class="v-agent-role">${VCT.esc(a.role)}</span>` : ''}</span></td>
                <td>
                    <div class="v-use-bar">
                        ${VCT.bar(use, 'purple')}
                        <span>${a.use_count || 0}</span>
                    </div>
                </td>
                <td class="num muted">${a.rnd ?? '—'}</td>
                <td class="num ${VCT.ratingClass(a.rating)}">${VCT.fmt(a.rating)}</td>
                <td class="num">${VCT.fmt(a.acs, 0)}</td>
                <td class="num">${VCT.fmt(a.kd)}</td>
                <td class="num">${VCT.pct(a.kast, 0)}</td>
                <td class="num">${VCT.fmt(a.adr, 1)}</td>
                <td class="num muted">${VCT.fmt(a.kpr)}</td>
                <td class="num muted">${VCT.fmt(a.apr)}</td>
                <td class="num">${VCT.fmt(a.fk_fd)}</td>
                <td class="num">${a.k ?? '—'}</td>
                <td class="num">${a.d ?? '—'}</td>
                <td class="num">${a.a ?? '—'}</td>
            </tr>`;
        }).join('')}
        </tbody>
    </table></div>`;
}

function partidosTable(partidos, jugadorId) {
    if (!partidos.length) return VCT.empty('Sin partidos registrados.');
    return `<div style="overflow-x:auto"><table class="v-table">
        <thead>
            <tr>
                <th>FECHA</th><th>EVENTO</th><th>RIVAL</th><th class="num">RES</th><th class="num">SCORE</th>
                <th class="num">MAPS</th><th>AGENTES</th><th class="num">R</th><th class="num">ACS</th><th class="num">K/D/A</th>
            </tr>
        </thead>
        <tbody>${partidos.map(p => {
            const esA = p.player_team_id === p.team_a_id;
            const rivalId = esA ? p.team_b_id : p.team_a_id;
            const rival = esA ? p.team_b : p.team_a;
            const rivalTag = esA ? p.team_b_tag : p.team_a_tag;
            const win = p.winner_id && p.winner_id === p.player_team_id;
            const score = esA ? `${p.score_a} - ${p.score_b}` : `${p.score_b} - ${p.score_a}`;
            return `<tr data-href="${VCT.matchHref(p.match_id)}" style="cursor:pointer">
                <td class="muted">${VCT.fmtDate(p.match_date)}</td>
                <td><a class="v-event-link" href="${VCT.eventHref(p.event_id, p.tournament)}">${VCT.esc(p.event_name || p.tournament)}</a></td>
                <td><a href="${VCT.teamHref(rivalId)}">${VCT.esc(rivalTag || rival || '—')}</a></td>
                <td class="num"><span class="v-result ${win ? 'win' : 'loss'}">${win ? 'V' : 'D'}</span></td>
                <td class="num">${score}</td>
                <td class="num muted">${p.maps}</td>
                <td>${(p.agents || '').split(',').map(a => `<span class="v-agent-chip">${VCT.esc(a)}</span>`).join(' ')}</td>
                <td class="num ${VCT.ratingClass(p.rating)}">${VCT.fmt(p.rating)}</td>
                <td class="num">${p.acs ?? '—'}</td>
                <td class="num"><span class="v-val-high">${p.kills}</span>/<span class="v-val-low">${p.deaths}</span>/${p.assists}</td>
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
                <th>MAPA</th><th class="num">MAPS</th><th class="num">R</th><th class="num">ACS</th>
                <th class="num">K/D/A</th><th class="num">KAST</th><th class="num">ADR</th>
                <th class="num">HS%</th><th class="num">FK/FD</th>
            </tr>
        </thead>
        <tbody>${mapas.map(m => `<tr>
            <td>${VCT.esc(m.map_name || '—')}</td>
            <td class="num muted">${m.matches}</td>
            <td class="num ${VCT.ratingClass(m.rating)}">${VCT.fmt(m.rating)}</td>
            <td class="num">${m.acs ?? '—'}</td>
            <td class="num"><span class="v-val-high">${m.kills}</span> / <span class="v-val-low">${m.deaths}</span> / ${m.assists}</td>
            <td class="num">${VCT.pct(m.kast, 0)}</td>
            <td class="num">${VCT.fmt(m.adr, 1)}</td>
            <td class="num">${VCT.pct(m.hs_percent, 0)}</td>
            <td class="num">${m.fk}/${m.fd}</td>
        </tr>`).join('')}
        </tbody>
    </table></div>`;
}

function equiposList(equipos) {
    if (!equipos.length) return VCT.empty('Sin historial de equipos.');
    return `<div style="overflow-x:auto"><table class="v-table">
        <thead><tr><th>MOVIMIENTO</th><th>EQUIPO</th><th>FECHA</th><th>REGION</th></tr></thead>
        <tbody>${equipos.map(e => `<tr>
            <td><span class="v-tx ${VCT.esc(e.action)}">${VCT.esc(e.action)}</span></td>
            <td><a href="${VCT.teamHref(e.team_id)}">${VCT.lozenge(e.team_name, e.tag, '', e.team_id)} ${VCT.esc(e.team_name)}</a></td>
            <td class="muted">${VCT.fmtDate(e.transaction_date)}</td>
            <td class="muted">${VCT.esc(e.region || '—')}</td>
        </tr>`).join('')}
        </tbody>
    </table></div>`;
}

function pintarDetalle(d) {
    const p = d.jugador;
    const t = d.totales || {};
    const mk = d.multikills || {};
    const kd = t.deaths ? (t.kills / t.deaths).toFixed(2) : '—';
    const clutch = (mk.v1 || 0) + (mk.v2 || 0) + (mk.v3 || 0) + (mk.v4 || 0) + (mk.v5 || 0);

    const el = document.getElementById('vistaDetalle');
    el.innerHTML = `
        <a class="v-back" href="index.html">← VOLVER A JUGADORES</a>

        <div class="v-banner" style="--wm-a:url('/api/media/jugador/${p.player_id}')">
            ${VCT.avatar(p.player_id, p.nickname, 'big')}
            <div class="v-banner-main">
                <h1>${VCT.esc(p.nickname || '—')}</h1>
                <div class="v-meta">
                    <span>${VCT.esc(p.real_name || '—')}</span>
                    <span>${VCT.flagHtml(p.country)} ${VCT.esc(p.country || '—')}</span>
                    ${p.team_id ? `<a href="${VCT.teamHref(p.team_id)}">${VCT.esc(p.team_name)}</a>` : '<span>SIN EQUIPO</span>'}
                </div>
            </div>
            <div style="text-align:right">
                <div class="v-vs-score ${VCT.ratingClass(t.rating)}">${VCT.fmt(t.rating)}</div>
                <div class="v-sub">RATING PROMEDIO · ${t.matches || 0} PARTIDOS</div>
            </div>
        </div>

        <div class="v-stats" style="margin-top:18px">
            ${VCT.stat('ACS', t.acs ?? '—')}
            ${VCT.stat('K/D', kd, VCT.kdColor(t.kills, t.deaths))}
            ${VCT.stat('KAST', VCT.pct(t.kast, 0))}
            ${VCT.stat('ADR', VCT.fmt(t.adr, 1))}
            ${VCT.stat('HS%', VCT.pct(t.hs_percent, 0))}
            ${VCT.stat('FK/FD', `${t.fk ?? 0}<small>/${t.fd ?? 0}</small>`)}
            ${VCT.stat('CLUTCHES 1vX', clutch)}
            ${VCT.stat('PLANTS/DEFUSES', `${mk.plants ?? 0}<small>/${mk.defuses ?? 0}</small>`)}
        </div>

        <div class="v-tabs">
            <button data-tab="agentes" class="active">AGENTES</button>
            <button data-tab="partidos">PARTIDOS</button>
            <button data-tab="mapas">MAPAS</button>
            <button data-tab="equipos">EQUIPOS</button>
        </div>

        <div class="v-tab-panel active" id="tab-agentes">
            <div class="v-panel">
                <div class="v-panel-title">AGENTES <span class="v-hint">VENTANA MÁS AMPLIA DISPONIBLE</span></div>
                ${agentesTable(d.agentes)}
            </div>
        </div>

        <div class="v-tab-panel" id="tab-partidos">
            <div class="v-panel">
                <div class="v-panel-title">PARTIDOS RECIENTES <span class="v-hint">ÚLTIMOS ${d.partidos.length}</span></div>
                ${partidosTable(d.partidos, p.player_id)}
            </div>
        </div>

        <div class="v-tab-panel" id="tab-mapas">
            <div class="v-panel">
                <div class="v-panel-title">RENDIMIENTO POR MAPA</div>
                ${mapasTable(d.mapas)}
            </div>
        </div>

        <div class="v-tab-panel" id="tab-equipos">
            <div class="v-panel">
                <div class="v-panel-title">HISTORIAL DE EQUIPOS</div>
                ${equiposList(d.equipos)}
            </div>
        </div>
    `;

    VCT.tabs(el);
}

/* ── INIT ───────────────────────────────────────────────────────────────── */
(async function init() {
    VCT.bindLinks(document);
    const pid = VCT.param('player');
    if (pid) {
        await cargarDetalle(pid);
        return;
    }
    await cargarLista();
    document.getElementById('q').addEventListener('input', VCT.debounce(() => {
        estado.q = document.getElementById('q').value.trim();
        cargarLista();
    }));
    document.getElementById('orden').addEventListener('change', function () {
        estado.orden = this.value;
        cargarLista();
    });
})();
