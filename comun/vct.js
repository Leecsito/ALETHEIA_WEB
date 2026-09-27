/**
 * ALETHEIA — Core JS compartido para los componentes VCT
 * (partidos/, equipos/, jugadores/, eventos/).
 *
 * Expone el objeto global `VCT` con helpers de API, formato y render.
 */

const VCT = (() => {
    'use strict';

    const API = `${window.location.origin}/api`;

    /* ── API ─────────────────────────────────────────────────────────────── */
    async function api(path) {
        const res = await fetch(`${API}${path}`);
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`);
        return data;
    }

    /* ── TEXTO / HTML ────────────────────────────────────────────────────── */
    const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));

    function param(name) {
        return new URLSearchParams(window.location.search).get(name);
    }

    function debounce(fn, ms = 300) {
        let t;
        return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
    }

    /* ── PAÍSES ──────────────────────────────────────────────────────────── */
    const COUNTRY_NAMES = {
        'united states': 'us', 'brazil': 'br', 'china': 'cn', 'india': 'in',
        'indonesia': 'id', 'japan': 'jp', 'philippines': 'ph', 'singapore': 'sg',
        'south korea': 'kr', 'thailand': 'th', 'türkiye': 'tr', 'turkiye': 'tr',
        'argentina': 'ar', 'europe': '', 'united kingdom': 'gb', 'canada': 'ca',
        'australia': 'au', 'france': 'fr', 'germany': 'de', 'spain': 'es',
        'sweden': 'se', 'poland': 'pl', 'russia': 'ru', 'vietnam': 'vn',
    };

    function flag(country) {
        if (!country) return '';
        const raw = String(country).trim();
        const code = raw.length === 2 ? raw.toLowerCase() : (COUNTRY_NAMES[raw.toLowerCase()] ?? '');
        if (!code) return '';
        const base = 0x1F1E6;
        return String.fromCodePoint(base + code.charCodeAt(0) - 97, base + code.charCodeAt(1) - 97);
    }

    function flagHtml(country) {
        const f = flag(country);
        return f ? `<span class="v-pl-flag" title="${esc(country)}">${f}</span>` : '';
    }

    /* ── EQUIPOS ─────────────────────────────────────────────────────────── */
    function teamColor(name) {
        let h = 0;
        for (const ch of String(name || '?')) h = (h * 31 + ch.charCodeAt(0)) % 360;
        return `hsl(${h} 55% 55%)`;
    }

    function initials(name) {
        const s = String(name || '?').replace(/[^a-z0-9]/gi, '');
        return (s.slice(0, 3) || '?').toUpperCase();
    }

    function lozenge(name, tag, cls = '', teamId = null) {
        const img = teamId
            ? `<img data-media="equipo:${teamId}" data-fallback="/api/media/equipo/${teamId}" alt="" loading="lazy" decoding="async" onload="this.parentNode.classList.add('has-img')" onerror="VCT.imgError(this)">`
            : '';
        const attr = teamId ? ` data-c-equipo="${teamId}"` : '';
        return `<span class="v-lozenge ${cls}"${attr}>${img}<span class="v-lozenge-txt">${esc((tag || initials(name)).slice(0, 4))}</span></span>`;
    }

    function avatar(playerId, nickname, cls = '') {
        const img = playerId
            ? `<img data-media="jugador:${playerId}" data-fallback="/api/media/jugador/${playerId}" alt="" loading="lazy" decoding="async" onload="this.parentNode.classList.add('has-img')" onerror="VCT.imgError(this)">`
            : '';
        return `<span class="v-avatar ${cls}">${img}<span class="v-avatar-txt">${esc(initials(nickname))}</span></span>`;
    }

    function initialsEvent(name) {
        const words = String(name || '').split(/[\s:–—\-,]+/).filter(w => w && !/^\d+$/.test(w));
        if (!words.length) return 'EV';
        if (/^[A-Z0-9]{2,4}$/.test(words[0])) return words[0];
        return words.slice(0, 3).map(w => w[0]).join('').toUpperCase();
    }

    function eventLogo(eventId, name, cls = '') {
        const key = eventId ? `evento:${eventId}` : `nombre:${name || ''}`;
        const fallback = eventId
            ? `/api/media/evento/${eventId}`
            : `/api/media/evento?nombre=${encodeURIComponent(name || '')}`;
        const attr = eventId
            ? ` data-c-evento="${eventId}"`
            : ` data-c-nombre="${esc(name || '')}"`;
        const img = (eventId || name)
            ? `<img data-media="${esc(key)}" data-fallback="${fallback}" alt="" loading="lazy" decoding="async" onload="this.parentNode.classList.add('has-img')" onerror="VCT.imgError(this)">`
            : '';
        return `<span class="v-elogo ${cls}"${attr}>${img}<span class="v-elogo-txt">${esc(initialsEvent(name))}</span></span>`;
    }

    const slug = texto => String(texto || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

    /* Un solo request por render: apunta las imágenes al CDN (si ya están
       resueltas), aplica colores medios y watermarks. Lo no resuelto se pide
       por el endpoint de redirect (que resuelve bajo demanda) y se reintenta. */
    async function aplicarMedia(root = document, intento = 0) {
        const parse = k => { const i = String(k).indexOf(':'); return [k.slice(0, i), k.slice(i + 1)]; };
        const equipos = new Set(), jugadores = new Set(), eventos = new Set(), nombres = new Set();
        const imgs = [...root.querySelectorAll('img[data-media]')];
        const wms = [...root.querySelectorAll('[data-wm],[data-wm2]')];

        const sumar = (t, v) => {
            if (!v) return;
            if (t === 'equipo') equipos.add(v);
            else if (t === 'jugador') jugadores.add(v);
            else if (t === 'evento') eventos.add(v);
            else if (t === 'nombre') nombres.add(v);
        };

        imgs.forEach(img => sumar(...parse(img.dataset.media)));
        wms.forEach(el => { if (el.dataset.wm) sumar(...parse(el.dataset.wm)); if (el.dataset.wm2) sumar(...parse(el.dataset.wm2)); });
        root.querySelectorAll('[data-c-equipo]').forEach(el => sumar('equipo', el.dataset.cEquipo));
        root.querySelectorAll('[data-c-equipo2]').forEach(el => sumar('equipo', el.dataset.cEquipo2));
        root.querySelectorAll('[data-c-evento]').forEach(el => sumar('evento', el.dataset.cEvento));
        root.querySelectorAll('[data-c-nombre]').forEach(el => sumar('nombre', el.dataset.cNombre));

        if (!equipos.size && !jugadores.size && !eventos.size && !nombres.size) return;

        const qs = new URLSearchParams();
        if (equipos.size) qs.set('equipos', [...equipos].join(','));
        if (jugadores.size) qs.set('jugadores', [...jugadores].join(','));
        if (eventos.size) qs.set('eventos', [...eventos].join(','));
        if (nombres.size) qs.set('nombres', [...nombres].join('|'));

        const lookup = (d, t, v) => t === 'equipo' ? d.equipos?.[v]
            : t === 'jugador' ? d.jugadores?.[v]
            : t === 'evento' ? d.eventos?.[v]
            : d.nombres?.[slug(v)];

        let pendientes = 0;
        try {
            const d = await api(`/media/meta?${qs}`);

            imgs.forEach(img => {
                const [t, v] = parse(img.dataset.media);
                const info = lookup(d, t, v);
                if (info?.u) {
                    if (img.src !== info.u) img.src = info.u;   // directo al CDN
                } else {
                    if (!img.src) img.src = img.dataset.fallback; // resuelve bajo demanda
                    pendientes++;
                }
            });

            const aplicar = (el, info) => {
                if (!info) return false;
                if (info.c) el.style.setProperty('--c', info.c);
                el.classList.toggle('on-light', !!info.d);
                return true;
            };
            root.querySelectorAll('[data-c-equipo]').forEach(el => { if (!aplicar(el, d.equipos?.[el.dataset.cEquipo])) pendientes++; });
            root.querySelectorAll('[data-c-equipo2]').forEach(el => {
                const info = d.equipos?.[el.dataset.cEquipo2];
                if (info?.c) el.style.setProperty('--c2', info.c);
                else pendientes++;
            });
            root.querySelectorAll('[data-c-evento]').forEach(el => { if (!aplicar(el, d.eventos?.[el.dataset.cEvento])) pendientes++; });
            root.querySelectorAll('[data-c-nombre]').forEach(el => { if (!aplicar(el, d.nombres?.[slug(el.dataset.cNombre)])) pendientes++; });

            wms.forEach(el => {
                if (el.dataset.wm) {
                    const [t, v] = parse(el.dataset.wm);
                    const info = lookup(d, t, v);
                    if (info?.u) el.style.setProperty('--wm-a', `url('${info.u}')`);
                }
                if (el.dataset.wm2) {
                    const [t, v] = parse(el.dataset.wm2);
                    const info = lookup(d, t, v);
                    if (info?.u) el.style.setProperty('--wm-b', `url('${info.u}')`);
                }
            });
        } catch (e) {
            console.error(e);
            pendientes++;
        }
        if (pendientes && intento < 3) setTimeout(() => aplicarMedia(root, intento + 1), 3500);
    }

    /* Imágenes locales de multimedia/ (agentes y mapas). */
    function agentIcon(agent) {
        const slug = String(agent || '').toLowerCase().replace(/[^a-z]/g, '');
        if (!slug) return '';
        return `<img class="v-agent-icon" src="/multimedia/agents/${slug}.avif" alt="${esc(agent)}" title="${esc(agent)}" loading="lazy" onerror="this.remove()">`;
    }

    function mapIcon(mapName, cls = '') {
        const slug = String(mapName || '').toUpperCase().replace(/[^A-Z]/g, '');
        if (!slug) return '';
        return `<img class="v-map-icon ${cls}" src="/multimedia/maps/${slug}.avif" alt="" loading="lazy" onerror="this.remove()">`;
    }

    /* Reintenta una imagen de media una vez; si vuelve a fallar, la quita
       (deja ver el fallback: siglas del equipo o iniciales del jugador). */
    function imgError(img) {
        if (!img.dataset.retry) {
            img.dataset.retry = '1';
            const base = img.src.split('?')[0];
            setTimeout(() => { img.src = `${base}?r=${Date.now()}`; }, 3000);
        } else {
            img.remove();
        }
    }

    const teamHref = id => `../equipos/index.html?team=${id}`;
    const playerHref = id => `../jugadores/index.html?player=${id}`;
    const matchHref = id => `../partidos/index.html?match=${id}`;
    const eventHref = (eventId, torneo) => eventId
        ? `../eventos/index.html?event=${eventId}`
        : `../eventos/index.html?torneo=${encodeURIComponent(torneo || '')}`;

    function teamCell(id, name, tag) {
        if (!id || !name) return `<span class="muted">—</span>`;
        return `<a href="${teamHref(id)}">${esc(tag || initials(name))}</a>`;
    }

    function playerCell(id, nickname) {
        if (!id || !nickname) return `<span class="muted">—</span>`;
        return `<a href="${playerHref(id)}">${esc(nickname)}</a>`;
    }

    /* ── FECHAS ──────────────────────────────────────────────────────────── */
    const MONTHS = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'];
    const DAYS = ['dom', 'lun', 'mar', 'mié', 'jue', 'vie', 'sáb'];

    function parseDate(iso) {
        if (!iso) return null;
        const [y, m, d] = String(iso).slice(0, 10).split('-').map(Number);
        if (!y || !m || !d) return null;
        return new Date(y, m - 1, d);
    }

    function fmtDate(iso) {
        const dt = parseDate(iso);
        if (!dt) return iso || '—';
        return `${dt.getDate()} ${MONTHS[dt.getMonth()]} ${dt.getFullYear()}`;
    }

    function todayIso() {
        const d = new Date();
        return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    }

    function dayLabel(iso) {
        const dt = parseDate(iso);
        if (!dt) return iso || 'SIN FECHA';
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const diff = Math.round((today - dt) / 86400000);
        if (diff === 0) return 'HOY';
        if (diff === 1) return 'AYER';
        return `${DAYS[dt.getDay()]} ${dt.getDate()} ${MONTHS[dt.getMonth()]}`;
    }

    /* ── COLORES / MÉTRICAS ──────────────────────────────────────────────── */
    function ratingClass(v) {
        v = parseFloat(v);
        if (isNaN(v)) return 'muted';
        if (v >= 1.15) return 'v-val-high';
        if (v >= 1.0) return 'v-val-mid';
        return 'v-val-low';
    }

    function wrClass(v) {
        v = parseFloat(v);
        if (isNaN(v)) return 'muted';
        if (v >= 55) return 'v-val-high';
        if (v >= 45) return 'v-val-mid';
        return 'v-val-low';
    }

    function kdColor(k, d) {
        const v = (k || 0) - (d || 0);
        if (v > 0) return 'v-val-high';
        if (v < 0) return 'v-val-low';
        return 'muted';
    }

    const fmt = (v, dec = 2) => (v === null || v === undefined || v === '' || isNaN(parseFloat(v)))
        ? '—' : parseFloat(v).toFixed(dec);

    const pct = (v, dec = 0) => (v === null || v === undefined || v === '' || isNaN(parseFloat(v)))
        ? '—' : `${parseFloat(v).toFixed(dec)}%`;

    function boLabel(maps) {
        maps = parseInt(maps) || 0;
        if (!maps) return '—';
        if (maps <= 1) return 'BO1';
        if (maps <= 3) return 'BO3';
        return 'BO5';
    }

    const bar = (val, cls = '') => `<div class="v-bar ${cls}"><i style="width:${Math.max(0, Math.min(100, parseFloat(val) || 0))}%"></i></div>`;

    const empty = msg => `<div class="v-empty">${esc(msg)}</div>`;

    function stat(label, value, cls = '') {
        return `<div class="v-stat"><span class="label">${esc(label)}</span><span class="value ${cls}">${value}</span></div>`;
    }

    /* ── FILAS DE PARTIDO ────────────────────────────────────────────────── */
    function matchRow(m, opts = {}) {
        const winA = m.winner_id && m.winner_id === m.team_a_id;
        const winB = m.winner_id && m.winner_id === m.team_b_id;
        const done = !!m.winner_id;
        const maps = m.maps_played || 0;
        const href = matchHref(m.match_id);
        const cAttrs = `${m.team_a_id ? ` data-c-equipo="${m.team_a_id}"` : ''}${m.team_b_id ? ` data-c-equipo2="${m.team_b_id}"` : ''}`;
        const linkOpen = opts.noLink
            ? `<div class="v-match no-link"${cAttrs}>`
            : `<div class="v-match" data-href="${href}"${cAttrs}>`;

        return `
        ${linkOpen}
            <div class="v-match-when">
                <b>${boLabel(maps)}</b>
                <span>${maps} MAPA${maps === 1 ? '' : 'S'}</span>
            </div>
            <div class="v-match-teams">
                <div class="v-mt ${winA ? 'win' : ''}">
                    ${lozenge(m.team_a, m.team_a_tag, '', m.team_a_id)}
                    <span class="v-mt-name">${esc(m.team_a || 'TBD')}</span>
                    <span class="v-mt-score">${m.score_a ?? '-'}</span>
                </div>
                <div class="v-mt ${winB ? 'win' : ''}">
                    ${lozenge(m.team_b, m.team_b_tag || (m.team_b ? null : 'TBD'), '', m.team_b_id)}
                    <span class="v-mt-name">${esc(m.team_b || 'TBD')}</span>
                    <span class="v-mt-score">${m.score_b ?? '-'}</span>
                </div>
            </div>
            <div class="v-match-status">
                ${done ? '<span class="v-pill done">FINALIZADO</span>' : '<span class="v-pill soon">PENDIENTE</span>'}
            </div>
            <div class="v-match-meta">
                <a class="v-event-link" href="${eventHref(m.event_id, m.tournament)}">${esc(m.event_name || m.tournament || '—')}</a>
                <span class="v-phase">${esc(m.phase || '')}</span>
            </div>
        </div>`;
    }

    function renderMatchList(container, matches, opts = {}) {
        if (!container) return;
        if (!matches || !matches.length) {
            container.innerHTML = empty('Sin partidos.');
            return;
        }
        let html = '';
        let current = null;
        const hoy = todayIso();
        for (const m of matches) {
            const key = m.match_date || '';
            if (key !== current) {
                current = key;
                html += `<div class="v-day${key === hoy ? ' today' : ''}">
                    <b>${dayLabel(key)}</b><span>${key ? fmtDate(key) : ''}</span>
                </div>`;
            }
            html += matchRow(m, opts);
        }
        container.innerHTML = html;
    }

    /* ── NAVEGACIÓN / UI ─────────────────────────────────────────────────── */
    function bindLinks(root = document) {
        root.addEventListener('click', e => {
            if (e.target.closest('a')) return;
            const el = e.target.closest('[data-href]');
            if (el && !el.classList.contains('no-link')) window.location.href = el.dataset.href;
        });
    }

    function loading(show) {
        const el = document.getElementById('vLoading');
        if (el) el.classList.toggle('hidden', !show);
    }

    function tabs(root, onSelect) {
        const buttons = root.querySelectorAll('button[data-tab]');
        buttons.forEach(btn => btn.addEventListener('click', () => {
            buttons.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            document.querySelectorAll('.v-tab-panel').forEach(p => p.classList.remove('active'));
            const panel = document.getElementById(`tab-${btn.dataset.tab}`);
            if (panel) panel.classList.add('active');
            if (onSelect) onSelect(btn.dataset.tab);
        }));
    }

    function activateTab(name) {
        const btn = document.querySelector(`button[data-tab="${name}"]`);
        if (btn) btn.click();
    }

    function showError(container, err) {
        if (container) container.innerHTML = empty(`Error: ${err.message || err}`);
        console.error(err);
    }

    return {
        API, api, esc, param, debounce,
        flag, flagHtml, teamColor, initials, lozenge, avatar,
        initialsEvent, eventLogo, slug, aplicarMedia, aplicarColores: aplicarMedia,
        agentIcon, mapIcon, imgError,
        teamHref, playerHref, matchHref, eventHref,
        teamCell, playerCell,
        parseDate, fmtDate, todayIso, dayLabel,
        ratingClass, wrClass, kdColor, fmt, pct, boLabel, bar, empty, stat,
        matchRow, renderMatchList,
        bindLinks, loading, tabs, activateTab, showError,
    };
})();
