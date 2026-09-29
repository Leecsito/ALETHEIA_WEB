/**
 * ALETHEIA — Componente HEADER reutilizable.
 *
 * Uso en cada componente (dentro del <body>):
 *     <div id="aeHeaderMount"></div>
 *     <script src="../header/header.js"></script>
 *
 * El script inyecta el marcado del header, resuelve las rutas relativas a la
 * raíz del sitio y marca como activa la página actual según `window.location`.
 *
 * Config opcional por página (antes de cargar el script):
 *     <script>window.AE_HEADER = { hidden: ['datos'] };</script>
 * `hidden` (array de ids) oculta entradas concretas del nav.
 */
(function () {
    'use strict';

    const ITEMS = [
        { id: 'aletheia', label: 'EN VIVO', href: 'aletheia/' },
        { id: 'preparar', label: 'PREPARAR PARTIDO', href: 'aletheia_preparar/' },
        { id: 'vct', label: 'VCT', href: 'partidos/' },
        { id: 'tablas', label: 'TABLAS', href: 'tablas/' },
        { id: 'visualizar', label: 'VISUALIZAR', href: 'visualizar/' },
        { id: 'datos', label: 'CARGAR DATOS', href: 'inicio/' },
    ];

    function basePath() {
        // Rutas tipo /aletheia/ o /aletheia/index.html -> profundidad 1.
        // En la raíz (/) la profundidad es 0.
        const seg = window.location.pathname.split('/').filter(Boolean);
        const depth = (seg.length && seg[seg.length - 1].includes('.'))
            ? seg.length - 1
            : seg.length;
        return '../'.repeat(Math.max(depth, 0));
    }

    function currentId() {
        const seg = window.location.pathname.toLowerCase();
        if (seg.includes('/aletheia_preparar')) return 'preparar';
        if (seg.includes('/aletheia')) return 'aletheia';
        if (seg.includes('/partidos') || seg.includes('/equipos') ||
            seg.includes('/jugadores') || seg.includes('/eventos')) return 'vct';
        if (seg.includes('/tablas')) return 'tablas';
        if (seg.includes('/visualizar')) return 'visualizar';
        if (seg.includes('/inicio')) return 'datos';
        return null;
    }

    function render() {
        const mount = document.getElementById('aeHeaderMount');
        if (!mount) return;

        const cfg = window.AE_HEADER || {};
        const hidden = Array.isArray(cfg.hidden) ? cfg.hidden : [];
        const base = basePath();
        const active = currentId();

        let html = `
            <a href="${base}aletheia/" class="ae-logo" aria-label="ALETHEIA — Inicio">
                <span class="ae-logo-mark">
                    <img class="ae-logo-ghost" src="${base}comun/ALETHEIA_ico.svg" alt="" aria-hidden="true"
                        onerror="this.remove()" />
                    <img class="ae-logo-img" src="${base}comun/ALETHEIA_ico.svg" alt="" width="56" height="56"
                        onerror="this.remove()" />
                </span>
                <span class="ae-logo-word"><span class="ae-logo-a">A</span>LETHEIA</span>
            </a>
            <nav class="ae-nav">`;

        ITEMS.forEach(item => {
            if (hidden.includes(item.id)) return;
            const cls = 'ae-btn' + (item.id === active ? ' active' : '');
            html += `<a href="${base}${item.href}" class="${cls}">${item.label}</a>`;
        });

        html += `</nav>`;
        mount.outerHTML = `<header class="ae-header">${html}</header>`;
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', render);
    } else {
        render();
    }
})();
