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
 *     <script>window.AE_HEADER = { title: 'EN VIVO', badge: 'PREDICTOR' };</script>
 * `title`/`badge` agregan un título central al header.
 * `hidden` (array de ids) oculta entradas concretas del nav.
 */
(function () {
    'use strict';

    const ITEMS = [
        { id: 'aletheia', label: 'EN VIVO', href: 'aletheia/index.html' },
        { id: 'preparar', label: 'PREPARAR PARTIDO', href: 'aletheia_preparar/index.html' },
        { id: 'tablas', label: 'TABLAS', href: 'tablas/index.html' },
        { id: 'visualizar', label: 'VISUALIZAR', href: 'visualizar/index.html' },
        { id: 'datos', label: 'CARGAR DATOS', href: 'inicio/index.html' },
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
            <a href="${base}aletheia/index.html" class="ae-logo"><span class="ae-logo-a">A</span>LETHEIA</a>
            <nav class="ae-nav">`;

        if (cfg.title) {
            html += `<span class="ae-title">${cfg.title}${cfg.badge ? ` <span class="ae-badge">${cfg.badge}</span>` : ''}</span>`;
        }

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
