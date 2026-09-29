/**
 * ALETHEIA — Canvas de nodos del header.
 *
 * Se incluye en cada página DESPUÉS de header.js:
 *     <script src="../header/header.js"></script>
 *     <script src="../header/header-nodes.js"></script>
 *
 * Dibuja detrás del contenido del header (lima) una red técnica de nodos
 * oscuros que se mueven despacio y rebotan en los bordes. Los nodos cercanos
 * al cursor reaccionan con repulsión suave (con easing) y se conectan a él
 * con líneas oscuras. Sin librerías. Los colores salen de las variables de
 * comun/theme.css (con la paleta como respaldo).
 */
(function () {
    'use strict';

    /* Respaldo (el header es lima: los nodos van en tonos oscuros) */
    const FALLBACK = {
        dark: '#0A0A0C',
        g1: '#4C5C2D',
        g2: '#788428',
    };

    const NODES_MIN = 40;
    const NODES_MAX = 90;
    const LINK_DIST = 78;         /* distancia máxima entre nodos para unirlos */
    const LINK_MAX_PER_NODE = 5;
    const CURSOR_LINK_DIST = 150; /* distancia máxima nodo-cursor */
    const REPULSE_DIST = 110;     /* radio de reacción al cursor */
    const REPULSE_FORCE = 46;     /* px/s^2 a distancia 0 */
    const SPEED_MIN = 6;          /* px/s */
    const SPEED_MAX = 15;

    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    /* ── utilidades de color ── */
    function hexToRgb(hex) {
        const n = parseInt(hex.slice(1), 16);
        return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
    }

    function cssVar(style, name, fallback) {
        const v = style.getPropertyValue(name).trim();
        return /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.test(v) ? v : fallback;
    }

    function readPalette() {
        const style = window.getComputedStyle(document.documentElement);
        return {
            /* Sobre el header lima los nodos son oscuros; cerca del cursor
               se funden al negro base. */
            a: hexToRgb(cssVar(style, '--g1', FALLBACK.g1)),
            b: hexToRgb(cssVar(style, '--g2', FALLBACK.g2)),
            hot: hexToRgb(cssVar(style, '--bg', FALLBACK.dark)),
        };
    }

    const mix = (a, b, t) => [
        Math.round(a[0] + (b[0] - a[0]) * t),
        Math.round(a[1] + (b[1] - a[1]) * t),
        Math.round(a[2] + (b[2] - a[2]) * t),
    ];

    const rgba = (rgb, a) => `rgba(${rgb[0]},${rgb[1]},${rgb[2]},${a})`;

    const rand = (min, max) => min + Math.random() * (max - min);

    /* ── instancia ── */
    function create(header) {
        const canvas = document.createElement('canvas');
        canvas.className = 'ae-nodes';
        canvas.setAttribute('aria-hidden', 'true');
        header.prepend(canvas);

        const ctx = canvas.getContext('2d', { alpha: true });
        if (!ctx) return null;

        const P = readPalette();

        let W = 1;
        let H = 1;
        let DPR = 1;
        let nodes = [];
        let raf = null;
        let last = 0;
        let running = false;

        const mouse = { x: -9999, y: -9999, active: false };

        function makeNode() {
            const base = Math.random() < 0.5 ? P.b : P.a;
            const angle = rand(0, Math.PI * 2);
            const speed = rand(SPEED_MIN, SPEED_MAX);
            const vx = Math.cos(angle) * speed;
            const vy = Math.sin(angle) * speed;
            return {
                x: rand(0, W),
                y: rand(0, H),
                r: rand(0.8, 1.7),
                vx,
                vy,
                bvx: vx,
                bvy: vy,
                base,
                heat: 0,
            };
        }

        function resize() {
            const rect = header.getBoundingClientRect();
            W = Math.max(1, Math.round(rect.width));
            H = Math.max(1, Math.round(rect.height));
            DPR = Math.min(2.5, window.devicePixelRatio || 1);

            canvas.width = Math.round(W * DPR);
            canvas.height = Math.round(H * DPR);
            canvas.style.width = W + 'px';
            canvas.style.height = H + 'px';

            const target = Math.max(NODES_MIN, Math.min(NODES_MAX, Math.round(W / 16)));

            if (nodes.length > target) {
                nodes.length = target;
            } else {
                while (nodes.length < target) nodes.push(makeNode());
            }

            nodes.forEach(n => {
                if (n.x > W) n.x = rand(0, W);
                if (n.y > H) n.y = rand(0, H);
            });

            if (reducedMotion) draw();
        }

        function step(dt) {
            for (const n of nodes) {
                /* Repulsión suave cerca del cursor, con easing posterior. */
                if (mouse.active) {
                    const dx = n.x - mouse.x;
                    const dy = n.y - mouse.y;
                    const d = Math.hypot(dx, dy) || 0.0001;
                    if (d < REPULSE_DIST) {
                        const f = (1 - d / REPULSE_DIST);
                        const push = (f * f * REPULSE_FORCE) * dt;
                        n.vx += (dx / d) * push;
                        n.vy += (dy / d) * push;
                    }
                }

                /* Vuelve poco a poco a su deriva base. */
                n.vx += (n.bvx - n.vx) * Math.min(1, dt * 2.2);
                n.vy += (n.bvy - n.vy) * Math.min(1, dt * 2.2);

                n.x += n.vx * dt;
                n.y += n.vy * dt;

                /* Rebote en los bordes. */
                if (n.x < n.r) { n.x = n.r; n.vx = Math.abs(n.vx); }
                else if (n.x > W - n.r) { n.x = W - n.r; n.vx = -Math.abs(n.vx); }
                if (n.y < n.r) { n.y = n.r; n.vy = Math.abs(n.vy); }
                else if (n.y > H - n.r) { n.y = H - n.r; n.vy = -Math.abs(n.vy); }

                /* "Calor" por cercanía al cursor (easing). */
                let targetHeat = 0;
                if (mouse.active) {
                    const d = Math.hypot(n.x - mouse.x, n.y - mouse.y);
                    if (d < REPULSE_DIST + 40) {
                        targetHeat = Math.max(0, 1 - d / (REPULSE_DIST + 40));
                    }
                }
                n.heat += (targetHeat - n.heat) * Math.min(1, dt * 6);
            }
        }

        function nodeColor(n) {
            const t = Math.min(1, n.heat * 1.3);
            return mix(n.base, P.hot, t);
        }

        function draw() {
            ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
            ctx.clearRect(0, 0, W, H);

            /* Líneas entre nodos cercanos (opacidad baja con la distancia). */
            ctx.lineWidth = 1;
            const links = new Array(nodes.length).fill(0);
            for (let i = 0; i < nodes.length; i++) {
                const a = nodes[i];
                if (links[i] >= LINK_MAX_PER_NODE) continue;
                for (let j = i + 1; j < nodes.length; j++) {
                    if (links[j] >= LINK_MAX_PER_NODE) continue;
                    const b = nodes[j];
                    const dx = a.x - b.x;
                    const dy = a.y - b.y;
                    const d2 = dx * dx + dy * dy;
                    if (d2 > LINK_DIST * LINK_DIST) continue;
                    const d = Math.sqrt(d2);
                    const alpha = (1 - d / LINK_DIST) * 0.2;
                    const heat = Math.max(a.heat, b.heat);
                    const color = heat > 0.35
                        ? mix(P.b, P.hot, Math.min(1, heat))
                        : P.b;
                    ctx.strokeStyle = rgba(color, alpha + heat * 0.14);
                    ctx.beginPath();
                    ctx.moveTo(a.x, a.y);
                    ctx.lineTo(b.x, b.y);
                    ctx.stroke();
                    links[i]++;
                    links[j]++;
                }
            }

            /* Líneas del cursor a los nodos cercanos (acento, opacidad decreciente). */
            if (mouse.active) {
                for (const n of nodes) {
                    const d = Math.hypot(n.x - mouse.x, n.y - mouse.y);
                    if (d >= CURSOR_LINK_DIST) continue;
                    const alpha = (1 - d / CURSOR_LINK_DIST) * 0.55;
                    ctx.strokeStyle = rgba(P.hot, alpha);
                    ctx.beginPath();
                    ctx.moveTo(mouse.x, mouse.y);
                    ctx.lineTo(n.x, n.y);
                    ctx.stroke();
                }
                ctx.fillStyle = rgba(P.hot, 0.55);
                ctx.beginPath();
                ctx.arc(mouse.x, mouse.y, 1.6, 0, Math.PI * 2);
                ctx.fill();
            }

            /* Nodos. */
            for (const n of nodes) {
                const c = nodeColor(n);
                if (n.heat > 0.15) {
                    ctx.fillStyle = rgba(c, 0.12 * n.heat);
                    ctx.beginPath();
                    ctx.arc(n.x, n.y, n.r + 3.5 * n.heat, 0, Math.PI * 2);
                    ctx.fill();
                }
                ctx.fillStyle = rgba(c, 0.55 + 0.45 * n.heat);
                ctx.beginPath();
                ctx.arc(n.x, n.y, n.r, 0, Math.PI * 2);
                ctx.fill();
            }
        }

        function loop(ts) {
            if (!running) return;
            const dt = Math.min(0.05, (ts - last) / 1000 || 0);
            last = ts;
            step(dt);
            draw();
            raf = window.requestAnimationFrame(loop);
        }

        function start() {
            if (running || reducedMotion) return;
            running = true;
            last = performance.now();
            raf = window.requestAnimationFrame(loop);
        }

        function stop() {
            running = false;
            if (raf) window.cancelAnimationFrame(raf);
            raf = null;
        }

        /* ── interacción con el ratón (solo dentro del header) ── */
        function onMove(e) {
            const rect = canvas.getBoundingClientRect();
            mouse.x = e.clientX - rect.left;
            mouse.y = e.clientY - rect.top;
            mouse.active = mouse.x >= 0 && mouse.y >= 0 && mouse.x <= W && mouse.y <= H;
            if (reducedMotion) draw();
        }

        function onLeave() {
            mouse.active = false;
            mouse.x = -9999;
            mouse.y = -9999;
            if (reducedMotion) draw();
        }

        header.addEventListener('mousemove', onMove, { passive: true });
        header.addEventListener('mouseleave', onLeave, { passive: true });

        /* ── tamaño / visibilidad ── */
        if (typeof ResizeObserver !== 'undefined') {
            new ResizeObserver(resize).observe(header);
        }
        window.addEventListener('resize', resize, { passive: true });

        document.addEventListener('visibilitychange', () => {
            if (document.hidden) stop();
            else start();
        });

        resize();
        if (reducedMotion) draw();
        else start();

        return { resize, stop, start };
    }

    function init() {
        const header = document.querySelector('.ae-header');
        if (!header || header.querySelector('.ae-nodes')) return;
        create(header);
    }

    /* header.js inyecta el header; nos aseguramos de exista ya o de esperarlo. */
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    const observer = new MutationObserver(() => {
        if (document.querySelector('.ae-header')) {
            init();
            observer.disconnect();
        }
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
    window.setTimeout(() => observer.disconnect(), 5000);
})();
