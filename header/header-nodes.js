/**
 * ALETHEIA — Canvas de nodos reutilizable.
 *
 * Se incluye en cada página DESPUÉS de header.js:
 *     <script src="../header/header.js"></script>
 *     <script src="../header/header-nodes.js"></script>
 *
 * Monta el efecto en:
 *   1. El header (nodos oscuros sobre el fondo lima; cerca del cursor se
 *      funden a negro).
 *   2. Un canvas fijo detrás del contenido de toda la página (`.ae-nodes-bg`),
 *      sutil: nodos verde oscuro sobre el fondo negro, líneas tenues y
 *      líneas al cursor en acento.
 *
 * Sin librerías. Los colores salen de las variables de comun/theme.css
 * (con la paleta como respaldo). Se puede desactivar el fondo con
 * `window.AE_NODES_BG = false` antes de cargar el script.
 */
(function () {
    'use strict';

    /* Paleta de respaldo (la misma que comun/theme.css) */
    const FALLBACK = { bg: '#0A0A0C', g1: '#4C5C2D', g2: '#788428', g3: '#B0C138', accent: '#E8FF47' };

    /* Cada modo define colores (variables CSS), densidad y comportamiento. */
    const MODES = {
        /* Header lima: nodos oscuros que se funden a negro al acercarse. */
        header: {
            vars: { a: '--g1', b: '--g2', hot: '--bg' },
            count: { div: 16, min: 40, max: 90 },
            linkDist: 80,
            linkAlpha: 0.2,
            linkHeat: 0.14,
            cursorDist: 150,
            cursorAlpha: 0.55,
            speed: [6, 15],
            radius: [0.9, 1.8],
            fixed: false,
        },
        /* Fondo oscuro: red VERDE visible detrás del contenido; el cursor la enciende.
           Densidad acotada y DPR máximo 1.25 (el fondo es decorativo): baja ~6x
           los píxeles por frame y los pares de enlaces respecto a 300 nodos/DPR 2.5. */
        bg: {
            vars: { a: '--g2', b: '--g3', hot: '--g3', spark: '--g3' },
            count: { div: 14, min: 32, max: 110 },
            linkDist: 125,
            linkAlpha: 0.26,
            linkHeat: 0.2,
            cursorDist: 180,
            cursorAlpha: 0.6,
            speed: [4, 11],
            radius: [0.9, 1.9],
            nodeAlpha: 0.72,
            halo: 0.13,
            dprMax: 1.25,
            fixed: true,
        },
    };

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

    function readPalette(mode) {
        const style = window.getComputedStyle(document.documentElement);
        const v = mode.vars;
        return {
            a: hexToRgb(cssVar(style, v.a, FALLBACK.g1)),
            b: hexToRgb(cssVar(style, v.b, FALLBACK.g2)),
            hot: hexToRgb(cssVar(style, v.hot, v.hot === '--g3' ? FALLBACK.g3 : FALLBACK.bg)),
            spark: v.spark ? hexToRgb(cssVar(style, v.spark, FALLBACK.g3)) : null,
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
    function create(canvas, mode, sizeEl, mouseEl) {
        const ctx = canvas.getContext('2d', { alpha: true });
        if (!ctx) return null;

        const P = readPalette(mode);

        let W = 1;
        let H = 1;
        let DPR = 1;
        let nodes = [];
        let raf = null;
        let last = 0;
        let running = false;

        const mouse = { x: -9999, y: -9999, active: false };

        function makeNode() {
            const base = (P.spark && Math.random() < 0.16)
                ? P.spark
                : (Math.random() < 0.5 ? P.b : P.a);
            const angle = rand(0, Math.PI * 2);
            const speed = rand(mode.speed[0], mode.speed[1]);
            const vx = Math.cos(angle) * speed;
            const vy = Math.sin(angle) * speed;
            return {
                x: rand(0, W),
                y: rand(0, H),
                r: rand(mode.radius[0], mode.radius[1]),
                vx,
                vy,
                bvx: vx,
                bvy: vy,
                base,
                heat: 0,
            };
        }

        function size() {
            if (sizeEl) {
                const rect = sizeEl.getBoundingClientRect();
                W = Math.max(1, Math.round(rect.width));
                H = Math.max(1, Math.round(rect.height));
            } else {
                W = Math.max(1, window.innerWidth);
                H = Math.max(1, window.innerHeight);
            }
            DPR = Math.min(mode.dprMax || 2.5, window.devicePixelRatio || 1);

            canvas.width = Math.round(W * DPR);
            canvas.height = Math.round(H * DPR);
            canvas.style.width = W + 'px';
            canvas.style.height = H + 'px';

            const c = mode.count;
            const target = Math.max(c.min, Math.min(c.max, Math.round(W / c.div)));

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
            const repulse = mode.fixed ? 120 : 110;
            const force = mode.fixed ? 34 : 46;
            const kick = mode.speed[0] * 0.6;
            for (const n of nodes) {
                /* Repulsión suave cerca del cursor, con easing posterior. */
                if (mouse.active) {
                    const dx = n.x - mouse.x;
                    const dy = n.y - mouse.y;
                    const d = Math.hypot(dx, dy) || 0.0001;
                    if (d < repulse) {
                        const f = (1 - d / repulse);
                        const push = (f * f * force) * dt;
                        n.vx += (dx / d) * push;
                        n.vy += (dy / d) * push;
                    }
                }

                /* Vuelve poco a poco a su deriva base. */
                n.vx += (n.bvx - n.vx) * Math.min(1, dt * 2.2);
                n.vy += (n.bvy - n.vy) * Math.min(1, dt * 2.2);

                n.x += n.vx * dt;
                n.y += n.vy * dt;

                /* Rebote en los bordes: refleja también la deriva base para
                   que el nodo vuelva al campo y no quede pegado a la pared. */
                if (n.x < n.r) { n.x = n.r; n.vx = Math.abs(n.vx); n.bvx = Math.max(Math.abs(n.bvx), kick); }
                else if (n.x > W - n.r) { n.x = W - n.r; n.vx = -Math.abs(n.vx); n.bvx = -Math.max(Math.abs(n.bvx), kick); }
                if (n.y < n.r) { n.y = n.r; n.vy = Math.abs(n.vy); n.bvy = Math.max(Math.abs(n.bvy), kick); }
                else if (n.y > H - n.r) { n.y = H - n.r; n.vy = -Math.abs(n.vy); n.bvy = -Math.max(Math.abs(n.bvy), kick); }

                /* "Calor" por cercanía al cursor (easing). */
                let targetHeat = 0;
                if (mouse.active) {
                    const d = Math.hypot(n.x - mouse.x, n.y - mouse.y);
                    if (d < repulse + 40) targetHeat = Math.max(0, 1 - d / (repulse + 40));
                }
                n.heat += (targetHeat - n.heat) * Math.min(1, dt * 6);
            }
        }

        function nodeColor(n) {
            return mix(n.base, P.hot, Math.min(1, n.heat * 1.3));
        }

        function draw() {
            ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
            ctx.clearRect(0, 0, W, H);

            /* Líneas entre nodos cercanos (opacidad baja con la distancia).
               Rejilla espacial: solo se comparan celdas vecinas, así el coste
               no crece con O(n²) al subir la densidad. */
            ctx.lineWidth = 1;
            const links = new Array(nodes.length).fill(0);
            const cell = mode.linkDist;
            const grid = new Map();
            for (let i = 0; i < nodes.length; i++) {
                const n = nodes[i];
                const k = Math.floor(n.x / cell) + ',' + Math.floor(n.y / cell);
                let bucket = grid.get(k);
                if (!bucket) grid.set(k, bucket = []);
                bucket.push(i);
            }
            const dist2 = mode.linkDist * mode.linkDist;
            for (let i = 0; i < nodes.length; i++) {
                const a = nodes[i];
                if (links[i] >= 5) continue;
                const gx = Math.floor(a.x / cell);
                const gy = Math.floor(a.y / cell);
                for (let cx = gx - 1; cx <= gx + 1; cx++) {
                    for (let cy = gy - 1; cy <= gy + 1; cy++) {
                        const bucket = grid.get(cx + ',' + cy);
                        if (!bucket) continue;
                        for (const j of bucket) {
                            if (j <= i || links[j] >= 5) continue;
                            const b = nodes[j];
                            const dx = a.x - b.x;
                            const dy = a.y - b.y;
                            const d2 = dx * dx + dy * dy;
                            if (d2 > dist2) continue;
                            const d = Math.sqrt(d2);
                            const alpha = (1 - d / mode.linkDist) * mode.linkAlpha;
                            const heat = Math.max(a.heat, b.heat);
                            const color = heat > 0.35 ? mix(P.b, P.hot, Math.min(1, heat)) : P.b;
                            ctx.strokeStyle = rgba(color, alpha + heat * mode.linkHeat);
                            ctx.beginPath();
                            ctx.moveTo(a.x, a.y);
                            ctx.lineTo(b.x, b.y);
                            ctx.stroke();
                            links[i]++;
                            links[j]++;
                        }
                    }
                }
            }

            /* Líneas del cursor a los nodos cercanos (opacidad decreciente). */
            if (mouse.active) {
                for (const n of nodes) {
                    const d = Math.hypot(n.x - mouse.x, n.y - mouse.y);
                    if (d >= mode.cursorDist) continue;
                    const alpha = (1 - d / mode.cursorDist) * mode.cursorAlpha;
                    ctx.strokeStyle = rgba(P.hot, alpha);
                    ctx.beginPath();
                    ctx.moveTo(mouse.x, mouse.y);
                    ctx.lineTo(n.x, n.y);
                    ctx.stroke();
                }
                ctx.fillStyle = rgba(P.hot, mode.cursorAlpha);
                ctx.beginPath();
                ctx.arc(mouse.x, mouse.y, 1.6, 0, Math.PI * 2);
                ctx.fill();
            }

            /* Nodos. */
            const nodeAlpha = mode.nodeAlpha || 0.55;
            for (const n of nodes) {
                const c = nodeColor(n);
                if (mode.halo) {
                    ctx.fillStyle = rgba(c, mode.halo);
                    ctx.beginPath();
                    ctx.arc(n.x, n.y, n.r + 2.6, 0, Math.PI * 2);
                    ctx.fill();
                }
                if (n.heat > 0.15) {
                    ctx.fillStyle = rgba(c, 0.12 * n.heat);
                    ctx.beginPath();
                    ctx.arc(n.x, n.y, n.r + 3.5 * n.heat, 0, Math.PI * 2);
                    ctx.fill();
                }
                ctx.fillStyle = rgba(c, nodeAlpha + (1 - nodeAlpha) * n.heat);
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

        /* ── ratón ── */
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

        mouseEl.addEventListener('mousemove', onMove, { passive: true });
        mouseEl.addEventListener('mouseleave', onLeave, { passive: true });

        /* ── tamaño / visibilidad ── */
        if (sizeEl && typeof ResizeObserver !== 'undefined') {
            new ResizeObserver(size).observe(sizeEl);
        } else {
            window.addEventListener('resize', size, { passive: true });
        }

        document.addEventListener('visibilitychange', () => {
            if (document.hidden) stop();
            else start();
        });

        size();
        if (reducedMotion) draw();
        else start();

        return { size, stop, start };
    }

    function initHeader(header) {
        if (!header || header.querySelector('.ae-nodes')) return;
        const canvas = document.createElement('canvas');
        canvas.className = 'ae-nodes';
        canvas.setAttribute('aria-hidden', 'true');
        header.prepend(canvas);
        create(canvas, MODES.header, header, header);
    }

    function initBg() {
        if (window.AE_NODES_BG === false) return;
        if (document.querySelector('.ae-nodes-bg')) return;
        const canvas = document.createElement('canvas');
        canvas.className = 'ae-nodes-bg';
        canvas.setAttribute('aria-hidden', 'true');
        document.body.appendChild(canvas);
        create(canvas, MODES.bg, null, document);
    }

    function init() {
        const header = document.querySelector('.ae-header');
        if (header) initHeader(header);
        initBg();
    }

    /* header.js inyecta el header; aseguramos que exista ya o lo esperamos. */
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
