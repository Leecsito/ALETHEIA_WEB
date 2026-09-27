/**
 * ALETHEIA — Fondo global de nodos conectados (canvas 2D vanilla, 0 dependencias).
 *
 * Va detrás de TODO el contenido en TODAS las páginas (incluida tablas/).
 * Es deliberadamente muy sutil y ligero:
 *  - 14–26 nodos según área de ventana, 24 fps, DPR fijo 1.
 *  - Líneas de 1px con alpha ≤ .10; puntos con alpha ≤ .20.
 *  - Movimiento muy lento (~0.09 px por frame).
 *  - `pointer-events:none`, `z-index:0`, insertado como primer hijo del body.
 *  - Pausa con `visibilitychange`; frame estático con `prefers-reduced-motion`.
 *  - Guard de carga única (se inyecta desde header.js, no por página).
 */
(() => {
    'use strict';
    if (window.__AE_BG_NODES__) return;
    window.__AE_BG_NODES__ = true;

    const canvas = document.createElement('canvas');
    canvas.setAttribute('aria-hidden', 'true');
    canvas.style.cssText = 'position:fixed;inset:0;width:100%;height:100%;pointer-events:none;z-index:0;';

    function insertar() {
        if (document.body && !canvas.parentNode) {
            document.body.insertBefore(canvas, document.body.firstChild);
        }
    }
    if (document.body) insertar();
    else document.addEventListener('DOMContentLoaded', insertar);

    const ctx = canvas.getContext('2d', { alpha: true });
    if (!ctx) return;

    const reduce = window.matchMedia('(prefers-reduced-motion: reduce)');
    const MIN_NODOS = 14;
    const MAX_NODOS = 26;
    const AREA_POR_NODO = 90000;
    const LINK_DIST = 110;
    const TARGET_FPS = 24;
    const VELOCIDAD = 0.09;

    let W = 0, H = 0;
    let nodos = [];
    let raf = null;
    let ultimo = 0;

    function init() {
        const n = Math.round(Math.min(MAX_NODOS, Math.max(MIN_NODOS, (W * H) / AREA_POR_NODO)));
        nodos = Array.from({ length: n }, () => ({
            x: Math.random() * W,
            y: Math.random() * H,
            vx: (Math.random() - 0.5) * 2 * VELOCIDAD,
            vy: (Math.random() - 0.5) * 2 * VELOCIDAD,
            r: Math.random() * 1.1 + 0.7,
            gris: Math.random() < 0.4,
        }));
    }

    function resize() {
        W = Math.max(1, window.innerWidth);
        H = Math.max(1, window.innerHeight);
        canvas.width = W;
        canvas.height = H;
        init();
        if (reduce.matches) draw();
    }

    function step() {
        for (const p of nodos) {
            p.x += p.vx;
            p.y += p.vy;
            if (p.x < -15) p.x = W + 15;
            else if (p.x > W + 15) p.x = -15;
            if (p.y < -15) p.y = H + 15;
            else if (p.y > H + 15) p.y = -15;
        }
    }

    function draw() {
        ctx.clearRect(0, 0, W, H);

        for (let i = 0; i < nodos.length; i++) {
            const a = nodos[i];
            for (let j = i + 1; j < nodos.length; j++) {
                const b = nodos[j];
                const dx = a.x - b.x;
                const dy = a.y - b.y;
                const d2 = dx * dx + dy * dy;
                if (d2 > LINK_DIST * LINK_DIST) continue;
                const alpha = (1 - Math.sqrt(d2) / LINK_DIST) * 0.10;
                ctx.strokeStyle = `rgba(232, 255, 71, ${alpha})`;
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.moveTo(a.x, a.y);
                ctx.lineTo(b.x, b.y);
                ctx.stroke();
            }
        }

        for (const p of nodos) {
            ctx.beginPath();
            ctx.fillStyle = p.gris ? 'rgba(212, 212, 216, .16)' : 'rgba(232, 255, 71, .20)';
            ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
            ctx.fill();
        }
    }

    function frame(ts) {
        raf = requestAnimationFrame(frame);
        if (ts - ultimo < 1000 / TARGET_FPS) return;
        ultimo = ts;
        step();
        draw();
    }

    function start() {
        if (raf !== null || reduce.matches) return;
        raf = requestAnimationFrame(frame);
    }

    function stop() {
        if (raf !== null) {
            cancelAnimationFrame(raf);
            raf = null;
        }
    }

    function sync() {
        if (document.hidden) stop();
        else start();
    }

    document.addEventListener('visibilitychange', sync);

    let t = null;
    window.addEventListener('resize', () => {
        clearTimeout(t);
        t = setTimeout(resize, 150);
    });

    reduce.addEventListener?.('change', () => {
        stop();
        resize();
        sync();
    });

    resize();
    sync();
})();
