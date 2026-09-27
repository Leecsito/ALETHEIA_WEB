/**
 * ALETHEIA — Hero canvas: red de partículas conectadas (wireframe 2D).
 *
 * Vanilla canvas 2D, CERO dependencias. Presupuesto: ~30 fps, <2 ms/frame.
 * Optimizaciones:
 *  - Pausa cuando el hero no está visible (IntersectionObserver) o la pestaña
 *    está oculta (visibilitychange).
 *  - `prefers-reduced-motion`: dibuja un único frame estático y no anima.
 *  - devicePixelRatio limitado a 1.5 y partículas según área (30–70).
 *  - Líneas solo entre vecinos (<120px); O(n²) con n≤70 es despreciable.
 */
(() => {
    'use strict';

    const canvas = document.getElementById('aeHeroCanvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d', { alpha: true });
    if (!ctx) return;

    const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)');
    const MAX_DPR = 1.5;
    const MIN_PARTICLES = 30;
    const MAX_PARTICLES = 70;
    const AREA_PER_PARTICLE = 26000;
    const LINK_DIST = 120;
    const TARGET_FPS = 30;
    const VELOCITY = 0.22;

    let W = 0, H = 0;
    let particles = [];
    let rafId = null;
    let lastPaint = 0;
    let visible = true;

    function initParticles(n) {
        particles = Array.from({ length: n }, () => ({
            x: Math.random() * W,
            y: Math.random() * H,
            vx: (Math.random() - 0.5) * 2 * VELOCITY,
            vy: (Math.random() - 0.5) * 2 * VELOCITY,
            r: Math.random() * 1.6 + 0.9,
            cyan: Math.random() < 0.42,
        }));
    }

    function resize() {
        const rect = canvas.parentElement.getBoundingClientRect();
        W = Math.max(1, Math.round(rect.width));
        H = Math.max(1, Math.round(rect.height));
        const dpr = Math.min(window.devicePixelRatio || 1, MAX_DPR);
        canvas.width = Math.round(W * dpr);
        canvas.height = Math.round(H * dpr);
        canvas.style.width = `${W}px`;
        canvas.style.height = `${H}px`;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

        const n = Math.round(Math.min(MAX_PARTICLES,
            Math.max(MIN_PARTICLES, (W * H) / AREA_PER_PARTICLE)));
        if (particles.length !== n) initParticles(n);
        if (reduceMotion.matches) draw();   // frame estático
    }

    function step() {
        for (const p of particles) {
            p.x += p.vx;
            p.y += p.vy;
            if (p.x < -20) p.x = W + 20;
            else if (p.x > W + 20) p.x = -20;
            if (p.y < -20) p.y = H + 20;
            else if (p.y > H + 20) p.y = -20;
        }
    }

    function draw() {
        ctx.clearRect(0, 0, W, H);

        // Líneas (vecinos cercanos)
        for (let i = 0; i < particles.length; i++) {
            const a = particles[i];
            for (let j = i + 1; j < particles.length; j++) {
                const b = particles[j];
                const dx = a.x - b.x;
                const dy = a.y - b.y;
                const d2 = dx * dx + dy * dy;
                if (d2 > LINK_DIST * LINK_DIST) continue;
                const alpha = (1 - Math.sqrt(d2) / LINK_DIST) * 0.34;
                ctx.strokeStyle = a.cyan && b.cyan
                    ? `rgba(56, 189, 248, ${alpha})`
                    : `rgba(124, 58, 237, ${alpha})`;
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.moveTo(a.x, a.y);
                ctx.lineTo(b.x, b.y);
                ctx.stroke();
            }
        }

        // Puntos
        for (const p of particles) {
            ctx.beginPath();
            ctx.fillStyle = p.cyan ? 'rgba(56, 189, 248, .85)' : 'rgba(167, 139, 250, .85)';
            ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
            ctx.fill();
        }
    }

    function frame(ts) {
        rafId = requestAnimationFrame(frame);
        const minDelta = 1000 / TARGET_FPS;
        if (ts - lastPaint < minDelta) return;
        lastPaint = ts;
        step();
        draw();
    }

    function start() {
        if (rafId !== null || reduceMotion.matches) return;
        rafId = requestAnimationFrame(frame);
    }

    function stop() {
        if (rafId !== null) {
            cancelAnimationFrame(rafId);
            rafId = null;
        }
    }

    function sync() {
        if (visible && !document.hidden) start();
        else stop();
    }

    // Visibilidad del hero y de la pestaña
    if ('IntersectionObserver' in window) {
        new IntersectionObserver(entries => {
            visible = entries.some(e => e.isIntersecting);
            sync();
        }, { threshold: 0 }).observe(canvas.parentElement);
    }
    document.addEventListener('visibilitychange', sync);

    // Resize (debounced)
    let resizeTimer = null;
    const onResize = () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(resize, 120);
    };
    if ('ResizeObserver' in window) new ResizeObserver(onResize).observe(canvas.parentElement);
    else window.addEventListener('resize', onResize);

    reduceMotion.addEventListener?.('change', () => {
        stop();
        resize();
        sync();
    });

    resize();
    sync();
})();
