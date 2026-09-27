# Design System Master File — ALETHEIA

> **LOGIC:** When building a specific page, first check `design-system/aletheia/pages/[page-name].md`.
> If that file exists, its rules **override** this Master file.
> If not, strictly follow the rules below.

---

**Project:** ALETHEIA (analítica + predicción Valorant VCT)
**Category:** Gaming / Esports — dark, glassmorphism
**Design Dials:** Variance 7/10 (Balanced / Modern) | Motion 7/10 (Standard) | Density 6/10 (Standard)
**Fuente de verdad:** este archivo. Cualquier cambio visual del sitio debe respetarlo.

**Estado de implementación:**
- ✅ Fases 1–3 (2026-09): tokens en `comun/vct.css`, glass en `header/header.css`, cards/paneles VCT,
  outline en títulos y hero canvas vanilla en `aletheia/` (`comun/hero.js`). Lima `#e8ff47` eliminada
  en header + VCT + EN VIVO.
- ⏳ Fase 4: adaptar componentes de `estilos/` (cuando exista) a `ae-*`/`v-*`.
- ⏳ Páginas pendientes de migrar: `inicio/`, `visualizar/`, `aletheia_preparar/` (siguen con lima).
  `tablas/` no se migra.

---

## Global Rules

### Estilo base: Glassmorphism (dark esports)

- Paneles/tarjetas translúcidos sobre fondo con **blobs de color desenfocados** (violeta/azul).
- `backdrop-filter: blur(12–20px)` + borde sutil `1px solid rgba(255,255,255,.10)` + highlight interior.
- Profundidad con sombras suaves; **nunca** mezclar con esquinas duras tipo terminal.
- El hero de la portada/EN VIVO lleva **canvas wireframe 2D** (partículas conectadas) detrás de tipografía grande; el resto de páginas usan solo blobs CSS.
- **Tablas densas, scoreboards y `tablas/` se mantienen opacos y legibles** (sin blur sobre datos).

### Color Palette

| Role | Hex | CSS Variable |
|------|-----|--------------|
| Primary (violeta) | `#7C3AED` | `--color-primary` |
| On Primary | `#FFFFFF` | `--color-on-primary` |
| Secondary (violeta claro) | `#A78BFA` | `--color-secondary` |
| On Secondary | `#0F172A` | `--color-on-secondary` |
| Accent / CTA (azul) | `#38BDF8` | `--color-accent` |
| On Accent | `#0B1020` | `--color-on-accent` |
| Background | `#0F0F23` | `--color-background` |
| Foreground | `#E2E8F0` | `--color-foreground` |
| Card | `#1E1C35` | `--color-card` |
| Card Foreground | `#E2E8F0` | `--color-card-foreground` |
| Muted | `#27273B` | `--color-muted` |
| Muted Foreground | `#94A3B8` | `--color-muted-foreground` |
| Border | `#4C1D95` (a baja opacidad en glass) | `--color-border` |
| Destructive / derrota | `#EF4444` | `--color-destructive` |
| Win / victoria | `#4ADE80` | `--color-success` |
| Live / alerta suave | `#F43F5E` | `--color-live` |
| Ring (focus) | `#7C3AED` | `--color-ring` |

**Color Notes:** violeta neón como marca; azul cian para acciones/links; verde/rojo solo semánticos (V/D y estados); rosa reservado a “EN VIVO”.

### Glass Tokens (nuevos, obligatorios para glass)

```css
:root {
  --glass-bg: rgba(30, 28, 53, 0.42);          /* superficie translúcida */
  --glass-bg-strong: rgba(30, 28, 53, 0.66);   /* header / modales */
  --glass-border: rgba(255, 255, 255, 0.10);
  --glass-highlight: rgba(255, 255, 255, 0.06);/* inset top highlight */
  --glass-blur: 14px;
  --glass-shadow: 0 12px 40px rgba(5, 5, 20, 0.55);
  --blob-violet: rgba(124, 58, 237, 0.35);
  --blob-blue: rgba(56, 189, 248, 0.22);
  --blob-magenta: rgba(217, 70, 239, 0.16);
}
```

### Typography

- **Display / títulos:** **Russo One** (reemplaza a Bebas Neue en títulos grandes; soporta el estilo outline).
- **UI / cuerpo:** **Chakra Petch** (300–700).
- **Datos numéricos:** **DM Mono** (se mantiene, solo en tablas/scoreboards/cifras; `tabular-nums`).
- **Mood:** gaming, bold, esports, competitive, energetic.
- Google Fonts: `family=Russo+One&family=Chakra+Petch:wght@300;400;500;600;700&family=DM+Mono:wght@400;500`
- Escala sugerida: 12 / 14 / 16 / 20 / 24 / 34 / 48 / 72 (hero).
- **Outline (solo títulos/hero):**
  ```css
  .outline-text {
    color: transparent;
    -webkit-text-stroke: 1.5px rgba(226, 232, 240, 0.9);
  }
  @supports not (-webkit-text-stroke: 1px black) { .outline-text { color: var(--color-foreground); } }
  ```

### Spacing Variables

| Token | Value | Usage |
|-------|-------|-------|
| `--space-xs` | `4px` | Gaps finos |
| `--space-sm` | `8px` | Iconos / inline |
| `--space-md` | `16px` | Padding estándar |
| `--space-lg` | `24px` | Secciones |
| `--space-xl` | `32px` | Gaps grandes |
| `--space-2xl` | `48px` | Márgenes de sección |
| `--space-3xl` | `64px` | Hero |

### Shadow Depths (glass-friendly)

| Level | Value | Usage |
|-------|-------|-------|
| `--shadow-sm` | `0 2px 8px rgba(5,5,20,.35)` | Chips, inputs |
| `--shadow-md` | `0 8px 24px rgba(5,5,20,.45)` | Cards, dropdowns |
| `--shadow-lg` | `0 16px 48px rgba(5,5,20,.55)` | Modales, hero cards |
| `--shadow-glow` | `0 0 24px rgba(124,58,237,.25)` | Hover de acento |

---

## Component Specs (mapping a clases existentes)

> **Regla de oro:** no se crean clases nuevas para reemplazar `ae-*`/`v-*`; se
> restilizan las existentes. Los snippets son referencia de tokens.

### Header (`header/header.css`, clases `ae-*`)

```css
.ae-header {
  background: var(--glass-bg-strong);
  backdrop-filter: blur(var(--glass-blur)) saturate(140%);
  border-bottom: 1px solid var(--glass-border);
}
.ae-btn.active { border-color: var(--color-accent); color: var(--color-accent); }
```

### Cards VCT (`.v-card`, `.v-panel`, `.v-stat`, `.v-banner`)

```css
.v-card {
  background: var(--glass-bg);
  backdrop-filter: blur(var(--glass-blur));
  border: 1px solid var(--glass-border);
  box-shadow: inset 0 1px 0 var(--glass-highlight), var(--shadow-md);
  border-radius: 10px;
}
.v-card:hover { border-color: rgba(124,58,237,.45); box-shadow: var(--shadow-lg), var(--shadow-glow); }
```

### Tablas / scoreboards (`.v-table`, `.v-board`, `tablas/`) — **opaco, sin glass**

```css
.v-table th { background: var(--color-muted); }
.v-table td { background: rgba(15, 15, 35, 0.92); }
```

### Botones / inputs (`.v-btn`, `.v-input`, `.v-select`)

```css
.v-btn:hover, .v-input:focus, .v-select:focus {
  border-color: var(--color-accent);
  box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.18);
}
```

### Blobs de fondo (`.v-bg` y equivalentes por página)

```css
.v-bg {
  background:
    radial-gradient(720px 420px at 12% -10%, var(--blob-violet), transparent 65%),
    radial-gradient(640px 380px at 88% 8%, var(--blob-blue), transparent 60%),
    radial-gradient(520px 320px at 50% 110%, var(--blob-magenta), transparent 65%);
  filter: blur(60px) saturate(120%);
}
```

### Hero animado (solo portada / EN VIVO)

- Canvas 2D **vanilla** (sin librerías): red de partículas conectadas, ~30 fps,
  pausa con `IntersectionObserver` + `visibilitychange`, desactivado con
  `prefers-reduced-motion`, `devicePixelRatio` máximo 1.5, partículas según área
  (40–70), líneas solo entre vecinos (<120px). Presupuesto: **< 2 ms/frame**.
- Tipografía del hero: Russo One grande + variante outline; el canvas va detrás
  con `pointer-events: none`.

---

## Motion

- **Standard**: stagger de entrada 300–450ms, `cubic-bezier(.22,1,.36,1)`.
- **Implementación:** CSS-only (`animation-delay` por hijo o `nth-child`) —
  **no añadir GSAP** (el sitio es vanilla, sin build; mantener 0 dependencias).
- Respetar `prefers-reduced-motion: reduce` → render final inmediato.
- ❌ No usar `back.out` ni overshoot en tablas de datos.
- ❌ No animar `width/height/top/left` (solo `transform`/`opacity`).
- Hover: 150–250ms; press: 80–150ms.

---

## Alcance (scope) de esta identidad

| Zona | Glass | Hero canvas | Notas |
|------|-------|-------------|-------|
| `header/` | ✅ | — | Compartido por todas las páginas (incluida `tablas/`, que no se modifica) |
| `partidos/`, `equipos/`, `jugadores/`, `eventos/` | ✅ cards/paneles/banners | ❌ | Tablas y scoreboards **opacos** |
| `aletheia/` (EN VIVO, portada) | ✅ | ✅ | Único lugar con canvas animado |
| `aletheia_preparar/`, `inicio/`, `visualizar/` | ✅ progresivo | ❌ | Se adaptan al mismo sistema |
| `tablas/` | ❌ | ❌ | Explorador raw — **no se toca** |
| `estilos/` (cuando exista) | — | — | Fuente de componentes; se **adaptan** a `ae-*`/`v-*`, nunca al revés |

---

## Anti-Patterns (Do NOT Use)

- ❌ Minimalismo plano o estilos “terminal” duros (el proyecto es esports/glass).
- ❌ WebGL/Three.js o librerías de partículas (peso y GPU innecesarios).
- ❌ Glass detrás de tablas densas o texto pequeño (mata la legibilidad).
- ❌ Emojis como iconos — usar SVG (Heroicons/Lucide).
- ❌ Colores aleatorios de equipo: el color sale de `--c` (color medio del logo).
- ❌ `cursor:pointer` faltante en elementos clickeables.
- ❌ Cambios de estado instantáneos (siempre 150–300ms) o sin `:focus-visible`.
- ❌ Degradar la tasa de acierto: **la precisión manda** (DOCUMENTACION.md §1).

---

## Pre-Delivery Checklist

- [ ] Contraste de texto ≥ 4.5:1 sobre glass (medir el fondo compuesto, no el token).
- [ ] `prefers-reduced-motion` respetado (canvas y stagger se apagan).
- [ ] Canvas pausado cuando no está visible / pestaña oculta.
- [ ] Sin scroll horizontal en 375 / 768 / 1024 / 1440.
- [ ] Focus visible (ring violeta) en nav, cards, tabs y botones.
- [ ] Tablas y scoreboards siguen 100% legibles (sin blur bajo texto).
- [ ] Clases `ae-*`/`v-*` y lógica JS intactas; enlaces sin `/index.html`.
- [ ] Sin nuevas dependencias JS/fuentes pesadas (Russo One + Chakra Petch + DM Mono).
