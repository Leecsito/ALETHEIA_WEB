# Design System Master File — ALETHEIA

> **LOGIC:** When building a specific page, first check `design-system/aletheia/pages/[page-name].md`.
> If that file exists, its rules **override** this Master file.
> If not, strictly follow the rules below.

---

**Project:** ALETHEIA (analítica + predicción Valorant VCT)
**Category:** Gaming / Esports — dark, glass, black + yellow
**Design Dials:** Variance 7/10 (Balanced / Modern) | Motion 7/10 (Standard) | Density 6/10 (Standard)
**Fuente de verdad:** este archivo. Cualquier cambio visual del sitio debe respetarlo.

**Estado de implementación:**
- ✅ Paleta negra + amarilla restaurada; **sin glow** en header, VCT y EN VIVO.
- ✅ Glass neutro (blur + fondo translúcido + borde sutil) solo en superficies grandes.
- ✅ Fondo global de nodos (`comun/bg-nodes.js`) en TODAS las páginas (incluida `tablas/`),
  inyectado una sola vez desde `header.js`.
- ⏳ Fase 4: adaptar componentes de `estilos/` (cuando exista) a `ae-*`/`v-*`.
- ⏳ Páginas pendientes de migrar a glass/tipografía: `inicio/`, `visualizar/`, `aletheia_preparar/`.
  `tablas/` no se migra (solo recibe el fondo de nodos).

---

## Global Rules

### Estilo base: Glass neutro (dark esports)

- Paneles/tarjetas translúcidos con `backdrop-filter: blur()` + borde sutil + highlight interior.
- **PROHIBIDO el glow**: nada de `box-shadow` de color, `drop-shadow` de color, text-shadow
  de color ni "auras" alrededor de botones/inputs/bordes. Solo sombras neutras de profundidad
  (`rgba(0,0,0,…)`) e insets claros.
- **El blur va solo en superficies grandes/únicas** (header, tabs, paneles, banners). Las
  cards repetidas en grids usan fondo translúcido + borde, sin `backdrop-filter` (rendimiento).
- Tablas densas, scoreboards y `tablas/` se mantienen **opacos y legibles**.
- Los logos oscuros usan **tile claro plano** (`on-light`), nunca halo/glow.

### Color Palette

| Role | Hex | CSS Variable |
|------|-----|--------------|
| Fondo | `#0a0a0c` | `--bg` |
| Superficie | `#111114` | `--surface` |
| Superficie 2 | `#16161a` | `--surface2` |
| Superficie 3 | `#1c1c22` | `--surface3` |
| Borde | `#222228` | `--border` |
| **Acento (amarillo)** | `#e8ff47` | `--accent` |
| Alerta / derrota | `#ff4757` | `--accent2` |
| Victoria | `#4ade80` | `--green` |
| Aviso | `#fb923c` | `--orange` |
| Info / barras azules | `#38bdf8` | `--blue` |
| Barras moradas | `#a78bfa` | `--purple` |
| Texto | `#d4d4d8` | `--text` |
| Texto tenue | `#52525b` | `--dim` |

**Nota:** `--blue`/`--purple` son colores **semánticos de datos** (barras, chips de agente) del
esquema original; no son acentos de marca. No introducir violeta/cian/magenta como acento.

### Glass Tokens (neutros)

```css
:root {
  --glass-bg: rgba(17, 17, 20, 0.44);
  --glass-bg-strong: rgba(10, 10, 12, 0.68);
  --glass-border: rgba(255, 255, 255, 0.09);
  --glass-highlight: rgba(255, 255, 255, 0.05);
  --glass-blur: 14px;
}
```

### Typography

- **Display / títulos:** **Russo One** (soporta el estilo outline).
- **UI / cuerpo:** **Chakra Petch** (400/500/600 — pesos recortados por rendimiento).
- **Datos numéricos:** **DM Mono** (400/500) solo en tablas/scoreboards/cifras (`tabular-nums`).
- Un solo request de Google Fonts con `display=swap` (no bloquea el first paint).
- Escala sugerida: 12 / 14 / 16 / 20 / 24 / 34 / 48 / 72 (hero).
- **Outline (solo títulos/hero):**
  ```css
  .outline-text { color: transparent; -webkit-text-stroke: 1.5px rgba(212, 212, 216, .92); }
  @supports not (-webkit-text-stroke: 1px black) { .outline-text { color: var(--text); } }
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

### Shadow Depths (solo neutras)

| Level | Value | Usage |
|-------|-------|-------|
| `--shadow-card` | `0 10px 30px rgba(0,0,0,.45)` | Cards, paneles |
| inset highlight | `inset 0 1px 0 var(--glass-highlight)` | Borde superior del glass |

---

## Component Specs (mapping a clases existentes)

> **Regla de oro:** no se crean clases nuevas para reemplazar `ae-*`/`v-*`; se restilizan
> las existentes y no se toca la lógica JS.

### Header (`header/header.css`, clases `ae-*`)

```css
.ae-header {
  background: var(--glass-bg-strong);
  backdrop-filter: blur(var(--glass-blur));
  border-bottom: 1px solid var(--glass-border);
}
.ae-btn.active { border-color: var(--accent); color: var(--accent); }
.ae-btn:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
```

### Cards VCT (`.v-card`, `.v-panel`, `.v-stat`, `.v-banner`)

```css
.v-card, .v-panel, .v-stat, .v-banner {
  background: var(--glass-bg);
  border: 1px solid var(--glass-border);
  box-shadow: inset 0 1px 0 var(--glass-highlight), var(--shadow-card);
  border-radius: 10-12px;
}
.v-panel, .v-banner { backdrop-filter: blur(var(--glass-blur)); } /* solo grandes */
.v-card:hover { border-color: rgba(232,255,71,.35); }              /* sin glow */
```

### Tablas / scoreboards (`.v-table`, `.v-board`, `tablas/`) — **opaco, sin glass**

```css
.v-table th { background: #16161a; }
.v-table td { background: rgba(10, 10, 12, .88); }
.v-board { background: linear-gradient(180deg, #17171d, #101014); }
```

### Fondo de nodos (global, todas las páginas)

- `comun/bg-nodes.js` (canvas 2D vanilla, 0 dependencias): 14–26 nodos según área,
  24 fps, DPR 1, líneas de 1px alpha ≤ .10, velocidad ~0.09 px/frame.
- `pointer-events:none`, `z-index:0`, primer hijo del body (detrás de todo).
- Pausa con `visibilitychange`; frame estático con `prefers-reduced-motion`.
- Carga única: `header.js` inyecta el script (`data-ae-bg`) y el propio script se
  auto-guarda con `window.__AE_BG_NODES__`.

---

## Motion

- **Standard**: stagger de entrada 300–450ms, `cubic-bezier(.22,1,.36,1)`.
- **Implementación:** CSS-only (`animation-delay` por hijo o `nth-child`) — **no añadir GSAP**
  (el sitio es vanilla, sin build; mantener 0 dependencias).
- Respetar `prefers-reduced-motion: reduce` → render final inmediato (nodos y stagger off).
- ❌ No usar `back.out` ni overshoot en tablas de datos.
- ❌ No animar `width/height/top/left` (solo `transform`/`opacity`).
- Hover: 150–250ms; press: 80–150ms.

---

## Alcance (scope) de esta identidad

| Zona | Glass | Nodos | Notas |
|------|-------|-------|-------|
| `header/` | ✅ | ✅ | Compartido por todas las páginas (incluida `tablas/`) |
| `partidos/`, `equipos/`, `jugadores/`, `eventos/` | ✅ cards/paneles/banners | ✅ | Tablas y scoreboards **opacos** |
| `aletheia/` (EN VIVO) | ✅ superficies grandes | ✅ | Hero sin canvas propio |
| `aletheia_preparar/`, `inicio/`, `visualizar/` | ⏳ pendiente | ✅ | Migración de glass/tipografía en fase posterior |
| `tablas/` | ❌ | ✅ | Explorador raw — no se toca salvo el nodo global |
| `estilos/` (cuando exista) | — | — | Se **adaptan** sus componentes a `ae-*`/`v-*` |

---

## Anti-Patterns (Do NOT Use)

- ❌ **Glow/resplandor**: `box-shadow` de color, `drop-shadow` de color, text-shadow de color, auras.
- ❌ Violeta/cian/magenta como acento de marca (la paleta es negra + amarilla).
- ❌ `backdrop-filter` en elementos repetidos (grids de cards) — costo GPU en scroll.
- ❌ Glass detrás de tablas densas o texto pequeño (mata la legibilidad).
- ❌ WebGL/Three.js o librerías de partículas (peso y GPU innecesarios).
- ❌ Emojis como iconos — usar SVG (Heroicons/Lucide).
- ❌ Colores aleatorios de equipo: el color sale de `--c` (color medio del logo).
- ❌ Degradar la tasa de acierto: **la precisión manda** (DOCUMENTACION.md §1).

---

## Pre-Delivery Checklist

- [ ] Sin glow de ningún tipo (grep de `glow`, `drop-shadow`, `text-shadow` de color).
- [ ] `backdrop-filter` solo en superficies grandes/únicas.
- [ ] Contraste de texto ≥ 4.5:1 sobre glass (medir el fondo compuesto).
- [ ] `prefers-reduced-motion` respetado (nodos y stagger se apagan).
- [ ] Nodos pausados con la pestaña oculta; CPU/frame < 2 ms.
- [ ] Sin scroll horizontal en 375 / 768 / 1024 / 1440.
- [ ] Focus visible (outline amarillo sólido) en nav, cards, tabs y botones.
- [ ] Tablas y scoreboards siguen 100% legibles.
- [ ] Clases `ae-*`/`v-*` y lógica JS intactas; enlaces sin `/index.html`.
- [ ] Fuentes: un request, `display=swap`, pesos recortados.
