# PLAN ABC PREDICT — Alineación de la web con los Planes A/B/C del backend (2026-10-03)

**Origen:** cierre de los Planes A (economía/pistols), B (capa de escenarios por mapa/lado) y C (veto completo + serie por mapa) en `ALETHEIA_PREDICT` (HEAD `8178166`, `modelo_version` **`5232151ff388`**) y auditoría de la web `D:\etc\xampp\htdocs\ALETHEIA` (HEAD `e35dc13`; working tree con `inicio/inicio.py` modificado: ETL de rounds que etiqueta R1/R13 como `pistol`, coherente con la DB).
**Naturaleza:** todo es web (`aletheia/`, `partidos/`, `aletheia.py`, docs). **No toca `core/` de Predict, ni la P(mapa), ni `modelo_version`.**
**Regla:** un commit por hallazgo/grupo; `DOCUMENTACION.md` de la web al día en cada uno; ningún commit mezcla UI con datos. Si algo mueve la P(mapa) o el `modelo_version`, se revierte.

---

## 0. Criterios de aceptación (Definition of Done)

1. **H1 (crítico):** ARMAR SERIE no puede llamar a `/api/serie` con 2 o 4 mapas; exige 1/3/5 y lo dice en la UI. Con 3 mapas (Bo3) y 5 (Bo5) el flujo queda igual que hoy.
2. **H2:** la P de serie en segundo plano de la lista nunca dispara una llamada con lista inválida (fallback de mapas jugados); si no hay pool completo, la píldora muestra “sin pool” sin martillar al servicio.
3. **H3:** cada mapa muestra `escenario_mapa` (P de escenario, n de cada equipo, delta e IC) cuando el backend lo devuelve; si viene `null` (flag apagado o modo DB) se oculta sin romper. `prob_victoria_a` no cambia.
4. **H4:** el bloque ECONOMÍA/RONDAS indica la semántica nueva (“sin pistols R1/R13; n = rondas simuladas”) y muestra `n` por categoría/celda; el bloque PISTOL se mantiene.
5. **H5:** el detalle de partido (`partidos/`) deja de mostrar `economy_summary` como “real” sin más: calcula la tasa por categoría desde `rounds` (excluyendo R1/R13) o etiqueta explícitamente la fuente y su limitación.
6. **H6:** el modo DB de `aletheia.py` es coherente con el contrato nuevo: `/api/serie` solo responde desde `predicciones_serie` con 1/3/5 mapas; en modo DB `escenario_mapa`/`prob_intervalo` quedan `null` y la UI los oculta (documentado).
7. **H7:** copy y `DOCUMENTACION.md` (web) sin referencias stale; changelog de la web actualizado.
8. **Invariante:** para un enfrentamiento preparado, las P por mapa y la P de serie con el veto completo son **idénticas** antes/después de estos cambios (solo cambia presentación y validación).

---

## 1. H1 (crítico) — ARMAR SERIE puede enviar 2/4 mapas y la API ahora responde 400

**Evidencia (web HEAD `e35dc13`):**
- `aletheia/script.js:1209-1240` (`updateSerie`): construye `mapas: matchMaps.map(...)` sin comprobar la cantidad; el POST va tal cual.
- `aletheia/script.js:131-137` (`inferFormat`) y `:1204-1207` (`updateSerieState`): con 2 mapas muestran `Bo3 · 2/3` (no bloquean).
- Los slots (`:1098-1165`, `syncSerieBuilder`) permiten huecos `qi-empty`; el usuario puede pulsar ARMAR SERIE con 2.
- Contrato nuevo (Predict, C0, `api/app.py::_veto_completo`): `/api/predecir` y `/api/serie` exigen **1/3/5** mapas; 2/4 → **400** (`'mapas' debe traer el veto completo…`).
- Efecto actual: `serieNote` muestra `Error: 'mapas' debe traer el veto completo (1 para Bo1, 3 para Bo3 o 5 para Bo5); recibí 2.`

**Cambio propuesto (`aletheia/script.js`):**
1. En `updateSerie()`, antes del POST: `const n = matchMaps.length; if (![1,3,5].includes(n)) { aviso claro: "Faltan N mapas: arma el veto completo (Bo3=3, Bo5=5)"; return; }`.
2. `updateSerieState()`/`marcarSeriePendiente()`: mostrar `faltan N` y deshabilitar/vaciar el banner mientras el veto esté incompleto.
3. `inferFormat(n)`: no inferir Bo3 con 2; devolver `—`/`incompleto` (el formato real lo fija `data-fmt` 1/3/5, `index.html:64-66`).
4. Opcional (mejor UX): al seleccionar un enfrentamiento, prellenar `matchMaps` con `current.serie_mapas` (pool del veto, 3/5) si existe, en orden picks+decider.

**Aceptación:** 2 mapas → no hay POST y se ve “faltan 1 mapa”; 3 mapas → 200 y banner normal; 5 mapas → 200.

---

## 2. H2 (alto) — Serie en segundo plano de la lista: fallback a mapas jugados

**Evidencia:**
- `aletheia/script.js:350-355` (`mapasSerieDe`): usa `info.serie_mapas` y, si viene vacío, los **mapas jugados** (`info.mapas`), que en un 2-0 son 2.
- `partidos/partidos.py:228-245,323`: `serie_mapas` = `match_veto` con `action in ('pick','decider')` (pool completo); si el partido no tiene veto, cae a los mapas jugados.
- `aletheia/script.js:359-410` (`cargarSeriesLista`): ante `!d.ok` marca `error: true` y no reintenta; el fallo es **silencioso** (píldora vacía).
- DB: `match_veto` tiene 1.097 `decider` para 1.102 partidos → 5 sin decider; y puede haber matches sin veto.

**Cambio propuesto (`aletheia/script.js`):**
1. En `mapasSerieDe`, devolver `[]` si la lista resultante no tiene 1/3/5 entradas (no llamar).
2. En `cargarSeriesLista`, cuando `!mapas.length`, marcar `simSerie[match_id] = { sin_pool: true }` y pintar “sin pool” en `seriePillHtml`.
3. Sin cambios en `partidos.py` (la consulta del pool ya es correcta); documentar que un match sin veto no tiene P de serie.

**Aceptación:** ningún POST a `/serie` con 2/4 en la carga de la lista; los 1.097 matches con decider siguen pintando píldora.

---

## 3. H3 (alto, feature) — `escenario_mapa` no se consume en la web

**Evidencia:**
- `grep escenario` en `aletheia/script.js`, `aletheia_preparar/script.js` y `aletheia.py`: **0 coincidencias**.
- Backend B1: cada mapa de `/api/predecir`, `/api/serie`, `/api/prediccion` y `/api/predicciones` incluye `escenario_mapa = {p_mapa, delta_logit, n_a, n_b, peso, lambda, k, p_lo, p_hi}` (o `null` con `ALETHEIA_ESCENARIO_MAPA=0` o motor no cargado). `prob_victoria_a` **no** cambia.
- La UI pinta motor (`paintLiveDetail :1016-1019`, `renderLiveMapPicker :798-804`, `syncSerieBuilder :1133-1146`) y análisis histórico (`:1013-1022`), pero no la capa.

**Cambio propuesto (`aletheia/script.js`):**
1. `paintLiveDetail`: tarjeta “ESCENARIO (MAPA/LADO)” con `p_mapa`, `n_a`/`n_b`, `delta_logit` e intervalo `p_lo–p_hi`; tooltip “capa de escenarios anclada al motor; no es el predictor”. Ocultar si `null`.
2. `renderLiveMapPicker`: pill `ESC p_mapa%` (solo si viene) junto a `MOTOR`/`hist`.
3. `syncSerieBuilder` (filas del armador): mostrar `esc %` por mapa.
4. Informe (`descargarAnalisis`, `:1449-1477`): añadir `escenario_mapa` al objeto por mapa y a `_mercadosTexto`/JSON.
5. Nota de `paintLiveDetail` (`:1059`): sustituir “la predicción del motor es la misma…” por “el MOTOR es plano; el ESCENARIO añade la lectura por mapa/lado; el histórico es descriptivo”.

**Aceptación:** con el servicio cargado, TL–PRX Ascent muestra escenario ≠ motor (p. ej. 0,46 vs 0,48) con n y IC; con `ALETHEIA_ESCENARIO_MAPA=0` o en modo DB, la tarjeta se oculta y nada más cambia.

---

## 4. H4 (medio) — Economía: semántica nueva y `n` no visibles

**Evidencia:**
- `aletheia/script.js:936-1008` (`renderEconomia`): pinta 4 categorías (`CAT_ECO :926-931`), PISTOL y la matriz 4×4; usa `p_gana_ronda`/`p_gana_a` pero **no** muestra `n` ni la semántica.
- Backend A1: `eco` ya **no** incluye pistols R1/R13 (barra ~12% en vez de ~40%), `p_gana_*` pondera por `Σw`, y `economia` publica `semantica` y `pistol_semantica`.
- La fila de caché nueva (`economia_json` re-precalculada, `5232151ff388`) ya trae los valores corregidos.

**Cambio propuesto (`aletheia/script.js`):**
1. `renderEconomia`: leer `eco.semantica`/`eco.pistol_semantica` y mostrarlas en `eco-note` (fallback al texto actual si no vienen).
2. Mostrar `n` por categoría (p. ej. `ECO 12% · n=…`) y `n` en las celdas destacadas del cruce.
3. Mantener la matriz y el bloque PISTOL tal cual.

**Aceptación:** la nota dice “sin pistols R1/R13”; `n` visible; matriz 4×4 intacta.

---

## 5. H5 (alto) — El detalle de partido muestra `economy_summary` como “real” (benchmark roto)

**Evidencia:**
- `partidos/partidos.py:374-379` consulta `economy_summary` y `partidos/script.js:172-200` (`econBlock`) pinta `ECO 0/3 · 0%`, `SEMI-ECO`, `SEMI-BUY`, `FULL BUY` (la columna derecha de la captura).
- Predict (Plan A): `economy_summary.eco_played` excluye R1 pero **incluye R13**, y `eco_won` excluye R1 **y** R13 → tasa 5,17% vs **29,69%** real sobre su propio denominador; el scorecard ya calcula el eco real desde `rounds`. Documentado como benchmark **no fiable**.
- Efecto: el panel real seguirá diciendo “0% en eco” mientras el motor dice ~12%; fuentes distintas sin aviso.

**Cambio propuesto:**
1. `partidos/partidos.py`: en el detalle, calcular la tasa real por `(map_id, team_id, categoría)` desde `rounds` (JOIN `maps`; excluir `round_num IN (1,13)`; el ganador se resuelve con `team_top_id`/`team_bot_id` que ya están en `rounds`), y devolverla como `economy` con la misma forma (`*_won`, `*_played`).
2. `partidos/script.js`: si el dato viene de `rounds`, mantener el rótulo; si se conserva `economy_summary` como fallback, añadir nota “fuente economy_summary (no fiable en eco)” y no compararlo con el motor.

**Aceptación:** para 753455, ECO de TL en Ascent coincide con `rounds` (excluyendo R1/R13) y el panel es coherente con el scorecard; valores `semi_buy`/`full_buy` no cambian (ya coincidían).

---

## 6. H6 (medio) — El modo DB de la web no sigue el contrato nuevo

**Evidencia:**
- `aletheia/aletheia.py:244-250` (`_formato_de_serie`): 2→Bo3, 4→Bo5 (el API ahora 400).
- `:392-468` (`_serie_desde_db`): puede devolver una fila de `predicciones_serie` para 2 mapas si existe (filas viejas) → incoherente con el servicio.
- `:335-345` (`_derivar_fila`): `prob_intervalo=None` y **sin** `escenario_mapa`; `_version_vigente_db` (`:348-362`) toma la versión más reciente de la caché.

**Cambio propuesto (`aletheia/aletheia.py`):**
1. `_serie_desde_db`: rechazar (devolver `None` → proxy) si `len(mapas)` no está en `{1,3,5}`.
2. Documentar que en modo DB `escenario_mapa`/`prob_intervalo` van `null` y la UI los oculta (v1). Opcional v2: replicar el escenario en DB con una consulta a `rounds` (mismo criterio que `core/escenario_mapa.py`), como ya se replica `analisis_mapa`.
3. `_formato_de_serie` solo se usa en modo DB; alinear con `{1,3,5}`.

**Aceptación:** con el servicio caído, un POST de 2 mapas no devuelve una serie inventada; con 3 y fila exacta en `predicciones_serie`, responde como hoy.

---

## 7. H7 (bajo) — Copy y documentación

**Evidencia:**
- `aletheia/script.js:1059` (“La predicción del motor… es la misma en todos los mapas y lados”) y `:1140` (tooltip “igual en todos los mapas”) ya no cuentan toda la historia (escenario).
- `DOCUMENTACION.md` (web) §4.4/§5 sin: veto completo, `escenario_mapa`, semántica de economía sin pistols, eco real desde `rounds`, `modelo_version 5232151ff388`.

**Cambio propuesto:** actualizar copy + `DOCUMENTACION.md` (web) con el contrato vigente y changelog. Incluir el cambio ya presente en `inicio/inicio.py` (ETL `pistol`) en la doc.

**Aceptación:** `Select-String` de control sin textos stale; doc web cita los campos nuevos.

---

## 8. H8 (operativa, sin código) — Estado de la caché

- Predict `modelo_version 5232151ff388`; caché **520/520** filas re-precalculadas (2026-10-03). La web, vía `_version_vigente_db`, leerá esa versión y `vigente:true`.
- Si se reentrena o cambia `core/`, repetir: reiniciar servicio + `POST /api/precalcular` (los 20 enfrentamientos). La web ya avisa con `desactualizado`/`vigente` (H4 del plan anterior).

---

## 9. Tests y verificación

| Ítem | Verificación |
|---|---|
| H1 | Smoke: ARMAR SERIE con 2 mapas → mensaje y **sin** POST (DevTools/red); con 3 → 200 (banner con `resultados_serie`). |
| H2 | Cargar la lista con un match sin veto (o simular `serie_mapas=[]`) → no hay llamadas `/serie` inválidas; píldora “sin pool” en su caso. |
| H3 | Con servicio: `GET /api/aletheia/predicciones?match_id=753455` trae `escenario_mapa` no nulo por mapa; la tarjeta lo pinta; `ALETHEIA_ESCENARIO_MAPA=0` → `null` → oculta. |
| H4 | La nota de economía cita “sin pistols R1/R13”; `n` visible; matriz 4×4 y pistol intactos. |
| H5 | `/api/partido/753455`: ECO por equipo calculado desde `rounds`; contraste con `economy_summary` documentado. |
| H6 | Con servicio caído: `/api/aletheia/serie` con 2 mapas → error claro (no serie de DB); con 3 y fila exacta → 200. |
| H7 | Greps de control sin “es la misma en todos los mapas” sin matiz; doc web actualizada. |
| Invariante | P(mapa) y P(serie) con veto completo idénticas a las de la caché `5232151ff388` (comparar antes/después por mapa). |

---

## 10. Commits propuestos (orden)

1. `fix(serie): exigir veto completo (1/3/5) en ARMAR SERIE` — H1 (JS).
2. `fix(lista): no pedir serie con mapas jugados si falta el pool` — H2 (JS).
3. `feat(ui): mostrar escenario_mapa (capa B) en vivo, armador e informe` — H3 (JS).
4. `feat(ui): semántica y n en economía del mapa` — H4 (JS).
5. `fix(partidos): economía real desde rounds (excluye R1/R13)` — H5 (Python+JS).
6. `fix(proxy): modo DB coherente con veto completo y campos derivados` — H6 (`aletheia.py`).
7. `docs(web): contrato ABC (veto, escenario, economía) y copy` — H7.
8. `chore(etl): commit del cambio pistol en inicio/inicio.py` — el working tree ya lo tiene; incluirlo aquí o en su propio commit.

Cada commit con smoke verificado; la auditoría (`AUDITORIA_ABC_PREDICT.md`) se ejecuta al final sobre el HEAD que los incluya.

---

## 11. Riesgos y rollback

| Riesgo | Mitigación |
|---|---|
| Bloquear ARMAR SERIE con 2 mapas rompe el flujo actual del usuario | Mensaje explícito “faltan N mapas”; precarga opcional desde `serie_mapas`; rollback: volver a permitir 2 (contra API 400, no recomendado). |
| `escenario_mapa` `null` en modo DB confunde | Ocultar el bloque; nota en doc; v2 replicarlo desde `rounds` si se pide. |
| Cambiar el real de economía en `partidos` mueve números históricos | Es una corrección de fuente (rounds vs economy_summary); documentarla y no tocar la DB. |
| `_serie_desde_db` estricto deja partidos sin píldora | Solo afecta a listas 2/4 sin pool; esos casos ya no deben mostrar serie. |
| Tocar `partidos.py` con `git` sucio (`inicio/inicio.py` sin commit) | Commitear primero el cambio pendiente del ETL (H8/commit 8) o dejarlo fuera de este ciclo si molesta. |

---

## 12. Fuera de alcance

- **Autenticación/público admin** y rotación del token de Turso: ya contemplados en el plan anterior (`PLAN_ACTUALIZACION_PREDICT.md`, H6); no se reabren aquí.
- **Replicar `escenario_mapa` en modo DB** (v2 opcional; requiere la lógica de lados sobre `rounds`).
- **Cambios en `economy_summary` (scraper/ETL)**: el dashboard de ingesta sigue igual; solo cambia su lectura como benchmark.
- **P(mapa)/`modelo_version`**: intocables; cualquier PR que los mueva se rechaza.
