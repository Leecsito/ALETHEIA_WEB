# PLAN ACTUALIZACIÓN PREDICT — Alineación web ↔ API (2026-10-02)

**Origen:** auditoría de la web (`D:\etc\xampp\htdocs\ALETHEIA`) frente al contrato vigente de `ALETHEIA_PREDICT` (HEAD `6304c32`) y a la documentación de ambos repos (`DOCUMENTACION.md` web §4.4/§5, `DOCUMENTACION.md` Predict §6).
**Base:** web HEAD `f93ea3d` (2026-09-30) · Predict HEAD `6304c32` (2026-10-02) · `modelo_version` vigente **`028805b25d64`**.
**Estado:** plan listo; los hallazgos están verificados sobre el árbol actual y **sin implementar**.
**Naturaleza:** H1/H2/H4/H5/H6 son web (`aletheia.py`, JS de las dos páginas, docs, secretos). **H3 toca también `api/app.py` de Predict** (aditivo, no toca `core/`). Ningún ítem cambia la P(mapa).
**Regla:** un commit por hallazgo/grupo coherente; `DOCUMENTACION.md` de la web al día en cada uno; ningún commit mezcla lógica de UI con secretos. Si algo mueve la P(mapa) o el `modelo_version`, se revierte.

---

## 0. Criterios de aceptación (Definition of Done)

1. **H1 (clave S1):** PREPARAR, RE-PRECALCULAR, ASOCIAR, BORRAR y EXPORTAR DATASET funcionan contra un servicio con `ALETHEIA_API_KEY` fijada; la clave viaja **solo server-side** (proxy) y nunca aparece en HTML/JS; sin clave el servicio sigue en **401** (fail-closed).
2. **H2 (`/api/serie`):** docstring del proxy, nota de la UI y documentación describen el contrato vigente (cache-aware: reutiliza la caché y **calcula/persiste** lo que falte) y los campos que ya devuelve (`fuente`, `modelo_version`, `n_sim`, `confianza_serie`, `resultados_serie`, `caminos_serie`, `prob_intervalo`).
3. **H3 (`prob_intervalo`, A6):** `GET /api/prediccion` y `GET /api/predicciones` devuelven `prob_intervalo` no nulo cuando el rating del equipo trae `rd`; la web lo muestra por mapa y en el informe; la P puntual y el `modelo_version` no cambian (solo `api/`).
4. **H4 (`desactualizado`):** la web avisa cuando `GET /api/modelo_version` trae `desactualizado:true` ("servicio pendiente de reiniciar").
5. **H5 (docs):** `DOCUMENTACION.md` (web), docstrings y comentarios sin referencias stale (admin "directo a ngrok", `/api/serie` "sin Monte Carlo", ausencia de `X-API-Key`, `precalculo_jobs`, `prob_intervalo`, `desactualizado`).
6. **H6 (secreto):** `render.yaml` sin `TURSO_AUTH_TOKEN`; token **rotado** en Turso y configurado en el panel de Render (`sync: false`); `ALETHEIA_API_KEY` como secreto en web y Predict.
7. Lecturas (simulaciones/predicciones/serie/comparación/scorecard/dataset) sin regresión; mismas P que hoy para un enfrentamiento ya preparado.

---

## 1. H1 — S1: la clave API rompe los endpoints admin

**Evidencia (web HEAD `f93ea3d`):**
- `aletheia.py:82-108` (`_request_service`): solo añade `ngrok-skip-browser-warning`; **nunca** `X-API-Key`.
- `aletheia_preparar/script.js:311-320` (`predictFetch('/api/precalcular')`) y `:399` (`predictFetch('/api/precalcular/estado')`): van **directo a ngrok** sin clave.
- `aletheia/script.js:635-645` (RE-PRECALCULAR) y `:687` (poll): igual, directo sin clave.
- `aletheia/script.js:492` (ASOCIAR), `:515` (BORRAR) y `:1788` (`/dataset?guardar=1`): van por el proxy, que **tampoco** añade la clave.
- Contrato Predict (`api/app.py:209-250`): `POST /api/precalcular`, `/api/asociar`, `/api/borrar`, `forzar=true` y `dataset?guardar=1` exigen `X-API-Key`; **fail-closed** si `ALETHEIA_API_KEY` falta. Verificado en vivo: `POST /api/precalcular` sin clave → **401**.
- Doc web stale: `DOCUMENTACION.md:384-391` y `:485-490` declaran el flujo "frontend → `PREDICT_DIRECTO` para lo largo" (sin clave).

**Cambio propuesto:**
1. `aletheia.py`:
   - `_request_service`: construir headers `{'ngrok-skip-browser-warning': '1'}` y añadir `X-API-Key: <ALETHEIA_API_KEY>` si la variable está configurada (server-side; se envía a todas las llamadas proxied — los endpoints públicos la ignoran).
   - Nueva ruta `GET /api/aletheia/precalcular/estado` → `_passthrough_get('/api/precalcular/estado')`.
   - Docstring del módulo: añadir `estado` y la nota de clave server-side.
2. `aletheia/script.js` y `aletheia_preparar/script.js`:
   - `precalcular` y su polling pasan a `proxyFetch(...)` (el POST async responde **202 al instante**, no choca con el timeout del proxy).
   - Eliminar `PREDICT_DIRECTO`, `NGROK_HEADER` y `predictFetch` (quedan sin uso; el navegador ya no llama a ngrok).
3. Entornos: `ALETHEIA_API_KEY` en `.env` local de la web y en el panel de Render (secreto). La **misma** clave en `.env` de Predict (su servicio la exige).

**Aceptación:** con clave fijada en ambos lados: `POST {proxy}/precalcular` → 202; `GET {proxy}/precalcular/estado` → 200 con progreso; PREPARAR completa 26 filas; RE-PRECALCULAR/ASOCIAR/BORRAR/DATASET verdes. Llamada directa sin clave → 401.

---

## 2. H2 — `/api/serie` documentado con el contrato viejo

**Evidencia:**
- `aletheia.py:247`: docstring "Probabilidad de serie desde caché, sin Monte Carlo".
- `DOCUMENTACION.md` (web) `:373-376`: "desde caché (sin Monte Carlo; no escribe en la DB)".
- `aletheia/script.js:1314`: nota "Serie desde caché (sin Monte Carlo)".
- Contrato vigente (L12b, Predict §6.5 `:1997-2005`): `/api/serie` es **cache-aware**; si falta una fila la **calcula y la persiste** (mapa y serie), y la segunda llamada idéntica sale de cache; devuelve por mapa `fuente`, `confianza`, `economia`, `analisis_mapa`, `prob_intervalo`, y a nivel serie `confianza_serie`, `modelo_version`, `n_sim`, `resultados_serie`, `caminos_serie`.

**Cambio propuesto:** actualizar el docstring del proxy, la nota de la UI (mencionar que puede calcular/persistir lo que falte, mostrando `fuente`) y la doc web. La P no cambia: solo el texto.

---

## 3. H3 — `prob_intervalo` (A6) no se consume en la web

**Evidencia:**
- `grep prob_intervalo` en `aletheia/script.js` y `aletheia_preparar/script.js`: **0 coincidencias**.
- Predict: el intervalo existe (`core/calibracion.py::intervalo_probabilidad`, A6) y la ruta **calculada** de `/api/predecir`/`/api/serie` lo expone; las filas de cache devuelven `null` (`api/app.py:370`, diseño H1 del plan de correcciones) y `GET /api/prediccion(es)` **no lo incluye**.
- Consecuencia: EN VIVO (que lee de caché) nunca puede mostrar el intervalo.

**Cambio propuesto (dos partes):**
1. **Predict (aditivo, solo `api/`; no toca `core/` ni `modelo_version`):**
   - Importar `intervalo_probabilidad` y resolver `rd` por equipo desde `motor.ratings` (helper `_prob_intervalo(equipo_a, equipo_b, p)`; sin `rd` → `None`).
   - Usarlo en la rama **cache** de `_resolver_mapas` (`api/app.py:363-373`, hoy fija `None`) y en los serializadores de `/api/prediccion` (`:909-916`) y `/api/predicciones` (`:942-949`).
   - Actualizar `DOCUMENTACION.md` de Predict §4.4/§6.1 (la lectura de caché también lo expone) y el changelog §9.
2. **Web:** pintar `prob_intervalo` en la tarjeta de mapa/banda (`paintLiveDetail`, `aletheia/script.js:995-1050`) y en el informe `.md` (`payload`, `:1428-1461`) como "IC95% lo–hi"; ocultarlo si es `null` (compatibilidad con filas viejas).

**Aceptación:** `/api/predicciones?match_id=<preparado>` devuelve `prob_intervalo:[lo,hi]`; la web lo muestra; `prob_victoria_a` idéntica; `modelo_version` intacto; tests de Predict verdes.

---

## 4. H4 — `modelo_version.desactualizado` se ignora

**Evidencia:** `aletheia/script.js:533-545` (`loadModeloVersion`) guarda `modelo_version` pero no `desactualizado`; doc web `:311-312` solo documenta `modelo_version`/`fecha`.
Contrato Predict (§6.1 `:1819`): `/api/modelo_version` devuelve `en_disco` y `desactualizado` (avisa de reentreno/cambio de `core/` pendiente de reiniciar).

**Cambio propuesto:** guardar `desactualizado` y mostrar un aviso en la lista ("⚠ servicio desactualizado: reinicia ALETHEIA_PREDICT; las filas nuevas quedarán viejas al reiniciar"). No bloquea la UI.

---

## 5. H5 — Documentación y comentarios stale

**Evidencia (además de H1/H2):**
- `DOCUMENTACION.md` (web) §4.4 `:219-382` y §5 `:480-498`: sin `X-API-Key`, sin `prob_intervalo`, sin `desactualizado`, sin `precalculo_jobs` (L16), sin proxy de `estado`; flujo "directo a ngrok" para admin.
- `aletheia.py:8-23` (lista de endpoints sin `estado`), `:36-38` (nota de corridas directas).
- Comentarios JS: `aletheia/script.js:3-5,623-624`; `aletheia_preparar/script.js:3-5`.

**Cambio propuesto:** actualizar todo lo anterior al contrato vigente; dejar `DOCUMENTACION.md` como fuente de verdad y anotar el changelog del repo web.

---

## 6. H6 — Secreto Turso en `render.yaml` (crítico)

**Evidencia:** `render.yaml` (web, **trackeado** según `git ls-files`) incluye `TURSO_AUTH_TOKEN` en texto plano; el JWT declara `a:"rw"` (lectura-escritura). Está en el historial de git.
**Cambio propuesto:**
1. **Rotar** el token en Turso (el viejo queda expuesto en el historial; la rotación es obligatoria, no basta borrarlo).
2. Eliminar el valor de `render.yaml` y declarar `sync: false` (configurarlo en el panel de Render), igual que hace el `render.yaml` de Predict.
3. Añadir `ALETHEIA_API_KEY` como variable secreta (H1).
**Aceptación:** `git grep TURSO_AUTH_TOKEN` sin coincidencias en el working tree; el servicio web funciona con el token rotado desde el panel.

---

## 7. Tests y verificación

| Ítem | Verificación |
|---|---|
| H1 | Script con `test_client` de Flask + monkeypatch de `requests.request` que captura headers (clave presente en admin; nunca en la respuesta HTML/JS). Integración: Predict local con `ALETHEIA_API_KEY=k`; directo sin clave → 401; vía proxy → 200/202. |
| H2 | Revisión de textos + smoke `POST {proxy}/serie` (200; `fuente` por mapa). |
| H3 | Predict: `py -m pytest -q` + aserto nuevo de `prob_intervalo` en `/api/predicciones`; comparar `prob_victoria_a` pre/post (idéntica). Web: smoke visual. |
| H4 | `GET /api/modelo_version` con `desactualizado:true` simulado (o reinicio pendiente) → aviso visible. |
| H5 | `Select-String` de control: sin "sin Monte Carlo", sin "PREDICT_DIRECTO" en flujos admin, sin claves en JS. |
| H6 | `git grep TURSO_AUTH_TOKEN` limpio; token rotado (evidencia en Turso/Render). |
| Transversal | PREPARAR → EN VIVO → ARMAR SERIE → COMPARACIÓN → ASOCIAR/BORRAR/DATASET end-to-end; P idénticas para un enfrentamiento ya preparado. |

---

## 8. Commits propuestos (orden)

1. `fix(proxy): enviar X-API-Key server-side y exponer /precalcular/estado` — H1 (proxy)
2. `fix(frontend): precalcular por el proxy en PREPARAR y EN VIVO` — H1 (JS)
3. `feat(api): exponer prob_intervalo en la lectura de caché (A6)` — H3 (Predict)
4. `feat(frontend): mostrar prob_intervalo y aviso de modelo desactualizado` — H3+H4
5. `docs(web): alinear documentación con el contrato vigente` — H2+H5
6. `chore(seguridad): rotar y sacar TURSO_AUTH_TOKEN de render.yaml` — H6

Cada commit con smoke verificado; la auditoría (`AUDITORIA_ACTUALIZACION_PREDICT.md`) se ejecuta al final sobre el HEAD que incluya los seis.

---

## 9. Documentación

- `DOCUMENTACION.md` (web) §4.4/§5: endpoints (incl. `estado`), clave server-side, `/api/serie` cache-aware, `prob_intervalo`, `desactualizado`, `precalculo_jobs`.
- `DOCUMENTACION.md` (Predict) §4.4/§6.1/§9: `prob_intervalo` también en lectura de caché (H3).
- Docstrings de `aletheia.py` y comentarios de los dos `script.js`.

---

## 10. Riesgos y rollback

| Riesgo | Mitigación |
|---|---|
| Clave distinta en web y Predict → 401 | Verificar match de la clave en el preflight de la auditoría; documentar dónde va cada una. |
| Mover precalcular al proxy choca con su timeout (120 s) | El POST async responde 202 al instante; el trabajo sigue en el hilo del servicio. Polling por proxy es rápido. |
| Filas viejas sin `prob_intervalo` | La UI oculta el dato si viene `null`; no rompe. |
| Rotar el token deja la web sin DB | Coordinar rotación + panel de Render antes del deploy; rollback: volver al token viejo (no recomendado) o re-desplegar. |
| `predictFetch` eliminado rompe algo no detectado | `grep` de control en los dos JS; el único uso era admin (H1). |

---

## 11. Fuera de alcance

- **Autenticación de la web**: las páginas admin (`/aletheia_preparar/`, RE-PRECALCULAR) son públicas; con la clave en el proxy, cualquiera que llegue a la web puede lanzar precálculos. El semáforo de Predict (429, un precálculo a la vez) y `MAX_SIM` lo acotan. Se documenta como observación; abrir un plan aparte si se quiere login.
- **`/api/equipos` sin métricas**: el adaptador del proxy deja `maps_played`/`avg_rating` en 0; exponer ratings de equipo es una feature nueva (no es una actualización de contrato).
- **Limpieza del historial git de la web** (mensajes y secreto ya commiteado): la rotación cubre el riesgo; reescribir historia es otra decisión.
- **Roadmap abierto de Predict** (§10 de su `DOCUMENTACION.md`): L3/A4/D.5b siguen su propio protocolo.
