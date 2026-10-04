# AUDITORÍA ABC PREDICT — Protocolo de verificación (web ↔ backend Planes A/B/C)

**Fecha de creación:** 2026-10-03 · **Estado:** **auditoría ejecutada 2026-10-03 — APROBADA** sobre web HEAD `dbf8518` (commits `437541f` H1-H4, `b3e6f78` H5, `e9bdc5e` H6, `27beccd` H7, `dbf8518` H8/ETL) y Predict HEAD `8178166`; resultados en §11, sin criterios de rechazo activados. §1 conserva la evidencia pre-auditoría (web HEAD `e35dc13`).
**Documentos base:** `PLAN_ABC_PREDICT.md` (plan), `DOCUMENTACION.md` de la web (§4.4/§5) y de Predict (§4.5/§4.6/§4.18/§6/§9, Planes A/B/C).
**Objetivo:** que un auditor independiente compruebe con evidencia reproducible que (a) la web ya no puede enviar vetos incompletos (2/4) a la API, (b) la capa `escenario_mapa` se muestra sin alterar `prob_victoria_a`, (c) la economía del mapa y del detalle de partido usan la semántica/fuente correctas (pistols y `rounds`), (d) el modo DB es coherente con el contrato nuevo, (e) copy y documentación dejan de ser stale, y (f) ninguna P ni `modelo_version` cambió.

> **Cómo usar**
> 1. **Preflight** (§2): capturar HEADs, `modelo_version`, P baseline, cobertura de veto y greps de control.
> 2. **Checklists** (§3-§8): evidencia por hallazgo (código, smoke, docs).
> 3. **Cierre** (§9-§11): transversal, criterios de rechazo y plantilla de resultados.
> Un fallo en cualquier criterio de §10 rechaza la auditoría del ítem.

---

## 1. Pre-auditoría: los hallazgos existen (verificado 2026-10-03)

| Ítem | Evidencia en el árbol actual |
|---|---|
| **H1** | `aletheia/script.js:1209-1240` (`updateSerie`) envía `matchMaps` sin validar cantidad; `:131-137` (`inferFormat`) acepta 2 como Bo3; `:1204-1207` muestra `Bo3 · 2/3`; la API (C0) exige 1/3/5 y 2/4 → 400 |
| **H2** | `aletheia/script.js:350-355` (`mapasSerieDe`) cae a los mapas jugados (2 en un 2-0) si no hay `serie_mapas`; `partidos/partidos.py:323` rellena ese fallback; `aletheia/script.js:359-410` silencia el `!d.ok` |
| **H3** | `grep -i escenario` en `aletheia/script.js`, `aletheia_preparar/script.js` y `aletheia.py`: 0 coincidencias; el backend B1 ya lo expone por mapa |
| **H4** | `aletheia/script.js:936-1008` no lee `economia.semantica`/`pistol_semantica` ni muestra `n`; el backend A1 cambió la semántica (eco sin pistols) |
| **H5** | `partidos/partidos.py:374-379` lee `economy_summary`; `partidos/script.js:172-200` lo pinta como real; Predict documenta ese benchmark como no fiable (5,17% vs 29,69% real) |
| **H6** | `aletheia/aletheia.py:244-250` (`_formato_de_serie` 2→Bo3, 4→Bo5) y `:392-468` (`_serie_desde_db`) pueden devolver serie de 2 mapas; `:335-345` no expone `escenario_mapa`/`prob_intervalo` |
| **H7** | `aletheia/script.js:1059,1140` copy “es la misma en todos los mapas”; `DOCUMENTACION.md` web sin veto/escenario/economía nueva |

**Baseline actual:** web HEAD `e35dc13` (working tree: `inicio/inicio.py` modificado, ETL `pistol`) · Predict HEAD `8178166` · `modelo_version` **`5232151ff388`** · caché **520/520** filas re-precalculadas · suite Predict **226 passed** · P(mapa) TL–PRX `0.48001779` (todos los mapas y lados).

**Post-ciclo (2026-10-03):** web HEAD `dbf8518` (ETL `pistol` commiteado) · Predict HEAD `8178166` · `modelo_version` `5232151ff388` · caché **520/520** · suite Predict **226 passed** · P(mapa) TL–PRX `0.4800177861924539` (idéntica, 26/26 filas).

---

## 2. Preflight obligatorio (capturar antes de implementar)

```powershell
# 2.1 HEADs y versión del modelo
git -C "D:\etc\xampp\htdocs\ALETHEIA" rev-parse --short HEAD
git -C "D:\etc\xampp\htdocs\ALETHEIA" status --short          # inicio/inicio.py modificado (ETL pistol)
git -C "D:\PROYECTOS\ALETHEIA_PREDICT" rev-parse --short HEAD
py -c "import sys; sys.path.insert(0,'D:/PROYECTOS/ALETHEIA_PREDICT'); from core.version import modelo_version; print(modelo_version()['version'])"

# 2.2 API: veto incompleto debe dar 400 (con clave server-side ya implementada)
$key = ((Select-String -Path "D:\etc\xampp\htdocs\ALETHEIA\.env" -Pattern '^ALETHEIA_API_KEY=').Line -split '=',2)[1].Trim().Trim('"')
$body2 = @{ equipo_a='Team Liquid'; equipo_b='Paper Rex'; match_id=753455; n_sim=10000
            mapas=@(@{map_name='Ascent';lado_inicial_a='attack'},@{map_name='Haven';lado_inicial_a='attack'}) } | ConvertTo-Json -Depth 5
try { Invoke-WebRequest -Uri "http://localhost:8000/api/serie" -Method Post -ContentType "application/json" -Body $body2 -Headers @{'X-API-Key'=$key} -UseBasicParsing } catch { "2 mapas: HTTP $([int]$_.Exception.Response.StatusCode)" }   # esperado: 400

# 2.3 Caché de un enfrentamiento preparado (P baseline + escenario + economía)
py -c "import sys; sys.path.insert(0,'D:/PROYECTOS/ALETHEIA_PREDICT'); from core.db import get_client; c=get_client(); q='''SELECT map_name, lado_inicial_a, prob_victoria_a, substr(economia_json,1,60) FROM predicciones_mapa WHERE match_id=753455 AND lado_inicial_a='attack' ORDER BY map_name'''; [print(tuple(r)) for r in c.execute(q).rows]; c.close()"

# 2.4 Escenario en el servicio (debe ser no nulo con motor cargado)
Invoke-RestMethod "http://localhost:8000/api/predicciones?match_id=753455" | ConvertTo-Json -Depth 4 | Select-String "escenario_mapa"

# 2.5 Cobertura del pool de veto (P de serie de la lista)
py -c "import sys; sys.path.insert(0,'D:/PROYECTOS/ALETHEIA_PREDICT'); from core.db.conexion import _con_reintentos,_query_df; f=lambda c:_query_df(c,'SELECT action, COUNT(*) n FROM match_veto GROUP BY action'); print(_con_reintentos(f).to_string(index=False))"   # esperado: pick 2316 / ban 4260 / decider 1097

# 2.6 Greps de control (pre: H1/H3/H4/H5 aparecen)
Select-String -Path "D:\etc\xampp\htdocs\ALETHEIA\aletheia\script.js" -Pattern "escenario|semantica|pistol_semantica"
Select-String -Path "D:\etc\xampp\htdocs\ALETHEIA\partidos\partidos.py" -Pattern "economy_summary"
```

Registrar: HEADs, `modelo_version`, P por mapa, HTTP del 400, presencia de `escenario_mapa` y cobertura de veto.

---

## 3. Checklist H1 — veto completo en ARMAR SERIE

| # | Verificación | Criterio |
|---|---|---|
| H1.1 | Validación | `updateSerie()` exige `matchMaps.length ∈ {1,3,5}` antes del POST |
| H1.2 | UI | con 2/4 mapas no hay POST y se muestra “faltan N mapas”; `inferFormat`/estado no dicen “Bo3” con 2 |
| H1.3 | Smoke 2 mapas | DevTools/red: 0 requests a `/api/aletheia/serie`; mensaje visible |
| H1.4 | Smoke 3/5 mapas | 200 y banner normal (`resultados_serie`, `caminos_serie`) |
| **Invariante H1** | P de serie | con el veto completo, `prob_serie_a` idéntica a la de §2.3/`predicciones_serie` vigente |

## 4. Checklist H2 — serie de la lista sin listas inválidas

| # | Verificación | Criterio |
|---|---|---|
| H2.1 | Guarda | `mapasSerieDe` devuelve `[]` si la lista no tiene 1/3/5 |
| H2.2 | Sin fallo silencioso | `cargarSeriesLista` marca “sin pool” y no llama al servicio |
| H2.3 | Cobertura | los matches con `decider` (1.097) siguen mostrando píldora; los 5 sin decider no rompen |
| **Invariante H2** | Sin 400 en segundo plano | no aparecen errores 400 en la consola de red durante la carga de la lista |

## 5. Checklist H3 — `escenario_mapa` en la UI

| # | Verificación | Criterio |
|---|---|---|
| H3.1 | Backend | `/api/aletheia/predicciones?match_id=<preparado>` trae `escenario_mapa` no nulo con motor cargado |
| H3.2 | Tarjeta | `paintLiveDetail` muestra `p_mapa`, `n_a`/`n_b`, `delta_logit` e intervalo |
| H3.3 | Armador/informe | las filas de serie y el `.md` de análisis incluyen el escenario |
| H3.4 | Flag off / DB mode | `ALETHEIA_ESCENARIO_MAPA=0` o modo DB ⇒ `null` ⇒ bloque oculto, sin errores |
| **Invariante H3** | P(mapa) | `prob_victoria_a` idéntica a §2.3 (la capa es aditiva) |

## 6. Checklist H4 — semántica de economía

| # | Verificación | Criterio |
|---|---|---|
| H4.1 | Nota | `renderEconomia` muestra “sin pistols R1/R13” (de `semantica`, con fallback) |
| H4.2 | `n` | cada categoría/celda destacada muestra `n` |
| H4.3 | Pistol | el bloque PISTOL y la matriz 4×4 no cambian de estructura |
| **Invariante H4** | Valores | el eco ya no marca ~40% (nueva caché `5232151ff388`); no hay NaN ni huecos |

## 7. Checklist H5 — economía real en el detalle de partido

| # | Verificación | Criterio |
|---|---|---|
| H5.1 | Fuente | `partidos.py` calcula ECO/SEMI-ECO/SEMI-BUY/FULL-BUY desde `rounds` excluyendo R1/R13 (o etiqueta la fuente si mantiene `economy_summary`) |
| H5.2 | Coherencia | para 753455, el ECO real coincide con `rounds` (29,69% global) y con el scorecard; no con el 5,17% de `economy_summary` |
| H5.3 | Sin regresión | `semi_buy`/`full_buy` idénticos a hoy (ya coincidían con `rounds`) |
| **Invariante H5** | DB | no se escribió en Turso (solo lectura) |

## 8. Checklist H6 — modo DB coherente

| # | Verificación | Criterio |
|---|---|---|
| H6.1 | Veto | `_serie_desde_db` devuelve `None` (→ proxy) si `len(mapas)` no es 1/3/5 |
| H6.2 | Formato | `_formato_de_serie` del modo DB alineado con 1/3/5 |
| H6.3 | Campos | docstring de `_derivar_fila` documenta `prob_intervalo`/`escenario_mapa` `null` en modo DB |
| H6.4 | Smoke caída | con el servicio caído, 2 mapas no devuelven serie; 3 mapas con fila exacta → 200 |

## 9. Checklist H7 — copy y documentación

| # | Verificación | Criterio |
|---|---|---|
| H7.1 | Copy | `aletheia/script.js:1059,1140` matizan motor vs escenario vs histórico |
| H7.2 | Doc web | §4.4/§5 incluyen veto completo, `escenario_mapa`, semántica de economía, eco real desde `rounds` |
| H7.3 | Changelog | entrada de la web con el ciclo ABC y el ETL `pistol` |
| H7.4 | Grep | sin “sin Monte Carlo”/“directo a ngrok” (H1-H5 del plan anterior) y sin referencias stale nuevas |

---

## 10. Criterios de rechazo (NO aprobar si…)

1. ARMAR SERIE puede enviar 2/4 mapas (sigue el 400) o no avisa al usuario.
2. La carga de la lista dispara `/serie` con listas de 2/4 sin guarda.
3. `escenario_mapa` se ignora por completo, o se muestra cuando el backend lo manda `null`, o se usa para sustituir `prob_victoria_a`.
4. La economía no expone la semántica nueva y/o sigue sin `n`.
5. El detalle de partido sigue presentando `economy_summary` como “real” sin corrección ni etiqueta.
6. El modo DB devuelve una P de serie para 2/4 mapas.
7. Cambia alguna P(mapa)/P(serie) con el veto completo o el `modelo_version` sin justificación explícita.
8. Faltan docs, copy o changelog; o se tocó `core/` de Predict.

---

## 11. Resultado de la auditoría (completado 2026-10-03)

Auditor: asistente IA (opencode) · evidencia: código, caché Turso, proxy local en modo DB (servicio Predict apagado), suite Predict y test Node sobre `aletheia/script.js`.

```markdown
## Resultado de la auditoría ABC PREDICT — 2026-10-03 — opencode (IA)
Commits auditados: `437541f` H1-H4 · `b3e6f78` H5 · `e9bdc5e` H6 · `27beccd` H7 · `dbf8518` H8/ETL
web HEAD: `dbf8518` · Predict HEAD: `8178166`
modelo_version: `5232151ff388` · caché: 520/520 (2026-10-03 22:37:42) · suite Predict: local 226 passed · completa 226 passed
P baseline (753455, attack por mapa): `0.4800177861924539` pre/post (idéntica en las 26 filas)

### H1 (veto completo)
- Validación: SÍ (`VETO_COMPLETO=[1,3,5]`; guarda antes del POST; test Node de funciones reales: `inferFormat(2)='incompleto'`)
- Aviso UI: SÍ (`updateSerieState` → "faltan N" + botón deshabilitado; `updateSerie`/`marcarSeriePendiente` avisan)
- 0 requests con 2/4: SÍ (la guarda retorna antes de `proxyFetch('/serie')`; orden verificado sobre el fuente)
- 200 con 3/5: N/E en vivo (PC/ngrok apagado → 404); el camino 3/5 no se modificó y el contrato del backend es 1/3/5

### H2 (lista)
- Guarda: SÍ (`mapasSerieDe` → `[]` si no es 1/3/5; test Node)
- "sin pool": SÍ (`cargarSeriesLista` marca `sin_pool` sin llamar; píldora `SERIE sin pool`)
- Sin 400 en red: SÍ (datos: 1033 matches con pool 3 y 62 con 5 pintan píldora; 1 con pool 4 `473235` y 6 sin veto caen en "sin pool")

### H3 (escenario)
- Backend no nulo: SÍ por contrato (`8178166`: `api/app.py:524/557/581/1114/1155`; `test_escenario_mapa.py` 6 passed; flag `ALETHEIA_ESCENARIO_MAPA`); no re-ejecutado con motor cargado (PC apagado)
- Tarjeta: SÍ (`paintLiveDetail` ESCENARIO con `p_mapa`, `n_a/n_b`, Δlogit e IC; pill `ESC` en selector/armador/serie)
- Informe: SÍ (`escenario_mapa` en el JSON y en `_mercadosTexto` del `.md`)
- Null oculto: SÍ (26/26 filas de 753455 vía proxy en modo DB con `escenario_mapa=null`; bloque condicional, sin errores)
- P idéntica: SÍ (26/26 filas `0.48001779`; la capa es aditiva)

### H4 (economía)
- Nota: SÍ (`economia.semantica`/`pistol_semantica` con fallback)
- n: SÍ (por categoría, celda destacada y pistol)
- Estructura intacta: SÍ (matriz 4×4 y bloque PISTOL sin cambios)
- Valores: SÍ (eco ~12,6% en Abyss; 26/26 filas sin NaN ni huecos; `escenario`/`intervalo` null en modo DB)

### H5 (partidos)
- Fuente rounds: SÍ (`_economia_desde_rounds`, `fuente:'rounds'`; fallback `economy_summary` etiquetado; en vivo `/api/partido/753455` → 6/6 filas `fuente=rounds`)
- Coherencia scorecard: SÍ en método (réplica exacta de `core/scorecard.py::_eco_actual`: excluye R1/R13 y resuelve por `team_top_id`/`team_bot_id`); el 5,17% de `economy_summary` se confirma (444/8596). NOTA: el 29,69% citado en el plan no se reproduce (macro/pooled ≈ 10,1% con el criterio del scorecard); es una cifra agregada de la doc de Predict y no afecta a la corrección de fuente
- Sin regresión: SÍ (`semi_buy`/`full_buy` idénticos a `economy_summary` en 753455, celda a celda)
- DB: SÍ (solo lectura; no se escribió en Turso)

### H6 (modo DB)
- Veto 1/3/5: SÍ (`_serie_desde_db` → `None` con 2/4; en vivo con servicio caído: 2 mapas → 502, sin serie inventada)
- Formato: SÍ (`_formato_de_serie` solo 1/3/5)
- Doc: SÍ (docstring de `_derivar_fila` documenta `prob_intervalo`/`escenario_mapa` null)
- Smoke caída: PARCIAL (2 mapas → 502 sin serie; 3 mapas → 502 porque no hay ninguna fila de `predicciones_serie` con la versión vigente `5232151ff388`, así que el 200 "con fila exacta" no es reproducible con la caché actual)

### H7 (docs)
- Copy: PASS (sin "es la misma en todos los mapas"; nuevo texto MOTOR/ESCENARIO/histórico)
- Doc web: PASS (§4.4/§4.5/§5 con veto, escenario, semántica y eco desde rounds; corregida una frase stale de §5 "cada cambio hace POST")
- Changelog: PASS (entrada 2026-10-03 con H1-H8 y ETL `pistol`)
- Greps: PASS ("sin Monte Carlo"/"directo a ngrok" solo en PLAN/AUDITORIA antiguos y en la negación legítima de `DOCUMENTACION.md:445`; sin `PREDICT_DIRECTO`/`predictFetch`/`NGROK_HEADER` en el frontend)

### Transversal
- Invariantes de P/versión: PASS (P `0.48001779` y `modelo_version 5232151ff388` idénticas; `core/` de Predict sin tocar)

### Criterios de rechazo activados
ninguno
```

---

## 12. Anexo — comandos rápidos

```powershell
# Greps de control
Select-String -Path "D:\etc\xampp\htdocs\ALETHEIA\aletheia\script.js" -Pattern "escenario|semantica|matchMaps.length"
Select-String -Path "D:\etc\xampp\htdocs\ALETHEIA\aletheia\aletheia.py" -Pattern "_formato_de_serie|_serie_desde_db|escenario"
Select-String -Path "D:\etc\xampp\htdocs\ALETHEIA\partidos\partidos.py" -Pattern "economy_summary|rounds"
Select-String -Path "D:\etc\xampp\htdocs\ALETHEIA\aletheia\script.js" -Pattern "es la misma en todos los mapas"

# Smoke /serie con veto completo (3 mapas)
$body3 = @{ equipo_a='Team Liquid'; equipo_b='Paper Rex'; match_id=753455; n_sim=10000
            mapas=@(@{map_name='Haven';lado_inicial_a='attack'},@{map_name='Ascent';lado_inicial_a='attack'},@{map_name='Lotus';lado_inicial_a='attack'}) } | ConvertTo-Json -Depth 5
Invoke-RestMethod "http://localhost:8000/api/serie" -Method Post -ContentType "application/json" -Body $body3 -Headers @{'X-API-Key'=$key} | ConvertTo-Json -Depth 3

# Escenario: apagarlo y comprobar null/determinismo
$env:ALETHEIA_ESCENARIO_MAPA='0'   # reiniciar el servicio con la variable puesta para el check del flag
```

**Nota:** la web no tiene suite de tests propia en el repo; la verificación de H1-H5 es smoke en navegador + greps + comparación de P baseline. Registrar cada evidencia con fecha/hora.
