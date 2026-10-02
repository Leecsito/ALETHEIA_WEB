# AUDITORÍA ACTUALIZACIÓN PREDICT — Protocolo de verificación

**Fecha de creación:** 2026-10-02 · **Estado:** protocolo listo; los ítems de `PLAN_ACTUALIZACION_PREDICT.md` están **sin implementar** (pre-auditoría §1 verificada sobre web HEAD `f93ea3d` y Predict HEAD `6304c32`).
**Documentos base:** `PLAN_ACTUALIZACION_PREDICT.md` (plan), `DOCUMENTACION.md` de la web (§4.4/§5) y de Predict (§4.4/§6/§9).
**Objetivo:** que un auditor independiente compruebe con evidencia reproducible que (a) los endpoints admin funcionan con la clave **server-side** sin filtrarla al navegador, (b) `/api/serie`/`prob_intervalo`/`desactualizado` se consumen según el contrato vigente, (c) la documentación y los comentarios dejan de ser stale, (d) el token de Turso ya no está en `render.yaml` y fue rotado, y (e) ninguna P(mapa) ni `modelo_version` cambió.

> **Cómo usar**
> 1. **Preflight** (§2): capturar HEADs, versión, P baseline, 401 directo y presencia del secreto.
> 2. **Checklists** (§3-§8): evidencia por hallazgo (código, smoke, docs).
> 3. **Cierre** (§9-§11): transversal, criterios de rechazo y plantilla de resultados.
> Un fallo en cualquier criterio de §10 rechaza la auditoría del ítem.

---

## 1. Pre-auditoría: los hallazgos existen (verificado 2026-10-02)

| Ítem | Evidencia en el árbol actual |
|---|---|
| **H1** | `aletheia.py:82-108` no añade `X-API-Key`; `aletheia_preparar/script.js:311,399` y `aletheia/script.js:635,687` llaman a `predictFetch` directo a ngrok sin clave; `aletheia/script.js:492,515,1788` usan el proxy para admin; Predict `api/app.py:209-250` exige clave (fail-closed) y la prueba en vivo da **401**; doc web `:384-391,485-490` documenta el flujo directo |
| **H2** | `aletheia.py:247` y doc web `:373-376` dicen `/api/serie` "sin Monte Carlo"; `aletheia/script.js:1314` repite la nota; el contrato vigente (Predict §6.5 `:1997-2005`) es cache-aware y persiste |
| **H3** | `grep prob_intervalo` en los dos `script.js`: 0 coincidencias; Predict solo lo expone en la ruta calculada (`api/app.py:395`) y lo fija a `None` en cache (`:370`) y en las lecturas crudas (`:909-955`) |
| **H4** | `aletheia/script.js:533-545` no lee `desactualizado`; doc web `:311-312` no lo documenta; Predict lo devuelve (`api/app.py:716-731`) |
| **H5** | Doc web §4.4/§5 (`:219-382,480-498`) y docstrings/comentarios (`aletheia.py:8-23,36-38`; JS `:3-5`) sin `X-API-Key`, `estado`, `precalculo_jobs`, `prob_intervalo`, `desactualizado` |
| **H6** | `render.yaml` (web, trackeado) con `TURSO_AUTH_TOKEN` en texto plano (JWT `a:"rw"`); `git ls-files` lo confirma |

**Baseline actual:** web HEAD `f93ea3d`; Predict HEAD `6304c32`; `modelo_version` **`028805b25d64`**; suite Predict local `173 passed, 4 deselected` y completa `177 passed`; walk-forward global **0.5517 / 0.6845** (n=2804).

---

## 2. Preflight obligatorio (capturar antes de implementar)

```powershell
# 2.1 HEADs y versión del modelo
git -C "D:\etc\xampp\htdocs\ALETHEIA" rev-parse --short HEAD
git -C "D:\PROYECTOS\ALETHEIA_PREDICT" rev-parse --short HEAD
py -c "import sys; sys.path.insert(0,'D:/PROYECTOS/ALETHEIA_PREDICT'); from core.version import modelo_version; print(modelo_version()['version'])"

# 2.2 Admin sin clave (debe dar 401) y con clave (debe dar 202/200)
$body = @{ equipo_a='T1'; equipo_b='JD Gaming'; match_id=753447; n_sim=100; mapas=@('Ascent'); sync=$true } | ConvertTo-Json
try { Invoke-WebRequest -Uri "http://localhost:8000/api/precalcular" -Method Post -ContentType "application/json" -Body $body -UseBasicParsing } catch { "sin clave: HTTP $([int]$_.Exception.Response.StatusCode)" }
# con clave (cuando H1 esté implementado):
# Invoke-WebRequest -Uri "http://localhost:8000/api/precalcular" -Method Post -ContentType "application/json" -Body $body -Headers @{ 'X-API-Key'=$env:ALETHEIA_API_KEY } -UseBasicParsing

# 2.3 Secreto en el repo web (pre: aparece; post: no)
git -C "D:\etc\xampp\htdocs\ALETHEIA" grep -n "TURSO_AUTH_TOKEN"

# 2.4 P baseline de un enfrentamiento preparado (guardar por mapa)
py -c "import sys; sys.path.insert(0,'D:/PROYECTOS/ALETHEIA_PREDICT'); from core.db import get_client; c=get_client(); q='''SELECT map_name, lado_inicial_a, prob_victoria_a FROM predicciones_mapa WHERE match_id=753447 AND lado_inicial_a='attack' ORDER BY map_name'''; [print(tuple(r)) for r in c.execute(q).rows]; c.close()"

# 2.5 Suite de Predict (invariante)
py -m pytest -q -m "not integration"   # en D:\PROYECTOS\ALETHEIA_PREDICT
```

Registrar: HEADs, `modelo_version`, P por mapa, salida del 401, presencia del token y resultado de la suite.

---

## 3. Checklist H1 — clave server-side y admin por proxy

| # | Verificación | Criterio |
|---|---|---|
| H1.1 | Header en el proxy | `aletheia.py::_request_service` añade `X-API-Key` cuando `ALETHEIA_API_KEY` está configurada |
| H1.2 | Sin fuga al navegador | `grep -i "api.key"` en `index.html`/`script.js` de las dos páginas: 0 coincidencias; la respuesta del proxy no contiene la clave |
| H1.3 | Ruta `estado` | `GET /api/aletheia/precalcular/estado?job_id=...` existe y reenvía el contrato (`{ok, job}`) |
| H1.4 | JS admin por proxy | PREPARAR, RE-PRECALCULAR, ASOCIAR, BORRAR y DATASET usan `proxyFetch`; `PREDICT_DIRECTO`/`predictFetch` eliminados |
| H1.5 | Fail-closed intacto | directo sin clave → 401; vía proxy con clave → 200/202 |
| **Invariante H1** | P(mapa) | las P de §2.4 no cambian tras el cambio |

---

## 4. Checklist H2 — contrato de `/api/serie`

| # | Verificación | Criterio |
|---|---|---|
| H2.1 | Docstring proxy | `aletheia.py` describe "cache-aware: reutiliza y calcula/persiste lo que falte" |
| H2.2 | Nota UI | la nota de ARMAR SERIE ya no dice "sin Monte Carlo"; menciona `fuente`/persistencia |
| H2.3 | Doc web | §4.4 documenta `fuente`, `modelo_version`, `n_sim`, `confianza_serie`, `resultados_serie`, `caminos_serie`, `prob_intervalo` |
| H2.4 | Smoke | `POST {proxy}/serie` responde 200 con `fuente` por mapa |

---

## 5. Checklist H3 — `prob_intervalo` en lectura de caché

| # | Verificación | Criterio |
|---|---|---|
| H3.1 | Predict lectura | `GET /api/predicciones?match_id=<preparado>` devuelve `prob_intervalo` no nulo (con `rd`) |
| H3.2 | Predict una fila | `GET /api/prediccion?...` ídem |
| H3.3 | Web | la tarjeta de mapa y el informe `.md` muestran el IC95%; si viene `null`, se oculta |
| H3.4 | Aditivo | `prob_victoria_a` idéntica a §2.4; `modelo_version` intacto (solo `api/`) |
| **Invariante H3** | Suite | `py -m pytest -q` verde en Predict (con aserto nuevo) |

---

## 6. Checklist H4 — aviso de modelo desactualizado

| # | Verificación | Criterio |
|---|---|---|
| H4.1 | Lectura | la web guarda `desactualizado` de `/api/modelo_version` |
| H4.2 | Aviso | con `desactualizado:true` aparece el aviso ("reinicia ALETHEIA_PREDICT") |
| H4.3 | Compatibilidad | con `false`/ausente no rompe ni avisa |

---

## 7. Checklist H5 — documentación y comentarios

| # | Verificación | Criterio |
|---|---|---|
| H5.1 | Doc web §4.4/§5 | incluye `X-API-Key` server-side, proxy de `estado`, `/api/serie` cache-aware, `prob_intervalo`, `desactualizado`, `precalculo_jobs` |
| H5.2 | Docstrings/comentarios | `aletheia.py` y los dos `script.js` sin "directo a ngrok" para admin ni "sin Monte Carlo" |
| H5.3 | Grep de control | `Select-String "sin Monte Carlo|PREDICT_DIRECTO"` solo en contexto legítimo (si queda alguno, justificado) |

---

## 8. Checklist H6 — secreto rotado y fuera del repo

| # | Verificación | Criterio |
|---|---|---|
| H6.1 | Working tree | `git grep TURSO_AUTH_TOKEN` sin coincidencias |
| H6.2 | `render.yaml` | declara `sync: false` para el token (o no lo declara) |
| H6.3 | Rotación | evidencia de token nuevo en Turso + panel de Render (manual) |
| H6.4 | Servicio web | tras rotar, las lecturas de DB de la web siguen verdes (`/api/partidos/resultados`, `/api/tablas`) |
| H6.5 | Historial | se reconoce que el token viejo está en el historial; la rotación es la mitigación |

---

## 9. Verificaciones transversales

| # | Verificación | Criterio |
|---|---|---|
| X.1 | Commits atómicos | un commit por hallazgo/grupo; ninguno mezcla UI con secretos |
| X.2 | Docs | `DOCUMENTACION.md` (web) y de Predict al día |
| X.3 | End-to-end | PREPARAR → EN VIVO → ARMAR SERIE → COMPARACIÓN → ASOCIAR/BORRAR/DATASET verdes |
| X.4 | P y versión | P de §2.4 idénticas; `modelo_version` `028805b25d64` (o el documentado si algún ítem tocó `core/`, no previsto) |
| X.5 | Clave | misma `ALETHEIA_API_KEY` en Predict y en la web (proxy); nunca en JS/HTML |
| X.6 | CORS | el navegador ya no llama a ngrok; `CORS_ORIGINS` de Predict no necesita el origen web para el flujo normal |

---

## 10. Criterios de rechazo (NO aprobar si…)

1. La clave viaja al navegador, queda en HTML/JS, o el proxy no la añade y los admin siguen en 401.
2. `PREDICT_DIRECTO`/`predictFetch` siguen usándose para admin o el flujo directo sigue documentado como vigente.
3. `/api/serie` sigue descrito como "sin Monte Carlo" en docstring, UI o doc web.
4. `prob_intervalo` no aparece en la lectura de caché o la web no lo muestra (salvo que H3 se declare N/A de forma justificada y documentada).
5. `desactualizado` sigue ignorado sin justificación.
6. `render.yaml` conserva el token o no hay evidencia de rotación.
7. Cambia alguna P(mapa) o el `modelo_version` sin justificación explícita.
8. Faltan docs, comentarios o la entrada de changelog correspondiente.

---

## 11. Plantilla de resultados (llenar al auditar)

```markdown
## Resultado de la auditoría ACTUALIZACIÓN PREDICT — <fecha> — <auditor>
Commits auditados: `<hash>` por hallazgo · web HEAD: `<hash>` · Predict HEAD: `<hash>`
modelo_version pre/post: `<hash>` / `<hash>` · suite Predict: local `<n>` · completa `<n>`
P baseline (753447): `<mapa:p>` pre/post

### H1 (clave/proxy)
- Header en proxy: SÍ/NO · Sin fuga: SÍ/NO · Ruta estado: SÍ/NO · JS por proxy: SÍ/NO · 401/202: SÍ/NO
### H2 (/api/serie)
- Docstring: PASS/FALLA · Nota UI: PASS/FALLA · Doc web: PASS/FALLA · Smoke: SÍ/NO
### H3 (prob_intervalo)
- `/api/predicciones`: no nulo SÍ/NO · `/api/prediccion`: SÍ/NO · Web: SÍ/NO · P idéntica: SÍ/NO
### H4 (desactualizado)
- Lectura: SÍ/NO · Aviso: SÍ/NO · Compatibilidad: SÍ/NO
### H5 (docs)
- Doc web: PASS/FALLA · Docstrings/comentarios: PASS/FALLA · Grep: PASS/FALLA
### H6 (secreto)
- Working tree limpio: SÍ/NO · sync:false: SÍ/NO · Rotación evidenciada: SÍ/NO · Web operativa: SÍ/NO
### Transversal
- X.1-X.6: PASS/FALLA

### Criterios de rechazo activados
<lista o "ninguno">
```

---

## 12. Anexo — comandos rápidos

```powershell
# Captura de headers del proxy (H1): test client + requests monkeypatcheado
py -c @"
import sys
from unittest import mock
sys.path.insert(0, r'D:\etc\xampp\htdocs\ALETHEIA')
from backend.app import app
import aletheia.aletheia as mod
capturado = {}
class R:
    status_code = 202
    def json(self): return {'ok': True, 'job_id': 'x', 'total': 26}
def fake(method, url, **kw):
    capturado['headers'] = kw.get('headers', {})
    return R()
with mock.patch.object(mod.requests, 'request', side_effect=fake):
    app.test_client().post('/api/aletheia/precalcular', json={'equipo_a': 'A', 'equipo_b': 'B'})
print(capturado['headers'])
"@

# Greps de control
Select-String -Path "D:\etc\xampp\htdocs\ALETHEIA\aletheia\script.js","D:\etc\xampp\htdocs\ALETHEIA\aletheia_preparar\script.js" -Pattern "PREDICT_DIRECTO|predictFetch|X-API-Key"
Select-String -Path "D:\etc\xampp\htdocs\ALETHEIA\aletheia.py","D:\etc\xampp\htdocs\ALETHEIA\aletheia\script.js" -Pattern "sin Monte Carlo"
git -C "D:\etc\xampp\htdocs\ALETHEIA" grep -n "TURSO_AUTH_TOKEN"

# Invariantes Predict
py -m pytest -q -m "not integration"   # en D:\PROYECTOS\ALETHEIA_PREDICT
py -c "import sys; sys.path.insert(0,'D:/PROYECTOS/ALETHEIA_PREDICT'); from core.db import get_client; c=get_client(); q='''SELECT map_name, prob_victoria_a FROM predicciones_mapa WHERE match_id=753447 AND lado_inicial_a='attack' ORDER BY map_name'''; [print(tuple(r)) for r in c.execute(q).rows]; c.close()"
```
