# INTEGRACIÓN WEB — Motor por capas de ALETHEIA_PREDICT (plan F1–F4)

- **Fecha**: 2026-10-07
- **Origen**: repo `ALETHEIA_PREDICT` (servicio externo), plan por capas L1/L2/L3 (F1–F4).
- **Destino**: web `ALETHEIA` (proxy `aletheia/aletheia.py` + frontend).
- **Objetivo**: qué cambió en la API del motor, qué endpoints nuevos exponer,
  cómo enrutarlos y qué verificar antes de desplegar. Todo es **aditivo**: los
  contratos actuales de `predecir`/`serie`/`prediccion(es)` siguen valiendo.

> Documentos relacionados del servicio: `DOCUMENTACION.md` §4.27/§4.28/§10.7
> (motor), §6.1 (endpoints) y §10.6 (operativa de cierre).

---

## 0. Resumen para la web

1. **Campos nuevos aditivos** en respuestas existentes: `mapas[].capa` y
   `capa_serie` (F1). Nada cambia de valor: la P(serie) sigue siendo el motor
   Glicko y `prob_victoria_a` sigue siendo la ESC por mapa/lado.
2. **Dos endpoints nuevos** que la web aún no expone:
   - `POST /api/recomendar` — L2: tier-list del pool + mejor mapa/ban y lado.
   - `GET /api/perfil` — L3: perfil económico PIT por equipo o cruce.
3. **Columna nueva** `predicciones_serie.recomendacion_json` (la escribe el
   servicio al calcular; la web no necesita leerla para funcionar).
4. **Enrutado**: `recomendar` y `perfil` **cargan el motor completo** (1–2 min
   en frío), así que van a `BASE_URL` (PC/ngrok) con `prefer='ngrok'`; no son
   cache-first ni tienen lectura directa a Turso.
5. **Despliegue pendiente**: los cambios F1–F4 del repo del motor están
   **sin commitear**; Render (`READ_URL`) sigue con el código viejo. Hasta
   commit+push+redeploy, `capa`/`capa_serie` no aparecen por Render y
   `recomendar`/`perfil` responden **404** en Render (sí funcionan en
   `BASE_URL`).

---

## 1. Estado de despliegue (leer antes de tocar la web)

| Servicio | URL | Código F1–F4 | Efecto |
|---|---|---|---|
| `BASE_URL` (`ALETHEIA_PREDICT_URL`) | `https://snugly-encore-sweep.ngrok-free.dev` (PC) | Sí (si la sesión local está levantada con el working tree nuevo) | Tiene `capa`, `recomendar`, `perfil` |
| `READ_URL` (`ALETHEIA_PREDICT_READ_URL`) | `https://aletheia-predict.onrender.com` | **No hasta redeploy** | Sirve caché TS; sin campos nuevos ni endpoints nuevos |

Checklist del despliegue del motor:

1. Commit + push del repo `ALETHEIA_PREDICT` (F1–F4; `modelo_mapa.json` y
   `modelo_rondas.json` **no** cambian).
2. Reiniciar el servicio (waitress/ngrok local y Render) y re-precalcular los
   enfrentamientos activos (`POST /api/precalcular`).
3. Verificar `GET /api/modelo_version`: el hash vigente con el working tree
   actual es **`302ba8d96fcf`** (el `7692bc0ace02` que cita la doc del motor es
   stale). Si al desplegar el hash **difiere** del de las filas en
   `predicciones_mapa`, esas filas se consideran viejas y hay que
   re-precalcular (la web lo detecta con `vigente`/`desactualizado`).
4. Migración `predicciones_serie.recomendacion_json`: se aplica sola con el
   primer `_asegurar_esquema()` del servicio (ya ejecutada en Turso con el
   precalculo de 754731). Si tu módulo de lectura directa lista columnas
   explícitas de `predicciones_serie`, añade `recomendacion_json`.

---

## 2. Cambios aditivos en endpoints existentes (F1)

### 2.1 `POST /api/predecir` y `POST /api/serie`

Por mapa, nueva clave:

| Campo | Tipo | Semántica |
|---|---|---|
| `mapas[].capa` | `"esc"` \| `"motor"` | De dónde sale `prob_victoria_a`: ESC (default) o P plana del motor (con `ALETHEIA_ESCENARIO_MAPA=0`) |
| `esc_peso` | number \| null | Ya existía: peso 0–1 del ESC (`n_min/(n_min+10)`) |

En la raíz:

| Campo | Tipo | Semántica |
|---|---|---|
| `capa_serie` | `"motor_glicko"` | La capa que decide el ganador global (siempre el motor; ESC no la mueve) |

No cambia nada más: el veto completo 1/3/5, `prob_motor_a/b` (plana, raíz),
`prob_serie_a/b`, `modelo_version`, `fuente` (`cache|calculado`), etc.

### 2.2 `GET /api/prediccion` y `GET /api/predicciones`

- Cada fila añade `capa` (`"esc"|"motor"`), **calculado por el servicio** (no
  se persiste; en modo DB directo no existe: usa `esc_peso != null` como
  indicador de ESC).
- El resto igual (`escenario_mapa` y `prob_intervalo` solo en el servicio;
  en modo DB quedan `null`).

### 2.3 `predicciones_serie.recomendacion_json` (F4)

- Columna TEXT nueva; el servicio la escribe **best-effort** cuando
  `/api/predecir` o `/api/serie` **calculan** filas y hay `match_id`
  (un cache-hit no escribe).
- Contiene la recomendación L2 serializada (mismo shape que
  `POST /api/recomendar`). **No hay endpoint de lectura**: para mostrar la
  recomendación, la web debe llamar a `POST /api/recomendar`.
- Para VIT–NSR `754731` **no** hay fila de serie calculada (todo salió de
  caché), así que no hay `recomendacion_json` para ese partido.

---

## 3. Endpoints nuevos

### 3.1 `POST /api/recomendar` — L2 (mapa/ban/lado)

- **Auth**: ninguna (público). **Coste**: carga el motor completo la primera
  vez → enrutar a `BASE_URL` (`prefer='ngrok'`), timeout ≥ 120 s.
- **Body**:

```json
{
  "equipo_a": "Team Vitality",
  "equipo_b": "Nongshim RedForce",
  "mapas": ["Lotus", "Split"]
}
```

  - `mapas` es **opcional**; acepta strings o `{"map_name": "..."}`. Sin
    `mapas`, usa el pool vigente (`map_pool`) con historial. Máx. 5.
  - `equipo_a == equipo_b` → 400.

- **Respuesta** (resumida; ejemplo real VIT–NSR):

```json
{
  "ok": true,
  "equipo_a": "Team Vitality",
  "equipo_b": "Nongshim RedForce",
  "capa": "esc",
  "p_motor_a": 0.5178,
  "p_motor_b": 0.4822,
  "lambda": 0.4, "k": 10.0, "k_peso": 10.0,
  "min_publicar": 10, "min_fuerte": 30,
  "mapas": [
    {
      "orden": 1,
      "map_name": "Lotus",
      "lado_recomendado_a": "attack",
      "prob_victoria_a": 0.5577, "prob_victoria_b": 0.4423,
      "p_lo": 0.5058, "p_hi": 0.6125,
      "delta_logit": 0.1608,
      "n_a": 201, "n_b": 175, "n_min": 175,
      "peso": 0.9459,
      "evidencia": "fuerte",
      "por_lado": {
        "attack":  {"lado": "attack",  "prob_victoria_a": 0.5577, "prob_victoria_b": 0.4423,
                     "p_lo": 0.5058, "p_hi": 0.6125, "delta_logit": 0.1608,
                     "n_a": 201, "n_b": 175, "n_min": 175, "peso": 0.9459, "evidencia": "fuerte"},
        "defense": {"lado": "defense", "prob_victoria_a": 0.5133, "prob_victoria_b": 0.4867,
                     "p_lo": 0.4581, "p_hi": 0.5677, "delta_logit": -0.0178,
                     "n_a": 189, "n_b": 171, "n_min": 171, "peso": 0.9448, "evidencia": "fuerte"}
      }
    }
  ],
  "mejor_mapa_a": {"map_name": "Lotus", "lado_recomendado_a": "attack", "evidencia": "fuerte", "...": "sin por_lado"},
  "mejor_ban_a":  {"map_name": "Split", "...": "peor mapa para A"},
  "mejor_mapa_b": {"map_name": "Split", "...": "espejo de mejor_ban_a"},
  "recomendacion_fuerte_a": {"map_name": "Lotus", "...": "o null si ningún n_min>=30"},
  "nota": "L2 (mapa/lado): ..."
}
```

  - `evidencia`: `sin_datos` (`n_min=0`) · `limitada` (`<10`) · `publicable`
    (`>=10`) · `fuerte` (`>=30`).
  - Semántica: `prob_victoria_a` es ESC de A en **el lado recomendado**
    (el mejor de los dos); `por_lado` trae ambos. **No mueve la P(serie)**
    (la raíz `p_motor_a` es solo la referencia Glicko).

- **Errores**: 400 (`Se requieren equipo_a y equipo_b.`, `Mapa desconocido`,
  equipos iguales), 500 (`No se pudo calcular la recomendación.`/motor).

### 3.2 `GET /api/perfil` — L3 (micro-económico PIT)

- **Auth**: ninguna. **Coste**: carga el motor completo → `BASE_URL`
  (`prefer='ngrok'`).
- **Query**:
  - `equipo_a` (o `equipo`) — obligatorio.
  - `equipo_b` — opcional. Con él devuelve el **cruce** (perfiles A y B +
    16 cruces); sin él, el **perfil del equipo**.
- **Respuesta — perfil de equipo**:

```json
{
  "ok": true,
  "equipo": "Team Vitality",
  "n_rondas": 2663,
  "categorias": {
    "full_buy": {"w": 845, "n": 1478, "tasa": 0.5717, "tasa_eb": 0.5716, "p_lo": 0.5463, "p_hi": 0.5967},
    "semi_buy": {"w": 283, "n": 508, "tasa": 0.5571, "tasa_eb": 0.5562, "p_lo": 0.5136, "p_hi": 0.5997},
    "semi_eco": {"w": 28, "n": 123, "tasa": 0.2276, "tasa_eb": 0.2262, "p_lo": 0.1625, "p_hi": 0.3093},
    "eco":     {"w": 9, "n": 105, "tasa": 0.0857, "tasa_eb": 0.0871, "p_lo": 0.0457, "p_hi": 0.1549}
  },
  "pistols": {
    "mitad_1": {"w": 72, "n": 125, "tasa": 0.576, "tasa_eb": 0.5704, "p_lo": 0.4884, "p_hi": 0.6591},
    "mitad_2": {"w": 60, "n": 125, "tasa": 0.48,  "tasa_eb": 0.4815, "p_lo": 0.3943, "p_hi": 0.5669}
  },
  "post_pistol": {
    "mitad_1": {
      "tras_ganar":  {"w": 64, "n": 72, "tasa": 0.8889, "...": "R2 tras ganar R1"},
      "tras_perder": {"w": 7,  "n": 53, "tasa": 0.1321, "...": "R2 tras perder R1"}
    },
    "mitad_2": { "tras_ganar": {}, "tras_perder": {} }
  },
  "cascada": {
    "mitad_1": {
      "tras_ganar":  {"n": 72, "rondas_esperadas": 7.542, "p_mitad": 0.6806, "p_lo": 0.5661, "p_hi": 0.7767},
      "tras_perder": {"n": 53, "rondas_esperadas": 5.151, "p_mitad": 0.2453, "p_lo": 0.1493, "p_hi": 0.3757}
    },
    "mitad_2": {}
  },
  "priors_liga": {"eco": 0.1012, "semi_eco": 0.2083, "semi_buy": 0.5118, "full_buy": 0.5572}
}
```

- **Respuesta — cruce** (`equipo_a`+`equipo_b`): añade
  `"capa": "l3_micro"`, `"n_rondas"` (total), `"perfil_a"`, `"perfil_b"` y:

```json
{
  "cruces": {
    "full_buy_vs_full_buy": {
      "w": 16774, "n": 33548, "tasa": 0.5, "tasa_eb": 0.5,
      "p_lo": 0.4946, "p_hi": 0.5054,
      "p_estimada_a": 0.4975, "delta_logit": -0.01
    },
    "...": "16 celdas {cat_a}_vs_{cat_b}"
  },
  "nota": "L3 (micro-eventos): ..."
}
```

- **Lectura honesta (no es bug)**: pistols R1/R13 y `full vs full` son ~50/50
  a propósito; la UI no debe presentarlos como señal fuerte. Los cruces son
  tasa de liga anclada a la ventaja por categoría del equipo.

---

## 4. Cómo exponerlos en el proxy de la web (`aletheia/aletheia.py`)

Añadir el blueprint igual que los demás, pero con `prefer='ngrok'` (cargan
motor; sin fallback a `READ_URL` porque en Render no está el código nuevo y el
cold start free tarda 1–2 min):

```python
@aletheia_bp.route('/api/aletheia/recomendar', methods=['POST'])
def aletheia_recomendar():
    """L2: tier-list/mejor mapa/ban/lado (capa ESC anclada al motor)."""
    return _passthrough_post('/api/recomendar', prefer='ngrok')

@aletheia_bp.route('/api/aletheia/perfil', methods=['GET'])
def aletheia_perfil():
    """L3: perfil económico PIT por equipo (`equipo_a`) o cruce (+`equipo_b`)."""
    return _passthrough_get('/api/perfil', prefer='ngrok')
```

- `_passthrough_post` ya reenvía el body y añade `X-API-Key` server-side si
  está configurada (los dos endpoints la ignoran, es inocuo).
- Para `/api/perfil` recuerda **reenviar el query string** (`equipo_a`,
  `equipo_b`); los `_passthrough_get` del módulo ya lo hacen.
- UI sugerida: sección "Plan de veto" (L2) con `evidencia`/`n`/intervalo y
  aviso "sin recomendación fuerte" cuando `recomendacion_fuerte_a: null`;
  panel "Micro / L3" con categorías y cascada del pistol.
- Etiquetas F1: si quieres mostrar la capa, usa `mapas[].capa` /
  `capa_serie`; en modo DB directo no vienen (no se persisten), así que
  muéstralas solo en respuestas del servicio.

---

## 5. Lo que NO cambia (recordatorio de contratos vigentes)

- Veto **completo** en `predecir`/`serie`: 1/3/5 mapas (1→bo1, 3→bo3,
  5→bo5); 2/4 → 400. El `formato` se deriva del nº de mapas.
- `prob_victoria_a` es **ESC por (mapa, lado)** (varía por mapa/lado; no
  promediar); `prob_motor_a/b` plana en la raíz; **la serie la decide el
  motor** (`prob_serie_a/b` + temperatura).
- `n_sim` se acota a `[100, MAX_SIM]` (`MAX_SIM=10000` en Render free,
  `50000` en el lanzador local/ngrok). No pedir 25K/50K contra Render.
- `X-API-Key` solo en admin/mutaciones (`precalcular`, `asociar`, `borrar`,
  `dataset?guardar=1`, `forzar:true`). Fail-closed si el servicio la tiene y
  falta.
- Errores uniformes: `{"ok": false, "error": "<mensaje>"}` con 400/401/404/429/500.
- `POST /api/precalcular` es **async** (202 + `job_id`) y su polling es
  `GET /api/precalcular/estado?job_id=...`; un 429 trae el job activo para
  adoptarlo.

---

## 6. QA rápido (ejemplos reales VIT–NSR 754731)

```bash
# Serie BO3 (cache-hit; 3 mapas = veto Bo3)
curl -X POST "$BASE/api/serie" -H "Content-Type: application/json" -d '{
  "equipo_a":"Team Vitality","equipo_b":"Nongshim RedForce",
  "mapas":[{"map_name":"Abyss","lado_inicial_a":"attack"},
           {"map_name":"Ascent","lado_inicial_a":"attack"},
           {"map_name":"Haven","lado_inicial_a":"attack"}]}'
# -> prob_motor_a 0.5178 · prob_serie_a 0.5214 · capa_serie "motor_glicko"
#    mapas[] fuente "cache", capa "esc", esc_peso 0.8876 / 0.9091 / 0.9419

# L2
curl -X POST "$BASE/api/recomendar" -H "Content-Type: application/json" \
  -d '{"equipo_a":"Team Vitality","equipo_b":"Nongshim RedForce"}'
# -> pick Lotus attack (0.5577, fuerte), ban Split; p_motor_a 0.5178

# L3
curl "$BASE/api/perfil?equipo_a=Team%20Vitality&equipo_b=Nongshim%20RedForce"
# -> capa "l3_micro", perfiles + 16 cruces
```

Notas de esta corrida: el precalculo de 754731 quedó con 14 filas
(`n_sim=50000`, `modelo_version=302ba8d96fcf`, 89.5 s, `updated_at=2026-10-07 20:05`).

---

## 7. Todo lo que necesita el desarrollo web, en una frase

**Para que la web "recepte bien el nuevo motor"**: (1) desplegar el repo del
motor a Render y re-precalcular; (2) añadir dos proxies nuevos
(`/api/aletheia/recomendar` y `/api/aletheia/perfil`) enrutados a ngrok;
(3) aceptar los campos aditivos `mapas[].capa` y `capa_serie`; (4) opcional:
leer `recomendacion_json` de `predicciones_serie` si quieres la recomendación
histórica (para el vivo, siempre `POST /api/recomendar`).
