"""
ALETHEIA — Caché TTL en memoria para consultas SQL.

Los datos solo cambian al correr un ETL, así que cachear resultados de lectura
unos minutos es seguro y acelera muchísimo las visitas repetidas. La caché es
por proceso (en gunicorn hay 1 worker, ver render.yaml) y se limpia sola al
pasar el TTL.
"""

import time
import threading
from functools import wraps


def ttl_cache(seconds=120, max_entradas=512):
    """Decora una función `query(sql, params)` y cachea su resultado por TTL.

    Incluye **single-flight**: si dos hilos piden la misma clave a la vez, solo
    uno ejecuta la consulta y el resto espera su resultado (evita repetir la
    misma query a Turso en peticiones concurrentes).
    """
    def deco(fn):
        store = {}
        inflight = {}
        lock = threading.Lock()

        @wraps(fn)
        def wrapper(*args, **kwargs):
            sql = args[0] if args else ''
            params = tuple(args[1]) if len(args) > 1 and args[1] is not None else ()
            key = (sql, params)
            ahora = time.time()
            with lock:
                hit = store.get(key)
                if hit and ahora - hit[0] < seconds:
                    return hit[1]
                esperando = inflight.get(key)
                if esperando is None:
                    esperando = inflight[key] = threading.Event()
                    es_dueno = True
                else:
                    es_dueno = False

            # El hilo que llegó segundo espera a que el dueño termine y reusa su
            # resultado (si el dueño falló, calcula por su cuenta).
            if not es_dueno:
                esperando.wait(timeout=seconds)
                with lock:
                    hit = store.get(key)
                    if hit and (time.time() - hit[0]) < seconds:
                        return hit[1]

            error = None
            resultado = None
            try:
                resultado = fn(*args, **kwargs)
            except BaseException as exc:  # noqa: BLE001 - se re-lanza
                error = exc
                raise
            finally:
                with lock:
                    if error is None:
                        # Guardar ANTES de despertar a los que esperan: así el
                        # primero que despierte ya encuentra el resultado.
                        store[key] = (time.time(), resultado)
                        if len(store) > max_entradas:
                            mas_vieja = min(store, key=lambda k: store[k][0])
                            store.pop(mas_vieja, None)
                    evento = inflight.pop(key, None)
                    if evento:
                        evento.set()
            return resultado

        def cache_clear():
            with lock:
                store.clear()

        wrapper.cache_clear = cache_clear
        return wrapper

    return deco
