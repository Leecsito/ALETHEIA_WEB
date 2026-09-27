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
    """Decora una función `query(sql, params)` y cachea su resultado por TTL."""
    def deco(fn):
        store = {}
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
            resultado = fn(*args, **kwargs)
            with lock:
                store[key] = (ahora, resultado)
                if len(store) > max_entradas:
                    mas_vieja = min(store, key=lambda k: store[k][0])
                    store.pop(mas_vieja, None)
            return resultado

        def cache_clear():
            with lock:
                store.clear()

        wrapper.cache_clear = cache_clear
        return wrapper

    return deco
