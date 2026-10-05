# -*- coding: utf-8 -*-
"""
ALETHEIA — Minificador/versionador de estáticos (F12 + F7).

Genera versiones `.min.css`/`.min.js` de los assets propios y reescribe los
`<link>`/`<script>` de los HTML para apuntar a ellas con `?v=<hash8>` (para la
caché inmutable de `backend/app.py`). Los archivos FUENTE se conservan intactos:
el desarrollo se hace sobre ellos y se vuelve a correr este script.

Uso (desde la raíz del proyecto):
    py tools/minificar_assets.py

Notas de seguridad:
- El CSS se minifica con máquina de estados (respeta cadenas/url()).
- El JS solo elimina líneas vacías e indentación (conserva comentarios y
  saltos de línea), por lo que no puede romper la sintaxis ni el ASI; el
  contenido de los template literals solo pierde indentación (cosmético).
"""

import hashlib
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ASSETS = [
    'comun/theme.css',
    'comun/vct.css',
    'comun/vct.js',
    'header/header.css',
    'header/header.js',
    'header/header-nodes.js',
    'aletheia/style.css',
    'aletheia/script.js',
]

PAGINAS = [
    'aletheia/index.html',
    'aletheia_preparar/index.html',
    'equipos/index.html',
    'eventos/index.html',
    'header/index.html',
    'inicio/index.html',
    'jugadores/index.html',
    'partidos/index.html',
    'tablas/index.html',
    'visualizar/index.html',
]


# ─── MINIFICADORES ───────────────────────────────────────────────────────────
def minificar_css(texto):
    salida = []
    i, n = 0, len(texto)
    comilla = None
    while i < n:
        c = texto[i]
        if comilla:
            salida.append(c)
            if c == '\\' and i + 1 < n:
                salida.append(texto[i + 1])
                i += 2
                continue
            if c == comilla:
                comilla = None
            i += 1
            continue
        if c in ('"', "'"):
            comilla = c
            salida.append(c)
            i += 1
            continue
        if c == '/' and i + 1 < n and texto[i + 1] == '*':
            fin = texto.find('*/', i + 2)
            i = n if fin == -1 else fin + 2
            continue
        salida.append(c)
        i += 1
    css = ''.join(salida)
    css = re.sub(r'\s+', ' ', css)
    css = re.sub(r'\s*([{};,])\s*', r'\1', css)
    css = re.sub(r'\s*:\s*', ':', css)
    css = re.sub(r';}', '}', css)
    return css.strip()


def minificar_js(texto):
    lineas = []
    for linea in texto.replace('\r\n', '\n').replace('\r', '\n').split('\n'):
        limpia = linea.strip()
        if limpia:
            lineas.append(limpia)
    return '\n'.join(lineas) + '\n'


def hash8(ruta):
    with open(ruta, 'rb') as fh:
        return hashlib.md5(fh.read()).hexdigest()[:8]


# ─── VERSIONADO DE HTML ──────────────────────────────────────────────────────
RE_REF = re.compile(r'''(?P<pre><(?:link|script)\b[^>]*?\b(?:href|src)=["'])(?P<url>[^"']+?\.(?:css|js))(?:\?[^"']*)?(?P<post>["'])''', re.I)


def procesar_html(ruta_html, mapa):
    """Reescribe las referencias .css/.js por su .min + ?v=<hash>."""
    with open(ruta_html, encoding='utf-8') as fh:
        html = fh.read()
    dir_html = os.path.dirname(ruta_html)

    def reemplazo(m):
        url = m.group('url')
        ref = url.split('?', 1)[0]
        if ref.startswith(('http://', 'https://', '//')):
            return m.group(0)
        absoluta = os.path.normpath(os.path.join(dir_html, ref))
        rel_proyecto = os.path.relpath(absoluta, ROOT).replace('\\', '/')
        info = mapa.get(rel_proyecto)
        if not info:
            # Ya versionado antes: volver al origen para recalcular el hash.
            raiz, ext = os.path.splitext(rel_proyecto)
            info = mapa.get(raiz[:-4] + ext) if raiz.endswith('.min') else None
        if not info:
            return m.group(0)
        nueva = os.path.relpath(
            os.path.join(ROOT, info['min']), os.path.join(ROOT, dir_html)
        ).replace('\\', '/')
        return f"{m.group('pre')}{nueva}?v={info['hash']}{m.group('post')}"

    nuevo = RE_REF.sub(reemplazo, html)
    if nuevo != html:
        with open(ruta_html, 'w', encoding='utf-8', newline='\n') as fh:
            fh.write(nuevo)
        return True
    return False


def main():
    mapa = {}
    print('Minificando:')
    for rel in ASSETS:
        origen = os.path.join(ROOT, rel)
        if not os.path.isfile(origen):
            print(f'  ! falta {rel}')
            continue
        with open(origen, encoding='utf-8') as fh:
            texto = fh.read()
        minificado = minificar_css(texto) if rel.endswith('.css') else minificar_js(texto)
        raiz, ext = os.path.splitext(rel)
        rel_min = f'{raiz}.min{ext}'
        destino = os.path.join(ROOT, rel_min)
        with open(destino, 'w', encoding='utf-8', newline='\n') as fh:
            fh.write(minificado)
        mapa[rel] = {'min': rel_min, 'hash': hash8(destino)}
        print(f'  {rel}  {len(texto):>7} -> {len(minificado):>7} B  v={mapa[rel]["hash"]}')
    print('Versionando HTML:')
    for rel in PAGINAS:
        ruta = os.path.join(ROOT, rel)
        if os.path.isfile(ruta) and procesar_html(ruta, mapa):
            print(f'  {rel}')
    print('Listo.')


if __name__ == '__main__':
    main()
