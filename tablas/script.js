const API = `${window.location.origin}/api`;

let currentTable = null;
let currentPage = 1;
let currentLimit = 50;
let searchTimer = null;

const tableTitle = document.getElementById('tableTitle');
const tableList = document.getElementById('tableList');
const emptyState = document.getElementById('emptyState');
const tablePanel = document.getElementById('tablePanel');
const tableHead = document.getElementById('tableHead');
const tableBody = document.getElementById('tableBody');
const rowInfo = document.getElementById('rowInfo');
const pagination = document.getElementById('pagination');
const searchInput = document.getElementById('searchInput');
const limitSelect = document.getElementById('limitSelect');
const loadingBar = document.getElementById('loadingBar');
const reportBtn = document.getElementById('reportBtn');
const reportPanel = document.getElementById('reportPanel');
const reportSummary = document.getElementById('reportSummary');
const reportGlobal = document.getElementById('reportGlobal');
const reportList = document.getElementById('reportList');
const reportCopy = document.getElementById('reportCopy');
const reportReload = document.getElementById('reportReload');

// ─── LOADING ─────────────────────────────────────────────────────────────────
function startLoad() {
    loadingBar.className = 'loading-bar active';
}
function endLoad() {
    loadingBar.className = 'loading-bar done';
    setTimeout(() => loadingBar.className = 'loading-bar', 400);
}

// ─── SIDEBAR ─────────────────────────────────────────────────────────────────
async function loadSidebar() {
    try {
        const res = await fetch(`${API}/tablas`);
        const data = await res.json();
        if (!data.ok) return;

        tableList.innerHTML = '';
        data.data.forEach(t => {
            const item = document.createElement('div');
            item.className = 'table-item';
            item.dataset.table = t.tabla;
            item.innerHTML = `
        <span class="table-item-name">${t.tabla}</span>
        <span class="table-item-count">${t.filas.toLocaleString()}</span>
      `;
            item.addEventListener('click', () => selectTable(t.tabla));
            tableList.appendChild(item);
        });
    } catch (e) {
        tableList.innerHTML = '<div class="table-list-loading">Error al conectar</div>';
    }
}

// ─── SELECCIONAR TABLA ────────────────────────────────────────────────────────
function selectTable(nombre) {
    currentTable = nombre;
    currentPage = 1;
    searchInput.value = '';

    // Sidebar activo
    document.querySelectorAll('.table-item').forEach(i => {
        i.classList.toggle('active', i.dataset.table === nombre);
    });
    reportBtn.classList.remove('active');

    tableTitle.textContent = nombre.toUpperCase();
    tableTitle.classList.add('active');
    emptyState.style.display = 'none';
    reportPanel.style.display = 'none';
    tablePanel.style.display = 'flex';

    fetchTable();
}

// ─── FETCH TABLE DATA ─────────────────────────────────────────────────────────
async function fetchTable() {
    if (!currentTable) return;
    startLoad();

    const search = searchInput.value.trim();
    const url = `${API}/tabla/${currentTable}?page=${currentPage}&limit=${currentLimit}${search ? `&search=${encodeURIComponent(search)}` : ''}`;

    try {
        const res = await fetch(url);
        const data = await res.json();
        if (!data.ok) { endLoad(); return; }

        renderHead(data.columns);
        renderBody(data.data, data.columns);
        renderPagination(data.page, data.pages, data.total);

        const from = (data.page - 1) * data.limit + 1;
        const to = Math.min(data.page * data.limit, data.total);
        rowInfo.textContent = `${from}–${to} de ${data.total.toLocaleString()} filas`;

    } catch (e) {
        tableBody.innerHTML = `<tr><td colspan="99" style="color:var(--accent2);padding:20px">Error: ${e.message}</td></tr>`;
    }

    endLoad();
}

// ─── RENDER HEAD ──────────────────────────────────────────────────────────────
function renderHead(columns) {
    tableHead.innerHTML = '';
    const tr = document.createElement('tr');
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col.toUpperCase();
        tr.appendChild(th);
    });
    tableHead.appendChild(tr);
}

// ─── RENDER BODY ──────────────────────────────────────────────────────────────
function renderBody(rows, columns) {
    tableBody.innerHTML = '';
    if (!rows.length) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = columns.length;
        td.style.cssText = 'padding:24px;color:var(--dim);text-align:center';
        td.textContent = 'Sin resultados';
        tr.appendChild(td);
        tableBody.appendChild(tr);
        return;
    }

    rows.forEach(row => {
        const tr = document.createElement('tr');
        columns.forEach(col => {
            const td = document.createElement('td');
            const val = row[col];

            if (val === null || val === undefined || val === '') {
                td.textContent = '';
                td.className = 'null-val';
                td.title = 'vacío';
            } else if (typeof val === 'boolean') {
                td.textContent = val ? 'true' : 'false';
                td.className = val ? 'bool-true' : 'bool-false';
            } else if (typeof val === 'number') {
                td.textContent = val.toLocaleString();
                td.className = 'num-val';
            } else {
                td.textContent = String(val);
                // Colorear IDs y campos de match
                if (col === 'match_id' || col === 'map_id' || col.endsWith('_id')) {
                    td.style.color = 'var(--accent)';
                }
            }

            td.title = String(val ?? '');
            tr.appendChild(td);
        });
        tableBody.appendChild(tr);
    });
}

// ─── PAGINACIÓN ───────────────────────────────────────────────────────────────
function renderPagination(page, pages, total) {
    pagination.innerHTML = '';
    if (pages <= 1) return;

    const addBtn = (label, pageNum, disabled = false, active = false) => {
        const btn = document.createElement('button');
        btn.className = 'page-btn' + (active ? ' active' : '');
        btn.textContent = label;
        btn.disabled = disabled;
        btn.addEventListener('click', () => { currentPage = pageNum; fetchTable(); });
        pagination.appendChild(btn);
    };

    const addDots = () => {
        const s = document.createElement('span');
        s.className = 'page-dots';
        s.textContent = '···';
        pagination.appendChild(s);
    };

    addBtn('←', page - 1, page === 1);

    // Ventana de páginas
    const window_size = 2;
    const show = new Set([1, pages]);
    for (let i = Math.max(1, page - window_size); i <= Math.min(pages, page + window_size); i++) show.add(i);

    let prev = 0;
    Array.from(show).sort((a, b) => a - b).forEach(p => {
        if (prev && p - prev > 1) addDots();
        addBtn(p, p, false, p === page);
        prev = p;
    });

    addBtn('→', page + 1, page === pages);

    const info = document.createElement('span');
    info.className = 'page-info';
    info.textContent = `Página ${page} de ${pages}`;
    pagination.appendChild(info);
}

// ─── REPORTE DE CALIDAD ───────────────────────────────────────────────────────
let lastReportText = '';

async function showReport() {
    currentTable = null;
    document.querySelectorAll('.table-item').forEach(i => i.classList.remove('active'));
    reportBtn.classList.add('active');
    tableTitle.textContent = 'REPORTE DE CALIDAD';
    tableTitle.classList.add('active');
    emptyState.style.display = 'none';
    tablePanel.style.display = 'none';
    reportPanel.style.display = 'flex';

    startLoad();
    try {
        const res = await fetch(`${API}/tablas/reporte`);
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || 'Error al generar el reporte');
        renderReport(data.reporte);
    } catch (e) {
        reportList.innerHTML = `<div style="padding:20px;color:var(--accent2)">Error: ${e.message}</div>`;
    }
    endLoad();
}

function renderReport(r) {
    lastReportText = r.texto || '';
    const filasTotales = Object.values(r.tablas || {}).reduce((a, b) => a + b, 0);
    reportSummary.innerHTML = `
        <div class="report-stat"><b>${r.total_partidos.toLocaleString()}</b><span>Partidos</span></div>
        <div class="report-stat"><b>${r.partidos_con_problemas.toLocaleString()}</b><span>Con problemas</span></div>
        <div class="report-stat"><b>${r.total_incidencias.toLocaleString()}</b><span>Incidencias</span></div>
        <div class="report-stat"><b>${filasTotales.toLocaleString()}</b><span>Filas en BD</span></div>
    `;

    reportGlobal.innerHTML = (r.global && r.global.length)
        ? r.global.map(g => `<div class="g-item">[${g.tabla}] ${g.etiqueta}: <b>${g.filas.toLocaleString()}</b></div>`).join('')
        : '<div class="g-item" style="border-color:var(--green)">Sin problemas globales</div>';

    if (!r.por_partido.length) {
        reportList.innerHTML = '<div style="padding:20px;color:var(--green)">Sin incidencias por partido 🎉</div>';
        return;
    }

    reportList.innerHTML = r.por_partido.map(p => `
        <div class="report-match">
            <a href="${p.url}" target="_blank" rel="noopener">vlr.gg/${p.match_id}</a>
            <span class="rmeta">${p.team_a || '?'} vs ${p.team_b || '?'}</span>
            <ul>${p.incidencias.map(i => `<li><span class="tag">[${i.tabla}]</span> ${i.etiqueta} (${i.filas})</li>`).join('')}</ul>
        </div>
    `).join('');
}

reportBtn.addEventListener('click', showReport);

reportReload.addEventListener('click', showReport);

reportCopy.addEventListener('click', async () => {
    try {
        await navigator.clipboard.writeText(lastReportText);
        reportCopy.textContent = '✓ COPIADO';
    } catch {
        reportCopy.textContent = '✗ ERROR';
    }
    setTimeout(() => reportCopy.textContent = 'COPIAR', 1500);
});

// ─── EVENTOS ─────────────────────────────────────────────────────────────────
searchInput.addEventListener('input', () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
        currentPage = 1;
        fetchTable();
    }, 400);
});

limitSelect.addEventListener('change', () => {
    currentLimit = parseInt(limitSelect.value);
    currentPage = 1;
    fetchTable();
});

// ─── INIT ─────────────────────────────────────────────────────────────────────
loadSidebar();