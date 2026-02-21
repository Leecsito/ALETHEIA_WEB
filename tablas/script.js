const API = 'http://localhost:5000/api';

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

    tableTitle.textContent = nombre.toUpperCase();
    tableTitle.classList.add('active');
    emptyState.style.display = 'none';
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
                td.textContent = 'null';
                td.className = 'null-val';
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