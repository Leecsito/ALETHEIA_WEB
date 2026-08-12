const API = `${window.location.origin}/api`;

const tablesGrid = document.getElementById('tablesGrid');
const btnExportZip = document.getElementById('btnExportZip');

// Evento ZIP
btnExportZip.addEventListener('click', () => {
  window.location.href = `${API}/export/zip`;
});

// Cargar estado de las tablas desde el backend
async function loadTablesInfo() {
  try {
    const res = await fetch(`${API}/export/tables`);
    const json = await res.json();

    if (!json.ok || !json.tablas) {
      tablesGrid.innerHTML = `<div class="loading-spinner">Error cargando tablas: ${json.error || 'Desconocido'}</div>`;
      return;
    }

    renderTables(json.tablas);
  } catch (err) {
    tablesGrid.innerHTML = `<div class="loading-spinner">Error de conexión con el servidor backend.</div>`;
  }
}

function renderTables(tablas) {
  tablesGrid.innerHTML = '';

  tablas.forEach(t => {
    const card = document.createElement('div');
    card.className = 'table-card';

    const colsHtml = t.columnas.map(c => `<span class="col-tag">${c}</span>`).join('');

    card.innerHTML = `
      <div class="card-header">
        <div class="table-name">${t.tabla.toUpperCase()}</div>
        <div class="table-rows-tag">${t.filas} filas</div>
      </div>
      <div class="card-body">
        <div class="cols-label">COLUMNAS (${t.columnas.length})</div>
        <div class="cols-tags">${colsHtml || '<span class="col-tag">Sin columnas</span>'}</div>
      </div>
      <div class="card-actions">
        <a href="${API}/export/csv/${t.tabla}" class="btn-export csv" download>CSV</a>
        <a href="${API}/export/excel/${t.tabla}" class="btn-export excel" download>EXCEL</a>
        <a href="${API}/export/json/${t.tabla}" class="btn-export json" download>JSON</a>
      </div>
    `;

    tablesGrid.appendChild(card);
  });
}

// Inicializar
loadTablesInfo();
