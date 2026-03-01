const isLocal = ['localhost', '127.0.0.1'].includes(window.location.hostname);
const API = isLocal ? `${window.location.origin}/api` : 'https://aletheia-web.onrender.com/api';

// ─── MAPA: nombre base de archivo → key del card ──────────────────────────
const FILE_MAP = {
  'vct_partidos': 'vct_partidos',
  'vlr_mapas': 'vlr_mapas',
  'vlr_rondas': 'vlr_rondas',
  'vlr_economia_rondas': 'vlr_economia_rondas',
  'vlr_stats_players_sides': 'vlr_stats_players_sides',
  'vlr_economia_resumen': 'vlr_economia_resumen',
  'vlr_enfrentamientos': 'vlr_enfrentamientos',
  'vlr_multikills_clutches': 'vlr_multikills_clutches',
  'vct_equipos': 'vct_equipos',
  'vct_jugadores': 'vct_jugadores',
};

const REQUIRED = [
  'vct_partidos', 'vlr_mapas', 'vlr_rondas', 'vlr_economia_rondas',
  'vlr_stats_players_sides', 'vlr_economia_resumen',
  'vlr_enfrentamientos', 'vlr_multikills_clutches'
];
const OPTIONAL = ['vct_equipos', 'vct_jugadores'];

// ─── ESTADO ──────────────────────────────────────────────────────────────────
const state = { files: {} };

// ─── DOM ─────────────────────────────────────────────────────────────────────
const btnRun = document.getElementById('btnRun');
const btnInit = document.getElementById('btnInit');
const btnClear = document.getElementById('btnClear');
const btnBulk = document.getElementById('btnBulk');
const bulkInput = document.getElementById('bulkInput');
const fileCount = document.getElementById('fileCount');
const countRequired = document.getElementById('countRequired');
const countOptional = document.getElementById('countOptional');
const logSection = document.getElementById('logSection');
const logBody = document.getElementById('logBody');
const dbStatus = document.getElementById('dbStatus');
const dbLabel = dbStatus.querySelector('.db-label');
const dot = dbStatus.querySelector('.dot');
const progressWrap = document.getElementById('progressWrap');
const progressBar = document.getElementById('progressBar');
const progressLabel = document.getElementById('progressLabel');

// ─── LOGGER ──────────────────────────────────────────────────────────────────
function log(msg, type = 'info') {
  logSection.style.display = 'block';
  const now = new Date();
  const time = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
  const line = document.createElement('div');
  line.className = `log-line ${type}`;
  line.innerHTML = `<span class="log-time">${time}</span><span class="log-msg">${msg}</span>`;
  logBody.appendChild(line);
  logBody.scrollTop = logBody.scrollHeight;
}

// ─── ASIGNAR ARCHIVO A CARD ───────────────────────────────────────────────────
function assignFile(key, file) {
  const card = document.querySelector(`.file-card[data-key="${key}"]`);
  if (!card) return false;
  state.files[key] = file;
  card.classList.remove('error');
  card.classList.add('loaded');
  card.querySelector('.state-icon').textContent = '✓';
  card.querySelector('.card-desc').textContent = file.name;
  return true;
}

// ─── FILE CARDS (click individual) ───────────────────────────────────────────
document.querySelectorAll('.file-card').forEach(card => {
  const input = card.querySelector('.file-input');
  const key = card.dataset.key;
  input.addEventListener('change', () => {
    const file = input.files[0];
    if (!file) return;
    if (!file.name.endsWith('.xlsx')) {
      card.classList.add('error');
      card.querySelector('.state-icon').textContent = '✕';
      log(`${file.name} — no es un archivo .xlsx`, 'error');
      return;
    }
    assignFile(key, file);
    log(`${key} → ${file.name} (${formatSize(file.size)})`, 'success');
    updateCounts();
  });
});

// ─── BULK UPLOAD ─────────────────────────────────────────────────────────────
btnBulk.addEventListener('click', () => bulkInput.click());

bulkInput.addEventListener('change', () => {
  const files = Array.from(bulkInput.files);
  let assigned = 0;
  let unmatched = [];

  files.forEach(file => {
    if (!file.name.endsWith('.xlsx')) return;
    const baseName = file.name.replace(/\.xlsx$/i, '').toLowerCase().trim();
    const key = Object.keys(FILE_MAP).find(k => k.toLowerCase() === baseName);
    if (key) {
      assignFile(key, file);
      assigned++;
    } else {
      unmatched.push(file.name);
    }
  });

  log(`─────────────────────────────────`, 'info');
  log(`Bulk upload: ${files.length} archivos seleccionados`, 'accent');
  log(`  ✓ ${assigned} asignados automáticamente`, 'success');
  if (unmatched.length) log(`  ⚠ No reconocidos: ${unmatched.join(', ')}`, 'warn');
  updateCounts();
  bulkInput.value = '';
});

// ─── CONTADORES ──────────────────────────────────────────────────────────────
function formatSize(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function updateCounts() {
  const reqLoaded = REQUIRED.filter(k => state.files[k]).length;
  const optLoaded = OPTIONAL.filter(k => state.files[k]).length;
  const total = reqLoaded + optLoaded;
  countRequired.textContent = `${reqLoaded} / 8`;
  countOptional.textContent = `${optLoaded} / 2`;
  fileCount.textContent = `${total} archivo${total !== 1 ? 's' : ''}`;
  // Solo necesitamos vct_partidos para poder ejecutar
  btnRun.disabled = !state.files['vct_partidos'];
}

// ─── PROGRESS ────────────────────────────────────────────────────────────────
function setProgress(pct, label) {
  progressWrap.style.display = 'block';
  progressBar.style.width = `${pct}%`;
  progressLabel.textContent = label;
}
function hideProgress() {
  setTimeout(() => { progressWrap.style.display = 'none'; progressBar.style.width = '0%'; }, 1500);
}

// ─── INIT DB ─────────────────────────────────────────────────────────────────
btnInit.addEventListener('click', async () => {
  btnInit.disabled = true; btnInit.textContent = '...';
  log('Inicializando tablas en SQLite...', 'warn');
  try {
    const res = await fetch(`${API}/init-db`, { method: 'POST' });
    const data = await res.json();
    if (data.ok) { log('✓ Tablas creadas correctamente.', 'success'); checkStatus(); }
    else { log(`✕ Error: ${data.error}`, 'error'); }
  } catch (e) { log(`✕ No se pudo conectar: ${e.message}`, 'error'); }
  finally { btnInit.disabled = false; btnInit.textContent = 'INIT DB'; }
});

// ─── STATUS ──────────────────────────────────────────────────────────────────
async function checkStatus() {
  try {
    const res = await fetch(`${API}/status`);
    const data = await res.json();
    if (data.ok) {
      dot.className = 'dot connected'; dbLabel.textContent = 'SQLite conectado';
      const totals = Object.entries(data.tables).map(([t, c]) => `${t}: ${c}`).join(' · ');
      log(`DB Status — ${totals}`, 'accent');
    } else {
      dot.className = 'dot error'; dbLabel.textContent = 'Error de conexión';
      log(`DB Error: ${data.error}`, 'error');
    }
  } catch { dot.className = 'dot error'; dbLabel.textContent = 'Backend no disponible'; }
}

// ─── ETL RUN ─────────────────────────────────────────────────────────────────
btnRun.addEventListener('click', async () => {
  btnRun.disabled = true;
  log('─────────────────────────────────', 'info');
  log('Iniciando ETL Pipeline...', 'accent');
  setProgress(5, 'Preparando archivos...');

  const form = new FormData();
  Object.entries(state.files).forEach(([key, file]) => form.append(key, file));
  setProgress(15, 'Subiendo archivos...');
  log(`Subiendo ${Object.keys(state.files).length} Excel al backend...`, 'info');

  try {
    setProgress(30, 'Procesando ETL...');
    const res = await fetch(`${API}/etl`, { method: 'POST', body: form });
    setProgress(80, 'Insertando en base de datos...');
    const data = await res.json();

    if (data.ok) {
      setProgress(100, 'ETL completado.');
      log('─────────────────────────────────', 'info');
      log('✓ ETL completado exitosamente.', 'success');
      const ins = data.inserted;
      if (ins.matches != null) log(`  matches:          ${ins.matches} partidos`, 'success');
      if (ins.maps != null) log(`  maps:             ${ins.maps} mapas`, 'success');
      if (ins.rounds != null) log(`  rounds:           ${ins.rounds} rondas`, 'success');
      if (ins.player_stats != null) log(`  player_stats:     ${ins.player_stats} filas`, 'success');
      if (ins.economy_summary != null) log(`  economy_summary:  ${ins.economy_summary} filas`, 'success');
      if (ins.duels != null) log(`  duels:            ${ins.duels} enfrentamientos`, 'success');
      if (ins.multikills != null) log(`  multikills:       ${ins.multikills} filas`, 'success');
      if (ins.teams != null) log(`  teams:            ${ins.teams} equipos`, 'success');
      if (ins.players != null) log(`  players:          ${ins.players} jugadores`, 'success');
      log('─────────────────────────────────', 'info');
      checkStatus(); hideProgress(); resetCards();
    } else {
      setProgress(0, 'Error en ETL.');
      log(`✕ Error: ${data.error}`, 'error');
      if (data.trace) log(data.trace.split('\n').slice(-3).join(' '), 'error');
      hideProgress();
    }
  } catch (e) {
    setProgress(0, 'Error de conexión.');
    log(`✕ ${e.message}`, 'error');
    hideProgress();
  } finally {
    btnRun.disabled = !state.files['vct_partidos'];
  }
});

// ─── RESET ───────────────────────────────────────────────────────────────────
function resetCards() {
  document.querySelectorAll('.file-card').forEach(card => {
    card.classList.remove('loaded', 'error');
    card.querySelector('.state-icon').textContent = '↑';
    card.querySelector('.file-input').value = '';
    card.querySelector('.card-desc').textContent = `${card.dataset.key}.xlsx`;
  });
  state.files = {};
  updateCounts();
}

btnClear.addEventListener('click', () => { logBody.innerHTML = ''; logSection.style.display = 'none'; });

// ─── INIT ─────────────────────────────────────────────────────────────────────
checkStatus();
log('Sistema listo. Usa "SUBIR TODOS" o sube los Excel uno a uno.', 'accent');