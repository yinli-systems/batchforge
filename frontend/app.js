const icons = {
  overview: '<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/>',
  batches: '<rect x="4" y="3" width="16" height="18" rx="2"/><path d="M8 8h8M8 12h8M8 16h5"/>',
  workers: '<rect x="6" y="6" width="12" height="12" rx="2"/><path d="M9 2v4m6-4v4M9 18v4m6-4v4M2 9h4m-4 6h4m12-6h4m-4 6h4"/><rect x="10" y="10" width="4" height="4" rx=".5"/>',
  code: '<path d="m8 7-5 5 5 5m8-10 5 5-5 5m-3-14-2 18"/>',
  key: '<circle cx="8" cy="9" r="4"/><path d="m11 12 9 9m-4-4 3-3m-6 0 3-3"/>',
  layers: '<path d="m12 3 9 5-9 5-9-5 9-5Zm-9 9 9 5 9-5M3 16l9 5 9-5"/>',
  refresh: '<path d="M20 7v5h-5M4 17v-5h5"/><path d="M6 7a7 7 0 0 1 12-1l2 3M4 15l2 3a7 7 0 0 0 12-1"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  arrow: '<path d="M5 12h14m-5-5 5 5-5 5"/>',
  chevron: '<path d="m9 5 7 7-7 7"/>',
  close: '<path d="m6 6 12 12M6 18 18 6"/>',
  eye: '<path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z"/><circle cx="12" cy="12" r="3"/>',
  lock: '<rect x="5" y="10" width="14" height="11" rx="2"/><path d="M8 10V7a4 4 0 0 1 8 0v3"/>',
  upload: '<path d="M12 16V3m-5 5 5-5 5 5M4 15v5a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1v-5"/>',
  download: '<path d="M12 3v13m-5-5 5 5 5-5M4 16v4a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1v-4"/>',
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6m0-10v.1"/>',
  check: '<path d="m5 12 4 4L19 6"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  tasks: '<path d="m3 6 2 2 3-3m-5 8 2 2 3-3m-5 8 2 2 3-3M12 7h9m-9 7h9m-9 7h9"/>',
  tokens: '<path d="m12 2 9 5v10l-9 5-9-5V7l9-5Zm0 10L3 7m9 5 9-5m-9 5v10"/>',
  activity: '<path d="M2 12h5l3-8 4 16 3-8h5"/>',
  search: '<circle cx="10" cy="10" r="6"/><path d="m15 15 6 6"/>',
  menu: '<path d="M4 6h16M4 12h16M4 18h16"/>',
  copy: '<rect x="8" y="8" width="12" height="13" rx="2"/><path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/>',
  alert: '<path d="m12 3 10 17H2L12 3Z"/><path d="M12 9v5m0 3v.1"/>',
  stop: '<rect x="5" y="5" width="14" height="14" rx="2"/>',
};
const icon = (name) =>
  `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${icons[name] || icons.info}</svg>`;
const $ = (selector, scope = document) => scope.querySelector(selector);
const $$ = (selector, scope = document) => [...scope.querySelectorAll(selector)];
const esc = (value) => String(value ?? '').replace(/[&<>"']/g, c => ({
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;'
} [c]));
const number = (value) => Number.isFinite(Number(value)) ? new Intl.NumberFormat('en-US').format(Number(
  value)) : '—';
const shortNumber = (value) => new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 1
}).format(Number(value) || 0);
const dateValue = (value) => value ? new Date(typeof value === 'number' ? value * 1000 : value) : null;
const dateText = (value, options = {}) => {
  const d = dateValue(value);
  return d && !Number.isNaN(d.getTime()) ? d.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    ...options
  }) : '—';
};
const relativeTime = (value) => {
  const date = dateValue(value);
  if (!date || Number.isNaN(date.getTime())) return '—';
  const sec = Math.max(0, Math.floor((Date.now() - date.getTime()) / 1000));
  return sec < 60 ? 'Just now' : sec < 3600 ? `${Math.floor(sec/60)}m ago` : sec < 86400 ?
    `${Math.floor(sec/3600)}h ago` : dateText(value, {
      hour: undefined,
      minute: undefined
    });
};
const terminal = (batch) => ['completed', 'failed', 'cancelled', 'expired'].includes(batch?.status);
const names = {
  overview: 'Overview',
  batches: 'Batches',
  workers: 'Workers',
  docs: 'API reference'
};
const storage = {
  get() {
    try {
      return sessionStorage.getItem('batchforge-api-key') || ''
    } catch {
      return ''
    }
  },
  set(value) {
    try {
      value ? sessionStorage.setItem('batchforge-api-key', value) : sessionStorage.removeItem(
        'batchforge-api-key')
    } catch {}
  }
};
const retryAttemptStorageKey = 'batchforge-retry-attempts';
function loadRetryAttempts() {
  try {
    const entries = JSON.parse(sessionStorage.getItem(retryAttemptStorageKey) || '[]');
    if (!Array.isArray(entries)) return new Map();
    return new Map(entries.filter(entry => Array.isArray(entry) && entry.length === 2 &&
      typeof entry[0] === 'string' && typeof entry[1] === 'string'));
  } catch {
    return new Map();
  }
}
const retryAttempts = loadRetryAttempts();
function saveRetryAttempts() {
  try {
    sessionStorage.setItem(retryAttemptStorageKey, JSON.stringify([...retryAttempts]));
  } catch {}
}
function newIdempotencyKey() {
  return globalThis.crypto?.randomUUID?.() ||
    `console-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}
const state = {
  key: storage.get(),
  connected: false,
  loading: false,
  overview: null,
  batches: [],
  hasMore: false,
  lastId: null,
  loadedMore: false,
  models: [],
  view: 'overview',
  error: '',
  updated: null,
  filter: 'all',
  search: '',
  detail: null,
  detailId: null,
  detailErrors: [],
  detailErrorsFile: null,
  detailLoading: false,
  uploadFile: null,
  inputMode: 'upload',
  createAttempt: null,
  refreshing: false
};
const counts = batch => ({
  total: 0,
  completed: 0,
  failed: 0,
  cancelled: 0,
  running: 0,
  pending: 0,
  ...batch?.request_counts
});
const completedCount = batch => {
  const c = counts(batch);
  return c.completed + c.failed + c.cancelled
};
const progress = batch => {
  const c = counts(batch);
  return c.total ? Math.min(100, Math.round(completedCount(batch) / c.total * 100)) : 0
};
const batchName = batch => batch.metadata?.name || batch.metadata?.title || 'Untitled batch';
const workerOnline = worker => worker.status === 'online';
const engineCountReported = value => Number.isInteger(value) && value >= 0 && value <= 4096;
const workerReadinessReported = worker => engineCountReported(worker.engine_count) &&
  engineCountReported(worker.ready_engine_count);
const workerReady = worker => workerOnline(worker) && workerReadinessReported(worker) &&
  worker.ready_engine_count > 0;
function workerHealth(worker) {
  if (!workerOnline(worker)) return {label: 'Offline', style: 'failed'};
  if (!workerReadinessReported(worker)) return {label: 'Readiness unknown', style: 'queued'};
  if (!worker.ready_engine_count) return {label: 'Warming / degraded', style: 'queued'};
  if (worker.ready_engine_count < worker.engine_count) return {label: 'Partially ready', style: 'queued'};
  return {label: 'Ready', style: 'success'};
}

function hydrateIcons(scope = document) {
  $$('[data-icon]', scope).forEach(el => {
    el.innerHTML = icon(el.dataset.icon)
  })
}

function statusBadge(status) {
  const style = {
    completed: 'success',
    in_progress: 'running',
    running: 'running',
    validating: 'queued',
    pending: 'queued',
    queued: 'queued',
    failed: 'failed',
    cancelled: '',
    expired: 'failed'
  } [status] || '';
  return `<span class="status-badge ${style}"><span class="status-dot"></span>${esc((status||'unknown').replaceAll('_',' '))}</span>`
}

function toast(message, type = 'success') {
  const el = document.createElement('div');
  el.className = `toast ${type==='error'?'error':''}`;
  el.innerHTML = `${icon(type==='error'?'alert':'check')}<span>${esc(message)}</span>`;
  $('#toast-region').append(el);
  setTimeout(() => el.remove(), 6000)
}
class APIError extends Error {
  constructor(message, status) {
    super(message);
    this.status = status
  }
}
async function api(path, options = {}) {
  const {
    raw = false, ...request
  } = options;
  const headers = new Headers(request.headers || {});
  headers.set('Authorization', `Bearer ${state.key}`);
  if (request.body && !(request.body instanceof FormData) && !headers.has('Content-Type')) headers.set(
    'Content-Type', 'application/json');
  let response;
  try {
    response = await fetch(path, {
      ...request,
      headers,
      credentials: 'same-origin',
      cache: 'no-store'
    })
  } catch {
    throw new APIError('Unable to reach the server. Check your connection and try again.', 0)
  }
  if (!response.ok) {
    let data;
    try {
      data = await response.json()
    } catch {}
    throw new APIError(data?.error?.message || `Request failed (${response.status}).`, response.status)
  }
  if (raw) return response;
  return response.status === 204 ? null : response.json()
}

function updateConnection() {
  const badge = $('#connection-badge');
  badge.className = `connection-badge ${state.connected?'connected':''}`;
  badge.innerHTML =
    `<span class="status-dot ${state.connected?'healthy':''}"></span>${state.connected?'Workspace connected':'Disconnected'}`;
  $('#account-label').textContent = state.connected ? 'Workspace connected' : 'Connect workspace';
  $('#account-description').textContent = state.connected ? 'Manage API connection' : 'API key required';
  const workers = state.overview?.workers || [];
  const online = workers.filter(workerOnline).length;
  const ready = workers.filter(workerReady).length;
  const fullyReady = online > 0 && workers.filter(workerOnline).every(worker => workerHealth(worker).style === 'success');
  $('#sidebar-health').textContent = !state.connected ? 'Not connected' : !online ? 'Waiting for workers' :
    fullyReady ? 'Inference available' : ready ? 'Cluster partially ready' :
    workers.some(worker => workerOnline(worker) && !workerReadinessReported(worker)) ? 'Readiness unknown' : 'Engines not ready';
  $('#sidebar-health-detail').textContent = !state.connected ? 'Connect to your inference cluster.' :
    `${ready} ready / ${online} connected workers.`;
  $('#sidebar-health-dot').className = `status-dot ${state.connected?(fullyReady?'healthy':'warning'):''}`;
  $('#nav-worker-dot').hidden = !online;
  $('#nav-worker-dot').classList.toggle('warning', !fullyReady);
  const active = (state.overview?.recent_batches || state.batches).filter(b => !terminal(b)).length;
  $('#nav-batch-count').textContent = number(active);
  $('#nav-batch-count').hidden = !state.connected || !active;
  $('#last-updated').textContent = state.updated ?
    `Last synced ${state.updated.toLocaleTimeString(undefined,{hour:'2-digit',minute:'2-digit',second:'2-digit'})} · Auto-refresh 8s` :
    'Waiting for connection'
}

function pageHeading(title, description, actions = '') {
  return `<div class="page-heading"><div><h1>${title}</h1><p>${description}</p></div><div class="page-heading-actions">${actions}</div></div>`
}
const createButton = () =>
  `<button class="button button-primary" data-action="create">${icon('plus')}Create batch<span class="keyboard-hint">N</span></button>`;

function connectionBanner() {
  return !state.connected ?
    `<div class="connection-banner"><span>${icon('key')}</span><div><h2>Your inference control plane is ready.</h2><p>Connect your workspace to submit tasks and inspect real-time execution.</p></div><button class="button button-primary" data-action="connect">Connect workspace${icon('arrow')}</button></div>` :
    ''
}

function errorBanner() {
  return state.error ?
    `<div class="error-banner">${icon('alert')}<span>${esc(state.error)}</span><button class="link-button" data-action="refresh">Retry${icon('refresh')}</button></div>` :
    ''
}

function metricCard(label, value, description, ic, unit = '') {
  return `<article class="metric-card"><div class="metric-top"><span>${label}</span>${icon(ic)}</div><div class="metric-value">${state.loading&&!state.overview?'<span class="skeleton"></span>':value}${unit?`<span class="metric-unit">${unit}</span>`:''}</div><div class="metric-bottom">${description}</div></article>`
}

function overviewView() {
  const s = state.overview?.stats;
  const dash = '—';
  const resolved = s ? Number(s.completed_tasks) + Number(s.failed_tasks) : 0;
  const success = resolved ? `${(Number(s.completed_tasks)/resolved*100).toFixed(1)}%` : '—';
  const completionNote = s && (Number(s.failed_tasks) || Number(s.cancelled_tasks))
    ? `<strong>${number(s.failed_tasks)}</strong> failed · ${number(s.cancelled_tasks)} cancelled`
    : `<strong>${success}</strong> success among resolved tasks`;
  const workers = state.overview?.workers || [];
  return `${pageHeading('Workspace overview','A clear view of every task, token, and worker.',`<a class="button button-secondary" href="#docs">${icon('code')}API reference</a>${createButton()}`)}${connectionBanner()}${errorBanner()}<div class="overview-intro"><span class="live-label"><span class="status-dot ${state.connected?'healthy':''}"></span>${state.connected?'LIVE WORKSPACE':'WORKSPACE PREVIEW'}</span><span>${state.connected?'All-time totals · tenant-scoped data':'Connect to view your cluster’s live activity'}</span></div><div class="metric-grid">${metricCard('Total tasks',s?number(s.total_tasks):dash,s?`<span class="dot"></span><strong>${number(s.queued_tasks)}</strong> queued · ${number(s.running_tasks)} running`:'Across all submitted batches','tasks')}${metricCard('Completed tasks',s?number(s.completed_tasks):dash,s?completionNote:'Successfully processed requests','check')}${metricCard('Tokens processed',s?shortNumber(Number(s.prompt_tokens)+Number(s.completion_tokens)):dash,s?`${shortNumber(s.prompt_tokens)} input · ${shortNumber(s.completion_tokens)} output`:'Measured usage from worker results','tokens')}${metricCard('Connected workers',state.overview?number(workers.filter(workerOnline).length):dash,state.overview?`${number(workers.length)} registered · ${number(workers.reduce((n,w)=>n+(Number(w.concurrency)||0),0))} registered slots`:'Workers reporting to the control plane','workers')}</div><div class="overview-middle"><section class="panel throughput-panel"><div class="panel-header chart-header"><div><div class="panel-title"><h2>Task throughput</h2><span class="tiny-badge">COMPLETIONS</span></div><div class="panel-subtitle">Successful task completions by reporting interval</div></div><span class="period-label">${icon('clock')}Recent activity</span></div><div class="chart-summary"><span class="chart-total">${state.overview?number((state.overview.throughput||[]).reduce((n,b)=>n+Number(b.completed||0),0)):'—'}</span><span class="chart-summary-label">completed in shown window</span></div><div class="chart-container" id="throughput-chart"></div></section>${clusterPanel(workers)}</div><section class="panel"><div class="panel-header"><div class="panel-title"><h2>Recent batches</h2>${s?`<span class="tiny-badge">${number(s.total_batches)}</span>`:''}</div><a class="link-button" href="#batches">View all batches${icon('arrow')}</a></div>${batchTable(state.overview?.recent_batches?.slice(0,5)||[],true)}</section>`
}

function clusterPanel(workers) {
  const connected = workers.filter(workerOnline);
  const ready = workers.filter(workerReady);
  const models = [...new Set(ready.map(worker => worker.model))];
  const fullyReady = connected.length > 0 && connected.every(worker => workerHealth(worker).style === 'success');
  const unknown = connected.some(worker => !workerReadinessReported(worker));
  const label = fullyReady ? 'Ready' : ready.length ? 'Partially ready' : connected.length ?
    (unknown ? 'Readiness unknown' : 'Engines not ready') : 'No connected workers';
  const tiles = (connected.length ? connected.slice(0, 3) : [null]).map(worker => {
    const style = !worker ? 'offline' : workerHealth(worker).style === 'success' ? '' : 'warming';
    return `<div class="gpu-tile ${style}">${icon('workers')}</div>`;
  }).join('');
  return `<section class="panel cluster-panel"><div class="panel-header"><div class="panel-title"><h2>Worker cluster</h2></div><span class="tiny-badge">${state.connected?'LIVE':'NOT CONNECTED'}</span></div><div class="cluster-body"><div class="cluster-orbit">${tiles}${connected.length>3?`<span class="gpu-more">+${connected.length-3}</span>`:''}</div><div class="cluster-row"><span>Inference status</span><strong class="${fullyReady?'text-green':connected.length?'text-orange':''}">${state.connected?label:'—'}</strong></div><div class="cluster-row"><span>Available workers</span><strong>${state.connected?`${ready.length} / ${connected.length}`:'—'}</strong></div><div class="cluster-row"><span>Ready models</span><strong>${state.connected?models.length:'—'}</strong></div></div><div class="cluster-footer"><span>Availability requires ready engines</span><a href="#workers">Inspect cluster${icon('arrow')}</a></div></section>`;
}

function batchTable(batches, compact = false) {
  if (state.loading && !state.overview && !batches.length)
  return `<div class="empty-state"><span class="spinner"></span><p style="margin-top:13px">Loading workspace activity…</p></div>`;
  if (!batches.length && state.view === 'batches' && state.batches.length) return emptyState('search',
    'No matching batches', 'Try another batch name, ID, or status filter.');
  if (!batches.length) return emptyState('batches', state.connected ? 'No batches yet' :
    'Your next workload starts here', state.connected ?
    'Upload a JSONL file to submit your first batch. Every task is tracked from queue to result.' :
    'Connect your workspace to see real batch activity and submit your first workload.', state.connected ?
    'Create your first batch' : 'Connect workspace', state.connected ? 'create' : 'connect');
  return `<div class="table-wrap"><table><thead><tr><th>BATCH</th><th>STATUS</th><th>PROGRESS</th><th>${compact?'CREATED':'COMPLETED / FAILED'}</th><th></th></tr></thead><tbody>${batches.map(b=>{const c=counts(b);return `<tr class="clickable" data-batch-id="${esc(b.id)}" tabindex="0" aria-label="View ${esc(batchName(b))}"><td><span class="batch-name">${esc(batchName(b))}</span><span class="batch-id">${esc(b.id)}</span></td><td>${statusBadge(b.status)}</td><td class="progress-cell"><div class="progress-meta"><span>${number(completedCount(b))} / ${number(c.total)}</span><span>${progress(b)}%</span></div><div class="progress-track"><div class="progress-fill ${b.status==='in_progress'?'running':c.failed?'failed':''}" style="width:${progress(b)}%"></div></div></td><td>${compact?`<span title="${esc(dateText(b.created_at))}">${relativeTime(b.created_at)}</span>`:`<span class="text-green">${number(c.completed)}</span> <span class="muted">/</span> <span class="${c.failed?'text-red':'muted'}">${number(c.failed)}</span>`}</td><td class="cell-arrow">${icon('chevron')}</td></tr>`}).join('')}</tbody></table></div><div class="table-footer"><span>${compact?'Latest workspace activity':`${number(batches.length)} batch${batches.length===1?'':'es'} shown`}</span><span>${iconText('lock','Durable · Tenant isolated')}</span></div>`
}

function iconText(name, text) {
  return `<span style="display:inline-flex;align-items:center;gap:5px"><span style="display:inline-flex;transform:scale(.65)">${icon(name)}</span>${text}</span>`
}

function emptyState(ic, title, description, buttonLabel = '', action = '') {
  return `<div class="empty-state"><div class="empty-symbol">${icon(ic)}</div><h3>${esc(title)}</h3><p>${esc(description)}</p>${buttonLabel?`<button class="button button-secondary" data-action="${action}">${icon(action==='create'?'plus':'key')}${buttonLabel}</button>`:''}</div>`
}

function filteredBatches() {
  const query = state.search.toLowerCase();
  return state.batches.filter(b => (state.filter === 'all' || b.status === state.filter) && (!query ||
    `${b.id} ${batchName(b)}`.toLowerCase().includes(query)))
}

function batchesView() {
  return `${pageHeading('Batches','Submit once. Track every request through completion.',createButton())}${connectionBanner()}${errorBanner()}<div class="filter-bar"><label class="search-field">${icon('search')}<input id="batch-search" type="search" placeholder="Search batch name or ID…" value="${esc(state.search)}" aria-label="Search batches"></label><label class="sr-only" for="batch-status-filter">Filter by status</label><select class="filter-select" id="batch-status-filter"><option value="all">All statuses</option>${['in_progress','completed','failed','cancelled'].map(s=>`<option value="${s}" ${state.filter===s?'selected':''}>${s.replaceAll('_',' ')}</option>`).join('')}</select><span class="filter-count" id="batch-filter-count">${number(filteredBatches().length)} matching batches</span></div><section class="panel" id="batch-list-panel">${batchTable(filteredBatches())}</section>${state.hasMore?'<div style="display:flex;justify-content:center;margin-top:18px"><button class="button button-secondary" id="load-more">Load more batches</button></div>':''}`
}

function workersView() {
  const workers = state.overview?.workers || [];
  return `${pageHeading('Worker cluster','Inspect the workers consuming your durable inference queue.',`<a class="button button-secondary" href="#docs">${icon('code')}Worker setup</a>`)}${connectionBanner()}${errorBanner()}${workers.length?`<div class="workers-grid">${workers.map(workerCard).join('')}</div>`:`<section class="panel">${emptyState('workers',state.connected?'No workers registered':'Connect to inspect your workers',state.connected?'Start a BatchForge worker connected to an OpenAI-compatible inference engine. Registered workers appear here automatically.':'Worker status, serving models, concurrency, and heartbeat health will appear after you connect.',state.connected?'':'Connect workspace','connect')}</section>`}<div class="panel worker-help"><h3>Execution is isolated from orchestration.</h3><p>GPU workers claim leased tasks, heartbeat during execution, and submit fenced results. If a worker disappears, unfinished tasks can be recovered without accepting stale attempts. Connected describes worker process liveness. Engine readiness is reported separately; configured slots do not guarantee available inference capacity. GPU utilization requires separate engine telemetry.</p></div>`
}

function workerCard(worker) {
  const alive = workerOnline(worker);
  const health = workerHealth(worker);
  const readiness = workerReadinessReported(worker) ?
    `${number(worker.ready_engine_count)} / ${number(worker.engine_count)}` : 'Not reported';
  return `<article class="panel worker-card"><div class="panel-header"><div class="worker-heading"><div class="worker-icon">${icon('workers')}</div><div><h2>${esc(worker.name||worker.id)}</h2><p>${esc(worker.id)}</p></div></div><span class="status-badge ${health.style}"><span class="status-dot"></span>${health.label}</span></div><div class="worker-body"><dl class="worker-spec"><div><dt>Configured concurrency</dt><dd>${number(worker.concurrency)} <span class="muted" style="font-size:10px">registered slots</span></dd></div><div><dt>Ready engines</dt><dd class="${alive&&workerReady(worker)?'text-green':alive?'text-orange':''}">${readiness}</dd></div><div><dt>Worker process</dt><dd>${alive?'Connected':'Offline'}</dd></div><div><dt>Last heartbeat</dt><dd title="${esc(dateText(worker.last_seen_at))}">${relativeTime(worker.last_seen_at)}</dd></div></dl><div class="worker-model"><span>Configured model</span><span class="model-pill" title="${esc(worker.model)}">${esc(worker.model)}</span></div></div></article>`;
}

function codeBlock(label, text) {
  return `<div class="code-block"><div class="code-toolbar"><span>${label}</span><button class="link-button" data-copy-code>${icon('copy')}Copy</button></div><pre><code>${esc(text)}</code></pre></div>`
}

function docsView() {
  const base = location.origin;
  const model = state.models[0]?.id || 'your-model-id';
  const sample = JSON.stringify({
    custom_id: 'product-001',
    method: 'POST',
    url: '/v1/chat/completions',
    body: {
      model,
      messages: [{
        role: 'system',
        content: 'Extract product name, brand, and color as JSON.'
      }, {
        role: 'user',
        content: 'Acme Trail running shoes in forest green.'
      }],
      max_tokens: 128,
      temperature: 0
    }
  });
  return `${pageHeading('API reference','An OpenAI-compatible batch API, backed by durable execution.',`<button class="button button-secondary" data-action="connect">${icon('key')}${state.connected?'Manage connection':'Connect workspace'}</button>`)}<div class="docs-layout"><nav class="docs-nav" aria-label="API reference sections"><a href="#docs" data-doc-anchor="authentication">Authentication</a><a href="#docs" data-doc-anchor="quickstart">Quickstart</a><a href="#docs" data-doc-anchor="requests">Request format</a><a href="#docs" data-doc-anchor="lifecycle">Batch lifecycle</a><a href="#docs" data-doc-anchor="results">Results & retries</a><a href="#docs" data-doc-anchor="workers">Workers & operations</a></nav><div class="docs-content"><section class="docs-section" id="doc-authentication"><h2>Authentication</h2><p>Send your tenant API key in the <code>Authorization</code> header. Public resources are scoped to that key’s tenant. Keep keys on your server and rotate them through your deployment configuration.</p>${codeBlock('HTTP',`Authorization: Bearer $BATCHFORGE_API_KEY\nContent-Type: application/json`)}<div class="docs-note"><p>The console stores your key in this tab’s session storage. Signing out clears it. Use HTTPS when accessing your deployment over a public network.</p></div></section><section class="docs-section" id="doc-quickstart"><h2>Submit your first batch</h2><p>Create a JSONL file, upload it, then submit a batch using the returned file ID. You can also do both steps with “Create batch” in this console.</p><div class="endpoint-heading"><span class="http-method post">POST</span><span class="endpoint-path">/v1/files</span></div>${codeBlock('SHELL',`curl ${base}/v1/files \\\n  -H "Authorization: Bearer $BATCHFORGE_API_KEY" \\\n  -F purpose=batch \\\n  -F file=@requests.jsonl`)}<div class="endpoint-heading"><span class="http-method post">POST</span><span class="endpoint-path">/v1/batches</span></div>${codeBlock('SHELL',`curl ${base}/v1/batches \\\n  -H "Authorization: Bearer $BATCHFORGE_API_KEY" \\\n  -H 'Content-Type: application/json' \\\n  -H 'Idempotency-Key: my-batch-001' \\\n  -d '{"input_file_id":"file_…","endpoint":"/v1/chat/completions","completion_window":"24h","metadata":{"name":"Catalog extraction"}}'`)}<p>Reuse the same <code>Idempotency-Key</code> and request body when retrying a submission after a network interruption. Reusing that key with a different body returns a conflict.</p></section><section class="docs-section" id="doc-requests"><h2>One request per line</h2><p>Every line is a JSON object with a unique <code>custom_id</code>. Set <code>body.model</code> to a model served by your workers. The model selector in the create dialog changes the generated sample; it does not rewrite an uploaded file.</p>${codeBlock('REQUESTS.JSONL',sample)}<div class="endpoint-heading"><span class="http-method">GET</span><span class="endpoint-path">/v1/models</span></div>${state.models.length?`<div class="models-list">${state.models.map(m=>`<span class="model-pill">${esc(m.id)}</span>`).join('')}</div>`:'<p>Connect your workspace to inspect configured model IDs.</p>'}</section><section class="docs-section" id="doc-lifecycle"><h2>Inspect progress and lifecycle</h2><div class="endpoint-heading"><span class="http-method">GET</span><span class="endpoint-path">/v1/batches/{batch_id}</span></div><p>Batch states are <code>in_progress</code>, <code>completed</code>, <code>failed</code>, and <code>cancelled</code>. Request counters distinguish pending, running, completed, failed, and cancelled tasks. A completed batch may contain failed tasks; inspect the request counters and error file.</p>${codeBlock('RESPONSE FIELDS',JSON.stringify({id:'batch_…',status:'in_progress',request_counts:{total:100,completed:60,failed:2,cancelled:0,running:8,pending:30},output_file_id:null,error_file_id:null},null,2))}<p>Unfinished tasks expire when the <code>24h</code> completion window ends. Actual completion depends on worker availability, model speed, queue depth, and configured limits.</p></section><section class="docs-section" id="doc-results"><h2>Download results and recover failures</h2><p>Terminal batches expose an <code>output_file_id</code> and/or <code>error_file_id</code>. Fetch each file through the authenticated content endpoint. Match responses to inputs using <code>custom_id</code>; execution order is not guaranteed.</p><div class="endpoint-heading"><span class="http-method">GET</span><span class="endpoint-path">/v1/files/{file_id}/content</span></div><div class="endpoint-heading"><span class="http-method post">POST</span><span class="endpoint-path">/v1/batches/{batch_id}/retry</span></div><p>Retry creates a new batch containing only failed tasks from a terminal batch. The original batch remains available for auditing. Cancellation stops new claims and fences outstanding attempts; results already accepted remain available.</p><div class="endpoint-heading"><span class="http-method post">POST</span><span class="endpoint-path">/v1/batches/{batch_id}/cancel</span></div></section><section class="docs-section" id="doc-workers"><h2>Workers and operations</h2><p>The API control plane requires PostgreSQL. GPU workers connect to a separately managed OpenAI-compatible inference engine. Worker registration and task execution use a dedicated worker token, separate from tenant API keys.</p>${codeBlock('WORKER ENVIRONMENT',`CONTROL_PLANE_URL=${base}\nBATCHFORGE_WORKER_TOKEN=<worker-only-secret>\n# See the repository deployment guide for worker and engine settings.`)}<p><code>GET /healthz</code> checks process liveness. <code>GET /readyz</code> checks database connectivity. <code>GET /metrics</code> exposes Prometheus metrics with worker-token authentication. Public batch data remains available only through authenticated tenant endpoints.</p></section></div></div>`
}

function render() {
  state.view = names[location.hash.slice(1)] ? location.hash.slice(1) : 'overview';
  $$('[data-view]').forEach(el => {
    el.classList.toggle('active', el.dataset.view === state.view);
    el.setAttribute('aria-current', el.dataset.view === state.view ? 'page' : 'false')
  });
  $('#breadcrumb-view').textContent = names[state.view];
  document.title = `${names[state.view]} · BatchForge`;
  const focus = document.activeElement;
  const focusedId = focus?.id;
  const selection = focus?.selectionStart;
  $('#main-content').innerHTML = ({
    overview: overviewView,
    batches: batchesView,
    workers: workersView,
    docs: docsView
  } [state.view])();
  if (state.view === 'overview') drawChart();
  if (focusedId && ['batch-search', 'batch-status-filter'].includes(focusedId)) {
    const input = $(`#${focusedId}`);
    if (input) {
      input.focus();
      if (typeof selection === 'number' && input.setSelectionRange) input.setSelectionRange(selection,
        selection)
    }
  }
  updateConnection()
}

function drawChart() {
  const el = $('#throughput-chart');
  if (!el) return;
  const buckets = state.overview?.throughput || [];
  const width = 650,
    height = 190,
    left = 35,
    right = 12,
    top = 12,
    bottom = 28,
    w = width - left - right,
    h = height - top - bottom;
  const peak = Math.max(1, ...buckets.map(b => Number(b.completed) || 0));
  const max = peak < 5 ? 5 : Math.ceil(peak / 5) * 5;
  let grid = '';
  for (let i = 0; i <= 4; i++) {
    const y = top + h * i / 4;
    grid +=
      `<line x1="${left}" x2="${width-right}" y1="${y}" y2="${y}" stroke="#23313d" stroke-dasharray="3 4"/><text x="${left-9}" y="${y+3}" text-anchor="end">${esc(shortNumber(max*(1-i/4)))}</text>`
  }
  const points = buckets.map((b, i) => [left + (buckets.length > 1 ? i / (buckets.length - 1) : .5) * w, top +
    h - (Number(b.completed) || 0) / max * h
  ]);
  const line = points.map((p, i) => `${i?'L':'M'}${p[0].toFixed(2)},${p[1].toFixed(2)}`).join(' ');
  const area = points.length ? `${line} L${points.at(-1)[0]},${top+h} L${points[0][0]},${top+h}Z` : '';
  const labels = buckets.length ? [0, Math.floor((buckets.length - 1) / 2), buckets.length - 1].filter((v, i,
    a) => a.indexOf(v) === i).map(i => {
    const d = dateValue(buckets[i].timestamp);
    return `<text x="${points[i][0]}" y="${height-6}" text-anchor="${i===0?'start':i===buckets.length-1?'end':'middle'}">${esc(d?d.toLocaleTimeString(undefined,{hour:'2-digit',minute:'2-digit'}):'')}</text>`
  }).join('') : '';
  el.innerHTML =
    `<svg class="throughput-chart" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="${buckets.length?'Actual completed tasks by reporting interval':'No throughput data available'}"><defs><linearGradient id="chart-fill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#74e4b2" stop-opacity=".22"/><stop offset="100%" stop-color="#74e4b2" stop-opacity="0"/></linearGradient></defs>${grid}${area?`<path d="${area}" fill="url(#chart-fill)"/><path d="${line}" fill="none" stroke="#74e4b2" stroke-width="2" vector-effect="non-scaling-stroke"/>`:''}${labels}</svg>${!buckets.length?`<div class="chart-empty"><p>${state.connected?'No completions recorded yet':'Your throughput, in real time.'}</p><span>${state.connected?'Completed tasks will populate this chart automatically.':'Connect a workspace to view actual task activity.'}</span></div>`:''}`
}
async function refresh({
  silent = false,
  includeBatches = false
} = {}) {
  if (!state.key || state.refreshing) return;
  state.refreshing = true;
  state.loading = !silent;
  $('#refresh-button').classList.add('spinning');
  if (!silent) render();
  try {
    const jobs = [api('/v1/overview')];
    if (includeBatches || state.view === 'batches') jobs.push(api('/v1/batches?limit=50'));
    const result = await Promise.allSettled(jobs);
    if (result[0].status === 'rejected') throw result[0].reason;
    state.overview = result[0].value;
    state.connected = true;
    state.updated = new Date();
    state.error = '';
    if (result[1]?.status === 'fulfilled') {
      const list = result[1].value;
      if (state.loadedMore) {
        const incoming = list.data || [];
        const ids = new Set(incoming.map(b => b.id));
        state.batches = [...incoming, ...state.batches.filter(b => !ids.has(b.id))]
      } else {
        state.batches = list.data || [];
        state.hasMore = !!list.has_more;
        state.lastId = list.last_id || state.batches.at(-1)?.id
      }
    } else if (result[1]?.status === 'rejected') {
      state.error = result[1].reason.message
    }
    try {
      const models = await api('/v1/models');
      state.models = models.data || []
    } catch {}
  } catch (error) {
    state.error = error.message;
    if (error.status === 401 || error.status === 403) {
      state.connected = false;
      state.error = 'Your API key was not accepted. Reconnect with a valid workspace key.'
    }
  } finally {
    state.loading = false;
    state.refreshing = false;
    $('#refresh-button').classList.remove('spinning');
    if (!$('#create-dialog').open && state.view !== 'docs') render();
    else updateConnection()
  }
}

function openConnect() {
  const dialog = $('#connect-dialog');
  $('#connect-error').hidden = true;
  $('#api-key').value = state.key;
  $('#api-key').type = 'password';
  $('#toggle-key').setAttribute('aria-label', 'Show API key');
  $('.auth-disconnect', dialog)?.remove();
  if (state.key) {
    const div = document.createElement('div');
    div.className = 'auth-disconnect';
    div.innerHTML =
      '<button class="text-button" type="button" data-action="disconnect">Disconnect this workspace</button>';
    $('#connect-form').append(div)
  }
  if (!dialog.open) dialog.showModal();
  setTimeout(() => $('#api-key').focus(), 30)
}
async function connect(event) {
  event.preventDefault();
  const key = $('#api-key').value.trim();
  if (!key) return;
  const button = $('#connect-submit');
  button.disabled = true;
  button.innerHTML = '<span class="spinner"></span>Connecting…';
  $('#connect-error').hidden = true;
  const oldKey = state.key;
  state.key = key;
  try {
    const overview = await api('/v1/overview');
    state.overview = overview;
    state.connected = true;
    state.error = '';
    state.updated = new Date();
    state.batches = [];
    state.loadedMore = false;
    state.models = [];
    storage.set(key);
    $('#connect-dialog').close();
    toast('Workspace connected. Live updates are enabled.');
    await refresh({
      silent: true,
      includeBatches: true
    });
    render()
  } catch (error) {
    state.key = oldKey;
    $('#connect-error').textContent = error.message;
    $('#connect-error').hidden = false
  } finally {
    button.disabled = false;
    button.innerHTML = `Connect workspace${icon('arrow')}`
  }
}

function disconnect() {
  storage.set('');
  Object.assign(state, {
    key: '',
    connected: false,
    overview: null,
    batches: [],
    models: [],
    error: '',
    updated: null,
    hasMore: false,
    loadedMore: false,
    detail: null,
    detailId: null
  });
  $('#connect-dialog').close();
  closeDrawer();
  render();
  toast('Workspace disconnected. The session key has been cleared.')
}

function setInputMode(mode) {
  state.inputMode = mode;
  $$('[data-input-mode]').forEach(b => {
    b.classList.toggle('active', b.dataset.inputMode === mode);
    b.setAttribute('aria-selected', String(b.dataset.inputMode === mode))
  });
  $('#upload-pane').hidden = mode !== 'upload';
  $('#paste-pane').hidden = mode !== 'paste';
  state.createAttempt = null;
  updateInputCount()
}

function updateInputCount() {
  const lines = $('#batch-jsonl').value.split(/\r?\n/).filter(s => s.trim()).length;
  $('#input-line-count').textContent = state.inputMode === 'paste' ?
    `${number(lines)} request${lines===1?'':'s'}` : state.uploadFile ?
    `${number(state.uploadFile.size)} bytes` : 'JSONL format'
}

function selectFile(file) {
  if (!file) return;
  state.uploadFile = file;
  state.createAttempt = null;
  $('#upload-label').textContent = file.name;
  $('#upload-description').textContent = `${number(file.size)} bytes · click to replace`;
  updateInputCount()
}
async function openCreate() {
  if (!state.connected) {
    openConnect();
    return
  }
  $('#create-error').hidden = true;
  $('#create-form').reset();
  state.uploadFile = null;
  state.createAttempt = null;
  $('#upload-label').textContent = 'Drop a JSONL file here';
  $('#upload-description').innerHTML = 'or <u>browse files</u> · one request per line';
  if (!state.models.length) {
    try {
      state.models = (await api('/v1/models')).data || []
    } catch (error) {
      toast(error.message, 'error')
    }
  }
  $('#batch-model').innerHTML = state.models.length ? state.models.map(m =>
      `<option value="${esc(m.id)}">${esc(m.id)}</option>`).join('') :
    '<option value="">No models registered</option>';
  setInputMode('upload');
  $('#create-dialog').showModal()
}

function loadSample() {
  const model = $('#batch-model').value;
  if (!model) {
    toast('Register a serving model before generating a sample.', 'error');
    return
  }
  const products = ['Acme Trail running shoes, forest green, size 42.',
    'Northstar insulated water bottle, 750 ml, midnight blue.',
    'Oak & Loom cotton T-shirt, terracotta, medium.'
  ];
  $('#batch-jsonl').value = products.map((text, i) => JSON.stringify({
    custom_id: `product-${String(i+1).padStart(3,'0')}`,
    method: 'POST',
    url: '/v1/chat/completions',
    body: {
      model,
      messages: [{
        role: 'system',
        content: 'Extract the product name, brand, color, and size or capacity. Return a JSON object only; use null for missing fields.'
      }, {
        role: 'user',
        content: text
      }],
      max_tokens: 128,
      temperature: 0
    }
  })).join('\n');
  setInputMode('paste');
  updateInputCount()
}

function validatePaste(text) {
  const lines = text.split(/\r?\n/).filter(line => line.trim());
  if (!lines.length) throw new Error('Paste at least one JSONL request, or upload a file.');
  const ids = new Set();
  for (let i = 0; i < lines.length; i++) {
    let row;
    try {
      row = JSON.parse(lines[i])
    } catch {
      throw new Error(`Line ${i+1}: invalid JSON. Each request must fit on a single line.`)
    }
    if (!row || typeof row !== 'object' || Array.isArray(row)) throw new Error(
      `Line ${i+1}: expected a JSON object.`);
    if (typeof row.custom_id !== 'string' || !row.custom_id.trim()) throw new Error(
      `Line ${i+1}: a nonempty custom_id is required.`);
    if (ids.has(row.custom_id)) throw new Error(`Line ${i+1}: custom_id “${row.custom_id}” is duplicated.`);
    ids.add(row.custom_id);
    if (row.method !== 'POST' || row.url !== '/v1/chat/completions' || !row.body?.model || !Array.isArray(row
        .body?.messages)) throw new Error(
      `Line ${i+1}: expected method POST, url /v1/chat/completions, and body with model and messages.`)
  }
  return `${lines.join('\n')}\n`
}
async function createBatch(event) {
  event.preventDefault();
  const submit = $('#create-submit');
  submit.disabled = true;
  $('#create-error').hidden = true;
  try {
    let attempt = state.createAttempt;
    if (!attempt) {
      let file;
      if (state.inputMode === 'paste') {
        const text = validatePaste($('#batch-jsonl').value);
        file = new File([text], 'requests.jsonl', {
          type: 'application/jsonl'
        })
      } else {
        file = state.uploadFile || $('#batch-file').files[0];
        if (!file) throw new Error('Choose a JSONL file or paste requests to create a batch.');
        if (!file.size) throw new Error('The selected file is empty.')
      }
      submit.innerHTML = '<span class="spinner"></span>Uploading requests…';
      const data = new FormData();
      data.append('file', file);
      data.append('purpose', 'batch');
      const uploaded = await api('/v1/files', {
        method: 'POST',
        body: data
      });
      const name = $('#batch-name').value.trim();
      attempt = {
        key: newIdempotencyKey(),
        payload: {
          input_file_id: uploaded.id,
          endpoint: '/v1/chat/completions',
          completion_window: '24h',
          metadata: name ? {
            name
          } : {}
        }
      };
      state.createAttempt = attempt
    }
    submit.innerHTML = '<span class="spinner"></span>Submitting batch…';
    const batch = await api('/v1/batches', {
      method: 'POST',
      headers: {
        'Idempotency-Key': attempt.key
      },
      body: JSON.stringify(attempt.payload)
    });
    $('#create-dialog').close();
    state.createAttempt = null;
    toast('Batch submitted. Tasks are durably queued.');
    location.hash = 'batches';
    await refresh({
      silent: true,
      includeBatches: true
    });
    await openBatch(batch.id)
  } catch (error) {
    $('#create-error').textContent = error.message;
    $('#create-error').hidden = false
  } finally {
    submit.disabled = false;
    submit.innerHTML = `${icon('plus')}Create batch`
  }
}
let previousDrawerFocus = null;
async function openBatch(id) {
  previousDrawerFocus = document.activeElement;
  state.detailId = id;
  state.detail = null;
  state.detailErrors = [];
  state.detailErrorsFile = null;
  state.detailLoading = true;
  $('#batch-drawer').hidden = false;
  $('#drawer-backdrop').hidden = false;
  document.body.style.overflow = 'hidden';
  renderDrawer();
  $('#batch-drawer').focus();
  await refreshDetail()
}

function closeDrawer() {
  state.detailId = null;
  state.detail = null;
  $('#batch-drawer').hidden = true;
  $('#drawer-backdrop').hidden = true;
  document.body.style.overflow = '';
  if (previousDrawerFocus?.isConnected) previousDrawerFocus.focus()
}
async function refreshDetail() {
  const id = state.detailId;
  if (!id || !state.key) return;
  try {
    const batch = await api(`/v1/batches/${encodeURIComponent(id)}`);
    if (state.detailId !== id) return;
    state.detail = batch;
    state.detailLoading = false;
    renderDrawer();
    if (batch.error_file_id && state.detailErrorsFile !== batch.error_file_id) {
      state.detailErrorsFile = batch.error_file_id;
      try {
        const response = await api(`/v1/files/${encodeURIComponent(batch.error_file_id)}/content`, {
          raw: true
        });
        const reader = response.body?.getReader();
        let text = '';
        if (reader) {
          const decoder = new TextDecoder();
          while (text.length < 65536) {
            const chunk = await reader.read();
            if (chunk.done) break;
            text += decoder.decode(chunk.value, {
              stream: true
            })
          }
          await reader.cancel()
        } else text = (await response.text()).slice(0, 65536);
        const lines = text.split(/\r?\n/).filter(Boolean);
        const errors = [];
        for (const line of lines.slice(0, 30)) {
          try {
            errors.push(JSON.parse(line))
          } catch {}
        }
        if (state.detailId === id) {
          state.detailErrors = errors;
          renderDrawer()
        }
      } catch (error) {
        if (state.detailId === id) toast(`Could not preview errors: ${error.message}`, 'error')
      }
    }
  } catch (error) {
    if (state.detailId !== id) return;
    state.detailLoading = false;
    if (!state.detail) $('#batch-drawer').innerHTML =
      `<div class="drawer-header"><h2>Batch details</h2><button class="icon-button" data-action="close-drawer" aria-label="Close details">${icon('close')}</button></div>${emptyState('alert','Unable to load batch',error.message)}<div style="text-align:center"><button class="button button-secondary" data-action="refresh-detail">Try again</button></div>`;
    else toast(error.message, 'error')
  }
}

function renderDrawer(){const drawer=$('#batch-drawer');const active=document.activeElement;const focusedAction=drawer.contains(active)?active?.dataset?.action:null;const focusedFile=drawer.contains(active)?active?.dataset?.downloadFile:null;const scrollTop=drawer.scrollTop;const b=state.detail;if(!b){$('#batch-drawer').innerHTML=`<div class="drawer-header"><h2>Batch details</h2><button class="icon-button" data-action="close-drawer" aria-label="Close details">${icon('close')}</button></div><div class="empty-state"><span class="spinner"></span><p style="margin-top:15px">Loading batch…</p></div>`;return}const c=counts(b);const errors=state.detailErrors.length?state.detailErrors:(Array.isArray(b.errors?.data)?b.errors.data:Array.isArray(b.errors)?b.errors:[]);const detailItems=[['Created',dateText(b.created_at)],['Started',dateText(b.in_progress_at)],['Finished',dateText(b.completed_at||b.failed_at||b.cancelled_at)],['Endpoint',b.endpoint],['Completion window',b.completion_window],['Input file',b.input_file_id]];$('#batch-drawer').innerHTML=`<div class="drawer-header"><div><span class="eyebrow">BATCH DETAILS</span><h2>${esc(batchName(b))}</h2><span class="batch-id">${esc(b.id)}</span></div><button class="icon-button" data-action="close-drawer" aria-label="Close details">${icon('close')}</button></div><div class="drawer-body"><div class="drawer-status-row">${statusBadge(b.status)}<button class="link-button" data-action="refresh-detail">${icon('refresh')}Refresh</button></div><div class="drawer-progress"><div class="progress-meta"><span>${number(completedCount(b))} of ${number(c.total)} tasks resolved</span><span>${progress(b)}%</span></div><div class="progress-track"><div class="progress-fill ${b.status==='in_progress'?'running':''}" style="width:${progress(b)}%"></div></div></div><section class="drawer-section"><dl class="detail-grid">${[['Completed',c.completed],['Failed',c.failed],['Running',c.running],['Pending',c.pending],['Cancelled',c.cancelled],['Total requests',c.total]].map(([label,value])=>`<div class="detail-item"><dt>${label}</dt><dd class="${label==='Completed'?'text-green':label==='Failed'&&value?'text-red':''}">${number(value)}</dd></div>`).join('')}</dl></section><section class="drawer-section"><h3>Results & recovery</h3><div class="drawer-actions">${b.output_file_id?`<button class="button button-secondary" data-download-file="${esc(b.output_file_id)}" data-filename="${esc(b.id)}-output.jsonl">${icon('download')}Download output</button>`:''}${b.error_file_id?`<button class="button button-secondary" data-download-file="${esc(b.error_file_id)}" data-filename="${esc(b.id)}-errors.jsonl">${icon('download')}Download errors</button>`:''}${terminal(b)&&c.failed?`<button class="button button-secondary" data-action="retry-batch">${icon('refresh')}Retry ${number(c.failed)} failed</button>`:''}${!terminal(b)?`<button class="button button-danger" data-action="cancel-batch">${icon('stop')}Cancel batch</button>`:''}</div><p class="drawer-note">${terminal(b)?'Retry creates a new batch from failed tasks only. Existing results are retained.':'Result files become available when this batch reaches a terminal state. Accepted results are retained if you cancel.'}</p></section>${errors.length?`<section class="drawer-section"><h3>Task errors <span class="muted">· preview, up to 30</span></h3><div class="task-errors">${errors.map(e=>`<div class="task-error"><strong>${esc(e.custom_id||e.code||'Task error')}</strong><p>${esc(e.error?.message||e.message||e.error?.code||'Unknown error')}</p></div>`).join('')}</div></section>`:''}<section class="drawer-section"><h3>Execution details</h3><dl class="detail-list">${detailItems.map(([label,value])=>`<div><dt>${label}</dt><dd>${esc(value)}</dd></div>`).join('')}</dl></section>${b.usage?`<section class="drawer-section"><h3>Token usage</h3><dl class="detail-list"><div><dt>Input tokens</dt><dd>${number(b.usage.prompt_tokens)}</dd></div><div><dt>Output tokens</dt><dd>${number(b.usage.completion_tokens)}</dd></div></dl></section>`:'<p class="drawer-note">Token usage is reported in workspace totals from accepted worker responses. Per-batch token breakdown is not exposed by this API version.</p>'}${Object.keys(b.metadata||{}).length?`<section class="drawer-section"><h3>Metadata</h3>${codeBlock('JSON',JSON.stringify(b.metadata,null,2))}</section>`:''}</div>`;drawer.scrollTop=scrollTop;if(focusedAction)drawer.querySelector(`[data-action="${focusedAction}"]`)?.focus({preventScroll:true});else if(focusedFile)$$('[data-download-file]',drawer).find(el=>el.dataset.downloadFile===focusedFile)?.focus({preventScroll:true})}
async function downloadFile(button) {
  const fileId = button.dataset.downloadFile;
  button.disabled = true;
  const old = button.innerHTML;
  button.innerHTML = '<span class="spinner"></span>Downloading…';
  try {
    const response = await api(`/v1/files/${encodeURIComponent(fileId)}/content`, {
      raw: true
    });
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = button.dataset.filename || `${fileId}.jsonl`;
    document.body.append(anchor);
    anchor.click();
    anchor.remove();
    setTimeout(() => URL.revokeObjectURL(url), 30000);
    toast('Result file downloaded.')
  } catch (error) {
    toast(error.message, 'error')
  } finally {
    button.disabled = false;
    button.innerHTML = old
  }
}
async function retryBatch(button) {
  const id = state.detailId;
  if (!id || button.disabled) return;
  button.disabled = true;
  if (!retryAttempts.has(id)) {
    retryAttempts.set(id, newIdempotencyKey());
    saveRetryAttempts();
  }
  try {
    const batch = await api(`/v1/batches/${encodeURIComponent(id)}/retry`, {
      method: 'POST',
      headers: {'Idempotency-Key': retryAttempts.get(id)}
    });
    if (!batch?.id) throw new APIError('The retry response did not include a batch ID. Retry safely with the same attempt.', 502);
    retryAttempts.delete(id);
    saveRetryAttempts();
    toast('A new batch was created for failed tasks.');
    await refresh({
      silent: true,
      includeBatches: true
    });
    await openBatch(batch.id)
  } catch (error) {
    toast(error.message, 'error');
  } finally {
    button.disabled = false
  }
}
async function cancelBatch(event) {
  event.preventDefault();
  const id = state.detailId;
  if (!id) return;
  const button = $('#confirm-submit');
  button.disabled = true;
  try {
    const batch = await api(`/v1/batches/${encodeURIComponent(id)}/cancel`, {
      method: 'POST'
    });
    $('#confirm-dialog').close();
    if (state.detailId === id) {
      state.detail = batch;
      renderDrawer();
    }
    const messages = {
      cancelled: 'Batch cancelled. Accepted results were preserved.',
      completed: 'Batch already completed before cancellation. Results are available.',
      failed: 'Batch already failed before cancellation. Inspect its task errors.',
      expired: 'Batch already expired before cancellation. Accepted results remain available.'
    };
    toast(messages[batch.status] || `Current batch status: ${(batch.status || 'unknown').replaceAll('_', ' ')}.`);
    await refreshDetail();
    await refresh({
      silent: true,
      includeBatches: true
    })
  } catch (error) {
    toast(error.message, 'error')
  } finally {
    button.disabled = false
  }
}
async function loadMore(button) {
  button.disabled = true;
  button.textContent = 'Loading…';
  try {
    const list = await api(`/v1/batches?limit=50&after=${encodeURIComponent(state.lastId)}`);
    const known = new Set(state.batches.map(b => b.id));
    state.batches.push(...(list.data || []).filter(b => !known.has(b.id)));
    state.loadedMore = true;
    state.hasMore = !!list.has_more;
    state.lastId = list.last_id || state.batches.at(-1)?.id;
    render()
  } catch (error) {
    toast(error.message, 'error');
    button.disabled = false;
    button.textContent = 'Load more batches'
  }
}
document.addEventListener('click', async event => {
  const close = event.target.closest('[data-close-dialog]');
  if (close) {
    $(`#${close.dataset.closeDialog}`).close();
    return
  }
  const docAnchor = event.target.closest('[data-doc-anchor]');
  if (docAnchor) {
    event.preventDefault();
    $(`#doc-${docAnchor.dataset.docAnchor}`)?.scrollIntoView({
      behavior: 'smooth',
      block: 'start'
    });
    return
  }
  const copy = event.target.closest('[data-copy-code]');
  if (copy) {
    try {
      await navigator.clipboard.writeText(copy.closest('.code-block').querySelector('code')
      .textContent);
      toast('Copied to clipboard.')
    } catch {
      toast('Clipboard unavailable. Select and copy the code manually.', 'error')
    }
    return
  }
  const download = event.target.closest('[data-download-file]');
  if (download) {
    await downloadFile(download);
    return
  }
  const action = event.target.closest('[data-action]');
  if (action) {
    switch (action.dataset.action) {
      case 'create':
        await openCreate();
        break;
      case 'connect':
        openConnect();
        break;
      case 'disconnect':
        disconnect();
        break;
      case 'refresh':
        await refresh({
          includeBatches: true
        });
        break;
      case 'close-drawer':
        closeDrawer();
        break;
      case 'refresh-detail':
        await refreshDetail();
        break;
      case 'retry-batch':
        await retryBatch(action);
        break;
      case 'cancel-batch':
        $('#confirm-dialog').showModal();
        break
    }
    return
  }
  const row = event.target.closest('[data-batch-id]');
  if (row) {
    await openBatch(row.dataset.batchId);
    return
  }
  const tab = event.target.closest('[data-input-mode]');
  if (tab) setInputMode(tab.dataset.inputMode);
  if (event.target.closest('#load-more')) await loadMore(event.target.closest('#load-more'))
});
$('#connect-form').addEventListener('submit', connect);
$('#create-form').addEventListener('submit', createBatch);
$('#confirm-form').addEventListener('submit', cancelBatch);
$('#account-button').addEventListener('click', openConnect);
$('#refresh-button').addEventListener('click', () => state.key ? refresh({
  includeBatches: true
}) : openConnect());
$('#toggle-key').addEventListener('click', () => {
  const field = $('#api-key');
  field.type = field.type === 'password' ? 'text' : 'password';
  $('#toggle-key').setAttribute('aria-label', field.type === 'password' ? 'Show API key' : 'Hide API key')
});
$('#batch-file').addEventListener('change', event => selectFile(event.target.files[0]));
$('#batch-jsonl').addEventListener('input', () => {
  state.createAttempt = null;
  updateInputCount()
});
$('#batch-name').addEventListener('input', () => {
  state.createAttempt = null
});
$('#load-sample').addEventListener('click', loadSample);
$('#drawer-backdrop').addEventListener('click', closeDrawer);
$('#mobile-menu').addEventListener('click', () => $('.sidebar').classList.toggle('mobile-open'));
$$('.nav-item').forEach(a => a.addEventListener('click', () => $('.sidebar').classList.remove(
'mobile-open')));
const dropzone = $('#dropzone');
['dragenter', 'dragover'].forEach(name => dropzone.addEventListener(name, event => {
  event.preventDefault();
  dropzone.classList.add('dragover')
}));
['dragleave', 'drop'].forEach(name => dropzone.addEventListener(name, event => {
  event.preventDefault();
  dropzone.classList.remove('dragover');
  if (name === 'drop') selectFile(event.dataTransfer.files[0])
}));
document.addEventListener('input', event => {
  if (event.target.id === 'batch-search') {
    state.search = event.target.value;
    $('#batch-list-panel').innerHTML = batchTable(filteredBatches());
    $('#batch-filter-count').textContent = `${number(filteredBatches().length)} matching batches`
  }
});
document.addEventListener('change', event => {
  if (event.target.id === 'batch-status-filter') {
    state.filter = event.target.value;
    $('#batch-list-panel').innerHTML = batchTable(filteredBatches());
    $('#batch-filter-count').textContent = `${number(filteredBatches().length)} matching batches`
  }
});
document.addEventListener('keydown', event => {
  if (event.key === 'Escape' && !$('#confirm-dialog').open) {
    if (state.detailId) closeDrawer();
    $('.sidebar').classList.remove('mobile-open')
  }
  if (event.key === 'Enter' && event.target.matches('[data-batch-id]')) openBatch(event.target.dataset
    .batchId);
  if (event.key.toLowerCase() === 'n' && !event.metaKey && !event.ctrlKey && !event.altKey && !['INPUT',
      'TEXTAREA', 'SELECT'
    ].includes(event.target.tagName) && !$$('dialog').some(d => d.open)) {
    event.preventDefault();
    openCreate()
  }
  if (event.key === 'Tab' && state.detailId && !$$('dialog').some(d => d.open)) {
    const focusable = $$('button,a[href],input,textarea,select,[tabindex="0"]', $('#batch-drawer'))
      .filter(el => !el.disabled);
    if (!focusable.length) return;
    const first = focusable[0],
      last = focusable.at(-1);
    if (event.shiftKey && (document.activeElement === first || document.activeElement === $(
        '#batch-drawer'))) {
      event.preventDefault();
      last.focus()
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus()
    }
  }
});
window.addEventListener('hashchange', () => {
  render();
  if (state.view === 'batches' && state.key) refresh({
    silent: true,
    includeBatches: true
  })
});
document.addEventListener('visibilitychange', () => {
  if (!document.hidden && state.key) {
    refresh({
      silent: true
    });
    if (state.detailId) refreshDetail()
  }
});
setInterval(() => {
  if (!document.hidden && state.key) {
    refresh({
      silent: true
    });
    if (state.detailId) refreshDetail()
  }
}, 8000);
hydrateIcons();
render();
if (state.key) refresh({
  includeBatches: true
});
