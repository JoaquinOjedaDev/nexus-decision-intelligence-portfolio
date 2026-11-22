const $ = (selector, root = document) => root.querySelector(selector);
const escape = (value) => String(value ?? '').replace(/[&<>"']/g, char => ({
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;'
} [char]));
const numeric = (value) => Number.isFinite(Number(value)) ? Number(value) : 0;
const number = (value, digits = 0) => new Intl.NumberFormat('en-US', {
  maximumFractionDigits: digits
}).format(numeric(value));
const money = (value) => new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0
}).format(numeric(value));
const percent = (value) => `${number(numeric(value) * 100, 1)}%`;
const readable = value => String(value ?? '').replaceAll('_', ' ').replace(/\b\w/g, c => c.toUpperCase());
const dateLabel = value => {
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? 'Not recorded' : date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC'
  });
};
const timeLabel = value => {
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? 'Not recorded' : date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    timeZone: 'UTC'
  }) + ' UTC';
};
const iconPaths = {
  overview: '<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>',
  decisions: '<path d="M4 7h16M4 17h16M8 3v8m8 2v8"/><circle cx="8" cy="7" r="3"/><circle cx="16" cy="17" r="3"/>',
  approvals: '<path d="M9 3h6v4H9zM8 5H5v16h14V5h-3M8 13l3 3 5-6"/>',
  health: '<path d="M3 12h4l3-7 4 14 3-7h4"/>',
  copilot: '<path d="M5 4h14v13H9l-4 4V4Z"/><path d="M9 8h6M9 12h4"/>',
  arrow: '<path d="M4 12h16m-6-6 6 6-6 6"/>',
  back: '<path d="M20 12H4m6-6-6 6 6 6"/>',
  box: '<path d="m12 3 9 5v9l-9 5-9-5V8l9-5Z M3 8l9 5 9-5M12 13v9M7 5l10 6"/>',
  shield: '<path d="m12 3 8 3v6c0 4-4 7-8 9-4-2-8-5-8-9V6l8-3Z M8 12l3 3 5-6"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  refresh: '<path d="M20 8a9 9 0 0 0-15-3L3 8m0-5v5h5M4 16a9 9 0 0 0 15 3l2-3m0 5v-5h-5"/>',
  currency: '<path d="M12 2v20m5-16H9a4 4 0 0 0 0 8h6a4 4 0 0 1 0 8H6"/>',
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7h.01"/>',
  download: '<path d="M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5"/>',
};
const icon = name =>
  `<svg class="icon" viewBox="0 0 24 24" aria-hidden="true">${iconPaths[name] || iconPaths.info}</svg>`;
const stateLabels = {
  pending: 'Awaiting review',
  approved: 'Approved',
  executed: 'Executed · mock',
  outcome_observed: 'Outcome recorded',
  rejected: 'Rejected',
  revoked: 'Revoked',
  invalidated: 'Invalidated · new plan required',
  execution_failed: 'Execution failed'
};
const stateClass = state => ({
  pending: 'warning',
  approved: 'success',
  executed: 'success',
  outcome_observed: 'success',
  rejected: 'neutral',
  revoked: 'neutral',
  invalidated: 'warning',
  execution_failed: 'error'
} [state] || 'neutral');
const badge = (label, style = 'neutral') => `<span class="badge ${escape(style)}">${escape(label)}</span>`;
const stateBadge = item => badge(reviewExpired(item) && item.state === 'pending' ? 'Review expired' : (stateLabels[
  item.state] || readable(item.state)), reviewExpired(item) && item.state === 'pending' ? 'error' : stateClass(
  item.state));
const navItems = [
  ['overview', 'Overview'],
  ['decisions', 'Decisions'],
  ['approvals', 'Approval queue'],
  ['health', 'System & audit'],
  ['copilot', 'Evidence assistant']
];
const state = {
  session: null,
  overview: null,
  recommendations: [],
  selected: null,
  filter: 'all',
  health: null,
  learning: null,
  audit: null,
  routeVersion: 0,
  action: null,
  noticeTimer: null,
  chartId: null,
  answer: null,
  localAnswer: null,
  serverTime: null,
  serverTimeReadAt: 0,
  expiryTimer: null,
  expirySignature: ''
};

function serverNow() {
  return state.serverTime === null ? Date.now() :
    state.serverTime + performance.now() - state.serverTimeReadAt;
}
function reviewExpired(item) {
  const deadline = Date.parse(item.valid_until);
  return Boolean(item.expired) || (Number.isFinite(deadline) && deadline <= serverNow());
}
function actionableReview(item) {
  return item.state === 'pending' && !reviewExpired(item);
}
function executionExpired(item) {
  const deadline = Date.parse(item.approval_expires_at);
  return item.approval_renewable === true || reviewExpired(item) ||
    (Number.isFinite(deadline) && deadline <= serverNow());
}
function canRenewApproval(item) {
  return role() === 'approver' && item.approval_renewable === true &&
    ['approved', 'execution_failed'].includes(item.state) && !reviewExpired(item);
}
function reviewCounts() {
  return {
    pending: state.recommendations.filter(actionableReview).length,
    expired: state.recommendations.filter(item => item.state === 'pending' && reviewExpired(item)).length
  };
}
function reviewCountNote() {
  const expired = reviewCounts().expired;
  return `${number(expired)} expired ${expired === 1 ? 'review' : 'reviews'} · retained for evidence only`;
}
function queueItems() {
  return state.recommendations.filter(item => state.filter === 'all' ||
    (state.filter === 'pending' ? actionableReview(item) :
      state.filter === 'expired' ? item.state === 'pending' && reviewExpired(item) :
        item.state === state.filter));
}
function reviewLinkLabel(item) {
  if (reviewExpired(item) && item.state === 'pending') return 'View expired evidence';
  if (['approved', 'execution_failed'].includes(item.state) && executionExpired(item))
    return 'View expired approval';
  return item.state === 'pending' ? 'Review decision' :
    item.state === 'approved' ? 'Review & execute' :
      item.state === 'executed' ? 'Review outcome' : 'View evidence';
}
function blockedAction(action, item) {
  if (['approve', 'reject'].includes(action) && !actionableReview(item))
    return 'This recommendation is no longer open for review. Your note has been kept.';
  if (action === 'renew' && !canRenewApproval(item))
    return 'This approval is no longer eligible for renewal. Your note has been kept.';
  if (action === 'execute' && executionExpired(item))
    return 'This approval has expired. Your form has been kept, but execution is no longer authorized.';
  return '';
}
function scheduleExpiryCheck() {
  clearTimeout(state.expiryTimer);
  const items = [...state.recommendations, state.selected, state.action?.item].filter(Boolean);
  const now = serverNow();
  const deadlines = items.flatMap(item => [
    item.state === 'pending' || ['approved', 'execution_failed'].includes(item.state) ?
      Date.parse(item.valid_until) : NaN,
    ['approved', 'execution_failed'].includes(item.state) ? Date.parse(item.approval_expires_at) : NaN
  ]).filter(value => Number.isFinite(value) && value > now);
  if (deadlines.length) state.expiryTimer = setTimeout(() => {
    updateExpiryUI();
    scheduleExpiryCheck();
  }, Math.min(2147483647, Math.max(1, Math.min(...deadlines) - now + 5)));
}
function updateExpiryUI() {
  if (!state.overview) return;
  const counts = reviewCounts();
  const navigation = $('#navigation a[href="#approvals"]');
  if (navigation) {
    let count = $('.nav-count', navigation);
    if (counts.pending) {
      if (!count) {
        count = document.createElement('span');
        count.className = 'nav-count';
        navigation.append(count);
      }
      count.textContent = number(counts.pending);
    } else count?.remove();
  }
  const items = [state.selected, ...state.recommendations].filter(Boolean);
  const signature = items.map(item => `${item.id}:${reviewExpired(item)}:${executionExpired(item)}`).join('|');
  if (signature !== state.expirySignature) {
    state.expirySignature = signature;
    const metric = $('[data-review-kpi]');
    if (metric) {
      $('.kpi-value', metric).textContent = number(counts.pending);
      $('.kpi-note', metric).textContent = reviewCountNote();
    }
    const summary = $('[data-review-counts]');
    if (summary) summary.textContent =
      `${number(counts.pending)} awaiting review · ${number(counts.expired)} expired reviews. Expired reviews retain evidence but cannot be approved.`;
    const priority = $('[data-next-decision]');
    if (priority) priority.innerHTML = renderNextDecision();
    if (location.hash.split('/')[0] === '#approvals') {
      const list = $('.approval-list');
      if (list) list.innerHTML = renderApprovalCards(queueItems());
    }
    document.querySelectorAll('[data-review-state]').forEach(target => {
      const item = items.find(value => value.id === target.dataset.reviewState);
      if (item) target.innerHTML = stateBadge(item);
    });
    document.querySelectorAll('[data-review-link]').forEach(target => {
      const item = items.find(value => value.id === target.dataset.reviewLink);
      if (item) target.innerHTML = `${escape(reviewLinkLabel(item))} ${icon('arrow')}`;
    });
    document.querySelectorAll('[data-review-actions]').forEach(target => {
      const item = items.find(value => value.id === target.dataset.reviewActions);
      if (item) target.innerHTML = actionButtons(item);
    });
  }
  if ($('#action-dialog').open && state.action) {
    const reason = blockedAction(state.action.action, state.action.item);
    if (reason) {
      $('#confirm-action').disabled = true;
      $('#dialog-error').textContent = reason;
      $('#dialog-error').hidden = false;
    }
  }
}

async function request(path, {
  method = 'GET',
  body,
  headers = {}
} = {}) {
  const options = {
    method,
    credentials: 'same-origin',
    headers: {
      ...headers
    }
  };
  if (body !== undefined) {
    options.headers['Content-Type'] = 'application/json';
    options.body = JSON.stringify(body);
  }
  if (method !== 'GET' && state.session?.csrf_token) options.headers['X-CSRF-Token'] = state.session
    .csrf_token;
  const response = await fetch(path, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(payload.error?.message || payload.detail ||
      `Request failed (${response.status}). Please retry.`);
    error.code = payload.error?.code;
    throw error;
  }
  return payload;
}

function notice(message, error = false) {
  const target = $('#notice');
  target.textContent = message;
  target.className = `notice${error ? ' error' : ''}`;
  target.hidden = false;
  clearTimeout(state.noticeTimer);
  state.noticeTimer = setTimeout(() => {
    target.hidden = true;
  }, 6500);
}

function role() {
  return state.session?.user?.role || 'planner';
}

function nav(active) {
  $('#navigation').innerHTML = navItems.map(([id, label]) =>
    `<a class="nav-link" href="#${id}"${id === active ? ' aria-current="page"' : ''}>${icon(id)}<span>${label}</span>${id === 'approvals' && reviewCounts().pending ? `<span class="nav-count">${number(reviewCounts().pending)}</span>` : ''}</a>`
    ).join('');
  $('#current-page').textContent = navItems.find(([id]) => id === active)?.[1] || 'Decisions';
}

function lifecycle() {
  return `<section class="lifecycle" aria-label="Six-stage data lifecycle">${[['Sources',state.overview?.metadata?.pipeline_mode === 'connected_runtime_baseline' ? 'Connected validated source' : 'CSV + JSONL fixtures'],['Data foundation','Contracts + dbt validation'],['Intelligence',state.overview?.metadata?.pipeline_mode === 'connected_runtime_baseline' ? 'Fresh seasonal baseline' : 'Empirical forecast bands'],['Decisioning',state.overview?.metadata?.pipeline_mode === 'connected_runtime_baseline' ? 'Runtime decision policy' : 'Constrained optimization'],['Human-approved action','Role checks + mock ERP'],['Feedback','Observed simulation']].map(([title,sub],index) => `<div class="lifecycle-step"><div class="step-top"><span class="step-number">0${index+1}</span><strong>${title}</strong></div><small>${sub}</small></div>`).join('')}</section>`;
}

function heading(eyebrow, title, subtitle, action = '') {
  return `<div class="page-heading"><div><span class="eyebrow">${escape(eyebrow)}</span><h1>${escape(title)}</h1><p>${escape(subtitle)}</p></div>${action}</div>`;
}

function metadataNote() {
  return `<div class="evidence-note">${icon('info')}<span>${escape(state.overview?.metadata?.data_scope_note || 'Synthetic fixture data with separately preserved source and model evidence.')} <a href="#copilot">Inspect the evidence</a>.</span></div>`;
}

function empty(title, message, action = '<a class="button secondary" href="#overview">Back to overview</a>') {
  return `<div class="empty-state"><h2>${escape(title)}</h2><p>${escape(message)}</p>${action}</div>`;
}

function kpi(label, value, note, glyph, highlight = false, attributes = '') {
  return `<div class="kpi" ${attributes}><div class="kpi-top"><span>${escape(label)}</span>${icon(glyph)}</div><strong class="kpi-value">${escape(value)}</strong><span class="kpi-note${highlight ? ' highlight' : ''}">${escape(note)}</span></div>`;
}

function chart(item, includeHistory = true) {
  const points = (item?.forecast_points || []).filter(p => ['q10', 'q50', 'q90'].every(key => Number.isFinite(
    Number(p[key]))));
  const history = includeHistory ? (item?.history || []).slice(-14).filter(p => Number.isFinite(Number(p
    .actual))) : [];
  if (!points.length) return empty('Forecast unavailable',
    'Select another recommendation to inspect its forecast.', '');
  const all = [...history, ...points];
  const width = Math.min(620, Math.max(300, window.innerWidth - 80)),
    height = 240,
    left = 39,
    right = 20,
    top = 24,
    bottom = 33;
  const max = Math.max(1, ...points.map(p => numeric(p.q90)), ...history.map(p => numeric(p.actual)));
  const yMax = Math.ceil(max * 1.15 / 5) * 5;
  const x = index => left + index * (width - left - right) / Math.max(1, all.length - 1);
  const y = value => height - bottom - numeric(value) * (height - top - bottom) / yMax;
  const path = (rows, key, start) => rows.map((p, i) =>
    `${i ? 'L' : 'M'}${x(start+i).toFixed(2)},${y(p[key]).toFixed(2)}`).join(' ');
  const upper = points.map((p, i) => `${x(history.length+i)},${y(p.q90)}`).join(' ');
  const lower = [...points].reverse().map((p, i) => `${x(history.length+points.length-1-i)},${y(p.q10)}`)
    .join(' ');
  const ticks = [0, .25, .5, .75, 1].map(ratio => {
    const value = yMax * ratio;
    return `<line class="chart-grid" x1="${left}" y1="${y(value)}" x2="${width-right}" y2="${y(value)}"/><text x="${left-10}" y="${y(value)+4}" text-anchor="end">${number(value)}</text>`;
  }).join('');
  const dateIndices = [...new Set([0, Math.floor((all.length - 1) / 3), Math.floor(2 * (all.length - 1) / 3),
    all.length - 1
  ])];
  const dates = dateIndices.map(i =>
    `<text x="${x(i)}" y="${height-8}" text-anchor="${i === 0 ? 'start' : i === all.length-1 ? 'end' : 'middle'}">${escape(new Date(all[i].date).toLocaleDateString('en-US',{month:'short',day:'numeric',timeZone:'UTC'}))}</text>`
    ).join('');
  const divider = history.length ?
    `<line class="chart-dash" x1="${x(history.length)-5}" y1="${top}" x2="${x(history.length)-5}" y2="${height-bottom}"/><text x="${x(history.length)+3}" y="15">Forecast</text>` :
    '';
  return `<svg class="chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Daily unit demand for ${escape(item.sku_id)} at ${escape(item.location_id)}. Observed history and empirical P10, P50, and P90 forecast over ${points.length} days.">${ticks}<polygon class="chart-band" points="${upper} ${lower}"/>${history.length ? `<path d="${path(history,'actual',0)}" fill="none" stroke="#526d7c" stroke-width="2"/>` : ''}<path class="chart-line" d="${path(points,'q50',history.length)}"/><path class="chart-dash" d="${path(points,'q90',history.length)}"/><path class="chart-dash" d="${path(points,'q10',history.length)}"/>${divider}${dates}<text x="${left}" y="13">Units / day</text></svg><div class="chart-legend">${history.length ? '<span class="legend-item">— Observed history</span>' : ''}<span class="legend-item"><span class="legend-line"></span>Median forecast</span><span class="legend-item"><span class="legend-band"></span>P10–P90 interval</span></div><details><summary>View forecast data and interpretation</summary><p class="field-help">${escape(item?.generation?.metadata?.forecast_interval_note || state.overview?.metadata?.forecast_interval_note)}</p><div class="table-wrap"><table><thead><tr><th>Date</th><th>P10</th><th>P50</th><th>P90</th></tr></thead><tbody>${points.map(p => `<tr><td>${escape(p.date)}</td><td>${number(p.q10)}</td><td>${number(p.q50)}</td><td>${number(p.q90)}</td></tr>`).join('')}</tbody></table></div></details>`;
}

function recommendationTable(items) {
  return `<div class="table-wrap responsive-table"><table><thead><tr><th>Product / location</th><th>Uncertainty</th><th>Order</th><th>Estimate</th><th>Status</th><th><span class="sr-only">Review</span></th></tr></thead><tbody>${items.map(item => `<tr><td><span class="cell-title">${escape(item.sku_id)}</span><span class="cell-subtitle">${escape(item.location_id)} · ${escape(item.supplier_id)}</span></td><td data-label="Uncertainty">${badge(`${readable(item.risk)} spread`,item.risk)}</td><td class="numeric" data-label="Order">${number(item.recommended_quantity)} units</td><td class="numeric" data-label="Estimate">${money(item.expected_cost)}</td><td data-label="Status"><span data-review-state="${escape(item.id)}">${stateBadge(item)}</span></td><td><a class="table-action" data-review-link="${escape(item.id)}" href="#decisions/${encodeURIComponent(item.id)}" aria-label="Inspect ${escape(item.sku_id)} at ${escape(item.location_id)}">${escape(reviewLinkLabel(item))} ${icon('arrow')}</a></td></tr>`).join('')}</tbody></table></div>`;
}

function renderNextDecision() {
  const priority = [...state.recommendations].sort((a, b) =>
    numeric(b.risk === 'high') - numeric(a.risk === 'high') || b.expected_cost - a.expected_cost
  ).find(actionableReview);
  return `<div class="panel-header"><div><h2>Next decision</h2><p>Inspect context before you approve</p></div>${priority ? badge(`${readable(priority.risk)} spread`,priority.risk) : ''}</div>${priority ? `<div class="priority-feature"><div class="large-quantity">${number(priority.recommended_quantity)}<span>units</span></div><p class="product-label">${escape(priority.sku_id)} / ${escape(priority.location_id)}</p><p class="reason">${escape(priority.reason)}</p><dl class="mini-facts"><div><dt>Order estimate</dt><dd>${money(priority.expected_cost)}</dd></div><div><dt>Regular lead time</dt><dd>${number(priority.lead_time_days)} days</dd></div></dl><a class="button secondary full-width" href="#decisions/${encodeURIComponent(priority.id)}">Explore recommendation ${icon('arrow')}</a></div>` : empty('No reviews awaiting action', reviewCounts().expired ? 'Expired reviews remain available as evidence. Generate a fresh plan to open a new review window.' : 'Current recommendations have already been reviewed.', '<a class="button secondary" href="#approvals">View decision history</a>')}`;
}

function renderOverview() {
  const overview = state.overview,
    items = overview.current_recommendations || state.recommendations,
    metrics = overview.metrics || {},
    summary = overview.summary || {};
  const selected = items.find(item => item.id === state.chartId) || items[0];
  return heading('OPERATIONS / CONTROL TOWER', 'Inventory, in focus.',
      'A clear path from forecast uncertainty to a decision you can stand behind.',
      `<a class="button primary" href="#approvals">Review decisions ${icon('arrow')}</a>`) + lifecycle() +
    `<section class="kpi-grid" aria-label="Decision metrics">${kpi('Awaiting review',number(reviewCounts().pending),reviewCountNote(),'approvals',false,'data-review-kpi')}${kpi('Planned order value',money(summary.current_order_value ?? metrics.order_value),`${percent(metrics.budget_utilization)} of ${money(metrics.budget)} budget`,'currency')}${kpi('Recommended units',number(items.reduce((sum,item) => sum+numeric(item.recommended_quantity),0)),`${number(items.length)} current decisions · ${number(summary.historical)} retained in history`,'box')}${kpi('Outcomes recorded',number(summary.outcome_observed),`${number(summary.effect_count)} mock order effects · all history`,'refresh',true)}</section><div class="overview-grid"><section class="panel"><div class="panel-header"><div><h2>Demand & uncertainty</h2><p>Historical observations → ${number(overview.metadata?.horizon_days)}-day forecast</p></div><label class="sr-only" for="chart-product">Forecast product</label><select class="chart-select" id="chart-product">${items.map(item => `<option value="${escape(item.id)}"${item.id === selected?.id ? ' selected' : ''}>${escape(item.sku_id)} / ${escape(item.location_id)}</option>`).join('')}</select></div><div class="panel-body">${chart(selected)}</div><div class="panel-footer"><p>${overview.metadata?.pipeline_mode === 'connected_runtime_baseline' ? 'Fresh seasonal baseline' : 'Frozen champion'} · ${escape(dateLabel(overview.metadata?.scientific_decision_time))}</p>${badge('Empirical intervals','success')}</div></section><section class="panel" data-next-decision>${renderNextDecision()}</section></div><section class="panel"><div class="panel-header"><div><h2>Your decision workspace</h2><p>Constraint-checked plans with source evidence attached</p></div><span class="badge neutral">${number(items.length)} recommendations</span></div>${recommendationTable(items)}<div class="panel-footer"><p>Uncertainty describes interval width, not failure probability.</p><a class="text-button" href="#decisions">Open workbench ${icon('arrow')}</a></div></section>${metadataNote()}`;
}

function switchPersonaButton(persona) {
  return `<button class="button secondary" data-persona="${persona}">Switch to ${readable(persona)}</button>`;
}

function actionButtons(item) {
  if (item.state === 'invalidated') return '<p class="callout warning">Source inputs changed, so this plan is invalidated. Generate a fresh plan before review. Its previous evidence remains available.</p>';
  if (reviewExpired(item) && item.state === 'pending')
  return '<p class="callout warning">This review window has expired. It can no longer be approved.</p>';
  if (['approved', 'execution_failed'].includes(item.state) && executionExpired(item)) {
    const warning = '<p class="callout warning">This approval has expired. Mock execution requires a current approval.</p>';
    if (canRenewApproval(item))
      return warning + `<button class="button primary" data-action="renew" data-id="${escape(item.id)}">Renew approval ${icon('approvals')}</button>`;
    if (role() === 'approver' && !reviewExpired(item))
      return warning + '<button class="button secondary" data-refresh>Refresh approval status</button>';
    if (item.approval_renewable === true && !reviewExpired(item)) return warning + switchPersonaButton('approver');
    return warning;
  }
  if (item.state === 'pending') return role() === 'approver' ?
    `<button class="button primary" data-action="approve" data-id="${escape(item.id)}">Approve recommendation ${icon('approvals')}</button><button class="button secondary" data-action="reject" data-id="${escape(item.id)}">Reject with a reason</button>` :
    '<p class="callout">An independent Approver can approve or reject this recommendation.</p>' +
      switchPersonaButton('approver');
  if (['approved', 'execution_failed'].includes(item.state)) return role() === 'approver' ?
    `<button class="button primary" data-action="execute" data-id="${escape(item.id)}">${item.state === 'execution_failed' ? 'Retry mock execution' : 'Execute mock order'} ${icon('arrow')}</button><button class="button secondary" data-action="revoke" data-id="${escape(item.id)}">Revoke approval</button>` :
    '<p class="callout">The approver can now execute this order through the local mock ERP.</p>' +
      switchPersonaButton('approver');
  if (item.state === 'executed') return role() === 'planner' ?
    `<button class="button primary" data-action="outcome" data-id="${escape(item.id)}">Record simulated outcome ${icon('refresh')}</button><p class="field-help">Observation opens ${escape(timeLabel(item.outcome_due_at))}. The demo compresses the wait to five seconds.</p>` :
    '<p class="callout">Execution is complete. A Planner can record the simulated outcome.</p>' +
      switchPersonaButton('planner');
  if (role() === 'auditor')
    return '<p class="callout">Auditor view is read-only. Inspect the evidence and decision history below.</p>';
  if (item.state === 'outcome_observed')
  return '<p class="callout">The feedback loop is complete. The observed scenario and original execution are preserved in the audit trail.</p>';
  return '<p class="callout">This decision is closed. Its evidence remains available for review.</p>';
}

function scenarioTable(scenarios) {
  return `<div class="table-wrap responsive-table"><table><thead><tr><th>Scenario</th><th>Expected cost</th><th>Baseline cost</th><th>Fill rate</th><th>Stockout</th></tr></thead><tbody>${scenarios.map(s => `<tr><td><span class="cell-title">${escape(s.name)}</span><span class="cell-subtitle">${number(s.evaluation_paths)} held-out paths</span></td><td class="numeric" data-label="Expected cost">${money(s.expected_cost)}</td><td class="numeric" data-label="Baseline cost">${money(s.baseline_cost)}</td><td class="numeric" data-label="Fill rate">${percent(s.fill_rate)}</td><td class="numeric" data-label="Stockout">${number(s.stockout_units,1)} units</td></tr>`).join('')}</tbody></table></div>`;
}

function renderDecisionCopilot(item) {
  if (!state.session?.local_copilot_enabled) return '';
  const current = item.generation?.is_current && !item.expired &&
    !['invalidated', 'rejected', 'revoked'].includes(item.state);
  const result = state.localAnswer?.id === item.id &&
    state.localAnswer?.fingerprint === item.fingerprint ? state.localAnswer.result : null;
  return `<section class="panel section-gap" aria-labelledby="decision-copilot-heading"><div class="panel-header"><div><h2 id="decision-copilot-heading">Ask about this decision</h2><p>Installed local model · governed read-only evidence</p></div>${icon('copilot')}</div><div class="panel-body">${current ? `<form id="decision-copilot-form"><label for="decision-copilot-question">Your question</label><textarea id="decision-copilot-question" rows="2" minlength="3" maxlength="1000" required placeholder="Why was this quantity recommended?"></textarea><p class="field-help">Ask about the forecast, inventory, supplier terms, scenarios, or a draft. The model selects evidence; the answer uses verified values and fictional documents. It cannot approve or execute.</p><button class="button primary section-gap-small" type="submit">Ask about decision ${icon('arrow')}</button></form><div id="decision-copilot-answer" aria-live="polite">${result ? renderDecisionAnswer(result) : ''}</div>` : '<p class="callout warning">This recommendation is no longer current and available for local model questions.</p>'}</div></section>`;
}

function renderDecisionAnswer(result) {
  const references = (result.document_citations || []).map((citation, index) =>
    `<div class="citation"><strong>[${index + 1}] ${escape(citation.reference_id)}</strong><p>${escape(citation.source_excerpt_untrusted)}</p></div>`).join('');
  const evidence = result.evidence?.citations || [];
  return `<div class="answer-box"><span class="eyebrow">VERIFIED DECISION ANSWER</span>${escape(result.answer)}</div><div class="answer-citations">${references}<details><summary>Inspect governed data reference</summary><pre>${escape(JSON.stringify(evidence, null, 2))}</pre></details></div>`;
}

function renderDetail(item) {
  const lineages = item.lineage || state.overview.lineage || [];
  const events = item.events || [];
  return `<a class="back-link" href="#decisions">${icon('back')} All recommendations</a>` + heading(
      'DECISION WORKBENCH', `${item.sku_id} / ${item.location_id}`,
      item.generation?.is_current === false ? 'Historical generation · original scenarios and lineage preserved.' : 'Review the order, test its assumptions, and follow its approval history.', `<span data-review-state="${escape(item.id)}">${stateBadge(item)}</span>`) +
    lifecycle() +
    `<div class="detail-grid"><div class="stack"><section class="panel"><div class="panel-header"><div><h2>The recommended order</h2><p>${escape(item.supplier_id)} · ${number(item.lead_time_days)}-day regular lead time</p></div>${badge(`${readable(item.risk)} uncertainty`,item.risk)}</div><div class="panel-body"><dl class="detail-facts"><div><dt>Order quantity</dt><dd>${number(item.recommended_quantity)} units</dd></div><div><dt>Order estimate</dt><dd>${money(item.expected_cost)}</dd></div><div><dt>Starting inventory</dt><dd>${number(item.initial_inventory)} units</dd></div></dl><p class="reason">${escape(item.reason)}</p>${chart(item)}<p class="field-help">${escape(item.risk_definition)} Cost includes fixed order charges. Scientific snapshot: ${escape(dateLabel(item.scientific_decision_time))}.</p></div></section><section class="panel"><div class="panel-header"><div><h2>How the plan holds up</h2><p>Portfolio-wide scenario evaluation · same order quantities</p></div></div>${scenarioTable(item.scenarios || [])}<div class="panel-body"><p class="field-help">Expected cost includes simulated operating costs. It differs from the purchase estimate above. These are held-out synthetic scenarios, not a claim of business savings.</p></div></section><section class="panel"><div class="panel-header"><div><h2>Decision history</h2><p>Each change retains an actor, timestamp, and integrity hash</p></div></div><div class="panel-body">${events.length ? `<ol class="trace-list">${events.map(event => `<li><strong>${escape(readable(event.event_type))}</strong><p>${escape(event.actor_id)} · ${escape(timeLabel(event.occurred_at))}</p>${event.details?.note ? `<p>${escape(event.details.note)}</p>` : ''}<details><summary>Inspect event record</summary><pre>${escape(JSON.stringify(event,null,2))}</pre></details></li>`).join('')}</ol>` : '<p class="reason">No workflow events recorded.</p>'}</div></section></div><div class="stack"><section class="panel"><div class="panel-header"><div><h2>Human review</h2><p>Acting as ${escape(state.session.user.name || readable(role()))}</p></div>${icon('shield')}</div><div class="panel-body"><div class="action-stack" data-review-actions="${escape(item.id)}">${actionButtons(item)}</div><p class="field-help">Review valid until ${escape(timeLabel(item.valid_until))}. ${item.approval_expires_at ? `Approval expires ${escape(timeLabel(item.approval_expires_at))}. ` : ''}All actions are bound to this exact recommendation.</p>${item.execution_ref ? `<details open><summary>Execution reference</summary><p class="code">${escape(item.execution_ref)}</p></details>` : ''}${item.outcome ? `<details open><summary>Observed outcome</summary><pre>${escape(JSON.stringify(item.outcome,null,2))}</pre></details>` : ''}</div></section><section class="panel"><div class="panel-header"><div><h2>Evidence, connected</h2><p>Inspectable authority at each stage</p></div></div><div class="panel-body"><ol class="trace-list">${lineages.map(line => `<li><strong>${escape(line.stage)}</strong><p>${escape(line.scope)}</p><p class="code">${escape(line.trace_id)}</p></li>`).join('')}<li><strong>Recommendation</strong><p>Checksum-bound immutable plan</p><p class="code">${escape(item.fingerprint)}</p></li></ol><details><summary>Full lineage and evidence references</summary><pre>${escape(JSON.stringify({lineage:lineages,manifest:item.manifest_fingerprint,evidence:item.evidence_sha256},null,2))}</pre></details></div></section></div></div>${renderDecisionCopilot(item)}${metadataNote()}`;
}

function renderDecisions() {
  return heading('PLANNING / RECOMMENDATIONS', 'From insight to action.',
      'Inspect every constraint-checked recommendation before it reaches an approver.') + lifecycle() +
    `<section class="panel"><div class="panel-header"><div><h2>Recommendation workbench</h2><p>${state.overview.metadata?.pipeline_mode === 'connected_runtime_baseline' ? 'Connected validated source · fresh seasonal baseline' : 'Frozen model evidence · fresh constrained optimization'}</p></div>${badge(`${state.recommendations.length} decisions`,'neutral')}</div>${recommendationTable(state.recommendations)}<div class="panel-footer"><p>Purchases are simulated. No external ERP or supplier is connected.</p></div></section><section class="panel section-gap"><div class="panel-header"><div><h2>Portfolio scenario comparison</h2><p>Operating cost and service under three evaluation conditions</p></div></div>${scenarioTable(state.overview.scenarios || [])}<div class="panel-body"><p class="field-help">Quantities are fixed across these scenarios. The baseline comparison is descriptive synthetic evidence; model or business superiority is not claimed.</p></div></section>${metadataNote()}`;
}

function renderApprovalCards(items) {
  return `${items.length ? items.map(item => `<article class="approval-card"><div><span class="eyebrow">${escape(item.location_id)} / ${escape(item.supplier_id)}</span><h2>${escape(item.sku_id)} · ${number(item.recommended_quantity)} units</h2><span data-review-state="${escape(item.id)}">${stateBadge(item)}</span><p>${money(item.expected_cost)} order estimate · ${number(item.lead_time_days)}-day regular lead time</p></div><div class="approval-card-actions"><a class="button ${(actionableReview(item) || (item.state === 'approved' && !executionExpired(item)) || item.state === 'executed') ? 'primary' : 'secondary'}" href="#decisions/${encodeURIComponent(item.id)}">${escape(reviewLinkLabel(item))} ${icon('arrow')}</a></div></article>`).join('') : empty('Nothing in this queue','Choose another status to see the remaining recommendations.','<button class="button secondary" data-filter="all">Show all decisions</button>')}`;
}

function renderApprovals() {
  const filters = [
    ['all', 'All decisions'],
    ['pending', 'Awaiting review'],
    ['expired', 'Expired reviews'],
    ['approved', 'Approved'],
    ['executed', 'Executed'],
    ['outcome_observed', 'Outcomes']
  ];
  const items = queueItems();
  return heading('HUMAN OVERSIGHT', 'A decision with an owner.',
      'Independent approval, controlled mock execution, and a recorded outcome.') +
    `<p class="callout">${role() === 'approver' ? 'You are the approver. Review the recommendation and explain your decision before executing a mock order.' : role() === 'planner' ? 'You are the planner. Inspect recommendations, then switch to Approver for independent review. Return as Planner to record feedback.' : 'You are the auditor. All recommendations and their evidence are available in read-only mode.'}</p><div class="filter-bar" aria-label="Filter approval queue">${filters.map(([id,label]) => `<button class="filter-button" data-filter="${id}" aria-pressed="${state.filter === id}">${label}</button>`).join('')}</div><p class="field-help" data-review-counts aria-live="polite">${number(reviewCounts().pending)} awaiting review · ${number(reviewCounts().expired)} expired reviews. Expired reviews retain evidence but cannot be approved.</p><div class="approval-list">${renderApprovalCards(items)}</div>${metadataNote()}`;
}

function healthValue(value) {
  return typeof value === 'number' ? number(value, 2) : typeof value === 'boolean' ? (value ? 'Yes' : 'No') :
    escape(value ?? '—');
}

function renderLearning() {
  const evidence = state.learning;
  const report = evidence?.report;
  const freshness = evidence?.status || 'unavailable';
  const freshnessLabel = {current: 'Source matched', stale: 'Stale source',
    not_run: 'Not run', unavailable: 'Unavailable'}[freshness] || 'Unavailable';
  const note = !evidence ? 'Learning evidence is unavailable. Refresh evidence to retry.' :
    !report ? 'No learning report is attached to this runtime. The current plan remains unchanged.' :
    freshness === 'stale' ? 'The source snapshot changed. This retained evaluation is historical, not current.' :
    'This evaluation matches the current source snapshot. It does not change the active model or plan.';
  const measured = (value, formatter = value => number(value, 4)) =>
    value == null ? 'Not measured' : formatter(value);
  const outcome = !report?.trained ? 'Training abstained' :
    ['REJECTED_GATE_FAILURE', 'CHALLENGER_LOST'].includes(report.evaluation_status) ? 'Candidate rejected' :
    report.evaluation_status === 'ELIGIBLE_BUT_BLOCKED_SYNTHETIC' ? 'Eligible, but blocked' :
    readable(report.evaluation_status);
  const facts = report ? [['Training performed', report.trained ? 'Yes' : 'No'],
    ['Training / calibration / final rows', ['training', 'calibration', 'final'].map(name =>
      measured(report.splits?.[name], value => number(value))).join(' / ')],
    ['80% / 95% interval coverage', ['candidate_coverage80', 'candidate_coverage95'].map(name =>
      measured(report.metrics?.[name], percent)).join(' / ')],
    ['Candidate / baseline WIS', ['candidate_wis', 'baseline_wis'].map(name =>
      measured(report.metrics?.[name])).join(' / ')],
    ['Coverage gates (80% / 95%)', ['coverage80_within_five_percentage_points',
      'coverage95_within_five_percentage_points'].map(name => report.quality_checks?.[name] == null ?
        'Not evaluated' : report.quality_checks[name] ? 'Passed' : 'Failed').join(' / ')],
    ['Artifact reload parity', report.quality_checks?.prediction_reload_parity == null ?
      'Not evaluated' : report.quality_checks.prediction_reload_parity ? 'Verified' : 'Failed']] : [];
  return `<section class="panel section-gap" aria-labelledby="learning-heading"><div class="panel-header"><div><h2 id="learning-heading">Candidate evaluation</h2><p>Read-only learning evidence · no promotion controls</p></div>${badge(freshnessLabel, freshness === 'stale' ? 'warning' : 'neutral')}</div><div class="panel-body"><p class="callout ${['stale','unavailable'].includes(freshness) ? 'warning' : ''}">${escape(note)}</p>${report ? `<h3>${escape(outcome)}</h3><p class="field-help">${escape(readable(report.evaluation_status))} · ${escape(readable(report.classification))} evidence<br>Not deployed · No human approval</p><dl class="detail-facts">${facts.map(([label,value]) => `<div><dt>${escape(label)}</dt><dd>${escape(value)}</dd></div>`).join('')}</dl><p class="field-help">Weighted interval score (WIS): lower is better. Split counts are evaluation rows, not distinct dates.</p><details><summary>Source and recorded revision bindings</summary><p class="field-help">Only source-snapshot currentness is checked here. Recorded code, plan, and finalization revisions are receipt-bound historical evidence, not revalidated current revisions.</p><ol class="trace-list">${Object.entries({...report.bindings, receipt_sha256:report.receipt_sha256, summary_sha256:evidence.summary_sha256}).map(([name,value]) => `<li><strong>${escape(readable(name))}</strong><p class="code">${escape(value ?? 'Not recorded')}</p></li>`).join('')}</ol></details>` : ''}</div></section>`;
}

function renderHealth() {
  const health = state.health,
    audit = state.audit,
    metrics = state.overview.metrics || {};
  const checks = health?.checks || [];
  const alerts = health?.alerts || [];
  const firing = alerts.some(alert => String(alert.status).toLowerCase() === 'firing');
  const events = [...(audit?.items || [])].reverse();
  return heading('OPERATIONS / EVIDENCE', 'See the whole system.',
      'Check data quality, exercise an incident, and inspect the chain behind every action.',
      `<button class="button secondary" data-refresh>${icon('refresh')} Refresh evidence</button>`) + renderLearning() +
    `<section class="kpi-grid section-gap" aria-label="Foundation quality metrics">${kpi('Source records',number(metrics.source_rows),'CSV and JSONL fixture','box')}${kpi('Validated records',number(metrics.accepted_rows),`${percent(metrics.source_validity_rate)} passed source contracts`,'shield')}${kpi('Quarantined records',number(metrics.quarantined_rows),'Rejected inputs retain a reason','info')}${kpi('Gold products',number(metrics.gold_products),`${number(metrics.gold_rows)} curated rows · dbt ${metrics.dbt_status || 'unknown'}`,'overview')}</section>${health ? `<div class="health-grid">${checks.map(check => `<section class="health-card"><h3>${escape(check.name)}</h3>${badge(readable(check.status),['healthy','pass','ok','verified'].includes(String(check.status).toLowerCase()) ? 'success' : ['warning','degraded'].includes(String(check.status).toLowerCase()) ? 'warning' : 'neutral')}<strong class="value">${healthValue(check.value)}</strong><p>${escape(check.detail)}</p></section>`).join('')}</div>` : '<p class="callout warning">Health evidence is unavailable. Refresh to retry the health endpoint.</p>'}` +
    `<section class="panel"><div class="panel-header"><div><h2>Incident exercise</h2><p>Persisted incident state with deduplicated notification evidence</p></div>${health ? badge(firing ? 'Incident active' : 'No active incident', firing ? 'warning' : 'success') : badge('Unavailable', 'neutral')}</div><div class="panel-body">${health ? `<p class="reason">Trigger a local demonstration incident, then resolve it. Inspect its notification and state history below.</p>${role() === 'planner' ? `<div class="filter-bar"><button class="button ${firing ? 'secondary' : 'primary'}" data-incident="trigger">Trigger demo incident</button><button class="button ${firing ? 'primary' : 'secondary'}" data-incident="resolve">Resolve demo incident</button></div>` : '<p class="field-help">Switch to Planner to operate the incident exercise.</p>'}${alerts.length ? `<ol class="trace-list">${alerts.map(alert => `<li><strong>${escape(alert.message || alert.id)}</strong><p>${escape(readable(alert.status))} · ${escape(readable(alert.severity))}${alert.created_at ? ` · ${escape(timeLabel(alert.created_at))}` : ''}</p></li>`).join('')}</ol>` : '<p class="field-help">No alert records are present.</p>'}<details><summary>Inspect operational metrics and incident delivery</summary><pre>${escape(JSON.stringify(health,null,2))}</pre></details>` : '<p>Incident status is unavailable. Refresh evidence to retry.</p>'}</div></section>` +
    `<section class="panel section-gap"><div class="panel-header"><div><h2>Decision audit trail</h2><p>${audit ? `${number(audit.total)} events · ordered and checksum-linked` : 'Decision audit is unavailable'}</p></div><div class="audit-integrity">${icon('shield')}${escape(audit?.integrity === 'verified' ? 'Integrity verified' : 'Unavailable')}</div></div>${audit ? `${events.length ? `<div class="table-wrap responsive-table"><table><thead><tr><th>Event</th><th>Actor</th><th>Timestamp</th><th>Evidence</th></tr></thead><tbody>${events.map(event => `<tr><td><span class="audit-type">${escape(readable(event.event_type))}</span><span class="cell-subtitle">#${number(event.sequence)} · ${escape(event.recommendation_id)}</span></td><td data-label="Actor"><span class="cell-title">${escape(event.actor_id)}</span><span class="cell-subtitle">${escape(readable(event.actor_role))}</span></td><td data-label="Timestamp">${escape(timeLabel(event.occurred_at))}</td><td data-label="Evidence"><details><summary>Inspect record</summary><pre>${escape(JSON.stringify(event,null,2))}</pre></details></td></tr>`).join('')}</tbody></table></div>` : empty('No decision events yet','Review a recommendation to start its audit trail.','<a class="button secondary" href="#decisions">Open recommendations</a>')}<div class="panel-footer"><p>Local integrity verification detects changed records; these hashes are not external signatures.</p><button class="text-button" data-export>${icon('download')} Export audit</button></div>` : '<div class="panel-body"><p>Refresh evidence to retry the audit endpoint.</p></div>'}</section>` +
    `<section class="panel section-gap"><div class="panel-header"><div><h2>Source inventory</h2><p>${state.overview.metadata?.pipeline_mode === 'connected_runtime_baseline' ? 'Connected R2 source snapshot' : 'Independent R2 foundation example'} · local fixtures</p></div></div><div class="table-wrap responsive-table"><table><thead><tr><th>Dataset</th><th>Format</th><th>Rows</th><th>File evidence</th></tr></thead><tbody>${(state.overview.sources || []).map(source => `<tr><td><span class="cell-title">${escape(source.dataset)}</span><span class="cell-subtitle">${escape(source.name)}</span></td><td data-label="Format">${escape(source.format.toUpperCase())}</td><td data-label="Rows">${number(source.rows)}</td><td data-label="File evidence"><details><summary>SHA-256</summary><p class="code">${escape(source.sha256)}</p></details></td></tr>`).join('')}</tbody></table></div></section>${metadataNote()}`;
}

function renderCopilot() {
  const suggestions = ['Which model is the champion?', 'How are recommendations approved?',
    'What are the limitations of this demo?'
  ];
  return heading('KNOWLEDGE / EVIDENCE', 'Ask. Inspect. Understand.',
      'A read-only guide to project documentation, methods, and operating boundaries.') +
    `<div class="copilot-grid"><section class="panel"><div class="panel-header"><div><h2>Evidence assistant</h2><p>Deterministic reference mode · no model API required</p></div>${icon('copilot')}</div><div class="panel-body"><div class="copilot-intro"><span class="copilot-mark">${icon('copilot')}</span><p class="reason">Ask general questions about the project’s forecasting methods, data, approval policy, or documented limitations. This reference tool searches project documents; it does not analyze the currently selected decision.</p></div><div class="suggestions">${suggestions.map(question => `<button class="suggestion" data-question="${escape(question)}">${escape(question)}</button>`).join('')}</div><form id="copilot-form"><label for="copilot-question">Your question</label><textarea id="copilot-question" rows="3" minlength="3" maxlength="1000" required placeholder="How does the project’s approval policy work?"></textarea><p class="field-help">Read-only retrieval. The assistant cannot approve or execute orders.</p><button class="button primary section-gap-small" type="submit">Ask the evidence ${icon('arrow')}</button></form><div id="copilot-answer" aria-live="polite">${state.answer ? renderAnswer(state.answer) : ''}</div></div></section><div class="stack"><section class="panel"><div class="panel-header"><h2>What this mode does</h2></div><div class="panel-body"><ol class="trace-list"><li><strong>Find relevant references</strong><p>Match your question against the project’s published documentation.</p></li><li><strong>Show the source</strong><p>Read the cited excerpt and document path beside the answer.</p></li><li><strong>Keep decisions human</strong><p>Role checks and immutable approval records remain separate from reference answers.</p></li></ol></div></section><section class="panel"><div class="panel-header"><h2>Model context</h2></div><div class="panel-body"><p class="reason">${escape(state.overview.metadata?.forecast_mode)}</p>${badge(readable(state.overview.metadata?.challenger_status),'neutral')}<p class="field-help">${state.overview.metadata?.pipeline_mode === 'connected_runtime_baseline' ? 'This runtime plan uses a fresh seasonal baseline. Frozen benchmark results remain separate scientific evidence.' : 'The current champion remains authoritative. A losing challenger is not silently promoted.'}</p></div></section></div></div>`;
}

function renderAnswer(answer) {
  return `<div class="answer-box"><span class="eyebrow">${answer.mode === 'deterministic-evidence' ? 'REFERENCE ANSWER' : 'EVIDENCE RESPONSE'}</span>${escape(answer.answer)}</div><div class="answer-citations">${(answer.citations || []).map((citation,index) => `<div class="citation"><strong>[${index+1}] ${escape(citation.title)}</strong><p class="code">${escape(citation.path)}</p><p>${escape(citation.excerpt)}</p></div>`).join('')}</div>`;
}
async function loadOverview() {
  const session = state.session;
  const overview = await request('/api/overview');
  const recommendations = overview.recommendations || (await request('/api/recommendations')).items;
  if (session !== state.session) return false;
  state.overview = overview;
  state.recommendations = recommendations;
  const checked = Date.parse(overview.checked_at);
  state.serverTime = Number.isFinite(checked) ? checked : Date.now();
  state.serverTimeReadAt = performance.now();
  scheduleExpiryCheck();
  return true;
}
async function renderRoute({
  focus = false
} = {}) {
  const version = ++state.routeVersion;
  const [page = 'overview', id] = location.hash.slice(1).split('/');
  const active = navItems.some(([key]) => key === page) ? page : 'overview';
  nav(active);
  const main = $('#main');
  let complete = true;
  try {
    if (active !== 'decisions' || !id) state.selected = null;
    if (active === 'decisions' && id) {
      main.innerHTML =
        '<div class="loading-state" role="status"><span class="loading-mark"></span><h2>Loading recommendation</h2></div>';
      const item = await request(`/api/recommendations/${encodeURIComponent(decodeURIComponent(id))}`);
      if (version !== state.routeVersion) return;
      state.selected = item;
      main.innerHTML = renderDetail(item);
    } else if (active === 'health') {
      main.innerHTML =
        '<div class="loading-state" role="status"><span class="loading-mark"></span><h2>Checking system evidence</h2></div>';
      const results = await Promise.allSettled([request('/api/health'), request('/api/audit'),
        request('/api/learning')]);
      if (version !== state.routeVersion) return;
      state.health = results[0].status === 'fulfilled' ? results[0].value : null;
      state.audit = results[1].status === 'fulfilled' ? results[1].value : null;
      state.learning = results[2].status === 'fulfilled' ? results[2].value : null;
      complete = results.every(result => result.status === 'fulfilled');
      main.innerHTML = renderHealth();
    } else {
      main.innerHTML = ({
        overview: renderOverview,
        decisions: renderDecisions,
        approvals: renderApprovals,
        copilot: renderCopilot
      } [active] || renderOverview)();
    }
    updateExpiryUI();
    scheduleExpiryCheck();
    document.title = `${navItems.find(([key]) => key === active)?.[1] || 'Overview'} | NEXUS`;
    if (focus) main.focus({
      preventScroll: true
    });
    return complete;
  } catch (error) {
    if (version !== state.routeVersion) return;
    main.innerHTML = empty('This view could not be loaded', error.message,
      '<button class="button primary" data-refresh>Try again</button>');
    if (focus) $('#main [data-refresh]')?.focus();
    return false;
  }
}
async function openAction(action, id) {
  const session = state.session;
  const hash = location.hash;
  try {
    const item = await request(`/api/recommendations/${encodeURIComponent(id)}`);
    if (state.session !== session || location.hash !== hash) return;
    const refusal = blockedAction(action, item);
    if (refusal) throw new Error(refusal);
    const titles = {
      approve: 'Approve this recommendation?',
      renew: 'Renew this approval?',
      reject: 'Reject this recommendation?',
      revoke: 'Revoke this approval?',
      execute: 'Execute this mock order?',
      outcome: 'Record the simulated outcome?'
    };
    const descriptions = {
      approve: 'Your approval authorizes the exact plan shown below. It is recorded separately from the planner who created the recommendation.',
      renew: 'Renew authorization for this exact plan and record a new decision note.',
      reject: 'Record why this recommendation should not proceed. The original plan and evidence remain available.',
      revoke: 'Revoke permission to execute this recommendation. Your reason becomes part of its decision history.',
      execute: 'Create one order effect in the local mock ERP. This demonstration does not contact a supplier or spend money.',
      outcome: 'Attach the governed synthetic observation to the original mock execution. The server checks that the demo observation window has matured.'
    };
    state.action = {
      action,
      item,
      key: crypto.randomUUID()
    };
    $('#dialog-title').textContent = titles[action];
    $('#dialog-description').textContent = descriptions[action];
    $('#dialog-summary').innerHTML =
      `<strong>${escape(item.sku_id)} / ${escape(item.location_id)}</strong><br>${number(item.recommended_quantity)} units · ${money(item.expected_cost)} order estimate<br>Acting as ${escape(state.session.user.name || role())}`;
    $('#action-note').value = '';
    $('#action-note').required = action !== 'execute';
    $('#note-field').hidden = action === 'execute';
    $('#confirm-action').textContent = {
      approve: 'Approve recommendation',
      renew: 'Renew approval',
      reject: 'Reject recommendation',
      revoke: 'Revoke approval',
      execute: 'Execute mock order',
      outcome: 'Record outcome'
    } [action];
    $('#confirm-action').disabled = false;
    $('#dialog-error').hidden = true;
    $('#action-dialog').showModal();
    scheduleExpiryCheck();
  } catch (error) {
    notice(error.message, true);
  }
}
async function refresh(options) {
  if (await loadOverview()) return renderRoute(options);
}
function showRefreshRetry(title, message) {
  $('#main').innerHTML = empty(title, message,
    '<button class="button primary" data-refresh>Refresh workspace</button>');
}
function focusReview() {
  const target = $('#main [data-review-actions] button') || $('#main [data-refresh]') || $('#main');
  target.focus({preventScroll: true});
}
async function refreshAfterSave(message, reviewHash) {
  notice(message);
  const hash = location.hash;
  if (hash === reviewHash) {
    const actions = $('#main [data-review-actions]');
    if (actions) actions.innerHTML = `<p class="callout" role="status">${escape(message)} Refreshing the decision…</p>`;
  }
  try {
    if (await refresh() === false) {
      const warning = `${message} Some updated information is unavailable. Refresh to retry.`;
      if (location.hash === reviewHash) showRefreshRetry('Decision saved', warning);
      notice(warning, true);
    }
  } catch {
    const warning = `${message} Workspace refresh failed. Refresh to retry.`;
    if (location.hash === hash) showRefreshRetry('Change saved', warning);
    notice(warning, true);
  }
  if (location.hash === reviewHash) focusReview();
}
async function initialize() {
  try {
    state.session = await request('/api/session');
    if (!state.session.user) state.session = await request('/api/session', {
      method: 'POST',
      body: {
        persona: 'planner'
      }
    });
    $('#persona').value = role();
    $('#persona-avatar').textContent = role().slice(0, 2).toUpperCase();
    await loadOverview();
    await renderRoute();
  } catch (error) {
    nav('overview');
    $('#main').innerHTML = empty('Your workspace needs attention', error.message,
      '<button class="button primary" data-initialize>Reconnect workspace</button>');
  }
}
async function switchPersona(persona, trigger) {
  const control = $('#persona');
  if (control.disabled) return;
  const hash = location.hash;
  control.disabled = true;
  if (trigger) trigger.disabled = true;
  try {
    $('#action-dialog').close();
    state.session = await request('/api/session', {
      method: 'POST',
      body: {
        persona
      }
    });
    control.value = role();
    $('#persona-avatar').textContent = role().slice(0, 2).toUpperCase();
    state.routeVersion++;
    state.overview = null;
    state.recommendations = [];
    state.selected = null;
    state.answer = null;
    state.localAnswer = null;
    nav(hash.slice(1).split('/')[0] || 'overview');
    $('#main').innerHTML = '<div class="loading-state" role="status"><span class="loading-mark"></span><h2>Loading workspace</h2></div>';
    const message = `Now acting as ${readable(role())}.`;
    try {
      if (await refresh() === false) notice(`${message} Some workspace information is unavailable.`, true);
      else notice(message);
    } catch {
      showRefreshRetry('Workspace refresh needed', `${message} Refresh to load this persona’s workspace.`);
      notice(`${message} Workspace refresh failed.`, true);
    }
    if (trigger && location.hash === hash) focusReview();
  } catch (error) {
    notice(error.message, true);
  } finally {
    control.value = role();
    control.disabled = false;
    if (trigger) trigger.disabled = false;
  }
}
$('#persona').addEventListener('change', event => {
  return switchPersona(event.currentTarget.value);
});
document.addEventListener('visibilitychange', () => {
  if (!document.hidden) {
    updateExpiryUI();
    scheduleExpiryCheck();
  }
});
window.addEventListener('hashchange', () => {
  if (state.overview) {
    window.scrollTo({
      top: 0
    });
    renderRoute({
      focus: true
    });
  }
});
$('#main').addEventListener('change', event => {
  if (event.target.id === 'chart-product') {
    state.chartId = event.target.value;
    renderRoute();
    $('#chart-product')?.focus();
  }
});
$('#main').addEventListener('click', async event => {
  const persona = event.target.closest('[data-persona]');
  if (persona) {
    await switchPersona(persona.dataset.persona, persona);
    return;
  }
  const action = event.target.closest('[data-action]');
  if (action) {
    await openAction(action.dataset.action, action.dataset.id);
    return;
  }
  const filter = event.target.closest('[data-filter]');
  if (filter) {
    state.filter = filter.dataset.filter;
    await renderRoute();
    $(`[data-filter="${state.filter}"]`)?.focus();
    return;
  }
  const question = event.target.closest('[data-question]');
  if (question) {
    $('#copilot-question').value = question.dataset.question;
    $('#copilot-question').focus();
    return;
  }
  if (event.target.closest('[data-initialize]')) {
    await initialize();
    return;
  }
  if (event.target.closest('[data-refresh]')) {
    try {
      const complete = await refresh({focus: true});
      if (complete) notice('Evidence refreshed.');
      if (complete === false) notice('Some workspace information is unavailable. Refresh to retry.', true);
    } catch (error) {
      notice(error.message, true);
    }
    return;
  }
  const incident = event.target.closest('[data-incident]');
  if (incident) {
    incident.disabled = true;
    try {
      await request('/api/incidents/demo', {
        method: 'POST',
        body: {
          action: incident.dataset.incident
        }
      });
      await refreshAfterSave(incident.dataset.incident === 'trigger' ?
        'Demo incident triggered. Inspect the persisted alert evidence.' :
        'Demo incident resolved. Recovery evidence recorded.');
    } catch (error) {
      incident.disabled = false;
      notice(error.message, true);
    }
    return;
  }
  if (state.audit && event.target.closest('[data-export]')) {
    const blob = new Blob([JSON.stringify(state.audit, null, 2)], {
      type: 'application/json'
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'nexus-decision-audit.json';
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
});
$('#main').addEventListener('submit', async event => {
  if (event.target.id === 'decision-copilot-form') {
    event.preventDefault();
    const button = $('button[type="submit"]', event.target);
    const question = $('#decision-copilot-question').value.trim();
    const item = state.selected;
    const session = state.session;
    if (!question || !item) return;
    button.disabled = true;
    button.textContent = 'Checking decision evidence…';
    try {
      const result = await request('/api/copilot/local', {
        method: 'POST',
        body: {
          question,
          recommendation_id: item.id,
          location_id: item.location_id,
          generation_sha256: item.generation.seed_sha256,
          fingerprint: item.fingerprint
        }
      });
      if (state.session === session && state.selected?.fingerprint === item.fingerprint) {
        state.localAnswer = {id: item.id, fingerprint: item.fingerprint, result};
        const target = $('#decision-copilot-answer');
        if (target) target.innerHTML = renderDecisionAnswer(result);
      }
    } catch (error) {
      notice(error.message, true);
    } finally {
      button.disabled = false;
      button.innerHTML = `Ask about decision ${icon('arrow')}`;
    }
    return;
  }
  if (event.target.id !== 'copilot-form') return;
  event.preventDefault();
  const button = $('button[type="submit"]', event.target);
  const question = $('#copilot-question').value.trim();
  if (!question) return;
  button.disabled = true;
  button.textContent = 'Finding evidence…';
  try {
    state.answer = await request('/api/copilot', {
      method: 'POST',
      body: {
        question
      }
    });
    const target = $('#copilot-answer');
    if (target) target.innerHTML = renderAnswer(state.answer);
  } catch (error) {
    notice(error.message, true);
  } finally {
    button.disabled = false;
    button.innerHTML = `Ask the evidence ${icon('arrow')}`;
  }
});
$('#close-dialog').addEventListener('click', () => $('#action-dialog').close());
$('#cancel-dialog').addEventListener('click', () => $('#action-dialog').close());
$('#action-form').addEventListener('submit', async event => {
  event.preventDefault();
  const confirmation = state.action;
  if (!confirmation) return;
  const button = $('#confirm-action');
  if (button.disabled) return;
  const {
    action,
    item,
    key
  } = confirmation;
  const reviewHash = location.hash;
  const refusal = blockedAction(action, item);
  if (refusal) {
    $('#dialog-error').textContent = refusal;
    $('#dialog-error').hidden = false;
    $('#confirm-action').disabled = true;
    return;
  }
  const oldText = button.textContent;
  button.disabled = true;
  button.textContent = 'Saving…';
  $('#dialog-error').hidden = true;
  const body = {
    fingerprint: item.fingerprint
  };
  if (action !== 'execute') body.note = $('#action-note').value.trim();
  if (action !== 'execute' && !body.note) {
    $('#dialog-error').textContent = 'Add a decision note before continuing.';
    $('#dialog-error').hidden = false;
    button.disabled = false;
    button.textContent = oldText;
    $('#action-note').focus();
    return;
  }
  let saved;
  try {
    const endpoint = action === 'renew' ? 'approve' : action;
    saved = await request(`/api/recommendations/${encodeURIComponent(item.id)}/${endpoint}`, {
      method: 'POST',
      body,
      headers: action === 'execute' ? {
        'Idempotency-Key': key
      } : {}
    });
  } catch (error) {
    if (state.action === confirmation && $('#action-dialog').open) {
      $('#dialog-error').textContent = error.message;
      $('#dialog-error').hidden = false;
    } else notice(error.message, true);
    return;
  } finally {
    if (state.action === confirmation) {
      button.disabled = Boolean(blockedAction(action, item));
      button.textContent = oldText;
    }
  }
  if (state.action === confirmation) {
    state.action = null;
    $('#action-dialog').close();
  }
  const message = {
    approve: 'Recommendation approved.',
    renew: 'Approval renewed.',
    reject: 'Recommendation rejected. Your reason is in the audit trail.',
    revoke: 'Approval revoked. The decision history has been preserved.',
    execute: saved.state === 'execution_failed' ? 'Mock execution failed. The failure was recorded.' :
      'Mock order executed. Switch to Planner to record the simulated outcome.',
    outcome: 'Outcome recorded. The decision feedback loop is complete.'
  }[action];
  await refreshAfterSave(message, reviewHash);
});
$('.skip-link').addEventListener('click', event => {
  event.preventDefault();
  $('#main').focus();
});
initialize();
