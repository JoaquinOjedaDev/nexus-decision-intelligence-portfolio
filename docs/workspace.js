'use strict';

const element = id => document.getElementById(id);
const number = new Intl.NumberFormat('en-US', {maximumFractionDigits: 1});
const usd = new Intl.NumberFormat('en-US', {style: 'currency', currency: 'USD', maximumFractionDigits: 0});
const percent = new Intl.NumberFormat('en-US', {style: 'percent', maximumFractionDigits: 1});
const day = new Intl.DateTimeFormat('en-US', {month: 'short', day: 'numeric', timeZone: 'UTC'});
const date = value => day.format(new Date(value));
const show = (id, value) => { element(id).textContent = value; };

function node(tag, text, className) {
  const item = document.createElement(tag);
  if (text !== undefined) item.textContent = text;
  if (className) item.className = className;
  return item;
}

const orders = snapshot.recommendations;
const picker = element('item');
const audit = snapshot.audit;
const auditOrder = orders.findIndex(order => order.sku_id === audit.sku && order.location_id === audit.location);
const forecastPeak = Math.max(...orders.flatMap(order => order.forecast.map(([, , , high]) => high)));
const forecastCeiling = Math.max(10, Math.ceil(forecastPeak / 10) * 10);

function renderForecast(forecast) {
  const chart = element('chart');
  const table = element('forecast-values');
  chart.replaceChildren();
  table.replaceChildren();
  for (const [when, low, median, high] of forecast) {
    const column = node('div', undefined, 'day');
    const range = node('span', undefined, 'range');
    range.style.bottom = `${low / forecastCeiling * 100}%`;
    range.style.height = `${(high - low) / forecastCeiling * 100}%`;
    const middle = node('span', undefined, 'median');
    middle.style.bottom = `${median / forecastCeiling * 100}%`;
    column.append(range, middle);
    chart.append(column);

    const row = node('tr');
    const dateCell = node('th', date(when));
    dateCell.scope = 'row';
    row.append(dateCell, ...[low, median, high].map(value => node('td', number.format(value))));
    table.append(row);
  }

  element('chart-dates').replaceChildren(
    node('span', date(forecast[0][0])),
    node('span', date(forecast.at(-1)[0])),
  );
}

function renderRecommendation() {
  const orderIndex = Number(picker.value);
  const order = orders[orderIndex];
  show('quantity', `${number.format(order.recommended_quantity)} units`);
  show('inventory', `${number.format(order.initial_inventory)} units`);
  show('purchase', usd.format(order.expected_cost));
  const leadTimeUnit = order.lead_time_days === 1 ? 'day' : 'days';
  show('supplier', `${order.supplier_id} · ${order.lead_time_days} ${leadTimeUnit}`);
  renderForecast(order.forecast);

  const hasTrail = orderIndex === auditOrder;
  element('audit-details').hidden = !hasTrail;
  element('show-audit').hidden = hasTrail || auditOrder === -1;
  show('audit-summary', hasTrail
    ? `${audit.sku} at ${audit.location}. Replay retained exactly ${audit.effects} mock order.`
    : `The recorded approval trail covers ${audit.sku} at ${audit.location}.`);
}

function renderScenarios() {
  const scenarios = snapshot.scenarios;
  const normal = scenarios.find(scenario => scenario.name === 'Normal');
  const surge = scenarios.find(scenario => scenario.name === 'Demand Surge');
  const ceiling = Math.max(1, ...scenarios.map(scenario => scenario.expected_cost));

  if (normal && surge && surge.expected_cost > normal.expected_cost && normal.expected_cost > 0) {
    const increase = new Intl.NumberFormat('en-US', {style: 'percent'})
      .format(surge.expected_cost / normal.expected_cost - 1);
    show('scenario-insight', `Demand surge raised mean operating cost ${increase} over normal.`);
  }

  for (const scenario of scenarios) {
    const row = node('li', undefined, 'scenario');
    const heading = node('div', undefined, 'scenario-heading');
    heading.append(node('h3', scenario.name), node('strong', usd.format(scenario.expected_cost)));

    const track = node('div', undefined, 'scenario-track');
    const bar = node('span');
    track.setAttribute('aria-hidden', 'true');
    bar.style.width = `${scenario.expected_cost / ceiling * 100}%`;
    if (scenario === surge) bar.classList.add('surge');
    track.append(bar);

    const metrics = node('p', `${percent.format(scenario.fill_rate)} fill · ${number.format(scenario.expedite_units)} mean expedited units`);
    row.append(heading, track, metrics);
    element('scenario-values').append(row);
  }

  const paths = normal && scenarios.every(scenario => scenario.evaluation_paths === normal.evaluation_paths)
    ? ` · ${normal.evaluation_paths} synthetic paths per scenario`
    : '';
  show('scenario-scope', `All recommended orders${paths}. Operating costs include expediting.`);
}

const {budget, purchase_cost: purchaseTotal, solver_status: solverStatus, feasible} = snapshot.plan;
const usage = purchaseTotal / budget;
const horizon = orders[0].forecast.length;
show('plan-description', `A ${horizon}-day supply plan with ${orders.length} recommended orders, using synthetic data.`);
show('cutoff', snapshot.cutoff);
show('solver', feasible ? `${solverStatus[0]}${solverStatus.slice(1).toLowerCase()} · feasibility checked` : 'Feasibility unverified');
show('purchase-total', usd.format(purchaseTotal));
show('budget-total', `of ${usd.format(budget)}`);
show('budget-usage', `${percent.format(usage)} allocated`);
show('budget-left', purchaseTotal <= budget
  ? `${usd.format(budget - purchaseTotal)} available`
  : `${usd.format(purchaseTotal - budget)} over budget`);
element('budget-fill').style.width = `${Math.min(usage, 1) * 100}%`;

for (const [index, order] of orders.entries()) {
  picker.add(new Option(`${order.sku_id} · ${order.location_id}`, String(index)));
}
element('chart-axis').append(...[forecastCeiling, forecastCeiling / 2, 0].map(value => node('span', String(value))));
picker.addEventListener('change', renderRecommendation);
element('show-audit').addEventListener('click', () => {
  picker.value = String(auditOrder);
  renderRecommendation();
  picker.focus();
});

const eventLabels = {
  recommendation_created: 'Recommendation recorded',
  approved: 'Independent approval',
  execution_requested: 'Execution requested',
  executed: 'Mock order recorded',
  outcome_observed: 'Simulated outcome recorded',
};
for (const event of audit.events) {
  const item = node('li');
  item.append(node('strong', eventLabels[event.event_type]), node('span', event.actor_role));
  element('audit-events').append(item);
}
show('execution', audit.execution_ref);
show('received', `${number.format(audit.received)} units`);
show('observed', `${number.format(audit.demand)} units`);

const validation = snapshot.validation;
show('validation',
  `${validation.accepted} of ${validation.records} source records passed validation. ` +
  `${validation.quarantined} were quarantined. ${validation.dbt_checks} dbt checks and ` +
  `${validation.workflow_checks} connected workflow checks passed.`);

renderRecommendation();
renderScenarios();
