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
const forecastPeak = Math.max(...orders.flatMap(order => order.forecast.map(([, , , high]) => high)));
const forecastCeiling = Math.max(10, Math.ceil(forecastPeak / 10) * 10);
const eventLabels = {
  recommendation_created: 'Order recommended',
  approved: 'Independent approval',
  execution_requested: 'Order requested',
  executed: 'Order recorded',
  outcome_observed: 'Outcome recorded',
};

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
  const order = orders[Number(picker.value)];
  show('quantity', `${number.format(order.recommended_quantity)} units`);
  show('inventory', `${number.format(order.initial_inventory)} units`);
  show('purchase', usd.format(order.expected_cost));
  const leadTimeUnit = order.lead_time_days === 1 ? 'day' : 'days';
  show('supplier', `${order.supplier_id} · ${order.lead_time_days} ${leadTimeUnit}`);
  renderForecast(order.forecast);
  renderOrderHistory(order);
}

function renderOrderHistory(order) {
  const history = order.history;
  show('audit-order', `${order.sku_id} at ${order.location_id}`);
  element('audit-record').hidden = !history;
  element('history-empty').hidden = Boolean(history);
  if (!history) return;

  const events = history.events.map(event => {
    const item = node('li');
    item.append(node('strong', eventLabels[event.event_type]), node('span', event.actor_role));
    return item;
  });
  element('audit-events').replaceChildren(...events);
  show('execution', history.execution_ref);
  show('received', `${number.format(history.received)} units`);
  show('observed', `${number.format(history.demand)} units`);
}

function renderScenarios() {
  const scenarios = snapshot.scenarios;
  const normal = scenarios.find(scenario => scenario.name === 'Normal');
  const surge = scenarios.find(scenario => scenario.name === 'Demand Surge');
  const ceiling = Math.max(1, ...scenarios.map(scenario => scenario.expected_cost));

  if (normal && surge && surge.expected_cost > normal.expected_cost && normal.expected_cost > 0) {
    const increase = new Intl.NumberFormat('en-US', {style: 'percent'})
      .format(surge.expected_cost / normal.expected_cost - 1);
    show('scenario-insight', `A demand surge costs ${increase} more than normal on average.`);
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

    const metrics = node('p', `${percent.format(scenario.fill_rate)} fill rate · ${number.format(scenario.expedite_units)} expedited units on average`);
    row.append(heading, track, metrics);
    element('scenario-values').append(row);
  }
}

const {budget, purchase_cost: purchaseTotal} = snapshot.plan;
const usage = purchaseTotal / budget;
const horizon = orders[0].forecast.length;
show('plan-description', `${orders.length} recommended orders over ${horizon} days.`);
show('purchase-total', usd.format(purchaseTotal));
show('budget-total', `of ${usd.format(budget)}`);
show('budget-left', purchaseTotal <= budget
  ? `${usd.format(budget - purchaseTotal)} remaining`
  : `${usd.format(purchaseTotal - budget)} over budget`);
element('budget-fill').style.width = `${Math.min(usage, 1) * 100}%`;

for (const [index, order] of orders.entries()) {
  const quantity = number.format(order.recommended_quantity);
  picker.add(new Option(`${order.sku_id} · ${order.location_id} · ${quantity} units`, String(index)));
}
element('chart-axis').append(...[forecastCeiling, forecastCeiling / 2, 0].map(value => node('span', String(value))));
picker.addEventListener('change', renderRecommendation);

renderRecommendation();
renderScenarios();
