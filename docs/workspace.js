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

function renderRecommendation() {
  const order = snapshot.recommendations[Number(element('item').value)];
  show('quantity', `${number.format(order.recommended_quantity)} units`);
  show('inventory', `${number.format(order.initial_inventory)} units`);
  show('purchase', usd.format(order.expected_cost));
  show('supplier', `${order.supplier_id} · ${order.lead_time_days} days`);

  const ceiling = Math.max(1, ...order.forecast.map(([, , , high]) => high));
  const chart = element('chart');
  const table = element('forecast-values');
  chart.replaceChildren();
  table.replaceChildren();
  for (const [when, low, median, high] of order.forecast) {
    const column = node('div', undefined, 'day');
    const range = node('span', undefined, 'range');
    range.style.bottom = `${low / ceiling * 100}%`;
    range.style.height = `${(high - low) / ceiling * 100}%`;
    const middle = node('span', undefined, 'median');
    middle.style.bottom = `${median / ceiling * 100}%`;
    column.append(range, middle);
    chart.append(column);

    const row = node('tr');
    const dateCell = node('th', date(when));
    dateCell.scope = 'row';
    row.append(dateCell, ...[low, median, high].map(value => node('td', number.format(value))));
    table.append(row);
  }

  const last = order.forecast.at(-1);
  element('chart-dates').replaceChildren(
    node('span', date(order.forecast[0][0])),
    node('span', `Scale: 0–${number.format(ceiling)} units/day`),
    node('span', date(last[0])),
  );
}

show('cutoff', snapshot.cutoff);
show('budget', `${usd.format(snapshot.plan.purchase_cost)} / ${usd.format(snapshot.plan.budget)}`);
show('solver', snapshot.plan.feasible ? `${snapshot.plan.solver_status.toLowerCase()} · feasibility checked` : 'Feasibility unverified');

for (const [index, order] of snapshot.recommendations.entries()) {
  element('item').add(new Option(`${order.sku_id} · ${order.location_id}`, String(index)));
}
element('item').addEventListener('change', renderRecommendation);
renderRecommendation();

for (const scenario of snapshot.scenarios) {
  const article = node('article', undefined, 'scenario');
  const facts = node('dl');
  for (const [label, value] of [
    ['Mean operating cost', usd.format(scenario.expected_cost)],
    ['Fill rate', percent.format(scenario.fill_rate)],
    ['Mean expedited units', number.format(scenario.expedite_units)],
  ]) {
    const fact = node('div');
    fact.append(node('dt', label), node('dd', value));
    facts.append(fact);
  }
  article.append(node('h3', scenario.name), facts);
  element('scenario-values').append(article);
}

const audit = snapshot.audit;
show('audit-summary', `Recorded workflow for ${audit.sku} at ${audit.location}. The replay check retained exactly ${audit.effects} mock order.`);
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
show('received', number.format(audit.received));
show('observed', `${number.format(audit.demand)} units`);

const validation = snapshot.validation;
show('validation',
  `${validation.accepted} of ${validation.records} source records passed validation. ` +
  `${validation.quarantined} were quarantined. ${validation.dbt_checks} dbt checks and ` +
  `${validation.workflow_checks} connected workflow checks passed.`);
