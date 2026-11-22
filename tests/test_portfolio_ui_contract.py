"""Check the control tower's presentation and request handling in a lightweight DOM."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


def test_review_priority_counts_and_actions_expire_with_server_time() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for the optional browser presentation contract")
    app = Path(__file__).resolve().parents[1] / "src/nexus/portfolio/static/app.js"
    harness = r"""
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(process.argv[1], 'utf8').replace(/\ninitialize\(\);\s*$/, '\n');
const element = {addEventListener() {}, close() {}, focus() {}};
const checks = `
state.serverTime = Date.parse('2030-01-01T00:00:00Z');
state.serverTimeReadAt = 0;
state.session = {user:{role:'approver', name:'Test Approver'}};
const past = {id:'past',sku_id:'EXPIRED-SKU',state:'pending',expired:false,
  valid_until:new Date(state.serverTime-1).toISOString(),risk:'high',expected_cost:1000};
const fresh = {id:'fresh',sku_id:'ACTIVE-SKU',state:'pending',expired:false,
  valid_until:new Date(state.serverTime+5000).toISOString(),risk:'low',expected_cost:1};
const closed = {...fresh,id:'closed',sku_id:'CLOSED-SKU',state:'rejected'};
state.recommendations = [past, fresh, closed];
state.overview = {metadata:{},metrics:{},summary:{pending:99}};
assert.equal(reviewCounts().pending,1);
assert.equal(reviewCounts().expired,1);
assert.match(renderNextDecision(),/ACTIVE-SKU/);
assert.doesNotMatch(renderNextDecision(),/EXPIRED-SKU|CLOSED-SKU/);
state.filter='pending'; assert.deepEqual(queueItems().map(x=>x.id),['fresh']);
state.filter='expired'; assert.deepEqual(queueItems().map(x=>x.id),['past']);
assert.match(reviewLinkLabel(past),/expired evidence/);
assert.equal(blockedAction('approve',fresh),'');
assert.match(renderCopilot(),/project documents/);
assert.doesNotMatch(renderCopilot(),/What evidence supports this decision/);
assert.match(lifecycle(),/Empirical forecast bands/);
state.overview.metadata.pipeline_mode='connected_runtime_baseline';
assert.match(lifecycle(),/Connected validated source/);
assert.match(lifecycle(),/Fresh seasonal baseline/);
assert.doesNotMatch(lifecycle(),/Empirical forecast bands/);
const invalidated = {...fresh,state:'invalidated'};
assert.match(stateBadge(invalidated),/Invalidated/);
assert.match(actionButtons(invalidated),/fresh plan/);
assert.doesNotMatch(actionButtons(invalidated),/data-action=/);
elapsed=5000;
assert.equal(reviewCounts().pending,0);
assert.equal(reviewCounts().expired,2);
assert.match(renderNextDecision(),/No reviews awaiting action/);
assert.doesNotMatch(renderNextDecision(),/ACTIVE-SKU|EXPIRED-SKU|CLOSED-SKU/);
assert.match(actionButtons(fresh),/expired/);
assert.doesNotMatch(actionButtons(fresh),/data-action="approve"/);
assert.match(blockedAction('approve',fresh),/no longer open/);
const approved = {...fresh,state:'approved',
  valid_until:new Date(state.serverTime+10000).toISOString(),
  approval_expires_at:new Date(state.serverTime+5000).toISOString()};
assert.equal(reviewExpired(approved),false);
assert.equal(executionExpired(approved),true);
assert.doesNotMatch(actionButtons(approved),/data-action="execute"/);
assert.doesNotMatch(actionButtons(approved),/data-action="renew"/);
assert.doesNotMatch(actionButtons({...approved,approval_renewable:'true'}),/data-action="renew"/);
const renewable = {...approved,state:'execution_failed',approval_renewable:true};
assert.match(actionButtons(renewable),/data-action="renew"/);
assert.equal(blockedAction('renew',renewable),'');
assert.match(blockedAction('renew',{...renewable,expired:true}),/no longer eligible/);
assert.match(blockedAction('execute',approved),/expired/);
state.filter='pending'; assert.equal(queueItems().length,0);
state.filter='expired'; assert.equal(queueItems().length,2);
state.session.user.role='planner';
assert.match(actionButtons({...fresh,valid_until:approved.valid_until}),/data-persona="approver"/);
assert.doesNotMatch(actionButtons(renewable),/data-action="renew"/);
state.session.user.role='approver';
assert.match(actionButtons({...approved,state:'executed'}),/data-persona="planner"/);
`;
vm.runInNewContext('let elapsed=0; const performance={now:()=>elapsed};\n'+source+checks, {
  assert, console, Date, Intl, Number, JSON, setTimeout:()=>0, clearTimeout() {},
  document:{querySelector:()=>element,querySelectorAll:()=>[],addEventListener(){}},
  window:{addEventListener(){},scrollTo(){}}, location:{hash:'#overview'}
});
"""
    result = subprocess.run(
        [node, "-e", harness, str(app)],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_reference_form_reenables_submit_after_success_and_request_failure() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for the optional browser presentation contract")
    app = Path(__file__).resolve().parents[1] / "src/nexus/portfolio/static/app.js"
    harness = r"""
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(process.argv[1], 'utf8').replace(/\ninitialize\(\);\s*$/, '\n');
let submit;
const button = {disabled:false,innerHTML:'',textContent:''};
const question = {value:'How are recommendations approved?'};
const answer = {innerHTML:''};
const generic = {addEventListener() {}, close() {}, focus() {}, classList:{add(){},remove(){}}};
const main = {addEventListener(type,callback){if(type==='submit')submit=callback;}};
const select = selector => ({'#main':main,'#copilot-question':question,
  '#copilot-answer':answer}[selector] || generic);
const sandbox = {assert,console,Date,Intl,Number,JSON,setTimeout:()=>0,clearTimeout(){},
  performance:{now:()=>0},document:{querySelector:select,querySelectorAll:()=>[],addEventListener(){}},
  window:{addEventListener(){},scrollTo(){}},location:{hash:'#copilot'},
  fetch:async()=>({ok:true,status:200,
    json:async()=>({answer:'Reference evidence.',citations:[]})})};
vm.runInNewContext(source,sandbox);
const event = {preventDefault(){},target:{id:'copilot-form',querySelector:()=>button}};
(async()=>{
  await submit(event);
  assert.equal(button.disabled,false);
  assert.match(button.innerHTML,/Ask the evidence/);
  assert.match(answer.innerHTML,/Reference evidence/);
  sandbox.fetch=async()=>{throw new Error('Expected isolated network failure');};
  await submit(event);
  assert.equal(button.disabled,false);
  assert.match(button.innerHTML,/Ask the evidence/);
})().catch(error=>{console.error(error);process.exitCode=1;});
"""
    result = subprocess.run(
        [node, "-e", harness, str(app)],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_decision_saves_personas_and_evidence_reconcile_with_api_responses() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for the optional browser presentation contract")
    app = Path(__file__).resolve().parents[1] / "src/nexus/portfolio/static/app.js"
    harness = r"""
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(process.argv[1], 'utf8').replace(/\ninitialize\(\);\s*$/, '\n');
const fixed = new Map();
let focused;
function element(selector) {
  if (!fixed.has(selector)) fixed.set(selector, {
    events: {}, disabled: false, hidden: true, open: false, innerHTML: '', textContent: '',
    addEventListener(type, handler) { this.events[type] = handler; },
    focus() { focused = selector; },
    close() { this.open = false; },
    showModal() { this.open = true; }
  });
  return fixed.get(selector);
}
const select = selector => {
  if (selector.startsWith('#main ')) {
    const html = element('#main').innerHTML;
    if (selector.endsWith('[data-refresh]') && html.includes('data-refresh')) {
      return element(selector);
    }
    if (selector.endsWith('button') && html.includes('data-action="execute"')) {
      return element(selector);
    }
    return null;
  }
  if (selector.startsWith('#navigation ') || selector.startsWith('[')) return null;
  return element(selector);
};
const sandbox = {
  assert, console, crypto: {randomUUID: () => 'fixture-key'}, performance: {now: () => 0},
  setTimeout: () => 0, clearTimeout() {}, getFocus: () => focused,
  document: {querySelector: select, querySelectorAll: () => [], addEventListener() {}},
  window: {addEventListener() {}}, location: {hash: '#decisions/fixture'}
};
const checks = `
(async () => {
  function respond(routes) {
    globalThis.fetch = async (path, options) => {
      const key = options.method + ' ' + path;
      assert.ok(Object.hasOwn(routes, key), key);
      const result = typeof routes[key] === 'function' ? routes[key](options) : routes[key];
      return {ok: result !== null, status: result === null ? 503 : 200,
        json: async () => result ?? {error: {message: 'Endpoint unavailable'}}};
    };
  }
  const base = '/api/recommendations/fixture';
  const pending = {id: 'fixture', state: 'pending', fingerprint: 'fixture-hash'};
  const approved = {...pending, state: 'approved'};
  const publish = 'POST ' + base + '/approve';
  const overview = 'GET /api/overview';
  const detail = 'GET ' + base;
  const save = async (item, action) => {
    state.action = {action, item, key: 'fixture-key'};
    $('#action-dialog').showModal();
    $('#action-note').value = 'Decision note';
    await $('#action-form').events.submit({preventDefault() {}});
  };
  const click = (selector, target = {}) => $('#main').events.click({
    target: {closest: value => value === selector ? target : null}
  });
  state.session = {user: {role: 'approver'}, csrf_token: 'fixture-csrf'};
  respond({[publish]: null});
  await save(pending, 'approve');
  assert.equal($('#action-dialog').open, true);
  assert.equal($('#dialog-error').textContent, 'Endpoint unavailable');
  assert.equal($('#action-note').value, 'Decision note');
  assert.equal($('#confirm-action').disabled, false);
  for (const reads of [{[overview]: null}, {[overview]: {recommendations: []}, [detail]: null}]) {
    respond({
      [publish]: approved,
      ...reads,
      [overview]: () => {
        assert.equal($('#notice').textContent, 'Recommendation approved.');
        return reads[overview];
      }
    });
    await save(pending, 'approve');
    assert.equal($('#action-dialog').open, false);
    assert.match($('#main').innerHTML, /Recommendation approved.*Refresh/s);
    assert.equal(getFocus(), '#main [data-refresh]');
  }
  respond({[publish]: approved, [overview]: {recommendations: [approved]}, [detail]: approved});
  await save(pending, 'approve');
  assert.equal($('#notice').textContent, 'Recommendation approved.');
  assert.equal(getFocus(), '#main [data-review-actions] button');

  const expiredApproval = {...approved, approval_expires_at: '2000-01-01T00:00:00Z'};
  respond({[detail]: expiredApproval});
  await openAction('renew', expiredApproval.id);
  assert.equal($('#action-dialog').open, false);
  assert.match($('#notice').textContent, /no longer eligible/);
  respond({
    [publish]: options => {
      assert.equal(options.headers['X-CSRF-Token'], 'fixture-csrf');
      assert.deepEqual(JSON.parse(options.body), {
        fingerprint: pending.fingerprint, note: 'Decision note'
      });
      return approved;
    },
    [overview]: {recommendations: [approved]},
    [detail]: approved
  });
  await save({...expiredApproval, approval_renewable: true}, 'renew');
  assert.equal($('#notice').textContent, 'Approval renewed.');
  assert.equal(getFocus(), '#main [data-review-actions] button');

  const control = $('#persona');
  control.value = 'auditor';
  respond({'POST /api/session': null});
  await control.events.change({currentTarget: control});
  assert.equal(control.value, 'approver');
  respond({
    'POST /api/session': options => {
      assert.equal(JSON.parse(options.body).persona, 'planner');
      return {user: {role: 'planner'}};
    },
    [overview]: null
  });
  await click('[data-persona]', {dataset: {persona: 'planner'}});
  assert.equal(control.value, 'planner');
  assert.equal($('#persona-avatar').textContent, 'PL');
  assert.match($('#main').innerHTML, /Now acting as Planner/);
  assert.doesNotMatch($('#main').innerHTML, /data-action=/);
  assert.equal(getFocus(), '#main [data-refresh]');

  location.hash = '#health';
  const noIncidents = {checks: [], alerts: []};
  const emptyAudit = {items: [], total: 0, integrity: 'verified'};
  const ready = {[overview]: {recommendations: []}, 'GET /api/learning': {}};
  respond({...ready, 'GET /api/health': null, 'GET /api/audit': emptyAudit});
  await click('[data-refresh]');
  assert.match($('#main').innerHTML, /Incident status is unavailable/);
  assert.doesNotMatch($('#main').innerHTML, /No active incident|No alert records/);
  assert.match($('#main').innerHTML, /Integrity verified/);
  assert.match($('#main').innerHTML, /data-export/);
  assert.match($('#notice').textContent, /unavailable/);
  respond({...ready, 'GET /api/health': noIncidents, 'GET /api/audit': null});
  await click('[data-refresh]');
  assert.match($('#main').innerHTML, /No active incident/);
  assert.match($('#main').innerHTML, /Decision audit is unavailable/);
  assert.doesNotMatch($('#main').innerHTML, /No decision events yet|data-export/);
  assert.match($('#notice').textContent, /unavailable/);
  respond({...ready, 'GET /api/health': noIncidents, 'GET /api/audit': emptyAudit});
  await click('[data-refresh]');
  assert.match($('#main').innerHTML, /No decision events yet/);
  assert.match($('#main').innerHTML, /data-export/);
  assert.equal($('#notice').textContent, 'Evidence refreshed.');
})()
`;
vm.runInNewContext(source + checks, sandbox).catch(error => {
  console.error(error);
  process.exitCode = 1;
});
"""
    result = subprocess.run(
        [node, "-e", harness, str(app)],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
