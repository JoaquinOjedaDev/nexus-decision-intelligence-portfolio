"""Execute expiry-aware browser presentation functions with a controlled server clock."""

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
assert.match(blockedAction('execute',approved),/expired/);
state.filter='pending'; assert.equal(queueItems().length,0);
state.filter='expired'; assert.equal(queueItems().length,2);
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
