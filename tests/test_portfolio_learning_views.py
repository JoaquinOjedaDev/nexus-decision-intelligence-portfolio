"""Source-scoped learning reports never become approval or deployment authority."""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("fastapi")
from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.learning_views import _summary, attach_learning_report, read_learning_report
from nexus.portfolio.runtime_control import RuntimeControl
from nexus.portfolio.store import PortfolioStore, StoreError, canonical
from nexus.portfolio.training_pipeline import LearningRun


def seed(source: str = "a") -> dict[str, Any]:
    return {
        "metadata": {
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": source * 64,
        },
        "executable": True,
        "recommendations": [
            {
                "id": f"rec-{index}",
                "sku_id": f"SKU-{index}",
                "location_id": location,
                "recommended_quantity": 20,
                "expected_cost": 100,
                "created_by": "planner.r4a",
            }
            for index, location in enumerate(("NORTH", "SOUTH", "NORTH"))
        ],
    }


def run(source: str = "a") -> LearningRun:
    return LearningRun(
        {
            "contract_version": "nexus.runtime-learning-result.v1",
            "bindings": {
                "source_snapshot_sha256": source * 64,
                "snapshot_sha256": "b" * 64,
                "plan_sha256": "c" * 64,
                "code_sha256": "d" * 64,
                "finalization_sha256": None,
            },
            "status": "ABSTAIN_INSUFFICIENT_EVIDENCE",
            "training_performed": False,
            "classification": "synthetic",
            "reason": "split_support_not_met",
            "snapshot": {"labels": [{"secret_source_row": "NOT_FOR_BROWSER"}]},
            "splits": {"training": {"rows": 20}, "calibration": {"rows": 4}, "final": {"rows": 2}},
        },
        None,
        None,
    )


@pytest.fixture
def setup(tmp_path: Path) -> tuple[PortfolioStore, Actor, Actor]:
    store = PortfolioStore(tmp_path, clock=lambda: 1_900_000_000.0)
    store.ensure_seed(seed())
    RuntimeControl(store)
    actor = resolve_persona("auditor", ("NORTH", "SOUTH"))
    registrar = Actor(
        "local-registrar", "Trusted local registrar", ActorRole.SYSTEM, actor.locations
    )
    return store, actor, registrar


def test_verified_attachment_persists_without_changing_three_pending_decisions(
    setup: tuple[PortfolioStore, Actor, Actor],
) -> None:
    store, actor, registrar = setup
    before = store.overview_snapshot(actor)
    assert read_learning_report(store, actor) == {"status": "not_run", "report": None}
    result = attach_learning_report(store, run(), actor=registrar)
    assert attach_learning_report(store, run(), actor=registrar) == result
    reopened = PortfolioStore(store.path.parent, clock=store.clock)
    assert read_learning_report(reopened, actor) == result
    assert reopened.overview_snapshot(actor) == before
    assert [item["state"] for item in before["recommendations"]] == ["pending"] * 3
    report = result["report"]
    assert report["splits"] == {"training": 20, "calibration": 4, "final": 2}
    assert report["metrics"]["candidate_wis"] is None
    assert report["trained"] is report["deployed"] is report["human_approval"] is False
    assert "NOT_FOR_BROWSER" not in canonical(result)
    assert "snapshot" not in report


def test_role_scope_and_source_binding_fail_closed(
    setup: tuple[PortfolioStore, Actor, Actor],
) -> None:
    store, actor, registrar = setup
    for denied in (
        actor,
        resolve_persona("planner", actor.locations),
        resolve_persona("approver", actor.locations),
    ):
        with pytest.raises(StoreError) as error:
            attach_learning_report(store, run(), actor=denied)
        assert error.value.code == "role_denied"
    with pytest.raises(StoreError, match="every reported location"):
        attach_learning_report(store, run(), actor=replace(registrar, locations=("NORTH",)))
    with pytest.raises(StoreError, match="every reported location"):
        read_learning_report(store, replace(actor, locations=("NORTH",)))
    with pytest.raises(StoreError, match="different source"):
        attach_learning_report(store, run("f"), actor=registrar)
    assert read_learning_report(store, actor)["status"] == "not_run"


def test_source_change_retains_stale_report_and_new_report_can_replace_pointer(
    setup: tuple[PortfolioStore, Actor, Actor],
) -> None:
    store, actor, registrar = setup
    original = attach_learning_report(store, run(), actor=registrar)
    control = RuntimeControl(store)
    now = datetime.fromtimestamp(store.clock(), UTC)
    control.submit(
        EventEnvelope(
            event_id=uuid4(),
            event_type="inventory.adjusted",
            source="fixture",
            subject_id="portfolio",
            occurred_at=now,
            ingested_at=now,
            idempotency_key=uuid4().hex,
            payload={"source_snapshot_sha256": "f" * 64},
        ),
        resolve_persona("planner", actor.locations),
    )
    worker = replace(registrar, actor_id="runtime-worker")
    job = control.claim(worker)
    assert job is not None
    control.activate(job["id"], job["token"], seed("f"), worker)
    stale = read_learning_report(store, actor)
    assert stale["status"] == "stale"
    assert stale["report"] == original["report"]
    current = attach_learning_report(store, run("f"), actor=registrar)
    assert current["status"] == "current"
    with store.connection() as db:
        assert (
            db.execute(
                "SELECT count(*) FROM metadata WHERE key LIKE 'learning_report:%'"
            ).fetchone()[0]
            == 2
        )


@pytest.mark.parametrize("damage", ["body", "pointer", "journal", "receipt"])
def test_changed_learning_or_governing_evidence_is_rejected(
    setup: tuple[PortfolioStore, Actor, Actor],
    damage: str,
) -> None:
    store, actor, registrar = setup
    evaluated = run()
    attach_learning_report(store, evaluated, actor=registrar)
    if damage == "receipt":
        evaluated.receipt["status"] = "MADE_UP_PASS"
        with pytest.raises(ValueError, match="mutated"):
            attach_learning_report(store, evaluated, actor=registrar)
        return
    with store.transaction() as db:
        if damage == "body":
            db.execute("UPDATE metadata SET value='{}' WHERE key LIKE 'learning_report:%'")
        elif damage == "pointer":
            db.execute(
                "UPDATE metadata SET value=? WHERE key='learning_report_active'", ("f" * 64,)
            )
        else:
            db.execute("UPDATE metadata SET value='changed' WHERE key='runtime_state_hash'")
    with pytest.raises(StoreError) as error:
        read_learning_report(store, actor)
    assert error.value.code == "integrity_error"


def test_http_requires_session_and_has_no_attachment_method(
    setup: tuple[PortfolioStore, Actor, Actor],
) -> None:
    TestClient = pytest.importorskip("fastapi.testclient").TestClient
    pytest.importorskip("jwt")
    from nexus.portfolio.api import create_app

    store, _, registrar = setup
    app = create_app(store.path.parent, pipeline="runtime")
    app.state.portfolio_store = store
    with TestClient(app) as client:
        assert client.get("/api/learning").status_code == 401
        assert client.post("/api/session", json={"persona": "auditor"}).status_code == 200
        assert client.get("/api/learning").json()["status"] == "not_run"
        attach_learning_report(store, run(), actor=registrar)
        assert client.get("/api/learning").json()["report"]["trained"] is False
        assert client.post("/api/learning", json={"path": "untrusted.json"}).status_code == 405


def test_browser_learning_states_do_not_claim_training_or_deployment() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for optional browser presentation verification")
    app = Path(__file__).resolve().parents[1] / "src/nexus/portfolio/static/app.js"
    checks = r"""
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const source=fs.readFileSync(process.argv[1],'utf8').replace(/\ninitialize\(\);\s*$/,'\n');
const element={addEventListener(){},close(){},focus(){}};
const sandbox={assert,console,Date,Intl,Number,JSON,setTimeout:()=>0,clearTimeout(){},
  performance:{now:()=>0},document:{querySelector:()=>element,querySelectorAll:()=>[],addEventListener(){}},
  window:{addEventListener(){},scrollTo(){}},location:{hash:'#health'}};
vm.runInNewContext(source+`
assert.match(renderLearning(),/unavailable/);
state.learning={status:'not_run',report:null};
assert.match(renderLearning(),/Not run/);
assert.doesNotMatch(renderLearning(),/0%|Candidate rejected/);
state.learning={status:'current',summary_sha256:'summary',report:{trained:true,
  evaluation_status:'REJECTED_GATE_FAILURE',classification:'synthetic',
  splits:{training:144,calibration:40,final:20},metrics:{candidate_coverage80:'0.5',candidate_coverage95:'0.75',candidate_wis:'2.345',baseline_wis:'1.2'},
  quality_checks:{prediction_reload_parity:true,coverage80_within_five_percentage_points:false,coverage95_within_five_percentage_points:false},
  bindings:{source_snapshot_sha256:'<script>'},receipt_sha256:'receipt'}};
let html=renderLearning();
assert.match(html,/Source matched/);assert.match(html,/Candidate rejected/);
assert.match(html,/50%/);assert.ok(html.includes('144 / 40 / 20'));
assert.ok(html.includes('50% / 75%'));assert.ok(html.includes('Failed / Failed'));
assert.match(html,/Not deployed · No human approval/);
assert.match(html,/Failed/);assert.match(html,/&lt;script&gt;/);
assert.doesNotMatch(html,/<button|<script>|data-action/);
state.learning.status='stale';
assert.match(renderLearning(),/historical, not current/);
state.learning.report.trained=false;
state.learning.report.evaluation_status='ABSTAIN_INSUFFICIENT_EVIDENCE';
assert.match(renderLearning(),/Training abstained/);
assert.doesNotMatch(renderLearning(),/Candidate rejected/);
`,sandbox);
"""
    result = subprocess.run(
        [node, "-e", checks, str(app)], check=False, capture_output=True, text=True, timeout=20
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_summary_retains_both_coverage_levels_without_deployment_authority() -> None:
    receipt = run().receipt
    receipt.update(
        training_performed=True,
        status="REJECTED_GATE_FAILURE",
        metrics={"candidate_coverage80": "0.71", "candidate_coverage95": "0.96"},
        quality_checks={
            "coverage80_within_five_percentage_points": False,
            "coverage95_within_five_percentage_points": True,
        },
    )
    # Projection-only fixture. Actual candidate/artifact verification is a separate boundary.
    summary = _summary(receipt, "e" * 64, ["NORTH", "SOUTH"])
    assert summary["metrics"]["candidate_coverage80"] == "0.71"
    assert summary["metrics"]["candidate_coverage95"] == "0.96"
    assert summary["quality_checks"]["coverage80_within_five_percentage_points"] is False
    assert summary["quality_checks"]["coverage95_within_five_percentage_points"] is True
    assert summary["evaluation_status"] == "REJECTED_GATE_FAILURE"
    assert summary["deployed"] is summary["human_approval"] is False
