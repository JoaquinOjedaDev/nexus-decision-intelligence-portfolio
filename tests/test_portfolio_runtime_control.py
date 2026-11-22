"""Replanning invalidates approvals, fences workers, and survives process restart."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from nexus.contracts import ActorRole, EventEnvelope
from nexus.policies.replanning import ReplanPolicy
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.runtime_control import RuntimeControl
from nexus.portfolio.store import PortfolioStore, StoreError


def seed(source: str = "a") -> dict[str, Any]:
    return {
        "metadata": {
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": source * 64,
        },
        "executable": True,
        "recommendations": [
            {
                "id": "rec-1",
                "sku_id": "SKU-001",
                "location_id": "DC-ARG",
                "recommended_quantity": 20,
                "expected_cost": 100,
                "created_by": "planner.r4a",
            }
        ],
    }


@pytest.fixture
def context(tmp_path: Path) -> tuple[RuntimeControl, list[float]]:
    now = [1_800_000_000.0]
    store = PortfolioStore(tmp_path, clock=lambda: now[0])
    store.ensure_seed(seed())
    return RuntimeControl(store, policy=ReplanPolicy(cooldown_seconds=30)), now


def actor(role: str = "planner") -> Actor:
    if role == "worker":
        return Actor("runtime-worker", "Runtime worker", ActorRole.SYSTEM, ("DC-ARG",))
    return resolve_persona(role, ("DC-ARG",))


def event(now: float, source: str = "b", kind: str = "inventory.adjusted") -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type=kind,
        source="local-fixture",
        subject_id="portfolio",
        occurred_at=datetime.fromtimestamp(now, UTC),
        ingested_at=datetime.fromtimestamp(now, UTC),
        idempotency_key=uuid4().hex,
        payload={"source_snapshot_sha256": source * 64},
    )


def test_material_event_invalidates_approved_plan_and_new_plan_needs_new_approval(
    context: tuple[RuntimeControl, list[float]],
) -> None:
    control, now = context
    store = control.store
    binding = store.recommendation("rec-1", actor())["fingerprint"]
    store.decide("rec-1", actor("approver"), "approve", binding)
    result = control.submit(event(now[0]), actor())
    assert result["invalidated"] == 1
    with pytest.raises(StoreError, match="current human approval"):
        store.execute("rec-1", actor("approver"), binding, "stale-order")
    job = control.claim(actor("worker"))
    assert job
    control.activate(job["id"], job["token"], seed("b"), actor("worker"))
    new = next(r for r in store.recommendations(actor()) if r["state"] == "pending")
    assert new["id"] != "rec-1" and new["fingerprint"] != binding
    with pytest.raises(StoreError, match="changed"):
        store.decide(new["id"], actor("approver"), "approve", binding)
    store.decide(new["id"], actor("approver"), "approve", new["fingerprint"])
    store.execute(new["id"], actor("approver"), new["fingerprint"], "new-order")
    assert store.metrics_snapshot()["effect_count"] == 1
    assert control.snapshot()["archived_generations"] == 1
    with store.connection() as db:
        historical = json.loads(db.execute("SELECT payload FROM runtime_generations").fetchone()[0])
    assert historical == seed()


def test_duplicate_and_coalesced_material_events_create_one_job(
    context: tuple[RuntimeControl, list[float]],
) -> None:
    control, now = context
    receipt = event(now[0])
    first = control.submit(receipt, actor())
    assert control.submit(receipt, actor())["duplicate"]
    second = control.submit(event(now[0], "c"), actor())
    assert second["job_id"] == first["job_id"]
    assert second["invalidated"] == 0
    assert len(control.snapshot()["jobs"]) == 1
    claimed = control.claim(actor("worker"))
    assert claimed and claimed["source_sha256"] == "c" * 64
    changed = receipt.model_copy(update={"payload": {"source_snapshot_sha256": "d" * 64}})
    with pytest.raises(StoreError, match="already bound"):
        control.submit(changed, actor())


def test_cooldown_does_not_delay_invalidation(context: tuple[RuntimeControl, list[float]]) -> None:
    control, now = context
    control.submit(event(now[0]), actor())
    job = control.claim(actor("worker"))
    assert job
    control.activate(job["id"], job["token"], seed("b"), actor("worker"))
    result = control.submit(event(now[0], "c"), actor())
    assert result["disposition"] == "defer_cooldown" and result["invalidated"] == 1
    assert control.claim(actor("worker")) is None
    now[0] += 30
    assert control.claim(actor("worker"))


def test_immediate_job_is_claimable_with_submicrosecond_clock(
    context: tuple[RuntimeControl, list[float]],
) -> None:
    control, now = context
    now[0] = 1_800_000_000.1234567
    control.submit(event(now[0]), actor())
    assert control.claim(actor("worker")) is not None


def test_worker_lease_recovery_fences_crashed_worker(
    context: tuple[RuntimeControl, list[float]],
) -> None:
    control, now = context
    control.submit(event(now[0]), actor())
    old = control.claim(actor("worker"), lease_seconds=10)
    assert old and control.claim(actor("worker")) is None
    now[0] += 10
    reopened = RuntimeControl(PortfolioStore(control.store.path.parent, clock=lambda: now[0]))
    new = reopened.claim(actor("worker"))
    assert new and new["attempts"] == 2 and old["token"] != new["token"]
    with pytest.raises(StoreError, match="lease"):
        control.activate(old["id"], old["token"], seed("b"), actor("worker"))
    reopened.activate(new["id"], new["token"], seed("b"), actor("worker"))
    assert reopened.snapshot()["jobs"][0]["state"] == "completed"
    with pytest.raises(StoreError):
        reopened.activate(new["id"], new["token"], seed("b"), actor("worker"))


def test_new_event_during_compute_blocks_old_activation(
    context: tuple[RuntimeControl, list[float]],
) -> None:
    control, now = context
    control.submit(event(now[0]), actor())
    old = control.claim(actor("worker"))
    assert old
    control.submit(event(now[0], "c"), actor())
    with pytest.raises(StoreError, match="newer material event"):
        control.activate(old["id"], old["token"], seed("b"), actor("worker"))
    control.fail(old["id"], old["token"], actor("worker"), reason="superseded_job")
    new = control.claim(actor("worker"))
    assert new
    control.activate(new["id"], new["token"], seed("c"), actor("worker"))


def test_nonmaterial_event_and_unchanged_source_do_not_replan(
    context: tuple[RuntimeControl, list[float]],
) -> None:
    control, now = context
    assert control.submit(event(now[0], kind="telemetry.observed"), actor())["job_id"] is None
    assert control.store.recommendation("rec-1", actor())["state"] == "pending"
    with pytest.raises(StoreError, match="changed source"):
        control.submit(event(now[0], "a"), actor())
    assert control.snapshot()["jobs"] == []


@pytest.mark.parametrize("claim_first", [False, True])
def test_source_reversion_supersedes_inflight_plan_without_restoring_approval(
    context: tuple[RuntimeControl, list[float]], claim_first: bool
) -> None:
    control, now = context
    original = control.store.recommendation("rec-1", actor())
    control.store.decide("rec-1", actor("approver"), "approve", original["fingerprint"])
    first = control.submit(event(now[0], "b"), actor())
    old_job = control.claim(actor("worker")) if claim_first else None

    reverted = control.submit(event(now[0], "a"), actor())
    assert reverted["job_id"] is not None
    assert reverted["invalidated"] == 0
    assert control.store.recommendation("rec-1", actor())["state"] == "invalidated"
    if old_job is not None:
        with pytest.raises(StoreError, match="newer material event"):
            control.activate(old_job["id"], old_job["token"], seed("b"), actor("worker"))
        control.fail(old_job["id"], old_job["token"], actor("worker"), reason="superseded_job")
    else:
        assert reverted["job_id"] == first["job_id"]
    replacement = control.claim(actor("worker"))
    assert replacement is not None and replacement["source_sha256"] == "a" * 64
    control.activate(replacement["id"], replacement["token"], seed("a"), actor("worker"))
    current = control.store.recommendation(f"{replacement['id']}-rec-1", actor())
    assert current["state"] == "pending"
    assert current["approved_by"] is None


@pytest.mark.parametrize("role", ["approver", "auditor", "worker"])
def test_only_global_planner_submits(
    role: str, context: tuple[RuntimeControl, list[float]]
) -> None:
    control, now = context
    with pytest.raises(StoreError, match="role"):
        control.submit(event(now[0]), actor(role))
    with pytest.raises(StoreError, match="locations"):
        control.submit(event(now[0]), replace(actor(), locations=()))


def test_worker_cannot_approve_or_execute(context: tuple[RuntimeControl, list[float]]) -> None:
    control, _ = context
    row = control.store.recommendation("rec-1", actor())
    with pytest.raises(StoreError):
        control.store.decide("rec-1", actor("worker"), "approve", row["fingerprint"])
    with pytest.raises(StoreError):
        control.store.execute("rec-1", actor("worker"), row["fingerprint"], "worker-order")
    with pytest.raises(StoreError):
        control.claim(actor("planner"))


def test_concurrent_event_delivery_and_claim_are_singleton(
    context: tuple[RuntimeControl, list[float]],
) -> None:
    control, now = context
    receipt = event(now[0])
    with ThreadPoolExecutor(max_workers=4) as pool:
        outputs = list(pool.map(lambda _: control.submit(receipt, actor()), range(8)))
        claims = list(pool.map(lambda _: control.claim(actor("worker")), range(8)))
    assert sum(not r["duplicate"] for r in outputs) == 1
    assert sum(r is not None for r in claims) == 1
    assert control.snapshot()["event_count"] == 1


def test_bad_or_failed_plan_leaves_old_generation_nonexecutable(
    context: tuple[RuntimeControl, list[float]],
) -> None:
    control, now = context
    control.submit(event(now[0]), actor())
    job = control.claim(actor("worker"))
    assert job
    for bad in (seed("c"), {**seed("b"), "executable": False}):
        with pytest.raises(StoreError):
            control.activate(job["id"], job["token"], bad, actor("worker"))
    control.fail(job["id"], job["token"], actor("worker"), reason="solver_failed")
    assert control.store.recommendation("rec-1", actor())["state"] == "invalidated"
    assert control.store.metrics_snapshot()["effect_count"] == 0
    assert control.snapshot()["jobs"][0]["state"] == "failed"


def test_frozen_state_and_future_event_are_rejected(
    tmp_path: Path, context: tuple[RuntimeControl, list[float]]
) -> None:
    control, now = context
    with pytest.raises(StoreError, match="future"):
        control.submit(event(now[0] + 1), actor())
    frozen = seed()
    frozen["metadata"]["pipeline_mode"] = "frozen"
    store = PortfolioStore(tmp_path / "frozen", clock=lambda: now[0])
    store.ensure_seed(frozen)
    with pytest.raises(StoreError, match="Frozen"):
        RuntimeControl(store).submit(event(now[0]), actor())


@pytest.mark.parametrize("target", ["runtime_events", "runtime_jobs"])
def test_runtime_journal_tamper_and_deletion_fail_closed(
    context: tuple[RuntimeControl, list[float]], target: str
) -> None:
    control, now = context
    control.submit(event(now[0]), actor())
    with control.store.transaction() as db:
        db.execute(f"DELETE FROM {target}")
    with pytest.raises(StoreError, match="changed"):
        control.snapshot()
    with pytest.raises(StoreError, match="changed"):
        control.claim(actor("worker"))


def test_health_aggregates_all_jobs_and_resolves_superseded_failures(
    context: tuple[RuntimeControl, list[float]],
) -> None:
    control, now = context
    for _ in range(51):
        control.submit(event(now[0]), actor())
        job = control.claim(actor("worker"))
        assert job
        control.fail(job["id"], job["token"], actor("worker"), reason="pipeline_failed")
    snapshot = control.snapshot()
    assert len(snapshot["jobs"]) == 50
    assert snapshot["operational"]["failed_unresolved"] == 51
    control.submit(event(now[0]), actor())
    job = control.claim(actor("worker"))
    assert job
    control.activate(job["id"], job["token"], seed("b"), actor("worker"))
    snapshot = control.snapshot()
    assert snapshot["operational"]["failed_unresolved"] == 0
    assert snapshot["operational"]["failed_historical"] == 51
