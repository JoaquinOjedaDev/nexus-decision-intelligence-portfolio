"""Opt-in native PostgreSQL v4 control and multi-generation workflow evidence.

All mutations use the shared identity-checked, non-default task cluster fixture.
Each case retains a random schema; these tests never start/stop services or erase
data. Constructed checksum-bound seeds test storage, not model/scientific quality.
"""

from __future__ import annotations

import copy
import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster

from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.postgres_backend import PostgresBackend, PostgresBackendError
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.postgres_workflow import PostgresWorkflowStore
from nexus.portfolio.store import DEMO_MATURITY_SECONDS, StoreError, canonical, digest

pytestmark = pytest.mark.portfolio_service
LOCATIONS = ("WH-NORTH", "WH-SOUTH", "WH-FORECAST")


def actor(role: str = "planner", locations: tuple[str, ...] = LOCATIONS) -> Actor:
    if role == "worker":
        return Actor("runtime-worker", "Runtime worker", ActorRole.SYSTEM, locations)
    return resolve_persona(role, locations)


def source_seed(
    root: Path, source: str = "a", *, extra_location: str | None = None
) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    evidence = canonical({"constructed": True, "source": source, "extra": extra_location})
    (root / "fixture.json").write_text(evidence, encoding="utf-8")
    rows = [
        {
            "id": f"rec-{index}",
            "sku_id": f"SKU-{index:03}",
            "location_id": location,
            "recommended_quantity": 24 + index,
            "unit_cost": 3.5,
            "expected_cost": (24 + index) * 3.5,
            "created_by": "planner.r4a",
            "observed_demand": 20,
            "outcome_provenance": "synthetic_scenario_replay",
            "outcome_scenario_seed": 4201,
            "scientific_decision_time": "2025-01-01T00:00:00+00:00",
        }
        for index, location in enumerate(LOCATIONS[:2], 1)
    ]
    if extra_location:
        rows.append({**rows[0], "id": "rec-extra", "location_id": extra_location})
    return {
        "metadata": {
            "synthetic": True,
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": source * 64,
        },
        "executable": True,
        "recommendations": rows,
        "forecasts": [{"sku_id": "FORECAST-ONLY", "location_id": LOCATIONS[2]}],
        "scenarios": [{"name": f"constructed-{source}"}],
        "metrics": {},
        "sources": [],
        "lineage": [{"fixture_source": source}],
        "artifacts": [
            {"path": "fixture.json", "file_sha256": hashlib.sha256(evidence.encode()).hexdigest()}
        ],
    }


@pytest.fixture
def clock() -> list[float]:
    return [1_800_000_000.1234567]


@pytest.fixture
def runtime(cluster: Cluster, tmp_path: Path, clock: list[float]) -> PostgresRuntimeStore:
    root = tmp_path / "runtime"
    seed = source_seed(root / "evidence" / "initial")
    store = PostgresRuntimeStore(
        cluster.backend(root / "evidence" / "initial").config,
        runtime_root=root,
        clock=lambda: clock[0],
    )
    cluster.migrate(store)
    store.ensure_seed(seed)
    return store


def event(now: float, source: str = "b", kind: str = "inventory.adjusted") -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type=kind,
        source="constructed-native-test",
        subject_id="portfolio",
        occurred_at=datetime.fromtimestamp(now, UTC),
        ingested_at=datetime.fromtimestamp(now, UTC),
        idempotency_key=uuid4().hex,
        payload={"source_snapshot_sha256": source * 64},
    )


def binding(store: PostgresRuntimeStore, identity: str = "rec-1") -> str:
    return str(store.recommendation(identity, actor())["fingerprint"])


def claimed(store: PostgresRuntimeStore, clock: list[float], source: str = "b") -> dict[str, Any]:
    store.control.submit(event(clock[0], source), actor())
    job = store.control.claim(actor("worker"))
    assert job is not None
    return job


def candidate(
    store: PostgresRuntimeStore,
    job: dict[str, Any],
    source: str = "b",
    *,
    extra_location: str | None = None,
) -> tuple[dict[str, Any], Path]:
    directory = store.control.attempt_evidence_dir(job["id"], job["token"], actor("worker"))
    return source_seed(directory, source, extra_location=extra_location), directory


def activate(store: PostgresRuntimeStore, job: dict[str, Any], source: str = "b") -> dict[str, Any]:
    seed, directory = candidate(store, job, source)
    return store.control.activate(
        job["id"], job["token"], seed, actor("worker"), evidence_dir=directory
    )


def current(store: PostgresRuntimeStore) -> list[dict[str, Any]]:
    return list(store.overview_snapshot(actor())["current_recommendations"])


def test_populated_v3_upgrade_preserves_workflow_and_backfills_frozen_session_scope(
    cluster: Cluster,
    tmp_path: Path,
    clock: list[float],
) -> None:
    root = tmp_path / "upgrade"
    seed = source_seed(root / "initial")
    old = PostgresWorkflowStore(cluster.backend(root / "initial").config, clock=lambda: clock[0])
    cluster.migrate(old)
    old.ensure_seed(seed)
    token, _ = old.create_session("planner")
    previous = old.session(token)
    assert previous is not None
    original = old.snapshot()
    ledger = cluster.query(old, "SELECT * FROM {schema}.schema_migrations ORDER BY version")
    store = PostgresRuntimeStore(old.config, runtime_root=root, clock=lambda: clock[0])
    cluster.migrate(store)
    retained = store.snapshot()
    for key in ("seed", "seed_sha256", "recommendations", "events", "audit_head"):
        assert retained[key] == original[key]
    assert (
        cluster.query(
            store, "SELECT * FROM {schema}.schema_migrations WHERE version<4 ORDER BY version"
        )
        == ledger
    )
    assert store.session(token) == previous
    scopes = cluster.query(store, "SELECT locations FROM {schema}.session_scopes")
    assert set(json.loads(scopes[0][0])) == set(previous[0].locations)
    stable = store.snapshot()
    cluster.migrate(store)
    store.ensure_seed(store.seed())
    assert store.snapshot() == stable
    for obsolete in (PostgresBackend(store.config), PostgresWorkflowStore(store.config)):
        with pytest.raises(PostgresBackendError):
            obsolete.snapshot()


def test_runtime_root_drift_is_refused_without_adopting_existing_state(
    runtime: PostgresRuntimeStore,
    tmp_path: Path,
    clock: list[float],
) -> None:
    before = runtime.snapshot()
    wrong = PostgresRuntimeStore(runtime.config, runtime_root=tmp_path, clock=lambda: clock[0])
    with pytest.raises((StoreError, PostgresBackendError)):
        wrong.snapshot()
    assert runtime.snapshot() == before


def test_duplicate_coalesced_events_and_conflicting_identity(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    receipt = event(clock[0])
    first = runtime.control.submit(receipt, actor())
    assert first["invalidated"] == 2 and not first["duplicate"]
    stable = runtime.snapshot()
    assert runtime.control.submit(receipt, actor())["duplicate"]
    assert runtime.snapshot() == stable
    next_event = runtime.control.submit(event(clock[0], "c"), actor())
    assert next_event["job_id"] == first["job_id"] and next_event["invalidated"] == 0
    job = runtime.control.claim(actor("worker"))
    assert job and job["source_sha256"] == "c" * 64
    changed = receipt.model_copy(update={"payload": {"source_snapshot_sha256": "d" * 64}})
    with pytest.raises(StoreError) as denied:
        runtime.control.submit(changed, actor())
    assert denied.value.code == "event_conflict"
    status = runtime.control.snapshot(actor("auditor"))
    assert status["event_count"] == 2 and len(status["jobs"]) == 1


def test_nonmaterial_event_is_retained_without_invalidating_or_replanning(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    result = runtime.control.submit(event(clock[0], kind="telemetry.observed"), actor())
    assert result["job_id"] is None and result["invalidated"] == 0
    assert runtime.recommendation("rec-1", actor())["state"] == "pending"
    with pytest.raises(StoreError) as denied:
        runtime.control.submit(event(clock[0], "a"), actor())
    assert denied.value.code == "unchanged_source"
    assert runtime.control.snapshot(actor("auditor"))["jobs"] == []


@pytest.mark.parametrize("intervening_state", ["queued", "running", "failed"])
def test_material_source_reversion_fences_intervening_work_and_requires_new_approval(
    runtime: PostgresRuntimeStore,
    clock: list[float],
    intervening_state: str,
) -> None:
    original = binding(runtime)
    runtime.decide("rec-1", actor("approver"), "approve", original)
    intervening = runtime.control.submit(event(clock[0], "b"), actor())
    old_job = None
    old_seed = None
    old_directory = None
    if intervening_state != "queued":
        old_job = runtime.control.claim(actor("worker"))
        assert old_job is not None
        old_seed, old_directory = candidate(runtime, old_job, "b")
        if intervening_state == "failed":
            runtime.control.fail(
                old_job["id"], old_job["token"], actor("worker"), reason="controlled_failure"
            )
    clock[0] += 1
    reverted = runtime.control.submit(event(clock[0], "a"), actor())
    assert not reverted["duplicate"] and reverted["job_id"] is not None
    assert runtime.control.snapshot(actor("auditor"))["event_count"] == 2
    if intervening_state == "queued":
        assert reverted["job_id"] == intervening["job_id"]
    else:
        assert old_job is not None and old_seed is not None and old_directory is not None
        with pytest.raises(StoreError):
            runtime.control.activate(
                old_job["id"],
                old_job["token"],
                old_seed,
                actor("worker"),
                evidence_dir=old_directory,
            )
        if intervening_state == "running":
            runtime.control.fail(
                old_job["id"], old_job["token"], actor("worker"), reason="superseded_job"
            )
    fresh = runtime.control.claim(actor("worker"))
    assert fresh is not None and fresh["source_sha256"] == "a" * 64
    assert fresh["id"] == reverted["job_id"]
    if intervening_state == "queued":
        stale_seed, directory = candidate(runtime, fresh, "b")
        with pytest.raises(StoreError) as refused:
            runtime.control.activate(
                fresh["id"], fresh["token"], stale_seed, actor("worker"), evidence_dir=directory
            )
        assert refused.value.code == "source_mismatch"
    activate(runtime, fresh, "a")
    assert runtime.seed()["metadata"]["source_snapshot_sha256"] == "a" * 64
    assert runtime.recommendation("rec-1", actor())["state"] == "invalidated"
    replacement = current(runtime)[0]
    assert replacement["state"] == "pending" and replacement["fingerprint"] != original
    with pytest.raises(StoreError):
        runtime.execute(
            replacement["id"], actor("approver"), replacement["fingerprint"], "unapproved-reversion"
        )
    assert runtime.metrics_snapshot(actor("auditor"))["effect_count"] == 0
    runtime.decide(replacement["id"], actor("approver"), "approve", replacement["fingerprint"])
    executed = runtime.execute(
        replacement["id"], actor("approver"), replacement["fingerprint"], "approved-reversion"
    )
    assert executed["state"] == "executed"


def test_concurrent_first_v4_migrations_retain_one_ledger_and_one_seed(
    cluster: Cluster,
    tmp_path: Path,
    clock: list[float],
) -> None:
    root = tmp_path / "concurrent-runtime"
    seed = source_seed(root / "initial")
    config = cluster.backend(root / "initial").config
    barrier = threading.Barrier(4)

    def initialize(_: int) -> dict[str, Any]:
        store = PostgresRuntimeStore(config, runtime_root=root, clock=lambda: clock[0])
        barrier.wait(timeout=20)
        result = cluster.migrate(store)
        store.ensure_seed(seed)
        return result

    with ThreadPoolExecutor(max_workers=4) as pool:
        receipts = list(pool.map(initialize, range(4)))
    assert len(receipts) == 4
    store = PostgresRuntimeStore(config, runtime_root=root, clock=lambda: clock[0])
    assert cluster.query(
        store, "SELECT version FROM {schema}.schema_migrations ORDER BY version"
    ) == [(2,), (3,), (4,)]
    assert store.seed() == seed
    assert len(store.snapshot()["events"]) == len(seed["recommendations"])


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"source_snapshot_sha256": "wrong"},
        {"source_snapshot_sha256": "B" * 64},
        {"source_snapshot_sha256": "b" * 64, "extra": True},
    ],
)
def test_event_payload_validation_is_atomic(
    runtime: PostgresRuntimeStore, clock: list[float], payload: dict[str, Any]
) -> None:
    before = runtime.snapshot()
    altered = event(clock[0]).model_copy(update={"payload": payload})
    with pytest.raises(StoreError):
        runtime.control.submit(altered, actor())
    assert runtime.snapshot() == before


@pytest.mark.parametrize("field", ["occurred_at", "ingested_at"])
def test_future_event_is_refused_without_changes(
    runtime: PostgresRuntimeStore, clock: list[float], field: str
) -> None:
    before = runtime.snapshot()
    altered = event(clock[0]).model_copy(update={field: datetime.fromtimestamp(clock[0] + 1, UTC)})
    with pytest.raises((StoreError, ValueError)):
        runtime.control.submit(altered, actor())
    assert runtime.snapshot() == before


def test_material_change_cancels_failed_outbox_without_erasing_attempt_evidence(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    fingerprint = binding(runtime)
    runtime.decide("rec-1", actor("approver"), "approve", fingerprint)
    runtime.execute("rec-1", actor("approver"), fingerprint, "failed-order", simulate_failure=True)
    old = runtime.snapshot()["outbox"][0]
    runtime.control.submit(event(clock[0]), actor())
    new = runtime.snapshot()["outbox"][0]
    assert new["state"] == "cancelled"
    for key in ("attempts", "last_error", "fingerprint", "operation_id"):
        assert new[key] == old[key]
    with pytest.raises(StoreError):
        runtime.execute("rec-1", actor("approver"), fingerprint, "failed-order")
    assert runtime.metrics_snapshot(actor("auditor"))["effect_count"] == 0


def test_default_cooldown_preserves_immediate_invalidation(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    job = claimed(runtime, clock)
    activate(runtime, job)
    result = runtime.control.submit(event(clock[0], "c"), actor())
    assert result["disposition"] == "defer_cooldown" and result["invalidated"] == 2
    assert runtime.control.claim(actor("worker")) is None
    clock[0] += 1799.999
    assert runtime.control.claim(actor("worker")) is None
    clock[0] += 0.0011
    assert runtime.control.claim(actor("worker")) is not None


def test_exact_lease_expiry_reclaim_uses_fresh_token_and_isolated_attempt_path(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    runtime.control.submit(event(clock[0]), actor())
    old = runtime.control.claim(actor("worker"), lease_seconds=10)
    assert old is not None
    old_path = runtime.control.attempt_evidence_dir(old["id"], old["token"], actor("worker"))
    assert not old_path.exists() and old["token"] not in str(old_path)
    assert old_path.name == f"attempt-1-{hashlib.sha256(old['token'].encode()).hexdigest()[:16]}"
    clock[0] += 9.999
    assert runtime.control.claim(actor("worker")) is None
    clock[0] = old["lease_until"]
    with pytest.raises(StoreError):
        runtime.control.attempt_evidence_dir(old["id"], old["token"], actor("worker"))
    reopened = PostgresRuntimeStore(
        runtime.config, runtime_root=runtime.runtime_root, clock=lambda: clock[0]
    )
    new = reopened.control.claim(actor("worker"))
    assert new and new["id"] == old["id"] and new["attempts"] == 2 and new["token"] != old["token"]
    new_path = reopened.control.attempt_evidence_dir(new["id"], new["token"], actor("worker"))
    assert new_path != old_path and not new_path.exists()
    for operation in (
        lambda: runtime.control.fail(
            old["id"], old["token"], actor("worker"), reason="stale_attempt"
        ),
        lambda: runtime.control.attempt_evidence_dir(old["id"], old["token"], actor("worker")),
    ):
        with pytest.raises(StoreError):
            operation()
    seed = source_seed(old_path, "b")
    with pytest.raises(StoreError):
        reopened.control.activate(
            new["id"], new["token"], seed, actor("worker"), evidence_dir=old_path
        )
    activate(reopened, new)
    assert reopened.control.snapshot(actor("auditor"))["jobs"][0]["state"] == "completed"


def test_new_material_event_fences_inflight_plan_and_failure_retains_classification(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    old = claimed(runtime, clock)
    old_seed, old_path = candidate(runtime, old)
    runtime.control.submit(event(clock[0], "c"), actor())
    with pytest.raises(StoreError):
        runtime.control.activate(
            old["id"], old["token"], old_seed, actor("worker"), evidence_dir=old_path
        )
    runtime.control.fail(old["id"], old["token"], actor("worker"), reason="superseded_job")
    newer = runtime.control.claim(actor("worker"))
    assert newer and newer["id"] != old["id"] and newer["source_sha256"] == "c" * 64
    activate(runtime, newer, "c")
    jobs = {row["id"]: row for row in runtime.control.snapshot(actor("auditor"))["jobs"]}
    assert jobs[old["id"]]["error"] == "superseded_job" and jobs[old["id"]]["state"] == "failed"
    assert jobs[newer["id"]]["state"] == "completed"


@pytest.mark.parametrize("executable", [False, None, 1, "true"])
def test_only_literal_executable_true_is_admitted(
    runtime: PostgresRuntimeStore, clock: list[float], executable: Any
) -> None:
    job = claimed(runtime, clock)
    seed, directory = candidate(runtime, job)
    seed["executable"] = executable
    before = runtime.snapshot()
    with pytest.raises(StoreError):
        runtime.control.activate(
            job["id"], job["token"], seed, actor("worker"), evidence_dir=directory
        )
    assert runtime.snapshot() == before


@pytest.mark.parametrize(
    "alteration",
    [
        "wrong_source",
        "duplicate_owner",
        "missing_artifacts",
        "missing_mode",
        "forged_scientific_contract",
    ],
)
def test_invalid_replacement_cannot_publish_partial_generation(
    runtime: PostgresRuntimeStore, clock: list[float], alteration: str
) -> None:
    job = claimed(runtime, clock)
    seed, directory = candidate(runtime, job)
    if alteration == "wrong_source":
        seed["metadata"]["source_snapshot_sha256"] = "c" * 64
    elif alteration == "duplicate_owner":
        seed["recommendations"].append(copy.deepcopy(seed["recommendations"][0]))
    elif alteration == "missing_artifacts":
        seed["artifacts"] = []
    elif alteration == "missing_mode":
        seed["metadata"].pop("pipeline_mode")
    else:
        seed["contract_version"] = "nexus.portfolio.runtime-seed.v1"
    before = runtime.snapshot()
    with pytest.raises((StoreError, PostgresBackendError, ValueError)):
        runtime.control.activate(
            job["id"], job["token"], seed, actor("worker"), evidence_dir=directory
        )
    assert runtime.snapshot() == before


def test_declared_artifact_change_after_build_is_rejected(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    job = claimed(runtime, clock)
    seed, directory = candidate(runtime, job)
    (directory / "fixture.json").write_text("changed after build", encoding="utf-8")
    before = runtime.snapshot()
    with pytest.raises((StoreError, PostgresBackendError)):
        runtime.control.activate(
            job["id"], job["token"], seed, actor("worker"), evidence_dir=directory
        )
    assert runtime.snapshot() == before


def test_activation_preserves_archived_execution_and_requires_fresh_human_approval(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    original_seed = runtime.seed()
    fingerprint = binding(runtime)
    runtime.decide("rec-1", actor("approver"), "approve", fingerprint)
    old = runtime.execute("rec-1", actor("approver"), fingerprint, "old-order")
    other = binding(runtime, "rec-2")
    runtime.decide("rec-2", actor("approver"), "approve", other)
    job = claimed(runtime, clock)
    result = activate(runtime, job)
    assert result is not None
    unchanged = runtime.snapshot()
    runtime.ensure_seed(runtime.seed())
    assert runtime.snapshot() == unchanged
    archive = runtime.recommendation_context("rec-1", actor())
    assert archive["generation"]["is_current"] is False
    assert archive["generation"]["seed_sha256"] == digest(original_seed)
    assert archive["execution_ref"] == old["execution_ref"]
    assert archive["scenarios"] == original_seed["scenarios"]
    assert runtime.recommendation("rec-2", actor())["state"] == "invalidated"
    new = current(runtime)[0]
    assert new["id"].startswith(job["id"]) and new["state"] == "pending"
    with pytest.raises(StoreError):
        runtime.execute(new["id"], actor("approver"), new["fingerprint"], "no-approval")
    with pytest.raises(StoreError):
        runtime.decide(new["id"], actor("approver"), "approve", fingerprint)
    with pytest.raises(StoreError):
        runtime.decide(new["id"], actor("worker"), "approve", new["fingerprint"])
    runtime.decide(new["id"], actor("approver"), "approve", new["fingerprint"])
    runtime.execute(new["id"], actor("approver"), new["fingerprint"], "new-order")
    clock[0] += DEMO_MATURITY_SECONDS
    for identity, fp in (("rec-1", fingerprint), (new["id"], new["fingerprint"])):
        assert runtime.observe(identity, actor(), fp)["state"] == "outcome_observed"
    assert runtime.metrics_snapshot(actor("auditor"))["effect_count"] == 2
    assert runtime.control.snapshot(actor("auditor"))["archived_generations"] == 1
    repeated = runtime.snapshot()
    reopened = PostgresRuntimeStore(
        runtime.config, runtime_root=runtime.runtime_root, clock=lambda: clock[0]
    )
    assert reopened.snapshot() == repeated
    assert (
        reopened.execute("rec-1", actor("approver"), fingerprint, "old-order")["execution_ref"]
        == old["execution_ref"]
    )


def test_frozen_session_scope_does_not_expand_with_new_generation(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    token, _ = runtime.create_session("planner")
    old_session = runtime.session(token)
    assert old_session is not None and set(old_session[0].locations) == set(LOCATIONS)
    job = claimed(runtime, clock)
    seed, directory = candidate(runtime, job, extra_location="WH-NEW")
    expanded_worker = actor("worker", (*LOCATIONS, "WH-NEW"))
    runtime.control.activate(job["id"], job["token"], seed, expanded_worker, evidence_dir=directory)
    retained = runtime.session(token)
    assert retained is not None and retained[0].locations == old_session[0].locations
    with pytest.raises(StoreError) as denied:
        runtime.control.snapshot(retained[0])
    assert denied.value.code == "scope_denied"
    fresh_token, _ = runtime.create_session("planner")
    fresh = runtime.session(fresh_token)
    assert fresh is not None and set(fresh[0].locations) == {*LOCATIONS, "WH-NEW"}
    runtime.control.snapshot(fresh[0])


@pytest.mark.parametrize("operation", ["status", "submit", "claim", "context", "overview"])
def test_forecast_only_location_is_required_for_shared_scope(
    runtime: PostgresRuntimeStore, clock: list[float], operation: str
) -> None:
    partial = actor("worker" if operation == "claim" else "planner", LOCATIONS[:2])
    before = runtime.snapshot()
    with pytest.raises(StoreError) as denied:
        if operation == "status":
            runtime.control.snapshot(partial)
        elif operation == "submit":
            runtime.control.submit(event(clock[0]), partial)
        elif operation == "claim":
            runtime.control.claim(partial)
        elif operation == "context":
            runtime.recommendation_context("rec-1", partial)
        else:
            runtime.overview_snapshot(partial)
    assert denied.value.code == "scope_denied"
    assert runtime.snapshot() == before


def test_archived_location_still_limits_global_status_and_replanning(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    job = claimed(runtime, clock)
    seed, directory = candidate(runtime, job)
    seed["recommendations"] = [seed["recommendations"][0]]
    runtime.control.activate(job["id"], job["token"], seed, actor("worker"), evidence_dir=directory)
    partial = actor("planner", ("WH-NORTH", "WH-FORECAST"))
    for operation in (
        lambda: runtime.control.snapshot(partial),
        lambda: runtime.control.submit(event(clock[0], "c"), partial),
    ):
        with pytest.raises(StoreError) as denied:
            operation()
        assert denied.value.code == "scope_denied"


def test_worker_tokens_and_session_secrets_are_not_in_read_projections(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    token, public = runtime.create_session("planner")
    validated = runtime.session(token)
    assert validated is not None
    job = claimed(runtime, clock)
    views = canonical(
        [
            runtime.snapshot(),
            runtime.control.snapshot(actor("auditor")),
            runtime.overview_snapshot(actor()),
        ]
    )
    assert all(
        secret not in views for secret in (token, validated[2], public["csrf_token"], job["token"])
    )
    assert "session_scopes" not in views and "session_tokens" not in views


def test_concurrent_duplicate_delivery_and_claim_use_one_guarded_job(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    receipt = event(clock[0])
    barrier = threading.Barrier(4)

    def deliver(_: int) -> dict[str, Any]:
        store = PostgresRuntimeStore(
            runtime.config, runtime_root=runtime.runtime_root, clock=lambda: clock[0]
        )
        barrier.wait(timeout=20)
        return store.control.submit(receipt, actor())

    with ThreadPoolExecutor(max_workers=4) as pool:
        outputs = list(pool.map(deliver, range(4)))
        claims = list(pool.map(lambda _: runtime.control.claim(actor("worker")), range(4)))
    assert sum(not output["duplicate"] for output in outputs) == 1
    assert len({output["job_id"] for output in outputs}) == 1
    assert sum(job is not None for job in claims) == 1
    assert runtime.control.snapshot(actor("auditor"))["event_count"] == 1


@pytest.mark.parametrize(
    "table", ["runtime_events", "runtime_generations", "runtime_journal", "session_scopes"]
)
def test_application_cannot_mutate_immutable_runtime_histories(
    cluster: Cluster, runtime: PostgresRuntimeStore, table: str
) -> None:
    with pytest.raises(cluster.driver.errors.InsufficientPrivilege):
        cluster.query(runtime, f"DELETE FROM {{schema}}.{table}", role="application")
    assert runtime.snapshot()["seed"] is not None


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE {schema}.runtime_jobs SET attempts=attempts+1",
        "UPDATE {schema}.runtime_jobs SET source_sha256=repeat('f',64)",
        "UPDATE {schema}.runtime_jobs SET state='completed'",
        "UPDATE {schema}.runtime_jobs SET lease_until=lease_until+100",
        "UPDATE {schema}.runtime_jobs SET token=repeat('e',64)",
    ],
)
def test_job_projection_tampering_is_detected_before_read_or_mutation(
    cluster: Cluster, runtime: PostgresRuntimeStore, clock: list[float], statement: str
) -> None:
    claimed(runtime, clock)
    cluster.query(runtime, statement)
    for operation in (
        runtime.snapshot,
        lambda: runtime.control.snapshot(actor("auditor")),
        lambda: runtime.control.claim(actor("worker")),
    ):
        with pytest.raises((StoreError, PostgresBackendError)) as corrupt:
            operation()
        assert corrupt.value.code == "integrity_error"


def test_privileged_frozen_scope_tampering_cannot_escalate_retained_token(
    cluster: Cluster, runtime: PostgresRuntimeStore
) -> None:
    token, _ = runtime.create_session("planner")
    cluster.query(runtime, "ALTER TABLE {schema}.session_scopes DISABLE TRIGGER USER")
    cluster.query(
        runtime,
        "UPDATE {schema}.session_scopes SET locations=%s",
        (canonical([*LOCATIONS, "WH-UNAUTHORIZED"]),),
    )
    cluster.query(runtime, "ALTER TABLE {schema}.session_scopes ENABLE TRIGGER USER")
    with pytest.raises((StoreError, PostgresBackendError)) as corrupt:
        runtime.session(token)
    assert corrupt.value.code == "integrity_error"


@pytest.mark.parametrize(
    "stage",
    [
        "runtime_event_written",
        "runtime_claim_written",
        "runtime_failure_written",
        "runtime_generation_archived",
        "runtime_projection_written",
        "runtime_activation_written",
    ],
)
def test_runtime_fault_boundaries_rollback_all_control_workflow_and_generation_rows(
    runtime: PostgresRuntimeStore,
    clock: list[float],
    stage: str,
) -> None:
    job = None
    seed = None
    directory = None
    if stage == "runtime_claim_written":
        runtime.control.submit(event(clock[0]), actor())
    elif stage not in {"runtime_event_written", "runtime_claim_written"}:
        job = claimed(runtime, clock)
        seed, directory = candidate(runtime, job)
    before = runtime.snapshot()

    def inject(observed: str) -> None:
        if observed == stage:
            raise RuntimeError(f"injected {stage}")

    failing = PostgresRuntimeStore(
        runtime.config,
        runtime_root=runtime.runtime_root,
        clock=lambda: clock[0],
        _failure_hook=inject,
    )
    with pytest.raises(RuntimeError, match="injected"):
        if stage == "runtime_event_written":
            failing.control.submit(event(clock[0]), actor())
        elif stage == "runtime_claim_written":
            failing.control.claim(actor("worker"))
        elif stage == "runtime_failure_written":
            assert job is not None
            failing.control.fail(
                job["id"], job["token"], actor("worker"), reason="controlled_failure"
            )
        else:
            assert job is not None and seed is not None and directory is not None
            failing.control.activate(
                job["id"], job["token"], seed, actor("worker"), evidence_dir=directory
            )
    assert runtime.snapshot() == before


@pytest.mark.parametrize("boundary", ["evidence_read", "before_commit"])
def test_lease_expiring_during_activation_rolls_back_before_publication(
    runtime: PostgresRuntimeStore,
    clock: list[float],
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    job = claimed(runtime, clock)
    seed, directory = candidate(runtime, job)
    before = runtime.snapshot()
    reached = []
    if boundary == "evidence_read":
        original = runtime._admit

        def expire(value: dict[str, Any], path: Path) -> None:
            original(value, path)
            if path == directory:
                reached.append(boundary)
                clock[0] = job["lease_until"]

        monkeypatch.setattr(runtime, "_admit", expire)
        failing = runtime
    else:

        def expire_stage(stage: str) -> None:
            if stage == "runtime_activation_written":
                reached.append(boundary)
                clock[0] = job["lease_until"]

        failing = PostgresRuntimeStore(
            runtime.config,
            runtime_root=runtime.runtime_root,
            clock=lambda: clock[0],
            _failure_hook=expire_stage,
        )
    with pytest.raises(StoreError) as refused:
        failing.control.activate(
            job["id"], job["token"], seed, actor("worker"), evidence_dir=directory
        )
    assert refused.value.code == "stale_worker" and reached == [boundary]
    assert runtime.snapshot() == before


def test_mutating_caller_seed_during_verification_cannot_change_published_plan(
    runtime: PostgresRuntimeStore,
    clock: list[float],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = claimed(runtime, clock)
    seed, directory = candidate(runtime, job)
    original_quantity = seed["recommendations"][0]["recommended_quantity"]
    original = runtime._admit

    def mutate_caller(value: dict[str, Any], path: Path) -> None:
        original(value, path)
        if path == directory:
            seed["recommendations"][0]["recommended_quantity"] = 900_000

    monkeypatch.setattr(runtime, "_admit", mutate_caller)
    runtime.control.activate(job["id"], job["token"], seed, actor("worker"), evidence_dir=directory)
    actual = next(row for row in current(runtime) if row["source_recommendation_id"] == "rec-1")
    assert actual["recommended_quantity"] == original_quantity


def test_builder_self_hash_is_retained_separately_from_workflow_projection_hash(
    runtime: PostgresRuntimeStore,
    clock: list[float],
) -> None:
    job = claimed(runtime, clock)
    seed, directory = candidate(runtime, job)
    producer_hash = digest(seed)
    seed["seed_hash"] = producer_hash
    runtime.control.activate(job["id"], job["token"], seed, actor("worker"), evidence_dir=directory)
    stored = runtime.seed()
    assert stored["metadata"]["runtime_artifact_seed_hash"] == producer_hash
    assert stored["metadata"]["runtime_job_id"] == job["id"]
    assert digest(stored) != producer_hash
    stable = runtime.snapshot()
    runtime.ensure_seed(stored)
    assert runtime.snapshot() == stable


def test_limited_job_display_does_not_hide_older_unresolved_failures(
    runtime: PostgresRuntimeStore,
    clock: list[float],
) -> None:
    for index in range(51):
        clock[0] += 1
        source = "b" if index % 2 else "c"
        job = claimed(runtime, clock, source)
        runtime.control.fail(job["id"], job["token"], actor("worker"), reason="controlled_failure")
    status = runtime.control.snapshot(actor("auditor"))
    assert len(status["jobs"]) == 50 and status["event_count"] == 51
    assert status["operational"]["failed_unresolved"] == 51
    assert status["operational"]["failed_historical"] == 0


def test_archived_artifact_corruption_is_not_hidden_by_healthy_current_generation(
    runtime: PostgresRuntimeStore,
    clock: list[float],
) -> None:
    job = claimed(runtime, clock)
    activate(runtime, job)
    (runtime.artifact_root / "fixture.json").write_text("archived corruption", encoding="utf-8")
    with pytest.raises((StoreError, PostgresBackendError)) as corrupt:
        runtime.snapshot()
    assert corrupt.value.code == "integrity_error"


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE {schema}.runtime_jobs SET attempts=attempts+1",
        "UPDATE {schema}.runtime_jobs SET source_sha256=repeat('f',64)",
        "UPDATE {schema}.runtime_jobs SET lease_until=lease_until+100",
    ],
)
def test_runtime_replay_rejects_illegal_job_projection_even_after_privileged_reseal(
    cluster: Cluster,
    runtime: PostgresRuntimeStore,
    clock: list[float],
    statement: str,
) -> None:
    claimed(runtime, clock)
    cluster.query(runtime, statement)
    # Deliberately privileged negative fixture: bypass only the incidental state
    # digest, so the immutable journal must independently reject this projection.
    with cluster.connect() as connection:
        connection.row_factory = cluster.driver.rows.dict_row
        runtime._seal(runtime._new_session(connection, write=True))
    with pytest.raises((StoreError, PostgresBackendError)) as corrupt:
        runtime.snapshot()
    assert corrupt.value.code == "integrity_error"


def test_archived_generation_timing_must_replay_even_after_privileged_reseal(
    cluster: Cluster,
    runtime: PostgresRuntimeStore,
    clock: list[float],
) -> None:
    activate(runtime, claimed(runtime, clock))
    cluster.query(runtime, "ALTER TABLE {schema}.runtime_generations DISABLE TRIGGER USER")
    cluster.query(runtime, "UPDATE {schema}.runtime_generations SET archived_at=archived_at+1")
    cluster.query(runtime, "ALTER TABLE {schema}.runtime_generations ENABLE TRIGGER USER")
    with cluster.connect() as connection:
        connection.row_factory = cluster.driver.rows.dict_row
        runtime._seal(runtime._new_session(connection, write=True))
    with pytest.raises((StoreError, PostgresBackendError)) as corrupt:
        runtime.snapshot()
    assert corrupt.value.code == "integrity_error"
