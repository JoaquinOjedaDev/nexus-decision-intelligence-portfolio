"""Portable runtime imports and role checks must not require a PostgreSQL driver."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_backend import PostgresConfig
from nexus.portfolio.postgres_runtime import PostgresRuntimeControl, PostgresRuntimeStore
from nexus.portfolio.store import StoreError


@pytest.fixture
def control(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PostgresRuntimeControl:
    # Force dependency absence even when a developer installed optional psycopg.
    monkeypatch.setitem(sys.modules, "psycopg", None)
    store = PostgresRuntimeStore(
        PostgresConfig(
            dsn="postgresql://unused:do-not-print@127.0.0.1:55438/nexus_portfolio_test_no_connect",
            schema="nexus_test_portable_runtime",
            artifact_root=tmp_path / "not-created" / "initial",
        ),
        runtime_root=tmp_path / "not-created",
    )
    return PostgresRuntimeControl(store)


def actor(role: ActorRole, identity: str = "denied") -> Actor:
    return Actor(identity, "Portable principal", role, ("WH-NORTH",))


def event() -> EventEnvelope:
    instant = datetime(2025, 1, 1, tzinfo=UTC)
    return EventEnvelope(
        event_id=uuid4(),
        event_type="inventory.adjusted",
        source="constructed-test",
        subject_id="portfolio",
        occurred_at=instant,
        ingested_at=instant,
        idempotency_key=uuid4().hex,
        payload={"source_snapshot_sha256": "b" * 64},
    )


def test_runtime_construction_is_lazy_and_exposes_no_sqlite_shim(
    control: PostgresRuntimeControl,
) -> None:
    store = control.store
    assert not store.config.artifact_root.parent.exists()
    assert not hasattr(store, "connection") and not hasattr(store, "transaction")
    assert "do-not-print" not in repr(store) and "do-not-print" not in repr(store.config)
    assert isinstance(store.control, PostgresRuntimeControl)


@pytest.mark.parametrize("role", [ActorRole.APPROVER, ActorRole.AUDITOR, ActorRole.SYSTEM])
def test_event_submission_denies_nonplanner_before_database_access(
    control: PostgresRuntimeControl, role: ActorRole
) -> None:
    with pytest.raises(StoreError) as denied:
        control.submit(event(), actor(role))
    assert denied.value.code == "role_denied" and denied.value.status == 403


@pytest.mark.parametrize("operation", ["claim", "fail", "activate", "attempt_evidence_dir"])
@pytest.mark.parametrize(
    "principal",
    [
        actor(ActorRole.PLANNER),
        actor(ActorRole.APPROVER),
        actor(ActorRole.AUDITOR),
        actor(ActorRole.SYSTEM, "another-system-worker"),
    ],
)
def test_worker_operations_require_the_scoped_runtime_identity_before_database(
    control: PostgresRuntimeControl, operation: str, principal: Actor, tmp_path: Path
) -> None:
    with pytest.raises(StoreError) as denied:
        if operation == "claim":
            control.claim(principal)
        elif operation == "fail":
            control.fail("job", "lease", principal, reason="pipeline_failed")
        elif operation == "attempt_evidence_dir":
            control.attempt_evidence_dir("job", "lease", principal)
        else:
            control.activate("job", "lease", {}, principal, evidence_dir=tmp_path)
    assert denied.value.code == "role_denied" and denied.value.status == 403


def test_service_identity_cannot_read_global_human_status_without_permission(
    control: PostgresRuntimeControl,
) -> None:
    with pytest.raises(StoreError) as denied:
        control.snapshot(actor(ActorRole.SYSTEM, "runtime-worker"))
    assert denied.value.code == "role_denied" and denied.value.status == 403


@pytest.mark.parametrize("role", list(ActorRole))
@pytest.mark.parametrize("identity", ["runtime-worker", "other", "runtime-event-consumer"])
def test_broker_admission_rejects_other_capabilities_before_database_access(
    control: PostgresRuntimeControl, role: ActorRole, identity: str
) -> None:
    if role is ActorRole.SYSTEM and identity == "runtime-event-consumer":
        return
    with pytest.raises(StoreError) as denied:
        control.submit_broker(event(), actor(role, identity))
    assert denied.value.code == "role_denied"


@pytest.mark.parametrize("operation", ["submit", "claim", "approve", "execute", "observe"])
def test_broker_identity_cannot_acquire_human_or_computation_capabilities(
    control: PostgresRuntimeControl, operation: str
) -> None:
    principal = actor(ActorRole.SYSTEM, "runtime-event-consumer")
    with pytest.raises(StoreError) as denied:
        if operation == "submit":
            control.submit(event(), principal)
        elif operation == "claim":
            control.claim(principal)
        elif operation == "approve":
            control.store.decide("rec-1", principal, "approve", "a" * 64)
        elif operation == "execute":
            control.store.execute("rec-1", principal, "a" * 64, "not-dispatched")
        else:
            control.store.observe("rec-1", principal, "a" * 64)
    assert denied.value.code == "role_denied"


@pytest.mark.parametrize("operation", ["approve", "reject", "revoke", "execute", "observe"])
def test_runtime_worker_is_never_a_human_workflow_principal(
    control: PostgresRuntimeControl, operation: str
) -> None:
    worker = actor(ActorRole.SYSTEM, "runtime-worker")
    with pytest.raises(StoreError) as denied:
        if operation == "execute":
            control.store.execute("rec-1", worker, "a" * 64, "not-dispatched")
        elif operation == "observe":
            control.store.observe("rec-1", worker, "a" * 64)
        else:
            control.store.decide("rec-1", worker, operation, "a" * 64)
    assert denied.value.code == "role_denied" and denied.value.status == 403


@pytest.mark.parametrize("lease", [True, False, 0, 9, 3601, 10.0, "10", None])
def test_invalid_worker_lease_refuses_before_driver_access(
    control: PostgresRuntimeControl, lease: Any
) -> None:
    with pytest.raises(ValueError, match="lease"):
        control.claim(actor(ActorRole.SYSTEM, "runtime-worker"), lease_seconds=lease)


@pytest.mark.parametrize("reason", ["", "not safe", "x" * 81, "UPPERCASE", "../secret"])
def test_failure_reason_is_a_bounded_code_not_arbitrary_diagnostics(
    control: PostgresRuntimeControl, reason: str
) -> None:
    with pytest.raises(ValueError, match="reason"):
        control.fail("job", "lease", actor(ActorRole.SYSTEM, "runtime-worker"), reason=reason)


@pytest.mark.parametrize("location", ["relative", "parent_traversal", "filesystem_root", "network"])
def test_runtime_root_requires_an_explicit_bounded_local_path(
    control: PostgresRuntimeControl, tmp_path: Path, location: str
) -> None:
    from nexus.portfolio.postgres_backend import PostgresBackendError

    roots = {
        "relative": Path("relative-root"),
        "parent_traversal": tmp_path / ".." / "outside",
        "filesystem_root": Path(tmp_path.anchor),
        "network": Path("//server/share/runtime"),
    }
    with pytest.raises(PostgresBackendError) as refused:
        PostgresRuntimeStore(control.store.config, runtime_root=roots[location])
    assert refused.value.code == "configuration_error"


@pytest.mark.parametrize("location", ["relative", "parent_traversal", "outside"])
def test_evidence_path_cannot_escape_the_declared_root_without_database_access(
    control: PostgresRuntimeControl, tmp_path: Path, location: str
) -> None:
    paths = {
        "relative": Path("relative-evidence"),
        "parent_traversal": control.store.runtime_root / ".." / "outside",
        "outside": tmp_path / "outside-evidence",
    }
    with pytest.raises(StoreError) as refused:
        control.store._path(paths[location])
    assert refused.value.code == "invalid_path"


def test_default_runtime_root_is_the_initial_artifact_root(control: PostgresRuntimeControl) -> None:
    store = PostgresRuntimeStore(control.store.config)
    assert store.runtime_root == store.artifact_root
    assert not store.artifact_root.exists()


@pytest.mark.parametrize("metadata", [None, 42, []])
def test_malformed_metadata_is_a_controlled_invalid_plan_without_driver_access(
    control: PostgresRuntimeControl, metadata: Any
) -> None:
    evidence = control.store.artifact_root
    evidence.mkdir(parents=True)
    seed = {"executable": True, "metadata": metadata, "recommendations": [], "artifacts": []}
    with pytest.raises(StoreError) as refused:
        control.store._admit(seed, evidence)
    assert refused.value.code == "invalid_plan"


@pytest.mark.parametrize(
    "field", ["runtime_job_id", "runtime_artifact_seed_hash", "source_recommendation_id"]
)
def test_producer_envelope_cannot_claim_reserved_workflow_identity(
    monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    from nexus.portfolio.postgres_runtime import _producer

    monkeypatch.setitem(sys.modules, "psycopg", None)
    seed: dict[str, Any] = {"metadata": {}, "recommendations": [{"id": "rec-1"}]}
    target = seed["recommendations"][0] if field == "source_recommendation_id" else seed["metadata"]
    target[field] = "reserved-workflow-identity"
    with pytest.raises(StoreError) as refused:
        _producer(seed)
    assert refused.value.code == "invalid_plan"
