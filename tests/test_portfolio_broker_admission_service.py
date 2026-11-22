"""Opt-in PostgreSQL broker identity tests; constructed fixtures, not transport evidence."""

from __future__ import annotations

from dataclasses import replace

import pytest
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import (
    LOCATIONS,
    actor,
    binding,
    event,
)
from test_portfolio_postgres_runtime_service import (
    clock as clock,
)
from test_portfolio_postgres_runtime_service import (
    runtime as runtime,
)

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.store import StoreError

pytestmark = pytest.mark.portfolio_service
BROKER = Actor("runtime-event-consumer", "Runtime event consumer", ActorRole.SYSTEM, LOCATIONS)


def test_broker_admission_replays_and_duplicate_delivery_preserves_one_job(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    fingerprint = binding(runtime)
    runtime.decide("rec-1", actor("approver"), "approve", fingerprint)
    incoming = event(clock[0])
    admitted = runtime.control.submit_broker(incoming, BROKER)
    assert not admitted["duplicate"] and admitted["invalidated"] == 2
    before = runtime.snapshot()
    replayed = PostgresRuntimeStore(
        runtime.config, runtime_root=runtime.runtime_root, clock=lambda: clock[0]
    )
    assert replayed.snapshot() == before
    duplicate = replayed.control.submit_broker(incoming, BROKER)
    assert duplicate == {"duplicate": True, "job_id": admitted["job_id"], "invalidated": 0}
    assert replayed.snapshot() == before
    with pytest.raises(StoreError):
        runtime.execute("rec-1", actor("approver"), fingerprint, "stale-approved-order")
    claimed = replayed.control.claim(actor("worker"))
    assert claimed is not None and claimed["id"] == admitted["job_id"]


def test_broker_scope_includes_forecast_only_and_historical_locations(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    before = runtime.snapshot()
    with pytest.raises(StoreError) as denied:
        runtime.control.submit_broker(event(clock[0]), replace(BROKER, locations=LOCATIONS[:2]))
    assert denied.value.code == "scope_denied"
    assert runtime.snapshot() == before


def test_broker_conflicting_event_identity_is_not_acknowledgeable(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    incoming = event(clock[0])
    runtime.control.submit_broker(incoming, BROKER)
    before = runtime.snapshot()
    changed = incoming.model_copy(update={"payload": {"source_snapshot_sha256": "c" * 64}})
    with pytest.raises(StoreError) as denied:
        runtime.control.submit_broker(changed, BROKER)
    assert denied.value.code == "event_conflict"
    assert runtime.snapshot() == before


def test_broker_fault_rolls_back_inbox_job_and_approval_invalidation(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    runtime.decide("rec-1", actor("approver"), "approve", binding(runtime))
    before = runtime.snapshot()

    def crash(stage: str) -> None:
        if stage == "runtime_event_written":
            raise RuntimeError("injected pre-commit failure")

    failing = PostgresRuntimeStore(
        runtime.config,
        runtime_root=runtime.runtime_root,
        clock=lambda: clock[0],
        _failure_hook=crash,
    )
    with pytest.raises(RuntimeError, match="injected"):
        failing.control.submit_broker(event(clock[0]), BROKER)
    assert runtime.snapshot() == before
