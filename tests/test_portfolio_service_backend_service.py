"""Opt-in adapter verification using the existing guarded native-cluster fixture.

No service is started, stopped or reset here. Constructed fixture plans prove
adapter behavior only; this lane does not build scientific evidence or a broker.
"""

from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import activate, actor, candidate, claimed
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_runtime import PostgresRuntimeControl, PostgresRuntimeStore
from nexus.portfolio.service_backend import (
    backend_label,
    bind_service_root,
    guarded_view,
    metrics,
    read_view,
    runtime_control,
    runtime_status,
    state_root,
)
from nexus.portfolio.store import StoreError, digest

pytestmark = pytest.mark.portfolio_service


@pytest.fixture(autouse=True)
def service_root_binding(runtime: PostgresRuntimeStore) -> None:
    bind_service_root(runtime, claim=True)


def test_native_view_uses_verified_generation_and_preserves_v4_state(
    runtime: PostgresRuntimeStore,
) -> None:
    before = runtime.snapshot()
    with read_view(runtime, actor=actor("auditor")) as view:
        assert view.seed == before["seed"] and view.seed_sha256 == before["seed_sha256"]
        assert view.evidence_dir == runtime.artifact_root
        assert view.retained_locations == frozenset({"WH-NORTH", "WH-SOUTH", "WH-FORECAST"})
        assert view.runtime_ingested_at is not None and len(view.runtime_identity) == 64
    with guarded_view(
        runtime, actor=actor("planner"), expected_seed_sha256=view.seed_sha256
    ) as retained:
        assert retained == view
    assert runtime.snapshot() == before
    assert backend_label(runtime) == "PostgreSQL" and state_root(runtime) == runtime.runtime_root


def test_native_status_and_metrics_keep_correct_scoped_signatures(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    claimed(runtime, clock)
    principal = actor("auditor")
    assert isinstance(runtime_control(runtime), PostgresRuntimeControl)
    assert runtime_status(runtime, principal) == runtime.control.snapshot(principal)
    assert metrics(runtime, principal) == runtime.metrics_snapshot(principal)
    assert metrics(runtime, principal)["database"] == "postgresql"


@pytest.mark.parametrize("reader", [read_view, guarded_view])
def test_native_partial_and_system_scope_never_yield_context(
    runtime: PostgresRuntimeStore, reader: Any
) -> None:
    for principal, code in (
        (actor("auditor", ("WH-NORTH", "WH-SOUTH")), "scope_denied"),
        (actor("worker"), "role_denied"),
    ):
        with pytest.raises(StoreError) as denied, reader(runtime, actor=principal):
            pytest.fail("Unauthorized native view")
        assert denied.value.code == code


def test_native_guard_holds_through_sidecar_commit(
    runtime: PostgresRuntimeStore, tmp_path: Path
) -> None:
    attempting, acquired = threading.Event(), threading.Event()

    def compete() -> None:
        attempting.set()
        with runtime.write_transaction():
            acquired.set()

    with ThreadPoolExecutor(max_workers=1) as pool:
        with guarded_view(runtime) as view:
            future = pool.submit(compete)
            assert attempting.wait(2)
            assert not acquired.wait(0.1)
            with sqlite3.connect(tmp_path / "sidecar.sqlite3") as sidecar:
                sidecar.execute("CREATE TABLE receipt (generation TEXT)")
                sidecar.execute("INSERT INTO receipt VALUES (?)", (view.seed_sha256,))
            assert not acquired.is_set()
        future.result(timeout=10)
    assert acquired.is_set()


def test_native_read_snapshot_remains_stable_during_independent_activation(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    job = claimed(runtime, clock)
    with read_view(runtime) as before:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(activate, runtime, job).result(timeout=20)
        assert before.seed["metadata"]["source_snapshot_sha256"] == "a" * 64
    with read_view(runtime) as after:
        assert after.seed["metadata"]["source_snapshot_sha256"] == "b" * 64
        assert after.runtime_identity == before.runtime_identity
    with (
        pytest.raises(StoreError) as stale,
        guarded_view(runtime, expected_seed_sha256=before.seed_sha256),
    ):
        pytest.fail("Old generation acquired a sidecar guard")
    assert stale.value.code == "generation_changed"


def test_native_archived_forecast_scope_is_retained_after_replacement(
    runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    job = claimed(runtime, clock)
    replacement, evidence = candidate(runtime, job, extra_location="WH-NEW")
    replacement["forecasts"] = []
    full = ("WH-NORTH", "WH-SOUTH", "WH-FORECAST", "WH-NEW")
    worker = Actor("runtime-worker", "Worker", ActorRole.SYSTEM, full)
    runtime.control.activate(job["id"], job["token"], replacement, worker, evidence_dir=evidence)
    with read_view(runtime, actor=actor("auditor", full)) as view:
        assert view.retained_locations == frozenset(full)
        assert view.evidence_dir == evidence
        assert view.seed_sha256 == digest(runtime.seed())
    with (
        pytest.raises(StoreError) as denied,
        read_view(runtime, actor=actor("auditor", ("WH-NORTH", "WH-SOUTH", "WH-NEW"))),
    ):
        pass
    assert denied.value.code == "scope_denied"


def test_native_sidecar_failure_releases_guard_without_changing_workflow(
    runtime: PostgresRuntimeStore,
) -> None:
    before = runtime.snapshot()
    with pytest.raises(RuntimeError, match="sidecar failure"), guarded_view(runtime):
        raise RuntimeError("sidecar failure")
    assert runtime.snapshot() == before
    with guarded_view(runtime):
        pass
