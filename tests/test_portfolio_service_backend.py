"""Portable SQLite behavior and explicitly fake PostgreSQL adapter wiring."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import sqlite3
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from test_portfolio_store import seed as seed

from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.postgres_backend import PostgresConfig
from nexus.portfolio.postgres_runtime import PostgresRuntimeControl, PostgresRuntimeStore
from nexus.portfolio.runtime_control import RuntimeControl
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
from nexus.portfolio.store import PortfolioStore, StoreError, digest, iso

NOW = 1_800_000_000.0
SCOPE = ("WH-NORTH", "WH-SOUTH", "WH-FORECAST")


def actor(locations: tuple[str, ...] = SCOPE) -> Actor:
    return resolve_persona("auditor", locations)


@pytest.fixture
def frozen(tmp_path: Path, seed: dict[str, Any]) -> PortfolioStore:
    store = PortfolioStore(tmp_path / "frozen", clock=lambda: NOW)
    seed["forecasts"] = [{"location_id": "WH-FORECAST"}]
    store.ensure_seed(seed)
    return store


@pytest.fixture
def connected(tmp_path: Path, seed: dict[str, Any]) -> PortfolioStore:
    store = PortfolioStore(tmp_path / "connected", clock=lambda: NOW)
    seed["metadata"].update(
        pipeline_mode="connected_runtime_baseline", source_snapshot_sha256="a" * 64
    )
    seed["forecasts"] = [{"location_id": "WH-FORECAST"}]
    seed["executable"] = True
    store.ensure_seed(seed)
    (store.path.parent / "evidence" / "initial").mkdir(parents=True)
    with store.transaction() as db:
        db.execute("INSERT INTO metadata VALUES ('runtime_evidence_path','evidence/initial')")
        db.execute("INSERT INTO metadata VALUES ('runtime_ingested_at',?)", (iso(NOW),))
    RuntimeControl(store)
    return store


def event(source: str = "b" * 64) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type="inventory.adjusted",
        source="adapter-test",
        subject_id="portfolio",
        occurred_at=datetime.fromtimestamp(NOW, UTC),
        ingested_at=datetime.fromtimestamp(NOW, UTC),
        idempotency_key=uuid4().hex,
        payload={"source_snapshot_sha256": source},
    )


def replan(store: PortfolioStore) -> None:
    control = RuntimeControl(store)
    control.submit(event(), resolve_persona("planner", SCOPE))
    worker = Actor("runtime-worker", "Fixture worker", ActorRole.SYSTEM, (*SCOPE, "WH-NEW"))
    job = control.claim(worker)
    assert job is not None
    candidate = store.seed()
    candidate["metadata"]["source_snapshot_sha256"] = "b" * 64
    candidate["forecasts"] = [{"location_id": "WH-NEW"}]
    evidence = store.path.parent / "evidence" / "replacement"
    evidence.mkdir()
    control.activate(job["id"], job["token"], candidate, worker, evidence_dir=evidence)


def test_frozen_views_preserve_existing_data_and_optional_evidence(frozen: PortfolioStore) -> None:
    with frozen.connection() as db:
        tables = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        before = [tuple(row) for row in db.execute("SELECT key FROM metadata ORDER BY key")]
    original = frozen.seed()
    with read_view(frozen, actor=actor()) as view:
        assert view.seed == original and view.seed_sha256 == digest(original)
        assert view.evidence_dir is None and view.runtime_ingested_at is None
        assert view.checked_at == NOW and view.retained_locations == frozenset(SCOPE)
        identity = view.runtime_identity
        view.seed["metadata"]["caller_mutation"] = True
    with guarded_view(frozen, actor=actor()) as retained:
        assert retained.seed == original and retained.runtime_identity == identity
    with frozen.connection() as db:
        assert (
            db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
            == tables
        )
        assert [tuple(row) for row in db.execute("SELECT key FROM metadata ORDER BY key")] == before
    assert state_root(frozen) == frozen.path.parent.resolve()
    assert backend_label(frozen) == "SQLite"


def test_pristine_legacy_feedback_seed_is_verified_without_bootstrap(tmp_path: Path) -> None:
    from test_portfolio_runtime_feedback import make_store

    store, clock = make_store(tmp_path)
    with read_view(store) as view:
        assert view.seed == store.seed() and view.checked_at == clock[0]
        assert view.retained_locations == frozenset({"DC-ARG"})
        assert view.evidence_dir is None
    with guarded_view(store, expected_seed_sha256=view.seed_sha256):
        pass
    with store.connection() as db:
        assert (
            db.execute("SELECT 1 FROM sqlite_master WHERE name GLOB 'runtime_*'").fetchone() is None
        )
        assert db.execute("SELECT 1 FROM metadata WHERE key GLOB 'runtime_*'").fetchone() is None
    with pytest.raises(StoreError) as unavailable:
        runtime_status(store, actor(("DC-ARG",)))
    assert unavailable.value.code == "runtime_not_initialized"


@pytest.mark.parametrize(
    "footprint",
    ["table", "marker", "job", "projection", "bad_hash", "bad_state", "missing_creation"],
)
def test_legacy_compatibility_never_adopts_runtime_footprints_or_tamper(
    tmp_path: Path, footprint: str
) -> None:
    from test_portfolio_runtime_feedback import make_store

    from nexus.portfolio.store import canonical

    store, _ = make_store(tmp_path)
    with store.transaction() as db:
        if footprint == "table":
            db.execute("CREATE TABLE runtime_jobs (id TEXT)")
        elif footprint == "marker":
            db.execute("INSERT INTO metadata VALUES ('runtime_state_hash','wrong')")
        elif footprint in {"job", "projection"}:
            seed = json.loads(
                db.execute("SELECT value FROM metadata WHERE key='seed'").fetchone()[0]
            )
            if footprint == "job":
                seed["metadata"]["runtime_job_id"] = "not-an-initial-generation"
            else:
                seed["recommendations"][0]["source_recommendation_id"] = "forged-owner"
            db.execute("UPDATE metadata SET value=? WHERE key='seed'", (canonical(seed),))
            db.execute("UPDATE metadata SET value=? WHERE key='seed_hash'", (digest(seed),))
        elif footprint == "bad_hash":
            db.execute("UPDATE metadata SET value='wrong' WHERE key='seed_hash'")
        elif footprint == "bad_state":
            db.execute("UPDATE recommendations SET state='approved'")
        else:
            db.execute("DELETE FROM events")
            db.execute("UPDATE metadata SET value=? WHERE key='audit_head'", ("0" * 64,))
    with pytest.raises(StoreError) as refused, guarded_view(store):
        pytest.fail("Unverified initial state was yielded")
    assert refused.value.code == "integrity_error"


@pytest.mark.parametrize("reader", [read_view, guarded_view])
def test_views_require_full_forecast_scope_and_expected_generation(
    frozen: PortfolioStore,
    reader: Any,
) -> None:
    with pytest.raises(StoreError) as denied, reader(frozen, actor=actor(SCOPE[:2])):
        pytest.fail("Partial scope was yielded")
    assert denied.value.code == "scope_denied"
    with pytest.raises(StoreError) as stale, reader(frozen, expected_seed_sha256="f" * 64):
        pytest.fail("A stale generation was yielded")
    assert stale.value.code == "generation_changed"
    with reader(frozen, expected_seed_sha256=digest(frozen.seed())) as current:
        assert current.seed_sha256 == digest(frozen.seed())


@pytest.mark.parametrize("value", ["short", "A" * 64, True, 4])
def test_expected_identity_requires_exact_sha_before_database(
    frozen: PortfolioStore, value: Any
) -> None:
    with pytest.raises(StoreError) as invalid, read_view(frozen, expected_seed_sha256=value):
        pass
    assert invalid.value.code == "invalid_seed_hash"


def test_archived_forecast_scope_and_runtime_identity_survive_replan(
    connected: PortfolioStore,
) -> None:
    with read_view(connected) as initial:
        assert initial.runtime_ingested_at == iso(NOW)
        assert initial.evidence_dir == connected.path.parent / "evidence" / "initial"
    replan(connected)
    with read_view(connected, actor=actor((*SCOPE, "WH-NEW"))) as current:
        assert current.runtime_identity == initial.runtime_identity
        assert current.seed_sha256 != initial.seed_sha256
        assert current.retained_locations == frozenset((*SCOPE, "WH-NEW"))
        assert current.evidence_dir == connected.path.parent / "evidence" / "replacement"
    with (
        pytest.raises(StoreError) as denied,
        read_view(connected, actor=actor(("WH-NORTH", "WH-SOUTH", "WH-NEW"))),
    ):
        pass
    assert denied.value.code == "scope_denied"


def test_read_snapshot_is_stable_while_another_writer_activates(connected: PortfolioStore) -> None:
    with read_view(connected) as before:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(replan, connected).result(timeout=10)
        assert before.seed["metadata"]["source_snapshot_sha256"] == "a" * 64
    with read_view(connected) as after:
        assert after.seed["metadata"]["source_snapshot_sha256"] == "b" * 64


def test_guard_holds_writer_lock_through_sidecar_commit(
    frozen: PortfolioStore, tmp_path: Path
) -> None:
    attempting, acquired = threading.Event(), threading.Event()

    def compete() -> None:
        attempting.set()
        with frozen.transaction():
            acquired.set()

    with ThreadPoolExecutor(max_workers=1) as pool:
        with guarded_view(frozen) as view:
            future = pool.submit(compete)
            assert attempting.wait(2)
            assert not acquired.wait(0.05)
            with sqlite3.connect(tmp_path / "sidecar.sqlite3") as db:
                db.execute("CREATE TABLE observations (generation TEXT)")
                db.execute("INSERT INTO observations VALUES (?)", (view.seed_sha256,))
            assert not acquired.is_set()
        future.result(timeout=5)
    assert acquired.is_set()


def test_guard_captures_clock_after_writer_lock(frozen: PortfolioStore) -> None:
    observed: list[bool] = []

    def clock() -> float:
        with sqlite3.connect(frozen.path, timeout=0) as probe:
            try:
                probe.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError:
                observed.append(True)
            else:
                probe.rollback()
                observed.append(False)
        return NOW

    frozen.clock = clock
    with guarded_view(frozen) as view:
        assert view.checked_at == NOW
    assert observed == [True]


def test_caller_exception_releases_writer_lock_unchanged(frozen: PortfolioStore) -> None:
    original = frozen.seed()
    with pytest.raises(RuntimeError, match="sidecar failed"), guarded_view(frozen):
        raise RuntimeError("sidecar failed")
    with frozen.transaction() as db:
        assert (
            json.loads(db.execute("SELECT value FROM metadata WHERE key='seed'").fetchone()[0])
            == original
        )


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE metadata SET value='wrong' WHERE key='seed_hash'",
        "UPDATE recommendations SET state='approved'",
        "UPDATE recommendations SET location_id='WH-OTHER'",
        "UPDATE events SET event_hash='wrong'",
    ],
)
def test_corrupt_sqlite_state_never_reaches_a_sidecar(
    frozen: PortfolioStore, statement: str
) -> None:
    with frozen.transaction() as db:
        db.execute(statement)
    with pytest.raises(StoreError) as corrupt, guarded_view(frozen):
        pytest.fail("Corrupt state was yielded")
    assert corrupt.value.code == "integrity_error"


@pytest.mark.parametrize("reference", ["../outside", "/outside", "evidence/missing", ""])
def test_evidence_paths_are_local_contained_and_existing(
    frozen: PortfolioStore, reference: str
) -> None:
    with frozen.transaction() as db:
        db.execute("INSERT INTO metadata VALUES ('runtime_evidence_path',?)", (reference,))
    with pytest.raises(StoreError) as invalid, read_view(frozen):
        pass
    assert invalid.value.code == "invalid_path"


def test_state_root_rejects_a_linked_database(
    frozen: PortfolioStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda path: path == frozen.path or original(path))
    with pytest.raises(StoreError) as invalid:
        state_root(frozen)
    assert invalid.value.code == "invalid_path"


@pytest.mark.parametrize("instant", [math.nan, math.inf, None])
def test_nonfinite_clock_is_refused(frozen: PortfolioStore, instant: Any) -> None:
    frozen.clock = lambda: instant
    with pytest.raises(StoreError) as invalid, guarded_view(frozen):
        pass
    assert invalid.value.code == "invalid_clock"


def test_metrics_are_scoped_and_frozen_status_does_not_initialize_control(
    frozen: PortfolioStore,
) -> None:
    result = metrics(frozen, actor())
    assert result == frozen.metrics_snapshot()
    assert result["states"] == {"pending": 2}
    with pytest.raises(StoreError) as denied:
        metrics(frozen, actor(SCOPE[:2]))
    assert denied.value.code == "scope_denied"
    with pytest.raises(StoreError) as disabled:
        runtime_status(frozen, actor())
    assert disabled.value.code == "runtime_required"
    with frozen.connection() as db:
        assert (
            db.execute("SELECT name FROM sqlite_master WHERE name='runtime_jobs'").fetchone()
            is None
        )


def test_runtime_status_uses_existing_control_without_constructor_reentry(
    connected: PortfolioStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    control = runtime_control(connected)
    assert isinstance(control, RuntimeControl)
    control.submit(event(), resolve_persona("planner", SCOPE))
    expected = control.snapshot()
    monkeypatch.setattr(
        RuntimeControl,
        "__init__",
        lambda *args, **kwargs: pytest.fail("Status must not construct control"),
    )
    assert runtime_status(connected, actor()) == expected


def test_runtime_status_aggregates_before_fifty_row_display_limit(
    connected: PortfolioStore,
) -> None:
    control = RuntimeControl(connected)
    worker = Actor("runtime-worker", "Worker", ActorRole.SYSTEM, SCOPE)
    for index in range(51):
        control.submit(
            event(hashlib.sha256(str(index).encode()).hexdigest()),
            resolve_persona("planner", SCOPE),
        )
        job = control.claim(worker)
        assert job is not None
        control.fail(job["id"], job["token"], worker, reason="fixture_failure")
    status = runtime_status(connected, actor())
    assert len(status["jobs"]) == 50 and status["operational"]["failed_unresolved"] == 51
    assert status["event_count"] == 51 and "token" not in json.dumps(status)


def postgres(tmp_path: Path, **kwargs: Any) -> PostgresRuntimeStore:
    store = PostgresRuntimeStore(
        PostgresConfig(
            dsn="postgresql://fixture:private@127.0.0.1:55438/nexus_portfolio_test_fixture",
            schema="nexus_test_adapter",
            artifact_root=tmp_path / "initial",
        ),
        runtime_root=tmp_path,
        clock=lambda: NOW,
        **kwargs,
    )
    bind_service_root(store, claim=True)
    return store


@pytest.mark.parametrize("operation", ["read", "guard", "status", "metrics"])
def test_system_actor_never_gets_human_global_reads_without_driver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    monkeypatch.setitem(sys.modules, "psycopg", None)
    store = postgres(tmp_path)
    worker = Actor("runtime-worker", "Worker", ActorRole.SYSTEM, SCOPE)
    with pytest.raises(StoreError) as denied:
        if operation in {"read", "guard"}:
            with (read_view if operation == "read" else guarded_view)(store, actor=worker):
                pass
        else:
            (runtime_status if operation == "status" else metrics)(store, worker)
    assert denied.value.code == "role_denied"


@pytest.mark.parametrize("operation", [runtime_status, metrics])
def test_status_and_metrics_require_an_explicit_actor(
    frozen: PortfolioStore, operation: Any
) -> None:
    with pytest.raises(StoreError) as denied:
        operation(frozen, None)
    assert denied.value.code == "role_denied"


@pytest.mark.parametrize("reader", [read_view, guarded_view])
def test_postgres_adapter_uses_one_supplied_unit_and_credential_free_identity(
    tmp_path: Path, seed: dict[str, Any], monkeypatch: pytest.MonkeyPatch, reader: Any
) -> None:
    store = postgres(tmp_path)
    seed["forecasts"] = [{"location_id": "WH-FORECAST"}]
    transitions: list[str] = []

    class Unit:
        def snapshot(self) -> dict[str, Any]:
            transitions.append("snapshot")
            return {
                "seed": copy.deepcopy(seed),
                "seed_sha256": digest(seed),
                "runtime_generations": [],
            }

        def _query(self, query: str, *, table: str) -> Any:
            assert (
                table == "metadata"
                and "runtime_ingested_at" in query
                and "session_key" not in query
            )
            return type("Cursor", (), {"fetchall": lambda self: []})()

    @contextmanager
    def transaction() -> Any:
        transitions.append("enter")
        yield Unit()
        transitions.append("exit")

    monkeypatch.setattr(
        store, "write_transaction" if reader is guarded_view else "read_snapshot", transaction
    )
    with reader(store, actor=actor()) as view:
        assert transitions == ["enter", "snapshot"]
        assert view.retained_locations == frozenset(SCOPE) and view.checked_at == NOW
        assert "private" not in repr(view) and backend_label(store) == "PostgreSQL"
        identity = view.runtime_identity
    assert transitions[-1] == "exit"
    store.config = replace(
        store.config, dsn=store.config.dsn.replace("fixture:private", "rotated:other-secret")
    )
    with reader(store) as changed_credentials:
        assert changed_credentials.runtime_identity == identity
    store.config = replace(store.config, schema="nexus_test_other")
    with pytest.raises(StoreError) as conflict, reader(store):
        pytest.fail("A different schema must not reuse the claimed service directory")
    assert conflict.value.code == "service_root_conflict"


def test_postgres_status_and_metrics_delegate_actor_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = postgres(tmp_path)
    principal = actor()
    calls: list[tuple[str, Actor]] = []
    monkeypatch.setattr(
        PostgresRuntimeControl,
        "snapshot",
        lambda self, supplied: calls.append(("status", supplied)) or {"adapter": "postgresql"},
    )
    monkeypatch.setattr(
        store,
        "metrics_snapshot",
        lambda supplied: calls.append(("metrics", supplied)) or {"database": "postgresql"},
    )
    assert isinstance(runtime_control(store), PostgresRuntimeControl)
    assert runtime_status(store, principal) == {"adapter": "postgresql"}
    assert metrics(store, principal) == {"database": "postgresql"}
    assert calls == [("status", principal), ("metrics", principal)]
