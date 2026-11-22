"""Actual isolated PostgreSQL action-fence and row-lock races; constructed data only."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Event
from time import monotonic, sleep

import pytest
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import actor, event
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime
from test_portfolio_transport_ledger_service import binding as binding
from test_portfolio_transport_ledger_service import ledger as ledger
from test_portfolio_transport_ledger_service import record

from nexus.portfolio.postgres_backend import PostgresBackendError
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.store import StoreError
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger, TransportRecord
from nexus.portfolio.transport_runtime import TransportRuntimeStore

pytestmark = pytest.mark.portfolio_service


@pytest.fixture
def fenced(
    cluster: Cluster,
    runtime: PostgresRuntimeStore,
    ledger: TransportLedger,
    binding: TransportBinding,
) -> TransportRuntimeStore:
    value = TransportRuntimeStore(
        runtime.config,
        transport=ledger,
        binding=binding,
        runtime_root=runtime.runtime_root,
        clock=runtime.clock,
    )
    before = runtime.snapshot()
    assert cluster.migrate(value)["applied"] is True
    assert value.snapshot() == before
    return value


def approve(store: PostgresRuntimeStore, identity: str = "rec-1") -> str:
    fingerprint = store.recommendation(identity, actor())["fingerprint"]
    store.decide(identity, actor("approver"), "approve", fingerprint)
    return str(fingerprint)


def test_migration_is_persistent_idempotent_and_not_downgradable(
    fenced: TransportRuntimeStore,
    runtime: PostgresRuntimeStore,
    cluster: Cluster,
) -> None:
    assert cluster.migrate(fenced)["applied"] is False
    with pytest.raises(PostgresBackendError, match="unknown or missing relations"):
        approve(runtime)
    with pytest.raises(PostgresBackendError):
        cluster.migrate(runtime)
    restarted = TransportRuntimeStore(
        fenced.config,
        transport=fenced.transport,
        binding=fenced.transport_binding,
        runtime_root=fenced.runtime_root,
        clock=fenced.clock,
    )
    fingerprint = approve(restarted)
    restarted.execute("rec-1", actor("approver"), fingerprint, "once")
    assert len(restarted.snapshot()["effects"]) == 1
    assert cluster.query(
        fenced, "SELECT version FROM {schema}.schema_migrations ORDER BY version"
    ) == [(2,), (3,), (4,), (6,)]


def test_capture_blocks_approval_and_execution_without_changing_workflow(
    fenced: TransportRuntimeStore,
    clock: list[float],
) -> None:
    approved = approve(fenced)
    pending = fenced.recommendation("rec-2", actor())["fingerprint"]
    fenced.transport.capture(fenced.transport_binding, [record(clock[0])])
    before = fenced.snapshot()
    with pytest.raises(StoreError) as failure:
        fenced.decide("rec-2", actor("approver"), "approve", pending)
    assert failure.value.code == "transport_pending"
    with pytest.raises(StoreError) as failure:
        fenced.execute("rec-1", actor("approver"), approved, "blocked")
    assert failure.value.code == "transport_pending"
    assert fenced.snapshot() == before
    # Safe actions remain possible; no switch or bypass flag is needed.
    fenced.decide("rec-2", actor("approver"), "reject", pending)
    fenced.decide("rec-1", actor("approver"), "revoke", approved)
    assert not fenced.snapshot()["effects"]


def test_material_delivery_does_not_reenable_old_advice(
    fenced: TransportRuntimeStore, clock: list[float]
) -> None:
    fingerprint = approve(fenced)
    fenced.transport.capture(fenced.transport_binding, [record(clock[0])])
    fenced.transport.dispatch_once(fenced.transport_binding, fenced.control)
    assert fenced.transport.verify_history(fenced.transport_binding)["delivered"] == [1, 0]
    with pytest.raises(StoreError):
        fenced.execute("rec-1", actor("approver"), fingerprint, "stale")
    assert not fenced.snapshot()["effects"]


def test_nonmaterial_delivery_and_receipt_retry_clear_fence_only_after_receipt(
    fenced: TransportRuntimeStore,
    clock: list[float],
) -> None:
    payload = event(clock[0], kind="telemetry.observed").model_copy(
        update={"source": "nexus.source-snapshot.v1"}
    )
    physical = TransportRecord(0, 0, b"portfolio", payload.model_dump_json().encode())
    fenced.transport.capture(fenced.transport_binding, [physical])

    def fail() -> None:
        raise RuntimeError("receipt not committed")

    with pytest.raises(RuntimeError, match="receipt not committed"):
        fenced.transport.dispatch_once(fenced.transport_binding, fenced.control, failpoint=fail)
    with pytest.raises(StoreError, match="awaits business admission"):
        approve(fenced)
    before = fenced.snapshot()
    receipt = fenced.transport.dispatch_once(fenced.transport_binding, fenced.control)
    assert receipt and receipt["admission"]["duplicate"] is True
    assert fenced.snapshot() == before
    fingerprint = approve(fenced)
    fenced.execute("rec-1", actor("approver"), fingerprint, "delivered")
    assert len(fenced.snapshot()["effects"]) == 1


def test_prior_effect_retrieval_is_not_new_execution_while_poison_pending(
    fenced: TransportRuntimeStore,
) -> None:
    fingerprint = approve(fenced)
    fenced.execute("rec-1", actor("approver"), fingerprint, "existing")
    fenced.transport.capture(fenced.transport_binding, [TransportRecord(0, 0, None, None)])
    fenced.execute("rec-1", actor("approver"), fingerprint, "existing")
    fenced.execute("rec-1", actor("approver"), fingerprint, "another-retry")
    with pytest.raises(StoreError, match="awaits business admission"):
        approve(fenced, "rec-2")
    assert len(fenced.snapshot()["effects"]) == 1


def test_binding_is_immutable_and_wrong_restarted_binding_refused(
    fenced: TransportRuntimeStore, cluster: Cluster
) -> None:
    for statement in (
        "UPDATE {schema}.runtime_transport_fence SET payload='x'",
        "DELETE FROM {schema}.runtime_transport_fence",
        "INSERT INTO {schema}.runtime_transport_fence VALUES (2,'x','x')",
    ):
        with pytest.raises(cluster.driver.errors.InsufficientPrivilege):
            cluster.query(fenced, statement, role="application")
    changed = replace(fenced.transport_binding, topic_id="different-topic")
    alternate = TransportRuntimeStore(
        fenced.config,
        transport=fenced.transport,
        binding=changed,
        runtime_root=fenced.runtime_root,
        clock=fenced.clock,
    )
    with pytest.raises(PostgresBackendError, match="Persisted runtime transport fence differs"):
        approve(alternate)


def test_capture_projection_reset_cannot_unfence_retained_poison(
    fenced: TransportRuntimeStore, cluster: Cluster
) -> None:
    fenced.transport.capture(fenced.transport_binding, [TransportRecord(0, 0, None, None)])
    cluster.query(
        fenced.transport,
        "UPDATE {schema}.transport_progress SET next_offset=0,head_hash=%s",
        ("0" * 64,),
    )
    with pytest.raises(PostgresBackendError, match="retained inbox tail"):
        approve(fenced)


def wait_for_capture_lock(cluster: Cluster, ledger: TransportLedger) -> None:
    deadline = monotonic() + 10
    while monotonic() < deadline:
        with cluster.connect("application") as db:
            rows = db.execute(
                "SELECT pid FROM pg_stat_activity WHERE pid<>pg_backend_pid() "
                "AND wait_event_type='Lock' AND query LIKE %s "
                "AND cardinality(pg_blocking_pids(pid))>0",
                ("%" + ledger.config.schema + "%",),
            ).fetchall()
        if rows:
            return
        sleep(0.025)
    pytest.fail("No PostgreSQL lock wait observed for the declared transport schema")


def test_capture_waits_until_execution_commit_not_just_precheck(
    fenced: TransportRuntimeStore,
    cluster: Cluster,
    clock: list[float],
) -> None:
    fingerprint = approve(fenced)
    gate, release = Event(), Event()

    def hold(stage: str) -> None:
        if stage == "transport_fence_acquired":
            gate.set()
            assert release.wait(15)

    fenced._failure_hook = hold
    with ThreadPoolExecutor(max_workers=2) as pool:
        execution = pool.submit(
            fenced.execute, "rec-1", actor("approver"), fingerprint, "race-once"
        )
        try:
            assert gate.wait(10)
            capture = pool.submit(
                fenced.transport.capture, fenced.transport_binding, [record(clock[0])]
            )
            wait_for_capture_lock(cluster, fenced.transport)
            assert not capture.done()
        finally:
            release.set()
        execution.result(timeout=10)
        capture.result(timeout=10)
    fenced._failure_hook = None
    assert len(fenced.snapshot()["effects"]) == 1
    with pytest.raises(StoreError, match="awaits business admission"):
        approve(fenced, "rec-2")


def test_capture_first_forces_wait_then_denial(
    fenced: TransportRuntimeStore, cluster: Cluster, clock: list[float]
) -> None:
    captured, release = Event(), Event()

    def hold() -> None:
        captured.set()
        assert release.wait(15)

    with ThreadPoolExecutor(max_workers=2) as pool:
        capture = pool.submit(
            fenced.transport.capture, fenced.transport_binding, [record(clock[0])], failpoint=hold
        )
        try:
            assert captured.wait(10)
            approval = pool.submit(approve, fenced)
            wait_for_capture_lock(cluster, fenced.transport)
            assert not approval.done()
        finally:
            release.set()
        capture.result(timeout=10)
        with pytest.raises(StoreError, match="awaits business admission"):
            approval.result(timeout=10)
    assert not fenced.snapshot()["effects"]


def test_approval_rechecks_expiry_after_fence_wait(
    fenced: TransportRuntimeStore,
    clock: list[float],
) -> None:
    original = fenced.snapshot()
    instant = clock[0]

    def expire(stage: str) -> None:
        if stage == "transport_fence_acquired":
            clock[0] += 86_401

    fenced._failure_hook = expire
    try:
        with pytest.raises(StoreError) as failure:
            approve(fenced)
        assert failure.value.code == "recommendation_expired"
    finally:
        fenced._failure_hook = None
        clock[0] = instant
    assert fenced.snapshot() == original


def test_new_schema_bootstrap_also_requires_exact_transport_binding(
    cluster: Cluster,
    tmp_path: Path,
    runtime: PostgresRuntimeStore,
    binding: TransportBinding,
    ledger: TransportLedger,
) -> None:
    from nexus.portfolio.transport_ledger import runtime_target

    config = cluster.backend(tmp_path / "new-runtime").config
    base = PostgresRuntimeStore(config)
    bound = replace(binding, runtime_target_sha256=runtime_target(base.control))
    new_ledger = TransportLedger(cluster.backend(tmp_path / "new-transport").config)
    cluster.migrate(new_ledger)
    new_ledger.register(bound)
    fresh = TransportRuntimeStore(config, transport=new_ledger, binding=bound)
    assert cluster.migrate(fresh)["applied"] is True
    assert fresh.snapshot()["seed"] is None
    assert cluster.migrate(fresh)["applied"] is False


def test_upgrade_preserves_approval_but_blocks_it_on_existing_backlog(
    runtime: PostgresRuntimeStore,
    ledger: TransportLedger,
    binding: TransportBinding,
    cluster: Cluster,
    clock: list[float],
) -> None:
    fingerprint = approve(runtime)
    ledger.capture(binding, [record(clock[0])])
    original = runtime.snapshot()
    store = TransportRuntimeStore(
        runtime.config,
        runtime_root=runtime.runtime_root,
        transport=ledger,
        binding=binding,
        clock=runtime.clock,
    )
    cluster.migrate(store)
    assert store.snapshot() == original
    with pytest.raises(StoreError, match="awaits business admission"):
        store.execute("rec-1", actor("approver"), fingerprint, "pre-upgrade")
    assert store.snapshot() == original


def test_dispatch_waiting_for_workflow_does_not_deadlock_fence(
    fenced: TransportRuntimeStore,
    cluster: Cluster,
    clock: list[float],
) -> None:
    fenced.transport.capture(fenced.transport_binding, [record(clock[0])])
    with ThreadPoolExecutor(max_workers=1) as pool:
        with fenced.write_transaction() as unit:
            dispatched = pool.submit(
                fenced.transport.dispatch_once, fenced.transport_binding, fenced.control
            )
            # Dispatcher holds delivery rows and waits for this workflow transaction.
            deadline = monotonic() + 10
            waiting = False
            while monotonic() < deadline:
                with cluster.connect("application") as db:
                    waiting = bool(
                        db.execute(
                            "SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' "
                            "AND query LIKE %s AND cardinality(pg_blocking_pids(pid))>0",
                            ("%" + fenced.config.schema + "%",),
                        ).fetchall()
                    )
                if waiting:
                    break
                sleep(0.025)
            assert waiting, "Dispatch did not reach its workflow lock"
            with pytest.raises(StoreError, match="awaits business admission"):
                fenced._action_gate(unit)
        assert dispatched.result(timeout=10) is not None
    assert fenced.transport.verify_history(fenced.transport_binding)["delivered"] == [1, 0]
