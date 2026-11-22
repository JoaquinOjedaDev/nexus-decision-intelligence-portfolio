"""Opt-in actual PostgreSQL inbox/recovery evidence in fresh guarded test schemas."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import LOCATIONS, actor, event
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime

from nexus.portfolio.broker_runtime import BrokerAdmissionError, RuntimeBrokerConfig
from nexus.portfolio.postgres_backend import PostgresBackendError
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.transport_ledger import (
    TransportBinding,
    TransportLedger,
    TransportRecord,
    runtime_target,
)

pytestmark = pytest.mark.portfolio_service


@pytest.fixture
def binding(runtime: PostgresRuntimeStore) -> TransportBinding:
    return TransportBinding(
        RuntimeBrokerConfig(
            "127.0.0.1:19092", "nexus-runtime-ledger-test", "nexus-runtime-ledger-ops", LOCATIONS
        ),
        "constructed-cluster",
        "constructed-topic-uuid",
        2,
        "operations",
        runtime_target(runtime.control),
    )


@pytest.fixture
def ledger(cluster: Cluster, tmp_path: Path, binding: TransportBinding) -> TransportLedger:
    result = TransportLedger(cluster.backend(tmp_path / "transport").config)
    receipt = cluster.migrate(result)
    assert receipt["version"] == 5 and receipt["applied"] is True
    result.register(binding)
    return result


def record(now: float, offset: int = 0, partition: int = 0) -> TransportRecord:
    payload = event(now).model_copy(update={"source": "nexus.source-snapshot.v1"})
    return TransportRecord(
        partition,
        offset,
        b"portfolio",
        payload.model_dump_json().encode(),
        (("repeat", b"\xff"), ("repeat", None)),
        1,
        int(now * 1000),
    )


def fail() -> None:
    raise RuntimeError("injected transaction boundary")


def test_migration_reapply_and_binding_refuse_reassignment(
    ledger: TransportLedger,
    cluster: Cluster,
    binding: TransportBinding,
) -> None:
    assert cluster.migrate(ledger)["applied"] is False
    ledger.register(binding)
    assert ledger.verify_history(binding)["captured"] == [0, 0]
    with pytest.raises(PostgresBackendError, match="cannot be rebound"):
        ledger.register(replace(binding, topic_id="replacement-topic"))
    with pytest.raises(PostgresBackendError, match="differs or is absent"):
        ledger.capture(replace(binding, cluster_id="other-cluster"), [record(1_800_000_000)])


def test_atomic_capture_duplicate_bytes_replay_and_reconciliation(
    ledger: TransportLedger,
    binding: TransportBinding,
    clock: list[float],
) -> None:
    one = record(clock[0])
    two = record(clock[0], partition=1)
    result = ledger.capture(binding, [one, two, replace(one, offset=1)])
    assert result["next_offsets"] == [2, 1] and result["inserted"] == 3
    assert ledger.capture(binding, [one])["duplicates"] == 1
    assert ledger.verify_history(binding)["captured"] == [2, 1]
    assert ledger.reconcile(binding, [0, 0], [(0, 2), (0, 1)]) == [2, 1]
    for committed, bounds in (
        ([3, 1], [(0, 3), (0, 1)]),
        ([0, 0], [(3, 3), (0, 1)]),
        ([-1, 0], [(0, 2), (0, 1)]),
        ([0, 0], [(0, 1), (0, 1)]),
    ):
        with pytest.raises(PostgresBackendError, match="durable proof"):
            ledger.reconcile(binding, committed, bounds)


def test_conflicting_physical_redelivery_is_not_business_deduplication(
    ledger: TransportLedger,
    binding: TransportBinding,
    clock: list[float],
) -> None:
    one = record(clock[0])
    ledger.capture(binding, [one])
    with pytest.raises(PostgresBackendError, match="durable bytes"):
        ledger.capture(binding, [replace(one, headers=(("different", b"header"),))])
    assert ledger.verify_history(binding)["captured"] == [1, 0]


def test_gap_and_injected_failure_rollback_entire_batch(
    ledger: TransportLedger,
    binding: TransportBinding,
    clock: list[float],
) -> None:
    one = record(clock[0])
    with pytest.raises(PostgresBackendError, match="skip an offset"):
        ledger.capture(binding, [one, replace(one, offset=2)])
    assert ledger.verify_history(binding)["captured"] == [0, 0]
    with pytest.raises(RuntimeError, match="injected"):
        ledger.capture(binding, [one], failpoint=fail)
    assert ledger.verify_history(binding)["captured"] == [0, 0]


def test_business_commit_receipt_crash_replays_without_second_job(
    ledger: TransportLedger,
    binding: TransportBinding,
    runtime: PostgresRuntimeStore,
    clock: list[float],
) -> None:
    ledger.capture(binding, [record(clock[0])])
    assert runtime.control.snapshot(actor())["event_count"] == 0
    with pytest.raises(RuntimeError, match="injected"):
        ledger.dispatch_once(binding, runtime.control, failpoint=fail)
    first = runtime.snapshot()
    assert ledger.verify_history(binding)["delivered"] == [0, 0]
    retry = ledger.dispatch_once(binding, runtime.control)
    assert retry is not None and retry["admission"]["duplicate"] is True
    assert runtime.snapshot() == first
    assert ledger.verify_history(binding)["delivered"] == [1, 0]
    assert ledger.dispatch_once(binding, runtime.control) is None


def test_audit_group_has_independent_progress_and_no_dispatch(
    ledger: TransportLedger,
    cluster: Cluster,
    binding: TransportBinding,
    runtime: PostgresRuntimeStore,
    tmp_path: Path,
    clock: list[float],
) -> None:
    audit_binding = replace(
        binding,
        broker=replace(binding.broker, group_id="nexus-runtime-ledger-audit"),
        purpose="audit",
        runtime_target_sha256=None,
    )
    audit = TransportLedger(cluster.backend(tmp_path / "audit").config)
    cluster.migrate(audit)
    audit.register(audit_binding)
    one = record(clock[0])
    ledger.capture(binding, [one])
    assert audit.verify_history(audit_binding)["captured"] == [0, 0]
    audit.capture(audit_binding, [one])
    assert audit.verify_history(audit_binding)["captured"] == [1, 0]
    assert audit.verify_history(audit_binding)["delivered"] == []
    with pytest.raises(PostgresBackendError, match="bound operations"):
        audit.dispatch_once(audit_binding, runtime.control)
    with pytest.raises(PostgresBackendError, match="bound operations"):
        ledger.dispatch_once(replace(binding, runtime_target_sha256="f" * 64), runtime.control)
    assert runtime.control.snapshot(actor())["event_count"] == 0


def test_concurrent_capture_and_dispatch_keep_one_physical_and_business_record(
    ledger: TransportLedger,
    binding: TransportBinding,
    runtime: PostgresRuntimeStore,
    clock: list[float],
) -> None:
    one = record(clock[0])
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: ledger.capture(binding, [one]), range(2)))
    assert sum(item["inserted"] for item in results) == 1
    assert sum(item["duplicates"] for item in results) == 1
    with ThreadPoolExecutor(max_workers=2) as pool:
        dispatched = list(
            pool.map(lambda _: ledger.dispatch_once(binding, runtime.control), range(2))
        )
    assert sum(item is not None for item in dispatched) == 1
    assert runtime.control.snapshot(actor())["event_count"] == 1
    assert ledger.verify_history(binding)["delivered"] == [1, 0]


def test_poison_is_retained_without_advancing_business_progress(
    ledger: TransportLedger,
    binding: TransportBinding,
    runtime: PostgresRuntimeStore,
) -> None:
    ledger.capture(binding, [TransportRecord(0, 0, None, None)])
    with pytest.raises(BrokerAdmissionError, match="invalid_payload_size"):
        ledger.dispatch_once(binding, runtime.control)
    state = ledger.verify_history(binding)
    assert state["captured"] == [1, 0] and state["delivered"] == [0, 0]


def test_owner_progress_tampering_is_detected_by_full_replay(
    ledger: TransportLedger,
    cluster: Cluster,
    binding: TransportBinding,
    clock: list[float],
) -> None:
    ledger.capture(binding, [record(clock[0])])
    cluster.query(
        ledger,
        "UPDATE {schema}.transport_progress SET next_offset=0,head_hash=%s WHERE partition=0",
        ("0" * 64,),
    )
    with pytest.raises(PostgresBackendError, match="full history"):
        ledger.verify_history(binding)


def test_application_cannot_rewrite_history_or_apply_migration(
    ledger: TransportLedger,
    cluster: Cluster,
    binding: TransportBinding,
) -> None:
    with pytest.raises(cluster.driver.Error):
        cluster.query(ledger, "DELETE FROM {schema}.transport_binding", role="application")
    with pytest.raises(PostgresBackendError, match="Separate bootstrap"):
        ledger.migrate(migration_dsn=cluster.config["application_dsn"])
    assert ledger.verify_history(binding)["captured"] == [0, 0]
