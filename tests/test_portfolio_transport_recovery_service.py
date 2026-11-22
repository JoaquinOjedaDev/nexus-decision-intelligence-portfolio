"""Real PostgreSQL poison inspection; no redrive or broker topic proof is claimed."""

import json
from dataclasses import replace

import pytest
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import actor
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime
from test_portfolio_transport_ledger_service import binding as binding
from test_portfolio_transport_ledger_service import ledger as ledger
from test_portfolio_transport_ledger_service import record

from nexus.portfolio.broker_runtime import BrokerAdmissionError
from nexus.portfolio.postgres_backend import PostgresBackendError
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.store import StoreError
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger, TransportRecord

pytestmark = pytest.mark.portfolio_service


def test_inspection_is_bound_to_pending_bytes_and_does_not_clear_poison(
    ledger: TransportLedger,
    binding: TransportBinding,
    runtime: PostgresRuntimeStore,
    cluster: Cluster,
) -> None:
    physical = TransportRecord(0, 0, b"portfolio", b"\xff\x00poison", (("a", b""), ("a", None)))
    ledger.capture(binding, [physical])
    before = ledger.verify_history(binding)
    with pytest.raises(BrokerAdmissionError):
        ledger.dispatch_once(binding, runtime.control)
    dead = ledger.pending_dead_letter(binding, 0, actor())
    assert dead is not None
    original_hash = cluster.query(ledger, "SELECT record_hash FROM {schema}.transport_inbox")[0][0]
    data = dead.verify(binding, physical, original_hash)
    assert data["physical"] == physical.payload()
    assert ledger.pending_dead_letter(binding, 0, actor()) == dead
    assert ledger.verify_history(binding) == before
    with pytest.raises(BrokerAdmissionError):
        ledger.dispatch_once(binding, runtime.control)
    assert runtime.control.snapshot(actor())["event_count"] == 0
    assert cluster.query(ledger, "SELECT COUNT(*) FROM {schema}.transport_receipts") == [(0,)]


def test_empty_partition_has_no_recovery_candidate(
    ledger: TransportLedger, binding: TransportBinding
) -> None:
    assert ledger.pending_dead_letter(binding, 0, actor()) is None


def test_compatible_pending_record_is_not_a_drop_candidate(
    ledger: TransportLedger, binding: TransportBinding, clock: list[float]
) -> None:
    ledger.capture(binding, [record(clock[0])])
    with pytest.raises(StoreError) as failure:
        ledger.pending_dead_letter(binding, 0, actor())
    assert failure.value.code == "not_schema_poison"
    assert ledger.verify_history(binding)["delivered"] == [0, 0]


@pytest.mark.parametrize("role", ["planner", "approver", "auditor"])
def test_audit_capable_roles_can_inspect_full_scope(
    ledger: TransportLedger, binding: TransportBinding, role: str
) -> None:
    ledger.capture(binding, [TransportRecord(0, 0, None, None)])
    candidate = ledger.pending_dead_letter(binding, 0, actor(role))
    assert (
        candidate is not None
        and json.loads(candidate.payload)["classification"] == "invalid_payload_size"
    )


def test_worker_and_partial_scope_denied(
    ledger: TransportLedger, binding: TransportBinding
) -> None:
    for principal, code in (
        (actor("worker"), "role_denied"),
        (replace(actor(), locations=()), "scope_denied"),
    ):
        with pytest.raises(StoreError) as failure:
            ledger.pending_dead_letter(binding, 0, principal)
        assert failure.value.code == code


@pytest.mark.parametrize("partition", [-1, 2, True, "0"])
def test_partition_cannot_be_selected_implicitly(
    ledger: TransportLedger, binding: TransportBinding, partition: object
) -> None:
    with pytest.raises(StoreError):
        ledger.pending_dead_letter(binding, partition, actor())  # type: ignore[arg-type]


def test_rebound_broker_identity_is_refused(
    ledger: TransportLedger, binding: TransportBinding
) -> None:
    with pytest.raises(PostgresBackendError):
        ledger.pending_dead_letter(replace(binding, topic_id="other"), 0, actor())


def test_inspection_tracks_current_head_not_a_later_poison(
    ledger: TransportLedger,
    binding: TransportBinding,
    runtime: PostgresRuntimeStore,
    clock: list[float],
) -> None:
    compatible = record(clock[0])
    poisoned = TransportRecord(0, 1, b"portfolio", b"invalid")
    ledger.capture(binding, [compatible, poisoned])
    with pytest.raises(StoreError, match="compatible envelope"):
        ledger.pending_dead_letter(binding, 0, actor())
    ledger.dispatch_once(binding, runtime.control)
    dead = ledger.pending_dead_letter(binding, 0, actor())
    assert dead is not None and json.loads(dead.payload)["physical"]["offset"] == 1
    assert ledger.verify_history(binding)["delivered"] == [1, 0]
