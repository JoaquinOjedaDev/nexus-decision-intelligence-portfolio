"""Real isolated PostgreSQL recovery journal; decisions are not delivery permits."""

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import actor, event
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime
from test_portfolio_transport_ledger_service import binding as binding
from test_portfolio_transport_ledger_service import ledger as ledger

from nexus.contracts import ActorRole
from nexus.portfolio.broker_runtime import BrokerAdmissionError
from nexus.portfolio.postgres_backend import PostgresBackendError
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.recovery_ledger import RecoveryTransportLedger
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.store import StoreError, canonical, digest
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger, TransportRecord
from nexus.portfolio.transport_recovery import RuntimeDeadLetter, RuntimeRedriveRequest
from nexus.simulation import generate_r2_smoke_fixture

pytestmark = pytest.mark.portfolio_service


@pytest.fixture
def recovery(
    ledger: TransportLedger, cluster: Cluster, clock: list[float]
) -> RecoveryTransportLedger:
    result = RecoveryTransportLedger(ledger.config, clock=lambda: clock[0])
    assert cluster.migrate(result)["applied"] is True
    return result


@pytest.fixture
def pending(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    cluster: Cluster,
    tmp_path: Path,
    clock: list[float],
) -> SimpleNamespace:
    physical = TransportRecord(0, 0, b"portfolio", b"invalid original\x00\xff")
    recovery.capture(binding, [physical])
    original_hash = cluster.query(recovery, "SELECT record_hash FROM {schema}.transport_inbox")[0][
        0
    ]
    dead = RuntimeDeadLetter.create(binding, physical, original_hash)
    generate_r2_smoke_fixture(tmp_path / "source")
    bundle = SourceBundle.capture(tmp_path / "source")
    replacement = (
        event(clock[0])
        .model_copy(
            update={
                "source": "nexus.source-snapshot.v1",
                "payload": {"source_snapshot_sha256": bundle.source_sha256},
            }
        )
        .model_dump_json()
        .encode()
    )
    request = RuntimeRedriveRequest.create(
        dead,
        binding,
        physical,
        original_hash,
        bundle,
        replacement,
        operator=actor(),
        reason="Correct malformed source envelope",
        submitted_at=datetime.fromtimestamp(clock[0], UTC),
    )
    return SimpleNamespace(
        physical=physical, original_hash=original_hash, dead=dead, bundle=bundle, request=request
    )


def retain(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    **kwargs: object,
) -> dict[str, object]:
    return recovery.retain_request(
        binding, 0, pending.original_hash, pending.bundle, pending.request, actor(), **kwargs
    )


def review(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    retained: dict[str, object],
    **kwargs: object,
) -> dict[str, object]:
    options = dict(
        decision_id=str(uuid4()),
        action="approve",
        reason="Checked exact original and replacement",
        ttl_seconds=60,
    )
    options.update(kwargs)
    return recovery.review_request(
        binding, retained["request_id"], retained["request_sha256"], actor("approver"), **options
    )


def test_migration_reapply_and_old_client_refusal(
    recovery: RecoveryTransportLedger,
    ledger: TransportLedger,
    binding: TransportBinding,
    cluster: Cluster,
) -> None:
    assert cluster.migrate(recovery)["applied"] is False
    assert cluster.migrate(recovery)["version"] == 9
    assert cluster.query(
        recovery, "SELECT version FROM {schema}.schema_migrations ORDER BY version"
    ) == [(5,), (9,)]
    with pytest.raises(PostgresBackendError):
        ledger.verify_history(binding)


def test_source_request_and_review_survive_reconstruction_without_delivery(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    runtime: PostgresRuntimeStore,
    clock: list[float],
) -> None:
    before = recovery.verify_history(binding)
    retained = retain(recovery, binding, pending)
    assert not retained["duplicate"] and retain(recovery, binding, pending)["duplicate"]
    decision = review(recovery, binding, retained)
    reopened = RecoveryTransportLedger(recovery.config, clock=lambda: clock[0])
    status = reopened.review_status(binding, retained["request_id"], actor("auditor"))
    assert status["review_valid"] and status["decision_id"] == decision["decision_id"]
    after = reopened.verify_history(binding)
    assert {k: v for k, v in after.items() if k != "recovery_journal"} == {
        k: v for k, v in before.items() if k != "recovery_journal"
    }
    assert after["recovery_journal"] == {
        "cases": 1,
        "sources": 1,
        "requests": 1,
        "decisions": 1,
        "claim": "verified_retained_history_not_redrive_authorization",
    }
    with pytest.raises(BrokerAdmissionError):
        reopened.dispatch_once(binding, runtime.control)
    assert runtime.control.snapshot(actor())["event_count"] == 0


def test_failed_retention_rolls_back_all_new_evidence(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    cluster: Cluster,
) -> None:
    def fail() -> None:
        raise RuntimeError("rollback request")

    with pytest.raises(RuntimeError):
        retain(recovery, binding, pending, failpoint=fail)
    for table in ("cases", "sources", "requests"):
        assert cluster.query(
            recovery, "SELECT COUNT(*) FROM {schema}.transport_recovery_" + table
        ) == [(0,)]
    assert recovery.verify_history(binding)["delivered"] == [0, 0]


def test_concurrent_retain_has_one_proposal(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    cluster: Cluster,
) -> None:
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: retain(recovery, binding, pending), range(2)))
    assert sum(not r["duplicate"] for r in results) == 1
    assert cluster.query(recovery, "SELECT COUNT(*) FROM {schema}.transport_recovery_requests") == [
        (1,)
    ]


def test_review_retry_revocation_and_terminal_transitions(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
) -> None:
    retained = retain(recovery, binding, pending)
    identity = str(uuid4())
    review(recovery, binding, retained, decision_id=identity)
    assert review(recovery, binding, retained, decision_id=identity)["duplicate"]
    with pytest.raises(StoreError, match="already bound"):
        review(recovery, binding, retained, decision_id=identity, reason="different")
    review(recovery, binding, retained, action="revoke")
    assert recovery.review_status(binding, retained["request_id"], actor())["reason"] == "revoke"
    with pytest.raises(StoreError, match="transition"):
        review(recovery, binding, retained)


def test_review_expiry_and_clock_regression_are_not_permissions(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    clock: list[float],
) -> None:
    retained = retain(recovery, binding, pending)
    review(recovery, binding, retained)
    clock[0] += 60
    assert recovery.review_status(binding, retained["request_id"], actor())["reason"] == "expired"
    clock[0] -= 61
    with pytest.raises(StoreError):
        recovery.review_status(binding, retained["request_id"], actor())


def test_rejection_cannot_be_reapproved_in_place(
    recovery: RecoveryTransportLedger, binding: TransportBinding, pending: SimpleNamespace
) -> None:
    retained = retain(recovery, binding, pending)
    review(recovery, binding, retained, action="reject")
    assert not recovery.review_status(binding, retained["request_id"], actor())["review_valid"]
    with pytest.raises(StoreError, match="transition"):
        review(recovery, binding, retained)


@pytest.mark.parametrize("role", ["planner", "auditor", "worker"])
def test_only_approver_may_review(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    role: str,
) -> None:
    retained = retain(recovery, binding, pending)
    with pytest.raises(StoreError) as failure:
        recovery.review_request(
            binding,
            retained["request_id"],
            retained["request_sha256"],
            actor(role),
            decision_id=str(uuid4()),
            action="approve",
            reason="checked",
        )
    assert failure.value.code == "role_denied"


def test_maker_checker_and_exact_hash_and_scope(
    recovery: RecoveryTransportLedger, binding: TransportBinding, pending: SimpleNamespace
) -> None:
    retained = retain(recovery, binding, pending)
    maker_as_reviewer = replace(actor(), role=ActorRole.APPROVER)
    with pytest.raises(StoreError) as failure:
        recovery.review_request(
            binding,
            retained["request_id"],
            retained["request_sha256"],
            maker_as_reviewer,
            decision_id=str(uuid4()),
            action="approve",
            reason="checked",
        )
    assert failure.value.code == "maker_checker_violation"
    with pytest.raises(StoreError, match="exact retained"):
        review(recovery, binding, {**retained, "request_sha256": "c" * 64})
    with pytest.raises(StoreError) as failure:
        recovery.review_status(binding, retained["request_id"], replace(actor(), locations=()))
    assert failure.value.code == "scope_denied"


def test_failed_review_rolls_back_without_sequence_gap(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    cluster: Cluster,
) -> None:
    retained = retain(recovery, binding, pending)

    def fail() -> None:
        raise RuntimeError("rollback review")

    with pytest.raises(RuntimeError):
        review(recovery, binding, retained, failpoint=fail)
    assert (
        recovery.review_status(binding, retained["request_id"], actor())["reason"]
        == "awaiting_review"
    )
    review(recovery, binding, retained)
    assert cluster.query(
        recovery, "SELECT sequence FROM {schema}.transport_recovery_decisions"
    ) == [(1,)]


def test_concurrent_review_has_one_authoritative_initial_decision(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    cluster: Cluster,
) -> None:
    retained = retain(recovery, binding, pending)

    def attempt(_: int) -> str:
        try:
            review(recovery, binding, retained)
            return "accepted"
        except StoreError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, range(2)))
    assert sorted(results) == ["accepted", "invalid_recovery_transition"]
    assert cluster.query(
        recovery, "SELECT COUNT(*) FROM {schema}.transport_recovery_decisions"
    ) == [(1,)]


def test_stale_selected_inbox_is_refused_without_retaining(
    recovery: RecoveryTransportLedger, binding: TransportBinding, pending: SimpleNamespace
) -> None:
    with pytest.raises(StoreError) as failure:
        recovery.retain_request(binding, 0, "c" * 64, pending.bundle, pending.request, actor())
    assert failure.value.code == "stale_recovery_target"


@pytest.mark.parametrize("ttl", [0, 901, True, 60.0])
def test_review_ttl_is_bounded(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    ttl: object,
) -> None:
    retained = retain(recovery, binding, pending)
    with pytest.raises(StoreError) as failure:
        review(recovery, binding, retained, ttl_seconds=ttl)
    assert failure.value.code == "invalid_recovery_decision"


def test_journal_rows_are_immutable_to_application(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    cluster: Cluster,
) -> None:
    retained = retain(recovery, binding, pending)
    review(recovery, binding, retained)
    for table in ("cases", "sources", "requests", "decisions"):
        with pytest.raises(cluster.driver.errors.InsufficientPrivilege):
            cluster.query(
                recovery, "DELETE FROM {schema}.transport_recovery_" + table, role="application"
            )


def test_self_rehashed_second_approval_is_refused_by_semantic_replay(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    cluster: Cluster,
) -> None:
    retained = retain(recovery, binding, pending)
    approved = review(recovery, binding, retained)
    value = {k: v for k, v in approved.items() if k != "duplicate"}
    value["decision_id"] = str(uuid4())
    cluster.query(
        recovery,
        "INSERT INTO {schema}.transport_recovery_decisions VALUES (%s,%s,2,%s,%s)",
        (value["decision_id"], retained["request_id"], canonical(value), digest(value)),
        role="application",
    )
    with pytest.raises(PostgresBackendError, match="semantics differ"):
        recovery.review_status(binding, retained["request_id"], actor())


def test_retained_source_has_exact_bundle_bytes(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    cluster: Cluster,
) -> None:
    retain(recovery, binding, pending)
    encoded = cluster.query(recovery, "SELECT payload FROM {schema}.transport_recovery_sources")[0][
        0
    ]
    assert json.loads(encoded) == {"bundle": pending.bundle.payload}


def test_review_uses_retained_source_after_original_path_is_unavailable(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    tmp_path: Path,
) -> None:
    retained = retain(recovery, binding, pending)
    (tmp_path / "source").rename(tmp_path / "source-preserved")
    review(recovery, binding, retained)
    assert recovery.review_status(binding, retained["request_id"], actor())["review_valid"]


def test_approved_recovery_cannot_bypass_publication_runtime_action_fence(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    runtime: PostgresRuntimeStore,
    cluster: Cluster,
) -> None:
    publication = PublicationRuntimeStore(
        runtime.config,
        transport=recovery,
        binding=binding,
        runtime_root=runtime.runtime_root,
        clock=runtime.clock,
    )
    cluster.migrate(publication)
    retained = retain(recovery, binding, pending)
    review(recovery, binding, retained)
    fingerprint = publication.recommendation("rec-1", actor())["fingerprint"]
    with pytest.raises(StoreError) as failure:
        publication.decide("rec-1", actor("approver"), "approve", fingerprint)
    assert failure.value.code == "transport_pending"
    assert not publication.snapshot()["effects"]
    assert recovery.verify_history(binding)["delivered"] == [0, 0]


def test_concurrent_fresh_migration_then_registration(
    cluster: Cluster, tmp_path: Path, binding: TransportBinding
) -> None:
    config = cluster.backend(tmp_path / "new-recovery").config
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda _: cluster.migrate(RecoveryTransportLedger(config)), range(2))
        )
    assert sum(r["applied"] for r in results) == 1
    restarted = RecoveryTransportLedger(config)
    restarted.register(binding)
    assert restarted.verify_history(binding)["captured"] == [0, 0]


@pytest.mark.parametrize("kind", ["cases", "sources"])
def test_full_replay_refuses_detached_evidence(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    cluster: Cluster,
    kind: str,
) -> None:
    if kind == "cases":
        identity = cluster.query(recovery, "SELECT id FROM {schema}.transport_inbox")[0][0]
        value = json.loads(pending.dead.payload)
    else:
        identity = pending.bundle.source_sha256
        value = {"bundle": pending.bundle.payload}
    cluster.query(
        recovery,
        "INSERT INTO {schema}.transport_recovery_" + kind + " VALUES (%s,%s,%s)",
        (identity, canonical(value), digest(value)),
        role="application",
    )
    with pytest.raises(PostgresBackendError, match="detached evidence"):
        recovery.verify_history(binding)


def test_full_replay_refuses_gap_even_when_each_review_is_legal(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    cluster: Cluster,
) -> None:
    retained = retain(recovery, binding, pending)
    approved = review(recovery, binding, retained)
    value = {k: v for k, v in approved.items() if k != "duplicate"}
    value.update(decision_id=str(uuid4()), action="revoke", expires_at=None)
    cluster.query(
        recovery,
        "INSERT INTO {schema}.transport_recovery_decisions VALUES (%s,%s,3,%s,%s)",
        (value["decision_id"], retained["request_id"], canonical(value), digest(value)),
        role="application",
    )
    assert recovery.review_status(binding, retained["request_id"], actor())["reason"] == "revoke"
    with pytest.raises(PostgresBackendError, match="sequence contains a gap"):
        recovery.verify_history(binding)


def test_full_replay_keeps_expired_then_revoked_reviews_as_history(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    clock: list[float],
) -> None:
    retained = retain(recovery, binding, pending)
    review(recovery, binding, retained)
    clock[0] += 61
    assert recovery.review_status(binding, retained["request_id"], actor())["reason"] == "expired"
    assert recovery.verify_history(binding)["recovery_journal"]["decisions"] == 1
    review(recovery, binding, retained, action="revoke")
    assert recovery.verify_history(binding)["recovery_journal"]["decisions"] == 2


def test_full_replay_does_not_miss_second_invalid_review(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    cluster: Cluster,
) -> None:
    retained = retain(recovery, binding, pending)
    approved = review(recovery, binding, retained)
    value = {k: v for k, v in approved.items() if k != "duplicate"}
    value["decision_id"] = str(uuid4())
    cluster.query(
        recovery,
        "INSERT INTO {schema}.transport_recovery_decisions VALUES (%s,%s,2,%s,%s)",
        (value["decision_id"], retained["request_id"], canonical(value), digest(value)),
        role="application",
    )
    with pytest.raises(PostgresBackendError, match="semantics differ"):
        recovery.verify_history(binding)


def test_full_replay_crosses_request_page_boundary(
    recovery: RecoveryTransportLedger,
    binding: TransportBinding,
    pending: SimpleNamespace,
    cluster: Cluster,
) -> None:
    retain(recovery, binding, pending)
    case_id = cluster.query(recovery, "SELECT id FROM {schema}.transport_recovery_cases")[0][0]
    original = json.loads(pending.request.payload)
    rows = []
    for index in range(1000):
        identity = {k: v for k, v in original.items() if k != "request_id"}
        identity["reason"] = f"Independent historical proposal {index}"
        value = {**identity, "request_id": digest(identity)}
        rows.append(
            (
                value["request_id"],
                case_id,
                pending.bundle.source_sha256,
                canonical(value),
                digest(value),
            )
        )
    statement = cluster.driver.sql.SQL(
        "INSERT INTO {}.transport_recovery_requests VALUES (%s,%s,%s,%s,%s)"
    ).format(cluster.driver.sql.Identifier(recovery.config.schema))
    with cluster.connect("application") as db, db.cursor() as cursor:
        cursor.executemany(statement, rows)
    evidence = recovery.verify_history(binding)
    assert evidence["recovery_journal"]["requests"] == 1001
    assert evidence["recovery_journal"]["cases"] == 1
    assert evidence["recovery_journal"]["sources"] == 1
    assert evidence["delivered"] == [0, 0]
