"""Guarded native recovery outbox tests; broker doubles do not prove live delivery."""

import hashlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import actor
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime
from test_portfolio_recovery_ledger_service import pending as pending
from test_portfolio_recovery_ledger_service import retain, review
from test_portfolio_transport_ledger_service import binding as binding
from test_portfolio_transport_ledger_service import ledger as ledger

from nexus.contracts import ActorRole
from nexus.portfolio import recovery_publication as module
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_backend import PostgresBackendError
from nexus.portfolio.recovery_ledger import RecoveryTransportLedger
from nexus.portfolio.recovery_publication import RecoveryPublicationLedger
from nexus.portfolio.recovery_publisher import RecoveryTopic
from nexus.portfolio.store import StoreError, canonical, digest

pytestmark = pytest.mark.portfolio_service


@pytest.fixture
def recovery(ledger, cluster: Cluster, clock) -> RecoveryPublicationLedger:
    result = RecoveryPublicationLedger(ledger.config, clock=lambda: clock[0])
    assert cluster.migrate(result)["version"] == 10
    return result


@pytest.fixture
def prepared(recovery, binding, pending):
    topics = [
        RecoveryTopic(
            binding,
            replace(binding.broker, topic="nexus-runtime-" + name.replace("_", "-")),
            str(index) * 32,
            name,
        )
        for index, name in enumerate(("dead_letter", "redrive_audit"), start=3)
    ]
    recovery.enroll_topics(binding, *topics, actor("approver"))
    retained = retain(recovery, binding, pending)
    decision = review(recovery, binding, retained)
    return SimpleNamespace(retained=retained, decision=decision, topics=topics)


def enqueue(recovery, binding, prepared, **kwargs):
    return recovery.enqueue_evidence(
        binding,
        prepared.retained["request_id"],
        prepared.retained["request_sha256"],
        prepared.decision["decision_id"],
        actor("approver"),
        **kwargs,
    )


def worker(binding):
    return Actor(
        "recovery-publisher", "Recovery worker", ActorRole.SYSTEM, binding.broker.locations
    )


@pytest.fixture
def publisher(monkeypatch):
    calls = []

    class Publisher:
        def __init__(self, topic):
            self.topic = topic

        def publish(self, intent_id, payload):
            calls.append((intent_id, payload))
            return {
                "contract": "nexus.recovery-publication-observation.v1",
                "intent_id": intent_id,
                "topic_binding_sha256": digest(self.topic.payload()),
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "key_sha256": hashlib.sha256(intent_id.encode()).hexdigest(),
                "partition": 0,
                "offset": len(calls) - 1,
                "claim": "broker_ack_and_exact_readback_not_recovery_authorization",
            }

    monkeypatch.setattr(module, "RecoveryEvidencePublisher", Publisher)
    return calls


def test_migration_upgrade_and_old_client_refusal(recovery, cluster, binding):
    assert not cluster.migrate(recovery)["applied"]
    with pytest.raises(PostgresBackendError):
        RecoveryTransportLedger(recovery.config).verify_history(binding)
    assert recovery.verify_history(binding)["recovery_publication"]["intents"] == 0


def test_atomic_idempotent_pair_and_restart(recovery, binding, prepared, clock):
    ids = enqueue(recovery, binding, prepared)
    assert len(ids) == 2 and len(set(ids)) == 2
    clock[0] += 1
    assert enqueue(recovery, binding, prepared) == ids
    reopened = RecoveryPublicationLedger(recovery.config, clock=lambda: clock[0])
    result = reopened.verify_history(binding)
    assert result["recovery_publication"] == {
        "intents": 2,
        "receipts": 0,
        "claim": "evidence_publication_not_delivery_resolution",
    }
    assert result["delivered"] == [0, 0]


def test_intent_rollback_leaves_neither_half(recovery, binding, prepared):
    def fail():
        raise RuntimeError("enqueue rollback")

    with pytest.raises(RuntimeError):
        enqueue(recovery, binding, prepared, failpoint=fail)
    assert recovery.verify_history(binding)["recovery_publication"]["intents"] == 0


def test_uncertain_broker_commit_retries_exact_intent(
    recovery, binding, prepared, publisher, clock
):
    enqueue(recovery, binding, prepared)

    def fail():
        raise RuntimeError("after broker commit")

    with pytest.raises(RuntimeError):
        recovery.publish_once(binding, worker(binding), failpoint=fail)
    assert recovery.verify_history(binding)["recovery_publication"]["receipts"] == 0
    restarted = RecoveryPublicationLedger(recovery.config, clock=lambda: clock[0])
    restarted.publish_once(binding, worker(binding))
    assert publisher[0] == publisher[1]
    restarted.publish_once(binding, worker(binding))
    assert restarted.publish_once(binding, worker(binding)) is None
    evidence = restarted.verify_history(binding)
    assert evidence["recovery_publication"]["receipts"] == 2 and evidence["delivered"] == [0, 0]


@pytest.mark.parametrize("action", ["expired", "revoked", "unreviewed"])
def test_no_intent_from_invalid_review(recovery, binding, prepared, clock, action):
    if action == "expired":
        clock[0] += 60
    elif action == "revoked":
        review(recovery, binding, prepared.retained, action="revoke")
    else:
        prepared.decision["decision_id"] = "not-current"
    with pytest.raises(StoreError, match="current unexpired"):
        enqueue(recovery, binding, prepared)
    assert recovery.verify_history(binding)["recovery_publication"]["intents"] == 0


def test_later_revocation_does_not_erase_historical_publication(
    recovery, binding, prepared, publisher
):
    enqueue(recovery, binding, prepared)
    review(recovery, binding, prepared.retained, action="revoke")
    recovery.publish_once(binding, worker(binding))
    assert (
        recovery.review_status(binding, prepared.retained["request_id"], actor())["reason"]
        == "revoke"
    )
    evidence = recovery.verify_history(binding)
    assert evidence["recovery_publication"]["receipts"] == 1 and evidence["delivered"] == [0, 0]


def test_concurrent_publishers_retain_one_receipt_each(recovery, binding, prepared, publisher):
    enqueue(recovery, binding, prepared)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda _: recovery.publish_once(binding, worker(binding)), range(2))
        )
    assert results[0]["intent_id"] != results[1]["intent_id"]
    assert len(publisher) == 2
    assert recovery.verify_history(binding)["recovery_publication"]["receipts"] == 2


def test_scoped_worker_and_topic_binding_required(recovery, binding, prepared, publisher):
    enqueue(recovery, binding, prepared)
    for wrong in (
        actor(),
        replace(worker(binding), locations=()),
        replace(worker(binding), actor_id="other"),
    ):
        with pytest.raises(StoreError):
            recovery.publish_once(binding, wrong)
    with pytest.raises(PostgresBackendError):
        recovery.enroll_topics(
            binding,
            replace(prepared.topics[0], topic_id="5" * 32),
            prepared.topics[1],
            actor("approver"),
        )
    assert not publisher


def test_self_rehashed_false_observation_refused(recovery, binding, prepared, publisher, cluster):
    ids = enqueue(recovery, binding, prepared)
    value = {"intent_id": ids[0], "offset": 0}
    cluster.query(
        recovery,
        "INSERT INTO {schema}.transport_recovery_publication_receipts VALUES (%s,%s,%s)",
        (ids[0], canonical(value), digest(value)),
        role="application",
    )
    with pytest.raises(PostgresBackendError, match="observation differs"):
        recovery.verify_history(binding)


def test_fresh_concurrent_migration_and_registration(cluster, binding, tmp_path):
    config = cluster.backend(tmp_path / "fresh-outbox").config
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda _: cluster.migrate(RecoveryPublicationLedger(config)), range(2))
        )
    assert sum(r["applied"] for r in results) == 1
    reopened = RecoveryPublicationLedger(config)
    reopened.register(binding)
    assert reopened.verify_history(binding)["recovery_publication"]["intents"] == 0


def test_outbox_tables_cannot_be_deleted_by_application(
    recovery, binding, prepared, publisher, cluster
):
    enqueue(recovery, binding, prepared)
    recovery.publish_once(binding, worker(binding))
    for table in (
        "transport_recovery_topics",
        "transport_recovery_publications",
        "transport_recovery_publication_receipts",
    ):
        with pytest.raises(cluster.driver.errors.InsufficientPrivilege):
            cluster.query(recovery, "DELETE FROM {schema}." + table, role="application")


def test_review_hash_conflict_cannot_create_publication(recovery, binding, prepared):
    prepared.retained["request_sha256"] = "0" * 64
    with pytest.raises(StoreError, match="exact retained request"):
        enqueue(recovery, binding, prepared)
    assert recovery.verify_history(binding)["recovery_publication"]["intents"] == 0
