"""Real source poison capture -> retained review -> durable evidence-topic receipts.

The V10 case stops before redrive; V11 verifies governed resolution and normal admission.
"""

import json
import time
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from test_portfolio_broker_runtime_service import Broker
from test_portfolio_broker_runtime_service import broker as broker
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import actor, event
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime
from test_portfolio_publication_runtime_service import publisher as source_actor
from test_portfolio_recovery_ledger_service import review
from test_portfolio_recovery_publication_service import worker
from test_portfolio_recovery_publisher_service import create_evidence_topic

from nexus.contracts import EventEnvelope
from nexus.portfolio.broker_runtime import BrokerAdmissionError
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.recovery_publication import RecoveryPublicationLedger
from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.source_publisher import SourceOutboxPublisher
from nexus.portfolio.store import StoreError, canonical
from nexus.portfolio.transport_consumer import DurableBrokerConsumer, inspect_topic
from nexus.portfolio.transport_ledger import (
    TransportBinding,
    TransportLedger,
    TransportRecord,
    runtime_target,
)
from nexus.portfolio.transport_recovery import RuntimeDeadLetter, RuntimeRedriveRequest
from nexus.simulation import generate_r2_smoke_fixture

pytestmark = pytest.mark.portfolio_service


@pytest.mark.parametrize(
    "resolve_replacement", [False, True], ids=["evidence-only", "controlled-resolution"]
)
def test_actual_poison_and_post_ack_crash_recover_evidence_not_business(
    broker: Broker, cluster, runtime, clock, tmp_path, record_property, resolve_replacement
):
    actual = inspect_topic(broker.config)
    binding = TransportBinding(
        broker.config,
        broker.cluster_id,
        actual["topic_id"],
        2,
        "operations",
        runtime_target(runtime.control),
    )
    ledger_type = RecoveryResolutionLedger if resolve_replacement else RecoveryPublicationLedger
    ledger = ledger_type(
        cluster.backend(tmp_path / "recovery-outbox").config, clock=lambda: clock[0]
    )
    cluster.migrate(ledger)
    ledger.register(binding)
    if resolve_replacement:
        runtime = PublicationRuntimeStore(
            runtime.config,
            transport=ledger,
            binding=binding,
            runtime_root=runtime.runtime_root,
            clock=lambda: clock[0],
        )
        cluster.migrate(runtime)
        ledger = RecoveryResolutionLedger(
            ledger.config, publication=runtime, clock=lambda: clock[0]
        )
    broker.produce(b"malformed source\x00\xff")
    bridge = DurableBrokerConsumer(ledger, binding)
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and bridge.poll_batch() is None:
            pass
        assert ledger.verify_history(binding)["captured"] == [1, 0]
    finally:
        bridge.close()
    raw, original_hash = cluster.query(
        ledger, "SELECT payload,record_hash FROM {schema}.transport_inbox"
    )[0]
    physical = TransportRecord.from_payload(json.loads(raw))
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
    if resolve_replacement:
        replacement = canonical(json.loads(replacement)).encode()
    proposal = RuntimeRedriveRequest.create(
        dead,
        binding,
        physical,
        original_hash,
        bundle,
        replacement,
        operator=actor(),
        reason="Replace malformed envelope using retained source evidence",
        submitted_at=datetime.fromtimestamp(clock[0], UTC),
    )
    retained = ledger.retain_request(binding, 0, original_hash, bundle, proposal, actor())
    decision = review(ledger, binding, retained)
    dlq = replace(create_evidence_topic(broker), source=binding)
    audit = replace(create_evidence_topic(broker), source=binding, kind="redrive_audit")
    ledger.enroll_topics(binding, dlq, audit, actor("approver"))
    ids = ledger.enqueue_evidence(
        binding,
        retained["request_id"],
        retained["request_sha256"],
        decision["decision_id"],
        actor("approver"),
    )

    def crash():
        raise RuntimeError("after verified broker ACK/readback before PostgreSQL receipt")

    with pytest.raises(RuntimeError, match="before PostgreSQL"):
        ledger.publish_once(binding, worker(binding), failpoint=crash)
    assert ledger.verify_history(binding)["recovery_publication"]["receipts"] == 0
    restarted = (
        RecoveryResolutionLedger(ledger.config, publication=runtime, clock=lambda: clock[0])
        if resolve_replacement
        else RecoveryPublicationLedger(ledger.config, clock=lambda: clock[0])
    )
    first = restarted.publish_once(binding, worker(binding))
    second = restarted.publish_once(binding, worker(binding))
    assert first["offset"] == 1  # Uncertain first publication remained physically present.
    assert second["offset"] == 0
    assert {first["intent_id"], second["intent_id"]} == set(ids)
    assert restarted.publish_once(binding, worker(binding)) is None
    evidence = restarted.verify_history(binding)
    assert evidence["recovery_publication"]["receipts"] == 2
    assert evidence["captured"] == [1, 0] and evidence["delivered"] == [0, 0]
    with pytest.raises(BrokerAdmissionError):
        restarted.dispatch_once(binding, runtime.control)
    assert runtime.control.snapshot(actor())["event_count"] == 0
    record_property("source_binding", json.dumps(binding.payload(), sort_keys=True))
    record_property("dead_letter_topic", json.dumps(dlq.payload(), sort_keys=True))
    record_property("audit_topic", json.dumps(audit.payload(), sort_keys=True))
    record_property("retained_observations", json.dumps([first, second], sort_keys=True))
    record_property("verified_history", json.dumps(evidence, sort_keys=True))
    if resolve_replacement:
        runtime.enqueue_source(
            bundle, EventEnvelope.model_validate_json(replacement), source_actor(runtime)
        )
        assert SourceOutboxPublisher(runtime).publish_once() is not None
        operations = DurableBrokerConsumer(restarted, binding)
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and operations.poll_batch() is None:
                pass
            assert restarted.verify_history(binding)["captured"] == [2, 0]
        finally:
            operations.close()
        audit_config = replace(broker.config, group_id=broker.config.group_id + "-audit")
        audit_binding = replace(
            binding, broker=audit_config, purpose="audit", runtime_target_sha256=None
        )
        audit_ledger = TransportLedger(cluster.backend(tmp_path / "audit-ledger").config)
        cluster.migrate(audit_ledger)
        audit_ledger.register(audit_binding)
        reader = broker.module.Consumer(audit_config.consumer_options())
        try:
            partitions = [broker.module.TopicPartition(audit_config.topic, p, 0) for p in range(2)]
            assert all(p.offset < 0 for p in reader.committed(partitions, timeout=5))
            assert all(reader.get_watermark_offsets(p, timeout=5)[0] == 0 for p in partitions)
            committed = reader.commit(offsets=partitions, asynchronous=False)
            assert all(p.error is None and p.offset == 0 for p in committed)
        finally:
            reader.close()
        audit_consumer = DurableBrokerConsumer(audit_ledger, audit_binding)
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and audit_consumer.poll_batch() is None:
                pass
            assert audit_ledger.verify_history(audit_binding)["captured"] == [2, 0]
        finally:
            audit_consumer.close()
        resolution = restarted.resolve(
            binding,
            retained["request_id"],
            retained["request_sha256"],
            decision["decision_id"],
            actor("approver"),
        )
        assert not resolution["duplicate"]
        assert restarted.verify_history(binding)["delivered"] == [1, 0]
        fingerprint = runtime.recommendation("rec-1", actor())["fingerprint"]
        with pytest.raises(StoreError) as denied:
            runtime.decide("rec-1", actor("approver"), "approve", fingerprint)
        assert denied.value.code == "transport_pending"
        admission = restarted.dispatch_once(binding, runtime.control)
        assert not admission["admission"]["duplicate"]
        assert runtime.control.snapshot(actor())["event_count"] == 1
        assert restarted.verify_history(binding)["delivered"] == [2, 0]
        assert not runtime.snapshot()["effects"]
        # Restart both group adapters: retained proof reconciles without resetting offsets.
        reopened = RecoveryResolutionLedger(
            ledger.config, publication=runtime, clock=lambda: clock[0]
        )
        for durable, pinned in ((reopened, binding), (audit_ledger, audit_binding)):
            restarted_consumer = DurableBrokerConsumer(durable, pinned)
            try:
                assert restarted_consumer.poll_batch() is None
            finally:
                restarted_consumer.close()
        assert reopened.resolve(
            binding,
            retained["request_id"],
            retained["request_sha256"],
            decision["decision_id"],
            actor("approver"),
        )["duplicate"]
        record_property("controlled_resolution", json.dumps(resolution, sort_keys=True))
        record_property(
            "final_operations", json.dumps(reopened.verify_history(binding), sort_keys=True)
        )
        record_property(
            "final_audit", json.dumps(audit_ledger.verify_history(audit_binding), sort_keys=True)
        )
