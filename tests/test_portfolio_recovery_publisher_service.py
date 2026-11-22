"""Opt-in real ACK/readback on unique retained local recovery topics, no redrive."""

import importlib
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from test_portfolio_broker_runtime_service import Broker
from test_portfolio_broker_runtime_service import broker as broker

from nexus.portfolio.broker_runtime import BrokerAdmissionError
from nexus.portfolio.recovery_publisher import RecoveryEvidencePublisher, RecoveryTopic
from nexus.portfolio.transport_consumer import inspect_topic
from nexus.portfolio.transport_ledger import TransportBinding

pytestmark = pytest.mark.portfolio_service


def create_evidence_topic(broker: Broker, *, retention: str = "-1") -> RecoveryTopic:
    identity = inspect_topic(broker.config)
    assert identity["cluster_id"] == broker.cluster_id
    source = TransportBinding(
        broker.config, broker.cluster_id, identity["topic_id"], 2, "operations", "a" * 64
    )
    config = replace(
        broker.config,
        topic=f"nexus-runtime-recovery-{uuid4().hex}",
        group_id=f"nexus-runtime-readback-{uuid4().hex}",
    )
    admin_module = importlib.import_module("confluent_kafka.admin")
    admin = admin_module.AdminClient({"bootstrap.servers": config.bootstrap_servers})
    metadata = admin.list_topics(timeout=5)
    assert metadata.cluster_id == broker.cluster_id and config.topic not in metadata.topics
    future = admin.create_topics(
        [
            admin_module.NewTopic(
                config.topic,
                num_partitions=1,
                replication_factor=1,
                config={
                    "cleanup.policy": "delete",
                    "retention.ms": retention,
                    "retention.bytes": "-1",
                },
            )
        ],
        request_timeout=10,
    )[config.topic]
    future.result(12)
    identity = inspect_topic(config)
    assert identity["cluster_id"] == broker.cluster_id and identity["partitions"] == [0]
    return RecoveryTopic(source, config, identity["topic_id"], "dead_letter")


@pytest.mark.parametrize("kind", ["dead_letter", "redrive_audit"])
def test_actual_ack_and_exact_readback_repeated_identity_is_not_exactly_once(
    broker: Broker, kind: str, record_property
):
    topic = replace(create_evidence_topic(broker), kind=kind)
    original = b"exact recovery evidence\x00\xff"
    first = RecoveryEvidencePublisher(topic).publish("b" * 64, original)
    second = RecoveryEvidencePublisher(topic).publish("b" * 64, original)
    assert first["offset"] == 0 and second["offset"] == 1
    assert first["payload_sha256"] == second["payload_sha256"]
    assert first["topic_binding_sha256"] == second["topic_binding_sha256"]
    assert first["claim"] == "broker_ack_and_exact_readback_not_recovery_authorization"
    record_property("topic_binding", json.dumps(topic.payload(), sort_keys=True))
    record_property("observed_publications", json.dumps([first, second], sort_keys=True))
    reader = broker.module.Consumer(topic.broker.consumer_options())
    try:
        partition = broker.module.TopicPartition(topic.broker.topic, 0)
        assert reader.committed([partition], timeout=5)[0].offset < 0
        assert reader.get_watermark_offsets(partition, timeout=5, cached=False) == (0, 2)
    finally:
        reader.close()


def test_actual_expiring_retention_refused_before_publication(broker: Broker, record_property):
    topic = create_evidence_topic(broker, retention="604800000")
    with pytest.raises(BrokerAdmissionError, match="retention_differs"):
        RecoveryEvidencePublisher(topic).publish("b" * 64, b"not published")
    record_property("topic_binding", json.dumps(topic.payload(), sort_keys=True))
    record_property("observed_retention_ms", "604800000")
    record_property("publication_refused", "true")
    reader = broker.module.Consumer(topic.broker.consumer_options())
    try:
        assert reader.get_watermark_offsets(
            broker.module.TopicPartition(topic.broker.topic, 0), timeout=5, cached=False
        ) == (0, 0)
    finally:
        reader.close()
