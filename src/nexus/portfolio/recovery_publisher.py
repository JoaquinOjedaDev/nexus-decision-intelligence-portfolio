"""Pinned recovery-evidence ACK and exact readback, never business authorization.

The caller must retain its immutable outbox intent before calling this adapter.
Returned receipts are observations, not persisted journal entries or skip permits.
Uncertain delivery requires retrying identical bytes; physical duplicates are possible.
"""

from __future__ import annotations

import hashlib
import importlib
import re
import time
from dataclasses import dataclass
from typing import Any, Literal

from nexus.portfolio.broker_runtime import BrokerAdmissionError, RuntimeBrokerConfig
from nexus.portfolio.store import digest
from nexus.portfolio.transport_consumer import inspect_topic
from nexus.portfolio.transport_ledger import TransportBinding


@dataclass(frozen=True)
class RecoveryTopic:
    source: TransportBinding
    broker: RuntimeBrokerConfig
    topic_id: str
    kind: Literal["dead_letter", "redrive_audit"]

    def __post_init__(self) -> None:
        if (
            self.source.purpose != "operations"
            or self.broker.bootstrap_servers != self.source.broker.bootstrap_servers
            or self.broker.topic == self.source.broker.topic
            or set(self.broker.locations) != set(self.source.broker.locations)
            or not isinstance(self.topic_id, str)
            or re.fullmatch(r"[0-9a-f]{32}", self.topic_id) is None
            or self.topic_id == "0" * 32
            or self.topic_id == self.source.topic_id
            or self.kind not in {"dead_letter", "redrive_audit"}
        ):
            raise ValueError("Pin a distinct same-cluster recovery evidence topic")

    def payload(self) -> dict[str, Any]:
        return {
            "contract": "nexus.recovery-topic.v1",
            "source_binding_sha256": digest(self.source.payload()),
            "endpoint": self.broker.bootstrap_servers,
            "topic": self.broker.topic,
            "topic_id": self.topic_id,
            "cluster_id": self.source.cluster_id,
            "partitions": [0],
            "kind": self.kind,
            "retention": {
                "cleanup.policy": "delete",
                "retention.ms": "-1",
                "retention.bytes": "-1",
            },
        }


class RecoveryEvidencePublisher:
    """Fail-stop bounded producer/readback; no topic creation, offsets or DB writes."""

    def __init__(self, topic: RecoveryTopic) -> None:
        self.topic = topic
        self.failed = False

    def _verify_topic(self) -> None:
        topic = self.topic
        if inspect_topic(topic.broker) != {
            "cluster_id": topic.source.cluster_id,
            "topic_id": topic.topic_id,
            "partitions": [0],
        }:
            raise BrokerAdmissionError("recovery_topic_identity_differs")
        admin_module = importlib.import_module("confluent_kafka.admin")
        admin = admin_module.AdminClient(
            {"bootstrap.servers": topic.broker.bootstrap_servers, "socket.timeout.ms": 5000}
        )
        resource = admin_module.ConfigResource(
            admin_module.ConfigResource.Type.TOPIC, topic.broker.topic
        )
        configs = admin.describe_configs([resource], request_timeout=5)[resource].result(6)
        for name, expected in topic.payload()["retention"].items():
            if name not in configs or configs[name].value != expected:
                raise BrokerAdmissionError("recovery_topic_retention_differs")

    def publish(self, intent_id: str, payload: bytes) -> dict[str, Any]:
        if self.failed:
            raise BrokerAdmissionError("recovery_publisher_restart_required")
        if (
            not isinstance(intent_id, str)
            or re.fullmatch(r"[0-9a-f]{64}", intent_id) is None
            or not isinstance(payload, bytes)
            or not 0 < len(payload) <= 512 * 1024
        ):
            raise ValueError("Use a retained content identity and bounded exact evidence bytes")
        topic = self.topic
        key = intent_id.encode("ascii")
        try:
            self._verify_topic()
            module = importlib.import_module("confluent_kafka")
            producer = module.Producer(
                {
                    "bootstrap.servers": topic.broker.bootstrap_servers,
                    "acks": "all",
                    "enable.idempotence": True,
                    "message.timeout.ms": 10000,
                    "allow.auto.create.topics": False,
                }
            )
            results: list[tuple[Any, Any]] = []
            producer.produce(
                topic.broker.topic,
                partition=0,
                key=key,
                value=payload,
                on_delivery=lambda error, message: results.append((error, message)),
            )
            if producer.flush(12) != 0 or len(results) != 1 or results[0][0] is not None:
                raise BrokerAdmissionError("recovery_ack_unconfirmed")
            acknowledged = results[0][1]
            if (
                acknowledged.topic() != topic.broker.topic
                or acknowledged.partition() != 0
                or type(acknowledged.offset()) is not int
                or acknowledged.offset() < 0
            ):
                raise BrokerAdmissionError("recovery_ack_identity_differs")
            offset = acknowledged.offset()
            consumer = module.Consumer(topic.broker.consumer_options())
            try:
                consumer.assign([module.TopicPartition(topic.broker.topic, 0, offset)])
                deadline = time.monotonic() + 10
                readback = None
                while time.monotonic() < deadline:
                    readback = consumer.poll(min(1, max(0, deadline - time.monotonic())))
                    if readback is not None:
                        break
                if readback is None:
                    raise BrokerAdmissionError("recovery_readback_unavailable")
                if (
                    readback.error() is not None
                    or readback.topic() != topic.broker.topic
                    or readback.partition() != 0
                    or readback.offset() != offset
                    or readback.key() != key
                    or readback.value() != payload
                    or readback.headers() not in (None, [])
                ):
                    raise BrokerAdmissionError("recovery_readback_differs")
            finally:
                consumer.close()
            self._verify_topic()
            return {
                "contract": "nexus.recovery-publication-observation.v1",
                "intent_id": intent_id,
                "topic_binding_sha256": digest(topic.payload()),
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "key_sha256": hashlib.sha256(key).hexdigest(),
                "partition": 0,
                "offset": offset,
                "claim": "broker_ack_and_exact_readback_not_recovery_authorization",
            }
        except BaseException:
            self.failed = True
            raise
