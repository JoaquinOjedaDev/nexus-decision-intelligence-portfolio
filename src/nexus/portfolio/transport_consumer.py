"""Explicit single-capture-worker-per-group adapter over the durable transport ledger.

No topic creation/deletion, arbitrary seek/reset, automatic migration or business
execution. Two independent groups use separate bound schemas. Topic UUID and
cluster checks prevent silently adopting a recreated same-name topic on recovery.
"""

from __future__ import annotations

import importlib
import time
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from nexus.portfolio.broker_runtime import BrokerAdmissionError, RuntimeBrokerConfig
from nexus.portfolio.capture_health import CaptureObservation, valid_instant
from nexus.portfolio.store import digest
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger, TransportRecord


def inspect_topic(config: RuntimeBrokerConfig) -> dict[str, Any]:
    """Read broker identity only; enrollment must still pin the expected cluster."""
    module = importlib.import_module("confluent_kafka")
    admin_module = importlib.import_module("confluent_kafka.admin")
    admin = admin_module.AdminClient(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "socket.timeout.ms": 5000,
        }
    )
    metadata = admin.list_topics(timeout=5)
    topic = admin.describe_topics(module.TopicCollection([config.topic]), request_timeout=5)[
        config.topic
    ].result(6)
    mask = (1 << 64) - 1
    topic_id = (
        f"{topic.topic_id.get_most_significant_bits() & mask:016x}"
        f"{topic.topic_id.get_least_significant_bits() & mask:016x}"
    )
    if topic_id == "0" * 32 or not metadata.cluster_id:
        raise BrokerAdmissionError("broker_identity_unavailable")
    return {
        "cluster_id": metadata.cluster_id,
        "topic_id": topic_id,
        "partitions": sorted(part.id for part in topic.partitions),
    }


class DurableBrokerConsumer:
    """Fail-stop capture; committed offsets never intentionally exceed database proof.

    Manual assignment is deliberate: one capture worker per group, all bound
    partitions. Automatic rebalance/multi-worker scaling is not claimed by this adapter.
    """

    def __init__(self, ledger: TransportLedger, binding: TransportBinding) -> None:
        self.ledger = ledger
        self.binding = binding
        self.instance_id = str(uuid4())
        self.module = importlib.import_module("confluent_kafka")
        self.consumer = self.module.Consumer(binding.broker.consumer_options())
        self.failed = True
        try:
            self._verify_broker()
            partitions = [
                self.module.TopicPartition(binding.broker.topic, p)
                for p in range(binding.partition_count)
            ]
            state = ledger.verify_history(binding)
            committed = self.consumer.committed(partitions, timeout=5)
            if len(committed) != len(partitions) or any(
                p.error is not None or p.topic != binding.broker.topic or p.partition != i
                for i, p in enumerate(committed)
            ):
                raise BrokerAdmissionError("broker_progress_unavailable")
            watermarks = [
                self.consumer.get_watermark_offsets(p, timeout=5, cached=False) for p in partitions
            ]
            offsets = []
            for i, part in enumerate(committed):
                # Missing broker progress is initialized only at the explicit zero origin,
                # with no durable captured records and no retention gap. Never adopt latest.
                if part.offset == self.module.OFFSET_INVALID:
                    if state["captured"][i] != 0 or watermarks[i][0] != 0:
                        raise BrokerAdmissionError("missing_broker_progress_requires_review")
                    offsets.append(0)
                else:
                    offsets.append(part.offset)
            reconciled = ledger.reconcile(binding, offsets, watermarks)
            self._verify_broker()
            self._commit(reconciled)
            self.consumer.assign(
                [
                    self.module.TopicPartition(binding.broker.topic, p, offset)
                    for p, offset in enumerate(reconciled)
                ]
            )
            self.failed = False
        except BaseException:
            self.consumer.close()
            raise

    def _verify_broker(self) -> None:
        actual = inspect_topic(self.binding.broker)
        if actual != {
            "cluster_id": self.binding.cluster_id,
            "topic_id": self.binding.topic_id,
            "partitions": list(range(self.binding.partition_count)),
        }:
            raise BrokerAdmissionError("broker_binding_changed")

    def _commit(self, offsets: list[int]) -> None:
        desired = [
            self.module.TopicPartition(self.binding.broker.topic, p, offset)
            for p, offset in enumerate(offsets)
        ]
        committed = self.consumer.commit(offsets=desired, asynchronous=False)
        if (
            not isinstance(committed, list)
            or len(committed) != len(desired)
            or any(
                actual.error is not None
                or (actual.topic, actual.partition, actual.offset)
                != (expected.topic, expected.partition, expected.offset)
                for actual, expected in zip(committed, desired, strict=True)
            )
        ):
            raise BrokerAdmissionError("broker_commit_unconfirmed")

    def poll_batch(self, maximum: int = 250) -> dict[str, Any] | None:
        if self.failed:
            raise BrokerAdmissionError("consumer_requires_reconciliation")
        if type(maximum) is not int or not 1 <= maximum <= 500:
            raise ValueError("Capture batch must contain one to 500 records")
        try:
            self._verify_broker()
            messages = self.consumer.consume(num_messages=maximum, timeout=0.5)
            if not messages:
                return None
            records = []
            for message in messages:
                if message.error() is not None or message.topic() != self.binding.broker.topic:
                    raise BrokerAdmissionError("broker_record_unavailable")
                kind, timestamp = message.timestamp()
                records.append(
                    TransportRecord(
                        message.partition(),
                        message.offset(),
                        message.key(),
                        message.value(),
                        tuple(message.headers() or ()),
                        kind,
                        timestamp,
                    )
                )
            receipt = self.ledger.capture(self.binding, records)
            # The context has committed raw bytes + contiguous progress before transport ACK.
            self._verify_broker()
            self._commit(receipt["next_offsets"])
            return receipt
        except BaseException:
            self.failed = True
            raise

    def sample_health(self, *, clock: Callable[[], float] = time.time) -> CaptureObservation:
        """Measure this reconciled consumer, with no consume, seek or offset commit.

        A sample is not a heartbeat for another process or a durable permit. Call
        from the single owning worker between polls; concurrent use is unsupported.
        Failure latches the adapter, so the next poll requires reconciliation.
        """
        if self.failed:
            raise BrokerAdmissionError("consumer_requires_reconciliation")
        try:
            started = clock()
            if not valid_instant(started):
                raise ValueError("Finite health clock required")
            self._verify_broker()
            captured = self.ledger.capture_progress(self.binding)
            partitions = [
                self.module.TopicPartition(self.binding.broker.topic, p)
                for p in range(self.binding.partition_count)
            ]
            committed = self.consumer.committed(partitions, timeout=5)
            if (
                not isinstance(committed, list)
                or len(committed) != len(partitions)
                or any(
                    p.error is not None
                    or type(p.partition) is not int
                    or p.topic != self.binding.broker.topic
                    or p.partition != i
                    for i, p in enumerate(committed)
                )
            ):
                raise BrokerAdmissionError("broker_progress_unavailable")
            watermarks = [
                self.consumer.get_watermark_offsets(p, timeout=5, cached=False) for p in partitions
            ]
            self._verify_broker()
            if any(type(p) is not tuple or len(p) != 2 for p in watermarks):
                raise BrokerAdmissionError("broker_progress_unavailable")
            return CaptureObservation(
                str(uuid4()),
                self.instance_id,
                digest(self.binding.payload()),
                started,
                clock(),
                tuple(p[0] for p in watermarks),
                tuple(p[1] for p in watermarks),
                captured,
                tuple(p.offset for p in committed),
            )
        except BaseException:
            self.failed = True
            raise

    def close(self) -> None:
        self.failed = True
        self.consumer.close()
