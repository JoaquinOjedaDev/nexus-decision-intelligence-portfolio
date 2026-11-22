"""Real Redpanda adapter for bounded R2D-B2a poison routing and redrive."""

# mypy: disable-error-code="attr-defined"

from __future__ import annotations

import hashlib
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, cast

from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from nexus.ingestion.r2d_b2_contracts import (
    R2DB2AAckLedger,
    R2DB2ABrokerAcknowledgement,
    R2DB2AConfig,
    R2DB2ATopic,
)
from nexus.ingestion.r2d_contracts import R2DPhysicalRecord, canonical_json_sha256
from nexus.ingestion.r2d_service import R2DServiceError, physical_record_from_message


@dataclass(frozen=True, slots=True)
class R2DB2ARuntime:
    bootstrap_servers: str


@dataclass(frozen=True, slots=True)
class R2DB2AProduceItem:
    key: bytes
    value: bytes
    partition: int
    timestamp_ms: int
    headers: tuple[tuple[str, bytes | None], ...]


class R2DB2AService:
    """Provision, produce, consume, and explicitly commit the three B2a topics."""

    def __init__(self, *, config: R2DB2AConfig, runtime: R2DB2ARuntime) -> None:
        self.config = config
        self.runtime = runtime

    def provision_topics(self, *, timeout_seconds: float = 30.0) -> None:
        broker = self.config.broker
        admin = AdminClient(
            {
                "bootstrap.servers": self.runtime.bootstrap_servers,
                "broker.address.family": "v4",
                "client.id": "nexus-r2d-b2a-admin",
            }
        )
        futures = admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=broker.partition_count,
                    replication_factor=broker.replication_factor,
                    config={"retention.ms": str(broker.retention_ms)},
                )
                for topic in broker.topics
            ],
            operation_timeout=timeout_seconds,
        )
        for topic, future in futures.items():
            try:
                future.result(timeout=timeout_seconds)
            except KafkaException as exc:
                error = exc.args[0] if exc.args else None
                if (
                    not isinstance(error, KafkaError)
                    or error.code() != KafkaError.TOPIC_ALREADY_EXISTS
                ):
                    raise R2DServiceError(
                        f"B2a topic provisioning failed for {topic}: {exc}"
                    ) from exc
        metadata = admin.list_topics(timeout=timeout_seconds)
        for topic in broker.topics:
            observed = metadata.topics.get(topic)
            if observed is None or observed.error is not None:
                raise R2DServiceError(f"B2a topic metadata is unavailable: {topic}")
            if len(observed.partitions) != broker.partition_count:
                raise R2DServiceError(f"B2a topic partition count differs from contract: {topic}")
            for partition_id, partition in observed.partitions.items():
                if partition_id not in range(broker.partition_count):
                    raise R2DServiceError(f"B2a topic has an unexpected partition: {topic}")
                if len(partition.replicas) != broker.replication_factor:
                    raise R2DServiceError(f"B2a topic replication factor differs: {topic}")
        self.require_exact_topic_configs(timeout_seconds=timeout_seconds)

    def require_exact_topic_configs(self, *, timeout_seconds: float = 30.0) -> None:
        """Fail unless every governed topic has the exact non-expiring retention policy."""

        broker = self.config.broker
        admin = AdminClient(
            {
                "bootstrap.servers": self.runtime.bootstrap_servers,
                "broker.address.family": "v4",
                "client.id": "nexus-r2d-b2a-config-audit",
            }
        )
        futures = admin.describe_configs(
            [ConfigResource(ConfigResource.Type.TOPIC, topic) for topic in broker.topics]
        )
        for resource, future in futures.items():
            try:
                observed = future.result(timeout=timeout_seconds)
            except KafkaException as exc:
                raise R2DServiceError(
                    f"B2a topic config is unavailable for {resource.name}: {exc}"
                ) from exc
            retention = observed.get("retention.ms")
            if retention is None or retention.value != str(broker.retention_ms):
                value = None if retention is None else retention.value
                raise R2DServiceError(f"B2a topic retention differs for {resource.name}: {value!r}")

    def _consumer(self, group_id: str) -> Consumer:
        broker = self.config.broker
        return Consumer(
            {
                "bootstrap.servers": self.runtime.bootstrap_servers,
                "broker.address.family": "v4",
                "group.id": group_id,
                "client.id": f"{group_id}-consumer",
                "enable.auto.commit": broker.consumer_auto_offset_commit,
                "enable.auto.offset.store": broker.consumer_auto_offset_store,
                "auto.offset.reset": "error",
                "isolation.level": broker.isolation_level,
            }
        )

    def topic_watermarks(self, topic: R2DB2ATopic) -> tuple[tuple[int, ...], tuple[int, ...]]:
        consumer = self._consumer(f"nexus-r2d-b2a-watermark-{topic}")
        try:
            pairs = tuple(
                consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10.0)
                for partition in range(self.config.broker.partition_count)
            )
            return tuple(item[0] for item in pairs), tuple(item[1] for item in pairs)
        finally:
            consumer.close()

    def require_fresh_empty_topics(self) -> dict[str, list[int]]:
        observed: dict[str, list[int]] = {}
        for topic in self.config.broker.topics:
            lows, highs = self.topic_watermarks(topic)
            if lows != (0, 0) or highs != (0, 0):
                raise R2DServiceError(f"B2a requires a fresh empty topic: {topic}")
            observed[topic] = list(highs)
        return observed

    def produce(
        self,
        *,
        phase: Literal["source_fixture", "dead_letter", "redrive_audit", "replacement"],
        topic: R2DB2ATopic,
        items: Sequence[R2DB2AProduceItem],
    ) -> R2DB2AAckLedger:
        if not items or len(items) > self.config.gate.source_fixture_records:
            raise R2DServiceError("B2a producer item count is outside the bounded gate")
        producer = Producer(
            {
                "bootstrap.servers": self.runtime.bootstrap_servers,
                "broker.address.family": "v4",
                "client.id": f"nexus-r2d-b2a-{phase}",
                "acks": self.config.broker.producer_acks,
                "enable.idempotence": self.config.broker.producer_enable_idempotence,
            }
        )
        acknowledgements: list[R2DB2ABrokerAcknowledgement] = []
        errors: list[str] = []
        for item in items:
            expected_sha256 = hashlib.sha256(item.value).hexdigest()

            def delivered(
                error: KafkaError | None,
                message: Message,
                *,
                expected: R2DB2AProduceItem = item,
                value_sha256: str = expected_sha256,
            ) -> None:
                if error is not None:
                    errors.append(str(error))
                    return
                timestamp_type, timestamp_ms = message.timestamp()
                if timestamp_type != TIMESTAMP_CREATE_TIME or timestamp_ms != expected.timestamp_ms:
                    errors.append("B2a broker acknowledgement timestamp differs from input")
                    return
                if message.key() != expected.key or message.partition() != expected.partition:
                    errors.append("B2a broker acknowledgement routing differs from input")
                    return
                observed_topic = message.topic()
                offset = message.offset()
                if observed_topic is None or offset is None:
                    errors.append("B2a broker acknowledgement lacks transport identity")
                    return
                acknowledgements.append(
                    R2DB2ABrokerAcknowledgement(
                        acknowledgement_kind="broker_acks_all",
                        topic=cast(R2DB2ATopic, observed_topic),
                        partition=expected.partition,
                        offset=offset,
                        key_hex=expected.key.hex(),
                        value_sha256=value_sha256,
                        broker_timestamp_ms=expected.timestamp_ms,
                    )
                )

            producer.produce(
                topic=topic,
                key=item.key,
                value=item.value,
                partition=item.partition,
                headers=list(item.headers),
                timestamp=item.timestamp_ms,
                on_delivery=delivered,
            )
            producer.poll(0)
        remaining = producer.flush(30.0)
        if remaining or errors or len(acknowledgements) != len(items):
            raise R2DServiceError(
                "B2a producer acknowledgement gate failed: "
                f"remaining={remaining}, errors={errors!r}, "
                f"acknowledged={len(acknowledgements)}/{len(items)}"
            )
        ordered = tuple(sorted(acknowledgements, key=lambda item: (item.partition, item.offset)))
        payload: dict[str, object] = {
            "contract_version": "r2d.b2a.broker-ack.v1",
            "phase": phase,
            "topic": topic,
            "producer_acks": "all",
            "acknowledgements": [item.model_dump(mode="json") for item in ordered],
        }
        return R2DB2AAckLedger.model_validate(
            {**payload, "content_sha256": canonical_json_sha256(payload)}
        )

    def consume_range(
        self,
        *,
        topic: R2DB2ATopic,
        group_id: str,
        starts: tuple[int, ...],
        ends: tuple[int, ...],
    ) -> tuple[R2DPhysicalRecord, ...]:
        if (
            len(starts) != 2
            or len(ends) != 2
            or any(start < 0 or start > end for start, end in zip(starts, ends, strict=True))
        ):
            raise R2DServiceError("B2a consumer range is invalid")
        consumer = self._consumer(group_id)
        next_offsets = list(starts)
        records: list[R2DPhysicalRecord] = []
        try:
            assignments = [
                TopicPartition(topic, partition, start)
                for partition, (start, end) in enumerate(zip(starts, ends, strict=True))
                if start < end
            ]
            if assignments:
                consumer.assign(assignments)
            deadline = time.monotonic() + self.config.gate.poll_timeout_seconds
            while tuple(next_offsets) != ends:
                if time.monotonic() >= deadline:
                    raise R2DServiceError("B2a consumer did not complete the frozen range")
                message = consumer.poll(timeout=1.0)
                if message is None:
                    continue
                if message.error() is not None:
                    raise R2DServiceError(f"B2a consumer poll failed: {message.error()}")
                record = physical_record_from_message(message)
                if record.topic != topic or record.partition not in range(2):
                    raise R2DServiceError("B2a consumer observed an unexpected topic or partition")
                expected_offset = next_offsets[record.partition]
                if record.offset != expected_offset or record.offset >= ends[record.partition]:
                    raise R2DServiceError(
                        "B2a consumer observed noncontiguous or out-of-range data"
                    )
                records.append(record)
                next_offsets[record.partition] = record.offset + 1
            consumer.commit(
                offsets=[
                    TopicPartition(topic, partition, end) for partition, end in enumerate(ends)
                ],
                asynchronous=False,
            )
        finally:
            consumer.close()
        return tuple(sorted(records, key=lambda item: (item.partition, item.offset)))

    def committed_offsets(self, *, topic: R2DB2ATopic, group_id: str) -> tuple[int, int]:
        consumer = self._consumer(group_id)
        try:
            committed = consumer.committed(
                [TopicPartition(topic, partition) for partition in range(2)], timeout=10.0
            )
            return cast(tuple[int, int], tuple(max(0, item.offset) for item in committed))
        finally:
            consumer.close()
