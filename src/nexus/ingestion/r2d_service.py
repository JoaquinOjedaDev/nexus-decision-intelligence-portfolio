"""Real Redpanda/PostgreSQL transport primitives for the bounded R2D-A2a gate."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, NoReturn, cast

import psycopg
from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]
from psycopg import Connection
from psycopg.types.json import Jsonb

from nexus.ingestion.r2d_capture import (
    capture_transcript,
    partition_for_key,
    routing_key_for_value,
)
from nexus.ingestion.r2d_contracts import (
    R2DCaptureTranscript,
    R2DConfig,
    R2DHeader,
    R2DParsedLineage,
    R2DPhysicalRecord,
    canonical_json_sha256,
    r2d_config_sha256,
)
from nexus.ingestion.r2d_service_contracts import (
    R2DA2ServiceConfig,
    R2DBrokerAckLedger,
    R2DBrokerAcknowledgement,
    migration_sha256,
)

R2DServiceFailpoint = Literal["before_db_commit", "after_db_commit_before_broker_commit"]


class R2DServiceError(RuntimeError):
    """Base error for service evidence that cannot be trusted."""


class R2DTopicContractError(R2DServiceError):
    """The real topic does not match the frozen topology."""


class R2DInboxConflictError(R2DServiceError):
    """An existing transport identity has different physical evidence."""


class R2DOffsetGapError(R2DServiceError):
    """Durable progress would move past an unproven offset."""


class R2DBrokerAheadError(R2DServiceError):
    """The broker claims progress that PostgreSQL cannot prove."""


class R2DInjectedCrash(R2DServiceError):
    """A deterministic test-only crash boundary was reached."""


@dataclass(frozen=True)
class R2DA2Runtime:
    """Non-published connection endpoints; the PostgreSQL DSN may contain a password."""

    bootstrap_servers: str
    postgres_dsn: str


@dataclass(frozen=True)
class R2DA2CaptureResult:
    """One bounded real-service capture result before downstream R2A reconstruction."""

    starts: tuple[int, ...]
    ends: tuple[int, ...]
    next_offsets: tuple[int, ...]
    broker_ack_ledger: R2DBrokerAckLedger
    transcript: R2DCaptureTranscript


def _required_headers(
    config: R2DConfig, routing_key: bytes
) -> list[tuple[str, str | bytes | None]]:
    contract = config.header_contract
    return [
        (contract.contract_header_name, contract.contract_header_value.encode("utf-8")),
        (contract.routing_header_name, routing_key),
        (contract.schema_header_name, contract.schema_header_value.encode("ascii")),
    ]


def _timestamp_type(message: Message) -> tuple[int | None, str]:
    timestamp_type, timestamp_ms = message.timestamp()
    if timestamp_type == TIMESTAMP_CREATE_TIME:
        return timestamp_ms, "create_time"
    if timestamp_type == TIMESTAMP_LOG_APPEND_TIME:
        return timestamp_ms, "log_append_time"
    return None, "not_available"


def physical_record_from_message(message: Message) -> R2DPhysicalRecord:
    """Adapt exact broker bytes and metadata to the shared frozen physical contract."""

    topic = message.topic()
    partition = message.partition()
    offset = message.offset()
    if topic is None or partition is None or offset is None:
        raise R2DServiceError("broker message is missing physical transport identity")
    timestamp_ms, timestamp_type = _timestamp_type(message)
    headers_list: list[tuple[str, bytes | None]] = []
    for header in message.headers() or ():
        if len(header) != 2 or not isinstance(header[0], str):
            raise R2DServiceError("broker message contains a malformed header")
        value = header[1]
        if value is not None and not isinstance(value, bytes):
            raise R2DServiceError("broker message header value is not exact bytes")
        headers_list.append((header[0], value))
    headers = tuple(headers_list)
    return R2DPhysicalRecord.from_bytes(
        topic=topic,
        partition=partition,
        offset=offset,
        key=message.key(),
        headers=headers,
        broker_timestamp_ms=timestamp_ms,
        broker_timestamp_type=cast(
            Literal["create_time", "log_append_time", "not_available"], timestamp_type
        ),
        value=message.value(),
    )


class R2DA2Service:
    """Provision, produce, and capture one isolated two-partition service fixture."""

    def __init__(
        self,
        *,
        project_root: Path,
        service_config: R2DA2ServiceConfig,
        data_config: R2DConfig,
        runtime: R2DA2Runtime,
    ) -> None:
        self.project_root = project_root.resolve()
        self.service_config = service_config
        self.data_config = data_config
        self.runtime = runtime
        if service_config.broker.topic != data_config.topic_contract.topic:
            raise R2DServiceError("A2a topic differs from the frozen R2D data contract")
        if (
            service_config.broker.partition_count != data_config.topic_contract.partition_count
            or service_config.broker.replication_factor
            != data_config.topic_contract.replication_factor
        ):
            raise R2DServiceError("A2a broker topology differs from the frozen R2D data contract")

    @property
    def topic(self) -> str:
        """Return the single governed topic."""

        return self.service_config.broker.topic

    def connect_postgres(self) -> Connection[tuple[object, ...]]:
        """Open an autocommit connection; explicit transaction blocks guard progress changes."""

        connection = psycopg.connect(self.runtime.postgres_dsn, autocommit=True)
        connection.execute(
            "SELECT set_config('lock_timeout', %s, false)",
            (f"{self.service_config.postgres.lock_timeout_ms}ms",),
        )
        connection.execute(
            "SELECT set_config('statement_timeout', %s, false)",
            (f"{self.service_config.postgres.statement_timeout_ms}ms",),
        )
        return connection

    def apply_migration(self, connection: Connection[tuple[object, ...]]) -> str:
        """Apply the immutable migration and reject version/hash disagreement."""

        migration_path = (self.project_root / self.service_config.postgres.migration_path).resolve()
        if not migration_path.is_relative_to(self.project_root):
            raise R2DServiceError("migration path escapes the project root")
        migration_sql = migration_path.read_text(encoding="utf-8")
        expected_sha256 = migration_sha256(self.project_root, self.service_config)
        with connection.transaction():
            connection.execute(migration_sql)
            connection.execute(
                """
                INSERT INTO r2d_capture.schema_migrations (version, migration_sha256)
                VALUES (%s, %s)
                ON CONFLICT (version) DO NOTHING
                """,
                (
                    self.service_config.postgres.migration_version,
                    expected_sha256,
                ),
            )
            row = connection.execute(
                """
                SELECT migration_sha256
                FROM r2d_capture.schema_migrations
                WHERE version = %s
                """,
                (self.service_config.postgres.migration_version,),
            ).fetchone()
            if row is None or row[0] != expected_sha256:
                raise R2DServiceError("PostgreSQL migration version/hash conflict")
        return expected_sha256

    def provision_topic(self, *, timeout_seconds: float = 30.0) -> None:
        """Create or strictly verify the exact two-partition RF1 topic."""

        contract = self.service_config.broker
        admin = AdminClient(
            {
                "bootstrap.servers": self.runtime.bootstrap_servers,
                "client.id": "nexus-r2d-a2a-admin",
            }
        )
        future = admin.create_topics(
            [
                NewTopic(
                    self.topic,
                    num_partitions=contract.partition_count,
                    replication_factor=contract.replication_factor,
                )
            ],
            operation_timeout=timeout_seconds,
        )[self.topic]
        try:
            future.result(timeout=timeout_seconds)
        except KafkaException as exc:
            kafka_error = exc.args[0] if exc.args else None
            if (
                not isinstance(kafka_error, KafkaError)
                or kafka_error.code() != KafkaError.TOPIC_ALREADY_EXISTS
            ):
                raise R2DTopicContractError(f"topic provisioning failed: {exc}") from exc

        metadata = admin.list_topics(topic=self.topic, timeout=timeout_seconds)
        topic_metadata = metadata.topics.get(self.topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise R2DTopicContractError("topic metadata is unavailable")
        if len(topic_metadata.partitions) != contract.partition_count:
            raise R2DTopicContractError("topic partition count differs from contract")
        for partition_id, partition in topic_metadata.partitions.items():
            if partition_id not in range(contract.partition_count):
                raise R2DTopicContractError("topic contains an unexpected partition")
            if len(partition.replicas) != contract.replication_factor:
                raise R2DTopicContractError("topic replication factor differs from contract")

    def _consumer(self) -> Consumer:
        broker = self.service_config.broker
        return Consumer(
            {
                "bootstrap.servers": self.runtime.bootstrap_servers,
                "group.id": broker.consumer_group_id,
                "client.id": "nexus-r2d-a2a-consumer",
                "enable.auto.commit": broker.consumer_auto_offset_commit,
                "enable.auto.offset.store": broker.consumer_auto_offset_store,
                "auto.offset.reset": "error",
                "isolation.level": broker.isolation_level,
            }
        )

    def broker_committed_offsets(self) -> tuple[int, ...]:
        """Read the consumer group's committed offsets, normalizing uninitialized to zero."""

        consumer = self._consumer()
        try:
            committed = consumer.committed(
                [
                    TopicPartition(self.topic, partition)
                    for partition in range(self.service_config.broker.partition_count)
                ],
                timeout=10.0,
            )
            return tuple(max(0, item.offset) for item in committed)
        finally:
            consumer.close()

    def commit_broker_offsets(self, offsets: tuple[int, ...]) -> None:
        """Synchronously commit one exact next-offset vector for recovery verification."""

        if len(offsets) != self.service_config.broker.partition_count or any(
            offset < 0 for offset in offsets
        ):
            raise R2DServiceError("broker commit vector differs from the frozen topology")
        consumer = self._consumer()
        try:
            consumer.commit(
                offsets=[
                    TopicPartition(self.topic, partition, offset)
                    for partition, offset in enumerate(offsets)
                ],
                asynchronous=False,
            )
        finally:
            consumer.close()

    def read_record_at(self, *, partition: int, offset: int) -> R2DPhysicalRecord:
        """Read one exact broker identity without committing or advancing group state."""

        if partition not in range(self.service_config.broker.partition_count) or offset < 0:
            raise R2DServiceError("requested broker identity is outside the frozen topology")
        consumer = self._consumer()
        try:
            consumer.assign([TopicPartition(self.topic, partition, offset)])
            deadline = time.monotonic() + self.service_config.gate.poll_timeout_seconds
            while time.monotonic() <= deadline:
                message = consumer.poll(timeout=1.0)
                if message is None:
                    continue
                if message.error() is not None:
                    raise R2DServiceError(f"consumer poll failed: {message.error()}")
                if message.partition() != partition or message.offset() != offset:
                    raise R2DServiceError("consumer returned a different broker identity")
                return physical_record_from_message(message)
            raise R2DServiceError("timed out reading the requested broker identity")
        finally:
            consumer.close()

    def _watermarks(self, consumer: Consumer) -> tuple[tuple[int, ...], tuple[int, ...]]:
        lows: list[int] = []
        highs: list[int] = []
        for partition in range(self.service_config.broker.partition_count):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(self.topic, partition),
                timeout=10.0,
                cached=False,
            )
            lows.append(low)
            highs.append(high)
        return tuple(lows), tuple(highs)

    def initialize_fresh_empty(
        self,
        connection: Connection[tuple[object, ...]],
    ) -> tuple[int, ...]:
        """Initialize progress only after both real partitions prove empty."""

        consumer = self._consumer()
        try:
            lows, highs = self._watermarks(consumer)
        finally:
            consumer.close()
        if any(low != 0 or high != 0 for low, high in zip(lows, highs, strict=True)):
            raise R2DServiceError("fresh initialization requires an empty zero-offset topic")
        with connection.transaction():
            existing_inbox = connection.execute("SELECT COUNT(*) FROM r2d_capture.inbox").fetchone()
            existing_progress = connection.execute(
                "SELECT COUNT(*) FROM r2d_capture.partition_progress"
            ).fetchone()
            if existing_inbox is None or existing_progress is None:
                raise R2DServiceError("PostgreSQL initialization counts are unavailable")
            if existing_inbox[0] != 0 or existing_progress[0] != 0:
                raise R2DServiceError("fresh initialization requires an empty PostgreSQL ledger")
            for partition in range(self.service_config.broker.partition_count):
                connection.execute(
                    """
                    INSERT INTO r2d_capture.partition_progress
                        (topic, partition_id, next_offset, initialization_kind)
                    VALUES (%s, %s, 0, 'receipted_fresh_empty')
                    """,
                    (self.topic, partition),
                )
        return tuple(0 for _ in range(self.service_config.broker.partition_count))

    def produce(self, values: Sequence[bytes]) -> R2DBrokerAckLedger:
        """Publish exact fixture bytes and require one successful ``acks=all`` callback each."""

        if not values or len(values) > self.service_config.gate.maximum_records:
            raise R2DServiceError("producer value count is outside the bounded gate")
        producer = Producer(
            {
                "bootstrap.servers": self.runtime.bootstrap_servers,
                "client.id": "nexus-r2d-a2a-producer",
                "acks": self.service_config.broker.producer_acks,
                "enable.idempotence": self.service_config.broker.producer_enable_idempotence,
            }
        )
        acknowledgements: list[R2DBrokerAcknowledgement] = []
        delivery_errors: list[str] = []

        for source_position, value in enumerate(values):
            routing_key = routing_key_for_value(value, self.data_config)
            if not routing_key:
                raise R2DServiceError("fixture value has no governed routing key")
            exact_routing_key = routing_key
            partition = partition_for_key(
                exact_routing_key,
                partition_count=self.service_config.broker.partition_count,
            )
            raw_value_sha256 = hashlib.sha256(value).hexdigest()
            timestamp_ms = 1_780_000_000_000 + source_position

            def delivered(
                error: KafkaError | None,
                message: Message,
                *,
                expected_key: bytes = exact_routing_key,
                expected_raw_sha256: str = raw_value_sha256,
                expected_timestamp_ms: int = timestamp_ms,
            ) -> None:
                if error is not None:
                    delivery_errors.append(str(error))
                    return
                if message.key() != expected_key:
                    delivery_errors.append("broker acknowledgement key differs from producer input")
                    return
                actual_timestamp_ms, timestamp_type = _timestamp_type(message)
                if timestamp_type != "create_time" or actual_timestamp_ms != expected_timestamp_ms:
                    delivery_errors.append("broker acknowledgement timestamp differs from input")
                    return
                topic = message.topic()
                partition = message.partition()
                offset = message.offset()
                if topic is None or partition is None or offset is None:
                    delivery_errors.append("broker acknowledgement has no transport identity")
                    return
                acknowledgements.append(
                    R2DBrokerAcknowledgement(
                        acknowledgement_kind="broker_acks_all",
                        real_broker_ack_claim=True,
                        topic=cast(Literal["inventory.adjusted.v1"], topic),
                        partition=partition,
                        offset=offset,
                        key_hex=expected_key.hex(),
                        raw_value_sha256=expected_raw_sha256,
                        broker_timestamp_ms=expected_timestamp_ms,
                    )
                )

            producer.produce(
                topic=self.topic,
                value=value,
                key=exact_routing_key,
                partition=partition,
                headers=_required_headers(self.data_config, exact_routing_key),
                timestamp=timestamp_ms,
                on_delivery=delivered,
            )
            producer.poll(0)
        remaining = producer.flush(30.0)
        if remaining or delivery_errors or len(acknowledgements) != len(values):
            raise R2DServiceError(
                "producer acknowledgement gate failed: "
                f"remaining={remaining}, errors={delivery_errors!r}, "
                f"acknowledged={len(acknowledgements)}/{len(values)}"
            )
        ordered = tuple(sorted(acknowledgements, key=lambda item: (item.partition, item.offset)))
        payload: dict[str, object] = {
            "contract_version": "r2d.a2a.broker-ack.v1",
            "release": "R2D-A2a",
            "producer_acks": "all",
            "real_broker_ack_claim": True,
            "acknowledgements": [item.model_dump(mode="json") for item in ordered],
            "acknowledged_record_count": len(ordered),
        }
        return R2DBrokerAckLedger.model_validate(
            {**payload, "content_sha256": canonical_json_sha256(payload)}
        )

    def _progress(self, connection: Connection[tuple[object, ...]]) -> tuple[int, ...]:
        rows = connection.execute(
            """
            SELECT partition_id, next_offset
            FROM r2d_capture.partition_progress
            WHERE topic = %s
            ORDER BY partition_id
            """,
            (self.topic,),
        ).fetchall()
        expected_partitions = tuple(range(self.service_config.broker.partition_count))
        partitions = tuple(cast(int, row[0]) for row in rows)
        if partitions != expected_partitions:
            raise R2DServiceError("PostgreSQL progress does not contain the exact partition set")
        return tuple(cast(int, row[1]) for row in rows)

    def _verify_inbox_interval(
        self,
        connection: Connection[tuple[object, ...]],
        *,
        partition: int,
        start: int,
        end: int,
    ) -> None:
        offsets = tuple(
            cast(int, row[0])
            for row in connection.execute(
                """
                SELECT broker_offset
                FROM r2d_capture.inbox
                WHERE topic = %s AND partition_id = %s
                  AND broker_offset >= %s AND broker_offset < %s
                ORDER BY broker_offset
                """,
                (self.topic, partition, start, end),
            ).fetchall()
        )
        if offsets != tuple(range(start, end)):
            raise R2DOffsetGapError("PostgreSQL inbox cannot prove contiguous progress")

    def reconcile_broker_offsets(
        self,
        connection: Connection[tuple[object, ...]],
        consumer: Consumer,
    ) -> tuple[int, ...]:
        """Reject broker-ahead state and catch broker commits up to verified DB progress."""

        database_next = self._progress(connection)
        requests = [
            TopicPartition(self.topic, partition)
            for partition in range(self.service_config.broker.partition_count)
        ]
        committed = consumer.committed(requests, timeout=10.0)
        catchup: list[TopicPartition] = []
        for partition, (database_offset, broker_partition) in enumerate(
            zip(database_next, committed, strict=True)
        ):
            broker_offset = broker_partition.offset
            if broker_offset < 0:
                broker_offset = 0
            if broker_offset > database_offset:
                raise R2DBrokerAheadError(
                    f"broker offset {broker_offset} is ahead of PostgreSQL {database_offset} "
                    f"for partition {partition}"
                )
            if broker_offset < database_offset:
                self._verify_inbox_interval(
                    connection,
                    partition=partition,
                    start=broker_offset,
                    end=database_offset,
                )
                catchup.append(TopicPartition(self.topic, partition, database_offset))
        if catchup:
            consumer.commit(offsets=catchup, asynchronous=False)
        return database_next

    def reconcile_current_broker_offsets(
        self,
        connection: Connection[tuple[object, ...]],
    ) -> tuple[int, ...]:
        """Reconcile the configured consumer group and close the recovery consumer."""

        consumer = self._consumer()
        try:
            return self.reconcile_broker_offsets(connection, consumer)
        finally:
            consumer.close()

    def persist_record(
        self,
        connection: Connection[tuple[object, ...]],
        record: R2DPhysicalRecord,
        *,
        failpoint: R2DServiceFailpoint | None = None,
        crash_handler: Callable[[R2DServiceFailpoint], NoReturn] | None = None,
    ) -> bool:
        """Persist one physical record and contiguous progress atomically.

        Returns ``True`` for a new row and ``False`` for a verified redelivery.
        """

        transcript = capture_transcript((record,), self.data_config)
        captured = transcript.records[0]
        headers_json = [header.model_dump(mode="json") for header in captured.record.headers]
        lineage_json = (
            None
            if captured.record.lineage is None
            else captured.record.lineage.model_dump(mode="json")
        )
        config_sha256 = r2d_config_sha256(self.data_config)
        inserted = False
        with connection.transaction():
            progress = connection.execute(
                """
                SELECT next_offset
                FROM r2d_capture.partition_progress
                WHERE topic = %s AND partition_id = %s
                FOR UPDATE
                """,
                (record.topic, record.partition),
            ).fetchone()
            if progress is None:
                raise R2DServiceError("partition progress was not initialized")
            next_offset = cast(int, progress[0])
            existing = connection.execute(
                """
                SELECT physical_fingerprint
                FROM r2d_capture.inbox
                WHERE topic = %s AND partition_id = %s AND broker_offset = %s
                """,
                (record.topic, record.partition, record.offset),
            ).fetchone()
            if existing is not None:
                if existing[0] != captured.physical_fingerprint:
                    raise R2DInboxConflictError(
                        "stored transport identity has a different physical fingerprint"
                    )
                if next_offset < record.offset + 1:
                    raise R2DOffsetGapError("redelivery exists without matching durable progress")
            else:
                if record.offset != next_offset:
                    raise R2DOffsetGapError(
                        f"record offset {record.offset} does not equal durable next {next_offset}"
                    )
                connection.execute(
                    """
                    INSERT INTO r2d_capture.inbox (
                        topic, partition_id, broker_offset, physical_fingerprint,
                        key_bytes, headers_json, broker_timestamp_ms, broker_timestamp_type,
                        raw_value, raw_value_sha256, transport_disposition, transport_reason,
                        lineage_json, lineage_sha256, config_sha256
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        record.topic,
                        record.partition,
                        record.offset,
                        captured.physical_fingerprint,
                        record.key_bytes,
                        Jsonb(headers_json),
                        record.broker_timestamp_ms,
                        record.broker_timestamp_type,
                        record.value_bytes,
                        captured.raw_value_sha256,
                        captured.decision.disposition,
                        captured.decision.reason,
                        None if lineage_json is None else Jsonb(lineage_json),
                        captured.lineage_sha256,
                        config_sha256,
                    ),
                )
                connection.execute(
                    """
                    UPDATE r2d_capture.partition_progress
                    SET next_offset = %s, updated_at = clock_timestamp()
                    WHERE topic = %s AND partition_id = %s
                    """,
                    (record.offset + 1, record.topic, record.partition),
                )
                inserted = True
            if failpoint == "before_db_commit":
                if crash_handler is not None:
                    crash_handler(failpoint)
                raise R2DInjectedCrash("injected crash before PostgreSQL commit")
        if failpoint == "after_db_commit_before_broker_commit":
            if crash_handler is not None:
                crash_handler(failpoint)
            raise R2DInjectedCrash("injected crash after DB commit before broker commit")
        return inserted

    def read_back_transcript(
        self,
        connection: Connection[tuple[object, ...]],
        *,
        starts: tuple[int, ...],
        ends: tuple[int, ...],
    ) -> R2DCaptureTranscript:
        """Rebuild the shared transcript and verify every stored derived field."""

        records: list[R2DPhysicalRecord] = []
        stored_evidence: dict[tuple[int, int], tuple[object, ...]] = {}
        rows = connection.execute(
            """
            SELECT partition_id, broker_offset, physical_fingerprint, key_bytes,
                   headers_json, broker_timestamp_ms, broker_timestamp_type,
                   raw_value, raw_value_sha256, transport_disposition, transport_reason,
                   lineage_json, lineage_sha256, config_sha256
            FROM r2d_capture.inbox
            WHERE topic = %s
            ORDER BY partition_id, broker_offset
            """,
            (self.topic,),
        ).fetchall()
        for row in rows:
            partition = cast(int, row[0])
            offset = cast(int, row[1])
            if partition >= len(starts) or not starts[partition] <= offset < ends[partition]:
                raise R2DServiceError("inbox contains a record outside the frozen barrier")
            headers_payload = cast(list[dict[str, str | None]], row[4])
            headers = tuple(R2DHeader.model_validate(item) for item in headers_payload)
            lineage_payload = cast(dict[str, object] | None, row[11])
            lineage = (
                None
                if lineage_payload is None
                else R2DParsedLineage.model_validate(lineage_payload)
            )
            record = R2DPhysicalRecord(
                topic=self.topic,
                partition=partition,
                offset=offset,
                key_hex=None if row[3] is None else cast(bytes, row[3]).hex(),
                headers=headers,
                broker_timestamp_ms=cast(int | None, row[5]),
                broker_timestamp_type=cast(
                    Literal["create_time", "log_append_time", "not_available"], row[6]
                ),
                value_hex=None if row[7] is None else cast(bytes, row[7]).hex(),
                lineage=lineage,
            )
            records.append(record)
            stored_evidence[(partition, offset)] = row
        transcript = capture_transcript(records, self.data_config)
        expected_config_sha256 = r2d_config_sha256(self.data_config)
        for item in transcript.records:
            row = stored_evidence[(item.record.partition, item.record.offset)]
            if (
                row[2] != item.physical_fingerprint
                or row[8] != item.raw_value_sha256
                or row[9] != item.decision.disposition
                or row[10] != item.decision.reason
                or row[12] != item.lineage_sha256
                or row[13] != expected_config_sha256
            ):
                raise R2DServiceError("PostgreSQL inbox read-back evidence mismatch")
        for partition, (start, end) in enumerate(zip(starts, ends, strict=True)):
            self._verify_inbox_interval(
                connection,
                partition=partition,
                start=start,
                end=end,
            )
        return transcript

    def capture_barrier(
        self,
        connection: Connection[tuple[object, ...]],
        broker_ack_ledger: R2DBrokerAckLedger,
        *,
        evidence_starts: tuple[int, ...] | None = None,
    ) -> R2DA2CaptureResult:
        """Capture the current high-watermark barrier with DB-before-broker commits."""

        consumer = self._consumer()
        try:
            starts = self.reconcile_broker_offsets(connection, consumer)
            _, ends = self._watermarks(consumer)
            if any(start > end for start, end in zip(starts, ends, strict=True)):
                raise R2DServiceError("durable start is beyond the broker high watermark")
            consumer.assign(
                [
                    TopicPartition(self.topic, partition, start)
                    for partition, start in enumerate(starts)
                ]
            )
            deadline = time.monotonic() + self.service_config.gate.poll_timeout_seconds
            while True:
                current = self._progress(connection)
                if current == ends:
                    break
                if time.monotonic() > deadline:
                    raise R2DServiceError("consumer did not complete the frozen barrier in time")
                message = consumer.poll(timeout=1.0)
                if message is None:
                    continue
                if message.error() is not None:
                    raise R2DServiceError(f"consumer poll failed: {message.error()}")
                record = physical_record_from_message(message)
                if record.offset >= ends[record.partition]:
                    raise R2DServiceError("consumer observed a record beyond the frozen barrier")
                self.persist_record(connection, record)
                consumer.commit(
                    offsets=[TopicPartition(self.topic, record.partition, record.offset + 1)],
                    asynchronous=False,
                )
            next_offsets = self._progress(connection)
            committed = consumer.committed(
                [
                    TopicPartition(self.topic, partition)
                    for partition in range(self.service_config.broker.partition_count)
                ],
                timeout=10.0,
            )
            committed_offsets = tuple(item.offset for item in committed)
            if committed_offsets != next_offsets:
                raise R2DServiceError("manual broker commits do not match PostgreSQL progress")
            transcript_starts = starts if evidence_starts is None else evidence_starts
            if len(transcript_starts) != len(starts) or any(
                evidence_start > operational_start
                for evidence_start, operational_start in zip(transcript_starts, starts, strict=True)
            ):
                raise R2DServiceError("evidence starts must cover the operational restart range")
            transcript = self.read_back_transcript(
                connection,
                starts=transcript_starts,
                ends=ends,
            )
            acknowledgement_identities = {
                (item.partition, item.offset) for item in broker_ack_ledger.acknowledgements
            }
            transcript_identities = {
                (item.record.partition, item.record.offset) for item in transcript.records
            }
            if acknowledgement_identities != transcript_identities:
                raise R2DServiceError("broker acknowledgements and durable capture differ")
            return R2DA2CaptureResult(
                starts=starts,
                ends=ends,
                next_offsets=next_offsets,
                broker_ack_ledger=broker_ack_ledger,
                transcript=transcript,
            )
        finally:
            consumer.close()


def dump_service_result(result: R2DA2CaptureResult) -> str:
    """Return a secret-free deterministic summary suitable for CLI or receipt assembly."""

    payload = {
        "starts": list(result.starts),
        "ends": list(result.ends),
        "next_offsets": list(result.next_offsets),
        "broker_ack_sha256": result.broker_ack_ledger.content_sha256,
        "transcript_sha256": result.transcript.content_sha256,
        "lineage_sha256": result.transcript.lineage_sha256,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))
