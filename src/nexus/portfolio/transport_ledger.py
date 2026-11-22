"""Per-group PostgreSQL inbox, contiguous checkpoints and idempotent business delivery.

Physical capture and business admission have independent locks and checkpoints.
Migration 005 owns a separate schema; existing workflow migrations are unchanged.
There is no claim of a distributed transaction between broker, inbox and business.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.broker_runtime import RuntimeBrokerConfig, decode_runtime_event
from nexus.portfolio.postgres_backend import (
    ZERO_HASH,
    PostgresBackend,
    PostgresBackendError,
    _dsn,
    _refuse,
)
from nexus.portfolio.postgres_migrations import read_migration_sql
from nexus.portfolio.postgres_runtime import PostgresRuntimeControl
from nexus.portfolio.store import StoreError, canonical, digest

if TYPE_CHECKING:
    from nexus.portfolio.transport_recovery import RuntimeDeadLetter


def runtime_target(control: PostgresRuntimeControl) -> str:
    route = _dsn(control.store.config.dsn)
    return digest(
        {
            "host": route["hostaddr"],
            "port": int(route["port"]),
            "database": route["dbname"],
            "schema": control.store.config.schema,
            "runtime_root": control.store.runtime_root.resolve().as_posix(),
        }
    )


@dataclass(frozen=True)
class TransportBinding:
    broker: RuntimeBrokerConfig
    cluster_id: str
    topic_id: str
    partition_count: int
    purpose: Literal["operations", "audit"]
    runtime_target_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.broker, RuntimeBrokerConfig) or any(
            not isinstance(item, str) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,100}", item)
            for item in (self.cluster_id, self.topic_id)
        ):
            raise ValueError("Pin explicit broker cluster and topic identities")
        if type(self.partition_count) is not int or not 1 <= self.partition_count <= 32:
            raise ValueError("Declare between one and 32 partitions")
        if (
            self.purpose not in {"operations", "audit"}
            or (self.purpose == "audit" and self.runtime_target_sha256 is not None)
            or (
                self.purpose == "operations"
                and (
                    not isinstance(self.runtime_target_sha256, str)
                    or not re.fullmatch(r"[0-9a-f]{64}", self.runtime_target_sha256)
                )
            )
        ):
            raise ValueError("Only operations may bind one explicit business projection")

    def payload(self) -> dict[str, Any]:
        return {
            "contract": "nexus.transport-binding.v1",
            "endpoint": self.broker.bootstrap_servers,
            "topic": self.broker.topic,
            "group": self.broker.group_id,
            "locations": list(self.broker.locations),
            "cluster_id": self.cluster_id,
            "topic_id": self.topic_id,
            "partition_count": self.partition_count,
            "purpose": self.purpose,
            "runtime_target_sha256": self.runtime_target_sha256,
            "initial_offset": 0,
        }


@dataclass(frozen=True)
class TransportRecord:
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes | None], ...] = ()
    timestamp_type: int = 0
    timestamp_ms: int = -1

    def __post_init__(self) -> None:
        if any(type(v) is not int or v < 0 for v in (self.partition, self.offset)):
            raise ValueError("Physical record requires nonnegative integer identity")
        if self.offset >= 2**63 - 1 or self.partition >= 32:
            raise ValueError("Physical identity exceeds bounded transport contract")
        if any(
            v is not None and (not isinstance(v, bytes) or len(v) > 65536)
            for v in (self.key, self.value)
        ):
            raise ValueError("Record key/value must be bounded bytes or a tombstone")
        if (
            not isinstance(self.headers, tuple)
            or len(self.headers) > 64
            or any(
                not isinstance(h, tuple)
                or len(h) != 2
                or not isinstance(h[0], str)
                or not h[0]
                or len(h[0].encode()) > 256
                or (h[1] is not None and (not isinstance(h[1], bytes) or len(h[1]) > 4096))
                for h in self.headers
            )
        ):
            raise ValueError("Headers must preserve a bounded ordered bytes multimap")
        if (
            type(self.timestamp_type) is not int
            or self.timestamp_type not in {0, 1, 2}
            or type(self.timestamp_ms) is not int
            or (self.timestamp_type == 0 and self.timestamp_ms != -1)
            or (self.timestamp_type != 0 and not 0 <= self.timestamp_ms < 2**63)
        ):
            raise ValueError("Kafka timestamp type and milliseconds differ")

    def payload(self) -> dict[str, Any]:
        def encode(value: bytes | None) -> str | None:
            return None if value is None else base64.b64encode(value).decode("ascii")

        return {
            "partition": self.partition,
            "offset": self.offset,
            "key": encode(self.key),
            "value": encode(self.value),
            "headers": [[k, encode(v)] for k, v in self.headers],
            "timestamp_type": self.timestamp_type,
            "timestamp_ms": self.timestamp_ms,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> TransportRecord:
        try:

            def decode(value: Any) -> bytes | None:
                if value is None:
                    return None
                if not isinstance(value, str):
                    raise ValueError
                return base64.b64decode(value, validate=True)

            result = cls(
                payload["partition"],
                payload["offset"],
                decode(payload["key"]),
                decode(payload["value"]),
                tuple((h[0], decode(h[1])) for h in payload["headers"]),
                payload["timestamp_type"],
                payload["timestamp_ms"],
            )
            if result.payload() != payload:
                raise ValueError
            return result
        except (KeyError, ValueError, TypeError, IndexError):
            raise PostgresBackendError(
                "integrity_error", "Physical record encoding differs"
            ) from None


class TransportLedger(PostgresBackend):
    """One immutable cluster/topic/group binding per separately owned schema."""

    _migration_version = 5
    _schema_columns: ClassVar[dict[str, dict[str, str]]] = {
        "schema_migrations": PostgresBackend._schema_columns["schema_migrations"],
        "transport_binding": {"singleton": "integer", "payload": "text", "payload_sha256": "text"},
        "transport_progress": {
            "partition": "integer",
            "next_offset": "bigint",
            "head_hash": "text",
        },
        "transport_delivery": {
            "partition": "integer",
            "next_offset": "bigint",
            "head_hash": "text",
        },
        **{
            table: {
                "id": "text",
                "partition": "integer",
                "broker_offset": "bigint",
                "payload": "text",
                "payload_sha256": "text",
                "previous_hash": "text",
                "record_hash": "text",
            }
            for table in ("transport_inbox", "transport_receipts")
        },
    }
    _schema_tables: ClassVar[set[str]] = set(_schema_columns)
    _nullable_columns: ClassVar[set[tuple[str, str]]] = set()
    _uniques: ClassVar[dict[tuple[str, str], list[int]]] = {
        (table, table + "_partition_broker_offset_key"): [2, 3]
        for table in ("transport_inbox", "transport_receipts")
    }
    _foreign_keys: ClassVar[dict[tuple[str, str], tuple[list[int], str, list[int]]]] = {
        ("transport_inbox", "transport_inbox_partition_fkey"): ([2], "transport_progress", [1]),
        ("transport_delivery", "transport_delivery_partition_fkey"): (
            [1],
            "transport_progress",
            [1],
        ),
        ("transport_receipts", "transport_receipts_partition_fkey"): (
            [2],
            "transport_delivery",
            [1],
        ),
        ("transport_receipts", "transport_receipts_id_fkey"): ([1], "transport_inbox", [1]),
    }
    _history_triggers: ClassVar[dict[str, str]] = {
        "schema_migrations": "immutable_migration_history",
        "transport_binding": "immutable_transport_binding",
        "transport_inbox": "immutable_transport_inbox",
        "transport_receipts": "immutable_transport_receipts",
    }
    _forbidden_grants: ClassVar[dict[str, str]] = {
        **{
            table: "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER"
            for table in ("transport_binding", "transport_inbox", "transport_receipts")
        },
        **{
            table: "DELETE,TRUNCATE,REFERENCES,TRIGGER"
            for table in ("transport_progress", "transport_delivery")
        },
    }

    @staticmethod
    def _migration_bytes(path: Path | None = None) -> tuple[str, str]:
        if path is not None:
            _refuse("migration_conflict", "Transport requires the canonical packaged migration")
        raw = read_migration_sql(5)
        return raw.decode("utf-8"), hashlib.sha256(raw).hexdigest()

    def _constraint_checks(self) -> dict[tuple[str, str], tuple[int, str]]:
        return {
            ("schema_migrations", "schema_migrations_migration_sha256_check"): (
                2,
                "migration_sha256~'^[0-9a-f]{64}$'::text",
            ),
            ("transport_binding", "transport_binding_singleton_check"): (1, "singleton=1"),
            ("transport_progress", "transport_progress_partition_check"): (1, "partition>=0"),
            **{
                (table, table + "_next_offset_check"): (2, "next_offset>=0")
                for table in ("transport_progress", "transport_delivery")
            },
            **{
                (table, table + "_broker_offset_check"): (3, "broker_offset>=0")
                for table in ("transport_inbox", "transport_receipts")
            },
        }

    @contextmanager
    def _transport(self, binding: TransportBinding, *, write: bool) -> Iterator[Any]:
        app = _dsn(self.config.dsn)
        with self._connection(app) as db, db.transaction():
            if not write:
                db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            self._verify_schema(db, self._migration_bytes()[1], app["user"])
            self._check_application_role(db, app["user"])
            rows = self._query(db, "SELECT * FROM {t}", t="transport_binding").fetchall()
            expected = binding.payload()
            if rows != [
                {"singleton": 1, "payload": canonical(expected), "payload_sha256": digest(expected)}
            ]:
                _refuse(
                    "transport_binding_conflict", "Explicit transport binding differs or is absent"
                )
            yield db

    def register(self, binding: TransportBinding) -> None:
        """Explicit initialization at zero only; recovery never silently initializes progress."""
        app = _dsn(self.config.dsn)
        with self._connection(app) as db, db.transaction():
            self._verify_schema(db, self._migration_bytes()[1], app["user"])
            self._check_application_role(db, app["user"])
            lock = int.from_bytes(
                hashlib.sha256(self.config.schema.encode()).digest()[:8], "big", signed=True
            )
            db.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
            existing = self._query(db, "SELECT * FROM {t}", t="transport_binding").fetchall()
            payload = binding.payload()
            if existing:
                if existing != [
                    {
                        "singleton": 1,
                        "payload": canonical(payload),
                        "payload_sha256": digest(payload),
                    }
                ]:
                    _refuse("transport_binding_conflict", "A transport schema cannot be rebound")
                self._progress(db, binding, delivery=False)
                self._progress(db, binding, delivery=True)
                return
            if any(
                self._query(db, "SELECT 1 FROM {t} LIMIT 1", t=table).fetchone()
                for table in self._schema_tables - {"schema_migrations", "transport_binding"}
            ):
                _refuse("integrity_error", "Cannot initialize over existing transport data")
            self._query(
                db,
                "INSERT INTO {t} VALUES (1,%s,%s)",
                (canonical(payload), digest(payload)),
                t="transport_binding",
            )
            for partition in range(binding.partition_count):
                self._query(
                    db,
                    "INSERT INTO {t} VALUES (%s,0,%s)",
                    (partition, ZERO_HASH),
                    t="transport_progress",
                )
                if binding.purpose == "operations":
                    self._query(
                        db,
                        "INSERT INTO {t} VALUES (%s,0,%s)",
                        (partition, ZERO_HASH),
                        t="transport_delivery",
                    )

    @staticmethod
    def _record_id(binding: TransportBinding, partition: int, offset: int) -> str:
        return digest([digest(binding.payload()), partition, offset])

    def _row_payload(self, binding: TransportBinding, row: dict[str, Any]) -> dict[str, Any]:
        try:
            payload = json.loads(row["payload"])
            valid = (
                isinstance(payload, dict)
                and canonical(payload) == row["payload"]
                and digest(payload) == row["payload_sha256"]
                and row["id"] == self._record_id(binding, row["partition"], row["broker_offset"])
                and re.fullmatch(r"[0-9a-f]{64}", row["previous_hash"]) is not None
                and row["record_hash"]
                == digest([row["id"], row["payload_sha256"], row["previous_hash"]])
            )
        except (ValueError, TypeError, KeyError):
            valid = False
        if not valid:
            _refuse("integrity_error", "Transport row differs from its immutable identity")
        return cast(dict[str, Any], payload)

    def _progress(
        self, db: Any, binding: TransportBinding, *, delivery: bool, lock: bool = False
    ) -> list[dict[str, Any]]:
        table = "transport_delivery" if delivery else "transport_progress"
        history = "transport_receipts" if delivery else "transport_inbox"
        rows = self._query(
            db, "SELECT * FROM {t} ORDER BY partition" + (" FOR UPDATE" if lock else ""), t=table
        ).fetchall()
        expected = (
            [] if delivery and binding.purpose == "audit" else list(range(binding.partition_count))
        )
        if [r["partition"] for r in rows] != expected:
            _refuse("integrity_error", "Transport progress topology differs")
        for row in rows:
            offset = row["next_offset"]
            if type(offset) is not int or offset < 0:
                _refuse("integrity_error", "Transport progress is invalid")
            if offset == 0:
                if row["head_hash"] != ZERO_HASH:
                    _refuse("integrity_error", "Empty transport progress has a nonempty head")
            else:
                tail = self._query(
                    db,
                    "SELECT * FROM {t} WHERE partition=%s AND broker_offset=%s",
                    (row["partition"], offset - 1),
                    t=history,
                ).fetchone()
                if tail is None or tail["record_hash"] != row["head_hash"]:
                    _refuse("integrity_error", "Transport progress has no matching durable tail")
                self._row_payload(binding, tail)
        return cast(list[dict[str, Any]], rows)

    def capture(
        self,
        binding: TransportBinding,
        records: Sequence[TransportRecord],
        *,
        failpoint: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        if not 1 <= len(records) <= 500 or any(
            not isinstance(record, TransportRecord) or record.partition >= binding.partition_count
            for record in records
        ):
            raise ValueError("Capture requires one to 500 records in the bound topology")
        inserted = duplicates = 0
        with self._transport(binding, write=True) as db:
            progress = self._progress(db, binding, delivery=False, lock=True)
            for record in records:
                current = progress[record.partition]
                payload = record.payload()
                if record.offset < current["next_offset"]:
                    old = self._query(
                        db,
                        "SELECT * FROM {t} WHERE partition=%s AND broker_offset=%s",
                        (record.partition, record.offset),
                        t="transport_inbox",
                    ).fetchone()
                    if old is None or self._row_payload(binding, old) != payload:
                        _refuse("physical_record_conflict", "Redelivery differs from durable bytes")
                    duplicates += 1
                    continue
                if record.offset != current["next_offset"]:
                    _refuse("transport_gap", "Contiguous transport capture cannot skip an offset")
                identity = self._record_id(binding, record.partition, record.offset)
                fingerprint = digest(payload)
                head = digest([identity, fingerprint, current["head_hash"]])
                self._query(
                    db,
                    "INSERT INTO {t} VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (
                        identity,
                        record.partition,
                        record.offset,
                        canonical(payload),
                        fingerprint,
                        current["head_hash"],
                        head,
                    ),
                    t="transport_inbox",
                )
                current.update(next_offset=record.offset + 1, head_hash=head)
                inserted += 1
            for row in progress:
                self._query(
                    db,
                    "UPDATE {t} SET next_offset=%s,head_hash=%s WHERE partition=%s",
                    (row["next_offset"], row["head_hash"], row["partition"]),
                    t="transport_progress",
                )
            if failpoint is not None:
                failpoint()
        return {
            "inserted": inserted,
            "duplicates": duplicates,
            "next_offsets": [r["next_offset"] for r in progress],
            "claim": "durable_physical_capture_not_business_admission",
        }

    def _verify_extension_history(self, db: Any, binding: TransportBinding) -> dict[str, Any]:
        """Replay optional histories in the same read-only transport snapshot."""
        return {}

    def capture_progress(self, binding: TransportBinding) -> tuple[int, ...]:
        """Read identity-checked durable tails without advancing or replaying the log."""
        with self._transport(binding, write=False) as db:
            return tuple(row["next_offset"] for row in self._progress(db, binding, delivery=False))

    def capture_action_bounds(
        self, db: Any, binding: TransportBinding, now: float
    ) -> tuple[float, float] | None:
        """Optional persisted health extension; base schemas do not claim this gate."""
        return None

    def verify_history(self, binding: TransportBinding) -> dict[str, Any]:
        """Full bounded-page replay, required at recovery; capture uses incremental tail checks."""
        with self._transport(binding, write=False) as db:
            captured = self._progress(db, binding, delivery=False)
            delivered = self._progress(db, binding, delivery=True)
            for history, progress in (
                ("transport_inbox", captured),
                ("transport_receipts", delivered),
            ):
                for pointer in progress:
                    offset, previous = 0, ZERO_HASH
                    while True:
                        if history == "transport_receipts":
                            rows = self._query(
                                db,
                                "SELECT r.*,i.record_hash AS captured_hash FROM {t} r "
                                "LEFT JOIN {inbox} i ON i.id=r.id "
                                "WHERE r.partition=%s AND r.broker_offset>=%s "
                                "ORDER BY r.broker_offset LIMIT 1000",
                                (pointer["partition"], offset),
                                t=history,
                                inbox="transport_inbox",
                            ).fetchall()
                        else:
                            rows = self._query(
                                db,
                                "SELECT * FROM {t} WHERE partition=%s AND broker_offset>=%s "
                                "ORDER BY broker_offset LIMIT 1000",
                                (pointer["partition"], offset),
                                t=history,
                            ).fetchall()
                        if not rows:
                            break
                        for row in rows:
                            payload = self._row_payload(binding, row)
                            if row["broker_offset"] != offset or row["previous_hash"] != previous:
                                _refuse(
                                    "integrity_error",
                                    "Transport history contains a gap or broken chain",
                                )
                            if history == "transport_inbox":
                                physical = TransportRecord.from_payload(payload)
                                if (physical.partition, physical.offset) != (
                                    row["partition"],
                                    offset,
                                ):
                                    _refuse("integrity_error", "Physical record coordinates differ")
                            else:
                                if (
                                    row["captured_hash"] is None
                                    or payload.get("inbox_record_hash") != row["captured_hash"]
                                ):
                                    _refuse(
                                        "integrity_error",
                                        "Delivery receipt lacks its captured record",
                                    )
                            offset, previous = offset + 1, row["record_hash"]
                    if (offset, previous) != (pointer["next_offset"], pointer["head_hash"]):
                        _refuse("integrity_error", "Transport projection differs from full history")
            if any(r["next_offset"] > captured[r["partition"]]["next_offset"] for r in delivered):
                _refuse("integrity_error", "Business delivery is ahead of durable capture")
            if (
                binding.purpose == "audit"
                and self._query(db, "SELECT 1 FROM {t} LIMIT 1", t="transport_receipts").fetchone()
            ):
                _refuse("integrity_error", "Audit consumers cannot own business receipts")
            return {
                "binding_sha256": digest(binding.payload()),
                "captured": [r["next_offset"] for r in captured],
                "delivered": [r["next_offset"] for r in delivered],
                "capture_heads": [r["head_hash"] for r in captured],
                **self._verify_extension_history(db, binding),
            }

    def reconcile(
        self,
        binding: TransportBinding,
        committed: Sequence[int],
        watermarks: Sequence[tuple[int, int]],
    ) -> list[int]:
        state = self.verify_history(binding)
        if len(committed) != binding.partition_count or len(watermarks) != binding.partition_count:
            raise ValueError("Exact transport vectors are required")
        for durable, acknowledged, (low, high) in zip(
            state["captured"], committed, watermarks, strict=True
        ):
            if (
                any(type(v) is not int or v < 0 for v in (acknowledged, low, high))
                or not low <= durable <= high
                or acknowledged > durable
            ):
                _refuse(
                    "transport_reconciliation_failed",
                    "Broker progress or retention differs from durable proof",
                )
        return list(state["captured"])

    def pending_dead_letter(
        self, binding: TransportBinding, partition: int, actor: Actor
    ) -> RuntimeDeadLetter | None:
        """Inspect exactly the pending record without acknowledging, dropping or repairing it.

        Actor must come from the authenticated service boundary. This read-only
        candidate is not a durable quarantine or a recovery permit. A later write
        must rejoin the exact inbox hash and delivery position under its lock.
        """
        from nexus.portfolio.transport_recovery import RuntimeDeadLetter

        if actor.role not in {ActorRole.PLANNER, ActorRole.APPROVER, ActorRole.AUDITOR}:
            raise StoreError(
                "role_denied", "Recovery inspection requires an audit-capable role", 403
            )
        if set(actor.locations) != set(binding.broker.locations):
            raise StoreError(
                "scope_denied", "Recovery inspection requires the complete bound scope", 403
            )
        if (
            binding.purpose != "operations"
            or type(partition) is not int
            or not 0 <= partition < binding.partition_count
        ):
            raise StoreError("invalid_dead_letter", "Inspect an explicit operations partition", 422)
        with self._transport(binding, write=False) as db:
            captured = self._progress(db, binding, delivery=False)[partition]
            delivered = self._progress(db, binding, delivery=True)[partition]
            offset = delivered["next_offset"]
            if offset > captured["next_offset"]:
                _refuse("integrity_error", "Business delivery is ahead of durable capture")
            if offset == captured["next_offset"]:
                return None
            row = self._query(
                db,
                "SELECT * FROM {t} WHERE partition=%s AND broker_offset=%s",
                (partition, offset),
                t="transport_inbox",
            ).fetchone()
            if row is None:
                _refuse("integrity_error", "Pending physical record is missing")
            record = TransportRecord.from_payload(self._row_payload(binding, row))
            if (record.partition, record.offset) != (partition, offset):
                _refuse("integrity_error", "Pending physical record coordinates differ")
            return RuntimeDeadLetter.create(binding, record, row["record_hash"])

    def dispatch_once(
        self,
        binding: TransportBinding,
        control: PostgresRuntimeControl,
        *,
        failpoint: Callable[[], None] | None = None,
    ) -> dict[str, Any] | None:
        if binding.purpose != "operations" or binding.runtime_target_sha256 != runtime_target(
            control
        ):
            _refuse(
                "dispatch_denied",
                "Only the explicitly bound operations projection may receive events",
            )
        actor = Actor(
            "runtime-event-consumer",
            "Runtime event consumer",
            ActorRole.SYSTEM,
            binding.broker.locations,
        )
        with self._transport(binding, write=True) as db:
            captured = self._progress(db, binding, delivery=False)
            delivered = self._progress(db, binding, delivery=True, lock=True)
            for pointer in delivered:
                partition, offset = pointer["partition"], pointer["next_offset"]
                if offset > captured[partition]["next_offset"]:
                    _refuse("integrity_error", "Delivery progress is ahead of capture")
                if offset == captured[partition]["next_offset"]:
                    continue
                row = self._query(
                    db,
                    "SELECT * FROM {t} WHERE partition=%s AND broker_offset=%s",
                    (partition, offset),
                    t="transport_inbox",
                ).fetchone()
                if row is None:
                    _refuse("integrity_error", "Pending physical record is missing")
                physical = TransportRecord.from_payload(self._row_payload(binding, row))
                if (physical.partition, physical.offset) != (partition, offset):
                    _refuse("integrity_error", "Pending physical record coordinates differ")
                event = decode_runtime_event(physical.value, physical.key)
                admission = control.submit_broker(event, actor)
                if failpoint is not None:
                    failpoint()  # Business committed; failed receipt retries via business dedupe.
                payload = {
                    "inbox_record_hash": row["record_hash"],
                    "event_id": str(event.event_id),
                    "admission": admission,
                    "claim": "business_admission_not_completed_replan",
                }
                fingerprint = digest(payload)
                head = digest([row["id"], fingerprint, pointer["head_hash"]])
                self._query(
                    db,
                    "INSERT INTO {t} VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (
                        row["id"],
                        partition,
                        offset,
                        canonical(payload),
                        fingerprint,
                        pointer["head_hash"],
                        head,
                    ),
                    t="transport_receipts",
                )
                self._query(
                    db,
                    "UPDATE {t} SET next_offset=%s,head_hash=%s WHERE partition=%s",
                    (offset + 1, head, partition),
                    t="transport_delivery",
                )
                return payload
        return None
