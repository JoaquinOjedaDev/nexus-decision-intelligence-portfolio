"""Durable source bytes and publication intent, fenced until business admission."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

from nexus.contracts import ActorRole, EventEnvelope
from nexus.policies.authorization import Permission
from nexus.policies.replanning import ReplanTrigger
from nexus.portfolio.auth import Actor
from nexus.portfolio.broker_runtime import BrokerAdmissionError, decode_runtime_event
from nexus.portfolio.postgres_backend import (
    PostgresBackendError,
    PostgresReadSession,
    _driver,
    _dsn,
    _refuse,
)
from nexus.portfolio.postgres_migrations import read_migration_sql
from nexus.portfolio.postgres_runtime import _RuntimeRead
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.store import StoreError, canonical, digest
from nexus.portfolio.transport_runtime import TransportRuntimeStore


class PublicationRuntimeStore(TransportRuntimeStore):
    """Explicit v7 runtime; a v6 client cannot omit publication fencing on restart."""

    _schema_columns: ClassVar[dict[str, dict[str, str]]] = {
        **TransportRuntimeStore._schema_columns,
        "runtime_source_bundles": {
            "source_sha256": "text",
            "payload": "text",
            "payload_sha256": "text",
        },
        "runtime_publications": {
            "id": "text",
            "sequence": "bigint",
            "source_sha256": "text",
            "payload": "text",
            "payload_sha256": "text",
            "event_sha256": "text",
            "created_at": "double precision",
        },
        "runtime_publication_receipts": {"id": "text", "payload": "text", "payload_sha256": "text"},
    }
    _schema_tables: ClassVar[set[str]] = set(_schema_columns)
    _uniques: ClassVar[dict[tuple[str, str], list[int]]] = {
        **TransportRuntimeStore._uniques,
        ("runtime_publications", "runtime_publications_sequence_key"): [2],
    }
    _foreign_keys: ClassVar[dict[tuple[str, str], tuple[list[int], str, list[int]]]] = {
        **TransportRuntimeStore._foreign_keys,
        ("runtime_publications", "runtime_publications_source_sha256_fkey"): (
            [3],
            "runtime_source_bundles",
            [1],
        ),
        ("runtime_publication_receipts", "runtime_publication_receipts_id_fkey"): (
            [1],
            "runtime_publications",
            [1],
        ),
    }
    _history_triggers: ClassVar[dict[str, str]] = {
        **TransportRuntimeStore._history_triggers,
        "runtime_source_bundles": "immutable_source_bundle",
        "runtime_publications": "immutable_source_publication",
        "runtime_publication_receipts": "immutable_publication_receipt",
    }
    _forbidden_grants: ClassVar[dict[str, str]] = {
        **TransportRuntimeStore._forbidden_grants,
        **{
            t: "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER"
            for t in (
                "runtime_source_bundles",
                "runtime_publications",
                "runtime_publication_receipts",
            )
        },
    }

    @staticmethod
    def _migration_bytes(path: Path | None = None) -> tuple[str, str]:
        if path is not None:
            _refuse("migration_conflict", "Publication requires the canonical packaged migration")
        raw = read_migration_sql(7)
        return raw.decode("utf-8"), hashlib.sha256(raw).hexdigest()

    def _verify_migration_history(self, db: Any, expected_hash: str, application_role: str) -> None:
        versions = [2, 3, 4, 6, 7]
        hashes = {v: hashlib.sha256(read_migration_sql(v)).hexdigest() for v in versions}
        rows = self._query(
            db, "SELECT * FROM {t} ORDER BY version", t="schema_migrations"
        ).fetchall()
        if (
            expected_hash != hashes[7]
            or [r["version"] for r in rows] != versions
            or any(
                r["migration_sha256"] != hashes[r["version"]]
                or r["application_role"] != application_role
                or not math.isfinite(r["applied_at"])
                for r in rows
            )
        ):
            _refuse("migration_conflict", "Publication runtime history conflicts")

    def migrate(self, *, migration_dsn: str, migration_path: Path | None = None) -> dict[str, Any]:
        app, migration = _dsn(self.config.dsn), _dsn(migration_dsn)
        if app["user"] == migration["user"] or any(
            app[k] != migration[k] for k in ("hostaddr", "port", "dbname")
        ):
            _refuse(
                "configuration_error",
                "Separate bootstrap identity on the same database is required",
            )
        template, checksum = self._migration_bytes(migration_path)
        self.transport.verify_history(self.transport_binding)
        base = TransportRuntimeStore(
            self.config,
            transport=self.transport,
            binding=self.transport_binding,
            runtime_root=self.runtime_root,
            clock=self.clock,
        )

        def versions(db: Any) -> list[int]:
            found = db.execute(
                "SELECT to_regclass(%s) AS relation",
                (self._table("schema_migrations").as_string(db),),
            ).fetchone()
            return (
                []
                if found["relation"] is None
                else [
                    r["version"]
                    for r in self._query(
                        db, "SELECT version FROM {t} ORDER BY version", t="schema_migrations"
                    ).fetchall()
                ]
            )

        with self._connection(migration) as db:
            current = versions(db)
        if current != [2, 3, 4, 6, 7]:
            try:
                base.migrate(migration_dsn=migration_dsn)
            except PostgresBackendError as exc:
                if exc.code != "migration_conflict":
                    raise
                with self._connection(migration) as db:
                    if versions(db) != [2, 3, 4, 6, 7]:
                        raise
        lock = int.from_bytes(
            hashlib.sha256(self.config.schema.encode()).digest()[:8], "big", signed=True
        )
        sql = _driver().sql
        with self._connection(migration) as db, db.transaction():
            db.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
            owner = db.execute(
                "SELECT pg_get_userbyid(nspowner) AS owner FROM pg_catalog.pg_namespace "
                "WHERE nspname=%s",
                (self.config.schema,),
            ).fetchone()
            if owner is None or owner["owner"] != migration["user"]:
                _refuse("migration_conflict", "Schema bootstrap owner differs")
            self._query(db, "SELECT * FROM {t} WHERE singleton=1 FOR UPDATE", t="workflow_guard")
            applied = versions(db) == [2, 3, 4, 6]
            if applied:
                base._verify_schema(db, base._migration_bytes()[1], app["user"])
                base._check_application_role(db, app["user"])
                _RuntimeRead(base, db).snapshot()
                db.execute(
                    sql.SQL(template).format(
                        schema=sql.Identifier(self.config.schema),
                        application_role=sql.Identifier(app["user"]),
                    )
                )
                instant = float(self.clock())
                if not math.isfinite(instant):
                    _refuse("invalid_clock", "Migration requires a finite clock")
                self._query(
                    db,
                    "INSERT INTO {t} VALUES (7,%s,%s,%s)",
                    (checksum, instant, app["user"]),
                    t="schema_migrations",
                )
            self._verify_schema(db, checksum, app["user"])
            self._check_application_role(db, app["user"])
            _RuntimeRead(self, db).snapshot()
            return {
                "version": 7,
                "migration_sha256": checksum,
                "schema": self.config.schema,
                "applied": applied,
                "server_version": db.execute("SHOW server_version").fetchone()["server_version"],
            }

    def _bundle(self, db: Any, source_sha256: str) -> SourceBundle:
        row = self._query(
            db,
            "SELECT * FROM {t} WHERE source_sha256=%s",
            (source_sha256,),
            t="runtime_source_bundles",
        ).fetchone()
        if row is None or digest(row["payload"]) != row["payload_sha256"]:
            _refuse("integrity_error", "Publication source bundle is missing or changed")
        result = SourceBundle(row["payload"])
        if result.source_sha256 != source_sha256:
            _refuse("integrity_error", "Publication source identity differs")
        return result

    @staticmethod
    def _publication(row: dict[str, Any]) -> EventEnvelope:
        try:
            value = json.loads(row["payload"])
            event = decode_runtime_event(canonical(value["event"]).encode(), b"portfolio")
            if (
                set(value) != {"event", "actor_id", "actor_role", "locations"}
                or canonical(value) != row["payload"]
                or digest(value) != row["payload_sha256"]
                or digest(value["event"]) != row["event_sha256"]
                or str(event.event_id) != row["id"]
                or event.payload != {"source_snapshot_sha256": row["source_sha256"]}
                or value["actor_id"] != "source-publisher"
                or value["actor_role"] != "system"
                or not isinstance(value["locations"], list)
                or not value["locations"]
                or row["sequence"] < 1
                or not math.isfinite(row["created_at"])
            ):
                raise ValueError
            return event
        except (ValueError, KeyError, TypeError, BrokerAdmissionError):
            _refuse("integrity_error", "Publication identity differs from its retained event")
        raise AssertionError("unreachable")

    def enqueue_source(
        self, bundle: SourceBundle, event: EventEnvelope, actor: Actor
    ) -> dict[str, Any]:
        if actor.role is not ActorRole.SYSTEM or actor.actor_id != "source-publisher":
            raise StoreError(
                "role_denied", "Only the configured source publisher may enqueue source bytes", 403
            )
        source = bundle.source_sha256
        event = decode_runtime_event(event.model_dump_json().encode(), b"portfolio")
        if event.payload != {"source_snapshot_sha256": source} or event.event_type not in {
            v.value for v in ReplanTrigger
        }:
            raise StoreError(
                "invalid_publication",
                "Bind a material source event to these exact source bytes",
                422,
            )
        value = {
            "event": event.model_dump(mode="json"),
            "actor_id": actor.actor_id,
            "actor_role": actor.role.value,
            "locations": sorted(actor.locations),
        }
        with self.write_transaction() as unit:
            snapshot = unit.snapshot()
            self._full_scope(snapshot, actor)
            if set(actor.locations) != set(self.transport_binding.broker.locations):
                raise StoreError(
                    "scope_denied", "Publisher scope must match the transport binding", 403
                )
            now = float(self.clock())
            if not math.isfinite(now) or max(
                event.occurred_at, event.ingested_at
            ) > datetime.fromtimestamp(now, UTC):
                raise StoreError("future_event", "Publication time cannot be in the future", 422)
            previous = unit._query(
                "SELECT * FROM {t} WHERE id=%s", (str(event.event_id),), t="runtime_publications"
            ).fetchone()
            if previous:
                self._publication(previous)
                if previous["payload"] != canonical(value):
                    raise StoreError(
                        "publication_conflict", "Event ID is bound to another publication"
                    )
                self._bundle(unit._db, source)
                return {
                    "id": str(event.event_id),
                    "sequence": previous["sequence"],
                    "duplicate": True,
                    "source_sha256": source,
                }
            last = unit._query(
                "SELECT sequence,source_sha256 FROM {t} ORDER BY sequence DESC LIMIT 1",
                t="runtime_publications",
            ).fetchone()
            current_source = (
                last["source_sha256"]
                if last
                else snapshot["seed"]["metadata"]["source_snapshot_sha256"]
            )
            if source == current_source:
                raise StoreError("unchanged_source", "Publication requires changed source bytes")
            present = unit._query(
                "SELECT payload FROM {t} WHERE source_sha256=%s",
                (source,),
                t="runtime_source_bundles",
            ).fetchone()
            if present is None:
                unit._query(
                    "INSERT INTO {t} VALUES (%s,%s,%s)",
                    (source, bundle.payload, digest(bundle.payload)),
                    t="runtime_source_bundles",
                )
            elif self._bundle(unit._db, source).payload != bundle.payload:
                _refuse("integrity_error", "Source identity has conflicting retained bytes")
            sequence = 1 if last is None else last["sequence"] + 1
            unit._query(
                "INSERT INTO {t} VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    str(event.event_id),
                    sequence,
                    source,
                    canonical(value),
                    digest(value),
                    digest(value["event"]),
                    now,
                ),
                t="runtime_publications",
            )
            self._stage("publication_enqueued")
            return {
                "id": str(event.event_id),
                "sequence": sequence,
                "duplicate": False,
                "source_sha256": source,
            }

    def _admission_gate(
        self, unit: PostgresReadSession, event: EventEnvelope, *, broker: bool = False
    ) -> None:
        if not broker:
            raise StoreError(
                "publication_delivery_required",
                "Published sources require the trusted broker admission path",
                403,
            )
        row = unit._query(
            "SELECT * FROM {t} WHERE id=%s", (str(event.event_id),), t="runtime_publications"
        ).fetchone()
        if row is None:
            raise StoreError(
                "publication_required",
                "Business admission requires retained source publication",
                409,
            )
        if self._publication(row).model_dump(mode="json") != event.model_dump(mode="json"):
            raise StoreError(
                "publication_conflict", "Admitted event differs from its retained publication", 409
            )
        self._bundle(unit._db, row["source_sha256"])
        earlier = unit._query(
            "SELECT p.id FROM {p} p LEFT JOIN {e} e ON e.id=p.id "
            "WHERE p.sequence<%s AND e.id IS NULL LIMIT 1",
            (row["sequence"],),
            p="runtime_publications",
            e="runtime_events",
        ).fetchone()
        if earlier is not None:
            raise StoreError(
                "publication_order_gap", "Earlier source publication must be admitted first", 409
            )

    def _action_gate(self, unit: PostgresReadSession) -> None:
        super()._action_gate(unit)
        row = unit._query(
            "SELECT p.id FROM {p} p LEFT JOIN {e} e ON e.id=p.id "
            "WHERE e.id IS NULL OR e.event_hash<>p.event_sha256 LIMIT 1",
            p="runtime_publications",
            e="runtime_events",
        ).fetchone()
        if row is not None:
            raise StoreError(
                "publication_pending",
                "Source publication awaits business admission; actions are fenced",
                409,
            )

    def materialize_source(self, source_sha256: str) -> Path:
        with self.read_snapshot() as unit:
            bundle = self._bundle(unit._db, source_sha256)
        target = self._path(self.runtime_root / "published-sources" / bundle.source_sha256)
        return bundle.materialize(target)

    def publication_status(self, actor: Actor) -> dict[str, Any]:
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.read_snapshot() as unit:
            self._full_scope(unit.snapshot(), actor)
            counts = unit._query(
                "SELECT COUNT(*) AS accepted,"
                "COUNT(*) FILTER (WHERE r.id IS NOT NULL) AS broker_acknowledged,"
                "COUNT(*) FILTER (WHERE e.event_hash=p.event_sha256) AS business_admitted "
                "FROM {p} p LEFT JOIN {r} r ON r.id=p.id LEFT JOIN {e} e ON e.id=p.id",
                p="runtime_publications",
                r="runtime_publication_receipts",
                e="runtime_events",
            ).fetchone()
            return {
                **counts,
                "pending_admission": counts["accepted"] - counts["business_admitted"],
                "claim": "retained_publications_not_upstream_freshness",
            }
