"""Persisted same-database transport fence for the PostgreSQL runtime.

Lock order: workflow guard -> capture progress (SHARE); never delivery locks.
Capture only locks capture progress. Dispatch locks delivery before entering the
workflow, so acquiring delivery here would introduce a deadlock. Shared capture
locks survive until the action commits, including the mock execution effect.
This fences committed capture, not events still awaiting publication or capture.
"""

from __future__ import annotations

import hashlib
import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, cast

from nexus.portfolio.postgres_backend import (
    PostgresBackendError,
    PostgresConfig,
    PostgresReadSession,
    _driver,
    _dsn,
    _refuse,
)
from nexus.portfolio.postgres_migrations import read_migration_sql
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore, _RuntimeRead, _RuntimeWrite
from nexus.portfolio.source_freshness import SourceWatchPolicy
from nexus.portfolio.store import StoreError, canonical, digest
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger, runtime_target


@dataclass(frozen=True)
class TransportFenceConfig:
    """Trusted construction input; never selected from an HTTP request or ambient discovery."""

    ledger: PostgresConfig
    binding: TransportBinding
    require_source_publication: bool = False
    source_watch: SourceWatchPolicy | None = None
    recovery_journal: bool = False
    recovery_resolution: bool = False
    capture_health_max_age_seconds: int | None = None

    def __post_init__(self) -> None:
        if self.capture_health_max_age_seconds is not None and (
            not self.recovery_resolution
            or type(self.capture_health_max_age_seconds) is not int
            or not 1 <= self.capture_health_max_age_seconds <= 60
        ):
            raise ValueError("Capture health requires explicit recovery and a bounded lifetime")
        if type(self.require_source_publication) is not bool:
            raise ValueError("Publication mode must be an explicit boolean")
        if type(self.recovery_journal) is not bool or (
            self.recovery_journal and not self.require_source_publication
        ):
            raise ValueError("Recovery journal requires explicit publication mode")
        if type(self.recovery_resolution) is not bool or (
            self.recovery_resolution and not self.recovery_journal
        ):
            raise ValueError("Recovery resolution requires the explicit recovery journal")
        if self.source_watch is not None and (
            not isinstance(self.source_watch, SourceWatchPolicy)
            or not self.require_source_publication
        ):
            raise ValueError("Source watch requires explicit publication mode and policy")


class TransportRuntimeStore(PostgresRuntimeStore):
    """Explicit v6 store. A v4 client refuses this schema, including after restart."""

    _schema_columns: ClassVar[dict[str, dict[str, str]]] = {
        **PostgresRuntimeStore._schema_columns,
        "runtime_transport_fence": {
            "singleton": "integer",
            "payload": "text",
            "payload_sha256": "text",
        },
    }
    _schema_tables: ClassVar[set[str]] = set(_schema_columns)
    _history_triggers: ClassVar[dict[str, str]] = {
        **PostgresRuntimeStore._history_triggers,
        "runtime_transport_fence": "immutable_runtime_transport_fence",
    }
    _forbidden_grants: ClassVar[dict[str, str]] = {
        **PostgresRuntimeStore._forbidden_grants,
        "runtime_transport_fence": "INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER",
    }

    def __init__(
        self,
        config: PostgresConfig,
        *,
        transport: TransportLedger,
        binding: TransportBinding,
        runtime_root: Path | None = None,
        clock: Callable[[], float] = time.time,
        _failure_hook: Callable[[str], None] | None = None,
    ) -> None:
        super().__init__(
            config, runtime_root=runtime_root, clock=clock, _failure_hook=_failure_hook
        )
        business, capture = _dsn(config.dsn), _dsn(transport.config.dsn)
        if (
            any(business[k] != capture[k] for k in ("hostaddr", "port", "dbname"))
            or config.schema == transport.config.schema
            or binding.purpose != "operations"
            or binding.runtime_target_sha256 != runtime_target(self.control)
        ):
            _refuse(
                "configuration_error",
                "Fence requires the bound operations ledger in the same database",
            )
        self.transport, self.transport_binding = transport, binding

    def _fence_payload(self) -> dict[str, Any]:
        return {
            "contract": "nexus.runtime-transport-fence.v1",
            "transport_schema": self.transport.config.schema,
            "transport_application_role": _dsn(self.transport.config.dsn)["user"],
            "binding": self.transport_binding.payload(),
        }

    @staticmethod
    def _migration_bytes(path: Path | None = None) -> tuple[str, str]:
        if path is not None:
            _refuse("migration_conflict", "Fence requires the canonical packaged migration")
        raw = read_migration_sql(6)
        return raw.decode("utf-8"), hashlib.sha256(raw).hexdigest()

    def _constraint_checks(self) -> dict[tuple[str, str], tuple[int, str]]:
        return {
            **super()._constraint_checks(),
            ("runtime_transport_fence", "runtime_transport_fence_singleton_check"): (
                1,
                "singleton=1",
            ),
        }

    def _verify_migration_history(self, db: Any, expected_hash: str, application_role: str) -> None:
        rows = self._query(
            db, "SELECT * FROM {t} ORDER BY version", t="schema_migrations"
        ).fetchall()
        versions = [2, 3, 4, 6]
        hashes = {v: hashlib.sha256(read_migration_sql(v)).hexdigest() for v in versions}
        if (
            expected_hash != hashes[6]
            or [r["version"] for r in rows] != versions
            or any(
                r["migration_sha256"] != hashes[r["version"]]
                or r["application_role"] != application_role
                or not math.isfinite(r["applied_at"])
                for r in rows
            )
        ):
            _refuse("migration_conflict", "Transport-fenced runtime migration history conflicts")

    def _verify_schema(self, db: Any, expected_hash: str, application_role: str) -> None:
        super()._verify_schema(db, expected_hash, application_role)
        expected = self._fence_payload()
        if self._query(db, "SELECT * FROM {t}", t="runtime_transport_fence").fetchall() != [
            {"singleton": 1, "payload": canonical(expected), "payload_sha256": digest(expected)}
        ]:
            _refuse("transport_binding_conflict", "Persisted runtime transport fence differs")

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
        # Exact immutable binding and complete existing ledger replay before enabling.
        self.transport.verify_history(self.transport_binding)
        base = PostgresRuntimeStore(self.config, runtime_root=self.runtime_root, clock=self.clock)

        def versions(db: Any) -> list[int]:
            relation = db.execute(
                "SELECT to_regclass(%s) AS relation",
                (self._table("schema_migrations").as_string(db),),
            ).fetchone()
            if relation["relation"] is None:
                return []
            return [
                r["version"]
                for r in self._query(
                    db, "SELECT version FROM {t} ORDER BY version", t="schema_migrations"
                ).fetchall()
            ]

        with self._connection(migration) as db:
            current = versions(db)
        if current != [2, 3, 4, 6]:
            try:
                base.migrate(migration_dsn=migration_dsn)
            except PostgresBackendError as exc:
                if exc.code != "migration_conflict":
                    raise
                with self._connection(migration) as db:
                    if versions(db) != [2, 3, 4, 6]:
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
            applied = versions(db) == [2, 3, 4]
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
                value = self._fence_payload()
                self._query(
                    db,
                    "INSERT INTO {t} VALUES (1,%s,%s)",
                    (canonical(value), digest(value)),
                    t="runtime_transport_fence",
                )
                instant = float(self.clock())
                if not math.isfinite(instant):
                    _refuse("invalid_clock", "Migration requires a finite clock")
                self._query(
                    db,
                    "INSERT INTO {t} VALUES (6,%s,%s,%s)",
                    (checksum, instant, app["user"]),
                    t="schema_migrations",
                )
            self._verify_schema(db, checksum, app["user"])
            self._check_application_role(db, app["user"])
            _RuntimeRead(self, db).snapshot()
            return {
                "version": 6,
                "migration_sha256": checksum,
                "schema": self.config.schema,
                "applied": applied,
                "server_version": db.execute("SHOW server_version").fetchone()["server_version"],
            }

    def _action_gate(self, unit: PostgresReadSession) -> None:
        ledger, binding, db = self.transport, self.transport_binding, unit._db
        # No second connection/transaction: these row locks survive the action commit.
        ledger._verify_schema(db, ledger._migration_bytes()[1], _dsn(ledger.config.dsn)["user"])
        expected = binding.payload()
        if ledger._query(db, "SELECT * FROM {t}", t="transport_binding").fetchall() != [
            {"singleton": 1, "payload": canonical(expected), "payload_sha256": digest(expected)}
        ]:
            _refuse("transport_binding_conflict", "Action transport binding differs")
        ledger._query(
            db, "SELECT * FROM {t} ORDER BY partition FOR SHARE", t="transport_progress"
        ).fetchall()
        captured = ledger._progress(db, binding, delivery=False)
        for pointer in captured:
            latest = ledger._query(
                db,
                "SELECT broker_offset FROM {t} WHERE partition=%s "
                "ORDER BY broker_offset DESC LIMIT 1",
                (pointer["partition"],),
                t="transport_inbox",
            ).fetchone()
            actual = 0 if latest is None else latest["broker_offset"] + 1
            if actual != pointer["next_offset"]:
                _refuse("integrity_error", "Capture progress differs from the retained inbox tail")
        delivered = ledger._progress(db, binding, delivery=True)
        if any(
            d["next_offset"] > c["next_offset"] for c, d in zip(captured, delivered, strict=True)
        ):
            _refuse("integrity_error", "Business delivery is ahead of capture")
        if any(
            c["next_offset"] != d["next_offset"] for c, d in zip(captured, delivered, strict=True)
        ):
            raise StoreError(
                "transport_pending",
                "Captured data awaits business admission; approval and new execution are fenced",
                409,
            )
        if set(self._locations(unit.snapshot())) != set(binding.broker.locations):
            raise StoreError(
                "transport_scope_changed",
                "Active plan locations differ from the pinned transport scope",
                409,
            )
        self._stage("transport_fence_acquired")
        bounds = ledger.capture_action_bounds(db, binding, float(self.clock()))
        if bounds is not None:
            cast(_RuntimeWrite, unit)._capture_health_bounds = bounds

    def _finish_write(self, session: PostgresReadSession) -> None:
        super()._finish_write(session)
        bounds = getattr(session, "_capture_health_bounds", None)
        if bounds is not None:
            now = float(self.clock())
            if not math.isfinite(now) or not bounds[0] <= now < bounds[1]:
                raise StoreError(
                    "capture_observation_expired",
                    "Capture health invalid before action commit",
                    409,
                )
