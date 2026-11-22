"""Persisted source observation bound to the v8 workflow action transaction."""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from nexus.contracts import ActorRole
from nexus.policies.authorization import Permission
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_backend import (
    PostgresBackendError,
    PostgresConfig,
    PostgresReadSession,
    _driver,
    _dsn,
    _refuse,
)
from nexus.portfolio.postgres_migrations import read_migration_sql
from nexus.portfolio.postgres_runtime import _RuntimeRead
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.source_freshness import (
    SourceObservation,
    SourceWatchPolicy,
    freshness_reason,
    probe_source,
)
from nexus.portfolio.store import StoreError, canonical, digest
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger


class SourceWatchRuntimeStore(PublicationRuntimeStore):
    """A v7 client cannot silently omit the persisted freshness policy on restart."""

    _schema_columns: ClassVar[dict[str, dict[str, str]]] = {
        **PublicationRuntimeStore._schema_columns,
        "runtime_source_watch": {
            "singleton": "integer",
            "payload": "text",
            "payload_sha256": "text",
        },
        "runtime_source_observations": {
            "id": "text",
            "sequence": "bigint",
            "payload": "text",
            "payload_sha256": "text",
        },
    }
    _schema_tables: ClassVar[set[str]] = set(_schema_columns)
    _uniques: ClassVar[dict[tuple[str, str], list[int]]] = {
        **PublicationRuntimeStore._uniques,
        ("runtime_source_observations", "runtime_source_observations_sequence_key"): [2],
    }
    _history_triggers: ClassVar[dict[str, str]] = {
        **PublicationRuntimeStore._history_triggers,
        "runtime_source_watch": "immutable_source_watch",
        "runtime_source_observations": "immutable_source_observation",
    }
    _forbidden_grants: ClassVar[dict[str, str]] = {
        **PublicationRuntimeStore._forbidden_grants,
        "runtime_source_watch": "INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER",
        "runtime_source_observations": "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER",
    }

    def __init__(
        self,
        config: PostgresConfig,
        *,
        transport: TransportLedger,
        binding: TransportBinding,
        watch: SourceWatchPolicy,
        runtime_root: Path | None = None,
        clock: Callable[[], float] = time.time,
        _failure_hook: Callable[[str], None] | None = None,
    ) -> None:
        super().__init__(
            config,
            transport=transport,
            binding=binding,
            runtime_root=runtime_root,
            clock=clock,
            _failure_hook=_failure_hook,
        )
        self.watch = watch

    @staticmethod
    def _migration_bytes(path: Path | None = None) -> tuple[str, str]:
        if path is not None:
            _refuse("migration_conflict", "Source watch requires the canonical migration")
        raw = read_migration_sql(8)
        return raw.decode("utf-8"), hashlib.sha256(raw).hexdigest()

    def _verify_migration_history(self, db: Any, expected_hash: str, application_role: str) -> None:
        versions = [2, 3, 4, 6, 7, 8]
        hashes = {v: hashlib.sha256(read_migration_sql(v)).hexdigest() for v in versions}
        rows = self._query(
            db, "SELECT * FROM {t} ORDER BY version", t="schema_migrations"
        ).fetchall()
        if (
            expected_hash != hashes[8]
            or [r["version"] for r in rows] != versions
            or any(
                r["migration_sha256"] != hashes[r["version"]]
                or r["application_role"] != application_role
                or not math.isfinite(r["applied_at"])
                for r in rows
            )
        ):
            _refuse("migration_conflict", "Source-watch runtime history conflicts")
        expected = self.watch.payload()
        if self._query(db, "SELECT * FROM {t}", t="runtime_source_watch").fetchall() != [
            {"singleton": 1, "payload": canonical(expected), "payload_sha256": digest(expected)}
        ]:
            _refuse("source_policy_mismatch", "Source-watch binding differs from retained policy")

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
        base = PublicationRuntimeStore(
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
        if current != [2, 3, 4, 6, 7, 8]:
            try:
                base.migrate(migration_dsn=migration_dsn)
            except PostgresBackendError as exc:
                if exc.code != "migration_conflict":
                    raise
                with self._connection(migration) as db:
                    if versions(db) != [2, 3, 4, 6, 7, 8]:
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
            applied = versions(db) == [2, 3, 4, 6, 7]
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
                value = self.watch.payload()
                self._query(
                    db,
                    "INSERT INTO {t} VALUES (1,%s,%s)",
                    (canonical(value), digest(value)),
                    t="runtime_source_watch",
                )
                instant = float(self.clock())
                if not math.isfinite(instant):
                    _refuse("invalid_clock", "Migration requires a finite clock")
                self._query(
                    db,
                    "INSERT INTO {t} VALUES (8,%s,%s,%s)",
                    (checksum, instant, app["user"]),
                    t="schema_migrations",
                )
            self._verify_schema(db, checksum, app["user"])
            self._check_application_role(db, app["user"])
            _RuntimeRead(self, db).snapshot()
            return {
                "version": 8,
                "migration_sha256": checksum,
                "schema": self.config.schema,
                "applied": applied,
                "server_version": db.execute("SHOW server_version").fetchone()["server_version"],
            }

    @staticmethod
    def _source_identity(unit: PostgresReadSession) -> tuple[str, int]:
        last = unit._query(
            "SELECT source_sha256,sequence FROM {t} ORDER BY sequence DESC LIMIT 1",
            t="runtime_publications",
        ).fetchone()
        if last is not None:
            return last["source_sha256"], last["sequence"]
        return unit.snapshot()["seed"]["metadata"]["source_snapshot_sha256"], 0

    def _latest_observation(
        self, unit: PostgresReadSession
    ) -> tuple[SourceObservation | None, int]:
        row = unit._query(
            "SELECT * FROM {t} ORDER BY sequence DESC LIMIT 1",
            t="runtime_source_observations",
        ).fetchone()
        if row is None:
            return None, 0
        try:
            value = json.loads(row["payload"])
            observed = SourceObservation(**value)
            if (
                canonical(value) != row["payload"]
                or digest(value) != row["payload_sha256"]
                or observed.observation_id != row["id"]
                or row["sequence"] < 1
                or observed.policy_sha256 != self.watch.sha256
            ):
                raise ValueError
            return observed, row["sequence"]
        except (ValueError, TypeError, KeyError):
            _refuse("integrity_error", "Retained source observation differs")
        raise AssertionError("unreachable")

    def poll_source(self, actor: Actor) -> dict[str, Any]:
        """Read the pinned source outside the workflow lock, then serialize its receipt.

        Changed bytes are retained as a failed freshness observation, not automatically
        published. The explicit publisher remains responsible for changed-source intent.
        """
        if actor.role is not ActorRole.SYSTEM or actor.actor_id != "source-observer":
            raise StoreError("role_denied", "Only the configured source observer may poll", 403)
        with self.read_snapshot() as unit:
            self._full_scope(unit.snapshot(), actor)
            source, publication_sequence = self._source_identity(unit)
        observed, _ = probe_source(
            self.watch,
            source_sha256=source,
            publication_sequence=publication_sequence,
            clock=self.clock,
        )
        self._stage("source_probe_completed")
        with self.write_transaction() as unit:
            self._full_scope(unit.snapshot(), actor)
            latest, sequence = self._latest_observation(unit)
            now = float(self.clock())
            # Validation includes nonfinite clocks; a regressed clock never grants health.
            current_source, current_sequence = self._source_identity(unit)
            result = freshness_reason(
                self.watch,
                observed,
                source_sha256=current_source,
                publication_sequence=current_sequence,
                now=now,
            )
            if now < observed.completed_at or (latest and now < latest.completed_at):
                raise StoreError(
                    "source_clock_regressed", "Source observation clock regressed", 409
                )
            if latest and (
                observed.started_at < latest.started_at
                or (
                    observed.started_at == latest.started_at
                    and latest.outcome != "healthy"
                    and observed.outcome == "healthy"
                )
            ):
                return {"recorded": False, "reason": "superseded_probe"}
            value = observed.payload()
            unit._query(
                "INSERT INTO {t} VALUES (%s,%s,%s,%s)",
                (observed.observation_id, sequence + 1, canonical(value), digest(value)),
                t="runtime_source_observations",
            )
            self._stage("source_observation_recorded")
            return {
                "recorded": True,
                "reason": result,
                "sequence": sequence + 1,
                "observation_id": observed.observation_id,
            }

    def _freshness(self, unit: PostgresReadSession) -> dict[str, Any]:
        observed, sequence = self._latest_observation(unit)
        source, publication = self._source_identity(unit)
        reason = freshness_reason(
            self.watch,
            observed,
            source_sha256=source,
            publication_sequence=publication,
            now=float(self.clock()),
        )
        return {
            "source_id": self.watch.source_id,
            "policy_sha256": self.watch.sha256,
            "max_age_seconds": self.watch.max_age_seconds,
            "observation_sequence": sequence,
            "reason": reason,
            "claim": "bounded_local_source_observation_not_upstream_event_time",
        }

    def _action_gate(self, unit: PostgresReadSession) -> None:
        super()._action_gate(unit)
        reason = self._freshness(unit)["reason"]
        if reason != "fresh":
            raise StoreError(reason, "Source observation is not current; actions are fenced", 409)
        self._stage("source_freshness_acquired")

    def source_watch_status(self, actor: Actor) -> dict[str, Any]:
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.read_snapshot() as unit:
            self._full_scope(unit.snapshot(), actor)
            return self._freshness(unit)
