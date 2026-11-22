"""Guarded PostgreSQL runtime scheduling and immutable generation ownership.

This trusted infrastructure facade is not an HTTP admission route or a broker.
Real runtime-seed.v1 artifacts are bound to their producer envelope; smaller
constructed seeds support storage tests only, never scientific provenance claims.
Computation happens outside transactions in a unique, live-claim evidence directory.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar, cast

from nexus.contracts import ActorRole, EventEnvelope
from nexus.policies.authorization import Permission
from nexus.policies.replanning import ReplanDisposition, ReplanPolicy
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.postgres_backend import (
    ZERO_HASH,
    PostgresBackend,
    PostgresBackendError,
    PostgresConfig,
    PostgresReadSession,
    PostgresWriteSession,
    _driver,
    _dsn,
)
from nexus.portfolio.postgres_migrations import read_migration_sql
from nexus.portfolio.postgres_workflow import (
    PostgresWorkflowStore,
    _integrity,
    _raw,
    _replay,
    _verified_snapshot,
    _WorkflowRead,
)
from nexus.portfolio.store import RECOMMENDATION_TTL_SECONDS, StoreError, canonical, digest, iso

_SHA = re.compile(r"[a-f0-9]{64}\Z")
_JOB = re.compile(r"[a-f0-9]{32}\Z")
_ERROR = re.compile(r"[a-z][a-z0-9_]{0,79}\Z")
_REASON = "Material source change; replacement requires a new human approval"
_CONTRACT = "nexus.portfolio.runtime-seed.v1"
_ACTIVE_STATES = {"pending", "approved", "execution_failed"}
_BASE_BINDINGS = frozenset(
    {"runtime_root_identity", "runtime_initial_evidence_path", "runtime_evidence_path"}
)
_SEED_BINDINGS = frozenset({"runtime_initial_seed_hash", "runtime_ingested_at"})
_RUNTIME_ORDER = {
    "runtime_events": "sequence",
    "runtime_jobs": "id",
    "runtime_generations": "seed_hash",
    "runtime_journal": "sequence",
    "session_scopes": "session_id",
}
_COLUMNS = {
    "runtime_events": {
        "sequence": "bigint",
        "id": "text",
        "event_hash": "text",
        "payload": "text",
        "job_id": "text",
        "received_at": "double precision",
    },
    "runtime_jobs": {
        "id": "text",
        "source_sha256": "text",
        "state": "text",
        "eligible_at": "double precision",
        "created_at": "double precision",
        "base_seed_hash": "text",
        "attempts": "integer",
        "token": "text",
        "lease_until": "double precision",
        "completed_at": "double precision",
        "result_sha256": "text",
        "error": "text",
    },
    "runtime_generations": {
        "seed_hash": "text",
        "payload": "text",
        "archived_at": "double precision",
        "replacement_job_id": "text",
        "evidence_path": "text",
    },
    "runtime_journal": {
        "sequence": "bigint",
        "operation": "text",
        "actor_id": "text",
        "actor_role": "text",
        "occurred_at": "double precision",
        "details": "text",
        "previous_hash": "text",
        "event_hash": "text",
    },
    "session_scopes": {
        "session_id": "text",
        "locations": "text",
        "generation_seed_hash": "text",
        "scope_version": "integer",
    },
}


def _hash_token(value: str) -> str:
    _integrity(isinstance(value, str))
    return hashlib.sha256(value.encode()).hexdigest()


def _instant(clock: Callable[[], float]) -> float:
    instant = float(clock())
    if not math.isfinite(instant):
        raise StoreError("invalid_clock", "A finite runtime clock is required")
    return instant


def _seed_locations(seed: dict[str, Any]) -> set[str]:
    locations: set[str] = set()
    for key in ("recommendations", "forecasts"):
        rows = seed.get(key, [])
        _integrity(isinstance(rows, list))
        for row in rows:
            location = row["location_id"]
            _integrity(isinstance(location, str) and bool(location))
            locations.add(location)
    return locations


def _normalize(seed: dict[str, Any]) -> dict[str, Any]:
    value: dict[str, Any] = json.loads(canonical(seed))
    metadata = value.get("metadata", {})
    if not isinstance(metadata, dict):
        raise ValueError("Runtime metadata must be an object")
    if metadata.pop("runtime_job_id", None) is not None:
        for item in value["recommendations"]:
            item["id"] = item.pop("source_recommendation_id")
        if "runtime_artifact_seed_hash" in metadata:
            value["seed_hash"] = metadata.pop("runtime_artifact_seed_hash")
    return value


def _safe_job(row: dict[str, Any]) -> dict[str, Any]:
    """Private replay representation binds the token without retaining it in the journal."""
    return {**row, "token": _hash_token(row["token"]) if row["token"] is not None else None}


def _runtime_raw(unit: PostgresReadSession) -> dict[str, Any]:
    return {
        **_raw(unit),
        **{
            name: unit._query("SELECT * FROM {table} ORDER BY " + order, table=name).fetchall()
            for name, order in _RUNTIME_ORDER.items()
        },
    }


def _policy_value(policy: ReplanPolicy) -> dict[str, Any]:
    value = policy.model_dump(mode="json")
    for name in ("material_event_types", "invalidating_event_types"):
        value[name] = sorted(value[name])
    return value


def _eligible(
    policy: ReplanPolicy, event: EventEnvelope, now: float, last: float | None
) -> tuple[bool, str, float]:
    evaluation = policy.evaluate(
        event,
        now=datetime.fromtimestamp(now, UTC),
        last_replan_at=datetime.fromtimestamp(last, UTC) if last is not None else None,
    )
    eligible = (
        now
        if evaluation.disposition is ReplanDisposition.TRIGGER_NOW
        else (last + policy.cooldown_seconds if last is not None else now)
    )
    return evaluation.material, evaluation.disposition.value, eligible


def _job_new(identity: str, source: str, eligible: float, now: float, base: str) -> dict[str, Any]:
    return {
        "id": identity,
        "source_sha256": source,
        "state": "queued",
        "eligible_at": eligible,
        "created_at": now,
        "base_seed_hash": base,
        "attempts": 0,
        "token": None,
        "lease_until": None,
        "completed_at": None,
        "result_sha256": None,
        "error": None,
    }


class _RuntimeRead(PostgresReadSession):
    def _snapshot(self) -> dict[str, Any]:
        return cast(PostgresRuntimeStore, self._backend)._runtime_snapshot(self)


class _RuntimeWrite(PostgresWriteSession):
    _activation_deadline: float | None = None
    _capture_health_bounds: tuple[float, float] | None = None

    def _snapshot(self) -> dict[str, Any]:
        return cast(PostgresRuntimeStore, self._backend)._runtime_snapshot(self)

    def ensure_seed(self, seed: dict[str, Any]) -> None:
        backend = cast(PostgresRuntimeStore, self._backend)
        stable = _clone(seed)
        current = self.snapshot()
        if current["seed"] is not None:
            if digest(stable) != current["seed_sha256"]:
                raise PostgresBackendError(
                    "seed_changed", "Evidence differs from the active generation"
                )
            return
        _producer(stable)
        backend._admit(stable, backend.artifact_root)
        super().ensure_seed(stable)
        for key, value in (
            ("runtime_initial_seed_hash", digest(stable)),
            ("runtime_ingested_at", iso(_instant(backend.clock))),
        ):
            self._query("INSERT INTO {table} VALUES (%s,%s)", (key, value), table="metadata")


class PostgresRuntimeStore(PostgresWorkflowStore):
    """Explicit v4 workflow/control store; no API, broker, or implicit source computation."""

    _schema_tables = PostgresWorkflowStore._schema_tables | set(_COLUMNS)
    _schema_columns: ClassVar[dict[str, dict[str, str]]] = {
        **PostgresWorkflowStore._schema_columns,
        **_COLUMNS,
    }
    _nullable_columns = PostgresWorkflowStore._nullable_columns | {
        ("runtime_events", "job_id"),
        *(
            ("runtime_jobs", column)
            for column in ("token", "lease_until", "completed_at", "result_sha256", "error")
        ),
    }
    _uniques: ClassVar[dict[tuple[str, str], list[int]]] = {
        **PostgresWorkflowStore._uniques,
        ("runtime_events", "runtime_events_id_key"): [2],
    }
    _foreign_keys: ClassVar[dict[tuple[str, str], tuple[list[int], str, list[int]]]] = {
        **PostgresWorkflowStore._foreign_keys,
        ("runtime_events", "runtime_events_job_id_fkey"): ([5], "runtime_jobs", [1]),
        ("runtime_generations", "runtime_generations_replacement_job_id_fkey"): (
            [4],
            "runtime_jobs",
            [1],
        ),
        ("session_scopes", "session_scopes_session_id_fkey"): ([1], "sessions", [1]),
    }
    _history_triggers: ClassVar[dict[str, str]] = {
        **PostgresWorkflowStore._history_triggers,
        "runtime_events": "immutable_runtime_event_history",
        "runtime_generations": "immutable_runtime_generation_history",
        "runtime_journal": "immutable_runtime_journal_history",
        "session_scopes": "immutable_session_scope_history",
    }
    _forbidden_grants: ClassVar[dict[str, str]] = {
        **PostgresWorkflowStore._forbidden_grants,
        "runtime_jobs": "DELETE,TRUNCATE,REFERENCES,TRIGGER",
        **{
            table: "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER"
            for table in (
                "runtime_events",
                "runtime_generations",
                "runtime_journal",
                "session_scopes",
            )
        },
    }

    def __init__(
        self,
        config: PostgresConfig,
        *,
        runtime_root: Path | None = None,
        clock: Callable[[], float] = time.time,
        _failure_hook: Callable[[str], None] | None = None,
    ) -> None:
        super().__init__(config, clock=clock, _failure_hook=_failure_hook)
        self.runtime_root = config.artifact_root if runtime_root is None else runtime_root
        if (
            not isinstance(self.runtime_root, Path)
            or not self.runtime_root.is_absolute()
            or self.runtime_root == Path(self.runtime_root.anchor)
            or self.runtime_root.anchor.startswith(("\\\\", "//"))
            or ".." in self.runtime_root.parts
        ):
            raise PostgresBackendError(
                "configuration_error", "An explicit local runtime root is required"
            )

    @property
    def control(self) -> PostgresRuntimeControl:
        return PostgresRuntimeControl(self)

    def _path(self, candidate: Path, *, directory: bool = False) -> Path:
        if (
            not isinstance(candidate, Path)
            or not candidate.is_absolute()
            or ".." in candidate.parts
        ):
            raise StoreError("invalid_path", "Runtime evidence requires an absolute local path")
        for path in (self.runtime_root, candidate):
            if path.anchor.startswith(("\\\\", "//")):
                raise StoreError("invalid_path", "Network runtime evidence is forbidden")
            for ancestor in (*reversed(path.parents), path):
                if ancestor.is_symlink() or ancestor.is_junction():
                    raise StoreError("invalid_path", "Linked runtime evidence is forbidden")
        resolved, root = candidate.resolve(), self.runtime_root.resolve()
        if not resolved.is_relative_to(root) or (directory and not resolved.is_dir()):
            raise StoreError("invalid_path", "Evidence must remain in the declared runtime root")
        return resolved

    def _relative(self, candidate: Path) -> str:
        return self._path(candidate).relative_to(self.runtime_root.resolve()).as_posix()

    def _evidence(self, reference: str) -> Path:
        if not isinstance(reference, str):
            raise StoreError("invalid_path", "Runtime evidence reference is invalid")
        relative = Path(reference)
        if (
            relative.is_absolute()
            or relative.drive
            or ".." in relative.parts
            or relative.as_posix() != reference
        ):
            raise StoreError("invalid_path", "Runtime evidence reference is not canonical")
        return self._path(self.runtime_root / relative, directory=True)

    def _root_bindings(self) -> dict[str, str]:
        initial = self._relative(self.artifact_root)
        return {
            "runtime_root_identity": digest(
                {"root": self.runtime_root.resolve().as_posix(), "initial": initial}
            ),
            "runtime_initial_evidence_path": initial,
            "runtime_evidence_path": initial,
        }

    def _admit(self, seed: dict[str, Any], evidence: Path) -> None:
        root = self._path(evidence, directory=True)
        if not isinstance(seed, dict) or seed.get("executable") is not True:
            raise StoreError(
                "non_executable_plan", "An explicitly executable runtime plan is required"
            )
        try:
            value = _normalize(seed)
            metadata = value["metadata"]
            items, artifacts = value["recommendations"], value["artifacts"]
            if (
                not isinstance(metadata, dict)
                or metadata.get("pipeline_mode") != "connected_runtime_baseline"
                or not _SHA.fullmatch(metadata.get("source_snapshot_sha256", ""))
                or not isinstance(items, list)
                or not items
                or len({item["id"] for item in items}) != len(items)
                or not isinstance(artifacts, list)
                or not artifacts
            ):
                raise ValueError
            _seed_locations(value)
            for item in items:
                if any(
                    not isinstance(item.get(key), str) or not item[key]
                    for key in ("id", "location_id")
                ):
                    raise ValueError
                if "created_by" in item and (
                    not isinstance(item["created_by"], str) or not item["created_by"]
                ):
                    raise ValueError
            if "seed_hash" in value and value["seed_hash"] != digest(
                {key: item for key, item in value.items() if key != "seed_hash"}
            ):
                raise ValueError
            declared = value.get("contract_version")
            if declared not in (None, _CONTRACT) or (
                declared is None
                and any(
                    key in value
                    for key in ("runtime_lineage", "original_manifest", "input_fingerprint")
                )
            ):
                raise ValueError
        except (KeyError, TypeError, ValueError, OverflowError):
            raise StoreError(
                "invalid_plan", "Runtime plan structure or self-identity is invalid"
            ) from None
        try:
            PostgresBackend(replace(self.config, artifact_root=root))._verify_artifacts(value)
            if declared == _CONTRACT:
                from nexus.portfolio.runtime_pipeline import load_runtime_seed

                if digest(load_runtime_seed(root)) != digest(value):
                    raise ValueError
        except (KeyError, TypeError, ValueError, OSError):
            raise StoreError(
                "integrity_error", "Runtime plan differs from retained evidence"
            ) from None

    def _raw_state(self, unit: PostgresReadSession) -> dict[str, Any]:
        return _runtime_raw(unit)

    def _admission_gate(
        self, unit: PostgresReadSession, event: EventEnvelope, *, broker: bool = False
    ) -> None:
        """Extension point checked under the workflow guard before event admission."""

    def _new_session(self, db: Any, *, write: bool) -> PostgresReadSession:
        return _RuntimeWrite(self, db) if write else _RuntimeRead(self, db)

    @staticmethod
    def _migration_bytes(path: Path | None = None) -> tuple[str, str]:
        if path is not None:
            return PostgresBackend._migration_bytes(path)
        try:
            raw = read_migration_sql(4)
            return raw.decode("utf-8"), hashlib.sha256(raw).hexdigest()
        except (ValueError, OSError, UnicodeError):
            raise PostgresBackendError(
                "migration_conflict", "Runtime migration artifact unavailable"
            ) from None

    def _verify_migration_history(self, db: Any, expected_hash: str, application_role: str) -> None:
        rows = self._query(
            db, "SELECT * FROM {table} ORDER BY version", table="schema_migrations"
        ).fetchall()
        hashes = {
            2: PostgresBackend._migration_bytes()[1],
            3: PostgresWorkflowStore._migration_bytes()[1],
            4: expected_hash,
        }
        if [row["version"] for row in rows] != [2, 3, 4] or any(
            row["migration_sha256"] != hashes[row["version"]]
            or row["application_role"] != application_role
            or not math.isfinite(row["applied_at"])
            for row in rows
        ):
            raise PostgresBackendError("migration_conflict", "Runtime migration history conflicts")

    def migrate(self, *, migration_dsn: str, migration_path: Path | None = None) -> dict[str, Any]:
        application, migration = _dsn(self.config.dsn), _dsn(migration_dsn)
        if application["user"] == migration["user"] or any(
            application[key] != migration[key] for key in ("hostaddr", "port", "dbname")
        ):
            raise PostgresBackendError(
                "configuration_error",
                "Separate bootstrap identity on the same database is required",
            )
        template, checksum = self._migration_bytes(migration_path)
        bindings = self._root_bindings()
        workflow = PostgresWorkflowStore(self.config, clock=self.clock)
        # Preserve a valid v3 checkpoint if a later v4 admission fails. Existing
        # v4 databases must never be passed through the intentionally strict v3 client.
        with self._connection(migration) as db:
            relation = db.execute(
                "SELECT to_regclass(%s) AS relation",
                (self._table("schema_migrations").as_string(db),),
            ).fetchone()
            versions = (
                []
                if relation["relation"] is None
                else [
                    row["version"]
                    for row in self._query(
                        db,
                        "SELECT version FROM {table} ORDER BY version",
                        table="schema_migrations",
                    ).fetchall()
                ]
            )
        if versions != [2, 3, 4]:
            try:
                workflow.migrate(migration_dsn=migration_dsn)
            except PostgresBackendError as exc:
                if exc.code != "migration_conflict":
                    raise
                # A concurrent initializer may have completed v4 since the
                # preliminary read. Only the full strict v4 verification below
                # can admit that state; no checksum/owner error is waived.
                with self._connection(migration) as db:
                    latest = [
                        row["version"]
                        for row in self._query(
                            db,
                            "SELECT version FROM {table} ORDER BY version",
                            table="schema_migrations",
                        ).fetchall()
                    ]
                if latest != [2, 3, 4]:
                    raise
        sql = _driver().sql
        lock = int.from_bytes(
            hashlib.sha256(self.config.schema.encode()).digest()[:8], "big", signed=True
        )
        with self._connection(migration) as db, db.transaction():
            db.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
            owner = db.execute(
                "SELECT pg_get_userbyid(nspowner) AS owner "
                "FROM pg_catalog.pg_namespace WHERE nspname=%s",
                (self.config.schema,),
            ).fetchone()
            if owner is None or owner["owner"] != migration["user"]:
                raise PostgresBackendError("migration_conflict", "Schema bootstrap owner differs")
            versions = [
                row["version"]
                for row in self._query(
                    db, "SELECT version FROM {table} ORDER BY version", table="schema_migrations"
                ).fetchall()
            ]
            self._query(
                db, "SELECT * FROM {table} WHERE singleton=1 FOR UPDATE", table="workflow_guard"
            )
            applied = versions == [2, 3]
            if applied:
                workflow._verify_schema(db, workflow._migration_bytes()[1], application["user"])
                workflow._check_application_role(db, application["user"])
                before = _WorkflowRead(workflow, db).snapshot()
                if before["seed"] is not None:
                    self._admit(before["seed"], self.artifact_root)
                db.execute(
                    sql.SQL(template).format(
                        schema=sql.Identifier(self.config.schema),
                        application_role=sql.Identifier(application["user"]),
                    )
                )
                self._query(
                    db,
                    "INSERT INTO {table} VALUES (4,%s,%s,%s)",
                    (checksum, _instant(self.clock), application["user"]),
                    table="schema_migrations",
                )
                unit = _RuntimeWrite(self, db)
                for key, value in bindings.items():
                    unit._query(
                        "INSERT INTO {table} VALUES (%s,%s)", (key, value), table="metadata"
                    )
                if before["seed"] is not None:
                    for key, value in (
                        ("runtime_initial_seed_hash", before["seed_sha256"]),
                        ("runtime_ingested_at", iso(_instant(self.clock))),
                    ):
                        unit._query(
                            "INSERT INTO {table} VALUES (%s,%s)", (key, value), table="metadata"
                        )
                    scopes = canonical(list(PostgresWorkflowStore._locations(before)))
                    for row in unit._query(
                        "SELECT id FROM {table} ORDER BY id", table="sessions"
                    ).fetchall():
                        unit._query(
                            "INSERT INTO {table} VALUES (%s,%s,%s,3)",
                            (row["id"], scopes, before["seed_sha256"]),
                            table="session_scopes",
                        )
                self._seal(unit)
            self._verify_schema(db, checksum, application["user"])
            self._check_application_role(db, application["user"])
            _RuntimeRead(self, db).snapshot()
            return {
                "version": 4,
                "migration_sha256": checksum,
                "schema": self.config.schema,
                "applied": applied,
                "server_version": db.execute("SHOW server_version").fetchone()["server_version"],
            }

    def _runtime_snapshot(self, unit: PostgresReadSession) -> dict[str, Any]:
        raw = _runtime_raw(unit)
        bindings = self._root_bindings()
        for key in ("runtime_root_identity", "runtime_initial_evidence_path"):
            _integrity(raw["metadata"].get(key) == bindings[key], "Runtime root binding differs")
        seeded = "seed" in raw["metadata"]
        extra = _BASE_BINDINGS | (_SEED_BINDINGS if seeded else frozenset())
        result = _verified_snapshot(
            unit, _raw_value=raw, _metadata_extra=extra, _verify=self._verify_runtime
        )
        if not seeded:
            _integrity(all(not raw[key] for key in _RUNTIME_ORDER))
            _integrity(
                raw["metadata"]["runtime_evidence_path"] == bindings["runtime_evidence_path"]
            )
        return {
            **result,
            "runtime_jobs": [
                {key: value for key, value in row.items() if key != "token"}
                for row in raw["runtime_jobs"]
            ],
            "runtime_events": raw["runtime_events"],
            "runtime_generations": raw["runtime_generations"],
            "runtime_journal": raw["runtime_journal"],
        }

    def _verify_runtime(self, raw: dict[str, Any], seed: dict[str, Any]) -> None:
        _verify_runtime(self, raw, seed)

    def _finish_write(self, session: PostgresReadSession) -> None:
        super()._finish_write(session)
        deadline = getattr(session, "_activation_deadline", None)
        if deadline is not None and _instant(self.clock) >= deadline:
            raise StoreError("stale_worker", "Worker lease expired before activation committed")

    @staticmethod
    def _locations(snapshot: dict[str, Any]) -> tuple[str, ...]:
        result = {row["location_id"] for row in snapshot["recommendations"]}
        if snapshot["seed"] is not None:
            result.update(_seed_locations(snapshot["seed"]))
        for row in snapshot.get("runtime_generations", []):
            result.update(_seed_locations(json.loads(row["payload"])))
        return tuple(sorted(result))

    @staticmethod
    def _full_scope(snapshot: dict[str, Any], actor: Actor) -> None:
        if not set(PostgresRuntimeStore._locations(snapshot)).issubset(actor.locations):
            raise StoreError(
                "scope_denied", "Shared runtime requires current and historical plan locations", 403
            )

    def _record_session_scope(
        self, unit: PostgresReadSession, session_id: str, actor: Actor
    ) -> None:
        # create_session has inserted identity rows but has not resealed yet.
        current = unit._query(
            "SELECT value FROM {table} WHERE key='seed_hash'", table="metadata"
        ).fetchone()["value"]
        unit._query(
            "INSERT INTO {table} VALUES (%s,%s,%s,4)",
            (session_id, canonical(sorted(actor.locations)), current),
            table="session_scopes",
        )

    def _session_actor(
        self, unit: PostgresReadSession, row: dict[str, Any], snapshot: dict[str, Any]
    ) -> Actor:
        scope = unit._query(
            "SELECT locations FROM {table} WHERE session_id=%s",
            (row["id"],),
            table="session_scopes",
        ).fetchone()
        _integrity(scope is not None)
        return resolve_persona(row["persona"], tuple(json.loads(scope["locations"])))

    @staticmethod
    def _generations(snapshot: dict[str, Any]) -> list[tuple[str, dict[str, Any], bool]]:
        return [
            (row["seed_hash"], json.loads(row["payload"]), False)
            for row in snapshot["runtime_generations"]
        ] + [(snapshot["seed_sha256"], snapshot["seed"], True)]

    def overview_snapshot(self, actor: Actor) -> dict[str, Any]:
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.read_snapshot() as unit:
            snapshot = unit.snapshot()
            self._full_scope(snapshot, actor)
            seed = unit.seed()
            owners = {
                item["id"]: (identity, current)
                for identity, generation, current in self._generations(snapshot)
                for item in generation["recommendations"]
            }
            records = [
                {
                    **self._public(row, snapshot, actor),
                    "generation_seed_sha256": owners[row["id"]][0],
                    "current_generation": owners[row["id"]][1],
                }
                for row in snapshot["recommendations"]
            ]
            return {
                **seed,
                "recommendations": records,
                "current_recommendations": [row for row in records if row["current_generation"]],
                "generation_seed_sha256": snapshot["seed_sha256"],
                "checked_at": iso(_instant(self.clock)),
            }

    def recommendation_context(self, recommendation_id: str, actor: Actor) -> dict[str, Any]:
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.read_snapshot() as unit:
            snapshot = unit.snapshot()
            row = self._row(snapshot, recommendation_id, actor)
            self._full_scope(snapshot, actor)
            identity, seed, current = next(
                (identity, seed, current)
                for identity, seed, current in self._generations(snapshot)
                if any(item["id"] == recommendation_id for item in seed["recommendations"])
            )
            return {
                **self._public(row, snapshot, actor),
                "events": [
                    event
                    for event in self._events(snapshot)
                    if event["recommendation_id"] == recommendation_id
                ],
                "scenarios": seed.get("scenarios", []),
                "lineage": seed.get("lineage", []),
                "generation": {
                    "seed_sha256": identity,
                    "is_current": current,
                    "metadata": seed.get("metadata", {}),
                },
            }


def _producer(seed: dict[str, Any]) -> None:
    """Admission accepts producer evidence, never workflow-owned projections."""
    metadata, items = seed.get("metadata"), seed.get("recommendations")
    if (
        not isinstance(metadata, dict)
        or {"runtime_job_id", "runtime_artifact_seed_hash"}.intersection(metadata)
        or not isinstance(items, list)
        or any(not isinstance(item, dict) or "source_recommendation_id" in item for item in items)
    ):
        raise StoreError("invalid_plan", "Admission requires producer-form planning evidence")


def _clone(seed: dict[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(canonical(seed))
        if not isinstance(value, dict):
            raise ValueError
        return cast(dict[str, Any], value)
    except (ValueError, TypeError, OverflowError):
        raise StoreError("invalid_plan", "Runtime plan requires finite canonical JSON") from None


def _verify_runtime(store: PostgresRuntimeStore, raw: dict[str, Any], seed: dict[str, Any]) -> None:
    """Replay runtime state and join every reserved business event to its owner.

    The private whole-state checksum is a fast corruption detector, not a
    substitute for replay. This independently rebuilds mutable jobs, generation
    succession, worker fences and the invalidation/creation event boundaries.
    """
    metadata = raw["metadata"]
    current_hash = digest(seed)
    generations = {
        row["seed_hash"]: json.loads(row["payload"]) for row in raw["runtime_generations"]
    }
    _integrity(
        current_hash not in generations and len(generations) == len(raw["runtime_generations"])
    )
    generations[current_hash] = seed
    archives = {row["seed_hash"]: row for row in raw["runtime_generations"]}
    paths = {identity: row["evidence_path"] for identity, row in archives.items()}
    paths[current_hash] = metadata["runtime_evidence_path"]
    owners: dict[str, dict[str, Any]] = {}
    owner_generation: dict[str, str] = {}
    for identity, generation in generations.items():
        _integrity(_SHA.fullmatch(identity) is not None and digest(generation) == identity)
        if identity in archives:
            _integrity(
                canonical(generation) == archives[identity]["payload"]
                and math.isfinite(archives[identity]["archived_at"])
            )
        store._admit(generation, store._evidence(paths[identity]))
        for item in generation["recommendations"]:
            _integrity(item["id"] not in owners)
            owners[item["id"]] = item
            owner_generation[item["id"]] = identity
    initial = metadata["runtime_initial_seed_hash"]
    _integrity(
        initial in generations and paths[initial] == metadata["runtime_initial_evidence_path"]
    )
    _integrity("runtime_job_id" not in generations[initial]["metadata"])
    events = raw["runtime_events"]
    for sequence, row in enumerate(events, 1):
        event = EventEnvelope.model_validate_json(row["payload"])
        payload = event.model_dump(mode="json")
        _integrity(
            row["sequence"] == sequence
            and canonical(payload) == row["payload"]
            and digest(payload) == row["event_hash"]
            and str(event.event_id) == row["id"]
            and math.isfinite(row["received_at"])
        )
        _integrity(
            set(event.payload) == {"source_snapshot_sha256"}
            and isinstance(event.payload["source_snapshot_sha256"], str)
            and _SHA.fullmatch(event.payload["source_snapshot_sha256"]) is not None
        )
        _integrity(
            max(event.occurred_at, event.ingested_at)
            <= datetime.fromtimestamp(row["received_at"], UTC)
        )
    _integrity(len({row["id"] for row in events}) == len(events))
    workflow = raw["events"]
    rows = {row["id"]: row for row in raw["recommendations"]}
    invalidations: dict[int, str] = {}
    creations: dict[str, tuple[str, float]] = {}
    jobs: dict[str, dict[str, Any]] = {}
    seen, current, event_count, last_boundary = {initial}, initial, 0, 0
    previous, previous_time = ZERO_HASH, float("-inf")
    completed_time: float | None = None
    scopes_by_generation = {initial: _seed_locations(generations[initial])}

    def boundary(
        details: dict[str, Any],
        journal: dict[str, Any],
        job_id: str | None,
        new_seed: dict[str, Any] | None = None,
        birth: float | None = None,
    ) -> None:
        nonlocal last_boundary
        start = details["workflow_start"]
        _integrity(type(start) is int and last_boundary <= start <= len(workflow))
        states: dict[str, str] = {}
        for event in workflow[:start]:
            kind = event["event_type"]
            if kind == "recommendation_created":
                states[event["recommendation_id"]] = "pending"
            elif kind in {
                "approved",
                "rejected",
                "revoked",
                "executed",
                "execution_failed",
                "outcome_observed",
                "invalidated",
            }:
                states[event["recommendation_id"]] = kind
        expected = (
            sorted(identity for identity, state in states.items() if state in _ACTIVE_STATES)
            if job_id is not None
            else []
        )
        _integrity(details["invalidated"] == expected)
        end = start + len(expected)
        _integrity(end <= len(workflow))
        for identity, event in zip(expected, workflow[start:end], strict=True):
            _integrity(
                event["event_type"] == "invalidated"
                and event["recommendation_id"] == identity
                and event["actor_id"] == journal["actor_id"]
                and event["actor_role"] == journal["actor_role"]
                and details.get("received_at", details.get("completed_at"))
                <= event["occurred_at"]
                <= journal["occurred_at"]
                and event["sequence"] not in invalidations
            )
            _integrity(
                json.loads(event["details"])
                == {
                    "job_id": job_id,
                    "fingerprint": rows[identity]["fingerprint"],
                    "reason": _REASON,
                }
            )
            invalidations[event["sequence"]] = cast(str, job_id)
        if new_seed is not None:
            assert birth is not None
            new_items = new_seed["recommendations"]
            _integrity(end + len(new_items) <= len(workflow))
            for item, event in zip(new_items, workflow[end : end + len(new_items)], strict=True):
                identity = item["id"]
                _integrity(
                    identity not in creations
                    and event["event_type"] == "recommendation_created"
                    and event["recommendation_id"] == identity
                    and event["actor_id"] == "runtime-worker"
                    and event["actor_role"] == "system"
                    and birth <= event["occurred_at"] <= journal["occurred_at"]
                    and rows[identity]["created_at"] == birth
                )
                creations[identity] = (cast(str, job_id), birth)
            end += len(new_items)
        last_boundary = end

    for sequence, journal in enumerate(raw["runtime_journal"], 1):
        details = json.loads(journal["details"])
        hashed = {key: value for key, value in journal.items() if key != "event_hash"}
        hashed["details"] = details
        now, operation = journal["occurred_at"], journal["operation"]
        _integrity(
            journal["sequence"] == sequence
            and journal["previous_hash"] == previous
            and digest(hashed) == journal["event_hash"]
            and canonical(details) == journal["details"]
            and math.isfinite(now)
            and now >= previous_time
        )
        locations = details["actor_locations"]
        _integrity(
            isinstance(locations, list)
            and all(isinstance(item, str) and item for item in locations)
            and locations == sorted(set(locations))
            and scopes_by_generation[current].issubset(locations)
            and isinstance(journal["actor_id"], str)
            and bool(journal["actor_id"])
        )
        if operation in {"submit", "broker_submit"}:
            _integrity(
                (
                    journal["actor_role"] == "planner"
                    if operation == "submit"
                    else journal["actor_role"] == "system"
                    and journal["actor_id"] == "runtime-event-consumer"
                )
                and set(details)
                == {
                    "actor_locations",
                    "received_at",
                    "event_sequence",
                    "policy",
                    "disposition",
                    "job",
                    "invalidated",
                    "workflow_start",
                }
            )
            received = details["received_at"]
            _integrity(
                type(received) in (int, float)
                and previous_time <= received <= now
                and details["event_sequence"] == event_count + 1
                and event_count < len(events)
            )
            row = events[event_count]
            event_count += 1
            _integrity(row["received_at"] == received)
            event = EventEnvelope.model_validate_json(row["payload"])
            policy = ReplanPolicy.model_validate_json(canonical(details["policy"]))
            _integrity(_policy_value(policy) == details["policy"])
            last = max(
                (job["completed_at"] for job in jobs.values() if job["state"] == "completed"),
                default=None,
            )
            material, disposition, eligible = _eligible(policy, event, received, last)
            _integrity(details["disposition"] == disposition)
            if material:
                source = event.payload["source_snapshot_sha256"]
                previous_material = next(
                    (
                        prior
                        for prior in reversed(events[: event_count - 1])
                        if prior["job_id"] is not None
                    ),
                    None,
                )
                _integrity(
                    source != generations[current]["metadata"]["source_snapshot_sha256"]
                    or (
                        previous_material is not None
                        and json.loads(previous_material["payload"])["payload"][
                            "source_snapshot_sha256"
                        ]
                        != source
                    )
                )
                queued = sorted(
                    (job for job in jobs.values() if job["state"] == "queued"),
                    key=lambda job: (job["created_at"], job["id"]),
                )
                if queued:
                    job = {**queued[0], "source_sha256": source, "eligible_at": eligible}
                else:
                    identity = row["job_id"]
                    _integrity(
                        isinstance(identity, str)
                        and _JOB.fullmatch(identity) is not None
                        and identity not in jobs
                    )
                    job = _job_new(identity, source, eligible, received, current)
                _integrity(details["job"] == job and row["job_id"] == job["id"])
                jobs[job["id"]] = job
            else:
                _integrity(row["job_id"] is None and details["job"] is None)
            boundary(details, journal, row["job_id"])
        elif operation == "claim":
            _integrity(
                journal["actor_id"] == "runtime-worker"
                and journal["actor_role"] == "system"
                and set(details) == {"actor_locations", "job", "lease_seconds"}
            )
            lease = details["lease_seconds"]
            _integrity(
                type(lease) is int
                and 10 <= lease <= 3600
                and not any(
                    job["state"] == "running" and job["lease_until"] > now for job in jobs.values()
                )
            )
            candidates = sorted(
                (
                    job
                    for job in jobs.values()
                    if (job["state"] == "queued" and job["eligible_at"] <= now)
                    or (job["state"] == "running" and job["lease_until"] <= now)
                ),
                key=lambda job: (job["created_at"], job["id"]),
            )
            _integrity(bool(candidates))
            old = candidates[0]
            token_hash = details["job"]["token"]
            _integrity(
                isinstance(token_hash, str)
                and _SHA.fullmatch(token_hash) is not None
                and token_hash != old["token"]
            )
            expected_job = {
                **old,
                "state": "running",
                "token": token_hash,
                "lease_until": now + lease,
                "attempts": old["attempts"] + 1,
            }
            _integrity(details["job"] == expected_job)
            jobs[old["id"]] = expected_job
        elif operation in {"fail", "activate"}:
            _integrity(
                journal["actor_id"] == "runtime-worker" and journal["actor_role"] == "system"
            )
            keys = {"actor_locations", "job_id", "token_sha256"} | (
                {"reason"}
                if operation == "fail"
                else {
                    "completed_at",
                    "result_sha256",
                    "previous_seed_hash",
                    "evidence_path",
                    "invalidated",
                    "workflow_start",
                }
            )
            _integrity(set(details) == keys and details["job_id"] in jobs)
            job = jobs[details["job_id"]]
            _integrity(
                job["state"] == "running"
                and job["token"] == details["token_sha256"]
                and now < job["lease_until"]
            )
            if operation == "fail":
                reason = details["reason"]
                _integrity(isinstance(reason, str) and _ERROR.fullmatch(reason) is not None)
                jobs[job["id"]] = {**job, "state": "failed", "error": reason, "token": None}
            else:
                instant, result = details["completed_at"], details["result_sha256"]
                latest = next(
                    (row for row in reversed(events[:event_count]) if row["job_id"] is not None),
                    None,
                )
                _integrity(
                    type(instant) in (int, float)
                    and previous_time <= instant <= now
                    and latest is not None
                    and latest["job_id"] == job["id"]
                    and json.loads(latest["payload"])["payload"]["source_snapshot_sha256"]
                    == job["source_sha256"]
                    and job["base_seed_hash"] == current
                    and details["previous_seed_hash"] == current
                    and result in generations
                    and result not in seen
                )
                new = generations[result]
                _integrity(
                    new["metadata"].get("runtime_job_id") == job["id"]
                    and new["metadata"]["source_snapshot_sha256"] == job["source_sha256"]
                    and _seed_locations(new).issubset(locations)
                )
                for item in new["recommendations"]:
                    _integrity(item["id"] == f"{job['id']}-{item['source_recommendation_id']}")
                path = f"evidence/jobs/{job['id']}/attempt-{job['attempts']}-{job['token'][:16]}"
                _integrity(details["evidence_path"] == path and paths[result] == path)
                _integrity(
                    current in archives
                    and archives[current]
                    == {
                        "seed_hash": current,
                        "payload": canonical(generations[current]),
                        "archived_at": instant,
                        "replacement_job_id": job["id"],
                        "evidence_path": paths[current],
                    }
                )
                boundary(details, journal, job["id"], new, instant)
                jobs[job["id"]] = {
                    **job,
                    "state": "completed",
                    "completed_at": instant,
                    "result_sha256": result,
                    "token": None,
                }
                scopes_by_generation[result] = scopes_by_generation[current] | _seed_locations(new)
                seen.add(result)
                current, completed_time = result, instant
        else:
            _integrity(False, "Unknown runtime journal operation")
        previous, previous_time = journal["event_hash"], now
    _integrity(event_count == len(events) and current == current_hash and seen == set(generations))
    _integrity(
        [_safe_job(row) for row in raw["runtime_jobs"]] == [jobs[key] for key in sorted(jobs)]
    )
    ingested = datetime.fromisoformat(metadata["runtime_ingested_at"])
    _integrity(
        ingested.tzinfo is not None and ingested.isoformat() == metadata["runtime_ingested_at"]
    )
    if completed_time is not None:
        _integrity(metadata["runtime_ingested_at"] == iso(completed_time))
    scope_rows = {row["session_id"]: row for row in raw["session_scopes"]}
    _integrity(set(scope_rows) == {row["id"] for row in raw["sessions"]})
    for row in scope_rows.values():
        _integrity(row["generation_seed_hash"] in scopes_by_generation)
        if row["scope_version"] == 3:
            _integrity(row["generation_seed_hash"] == initial)
            expected_scope = {
                item["location_id"] for item in generations[initial]["recommendations"]
            }
        else:
            _integrity(row["scope_version"] == 4)
            expected_scope = scopes_by_generation[row["generation_seed_hash"]]
        _integrity(row["locations"] == canonical(sorted(expected_scope)))

    def creation(row: dict[str, Any], event: dict[str, Any]) -> None:
        details = json.loads(event["details"])
        if owner_generation[row["id"]] == initial:
            _integrity(
                event["actor_id"] == row["creator"]
                and event["actor_role"] == "planner"
                and details
                == {"fingerprint": row["fingerprint"], "source": "connected runtime baseline"}
            )
        else:
            _integrity(row["id"] in creations)
            job_id, birth = creations[row["id"]]
            _integrity(
                row["created_at"] == birth
                and event["actor_id"] == "runtime-worker"
                and event["actor_role"] == "system"
                and details
                == {
                    "fingerprint": row["fingerprint"],
                    "source": "connected runtime baseline",
                    "job_id": job_id,
                }
            )

    def transition(
        row: dict[str, Any],
        event: dict[str, Any],
        state: dict[str, Any],
        deliveries: dict[str, Any],
        pending: set[str],
    ) -> bool:
        if event["event_type"] != "invalidated":
            return False
        _integrity(
            event["sequence"] in invalidations
            and state["state"] in _ACTIVE_STATES
            and row["id"] not in pending
        )
        state["state"] = "invalidated"
        if row["id"] in deliveries:
            deliveries[row["id"]]["state"] = "cancelled"
        return True

    _replay(raw, seed, _owners=owners, _creation=creation, _transition=transition)


def _journal(
    unit: PostgresWriteSession, operation: str, actor: Actor, now: float, details: dict[str, Any]
) -> None:
    previous = unit._query(
        "SELECT sequence,event_hash FROM {table} ORDER BY sequence DESC LIMIT 1",
        table="runtime_journal",
    ).fetchone()
    value = {
        "sequence": previous["sequence"] + 1 if previous else 1,
        "operation": operation,
        "actor_id": actor.actor_id,
        "actor_role": actor.role.value,
        "occurred_at": now,
        "details": {**details, "actor_locations": sorted(actor.locations)},
        "previous_hash": previous["event_hash"] if previous else ZERO_HASH,
    }
    unit._query(
        "INSERT INTO {table} VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            value["sequence"],
            operation,
            actor.actor_id,
            actor.role.value,
            now,
            canonical(value["details"]),
            value["previous_hash"],
            digest(value),
        ),
        table="runtime_journal",
    )


class PostgresRuntimeControl:
    """Guard-serialized admission and fenced compute ownership, without external I/O."""

    def __init__(self, store: PostgresRuntimeStore, *, policy: ReplanPolicy | None = None) -> None:
        self.store = store
        self.policy = policy or ReplanPolicy()

    @staticmethod
    def _role(actor: Actor, *, worker: bool = False, broker: bool = False) -> None:
        if worker and broker:
            raise ValueError("Worker and broker capabilities are distinct")
        identity = "runtime-worker" if worker else "runtime-event-consumer"
        if actor.role is not (ActorRole.SYSTEM if worker or broker else ActorRole.PLANNER) or (
            (worker or broker) and actor.actor_id != identity
        ):
            raise StoreError("role_denied", "This role cannot change runtime planning", 403)

    def _authorize(
        self, unit: PostgresReadSession, actor: Actor, *, worker: bool = False, broker: bool = False
    ) -> dict[str, Any]:
        self._role(actor, worker=worker, broker=broker)
        snapshot = unit.snapshot()
        self.store._full_scope(snapshot, actor)
        if snapshot["seed"] is None:
            raise StoreError("not_ready", "Initialize a connected runtime first", 503)
        return snapshot

    def _invalidate(self, unit: PostgresWriteSession, job_id: str, actor: Actor) -> list[str]:
        rows = unit._query(
            "SELECT * FROM {table} WHERE state IN ('pending','approved','execution_failed') "
            "ORDER BY id",
            table="recommendations",
        ).fetchall()
        for row in rows:
            unit._query(
                "UPDATE {table} SET state='invalidated' WHERE id=%s",
                (row["id"],),
                table="recommendations",
            )
            unit._query(
                "UPDATE {table} SET state='cancelled' WHERE recommendation_id=%s",
                (row["id"],),
                table="outbox",
            )
            self.store._event(
                unit,
                row["id"],
                "invalidated",
                actor.actor_id,
                actor.role.value,
                {"job_id": job_id, "fingerprint": row["fingerprint"], "reason": _REASON},
            )
        return [row["id"] for row in rows]

    def submit(self, event: EventEnvelope, actor: Actor) -> dict[str, Any]:
        return self._submit(event, actor, broker=False)

    def submit_broker(self, event: EventEnvelope, actor: Actor) -> dict[str, Any]:
        """Trusted consumer admission, never a human or HTTP capability.

        Returning proves durable event/job admission, not completed computation.
        Transport offsets may be acknowledged only after this transaction commits.
        """
        return self._submit(event, actor, broker=True)

    def _submit(self, event: EventEnvelope, actor: Actor, *, broker: bool) -> dict[str, Any]:
        self._role(actor, broker=broker)
        event = EventEnvelope.model_validate_json(event.model_dump_json())
        if (
            set(event.payload) != {"source_snapshot_sha256"}
            or not isinstance(event.payload["source_snapshot_sha256"], str)
            or not _SHA.fullmatch(event.payload["source_snapshot_sha256"])
        ):
            raise StoreError("invalid_event", "Bind exactly one source snapshot SHA-256", 422)
        payload = event.model_dump(mode="json")
        with self.store.write_transaction() as unit:
            snapshot = self._authorize(unit, actor, broker=broker)
            self.store._admission_gate(unit, event, broker=broker)
            now = _instant(self.store.clock)
            if max(event.ingested_at, event.occurred_at) > datetime.fromtimestamp(now, UTC):
                raise StoreError("future_event", "An event cannot be received in the future", 422)
            previous = unit._query(
                "SELECT * FROM {table} WHERE id=%s", (str(event.event_id),), table="runtime_events"
            ).fetchone()
            if previous:
                if previous["event_hash"] != digest(payload):
                    raise StoreError("event_conflict", "Event ID is bound to other content")
                return {"duplicate": True, "job_id": previous["job_id"], "invalidated": 0}
            last = unit._query(
                "SELECT MAX(completed_at) AS latest FROM {table} WHERE state='completed'",
                table="runtime_jobs",
            ).fetchone()["latest"]
            material, disposition, eligible = _eligible(self.policy, event, now, last)
            job_id, job, invalidated = None, None, []
            if material:
                source = event.payload["source_snapshot_sha256"]
                previous_material = next(
                    (
                        prior
                        for prior in reversed(snapshot["runtime_events"])
                        if prior["job_id"] is not None
                    ),
                    None,
                )
                reverts_admitted_source = previous_material is not None and (
                    json.loads(previous_material["payload"])["payload"]["source_snapshot_sha256"]
                    != source
                )
                if (
                    source == snapshot["seed"]["metadata"]["source_snapshot_sha256"]
                    and not reverts_admitted_source
                ):
                    raise StoreError(
                        "unchanged_source", "Material replanning requires changed source bytes"
                    )
                queued = unit._query(
                    "SELECT * FROM {table} WHERE state='queued' ORDER BY created_at,id LIMIT 1",
                    table="runtime_jobs",
                ).fetchone()
                if queued:
                    job_id = queued["id"]
                    unit._query(
                        "UPDATE {table} SET source_sha256=%s,eligible_at=%s WHERE id=%s",
                        (source, eligible, job_id),
                        table="runtime_jobs",
                    )
                else:
                    job_id = secrets.token_hex(16)
                    unit._query(
                        "INSERT INTO {table}(id,source_sha256,state,eligible_at,created_at,"
                        "base_seed_hash) VALUES (%s,%s,'queued',%s,%s,%s)",
                        (job_id, source, eligible, now, snapshot["seed_sha256"]),
                        table="runtime_jobs",
                    )
                invalidated = self._invalidate(unit, job_id, actor)
                job = unit._query(
                    "SELECT * FROM {table} WHERE id=%s", (job_id,), table="runtime_jobs"
                ).fetchone()
            sequence = len(snapshot["runtime_events"]) + 1
            unit._query(
                "INSERT INTO {table} VALUES (%s,%s,%s,%s,%s,%s)",
                (sequence, str(event.event_id), digest(payload), canonical(payload), job_id, now),
                table="runtime_events",
            )
            _journal(
                unit,
                "broker_submit" if broker else "submit",
                actor,
                _instant(self.store.clock),
                {
                    "received_at": now,
                    "event_sequence": sequence,
                    "policy": _policy_value(self.policy),
                    "disposition": disposition,
                    "job": _safe_job(job) if job else None,
                    "invalidated": invalidated,
                    "workflow_start": snapshot["event_count"],
                },
            )
            self.store._stage("runtime_event_written")
            return {
                "duplicate": False,
                "job_id": job_id,
                "invalidated": len(invalidated),
                "disposition": disposition,
            }

    def claim(self, actor: Actor, *, lease_seconds: int = 300) -> dict[str, Any] | None:
        self._role(actor, worker=True)
        if type(lease_seconds) is not int or not 10 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be an integer between 10 and 3600")
        with self.store.write_transaction() as unit:
            self._authorize(unit, actor, worker=True)
            now = _instant(self.store.clock)
            if unit._query(
                "SELECT 1 FROM {table} WHERE state='running' AND lease_until>%s",
                (now,),
                table="runtime_jobs",
            ).fetchone():
                return None
            old = unit._query(
                "SELECT * FROM {table} WHERE (state='queued' AND eligible_at<=%s) "
                "OR (state='running' AND lease_until<=%s) ORDER BY created_at,id LIMIT 1",
                (now, now),
                table="runtime_jobs",
            ).fetchone()
            if old is None:
                return None
            token = secrets.token_hex(32)
            unit._query(
                "UPDATE {table} SET state='running',token=%s,lease_until=%s,"
                "attempts=attempts+1 WHERE id=%s",
                (token, now + lease_seconds, old["id"]),
                table="runtime_jobs",
            )
            job = {
                **old,
                "state": "running",
                "token": token,
                "lease_until": now + lease_seconds,
                "attempts": old["attempts"] + 1,
            }
            _journal(
                unit, "claim", actor, now, {"job": _safe_job(job), "lease_seconds": lease_seconds}
            )
            self.store._stage("runtime_claim_written")
            return job

    def _claimed(self, unit: PostgresReadSession, job_id: str, token: str) -> dict[str, Any]:
        row = unit._query(
            "SELECT * FROM {table} WHERE id=%s", (job_id,), table="runtime_jobs"
        ).fetchone()
        if (
            row is None
            or row["state"] != "running"
            or not isinstance(token, str)
            or row["token"] != token
            or _instant(self.store.clock) >= row["lease_until"]
        ):
            raise StoreError("stale_worker", "Worker lease expired or was replaced")
        return cast(dict[str, Any], row)

    def _attempt(self, job: dict[str, Any], token: str) -> Path:
        return self.store._path(
            self.store.runtime_root
            / "evidence"
            / "jobs"
            / job["id"]
            / f"attempt-{job['attempts']}-{_hash_token(token)[:16]}"
        )

    def attempt_evidence_dir(self, job_id: str, token: str, actor: Actor) -> Path:
        self._role(actor, worker=True)
        with self.store.write_transaction() as unit:
            self._authorize(unit, actor, worker=True)
            return self._attempt(self._claimed(unit, job_id, token), token)

    def fail(self, job_id: str, token: str, actor: Actor, *, reason: str) -> None:
        self._role(actor, worker=True)
        if not isinstance(reason, str) or not _ERROR.fullmatch(reason):
            raise ValueError("reason must be a bounded error classification")
        with self.store.write_transaction() as unit:
            self._authorize(unit, actor, worker=True)
            job = self._claimed(unit, job_id, token)
            now = _instant(self.store.clock)
            if now >= job["lease_until"]:
                raise StoreError("stale_worker", "Worker lease expired")
            unit._query(
                "UPDATE {table} SET state='failed',error=%s,token=NULL WHERE id=%s",
                (reason, job_id),
                table="runtime_jobs",
            )
            _journal(
                unit,
                "fail",
                actor,
                now,
                {"job_id": job_id, "token_sha256": _hash_token(token), "reason": reason},
            )
            self.store._stage("runtime_failure_written")

    def activate(
        self, job_id: str, token: str, seed: dict[str, Any], actor: Actor, *, evidence_dir: Path
    ) -> dict[str, Any]:
        self._role(actor, worker=True)
        stable = _clone(seed)
        _producer(stable)
        with self.store.write_transaction() as unit:
            snapshot = self._authorize(unit, actor, worker=True)
            job = self._claimed(unit, job_id, token)
            expected = self._attempt(job, token)
            if self.store._path(evidence_dir, directory=True) != expected:
                raise StoreError(
                    "invalid_path", "Activation requires the exact live attempt evidence directory"
                )
            self.store._admit(stable, evidence_dir)
            if not _seed_locations(stable).issubset(actor.locations):
                raise StoreError(
                    "scope_denied", "Replacement includes an unauthorized location", 403
                )
            if stable["metadata"]["source_snapshot_sha256"] != job["source_sha256"]:
                raise StoreError(
                    "source_mismatch", "Computed plan does not match the queued source"
                )
            latest = next(
                (row for row in reversed(snapshot["runtime_events"]) if row["job_id"] is not None),
                None,
            )
            if (
                latest is None
                or latest["job_id"] != job_id
                or json.loads(latest["payload"])["payload"]["source_snapshot_sha256"]
                != job["source_sha256"]
                or job["base_seed_hash"] != snapshot["seed_sha256"]
            ):
                raise StoreError(
                    "superseded_job", "A newer source or generation superseded this plan"
                )
            # Artifact verification may be slow: authorize and recheck current
            # ownership after that I/O, still holding the workflow serialization guard.
            self._role(actor, worker=True)
            self.store._full_scope(snapshot, actor)
            self._claimed(unit, job_id, token)
            now = _instant(self.store.clock)
            if now >= job["lease_until"]:
                raise StoreError("stale_worker", "Worker lease expired during verification")
            replacement = _clone(stable)
            replacement["metadata"]["runtime_job_id"] = job_id
            for item in replacement["recommendations"]:
                item["source_recommendation_id"] = item["id"]
                item["id"] = f"{job_id}-{item['id']}"
            if "seed_hash" in replacement:
                replacement["metadata"]["runtime_artifact_seed_hash"] = replacement.pop("seed_hash")
            old = snapshot["seed"]
            previous_path = unit._query(
                "SELECT value FROM {table} WHERE key='runtime_evidence_path'", table="metadata"
            ).fetchone()["value"]
            unit._query(
                "INSERT INTO {table} VALUES (%s,%s,%s,%s,%s)",
                (snapshot["seed_sha256"], canonical(old), now, job_id, previous_path),
                table="runtime_generations",
            )
            self.store._stage("runtime_generation_archived")
            invalidated = self._invalidate(unit, job_id, actor)
            for item in replacement["recommendations"]:
                payload = {
                    key: value for key, value in item.items() if key not in {"state", "status"}
                }
                payload.update(
                    created_at=iso(now), valid_until=iso(now + RECOMMENDATION_TTL_SECONDS)
                )
                fingerprint = digest(payload)
                unit._query(
                    "INSERT INTO {table}(id,payload,fingerprint,location_id,creator,state,"
                    "created_at,valid_until) VALUES (%s,%s,%s,%s,%s,'pending',%s,%s)",
                    (
                        payload["id"],
                        canonical(payload),
                        fingerprint,
                        payload["location_id"],
                        payload.get("created_by", "planner.r4a"),
                        now,
                        now + RECOMMENDATION_TTL_SECONDS,
                    ),
                    table="recommendations",
                )
                self.store._event(
                    unit,
                    payload["id"],
                    "recommendation_created",
                    actor.actor_id,
                    actor.role.value,
                    {
                        "fingerprint": fingerprint,
                        "source": "connected runtime baseline",
                        "job_id": job_id,
                    },
                )
            self.store._stage("runtime_projection_written")
            result = digest(replacement)
            evidence_path = self.store._relative(evidence_dir)
            for key, value in (
                ("seed", canonical(replacement)),
                ("seed_hash", result),
                ("runtime_evidence_path", evidence_path),
                ("runtime_ingested_at", iso(now)),
            ):
                unit._query(
                    "UPDATE {table} SET value=%s WHERE key=%s", (value, key), table="metadata"
                )
            unit._query(
                "UPDATE {table} SET state='completed',completed_at=%s,result_sha256=%s,"
                "token=NULL WHERE id=%s",
                (now, result, job_id),
                table="runtime_jobs",
            )
            _journal(
                unit,
                "activate",
                actor,
                _instant(self.store.clock),
                {
                    "job_id": job_id,
                    "token_sha256": _hash_token(token),
                    "completed_at": now,
                    "result_sha256": result,
                    "previous_seed_hash": snapshot["seed_sha256"],
                    "evidence_path": evidence_path,
                    "invalidated": invalidated,
                    "workflow_start": snapshot["event_count"],
                },
            )
            cast(_RuntimeWrite, unit)._activation_deadline = job["lease_until"]
            self.store._stage("runtime_activation_written")
            return {
                "job_id": job_id,
                "seed_sha256": result,
                "recommendations": len(replacement["recommendations"]),
                "human_approval_required": True,
            }

    def snapshot(self, actor: Actor) -> dict[str, Any]:
        self.store._permission(actor, Permission.VIEW_AUDIT)
        with self.store.read_snapshot() as unit:
            snapshot = unit.snapshot()
            self.store._full_scope(snapshot, actor)
            jobs = snapshot["runtime_jobs"]
            sequences = {
                job["id"]: max(
                    (
                        event["sequence"]
                        for event in snapshot["runtime_events"]
                        if event["job_id"] == job["id"]
                    ),
                    default=0,
                )
                for job in jobs
            }
            completed = max(
                (sequences[job["id"]] for job in jobs if job["state"] == "completed"), default=0
            )
            now = _instant(self.store.clock)
            operational = {
                state: sum(job["state"] == state for job in jobs)
                for state in ("queued", "running", "completed")
            }
            operational.update(
                lease_expired=sum(
                    job["state"] == "running" and job["lease_until"] <= now for job in jobs
                ),
                failed_unresolved=sum(
                    job["state"] == "failed" and sequences[job["id"]] > completed for job in jobs
                ),
                failed_historical=sum(
                    job["state"] == "failed" and sequences[job["id"]] <= completed for job in jobs
                ),
            )
            return {
                "jobs": sorted(jobs, key=lambda row: (row["created_at"], row["id"]), reverse=True)[
                    :50
                ],
                "operational": operational,
                "event_count": len(snapshot["runtime_events"]),
                "archived_generations": len(snapshot["runtime_generations"]),
                "adapter": "postgresql",
                "checked_at": iso(now),
            }
