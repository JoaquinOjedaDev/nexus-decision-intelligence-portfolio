"""Version-3 PostgreSQL workflow for one verified initial planning generation.

This explicit facade is not wired into the API, runtime controller or broker.
Human policy and canonical business events match the SQLite portfolio, while
read-time replay additionally reconciles every effect and delivery projection.
"""

from __future__ import annotations

import hashlib
import json
import math
import secrets
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar, cast

from nexus.contracts import ActorRole
from nexus.policies.authorization import Permission
from nexus.portfolio.auth import (
    SESSION_TTL_SECONDS,
    Actor,
    SessionCodec,
    random_token,
    resolve_persona,
)
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
from nexus.portfolio.store import (
    APPROVAL_TTL_SECONDS,
    DEMO_MATURITY_SECONDS,
    RECOMMENDATION_TTL_SECONDS,
    PortfolioStore,
    StoreError,
    canonical,
    digest,
    iso,
)

_EXTRA_COLUMNS = {
    "outbox": {
        "recommendation_id": "text",
        "operation_id": "text",
        "state": "text",
        "attempts": "integer",
        "last_error": "text",
        "fingerprint": "text",
    },
    "effects": {
        "recommendation_id": "text",
        "external_reference": "text",
        "fingerprint": "text",
        "payload": "text",
        "created_at": "double precision",
    },
    "idempotency": {"key": "text", "recommendation_id": "text", "fingerprint": "text"},
    "sessions": {
        "id": "text",
        "persona": "text",
        "csrf": "text",
        "expires_at": "double precision",
        "revoked": "integer",
    },
    "session_tokens": {
        "session_id": "text",
        "subject": "text",
        "token_id": "text",
        "issued_at": "bigint",
        "not_before": "bigint",
    },
}
_TABLE_ORDER = {
    "recommendations": "id",
    "events": "sequence",
    "outbox": "recommendation_id",
    "effects": "recommendation_id",
    "idempotency": "key",
    "sessions": "id",
    "session_tokens": "session_id",
    "workflow_guard": "singleton",
}
_MUTABLE = (
    "state",
    "approval_actor",
    "approval_fingerprint",
    "approval_expires_at",
    "execution_ref",
    "executed_at",
    "outcome_due_at",
    "outcome_json",
)
_LOCAL_FIELDS = {
    "id",
    "sku",
    "sku_id",
    "location_id",
    "supplier_id",
    "recommended_quantity",
    "unit_cost",
    "expected_cost",
    "created_by",
    "created_at",
    "valid_until",
}


def _integrity(
    condition: bool, message: str = "Workflow state disagrees with verified history"
) -> None:
    if not condition:
        raise StoreError("integrity_error", message)


def _raw(unit: PostgresReadSession) -> dict[str, Any]:
    value = {
        table: unit._query("SELECT * FROM {table} ORDER BY " + order, table=table).fetchall()
        for table, order in _TABLE_ORDER.items()
    }
    value["metadata"] = {
        row["key"]: row["value"]
        for row in unit._query("SELECT * FROM {table}", table="metadata").fetchall()
    }
    return value


def _state_hash(raw: dict[str, Any]) -> str:
    metadata = {
        key: value for key, value in raw["metadata"].items() if key != "workflow_state_hash"
    }
    # Never include the secret itself in exported diagnostics or public snapshots.
    metadata["session_key"] = hashlib.sha256(metadata["session_key"].encode()).hexdigest()
    return digest({**raw, "metadata": metadata})


def _effect(row: dict[str, Any], payload: dict[str, Any], instant: float) -> dict[str, Any]:
    operation = digest({"id": row["id"], "fingerprint": row["fingerprint"]})
    return {
        "recommendation_id": row["id"],
        "external_reference": "MOCK-PO-" + operation[:16].upper(),
        "fingerprint": row["fingerprint"],
        "created_at": instant,
        "payload": canonical(
            {
                "sku_id": payload.get("sku_id", payload.get("sku")),
                "location_id": row["location_id"],
                "quantity": payload["recommended_quantity"],
                "simulated": True,
            }
        ),
    }


def _replay(
    raw: dict[str, Any],
    seed: dict[str, Any],
    *,
    _owners: dict[str, dict[str, Any]] | None = None,
    _creation: Callable[[dict[str, Any], dict[str, Any]], None] | None = None,
    _transition: Callable[
        [dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], set[str]], bool
    ]
    | None = None,
) -> None:
    rows = {row["id"]: row for row in raw["recommendations"]}
    owners = (
        _owners if _owners is not None else {item["id"]: item for item in seed["recommendations"]}
    )
    _integrity(set(owners) == set(rows))
    if _owners is None:
        _integrity(len(owners) == len(seed["recommendations"]))
    projection: dict[str, dict[str, Any]] = {key: {name: None for name in _MUTABLE} for key in rows}
    deliveries: dict[str, dict[str, Any]] = {}
    effects: dict[str, dict[str, Any]] = {}
    created: set[str] = set()
    pending_requests: set[str] = set()
    payloads = {}
    for identity, row in rows.items():
        payload = {
            key: value for key, value in owners[identity].items() if key not in {"state", "status"}
        }
        payload.update(created_at=iso(row["created_at"]), valid_until=iso(row["valid_until"]))
        _integrity(
            canonical(payload) == row["payload"]
            and digest(payload) == row["fingerprint"]
            and row["creator"] == payload.get("created_by", "planner.r4a")
            and row["location_id"] == payload["location_id"]
            and row["valid_until"] == row["created_at"] + RECOMMENDATION_TTL_SECONDS
        )
        payloads[identity] = payload
    previous = ZERO_HASH
    previous_time = float("-inf")
    for sequence, event in enumerate(raw["events"], 1):
        value = dict(event)
        event_hash = value.pop("event_hash")
        details = json.loads(value["details"])
        value["details"] = details
        identity, kind, instant = (
            event["recommendation_id"],
            event["event_type"],
            event["occurred_at"],
        )
        _integrity(
            identity in rows
            and event["sequence"] == sequence
            and event["previous_hash"] == previous
            and digest(value) == event_hash
            and canonical(details) == event["details"]
            and math.isfinite(instant)
            and instant >= previous_time
        )
        previous, previous_time = event_hash, instant
        row, state, payload = rows[identity], projection[identity], payloads[identity]
        role, actor = event["actor_role"], event["actor_id"]
        _integrity(isinstance(actor, str) and bool(actor))
        operation = digest({"id": identity, "fingerprint": row["fingerprint"]})
        if kind == "recommendation_created":
            _integrity(identity not in created and instant >= row["created_at"])
            if _creation is not None:
                _creation(row, event)
                created.add(identity)
                state["state"] = "pending"
                continue
            source = (
                "connected runtime baseline"
                if seed.get("metadata", {}).get("pipeline_mode") == "connected_runtime_baseline"
                else "frozen synthetic experiment"
            )
            _integrity(
                identity not in created
                and actor == row["creator"]
                and role == "planner"
                and instant >= row["created_at"]
                and details == {"fingerprint": row["fingerprint"], "source": source}
            )
            created.add(identity)
            state["state"] = "pending"
            continue
        _integrity(identity in created)
        if _transition is not None and _transition(row, event, state, deliveries, pending_requests):
            continue
        if kind.startswith("foundation."):
            continue
        if kind in {"approved", "rejected", "revoked"}:
            legal = {"approved", "execution_failed"} if kind == "revoked" else {"pending"}
            keys = {"fingerprint", "note"} | (
                {"approval_expires_at"} if kind == "approved" else set()
            )
            _integrity(
                role == "approver"
                and actor != row["creator"]
                and state["state"] in legal
                and identity not in pending_requests
                and set(details) == keys
                and details["fingerprint"] == row["fingerprint"]
                and isinstance(details["note"], str)
            )
            if kind != "revoked":
                _integrity(instant < row["valid_until"])
            if kind == "approved":
                expiry = details["approval_expires_at"]
                _integrity(
                    type(expiry) in (int, float)
                    and math.isfinite(expiry)
                    and instant < expiry <= min(row["valid_until"], instant + APPROVAL_TTL_SECONDS)
                )
                state.update(
                    approval_actor=actor,
                    approval_fingerprint=row["fingerprint"],
                    approval_expires_at=expiry,
                )
            if kind == "revoked" and identity in deliveries:
                deliveries[identity]["state"] = "cancelled"
            state["state"] = kind
        elif kind == "execution_requested":
            _integrity(
                role == "approver"
                and state["state"] in {"approved", "execution_failed"}
                and identity not in pending_requests
                and state["approval_actor"] != row["creator"]
                and state["approval_fingerprint"] == row["fingerprint"]
                and instant < state["approval_expires_at"]
                and instant < row["valid_until"]
                and details == {"operation_id": operation, "fingerprint": row["fingerprint"]}
            )
            attempts = deliveries.get(identity, {}).get("attempts", 0) + 1
            deliveries[identity] = {
                "recommendation_id": identity,
                "operation_id": operation,
                "state": "pending",
                "attempts": attempts,
                "last_error": None,
                "fingerprint": row["fingerprint"],
            }
            pending_requests.add(identity)
        elif kind == "execution_failed":
            _integrity(
                identity in pending_requests
                and actor == "mock-erp-adapter"
                and role == "system"
                and details
                == {
                    "reason": "Simulated timeout before effect; safe to retry",
                    "effect_created": False,
                }
                and identity not in effects
            )
            pending_requests.remove(identity)
            state["state"] = "execution_failed"
            deliveries[identity].update(state="failed", last_error="simulated ERP timeout")
        elif kind == "executed":
            _integrity(
                identity in pending_requests
                and actor == "mock-erp-adapter"
                and role == "system"
                and identity not in effects
                and set(details)
                == {
                    "external_reference",
                    "fingerprint",
                    "simulated",
                    "executed_at",
                    "outcome_due_at",
                }
            )
            executed = details["executed_at"]
            _integrity(
                type(executed) in (int, float)
                and math.isfinite(executed)
                and row["created_at"] <= executed <= instant
                and executed < state["approval_expires_at"]
                and executed < row["valid_until"]
            )
            effect = _effect(row, payload, executed)
            due = executed + DEMO_MATURITY_SECONDS
            _integrity(
                details
                == {
                    "external_reference": effect["external_reference"],
                    "fingerprint": row["fingerprint"],
                    "simulated": True,
                    "executed_at": executed,
                    "outcome_due_at": due,
                }
            )
            pending_requests.remove(identity)
            effects[identity] = effect
            deliveries[identity].update(state="delivered", last_error=None)
            state.update(
                state="executed",
                execution_ref=effect["external_reference"],
                executed_at=executed,
                outcome_due_at=due,
            )
        elif kind == "outcome_observed":
            recorded = datetime.fromisoformat(details["recorded_at"])
            expected = {
                "received_quantity": payload["recommended_quantity"],
                "observed_demand": payload.get("observed_demand"),
                "observation_date": payload.get("outcome_observation_date"),
                "provenance": payload.get("outcome_provenance", "synthetic_scenario_replay"),
                "scenario_seed": payload.get("outcome_scenario_seed"),
                "execution_ref": state["execution_ref"],
                "recorded_at": details["recorded_at"],
                "maturity_seconds": DEMO_MATURITY_SECONDS,
                "simulated": True,
                "causal_business_impact_claim": False,
                "note": details["note"],
            }
            _integrity(
                role == "planner"
                and state["state"] == "executed"
                # The public ISO receipt has microsecond precision; compare its
                # bounds in that same representation, not against a finer float.
                # observe() separately enforces the raw persisted maturity time.
                and datetime.fromisoformat(iso(state["outcome_due_at"]))
                <= recorded
                <= datetime.fromisoformat(iso(instant))
                and recorded.isoformat() == details["recorded_at"]
                and details == expected
                and isinstance(details["note"], str)
            )
            state.update(state="outcome_observed", outcome_json=canonical(details))
        else:
            _integrity(False, "Unknown workflow event")
    _integrity(created == set(rows) and not pending_requests)
    _integrity(
        raw["workflow_guard"]
        == [{"singleton": 1, "event_count": len(raw["events"]), "audit_head": previous}]
    )
    _integrity(raw["metadata"]["audit_head"] == previous)
    for identity, row in rows.items():
        _integrity({name: row[name] for name in _MUTABLE} == projection[identity])
    _integrity(raw["effects"] == [effects[key] for key in sorted(effects)])
    _integrity(raw["outbox"] == [deliveries[key] for key in sorted(deliveries)])
    for request in raw["idempotency"]:
        identity = request["recommendation_id"]
        _integrity(
            identity in deliveries
            and 0 < len(request["key"]) <= 128
            and request["fingerprint"] == rows[identity]["fingerprint"]
        )
    _integrity(set(deliveries) == {item["recommendation_id"] for item in raw["idempotency"]})


def _verified_snapshot(
    unit: PostgresReadSession,
    *,
    _raw_value: dict[str, Any] | None = None,
    _metadata_extra: frozenset[str] = frozenset(),
    _verify: Callable[[dict[str, Any], dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    raw = _raw_value if _raw_value is not None else _raw(unit)
    metadata = raw["metadata"]
    _integrity(
        len(metadata.get("session_key", "")) == 64
        and all(char in "0123456789abcdef" for char in metadata.get("session_key", ""))
    )
    _integrity(
        metadata.get("workflow_state_hash") == _state_hash(raw), "Workflow checksum mismatch"
    )
    tokens = {row["session_id"]: row for row in raw["session_tokens"]}
    _integrity(set(tokens) == {row["id"] for row in raw["sessions"]})
    for session in raw["sessions"]:
        token = tokens[session["id"]]
        actor = resolve_persona(session["persona"], ())
        _integrity(
            session["revoked"] in (0, 1)
            and token["subject"] == actor.actor_id
            and token["not_before"] == token["issued_at"]
            and session["expires_at"] == token["issued_at"] + SESSION_TTL_SECONDS
            and all(
                isinstance(item, str) and len(item) == 64
                for item in (session["id"], session["csrf"], token["token_id"])
            )
        )
    if "seed" not in metadata:
        _integrity(
            set(metadata) == {"session_key", "workflow_state_hash"} | _metadata_extra
            and all(not raw[table] for table in _TABLE_ORDER if table != "workflow_guard")
            and raw["workflow_guard"]
            == [{"singleton": 1, "event_count": 0, "audit_head": ZERO_HASH}]
        )
        return {
            "seed": None,
            "seed_sha256": None,
            "recommendations": [],
            "events": [],
            "outbox": [],
            "effects": [],
            "audit_head": ZERO_HASH,
            "event_count": 0,
            "idempotency_count": 0,
        }
    _integrity(
        set(metadata)
        == {"session_key", "workflow_state_hash", "seed", "seed_hash", "audit_head"}
        | _metadata_extra
    )
    seed = json.loads(metadata["seed"])
    _integrity(canonical(seed) == metadata["seed"] and digest(seed) == metadata["seed_hash"])
    if _verify is None:
        unit._backend._verify_artifacts(seed)
        _replay(raw, seed)
    else:
        _verify(raw, seed)
    return {
        "seed": seed,
        "seed_sha256": metadata["seed_hash"],
        "recommendations": raw["recommendations"],
        "events": raw["events"],
        "outbox": raw["outbox"],
        "effects": raw["effects"],
        "audit_head": metadata["audit_head"],
        "event_count": len(raw["events"]),
        "idempotency_count": len(raw["idempotency"]),
    }


class _WorkflowRead(PostgresReadSession):
    def _snapshot(self) -> dict[str, Any]:
        return _verified_snapshot(self)


class _WorkflowWrite(PostgresWriteSession):
    def _snapshot(self) -> dict[str, Any]:
        return _verified_snapshot(self)


class PostgresWorkflowStore(PostgresBackend):
    """One-generation, authenticated-policy workflow; no API or scheduler selector yet."""

    _schema_tables = PostgresBackend._schema_tables | set(_EXTRA_COLUMNS)
    _schema_columns: ClassVar[dict[str, dict[str, str]]] = {
        **PostgresBackend._schema_columns,
        **_EXTRA_COLUMNS,
    }
    _nullable_columns = PostgresBackend._nullable_columns | {("outbox", "last_error")}
    _uniques: ClassVar[dict[tuple[str, str], list[int]]] = {
        ("outbox", "outbox_operation_id_key"): [2],
        ("effects", "effects_external_reference_key"): [2],
        ("session_tokens", "session_tokens_token_id_key"): [3],
    }
    _foreign_keys: ClassVar[dict[tuple[str, str], tuple[list[int], str, list[int]]]] = {
        **PostgresBackend._foreign_keys,
        **{
            (table, table + "_recommendation_id_fkey"): ([1], "recommendations", [1])
            for table in ("outbox", "effects")
        },
        ("idempotency", "idempotency_recommendation_id_fkey"): ([2], "recommendations", [1]),
        ("session_tokens", "session_tokens_session_id_fkey"): ([1], "sessions", [1]),
    }
    _history_triggers: ClassVar[dict[str, str]] = {
        **PostgresBackend._history_triggers,
        "effects": "immutable_effect_history",
        "idempotency": "immutable_idempotency_history",
        "session_tokens": "immutable_session_token_history",
    }
    _forbidden_grants: ClassVar[dict[str, str]] = {
        **PostgresBackend._forbidden_grants,
        **{
            table: "DELETE,TRUNCATE,REFERENCES,TRIGGER"
            for table in ("recommendations", "outbox", "sessions")
        },
        **{
            table: "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER"
            for table in ("effects", "idempotency", "session_tokens")
        },
    }
    _permission = staticmethod(PortfolioStore._permission)

    def __init__(
        self,
        config: PostgresConfig,
        *,
        clock: Callable[[], float] = time.time,
        _failure_hook: Callable[[str], None] | None = None,
    ) -> None:
        super().__init__(config, clock=clock)
        self._failure_hook = _failure_hook

    @staticmethod
    def _migration_bytes(path: Path | None = None) -> tuple[str, str]:
        if path is not None:
            return PostgresBackend._migration_bytes(path)
        try:
            raw = read_migration_sql(3)
            return raw.decode("utf-8"), hashlib.sha256(raw).hexdigest()
        except (ValueError, OSError, UnicodeError):
            raise PostgresBackendError(
                "migration_conflict", "Workflow migration artifact unavailable"
            ) from None

    def _verify_migration_history(self, db: Any, expected_hash: str, application_role: str) -> None:
        rows = self._query(
            db, "SELECT * FROM {table} ORDER BY version", table="schema_migrations"
        ).fetchall()
        hashes = {2: PostgresBackend._migration_bytes()[1], 3: expected_hash}
        if (
            len(rows) != 2
            or [row["version"] for row in rows] != [2, 3]
            or any(
                row["migration_sha256"] != hashes[row["version"]]
                or row["application_role"] != application_role
                or not math.isfinite(row["applied_at"])
                for row in rows
            )
        ):
            raise PostgresBackendError("migration_conflict", "Workflow migration history conflicts")

    def _check_application_role(self, db: Any, role: str) -> None:
        super()._check_application_role(db, role)
        for column in ("id", "persona", "csrf", "expires_at"):
            result = db.execute(
                "SELECT has_column_privilege(%s,%s,%s,'UPDATE') AS edit",
                (role, self._table("sessions").as_string(db), column),
            ).fetchone()
            if result["edit"]:
                raise PostgresBackendError(
                    "privilege_denied", "Session identity columns must be immutable"
                )

    def _new_session(self, db: Any, *, write: bool) -> PostgresReadSession:
        return _WorkflowWrite(self, db) if write else _WorkflowRead(self, db)

    def _raw_state(self, unit: PostgresReadSession) -> dict[str, Any]:
        return _raw(unit)

    def _seal(self, unit: PostgresReadSession) -> None:
        raw = self._raw_state(unit)
        unit._query(
            "INSERT INTO {table} VALUES ('workflow_state_hash',%s) "
            "ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
            (_state_hash(raw),),
            table="metadata",
        )

    def _finish_write(self, session: PostgresReadSession) -> None:
        self._seal(session)
        session.snapshot()

    def migrate(self, *, migration_dsn: str, migration_path: Path | None = None) -> dict[str, Any]:
        application, migration = _dsn(self.config.dsn), _dsn(migration_dsn)
        if application["user"] == migration["user"] or any(
            application[key] != migration[key] for key in ("hostaddr", "port", "dbname")
        ):
            raise PostgresBackendError(
                "configuration_error",
                "Separate bootstrap identity on the same database is required",
            )
        templates = {
            2: PostgresBackend._migration_bytes(),
            3: self._migration_bytes(migration_path),
        }
        sql = _driver().sql
        lock = int.from_bytes(
            hashlib.sha256(self.config.schema.encode()).digest()[:8], "big", signed=True
        )
        foundation = PostgresBackend(self.config, clock=self.clock)
        with self._connection(migration) as db, db.transaction():
            db.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
            owner = db.execute(
                "SELECT pg_get_userbyid(nspowner) AS owner FROM pg_catalog.pg_namespace "
                "WHERE nspname=%s",
                (self.config.schema,),
            ).fetchone()
            if owner is None:
                db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.config.schema)))
                db.execute(
                    sql.SQL(templates[2][0]).format(
                        schema=sql.Identifier(self.config.schema),
                        application_role=sql.Identifier(application["user"]),
                    )
                )
                self._query(
                    db,
                    "INSERT INTO {table} VALUES (2,%s,%s,%s)",
                    (templates[2][1], float(self.clock()), application["user"]),
                    table="schema_migrations",
                )
            elif owner["owner"] != migration["user"]:
                raise PostgresBackendError("migration_conflict", "Schema bootstrap owner differs")
            # Refuse unrecorded tables before selecting a possibly absent ledger.
            ledger = db.execute(
                "SELECT to_regclass(%s) AS relation",
                (self._table("schema_migrations").as_string(db),),
            ).fetchone()
            if ledger["relation"] is None:
                raise PostgresBackendError(
                    "migration_conflict", "Existing schema has no migration history"
                )
            versions = self._query(
                db, "SELECT version FROM {table} ORDER BY version", table="schema_migrations"
            ).fetchall()
            upgraded = [row["version"] for row in versions] == [2]
            if upgraded:
                foundation._verify_schema(db, templates[2][1], application["user"])
                foundation._check_application_role(db, application["user"])
                self._query(
                    db, "SELECT * FROM {table} WHERE singleton=1 FOR UPDATE", table="workflow_guard"
                )
                PostgresReadSession(foundation, db).snapshot()
                db.execute(
                    sql.SQL(templates[3][0]).format(
                        schema=sql.Identifier(self.config.schema),
                        application_role=sql.Identifier(application["user"]),
                    )
                )
                self._query(
                    db,
                    "INSERT INTO {table} VALUES (3,%s,%s,%s)",
                    (templates[3][1], float(self.clock()), application["user"]),
                    table="schema_migrations",
                )
                self._query(
                    db,
                    "INSERT INTO {table} VALUES ('session_key',%s)",
                    (secrets.token_hex(32),),
                    table="metadata",
                )
                self._seal(_WorkflowWrite(self, db))
            self._verify_schema(db, templates[3][1], application["user"])
            self._check_application_role(db, application["user"])
            self._query(
                db, "SELECT * FROM {table} WHERE singleton=1 FOR UPDATE", table="workflow_guard"
            )
            _WorkflowRead(self, db).snapshot()
            return {
                "version": 3,
                "migration_sha256": templates[3][1],
                "schema": self.config.schema,
                "applied": upgraded,
                "server_version": db.execute("SHOW server_version").fetchone()["server_version"],
            }

    def _stage(self, name: str) -> None:
        if self._failure_hook is not None:
            self._failure_hook(name)

    def _event(
        self,
        unit: PostgresWriteSession,
        recommendation_id: str,
        event_type: str,
        actor_id: str,
        actor_role: str,
        details: dict[str, Any],
    ) -> None:
        unit._append_event(recommendation_id, event_type, actor_id, actor_role, details)
        self._stage("audit_written")

    @staticmethod
    def _locations(snapshot: dict[str, Any]) -> tuple[str, ...]:
        return tuple(sorted({row["location_id"] for row in snapshot["recommendations"]}))

    def locations(self) -> tuple[str, ...]:
        with self.read_snapshot() as unit:
            return self._locations(unit.snapshot())

    @staticmethod
    def _full_scope(snapshot: dict[str, Any], actor: Actor) -> None:
        if not set(PostgresWorkflowStore._locations(snapshot)).issubset(actor.locations):
            raise StoreError(
                "scope_denied", "Shared plan context requires every plan location", 403
            )

    @staticmethod
    def _row(snapshot: dict[str, Any], recommendation_id: str, actor: Actor) -> dict[str, Any]:
        row = next(
            (item for item in snapshot["recommendations"] if item["id"] == recommendation_id), None
        )
        if row is None:
            raise StoreError("not_found", "Recommendation was not found", 404)
        PortfolioStore._scope(row, actor)
        return cast(dict[str, Any], row)

    def _public(
        self, row: dict[str, Any], snapshot: dict[str, Any], actor: Actor
    ) -> dict[str, Any]:
        value = json.loads(row["payload"])
        partial = not set(self._locations(snapshot)).issubset(actor.locations)
        if partial:
            value = {key: item for key, item in value.items() if key in _LOCAL_FIELDS}
            value["context_redacted"] = True
        now = self.clock()
        value.update(
            fingerprint=row["fingerprint"],
            status=row["state"],
            state=row["state"],
            approved_by=row["approval_actor"],
            approval_expires_at=iso(row["approval_expires_at"])
            if row["approval_expires_at"]
            else None,
            approval_renewable=False,
            execution_ref=row["execution_ref"],
            outcome_due_at=iso(row["outcome_due_at"]) if row["outcome_due_at"] else None,
            outcome=json.loads(row["outcome_json"]) if row["outcome_json"] else None,
            expired=now >= row["valid_until"],
            outcome_ready=bool(
                row["state"] == "executed"
                and row["outcome_due_at"] is not None
                and now >= row["outcome_due_at"]
            ),
        )
        return cast(dict[str, Any], value)

    @staticmethod
    def _events(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            {**row, "details": json.loads(row["details"]), "occurred_at": iso(row["occurred_at"])}
            for row in snapshot["events"]
        ]

    def recommendations(self, actor: Actor) -> list[dict[str, Any]]:
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.read_snapshot() as unit:
            snapshot = unit.snapshot()
            return [
                self._public(row, snapshot, actor)
                for row in snapshot["recommendations"]
                if row["location_id"] in actor.locations
            ]

    def recommendation(self, recommendation_id: str, actor: Actor) -> dict[str, Any]:
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.read_snapshot() as unit:
            snapshot = unit.snapshot()
            value = self._public(self._row(snapshot, recommendation_id, actor), snapshot, actor)
            value["events"] = [
                event
                for event in self._events(snapshot)
                if event["recommendation_id"] == recommendation_id
            ]
            return value

    def overview_snapshot(self, actor: Actor) -> dict[str, Any]:
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.read_snapshot() as unit:
            snapshot = unit.snapshot()
            self._full_scope(snapshot, actor)
            seed = unit.seed()
            records = [
                {
                    **self._public(row, snapshot, actor),
                    "generation_seed_sha256": snapshot["seed_sha256"],
                    "current_generation": True,
                }
                for row in snapshot["recommendations"]
            ]
            return {
                **seed,
                "recommendations": records,
                "current_recommendations": records,
                "generation_seed_sha256": snapshot["seed_sha256"],
                "checked_at": iso(self.clock()),
            }

    def recommendation_context(self, recommendation_id: str, actor: Actor) -> dict[str, Any]:
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.read_snapshot() as unit:
            snapshot = unit.snapshot()
            row = self._row(snapshot, recommendation_id, actor)
            self._full_scope(snapshot, actor)
            seed = unit.seed()
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
                    "seed_sha256": snapshot["seed_sha256"],
                    "is_current": True,
                    "metadata": seed.get("metadata", {}),
                },
            }

    def audit(self, actor: Actor) -> list[dict[str, Any]]:
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.read_snapshot() as unit:
            snapshot = unit.snapshot()
            allowed = {
                row["id"]
                for row in snapshot["recommendations"]
                if row["location_id"] in actor.locations
            }
            return [
                event for event in self._events(snapshot) if event["recommendation_id"] in allowed
            ]

    def metrics_snapshot(self, actor: Actor) -> dict[str, Any]:
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.read_snapshot() as unit:
            snapshot = unit.snapshot()
            self._full_scope(snapshot, actor)
            states: dict[str, int] = {}
            outbox: dict[str, int] = {}
            for row in snapshot["recommendations"]:
                states[row["state"]] = states.get(row["state"], 0) + 1
            for row in snapshot["outbox"]:
                outbox[row["state"]] = outbox.get(row["state"], 0) + 1
            return {
                "states": states,
                "outbox": outbox,
                "effect_count": len(snapshot["effects"]),
                "event_count": snapshot["event_count"],
                "retry_count": sum(max(row["attempts"] - 1, 0) for row in snapshot["outbox"]),
                "database": "postgresql",
                "persistence": "local PostgreSQL transaction",
                "checked_at": iso(self.clock()),
            }

    def create_session(self, persona: str) -> tuple[str, dict[str, Any]]:
        resolve_persona(persona, ())  # Invalid fixture personas fail before database access.
        with self.write_transaction() as unit:
            unit.seed()
            actor = resolve_persona(persona, self._locations(unit.snapshot()))
            session_id, csrf, token_id = random_token(), random_token(), random_token()
            issued = int(self.clock())
            expiry = issued + SESSION_TTL_SECONDS
            unit._query(
                "INSERT INTO {table}(id,persona,csrf,expires_at) VALUES (%s,%s,%s,%s)",
                (session_id, persona, csrf, expiry),
                table="sessions",
            )
            unit._query(
                "INSERT INTO {table} VALUES (%s,%s,%s,%s,%s)",
                (session_id, actor.actor_id, token_id, issued, issued),
                table="session_tokens",
            )
            self._record_session_scope(unit, session_id, actor)
            secret = unit._query(
                "SELECT value FROM {table} WHERE key='session_key'", table="metadata"
            ).fetchone()["value"]
            token = SessionCodec(bytes.fromhex(secret)).sign(
                session_id, expiry, subject=actor.actor_id, token_id=token_id, issued_at=issued
            )
        return token, {"user": actor.public(), "csrf_token": csrf, "expires_at": iso(expiry)}

    def _record_session_scope(
        self, unit: PostgresReadSession, session_id: str, actor: Actor
    ) -> None:
        """Version-specific durable scope binding; v3 has one fixed generation."""

    def _session_actor(
        self, unit: PostgresReadSession, row: dict[str, Any], snapshot: dict[str, Any]
    ) -> Actor:
        return resolve_persona(row["persona"], self._locations(snapshot))

    def session(self, token: str) -> tuple[Actor, str, str] | None:
        with self.read_snapshot() as unit:
            snapshot = unit.snapshot()
            secret = unit._query(
                "SELECT value FROM {table} WHERE key='session_key'", table="metadata"
            ).fetchone()["value"]
            claims = SessionCodec(bytes.fromhex(secret)).verify(token, now=self.clock())
            if claims is None:
                return None
            row = unit._query(
                "SELECT s.*,t.subject,t.token_id,t.issued_at,t.not_before "
                "FROM {sessions} s JOIN {tokens} t ON t.session_id=s.id "
                "WHERE s.id=%s AND s.revoked=0 AND s.expires_at>%s",
                (claims.session_id, self.clock()),
                sessions="sessions",
                tokens="session_tokens",
            ).fetchone()
            if row is None:
                return None
            actor = self._session_actor(unit, row, snapshot)
            if (
                claims.subject != actor.actor_id
                or claims.subject != row["subject"]
                or claims.token_id != row["token_id"]
                or claims.issued_at != row["issued_at"]
                or claims.not_before != row["not_before"]
                or claims.expires_at != row["expires_at"]
            ):
                return None
            return actor, row["csrf"], claims.session_id

    def revoke_session(self, session_id: str) -> None:
        with self.write_transaction() as unit:
            unit._query("UPDATE {table} SET revoked=1 WHERE id=%s", (session_id,), table="sessions")

    def _binding(self, row: dict[str, Any], fingerprint: str, *, fresh: bool = True) -> None:
        if fingerprint != row["fingerprint"]:
            raise StoreError("stale_fingerprint", "The reviewed recommendation has changed", 409)
        if fresh and self.clock() >= row["valid_until"]:
            raise StoreError("recommendation_expired", "The recommendation has expired", 409)

    @staticmethod
    def _note(note: str) -> None:
        if not isinstance(note, str):
            raise StoreError("invalid_note", "A note must be text", 422)

    def _result(
        self, unit: PostgresReadSession, recommendation_id: str, actor: Actor
    ) -> dict[str, Any]:
        self._seal(unit)
        snapshot = unit.snapshot()
        return self._public(self._row(snapshot, recommendation_id, actor), snapshot, actor)

    def _action_gate(self, unit: PostgresReadSession) -> None:
        """Extension point called inside the workflow write transaction, never before it."""

    def decide(
        self, recommendation_id: str, actor: Actor, action: str, fingerprint: str, note: str = ""
    ) -> dict[str, Any]:
        if action not in {"approve", "reject", "revoke"}:
            raise StoreError("invalid_action", "Unsupported decision", 422)
        self._permission(actor, Permission.REJECT if action == "reject" else Permission.APPROVE)
        self._note(note)
        with self.write_transaction() as unit:
            snapshot = unit.snapshot()
            row = self._row(snapshot, recommendation_id, actor)
            self._binding(row, fingerprint, fresh=action != "revoke")
            if actor.actor_id == row["creator"]:
                raise StoreError("maker_checker", "The maker cannot approve their own work", 403)
            if action == "approve":
                self._action_gate(unit)
                # The fence can wait on an in-flight capture transaction.
                self._binding(row, fingerprint)
            target = {"approve": "approved", "reject": "rejected", "revoke": "revoked"}[action]
            if row["state"] == target and (
                action != "approve" or row["approval_actor"] == actor.actor_id
            ):
                return self._public(row, snapshot, actor)
            legal = {"approved", "execution_failed"} if action == "revoke" else {"pending"}
            if row["state"] not in legal:
                raise StoreError("illegal_transition", f"Cannot {action} from {row['state']}")
            details: dict[str, Any] = {"fingerprint": fingerprint, "note": note}
            if action == "approve":
                expiry = min(row["valid_until"], self.clock() + APPROVAL_TTL_SECONDS)
                unit._query(
                    "UPDATE {table} SET state=%s,approval_actor=%s,approval_fingerprint=%s,"
                    "approval_expires_at=%s WHERE id=%s",
                    (target, actor.actor_id, fingerprint, expiry, recommendation_id),
                    table="recommendations",
                )
                details["approval_expires_at"] = expiry
            else:
                unit._query(
                    "UPDATE {table} SET state=%s WHERE id=%s",
                    (target, recommendation_id),
                    table="recommendations",
                )
                if action == "revoke":
                    unit._query(
                        "UPDATE {table} SET state='cancelled' WHERE recommendation_id=%s",
                        (recommendation_id,),
                        table="outbox",
                    )
            self._event(unit, recommendation_id, target, actor.actor_id, actor.role.value, details)
            return self._result(unit, recommendation_id, actor)

    def execute(
        self,
        recommendation_id: str,
        actor: Actor,
        fingerprint: str,
        idempotency_key: str,
        *,
        simulate_failure: bool = False,
    ) -> dict[str, Any]:
        self._permission(actor, Permission.REQUEST_EXECUTION)
        if not isinstance(idempotency_key, str) or not 0 < len(idempotency_key) <= 128:
            raise StoreError(
                "invalid_idempotency_key", "A bounded idempotency key is required", 422
            )
        with self.write_transaction() as unit:
            snapshot = unit.snapshot()
            row = self._row(snapshot, recommendation_id, actor)
            self._binding(row, fingerprint, fresh=False)
            old = unit._query(
                "SELECT * FROM {table} WHERE key=%s", (idempotency_key,), table="idempotency"
            ).fetchone()
            if old and (
                old["recommendation_id"] != recommendation_id or old["fingerprint"] != fingerprint
            ):
                raise StoreError(
                    "idempotency_conflict", "This request key is bound to other content"
                )
            effect = next(
                (
                    value
                    for value in snapshot["effects"]
                    if value["recommendation_id"] == recommendation_id
                ),
                None,
            )
            if effect:
                unit._query(
                    "INSERT INTO {table} VALUES (%s,%s,%s) ON CONFLICT(key) DO NOTHING",
                    (idempotency_key, recommendation_id, fingerprint),
                    table="idempotency",
                )
                self._stage("idempotency_recorded")
                return self._result(unit, recommendation_id, actor)
            self._binding(row, fingerprint)
            self._action_gate(unit)
            if row["state"] not in {"approved", "execution_failed"}:
                raise StoreError("approval_required", "Execution requires a current human approval")
            if (
                row["approval_fingerprint"] != fingerprint
                or row["approval_actor"] == row["creator"]
                or row["approval_expires_at"] is None
                or self.clock() >= row["approval_expires_at"]
            ):
                raise StoreError(
                    "approval_expired", "Approval is expired or bound to other content"
                )
            unit._query(
                "INSERT INTO {table} VALUES (%s,%s,%s) ON CONFLICT(key) DO NOTHING",
                (idempotency_key, recommendation_id, fingerprint),
                table="idempotency",
            )
            self._stage("idempotency_recorded")
            operation = digest({"id": recommendation_id, "fingerprint": fingerprint})
            unit._query(
                "INSERT INTO {table} VALUES (%s,%s,'pending',1,NULL,%s) "
                "ON CONFLICT(recommendation_id) DO UPDATE SET "
                "attempts={table}.attempts+1,state='pending',last_error=NULL",
                (recommendation_id, operation, fingerprint),
                table="outbox",
            )
            self._stage("outbox_written")
            self._event(
                unit,
                recommendation_id,
                "execution_requested",
                actor.actor_id,
                actor.role.value,
                {"operation_id": operation, "fingerprint": fingerprint},
            )
            if simulate_failure:
                unit._query(
                    "UPDATE {table} SET state='failed',last_error='simulated ERP timeout' "
                    "WHERE recommendation_id=%s",
                    (recommendation_id,),
                    table="outbox",
                )
                unit._query(
                    "UPDATE {table} SET state='execution_failed' WHERE id=%s",
                    (recommendation_id,),
                    table="recommendations",
                )
                self._event(
                    unit,
                    recommendation_id,
                    "execution_failed",
                    "mock-erp-adapter",
                    "system",
                    {
                        "reason": "Simulated timeout before effect; safe to retry",
                        "effect_created": False,
                    },
                )
                return self._result(unit, recommendation_id, actor)
            instant = float(self.clock())
            # Recheck at effect time; slow transactions cannot execute after expiry.
            if instant >= row["valid_until"] or instant >= row["approval_expires_at"]:
                raise StoreError(
                    "approval_expired", "Approval expired before the effect was committed"
                )
            effect = _effect(row, json.loads(row["payload"]), instant)
            unit._query(
                "INSERT INTO {table} VALUES (%s,%s,%s,%s,%s)",
                (
                    recommendation_id,
                    effect["external_reference"],
                    fingerprint,
                    effect["payload"],
                    instant,
                ),
                table="effects",
            )
            self._stage("effect_written")
            due = instant + DEMO_MATURITY_SECONDS
            unit._query(
                "UPDATE {table} SET state='executed',execution_ref=%s,executed_at=%s,"
                "outcome_due_at=%s WHERE id=%s",
                (effect["external_reference"], instant, due, recommendation_id),
                table="recommendations",
            )
            unit._query(
                "UPDATE {table} SET state='delivered' WHERE recommendation_id=%s",
                (recommendation_id,),
                table="outbox",
            )
            self._stage("projection_written")
            self._event(
                unit,
                recommendation_id,
                "executed",
                "mock-erp-adapter",
                "system",
                {
                    "external_reference": effect["external_reference"],
                    "fingerprint": fingerprint,
                    "simulated": True,
                    "executed_at": instant,
                    "outcome_due_at": due,
                },
            )
            return self._result(unit, recommendation_id, actor)

    def observe(
        self, recommendation_id: str, actor: Actor, fingerprint: str, note: str = ""
    ) -> dict[str, Any]:
        if actor.role is not ActorRole.PLANNER:
            raise StoreError("role_denied", "A planner must request the simulated outcome", 403)
        self._note(note)
        with self.write_transaction() as unit:
            snapshot = unit.snapshot()
            row = self._row(snapshot, recommendation_id, actor)
            self._binding(row, fingerprint, fresh=False)
            if row["state"] == "outcome_observed":
                return self._public(row, snapshot, actor)
            if row["state"] != "executed":
                raise StoreError("execution_required", "An outcome requires a successful execution")
            if row["outcome_due_at"] is None or self.clock() < row["outcome_due_at"]:
                raise StoreError(
                    "outcome_immature", "The simulated observation window is still open", 425
                )
            payload = json.loads(row["payload"])
            outcome = {
                "received_quantity": payload["recommended_quantity"],
                "observed_demand": payload.get("observed_demand"),
                "observation_date": payload.get("outcome_observation_date"),
                "provenance": payload.get("outcome_provenance", "synthetic_scenario_replay"),
                "scenario_seed": payload.get("outcome_scenario_seed"),
                "execution_ref": row["execution_ref"],
                "recorded_at": iso(self.clock()),
                "maturity_seconds": DEMO_MATURITY_SECONDS,
                "simulated": True,
                "causal_business_impact_claim": False,
                "note": note,
            }
            unit._query(
                "UPDATE {table} SET state='outcome_observed',outcome_json=%s WHERE id=%s",
                (canonical(outcome), recommendation_id),
                table="recommendations",
            )
            self._event(
                unit,
                recommendation_id,
                "outcome_observed",
                actor.actor_id,
                actor.role.value,
                outcome,
            )
            return self._result(unit, recommendation_id, actor)
