"""Durable local workflow with transactional approval and mock ERP execution.

SQLite serializes writers with BEGIN IMMEDIATE. A unique effect per immutable
recommendation makes double clicks and retries safe across threads and processes.
The outbox and mock ERP live in one transaction in this educational adapter;
an external ERP would require its own idempotent receiver and delivery worker.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from nexus.contracts import ActorRole
from nexus.policies.authorization import AuthorizationError, Permission, assert_permission
from nexus.portfolio.auth import (
    SESSION_TTL_SECONDS,
    Actor,
    SessionCodec,
    random_token,
    resolve_persona,
)

APPROVAL_TTL_SECONDS = 900
RECOMMENDATION_TTL_SECONDS = 86_400
DEMO_MATURITY_SECONDS = 5


class StoreError(ValueError):
    """Expected, safe-to-display workflow refusal."""

    def __init__(self, code: str, message: str, status: int = 409) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def iso(value: float) -> str:
    return datetime.fromtimestamp(value, UTC).isoformat()


class PortfolioStore:
    """Single-machine persistent demo state; no cloud services are required."""

    def __init__(self, state_dir: Path, *, clock: Callable[[], float] = time.time) -> None:
        self.path = state_dir / "portfolio.sqlite3"
        self.clock = clock
        state_dir.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS recommendations (
                  id TEXT PRIMARY KEY, payload TEXT NOT NULL, fingerprint TEXT NOT NULL,
                  location_id TEXT NOT NULL, creator TEXT NOT NULL, state TEXT NOT NULL,
                  created_at REAL NOT NULL, valid_until REAL NOT NULL,
                  approval_actor TEXT, approval_fingerprint TEXT, approval_expires_at REAL,
                  execution_ref TEXT, executed_at REAL, outcome_due_at REAL, outcome_json TEXT);
                CREATE TABLE IF NOT EXISTS events (
                  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                  recommendation_id TEXT NOT NULL REFERENCES recommendations(id),
                  event_type TEXT NOT NULL, actor_id TEXT NOT NULL, actor_role TEXT NOT NULL,
                  occurred_at REAL NOT NULL, details TEXT NOT NULL,
                  previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS outbox (
                  recommendation_id TEXT PRIMARY KEY REFERENCES recommendations(id),
                  operation_id TEXT NOT NULL UNIQUE, state TEXT NOT NULL,
                  attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT,
                  fingerprint TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS effects (
                  recommendation_id TEXT PRIMARY KEY REFERENCES recommendations(id),
                  external_reference TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL,
                  payload TEXT NOT NULL, created_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS idempotency (
                  key TEXT PRIMARY KEY, recommendation_id TEXT NOT NULL,
                  fingerprint TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS sessions (
                  id TEXT PRIMARY KEY, persona TEXT NOT NULL, csrf TEXT NOT NULL,
                  expires_at REAL NOT NULL, revoked INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS session_tokens (
                  session_id TEXT PRIMARY KEY REFERENCES sessions(id),
                  subject TEXT NOT NULL, token_id TEXT NOT NULL UNIQUE,
                  issued_at INTEGER NOT NULL, not_before INTEGER NOT NULL);
                """
            )
            db.execute(
                "INSERT OR IGNORE INTO metadata VALUES ('session_key', ?)",
                (secrets.token_hex(32),),
            )
            db.commit()

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=30000")
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def has_seed(self) -> bool:
        with self.connection() as db:
            return db.execute("SELECT 1 FROM metadata WHERE key='seed'").fetchone() is not None

    def ensure_seed(self, seed: dict[str, Any]) -> None:
        """Persist once; restarts never reset a reviewer decision or extend expiry."""

        now = self.clock()
        with self.transaction() as db:
            existing = db.execute("SELECT value FROM metadata WHERE key='seed'").fetchone()
            if existing is not None:
                if digest(json.loads(existing[0])) != digest(seed):
                    raise StoreError(
                        "seed_changed",
                        "Evidence differs from the persisted workflow; "
                        "choose a new demo state directory",
                    )
                return
            db.execute("INSERT INTO metadata VALUES ('seed', ?)", (canonical(seed),))
            db.execute("INSERT INTO metadata VALUES ('seed_hash', ?)", (digest(seed),))
            for item in seed["recommendations"]:
                payload = dict(item)
                payload.pop("status", None)
                payload.pop("state", None)
                payload["created_at"] = iso(now)
                payload["valid_until"] = iso(now + RECOMMENDATION_TTL_SECONDS)
                fingerprint = digest(payload)
                db.execute(
                    """INSERT INTO recommendations
                    (id,payload,fingerprint,location_id,creator,state,created_at,valid_until)
                    VALUES (?,?,?,?,?,'pending',?,?)""",
                    (
                        payload["id"],
                        canonical(payload),
                        fingerprint,
                        payload["location_id"],
                        payload.get("created_by", "planner.r4a"),
                        now,
                        now + RECOMMENDATION_TTL_SECONDS,
                    ),
                )
                self._event(
                    db,
                    payload["id"],
                    "recommendation_created",
                    str(payload.get("created_by", "planner.r4a")),
                    "planner",
                    {
                        "fingerprint": fingerprint,
                        "source": (
                            "connected runtime baseline"
                            if seed.get("metadata", {}).get("pipeline_mode")
                            == "connected_runtime_baseline"
                            else "frozen synthetic experiment"
                        ),
                    },
                )

    def seed(self) -> dict[str, Any]:
        with self.connection() as db:
            row = db.execute("SELECT value FROM metadata WHERE key='seed'").fetchone()
            expected = db.execute("SELECT value FROM metadata WHERE key='seed_hash'").fetchone()
        if row is None or expected is None:
            raise StoreError("not_ready", "Demo initialization has not completed", 503)
        value = cast(dict[str, Any], json.loads(row[0]))
        if digest(value) != expected[0]:
            raise StoreError("integrity_error", "Stored evidence failed its integrity check", 409)
        return value

    def locations(self) -> tuple[str, ...]:
        with self.connection() as db:
            return tuple(
                row[0]
                for row in db.execute(
                    "SELECT DISTINCT location_id FROM recommendations ORDER BY location_id"
                )
            )

    @staticmethod
    def _generation_catalog(
        db: sqlite3.Connection,
    ) -> tuple[dict[str, Any], str, dict[str, tuple[str, dict[str, Any], dict[str, Any]]]]:
        """Read and validate all ownership bindings inside the caller's read snapshot."""
        values = dict(
            db.execute("SELECT key,value FROM metadata WHERE key IN ('seed','seed_hash')")
        )
        if set(values) != {"seed", "seed_hash"}:
            raise StoreError("not_ready", "Demo initialization has not completed", 503)
        current: dict[str, Any] = json.loads(values["seed"])
        current_hash = values["seed_hash"]
        if digest(current) != current_hash:
            raise StoreError("integrity_error", "Stored evidence failed its integrity check")
        generations = [(current_hash, current)]
        if current.get("metadata", {}).get("pipeline_mode") == "connected_runtime_baseline":
            from nexus.portfolio.runtime_control import RuntimeControl

            marker = db.execute(
                "SELECT value FROM metadata WHERE key='runtime_state_hash'"
            ).fetchone()
            if marker is None or marker[0] != RuntimeControl._state_hash(db):
                raise StoreError("integrity_error", "Runtime journal or projection was changed")
            for row in db.execute("SELECT seed_hash,payload FROM runtime_generations"):
                archived = json.loads(row["payload"])
                if digest(archived) != row["seed_hash"]:
                    raise StoreError("integrity_error", "Archived generation hash mismatch")
                generations.append((row["seed_hash"], archived))
        owners: dict[str, tuple[str, dict[str, Any], dict[str, Any]]] = {}
        for identity, seed in generations:
            for item in seed["recommendations"]:
                if item["id"] in owners:
                    raise StoreError("integrity_error", "Recommendation generation is ambiguous")
                owners[item["id"]] = identity, seed, item
        return current, current_hash, owners

    @staticmethod
    def _assert_generation_item(row: sqlite3.Row, item: dict[str, Any]) -> None:
        expected = {key: value for key, value in item.items() if key not in {"state", "status"}}
        expected.update(created_at=iso(row["created_at"]), valid_until=iso(row["valid_until"]))
        if digest(expected) != row["fingerprint"]:
            raise StoreError("integrity_error", "Recommendation differs from its owning generation")

    def overview_snapshot(self, actor: Actor) -> dict[str, Any]:
        """Return current evidence and scoped history without crossing generation snapshots."""
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.connection() as db:
            db.execute("BEGIN")
            self._verify_events(db)
            seed, current_hash, owners = self._generation_catalog(db)
            recommendations = []
            for row in db.execute(
                "SELECT id,location_id FROM recommendations ORDER BY id"
            ).fetchall():
                if row["location_id"] not in actor.locations:
                    continue
                record = self._row(db, row["id"], actor)
                if row["id"] not in owners:
                    raise StoreError(
                        "integrity_error", "Recommendation owning generation is missing"
                    )
                identity, _, item = owners[row["id"]]
                self._assert_generation_item(record, item)
                recommendations.append(
                    {
                        **self._public(record),
                        "generation_seed_sha256": identity,
                        "current_generation": identity == current_hash,
                    }
                )
            return {
                **seed,
                "recommendations": recommendations,
                "current_recommendations": [
                    item for item in recommendations if item["current_generation"]
                ],
                "generation_seed_sha256": current_hash,
                "checked_at": iso(self.clock()),
            }

    def recommendation_context(self, recommendation_id: str, actor: Actor) -> dict[str, Any]:
        """Attach only evidence from the exact immutable generation that owns this ID."""
        self._permission(actor, Permission.VIEW_AUDIT)
        with self.connection() as db:
            db.execute("BEGIN")
            events = self._verify_events(db)
            row = self._row(db, recommendation_id, actor)
            _, current_hash, owners = self._generation_catalog(db)
            if recommendation_id not in owners:
                raise StoreError("integrity_error", "Recommendation owning generation is missing")
            identity, seed, item = owners[recommendation_id]
            self._assert_generation_item(row, item)
            return {
                **self._public(row),
                "events": [
                    event for event in events if event["recommendation_id"] == recommendation_id
                ],
                "scenarios": seed.get("scenarios", []),
                "lineage": seed.get("lineage", []),
                "generation": {
                    "seed_sha256": identity,
                    "is_current": identity == current_hash,
                    "metadata": seed.get("metadata", {}),
                },
            }

    def create_session(self, persona: str) -> tuple[str, dict[str, Any]]:
        actor = resolve_persona(persona, self.locations())
        session_id, csrf, token_id = random_token(), random_token(), random_token()
        issued_at = int(self.clock())
        expiry = issued_at + SESSION_TTL_SECONDS
        with self.transaction() as db:
            db.execute(
                "INSERT INTO sessions(id,persona,csrf,expires_at) VALUES (?,?,?,?)",
                (session_id, persona, csrf, expiry),
            )
            db.execute(
                "INSERT INTO session_tokens VALUES (?,?,?,?,?)",
                (session_id, actor.actor_id, token_id, issued_at, issued_at),
            )
            secret = db.execute("SELECT value FROM metadata WHERE key='session_key'").fetchone()[0]
            token = SessionCodec(bytes.fromhex(secret)).sign(
                session_id,
                expiry,
                subject=actor.actor_id,
                token_id=token_id,
                issued_at=issued_at,
            )
        return token, {"user": actor.public(), "csrf_token": csrf, "expires_at": iso(expiry)}

    def session(self, token: str) -> tuple[Actor, str, str] | None:
        with self.connection() as db:
            secret = db.execute("SELECT value FROM metadata WHERE key='session_key'").fetchone()[0]
            claims = SessionCodec(bytes.fromhex(secret)).verify(token, now=self.clock())
            if claims is None:
                return None
            row = db.execute(
                """SELECT sessions.*,session_tokens.subject,session_tokens.token_id,
                session_tokens.issued_at,session_tokens.not_before
                FROM sessions JOIN session_tokens ON sessions.id=session_tokens.session_id
                WHERE sessions.id=? AND revoked=0 AND expires_at>?""",
                (claims.session_id, self.clock()),
            ).fetchone()
        if row is None:
            return None
        try:
            actor = resolve_persona(row["persona"], self.locations())
        except ValueError:
            return None
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
        with self.transaction() as db:
            db.execute("UPDATE sessions SET revoked=1 WHERE id=?", (session_id,))

    @staticmethod
    def _scope(row: sqlite3.Row, actor: Actor) -> None:
        if row["location_id"] not in actor.locations:
            raise StoreError("scope_denied", "This location is outside your assigned scope", 403)

    def _row(self, db: sqlite3.Connection, recommendation_id: str, actor: Actor) -> sqlite3.Row:
        row = db.execute(
            "SELECT * FROM recommendations WHERE id=?", (recommendation_id,)
        ).fetchone()
        if row is None:
            raise StoreError("not_found", "Recommendation was not found", 404)
        self._scope(row, actor)
        payload = json.loads(row["payload"])
        if (
            digest(payload) != row["fingerprint"]
            or payload["id"] != row["id"]
            or payload["location_id"] != row["location_id"]
            or payload.get("created_by", "planner.r4a") != row["creator"]
            or payload["valid_until"] != iso(row["valid_until"])
        ):
            raise StoreError("integrity_error", "Recommendation failed its integrity check", 409)
        self._assert_projection(db, row)
        return cast(sqlite3.Row, row)

    @staticmethod
    def _assert_projection(db: sqlite3.Connection, row: sqlite3.Row) -> None:
        """A mutable projection must still agree with the immutable event history."""

        expected_state = "pending"
        approval: dict[str, Any] | None = None
        execution: dict[str, Any] | None = None
        outcome: dict[str, Any] | None = None
        state_events = {
            "approved",
            "rejected",
            "revoked",
            "executed",
            "execution_failed",
            "outcome_observed",
            "invalidated",
        }
        for event in db.execute(
            "SELECT * FROM events WHERE recommendation_id=? ORDER BY sequence", (row["id"],)
        ):
            if event["event_type"] in state_events:
                expected_state = event["event_type"]
            if event["event_type"] == "approved":
                approval = {**json.loads(event["details"]), "actor_id": event["actor_id"]}
            if event["event_type"] == "executed":
                execution = json.loads(event["details"])
            if event["event_type"] == "outcome_observed":
                outcome = json.loads(event["details"])
        invalid = row["state"] != expected_state
        if approval is not None:
            invalid |= (
                approval["actor_id"] != row["approval_actor"]
                or approval["fingerprint"] != row["approval_fingerprint"]
                or approval["approval_expires_at"] != row["approval_expires_at"]
            )
        if execution is not None:
            invalid |= (
                execution["external_reference"] != row["execution_ref"]
                or execution["executed_at"] != row["executed_at"]
                or execution["outcome_due_at"] != row["outcome_due_at"]
            )
        if outcome is not None:
            invalid |= canonical(outcome) != row["outcome_json"]
        if invalid:
            raise StoreError("integrity_error", "Workflow state disagrees with its audit history")

    @staticmethod
    def _approval_renewable(row: sqlite3.Row, now: float) -> bool:
        return (
            row["state"] in {"approved", "execution_failed"}
            and row["approval_actor"] is not None
            and row["approval_actor"] != row["creator"]
            and row["approval_fingerprint"] == row["fingerprint"]
            and row["approval_expires_at"] is not None
            and row["approval_expires_at"] <= now < row["valid_until"]
        )

    def _public(self, row: sqlite3.Row) -> dict[str, Any]:
        now = self.clock()
        value: dict[str, Any] = json.loads(row["payload"])
        value.update(
            {
                "fingerprint": row["fingerprint"],
                "status": row["state"],
                "state": row["state"],
                "approved_by": row["approval_actor"],
                "approval_expires_at": (
                    iso(row["approval_expires_at"]) if row["approval_expires_at"] else None
                ),
                "approval_renewable": self._approval_renewable(row, now),
                "execution_ref": row["execution_ref"],
                "outcome_due_at": iso(row["outcome_due_at"]) if row["outcome_due_at"] else None,
                "outcome": json.loads(row["outcome_json"]) if row["outcome_json"] else None,
                "expired": now >= row["valid_until"],
                "outcome_ready": bool(
                    row["state"] == "executed"
                    and row["outcome_due_at"] is not None
                    and now >= row["outcome_due_at"]
                ),
            }
        )
        return value

    def recommendations(self, actor: Actor) -> list[dict[str, Any]]:
        with self.connection() as db:
            self._verify_events(db)
            rows = db.execute("SELECT id,location_id FROM recommendations ORDER BY id").fetchall()
            return [
                self._public(self._row(db, row["id"], actor))
                for row in rows
                if row["location_id"] in actor.locations
            ]

    def recommendation(self, recommendation_id: str, actor: Actor) -> dict[str, Any]:
        with self.connection() as db:
            value = self._public(self._row(db, recommendation_id, actor))
        value["events"] = [
            event for event in self.audit(actor) if event["recommendation_id"] == recommendation_id
        ]
        return value

    @staticmethod
    def _permission(actor: Actor, permission: Permission) -> None:
        try:
            assert_permission(actor.role, permission)
        except AuthorizationError as exc:
            raise StoreError("role_denied", "Your role cannot perform this action", 403) from exc

    def _binding(self, row: sqlite3.Row, fingerprint: str, *, fresh: bool = True) -> None:
        if fingerprint != row["fingerprint"]:
            raise StoreError("stale_fingerprint", "The reviewed recommendation has changed", 409)
        if fresh and self.clock() >= row["valid_until"]:
            raise StoreError("recommendation_expired", "The recommendation has expired", 409)

    def decide(
        self,
        recommendation_id: str,
        actor: Actor,
        action: str,
        fingerprint: str,
        note: str = "",
    ) -> dict[str, Any]:
        if action not in {"approve", "reject", "revoke"}:
            raise StoreError("invalid_action", "Unsupported decision", 422)
        self._permission(actor, Permission.REJECT if action == "reject" else Permission.APPROVE)
        with self.transaction() as db:
            self._verify_events(db)
            row = self._row(db, recommendation_id, actor)
            self._binding(row, fingerprint, fresh=action != "revoke")
            if actor.actor_id == row["creator"]:
                raise StoreError("maker_checker", "The maker cannot approve their own work", 403)
            target = {"approve": "approved", "reject": "rejected", "revoke": "revoked"}[action]
            now = self.clock()
            if row["state"] == target and action != "approve":
                return self._public(row)
            if action == "approve" and row["state"] in {"approved", "execution_failed"}:
                if row["approval_actor"] != actor.actor_id:
                    raise StoreError("illegal_transition", "Another approver owns this approval")
                if (
                    row["state"] == "approved"
                    and row["approval_expires_at"] is not None
                    and now < row["approval_expires_at"]
                ):
                    return self._public(row)
            if action == "revoke":
                allowed = row["state"] in {"approved", "execution_failed"}
            else:
                allowed = row["state"] == "pending" or (
                    action == "approve" and self._approval_renewable(row, now)
                )
            if not allowed:
                raise StoreError("illegal_transition", f"Cannot {action} from {row['state']}")
            if action == "approve":
                expiry = min(row["valid_until"], now + APPROVAL_TTL_SECONDS)
                db.execute(
                    """UPDATE recommendations SET state=?,approval_actor=?,
                    approval_fingerprint=?,approval_expires_at=? WHERE id=?""",
                    (target, actor.actor_id, fingerprint, expiry, recommendation_id),
                )
            else:
                db.execute(
                    "UPDATE recommendations SET state=? WHERE id=?", (target, recommendation_id)
                )
                if action == "revoke":
                    db.execute(
                        "UPDATE outbox SET state='cancelled' WHERE recommendation_id=?",
                        (recommendation_id,),
                    )
            details: dict[str, Any] = {"fingerprint": fingerprint, "note": note}
            if action == "approve":
                details["approval_expires_at"] = expiry
            self._event(db, recommendation_id, target, actor.actor_id, actor.role.value, details)
            return self._public(self._row(db, recommendation_id, actor))

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
        if not idempotency_key or len(idempotency_key) > 128:
            raise StoreError(
                "invalid_idempotency_key", "A bounded idempotency key is required", 422
            )
        with self.transaction() as db:
            self._verify_events(db)
            row = self._row(db, recommendation_id, actor)
            self._binding(row, fingerprint, fresh=False)
            old_key = db.execute(
                "SELECT * FROM idempotency WHERE key=?", (idempotency_key,)
            ).fetchone()
            if old_key and (
                old_key["recommendation_id"] != recommendation_id
                or old_key["fingerprint"] != fingerprint
            ):
                raise StoreError(
                    "idempotency_conflict", "This request key is bound to other content"
                )
            effect = db.execute(
                "SELECT * FROM effects WHERE recommendation_id=?", (recommendation_id,)
            ).fetchone()
            if effect:
                if (
                    effect["fingerprint"] != fingerprint
                    or effect["external_reference"] != row["execution_ref"]
                ):
                    raise StoreError(
                        "integrity_error", "Execution binding failed its integrity check"
                    )
                expected_payload = json.loads(row["payload"])
                expected_effect = {
                    "sku_id": expected_payload.get("sku_id", expected_payload.get("sku")),
                    "location_id": row["location_id"],
                    "quantity": expected_payload["recommended_quantity"],
                    "simulated": True,
                }
                if canonical(expected_effect) != effect["payload"]:
                    raise StoreError("integrity_error", "Persisted mock effect has changed")
                db.execute(
                    "INSERT OR IGNORE INTO idempotency VALUES (?,?,?)",
                    (idempotency_key, recommendation_id, fingerprint),
                )
                return self._public(row)
            self._binding(row, fingerprint)
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
            db.execute(
                "INSERT OR IGNORE INTO idempotency VALUES (?,?,?)",
                (idempotency_key, recommendation_id, fingerprint),
            )
            operation_id = digest({"id": recommendation_id, "fingerprint": fingerprint})
            db.execute(
                """INSERT INTO outbox VALUES (?,?,'pending',1,NULL,?)
                ON CONFLICT(recommendation_id) DO UPDATE SET
                attempts=attempts+1,state='pending',last_error=NULL""",
                (recommendation_id, operation_id, fingerprint),
            )
            self._event(
                db,
                recommendation_id,
                "execution_requested",
                actor.actor_id,
                actor.role.value,
                {"operation_id": operation_id, "fingerprint": fingerprint},
            )
            if simulate_failure:
                db.execute(
                    """UPDATE outbox SET state='failed',last_error='simulated ERP timeout'
                    WHERE recommendation_id=?""",
                    (recommendation_id,),
                )
                db.execute(
                    "UPDATE recommendations SET state='execution_failed' WHERE id=?",
                    (recommendation_id,),
                )
                self._event(
                    db,
                    recommendation_id,
                    "execution_failed",
                    "mock-erp-adapter",
                    "system",
                    {
                        "reason": "Simulated timeout before effect; safe to retry",
                        "effect_created": False,
                    },
                )
                return self._public(self._row(db, recommendation_id, actor))
            payload = json.loads(row["payload"])
            external_reference = f"MOCK-PO-{operation_id[:16].upper()}"
            effect_payload = {
                "sku_id": payload.get("sku_id", payload.get("sku")),
                "location_id": row["location_id"],
                "quantity": payload["recommended_quantity"],
                "simulated": True,
            }
            execution_time = float(self.clock())
            outcome_due = execution_time + DEMO_MATURITY_SECONDS
            db.execute(
                "INSERT INTO effects VALUES (?,?,?,?,?)",
                (
                    recommendation_id,
                    external_reference,
                    fingerprint,
                    canonical(effect_payload),
                    execution_time,
                ),
            )
            db.execute(
                """UPDATE recommendations SET state='executed',execution_ref=?,executed_at=?,
                outcome_due_at=? WHERE id=?""",
                (
                    external_reference,
                    execution_time,
                    outcome_due,
                    recommendation_id,
                ),
            )
            db.execute(
                "UPDATE outbox SET state='delivered' WHERE recommendation_id=?",
                (recommendation_id,),
            )
            self._event(
                db,
                recommendation_id,
                "executed",
                "mock-erp-adapter",
                "system",
                {
                    "external_reference": external_reference,
                    "fingerprint": fingerprint,
                    "simulated": True,
                    "executed_at": execution_time,
                    "outcome_due_at": outcome_due,
                },
            )
            return self._public(self._row(db, recommendation_id, actor))

    def observe(
        self, recommendation_id: str, actor: Actor, fingerprint: str, note: str = ""
    ) -> dict[str, Any]:
        if actor.role is not ActorRole.PLANNER:
            raise StoreError("role_denied", "A planner must request the simulated outcome", 403)
        with self.transaction() as db:
            self._verify_events(db)
            row = self._row(db, recommendation_id, actor)
            self._binding(row, fingerprint, fresh=False)
            if row["state"] == "outcome_observed":
                return self._public(row)
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
            db.execute(
                "UPDATE recommendations SET state='outcome_observed',outcome_json=? WHERE id=?",
                (canonical(outcome), recommendation_id),
            )
            self._event(
                db, recommendation_id, "outcome_observed", actor.actor_id, actor.role.value, outcome
            )
            return self._public(self._row(db, recommendation_id, actor))

    def _event(
        self,
        db: sqlite3.Connection,
        recommendation_id: str,
        event_type: str,
        actor_id: str,
        actor_role: str,
        details: dict[str, Any],
    ) -> None:
        previous = db.execute(
            "SELECT event_hash FROM events ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        previous_hash = previous[0] if previous else "0" * 64
        sequence = db.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM events").fetchone()[0]
        instant = float(self.clock())
        data = {
            "sequence": sequence,
            "recommendation_id": recommendation_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "actor_role": actor_role,
            "occurred_at": instant,
            "details": details,
            "previous_hash": previous_hash,
        }
        db.execute(
            """INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                sequence,
                recommendation_id,
                event_type,
                actor_id,
                actor_role,
                instant,
                canonical(details),
                previous_hash,
                digest(data),
            ),
        )
        db.execute("INSERT OR REPLACE INTO metadata VALUES ('audit_head',?)", (digest(data),))

    @staticmethod
    def _verify_events(db: sqlite3.Connection) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        previous_hash = "0" * 64
        for index, row in enumerate(db.execute("SELECT * FROM events ORDER BY sequence"), 1):
            data = dict(row)
            expected_hash = data.pop("event_hash")
            data["details"] = json.loads(data["details"])
            if (
                data["sequence"] != index
                or data["previous_hash"] != previous_hash
                or digest(data) != expected_hash
            ):
                raise StoreError("integrity_error", "Audit history failed its integrity check", 409)
            previous_hash = expected_hash
            data["event_hash"] = expected_hash
            data["occurred_at"] = iso(data["occurred_at"])
            events.append(data)
        head = db.execute("SELECT value FROM metadata WHERE key='audit_head'").fetchone()
        if (events and head is None) or (head is not None and head[0] != previous_hash):
            raise StoreError("integrity_error", "Audit history is incomplete", 409)
        return events

    def audit(self, actor: Actor) -> list[dict[str, Any]]:
        with self.connection() as db:
            events = self._verify_events(db)
            locations = {
                row["id"]: row["location_id"]
                for row in db.execute("SELECT id,location_id FROM recommendations")
            }
        return [
            event
            for event in events
            if locations.get(event["recommendation_id"]) in actor.locations
        ]

    def metrics_snapshot(self) -> dict[str, Any]:
        """Read-only operational counters; the root observability module consumes these."""

        with self.connection() as db:
            self._verify_events(db)
            all_locations = tuple(
                row[0] for row in db.execute("SELECT DISTINCT location_id FROM recommendations")
            )
            observer = Actor("metrics-observer", "Metrics", ActorRole.AUDITOR, all_locations)
            for record in db.execute("SELECT id FROM recommendations").fetchall():
                self._row(db, record[0], observer)
            states = {
                row[0]: row[1]
                for row in db.execute("SELECT state,COUNT(*) FROM recommendations GROUP BY state")
            }
            outbox = {
                row[0]: row[1]
                for row in db.execute("SELECT state,COUNT(*) FROM outbox GROUP BY state")
            }
            return {
                "states": states,
                "outbox": outbox,
                "effect_count": db.execute("SELECT COUNT(*) FROM effects").fetchone()[0],
                "event_count": db.execute("SELECT COUNT(*) FROM events").fetchone()[0],
                "retry_count": db.execute(
                    "SELECT COALESCE(SUM(MAX(attempts-1,0)),0) FROM outbox"
                ).fetchone()[0],
                "database": "sqlite",
                "persistence": "local WAL",
                "checked_at": iso(self.clock()),
            }
