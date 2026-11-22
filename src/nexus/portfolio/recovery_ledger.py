"""Append-only native transport quarantine, source-bound proposals and human review.

Review is not delivery. This extension deliberately does not advance an inbox
checkpoint until the separate acknowledged recovery execution path is implemented.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar
from uuid import UUID

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_backend import PostgresBackendError, _driver, _dsn, _refuse
from nexus.portfolio.postgres_migrations import read_migration_sql
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.store import StoreError, canonical, digest
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger, TransportRecord
from nexus.portfolio.transport_recovery import RuntimeDeadLetter, RuntimeRedriveRequest


class RecoveryTransportLedger(TransportLedger):
    """Explicit [5,9] transport extension; v5 clients refuse this schema."""

    _versions: ClassVar[tuple[int, ...]] = (5, 9)
    _prior_ledger_type: ClassVar[type[TransportLedger]] = TransportLedger

    _schema_columns: ClassVar[dict[str, dict[str, str]]] = {
        **TransportLedger._schema_columns,
        **{
            name: {"id": "text", "payload": "text", "payload_sha256": "text"}
            for name in ("transport_recovery_cases", "transport_recovery_sources")
        },
        "transport_recovery_requests": {
            "id": "text",
            "case_id": "text",
            "source_id": "text",
            "payload": "text",
            "payload_sha256": "text",
        },
        "transport_recovery_decisions": {
            "id": "text",
            "request_id": "text",
            "sequence": "bigint",
            "payload": "text",
            "payload_sha256": "text",
        },
    }
    _schema_tables: ClassVar[set[str]] = set(_schema_columns)
    _uniques: ClassVar[dict[tuple[str, str], list[int]]] = {
        **TransportLedger._uniques,
        ("transport_recovery_decisions", "transport_recovery_decisions_sequence_key"): [3],
    }
    _foreign_keys: ClassVar[dict[tuple[str, str], tuple[list[int], str, list[int]]]] = {
        **TransportLedger._foreign_keys,
        ("transport_recovery_cases", "transport_recovery_cases_id_fkey"): (
            [1],
            "transport_inbox",
            [1],
        ),
        ("transport_recovery_requests", "transport_recovery_requests_case_id_fkey"): (
            [2],
            "transport_recovery_cases",
            [1],
        ),
        ("transport_recovery_requests", "transport_recovery_requests_source_id_fkey"): (
            [3],
            "transport_recovery_sources",
            [1],
        ),
        ("transport_recovery_decisions", "transport_recovery_decisions_request_id_fkey"): (
            [2],
            "transport_recovery_requests",
            [1],
        ),
    }
    _history_triggers: ClassVar[dict[str, str]] = {
        **TransportLedger._history_triggers,
        "transport_recovery_cases": "immutable_recovery_case",
        "transport_recovery_sources": "immutable_recovery_source",
        "transport_recovery_requests": "immutable_recovery_request",
        "transport_recovery_decisions": "immutable_recovery_decision",
    }
    _forbidden_grants: ClassVar[dict[str, str]] = {
        **TransportLedger._forbidden_grants,
        **{
            name: "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER"
            for name in (
                "transport_recovery_cases",
                "transport_recovery_sources",
                "transport_recovery_requests",
                "transport_recovery_decisions",
            )
        },
    }

    @classmethod
    def _migration_bytes(cls, path: Path | None = None) -> tuple[str, str]:
        if path is not None:
            _refuse("migration_conflict", "Recovery requires the canonical packaged migration")
        raw = read_migration_sql(cls._versions[-1])
        return raw.decode("utf-8"), hashlib.sha256(raw).hexdigest()

    def _verify_migration_history(self, db: Any, expected_hash: str, application_role: str) -> None:
        hashes = {v: hashlib.sha256(read_migration_sql(v)).hexdigest() for v in self._versions}
        rows = self._query(
            db, "SELECT * FROM {t} ORDER BY version", t="schema_migrations"
        ).fetchall()
        if (
            expected_hash != hashes[self._versions[-1]]
            or [r["version"] for r in rows] != list(self._versions)
            or any(
                r["migration_sha256"] != hashes[r["version"]]
                or r["application_role"] != application_role
                or not math.isfinite(r["applied_at"])
                for r in rows
            )
        ):
            _refuse("migration_conflict", "Recovery transport migration history conflicts")

    def migrate(self, *, migration_dsn: str, migration_path: Path | None = None) -> dict[str, Any]:
        app, migration = _dsn(self.config.dsn), _dsn(migration_dsn)
        if app["user"] == migration["user"] or any(
            app[k] != migration[k] for k in ("hostaddr", "port", "dbname")
        ):
            _refuse(
                "configuration_error", "Use the separate bootstrap identity on the same database"
            )
        template, checksum = self._migration_bytes(migration_path)
        base = self._prior_ledger_type(self.config, clock=self.clock)
        # A new schema is first established by the unchanged canonical transport migration.
        with self._connection(migration) as db:
            found = db.execute(
                "SELECT to_regclass(%s) AS relation",
                (self._table("schema_migrations").as_string(db),),
            ).fetchone()
            existing = (
                []
                if found["relation"] is None
                else [
                    row["version"]
                    for row in self._query(
                        db, "SELECT version FROM {t} ORDER BY version", t="schema_migrations"
                    ).fetchall()
                ]
            )
        if found["relation"] is None or (
            len(existing) < len(self._versions) - 1
            and existing == list(self._versions[: len(existing)])
        ):
            try:
                base.migrate(migration_dsn=migration_dsn)
            except PostgresBackendError as exc:
                if exc.code != "migration_conflict":
                    raise
                # Another initializer may already have completed the extension.
                with self._connection(migration) as db:
                    self._verify_schema(db, checksum, app["user"])
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
            versions = [
                r["version"]
                for r in self._query(
                    db, "SELECT version FROM {t} ORDER BY version", t="schema_migrations"
                ).fetchall()
            ]
            applied = versions == list(self._versions[:-1])
            if applied:
                base._verify_schema(db, base._migration_bytes()[1], app["user"])
                base._check_application_role(db, app["user"])
                # Match dispatcher lock order; never obtain a workflow lock here.
                self._query(
                    db, "SELECT * FROM {t} ORDER BY partition FOR UPDATE", t="transport_delivery"
                ).fetchall()
                self._query(
                    db, "SELECT * FROM {t} ORDER BY partition FOR UPDATE", t="transport_progress"
                ).fetchall()
                db.execute(
                    sql.SQL(template).format(
                        schema=sql.Identifier(self.config.schema),
                        application_role=sql.Identifier(app["user"]),
                    )
                )
                self._initialize_extension(db)
                instant = self._now()
                self._query(
                    db,
                    "INSERT INTO {t} VALUES (%s,%s,%s,%s)",
                    (self._versions[-1], checksum, instant.timestamp(), app["user"]),
                    t="schema_migrations",
                )
            self._verify_schema(db, checksum, app["user"])
            self._check_application_role(db, app["user"])
            return {
                "version": self._versions[-1],
                "migration_sha256": checksum,
                "schema": self.config.schema,
                "applied": applied,
                "server_version": db.execute("SHOW server_version").fetchone()["server_version"],
            }

    def _initialize_extension(self, db: Any) -> None:
        """Initialize immutable extension policy only within its migration transaction."""

    def _now(self) -> datetime:
        value = self.clock()
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
        ):
            raise StoreError("invalid_clock", "Recovery requires a finite UTC clock", 503)
        try:
            return datetime.fromtimestamp(value, UTC)
        except (ValueError, OverflowError, OSError):
            raise StoreError("invalid_clock", "Recovery clock is outside UTC range", 503) from None

    @staticmethod
    def _authorize(binding: TransportBinding, actor: Actor, role: ActorRole | None = None) -> None:
        if (
            not isinstance(actor.actor_id, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,159}", actor.actor_id) is None
        ):
            raise StoreError("role_denied", "Recovery requires a bounded actor identity", 403)
        if binding.purpose != "operations":
            raise StoreError("recovery_denied", "Only the operations ledger may own recovery", 403)
        if (role is not None and actor.role is not role) or actor.role not in {
            ActorRole.PLANNER,
            ActorRole.APPROVER,
            ActorRole.AUDITOR,
        }:
            raise StoreError("role_denied", "Recovery role is not authorized", 403)
        if set(actor.locations) != set(binding.broker.locations):
            raise StoreError("scope_denied", "Recovery requires the complete bound scope", 403)

    def _pending(
        self, db: Any, binding: TransportBinding, partition: int
    ) -> tuple[dict[str, Any], TransportRecord]:
        if type(partition) is not int or not 0 <= partition < binding.partition_count:
            raise StoreError("invalid_partition", "Use an explicit bound partition", 422)
        delivered = self._progress(db, binding, delivery=True, lock=True)[partition]
        captured = self._progress(db, binding, delivery=False)[partition]
        if delivered["next_offset"] > captured["next_offset"]:
            _refuse("integrity_error", "Business delivery is ahead of capture")
        if delivered["next_offset"] == captured["next_offset"]:
            raise StoreError("recovery_not_pending", "No captured record awaits delivery", 409)
        row = self._query(
            db,
            "SELECT * FROM {t} WHERE partition=%s AND broker_offset=%s",
            (partition, delivered["next_offset"]),
            t="transport_inbox",
        ).fetchone()
        if row is None:
            _refuse("integrity_error", "Pending record is missing")
        record = TransportRecord.from_payload(self._row_payload(binding, row))
        if (record.partition, record.offset) != (partition, delivered["next_offset"]):
            _refuse("integrity_error", "Pending physical coordinates differ")
        return row, record

    def _retain(self, db: Any, table: str, identity: str, value: dict[str, Any]) -> None:
        existing = self._query(db, "SELECT * FROM {t} WHERE id=%s", (identity,), t=table).fetchone()
        expected = {"id": identity, "payload": canonical(value), "payload_sha256": digest(value)}
        if existing is not None:
            if existing != expected:
                _refuse("integrity_error", "Retained recovery evidence conflicts")
            return
        self._query(
            db,
            "INSERT INTO {t} VALUES (%s,%s,%s)",
            (identity, expected["payload"], expected["payload_sha256"]),
            t=table,
        )

    def retain_request(
        self,
        binding: TransportBinding,
        partition: int,
        expected_inbox_hash: str,
        bundle: SourceBundle,
        request: RuntimeRedriveRequest,
        actor: Actor,
        *,
        failpoint: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        self._authorize(binding, actor, ActorRole.PLANNER)
        with self._transport(binding, write=True) as db:
            row, physical = self._pending(db, binding, partition)
            if row["record_hash"] != expected_inbox_hash:
                raise StoreError(
                    "stale_recovery_target", "The selected pending inbox record differs", 409
                )
            dead = RuntimeDeadLetter.create(binding, physical, expected_inbox_hash)
            value = request.verify(
                dead,
                binding,
                physical,
                expected_inbox_hash,
                bundle,
                operator=actor,
                now=self._now(),
            )
            self._retain(db, "transport_recovery_cases", row["id"], json.loads(dead.payload))
            self._retain(
                db, "transport_recovery_sources", bundle.source_sha256, {"bundle": bundle.payload}
            )
            existing = self._query(
                db,
                "SELECT * FROM {t} WHERE id=%s",
                (value["request_id"],),
                t="transport_recovery_requests",
            ).fetchone()
            expected = {
                "id": value["request_id"],
                "case_id": row["id"],
                "source_id": bundle.source_sha256,
                "payload": request.payload,
                "payload_sha256": digest(value),
            }
            if existing is not None and existing != expected:
                _refuse("integrity_error", "Retained redrive proposal conflicts")
            if existing is None:
                self._query(
                    db,
                    "INSERT INTO {t} VALUES (%s,%s,%s,%s,%s)",
                    tuple(expected.values()),
                    t="transport_recovery_requests",
                )
            if failpoint is not None:
                failpoint()
            return {
                "request_id": value["request_id"],
                "request_sha256": digest(value),
                "duplicate": existing is not None,
                "claim": "retained_proposal_not_delivery",
            }

    @staticmethod
    def _json(row: dict[str, Any] | None) -> dict[str, Any]:
        try:
            if row is None:
                raise ValueError
            value = json.loads(row["payload"])
            if (
                not isinstance(value, dict)
                or canonical(value) != row["payload"]
                or digest(value) != row["payload_sha256"]
            ):
                raise ValueError
            return value
        except (ValueError, TypeError, KeyError):
            _refuse("integrity_error", "Recovery journal payload differs")
        raise AssertionError("unreachable")

    def _load_request(
        self, db: Any, binding: TransportBinding, request_id: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        row = self._query(
            db, "SELECT * FROM {t} WHERE id=%s", (request_id,), t="transport_recovery_requests"
        ).fetchone()
        value = self._json(row)
        assert row is not None
        now = self._now()
        try:
            submitted = datetime.fromisoformat(value["submitted_at"])
            if submitted.tzinfo is None or submitted.utcoffset() is None:
                raise ValueError
        except (ValueError, TypeError, KeyError):
            _refuse("integrity_error", "Recovery submission time differs")
        if submitted > now:
            raise StoreError("invalid_clock", "Clock precedes retained proposal", 409)
        original = self._query(
            db, "SELECT * FROM {t} WHERE id=%s", (row["case_id"],), t="transport_inbox"
        ).fetchone()
        if original is None:
            _refuse("integrity_error", "Recovery inbox evidence is missing")
        physical = TransportRecord.from_payload(self._row_payload(binding, original))
        dead_value = self._json(
            self._query(
                db, "SELECT * FROM {t} WHERE id=%s", (row["case_id"],), t="transport_recovery_cases"
            ).fetchone()
        )
        dead = RuntimeDeadLetter(canonical(dead_value))
        source = self._json(
            self._query(
                db,
                "SELECT * FROM {t} WHERE id=%s",
                (row["source_id"],),
                t="transport_recovery_sources",
            ).fetchone()
        )
        try:
            bundle = SourceBundle(source["bundle"])
            if set(source) != {"bundle"} or bundle.source_sha256 != row["source_id"]:
                raise ValueError
            actor = Actor(
                value["operator_id"],
                "Retained proposer",
                ActorRole(value["operator_role"]),
                tuple(value["locations"]),
            )
            checked = RuntimeRedriveRequest(row["payload"]).verify(
                dead,
                binding,
                physical,
                original["record_hash"],
                bundle,
                operator=actor,
                now=now,
            )
            if checked["request_id"] != row["id"]:
                raise ValueError
        except (ValueError, KeyError, TypeError):
            _refuse("integrity_error", "Recovery proposal semantics differ")
        return row, original

    def _reviews(
        self, db: Any, binding: TransportBinding, request: dict[str, Any], original: dict[str, Any]
    ) -> list[dict[str, Any]]:
        proposal = self._json(request)
        rows = self._query(
            db,
            "SELECT * FROM {t} WHERE request_id=%s ORDER BY sequence LIMIT 3",
            (request["id"],),
            t="transport_recovery_decisions",
        ).fetchall()
        results: list[dict[str, Any]] = []
        previous_sequence = 0
        for row in rows:
            value = self._json(row)
            try:
                actor = Actor(
                    value["actor_id"],
                    "Retained reviewer",
                    ActorRole(value["actor_role"]),
                    tuple(value["locations"]),
                )
                self._authorize(binding, actor, ActorRole.APPROVER)
                fixed = {
                    "contract": "nexus.runtime-recovery-review.v1",
                    "decision_id": row["id"],
                    "request_id": request["id"],
                    "request_sha256": request["payload_sha256"],
                    "case_id": request["case_id"],
                    "inbox_record_hash": original["record_hash"],
                    "actor_role": "approver",
                    "locations": sorted(binding.broker.locations),
                    "claim": "recovery_review_not_delivery_or_business_approval",
                }
                if (
                    set(value)
                    != set(fixed)
                    | {"actor_id", "action", "reason", "ttl_seconds", "created_at", "expires_at"}
                    or any(value[key] != expected for key, expected in fixed.items())
                    or actor.actor_id == proposal["operator_id"]
                    or str(UUID(row["id"])) != row["id"]
                    or row["request_id"] != request["id"]
                    or row["sequence"] <= previous_sequence
                    or type(value["ttl_seconds"]) is not int
                    or not 1 <= value["ttl_seconds"] <= 900
                    or not isinstance(value["reason"], str)
                    or not 1 <= len(value["reason"].strip()) <= 500
                    or value["reason"] != value["reason"].strip()
                    or type(value["created_at"]) not in (int, float)
                    or not math.isfinite(value["created_at"])
                    or value["created_at"]
                    < datetime.fromisoformat(proposal["submitted_at"]).timestamp()
                    or (not results and value["action"] not in {"approve", "reject"})
                    or (
                        results
                        and (
                            len(results) != 1
                            or results[0]["action"] != "approve"
                            or value["action"] != "revoke"
                        )
                    )
                    or (results and value["created_at"] < results[-1]["created_at"])
                    or value["expires_at"]
                    != (
                        value["created_at"] + value["ttl_seconds"]
                        if value["action"] == "approve"
                        else None
                    )
                ):
                    raise ValueError
            except (ValueError, KeyError, TypeError, AttributeError):
                _refuse("integrity_error", "Recovery decision semantics differ")
            if value["created_at"] > self._now().timestamp():
                raise StoreError("invalid_clock", "Clock precedes retained review", 409)
            results.append(value)
            previous_sequence = row["sequence"]
        return results

    def _verify_extension_history(self, db: Any, binding: TransportBinding) -> dict[str, Any]:
        """Reject detached evidence and replay every review in the transport snapshot.

        This local service integrity check grants no human or execution authority.
        Requests are keyset-paged; each has at most two legal review records.
        Expiry is not corruption: past approvals remain valid historical evidence.
        """
        counts = {}
        for suffix in ("cases", "sources", "requests", "decisions"):
            counts[suffix] = self._query(
                db, "SELECT COUNT(*) AS n FROM {t}", t="transport_recovery_" + suffix
            ).fetchone()["n"]
        if binding.purpose != "operations" and any(counts.values()):
            _refuse("integrity_error", "Audit consumers cannot own recovery histories")
        for suffix, reference in (("cases", "case_id"), ("sources", "source_id")):
            orphan = self._query(
                db,
                "SELECT 1 FROM {t} evidence WHERE NOT EXISTS "
                "(SELECT 1 FROM {requests} request WHERE request."
                + reference
                + "=evidence.id) LIMIT 1",
                t="transport_recovery_" + suffix,
                requests="transport_recovery_requests",
            ).fetchone()
            if orphan is not None:
                _refuse("integrity_error", "Recovery journal contains detached evidence")
        sequence = self._query(
            db,
            "SELECT MIN(sequence) AS first, MAX(sequence) AS last FROM {t}",
            t="transport_recovery_decisions",
        ).fetchone()
        if counts["decisions"] and (
            sequence["first"] != 1 or sequence["last"] != counts["decisions"]
        ):
            _refuse("integrity_error", "Recovery decision sequence contains a gap")
        last_id = ""
        checked_requests = checked_decisions = 0
        while True:
            rows = self._query(
                db,
                "SELECT id FROM {t} WHERE id>%s ORDER BY id LIMIT 1000",
                (last_id,),
                t="transport_recovery_requests",
            ).fetchall()
            if not rows:
                break
            for item in rows:
                request, original = self._load_request(db, binding, item["id"])
                checked_decisions += len(self._reviews(db, binding, request, original))
                checked_requests += 1
                last_id = item["id"]
        if checked_requests != counts["requests"] or checked_decisions != counts["decisions"]:
            _refuse("integrity_error", "Recovery journal contains unverified history")
        return {
            "recovery_journal": {
                **counts,
                "claim": "verified_retained_history_not_redrive_authorization",
            }
        }

    def review_status(
        self, binding: TransportBinding, request_id: str, actor: Actor
    ) -> dict[str, Any]:
        """Report review validity, never an execution permit or an acknowledged redrive."""
        self._authorize(binding, actor)
        with self._transport(binding, write=False) as db:
            row, original = self._load_request(db, binding, request_id)
            reviews = self._reviews(db, binding, row, original)
            latest = reviews[-1] if reviews else None
            pointer = self._progress(db, binding, delivery=True)[original["partition"]]
            pending = pointer["next_offset"] == original["broker_offset"]
            reason = "awaiting_review" if latest is None else latest["action"]
            if (
                latest is not None
                and latest["action"] == "approve"
                and self._now().timestamp() >= latest["expires_at"]
            ):
                reason = "expired"
            if not pending:
                reason = "target_not_pending"
            return {
                "request_id": request_id,
                "request_sha256": row["payload_sha256"],
                "decision_id": None if latest is None else latest["decision_id"],
                "reason": reason,
                "review_valid": reason == "approve",
                "claim": "review_status_not_redrive_execution_permission",
            }

    def review_request(
        self,
        binding: TransportBinding,
        request_id: str,
        expected_request_hash: str,
        actor: Actor,
        *,
        decision_id: str,
        action: str,
        reason: str,
        ttl_seconds: int = 900,
        failpoint: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        self._authorize(binding, actor, ActorRole.APPROVER)
        try:
            valid_id = str(UUID(decision_id)) == decision_id
        except (TypeError, ValueError, AttributeError):
            valid_id = False
        if (
            not valid_id
            or action not in {"approve", "reject", "revoke"}
            or not isinstance(reason, str)
            or not 1 <= len(reason.strip()) <= 500
            or type(ttl_seconds) is not int
            or not 1 <= ttl_seconds <= 900
        ):
            raise StoreError(
                "invalid_recovery_decision", "Use a bounded explicit recovery decision", 422
            )
        with self._transport(binding, write=True) as db:
            self._progress(db, binding, delivery=True, lock=True)
            row, original = self._load_request(db, binding, request_id)
            proposal = self._json(row)
            reviews = self._reviews(db, binding, row, original)
            if row["payload_sha256"] != expected_request_hash:
                raise StoreError(
                    "stale_recovery_request", "Review the exact retained proposal", 409
                )
            if proposal["operator_id"] == actor.actor_id:
                raise StoreError(
                    "maker_checker_violation", "Recovery requires independent review", 403
                )
            pending, _ = self._pending(db, binding, original["partition"])
            if pending["id"] != original["id"]:
                raise StoreError(
                    "stale_recovery_target", "Recovery target is no longer pending", 409
                )
            intended = {
                "contract": "nexus.runtime-recovery-review.v1",
                "decision_id": decision_id,
                "request_id": request_id,
                "request_sha256": expected_request_hash,
                "case_id": row["case_id"],
                "inbox_record_hash": original["record_hash"],
                "actor_id": actor.actor_id,
                "actor_role": actor.role.value,
                "locations": sorted(actor.locations),
                "action": action,
                "reason": reason.strip(),
                "ttl_seconds": ttl_seconds,
                "claim": "recovery_review_not_delivery_or_business_approval",
            }
            previous_id = self._query(
                db,
                "SELECT * FROM {t} WHERE id=%s",
                (decision_id,),
                t="transport_recovery_decisions",
            ).fetchone()
            if previous_id is not None:
                old = self._json(previous_id)
                if {
                    k: v for k, v in old.items() if k not in {"created_at", "expires_at"}
                } != intended:
                    raise StoreError(
                        "recovery_decision_conflict", "Decision ID is already bound", 409
                    )
                return {**old, "duplicate": True}
            prior = reviews[-1] if reviews else None
            if (prior is None and action == "revoke") or (
                prior is not None and (prior["action"] != "approve" or action != "revoke")
            ):
                raise StoreError(
                    "invalid_recovery_transition", "Recovery review transition is not allowed", 409
                )
            now = self._now().timestamp()
            if prior is not None and now < prior["created_at"]:
                raise StoreError("invalid_clock", "Review clock precedes retained decision", 409)
            sequence = self._query(
                db,
                "SELECT COALESCE(MAX(sequence),0)+1 AS next FROM {t}",
                t="transport_recovery_decisions",
            ).fetchone()["next"]
            value = {
                **intended,
                "created_at": now,
                "expires_at": now + ttl_seconds if action == "approve" else None,
            }
            self._query(
                db,
                "INSERT INTO {t} VALUES (%s,%s,%s,%s,%s)",
                (decision_id, request_id, sequence, canonical(value), digest(value)),
                t="transport_recovery_decisions",
            )
            if failpoint is not None:
                failpoint()
            return {**value, "duplicate": False}
