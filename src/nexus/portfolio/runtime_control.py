"""Single-host transactional replanning with fenced workers and plan history.

Event receipt and invalidation share the workflow database. Compute occurs outside
the transaction; a lease token and source binding fence stale worker completion.
This is not a production broker, service-identity provider or scientific promotion.
"""

from __future__ import annotations

import json
import re
import secrets
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from nexus.contracts import ActorRole, EventEnvelope
from nexus.policies.replanning import ReplanDisposition, ReplanPolicy
from nexus.portfolio.auth import Actor
from nexus.portfolio.store import (
    RECOMMENDATION_TTL_SECONDS,
    PortfolioStore,
    StoreError,
    canonical,
    digest,
    iso,
)


class RuntimeControl:
    def __init__(self, store: PortfolioStore, *, policy: ReplanPolicy | None = None) -> None:
        self.store = store
        self.policy = policy or ReplanPolicy()
        with store.connection() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS runtime_events (
                  sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
                  event_hash TEXT NOT NULL, payload TEXT NOT NULL, job_id TEXT,
                  received_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS runtime_jobs (
                  id TEXT PRIMARY KEY, source_sha256 TEXT NOT NULL, state TEXT NOT NULL,
                  eligible_at REAL NOT NULL, created_at REAL NOT NULL, base_seed_hash TEXT NOT NULL,
                  attempts INTEGER NOT NULL DEFAULT 0, token TEXT, lease_until REAL,
                  completed_at REAL, result_sha256 TEXT, error TEXT);
                CREATE TABLE IF NOT EXISTS runtime_generations (
                  seed_hash TEXT PRIMARY KEY, payload TEXT NOT NULL, archived_at REAL NOT NULL,
                  replacement_job_id TEXT NOT NULL);
            """)
            db.commit()
        with store.transaction() as db:
            marker = db.execute("SELECT 1 FROM metadata WHERE key='runtime_state_hash'").fetchone()
            if marker is None:
                if any(
                    db.execute(f"SELECT 1 FROM {table}").fetchone()
                    for table in ("runtime_events", "runtime_jobs", "runtime_generations")
                ):
                    raise StoreError("integrity_error", "Runtime integrity marker is missing")
                self._seal(db)
            self._verify(db)

    @staticmethod
    def _state_hash(db: sqlite3.Connection) -> str:
        return digest(
            {
                "bindings": [
                    dict(r)
                    for r in db.execute(
                        "SELECT * FROM metadata WHERE key IN "
                        "('runtime_evidence_path','runtime_ingested_at') ORDER BY key"
                    )
                ],
                "events": [
                    dict(r) for r in db.execute("SELECT * FROM runtime_events ORDER BY sequence")
                ],
                "jobs": [dict(r) for r in db.execute("SELECT * FROM runtime_jobs ORDER BY id")],
                "generations": [
                    dict(r)
                    for r in db.execute("SELECT * FROM runtime_generations ORDER BY seed_hash")
                ],
            }
        )

    def _seal(self, db: sqlite3.Connection) -> None:
        db.execute(
            "INSERT OR REPLACE INTO metadata VALUES ('runtime_state_hash',?)",
            (self._state_hash(db),),
        )

    def _verify(self, db: sqlite3.Connection) -> None:
        expected = db.execute(
            "SELECT value FROM metadata WHERE key='runtime_state_hash'"
        ).fetchone()
        if expected is None or expected[0] != self._state_hash(db):
            raise StoreError("integrity_error", "Runtime journal or projection was changed")

    def _authorize(self, actor: Actor, *, worker: bool = False) -> None:
        if actor.role is not (ActorRole.SYSTEM if worker else ActorRole.PLANNER) or (
            worker and actor.actor_id != "runtime-worker"
        ):
            raise StoreError("role_denied", "This role cannot change runtime planning", 403)
        # Shared budget: a partial-location actor cannot replace a global plan.
        if not set(self.store.locations()).issubset(actor.locations):
            raise StoreError("scope_denied", "Runtime planning requires all plan locations", 403)

    @staticmethod
    def _seed(db: sqlite3.Connection) -> dict[str, Any]:
        values = dict(
            db.execute("SELECT key,value FROM metadata WHERE key IN ('seed','seed_hash')")
        )
        if "seed" not in values or "seed_hash" not in values:
            raise StoreError("not_ready", "Initialize a connected runtime first", 503)
        seed: dict[str, Any] = json.loads(values["seed"])
        if digest(seed) != values["seed_hash"]:
            raise StoreError("integrity_error", "Runtime seed integrity check failed")
        if seed.get("metadata", {}).get("pipeline_mode") != "connected_runtime_baseline":
            raise StoreError("runtime_required", "Frozen demonstration state cannot be replanned")
        return seed

    def _invalidate(self, db: sqlite3.Connection, job_id: str, actor: Actor) -> int:
        rows = db.execute(
            "SELECT * FROM recommendations WHERE state IN ('pending','approved','execution_failed')"
        ).fetchall()
        for row in rows:
            self.store._assert_projection(db, row)
            db.execute("UPDATE recommendations SET state='invalidated' WHERE id=?", (row["id"],))
            db.execute(
                "UPDATE outbox SET state='cancelled' WHERE recommendation_id=?", (row["id"],)
            )
            self.store._event(
                db,
                row["id"],
                "invalidated",
                actor.actor_id,
                actor.role.value,
                {
                    "job_id": job_id,
                    "fingerprint": row["fingerprint"],
                    "reason": "Material source change; replacement requires a new human approval",
                },
            )
        return len(rows)

    def submit(self, event: EventEnvelope, actor: Actor) -> dict[str, Any]:
        """Deduplicate IDs, invalidate immediately, coalesce pending material work."""
        self._authorize(actor)
        now = float(self.store.clock())
        if event.ingested_at > datetime.fromtimestamp(now, UTC):
            raise StoreError("future_event", "An event cannot be received in the future", 422)
        if set(event.payload) != {"source_snapshot_sha256"}:
            raise StoreError("invalid_event", "Bind exactly one source snapshot hash", 422)
        source_hash = event.payload["source_snapshot_sha256"]
        if not isinstance(source_hash, str) or not re.fullmatch(r"[a-f0-9]{64}", source_hash):
            raise StoreError("invalid_event", "Source snapshot hash must be SHA-256", 422)
        payload = event.model_dump(mode="json")
        identity = digest(payload)
        with self.store.transaction() as db:
            self._verify(db)
            self.store._verify_events(db)
            seed = self._seed(db)
            previous = db.execute(
                "SELECT * FROM runtime_events WHERE id=?", (str(event.event_id),)
            ).fetchone()
            if previous:
                if previous["event_hash"] != identity:
                    raise StoreError("event_conflict", "Event ID is already bound to other content")
                return {"duplicate": True, "job_id": previous["job_id"], "invalidated": 0}
            last = db.execute(
                "SELECT MAX(completed_at) FROM runtime_jobs WHERE state='completed'"
            ).fetchone()[0]
            evaluation = self.policy.evaluate(
                event,
                now=datetime.fromtimestamp(now, UTC),
                last_replan_at=datetime.fromtimestamp(last, UTC) if last is not None else None,
            )
            job_id = None
            invalidated = 0
            if evaluation.material:
                queued = db.execute(
                    "SELECT id,source_sha256 FROM runtime_jobs WHERE state='queued' "
                    "ORDER BY created_at,id LIMIT 1"
                ).fetchone()
                running = db.execute(
                    "SELECT source_sha256 FROM runtime_jobs WHERE state='running' "
                    "ORDER BY created_at,id LIMIT 1"
                ).fetchone()
                if source_hash == seed["metadata"].get("source_snapshot_sha256") and (
                    (queued is None or queued["source_sha256"] == source_hash)
                    and (running is None or running["source_sha256"] == source_hash)
                ):
                    raise StoreError(
                        "unchanged_source", "Material replanning requires changed source bytes"
                    )
                # Policy datetimes have microsecond precision; converting an
                # immediate decision back to a binary float can round beyond
                # the injected clock. Immediate means eligible at this exact tick.
                eligible = (
                    now
                    if evaluation.disposition is ReplanDisposition.TRIGGER_NOW
                    else last + self.policy.cooldown_seconds
                    if last is not None
                    else now
                )
                if queued:
                    job_id = queued[0]
                    db.execute(
                        "UPDATE runtime_jobs SET source_sha256=?,eligible_at=? WHERE id=?",
                        (source_hash, eligible, job_id),
                    )
                else:
                    job_id = secrets.token_hex(16)
                    db.execute(
                        "INSERT INTO runtime_jobs(id,source_sha256,state,eligible_at,"
                        "created_at,base_seed_hash) VALUES (?,?,'queued',?,?,?)",
                        (job_id, source_hash, eligible, now, digest(seed)),
                    )
                invalidated = self._invalidate(db, job_id, actor)
            db.execute(
                "INSERT INTO runtime_events(id,event_hash,payload,job_id,received_at) "
                "VALUES (?,?,?,?,?)",
                (str(event.event_id), identity, canonical(payload), job_id, now),
            )
            self._seal(db)
            return {
                "duplicate": False,
                "job_id": job_id,
                "invalidated": invalidated,
                "disposition": evaluation.disposition.value,
            }

    def claim(self, actor: Actor, *, lease_seconds: int = 300) -> dict[str, Any] | None:
        """A reclaimed lease always receives a fresh token; only one worker runs."""
        self._authorize(actor, worker=True)
        if type(lease_seconds) is not int or not 10 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be an integer between 10 and 3600")
        now = float(self.store.clock())
        with self.store.transaction() as db:
            self._verify(db)
            self._seed(db)
            if db.execute(
                "SELECT 1 FROM runtime_jobs WHERE state='running' AND lease_until>?", (now,)
            ).fetchone():
                return None
            row = db.execute(
                "SELECT * FROM runtime_jobs WHERE (state='queued' AND eligible_at<=?) "
                "OR (state='running' AND lease_until<=?) ORDER BY created_at,id LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            token = secrets.token_hex(32)
            db.execute(
                "UPDATE runtime_jobs SET state='running',token=?,lease_until=?,"
                "attempts=attempts+1 WHERE id=?",
                (token, now + lease_seconds, row["id"]),
            )
            self._seal(db)
            return {
                **dict(row),
                "state": "running",
                "token": token,
                "lease_until": now + lease_seconds,
                "attempts": row["attempts"] + 1,
            }

    def _claimed(self, db: sqlite3.Connection, job_id: str, token: str) -> sqlite3.Row:
        self._verify(db)
        row = db.execute("SELECT * FROM runtime_jobs WHERE id=?", (job_id,)).fetchone()
        if (
            row is None
            or row["state"] != "running"
            or row["token"] != token
            or self.store.clock() >= row["lease_until"]
        ):
            raise StoreError("stale_worker", "Worker lease expired or was replaced")
        return cast(sqlite3.Row, row)

    def fail(self, job_id: str, token: str, actor: Actor, *, reason: str) -> None:
        self._authorize(actor, worker=True)
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", reason):
            raise ValueError("reason must be a bounded error classification")
        with self.store.transaction() as db:
            self._claimed(db, job_id, token)
            db.execute(
                "UPDATE runtime_jobs SET state='failed',error=?,token=NULL WHERE id=?",
                (reason, job_id),
            )
            self._seal(db)

    def activate(
        self,
        job_id: str,
        token: str,
        seed: dict[str, Any],
        actor: Actor,
        *,
        evidence_dir: Path | None = None,
    ) -> dict[str, Any]:
        """Archive old evidence and insert a new, unapproved generation atomically.

        The local worker must supply output of the checksum-verifying runtime
        builder. This method is deliberately not a public HTTP artifact-upload API.
        """
        self._authorize(actor, worker=True)
        evidence_path = None
        if evidence_dir is not None:
            root = self.store.path.parent.resolve()
            resolved = evidence_dir.resolve()
            if not resolved.is_relative_to(root):
                raise StoreError("invalid_path", "Evidence must stay inside this runtime directory")
            evidence_path = resolved.relative_to(root).as_posix()
        metadata = seed.get("metadata", {})
        if (
            metadata.get("pipeline_mode") != "connected_runtime_baseline"
            or not seed.get("recommendations")
            or seed.get("executable") is False
        ):
            raise StoreError(
                "non_executable_plan", "Only a verified executable runtime plan can activate"
            )
        if not {row["location_id"] for row in seed["recommendations"]}.issubset(actor.locations):
            raise StoreError("scope_denied", "Replacement includes an unauthorized location", 403)
        now = float(self.store.clock())
        with self.store.transaction() as db:
            self.store._verify_events(db)
            job = self._claimed(db, job_id, token)
            old = self._seed(db)
            if metadata.get("source_snapshot_sha256") != job["source_sha256"]:
                raise StoreError(
                    "source_mismatch", "Computed plan does not match the queued source snapshot"
                )
            latest = db.execute(
                "SELECT job_id,payload FROM runtime_events WHERE job_id IS NOT NULL "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            if latest and (
                latest["job_id"] != job_id
                or json.loads(latest["payload"])["payload"]["source_snapshot_sha256"]
                != job["source_sha256"]
            ):
                raise StoreError("superseded_job", "A newer material event superseded this plan")
            if digest(old) != job["base_seed_hash"]:
                raise StoreError("superseded_job", "Active generation changed during computation")
            replacement = json.loads(canonical(seed))
            replacement["metadata"]["runtime_job_id"] = job_id
            for item in replacement["recommendations"]:
                item["source_recommendation_id"] = item["id"]
                item["id"] = f"{job_id}-{item['id']}"
            # The builder's self-hash describes its immutable artifact, not this
            # workflow projection. Preserve that identity separately.
            if "seed_hash" in replacement:
                replacement["metadata"]["runtime_artifact_seed_hash"] = replacement.pop("seed_hash")
            db.execute(
                "INSERT OR IGNORE INTO runtime_generations VALUES (?,?,?,?)",
                (digest(old), canonical(old), now, job_id),
            )
            self._invalidate(db, job_id, actor)
            for item in replacement["recommendations"]:
                payload = {k: v for k, v in item.items() if k not in {"status", "state"}}
                payload.update(
                    created_at=iso(now), valid_until=iso(now + RECOMMENDATION_TTL_SECONDS)
                )
                fingerprint = digest(payload)
                db.execute(
                    "INSERT INTO recommendations(id,payload,fingerprint,location_id,creator,"
                    "state,created_at,valid_until) VALUES (?,?,?,?,?,'pending',?,?)",
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
                self.store._event(
                    db,
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
            result_hash = digest(replacement)
            db.execute("UPDATE metadata SET value=? WHERE key='seed'", (canonical(replacement),))
            db.execute("UPDATE metadata SET value=? WHERE key='seed_hash'", (result_hash,))
            if evidence_path is not None:
                db.execute(
                    "INSERT OR REPLACE INTO metadata VALUES ('runtime_evidence_path',?)",
                    (evidence_path,),
                )
            db.execute(
                "INSERT OR REPLACE INTO metadata VALUES ('runtime_ingested_at',?)", (iso(now),)
            )
            db.execute(
                "UPDATE runtime_jobs SET state='completed',completed_at=?,"
                "result_sha256=?,token=NULL WHERE id=?",
                (now, result_hash, job_id),
            )
            self._seal(db)
            return {
                "job_id": job_id,
                "seed_sha256": result_hash,
                "recommendations": len(replacement["recommendations"]),
                "human_approval_required": True,
            }

    def snapshot(self) -> dict[str, Any]:
        with self.store.connection() as db:
            db.execute("BEGIN")
            self._verify(db)
            # Operational state comes from all durable rows, never the bounded
            # display page. A later complete global plan resolves prior failures.
            latest_completed_event = db.execute(
                "SELECT COALESCE(MAX(e.sequence),0) FROM runtime_events e "
                "JOIN runtime_jobs j ON j.id=e.job_id WHERE j.state='completed'"
            ).fetchone()[0]
            all_jobs = db.execute(
                "SELECT j.id,j.state,j.lease_until,COALESCE(MAX(e.sequence),0) AS event_sequence "
                "FROM runtime_jobs j LEFT JOIN runtime_events e ON e.job_id=j.id "
                "GROUP BY j.id,j.state,j.lease_until"
            ).fetchall()
            operational = {
                "queued": sum(row["state"] == "queued" for row in all_jobs),
                "running": sum(row["state"] == "running" for row in all_jobs),
                "lease_expired": sum(
                    row["state"] == "running" and row["lease_until"] <= self.store.clock()
                    for row in all_jobs
                ),
                "failed_unresolved": sum(
                    row["state"] == "failed" and row["event_sequence"] > latest_completed_event
                    for row in all_jobs
                ),
                "failed_historical": sum(
                    row["state"] == "failed" and row["event_sequence"] <= latest_completed_event
                    for row in all_jobs
                ),
                "completed": sum(row["state"] == "completed" for row in all_jobs),
            }
            jobs = [
                dict(row)
                for row in db.execute(
                    "SELECT id,source_sha256,state,eligible_at,created_at,attempts,"
                    "completed_at,result_sha256,error FROM runtime_jobs "
                    "ORDER BY created_at DESC,id LIMIT 50"
                )
            ]
            return {
                "jobs": jobs,
                "operational": operational,
                "event_count": db.execute("SELECT COUNT(*) FROM runtime_events").fetchone()[0],
                "archived_generations": db.execute(
                    "SELECT COUNT(*) FROM runtime_generations"
                ).fetchone()[0],
                "adapter": "single_host_sqlite",
                "checked_at": iso(self.store.clock()),
            }
