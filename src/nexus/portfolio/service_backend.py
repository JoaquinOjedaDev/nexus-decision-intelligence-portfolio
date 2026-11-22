"""Explicit service views over SQLite or the verified PostgreSQL v4 facade.

No SQL-compatibility shim is provided. A guarded view holds the workflow writer
lock while a caller commits a local sidecar, always in workflow -> sidecar order.
This excludes concurrent generation activation; it is not a distributed commit.
Callers must not open another workflow transaction while retaining that guard.
Actor-less views are a trusted internal boundary, not an anonymous API capability.
"""

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from nexus.contracts import ActorRole
from nexus.policies.authorization import Permission
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_backend import PostgresReadSession, _dsn
from nexus.portfolio.postgres_runtime import PostgresRuntimeControl, PostgresRuntimeStore
from nexus.portfolio.runtime_control import RuntimeControl
from nexus.portfolio.store import PortfolioStore, StoreError, canonical, digest, iso

OperationalStore = PortfolioStore | PostgresRuntimeStore
_SHA = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class RuntimeView:
    seed: dict[str, Any]
    seed_sha256: str
    retained_locations: frozenset[str]
    runtime_ingested_at: str | None
    evidence_dir: Path | None
    checked_at: float
    runtime_identity: str


def _local(path: Path) -> Path:
    if not isinstance(path, Path) or path.anchor.startswith(("\\\\", "//")) or ".." in path.parts:
        raise StoreError("invalid_path", "State requires a local path without traversal")
    absolute = path.absolute()
    for item in (absolute, *absolute.parents):
        if item.is_symlink() or item.is_junction():
            raise StoreError("invalid_path", "Linked state paths are not allowed")
    return absolute.resolve()


def state_root(store: OperationalStore) -> Path:
    """Return the safe physical sidecar root, without creating files or directories."""
    if isinstance(store, PostgresRuntimeStore):
        bind_service_root(store)
        return _local(store.runtime_root)
    if isinstance(store, PortfolioStore):
        return _local(store.path).parent
    raise TypeError("An explicit supported operational store is required")


def backend_label(store: OperationalStore) -> str:
    if isinstance(store, PostgresRuntimeStore):
        return "PostgreSQL"
    if isinstance(store, PortfolioStore):
        return "SQLite"
    raise TypeError("An explicit supported operational store is required")


def _identity(store: OperationalStore, root: Path) -> str:
    if isinstance(store, PostgresRuntimeStore):
        route = _dsn(store.config.dsn)
        # Credentials and application-role rotations must not change ownership.
        return digest(
            {
                "backend": "postgresql",
                "root": root.as_posix(),
                "schema": store.config.schema,
                "host": route["hostaddr"],
                "port": int(route["port"]),
                "database": route["dbname"],
            }
        )
    return digest({"backend": "sqlite", "database": _local(store.path).as_posix()})


def bind_service_root(store: OperationalStore, *, claim: bool = False) -> None:
    """Fence every sidecar to one PostgreSQL route/schema, without changing SQLite.

    Explicit preparation/opening may claim an unused root. Partial claims fail
    closed and are retained. Existing unbound journals are never adopted.
    This is an ownership check, not protection against a privileged administrator.
    """
    if not isinstance(store, PostgresRuntimeStore):
        return
    root = _local(store.runtime_root)
    if root == Path(root.anchor):
        raise StoreError("invalid_path", "Service state requires a bounded directory")
    marker = root / ".nexus-service-root.json"
    _local(marker)
    expected = canonical(
        {
            "contract": "nexus.service-root.v1",
            "runtime_identity": _identity(store, root),
        }
    ).encode("utf-8")

    def verify() -> None:
        _local(marker)
        try:
            with marker.open("rb") as handle:
                actual = handle.read(len(expected) + 1)
        except OSError:
            raise StoreError(
                "service_root_unavailable", "Service root binding is unavailable"
            ) from None
        if actual != expected:
            raise StoreError(
                "service_root_conflict",
                "Service directory belongs to another runtime or has an invalid binding",
            )

    if marker.exists():
        verify()
        return
    if not claim:
        raise StoreError(
            "service_root_unbound", "Explicitly bind the PostgreSQL service directory before use"
        )
    for name in (
        "portfolio.sqlite3",
        "alerts.sqlite3",
        "feedback.sqlite3",
        "feedback-monitoring.sqlite3",
        "monitoring.sqlite3",
        "learning-attachments.sqlite3",
    ):
        for suffix in ("", "-wal", "-shm", "-journal"):
            path = root / (name + suffix)
            _local(path)
            if path.exists():
                raise StoreError(
                    "service_root_unbound",
                    "Existing unbound journals require a separate reviewed migration",
                )
    try:
        root.mkdir(parents=True, exist_ok=True)
        _local(root)
        descriptor = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        verify()
        return
    except OSError:
        raise StoreError("service_root_unavailable", "Service root claim is unavailable") from None
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(expected)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError:
        # Do not erase an ambiguous partial claim.
        raise StoreError(
            "service_root_unavailable", "Service root claim did not complete safely"
        ) from None
    verify()


def _permission(actor: Actor | None) -> None:
    if actor is not None:
        if not isinstance(actor, Actor):
            raise StoreError("role_denied", "An authenticated actor is required", 403)
        PortfolioStore._permission(actor, Permission.VIEW_AUDIT)


def _expected(value: str | None) -> None:
    if value is not None and (not isinstance(value, str) or not _SHA.fullmatch(value)):
        raise StoreError("invalid_seed_hash", "Expected generation must be a SHA-256", 422)


def _clock(store: OperationalStore) -> float:
    try:
        value = float(store.clock())
    except (TypeError, ValueError, OverflowError):
        raise StoreError("invalid_clock", "A finite service clock is required") from None
    if not math.isfinite(value):
        raise StoreError("invalid_clock", "A finite service clock is required")
    return value


def _locations(generations: list[dict[str, Any]]) -> frozenset[str]:
    locations: set[str] = set()
    for seed in generations:
        for key in ("recommendations", "forecasts"):
            items = seed.get(key, [])
            if not isinstance(items, list):
                raise StoreError("integrity_error", "Retained plan scope is invalid")
            for item in items:
                location = item.get("location_id") if isinstance(item, dict) else None
                if not isinstance(location, str) or not location:
                    raise StoreError("integrity_error", "Retained plan scope is invalid")
                locations.add(location)
    return frozenset(locations)


def _evidence(root: Path, reference: str | None) -> Path | None:
    if reference is None:
        return None
    relative = Path(reference)
    if (
        not reference
        or relative.is_absolute()
        or relative.drive
        or relative.as_posix() != reference
    ):
        raise StoreError("invalid_path", "Evidence reference must be a relative canonical path")
    path = _local(root / relative)
    if not path.is_relative_to(root) or not path.is_dir():
        raise StoreError("invalid_path", "Evidence must remain in the runtime state directory")
    return path


def _finish_view(
    store: OperationalStore,
    seed: dict[str, Any],
    identity: str,
    generations: list[dict[str, Any]],
    metadata: dict[str, str],
    checked_at: float,
    actor: Actor | None,
    expected: str | None,
) -> RuntimeView:
    # Recheck human permission under the same guard/snapshot as the scope check.
    _permission(actor)
    locations = _locations(generations)
    if actor is not None and not locations.issubset(actor.locations):
        raise StoreError("scope_denied", "Service evidence requires all retained locations", 403)
    if expected is not None and identity != expected:
        raise StoreError("generation_changed", "The active generation changed before access")
    ingested = metadata.get("runtime_ingested_at")
    if ingested is not None:
        try:
            watermark = datetime.fromisoformat(ingested)
            if watermark.tzinfo is None or not math.isfinite(watermark.timestamp()):
                raise ValueError
        except (TypeError, ValueError, OverflowError):
            raise StoreError("integrity_error", "Retained ingestion timestamp is invalid") from None
    root = state_root(store)
    return RuntimeView(
        seed,
        identity,
        locations,
        ingested,
        _evidence(root, metadata.get("runtime_evidence_path")),
        checked_at,
        _identity(store, root),
    )


def _sqlite_view(
    store: PortfolioStore,
    db: sqlite3.Connection,
    actor: Actor | None,
    expected: str | None,
) -> RuntimeView:
    checked_at = _clock(store)
    events = store._verify_events(db)
    pristine = _pristine_initial(db)
    if pristine:
        seed, identity, owners = pristine
    else:
        seed, identity, owners = store._generation_catalog(db)
    records = db.execute("SELECT id,location_id FROM recommendations ORDER BY id").fetchall()
    if {row["id"] for row in records} != set(owners):
        raise StoreError("integrity_error", "Recommendation generation ownership is incomplete")
    observer = Actor(
        "service-view",
        "Internal verification",
        ActorRole.AUDITOR,
        tuple(row["location_id"] for row in records),
    )
    for record in records:
        row = store._row(db, record["id"], observer)
        store._assert_generation_item(row, owners[row["id"]][2])
        if pristine:
            created = [
                event
                for event in events
                if event["recommendation_id"] == row["id"]
                and event["event_type"] == "recommendation_created"
            ]
            if len(created) != 1 or (
                created[0]["actor_id"] != row["creator"]
                or created[0]["actor_role"] != "planner"
                or created[0]["details"]
                != {"fingerprint": row["fingerprint"], "source": "connected runtime baseline"}
            ):
                raise StoreError("integrity_error", "Initial generation creation is invalid")
    if pristine and any(
        event["recommendation_id"] not in owners
        or event["event_type"] == "invalidated"
        or "job_id" in event["details"]
        for event in events
    ):
        raise StoreError("integrity_error", "Initial generation contains runtime job history")
    generations = [seed]
    if (
        not pristine
        and seed.get("metadata", {}).get("pipeline_mode") == "connected_runtime_baseline"
    ):
        generations.extend(
            json.loads(row["payload"])
            for row in db.execute("SELECT payload FROM runtime_generations ORDER BY seed_hash")
        )
    metadata = dict(
        db.execute(
            "SELECT key,value FROM metadata WHERE key IN "
            "('runtime_ingested_at','runtime_evidence_path')"
        )
    )
    return _finish_view(store, seed, identity, generations, metadata, checked_at, actor, expected)


def _pristine_initial(
    db: sqlite3.Connection,
) -> tuple[dict[str, Any], str, dict[str, tuple[str, dict[str, Any], dict[str, Any]]]] | None:
    """Read legacy initial seed-only state without installing a runtime controller.

    Any durable runtime footprint disables this compatibility path. In
    particular, a missing seal on an existing runtime must never be adopted.
    This is workflow verification, not a claim about scientific artifact bytes.
    """
    values = dict(db.execute("SELECT key,value FROM metadata WHERE key IN ('seed','seed_hash')"))
    if set(values) != {"seed", "seed_hash"}:
        return None
    seed = json.loads(values["seed"])
    if seed.get("metadata", {}).get("pipeline_mode") != "connected_runtime_baseline":
        return None
    if (
        db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name GLOB 'runtime_*'"
        ).fetchone()
        or db.execute("SELECT 1 FROM metadata WHERE key GLOB 'runtime_*'").fetchone()
    ):
        return None
    if digest(seed) != values["seed_hash"]:
        raise StoreError("integrity_error", "Initial seed identity mismatch")
    if {"runtime_job_id", "runtime_artifact_seed_hash"}.intersection(seed["metadata"]) or any(
        "source_recommendation_id" in item for item in seed["recommendations"]
    ):
        raise StoreError("integrity_error", "Initial seed contains runtime projection ownership")
    owners = {item["id"]: (values["seed_hash"], seed, item) for item in seed["recommendations"]}
    if len(owners) != len(seed["recommendations"]):
        raise StoreError("integrity_error", "Initial recommendation ownership is ambiguous")
    return seed, values["seed_hash"], owners


def _postgres_view(
    store: PostgresRuntimeStore,
    unit: PostgresReadSession,
    actor: Actor | None,
    expected: str | None,
) -> RuntimeView:
    checked_at = _clock(store)
    snapshot = unit.snapshot()
    seed = snapshot["seed"]
    if seed is None:
        raise StoreError("not_ready", "Runtime initialization has not completed", 503)
    generations = [seed, *(json.loads(row["payload"]) for row in snapshot["runtime_generations"])]
    metadata = {
        row["key"]: row["value"]
        for row in unit._query(
            "SELECT key,value FROM {table} WHERE key IN "
            "('runtime_ingested_at','runtime_evidence_path')",
            table="metadata",
        ).fetchall()
    }
    return _finish_view(
        store, seed, snapshot["seed_sha256"], generations, metadata, checked_at, actor, expected
    )


@contextmanager
def _sqlite_context(
    store: PortfolioStore,
    *,
    write: bool,
    actor: Actor | None,
    expected: str | None,
) -> Iterator[tuple[sqlite3.Connection, RuntimeView]]:
    context = store.transaction() if write else store.connection()
    with context as db:
        if not write:
            db.execute("BEGIN")
            # BEGIN is deferred in SQLite: establish its read snapshot before
            # capturing the clock so the selected generation cannot be newer.
            db.execute("SELECT value FROM metadata WHERE key='seed_hash'").fetchone()
        try:
            view = _sqlite_view(store, db, actor, expected)
        except (
            KeyError,
            TypeError,
            ValueError,
            AttributeError,
            OverflowError,
            sqlite3.DatabaseError,
        ) as exc:
            if isinstance(exc, StoreError):
                raise
            raise StoreError("integrity_error", "Retained workflow evidence is invalid") from None
        yield db, view


@contextmanager
def guarded_view(
    store: OperationalStore,
    *,
    actor: Actor | None = None,
    expected_seed_sha256: str | None = None,
) -> Iterator[RuntimeView]:
    """Hold the writer guard through the caller's sidecar commit, without exposing SQL."""
    _permission(actor)
    _expected(expected_seed_sha256)
    state_root(store)
    if isinstance(store, PostgresRuntimeStore):
        with store.write_transaction() as unit:
            yield _postgres_view(store, unit, actor, expected_seed_sha256)
    else:
        with _sqlite_context(store, write=True, actor=actor, expected=expected_seed_sha256) as (
            _,
            view,
        ):
            yield view


@contextmanager
def read_view(
    store: OperationalStore,
    *,
    actor: Actor | None = None,
    expected_seed_sha256: str | None = None,
) -> Iterator[RuntimeView]:
    """Read one consistent generation; this does not fence a later sidecar write."""
    _permission(actor)
    _expected(expected_seed_sha256)
    state_root(store)
    if isinstance(store, PostgresRuntimeStore):
        with store.read_snapshot() as unit:
            yield _postgres_view(store, unit, actor, expected_seed_sha256)
    else:
        with _sqlite_context(store, write=False, actor=actor, expected=expected_seed_sha256) as (
            _,
            view,
        ):
            yield view


def runtime_control(store: OperationalStore) -> RuntimeControl | PostgresRuntimeControl:
    """Explicit controller construction; invoke outside an existing workflow guard."""
    state_root(store)
    return (
        PostgresRuntimeControl(store)
        if isinstance(store, PostgresRuntimeStore)
        else RuntimeControl(store)
    )


def runtime_status(store: OperationalStore, actor: Actor) -> dict[str, Any]:
    if actor is None:
        raise StoreError("role_denied", "An authenticated actor is required", 403)
    _permission(actor)
    state_root(store)
    if isinstance(store, PostgresRuntimeStore):
        return PostgresRuntimeControl(store).snapshot(actor)
    # A status read must not initialize tables or a control seal. Read the
    # existing SQLite control projection in this same scoped, verified snapshot.
    with _sqlite_context(store, write=False, actor=actor, expected=None) as (db, view):
        if view.seed.get("metadata", {}).get("pipeline_mode") != "connected_runtime_baseline":
            raise StoreError("runtime_required", "Runtime status requires connected planning")
        if db.execute("SELECT 1 FROM metadata WHERE key='runtime_state_hash'").fetchone() is None:
            raise StoreError(
                "runtime_not_initialized", "Runtime control has not been initialized", 503
            )
        jobs = [
            dict(row)
            for row in db.execute(
                "SELECT id,source_sha256,state,eligible_at,created_at,attempts,lease_until,"
                "completed_at,result_sha256,error FROM runtime_jobs ORDER BY created_at DESC,id"
            )
        ]
        sequences = {
            row["job_id"]: row["last_sequence"]
            for row in db.execute(
                "SELECT job_id,MAX(sequence) AS last_sequence FROM runtime_events "
                "WHERE job_id IS NOT NULL GROUP BY job_id"
            )
        }
        completed = max(
            (sequences.get(job["id"], 0) for job in jobs if job["state"] == "completed"), default=0
        )
        operational = {
            state: sum(job["state"] == state for job in jobs)
            for state in ("queued", "running", "completed")
        }
        operational.update(
            lease_expired=sum(
                job["state"] == "running" and job["lease_until"] <= view.checked_at for job in jobs
            ),
            failed_unresolved=sum(
                job["state"] == "failed" and sequences.get(job["id"], 0) > completed for job in jobs
            ),
            failed_historical=sum(
                job["state"] == "failed" and sequences.get(job["id"], 0) <= completed
                for job in jobs
            ),
        )
        return {
            "jobs": [
                {key: value for key, value in row.items() if key != "lease_until"}
                for row in jobs[:50]
            ],
            "operational": operational,
            "event_count": db.execute("SELECT COUNT(*) FROM runtime_events").fetchone()[0],
            "archived_generations": db.execute(
                "SELECT COUNT(*) FROM runtime_generations"
            ).fetchone()[0],
            "adapter": "single_host_sqlite",
            "checked_at": iso(view.checked_at),
        }


def metrics(store: OperationalStore, actor: Actor) -> dict[str, Any]:
    if actor is None:
        raise StoreError("role_denied", "An authenticated actor is required", 403)
    _permission(actor)
    state_root(store)
    if isinstance(store, PostgresRuntimeStore):
        return store.metrics_snapshot(actor)
    with _sqlite_context(store, write=False, actor=actor, expected=None) as (db, view):
        return {
            "states": dict(db.execute("SELECT state,COUNT(*) FROM recommendations GROUP BY state")),
            "outbox": dict(db.execute("SELECT state,COUNT(*) FROM outbox GROUP BY state")),
            "effect_count": db.execute("SELECT COUNT(*) FROM effects").fetchone()[0],
            "event_count": db.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "retry_count": db.execute(
                "SELECT COALESCE(SUM(MAX(attempts-1,0)),0) FROM outbox"
            ).fetchone()[0],
            "database": "sqlite",
            "persistence": "local WAL",
            "checked_at": iso(view.checked_at),
        }
