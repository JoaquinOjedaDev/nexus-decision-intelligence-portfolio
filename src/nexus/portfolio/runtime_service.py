"""Local CLI/API wiring for the connected runtime, without resetting frozen state."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.capture_health_ledger import CaptureHealthLedger
from nexus.portfolio.postgres_backend import PostgresConfig
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.recovery_ledger import RecoveryTransportLedger
from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger
from nexus.portfolio.runtime_control import RuntimeControl
from nexus.portfolio.runtime_feedback import retain_runtime_forecasts
from nexus.portfolio.runtime_pipeline import (
    RuntimePipelineError,
    build_runtime_seed,
    runtime_source_fingerprint,
    verify_runtime_current,
)
from nexus.portfolio.service_backend import (
    OperationalStore,
    bind_service_root,
    read_view,
    runtime_control,
    state_root,
)
from nexus.portfolio.source_watch_runtime import SourceWatchRuntimeStore
from nexus.portfolio.store import PortfolioStore, StoreError, canonical, digest, iso
from nexus.portfolio.transport_ledger import TransportLedger
from nexus.portfolio.transport_runtime import TransportFenceConfig, TransportRuntimeStore


def verify_runtime_store(project_root: Path, store: OperationalStore) -> dict[str, Any]:
    """Verify artifact bytes/current scientific code and the active workflow projection."""
    stored: dict[str, Any]
    if isinstance(store, PostgresRuntimeStore):
        with read_view(store) as view:
            stored = view.seed
            evidence_dir = view.evidence_dir
        if evidence_dir is None:
            raise StoreError(
                "runtime_evidence_missing", "Runtime evidence reference is missing", 503
            )
        verified = verify_runtime_current(project_root, evidence_dir)
        _compare_producer(stored, verified)
        return stored
    control = RuntimeControl(store)
    with store.connection() as db:
        db.execute("BEGIN")
        control._verify(db)
        metadata = dict(
            db.execute(
                "SELECT key,value FROM metadata WHERE key IN "
                "('seed','seed_hash','runtime_evidence_path')"
            )
        )
        if "seed" not in metadata or "seed_hash" not in metadata:
            raise StoreError("not_ready", "Runtime seed is missing", 503)
        stored = json.loads(metadata["seed"])
        if digest(stored) != metadata["seed_hash"]:
            raise StoreError("integrity_error", "Stored runtime seed was changed")
        reference = metadata.get("runtime_evidence_path")
    if reference is None:
        raise StoreError("runtime_evidence_missing", "Runtime evidence reference is missing", 503)
    root = store.path.parent.resolve()
    evidence_dir = (root / reference).resolve()
    if not evidence_dir.is_relative_to(root):
        raise StoreError("invalid_path", "Runtime evidence escaped its state directory")
    verified = verify_runtime_current(project_root, evidence_dir)
    _compare_producer(stored, verified)
    store.metrics_snapshot()
    return stored


def _compare_producer(stored: dict[str, Any], verified: dict[str, Any]) -> None:
    """Reconcile job-prefixed workflow identity with the immutable producer seed."""
    normalized = json.loads(canonical(stored))
    metadata = normalized["metadata"]
    if metadata.pop("runtime_job_id", None) is not None:
        normalized["seed_hash"] = metadata.pop("runtime_artifact_seed_hash")
        for item in normalized["recommendations"]:
            item["id"] = item.pop("source_recommendation_id")
    if digest(normalized) != digest(verified):
        raise StoreError("integrity_error", "Active plan is not bound to its verified artifacts")


def _postgres_store(
    state_dir: Path, config: PostgresConfig, *, transport_fence: TransportFenceConfig | None = None
) -> PostgresRuntimeStore:
    candidate = state_dir.absolute()
    if (
        candidate == Path(candidate.anchor)
        or candidate.anchor.startswith(("\\\\", "//"))
        or ".." in candidate.parts
        or any(path.is_symlink() or path.is_junction() for path in (*candidate.parents, candidate))
    ):
        raise StoreError("invalid_path", "Use a separate explicit local runtime directory")
    root = candidate.resolve()
    if config.artifact_root != root / "evidence" / "initial":
        raise StoreError("configuration_error", "PostgreSQL initial evidence does not match state")
    if (root / "portfolio.sqlite3").exists():
        raise StoreError("backend_conflict", "Existing SQLite state must use a separate directory")
    if transport_fence is None:
        return PostgresRuntimeStore(config, runtime_root=root)
    ledger_type: type[TransportLedger] = (
        RecoveryResolutionLedger
        if transport_fence.recovery_resolution
        else RecoveryTransportLedger
        if transport_fence.recovery_journal
        else TransportLedger
    )
    ledger = (
        ledger_type(transport_fence.ledger)
        if transport_fence.capture_health_max_age_seconds is None
        else CaptureHealthLedger(
            transport_fence.ledger,
            health_binding=transport_fence.binding,
            max_age_seconds=transport_fence.capture_health_max_age_seconds,
        )
    )
    if transport_fence.source_watch is not None:
        result: TransportRuntimeStore = SourceWatchRuntimeStore(
            config,
            runtime_root=root,
            transport=ledger,
            binding=transport_fence.binding,
            watch=transport_fence.source_watch,
        )
    else:
        store_type = (
            PublicationRuntimeStore
            if transport_fence.require_source_publication
            else TransportRuntimeStore
        )
        result = store_type(
            config, runtime_root=root, transport=ledger, binding=transport_fence.binding
        )
    if isinstance(ledger, RecoveryResolutionLedger):
        assert isinstance(result, PublicationRuntimeStore)
        ledger.publication = result
    return result


def load_postgres_runtime(
    project_root: Path,
    state_dir: Path,
    config: PostgresConfig,
    *,
    transport_fence: TransportFenceConfig | None = None,
) -> PostgresRuntimeStore:
    """Open prepared state only: no migration, source computation or SQLite fallback."""
    store = _postgres_store(state_dir, config, transport_fence=transport_fence)
    if not store.has_seed():
        raise StoreError("not_ready", "Prepare the PostgreSQL runtime before serving it", 503)
    bind_service_root(store, claim=True)
    captured = verify_runtime_store(project_root, store)
    # Verification used one captured generation outside the writer transaction.
    # Serving a changed generation needs its own current-code verification.
    with read_view(store, expected_seed_sha256=digest(captured)):
        pass
    retain_runtime_forecasts(store)
    return store


def prepare_postgres_runtime(
    project_root: Path,
    state_dir: Path,
    config: PostgresConfig,
    *,
    source_dir: Path | None = None,
    planning_cutoff: datetime | None = None,
) -> PostgresRuntimeStore:
    """Explicit CLI preparation against an already migrated least-privilege schema."""
    store = _postgres_store(state_dir, config)
    if store.has_seed():
        loaded = load_postgres_runtime(project_root, state_dir, config)
        seed = loaded.seed()
        if (
            source_dir is not None
            and runtime_source_fingerprint(source_dir) != seed["metadata"]["source_snapshot_sha256"]
        ):
            raise StoreError(
                "source_changed", "Submit a material event and run the worker to replan"
            )
        if (
            planning_cutoff is not None
            and planning_cutoff.isoformat() != seed["metadata"]["scientific_decision_time"]
        ):
            raise StoreError(
                "cutoff_changed", "Existing scientific cutoff cannot be silently changed"
            )
        return loaded
    evidence = config.artifact_root
    bind_service_root(store, claim=True)
    # A crash before the seed commit may have left complete producer evidence.
    # Its actual ingestion completion time is not durably bound to this schema.
    # Do not relabel old evidence as fresh or overwrite a retained failed run.
    if evidence.exists() and any(evidence.iterdir()):
        raise StoreError(
            "incomplete_preparation",
            "Unseeded evidence is retained; use a fresh explicit runtime/schema for a new build",
        )
    seed = build_runtime_seed(project_root, evidence, source_dir, planning_cutoff=planning_cutoff)
    if seed.get("executable") is not True:
        raise StoreError("non_executable_plan", "Planning failed; inspect retained evidence", 503)
    _compare_producer(seed, verify_runtime_current(project_root, evidence))
    store.ensure_seed(seed)
    retain_runtime_forecasts(store)
    return store


def prepare_runtime(
    project_root: Path,
    state_dir: Path,
    *,
    source_dir: Path | None = None,
    planning_cutoff: datetime | None = None,
) -> PortfolioStore:
    """Initialize only an empty state; restarting never replaces existing advice."""
    store = PortfolioStore(state_dir)
    if store.has_seed():
        if store.seed().get("metadata", {}).get("pipeline_mode") != "connected_runtime_baseline":
            raise StoreError(
                "runtime_disabled",
                "Choose a separate connected runtime; frozen state is preserved",
                409,
            )
        # Recover a crash after the seed commit but before its initial artifact
        # reference, only after proving exact equality with immutable evidence.
        with store.connection() as db:
            reference = db.execute(
                "SELECT 1 FROM metadata WHERE key='runtime_evidence_path'"
            ).fetchone()
        if reference is None:
            initial = verify_runtime_current(project_root, state_dir / "evidence" / "initial")
            if digest(initial) != digest(store.seed()):
                raise StoreError("integrity_error", "Cannot recover mismatched initial evidence")
            with store.transaction() as db:
                db.execute(
                    "INSERT INTO metadata VALUES ('runtime_evidence_path',?)", ("evidence/initial",)
                )
                # The ingestion completion time was lost in this crash window;
                # unknown freshness is safer than inventing a recent timestamp.
        seed = verify_runtime_store(project_root, store)
        if (
            source_dir is not None
            and runtime_source_fingerprint(source_dir) != seed["metadata"]["source_snapshot_sha256"]
        ):
            raise StoreError(
                "source_changed", "Submit a material event and run the worker to replan"
            )
        if (
            planning_cutoff is not None
            and planning_cutoff.isoformat() != seed["metadata"]["scientific_decision_time"]
        ):
            raise StoreError(
                "cutoff_changed", "Existing scientific cutoff cannot be silently changed"
            )
        retain_runtime_forecasts(store)
        return store
    evidence_dir = state_dir / "evidence" / "initial"
    seed = build_runtime_seed(
        project_root, evidence_dir, source_dir, planning_cutoff=planning_cutoff
    )
    if not seed["executable"]:
        raise StoreError(
            "non_executable_plan", "Planning failed; inspect the retained failure receipt", 503
        )
    store.ensure_seed(seed)
    with store.transaction() as db:
        db.execute(
            "INSERT OR REPLACE INTO metadata VALUES ('runtime_evidence_path',?)",
            ("evidence/initial",),
        )
        db.execute(
            "INSERT OR REPLACE INTO metadata VALUES ('runtime_ingested_at',?)",
            (iso(store.clock()),),
        )
    RuntimeControl(store)
    retain_runtime_forecasts(store)
    return store


def run_runtime_worker(
    project_root: Path,
    store: OperationalStore,
    *,
    source_dir: Path | None = None,
    planning_cutoff: datetime | None = None,
) -> dict[str, Any]:
    """Process at most one durable job; no HTTP request performs expensive dbt work."""
    started = perf_counter()
    timings: dict[str, float] = {}
    control = runtime_control(store)
    retain_runtime_forecasts(store)
    worker = Actor("runtime-worker", "Local planning worker", ActorRole.SYSTEM, store.locations())
    claimed = perf_counter()
    timings["prepare"] = claimed - started
    job = control.claim(worker)
    source_started = perf_counter()
    timings["claim"] = source_started - claimed
    if job is None:
        return {"status": "idle", "reason": "No eligible work, or another worker holds the lease"}
    # Fencing the database is not enough: a reclaimed worker must not share
    # writable DuckDB/cache files with the abandoned attempt.
    try:
        evidence_dir = (
            store.control.attempt_evidence_dir(job["id"], job["token"], worker)
            if isinstance(store, PostgresRuntimeStore)
            else state_root(store)
            / "evidence"
            / "jobs"
            / job["id"]
            / f"attempt-{job['attempts']}-{job['token'][:16]}"
        )
        if isinstance(store, PublicationRuntimeStore):
            if (
                source_dir is not None
                and runtime_source_fingerprint(source_dir) != job["source_sha256"]
            ):
                raise StoreError(
                    "source_mismatch", "Explicit source differs from retained publication"
                )
            source_dir = store.materialize_source(job["source_sha256"])
        if source_dir is None or runtime_source_fingerprint(source_dir) != job["source_sha256"]:
            raise StoreError("source_mismatch", "Worker source does not match the queued snapshot")
        build_started = perf_counter()
        timings["source"] = build_started - source_started
        seed = build_runtime_seed(
            project_root, evidence_dir, source_dir, planning_cutoff=planning_cutoff
        )
        verify_started = perf_counter()
        timings["build"] = verify_started - build_started
        if isinstance(store, PostgresRuntimeStore):
            _compare_producer(seed, verify_runtime_current(project_root, evidence_dir))
        activate_started = perf_counter()
        timings["verify"] = activate_started - verify_started
        result = control.activate(job["id"], job["token"], seed, worker, evidence_dir=evidence_dir)
        timings["activate"] = perf_counter() - activate_started
    except (RuntimePipelineError, StoreError, ValueError, OSError) as error:
        reason = error.code if isinstance(error, StoreError) else "pipeline_failed"
        try:
            control.fail(job["id"], job["token"], worker, reason=reason)
        except StoreError as stale:
            if stale.code != "stale_worker":
                raise
        return {
            "status": "failed",
            "job_id": job["id"],
            "reason": reason,
            "human_approval_required": True,
        }
    # The workflow commit has already succeeded. A journal error must not mark a
    # completed job failed; restart/tick can safely retry retention, with its true
    # later registration time (never backdated prospective evidence).
    retain_started = perf_counter()
    retain_runtime_forecasts(store)
    timings["retain"] = perf_counter() - retain_started
    timings["total"] = perf_counter() - started
    return {"status": "completed", **result, "timings": timings}
