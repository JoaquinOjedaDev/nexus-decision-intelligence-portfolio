"""Installed-package PostgreSQL replanning with two actual retained restarts.

Run prepare -> explicit restart -> advance -> explicit restart -> verify.
Two real dbt/forecast/optimizer builds use separate synthetic source packages.
No HTTP/broker/IdP, crash/backup, scientific promotion or load/soak is claimed.
All application imports must come from the installed wheel (-I -B, empty cwd).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_backend import PostgresConfig
from nexus.portfolio.postgres_migrations import read_migration_sql
from nexus.portfolio.postgres_runtime import PostgresRuntimeControl, PostgresRuntimeStore
from nexus.portfolio.postgres_workflow import PostgresWorkflowStore
from nexus.portfolio.runtime_acceptance import _changed_source
from nexus.portfolio.runtime_pipeline import build_runtime_seed, runtime_source_fingerprint
from nexus.portfolio.store import canonical, digest
from nexus.storage import file_sha256

PROJECT = Path(__file__).resolve().parents[1]


def helpers() -> Any:
    """Load the existing checked harness without adding checkout paths to sys.path."""
    specification = importlib.util.spec_from_file_location(
        "nexus_runtime_workflow_harness", PROJECT / "scripts/postgres_workflow_wheel_acceptance.py"
    )
    if specification is None or specification.loader is None:
        raise ValueError("Workflow acceptance helper is unavailable")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def package_identity(helper: Any, wheel: Path) -> dict[str, Any]:
    identity = helper.installed_identity(wheel)
    installed = Path(identity["installed_root"])
    for name in ("nexus.portfolio.postgres_runtime", "nexus.portfolio.runtime_acceptance"):
        origin = getattr(sys.modules.get(name), "__file__", None)
        helper.require(
            origin is not None
            and Path(origin).resolve() == installed / (name.replace(".", "/") + ".py"),
            "Runtime acceptance imported an unverified application module",
        )
    with zipfile.ZipFile(wheel) as archive:
        expected = archive.read("nexus/portfolio/sql_migrations/004_portfolio_runtime.sql")
    helper.require(
        read_migration_sql(4) == expected
        and (PROJECT / "infra/postgres/migrations/004_portfolio_runtime.sql").read_bytes()
        == expected,
        "Runtime migration resource differs from the canonical installed bytes",
    )
    return {
        **identity,
        "runtime_acceptance_sha256": file_sha256(Path(__file__)),
        "runtime_migration_sha256": file_sha256(
            PROJECT / "infra/postgres/migrations/004_portfolio_runtime.sql"
        ),
    }


def require_restart(helper: Any, earlier: dict[str, Any], later: dict[str, Any]) -> None:
    before = earlier["server"]
    helper.require(
        all(
            later[key] == before[key]
            for key in ("cluster_id", "system_identifier", "binary_hashes")
        )
        and datetime.fromisoformat(later["postmaster_started_at"])
        > datetime.fromisoformat(earlier["recorded_at"]),
        "A retained restart of the same cluster after the phase receipt is required",
    )


def normalize_projection(seed: dict[str, Any]) -> dict[str, Any]:
    result = json.loads(canonical(seed))
    metadata = result["metadata"]
    if metadata.pop("runtime_job_id", None) is not None:
        result["seed_hash"] = metadata.pop("runtime_artifact_seed_hash")
        for recommendation in result["recommendations"]:
            recommendation["id"] = recommendation.pop("source_recommendation_id")
    return result


def runtime_store(config: dict[str, Any], spec: dict[str, Any]) -> PostgresRuntimeStore:
    return PostgresRuntimeStore(
        PostgresConfig(
            dsn=config["application_dsn"],
            schema=spec["schema"],
            artifact_root=Path(spec["initial_evidence"]),
        ),
        runtime_root=Path(spec["runtime_root"]),
    )


def read_context(
    cluster: Path, output: Path
) -> tuple[Any, Any, Path, dict[str, Any], dict[str, Any]]:
    helper = helpers()
    native = helper.native_helper()
    output = native.safe_root(output)
    spec = json.loads((output / "specification.json").read_text(encoding="utf-8"))
    prepared = json.loads((output / "prepared.json").read_text(encoding="utf-8"))
    helper.require(
        file_sha256(output / "specification.json") == prepared["specification_sha256"],
        "The frozen acceptance specification changed",
    )
    helper.require(
        package_identity(helper, Path(spec["wheel"])) == spec["package"],
        "Installed code, migration or harness changed between phases",
    )
    for key in ("runtime_root", "initial_evidence", "initial_source", "changed_source"):
        helper.require(
            native.safe_root(Path(spec[key])).is_relative_to(output),
            "Acceptance paths must stay in the retained output directory",
        )
    config = native.read_config(cluster)
    return helper, native, output, spec, config


def identities(
    helper: Any, store: PostgresRuntimeStore, output: Path
) -> tuple[Actor, Actor, Actor]:
    private = json.loads((output / "private/session-secrets.json").read_text(encoding="utf-8"))
    planner = helper.actor(store, private["planner"])
    approver = helper.actor(store, private["approver"])
    worker = Actor(
        "runtime-worker", "Synthetic planning worker", ActorRole.SYSTEM, planner.locations
    )
    return planner, approver, worker


def prepare(cluster: Path, output: Path, wheel: Path) -> dict[str, Any]:
    helper = helpers()
    native = helper.native_helper()
    output = native.safe_root(output)
    helper.require(not output.exists(), "Use a new output and preserve earlier attempts")
    config = native.read_config(cluster)
    live = helper.server(native, config)
    package = package_identity(helper, wheel)
    output.mkdir(parents=True, exist_ok=False)
    private = output / "private"
    private.mkdir()
    native.protect(private)
    root = output / "runtime-root"
    initial_evidence = root / "evidence/initial"
    helper.retain(
        output / "started.json",
        {
            "status": "STARTED_NOT_VERIFIED",
            "package": package,
            "server": live,
            "recorded_at": datetime.now(UTC).isoformat(),
        },
    )
    started = time.perf_counter()
    seed = build_runtime_seed(PROJECT, initial_evidence)
    build_seconds = time.perf_counter() - started
    initial_source = initial_evidence / "source"
    helper.require(
        helper.source_seed(initial_evidence, initial_source) == seed,
        "Initial producer output did not pass current source/code verification",
    )
    changed_source = _changed_source(initial_source, output / "changed-source")
    source_hash = runtime_source_fingerprint(changed_source)
    helper.require(
        source_hash != seed["metadata"]["source_snapshot_sha256"],
        "The declared synthetic source change did not alter source identity",
    )
    spec = {
        "contract_version": "nexus.postgres-runtime-acceptance.v1",
        "package": package,
        "wheel": str(wheel.resolve()),
        "runtime_root": str(root),
        "initial_evidence": str(initial_evidence),
        "initial_source": str(initial_source),
        "changed_source": str(changed_source),
        "source_sha256": source_hash,
        "schema": "nexus_runtime_accept_" + uuid4().hex,
        "initial_seed_sha256": digest(seed),
        "initial_build_seconds": build_seconds,
    }
    helper.retain(output / "specification.json", spec)
    store = runtime_store(config, spec)
    previous = PostgresWorkflowStore(store.config)
    previous.migrate(migration_dsn=config["migration_dsn"])
    previous.ensure_seed(seed)
    tokens = {name: previous.create_session(name)[0] for name in ("planner", "approver")}
    helper.retain(private / "session-secrets.json", tokens)
    before = previous.snapshot()
    migration = store.migrate(migration_dsn=config["migration_dsn"])
    upgraded = store.snapshot()
    helper.require(
        all(
            upgraded[key] == before[key]
            for key in ("seed", "recommendations", "events", "audit_head")
        ),
        "Version-4 migration changed existing workflow state",
    )
    planner, approver, worker = identities(helper, store, output)
    records = store.recommendations(planner)
    kept, invalidated = records[:2]
    for record in (kept, invalidated):
        store.decide(record["id"], approver, "approve", record["fingerprint"])
    executed = store.execute(kept["id"], approver, kept["fingerprint"], "retained-old-effect")
    control = PostgresRuntimeControl(store)
    instant = datetime.fromtimestamp(int(time.time()), UTC)
    event = EventEnvelope(
        event_id=uuid4(),
        event_type="demand.spike",
        source="synthetic-postgres-acceptance",
        subject_id="portfolio",
        occurred_at=instant,
        ingested_at=instant,
        idempotency_key=uuid4().hex,
        payload={"source_snapshot_sha256": source_hash},
    )
    receipt = control.submit(event, planner)
    helper.require(receipt["invalidated"] == len(records) - 1, "Material change left stale advice")
    helper.require(
        control.submit(event, planner)["duplicate"] is True, "Duplicate event was replayed"
    )
    helper.require(
        store.recommendation(invalidated["id"], planner)["state"] == "invalidated",
        "The prior approval did not invalidate immediately",
    )
    helper.refused(
        "approval_required",
        lambda: store.execute(
            invalidated["id"], approver, invalidated["fingerprint"], "stale-order"
        ),
    )
    job = control.claim(worker)
    helper.require(
        job is not None and job["source_sha256"] == source_hash, "No matching durable claim"
    )
    helper.require(control.claim(worker) is None, "A second worker claimed active work")
    attempt = control.attempt_evidence_dir(job["id"], job["token"], worker)
    helper.require(not attempt.exists(), "The claimed attempt must have its own fresh output")
    helper.retain(private / "claim-secrets.json", job)
    prepared = {
        "status": "PREPARED_RESTART_NOT_RUN",
        "recorded_at": datetime.now(UTC).isoformat(),
        "server": live,
        "specification_sha256": file_sha256(output / "specification.json"),
        "migration": migration,
        "snapshot_sha256": digest(store.snapshot()),
        "old_executed": {
            "id": kept["id"],
            "fingerprint": kept["fingerprint"],
            "execution_ref": executed["execution_ref"],
        },
        "old_invalidated": {"id": invalidated["id"], "fingerprint": invalidated["fingerprint"]},
        "job_id": job["id"],
        "attempt_directory": str(attempt),
        "checks": [
            "installed_code_and_all_migrations",
            "real_initial_source_dbt_forecast_optimization",
            "populated_v3_to_v4_preserves_state_and_sessions",
            "source_change_invalidates_prior_approval",
            "old_executed_effect_survives_invalidation",
            "duplicate_event_does_not_duplicate_job",
            "one_live_claim_with_distinct_attempt_directory",
        ],
    }
    helper.retain(output / "prepared.json", prepared)
    return prepared


def advance(cluster: Path, output: Path) -> dict[str, Any]:
    helper, native, output, spec, config = read_context(cluster, output)
    prepared = json.loads((output / "prepared.json").read_text(encoding="utf-8"))
    helper.require(not (output / "advanced.json").exists(), "Advancement is already recorded")
    live = helper.server(native, config)
    require_restart(helper, prepared, live)
    store = runtime_store(config, spec)
    helper.require(
        digest(store.snapshot()) == prepared["snapshot_sha256"], "Retained claim/workflow changed"
    )
    planner, approver, worker = identities(helper, store, output)
    control = PostgresRuntimeControl(store)
    job = json.loads((output / "private/claim-secrets.json").read_text(encoding="utf-8"))
    helper.require(control.claim(worker) is None, "A live retained claim was duplicated")
    attempt = control.attempt_evidence_dir(job["id"], job["token"], worker)
    helper.require(
        str(attempt) == prepared["attempt_directory"] and not attempt.exists(),
        "Attempt binding changed",
    )
    initial = helper.source_seed(Path(spec["initial_evidence"]), Path(spec["initial_source"]))
    helper.require(
        digest(initial) == spec["initial_seed_sha256"], "Initial source evidence changed"
    )
    source = Path(spec["changed_source"])
    helper.require(
        runtime_source_fingerprint(source) == spec["source_sha256"], "Changed source drifted"
    )
    started = time.perf_counter()
    candidate = build_runtime_seed(
        PROJECT,
        attempt,
        source,
        planning_cutoff=datetime.fromisoformat(initial["metadata"]["scientific_decision_time"]),
    )
    elapsed = time.perf_counter() - started
    helper.require(
        helper.source_seed(attempt, source) == candidate, "Replacement evidence is not current"
    )
    helper.require(
        candidate["metadata"]["source_snapshot_sha256"] == spec["source_sha256"]
        and candidate["runtime_lineage"]["gold_demand_sha256"]
        != initial["runtime_lineage"]["gold_demand_sha256"],
        "The changed source did not reach validated Gold demand",
    )
    activated = control.activate(job["id"], job["token"], candidate, worker, evidence_dir=attempt)
    helper.require(
        digest(normalize_projection(store.seed())) == digest(candidate),
        "Active projection differs from producer output",
    )
    helper.require(
        control.snapshot(planner)["archived_generations"] == 1,
        "Initial generation was not archived",
    )
    prior = prepared["old_invalidated"]
    helper.require(
        store.recommendation(prior["id"], planner)["state"] == "invalidated",
        "Archived approval revived",
    )
    records = store.overview_snapshot(planner)["current_recommendations"]
    helper.require(
        len(records) == len(candidate["recommendations"]), "Wrong active recommendation set"
    )
    new = records[0]
    helper.refused(
        "approval_required",
        lambda: store.execute(new["id"], approver, new["fingerprint"], "unapproved-new"),
    )
    helper.refused(
        "role_denied", lambda: store.decide(new["id"], worker, "approve", new["fingerprint"])
    )
    helper.require(
        store.metrics_snapshot(planner)["effect_count"] == 1, "Replacement recreated old effects"
    )
    # Leave the new generation unapproved across the second real restart.
    advanced = {
        "status": "ADVANCED_SECOND_RESTART_NOT_RUN",
        "recorded_at": datetime.now(UTC).isoformat(),
        "server": live,
        "prepared_sha256": file_sha256(output / "prepared.json"),
        "snapshot_sha256": digest(store.snapshot()),
        "activated": activated,
        "new_recommendation": {"id": new["id"], "fingerprint": new["fingerprint"]},
        "replacement_evidence": str(attempt),
        "replacement_seed_sha256": digest(candidate),
        "replacement_build_seconds": elapsed,
        "checks": prepared["checks"]
        + [
            "retained_restart_preserves_live_claim_and_sessions",
            "real_changed_source_dbt_forecast_optimization",
            "job_bound_activation_matches_verified_producer",
            "archived_owner_and_old_effect_preserved",
            "replacement_needs_new_independent_approval",
        ],
    }
    helper.retain(output / "advanced.json", advanced)
    return advanced


def verify(cluster: Path, output: Path) -> dict[str, Any]:
    helper, native, output, spec, config = read_context(cluster, output)
    prepared = json.loads((output / "prepared.json").read_text(encoding="utf-8"))
    advanced = json.loads((output / "advanced.json").read_text(encoding="utf-8"))
    helper.require(
        file_sha256(output / "prepared.json") == advanced["prepared_sha256"],
        "Prepared receipt changed",
    )
    live = helper.server(native, config)
    require_restart(helper, advanced, live)
    store = runtime_store(config, spec)
    snapshot = store.snapshot()
    helper.require(
        digest(snapshot) == advanced["snapshot_sha256"],
        "Two-generation state changed across restart",
    )
    migration = store.migrate(migration_dsn=config["migration_dsn"])
    helper.require(
        migration["applied"] is False
        and all(
            migration[key] == prepared["migration"][key]
            for key in ("version", "migration_sha256", "schema", "server_version")
        ),
        "Migration reapply changed its immutable identity",
    )
    store.ensure_seed(store.seed())
    helper.require(store.snapshot() == snapshot, "Seed retry rewrote active or archived state")
    initial = helper.source_seed(Path(spec["initial_evidence"]), Path(spec["initial_source"]))
    candidate = helper.source_seed(
        Path(advanced["replacement_evidence"]), Path(spec["changed_source"])
    )
    helper.require(
        digest(initial) == spec["initial_seed_sha256"]
        and digest(candidate) == advanced["replacement_seed_sha256"]
        and normalize_projection(store.seed()) == candidate,
        "Retained generations lost their source/code/artifact bindings",
    )
    planner, approver, worker = identities(helper, store, output)
    control = PostgresRuntimeControl(store)
    helper.require(control.claim(worker) is None, "Completed job became claimable")
    old = prepared["old_executed"]
    context = store.recommendation_context(old["id"], planner)
    helper.require(
        context["generation"]["is_current"] is False
        and context["generation"]["seed_sha256"] == spec["initial_seed_sha256"]
        and context["execution_ref"] == old["execution_ref"],
        "Historical recommendation points to the wrong generation or effect",
    )
    new = advanced["new_recommendation"]
    helper.refused(
        "approval_required",
        lambda: store.execute(new["id"], approver, new["fingerprint"], "unapproved-restarted"),
    )
    store.decide(new["id"], approver, "approve", new["fingerprint"])
    store.execute(new["id"], approver, new["fingerprint"], "new-generation-order")
    for record in (old, new):
        deadline = time.monotonic() + 10
        while not store.recommendation(record["id"], planner)["outcome_ready"]:
            helper.require(
                time.monotonic() < deadline, "Actual demo maturity wait exceeded 10 seconds"
            )
            time.sleep(0.1)
        observed = store.observe(
            record["id"], planner, record["fingerprint"], "Explicit synthetic runtime outcome"
        )
        retained = store.recommendation(record["id"], planner)
        outcome_events = [
            event for event in retained["events"] if event["event_type"] == "outcome_observed"
        ]
        helper.require(
            observed["state"] == retained["state"] == "outcome_observed"
            and observed["outcome"] == retained["outcome"]
            and isinstance(retained["outcome"], dict)
            and bool(retained["outcome"])
            and len(outcome_events) == 1
            and outcome_events[0]["actor_id"] == planner.actor_id,
            "A mature outcome was not durably recorded exactly once by the planner",
        )
    helper.require(
        store.metrics_snapshot(planner)["effect_count"] == 2
        and store.metrics_snapshot(planner)["states"].get("outcome_observed") == 2,
        "Wrong unique mock effect or mature outcome count",
    )
    result = {
        "status": "PASS",
        "recorded_at": datetime.now(UTC).isoformat(),
        "server": live,
        "package": spec["package"],
        "specification_sha256": file_sha256(output / "specification.json"),
        "prepared_sha256": file_sha256(output / "prepared.json"),
        "advanced_sha256": file_sha256(output / "advanced.json"),
        "snapshot_sha256": digest(store.snapshot()),
        "migration_reapply": migration,
        "initial_build_seconds": spec["initial_build_seconds"],
        "replacement_build_seconds": advanced["replacement_build_seconds"],
        "checks": advanced["checks"]
        + [
            "second_restart_preserves_both_generations",
            "unchanged_migration_and_seed_retry",
            "current_and_archived_source_artifacts_verified",
            "old_approval_never_authorizes_replacement",
            "independent_new_approval_and_two_unique_mock_effects",
            "real_mature_outcomes_from_both_generations",
        ],
        "not_claimed": [
            "actual human identities",
            "HTTP/API/UI integration",
            "broker delivery",
            "crash or backup restore",
            "distributed atomicity",
            "Compose/Linux",
            "scale/load/soak",
            "scientific model promotion",
            "95 percent complete",
        ],
    }
    helper.retain(output / "result.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "advance", "verify"))
    parser.add_argument("--cluster-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wheel", type=Path)
    args = parser.parse_args()
    os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", "")
    os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    if args.action == "prepare":
        if args.wheel is None:
            parser.error("prepare requires --wheel")
        result = prepare(args.cluster_root, args.output, args.wheel)
    else:
        result = (advance if args.action == "advance" else verify)(args.cluster_root, args.output)
    print(
        json.dumps(
            {
                "status": result["status"],
                "checks": len(result["checks"]),
                "output": str(args.output),
            }
        )
    )
