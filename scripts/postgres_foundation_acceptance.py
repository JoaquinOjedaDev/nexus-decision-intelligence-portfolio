"""Verify real source-backed seed parity before/after a task-owned PostgreSQL restart.

Run as ``python -m scripts.postgres_foundation_acceptance`` from the checkout.
This never starts/stops services and never migrates the existing application.
The separate native-cluster launcher performs the explicit retained restart.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg

from nexus.portfolio.postgres_backend import PostgresBackend, PostgresConfig
from nexus.portfolio.runtime_contracts import load_runtime_policy
from nexus.portfolio.runtime_pipeline import (
    RUNTIME_SEED_FILENAME,
    _request_fingerprint,
    load_runtime_seed,
    runtime_source_fingerprint,
    verify_runtime_current,
)
from nexus.portfolio.store import PortfolioStore, digest
from scripts.native_postgres_test_cluster import PROJECT, inspect_server, read_config, safe_root

CODE_FILES = (
    "scripts/postgres_foundation_acceptance.py",
    "scripts/native_postgres_test_cluster.py",
    "src/nexus/portfolio/postgres_backend.py",
    "src/nexus/portfolio/store.py",
    "src/nexus/portfolio/runtime_pipeline.py",
    "infra/postgres/migrations/002_portfolio_ops.sql",
    "tests/test_portfolio_postgres_backend.py",
    "tests/test_portfolio_postgres_backend_service.py",
    "pyproject.toml",
    "uv.lock",
)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def retain(path: Path, value: dict[str, Any]) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)


def code_identity() -> dict[str, str]:
    return {name: sha(PROJECT / name) for name in CODE_FILES}


def runtime(evidence: Path, source: Path) -> dict[str, Any]:
    # Refuse to build missing evidence: no hidden dbt run or rewrite during acceptance.
    if not (evidence / RUNTIME_SEED_FILENAME).is_file():
        raise ValueError("A completed retained runtime build is required")
    initial = verify_runtime_current(PROJECT, evidence)
    source_hash = runtime_source_fingerprint(source)
    if source_hash != initial["metadata"]["source_snapshot_sha256"]:
        raise ValueError("Current source bytes differ from the retained runtime")
    fingerprint = _request_fingerprint(
        PROJECT,
        source,
        load_runtime_policy(PROJECT / "configs/portfolio/runtime-baseline.yaml"),
        datetime.fromisoformat(initial["metadata"]["scientific_decision_time"]),
        None,
    )
    seed = load_runtime_seed(evidence, expected_fingerprint=fingerprint)
    if (
        not seed["executable"]
        or not seed["recommendations"]
        or seed["metrics"]["dbt_status"] != "PASS"
        or not seed["metrics"]["independently_feasible"]
        or seed["metadata"]["classification"] != "synthetic"
    ):
        raise ValueError("An executable, validated synthetic planning seed is required")
    return seed


def server_identity(config: dict[str, Any]) -> dict[str, Any]:
    status = inspect_server(config)
    with psycopg.connect(config["admin_dsn"], options="-c search_path=pg_catalog") as db:
        started = db.execute("SELECT pg_postmaster_start_time()").fetchone()[0]
    return {
        **status,
        "postmaster_started_at": started.isoformat(),
        "binary_hashes": config["binary_hashes"],
    }


def sqlite_projection(store: PortfolioStore) -> dict[str, Any]:
    with store.connection() as db:
        recommendations = [
            dict(row) for row in db.execute("SELECT * FROM recommendations ORDER BY id")
        ]
        events = [dict(row) for row in db.execute("SELECT * FROM events ORDER BY sequence")]
        head = db.execute("SELECT value FROM metadata WHERE key='audit_head'").fetchone()[0]
    seed = store.seed()
    return {
        "seed": seed,
        "seed_sha256": digest(seed),
        "recommendations": recommendations,
        "events": events,
        "audit_head": head,
        "event_count": len(events),
    }


def backend(config: dict[str, Any], spec: dict[str, Any]) -> PostgresBackend:
    return PostgresBackend(
        PostgresConfig(
            dsn=config["application_dsn"],
            schema=spec["schema"],
            artifact_root=Path(spec["evidence_directory"]),
        ),
        clock=lambda: spec["creation_epoch"],
    )


def prepare(cluster: Path, output: Path, evidence: Path, source: Path) -> dict[str, Any]:
    output, evidence, source = safe_root(output), safe_root(evidence), safe_root(source)
    if output.exists():
        raise ValueError("Use a new acceptance output; retain earlier attempts")
    config = read_config(cluster)
    server = server_identity(config)
    seed = runtime(evidence, source)
    spec = {
        "contract_version": "nexus.postgres-foundation-acceptance.v1",
        "cluster_id": config["cluster_id"],
        "system_identifier": server["system_identifier"],
        "schema": "nexus_accept_" + uuid4().hex,
        "evidence_directory": str(evidence),
        "source_directory": str(source),
        "creation_epoch": time.time(),
        "code_sha256": code_identity(),
        "seed_file_sha256": sha(evidence / RUNTIME_SEED_FILENAME),
        "canonical_seed_sha256": digest(seed),
        "python": platform.python_version(),
        "psycopg": importlib.metadata.version("psycopg"),
    }
    output.mkdir(parents=True, exist_ok=False)
    retain(output / "specification.json", spec)
    pg = backend(config, spec)
    migration = pg.migrate(migration_dsn=config["migration_dsn"])
    pg.ensure_seed(seed)
    sqlite = PortfolioStore(output / "sqlite-parity", clock=lambda: spec["creation_epoch"])
    sqlite.ensure_seed(seed)
    snapshot = pg.snapshot()
    if snapshot != sqlite_projection(sqlite):
        raise ValueError("PostgreSQL and SQLite canonical state differ")
    receipt = {
        "status": "PREPARED_RESTART_NOT_RUN",
        "prepared_at": datetime.now(UTC).isoformat(),
        "specification_sha256": sha(output / "specification.json"),
        "server": server,
        "migration": migration,
        "snapshot_sha256": digest(snapshot),
        "seed_sha256": snapshot["seed_sha256"],
        "audit_head": snapshot["audit_head"],
        "event_count": snapshot["event_count"],
        "recommendations": len(snapshot["recommendations"]),
        "verified_artifacts": len(seed["artifacts"]),
        "source_rows": seed["metrics"]["source_rows"],
        "gold_rows": seed["metrics"]["gold_rows"],
        "solver_status": seed["metrics"]["solver_status"],
        "scientific_decision_time": seed["metadata"]["scientific_decision_time"],
    }
    retain(output / "prepared.json", receipt)
    return receipt


def verify(cluster: Path, output: Path) -> dict[str, Any]:
    output = safe_root(output)
    spec = json.loads((output / "specification.json").read_text(encoding="utf-8"))
    before = json.loads((output / "prepared.json").read_text(encoding="utf-8"))
    if (
        before["specification_sha256"] != sha(output / "specification.json")
        or spec["code_sha256"] != code_identity()
    ):
        raise ValueError("Acceptance specification or governing code changed")
    config = read_config(cluster)
    server = server_identity(config)
    if (
        server["cluster_id"] != spec["cluster_id"]
        or server["system_identifier"] != spec["system_identifier"]
        or server["postmaster_started_at"] == before["server"]["postmaster_started_at"]
        or datetime.fromisoformat(server["postmaster_started_at"])
        <= datetime.fromisoformat(before["prepared_at"])
        or server["binary_hashes"] != before["server"]["binary_hashes"]
    ):
        raise ValueError("A retained restart of the same task-owned cluster is required")
    evidence, source = (
        safe_root(Path(spec["evidence_directory"])),
        safe_root(Path(spec["source_directory"])),
    )
    seed = runtime(evidence, source)
    if (
        sha(evidence / RUNTIME_SEED_FILENAME) != spec["seed_file_sha256"]
        or digest(seed) != spec["canonical_seed_sha256"]
    ):
        raise ValueError("Retained source-backed seed changed")
    pg = backend(config, spec)
    snapshot = pg.snapshot()
    if digest(snapshot) != before["snapshot_sha256"]:
        raise ValueError("Database state did not survive restart unchanged")
    migration = pg.migrate(migration_dsn=config["migration_dsn"])
    # A newly constructed application instance uses the current clock, not the seed clock.
    current = PostgresBackend(pg.config)
    current.ensure_seed(seed)
    migration_identity = ("version", "migration_sha256", "schema", "server_version")
    if (
        current.snapshot() != snapshot
        or migration["applied"] is not False
        or any(migration[key] != before["migration"][key] for key in migration_identity)
    ):
        raise ValueError("Bootstrap retry changed audit, expiry, seed, or migration identity")
    sqlite = PortfolioStore(output / "sqlite-parity")
    if sqlite_projection(sqlite) != snapshot:
        raise ValueError("Retained canonical SQLite parity failed")
    result = {
        "status": "PASS",
        "verified_at": datetime.now(UTC).isoformat(),
        "scope": "Native PostgreSQL source-backed initial-seed persistence and retained restart",
        "prepared_sha256": sha(output / "prepared.json"),
        "specification_sha256": sha(output / "specification.json"),
        "before_server": before["server"],
        "after_server": server,
        "snapshot_sha256": digest(snapshot),
        "migration_reapply": migration,
        "checks": {
            "current_source_code_policy_and_artifacts_verified": True,
            "canonical_seed_payload_timestamp_and_audit_parity": True,
            "same_cluster_distinct_postmaster_start": True,
            "restart_occurred_after_seed_parity_receipt": True,
            "retained_state_unchanged": True,
            "migration_and_seed_retry_unchanged": True,
        },
        "not_claimed": [
            "new source/dbt/forecast build",
            "API or workflow migration",
            "human approval",
            "broker delivery",
            "PostgreSQL 17.6 Compose/Linux evidence",
            "backup/restore",
            "crash recovery",
            "load or soak performance",
            "scientific promotion",
        ],
    }
    retain(output / "result.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "verify"))
    parser.add_argument("--cluster-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--source", type=Path)
    args = parser.parse_args()
    if args.action == "prepare":
        if args.evidence is None or args.source is None:
            parser.error("prepare requires --evidence and --source")
        result = prepare(args.cluster_root, args.output, args.evidence, args.source)
    else:
        result = verify(args.cluster_root, args.output)
    print(json.dumps(result, indent=2, sort_keys=True))
