"""Actual installed-wheel, source-backed PostgreSQL workflow/restart acceptance.

Execute with the new wheel environment's Python using -I -B, from an empty cwd.
The adjacent native-cluster helper is a guarded test harness, not imported app
code. All nexus imports must come from the installed wheel, with matching bytes.
Service lifecycle is explicit and external to this script. No real purchases,
identity provider, broker, HTTP service, model promotion or soak are claimed.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import json
import sys
import sysconfig
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

import psycopg

import nexus
from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_backend import PostgresBackend, PostgresConfig
from nexus.portfolio.postgres_migrations import read_migration_sql
from nexus.portfolio.postgres_workflow import PostgresWorkflowStore
from nexus.portfolio.runtime_contracts import load_runtime_policy
from nexus.portfolio.runtime_pipeline import (
    RUNTIME_SEED_FILENAME,
    _request_fingerprint,
    load_runtime_seed,
    runtime_source_fingerprint,
    verify_runtime_current,
)
from nexus.portfolio.store import PortfolioStore, StoreError, digest
from nexus.storage import file_sha256

PROJECT = Path(__file__).resolve().parents[1]
CRITICAL_MODULES = (
    "nexus.portfolio.postgres_backend",
    "nexus.portfolio.postgres_workflow",
    "nexus.portfolio.postgres_migrations",
    "nexus.portfolio.store",
    "nexus.portfolio.runtime_pipeline",
)


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def retain(path: Path, payload: dict[str, Any]) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(payload, stream, sort_keys=True, indent=2, allow_nan=False)


def native_helper() -> Any:
    spec = importlib.util.spec_from_file_location(
        "nexus_workflow_acceptance_native_helper",
        PROJECT / "scripts/native_postgres_test_cluster.py",
    )
    require(spec is not None and spec.loader is not None, "Native helper is unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def installed_identity(wheel: Path) -> dict[str, Any]:
    require(bool(sys.flags.isolated), "Use Python -I to exclude ambient source paths")
    require(bool(sys.dont_write_bytecode), "Use Python -B for this retained wheel verification")
    purelib = Path(sysconfig.get_paths()["purelib"]).resolve()
    require(
        Path(nexus.__file__).resolve() == purelib / "nexus/__init__.py",
        "nexus is not installed here",
    )
    modules: dict[str, str] = {}
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        require(len(names) == len(set(names)), "Wheel contains duplicate paths")
        for name in names:
            path = PurePosixPath(name)
            require(not path.is_absolute() and ".." not in path.parts, "Unsafe wheel member")
            require(path.suffix != ".pth", "Wheel must not install path hooks")
            if name.startswith("nexus/") and path.suffix in {".py", ".sql"}:
                installed = purelib.joinpath(*path.parts)
                require(installed.is_file(), "Installed wheel member is missing")
                require(
                    installed.resolve().is_relative_to(purelib),
                    "Installed member escapes the isolated environment",
                )
                expected = archive.read(name)
                require(installed.read_bytes() == expected, "Installed bytes differ from wheel")
                modules[name] = file_sha256(installed)
        for version in (2, 3):
            filename = {2: "002_portfolio_ops.sql", 3: "003_portfolio_workflow.sql"}[version]
            expected = archive.read("nexus/portfolio/sql_migrations/" + filename)
            require(read_migration_sql(version) == expected, "SQL loader did not use wheel bytes")
            require(
                (PROJECT / "infra/postgres/migrations" / filename).read_bytes() == expected,
                "Canonical migration changed after wheel build",
            )
    require(bool(modules), "Wheel has no application modules")
    inventory = {
        path.relative_to(purelib).as_posix()
        for path in (purelib / "nexus").rglob("*")
        if path.is_file() and path.suffix in {".py", ".sql"}
    }
    require(inventory == set(modules), "Installed application contains unverified source files")
    for name in CRITICAL_MODULES:
        relative = name.replace(".", "/") + ".py"
        module = sys.modules.get(name)
        origin = getattr(module, "__file__", None)
        require(
            relative in modules
            and origin is not None
            and Path(origin).resolve()
            == purelib.joinpath(*PurePosixPath(relative).parts).resolve(),
            "Imported critical module is not the verified wheel member: " + name,
        )
    return {
        "wheel_sha256": file_sha256(wheel),
        "module_tree_sha256": digest(modules),
        "module_count": len(modules),
        "python": sys.version.split()[0],
        "psycopg": importlib.metadata.version("psycopg"),
        "installed_root": str(purelib),
        "isolated_mode": True,
        "harness_sha256": file_sha256(Path(__file__)),
        "native_helper_sha256": file_sha256(PROJECT / "scripts/native_postgres_test_cluster.py"),
        "lock_sha256": file_sha256(PROJECT / "uv.lock"),
        "project_config_sha256": file_sha256(PROJECT / "pyproject.toml"),
    }


def source_seed(evidence: Path, source: Path) -> dict[str, Any]:
    initial = verify_runtime_current(PROJECT, evidence)
    require(
        runtime_source_fingerprint(source) == initial["metadata"]["source_snapshot_sha256"],
        "Source package differs from the current runtime",
    )
    fingerprint = _request_fingerprint(
        PROJECT,
        source,
        load_runtime_policy(PROJECT / "configs/portfolio/runtime-baseline.yaml"),
        datetime.fromisoformat(initial["metadata"]["scientific_decision_time"]),
        None,
    )
    seed = load_runtime_seed(evidence, expected_fingerprint=fingerprint)
    require(
        seed["executable"]
        and len(seed["recommendations"]) >= 4
        and seed["metrics"]["dbt_status"] == "PASS"
        and seed["metrics"]["independently_feasible"]
        and seed["metadata"]["classification"] == "synthetic",
        "Acceptance requires at least four validated synthetic recommendations",
    )
    return seed


def server(helper: Any, config: dict[str, Any]) -> dict[str, Any]:
    status = helper.inspect_server(config)
    with psycopg.connect(config["admin_dsn"], options="-c search_path=pg_catalog") as db:
        instant = db.execute("SELECT pg_postmaster_start_time()").fetchone()[0]
    return {
        **status,
        "postmaster_started_at": instant.isoformat(),
        "binary_hashes": config["binary_hashes"],
    }


def refused(expected_code: str, action: Any) -> None:
    try:
        action()
    except StoreError as error:
        require(error.code == expected_code, "Unexpected refusal: " + error.code)
    else:
        raise ValueError("Required refusal did not occur: " + expected_code)


def actor(store: PostgresWorkflowStore, token: str) -> Actor:
    result = store.session(token)
    require(result is not None, "A persisted session failed verification")
    return result[0]


def store_config(config: dict[str, Any], spec: dict[str, Any]) -> PostgresConfig:
    return PostgresConfig(
        dsn=config["application_dsn"],
        schema=spec["schema"],
        artifact_root=Path(spec["evidence_directory"]),
    )


def prepare(
    cluster: Path, output: Path, evidence: Path, source: Path, wheel: Path
) -> dict[str, Any]:
    helper = native_helper()
    output, evidence, source = map(helper.safe_root, (output, evidence, source))
    require(not output.exists(), "Use a fresh acceptance output; preserve earlier attempts")
    config = helper.read_config(cluster)
    before_server = server(helper, config)
    package = installed_identity(wheel)
    seed = source_seed(evidence, source)
    spec = {
        "contract_version": "nexus.postgres-workflow-wheel-acceptance.v1",
        "package": package,
        "cluster_id": config["cluster_id"],
        "system_identifier": before_server["system_identifier"],
        "schema": "nexus_wheel_" + uuid4().hex,
        "evidence_directory": str(evidence),
        "source_directory": str(source),
        "wheel_path": str(wheel.resolve()),
        "creation_epoch": time.time(),
        "canonical_seed_sha256": digest(seed),
        "seed_file_sha256": file_sha256(evidence / RUNTIME_SEED_FILENAME),
    }
    output.mkdir(parents=True, exist_ok=False)
    private = output / "private"
    private.mkdir()
    helper.protect(private)
    retain(output / "specification.json", spec)
    pg_config = store_config(config, spec)
    foundation = PostgresBackend(pg_config, clock=lambda: spec["creation_epoch"])
    foundation_migration = foundation.migrate(migration_dsn=config["migration_dsn"])
    foundation.ensure_seed(seed)
    before = foundation.snapshot()
    parity_root = output / "sqlite-parity"
    parity_root.mkdir()
    helper.protect(parity_root)
    sqlite = PortfolioStore(parity_root, clock=lambda: spec["creation_epoch"])
    sqlite.ensure_seed(seed)
    with sqlite.connection() as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM recommendations ORDER BY id")]
        events = [dict(row) for row in db.execute("SELECT * FROM events ORDER BY sequence")]
    require(
        before["recommendations"] == rows and before["events"] == events,
        "SQLite seed parity failed",
    )
    store = PostgresWorkflowStore(pg_config)
    migration = store.migrate(migration_dsn=config["migration_dsn"])
    upgraded = store.snapshot()
    require(
        all(
            upgraded[key] == before[key]
            for key in ("seed", "seed_sha256", "recommendations", "events", "audit_head")
        ),
        "Workflow migration changed initial seed/projection/history",
    )
    tokens = {name: store.create_session(name)[0] for name in ("planner", "approver", "auditor")}
    planner, approver, auditor = (actor(store, tokens[name]) for name in tokens)
    records = store.recommendations(planner)
    require(len(planner.locations) >= 2, "Scope acceptance requires multiple locations")
    first, second, third, fourth = records[:4]
    restricted = Actor(planner.actor_id, planner.name, planner.role, (first["location_id"],))
    foreign = next(row for row in records if row["location_id"] not in restricted.locations)
    refused("scope_denied", lambda: store.recommendation(foreign["id"], restricted))
    refused("scope_denied", lambda: store.overview_snapshot(restricted))
    refused("scope_denied", lambda: store.metrics_snapshot(restricted))
    refused(
        "role_denied", lambda: store.decide(first["id"], planner, "approve", first["fingerprint"])
    )
    maker = Actor(first["created_by"], "Maker test", ActorRole.APPROVER, approver.locations)
    refused(
        "maker_checker", lambda: store.decide(first["id"], maker, "approve", first["fingerprint"])
    )
    system = Actor("service.test", "System test", ActorRole.SYSTEM, approver.locations)
    refused(
        "role_denied",
        lambda: store.execute(first["id"], system, first["fingerprint"], "system-denied"),
    )
    store.decide(
        first["id"], approver, "approve", first["fingerprint"], "Synthetic independent review"
    )
    failed = store.execute(
        first["id"], approver, first["fingerprint"], "retained-failure", simulate_failure=True
    )
    require(failed["state"] == "execution_failed", "Simulated failed delivery not retained")
    store.decide(second["id"], approver, "approve", second["fingerprint"])
    store.decide(third["id"], approver, "reject", third["fingerprint"])
    store.decide(fourth["id"], approver, "approve", fourth["fingerprint"])
    store.decide(fourth["id"], approver, "revoke", fourth["fingerprint"])
    require(
        store.metrics_snapshot(auditor)["effect_count"] == 0, "A failed delivery created an effect"
    )
    snapshot = store.snapshot()
    public = json.dumps(snapshot)
    require(
        all(token not in public for token in tokens.values()), "Snapshot leaked session credentials"
    )
    retain(private / "session-secrets.json", {"tokens": tokens})
    prepared = {
        "status": "PREPARED_RESTART_NOT_RUN",
        "prepared_at": datetime.now(UTC).isoformat(),
        "specification_sha256": file_sha256(output / "specification.json"),
        "before_server": before_server,
        "foundation_migration": foundation_migration,
        "workflow_migration": migration,
        "snapshot_sha256": digest(snapshot),
        "selected": [{"id": row["id"], "fingerprint": row["fingerprint"]} for row in records[:4]],
        "source_rows": seed["metrics"]["source_rows"],
        "gold_rows": seed["metrics"]["gold_rows"],
        "verified_artifacts": len(seed["artifacts"]),
        "audit_head": snapshot["audit_head"],
        "checks": [
            "installed_wheel_SQL_and_code",
            "current_source_and_artifacts",
            "SQLite_initial_parity",
            "v2_to_v3_preserves_initial_state",
            "scoped_and_role_denials",
            "maker_checker_denial",
            "failed_delivery_zero_effects",
            "approval_rejection_revocation_retained",
        ],
    }
    retain(output / "prepared.json", prepared)
    return prepared


def verify(cluster: Path, output: Path) -> dict[str, Any]:
    helper = native_helper()
    output = helper.safe_root(output)
    spec = json.loads((output / "specification.json").read_text(encoding="utf-8"))
    prepared = json.loads((output / "prepared.json").read_text(encoding="utf-8"))
    require(
        prepared["specification_sha256"] == file_sha256(output / "specification.json"),
        "Specification changed",
    )
    require(
        installed_identity(Path(spec["wheel_path"])) == spec["package"],
        "Installed package or harness changed",
    )
    config = helper.read_config(cluster)
    after_server = server(helper, config)
    require(
        after_server["cluster_id"] == spec["cluster_id"]
        and after_server["system_identifier"] == spec["system_identifier"]
        and after_server["binary_hashes"] == prepared["before_server"]["binary_hashes"]
        and datetime.fromisoformat(after_server["postmaster_started_at"])
        > datetime.fromisoformat(prepared["prepared_at"]),
        "A retained restart of the same cluster after prepared state is required",
    )
    evidence = helper.safe_root(Path(spec["evidence_directory"]))
    source = helper.safe_root(Path(spec["source_directory"]))
    seed = source_seed(evidence, source)
    require(
        digest(seed) == spec["canonical_seed_sha256"]
        and file_sha256(evidence / RUNTIME_SEED_FILENAME) == spec["seed_file_sha256"],
        "Source-backed seed changed",
    )
    store = PostgresWorkflowStore(store_config(config, spec))
    snapshot = store.snapshot()
    require(
        digest(snapshot) == prepared["snapshot_sha256"], "Workflow state changed across restart"
    )
    reapplied = store.migrate(migration_dsn=config["migration_dsn"])
    migration_identity = ("version", "migration_sha256", "schema", "server_version")
    require(
        reapplied["applied"] is False
        and all(
            reapplied[key] == prepared["workflow_migration"][key] for key in migration_identity
        ),
        "Migration retry changed the version, SQL checksum, schema or server identity",
    )
    store.ensure_seed(seed)
    require(store.snapshot() == snapshot, "Migration/seed retry changed persisted state or expiry")
    tokens = json.loads((output / "private/session-secrets.json").read_text(encoding="utf-8"))[
        "tokens"
    ]
    planner, approver, auditor = (
        actor(store, tokens[name]) for name in ("planner", "approver", "auditor")
    )
    first, second, third, fourth = prepared["selected"]

    def retry(index: int) -> dict[str, Any]:
        return store.execute(first["id"], approver, first["fingerprint"], "retry-" + str(index))

    with ThreadPoolExecutor(max_workers=4) as pool:
        delivered = list(pool.map(retry, range(4)))
    require(
        len({item["execution_ref"] for item in delivered}) == 1,
        "Concurrent retries created different orders",
    )
    require(
        store.metrics_snapshot(auditor)["effect_count"] == 1,
        "Concurrent retry did not retain exactly one effect",
    )
    store.execute(second["id"], approver, second["fingerprint"], "second-order")
    for record in (third, fourth):
        refused(
            "approval_required",
            lambda record=record: store.execute(
                record["id"], approver, record["fingerprint"], "denied-" + record["id"]
            ),
        )
    refused("role_denied", lambda: store.observe(first["id"], approver, first["fingerprint"]))
    for record in (first, second):
        due = datetime.fromisoformat(
            store.recommendation(record["id"], planner)["outcome_due_at"]
        ).timestamp()
        if time.time() < due:
            refused(
                "outcome_immature",
                lambda record=record: store.observe(record["id"], planner, record["fingerprint"]),
            )
        require(due - time.time() <= 10, "Unexpected maturity wait; refusing to stretch test time")
        deadline = time.monotonic() + 10
        while True:
            remaining = due - time.time()
            if remaining <= 0:
                break
            require(time.monotonic() < deadline, "Wall clock did not reach the maturity boundary")
            time.sleep(min(0.5, remaining))
        observed = store.observe(
            record["id"], planner, record["fingerprint"], "Explicit simulated outcome"
        )
        require(
            observed["state"] == "outcome_observed" and observed["outcome"]["simulated"],
            "Expected simulated outcome missing",
        )
        prior = store.snapshot()
        store.observe(record["id"], planner, record["fingerprint"])
        require(store.snapshot() == prior, "Outcome retry rewrote history")
    session = store.session(tokens["auditor"])
    require(session is not None, "Auditor session disappeared")
    store.revoke_session(session[2])
    require(store.session(tokens["auditor"]) is None, "Revoked session still verifies")
    final = store.snapshot()
    require(
        store.metrics_snapshot(approver)["effect_count"] == 2, "Wrong final unique effect count"
    )
    result = {
        "status": "PASS",
        "verified_at": datetime.now(UTC).isoformat(),
        "scope": (
            "Installed-wheel PostgreSQL initial-generation workflow and retained restart; "
            "synthetic identities and mock ERP"
        ),
        "specification_sha256": file_sha256(output / "specification.json"),
        "prepared_sha256": file_sha256(output / "prepared.json"),
        "package": spec["package"],
        "before_server": prepared["before_server"],
        "after_server": after_server,
        "migration_reapply": reapplied,
        "snapshot_sha256": digest(final),
        "audit_head": final["audit_head"],
        "checks": prepared["checks"]
        + [
            "retained_restart_identity_and_state",
            "persisted_JWT_sessions",
            "migration_seed_retry_no_expiry_extension",
            "concurrent_retry_one_mock_effect",
            "rejected_and_revoked_execution_denied",
            "planner_only_real_maturity_observation",
            "outcome_retry_unchanged",
            "session_revocation",
            "two_unique_mock_orders",
        ],
        "not_claimed": [
            "actual human approval",
            "external identity provider",
            "API/UI migration",
            "broker",
            "distributed atomicity",
            "backup/crash recovery",
            "Compose/Linux",
            "load/soak",
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
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--source", type=Path)
    args = parser.parse_args()
    if args.action == "prepare":
        if args.wheel is None or args.evidence is None or args.source is None:
            parser.error("prepare requires --wheel, --evidence and --source")
        result = prepare(args.cluster_root, args.output, args.evidence, args.source, args.wheel)
    else:
        result = verify(args.cluster_root, args.output)
    print(
        json.dumps(
            {
                "status": result["status"],
                "checks": len(result["checks"]),
                "output": str(args.output),
            }
        )
    )
