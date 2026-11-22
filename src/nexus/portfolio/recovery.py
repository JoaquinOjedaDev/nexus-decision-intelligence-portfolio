"""Local workflow-database recovery, not an environment restore or model promotion.

The online SQLite snapshot is scrubbed in memory before any backup file is written.
Bundles contain business state only; Gold/model files and sidecar databases remain
outside this contract. Checksums detect corruption, not a malicious administrator.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import sqlite3
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.runtime_control import RuntimeControl
from nexus.portfolio.store import PortfolioStore, canonical, digest

CONTRACT = "nexus.portfolio.database-backup.v1"
NOTICE = (
    "Database-only recovery. Runtime evidence, source/Gold/model artifacts, monitoring and "
    "learning-book sidecars were not restored. Do not serve this directory as a complete "
    "runtime or promote it from DEV to RELEASE. Reconcile verified external evidence and "
    "worker leases separately; log in again using a new local session."
)
_RESERVED = {"portfolio", "runtime-learning-demo-v2", "runtime-browser-review"}
_MAX_DATABASE_BYTES = 256 * 1024 * 1024


def _admin(actor: Actor, locations: list[str] | None = None) -> None:
    if actor.role is not ActorRole.SYSTEM:
        raise ValueError("Recovery requires an explicit trusted SYSTEM actor")
    if locations is not None and not set(locations).issubset(actor.locations):
        raise ValueError("Recovery requires every retained location, including history")


def _local_path(target: Path) -> Path:
    path = target.absolute()
    if path.drive.startswith("\\\\") or any(
        part.is_symlink() or part.is_junction() for part in (path, *path.parents)
    ):
        raise ValueError("Recovery requires local paths without UNC shares or linked ancestors")
    return path


def _new_target(target: Path, source: Path | None = None) -> Path:
    path = _local_path(target)
    if path.exists() or path.is_symlink() or path.is_junction():
        raise ValueError(
            "Recovery output must be a new path; existing directories are never replaced"
        )
    if not path.parent.is_dir() or any(part.casefold() in _RESERVED for part in path.parts):
        raise ValueError("Choose a new isolated output below an existing non-demo parent directory")
    resolved = path.resolve()
    if source is not None and resolved.is_relative_to(source.resolve()):
        raise ValueError("Recovery output cannot be nested inside its source")
    return resolved


def _schema(db: sqlite3.Connection) -> str:
    rows = db.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
    ).fetchall()
    return digest(
        {
            "objects": [list(row) for row in rows],
            "user_version": db.execute("PRAGMA user_version").fetchone()[0],
            "application_id": db.execute("PRAGMA application_id").fetchone()[0],
        }
    )


@lru_cache(maxsize=2)
def _supported_schema(runtime: bool) -> str:
    # Compare with this installed implementation, not a caller-supplied expected hash.
    with tempfile.TemporaryDirectory(prefix="nexus-recovery-schema-") as directory:
        store = PortfolioStore(Path(directory))
        if runtime:
            RuntimeControl(store)
        with store.connection() as db:
            return _schema(db)


def _validate(db: sqlite3.Connection, actor: Actor) -> dict[str, Any]:
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA trusted_schema=OFF")
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
    runtime = "runtime_jobs" in tables
    if _schema(db) != _supported_schema(runtime):
        raise ValueError("Incompatible database schema; migrations are outside recovery scope")
    locations = [
        row[0]
        for row in db.execute(
            "SELECT DISTINCT location_id FROM recommendations ORDER BY location_id"
        )
    ]
    _admin(actor, locations)
    if [row[0] for row in db.execute("PRAGMA integrity_check")] != ["ok"] or list(
        db.execute("PRAGMA foreign_key_check")
    ):
        raise ValueError("SQLite integrity or foreign-key validation failed")
    events = PortfolioStore._verify_events(db)
    seed, seed_hash, owners = PortfolioStore._generation_catalog(db)
    records = db.execute("SELECT * FROM recommendations ORDER BY id").fetchall()
    if {row["id"] for row in records} != set(owners):
        raise ValueError("Recommendation history differs from its generation catalog")
    for row in records:
        if digest(json.loads(row["payload"])) != row["fingerprint"]:
            raise ValueError("Recommendation payload integrity failed")
        PortfolioStore._assert_generation_item(row, owners[row["id"]][2])
        PortfolioStore._assert_projection(db, row)
    # The complete business digest also binds outbox/idempotency/runtime/learning metadata.
    business = {}
    for table in sorted(tables - {"sessions", "session_tokens", "sqlite_sequence"}):
        query = f'SELECT * FROM "{table}"'
        if table == "metadata":
            query += " WHERE key!='session_key'"
        business[table] = sorted((dict(row) for row in db.execute(query)), key=canonical)
    return {
        "schema_sha256": _schema(db),
        "business_sha256": digest(business),
        "seed_sha256": seed_hash,
        "locations": locations,
        "event_count": len(events),
        "recommendation_count": len(records),
        "effect_count": len(business["effects"]),
        "pipeline_mode": seed.get("metadata", {}).get("pipeline_mode", "frozen_demo"),
    }


def _write(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def backup_store(store: PortfolioStore, backup_dir: Path, *, actor: Actor) -> dict[str, Any]:
    """Create a new, sanitized consistent snapshot. The source is never modified."""
    _admin(actor)
    _local_path(store.path.parent)
    target = _new_target(backup_dir, store.path.parent)
    started = time.perf_counter()
    with closing(sqlite3.connect(":memory:")) as snapshot, store.connection() as source:
        source.execute("BEGIN")
        _admin(
            actor,
            [row[0] for row in source.execute("SELECT DISTINCT location_id FROM recommendations")],
        )
        if (
            source.execute("PRAGMA page_count").fetchone()[0]
            * source.execute("PRAGMA page_size").fetchone()[0]
            > _MAX_DATABASE_BYTES
        ):
            raise ValueError("Database exceeds the bounded local recovery profile")
        source.backup(snapshot)
        source.rollback()
        binding = _validate(snapshot, actor)
        snapshot.execute("PRAGMA secure_delete=ON")
        snapshot.execute("DELETE FROM session_tokens")
        snapshot.execute("DELETE FROM sessions")
        snapshot.execute("DELETE FROM metadata WHERE key='session_key'")
        snapshot.commit()
        snapshot.execute("VACUUM")
        if _validate(snapshot, actor) != binding:
            raise ValueError("Identity scrubbing changed business state")
        target.mkdir(mode=0o700)
        database = target / "portfolio.sqlite3"
        with closing(sqlite3.connect(database)) as destination:
            snapshot.backup(destination)
        with database.open("r+b") as handle:
            os.fsync(handle.fileno())
        manifest = {
            "contract": CONTRACT,
            "database": "portfolio.sqlite3",
            **binding,
            "database_sha256": hashlib.sha256(database.read_bytes()).hexdigest(),
            "database_bytes": database.stat().st_size,
            "sessions_exported": False,
            "runtime_evidence_restored": False,
            "scope": NOTICE,
            "created_at": datetime.now(UTC).isoformat(),
            "elapsed_ms": (time.perf_counter() - started) * 1000,
        }
        encoded = canonical(manifest).encode()
        _write(target / "manifest.json", encoded)
        _write(target / "manifest.sha256", hashlib.sha256(encoded).hexdigest().encode())
    return manifest


def restore_store(backup_dir: Path, target_state_dir: Path, *, actor: Actor) -> dict[str, Any]:
    """Verify before creating a new DB-only target; generate a fresh session signing key."""
    _admin(actor)
    _local_path(backup_dir)
    target = _new_target(target_state_dir, backup_dir)
    started = time.perf_counter()
    if backup_dir.is_symlink() or backup_dir.is_junction():
        raise ValueError("Backup bundles cannot be links")
    for name in ("portfolio.sqlite3", "manifest.json", "manifest.sha256"):
        path = backup_dir / name
        if not path.is_file() or path.is_symlink():
            raise ValueError("Backup bundle is incomplete or linked")
    if (backup_dir / "manifest.json").stat().st_size > 65_536 or (
        backup_dir / "portfolio.sqlite3"
    ).stat().st_size > _MAX_DATABASE_BYTES:
        raise ValueError("Backup exceeds the bounded local recovery profile")
    encoded = (backup_dir / "manifest.json").read_bytes()
    if hashlib.sha256(encoded).hexdigest() != (backup_dir / "manifest.sha256").read_text():
        raise ValueError("Backup manifest checksum mismatch")
    manifest = json.loads(encoded)
    if manifest.get("contract") != CONTRACT or manifest.get("database") != "portfolio.sqlite3":
        raise ValueError("Incompatible backup contract")
    _admin(actor, manifest["locations"])
    data = (backup_dir / "portfolio.sqlite3").read_bytes()
    if (
        hashlib.sha256(data).hexdigest() != manifest["database_sha256"]
        or len(data) != manifest["database_bytes"]
    ):
        raise ValueError("Backup database checksum mismatch")
    with closing(sqlite3.connect(":memory:")) as snapshot:
        snapshot.deserialize(data)
        binding = _validate(snapshot, actor)
        if any(binding[key] != manifest[key] for key in binding):
            raise ValueError("Backup manifest and business snapshot disagree")
        if (
            any(
                snapshot.execute(f"SELECT 1 FROM {table}").fetchone()
                for table in ("sessions", "session_tokens")
            )
            or snapshot.execute("SELECT 1 FROM metadata WHERE key='session_key'").fetchone()
        ):
            raise ValueError("Backup contains forbidden session identity material")
        target.mkdir(mode=0o700)
        with closing(sqlite3.connect(target / "portfolio.sqlite3")) as destination:
            snapshot.backup(destination)
    restored = PortfolioStore(target)
    with restored.connection() as db:
        if _validate(db, actor) != binding:
            raise ValueError("Restored business state differs from its backup")
    receipt = {
        "contract": "nexus.portfolio.database-restore.v1",
        **binding,
        "runtime_evidence_restored": False,
        "sessions_invalidated": True,
        "fresh_session_key_created": True,
        "scope": NOTICE,
        "backup_manifest_sha256": hashlib.sha256(encoded).hexdigest(),
        "elapsed_ms": (time.perf_counter() - started) * 1000,
    }
    _write(target / "RESTORE_NOTICE.json", canonical(receipt).encode())
    return receipt


_CHILD = r"""
import json, sqlite3, sys
from pathlib import Path
from nexus.portfolio.store import PortfolioStore
from nexus.portfolio.auth import resolve_persona
store=PortfolioStore(Path(sys.argv[1]))
db=sqlite3.connect(store.path)
db.execute('BEGIN')
db.execute('SELECT COUNT(*) FROM recommendations').fetchone()
actor=resolve_persona('approver',store.locations())
item=store.recommendation('recovery-rec',actor)
store.decide(item['id'],actor,'approve',item['fingerprint'],'Independent crash drill')
result=store.execute(item['id'],actor,item['fingerprint'],'recovery-idempotent-key')
db.rollback()
db.execute('BEGIN IMMEDIATE')
db.execute("UPDATE recommendations SET state='rejected' WHERE id='recovery-rec'")
print(json.dumps({'acknowledged':'executed','fingerprint':item['fingerprint'],
                  'execution_ref':result['execution_ref'],'uncommitted_write_open':True,
                  'committed_wal_bytes':store.path.with_name('portfolio.sqlite3-wal').stat().st_size}),flush=True)
sys.stdin.read()
"""


def run_recovery_acceptance(output_dir: Path, *, allow_mutation: bool = False) -> dict[str, Any]:
    """Terminate only a child we create, after its committed effect is acknowledged."""
    if allow_mutation is not True:
        raise ValueError("Explicit allow_mutation=True is required for a new isolated drill")
    target = _new_target(output_dir)
    source_files = [
        Path(__file__).with_name(name)
        for name in ("recovery.py", "store.py", "auth.py", "runtime_control.py")
    ]
    code = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in source_files}
    target.mkdir(mode=0o700)
    store = PortfolioStore(target / "original")
    store.ensure_seed(
        {
            "metadata": {
                "synthetic": True,
                "pipeline_mode": "connected_runtime_baseline",
                "source_snapshot_sha256": "a" * 64,
            },
            "recommendations": [
                {
                    "id": "recovery-rec",
                    "sku_id": "RECOVERY-SKU",
                    "location_id": "RECOVERY-ONLY",
                    "created_by": "planner.r4a",
                    "recommended_quantity": 20,
                    "expected_cost": 100,
                }
            ],
        }
    )
    RuntimeControl(store)
    admin = Actor("recovery-registrar", "Recovery", ActorRole.SYSTEM, store.locations())
    old_token, _ = store.create_session("auditor")
    before = time.perf_counter()
    child = subprocess.Popen(
        [sys.executable, "-u", "-c", _CHILD, str(store.path.parent)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    try:
        assert child.stdout is not None
        with ThreadPoolExecutor(max_workers=1) as reader:
            line = reader.submit(child.stdout.readline)
            try:
                acknowledged = json.loads(line.result(timeout=30))
            except BaseException:
                child.terminate()
                child.wait(timeout=10)
                raise
        acknowledged_ms = (time.perf_counter() - before) * 1000
        killed = time.perf_counter()
        child.terminate()
        child.wait(timeout=10)
        terminate_ms = (time.perf_counter() - killed) * 1000
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
        for handle in (child.stdin, child.stdout, child.stderr):
            if handle is not None:
                handle.close()
    recovered_at = time.perf_counter()
    reopened = PortfolioStore(store.path.parent)
    approver = resolve_persona("approver", reopened.locations())
    recovered = reopened.recommendation("recovery-rec", approver)
    duplicate = reopened.execute(
        "recovery-rec", approver, acknowledged["fingerprint"], "recovery-idempotent-key"
    )
    restart_ms = (time.perf_counter() - recovered_at) * 1000
    backup = backup_store(reopened, target / "backup", actor=admin)
    restore = restore_store(target / "backup", target / "restored", actor=admin)
    restored = PortfolioStore(target / "restored")
    restored_duplicate = restored.execute(
        "recovery-rec", approver, acknowledged["fingerprint"], "recovery-idempotent-key"
    )
    new_token, _ = restored.create_session("auditor")
    checks = {
        "committed_wal_present_at_crash": acknowledged["committed_wal_bytes"] > 0,
        "independent_approval_retained": recovered["approved_by"] == approver.actor_id
        and recovered["approved_by"] != recovered["created_by"],
        "acknowledged_effect_survives": recovered["state"] == "executed",
        "uncommitted_state_rolled_back": recovered["state"] != "rejected",
        "same_effect_after_duplicate": duplicate["execution_ref"] == acknowledged["execution_ref"],
        "restored_duplicate_same_effect": restored_duplicate["execution_ref"]
        == acknowledged["execution_ref"],
        "exactly_one_restored_effect": restored.metrics_snapshot()["effect_count"] == 1,
        "old_session_invalid": restored.session(old_token) is None,
        "fresh_session_valid": restored.session(new_token) is not None,
        "business_snapshot_equal": backup["business_sha256"] == restore["business_sha256"],
        "code_unchanged": code
        == {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in source_files},
    }
    receipt = {
        "contract": "nexus.portfolio.recovery-acceptance.v1",
        "status": "pass" if all(checks.values()) else "fail",
        "checks": checks,
        "recorded_at": datetime.now(UTC).isoformat(),
        "environment": {
            "system": platform.system(),
            "release": platform.release(),
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "machine": platform.machine(),
            "processor": platform.processor(),
            "logical_cpus": os.cpu_count(),
        },
        "binding": {
            "code_files": code,
            "code_sha256": digest(code),
            "fixture_kind": "synthetic_workflow_contract_fixture_no_data_pipeline",
            "seed_sha256": backup["seed_sha256"],
        },
        "crash": {
            "child_pid": child.pid,
            "child_exit_code": child.returncode,
            "mechanism": "TerminateProcess" if os.name == "nt" else "SIGTERM",
            "acknowledgment": acknowledged,
            "acknowledged_elapsed_ms": acknowledged_ms,
            "termination_elapsed_ms": terminate_ms,
            "restart_elapsed_ms": restart_ms,
        },
        "backup": backup,
        "restore": restore,
        "runtime_evidence_restored": False,
        "scope": NOTICE,
        "limitations": [
            "Single-host process crash, not host-power loss or disk failure",
            "No migrations, projection rebuild, multi-database atomic recovery, "
            "worker-lease recovery, external artifact restore, or production RTO/RPO claim",
        ],
    }
    _write(target / "recovery-acceptance.json", canonical(receipt).encode())
    return receipt
