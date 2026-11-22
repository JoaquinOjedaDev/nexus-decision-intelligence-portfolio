"""Real local SQLite recovery with retained history, WAL and identity scrubbing."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.recovery import backup_store, restore_store, run_recovery_acceptance
from nexus.portfolio.runtime_control import RuntimeControl
from nexus.portfolio.store import PortfolioStore, canonical


def seed(source: str = "a") -> dict[str, Any]:
    return {
        "metadata": {
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": source * 64,
        },
        "recommendations": [
            {
                "id": f"rec-{index}",
                "sku_id": f"SKU-{index}",
                "location_id": location,
                "created_by": "planner.r4a",
                "recommended_quantity": 20,
                "expected_cost": 100,
            }
            for index, location in enumerate(("NORTH", "SOUTH"))
        ],
    }


@pytest.fixture
def prepared(tmp_path: Path) -> tuple[PortfolioStore, Actor]:
    store = PortfolioStore(tmp_path / "original", clock=lambda: 1_900_000_000.0)
    store.ensure_seed(seed())
    RuntimeControl(store)
    return store, Actor("recovery-system", "Recovery", ActorRole.SYSTEM, ("NORTH", "SOUTH"))


def execute(store: PortfolioStore, identity: str = "rec-0") -> tuple[str, str]:
    actor = resolve_persona("approver", store.locations())
    fingerprint = store.recommendation(identity, actor)["fingerprint"]
    store.decide(identity, actor, "approve", fingerprint, "Independent fixture review")
    result = store.execute(identity, actor, fingerprint, "fixture-recovery-key")
    return fingerprint, result["execution_ref"]


def update_manifest(bundle: Path, **updates: Any) -> None:
    manifest = json.loads((bundle / "manifest.json").read_text())
    data = (bundle / "portfolio.sqlite3").read_bytes()
    manifest.update(database_sha256=hashlib.sha256(data).hexdigest(), database_bytes=len(data))
    manifest.update(updates)
    encoded = canonical(manifest).encode()
    (bundle / "manifest.json").write_bytes(encoded)
    (bundle / "manifest.sha256").write_text(hashlib.sha256(encoded).hexdigest())


def test_session_secrets_never_reach_backup_bytes_and_old_tokens_fail_after_restore(
    prepared: tuple[PortfolioStore, Actor],
    tmp_path: Path,
) -> None:
    pytest.importorskip("jwt")
    store, admin = prepared
    old_token, session = store.create_session("auditor")
    with store.connection() as db:
        old_key = db.execute("SELECT value FROM metadata WHERE key='session_key'").fetchone()[0]
        identities = list(db.execute("SELECT id,csrf FROM sessions"))
    fingerprint, reference = execute(store)
    before = store.metrics_snapshot()
    manifest = backup_store(store, tmp_path / "backup", actor=admin)
    data = (tmp_path / "backup/portfolio.sqlite3").read_bytes()
    for secret in (old_key, session["csrf_token"], old_token, *(row[0] for row in identities)):
        assert secret.encode() not in data
    assert store.metrics_snapshot() == before
    assert store.session(old_token) is not None
    assert manifest["sessions_exported"] is manifest["runtime_evidence_restored"] is False
    receipt = restore_store(tmp_path / "backup", tmp_path / "restored", actor=admin)
    restored = PortfolioStore(tmp_path / "restored", clock=store.clock)
    assert restored.session(old_token) is None
    new_token, _ = restored.create_session("auditor")
    assert restored.session(new_token) is not None
    assert restored.metrics_snapshot() == before
    assert receipt["business_sha256"] == manifest["business_sha256"]
    assert receipt["runtime_evidence_restored"] is False
    assert (tmp_path / "restored/RESTORE_NOTICE.json").is_file()
    actor = resolve_persona("approver", restored.locations())
    assert (
        restored.execute("rec-0", actor, fingerprint, "fixture-recovery-key")["execution_ref"]
        == reference
    )
    assert restored.metrics_snapshot()["effect_count"] == 1


def test_online_backup_reads_committed_wal_snapshot_during_uncommitted_writer(
    prepared: tuple[PortfolioStore, Actor],
    tmp_path: Path,
) -> None:
    store, admin = prepared
    with store.connection() as writer:
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("BEGIN")
        writer.execute("SELECT COUNT(*) FROM recommendations").fetchone()
        execute(store)
        assert store.path.with_name("portfolio.sqlite3-wal").stat().st_size > 0
        writer.rollback()
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE recommendations SET state='rejected' WHERE id='rec-0'")
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(backup_store, store, tmp_path / "backup", actor=admin)
                manifest = pending.result(timeout=15)
        finally:
            writer.rollback()
    receipt = restore_store(tmp_path / "backup", tmp_path / "restored", actor=admin)
    assert receipt["business_sha256"] == manifest["business_sha256"]
    restored = PortfolioStore(tmp_path / "restored", clock=store.clock)
    assert restored.recommendation("rec-0", admin)["state"] == "executed"


def test_generation_history_and_attached_metadata_are_retained(
    prepared: tuple[PortfolioStore, Actor],
    tmp_path: Path,
) -> None:
    store, admin = prepared
    execute(store)
    control = RuntimeControl(store)
    now = datetime.fromtimestamp(store.clock(), UTC)
    control.submit(
        EventEnvelope(
            event_id=uuid4(),
            event_type="inventory.adjusted",
            source="fixture",
            subject_id="global",
            occurred_at=now,
            ingested_at=now,
            idempotency_key=uuid4().hex,
            payload={"source_snapshot_sha256": "b" * 64},
        ),
        resolve_persona("planner", admin.locations),
    )
    worker = replace(admin, actor_id="runtime-worker")
    job = control.claim(worker)
    assert job is not None
    control.activate(job["id"], job["token"], seed("b"), worker)
    with store.transaction() as db:
        db.execute("INSERT INTO metadata VALUES ('recovery_fixture_attached_summary','retained')")
    before = store.overview_snapshot(resolve_persona("auditor", admin.locations))
    backup_store(store, tmp_path / "backup", actor=admin)
    restore_store(tmp_path / "backup", tmp_path / "restored", actor=admin)
    restored = PortfolioStore(tmp_path / "restored", clock=store.clock)
    assert restored.overview_snapshot(resolve_persona("auditor", admin.locations)) == before
    assert (
        restored.recommendation_context("rec-0", resolve_persona("auditor", admin.locations))[
            "generation"
        ]["is_current"]
        is False
    )
    with restored.connection() as db:
        assert (
            db.execute(
                "SELECT value FROM metadata WHERE key='recovery_fixture_attached_summary'"
            ).fetchone()[0]
            == "retained"
        )


@pytest.mark.parametrize("invalid", ["planner", "partial"])
def test_authorization_precedes_export_and_restore(
    prepared: tuple[PortfolioStore, Actor],
    tmp_path: Path,
    invalid: str,
) -> None:
    store, admin = prepared
    denied = (
        resolve_persona("planner", admin.locations)
        if invalid == "planner"
        else replace(admin, locations=("NORTH",))
    )
    with pytest.raises(ValueError, match=r"SYSTEM|every retained"):
        backup_store(store, tmp_path / "denied-backup", actor=denied)
    assert not (tmp_path / "denied-backup").exists()
    backup_store(store, tmp_path / "backup", actor=admin)
    with pytest.raises(ValueError, match=r"SYSTEM|every retained"):
        restore_store(tmp_path / "backup", tmp_path / "denied-restore", actor=denied)
    assert not (tmp_path / "denied-restore").exists()


@pytest.mark.parametrize("damage", ["database", "manifest", "contract", "schema", "sessions"])
def test_corrupt_incompatible_or_secret_containing_bundle_refused_before_output(
    prepared: tuple[PortfolioStore, Actor],
    tmp_path: Path,
    damage: str,
) -> None:
    store, admin = prepared
    bundle = tmp_path / "backup"
    backup_store(store, bundle, actor=admin)
    if damage == "database":
        data = (bundle / "portfolio.sqlite3").read_bytes()
        (bundle / "portfolio.sqlite3").write_bytes(data[:-1])
    elif damage == "manifest":
        (bundle / "manifest.json").write_text("{}")
    elif damage == "contract":
        update_manifest(bundle, contract="future-incompatible.v9")
    else:
        with closing(sqlite3.connect(bundle / "portfolio.sqlite3")) as db, db:
            if damage == "schema":
                db.execute("ALTER TABLE recommendations ADD COLUMN unexpected TEXT")
            else:
                db.execute("INSERT INTO metadata VALUES ('session_key','forbidden-old-secret')")
        update_manifest(bundle)
    with pytest.raises(ValueError, match=r"checksum|Incompatible|session identity"):
        restore_store(bundle, tmp_path / "refused", actor=admin)
    assert not (tmp_path / "refused").exists()


def test_existing_nested_and_reserved_outputs_never_overwritten(
    prepared: tuple[PortfolioStore, Actor],
    tmp_path: Path,
) -> None:
    store, admin = prepared
    empty = tmp_path / "empty"
    empty.mkdir()
    sentinel = tmp_path / "keep"
    sentinel.write_text("user data")
    for path in (
        empty,
        sentinel,
        store.path.parent / "nested",
        tmp_path / "portfolio",
        tmp_path / "PoRtFoLiO",
    ):
        with pytest.raises(ValueError):
            backup_store(store, path, actor=admin)
    assert sentinel.read_text() == "user data"
    assert list(empty.iterdir()) == []
    with pytest.raises(ValueError, match="allow_mutation"):
        run_recovery_acceptance(tmp_path / "not-created")
    assert not (tmp_path / "not-created").exists()


def test_real_process_termination_recovers_acknowledged_effect_and_new_identity(
    tmp_path: Path,
) -> None:
    pytest.importorskip("jwt")
    result = run_recovery_acceptance(tmp_path / "crash-drill", allow_mutation=True)
    assert result["status"] == "pass"
    assert all(result["checks"].values())
    assert result["crash"]["child_exit_code"] != 0
    assert result["crash"]["acknowledgment"]["uncommitted_write_open"] is True
    for phase in ("acknowledged_elapsed_ms", "termination_elapsed_ms", "restart_elapsed_ms"):
        assert result["crash"][phase] > 0
    assert result["backup"]["elapsed_ms"] > 0
    assert result["restore"]["elapsed_ms"] > 0
    assert result["runtime_evidence_restored"] is False
    with pytest.raises(ValueError, match="new path"):
        run_recovery_acceptance(tmp_path / "crash-drill", allow_mutation=True)
