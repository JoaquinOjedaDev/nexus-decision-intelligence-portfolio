"""Pure fail-closed checks for the native PostgreSQL acceptance coordinator.

All database, runtime and server operations below are explicit fakes. These
tests verify coordinator control flow, not actual PostgreSQL/restart evidence.
The actual service acceptance and its retained receipt remain separate.
"""

from __future__ import annotations

import copy
import importlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("psycopg", reason="Acceptance coordinator uses the optional service dependency")
acceptance = importlib.import_module("scripts.postgres_foundation_acceptance")
pytestmark = pytest.mark.portfolio_service


def _seed() -> dict[str, Any]:
    return {
        "executable": True,
        "recommendations": [{"id": "fixture-recommendation"}],
        "artifacts": [],
        "metadata": {
            "source_snapshot_sha256": "a" * 64,
            "classification": "synthetic",
            "scientific_decision_time": "2026-01-01T00:00:00+00:00",
        },
        "metrics": {
            "dbt_status": "PASS",
            "independently_feasible": True,
            "source_rows": 20,
            "gold_rows": 16,
            "solver_status": "OPTIMAL",
        },
    }


@dataclass
class FakeBackend:
    label: str
    calls: list[str]
    state: dict[str, Any]
    migration: dict[str, Any]
    config: object = field(default_factory=object)

    def migrate(self, **_: Any) -> dict[str, Any]:
        self.calls.append(f"{self.label}.migrate")
        return copy.deepcopy(self.migration)

    def ensure_seed(self, value: dict[str, Any]) -> None:
        self.calls.append(f"{self.label}.ensure_seed")
        assert value == self.state["seed"]

    def snapshot(self) -> dict[str, Any]:
        self.calls.append(f"{self.label}.snapshot")
        return copy.deepcopy(self.state)


@dataclass
class Harness:
    cluster: Path
    output: Path
    evidence: Path
    source: Path
    seed: dict[str, Any]
    calls: list[str]
    config: dict[str, Any]
    server: dict[str, Any]
    pg: FakeBackend
    retry: FakeBackend
    original: dict[str, Any]


@pytest.fixture
def harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Harness:
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    (evidence / acceptance.RUNTIME_SEED_FILENAME).write_bytes(b"constructed coordinator fixture\n")
    seed = _seed()
    state = {
        "seed": seed,
        "seed_sha256": acceptance.digest(seed),
        "recommendations": [{"id": "fixture-recommendation", "created_at": 1234.5}],
        "events": [{"sequence": 1, "event_type": "constructed.fixture"}],
        "audit_head": "b" * 64,
        "event_count": 1,
    }
    migration = {
        "version": 2,
        "migration_sha256": "c" * 64,
        "schema": "nexus_accept_fixture",
        "server_version": "18.1",
        "applied": False,
    }
    calls: list[str] = []
    result = Harness(
        cluster=tmp_path / "cluster",
        output=tmp_path / "acceptance",
        evidence=evidence,
        source=tmp_path / "source",
        seed=seed,
        calls=calls,
        config={
            "cluster_id": "coordinator-fixture-not-a-real-cluster",
            "application_dsn": "never-connect-fake",
            "migration_dsn": "never-connect-fake",
        },
        server={
            "cluster_id": "coordinator-fixture-not-a-real-cluster",
            "system_identifier": "fixture-system-identifier",
            "postmaster_started_at": "2026-09-14T01:00:00+00:00",
            "binary_hashes": {"postgres.exe": "d" * 64},
        },
        pg=FakeBackend("pg", calls, copy.deepcopy(state), copy.deepcopy(migration)),
        retry=FakeBackend("retry", calls, copy.deepcopy(state), copy.deepcopy(migration)),
        original=copy.deepcopy(state),
    )

    def forbidden_connect(*_: Any, **__: Any) -> None:
        pytest.fail("Pure acceptance coordinator tests must never connect to PostgreSQL")

    def fake_runtime(*_: Any) -> dict[str, Any]:
        calls.append("runtime")
        return copy.deepcopy(result.seed)

    def fake_backend(*_: Any) -> FakeBackend:
        calls.append("backend")
        return result.pg

    def fake_server(*_: Any) -> dict[str, Any]:
        calls.append("server")
        return copy.deepcopy(result.server)

    def fake_config(*_: Any) -> dict[str, Any]:
        calls.append("config")
        return copy.deepcopy(result.config)

    monkeypatch.setattr(acceptance.psycopg, "connect", forbidden_connect)
    monkeypatch.setattr(acceptance, "safe_root", lambda path: path.resolve())
    monkeypatch.setattr(acceptance, "read_config", fake_config)
    monkeypatch.setattr(acceptance, "server_identity", fake_server)
    monkeypatch.setattr(acceptance, "runtime", fake_runtime)
    monkeypatch.setattr(acceptance, "backend", fake_backend)
    monkeypatch.setattr(acceptance, "code_identity", lambda: {"fixture-code": "e" * 64})
    monkeypatch.setattr(acceptance, "PostgresBackend", lambda *_: result.retry)
    monkeypatch.setattr(
        acceptance, "PortfolioStore", lambda *_, **__: SimpleNamespace(ensure_seed=lambda _: None)
    )
    monkeypatch.setattr(acceptance, "sqlite_projection", lambda _: copy.deepcopy(result.original))
    return result


@pytest.fixture
def prepared(harness: Harness) -> Harness:
    harness.output.mkdir()
    spec = {
        "cluster_id": harness.config["cluster_id"],
        "system_identifier": harness.server["system_identifier"],
        "schema": "nexus_accept_fixture",
        "evidence_directory": str(harness.evidence),
        "source_directory": str(harness.source),
        "creation_epoch": 1234.5,
        "code_sha256": {"fixture-code": "e" * 64},
        "seed_file_sha256": acceptance.sha(harness.evidence / acceptance.RUNTIME_SEED_FILENAME),
        "canonical_seed_sha256": acceptance.digest(harness.seed),
    }
    acceptance.retain(harness.output / "specification.json", spec)
    before = {
        "status": "PREPARED_RESTART_NOT_RUN",
        "prepared_at": "2026-09-14T00:30:00+00:00",
        "specification_sha256": acceptance.sha(harness.output / "specification.json"),
        "snapshot_sha256": acceptance.digest(harness.original),
        "server": {
            **harness.server,
            "postmaster_started_at": "2026-09-14T00:00:00+00:00",
        },
        "migration": {**harness.pg.migration, "applied": True},
    }
    acceptance.retain(harness.output / "prepared.json", before)
    return harness


def test_runtime_missing_evidence_cannot_implicitly_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*_: Any, **__: Any) -> None:
        pytest.fail("Missing retained evidence cannot invoke validation or dbt construction")

    monkeypatch.setattr(acceptance, "verify_runtime_current", forbidden)
    monkeypatch.setattr(acceptance, "load_runtime_seed", forbidden)
    with pytest.raises(ValueError, match="completed retained runtime"):
        acceptance.runtime(tmp_path / "missing", tmp_path / "source")
    assert not (tmp_path / "missing").exists()


def test_runtime_source_mismatch_is_refused_before_seed_builder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / acceptance.RUNTIME_SEED_FILENAME).write_bytes(b"retained fixture")
    monkeypatch.setattr(acceptance, "verify_runtime_current", lambda *_: _seed())
    monkeypatch.setattr(acceptance, "runtime_source_fingerprint", lambda _: "f" * 64)

    def forbidden(*_: Any, **__: Any) -> None:
        pytest.fail("A mismatched source cannot invoke the seed builder")

    monkeypatch.setattr(acceptance, "_request_fingerprint", forbidden)
    with pytest.raises(ValueError, match="Current source bytes differ"):
        acceptance.runtime(tmp_path, tmp_path / "source")


@pytest.mark.parametrize("state", ["valid", "dbt_failed", "infeasible", "not_synthetic"])
def test_runtime_uses_current_request_identity_and_retains_validation_gates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    retained = tmp_path / acceptance.RUNTIME_SEED_FILENAME
    retained.write_bytes(b"read-only retained fixture")
    source = tmp_path / "source"
    candidate = _seed()
    if state == "dbt_failed":
        candidate["metrics"]["dbt_status"] = "FAIL"
    elif state == "infeasible":
        candidate["metrics"]["independently_feasible"] = False
    elif state == "not_synthetic":
        candidate["metadata"]["classification"] = "production"
    policy = object()
    calls = []

    def fingerprint(project: Path, path: Path, loaded: object, cutoff: Any, scope: Any) -> str:
        assert project == acceptance.PROJECT and path == source and loaded is policy
        assert cutoff.isoformat() == _seed()["metadata"]["scientific_decision_time"]
        assert scope is None
        calls.append("fingerprint")
        return "f" * 64

    def load(path: Path, *, expected_fingerprint: str) -> dict[str, Any]:
        assert path == tmp_path and expected_fingerprint == "f" * 64
        calls.append("read-only-load")
        return candidate

    monkeypatch.setattr(acceptance, "verify_runtime_current", lambda *_: _seed())
    monkeypatch.setattr(acceptance, "runtime_source_fingerprint", lambda _: "a" * 64)
    monkeypatch.setattr(acceptance, "load_runtime_policy", lambda _: policy)
    monkeypatch.setattr(acceptance, "_request_fingerprint", fingerprint)
    monkeypatch.setattr(acceptance, "load_runtime_seed", load)
    if state == "valid":
        assert acceptance.runtime(tmp_path, source) == candidate
    else:
        with pytest.raises(ValueError, match="executable, validated synthetic planning seed"):
            acceptance.runtime(tmp_path, source)
    assert calls == ["fingerprint", "read-only-load"]
    assert retained.read_bytes() == b"read-only retained fixture"


def test_prepare_existing_output_refuses_all_external_operations(harness: Harness) -> None:
    harness.output.mkdir()
    retained = harness.output / "earlier-attempt.txt"
    retained.write_text("preserve me", encoding="utf-8")
    with pytest.raises(ValueError, match="new acceptance output"):
        acceptance.prepare(harness.cluster, harness.output, harness.evidence, harness.source)
    assert harness.calls == []
    assert retained.read_text(encoding="utf-8") == "preserve me"


def test_prepare_runtime_refusal_happens_before_output_or_database_mutations(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    def changed_runtime(*_: Any) -> None:
        raise ValueError("Current source bytes differ from the retained runtime")

    monkeypatch.setattr(acceptance, "runtime", changed_runtime)
    with pytest.raises(ValueError, match="Current source bytes differ"):
        acceptance.prepare(harness.cluster, harness.output, harness.evidence, harness.source)
    assert harness.calls == ["config", "server"]
    assert not harness.output.exists()


def test_prepare_success_retains_a_pending_restart_not_pass(harness: Harness) -> None:
    harness.pg.migration["applied"] = True
    receipt = acceptance.prepare(harness.cluster, harness.output, harness.evidence, harness.source)
    assert receipt["status"] == "PREPARED_RESTART_NOT_RUN"
    assert harness.calls == [
        "config",
        "server",
        "runtime",
        "backend",
        "pg.migrate",
        "pg.ensure_seed",
        "pg.snapshot",
    ]
    assert (harness.output / "specification.json").is_file()
    assert (harness.output / "prepared.json").is_file()
    assert not (harness.output / "result.json").exists()


@pytest.mark.parametrize("changed", ["code", "specification"])
def test_verify_changed_governing_identity_refuses_before_server_or_database(
    prepared: Harness, monkeypatch: pytest.MonkeyPatch, changed: str
) -> None:
    if changed == "code":
        monkeypatch.setattr(acceptance, "code_identity", lambda: {"fixture-code": "f" * 64})
    else:
        path = prepared.output / "specification.json"
        path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="specification or governing code changed"):
        acceptance.verify(prepared.cluster, prepared.output)
    assert prepared.calls == []
    assert not (prepared.output / "result.json").exists()


@pytest.mark.parametrize(
    ("field_name", "new_value"),
    [
        ("cluster_id", "another-cluster"),
        ("system_identifier", "another-system"),
        ("postmaster_started_at", "2026-09-14T00:00:00+00:00"),
        ("postmaster_started_at", "2026-09-14T00:15:00+00:00"),
        ("binary_hashes", {"postgres.exe": "f" * 64}),
    ],
)
def test_verify_requires_same_cluster_and_a_distinct_postmaster_start(
    prepared: Harness, field_name: str, new_value: Any
) -> None:
    prepared.server[field_name] = new_value
    with pytest.raises(ValueError, match="retained restart of the same task-owned cluster"):
        acceptance.verify(prepared.cluster, prepared.output)
    assert prepared.calls == ["config", "server"]
    assert not (prepared.output / "result.json").exists()


@pytest.mark.parametrize("changed", ["file", "canonical_seed"])
def test_verify_changed_source_backed_seed_refuses_before_database(
    prepared: Harness, changed: str
) -> None:
    if changed == "file":
        (prepared.evidence / acceptance.RUNTIME_SEED_FILENAME).write_bytes(b"changed seed")
    else:
        prepared.seed["metadata"]["source_snapshot_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="Retained source-backed seed changed"):
        acceptance.verify(prepared.cluster, prepared.output)
    assert prepared.calls == ["config", "server", "runtime"]
    assert not (prepared.output / "result.json").exists()


def test_verify_changed_snapshot_refuses_before_bootstrap_retry(prepared: Harness) -> None:
    prepared.pg.state["event_count"] = 9
    with pytest.raises(ValueError, match="Database state did not survive restart unchanged"):
        acceptance.verify(prepared.cluster, prepared.output)
    assert prepared.calls == ["config", "server", "runtime", "backend", "pg.snapshot"]
    assert not (prepared.output / "result.json").exists()


@pytest.mark.parametrize("changed", ["retry_snapshot", "migration_identity", "migration_reapplied"])
def test_verify_changed_retry_never_writes_pass(prepared: Harness, changed: str) -> None:
    if changed == "retry_snapshot":
        prepared.retry.state["recommendations"][0]["created_at"] = 5678.0
    elif changed == "migration_identity":
        prepared.pg.migration["migration_sha256"] = "f" * 64
    else:
        prepared.pg.migration["applied"] = True
    with pytest.raises(ValueError, match="Bootstrap retry changed"):
        acceptance.verify(prepared.cluster, prepared.output)
    assert not (prepared.output / "result.json").exists()


def test_verify_healthy_restart_accepts_idempotent_migration_not_reapplication(
    prepared: Harness,
) -> None:
    receipt = acceptance.verify(prepared.cluster, prepared.output)
    assert receipt["status"] == "PASS"
    assert all(receipt["checks"].values())
    assert (
        receipt["before_server"]["postmaster_started_at"]
        != receipt["after_server"]["postmaster_started_at"]
    )
    assert receipt["migration_reapply"]["applied"] is False
    assert "API or workflow migration" in receipt["not_claimed"]
    assert json.loads((prepared.output / "result.json").read_text(encoding="utf-8")) == receipt
