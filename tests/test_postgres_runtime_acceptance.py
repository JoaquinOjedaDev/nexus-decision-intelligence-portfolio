"""Portable orchestration checks, not PostgreSQL/restart/scientific evidence.

Every database, native lifecycle and producer dependency is replaced explicitly.
Constructed phase receipts exercise refusal ordering and cannot satisfy the
separately run, installed-wheel native acceptance.
"""

from __future__ import annotations

import copy
import importlib
import json
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("psycopg", reason="Coordinator imports the optional service dependency")
acceptance = importlib.import_module("scripts.postgres_runtime_acceptance")
workflow_harness = importlib.import_module("scripts.postgres_workflow_wheel_acceptance")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


@pytest.fixture(autouse=True)
def forbid_real_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_: Any, **__: Any) -> None:
        pytest.fail("Portable tests must not call native services, databases or real builds")

    monkeypatch.setattr(workflow_harness.psycopg, "connect", forbidden)
    monkeypatch.setattr(workflow_harness, "native_helper", forbidden)
    for name in (
        "helpers",
        "build_runtime_seed",
        "runtime_store",
        "PostgresWorkflowStore",
        "PostgresRuntimeControl",
    ):
        monkeypatch.setattr(acceptance, name, forbidden)


@dataclass
class PackageFixture:
    wheel: Path
    project: Path
    installed: Path
    helper: Any
    origins: dict[str, Any]
    migration: bytes


@pytest.fixture
def package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PackageFixture:
    project, installed = tmp_path / "project", tmp_path / "site-packages"
    migration = b"-- constructed portable migration\n"
    path = project / "infra/postgres/migrations/004_portfolio_runtime.sql"
    path.parent.mkdir(parents=True)
    path.write_bytes(migration)
    runner = project / "scripts/postgres_runtime_acceptance.py"
    runner.parent.mkdir()
    runner.write_bytes(b"# constructed harness identity\n")
    wheel = tmp_path / "constructed.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("nexus/portfolio/sql_migrations/004_portfolio_runtime.sql", migration)
    origins = {
        name: SimpleNamespace(__file__=installed / (name.replace(".", "/") + ".py"))
        for name in ("nexus.portfolio.postgres_runtime", "nexus.portfolio.runtime_acceptance")
    }
    helper = SimpleNamespace(
        installed_identity=lambda _: {"installed_root": str(installed), "fixture": True},
        require=workflow_harness.require,
    )
    monkeypatch.setattr(acceptance, "PROJECT", project)
    monkeypatch.setattr(acceptance, "__file__", str(runner))
    monkeypatch.setattr(acceptance, "sys", SimpleNamespace(modules=origins))
    monkeypatch.setattr(acceptance, "read_migration_sql", lambda _: migration)
    return PackageFixture(wheel, project, installed, helper, origins, migration)


def test_package_identity_adds_runtime_harness_and_canonical_migration_binding(
    package: PackageFixture,
) -> None:
    result = acceptance.package_identity(package.helper, package.wheel)
    assert result["fixture"] is True
    assert result["runtime_acceptance_sha256"] == acceptance.file_sha256(
        package.project / "scripts/postgres_runtime_acceptance.py"
    )
    assert result["runtime_migration_sha256"] == acceptance.file_sha256(
        package.project / "infra/postgres/migrations/004_portfolio_runtime.sql"
    )


@pytest.mark.parametrize(
    "name", ["nexus.portfolio.postgres_runtime", "nexus.portfolio.runtime_acceptance"]
)
@pytest.mark.parametrize("damage", ["unloaded", "missing_origin", "checkout", "wrong_member"])
def test_runtime_imports_must_match_exact_installed_members(
    package: PackageFixture, name: str, damage: str
) -> None:
    if damage == "unloaded":
        del package.origins[name]
    elif damage == "missing_origin":
        package.origins[name] = SimpleNamespace()
    else:
        package.origins[name].__file__ = (
            package.project / "src" / (name.replace(".", "/") + ".py")
            if damage == "checkout"
            else package.installed / "nexus/portfolio/store.py"
        )
    with pytest.raises(ValueError, match="unverified application module"):
        acceptance.package_identity(package.helper, package.wheel)


@pytest.mark.parametrize("damage", ["loader", "canonical", "wheel"])
def test_runtime_migration_requires_three_way_byte_equality(
    package: PackageFixture, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    if damage == "loader":
        monkeypatch.setattr(acceptance, "read_migration_sql", lambda _: b"changed")
    elif damage == "canonical":
        (package.project / "infra/postgres/migrations/004_portfolio_runtime.sql").write_bytes(
            b"changed"
        )
    else:
        with zipfile.ZipFile(package.wheel, "w") as archive:
            archive.writestr("nexus/portfolio/sql_migrations/004_portfolio_runtime.sql", b"changed")
    with pytest.raises(ValueError, match="Runtime migration resource differs"):
        acceptance.package_identity(package.helper, package.wheel)


def test_normalize_projection_restores_producer_without_mutating_caller() -> None:
    source = {
        "seed_hash": "producer-hash",
        "metadata": {"classification": "synthetic"},
        "recommendations": [{"id": "one"}, {"id": "two"}],
    }
    projected = copy.deepcopy(source)
    projected["seed_hash"] = "projection-hash"
    projected["metadata"].update(runtime_job_id="job", runtime_artifact_seed_hash="producer-hash")
    for row in projected["recommendations"]:
        row["source_recommendation_id"] = row["id"]
        row["id"] = "job:" + row["id"]
    before = copy.deepcopy(projected)
    assert acceptance.normalize_projection(projected) == source
    assert projected == before
    assert acceptance.normalize_projection(source) == source


@pytest.mark.parametrize("missing", ["runtime_artifact_seed_hash", "source_recommendation_id"])
def test_incomplete_projection_cannot_be_silently_normalized(missing: str) -> None:
    projected = {
        "metadata": {"runtime_job_id": "job", "runtime_artifact_seed_hash": "producer"},
        "recommendations": [{"id": "job:one", "source_recommendation_id": "one"}],
    }
    del (
        projected["metadata"]
        if missing == "runtime_artifact_seed_hash"
        else projected["recommendations"][0]
    )[missing]
    with pytest.raises(KeyError):
        acceptance.normalize_projection(projected)


@dataclass
class FakeStore:
    calls: list[str]
    state: dict[str, Any]
    migration: dict[str, Any]
    mutate_on_retry: bool = False
    effect_count: int = 1
    history: dict[str, Any] = field(default_factory=dict)
    outcomes: set[str] = field(default_factory=set)

    def snapshot(self) -> dict[str, Any]:
        self.calls.append("snapshot")
        return copy.deepcopy(self.state)

    def seed(self) -> dict[str, Any]:
        return copy.deepcopy(self.state["seed"])

    def migrate(self, **_: Any) -> dict[str, Any]:
        self.calls.append("migrate")
        return copy.deepcopy(self.migration)

    def ensure_seed(self, seed: dict[str, Any]) -> None:
        self.calls.append("ensure_seed")
        assert seed == self.state["seed"]
        if self.mutate_on_retry:
            self.state["unexpected_rewrite"] = True

    def recommendation(self, identifier: str, _: Any) -> dict[str, Any]:
        self.calls.append("recommendation:" + identifier)
        return {
            "id": identifier,
            "state": "outcome_observed" if identifier in self.outcomes else "invalidated",
            "outcome_ready": True,
            "outcome": {"units": 3} if identifier in self.outcomes else None,
            "events": (
                [{"event_type": "outcome_observed", "actor_id": "planner"}]
                if identifier in self.outcomes
                else []
            ),
        }

    def overview_snapshot(self, _: Any) -> dict[str, Any]:
        return {"current_recommendations": copy.deepcopy(self.state["seed"]["recommendations"])}

    def recommendation_context(self, identifier: str, _: Any) -> dict[str, Any]:
        self.calls.append("context:" + identifier)
        return copy.deepcopy(self.history)

    def metrics_snapshot(self, _: Any) -> dict[str, Any]:
        return {
            "effect_count": self.effect_count,
            "states": {"outcome_observed": len(self.outcomes)},
        }

    def decide(self, identifier: str, actor: Any, *_: Any) -> dict[str, Any]:
        if actor.role == acceptance.ActorRole.SYSTEM:
            raise workflow_harness.StoreError("role_denied", "constructed refusal")
        self.calls.append("approve:" + identifier)
        return {"state": "approved"}

    def execute(self, identifier: str, _: Any, __: str, key: str) -> dict[str, Any]:
        if key.startswith("unapproved"):
            raise workflow_harness.StoreError("approval_required", "constructed refusal")
        self.calls.append("execute:" + identifier)
        self.effect_count += 1
        return {"execution_ref": "new-effect"}

    def observe(self, identifier: str, *_: Any) -> dict[str, Any]:
        self.calls.append("observe:" + identifier)
        self.outcomes.add(identifier)
        return {"state": "outcome_observed", "outcome": {"units": 3}}


@dataclass
class PhaseFixture:
    cluster: Path
    output: Path
    spec: dict[str, Any]
    prepared: dict[str, Any]
    advanced: dict[str, Any]
    server: dict[str, Any]
    package: dict[str, Any]
    initial: dict[str, Any]
    candidate: dict[str, Any]
    store: FakeStore
    helper: Any
    control: Any
    calls: list[str]

    def write_prepared(self) -> None:
        self.prepared["specification_sha256"] = acceptance.file_sha256(
            self.output / "specification.json"
        )
        write_json(self.output / "prepared.json", self.prepared)

    def write_advanced(self) -> None:
        self.advanced["prepared_sha256"] = acceptance.file_sha256(self.output / "prepared.json")
        write_json(self.output / "advanced.json", self.advanced)


@pytest.fixture
def phase(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PhaseFixture:
    output = tmp_path / "retained"
    output.mkdir()
    calls: list[str] = []
    initial = {
        "seed_hash": "initial-producer",
        "metadata": {
            "classification": "synthetic",
            "scientific_decision_time": "2026-06-24T00:00:00+00:00",
            "source_snapshot_sha256": "a" * 64,
        },
        "recommendations": [{"id": "old", "fingerprint": "old-fingerprint"}],
        "runtime_lineage": {"gold_demand_sha256": "gold-initial"},
    }
    candidate = copy.deepcopy(initial)
    candidate["seed_hash"] = "replacement-producer"
    candidate["metadata"]["source_snapshot_sha256"] = "b" * 64
    candidate["runtime_lineage"]["gold_demand_sha256"] = "gold-replacement"
    candidate["recommendations"] = [{"id": "new", "fingerprint": "new-fingerprint"}]
    root = output / "runtime-root"
    initial_dir = root / "evidence/initial"
    attempt = root / "evidence/jobs/constructed/attempt-1-token-digest"
    package = {"constructed_package": True, "runtime_acceptance_sha256": "d" * 64}
    spec = {
        "package": copy.deepcopy(package),
        "wheel": str(tmp_path / "constructed.whl"),
        "runtime_root": str(root),
        "initial_evidence": str(initial_dir),
        "initial_source": str(initial_dir / "source"),
        "changed_source": str(output / "changed-source"),
        "source_sha256": "b" * 64,
        "schema": "constructed_no_database",
        "initial_seed_sha256": acceptance.digest(initial),
        "initial_build_seconds": 1.25,
    }
    migration = {
        "version": 4,
        "migration_sha256": "c" * 64,
        "schema": spec["schema"],
        "server_version": "constructed-only",
        "applied": False,
    }
    server = {
        "cluster_id": "constructed-cluster",
        "system_identifier": "constructed-system",
        "binary_hashes": {"postgres.exe": "e" * 64},
        "postmaster_started_at": "2026-09-14T01:00:00+00:00",
    }
    store = FakeStore(calls, {"seed": copy.deepcopy(initial), "audit_head": "head"}, migration)
    prepared = {
        "status": "PREPARED_RESTART_NOT_RUN",
        "recorded_at": "2026-09-14T00:30:00+00:00",
        "server": {**server, "postmaster_started_at": "2026-09-14T00:00:00+00:00"},
        "snapshot_sha256": acceptance.digest(store.state),
        "migration": {**migration, "applied": True},
        "attempt_directory": str(attempt),
        "old_executed": {
            "id": "old",
            "fingerprint": "old-fingerprint",
            "execution_ref": "old-effect",
        },
        "old_invalidated": {"id": "stale", "fingerprint": "stale-fingerprint"},
        "checks": ["constructed-prepared-check"],
    }
    advanced = {
        "status": "ADVANCED_SECOND_RESTART_NOT_RUN",
        "recorded_at": "2026-09-14T00:45:00+00:00",
        "server": {**server, "postmaster_started_at": "2026-09-14T00:40:00+00:00"},
        "snapshot_sha256": acceptance.digest(store.state),
        "replacement_evidence": str(attempt),
        "replacement_seed_sha256": acceptance.digest(candidate),
        "new_recommendation": {"id": "new", "fingerprint": "new-fingerprint"},
        "replacement_build_seconds": 2.5,
        "checks": ["constructed-advanced-check"],
    }
    store.history = {
        "generation": {"is_current": False, "seed_sha256": spec["initial_seed_sha256"]},
        "execution_ref": "old-effect",
    }

    def configuration(_: Path) -> dict[str, Any]:
        calls.append("config")
        return {"migration_dsn": "constructed-never-connect"}

    def live_server(*_: Any) -> dict[str, Any]:
        calls.append("server")
        return copy.deepcopy(server)

    def source_seed(evidence: Path, _: Path) -> dict[str, Any]:
        calls.append("source_seed")
        return copy.deepcopy(initial if evidence == initial_dir else candidate)

    def fake_store(*_: Any) -> FakeStore:
        calls.append("store")
        return store

    def build(_: Path, evidence: Path, source: Path, **kwargs: Any) -> dict[str, Any]:
        calls.append("build")
        assert evidence == attempt
        assert source == Path(spec["changed_source"])
        assert (
            kwargs["planning_cutoff"].isoformat() == initial["metadata"]["scientific_decision_time"]
        )
        return copy.deepcopy(candidate)

    def activate(
        job: str, token: str, seed: dict[str, Any], _: Any, **kwargs: Any
    ) -> dict[str, Any]:
        calls.append("activate")
        assert job == "constructed-job" and token == "private-claim-token-never-publish"
        assert kwargs["evidence_dir"] == attempt
        store.state["seed"] = copy.deepcopy(seed)
        return {"job_id": job, "state": "completed"}

    native = SimpleNamespace(safe_root=lambda path: path.resolve(), read_config=configuration)
    helper = SimpleNamespace(
        native_helper=lambda: native,
        require=workflow_harness.require,
        refused=workflow_harness.refused,
        retain=workflow_harness.retain,
        server=live_server,
        source_seed=source_seed,
    )
    control = SimpleNamespace(
        claim=lambda _: None,
        attempt_evidence_dir=lambda *_: attempt,
        activate=activate,
        snapshot=lambda _: {"archived_generations": 1},
    )
    result = PhaseFixture(
        tmp_path / "cluster",
        output,
        spec,
        prepared,
        advanced,
        server,
        package,
        initial,
        candidate,
        store,
        helper,
        control,
        calls,
    )
    write_json(output / "specification.json", spec)
    result.write_prepared()
    write_json(
        output / "private/claim-secrets.json",
        {"id": "constructed-job", "token": "private-claim-token-never-publish"},
    )
    monkeypatch.setattr(acceptance, "helpers", lambda: helper)
    monkeypatch.setattr(acceptance, "package_identity", lambda *_: copy.deepcopy(package))
    monkeypatch.setattr(acceptance, "runtime_store", fake_store)
    monkeypatch.setattr(acceptance, "PostgresRuntimeControl", lambda _: control)
    monkeypatch.setattr(acceptance, "build_runtime_seed", build)
    monkeypatch.setattr(acceptance, "runtime_source_fingerprint", lambda _: "b" * 64)
    monkeypatch.setattr(
        acceptance,
        "identities",
        lambda *_: tuple(
            SimpleNamespace(role=role, actor_id=name)
            for role, name in (
                (acceptance.ActorRole.PLANNER, "planner"),
                (acceptance.ActorRole.APPROVER, "approver"),
                (acceptance.ActorRole.SYSTEM, "runtime-worker"),
            )
        ),
    )
    return result


def use_advanced(phase: PhaseFixture) -> None:
    phase.store.state["seed"] = copy.deepcopy(phase.candidate)
    phase.advanced["snapshot_sha256"] = acceptance.digest(phase.store.state)
    phase.write_advanced()


def test_existing_prepare_output_refuses_before_native_or_build(phase: PhaseFixture) -> None:
    before = (phase.output / "prepared.json").read_bytes()
    with pytest.raises(ValueError, match="Use a new output"):
        acceptance.prepare(phase.cluster, phase.output, Path(phase.spec["wheel"]))
    assert phase.calls == []
    assert (phase.output / "prepared.json").read_bytes() == before


def test_prepare_retains_only_private_tokens_and_never_claims_final_pass(
    phase: PhaseFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = phase.output.parent / "fresh-constructed-attempt"
    attempt = output / "runtime-root/evidence/jobs/job/attempt-1-hashed-token"
    private_tokens = {"planner": "planner-secret-fixture", "approver": "approver-secret-fixture"}
    seed = copy.deepcopy(phase.initial)
    seed["recommendations"] = [
        {"id": "rec-" + str(index), "fingerprint": "fingerprint-" + str(index)}
        for index in range(4)
    ]
    phase.store.state.update(seed=seed, recommendations=seed["recommendations"], events=[])
    phase.store.effect_count = 0
    phase.store.config = SimpleNamespace()  # type: ignore[attr-defined]
    monkeypatch.setattr(
        phase.store, "recommendations", lambda _: seed["recommendations"], raising=False
    )
    monkeypatch.setattr(
        phase.store,
        "create_session",
        lambda persona: (private_tokens[persona], "private-csrf-fixture"),
        raising=False,
    )
    original_execute = phase.store.execute

    def execute(identifier: str, actor: Any, fingerprint: str, key: str) -> dict[str, Any]:
        if key == "stale-order":
            raise workflow_harness.StoreError("approval_required", "constructed stale refusal")
        return original_execute(identifier, actor, fingerprint, key)

    def build(project: Path, evidence: Path) -> dict[str, Any]:
        assert project == acceptance.PROJECT
        assert evidence == output / "runtime-root/evidence/initial"
        assert (output / "started.json").exists()
        assert "protect_private" in phase.calls
        phase.calls.append("build_initial")
        return copy.deepcopy(seed)

    def change(source: Path, target: Path) -> Path:
        assert source == output / "runtime-root/evidence/initial/source"
        assert target == output / "changed-source"
        phase.calls.append("copy_declared_source")
        return target

    def submit(event: Any, _: Any) -> dict[str, Any]:
        assert event.payload == {"source_snapshot_sha256": "b" * 64}
        assert event.occurred_at == event.ingested_at
        phase.calls.append("submit")
        return {"invalidated": 3, "duplicate": phase.calls.count("submit") == 2}

    job = {"id": "job", "token": "claim-secret-fixture", "source_sha256": "b" * 64}
    claims = iter([job, None])
    native = phase.helper.native_helper()
    native.protect = lambda path: phase.calls.append("protect_private")
    phase.helper.source_seed = lambda *_: copy.deepcopy(seed)
    phase.control.submit = submit
    phase.control.claim = lambda _: next(claims)
    phase.control.attempt_evidence_dir = lambda *_: attempt
    monkeypatch.setattr(phase.store, "execute", execute)
    monkeypatch.setattr(acceptance, "PostgresWorkflowStore", lambda _: phase.store)
    monkeypatch.setattr(acceptance, "build_runtime_seed", build)
    monkeypatch.setattr(acceptance, "_changed_source", change)
    result = acceptance.prepare(phase.cluster, output, Path(phase.spec["wheel"]))
    assert result["status"] == "PREPARED_RESTART_NOT_RUN"
    assert len(result["checks"]) == 7
    assert phase.store.effect_count == 1
    assert (
        json.loads((output / "private/session-secrets.json").read_text(encoding="utf-8"))
        == private_tokens
    )
    assert json.loads((output / "private/claim-secrets.json").read_text(encoding="utf-8")) == job
    for name in ("started.json", "specification.json", "prepared.json"):
        public = (output / name).read_text(encoding="utf-8")
        assert not any(token in public for token in (*private_tokens.values(), job["token"]))
    assert result["snapshot_sha256"] == acceptance.digest(phase.store.state)
    assert not (output / "advanced.json").exists()
    assert not (output / "result.json").exists()


@pytest.mark.parametrize("changed", ["specification", "package", "harness"])
def test_context_identity_drift_refuses_before_native_configuration(
    phase: PhaseFixture, changed: str
) -> None:
    if changed == "specification":
        path = phase.output / "specification.json"
        path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        reason = "frozen acceptance specification changed"
    else:
        phase.package[
            "constructed_package" if changed == "package" else "runtime_acceptance_sha256"
        ] = "changed"
        reason = "Installed code, migration or harness changed"
    with pytest.raises(ValueError, match=reason):
        acceptance.read_context(phase.cluster, phase.output)
    assert phase.calls == []


@pytest.mark.parametrize(
    "key", ["runtime_root", "initial_evidence", "initial_source", "changed_source"]
)
def test_context_paths_must_remain_inside_acceptance_output(phase: PhaseFixture, key: str) -> None:
    phase.spec[key] = str(phase.output.parent / "outside")
    write_json(phase.output / "specification.json", phase.spec)
    phase.write_prepared()
    with pytest.raises(ValueError, match="paths must stay"):
        acceptance.read_context(phase.cluster, phase.output)
    assert phase.calls == []


@pytest.mark.parametrize("action", ["advance", "verify"])
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("cluster_id", "another-cluster"),
        ("system_identifier", "another-system"),
        ("binary_hashes", {"postgres.exe": "changed"}),
        ("postmaster_started_at", "2026-09-14T00:00:00+00:00"),
        ("postmaster_started_at", "2026-09-14T00:30:00+00:00"),
    ],
)
def test_each_phase_requires_later_restart_of_same_cluster(
    phase: PhaseFixture, action: str, key: str, value: Any
) -> None:
    if action == "verify":
        use_advanced(phase)
    phase.server[key] = value
    with pytest.raises(ValueError, match="retained restart of the same cluster"):
        getattr(acceptance, action)(phase.cluster, phase.output)
    assert phase.calls == ["config", "server"]
    assert not (phase.output / "result.json").exists()


@pytest.mark.parametrize("action", ["advance", "verify"])
def test_each_phase_refuses_changed_retained_state_before_further_work(
    phase: PhaseFixture, action: str
) -> None:
    if action == "verify":
        use_advanced(phase)
    phase.store.state["audit_head"] = "changed"
    with pytest.raises(ValueError, match="changed"):
        getattr(acceptance, action)(phase.cluster, phase.output)
    assert phase.calls == ["config", "server", "store", "snapshot"]
    assert not (phase.output / "result.json").exists()


@pytest.mark.parametrize(
    "damage", ["live_claim", "attempt_path", "attempt_exists", "initial", "source"]
)
def test_advance_refuses_invalid_claim_or_source_before_build(
    phase: PhaseFixture, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    if damage == "live_claim":
        phase.control.claim = lambda _: {"id": "duplicate"}
    elif damage == "attempt_path":
        phase.control.attempt_evidence_dir = lambda *_: phase.output / "wrong-attempt"
    elif damage == "attempt_exists":
        Path(phase.prepared["attempt_directory"]).mkdir(parents=True)
    elif damage == "initial":
        phase.initial["seed_hash"] = "drifted"
    else:
        monkeypatch.setattr(acceptance, "runtime_source_fingerprint", lambda _: "changed")
    with pytest.raises(ValueError):
        acceptance.advance(phase.cluster, phase.output)
    assert "build" not in phase.calls and "activate" not in phase.calls
    assert not (phase.output / "advanced.json").exists()


@pytest.mark.parametrize("damage", ["producer_verification", "source_identity", "gold_demand"])
def test_advance_requires_verified_changed_source_to_reach_gold_before_activation(
    phase: PhaseFixture, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    if damage == "producer_verification":
        original = phase.helper.source_seed
        phase.helper.source_seed = lambda path, source: (
            {"unexpected": True}
            if path == Path(phase.prepared["attempt_directory"])
            else original(path, source)
        )
    elif damage == "source_identity":
        phase.candidate["metadata"]["source_snapshot_sha256"] = "a" * 64
    else:
        phase.candidate["runtime_lineage"]["gold_demand_sha256"] = "gold-initial"
    with pytest.raises(ValueError):
        acceptance.advance(phase.cluster, phase.output)
    assert "build" in phase.calls and "activate" not in phase.calls
    assert not (phase.output / "advanced.json").exists()


def test_advance_retains_nonfinal_receipt_without_claim_token(phase: PhaseFixture) -> None:
    result = acceptance.advance(phase.cluster, phase.output)
    assert result["status"] == "ADVANCED_SECOND_RESTART_NOT_RUN"
    assert result["prepared_sha256"] == acceptance.file_sha256(phase.output / "prepared.json")
    assert result["replacement_seed_sha256"] == acceptance.digest(phase.candidate)
    assert "build" in phase.calls and "activate" in phase.calls
    assert "private-claim-token-never-publish" not in json.dumps(result)
    assert not (phase.output / "result.json").exists()


def test_verify_refuses_changed_prepared_receipt_before_database(phase: PhaseFixture) -> None:
    use_advanced(phase)
    path = phase.output / "prepared.json"
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Prepared receipt changed"):
        acceptance.verify(phase.cluster, phase.output)
    assert phase.calls == ["config"]


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("applied", True),
        ("applied", 0),
        ("version", 3),
        ("migration_sha256", "changed"),
        ("schema", "different"),
        ("server_version", "different"),
    ],
)
def test_verify_migration_identity_refuses_before_seed_retry(
    phase: PhaseFixture, key: str, value: Any
) -> None:
    use_advanced(phase)
    phase.store.migration[key] = value
    with pytest.raises(ValueError, match="Migration reapply changed"):
        acceptance.verify(phase.cluster, phase.output)
    assert "migrate" in phase.calls and "ensure_seed" not in phase.calls
    assert not (phase.output / "result.json").exists()


def test_verify_seed_retry_must_preserve_all_state(phase: PhaseFixture) -> None:
    use_advanced(phase)
    phase.store.mutate_on_retry = True
    with pytest.raises(ValueError, match="Seed retry rewrote"):
        acceptance.verify(phase.cluster, phase.output)
    assert "source_seed" not in phase.calls
    assert not (phase.output / "result.json").exists()


@pytest.mark.parametrize("damage", ["initial", "replacement", "active_projection"])
def test_verify_generations_remain_bound_to_producer_evidence(
    phase: PhaseFixture, damage: str
) -> None:
    use_advanced(phase)
    if damage == "initial":
        phase.initial["seed_hash"] = "changed"
    elif damage == "replacement":
        phase.candidate["seed_hash"] = "changed"
    else:
        phase.store.state["seed"]["seed_hash"] = "changed"
        phase.advanced["snapshot_sha256"] = acceptance.digest(phase.store.state)
        phase.write_advanced()
    with pytest.raises(ValueError, match="source/code/artifact bindings"):
        acceptance.verify(phase.cluster, phase.output)
    assert not any(call.startswith("approve:") for call in phase.calls)
    assert not (phase.output / "result.json").exists()


@pytest.mark.parametrize("damage", ["claimable", "current", "generation", "effect"])
def test_verify_old_generation_or_completed_job_corruption_cannot_pass(
    phase: PhaseFixture, damage: str
) -> None:
    use_advanced(phase)
    if damage == "claimable":
        phase.control.claim = lambda _: {"id": "completed-job"}
    elif damage == "current":
        phase.store.history["generation"]["is_current"] = True
    elif damage == "generation":
        phase.store.history["generation"]["seed_sha256"] = "wrong"
    else:
        phase.store.history["execution_ref"] = "wrong"
    with pytest.raises(ValueError):
        acceptance.verify(phase.cluster, phase.output)
    assert not any(call.startswith("approve:") for call in phase.calls)
    assert not (phase.output / "result.json").exists()


def test_verify_wrong_unique_effect_count_cannot_emit_pass(phase: PhaseFixture) -> None:
    use_advanced(phase)
    phase.store.effect_count = 7
    with pytest.raises(ValueError, match="Wrong unique mock effect or mature outcome count"):
        acceptance.verify(phase.cluster, phase.output)
    assert not (phase.output / "result.json").exists()


@pytest.mark.parametrize(
    "damage",
    [
        "noop",
        "returned_state",
        "retained_state",
        "different_outcome",
        "empty_outcome",
        "no_event",
        "duplicate_event",
        "wrong_actor",
        "wrong_total",
    ],
)
def test_verify_missing_or_inconsistent_durable_outcomes_cannot_emit_pass(
    phase: PhaseFixture, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    use_advanced(phase)
    original_observe = phase.store.observe
    original_recommendation = phase.store.recommendation

    def observe(identifier: str, *args: Any) -> dict[str, Any]:
        if damage == "noop":
            return {"state": "outcome_observed", "outcome": {"units": 3}}
        result = original_observe(identifier, *args)
        if damage == "returned_state":
            result["state"] = "executed"
        return result

    def recommendation(identifier: str, actor: Any) -> dict[str, Any]:
        result = original_recommendation(identifier, actor)
        if identifier not in phase.store.outcomes:
            return result
        if damage == "retained_state":
            result["state"] = "executed"
        elif damage == "different_outcome":
            result["outcome"] = {"units": 99}
        elif damage == "empty_outcome":
            result["outcome"] = {}
        elif damage == "no_event":
            result["events"] = []
        elif damage == "duplicate_event":
            result["events"] *= 2
        elif damage == "wrong_actor":
            result["events"][0]["actor_id"] = "runtime-worker"
        return result

    monkeypatch.setattr(phase.store, "observe", observe)
    monkeypatch.setattr(phase.store, "recommendation", recommendation)
    if damage == "wrong_total":
        monkeypatch.setattr(
            phase.store,
            "metrics_snapshot",
            lambda _: {"effect_count": 2, "states": {"outcome_observed": 1}},
        )
    with pytest.raises(ValueError, match="outcome"):
        acceptance.verify(phase.cluster, phase.output)
    assert not (phase.output / "result.json").exists()


def test_verify_constructed_success_retains_explicit_boundaries_and_redacts_tokens(
    phase: PhaseFixture,
) -> None:
    use_advanced(phase)
    result = acceptance.verify(phase.cluster, phase.output)
    assert result["status"] == "PASS"
    assert phase.store.outcomes == {"old", "new"}
    assert result["advanced_sha256"] == acceptance.file_sha256(phase.output / "advanced.json")
    assert result["snapshot_sha256"] == acceptance.digest(phase.store.state)
    assert "real_mature_outcomes_from_both_generations" in result["checks"]
    assert {"scale/load/soak", "Compose/Linux", "95 percent complete"} <= set(result["not_claimed"])
    assert "private-claim-token-never-publish" not in json.dumps(result)
    assert json.loads((phase.output / "result.json").read_text(encoding="utf-8")) == result


def test_private_session_tokens_are_resolved_without_publication(phase: PhaseFixture) -> None:
    planner = acceptance.Actor(
        "planner", "Synthetic planner", acceptance.ActorRole.PLANNER, ("L1",)
    )
    approver = acceptance.Actor(
        "approver", "Synthetic approver", acceptance.ActorRole.APPROVER, ("L1",)
    )
    tokens = {"planner": "private-planner-token", "approver": "private-approver-token"}
    write_json(phase.output / "private/session-secrets.json", tokens)
    seen: list[str] = []

    def actor(_: Any, token: str) -> Any:
        seen.append(token)
        return planner if token == tokens["planner"] else approver

    # The fixture replaces identities for phase tests; load just its original
    # callable from a saved module-level reference instead of another app import.
    helper = SimpleNamespace(actor=actor)
    resolved = ORIGINAL_IDENTITIES(helper, phase.store, phase.output)
    assert resolved[:2] == (planner, approver)
    assert resolved[2].actor_id == "runtime-worker"
    assert resolved[2].role == acceptance.ActorRole.SYSTEM
    assert resolved[2].locations == planner.locations
    assert seen == list(tokens.values())


ORIGINAL_IDENTITIES = acceptance.identities
