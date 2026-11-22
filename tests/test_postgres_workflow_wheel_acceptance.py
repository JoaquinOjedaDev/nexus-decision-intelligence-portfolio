"""Portable fail-closed checks for the installed-wheel workflow coordinator.

Temporary wheels and explicit fakes exercise harness control flow only. No
database, service lifecycle, session credentials or real restart is involved;
these tests cannot stand in for the separately retained native acceptance.
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
acceptance = importlib.import_module("scripts.postgres_workflow_wheel_acceptance")


@pytest.fixture(autouse=True)
def forbid_database_and_native_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_: Any, **__: Any) -> None:
        pytest.fail("Portable coordinator tests must not contact a database or native helper")

    monkeypatch.setattr(acceptance.psycopg, "connect", forbidden)
    monkeypatch.setattr(acceptance, "native_helper", forbidden)


@dataclass
class WheelFixture:
    wheel: Path
    purelib: Path
    project: Path
    members: dict[str, bytes]
    migrations: dict[int, bytes]

    def write_wheel(self) -> None:
        with zipfile.ZipFile(self.wheel, "w") as archive:
            for name, content in self.members.items():
                archive.writestr(name, content)


@pytest.fixture
def installed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> WheelFixture:
    project = tmp_path / "constructed-project"
    purelib = tmp_path / "constructed-site-packages"
    migrations = {2: b"-- constructed migration 002\n", 3: b"-- constructed migration 003\n"}
    members = {
        "nexus/__init__.py": b'"""Constructed wheel fixture."""\n',
        "nexus/portfolio/postgres_backend.py": b"BACKEND_FIXTURE = True\n",
        "nexus/portfolio/postgres_workflow.py": b"FIXTURE = True\n",
        "nexus/portfolio/postgres_migrations.py": b"MIGRATIONS_FIXTURE = True\n",
        "nexus/portfolio/store.py": b"STORE_FIXTURE = True\n",
        "nexus/portfolio/runtime_pipeline.py": b"RUNTIME_FIXTURE = True\n",
        "nexus/portfolio/sql_migrations/002_portfolio_ops.sql": migrations[2],
        "nexus/portfolio/sql_migrations/003_portfolio_workflow.sql": migrations[3],
    }
    for name, content in members.items():
        path = purelib / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        if name.endswith(".sql"):
            canonical = project / "infra/postgres/migrations" / Path(name).name
            canonical.parent.mkdir(parents=True, exist_ok=True)
            canonical.write_bytes(content)
    for name in (
        "scripts/postgres_workflow_wheel_acceptance.py",
        "scripts/native_postgres_test_cluster.py",
        "uv.lock",
        "pyproject.toml",
    ):
        path = project / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"constructed governing fixture\n")
    result = WheelFixture(tmp_path / "constructed.whl", purelib, project, members, migrations)
    result.write_wheel()
    monkeypatch.setattr(acceptance, "PROJECT", project)
    monkeypatch.setattr(
        acceptance, "__file__", str(project / "scripts/postgres_workflow_wheel_acceptance.py")
    )
    origins = {
        name: SimpleNamespace(__file__=purelib / (name.replace(".", "/") + ".py"))
        for name in acceptance.CRITICAL_MODULES
    }
    origins["nexus"] = SimpleNamespace(__file__=purelib / "nexus/__init__.py")
    monkeypatch.setattr(acceptance, "nexus", origins["nexus"])
    monkeypatch.setattr(
        acceptance, "sysconfig", SimpleNamespace(get_paths=lambda: {"purelib": str(purelib)})
    )
    monkeypatch.setattr(
        acceptance,
        "sys",
        SimpleNamespace(
            flags=SimpleNamespace(isolated=1),
            dont_write_bytecode=True,
            version="3.12.0",
            modules=origins,
        ),
    )
    monkeypatch.setattr(
        acceptance,
        "importlib",
        SimpleNamespace(metadata=SimpleNamespace(version=lambda _: "constructed-driver-version")),
    )
    monkeypatch.setattr(acceptance, "read_migration_sql", lambda version: migrations[version])
    return result


def test_installed_identity_binds_wheel_application_resources_and_governing_files(
    installed: WheelFixture,
) -> None:
    receipt = acceptance.installed_identity(installed.wheel)
    assert receipt["wheel_sha256"] == acceptance.file_sha256(installed.wheel)
    assert receipt["module_tree_sha256"] == acceptance.digest(
        {name: acceptance.file_sha256(installed.purelib / name) for name in installed.members}
    )
    assert receipt["module_count"] == 8
    assert receipt["isolated_mode"] is True
    assert receipt["installed_root"] == str(installed.purelib.resolve())
    assert receipt["python"] == "3.12.0"
    assert receipt["psycopg"] == "constructed-driver-version"
    for key, relative in {
        "harness_sha256": "scripts/postgres_workflow_wheel_acceptance.py",
        "native_helper_sha256": "scripts/native_postgres_test_cluster.py",
        "lock_sha256": "uv.lock",
        "project_config_sha256": "pyproject.toml",
    }.items():
        assert receipt[key] == acceptance.file_sha256(installed.project / relative)


@pytest.mark.parametrize("flag", ["isolated", "dont_write_bytecode"])
def test_installed_identity_requires_isolated_nonwriting_python(
    installed: WheelFixture, monkeypatch: pytest.MonkeyPatch, flag: str
) -> None:
    if flag == "isolated":
        acceptance.sys.flags.isolated = 0
        reason = "Use Python -I"
    else:
        acceptance.sys.dont_write_bytecode = False
        reason = "Use Python -B"

    def forbidden() -> None:
        pytest.fail("Process flags must be checked before inspecting package paths")

    monkeypatch.setattr(acceptance.sysconfig, "get_paths", forbidden)
    with pytest.raises(ValueError, match=reason):
        acceptance.installed_identity(installed.wheel)


def test_installed_identity_refuses_source_checkout_import(
    installed: WheelFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        acceptance, "nexus", SimpleNamespace(__file__=installed.project / "src/nexus/__init__.py")
    )
    with pytest.raises(ValueError, match="nexus is not installed here"):
        acceptance.installed_identity(installed.wheel)


@pytest.mark.parametrize(
    "member",
    [
        "nexus/unverified.py",
        "nexus/portfolio/sql_migrations/unverified.sql",
        "nexus/portfolio/postgres_workflow/__init__.py",
    ],
)
def test_installed_identity_refuses_extra_source_or_shadow_package(
    installed: WheelFixture, member: str
) -> None:
    extra = installed.purelib / member
    extra.parent.mkdir(parents=True, exist_ok=True)
    extra.write_bytes(b"unverified installed source, never execute")
    with pytest.raises(ValueError, match="Installed application contains unverified source files"):
        acceptance.installed_identity(installed.wheel)


@pytest.mark.parametrize(
    "name",
    [
        "nexus.portfolio.postgres_backend",
        "nexus.portfolio.postgres_workflow",
        "nexus.portfolio.postgres_migrations",
        "nexus.portfolio.store",
        "nexus.portfolio.runtime_pipeline",
    ],
)
@pytest.mark.parametrize("origin", ["outside_environment", "wrong_verified_file"])
def test_installed_identity_binds_each_import_to_its_exact_verified_member(
    installed: WheelFixture, name: str, origin: str
) -> None:
    path = (
        installed.project / "src" / (name.replace(".", "/") + ".py")
        if origin == "outside_environment"
        else installed.purelib / "nexus/__init__.py"
    )
    acceptance.sys.modules[name] = SimpleNamespace(__file__=path)
    with pytest.raises(
        ValueError, match="Imported critical module is not the verified wheel member"
    ):
        acceptance.installed_identity(installed.wheel)


@pytest.mark.parametrize("damage", ["unloaded", "missing_origin"])
def test_installed_identity_requires_loaded_module_origin(
    installed: WheelFixture, damage: str
) -> None:
    name = "nexus.portfolio.runtime_pipeline"
    if damage == "unloaded":
        del acceptance.sys.modules[name]
    else:
        acceptance.sys.modules[name] = SimpleNamespace()
    with pytest.raises(
        ValueError, match="Imported critical module is not the verified wheel member"
    ):
        acceptance.installed_identity(installed.wheel)


def test_installed_identity_binds_root_package_to_its_exact_verified_member(
    installed: WheelFixture,
) -> None:
    acceptance.nexus.__file__ = installed.purelib / "nexus/portfolio/store.py"
    with pytest.raises(ValueError):
        acceptance.installed_identity(installed.wheel)


@pytest.mark.parametrize(
    "member",
    [
        "nexus/portfolio/postgres_workflow.py",
        "nexus/portfolio/sql_migrations/003_portfolio_workflow.sql",
    ],
)
@pytest.mark.parametrize("damage", ["missing", "changed"])
def test_installed_identity_refuses_missing_or_changed_installed_members(
    installed: WheelFixture, member: str, damage: str
) -> None:
    path = installed.purelib / member
    if damage == "missing":
        path.unlink()
        reason = "Installed wheel member is missing"
    else:
        path.write_bytes(b"changed installed fixture")
        reason = "Installed bytes differ from wheel"
    with pytest.raises(ValueError, match=reason):
        acceptance.installed_identity(installed.wheel)


def test_installed_identity_refuses_wheel_bytes_changed_after_install(
    installed: WheelFixture,
) -> None:
    installed.members["nexus/portfolio/postgres_workflow.py"] = b"changed wheel fixture"
    installed.write_wheel()
    with pytest.raises(ValueError, match="Installed bytes differ from wheel"):
        acceptance.installed_identity(installed.wheel)


@pytest.mark.parametrize("version", [2, 3])
def test_installed_identity_refuses_sql_loader_not_using_wheel_resource(
    installed: WheelFixture, version: int
) -> None:
    installed.migrations[version] = b"unexpected SQL-loader bytes"
    with pytest.raises(ValueError, match="SQL loader did not use wheel bytes"):
        acceptance.installed_identity(installed.wheel)


@pytest.mark.parametrize("version", [2, 3])
def test_installed_identity_refuses_changed_canonical_migration(
    installed: WheelFixture, version: int
) -> None:
    filename = {2: "002_portfolio_ops.sql", 3: "003_portfolio_workflow.sql"}[version]
    (installed.project / "infra/postgres/migrations" / filename).write_bytes(b"new canonical bytes")
    with pytest.raises(ValueError, match="Canonical migration changed after wheel build"):
        acceptance.installed_identity(installed.wheel)


@pytest.mark.parametrize("version", [2, 3])
def test_installed_identity_requires_both_packaged_sql_resources(
    installed: WheelFixture, version: int
) -> None:
    filename = {2: "002_portfolio_ops.sql", 3: "003_portfolio_workflow.sql"}[version]
    del installed.members["nexus/portfolio/sql_migrations/" + filename]
    installed.write_wheel()
    with pytest.raises(KeyError, match=filename):
        acceptance.installed_identity(installed.wheel)


@pytest.mark.parametrize(
    ("member", "reason"),
    [
        ("../outside.py", "Unsafe wheel member"),
        ("/absolute.py", "Unsafe wheel member"),
        ("nexus/../../outside.py", "Unsafe wheel member"),
        ("startup.pth", "Wheel must not install path hooks"),
        ("fixture.data/purelib/startup.pth", "Wheel must not install path hooks"),
    ],
)
def test_installed_identity_refuses_unsafe_members_and_path_hooks(
    installed: WheelFixture, member: str, reason: str
) -> None:
    installed.members[member] = b"untrusted fixture, never execute"
    installed.write_wheel()
    with pytest.raises(ValueError, match=reason):
        acceptance.installed_identity(installed.wheel)


def test_installed_identity_refuses_duplicate_archive_members(installed: WheelFixture) -> None:
    with (
        zipfile.ZipFile(installed.wheel, "a") as archive,
        pytest.warns(UserWarning, match="Duplicate"),
    ):
        archive.writestr("nexus/__init__.py", b"ambiguous duplicate")
    with pytest.raises(ValueError, match="Wheel contains duplicate paths"):
        acceptance.installed_identity(installed.wheel)


def test_retain_never_overwrites_existing_evidence(tmp_path: Path) -> None:
    path = tmp_path / "receipt.json"
    acceptance.retain(path, {"status": "constructed"})
    previous = path.read_bytes()
    with pytest.raises(FileExistsError):
        acceptance.retain(path, {"status": "replacement"})
    assert path.read_bytes() == previous


@pytest.mark.parametrize("actual", ["scope_denied", "role_denied", None])
def test_refused_requires_the_expected_domain_refusal(actual: str | None) -> None:
    def action() -> None:
        if actual is not None:
            raise acceptance.StoreError(actual, "constructed refusal")

    if actual == "scope_denied":
        acceptance.refused("scope_denied", action)
    else:
        reason = "Unexpected refusal" if actual else "Required refusal did not occur"
        with pytest.raises(ValueError, match=reason):
            acceptance.refused("scope_denied", action)


@dataclass
class FakeWorkflow:
    calls: list[str]
    state: dict[str, Any]
    migration: dict[str, Any]
    mutate_on_seed_retry: bool = False

    def snapshot(self) -> dict[str, Any]:
        self.calls.append("snapshot")
        return copy.deepcopy(self.state)

    def migrate(self, **_: Any) -> dict[str, Any]:
        self.calls.append("migrate")
        return copy.deepcopy(self.migration)

    def ensure_seed(self, seed: dict[str, Any]) -> None:
        self.calls.append("ensure_seed")
        assert seed == self.state["seed"]
        if self.mutate_on_seed_retry:
            self.state["creation_epoch"] = 9999


@dataclass
class RestartFixture:
    cluster: Path
    output: Path
    evidence: Path
    source: Path
    wheel: Path
    seed: dict[str, Any]
    package: dict[str, Any]
    server: dict[str, Any]
    workflow: FakeWorkflow
    calls: list[str] = field(default_factory=list)


@pytest.fixture
def prepared(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RestartFixture:
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    (evidence / acceptance.RUNTIME_SEED_FILENAME).write_bytes(b"constructed seed fixture")
    output = tmp_path / "acceptance"
    output.mkdir()
    seed = {"metadata": {"classification": "synthetic"}, "fixture": "source-backed"}
    calls: list[str] = []
    migration = {
        "version": 3,
        "migration_sha256": "c" * 64,
        "schema": "nexus_wheel_constructed",
        "server_version": "18.1",
        "applied": False,
    }
    state = {"seed": seed, "audit_head": "b" * 64, "creation_epoch": 1234.5}
    result = RestartFixture(
        cluster=tmp_path / "cluster",
        output=output,
        evidence=evidence,
        source=tmp_path / "source",
        wheel=tmp_path / "constructed.whl",
        seed=seed,
        package={"constructed_package": "a" * 64},
        server={
            "cluster_id": "constructed-cluster",
            "system_identifier": "constructed-system",
            "postmaster_started_at": "2026-09-14T01:00:00+00:00",
            "binary_hashes": {"postgres.exe": "d" * 64},
        },
        workflow=FakeWorkflow(calls, copy.deepcopy(state), migration),
        calls=calls,
    )
    spec = {
        "package": result.package,
        "cluster_id": result.server["cluster_id"],
        "system_identifier": result.server["system_identifier"],
        "schema": migration["schema"],
        "evidence_directory": str(evidence),
        "source_directory": str(result.source),
        "wheel_path": str(result.wheel),
        "canonical_seed_sha256": acceptance.digest(seed),
        "seed_file_sha256": acceptance.file_sha256(evidence / acceptance.RUNTIME_SEED_FILENAME),
    }
    acceptance.retain(output / "specification.json", spec)
    acceptance.retain(
        output / "prepared.json",
        {
            "status": "PREPARED_RESTART_NOT_RUN",
            "prepared_at": "2026-09-14T00:30:00+00:00",
            "specification_sha256": acceptance.file_sha256(output / "specification.json"),
            "before_server": {
                **result.server,
                "postmaster_started_at": "2026-09-14T00:00:00+00:00",
            },
            "workflow_migration": {**migration, "applied": True},
            "snapshot_sha256": acceptance.digest(state),
        },
    )

    def config(_: Path) -> dict[str, Any]:
        calls.append("config")
        return {"migration_dsn": "constructed-no-connection"}

    def server(*_: Any) -> dict[str, Any]:
        calls.append("server")
        return copy.deepcopy(result.server)

    def source_seed(*_: Any) -> dict[str, Any]:
        calls.append("source_seed")
        return copy.deepcopy(result.seed)

    def workflow(_: Any) -> FakeWorkflow:
        calls.append("workflow")
        return result.workflow

    monkeypatch.setattr(
        acceptance,
        "native_helper",
        lambda: SimpleNamespace(safe_root=lambda path: path.resolve(), read_config=config),
    )
    monkeypatch.setattr(acceptance, "server", server)
    monkeypatch.setattr(acceptance, "installed_identity", lambda _: copy.deepcopy(result.package))
    monkeypatch.setattr(acceptance, "source_seed", source_seed)
    monkeypatch.setattr(acceptance, "store_config", lambda *_: object())
    monkeypatch.setattr(acceptance, "PostgresWorkflowStore", workflow)
    return result


def test_prepare_existing_output_refuses_before_server_or_database(
    prepared: RestartFixture,
) -> None:
    previous = (prepared.output / "prepared.json").read_bytes()
    with pytest.raises(ValueError, match="Use a fresh acceptance output"):
        acceptance.prepare(
            prepared.cluster, prepared.output, prepared.evidence, prepared.source, prepared.wheel
        )
    assert prepared.calls == []
    assert (prepared.output / "prepared.json").read_bytes() == previous


@pytest.mark.parametrize("changed", ["specification", "package"])
def test_verify_identity_changes_refuse_before_server_or_workflow(
    prepared: RestartFixture, changed: str
) -> None:
    if changed == "specification":
        path = prepared.output / "specification.json"
        path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        reason = "Specification changed"
    else:
        prepared.package["constructed_package"] = "changed"
        reason = "Installed package or harness changed"
    with pytest.raises(ValueError, match=reason):
        acceptance.verify(prepared.cluster, prepared.output)
    assert prepared.calls == []
    assert not (prepared.output / "result.json").exists()


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("cluster_id", "different-cluster"),
        ("system_identifier", "different-system"),
        ("binary_hashes", {"postgres.exe": "f" * 64}),
        ("postmaster_started_at", "2026-09-14T00:00:00+00:00"),
        ("postmaster_started_at", "2026-09-14T00:15:00+00:00"),
        ("postmaster_started_at", "2026-09-14T00:30:00+00:00"),
    ],
)
def test_verify_requires_same_cluster_and_restart_strictly_after_prepared_state(
    prepared: RestartFixture, key: str, value: Any
) -> None:
    prepared.server[key] = value
    with pytest.raises(
        ValueError, match="retained restart of the same cluster after prepared state"
    ):
        acceptance.verify(prepared.cluster, prepared.output)
    assert prepared.calls == ["config", "server"]
    assert not (prepared.output / "result.json").exists()


@pytest.mark.parametrize("changed", ["canonical_seed", "seed_file"])
def test_verify_source_identity_changes_refuse_before_workflow(
    prepared: RestartFixture, changed: str
) -> None:
    if changed == "canonical_seed":
        prepared.seed["fixture"] = "changed"
    else:
        (prepared.evidence / acceptance.RUNTIME_SEED_FILENAME).write_bytes(b"changed")
    with pytest.raises(ValueError, match="Source-backed seed changed"):
        acceptance.verify(prepared.cluster, prepared.output)
    assert prepared.calls == ["config", "server", "source_seed"]
    assert not (prepared.output / "result.json").exists()


def test_verify_changed_snapshot_refuses_before_migration(prepared: RestartFixture) -> None:
    prepared.workflow.state["creation_epoch"] = 5678.0
    with pytest.raises(ValueError, match="Workflow state changed across restart"):
        acceptance.verify(prepared.cluster, prepared.output)
    assert prepared.calls == ["config", "server", "source_seed", "workflow", "snapshot"]
    assert not (prepared.output / "result.json").exists()


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("applied", True),
        ("applied", 0),
        ("version", 2),
        ("migration_sha256", "f" * 64),
        ("schema", "another_schema"),
        ("server_version", "different-server-version"),
    ],
)
def test_verify_reapply_refusal_precedes_seed_retry_or_session_access(
    prepared: RestartFixture, key: str, value: Any
) -> None:
    prepared.workflow.migration[key] = value
    with pytest.raises(ValueError, match="Migration retry changed"):
        acceptance.verify(prepared.cluster, prepared.output)
    assert prepared.calls == ["config", "server", "source_seed", "workflow", "snapshot", "migrate"]
    assert not (prepared.output / "private").exists()
    assert not (prepared.output / "result.json").exists()


def test_verify_good_reapply_checks_seed_retry_before_session_access(
    prepared: RestartFixture,
) -> None:
    prepared.workflow.mutate_on_seed_retry = True
    with pytest.raises(ValueError, match="Migration/seed retry changed persisted state or expiry"):
        acceptance.verify(prepared.cluster, prepared.output)
    assert prepared.calls == [
        "config",
        "server",
        "source_seed",
        "workflow",
        "snapshot",
        "migrate",
        "ensure_seed",
        "snapshot",
    ]
    assert not (prepared.output / "private").exists()
    assert not (prepared.output / "result.json").exists()


def test_portable_receipt_fixture_does_not_claim_a_real_restart(prepared: RestartFixture) -> None:
    receipt = json.loads((prepared.output / "prepared.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "PREPARED_RESTART_NOT_RUN"
    assert not (prepared.output / "result.json").exists()
