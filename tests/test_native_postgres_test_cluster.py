"""Portable launcher safety checks: constructed files, mocked driver and processes.

No live database, installed executable, socket or Windows service is accessed.
The optional psycopg parser is real; every connection and subprocess is replaced.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def helper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    pytest.importorskip("psycopg")
    spec = importlib.util.spec_from_file_location(
        "nexus_native_postgres_launcher_test", ROOT / "scripts/native_postgres_test_cluster.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "PROJECT", tmp_path / "project")

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("A launcher unit test attempted an unmocked external operation")

    monkeypatch.setattr(module.psycopg, "connect", forbidden)
    monkeypatch.setattr(module.subprocess, "run", forbidden)
    monkeypatch.setattr(module.socket, "socket", forbidden)
    return module


@pytest.fixture
def configuration(helper: ModuleType) -> dict[str, Any]:
    root = helper.PROJECT / "artifacts/native-unit"
    root.mkdir(parents=True)
    binaries = helper.PROJECT / "constructed-binaries"
    binaries.mkdir()
    hashes = {}
    for name in ("postgres", "initdb", "pg_ctl"):
        path = binaries / (name + ".exe" if helper.os.name == "nt" else name)
        content = b"non-executable unit fixture " + name.encode()
        path.write_bytes(content)
        hashes[name] = hashlib.sha256(content).hexdigest()
    value = {
        "contract_version": "nexus.native-postgres-test.v1",
        "cluster_id": "constructed-native-unit-identity",
        "cluster_root": str(root),
        "data_directory": str(root / "data"),
        "expected_database": helper.DATABASE,
        "expected_port": 55438,
        "bin_dir": str(binaries),
        "binary_hashes": hashes,
    }
    for field, role in (
        ("admin_dsn", helper.ADMIN),
        ("migration_dsn", helper.MIGRATOR),
        ("application_dsn", helper.APP),
    ):
        value[field] = helper.make_conninfo(
            host="127.0.0.1",
            port=55438,
            dbname=helper.DATABASE,
            user=role,
            password="a" * 64,
            connect_timeout=5,
        )
    (root / helper.CONFIG).write_text(json.dumps(value), encoding="utf-8")
    return value


class FakeDatabase:
    def __init__(self, *results: Any) -> None:
        self.results = iter(results)
        self.statements: list[str] = []

    def __enter__(self) -> FakeDatabase:
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def execute(self, statement: Any, parameters: Any = None) -> FakeDatabase:
        self.statements.append(statement if isinstance(statement, str) else statement.as_string())
        return self

    def fetchone(self) -> Any:
        return next(self.results)

    def fetchall(self) -> Any:
        return next(self.results)


def bootstrap_row(helper: ModuleType, config: dict[str, Any]) -> tuple[Any, ...]:
    return ("postgres", helper.ADMIN, 55438, config["data_directory"], "127.0.0.1", "123456789")


def test_safe_root_accepts_only_a_separate_directory_without_creating_it(
    helper: ModuleType,
) -> None:
    target = helper.PROJECT / "artifacts/not-created"
    assert helper.safe_root(target) == target.resolve()
    assert not target.exists()


@pytest.mark.parametrize("relative", ["outside", "artifacts"])
def test_safe_root_rejects_outside_and_artifacts_root(helper: ModuleType, relative: str) -> None:
    with pytest.raises(ValueError, match="separate project artifacts"):
        helper.safe_root(helper.PROJECT / relative)


@pytest.mark.parametrize("link_kind", ["is_symlink", "is_junction"])
def test_safe_root_rejects_link_traversal_before_resolving(
    helper: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    link_kind: str,
) -> None:
    linked = helper.PROJECT / "artifacts/linked"
    monkeypatch.setattr(Path, link_kind, lambda path: path == linked)
    with pytest.raises(ValueError, match="traverse a link"):
        helper.safe_root(linked / "cluster")


@pytest.mark.parametrize("change", ["remote_host", "dsn_options", "binary_bytes"])
def test_read_config_rejects_unsafe_connection_or_changed_binary(
    helper: ModuleType,
    configuration: dict[str, Any],
    change: str,
) -> None:
    root = Path(configuration["cluster_root"])
    assert helper.read_config(root) == configuration
    if change == "binary_bytes":
        suffix = ".exe" if helper.os.name == "nt" else ""
        (Path(configuration["bin_dir"]) / ("postgres" + suffix)).write_bytes(b"changed fixture")
    else:
        options = helper.conninfo_to_dict(configuration["application_dsn"])
        options.update(
            {"host": "database.example"}
            if change == "remote_host"
            else {"options": "-c role=unexpected"}
        )
        configuration["application_dsn"] = helper.make_conninfo(**options)
        (root / helper.CONFIG).write_text(json.dumps(configuration), encoding="utf-8")
    with pytest.raises(ValueError, match=r"DSN|executable"):
        helper.read_config(root)


@pytest.mark.parametrize("field", [1, 3])
def test_verify_bootstrap_rejects_wrong_live_identity_before_receipt_write(
    helper: ModuleType,
    configuration: dict[str, Any],
    field: int,
) -> None:
    row = list(bootstrap_row(helper, configuration))
    row[field] = "wrong-role" if field == 1 else str(helper.PROJECT / "unrelated-data")
    with pytest.raises(ValueError, match="bootstrap server identity mismatch"):
        helper.verify_bootstrap(FakeDatabase(tuple(row)), configuration)
    assert not (Path(configuration["cluster_root"]) / "bootstrap-identity.json").exists()


def test_verify_bootstrap_persists_then_refuses_a_different_system_identifier(
    helper: ModuleType,
    configuration: dict[str, Any],
) -> None:
    original = bootstrap_row(helper, configuration)
    helper.verify_bootstrap(FakeDatabase(original), configuration)
    receipt = Path(configuration["cluster_root"]) / "bootstrap-identity.json"
    retained = receipt.read_bytes()
    altered = (*original[:-1], "987654321")
    with pytest.raises(ValueError, match="system identity changed"):
        helper.verify_bootstrap(FakeDatabase(altered), configuration)
    assert receipt.read_bytes() == retained


def test_read_only_bootstrap_verification_cannot_create_missing_identity(
    helper: ModuleType,
    configuration: dict[str, Any],
) -> None:
    with pytest.raises(ValueError, match="identity"):
        helper.verify_bootstrap(
            FakeDatabase(bootstrap_row(helper, configuration)),
            configuration,
            create_identity=False,
        )
    assert not (Path(configuration["cluster_root"]) / "bootstrap-identity.json").exists()


def test_bootstrap_refuses_existing_target_database_before_any_create(
    helper: ModuleType,
    configuration: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = FakeDatabase(bootstrap_row(helper, configuration), (1,))
    monkeypatch.setattr(helper.psycopg, "connect", lambda *args, **kwargs: database)
    with pytest.raises(ValueError, match="provisioning already began"):
        helper.bootstrap(Path(configuration["cluster_root"]))
    assert len(database.statements) == 2
    assert all(statement.startswith("SELECT") for statement in database.statements)


def test_normal_status_refuses_changed_retained_system_identifier(
    helper: ModuleType,
    configuration: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    helper.verify_bootstrap(FakeDatabase(bootstrap_row(helper, configuration)), configuration)
    current = (
        helper.DATABASE,
        helper.ADMIN,
        55438,
        configuration["data_directory"],
        "18.1",
        "127.0.0.1",
        "on",
        "on",
        "on",
        "987654321",
    )
    database = FakeDatabase(current, [(configuration["cluster_id"],)])
    monkeypatch.setattr(helper.psycopg, "connect", lambda *args, **kwargs: database)
    with pytest.raises(ValueError, match="system identity changed"):
        helper.inspect_server(configuration)
    assert all(statement.startswith("SELECT") for statement in database.statements)


def test_run_binary_log_uses_a_real_closed_file_handle_not_an_inherited_pipe(
    helper: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def run(arguments: list[str], **kwargs: Any) -> SimpleNamespace:
        calls.append(kwargs)
        assert arguments == ["constructed-pg-ctl", "start"]
        assert kwargs["stdout"].fileno() >= 0
        assert not kwargs["stdout"].closed
        assert kwargs["stderr"] == helper.subprocess.STDOUT
        assert "capture_output" not in kwargs
        assert kwargs["timeout"] == 45
        assert kwargs["creationflags"] == helper.FLAGS
        kwargs["stdout"].write("constructed process output\n")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(helper.subprocess, "run", run)
    log = tmp_path / "controller.log"
    assert helper.run_binary(["constructed-pg-ctl", "start"], log=log) == ""
    assert calls[0]["stdout"].closed
    assert log.read_text(encoding="utf-8") == "constructed process output\n"


def test_failed_binary_does_not_expose_arguments_or_captured_diagnostics(
    helper: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        helper.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout="private diagnostic",
            stderr="private diagnostic",
        ),
    )
    with pytest.raises(RuntimeError) as caught:
        helper.run_binary(["constructed-pg-ctl", "credential-fixture"])
    assert "private diagnostic" not in str(caught.value)
    assert "credential-fixture" not in str(caught.value)
    assert "inspect the private cluster log" in str(caught.value)


@pytest.mark.parametrize("platform", ["posix", "nt"])
def test_start_uses_only_loopback_tcp_without_system_unix_socket_directory(
    helper: ModuleType,
    configuration: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    platform: str,
) -> None:
    calls: list[tuple[list[str], Path]] = []
    bindings: list[tuple[str, int]] = []

    class Probe:
        def __enter__(self) -> Probe:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def bind(self, address: tuple[str, int]) -> None:
            bindings.append(address)

    monkeypatch.setattr(helper, "read_config", lambda root: configuration)
    monkeypatch.setattr(helper, "os", SimpleNamespace(name=platform))
    monkeypatch.setattr(helper.socket, "socket", Probe)
    monkeypatch.setattr(
        helper, "run_binary", lambda arguments, *, log: calls.append((arguments, log))
    )
    root = Path(configuration["cluster_root"])
    assert helper.start(root) == configuration
    assert bindings == [("127.0.0.1", 55438)]
    assert len(calls) == 1
    arguments, log = calls[0]
    assert Path(arguments[0]).name == ("pg_ctl.exe" if platform == "nt" else "pg_ctl")
    assert arguments[arguments.index("-D") + 1] == configuration["data_directory"]
    options = arguments[arguments.index("-o") + 1]
    assert "-h 127.0.0.1 -p 55438" in options
    assert options.endswith("-c unix_socket_directories=")
    assert "-c fsync=on -c synchronous_commit=on -c full_page_writes=on" in options
    assert arguments[-1] == "start"
    assert log == root / "controller.log"
