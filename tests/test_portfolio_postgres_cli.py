"""Explicit configuration and redacted CLI failures; no services or paid calls."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from typer.main import get_command
from typer.testing import CliRunner

from nexus.portfolio import postgres_cli
from nexus.portfolio.postgres_backend import PostgresBackendError
from nexus.portfolio.store import StoreError


def private_config(tmp_path: Path) -> Path:
    path = tmp_path / "application.json"
    path.write_text(
        json.dumps(
            {
                "dsn": (
                    "host=127.0.0.1 port=55438 dbname=nexus_test "
                    "user=nexus_app password=DO_NOT_PRINT"
                ),
                "schema": "nexus_cli_test",
                "runtime_root": str(tmp_path / "runtime"),
            }
        ),
        encoding="utf-8",
    )
    return path


def test_config_has_explicit_root_and_redacted_repr(tmp_path: Path) -> None:
    root, config = postgres_cli.read_config(private_config(tmp_path))
    assert root == tmp_path / "runtime"
    assert config.artifact_root == root / "evidence" / "initial"
    assert "DO_NOT_PRINT" not in repr(config)
    assert not root.exists()


@pytest.mark.parametrize(
    "mutation", ["extra", "relative", "remote", "empty", "non_object", "large"]
)
def test_invalid_config_does_not_echo_credentials(tmp_path: Path, mutation: str) -> None:
    path = private_config(tmp_path)
    values = json.loads(path.read_text(encoding="utf-8"))
    if mutation == "extra":
        values["migration_dsn"] = "DO_NOT_PRINT"
    elif mutation == "relative":
        values["runtime_root"] = "relative-state"
    elif mutation == "remote":
        values["dsn"] = values["dsn"].replace("127.0.0.1", "example.com")
    elif mutation == "empty":
        values["schema"] = ""
    elif mutation == "non_object":
        values = ["DO_NOT_PRINT"]
    else:
        values["dsn"] = "DO_NOT_PRINT" * 2000
    path.write_text(json.dumps(values), encoding="utf-8")
    result = CliRunner().invoke(postgres_cli.app, ["status", str(path)])
    assert result.exit_code == 1
    assert "DO_NOT_PRINT" not in result.output
    assert "configuration_error" in result.output
    assert not (tmp_path / "runtime").exists()


@pytest.mark.parametrize("command", ["prepare", "serve", "status"])
def test_database_unavailable_is_redacted_and_no_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    path = private_config(tmp_path)

    def unavailable(*args: Any, **kwargs: Any) -> None:
        raise PostgresBackendError("connection_failed", "Local database is unavailable")

    monkeypatch.setattr(postgres_cli, "load_postgres_runtime", unavailable)
    monkeypatch.setattr(postgres_cli, "prepare_postgres_runtime", unavailable)
    result = CliRunner().invoke(postgres_cli.app, [command, str(path)])
    assert result.exit_code == 1
    assert "connection_failed" in result.output
    assert "DO_NOT_PRINT" not in result.output
    assert "Traceback" not in result.output
    assert not (tmp_path / "runtime").exists()


@pytest.mark.parametrize("command", ["event", "actual"])
def test_malformed_event_fails_before_opening_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    path = private_config(tmp_path)
    payload = tmp_path / "event.json"
    payload.write_text('{"invalid":"DO_NOT_PRINT"}', encoding="utf-8")

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("invalid event must fail before reading config or database")

    monkeypatch.setattr(postgres_cli, "read_config", forbidden)
    monkeypatch.setattr(postgres_cli, "load_postgres_runtime", forbidden)
    result = CliRunner().invoke(postgres_cli.app, [command, str(path), str(payload)])
    assert result.exit_code == 1
    assert "invalid_input" in result.output
    assert "DO_NOT_PRINT" not in result.output


def test_existing_sqlite_state_rejected_without_replacement(tmp_path: Path) -> None:
    path = private_config(tmp_path)
    directory = tmp_path / "runtime"
    directory.mkdir()
    database = directory / "portfolio.sqlite3"
    database.write_bytes(b"untouched")
    with pytest.raises(StoreError):
        postgres_cli.read_config(path)
    assert database.read_bytes() == b"untouched"


def test_all_explicit_commands_are_discoverable() -> None:
    result = CliRunner().invoke(postgres_cli.app, ["--help"], color=False)
    assert result.exit_code == 0
    assert all(
        command in result.output
        for command in ("prepare", "serve", "status", "event", "worker", "actual")
    )


def test_registered_postgres_path_is_explicit_and_does_not_change_sqlite_default() -> None:
    from nexus.cli import app

    runner = CliRunner()
    nested = runner.invoke(app, ["portfolio", "postgres", "--help"], color=False)
    assert nested.exit_code == 0
    assert all(command in nested.output for command in ("prepare", "serve", "status", "worker"))
    normal = runner.invoke(app, ["portfolio", "serve", "--help"], color=False)
    assert normal.exit_code == 0
    serve_options = get_command(app).commands["portfolio"].commands["serve"].params
    assert any("--state-dir" in option.opts for option in serve_options)
    assert not any("--config-file" in option.opts for option in serve_options)


def test_registered_status_requires_private_config_without_implicit_database_access() -> None:
    from nexus.cli import app

    result = CliRunner().invoke(app, ["portfolio", "postgres", "status"], color=False)
    assert result.exit_code == 2
