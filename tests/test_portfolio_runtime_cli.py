"""Thin local CLI integration; substantive acceptance lives in its owning modules."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from nexus.portfolio.cli import app


def test_learning_help_advertises_optional_local_tracking() -> None:
    result = CliRunner().invoke(app, ["learn-runtime", "--help"], color=False)
    assert result.exit_code == 0
    assert "track-local" in result.stdout
    assert "tracking" in result.stdout and "MLflow" in result.stdout


@pytest.mark.parametrize("status,exit_code", [("pass", 0), ("fail", 1)])
def test_recovery_cli_forwards_only_explicit_isolated_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: str, exit_code: int
) -> None:
    from nexus.portfolio import recovery

    calls: list[tuple[Path, bool]] = []

    def run(path: Path, *, allow_mutation: bool = False) -> dict[str, Any]:
        calls.append((path, allow_mutation))
        return {"status": status, "runtime_evidence_restored": False}

    monkeypatch.setattr(recovery, "run_recovery_acceptance", run)
    directory = tmp_path / "new-recovery"
    result = CliRunner().invoke(app, ["verify-recovery", str(directory)])
    assert result.exit_code == exit_code, result.output
    assert calls == [(directory, True)]
    assert not directory.exists()


def test_invalid_actual_does_not_initialize_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nexus.portfolio import runtime_service

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("invalid actual must fail before runtime preparation")

    monkeypatch.setattr(runtime_service, "prepare_runtime", forbidden)
    path = tmp_path / "invalid.json"
    path.write_text('{"event_id":"untrusted-missing-required-fields"}', encoding="utf-8")
    result = CliRunner().invoke(app, ["runtime-actual", str(path)])
    assert result.exit_code != 0
