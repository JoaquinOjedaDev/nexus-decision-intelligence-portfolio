from __future__ import annotations

import json
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace

from click.utils import strip_ansi
from pytest import MonkeyPatch
from typer.testing import CliRunner

from nexus.cli import app

runner = CliRunner()


def _install_fake_verifier(
    monkeypatch: MonkeyPatch,
    verifier: Callable[..., SimpleNamespace],
) -> None:
    module = ModuleType("nexus.r2b")
    module.__dict__["verify_r2b"] = verifier
    monkeypatch.setitem(sys.modules, "nexus.r2b", module)


def test_r2b_verify_emits_json_and_forwards_explicit_paths(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    observed: list[tuple[Path, Path, Path]] = []
    project_root = tmp_path / "project"
    artifact_root = tmp_path / "evidence"
    config_path = tmp_path / "r2b.yaml"
    project_root.mkdir()

    def passing_verify(
        *,
        project_root: Path,
        artifact_root: Path,
        config_path: Path,
    ) -> SimpleNamespace:
        observed.append((project_root, artifact_root, config_path))
        return SimpleNamespace(
            passed=True,
            to_summary=lambda: {
                "release": "R2B",
                "status": "PASS",
                "checks": {"direct_parity": True},
            },
        )

    _install_fake_verifier(monkeypatch, passing_verify)
    result = runner.invoke(
        app,
        [
            "r2b",
            "verify",
            "--project-root",
            str(project_root),
            "--artifact-root",
            str(artifact_root),
            "--config-path",
            str(config_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert json.loads(result.stdout) == {
        "release": "R2B",
        "status": "PASS",
        "checks": {"direct_parity": True},
    }
    assert observed == [(project_root, artifact_root, config_path)]


def test_r2b_verify_exits_nonzero_after_emitting_failure_summary(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    def failing_verify(
        *,
        project_root: Path,
        artifact_root: Path,
        config_path: Path,
    ) -> SimpleNamespace:
        del project_root, artifact_root, config_path
        return SimpleNamespace(
            passed=False,
            to_summary=lambda: {
                "release": "R2B",
                "status": "FAIL",
                "checks": {"reverse_order_replay": False},
            },
        )

    _install_fake_verifier(monkeypatch, failing_verify)
    result = runner.invoke(
        app,
        [
            "r2b",
            "verify",
            "--artifact-root",
            str(tmp_path / "failed"),
        ],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "release": "R2B",
        "status": "FAIL",
        "checks": {"reverse_order_replay": False},
    }


def test_r2b_command_group_exposes_verify_and_all_path_options() -> None:
    group = runner.invoke(app, ["r2b", "--help"], color=True)
    command = runner.invoke(app, ["r2b", "verify", "--help"], color=True)
    group_output = strip_ansi(group.stdout)
    command_output = strip_ansi(command.stdout)

    assert group.exit_code == 0, group.stdout
    assert "verify" in group_output
    assert command.exit_code == 0, command.stdout
    assert "--project-root" in command_output
    assert "--artifact-root" in command_output
    assert "--config-path" in command_output
