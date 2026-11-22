from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import nexus.r3b as r3b
from nexus.cli import app

pytestmark = pytest.mark.r3b

runner = CliRunner()


def _verification_result(
    tmp_path: Path,
    *,
    passed: bool,
) -> SimpleNamespace:
    status = "R3B_ENGINEERING_PASS_SYNTHETIC_PROMOTION_BLOCKED" if passed else "R3B_GATE_FAIL"
    return SimpleNamespace(
        passed=passed,
        status=status,
        checks=(
            SimpleNamespace(
                name="synthetic_check",
                passed=passed,
                detail="fake CLI boundary result",
            ),
        ),
        platform_receipt_path=(tmp_path / "r3b-platform-receipt.json") if passed else None,
        final_promotion=(SimpleNamespace(status="CHALLENGER_LOST") if passed else None),
        summary_path=tmp_path / "r3b-verification-summary.json",
    )


def test_r3b_help_exposes_the_three_operator_commands() -> None:
    result = runner.invoke(app, ["r3b", "--help"])

    assert result.exit_code == 0
    assert "run" in result.stdout
    assert "verify" in result.stdout
    assert "compare-platforms" in result.stdout


@pytest.mark.parametrize(("passed", "expected_exit"), ((True, 0), (False, 1)))
def test_r3b_verify_cli_exit_code_follows_the_gate_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    passed: bool,
    expected_exit: int,
) -> None:
    fake_result = _verification_result(tmp_path, passed=passed)
    monkeypatch.setattr(r3b, "verify_r3b", lambda **_kwargs: fake_result)

    result = runner.invoke(
        app,
        ["r3b", "verify", "--artifact-dir", str(tmp_path / "artifacts")],
    )

    assert result.exit_code == expected_exit
    assert fake_result.status in result.stdout
    assert f'"passed": {str(passed).lower()}' in result.stdout
