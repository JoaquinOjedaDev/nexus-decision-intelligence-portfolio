from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Literal

import pytest
from typer.testing import CliRunner

import nexus.r4a as r4a
from nexus.cli import app
from nexus.decisioning.r4a_contracts import (
    R4A_PASS_CHECK_NAMES,
    VerificationCheck,
    VerificationReceipt,
    VerificationRuntime,
)

pytestmark = pytest.mark.r4a

runner = CliRunner()


def _write_platform_receipt(
    path: Path,
    *,
    platform_name: Literal["linux", "windows"],
    replay_sha256: str,
) -> None:
    receipt = VerificationReceipt(
        release="R4A",
        status="PASS",
        full_g4_status="OPEN",
        runtime=VerificationRuntime(
            os_family=platform_name,
            architecture="amd64" if platform_name == "windows" else "x86_64",
            python_minor="3.13",
            github_runner_os_attested=True,
        ),
        checks=tuple(
            VerificationCheck(
                name=name,
                passed=True,
                detail=(
                    "two fresh replay semantic hashes match"
                    if name == "two_run_replay"
                    else "verified"
                ),
                evidence_sha256=(replay_sha256 if name == "two_run_replay" else None),
            )
            for name in R4A_PASS_CHECK_NAMES
        ),
        replay_semantic_sha256=replay_sha256,
        superiority_claim_allowed=False,
    )
    path.write_text(json.dumps(receipt.model_dump(mode="json"), indent=2), encoding="utf-8")


def test_r4a_help_exposes_run_and_verify_commands() -> None:
    result = runner.invoke(app, ["r4a", "--help"])

    assert result.exit_code == 0
    assert "run" in result.stdout
    assert "verify" in result.stdout
    assert "compare-platforms" in result.stdout


def test_r4a_run_cli_reports_the_submit_only_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_result = SimpleNamespace(
        config=SimpleNamespace(classification="synthetic"),
        evaluation=SimpleNamespace(claim="NO_SUPERIORITY_CLAIM"),
        gate=SimpleNamespace(full_g4_status="OPEN", status="MATERIALIZED_UNVERIFIED"),
        optimizer=SimpleNamespace(
            feasibility=SimpleNamespace(independently_feasible=True),
            raw_solver_status="OPTIMAL",
            review_disposition="REVIEWABLE",
        ),
        lifecycle_state=SimpleNamespace(value="pending_approval"),
        manifest=object(),
        semantic_sha256="a" * 64,
    )
    monkeypatch.setattr(r4a, "run_r4a_once", lambda **_kwargs: fake_result)

    result = runner.invoke(
        app,
        ["r4a", "run", "--artifact-dir", str(tmp_path / "run")],
    )

    assert result.exit_code == 0
    assert '"raw_solver_status": "OPTIMAL"' in result.stdout
    assert '"lifecycle_state": "pending_approval"' in result.stdout
    assert '"manifest_created": true' in result.stdout


@pytest.mark.parametrize(("passed", "expected_exit"), ((True, 0), (False, 1)))
def test_r4a_verify_cli_exit_code_follows_the_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    passed: bool,
    expected_exit: int,
) -> None:
    fake_result = SimpleNamespace(
        passed=passed,
        receipt=SimpleNamespace(
            checks=(
                SimpleNamespace(
                    model_dump=lambda **_kwargs: {
                        "name": "bounded_check",
                        "passed": passed,
                        "detail": "fake CLI boundary result",
                    }
                ),
            ),
            full_g4_status="OPEN",
            runtime=SimpleNamespace(
                model_dump=lambda **_kwargs: {
                    "architecture": "amd64",
                    "github_runner_os_attested": False,
                    "os_family": "windows",
                    "python_minor": "3.13",
                }
            ),
            status="PASS" if passed else "FAIL",
            superiority_claim_allowed=False,
        ),
        artifact=SimpleNamespace(path=tmp_path / "r4a-verification.json"),
    )
    monkeypatch.setattr(r4a, "verify_r4a", lambda **_kwargs: fake_result)

    result = runner.invoke(
        app,
        ["r4a", "verify", "--artifact-dir", str(tmp_path / "verification")],
    )

    assert result.exit_code == expected_exit
    assert f'"passed": {str(passed).lower()}' in result.stdout
    assert f'"status": "{"PASS" if passed else "FAIL"}"' in result.stdout


def test_r4a_compare_platforms_cli_accepts_identical_normalized_receipts(
    tmp_path: Path,
) -> None:
    left = tmp_path / "linux.json"
    right = tmp_path / "windows.json"
    _write_platform_receipt(left, platform_name="linux", replay_sha256="c" * 64)
    _write_platform_receipt(right, platform_name="windows", replay_sha256="c" * 64)

    result = runner.invoke(
        app,
        [
            "r4a",
            "compare-platforms",
            "--left",
            str(left),
            "--right",
            str(right),
        ],
    )

    assert result.exit_code == 0
    assert '"status": "CROSS_PLATFORM_PASS"' in result.stdout
    assert '"passed": true' in result.stdout


def test_r4a_compare_platforms_cli_returns_nonzero_for_material_mismatch(
    tmp_path: Path,
) -> None:
    left = tmp_path / "linux.json"
    right = tmp_path / "windows.json"
    _write_platform_receipt(left, platform_name="linux", replay_sha256="c" * 64)
    _write_platform_receipt(right, platform_name="windows", replay_sha256="d" * 64)

    result = runner.invoke(
        app,
        [
            "r4a",
            "compare-platforms",
            "--left",
            str(left),
            "--right",
            str(right),
        ],
    )

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)
    assert "disagree on normalized semantic evidence" in str(result.exception)
