from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from pytest import MonkeyPatch
from typer.testing import CliRunner

from nexus.cli import app
from nexus.r3 import R3VerificationResult

runner = CliRunner()


def _fake_demo_result(summary_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        snapshot=SimpleNamespace(
            contract=SimpleNamespace(
                source_row_count=112,
                registration_sha256="a" * 64,
            )
        ),
        config=SimpleNamespace(
            feature_contract=SimpleNamespace(version="r3a-pit-v1"),
            promotion=SimpleNamespace(automatic=False),
        ),
        feature_rows=tuple(range(84)),
        forecasts=tuple(range(12)),
        metric_rows=tuple(range(84)),
        overall_metrics=SimpleNamespace(weighted_interval_score=Decimal("2.5")),
        stress_metrics=SimpleNamespace(weighted_interval_score=Decimal("3.5")),
        hierarchical_forecasts=tuple(range(735)),
        hierarchical_actuals=tuple(range(147)),
        semantic_manifest=SimpleNamespace(content_sha256="b" * 64),
        duration_seconds=1.25,
        peak_rss_mib=128.5,
        artifact_bytes=4096,
        summary_path=summary_path,
    )


def test_r3_demo_command_reports_each_scientific_boundary(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    observed: list[tuple[Path, Path, Path | None]] = []
    summary_path = tmp_path / "r3a-execution-evidence.json"

    def fake_demo(
        *,
        project_root: Path,
        artifact_dir: Path,
        config_path: Path | None,
    ) -> SimpleNamespace:
        observed.append((project_root, artifact_dir, config_path))
        return _fake_demo_result(summary_path)

    monkeypatch.setattr("nexus.r3.run_r3a_demo", fake_demo)
    artifact_dir = tmp_path / "demo"
    config_path = tmp_path / "contract.yaml"

    result = runner.invoke(
        app,
        [
            "r3",
            "demo",
            "--artifact-dir",
            str(artifact_dir),
            "--config-path",
            str(config_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    normalized_output = " ".join(result.stdout.split())
    assert "NEXUS R3A probabilistic intelligence" in normalized_output
    assert "Registered R2 Gold" in normalized_output
    assert "Point-in-time" in normalized_output
    assert "84 feature rows" in normalized_output
    assert "Probabilistic" in normalized_output
    assert "12 series-origin" in normalized_output
    assert "Bottom-up hierarchy" in normalized_output
    assert "MATERIALIZED_UNVERIFIED" in normalized_output
    assert "Evidence:" in normalized_output
    assert len(observed) == 1
    assert observed[0][0].is_dir()
    assert observed[0][1:] == (artifact_dir, config_path)


def test_r3_verify_command_reports_pass_and_exits_nonzero_on_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    observed_artifact_dirs: list[Path] = []

    def passing_verify(*, project_root: Path, artifact_dir: Path) -> list[R3VerificationResult]:
        assert project_root.is_dir()
        observed_artifact_dirs.append(artifact_dir)
        return [R3VerificationResult("registered replay", True, "hashes match")]

    monkeypatch.setattr("nexus.r3.verify_r3", passing_verify)
    artifact_dir = tmp_path / "verification"
    passed = runner.invoke(app, ["r3", "verify", "--artifact-dir", str(artifact_dir)])

    assert passed.exit_code == 0, passed.stdout
    assert "NEXUS R3A verification" in passed.stdout
    assert "registered replay" in passed.stdout
    assert "PASS" in passed.stdout
    assert observed_artifact_dirs == [artifact_dir]

    def failing_verify(*, project_root: Path, artifact_dir: Path) -> list[R3VerificationResult]:
        del project_root, artifact_dir
        return [R3VerificationResult("registered replay", False, "semantic hash mismatch")]

    monkeypatch.setattr("nexus.r3.verify_r3", failing_verify)
    failed = runner.invoke(
        app,
        ["r3", "verify", "--artifact-dir", str(tmp_path / "failed")],
    )

    assert failed.exit_code == 1
    assert "FAIL" in failed.stdout
    assert "semantic hash mismatch" in failed.stdout


def test_r3_command_group_exposes_demo_and_verify() -> None:
    result = runner.invoke(app, ["r3", "--help"])

    assert result.exit_code == 0, result.stdout
    assert "demo" in result.stdout
    assert "verify" in result.stdout
