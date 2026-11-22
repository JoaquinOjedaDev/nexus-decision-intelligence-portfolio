from __future__ import annotations

import json
from pathlib import Path

from pytest import MonkeyPatch

from nexus.r3 import run_r3a_demo, verify_r3

ROOT = Path(__file__).resolve().parents[1]


def test_r3a_demo_materializes_the_registered_scientific_contract(tmp_path: Path) -> None:
    result = run_r3a_demo(project_root=ROOT, artifact_dir=tmp_path / "demo")

    assert result.snapshot.contract.source_dataset == "gold.r2_demand_history"
    assert result.snapshot.contract.source_row_count == 112
    assert len(result.feature_rows) == 84
    assert len(result.forecasts) == 12
    assert len(result.metric_rows) == 84
    assert result.overall_metrics.observation_count == 84
    assert result.stress_metrics.observation_count == 28
    assert str(result.stress_metrics.mase) == "1.273953823953823953823953824"
    assert len(result.hierarchical_forecasts) == 735
    assert result.config.promotion.automatic is False
    assert result.config.promotion.synthetic_performance_claims_allowed is False
    assert all(
        row.max_feature_available_at is None or row.max_feature_available_at <= row.issue_time
        for row in result.feature_rows
    )
    assert all(
        point.q025 <= point.q10 <= point.q50 <= point.q90 <= point.q975
        for forecast in result.forecasts
        for point in forecast.points
    )

    expected_datasets = {
        "snapshot.r3a_demand_registration",
        "features.r3a_point_in_time",
        "labels.r3a_matured",
        "forecasts.r3a_seasonal_naive",
        "manifests.r3a_forecast_provenance",
        "evaluation.r3a_baseline",
    }
    assert {artifact.dataset for artifact in result.artifacts} == expected_datasets
    assert all(artifact.path.is_file() for artifact in result.artifacts)
    provenance_ref = next(
        artifact
        for artifact in result.artifacts
        if artifact.dataset == "manifests.r3a_forecast_provenance"
    )
    provenance = json.loads(provenance_ref.path.read_text(encoding="utf-8"))
    assert provenance["forecast_count"] == 12
    assert all(item["forecast_sha256"] for item in provenance["forecasts"])
    assert all(item["residuals_sha256"] for item in provenance["forecasts"])
    assert result.semantic_manifest.dataset == "manifests.r3a_semantic"
    semantic = json.loads(result.semantic_manifest.path.read_text(encoding="utf-8"))
    assert semantic["release"] == "R3A"
    assert semantic["status"] == "MATERIALIZED_UNVERIFIED"
    assert semantic["champion"]["automatic_promotion"] is False
    assert semantic["champion"]["candidate_status"] == ("not_run_insufficient_representative_data")

    execution = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert execution["status"] == "MATERIALIZED_UNVERIFIED"
    assert execution["runtime"]["python_version"]
    assert execution["semantic_manifest_sha256"] == result.semantic_manifest.content_sha256


def test_r3a_verifier_replays_identical_registered_artifacts(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "verification"

    results = verify_r3(project_root=ROOT, artifact_dir=artifact_dir)

    assert len(results) == 10
    assert all(result.passed for result in results), results
    assert {result.name for result in results} == {
        "registered exact R2 Gold snapshot",
        "point-in-time feature boundary",
        "adversarial temporal leakage rejection",
        "nonnegative monotonic probabilistic baseline",
        "matured labels + probabilistic metrics",
        "bottom-up hierarchy coherence",
        "reproducible registered-snapshot run",
        "fail-closed promotion policy",
        "coverage recorded against smoke tolerance",
        "bounded modeling resource profile",
    }
    summary = json.loads(
        (artifact_dir / "r3a-verification-summary.json").read_text(encoding="utf-8")
    )
    assert summary["status"] == "PASS"
    assert summary["release"] == "R3A"
    assert len(summary["artifact_hashes"]) == 7
    assert len(summary["gate_manifest_sha256"]) == 64
    assert Path(summary["gate_manifest_path"]).is_file()
    assert set(summary["execution_evidence_file_sha256"]) == {"run_a", "run_b"}


def test_r3a_verifier_persists_an_immutable_fail_gate_for_setup_errors(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    def failed_build(*, project_root: Path, artifact_dir: Path, seed: int) -> None:
        del project_root, artifact_dir, seed
        raise RuntimeError("injected upstream failure")

    monkeypatch.setattr("nexus.r3._build_upstream", failed_build)
    artifact_dir = tmp_path / "failed-verification"

    results = verify_r3(project_root=ROOT, artifact_dir=artifact_dir)

    assert len(results) == 1
    assert results[0].passed is False
    assert "injected upstream failure" in results[0].detail
    summary = json.loads(
        (artifact_dir / "r3a-verification-summary.json").read_text(encoding="utf-8")
    )
    assert summary["status"] == "FAIL"
    assert summary["failure_phase"] == "setup_or_materialization"
    gate_path = Path(summary["gate_manifest_path"])
    assert gate_path.is_file()
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert gate["status"] == "FAIL"
    assert gate["checks"][0]["passed"] is False
