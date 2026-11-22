from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from nexus.modeling.r3_hierarchy import HierarchyLevel
from nexus.modeling.r3b_evaluation import R3BPlatformReceipt
from nexus.r3b import R3BVerificationResult, verify_r3b

pytestmark = [pytest.mark.r3b, pytest.mark.r3b_acceptance]


@pytest.fixture(scope="module")
def verified_r3b(tmp_path_factory: pytest.TempPathFactory) -> R3BVerificationResult:
    """Exercise the complete bounded gate once; all acceptance assertions share it."""

    configured_root = os.environ.get("R3B_ACCEPTANCE_ARTIFACT_ROOT", "").strip()
    artifact_root = (
        Path(configured_root) if configured_root else tmp_path_factory.mktemp("r3b-acceptance")
    )
    return verify_r3b(artifact_root=artifact_root)


def test_complete_r3b_gate_proves_the_bounded_engineering_claims(
    verified_r3b: R3BVerificationResult,
) -> None:
    result = verified_r3b

    assert result.passed is True
    assert result.status == "R3B_ENGINEERING_PASS_SYNTHETIC_PROMOTION_BLOCKED"
    assert result.first_run is not None
    assert result.second_run is not None
    assert result.resource_evidence is not None
    assert result.resource_evidence.passed is True
    assert result.final_promotion is not None

    first = result.first_run
    second = result.second_run
    assert first.platform_receipt.to_json() == second.platform_receipt.to_json()
    replay_check = next(
        check for check in result.checks if check.name == "same_platform_exact_replay"
    )
    assert replay_check.passed is True

    expected_horizons = set(first.config.forecast_contract.horizons_days)
    assert set(first.calibrators) == expected_horizons
    assert all(calibrator.assessed for calibrator in first.calibrators.values())
    assert all(calibrator.support >= 20 for calibrator in first.calibrators.values())
    assert not any(row.candidate.cold_start for row in first.calibration_evaluation.rows)
    assert {
        horizon: sum(
            row.truth.horizon_days == horizon and not row.candidate.cold_start
            for row in first.calibration_evaluation.rows
        )
        for horizon in expected_horizons
    } == {horizon: first.calibrators[horizon].support for horizon in expected_horizons}

    evaluated = first.final_evaluation.rows
    assert len(evaluated) > 0
    candidate_keys = {row.candidate.forecast_key for row in evaluated}
    baseline_keys = {row.baseline.forecast_key for row in evaluated}
    truth_keys = {row.truth.forecast_key for row in evaluated}
    assert candidate_keys == baseline_keys == truth_keys
    assert len(candidate_keys) == len(evaluated)
    cold_start_rows = tuple(row for row in evaluated if row.candidate.cold_start)
    assert cold_start_rows
    assert all(row.candidate.quantiles == row.baseline.quantiles for row in cold_start_rows)

    assert first.lead_time_evaluation.training_label_count > 0
    assert first.lead_time_evaluation.holdout_label_count > 0
    assert all(
        row.prediction_as_of < row.label_available_at for row in first.lead_time_evaluation.rows
    )
    assert {
        artifact.dataset
        for artifact in first.artifacts
        if artifact.dataset.startswith("r3b.forecasts.lead_time")
        or artifact.dataset.startswith("r3b.evaluation.lead_time")
    } == {
        "r3b.forecasts.lead_time_temporal_holdout",
        "r3b.evaluation.lead_time",
    }

    assert {row.level for row in first.hierarchy_rows} == {
        HierarchyLevel.SKU_LOCATION,
        HierarchyLevel.SKU,
        HierarchyLevel.NETWORK,
    }
    receipt_descriptors = {item.path: item.value for item in first.platform_receipt.descriptors}
    assert receipt_descriptors["hierarchy.joint_distribution_calibration_claimed"] == "false"
    hierarchy_artifact = next(
        artifact
        for artifact in first.artifacts
        if artifact.dataset == "r3b.forecasts.final_stress_hierarchy"
    )
    hierarchy_evidence = json.loads(hierarchy_artifact.path.read_text(encoding="utf-8"))
    assert hierarchy_evidence["joint_distribution_calibration_claimed"] is False

    assert first.tracking.tracking_store.name == "mlflow.sqlite"
    assert first.tracking.tracking_store.is_file()
    assert first.tracking.registered_model_version >= 1
    assert first.promotion.promotion_applied is False
    assert second.promotion.promotion_applied is False
    assert result.final_promotion.promotion_applied is False

    assert result.platform_receipt_path is not None
    assert result.platform_receipt_path.is_file()
    persisted_receipt = R3BPlatformReceipt.from_json(
        result.platform_receipt_path.read_text(encoding="utf-8")
    )
    assert persisted_receipt == first.platform_receipt
    assert result.gate_manifest.path.is_file()
    assert result.summary_path.is_file()
    gate_evidence = json.loads(result.gate_manifest.path.read_text(encoding="utf-8"))
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert gate_evidence["status"] == result.status
    assert gate_evidence["promotion_applied"] is False
    assert gate_evidence["promotion_status"] == result.final_promotion.status
    assert summary["passed"] is True
    assert summary["status"] == result.status


def test_missing_config_fails_closed_and_persists_gate_evidence(tmp_path: Path) -> None:
    artifact_root = tmp_path / "missing-config-gate"
    result = verify_r3b(
        artifact_root=artifact_root,
        config_path=tmp_path / "does-not-exist.yaml",
    )

    assert result.passed is False
    assert result.status == "R3B_GATE_FAIL"
    assert result.first_run is None
    assert result.second_run is None
    assert result.resource_evidence is None
    assert result.final_promotion is None
    assert result.platform_receipt_path is None
    assert any(
        check.name == "setup_or_execution" and check.passed is False for check in result.checks
    )
    assert result.gate_manifest.path.is_file()
    assert result.summary_path.is_file()
    gate_evidence = json.loads(result.gate_manifest.path.read_text(encoding="utf-8"))
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert gate_evidence["status"] == "FAIL"
    assert summary["passed"] is False
    assert summary["status"] == "R3B_GATE_FAIL"
    assert Path(summary["gate_manifest"]).is_file()
