from __future__ import annotations

from pathlib import Path

import pytest

from nexus.modeling.r3b_tracking import R3BTrackingError, log_r3b_challenger
from nexus.storage import file_sha256

pytestmark = pytest.mark.r3b

SHA = "0" * 64


def test_local_mlflow_run_is_registered_and_normalized_without_mutable_ids(
    tmp_path: Path,
) -> None:
    evidence = tmp_path / "input-evidence.json"
    evidence.write_text('{"gate":"R3B"}\n', encoding="utf-8")

    receipt = log_r3b_challenger(
        tmp_path / "tracking",
        experiment_name="nexus-r3b-synthetic",
        run_name="r3b-final-stress",
        registered_model_name="nexus-r3b-demand-challenger",
        params={"seed": 20260718, "deterministic": True},
        metrics={"candidate_wis": 1.25, "baseline_wis": 1.5},
        tags={"classification": "synthetic"},
        semantic_hashes={"config": SHA, "data": "1" * 64},
        model_json='{"model":"serialized"}',
        evidence_paths=(evidence,),
    )

    assert receipt.tracking_store.is_file()
    assert receipt.registered_model_version == 1
    assert receipt.readback_verified is True
    assert receipt.registered_model_source == f"runs:/{receipt.run_id}/model"
    assert receipt.registered_model_status == "READY"
    assert receipt.model_artifact_sha256 == file_sha256(
        next(receipt.artifact_root.rglob("ensemble.json"))
    )
    assert receipt.evidence_file_sha256 == {evidence.name: file_sha256(evidence)}
    assert len(receipt.run_id) > 10
    normalized = receipt.normalized_payload()
    assert "run_id" not in normalized
    assert "tracking_store" not in normalized
    assert "model_artifact_sha256" not in normalized
    assert normalized["semantic_hashes"] == {"config": SHA, "data": "1" * 64}
    assert normalized["tags"] == {"classification": "synthetic"}
    assert len(receipt.normalized_sha256) == 64


def test_tracking_boundary_rejects_nonfinite_metrics_and_invalid_hashes(tmp_path: Path) -> None:
    common = {
        "output_dir": tmp_path / "tracking",
        "experiment_name": "experiment",
        "run_name": "run",
        "registered_model_name": "model",
        "params": {},
        "tags": {},
        "model_json": "{}",
    }
    with pytest.raises(R3BTrackingError, match="finite"):
        log_r3b_challenger(
            **common,
            metrics={"wis": float("nan")},
            semantic_hashes={"config": SHA},
        )
    with pytest.raises(R3BTrackingError, match="SHA-256"):
        log_r3b_challenger(
            **common,
            metrics={"wis": 1.0},
            semantic_hashes={"config": "not-a-hash"},
        )
