from __future__ import annotations

import json
import shutil
from pathlib import Path

from nexus.data import QuarantineReason
from nexus.r1 import run_r1_demo, verify_r1
from nexus.workflow import DecisionState

ROOT = Path(__file__).resolve().parents[1]
SOURCES = ROOT / "fixtures" / "r1"


def test_complete_r1_loop_matches_the_executable_contract(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "demo"

    result = run_r1_demo(source_dir=SOURCES, artifact_dir=artifact_dir)

    assert [int(point.predicted_demand) for point in result.forecast.points] == [
        11,
        13,
        9,
        12,
        14,
        10,
        8,
    ]
    assert result.forecast.total_expected_demand() == 77
    assert result.recommendation.on_hand == 35
    assert result.recommendation.on_order == 10
    assert result.recommendation.safety_stock == 15
    assert result.recommendation.recommended_quantity == 47
    assert result.projection.state is DecisionState.OUTCOME_OBSERVED
    assert len(result.projection.events) == 5
    assert result.effect_count == 1
    assert result.actual_demand == 79
    assert result.absolute_demand_error == 2
    assert result.summary_path.is_file()
    assert (artifact_dir / "workflow" / "lifecycle.jsonl").is_file()
    assert (artifact_dir / "workflow" / "mock-erp-effects.json").is_file()


def test_complete_r1_loop_resumes_without_a_second_external_effect(
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "demo"

    first = run_r1_demo(source_dir=SOURCES, artifact_dir=artifact_dir)
    second = run_r1_demo(source_dir=SOURCES, artifact_dir=artifact_dir)

    assert second.manifest.fingerprint == first.manifest.fingerprint
    assert second.effect == first.effect
    assert second.effect_count == 1
    assert len(second.projection.events) == 5
    persisted = json.loads(
        (artifact_dir / "workflow" / "mock-erp-effects.json").read_text(encoding="utf-8")
    )
    assert len(persisted["effects"]) == 1


def test_r1_verification_runs_two_equivalent_closed_loops(tmp_path: Path) -> None:
    results = verify_r1(source_dir=SOURCES, artifact_dir=tmp_path / "verification")

    assert results
    assert all(result.passed for result in results)
    assert (tmp_path / "verification" / "r1-verification-summary.json").is_file()


def test_late_old_dated_correction_cannot_enter_end_to_end_forecast(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    shutil.copytree(SOURCES, source_dir)
    demand_path = source_dir / "demand_history.csv"
    demand_text = demand_path.read_text(encoding="utf-8").replace(
        "2026-07-08,SKU-001,DC-ARG-01,11,2026-07-15T08:00:00Z",
        "2026-07-08,SKU-001,DC-ARG-01,999,2026-07-15T09:00:01Z",
    )
    demand_path.write_text(demand_text, encoding="utf-8")

    result = run_r1_demo(
        source_dir=source_dir,
        artifact_dir=tmp_path / "late-correction-demo",
    )

    assert result.forecast.points[0].predicted_demand == 0
    assert all(point.predicted_demand != 999 for point in result.forecast.points)
    assert any(
        row.reason_code is QuarantineReason.BATCH_NOT_AVAILABLE_AS_OF
        for row in result.data.quarantined
    )
