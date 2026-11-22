"""Scientific provenance and tamper checks for the actual portfolio adapter."""

from __future__ import annotations

import copy
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

import pytest

from nexus.contracts import RecommendationManifest
from nexus.decisioning.r4a_contracts import load_champion_forecast_adapter, load_r4a_config
from nexus.decisioning.r4a_scenarios import build_scenarios
from nexus.modeling.r3_artifacts import canonical_json_sha256
from nexus.portfolio.engine import (
    SEED_FILENAME,
    _input_fingerprint,
    build_seed,
    load_seed,
    validate_seed,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def evidence(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict[str, Any]]:
    root = tmp_path_factory.mktemp("portfolio-evidence")
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv(
            "PATH", str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]
        )
        seed = build_seed(PROJECT_ROOT, root)
    return root, seed


def test_fresh_build_uses_existing_authorities(evidence: tuple[Path, dict[str, Any]]) -> None:
    _, seed = evidence
    config = load_r4a_config(PROJECT_ROOT / "configs/decisioning/r4a-smoke.yaml")
    adapter = load_champion_forecast_adapter(config)
    manifest = RecommendationManifest.model_validate_json(json.dumps(seed["original_manifest"]))
    assert seed["metadata"]["model_champion"] == adapter.selected_model
    assert seed["metadata"]["challenger_status"] == "CHALLENGER_LOST"
    assert seed["metadata"]["superiority_claim_allowed"] is False
    assert seed["manifest_fingerprint"] == manifest.fingerprint
    assert seed["metrics"]["dbt_status"] == "PASS"
    assert "PASS=" in seed["metrics"]["dbt_summary"]
    assert any(item["dataset"] == "r2.dbt_build_manifest" for item in seed["artifacts"])
    assert seed["metrics"]["independently_feasible"] is True
    assert seed["metrics"]["quarantined_rows"] > 0
    assert seed["metrics"]["gold_products"] == 5
    assert seed["metrics"]["order_value"] <= seed["metrics"]["budget"]
    for line in seed["recommendations"]:
        assert line["observed_demand"] >= 0
        assert line["outcome_provenance"] == "synthetic_scenario_replay"
        assert line["status"] == "pending"
        assert line["history"]
        assert (
            max(point["date"] for point in line["history"])
            < adapter.source_origin.date().isoformat()
        )


def test_reload_verifies_artifacts_without_rerunning_solver(
    evidence: tuple[Path, dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, seed = evidence

    def forbidden(**kwargs: Any) -> None:
        raise AssertionError("cached load must not rerun the solver")

    monkeypatch.setattr("nexus.portfolio.engine.run_r4a_once", forbidden)
    assert build_seed(PROJECT_ROOT, root) == seed


def test_fresh_replay_has_same_decision_fingerprint(
    evidence: tuple[Path, dict[str, Any]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, seed = evidence
    monkeypatch.setenv("PATH", str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"])
    replay = build_seed(PROJECT_ROOT, tmp_path)
    assert replay["manifest_fingerprint"] == seed["manifest_fingerprint"]
    assert replay["recommendations"] == seed["recommendations"]
    assert replay["scenarios"] == seed["scenarios"]


def test_scenario_metrics_are_derived_from_persisted_simulation(
    evidence: tuple[Path, dict[str, Any]],
) -> None:
    root, seed = evidence
    reference = next(
        item for item in seed["artifacts"] if item["dataset"] == "portfolio.scenario_paths"
    )
    paths = json.loads((root / reference["path"]).read_text(encoding="utf-8"))["optimizer"]
    for scenario in seed["scenarios"]:
        matching = [path for path in paths if path["path_key"].endswith(":" + scenario["id"])]
        assert len(matching) == scenario["evaluation_paths"]
        assert scenario["expected_cost"] == pytest.approx(
            sum(path["total_cost_cents"] for path in matching) / len(matching) / 100
        )
    by_id = {scenario["id"]: scenario for scenario in seed["scenarios"]}
    assert by_id["demand_surge"]["demand_units"] > by_id["normal"]["demand_units"]
    assert by_id["supplier_delay"]["expected_cost"] != by_id["normal"]["expected_cost"]


def test_changed_scenario_changes_computation() -> None:
    config = load_r4a_config(PROJECT_ROOT / "configs/decisioning/r4a-smoke.yaml")
    changed = config.model_copy(
        update={"scenarios": config.scenarios.model_copy(update={"demand_surge_percent": 175})}
    )
    original = build_scenarios(config, purpose="evaluation")
    alternative = build_scenarios(changed, purpose="evaluation")
    for before, after in zip(original, alternative, strict=True):
        if before.name == "demand_surge":
            assert sum(sum(line.daily_demand_units) for line in after.lines) > sum(
                sum(line.daily_demand_units) for line in before.lines
            )
        else:
            assert before == after


def test_corrupt_persisted_artifact_fails_closed(
    evidence: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    root, seed = evidence
    copied = tmp_path / "copied"
    shutil.copytree(root, copied)
    artifact = next(item for item in seed["artifacts"] if item["dataset"] == "r4a.decision")
    (copied / artifact["path"]).write_text('{"forged":true}', encoding="utf-8")
    with pytest.raises(ValueError, match="artifact checksum mismatch"):
        load_seed(copied)


def test_forged_recommendation_rejected_even_after_rehash(
    evidence: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    _, seed = evidence
    forged = copy.deepcopy(seed)
    forged["recommendations"][0]["recommended_quantity"] += 1
    (tmp_path / SEED_FILENAME).write_text(
        json.dumps({"payload": forged, "payload_sha256": canonical_json_sha256(forged)}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="differs from governed manifest"):
        load_seed(tmp_path)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1.0])
def test_invalid_numeric_values_rejected(
    evidence: tuple[Path, dict[str, Any]], value: float
) -> None:
    _, seed = evidence
    invalid = copy.deepcopy(seed)
    invalid["recommendations"][0]["expected_cost"] = value
    with pytest.raises(ValueError):
        validate_seed(invalid)


def test_path_traversal_rejected(evidence: tuple[Path, dict[str, Any]], tmp_path: Path) -> None:
    _, seed = evidence
    forged = copy.deepcopy(seed)
    forged["artifacts"] = [{"path": "../outside", "file_sha256": "0" * 64}]
    (tmp_path / SEED_FILENAME).write_text(
        json.dumps({"payload": forged, "payload_sha256": canonical_json_sha256(forged)}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unsafe"):
        load_seed(tmp_path)


def test_governing_input_changes_invalidate_cache(evidence: tuple[Path, dict[str, Any]]) -> None:
    root, _ = evidence
    from nexus.portfolio.engine import StalePortfolioEvidenceError

    with pytest.raises(StalePortfolioEvidenceError, match="stale"):
        load_seed(root, expected_input_fingerprint="0" * 64)


@pytest.mark.parametrize(
    "relative",
    ["dbt/models/schema.yml", "dbt/models/schema.yaml", "dbt/seeds/input.csv"],
)
def test_dbt_contract_and_seed_changes_invalidate_input_fingerprint(
    tmp_path: Path, relative: str
) -> None:
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_text("original\n", encoding="utf-8")
    before = _input_fingerprint(tmp_path)
    path.write_text("changed\n", encoding="utf-8")
    assert _input_fingerprint(tmp_path) != before


def test_dbt_generated_user_file_does_not_invalidate_input_fingerprint(
    tmp_path: Path,
) -> None:
    before = _input_fingerprint(tmp_path)
    generated = tmp_path / "dbt/.user.yml"
    generated.parent.mkdir(parents=True)
    generated.write_text("generated: true\n", encoding="utf-8")
    assert _input_fingerprint(tmp_path) == before
