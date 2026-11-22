"""Connected runtime replay, point-in-time sensitivity and fail-closed evidence."""

from __future__ import annotations

import copy
import csv
import json
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from nexus.contracts import RecommendationManifest
from nexus.decisioning.r4a_contracts import R4AConfig
from nexus.decisioning.r4a_optimizer import solve_constrained_policy
from nexus.modeling.r3_artifacts import canonical_json_sha256
from nexus.portfolio import runtime_pipeline as pipeline
from nexus.portfolio.runtime_contracts import RuntimeForecastAdapter, load_runtime_policy
from nexus.storage import file_sha256

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def runtime(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    directory = tmp_path_factory.mktemp("connected-runtime")
    captured: dict[str, Any] = {"directory": directory}
    original = pipeline.materialize_runtime_forecast

    def capture(*args: Any, **kwargs: Any) -> pipeline.RuntimeForecastArtifacts:
        result = original(*args, **kwargs)
        captured["forecasts"] = result
        captured["rebuild"] = args[0]
        return result

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv(
            "PATH", str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]
        )
        monkeypatch.setattr(pipeline, "materialize_runtime_forecast", capture)
        captured["seed"] = pipeline.build_runtime_seed(ROOT, directory)
    return captured


@pytest.fixture(autouse=True)
def executable_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PATH", str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"])


def _changed_source(source: Path, target: Path, *, demand_date: str, delta: int = 3) -> Path:
    shutil.copytree(source, target)
    path = target / "fulfillment_outcomes.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames
        rows = list(reader)
    assert fields
    changed = False
    for row in rows:
        if (
            row["sku_id"] == "SKU-001"
            and row["location_id"] == "DC-ARG-01"
            and row["demand_date"] == demand_date
        ):
            row["demand_qty"] = str(int(row["demand_qty"]) + delta)
            changed = True
    assert changed
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    manifest_path = target / "source-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for item in manifest["sources"]:
        if item["file_name"] == path.name:
            item["content_sha256"] = file_sha256(path)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return target


def test_runtime_connects_true_gold_keys_inventory_forecasts_and_manifest(
    runtime: dict[str, Any],
) -> None:
    seed = runtime["seed"]
    forecasts = runtime["forecasts"]
    assert seed["executable"] is True
    assert seed["metrics"]["solver_status"] == "OPTIMAL"
    assert seed["metrics"]["dbt_status"] == "PASS"
    assert seed["metadata"]["pipeline_mode"] == "connected_runtime_baseline"
    assert seed["metadata"]["learned_model_training_claimed"] is False
    assert seed["metadata"]["historical_challenger_status"] == "CHALLENGER_LOST"
    inventory = {(row["sku_id"], row["location_id"]): row for row in forecasts.inventory}
    assert len(seed["recommendations"]) == len(inventory) == 4
    for item in seed["recommendations"]:
        gold = inventory[(item["sku_id"], item["location_id"])]
        assert item["initial_inventory"] == gold["on_hand_qty"]
        assert item["on_order_excluded"] == gold["on_order_qty"]
        assert item["supplier_id"] == gold["supplier_id"]
    adapter = forecasts.adapter
    for forecast in seed["forecasts"]:
        assert [point["date"] for point in forecast["points"]] == [
            (adapter.issue_time.date() + timedelta(days=day)).isoformat() for day in range(14)
        ]
        assert all(
            point["q025"] <= point["q10"] <= point["q50"] <= point["q90"] <= point["q975"]
            for point in forecast["points"]
        )
        assert all(
            datetime.fromisoformat(row["available_at"]) <= adapter.issue_time
            for row in forecast["history"]
        )
    manifest = RecommendationManifest.model_validate_json(json.dumps(seed["original_manifest"]))
    assert manifest.fingerprint == seed["manifest_fingerprint"]
    assert manifest.forecast_artifact.content_sha256 == seed["runtime_lineage"]["forecast_sha256"]
    assert adapter.gold_demand_sha256 == seed["runtime_lineage"]["gold_demand_sha256"]
    assert adapter.baseline_uses_engineered_features is False
    config = pipeline.runtime_planning_config(
        forecasts, load_runtime_policy(ROOT / "configs/portfolio/runtime-baseline.yaml")
    )
    assert not hasattr(config, "forecast_authority")


def test_runtime_fresh_replay_is_deterministic(runtime: dict[str, Any], tmp_path: Path) -> None:
    replay = pipeline.build_runtime_seed(ROOT, tmp_path)
    assert replay["manifest_fingerprint"] == runtime["seed"]["manifest_fingerprint"]
    assert replay["runtime_lineage"] == runtime["seed"]["runtime_lineage"]
    assert replay["forecasts"] == runtime["seed"]["forecasts"]
    assert replay["recommendations"] == runtime["seed"]["recommendations"]


def test_visible_source_demand_changes_forecast_and_invalidates_approval(
    runtime: dict[str, Any], tmp_path: Path
) -> None:
    source = _changed_source(
        runtime["directory"] / "source", tmp_path / "source", demand_date="2026-06-23"
    )
    changed = pipeline.build_runtime_seed(ROOT, tmp_path / "evidence", source)
    old = runtime["seed"]
    assert (
        changed["forecasts"][0]["points"][0]["q50"] == old["forecasts"][0]["points"][0]["q50"] + 3
    )
    assert (
        changed["runtime_lineage"]["gold_demand_sha256"]
        != old["runtime_lineage"]["gold_demand_sha256"]
    )
    assert (
        changed["runtime_lineage"]["gold_inventory_sha256"]
        == old["runtime_lineage"]["gold_inventory_sha256"]
    )
    assert changed["manifest_fingerprint"] != old["manifest_fingerprint"]
    assert set(item["id"] for item in changed["recommendations"]).isdisjoint(
        item["id"] for item in old["recommendations"]
    )
    with pytest.raises(pipeline.RuntimePipelineError, match="inputs changed"):
        pipeline.build_runtime_seed(ROOT, runtime["directory"], source)


def test_future_source_changes_do_not_leak_into_early_forecasts(
    runtime: dict[str, Any], tmp_path: Path
) -> None:
    cutoff = datetime(2026, 6, 20, tzinfo=UTC)
    original_source = runtime["directory"] / "source"
    changed_source = _changed_source(
        original_source, tmp_path / "late-source", demand_date="2026-06-28", delta=900
    )
    before = pipeline.build_runtime_seed(
        ROOT, tmp_path / "before", original_source, planning_cutoff=cutoff
    )
    after = pipeline.build_runtime_seed(
        ROOT, tmp_path / "after", changed_source, planning_cutoff=cutoff
    )
    assert (
        before["runtime_lineage"]["gold_demand_sha256"]
        == after["runtime_lineage"]["gold_demand_sha256"]
    )
    assert [item["points"] for item in before["forecasts"]] == [
        item["points"] for item in after["forecasts"]
    ]
    assert [item["input_sha256"] for item in before["forecasts"]] == [
        item["input_sha256"] for item in after["forecasts"]
    ]
    assert before["runtime_lineage"]["feature_sha256"] == after["runtime_lineage"]["feature_sha256"]
    assert (
        before["metadata"]["source_snapshot_sha256"] != after["metadata"]["source_snapshot_sha256"]
    )


def test_missing_join_key_fails_closed(runtime: dict[str, Any]) -> None:
    policy = load_runtime_policy(ROOT / "configs/portfolio/runtime-baseline.yaml")
    incomplete = policy.model_copy(update={"lines": policy.lines[:-1]})
    with pytest.raises(pipeline.RuntimePipelineError, match="keys must match"):
        pipeline.runtime_planning_config(runtime["forecasts"], incomplete)


def test_source_hash_and_directory_independence(runtime: dict[str, Any], tmp_path: Path) -> None:
    source = runtime["directory"] / "source"
    cloned = tmp_path / "source"
    shutil.copytree(source, cloned)
    assert pipeline.runtime_source_fingerprint(source) == pipeline.runtime_source_fingerprint(
        cloned
    )
    with (cloned / "fulfillment_outcomes.csv").open("a", encoding="utf-8") as handle:
        handle.write("forged\n")
    with pytest.raises(pipeline.RuntimePipelineError, match="checksum"):
        pipeline.runtime_source_fingerprint(cloned)


def test_immutable_runtime_artifact_tamper_rejected(
    runtime: dict[str, Any], tmp_path: Path
) -> None:
    shutil.copytree(runtime["directory"], tmp_path / "copied")
    artifact = next(
        item
        for item in runtime["seed"]["artifacts"]
        if item["dataset"] == "runtime.forecast_adapter"
    )
    (tmp_path / "copied" / artifact["path"]).write_text("{}", encoding="utf-8")
    with pytest.raises(pipeline.RuntimePipelineError, match="artifact checksum"):
        pipeline.load_runtime_seed(tmp_path / "copied")


@pytest.mark.parametrize("mutation", ["nonfinite", "unordered", "wrong_date"])
def test_runtime_forecast_contract_rejects_bad_scientific_values(
    runtime: dict[str, Any], mutation: str
) -> None:
    value = runtime["forecasts"].adapter.model_dump(mode="python")
    if mutation == "nonfinite":
        value["lines"][0]["points"][0]["q90"] = float("inf")
    elif mutation == "unordered":
        value["lines"][0]["points"][0]["q10"] = 999.0
    else:
        value["lines"][0]["points"][0]["target_date"] += timedelta(days=1)
    with pytest.raises(ValueError):
        RuntimeForecastAdapter.model_validate(value)


def test_unknown_solver_fallback_cannot_execute(runtime: dict[str, Any]) -> None:
    policy = load_runtime_policy(ROOT / "configs/portfolio/runtime-baseline.yaml")
    config = pipeline.runtime_planning_config(runtime["forecasts"], policy)
    scenarios = pipeline.runtime_scenarios(runtime["forecasts"].adapter, policy, purpose="planning")
    decision = solve_constrained_policy(
        cast(R4AConfig, config), scenarios, inject_failure="unknown"
    )
    assert decision.fallback_used
    assert not pipeline.runtime_decision_executable(decision)


def test_failed_solver_persists_non_executable_receipt(tmp_path: Path) -> None:
    seed = pipeline.build_runtime_seed(ROOT, tmp_path, inject_failure="error")
    assert seed["executable"] is False
    assert seed["original_manifest"] is None
    assert seed["manifest_fingerprint"] is None
    assert seed["recommendations"] == []
    assert seed["metrics"]["solver_status"] == "ERROR"
    assert pipeline.load_runtime_seed(tmp_path) == seed


def test_restart_checks_current_code_without_rerun(
    runtime: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(**kwargs: Any) -> None:
        raise AssertionError("restart verifier cannot rerun dbt or optimization")

    monkeypatch.setattr(pipeline, "run_r2_rebuild", forbidden)
    monkeypatch.setattr(pipeline, "solve_constrained_policy", forbidden)
    assert pipeline.verify_runtime_current(ROOT, runtime["directory"]) == runtime["seed"]
    monkeypatch.setattr(pipeline, "runtime_code_fingerprint", lambda root: "0" * 64)
    with pytest.raises(pipeline.RuntimePipelineError, match="governing code changed"):
        pipeline.verify_runtime_current(ROOT, runtime["directory"])


def test_rehashed_forged_order_is_rejected(runtime: dict[str, Any], tmp_path: Path) -> None:
    copied = tmp_path / "evidence"
    shutil.copytree(runtime["directory"], copied)
    seed = copy.deepcopy(runtime["seed"])
    seed["recommendations"][0]["recommended_quantity"] += 1
    seed["seed_hash"] = canonical_json_sha256(
        {key: value for key, value in seed.items() if key != "seed_hash"}
    )
    (copied / pipeline.RUNTIME_SEED_FILENAME).write_text(
        json.dumps({"payload": seed, "payload_sha256": canonical_json_sha256(seed)}),
        encoding="utf-8",
    )
    with pytest.raises(pipeline.RuntimePipelineError, match="quantities differ"):
        pipeline.load_runtime_seed(copied)


@pytest.mark.parametrize("interruption", ["fsync", "link"])
def test_atomic_seed_interruption_never_publishes_partial_canonical(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interruption: str
) -> None:
    envelope = {"payload": {"test": "complete"}, "payload_sha256": "a" * 64}

    def interrupted(*args: Any, **kwargs: Any) -> None:
        raise InterruptedError("injected interruption before publication")

    with monkeypatch.context() as patch:
        patch.setattr(pipeline.os, interruption, interrupted)
        with pytest.raises(InterruptedError):
            pipeline._publish_runtime_seed(tmp_path, envelope)
    assert not (tmp_path / pipeline.RUNTIME_SEED_FILENAME).exists()
    assert not list(tmp_path.glob(".runtime-seed-*.tmp"))
    pipeline._publish_runtime_seed(tmp_path, envelope)
    assert json.loads((tmp_path / pipeline.RUNTIME_SEED_FILENAME).read_bytes()) == envelope


def test_atomic_seed_retry_ignores_crash_orphan_and_preserves_existing_bytes(
    tmp_path: Path,
) -> None:
    orphan = tmp_path / ".runtime-seed-crashed-writer.tmp"
    orphan.write_bytes(b'{"partial":')
    envelope = {"payload": {"test": "complete"}}
    pipeline._publish_runtime_seed(tmp_path, envelope)
    canonical = tmp_path / pipeline.RUNTIME_SEED_FILENAME
    before = canonical.read_bytes()
    pipeline._publish_runtime_seed(tmp_path, envelope)
    assert canonical.read_bytes() == before
    with pytest.raises(pipeline.RuntimePipelineError, match="existing bytes preserved"):
        pipeline._publish_runtime_seed(tmp_path, {"payload": {"test": "different"}})
    assert canonical.read_bytes() == before
    assert orphan.read_bytes() == b'{"partial":'
    assert list(tmp_path.glob(".runtime-seed-*.tmp")) == [orphan]


def test_competing_atomic_seed_publishers_cannot_clobber_each_other(tmp_path: Path) -> None:
    def publish(number: int) -> tuple[int, bool]:
        try:
            pipeline._publish_runtime_seed(tmp_path, {"writer": number})
            return number, True
        except pipeline.RuntimePipelineError:
            return number, False

    with ThreadPoolExecutor(max_workers=2) as executor:
        attempts = list(executor.map(publish, (1, 2)))
    winners = [number for number, succeeded in attempts if succeeded]
    assert len(winners) == 1
    assert json.loads((tmp_path / pipeline.RUNTIME_SEED_FILENAME).read_bytes()) == {
        "writer": winners[0]
    }
    assert not list(tmp_path.glob(".runtime-seed-*.tmp"))


def test_real_builder_retry_after_interrupted_publication_completes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def interrupted(root: Path, envelope: dict[str, Any]) -> None:
        raise InterruptedError("injected publication crash after scientific build")

    with monkeypatch.context() as patch:
        patch.setattr(pipeline, "_publish_runtime_seed", interrupted)
        with pytest.raises(InterruptedError):
            pipeline.build_runtime_seed(ROOT, tmp_path)
    assert not (tmp_path / pipeline.RUNTIME_SEED_FILENAME).exists()
    recovered = pipeline.build_runtime_seed(ROOT, tmp_path)
    assert recovered["executable"] is True
    assert pipeline.verify_runtime_current(ROOT, tmp_path) == recovered
