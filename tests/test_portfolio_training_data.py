"""Mature-label boundary tests plus one separately scoped real dbt integration."""

from __future__ import annotations

import json
import os
import shutil
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from jsonschema import Draft202012Validator

from nexus.data.r2_models import R2RebuildResult, R2SourceManifest
from nexus.data.r2_pipeline import land_r2_sources
from nexus.modeling.r3_artifacts import canonical_json_sha256
from nexus.portfolio.runtime_contracts import load_runtime_policy
from nexus.portfolio.training_data import (
    FinalizationManifest,
    TrainingDataError,
    build_training_snapshot,
    pit_history,
    validate_training_snapshot,
)
from nexus.portfolio.training_fixture import TrainingFixture, generate_training_fixture
from nexus.r2 import run_r2_rebuild
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import file_sha256, write_immutable_parquet

ROOT = Path(__file__).resolve().parents[1]


def _unit_gold(source: Path, target: Path) -> R2RebuildResult:
    """Tiny explicit Gold fixture for adapter unit tests; NOT a claimed dbt run."""
    landing = land_r2_sources(source_dir=source, output_dir=target / "landing")
    demand_rows: list[dict[str, Any]] = []
    fulfillment_rows: list[dict[str, Any]] = []
    for row in landing.fulfillment_outcomes:
        key = f"{row.sku_id}::{row.location_id}"
        demand_rows.append(
            {
                "demand_history_key": f"{row.demand_date.isoformat()}::{key}",
                "demand_date": row.demand_date.isoformat(),
                "sku_location_key": key,
                "sku_id": row.sku_id,
                "location_id": row.location_id,
                "demand_qty": row.demand_qty,
                "fulfillment_record_count": 1,
                "latest_available_at": row.available_at.isoformat().replace("+00:00", "Z"),
            }
        )
        fulfillment_rows.append(
            {
                **row.model_dump(mode="json"),
                "sku_location_key": key,
                "unfulfilled_qty": row.demand_qty - row.fulfilled_qty,
                "fill_rate": row.fulfilled_qty / row.demand_qty if row.demand_qty else 1.0,
            }
        )
    demand = write_immutable_parquet(
        target / "gold", dataset="gold.r2_demand_history", rows=demand_rows
    )
    fulfillment = write_immutable_parquet(
        target / "gold", dataset="gold.r2_fulfillment_outcomes", rows=fulfillment_rows
    )
    return R2RebuildResult(
        planning_cutoff=landing.source_manifest.planning_cutoff,
        source_manifest_sha256=landing.source_manifest_sha256,
        source_manifest_file_sha256=landing.source_manifest_file_sha256,
        semantic_manifest_sha256=canonical_json_sha256(
            [demand.content_sha256, fulfillment.content_sha256]
        ),
        landing=landing,
        gold_artifacts=(demand, fulfillment),
        gold_row_counts={item.dataset: item.row_count for item in (demand, fulfillment)},
        quality_counts={},
        summary_path=target / "unit-only.json",
        database_path=target / "unit-only.duckdb",
        duration_seconds=0,
    )


@pytest.fixture(scope="module")
def learning(tmp_path_factory: pytest.TempPathFactory) -> tuple[TrainingFixture, R2RebuildResult]:
    root = tmp_path_factory.mktemp("learning-data")
    fixture = generate_training_fixture(root / "source")
    return fixture, _unit_gold(fixture.source_dir, root / "unit-gold")


def _snapshot(fixture: TrainingFixture, rebuild: R2RebuildResult, **kwargs: Any):
    return build_training_snapshot(
        rebuild,
        source_dir=fixture.source_dir,
        finalization_path=fixture.finalization_path,
        expected_finalization_sha256=fixture.finalization_sha256,
        **kwargs,
    )


def test_learning_fixture_explicit_contract_and_deterministic_replay(
    learning: tuple[TrainingFixture, R2RebuildResult],
    tmp_path: Path,
) -> None:
    fixture, _ = learning
    replay = generate_training_fixture(tmp_path / "source")
    assert replay.source_snapshot_sha256 == fixture.source_snapshot_sha256
    assert replay.finalization_sha256 == fixture.finalization_sha256
    assert replay.planning_cutoff == datetime(2026, 6, 4, 4, tzinfo=UTC)
    manifest = json.loads((replay.source_dir / "source-manifest.json").read_text())
    assert manifest["generator"] == "nexus-r2-learning-supply-network"
    assert manifest["profile"] == "learning" and manifest["days"] == 365
    schema = json.loads((ROOT / "contracts/schemas/r2-source-manifest.schema.json").read_text())
    Draft202012Validator(schema).validate(manifest)
    assert R2SourceManifest.model_validate_json(json.dumps(manifest)).profile == "learning"
    assert generate_training_fixture(replay.source_dir) == replay
    with pytest.raises(ValueError, match="conflicts"):
        generate_training_fixture(replay.source_dir, seed=20260719)


def test_snapshot_counts_real_censoring_and_binds_all_artifacts(
    learning: tuple[TrainingFixture, R2RebuildResult],
) -> None:
    fixture, rebuild = learning
    snapshot = _snapshot(fixture, rebuild)
    assert snapshot.status == "READY"
    assert len(snapshot.labels) >= 500
    assert len(snapshot.labels) + sum(snapshot.excluded_counts.values()) == 4 * 365
    actual_unfulfilled_days = sum(
        row.fulfilled_qty < row.demand_qty for row in rebuild.landing.fulfillment_outcomes
    )
    assert snapshot.excluded_counts.get("unfulfilled_censored", 0) == actual_unfulfilled_days
    assert snapshot.source_snapshot_sha256 == fixture.source_snapshot_sha256
    assert snapshot.finalization_sha256 == fixture.finalization_sha256
    assert (
        snapshot.gold_demand_sha256
        == rebuild.gold_artifact("gold.r2_demand_history").content_sha256
    )
    payload = snapshot.to_dict()
    assert snapshot.snapshot_sha256 == canonical_json_sha256(
        {key: value for key, value in payload.items() if key != "snapshot_sha256"}
    )
    assert _snapshot(fixture, rebuild).snapshot_sha256 == snapshot.snapshot_sha256


def test_balanced_receipts_preserve_demand_and_bound_actual_inventory(
    learning: tuple[TrainingFixture, R2RebuildResult],
) -> None:
    fixture, rebuild = learning
    # This is the original v1 fulfillment-file identity, not a newly fitted target.
    assert file_sha256(fixture.source_dir / "fulfillment_outcomes.csv") == (
        "27a85ee36661211f37ecf1b809135ea5539f2cdce99b1531f6ff70b7218bb8f1"
    )
    recipe = json.loads((fixture.source_dir / "learning-recipe.json").read_text())
    assert recipe["generator_version"] == "1.1.0"
    assert "no future realized demand" in recipe["replenishment_policy"]
    assert fixture.source_snapshot_sha256 != (
        "6c3a51bd43a107dd329ab4cf2c4546376d4e7a381aac3dc1cc21e0a1da6ca207"
    )
    final_stock = {}
    landing = rebuild.landing
    for opening in landing.opening_inventory:
        key = opening.sku_id, opening.location_id
        receipts = {
            row.received_at.date(): row.received_qty
            for row in landing.receipts
            if (row.sku_id, row.location_id) == key
        }
        outcomes = sorted(
            (row for row in landing.fulfillment_outcomes if (row.sku_id, row.location_id) == key),
            key=lambda row: row.demand_date,
        )
        assert len(outcomes) == 365
        stock = opening.on_hand_qty
        for row in outcomes:
            stock += receipts.get(row.demand_date, 0)
            assert row.fulfilled_qty == row.demand_qty <= stock
            stock -= row.fulfilled_qty
            assert 0 <= stock <= 150  # Daily physical inventory, not a solver feasibility mock.
        assert stock == opening.on_hand_qty + sum(receipts.values()) - sum(
            row.fulfilled_qty for row in outcomes
        )
        final_stock[key] = stock
    assert list(final_stock.values()) == [41, 43, 81, 60]
    policy = load_runtime_policy(ROOT / "configs/portfolio/runtime-baseline.yaml")
    for warehouse in policy.location_capacities:
        final_volume = sum(
            final_stock[(line.sku_id, line.location_id)] * line.unit_volume
            for line in policy.lines
            if line.location_id == warehouse.location_id
        )
        assert final_volume <= warehouse.storage_capacity_volume


def test_original_28day_fixture_abstains_without_inferred_finality(tmp_path: Path) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    rebuild = _unit_gold(fixture.source_dir, tmp_path / "gold")
    snapshot = build_training_snapshot(rebuild, source_dir=fixture.source_dir)
    assert snapshot.status == "ABSTAIN_INSUFFICIENT_EVIDENCE"
    assert snapshot.labels == ()
    assert snapshot.excluded_counts == {"missing_finalization": 107, "unfulfilled_censored": 5}
    assert "no_explicit_correction_closure" in snapshot.reason_codes


def test_arrival_and_closure_are_both_filtered_at_each_historical_issue(
    learning: tuple[TrainingFixture, R2RebuildResult],
) -> None:
    fixture, rebuild = learning
    snapshot = _snapshot(fixture, rebuild)
    assert pit_history(snapshot, datetime(2025, 6, 4, 23, tzinfo=UTC)) == ()
    issue = datetime(2025, 6, 7, 4, tzinfo=UTC)
    history = pit_history(snapshot, issue)
    assert history and all(row.latest_available_at <= issue for row in history)
    assert max(row.demand_date for row in history).isoformat() == "2025-06-03"
    early = _snapshot(fixture, rebuild, as_of=issue)
    assert early.status == "ABSTAIN_INSUFFICIENT_EVIDENCE"
    assert early.excluded_counts["future_or_incomplete"] > 0
    assert early.excluded_counts["correction_window_open"] > 0
    assert len(early.labels) < len(snapshot.labels)
    with pytest.raises(TrainingDataError, match="exceeds"):
        pit_history(snapshot, snapshot.as_of + timedelta(seconds=1))
    with pytest.raises(TrainingDataError):
        pit_history(replace(snapshot, labels=snapshot.labels[1:]), issue)


@pytest.mark.parametrize("field", ["source", "labels", "readiness", "maturity", "nonfinite"])
def test_public_snapshot_verifier_rejects_mutation_before_fit(
    learning: tuple[TrainingFixture, R2RebuildResult],
    field: str,
) -> None:
    fixture, rebuild = learning
    original = _snapshot(fixture, rebuild)
    validate_training_snapshot(original)
    if field == "source":
        changed = replace(original, source_snapshot_sha256="e" * 64)
    elif field == "labels":
        changed = replace(original, labels=original.labels[:-1])
    elif field == "readiness":
        changed = replace(original, status="ABSTAIN_INSUFFICIENT_EVIDENCE")
    elif field == "maturity":
        changed = replace(original, as_of=datetime(2025, 6, 2, tzinfo=UTC))
    else:
        row = original.labels[0].model_copy(update={"quantity": float("nan")})
        changed = replace(original, labels=(row, *original.labels[1:]))
    with pytest.raises(TrainingDataError):
        validate_training_snapshot(changed)


def test_generator_profile_pair_is_identical_in_python_and_json_contracts(
    learning: tuple[TrainingFixture, R2RebuildResult],
) -> None:
    fixture, _ = learning
    payload = json.loads((fixture.source_dir / "source-manifest.json").read_text())
    payload["profile"] = "smoke"
    with pytest.raises(ValueError, match="declared generator"):
        R2SourceManifest.model_validate_json(json.dumps(payload))
    schema = json.loads((ROOT / "contracts/schemas/r2-source-manifest.schema.json").read_text())
    assert list(Draft202012Validator(schema).iter_errors(payload))


@pytest.mark.parametrize("change", ["missing_grain", "duplicate_id", "forged_quantity"])
def test_gold_grain_and_business_source_mismatch_rejected(
    learning: tuple[TrainingFixture, R2RebuildResult],
    tmp_path: Path,
    change: str,
) -> None:
    fixture, rebuild = learning
    original = rebuild.gold_artifact("gold.r2_fulfillment_outcomes")
    rows = pq.read_table(original.path).to_pylist()
    if change == "missing_grain":
        rows.pop()
    elif change == "duplicate_id":
        rows.append(dict(rows[0]))
    else:
        rows[0]["demand_qty"] += 1
    changed = write_immutable_parquet(tmp_path, dataset=original.dataset, rows=rows)
    replacement = replace(
        rebuild, gold_artifacts=(rebuild.gold_artifact("gold.r2_demand_history"), changed)
    )
    with pytest.raises(TrainingDataError):
        _snapshot(fixture, replacement)


def test_finalization_checksum_source_and_duplicate_keys_fail_closed(
    learning: tuple[TrainingFixture, R2RebuildResult],
    tmp_path: Path,
) -> None:
    fixture, rebuild = learning
    with pytest.raises(TrainingDataError, match="checksum"):
        build_training_snapshot(
            rebuild,
            source_dir=fixture.source_dir,
            finalization_path=fixture.finalization_path,
            expected_finalization_sha256="0" * 64,
        )
    payload = json.loads(fixture.finalization_path.read_text())
    payload["source_snapshot_sha256"] = "a" * 64
    path = tmp_path / "wrong-source.json"
    path.write_text(json.dumps(payload))
    with pytest.raises(TrainingDataError, match="source snapshot"):
        build_training_snapshot(rebuild, source_dir=fixture.source_dir, finalization_path=path)
    payload["labels"].append(payload["labels"][0])
    with pytest.raises(ValueError, match="unique"):
        FinalizationManifest.model_validate_json(json.dumps(payload))


def test_open_finalization_is_excluded_not_treated_as_closed(
    learning: tuple[TrainingFixture, R2RebuildResult],
    tmp_path: Path,
) -> None:
    fixture, rebuild = learning
    payload = json.loads(fixture.finalization_path.read_text())
    payload["labels"][0]["correction_closed"] = False
    path = tmp_path / "open-label.json"
    path.write_text(json.dumps(payload))
    snapshot = build_training_snapshot(
        rebuild,
        source_dir=fixture.source_dir,
        finalization_path=path,
        expected_finalization_sha256=file_sha256(path),
    )
    assert snapshot.excluded_counts["correction_window_open"] == 1
    assert len(snapshot.labels) == len(_snapshot(fixture, rebuild).labels) - 1
    assert snapshot.snapshot_sha256 != _snapshot(fixture, rebuild).snapshot_sha256


def test_gold_file_tamper_and_unmatched_source_are_rejected(
    learning: tuple[TrainingFixture, R2RebuildResult],
    tmp_path: Path,
) -> None:
    fixture, rebuild = learning
    original = rebuild.gold_artifact("gold.r2_fulfillment_outcomes")
    copied = tmp_path / "tampered.parquet"
    shutil.copyfile(original.path, copied)
    rows = pq.read_table(copied).to_pylist()
    rows[0]["fulfilled_qty"] = 0
    import pyarrow as pa

    pq.write_table(pa.Table.from_pylist(rows), copied)
    changed = replace(original, path=copied)
    with pytest.raises(TrainingDataError, match="checksum"):
        _snapshot(
            fixture,
            replace(
                rebuild, gold_artifacts=(rebuild.gold_artifact("gold.r2_demand_history"), changed)
            ),
        )
    other = generate_training_fixture(tmp_path / "other", seed=20260719)
    with pytest.raises(TrainingDataError, match="source package"):
        build_training_snapshot(rebuild, source_dir=other.source_dir)


def test_learning_source_real_dbt_and_label_bridge(
    learning: tuple[TrainingFixture, R2RebuildResult],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _ = learning
    monkeypatch.setenv("PATH", str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"])
    rebuild = run_r2_rebuild(
        project_root=ROOT,
        source_dir=fixture.source_dir,
        artifact_dir=tmp_path / "real-dbt",
        planning_cutoff=fixture.planning_cutoff,
    )
    snapshot = _snapshot(fixture, rebuild)
    assert snapshot.status == "READY"
    assert rebuild.gold_row_counts["gold.r2_demand_history"] == 1460
    assert len(snapshot.labels) + sum(snapshot.excluded_counts.values()) == 1460
