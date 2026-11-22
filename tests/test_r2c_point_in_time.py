from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from pathlib import Path

import pytest

from nexus.data import load_r2c_profile
from nexus.simulation import (
    generate_r2c_profile,
    iter_r2c_dataset_rows,
    iter_r2c_demand_snapshot,
    iter_r2c_lead_time_label_snapshot,
)

ROOT = Path(__file__).resolve().parents[1]


def test_lead_time_snapshot_masks_labels_until_they_are_available(tmp_path: Path) -> None:
    config = load_r2c_profile(ROOT / "configs" / "profiles" / "smoke.yaml")
    generated = generate_r2c_profile(tmp_path / "profile", config=config)
    cutoff_day = config.end_date - timedelta(days=config.holdout_days // 2)
    cutoff = datetime.combine(cutoff_day, time(23, 59), tzinfo=UTC)
    raw = {
        row["order_id"]: row
        for row in iter_r2c_dataset_rows(
            generated.output_dir,
            dataset="representative.lead_time_history",
        )
        if row["ordered_available_at"] <= cutoff
    }
    complete_rows = [row for row in raw.values() if row["status"] == "complete"]
    assert complete_rows
    assert all(row["completed_at"] > row["ordered_at"] for row in complete_rows)
    assert all(row["actual_lead_time_days"] > 0 for row in complete_rows)
    assert all(
        row["label_available_at"] <= generated.manifest.generated_through for row in complete_rows
    )
    snapshot = {
        row["order_id"]: row
        for row in iter_r2c_lead_time_label_snapshot(
            generated.output_dir,
            planning_cutoff=cutoff,
        )
    }

    mature = [
        order_id
        for order_id, row in raw.items()
        if row["label_available_at"] is not None and row["label_available_at"] <= cutoff
    ]
    immature = [order_id for order_id in raw if order_id not in mature]
    assert mature and immature
    assert all(snapshot[order_id]["actual_lead_time_days"] is not None for order_id in mature)
    assert all(snapshot[order_id]["actual_lead_time_days"] is None for order_id in immature)
    assert all(snapshot[order_id]["status"] == "right_censored" for order_id in immature)
    assert all(snapshot[order_id]["received_qty"] == 0 for order_id in immature)
    assert all(snapshot[order_id]["receipt_count"] == 0 for order_id in immature)
    assert all(snapshot[order_id]["delay_days"] is None for order_id in immature)
    assert all(snapshot[order_id]["is_late"] is None for order_id in immature)


def test_lead_time_label_is_visible_at_its_exact_availability_boundary(tmp_path: Path) -> None:
    config = load_r2c_profile(ROOT / "configs" / "profiles" / "smoke.yaml")
    generated = generate_r2c_profile(tmp_path / "profile", config=config)
    complete = next(
        row
        for row in iter_r2c_dataset_rows(
            generated.output_dir,
            dataset="representative.lead_time_history",
        )
        if row["label_available_at"] is not None
    )
    boundary = complete["label_available_at"]
    assert isinstance(boundary, datetime)
    before = {
        row["order_id"]: row
        for row in iter_r2c_lead_time_label_snapshot(
            generated.output_dir,
            planning_cutoff=boundary - timedelta(microseconds=1),
        )
    }[complete["order_id"]]
    at_boundary = {
        row["order_id"]: row
        for row in iter_r2c_lead_time_label_snapshot(
            generated.output_dir,
            planning_cutoff=boundary,
        )
    }[complete["order_id"]]
    assert before["actual_lead_time_days"] is None
    assert at_boundary["actual_lead_time_days"] == complete["actual_lead_time_days"]


def test_demand_snapshot_excludes_late_arrivals_until_available(tmp_path: Path) -> None:
    config = load_r2c_profile(ROOT / "configs" / "profiles" / "smoke.yaml")
    generated = generate_r2c_profile(tmp_path / "profile", config=config)
    cutoff_day = config.end_date - timedelta(days=config.holdout_days // 2)
    cutoff = datetime.combine(cutoff_day, time(23, 59), tzinfo=UTC)
    raw = list(
        iter_r2c_dataset_rows(
            generated.output_dir,
            dataset="representative.demand_history",
        )
    )
    snapshot = list(iter_r2c_demand_snapshot(generated.output_dir, planning_cutoff=cutoff))
    assert snapshot == [row for row in raw if row["available_at"] <= cutoff]
    assert any(row["demand_date"] <= cutoff.date() and row["available_at"] > cutoff for row in raw)


def test_snapshot_rejects_a_naive_cutoff(tmp_path: Path) -> None:
    config = load_r2c_profile(ROOT / "configs" / "profiles" / "smoke.yaml")
    generated = generate_r2c_profile(tmp_path / "profile", config=config)
    with pytest.raises(ValueError, match="explicit timezone"):
        tuple(
            iter_r2c_lead_time_label_snapshot(
                generated.output_dir,
                planning_cutoff=datetime(2026, 1, 1),
            )
        )
