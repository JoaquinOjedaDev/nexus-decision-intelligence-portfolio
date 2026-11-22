from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from nexus.modeling.gold_adapter import R3DemandObservation
from nexus.modeling.r3_contracts import load_r3a_config
from nexus.modeling.r3_features import (
    R3FeatureError,
    build_point_in_time_features,
    feature_artifact_rows,
)


def observation(
    day: int,
    quantity: int,
    *,
    available_at: datetime | None = None,
) -> R3DemandObservation:
    demand_date = date(2026, 6, day)
    return R3DemandObservation(
        demand_history_key=f"{demand_date.isoformat()}::SKU-001::LOC-01",
        demand_date=demand_date,
        sku_location_key="SKU-001::LOC-01",
        sku_id="SKU-001",
        location_id="LOC-01",
        demand_qty=quantity,
        fulfillment_record_count=1,
        latest_available_at=available_at or datetime(2026, 6, day + 1, 3, tzinfo=UTC),
    )


def history() -> list[R3DemandObservation]:
    return [observation(day, day) for day in range(1, 22)]


def test_features_are_rebuilt_inside_each_origin_without_labels() -> None:
    rows = build_point_in_time_features(history(), config=load_r3a_config())

    assert len(rows) == 3 * 7
    first = rows[0]
    assert first.origin_name == "cold_start_probe"
    assert first.history_count == 7
    assert first.lag_1 == 7
    assert first.lag_7 == 1
    assert first.lag_14 is None
    assert first.seasonal_target_lag_7 == 1
    assert first.rolling_mean_7 is not None
    assert first.max_feature_available_at == datetime(2026, 6, 8, 3, tzinfo=UTC)
    assert first.max_feature_available_at <= first.issue_time
    assert "label" not in feature_artifact_rows(rows)[0]
    assert "demand_qty" not in feature_artifact_rows(rows)[0]


def test_future_and_old_dated_late_arrivals_cannot_change_an_earlier_origin() -> None:
    config = load_r3a_config()
    without_day_seven = [row for row in history() if row.demand_date != date(2026, 6, 7)]
    base = build_point_in_time_features(without_day_seven, config=config)
    late_old_date = observation(
        7,
        999,
        available_at=datetime(2026, 6, 9, 3, tzinfo=UTC),
    )
    future = observation(22, 999)
    changed = build_point_in_time_features(
        [*without_day_seven, late_old_date, future],
        config=config,
    )

    original_first = [row for row in base if row.origin_name == "cold_start_probe"]
    changed_first = [row for row in changed if row.origin_name == "cold_start_probe"]
    assert feature_artifact_rows(original_first) == feature_artifact_rows(changed_first)


def test_feature_output_is_input_order_independent() -> None:
    config = load_r3a_config()
    ordered = build_point_in_time_features(history(), config=config)
    reversed_rows = build_point_in_time_features(reversed(history()), config=config)
    assert feature_artifact_rows(ordered) == feature_artifact_rows(reversed_rows)


def test_duplicate_grain_date_fails_closed() -> None:
    rows = history()
    with pytest.raises(R3FeatureError, match="unique"):
        build_point_in_time_features([*rows, rows[0]], config=load_r3a_config())


def test_feature_row_rejects_evidence_after_issue_time() -> None:
    rows = history()
    rows[6] = observation(
        7,
        7,
        available_at=datetime(2026, 6, 8, 4, tzinfo=UTC) + timedelta(seconds=1),
    )
    features = build_point_in_time_features(rows, config=load_r3a_config())
    first = next(row for row in features if row.origin_name == "cold_start_probe")
    assert first.history_count == 6
    assert first.lag_1 is None
