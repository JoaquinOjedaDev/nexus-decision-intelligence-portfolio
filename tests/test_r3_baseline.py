from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from nexus.modeling.r3_baseline import (
    forecast_croston_sba,
    forecast_probabilistic_seasonal_naive,
    join_matured_labels,
    rolling_croston_sba,
    rolling_probabilistic_seasonal_naive,
)
from nexus.modeling.seasonal_naive import DailyDemandObservation

SKU = "SKU-INTERMITTENT"
LOCATION = "DC-01"
ISSUE_TIME = datetime(2026, 7, 22, 4, tzinfo=UTC)


def observations(
    quantities: tuple[int, ...],
    *,
    start: date = date(2026, 7, 1),
) -> tuple[DailyDemandObservation, ...]:
    return tuple(
        DailyDemandObservation(
            sku_id=SKU,
            location_id=LOCATION,
            demand_date=start + timedelta(days=offset),
            quantity=quantity,
            available_at=datetime.combine(
                start + timedelta(days=offset + 1), datetime.min.time(), tzinfo=UTC
            ),
        )
        for offset, quantity in enumerate(quantities)
    )


def test_seasonal_median_delegates_to_existing_baseline_and_intervals_are_monotonic() -> None:
    first_week = (2, 4, 6, 8, 10, 12, 14)
    latest_week = (1, 6, 5, 12, 9, 18, 14)

    result = forecast_probabilistic_seasonal_naive(
        observations(first_week + latest_week),
        sku_id=SKU,
        location_id=LOCATION,
        issue_time=datetime(2026, 7, 15, 4, tzinfo=UTC),
        horizon_days=7,
    )

    assert tuple(point.q50 for point in result.points) == tuple(map(Decimal, latest_week))
    assert result.residuals == tuple(
        Decimal(latest - first) for first, latest in zip(first_week, latest_week, strict=True)
    )
    assert result.residual_count == 7
    assert result.median_forecast_sha256 is not None
    assert all(
        point.q025 <= point.q10 <= point.q50 <= point.q90 <= point.q975 for point in result.points
    )
    assert all(point.q025 >= 0 for point in result.points)


def test_no_knowable_seasonal_residuals_produce_degenerate_median_intervals() -> None:
    result = forecast_probabilistic_seasonal_naive(
        observations((0, 1, 0, 2, 0, 3, 0), start=date(2026, 7, 15)),
        sku_id=SKU,
        location_id=LOCATION,
        issue_time=ISSUE_TIME,
        horizon_days=7,
    )

    assert result.residual_count == 0
    assert all(len(set(point.quantiles)) == 1 for point in result.points)


def test_seasonal_baseline_blocks_issue_day_future_and_old_dated_late_arrivals() -> None:
    history = observations(tuple(range(1, 15)))
    baseline = forecast_probabilistic_seasonal_naive(
        history,
        sku_id=SKU,
        location_id=LOCATION,
        issue_time=ISSUE_TIME,
        horizon_days=3,
    )
    old_late = DailyDemandObservation(
        sku_id=SKU,
        location_id=LOCATION,
        demand_date=date(2026, 7, 1),
        quantity=9_999,
        available_at=ISSUE_TIME + timedelta(seconds=1),
    )
    issue_day = DailyDemandObservation(
        sku_id=SKU,
        location_id=LOCATION,
        demand_date=ISSUE_TIME.date(),
        quantity=9_999,
        available_at=ISSUE_TIME,
    )

    protected = forecast_probabilistic_seasonal_naive(
        (issue_day, old_late, *reversed(history)),
        sku_id=SKU,
        location_id=LOCATION,
        issue_time=ISSUE_TIME,
        horizon_days=3,
    )

    assert protected == baseline
    assert protected.forecast_sha256 == baseline.forecast_sha256


def test_quantile_clipping_preserves_nonnegativity_and_order_for_negative_residuals() -> None:
    result = forecast_probabilistic_seasonal_naive(
        observations((20, 20, 20, 20, 20, 20, 20, 0, 0, 0, 1, 1, 1, 1)),
        sku_id=SKU,
        location_id=LOCATION,
        issue_time=datetime(2026, 7, 15, 4, tzinfo=UTC),
        horizon_days=7,
    )

    for point in result.points:
        assert Decimal(0) <= point.q025 <= point.q10 <= point.q50 <= point.q90 <= point.q975


def test_croston_sba_has_named_shared_contract_on_intermittent_history() -> None:
    result = forecast_croston_sba(
        observations((0, 4, 0, 0, 8)),
        sku_id=SKU,
        location_id=LOCATION,
        issue_time=datetime(2026, 7, 7, 4, tzinfo=UTC),
        horizon_days=4,
        alpha=Decimal("0.20"),
    )

    expected_median = Decimal("0.9") * (Decimal("4.8") / Decimal("2.2"))
    assert result.model_name == "croston_sba_empirical_intervals"
    assert all(point.q50 == expected_median for point in result.points)
    assert result.residual_count == 4
    assert all(point.median_source == "croston_sba_state" for point in result.points)
    assert all(
        point.q025 <= point.q10 <= point.q50 <= point.q90 <= point.q975 for point in result.points
    )


def test_croston_is_permutation_invariant_and_rejects_late_arrival_leakage() -> None:
    history = observations((0, 0, 5, 0, 0, 0, 9, 0, 0, 4))
    arguments = {
        "sku_id": SKU,
        "location_id": LOCATION,
        "issue_time": datetime(2026, 7, 12, 4, tzinfo=UTC),
        "horizon_days": 5,
    }
    baseline = forecast_croston_sba(history, **arguments)  # type: ignore[arg-type]
    late = DailyDemandObservation(
        sku_id=SKU,
        location_id=LOCATION,
        demand_date=date(2026, 7, 3),
        quantity=999,
        available_at=datetime(2026, 7, 12, 4, 0, 1, tzinfo=UTC),
    )

    protected = forecast_croston_sba((late, *reversed(history)), **arguments)  # type: ignore[arg-type]

    assert protected == baseline
    assert protected.input_sha256 == baseline.input_sha256


def test_croston_all_zero_history_uses_explicit_cold_start_fallback() -> None:
    result = forecast_croston_sba(
        observations((0, 0, 0)),
        sku_id=SKU,
        location_id=LOCATION,
        issue_time=datetime(2026, 7, 5, 4, tzinfo=UTC),
        horizon_days=2,
        cold_start_daily_demand=1,
    )

    assert all(point.quantiles == (Decimal(1),) * 5 for point in result.points)
    assert all(point.median_source == "cold_start_fallback" for point in result.points)


@pytest.mark.parametrize("alpha", [Decimal(0), Decimal("1.01"), -1])
def test_croston_rejects_invalid_smoothing_parameter(alpha: Decimal | int) -> None:
    with pytest.raises(ValueError, match="alpha"):
        forecast_croston_sba(
            (),
            sku_id=SKU,
            location_id=LOCATION,
            issue_time=ISSUE_TIME,
            horizon_days=1,
            alpha=alpha,
        )


def test_rolling_origins_are_sorted_and_each_has_its_own_point_in_time_view() -> None:
    history = observations(tuple(range(1, 22)), start=date(2026, 6, 1))
    early = datetime(2026, 6, 15, 4, tzinfo=UTC)
    late = datetime(2026, 6, 22, 4, tzinfo=UTC)

    seasonal = rolling_probabilistic_seasonal_naive(
        reversed(history),
        sku_id=SKU,
        location_id=LOCATION,
        issue_times=(late, early),
        horizon_days=2,
    )
    croston = rolling_croston_sba(
        reversed(history),
        sku_id=SKU,
        location_id=LOCATION,
        issue_times=(late, early),
        horizon_days=2,
    )

    assert tuple(item.issue_time for item in seasonal) == (early, late)
    assert tuple(item.issue_time for item in croston) == (early, late)
    assert seasonal[0].eligible_observation_count < seasonal[1].eligible_observation_count
    assert croston[0].eligible_observation_count < croston[1].eligible_observation_count


def test_label_cutoff_excludes_incomplete_days_and_old_dated_late_labels() -> None:
    forecast = forecast_probabilistic_seasonal_naive(
        observations(tuple(range(1, 15))),
        sku_id=SKU,
        location_id=LOCATION,
        issue_time=ISSUE_TIME,
        horizon_days=3,
    )
    cutoff = datetime(2026, 7, 24, 4, tzinfo=UTC)
    labels = (
        DailyDemandObservation(SKU, LOCATION, date(2026, 7, 22), 10, cutoff - timedelta(hours=2)),
        DailyDemandObservation(SKU, LOCATION, date(2026, 7, 23), 20, cutoff + timedelta(seconds=1)),
        DailyDemandObservation(SKU, LOCATION, date(2026, 7, 24), 30, cutoff),
    )

    rows = join_matured_labels((forecast,), reversed(labels), label_cutoff=cutoff)

    assert len(rows) == 1
    assert rows[0].target_date == date(2026, 7, 22)
    assert rows[0].actual == 10


def test_join_requires_an_explicit_aware_label_cutoff() -> None:
    with pytest.raises(ValueError, match="label_cutoff"):
        join_matured_labels((), (), label_cutoff=datetime(2026, 7, 24))
