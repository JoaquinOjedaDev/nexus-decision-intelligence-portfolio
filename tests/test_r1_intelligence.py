from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from nexus.decisioning import ReplenishmentReasonCode, recommend_base_stock
from nexus.modeling import (
    DailyDemandObservation,
    ForecastSource,
    SeasonalNaiveForecast,
    forecast_seasonal_naive,
)

SKU = "SKU-001"
LOCATION = "DC-ARG-01"
ISSUE_TIME = datetime(2026, 7, 15, tzinfo=UTC)
LATEST_WEEK = (11, 13, 9, 12, 14, 10, 8)


def latest_week_observations(
    values: tuple[int, ...] = LATEST_WEEK,
) -> tuple[DailyDemandObservation, ...]:
    start = date(2026, 7, 8)
    return tuple(
        DailyDemandObservation(
            sku_id=SKU,
            location_id=LOCATION,
            demand_date=start + timedelta(days=offset),
            quantity=quantity,
            available_at=ISSUE_TIME - timedelta(days=6 - offset),
        )
        for offset, quantity in enumerate(values)
    )


def forecast(
    observations: tuple[DailyDemandObservation, ...] | None = None,
    *,
    fallback: Decimal | int = 0,
) -> SeasonalNaiveForecast:
    return forecast_seasonal_naive(
        latest_week_observations() if observations is None else observations,
        sku_id=SKU,
        location_id=LOCATION,
        issue_time=ISSUE_TIME,
        history_cutoff=ISSUE_TIME,
        horizon_days=7,
        season_length_days=7,
        cold_start_daily_demand=fallback,
    )


def test_acceptance_example_forecasts_77_and_recommends_47() -> None:
    result = forecast()

    assert tuple(point.predicted_demand for point in result.points) == tuple(
        Decimal(value) for value in LATEST_WEEK
    )
    assert result.total_expected_demand() == Decimal(77)
    assert all(point.source is ForecastSource.OBSERVED_SEASONAL_LAG for point in result.points)

    recommendation = recommend_base_stock(
        result,
        lead_time_days=7,
        safety_stock=15,
        on_hand=35,
        on_order=10,
    )

    assert recommendation.base_stock_level == Decimal(92)
    assert recommendation.net_requirement == Decimal(47)
    assert recommendation.recommended_quantity == 47
    assert recommendation.reason_code is ReplenishmentReasonCode.BELOW_BASE_STOCK
    assert recommendation.to_recommendation_line().recommended_quantity == 47


def test_post_cutoff_and_issue_day_records_cannot_leak_into_forecast() -> None:
    baseline = forecast()
    late_correction = DailyDemandObservation(
        sku_id=SKU,
        location_id=LOCATION,
        demand_date=date(2026, 7, 8),
        quantity=999,
        available_at=ISSUE_TIME + timedelta(seconds=1),
    )
    issue_day_actual = DailyDemandObservation(
        sku_id=SKU,
        location_id=LOCATION,
        demand_date=ISSUE_TIME.date(),
        quantity=999,
        available_at=ISSUE_TIME,
    )

    protected = forecast((*latest_week_observations(), late_correction, issue_day_actual))

    assert protected.points == baseline.points
    assert protected.input_sha256 == baseline.input_sha256
    assert protected.forecast_sha256 == baseline.forecast_sha256
    assert protected.eligible_observation_count == 7


def test_cold_start_uses_explicit_deterministic_fallback() -> None:
    result = forecast((), fallback=3)

    assert result.total_expected_demand() == Decimal(21)
    assert result.eligible_observation_count == 0
    assert all(point.predicted_demand == Decimal(3) for point in result.points)
    assert all(point.source is ForecastSource.COLD_START_FALLBACK for point in result.points)
    assert all(point.source_date is None for point in result.points)


def test_output_is_deterministic_order_independent_and_immutable() -> None:
    chronological = forecast()
    reversed_input = forecast(tuple(reversed(latest_week_observations())))

    assert reversed_input == chronological
    assert reversed_input.forecast_sha256 == chronological.forecast_sha256
    first_point = chronological.points[0]
    with pytest.raises(FrozenInstanceError):
        first_point.predicted_demand = Decimal(999)  # type: ignore[misc]

    first_decision = recommend_base_stock(
        chronological, lead_time_days=7, safety_stock=15, on_hand=35, on_order=10
    )
    second_decision = recommend_base_stock(
        reversed_input, lead_time_days=7, safety_stock=15, on_hand=35, on_order=10
    )
    assert first_decision.decision_sha256 == second_decision.decision_sha256


def test_recommendation_is_nonnegative_and_reports_sufficient_inventory() -> None:
    recommendation = recommend_base_stock(
        forecast(),
        lead_time_days=7,
        safety_stock=15,
        on_hand=100,
        on_order=20,
    )

    assert recommendation.net_requirement == Decimal(0)
    assert recommendation.recommended_quantity == 0
    assert recommendation.reason_code is ReplenishmentReasonCode.INVENTORY_SUFFICIENT


def test_recommendation_is_monotonic_in_demand_and_safety_stock() -> None:
    low_demand = forecast(latest_week_observations((1, 1, 1, 1, 1, 1, 1)))
    high_demand = forecast(latest_week_observations((2, 2, 2, 2, 2, 2, 2)))

    low = recommend_base_stock(low_demand, lead_time_days=7, safety_stock=2, on_hand=0, on_order=0)
    higher_demand = recommend_base_stock(
        high_demand, lead_time_days=7, safety_stock=2, on_hand=0, on_order=0
    )
    higher_safety = recommend_base_stock(
        low_demand, lead_time_days=7, safety_stock=5, on_hand=0, on_order=0
    )

    assert higher_demand.recommended_quantity >= low.recommended_quantity
    assert higher_safety.recommended_quantity >= low.recommended_quantity


def test_fractional_shortage_uses_explicit_ceiling_policy() -> None:
    result = forecast((), fallback=Decimal("0.1"))

    recommendation = recommend_base_stock(
        result, lead_time_days=1, safety_stock=0, on_hand=0, on_order=0
    )

    assert recommendation.net_requirement == Decimal("0.1")
    assert recommendation.recommended_quantity == 1
    assert recommendation.rounding_policy == "ceiling_to_whole_unit"


def test_numerically_equal_decimal_scales_have_identical_hashes() -> None:
    first_forecast = forecast((), fallback=Decimal("1.0"))
    second_forecast = forecast((), fallback=Decimal("1.00"))

    assert first_forecast.forecast_sha256 == second_forecast.forecast_sha256

    first_decision = recommend_base_stock(
        first_forecast,
        lead_time_days=7,
        safety_stock=Decimal("2.0"),
        on_hand=Decimal("1.0"),
        on_order=Decimal("0.0"),
    )
    second_decision = recommend_base_stock(
        second_forecast,
        lead_time_days=7,
        safety_stock=Decimal("2.00"),
        on_hand=Decimal("1.00"),
        on_order=Decimal("0.00"),
    )

    assert first_decision.decision_sha256 == second_decision.decision_sha256


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("horizon_days", 0, "horizon_days"),
        ("season_length_days", -1, "season_length_days"),
        ("cold_start_daily_demand", -1, "cold_start_daily_demand"),
    ],
)
def test_forecast_rejects_invalid_configuration(field: str, value: int, message: str) -> None:
    arguments: dict[str, object] = {
        "sku_id": SKU,
        "location_id": LOCATION,
        "issue_time": ISSUE_TIME,
        "history_cutoff": ISSUE_TIME,
        "horizon_days": 7,
        "season_length_days": 7,
        "cold_start_daily_demand": 0,
    }
    arguments[field] = value

    with pytest.raises(ValueError, match=message):
        forecast_seasonal_naive(latest_week_observations(), **arguments)  # type: ignore[arg-type]


def test_recommendation_rejects_invalid_inventory_or_horizon() -> None:
    result = forecast()
    with pytest.raises(ValueError, match="forecast horizon"):
        recommend_base_stock(result, lead_time_days=8, safety_stock=0, on_hand=0, on_order=0)
    with pytest.raises(ValueError, match="on_hand"):
        recommend_base_stock(result, lead_time_days=7, safety_stock=0, on_hand=-1, on_order=0)
