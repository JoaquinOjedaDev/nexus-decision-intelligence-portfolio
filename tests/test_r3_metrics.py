from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from nexus.modeling.r3_metrics import (
    ForecastMetricRow,
    ZeroScaleBehavior,
    block_bootstrap_95,
    central_interval_coverage,
    central_interval_score,
    forecast_bias,
    mean_absolute_scaled_error,
    mean_absolute_scaled_error_from_scale,
    mean_weighted_interval_score,
    panel_mean_absolute_scaled_error,
    pinball_loss,
    summarize_probabilistic_metrics,
    weighted_interval_score,
)


def metric_row(
    *,
    sku: str = "SKU-1",
    origin_day: int = 1,
    horizon: int = 0,
    actual: int = 10,
    median: int = 9,
) -> ForecastMetricRow:
    origin = datetime(2026, 7, origin_day, 4, tzinfo=UTC)
    return ForecastMetricRow(
        sku_id=sku,
        location_id="DC-1",
        origin_time=origin,
        target_date=origin.date() + timedelta(days=horizon),
        actual=Decimal(actual),
        q025=Decimal(median - 2),
        q10=Decimal(median - 1),
        q50=Decimal(median),
        q90=Decimal(median + 2),
        q975=Decimal(median + 3),
    )


def test_pinball_loss_penalizes_under_and_over_forecasts_asymmetrically() -> None:
    assert pinball_loss(10, 8, Decimal("0.9")) == Decimal("1.8")
    assert pinball_loss(10, 12, Decimal("0.9")) == Decimal("0.2")
    assert pinball_loss(10, 10, Decimal("0.5")) == 0


@pytest.mark.parametrize(
    ("actual", "expected"),
    [(10, Decimal(4)), (6, Decimal(24)), (14, Decimal(24))],
)
def test_central_interval_score_has_width_and_tail_penalties(
    actual: int, expected: Decimal
) -> None:
    assert central_interval_score(actual, 8, 12, Decimal("0.2")) == expected


def test_interval_coverage_is_inclusive_and_shape_checked() -> None:
    assert central_interval_coverage((8, 12, 13), (8, 8, 8), (12, 12, 12)) == Decimal("2") / 3
    with pytest.raises(ValueError, match="same length"):
        central_interval_coverage((1, 2), (0,), (3, 3))
    with pytest.raises(ValueError, match="lower"):
        central_interval_coverage((1,), (2,), (0,))


def test_weighted_interval_score_matches_predeclared_80_and_95_formula() -> None:
    score = weighted_interval_score(
        10,
        median=9,
        lower_80=8,
        upper_80=11,
        lower_95=7,
        upper_95=12,
    )

    assert score == Decimal("0.37")


def test_forecast_bias_sign_is_explicit() -> None:
    assert forecast_bias((10, 10), (12, 11)) == Decimal("1.5")
    assert forecast_bias((10, 10), (8, 9)) == Decimal("-1.5")


def test_mase_uses_seasonal_training_scale() -> None:
    result = mean_absolute_scaled_error(
        (3, 5),
        (2, 7),
        training_actuals=(1, 3, 2, 4),
        season_length=2,
    )

    assert result == Decimal("1.5")


def test_mase_zero_scale_behavior_is_never_implicit_nan() -> None:
    assert mean_absolute_scaled_error_from_scale((2,), (2,), scale=0) == 0
    assert mean_absolute_scaled_error_from_scale((2,), (3,), scale=0).is_infinite()
    with pytest.raises(ZeroDivisionError, match="zero"):
        mean_absolute_scaled_error_from_scale(
            (2,),
            (3,),
            scale=0,
            zero_scale_behavior=ZeroScaleBehavior.RAISE,
        )


def test_metric_summary_reports_all_frozen_r3a_metrics() -> None:
    rows = (metric_row(actual=9, median=9), metric_row(origin_day=2, actual=11, median=10))

    summary = summarize_probabilistic_metrics(rows, mase_scale=1)

    assert summary.observation_count == 2
    assert summary.interval_coverage_80 == 1
    assert summary.interval_coverage_95 == 1
    assert summary.bias == Decimal("-0.5")
    assert summary.mase == Decimal("0.5")
    assert summary.weighted_interval_score == mean_weighted_interval_score(rows)
    assert all(
        value >= 0
        for value in (
            summary.pinball_q025,
            summary.pinball_q10,
            summary.pinball_q50,
            summary.pinball_q90,
            summary.pinball_q975,
            summary.interval_score_80,
            summary.interval_score_95,
            summary.weighted_interval_score,
        )
    )


def test_panel_mase_scales_each_block_before_equal_observation_aggregation() -> None:
    first = metric_row(sku="SKU-1", actual=10, median=9)
    second = metric_row(sku="SKU-2", actual=10, median=9)
    rows = (second, first)
    scales = {
        first.block_key: Decimal(1),
        second.block_key: Decimal(3),
    }

    panel = panel_mean_absolute_scaled_error(rows, scales_by_block=scales)
    pooled = mean_absolute_scaled_error_from_scale(
        (row.actual for row in rows),
        (row.q50 for row in rows),
        scale=2,
    )

    expected_panel = (Decimal(1) + Decimal(1) / Decimal(3)) / Decimal(2)
    assert panel == expected_panel
    assert pooled == Decimal("0.5")
    assert panel != pooled
    assert summarize_probabilistic_metrics(rows, mase_scales_by_block=scales).mase == panel
    assert (
        panel_mean_absolute_scaled_error(
            tuple(reversed(rows)), scales_by_block=dict(reversed(tuple(scales.items())))
        )
        == panel
    )


def test_metric_summary_rejects_ambiguous_pooled_and_panel_mase_inputs() -> None:
    row = metric_row()

    with pytest.raises(ValueError, match="mutually exclusive"):
        summarize_probabilistic_metrics(
            (row,),
            mase_scale=1,
            mase_scales_by_block={row.block_key: Decimal(1)},
        )


def test_panel_mase_fails_closed_for_missing_or_duplicate_normalized_scales() -> None:
    row = metric_row()

    with pytest.raises(ValueError, match="missing panel MASE scale"):
        panel_mean_absolute_scaled_error((row,), scales_by_block={})

    with pytest.raises(ValueError, match="duplicate normalized"):
        panel_mean_absolute_scaled_error(
            (row,),
            scales_by_block={
                row.block_key: Decimal(1),
                (f" {row.sku_id} ", row.location_id, row.origin_time): Decimal(1),
            },
        )


def test_panel_mase_applies_zero_scale_policy_per_block() -> None:
    perfect = metric_row(sku="SKU-1", actual=9, median=9)
    wrong = metric_row(sku="SKU-2", actual=10, median=9)

    assert (
        panel_mean_absolute_scaled_error(
            (perfect,), scales_by_block={perfect.block_key: Decimal(0)}
        )
        == 0
    )
    assert panel_mean_absolute_scaled_error(
        (wrong,), scales_by_block={wrong.block_key: Decimal(0)}
    ).is_infinite()
    with pytest.raises(ZeroDivisionError, match="SKU-2"):
        panel_mean_absolute_scaled_error(
            (wrong,),
            scales_by_block={wrong.block_key: Decimal(0)},
            zero_scale_behavior=ZeroScaleBehavior.RAISE,
        )


def test_metric_row_rejects_nonmonotonic_or_negative_forecasts() -> None:
    with pytest.raises(ValueError, match="monotonic"):
        ForecastMetricRow(
            "SKU",
            "DC",
            datetime(2026, 7, 1, tzinfo=UTC),
            date(2026, 7, 1),
            Decimal(1),
            Decimal(0),
            Decimal(2),
            Decimal(1),
            Decimal(3),
            Decimal(4),
        )
    with pytest.raises(ValueError, match="q025"):
        ForecastMetricRow(
            "SKU",
            "DC",
            datetime(2026, 7, 1, tzinfo=UTC),
            date(2026, 7, 1),
            Decimal(1),
            Decimal(-1),
            Decimal(0),
            Decimal(1),
            Decimal(2),
            Decimal(3),
        )


def test_block_bootstrap_is_seeded_blocked_and_input_permutation_invariant() -> None:
    rows = tuple(
        metric_row(sku=sku, origin_day=origin, horizon=horizon, actual=actual, median=median)
        for sku, origin, horizon, actual, median in (
            ("SKU-2", 8, 1, 8, 10),
            ("SKU-1", 1, 0, 10, 9),
            ("SKU-2", 8, 0, 7, 10),
            ("SKU-1", 1, 1, 12, 9),
            ("SKU-1", 8, 0, 10, 10),
        )
    )

    def bias(rows_to_score: tuple[ForecastMetricRow, ...]) -> Decimal:
        return forecast_bias(
            (row.actual for row in rows_to_score),
            (row.q50 for row in rows_to_score),
        )

    first = block_bootstrap_95(rows, bias, resamples=200, seed=20_260_718)
    permuted = block_bootstrap_95(tuple(reversed(rows)), bias, resamples=200, seed=20_260_718)

    assert first == permuted
    assert first.block_count == 3
    assert first.estimate == bias(rows)
    assert first.lower_95 <= first.upper_95
    assert first.confidence_level == Decimal("0.95")


def test_bootstrap_validates_nonempty_rows_and_resample_count() -> None:
    with pytest.raises(ValueError, match="at least one"):
        block_bootstrap_95((), lambda _: Decimal(0))
    with pytest.raises(ValueError, match="resamples"):
        block_bootstrap_95((metric_row(),), lambda _: Decimal(0), resamples=0)
