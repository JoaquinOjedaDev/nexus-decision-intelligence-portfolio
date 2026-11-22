"""Deterministic probabilistic-forecast metrics for the R3 rolling-origin gate.

The functions in this module intentionally use :class:`~decimal.Decimal` internally.
That keeps the small, contract-smoke evaluation reproducible across operating systems
and makes edge-case policies (especially a zero MASE scale) explicit rather than
silently producing a platform-dependent ``nan``.
"""

from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum

type MetricNumber = Decimal | int | float

Q025 = Decimal("0.025")
Q10 = Decimal("0.10")
Q50 = Decimal("0.50")
Q90 = Decimal("0.90")
Q975 = Decimal("0.975")


def _decimal(value: MetricNumber, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float)):
        raise TypeError(f"{name} must be a Decimal, int, or float")
    normalized = Decimal(str(value)) if isinstance(value, float) else Decimal(value)
    if not normalized.is_finite():
        raise ValueError(f"{name} must be finite")
    return normalized


def _positive_integer(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone offset")
    return value.astimezone(UTC)


def _identifier(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be blank")
    return normalized


def _paired(
    actuals: Iterable[MetricNumber], forecasts: Iterable[MetricNumber]
) -> tuple[tuple[Decimal, ...], tuple[Decimal, ...]]:
    normalized_actuals = tuple(_decimal(value, "actual") for value in actuals)
    normalized_forecasts = tuple(_decimal(value, "forecast") for value in forecasts)
    if not normalized_actuals:
        raise ValueError("at least one observation is required")
    if len(normalized_actuals) != len(normalized_forecasts):
        raise ValueError("actuals and forecasts must have the same length")
    return normalized_actuals, normalized_forecasts


def _mean(values: Sequence[Decimal]) -> Decimal:
    if not values:
        raise ValueError("at least one value is required")
    return sum(values, start=Decimal(0)) / Decimal(len(values))


def _empirical_quantile(values: Sequence[Decimal], quantile: Decimal) -> Decimal:
    """Return the deterministic type-7 empirical quantile of non-empty ``values``."""

    if not values:
        raise ValueError("at least one value is required")
    if quantile < 0 or quantile > 1:
        raise ValueError("quantile must be between zero and one")
    ordered = tuple(sorted(values))
    if len(ordered) == 1:
        return ordered[0]
    rank = Decimal(len(ordered) - 1) * quantile
    lower_index = int(rank)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = rank - Decimal(lower_index)
    return ordered[lower_index] + fraction * (ordered[upper_index] - ordered[lower_index])


@dataclass(frozen=True, slots=True)
class ForecastMetricRow:
    """One matured label joined to one five-quantile forecast point.

    ``origin_time`` is part of the block identity. Consequently, bootstrap resampling
    keeps all horizons from a SKU-location rolling origin together.
    """

    sku_id: str
    location_id: str
    origin_time: datetime
    target_date: date
    actual: Decimal
    q025: Decimal
    q10: Decimal
    q50: Decimal
    q90: Decimal
    q975: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "sku_id", _identifier(self.sku_id, "sku_id"))
        object.__setattr__(self, "location_id", _identifier(self.location_id, "location_id"))
        origin_time = _aware_utc(self.origin_time, "origin_time")
        object.__setattr__(self, "origin_time", origin_time)
        if self.target_date < origin_time.date():
            raise ValueError("target_date cannot be earlier than the forecast origin")

        numeric_fields = ("actual", "q025", "q10", "q50", "q90", "q975")
        for field_name in numeric_fields:
            normalized = _decimal(getattr(self, field_name), field_name)
            if normalized < 0:
                raise ValueError(f"{field_name} must be nonnegative")
            object.__setattr__(self, field_name, normalized)
        if not self.q025 <= self.q10 <= self.q50 <= self.q90 <= self.q975:
            raise ValueError("forecast quantiles must be monotonic")

    @property
    def block_key(self) -> tuple[str, str, datetime]:
        """Return the required SKU-location-origin bootstrap block key."""

        return self.sku_id, self.location_id, self.origin_time


@dataclass(frozen=True, slots=True)
class ProbabilisticMetricSummary:
    """Equal-observation-weighted metrics for a matured evaluation window."""

    observation_count: int
    pinball_q025: Decimal
    pinball_q10: Decimal
    pinball_q50: Decimal
    pinball_q90: Decimal
    pinball_q975: Decimal
    interval_score_80: Decimal
    interval_coverage_80: Decimal
    interval_score_95: Decimal
    interval_coverage_95: Decimal
    weighted_interval_score: Decimal
    bias: Decimal
    mase: Decimal | None


class ZeroScaleBehavior(StrEnum):
    """Explicit behavior when the in-sample seasonal MASE scale is zero."""

    ZERO_IF_PERFECT_ELSE_INFINITY = "zero_if_perfect_else_infinity"
    RAISE = "raise"


@dataclass(frozen=True, slots=True)
class BootstrapInterval:
    """A deterministic percentile 95% confidence interval."""

    estimate: Decimal
    lower_95: Decimal
    upper_95: Decimal
    confidence_level: Decimal
    resamples: int
    seed: int
    block_count: int


def pinball_loss(actual: MetricNumber, forecast: MetricNumber, quantile: MetricNumber) -> Decimal:
    """Return pinball loss for one observation and one quantile forecast."""

    normalized_actual = _decimal(actual, "actual")
    normalized_forecast = _decimal(forecast, "forecast")
    normalized_quantile = _decimal(quantile, "quantile")
    if normalized_quantile <= 0 or normalized_quantile >= 1:
        raise ValueError("quantile must be strictly between zero and one")
    error = normalized_actual - normalized_forecast
    if error >= 0:
        return normalized_quantile * error
    return (Decimal(1) - normalized_quantile) * (-error)


def mean_pinball_loss(
    actuals: Iterable[MetricNumber],
    forecasts: Iterable[MetricNumber],
    quantile: MetricNumber,
) -> Decimal:
    """Return equal-observation-weighted mean pinball loss."""

    normalized_actuals, normalized_forecasts = _paired(actuals, forecasts)
    return _mean(
        tuple(
            pinball_loss(actual, forecast, quantile)
            for actual, forecast in zip(normalized_actuals, normalized_forecasts, strict=True)
        )
    )


def central_interval_score(
    actual: MetricNumber,
    lower: MetricNumber,
    upper: MetricNumber,
    alpha: MetricNumber,
) -> Decimal:
    """Return the proper score for one central ``1 - alpha`` prediction interval."""

    normalized_actual = _decimal(actual, "actual")
    normalized_lower = _decimal(lower, "lower")
    normalized_upper = _decimal(upper, "upper")
    normalized_alpha = _decimal(alpha, "alpha")
    if normalized_alpha <= 0 or normalized_alpha >= 1:
        raise ValueError("alpha must be strictly between zero and one")
    if normalized_lower > normalized_upper:
        raise ValueError("lower cannot exceed upper")

    score = normalized_upper - normalized_lower
    penalty_weight = Decimal(2) / normalized_alpha
    if normalized_actual < normalized_lower:
        score += penalty_weight * (normalized_lower - normalized_actual)
    elif normalized_actual > normalized_upper:
        score += penalty_weight * (normalized_actual - normalized_upper)
    return score


def mean_central_interval_score(
    actuals: Iterable[MetricNumber],
    lowers: Iterable[MetricNumber],
    uppers: Iterable[MetricNumber],
    alpha: MetricNumber,
) -> Decimal:
    """Return the mean central interval score after strict shape validation."""

    normalized_actuals = tuple(_decimal(value, "actual") for value in actuals)
    normalized_lowers = tuple(_decimal(value, "lower") for value in lowers)
    normalized_uppers = tuple(_decimal(value, "upper") for value in uppers)
    if not normalized_actuals:
        raise ValueError("at least one observation is required")
    if len(normalized_actuals) != len(normalized_lowers) or len(normalized_actuals) != len(
        normalized_uppers
    ):
        raise ValueError("actuals, lowers, and uppers must have the same length")
    return _mean(
        tuple(
            central_interval_score(actual, lower, upper, alpha)
            for actual, lower, upper in zip(
                normalized_actuals, normalized_lowers, normalized_uppers, strict=True
            )
        )
    )


def central_interval_coverage(
    actuals: Iterable[MetricNumber],
    lowers: Iterable[MetricNumber],
    uppers: Iterable[MetricNumber],
) -> Decimal:
    """Return inclusive empirical interval coverage (boundary hits count as covered)."""

    normalized_actuals = tuple(_decimal(value, "actual") for value in actuals)
    normalized_lowers = tuple(_decimal(value, "lower") for value in lowers)
    normalized_uppers = tuple(_decimal(value, "upper") for value in uppers)
    if not normalized_actuals:
        raise ValueError("at least one observation is required")
    if len(normalized_actuals) != len(normalized_lowers) or len(normalized_actuals) != len(
        normalized_uppers
    ):
        raise ValueError("actuals, lowers, and uppers must have the same length")
    for lower, upper in zip(normalized_lowers, normalized_uppers, strict=True):
        if lower > upper:
            raise ValueError("lower cannot exceed upper")
    covered = sum(
        lower <= actual <= upper
        for actual, lower, upper in zip(
            normalized_actuals, normalized_lowers, normalized_uppers, strict=True
        )
    )
    return Decimal(covered) / Decimal(len(normalized_actuals))


def weighted_interval_score(
    actual: MetricNumber,
    *,
    median: MetricNumber,
    lower_80: MetricNumber,
    upper_80: MetricNumber,
    lower_95: MetricNumber,
    upper_95: MetricNumber,
) -> Decimal:
    """Return standard WIS using the median and central 80%/95% intervals."""

    normalized_actual = _decimal(actual, "actual")
    normalized_median = _decimal(median, "median")
    score_80 = central_interval_score(actual, lower_80, upper_80, Decimal("0.20"))
    score_95 = central_interval_score(actual, lower_95, upper_95, Decimal("0.05"))
    weighted_sum = (
        Decimal("0.5") * abs(normalized_actual - normalized_median)
        + Decimal("0.10") * score_80
        + Decimal("0.025") * score_95
    )
    return weighted_sum / Decimal("2.5")


def mean_weighted_interval_score(rows: Sequence[ForecastMetricRow]) -> Decimal:
    """Return equal-observation-weighted WIS for metric rows."""

    if not rows:
        raise ValueError("at least one metric row is required")
    return _mean(
        tuple(
            weighted_interval_score(
                row.actual,
                median=row.q50,
                lower_80=row.q10,
                upper_80=row.q90,
                lower_95=row.q025,
                upper_95=row.q975,
            )
            for row in rows
        )
    )


def forecast_bias(actuals: Iterable[MetricNumber], forecasts: Iterable[MetricNumber]) -> Decimal:
    """Return mean ``forecast - actual``; positive values mean over-forecasting."""

    normalized_actuals, normalized_forecasts = _paired(actuals, forecasts)
    return _mean(
        tuple(
            forecast - actual
            for actual, forecast in zip(normalized_actuals, normalized_forecasts, strict=True)
        )
    )


def mean_absolute_scaled_error_from_scale(
    actuals: Iterable[MetricNumber],
    forecasts: Iterable[MetricNumber],
    *,
    scale: MetricNumber,
    zero_scale_behavior: ZeroScaleBehavior = ZeroScaleBehavior.ZERO_IF_PERFECT_ELSE_INFINITY,
) -> Decimal:
    """Return MASE from a precomputed nonnegative scale with an explicit zero policy."""

    normalized_actuals, normalized_forecasts = _paired(actuals, forecasts)
    normalized_scale = _decimal(scale, "scale")
    if normalized_scale < 0:
        raise ValueError("scale must be nonnegative")
    absolute_error = _mean(
        tuple(
            abs(actual - forecast)
            for actual, forecast in zip(normalized_actuals, normalized_forecasts, strict=True)
        )
    )
    if normalized_scale != 0:
        return absolute_error / normalized_scale
    if zero_scale_behavior is ZeroScaleBehavior.RAISE:
        raise ZeroDivisionError("MASE scale is zero")
    if absolute_error == 0:
        return Decimal(0)
    return Decimal("Infinity")


def mean_absolute_scaled_error(
    actuals: Iterable[MetricNumber],
    forecasts: Iterable[MetricNumber],
    *,
    training_actuals: Iterable[MetricNumber],
    season_length: int,
    zero_scale_behavior: ZeroScaleBehavior = ZeroScaleBehavior.ZERO_IF_PERFECT_ELSE_INFINITY,
) -> Decimal:
    """Return seasonal MASE using only the explicitly supplied training history."""

    normalized_season = _positive_integer(season_length, "season_length")
    history = tuple(_decimal(value, "training_actual") for value in training_actuals)
    if len(history) <= normalized_season:
        raise ValueError("training_actuals must contain at least one seasonal-lag pair")
    scale = _mean(
        tuple(
            abs(history[index] - history[index - normalized_season])
            for index in range(normalized_season, len(history))
        )
    )
    return mean_absolute_scaled_error_from_scale(
        actuals,
        forecasts,
        scale=scale,
        zero_scale_behavior=zero_scale_behavior,
    )


def summarize_probabilistic_metrics(
    rows: Sequence[ForecastMetricRow],
    *,
    mase_scale: MetricNumber | None = None,
    mase_scales_by_block: Mapping[tuple[str, str, datetime], MetricNumber] | None = None,
    zero_scale_behavior: ZeroScaleBehavior = ZeroScaleBehavior.ZERO_IF_PERFECT_ELSE_INFINITY,
) -> ProbabilisticMetricSummary:
    """Compute the frozen R3A metric set over matured rows only."""

    if not rows:
        raise ValueError("at least one metric row is required")
    if mase_scale is not None and mase_scales_by_block is not None:
        raise ValueError("mase_scale and mase_scales_by_block are mutually exclusive")
    actuals = tuple(row.actual for row in rows)
    q025 = tuple(row.q025 for row in rows)
    q10 = tuple(row.q10 for row in rows)
    q50 = tuple(row.q50 for row in rows)
    q90 = tuple(row.q90 for row in rows)
    q975 = tuple(row.q975 for row in rows)
    if mase_scales_by_block is not None:
        mase = panel_mean_absolute_scaled_error(
            rows,
            scales_by_block=mase_scales_by_block,
            zero_scale_behavior=zero_scale_behavior,
        )
    elif mase_scale is not None:
        mase = mean_absolute_scaled_error_from_scale(
            actuals,
            q50,
            scale=mase_scale,
            zero_scale_behavior=zero_scale_behavior,
        )
    else:
        mase = None
    return ProbabilisticMetricSummary(
        observation_count=len(rows),
        pinball_q025=mean_pinball_loss(actuals, q025, Q025),
        pinball_q10=mean_pinball_loss(actuals, q10, Q10),
        pinball_q50=mean_pinball_loss(actuals, q50, Q50),
        pinball_q90=mean_pinball_loss(actuals, q90, Q90),
        pinball_q975=mean_pinball_loss(actuals, q975, Q975),
        interval_score_80=mean_central_interval_score(actuals, q10, q90, Decimal("0.20")),
        interval_coverage_80=central_interval_coverage(actuals, q10, q90),
        interval_score_95=mean_central_interval_score(actuals, q025, q975, Decimal("0.05")),
        interval_coverage_95=central_interval_coverage(actuals, q025, q975),
        weighted_interval_score=mean_weighted_interval_score(rows),
        bias=forecast_bias(actuals, q50),
        mase=mase,
    )


def _row_sort_key(
    row: ForecastMetricRow,
) -> tuple[str, str, datetime, date, Decimal, Decimal, Decimal, Decimal, Decimal, Decimal]:
    return (
        row.sku_id,
        row.location_id,
        row.origin_time,
        row.target_date,
        row.actual,
        row.q025,
        row.q10,
        row.q50,
        row.q90,
        row.q975,
    )


def _scale_key(value: object) -> tuple[str, str, datetime]:
    if not isinstance(value, tuple) or len(value) != 3:
        raise TypeError("panel MASE scale keys must be (sku_id, location_id, origin_time) tuples")
    sku_id, location_id, origin_time = value
    if not isinstance(sku_id, str) or not isinstance(location_id, str):
        raise TypeError("panel MASE scale key identifiers must be strings")
    if not isinstance(origin_time, datetime):
        raise TypeError("panel MASE scale key origin_time must be a datetime")
    return (
        _identifier(sku_id, "scale sku_id"),
        _identifier(location_id, "scale location_id"),
        _aware_utc(origin_time, "scale origin_time"),
    )


def panel_mean_absolute_scaled_error(
    rows: Sequence[ForecastMetricRow],
    *,
    scales_by_block: Mapping[tuple[str, str, datetime], MetricNumber],
    zero_scale_behavior: ZeroScaleBehavior = ZeroScaleBehavior.ZERO_IF_PERFECT_ELSE_INFINITY,
) -> Decimal:
    """Return equal-observation panel MASE using one scale per series-origin block.

    Each error is divided by the seasonal scale fitted for its own
    ``(sku_id, location_id, origin_time)`` block before observations are averaged. This
    avoids the scientifically different ratio-of-pooled-means calculation. Missing
    scales and normalized duplicate keys fail closed.
    """

    if not rows:
        raise ValueError("at least one metric row is required")
    normalized_scales: dict[tuple[str, str, datetime], Decimal] = {}
    for raw_key, raw_scale in scales_by_block.items():
        key = _scale_key(raw_key)
        if key in normalized_scales:
            raise ValueError(f"duplicate normalized panel MASE scale key: {key!r}")
        scale = _decimal(raw_scale, "panel MASE scale")
        if scale < 0:
            raise ValueError("panel MASE scales must be nonnegative")
        normalized_scales[key] = scale

    scaled_errors: list[Decimal] = []
    for row in sorted(rows, key=_row_sort_key):
        try:
            scale = normalized_scales[row.block_key]
        except KeyError as error:
            raise ValueError(f"missing panel MASE scale for block {row.block_key!r}") from error
        absolute_error = abs(row.actual - row.q50)
        if scale != 0:
            scaled_errors.append(absolute_error / scale)
        elif zero_scale_behavior is ZeroScaleBehavior.RAISE:
            raise ZeroDivisionError(f"panel MASE scale is zero for block {row.block_key!r}")
        elif absolute_error == 0:
            scaled_errors.append(Decimal(0))
        else:
            scaled_errors.append(Decimal("Infinity"))
    return _mean(scaled_errors)


def block_bootstrap_95(
    rows: Sequence[ForecastMetricRow],
    metric: Callable[[tuple[ForecastMetricRow, ...]], MetricNumber],
    *,
    resamples: int = 1_000,
    seed: int = 20_260_718,
) -> BootstrapInterval:
    """Bootstrap a deterministic 95% percentile CI by SKU-location-origin block.

    Inputs and blocks are canonically sorted before seeded resampling, so permuting the
    input rows cannot change the estimate or confidence interval.
    """

    normalized_resamples = _positive_integer(resamples, "resamples")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")
    if not rows:
        raise ValueError("at least one metric row is required")

    canonical_rows = tuple(sorted(rows, key=_row_sort_key))
    grouped: dict[tuple[str, str, datetime], list[ForecastMetricRow]] = defaultdict(list)
    for row in canonical_rows:
        grouped[row.block_key].append(row)
    block_keys = tuple(sorted(grouped))
    blocks = tuple(tuple(grouped[key]) for key in block_keys)

    estimate = _decimal(metric(canonical_rows), "metric estimate")
    generator = random.Random(seed)
    replicate_estimates: list[Decimal] = []
    for _ in range(normalized_resamples):
        sampled_rows = tuple(
            row for _block in range(len(blocks)) for row in blocks[generator.randrange(len(blocks))]
        )
        canonical_sample = tuple(sorted(sampled_rows, key=_row_sort_key))
        replicate_estimates.append(_decimal(metric(canonical_sample), "bootstrap metric"))

    return BootstrapInterval(
        estimate=estimate,
        lower_95=_empirical_quantile(replicate_estimates, Decimal("0.025")),
        upper_95=_empirical_quantile(replicate_estimates, Decimal("0.975")),
        confidence_level=Decimal("0.95"),
        resamples=normalized_resamples,
        seed=seed,
        block_count=len(blocks),
    )
