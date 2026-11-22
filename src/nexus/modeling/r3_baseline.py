"""Point-in-time probabilistic baselines for the R3 rolling-origin evaluation.

R3A deliberately starts with transparent models.  The seasonal-naive median delegates
to the already-tested R1 implementation, while empirical, median-centred residuals add
uncertainty without allowing a record unavailable at the origin into calibration.  A
named Croston-SBA baseline provides the same artifact contract for intermittent demand.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from nexus.modeling.r3_metrics import ForecastMetricRow
from nexus.modeling.seasonal_naive import (
    DailyDemandObservation,
    ForecastSource,
    forecast_seasonal_naive,
)

_SEASONAL_MODEL_NAME = "seasonal_naive_empirical_intervals"
_SEASONAL_MODEL_VERSION = "1.0.0"
_CROSTON_MODEL_NAME = "croston_sba_empirical_intervals"
_CROSTON_MODEL_VERSION = "1.0.0"
_QUANTILE_METHOD = "type_7_median_centred_empirical_residuals"


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone offset")
    return value.astimezone(UTC)


def _identifier(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be blank")
    return normalized


def _positive_integer(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _nonnegative_decimal(value: Decimal | int, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int)):
        raise TypeError(f"{name} must be an int or Decimal")
    normalized = Decimal(value)
    if not normalized.is_finite() or normalized < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    return normalized


def _alpha(value: Decimal | int) -> Decimal:
    normalized = _nonnegative_decimal(value, "alpha")
    if normalized == 0 or normalized > 1:
        raise ValueError("alpha must be greater than zero and no greater than one")
    return normalized


def _canonical_decimal(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _sha256(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _eligible_observations(
    supplied: Sequence[DailyDemandObservation],
    *,
    sku_id: str,
    location_id: str,
    issue_time: datetime,
) -> tuple[DailyDemandObservation, ...]:
    for observation in supplied:
        if observation.sku_id != sku_id or observation.location_id != location_id:
            raise ValueError("all observations must match the requested sku_id and location_id")
    eligible = tuple(
        sorted(
            (
                observation
                for observation in supplied
                if observation.demand_date < issue_time.date()
                and observation.available_at <= issue_time
            ),
            key=lambda observation: observation.demand_date,
        )
    )
    eligible_dates = tuple(observation.demand_date for observation in eligible)
    if len(eligible_dates) != len(set(eligible_dates)):
        raise ValueError("eligible observations must be unique by demand_date")
    return eligible


def _observation_hash(observations: Sequence[DailyDemandObservation]) -> str:
    return _sha256(
        [
            {
                "sku_id": observation.sku_id,
                "location_id": observation.location_id,
                "demand_date": observation.demand_date.isoformat(),
                "quantity": observation.quantity,
                "available_at": observation.available_at.isoformat(),
            }
            for observation in observations
        ]
    )


def _empirical_quantile(values: Sequence[Decimal], quantile: Decimal) -> Decimal:
    if not values:
        raise ValueError("at least one residual is required")
    ordered = tuple(sorted(values))
    if len(ordered) == 1:
        return ordered[0]
    rank = Decimal(len(ordered) - 1) * quantile
    lower_index = int(rank)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = rank - Decimal(lower_index)
    return ordered[lower_index] + fraction * (ordered[upper_index] - ordered[lower_index])


def _centred_residual_offsets(
    residuals: Sequence[Decimal],
) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
    if not residuals:
        zero = Decimal(0)
        return zero, zero, zero, zero, zero
    residual_median = _empirical_quantile(residuals, Decimal("0.50"))
    return tuple(
        _empirical_quantile(residuals, quantile) - residual_median
        for quantile in (
            Decimal("0.025"),
            Decimal("0.10"),
            Decimal("0.50"),
            Decimal("0.90"),
            Decimal("0.975"),
        )
    )  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class ProbabilisticForecastPoint:
    """One nonnegative, monotonic five-quantile daily forecast."""

    target_date: date
    q025: Decimal
    q10: Decimal
    q50: Decimal
    q90: Decimal
    q975: Decimal
    median_source: str
    source_date: date | None = None

    def __post_init__(self) -> None:
        for field_name in ("q025", "q10", "q50", "q90", "q975"):
            value = getattr(self, field_name)
            if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
                raise ValueError(f"{field_name} must be a finite nonnegative Decimal")
        if not self.q025 <= self.q10 <= self.q50 <= self.q90 <= self.q975:
            raise ValueError("forecast quantiles must be monotonic")
        object.__setattr__(self, "median_source", _identifier(self.median_source, "median_source"))

    @property
    def quantiles(self) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
        return self.q025, self.q10, self.q50, self.q90, self.q975


@dataclass(frozen=True, slots=True)
class ProbabilisticBaselineForecast:
    """Immutable baseline artifact shared by seasonal-naive and Croston-SBA."""

    sku_id: str
    location_id: str
    issue_time: datetime
    horizon_days: int
    model_name: str
    model_version: str
    eligible_observation_count: int
    residual_count: int
    input_sha256: str
    residuals: tuple[Decimal, ...]
    points: tuple[ProbabilisticForecastPoint, ...]
    parameters: tuple[tuple[str, str], ...]
    median_forecast_sha256: str | None = None
    quantile_method: str = field(default=_QUANTILE_METHOD, init=False)
    forecast_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if self.horizon_days != len(self.points):
            raise ValueError("horizon_days must equal the number of forecast points")
        if self.residual_count != len(self.residuals):
            raise ValueError("residual_count must equal the number of residuals")
        expected_dates = tuple(
            self.issue_time.date() + timedelta(days=offset) for offset in range(self.horizon_days)
        )
        if tuple(point.target_date for point in self.points) != expected_dates:
            raise ValueError("forecast points must be contiguous from the issue date")
        payload = {
            "sku_id": self.sku_id,
            "location_id": self.location_id,
            "issue_time": self.issue_time.isoformat(),
            "horizon_days": self.horizon_days,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "eligible_observation_count": self.eligible_observation_count,
            "input_sha256": self.input_sha256,
            "residuals": [_canonical_decimal(value) for value in self.residuals],
            "parameters": self.parameters,
            "median_forecast_sha256": self.median_forecast_sha256,
            "quantile_method": self.quantile_method,
            "points": [
                {
                    "target_date": point.target_date.isoformat(),
                    "q025": _canonical_decimal(point.q025),
                    "q10": _canonical_decimal(point.q10),
                    "q50": _canonical_decimal(point.q50),
                    "q90": _canonical_decimal(point.q90),
                    "q975": _canonical_decimal(point.q975),
                    "median_source": point.median_source,
                    "source_date": (
                        None if point.source_date is None else point.source_date.isoformat()
                    ),
                }
                for point in self.points
            ],
        }
        object.__setattr__(self, "forecast_sha256", _sha256(payload))


def _quantile_point(
    *,
    target_date: date,
    median: Decimal,
    offsets: tuple[Decimal, Decimal, Decimal, Decimal, Decimal],
    median_source: str,
    source_date: date | None,
) -> ProbabilisticForecastPoint:
    q025 = max(Decimal(0), median + offsets[0])
    q10 = max(q025, median + offsets[1], Decimal(0))
    q50 = median
    q90 = max(q50, median + offsets[3])
    q975 = max(q90, median + offsets[4])
    return ProbabilisticForecastPoint(
        target_date=target_date,
        q025=q025,
        q10=min(q10, q50),
        q50=q50,
        q90=q90,
        q975=q975,
        median_source=median_source,
        source_date=source_date,
    )


def _seasonal_residuals(
    eligible: Sequence[DailyDemandObservation], season_length_days: int
) -> tuple[Decimal, ...]:
    observed = {observation.demand_date: observation.quantity for observation in eligible}
    return tuple(
        Decimal(
            observation.quantity
            - observed[observation.demand_date - timedelta(days=season_length_days)]
        )
        for observation in eligible
        if observation.demand_date - timedelta(days=season_length_days) in observed
    )


def forecast_probabilistic_seasonal_naive(
    observations: Iterable[DailyDemandObservation],
    *,
    sku_id: str,
    location_id: str,
    issue_time: datetime,
    horizon_days: int,
    season_length_days: int = 7,
    cold_start_daily_demand: Decimal | int = 0,
) -> ProbabilisticBaselineForecast:
    """Forecast five quantiles using only facts knowable at ``issue_time``.

    The q50 path is exactly the existing deterministic seasonal-naive forecast.  The
    empirical residual sample contains only pairs for which both the target actual and
    its seasonal lag pass ``demand_date < issue_date`` and
    ``available_at <= issue_time``.  Residual quantiles are median-centred so q50 stays
    the audited R1 baseline rather than becoming a hidden bias-corrected model.
    """

    normalized_sku = _identifier(sku_id, "sku_id")
    normalized_location = _identifier(location_id, "location_id")
    normalized_issue = _aware_utc(issue_time, "issue_time")
    normalized_horizon = _positive_integer(horizon_days, "horizon_days")
    normalized_season = _positive_integer(season_length_days, "season_length_days")
    normalized_fallback = _nonnegative_decimal(cold_start_daily_demand, "cold_start_daily_demand")
    supplied = tuple(observations)
    eligible = _eligible_observations(
        supplied,
        sku_id=normalized_sku,
        location_id=normalized_location,
        issue_time=normalized_issue,
    )
    median_forecast = forecast_seasonal_naive(
        supplied,
        sku_id=normalized_sku,
        location_id=normalized_location,
        issue_time=normalized_issue,
        history_cutoff=normalized_issue,
        horizon_days=normalized_horizon,
        season_length_days=normalized_season,
        cold_start_daily_demand=normalized_fallback,
    )
    residuals = _seasonal_residuals(eligible, normalized_season)
    offsets = _centred_residual_offsets(residuals)
    points = tuple(
        _quantile_point(
            target_date=median_point.target_date,
            median=median_point.predicted_demand,
            offsets=offsets,
            median_source=median_point.source.value,
            source_date=median_point.source_date,
        )
        for median_point in median_forecast.points
    )
    return ProbabilisticBaselineForecast(
        sku_id=normalized_sku,
        location_id=normalized_location,
        issue_time=normalized_issue,
        horizon_days=normalized_horizon,
        model_name=_SEASONAL_MODEL_NAME,
        model_version=_SEASONAL_MODEL_VERSION,
        eligible_observation_count=len(eligible),
        residual_count=len(residuals),
        input_sha256=median_forecast.input_sha256,
        residuals=residuals,
        points=points,
        parameters=(
            ("season_length_days", str(normalized_season)),
            ("cold_start_daily_demand", _canonical_decimal(normalized_fallback)),
        ),
        median_forecast_sha256=median_forecast.forecast_sha256,
    )


def _croston_state_and_residuals(
    eligible: Sequence[DailyDemandObservation],
    *,
    alpha: Decimal,
    fallback: Decimal,
) -> tuple[Decimal, tuple[Decimal, ...]]:
    if not eligible:
        return fallback, ()
    smoothed_size: Decimal | None = None
    smoothed_interval: Decimal | None = None
    last_nonzero_date: date | None = None
    current_forecast = fallback
    residuals: list[Decimal] = []
    history_start = eligible[0].demand_date

    for index, observation in enumerate(eligible):
        if index > 0:
            residuals.append(Decimal(observation.quantity) - current_forecast)
        if observation.quantity == 0:
            continue
        observed_size = Decimal(observation.quantity)
        if smoothed_size is None or smoothed_interval is None or last_nonzero_date is None:
            smoothed_size = observed_size
            smoothed_interval = Decimal((observation.demand_date - history_start).days + 1)
        else:
            observed_interval = Decimal((observation.demand_date - last_nonzero_date).days)
            smoothed_size = alpha * observed_size + (Decimal(1) - alpha) * smoothed_size
            smoothed_interval = alpha * observed_interval + (Decimal(1) - alpha) * smoothed_interval
        last_nonzero_date = observation.demand_date
        current_forecast = (Decimal(1) - alpha / Decimal(2)) * (smoothed_size / smoothed_interval)

    return current_forecast, tuple(residuals)


def forecast_croston_sba(
    observations: Iterable[DailyDemandObservation],
    *,
    sku_id: str,
    location_id: str,
    issue_time: datetime,
    horizon_days: int,
    alpha: Decimal | int = Decimal("0.10"),
    cold_start_daily_demand: Decimal | int = 0,
) -> ProbabilisticBaselineForecast:
    """Return a constant-horizon Croston-SBA intermittent-demand forecast.

    One-step historical residuals are generated chronologically: every residual uses
    the Croston state *before* that day's actual updates the state.  Combined with the
    strict origin filter, this makes the interval wrapper point-in-time correct.
    """

    normalized_sku = _identifier(sku_id, "sku_id")
    normalized_location = _identifier(location_id, "location_id")
    normalized_issue = _aware_utc(issue_time, "issue_time")
    normalized_horizon = _positive_integer(horizon_days, "horizon_days")
    normalized_alpha = _alpha(alpha)
    normalized_fallback = _nonnegative_decimal(cold_start_daily_demand, "cold_start_daily_demand")
    supplied = tuple(observations)
    eligible = _eligible_observations(
        supplied,
        sku_id=normalized_sku,
        location_id=normalized_location,
        issue_time=normalized_issue,
    )
    median, residuals = _croston_state_and_residuals(
        eligible,
        alpha=normalized_alpha,
        fallback=normalized_fallback,
    )
    offsets = _centred_residual_offsets(residuals)
    median_source = (
        "croston_sba_state"
        if any(observation.quantity > 0 for observation in eligible)
        else ForecastSource.COLD_START_FALLBACK.value
    )
    points = tuple(
        _quantile_point(
            target_date=normalized_issue.date() + timedelta(days=offset),
            median=median,
            offsets=offsets,
            median_source=median_source,
            source_date=None,
        )
        for offset in range(normalized_horizon)
    )
    return ProbabilisticBaselineForecast(
        sku_id=normalized_sku,
        location_id=normalized_location,
        issue_time=normalized_issue,
        horizon_days=normalized_horizon,
        model_name=_CROSTON_MODEL_NAME,
        model_version=_CROSTON_MODEL_VERSION,
        eligible_observation_count=len(eligible),
        residual_count=len(residuals),
        input_sha256=_observation_hash(eligible),
        residuals=residuals,
        points=points,
        parameters=(
            ("alpha", _canonical_decimal(normalized_alpha)),
            ("cold_start_daily_demand", _canonical_decimal(normalized_fallback)),
        ),
    )


def _origins(issue_times: Iterable[datetime]) -> tuple[datetime, ...]:
    normalized = tuple(_aware_utc(value, "issue_time") for value in issue_times)
    if not normalized:
        raise ValueError("at least one issue_time is required")
    if len(normalized) != len(set(normalized)):
        raise ValueError("issue_times must be unique")
    return tuple(sorted(normalized))


def rolling_probabilistic_seasonal_naive(
    observations: Iterable[DailyDemandObservation],
    *,
    sku_id: str,
    location_id: str,
    issue_times: Iterable[datetime],
    horizon_days: int,
    season_length_days: int = 7,
    cold_start_daily_demand: Decimal | int = 0,
) -> tuple[ProbabilisticBaselineForecast, ...]:
    """Generate chronologically ordered, point-in-time seasonal rolling origins."""

    supplied = tuple(observations)
    return tuple(
        forecast_probabilistic_seasonal_naive(
            supplied,
            sku_id=sku_id,
            location_id=location_id,
            issue_time=issue_time,
            horizon_days=horizon_days,
            season_length_days=season_length_days,
            cold_start_daily_demand=cold_start_daily_demand,
        )
        for issue_time in _origins(issue_times)
    )


def rolling_croston_sba(
    observations: Iterable[DailyDemandObservation],
    *,
    sku_id: str,
    location_id: str,
    issue_times: Iterable[datetime],
    horizon_days: int,
    alpha: Decimal | int = Decimal("0.10"),
    cold_start_daily_demand: Decimal | int = 0,
) -> tuple[ProbabilisticBaselineForecast, ...]:
    """Generate chronologically ordered, point-in-time Croston-SBA rolling origins."""

    supplied = tuple(observations)
    return tuple(
        forecast_croston_sba(
            supplied,
            sku_id=sku_id,
            location_id=location_id,
            issue_time=issue_time,
            horizon_days=horizon_days,
            alpha=alpha,
            cold_start_daily_demand=cold_start_daily_demand,
        )
        for issue_time in _origins(issue_times)
    )


def join_matured_labels(
    forecasts: Iterable[ProbabilisticBaselineForecast],
    labels: Iterable[DailyDemandObservation],
    *,
    label_cutoff: datetime,
) -> tuple[ForecastMetricRow, ...]:
    """Join forecasts to labels that are explicitly mature at ``label_cutoff``.

    A label is mature only when its demand day is complete in UTC
    (``demand_date < label_cutoff.date()``) and it had arrived by the cutoff.  Thus an
    old-dated record arriving after the cutoff cannot enter an evaluation retroactively.
    """

    normalized_cutoff = _aware_utc(label_cutoff, "label_cutoff")
    matured = tuple(
        label
        for label in labels
        if label.demand_date < normalized_cutoff.date() and label.available_at <= normalized_cutoff
    )
    labels_by_key: dict[tuple[str, str, date], DailyDemandObservation] = {}
    for label in matured:
        key = label.sku_id, label.location_id, label.demand_date
        if key in labels_by_key:
            raise ValueError("matured labels must be unique by sku_id, location_id, demand_date")
        labels_by_key[key] = label

    canonical_forecasts = tuple(
        sorted(
            (forecast for forecast in forecasts if forecast.issue_time <= normalized_cutoff),
            key=lambda forecast: (
                forecast.sku_id,
                forecast.location_id,
                forecast.issue_time,
                forecast.model_name,
            ),
        )
    )
    forecast_keys: set[tuple[str, str, datetime, str]] = set()
    rows: list[ForecastMetricRow] = []
    for forecast in canonical_forecasts:
        forecast_key = (
            forecast.sku_id,
            forecast.location_id,
            forecast.issue_time,
            forecast.model_name,
        )
        if forecast_key in forecast_keys:
            raise ValueError("forecasts must be unique by series, origin, and model")
        forecast_keys.add(forecast_key)
        for point in forecast.points:
            matched_label = labels_by_key.get(
                (forecast.sku_id, forecast.location_id, point.target_date)
            )
            if matched_label is None:
                continue
            rows.append(
                ForecastMetricRow(
                    sku_id=forecast.sku_id,
                    location_id=forecast.location_id,
                    origin_time=forecast.issue_time,
                    target_date=point.target_date,
                    actual=Decimal(matched_label.quantity),
                    q025=point.q025,
                    q10=point.q10,
                    q50=point.q50,
                    q90=point.q90,
                    q975=point.q975,
                )
            )
    return tuple(
        sorted(
            rows,
            key=lambda row: (row.sku_id, row.location_id, row.origin_time, row.target_date),
        )
    )
