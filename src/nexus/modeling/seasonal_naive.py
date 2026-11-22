"""Point-in-time-correct seasonal-naive demand forecasting.

The baseline deliberately has no learned parameters: demand for a target day is copied
from the same seasonal position in the previous period. If that source day is absent,
an explicit caller-supplied cold-start value is used. Forecasts longer than one season
recursively reuse already-produced forecast points.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

_MODEL_NAME = "seasonal_naive"
_MODEL_VERSION = "1.0.0"


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone offset")
    return value.astimezone(UTC)


def _positive_integer(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _identifier(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be blank")
    return normalized


def _nonnegative_decimal(value: Decimal | int, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int)):
        raise TypeError(f"{name} must be an int or Decimal")
    normalized = Decimal(value)
    if not normalized.is_finite() or normalized < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    return normalized


def _sha256(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_decimal(value: Decimal) -> str:
    """Represent numerically equal decimals identically in scientific hashes."""

    if value == 0:
        return "0"
    return format(value.normalize(), "f")


@dataclass(frozen=True, slots=True)
class DailyDemandObservation:
    """One already-aggregated daily demand fact and when it became knowable."""

    sku_id: str
    location_id: str
    demand_date: date
    quantity: int
    available_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "sku_id", _identifier(self.sku_id, "sku_id"))
        object.__setattr__(self, "location_id", _identifier(self.location_id, "location_id"))
        if isinstance(self.quantity, bool) or not isinstance(self.quantity, int):
            raise TypeError("quantity must be an integer")
        if self.quantity < 0:
            raise ValueError("quantity must be nonnegative")
        available_at = _aware_utc(self.available_at, "available_at")
        if self.demand_date > available_at.date():
            raise ValueError("demand_date cannot be later than available_at")
        object.__setattr__(self, "available_at", available_at)


class ForecastSource(StrEnum):
    """Origin of one transparent forecast value."""

    OBSERVED_SEASONAL_LAG = "observed_seasonal_lag"
    RECURSIVE_SEASONAL_LAG = "recursive_seasonal_lag"
    COLD_START_FALLBACK = "cold_start_fallback"


@dataclass(frozen=True, slots=True)
class ForecastPoint:
    """One immutable daily prediction with an inspectable source."""

    target_date: date
    predicted_demand: Decimal
    source_date: date | None
    source: ForecastSource


@dataclass(frozen=True, slots=True)
class SeasonalNaiveForecast:
    """Immutable output suitable for content-addressed provenance."""

    sku_id: str
    location_id: str
    issue_time: datetime
    history_cutoff: datetime
    horizon_days: int
    season_length_days: int
    cold_start_daily_demand: Decimal
    eligible_observation_count: int
    points: tuple[ForecastPoint, ...]
    input_sha256: str
    model_name: str = field(default=_MODEL_NAME, init=False)
    model_version: str = field(default=_MODEL_VERSION, init=False)
    forecast_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        content = {
            "model_name": self.model_name,
            "model_version": self.model_version,
            "sku_id": self.sku_id,
            "location_id": self.location_id,
            "issue_time": self.issue_time.isoformat(),
            "history_cutoff": self.history_cutoff.isoformat(),
            "horizon_days": self.horizon_days,
            "season_length_days": self.season_length_days,
            "cold_start_daily_demand": _canonical_decimal(self.cold_start_daily_demand),
            "input_sha256": self.input_sha256,
            "points": [
                {
                    "target_date": point.target_date.isoformat(),
                    "predicted_demand": _canonical_decimal(point.predicted_demand),
                    "source_date": (
                        None if point.source_date is None else point.source_date.isoformat()
                    ),
                    "source": point.source.value,
                }
                for point in self.points
            ],
        }
        object.__setattr__(self, "forecast_sha256", _sha256(content))

    def total_expected_demand(self, days: int | None = None) -> Decimal:
        """Sum the first ``days`` predictions, or the complete horizon when omitted."""

        selected_days = self.horizon_days if days is None else _positive_integer(days, "days")
        if selected_days > self.horizon_days:
            raise ValueError("days cannot exceed the forecast horizon")
        return sum(
            (point.predicted_demand for point in self.points[:selected_days]),
            start=Decimal(0),
        )


def forecast_seasonal_naive(
    observations: Iterable[DailyDemandObservation],
    *,
    sku_id: str,
    location_id: str,
    issue_time: datetime,
    history_cutoff: datetime,
    horizon_days: int,
    season_length_days: int = 7,
    cold_start_daily_demand: Decimal | int = 0,
) -> SeasonalNaiveForecast:
    """Forecast daily demand without using information unavailable at the cutoff.

    The first target is the UTC calendar date containing ``issue_time``. A history
    record is eligible only when it represents an earlier date and ``available_at`` is
    no later than ``history_cutoff``. This dual rule blocks both future demand and
    late-arriving corrections from leaking into the forecast.
    """

    normalized_sku = _identifier(sku_id, "sku_id")
    normalized_location = _identifier(location_id, "location_id")
    normalized_issue = _aware_utc(issue_time, "issue_time")
    normalized_cutoff = _aware_utc(history_cutoff, "history_cutoff")
    if normalized_cutoff > normalized_issue:
        raise ValueError("history_cutoff cannot be later than issue_time")
    normalized_horizon = _positive_integer(horizon_days, "horizon_days")
    normalized_season = _positive_integer(season_length_days, "season_length_days")
    normalized_fallback = _nonnegative_decimal(cold_start_daily_demand, "cold_start_daily_demand")

    supplied = tuple(observations)
    for observation in supplied:
        if observation.sku_id != normalized_sku or observation.location_id != normalized_location:
            raise ValueError("all observations must match the requested sku_id and location_id")

    eligible = tuple(
        sorted(
            (
                observation
                for observation in supplied
                if observation.demand_date < normalized_issue.date()
                and observation.available_at <= normalized_cutoff
            ),
            key=lambda observation: observation.demand_date,
        )
    )
    eligible_dates = [observation.demand_date for observation in eligible]
    if len(eligible_dates) != len(set(eligible_dates)):
        raise ValueError("eligible observations must be unique by demand_date")

    input_content = [
        {
            "sku_id": observation.sku_id,
            "location_id": observation.location_id,
            "demand_date": observation.demand_date.isoformat(),
            "quantity": observation.quantity,
            "available_at": observation.available_at.isoformat(),
        }
        for observation in eligible
    ]
    input_sha256 = _sha256(input_content)

    observed_by_date = {
        observation.demand_date: Decimal(observation.quantity) for observation in eligible
    }
    predicted_by_date: dict[date, Decimal] = {}
    points: list[ForecastPoint] = []
    for offset in range(normalized_horizon):
        target_date = normalized_issue.date() + timedelta(days=offset)
        seasonal_source_date = target_date - timedelta(days=normalized_season)
        point_source_date: date | None = seasonal_source_date
        if seasonal_source_date in observed_by_date:
            prediction = observed_by_date[seasonal_source_date]
            source = ForecastSource.OBSERVED_SEASONAL_LAG
        elif seasonal_source_date in predicted_by_date:
            prediction = predicted_by_date[seasonal_source_date]
            source = ForecastSource.RECURSIVE_SEASONAL_LAG
        else:
            prediction = normalized_fallback
            source = ForecastSource.COLD_START_FALLBACK
            point_source_date = None
        predicted_by_date[target_date] = prediction
        points.append(
            ForecastPoint(
                target_date=target_date,
                predicted_demand=prediction,
                source_date=point_source_date,
                source=source,
            )
        )

    return SeasonalNaiveForecast(
        sku_id=normalized_sku,
        location_id=normalized_location,
        issue_time=normalized_issue,
        history_cutoff=normalized_cutoff,
        horizon_days=normalized_horizon,
        season_length_days=normalized_season,
        cold_start_daily_demand=normalized_fallback,
        eligible_observation_count=len(eligible),
        points=tuple(points),
        input_sha256=input_sha256,
    )
