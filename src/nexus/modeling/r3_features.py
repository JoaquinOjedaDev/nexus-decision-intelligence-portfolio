"""Versioned point-in-time features for the bounded R3A intelligence gate."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

import pyarrow as pa

from nexus.modeling.gold_adapter import R3DemandObservation
from nexus.modeling.r3_contracts import R3AConfig, R3RollingOrigin, normalize_r3_utc


class R3FeatureError(ValueError):
    """A feature row cannot be produced without crossing the scientific contract."""


FEATURE_ARTIFACT_SCHEMA = pa.schema(
    [
        pa.field("feature_version", pa.string(), nullable=False),
        pa.field("origin_name", pa.string(), nullable=False),
        pa.field("issue_time", pa.string(), nullable=False),
        pa.field("sku_location_key", pa.string(), nullable=False),
        pa.field("sku_id", pa.string(), nullable=False),
        pa.field("location_id", pa.string(), nullable=False),
        pa.field("horizon_day", pa.int32(), nullable=False),
        pa.field("target_date", pa.string(), nullable=False),
        pa.field("history_count", pa.int64(), nullable=False),
        pa.field("max_feature_available_at", pa.string(), nullable=True),
        pa.field("lag_1", pa.int64(), nullable=True),
        pa.field("lag_7", pa.int64(), nullable=True),
        pa.field("lag_14", pa.int64(), nullable=True),
        pa.field("seasonal_target_lag_7", pa.int64(), nullable=True),
        pa.field("rolling_mean_7", pa.string(), nullable=True),
        pa.field("rolling_mean_14", pa.string(), nullable=True),
        pa.field("rolling_nonzero_rate_7", pa.string(), nullable=True),
        pa.field("rolling_nonzero_rate_14", pa.string(), nullable=True),
        pa.field("days_since_nonzero", pa.int64(), nullable=True),
        pa.field("target_day_of_week", pa.int32(), nullable=False),
    ],
    metadata={b"nexus.contract": b"r3.point-in-time-features.v1"},
)


@dataclass(frozen=True, slots=True)
class R3PointInTimeFeatureRow:
    """One model-ready row whose contributing facts are bounded by an issue time."""

    feature_version: str
    origin_name: str
    issue_time: datetime
    sku_id: str
    location_id: str
    horizon_day: int
    target_date: date
    history_count: int
    max_feature_available_at: datetime | None
    lag_1: int | None
    lag_7: int | None
    lag_14: int | None
    seasonal_target_lag_7: int | None
    rolling_mean_7: Decimal | None
    rolling_mean_14: Decimal | None
    rolling_nonzero_rate_7: Decimal | None
    rolling_nonzero_rate_14: Decimal | None
    days_since_nonzero: int | None
    target_day_of_week: int

    def __post_init__(self) -> None:
        normalized_issue = normalize_r3_utc(self.issue_time)
        object.__setattr__(self, "issue_time", normalized_issue)
        if not self.feature_version.strip() or not self.origin_name.strip():
            raise R3FeatureError("feature version and origin name must not be blank")
        if not self.sku_id.strip() or not self.location_id.strip():
            raise R3FeatureError("feature grain identifiers must not be blank")
        if self.horizon_day <= 0:
            raise R3FeatureError("horizon_day must be positive")
        expected_target = normalized_issue.date() + timedelta(days=self.horizon_day - 1)
        if self.target_date != expected_target:
            raise R3FeatureError("target_date does not match issue_time and horizon_day")
        if self.history_count < 0:
            raise R3FeatureError("history_count must be nonnegative")
        if self.max_feature_available_at is not None:
            normalized_max = normalize_r3_utc(self.max_feature_available_at)
            object.__setattr__(self, "max_feature_available_at", normalized_max)
            if normalized_max > normalized_issue:
                raise R3FeatureError("feature evidence crosses its issue time")
        for name in ("lag_1", "lag_7", "lag_14", "seasonal_target_lag_7"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise R3FeatureError(f"{name} must be nonnegative when present")
        for name in (
            "rolling_mean_7",
            "rolling_mean_14",
            "rolling_nonzero_rate_7",
            "rolling_nonzero_rate_14",
        ):
            value = getattr(self, name)
            if value is not None and (not value.is_finite() or value < 0):
                raise R3FeatureError(f"{name} must be finite and nonnegative")
        for name in ("rolling_nonzero_rate_7", "rolling_nonzero_rate_14"):
            value = getattr(self, name)
            if value is not None and value > 1:
                raise R3FeatureError(f"{name} cannot exceed one")
        if self.days_since_nonzero is not None and self.days_since_nonzero <= 0:
            raise R3FeatureError("days_since_nonzero must be positive when present")
        if not 0 <= self.target_day_of_week <= 6:
            raise R3FeatureError("target_day_of_week must be in [0, 6]")

    @property
    def sku_location_key(self) -> str:
        return f"{self.sku_id}::{self.location_id}"


def _stable_observations(
    observations: Iterable[R3DemandObservation],
) -> tuple[R3DemandObservation, ...]:
    rows = tuple(
        sorted(
            observations,
            key=lambda row: (row.sku_id, row.location_id, row.demand_date),
        )
    )
    keys = [(row.sku_id, row.location_id, row.demand_date) for row in rows]
    if len(keys) != len(set(keys)):
        raise R3FeatureError("observations must be unique by sku-location-date")
    return rows


def eligible_history(
    observations: Sequence[R3DemandObservation],
    *,
    origin: R3RollingOrigin,
) -> tuple[R3DemandObservation, ...]:
    """Return facts knowable at an origin using both business and availability time."""

    issue_time = normalize_r3_utc(origin.issue_time)
    return tuple(
        row
        for row in observations
        if row.demand_date < issue_time.date() and row.latest_available_at <= issue_time
    )


def _mean(rows: Sequence[R3DemandObservation]) -> Decimal | None:
    if not rows:
        return None
    return Decimal(sum(row.demand_qty for row in rows)) / Decimal(len(rows))


def _nonzero_rate(rows: Sequence[R3DemandObservation]) -> Decimal | None:
    if not rows:
        return None
    return Decimal(sum(row.demand_qty > 0 for row in rows)) / Decimal(len(rows))


def _window(
    rows: Sequence[R3DemandObservation],
    *,
    issue_date: date,
    days: int,
) -> tuple[R3DemandObservation, ...]:
    lower = issue_date - timedelta(days=days)
    return tuple(row for row in rows if lower <= row.demand_date < issue_date)


def build_features_for_origin(
    observations: Iterable[R3DemandObservation],
    *,
    origin: R3RollingOrigin,
    horizon_days: int,
    feature_version: str = "r3a-pit-v1",
) -> tuple[R3PointInTimeFeatureRow, ...]:
    """Shared label-free calculation for one explicitly declared rolling origin."""
    if type(horizon_days) is not int or horizon_days <= 0:
        raise R3FeatureError("horizon_days must be a positive integer")

    grouped: dict[tuple[str, str], list[R3DemandObservation]] = defaultdict(list)
    for observation in _stable_observations(observations):
        grouped[(observation.sku_id, observation.location_id)].append(observation)
    if not grouped:
        raise R3FeatureError("at least one demand observation is required")

    features: list[R3PointInTimeFeatureRow] = []
    issue_date = origin.issue_time.date()
    for (sku_id, location_id), grain_rows in sorted(grouped.items()):
        history = eligible_history(grain_rows, origin=origin)
        history_by_date = {row.demand_date: row for row in history}
        maximum_availability = max(
            (row.latest_available_at for row in history),
            default=None,
        )
        window_7 = _window(history, issue_date=issue_date, days=7)
        window_14 = _window(history, issue_date=issue_date, days=14)
        nonzero_dates = [row.demand_date for row in history if row.demand_qty > 0]
        days_since_nonzero = (issue_date - max(nonzero_dates)).days if nonzero_dates else None

        issue_lags = {
            lag: (
                history_by_date[issue_date - timedelta(days=lag)].demand_qty
                if issue_date - timedelta(days=lag) in history_by_date
                else None
            )
            for lag in (1, 7, 14)
        }
        for horizon_day in range(1, horizon_days + 1):
            target_date = issue_date + timedelta(days=horizon_day - 1)
            seasonal_date = target_date - timedelta(days=7)
            seasonal = history_by_date.get(seasonal_date)
            features.append(
                R3PointInTimeFeatureRow(
                    feature_version=feature_version,
                    origin_name=origin.name,
                    issue_time=origin.issue_time,
                    sku_id=sku_id,
                    location_id=location_id,
                    horizon_day=horizon_day,
                    target_date=target_date,
                    history_count=len(history),
                    max_feature_available_at=maximum_availability,
                    lag_1=issue_lags[1],
                    lag_7=issue_lags[7],
                    lag_14=issue_lags[14],
                    seasonal_target_lag_7=(None if seasonal is None else seasonal.demand_qty),
                    rolling_mean_7=_mean(window_7),
                    rolling_mean_14=_mean(window_14),
                    rolling_nonzero_rate_7=_nonzero_rate(window_7),
                    rolling_nonzero_rate_14=_nonzero_rate(window_14),
                    days_since_nonzero=days_since_nonzero,
                    target_day_of_week=target_date.weekday(),
                )
            )

    return tuple(
        sorted(
            features,
            key=lambda row: (
                row.issue_time,
                row.sku_id,
                row.location_id,
                row.horizon_day,
            ),
        )
    )


def build_point_in_time_features(
    observations: Iterable[R3DemandObservation],
    *,
    config: R3AConfig,
) -> tuple[R3PointInTimeFeatureRow, ...]:
    """Build the unchanged R3A artifact through the shared one-origin primitive."""
    if config.feature_contract.lags_days != (1, 7, 14):
        raise R3FeatureError("r3a-pit-v1 requires lags [1, 7, 14]")
    if config.feature_contract.rolling_windows_days != (7, 14):
        raise R3FeatureError("r3a-pit-v1 requires rolling windows [7, 14]")
    if config.forecast_contract.season_length_days != 7:
        raise R3FeatureError("r3a-pit-v1 requires a seven-day season")
    rows = tuple(observations)
    return tuple(
        feature
        for origin in config.evaluation.rolling_origins
        for feature in build_features_for_origin(
            rows,
            origin=origin,
            horizon_days=config.forecast_contract.horizon_days,
            feature_version=config.feature_contract.version,
        )
    )


def feature_artifact_rows(
    rows: Sequence[R3PointInTimeFeatureRow],
) -> list[dict[str, object]]:
    """Return canonical, label-free rows suitable for content-addressed Parquet."""

    def decimal_text(value: Decimal | None) -> str | None:
        if value is None:
            return None
        return "0" if value == 0 else format(value.normalize(), "f")

    return [
        {
            "feature_version": row.feature_version,
            "origin_name": row.origin_name,
            "issue_time": row.issue_time.isoformat().replace("+00:00", "Z"),
            "sku_location_key": row.sku_location_key,
            "sku_id": row.sku_id,
            "location_id": row.location_id,
            "horizon_day": row.horizon_day,
            "target_date": row.target_date.isoformat(),
            "history_count": row.history_count,
            "max_feature_available_at": (
                None
                if row.max_feature_available_at is None
                else row.max_feature_available_at.isoformat().replace("+00:00", "Z")
            ),
            "lag_1": row.lag_1,
            "lag_7": row.lag_7,
            "lag_14": row.lag_14,
            "seasonal_target_lag_7": row.seasonal_target_lag_7,
            "rolling_mean_7": decimal_text(row.rolling_mean_7),
            "rolling_mean_14": decimal_text(row.rolling_mean_14),
            "rolling_nonzero_rate_7": decimal_text(row.rolling_nonzero_rate_7),
            "rolling_nonzero_rate_14": decimal_text(row.rolling_nonzero_rate_14),
            "days_since_nonzero": row.days_since_nonzero,
            "target_day_of_week": row.target_day_of_week,
        }
        for row in rows
    ]


__all__ = [
    "FEATURE_ARTIFACT_SCHEMA",
    "R3FeatureError",
    "R3PointInTimeFeatureRow",
    "build_features_for_origin",
    "build_point_in_time_features",
    "eligible_history",
    "feature_artifact_rows",
]
