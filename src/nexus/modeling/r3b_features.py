"""Point-in-time feature and label datasets for the bounded R3B gate.

The module deliberately keeps feature rows, training labels, and privileged
simulator truth in separate types.  R2C's availability-aware readers are the
outer boundary; every historical training example then applies its own earlier
as-of cutoff so a final snapshot cannot leak corrections into the past.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal, cast

from nexus.modeling.r3b_contracts import (
    R3B_DEMAND_FEATURES,
    R3B_FORBIDDEN_FIELDS,
    R3B_LEAD_TIME_FEATURES,
    R3B_MINIMUM_LEARNED_HISTORY_OBSERVATIONS,
    R3BConfig,
)
from nexus.modeling.r3b_data import R3BDataSnapshot, canonical_rows_sha256
from nexus.simulation.representative_supply_network import (
    iter_r2c_dataset_rows,
    iter_r2c_demand_snapshot,
    iter_r2c_lead_time_label_snapshot,
)

DEMAND_FEATURE_COLUMNS = R3B_DEMAND_FEATURES
LEAD_TIME_FEATURE_COLUMNS = R3B_LEAD_TIME_FEATURES
FORBIDDEN_FEATURE_COLUMNS = R3B_FORBIDDEN_FIELDS

type FeatureValue = float | int | None
type ModelKind = Literal["demand", "lead_time"]


class R3BFeatureError(ValueError):
    """A feature dataset violates the frozen R3B point-in-time contract."""


def _aware_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise R3BFeatureError(f"{field} must include an explicit timezone")
    return value.astimezone(UTC)


def _issue_time(day: date, configured_time: str) -> datetime:
    parsed = time.fromisoformat(configured_time)
    return datetime.combine(day, parsed, tzinfo=UTC)


def _contract_issue_time(value: datetime, *, config: R3BConfig, field: str) -> datetime:
    normalized = _aware_utc(value, field)
    if normalized.strftime("%H:%M:%S") != config.forecast_contract.issue_time_utc:
        raise R3BFeatureError(f"{field} must use the frozen R3B UTC issue time")
    return normalized


def _finite_nonnegative(value: float | int, field: str) -> float:
    normalized = float(value)
    if normalized < 0 or normalized != normalized or normalized in {float("inf"), float("-inf")}:
        raise R3BFeatureError(f"{field} must be finite and nonnegative")
    return normalized


def assert_allowed_feature_columns(
    columns: Iterable[str],
    *,
    config: R3BConfig,
    model_kind: ModelKind,
) -> tuple[str, ...]:
    """Require the exact ordered feature vocabulary and reject denylist aliases."""

    normalized = tuple(columns)
    if not normalized or any(not isinstance(item, str) or not item.strip() for item in normalized):
        raise R3BFeatureError("feature columns must be nonblank strings")
    if len(normalized) != len(set(normalized)):
        raise R3BFeatureError("feature columns must be unique")
    expected = (
        tuple(config.feature_contract.allowed_demand_features)
        if model_kind == "demand"
        else tuple(config.feature_contract.allowed_lead_time_features)
    )
    if normalized != expected:
        raise R3BFeatureError(
            f"{model_kind} feature columns must match the frozen ordered contract"
        )
    denied = {item.casefold() for item in config.feature_contract.forbidden_fields}
    overlap = [item for item in normalized if item.casefold() in denied]
    if overlap:
        raise R3BFeatureError(f"forbidden feature columns detected: {', '.join(overlap)}")
    return normalized


@dataclass(frozen=True, slots=True)
class R3BDemandFeatureRow:
    """Label-free direct-horizon demand features at one historical issue time."""

    sku_id: str
    location_id: str
    issue_time: datetime
    target_date: date
    horizon_days: int
    max_feature_available_at: datetime | None
    lag_sales_1: float | None
    lag_sales_7: float | None
    lag_sales_14: float | None
    rolling_mean_sales_7: float | None
    rolling_mean_sales_14: float | None
    rolling_nonzero_rate_7: float | None
    rolling_nonzero_rate_14: float | None
    days_since_nonzero: int | None
    history_count: int
    target_day_of_week: int

    def __post_init__(self) -> None:
        issue = _aware_utc(self.issue_time, "issue_time")
        object.__setattr__(self, "issue_time", issue)
        if self.max_feature_available_at is not None:
            available = _aware_utc(self.max_feature_available_at, "max_feature_available_at")
            if available > issue:
                raise R3BFeatureError("feature evidence cannot be available after issue_time")
            object.__setattr__(self, "max_feature_available_at", available)
        if self.target_date < issue.date():
            raise R3BFeatureError("target_date cannot precede issue_time")
        if self.horizon_days not in {7, 14, 28}:
            raise R3BFeatureError("horizon_days must be one of 7, 14, 28")
        if self.target_date != issue.date() + timedelta(days=self.horizon_days - 1):
            raise R3BFeatureError("target_date must match the direct-horizon issue boundary")
        if self.history_count < 0 or not 0 <= self.target_day_of_week <= 6:
            raise R3BFeatureError("history_count or weekday is invalid")

    @property
    def feature_columns(self) -> tuple[str, ...]:
        return DEMAND_FEATURE_COLUMNS

    def feature_vector(self) -> tuple[FeatureValue, ...]:
        return (
            self.lag_sales_1,
            self.lag_sales_7,
            self.lag_sales_14,
            self.rolling_mean_sales_7,
            self.rolling_mean_sales_14,
            self.rolling_nonzero_rate_7,
            self.rolling_nonzero_rate_14,
            self.days_since_nonzero,
            self.history_count,
            self.target_day_of_week,
            self.horizon_days,
        )

    @property
    def forecast_key(self) -> tuple[str, str, date, int]:
        return self.sku_id, self.location_id, self.target_date, self.horizon_days

    @property
    def cold_start(self) -> bool:
        return self.history_count < R3B_MINIMUM_LEARNED_HISTORY_OBSERVATIONS

    def artifact_row(self) -> dict[str, object]:
        row = asdict(self)
        if any(field in row for field in FORBIDDEN_FEATURE_COLUMNS):
            raise R3BFeatureError("feature artifact contains a forbidden source field")
        return row


@dataclass(frozen=True, slots=True)
class R3BDemandTrainingExample:
    """A feature row composed with one separately governed mature label."""

    features: R3BDemandFeatureRow
    target_observed_sales_qty: float
    label_available_at: datetime

    def __post_init__(self) -> None:
        label_available = _aware_utc(self.label_available_at, "label_available_at")
        object.__setattr__(self, "label_available_at", label_available)
        object.__setattr__(
            self,
            "target_observed_sales_qty",
            _finite_nonnegative(self.target_observed_sales_qty, "target_observed_sales_qty"),
        )

    def identity_row(self) -> dict[str, object]:
        return {
            "features": self.features.artifact_row(),
            "label_available_at": self.label_available_at,
            "target_observed_sales_qty": self.target_observed_sales_qty,
        }


@dataclass(frozen=True, slots=True)
class R3BDemandTrainingDataset:
    outer_fit_origin: datetime
    feature_columns: tuple[str, ...]
    examples: tuple[R3BDemandTrainingExample, ...]
    excluded_inactive_labels: int
    excluded_censored_labels: int
    excluded_immature_labels: int
    dataset_sha256: str

    def feature_matrix(self) -> tuple[tuple[FeatureValue, ...], ...]:
        return tuple(item.features.feature_vector() for item in self.examples)

    def targets(self) -> tuple[float, ...]:
        return tuple(item.target_observed_sales_qty for item in self.examples)


@dataclass(frozen=True, slots=True)
class R3BLeadTimeFeatureRow:
    order_id: str
    supplier_id: str
    origin_time: datetime
    ordered_at: datetime
    ordered_available_at: datetime
    ordered_qty: float
    promised_lead_time_days: float
    order_day_of_week: int
    order_month: int

    def __post_init__(self) -> None:
        origin = _aware_utc(self.origin_time, "origin_time")
        ordered = _aware_utc(self.ordered_at, "ordered_at")
        available = _aware_utc(self.ordered_available_at, "ordered_available_at")
        object.__setattr__(self, "origin_time", origin)
        object.__setattr__(self, "ordered_at", ordered)
        object.__setattr__(self, "ordered_available_at", available)
        if ordered > available or available > origin:
            raise R3BFeatureError(
                "order availability must follow the order and not exceed the modeling origin"
            )
        if not 0 <= self.order_day_of_week <= 6 or not 1 <= self.order_month <= 12:
            raise R3BFeatureError("lead-time calendar feature is invalid")
        _finite_nonnegative(self.ordered_qty, "ordered_qty")
        _finite_nonnegative(self.promised_lead_time_days, "promised_lead_time_days")

    @property
    def feature_columns(self) -> tuple[str, ...]:
        return LEAD_TIME_FEATURE_COLUMNS

    def feature_vector(self) -> tuple[FeatureValue, ...]:
        return (
            self.ordered_qty,
            self.promised_lead_time_days,
            self.order_day_of_week,
            self.order_month,
        )

    def artifact_row(self) -> dict[str, object]:
        return {
            "order_id": self.order_id,
            "ordered_at": self.ordered_at,
            "ordered_available_at": self.ordered_available_at,
            "origin_time": self.origin_time,
            "supplier_id": self.supplier_id,
            **dict(zip(self.feature_columns, self.feature_vector(), strict=True)),
        }


@dataclass(frozen=True, slots=True)
class R3BLeadTimeTrainingExample:
    features: R3BLeadTimeFeatureRow
    target_actual_lead_time_days: float
    label_available_at: datetime

    def __post_init__(self) -> None:
        available = _aware_utc(self.label_available_at, "label_available_at")
        object.__setattr__(self, "label_available_at", available)
        object.__setattr__(
            self,
            "target_actual_lead_time_days",
            _finite_nonnegative(
                self.target_actual_lead_time_days,
                "target_actual_lead_time_days",
            ),
        )

    def identity_row(self) -> dict[str, object]:
        return {
            "features": self.features.artifact_row(),
            "label_available_at": self.label_available_at,
            "target_actual_lead_time_days": self.target_actual_lead_time_days,
        }


@dataclass(frozen=True, slots=True)
class R3BLeadTimeTrainingDataset:
    origin_time: datetime
    feature_columns: tuple[str, ...]
    examples: tuple[R3BLeadTimeTrainingExample, ...]
    known_order_count: int
    excluded_right_censored_count: int
    dataset_sha256: str

    def feature_matrix(self) -> tuple[tuple[FeatureValue, ...], ...]:
        return tuple(item.features.feature_vector() for item in self.examples)

    def targets(self) -> tuple[float, ...]:
        return tuple(item.target_actual_lead_time_days for item in self.examples)


def _read_demand_rows(snapshot: R3BDataSnapshot, cutoff: datetime) -> tuple[dict[str, object], ...]:
    rows = tuple(
        iter_r2c_demand_snapshot(
            snapshot.output_dir,
            planning_cutoff=cutoff,
            verified_manifest=snapshot.manifest,
        )
    )
    keys = [
        (cast(str, row["sku_id"]), cast(str, row["location_id"]), cast(date, row["demand_date"]))
        for row in rows
    ]
    if len(keys) != len(set(keys)):
        raise R3BFeatureError("R2C demand history must be unique at SKU/location/day grain")
    return tuple(
        row
        for _, row in sorted(
            zip(keys, rows, strict=True),
            key=lambda item: item[0],
        )
    )


def _product_locations(snapshot: R3BDataSnapshot) -> tuple[dict[str, object], ...]:
    rows = tuple(
        iter_r2c_dataset_rows(
            snapshot.output_dir,
            dataset="representative.product_locations",
            verified_manifest=snapshot.manifest,
        )
    )
    keys = [(cast(str, row["sku_id"]), cast(str, row["location_id"])) for row in rows]
    if len(keys) != len(set(keys)):
        raise R3BFeatureError("R2C product-location grain must be unique")
    return tuple(row for _, row in sorted(zip(keys, rows, strict=True), key=lambda item: item[0]))


def _feature_row(
    *,
    sku_id: str,
    location_id: str,
    issue: datetime,
    target_date: date,
    horizon: int,
    series_rows: Sequence[Mapping[str, object]],
) -> R3BDemandFeatureRow:
    visible = [
        row
        for row in series_rows
        if cast(date, row["demand_date"]) < issue.date()
        and cast(datetime, row["available_at"]).astimezone(UTC) <= issue
        and cast(bool, row["is_active"])
        and not cast(bool, row["stockout_censored"])
    ]
    by_date = {cast(date, row["demand_date"]): row for row in visible}

    def lag(days: int) -> float | None:
        row = by_date.get(issue.date() - timedelta(days=days))
        return None if row is None else float(cast(int, row["observed_sales_qty"]))

    def window(days: int) -> tuple[float | None, float | None]:
        start = issue.date() - timedelta(days=days)
        values = [
            float(cast(int, row["observed_sales_qty"]))
            for row in visible
            if start <= cast(date, row["demand_date"]) < issue.date()
        ]
        if not values:
            return None, None
        return sum(values) / len(values), sum(value > 0 for value in values) / len(values)

    mean_7, nonzero_7 = window(7)
    mean_14, nonzero_14 = window(14)
    nonzero_dates = [
        cast(date, row["demand_date"])
        for row in visible
        if cast(int, row["observed_sales_qty"]) > 0
    ]
    max_available = max(
        (cast(datetime, row["available_at"]).astimezone(UTC) for row in visible),
        default=None,
    )
    return R3BDemandFeatureRow(
        sku_id=sku_id,
        location_id=location_id,
        issue_time=issue,
        target_date=target_date,
        horizon_days=horizon,
        max_feature_available_at=max_available,
        lag_sales_1=lag(1),
        lag_sales_7=lag(7),
        lag_sales_14=lag(14),
        rolling_mean_sales_7=mean_7,
        rolling_mean_sales_14=mean_14,
        rolling_nonzero_rate_7=nonzero_7,
        rolling_nonzero_rate_14=nonzero_14,
        days_since_nonzero=(issue.date() - max(nonzero_dates)).days if nonzero_dates else None,
        history_count=len(visible),
        target_day_of_week=target_date.weekday(),
    )


def build_demand_training_examples(
    snapshot: R3BDataSnapshot,
    *,
    config: R3BConfig,
    outer_fit_origin: datetime,
) -> R3BDemandTrainingDataset:
    """Build canonical direct-horizon examples with per-example historical cutoffs."""

    origin = _contract_issue_time(
        outer_fit_origin,
        config=config,
        field="outer_fit_origin",
    )
    feature_columns = assert_allowed_feature_columns(
        DEMAND_FEATURE_COLUMNS,
        config=config,
        model_kind="demand",
    )
    rows = _read_demand_rows(snapshot, origin)
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(cast(str, row["sku_id"]), cast(str, row["location_id"]))].append(row)

    inactive = censored = immature = 0
    examples: list[R3BDemandTrainingExample] = []
    for row in rows:
        target_date = cast(date, row["demand_date"])
        label_available = cast(datetime, row["available_at"]).astimezone(UTC)
        if target_date >= origin.date() or label_available > origin:
            immature += 1
            continue
        if not cast(bool, row["is_active"]):
            inactive += 1
            continue
        if cast(bool, row["stockout_censored"]):
            censored += 1
            continue
        sku_id = cast(str, row["sku_id"])
        location_id = cast(str, row["location_id"])
        for horizon in config.forecast_contract.horizons_days:
            sample_day = target_date - timedelta(days=horizon - 1)
            if sample_day < snapshot.manifest.start_date:
                continue
            issue = _issue_time(sample_day, config.forecast_contract.issue_time_utc)
            examples.append(
                R3BDemandTrainingExample(
                    features=_feature_row(
                        sku_id=sku_id,
                        location_id=location_id,
                        issue=issue,
                        target_date=target_date,
                        horizon=horizon,
                        series_rows=grouped[(sku_id, location_id)],
                    ),
                    target_observed_sales_qty=float(cast(int, row["observed_sales_qty"])),
                    label_available_at=label_available,
                )
            )
    ordered = tuple(
        sorted(
            examples,
            key=lambda item: (
                item.features.issue_time,
                item.features.sku_id,
                item.features.location_id,
                item.features.horizon_days,
            ),
        )
    )
    identity_rows = [item.identity_row() for item in ordered]
    return R3BDemandTrainingDataset(
        outer_fit_origin=origin,
        feature_columns=feature_columns,
        examples=ordered,
        excluded_inactive_labels=inactive,
        excluded_censored_labels=censored,
        excluded_immature_labels=immature,
        dataset_sha256=canonical_rows_sha256(identity_rows),
    )


def build_demand_forecast_rows(
    snapshot: R3BDataSnapshot,
    *,
    config: R3BConfig,
    issue_time: datetime,
) -> tuple[R3BDemandFeatureRow, ...]:
    """Build one label-free 7/14/28 prediction grid from origin-visible facts."""

    issue = _contract_issue_time(issue_time, config=config, field="issue_time")
    assert_allowed_feature_columns(DEMAND_FEATURE_COLUMNS, config=config, model_kind="demand")
    rows = _read_demand_rows(snapshot, issue)
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(cast(str, row["sku_id"]), cast(str, row["location_id"]))].append(row)
    result: list[R3BDemandFeatureRow] = []
    for product in _product_locations(snapshot):
        sku_id = cast(str, product["sku_id"])
        location_id = cast(str, product["location_id"])
        available_at = cast(datetime, product["available_at"]).astimezone(UTC)
        if available_at > issue or cast(date, product["activation_date"]) > issue.date():
            continue
        for horizon in config.forecast_contract.horizons_days:
            target = issue.date() + timedelta(days=horizon - 1)
            result.append(
                _feature_row(
                    sku_id=sku_id,
                    location_id=location_id,
                    issue=issue,
                    target_date=target,
                    horizon=horizon,
                    series_rows=grouped[(sku_id, location_id)],
                )
            )
    return tuple(sorted(result, key=lambda row: (row.sku_id, row.location_id, row.horizon_days)))


def build_lead_time_training_examples(
    snapshot: R3BDataSnapshot,
    *,
    config: R3BConfig,
    origin_time: datetime,
) -> R3BLeadTimeTrainingDataset:
    """Build mature-only lead-time examples and count every censored exclusion."""

    origin = _contract_issue_time(origin_time, config=config, field="origin_time")
    feature_columns = assert_allowed_feature_columns(
        LEAD_TIME_FEATURE_COLUMNS,
        config=config,
        model_kind="lead_time",
    )
    rows = tuple(
        iter_r2c_lead_time_label_snapshot(
            snapshot.output_dir,
            planning_cutoff=origin,
            verified_manifest=snapshot.manifest,
        )
    )
    order_ids = [cast(str, row["order_id"]) for row in rows]
    if len(order_ids) != len(set(order_ids)):
        raise R3BFeatureError("lead-time snapshot order IDs must be unique")
    examples: list[R3BLeadTimeTrainingExample] = []
    censored = 0
    for row in rows:
        if cast(bool, row["right_censored"]):
            censored += 1
            continue
        target = cast(float | None, row["actual_lead_time_days"])
        label_available = cast(datetime | None, row["label_available_at"])
        if target is None or label_available is None:
            raise R3BFeatureError("uncensored lead-time row is missing its mature label")
        ordered = cast(datetime, row["ordered_at"]).astimezone(UTC)
        ordered_available = cast(datetime, row["ordered_available_at"]).astimezone(UTC)
        expected = cast(datetime, row["expected_at"]).astimezone(UTC)
        promised = (expected - ordered).total_seconds() / 86_400
        features = R3BLeadTimeFeatureRow(
            order_id=cast(str, row["order_id"]),
            supplier_id=cast(str, row["supplier_id"]),
            origin_time=origin,
            ordered_at=ordered,
            ordered_available_at=ordered_available,
            ordered_qty=float(cast(int, row["ordered_qty"])),
            promised_lead_time_days=promised,
            order_day_of_week=ordered.weekday(),
            order_month=ordered.month,
        )
        examples.append(
            R3BLeadTimeTrainingExample(
                features=features,
                target_actual_lead_time_days=target,
                label_available_at=label_available,
            )
        )
    ordered_examples = tuple(sorted(examples, key=lambda item: item.features.order_id))
    return R3BLeadTimeTrainingDataset(
        origin_time=origin,
        feature_columns=feature_columns,
        examples=ordered_examples,
        known_order_count=len(rows),
        excluded_right_censored_count=censored,
        dataset_sha256=canonical_rows_sha256([item.identity_row() for item in ordered_examples]),
    )


def write_feature_snapshot(path: Path, rows: Sequence[R3BDemandFeatureRow]) -> str:
    """Write a deterministic label-free JSON snapshot for debugging and education."""

    import json

    payload_rows = [row.artifact_row() for row in rows]
    digest = canonical_rows_sha256(payload_rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload_rows, default=str, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return digest


__all__ = [
    "DEMAND_FEATURE_COLUMNS",
    "FORBIDDEN_FEATURE_COLUMNS",
    "LEAD_TIME_FEATURE_COLUMNS",
    "R3BDemandFeatureRow",
    "R3BDemandTrainingDataset",
    "R3BDemandTrainingExample",
    "R3BFeatureError",
    "R3BLeadTimeFeatureRow",
    "R3BLeadTimeTrainingDataset",
    "R3BLeadTimeTrainingExample",
    "assert_allowed_feature_columns",
    "build_demand_forecast_rows",
    "build_demand_training_examples",
    "build_lead_time_training_examples",
    "write_feature_snapshot",
]
