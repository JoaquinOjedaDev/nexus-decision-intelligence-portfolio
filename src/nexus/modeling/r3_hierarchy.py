"""Deterministic bottom-up hierarchy helpers for R3 contract-smoke forecasts.

The aggregation in this module deliberately sums marginal quantiles.  That makes
the hierarchy arithmetically coherent and is useful for contract, provenance,
and pipeline tests, but it is *not* a claim that the resulting aggregate
quantiles are calibrated samples from a joint demand distribution.  Full R3
calibration must evaluate the reconciled aggregate distributions separately.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from itertools import pairwise


class HierarchyValidationError(ValueError):
    """A bottom-level grid or hierarchical result violates its frozen contract."""


class HierarchyLevel(StrEnum):
    """The three supported levels, from the decision grain to the network."""

    SKU_LOCATION = "sku_location"
    SKU = "sku"
    NETWORK = "network"


_LEVEL_ORDER = {
    HierarchyLevel.SKU_LOCATION: 0,
    HierarchyLevel.SKU: 1,
    HierarchyLevel.NETWORK: 2,
}


def _identifier(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise HierarchyValidationError(f"{name} must not be blank")
    return normalized


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise HierarchyValidationError(f"{name} must include a timezone offset")
    return value.astimezone(UTC)


def _target_date(value: date) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise HierarchyValidationError("target_date must be a date, not a datetime")
    return value


def _finite_nonnegative(value: Decimal, name: str) -> Decimal:
    if isinstance(value, bool):
        raise HierarchyValidationError(f"{name} must be a decimal number")
    try:
        normalized = value if isinstance(value, Decimal) else Decimal(str(value))
    except Exception as exc:  # pragma: no cover - defensive for runtime callers
        raise HierarchyValidationError(f"{name} must be a decimal number") from exc
    if not normalized.is_finite() or normalized < 0:
        raise HierarchyValidationError(f"{name} must be finite and nonnegative")
    return normalized


def _quantile(value: Decimal) -> Decimal:
    normalized = _finite_nonnegative(value, "quantile")
    if normalized > 1:
        raise HierarchyValidationError("quantile must be between 0 and 1 inclusive")
    return normalized


def _validate_issue_target(issue_time: datetime, target_date: date) -> tuple[datetime, date]:
    normalized_issue = _aware_utc(issue_time, "issue_time")
    normalized_target = _target_date(target_date)
    if normalized_target < normalized_issue.date():
        raise HierarchyValidationError("target_date cannot be before the issue date")
    return normalized_issue, normalized_target


@dataclass(frozen=True, slots=True)
class BottomLevelQuantileForecast:
    """One immutable marginal forecast at the SKU/location decision grain."""

    issue_time: datetime
    target_date: date
    sku_id: str
    location_id: str
    quantile: Decimal
    predicted_quantity: Decimal

    def __post_init__(self) -> None:
        issue_time, target_date = _validate_issue_target(self.issue_time, self.target_date)
        object.__setattr__(self, "issue_time", issue_time)
        object.__setattr__(self, "target_date", target_date)
        object.__setattr__(self, "sku_id", _identifier(self.sku_id, "sku_id"))
        object.__setattr__(self, "location_id", _identifier(self.location_id, "location_id"))
        object.__setattr__(self, "quantile", _quantile(self.quantile))
        object.__setattr__(
            self,
            "predicted_quantity",
            _finite_nonnegative(self.predicted_quantity, "predicted_quantity"),
        )


@dataclass(frozen=True, slots=True)
class BottomLevelActual:
    """One immutable actual aligned to a forecast issue and target date."""

    issue_time: datetime
    target_date: date
    sku_id: str
    location_id: str
    actual_quantity: Decimal

    def __post_init__(self) -> None:
        issue_time, target_date = _validate_issue_target(self.issue_time, self.target_date)
        object.__setattr__(self, "issue_time", issue_time)
        object.__setattr__(self, "target_date", target_date)
        object.__setattr__(self, "sku_id", _identifier(self.sku_id, "sku_id"))
        object.__setattr__(self, "location_id", _identifier(self.location_id, "location_id"))
        object.__setattr__(
            self,
            "actual_quantity",
            _finite_nonnegative(self.actual_quantity, "actual_quantity"),
        )


def _validate_node(level: HierarchyLevel, sku_id: str | None, location_id: str | None) -> None:
    if level is HierarchyLevel.SKU_LOCATION:
        if sku_id is None or location_id is None:
            raise HierarchyValidationError("sku_location rows require sku_id and location_id")
    elif level is HierarchyLevel.SKU:
        if sku_id is None or location_id is not None:
            raise HierarchyValidationError("sku rows require sku_id and no location_id")
    elif sku_id is not None or location_id is not None:
        raise HierarchyValidationError("network rows cannot carry sku_id or location_id")


@dataclass(frozen=True, slots=True)
class HierarchicalQuantileForecast:
    """One immutable forecast row at a declared hierarchy level."""

    issue_time: datetime
    target_date: date
    level: HierarchyLevel
    sku_id: str | None
    location_id: str | None
    quantile: Decimal
    predicted_quantity: Decimal

    def __post_init__(self) -> None:
        issue_time, target_date = _validate_issue_target(self.issue_time, self.target_date)
        sku_id = None if self.sku_id is None else _identifier(self.sku_id, "sku_id")
        location_id = (
            None if self.location_id is None else _identifier(self.location_id, "location_id")
        )
        _validate_node(self.level, sku_id, location_id)
        object.__setattr__(self, "issue_time", issue_time)
        object.__setattr__(self, "target_date", target_date)
        object.__setattr__(self, "sku_id", sku_id)
        object.__setattr__(self, "location_id", location_id)
        object.__setattr__(self, "quantile", _quantile(self.quantile))
        object.__setattr__(
            self,
            "predicted_quantity",
            _finite_nonnegative(self.predicted_quantity, "predicted_quantity"),
        )


@dataclass(frozen=True, slots=True)
class HierarchicalActual:
    """One immutable actual row at a declared hierarchy level."""

    issue_time: datetime
    target_date: date
    level: HierarchyLevel
    sku_id: str | None
    location_id: str | None
    actual_quantity: Decimal

    def __post_init__(self) -> None:
        issue_time, target_date = _validate_issue_target(self.issue_time, self.target_date)
        sku_id = None if self.sku_id is None else _identifier(self.sku_id, "sku_id")
        location_id = (
            None if self.location_id is None else _identifier(self.location_id, "location_id")
        )
        _validate_node(self.level, sku_id, location_id)
        object.__setattr__(self, "issue_time", issue_time)
        object.__setattr__(self, "target_date", target_date)
        object.__setattr__(self, "sku_id", sku_id)
        object.__setattr__(self, "location_id", location_id)
        object.__setattr__(
            self,
            "actual_quantity",
            _finite_nonnegative(self.actual_quantity, "actual_quantity"),
        )


def _forecast_sort_key(
    row: BottomLevelQuantileForecast | HierarchicalQuantileForecast,
) -> tuple[datetime, date, int, str, str, Decimal]:
    level = (
        HierarchyLevel.SKU_LOCATION if isinstance(row, BottomLevelQuantileForecast) else row.level
    )
    return (
        row.issue_time,
        row.target_date,
        _LEVEL_ORDER[level],
        row.sku_id or "",
        row.location_id or "",
        row.quantile,
    )


def _actual_sort_key(
    row: BottomLevelActual | HierarchicalActual,
) -> tuple[datetime, date, int, str, str]:
    level = HierarchyLevel.SKU_LOCATION if isinstance(row, BottomLevelActual) else row.level
    return (
        row.issue_time,
        row.target_date,
        _LEVEL_ORDER[level],
        row.sku_id or "",
        row.location_id or "",
    )


def _validate_bottom_forecasts(rows: Sequence[BottomLevelQuantileForecast]) -> None:
    seen: set[tuple[datetime, date, str, str, Decimal]] = set()
    series: dict[tuple[datetime, date, str, str], list[tuple[Decimal, Decimal]]] = defaultdict(list)
    for row in rows:
        key = (row.issue_time, row.target_date, row.sku_id, row.location_id, row.quantile)
        if key in seen:
            raise HierarchyValidationError(f"duplicate bottom-level forecast key: {key!r}")
        seen.add(key)
        series[key[:-1]].append((row.quantile, row.predicted_quantity))

    grids_by_origin: dict[tuple[datetime, date], tuple[Decimal, ...]] = {}
    for series_key, points in sorted(series.items()):
        ordered = sorted(points)
        quantiles = tuple(quantile for quantile, _ in ordered)
        if any(left >= right for left, right in pairwise(quantiles)):
            raise HierarchyValidationError(
                f"quantile keys must be strictly increasing for {series_key!r}"
            )
        if any(left_value > right_value for (_, left_value), (_, right_value) in pairwise(ordered)):
            raise HierarchyValidationError(
                f"crossing quantiles for bottom-level series {series_key!r}"
            )
        origin = series_key[:2]
        prior = grids_by_origin.setdefault(origin, quantiles)
        if prior != quantiles:
            raise HierarchyValidationError(
                f"all bottom-level series at {origin!r} must share one quantile grid"
            )


def aggregate_quantile_forecasts(
    records: Iterable[BottomLevelQuantileForecast],
) -> tuple[HierarchicalQuantileForecast, ...]:
    """Aggregate SKU/location marginal quantiles to SKU and network totals.

    Rows are sorted before Decimal summation, so equivalent input order produces
    byte-for-byte equivalent logical results.  Summing marginal quantiles is only
    a hierarchy contract-smoke approximation; it does not establish calibrated
    aggregate uncertainty for correlated demand.
    """

    bottom = tuple(sorted(records, key=_forecast_sort_key))
    _validate_bottom_forecasts(bottom)

    rows: list[HierarchicalQuantileForecast] = [
        HierarchicalQuantileForecast(
            issue_time=row.issue_time,
            target_date=row.target_date,
            level=HierarchyLevel.SKU_LOCATION,
            sku_id=row.sku_id,
            location_id=row.location_id,
            quantile=row.quantile,
            predicted_quantity=row.predicted_quantity,
        )
        for row in bottom
    ]
    sku_totals: dict[tuple[datetime, date, str, Decimal], Decimal] = defaultdict(Decimal)
    for row in bottom:
        sku_totals[(row.issue_time, row.target_date, row.sku_id, row.quantile)] += (
            row.predicted_quantity
        )
    sku_rows: list[HierarchicalQuantileForecast] = []
    for (issue_time, target_date, sku_id, quantile), quantity in sorted(sku_totals.items()):
        sku_rows.append(
            HierarchicalQuantileForecast(
                issue_time=issue_time,
                target_date=target_date,
                level=HierarchyLevel.SKU,
                sku_id=sku_id,
                location_id=None,
                quantile=quantile,
                predicted_quantity=quantity,
            )
        )
    rows.extend(sku_rows)

    network_totals: dict[tuple[datetime, date, Decimal], Decimal] = defaultdict(Decimal)
    for sku_row in sku_rows:
        network_totals[(sku_row.issue_time, sku_row.target_date, sku_row.quantile)] += (
            sku_row.predicted_quantity
        )
    for (issue_time, target_date, quantile), quantity in sorted(network_totals.items()):
        rows.append(
            HierarchicalQuantileForecast(
                issue_time=issue_time,
                target_date=target_date,
                level=HierarchyLevel.NETWORK,
                sku_id=None,
                location_id=None,
                quantile=quantile,
                predicted_quantity=quantity,
            )
        )

    result = tuple(sorted(rows, key=_forecast_sort_key))
    validate_bottom_up_coherence(result)
    return result


def aggregate_actuals(records: Iterable[BottomLevelActual]) -> tuple[HierarchicalActual, ...]:
    """Aggregate immutable SKU/location actuals to SKU and network totals."""

    bottom = tuple(sorted(records, key=_actual_sort_key))
    seen: set[tuple[datetime, date, str, str]] = set()
    for row in bottom:
        key = (row.issue_time, row.target_date, row.sku_id, row.location_id)
        if key in seen:
            raise HierarchyValidationError(f"duplicate bottom-level actual key: {key!r}")
        seen.add(key)

    rows: list[HierarchicalActual] = [
        HierarchicalActual(
            issue_time=row.issue_time,
            target_date=row.target_date,
            level=HierarchyLevel.SKU_LOCATION,
            sku_id=row.sku_id,
            location_id=row.location_id,
            actual_quantity=row.actual_quantity,
        )
        for row in bottom
    ]
    sku_totals: dict[tuple[datetime, date, str], Decimal] = defaultdict(Decimal)
    for row in bottom:
        sku_totals[(row.issue_time, row.target_date, row.sku_id)] += row.actual_quantity
    sku_rows: list[HierarchicalActual] = []
    for (issue_time, target_date, sku_id), quantity in sorted(sku_totals.items()):
        sku_rows.append(
            HierarchicalActual(
                issue_time=issue_time,
                target_date=target_date,
                level=HierarchyLevel.SKU,
                sku_id=sku_id,
                location_id=None,
                actual_quantity=quantity,
            )
        )
    rows.extend(sku_rows)

    network_totals: dict[tuple[datetime, date], Decimal] = defaultdict(Decimal)
    for sku_row in sku_rows:
        network_totals[(sku_row.issue_time, sku_row.target_date)] += sku_row.actual_quantity
    for (issue_time, target_date), quantity in sorted(network_totals.items()):
        rows.append(
            HierarchicalActual(
                issue_time=issue_time,
                target_date=target_date,
                level=HierarchyLevel.NETWORK,
                sku_id=None,
                location_id=None,
                actual_quantity=quantity,
            )
        )
    return tuple(sorted(rows, key=_actual_sort_key))


def validate_bottom_up_coherence(rows: Sequence[HierarchicalQuantileForecast]) -> None:
    """Raise when hierarchical rows are incomplete, crossed, duplicated, or incoherent."""

    by_key: dict[
        tuple[datetime, date, HierarchyLevel, str | None, str | None, Decimal], Decimal
    ] = {}
    node_series: dict[
        tuple[datetime, date, HierarchyLevel, str | None, str | None],
        list[tuple[Decimal, Decimal]],
    ] = defaultdict(list)
    for row in rows:
        key = (
            row.issue_time,
            row.target_date,
            row.level,
            row.sku_id,
            row.location_id,
            row.quantile,
        )
        if key in by_key:
            raise HierarchyValidationError(f"duplicate hierarchical forecast key: {key!r}")
        by_key[key] = row.predicted_quantity
        node_series[key[:-1]].append((row.quantile, row.predicted_quantity))

    for node_key, points in sorted(node_series.items(), key=lambda item: repr(item[0])):
        ordered = sorted(points)
        quantiles = tuple(quantile for quantile, _ in ordered)
        if any(left >= right for left, right in pairwise(quantiles)):
            raise HierarchyValidationError(
                f"quantile keys must be strictly increasing for {node_key!r}"
            )
        if any(left_value > right_value for (_, left_value), (_, right_value) in pairwise(ordered)):
            raise HierarchyValidationError(f"crossing hierarchical quantiles for {node_key!r}")

    bottom = [row for row in rows if row.level is HierarchyLevel.SKU_LOCATION]
    expected_sku: dict[tuple[datetime, date, str, Decimal], Decimal] = defaultdict(Decimal)
    for row in bottom:
        assert row.sku_id is not None
        expected_sku[(row.issue_time, row.target_date, row.sku_id, row.quantile)] += (
            row.predicted_quantity
        )
    actual_sku = {
        (row.issue_time, row.target_date, row.sku_id, row.quantile): row.predicted_quantity
        for row in rows
        if row.level is HierarchyLevel.SKU
    }
    if dict(expected_sku) != actual_sku:
        raise HierarchyValidationError("SKU aggregates do not equal their bottom-level sums")

    expected_network: dict[tuple[datetime, date, Decimal], Decimal] = defaultdict(Decimal)
    for (issue_time, target_date, _sku_id, quantile), quantity in expected_sku.items():
        expected_network[(issue_time, target_date, quantile)] += quantity
    actual_network = {
        (row.issue_time, row.target_date, row.quantile): row.predicted_quantity
        for row in rows
        if row.level is HierarchyLevel.NETWORK
    }
    if dict(expected_network) != actual_network:
        raise HierarchyValidationError("network aggregates do not equal their SKU sums")
