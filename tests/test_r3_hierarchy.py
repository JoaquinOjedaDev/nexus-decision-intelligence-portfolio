from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from nexus.modeling.r3_hierarchy import (
    BottomLevelActual,
    BottomLevelQuantileForecast,
    HierarchicalQuantileForecast,
    HierarchyLevel,
    HierarchyValidationError,
    aggregate_actuals,
    aggregate_quantile_forecasts,
    validate_bottom_up_coherence,
)

ISSUE = datetime(2026, 7, 1, 9, tzinfo=UTC)
TARGET = date(2026, 7, 2)


def _forecast_rows() -> tuple[BottomLevelQuantileForecast, ...]:
    values = {
        ("SKU-A", "LOC-1"): ("1", "2", "3"),
        ("SKU-A", "LOC-2"): ("2", "4", "6"),
        ("SKU-B", "LOC-1"): ("3", "5", "7"),
    }
    quantiles = (Decimal("0.1"), Decimal("0.5"), Decimal("0.9"))
    return tuple(
        BottomLevelQuantileForecast(
            issue_time=ISSUE,
            target_date=TARGET,
            sku_id=sku_id,
            location_id=location_id,
            quantile=quantile,
            predicted_quantity=Decimal(quantity),
        )
        for (sku_id, location_id), quantities in values.items()
        for quantile, quantity in zip(quantiles, quantities, strict=True)
    )


def test_bottom_up_forecast_aggregation_is_order_invariant_and_coherent() -> None:
    source = _forecast_rows()
    first = aggregate_quantile_forecasts(source)
    second = aggregate_quantile_forecasts(reversed(source))

    assert first == second
    assert len(first) == 18
    validate_bottom_up_coherence(first)

    sku_a = {
        row.quantile: row.predicted_quantity
        for row in first
        if row.level is HierarchyLevel.SKU and row.sku_id == "SKU-A"
    }
    network = {
        row.quantile: row.predicted_quantity for row in first if row.level is HierarchyLevel.NETWORK
    }
    assert sku_a == {
        Decimal("0.1"): Decimal("3"),
        Decimal("0.5"): Decimal("6"),
        Decimal("0.9"): Decimal("9"),
    }
    assert network == {
        Decimal("0.1"): Decimal("6"),
        Decimal("0.5"): Decimal("11"),
        Decimal("0.9"): Decimal("16"),
    }


def test_bottom_up_actual_aggregation_is_order_invariant() -> None:
    rows = (
        BottomLevelActual(ISSUE, TARGET, "SKU-A", "LOC-1", Decimal("3")),
        BottomLevelActual(ISSUE, TARGET, "SKU-A", "LOC-2", Decimal("4")),
        BottomLevelActual(ISSUE, TARGET, "SKU-B", "LOC-1", Decimal("8")),
    )
    first = aggregate_actuals(rows)
    second = aggregate_actuals(reversed(rows))

    assert first == second
    assert [row.actual_quantity for row in first if row.level is HierarchyLevel.NETWORK] == [
        Decimal("15")
    ]


def test_crossing_quantiles_and_inconsistent_grids_fail_closed() -> None:
    crossing = (
        BottomLevelQuantileForecast(ISSUE, TARGET, "SKU-A", "LOC-1", Decimal("0.1"), Decimal("5")),
        BottomLevelQuantileForecast(ISSUE, TARGET, "SKU-A", "LOC-1", Decimal("0.9"), Decimal("4")),
    )
    with pytest.raises(HierarchyValidationError, match="crossing quantiles"):
        aggregate_quantile_forecasts(crossing)

    inconsistent = (
        *crossing[:1],
        BottomLevelQuantileForecast(ISSUE, TARGET, "SKU-A", "LOC-1", Decimal("0.5"), Decimal("5")),
        BottomLevelQuantileForecast(ISSUE, TARGET, "SKU-B", "LOC-1", Decimal("0.1"), Decimal("2")),
        BottomLevelQuantileForecast(ISSUE, TARGET, "SKU-B", "LOC-1", Decimal("0.9"), Decimal("3")),
    )
    with pytest.raises(HierarchyValidationError, match="share one quantile grid"):
        aggregate_quantile_forecasts(inconsistent)


def test_quantile_domain_duplicates_and_aggregate_tampering_fail_closed() -> None:
    with pytest.raises(HierarchyValidationError, match="between 0 and 1"):
        BottomLevelQuantileForecast(ISSUE, TARGET, "SKU-A", "LOC-1", Decimal("1.1"), Decimal("1"))

    row = _forecast_rows()[0]
    with pytest.raises(HierarchyValidationError, match="duplicate bottom-level"):
        aggregate_quantile_forecasts((row, row))

    coherent = aggregate_quantile_forecasts(_forecast_rows())
    tampered = tuple(
        replace(item, predicted_quantity=Decimal("12"))
        if item.level is HierarchyLevel.NETWORK and item.quantile == Decimal("0.5")
        else item
        for item in coherent
    )
    with pytest.raises(HierarchyValidationError, match="network aggregates"):
        validate_bottom_up_coherence(tampered)


def test_hierarchy_records_are_frozen_and_validate_node_shape() -> None:
    row = _forecast_rows()[0]
    with pytest.raises(FrozenInstanceError):
        row.sku_id = "CHANGED"  # type: ignore[misc]

    with pytest.raises(HierarchyValidationError, match="network rows cannot"):
        HierarchicalQuantileForecast(
            issue_time=ISSUE,
            target_date=TARGET,
            level=HierarchyLevel.NETWORK,
            sku_id="SKU-A",
            location_id=None,
            quantile=Decimal("0.5"),
            predicted_quantity=Decimal("1"),
        )


def test_empty_hierarchy_is_a_valid_contract_smoke_result() -> None:
    assert aggregate_quantile_forecasts(()) == ()
    assert aggregate_actuals(()) == ()
    validate_bottom_up_coherence(())
