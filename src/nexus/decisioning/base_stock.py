"""Deterministic base-stock replenishment heuristic.

The rule is intentionally inspectable:

    shortage = lead-time demand + safety stock - on-hand - on-order
    recommendation = ceil(max(0, shortage))

Ceiling is conservative for indivisible units: any positive fractional shortage becomes
one unit. There is therefore no platform-dependent floating-point or tie behavior.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, Decimal
from enum import StrEnum

from nexus.contracts import RecommendationLine
from nexus.modeling import SeasonalNaiveForecast

_METHOD_NAME = "base_stock_reorder"
_METHOD_VERSION = "1.0.0"
_ROUNDING_POLICY = "ceiling_to_whole_unit"


def _nonnegative_decimal(value: Decimal | int, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int)):
        raise TypeError(f"{name} must be an int or Decimal")
    normalized = Decimal(value)
    if not normalized.is_finite() or normalized < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    return normalized


def _canonical_decimal(value: Decimal) -> str:
    """Represent numerically equal decimals identically in decision hashes."""

    if value == 0:
        return "0"
    return format(value.normalize(), "f")


class ReplenishmentReasonCode(StrEnum):
    """Small, stable vocabulary for downstream review and reporting."""

    BELOW_BASE_STOCK = "below_base_stock"
    INVENTORY_SUFFICIENT = "inventory_sufficient"


@dataclass(frozen=True, slots=True)
class BaseStockRecommendation:
    """Immutable decision record containing every term in the formula."""

    sku_id: str
    location_id: str
    forecast_sha256: str
    horizon_days: int
    expected_lead_time_demand: Decimal
    safety_stock: Decimal
    on_hand: Decimal
    on_order: Decimal
    base_stock_level: Decimal
    net_requirement: Decimal
    recommended_quantity: int
    reason_code: ReplenishmentReasonCode
    method_name: str = field(default=_METHOD_NAME, init=False)
    method_version: str = field(default=_METHOD_VERSION, init=False)
    rounding_policy: str = field(default=_ROUNDING_POLICY, init=False)
    decision_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        content = {
            "method_name": self.method_name,
            "method_version": self.method_version,
            "rounding_policy": self.rounding_policy,
            "sku_id": self.sku_id,
            "location_id": self.location_id,
            "forecast_sha256": self.forecast_sha256,
            "horizon_days": self.horizon_days,
            "expected_lead_time_demand": _canonical_decimal(self.expected_lead_time_demand),
            "safety_stock": _canonical_decimal(self.safety_stock),
            "on_hand": _canonical_decimal(self.on_hand),
            "on_order": _canonical_decimal(self.on_order),
            "base_stock_level": _canonical_decimal(self.base_stock_level),
            "net_requirement": _canonical_decimal(self.net_requirement),
            "recommended_quantity": self.recommended_quantity,
            "reason_code": self.reason_code.value,
        }
        encoded = json.dumps(
            content,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        object.__setattr__(self, "decision_sha256", hashlib.sha256(encoded).hexdigest())

    def to_recommendation_line(self) -> RecommendationLine:
        """Adapt the decision to the existing immutable manifest contract."""

        return RecommendationLine(
            sku_id=self.sku_id,
            location_id=self.location_id,
            recommended_quantity=self.recommended_quantity,
            horizon_days=self.horizon_days,
            reason_code=self.reason_code.value,
        )


def recommend_base_stock(
    forecast: SeasonalNaiveForecast,
    *,
    lead_time_days: int,
    safety_stock: Decimal | int,
    on_hand: Decimal | int,
    on_order: Decimal | int,
) -> BaseStockRecommendation:
    """Convert the first lead-time forecast window into one reorder decision."""

    if isinstance(lead_time_days, bool) or not isinstance(lead_time_days, int):
        raise TypeError("lead_time_days must be an integer")
    if lead_time_days <= 0:
        raise ValueError("lead_time_days must be positive")
    if lead_time_days > forecast.horizon_days:
        raise ValueError("lead_time_days cannot exceed the forecast horizon")

    normalized_safety = _nonnegative_decimal(safety_stock, "safety_stock")
    normalized_on_hand = _nonnegative_decimal(on_hand, "on_hand")
    normalized_on_order = _nonnegative_decimal(on_order, "on_order")
    expected_demand = forecast.total_expected_demand(lead_time_days)
    base_stock_level = expected_demand + normalized_safety
    raw_requirement = base_stock_level - normalized_on_hand - normalized_on_order
    net_requirement = max(Decimal(0), raw_requirement)
    recommended_quantity = int(net_requirement.to_integral_value(rounding=ROUND_CEILING))
    reason_code = (
        ReplenishmentReasonCode.BELOW_BASE_STOCK
        if recommended_quantity > 0
        else ReplenishmentReasonCode.INVENTORY_SUFFICIENT
    )

    return BaseStockRecommendation(
        sku_id=forecast.sku_id,
        location_id=forecast.location_id,
        forecast_sha256=forecast.forecast_sha256,
        horizon_days=lead_time_days,
        expected_lead_time_demand=expected_demand,
        safety_stock=normalized_safety,
        on_hand=normalized_on_hand,
        on_order=normalized_on_order,
        base_stock_level=base_stock_level,
        net_requirement=net_requirement,
        recommended_quantity=recommended_quantity,
        reason_code=reason_code,
    )
