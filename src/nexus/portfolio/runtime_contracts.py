"""Fresh baseline contracts, separate from the frozen R3B champion authority."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal, Self, cast

import yaml
from pydantic import Field, NonNegativeInt, PositiveInt, field_validator, model_validator

from nexus.decisioning.r4a_contracts import (
    EvaluationPolicy,
    LocationCapacity,
    PortfolioLine,
    R4Contract,
    R4Identifier,
    R4Sha256,
    ScenarioPolicy,
    SolverPolicy,
    SupplierCapacity,
)


class RuntimeItemPolicy(R4Contract):
    sku_id: R4Identifier
    location_id: R4Identifier
    regular_lead_time_days: PositiveInt
    order_multiple_units: PositiveInt
    minimum_order_units: NonNegativeInt
    maximum_order_units: PositiveInt
    unit_volume: PositiveInt
    unit_cost_cents: PositiveInt
    fixed_order_cost_cents: NonNegativeInt
    holding_cost_cents_per_unit_day: NonNegativeInt
    stockout_cost_cents_per_unit: PositiveInt
    expedite_cost_cents_per_unit: PositiveInt
    working_capital_cost_cents_per_unit: NonNegativeInt
    service_target_bps: Annotated[int, Field(ge=0, le=10000)]

    @property
    def key(self) -> str:
        return f"{self.sku_id}:{self.location_id}"


class RuntimePolicy(R4Contract):
    contract_version: Literal["nexus.runtime-policy.v1"]
    classification: Literal["synthetic"]
    currency: Literal["USD"]
    horizon_days: Literal[14]
    budget_cents: PositiveInt
    inventory_policy: Literal["gold_on_hand_only_open_orders_excluded"]
    lines: tuple[RuntimeItemPolicy, ...]
    location_capacities: tuple[LocationCapacity, ...]
    supplier_capacities: tuple[SupplierCapacity, ...]
    scenarios: ScenarioPolicy
    solver: SolverPolicy
    evaluation: EvaluationPolicy

    @field_validator("lines", "location_capacities", "supplier_capacities", mode="before")
    @classmethod
    def freeze(cls, value: Any) -> tuple[Any, ...]:
        return tuple(value)

    @model_validator(mode="after")
    def unique_keys(self) -> Self:
        keys = [line.key for line in self.lines]
        if not keys or keys != sorted(set(keys)):
            raise ValueError("runtime policy keys must be nonempty, unique and sorted")
        for field, key in (
            (self.location_capacities, "location_id"),
            (self.supplier_capacities, "supplier_id"),
        ):
            values = [getattr(item, key) for item in field]
            if len(values) != len(set(values)):
                raise ValueError("runtime capacities must have unique identifiers")
        return self


class RuntimePlanningConfig(R4Contract):
    """Structural inputs consumed by the unchanged shared optimizer.

    No ``forecast_authority`` field exists: fresh baseline provenance is a distinct
    contract and cannot be mistaken for the frozen R3B promotion evidence.
    """

    contract_version: Literal["nexus.runtime-planning.v1"] = "nexus.runtime-planning.v1"
    decision_time: datetime
    horizon_days: Literal[14]
    budget_cents: PositiveInt
    portfolio: tuple[PortfolioLine, ...]
    location_capacities: tuple[LocationCapacity, ...]
    supplier_capacities: tuple[SupplierCapacity, ...]
    scenarios: ScenarioPolicy
    solver: SolverPolicy
    evaluation: EvaluationPolicy


class RuntimeForecastPoint(R4Contract):
    target_date: date
    q025: Annotated[float, Field(ge=0, allow_inf_nan=False)]
    q10: Annotated[float, Field(ge=0, allow_inf_nan=False)]
    q50: Annotated[float, Field(ge=0, allow_inf_nan=False)]
    q90: Annotated[float, Field(ge=0, allow_inf_nan=False)]
    q975: Annotated[float, Field(ge=0, allow_inf_nan=False)]

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if not self.q025 <= self.q10 <= self.q50 <= self.q90 <= self.q975:
            raise ValueError("runtime forecast quantiles must be ordered")
        return self


class RuntimeForecastLine(R4Contract):
    sku_id: R4Identifier
    location_id: R4Identifier
    eligible_observation_count: PositiveInt
    eligible_input_sha256: R4Sha256
    baseline_forecast_sha256: R4Sha256
    residual_count: NonNegativeInt
    points: tuple[RuntimeForecastPoint, ...]

    @property
    def key(self) -> str:
        return f"{self.sku_id}:{self.location_id}"


class RuntimeForecastAdapter(R4Contract):
    contract_version: Literal["nexus.runtime-forecast-adapter.v1"] = (
        "nexus.runtime-forecast-adapter.v1"
    )
    source_contract: Literal["nexus.r2-gold.connected-baseline.v1"] = (
        "nexus.r2-gold.connected-baseline.v1"
    )
    model_name: Literal["seasonal_naive_empirical_intervals"] = "seasonal_naive_empirical_intervals"
    model_version: Literal["1.0.0"] = "1.0.0"
    baseline_uses_engineered_features: Literal[False] = False
    learned_model_training_claimed: Literal[False] = False
    superiority_claimed: Literal[False] = False
    issue_time: datetime
    horizon_days: Literal[14] = 14
    source_manifest_sha256: R4Sha256
    gold_demand_sha256: R4Sha256
    gold_inventory_sha256: R4Sha256
    registration_sha256: R4Sha256
    feature_sha256: R4Sha256
    lines: tuple[RuntimeForecastLine, ...]

    @model_validator(mode="after")
    def aligned(self) -> Self:
        if self.issue_time.tzinfo is None or self.issue_time.utcoffset() is None:
            raise ValueError("runtime issue time must be timezone aware")
        keys = [line.key for line in self.lines]
        if not keys or keys != sorted(set(keys)):
            raise ValueError("runtime forecast keys must be unique and ordered")
        dates = [self.issue_time.date() + timedelta(days=day) for day in range(self.horizon_days)]
        for line in self.lines:
            if [point.target_date for point in line.points] != dates:
                raise ValueError(
                    "runtime forecast must start on the issue date for exactly 14 days"
                )
        return self


def load_runtime_policy(path: Path) -> RuntimePolicy:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("runtime policy must be a mapping")
    return RuntimePolicy.model_validate(cast(dict[str, Any], payload))
