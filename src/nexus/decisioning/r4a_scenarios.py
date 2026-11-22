"""Stable, keyed R4A scenario generation with purpose-separated seeds."""

from __future__ import annotations

import hashlib

from nexus.decisioning.r4a_contracts import (
    DemandScenario,
    PlanningLine,
    PlanningSnapshot,
    R4AConfig,
    ScenarioDemandLine,
    ScenarioName,
    load_champion_forecast_adapter,
)


def stable_keyed_int(seed: int, *parts: object, modulus: int) -> int:
    """Map a semantic key to an integer without global RNG or iteration-order state."""

    if modulus <= 0:
        raise ValueError("modulus must be positive")
    key = "\x1f".join(("nexus-r4a-keyed-v1", str(seed), *(str(part) for part in parts)))
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % modulus


def build_planning_snapshot(config: R4AConfig) -> PlanningSnapshot:
    """Adapt the frozen four-line portfolio while preserving R3 champion authority."""

    authority = config.forecast_authority
    if authority.selected_model != authority.current_champion:
        raise ValueError("planning may only consume the currently governed champion")
    if authority.challenger_gate_status != "CHALLENGER_LOST":
        raise ValueError("R4A expects the learned challenger to remain rejected")
    adapter = load_champion_forecast_adapter(config)
    forecast_lines = {line.key: line for line in adapter.lines}
    return PlanningSnapshot(
        decision_time=config.decision_time,
        horizon_days=config.horizon_days,
        classification=config.classification,
        forecast_model=authority.selected_model,
        rejected_challenger_status=authority.challenger_gate_status,
        lines=tuple(
            PlanningLine(
                sku_id=line.sku_id,
                location_id=line.location_id,
                supplier_id=line.supplier_id,
                daily_q10_demand=forecast_lines[line.key].daily_q10_demand,
                daily_median_demand=forecast_lines[line.key].daily_median_demand,
                daily_q90_demand=forecast_lines[line.key].daily_q90_demand,
                initial_inventory_units=line.initial_inventory_units,
                regular_lead_time_days=line.regular_lead_time_days,
            )
            for line in config.portfolio
        ),
    )


def _scenario_demand(
    quantiles: tuple[int, int, int],
    *,
    config: R4AConfig,
    purpose: str,
    scenario_name: ScenarioName,
    seed: int,
    line_key: str,
    day_index: int,
) -> int:
    draw = stable_keyed_int(
        seed,
        purpose,
        "shared-demand-draw",
        line_key,
        day_index,
        modulus=10_000,
    )
    cumulative = 0
    sampled = quantiles[-1]
    for support, weight in zip(
        quantiles,
        config.scenarios.demand_quantile_weights_bps,
        strict=True,
    ):
        cumulative += weight
        if draw < cumulative:
            sampled = support
            break
    scenario_percent = (
        config.scenarios.demand_surge_percent if scenario_name == "demand_surge" else 100
    )
    return max(0, (sampled * scenario_percent + 50) // 100)


def build_scenarios(
    config: R4AConfig,
    *,
    purpose: str,
) -> tuple[DemandScenario, ...]:
    """Generate ordered scenarios; planning and evaluation seed domains cannot overlap."""

    if purpose not in {"planning", "evaluation"}:
        raise ValueError("purpose must be planning or evaluation")
    seeds = (
        config.scenarios.planning_seeds
        if purpose == "planning"
        else config.scenarios.evaluation_seeds
    )
    planning = build_planning_snapshot(config)
    scenarios: list[DemandScenario] = []
    for seed in seeds:
        for name in config.scenarios.names:
            lines = tuple(
                ScenarioDemandLine(
                    sku_id=line.sku_id,
                    location_id=line.location_id,
                    daily_demand_units=tuple(
                        _scenario_demand(
                            (lower, median, upper),
                            config=config,
                            purpose=purpose,
                            scenario_name=name,
                            seed=seed,
                            line_key=line.key,
                            day_index=day_index,
                        )
                        for day_index, (lower, median, upper) in enumerate(
                            zip(
                                line.daily_q10_demand,
                                line.daily_median_demand,
                                line.daily_q90_demand,
                                strict=True,
                            )
                        )
                    ),
                    regular_delay_days=(
                        config.scenarios.supplier_delay_days if name == "supplier_delay" else 0
                    ),
                )
                for line in planning.lines
            )
            scenarios.append(
                DemandScenario(
                    name=name,
                    purpose=purpose,  # type: ignore[arg-type]
                    seed=seed,
                    probability_weight=1,
                    lines=lines,
                )
            )
    return tuple(scenarios)


__all__ = ["build_planning_snapshot", "build_scenarios", "stable_keyed_int"]
