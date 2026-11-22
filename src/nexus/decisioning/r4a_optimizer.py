"""Bounded CP-SAT replenishment optimizer and engine-neutral feasibility proof."""

from __future__ import annotations

import importlib
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

from nexus.decisioning.r4a_contracts import (
    DemandScenario,
    EvaluationBaseline,
    FeasibilityIssue,
    FeasibilityReport,
    OrderDecisionLine,
    PolicyDecision,
    PortfolioLine,
    R4AConfig,
    RawSolverStatus,
    ReviewDisposition,
    ScenarioRecourseLine,
)

InjectedFailure = Literal["unknown", "infeasible", "model_invalid", "error"]


def _round_up_multiple(value: int, multiple: int) -> int:
    return ((max(0, value) + multiple - 1) // multiple) * multiple


def _order_cost_cents(line: PortfolioLine, quantity: int) -> int:
    if quantity == 0:
        return 0
    return line.fixed_order_cost_cents + quantity * line.unit_cost_cents


def _line_map(config: R4AConfig) -> dict[str, PortfolioLine]:
    return {line.key: line for line in config.portfolio}


def _scenario_line_map(scenario: DemandScenario) -> dict[str, Any]:
    return {line.key: line for line in scenario.lines}


def _project_orders(
    config: R4AConfig,
    desired: Mapping[str, int],
) -> dict[str, int]:
    """Project a heuristic onto order, supplier, storage, and budget constraints."""

    quantities: dict[str, int] = {}
    for line in config.portfolio:
        quantity = min(
            line.maximum_order_units,
            _round_up_multiple(desired.get(line.key, 0), line.order_multiple_units),
        )
        if 0 < quantity < line.minimum_order_units:
            quantity = line.minimum_order_units
        quantities[line.key] = quantity

    # A conservative pre-demand storage envelope guarantees every scenario/day cap.
    location_caps = {
        capacity.location_id: capacity.storage_capacity_volume
        for capacity in config.location_capacities
    }
    while True:
        overloaded = {
            location
            for location, capacity in location_caps.items()
            if sum(
                (line.initial_inventory_units + quantities[line.key]) * line.unit_volume
                for line in config.portfolio
                if line.location_id == location
            )
            > capacity
        }
        if not overloaded:
            break
        candidates = [
            line
            for line in reversed(config.portfolio)
            if line.location_id in overloaded and quantities[line.key] > 0
        ]
        if not candidates:
            break
        line = candidates[0]
        reduced = quantities[line.key] - line.order_multiple_units
        quantities[line.key] = reduced if reduced >= line.minimum_order_units else 0

    supplier_caps = {
        capacity.supplier_id: capacity.regular_capacity_units
        for capacity in config.supplier_capacities
    }
    for supplier_id, capacity in supplier_caps.items():
        while (
            sum(
                quantities[line.key] for line in config.portfolio if line.supplier_id == supplier_id
            )
            > capacity
        ):
            candidates = [
                line
                for line in reversed(config.portfolio)
                if line.supplier_id == supplier_id and quantities[line.key] > 0
            ]
            if not candidates:
                break
            line = candidates[0]
            reduced = quantities[line.key] - line.order_multiple_units
            quantities[line.key] = reduced if reduced >= line.minimum_order_units else 0

    while (
        sum(_order_cost_cents(line, quantities[line.key]) for line in config.portfolio)
        > config.budget_cents
    ):
        candidates = [line for line in reversed(config.portfolio) if quantities[line.key] > 0]
        if not candidates:
            break
        line = candidates[0]
        reduced = quantities[line.key] - line.order_multiple_units
        quantities[line.key] = reduced if reduced >= line.minimum_order_units else 0
    return quantities


def heuristic_order_quantities(
    config: R4AConfig,
    scenarios: Sequence[DemandScenario],
) -> dict[str, int]:
    """Create a transparent robust base-stock target, then project all hard constraints."""

    if not scenarios:
        raise ValueError("at least one planning scenario is required")
    maximum_demands: dict[str, int] = defaultdict(int)
    for scenario in scenarios:
        for line in scenario.lines:
            maximum_demands[line.key] = max(maximum_demands[line.key], sum(line.daily_demand_units))
    desired = {
        line.key: max(0, maximum_demands[line.key] - line.initial_inventory_units)
        for line in config.portfolio
    }
    return _project_orders(config, desired)


def build_safe_recourse(
    config: R4AConfig,
    scenarios: Sequence[DemandScenario],
    quantities: Mapping[str, int],
) -> tuple[ScenarioRecourseLine, ...]:
    """Allocate bounded same-day expedite, then expose any remaining demand as lost sales."""

    config_lines = _line_map(config)
    expedite_caps = {
        item.location_id: item.expedite_capacity_units_per_day
        for item in config.location_capacities
    }
    rows: list[ScenarioRecourseLine] = []
    for scenario in scenarios:
        demand_map = _scenario_line_map(scenario)
        inventories = {key: config_lines[key].initial_inventory_units for key in sorted(demand_map)}
        inventory_paths: dict[str, list[int]] = {key: [] for key in sorted(demand_map)}
        lost_paths: dict[str, list[int]] = {key: [] for key in sorted(demand_map)}
        expedite_paths: dict[str, list[int]] = {key: [] for key in sorted(demand_map)}
        for day_index in range(config.horizon_days):
            shortfalls: dict[str, int] = {}
            ending: dict[str, int] = {}
            for key in sorted(demand_map):
                demand_line = demand_map[key]
                line = config_lines[key]
                arrival_day = line.regular_lead_time_days - 1 + demand_line.regular_delay_days
                available = inventories[key]
                if day_index == arrival_day:
                    available += quantities[key]
                demand = demand_line.daily_demand_units[day_index]
                ending[key] = max(0, available - demand)
                shortfalls[key] = max(0, demand - available)

            allocated = {key: 0 for key in shortfalls}
            for location_id, capacity in sorted(expedite_caps.items()):
                remaining = capacity
                priority = sorted(
                    (
                        key
                        for key in shortfalls
                        if config_lines[key].location_id == location_id and shortfalls[key] > 0
                    ),
                    key=lambda key: (
                        -(
                            config_lines[key].stockout_cost_cents_per_unit
                            - config_lines[key].expedite_cost_cents_per_unit
                        ),
                        -config_lines[key].service_target_bps,
                        key,
                    ),
                )
                for key in priority:
                    quantity = min(shortfalls[key], remaining)
                    allocated[key] = quantity
                    remaining -= quantity
                    if remaining == 0:
                        break

            for key in sorted(demand_map):
                expedite = allocated[key]
                lost = shortfalls[key] - expedite
                inventories[key] = ending[key]
                inventory_paths[key].append(ending[key])
                lost_paths[key].append(lost)
                expedite_paths[key].append(expedite)

        for key in sorted(demand_map):
            demand_line = demand_map[key]
            rows.append(
                ScenarioRecourseLine(
                    scenario_name=scenario.name,
                    scenario_seed=scenario.seed,
                    sku_id=demand_line.sku_id,
                    location_id=demand_line.location_id,
                    end_inventory_units=tuple(inventory_paths[key]),
                    lost_sales_units=tuple(lost_paths[key]),
                    expedite_units=tuple(expedite_paths[key]),
                )
            )
    return tuple(rows)


def validate_decision(
    config: R4AConfig,
    scenarios: Sequence[DemandScenario],
    lines: Sequence[OrderDecisionLine],
    recourse: Sequence[ScenarioRecourseLine],
) -> FeasibilityReport:
    """Recompute every hard constraint without importing or trusting the solver engine."""

    issues: list[FeasibilityIssue] = []
    config_lines = _line_map(config)
    orders = {line.key: line.order_quantity_units for line in lines}
    expected_keys = set(config_lines)
    if set(orders) != expected_keys or len(lines) != len(expected_keys):
        issues.append(
            FeasibilityIssue(
                code="order_key_set",
                scope="portfolio",
                detail="decision lines do not match the configured sku-location portfolio",
            )
        )
    for key, quantity in orders.items():
        configured = config_lines.get(key)
        if configured is None:
            continue
        if quantity > configured.maximum_order_units:
            issues.append(
                FeasibilityIssue(code="maximum_order", scope=key, detail="maximum order exceeded")
            )
        if quantity and quantity < configured.minimum_order_units:
            issues.append(
                FeasibilityIssue(code="minimum_order", scope=key, detail="MOQ not satisfied")
            )
        if quantity % configured.order_multiple_units:
            issues.append(
                FeasibilityIssue(code="order_multiple", scope=key, detail="order pack violated")
            )

    for supplier in config.supplier_capacities:
        used = sum(
            orders.get(line.key, 0)
            for line in config.portfolio
            if line.supplier_id == supplier.supplier_id
        )
        if used > supplier.regular_capacity_units:
            issues.append(
                FeasibilityIssue(
                    code="supplier_capacity",
                    scope=supplier.supplier_id,
                    detail=f"{used} units exceeds {supplier.regular_capacity_units}",
                )
            )
    budget_used = sum(_order_cost_cents(line, orders.get(line.key, 0)) for line in config.portfolio)
    if budget_used > config.budget_cents:
        issues.append(
            FeasibilityIssue(
                code="budget",
                scope="portfolio",
                detail=f"{budget_used} cents exceeds {config.budget_cents}",
            )
        )

    recourse_map = {(item.scenario_seed, item.scenario_name, item.key): item for item in recourse}
    expected_recourse = {
        (scenario.seed, scenario.name, demand_line.key)
        for scenario in scenarios
        for demand_line in scenario.lines
    }
    if set(recourse_map) != expected_recourse or len(recourse) != len(expected_recourse):
        issues.append(
            FeasibilityIssue(
                code="recourse_key_set",
                scope="scenarios",
                detail="recourse rows do not match every scenario and portfolio line",
            )
        )

    service_targets_met = True
    capacities = {
        item.location_id: item.storage_capacity_volume for item in config.location_capacities
    }
    for scenario in scenarios:
        scenario_lines = _scenario_line_map(scenario)
        for key, demand_line in scenario_lines.items():
            item = recourse_map.get((scenario.seed, scenario.name, key))
            line = config_lines[key]
            if item is None:
                service_targets_met = False
                continue
            horizon = config.horizon_days
            if not (
                len(item.end_inventory_units)
                == len(item.lost_sales_units)
                == len(item.expedite_units)
                == horizon
            ):
                issues.append(
                    FeasibilityIssue(
                        code="recourse_horizon",
                        scope=f"{scenario.seed}:{scenario.name}:{key}",
                        detail="recourse vectors do not cover the planning horizon",
                    )
                )
                service_targets_met = False
                continue
            previous = line.initial_inventory_units
            arrival_day = line.regular_lead_time_days - 1 + demand_line.regular_delay_days
            for day_index, demand in enumerate(demand_line.daily_demand_units):
                arrival = orders.get(key, 0) if day_index == arrival_day else 0
                available = previous + arrival
                expected_end = max(0, available - demand)
                expected_shortfall = max(0, demand - available)
                if (
                    expected_end != item.end_inventory_units[day_index]
                    or item.expedite_units[day_index] + item.lost_sales_units[day_index]
                    != expected_shortfall
                ):
                    issues.append(
                        FeasibilityIssue(
                            code="inventory_conservation",
                            scope=f"{scenario.seed}:{scenario.name}:{key}:d{day_index + 1}",
                            detail=(
                                "inventory, demand, lost-sales, and expedite do not conserve units"
                            ),
                        )
                    )
                previous = item.end_inventory_units[day_index]
            total_demand = sum(demand_line.daily_demand_units)
            lost = sum(item.lost_sales_units)
            allowed_scaled = total_demand * (10_000 - line.service_target_bps)
            if lost * 10_000 > allowed_scaled:
                service_targets_met = False
                issues.append(
                    FeasibilityIssue(
                        code="service_target",
                        scope=f"{scenario.seed}:{scenario.name}:{key}",
                        detail=f"lost sales {lost} exceed the {line.service_target_bps} bps target",
                    )
                )

        for day_index in range(config.horizon_days):
            for location_id, capacity in capacities.items():
                occupied = sum(
                    recourse_map[(scenario.seed, scenario.name, line.key)].end_inventory_units[
                        day_index
                    ]
                    * line.unit_volume
                    for line in config.portfolio
                    if line.location_id == location_id
                    and (scenario.seed, scenario.name, line.key) in recourse_map
                )
                if occupied > capacity:
                    issues.append(
                        FeasibilityIssue(
                            code="storage_capacity",
                            scope=f"{scenario.seed}:{scenario.name}:{location_id}:d{day_index + 1}",
                            detail=f"{occupied} volume exceeds {capacity}",
                        )
                    )
                expedited = sum(
                    recourse_map[(scenario.seed, scenario.name, line.key)].expedite_units[day_index]
                    for line in config.portfolio
                    if line.location_id == location_id
                    and (scenario.seed, scenario.name, line.key) in recourse_map
                )
                expedite_capacity = next(
                    item.expedite_capacity_units_per_day
                    for item in config.location_capacities
                    if item.location_id == location_id
                )
                if expedited > expedite_capacity:
                    issues.append(
                        FeasibilityIssue(
                            code="expedite_capacity",
                            scope=f"{scenario.seed}:{scenario.name}:{location_id}:d{day_index + 1}",
                            detail=f"{expedited} units exceeds {expedite_capacity}",
                        )
                    )
    return FeasibilityReport(
        independently_feasible=not issues,
        service_targets_met=service_targets_met,
        issues=tuple(issues),
    )


def _fallback_decision(
    config: R4AConfig,
    scenarios: Sequence[DemandScenario],
    *,
    status: RawSolverStatus,
    reason: str,
) -> PolicyDecision:
    quantities = heuristic_order_quantities(config, scenarios)
    lines = tuple(
        OrderDecisionLine(
            sku_id=line.sku_id,
            location_id=line.location_id,
            order_quantity_units=quantities[line.key],
            reason_code="safe_fallback_order",
        )
        for line in config.portfolio
    )
    recourse = build_safe_recourse(config, scenarios, quantities)
    feasibility = validate_decision(config, scenarios, lines, recourse)
    if (
        status == "UNKNOWN"
        and feasibility.independently_feasible
        and feasibility.service_targets_met
    ):
        disposition: ReviewDisposition = "REVIEWABLE"
    elif status == "INFEASIBLE":
        disposition = "RECEIPT_ONLY_INFEASIBLE"
    else:
        disposition = "RECEIPT_ONLY_SOLVER_FAILURE"
    return PolicyDecision(
        policy_name="projected_base_stock",
        raw_solver_status=status,
        solver_objective_cents=None,
        solver_best_bound_cents=None,
        optimality_gap_bps=None,
        deterministic_tie_break_applied=False,
        fallback_used=True,
        fallback_reason=reason,
        lines=lines,
        recourse=recourse,
        feasibility=feasibility,
        review_disposition=disposition,
    )


def solve_constrained_policy(
    config: R4AConfig,
    scenarios: Sequence[DemandScenario],
    *,
    inject_failure: InjectedFailure | None = None,
) -> PolicyDecision:
    """Solve R4A with CP-SAT; injected failures exercise fail-closed paths deterministically."""

    if not scenarios:
        raise ValueError("at least one planning scenario is required")
    if inject_failure == "unknown":
        return _fallback_decision(config, scenarios, status="UNKNOWN", reason="injected_unknown")
    if inject_failure == "infeasible":
        return _fallback_decision(
            config, scenarios, status="INFEASIBLE", reason="injected_infeasible"
        )
    if inject_failure == "model_invalid":
        return _fallback_decision(
            config, scenarios, status="MODEL_INVALID", reason="injected_model_invalid"
        )
    if inject_failure == "error":
        return _fallback_decision(config, scenarios, status="ERROR", reason="injected_error")

    try:
        cp_model = importlib.import_module("ortools.sat.python.cp_model")
    except (ImportError, ModuleNotFoundError):
        return _fallback_decision(
            config, scenarios, status="ERROR", reason="ortools_dependency_unavailable"
        )

    model = cp_model.CpModel()
    order_quantities: dict[str, Any] = {}
    order_open: dict[str, Any] = {}
    for line in config.portfolio:
        maximum_packs = line.maximum_order_units // line.order_multiple_units
        packs = model.new_int_var(0, maximum_packs, f"packs__{line.key}")
        quantity = model.new_int_var(0, line.maximum_order_units, f"order__{line.key}")
        opened = model.new_bool_var(f"opened__{line.key}")
        model.add(quantity == packs * line.order_multiple_units)
        model.add(quantity <= line.maximum_order_units * opened)
        model.add(quantity >= line.minimum_order_units * opened)
        order_quantities[line.key] = quantity
        order_open[line.key] = opened

    for supplier_capacity in config.supplier_capacities:
        model.add(
            sum(
                order_quantities[line.key]
                for line in config.portfolio
                if line.supplier_id == supplier_capacity.supplier_id
            )
            <= supplier_capacity.regular_capacity_units
        )
    purchase_commitment = sum(
        order_quantities[line.key] * line.unit_cost_cents
        + order_open[line.key] * line.fixed_order_cost_cents
        for line in config.portfolio
    )
    model.add(purchase_commitment <= config.budget_cents)
    first_stage_cost = purchase_commitment + sum(
        order_quantities[line.key] * line.working_capital_cost_cents_per_unit
        for line in config.portfolio
    )

    inventory_vars: dict[tuple[int, str, int], Any] = {}
    lost_vars: dict[tuple[int, str, int], Any] = {}
    expedite_vars: dict[tuple[int, str, int], Any] = {}
    scenario_cost_terms: list[Any] = []
    config_lines = _line_map(config)
    for scenario_index, scenario in enumerate(scenarios):
        demand_map = _scenario_line_map(scenario)
        for key, demand_line in demand_map.items():
            line = config_lines[key]
            maximum_inventory = (
                line.initial_inventory_units
                + line.maximum_order_units
                + sum(demand_line.daily_demand_units)
            )
            arrival_day = line.regular_lead_time_days - 1 + demand_line.regular_delay_days
            line_lost: list[Any] = []
            for day_index, demand in enumerate(demand_line.daily_demand_units):
                inventory = model.new_int_var(
                    0, maximum_inventory, f"inv__{scenario_index}__{key}__{day_index}"
                )
                lost = model.new_int_var(0, demand, f"lost__{scenario_index}__{key}__{day_index}")
                expedite = model.new_int_var(
                    0, demand, f"expedite__{scenario_index}__{key}__{day_index}"
                )
                previous: Any = (
                    line.initial_inventory_units
                    if day_index == 0
                    else inventory_vars[(scenario_index, key, day_index - 1)]
                )
                arrival: Any = order_quantities[key] if day_index == arrival_day else 0
                model.add_max_equality(inventory, [previous + arrival - demand, 0])
                # If x = previous + arrival - demand and inventory = max(x, 0),
                # then max(-x, 0) = inventory - x. The linear balance is exactly
                # the same shortfall, without a second max constraint/variable.
                # Expedites still cover current shortfall only, never future stock.
                model.add(expedite + lost == inventory - previous - arrival + demand)
                inventory_vars[(scenario_index, key, day_index)] = inventory
                lost_vars[(scenario_index, key, day_index)] = lost
                expedite_vars[(scenario_index, key, day_index)] = expedite
                line_lost.append(lost)
                scenario_cost_terms.extend(
                    (
                        inventory
                        * line.holding_cost_cents_per_unit_day
                        * scenario.probability_weight,
                        lost * line.stockout_cost_cents_per_unit * scenario.probability_weight,
                        expedite * line.expedite_cost_cents_per_unit * scenario.probability_weight,
                    )
                )
            total_demand = sum(demand_line.daily_demand_units)
            model.add(sum(line_lost) * 10_000 <= total_demand * (10_000 - line.service_target_bps))
        for day_index in range(config.horizon_days):
            for location_capacity in config.location_capacities:
                model.add(
                    sum(
                        inventory_vars[(scenario_index, line.key, day_index)] * line.unit_volume
                        for line in config.portfolio
                        if line.location_id == location_capacity.location_id
                    )
                    <= location_capacity.storage_capacity_volume
                )
                model.add(
                    sum(
                        expedite_vars[(scenario_index, line.key, day_index)]
                        for line in config.portfolio
                        if line.location_id == location_capacity.location_id
                    )
                    <= location_capacity.expedite_capacity_units_per_day
                )

    scenario_weight_total = sum(scenario.probability_weight for scenario in scenarios)
    primary_objective = first_stage_cost * scenario_weight_total + sum(scenario_cost_terms)
    model.minimize(primary_objective)

    solver = cp_model.CpSolver()
    solver.parameters.num_search_workers = config.solver.workers
    solver.parameters.random_seed = config.solver.random_seed
    solver.parameters.max_deterministic_time = config.solver.max_deterministic_time
    # The outer command/CI timeout is the hard wall-clock kill switch. This generous
    # per-solve ceiling must not become the normal stopping rule, because elapsed wall
    # time varies across platforms and would make FEASIBLE/OPTIMAL receipts drift.
    solver.parameters.max_time_in_seconds = config.solver.max_wall_time_seconds
    try:
        status_code = solver.solve(model)
    except Exception:
        return _fallback_decision(config, scenarios, status="ERROR", reason="solver_exception")
    status_lookup = {
        cp_model.OPTIMAL: "OPTIMAL",
        cp_model.FEASIBLE: "FEASIBLE",
        cp_model.INFEASIBLE: "INFEASIBLE",
        cp_model.MODEL_INVALID: "MODEL_INVALID",
        cp_model.UNKNOWN: "UNKNOWN",
    }
    raw_status = cast(RawSolverStatus, status_lookup.get(status_code, "ERROR"))
    if raw_status not in {"OPTIMAL", "FEASIBLE"}:
        return _fallback_decision(
            config,
            scenarios,
            status=raw_status,
            reason=f"solver_{raw_status.lower()}",
        )

    primary_value = round(solver.objective_value)
    if raw_status == "OPTIMAL":
        best_bound = primary_value
        optimality_gap_bps = 0
    else:
        best_bound = max(0, math.floor(solver.best_objective_bound + 1e-9))
        optimality_gap_bps = min(
            10_000,
            math.ceil(max(0, primary_value - best_bound) * 10_000 / max(1, primary_value)),
        )
    tie_break_applied = raw_status in {"OPTIMAL", "FEASIBLE"}
    if tie_break_applied:
        model.add(primary_objective == primary_value)
        for line in config.portfolio:
            model.minimize(order_quantities[line.key])
            tied_status = solver.solve(model)
            if tied_status != cp_model.OPTIMAL:
                return _fallback_decision(
                    config,
                    scenarios,
                    status="UNKNOWN",
                    reason="deterministic_tie_break_incomplete",
                )
            model.add(order_quantities[line.key] == int(solver.value(order_quantities[line.key])))

    lines = tuple(
        OrderDecisionLine(
            sku_id=line.sku_id,
            location_id=line.location_id,
            order_quantity_units=int(solver.value(order_quantities[line.key])),
            reason_code=(
                "optimized_regular_order"
                if int(solver.value(order_quantities[line.key])) > 0
                else "inventory_sufficient"
            ),
        )
        for line in config.portfolio
    )
    recourse = tuple(
        ScenarioRecourseLine(
            scenario_name=scenario.name,
            scenario_seed=scenario.seed,
            sku_id=demand_line.sku_id,
            location_id=demand_line.location_id,
            end_inventory_units=tuple(
                int(solver.value(inventory_vars[(scenario_index, demand_line.key, day_index)]))
                for day_index in range(config.horizon_days)
            ),
            lost_sales_units=tuple(
                int(solver.value(lost_vars[(scenario_index, demand_line.key, day_index)]))
                for day_index in range(config.horizon_days)
            ),
            expedite_units=tuple(
                int(solver.value(expedite_vars[(scenario_index, demand_line.key, day_index)]))
                for day_index in range(config.horizon_days)
            ),
        )
        for scenario_index, scenario in enumerate(scenarios)
        for demand_line in scenario.lines
    )
    feasibility = validate_decision(config, scenarios, lines, recourse)
    disposition: ReviewDisposition = (
        "REVIEWABLE"
        if feasibility.independently_feasible and feasibility.service_targets_met
        else "RECEIPT_ONLY_INFEASIBLE"
    )
    return PolicyDecision(
        policy_name="constrained_cp_sat",
        raw_solver_status=raw_status,
        solver_objective_cents=primary_value,
        solver_best_bound_cents=best_bound,
        optimality_gap_bps=optimality_gap_bps,
        deterministic_tie_break_applied=tie_break_applied,
        fallback_used=False,
        fallback_reason=None,
        lines=lines,
        recourse=recourse,
        feasibility=feasibility,
        review_disposition=disposition,
    )


def build_heuristic_policy(
    config: R4AConfig,
    scenarios: Sequence[DemandScenario],
) -> EvaluationBaseline:
    """Materialize the projected heuristic as an independently validated policy."""

    quantities = heuristic_order_quantities(config, scenarios)
    lines = tuple(
        OrderDecisionLine(
            sku_id=line.sku_id,
            location_id=line.location_id,
            order_quantity_units=quantities[line.key],
            reason_code=(
                "projected_heuristic_order" if quantities[line.key] else "inventory_sufficient"
            ),
        )
        for line in config.portfolio
    )
    recourse = build_safe_recourse(config, scenarios, quantities)
    feasibility = validate_decision(config, scenarios, lines, recourse)
    # This object is an evaluation baseline, not a recommendation submission.
    return EvaluationBaseline(
        lines=lines,
        recourse=recourse,
        feasibility=feasibility,
    )


__all__ = [
    "InjectedFailure",
    "build_heuristic_policy",
    "build_safe_recourse",
    "heuristic_order_quantities",
    "solve_constrained_policy",
    "validate_decision",
]
