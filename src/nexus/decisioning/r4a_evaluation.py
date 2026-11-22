"""Frozen-path policy evaluation for R4A.

Both policies see exactly the same demand and lead-time paths.  Evaluation never
re-tunes either order vector, and synthetic evidence is explicitly non-promotional.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from nexus.decisioning.r4a_contracts import (
    CostInterval,
    DemandScenario,
    EvaluationReport,
    OrderDecisionLine,
    PolicyMetrics,
    R4AConfig,
)
from nexus.decisioning.r4a_optimizer import build_safe_recourse, solve_constrained_policy
from nexus.decisioning.r4a_scenarios import stable_keyed_int
from nexus.modeling.r3_artifacts import canonical_json_sha256


@dataclass(frozen=True, slots=True)
class PathMetrics:
    path_key: str
    demand_units: int
    served_units: int
    stockout_units: int
    stockout_cost_cents: int
    inventory_unit_days: int
    holding_cost_cents: int
    expedite_units: int
    expedite_cost_cents: int
    working_capital_cost_cents: int
    order_cost_cents: int
    total_cost_cents: int


def simulate_order_policy(
    config: R4AConfig,
    scenarios: tuple[DemandScenario, ...],
    lines: tuple[OrderDecisionLine, ...],
) -> tuple[PathMetrics, ...]:
    """Replay an order vector with the same bounded deterministic recourse for both policies."""

    orders = {line.key: line.order_quantity_units for line in lines}
    config_lines = {line.key: line for line in config.portfolio}
    recourse = build_safe_recourse(config, scenarios, orders)
    recourse_map = {(line.scenario_seed, line.scenario_name, line.key): line for line in recourse}
    paths: list[PathMetrics] = []
    for scenario in scenarios:
        demand_units = 0
        served_units = 0
        stockout_units = 0
        stockout_cost = 0
        inventory_unit_days = 0
        holding_cost = 0
        expedite_units = 0
        expedite_cost = 0
        order_cost = 0
        working_capital_cost = 0
        for demand_line in scenario.lines:
            line = config_lines[demand_line.key]
            quantity = orders[demand_line.key]
            recourse_line = recourse_map[(scenario.seed, scenario.name, demand_line.key)]
            if quantity:
                order_cost += line.fixed_order_cost_cents + quantity * line.unit_cost_cents
                working_capital_cost += quantity * line.working_capital_cost_cents_per_unit
            demand = sum(demand_line.daily_demand_units)
            lost = sum(recourse_line.lost_sales_units)
            expedited = sum(recourse_line.expedite_units)
            inventory_days = sum(recourse_line.end_inventory_units)
            demand_units += demand
            served_units += demand - lost
            stockout_units += lost
            stockout_cost += lost * line.stockout_cost_cents_per_unit
            expedite_units += expedited
            expedite_cost += expedited * line.expedite_cost_cents_per_unit
            inventory_unit_days += inventory_days
            holding_cost += inventory_days * line.holding_cost_cents_per_unit_day
        total_cost = (
            order_cost + working_capital_cost + holding_cost + stockout_cost + expedite_cost
        )
        paths.append(
            PathMetrics(
                path_key=f"{scenario.seed}:{scenario.name}",
                demand_units=demand_units,
                served_units=served_units,
                stockout_units=stockout_units,
                stockout_cost_cents=stockout_cost,
                inventory_unit_days=inventory_unit_days,
                holding_cost_cents=holding_cost,
                expedite_units=expedite_units,
                expedite_cost_cents=expedite_cost,
                working_capital_cost_cents=working_capital_cost,
                order_cost_cents=order_cost,
                total_cost_cents=total_cost,
            )
        )
    return tuple(paths)


def _aggregate(
    policy_name: Literal["constrained_cp_sat", "projected_base_stock"],
    paths: tuple[PathMetrics, ...],
    *,
    regret_cents: int,
    horizon_days: int,
    portfolio_lines: int,
) -> PolicyMetrics:
    demand = sum(item.demand_units for item in paths)
    served = sum(item.served_units for item in paths)
    inventory_days = sum(item.inventory_unit_days for item in paths)
    denominator = max(1, len(paths) * horizon_days * portfolio_lines)
    return PolicyMetrics(
        policy_name=policy_name,
        observation_paths=len(paths),
        fill_rate_bps=(served * 10_000 + demand // 2) // demand if demand else 10_000,
        stockout_units=sum(item.stockout_units for item in paths),
        stockout_cost_cents=sum(item.stockout_cost_cents for item in paths),
        average_inventory_milliunits=(inventory_days * 1_000 + denominator // 2) // denominator,
        holding_cost_cents=sum(item.holding_cost_cents for item in paths),
        expedite_units=sum(item.expedite_units for item in paths),
        expedite_cost_cents=sum(item.expedite_cost_cents for item in paths),
        working_capital_cost_cents=sum(item.working_capital_cost_cents for item in paths),
        order_cost_cents=sum(item.order_cost_cents for item in paths),
        total_cost_cents=sum(item.total_cost_cents for item in paths),
        regret_cents=regret_cents,
    )


def _nearest_quantile(sorted_values: list[int], numerator: int, denominator: int) -> int:
    index = ((len(sorted_values) - 1) * numerator + denominator // 2) // denominator
    return sorted_values[index]


def _cost_interval(
    config: R4AConfig,
    optimizer: tuple[PathMetrics, ...],
    heuristic: tuple[PathMetrics, ...],
) -> CostInterval:
    clustered: dict[str, int] = {}
    for left, right in zip(optimizer, heuristic, strict=True):
        left_seed = left.path_key.split(":", maxsplit=1)[0]
        right_seed = right.path_key.split(":", maxsplit=1)[0]
        if left.path_key != right.path_key or left_seed != right_seed:
            raise ValueError("policy paths must be paired by identical seed and scenario")
        clustered[left_seed] = clustered.get(left_seed, 0) + (
            left.total_cost_cents - right.total_cost_cents
        )
    differences = [clustered[key] for key in sorted(clustered)]
    point = (sum(differences) + len(differences) // 2) // len(differences)
    bootstrap_means: list[int] = []
    for resample in range(config.evaluation.bootstrap_resamples):
        sampled_total = 0
        for draw in range(len(differences)):
            index = stable_keyed_int(
                config.evaluation.bootstrap_seed,
                "policy-cost-bootstrap",
                resample,
                draw,
                modulus=len(differences),
            )
            sampled_total += differences[index]
        bootstrap_means.append((sampled_total + len(differences) // 2) // len(differences))
    bootstrap_means.sort()
    return CostInterval(
        metric="optimizer_minus_heuristic_mean_seed_cost_cents",
        confidence_level_bps=9_500,
        lower_cents=_nearest_quantile(bootstrap_means, 25, 1_000),
        point_cents=point,
        upper_cents=_nearest_quantile(bootstrap_means, 975, 1_000),
    )


def _perfect_information_costs(
    config: R4AConfig,
    scenarios: tuple[DemandScenario, ...],
) -> dict[str, int]:
    """Solve each frozen path separately to create a bounded hindsight-cost oracle."""

    costs: dict[str, int] = {}
    for scenario in scenarios:
        if scenario.probability_weight != 1:
            raise ValueError("R4A perfect-information paths require unit scenario weights")
        oracle = solve_constrained_policy(config, (scenario,))
        if (
            oracle.raw_solver_status != "OPTIMAL"
            or not oracle.feasibility.independently_feasible
            or oracle.solver_objective_cents is None
        ):
            raise RuntimeError(
                f"perfect-information oracle failed for {scenario.seed}:{scenario.name}"
            )
        costs[f"{scenario.seed}:{scenario.name}"] = oracle.solver_objective_cents
    return costs


def evaluate_policies(
    config: R4AConfig,
    evaluation_scenarios: tuple[DemandScenario, ...],
    *,
    optimizer_lines: tuple[OrderDecisionLine, ...],
    heuristic_lines: tuple[OrderDecisionLine, ...],
) -> EvaluationReport:
    """Compare frozen order vectors on identical, purpose-separated evaluation paths."""

    if any(scenario.purpose != "evaluation" for scenario in evaluation_scenarios):
        raise ValueError("policy evaluation may consume only evaluation scenarios")
    observed_seeds = tuple(dict.fromkeys(scenario.seed for scenario in evaluation_scenarios))
    if observed_seeds != config.scenarios.evaluation_seeds:
        raise ValueError("evaluation paths do not match the frozen evaluation seed contract")
    optimizer_paths = simulate_order_policy(config, evaluation_scenarios, optimizer_lines)
    heuristic_paths = simulate_order_policy(config, evaluation_scenarios, heuristic_lines)
    if tuple(path.path_key for path in optimizer_paths) != tuple(
        path.path_key for path in heuristic_paths
    ):
        raise ValueError("optimizer and heuristic must use identical evaluation paths")
    oracle_costs = _perfect_information_costs(config, evaluation_scenarios)
    optimizer_regret = sum(
        path.total_cost_cents - oracle_costs[path.path_key] for path in optimizer_paths
    )
    heuristic_regret = sum(
        path.total_cost_cents - oracle_costs[path.path_key] for path in heuristic_paths
    )
    if optimizer_regret < 0 or heuristic_regret < 0:
        raise RuntimeError("a policy cost cannot beat the exact perfect-information oracle")
    path_identity = [scenario.model_dump(mode="json") for scenario in evaluation_scenarios]
    return EvaluationReport(
        evaluation_seeds=config.scenarios.evaluation_seeds,
        paths_sha256=canonical_json_sha256(path_identity),
        optimizer=_aggregate(
            "constrained_cp_sat",
            optimizer_paths,
            regret_cents=optimizer_regret,
            horizon_days=config.horizon_days,
            portfolio_lines=len(config.portfolio),
        ),
        heuristic=_aggregate(
            "projected_base_stock",
            heuristic_paths,
            regret_cents=heuristic_regret,
            horizon_days=config.horizon_days,
            portfolio_lines=len(config.portfolio),
        ),
        cost_difference_interval=_cost_interval(config, optimizer_paths, heuristic_paths),
        superiority_claim_allowed=False,
        claim="NO_SUPERIORITY_CLAIM",
    )


__all__ = ["PathMetrics", "evaluate_policies", "simulate_order_policy"]
