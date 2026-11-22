from __future__ import annotations

import pytest
from pydantic import ValidationError

from nexus.decisioning.r4a_contracts import (
    DemandScenario,
    EvaluationBaseline,
    LocationCapacity,
    OrderDecisionLine,
    PolicyDecision,
    PortfolioLine,
    R4AConfig,
    ScenarioDemandLine,
    ScenarioRecourseLine,
    SupplierCapacity,
    load_r4a_config,
)
from nexus.decisioning.r4a_optimizer import (
    build_heuristic_policy,
    build_safe_recourse,
    solve_constrained_policy,
    validate_decision,
)
from nexus.decisioning.r4a_scenarios import build_scenarios

pytestmark = pytest.mark.r4a


def _order_lines(config: R4AConfig, quantities: dict[str, int]) -> tuple[OrderDecisionLine, ...]:
    return tuple(
        OrderDecisionLine(
            sku_id=line.sku_id,
            location_id=line.location_id,
            order_quantity_units=quantities.get(line.key, 0),
            reason_code="projected_heuristic_order",
        )
        for line in config.portfolio
    )


def _tiny_config(*, expedite_cost: int = 3) -> R4AConfig:
    base = load_r4a_config()
    lines: list[PortfolioLine] = []
    for original in base.portfolio:
        lines.append(
            PortfolioLine(
                sku_id=original.sku_id,
                location_id=original.location_id,
                supplier_id=original.supplier_id,
                initial_inventory_units=0,
                regular_lead_time_days=1,
                order_multiple_units=5,
                minimum_order_units=5,
                maximum_order_units=10,
                unit_volume=1,
                unit_cost_cents=1,
                fixed_order_cost_cents=0,
                holding_cost_cents_per_unit_day=1,
                stockout_cost_cents_per_unit=4,
                expedite_cost_cents_per_unit=expedite_cost,
                working_capital_cost_cents_per_unit=0,
                service_target_bps=10_000,
            )
        )
    return R4AConfig.model_validate(
        {
            **base.model_dump(mode="python"),
            "budget_cents": 100,
            "portfolio": tuple(lines),
            "location_capacities": tuple(
                LocationCapacity(
                    location_id=item.location_id,
                    storage_capacity_volume=100,
                    expedite_capacity_units_per_day=100,
                )
                for item in base.location_capacities
            ),
            "supplier_capacities": tuple(
                SupplierCapacity(supplier_id=item.supplier_id, regular_capacity_units=100)
                for item in base.supplier_capacities
            ),
        }
    )


def _tiny_scenario(config: R4AConfig) -> tuple[DemandScenario, ...]:
    return (
        DemandScenario(
            name="normal",
            purpose="planning",
            seed=7,
            probability_weight=1,
            lines=tuple(
                ScenarioDemandLine(
                    sku_id=line.sku_id,
                    location_id=line.location_id,
                    daily_demand_units=((1,) * 14 if index == 0 else (0,) * 14),
                    regular_delay_days=0,
                )
                for index, line in enumerate(config.portfolio)
            ),
        ),
    )


def _tiny_objective(quantity: int, *, expedite_cost: int) -> int:
    inventory = 0
    total = quantity
    for day_index in range(14):
        if day_index == 0:
            inventory += quantity
        expedite = max(0, 1 - inventory)
        inventory = inventory + expedite - 1
        total += inventory + expedite * expedite_cost
    return total


@pytest.fixture(scope="module")
def smoke_decisions() -> tuple[
    R4AConfig,
    tuple[DemandScenario, ...],
    PolicyDecision,
    EvaluationBaseline,
]:
    config = load_r4a_config()
    scenarios = build_scenarios(config, purpose="planning")
    optimizer = solve_constrained_policy(config, scenarios)
    heuristic = build_heuristic_policy(config, scenarios)
    return config, scenarios, optimizer, heuristic


def test_actual_cp_sat_solution_and_both_independent_policy_proofs(
    smoke_decisions: tuple[
        R4AConfig,
        tuple[DemandScenario, ...],
        PolicyDecision,
        EvaluationBaseline,
    ],
) -> None:
    config, scenarios, optimizer, heuristic = smoke_decisions

    assert optimizer.raw_solver_status == "OPTIMAL"
    assert optimizer.policy_name == "constrained_cp_sat"
    assert optimizer.fallback_used is False
    assert optimizer.review_disposition == "REVIEWABLE"
    assert optimizer.solver_objective_cents is not None
    assert optimizer.solver_best_bound_cents is not None
    assert optimizer.solver_best_bound_cents <= optimizer.solver_objective_cents
    assert optimizer.optimality_gap_bps is not None
    assert validate_decision(config, scenarios, optimizer.lines, optimizer.recourse) == (
        optimizer.feasibility
    )
    assert optimizer.feasibility.independently_feasible is True
    assert validate_decision(config, scenarios, heuristic.lines, heuristic.recourse) == (
        heuristic.feasibility
    )
    assert heuristic.feasibility.independently_feasible is True
    assert heuristic.role == "EVALUATION_BASELINE"
    assert heuristic.solver_was_run is False
    assert heuristic.review_eligible is False
    assert "review_disposition" not in heuristic.model_dump(mode="json")


def test_inventory_conservation_and_one_based_business_day_arrival() -> None:
    config = _tiny_config()
    scenarios = _tiny_scenario(config)
    quantities = {line.key: 0 for line in config.portfolio}
    quantities[config.portfolio[0].key] = 5

    recourse = build_safe_recourse(config, scenarios, quantities)
    active = recourse[0]

    assert active.end_inventory_units[0] == 4
    assert active.expedite_units[0] == 0
    assert active.lost_sales_units[0] == 0
    previous = config.portfolio[0].initial_inventory_units
    demand_line = scenarios[0].lines[0]
    arrival_day = config.portfolio[0].regular_lead_time_days - 1
    for day_index, demand in enumerate(demand_line.daily_demand_units):
        arrival = quantities[demand_line.key] if day_index == arrival_day else 0
        available = previous + arrival
        assert active.end_inventory_units[day_index] == max(0, available - demand)
        assert active.expedite_units[day_index] + active.lost_sales_units[day_index] == max(
            0, demand - available
        )
        previous = active.end_inventory_units[day_index]


def test_tiny_solver_matches_brute_force_and_uses_lexicographic_tie_break() -> None:
    config = _tiny_config(expedite_cost=3)
    scenarios = _tiny_scenario(config)

    decision = solve_constrained_policy(config, scenarios)
    oracle_objective, oracle_quantity = min(
        (_tiny_objective(quantity, expedite_cost=3), quantity) for quantity in (0, 5, 10)
    )

    assert decision.raw_solver_status == "OPTIMAL"
    assert decision.solver_objective_cents == oracle_objective == 42
    assert decision.lines[0].order_quantity_units == oracle_quantity == 0
    assert decision.deterministic_tie_break_applied is True


@pytest.mark.parametrize("initial", [0, 4, 20])
@pytest.mark.parametrize("delay", [0, 3, 20])
@pytest.mark.parametrize("expedite_cost", [3, 4])
def test_inventory_balance_matches_enumerated_orders(
    initial: int, delay: int, expedite_cost: int
) -> None:
    base = _tiny_config(expedite_cost=expedite_cost)
    config = R4AConfig.model_validate(
        {
            **base.model_dump(mode="python"),
            "portfolio": (
                {**base.portfolio[0].model_dump(mode="python"), "initial_inventory_units": initial},
                *base.portfolio[1:],
            ),
        }
    )
    original = _tiny_scenario(config)[0]
    scenario = DemandScenario.model_validate(
        {
            **original.model_dump(mode="python"),
            "lines": (
                {**original.lines[0].model_dump(mode="python"), "regular_delay_days": delay},
                *original.lines[1:],
            ),
        }
    )

    def enumerate_cost(quantity: int) -> int:
        inventory, cost = initial, quantity
        for day in range(14):
            if day == delay:
                inventory += quantity
            shortage = max(0, 1 - inventory)
            inventory = max(0, inventory - 1)
            cost += inventory + shortage * expedite_cost
        return cost

    objective, quantity = min((enumerate_cost(q), q) for q in (0, 5, 10))
    decision = solve_constrained_policy(config, (scenario,))
    assert decision.raw_solver_status == "OPTIMAL"
    assert decision.solver_objective_cents == objective
    assert [line.order_quantity_units for line in decision.lines] == [quantity, 0, 0, 0]
    assert decision.deterministic_tie_break_applied and not decision.fallback_used
    assert (
        validate_decision(config, (scenario,), decision.lines, decision.recourse)
        == decision.feasibility
    )
    assert decision.feasibility.independently_feasible


@pytest.mark.parametrize("inject_failure", ["unknown", "error"])
def test_injected_solver_failures_are_repeatable_safe_and_non_actionable(
    inject_failure: str,
) -> None:
    config = load_r4a_config()
    scenarios = build_scenarios(config, purpose="planning")

    first = solve_constrained_policy(config, scenarios, inject_failure=inject_failure)
    second = solve_constrained_policy(config, scenarios, inject_failure=inject_failure)

    assert first == second
    assert first.raw_solver_status == inject_failure.upper()
    assert first.fallback_used is True
    assert first.fallback_reason == f"injected_{inject_failure}"
    if inject_failure == "unknown":
        assert first.review_disposition == "REVIEWABLE"
        assert first.feasibility.independently_feasible is True
        assert first.feasibility.service_targets_met is True
    else:
        assert first.review_disposition == "RECEIPT_ONLY_SOLVER_FAILURE"


@pytest.mark.parametrize("constraint", ["budget", "storage_capacity"])
def test_relaxing_budget_or_storage_cannot_remove_feasibility_or_worsen_optimum(
    constraint: str,
) -> None:
    relaxed = _tiny_config(expedite_cost=4)
    if constraint == "budget":
        tight = relaxed.model_copy(update={"budget_cents": 1})
    else:
        tight = relaxed.model_copy(
            update={
                "location_capacities": tuple(
                    item.model_copy(update={"storage_capacity_volume": 1})
                    for item in relaxed.location_capacities
                )
            }
        )
    scenarios = _tiny_scenario(relaxed)

    tight_result = solve_constrained_policy(tight, scenarios)
    relaxed_result = solve_constrained_policy(relaxed, scenarios)

    assert tight_result.raw_solver_status == "OPTIMAL"
    assert relaxed_result.raw_solver_status == "OPTIMAL"
    assert tight_result.feasibility.independently_feasible is True
    assert relaxed_result.feasibility.independently_feasible is True
    assert relaxed_result.solver_objective_cents is not None
    assert tight_result.solver_objective_cents is not None
    assert relaxed_result.solver_objective_cents <= tight_result.solver_objective_cents


def test_independent_validator_reports_order_supplier_budget_and_storage_violations() -> None:
    config = load_r4a_config()
    scenarios = build_scenarios(config, purpose="planning")
    quantities = {
        config.portfolio[0].key: config.portfolio[0].maximum_order_units + 1,
        config.portfolio[1].key: config.portfolio[1].maximum_order_units,
        config.portfolio[2].key: config.portfolio[2].maximum_order_units,
        config.portfolio[3].key: config.portfolio[3].maximum_order_units,
    }
    lines = _order_lines(config, quantities)
    recourse = list(build_safe_recourse(config, scenarios, quantities))
    first = recourse[0]
    recourse[0] = ScenarioRecourseLine(
        scenario_name=first.scenario_name,
        scenario_seed=first.scenario_seed,
        sku_id=first.sku_id,
        location_id=first.location_id,
        end_inventory_units=(1_000,) * config.horizon_days,
        lost_sales_units=first.lost_sales_units,
        expedite_units=(1_000,) * config.horizon_days,
    )

    report = validate_decision(config, scenarios, lines, tuple(recourse))
    codes = {issue.code for issue in report.issues}

    assert {
        "maximum_order",
        "order_multiple",
        "supplier_capacity",
        "budget",
        "storage_capacity",
        "expedite_capacity",
    } <= codes
    assert report.independently_feasible is False


def test_independent_validator_reports_moq_and_service_violations() -> None:
    config = load_r4a_config()
    scenario = build_scenarios(config, purpose="planning")[0]
    scenarios = (scenario,)
    quantities = {line.key: 0 for line in config.portfolio}
    quantities[config.portfolio[0].key] = 10
    lines = _order_lines(config, quantities)
    recourse = list(build_safe_recourse(config, scenarios, quantities))
    demand_line = scenario.lines[1]
    configured = config.portfolio[1]
    target_index = next(index for index, row in enumerate(recourse) if row.key == demand_line.key)
    recourse[target_index] = ScenarioRecourseLine(
        scenario_name=scenario.name,
        scenario_seed=scenario.seed,
        sku_id=demand_line.sku_id,
        location_id=demand_line.location_id,
        end_inventory_units=(configured.initial_inventory_units,) * config.horizon_days,
        lost_sales_units=demand_line.daily_demand_units,
        expedite_units=(0,) * config.horizon_days,
    )

    report = validate_decision(config, scenarios, lines, tuple(recourse))
    codes = {issue.code for issue in report.issues}

    assert "minimum_order" in codes
    assert "service_target" in codes
    assert report.service_targets_met is False


def test_order_decision_contract_rejects_float_quantities() -> None:
    with pytest.raises(ValidationError, match="valid integer"):
        OrderDecisionLine(
            sku_id="SKU-A",
            location_id="LOC-1",
            order_quantity_units=5.5,
            reason_code="optimized_regular_order",
        )
