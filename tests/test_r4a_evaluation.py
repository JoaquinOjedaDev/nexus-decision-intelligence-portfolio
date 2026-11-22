from __future__ import annotations

import pytest

from nexus.decisioning.r4a_contracts import (
    DemandScenario,
    EvaluationBaseline,
    PolicyDecision,
    R4AConfig,
    load_r4a_config,
)
from nexus.decisioning.r4a_evaluation import evaluate_policies, simulate_order_policy
from nexus.decisioning.r4a_optimizer import build_heuristic_policy, solve_constrained_policy
from nexus.decisioning.r4a_scenarios import build_scenarios

pytestmark = pytest.mark.r4a


@pytest.fixture(scope="module")
def evaluated_policies() -> tuple[
    R4AConfig,
    tuple[DemandScenario, ...],
    PolicyDecision,
    EvaluationBaseline,
]:
    config = load_r4a_config()
    planning = build_scenarios(config, purpose="planning")
    optimizer = solve_constrained_policy(config, planning)
    heuristic = build_heuristic_policy(config, planning)
    evaluation = build_scenarios(config, purpose="evaluation")
    return config, evaluation, optimizer, heuristic


def test_both_policies_are_replayed_on_exactly_the_same_scenario_keys(
    evaluated_policies: tuple[
        R4AConfig,
        tuple[DemandScenario, ...],
        PolicyDecision,
        EvaluationBaseline,
    ],
) -> None:
    config, scenarios, optimizer, heuristic = evaluated_policies

    optimizer_paths = simulate_order_policy(config, scenarios, optimizer.lines)
    heuristic_paths = simulate_order_policy(config, scenarios, heuristic.lines)

    assert tuple(path.path_key for path in optimizer_paths) == tuple(
        path.path_key for path in heuristic_paths
    )
    assert len(optimizer_paths) == (
        len(config.scenarios.evaluation_seeds) * len(config.scenarios.names)
    )
    for path in (*optimizer_paths, *heuristic_paths):
        assert path.served_units + path.stockout_units == path.demand_units
        assert path.total_cost_cents == (
            path.order_cost_cents
            + path.working_capital_cost_cents
            + path.holding_cost_cents
            + path.stockout_cost_cents
            + path.expedite_cost_cents
        )


def test_frozen_path_evaluation_reports_both_policies_without_superiority_claim(
    evaluated_policies: tuple[
        R4AConfig,
        tuple[DemandScenario, ...],
        PolicyDecision,
        EvaluationBaseline,
    ],
) -> None:
    config, scenarios, optimizer, heuristic = evaluated_policies

    report = evaluate_policies(
        config,
        scenarios,
        optimizer_lines=optimizer.lines,
        heuristic_lines=heuristic.lines,
    )

    assert report.evaluation_seeds == config.scenarios.evaluation_seeds
    assert report.optimizer.policy_name == "constrained_cp_sat"
    assert report.heuristic.policy_name == "projected_base_stock"
    assert report.optimizer.observation_paths == report.heuristic.observation_paths
    assert 0 <= report.optimizer.fill_rate_bps <= 10_000
    assert 0 <= report.heuristic.fill_rate_bps <= 10_000
    assert report.cost_difference_interval.lower_cents <= (
        report.cost_difference_interval.upper_cents
    )
    assert report.superiority_claim_allowed is False
    assert report.claim == "NO_SUPERIORITY_CLAIM"


def test_evaluation_rejects_planning_paths() -> None:
    config = load_r4a_config()
    planning = build_scenarios(config, purpose="planning")
    optimizer = solve_constrained_policy(config, planning)
    heuristic = build_heuristic_policy(config, planning)

    with pytest.raises(ValueError, match="only evaluation scenarios"):
        evaluate_policies(
            config,
            planning,
            optimizer_lines=optimizer.lines,
            heuristic_lines=heuristic.lines,
        )
