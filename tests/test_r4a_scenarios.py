from __future__ import annotations

import pytest

from nexus.decisioning.r4a_contracts import DemandScenario, R4AConfig, load_r4a_config
from nexus.decisioning.r4a_scenarios import (
    build_planning_snapshot,
    build_scenarios,
    stable_keyed_int,
)

pytestmark = pytest.mark.r4a


def _scenario_values(
    scenarios: tuple[DemandScenario, ...],
) -> dict[tuple[str, int, str, str], tuple[tuple[int, ...], int]]:
    return {
        (scenario.purpose, scenario.seed, scenario.name, line.key): (
            line.daily_demand_units,
            line.regular_delay_days,
        )
        for scenario in scenarios
        for line in scenario.lines
    }


def test_keyed_scenario_draws_do_not_depend_on_caller_iteration_order() -> None:
    config = load_r4a_config()
    semantic_keys = tuple(
        (line.key, day_index)
        for line in config.portfolio
        for day_index in range(config.horizon_days)
    )

    def draws(keys: tuple[tuple[str, int], ...]) -> dict[tuple[str, int], int]:
        return {
            key: stable_keyed_int(
                config.scenarios.planning_seeds[0],
                "planning",
                "shared-demand-draw",
                key[0],
                key[1],
                modulus=10_000,
            )
            for key in keys
        }

    assert draws(semantic_keys) == draws(tuple(reversed(semantic_keys)))


def test_planning_and_evaluation_scenarios_use_separate_frozen_seed_domains() -> None:
    config = load_r4a_config()
    planning = build_scenarios(config, purpose="planning")
    evaluation = build_scenarios(config, purpose="evaluation")

    planning_seeds = {scenario.seed for scenario in planning}
    evaluation_seeds = {scenario.seed for scenario in evaluation}
    assert planning_seeds == set(config.scenarios.planning_seeds)
    assert evaluation_seeds == set(config.scenarios.evaluation_seeds)
    assert planning_seeds.isdisjoint(evaluation_seeds)
    assert {scenario.purpose for scenario in planning} == {"planning"}
    assert {scenario.purpose for scenario in evaluation} == {"evaluation"}


def test_supplier_delay_reuses_the_normal_demand_draw_and_changes_only_arrival() -> None:
    config = load_r4a_config()
    scenarios = build_scenarios(config, purpose="planning")
    by_key = {(scenario.seed, scenario.name): scenario for scenario in scenarios}

    for seed in config.scenarios.planning_seeds:
        normal = by_key[(seed, "normal")]
        delayed = by_key[(seed, "supplier_delay")]
        assert tuple(line.daily_demand_units for line in normal.lines) == tuple(
            line.daily_demand_units for line in delayed.lines
        )
        assert {line.regular_delay_days for line in normal.lines} == {0}
        assert {line.regular_delay_days for line in delayed.lines} == {
            config.scenarios.supplier_delay_days
        }


def test_planning_snapshot_preserves_the_governed_champion_boundary() -> None:
    config = load_r4a_config()

    snapshot = build_planning_snapshot(config)

    assert snapshot.forecast_model == "seasonal_naive_empirical_intervals"
    assert snapshot.rejected_challenger_status == "CHALLENGER_LOST"
    assert tuple(line.key for line in snapshot.lines) == tuple(
        line.key for line in config.portfolio
    )
    assert all(
        lower <= median <= upper
        for line in snapshot.lines
        for lower, median, upper in zip(
            line.daily_q10_demand,
            line.daily_median_demand,
            line.daily_q90_demand,
            strict=True,
        )
    )


def test_stable_keyed_integer_is_repeatable_and_validates_its_domain() -> None:
    first = stable_keyed_int(41, "normal", "SKU-A:LOC-1", 3, modulus=17)
    second = stable_keyed_int(41, "normal", "SKU-A:LOC-1", 3, modulus=17)

    assert first == second
    assert 0 <= first < 17
    with pytest.raises(ValueError, match="modulus must be positive"):
        stable_keyed_int(41, "normal", modulus=0)


def test_scenario_builder_rejects_an_unknown_purpose() -> None:
    config: R4AConfig = load_r4a_config()

    with pytest.raises(ValueError, match="purpose must be planning or evaluation"):
        build_scenarios(config, purpose="training")
