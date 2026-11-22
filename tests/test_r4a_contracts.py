from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

import nexus.decisioning.r4a_contracts as r4a_contracts
from nexus.decisioning.r4a_contracts import (
    DEFAULT_R4A_CONFIG_PATH,
    FeasibilityReport,
    OrderDecisionLine,
    PolicyDecision,
    R4AConfig,
    load_champion_forecast_adapter,
    load_r4a_config,
)

pytestmark = pytest.mark.r4a


def _raw_config() -> dict[str, object]:
    payload = yaml.safe_load(DEFAULT_R4A_CONFIG_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_r4a_config_is_closed_strict_and_immutable() -> None:
    config = load_r4a_config()

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        R4AConfig.model_validate({**_raw_config(), "undeclared_control": True})

    wrong_type = _raw_config()
    wrong_type["budget_cents"] = "360000"
    with pytest.raises(ValidationError, match="valid integer"):
        R4AConfig.model_validate(wrong_type)

    with pytest.raises(ValidationError, match="frozen"):
        config.budget_cents = 1  # type: ignore[misc]


def test_r3_champion_is_the_only_permitted_forecast_authority() -> None:
    payload = _raw_config()
    authority = deepcopy(payload["forecast_authority"])
    assert isinstance(authority, dict)
    authority["selected_model"] = "global_lightgbm_quantile_candidate"
    payload["forecast_authority"] = authority

    with pytest.raises(ValidationError, match="seasonal_naive_empirical_intervals"):
        R4AConfig.model_validate(payload)


def test_champion_adapter_is_hash_verified_and_rejects_file_tampering(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = load_r4a_config()
    source = r4a_contracts.PROJECT_ROOT / config.forecast_authority.adapter_path
    payload = source.read_bytes()
    target = tmp_path / "champion-adapter.json"
    target.write_bytes(payload)
    local_authority = config.forecast_authority.model_copy(
        update={
            "adapter_path": target.name,
            "adapter_file_sha256": hashlib.sha256(payload).hexdigest(),
        }
    )
    local_config = config.model_copy(update={"forecast_authority": local_authority})
    monkeypatch.setattr(r4a_contracts, "PROJECT_ROOT", tmp_path)

    adapter = load_champion_forecast_adapter(local_config)
    assert adapter.selected_model == "seasonal_naive_empirical_intervals"
    assert adapter.challenger_gate_status == "CHALLENGER_LOST"
    assert adapter.joint_distribution_calibration_claimed is False

    target.write_bytes(payload + b"\n")
    with pytest.raises(ValueError, match="checksum does not match"):
        load_champion_forecast_adapter(local_config)


def test_planning_and_evaluation_seed_domains_must_be_disjoint() -> None:
    payload = _raw_config()
    scenarios = deepcopy(payload["scenarios"])
    assert isinstance(scenarios, dict)
    scenarios["evaluation_seeds"] = [4102, 4201]
    payload["scenarios"] = scenarios

    with pytest.raises(ValidationError, match="must be disjoint"):
        R4AConfig.model_validate(payload)


def test_quantity_contract_rejects_negative_and_non_integer_values() -> None:
    base = {
        "sku_id": "SKU-A",
        "location_id": "LOC-1",
        "reason_code": "optimized_regular_order",
    }

    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        OrderDecisionLine(**base, order_quantity_units=-1)
    with pytest.raises(ValidationError, match="valid integer"):
        OrderDecisionLine(**base, order_quantity_units=10.5)


def test_solver_error_cannot_cross_the_human_review_boundary() -> None:
    lines = (
        OrderDecisionLine(
            sku_id="SKU-A",
            location_id="LOC-1",
            order_quantity_units=0,
            reason_code="safe_fallback_order",
        ),
    )
    feasibility = FeasibilityReport(
        independently_feasible=True,
        service_targets_met=True,
    )

    with pytest.raises(ValidationError, match="only an independently feasible solver result"):
        PolicyDecision(
            policy_name="projected_base_stock",
            raw_solver_status="ERROR",
            solver_objective_cents=None,
            solver_best_bound_cents=None,
            optimality_gap_bps=None,
            deterministic_tie_break_applied=False,
            fallback_used=True,
            fallback_reason="injected_error",
            lines=lines,
            recourse=(),
            feasibility=feasibility,
            review_disposition="REVIEWABLE",
        )


@pytest.mark.parametrize(
    "malformed",
    (
        {
            "policy_name": "projected_base_stock",
            "fallback_used": True,
            "fallback_reason": "not_a_real_solver_result",
        },
        {"deterministic_tie_break_applied": False},
    ),
)
def test_solver_success_requires_the_governed_optimizer_shape(
    malformed: dict[str, object],
) -> None:
    payload: dict[str, object] = {
        "policy_name": "constrained_cp_sat",
        "raw_solver_status": "OPTIMAL",
        "solver_objective_cents": 10,
        "solver_best_bound_cents": 10,
        "optimality_gap_bps": 0,
        "deterministic_tie_break_applied": True,
        "fallback_used": False,
        "fallback_reason": None,
        "lines": (
            OrderDecisionLine(
                sku_id="SKU-A",
                location_id="LOC-1",
                order_quantity_units=0,
                reason_code="inventory_sufficient",
            ),
        ),
        "recourse": (),
        "feasibility": FeasibilityReport(
            independently_feasible=True,
            service_targets_met=True,
        ),
        "review_disposition": "REVIEWABLE",
    }
    payload.update(malformed)

    with pytest.raises(ValidationError, match="solver success requires the constrained policy"):
        PolicyDecision.model_validate(payload)


def test_receipt_only_disposition_must_match_the_raw_failure_class() -> None:
    with pytest.raises(ValidationError, match="review disposition does not match"):
        PolicyDecision(
            policy_name="projected_base_stock",
            raw_solver_status="ERROR",
            solver_objective_cents=None,
            solver_best_bound_cents=None,
            optimality_gap_bps=None,
            deterministic_tie_break_applied=False,
            fallback_used=True,
            fallback_reason="injected_error",
            lines=(
                OrderDecisionLine(
                    sku_id="SKU-A",
                    location_id="LOC-1",
                    order_quantity_units=0,
                    reason_code="safe_fallback_order",
                ),
            ),
            recourse=(),
            feasibility=FeasibilityReport(
                independently_feasible=True,
                service_targets_met=True,
            ),
            review_disposition="RECEIPT_ONLY_INFEASIBLE",
        )
