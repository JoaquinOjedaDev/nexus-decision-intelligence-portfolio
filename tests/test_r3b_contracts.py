from __future__ import annotations

from copy import deepcopy
from datetime import UTC
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from pydantic import ValidationError

from nexus.modeling.r3b_contracts import (
    R3B_DEMAND_FEATURES,
    R3B_FORBIDDEN_FIELDS,
    R3B_HORIZONS_DAYS,
    R3B_LEAD_TIME_FEATURES,
    R3B_PROMOTION_STATUSES,
    R3B_QUANTILES,
    R3BConfig,
    load_r3b_config,
    r3b_config_sha256,
)

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "modeling" / "r3b-smoke.yaml"

pytestmark = pytest.mark.r3b


def _payload() -> dict[str, Any]:
    value = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    return cast(dict[str, Any], value)


def test_r3b_smoke_loads_as_an_exact_frozen_synthetic_contract() -> None:
    config = load_r3b_config(CONFIG_PATH)

    assert (config.release, config.contract_version, config.seed) == ("R3B", "1.0.0", 20260718)
    assert (config.data_contract.r2c_profile, config.data_contract.classification) == (
        "smoke",
        "synthetic",
    )
    assert config.data_contract.source_profile == "smoke"
    assert config.data_contract.target == config.data_contract.demand_target == "observed_sales_qty"
    assert config.data_contract.activity_policy == "active_only"
    assert config.data_contract.censoring_policy == "uncensored_only"
    assert config.data_contract.latent_demand_usage == "post_persistence_evaluation_only"
    assert config.data_contract.cohort_usage == "post_persistence_evaluation_only"
    assert config.forecast_contract.issue_time_utc == "04:00:00"
    assert config.forecast_contract.horizons_days == R3B_HORIZONS_DAYS == (7, 14, 28)
    assert config.forecast_contract.maximum_horizon_days == 28
    assert config.forecast_contract.quantiles == R3B_QUANTILES
    assert config.forecast_contract.minimum_history_observations_for_learned_candidate == 14
    assert config.feature_contract.allowed_demand_features == R3B_DEMAND_FEATURES
    assert config.feature_contract.allowed_lead_time_features == R3B_LEAD_TIME_FEATURES
    assert config.feature_contract.forbidden_fields == R3B_FORBIDDEN_FIELDS
    assert config.feature_contract.forbidden_features == R3B_FORBIDDEN_FIELDS
    assert not set(R3B_DEMAND_FEATURES) & set(R3B_FORBIDDEN_FIELDS)
    assert not set(R3B_LEAD_TIME_FEATURES) & set(R3B_FORBIDDEN_FIELDS)
    assert tuple(origin.role for origin in config.evaluation.rolling_origins) == (
        "diagnostic",
        "calibration",
        "final_stress",
    )
    assert all(origin.issue_time.tzinfo is UTC for origin in config.evaluation.rolling_origins)
    with pytest.raises(ValidationError, match="frozen"):
        config.demand_model.num_threads = 4


def test_r3b_model_labels_calibration_reconciliation_and_promotion_are_frozen() -> None:
    config = load_r3b_config(CONFIG_PATH)

    assert config.demand_model.device_type == "cpu"
    assert config.demand_model.boosting_type == "gbdt"
    assert config.demand_model.deterministic is config.demand_model.force_col_wise is True
    assert {
        config.demand_model.seed,
        config.demand_model.data_random_seed,
        config.demand_model.feature_fraction_seed,
        config.demand_model.bagging_seed,
    } == {20260718}
    assert (config.demand_model.num_threads, config.demand_model.num_boost_round) == (2, 64)
    assert config.lead_time_model.maturity_policy == "label_available_at_lte_origin"
    assert config.lead_time_model.right_censoring_policy == "exclude_right_censored"
    assert config.lead_time_model.allowed_features == R3B_LEAD_TIME_FEATURES
    assert config.lead_time_model.baseline == "empirical_mature_uncensored_quantiles"
    assert config.calibration.method == "empirical_residual_quantile_offsets"
    assert config.calibration.minimum_support_per_horizon == 20
    assert config.calibration.insufficient_support_policy == "abstain_insufficient_evidence"
    assert config.reconciliation.method == "arithmetic_bottom_up"
    assert config.reconciliation.hierarchy == ("sku_location", "sku", "network")
    assert config.reconciliation.joint_distribution_calibration_claimed is False
    assert config.promotion.automatic is False
    assert config.promotion.synthetic_performance_claims_allowed is False
    assert config.promotion.minimum_wis_improvement == 0.10
    assert config.promotion.require_positive_paired_bootstrap_lower_bound is True
    assert config.promotion.allowed_statuses == R3B_PROMOTION_STATUSES
    assert config.promotion.passing_candidate_status == "ELIGIBLE_BUT_BLOCKED_SYNTHETIC"
    assert config.promotion.losing_candidate_status == "CHALLENGER_LOST"
    assert config.promotion.insufficient_evidence_status == "ABSTAIN_INSUFFICIENT_EVIDENCE"
    assert config.promotion.gate_failure_status == "REJECTED_GATE_FAILURE"


def test_r3b_resource_tolerance_and_hash_contracts_are_exact() -> None:
    config = load_r3b_config(CONFIG_PATH)

    assert config.resource_budget.model_dump() == {
        "wall_time_scope": "r3b_complete_verification",
        "wall_time_seconds": 300,
        "peak_rss_scope": "complete_process_tree_high_water",
        "peak_rss_mib": 2048,
        "thread_scope": "complete_process_tree_native_threads_high_water",
        "maximum_process_tree_threads": 96,
        "artifact_scope": "r3b_governed_artifacts_including_tracking_store",
        "artifact_mib": 100,
        "shutdown_scope": "process_tree_descendants_after_cleanup",
        "shutdown_seconds": 10,
        "maximum_surviving_descendants": 0,
    }
    assert config.numeric_tolerance.absolute_tolerance == 1e-7
    assert config.numeric_tolerance.relative_tolerance == 1e-6
    reordered = R3BConfig.model_validate(dict(reversed(list(_payload().items()))))
    assert r3b_config_sha256(config) == r3b_config_sha256(reordered)
    assert len(r3b_config_sha256(config)) == 64


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    [
        ("data_contract", "target", "synthetic_latent_demand_qty", "observed_sales_qty"),
        ("data_contract", "activity_policy", "all_rows", "active_only"),
        ("data_contract", "censoring_policy", "all_rows", "uncensored_only"),
        ("forecast_contract", "issue_time_utc", "03:00:00", "exactly 04:00:00"),
        ("forecast_contract", "horizons_days", [7, 14], "exactly 7, 14, 28"),
        (
            "forecast_contract",
            "quantiles",
            [0.05, 0.1, 0.5, 0.9, 0.95],
            "must exactly match",
        ),
        ("feature_contract", "version", "r3b-pit-v2", "r3b-pit-v1"),
        (
            "feature_contract",
            "allowed_demand_features",
            list(R3B_DEMAND_FEATURES[:-1]),
            "exact ordered demand feature set",
        ),
        (
            "feature_contract",
            "forbidden_fields",
            list(R3B_FORBIDDEN_FIELDS[:-1]),
            "exact ordered forbidden-field denylist",
        ),
        ("demand_model", "device_type", "gpu", "cpu"),
        ("demand_model", "boosting_type", "dart", "gbdt"),
        ("demand_model", "deterministic", False, "True"),
        ("demand_model", "force_col_wise", False, "True"),
        ("demand_model", "seed", 7, "20260718"),
        ("demand_model", "num_threads", 4, "2"),
        ("demand_model", "num_boost_round", 65, "64"),
        (
            "lead_time_model",
            "right_censoring_policy",
            "include_right_censored",
            "exclude_right_censored",
        ),
        ("calibration", "minimum_support_per_horizon", 19, "20"),
        ("promotion", "automatic", True, "False"),
        ("promotion", "minimum_wis_improvement", 0.09, "exactly 0.10"),
        (
            "promotion",
            "require_positive_paired_bootstrap_lower_bound",
            False,
            "True",
        ),
        ("resource_budget", "peak_rss_mib", 2047, "2048"),
        ("numeric_tolerance", "relative_tolerance", 1e-5, "exactly 1e-6"),
    ],
)
def test_r3b_executable_choices_fail_closed(
    section: str,
    field: str,
    value: object,
    message: str,
) -> None:
    payload = deepcopy(_payload())
    payload[section][field] = value
    with pytest.raises(ValidationError, match=message):
        R3BConfig.model_validate(payload)


def test_r3b_origins_unknown_fields_and_strict_types_fail_closed() -> None:
    naive = _payload()
    naive["evaluation"]["rolling_origins"][0]["issue_time"] = "2026-01-19T04:00:00"
    with pytest.raises(ValidationError, match="timezone info"):
        R3BConfig.model_validate(naive)

    incomplete = _payload()
    incomplete["evaluation"]["rolling_origins"] = incomplete["evaluation"]["rolling_origins"][:2]
    with pytest.raises(ValidationError, match="exactly one ordered"):
        R3BConfig.model_validate(incomplete)

    misordered = _payload()
    origins = misordered["evaluation"]["rolling_origins"]
    origins[0]["role"], origins[1]["role"] = origins[1]["role"], origins[0]["role"]
    with pytest.raises(ValidationError, match="exactly one ordered"):
        R3BConfig.model_validate(misordered)

    duplicated = _payload()
    duplicated["evaluation"]["rolling_origins"][1]["role"] = "diagnostic"
    with pytest.raises(ValidationError, match="exactly one ordered"):
        R3BConfig.model_validate(duplicated)

    wrong_hour = _payload()
    wrong_hour["evaluation"]["rolling_origins"][0]["issue_time"] = "2026-01-19T05:00:00Z"
    with pytest.raises(ValidationError, match="issue_time_utc"):
        R3BConfig.model_validate(wrong_hour)

    extra = _payload()
    extra["demand_model"]["verbosity"] = -1
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        R3BConfig.model_validate(extra)

    wrong_type = _payload()
    wrong_type["demand_model"]["num_threads"] = "2"
    with pytest.raises(ValidationError):
        R3BConfig.model_validate(wrong_type)


def test_equivalent_origin_offset_is_normalized_to_utc() -> None:
    payload = _payload()
    payload["evaluation"]["rolling_origins"][0]["issue_time"] = "2026-01-19T01:00:00-03:00"

    origin = R3BConfig.model_validate(payload).evaluation.rolling_origins[0]

    assert origin.issue_time.tzinfo is UTC
    assert origin.issue_time.hour == 4
