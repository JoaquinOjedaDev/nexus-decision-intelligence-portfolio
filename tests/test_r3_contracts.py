from __future__ import annotations

from copy import deepcopy
from datetime import UTC
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from pydantic import ValidationError

from nexus.modeling.r3_contracts import R3AConfig, load_r3a_config

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "modeling" / "r3a-smoke.yaml"


def _payload() -> dict[str, Any]:
    value = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    return cast(dict[str, Any], value)


def test_r3a_smoke_config_loads_as_a_strict_frozen_utc_contract() -> None:
    config = load_r3a_config(CONFIG_PATH)

    assert config.release == "R3A"
    assert config.forecast_contract.quantiles == (0.025, 0.1, 0.5, 0.9, 0.975)
    assert config.feature_contract.lags_days == (1, 7, 14)
    assert config.feature_contract.rolling_windows_days == (7, 14)
    assert config.evaluation.bootstrap.confidence_level == 0.95
    assert config.promotion.current_champion == "seasonal_naive_empirical_intervals"
    assert config.resource_budget.wall_time_scope == "r3a_modeling_materialization"
    assert config.resource_budget.peak_rss_scope == "python_process_lifetime_high_water"
    assert config.resource_budget.thread_scope == "python_interpreter_threads_after_materialization"
    assert config.resource_budget.artifact_scope == "governed_scientific_artifacts"
    resource_fields = type(config.resource_budget).model_fields
    assert "tracking_store_mib" not in resource_fields
    assert "shutdown_seconds" not in resource_fields
    assert tuple(origin.role for origin in config.evaluation.rolling_origins) == (
        "diagnostic",
        "calibration",
        "final_stress",
    )
    assert all(origin.issue_time.tzinfo is UTC for origin in config.evaluation.rolling_origins)

    with pytest.raises(ValidationError, match="frozen"):
        config.resource_budget.wall_time_seconds = 0


def test_equivalent_offset_origin_is_normalized_to_utc() -> None:
    payload = _payload()
    payload["evaluation"]["rolling_origins"][0]["issue_time"] = "2026-06-08T01:00:00-03:00"

    config = R3AConfig.model_validate(payload)

    assert config.evaluation.rolling_origins[0].issue_time.tzinfo is UTC
    assert config.evaluation.rolling_origins[0].issue_time.hour == 4


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("forecast_contract", "quantiles"), [0.1, 0.5, 0.5, 0.9], "strictly increasing"),
        (("forecast_contract", "quantiles"), [0.1, 0.2, 0.9], "0.5 median"),
        (
            ("forecast_contract", "quantiles"),
            [0.05, 0.1, 0.5, 0.9, 0.95],
            "must exactly match",
        ),
        (("feature_contract", "lags_days"), [7, 1], "strictly increasing"),
        (("feature_contract", "lags_days"), [1, 7], "requires lags_days"),
        (("feature_contract", "rolling_windows_days"), [7, 7], "strictly increasing"),
        (
            ("feature_contract", "rolling_windows_days"),
            [7, 14, 21],
            "requires rolling_windows_days",
        ),
        (("feature_contract", "version"), "r3a-pit-v2", "r3a-pit-v1"),
        (
            ("feature_contract", "features"),
            ["lag_demand", "rolling_mean"],
            "exact ordered feature set",
        ),
        (("feature_contract", "fit_scope"), "full_history", "each_origin_only"),
        (("resource_budget", "wall_time_seconds"), 0, "greater than 0"),
    ],
)
def test_invalid_probability_feature_and_budget_contracts_fail_closed(
    path: tuple[str, str],
    value: object,
    message: str,
) -> None:
    payload = deepcopy(_payload())
    payload[path[0]][path[1]] = value

    with pytest.raises(ValidationError, match=message):
        R3AConfig.model_validate(payload)


def test_naive_or_misordered_origins_and_unknown_fields_fail_closed() -> None:
    naive = _payload()
    naive["evaluation"]["rolling_origins"][0]["issue_time"] = "2026-06-08T04:00:00"
    with pytest.raises(ValidationError, match="timezone info"):
        R3AConfig.model_validate(naive)

    misordered = _payload()
    misordered["evaluation"]["rolling_origins"][0]["role"] = "calibration"
    with pytest.raises(ValidationError, match="unique and ordered"):
        R3AConfig.model_validate(misordered)

    extra = _payload()
    extra["unregistered_setting"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        R3AConfig.model_validate(extra)


def test_executable_scientific_and_resource_choices_are_not_configurable() -> None:
    confidence = _payload()
    confidence["evaluation"]["bootstrap"]["confidence_level"] = 0.90
    with pytest.raises(ValidationError, match=r"exactly 0\.95"):
        R3AConfig.model_validate(confidence)

    champion = _payload()
    champion["promotion"]["current_champion"] = "another_valid_model_name"
    with pytest.raises(ValidationError, match="seasonal_naive_empirical_intervals"):
        R3AConfig.model_validate(champion)

    scopes = {
        "wall_time_scope": "whole_cli_process",
        "peak_rss_scope": "modeling_delta_rss",
        "thread_scope": "operating_system_threads",
        "artifact_scope": "artifact_directory_recursive",
    }
    for field, invalid_value in scopes.items():
        payload = _payload()
        payload["resource_budget"][field] = invalid_value
        with pytest.raises(ValidationError, match="Input should be"):
            R3AConfig.model_validate(payload)

    for removed_field in ("tracking_store_mib", "shutdown_seconds"):
        payload = _payload()
        payload["resource_budget"][removed_field] = 1
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            R3AConfig.model_validate(payload)
