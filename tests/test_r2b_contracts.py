from __future__ import annotations

from copy import deepcopy
from datetime import UTC
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from pydantic import ValidationError

from nexus.orchestration.r2b_contracts import R2BConfig, load_r2b_config

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "orchestration" / "r2b-backfill.yaml"


def _payload() -> dict[str, Any]:
    value = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    return cast(dict[str, Any], value)


def test_r2b_config_loads_as_a_strict_frozen_bounded_contract() -> None:
    config = load_r2b_config(CONFIG_PATH)

    assert config.release == "R2B"
    assert config.contract_version == "1.0.0"
    assert config.seed == 20260718
    assert tuple(partition.name for partition in config.partition_contract.partitions) == (
        "early",
        "final",
    )
    assert tuple(partition.role for partition in config.partition_contract.partitions) == (
        "correction_visibility_probe",
        "release_snapshot",
    )
    assert all(
        partition.planning_cutoff.tzinfo is UTC
        for partition in config.partition_contract.partitions
    )
    assert config.retry_contract.maximum_retries == 1
    assert config.retry_contract.delay_seconds == 0
    assert config.retry_contract.injected_failure.model_dump() == {
        "partition": "final",
        "stage": "post_validation",
        "failing_attempts": 1,
    }
    assert config.resource_budget.model_dump() == {
        "wall_time_scope": "r2b_complete_verification",
        "wall_time_seconds": 240,
        "peak_rss_scope": "orchestrator_process_tree_high_water",
        "peak_rss_mib": 2048,
        "thread_scope": "python_interpreter_threads_after_run",
        "maximum_python_threads": 4,
        "artifact_scope": "r2b_governed_manifests_and_r2_snapshots",
        "artifact_mib": 100,
    }

    with pytest.raises(ValidationError, match="frozen"):
        config.retry_contract.maximum_retries = 2


def test_partition_keys_bind_the_full_manifest_file_hash_and_utc_cutoff() -> None:
    config = load_r2b_config(CONFIG_PATH)
    manifest_hash = config.source_contract.source_manifest_file_sha256

    keys = tuple(
        config.partition_key(partition) for partition in config.partition_contract.partitions
    )

    assert len(manifest_hash) == 64
    assert keys == (
        f"r2b-v1__{manifest_hash}__20260620T000000Z",
        f"r2b-v1__{manifest_hash}__20260630T000000Z",
    )
    assert all(len(key.split("__")[1]) == 64 for key in keys)
    assert tuple(config.partition_for_key(key).name for key in keys) == ("early", "final")
    with pytest.raises(KeyError, match="unknown R2B partition key"):
        config.partition_for_key(f"r2b-v1__{manifest_hash[:16]}__20260620T000000Z")


def test_equivalent_offset_cutoff_is_normalized_to_utc() -> None:
    payload = _payload()
    payload["partition_contract"]["partitions"][0]["planning_cutoff"] = "2026-06-19T21:00:00-03:00"

    config = R2BConfig.model_validate(payload)

    cutoff = config.partition_contract.partitions[0].planning_cutoff
    assert cutoff.tzinfo is UTC
    assert cutoff.isoformat() == "2026-06-20T00:00:00+00:00"


def test_naive_or_invalid_cutoffs_and_incomplete_partitions_fail_closed() -> None:
    naive = _payload()
    naive["partition_contract"]["partitions"][0]["planning_cutoff"] = "2026-06-20T00:00:00"
    with pytest.raises(ValidationError, match="timezone info"):
        R2BConfig.model_validate(naive)

    invalid = _payload()
    invalid["partition_contract"]["partitions"][0]["planning_cutoff"] = "not-a-date"
    with pytest.raises(ValidationError, match="valid ISO-8601"):
        R2BConfig.model_validate(invalid)

    incomplete = _payload()
    incomplete["partition_contract"]["partitions"] = incomplete["partition_contract"]["partitions"][
        :1
    ]
    with pytest.raises(ValidationError, match="ordered exactly early, final"):
        R2BConfig.model_validate(incomplete)

    reversed_cutoffs = _payload()
    reversed_cutoffs["partition_contract"]["partitions"][0]["planning_cutoff"] = (
        "2026-07-01T00:00:00Z"
    )
    with pytest.raises(ValidationError, match="strictly increasing"):
        R2BConfig.model_validate(reversed_cutoffs)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("retry_contract", "maximum_retries"), 2, "Input should be 1"),
        (("retry_contract", "delay_seconds"), 1, "Input should be 0"),
        (("resource_budget", "wall_time_seconds"), 241, "Input should be 240"),
        (("resource_budget", "peak_rss_mib"), 2047, "Input should be 2048"),
        (("resource_budget", "maximum_python_threads"), 5, "Input should be 4"),
        (("resource_budget", "artifact_mib"), 101, "Input should be 100"),
    ],
)
def test_retry_and_resource_boundaries_fail_closed(
    path: tuple[str, str],
    value: object,
    message: str,
) -> None:
    payload = deepcopy(_payload())
    payload[path[0]][path[1]] = value

    with pytest.raises(ValidationError, match=message):
        R2BConfig.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("wall_time_scope", "whole_cli_process"),
        ("peak_rss_scope", "python_process_lifetime_high_water"),
        ("thread_scope", "operating_system_threads"),
        ("artifact_scope", "artifact_directory_recursive"),
    ],
)
def test_resource_measurement_scopes_are_not_configurable(
    field: str,
    invalid_value: str,
) -> None:
    payload = _payload()
    payload["resource_budget"][field] = invalid_value

    with pytest.raises(ValidationError, match="Input should be"):
        R2BConfig.model_validate(payload)


def test_misnamed_partitions_exact_retry_fields_and_unknown_keys_fail_closed() -> None:
    misnamed = _payload()
    misnamed["partition_contract"]["partitions"][0]["name"] = "historical"
    with pytest.raises(ValidationError, match="early"):
        R2BConfig.model_validate(misnamed)

    wrong_failure = _payload()
    wrong_failure["retry_contract"]["injected_failure"]["partition"] = "early"
    with pytest.raises(ValidationError, match="final"):
        R2BConfig.model_validate(wrong_failure)

    extra = _payload()
    extra["unregistered_setting"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        R2BConfig.model_validate(extra)
