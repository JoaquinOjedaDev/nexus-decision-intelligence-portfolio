"""Strict executable contract for bounded R2B cutoff partition replay."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Annotated, Any, Literal, Self, cast

import yaml
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    StringConstraints,
    field_validator,
    model_validator,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_R2B_CONFIG_PATH = PROJECT_ROOT / "configs" / "orchestration" / "r2b-backfill.yaml"

R2BIdentifier = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=160,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    ),
]
R2BSha256 = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]


def _parse_datetime(value: object) -> object:
    if not isinstance(value, str):
        return value
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("planning_cutoff must be a valid ISO-8601 timestamp") from error


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("planning_cutoff must include an explicit timezone offset")
    return value.astimezone(UTC)


def _tuple_from_yaml(value: object) -> object:
    return tuple(value) if isinstance(value, list) else value


class R2BContract(BaseModel):
    """Closed and immutable base for orchestration contracts."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
    )


class R2BSourceContract(R2BContract):
    """Exact R2A source package authorized for the bounded partition replay."""

    generator: Literal["nexus-r2-supply-network"]
    source_manifest_sha256: R2BSha256
    source_manifest_file_sha256: R2BSha256


class R2BPartition(R2BContract):
    """One named point-in-time data partition."""

    name: Literal["early", "final"]
    planning_cutoff: AwareDatetime
    role: Literal["correction_visibility_probe", "release_snapshot"]

    _parse_cutoff = field_validator("planning_cutoff", mode="before")(_parse_datetime)
    _normalize_cutoff = field_validator("planning_cutoff")(_normalize_utc)


class R2BPartitionContract(R2BContract):
    """Stable key scheme and complete ordered cutoff set."""

    key_format: Literal["r2b_v1__manifest_sha256__cutoff_utc"]
    partitions: tuple[R2BPartition, ...]

    _freeze_partitions = field_validator("partitions", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def partitions_are_complete_unique_and_ordered(self) -> Self:
        if tuple(partition.name for partition in self.partitions) != ("early", "final"):
            raise ValueError("partitions must be ordered exactly early, final")
        if tuple(partition.role for partition in self.partitions) != (
            "correction_visibility_probe",
            "release_snapshot",
        ):
            raise ValueError("partition roles must match the frozen early/final contract")
        cutoffs = tuple(partition.planning_cutoff for partition in self.partitions)
        if any(left >= right for left, right in pairwise(cutoffs)):
            raise ValueError("partition cutoffs must be strictly increasing")
        return self


class R2BInjectedFailure(R2BContract):
    """Single deterministic failure used to prove bounded retry behavior."""

    partition: Literal["final"]
    stage: Literal["post_validation"]
    failing_attempts: Literal[1]


class R2BRetryContract(R2BContract):
    """Fail-fast retry envelope; unbounded retries are forbidden."""

    maximum_retries: Literal[1]
    delay_seconds: Literal[0]
    injected_failure: R2BInjectedFailure


class R2BEvidenceContract(R2BContract):
    """Mandatory parity and identity checks for the bounded gate."""

    require_direct_parity: Literal[True]
    require_reverse_order_replay: Literal[True]
    require_idempotent_rerun: Literal[True]
    bind_dagster_run_ids_to_execution_only: Literal[True]


class R2BResourceBudget(R2BContract):
    """Laptop-safe budget for the complete orchestration verifier."""

    wall_time_scope: Literal["r2b_complete_verification"]
    wall_time_seconds: Literal[240]
    peak_rss_scope: Literal["orchestrator_process_tree_high_water"]
    peak_rss_mib: Literal[2048]
    thread_scope: Literal["python_interpreter_threads_after_run"]
    maximum_python_threads: Literal[4]
    artifact_scope: Literal["r2b_governed_manifests_and_r2_snapshots"]
    artifact_mib: Literal[100]


class R2BConfig(R2BContract):
    """Complete executable R2B orchestration contract."""

    release: Literal["R2B"]
    contract_version: Literal["1.0.0"]
    profile: Literal["r2a-synthetic-contract-smoke"]
    seed: Literal[20260718]
    source_contract: R2BSourceContract
    partition_contract: R2BPartitionContract
    retry_contract: R2BRetryContract
    evidence_contract: R2BEvidenceContract
    resource_budget: R2BResourceBudget
    deferred_full_g2: tuple[R2BIdentifier, ...]

    _freeze_deferred = field_validator("deferred_full_g2", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def deferred_scope_is_explicit_and_unique(self) -> Self:
        if not self.deferred_full_g2 or len(set(self.deferred_full_g2)) != len(
            self.deferred_full_g2
        ):
            raise ValueError("deferred_full_g2 entries must be nonempty and unique")
        return self

    def partition_key(self, partition: R2BPartition) -> str:
        """Bind a partition to the exact source-manifest bytes and UTC cutoff."""

        cutoff = partition.planning_cutoff.strftime("%Y%m%dT%H%M%SZ")
        return f"r2b-v1__{self.source_contract.source_manifest_file_sha256}__{cutoff}"

    def partition_for_key(self, partition_key: str) -> R2BPartition:
        """Resolve only a key generated by this exact contract."""

        for partition in self.partition_contract.partitions:
            if self.partition_key(partition) == partition_key:
                return partition
        raise KeyError(f"unknown R2B partition key: {partition_key}")


def load_r2b_config(path: Path = DEFAULT_R2B_CONFIG_PATH) -> R2BConfig:
    """Load and strictly validate the frozen R2B YAML contract."""

    with path.open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError("R2B configuration root must be a mapping")
    return R2BConfig.model_validate(cast(dict[str, Any], payload))


def r2b_config_sha256(config: R2BConfig) -> str:
    """Hash the canonical semantic configuration independently from YAML formatting."""

    encoded = json.dumps(
        config.model_dump(mode="json"),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
