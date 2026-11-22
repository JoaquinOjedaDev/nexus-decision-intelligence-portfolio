"""Strict contracts for representative, partitioned R2C synthetic data profiles."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal, Self, cast

import yaml
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    StringConstraints,
    field_validator,
    model_validator,
)

R2CProfileName = Literal["smoke", "demo", "full"]
R2CCohortName = Literal["smooth", "intermittent", "erratic", "lumpy", "cold_start"]
R2CDatasetName = Literal[
    "representative.suppliers",
    "representative.product_locations",
    "representative.demand_history",
    "representative.lead_time_history",
]
R2CSha256 = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]


class R2CContract(BaseModel):
    """Closed, immutable base for R2C configuration and evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)


class R2CCohortCounts(R2CContract):
    """Exact simulator-truth cohort allocation for a profile."""

    smooth: NonNegativeInt
    intermittent: NonNegativeInt
    erratic: NonNegativeInt
    lumpy: NonNegativeInt
    cold_start: NonNegativeInt

    def as_dict(self) -> dict[R2CCohortName, int]:
        """Return cohort counts in the frozen presentation order."""

        return {
            "smooth": self.smooth,
            "intermittent": self.intermittent,
            "erratic": self.erratic,
            "lumpy": self.lumpy,
            "cold_start": self.cold_start,
        }


class R2CResourceBudget(R2CContract):
    """Declared execution envelope; a plan is not evidence that the budget was met."""

    maximum_runtime_seconds: PositiveInt
    peak_rss_mib: PositiveInt
    maximum_artifact_mib: PositiveInt
    maximum_python_threads: PositiveInt


class R2CProfileConfig(R2CContract):
    """Versioned profile contract with unambiguous Cartesian dimensions."""

    name: R2CProfileName
    contract_version: Literal["1.0.0"]
    classification: Literal["synthetic"]
    seed: NonNegativeInt
    start_date: date
    sku_count: PositiveInt
    location_count: PositiveInt
    series_count: PositiveInt
    supplier_count: PositiveInt
    history_days: PositiveInt
    partition_granularity: Literal["month"]
    write_batch_rows: PositiveInt
    order_cycle_days: PositiveInt
    forecast_horizons_days: tuple[PositiveInt, ...]
    holdout_days: PositiveInt
    cold_start_history_days: PositiveInt
    stress_days: PositiveInt
    cohort_counts: R2CCohortCounts
    resource_budget: R2CResourceBudget
    requires_explicit_opt_in: bool

    @field_validator("forecast_horizons_days", mode="before")
    @classmethod
    def freeze_horizons(cls, value: object) -> object:
        """Normalize YAML lists before strict tuple validation."""

        return tuple(cast(list[object] | tuple[object, ...], value))

    @model_validator(mode="after")
    def dimensions_and_windows_are_coherent(self) -> Self:
        """Reject ambiguous scale, unsafe profile names, and unusable evaluation windows."""

        if self.series_count != self.sku_count * self.location_count:
            raise ValueError("series_count must equal sku_count * location_count")
        if sum(self.cohort_counts.as_dict().values()) != self.series_count:
            raise ValueError("cohort_counts must sum exactly to series_count")
        if self.supplier_count > self.series_count:
            raise ValueError("supplier_count cannot exceed series_count")
        if self.forecast_horizons_days != (7, 14, 28):
            raise ValueError("forecast_horizons_days must be frozen at 7, 14, 28")
        if self.history_days < self.holdout_days + 2 * max(self.forecast_horizons_days):
            raise ValueError("history_days is too short for the frozen holdout and horizons")
        if self.cold_start_history_days > self.history_days:
            raise ValueError("cold_start_history_days cannot exceed history_days")
        if self.cold_start_history_days < max(self.forecast_horizons_days):
            raise ValueError("cold starts must contain at least the longest forecast horizon")
        if self.stress_days > self.holdout_days:
            raise ValueError("stress_days cannot exceed holdout_days")
        if self.name == "full" and not self.requires_explicit_opt_in:
            raise ValueError("the full scale plan must require explicit opt-in")
        if self.name != "full" and self.requires_explicit_opt_in:
            raise ValueError("only the full scale plan may require explicit opt-in")
        expected_shapes = {
            "smoke": (20, 2, 40, 5, 84, (14, 10, 8, 4, 4)),
            "demo": (100, 5, 500, 10, 730, (175, 125, 100, 50, 50)),
            "full": (3049, 10, 30490, 100, 1941, (10672, 7623, 6098, 3049, 3048)),
        }
        shape = (
            self.sku_count,
            self.location_count,
            self.series_count,
            self.supplier_count,
            self.history_days,
            tuple(self.cohort_counts.as_dict().values()),
        )
        if shape != expected_shapes[self.name]:
            raise ValueError(f"{self.name} dimensions and cohorts do not match contract 1.0.0")
        expected_budgets = {
            "smoke": (120, 1536, 100, 4),
            "demo": (900, 3072, 1024, 4),
            "full": (7200, 16384, 32768, 16),
        }
        budget = (
            self.resource_budget.maximum_runtime_seconds,
            self.resource_budget.peak_rss_mib,
            self.resource_budget.maximum_artifact_mib,
            self.resource_budget.maximum_python_threads,
        )
        if budget != expected_budgets[self.name]:
            raise ValueError(f"{self.name} resource budget does not match contract 1.0.0")
        return self

    @property
    def end_date(self) -> date:
        """Return the inclusive final date represented by the profile."""

        from datetime import timedelta

        return self.start_date + timedelta(days=self.history_days - 1)


class R2CShard(R2CContract):
    """One immutable physical shard with a storage-neutral logical identity."""

    dataset: R2CDatasetName
    partition: Annotated[
        str,
        StringConstraints(pattern=r"^(all|month=\d{4}-\d{2})$"),
    ]
    relative_path: Annotated[str, StringConstraints(min_length=1, max_length=500)]
    row_count: NonNegativeInt
    semantic_sha256: R2CSha256
    schema_sha256: R2CSha256
    file_sha256: R2CSha256
    file_bytes: NonNegativeInt

    @field_validator("relative_path")
    @classmethod
    def relative_path_is_portable(cls, value: str) -> str:
        """Keep host paths and traversal out of promotable manifests."""

        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts or "\\" in value:
            raise ValueError("relative_path must be a safe POSIX-relative path")
        return value


class R2CControls(R2CContract):
    """Observed controls calculated from the generated logical rows."""

    supplier_rows: PositiveInt
    product_location_rows: PositiveInt
    demand_rows: PositiveInt
    lead_time_rows: PositiveInt
    promotion_rows: NonNegativeInt
    disruption_rows: NonNegativeInt
    stockout_censored_rows: NonNegativeInt
    completed_orders: NonNegativeInt
    partial_orders: NonNegativeInt
    open_orders: NonNegativeInt
    late_orders: NonNegativeInt
    late_arriving_demand_rows: NonNegativeInt
    cohort_counts: R2CCohortCounts


class R2CManifest(R2CContract):
    """Deterministic package manifest separating semantic and physical identity."""

    schema_version: Literal["1.0.0"]
    generator: Literal["nexus-r2c-representative-supply-network"]
    classification: Literal["synthetic"]
    profile: R2CProfileName
    profile_contract: R2CProfileConfig
    profile_contract_sha256: R2CSha256
    dataset_semantic_sha256: R2CSha256
    start_date: date
    end_date: date
    generated_through: AwareDatetime
    shards: tuple[R2CShard, ...]
    controls: R2CControls

    @field_validator("shards", mode="before")
    @classmethod
    def freeze_shards(cls, value: object) -> object:
        """Normalize decoded JSON arrays before strict tuple validation."""

        return tuple(cast(list[object] | tuple[object, ...], value))

    @field_validator("generated_through")
    @classmethod
    def normalize_generated_through(cls, value: datetime) -> datetime:
        """Require an aware instant and normalize it to UTC."""

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("generated_through must include an explicit timezone")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def manifest_is_ordered_and_unique(self) -> Self:
        """Make manifest composition deterministic and reject shard aliasing."""

        identities = [(shard.dataset, shard.partition) for shard in self.shards]
        paths = [shard.relative_path for shard in self.shards]
        if identities != sorted(identities):
            raise ValueError("shards must be ordered by dataset and partition")
        if len(identities) != len(set(identities)) or len(paths) != len(set(paths)):
            raise ValueError("shard identities and paths must be unique")
        if self.end_date < self.start_date:
            raise ValueError("end_date cannot precede start_date")
        if self.profile != self.profile_contract.name:
            raise ValueError("profile must match the resolved profile contract")
        if self.profile_contract_sha256 != r2c_profile_sha256(self.profile_contract):
            raise ValueError("profile_contract_sha256 does not bind the resolved profile")
        if (
            self.start_date != self.profile_contract.start_date
            or self.end_date != self.profile_contract.end_date
        ):
            raise ValueError("manifest dates must match the resolved profile contract")
        expected_generated_through = datetime.combine(
            self.end_date + timedelta(days=3),
            time(hour=3),
            tzinfo=UTC,
        )
        if self.generated_through != expected_generated_through:
            raise ValueError("generated_through must match the frozen observation boundary")
        return self


class R2CProfilePlan(R2CContract):
    """Safe sizing output available without materializing a profile."""

    profile: R2CProfileName
    series_count: PositiveInt
    demand_rows: PositiveInt
    maximum_order_rows: PositiveInt
    estimated_uncompressed_gib: float = Field(ge=0)
    minimum_free_disk_gib: float = Field(gt=0)
    requires_explicit_opt_in: bool
    executable_by_default: bool


class R2CVerificationResult(R2CContract):
    """Machine-readable bounded-gate result."""

    passed: bool
    profile: R2CProfileName
    profile_contract_sha256: R2CSha256
    dataset_semantic_sha256: R2CSha256
    checks: dict[str, bool]
    controls: R2CControls
    wall_time_seconds: float = Field(ge=0)
    peak_rss_mib: float = Field(ge=0)
    artifact_bytes: NonNegativeInt
    python_threads_after_run: PositiveInt
    summary_path: str


@dataclass(frozen=True, slots=True)
class R2CGeneratedProfile:
    """Filesystem result for one governed representative profile."""

    output_dir: Path
    manifest_path: Path
    manifest: R2CManifest
    manifest_file_sha256: str


def canonical_r2c_sha256(value: object) -> str:
    """Hash canonical JSON for profile and manifest identities."""

    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def r2c_profile_sha256(config: R2CProfileConfig) -> str:
    """Hash the complete semantic and execution profile contract."""

    return canonical_r2c_sha256(config.model_dump(mode="json"))


def load_r2c_profile(path: Path) -> R2CProfileConfig:
    """Load one strict R2C profile YAML."""

    with path.open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError("R2C profile root must be a mapping")
    return R2CProfileConfig.model_validate(cast(dict[str, Any], payload))
