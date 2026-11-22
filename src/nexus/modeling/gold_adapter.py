"""Fail-closed, in-memory adapter from the R2 Gold contract into R3A."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Annotated, Final, Literal, Self, cast

import pyarrow.parquet as pq
from pydantic import (
    AwareDatetime,
    NonNegativeInt,
    StringConstraints,
    field_validator,
    model_validator,
)

from nexus.data.r2_models import R2RebuildResult
from nexus.modeling.r3_contracts import (
    R3Contract,
    R3Identifier,
    R3Sha256,
    normalize_r3_utc,
)
from nexus.storage import canonical_rows_sha256, canonical_schema_sha256

R3_DEMAND_SOURCE_DATASET: Final = "gold.r2_demand_history"
_EXPECTED_COLUMNS = (
    "demand_history_key",
    "demand_date",
    "sku_location_key",
    "sku_id",
    "location_id",
    "demand_qty",
    "fulfillment_record_count",
    "latest_available_at",
)
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$"
)
_SnapshotName = Annotated[
    str,
    StringConstraints(pattern=r"^r3a\.registered\.demand_history$"),
]


class R3GoldAdapterError(RuntimeError):
    """The R2 artifact cannot safely cross the R3A modeling boundary."""


class R3DemandObservation(R3Contract):
    """One parsed daily demand observation with point-in-time availability."""

    demand_history_key: R3Identifier
    demand_date: date
    sku_location_key: R3Identifier
    sku_id: R3Identifier
    location_id: R3Identifier
    demand_qty: NonNegativeInt
    fulfillment_record_count: NonNegativeInt
    latest_available_at: AwareDatetime

    _normalize_availability = field_validator("latest_available_at")(normalize_r3_utc)

    @model_validator(mode="after")
    def source_keys_and_times_are_coherent(self) -> Self:
        expected_location_key = f"{self.sku_id}::{self.location_id}"
        if self.sku_location_key != expected_location_key:
            raise ValueError("sku_location_key does not match sku_id and location_id")
        expected_history_key = f"{self.demand_date.isoformat()}::{expected_location_key}"
        if self.demand_history_key != expected_history_key:
            raise ValueError("demand_history_key does not match the observation grain")
        if self.latest_available_at.date() <= self.demand_date:
            raise ValueError("daily demand must become available after its demand date")
        return self


class R3DemandSnapshotContract(R3Contract):
    """Registered lineage identity for one exact R2-to-R3 demand snapshot."""

    contract_version: Literal["1.0.0"]
    registered_name: _SnapshotName
    source_dataset: Literal["gold.r2_demand_history"]
    semantic_manifest_sha256: R3Sha256
    source_manifest_sha256: R3Sha256
    source_manifest_file_sha256: R3Sha256
    planning_cutoff: AwareDatetime
    source_content_sha256: R3Sha256
    source_schema_sha256: R3Sha256
    source_row_count: NonNegativeInt
    registration_sha256: R3Sha256

    _normalize_cutoff = field_validator("planning_cutoff")(normalize_r3_utc)

    @classmethod
    def register(
        cls,
        *,
        semantic_manifest_sha256: str,
        source_manifest_sha256: str,
        source_manifest_file_sha256: str,
        planning_cutoff: datetime,
        source_content_sha256: str,
        source_schema_sha256: str,
        source_row_count: int,
    ) -> R3DemandSnapshotContract:
        """Create a self-verifying registration over data and upstream lineage."""

        normalized_cutoff = normalize_r3_utc(planning_cutoff)
        identity = cls._identity_payload(
            semantic_manifest_sha256=semantic_manifest_sha256,
            source_manifest_sha256=source_manifest_sha256,
            source_manifest_file_sha256=source_manifest_file_sha256,
            planning_cutoff=normalized_cutoff,
            source_content_sha256=source_content_sha256,
            source_schema_sha256=source_schema_sha256,
            source_row_count=source_row_count,
        )
        return cls(
            contract_version="1.0.0",
            registered_name="r3a.registered.demand_history",
            source_dataset=R3_DEMAND_SOURCE_DATASET,
            semantic_manifest_sha256=semantic_manifest_sha256,
            source_manifest_sha256=source_manifest_sha256,
            source_manifest_file_sha256=source_manifest_file_sha256,
            planning_cutoff=normalized_cutoff,
            source_content_sha256=source_content_sha256,
            source_schema_sha256=source_schema_sha256,
            source_row_count=source_row_count,
            registration_sha256=canonical_rows_sha256([identity]),
        )

    @staticmethod
    def _identity_payload(
        *,
        semantic_manifest_sha256: str,
        source_manifest_sha256: str,
        source_manifest_file_sha256: str,
        planning_cutoff: datetime,
        source_content_sha256: str,
        source_schema_sha256: str,
        source_row_count: int,
    ) -> dict[str, object]:
        return {
            "contract_version": "1.0.0",
            "planning_cutoff": normalize_r3_utc(planning_cutoff).isoformat().replace("+00:00", "Z"),
            "registered_name": "r3a.registered.demand_history",
            "semantic_manifest_sha256": semantic_manifest_sha256,
            "source_content_sha256": source_content_sha256,
            "source_dataset": R3_DEMAND_SOURCE_DATASET,
            "source_manifest_file_sha256": source_manifest_file_sha256,
            "source_manifest_sha256": source_manifest_sha256,
            "source_row_count": source_row_count,
            "source_schema_sha256": source_schema_sha256,
        }

    @model_validator(mode="after")
    def registration_matches_bound_lineage(self) -> Self:
        expected = canonical_rows_sha256(
            [
                self._identity_payload(
                    semantic_manifest_sha256=self.semantic_manifest_sha256,
                    source_manifest_sha256=self.source_manifest_sha256,
                    source_manifest_file_sha256=self.source_manifest_file_sha256,
                    planning_cutoff=self.planning_cutoff,
                    source_content_sha256=self.source_content_sha256,
                    source_schema_sha256=self.source_schema_sha256,
                    source_row_count=self.source_row_count,
                )
            ]
        )
        if self.registration_sha256 != expected:
            raise ValueError("registration_sha256 does not bind the declared lineage")
        return self


class R3DemandSnapshot(R3Contract):
    """Immutable in-memory modeling input; persistence belongs to the R3 artifact layer."""

    contract: R3DemandSnapshotContract
    observations: tuple[R3DemandObservation, ...]

    @model_validator(mode="after")
    def rows_match_the_registered_contract(self) -> Self:
        if len(self.observations) != self.contract.source_row_count:
            raise ValueError("snapshot row count does not match its registration")
        grains = [(row.sku_id, row.location_id, row.demand_date) for row in self.observations]
        if len(grains) != len(set(grains)):
            raise ValueError("demand observations must be unique by sku-location-date")
        if grains != sorted(grains):
            raise ValueError("demand observations must use stable sku-location-date order")
        if any(
            row.latest_available_at > self.contract.planning_cutoff for row in self.observations
        ):
            raise ValueError("snapshot contains data unavailable at the planning cutoff")
        return self


def _parse_date(value: object) -> date:
    if type(value) is not str or _DATE_PATTERN.fullmatch(value) is None:
        raise R3GoldAdapterError("demand_date must be a canonical YYYY-MM-DD string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise R3GoldAdapterError("demand_date is not a valid calendar date") from error
    if parsed.isoformat() != value:
        raise R3GoldAdapterError("demand_date must use canonical ISO-8601 form")
    return parsed


def _parse_timestamp(value: object) -> datetime:
    if type(value) is not str or _TIMESTAMP_PATTERN.fullmatch(value) is None:
        raise R3GoldAdapterError(
            "latest_available_at must be an explicit timezone-aware ISO-8601 string"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return normalize_r3_utc(parsed)
    except ValueError as error:
        raise R3GoldAdapterError("latest_available_at is not a valid timestamp") from error


def _require_string(row: dict[str, object], field: str) -> str:
    value = row[field]
    if type(value) is not str:
        raise R3GoldAdapterError(f"{field} must be a string")
    return value


def _require_nonnegative_integer(row: dict[str, object], field: str) -> int:
    value = row[field]
    if type(value) is not int or value < 0:
        raise R3GoldAdapterError(f"{field} must be a nonnegative integer")
    return value


def _parse_observation(
    row: dict[str, object],
    *,
    planning_cutoff: datetime,
) -> R3DemandObservation:
    if tuple(row) != _EXPECTED_COLUMNS or set(row) != set(_EXPECTED_COLUMNS):
        raise R3GoldAdapterError("R2 demand history columns do not match the exact contract")
    demand_date = _parse_date(row["demand_date"])
    latest_available_at = _parse_timestamp(row["latest_available_at"])
    if latest_available_at > planning_cutoff:
        raise R3GoldAdapterError("R2 demand history contains a late-arriving future value")
    try:
        return R3DemandObservation(
            demand_history_key=_require_string(row, "demand_history_key"),
            demand_date=demand_date,
            sku_location_key=_require_string(row, "sku_location_key"),
            sku_id=_require_string(row, "sku_id"),
            location_id=_require_string(row, "location_id"),
            demand_qty=_require_nonnegative_integer(row, "demand_qty"),
            fulfillment_record_count=_require_nonnegative_integer(row, "fulfillment_record_count"),
            latest_available_at=latest_available_at,
        )
    except ValueError as error:
        raise R3GoldAdapterError(f"malformed R2 demand observation: {error}") from error


def adapt_r2_demand_history(rebuild: R2RebuildResult) -> R3DemandSnapshot:
    """Verify and parse the exact R2 demand Gold artifact without writing any files."""

    try:
        planning_cutoff = normalize_r3_utc(rebuild.planning_cutoff)
    except ValueError as error:
        raise R3GoldAdapterError("R2 rebuild planning cutoff must be timezone-aware") from error

    try:
        artifact = rebuild.gold_artifact(R3_DEMAND_SOURCE_DATASET)
    except KeyError as error:
        raise R3GoldAdapterError(
            f"R2 rebuild is missing required Gold dataset {R3_DEMAND_SOURCE_DATASET}"
        ) from error
    if artifact.dataset != R3_DEMAND_SOURCE_DATASET:
        raise R3GoldAdapterError("R2 gold_artifact returned the wrong dataset")

    try:
        table = pq.read_table(artifact.path)
        raw_rows = cast(list[dict[str, object]], table.to_pylist())
        observed_content_sha256 = canonical_rows_sha256(raw_rows)
        observed_schema_sha256 = canonical_schema_sha256(table.schema)
    except Exception as error:
        raise R3GoldAdapterError(f"cannot verify R2 Gold Parquet: {error}") from error

    if table.num_rows != artifact.row_count:
        raise R3GoldAdapterError("R2 Gold row count does not match its artifact contract")
    if observed_schema_sha256 != artifact.schema_sha256:
        raise R3GoldAdapterError("R2 Gold schema hash does not match its artifact contract")
    if observed_content_sha256 != artifact.content_sha256:
        raise R3GoldAdapterError("R2 Gold content hash does not match its artifact contract")
    if tuple(table.column_names) != _EXPECTED_COLUMNS:
        raise R3GoldAdapterError("R2 demand history schema has unexpected columns or order")

    observations = tuple(
        sorted(
            (_parse_observation(row, planning_cutoff=planning_cutoff) for row in raw_rows),
            key=lambda row: (row.sku_id, row.location_id, row.demand_date),
        )
    )
    grains = [(row.sku_id, row.location_id, row.demand_date) for row in observations]
    if len(grains) != len(set(grains)):
        raise R3GoldAdapterError("R2 demand history contains duplicate sku-location-date rows")

    try:
        contract = R3DemandSnapshotContract.register(
            semantic_manifest_sha256=rebuild.semantic_manifest_sha256,
            source_manifest_sha256=rebuild.source_manifest_sha256,
            source_manifest_file_sha256=rebuild.source_manifest_file_sha256,
            planning_cutoff=planning_cutoff,
            source_content_sha256=artifact.content_sha256,
            source_schema_sha256=artifact.schema_sha256,
            source_row_count=artifact.row_count,
        )
        return R3DemandSnapshot(contract=contract, observations=observations)
    except ValueError as error:
        raise R3GoldAdapterError(f"cannot register R3A demand snapshot: {error}") from error
