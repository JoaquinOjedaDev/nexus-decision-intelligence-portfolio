"""Typed records and result objects for the deterministic R1 data path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    NonNegativeInt,
    PositiveInt,
    StringConstraints,
    field_validator,
    model_validator,
)

Identifier = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=160,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    ),
]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]


def normalize_utc(value: datetime) -> datetime:
    """Reject naive datetimes and normalize equivalent instants to UTC."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include an explicit timezone offset")
    return value.astimezone(UTC)


class R1Record(BaseModel):
    """Closed, immutable, strictly typed base for R1 records."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
    )


class ProductLocationRecord(R1Record):
    """Planning attributes for one SKU/location decision grain."""

    sku_id: Identifier
    location_id: Identifier
    lead_time_days: PositiveInt
    safety_stock_qty: NonNegativeInt
    active: bool
    available_at: AwareDatetime

    _normalize_available_at = field_validator("available_at")(normalize_utc)


class DemandRecord(R1Record):
    """Observed daily demand at the SKU/location grain."""

    demand_date: date
    sku_id: Identifier
    location_id: Identifier
    demand_qty: NonNegativeInt
    available_at: AwareDatetime

    _normalize_available_at = field_validator("available_at")(normalize_utc)

    @model_validator(mode="after")
    def demand_cannot_be_known_before_its_date(self) -> Self:
        if self.available_at.date() < self.demand_date:
            raise ValueError("available_at cannot precede demand_date")
        return self


class InventorySnapshotRecord(R1Record):
    """Point-in-time inventory supplied by the operational source."""

    snapshot_at: AwareDatetime
    sku_id: Identifier
    location_id: Identifier
    on_hand_qty: NonNegativeInt
    on_order_qty: NonNegativeInt
    available_at: AwareDatetime

    _normalize_snapshot_at = field_validator("snapshot_at")(normalize_utc)
    _normalize_available_at = field_validator("available_at")(normalize_utc)

    @model_validator(mode="after")
    def availability_follows_snapshot(self) -> Self:
        if self.available_at < self.snapshot_at:
            raise ValueError("available_at cannot precede snapshot_at")
        return self


class InventoryAdjustmentPayload(R1Record):
    """Business payload carried by an ``inventory.adjusted`` envelope."""

    sku_id: Identifier
    location_id: Identifier
    delta_qty: int
    reason_code: Identifier

    @model_validator(mode="after")
    def reject_noop_adjustment(self) -> Self:
        if self.delta_qty == 0:
            raise ValueError("delta_qty must not be zero")
        return self


class AcceptedInventoryAdjustment(R1Record):
    """Flattened, validated inventory event retained in the raw layer."""

    event_id: UUID
    idempotency_key: Identifier
    source: Identifier
    subject_id: Identifier
    occurred_at: AwareDatetime
    ingested_at: AwareDatetime
    sku_id: Identifier
    location_id: Identifier
    delta_qty: int
    reason_code: Identifier

    _normalize_occurred_at = field_validator("occurred_at")(normalize_utc)
    _normalize_ingested_at = field_validator("ingested_at")(normalize_utc)


class GoldDemandRecord(R1Record):
    """Validated demand record made available to forecasting."""

    demand_date: date
    sku_id: Identifier
    location_id: Identifier
    demand_qty: NonNegativeInt
    available_at: AwareDatetime

    _normalize_available_at = field_validator("available_at")(normalize_utc)


class GoldInventoryRecord(R1Record):
    """Inventory position after applying accepted post-snapshot adjustments."""

    sku_id: Identifier
    location_id: Identifier
    on_hand_qty: NonNegativeInt
    on_order_qty: NonNegativeInt
    snapshot_at: AwareDatetime
    available_at: AwareDatetime
    as_of: AwareDatetime
    adjustment_event_count: NonNegativeInt

    _normalize_snapshot_at = field_validator("snapshot_at")(normalize_utc)
    _normalize_available_at = field_validator("available_at")(normalize_utc)
    _normalize_as_of = field_validator("as_of")(normalize_utc)


class QuarantineReason(StrEnum):
    """Stable machine-readable reasons for rejecting source records."""

    BATCH_CONTRACT_INVALID = "batch_contract_invalid"
    DUPLICATE_BUSINESS_KEY = "duplicate_business_key"
    EVENT_CONTRACT_INVALID = "event_contract_invalid"
    UNSUPPORTED_EVENT_TYPE = "unsupported_event_type"
    SUBJECT_PAYLOAD_MISMATCH = "subject_payload_mismatch"
    DUPLICATE_EVENT_ID = "duplicate_event_id"
    DUPLICATE_IDEMPOTENCY_KEY = "duplicate_idempotency_key"
    UNKNOWN_PRODUCT_LOCATION = "unknown_product_location"
    SNAPSHOT_AFTER_AS_OF = "snapshot_after_as_of"
    DEMAND_NOT_COMPLETE_AS_OF = "demand_not_complete_as_of"
    BATCH_NOT_AVAILABLE_AS_OF = "batch_not_available_as_of"
    EVENT_NOT_AVAILABLE_AS_OF = "event_not_available_as_of"
    EVENT_NOT_AFTER_SNAPSHOT = "event_not_after_snapshot"


class QuarantineRecord(R1Record):
    """Rejected source record with a deterministic reason and safe diagnostic."""

    dataset: Identifier
    source_file: Identifier
    source_row: PositiveInt
    reason_code: QuarantineReason
    detail: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
    record_reference: Identifier


@dataclass(frozen=True, slots=True)
class R1DataArtifact:
    """One content-addressed immutable Parquet output."""

    dataset: str
    path: Path
    content_sha256: str
    row_count: int


@dataclass(frozen=True, slots=True)
class R1DataPathResult:
    """Pure records and artifact references returned to an R1 orchestrator."""

    as_of: datetime
    product_locations: tuple[ProductLocationRecord, ...]
    gold_demand: tuple[GoldDemandRecord, ...]
    gold_inventory: tuple[GoldInventoryRecord, ...]
    accepted_inventory_adjustments: tuple[AcceptedInventoryAdjustment, ...]
    quarantined: tuple[QuarantineRecord, ...]
    artifacts: tuple[R1DataArtifact, ...]

    def artifact(self, dataset: str) -> R1DataArtifact:
        """Return one named artifact or fail loudly on an integration typo."""

        for artifact in self.artifacts:
            if artifact.dataset == dataset:
                return artifact
        raise KeyError(f"unknown R1 data artifact: {dataset}")

    def demand_for(self, sku_id: str, location_id: str) -> tuple[GoldDemandRecord, ...]:
        """Return ordered demand history for one decision grain."""

        return tuple(
            row
            for row in self.gold_demand
            if row.sku_id == sku_id and row.location_id == location_id
        )
