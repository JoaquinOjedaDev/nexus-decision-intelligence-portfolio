"""Strict contracts for the bounded R2A trustworthy-data core."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal, Self
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

from nexus.storage import ContentAddressedArtifact

R2Identifier = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=160,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    ),
]
R2Sha256 = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
R2FileName = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$"),
]


def normalize_r2_utc(value: datetime) -> datetime:
    """Reject naive timestamps and normalize equivalent instants to UTC."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include an explicit timezone offset")
    return value.astimezone(UTC)


class R2Record(BaseModel):
    """Closed and immutable base for R2A contracts."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
    )


class R2SourceFile(R2Record):
    """One byte-addressed source file in a reproducible source package."""

    dataset: R2Identifier
    file_name: R2FileName
    content_sha256: R2Sha256
    record_count: NonNegativeInt


class R2ExpectedControls(R2Record):
    """Exact smoke-profile controls declared by a source package."""

    accepted_inventory_adjustments: NonNegativeInt
    deferred_inventory_adjustments: NonNegativeInt
    fulfillment_rows: NonNegativeInt
    opening_inventory_rows: NonNegativeInt
    product_location_rows: NonNegativeInt
    purchase_order_rows: NonNegativeInt
    quarantine_rows: NonNegativeInt
    receipt_rows: NonNegativeInt
    supplier_rows: NonNegativeInt
    superseded_inventory_adjustments: NonNegativeInt


class R2SourceManifest(R2Record):
    """Manifest that binds generator configuration to exact source bytes."""

    schema_version: Literal["1.0.0"]
    classification: Literal["synthetic"]
    generator: Literal["nexus-r2-supply-network", "nexus-r2-learning-supply-network"]
    profile: Literal["smoke", "learning"]
    seed: NonNegativeInt
    start_date: date
    days: PositiveInt
    planning_cutoff: AwareDatetime
    early_replay_cutoff: AwareDatetime
    sources: tuple[R2SourceFile, ...]
    expected_controls: R2ExpectedControls

    _normalize_planning_cutoff = field_validator("planning_cutoff")(normalize_r2_utc)
    _normalize_early_cutoff = field_validator("early_replay_cutoff")(normalize_r2_utc)

    @model_validator(mode="after")
    def cutoffs_are_ordered_and_sources_are_unique(self) -> Self:
        expected_generator = (
            "nexus-r2-supply-network"
            if self.profile == "smoke"
            else "nexus-r2-learning-supply-network"
        )
        if self.generator != expected_generator:
            raise ValueError("source profile must match its declared generator")
        if self.early_replay_cutoff >= self.planning_cutoff:
            raise ValueError("early_replay_cutoff must precede planning_cutoff")
        files = [source.file_name for source in self.sources]
        datasets = [source.dataset for source in self.sources]
        if len(files) != len(set(files)) or len(datasets) != len(set(datasets)):
            raise ValueError("source file names and dataset names must be unique")
        return self


class R2Supplier(R2Record):
    """Synthetic supplier master record."""

    supplier_id: R2Identifier
    supplier_name: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=160),
    ]
    active: bool
    available_at: AwareDatetime

    _normalize_available_at = field_validator("available_at")(normalize_r2_utc)


class R2ProductLocation(R2Record):
    """Active SKU/location planning grain and its primary supplier."""

    sku_id: R2Identifier
    location_id: R2Identifier
    supplier_id: R2Identifier
    active: bool
    available_at: AwareDatetime

    _normalize_available_at = field_validator("available_at")(normalize_r2_utc)


class R2OpeningInventory(R2Record):
    """Opening stock for a replay partition."""

    sku_id: R2Identifier
    location_id: R2Identifier
    on_hand_qty: NonNegativeInt
    snapshot_at: AwareDatetime
    available_at: AwareDatetime

    _normalize_snapshot_at = field_validator("snapshot_at")(normalize_r2_utc)
    _normalize_available_at = field_validator("available_at")(normalize_r2_utc)

    @model_validator(mode="after")
    def availability_follows_snapshot(self) -> Self:
        if self.available_at < self.snapshot_at:
            raise ValueError("available_at cannot precede snapshot_at")
        if any(
            (
                self.snapshot_at.hour,
                self.snapshot_at.minute,
                self.snapshot_at.second,
                self.snapshot_at.microsecond,
            )
        ):
            raise ValueError("R2A opening snapshot must be at a UTC day boundary")
        return self


class R2FulfillmentOutcome(R2Record):
    """Daily demand and fulfilled quantity at the decision grain."""

    fulfillment_id: R2Identifier
    demand_date: date
    sku_id: R2Identifier
    location_id: R2Identifier
    demand_qty: NonNegativeInt
    fulfilled_qty: NonNegativeInt
    available_at: AwareDatetime

    _normalize_available_at = field_validator("available_at")(normalize_r2_utc)

    @model_validator(mode="after")
    def fulfillment_does_not_exceed_demand(self) -> Self:
        if self.fulfilled_qty > self.demand_qty:
            raise ValueError("fulfilled_qty cannot exceed demand_qty")
        if self.available_at.date() <= self.demand_date:
            raise ValueError("daily outcome is available only after the demand date")
        return self


class R2PurchaseOrder(R2Record):
    """One purchase-order line placed with the primary supplier."""

    order_id: R2Identifier
    sku_id: R2Identifier
    location_id: R2Identifier
    supplier_id: R2Identifier
    ordered_qty: PositiveInt
    ordered_at: AwareDatetime
    expected_at: AwareDatetime
    available_at: AwareDatetime

    _normalize_ordered_at = field_validator("ordered_at")(normalize_r2_utc)
    _normalize_expected_at = field_validator("expected_at")(normalize_r2_utc)
    _normalize_available_at = field_validator("available_at")(normalize_r2_utc)

    @model_validator(mode="after")
    def order_times_are_coherent(self) -> Self:
        if self.expected_at <= self.ordered_at:
            raise ValueError("expected_at must follow ordered_at")
        if self.available_at < self.ordered_at:
            raise ValueError("available_at cannot precede ordered_at")
        return self


class R2Receipt(R2Record):
    """Receipt against one purchase-order line."""

    receipt_id: R2Identifier
    order_id: R2Identifier
    sku_id: R2Identifier
    location_id: R2Identifier
    supplier_id: R2Identifier
    received_qty: PositiveInt
    received_at: AwareDatetime
    available_at: AwareDatetime

    _normalize_received_at = field_validator("received_at")(normalize_r2_utc)
    _normalize_available_at = field_validator("available_at")(normalize_r2_utc)

    @model_validator(mode="after")
    def receipt_availability_follows_receipt(self) -> Self:
        if self.available_at < self.received_at:
            raise ValueError("available_at cannot precede received_at")
        return self


class R2InventoryAdjustmentPayload(R2Record):
    """Revisioned payload for the one external R2A operational event."""

    logical_event_id: R2Identifier
    revision: PositiveInt
    sku_id: R2Identifier
    location_id: R2Identifier
    delta_qty: int
    reason_code: R2Identifier

    @model_validator(mode="after")
    def reject_noop_adjustment(self) -> Self:
        if self.delta_qty == 0:
            raise ValueError("delta_qty must not be zero")
        return self


class R2InventoryAdjustment(R2Record):
    """Contract-valid, flattened adjustment retained before cutoff resolution."""

    contract_version: R2Identifier
    event_id: UUID
    event_version: PositiveInt
    source: R2Identifier
    logical_event_id: R2Identifier
    revision: PositiveInt
    idempotency_key: R2Identifier
    occurred_at: AwareDatetime
    ingested_at: AwareDatetime
    sku_id: R2Identifier
    location_id: R2Identifier
    delta_qty: int
    reason_code: R2Identifier

    _normalize_occurred_at = field_validator("occurred_at")(normalize_r2_utc)
    _normalize_ingested_at = field_validator("ingested_at")(normalize_r2_utc)


class R2QuarantineReason(StrEnum):
    """Stable dispositions for data that cannot enter validated R2A records."""

    BATCH_CONTRACT_INVALID = "batch_contract_invalid"
    DUPLICATE_BUSINESS_KEY = "duplicate_business_key"
    EVENT_CONTRACT_INVALID = "event_contract_invalid"
    UNSUPPORTED_EVENT_TYPE = "unsupported_event_type"
    UNSUPPORTED_EVENT_VERSION = "unsupported_event_version"
    UNSUPPORTED_CONTRACT_VERSION = "unsupported_contract_version"
    SUBJECT_PAYLOAD_MISMATCH = "subject_payload_mismatch"
    DUPLICATE_EVENT_ID = "duplicate_event_id"
    DUPLICATE_IDEMPOTENCY_KEY = "duplicate_idempotency_key"
    IDENTITY_CONFLICT = "identity_conflict"
    UNKNOWN_PRODUCT_LOCATION = "unknown_product_location"
    UNKNOWN_SUPPLIER = "unknown_supplier"
    PRIMARY_SUPPLIER_MISMATCH = "primary_supplier_mismatch"
    UNKNOWN_ORDER = "unknown_order"
    CORRECTION_CHAIN_INVALID = "correction_chain_invalid"


class R2QuarantineRecord(R2Record):
    """Safe diagnostic for one rejected source record."""

    dataset: R2Identifier
    source_file: R2FileName
    source_row: PositiveInt
    reason_code: R2QuarantineReason
    detail: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=500),
    ]
    record_reference: R2Identifier


@dataclass(frozen=True, slots=True)
class R2GeneratedFixture:
    """Exact source package produced by the deterministic simulator."""

    source_dir: Path
    manifest_path: Path
    manifest: R2SourceManifest
    manifest_sha256: str
    manifest_file_sha256: str


@dataclass(frozen=True, slots=True)
class R2LandingResult:
    """Validated inputs, quarantine evidence, and immutable artifact references."""

    source_manifest: R2SourceManifest
    source_manifest_sha256: str
    source_manifest_file_sha256: str
    product_locations: tuple[R2ProductLocation, ...]
    suppliers: tuple[R2Supplier, ...]
    opening_inventory: tuple[R2OpeningInventory, ...]
    fulfillment_outcomes: tuple[R2FulfillmentOutcome, ...]
    purchase_orders: tuple[R2PurchaseOrder, ...]
    receipts: tuple[R2Receipt, ...]
    inventory_adjustments: tuple[R2InventoryAdjustment, ...]
    quarantined: tuple[R2QuarantineRecord, ...]
    validated_artifacts: tuple[ContentAddressedArtifact, ...]
    source_native_paths: tuple[Path, ...]

    def artifact(self, dataset: str) -> ContentAddressedArtifact:
        """Return one validated artifact or fail on a wiring error."""

        for artifact in self.validated_artifacts:
            if artifact.dataset == dataset:
                return artifact
        raise KeyError(f"unknown R2A data artifact: {dataset}")


@dataclass(frozen=True, slots=True)
class R2RebuildResult:
    """One complete dbt-backed trustworthy-data rebuild."""

    planning_cutoff: datetime
    source_manifest_sha256: str
    source_manifest_file_sha256: str
    semantic_manifest_sha256: str
    landing: R2LandingResult
    gold_artifacts: tuple[ContentAddressedArtifact, ...]
    gold_row_counts: dict[str, int]
    quality_counts: dict[str, int | None]
    summary_path: Path
    database_path: Path
    duration_seconds: float

    def gold_artifact(self, dataset: str) -> ContentAddressedArtifact:
        """Return one exported Gold artifact."""

        for artifact in self.gold_artifacts:
            if artifact.dataset == dataset:
                return artifact
        raise KeyError(f"unknown R2A Gold artifact: {dataset}")
