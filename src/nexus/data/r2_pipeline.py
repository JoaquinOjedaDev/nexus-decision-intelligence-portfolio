"""Immutable landing and strict validation for the R2A data foundation."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Hashable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import cast
from uuid import UUID

import pyarrow as pa
from pydantic import BaseModel, ValidationError

from nexus.contracts import EventEnvelope
from nexus.data.r2_models import (
    R2FulfillmentOutcome,
    R2InventoryAdjustment,
    R2InventoryAdjustmentPayload,
    R2LandingResult,
    R2OpeningInventory,
    R2ProductLocation,
    R2PurchaseOrder,
    R2QuarantineReason,
    R2QuarantineRecord,
    R2Receipt,
    R2SourceManifest,
    R2Supplier,
    normalize_r2_utc,
)
from nexus.storage import file_sha256, write_immutable_parquet

_EXPECTED_EVENT_TYPE = "inventory.adjusted"
_EXPECTED_CONTRACT_VERSION = "1.0"
_EXPECTED_EVENT_VERSION = 1
_EXPECTED_SOURCES = {
    "source.fulfillment_outcomes": "fulfillment_outcomes.csv",
    "source.inventory_adjustments": "inventory_events.jsonl",
    "source.opening_inventory": "opening_inventory.csv",
    "source.product_locations": "product_locations.csv",
    "source.purchase_orders": "purchase_orders.csv",
    "source.receipts": "receipts.csv",
    "source.suppliers": "suppliers.csv",
}
_CSV_COLUMNS = {
    "suppliers.csv": ("supplier_id", "supplier_name", "active", "available_at"),
    "product_locations.csv": (
        "sku_id",
        "location_id",
        "supplier_id",
        "active",
        "available_at",
    ),
    "opening_inventory.csv": (
        "sku_id",
        "location_id",
        "on_hand_qty",
        "snapshot_at",
        "available_at",
    ),
    "fulfillment_outcomes.csv": (
        "fulfillment_id",
        "demand_date",
        "sku_id",
        "location_id",
        "demand_qty",
        "fulfilled_qty",
        "available_at",
    ),
    "purchase_orders.csv": (
        "order_id",
        "sku_id",
        "location_id",
        "supplier_id",
        "ordered_qty",
        "ordered_at",
        "expected_at",
        "available_at",
    ),
    "receipts.csv": (
        "receipt_id",
        "order_id",
        "sku_id",
        "location_id",
        "supplier_id",
        "received_qty",
        "received_at",
        "available_at",
    ),
}
_PARQUET_SCHEMAS: dict[str, pa.Schema] = {
    "validated.suppliers": pa.schema(
        [
            pa.field("supplier_id", pa.string(), nullable=False),
            pa.field("supplier_name", pa.string(), nullable=False),
            pa.field("active", pa.bool_(), nullable=False),
            pa.field("available_at", pa.string(), nullable=False),
        ]
    ),
    "validated.product_locations": pa.schema(
        [
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("supplier_id", pa.string(), nullable=False),
            pa.field("active", pa.bool_(), nullable=False),
            pa.field("available_at", pa.string(), nullable=False),
        ]
    ),
    "validated.opening_inventory": pa.schema(
        [
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("on_hand_qty", pa.int64(), nullable=False),
            pa.field("snapshot_at", pa.string(), nullable=False),
            pa.field("available_at", pa.string(), nullable=False),
        ]
    ),
    "validated.fulfillment_outcomes": pa.schema(
        [
            pa.field("fulfillment_id", pa.string(), nullable=False),
            pa.field("demand_date", pa.string(), nullable=False),
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("demand_qty", pa.int64(), nullable=False),
            pa.field("fulfilled_qty", pa.int64(), nullable=False),
            pa.field("available_at", pa.string(), nullable=False),
        ]
    ),
    "validated.purchase_orders": pa.schema(
        [
            pa.field("order_id", pa.string(), nullable=False),
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("supplier_id", pa.string(), nullable=False),
            pa.field("ordered_qty", pa.int64(), nullable=False),
            pa.field("ordered_at", pa.string(), nullable=False),
            pa.field("expected_at", pa.string(), nullable=False),
            pa.field("available_at", pa.string(), nullable=False),
        ]
    ),
    "validated.receipts": pa.schema(
        [
            pa.field("receipt_id", pa.string(), nullable=False),
            pa.field("order_id", pa.string(), nullable=False),
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("supplier_id", pa.string(), nullable=False),
            pa.field("received_qty", pa.int64(), nullable=False),
            pa.field("received_at", pa.string(), nullable=False),
            pa.field("available_at", pa.string(), nullable=False),
        ]
    ),
    "validated.inventory_adjustments": pa.schema(
        [
            pa.field("contract_version", pa.string(), nullable=False),
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("event_version", pa.int64(), nullable=False),
            pa.field("source", pa.string(), nullable=False),
            pa.field("logical_event_id", pa.string(), nullable=False),
            pa.field("revision", pa.int64(), nullable=False),
            pa.field("idempotency_key", pa.string(), nullable=False),
            pa.field("occurred_at", pa.string(), nullable=False),
            pa.field("ingested_at", pa.string(), nullable=False),
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("delta_qty", pa.int64(), nullable=False),
            pa.field("reason_code", pa.string(), nullable=False),
        ]
    ),
    "quarantine.r2": pa.schema(
        [
            pa.field("dataset", pa.string(), nullable=False),
            pa.field("source_file", pa.string(), nullable=False),
            pa.field("source_row", pa.int64(), nullable=False),
            pa.field("reason_code", pa.string(), nullable=False),
            pa.field("detail", pa.string(), nullable=False),
            pa.field("record_reference", pa.string(), nullable=False),
        ]
    ),
}


class R2SourceContractError(ValueError):
    """The source package cannot be trusted enough to identify row boundaries."""


class R2SourceIntegrityError(ValueError):
    """Source bytes do not match the immutable source manifest."""


class R2IdentityConflictError(ValueError):
    """A reused identity points at different business content."""


class R2CorrectionChainError(ValueError):
    """Revision history cannot be folded without ambiguity."""


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _parse_datetime(value: str) -> datetime:
    return normalize_r2_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError("boolean must be true or false")


def _required(row: Mapping[str, str | None], field: str) -> str:
    value = row.get(field)
    if value is None or not value.strip():
        raise ValueError(f"{field} is required")
    return value.strip()


def _validation_detail(error: ValidationError) -> str:
    first = error.errors(include_url=False, include_context=False)[0]
    location = ".".join(str(part) for part in first["loc"]) or "record"
    return f"{location}: {first['msg']}"


def _quarantine(
    *,
    dataset: str,
    source_file: str,
    source_row: int,
    reason: R2QuarantineReason,
    detail: str,
    reference: str,
) -> R2QuarantineRecord:
    return R2QuarantineRecord(
        dataset=dataset,
        source_file=source_file,
        source_row=source_row,
        reason_code=reason,
        detail=detail[:500],
        record_reference=reference,
    )


def _read_csv_rows(
    path: Path,
    *,
    source_file: str,
) -> list[dict[str | None, str | list[str] | None]]:
    expected_columns = _CSV_COLUMNS[source_file]
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        actual_columns = tuple(reader.fieldnames or ())
        if actual_columns != expected_columns:
            raise R2SourceContractError(
                f"{source_file} columns must be {expected_columns!r}; received {actual_columns!r}"
            )
        return list(reader)


def _parse_csv_rows[T: BaseModel](
    *,
    path: Path,
    dataset: str,
    source_file: str,
    parser: Callable[[Mapping[str, str | None]], T],
) -> tuple[list[tuple[int, T]], list[R2QuarantineRecord]]:
    accepted: list[tuple[int, T]] = []
    quarantined: list[R2QuarantineRecord] = []
    for row_number, row in enumerate(
        _read_csv_rows(path, source_file=source_file),
        start=2,
    ):
        reference = f"row-{row_number}"
        trailing_values = row.get(None)
        if trailing_values:
            quarantined.append(
                _quarantine(
                    dataset=dataset,
                    source_file=source_file,
                    source_row=row_number,
                    reason=R2QuarantineReason.BATCH_CONTRACT_INVALID,
                    detail="row contains fields beyond the declared CSV contract",
                    reference=reference,
                )
            )
            continue
        try:
            accepted.append(
                (
                    row_number,
                    parser(cast(Mapping[str, str | None], row)),
                )
            )
        except (ValidationError, ValueError) as error:
            detail = _validation_detail(error) if isinstance(error, ValidationError) else str(error)
            quarantined.append(
                _quarantine(
                    dataset=dataset,
                    source_file=source_file,
                    source_row=row_number,
                    reason=R2QuarantineReason.BATCH_CONTRACT_INVALID,
                    detail=detail,
                    reference=reference,
                )
            )
    return accepted, quarantined


def _deduplicate_batch[T: BaseModel](
    entries: Sequence[tuple[int, T]],
    *,
    dataset: str,
    source_file: str,
    key: Callable[[T], Hashable],
) -> tuple[list[T], list[R2QuarantineRecord]]:
    accepted: list[T] = []
    quarantined: list[R2QuarantineRecord] = []
    seen: dict[Hashable, T] = {}
    for row_number, record in entries:
        business_key = key(record)
        previous = seen.get(business_key)
        if previous is None:
            seen[business_key] = record
            accepted.append(record)
            continue
        if previous.model_dump(mode="json") != record.model_dump(mode="json"):
            raise R2IdentityConflictError(
                f"{dataset} business key {business_key!r} has conflicting records"
            )
        quarantined.append(
            _quarantine(
                dataset=dataset,
                source_file=source_file,
                source_row=row_number,
                reason=R2QuarantineReason.DUPLICATE_BUSINESS_KEY,
                detail="identical business key already accepted",
                reference=f"row-{row_number}",
            )
        )
    return accepted, quarantined


def _parse_supplier(row: Mapping[str, str | None]) -> R2Supplier:
    return R2Supplier(
        supplier_id=_required(row, "supplier_id"),
        supplier_name=_required(row, "supplier_name"),
        active=_parse_bool(_required(row, "active")),
        available_at=_parse_datetime(_required(row, "available_at")),
    )


def _parse_product(row: Mapping[str, str | None]) -> R2ProductLocation:
    return R2ProductLocation(
        sku_id=_required(row, "sku_id"),
        location_id=_required(row, "location_id"),
        supplier_id=_required(row, "supplier_id"),
        active=_parse_bool(_required(row, "active")),
        available_at=_parse_datetime(_required(row, "available_at")),
    )


def _parse_opening(row: Mapping[str, str | None]) -> R2OpeningInventory:
    return R2OpeningInventory(
        sku_id=_required(row, "sku_id"),
        location_id=_required(row, "location_id"),
        on_hand_qty=int(_required(row, "on_hand_qty")),
        snapshot_at=_parse_datetime(_required(row, "snapshot_at")),
        available_at=_parse_datetime(_required(row, "available_at")),
    )


def _parse_fulfillment(row: Mapping[str, str | None]) -> R2FulfillmentOutcome:
    return R2FulfillmentOutcome(
        fulfillment_id=_required(row, "fulfillment_id"),
        demand_date=_parse_date(_required(row, "demand_date")),
        sku_id=_required(row, "sku_id"),
        location_id=_required(row, "location_id"),
        demand_qty=int(_required(row, "demand_qty")),
        fulfilled_qty=int(_required(row, "fulfilled_qty")),
        available_at=_parse_datetime(_required(row, "available_at")),
    )


def _parse_order(row: Mapping[str, str | None]) -> R2PurchaseOrder:
    return R2PurchaseOrder(
        order_id=_required(row, "order_id"),
        sku_id=_required(row, "sku_id"),
        location_id=_required(row, "location_id"),
        supplier_id=_required(row, "supplier_id"),
        ordered_qty=int(_required(row, "ordered_qty")),
        ordered_at=_parse_datetime(_required(row, "ordered_at")),
        expected_at=_parse_datetime(_required(row, "expected_at")),
        available_at=_parse_datetime(_required(row, "available_at")),
    )


def _parse_receipt(row: Mapping[str, str | None]) -> R2Receipt:
    return R2Receipt(
        receipt_id=_required(row, "receipt_id"),
        order_id=_required(row, "order_id"),
        sku_id=_required(row, "sku_id"),
        location_id=_required(row, "location_id"),
        supplier_id=_required(row, "supplier_id"),
        received_qty=int(_required(row, "received_qty")),
        received_at=_parse_datetime(_required(row, "received_at")),
        available_at=_parse_datetime(_required(row, "available_at")),
    )


def _filter_relationships[T: BaseModel](
    entries: Sequence[tuple[int, T]],
    *,
    dataset: str,
    source_file: str,
    is_valid: Callable[[T], bool],
    reason: R2QuarantineReason,
    detail: str,
    reference: Callable[[T], str],
) -> tuple[list[tuple[int, T]], list[R2QuarantineRecord]]:
    accepted: list[tuple[int, T]] = []
    quarantined: list[R2QuarantineRecord] = []
    for row_number, record in entries:
        if is_valid(record):
            accepted.append((row_number, record))
        else:
            quarantined.append(
                _quarantine(
                    dataset=dataset,
                    source_file=source_file,
                    source_row=row_number,
                    reason=reason,
                    detail=detail,
                    reference=reference(record),
                )
            )
    return accepted, quarantined


def _event_identity_hash(
    envelope: EventEnvelope,
    payload: R2InventoryAdjustmentPayload,
) -> str:
    """Protect every governed field associated with an event ID."""

    return _canonical_sha256(
        {
            "contract_version": envelope.contract_version,
            "correlation_id": envelope.correlation_id,
            "event_id": str(envelope.event_id),
            "event_type": envelope.event_type,
            "event_version": envelope.event_version,
            "idempotency_key": envelope.idempotency_key,
            "ingested_at": envelope.ingested_at.isoformat(),
            "source": envelope.source,
            "subject_id": envelope.subject_id,
            "occurred_at": envelope.occurred_at.isoformat(),
            "payload": payload.model_dump(mode="json"),
            "trace_id": envelope.trace_id,
        }
    )


def _idempotent_effect_hash(
    envelope: EventEnvelope,
    payload: R2InventoryAdjustmentPayload,
) -> str:
    """Protect the business effect represented by an idempotency key."""

    return _canonical_sha256(
        {
            "contract_version": envelope.contract_version,
            "event_type": envelope.event_type,
            "event_version": envelope.event_version,
            "source": envelope.source,
            "subject_id": envelope.subject_id,
            "occurred_at": envelope.occurred_at.isoformat(),
            "payload": payload.model_dump(mode="json"),
        }
    )


def _load_inventory_events(
    path: Path,
    *,
    source_file: str,
    product_keys: set[tuple[str, str]],
) -> tuple[list[R2InventoryAdjustment], list[R2QuarantineRecord]]:
    candidates: list[
        tuple[
            int,
            EventEnvelope,
            R2InventoryAdjustmentPayload,
            str,
            str,
            R2QuarantineRecord | None,
        ]
    ] = []
    quarantined: list[R2QuarantineRecord] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            reference = f"line-{line_number}"
            try:
                envelope = EventEnvelope.model_validate_json(line)
                reference = str(envelope.event_id)
            except ValidationError as error:
                quarantined.append(
                    _quarantine(
                        dataset="source.inventory_adjustments",
                        source_file=source_file,
                        source_row=line_number,
                        reason=R2QuarantineReason.EVENT_CONTRACT_INVALID,
                        detail=_validation_detail(error),
                        reference=reference,
                    )
                )
                continue
            if envelope.contract_version != _EXPECTED_CONTRACT_VERSION:
                quarantined.append(
                    _quarantine(
                        dataset="source.inventory_adjustments",
                        source_file=source_file,
                        source_row=line_number,
                        reason=R2QuarantineReason.UNSUPPORTED_CONTRACT_VERSION,
                        detail=f"expected contract version {_EXPECTED_CONTRACT_VERSION}",
                        reference=reference,
                    )
                )
                continue
            if envelope.event_type != _EXPECTED_EVENT_TYPE:
                quarantined.append(
                    _quarantine(
                        dataset="source.inventory_adjustments",
                        source_file=source_file,
                        source_row=line_number,
                        reason=R2QuarantineReason.UNSUPPORTED_EVENT_TYPE,
                        detail=f"expected {_EXPECTED_EVENT_TYPE}",
                        reference=reference,
                    )
                )
                continue
            if envelope.event_version != _EXPECTED_EVENT_VERSION:
                quarantined.append(
                    _quarantine(
                        dataset="source.inventory_adjustments",
                        source_file=source_file,
                        source_row=line_number,
                        reason=R2QuarantineReason.UNSUPPORTED_EVENT_VERSION,
                        detail=f"expected event version {_EXPECTED_EVENT_VERSION}",
                        reference=reference,
                    )
                )
                continue
            try:
                payload = R2InventoryAdjustmentPayload.model_validate(envelope.payload)
            except ValidationError as error:
                quarantined.append(
                    _quarantine(
                        dataset="source.inventory_adjustments",
                        source_file=source_file,
                        source_row=line_number,
                        reason=R2QuarantineReason.EVENT_CONTRACT_INVALID,
                        detail=_validation_detail(error),
                        reference=reference,
                    )
                )
                continue
            relationship_rejection: R2QuarantineRecord | None = None
            if envelope.subject_id != f"{payload.sku_id}:{payload.location_id}":
                relationship_rejection = _quarantine(
                    dataset="source.inventory_adjustments",
                    source_file=source_file,
                    source_row=line_number,
                    reason=R2QuarantineReason.SUBJECT_PAYLOAD_MISMATCH,
                    detail="subject_id does not match payload grain",
                    reference=reference,
                )
            elif (payload.sku_id, payload.location_id) not in product_keys:
                relationship_rejection = _quarantine(
                    dataset="source.inventory_adjustments",
                    source_file=source_file,
                    source_row=line_number,
                    reason=R2QuarantineReason.UNKNOWN_PRODUCT_LOCATION,
                    detail="payload grain is absent from active master data",
                    reference=reference,
                )
            candidates.append(
                (
                    line_number,
                    envelope,
                    payload,
                    _event_identity_hash(envelope, payload),
                    _idempotent_effect_hash(envelope, payload),
                    relationship_rejection,
                )
            )

    candidates.sort(
        key=lambda item: (
            item[1].ingested_at,
            item[1].occurred_at,
            str(item[1].event_id),
            item[0],
        )
    )
    seen_event_ids: dict[UUID, str] = {}
    seen_idempotency_keys: dict[str, str] = {}
    accepted_with_lines: list[tuple[int, R2InventoryAdjustment]] = []
    for (
        line_number,
        envelope,
        payload,
        event_identity_hash,
        idempotent_effect_hash,
        relationship_rejection,
    ) in candidates:
        previous_event = seen_event_ids.get(envelope.event_id)
        if previous_event is not None and previous_event != event_identity_hash:
            raise R2IdentityConflictError(
                f"event_id {envelope.event_id} points at conflicting content"
            )
        previous_idempotency = seen_idempotency_keys.get(envelope.idempotency_key)
        if previous_idempotency is not None and previous_idempotency != idempotent_effect_hash:
            raise R2IdentityConflictError(
                f"idempotency key {envelope.idempotency_key!r} has conflicting content"
            )

        duplicate_event = previous_event is not None
        duplicate_idempotency = previous_idempotency is not None
        seen_event_ids[envelope.event_id] = event_identity_hash
        seen_idempotency_keys[envelope.idempotency_key] = idempotent_effect_hash
        if duplicate_event:
            quarantined.append(
                _quarantine(
                    dataset="source.inventory_adjustments",
                    source_file=source_file,
                    source_row=line_number,
                    reason=R2QuarantineReason.DUPLICATE_EVENT_ID,
                    detail="event_id already accepted",
                    reference=str(envelope.event_id),
                )
            )
            continue
        if duplicate_idempotency:
            quarantined.append(
                _quarantine(
                    dataset="source.inventory_adjustments",
                    source_file=source_file,
                    source_row=line_number,
                    reason=R2QuarantineReason.DUPLICATE_IDEMPOTENCY_KEY,
                    detail="idempotency_key already accepted",
                    reference=str(envelope.event_id),
                )
            )
            continue
        if relationship_rejection is not None:
            quarantined.append(relationship_rejection)
            continue
        adjustment = R2InventoryAdjustment(
            contract_version=envelope.contract_version,
            event_id=envelope.event_id,
            event_version=envelope.event_version,
            source=envelope.source,
            logical_event_id=payload.logical_event_id,
            revision=payload.revision,
            idempotency_key=envelope.idempotency_key,
            occurred_at=envelope.occurred_at,
            ingested_at=envelope.ingested_at,
            sku_id=payload.sku_id,
            location_id=payload.location_id,
            delta_qty=payload.delta_qty,
            reason_code=payload.reason_code,
        )
        accepted_with_lines.append((line_number, adjustment))

    by_logical_id: dict[tuple[str, str], list[tuple[int, R2InventoryAdjustment]]] = {}
    for line_number, adjustment in accepted_with_lines:
        key = (adjustment.source, adjustment.logical_event_id)
        by_logical_id.setdefault(key, []).append((line_number, adjustment))
    accepted: list[R2InventoryAdjustment] = []
    for (source, logical_event_id), revision_entries in by_logical_id.items():
        revision_entries.sort(
            key=lambda item: (
                item[1].revision,
                item[1].ingested_at,
                str(item[1].event_id),
            )
        )
        first_revision = revision_entries[0][1]
        canonical_grain = (
            first_revision.sku_id,
            first_revision.location_id,
            first_revision.occurred_at,
        )
        expected_revision = 1
        chain_error: str | None = None
        seen_revisions: dict[int, R2InventoryAdjustment] = {}
        for line_number, record in revision_entries:
            previous_revision = seen_revisions.get(record.revision)
            if previous_revision is not None:
                if previous_revision.model_dump(mode="json") != record.model_dump(mode="json"):
                    raise R2IdentityConflictError(
                        f"{source}:{logical_event_id} revision {record.revision} "
                        "points at conflicting content"
                    )
                chain_error = (
                    f"{source}:{logical_event_id} revision {record.revision} is duplicated"
                )
            elif chain_error is None and record.revision != expected_revision:
                chain_error = (
                    f"{source}:{logical_event_id} expected revision "
                    f"{expected_revision}, received {record.revision}"
                )
            elif (
                chain_error is None
                and (
                    record.sku_id,
                    record.location_id,
                    record.occurred_at,
                )
                != canonical_grain
            ):
                chain_error = (
                    f"{source}:{logical_event_id} revisions must preserve grain and business time"
                )

            seen_revisions[record.revision] = record
            if chain_error is None:
                accepted.append(record)
                expected_revision += 1
            else:
                quarantined.append(
                    _quarantine(
                        dataset="source.inventory_adjustments",
                        source_file=source_file,
                        source_row=line_number,
                        reason=R2QuarantineReason.CORRECTION_CHAIN_INVALID,
                        detail=chain_error,
                        reference=str(record.event_id),
                    )
                )

    accepted.sort(
        key=lambda item: (
            item.source,
            item.logical_event_id,
            item.revision,
            item.ingested_at,
            str(item.event_id),
        )
    )
    return accepted, quarantined


def _record_count(path: Path) -> int:
    if path.suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            next(reader, None)
            return sum(1 for _ in reader)
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def _land_source_bytes(
    source_bytes: bytes,
    *,
    source_name: str,
    dataset: str,
    content_sha256: str,
    root: Path,
) -> Path:
    """Persist the exact verified bytes that downstream parsing will consume."""

    actual_sha256 = hashlib.sha256(source_bytes).hexdigest()
    if actual_sha256 != content_sha256:
        raise R2SourceIntegrityError(
            f"{source_name} sha256 mismatch: expected {content_sha256}, received {actual_sha256}"
        )
    target_dir = root / dataset.replace(".", "_")
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{content_sha256}{Path(source_name).suffix}"
    if target.exists():
        if file_sha256(target) != content_sha256:
            raise R2SourceIntegrityError(f"source-native artifact is corrupt: {target}")
        return target
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{content_sha256}.",
            suffix=".tmp",
            dir=target_dir,
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(source_bytes)
        os.replace(temporary_path, target)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return target


def _rows(records: Sequence[BaseModel]) -> list[dict[str, object]]:
    return [cast(dict[str, object], record.model_dump(mode="json")) for record in records]


def land_r2_sources(
    *,
    source_dir: Path,
    output_dir: Path,
    namespace_by_manifest: bool = False,
) -> R2LandingResult:
    """Verify source bytes, retain them immutably, and emit validated Parquet."""

    source_dir = source_dir.resolve()
    output_dir = output_dir.resolve()
    manifest_path = source_dir / "source-manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"R2A source manifest is missing: {manifest_path}")
    if not manifest_path.resolve().is_relative_to(source_dir):
        raise R2SourceIntegrityError(
            f"source manifest escapes the source directory: {manifest_path}"
        )
    manifest_bytes = manifest_path.read_bytes()
    manifest_file_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    try:
        source_manifest = R2SourceManifest.model_validate_json(manifest_bytes)
    except ValidationError as error:
        raise R2SourceContractError(_validation_detail(error)) from error

    manifest_sources = {source.dataset: source.file_name for source in source_manifest.sources}
    if manifest_sources != _EXPECTED_SOURCES:
        raise R2SourceContractError(
            f"source manifest must bind exactly {_EXPECTED_SOURCES!r}; "
            f"received {manifest_sources!r}"
        )
    manifest_sha256 = _canonical_sha256(source_manifest.model_dump(mode="json"))
    if namespace_by_manifest:
        output_dir = output_dir / manifest_file_sha256[:16]
        identity_path = output_dir / "source-package.sha256"
        if identity_path.exists():
            if identity_path.read_text(encoding="ascii").strip() != manifest_file_sha256:
                raise R2SourceIntegrityError(
                    f"short source-package namespace collision: {output_dir}"
                )
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
            try:
                with identity_path.open("x", encoding="ascii", newline="\n") as handle:
                    handle.write(manifest_file_sha256 + "\n")
            except FileExistsError:
                if identity_path.read_text(encoding="ascii").strip() != manifest_file_sha256:
                    raise R2SourceIntegrityError(
                        f"short source-package namespace collision: {output_dir}"
                    ) from None
    source_native_root = output_dir / "raw"
    source_native_paths: list[Path] = [
        _land_source_bytes(
            manifest_bytes,
            source_name=manifest_path.name,
            dataset="source.manifest",
            content_sha256=manifest_file_sha256,
            root=source_native_root,
        )
    ]
    landed_sources: dict[str, Path] = {}
    for source in source_manifest.sources:
        path = source_dir / source.file_name
        if not path.is_file():
            raise FileNotFoundError(f"manifest-bound R2A source is missing: {path}")
        if not path.resolve().is_relative_to(source_dir):
            raise R2SourceIntegrityError(
                f"manifest-bound source escapes the source directory: {path}"
            )
        source_bytes = path.read_bytes()
        landed_path = _land_source_bytes(
            source_bytes,
            source_name=source.file_name,
            dataset=source.dataset,
            content_sha256=source.content_sha256,
            root=source_native_root,
        )
        actual_count = _record_count(landed_path)
        if actual_count != source.record_count:
            raise R2SourceIntegrityError(
                f"{source.file_name} row count mismatch: "
                f"expected {source.record_count}, received {actual_count}"
            )
        source_native_paths.append(landed_path)
        landed_sources[source.dataset] = landed_path

    quarantined: list[R2QuarantineRecord] = []
    supplier_entries, rejected = _parse_csv_rows(
        path=landed_sources["source.suppliers"],
        dataset="source.suppliers",
        source_file="suppliers.csv",
        parser=_parse_supplier,
    )
    quarantined.extend(rejected)
    suppliers, rejected = _deduplicate_batch(
        supplier_entries,
        dataset="source.suppliers",
        source_file="suppliers.csv",
        key=lambda row: row.supplier_id,
    )
    quarantined.extend(rejected)
    supplier_keys = {row.supplier_id for row in suppliers if row.active}

    product_entries, rejected = _parse_csv_rows(
        path=landed_sources["source.product_locations"],
        dataset="source.product_locations",
        source_file="product_locations.csv",
        parser=_parse_product,
    )
    quarantined.extend(rejected)
    product_entries, rejected = _filter_relationships(
        product_entries,
        dataset="source.product_locations",
        source_file="product_locations.csv",
        is_valid=lambda row: row.supplier_id in supplier_keys,
        reason=R2QuarantineReason.UNKNOWN_SUPPLIER,
        detail="primary supplier is absent from active supplier master",
        reference=lambda row: f"{row.sku_id}:{row.location_id}",
    )
    quarantined.extend(rejected)
    products, rejected = _deduplicate_batch(
        product_entries,
        dataset="source.product_locations",
        source_file="product_locations.csv",
        key=lambda row: (row.sku_id, row.location_id),
    )
    quarantined.extend(rejected)
    product_keys = {(row.sku_id, row.location_id) for row in products if row.active}
    product_supplier_by_key = {
        (row.sku_id, row.location_id): row.supplier_id for row in products if row.active
    }

    opening_entries, rejected = _parse_csv_rows(
        path=landed_sources["source.opening_inventory"],
        dataset="source.opening_inventory",
        source_file="opening_inventory.csv",
        parser=_parse_opening,
    )
    quarantined.extend(rejected)
    opening_entries, rejected = _filter_relationships(
        opening_entries,
        dataset="source.opening_inventory",
        source_file="opening_inventory.csv",
        is_valid=lambda row: (row.sku_id, row.location_id) in product_keys,
        reason=R2QuarantineReason.UNKNOWN_PRODUCT_LOCATION,
        detail="opening grain is absent from active product-location master",
        reference=lambda row: f"{row.sku_id}:{row.location_id}",
    )
    quarantined.extend(rejected)
    openings, rejected = _deduplicate_batch(
        opening_entries,
        dataset="source.opening_inventory",
        source_file="opening_inventory.csv",
        key=lambda row: (row.sku_id, row.location_id),
    )
    quarantined.extend(rejected)

    fulfillment_entries, rejected = _parse_csv_rows(
        path=landed_sources["source.fulfillment_outcomes"],
        dataset="source.fulfillment_outcomes",
        source_file="fulfillment_outcomes.csv",
        parser=_parse_fulfillment,
    )
    quarantined.extend(rejected)
    fulfillment_entries, rejected = _filter_relationships(
        fulfillment_entries,
        dataset="source.fulfillment_outcomes",
        source_file="fulfillment_outcomes.csv",
        is_valid=lambda row: (row.sku_id, row.location_id) in product_keys,
        reason=R2QuarantineReason.UNKNOWN_PRODUCT_LOCATION,
        detail="fulfillment grain is absent from active product-location master",
        reference=lambda row: row.fulfillment_id,
    )
    quarantined.extend(rejected)
    fulfillments, rejected = _deduplicate_batch(
        fulfillment_entries,
        dataset="source.fulfillment_outcomes",
        source_file="fulfillment_outcomes.csv",
        key=lambda row: row.fulfillment_id,
    )
    quarantined.extend(rejected)

    order_entries, rejected = _parse_csv_rows(
        path=landed_sources["source.purchase_orders"],
        dataset="source.purchase_orders",
        source_file="purchase_orders.csv",
        parser=_parse_order,
    )
    quarantined.extend(rejected)
    order_entries, rejected = _filter_relationships(
        order_entries,
        dataset="source.purchase_orders",
        source_file="purchase_orders.csv",
        is_valid=lambda row: (row.sku_id, row.location_id) in product_keys,
        reason=R2QuarantineReason.UNKNOWN_PRODUCT_LOCATION,
        detail="order grain is absent from active product-location master",
        reference=lambda row: row.order_id,
    )
    quarantined.extend(rejected)
    order_entries, rejected = _filter_relationships(
        order_entries,
        dataset="source.purchase_orders",
        source_file="purchase_orders.csv",
        is_valid=lambda row: row.supplier_id in supplier_keys,
        reason=R2QuarantineReason.UNKNOWN_SUPPLIER,
        detail="order supplier is absent from active supplier master",
        reference=lambda row: row.order_id,
    )
    quarantined.extend(rejected)
    order_entries, rejected = _filter_relationships(
        order_entries,
        dataset="source.purchase_orders",
        source_file="purchase_orders.csv",
        is_valid=lambda row: (
            product_supplier_by_key[(row.sku_id, row.location_id)] == row.supplier_id
        ),
        reason=R2QuarantineReason.PRIMARY_SUPPLIER_MISMATCH,
        detail="order supplier does not match the product-location primary supplier",
        reference=lambda row: row.order_id,
    )
    quarantined.extend(rejected)
    orders, rejected = _deduplicate_batch(
        order_entries,
        dataset="source.purchase_orders",
        source_file="purchase_orders.csv",
        key=lambda row: row.order_id,
    )
    quarantined.extend(rejected)
    orders_by_id = {row.order_id: row for row in orders}

    receipt_entries, rejected = _parse_csv_rows(
        path=landed_sources["source.receipts"],
        dataset="source.receipts",
        source_file="receipts.csv",
        parser=_parse_receipt,
    )
    quarantined.extend(rejected)

    def receipt_matches_order(receipt: R2Receipt) -> bool:
        order = orders_by_id.get(receipt.order_id)
        return (
            order is not None
            and (
                receipt.sku_id,
                receipt.location_id,
                receipt.supplier_id,
            )
            == (
                order.sku_id,
                order.location_id,
                order.supplier_id,
            )
            and receipt.received_at >= order.ordered_at
        )

    receipt_entries, rejected = _filter_relationships(
        receipt_entries,
        dataset="source.receipts",
        source_file="receipts.csv",
        is_valid=receipt_matches_order,
        reason=R2QuarantineReason.UNKNOWN_ORDER,
        detail="receipt order or grain does not match a validated purchase order",
        reference=lambda row: row.receipt_id,
    )
    quarantined.extend(rejected)
    receipts, rejected = _deduplicate_batch(
        receipt_entries,
        dataset="source.receipts",
        source_file="receipts.csv",
        key=lambda row: row.receipt_id,
    )
    quarantined.extend(rejected)

    adjustments, rejected = _load_inventory_events(
        landed_sources["source.inventory_adjustments"],
        source_file="inventory_events.jsonl",
        product_keys=product_keys,
    )
    quarantined.extend(rejected)

    suppliers.sort(key=lambda row: row.supplier_id)
    products.sort(key=lambda row: (row.sku_id, row.location_id))
    openings.sort(key=lambda row: (row.sku_id, row.location_id))
    fulfillments.sort(key=lambda row: row.fulfillment_id)
    orders.sort(key=lambda row: row.order_id)
    receipts.sort(key=lambda row: row.receipt_id)
    quarantined.sort(key=lambda row: (row.source_file, row.source_row, row.reason_code.value))

    datasets: tuple[tuple[str, Sequence[BaseModel]], ...] = (
        ("validated.suppliers", suppliers),
        ("validated.product_locations", products),
        ("validated.opening_inventory", openings),
        ("validated.fulfillment_outcomes", fulfillments),
        ("validated.purchase_orders", orders),
        ("validated.receipts", receipts),
        ("validated.inventory_adjustments", adjustments),
        ("quarantine.r2", quarantined),
    )
    artifacts = tuple(
        write_immutable_parquet(
            output_dir,
            dataset=dataset,
            rows=_rows(records),
            schema=_PARQUET_SCHEMAS[dataset],
        )
        for dataset, records in datasets
    )
    return R2LandingResult(
        source_manifest=source_manifest,
        source_manifest_sha256=manifest_sha256,
        source_manifest_file_sha256=manifest_file_sha256,
        product_locations=tuple(products),
        suppliers=tuple(suppliers),
        opening_inventory=tuple(openings),
        fulfillment_outcomes=tuple(fulfillments),
        purchase_orders=tuple(orders),
        receipts=tuple(receipts),
        inventory_adjustments=tuple(adjustments),
        quarantined=tuple(quarantined),
        validated_artifacts=artifacts,
        source_native_paths=tuple(sorted(source_native_paths)),
    )
