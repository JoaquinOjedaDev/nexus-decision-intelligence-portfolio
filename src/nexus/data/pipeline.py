"""Deterministic batch + event ingestion for the R1 vertical slice."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import cast
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ValidationError

from nexus.contracts import EventEnvelope
from nexus.data.models import (
    AcceptedInventoryAdjustment,
    DemandRecord,
    GoldDemandRecord,
    GoldInventoryRecord,
    InventoryAdjustmentPayload,
    InventorySnapshotRecord,
    ProductLocationRecord,
    QuarantineReason,
    QuarantineRecord,
    R1DataArtifact,
    R1DataPathResult,
    normalize_utc,
)

_EXPECTED_EVENT_TYPE = "inventory.adjusted"


class R1SourceSchemaError(ValueError):
    """A source file has no reliable row boundary because its header is wrong."""


class ImmutableArtifactConflict(RuntimeError):
    """An existing content-addressed artifact does not contain its named content."""


_PARQUET_SCHEMAS: dict[str, pa.Schema] = {
    "raw.product_location_master": pa.schema(
        [
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("lead_time_days", pa.int64(), nullable=False),
            pa.field("safety_stock_qty", pa.int64(), nullable=False),
            pa.field("active", pa.bool_(), nullable=False),
            pa.field("available_at", pa.string(), nullable=False),
        ]
    ),
    "raw.demand_history": pa.schema(
        [
            pa.field("demand_date", pa.string(), nullable=False),
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("demand_qty", pa.int64(), nullable=False),
            pa.field("available_at", pa.string(), nullable=False),
        ]
    ),
    "raw.inventory_snapshot": pa.schema(
        [
            pa.field("snapshot_at", pa.string(), nullable=False),
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("on_hand_qty", pa.int64(), nullable=False),
            pa.field("on_order_qty", pa.int64(), nullable=False),
            pa.field("available_at", pa.string(), nullable=False),
        ]
    ),
    "raw.inventory_adjustments": pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("idempotency_key", pa.string(), nullable=False),
            pa.field("source", pa.string(), nullable=False),
            pa.field("subject_id", pa.string(), nullable=False),
            pa.field("occurred_at", pa.string(), nullable=False),
            pa.field("ingested_at", pa.string(), nullable=False),
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("delta_qty", pa.int64(), nullable=False),
            pa.field("reason_code", pa.string(), nullable=False),
        ]
    ),
    "gold.demand_history": pa.schema(
        [
            pa.field("demand_date", pa.string(), nullable=False),
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("demand_qty", pa.int64(), nullable=False),
            pa.field("available_at", pa.string(), nullable=False),
        ]
    ),
    "gold.inventory_position": pa.schema(
        [
            pa.field("sku_id", pa.string(), nullable=False),
            pa.field("location_id", pa.string(), nullable=False),
            pa.field("on_hand_qty", pa.int64(), nullable=False),
            pa.field("on_order_qty", pa.int64(), nullable=False),
            pa.field("snapshot_at", pa.string(), nullable=False),
            pa.field("available_at", pa.string(), nullable=False),
            pa.field("as_of", pa.string(), nullable=False),
            pa.field("adjustment_event_count", pa.int64(), nullable=False),
        ]
    ),
    "quarantine.r1": pa.schema(
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

_CSV_COLUMNS = {
    "product_location_master.csv": (
        "sku_id",
        "location_id",
        "lead_time_days",
        "safety_stock_qty",
        "active",
        "available_at",
    ),
    "demand_history.csv": (
        "demand_date",
        "sku_id",
        "location_id",
        "demand_qty",
        "available_at",
    ),
    "inventory_snapshot.csv": (
        "snapshot_at",
        "sku_id",
        "location_id",
        "on_hand_qty",
        "on_order_qty",
        "available_at",
    ),
}


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return normalize_utc(parsed)


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
    diagnostics = []
    for item in error.errors(include_url=False, include_input=False):
        location = ".".join(str(part) for part in item["loc"]) or "record"
        diagnostics.append(f"{location}: {item['type']}")
    return "; ".join(diagnostics)[:500]


def _read_csv_rows(path: Path, expected_columns: tuple[str, ...]) -> list[dict[str, str | None]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        actual_columns = tuple(reader.fieldnames or ())
        if actual_columns != expected_columns:
            raise R1SourceSchemaError(
                f"{path.name} columns must be {expected_columns!r}; received {actual_columns!r}"
            )
        return [dict(row) for row in reader]


def _quarantine(
    *,
    dataset: str,
    source_file: str,
    source_row: int,
    reason: QuarantineReason,
    detail: str,
    reference: str,
) -> QuarantineRecord:
    return QuarantineRecord(
        dataset=dataset,
        source_file=source_file,
        source_row=source_row,
        reason_code=reason,
        detail=detail,
        record_reference=reference,
    )


def _parse_batch_rows[T: BaseModel](
    *,
    path: Path,
    dataset: str,
    parser: Callable[[Mapping[str, str | None]], T],
) -> tuple[list[tuple[int, T]], list[QuarantineRecord]]:
    expected_columns = _CSV_COLUMNS[path.name]
    source_rows = _read_csv_rows(path, expected_columns)
    accepted: list[tuple[int, T]] = []
    quarantined: list[QuarantineRecord] = []
    for row_number, source_row in enumerate(source_rows, start=2):
        try:
            accepted.append((row_number, parser(source_row)))
        except (ValidationError, ValueError) as error:
            detail = (
                _validation_detail(error)
                if isinstance(error, ValidationError)
                else f"record: {type(error).__name__}"
            )
            quarantined.append(
                _quarantine(
                    dataset=dataset,
                    source_file=path.name,
                    source_row=row_number,
                    reason=QuarantineReason.BATCH_CONTRACT_INVALID,
                    detail=detail,
                    reference=f"row-{row_number}",
                )
            )
    return accepted, quarantined


def _parse_product(row: Mapping[str, str | None]) -> ProductLocationRecord:
    return ProductLocationRecord(
        sku_id=_required(row, "sku_id"),
        location_id=_required(row, "location_id"),
        lead_time_days=int(_required(row, "lead_time_days")),
        safety_stock_qty=int(_required(row, "safety_stock_qty")),
        active=_parse_bool(_required(row, "active")),
        available_at=_parse_datetime(_required(row, "available_at")),
    )


def _parse_demand(row: Mapping[str, str | None]) -> DemandRecord:
    return DemandRecord(
        demand_date=date.fromisoformat(_required(row, "demand_date")),
        sku_id=_required(row, "sku_id"),
        location_id=_required(row, "location_id"),
        demand_qty=int(_required(row, "demand_qty")),
        available_at=_parse_datetime(_required(row, "available_at")),
    )


def _parse_inventory(row: Mapping[str, str | None]) -> InventorySnapshotRecord:
    return InventorySnapshotRecord(
        snapshot_at=_parse_datetime(_required(row, "snapshot_at")),
        sku_id=_required(row, "sku_id"),
        location_id=_required(row, "location_id"),
        on_hand_qty=int(_required(row, "on_hand_qty")),
        on_order_qty=int(_required(row, "on_order_qty")),
        available_at=_parse_datetime(_required(row, "available_at")),
    )


def _deduplicate_batch[T: BaseModel](
    rows: Iterable[tuple[int, T]],
    *,
    dataset: str,
    source_file: str,
    key: Callable[[T], tuple[object, ...]],
) -> tuple[list[tuple[int, T]], list[QuarantineRecord]]:
    accepted: list[tuple[int, T]] = []
    quarantined: list[QuarantineRecord] = []
    seen: set[tuple[object, ...]] = set()
    for row_number, row in rows:
        business_key = key(row)
        if business_key in seen:
            quarantined.append(
                _quarantine(
                    dataset=dataset,
                    source_file=source_file,
                    source_row=row_number,
                    reason=QuarantineReason.DUPLICATE_BUSINESS_KEY,
                    detail="business key already accepted",
                    reference=f"row-{row_number}",
                )
            )
            continue
        seen.add(business_key)
        accepted.append((row_number, row))
    return accepted, quarantined


def _json_rows(records: Sequence[BaseModel]) -> list[dict[str, object]]:
    return [record.model_dump(mode="json") for record in records]


def _canonical_sha256(rows: Sequence[Mapping[str, object]]) -> str:
    encoded = json.dumps(
        rows,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _artifact_directory(dataset: str) -> Path:
    layer, name = dataset.split(".", maxsplit=1)
    return Path(layer) / name


def _write_immutable_parquet(
    output_dir: Path,
    *,
    dataset: str,
    records: Sequence[BaseModel],
) -> R1DataArtifact:
    rows = _json_rows(records)
    content_sha256 = _canonical_sha256(rows)
    target_dir = output_dir / _artifact_directory(dataset)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{content_sha256}.parquet"
    schema = _PARQUET_SCHEMAS[dataset]

    if target.exists():
        existing_rows = cast(list[dict[str, object]], pq.read_table(target).to_pylist())
        if _canonical_sha256(existing_rows) != content_sha256:
            raise ImmutableArtifactConflict(f"content-addressed artifact is corrupt: {target}")
    else:
        table = pa.Table.from_pylist(rows, schema=schema)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                prefix=f".{content_sha256}.",
                suffix=".tmp",
                dir=target_dir,
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
            pq.write_table(
                table,
                temporary_path,
                compression="zstd",
                version="2.6",
                use_dictionary=True,
            )
            os.replace(temporary_path, target)
            temporary_path = None
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    return R1DataArtifact(
        dataset=dataset,
        path=target,
        content_sha256=content_sha256,
        row_count=len(rows),
    )


def _load_inventory_events(
    path: Path,
    *,
    as_of: datetime,
    product_keys: set[tuple[str, str]],
    snapshots: Mapping[tuple[str, str], InventorySnapshotRecord],
) -> tuple[list[AcceptedInventoryAdjustment], list[QuarantineRecord]]:
    accepted: list[AcceptedInventoryAdjustment] = []
    quarantined: list[QuarantineRecord] = []
    seen_event_ids: set[UUID] = set()
    seen_idempotency_keys: set[str] = set()

    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            reference = f"line-{line_number}"
            try:
                envelope = EventEnvelope.model_validate_json(line)
                reference = str(envelope.event_id)
                payload = InventoryAdjustmentPayload.model_validate(envelope.payload)
            except ValidationError as error:
                quarantined.append(
                    _quarantine(
                        dataset="raw.inventory_adjustments",
                        source_file=path.name,
                        source_row=line_number,
                        reason=QuarantineReason.EVENT_CONTRACT_INVALID,
                        detail=_validation_detail(error),
                        reference=reference,
                    )
                )
                continue

            if envelope.event_type != _EXPECTED_EVENT_TYPE:
                quarantined.append(
                    _quarantine(
                        dataset="raw.inventory_adjustments",
                        source_file=path.name,
                        source_row=line_number,
                        reason=QuarantineReason.UNSUPPORTED_EVENT_TYPE,
                        detail=f"expected {_EXPECTED_EVENT_TYPE}",
                        reference=reference,
                    )
                )
                continue
            expected_subject = f"{payload.sku_id}:{payload.location_id}"
            if envelope.subject_id != expected_subject:
                quarantined.append(
                    _quarantine(
                        dataset="raw.inventory_adjustments",
                        source_file=path.name,
                        source_row=line_number,
                        reason=QuarantineReason.SUBJECT_PAYLOAD_MISMATCH,
                        detail="subject_id does not match payload grain",
                        reference=reference,
                    )
                )
                continue
            product_key = (payload.sku_id, payload.location_id)
            if product_key not in product_keys:
                quarantined.append(
                    _quarantine(
                        dataset="raw.inventory_adjustments",
                        source_file=path.name,
                        source_row=line_number,
                        reason=QuarantineReason.UNKNOWN_PRODUCT_LOCATION,
                        detail="payload grain is absent from active master data",
                        reference=reference,
                    )
                )
                continue
            if envelope.event_id in seen_event_ids:
                quarantined.append(
                    _quarantine(
                        dataset="raw.inventory_adjustments",
                        source_file=path.name,
                        source_row=line_number,
                        reason=QuarantineReason.DUPLICATE_EVENT_ID,
                        detail="event_id already accepted",
                        reference=reference,
                    )
                )
                continue
            if envelope.idempotency_key in seen_idempotency_keys:
                quarantined.append(
                    _quarantine(
                        dataset="raw.inventory_adjustments",
                        source_file=path.name,
                        source_row=line_number,
                        reason=QuarantineReason.DUPLICATE_IDEMPOTENCY_KEY,
                        detail="idempotency_key already accepted",
                        reference=reference,
                    )
                )
                continue
            snapshot = snapshots.get(product_key)
            if snapshot is None:
                quarantined.append(
                    _quarantine(
                        dataset="raw.inventory_adjustments",
                        source_file=path.name,
                        source_row=line_number,
                        reason=QuarantineReason.UNKNOWN_PRODUCT_LOCATION,
                        detail="no inventory snapshot exists for payload grain",
                        reference=reference,
                    )
                )
                continue
            if envelope.occurred_at <= snapshot.snapshot_at:
                quarantined.append(
                    _quarantine(
                        dataset="raw.inventory_adjustments",
                        source_file=path.name,
                        source_row=line_number,
                        reason=QuarantineReason.EVENT_NOT_AFTER_SNAPSHOT,
                        detail="occurred_at must be later than snapshot_at",
                        reference=reference,
                    )
                )
                continue
            if envelope.occurred_at > as_of or envelope.ingested_at > as_of:
                quarantined.append(
                    _quarantine(
                        dataset="raw.inventory_adjustments",
                        source_file=path.name,
                        source_row=line_number,
                        reason=QuarantineReason.EVENT_NOT_AVAILABLE_AS_OF,
                        detail="event was not both occurred and ingested by as_of",
                        reference=reference,
                    )
                )
                continue

            accepted.append(
                AcceptedInventoryAdjustment(
                    event_id=envelope.event_id,
                    idempotency_key=envelope.idempotency_key,
                    source=envelope.source,
                    subject_id=envelope.subject_id,
                    occurred_at=envelope.occurred_at,
                    ingested_at=envelope.ingested_at,
                    sku_id=payload.sku_id,
                    location_id=payload.location_id,
                    delta_qty=payload.delta_qty,
                    reason_code=payload.reason_code,
                )
            )
            seen_event_ids.add(envelope.event_id)
            seen_idempotency_keys.add(envelope.idempotency_key)

    return accepted, quarantined


def run_r1_data_path(
    *,
    source_dir: Path,
    output_dir: Path,
    as_of: datetime,
) -> R1DataPathResult:
    """Validate R1 fixtures and emit deterministic content-addressed Parquet.

    The caller supplies every path and the availability cutoff. This keeps the
    function independent of the current working directory and makes point-in-time
    behavior explicit for orchestration and tests.
    """

    as_of = normalize_utc(as_of)
    source_dir = source_dir.resolve()
    output_dir = output_dir.resolve()

    product_path = source_dir / "product_location_master.csv"
    demand_path = source_dir / "demand_history.csv"
    inventory_path = source_dir / "inventory_snapshot.csv"
    events_path = source_dir / "inventory_events.jsonl"
    for source_path in (product_path, demand_path, inventory_path, events_path):
        if not source_path.is_file():
            raise FileNotFoundError(f"required R1 source is missing: {source_path}")

    product_rows, quarantined = _parse_batch_rows(
        path=product_path,
        dataset="raw.product_location_master",
        parser=_parse_product,
    )
    product_entries, duplicate_products = _deduplicate_batch(
        product_rows,
        dataset="raw.product_location_master",
        source_file=product_path.name,
        key=lambda row: (row.sku_id, row.location_id),
    )
    quarantined.extend(duplicate_products)
    products: list[ProductLocationRecord] = []
    for row_number, product_record in product_entries:
        if product_record.available_at > as_of:
            quarantined.append(
                _quarantine(
                    dataset="raw.product_location_master",
                    source_file=product_path.name,
                    source_row=row_number,
                    reason=QuarantineReason.BATCH_NOT_AVAILABLE_AS_OF,
                    detail="master-data row was not available by as_of",
                    reference=f"row-{row_number}",
                )
            )
        else:
            products.append(product_record)
    active_product_keys = {(row.sku_id, row.location_id) for row in products if row.active}

    demand_rows, demand_quarantine = _parse_batch_rows(
        path=demand_path,
        dataset="raw.demand_history",
        parser=_parse_demand,
    )
    quarantined.extend(demand_quarantine)
    demand_entries, duplicate_demand = _deduplicate_batch(
        demand_rows,
        dataset="raw.demand_history",
        source_file=demand_path.name,
        key=lambda row: (row.demand_date, row.sku_id, row.location_id),
    )
    quarantined.extend(duplicate_demand)
    accepted_demand: list[DemandRecord] = []
    for row_number, demand_record in demand_entries:
        if (demand_record.sku_id, demand_record.location_id) not in active_product_keys:
            quarantined.append(
                _quarantine(
                    dataset="raw.demand_history",
                    source_file=demand_path.name,
                    source_row=row_number,
                    reason=QuarantineReason.UNKNOWN_PRODUCT_LOCATION,
                    detail="demand grain is absent from active master data",
                    reference=f"row-{row_number}",
                )
            )
        elif demand_record.available_at > as_of:
            quarantined.append(
                _quarantine(
                    dataset="raw.demand_history",
                    source_file=demand_path.name,
                    source_row=row_number,
                    reason=QuarantineReason.BATCH_NOT_AVAILABLE_AS_OF,
                    detail="demand row was not available by as_of",
                    reference=f"row-{row_number}",
                )
            )
        elif demand_record.demand_date >= as_of.date():
            quarantined.append(
                _quarantine(
                    dataset="raw.demand_history",
                    source_file=demand_path.name,
                    source_row=row_number,
                    reason=QuarantineReason.DEMAND_NOT_COMPLETE_AS_OF,
                    detail="daily demand is accepted only after its calendar day closes",
                    reference=f"row-{row_number}",
                )
            )
        else:
            accepted_demand.append(demand_record)

    inventory_rows, inventory_quarantine = _parse_batch_rows(
        path=inventory_path,
        dataset="raw.inventory_snapshot",
        parser=_parse_inventory,
    )
    quarantined.extend(inventory_quarantine)
    inventory_entries, duplicate_inventory = _deduplicate_batch(
        inventory_rows,
        dataset="raw.inventory_snapshot",
        source_file=inventory_path.name,
        key=lambda row: (row.sku_id, row.location_id),
    )
    quarantined.extend(duplicate_inventory)
    accepted_inventory: list[InventorySnapshotRecord] = []
    for row_number, inventory_record in inventory_entries:
        if (
            inventory_record.sku_id,
            inventory_record.location_id,
        ) not in active_product_keys:
            quarantined.append(
                _quarantine(
                    dataset="raw.inventory_snapshot",
                    source_file=inventory_path.name,
                    source_row=row_number,
                    reason=QuarantineReason.UNKNOWN_PRODUCT_LOCATION,
                    detail="inventory grain is absent from active master data",
                    reference=f"row-{row_number}",
                )
            )
        elif inventory_record.available_at > as_of:
            quarantined.append(
                _quarantine(
                    dataset="raw.inventory_snapshot",
                    source_file=inventory_path.name,
                    source_row=row_number,
                    reason=QuarantineReason.BATCH_NOT_AVAILABLE_AS_OF,
                    detail="inventory snapshot was not available by as_of",
                    reference=f"row-{row_number}",
                )
            )
        elif inventory_record.snapshot_at > as_of:
            quarantined.append(
                _quarantine(
                    dataset="raw.inventory_snapshot",
                    source_file=inventory_path.name,
                    source_row=row_number,
                    reason=QuarantineReason.SNAPSHOT_AFTER_AS_OF,
                    detail="snapshot_at is later than as_of",
                    reference=f"row-{row_number}",
                )
            )
        else:
            accepted_inventory.append(inventory_record)

    snapshots_by_key = {(row.sku_id, row.location_id): row for row in accepted_inventory}
    accepted_events, event_quarantine = _load_inventory_events(
        events_path,
        as_of=as_of,
        product_keys=active_product_keys,
        snapshots=snapshots_by_key,
    )
    quarantined.extend(event_quarantine)

    products.sort(key=lambda row: (row.sku_id, row.location_id))
    accepted_demand.sort(key=lambda row: (row.demand_date, row.sku_id, row.location_id))
    accepted_inventory.sort(key=lambda row: (row.sku_id, row.location_id))
    accepted_events.sort(key=lambda row: (row.occurred_at, str(row.event_id)))
    quarantined.sort(key=lambda row: (row.source_file, row.source_row, row.reason_code.value))

    gold_demand = [
        GoldDemandRecord(
            demand_date=row.demand_date,
            sku_id=row.sku_id,
            location_id=row.location_id,
            demand_qty=row.demand_qty,
            available_at=row.available_at,
        )
        for row in accepted_demand
    ]
    adjustments_by_key: dict[tuple[str, str], list[AcceptedInventoryAdjustment]] = {}
    for event in accepted_events:
        adjustments_by_key.setdefault((event.sku_id, event.location_id), []).append(event)
    gold_inventory: list[GoldInventoryRecord] = []
    for snapshot in accepted_inventory:
        key = (snapshot.sku_id, snapshot.location_id)
        adjustments = adjustments_by_key.get(key, [])
        adjusted_on_hand = snapshot.on_hand_qty + sum(event.delta_qty for event in adjustments)
        if adjusted_on_hand < 0:
            raise ValueError(f"accepted adjustments make on_hand negative for {key!r}")
        gold_inventory.append(
            GoldInventoryRecord(
                sku_id=snapshot.sku_id,
                location_id=snapshot.location_id,
                on_hand_qty=adjusted_on_hand,
                on_order_qty=snapshot.on_order_qty,
                snapshot_at=snapshot.snapshot_at,
                available_at=max(
                    (snapshot.available_at, *(event.ingested_at for event in adjustments))
                ),
                as_of=as_of,
                adjustment_event_count=len(adjustments),
            )
        )

    artifact_inputs: tuple[tuple[str, Sequence[BaseModel]], ...] = (
        ("raw.product_location_master", products),
        ("raw.demand_history", accepted_demand),
        ("raw.inventory_snapshot", accepted_inventory),
        ("raw.inventory_adjustments", accepted_events),
        ("gold.demand_history", gold_demand),
        ("gold.inventory_position", gold_inventory),
        ("quarantine.r1", quarantined),
    )
    artifacts = tuple(
        _write_immutable_parquet(
            output_dir,
            dataset=dataset,
            records=records,
        )
        for dataset, records in artifact_inputs
    )

    return R1DataPathResult(
        as_of=as_of,
        product_locations=tuple(products),
        gold_demand=tuple(gold_demand),
        gold_inventory=tuple(gold_inventory),
        accepted_inventory_adjustments=tuple(accepted_events),
        quarantined=tuple(quarantined),
        artifacts=artifacts,
    )
