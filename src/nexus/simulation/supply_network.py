"""Deterministic bounded supply-network generator for the R2A gate."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import random
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel

from nexus.contracts import EventEnvelope
from nexus.data.r2_models import (
    R2ExpectedControls,
    R2FulfillmentOutcome,
    R2GeneratedFixture,
    R2InventoryAdjustmentPayload,
    R2OpeningInventory,
    R2ProductLocation,
    R2PurchaseOrder,
    R2Receipt,
    R2SourceFile,
    R2SourceManifest,
    R2Supplier,
)
from nexus.storage import file_sha256

R2_SMOKE_SEED = 20260718
R2_SMOKE_DAYS = 28
R2_SMOKE_START_DATE = date(2026, 6, 1)
R2_SMOKE_EARLY_CUTOFF = datetime(2026, 6, 20, tzinfo=UTC)
R2_SMOKE_PLANNING_CUTOFF = datetime(2026, 6, 30, tzinfo=UTC)
_SCHEMA_VERSION: Literal["1.0.0"] = "1.0.0"
_SUPPLIERS = (("SUP-001", "Andes Components"), ("SUP-002", "Pampas Supply"))
_GRAINS = (
    ("SKU-001", "DC-ARG-01", "SUP-001", 100),
    ("SKU-001", "DC-ARG-02", "SUP-002", 80),
    ("SKU-002", "DC-ARG-01", "SUP-002", 60),
    ("SKU-002", "DC-ARG-02", "SUP-001", 50),
)


def _utc(day: date, hour: int = 0, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour=hour, minute=minute), tzinfo=UTC)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _model_row(record: BaseModel) -> dict[str, object]:
    return dict(record.model_dump(mode="json"))


def _csv_text(fieldnames: Sequence[str], rows: Sequence[Mapping[str, object]]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=list(fieldnames),
        lineterminator="\n",
        extrasaction="raise",
    )
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def _write_exact(path: Path, text: str) -> None:
    encoded = text.encode("utf-8")
    if path.exists():
        if path.read_bytes() != encoded:
            raise FileExistsError(f"refusing to mutate deterministic source file: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded)


def _event(
    *,
    event_name: str,
    logical_event_id: str,
    revision: int,
    sku_id: str,
    location_id: str,
    delta_qty: int,
    occurred_at: datetime,
    ingested_at: datetime,
    idempotency_key: str | None = None,
) -> dict[str, object]:
    payload = R2InventoryAdjustmentPayload(
        logical_event_id=logical_event_id,
        revision=revision,
        sku_id=sku_id,
        location_id=location_id,
        delta_qty=delta_qty,
        reason_code="synthetic_reconciliation",
    )
    envelope = EventEnvelope(
        event_id=uuid5(NAMESPACE_URL, f"nexus-r2:{event_name}"),
        event_type="inventory.adjusted",
        event_version=1,
        source="synthetic.wms",
        subject_id=f"{sku_id}:{location_id}",
        occurred_at=occurred_at,
        ingested_at=ingested_at,
        idempotency_key=idempotency_key or f"r2-{event_name}",
        correlation_id="r2-smoke",
        trace_id="trace-r2-smoke",
        payload=payload.model_dump(mode="json"),
    )
    return dict(envelope.model_dump(mode="json"))


def _build_adjustment_lines() -> list[str]:
    cycle_original = _event(
        event_name="cycle-original",
        logical_event_id="ADJ-CYCLE-001",
        revision=1,
        sku_id="SKU-001",
        location_id="DC-ARG-01",
        delta_qty=-5,
        occurred_at=datetime(2026, 6, 10, 10, tzinfo=UTC),
        ingested_at=datetime(2026, 6, 10, 12, tzinfo=UTC),
    )
    earlier_promo = _event(
        event_name="promo-display",
        logical_event_id="ADJ-PROMO-001",
        revision=1,
        sku_id="SKU-001",
        location_id="DC-ARG-01",
        delta_qty=4,
        occurred_at=datetime(2026, 6, 5, 9, tzinfo=UTC),
        ingested_at=datetime(2026, 6, 5, 11, tzinfo=UTC),
    )
    cycle_correction = _event(
        event_name="cycle-correction",
        logical_event_id="ADJ-CYCLE-001",
        revision=2,
        sku_id="SKU-001",
        location_id="DC-ARG-01",
        delta_qty=-3,
        occurred_at=datetime(2026, 6, 10, 10, tzinfo=UTC),
        ingested_at=datetime(2026, 6, 25, 10, tzinfo=UTC),
    )
    second_grain = _event(
        event_name="second-grain",
        logical_event_id="ADJ-CYCLE-002",
        revision=1,
        sku_id="SKU-002",
        location_id="DC-ARG-02",
        delta_qty=2,
        occurred_at=datetime(2026, 6, 15, 9, tzinfo=UTC),
        ingested_at=datetime(2026, 6, 16, 9, tzinfo=UTC),
    )
    future_event = _event(
        event_name="future-arrival",
        logical_event_id="ADJ-FUTURE-001",
        revision=1,
        sku_id="SKU-001",
        location_id="DC-ARG-02",
        delta_qty=100,
        occurred_at=datetime(2026, 6, 28, 9, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 1, 9, tzinfo=UTC),
    )
    duplicate_idempotency = _event(
        event_name="idempotency-redelivery",
        logical_event_id="ADJ-PROMO-001",
        revision=1,
        sku_id="SKU-001",
        location_id="DC-ARG-01",
        delta_qty=4,
        occurred_at=datetime(2026, 6, 5, 9, tzinfo=UTC),
        ingested_at=datetime(2026, 6, 5, 12, tzinfo=UTC),
        idempotency_key=str(earlier_promo["idempotency_key"]),
    )
    unknown_product = _event(
        event_name="unknown-product",
        logical_event_id="ADJ-UNKNOWN-001",
        revision=1,
        sku_id="SKU-999",
        location_id="DC-ARG-99",
        delta_qty=-1,
        occurred_at=datetime(2026, 6, 12, 9, tzinfo=UTC),
        ingested_at=datetime(2026, 6, 12, 10, tzinfo=UTC),
    )

    # Physical order is deliberately not event-time order.
    ordered = [
        cycle_original,
        earlier_promo,
        second_grain,
        cycle_correction,
        future_event,
        earlier_promo,
        duplicate_idempotency,
        unknown_product,
    ]
    lines = [_canonical_json(event) for event in ordered]
    lines.append('{"contract_version":"1.0","event_type":"inventory.adjusted","payload":')
    return lines


def _generate_records(
    *,
    seed: int,
) -> tuple[
    list[R2Supplier],
    list[R2ProductLocation],
    list[R2OpeningInventory],
    list[R2FulfillmentOutcome],
    list[R2PurchaseOrder],
    list[R2Receipt],
]:
    rng = random.Random(seed)
    master_available_at = datetime(2026, 5, 31, 8, tzinfo=UTC)
    suppliers = [
        R2Supplier(
            supplier_id=supplier_id,
            supplier_name=name,
            active=True,
            available_at=master_available_at,
        )
        for supplier_id, name in _SUPPLIERS
    ]
    products = [
        R2ProductLocation(
            sku_id=sku_id,
            location_id=location_id,
            supplier_id=supplier_id,
            active=True,
            available_at=master_available_at,
        )
        for sku_id, location_id, supplier_id, _ in _GRAINS
    ]
    openings = [
        R2OpeningInventory(
            sku_id=sku_id,
            location_id=location_id,
            on_hand_qty=opening_qty,
            snapshot_at=datetime(2026, 6, 1, 0, tzinfo=UTC),
            available_at=datetime(2026, 6, 1, 0, 5, tzinfo=UTC),
        )
        for sku_id, location_id, _, opening_qty in _GRAINS
    ]

    orders: list[R2PurchaseOrder] = []
    receipts: list[R2Receipt] = []
    receipt_quantities_by_day: dict[tuple[str, str, date], int] = {}
    order_days = (0, 7, 14, 21, 27)
    for grain_index, (sku_id, location_id, supplier_id, _) in enumerate(_GRAINS):
        order_qty = 55 + 5 * (grain_index % 2) + 5 * (grain_index // 2)
        for order_index, day_offset in enumerate(order_days, start=1):
            order_day = R2_SMOKE_START_DATE + timedelta(days=day_offset)
            order_id = f"PO-{sku_id}-{location_id}-{order_index:02d}"
            expected_at = _utc(order_day + timedelta(days=3), 12)
            orders.append(
                R2PurchaseOrder(
                    order_id=order_id,
                    sku_id=sku_id,
                    location_id=location_id,
                    supplier_id=supplier_id,
                    ordered_qty=order_qty,
                    ordered_at=_utc(order_day, 9),
                    expected_at=expected_at,
                    available_at=_utc(order_day, 9, 5),
                )
            )
            if order_index == len(order_days):
                continue
            delay_days = 1 if supplier_id == "SUP-001" and order_index == 2 else 0
            received_at = expected_at + timedelta(days=delay_days)
            receipt_id = f"RCPT-{sku_id}-{location_id}-{order_index:02d}"
            receipts.append(
                R2Receipt(
                    receipt_id=receipt_id,
                    order_id=order_id,
                    sku_id=sku_id,
                    location_id=location_id,
                    supplier_id=supplier_id,
                    received_qty=order_qty,
                    received_at=received_at,
                    available_at=received_at + timedelta(minutes=5),
                )
            )
            key = (sku_id, location_id, received_at.date())
            receipt_quantities_by_day[key] = receipt_quantities_by_day.get(key, 0) + order_qty

    fulfillments: list[R2FulfillmentOutcome] = []
    weekday_pattern = (0, 1, -1, 2, 0, 3, -2)
    for grain_index, (sku_id, location_id, _, opening_qty) in enumerate(_GRAINS):
        available_stock = opening_qty
        base_demand = 8 + 2 * (grain_index // 2) + grain_index % 2
        for day_offset in range(R2_SMOKE_DAYS):
            demand_day = R2_SMOKE_START_DATE + timedelta(days=day_offset)
            available_stock += receipt_quantities_by_day.get(
                (sku_id, location_id, demand_day),
                0,
            )
            demand_qty = max(
                0,
                base_demand + weekday_pattern[demand_day.weekday()] + rng.randint(-1, 1),
            )
            fulfilled_qty = min(available_stock, demand_qty)
            available_stock -= fulfilled_qty
            fulfillments.append(
                R2FulfillmentOutcome(
                    fulfillment_id=f"FUL-{demand_day:%Y%m%d}-{sku_id}-{location_id}",
                    demand_date=demand_day,
                    sku_id=sku_id,
                    location_id=location_id,
                    demand_qty=demand_qty,
                    fulfilled_qty=fulfilled_qty,
                    available_at=_utc(demand_day + timedelta(days=1), 3),
                )
            )

    return suppliers, products, openings, fulfillments, orders, receipts


def generate_r2_smoke_fixture(
    output_dir: Path,
    *,
    seed: int = R2_SMOKE_SEED,
) -> R2GeneratedFixture:
    """Generate byte-identical smoke sources for the same seed and contract."""

    output_dir = output_dir.resolve()
    suppliers, products, openings, fulfillments, orders, receipts = _generate_records(seed=seed)
    files: dict[str, tuple[str, str, int]] = {}

    csv_specs: tuple[tuple[str, str, Sequence[str], Sequence[BaseModel]], ...] = (
        (
            "source.suppliers",
            "suppliers.csv",
            ("supplier_id", "supplier_name", "active", "available_at"),
            suppliers,
        ),
        (
            "source.product_locations",
            "product_locations.csv",
            ("sku_id", "location_id", "supplier_id", "active", "available_at"),
            products,
        ),
        (
            "source.opening_inventory",
            "opening_inventory.csv",
            ("sku_id", "location_id", "on_hand_qty", "snapshot_at", "available_at"),
            openings,
        ),
        (
            "source.fulfillment_outcomes",
            "fulfillment_outcomes.csv",
            (
                "fulfillment_id",
                "demand_date",
                "sku_id",
                "location_id",
                "demand_qty",
                "fulfilled_qty",
                "available_at",
            ),
            fulfillments,
        ),
        (
            "source.purchase_orders",
            "purchase_orders.csv",
            (
                "order_id",
                "sku_id",
                "location_id",
                "supplier_id",
                "ordered_qty",
                "ordered_at",
                "expected_at",
                "available_at",
            ),
            orders,
        ),
        (
            "source.receipts",
            "receipts.csv",
            (
                "receipt_id",
                "order_id",
                "sku_id",
                "location_id",
                "supplier_id",
                "received_qty",
                "received_at",
                "available_at",
            ),
            receipts,
        ),
    )
    for dataset, file_name, fields, records in csv_specs:
        path = output_dir / file_name
        _write_exact(path, _csv_text(fields, [_model_row(record) for record in records]))
        files[dataset] = (file_name, file_sha256(path), len(records))

    adjustment_lines = _build_adjustment_lines()
    adjustment_path = output_dir / "inventory_events.jsonl"
    _write_exact(adjustment_path, "\n".join(adjustment_lines) + "\n")
    files["source.inventory_adjustments"] = (
        adjustment_path.name,
        file_sha256(adjustment_path),
        len(adjustment_lines),
    )

    sources = tuple(
        R2SourceFile(
            dataset=dataset,
            file_name=file_name,
            content_sha256=content_sha256,
            record_count=record_count,
        )
        for dataset, (file_name, content_sha256, record_count) in sorted(files.items())
    )
    manifest = R2SourceManifest(
        schema_version=_SCHEMA_VERSION,
        classification="synthetic",
        generator="nexus-r2-supply-network",
        profile="smoke",
        seed=seed,
        start_date=R2_SMOKE_START_DATE,
        days=R2_SMOKE_DAYS,
        planning_cutoff=R2_SMOKE_PLANNING_CUTOFF,
        early_replay_cutoff=R2_SMOKE_EARLY_CUTOFF,
        sources=sources,
        expected_controls=R2ExpectedControls(
            accepted_inventory_adjustments=5,
            deferred_inventory_adjustments=1,
            fulfillment_rows=len(fulfillments),
            opening_inventory_rows=len(openings),
            product_location_rows=len(products),
            purchase_order_rows=len(orders),
            quarantine_rows=4,
            receipt_rows=len(receipts),
            supplier_rows=len(suppliers),
            superseded_inventory_adjustments=1,
        ),
    )
    manifest_text = (
        json.dumps(
            manifest.model_dump(mode="json"),
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
        + "\n"
    )
    manifest_path = output_dir / "source-manifest.json"
    _write_exact(manifest_path, manifest_text)
    manifest_sha256 = hashlib.sha256(
        _canonical_json(manifest.model_dump(mode="json")).encode("utf-8")
    ).hexdigest()
    return R2GeneratedFixture(
        source_dir=output_dir,
        manifest_path=manifest_path,
        manifest=manifest,
        manifest_sha256=manifest_sha256,
        manifest_file_sha256=hashlib.sha256(manifest_text.encode("utf-8")).hexdigest(),
    )
