"""Separately declared deterministic R2 learning sources and synthetic finalization.

The frozen 28-day generator is never called or modified. Inventory/fulfillment
remain arithmetically consistent; correction closure is an explicit simulation
rule, not evidence of real business finality.
"""

from __future__ import annotations

import csv
import io
import json
import os
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from nexus.data.r2_models import (
    R2ExpectedControls,
    R2FulfillmentOutcome,
    R2OpeningInventory,
    R2ProductLocation,
    R2PurchaseOrder,
    R2Receipt,
    R2Record,
    R2SourceFile,
    R2SourceManifest,
    R2Supplier,
)
from nexus.portfolio.runtime_pipeline import runtime_source_fingerprint
from nexus.portfolio.training_data import (
    FinalizationLabel,
    FinalizationManifest,
    fulfillment_rows_sha256,
)
from nexus.storage import file_sha256

TRAINING_START_DATE = date(2025, 6, 1)


@dataclass(frozen=True, slots=True)
class TrainingFixture:
    source_dir: Path
    finalization_path: Path
    source_snapshot_sha256: str
    finalization_sha256: str
    planning_cutoff: datetime


def _write_once(path: Path, content: bytes) -> None:
    temporary = path.parent / f".{path.name}-{uuid4().hex}.tmp"
    owned = False
    try:
        with temporary.open("xb") as handle:
            owned = True
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != content:
                raise ValueError(
                    f"learning fixture conflicts with existing file: {path.name}"
                ) from None
    finally:
        if owned:
            temporary.unlink(missing_ok=True)


def _encoded(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True, indent=2, allow_nan=False) + "\n").encode("utf-8")


def _csv(records: list[R2Record], fields: tuple[str, ...]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for record in records:
        payload = record.model_dump(mode="json")
        writer.writerow(
            {
                key: str(value).lower() if isinstance(value, bool) else value
                for key, value in payload.items()
            }
        )
    return output.getvalue().encode("utf-8")


def generate_training_fixture(
    output_dir: Path,
    *,
    seed: int = 20260718,
    days: int = 365,
) -> TrainingFixture:
    """Generate a new source package; identical replay is allowed, replacement is not."""
    if type(seed) is not int or seed < 0 or type(days) is not int or not 28 <= days <= 730:
        raise ValueError("learning fixture requires nonnegative integer seed and28..730days")
    root = output_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    end = TRAINING_START_DATE + timedelta(days=days)
    cutoff = datetime.combine(end + timedelta(days=3), time(4), UTC)
    master_at = datetime.combine(TRAINING_START_DATE - timedelta(days=1), time(8), UTC)
    grains = (
        ("SKU-001", "DC-ARG-01", "SUP-001"),
        ("SKU-001", "DC-ARG-02", "SUP-001"),
        ("SKU-002", "DC-ARG-01", "SUP-002"),
        ("SKU-002", "DC-ARG-02", "SUP-002"),
    )
    suppliers = [
        R2Supplier(
            supplier_id=f"SUP-{number:03}",
            supplier_name=f"Synthetic learning supplier{number}",
            active=True,
            available_at=master_at,
        )
        for number in (1, 2)
    ]
    products = [
        R2ProductLocation(
            sku_id=sku,
            location_id=location,
            supplier_id=supplier,
            active=True,
            available_at=master_at,
        )
        for sku, location, supplier in grains
    ]
    openings: list[R2OpeningInventory] = []
    fulfillments: list[R2FulfillmentOutcome] = []
    orders: list[R2PurchaseOrder] = []
    receipts: list[R2Receipt] = []
    generator = random.Random(seed)
    pattern = (0, 1, -1, 2, 0, 3, -2)
    for index, (sku, location, supplier) in enumerate(grains):
        stock = 60 + index * 10
        opening = datetime.combine(TRAINING_START_DATE, time.min, UTC)
        openings.append(
            R2OpeningInventory(
                sku_id=sku,
                location_id=location,
                on_hand_qty=stock,
                snapshot_at=opening,
                available_at=opening + timedelta(minutes=5),
            )
        )
        received: dict[date, int] = {}
        for offset in range(0, days, 7):
            order_day = TRAINING_START_DATE + timedelta(days=offset)
            ordered_at = datetime.combine(order_day, time(6), UTC)
            expected = datetime.combine(order_day + timedelta(days=2), time(12), UTC)
            # Fixed ex-ante mean weekly demand: 7*(8+index) + sum(pattern).
            # No future realized demand, stock observation, or RNG call is used.
            quantity = 59 + index * 7
            identifier = f"LEARN-PO-{sku}-{location}-{offset:04}"
            orders.append(
                R2PurchaseOrder(
                    order_id=identifier,
                    sku_id=sku,
                    location_id=location,
                    supplier_id=supplier,
                    ordered_qty=quantity,
                    ordered_at=ordered_at,
                    expected_at=expected,
                    available_at=ordered_at + timedelta(minutes=5),
                )
            )
            if expected.date() < end:
                receipts.append(
                    R2Receipt(
                        receipt_id="RCPT-" + identifier,
                        order_id=identifier,
                        sku_id=sku,
                        location_id=location,
                        supplier_id=supplier,
                        received_qty=quantity,
                        received_at=expected,
                        available_at=expected + timedelta(minutes=5),
                    )
                )
                received[expected.date()] = quantity
        for offset in range(days):
            demand_day = TRAINING_START_DATE + timedelta(days=offset)
            stock += received.get(demand_day, 0)
            demand = max(0, 8 + index + pattern[demand_day.weekday()] + generator.randint(-1, 1))
            fulfilled = min(stock, demand)
            stock -= fulfilled
            fulfillments.append(
                R2FulfillmentOutcome(
                    fulfillment_id=f"LEARN-FUL-{demand_day:%Y%m%d}-{sku}-{location}",
                    demand_date=demand_day,
                    sku_id=sku,
                    location_id=location,
                    demand_qty=demand,
                    fulfilled_qty=fulfilled,
                    available_at=datetime.combine(demand_day + timedelta(days=1), time(3), UTC),
                )
            )
    tables: tuple[tuple[str, str, list[Any], type[R2Record]], ...] = (
        ("source.suppliers", "suppliers.csv", suppliers, R2Supplier),
        ("source.product_locations", "product_locations.csv", products, R2ProductLocation),
        ("source.opening_inventory", "opening_inventory.csv", openings, R2OpeningInventory),
        (
            "source.fulfillment_outcomes",
            "fulfillment_outcomes.csv",
            fulfillments,
            R2FulfillmentOutcome,
        ),
        ("source.purchase_orders", "purchase_orders.csv", orders, R2PurchaseOrder),
        ("source.receipts", "receipts.csv", receipts, R2Receipt),
    )
    sources: list[R2SourceFile] = []
    for dataset, name, records, contract in tables:
        path = root / name
        _write_once(path, _csv(records, tuple(contract.model_fields)))
        sources.append(
            R2SourceFile(
                dataset=dataset,
                file_name=name,
                content_sha256=file_sha256(path),
                record_count=len(records),
            )
        )
    events_path = root / "inventory_events.jsonl"
    _write_once(events_path, b"")
    sources.append(
        R2SourceFile(
            dataset="source.inventory_adjustments",
            file_name=events_path.name,
            content_sha256=file_sha256(events_path),
            record_count=0,
        )
    )
    manifest = R2SourceManifest(
        schema_version="1.0.0",
        classification="synthetic",
        generator="nexus-r2-learning-supply-network",
        profile="learning",
        seed=seed,
        start_date=TRAINING_START_DATE,
        days=days,
        planning_cutoff=cutoff,
        early_replay_cutoff=datetime.combine(
            TRAINING_START_DATE + timedelta(days=14), time(4), UTC
        ),
        sources=tuple(sorted(sources, key=lambda item: item.dataset)),
        expected_controls=R2ExpectedControls(
            accepted_inventory_adjustments=0,
            deferred_inventory_adjustments=0,
            fulfillment_rows=len(fulfillments),
            opening_inventory_rows=len(openings),
            product_location_rows=len(products),
            purchase_order_rows=len(orders),
            quarantine_rows=0,
            receipt_rows=len(receipts),
            supplier_rows=len(suppliers),
            superseded_inventory_adjustments=0,
        ),
    )
    _write_once(root / "source-manifest.json", _encoded(manifest.model_dump(mode="json")))
    source_hash = runtime_source_fingerprint(root)
    closure = FinalizationManifest(
        contract_version="nexus.synthetic-label-finalization.v1",
        classification="synthetic",
        authority="nexus-r2-learning-supply-network",
        source_snapshot_sha256=source_hash,
        rule="explicit_synthetic_closure_72h_after_daily_completion",
        labels=tuple(
            FinalizationLabel(
                sku_id=row.sku_id,
                location_id=row.location_id,
                demand_date=row.demand_date,
                source_rows_sha256=fulfillment_rows_sha256((row,)),
                correction_closed=True,
                correction_closed_at=datetime.combine(
                    row.demand_date + timedelta(days=4), time.min, UTC
                ),
            )
            for row in sorted(
                fulfillments, key=lambda row: (row.sku_id, row.location_id, row.demand_date)
            )
        ),
    )
    closure_path = root / "outcome-finalization.json"
    _write_once(closure_path, _encoded(closure.model_dump(mode="json")))
    closure_hash = file_sha256(closure_path)
    _write_once(
        root / "learning-recipe.json",
        _encoded(
            {
                "contract_version": "nexus.training-fixture-recipe.v1",
                "generator": "nexus-r2-learning-supply-network",
                "generator_version": "1.1.0",
                "classification": "synthetic",
                "seed": seed,
                "days": days,
                "start_date": TRAINING_START_DATE.isoformat(),
                "last_demand_date": (end - timedelta(days=1)).isoformat(),
                "planning_cutoff": cutoff.isoformat(),
                "source_snapshot_sha256": source_hash,
                "finalization_sha256": closure_hash,
                "finalization_file": closure_path.name,
                "correction_closure": "Explicit simulation rule; no real-world finality claim",
                "demand_process": "Seeded weekday pattern with bounded integer noise",
                "fulfillment_process": (
                    "Opening stock plus weekly receipts minus actual fulfilled units"
                ),
                "replenishment_policy": (
                    "Fixed weekly quantity 59 + 7*series_index, the declared process mean; "
                    "two-day lead time; no future realized demand; no historical capacity model"
                ),
                "frozen_smoke_fixture_modified": False,
            }
        ),
    )
    return TrainingFixture(root, closure_path, source_hash, closure_hash, cutoff)
