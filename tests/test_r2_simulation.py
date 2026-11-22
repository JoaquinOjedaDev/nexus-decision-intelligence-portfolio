from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from nexus.data.r2_models import (
    R2FulfillmentOutcome,
    R2InventoryAdjustmentPayload,
    R2OpeningInventory,
    R2PurchaseOrder,
    R2Receipt,
    R2SourceManifest,
    R2Supplier,
)
from nexus.simulation import R2_SMOKE_SEED, generate_r2_smoke_fixture

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_R2 = json.loads((ROOT / "fixtures" / "r2" / "expected.json").read_text(encoding="utf-8"))
EXPECTED_SOURCE_MANIFEST_SHA256 = EXPECTED_R2["source_manifest_sha256"]


def _source_bytes(source_dir: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in sorted(source_dir.iterdir()) if path.is_file()}


def test_same_seed_generates_byte_identical_bounded_source_packages(tmp_path: Path) -> None:
    first = generate_r2_smoke_fixture(tmp_path / "first")
    second = generate_r2_smoke_fixture(tmp_path / "second")
    different_seed = generate_r2_smoke_fixture(
        tmp_path / "different-seed",
        seed=R2_SMOKE_SEED + 1,
    )

    assert first.manifest_sha256 == second.manifest_sha256 == EXPECTED_SOURCE_MANIFEST_SHA256
    assert first.manifest.seed == EXPECTED_R2["seed"]
    assert _source_bytes(first.source_dir) == _source_bytes(second.source_dir)
    assert tuple(source.dataset for source in first.manifest.sources) == tuple(
        sorted(source.dataset for source in first.manifest.sources)
    )
    assert first.manifest.expected_controls.model_dump() == {
        "accepted_inventory_adjustments": 5,
        "deferred_inventory_adjustments": 1,
        "fulfillment_rows": 112,
        "opening_inventory_rows": 4,
        "product_location_rows": 4,
        "purchase_order_rows": 20,
        "quarantine_rows": 4,
        "receipt_rows": 16,
        "supplier_rows": 2,
        "superseded_inventory_adjustments": 1,
    }

    assert different_seed.manifest.seed == R2_SMOKE_SEED + 1
    assert different_seed.manifest_sha256 != first.manifest_sha256
    assert (different_seed.source_dir / "fulfillment_outcomes.csv").read_bytes() != (
        first.source_dir / "fulfillment_outcomes.csv"
    ).read_bytes()


def test_generator_is_idempotent_but_refuses_to_mutate_existing_sources(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    first = generate_r2_smoke_fixture(source_dir)
    initial_state = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in source_dir.iterdir()
        if path.is_file()
    }

    second = generate_r2_smoke_fixture(source_dir)

    assert second.manifest_sha256 == first.manifest_sha256
    assert {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in source_dir.iterdir()
        if path.is_file()
    } == initial_state

    with pytest.raises(FileExistsError, match="refusing to mutate deterministic source file"):
        generate_r2_smoke_fixture(source_dir, seed=R2_SMOKE_SEED + 1)


def test_r2_models_reject_invalid_time_quantity_and_manifest_invariants(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    manifest_data = fixture.manifest.model_dump(mode="python")
    manifest_data["early_replay_cutoff"] = manifest_data["planning_cutoff"]
    with pytest.raises(ValidationError, match="early_replay_cutoff must precede"):
        R2SourceManifest.model_validate(manifest_data)

    duplicate_sources = fixture.manifest.model_dump(mode="python")
    duplicate_sources["sources"] = (
        fixture.manifest.sources[0],
        fixture.manifest.sources[0],
    )
    with pytest.raises(ValidationError, match="source file names and dataset names must be unique"):
        R2SourceManifest.model_validate(duplicate_sources)

    snapshot_at = datetime(2026, 6, 1, tzinfo=UTC)
    with pytest.raises(ValidationError, match="available_at cannot precede snapshot_at"):
        R2OpeningInventory(
            sku_id="SKU-001",
            location_id="DC-ARG-01",
            on_hand_qty=1,
            snapshot_at=snapshot_at,
            available_at=snapshot_at - timedelta(seconds=1),
        )

    with pytest.raises(ValidationError, match="fulfilled_qty cannot exceed demand_qty"):
        R2FulfillmentOutcome(
            fulfillment_id="FUL-001",
            demand_date=snapshot_at.date(),
            sku_id="SKU-001",
            location_id="DC-ARG-01",
            demand_qty=1,
            fulfilled_qty=2,
            available_at=snapshot_at + timedelta(days=1),
        )

    with pytest.raises(ValidationError, match="expected_at must follow ordered_at"):
        R2PurchaseOrder(
            order_id="PO-001",
            sku_id="SKU-001",
            location_id="DC-ARG-01",
            supplier_id="SUP-001",
            ordered_qty=1,
            ordered_at=snapshot_at,
            expected_at=snapshot_at,
            available_at=snapshot_at,
        )

    with pytest.raises(ValidationError, match="available_at cannot precede received_at"):
        R2Receipt(
            receipt_id="RCPT-001",
            order_id="PO-001",
            sku_id="SKU-001",
            location_id="DC-ARG-01",
            supplier_id="SUP-001",
            received_qty=1,
            received_at=snapshot_at,
            available_at=snapshot_at - timedelta(seconds=1),
        )

    with pytest.raises(ValidationError, match="delta_qty must not be zero"):
        R2InventoryAdjustmentPayload(
            logical_event_id="ADJ-001",
            revision=1,
            sku_id="SKU-001",
            location_id="DC-ARG-01",
            delta_qty=0,
            reason_code="test",
        )

    with pytest.raises(ValidationError, match="timezone info"):
        R2Supplier(
            supplier_id="SUP-001",
            supplier_name="Supplier",
            active=True,
            available_at=datetime(2026, 6, 1),
        )
