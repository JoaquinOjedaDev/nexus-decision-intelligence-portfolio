from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
from pytest import MonkeyPatch

from nexus.data import QuarantineReason, run_r1_data_path

FIXTURES = Path(__file__).parents[1] / "fixtures" / "r1"
AS_OF = datetime(2026, 7, 15, 12, tzinfo=UTC)


def test_r1_fixture_produces_expected_gold_records_and_quarantine(tmp_path: Path) -> None:
    result = run_r1_data_path(
        source_dir=FIXTURES,
        output_dir=tmp_path / "lake",
        as_of=AS_OF,
    )

    product = result.product_locations[0]
    assert (product.sku_id, product.location_id) == ("SKU-001", "DC-ARG-01")
    assert (product.lead_time_days, product.safety_stock_qty) == (7, 15)

    demand = result.demand_for("SKU-001", "DC-ARG-01")
    assert len(demand) == 14
    assert [row.demand_qty for row in demand[-7:]] == [11, 13, 9, 12, 14, 10, 8]
    assert sum(row.demand_qty for row in demand[-7:]) == 77

    assert len(result.gold_inventory) == 1
    inventory = result.gold_inventory[0]
    assert (inventory.on_hand_qty, inventory.on_order_qty) == (35, 10)
    assert inventory.adjustment_event_count == 1
    assert len(result.accepted_inventory_adjustments) == 1
    assert result.accepted_inventory_adjustments[0].delta_qty == -5

    assert [row.reason_code for row in result.quarantined] == [
        QuarantineReason.DUPLICATE_EVENT_ID,
        QuarantineReason.EVENT_CONTRACT_INVALID,
    ]

    gold_demand_artifact = result.artifact("gold.demand_history")
    gold_inventory_artifact = result.artifact("gold.inventory_position")
    raw_event_artifact = result.artifact("raw.inventory_adjustments")
    assert pq.read_table(gold_demand_artifact.path).num_rows == 14
    assert pq.read_table(gold_inventory_artifact.path).to_pylist() == [
        {
            "sku_id": "SKU-001",
            "location_id": "DC-ARG-01",
            "on_hand_qty": 35,
            "on_order_qty": 10,
            "snapshot_at": "2026-07-15T07:00:00Z",
            "available_at": "2026-07-15T08:30:03Z",
            "as_of": "2026-07-15T12:00:00Z",
            "adjustment_event_count": 1,
        }
    ]
    assert pq.read_table(raw_event_artifact.path).num_rows == 1

    manifest = json.loads((FIXTURES / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["classification"] == "synthetic"
    assert manifest["expected_controls"] == {
        "accepted_inventory_events": len(result.accepted_inventory_adjustments),
        "duplicate_events": 1,
        "gold_demand_rows": len(result.gold_demand),
        "malformed_events": 1,
    }


def test_r1_outputs_are_content_addressed_and_second_run_does_not_rewrite(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "lake"
    first = run_r1_data_path(source_dir=FIXTURES, output_dir=output_dir, as_of=AS_OF)
    first_state = {
        artifact.dataset: (
            artifact.path,
            artifact.content_sha256,
            artifact.path.stat().st_mtime_ns,
        )
        for artifact in first.artifacts
    }

    second = run_r1_data_path(source_dir=FIXTURES, output_dir=output_dir, as_of=AS_OF)
    second_state = {
        artifact.dataset: (
            artifact.path,
            artifact.content_sha256,
            artifact.path.stat().st_mtime_ns,
        )
        for artifact in second.artifacts
    }

    assert second_state == first_state
    assert len(list(output_dir.rglob("*.parquet"))) == 7
    assert not list(output_dir.rglob("*.tmp"))


def test_duplicate_idempotency_key_is_rejected_even_with_a_new_event_id(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    shutil.copytree(FIXTURES, source_dir)
    events_path = source_dir / "inventory_events.jsonl"
    original_lines = events_path.read_text(encoding="utf-8").splitlines()
    duplicate = json.loads(original_lines[0])
    duplicate["event_id"] = "33333333-3333-4333-8333-333333333333"
    events_path.write_text(
        "\n".join([*original_lines, json.dumps(duplicate, separators=(",", ":"))]) + "\n",
        encoding="utf-8",
    )

    result = run_r1_data_path(
        source_dir=source_dir,
        output_dir=tmp_path / "lake",
        as_of=AS_OF,
    )

    assert len(result.accepted_inventory_adjustments) == 1
    assert (
        sum(
            row.reason_code is QuarantineReason.DUPLICATE_IDEMPOTENCY_KEY
            for row in result.quarantined
        )
        == 1
    )
    assert result.gold_inventory[0].on_hand_qty == 35


def test_event_must_be_occurred_and_ingested_by_as_of(tmp_path: Path) -> None:
    result = run_r1_data_path(
        source_dir=FIXTURES,
        output_dir=tmp_path / "lake",
        as_of=datetime(2026, 7, 15, 8, tzinfo=UTC),
    )

    assert result.accepted_inventory_adjustments == ()
    assert result.gold_inventory[0].on_hand_qty == 40
    assert (
        sum(
            row.reason_code is QuarantineReason.EVENT_NOT_AVAILABLE_AS_OF
            for row in result.quarantined
        )
        == 2
    )
    assert any(
        row.reason_code is QuarantineReason.EVENT_CONTRACT_INVALID for row in result.quarantined
    )


def test_old_dated_batch_rows_received_after_cutoff_are_quarantined(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    shutil.copytree(FIXTURES, source_dir)
    demand_path = source_dir / "demand_history.csv"
    demand_text = demand_path.read_text(encoding="utf-8").replace(
        "2026-07-08,SKU-001,DC-ARG-01,11,2026-07-15T08:00:00Z",
        "2026-07-08,SKU-001,DC-ARG-01,999,2026-07-15T09:00:01Z",
    )
    demand_path.write_text(demand_text, encoding="utf-8")
    inventory_path = source_dir / "inventory_snapshot.csv"
    inventory_text = inventory_path.read_text(encoding="utf-8").replace(
        "40,10,2026-07-15T07:00:03Z",
        "999,10,2026-07-15T09:00:01Z",
    )
    inventory_path.write_text(inventory_text, encoding="utf-8")

    result = run_r1_data_path(
        source_dir=source_dir,
        output_dir=tmp_path / "lake",
        as_of=datetime(2026, 7, 15, 9, tzinfo=UTC),
    )

    assert len(result.gold_demand) == 13
    assert all(row.demand_qty != 999 for row in result.gold_demand)
    assert result.gold_inventory == ()
    assert (
        sum(
            row.reason_code is QuarantineReason.BATCH_NOT_AVAILABLE_AS_OF
            for row in result.quarantined
        )
        == 2
    )


def test_pipeline_uses_only_explicit_source_and_output_paths(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    unrelated_working_dir = tmp_path / "working"
    unrelated_working_dir.mkdir()
    monkeypatch.chdir(unrelated_working_dir)

    result = run_r1_data_path(
        source_dir=FIXTURES,
        output_dir=tmp_path / "requested-output",
        as_of=AS_OF,
    )

    assert not list(unrelated_working_dir.iterdir())
    assert all(
        artifact.path.is_relative_to(tmp_path / "requested-output") for artifact in result.artifacts
    )
