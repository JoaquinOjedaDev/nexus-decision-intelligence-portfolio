from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from nexus.r2d import (
    R2DParityError,
    build_portable_captured_route,
    normalize_business_outcome,
    require_business_parity,
    route_provenance,
    run_route_rebuilds,
    split_lf_jsonl_values,
)
from nexus.simulation import generate_r2_smoke_fixture

ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.r2d, pytest.mark.r2d_integration]


def test_direct_and_captured_routes_match_at_both_cutoffs_with_distinct_provenance(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "direct-source")
    direct_values = split_lf_jsonl_values(
        (fixture.source_dir / "inventory_events.jsonl").read_bytes()
    )
    capture = build_portable_captured_route(
        reference_source_dir=fixture.source_dir,
        output_dir=tmp_path / "captured-source",
    )
    captured = capture.package
    captured_values = split_lf_jsonl_values(
        (captured.source_dir / "inventory_events.jsonl").read_bytes()
    )

    direct_rebuilds = run_route_rebuilds(
        project_root=ROOT,
        source_dir=fixture.source_dir,
        artifact_dir=tmp_path / "direct-artifacts",
        early_cutoff=fixture.manifest.early_replay_cutoff,
        final_cutoff=fixture.manifest.planning_cutoff,
    )
    captured_rebuilds = run_route_rebuilds(
        project_root=ROOT,
        source_dir=captured.source_dir,
        artifact_dir=tmp_path / "captured-artifacts",
        early_cutoff=captured.manifest.early_replay_cutoff,
        final_cutoff=captured.manifest.planning_cutoff,
    )
    direct_outcome = normalize_business_outcome(
        raw_values=direct_values,
        rebuilds=direct_rebuilds,
    )
    captured_outcome = normalize_business_outcome(
        raw_values=captured_values,
        rebuilds=captured_rebuilds,
    )

    require_business_parity(direct_outcome, captured_outcome)
    assert direct_outcome == captured_outcome
    assert len(direct_outcome.cutoffs) == 2
    assert all(len(cutoff.gold) == 5 for cutoff in direct_outcome.cutoffs)

    direct_provenance = route_provenance(route="direct", rebuilds=direct_rebuilds)
    captured_provenance = route_provenance(
        route="captured",
        rebuilds=captured_rebuilds,
        checkpoint_sha256=capture.checkpoint.content_sha256,
    )
    assert direct_provenance != captured_provenance
    assert direct_provenance.source_manifest_sha256 != captured_provenance.source_manifest_sha256
    assert (
        direct_provenance.source_manifest_file_sha256
        != captured_provenance.source_manifest_file_sha256
    )
    assert direct_provenance.semantic_manifests != captured_provenance.semantic_manifests
    assert direct_provenance.checkpoint_sha256 is None
    assert captured_provenance.checkpoint_sha256 == capture.checkpoint.content_sha256

    assert all(capture.transcript.partition_records(partition) for partition in (0, 1))
    assert all(
        barrier.start_offset == 0
        and barrier.next_offset == barrier.end_offset_exclusive
        and len(barrier.physical_fingerprints)
        == barrier.end_offset_exclusive - barrier.start_offset
        for barrier in capture.checkpoint.partitions
    )
    assert capture.checkpoint.producer_ack_frozen_source_match
    assert capture.checkpoint.producer_ack_sha256 == capture.producer_ack_ledger.content_sha256
    assert capture.producer_ack_ledger.acknowledged_record_count == 9
    assert capture.producer_ack_ledger.real_broker_ack_claim is False
    correction_chain = [
        item
        for item in capture.transcript.records
        if item.record.lineage is not None
        and item.record.lineage.logical_event_id == "ADJ-CYCLE-001"
    ]
    assert len(correction_chain) == 2
    assert tuple(
        item.record.lineage.revision
        for item in sorted(correction_chain, key=lambda value: value.record.offset)
        if item.record.lineage is not None
    ) == (1, 2)
    assert len({item.record.partition for item in correction_chain}) == 1
    assert len({item.record.key_hex for item in correction_chain}) == 1

    early_inventory = pq.read_table(
        captured_rebuilds.early.gold_artifact("gold.r2_inventory_position").path
    ).to_pylist()
    final_inventory = pq.read_table(
        captured_rebuilds.final.gold_artifact("gold.r2_inventory_position").path
    ).to_pylist()
    early_target = next(
        row for row in early_inventory if row["sku_location_key"] == "SKU-001::DC-ARG-01"
    )
    final_target = next(
        row for row in final_inventory if row["sku_location_key"] == "SKU-001::DC-ARG-01"
    )
    assert early_target["adjustment_qty"] == -1
    assert final_target["adjustment_qty"] == 1
    assert captured_rebuilds.early.quality_counts["deferred_inventory_adjustments"] == 2
    assert captured_rebuilds.final.quality_counts["deferred_inventory_adjustments"] == 1
    assert captured_rebuilds.final.quality_counts["superseded_inventory_adjustments"] == 1


def test_business_parity_rejects_a_changed_raw_value_multiset(tmp_path: Path) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    values = split_lf_jsonl_values((fixture.source_dir / "inventory_events.jsonl").read_bytes())
    rebuilds = run_route_rebuilds(
        project_root=ROOT,
        source_dir=fixture.source_dir,
        artifact_dir=tmp_path / "artifacts",
        early_cutoff=fixture.manifest.early_replay_cutoff,
        final_cutoff=fixture.manifest.planning_cutoff,
    )
    outcome = normalize_business_outcome(raw_values=values, rebuilds=rebuilds)
    changed = replace(outcome, raw_value_sha256_counts=(("0" * 64, 1),))

    with pytest.raises(R2DParityError, match="normalized business evidence"):
        require_business_parity(outcome, changed)
