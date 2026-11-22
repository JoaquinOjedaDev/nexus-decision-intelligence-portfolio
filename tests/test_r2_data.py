from __future__ import annotations

import copy
import json
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import nexus.data.r2_pipeline as r2_pipeline_module
from nexus.data import (
    R2IdentityConflictError,
    R2LandingResult,
    R2QuarantineReason,
    R2SourceContractError,
    R2SourceIntegrityError,
    R2SourceManifest,
    land_r2_sources,
)
from nexus.r2 import R2ExpectedControlError, run_r2_rebuild
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import ArtifactConflictError, file_sha256


def _rebind_source_file(source_dir: Path, file_name: str) -> None:
    source_path = source_dir / file_name
    manifest_path = source_dir / "source-manifest.json"
    manifest = R2SourceManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    if source_path.suffix == ".csv":
        record_count = max(
            len(source_path.read_text(encoding="utf-8").splitlines()) - 1,
            0,
        )
    else:
        record_count = sum(
            1 for line in source_path.read_text(encoding="utf-8").splitlines() if line.strip()
        )
    sources = tuple(
        source.model_copy(
            update={
                "content_sha256": file_sha256(source_path),
                "record_count": record_count,
            }
        )
        if source.file_name == file_name
        else source
        for source in manifest.sources
    )
    rebound = manifest.model_copy(update={"sources": sources})
    manifest_path.write_text(
        json.dumps(
            rebound.model_dump(mode="json"),
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _rebind_event_source(source_dir: Path) -> None:
    _rebind_source_file(source_dir, "inventory_events.jsonl")


def _write_events(source_dir: Path, events: list[dict[str, object]]) -> None:
    events_path = source_dir / "inventory_events.jsonl"
    events_path.write_text(
        "\n".join(json.dumps(event, separators=(",", ":"), sort_keys=True) for event in events)
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    _rebind_event_source(source_dir)


def _event_variant(
    template: dict[str, object],
    *,
    event_id: str,
    idempotency_key: str,
    effect: str,
    delta_qty: int,
    ingested_at: str,
    sku_id: str = "SKU-001",
    location_id: str = "DC-ARG-01",
) -> dict[str, object]:
    event = copy.deepcopy(template)
    payload = event["payload"]
    assert isinstance(payload, dict)
    payload.update(
        {
            "delta_qty": delta_qty,
            "location_id": location_id,
            "logical_event_id": f"ADJ-{effect}",
            "revision": 1,
            "sku_id": sku_id,
        }
    )
    event.update(
        {
            "event_id": event_id,
            "idempotency_key": idempotency_key,
            "ingested_at": ingested_at,
            "occurred_at": "2026-06-02T09:00:00Z",
            "subject_id": f"{sku_id}:{location_id}",
        }
    )
    return event


def _artifact_hashes(result: R2LandingResult) -> dict[str, str]:
    artifacts = result.validated_artifacts
    return {
        artifact.dataset: artifact.content_sha256
        for artifact in artifacts
        if artifact.dataset.startswith("validated.")
    }


def test_landing_emits_expected_records_dispositions_and_correction_history(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    result = land_r2_sources(
        source_dir=fixture.source_dir,
        output_dir=tmp_path / "lake",
    )

    assert (
        len(result.suppliers),
        len(result.product_locations),
        len(result.opening_inventory),
        len(result.fulfillment_outcomes),
        len(result.purchase_orders),
        len(result.receipts),
        len(result.inventory_adjustments),
        len(result.quarantined),
    ) == (2, 4, 4, 112, 20, 16, 5, 4)
    assert Counter(row.reason_code for row in result.quarantined) == Counter(
        {
            R2QuarantineReason.DUPLICATE_EVENT_ID: 1,
            R2QuarantineReason.DUPLICATE_IDEMPOTENCY_KEY: 1,
            R2QuarantineReason.UNKNOWN_PRODUCT_LOCATION: 1,
            R2QuarantineReason.EVENT_CONTRACT_INVALID: 1,
        }
    )

    cycle_history = [
        (row.revision, row.delta_qty, row.ingested_at.isoformat())
        for row in result.inventory_adjustments
        if row.logical_event_id == "ADJ-CYCLE-001"
    ]
    assert cycle_history == [
        (1, -5, "2026-06-10T12:00:00+00:00"),
        (2, -3, "2026-06-25T10:00:00+00:00"),
    ]
    assert sum(row.logical_event_id == "ADJ-PROMO-001" for row in result.inventory_adjustments) == 1
    assert result.source_manifest_sha256 == fixture.manifest_sha256
    assert {artifact.row_count for artifact in result.validated_artifacts} >= {
        2,
        4,
        5,
        16,
        20,
        112,
    }
    assert len(result.source_native_paths) == 8


def test_manifest_bound_source_and_manifest_shape_tampering_fail_closed(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    fulfillment_path = fixture.source_dir / "fulfillment_outcomes.csv"
    fulfillment_path.write_bytes(fulfillment_path.read_bytes() + b"\n")

    with pytest.raises(R2SourceIntegrityError, match="sha256 mismatch"):
        land_r2_sources(
            source_dir=fixture.source_dir,
            output_dir=tmp_path / "tampered-source-lake",
        )

    second = generate_r2_smoke_fixture(tmp_path / "second-source")
    manifest_path = second.manifest_path
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_payload["sources"].pop()
    manifest_path.write_text(
        json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(R2SourceContractError, match="source manifest must bind exactly"):
        land_r2_sources(
            source_dir=second.source_dir,
            output_dir=tmp_path / "tampered-manifest-lake",
        )


def test_rebuild_rejects_manifest_controls_that_disagree_with_observed_data(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    manifest_payload = json.loads(fixture.manifest_path.read_text(encoding="utf-8"))
    manifest_payload["expected_controls"]["supplier_rows"] = 3
    fixture.manifest_path.write_text(
        json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(R2ExpectedControlError, match="supplier_rows"):
        run_r2_rebuild(
            project_root=Path(__file__).resolve().parents[1],
            source_dir=fixture.source_dir,
            artifact_dir=tmp_path / "rebuild",
            planning_cutoff=fixture.manifest.planning_cutoff,
        )


def test_identical_redelivery_is_quarantined_but_conflicting_identity_fails(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    events_path = fixture.source_dir / "inventory_events.jsonl"
    lines = events_path.read_text(encoding="utf-8").splitlines()
    conflicting = json.loads(lines[0])
    conflicting["payload"]["delta_qty"] = -6
    lines.append(json.dumps(conflicting, separators=(",", ":"), sort_keys=True))
    events_path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    _rebind_event_source(fixture.source_dir)

    with pytest.raises(R2IdentityConflictError, match=r"event_id .* conflicting content"):
        land_r2_sources(
            source_dir=fixture.source_dir,
            output_dir=tmp_path / "lake",
        )


def test_broken_source_correction_chain_keeps_prefix_and_quarantines_suffix(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    template = json.loads(
        (fixture.source_dir / "inventory_events.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    revision_one = _event_variant(
        template,
        event_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        idempotency_key="KEY-GAP-1",
        effect="GAP",
        delta_qty=-1,
        ingested_at="2026-06-02T10:00:00Z",
    )
    revision_three = _event_variant(
        template,
        event_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        idempotency_key="KEY-GAP-3",
        effect="GAP",
        delta_qty=-3,
        ingested_at="2026-06-02T12:00:00Z",
    )
    assert isinstance(revision_three["payload"], dict)
    revision_three["payload"]["revision"] = 3
    _write_events(fixture.source_dir, [revision_three, revision_one])

    result = land_r2_sources(
        source_dir=fixture.source_dir,
        output_dir=tmp_path / "lake",
    )

    assert [(row.revision, row.delta_qty) for row in result.inventory_adjustments] == [(1, -1)]
    broken = [
        row
        for row in result.quarantined
        if row.reason_code is R2QuarantineReason.CORRECTION_CHAIN_INVALID
    ]
    assert len(broken) == 1
    assert "expected revision 2, received 3" in broken[0].detail


def test_visible_revision_waits_for_late_predecessor_instead_of_becoming_effective(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    template = json.loads(
        (fixture.source_dir / "inventory_events.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    revision_one = _event_variant(
        template,
        event_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        idempotency_key="KEY-LATE-1",
        effect="LATE-PREFIX",
        delta_qty=-1,
        ingested_at="2026-06-15T10:00:00Z",
    )
    revision_two = _event_variant(
        template,
        event_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        idempotency_key="KEY-LATE-2",
        effect="LATE-PREFIX",
        delta_qty=-2,
        ingested_at="2026-06-08T10:00:00Z",
    )
    assert isinstance(revision_two["payload"], dict)
    revision_two["payload"]["revision"] = 2
    _write_events(fixture.source_dir, [revision_two, revision_one])
    manifest_payload = json.loads(fixture.manifest_path.read_text(encoding="utf-8"))
    manifest_payload["expected_controls"]["accepted_inventory_adjustments"] = 2
    manifest_payload["expected_controls"]["quarantine_rows"] = 0
    fixture.manifest_path.write_text(
        json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )

    result = run_r2_rebuild(
        project_root=Path(__file__).resolve().parents[1],
        source_dir=fixture.source_dir,
        artifact_dir=tmp_path / "rebuild",
        planning_cutoff=datetime.fromisoformat("2026-06-10T12:00:00+00:00"),
    )

    assert result.quality_counts["effective_inventory_adjustments"] == 0
    assert result.quality_counts["superseded_inventory_adjustments"] == 0
    assert result.quality_counts["deferred_inventory_adjustments"] == 2
    assert result.quality_counts["chain_deferred_inventory_adjustments"] == 1
    inventory = pq.read_table(result.gold_artifact("gold.r2_inventory_position").path).to_pylist()
    target = next(row for row in inventory if row["sku_location_key"] == "SKU-001::DC-ARG-01")
    assert target["adjustment_qty"] == 0


def test_csv_row_with_trailing_fields_is_quarantined(tmp_path: Path) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    fulfillment_path = fixture.source_dir / "fulfillment_outcomes.csv"
    lines = fulfillment_path.read_text(encoding="utf-8").splitlines()
    lines[1] += ",unexpected"
    fulfillment_path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    _rebind_source_file(fixture.source_dir, fulfillment_path.name)

    result = land_r2_sources(
        source_dir=fixture.source_dir,
        output_dir=tmp_path / "lake",
    )

    trailing = [
        record for record in result.quarantined if record.source_file == fulfillment_path.name
    ]
    assert len(result.fulfillment_outcomes) == 111
    assert len(trailing) == 1
    assert trailing[0].reason_code is R2QuarantineReason.BATCH_CONTRACT_INVALID
    assert trailing[0].source_row == 2
    assert trailing[0].detail == "row contains fields beyond the declared CSV contract"


def test_purchase_order_supplier_must_match_product_location_primary_supplier(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    orders_path = fixture.source_dir / "purchase_orders.csv"
    original = "PO-SKU-001-DC-ARG-01-05,SKU-001,DC-ARG-01,SUP-001,55,2026-06-28T09:00:00Z"
    conflicting = original.replace("SUP-001", "SUP-002")
    orders_text = orders_path.read_text(encoding="utf-8")
    assert original in orders_text
    orders_path.write_text(
        orders_text.replace(original, conflicting, 1),
        encoding="utf-8",
        newline="\n",
    )
    _rebind_source_file(fixture.source_dir, orders_path.name)

    result = land_r2_sources(
        source_dir=fixture.source_dir,
        output_dir=tmp_path / "lake",
    )

    mismatches = [
        record
        for record in result.quarantined
        if record.reason_code is R2QuarantineReason.PRIMARY_SUPPLIER_MISMATCH
    ]
    assert len(result.purchase_orders) == 19
    assert len(result.receipts) == 16
    assert len(mismatches) == 1
    assert mismatches[0].record_reference == "PO-SKU-001-DC-ARG-01-05"


def test_unsupported_event_version_is_quarantined(tmp_path: Path) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    events_path = fixture.source_dir / "inventory_events.jsonl"
    lines = events_path.read_text(encoding="utf-8").splitlines()
    future_event = json.loads(lines[4])
    future_event["event_version"] = 2
    lines[4] = json.dumps(future_event, separators=(",", ":"), sort_keys=True)
    events_path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    _rebind_event_source(fixture.source_dir)

    result = land_r2_sources(
        source_dir=fixture.source_dir,
        output_dir=tmp_path / "lake",
    )

    unsupported = [
        record
        for record in result.quarantined
        if record.reason_code is R2QuarantineReason.UNSUPPORTED_EVENT_VERSION
    ]
    assert len(result.inventory_adjustments) == 4
    assert len(unsupported) == 1
    assert unsupported[0].record_reference == future_event["event_id"]
    assert unsupported[0].detail == "expected event version 1"


def test_event_id_collision_fails_before_a_later_event_can_be_accepted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    template = json.loads(
        (fixture.source_dir / "inventory_events.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    event_x = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    event_y = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    events = [
        _event_variant(
            template,
            event_id=event_x,
            idempotency_key="KEY-A",
            effect="S",
            delta_qty=-1,
            ingested_at="2026-06-02T10:00:00Z",
        ),
        _event_variant(
            template,
            event_id=event_x,
            idempotency_key="KEY-B",
            effect="S",
            delta_qty=-1,
            ingested_at="2026-06-02T11:00:00Z",
        ),
        _event_variant(
            template,
            event_id=event_y,
            idempotency_key="KEY-B",
            effect="T",
            delta_qty=-2,
            ingested_at="2026-06-02T12:00:00Z",
        ),
    ]
    _write_events(fixture.source_dir, events)
    created_event_ids: list[str] = []
    original_model = r2_pipeline_module.R2InventoryAdjustment

    def tracking_adjustment(**kwargs: object) -> object:
        created_event_ids.append(str(kwargs["event_id"]))
        return original_model(**kwargs)

    monkeypatch.setattr(
        r2_pipeline_module,
        "R2InventoryAdjustment",
        tracking_adjustment,
    )

    with pytest.raises(
        R2IdentityConflictError,
        match=rf"event_id {event_x} points at conflicting content",
    ):
        land_r2_sources(
            source_dir=fixture.source_dir,
            output_dir=tmp_path / "lake",
        )

    assert created_event_ids == [event_x]


def test_duplicate_idempotency_still_registers_the_redelivered_event_id(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    template = json.loads(
        (fixture.source_dir / "inventory_events.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    event_a = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    event_b = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    events = [
        _event_variant(
            template,
            event_id=event_a,
            idempotency_key="KEY-K",
            effect="X",
            delta_qty=-1,
            ingested_at="2026-06-02T10:00:00Z",
        ),
        _event_variant(
            template,
            event_id=event_b,
            idempotency_key="KEY-K",
            effect="X",
            delta_qty=-1,
            ingested_at="2026-06-02T11:00:00Z",
        ),
        _event_variant(
            template,
            event_id=event_b,
            idempotency_key="KEY-K2",
            effect="Y",
            delta_qty=-2,
            ingested_at="2026-06-02T12:00:00Z",
        ),
    ]
    _write_events(fixture.source_dir, events)

    with pytest.raises(
        R2IdentityConflictError,
        match=rf"event_id {event_b} points at conflicting content",
    ):
        land_r2_sources(
            source_dir=fixture.source_dir,
            output_dir=tmp_path / "lake",
        )


@pytest.mark.parametrize(
    ("collision_axis", "expected_message"),
    [
        ("event_id", r"event_id .* points at conflicting content"),
        ("idempotency_key", r"idempotency key .* has conflicting content"),
    ],
)
def test_relationship_quarantined_event_identities_still_block_conflicting_reuse(
    tmp_path: Path,
    collision_axis: str,
    expected_message: str,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    template = json.loads(
        (fixture.source_dir / "inventory_events.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    quarantined_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    quarantined_key = "KEY-RELATIONSHIP"
    valid_id = (
        quarantined_id if collision_axis == "event_id" else "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    )
    valid_key = quarantined_key if collision_axis == "idempotency_key" else "KEY-VALID"
    events = [
        _event_variant(
            template,
            event_id=quarantined_id,
            idempotency_key=quarantined_key,
            effect="UNKNOWN",
            delta_qty=-1,
            ingested_at="2026-06-02T10:00:00Z",
            sku_id="SKU-999",
            location_id="DC-ARG-99",
        ),
        _event_variant(
            template,
            event_id=valid_id,
            idempotency_key=valid_key,
            effect="VALID",
            delta_qty=-2,
            ingested_at="2026-06-02T11:00:00Z",
        ),
    ]
    _write_events(fixture.source_dir, events)

    with pytest.raises(R2IdentityConflictError, match=expected_message):
        land_r2_sources(
            source_dir=fixture.source_dir,
            output_dir=tmp_path / "lake",
        )


def test_landing_parses_captured_bytes_if_original_mutates_after_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    supplier_source = fixture.source_dir / "suppliers.csv"
    captured_supplier_bytes = supplier_source.read_bytes()
    original_land = r2_pipeline_module._land_source_bytes

    def land_then_mutate_original(
        source_bytes: bytes,
        *,
        source_name: str,
        dataset: str,
        content_sha256: str,
        root: Path,
    ) -> Path:
        landed = original_land(
            source_bytes,
            source_name=source_name,
            dataset=dataset,
            content_sha256=content_sha256,
            root=root,
        )
        if source_name == supplier_source.name:
            supplier_source.write_bytes(b"mutated-after-capture\n")
        return landed

    monkeypatch.setattr(
        r2_pipeline_module,
        "_land_source_bytes",
        land_then_mutate_original,
    )

    result = land_r2_sources(
        source_dir=fixture.source_dir,
        output_dir=tmp_path / "lake",
    )

    landed_supplier = next(
        path for path in result.source_native_paths if path.parent.name == "source_suppliers"
    )
    assert supplier_source.read_bytes() == b"mutated-after-capture\n"
    assert landed_supplier.read_bytes() == captured_supplier_bytes
    assert [supplier.supplier_id for supplier in result.suppliers] == ["SUP-001", "SUP-002"]


def test_validated_state_is_independent_of_event_delivery_order(tmp_path: Path) -> None:
    ordered = generate_r2_smoke_fixture(tmp_path / "ordered-source")
    permuted_dir = tmp_path / "permuted-source"
    shutil.copytree(ordered.source_dir, permuted_dir)
    events_path = permuted_dir / "inventory_events.jsonl"
    lines = events_path.read_text(encoding="utf-8").splitlines()
    events_path.write_text(
        "\n".join(reversed(lines)) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    _rebind_event_source(permuted_dir)

    ordered_result = land_r2_sources(
        source_dir=ordered.source_dir,
        output_dir=tmp_path / "ordered-lake",
    )
    permuted_result = land_r2_sources(
        source_dir=permuted_dir,
        output_dir=tmp_path / "permuted-lake",
    )

    assert ordered_result.source_manifest_sha256 != permuted_result.source_manifest_sha256
    assert ordered_result.inventory_adjustments == permuted_result.inventory_adjustments
    assert _artifact_hashes(ordered_result) == _artifact_hashes(permuted_result)
    assert Counter(row.reason_code for row in ordered_result.quarantined) == Counter(
        row.reason_code for row in permuted_result.quarantined
    )


def test_content_addressed_landing_is_idempotent_and_detects_artifact_corruption(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    lake = tmp_path / "lake"
    first = land_r2_sources(source_dir=fixture.source_dir, output_dir=lake)
    paths = [
        *first.source_native_paths,
        *(artifact.path for artifact in first.validated_artifacts),
    ]
    initial_state = {path: (path.stat().st_mtime_ns, file_sha256(path)) for path in paths}

    second = land_r2_sources(source_dir=fixture.source_dir, output_dir=lake)

    assert {path: (path.stat().st_mtime_ns, file_sha256(path)) for path in paths} == initial_state
    assert [
        (artifact.dataset, artifact.path, artifact.content_sha256)
        for artifact in second.validated_artifacts
    ] == [
        (artifact.dataset, artifact.path, artifact.content_sha256)
        for artifact in first.validated_artifacts
    ]
    assert all(
        artifact.path.stem == artifact.content_sha256 for artifact in first.validated_artifacts
    )
    assert not list(lake.rglob("*.tmp"))

    suppliers_artifact = first.artifact("validated.suppliers")
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "supplier_id": "CORRUPTED",
                    "supplier_name": "Corrupted",
                    "active": True,
                    "available_at": "2026-05-31T08:00:00Z",
                }
            ]
        ),
        suppliers_artifact.path,
    )
    with pytest.raises(ArtifactConflictError, match="content-addressed artifact is corrupt"):
        land_r2_sources(source_dir=fixture.source_dir, output_dir=lake)
