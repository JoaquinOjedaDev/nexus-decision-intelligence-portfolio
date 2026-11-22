from __future__ import annotations

import os
from pathlib import Path

import pytest

from nexus.data import R2SourceIntegrityError, R2SourceManifest, land_r2_sources
from nexus.ingestion import raw_value_multiset_sha256
from nexus.r2d import (
    R2DPackageConflictError,
    build_portable_captured_route,
    raw_value_sha256_counts,
    reconstruct_r2_source_package,
    split_lf_jsonl_values,
)
from nexus.simulation import generate_r2_smoke_fixture


def test_reconstruction_conserves_raw_values_and_copies_batch_sources_exactly(
    tmp_path: Path,
) -> None:
    direct = generate_r2_smoke_fixture(tmp_path / "direct")
    direct_jsonl = (direct.source_dir / "inventory_events.jsonl").read_bytes()
    direct_values = split_lf_jsonl_values(direct_jsonl)
    capture = build_portable_captured_route(
        reference_source_dir=direct.source_dir,
        output_dir=tmp_path / "captured",
    )
    package = capture.package
    captured_values = split_lf_jsonl_values(
        (package.source_dir / "inventory_events.jsonl").read_bytes()
    )

    assert package.inventory_record_count == len(direct_values) == 9
    assert raw_value_sha256_counts(direct_values) == raw_value_sha256_counts(captured_values)
    assert all(item.decision.disposition == "admitted" for item in capture.transcript.records)
    assert capture.producer_ack_ledger.acknowledged_record_count == 9
    assert capture.producer_ack_ledger.frozen_source_match
    assert capture.producer_ack_ledger.real_broker_ack_claim is False
    assert capture.producer_ack_ledger.raw_value_multiset_sha256 == raw_value_multiset_sha256(
        direct_values
    )
    assert capture.checkpoint.producer_ack_sha256 == capture.producer_ack_ledger.content_sha256
    assert capture.checkpoint.lineage_sha256 == capture.transcript.lineage_sha256

    direct_manifest = direct.manifest
    captured_manifest = package.manifest
    assert captured_manifest.expected_controls == direct_manifest.expected_controls
    assert captured_manifest.model_dump(exclude={"sources"}) == direct_manifest.model_dump(
        exclude={"sources"}
    )
    direct_sources = {source.dataset: source for source in direct_manifest.sources}
    captured_sources = {source.dataset: source for source in captured_manifest.sources}
    assert set(captured_sources) == set(direct_sources)
    for dataset, source in direct_sources.items():
        captured_source = captured_sources[dataset]
        if dataset == "source.inventory_adjustments":
            assert captured_source.content_sha256 != source.content_sha256
            assert captured_source.record_count == source.record_count
        else:
            assert captured_source == source
            assert (package.source_dir / source.file_name).read_bytes() == (
                direct.source_dir / source.file_name
            ).read_bytes()


def test_reconstructed_package_is_immutable_and_manifest_tamper_fails_closed(
    tmp_path: Path,
) -> None:
    direct = generate_r2_smoke_fixture(tmp_path / "direct")
    captured_dir = tmp_path / "captured"
    capture = build_portable_captured_route(
        reference_source_dir=direct.source_dir,
        output_dir=captured_dir,
    )
    package = capture.package
    captured_jsonl = (captured_dir / "inventory_events.jsonl").read_bytes()

    repeated = reconstruct_r2_source_package(
        reference_source_dir=direct.source_dir,
        output_dir=captured_dir,
        inventory_jsonl=captured_jsonl,
    )
    assert repeated.manifest_file_sha256 == package.manifest_file_sha256

    event_path = captured_dir / "inventory_events.jsonl"
    event_path.write_bytes(event_path.read_bytes() + b"{}\n")
    with pytest.raises(R2SourceIntegrityError, match="sha256 mismatch"):
        land_r2_sources(source_dir=captured_dir, output_dir=tmp_path / "tampered-lake")
    with pytest.raises(R2DPackageConflictError, match="refusing to overwrite"):
        reconstruct_r2_source_package(
            reference_source_dir=direct.source_dir,
            output_dir=captured_dir,
            inventory_jsonl=captured_jsonl,
        )


def test_reconstruction_updates_only_inventory_binding_in_strict_manifest(
    tmp_path: Path,
) -> None:
    direct = generate_r2_smoke_fixture(tmp_path / "direct")
    capture = build_portable_captured_route(
        reference_source_dir=direct.source_dir,
        output_dir=tmp_path / "captured",
    )
    package = capture.package

    strict = R2SourceManifest.model_validate_json(package.manifest_path.read_bytes())
    changed = [
        (left, right)
        for left, right in zip(direct.manifest.sources, strict.sources, strict=True)
        if left != right
    ]
    assert len(changed) == 1
    assert changed[0][0].dataset == changed[0][1].dataset == "source.inventory_adjustments"


def test_capture_and_reconstruction_are_independent_of_poll_interleaving(
    tmp_path: Path,
) -> None:
    direct = generate_r2_smoke_fixture(tmp_path / "direct")
    record_count = len(
        split_lf_jsonl_values((direct.source_dir / "inventory_events.jsonl").read_bytes())
    )
    ordered = build_portable_captured_route(
        reference_source_dir=direct.source_dir,
        output_dir=tmp_path / "captured-ordered",
    )
    permuted = build_portable_captured_route(
        reference_source_dir=direct.source_dir,
        output_dir=tmp_path / "captured-permuted",
        poll_order=tuple(reversed(range(record_count))),
    )

    assert ordered.transcript == permuted.transcript
    assert ordered.checkpoint == permuted.checkpoint
    assert ordered.package.manifest_file_sha256 == permuted.package.manifest_file_sha256
    assert (ordered.package.source_dir / "inventory_events.jsonl").read_bytes() == (
        permuted.package.source_dir / "inventory_events.jsonl"
    ).read_bytes()


def test_reconstruction_rejects_a_symlinked_output_ancestor(tmp_path: Path) -> None:
    direct = generate_r2_smoke_fixture(tmp_path / "direct")
    real_root = tmp_path / "real-root"
    real_root.mkdir()
    linked_root = tmp_path / "linked-root"
    try:
        os.symlink(real_root, linked_root, target_is_directory=True)
    except (OSError, NotImplementedError) as error:
        pytest.skip(f"directory symlinks are unavailable: {error}")

    with pytest.raises(R2DPackageConflictError, match=r"link|reparse"):
        build_portable_captured_route(
            reference_source_dir=direct.source_dir,
            output_dir=linked_root / "captured",
        )
    assert list(real_root.iterdir()) == []
