from __future__ import annotations

import json
from pathlib import Path

import pytest

from nexus.data import R2GeneratedFixture, R2LandingResult
from nexus.orchestration.r2b_checkpoint import (
    R2BValidationCheckpointRef,
    checkpoint_governed_files,
    load_r2b_validation_checkpoint,
    reference_manifest,
    write_r2b_validation_checkpoint,
)
from nexus.r2 import materialize_r2_validation_checkpoint
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import ArtifactConflictError, file_sha256

VALIDATION_CODE_SHA256 = "b" * 64


def _checkpoint(
    root: Path,
) -> tuple[R2GeneratedFixture, R2LandingResult, R2BValidationCheckpointRef]:
    fixture = generate_r2_smoke_fixture(root / "source")
    landing = materialize_r2_validation_checkpoint(
        source_dir=fixture.source_dir,
        artifact_dir=root / "rebuild",
    )
    reference = write_r2b_validation_checkpoint(
        checkpoint_root=root,
        validation_code_sha256=VALIDATION_CODE_SHA256,
        landing=landing,
    )
    return fixture, landing, reference


def _load(reference: R2BValidationCheckpointRef) -> R2LandingResult:
    manifest = reference_manifest(reference)
    return load_r2b_validation_checkpoint(
        reference,
        expected_validation_code_sha256=VALIDATION_CODE_SHA256,
        expected_source_manifest_sha256=manifest.source_manifest_sha256,
        expected_source_manifest_file_sha256=manifest.source_manifest_file_sha256,
    )


def _file_state(reference: R2BValidationCheckpointRef) -> dict[Path, tuple[str, int]]:
    return {
        path: (file_sha256(path), path.stat().st_mtime_ns)
        for path in checkpoint_governed_files(reference)
    }


def test_checkpoint_round_trip_survives_source_removal_and_landing_release(
    tmp_path: Path,
) -> None:
    fixture, landing, reference = _checkpoint(tmp_path / "checkpoint")
    expected_records = {
        field: getattr(landing, field)
        for field in (
            "product_locations",
            "suppliers",
            "opening_inventory",
            "fulfillment_outcomes",
            "purchase_orders",
            "receipts",
            "inventory_adjustments",
            "quarantined",
        )
    }
    expected_artifacts = {
        artifact.dataset: (
            artifact.content_sha256,
            artifact.schema_sha256,
            artifact.row_count,
        )
        for artifact in landing.validated_artifacts
    }

    fixture.source_dir.rename(fixture.source_dir.with_name("detached-original-source"))
    del landing

    restored = _load(reference)

    assert restored.source_manifest_sha256 == fixture.manifest_sha256
    assert restored.source_manifest_file_sha256 == fixture.manifest_file_sha256
    assert {field: getattr(restored, field) for field in expected_records} == expected_records
    assert {
        artifact.dataset: (
            artifact.content_sha256,
            artifact.schema_sha256,
            artifact.row_count,
        )
        for artifact in restored.validated_artifacts
    } == expected_artifacts
    assert all(
        path.is_relative_to(reference.checkpoint_root / "rebuild" / "lake")
        for path in restored.source_native_paths
    )


def test_exact_checkpoint_republication_preserves_bytes_and_mtimes(tmp_path: Path) -> None:
    _, landing, first = _checkpoint(tmp_path / "checkpoint")
    before = _file_state(first)

    second = write_r2b_validation_checkpoint(
        checkpoint_root=first.checkpoint_root,
        validation_code_sha256=VALIDATION_CODE_SHA256,
        landing=landing,
    )

    assert second == first
    assert _file_state(second) == before


@pytest.mark.parametrize("failure", ["corrupt", "missing"])
def test_checkpoint_fails_closed_for_unreadable_validated_parquet(
    tmp_path: Path,
    failure: str,
) -> None:
    _, landing, reference = _checkpoint(tmp_path / failure)
    target = landing.validated_artifacts[0].path
    if failure == "corrupt":
        target.write_bytes(b"not parquet")
    else:
        target.rename(target.with_suffix(".detached"))

    with pytest.raises(ArtifactConflictError, match="cannot read checkpoint Parquet"):
        _load(reference)


def test_checkpoint_identity_excludes_roots_cutoffs_and_execution_facts(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    references: list[R2BValidationCheckpointRef] = []
    for name in ("machine-a", "machine-b"):
        root = tmp_path / name
        landing = materialize_r2_validation_checkpoint(
            source_dir=fixture.source_dir,
            artifact_dir=root / "rebuild",
        )
        references.append(
            write_r2b_validation_checkpoint(
                checkpoint_root=root,
                validation_code_sha256=VALIDATION_CODE_SHA256,
                landing=landing,
            )
        )

    first, second = references
    assert first.checkpoint_root != second.checkpoint_root
    assert first.manifest_path != second.manifest_path
    assert first.content_sha256 == second.content_sha256
    assert first.schema_sha256 == second.schema_sha256
    assert first.file_sha256 == second.file_sha256
    assert first.manifest_path.read_bytes() == second.manifest_path.read_bytes()

    payload = json.loads(first.manifest_path.read_text(encoding="utf-8"))
    serialized = json.dumps(payload, sort_keys=True)
    excluded_fields = {
        "artifact_dir",
        "attempt_count",
        "checkpoint_root",
        "dagster_run_id",
        "duration_seconds",
        "manifest_path",
        "planning_cutoff",
        "source_dir",
    }

    def nested_keys(value: object) -> set[str]:
        if isinstance(value, dict):
            return set(value) | set().union(*(nested_keys(item) for item in value.values()))
        if isinstance(value, list):
            return set().union(*(nested_keys(item) for item in value))
        return set()

    assert nested_keys(payload).isdisjoint(excluded_fields)
    assert str(first.checkpoint_root) not in serialized
    assert str(second.checkpoint_root) not in serialized
