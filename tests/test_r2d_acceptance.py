from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path
from typing import Literal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nexus.ingestion import (
    R2D_A1_PASS_CHECK_NAMES,
    R2DCaptureTranscript,
    R2DConfig,
    R2DResourceEvidence,
    R2DVerificationCheck,
    R2DVerificationReceipt,
    R2DVerificationRuntime,
    build_capture_checkpoint,
    capture_transcript,
    load_r2d_config,
    r2d_evidence_bindings_sha256,
)
from nexus.ingestion.r2d_resources import (
    ARTIFACT_METHOD,
    INTEGRITY_METHOD,
    MIB,
    NATIVE_THREAD_METHOD,
    RSS_METHOD,
    SHUTDOWN_METHOD,
    WALL_TIME_METHOD,
    R2DResourceCheck,
)
from nexus.modeling.r3_artifacts import canonical_json_bytes, canonical_json_sha256
from nexus.r2d import (
    R2DA1RunResult,
    R2DA1VerificationResult,
    R2DPackageConflictError,
    _normalized_payload,
    _resource_evidence_from_payload,
    _verified_portable_recovery,
    _verified_r2a_summary_payload,
    compare_r2d_a1_receipt_files,
    verify_r2d_a1,
)
from nexus.storage import (
    canonical_rows_sha256,
    canonical_schema_sha256,
    file_sha256,
)

pytestmark = [pytest.mark.r2d, pytest.mark.r2d_acceptance]

_BINDING_FIELDS = (
    "source_manifest_sha256",
    "config_sha256",
    "normalized_business_sha256",
    "transcript_sha256",
    "checkpoint_sha256",
    "producer_ack_sha256",
    "lineage_sha256",
    "evidence_checkpoint_sha256",
    "code_sha256",
    "artifact_set_sha256",
    "direct_provenance_sha256",
    "captured_provenance_sha256",
    "resource_evidence_sha256",
)

_ACCEPTANCE_ARTIFACT_ROOT_ENV = "R2D_A1_ACCEPTANCE_ARTIFACT_ROOT"


@pytest.fixture(scope="module")
def verified(tmp_path_factory: pytest.TempPathFactory) -> R2DA1VerificationResult:
    configured_artifact_root = os.environ.get(_ACCEPTANCE_ARTIFACT_ROOT_ENV)
    artifact_root = (
        Path(configured_artifact_root)
        if configured_artifact_root
        else tmp_path_factory.mktemp("r2d-a1-verification")
    )
    previous_path = os.environ.get("PATH", "")
    os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + previous_path
    try:
        result = verify_r2d_a1(artifact_root)
        if os.environ.get("GITHUB_ACTIONS") == "true":
            assert result.receipt.runtime.github_runner_os_attested
        return result
    finally:
        os.environ["PATH"] = previous_path


def _write_receipt(path: Path, receipt: R2DVerificationReceipt) -> None:
    path.write_bytes(canonical_json_bytes(receipt.model_dump(mode="json")) + b"\n")


def _write_bundle(path: Path, payload: dict[str, object]) -> None:
    path.write_bytes(canonical_json_bytes(payload) + b"\n")


def _rebound_receipt(
    receipt: R2DVerificationReceipt,
    **updates: str,
) -> R2DVerificationReceipt:
    payload = receipt.model_dump(mode="python")
    payload.update(updates)
    evidence_checks = {
        "normalized_business_sha256": (
            "two_fresh_replay",
            "direct_captured_business_parity",
        ),
        "config_sha256": ("config_and_schema_identity",),
        "transcript_sha256": (
            "deterministic_two_partition_transcript",
            "transport_admission_and_physical_identity",
        ),
        "checkpoint_sha256": ("complete_half_open_checkpoint",),
        "producer_ack_sha256": ("raw_value_conservation",),
        "evidence_checkpoint_sha256": (
            "manifest_bound_source_reconstruction",
            "public_r2a_semantic_authority",
            "point_in_time_correction_semantics",
            "route_provenance_integrity",
        ),
        "resource_evidence_sha256": ("bounded_portable_resource_envelope",),
        "artifact_set_sha256": ("immutable_artifacts",),
    }
    checks = list(receipt.checks)
    for field, check_names in evidence_checks.items():
        if field not in updates:
            continue
        for check_name in check_names:
            index = R2D_A1_PASS_CHECK_NAMES.index(check_name)
            checks[index] = checks[index].model_copy(update={"evidence_sha256": updates[field]})
    payload["checks"] = tuple(checks)
    payload["evidence_bindings_sha256"] = r2d_evidence_bindings_sha256(
        **{name: payload[name] for name in _BINDING_FIELDS}
    )
    return R2DVerificationReceipt.model_validate(payload)


def _replace_bundle_publication(
    package_root: Path,
    bundle: dict[str, object],
    *,
    kind: str,
    payload: object,
) -> str:
    content_sha256 = canonical_json_sha256(payload)
    publications = bundle["publications"]
    assert isinstance(publications, list)
    publication = next(item for item in publications if item["kind"] == kind)
    relative = Path(*publication["path"].split("/"))
    replacement = relative.with_name(f"{content_sha256}.json")
    path = package_root / replacement
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(payload) + b"\n")
    publication.update(
        {
            "path": replacement.as_posix(),
            "content_sha256": content_sha256,
            "file_sha256": file_sha256(path),
        }
    )
    return content_sha256


def _bind_bundle_to_receipt(
    bundle: dict[str, object],
    receipt: R2DVerificationReceipt,
) -> None:
    bundle["receipt_binding"] = {
        **{name: getattr(receipt, name) for name in _BINDING_FIELDS},
        "evidence_bindings_sha256": receipt.evidence_bindings_sha256,
    }


def _self_rebind_parquet_without_r2a_summary(
    receipt_path: Path,
    *,
    stage: Literal["validated", "gold"],
) -> bytes:
    """Rebind every mutable layer around one changed Parquet, but not the R2A PASS summary."""

    bundle_path = receipt_path.with_name("r2d-a1-portable-bundle.json")
    bundle = json.loads(bundle_path.read_bytes())
    artifact_index = bundle["artifact_index"]
    entries = artifact_index["entries"]
    entry = next(
        item
        for item in entries
        if item["kind"] == "parquet"
        and item["route"] == "direct"
        and item["stage"] == stage
        and item["row_count"] > 0
    )
    route = entry["route"]
    cutoff = entry["cutoff"]
    dataset = entry["dataset"]
    run_root = receipt_path.parent / Path(*bundle["run_root"].split("/"))

    summary_entry = next(
        item
        for item in entries
        if item["kind"] == "r2a_summary" and item["route"] == route and item["cutoff"] == cutoff
    )
    summary_path = run_root / Path(*summary_entry["path"].split("/"))
    immutable_summary = summary_path.read_bytes()
    assert json.loads(immutable_summary)["status"] == "PASS"

    original_relative = Path(*entry["path"].split("/"))
    original_path = run_root / original_relative
    table = pq.read_table(original_path)
    rebound_table = pa.concat_tables((table, table.slice(0, 1)))
    rebound_content_sha256 = canonical_rows_sha256(rebound_table.to_pylist())
    rebound_schema_sha256 = canonical_schema_sha256(rebound_table.schema)
    rebound_relative = original_relative.with_name(f"{rebound_content_sha256}.parquet")
    rebound_path = run_root / rebound_relative
    pq.write_table(rebound_table, rebound_path)
    rebound_file_sha256 = file_sha256(rebound_path)
    rebound_values = {
        "row_count": rebound_table.num_rows,
        "content_sha256": rebound_content_sha256,
        "schema_sha256": rebound_schema_sha256,
        "file_sha256": rebound_file_sha256,
    }
    entry.update({"path": rebound_relative.as_posix(), **rebound_values})

    provenance_field = f"{route}_provenance"
    route_provenance = bundle[provenance_field]
    provenance_collection = "validated_artifacts" if stage == "validated" else "gold_artifacts"
    provenance_artifact = next(
        item
        for item in route_provenance[provenance_collection]
        if item["cutoff"] == cutoff and item["stage"] == stage and item["dataset"] == dataset
    )
    provenance_artifact.update(rebound_values)
    provenance_sha256 = canonical_json_sha256(route_provenance)

    provenance_manifest = next(
        item
        for item in entries
        if item["kind"] == "r2d_manifest" and item["dataset"] == f"r2d.a1.{route}_provenance"
    )
    manifest_relative = Path(*provenance_manifest["path"].split("/")).with_name(
        f"{provenance_sha256}.json"
    )
    manifest_path = run_root / manifest_relative
    manifest_path.write_bytes(canonical_json_bytes(route_provenance) + b"\n")
    provenance_manifest.update(
        {
            "path": manifest_relative.as_posix(),
            "content_sha256": provenance_sha256,
            "file_sha256": file_sha256(manifest_path),
        }
    )

    entries.sort(key=lambda item: (str(item["path"]), str(item["kind"])))
    artifact_index_payload = {
        name: value for name, value in artifact_index.items() if name != "artifact_set_sha256"
    }
    artifact_set_sha256 = canonical_json_sha256(artifact_index_payload)
    artifact_index["artifact_set_sha256"] = artifact_set_sha256
    _replace_bundle_publication(
        receipt_path.parent,
        bundle,
        kind="artifact_index",
        payload=artifact_index,
    )

    evidence_checkpoint = bundle["evidence_checkpoint"]
    evidence_checkpoint["artifact_set_sha256"] = artifact_set_sha256
    evidence_checkpoint["content_sha256"] = canonical_json_sha256(
        {name: value for name, value in evidence_checkpoint.items() if name != "content_sha256"}
    )
    evidence_checkpoint_sha256 = _replace_bundle_publication(
        receipt_path.parent,
        bundle,
        kind="evidence_checkpoint",
        payload=evidence_checkpoint,
    )
    receipt = _rebound_receipt(
        R2DVerificationReceipt.model_validate_json(receipt_path.read_bytes()),
        artifact_set_sha256=artifact_set_sha256,
        evidence_checkpoint_sha256=evidence_checkpoint_sha256,
        **{f"{route}_provenance_sha256": provenance_sha256},
    )
    _bind_bundle_to_receipt(bundle, receipt)
    _write_bundle(bundle_path, bundle)
    _write_receipt(receipt_path, receipt)

    assert summary_path.read_bytes() == immutable_summary
    return immutable_summary


def _finalize_self_rebound_bundle(
    receipt_path: Path,
    bundle: dict[str, object],
    *,
    receipt_updates: dict[str, str],
) -> None:
    artifact_index = bundle["artifact_index"]
    entries = artifact_index["entries"]
    entries.sort(key=lambda item: (str(item["path"]), str(item["kind"])))
    artifact_index_payload = {
        name: value for name, value in artifact_index.items() if name != "artifact_set_sha256"
    }
    artifact_set_sha256 = canonical_json_sha256(artifact_index_payload)
    artifact_index["artifact_set_sha256"] = artifact_set_sha256
    _replace_bundle_publication(
        receipt_path.parent,
        bundle,
        kind="artifact_index",
        payload=artifact_index,
    )

    evidence_checkpoint = bundle["evidence_checkpoint"]
    evidence_checkpoint["artifact_set_sha256"] = artifact_set_sha256
    evidence_checkpoint["content_sha256"] = canonical_json_sha256(
        {name: value for name, value in evidence_checkpoint.items() if name != "content_sha256"}
    )
    evidence_checkpoint_sha256 = _replace_bundle_publication(
        receipt_path.parent,
        bundle,
        kind="evidence_checkpoint",
        payload=evidence_checkpoint,
    )
    receipt = _rebound_receipt(
        R2DVerificationReceipt.model_validate_json(receipt_path.read_bytes()),
        **{
            **receipt_updates,
            "artifact_set_sha256": artifact_set_sha256,
            "evidence_checkpoint_sha256": evidence_checkpoint_sha256,
        },
    )
    _bind_bundle_to_receipt(bundle, receipt)
    _write_bundle(receipt_path.with_name("r2d-a1-portable-bundle.json"), bundle)
    _write_receipt(receipt_path, receipt)


def _rebind_indexed_manifest(
    bundle: dict[str, object],
    run_root: Path,
    *,
    dataset: str,
    payload: object,
) -> str:
    content_sha256 = canonical_json_sha256(payload)
    manifest = next(
        item
        for item in bundle["artifact_index"]["entries"]
        if item["kind"] == "r2d_manifest" and item["dataset"] == dataset
    )
    relative = Path(*manifest["path"].split("/")).with_name(f"{content_sha256}.json")
    path = run_root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(payload) + b"\n")
    manifest.update(
        {
            "path": relative.as_posix(),
            "content_sha256": content_sha256,
            "file_sha256": file_sha256(path),
        }
    )
    return content_sha256


def _self_rebind_capture_timestamp(receipt_path: Path) -> tuple[str, str]:
    """Rebind every mutable descendant after changing one broker timestamp."""

    bundle_path = receipt_path.with_name("r2d-a1-portable-bundle.json")
    bundle = json.loads(bundle_path.read_bytes())
    run_root = receipt_path.parent / Path(*bundle["run_root"].split("/"))
    config = R2DConfig.model_validate(bundle["config"])
    original_transcript = R2DCaptureTranscript.model_validate_json(
        canonical_json_bytes(bundle["transcript"])
    )
    original_checkpoint = bundle["capture_checkpoint"]

    physical_records = [item.record for item in original_transcript.records]
    target_index = next(
        index
        for index, record in enumerate(physical_records)
        if record.broker_timestamp_ms is not None
    )
    target = physical_records[target_index]
    assert target.broker_timestamp_ms is not None
    physical_records[target_index] = target.model_copy(
        update={"broker_timestamp_ms": target.broker_timestamp_ms + 1}
    )
    transcript = capture_transcript(physical_records, config)
    assert transcript.content_sha256 != original_transcript.content_sha256
    assert transcript.lineage_sha256 == original_transcript.lineage_sha256

    partition_checkpoints = original_checkpoint["partitions"]
    checkpoint = build_capture_checkpoint(
        config=config,
        transcript=transcript,
        starts={item["partition"]: item["start_offset"] for item in partition_checkpoints},
        ends={item["partition"]: item["end_offset_exclusive"] for item in partition_checkpoints},
        next_offsets={item["partition"]: item["next_offset"] for item in partition_checkpoints},
    )
    assert checkpoint.producer_ack_sha256 == bundle["producer_ack_ledger"]["content_sha256"]
    bundle["transcript"] = transcript.model_dump(mode="json")
    bundle["capture_checkpoint"] = checkpoint.model_dump(mode="json")
    _rebind_indexed_manifest(
        bundle,
        run_root,
        dataset="r2d.a1.capture_transcript",
        payload=bundle["transcript"],
    )
    _rebind_indexed_manifest(
        bundle,
        run_root,
        dataset="r2d.a1.capture_checkpoint",
        payload=bundle["capture_checkpoint"],
    )

    captured_provenance = bundle["captured_provenance"]
    captured_provenance["checkpoint_sha256"] = checkpoint.content_sha256
    captured_provenance_sha256 = _rebind_indexed_manifest(
        bundle,
        run_root,
        dataset="r2d.a1.captured_provenance",
        payload=captured_provenance,
    )
    portable_recovery = bundle["portable_recovery"]
    portable_recovery["checkpoint_sha256"] = checkpoint.content_sha256
    _rebind_indexed_manifest(
        bundle,
        run_root,
        dataset="r2d.a1.portable_recovery",
        payload=portable_recovery,
    )

    evidence_checkpoint = bundle["evidence_checkpoint"]
    evidence_checkpoint.update(
        {
            "transcript_sha256": transcript.content_sha256,
            "lineage_sha256": transcript.lineage_sha256,
            "capture_checkpoint_sha256": checkpoint.content_sha256,
        }
    )
    _finalize_self_rebound_bundle(
        receipt_path,
        bundle,
        receipt_updates={
            "transcript_sha256": transcript.content_sha256,
            "checkpoint_sha256": checkpoint.content_sha256,
            "lineage_sha256": transcript.lineage_sha256,
            "captured_provenance_sha256": captured_provenance_sha256,
        },
    )
    return transcript.content_sha256, checkpoint.content_sha256


def _self_rebind_direct_validated_and_summary(
    receipt_path: Path,
) -> tuple[str, str]:
    """Rebind a changed direct validated Parquet through its R2A PASS summary."""

    bundle_path = receipt_path.with_name("r2d-a1-portable-bundle.json")
    bundle = json.loads(bundle_path.read_bytes())
    entries = bundle["artifact_index"]["entries"]
    run_root = receipt_path.parent / Path(*bundle["run_root"].split("/"))
    entry = next(
        item
        for item in entries
        if item["kind"] == "parquet"
        and item["route"] == "direct"
        and item["stage"] == "validated"
        and item["dataset"] == "validated.inventory_adjustments"
        and item["row_count"] > 0
    )
    cutoff = entry["cutoff"]
    dataset = entry["dataset"]
    original_relative = Path(*entry["path"].split("/"))
    table = pq.read_table(run_root / original_relative)
    rebound_table = pa.concat_tables((table, table.slice(0, 1)))
    content_sha256 = canonical_rows_sha256(rebound_table.to_pylist())
    assert content_sha256 != entry["content_sha256"]
    schema_sha256 = canonical_schema_sha256(rebound_table.schema)
    rebound_relative = original_relative.with_name(f"{content_sha256}.parquet")
    rebound_path = run_root / rebound_relative
    pq.write_table(rebound_table, rebound_path)
    rebound_values = {
        "row_count": rebound_table.num_rows,
        "content_sha256": content_sha256,
        "schema_sha256": schema_sha256,
        "file_sha256": file_sha256(rebound_path),
    }
    entry.update({"path": rebound_relative.as_posix(), **rebound_values})

    direct_provenance = bundle["direct_provenance"]
    provenance_artifact = next(
        item
        for item in direct_provenance["validated_artifacts"]
        if item["cutoff"] == cutoff and item["stage"] == "validated" and item["dataset"] == dataset
    )
    provenance_artifact.update(rebound_values)

    summary_entry = next(
        item
        for item in entries
        if item["kind"] == "r2a_summary" and item["route"] == "direct" and item["cutoff"] == cutoff
    )
    summary_path = run_root / Path(*summary_entry["path"].split("/"))
    summary = json.loads(summary_path.read_bytes())
    summary_artifact = next(item for item in summary["validated"] if item["dataset"] == dataset)
    summary_artifact.update(
        {
            "row_count": rebound_values["row_count"],
            "content_sha256": content_sha256,
            "schema_sha256": schema_sha256,
        }
    )
    semantic_payload = dict(summary)
    semantic_payload.pop("semantic_manifest_sha256")
    semantic_payload.pop("status")
    semantic_dbt = dict(summary["dbt"])
    semantic_dbt.pop("detail")
    semantic_payload["dbt"] = semantic_dbt
    semantic_sha256 = canonical_rows_sha256([{"manifest": semantic_payload}])
    assert semantic_sha256 != summary_entry["semantic_manifest_sha256"]
    summary["semantic_manifest_sha256"] = semantic_sha256
    summary_relative = Path(*summary_entry["path"].split("/")).with_name(f"{semantic_sha256}.json")
    summary_path = run_root / summary_relative
    summary_path.write_bytes(canonical_json_bytes(summary) + b"\n")
    summary_file_sha256 = file_sha256(summary_path)
    summary_entry.update(
        {
            "path": summary_relative.as_posix(),
            "semantic_manifest_sha256": semantic_sha256,
            "file_sha256": summary_file_sha256,
        }
    )
    provenance_summary = next(
        item for item in direct_provenance["summaries"] if item["cutoff"] == cutoff
    )
    provenance_summary.update(
        {
            "semantic_manifest_sha256": semantic_sha256,
            "file_sha256": summary_file_sha256,
        }
    )
    direct_provenance["semantic_manifests"] = [
        [item["cutoff"], item["semantic_manifest_sha256"]]
        for item in direct_provenance["summaries"]
    ]
    direct_provenance["summary_file_sha256"] = [
        [item["cutoff"], item["file_sha256"]] for item in direct_provenance["summaries"]
    ]
    direct_provenance_sha256 = _rebind_indexed_manifest(
        bundle,
        run_root,
        dataset="r2d.a1.direct_provenance",
        payload=direct_provenance,
    )
    _finalize_self_rebound_bundle(
        receipt_path,
        bundle,
        receipt_updates={"direct_provenance_sha256": direct_provenance_sha256},
    )
    return content_sha256, semantic_sha256


def _self_rebind_embedded_payloads(
    receipt_path: Path,
    updates: dict[str, object],
) -> None:
    dataset_by_field = {
        "normalized_business": "r2d.a1.normalized_business_outcome",
        "correction_semantics": "r2d.a1.correction_semantics",
        "portable_recovery": "r2d.a1.portable_recovery",
    }
    if not updates or not set(updates).issubset(dataset_by_field):
        raise AssertionError("test helper received an unsupported embedded payload")
    bundle_path = receipt_path.with_name("r2d-a1-portable-bundle.json")
    bundle = json.loads(bundle_path.read_bytes())
    artifact_index = bundle["artifact_index"]
    entries = artifact_index["entries"]
    run_root = receipt_path.parent / Path(*bundle["run_root"].split("/"))
    receipt_updates: dict[str, str] = {}
    for field, payload in updates.items():
        bundle[field] = payload
        content_sha256 = canonical_json_sha256(payload)
        manifest = next(
            item
            for item in entries
            if item["kind"] == "r2d_manifest" and item["dataset"] == dataset_by_field[field]
        )
        relative = Path(*manifest["path"].split("/")).with_name(f"{content_sha256}.json")
        path = run_root / relative
        path.write_bytes(canonical_json_bytes(payload) + b"\n")
        manifest.update(
            {
                "path": relative.as_posix(),
                "content_sha256": content_sha256,
                "file_sha256": file_sha256(path),
            }
        )
        if field == "normalized_business":
            receipt_updates["normalized_business_sha256"] = content_sha256
    _finalize_self_rebound_bundle(
        receipt_path,
        bundle,
        receipt_updates=receipt_updates,
    )


def _self_rebind_expanded_budget_config(receipt_path: Path) -> None:
    bundle_path = receipt_path.with_name("r2d-a1-portable-bundle.json")
    bundle = json.loads(bundle_path.read_bytes())
    config_payload = bundle["config"]
    resource_contract = config_payload["resource_contract"]
    resource_contract["wall_time_seconds"] += 3600.0
    bundle["config"] = config_payload
    config_sha256 = canonical_json_sha256(config_payload)
    bundle["artifact_index"]["config_sha256"] = config_sha256

    resource = bundle["resource_evidence"]
    resource["budget"] = resource_contract
    wall_check = next(item for item in resource["checks"] if item["name"] == "wall_time_seconds")
    wall_check["limit"] = resource_contract["wall_time_seconds"]
    resource_sha256 = _replace_bundle_publication(
        receipt_path.parent,
        bundle,
        kind="resource_evidence",
        payload=resource,
    )
    _finalize_self_rebound_bundle(
        receipt_path,
        bundle,
        receipt_updates={
            "config_sha256": config_sha256,
            "resource_evidence_sha256": resource_sha256,
        },
    )


def _self_rebind_reordered_captured_inventory(receipt_path: Path) -> None:
    """Rebind every mutable descendant of a same-multiset captured reorder."""

    bundle_path = receipt_path.with_name("r2d-a1-portable-bundle.json")
    bundle = json.loads(bundle_path.read_bytes())
    artifact_index = bundle["artifact_index"]
    entries = artifact_index["entries"]
    run_root = receipt_path.parent / Path(*bundle["run_root"].split("/"))

    captured_inventory_entry = next(
        item
        for item in entries
        if item["kind"] == "source_file"
        and item["route"] == "captured"
        and item["dataset"] == "source.inventory_adjustments"
    )
    captured_inventory_path = run_root / Path(*captured_inventory_entry["path"].split("/"))
    original_lines = captured_inventory_path.read_bytes().splitlines()
    assert len(original_lines) > 1
    reordered_inventory = b"\n".join((*original_lines[1:], original_lines[0])) + b"\n"
    captured_inventory_path.write_bytes(reordered_inventory)
    reordered_inventory_sha256 = file_sha256(captured_inventory_path)
    captured_inventory_entry.update(
        {
            "content_sha256": reordered_inventory_sha256,
            "file_sha256": reordered_inventory_sha256,
            "row_count": len(original_lines),
        }
    )

    captured_manifest_entry = next(
        item
        for item in entries
        if item["kind"] == "source_manifest" and item["route"] == "captured"
    )
    captured_manifest_path = run_root / Path(*captured_manifest_entry["path"].split("/"))
    captured_manifest = json.loads(captured_manifest_path.read_bytes())
    inventory_source = next(
        source
        for source in captured_manifest["sources"]
        if source["dataset"] == "source.inventory_adjustments"
    )
    inventory_source.update(
        {
            "content_sha256": reordered_inventory_sha256,
            "record_count": len(original_lines),
        }
    )
    captured_manifest_path.write_bytes(
        (
            json.dumps(
                captured_manifest,
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
            )
            + "\n"
        ).encode("utf-8")
    )
    captured_manifest_sha256 = canonical_json_sha256(captured_manifest)
    captured_manifest_file_sha256 = file_sha256(captured_manifest_path)
    captured_manifest_entry.update(
        {
            "content_sha256": captured_manifest_sha256,
            "file_sha256": captured_manifest_file_sha256,
        }
    )

    captured_provenance = bundle["captured_provenance"]
    captured_provenance.update(
        {
            "source_manifest_sha256": captured_manifest_sha256,
            "source_manifest_file_sha256": captured_manifest_file_sha256,
        }
    )
    provenance_inventory = next(
        item
        for item in captured_provenance["source_files"]
        if item["dataset"] == "source.inventory_adjustments"
    )
    provenance_inventory.update(
        {
            "file_sha256": reordered_inventory_sha256,
            "record_count": len(original_lines),
        }
    )

    semantic_manifests: list[list[str]] = []
    summary_file_sha256: list[list[str]] = []
    for summary_entry in (
        item for item in entries if item["kind"] == "r2a_summary" and item["route"] == "captured"
    ):
        cutoff = summary_entry["cutoff"]
        old_semantic_sha256 = summary_entry["semantic_manifest_sha256"]
        summary_path = run_root / Path(*summary_entry["path"].split("/"))
        summary = json.loads(summary_path.read_bytes())
        summary.update(
            {
                "source_manifest_sha256": captured_manifest_sha256,
                "source_manifest_file_sha256": captured_manifest_file_sha256,
            }
        )
        semantic_payload = dict(summary)
        semantic_payload.pop("semantic_manifest_sha256")
        semantic_payload.pop("status")
        semantic_dbt = dict(summary["dbt"])
        semantic_dbt.pop("detail")
        semantic_payload["dbt"] = semantic_dbt
        semantic_sha256 = canonical_rows_sha256([{"manifest": semantic_payload}])
        summary["semantic_manifest_sha256"] = semantic_sha256
        summary_relative = Path(*summary_entry["path"].split("/")).with_name(
            f"{semantic_sha256}.json"
        )
        summary_path = run_root / summary_relative
        summary_path.write_bytes(canonical_json_bytes(summary) + b"\n")
        summary_file = file_sha256(summary_path)
        summary_entry.update(
            {
                "path": summary_relative.as_posix(),
                "semantic_manifest_sha256": semantic_sha256,
                "file_sha256": summary_file,
            }
        )
        summary_provenance = next(
            item for item in captured_provenance["summaries"] if item["cutoff"] == cutoff
        )
        summary_provenance.update(
            {
                "semantic_manifest_sha256": semantic_sha256,
                "file_sha256": summary_file,
            }
        )
        semantic_manifests.append([cutoff, semantic_sha256])
        summary_file_sha256.append([cutoff, summary_file])

        cutoff_name = "early" if "/early/" in summary_entry["path"] else "final"
        recovery_summary_path = (
            run_root
            / "portable-recovery"
            / "derived"
            / cutoff_name
            / "manifests"
            / f"{old_semantic_sha256}.json"
        )
        recovery_summary = json.loads(recovery_summary_path.read_bytes())
        recovery_summary.update(
            {
                "source_manifest_sha256": captured_manifest_sha256,
                "source_manifest_file_sha256": captured_manifest_file_sha256,
            }
        )
        recovery_semantic_payload = dict(recovery_summary)
        recovery_semantic_payload.pop("semantic_manifest_sha256")
        recovery_semantic_payload.pop("status")
        recovery_semantic_dbt = dict(recovery_summary["dbt"])
        recovery_semantic_dbt.pop("detail")
        recovery_semantic_payload["dbt"] = recovery_semantic_dbt
        assert canonical_rows_sha256([{"manifest": recovery_semantic_payload}]) == semantic_sha256
        recovery_summary["semantic_manifest_sha256"] = semantic_sha256
        recovery_summary_path.with_name(f"{semantic_sha256}.json").write_bytes(
            canonical_json_bytes(recovery_summary) + b"\n"
        )

    captured_provenance["semantic_manifests"] = semantic_manifests
    captured_provenance["summary_file_sha256"] = summary_file_sha256
    captured_provenance_sha256 = canonical_json_sha256(captured_provenance)
    captured_provenance_entry = next(
        item
        for item in entries
        if item["kind"] == "r2d_manifest" and item["dataset"] == "r2d.a1.captured_provenance"
    )
    captured_provenance_relative = Path(*captured_provenance_entry["path"].split("/")).with_name(
        f"{captured_provenance_sha256}.json"
    )
    captured_provenance_path = run_root / captured_provenance_relative
    captured_provenance_path.write_bytes(canonical_json_bytes(captured_provenance) + b"\n")
    captured_provenance_entry.update(
        {
            "path": captured_provenance_relative.as_posix(),
            "content_sha256": captured_provenance_sha256,
            "file_sha256": file_sha256(captured_provenance_path),
        }
    )

    recovery_source = run_root / "portable-recovery" / "reconstructed-source"
    (recovery_source / "inventory_events.jsonl").write_bytes(reordered_inventory)
    (recovery_source / "source-manifest.json").write_bytes(captured_manifest_path.read_bytes())
    recovery = bundle["portable_recovery"]
    recovery.update(
        {
            "reconstructed_jsonl_sha256": reordered_inventory_sha256,
            "semantic_manifests": semantic_manifests,
        }
    )
    recovery_sha256 = canonical_json_sha256(recovery)
    recovery_entry = next(
        item
        for item in entries
        if item["kind"] == "r2d_manifest" and item["dataset"] == "r2d.a1.portable_recovery"
    )
    recovery_relative = Path(*recovery_entry["path"].split("/")).with_name(
        f"{recovery_sha256}.json"
    )
    recovery_path = run_root / recovery_relative
    recovery_path.write_bytes(canonical_json_bytes(recovery) + b"\n")
    recovery_entry.update(
        {
            "path": recovery_relative.as_posix(),
            "content_sha256": recovery_sha256,
            "file_sha256": file_sha256(recovery_path),
        }
    )

    bundle["evidence_checkpoint"]["reconstructed_jsonl_sha256"] = reordered_inventory_sha256
    _finalize_self_rebound_bundle(
        receipt_path,
        bundle,
        receipt_updates={"captured_provenance_sha256": captured_provenance_sha256},
    )


def _passing_resource_payload() -> tuple[dict[str, object], R2DConfig]:
    config = load_r2d_config()
    budget = config.resource_contract
    checks = (
        R2DResourceCheck("measurement_integrity", True, 0, 0, "==", INTEGRITY_METHOD),
        R2DResourceCheck(
            "wall_time_seconds", True, 1.0, budget.wall_time_seconds, "<=", WALL_TIME_METHOD
        ),
        R2DResourceCheck(
            "peak_process_tree_rss_bytes",
            True,
            MIB,
            budget.peak_process_tree_rss_mib * MIB,
            "<=",
            RSS_METHOD,
        ),
        R2DResourceCheck(
            "peak_process_tree_native_threads",
            True,
            1,
            budget.maximum_process_tree_threads,
            "<=",
            NATIVE_THREAD_METHOD,
        ),
        R2DResourceCheck(
            "governed_artifact_bytes",
            True,
            1,
            budget.artifact_mib * MIB,
            "<=",
            ARTIFACT_METHOD,
        ),
        R2DResourceCheck(
            "shutdown_time_seconds", True, 0.0, budget.shutdown_seconds, "<=", SHUTDOWN_METHOD
        ),
        R2DResourceCheck(
            "surviving_descendant_count",
            True,
            0,
            budget.maximum_surviving_descendants,
            "<=",
            SHUTDOWN_METHOD,
        ),
    )
    evidence = R2DResourceEvidence(
        wall_time_seconds=1.0,
        peak_process_tree_rss_bytes=MIB,
        peak_process_tree_native_threads=1,
        maximum_descendant_count=0,
        shutdown_time_seconds=0.0,
        surviving_descendant_pids=(),
        governed_artifact_bytes=1,
        governed_artifact_file_count=1,
        sample_interval_seconds=0.02,
        budget=budget,
        checks=checks,
        measurement_errors=(),
    )
    return evidence.to_dict(), config


def _platform_receipt(
    base: R2DVerificationReceipt,
    *,
    os_family: Literal["linux", "windows", "macos", "other"],
    attested: bool = True,
    python_minor: str | None = None,
    normalized_sha256: str | None = None,
    resource_sha256: str | None = None,
) -> R2DVerificationReceipt:
    payload = base.model_dump(mode="python")
    payload["runtime"] = R2DVerificationRuntime(
        os_family=os_family,
        architecture="x86_64",
        python_minor=python_minor or base.runtime.python_minor,
        github_runner_os_attested=attested,
    )
    checks = list(base.checks)
    if normalized_sha256 is not None:
        payload["normalized_business_sha256"] = normalized_sha256
        checks[0] = checks[0].model_copy(update={"evidence_sha256": normalized_sha256})
        parity_index = R2D_A1_PASS_CHECK_NAMES.index("direct_captured_business_parity")
        checks[parity_index] = checks[parity_index].model_copy(
            update={"evidence_sha256": normalized_sha256}
        )
    if resource_sha256 is not None:
        payload["resource_evidence_sha256"] = resource_sha256
        index = R2D_A1_PASS_CHECK_NAMES.index("bounded_portable_resource_envelope")
        checks[index] = R2DVerificationCheck(
            name="bounded_portable_resource_envelope",
            passed=True,
            detail="different independently verified measured resource evidence",
            evidence_sha256=resource_sha256,
        )
    payload["checks"] = tuple(checks)
    bindings = {name: payload[name] for name in _BINDING_FIELDS}
    payload["evidence_bindings_sha256"] = r2d_evidence_bindings_sha256(**bindings)
    return R2DVerificationReceipt.model_validate(payload)


def _copy_platform_package(
    verified: R2DA1VerificationResult,
    target: Path,
    *,
    os_family: Literal["linux", "windows", "macos", "other"],
    attested: bool = True,
    python_minor: str | None = None,
    normalized_sha256: str | None = None,
    resource_difference: bool = False,
) -> Path:
    shutil.copytree(verified.receipt_path.parent, target)
    bundle_path = target / "r2d-a1-portable-bundle.json"
    bundle = json.loads(bundle_path.read_bytes())
    resource_sha256: str | None = None
    if resource_difference:
        resource = bundle["resource_evidence"]
        resource["observed"]["wall_time_seconds"] += 0.125
        wall_check = next(
            item for item in resource["checks"] if item["name"] == "wall_time_seconds"
        )
        wall_check["observed"] = resource["observed"]["wall_time_seconds"]
        resource_sha256 = canonical_json_sha256(resource)
    receipt = _platform_receipt(
        verified.receipt,
        os_family=os_family,
        attested=attested,
        python_minor=python_minor,
        normalized_sha256=normalized_sha256,
        resource_sha256=resource_sha256,
    )
    if resource_difference:
        publication = next(
            item for item in bundle["publications"] if item["kind"] == "resource_evidence"
        )
        old_relative = Path(*publication["path"].split("/"))
        new_relative = old_relative.with_name(f"{resource_sha256}.json")
        new_path = target / new_relative
        new_path.parent.mkdir(parents=True, exist_ok=True)
        new_path.write_bytes(canonical_json_bytes(bundle["resource_evidence"]) + b"\n")
        publication.update(
            {
                "path": new_relative.as_posix(),
                "content_sha256": resource_sha256,
                "file_sha256": file_sha256(new_path),
            }
        )
    bundle["receipt_binding"] = {
        **{name: getattr(receipt, name) for name in _BINDING_FIELDS},
        "evidence_bindings_sha256": receipt.evidence_bindings_sha256,
    }
    _write_bundle(bundle_path, bundle)
    receipt_path = target / "r2d-a1-verification-receipt.json"
    _write_receipt(receipt_path, receipt)
    return receipt_path


def test_resource_envelope_rejects_self_rebound_over_budget_observation() -> None:
    payload, config = _passing_resource_payload()
    assert _resource_evidence_from_payload(payload, config).passed

    observed = payload["observed"]
    checks = payload["checks"]
    assert isinstance(observed, dict) and isinstance(checks, list)
    observed["wall_time_seconds"] = config.resource_contract.wall_time_seconds + 1.0
    wall_check = next(item for item in checks if item["name"] == "wall_time_seconds")
    wall_check.update(
        {
            "observed": observed["wall_time_seconds"],
            "limit": observed["wall_time_seconds"],
            "passed": True,
            "status": "PASS",
        }
    )

    with pytest.raises(ValueError, match="passing frozen envelope"):
        _resource_evidence_from_payload(payload, config)


@pytest.mark.parametrize("link_position", ["artifact_root", "run_a"])
def test_verifier_preflight_rejects_linked_governed_roots_before_writes(
    tmp_path: Path,
    link_position: str,
) -> None:
    external = tmp_path / f"external-{link_position}"
    external.mkdir()
    artifact_root = tmp_path / f"artifacts-{link_position}"
    if link_position == "artifact_root":
        link = artifact_root
    else:
        artifact_root.mkdir()
        link = artifact_root / "run-a"
    try:
        link.symlink_to(external, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"directory symlinks unavailable: {error}")

    with pytest.raises(R2DPackageConflictError, match="link-like governed directory"):
        verify_r2d_a1(artifact_root)

    assert tuple(external.iterdir()) == ()


def test_verifier_passes_exact_checklist_nonclaims_and_nested_artifacts(
    verified: R2DA1VerificationResult,
) -> None:
    assert verified.passed
    assert verified.receipt.status == "PASS"
    assert tuple(item.name for item in verified.receipt.checks) == R2D_A1_PASS_CHECK_NAMES
    assert len(verified.receipt.checks) == 13
    assert verified.receipt.checks[0].detail == "two fresh replay semantic hashes match"
    assert verified.receipt.full_r2d_a_status == "OPEN"
    assert verified.receipt.full_g2_status == "OPEN"
    assert verified.receipt.service_backed_status == "NOT_RUN"
    assert verified.receipt.operational_recovery_claim is False
    assert verified.receipt_path.name == "r2d-a1-verification-receipt.json"
    assert verified.portable_bundle is not None
    assert verified.portable_bundle.path.name == "r2d-a1-portable-bundle.json"
    assert verified.resource_evidence is not None and verified.resource_evidence.passed
    assert verified.run_a is not None and verified.run_b is not None
    assert verified.run_a.semantic_sha256 == verified.run_b.semantic_sha256

    for run in (verified.run_a, verified.run_b):
        assert len(run.artifacts) == 10
        assert run.capture.producer_ack_ledger.frozen_source_match
        assert run.capture.producer_ack_ledger.real_broker_ack_claim is False
        assert run.capture.checkpoint.producer_ack_frozen_source_match
        assert run.correction_semantics["rev2_only_quarantine_reason_counts"] == {
            "correction_chain_invalid": 1
        }
        assert run.correction_semantics["complete_validated_revisions"] == [1, 2]
        gap_reasons = run.correction_semantics["gap_deferred_reasons"]
        assert isinstance(gap_reasons, list)
        assert "incomplete_visible_revision_chain" in gap_reasons
        assert run.recovery_evidence["exact_business_and_provenance_semantics_match"]
        for artifact in run.artifacts:
            parsed = artifact.path.read_text(encoding="utf-8")
            assert canonical_json_sha256(json.loads(parsed)) == artifact.content_sha256
            assert file_sha256(artifact.path) == artifact.file_sha256
    reloaded = R2DVerificationReceipt.model_validate_json(verified.receipt_path.read_bytes())
    assert reloaded == verified.receipt
    assert verified.receipt_path.read_bytes() == (
        canonical_json_bytes(reloaded.model_dump(mode="json")) + b"\n"
    )


def test_invalid_config_persists_exact_strict_fail_checklist(tmp_path: Path) -> None:
    invalid = tmp_path / "invalid.yaml"
    invalid.write_text("release: not-r2d\n", encoding="utf-8")

    result = verify_r2d_a1(tmp_path / "failed", config_path=invalid)

    assert not result.passed
    assert result.receipt.status == "FAIL"
    assert tuple(item.name for item in result.receipt.checks) == R2D_A1_PASS_CHECK_NAMES
    assert len(result.receipt.checks) == 13
    assert all(not item.passed for item in result.receipt.checks)
    assert result.receipt.full_r2d_a_status == result.receipt.full_g2_status == "OPEN"
    assert result.receipt.service_backed_status == "NOT_RUN"
    assert result.receipt.operational_recovery_claim is False
    assert result.portable_bundle is None
    assert R2DVerificationReceipt.model_validate_json(result.receipt_path.read_bytes()) == (
        result.receipt
    )


def test_cross_platform_comparator_accepts_semantics_and_resource_only_difference(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "linux",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / "windows",
        os_family="windows",
        resource_difference=True,
    )
    left = R2DVerificationReceipt.model_validate_json(left_path.read_bytes())

    assert compare_r2d_a1_receipt_files(left_path, right_path) == {
        "status": "R2D_A1_CROSS_PLATFORM_PASS",
        "passed": True,
        "platforms": ["linux", "windows"],
        "python_minor": left.runtime.python_minor,
        "release": "R2D-A1",
        "full_r2d_a_status": "OPEN",
        "full_g2_status": "OPEN",
        "service_backed_status": "NOT_RUN",
        "operational_recovery_claim": False,
        "normalized_business_sha256": left.normalized_business_sha256,
    }


def test_cross_platform_comparator_accepts_packages_after_source_root_relocation(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "relocated-linux",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / "relocated-windows",
        os_family="windows",
    )
    producer_bundle = json.loads(
        verified.receipt_path.with_name("r2d-a1-portable-bundle.json").read_bytes()
    )
    producer_run_root = verified.receipt_path.parent / Path(*producer_bundle["run_root"].split("/"))
    hidden_run_root = producer_run_root.with_name(f"{producer_run_root.name}-relocation-hidden")
    assert producer_run_root.is_dir()
    assert not hidden_run_root.exists()
    producer_run_root.rename(hidden_run_root)
    try:
        comparison = compare_r2d_a1_receipt_files(left_path, right_path)
    finally:
        hidden_run_root.rename(producer_run_root)

    assert comparison["status"] == "R2D_A1_CROSS_PLATFORM_PASS"
    assert comparison["passed"] is True


def test_cross_platform_comparator_rejects_forged_gap_warehouse_catalog(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "catalog-linux",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / "catalog-windows",
        os_family="windows",
    )
    bundle = json.loads(right_path.with_name("r2d-a1-portable-bundle.json").read_bytes())
    run_root = right_path.parent / Path(*bundle["run_root"].split("/"))
    warehouse = next(
        (run_root / "correction-semantics" / "gap-rebuild" / "warehouse").glob("*.duckdb")
    )
    with duckdb.connect(str(warehouse)) as connection:
        row = connection.execute(
            "select sql from duckdb_views() "
            "where internal = false "
            "and view_name = 'int_r2_deferred_inventory_adjustments'"
        ).fetchone()
        assert row is not None and isinstance(row[0], str)
        catalog = warehouse.stem
        connection.execute("attach ':memory:' as \"forged-catalog\"")
        connection.execute(
            f'create view "forged-catalog".main.stg_r2_inventory_adjustments as '
            f'select * from "{catalog}".main.stg_r2_inventory_adjustments'
        )
        forged_sql = (
            row[0]
            .replace(
                f'"{catalog}".main.stg_r2_inventory_adjustments',
                '"forged-catalog".main.stg_r2_inventory_adjustments',
            )
            .replace("CREATE VIEW", "CREATE OR REPLACE VIEW", 1)
        )
        connection.execute(forged_sql)

    with pytest.raises(ValueError, match="catalog binding"):
        compare_r2d_a1_receipt_files(left_path, right_path)


def test_cross_platform_comparator_rejects_invalid_pairings(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "linux",
        os_family="linux",
    )
    with pytest.raises(ValueError, match="distinct receipt files"):
        compare_r2d_a1_receipt_files(left_path, left_path)

    invalid_options: tuple[
        tuple[Literal["linux", "windows", "macos", "other"], bool, str | None, str | None],
        ...,
    ] = (
        ("linux", True, None, None),
        ("windows", False, None, None),
        (
            "windows",
            True,
            "3.12" if verified.receipt.runtime.python_minor == "3.13" else "3.13",
            None,
        ),
        ("windows", True, None, "e" * 64),
    )
    for index, (family, attested, python_minor, normalized_sha256) in enumerate(invalid_options):
        invalid_path = _copy_platform_package(
            verified,
            tmp_path / f"invalid-{index}",
            os_family=family,
            attested=attested,
            python_minor=python_minor,
            normalized_sha256=normalized_sha256,
        )
        with pytest.raises(ValueError):
            compare_r2d_a1_receipt_files(left_path, invalid_path)


@pytest.mark.parametrize("kind", ["source_file", "parquet", "r2a_summary"])
def test_comparator_rejects_indexed_source_artifact_and_summary_tamper(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
    kind: str,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / f"linux-{kind}",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / f"windows-{kind}",
        os_family="windows",
    )
    bundle = json.loads(right_path.with_name("r2d-a1-portable-bundle.json").read_bytes())
    entry = next(item for item in bundle["artifact_index"]["entries"] if item["kind"] == kind)
    target = right_path.parent / bundle["run_root"] / Path(*entry["path"].split("/"))
    if kind == "r2a_summary":
        summary = json.loads(target.read_bytes())
        summary["quality_counts"]["effective_inventory_adjustments"] += 1
        target.write_bytes(canonical_json_bytes(summary) + b"\n")
    else:
        target.write_bytes(target.read_bytes() + b"tamper")

    with pytest.raises(ValueError, match="artifact index"):
        compare_r2d_a1_receipt_files(left_path, right_path)


@pytest.mark.parametrize("field", ["correction_semantics", "portable_recovery"])
def test_comparator_rejects_tampered_embedded_evidence(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
    field: str,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / f"linux-{field}",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / f"windows-{field}",
        os_family="windows",
    )
    bundle_path = right_path.with_name("r2d-a1-portable-bundle.json")
    bundle = json.loads(bundle_path.read_bytes())
    if field == "correction_semantics":
        bundle[field]["later_complete_replay_corrects"] = False
    else:
        bundle[field]["exact_business_and_provenance_semantics_match"] = False
    _write_bundle(bundle_path, bundle)

    with pytest.raises(ValueError, match="artifact index"):
        compare_r2d_a1_receipt_files(left_path, right_path)


def test_comparator_rejects_self_rebound_over_budget_resource_evidence(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "linux-resource-limit",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / "windows-resource-limit",
        os_family="windows",
    )
    bundle_path = right_path.with_name("r2d-a1-portable-bundle.json")
    bundle = json.loads(bundle_path.read_bytes())
    resource = bundle["resource_evidence"]
    over_budget = resource["budget"]["wall_time_seconds"] + 1.0
    resource["observed"]["wall_time_seconds"] = over_budget
    wall_check = next(item for item in resource["checks"] if item["name"] == "wall_time_seconds")
    wall_check.update(
        {
            "observed": over_budget,
            "limit": over_budget,
            "passed": True,
            "status": "PASS",
        }
    )
    resource_sha256 = _replace_bundle_publication(
        right_path.parent,
        bundle,
        kind="resource_evidence",
        payload=resource,
    )
    receipt = _rebound_receipt(
        R2DVerificationReceipt.model_validate_json(right_path.read_bytes()),
        resource_evidence_sha256=resource_sha256,
    )
    _bind_bundle_to_receipt(bundle, receipt)
    _write_bundle(bundle_path, bundle)
    _write_receipt(right_path, receipt)

    with pytest.raises(ValueError, match="passing frozen envelope"):
        compare_r2d_a1_receipt_files(left_path, right_path)


def test_r2a_summary_rejects_self_rebound_dbt_code_identity(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    assert verified.run_a is not None
    original_path = verified.run_a.direct_rebuilds.early.summary_path
    summary = json.loads(original_path.read_bytes())
    dbt = summary["dbt"]
    dbt["code_sha256"] = "e" * 64
    semantic_payload = dict(summary)
    semantic_payload.pop("semantic_manifest_sha256")
    semantic_payload.pop("status")
    semantic_dbt = dict(dbt)
    semantic_dbt.pop("detail")
    semantic_payload["dbt"] = semantic_dbt
    semantic_sha256 = canonical_rows_sha256([{"manifest": semantic_payload}])
    summary["semantic_manifest_sha256"] = semantic_sha256
    rebound_path = tmp_path / f"{semantic_sha256}.json"
    rebound_path.write_bytes(canonical_json_bytes(summary) + b"\n")

    with pytest.raises(ValueError, match="code identity"):
        _verified_r2a_summary_payload(
            rebound_path,
            cutoff=summary["planning_cutoff"],
            semantic_sha256=semantic_sha256,
            source_manifest_sha256=summary["source_manifest_sha256"],
            source_manifest_file_sha256=summary["source_manifest_file_sha256"],
        )


@pytest.mark.parametrize(
    ("field", "error_pattern"),
    [
        ("normalized_business", "normalized business"),
        ("correction_semantics", "correction semantics"),
        ("portable_recovery", "portable recovery"),
    ],
)
def test_comparator_rejects_fully_rebound_semantic_envelope(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
    field: Literal[
        "normalized_business",
        "correction_semantics",
        "portable_recovery",
    ],
    error_pattern: str,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / f"linux-semantic-rebind-{field}",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / f"windows-semantic-rebind-{field}",
        os_family="windows",
    )
    for receipt_path in (left_path, right_path):
        bundle = json.loads(receipt_path.with_name("r2d-a1-portable-bundle.json").read_bytes())
        run_root = receipt_path.parent / Path(*bundle["run_root"].split("/"))
        if field == "normalized_business":
            anchor_entry = next(
                item
                for item in bundle["artifact_index"]["entries"]
                if item["kind"] == "r2a_summary" and item["route"] == "direct"
            )
            anchor_path = run_root / Path(*anchor_entry["path"].split("/"))
            normalized = bundle["normalized_business"]
            normalized["raw_value_sha256_counts"][0][1] += 1
            recovery = bundle["portable_recovery"]
            recovery["normalized_business_sha256"] = canonical_json_sha256(normalized)
            updates = {
                "normalized_business": normalized,
                "portable_recovery": recovery,
            }
        elif field == "correction_semantics":
            anchor_path = (
                run_root / "correction-semantics" / "complete-source" / "inventory_events.jsonl"
            )
            correction = bundle["correction_semantics"]
            correction["later_complete_replay_corrects"] = False
            updates = {"correction_semantics": correction}
        else:
            anchor_entry = next(
                item
                for item in bundle["artifact_index"]["entries"]
                if item["kind"] == "source_file"
                and item["route"] == "captured"
                and item["dataset"] == "source.inventory_adjustments"
            )
            anchor_path = run_root / Path(*anchor_entry["path"].split("/"))
            recovery = bundle["portable_recovery"]
            recovery["exact_business_and_provenance_semantics_match"] = False
            updates = {"portable_recovery": recovery}
        governed_anchor = anchor_path.read_bytes()
        _self_rebind_embedded_payloads(receipt_path, updates)
        assert anchor_path.read_bytes() == governed_anchor

    with pytest.raises(ValueError, match=error_pattern):
        compare_r2d_a1_receipt_files(left_path, right_path)


def test_comparator_rejects_fully_rebound_expanded_resource_budget_config(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "linux-config-budget-rebind",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / "windows-config-budget-rebind",
        os_family="windows",
    )
    for receipt_path in (left_path, right_path):
        _self_rebind_expanded_budget_config(receipt_path)

    with pytest.raises(ValueError, match=r"wall_time_seconds|checked-in frozen config"):
        compare_r2d_a1_receipt_files(left_path, right_path)


def test_comparator_rejects_fully_rebound_capture_timestamp(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "linux-capture-timestamp-rebind",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / "windows-capture-timestamp-rebind",
        os_family="windows",
    )
    attacked = tuple(
        _self_rebind_capture_timestamp(receipt_path) for receipt_path in (left_path, right_path)
    )

    assert attacked[0] == attacked[1]
    assert attacked[0] != (
        verified.receipt.transcript_sha256,
        verified.receipt.checkpoint_sha256,
    )
    with pytest.raises(
        ValueError,
        match="capture transcript differs from the frozen expected evidence",
    ):
        compare_r2d_a1_receipt_files(left_path, right_path)


def test_comparator_rejects_fully_rebound_direct_validated_and_pass_summary(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "linux-direct-provenance-rebind",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / "windows-direct-provenance-rebind",
        os_family="windows",
    )
    attacked = tuple(
        _self_rebind_direct_validated_and_summary(receipt_path)
        for receipt_path in (left_path, right_path)
    )

    assert attacked[0] == attacked[1]
    with pytest.raises(
        ValueError,
        match="direct provenance differs from the frozen expected projection",
    ):
        compare_r2d_a1_receipt_files(left_path, right_path)


def test_comparator_rejects_fully_rebound_reordered_captured_inventory(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "linux-captured-order-rebind",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / "windows-captured-order-rebind",
        os_family="windows",
    )
    for receipt_path in (left_path, right_path):
        bundle = json.loads(receipt_path.with_name("r2d-a1-portable-bundle.json").read_bytes())
        run_root = receipt_path.parent / Path(*bundle["run_root"].split("/"))
        transcript_entry = next(
            item
            for item in bundle["artifact_index"]["entries"]
            if item["kind"] == "r2d_manifest" and item["dataset"] == "r2d.a1.capture_transcript"
        )
        transcript_path = run_root / Path(*transcript_entry["path"].split("/"))
        admitted_transcript = transcript_path.read_bytes()
        _self_rebind_reordered_captured_inventory(receipt_path)
        assert transcript_path.read_bytes() == admitted_transcript

    with pytest.raises(ValueError, match="exact ordered transcript reconstruction"):
        compare_r2d_a1_receipt_files(left_path, right_path)


@pytest.mark.parametrize("stage", ["validated", "gold"])
def test_comparator_rejects_fully_rebound_parquet_disagreeing_with_pass_summary(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
    stage: Literal["validated", "gold"],
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / f"linux-summary-join-{stage}",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / f"windows-summary-join-{stage}",
        os_family="windows",
    )
    left_summary = _self_rebind_parquet_without_r2a_summary(left_path, stage=stage)
    right_summary = _self_rebind_parquet_without_r2a_summary(right_path, stage=stage)

    assert left_summary == right_summary
    with pytest.raises(ValueError, match="artifact index"):
        compare_r2d_a1_receipt_files(left_path, right_path)


def test_comparator_rejects_rebound_artifact_index_omission(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "linux-index-omission",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / "windows-index-omission",
        os_family="windows",
    )
    bundle_path = right_path.with_name("r2d-a1-portable-bundle.json")
    bundle = json.loads(bundle_path.read_bytes())
    artifact_index = bundle["artifact_index"]
    omitted = next(
        item
        for item in artifact_index["entries"]
        if item["kind"] == "parquet" and item["stage"] == "gold"
    )
    artifact_index["entries"].remove(omitted)
    artifact_index_payload = {
        name: value for name, value in artifact_index.items() if name != "artifact_set_sha256"
    }
    artifact_set_sha256 = canonical_json_sha256(artifact_index_payload)
    artifact_index["artifact_set_sha256"] = artifact_set_sha256
    _replace_bundle_publication(
        right_path.parent,
        bundle,
        kind="artifact_index",
        payload=artifact_index,
    )
    evidence_checkpoint = bundle["evidence_checkpoint"]
    evidence_checkpoint["artifact_set_sha256"] = artifact_set_sha256
    evidence_checkpoint["content_sha256"] = canonical_json_sha256(
        {name: value for name, value in evidence_checkpoint.items() if name != "content_sha256"}
    )
    evidence_checkpoint_sha256 = _replace_bundle_publication(
        right_path.parent,
        bundle,
        kind="evidence_checkpoint",
        payload=evidence_checkpoint,
    )
    receipt = _rebound_receipt(
        R2DVerificationReceipt.model_validate_json(right_path.read_bytes()),
        artifact_set_sha256=artifact_set_sha256,
        evidence_checkpoint_sha256=evidence_checkpoint_sha256,
    )
    _bind_bundle_to_receipt(bundle, receipt)
    _write_bundle(bundle_path, bundle)
    _write_receipt(right_path, receipt)

    with pytest.raises(ValueError, match="artifact index"):
        compare_r2d_a1_receipt_files(left_path, right_path)


def test_comparator_rejects_self_rebound_evidence_checkpoint(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "linux-checkpoint-rebound",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / "windows-checkpoint-rebound",
        os_family="windows",
    )
    bundle_path = right_path.with_name("r2d-a1-portable-bundle.json")
    bundle = json.loads(bundle_path.read_bytes())
    evidence_checkpoint = bundle["evidence_checkpoint"]
    evidence_checkpoint["reconstructed_jsonl_sha256"] = "e" * 64
    evidence_checkpoint["content_sha256"] = canonical_json_sha256(
        {name: value for name, value in evidence_checkpoint.items() if name != "content_sha256"}
    )
    evidence_checkpoint_sha256 = _replace_bundle_publication(
        right_path.parent,
        bundle,
        kind="evidence_checkpoint",
        payload=evidence_checkpoint,
    )
    receipt = _rebound_receipt(
        R2DVerificationReceipt.model_validate_json(right_path.read_bytes()),
        evidence_checkpoint_sha256=evidence_checkpoint_sha256,
    )
    _bind_bundle_to_receipt(bundle, receipt)
    _write_bundle(bundle_path, bundle)
    _write_receipt(right_path, receipt)

    with pytest.raises(ValueError, match="semantic or content binding"):
        compare_r2d_a1_receipt_files(left_path, right_path)


def test_comparator_rejects_noncanonical_tampered_receipt(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    left_path = _copy_platform_package(
        verified,
        tmp_path / "linux",
        os_family="linux",
    )
    right_path = _copy_platform_package(
        verified,
        tmp_path / "windows",
        os_family="windows",
    )
    right_path.write_bytes(right_path.read_bytes() + b" ")

    with pytest.raises(ValueError, match="not canonical"):
        compare_r2d_a1_receipt_files(left_path, right_path)


def _copied_recovery_run(
    verified: R2DA1VerificationResult,
    target: Path,
) -> tuple[R2DA1RunResult, Path]:
    assert verified.run_a is not None
    run = verified.run_a
    shutil.copytree(run.fixture.source_dir.parent, target)
    return run, target


def _verify_copied_recovery(run: R2DA1RunResult, root: Path) -> None:
    _verified_portable_recovery(
        run.recovery_evidence,
        index=run.artifact_index,
        root=root,
        config=run.config,
        checkpoint=run.capture.checkpoint,
        normalized_business=_normalized_payload(run.normalized_outcome),
    )


@pytest.mark.parametrize("mutation", ["missing", "tampered", "extra"])
def test_recovery_rejects_invalid_validated_parquet_topology(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
    mutation: str,
) -> None:
    run, root = _copied_recovery_run(verified, tmp_path / f"recovery-lake-{mutation}")
    lake = root / "portable-recovery" / "derived" / "early" / "lake"
    parquets = sorted(lake.rglob("*.parquet"))
    assert len(parquets) == 8
    if mutation == "missing":
        parquets[0].unlink()
    elif mutation == "tampered":
        parquets[0].write_bytes(parquets[1].read_bytes())
    else:
        shutil.copy2(parquets[0], parquets[0].with_name(f"{'f' * 64}.parquet"))

    with pytest.raises(ValueError, match=r"portable recovery|retained Parquet|artifact is missing"):
        _verify_copied_recovery(run, root)


def test_recovery_rejects_wrong_warehouse_filename(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
) -> None:
    run, root = _copied_recovery_run(verified, tmp_path / "recovery-warehouse-name")
    warehouse = next(
        (root / "portable-recovery" / "derived" / "early" / "warehouse").glob("*.duckdb")
    )
    warehouse.rename(warehouse.with_name("wrong-build-input.duckdb"))

    with pytest.raises(ValueError, match="warehouse filename"):
        _verify_copied_recovery(run, root)


@pytest.mark.parametrize("mutation", ["row", "extra_table", "missing_table"])
def test_recovery_rejects_invalid_duckdb_gold_state(
    verified: R2DA1VerificationResult,
    tmp_path: Path,
    mutation: str,
) -> None:
    run, root = _copied_recovery_run(verified, tmp_path / f"recovery-duckdb-{mutation}")
    warehouse = next(
        (root / "portable-recovery" / "derived" / "early" / "warehouse").glob("*.duckdb")
    )
    with duckdb.connect(str(warehouse)) as connection:
        if mutation == "row":
            connection.execute(
                "update gold_r2_inventory_position set on_hand_qty = on_hand_qty + 1"
            )
        elif mutation == "extra_table":
            connection.execute("create table forged_gold_evidence(value integer)")
        else:
            connection.execute("drop table gold_r2_open_orders")

    with pytest.raises(ValueError, match=r"warehouse (Gold|table topology)"):
        _verify_copied_recovery(run, root)
