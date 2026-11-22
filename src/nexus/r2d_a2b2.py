"""Final retained-state rebuild, service parity, and recovery gate for R2D-A2."""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast

from nexus.ingestion.r2d_a2_contracts import (
    R2D_A2_PASS_CHECK_NAMES,
    R2DA2ResourceEvidence,
    R2DA2ServiceCheckpoint,
    R2DA2VerificationCheck,
    R2DA2VerificationReceipt,
    load_r2d_a2_service_image_evidence,
    r2d_a2_code_sha256,
)
from nexus.ingestion.r2d_a2b_contracts import (
    R2DA2b1VerificationReceipt,
    r2d_a2b1_code_sha256,
)
from nexus.ingestion.r2d_capture import (
    raw_value_multiset_sha256,
    reconstruct_jsonl_bytes,
)
from nexus.ingestion.r2d_contracts import (
    canonical_json_sha256,
    load_r2d_config,
    r2d_config_sha256,
    r2d_sha256_multiset_sha256,
)
from nexus.ingestion.r2d_service import R2DA2Runtime, R2DA2Service
from nexus.ingestion.r2d_service_contracts import (
    load_r2d_a2a_config,
    r2d_a2a_config_sha256,
)
from nexus.modeling.r3_artifacts import ScientificArtifactRef, write_immutable_json_manifest
from nexus.r2d import (
    R2DNormalizedBusinessOutcome,
    R2DRouteProvenance,
    normalize_business_outcome,
    reconstruct_r2_source_package,
    require_business_parity,
    route_provenance,
    run_route_rebuilds,
    split_lf_jsonl_values,
)
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import file_sha256

_ARTIFACT_LIMIT_BYTES = 1024 * 1024 * 1024
_WALL_TIME_LIMIT_SECONDS = 600


@dataclass(frozen=True)
class R2DA2VerificationResult:
    """Final bounded A2 receipt and immutable publication."""

    receipt: R2DA2VerificationReceipt
    receipt_artifact: ScientificArtifactRef


def _preflight_artifact_root(project_root: Path, artifact_root: Path) -> Path:
    resolved_project = project_root.resolve()
    resolved = artifact_root.resolve()
    if resolved == resolved_project or not resolved.is_relative_to(resolved_project):
        raise ValueError("A2 artifact root must be a dedicated directory inside the project")
    current = resolved_project
    for part in resolved.relative_to(resolved_project).parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise ValueError("A2 artifact root may not traverse a symlink")
    if resolved.exists() and any(resolved.iterdir()):
        raise ValueError("A2 verification requires a fresh empty artifact root")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _canonical_dataclass_sha256(value: R2DNormalizedBusinessOutcome) -> str:
    return canonical_json_sha256(asdict(value))


def _stable_provenance_payload(provenance: R2DRouteProvenance) -> dict[str, object]:
    """Keep route semantics while excluding paths and physical serialization hashes."""

    return {
        "source_manifest_sha256": provenance.source_manifest_sha256,
        "source_manifest_file_sha256": provenance.source_manifest_file_sha256,
        "semantic_manifests": [list(item) for item in provenance.semantic_manifests],
        "source_files": [asdict(item) for item in provenance.source_files],
        "validated_artifacts": [
            {
                "cutoff": item.cutoff,
                "stage": item.stage,
                "dataset": item.dataset,
                "row_count": item.row_count,
                "content_sha256": item.content_sha256,
                "schema_sha256": item.schema_sha256,
            }
            for item in provenance.validated_artifacts
        ],
        "gold_artifacts": [
            {
                "cutoff": item.cutoff,
                "stage": item.stage,
                "dataset": item.dataset,
                "row_count": item.row_count,
                "content_sha256": item.content_sha256,
                "schema_sha256": item.schema_sha256,
            }
            for item in provenance.gold_artifacts
        ],
        "summaries": [
            {
                "cutoff": item.cutoff,
                "semantic_manifest_sha256": item.semantic_manifest_sha256,
            }
            for item in provenance.summaries
        ],
    }


def _artifact_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _write_manifest(
    output: Path,
    *,
    dataset: str,
    payload: object,
    contract: str,
) -> ScientificArtifactRef:
    return write_immutable_json_manifest(
        output,
        dataset=dataset,
        manifest=payload,
        schema_identity={"contract": contract},
    )


def _load_a2b1_receipt(path: Path) -> R2DA2b1VerificationReceipt:
    try:
        return R2DA2b1VerificationReceipt.model_validate_json(path.read_bytes())
    except OSError as exc:
        raise ValueError(f"cannot read the required A2b-1 receipt: {path}") from exc


def _normalized_payload(value: R2DNormalizedBusinessOutcome) -> dict[str, object]:
    return cast(dict[str, object], asdict(value))


def verify_r2d_a2b2(
    *,
    project_root: Path,
    artifact_root: Path,
    bootstrap_servers: str,
    postgres_dsn: str,
    a2b1_receipt_path: Path,
    service_image_evidence_path: Path,
) -> R2DA2VerificationResult:
    """Close bounded R2D-A2 with real-service R2A parity and retained-state replay."""

    started = time.perf_counter()
    root = project_root.resolve()
    service_images = load_r2d_a2_service_image_evidence(service_image_evidence_path)
    output = _preflight_artifact_root(root, artifact_root)
    data_config = load_r2d_config(root / "configs" / "ingestion" / "r2d-a1.yaml")
    service_config = load_r2d_a2a_config(root / "configs" / "ingestion" / "r2d-a2a.yaml")
    data_config_sha256 = r2d_config_sha256(data_config)
    service_config_sha256 = r2d_a2a_config_sha256(service_config)
    a2b1_receipt = _load_a2b1_receipt(a2b1_receipt_path)
    if (
        a2b1_receipt.data_config_sha256 != data_config_sha256
        or a2b1_receipt.service_config_sha256 != service_config_sha256
        or a2b1_receipt.raw_value_multiset_sha256
        != data_config.source_contract.raw_value_multiset_sha256
        or a2b1_receipt.record_count != data_config.source_contract.inventory_event_count
        or a2b1_receipt.code_sha256 != r2d_a2b1_code_sha256(root)
    ):
        raise ValueError("A2b-1 receipt does not bind the current A2 data/service contract")
    a2b1_receipt_artifact = _write_manifest(
        output,
        dataset="r2d.a2.bound_a2b1_receipt",
        payload=a2b1_receipt.model_dump(mode="json"),
        contract=a2b1_receipt.contract_version,
    )
    if a2b1_receipt_artifact.file_sha256 != file_sha256(a2b1_receipt_path):
        raise ValueError("A2b-1 receipt bytes are not canonical immutable JSON")

    service_image_artifact = _write_manifest(
        output,
        dataset="r2d.a2.service_images",
        payload=service_images.model_dump(mode="json"),
        contract=service_images.contract_version,
    )
    if service_image_artifact.file_sha256 != file_sha256(service_image_evidence_path):
        raise ValueError("A2 service image evidence bytes are not canonical immutable JSON")

    fixture = generate_r2_smoke_fixture(output / "reference-source")
    direct_jsonl = (fixture.source_dir / "inventory_events.jsonl").read_bytes()
    direct_values = split_lf_jsonl_values(direct_jsonl)
    service = R2DA2Service(
        project_root=root,
        service_config=service_config,
        data_config=data_config,
        runtime=R2DA2Runtime(
            bootstrap_servers=bootstrap_servers,
            postgres_dsn=postgres_dsn,
        ),
    )
    service.provision_topic()
    with service.connect_postgres() as connection:
        migration_identity = service.apply_migration(connection)
        starts = service.initialize_fresh_empty(connection)
        if a2b1_receipt.migration_sha256 != migration_identity:
            raise ValueError("A2b-1 receipt does not bind the current PostgreSQL migration")
        broker_ack_ledger = service.produce(direct_values)
        capture = service.capture_barrier(connection, broker_ack_ledger)

    captured_values = tuple(
        item.record.value_bytes
        for item in capture.transcript.records
        if item.record.value_bytes is not None and item.decision.disposition == "admitted"
    )
    raw_multiset_sha256 = raw_value_multiset_sha256(captured_values)
    acknowledged_multiset_sha256 = r2d_sha256_multiset_sha256(
        tuple(item.raw_value_sha256 for item in broker_ack_ledger.acknowledgements)
    )
    reconstructed_jsonl = reconstruct_jsonl_bytes(capture.transcript, data_config)
    reconstructed_values = split_lf_jsonl_values(reconstructed_jsonl)
    if not (
        len(direct_values)
        == broker_ack_ledger.acknowledged_record_count
        == len(captured_values)
        == len(reconstructed_values)
        == data_config.source_contract.inventory_event_count
    ):
        raise ValueError("A2 acknowledged/captured/reconstructed record counts differ")
    if not (
        raw_value_multiset_sha256(direct_values)
        == acknowledged_multiset_sha256
        == raw_multiset_sha256
        == raw_value_multiset_sha256(reconstructed_values)
        == data_config.source_contract.raw_value_multiset_sha256
    ):
        raise ValueError("A2 acknowledged raw-value multiplicity is not conserved")

    checkpoint_payload: dict[str, object] = {
        "contract_version": "r2d.a2.service-checkpoint.v1",
        "topic": service.topic,
        "starts": list(starts),
        "ends": list(capture.ends),
        "next_offsets": list(capture.next_offsets),
        "record_count": len(capture.transcript.records),
        "data_config_sha256": data_config_sha256,
        "service_config_sha256": service_config_sha256,
        "service_image_evidence_sha256": service_images.content_sha256,
        "migration_sha256": migration_identity,
        "broker_ack_sha256": broker_ack_ledger.content_sha256,
        "transcript_sha256": capture.transcript.content_sha256,
        "lineage_sha256": capture.transcript.lineage_sha256,
        "raw_value_multiset_sha256": raw_multiset_sha256,
    }
    checkpoint = R2DA2ServiceCheckpoint.model_validate(
        {
            **checkpoint_payload,
            "content_sha256": canonical_json_sha256(checkpoint_payload),
        }
    )
    checkpoint_artifact = _write_manifest(
        output,
        dataset="r2d.a2.service_checkpoint",
        payload=checkpoint.model_dump(mode="json"),
        contract=checkpoint.contract_version,
    )

    service_package = reconstruct_r2_source_package(
        reference_source_dir=fixture.source_dir,
        output_dir=output / "service-source",
        inventory_jsonl=reconstructed_jsonl,
    )
    cutoffs = tuple(
        item.planning_cutoff
        for item in sorted(
            data_config.snapshot_contract.planning_cutoffs,
            key=lambda item: item.planning_cutoff,
        )
    )
    direct_rebuilds = run_route_rebuilds(
        project_root=root,
        source_dir=fixture.source_dir,
        artifact_dir=output / "routes" / "direct",
        early_cutoff=cutoffs[0],
        final_cutoff=cutoffs[1],
    )
    service_first_dir = output / "routes" / "service-first"
    service_rebuilds = run_route_rebuilds(
        project_root=root,
        source_dir=service_package.source_dir,
        artifact_dir=service_first_dir,
        early_cutoff=cutoffs[0],
        final_cutoff=cutoffs[1],
    )
    direct_normalized = normalize_business_outcome(
        raw_values=direct_values,
        rebuilds=direct_rebuilds,
    )
    service_normalized = normalize_business_outcome(
        raw_values=reconstructed_values,
        rebuilds=service_rebuilds,
    )
    require_business_parity(direct_normalized, service_normalized)
    normalized_sha256 = _canonical_dataclass_sha256(service_normalized)
    if normalized_sha256 != data_config.expected_evidence_contract.normalized_business_sha256:
        raise ValueError("real-service normalized business evidence differs from portable R2D-A1")

    direct_provenance = route_provenance(route="direct", rebuilds=direct_rebuilds)
    service_provenance = route_provenance(
        route="captured",
        rebuilds=service_rebuilds,
        checkpoint_sha256=checkpoint.content_sha256,
    )
    direct_provenance_sha256 = canonical_json_sha256(_stable_provenance_payload(direct_provenance))
    service_provenance_sha256 = canonical_json_sha256(
        _stable_provenance_payload(service_provenance)
    )
    if direct_provenance_sha256 == service_provenance_sha256:
        raise ValueError("direct and real-service provenance unexpectedly collapsed")

    shutil.rmtree(service_first_dir)
    provenance_distinction_sha256 = canonical_json_sha256(
        {"direct": direct_provenance_sha256, "service": service_provenance_sha256}
    )
    if service_first_dir.exists():
        raise ValueError("derived service outputs were not deleted before recovery")
    with service.connect_postgres() as connection:
        recovery_transcript = service.read_back_transcript(
            connection,
            starts=starts,
            ends=capture.ends,
        )
    if recovery_transcript != capture.transcript:
        raise ValueError("PostgreSQL retained-state transcript changed before recovery")
    recovery_jsonl = reconstruct_jsonl_bytes(recovery_transcript, data_config)
    recovery_package = reconstruct_r2_source_package(
        reference_source_dir=fixture.source_dir,
        output_dir=output / "recovery-source",
        inventory_jsonl=recovery_jsonl,
    )
    recovery_rebuilds = run_route_rebuilds(
        project_root=root,
        source_dir=recovery_package.source_dir,
        artifact_dir=output / "routes" / "service-recovery",
        early_cutoff=cutoffs[0],
        final_cutoff=cutoffs[1],
    )
    recovery_normalized = normalize_business_outcome(
        raw_values=split_lf_jsonl_values(recovery_jsonl),
        rebuilds=recovery_rebuilds,
    )
    require_business_parity(service_normalized, recovery_normalized)
    recovery_provenance = route_provenance(
        route="captured",
        rebuilds=recovery_rebuilds,
        checkpoint_sha256=checkpoint.content_sha256,
    )
    recovery_provenance_sha256 = canonical_json_sha256(
        _stable_provenance_payload(recovery_provenance)
    )
    if recovery_provenance_sha256 != service_provenance_sha256:
        raise ValueError("retained-state recovery changed stable service provenance")

    normalized_artifact = _write_manifest(
        output,
        dataset="r2d.a2.normalized_business",
        payload=_normalized_payload(service_normalized),
        contract="r2d.a2.normalized-business.v1",
    )
    direct_provenance_artifact = _write_manifest(
        output,
        dataset="r2d.a2.direct_provenance",
        payload=_stable_provenance_payload(direct_provenance),
        contract="r2d.a2.route-provenance.v1",
    )
    service_provenance_artifact = _write_manifest(
        output,
        dataset="r2d.a2.service_provenance",
        payload=_stable_provenance_payload(service_provenance),
        contract="r2d.a2.route-provenance.v1",
    )
    recovery_provenance_artifact = _write_manifest(
        output,
        dataset="r2d.a2.recovery_provenance",
        payload=_stable_provenance_payload(recovery_provenance),
        contract="r2d.a2.route-provenance.v1",
    )
    recovery_payload: dict[str, object] = {
        "contract_version": "r2d.a2.service-recovery.v1",
        "checkpoint_sha256": checkpoint.content_sha256,
        "first_derived_state_deleted": True,
        "postgres_transcript_sha256": recovery_transcript.content_sha256,
        "reconstructed_inventory_sha256": hashlib.sha256(recovery_jsonl).hexdigest(),
        "source_manifest_sha256": canonical_json_sha256(
            recovery_package.manifest.model_dump(mode="json")
        ),
        "source_manifest_file_sha256": recovery_package.manifest_file_sha256,
        "normalized_business_sha256": _canonical_dataclass_sha256(recovery_normalized),
        "stable_provenance_sha256": recovery_provenance_sha256,
        "semantic_manifests": [list(item) for item in recovery_provenance.semantic_manifests],
        "gold_artifact_count": len(recovery_provenance.gold_artifacts),
    }
    recovery_sha256 = canonical_json_sha256(recovery_payload)
    recovery_artifact = _write_manifest(
        output,
        dataset="r2d.a2.service_recovery",
        payload={**recovery_payload, "content_sha256": recovery_sha256},
        contract="r2d.a2.service-recovery.v1",
    )

    governed_route_bytes = _artifact_bytes(output)
    elapsed = time.perf_counter() - started
    resource_payload: dict[str, object] = {
        "contract_version": "r2d.a2.resource-evidence.v1",
        "wall_time_seconds": elapsed,
        "wall_time_limit_seconds": _WALL_TIME_LIMIT_SECONDS,
        "pre_receipt_artifact_bytes": governed_route_bytes,
        "artifact_limit_bytes": _ARTIFACT_LIMIT_BYTES,
        "passed": (
            elapsed <= _WALL_TIME_LIMIT_SECONDS and governed_route_bytes <= _ARTIFACT_LIMIT_BYTES
        ),
    }
    resource_evidence = R2DA2ResourceEvidence.model_validate(
        {**resource_payload, "content_sha256": canonical_json_sha256(resource_payload)}
    )
    resource_artifact = _write_manifest(
        output,
        dataset="r2d.a2.resource_evidence",
        payload=resource_evidence.model_dump(mode="json"),
        contract=resource_evidence.contract_version,
    )
    evidence_refs = (
        a2b1_receipt_artifact,
        service_image_artifact,
        checkpoint_artifact,
        normalized_artifact,
        direct_provenance_artifact,
        service_provenance_artifact,
        recovery_provenance_artifact,
        recovery_artifact,
        resource_artifact,
    )
    artifact_set_sha256 = canonical_json_sha256(
        [
            {
                "dataset": item.dataset,
                "content_sha256": item.content_sha256,
                "schema_sha256": item.schema_sha256,
                "file_sha256": item.file_sha256,
            }
            for item in evidence_refs
        ]
    )
    r2a_semantics_sha256 = canonical_json_sha256(
        {
            "direct": [list(item) for item in direct_provenance.semantic_manifests],
            "service": [list(item) for item in service_provenance.semantic_manifests],
            "recovery": [list(item) for item in recovery_provenance.semantic_manifests],
        }
    )
    source_reconstruction_sha256 = canonical_json_sha256(
        {
            "inventory_jsonl_sha256": service_package.inventory_jsonl_sha256,
            "inventory_record_count": service_package.inventory_record_count,
            "source_manifest_sha256": canonical_json_sha256(
                service_package.manifest.model_dump(mode="json")
            ),
            "source_manifest_file_sha256": service_package.manifest_file_sha256,
        }
    )
    evidence_by_check = {
        "verified_a2b1_crash_receipt": a2b1_receipt.content_sha256,
        "resolved_service_images": service_images.content_sha256,
        "real_service_capture_complete": checkpoint.content_sha256,
        "postgres_source_reconstruction": source_reconstruction_sha256,
        "acknowledged_raw_conservation": raw_multiset_sha256,
        "public_r2a_two_cutoffs": r2a_semantics_sha256,
        "direct_service_business_parity": normalized_sha256,
        "portable_reference_business_join": (
            data_config.expected_evidence_contract.normalized_business_sha256
        ),
        "route_provenance_is_distinct": provenance_distinction_sha256,
        "derived_state_recovery_identity": recovery_sha256,
        "bounded_resource_envelope": resource_evidence.content_sha256,
    }
    checks = tuple(
        R2DA2VerificationCheck(name=name, passed=True, evidence_sha256=evidence_by_check[name])
        for name in R2D_A2_PASS_CHECK_NAMES
    )
    payload: dict[str, object] = {
        "contract_version": "r2d.a2.verification-receipt.v2",
        "release": "R2D-A2",
        "status": "PASS",
        "checks": [item.model_dump(mode="json") for item in checks],
        "topic": service.topic,
        "partition_count": service_config.broker.partition_count,
        "record_count": len(capture.transcript.records),
        "planning_cutoffs": [item.isoformat().replace("+00:00", "Z") for item in cutoffs],
        "data_config_sha256": data_config_sha256,
        "service_config_sha256": service_config_sha256,
        "service_image_evidence_content_sha256": service_images.content_sha256,
        "service_image_evidence_file_sha256": service_image_artifact.file_sha256,
        "postgres_observed_image_id": service_images.observed_image_id("postgres"),
        "redpanda_observed_image_id": service_images.observed_image_id("redpanda"),
        "migration_sha256": migration_identity,
        "code_sha256": r2d_a2_code_sha256(root),
        "a2b1_receipt_content_sha256": a2b1_receipt.content_sha256,
        "a2b1_receipt_file_sha256": a2b1_receipt_artifact.file_sha256,
        "broker_ack_sha256": broker_ack_ledger.content_sha256,
        "transcript_sha256": capture.transcript.content_sha256,
        "lineage_sha256": capture.transcript.lineage_sha256,
        "service_checkpoint_sha256": checkpoint.content_sha256,
        "raw_value_multiset_sha256": raw_multiset_sha256,
        "reconstructed_inventory_sha256": service_package.inventory_jsonl_sha256,
        "reconstructed_source_manifest_sha256": canonical_json_sha256(
            service_package.manifest.model_dump(mode="json")
        ),
        "reconstructed_source_manifest_file_sha256": service_package.manifest_file_sha256,
        "normalized_business_sha256": normalized_sha256,
        "direct_provenance_sha256": direct_provenance_sha256,
        "service_provenance_sha256": service_provenance_sha256,
        "source_reconstruction_sha256": source_reconstruction_sha256,
        "r2a_semantics_sha256": r2a_semantics_sha256,
        "recovery_provenance_sha256": recovery_provenance_sha256,
        "service_recovery_sha256": recovery_sha256,
        "resource_evidence_sha256": resource_evidence.content_sha256,
        "artifact_set_sha256": artifact_set_sha256,
        "service_foundation_status": "PASS",
        "provenance_distinction_sha256": provenance_distinction_sha256,
        "crash_recovery_status": "PASS",
        "downstream_parity_status": "PASS",
        "r2d_a2_status": "PASS",
        "full_r2d_a_status": "PASS",
        "full_g2_status": "OPEN",
        "operational_recovery_claim": True,
        "exactly_once_claim": False,
    }
    receipt = R2DA2VerificationReceipt.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )
    receipt_artifact = _write_manifest(
        output,
        dataset="r2d.a2.verification_receipt",
        payload=receipt.model_dump(mode="json"),
        contract=receipt.contract_version,
    )
    return R2DA2VerificationResult(receipt=receipt, receipt_artifact=receipt_artifact)


def dump_a2_result(result: R2DA2VerificationResult) -> str:
    """Return a secret-free CLI summary."""

    receipt = result.receipt
    return json.dumps(
        {
            "status": receipt.status,
            "release": receipt.release,
            "checks": [item.model_dump(mode="json") for item in receipt.checks],
            "normalized_business_sha256": receipt.normalized_business_sha256,
            "service_image_evidence_sha256": (receipt.service_image_evidence_content_sha256),
            "r2d_a2_status": receipt.r2d_a2_status,
            "full_r2d_a_status": receipt.full_r2d_a_status,
            "full_g2_status": receipt.full_g2_status,
            "exactly_once_claim": receipt.exactly_once_claim,
            "receipt": str(result.receipt_artifact.path),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
