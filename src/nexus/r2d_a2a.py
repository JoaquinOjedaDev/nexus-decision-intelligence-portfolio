"""Executable verifier for the first real-service R2D-A2 checkpoint."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from nexus.ingestion.r2d_capture import raw_value_multiset_sha256
from nexus.ingestion.r2d_contracts import (
    canonical_json_sha256,
    load_r2d_config,
    r2d_config_sha256,
)
from nexus.ingestion.r2d_service import R2DA2Runtime, R2DA2Service
from nexus.ingestion.r2d_service_contracts import (
    R2D_A2A_PASS_CHECK_NAMES,
    R2DA2aVerificationCheck,
    R2DA2aVerificationReceipt,
    load_r2d_a2a_config,
    r2d_a2a_code_sha256,
    r2d_a2a_config_sha256,
)
from nexus.modeling.r3_artifacts import ScientificArtifactRef, write_immutable_json_manifest
from nexus.r2d import split_lf_jsonl_values
from nexus.simulation import generate_r2_smoke_fixture


@dataclass(frozen=True)
class R2DA2aVerificationResult:
    """Passing service-foundation receipt and immutable publication."""

    receipt: R2DA2aVerificationReceipt
    receipt_artifact: ScientificArtifactRef


def _preflight_artifact_root(project_root: Path, artifact_root: Path) -> Path:
    resolved_project = project_root.resolve()
    resolved = artifact_root.resolve()
    if resolved == resolved_project or not resolved.is_relative_to(resolved_project):
        raise ValueError("A2a artifact root must be a dedicated directory inside the project")
    current = resolved_project
    for part in resolved.relative_to(resolved_project).parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise ValueError("A2a artifact root may not traverse a symlink")
    if resolved.exists() and any(resolved.iterdir()):
        raise ValueError("A2a verification requires a fresh empty artifact root")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def verify_r2d_a2a(
    *,
    project_root: Path,
    artifact_root: Path,
    bootstrap_servers: str,
    postgres_dsn: str,
    data_config_path: Path | None = None,
    service_config_path: Path | None = None,
) -> R2DA2aVerificationResult:
    """Run the fresh-topic real broker/PostgreSQL service-foundation gate."""

    root = project_root.resolve()
    output = _preflight_artifact_root(root, artifact_root)
    data_config = load_r2d_config(
        data_config_path or root / "configs" / "ingestion" / "r2d-a1.yaml"
    )
    service_config = load_r2d_a2a_config(
        service_config_path or root / "configs" / "ingestion" / "r2d-a2a.yaml"
    )
    fixture = generate_r2_smoke_fixture(output / "source")
    values = split_lf_jsonl_values((fixture.source_dir / "inventory_events.jsonl").read_bytes())
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
        broker_ack_ledger = service.produce(values)
        capture = service.capture_barrier(connection, broker_ack_ledger)
        inbox_count_row = connection.execute("SELECT COUNT(*) FROM r2d_capture.inbox").fetchone()
    if inbox_count_row is None or inbox_count_row[0] != len(values):
        raise ValueError("PostgreSQL inbox count differs from the frozen fixture")
    if capture.starts != starts or capture.next_offsets != capture.ends:
        raise ValueError("service capture did not complete its frozen barrier")
    captured_values = tuple(
        item.record.value_bytes
        for item in capture.transcript.records
        if item.record.value_bytes is not None
    )
    raw_multiset_sha256 = raw_value_multiset_sha256(captured_values)
    if raw_multiset_sha256 != data_config.source_contract.raw_value_multiset_sha256:
        raise ValueError("service capture raw-value multiset differs from frozen source")

    offset_evidence = canonical_json_sha256(
        {
            "starts": list(capture.starts),
            "ends": list(capture.ends),
            "next_offsets": list(capture.next_offsets),
        }
    )
    evidence_by_check = {
        "real_topic_topology": r2d_a2a_config_sha256(service_config),
        "receipted_fresh_empty_initialization": offset_evidence,
        "real_broker_acks_all": capture.broker_ack_ledger.content_sha256,
        "atomic_postgres_inbox_progress": capture.transcript.content_sha256,
        "manual_commit_convergence": offset_evidence,
        "exact_byte_readback": raw_multiset_sha256,
    }
    checks = tuple(
        R2DA2aVerificationCheck(
            name=name,
            passed=True,
            evidence_sha256=evidence_by_check[name],
        )
        for name in R2D_A2A_PASS_CHECK_NAMES
    )
    payload: dict[str, object] = {
        "contract_version": "r2d.a2a.verification-receipt.v1",
        "release": "R2D-A2a",
        "status": "PASS",
        "checks": [item.model_dump(mode="json") for item in checks],
        "topic": service_config.broker.topic,
        "partition_count": service_config.broker.partition_count,
        "replication_factor": service_config.broker.replication_factor,
        "producer_acks": service_config.broker.producer_acks,
        "record_count": len(capture.transcript.records),
        "starts": list(capture.starts),
        "ends": list(capture.ends),
        "next_offsets": list(capture.next_offsets),
        "data_config_sha256": r2d_config_sha256(data_config),
        "service_config_sha256": r2d_a2a_config_sha256(service_config),
        "migration_sha256": migration_identity,
        "code_sha256": r2d_a2a_code_sha256(root),
        "broker_ack_sha256": capture.broker_ack_ledger.content_sha256,
        "transcript_sha256": capture.transcript.content_sha256,
        "lineage_sha256": capture.transcript.lineage_sha256,
        "raw_value_multiset_sha256": raw_multiset_sha256,
        "service_foundation_status": "PASS",
        "crash_recovery_status": "NOT_RUN",
        "downstream_parity_status": "NOT_RUN",
        "r2d_a2_status": "IN_PROGRESS",
        "full_r2d_a_status": "OPEN",
        "full_g2_status": "OPEN",
        "exactly_once_claim": False,
    }
    receipt = R2DA2aVerificationReceipt.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )
    receipt_artifact = write_immutable_json_manifest(
        output,
        dataset="r2d.a2a.verification_receipt",
        manifest=receipt.model_dump(mode="json"),
        schema_identity={
            "contract_version": receipt.contract_version,
            "release": receipt.release,
        },
    )
    return R2DA2aVerificationResult(receipt=receipt, receipt_artifact=receipt_artifact)
