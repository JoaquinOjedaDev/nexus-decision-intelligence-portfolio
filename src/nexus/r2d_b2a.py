"""Executable live dead-letter and governed redrive gate for R2D-B2a."""

from __future__ import annotations

import hashlib
import json
import os
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nexus.ingestion.r2d_b1 import (
    build_dead_letter_record,
    classify_schema_value,
    create_redrive_request,
    validate_redrive_ledger,
)
from nexus.ingestion.r2d_b1_contracts import (
    R2DB1DeadLetterRecord,
    R2DB1RedriveRequest,
    load_r2d_b1_config,
    r2d_b1_config_sha256,
)
from nexus.ingestion.r2d_b2_contracts import (
    DEFAULT_R2D_B2A_CONFIG_PATH,
    PROJECT_ROOT,
    R2D_B2A_EVIDENCE_DATASETS,
    R2D_B2A_PASS_CHECK_NAMES,
    R2DB2AAckLedger,
    R2DB2AArtifactRef,
    R2DB2AServiceImageEvidence,
    R2DB2AVerificationReceipt,
    load_r2d_b2a_config,
    load_r2d_b2a_service_image,
    r2d_b2a_code_sha256,
    r2d_b2a_config_sha256,
)
from nexus.ingestion.r2d_capture import capture_transcript, partition_for_key
from nexus.ingestion.r2d_contracts import R2DConfig, canonical_json_sha256, load_r2d_config
from nexus.modeling.r3_artifacts import (
    ScientificArtifactRef,
    canonical_json_bytes,
    write_immutable_json_manifest,
)
from nexus.storage import file_sha256


class R2DB2AGovernanceError(ValueError):
    """Fail-closed B2a evidence or service-boundary error."""


@dataclass(frozen=True, slots=True)
class R2DB2AVerificationResult:
    receipt: R2DB2AVerificationReceipt
    receipt_artifact: ScientificArtifactRef

    @property
    def passed(self) -> bool:
        return self.receipt.status == "PASS"


def _event(
    *,
    logical_event_id: str,
    event_id: str,
    idempotency_key: str,
    event_version: int,
) -> dict[str, Any]:
    return {
        "contract_version": "1.0",
        "event_id": event_id,
        "event_type": "inventory.adjusted",
        "event_version": event_version,
        "source": "synthetic.wms",
        "subject_id": "SKU-001:DC-ARG-01",
        "occurred_at": "2026-06-01T00:00:00Z",
        "ingested_at": "2026-06-01T00:00:01Z",
        "idempotency_key": idempotency_key,
        "payload": {
            "logical_event_id": logical_event_id,
            "revision": 1,
            "sku_id": "SKU-001",
            "location_id": "DC-ARG-01",
            "delta_qty": 1,
            "reason_code": "cycle_count",
        },
    }


def _event_bytes(event: dict[str, Any]) -> bytes:
    return canonical_json_bytes(event)


def _key_for_partition(partition: int) -> tuple[str, bytes]:
    for suffix in range(1, 1000):
        logical_event_id = f"ADJ-B2A-{partition}-{suffix:03d}"
        key = f"synthetic.wms:{logical_event_id}".encode()
        if partition_for_key(key, partition_count=2) == partition:
            return logical_event_id, key
    raise R2DB2AGovernanceError("cannot derive a deterministic B2a routing key")


def _source_headers(data_config: R2DConfig, routing_key: bytes) -> tuple[tuple[str, bytes], ...]:
    contract = data_config.header_contract
    return (
        (contract.contract_header_name, contract.contract_header_value.encode()),
        (contract.routing_header_name, routing_key),
        (contract.schema_header_name, contract.schema_header_value.encode("ascii")),
    )


def _governance_headers(contract: str, causation: str) -> tuple[tuple[str, bytes], ...]:
    return (
        ("nexus-causation", causation.encode()),
        ("nexus-contract", contract.encode()),
    )


def _artifact_ref(root: Path, artifact: ScientificArtifactRef) -> R2DB2AArtifactRef:
    return R2DB2AArtifactRef(
        dataset=artifact.dataset,
        relative_path=artifact.path.resolve().relative_to(root.resolve()).as_posix(),
        content_sha256=artifact.content_sha256,
        schema_sha256=artifact.schema_sha256,
        file_sha256=artifact.file_sha256,
        row_count=1,
    )


def _runtime() -> dict[str, object]:
    runner_os = os.getenv("RUNNER_OS")
    return {
        "operating_system": runner_os or platform.system(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "github_runner_os_attested": os.getenv("GITHUB_ACTIONS") == "true" and bool(runner_os),
    }


def _publish_evidence(
    root: Path, payloads: dict[str, dict[str, object]]
) -> tuple[R2DB2AArtifactRef, ...]:
    if tuple(f"r2d.b2a.{name}" for name in payloads) != R2D_B2A_EVIDENCE_DATASETS:
        raise R2DB2AGovernanceError("B2a evidence payload topology differs from contract")
    artifacts = []
    for name, payload in payloads.items():
        artifact = write_immutable_json_manifest(
            root,
            dataset=f"r2d.b2a.{name}",
            manifest=payload,
            schema_identity={"contract": payload["contract_version"]},
        )
        artifacts.append(_artifact_ref(root, artifact))
    return tuple(artifacts)


def verify_r2d_b2a(
    *,
    artifact_root: Path,
    bootstrap_servers: str,
    service_image_evidence_path: Path,
    config_path: Path = DEFAULT_R2D_B2A_CONFIG_PATH,
    project_root: Path = PROJECT_ROOT,
) -> R2DB2AVerificationResult:
    """Prove live poison isolation and one governed manual redrive on fresh topics."""
    from nexus.ingestion.r2d_b2_service import (
        R2DB2AProduceItem,
        R2DB2ARuntime,
        R2DB2AService,
    )

    output = artifact_root.resolve()
    output.mkdir(parents=True, exist_ok=True)
    config = load_r2d_b2a_config(config_path)
    service_image = load_r2d_b2a_service_image(service_image_evidence_path)
    service_image_file_sha256 = file_sha256(service_image_evidence_path)
    b1_config = load_r2d_b1_config(project_root / config.inherited_b1.config_path)
    data_config = load_r2d_config(project_root / b1_config.data_contract.r2d_a1_config_path)
    if data_config.topic_contract.topic != config.broker.source_topic:
        raise R2DB2AGovernanceError("B2a source topic differs from inherited R2D-A/B1 authority")
    service = R2DB2AService(
        config=config,
        runtime=R2DB2ARuntime(bootstrap_servers=bootstrap_servers),
    )
    service.provision_topics()
    fresh = service.require_fresh_empty_topics()

    compatible_id, compatible_key = _key_for_partition(0)
    poison_id, poison_key = _key_for_partition(1)
    compatible = _event_bytes(
        _event(
            logical_event_id=compatible_id,
            event_id="20000000-0000-4000-8000-000000000001",
            idempotency_key="idem-ADJ-B2A-COMPATIBLE",
            event_version=1,
        )
    )
    poison = _event_bytes(
        _event(
            logical_event_id=poison_id,
            event_id="20000000-0000-4000-8000-000000000002",
            idempotency_key="idem-ADJ-B2A-POISON",
            event_version=2,
        )
    )
    source_ack = service.produce(
        phase="source_fixture",
        topic=config.broker.source_topic,
        items=(
            R2DB2AProduceItem(
                key=compatible_key,
                value=compatible,
                partition=0,
                timestamp_ms=1_781_524_800_000,
                headers=_source_headers(data_config, compatible_key),
            ),
            R2DB2AProduceItem(
                key=poison_key,
                value=poison,
                partition=1,
                timestamp_ms=1_781_524_800_001,
                headers=_source_headers(data_config, poison_key),
            ),
        ),
    )
    source_lows, source_ends = service.topic_watermarks(config.broker.source_topic)
    source_records = service.consume_range(
        topic=config.broker.source_topic,
        group_id=config.broker.router_consumer_group_id,
        starts=source_lows,
        ends=source_ends,
    )
    route_rows: list[dict[str, object]] = []
    dead_letters: list[R2DB1DeadLetterRecord] = []
    for record in source_records:
        captured = capture_transcript((record,), data_config).records[0]
        if record.value_bytes is None:
            raise R2DB2AGovernanceError("B2a source fixture unexpectedly contained a tombstone")
        decision = classify_schema_value(record.value_bytes, b1_config, project_root=project_root)
        route_rows.append(
            {
                "partition": record.partition,
                "offset": record.offset,
                "physical_fingerprint": captured.physical_fingerprint,
                "schema_decision": decision.model_dump(mode="json"),
            }
        )
        if decision.disposition == "dead_letter":
            dead_letters.append(
                build_dead_letter_record(captured, decision, b1_config, data_config)
            )
    if len(source_records) != 2 or len(dead_letters) != 1:
        raise R2DB2AGovernanceError(
            "B2a source routing did not produce the exact 1-forward/1-DLQ split"
        )
    if sum(row["schema_decision"]["disposition"] == "forward" for row in route_rows) != 1:  # type: ignore[index]
        raise R2DB2AGovernanceError("B2a source routing did not preserve one compatible record")

    dead_letter = dead_letters[0]
    dead_letter_bytes = canonical_json_bytes(dead_letter.model_dump(mode="json"))
    dlq_key = dead_letter.dead_letter_id.encode()
    dlq_partition = partition_for_key(dlq_key, partition_count=2)
    dead_letter_ack = service.produce(
        phase="dead_letter",
        topic=config.broker.dead_letter_topic,
        items=(
            R2DB2AProduceItem(
                key=dlq_key,
                value=dead_letter_bytes,
                partition=dlq_partition,
                timestamp_ms=1_781_524_800_100,
                headers=_governance_headers(
                    dead_letter.contract_version, dead_letter.original_physical_fingerprint
                ),
            ),
        ),
    )
    dlq_lows, dlq_ends = service.topic_watermarks(config.broker.dead_letter_topic)
    dlq_records = service.consume_range(
        topic=config.broker.dead_letter_topic,
        group_id=config.broker.dead_letter_audit_consumer_group_id,
        starts=dlq_lows,
        ends=dlq_ends,
    )
    if len(dlq_records) != 1 or dlq_records[0].value_bytes != dead_letter_bytes:
        raise R2DB2AGovernanceError("B2a DLQ did not preserve the exact dead-letter bytes")
    observed_dead_letter = R2DB1DeadLetterRecord.model_validate_json(dead_letter_bytes)
    if observed_dead_letter != dead_letter:
        raise R2DB2AGovernanceError("B2a DLQ round trip changed governed dead-letter evidence")

    replacement = _event_bytes(
        _event(
            logical_event_id=poison_id,
            event_id="20000000-0000-4000-8000-000000000003",
            idempotency_key="idem-ADJ-B2A-REDRIVE-001",
            event_version=1,
        )
    )
    redrive = create_redrive_request(
        dead_letter,
        replacement,
        operator_id=config.operator.operator_id,
        reason=config.operator.reason,
        submitted_at=config.operator.submitted_at,
        config=b1_config,
    )
    validate_redrive_ledger((redrive,), (dead_letter,), b1_config)
    redrive_bytes = canonical_json_bytes(redrive.model_dump(mode="json"))
    redrive_key = redrive.redrive_id.encode()
    redrive_partition = partition_for_key(redrive_key, partition_count=2)
    redrive_ack = service.produce(
        phase="redrive_audit",
        topic=config.broker.redrive_audit_topic,
        items=(
            R2DB2AProduceItem(
                key=redrive_key,
                value=redrive_bytes,
                partition=redrive_partition,
                timestamp_ms=1_781_524_800_200,
                headers=_governance_headers(redrive.contract_version, dead_letter.dead_letter_id),
            ),
        ),
    )
    redrive_lows, redrive_ends = service.topic_watermarks(config.broker.redrive_audit_topic)
    redrive_records = service.consume_range(
        topic=config.broker.redrive_audit_topic,
        group_id=config.broker.redrive_audit_consumer_group_id,
        starts=redrive_lows,
        ends=redrive_ends,
    )
    if len(redrive_records) != 1 or redrive_records[0].value_bytes != redrive_bytes:
        raise R2DB2AGovernanceError("B2a redrive audit topic changed the append-only request")
    observed_redrive = R2DB1RedriveRequest.model_validate_json(redrive_bytes)
    validate_redrive_ledger((observed_redrive,), (observed_dead_letter,), b1_config)

    replacement_ack = service.produce(
        phase="replacement",
        topic=config.broker.source_topic,
        items=(
            R2DB2AProduceItem(
                key=poison_key,
                value=replacement,
                partition=1,
                timestamp_ms=1_781_524_800_300,
                headers=_source_headers(data_config, poison_key),
            ),
        ),
    )
    _, replacement_ends = service.topic_watermarks(config.broker.source_topic)
    replacement_records = service.consume_range(
        topic=config.broker.source_topic,
        group_id=config.broker.router_consumer_group_id,
        starts=source_ends,
        ends=replacement_ends,
    )
    if len(replacement_records) != 1 or replacement_records[0].value_bytes != replacement:
        raise R2DB2AGovernanceError("B2a replacement did not return to the source topic exactly")
    replacement_decision = classify_schema_value(replacement, b1_config, project_root=project_root)
    if replacement_decision.disposition != "forward":
        raise R2DB2AGovernanceError("B2a replacement no longer passes current B1 authority")

    immutable_records = service.consume_range(
        topic=config.broker.dead_letter_topic,
        group_id="nexus-r2d-b2a-dlq-immutability-v1",
        starts=dlq_lows,
        ends=dlq_ends,
    )
    _, dlq_ends_after = service.topic_watermarks(config.broker.dead_letter_topic)
    if (
        len(immutable_records) != 1
        or immutable_records[0].value_bytes != dead_letter_bytes
        or dlq_ends_after != dlq_ends
    ):
        raise R2DB2AGovernanceError("B2a redrive mutated or appended to original DLQ evidence")

    committed = {
        "router_source": list(
            service.committed_offsets(
                topic=config.broker.source_topic,
                group_id=config.broker.router_consumer_group_id,
            )
        ),
        "dead_letter_audit": list(
            service.committed_offsets(
                topic=config.broker.dead_letter_topic,
                group_id=config.broker.dead_letter_audit_consumer_group_id,
            )
        ),
        "redrive_audit": list(
            service.committed_offsets(
                topic=config.broker.redrive_audit_topic,
                group_id=config.broker.redrive_audit_consumer_group_id,
            )
        ),
    }
    expected_committed = {
        "router_source": list(replacement_ends),
        "dead_letter_audit": list(dlq_ends),
        "redrive_audit": list(redrive_ends),
    }
    if committed != expected_committed:
        raise R2DB2AGovernanceError("B2a manual commits did not converge to frozen end offsets")

    payloads: dict[str, dict[str, object]] = {
        "inherited_b1": {
            "contract_version": "r2d.b2a.inherited-b1.v1",
            "config_path": config.inherited_b1.config_path,
            "config_sha256": r2d_b1_config_sha256(b1_config),
            "business_validation_owner": b1_config.schema_policy.business_validation_owner,
        },
        "service_image": service_image.model_dump(mode="json"),
        "topic_topology": {
            "contract_version": "r2d.b2a.topic-topology.v1",
            "topics": list(config.broker.topics),
            "partition_count": config.broker.partition_count,
            "replication_factor": config.broker.replication_factor,
            "retention_ms": config.broker.retention_ms,
            "fresh_empty_offsets": fresh,
            "final_end_offsets": {
                config.broker.source_topic: list(replacement_ends),
                config.broker.dead_letter_topic: list(dlq_ends),
                config.broker.redrive_audit_topic: list(redrive_ends),
            },
        },
        "source_ack": source_ack.model_dump(mode="json"),
        "route": {
            "contract_version": "r2d.b2a.route.v1",
            "records": route_rows,
            "forward_count": 1,
            "dead_letter_count": 1,
            "dead_letters": [dead_letter.model_dump(mode="json")],
        },
        "dead_letter_ack": dead_letter_ack.model_dump(mode="json"),
        "dead_letter_round_trip": {
            "contract_version": "r2d.b2a.dead-letter-round-trip.v1",
            "dead_letter_id": dead_letter.dead_letter_id,
            "dead_letter_content_sha256": dead_letter.content_sha256,
            "published_value_sha256": hashlib.sha256(dead_letter_bytes).hexdigest(),
            "observed_value_sha256": hashlib.sha256(dlq_records[0].value_bytes or b"").hexdigest(),
            "post_redrive_value_sha256": hashlib.sha256(
                immutable_records[0].value_bytes or b""
            ).hexdigest(),
            "end_offsets_before_redrive": list(dlq_ends),
            "end_offsets_after_redrive": list(dlq_ends_after),
        },
        "redrive_ack": redrive_ack.model_dump(mode="json"),
        "redrive": {
            "contract_version": "r2d.b2a.redrive.v1",
            "request": redrive.model_dump(mode="json"),
            "audit_value_sha256": hashlib.sha256(redrive_bytes).hexdigest(),
            "automatic_redrive": False,
        },
        "replacement_ack": {
            **replacement_ack.model_dump(mode="json"),
            "replacement_schema_decision": replacement_decision.model_dump(mode="json"),
        },
        "manual_commit": {
            "contract_version": "r2d.b2a.manual-commit.v1",
            "committed_offsets": committed,
            "expected_end_offsets": expected_committed,
            "auto_offset_commit": False,
            "auto_offset_store": False,
        },
        "scope": {
            "contract_version": "r2d.b2a.scope.v1",
            **config.scope.model_dump(mode="json"),
            "automatic_redrive": False,
            "exactly_once_claim": False,
        },
    }
    evidence_refs = _publish_evidence(output, payloads)
    artifact_set_sha256 = canonical_json_sha256(
        [item.model_dump(mode="json") for item in evidence_refs]
    )
    evidence_sha = {name: canonical_json_sha256(payload) for name, payload in payloads.items()}
    check_evidence = {
        "inherited_b1_authority": config.inherited_b1.config_sha256,
        "exact_service_topic_topology": evidence_sha["topic_topology"],
        "observed_service_image": service_image.content_sha256,
        "real_source_acks_all": source_ack.content_sha256,
        "live_poison_isolation": evidence_sha["route"],
        "byte_preserving_dlq_round_trip": evidence_sha["dead_letter_round_trip"],
        "append_only_operator_redrive": evidence_sha["redrive"],
        "replacement_returns_to_source": evidence_sha["replacement_ack"],
        "original_dlq_immutability": evidence_sha["dead_letter_round_trip"],
        "manual_commit_convergence": evidence_sha["manual_commit"],
        "immutable_artifact_set": artifact_set_sha256,
        "honest_scope": evidence_sha["scope"],
    }
    receipt_payload: dict[str, object] = {
        "contract_version": "r2d.b2a.verification-receipt.v1",
        "release": "R2D-B2a",
        "status": "PASS",
        "checks": [
            {"name": name, "passed": True, "evidence_sha256": check_evidence[name]}
            for name in R2D_B2A_PASS_CHECK_NAMES
        ],
        "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        "config_sha256": r2d_b2a_config_sha256(config),
        "code_sha256": r2d_b2a_code_sha256(project_root),
        "inherited_b1_config_sha256": config.inherited_b1.config_sha256,
        "topology_sha256": evidence_sha["topic_topology"],
        "service_image_sha256": service_image.content_sha256,
        "service_image_file_sha256": service_image_file_sha256,
        "source_ack_sha256": source_ack.content_sha256,
        "route_evidence_sha256": evidence_sha["route"],
        "dead_letter_ack_sha256": dead_letter_ack.content_sha256,
        "dead_letter_round_trip_sha256": evidence_sha["dead_letter_round_trip"],
        "redrive_ack_sha256": redrive_ack.content_sha256,
        "redrive_evidence_sha256": evidence_sha["redrive"],
        "replacement_ack_sha256": evidence_sha["replacement_ack"],
        "manual_commit_sha256": evidence_sha["manual_commit"],
        "artifact_set_sha256": artifact_set_sha256,
        "scope_sha256": evidence_sha["scope"],
        "source_record_count": 2,
        "forward_record_count": 1,
        "dead_letter_record_count": 1,
        "redrive_request_count": 1,
        "replacement_record_count": 1,
        "runtime": _runtime(),
        **config.scope.model_dump(mode="json"),
        "exactly_once_claim": False,
    }
    receipt = R2DB2AVerificationReceipt.model_validate(
        {**receipt_payload, "content_sha256": canonical_json_sha256(receipt_payload)}
    )
    receipt_artifact = write_immutable_json_manifest(
        output,
        dataset="r2d.b2a.verification_receipt",
        manifest=receipt.model_dump(mode="json"),
        schema_identity={"contract": receipt.contract_version},
    )
    load_r2d_b2a_receipt(receipt_artifact.path)
    return R2DB2AVerificationResult(receipt=receipt, receipt_artifact=receipt_artifact)


def load_r2d_b2a_receipt(path: Path) -> R2DB2AVerificationReceipt:
    try:
        raw = path.read_bytes()
        parsed = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2DB2AGovernanceError(f"cannot load B2a receipt: {path}") from exc
    if raw != canonical_json_bytes(parsed) + b"\n":
        raise R2DB2AGovernanceError("B2a receipt is not canonical JSON")
    receipt = R2DB2AVerificationReceipt.model_validate(parsed)
    root = path.resolve().parents[3]
    payloads: dict[str, dict[str, object]] = {}
    for reference in receipt.evidence_refs:
        artifact = (root / reference.relative_path).resolve()
        if not artifact.is_relative_to(root) or not artifact.is_file():
            raise R2DB2AGovernanceError("B2a evidence artifact is missing or escapes its root")
        try:
            artifact_raw = artifact.read_bytes()
            artifact_payload = json.loads(artifact_raw)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise R2DB2AGovernanceError("B2a evidence artifact is unreadable") from exc
        if not isinstance(artifact_payload, dict):
            raise R2DB2AGovernanceError("B2a evidence artifact must be a JSON object")
        payloads[reference.dataset] = artifact_payload

        if artifact_raw != canonical_json_bytes(artifact_payload) + b"\n":
            raise R2DB2AGovernanceError("B2a evidence artifact is not canonical JSON")
        if canonical_json_sha256(artifact_payload) != reference.content_sha256:
            raise R2DB2AGovernanceError("B2a evidence artifact content identity mismatch")
        if file_sha256(artifact) != reference.file_sha256:
            raise R2DB2AGovernanceError("B2a evidence artifact file identity mismatch")
        contract = artifact_payload.get("contract_version")
        if canonical_json_sha256({"contract": contract}) != reference.schema_sha256:
            raise R2DB2AGovernanceError("B2a evidence artifact schema identity mismatch")
        if reference.dataset == "r2d.b2a.service_image":
            observed_image = R2DB2AServiceImageEvidence.model_validate(artifact_payload)
            if observed_image.content_sha256 != receipt.service_image_sha256:
                raise R2DB2AGovernanceError("B2a service-image evidence binding mismatch")
    current_config = load_r2d_b2a_config()
    if receipt.config_sha256 != r2d_b2a_config_sha256(current_config):
        raise R2DB2AGovernanceError("B2a receipt differs from the current config")
    if receipt.code_sha256 != r2d_b2a_code_sha256(PROJECT_ROOT):
        raise R2DB2AGovernanceError("B2a receipt differs from the current code")
    inherited = payloads["r2d.b2a.inherited_b1"]
    if inherited.get("config_sha256") != receipt.inherited_b1_config_sha256:
        raise R2DB2AGovernanceError("B2a inherited authority is detached from its artifact")
    for dataset, expected in (
        ("r2d.b2a.source_ack", receipt.source_ack_sha256),
        ("r2d.b2a.dead_letter_ack", receipt.dead_letter_ack_sha256),
        ("r2d.b2a.redrive_ack", receipt.redrive_ack_sha256),
    ):
        ledger = R2DB2AAckLedger.model_validate(payloads[dataset])
        if ledger.content_sha256 != expected:
            raise R2DB2AGovernanceError("B2a broker acknowledgement is detached from receipt")
    by_dataset = {item.dataset: item for item in receipt.evidence_refs}
    for dataset, expected in (
        ("r2d.b2a.topic_topology", receipt.topology_sha256),
        ("r2d.b2a.route", receipt.route_evidence_sha256),
        ("r2d.b2a.dead_letter_round_trip", receipt.dead_letter_round_trip_sha256),
        ("r2d.b2a.redrive", receipt.redrive_evidence_sha256),
        ("r2d.b2a.replacement_ack", receipt.replacement_ack_sha256),
        ("r2d.b2a.manual_commit", receipt.manual_commit_sha256),
        ("r2d.b2a.scope", receipt.scope_sha256),
    ):
        if by_dataset[dataset].content_sha256 != expected:
            raise R2DB2AGovernanceError("B2a receipt is detached from governed evidence")
    return receipt


def dump_r2d_b2a_result(result: R2DB2AVerificationResult) -> str:
    return json.dumps(
        {
            "checks": [item.model_dump(mode="json") for item in result.receipt.checks],
            "full_g2_status": result.receipt.full_g2_status,
            "full_r2d_b_status": result.receipt.full_r2d_b_status,
            "persistent_dagster_status": result.receipt.persistent_dagster_status,
            "receipt": str(result.receipt_artifact.path),
            "retained_service_restart_status": result.receipt.retained_service_restart_status,
            "service_dead_letter_topic_status": result.receipt.service_dead_letter_topic_status,
            "status": result.receipt.status,
        },
        sort_keys=True,
    )
