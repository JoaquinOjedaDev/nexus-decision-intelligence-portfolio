"""Bounded portable verification gate for R2D-B1 governed poison handling."""

from __future__ import annotations

import hashlib
import json
import os
import platform
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from pydantic import ValidationError

from nexus.ingestion.r2d_b1 import (
    R2DB1GovernanceError,
    assess_backward_compatibility,
    build_dead_letter_record,
    classify_schema_value,
    create_redrive_request,
    validate_redrive_ledger,
)
from nexus.ingestion.r2d_b1_contracts import (
    DEFAULT_R2D_B1_CONFIG_PATH,
    PROJECT_ROOT,
    R2D_B1_PASS_CHECK_NAMES,
    R2DB1Config,
    R2DB1DeadLetterRecord,
    R2DB1RedriveRequest,
    R2DB1VerificationReceipt,
    load_r2d_b1_config,
    r2d_b1_config_sha256,
)
from nexus.ingestion.r2d_capture import capture_transcript, partition_for_key
from nexus.ingestion.r2d_contracts import (
    R2DCapturedRecord,
    R2DConfig,
    R2DPhysicalRecord,
    load_r2d_config,
    r2d_config_sha256,
    r2d_lf_canonical_file_sha256,
)
from nexus.modeling.r3_artifacts import (
    ScientificArtifactRef,
    canonical_json_bytes,
    canonical_json_sha256,
    write_immutable_json_manifest,
)
from nexus.storage import file_sha256

_CODE_FILES = (
    "configs/ingestion/r2d-b1.yaml",
    "contracts/schemas/event-envelope.schema.json",
    "contracts/schemas/r2-inventory-adjustment-payload.schema.json",
    "src/nexus/ingestion/r2d_b1_contracts.py",
    "src/nexus/ingestion/r2d_b1.py",
    "src/nexus/r2d_b1.py",
    "src/nexus/cli.py",
)


@dataclass(frozen=True, slots=True)
class R2DB1RunEvidence:
    artifacts: tuple[ScientificArtifactRef, ...]
    payloads: dict[str, dict[str, object]]
    normalized_semantics_sha256: str
    artifact_set_sha256: str


@dataclass(frozen=True, slots=True)
class R2DB1VerificationResult:
    receipt: R2DB1VerificationReceipt
    receipt_artifact: ScientificArtifactRef

    @property
    def passed(self) -> bool:
        return self.receipt.status == "PASS"


def r2d_b1_code_sha256(project_root: Path = PROJECT_ROOT) -> str:
    """Bind the exact checked-in text that implements and configures B1."""

    return canonical_json_sha256(
        {
            "contract_version": "r2d.b1.code-bundle.v1",
            "files": {
                relative: r2d_lf_canonical_file_sha256(project_root / relative)
                for relative in _CODE_FILES
            },
        }
    )


def _base_event() -> dict[str, Any]:
    return {
        "contract_version": "1.0",
        "event_id": "10000000-0000-4000-8000-000000000001",
        "event_type": "inventory.adjusted",
        "event_version": 1,
        "source": "synthetic.wms",
        "subject_id": "SKU-001:DC-ARG-01",
        "occurred_at": "2026-06-01T00:00:00Z",
        "ingested_at": "2026-06-01T00:00:01Z",
        "idempotency_key": "idem-ADJ-B1-001",
        "payload": {
            "logical_event_id": "ADJ-B1-001",
            "revision": 1,
            "sku_id": "SKU-001",
            "location_id": "DC-ARG-01",
            "delta_qty": 1,
            "reason_code": "cycle_count",
        },
    }


def _event_bytes(event: dict[str, Any]) -> bytes:
    return json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _captured(raw_value: bytes, offset: int, data_config: R2DConfig) -> R2DCapturedRecord:
    key = b"synthetic.wms:ADJ-B1-001"
    record = R2DPhysicalRecord.from_bytes(
        topic=data_config.topic_contract.topic,
        partition=partition_for_key(key),
        offset=offset,
        key=key,
        headers=(
            (
                data_config.header_contract.contract_header_name,
                data_config.header_contract.contract_header_value.encode("utf-8"),
            ),
            (
                data_config.header_contract.schema_header_name,
                data_config.header_contract.schema_header_value.encode("ascii"),
            ),
            (data_config.header_contract.routing_header_name, key),
        ),
        broker_timestamp_ms=1_780_272_001_000 + offset,
        broker_timestamp_type="create_time",
        value=raw_value,
    )
    return capture_transcript((record,), data_config).records[0]


def _classification_cases() -> tuple[tuple[str, bytes, str, str], ...]:
    base = _base_event()
    defaulted_business_invalid = deepcopy(base)
    defaulted_business_invalid.pop("contract_version")
    defaulted_business_invalid.pop("event_version")
    defaulted_business_invalid["subject_id"] = "WRONG:SUBJECT"
    payload_extra = deepcopy(base)
    payload_extra["payload"]["unknown_field"] = "poison"
    return (
        ("valid", _event_bytes(base), "forward", "compatible"),
        (
            "business_invalid_schema_valid",
            _event_bytes(defaulted_business_invalid),
            "forward",
            "compatible",
        ),
        ("invalid_utf8", b"\xff", "dead_letter", "invalid_utf8"),
        ("invalid_json", b'{"contract_version":', "dead_letter", "invalid_json"),
        (
            "unsupported_contract",
            _event_bytes({**base, "contract_version": "2.0"}),
            "dead_letter",
            "unsupported_contract_version",
        ),
        (
            "unsupported_type",
            _event_bytes({**base, "event_type": "inventory.deleted"}),
            "dead_letter",
            "unsupported_event_type",
        ),
        (
            "unsupported_version",
            _event_bytes({**base, "event_version": 2}),
            "dead_letter",
            "unsupported_event_version",
        ),
        (
            "invalid_envelope",
            _event_bytes({**base, "event_id": "not-a-uuid"}),
            "dead_letter",
            "envelope_schema_invalid",
        ),
        (
            "invalid_payload",
            _event_bytes(payload_extra),
            "dead_letter",
            "payload_schema_invalid",
        ),
    )


def _load_schema(project_root: Path, relative: str) -> dict[str, object]:
    payload = json.loads((project_root / relative).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise R2DB1GovernanceError(f"schema is not an object: {relative}")
    return payload


def _tamper_matrix(
    dead_letter: R2DB1DeadLetterRecord,
    redrive: R2DB1RedriveRequest,
    config: R2DB1Config,
) -> dict[str, bool]:
    results: dict[str, bool] = {}
    tampered_dead_letter = dead_letter.model_dump(mode="json")
    tampered_dead_letter["original_raw_value_hex"] = b"changed".hex()
    try:
        R2DB1DeadLetterRecord.model_validate_json(json.dumps(tampered_dead_letter))
    except ValidationError:
        results["dead_letter_byte_tamper_rejected"] = True
    else:
        results["dead_letter_byte_tamper_rejected"] = False

    detached = redrive.model_copy(update={"dead_letter_content_sha256": "0" * 64})
    try:
        validate_redrive_ledger((detached,), (dead_letter,), config)
    except (ValidationError, R2DB1GovernanceError):
        results["redrive_evidence_rebind_rejected"] = True
    else:
        results["redrive_evidence_rebind_rejected"] = False

    try:
        validate_redrive_ledger((redrive, redrive), (dead_letter,), config)
    except R2DB1GovernanceError:
        results["duplicate_redrive_rejected"] = True
    else:
        results["duplicate_redrive_rejected"] = False
    return results


def _semantic_payloads(
    *,
    config: R2DB1Config,
    data_config: R2DConfig,
    project_root: Path,
) -> dict[str, dict[str, object]]:
    baseline = _load_schema(project_root, config.schema_policy.payload_schema_path)
    compatible_candidate = deepcopy(baseline)
    compatible_candidate["$id"] = (
        "https://nexus.local/contracts/r2-inventory-adjustment-payload/1.1"
    )
    compatible_properties = cast(dict[str, object], compatible_candidate["properties"])
    compatible_properties["operator_note"] = {"type": ["string", "null"]}
    breaking_candidate = deepcopy(compatible_candidate)
    breaking_candidate["required"] = [
        *cast(list[object], breaking_candidate["required"]),
        "operator_note",
    ]
    compatible_assessment = assess_backward_compatibility(baseline, compatible_candidate)
    breaking_assessment = assess_backward_compatibility(baseline, breaking_candidate)
    if not compatible_assessment.compatible or breaking_assessment.compatible:
        raise R2DB1GovernanceError("frozen schema compatibility matrix did not hold")

    classification_rows: list[dict[str, object]] = []
    dead_letters: list[R2DB1DeadLetterRecord] = []
    version_dead_letter: R2DB1DeadLetterRecord | None = None
    for offset, (name, raw_value, expected_disposition, expected_reason) in enumerate(
        _classification_cases()
    ):
        decision = classify_schema_value(raw_value, config, project_root=project_root)
        if decision.disposition != expected_disposition or decision.reason != expected_reason:
            raise R2DB1GovernanceError(f"classification matrix changed for {name}")
        classification_rows.append(
            {
                "case": name,
                "decision": decision.model_dump(mode="json"),
                "expected_disposition": expected_disposition,
                "expected_reason": expected_reason,
            }
        )
        captured = _captured(raw_value, offset, data_config)
        if captured.decision.disposition == "admitted" and decision.disposition == "dead_letter":
            dead_letter = build_dead_letter_record(captured, decision, config, data_config)
            dead_letters.append(dead_letter)
            if name == "unsupported_version":
                version_dead_letter = dead_letter
    if version_dead_letter is None:
        raise R2DB1GovernanceError("version poison did not produce dead-letter evidence")

    replacement_event = _base_event()
    replacement_event["event_id"] = "20000000-0000-4000-8000-000000000002"
    replacement_event["idempotency_key"] = "idem-ADJ-B1-REDRIVE-001"
    replacement = _event_bytes(replacement_event)
    redrive = create_redrive_request(
        version_dead_letter,
        replacement,
        operator_id="operator@example.com",
        reason="Correct unsupported event version after source-owner approval",
        submitted_at=datetime(2026, 8, 11, 20, 0, tzinfo=UTC),
        config=config,
    )
    validate_redrive_ledger((redrive,), tuple(dead_letters), config)
    tamper_matrix = _tamper_matrix(version_dead_letter, redrive, config)
    if not all(tamper_matrix.values()):
        raise R2DB1GovernanceError("one or more B1 tamper checks did not fail closed")

    contract_payload: dict[str, object] = {
        "contract_version": "r2d.b1.contract-evidence.v1",
        "b1_config_sha256": r2d_b1_config_sha256(config),
        "r2d_a1_config_sha256": r2d_config_sha256(data_config),
        "schema_bundle_sha256": config.schema_policy.schema_bundle_sha256,
        "business_validation_owner": config.schema_policy.business_validation_owner,
    }
    compatibility_payload: dict[str, object] = {
        "contract_version": "r2d.b1.compatibility-matrix.v1",
        "accepted": compatible_assessment.model_dump(mode="json"),
        "rejected": breaking_assessment.model_dump(mode="json"),
    }
    classification_payload: dict[str, object] = {
        "contract_version": "r2d.b1.classification-matrix.v1",
        "rows": classification_rows,
    }
    dead_letter_payload: dict[str, object] = {
        "contract_version": "r2d.b1.dead-letter-set.v1",
        "records": [item.model_dump(mode="json") for item in dead_letters],
    }
    redrive_payload: dict[str, object] = {
        "contract_version": "r2d.b1.redrive-ledger.v1",
        "requests": [redrive.model_dump(mode="json")],
    }
    tamper_payload: dict[str, object] = {
        "contract_version": "r2d.b1.tamper-matrix.v1",
        "checks": tamper_matrix,
    }
    scope_payload: dict[str, object] = {
        "contract_version": "r2d.b1.scope.v1",
        **config.scope.model_dump(mode="json"),
        "full_r2d_b_status": "IN_PROGRESS",
    }
    normalized_payload: dict[str, object] = {
        "contract_version": "r2d.b1.normalized-semantics.v1",
        "contract_evidence_sha256": canonical_json_sha256(contract_payload),
        "compatibility_assessment_sha256": canonical_json_sha256(compatibility_payload),
        "classification_matrix_sha256": canonical_json_sha256(classification_payload),
        "dead_letter_set_sha256": canonical_json_sha256(dead_letter_payload),
        "redrive_ledger_sha256": canonical_json_sha256(redrive_payload),
        "tamper_matrix_sha256": canonical_json_sha256(tamper_payload),
        "scope_sha256": canonical_json_sha256(scope_payload),
    }
    return {
        "contract": contract_payload,
        "compatibility": compatibility_payload,
        "classification": classification_payload,
        "dead_letter": dead_letter_payload,
        "redrive": redrive_payload,
        "tamper": tamper_payload,
        "scope": scope_payload,
        "normalized": normalized_payload,
    }


def _run_once(
    *,
    output_dir: Path,
    config: R2DB1Config,
    data_config: R2DConfig,
    project_root: Path,
) -> R2DB1RunEvidence:
    payloads = _semantic_payloads(
        config=config,
        data_config=data_config,
        project_root=project_root,
    )
    artifacts = tuple(
        write_immutable_json_manifest(
            output_dir,
            dataset=f"r2d.b1.{name}",
            manifest=payload,
            schema_identity={"contract": payload["contract_version"]},
        )
        for name, payload in payloads.items()
    )
    artifact_set_sha256 = canonical_json_sha256(
        [
            {
                "content_sha256": item.content_sha256,
                "dataset": item.dataset,
                "file_sha256": item.file_sha256,
                "row_count": item.row_count,
                "schema_sha256": item.schema_sha256,
            }
            for item in sorted(artifacts, key=lambda ref: ref.dataset)
        ]
    )
    return R2DB1RunEvidence(
        artifacts=artifacts,
        payloads=payloads,
        normalized_semantics_sha256=canonical_json_sha256(payloads["normalized"]),
        artifact_set_sha256=artifact_set_sha256,
    )


def _expected_semantic_receipt_fields(
    *,
    config: R2DB1Config,
    data_config: R2DConfig,
    project_root: Path,
) -> dict[str, str]:
    """Independently rederive every portable semantic and artifact-set identity."""

    payloads = _semantic_payloads(
        config=config,
        data_config=data_config,
        project_root=project_root,
    )
    evidence_hashes = {name: canonical_json_sha256(payload) for name, payload in payloads.items()}
    artifact_rows = []
    for name, payload in payloads.items():
        canonical = canonical_json_bytes(payload)
        artifact_rows.append(
            {
                "content_sha256": evidence_hashes[name],
                "dataset": f"r2d.b1.{name}",
                "file_sha256": hashlib.sha256(canonical + b"\n").hexdigest(),
                "row_count": 1,
                "schema_sha256": canonical_json_sha256({"contract": payload["contract_version"]}),
            }
        )
    return {
        "config_sha256": r2d_b1_config_sha256(config),
        "code_sha256": r2d_b1_code_sha256(project_root),
        "contract_evidence_sha256": evidence_hashes["contract"],
        "compatibility_assessment_sha256": evidence_hashes["compatibility"],
        "classification_matrix_sha256": evidence_hashes["classification"],
        "dead_letter_set_sha256": evidence_hashes["dead_letter"],
        "redrive_ledger_sha256": evidence_hashes["redrive"],
        "tamper_matrix_sha256": evidence_hashes["tamper"],
        "normalized_semantics_sha256": evidence_hashes["normalized"],
        "artifact_set_sha256": canonical_json_sha256(
            sorted(artifact_rows, key=lambda item: str(item["dataset"]))
        ),
        "scope_sha256": evidence_hashes["scope"],
    }


def _runtime() -> dict[str, object]:
    runner_os = os.getenv("RUNNER_OS")
    return {
        "operating_system": runner_os or platform.system(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "github_runner_os_attested": os.getenv("GITHUB_ACTIONS") == "true" and bool(runner_os),
    }


def verify_r2d_b1(
    *,
    artifact_root: Path,
    config_path: Path = DEFAULT_R2D_B1_CONFIG_PATH,
    project_root: Path = PROJECT_ROOT,
) -> R2DB1VerificationResult:
    """Execute two deterministic replays and publish the strict B1 receipt."""

    config = load_r2d_b1_config(config_path)
    data_config = load_r2d_config(project_root / config.data_contract.r2d_a1_config_path)
    output = artifact_root.resolve()
    output.mkdir(parents=True, exist_ok=True)
    first = _run_once(
        output_dir=output / "run-a",
        config=config,
        data_config=data_config,
        project_root=project_root,
    )
    second = _run_once(
        output_dir=output / "run-b",
        config=config,
        data_config=data_config,
        project_root=project_root,
    )
    if (
        first.normalized_semantics_sha256 != second.normalized_semantics_sha256
        or first.artifact_set_sha256 != second.artifact_set_sha256
        or first.payloads != second.payloads
    ):
        raise R2DB1GovernanceError("two fresh R2D-B1 replays are not deterministic")

    expected_fields = _expected_semantic_receipt_fields(
        config=config,
        data_config=data_config,
        project_root=project_root,
    )
    if (
        first.normalized_semantics_sha256 != expected_fields["normalized_semantics_sha256"]
        or first.artifact_set_sha256 != expected_fields["artifact_set_sha256"]
    ):
        raise R2DB1GovernanceError("published B1 evidence differs from independent derivation")
    check_evidence = {
        "inherited_contract_integrity": expected_fields["contract_evidence_sha256"],
        "conservative_schema_compatibility": expected_fields["compatibility_assessment_sha256"],
        "poison_isolation_matrix": expected_fields["classification_matrix_sha256"],
        "business_validation_boundary": expected_fields["classification_matrix_sha256"],
        "dead_letter_byte_preservation": expected_fields["dead_letter_set_sha256"],
        "append_only_operator_redrive": expected_fields["redrive_ledger_sha256"],
        "original_evidence_immutability": expected_fields["dead_letter_set_sha256"],
        "tamper_rejection": expected_fields["tamper_matrix_sha256"],
        "deterministic_two_replay": expected_fields["normalized_semantics_sha256"],
        "immutable_artifact_set": expected_fields["artifact_set_sha256"],
        "honest_scope": expected_fields["scope_sha256"],
    }
    checks = [
        {
            "name": name,
            "passed": True,
            "evidence_sha256": check_evidence[name],
            "detail": f"verified {name.replace('_', ' ')}",
        }
        for name in R2D_B1_PASS_CHECK_NAMES
    ]
    receipt_payload: dict[str, object] = {
        "contract_version": "r2d.b1.verification-receipt.v1",
        "release": "R2D-B1",
        "status": "PASS",
        "checks": checks,
        **expected_fields,
        "runtime": _runtime(),
        "portable_contract_status": "PASS",
        "service_dead_letter_topic_status": "NOT_RUN",
        "persistent_dagster_status": "NOT_RUN",
        "operational_alert_delivery_status": "NOT_RUN",
        "full_r2d_b_status": "IN_PROGRESS",
        "full_g2_status": "OPEN",
    }
    receipt = R2DB1VerificationReceipt.model_validate(
        {**receipt_payload, "content_sha256": canonical_json_sha256(receipt_payload)}
    )
    receipt_artifact = write_immutable_json_manifest(
        output,
        dataset="r2d.b1.verification_receipt",
        manifest=receipt.model_dump(mode="json"),
        schema_identity={"contract": receipt.contract_version},
    )
    return R2DB1VerificationResult(receipt=receipt, receipt_artifact=receipt_artifact)


def load_r2d_b1_receipt(path: Path) -> R2DB1VerificationReceipt:
    """Strictly reload canonical receipt bytes."""

    try:
        raw = path.read_bytes()
        parsed = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2DB1GovernanceError(f"cannot load R2D-B1 receipt: {path}") from exc
    if raw != canonical_json_bytes(parsed) + b"\n":
        raise R2DB1GovernanceError("R2D-B1 receipt bytes are not canonical JSON")
    return R2DB1VerificationReceipt.model_validate_json(raw)


def _verify_r2d_b1_bundle(
    receipt_path: Path,
    receipt: R2DB1VerificationReceipt,
    expected_payloads: dict[str, dict[str, object]],
) -> None:
    """Rehash the exact two-replay bundle surrounding a platform receipt."""

    absolute_receipt = receipt_path.absolute()
    if absolute_receipt.is_symlink():
        raise R2DB1GovernanceError("R2D-B1 receipt path cannot be a symlink")
    expected_tail = ("r2d", "b1", "verification_receipt")
    if tuple(part.lower() for part in absolute_receipt.parent.parts[-3:]) != expected_tail:
        raise R2DB1GovernanceError("R2D-B1 receipt is outside the governed bundle topology")
    bundle_root = absolute_receipt.parents[3]
    if any(path.is_symlink() for path in bundle_root.rglob("*")):
        raise R2DB1GovernanceError("R2D-B1 bundle cannot contain symlinks")

    expected_files: dict[Path, bytes] = {}
    for run_name in ("run-a", "run-b"):
        for name, payload in expected_payloads.items():
            content_sha256 = canonical_json_sha256(payload)
            relative = Path(run_name) / "r2d" / "b1" / name / f"{content_sha256}.json"
            expected_files[relative] = canonical_json_bytes(payload) + b"\n"
    receipt_payload = receipt.model_dump(mode="json")
    receipt_artifact_sha256 = canonical_json_sha256(receipt_payload)
    receipt_relative = (
        Path("r2d") / "b1" / "verification_receipt" / f"{receipt_artifact_sha256}.json"
    )
    expected_files[receipt_relative] = canonical_json_bytes(receipt_payload) + b"\n"

    actual_files = {
        path.relative_to(bundle_root) for path in bundle_root.rglob("*") if path.is_file()
    }
    if actual_files != set(expected_files):
        raise R2DB1GovernanceError("R2D-B1 bundle topology is incomplete or contains extras")
    for relative, expected_bytes in expected_files.items():
        if (bundle_root / relative).read_bytes() != expected_bytes:
            raise R2DB1GovernanceError(f"R2D-B1 bundle artifact mismatch: {relative.as_posix()}")


def compare_r2d_b1_receipts(left: Path, right: Path) -> dict[str, object]:
    """Join two attested platform receipts against current deterministic semantics."""

    receipts = (load_r2d_b1_receipt(left), load_r2d_b1_receipt(right))
    if any(receipt.status != "PASS" for receipt in receipts):
        raise R2DB1GovernanceError("cross-platform comparison requires two PASS receipts")
    systems = {receipt.runtime.operating_system.lower() for receipt in receipts}
    if systems != {"linux", "windows"} or not all(
        receipt.runtime.github_runner_os_attested for receipt in receipts
    ):
        raise R2DB1GovernanceError("cross-platform comparison requires attested Linux and Windows")
    stable_fields = (
        "config_sha256",
        "code_sha256",
        "contract_evidence_sha256",
        "compatibility_assessment_sha256",
        "classification_matrix_sha256",
        "dead_letter_set_sha256",
        "redrive_ledger_sha256",
        "tamper_matrix_sha256",
        "normalized_semantics_sha256",
        "artifact_set_sha256",
        "scope_sha256",
        "portable_contract_status",
        "service_dead_letter_topic_status",
        "persistent_dagster_status",
        "operational_alert_delivery_status",
        "full_r2d_b_status",
        "full_g2_status",
    )
    for field in stable_fields:
        if getattr(receipts[0], field) != getattr(receipts[1], field):
            raise R2DB1GovernanceError(f"R2D-B1 cross-platform field differs: {field}")
    config = load_r2d_b1_config()
    data_config = load_r2d_config(PROJECT_ROOT / config.data_contract.r2d_a1_config_path)
    expected_payloads = _semantic_payloads(
        config=config,
        data_config=data_config,
        project_root=PROJECT_ROOT,
    )
    expected_fields = _expected_semantic_receipt_fields(
        config=config,
        data_config=data_config,
        project_root=PROJECT_ROOT,
    )
    for receipt_path, receipt in zip((left, right), receipts, strict=True):
        _verify_r2d_b1_bundle(receipt_path, receipt, expected_payloads)
    for field, expected_value in expected_fields.items():
        if getattr(receipts[0], field) != expected_value:
            raise R2DB1GovernanceError(f"R2D-B1 receipt differs from current derivation: {field}")
    return {
        "R2D_B1_CROSS_PLATFORM_PASS": True,
        "linux_receipt": str(
            left if receipts[0].runtime.operating_system.lower() == "linux" else right
        ),
        "windows_receipt": str(
            left if receipts[0].runtime.operating_system.lower() == "windows" else right
        ),
        "normalized_semantics_sha256": receipts[0].normalized_semantics_sha256,
    }


def dump_r2d_b1_result(result: R2DB1VerificationResult) -> str:
    """Return the stable CLI summary."""

    return json.dumps(
        {
            "status": result.receipt.status,
            "release": result.receipt.release,
            "checks": [check.model_dump(mode="json") for check in result.receipt.checks],
            "portable_contract_status": result.receipt.portable_contract_status,
            "service_dead_letter_topic_status": result.receipt.service_dead_letter_topic_status,
            "persistent_dagster_status": result.receipt.persistent_dagster_status,
            "operational_alert_delivery_status": (result.receipt.operational_alert_delivery_status),
            "full_r2d_b_status": result.receipt.full_r2d_b_status,
            "full_g2_status": result.receipt.full_g2_status,
            "receipt": str(result.receipt_artifact.path),
            "receipt_file_sha256": file_sha256(result.receipt_artifact.path),
        },
        sort_keys=True,
    )
