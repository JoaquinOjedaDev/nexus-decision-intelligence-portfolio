from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from pydantic import ValidationError

from nexus.ingestion import (
    R2D_A1_PASS_CHECK_NAMES,
    R2D_SOURCE_MANIFEST_SHA256,
    R2DResourceContract,
    R2DVerificationCheck,
    R2DVerificationReceipt,
    R2DVerificationRuntime,
    load_r2d_config,
    r2d_evidence_bindings_sha256,
)

SHA = "a" * 64
NORMALIZED_BUSINESS_SHA = "b" * 64
CONFIG_SHA = "c" * 64
TRANSCRIPT_SHA = "d" * 64
CHECKPOINT_SHA = "e" * 64
PRODUCER_ACK_SHA = "f" * 64
EVIDENCE_CHECKPOINT_SHA = "1" * 64
RESOURCE_SHA = "2" * 64
ARTIFACT_SET_SHA = "3" * 64

CHECK_EVIDENCE = {
    "two_fresh_replay": NORMALIZED_BUSINESS_SHA,
    "config_and_schema_identity": CONFIG_SHA,
    "deterministic_two_partition_transcript": TRANSCRIPT_SHA,
    "transport_admission_and_physical_identity": TRANSCRIPT_SHA,
    "complete_half_open_checkpoint": CHECKPOINT_SHA,
    "raw_value_conservation": PRODUCER_ACK_SHA,
    "manifest_bound_source_reconstruction": EVIDENCE_CHECKPOINT_SHA,
    "public_r2a_semantic_authority": EVIDENCE_CHECKPOINT_SHA,
    "direct_captured_business_parity": NORMALIZED_BUSINESS_SHA,
    "point_in_time_correction_semantics": EVIDENCE_CHECKPOINT_SHA,
    "route_provenance_integrity": EVIDENCE_CHECKPOINT_SHA,
    "bounded_portable_resource_envelope": RESOURCE_SHA,
    "immutable_artifacts": ARTIFACT_SET_SHA,
}


def _runtime() -> R2DVerificationRuntime:
    return R2DVerificationRuntime(
        os_family="linux",
        architecture="x86_64",
        python_minor="3.13",
        github_runner_os_attested=True,
    )


def _checks(*, failed_name: str | None = None) -> tuple[R2DVerificationCheck, ...]:
    return tuple(
        R2DVerificationCheck(
            name=name,
            passed=name != failed_name,
            detail=(
                "two fresh replay semantic hashes match"
                if name == R2D_A1_PASS_CHECK_NAMES[0]
                else f"bounded evidence verified for {name}"
            ),
            evidence_sha256=CHECK_EVIDENCE[name],
        )
        for name in R2D_A1_PASS_CHECK_NAMES
    )


def _receipt_payload(*, status: str = "PASS") -> dict[str, Any]:
    failed_name = None if status == "PASS" else "immutable_artifacts"
    identities = {
        "source_manifest_sha256": R2D_SOURCE_MANIFEST_SHA256,
        "config_sha256": CONFIG_SHA,
        "normalized_business_sha256": NORMALIZED_BUSINESS_SHA,
        "transcript_sha256": TRANSCRIPT_SHA,
        "checkpoint_sha256": CHECKPOINT_SHA,
        "producer_ack_sha256": PRODUCER_ACK_SHA,
        "lineage_sha256": SHA,
        "evidence_checkpoint_sha256": EVIDENCE_CHECKPOINT_SHA,
        "code_sha256": SHA,
        "artifact_set_sha256": ARTIFACT_SET_SHA,
        "direct_provenance_sha256": SHA,
        "captured_provenance_sha256": SHA,
        "resource_evidence_sha256": RESOURCE_SHA,
    }
    return {
        "contract_version": "r2d.a1.verification.v1",
        "release": "R2D-A1",
        "status": status,
        "full_r2d_a_status": "OPEN",
        "full_g2_status": "OPEN",
        "service_backed_status": "NOT_RUN",
        "operational_recovery_claim": False,
        "runtime": _runtime(),
        "checks": _checks(failed_name=failed_name),
        **identities,
        "evidence_bindings_sha256": r2d_evidence_bindings_sha256(**identities),
    }


def test_resource_contract_is_exact_and_portable() -> None:
    resource = load_r2d_config().resource_contract

    assert resource.model_dump() == {
        "wall_time_seconds": 300,
        "peak_process_tree_rss_mib": 4096,
        "maximum_process_tree_threads": 256,
        "artifact_mib": 1024,
        "shutdown_seconds": 10,
        "maximum_surviving_descendants": 0,
    }
    payload = resource.model_dump()
    payload["wall_time_seconds"] = 301
    with pytest.raises(ValidationError, match="Input should be 300"):
        R2DResourceContract.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("wall_time_seconds", 301),
        ("peak_process_tree_rss_mib", 4095),
        ("maximum_process_tree_threads", 257),
        ("artifact_mib", 1025),
        ("shutdown_seconds", 11),
        ("maximum_surviving_descendants", 1),
    ],
)
def test_resource_budget_tampering_fails_strict_validation(field: str, value: int) -> None:
    payload = load_r2d_config().model_dump()
    payload["resource_contract"][field] = value

    with pytest.raises(ValidationError):
        type(load_r2d_config()).model_validate(payload)


@pytest.mark.parametrize("status", ["PASS", "FAIL"])
def test_valid_pass_and_fail_receipts_retain_explicit_open_boundaries(status: str) -> None:
    receipt = R2DVerificationReceipt.model_validate(_receipt_payload(status=status))

    assert receipt.status == status
    assert receipt.full_r2d_a_status == "OPEN"
    assert receipt.full_g2_status == "OPEN"
    assert receipt.service_backed_status == "NOT_RUN"
    assert receipt.operational_recovery_claim is False


def test_pass_requires_exact_unique_ordered_all_true_checklist() -> None:
    duplicate = _receipt_payload()
    duplicate["checks"] = (*duplicate["checks"], duplicate["checks"][0])
    with pytest.raises(ValidationError, match="must be unique"):
        R2DVerificationReceipt.model_validate(duplicate)

    missing = _receipt_payload()
    missing["checks"] = missing["checks"][:-1]
    with pytest.raises(ValidationError, match="exact ordered"):
        R2DVerificationReceipt.model_validate(missing)

    reordered = _receipt_payload()
    checks = list(reordered["checks"])
    checks[1], checks[2] = checks[2], checks[1]
    reordered["checks"] = tuple(checks)
    with pytest.raises(ValidationError, match="exact ordered"):
        R2DVerificationReceipt.model_validate(reordered)

    false_check = _receipt_payload()
    checks = list(false_check["checks"])
    checks[-1] = checks[-1].model_copy(update={"passed": False})
    false_check["checks"] = tuple(checks)
    with pytest.raises(ValidationError, match="every R2D-A1"):
        R2DVerificationReceipt.model_validate(false_check)


def test_pass_replay_check_binds_canonical_detail_and_normalized_business_hash() -> None:
    wrong_detail = _receipt_payload()
    first = wrong_detail["checks"][0].model_copy(update={"detail": "looks equivalent"})
    wrong_detail["checks"] = (first, *wrong_detail["checks"][1:])
    with pytest.raises(ValidationError, match="canonical two-fresh-replay detail"):
        R2DVerificationReceipt.model_validate(wrong_detail)

    tampered = _receipt_payload()
    tampered["normalized_business_sha256"] = "c" * 64
    with pytest.raises(ValidationError, match="does not bind its required top-level evidence"):
        R2DVerificationReceipt.model_validate(tampered)


@pytest.mark.parametrize("check_name", R2D_A1_PASS_CHECK_NAMES)
def test_every_pass_check_requires_non_null_exact_mapped_evidence(check_name: str) -> None:
    payload = _receipt_payload()
    checks = list(payload["checks"])
    index = R2D_A1_PASS_CHECK_NAMES.index(check_name)
    checks[index] = checks[index].model_copy(update={"evidence_sha256": "9" * 64})
    payload["checks"] = tuple(checks)

    with pytest.raises(ValidationError, match="does not bind its required top-level evidence"):
        R2DVerificationReceipt.model_validate(payload)

    missing = _receipt_payload()
    checks = list(missing["checks"])
    checks[index] = checks[index].model_copy(update={"evidence_sha256": None})
    missing["checks"] = tuple(checks)
    with pytest.raises(ValidationError, match="non-null evidence"):
        R2DVerificationReceipt.model_validate(missing)


def test_fail_requires_a_failed_check_and_still_rejects_duplicate_names() -> None:
    all_true = _receipt_payload(status="FAIL")
    all_true["checks"] = _checks()
    with pytest.raises(ValidationError, match="at least one failed"):
        R2DVerificationReceipt.model_validate(all_true)

    duplicate = _receipt_payload(status="FAIL")
    duplicate["checks"] = (*duplicate["checks"], duplicate["checks"][0])
    with pytest.raises(ValidationError, match="must be unique"):
        R2DVerificationReceipt.model_validate(duplicate)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("os_family", "solaris"),
        ("architecture", "not allowed"),
        ("python_minor", "3.11"),
        ("python_minor", "3.13.1"),
        ("github_runner_os_attested", "true"),
    ],
)
def test_runtime_contract_is_strict(field: str, value: object) -> None:
    payload = _runtime().model_dump()
    payload[field] = value

    with pytest.raises(ValidationError):
        R2DVerificationRuntime.model_validate(payload)


def test_receipt_rejects_unknown_fields_and_invalid_hashes() -> None:
    extra = _receipt_payload()
    extra["unregistered_claim"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        R2DVerificationReceipt.model_validate(extra)

    bad_hash = deepcopy(_receipt_payload())
    bad_hash["checkpoint_sha256"] = "not-a-sha"
    with pytest.raises(ValidationError, match="String should match pattern"):
        R2DVerificationReceipt.model_validate(bad_hash)
