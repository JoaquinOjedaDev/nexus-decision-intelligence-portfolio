from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError
from typer.main import get_command
from typer.testing import CliRunner

from nexus.cli import app
from nexus.ingestion.r2d_a2_contracts import (
    R2D_A2_PASS_CHECK_NAMES,
    R2DA2ResourceEvidence,
    R2DA2ServiceCheckpoint,
    R2DA2ServiceImageEvidence,
    R2DA2VerificationCheck,
    R2DA2VerificationReceipt,
    load_r2d_a2_service_image_evidence,
    r2d_a2_code_sha256,
)
from nexus.ingestion.r2d_contracts import canonical_json_sha256

pytestmark = pytest.mark.r2d_service

ROOT = Path(__file__).resolve().parents[1]
runner = CliRunner()


def _service_images() -> R2DA2ServiceImageEvidence:
    payload: dict[str, object] = {
        "contract_version": "r2d.a2.service-images.v1",
        "images": [
            {
                "service": "postgres",
                "configured_reference": "postgres:17.6-alpine",
                "observed_image_id": f"sha256:{'a' * 64}",
                "operating_system": "linux",
                "architecture": "amd64",
            },
            {
                "service": "redpanda",
                "configured_reference": ("docker.redpanda.com/redpandadata/redpanda:v26.1.9"),
                "observed_image_id": f"sha256:{'b' * 64}",
                "operating_system": "linux",
                "architecture": "amd64",
            },
        ],
    }
    return R2DA2ServiceImageEvidence.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def _canonical_json_file(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _checkpoint() -> R2DA2ServiceCheckpoint:
    payload: dict[str, object] = {
        "contract_version": "r2d.a2.service-checkpoint.v1",
        "topic": "inventory.adjusted.v1",
        "starts": [0, 0],
        "ends": [3, 6],
        "next_offsets": [3, 6],
        "record_count": 9,
        "data_config_sha256": "a" * 64,
        "service_config_sha256": "b" * 64,
        "service_image_evidence_sha256": "2" * 64,
        "migration_sha256": "c" * 64,
        "broker_ack_sha256": "d" * 64,
        "transcript_sha256": "e" * 64,
        "lineage_sha256": "f" * 64,
        "raw_value_multiset_sha256": "1" * 64,
    }
    return R2DA2ServiceCheckpoint.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def _resource() -> R2DA2ResourceEvidence:
    payload: dict[str, object] = {
        "contract_version": "r2d.a2.resource-evidence.v1",
        "wall_time_seconds": 123.5,
        "wall_time_limit_seconds": 600,
        "pre_receipt_artifact_bytes": 10_000,
        "artifact_limit_bytes": 1_073_741_824,
        "passed": True,
    }
    return R2DA2ResourceEvidence.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def _receipt() -> R2DA2VerificationReceipt:
    checks = tuple(
        R2DA2VerificationCheck(name=name, passed=True, evidence_sha256="2" * 64)
        for name in R2D_A2_PASS_CHECK_NAMES
    )
    payload: dict[str, object] = {
        "contract_version": "r2d.a2.verification-receipt.v2",
        "release": "R2D-A2",
        "status": "PASS",
        "checks": [item.model_dump(mode="json") for item in checks],
        "topic": "inventory.adjusted.v1",
        "partition_count": 2,
        "record_count": 9,
        "planning_cutoffs": ["2026-06-20T00:00:00Z", "2026-06-30T00:00:00Z"],
        "data_config_sha256": "3" * 64,
        "service_config_sha256": "4" * 64,
        "service_image_evidence_content_sha256": "0" * 64,
        "service_image_evidence_file_sha256": "1" * 64,
        "postgres_observed_image_id": f"sha256:{'a' * 64}",
        "redpanda_observed_image_id": f"sha256:{'b' * 64}",
        "migration_sha256": "5" * 64,
        "code_sha256": "6" * 64,
        "a2b1_receipt_content_sha256": "7" * 64,
        "a2b1_receipt_file_sha256": "8" * 64,
        "broker_ack_sha256": "9" * 64,
        "transcript_sha256": "a" * 64,
        "lineage_sha256": "b" * 64,
        "service_checkpoint_sha256": "c" * 64,
        "raw_value_multiset_sha256": "d" * 64,
        "reconstructed_inventory_sha256": "e" * 64,
        "reconstructed_source_manifest_sha256": "f" * 64,
        "reconstructed_source_manifest_file_sha256": "1" * 64,
        "source_reconstruction_sha256": "8" * 64,
        "r2a_semantics_sha256": "9" * 64,
        "normalized_business_sha256": "2" * 64,
        "direct_provenance_sha256": "3" * 64,
        "service_provenance_sha256": "4" * 64,
        "recovery_provenance_sha256": "4" * 64,
        "provenance_distinction_sha256": "a" * 64,
        "service_recovery_sha256": "5" * 64,
        "resource_evidence_sha256": "6" * 64,
        "artifact_set_sha256": "7" * 64,
        "service_foundation_status": "PASS",
        "crash_recovery_status": "PASS",
        "downstream_parity_status": "PASS",
        "r2d_a2_status": "PASS",
        "full_r2d_a_status": "PASS",
        "full_g2_status": "OPEN",
        "operational_recovery_claim": True,
        "exactly_once_claim": False,
    }
    evidence_by_check = {
        "verified_a2b1_crash_receipt": payload["a2b1_receipt_content_sha256"],
        "resolved_service_images": payload["service_image_evidence_content_sha256"],
        "real_service_capture_complete": payload["service_checkpoint_sha256"],
        "postgres_source_reconstruction": payload["source_reconstruction_sha256"],
        "acknowledged_raw_conservation": payload["raw_value_multiset_sha256"],
        "public_r2a_two_cutoffs": payload["r2a_semantics_sha256"],
        "direct_service_business_parity": payload["normalized_business_sha256"],
        "portable_reference_business_join": payload["normalized_business_sha256"],
        "route_provenance_is_distinct": payload["provenance_distinction_sha256"],
        "derived_state_recovery_identity": payload["service_recovery_sha256"],
        "bounded_resource_envelope": payload["resource_evidence_sha256"],
    }
    payload["checks"] = [
        R2DA2VerificationCheck(
            name=name,
            passed=True,
            evidence_sha256=evidence_by_check[name],
        ).model_dump(mode="json")
        for name in R2D_A2_PASS_CHECK_NAMES
    ]
    return R2DA2VerificationReceipt.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def test_service_image_evidence_is_exact_canonical_and_reloadable(tmp_path: Path) -> None:
    evidence = _service_images()
    path = tmp_path / "service-images.json"
    _canonical_json_file(path, evidence.model_dump(mode="json"))

    loaded = load_r2d_a2_service_image_evidence(path)

    assert loaded == evidence
    assert loaded.observed_image_id("postgres") == f"sha256:{'a' * 64}"
    assert loaded.observed_image_id("redpanda") == f"sha256:{'b' * 64}"


def test_service_image_evidence_rejects_reordered_or_mutated_identities() -> None:
    evidence = _service_images()

    reordered = evidence.model_dump(mode="python")
    reordered["images"] = list(reversed(reordered["images"]))
    reordered["content_sha256"] = canonical_json_sha256(
        {key: value for key, value in reordered.items() if key != "content_sha256"}
    )
    with pytest.raises(ValidationError, match="postgres then redpanda exactly once"):
        R2DA2ServiceImageEvidence.model_validate(reordered)

    wrong_reference = evidence.model_dump(mode="python")
    wrong_reference["images"][0]["configured_reference"] = "postgres:latest"
    wrong_reference["content_sha256"] = canonical_json_sha256(
        {key: value for key, value in wrong_reference.items() if key != "content_sha256"}
    )
    with pytest.raises(ValidationError, match="governed Compose topology"):
        R2DA2ServiceImageEvidence.model_validate(wrong_reference)

    malformed_id = evidence.model_dump(mode="python")
    malformed_id["images"][0]["observed_image_id"] = "sha256:short"
    malformed_id["content_sha256"] = canonical_json_sha256(
        {key: value for key, value in malformed_id.items() if key != "content_sha256"}
    )
    with pytest.raises(ValidationError, match="should match pattern"):
        R2DA2ServiceImageEvidence.model_validate(malformed_id)

    duplicate_id = evidence.model_dump(mode="python")
    duplicate_id["images"][1]["observed_image_id"] = duplicate_id["images"][0]["observed_image_id"]
    duplicate_id["content_sha256"] = canonical_json_sha256(
        {key: value for key, value in duplicate_id.items() if key != "content_sha256"}
    )
    with pytest.raises(ValidationError, match="distinct observed image IDs"):
        R2DA2ServiceImageEvidence.model_validate(duplicate_id)


def test_service_image_loader_fails_closed_on_absent_tampered_or_noncanonical_bytes(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing.json"
    with pytest.raises(ValueError, match="cannot read required"):
        load_r2d_a2_service_image_evidence(missing)

    evidence = _service_images()
    path = tmp_path / "service-images.json"
    payload = evidence.model_dump(mode="json")
    payload["content_sha256"] = "f" * 64
    _canonical_json_file(path, payload)
    with pytest.raises(ValueError, match="invalid or tampered"):
        load_r2d_a2_service_image_evidence(path)

    _canonical_json_file(path, evidence.model_dump(mode="json"))
    path.write_bytes(b" " + path.read_bytes())
    with pytest.raises(ValueError, match="not canonical JSON"):
        load_r2d_a2_service_image_evidence(path)


def test_service_checkpoint_is_exact_complete_and_content_addressed() -> None:
    checkpoint = _checkpoint()

    assert checkpoint.starts == (0, 0)
    assert checkpoint.ends == checkpoint.next_offsets == (3, 6)
    with pytest.raises(ValidationError, match="reach every frozen end"):
        R2DA2ServiceCheckpoint.model_validate(
            {**checkpoint.model_dump(mode="python"), "next_offsets": [3, 5]}
        )


def test_resource_evidence_rejects_over_budget_or_false_pass() -> None:
    resource = _resource()

    assert resource.passed is True
    with pytest.raises(ValidationError, match="wall time exceeds"):
        R2DA2ResourceEvidence.model_validate(
            {**resource.model_dump(mode="python"), "wall_time_seconds": 601.0}
        )
    with pytest.raises(ValidationError):
        R2DA2ResourceEvidence.model_validate(
            {**resource.model_dump(mode="python"), "passed": False}
        )


def test_final_a2_receipt_is_exact_and_keeps_g2_exactly_once_open() -> None:
    receipt = _receipt()

    assert tuple(item.name for item in receipt.checks) == R2D_A2_PASS_CHECK_NAMES
    assert receipt.full_r2d_a_status == receipt.r2d_a2_status == "PASS"
    assert receipt.full_g2_status == "OPEN"
    assert receipt.operational_recovery_claim is True
    assert receipt.exactly_once_claim is False
    assert len(r2d_a2_code_sha256(ROOT)) == 64

    with pytest.raises(ValidationError, match="incomplete, duplicated, or reordered"):
        R2DA2VerificationReceipt.model_validate(
            {**receipt.model_dump(mode="python"), "checks": list(reversed(receipt.checks))}
        )
    with pytest.raises(ValidationError, match="provenance must remain distinct"):
        R2DA2VerificationReceipt.model_validate(
            {
                **receipt.model_dump(mode="python"),
                "service_provenance_sha256": receipt.direct_provenance_sha256,
            }
        )

    rebound = receipt.model_dump(mode="python")
    rebound["service_image_evidence_content_sha256"] = "f" * 64
    rebound["content_sha256"] = canonical_json_sha256(
        {key: value for key, value in rebound.items() if key != "content_sha256"}
    )
    with pytest.raises(ValidationError, match="checklist evidence does not bind"):
        R2DA2VerificationReceipt.model_validate(rebound)


def test_a2b2_cli_requires_prior_crash_receipt_without_exposing_secret_default() -> None:
    result = runner.invoke(app, ["r2d", "verify-service-a2b2", "--help"])

    assert result.exit_code == 0, result.stdout
    root_command = get_command(app)
    r2d_command = root_command.commands["r2d"]  # type: ignore[attr-defined]
    a2b2_command = r2d_command.commands["verify-service-a2b2"]  # type: ignore[attr-defined]
    options = {
        option for parameter in a2b2_command.params for option in getattr(parameter, "opts", ())
    }
    assert {
        "--a2b1-receipt",
        "--bootstrap-servers",
        "--postgres-dsn",
        "--service-image-evidence",
    } <= options
    assert "local-only-change-me" not in result.stdout


def test_a2b2_ci_is_same_commit_isolated_receipted_and_always_torn_down() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    job = workflow.split("  r2d-service-a2b2:\n", maxsplit=1)[1].split(
        "  r3b-learned-intelligence:\n", maxsplit=1
    )[0]

    assert "needs: r2d-service-a2b1" in job
    assert "r2d-a2b1-service-receipt-linux-py313" in job
    assert "nexus-r2d-a2b2-${{ github.run_id }}-${{ github.run_attempt }}" in job
    assert "verify-service-a2b2" in job
    assert "r2d-a2-final-evidence-linux-py313" in job
    assert "a2b2-service-images.json" in job
    assert "--service-image-evidence artifacts/r2d/a2b2-service-images.json" in job
    assert job.index("Record canonical resolved service images") < job.index(
        "R2D-A2 retained-state rebuild and semantic parity gate"
    )
    assert "if: always()" in job
    assert "down -v --remove-orphans" in job
    assert "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}" in job
