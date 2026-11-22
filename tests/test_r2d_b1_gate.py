from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from nexus.cli import app
from nexus.ingestion.r2d_b1 import R2DB1GovernanceError
from nexus.ingestion.r2d_b1_contracts import (
    DEFAULT_R2D_B1_CONFIG_PATH,
    R2D_B1_PASS_CHECK_NAMES,
    R2DB1VerificationReceipt,
)
from nexus.modeling.r3_artifacts import canonical_json_bytes, canonical_json_sha256
from nexus.r2d_b1 import (
    R2DB1VerificationResult,
    compare_r2d_b1_receipts,
    load_r2d_b1_receipt,
    verify_r2d_b1,
)

pytestmark = pytest.mark.r2d

runner = CliRunner()


@pytest.fixture(scope="module")
def verified(tmp_path_factory: pytest.TempPathFactory) -> R2DB1VerificationResult:
    return verify_r2d_b1(artifact_root=tmp_path_factory.mktemp("r2d-b1-gate"))


def _write_bundle(
    directory: Path,
    verified: R2DB1VerificationResult,
    *,
    operating_system: str,
    attested: bool = True,
    classification_sha256: str | None = None,
) -> Path:
    source_root = verified.receipt_artifact.path.parents[3]
    shutil.copytree(source_root, directory)
    base = verified.receipt
    payload = base.model_dump(mode="json", exclude={"content_sha256"})
    payload["runtime"] = {
        **payload["runtime"],
        "operating_system": operating_system,
        "github_runner_os_attested": attested,
    }
    if classification_sha256 is not None:
        payload["classification_matrix_sha256"] = classification_sha256
        for check in payload["checks"]:
            if check["name"] in {
                "poison_isolation_matrix",
                "business_validation_boundary",
            }:
                check["evidence_sha256"] = classification_sha256
    payload["content_sha256"] = canonical_json_sha256(payload)
    receipt = R2DB1VerificationReceipt.model_validate(payload)
    receipt_directory = directory / "r2d" / "b1" / "verification_receipt"
    for original in receipt_directory.glob("*.json"):
        original.unlink()
    receipt_payload = receipt.model_dump(mode="json")
    artifact_sha256 = canonical_json_sha256(receipt_payload)
    path = receipt_directory / f"{artifact_sha256}.json"
    path.write_bytes(canonical_json_bytes(receipt_payload) + b"\n")
    return path


def test_verifier_publishes_exact_honest_gate(
    verified: R2DB1VerificationResult,
) -> None:
    receipt = verified.receipt
    assert verified.passed
    assert receipt.status == "PASS"
    assert tuple(check.name for check in receipt.checks) == R2D_B1_PASS_CHECK_NAMES
    assert all(check.passed for check in receipt.checks)
    assert receipt.portable_contract_status == "PASS"
    assert receipt.service_dead_letter_topic_status == "NOT_RUN"
    assert receipt.persistent_dagster_status == "NOT_RUN"
    assert receipt.operational_alert_delivery_status == "NOT_RUN"
    assert receipt.full_r2d_b_status == "IN_PROGRESS"
    assert receipt.full_g2_status == "OPEN"
    assert load_r2d_b1_receipt(verified.receipt_artifact.path) == receipt


def test_receipt_validation_rejects_incomplete_false_and_detached_checks(
    verified: R2DB1VerificationResult,
) -> None:
    baseline = verified.receipt.model_dump(mode="json")
    mutations = []

    missing = json.loads(json.dumps(baseline))
    missing["checks"] = missing["checks"][:-1]
    mutations.append(missing)

    false_check = json.loads(json.dumps(baseline))
    false_check["checks"][0]["passed"] = False
    mutations.append(false_check)

    detached = json.loads(json.dumps(baseline))
    detached["checks"][0]["evidence_sha256"] = "0" * 64
    mutations.append(detached)

    for payload in mutations:
        payload["content_sha256"] = canonical_json_sha256(
            {key: value for key, value in payload.items() if key != "content_sha256"}
        )
        with pytest.raises(ValidationError):
            R2DB1VerificationReceipt.model_validate(payload)


def test_canonical_loader_rejects_physical_rewrite(
    tmp_path: Path,
    verified: R2DB1VerificationResult,
) -> None:
    payload = verified.receipt.model_dump(mode="json")
    rewritten = tmp_path / "pretty.json"
    rewritten.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    with pytest.raises(R2DB1GovernanceError, match="not canonical"):
        load_r2d_b1_receipt(rewritten)


def test_cross_platform_comparator_rederives_every_semantic_field(
    tmp_path: Path,
    verified: R2DB1VerificationResult,
) -> None:
    linux = _write_bundle(tmp_path / "linux", verified, operating_system="Linux")
    windows = _write_bundle(
        tmp_path / "windows",
        verified,
        operating_system="Windows",
    )
    result = compare_r2d_b1_receipts(linux, windows)
    assert result["R2D_B1_CROSS_PLATFORM_PASS"] is True

    forged = tmp_path / "forged"
    forged.mkdir()
    forged_linux = _write_bundle(
        forged / "linux",
        verified,
        operating_system="Linux",
        classification_sha256="0" * 64,
    )
    forged_windows = _write_bundle(
        forged / "windows",
        verified,
        operating_system="Windows",
        classification_sha256="0" * 64,
    )
    with pytest.raises(R2DB1GovernanceError, match="current derivation"):
        compare_r2d_b1_receipts(forged_linux, forged_windows)


def test_cross_platform_comparator_requires_two_attested_platforms(
    tmp_path: Path,
    verified: R2DB1VerificationResult,
) -> None:
    linux = _write_bundle(tmp_path / "linux", verified, operating_system="Linux")
    linux_two = _write_bundle(
        tmp_path / "same",
        verified,
        operating_system="Linux",
    )
    with pytest.raises(R2DB1GovernanceError, match="attested Linux and Windows"):
        compare_r2d_b1_receipts(linux, linux_two)

    unattested_windows = _write_bundle(
        tmp_path / "unattested",
        verified,
        operating_system="Windows",
        attested=False,
    )
    with pytest.raises(R2DB1GovernanceError, match="attested Linux and Windows"):
        compare_r2d_b1_receipts(linux, unattested_windows)


def test_cross_platform_comparator_rejects_bundle_artifact_tamper(
    tmp_path: Path,
    verified: R2DB1VerificationResult,
) -> None:
    linux = _write_bundle(tmp_path / "linux", verified, operating_system="Linux")
    windows = _write_bundle(tmp_path / "windows", verified, operating_system="Windows")
    classification_files = list(
        (linux.parents[3] / "run-a" / "r2d" / "b1" / "classification").glob("*.json")
    )
    assert len(classification_files) == 1
    classification_files[0].write_bytes(b"{}\n")
    with pytest.raises(R2DB1GovernanceError, match="bundle artifact mismatch"):
        compare_r2d_b1_receipts(linux, windows)


def test_b1_cli_help_and_explicit_verification(tmp_path: Path) -> None:
    help_result = runner.invoke(app, ["r2d", "verify-b1", "--help"])
    assert help_result.exit_code == 0
    assert "poison isolation" in help_result.output

    result = runner.invoke(
        app,
        [
            "r2d",
            "verify-b1",
            "--artifact-dir",
            str(tmp_path / "cli"),
            "--config-path",
            str(DEFAULT_R2D_B1_CONFIG_PATH),
        ],
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    assert output["status"] == "PASS"
    assert output["portable_contract_status"] == "PASS"
    assert output["service_dead_letter_topic_status"] == "NOT_RUN"
    assert Path(output["receipt"]).is_file()
