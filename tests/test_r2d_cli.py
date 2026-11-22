from __future__ import annotations

import json
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from click.utils import strip_ansi
from pydantic import ValidationError
from typer.testing import CliRunner

from nexus.cli import app
from nexus.config import PROJECT_ROOT
from nexus.ingestion import (
    R2D_A1_PASS_CHECK_NAMES,
    R2D_SOURCE_MANIFEST_SHA256,
    R2DVerificationCheck,
    R2DVerificationReceipt,
    R2DVerificationRuntime,
    r2d_evidence_bindings_sha256,
)

pytestmark = pytest.mark.r2d

runner = CliRunner()


def _model(payload: dict[str, object]) -> SimpleNamespace:
    return SimpleNamespace(model_dump=lambda **_kwargs: payload)


def _verification_result(tmp_path: Path, *, passed: bool) -> SimpleNamespace:
    status = "PASS" if passed else "FAIL"
    return SimpleNamespace(
        passed=passed,
        receipt=SimpleNamespace(
            checks=(
                _model(
                    {
                        "detail": "portable evidence only; services not exercised",
                        "name": "portable_contract",
                        "passed": passed,
                    }
                ),
            ),
            full_g2_status="OPEN",
            full_r2d_a_status="OPEN",
            operational_recovery_claim=False,
            release="R2D-A1",
            runtime=_model(
                {
                    "github_runner_os_attested": False,
                    "os_family": "windows",
                    "python_minor": "3.13",
                }
            ),
            service_backed_status="NOT_RUN",
            status=status,
        ),
        receipt_path=tmp_path / "r2d-a1-platform-receipt.json",
    )


def _passing_contract_receipt() -> R2DVerificationReceipt:
    normalized_sha256 = "a" * 64
    config_sha256 = "b" * 64
    transcript_sha256 = "c" * 64
    checkpoint_sha256 = "d" * 64
    direct_provenance_sha256 = "e" * 64
    captured_provenance_sha256 = "f" * 64
    resource_evidence_sha256 = "1" * 64
    producer_ack_sha256 = "3" * 64
    evidence_checkpoint_sha256 = "5" * 64
    artifact_set_sha256 = "7" * 64
    identities = {
        "source_manifest_sha256": R2D_SOURCE_MANIFEST_SHA256,
        "config_sha256": config_sha256,
        "normalized_business_sha256": normalized_sha256,
        "transcript_sha256": transcript_sha256,
        "checkpoint_sha256": checkpoint_sha256,
        "producer_ack_sha256": producer_ack_sha256,
        "lineage_sha256": "4" * 64,
        "evidence_checkpoint_sha256": evidence_checkpoint_sha256,
        "code_sha256": "6" * 64,
        "artifact_set_sha256": artifact_set_sha256,
        "direct_provenance_sha256": direct_provenance_sha256,
        "captured_provenance_sha256": captured_provenance_sha256,
        "resource_evidence_sha256": resource_evidence_sha256,
    }
    check_evidence = {
        "two_fresh_replay": normalized_sha256,
        "config_and_schema_identity": config_sha256,
        "deterministic_two_partition_transcript": transcript_sha256,
        "transport_admission_and_physical_identity": transcript_sha256,
        "complete_half_open_checkpoint": checkpoint_sha256,
        "raw_value_conservation": producer_ack_sha256,
        "manifest_bound_source_reconstruction": evidence_checkpoint_sha256,
        "public_r2a_semantic_authority": evidence_checkpoint_sha256,
        "direct_captured_business_parity": normalized_sha256,
        "point_in_time_correction_semantics": evidence_checkpoint_sha256,
        "route_provenance_integrity": evidence_checkpoint_sha256,
        "bounded_portable_resource_envelope": resource_evidence_sha256,
        "immutable_artifacts": artifact_set_sha256,
    }
    return R2DVerificationReceipt(
        contract_version="r2d.a1.verification.v1",
        release="R2D-A1",
        status="PASS",
        full_r2d_a_status="OPEN",
        full_g2_status="OPEN",
        service_backed_status="NOT_RUN",
        operational_recovery_claim=False,
        runtime=R2DVerificationRuntime(
            os_family="linux",
            architecture="x86_64",
            python_minor="3.13",
            github_runner_os_attested=True,
        ),
        checks=tuple(
            R2DVerificationCheck(
                name=name,
                passed=True,
                detail=(
                    "two fresh replay semantic hashes match"
                    if name == "two_fresh_replay"
                    else "verified"
                ),
                evidence_sha256=check_evidence[name],
            )
            for name in R2D_A1_PASS_CHECK_NAMES
        ),
        **identities,
        evidence_bindings_sha256=r2d_evidence_bindings_sha256(**identities),
    )


def _install_fake_r2d(
    monkeypatch: pytest.MonkeyPatch,
    *,
    verifier: Callable[..., SimpleNamespace],
    comparator: Callable[..., dict[str, object]] | None = None,
) -> None:
    module = ModuleType("nexus.r2d")
    module.__dict__["verify_r2d_a1"] = verifier
    module.__dict__["compare_r2d_a1_receipt_files"] = comparator or (
        lambda _left, _right: {"passed": True}
    )
    monkeypatch.setitem(sys.modules, "nexus.r2d", module)


def test_r2d_receipt_fixture_round_trips_without_service_claims() -> None:
    receipt = _passing_contract_receipt()

    assert R2DVerificationReceipt.model_validate_json(receipt.model_dump_json()) == receipt
    assert receipt.service_backed_status == "NOT_RUN"
    assert receipt.operational_recovery_claim is False
    payload = receipt.model_dump(mode="json")
    payload["operational_recovery_claim"] = True
    with pytest.raises(ValidationError):
        R2DVerificationReceipt.model_validate(payload)


def test_r2d_help_separates_portable_and_bounded_service_commands() -> None:
    group = runner.invoke(app, ["r2d", "--help"], color=True)
    verify = runner.invoke(app, ["r2d", "verify", "--help"], color=True)
    service = runner.invoke(app, ["r2d", "verify-service-a2a", "--help"], color=True)
    group_output = strip_ansi(group.stdout)
    verify_output = strip_ansi(verify.stdout)
    service_output = strip_ansi(service.stdout)

    assert group.exit_code == 0, group.stdout
    assert "verify" in group_output
    assert "compare-platforms" in group_output
    assert "verify-service-a2a" in group_output
    assert "redpanda" in group_output.lower()
    assert verify.exit_code == 0, verify.stdout
    assert "--artifact-dir" in verify_output
    assert "--config-path" in verify_output
    assert "--bootstrap-servers" not in verify_output
    assert "--postgres-dsn" not in verify_output
    assert service.exit_code == 0, service.stdout
    assert "--bootstrap-servers" in service_output
    assert "--postgres-dsn" in service_output
    assert "without claiming crash recovery" in service_output


def test_r2d_ci_topology_runs_acceptance_once_per_full_gate_platform() -> None:
    workflow = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    r2d_core = workflow.split("  r2d-core:\n", maxsplit=1)[1].split(
        "  r2d-core-cross-platform-receipt:\n", maxsplit=1
    )[0]

    assert r2d_core.count("full_gate: true") == 2
    assert r2d_core.count("full_gate: false") == 1
    assert r2d_core.count("semantics_integration: true") == 1
    assert r2d_core.count("uv run python -m pytest -q tests/test_r2d_acceptance.py") == 1
    assert r2d_core.count("R2D_A1_ACCEPTANCE_ARTIFACT_ROOT: artifacts/r2d/a1-verification") == 1
    assert "uv run nexus r2d verify" not in r2d_core
    assert workflow.count("not r2d_acceptance and not r2d_integration and not r2d_service") == 3


def test_r2d_verify_forwards_paths_and_emits_honest_portable_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "evidence"
    config_path = tmp_path / "r2d-a1.yaml"
    observed: list[tuple[Path, Path]] = []
    expected = _verification_result(tmp_path, passed=True)

    def verify_r2d_a1(*, artifact_root: Path, config_path: Path) -> SimpleNamespace:
        observed.append((artifact_root, config_path))
        return expected

    _install_fake_r2d(monkeypatch, verifier=verify_r2d_a1)
    result = runner.invoke(
        app,
        [
            "r2d",
            "verify",
            "--artifact-dir",
            str(artifact_root),
            "--config-path",
            str(config_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert observed == [(artifact_root, config_path)]
    assert json.loads(result.stdout) == {
        "checks": [
            {
                "detail": "portable evidence only; services not exercised",
                "name": "portable_contract",
                "passed": True,
            }
        ],
        "full_g2_status": "OPEN",
        "full_r2d_a_status": "OPEN",
        "operational_recovery_claim": False,
        "passed": True,
        "receipt": str(expected.receipt_path),
        "release": "R2D-A1",
        "runtime": {
            "github_runner_os_attested": False,
            "os_family": "windows",
            "python_minor": "3.13",
        },
        "service_backed_status": "NOT_RUN",
        "status": "PASS",
    }


def test_r2d_verify_defaults_are_portable_and_failure_exits_nonzero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[Path, Path]] = []

    def verify_r2d_a1(*, artifact_root: Path, config_path: Path) -> SimpleNamespace:
        observed.append((artifact_root, config_path))
        return _verification_result(tmp_path, passed=False)

    _install_fake_r2d(monkeypatch, verifier=verify_r2d_a1)
    result = runner.invoke(app, ["r2d", "verify"])

    assert result.exit_code == 1
    assert observed == [
        (
            PROJECT_ROOT / "artifacts" / "r2d" / "a1-verification",
            PROJECT_ROOT / "configs" / "ingestion" / "r2d-a1.yaml",
        )
    ]
    payload = json.loads(result.stdout)
    assert payload["passed"] is False
    assert payload["status"] == "FAIL"
    assert payload["service_backed_status"] == "NOT_RUN"
    assert payload["operational_recovery_claim"] is False
    assert payload["full_r2d_a_status"] == payload["full_g2_status"] == "OPEN"


def test_r2d_compare_platforms_forwards_two_receipts_and_prints_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    left = tmp_path / "linux.json"
    right = tmp_path / "windows.json"
    observed: list[tuple[Path, Path]] = []

    def compare(left: Path, right: Path) -> dict[str, object]:
        observed.append((left, right))
        return {
            "full_g2_status": "OPEN",
            "full_r2d_a_status": "OPEN",
            "operational_recovery_claim": False,
            "passed": True,
            "service_backed_status": "NOT_RUN",
            "status": "R2D_A1_CROSS_PLATFORM_PASS",
        }

    _install_fake_r2d(
        monkeypatch,
        verifier=lambda **_kwargs: _verification_result(tmp_path, passed=True),
        comparator=compare,
    )
    result = runner.invoke(
        app,
        [
            "r2d",
            "compare-platforms",
            "--left",
            str(left),
            "--right",
            str(right),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert observed == [(left, right)]
    assert json.loads(result.stdout) == {
        "full_g2_status": "OPEN",
        "full_r2d_a_status": "OPEN",
        "operational_recovery_claim": False,
        "passed": True,
        "service_backed_status": "NOT_RUN",
        "status": "R2D_A1_CROSS_PLATFORM_PASS",
    }
