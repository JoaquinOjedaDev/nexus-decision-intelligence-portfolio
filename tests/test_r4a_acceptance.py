from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import Literal

import pytest
from pydantic import ValidationError

import nexus.r4a as r4a
from nexus.contracts import LifecycleEventType, RecommendationManifest
from nexus.decisioning.r4a_contracts import (
    R4A_PASS_CHECK_NAMES,
    PolicyDecision,
    VerificationCheck,
    VerificationReceipt,
    VerificationRuntime,
)
from nexus.r4a import (
    R4ARunResult,
    R4AVerificationResult,
    compare_r4a_verification_receipt_files,
    run_r4a_once,
    verify_r4a,
)
from nexus.workflow.ledger import DecisionState

pytestmark = pytest.mark.r4a


def _passing_receipt(
    *,
    platform_name: Literal["linux", "windows", "macos", "other"] = "linux",
    replay_sha256: str = "a" * 64,
    python_minor: str = "3.13",
    runner_attested: bool = True,
) -> VerificationReceipt:
    return VerificationReceipt(
        release="R4A",
        status="PASS",
        full_g4_status="OPEN",
        runtime=VerificationRuntime(
            os_family=platform_name,
            architecture="amd64" if platform_name == "windows" else "x86_64",
            python_minor=python_minor,
            github_runner_os_attested=runner_attested,
        ),
        checks=tuple(
            VerificationCheck(
                name=name,
                passed=True,
                detail=(
                    "two fresh replay semantic hashes match"
                    if name == "two_run_replay"
                    else "verified"
                ),
                evidence_sha256=(replay_sha256 if name == "two_run_replay" else None),
            )
            for name in R4A_PASS_CHECK_NAMES
        ),
        replay_semantic_sha256=replay_sha256,
        superiority_claim_allowed=False,
    )


def _write_receipt(path: Path, receipt: VerificationReceipt, *, pretty: bool = False) -> None:
    payload = receipt.model_dump(mode="json")
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) if pretty else receipt.model_dump_json(),
        encoding="utf-8",
    )


@pytest.fixture(scope="module")
def replay_pair(tmp_path_factory: pytest.TempPathFactory) -> tuple[R4ARunResult, R4ARunResult]:
    root = tmp_path_factory.mktemp("r4a-replay")
    return (
        run_r4a_once(output_dir=root / "run-a"),
        run_r4a_once(output_dir=root / "run-b"),
    )


@pytest.fixture(scope="module")
def verified_r4a(tmp_path_factory: pytest.TempPathFactory) -> R4AVerificationResult:
    return verify_r4a(artifact_root=tmp_path_factory.mktemp("r4a-verification"))


def test_two_fresh_runs_have_identical_semantics_and_receipts(
    replay_pair: tuple[R4ARunResult, R4ARunResult],
) -> None:
    first, second = replay_pair

    assert first.semantic_sha256 == second.semantic_sha256
    assert first.optimizer == second.optimizer
    assert first.heuristic == second.heuristic
    assert first.evaluation == second.evaluation
    assert first.gate == second.gate
    assert first.manifest == second.manifest
    assert {item.dataset: item.content_sha256 for item in first.artifacts} == {
        item.dataset: item.content_sha256 for item in second.artifacts
    }


def test_manifest_fingerprint_changes_with_material_input_and_rejects_tampering(
    replay_pair: tuple[R4ARunResult, R4ARunResult],
) -> None:
    original = replay_pair[0].manifest
    assert original is not None
    changed_payload = original.model_dump(mode="python")
    changed_payload.pop("fingerprint")
    recommendations = list(changed_payload["recommendations"])
    first_line = deepcopy(recommendations[0])
    first_line["recommended_quantity"] += 1
    recommendations[0] = first_line
    changed_payload["recommendations"] = tuple(recommendations)

    changed = RecommendationManifest.model_validate(changed_payload)
    assert changed.fingerprint != original.fingerprint

    tampered_payload = dict(changed_payload)
    tampered_payload["fingerprint"] = original.fingerprint
    with pytest.raises(ValidationError, match="fingerprint does not match"):
        RecommendationManifest.model_validate(tampered_payload)


def test_submit_only_run_stops_pending_approval_without_execution(
    replay_pair: tuple[R4ARunResult, R4ARunResult],
) -> None:
    result = replay_pair[0]
    lifecycle = result.artifact["r4a.lifecycle_submission"]
    payload = json.loads(lifecycle.path.read_text(encoding="utf-8"))

    assert result.manifest is not None
    assert result.lifecycle_state is DecisionState.PENDING_APPROVAL
    assert result.gate.lifecycle_state == "pending_approval"
    assert "persisted_policy_revalidation_completed" in result.gate.checks
    assert payload["state"] == "pending_approval"
    assert [event["event_type"] for event in payload["events"]] == [
        LifecycleEventType.SUBMITTED_FOR_APPROVAL.value
    ]
    assert not any("execution" in event["event_type"] for event in payload["events"])


def test_service_infeasible_decision_is_non_actionable_and_creates_no_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def service_infeasible(*_args: object, **_kwargs: object) -> PolicyDecision:
        baseline = r4a.build_heuristic_policy(_args[0], _args[1])
        first_recourse = baseline.recourse[0].model_copy(
            update={
                "lost_sales_units": tuple(
                    value + 1_000 for value in baseline.recourse[0].lost_sales_units
                )
            }
        )
        recourse = (first_recourse, *baseline.recourse[1:])
        feasibility = r4a.validate_decision(
            _args[0],
            _args[1],
            baseline.lines,
            recourse,
        )
        return PolicyDecision(
            policy_name="projected_base_stock",
            raw_solver_status="INFEASIBLE",
            solver_objective_cents=None,
            solver_best_bound_cents=None,
            optimality_gap_bps=None,
            deterministic_tie_break_applied=False,
            fallback_used=True,
            fallback_reason="service_target_infeasible",
            lines=baseline.lines,
            recourse=recourse,
            feasibility=feasibility,
            review_disposition="RECEIPT_ONLY_INFEASIBLE",
        )

    monkeypatch.setattr(r4a, "solve_constrained_policy", service_infeasible)

    result = run_r4a_once(output_dir=tmp_path / "service-infeasible")

    assert result.optimizer.feasibility.service_targets_met is False
    assert result.optimizer.review_disposition == "RECEIPT_ONLY_INFEASIBLE"
    assert result.manifest is None
    assert result.lifecycle_state is None
    assert "r4a.recommendation_manifest" not in result.artifact
    assert "r4a.lifecycle_submission" not in result.artifact


def test_complete_bounded_verification_gate_passes_and_persists_evidence(
    verified_r4a: R4AVerificationResult,
) -> None:
    assert verified_r4a.passed is True
    assert verified_r4a.receipt.status == "PASS"
    assert verified_r4a.receipt.full_g4_status == "OPEN"
    assert all(check.passed for check in verified_r4a.receipt.checks)
    assert verified_r4a.receipt.superiority_claim_allowed is False
    assert verified_r4a.artifact.path.is_file()
    persisted = json.loads(verified_r4a.artifact.path.read_text(encoding="utf-8"))
    assert persisted == verified_r4a.receipt.model_dump(mode="json")


def test_cross_platform_comparator_accepts_byte_different_normalized_receipts(
    tmp_path: Path,
) -> None:
    left = tmp_path / "linux.json"
    right = tmp_path / "windows.json"
    _write_receipt(left, _passing_receipt(platform_name="linux"))
    _write_receipt(right, _passing_receipt(platform_name="windows"), pretty=True)

    result = compare_r4a_verification_receipt_files(left, right)

    assert result == {
        "full_g4_status": "OPEN",
        "passed": True,
        "platforms": ["linux", "windows"],
        "python_minor": "3.13",
        "replay_semantic_sha256": "a" * 64,
        "status": "CROSS_PLATFORM_PASS",
        "superiority_claim_allowed": False,
    }


def test_cross_platform_comparator_rejects_a_material_semantic_mismatch(
    tmp_path: Path,
) -> None:
    left = tmp_path / "linux.json"
    right = tmp_path / "windows.json"
    _write_receipt(
        left,
        _passing_receipt(platform_name="linux", replay_sha256="a" * 64),
    )
    _write_receipt(
        right,
        _passing_receipt(platform_name="windows", replay_sha256="b" * 64),
    )

    with pytest.raises(ValueError, match="disagree on normalized semantic evidence"):
        compare_r4a_verification_receipt_files(left, right)


@pytest.mark.parametrize("mutation", ("incomplete", "duplicate", "failed_check"))
def test_cross_platform_comparator_rejects_malformed_pass_evidence(
    tmp_path: Path,
    mutation: str,
) -> None:
    left = tmp_path / "linux.json"
    right = tmp_path / "windows.json"
    receipt = _passing_receipt().model_dump(mode="json")
    if mutation == "incomplete":
        receipt["checks"] = receipt["checks"][:-1]
    elif mutation == "duplicate":
        receipt["checks"][-1] = receipt["checks"][0]
    else:
        receipt["checks"][0]["passed"] = False
    payload = json.dumps(receipt, sort_keys=True)
    left.write_text(payload, encoding="utf-8")
    right.write_text(payload, encoding="utf-8")

    with pytest.raises(ValidationError):
        compare_r4a_verification_receipt_files(left, right)


def test_cross_platform_comparator_rejects_the_same_receipt_file(tmp_path: Path) -> None:
    receipt_path = tmp_path / "linux.json"
    _write_receipt(receipt_path, _passing_receipt(platform_name="linux"))

    with pytest.raises(ValueError, match="two distinct receipt files"):
        compare_r4a_verification_receipt_files(receipt_path, receipt_path)


@pytest.mark.parametrize(
    ("left_receipt", "right_receipt", "message"),
    (
        (
            _passing_receipt(platform_name="linux"),
            _passing_receipt(platform_name="linux"),
            "one Linux and one Windows",
        ),
        (
            _passing_receipt(platform_name="macos"),
            _passing_receipt(platform_name="windows"),
            "one Linux and one Windows",
        ),
        (
            _passing_receipt(platform_name="linux", runner_attested=False),
            _passing_receipt(platform_name="windows"),
            "runner OS attestation",
        ),
        (
            _passing_receipt(platform_name="linux", python_minor="3.12"),
            _passing_receipt(platform_name="windows", python_minor="3.13"),
            "same Python minor version",
        ),
    ),
)
def test_cross_platform_comparator_rejects_invalid_runtime_pairs(
    tmp_path: Path,
    left_receipt: VerificationReceipt,
    right_receipt: VerificationReceipt,
    message: str,
) -> None:
    left = tmp_path / "left.json"
    right = tmp_path / "right.json"
    _write_receipt(left, left_receipt)
    _write_receipt(right, right_receipt)

    with pytest.raises(ValueError, match=message):
        compare_r4a_verification_receipt_files(left, right)


def test_pass_receipt_rejects_an_unbound_replay_hash() -> None:
    payload = _passing_receipt(platform_name="linux").model_dump(mode="json")
    payload["checks"][0]["evidence_sha256"] = "b" * 64

    with pytest.raises(ValidationError, match="bind the replay check"):
        VerificationReceipt.model_validate(payload)


def test_pass_receipt_rejects_misleading_replay_detail() -> None:
    payload = _passing_receipt(platform_name="linux").model_dump(mode="json")
    payload["checks"][0]["detail"] = f"semantic_sha256={'b' * 64}"

    with pytest.raises(ValidationError, match="canonical replay-check detail"):
        VerificationReceipt.model_validate(payload)


def test_mismatched_replay_receipt_reports_difference(
    replay_pair: tuple[R4ARunResult, R4ARunResult],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first, second = replay_pair

    def run_with_mismatch(*, output_dir: Path, **_kwargs: object) -> R4ARunResult:
        return replace(second, semantic_sha256="b" * 64) if output_dir.name == "run-b" else first

    monkeypatch.setattr(r4a, "run_r4a_once", run_with_mismatch)
    receipt = verify_r4a(artifact_root=tmp_path / "mismatch").receipt
    replay = next(check for check in receipt.checks if check.name == "two_run_replay")
    assert receipt.status == "FAIL"
    assert replay.passed is False
    assert replay.detail == "two fresh replay semantic hashes differ"


def test_invalid_config_persists_a_fail_closed_verification_receipt(tmp_path: Path) -> None:
    invalid_config = tmp_path / "invalid-r4a.yaml"
    invalid_config.write_text("release: NOT-R4A\nunknown_control: true\n", encoding="utf-8")
    artifact_root = tmp_path / "invalid-config-gate"

    result = verify_r4a(artifact_root=artifact_root, config_path=invalid_config)

    assert result.passed is False
    assert result.receipt.status == "FAIL"
    assert result.receipt.full_g4_status == "OPEN"
    assert len(result.receipt.checks) == 1
    assert result.receipt.checks[0].name == "setup_or_execution"
    assert result.receipt.checks[0].passed is False
    assert "ValidationError" in result.receipt.checks[0].detail
    assert result.artifact.path.is_file()
    persisted = json.loads(result.artifact.path.read_text(encoding="utf-8"))
    assert persisted == result.receipt.model_dump(mode="json")
