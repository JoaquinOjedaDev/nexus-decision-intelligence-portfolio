from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from nexus.ingestion.r2d_a2b_contracts import (
    R2D_A2B1_PASS_CHECK_NAMES,
    R2DA2b1CrashWindowEvidence,
    R2DA2b1VerificationCheck,
    R2DA2b1VerificationReceipt,
    r2d_a2b1_code_sha256,
)
from nexus.ingestion.r2d_contracts import canonical_json_sha256

pytestmark = pytest.mark.r2d_service

ROOT = Path(__file__).resolve().parents[1]


def _crash_evidence(
    failpoint: str,
) -> R2DA2b1CrashWindowEvidence:
    before_commit = failpoint == "before_db_commit"
    payload: dict[str, object] = {
        "contract_version": "r2d.a2b1.crash-window.v1",
        "failpoint": failpoint,
        "partition": 0 if before_commit else 1,
        "offset": 0,
        "expected_exit_code": 86 if before_commit else 87,
        "observed_exit_code": 86 if before_commit else 87,
        "db_next_before": 0,
        "db_next_after": 0 if before_commit else 1,
        "broker_next_before": 0,
        "broker_next_after": 0,
        "inbox_rows_before": 0,
        "inbox_rows_after": 0 if before_commit else 1,
    }
    return R2DA2b1CrashWindowEvidence.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def _receipt() -> R2DA2b1VerificationReceipt:
    checks = tuple(
        R2DA2b1VerificationCheck(name=name, passed=True, evidence_sha256="a" * 64)
        for name in R2D_A2B1_PASS_CHECK_NAMES
    )
    payload: dict[str, object] = {
        "contract_version": "r2d.a2b1.verification-receipt.v1",
        "release": "R2D-A2b-1",
        "status": "PASS",
        "checks": [check.model_dump(mode="json") for check in checks],
        "topic": "inventory.adjusted.v1",
        "partition_count": 2,
        "record_count": 9,
        "initial_starts": [0, 0],
        "restart_starts": [1, 1],
        "ends": [3, 6],
        "next_offsets": [3, 6],
        "worker_exit_codes": [86, 87],
        "worker_survivor_count": 0,
        "data_config_sha256": "b" * 64,
        "service_config_sha256": "c" * 64,
        "migration_sha256": "d" * 64,
        "code_sha256": "e" * 64,
        "broker_ack_sha256": "f" * 64,
        "transcript_sha256": "1" * 64,
        "lineage_sha256": "2" * 64,
        "raw_value_multiset_sha256": "3" * 64,
        "before_db_commit_evidence_sha256": "4" * 64,
        "after_db_commit_evidence_sha256": "5" * 64,
        "crash_recovery_status": "PASS",
        "downstream_parity_status": "NOT_RUN",
        "r2d_a2_status": "IN_PROGRESS",
        "full_r2d_a_status": "OPEN",
        "full_g2_status": "OPEN",
        "exactly_once_claim": False,
    }
    return R2DA2b1VerificationReceipt.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def test_both_crash_window_contracts_are_distinct_and_content_addressed() -> None:
    before = _crash_evidence("before_db_commit")
    after = _crash_evidence("after_db_commit_before_broker_commit")

    assert before.db_next_after == before.db_next_before
    assert before.inbox_rows_after == before.inbox_rows_before
    assert after.db_next_after == after.db_next_before + 1
    assert after.broker_next_after == after.broker_next_before
    assert before.content_sha256 != after.content_sha256


def test_crash_window_contract_rejects_false_boundary_evidence() -> None:
    before = _crash_evidence("before_db_commit")
    after = _crash_evidence("after_db_commit_before_broker_commit")

    with pytest.raises(ValidationError, match="pre-commit crash created"):
        R2DA2b1CrashWindowEvidence.model_validate(
            {**before.model_dump(mode="python"), "db_next_after": 1}
        )
    with pytest.raises(ValidationError, match="required recovery gap"):
        R2DA2b1CrashWindowEvidence.model_validate(
            {**after.model_dump(mode="python"), "broker_next_after": 1}
        )
    with pytest.raises(ValidationError, match="exit code differs"):
        R2DA2b1CrashWindowEvidence.model_validate(
            {**before.model_dump(mode="python"), "observed_exit_code": 90}
        )


def test_a2b1_receipt_is_exact_honest_and_does_not_claim_downstream_parity() -> None:
    receipt = _receipt()

    assert tuple(check.name for check in receipt.checks) == R2D_A2B1_PASS_CHECK_NAMES
    assert receipt.crash_recovery_status == "PASS"
    assert receipt.downstream_parity_status == "NOT_RUN"
    assert receipt.r2d_a2_status == "IN_PROGRESS"
    assert receipt.full_r2d_a_status == "OPEN"
    assert receipt.full_g2_status == "OPEN"
    assert receipt.exactly_once_claim is False
    assert len(r2d_a2b1_code_sha256(ROOT)) == 64

    with pytest.raises(ValidationError, match="incomplete, duplicated, or reordered"):
        R2DA2b1VerificationReceipt.model_validate(
            {**receipt.model_dump(mode="python"), "checks": list(reversed(receipt.checks))}
        )
    with pytest.raises(ValidationError, match="hard-crash exit codes"):
        R2DA2b1VerificationReceipt.model_validate(
            {**receipt.model_dump(mode="python"), "worker_exit_codes": [86, 88]}
        )


def test_a2b1_uses_spawned_process_death_not_a_caught_exception() -> None:
    verifier = (ROOT / "src" / "nexus" / "r2d_a2b.py").read_text(encoding="utf-8")
    service = (ROOT / "src" / "nexus" / "ingestion" / "r2d_service.py").read_text(encoding="utf-8")

    assert 'multiprocessing.get_context("spawn")' in verifier
    assert "os._exit(exit_code)" in verifier
    assert "crash_handler(failpoint)" in service
    assert 'failpoint == "before_db_commit"' in service
    assert 'failpoint == "after_db_commit_before_broker_commit"' in service


def test_a2b1_ci_job_is_isolated_receipted_and_always_torn_down() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "r2d-service-a2b1:" in workflow
    assert "nexus-r2d-a2b1-${{ github.run_id }}-${{ github.run_attempt }}" in workflow
    assert "verify-service-a2b1" in workflow
    assert "r2d-a2b1-service-receipt-linux-py313" in workflow
    assert "if: always()" in workflow
    assert "down -v --remove-orphans" in workflow
    assert "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}" in workflow
