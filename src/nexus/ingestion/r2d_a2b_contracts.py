"""Strict evidence contracts for the R2D-A2b-1 crash-recovery checkpoint."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

from pydantic import NonNegativeInt, PositiveInt, field_validator, model_validator

from nexus.ingestion.r2d_contracts import (
    R2DContract,
    R2DSha256,
    canonical_json_sha256,
    r2d_lf_canonical_file_sha256,
)

R2DA2b1CheckName = Literal[
    "hard_crash_before_db_commit",
    "hard_crash_after_db_commit_before_broker_commit",
    "restart_redelivery_is_idempotent",
    "broker_below_db_catches_up",
    "broker_equal_db_continues",
    "broker_ahead_db_fails_closed",
    "physical_conflict_fails_closed",
    "full_barrier_converges",
    "crash_workers_leave_no_survivors",
]
R2D_A2B1_PASS_CHECK_NAMES: tuple[R2DA2b1CheckName, ...] = (
    "hard_crash_before_db_commit",
    "hard_crash_after_db_commit_before_broker_commit",
    "restart_redelivery_is_idempotent",
    "broker_below_db_catches_up",
    "broker_equal_db_continues",
    "broker_ahead_db_fails_closed",
    "physical_conflict_fails_closed",
    "full_barrier_converges",
    "crash_workers_leave_no_survivors",
)

R2D_A2B1_CODE_FILES = (
    "src/nexus/r2d_a2b.py",
    "src/nexus/cli.py",
    "src/nexus/ingestion/r2d_a2b_contracts.py",
    "src/nexus/ingestion/r2d_service.py",
    "src/nexus/ingestion/r2d_service_contracts.py",
    "src/nexus/ingestion/r2d_capture.py",
    "src/nexus/ingestion/r2d_contracts.py",
    "infra/postgres/migrations/001_r2d_capture.sql",
    "infra/compose.yaml",
    ".github/workflows/ci.yml",
    "pyproject.toml",
    "uv.lock",
)


class R2DA2b1CrashWindowEvidence(R2DContract):
    """One real child-process death and its durable/broker boundary observations."""

    contract_version: Literal["r2d.a2b1.crash-window.v1"]
    failpoint: Literal["before_db_commit", "after_db_commit_before_broker_commit"]
    partition: NonNegativeInt
    offset: NonNegativeInt
    expected_exit_code: PositiveInt
    observed_exit_code: PositiveInt
    db_next_before: NonNegativeInt
    db_next_after: NonNegativeInt
    broker_next_before: NonNegativeInt
    broker_next_after: NonNegativeInt
    inbox_rows_before: NonNegativeInt
    inbox_rows_after: NonNegativeInt
    content_sha256: R2DSha256

    def content_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    @model_validator(mode="after")
    def evidence_matches_the_named_boundary(self) -> Self:
        if self.partition >= 2:
            raise ValueError("crash evidence partition is outside the frozen topology")
        if self.observed_exit_code != self.expected_exit_code:
            raise ValueError("crash worker exit code differs from its failpoint contract")
        if self.failpoint == "before_db_commit":
            if (
                self.db_next_after != self.db_next_before
                or self.broker_next_after != self.broker_next_before
                or self.inbox_rows_after != self.inbox_rows_before
            ):
                raise ValueError("pre-commit crash created a durable or broker effect")
        elif (
            self.db_next_after != self.db_next_before + 1
            or self.broker_next_after != self.broker_next_before
            or self.inbox_rows_after != self.inbox_rows_before + 1
        ):
            raise ValueError("post-DB/pre-broker crash did not expose the required recovery gap")
        if self.content_sha256 != canonical_json_sha256(self.content_payload()):
            raise ValueError("crash-window evidence content SHA-256 mismatch")
        return self


class R2DA2b1VerificationCheck(R2DContract):
    name: R2DA2b1CheckName
    passed: Literal[True]
    evidence_sha256: R2DSha256


class R2DA2b1VerificationReceipt(R2DContract):
    """Bounded crash/reconciliation PASS; service-to-R2A parity remains unrun."""

    contract_version: Literal["r2d.a2b1.verification-receipt.v1"]
    release: Literal["R2D-A2b-1"]
    status: Literal["PASS"]
    checks: tuple[R2DA2b1VerificationCheck, ...]
    topic: Literal["inventory.adjusted.v1"]
    partition_count: Literal[2]
    record_count: PositiveInt
    initial_starts: tuple[NonNegativeInt, ...]
    restart_starts: tuple[NonNegativeInt, ...]
    ends: tuple[NonNegativeInt, ...]
    next_offsets: tuple[NonNegativeInt, ...]
    worker_exit_codes: tuple[PositiveInt, ...]
    worker_survivor_count: Literal[0]
    data_config_sha256: R2DSha256
    service_config_sha256: R2DSha256
    migration_sha256: R2DSha256
    code_sha256: R2DSha256
    broker_ack_sha256: R2DSha256
    transcript_sha256: R2DSha256
    lineage_sha256: R2DSha256
    raw_value_multiset_sha256: R2DSha256
    before_db_commit_evidence_sha256: R2DSha256
    after_db_commit_evidence_sha256: R2DSha256
    crash_recovery_status: Literal["PASS"]
    downstream_parity_status: Literal["NOT_RUN"]
    r2d_a2_status: Literal["IN_PROGRESS"]
    full_r2d_a_status: Literal["OPEN"]
    full_g2_status: Literal["OPEN"]
    exactly_once_claim: Literal[False]
    content_sha256: R2DSha256

    @field_validator(
        "checks",
        "initial_starts",
        "restart_starts",
        "ends",
        "next_offsets",
        "worker_exit_codes",
        mode="before",
    )
    @classmethod
    def sequences_are_immutable(cls, value: object) -> tuple[object, ...] | object:
        return tuple(value) if isinstance(value, list) else value

    def content_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    @model_validator(mode="after")
    def receipt_is_complete_honest_and_content_addressed(self) -> Self:
        if tuple(check.name for check in self.checks) != R2D_A2B1_PASS_CHECK_NAMES:
            raise ValueError("A2b-1 receipt checks are incomplete, duplicated, or reordered")
        if self.record_count > 64:
            raise ValueError("A2b-1 record count exceeds the bounded contract")
        if self.initial_starts != (0, 0):
            raise ValueError("A2b-1 must begin from a receipted fresh-empty state")
        if any(len(vector) != 2 for vector in (self.restart_starts, self.ends, self.next_offsets)):
            raise ValueError("A2b-1 must bind exactly two partitions")
        if self.next_offsets != self.ends:
            raise ValueError("A2b-1 durable progress must reach the frozen end vector")
        if self.worker_exit_codes != (86, 87):
            raise ValueError("A2b-1 must bind both deterministic hard-crash exit codes")
        if self.content_sha256 != canonical_json_sha256(self.content_payload()):
            raise ValueError("A2b-1 receipt content SHA-256 mismatch")
        return self


def r2d_a2b1_code_sha256(project_root: Path) -> str:
    """Bind every checked-in file that can affect the A2b-1 claim."""

    root = project_root.resolve()
    identities = {path: r2d_lf_canonical_file_sha256(root / path) for path in R2D_A2B1_CODE_FILES}
    return canonical_json_sha256(identities)
