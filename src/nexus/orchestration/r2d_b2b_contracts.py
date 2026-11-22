"""Strict contracts for persistent Dagster execution in the bounded R2D-B2b gate."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Literal, Self

import yaml
from pydantic import StringConstraints, field_validator, model_validator

from nexus.ingestion.r2d_contracts import (
    R2DContract,
    R2DSha256,
    canonical_json_sha256,
    r2d_lf_canonical_file_sha256,
)
from nexus.modeling.r3_artifacts import canonical_json_bytes
from nexus.orchestration.r2b_assets import r2b_orchestration_code_sha256
from nexus.orchestration.r2b_contracts import load_r2b_config, r2b_config_sha256

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_R2D_B2B_CONFIG_PATH = PROJECT_ROOT / "configs" / "orchestration" / "r2d-b2b.yaml"
DEFAULT_R2D_B2B_DAGSTER_CONFIG_PATH = (
    PROJECT_ROOT / "configs" / "orchestration" / "r2d-b2b-dagster.yaml"
)
DEFAULT_R2D_B2B_WORKSPACE_PATH = (
    PROJECT_ROOT / "configs" / "orchestration" / "r2d-b2b-workspace.yaml"
)

R2D_B2B_PARTITION_KEYS = (
    "r2b-v1__aa25517d49cb6c36b76e9b6a218a83e7419359f5a7c40626412797ca13f8d768__20260620T000000Z",
    "r2b-v1__aa25517d49cb6c36b76e9b6a218a83e7419359f5a7c40626412797ca13f8d768__20260630T000000Z",
)

R2DB2BIdentifier = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/@-]*$", min_length=1, max_length=180),
]
R2DB2BCheckName = Literal[
    "inherited_r2b_authority",
    "observed_postgres_image",
    "postgres_backed_dagster_storage",
    "daemon_epoch_one_heartbeat",
    "actual_bounded_backfill_submission",
    "first_partition_daemon_dispatch",
    "daemon_restart_continuity",
    "exact_partition_run_success",
    "backfill_semantic_parity",
    "cross_process_persistent_readback",
    "immutable_artifact_set",
    "bounded_resource_cleanup",
    "honest_scope",
]
R2D_B2B_PASS_CHECK_NAMES: tuple[R2DB2BCheckName, ...] = (
    "inherited_r2b_authority",
    "observed_postgres_image",
    "postgres_backed_dagster_storage",
    "daemon_epoch_one_heartbeat",
    "actual_bounded_backfill_submission",
    "first_partition_daemon_dispatch",
    "daemon_restart_continuity",
    "exact_partition_run_success",
    "backfill_semantic_parity",
    "cross_process_persistent_readback",
    "immutable_artifact_set",
    "bounded_resource_cleanup",
    "honest_scope",
)
R2DB2BEvidenceDataset = Literal[
    "r2d.b2b.inherited_r2b",
    "r2d.b2b.service_image",
    "r2d.b2b.instance",
    "r2d.b2b.daemon_epoch_one",
    "r2d.b2b.backfill_submission",
    "r2d.b2b.daemon_epoch_two",
    "r2d.b2b.run_storage",
    "r2d.b2b.semantic_parity",
    "r2d.b2b.restart_readback",
    "r2d.b2b.resource",
    "r2d.b2b.scope",
]
R2D_B2B_EVIDENCE_DATASETS: tuple[R2DB2BEvidenceDataset, ...] = (
    "r2d.b2b.inherited_r2b",
    "r2d.b2b.service_image",
    "r2d.b2b.instance",
    "r2d.b2b.daemon_epoch_one",
    "r2d.b2b.backfill_submission",
    "r2d.b2b.daemon_epoch_two",
    "r2d.b2b.run_storage",
    "r2d.b2b.semantic_parity",
    "r2d.b2b.restart_readback",
    "r2d.b2b.resource",
    "r2d.b2b.scope",
)


class R2DB2BInheritedR2B(R2DContract):
    config_path: Literal["configs/orchestration/r2b-backfill.yaml"]
    config_sha256: Literal["8ea10d99101638fe8b3e0194ae255246b44c62c0f6378761eb303db340b7af8b"]
    orchestration_code_sha256: Literal[
        "63e6b658c26049a36fcf569121d4f86a740255d1c71517c8306b8aac8ace44e9"
    ]


class R2DB2BDagsterContract(R2DContract):
    job_name: Literal["r2d_b2b_backfill_job"]
    location_name: Literal["r2d_b2b"]
    partition_keys: tuple[str, ...]
    expected_run_count: Literal[2]
    maximum_concurrent_runs: Literal[1]
    daemon_heartbeat_timeout_seconds: Literal[45]
    dequeue_interval_seconds: Literal[5]
    run_timeout_seconds: Literal[240]
    queued_pause_observation_seconds: Literal[2]
    require_actual_backfill_record: Literal[True]
    require_daemon_restart: Literal[True]
    require_cross_process_readback: Literal[True]

    @field_validator("partition_keys", mode="before")
    @classmethod
    def freeze_partition_keys(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def exact_partitions_are_frozen(self) -> Self:
        if self.partition_keys != R2D_B2B_PARTITION_KEYS:
            raise ValueError("B2b partition keys differ from the inherited R2B contract")
        return self


class R2DB2BPostgresContract(R2DContract):
    configured_reference: Literal["postgres:17.6-alpine"]
    schema_name: Literal["public"]
    run_storage_class: Literal["dagster_postgres.run_storage.run_storage.PostgresRunStorage"]
    event_log_storage_class: Literal["dagster_postgres.event_log.event_log.PostgresEventLogStorage"]
    schedule_storage_class: Literal[
        "dagster_postgres.schedule_storage.schedule_storage.PostgresScheduleStorage"
    ]
    required_tables: tuple[str, ...]

    @field_validator("required_tables", mode="before")
    @classmethod
    def freeze_tables(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def table_contract_is_exact(self) -> Self:
        expected = (
            "asset_keys",
            "bulk_actions",
            "event_logs",
            "jobs",
            "run_tags",
            "runs",
            "secondary_indexes",
        )
        if self.required_tables != expected:
            raise ValueError("B2b required PostgreSQL tables are incomplete or reordered")
        return self


class R2DB2BResourceBudget(R2DContract):
    wall_time_seconds: Literal[300]
    peak_rss_mib: Literal[4096]
    maximum_native_threads: Literal[512]
    artifact_mib: Literal[1024]
    shutdown_seconds: Literal[15]
    maximum_descendant_processes: Literal[32]


class R2DB2BScopeContract(R2DContract):
    inherited_r2b_status: Literal["PASS"]
    persistent_dagster_status: Literal["PASS"]
    actual_backfill_status: Literal["PASS"]
    dagster_process_restart_status: Literal["PASS"]
    retained_multiservice_restart_status: Literal["NOT_RUN"]
    operational_alert_delivery_status: Literal["NOT_RUN"]
    full_r2d_b_status: Literal["IN_PROGRESS"]
    full_g2_status: Literal["OPEN"]


class R2DB2BConfig(R2DContract):
    release: Literal["R2D-B2b"]
    contract_version: Literal["1.0.0"]
    inherited_r2b: R2DB2BInheritedR2B
    dagster: R2DB2BDagsterContract
    postgres: R2DB2BPostgresContract
    resource_budget: R2DB2BResourceBudget
    scope: R2DB2BScopeContract


class R2DB2BServiceImageEvidence(R2DContract):
    contract_version: Literal["r2d.b2b.service-image.v1"]
    service: Literal["postgres"]
    configured_reference: Literal["postgres:17.6-alpine"]
    observed_image_id: Annotated[str, StringConstraints(pattern=r"^sha256:[a-f0-9]{64}$")]
    operating_system: Literal["linux"]
    architecture: Literal["amd64"]
    content_sha256: R2DSha256

    @model_validator(mode="after")
    def evidence_is_content_addressed(self) -> Self:
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("B2b service-image evidence content SHA-256 mismatch")
        return self


class R2DB2BArtifactRef(R2DContract):
    dataset: R2DB2BEvidenceDataset
    relative_path: str
    content_sha256: R2DSha256
    schema_sha256: R2DSha256
    file_sha256: R2DSha256
    row_count: Literal[1]


class R2DB2BVerificationCheck(R2DContract):
    name: R2DB2BCheckName
    passed: Literal[True]
    evidence_sha256: R2DSha256


class R2DB2BRuntime(R2DContract):
    operating_system: str
    machine: str
    python_version: str
    dagster_version: str
    dagster_postgres_version: str
    github_runner_os_attested: bool


class R2DB2BVerificationReceipt(R2DContract):
    contract_version: Literal["r2d.b2b.verification-receipt.v1"]
    release: Literal["R2D-B2b"]
    status: Literal["PASS"]
    checks: tuple[R2DB2BVerificationCheck, ...]
    evidence_refs: tuple[R2DB2BArtifactRef, ...]
    config_sha256: R2DSha256
    code_sha256: R2DSha256
    inherited_r2b_config_sha256: R2DSha256
    inherited_r2b_orchestration_code_sha256: R2DSha256
    service_image_sha256: R2DSha256
    service_image_file_sha256: R2DSha256
    instance_sha256: R2DSha256
    daemon_epoch_one_sha256: R2DSha256
    backfill_submission_sha256: R2DSha256
    daemon_epoch_two_sha256: R2DSha256
    run_storage_sha256: R2DSha256
    semantic_parity_sha256: R2DSha256
    restart_readback_sha256: R2DSha256
    resource_sha256: R2DSha256
    artifact_set_sha256: R2DSha256
    scope_sha256: R2DSha256
    backfill_id: R2DB2BIdentifier
    dagster_run_ids: tuple[R2DB2BIdentifier, ...]
    partition_keys: tuple[str, ...]
    runtime: R2DB2BRuntime
    inherited_r2b_status: Literal["PASS"]
    persistent_dagster_status: Literal["PASS"]
    actual_backfill_status: Literal["PASS"]
    dagster_process_restart_status: Literal["PASS"]
    retained_multiservice_restart_status: Literal["NOT_RUN"]
    operational_alert_delivery_status: Literal["NOT_RUN"]
    full_r2d_b_status: Literal["IN_PROGRESS"]
    full_g2_status: Literal["OPEN"]
    high_availability_claim: Literal[False]
    automatic_scheduling_claim: Literal[False]
    retained_multiservice_restart_claim: Literal[False]
    content_sha256: R2DSha256

    @field_validator("checks", "evidence_refs", "dagster_run_ids", "partition_keys", mode="before")
    @classmethod
    def freeze_sequences(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def receipt_is_honest_complete_and_bound(self) -> Self:
        if tuple(item.name for item in self.checks) != R2D_B2B_PASS_CHECK_NAMES:
            raise ValueError("B2b verification checks are incomplete, duplicated, or reordered")
        if tuple(item.dataset for item in self.evidence_refs) != R2D_B2B_EVIDENCE_DATASETS:
            raise ValueError("B2b evidence topology is incomplete, duplicated, or reordered")
        if self.partition_keys != R2D_B2B_PARTITION_KEYS:
            raise ValueError("B2b receipt partition keys differ from the frozen contract")
        if len(self.dagster_run_ids) != 2 or len(set(self.dagster_run_ids)) != 2:
            raise ValueError("B2b receipt must bind exactly two distinct Dagster runs")
        if tuple(sorted(self.dagster_run_ids)) != self.dagster_run_ids:
            raise ValueError("B2b Dagster run IDs must be canonically ordered")
        if self.evidence_refs[1].file_sha256 != self.service_image_file_sha256:
            raise ValueError("B2b receipt does not bind the service-image evidence bytes")
        expected: dict[R2DB2BCheckName, str] = {
            "inherited_r2b_authority": self.inherited_r2b_config_sha256,
            "observed_postgres_image": self.service_image_sha256,
            "postgres_backed_dagster_storage": self.instance_sha256,
            "daemon_epoch_one_heartbeat": self.daemon_epoch_one_sha256,
            "actual_bounded_backfill_submission": self.backfill_submission_sha256,
            "first_partition_daemon_dispatch": self.run_storage_sha256,
            "daemon_restart_continuity": self.daemon_epoch_two_sha256,
            "exact_partition_run_success": self.run_storage_sha256,
            "backfill_semantic_parity": self.semantic_parity_sha256,
            "cross_process_persistent_readback": self.restart_readback_sha256,
            "immutable_artifact_set": self.artifact_set_sha256,
            "bounded_resource_cleanup": self.resource_sha256,
            "honest_scope": self.scope_sha256,
        }
        if any(item.evidence_sha256 != expected[item.name] for item in self.checks):
            raise ValueError("B2b verification check is detached from its evidence")
        artifact_rows = [item.model_dump(mode="json") for item in self.evidence_refs]
        if canonical_json_sha256(artifact_rows) != self.artifact_set_sha256:
            raise ValueError("B2b artifact-set SHA-256 mismatch")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("B2b receipt content SHA-256 mismatch")
        return self


R2D_B2B_CODE_FILES = (
    "configs/orchestration/r2d-b2b.yaml",
    "configs/orchestration/r2d-b2b-dagster.yaml",
    "configs/orchestration/r2d-b2b-workspace.yaml",
    "src/nexus/orchestration/r2d_b2b_contracts.py",
    "src/nexus/orchestration/r2d_b2b_definitions.py",
    "src/nexus/r2d_b2b.py",
    "src/nexus/orchestration/r2b_assets.py",
    "src/nexus/orchestration/r2b_checkpoint.py",
    "src/nexus/orchestration/r2b_contracts.py",
    "src/nexus/r2.py",
    "src/nexus/cli.py",
    "tests/test_r2d_b2b_contracts.py",
    "infra/compose.yaml",
    ".github/workflows/ci.yml",
    "pyproject.toml",
    "uv.lock",
)


def load_r2d_b2b_config(path: Path = DEFAULT_R2D_B2B_CONFIG_PATH) -> R2DB2BConfig:
    config = R2DB2BConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
    inherited_path = PROJECT_ROOT / config.inherited_r2b.config_path
    inherited = load_r2b_config(inherited_path)
    if r2b_config_sha256(inherited) != config.inherited_r2b.config_sha256:
        raise ValueError("B2b inherited R2B config identity mismatch")
    if (
        r2b_orchestration_code_sha256(PROJECT_ROOT)
        != config.inherited_r2b.orchestration_code_sha256
    ):
        raise ValueError("B2b inherited R2B orchestration identity mismatch")
    expected_keys = tuple(
        inherited.partition_key(partition) for partition in inherited.partition_contract.partitions
    )
    if expected_keys != config.dagster.partition_keys:
        raise ValueError("B2b partition keys do not derive from the inherited R2B config")
    return config


def r2d_b2b_config_sha256(config: R2DB2BConfig) -> str:
    return canonical_json_sha256(config.model_dump(mode="json"))


def r2d_b2b_code_sha256(project_root: Path = PROJECT_ROOT) -> str:
    root = project_root.resolve()
    identities = {path: r2d_lf_canonical_file_sha256(root / path) for path in R2D_B2B_CODE_FILES}
    return canonical_json_sha256(identities)


def load_r2d_b2b_service_image(path: Path) -> R2DB2BServiceImageEvidence:
    try:
        raw = path.read_bytes()
        parsed = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot load B2b service-image evidence: {path}") from exc
    if raw != canonical_json_bytes(parsed) + b"\n":
        raise ValueError("B2b service-image evidence must be canonical JSON")
    return R2DB2BServiceImageEvidence.model_validate(parsed)
