"""Strict contracts for the bounded R2D-B2a live dead-letter service gate."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Literal, Self

import yaml
from pydantic import (
    AwareDatetime,
    NonNegativeInt,
    StringConstraints,
    field_validator,
    model_validator,
)

from nexus.ingestion.r2d_b1_contracts import load_r2d_b1_config, r2d_b1_config_sha256
from nexus.ingestion.r2d_contracts import (
    R2DContract,
    R2DSha256,
    canonical_json_sha256,
    r2d_lf_canonical_file_sha256,
)
from nexus.modeling.r3_artifacts import canonical_json_bytes

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_R2D_B2A_CONFIG_PATH = PROJECT_ROOT / "configs" / "ingestion" / "r2d-b2a.yaml"

R2DB2ATopic = Literal[
    "inventory.adjusted.v1",
    "inventory.adjusted.dlq.v1",
    "inventory.adjusted.redrive.v1",
]
R2DB2AIdentifier = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/@-]*$", min_length=1, max_length=160),
]
R2DB2ACheckName = Literal[
    "inherited_b1_authority",
    "observed_service_image",
    "exact_service_topic_topology",
    "real_source_acks_all",
    "live_poison_isolation",
    "byte_preserving_dlq_round_trip",
    "append_only_operator_redrive",
    "replacement_returns_to_source",
    "original_dlq_immutability",
    "manual_commit_convergence",
    "immutable_artifact_set",
    "honest_scope",
]
R2D_B2A_PASS_CHECK_NAMES: tuple[R2DB2ACheckName, ...] = (
    "inherited_b1_authority",
    "observed_service_image",
    "exact_service_topic_topology",
    "real_source_acks_all",
    "live_poison_isolation",
    "byte_preserving_dlq_round_trip",
    "append_only_operator_redrive",
    "replacement_returns_to_source",
    "original_dlq_immutability",
    "manual_commit_convergence",
    "immutable_artifact_set",
    "honest_scope",
)
R2D_B2A_EVIDENCE_DATASETS = (
    "r2d.b2a.inherited_b1",
    "r2d.b2a.service_image",
    "r2d.b2a.topic_topology",
    "r2d.b2a.source_ack",
    "r2d.b2a.route",
    "r2d.b2a.dead_letter_ack",
    "r2d.b2a.dead_letter_round_trip",
    "r2d.b2a.redrive_ack",
    "r2d.b2a.redrive",
    "r2d.b2a.replacement_ack",
    "r2d.b2a.manual_commit",
    "r2d.b2a.scope",
)


class R2DB2AInheritedB1(R2DContract):
    config_path: Literal["configs/ingestion/r2d-b1.yaml"]
    config_sha256: Literal["61dacd3ef618138b2e91701c3bc032e91e978065c08bb916429e1a2f18323b3e"]


class R2DB2ABrokerContract(R2DContract):
    source_topic: Literal["inventory.adjusted.v1"]
    dead_letter_topic: Literal["inventory.adjusted.dlq.v1"]
    redrive_audit_topic: Literal["inventory.adjusted.redrive.v1"]
    partition_count: Literal[2]
    replication_factor: Literal[1]
    retention_ms: Literal[-1]
    producer_acks: Literal["all"]
    producer_enable_idempotence: Literal[True]
    router_consumer_group_id: Literal["nexus-r2d-b2a-router-v1"]
    dead_letter_audit_consumer_group_id: Literal["nexus-r2d-b2a-dlq-audit-v1"]
    redrive_audit_consumer_group_id: Literal["nexus-r2d-b2a-redrive-audit-v1"]
    consumer_auto_offset_commit: Literal[False]
    consumer_auto_offset_store: Literal[False]
    isolation_level: Literal["read_committed"]

    @property
    def topics(self) -> tuple[R2DB2ATopic, ...]:
        return (self.source_topic, self.dead_letter_topic, self.redrive_audit_topic)


class R2DB2AOperatorContract(R2DContract):
    operator_id: Literal["operator:nexus-b2a-ci"]
    reason: Literal["Correct unsupported event version after governed review"]
    submitted_at: AwareDatetime


class R2DB2AGateContract(R2DContract):
    source_fixture_records: Literal[2]
    expected_forward_records: Literal[1]
    expected_dead_letter_records: Literal[1]
    expected_redrive_requests: Literal[1]
    expected_replacement_records: Literal[1]
    poll_timeout_seconds: Literal[30]
    require_fresh_empty_topics: Literal[True]


class R2DB2AScopeContract(R2DContract):
    portable_b1_status: Literal["PASS"]
    service_dead_letter_topic_status: Literal["PASS"]
    persistent_dagster_status: Literal["NOT_RUN"]
    retained_service_restart_status: Literal["NOT_RUN"]
    operational_alert_delivery_status: Literal["NOT_RUN"]
    full_r2d_b_status: Literal["IN_PROGRESS"]
    full_g2_status: Literal["OPEN"]


class R2DB2AConfig(R2DContract):
    release: Literal["R2D-B2a"]
    contract_version: Literal["1.0.0"]
    inherited_b1: R2DB2AInheritedB1
    broker: R2DB2ABrokerContract
    operator: R2DB2AOperatorContract
    gate: R2DB2AGateContract
    scope: R2DB2AScopeContract


class R2DB2AServiceImageEvidence(R2DContract):
    contract_version: Literal["r2d.b2a.service-image.v1"]
    service: Literal["redpanda"]
    configured_reference: Literal["docker.redpanda.com/redpandadata/redpanda:v26.1.9"]
    observed_image_id: Annotated[str, StringConstraints(pattern=r"^sha256:[a-f0-9]{64}$")]
    operating_system: Literal["linux"]
    architecture: Literal["amd64"]
    content_sha256: R2DSha256

    @model_validator(mode="after")
    def image_evidence_is_content_addressed(self) -> Self:
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("B2a service-image evidence content SHA-256 mismatch")
        return self


class R2DB2ABrokerAcknowledgement(R2DContract):
    acknowledgement_kind: Literal["broker_acks_all"]
    topic: R2DB2ATopic
    partition: NonNegativeInt
    offset: NonNegativeInt
    key_hex: str
    value_sha256: R2DSha256
    broker_timestamp_ms: NonNegativeInt

    @model_validator(mode="after")
    def acknowledgement_is_bounded(self) -> Self:
        if self.partition >= 2:
            raise ValueError("B2a acknowledgement partition is outside the frozen topology")
        if not self.key_hex or len(self.key_hex) % 2:
            raise ValueError("B2a acknowledgement key must be nonempty even hexadecimal")
        try:
            bytes.fromhex(self.key_hex)
        except ValueError as exc:
            raise ValueError("B2a acknowledgement key is not hexadecimal") from exc
        return self


class R2DB2AAckLedger(R2DContract):
    contract_version: Literal["r2d.b2a.broker-ack.v1"]
    phase: Literal["source_fixture", "dead_letter", "redrive_audit", "replacement"]
    topic: R2DB2ATopic
    producer_acks: Literal["all"]
    acknowledgements: tuple[R2DB2ABrokerAcknowledgement, ...]
    content_sha256: R2DSha256

    @field_validator("acknowledgements", mode="before")
    @classmethod
    def freeze_acknowledgements(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def ledger_is_complete_and_content_addressed(self) -> Self:
        identities = tuple((item.partition, item.offset) for item in self.acknowledgements)
        if not identities or identities != tuple(sorted(identities)):
            raise ValueError("B2a acknowledgements must be nonempty and canonically ordered")
        if len(identities) != len(set(identities)):
            raise ValueError("B2a acknowledgement identities must be unique")
        if any(item.topic != self.topic for item in self.acknowledgements):
            raise ValueError("B2a acknowledgement ledger mixes topics")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("B2a acknowledgement ledger content SHA-256 mismatch")
        return self


class R2DB2AArtifactRef(R2DContract):
    dataset: str
    relative_path: str
    content_sha256: R2DSha256
    schema_sha256: R2DSha256
    file_sha256: R2DSha256
    row_count: Literal[1]


class R2DB2AVerificationCheck(R2DContract):
    name: R2DB2ACheckName
    passed: Literal[True]
    evidence_sha256: R2DSha256


class R2DB2ARuntime(R2DContract):
    operating_system: str
    machine: str
    python_version: str
    github_runner_os_attested: bool


class R2DB2AVerificationReceipt(R2DContract):
    contract_version: Literal["r2d.b2a.verification-receipt.v1"]
    release: Literal["R2D-B2a"]
    status: Literal["PASS"]
    checks: tuple[R2DB2AVerificationCheck, ...]
    evidence_refs: tuple[R2DB2AArtifactRef, ...]
    config_sha256: R2DSha256
    code_sha256: R2DSha256
    inherited_b1_config_sha256: R2DSha256
    topology_sha256: R2DSha256
    service_image_sha256: R2DSha256
    service_image_file_sha256: R2DSha256
    source_ack_sha256: R2DSha256
    route_evidence_sha256: R2DSha256
    dead_letter_ack_sha256: R2DSha256
    dead_letter_round_trip_sha256: R2DSha256
    redrive_ack_sha256: R2DSha256
    redrive_evidence_sha256: R2DSha256
    replacement_ack_sha256: R2DSha256
    manual_commit_sha256: R2DSha256
    artifact_set_sha256: R2DSha256
    scope_sha256: R2DSha256
    source_record_count: Literal[2]
    forward_record_count: Literal[1]
    dead_letter_record_count: Literal[1]
    redrive_request_count: Literal[1]
    replacement_record_count: Literal[1]
    runtime: R2DB2ARuntime
    portable_b1_status: Literal["PASS"]
    service_dead_letter_topic_status: Literal["PASS"]
    persistent_dagster_status: Literal["NOT_RUN"]
    retained_service_restart_status: Literal["NOT_RUN"]
    operational_alert_delivery_status: Literal["NOT_RUN"]
    full_r2d_b_status: Literal["IN_PROGRESS"]
    full_g2_status: Literal["OPEN"]
    exactly_once_claim: Literal[False]
    content_sha256: R2DSha256

    @field_validator("checks", "evidence_refs", mode="before")
    @classmethod
    def freeze_sequences(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def receipt_is_honest_complete_and_bound(self) -> Self:
        if tuple(item.name for item in self.checks) != R2D_B2A_PASS_CHECK_NAMES:
            raise ValueError("B2a verification checks are incomplete, duplicated, or reordered")
        if tuple(item.dataset for item in self.evidence_refs) != R2D_B2A_EVIDENCE_DATASETS:
            raise ValueError("B2a evidence topology is incomplete, duplicated, or reordered")
        if self.evidence_refs[1].file_sha256 != self.service_image_file_sha256:
            raise ValueError("B2a receipt does not bind the service-image evidence bytes")
        expected: dict[R2DB2ACheckName, str] = {
            "inherited_b1_authority": self.inherited_b1_config_sha256,
            "exact_service_topic_topology": self.topology_sha256,
            "real_source_acks_all": self.source_ack_sha256,
            "observed_service_image": self.service_image_sha256,
            "live_poison_isolation": self.route_evidence_sha256,
            "byte_preserving_dlq_round_trip": self.dead_letter_round_trip_sha256,
            "append_only_operator_redrive": self.redrive_evidence_sha256,
            "replacement_returns_to_source": self.replacement_ack_sha256,
            "original_dlq_immutability": self.dead_letter_round_trip_sha256,
            "manual_commit_convergence": self.manual_commit_sha256,
            "immutable_artifact_set": self.artifact_set_sha256,
            "honest_scope": self.scope_sha256,
        }
        if any(item.evidence_sha256 != expected[item.name] for item in self.checks):
            raise ValueError("B2a verification check is detached from its evidence")
        artifact_rows = [item.model_dump(mode="json") for item in self.evidence_refs]
        if canonical_json_sha256(artifact_rows) != self.artifact_set_sha256:
            raise ValueError("B2a artifact-set SHA-256 mismatch")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("B2a receipt content SHA-256 mismatch")
        return self


R2D_B2A_CODE_FILES = (
    "configs/ingestion/r2d-b2a.yaml",
    "src/nexus/ingestion/r2d_b2_contracts.py",
    "src/nexus/ingestion/r2d_b2_service.py",
    "src/nexus/ingestion/r2d_b1.py",
    "src/nexus/ingestion/r2d_b1_contracts.py",
    "src/nexus/ingestion/r2d_capture.py",
    "src/nexus/ingestion/r2d_contracts.py",
    "src/nexus/r2d_b2a.py",
    "tests/test_r2d_b2_contracts.py",
    "src/nexus/cli.py",
    "infra/compose.yaml",
    ".github/workflows/ci.yml",
    "pyproject.toml",
    "uv.lock",
)


def load_r2d_b2a_config(path: Path = DEFAULT_R2D_B2A_CONFIG_PATH) -> R2DB2AConfig:
    config = R2DB2AConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
    inherited_path = PROJECT_ROOT / config.inherited_b1.config_path
    inherited = load_r2d_b1_config(inherited_path)
    if r2d_b1_config_sha256(inherited) != config.inherited_b1.config_sha256:
        raise ValueError("B2a inherited B1 config identity mismatch")
    return config


def r2d_b2a_config_sha256(config: R2DB2AConfig) -> str:
    return canonical_json_sha256(config.model_dump(mode="json"))


def r2d_b2a_code_sha256(project_root: Path = PROJECT_ROOT) -> str:
    root = project_root.resolve()
    identities = {path: r2d_lf_canonical_file_sha256(root / path) for path in R2D_B2A_CODE_FILES}
    return canonical_json_sha256(identities)


def load_r2d_b2a_service_image(path: Path) -> R2DB2AServiceImageEvidence:
    try:
        raw = path.read_bytes()
        parsed = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot load B2a service-image evidence: {path}") from exc
    if raw != canonical_json_bytes(parsed) + b"\n":
        raise ValueError("B2a service-image evidence must be canonical JSON")
    return R2DB2AServiceImageEvidence.model_validate(parsed)
