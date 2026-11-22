"""Strict contracts for the portable R2D-B1 schema and dead-letter gate."""

from __future__ import annotations

import hashlib
from datetime import UTC
from pathlib import Path
from typing import Annotated, Literal, Self
from uuid import UUID

import yaml
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    NonNegativeInt,
    StringConstraints,
    field_validator,
    model_validator,
)

from nexus.ingestion.r2d_contracts import (
    R2DHeader,
    load_r2d_config,
    r2d_config_sha256,
    r2d_lf_canonical_file_sha256,
)
from nexus.modeling.r3_artifacts import canonical_json_sha256

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_R2D_B1_CONFIG_PATH = PROJECT_ROOT / "configs" / "ingestion" / "r2d-b1.yaml"

R2DB1Sha256 = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
R2DB1Identifier = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/@-]*$", min_length=1, max_length=160),
]
R2DB1Reason = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
R2DB1SchemaReason = Literal[
    "compatible",
    "invalid_utf8",
    "invalid_json",
    "envelope_schema_invalid",
    "unsupported_contract_version",
    "unsupported_event_type",
    "unsupported_event_version",
    "payload_schema_invalid",
]

R2DB1CheckName = Literal[
    "inherited_contract_integrity",
    "conservative_schema_compatibility",
    "poison_isolation_matrix",
    "business_validation_boundary",
    "dead_letter_byte_preservation",
    "append_only_operator_redrive",
    "original_evidence_immutability",
    "tamper_rejection",
    "deterministic_two_replay",
    "immutable_artifact_set",
    "honest_scope",
]
R2D_B1_PASS_CHECK_NAMES: tuple[R2DB1CheckName, ...] = (
    "inherited_contract_integrity",
    "conservative_schema_compatibility",
    "poison_isolation_matrix",
    "business_validation_boundary",
    "dead_letter_byte_preservation",
    "append_only_operator_redrive",
    "original_evidence_immutability",
    "tamper_rejection",
    "deterministic_two_replay",
    "immutable_artifact_set",
    "honest_scope",
)


def _tuple_from_yaml(value: object) -> object:
    return tuple(value) if isinstance(value, list) else value


class R2DB1Contract(BaseModel):
    """Closed, immutable base for R2D-B1 configuration and evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)


class R2DB1DataContract(R2DB1Contract):
    """Exact R2D-A data contract inherited by the governance boundary."""

    r2d_a1_config_path: Literal["configs/ingestion/r2d-a1.yaml"]
    r2d_a1_config_sha256: Literal[
        "310f33d61df4188ba58644723b7021784c5b12dc76e6e00adf6c4457cad8fc2a"
    ]


class R2DB1SchemaPolicy(R2DB1Contract):
    """Supported identities and the deliberately conservative evolution algorithm."""

    event_type: Literal["inventory.adjusted"]
    supported_contract_versions: tuple[Literal["1.0"], ...]
    supported_event_versions: tuple[Literal[1], ...]
    envelope_schema_path: Literal["contracts/schemas/event-envelope.schema.json"]
    payload_schema_path: Literal["contracts/schemas/r2-inventory-adjustment-payload.schema.json"]
    schema_bundle_sha256: Literal[
        "661dc1829eff8f0245ca35c39c562222945b0c8d4f8b570f5d38154c90f6c14d"
    ]
    compatibility_algorithm: Literal["conservative_object_backward_v1"]
    incompatible_action: Literal["dead_letter"]
    business_validation_owner: Literal["R2A"]

    _freeze_versions = field_validator(
        "supported_contract_versions", "supported_event_versions", mode="before"
    )(_tuple_from_yaml)

    @model_validator(mode="after")
    def supported_identity_is_exact(self) -> Self:
        if self.supported_contract_versions != ("1.0",):
            raise ValueError("R2D-B1 must support exactly envelope contract 1.0")
        if self.supported_event_versions != (1,):
            raise ValueError("R2D-B1 must support exactly inventory event version 1")
        return self


class R2DB1DeadLetterPolicy(R2DB1Contract):
    """Fail-closed evidence and operator-redrive requirements."""

    evidence_dataset: Literal["r2d.b1.dead_letter"]
    redrive_dataset: Literal["r2d.b1.redrive"]
    preserve_original_bytes: Literal[True]
    redrive_mode: Literal["manual_append_only"]
    require_operator_identity: Literal[True]
    require_reason: Literal[True]
    auto_redrive_identity_conflicts: Literal[False]
    maximum_reason_characters: Literal[500]


class R2DB1ScopeContract(R2DB1Contract):
    """Claims deliberately excluded from the portable B1 checkpoint."""

    portable_contract_status: Literal["PASS"]
    service_dead_letter_topic_status: Literal["NOT_RUN"]
    persistent_dagster_status: Literal["NOT_RUN"]
    operational_alert_delivery_status: Literal["NOT_RUN"]
    full_g2_status: Literal["OPEN"]


class R2DB1Config(R2DB1Contract):
    """Complete frozen configuration for the first R2D-B slice."""

    release: Literal["R2D-B1"]
    contract_version: Literal["1.0.0"]
    data_contract: R2DB1DataContract
    schema_policy: R2DB1SchemaPolicy
    dead_letter_policy: R2DB1DeadLetterPolicy
    scope: R2DB1ScopeContract


class R2DB1CompatibilityAssessment(R2DB1Contract):
    """Deterministic result of the conservative object-schema comparison."""

    contract_version: Literal["r2d.b1.compatibility-assessment.v1"]
    algorithm: Literal["conservative_object_backward_v1"]
    baseline_schema_sha256: R2DB1Sha256
    candidate_schema_sha256: R2DB1Sha256
    compatible: bool
    violations: tuple[str, ...]
    content_sha256: R2DB1Sha256

    _freeze_violations = field_validator("violations", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def content_address_is_valid(self) -> Self:
        if self.violations != tuple(sorted(set(self.violations))):
            raise ValueError("compatibility violations must be sorted and unique")
        if self.compatible == bool(self.violations):
            raise ValueError("compatible must be true exactly when violations are empty")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("compatibility assessment content SHA-256 mismatch")
        return self


class R2DB1SchemaDecision(R2DB1Contract):
    """Schema-only disposition; semantic validity remains owned by R2A."""

    contract_version: Literal["r2d.b1.schema-decision.v1"]
    disposition: Literal["forward", "dead_letter"]
    reason: R2DB1SchemaReason
    raw_value_sha256: R2DB1Sha256
    schema_bundle_sha256: R2DB1Sha256
    envelope_contract_version: str | None
    event_type: str | None
    event_version: int | None
    event_id: UUID | None
    idempotency_key: str | None
    content_sha256: R2DB1Sha256

    @model_validator(mode="after")
    def decision_is_honest_and_bound(self) -> Self:
        if (self.disposition == "forward") != (self.reason == "compatible"):
            raise ValueError("only a compatible schema decision may be forwarded")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("schema decision content SHA-256 mismatch")
        return self


def r2d_b1_dead_letter_id(*, physical_fingerprint: str, decision_sha256: str) -> str:
    identity = canonical_json_sha256(
        {
            "contract": "r2d.b1.dead-letter-id.v1",
            "decision_sha256": decision_sha256,
            "physical_fingerprint": physical_fingerprint,
        }
    )
    return f"dlq_{identity}"


class R2DB1DeadLetterRecord(R2DB1Contract):
    """Byte-preserving evidence for one schema-poisoned admitted record."""

    contract_version: Literal["r2d.b1.dead-letter.v1"]
    dead_letter_id: Annotated[str, StringConstraints(pattern=r"^dlq_[a-f0-9]{64}$")]
    original_topic: str
    original_partition: NonNegativeInt
    original_offset: NonNegativeInt
    original_physical_fingerprint: R2DB1Sha256
    original_key_hex: str | None
    original_headers: tuple[R2DHeader, ...]
    original_raw_value_hex: str
    original_raw_value_sha256: R2DB1Sha256
    schema_decision: R2DB1SchemaDecision
    content_sha256: R2DB1Sha256

    _freeze_headers = field_validator("original_headers", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def original_bytes_and_identity_are_valid(self) -> Self:
        try:
            raw_value = bytes.fromhex(self.original_raw_value_hex)
            if self.original_key_hex is not None:
                bytes.fromhex(self.original_key_hex)
        except ValueError as exc:
            raise ValueError("dead-letter byte fields must be even hexadecimal") from exc
        if hashlib.sha256(raw_value).hexdigest() != self.original_raw_value_sha256:
            raise ValueError("dead-letter original raw-value SHA-256 mismatch")
        if self.schema_decision.disposition != "dead_letter":
            raise ValueError("dead-letter evidence requires a dead_letter schema decision")
        if self.schema_decision.raw_value_sha256 != self.original_raw_value_sha256:
            raise ValueError("dead-letter decision does not bind the original raw value")
        expected_id = r2d_b1_dead_letter_id(
            physical_fingerprint=self.original_physical_fingerprint,
            decision_sha256=self.schema_decision.content_sha256,
        )
        if self.dead_letter_id != expected_id:
            raise ValueError("dead-letter identity mismatch")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("dead-letter content SHA-256 mismatch")
        return self


def r2d_b1_redrive_id(payload: dict[str, object]) -> str:
    return f"rdv_{canonical_json_sha256(payload)}"


class R2DB1RedriveRequest(R2DB1Contract):
    """Append-only manual correction that never mutates original DLQ evidence."""

    contract_version: Literal["r2d.b1.redrive-request.v1"]
    redrive_id: Annotated[str, StringConstraints(pattern=r"^rdv_[a-f0-9]{64}$")]
    dead_letter_id: Annotated[str, StringConstraints(pattern=r"^dlq_[a-f0-9]{64}$")]
    dead_letter_content_sha256: R2DB1Sha256
    original_raw_value_sha256: R2DB1Sha256
    causation_reference: Annotated[str, StringConstraints(pattern=r"^dlq_[a-f0-9]{64}$")]
    operator_id: R2DB1Identifier
    reason: R2DB1Reason
    submitted_at: AwareDatetime
    replacement_raw_value_hex: str
    replacement_raw_value_sha256: R2DB1Sha256
    replacement_event_id: UUID
    replacement_idempotency_key: R2DB1Identifier
    replacement_schema_decision_sha256: R2DB1Sha256
    content_sha256: R2DB1Sha256

    _normalize_submitted_at = field_validator("submitted_at")(lambda value: value.astimezone(UTC))

    @model_validator(mode="after")
    def request_is_content_addressed(self) -> Self:
        try:
            replacement = bytes.fromhex(self.replacement_raw_value_hex)
        except ValueError as exc:
            raise ValueError("replacement raw value must be even hexadecimal") from exc
        if hashlib.sha256(replacement).hexdigest() != self.replacement_raw_value_sha256:
            raise ValueError("replacement raw-value SHA-256 mismatch")
        if self.causation_reference != self.dead_letter_id:
            raise ValueError("redrive causation must reference the immutable dead-letter ID")
        identity_payload = self.model_dump(mode="json", exclude={"redrive_id", "content_sha256"})
        if self.redrive_id != r2d_b1_redrive_id(identity_payload):
            raise ValueError("redrive identity mismatch")
        content_payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(content_payload) != self.content_sha256:
            raise ValueError("redrive content SHA-256 mismatch")
        return self


class R2DB1VerificationCheck(R2DB1Contract):
    """One exact ordered B1 gate assertion."""

    name: R2DB1CheckName
    passed: bool
    evidence_sha256: R2DB1Sha256
    detail: str


class R2DB1VerificationRuntime(R2DB1Contract):
    """Runtime attestation kept outside cross-platform semantic identity."""

    operating_system: str
    machine: str
    python_version: str
    github_runner_os_attested: bool


class R2DB1VerificationReceipt(R2DB1Contract):
    """Content-addressed receipt for the bounded portable B1 gate."""

    contract_version: Literal["r2d.b1.verification-receipt.v1"]
    release: Literal["R2D-B1"]
    status: Literal["PASS", "FAIL"]
    checks: tuple[R2DB1VerificationCheck, ...]
    config_sha256: R2DB1Sha256
    code_sha256: R2DB1Sha256
    contract_evidence_sha256: R2DB1Sha256
    compatibility_assessment_sha256: R2DB1Sha256
    classification_matrix_sha256: R2DB1Sha256
    dead_letter_set_sha256: R2DB1Sha256
    redrive_ledger_sha256: R2DB1Sha256
    tamper_matrix_sha256: R2DB1Sha256
    normalized_semantics_sha256: R2DB1Sha256
    artifact_set_sha256: R2DB1Sha256
    scope_sha256: R2DB1Sha256
    runtime: R2DB1VerificationRuntime
    portable_contract_status: Literal["PASS"]
    service_dead_letter_topic_status: Literal["NOT_RUN"]
    persistent_dagster_status: Literal["NOT_RUN"]
    operational_alert_delivery_status: Literal["NOT_RUN"]
    full_r2d_b_status: Literal["IN_PROGRESS"]
    full_g2_status: Literal["OPEN"]
    content_sha256: R2DB1Sha256

    _freeze_checks = field_validator("checks", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def receipt_is_honest_complete_and_bound(self) -> Self:
        if tuple(check.name for check in self.checks) != R2D_B1_PASS_CHECK_NAMES:
            raise ValueError("R2D-B1 verification checklist is incomplete or reordered")
        if len({check.name for check in self.checks}) != len(self.checks):
            raise ValueError("R2D-B1 verification checklist contains duplicates")
        if self.status == "PASS" and not all(check.passed for check in self.checks):
            raise ValueError("PASS requires every R2D-B1 verification check to pass")
        if self.status == "FAIL" and all(check.passed for check in self.checks):
            raise ValueError("FAIL requires at least one failed R2D-B1 verification check")
        expected_evidence: dict[R2DB1CheckName, str] = {
            "inherited_contract_integrity": self.contract_evidence_sha256,
            "conservative_schema_compatibility": self.compatibility_assessment_sha256,
            "poison_isolation_matrix": self.classification_matrix_sha256,
            "business_validation_boundary": self.classification_matrix_sha256,
            "dead_letter_byte_preservation": self.dead_letter_set_sha256,
            "append_only_operator_redrive": self.redrive_ledger_sha256,
            "original_evidence_immutability": self.dead_letter_set_sha256,
            "tamper_rejection": self.tamper_matrix_sha256,
            "deterministic_two_replay": self.normalized_semantics_sha256,
            "immutable_artifact_set": self.artifact_set_sha256,
            "honest_scope": self.scope_sha256,
        }
        for check in self.checks:
            if check.evidence_sha256 != expected_evidence[check.name]:
                raise ValueError(f"R2D-B1 check is detached from evidence: {check.name}")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("R2D-B1 receipt content SHA-256 mismatch")
        return self


def load_r2d_b1_config(path: Path = DEFAULT_R2D_B1_CONFIG_PATH) -> R2DB1Config:
    """Load B1 strictly and verify every inherited checked-in identity."""

    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError(f"cannot load R2D-B1 config: {path}") from exc
    config = R2DB1Config.model_validate(payload)
    data_config_path = PROJECT_ROOT / config.data_contract.r2d_a1_config_path
    if r2d_config_sha256(load_r2d_config(data_config_path)) != (
        config.data_contract.r2d_a1_config_sha256
    ):
        raise ValueError("R2D-B1 inherited R2D-A data config identity mismatch")
    schema_paths = (
        PROJECT_ROOT / config.schema_policy.envelope_schema_path,
        PROJECT_ROOT / config.schema_policy.payload_schema_path,
    )
    expected_hashes = (
        "69337c05164d83876f51efb6adda5c799adf19aa5a3a3f8a4a545188fd442380",
        "c0082351aadcd1359ad629e85fd121782d4a6a28b114404766d180b8b411cb11",
    )
    for schema_path, expected in zip(schema_paths, expected_hashes, strict=True):
        if r2d_lf_canonical_file_sha256(schema_path) != expected:
            raise ValueError(f"R2D-B1 checked-in schema identity mismatch: {schema_path.name}")
    return config


def r2d_b1_config_sha256(config: R2DB1Config) -> str:
    """Return the canonical semantic identity of the strict B1 configuration."""

    return canonical_json_sha256(config.model_dump(mode="json"))
