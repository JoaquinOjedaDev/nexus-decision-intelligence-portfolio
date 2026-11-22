"""Strict final R2D-A2 service-parity and recovery evidence contracts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import (
    NonNegativeInt,
    PositiveInt,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from nexus.ingestion.r2d_contracts import (
    R2DContract,
    R2DSha256,
    canonical_json_sha256,
    r2d_lf_canonical_file_sha256,
)

R2DA2CheckName = Literal[
    "verified_a2b1_crash_receipt",
    "resolved_service_images",
    "real_service_capture_complete",
    "postgres_source_reconstruction",
    "acknowledged_raw_conservation",
    "public_r2a_two_cutoffs",
    "direct_service_business_parity",
    "portable_reference_business_join",
    "route_provenance_is_distinct",
    "derived_state_recovery_identity",
    "bounded_resource_envelope",
]
R2D_A2_PASS_CHECK_NAMES: tuple[R2DA2CheckName, ...] = (
    "verified_a2b1_crash_receipt",
    "resolved_service_images",
    "real_service_capture_complete",
    "postgres_source_reconstruction",
    "acknowledged_raw_conservation",
    "public_r2a_two_cutoffs",
    "direct_service_business_parity",
    "portable_reference_business_join",
    "route_provenance_is_distinct",
    "derived_state_recovery_identity",
    "bounded_resource_envelope",
)

R2D_A2_CODE_FILES = (
    "src/nexus/r2d_a2b2.py",
    "src/nexus/cli.py",
    "src/nexus/r2.py",
    "src/nexus/r2d.py",
    "src/nexus/data/r2_models.py",
    "src/nexus/data/r2_pipeline.py",
    "src/nexus/ingestion/r2d_a2_contracts.py",
    "src/nexus/ingestion/r2d_a2b_contracts.py",
    "src/nexus/ingestion/r2d_capture.py",
    "src/nexus/ingestion/r2d_contracts.py",
    "src/nexus/ingestion/r2d_service.py",
    "src/nexus/ingestion/r2d_service_contracts.py",
    "src/nexus/simulation/supply_network.py",
    "src/nexus/storage/artifacts.py",
    "configs/ingestion/r2d-a1.yaml",
    "configs/ingestion/r2d-a2a.yaml",
    "contracts/schemas/event-envelope.schema.json",
    "contracts/schemas/r2-inventory-adjustment-payload.schema.json",
    "contracts/schemas/r2-source-manifest.schema.json",
    "infra/postgres/migrations/001_r2d_capture.sql",
    "infra/compose.yaml",
    "dbt/dbt_project.yml",
    "dbt/profiles.yml",
    ".github/workflows/ci.yml",
    "pyproject.toml",
    "uv.lock",
)

R2DA2ObservedImageId = Annotated[
    str,
    StringConstraints(pattern=r"^sha256:[a-f0-9]{64}$"),
]


class R2DA2ServiceImageIdentity(R2DContract):
    """One configured service reference paired with its observed immutable image ID."""

    service: Literal["postgres", "redpanda"]
    configured_reference: str
    observed_image_id: R2DA2ObservedImageId
    operating_system: Literal["linux"]
    architecture: Literal["amd64"]

    @model_validator(mode="after")
    def configured_reference_matches_service(self) -> Self:
        expected = {
            "postgres": "postgres:17.6-alpine",
            "redpanda": "docker.redpanda.com/redpandadata/redpanda:v26.1.9",
        }
        if self.configured_reference != expected[self.service]:
            raise ValueError(
                "A2 service image reference differs from the governed Compose topology"
            )
        return self


class R2DA2ServiceImageEvidence(R2DContract):
    """Canonical pre-verification identities of the two Linux service images."""

    contract_version: Literal["r2d.a2.service-images.v1"]
    images: tuple[R2DA2ServiceImageIdentity, ...]
    content_sha256: R2DSha256

    @field_validator("images", mode="before")
    @classmethod
    def images_are_immutable(cls, value: object) -> tuple[object, ...] | object:
        return tuple(value) if isinstance(value, list) else value

    def content_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    @model_validator(mode="after")
    def evidence_is_exact_and_content_addressed(self) -> Self:
        if tuple(image.service for image in self.images) != ("postgres", "redpanda"):
            raise ValueError(
                "A2 service image evidence must bind postgres then redpanda exactly once"
            )
        if self.images[0].observed_image_id == self.images[1].observed_image_id:
            raise ValueError("A2 services must resolve to distinct observed image IDs")
        if self.content_sha256 != canonical_json_sha256(self.content_payload()):
            raise ValueError("A2 service image evidence content SHA-256 mismatch")
        return self

    def observed_image_id(self, service: Literal["postgres", "redpanda"]) -> str:
        """Return the observed immutable image ID for one exact governed service."""

        return next(image.observed_image_id for image in self.images if image.service == service)


def load_r2d_a2_service_image_evidence(path: Path) -> R2DA2ServiceImageEvidence:
    """Read only strict canonical JSON produced from the running Linux containers."""

    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"cannot read required A2 service image evidence: {path}") from exc
    try:
        evidence = R2DA2ServiceImageEvidence.model_validate_json(raw)
    except ValidationError as exc:
        raise ValueError("A2 service image evidence is invalid or tampered") from exc
    canonical = (
        json.dumps(
            evidence.model_dump(mode="json"),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )
    if raw != canonical:
        raise ValueError("A2 service image evidence is not canonical JSON")
    return evidence


class R2DA2ServiceCheckpoint(R2DContract):
    """Durable PostgreSQL barrier used to reconstruct the service source package."""

    contract_version: Literal["r2d.a2.service-checkpoint.v1"]
    topic: Literal["inventory.adjusted.v1"]
    starts: tuple[NonNegativeInt, ...]
    ends: tuple[NonNegativeInt, ...]
    next_offsets: tuple[NonNegativeInt, ...]
    record_count: PositiveInt
    data_config_sha256: R2DSha256
    service_config_sha256: R2DSha256
    service_image_evidence_sha256: R2DSha256
    migration_sha256: R2DSha256
    broker_ack_sha256: R2DSha256
    transcript_sha256: R2DSha256
    lineage_sha256: R2DSha256
    raw_value_multiset_sha256: R2DSha256
    content_sha256: R2DSha256

    @field_validator("starts", "ends", "next_offsets", mode="before")
    @classmethod
    def vectors_are_immutable(cls, value: object) -> tuple[object, ...] | object:
        return tuple(value) if isinstance(value, list) else value

    def content_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    @model_validator(mode="after")
    def checkpoint_is_complete_and_content_addressed(self) -> Self:
        if any(len(vector) != 2 for vector in (self.starts, self.ends, self.next_offsets)):
            raise ValueError("A2 checkpoint must bind exactly two partitions")
        if self.starts != (0, 0):
            raise ValueError("A2 parity capture must start from fresh-empty service state")
        if self.next_offsets != self.ends:
            raise ValueError("A2 checkpoint must reach every frozen end offset")
        if self.record_count > 64:
            raise ValueError("A2 checkpoint exceeds the bounded record limit")
        if self.content_sha256 != canonical_json_sha256(self.content_payload()):
            raise ValueError("A2 checkpoint content SHA-256 mismatch")
        return self


class R2DA2ResourceEvidence(R2DContract):
    """Bounded local process/artifact envelope; Compose teardown is a CI postcondition."""

    contract_version: Literal["r2d.a2.resource-evidence.v1"]
    wall_time_seconds: float
    wall_time_limit_seconds: Literal[600]
    pre_receipt_artifact_bytes: NonNegativeInt
    artifact_limit_bytes: Literal[1073741824]
    passed: Literal[True]
    content_sha256: R2DSha256

    def content_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    @model_validator(mode="after")
    def resource_evidence_is_bounded_and_content_addressed(self) -> Self:
        if self.wall_time_seconds < 0 or self.wall_time_seconds > self.wall_time_limit_seconds:
            raise ValueError("A2 wall time exceeds the bounded resource contract")
        if self.pre_receipt_artifact_bytes > self.artifact_limit_bytes:
            raise ValueError("A2 artifact bytes exceed the bounded resource contract")
        if self.content_sha256 != canonical_json_sha256(self.content_payload()):
            raise ValueError("A2 resource evidence content SHA-256 mismatch")
        return self


class R2DA2VerificationCheck(R2DContract):
    """One exact final A2 PASS check."""

    name: R2DA2CheckName
    passed: Literal[True]
    evidence_sha256: R2DSha256


class R2DA2VerificationReceipt(R2DContract):
    """Final bounded A2 receipt; G2 and exactly-once remain explicit non-claims."""

    contract_version: Literal["r2d.a2.verification-receipt.v2"]
    release: Literal["R2D-A2"]
    status: Literal["PASS"]
    checks: tuple[R2DA2VerificationCheck, ...]
    topic: Literal["inventory.adjusted.v1"]
    partition_count: Literal[2]
    record_count: PositiveInt
    planning_cutoffs: tuple[str, str]
    data_config_sha256: R2DSha256
    service_config_sha256: R2DSha256
    service_image_evidence_content_sha256: R2DSha256
    service_image_evidence_file_sha256: R2DSha256
    postgres_observed_image_id: R2DA2ObservedImageId
    redpanda_observed_image_id: R2DA2ObservedImageId
    migration_sha256: R2DSha256
    code_sha256: R2DSha256
    a2b1_receipt_content_sha256: R2DSha256
    a2b1_receipt_file_sha256: R2DSha256
    broker_ack_sha256: R2DSha256
    transcript_sha256: R2DSha256
    lineage_sha256: R2DSha256
    service_checkpoint_sha256: R2DSha256
    raw_value_multiset_sha256: R2DSha256
    reconstructed_inventory_sha256: R2DSha256
    reconstructed_source_manifest_sha256: R2DSha256
    reconstructed_source_manifest_file_sha256: R2DSha256
    source_reconstruction_sha256: R2DSha256
    r2a_semantics_sha256: R2DSha256
    normalized_business_sha256: R2DSha256
    direct_provenance_sha256: R2DSha256
    service_provenance_sha256: R2DSha256
    recovery_provenance_sha256: R2DSha256
    provenance_distinction_sha256: R2DSha256
    service_recovery_sha256: R2DSha256
    resource_evidence_sha256: R2DSha256
    artifact_set_sha256: R2DSha256
    service_foundation_status: Literal["PASS"]
    crash_recovery_status: Literal["PASS"]
    downstream_parity_status: Literal["PASS"]
    r2d_a2_status: Literal["PASS"]
    full_r2d_a_status: Literal["PASS"]
    full_g2_status: Literal["OPEN"]
    operational_recovery_claim: Literal[True]
    exactly_once_claim: Literal[False]
    content_sha256: R2DSha256

    @field_validator("checks", "planning_cutoffs", mode="before")
    @classmethod
    def sequences_are_immutable(cls, value: object) -> tuple[object, ...] | object:
        return tuple(value) if isinstance(value, list) else value

    def content_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    @model_validator(mode="after")
    def receipt_is_complete_honest_and_content_addressed(self) -> Self:
        if tuple(check.name for check in self.checks) != R2D_A2_PASS_CHECK_NAMES:
            raise ValueError("A2 receipt checks are incomplete, duplicated, or reordered")
        if self.record_count > 64:
            raise ValueError("A2 receipt record count exceeds the bounded contract")
        if len(set(self.planning_cutoffs)) != 2 or tuple(sorted(self.planning_cutoffs)) != (
            self.planning_cutoffs
        ):
            raise ValueError("A2 receipt must bind two unique ordered planning cutoffs")
        if self.direct_provenance_sha256 == self.service_provenance_sha256:
            raise ValueError("A2 direct and service route provenance must remain distinct")
        if self.postgres_observed_image_id == self.redpanda_observed_image_id:
            raise ValueError("A2 receipt service image identities must remain distinct")
        expected_evidence = {
            "verified_a2b1_crash_receipt": self.a2b1_receipt_content_sha256,
            "resolved_service_images": self.service_image_evidence_content_sha256,
            "real_service_capture_complete": self.service_checkpoint_sha256,
            "postgres_source_reconstruction": self.source_reconstruction_sha256,
            "acknowledged_raw_conservation": self.raw_value_multiset_sha256,
            "public_r2a_two_cutoffs": self.r2a_semantics_sha256,
            "direct_service_business_parity": self.normalized_business_sha256,
            "portable_reference_business_join": self.normalized_business_sha256,
            "route_provenance_is_distinct": self.provenance_distinction_sha256,
            "derived_state_recovery_identity": self.service_recovery_sha256,
            "bounded_resource_envelope": self.resource_evidence_sha256,
        }
        if any(check.evidence_sha256 != expected_evidence[check.name] for check in self.checks):
            raise ValueError("A2 receipt checklist evidence does not bind its top-level identities")
        if self.content_sha256 != canonical_json_sha256(self.content_payload()):
            raise ValueError("A2 receipt content SHA-256 mismatch")
        return self


def r2d_a2_code_sha256(project_root: Path) -> str:
    """Bind every checked-in file that can change the final A2 claim."""

    root = project_root.resolve()
    files = [root / item for item in R2D_A2_CODE_FILES]
    files.extend(path for path in root.glob("dbt/models/r2/**/*") if path.is_file())
    files.extend(path for path in root.glob("dbt/tests/r2_*.sql") if path.is_file())
    files.extend(path for path in root.glob("dbt/tests/generic/r2_*.sql") if path.is_file())
    identities = {
        path.relative_to(root).as_posix(): r2d_lf_canonical_file_sha256(path)
        for path in sorted(set(files))
    }
    return canonical_json_sha256(identities)
