"""Strict portable contracts for the bounded R2D-A1 capture gate."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Annotated, Any, Literal, Self, cast

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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_R2D_CONFIG_PATH = PROJECT_ROOT / "configs" / "ingestion" / "r2d-a1.yaml"

R2DSha256 = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
R2DHex = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]*$")]
R2DIdentifier = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=192,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    ),
]
R2DHeaderName = Annotated[
    str,
    StringConstraints(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9-]*$"),
]
R2DAdmissionReason = Literal[
    "admitted",
    "tombstone",
    "empty",
    "oversized",
    "non_utf8",
    "whitespace_only",
    "newline_unsafe",
    "key_mismatch",
    "header_mismatch",
]
R2DVerificationDetail = Annotated[str, StringConstraints(min_length=1, max_length=500)]
R2DPythonMinor = Annotated[str, StringConstraints(pattern=r"^3\.(?:12|13)$")]

R2D_A1_PASS_CHECK_NAMES: tuple[str, ...] = (
    "two_fresh_replay",
    "config_and_schema_identity",
    "deterministic_two_partition_transcript",
    "transport_admission_and_physical_identity",
    "complete_half_open_checkpoint",
    "raw_value_conservation",
    "manifest_bound_source_reconstruction",
    "public_r2a_semantic_authority",
    "direct_captured_business_parity",
    "point_in_time_correction_semantics",
    "route_provenance_integrity",
    "bounded_portable_resource_envelope",
    "immutable_artifacts",
)

R2D_SOURCE_MANIFEST_SHA256 = "03e32a3f197839134e44b8f6d384787a99ec22e2c088ee4edea8d8322814346b"
R2D_SOURCE_MANIFEST_FILE_SHA256 = "aa25517d49cb6c36b76e9b6a218a83e7419359f5a7c40626412797ca13f8d768"
R2D_INVENTORY_EVENTS_FILE_SHA256 = (
    "0418e3bf2d1212c766328c94d3ed46342c731f1cdbde954acc2024aea5bdb39c"
)
R2D_RAW_VALUE_MULTISET_SHA256 = "d2b8ca4dc666eec083c4b3d795c102ba2d10255cfaa68aaf32a018952b4b7242"
R2D_SCHEMA_BUNDLE_SHA256 = "661dc1829eff8f0245ca35c39c562222945b0c8d4f8b570f5d38154c90f6c14d"
R2D_NORMALIZED_BUSINESS_SHA256 = "b108686db885928d368b193c693b870e3a5cd187ecc18806062991ca286c8962"
R2D_CAPTURE_TRANSCRIPT_SHA256 = "2aa0422110a2afed32b932a8d93e48380a43616474d1db48484947c6e0f72716"
R2D_DIRECT_PROVENANCE_PROJECTION_SHA256 = (
    "b12380c6f2b9e6e1d7e015833bdaf4b9a667bfca47619e962bbe13b73b69d183"
)
R2D_CORRECTION_SEMANTICS_PROJECTION_SHA256 = (
    "9f93334dd33e45858c3867a2a2b36369ccd7469c6cb1956f890a583dba63ce2c"
)
R2D_PORTABLE_RECOVERY_PROJECTION_SHA256 = (
    "3640842ffb325845f3d981dd7f9c65f094216b9da1605b0a8118ceeba70bde09"
)

_EXPECTED_ALLOWED_HEADERS = (
    "nexus-contract",
    "nexus-routing-key",
    "nexus-schema-sha256",
    "traceparent",
)
_EXPECTED_EXCLUSION_PRECEDENCE = (
    "tombstone",
    "empty",
    "oversized",
    "non_utf8",
    "whitespace_only",
    "newline_unsafe",
    "key_mismatch",
    "header_mismatch",
)
_EXPECTED_SCHEMA_BUNDLE_SHA256 = R2D_SCHEMA_BUNDLE_SHA256


def _tuple_from_yaml(value: object) -> object:
    return tuple(value) if isinstance(value, list) else value


def _parse_datetime(value: object) -> object:
    if not isinstance(value, str):
        return value
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("planning_cutoff must be a valid ISO-8601 timestamp") from error


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("planning_cutoff must include an explicit timezone offset")
    return value.astimezone(UTC)


def _require_even_hex(value: str | None) -> str | None:
    if value is not None and len(value) % 2:
        raise ValueError("hex byte strings must contain an even number of characters")
    return value


class R2DContract(BaseModel):
    """Closed and immutable base for every R2D-A1 evidence contract."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)


class R2DSourceContract(R2DContract):
    """Exact frozen R2A fixture identities authorized for the portable gate."""

    generator: Literal["nexus-r2-supply-network"]
    source_manifest_sha256: Literal[
        "03e32a3f197839134e44b8f6d384787a99ec22e2c088ee4edea8d8322814346b"
    ]
    source_manifest_file_sha256: Literal[
        "aa25517d49cb6c36b76e9b6a218a83e7419359f5a7c40626412797ca13f8d768"
    ]
    inventory_events_file_sha256: Literal[
        "0418e3bf2d1212c766328c94d3ed46342c731f1cdbde954acc2024aea5bdb39c"
    ]
    inventory_event_count: Literal[9]
    raw_value_multiset_sha256: Literal[
        "d2b8ca4dc666eec083c4b3d795c102ba2d10255cfaa68aaf32a018952b4b7242"
    ]


class R2DRoutingKeyOverride(R2DContract):
    """Manifest-authorized key for one value whose routing identity cannot be parsed."""

    raw_value_sha256: Literal["c3b9d488773f31ee841b256a2da6f6a6b1d9224a06268abe1d33000814eb38ec"]
    routing_key: Literal["synthetic.wms:ADJ-MALFORMED-001"]


class R2DTopicContract(R2DContract):
    """Frozen laptop-safe topic behavior; no broker runtime is implied."""

    topic: Literal["inventory.adjusted.v1"]
    partition_count: Literal[2]
    replication_factor: Literal[1]
    key_format: Literal["source:logical_event_id"]
    partitioner: Literal["sha256_modulo"]
    reconstruction_order: Literal["partition_then_offset"]
    future_service_producer_acks: Literal["all"]
    portable_acknowledgement_kind: Literal["simulated_portable"]
    real_broker_ack_claim: Literal[False]
    consumer_auto_offset_commit: Literal[False]
    routing_key_overrides: tuple[R2DRoutingKeyOverride, ...]

    _freeze_overrides = field_validator("routing_key_overrides", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def malformed_fixture_override_is_exact(self) -> Self:
        if len(self.routing_key_overrides) != 1:
            raise ValueError("R2D-A1 requires exactly one malformed-fixture routing override")
        return self


class R2DSchemaContract(R2DContract):
    """Exact checked-in envelope and payload schema identities."""

    envelope_contract_version: Literal["1.0"]
    inventory_event_version: Literal[1]
    envelope_schema_id: Literal["https://nexus.local/contracts/event-envelope/1.0"]
    payload_schema_id: Literal[
        "https://nexus.local/contracts/r2-inventory-adjustment-payload.schema.json"
    ]
    envelope_schema_sha256: Literal[
        "69337c05164d83876f51efb6adda5c799adf19aa5a3a3f8a4a545188fd442380"
    ]
    payload_schema_sha256: Literal[
        "c0082351aadcd1359ad629e85fd121782d4a6a28b114404766d180b8b411cb11"
    ]
    schema_bundle_sha256: Literal[
        "661dc1829eff8f0245ca35c39c562222945b0c8d4f8b570f5d38154c90f6c14d"
    ]

    @model_validator(mode="after")
    def bundle_hash_is_canonically_derived(self) -> Self:
        if r2d_schema_bundle_sha256(self) != self.schema_bundle_sha256:
            raise ValueError("schema_bundle_sha256 must match the canonical schema bundle payload")
        return self


class R2DHeaderContract(R2DContract):
    """Allowed transport headers and exact required static identities."""

    allowed_names: tuple[R2DHeaderName, ...]
    routing_header_name: Literal["nexus-routing-key"]
    contract_header_name: Literal["nexus-contract"]
    contract_header_value: Literal["inventory.adjusted.v1"]
    schema_header_name: Literal["nexus-schema-sha256"]
    schema_header_value: Literal["661dc1829eff8f0245ca35c39c562222945b0c8d4f8b570f5d38154c90f6c14d"]
    optional_header_names: tuple[Literal["traceparent"], ...]

    _freeze_sequences = field_validator("allowed_names", "optional_header_names", mode="before")(
        _tuple_from_yaml
    )

    @model_validator(mode="after")
    def header_vocabulary_is_exact(self) -> Self:
        if self.allowed_names != _EXPECTED_ALLOWED_HEADERS:
            raise ValueError("allowed_names must match the exact ordered R2D-A1 header set")
        if self.optional_header_names != ("traceparent",):
            raise ValueError("traceparent must be the only optional R2D-A1 header")
        if self.schema_header_value != _EXPECTED_SCHEMA_BUNDLE_SHA256:
            raise ValueError("schema header must bind the exact R2D-A1 schema bundle")
        return self


class R2DAdmissionContract(R2DContract):
    """Exact bounded fixture limits and narrow transport-only rejection order."""

    maximum_unique_records: Literal[64]
    nominal_value_bytes: Literal[1024]
    maximum_value_bytes: Literal[4096]
    maximum_key_bytes: Literal[192]
    maximum_headers: Literal[8]
    maximum_header_name_bytes: Literal[64]
    maximum_header_value_bytes: Literal[256]
    exclusion_precedence: tuple[
        Literal[
            "tombstone",
            "empty",
            "oversized",
            "non_utf8",
            "whitespace_only",
            "newline_unsafe",
            "key_mismatch",
            "header_mismatch",
        ],
        ...,
    ]

    _freeze_precedence = field_validator("exclusion_precedence", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def precedence_is_exact(self) -> Self:
        if self.exclusion_precedence != _EXPECTED_EXCLUSION_PRECEDENCE:
            raise ValueError("exclusion_precedence must match the exact R2D-A1 order")
        return self


class R2DPlanningCutoff(R2DContract):
    """One trusted evaluation cutoff, normalized to UTC."""

    name: Literal["early", "final"]
    planning_cutoff: AwareDatetime

    _parse_cutoff = field_validator("planning_cutoff", mode="before")(_parse_datetime)
    _normalize_cutoff = field_validator("planning_cutoff")(_normalize_utc)


class R2DSnapshotContract(R2DContract):
    """Half-open barrier and exact two-cutoff semantics."""

    offset_interval: Literal["half_open"]
    completion_rule: Literal["contiguous_next_offset_equals_end_offset_exclusive"]
    planning_cutoffs: tuple[R2DPlanningCutoff, ...]

    _freeze_cutoffs = field_validator("planning_cutoffs", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def cutoffs_are_exact_and_ordered(self) -> Self:
        if tuple(cutoff.name for cutoff in self.planning_cutoffs) != ("early", "final"):
            raise ValueError("planning_cutoffs must be ordered exactly early, final")
        expected = (
            datetime(2026, 6, 20, tzinfo=UTC),
            datetime(2026, 6, 30, tzinfo=UTC),
        )
        actual = tuple(cutoff.planning_cutoff for cutoff in self.planning_cutoffs)
        if actual != expected or any(left >= right for left, right in pairwise(actual)):
            raise ValueError("R2D-A1 planning cutoffs must match the exact frozen UTC pair")
        return self


class R2DScopeContract(R2DContract):
    """Prevents the portable core from being mislabeled as operational recovery."""

    implementation: Literal["portable_in_memory_capture"]
    broker_dependency: Literal["none"]
    database_dependency: Literal["none"]
    operational_recovery_claim: Literal[False]
    exactly_once_claim: Literal[False]


class R2DResourceContract(R2DContract):
    """Exact portable CI budget; measurements belong in verification evidence."""

    wall_time_seconds: Literal[300]
    peak_process_tree_rss_mib: Literal[4096]
    maximum_process_tree_threads: Literal[256]
    artifact_mib: Literal[1024]
    shutdown_seconds: Literal[10]
    maximum_surviving_descendants: Literal[0]


class R2DExpectedEvidenceContract(R2DContract):
    """Frozen bounded semantic anchors independently authorized for R2D-A1."""

    contract_version: Literal["r2d.a1.expected-evidence.v1"]
    correction_projection: Literal["without_file_evidence"]
    portable_recovery_projection: Literal["without_checkpoint_evidence"]
    direct_provenance_projection: Literal["without_file_evidence"]
    capture_transcript_sha256: Literal[
        "2aa0422110a2afed32b932a8d93e48380a43616474d1db48484947c6e0f72716"
    ]
    normalized_business_sha256: Literal[
        "b108686db885928d368b193c693b870e3a5cd187ecc18806062991ca286c8962"
    ]
    correction_semantics_projection_sha256: Literal[
        "9f93334dd33e45858c3867a2a2b36369ccd7469c6cb1956f890a583dba63ce2c"
    ]
    portable_recovery_projection_sha256: Literal[
        "3640842ffb325845f3d981dd7f9c65f094216b9da1605b0a8118ceeba70bde09"
    ]
    direct_provenance_projection_sha256: Literal[
        "b12380c6f2b9e6e1d7e015833bdaf4b9a667bfca47619e962bbe13b73b69d183"
    ]


class R2DConfig(R2DContract):
    """Complete, strict, content-addressable R2D-A1 configuration."""

    release: Literal["R2D-A1"]
    contract_version: Literal["1.0.0"]
    profile: Literal["r2a-synthetic-contract-smoke"]
    seed: Literal[20260718]
    source_contract: R2DSourceContract
    topic_contract: R2DTopicContract
    schema_contract: R2DSchemaContract
    header_contract: R2DHeaderContract
    admission_contract: R2DAdmissionContract
    snapshot_contract: R2DSnapshotContract
    expected_evidence_contract: R2DExpectedEvidenceContract
    resource_contract: R2DResourceContract
    scope_contract: R2DScopeContract

    @model_validator(mode="after")
    def schema_header_binds_the_derived_bundle(self) -> Self:
        if self.header_contract.schema_header_value != self.schema_contract.schema_bundle_sha256:
            raise ValueError("schema header must equal the canonically derived schema bundle hash")
        return self


class R2DVerificationRuntime(R2DContract):
    """Portable runtime facts bound without claiming a service-backed execution."""

    os_family: Literal["linux", "windows", "macos", "other"]
    architecture: R2DIdentifier
    python_minor: R2DPythonMinor
    github_runner_os_attested: bool


class R2DVerificationCheck(R2DContract):
    """One named, falsifiable verification result and optional evidence identity."""

    name: R2DIdentifier
    passed: bool
    detail: R2DVerificationDetail
    evidence_sha256: R2DSha256 | None = None


class R2DVerificationReceipt(R2DContract):
    """Fail-closed portable R2D-A1 verification receipt."""

    contract_version: Literal["r2d.a1.verification.v1"]
    release: Literal["R2D-A1"]
    status: Literal["PASS", "FAIL"]
    full_r2d_a_status: Literal["OPEN"]
    full_g2_status: Literal["OPEN"]
    service_backed_status: Literal["NOT_RUN"]
    operational_recovery_claim: Literal[False]
    runtime: R2DVerificationRuntime
    checks: tuple[R2DVerificationCheck, ...]
    source_manifest_sha256: Literal[
        "03e32a3f197839134e44b8f6d384787a99ec22e2c088ee4edea8d8322814346b"
    ]
    config_sha256: R2DSha256
    normalized_business_sha256: R2DSha256
    transcript_sha256: R2DSha256
    checkpoint_sha256: R2DSha256
    producer_ack_sha256: R2DSha256
    lineage_sha256: R2DSha256
    evidence_checkpoint_sha256: R2DSha256
    code_sha256: R2DSha256
    artifact_set_sha256: R2DSha256
    direct_provenance_sha256: R2DSha256
    captured_provenance_sha256: R2DSha256
    resource_evidence_sha256: R2DSha256
    evidence_bindings_sha256: R2DSha256

    _freeze_checks = field_validator("checks", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def checklist_is_honest_and_complete(self) -> Self:
        names = tuple(check.name for check in self.checks)
        if len(names) != len(set(names)):
            raise ValueError("verification check names must be unique")
        if names != R2D_A1_PASS_CHECK_NAMES:
            raise ValueError("verification receipt requires the exact ordered R2D-A1 checklist")
        if self.status == "PASS":
            if self.source_manifest_sha256 != R2D_SOURCE_MANIFEST_SHA256:
                raise ValueError("PASS requires the frozen R2D-A1 source manifest")
            if not all(check.passed for check in self.checks):
                raise ValueError("PASS requires every R2D-A1 verification check to pass")
            first = self.checks[0]
            if first.detail != "two fresh replay semantic hashes match":
                raise ValueError("PASS requires the canonical two-fresh-replay detail")
            expected_evidence = {
                "two_fresh_replay": self.normalized_business_sha256,
                "config_and_schema_identity": self.config_sha256,
                "deterministic_two_partition_transcript": self.transcript_sha256,
                "transport_admission_and_physical_identity": self.transcript_sha256,
                "complete_half_open_checkpoint": self.checkpoint_sha256,
                "raw_value_conservation": self.producer_ack_sha256,
                "manifest_bound_source_reconstruction": self.evidence_checkpoint_sha256,
                "public_r2a_semantic_authority": self.evidence_checkpoint_sha256,
                "direct_captured_business_parity": self.normalized_business_sha256,
                "point_in_time_correction_semantics": self.evidence_checkpoint_sha256,
                "route_provenance_integrity": self.evidence_checkpoint_sha256,
                "bounded_portable_resource_envelope": self.resource_evidence_sha256,
                "immutable_artifacts": self.artifact_set_sha256,
            }
            for check in self.checks:
                if check.evidence_sha256 is None:
                    raise ValueError("every PASS check requires a non-null evidence SHA-256")
                if check.evidence_sha256 != expected_evidence[check.name]:
                    raise ValueError(
                        f"PASS check {check.name} does not bind its required top-level evidence"
                    )
        elif all(check.passed for check in self.checks):
            raise ValueError("FAIL requires at least one failed verification check")
        expected_bindings_sha256 = r2d_evidence_bindings_sha256(
            source_manifest_sha256=self.source_manifest_sha256,
            config_sha256=self.config_sha256,
            normalized_business_sha256=self.normalized_business_sha256,
            transcript_sha256=self.transcript_sha256,
            checkpoint_sha256=self.checkpoint_sha256,
            producer_ack_sha256=self.producer_ack_sha256,
            lineage_sha256=self.lineage_sha256,
            evidence_checkpoint_sha256=self.evidence_checkpoint_sha256,
            code_sha256=self.code_sha256,
            artifact_set_sha256=self.artifact_set_sha256,
            direct_provenance_sha256=self.direct_provenance_sha256,
            captured_provenance_sha256=self.captured_provenance_sha256,
            resource_evidence_sha256=self.resource_evidence_sha256,
        )
        if self.evidence_bindings_sha256 != expected_bindings_sha256:
            raise ValueError("receipt evidence bindings SHA-256 mismatch")
        return self


class R2DHeader(R2DContract):
    """One Kafka-compatible header with exact nullable bytes encoded as hex."""

    name: R2DHeaderName
    value_hex: R2DHex | None

    _even_hex = field_validator("value_hex")(_require_even_hex)

    @classmethod
    def from_bytes(cls, name: str, value: bytes | None) -> R2DHeader:
        """Construct a header without an ambiguous implicit text encoding."""

        return cls(name=name, value_hex=None if value is None else value.hex())

    @property
    def value_bytes(self) -> bytes | None:
        """Return the exact nullable header bytes."""

        return None if self.value_hex is None else bytes.fromhex(self.value_hex)


class R2DParsedLineage(R2DContract):
    """Best-effort parsed lineage; never part of physical transport identity."""

    event_id: str | None = None
    idempotency_key: str | None = None
    source: str | None = None
    logical_event_id: str | None = None
    revision: int | None = None
    correlation_id: str | None = None
    trace_id: str | None = None


class R2DPhysicalRecord(R2DContract):
    """Portable representation of one polled physical broker record."""

    topic: R2DIdentifier
    partition: NonNegativeInt
    offset: NonNegativeInt
    key_hex: R2DHex | None
    headers: tuple[R2DHeader, ...] = ()
    broker_timestamp_ms: NonNegativeInt | None
    broker_timestamp_type: Literal["create_time", "log_append_time", "not_available"]
    value_hex: R2DHex | None
    lineage: R2DParsedLineage | None = None

    _even_key_hex = field_validator("key_hex")(_require_even_hex)
    _even_value_hex = field_validator("value_hex")(_require_even_hex)

    @model_validator(mode="after")
    def timestamp_fields_agree(self) -> Self:
        unavailable = self.broker_timestamp_type == "not_available"
        if unavailable != (self.broker_timestamp_ms is None):
            raise ValueError("broker timestamp value and type must agree")
        return self

    @classmethod
    def from_bytes(
        cls,
        *,
        topic: str,
        partition: int,
        offset: int,
        key: bytes | None,
        headers: tuple[tuple[str, bytes | None], ...],
        broker_timestamp_ms: int | None,
        broker_timestamp_type: Literal["create_time", "log_append_time", "not_available"],
        value: bytes | None,
        lineage: R2DParsedLineage | None = None,
    ) -> R2DPhysicalRecord:
        """Construct a record while making every bytes-to-hex conversion explicit."""

        return cls(
            topic=topic,
            partition=partition,
            offset=offset,
            key_hex=None if key is None else key.hex(),
            headers=tuple(R2DHeader.from_bytes(name, item) for name, item in headers),
            broker_timestamp_ms=broker_timestamp_ms,
            broker_timestamp_type=broker_timestamp_type,
            value_hex=None if value is None else value.hex(),
            lineage=lineage,
        )

    @property
    def key_bytes(self) -> bytes | None:
        """Return the exact nullable key bytes."""

        return None if self.key_hex is None else bytes.fromhex(self.key_hex)

    @property
    def value_bytes(self) -> bytes | None:
        """Return the exact nullable value bytes."""

        return None if self.value_hex is None else bytes.fromhex(self.value_hex)

    @property
    def transport_identity(self) -> tuple[str, int, int]:
        """Return the only identity eligible for transport deduplication."""

        return self.topic, self.partition, self.offset


class R2DTransportDecision(R2DContract):
    """One narrow transport outcome, separate from R2A business validation."""

    disposition: Literal["admitted", "excluded"]
    reason: R2DAdmissionReason

    @model_validator(mode="after")
    def disposition_matches_reason(self) -> Self:
        if (self.reason == "admitted") != (self.disposition == "admitted"):
            raise ValueError("only reason admitted may use the admitted disposition")
        return self


class R2DProducerAcknowledgement(R2DContract):
    """One simulated portable acknowledgement; it is explicitly not a broker receipt."""

    acknowledgement_kind: Literal["simulated_portable"]
    real_broker_ack_claim: Literal[False]
    topic: Literal["inventory.adjusted.v1"]
    partition: NonNegativeInt
    offset: NonNegativeInt
    key_hex: R2DHex
    raw_value_sha256: R2DSha256

    _even_key_hex = field_validator("key_hex")(_require_even_hex)

    @property
    def transport_identity(self) -> tuple[str, int, int]:
        """Return the exact physical identity acknowledged by the simulator."""

        return self.topic, self.partition, self.offset


class R2DProducerAckLedger(R2DContract):
    """Content-addressed simulated acknowledgement ledger for portable evidence."""

    contract_version: Literal["r2d.a1.portable-producer-ack.v1"]
    release: Literal["R2D-A1"]
    acknowledgement_kind: Literal["simulated_portable"]
    real_broker_ack_claim: Literal[False]
    source_manifest_sha256: R2DSha256
    source_manifest_file_sha256: R2DSha256
    inventory_events_file_sha256: R2DSha256
    acknowledged_record_count: NonNegativeInt
    acknowledgements: tuple[R2DProducerAcknowledgement, ...]
    raw_value_multiset_sha256: R2DSha256
    frozen_source_match: bool
    content_sha256: R2DSha256

    _freeze_acknowledgements = field_validator("acknowledgements", mode="before")(_tuple_from_yaml)

    def content_payload(self) -> dict[str, object]:
        """Return every semantic ledger field except its self-address."""

        return {
            "contract_version": self.contract_version,
            "release": self.release,
            "acknowledgement_kind": self.acknowledgement_kind,
            "real_broker_ack_claim": self.real_broker_ack_claim,
            "source_manifest_sha256": self.source_manifest_sha256,
            "source_manifest_file_sha256": self.source_manifest_file_sha256,
            "inventory_events_file_sha256": self.inventory_events_file_sha256,
            "acknowledged_record_count": self.acknowledged_record_count,
            "acknowledgements": [item.model_dump(mode="json") for item in self.acknowledgements],
            "raw_value_multiset_sha256": self.raw_value_multiset_sha256,
            "frozen_source_match": self.frozen_source_match,
        }

    @model_validator(mode="after")
    def ledger_is_ordered_unique_truthful_and_content_addressed(self) -> Self:
        identities = tuple(item.transport_identity for item in self.acknowledgements)
        if identities != tuple(sorted(identities, key=lambda item: (item[1], item[2]))):
            raise ValueError("producer acknowledgements must be ordered by partition then offset")
        if len(identities) != len(set(identities)):
            raise ValueError("producer acknowledgements must have unique transport identities")
        if any(item.partition >= 2 for item in self.acknowledgements):
            raise ValueError(
                "producer acknowledgement partition is outside the two-partition scope"
            )
        if self.acknowledged_record_count != len(self.acknowledgements):
            raise ValueError("acknowledged_record_count must equal the acknowledgement count")
        recomputed_multiset_sha256 = r2d_sha256_multiset_sha256(
            tuple(item.raw_value_sha256 for item in self.acknowledgements)
        )
        if self.raw_value_multiset_sha256 != recomputed_multiset_sha256:
            raise ValueError("producer acknowledgement raw-value multiset SHA-256 mismatch")
        expected_source_match = (
            self.source_manifest_sha256 == R2D_SOURCE_MANIFEST_SHA256
            and self.source_manifest_file_sha256 == R2D_SOURCE_MANIFEST_FILE_SHA256
            and self.inventory_events_file_sha256 == R2D_INVENTORY_EVENTS_FILE_SHA256
            and self.acknowledged_record_count == 9
            and self.raw_value_multiset_sha256 == R2D_RAW_VALUE_MULTISET_SHA256
        )
        if self.frozen_source_match != expected_source_match:
            raise ValueError("frozen_source_match must equal the derived source-contract result")
        if self.content_sha256 != canonical_json_sha256(self.content_payload()):
            raise ValueError("producer acknowledgement ledger content SHA-256 mismatch")
        return self


class R2DCapturedRecord(R2DContract):
    """Content-addressed capture of one unique physical transport record."""

    record: R2DPhysicalRecord
    decision: R2DTransportDecision
    raw_value_sha256: R2DSha256 | None
    physical_fingerprint: R2DSha256
    lineage_sha256: R2DSha256

    @property
    def transport_identity(self) -> tuple[str, int, int]:
        """Expose the physical deduplication identity."""

        return self.record.transport_identity


class R2DCaptureTranscript(R2DContract):
    """Canonical two-partition capture, independent from poll interleaving."""

    topic: Literal["inventory.adjusted.v1"]
    partition_count: Literal[2]
    records: tuple[R2DCapturedRecord, ...]
    content_sha256: R2DSha256
    lineage_sha256: R2DSha256

    def partition_records(self, partition: int) -> tuple[R2DCapturedRecord, ...]:
        """Return a partition's records in canonical offset order."""

        return tuple(item for item in self.records if item.record.partition == partition)


class R2DPartitionCheckpoint(R2DContract):
    """One partition's governed half-open range and durable progress."""

    partition: NonNegativeInt
    start_offset: NonNegativeInt
    end_offset_exclusive: NonNegativeInt
    next_offset: NonNegativeInt
    physical_fingerprints: tuple[R2DSha256, ...]

    @model_validator(mode="after")
    def offsets_form_a_half_open_range(self) -> Self:
        if not self.start_offset <= self.next_offset <= self.end_offset_exclusive:
            raise ValueError("checkpoint offsets must satisfy start <= next <= end")
        return self


class R2DCaptureCheckpoint(R2DContract):
    """Content-addressed checkpoint binding transcript, config, and barriers."""

    topic: Literal["inventory.adjusted.v1"]
    source_manifest_sha256: R2DSha256
    config_sha256: R2DSha256
    transcript_sha256: R2DSha256
    producer_ack_sha256: R2DSha256
    producer_ack_frozen_source_match: bool
    lineage_sha256: R2DSha256
    partitions: tuple[R2DPartitionCheckpoint, ...]
    content_sha256: R2DSha256

    @classmethod
    def from_transcript(
        cls,
        *,
        config: R2DConfig,
        transcript: R2DCaptureTranscript,
        starts: dict[int, int],
        ends: dict[int, int] | None = None,
        next_offsets: dict[int, int] | None = None,
        producer_ack_ledger: R2DProducerAckLedger | None = None,
        require_frozen_source: bool = True,
    ) -> R2DCaptureCheckpoint:
        """Build a checkpoint; completeness is verified by the public verifier."""

        from nexus.ingestion.r2d_capture import build_capture_checkpoint

        return build_capture_checkpoint(
            config=config,
            transcript=transcript,
            starts=starts,
            ends=ends,
            next_offsets=next_offsets,
            producer_ack_ledger=producer_ack_ledger,
            require_frozen_source=require_frozen_source,
        )


class R2DEvidenceCheckpoint(R2DContract):
    """Strict portable evidence join binding source, capture, code, and artifact identities."""

    contract_version: Literal["r2d.a1.evidence-checkpoint.v1"]
    release: Literal["R2D-A1"]
    source_manifest_sha256: Literal[
        "03e32a3f197839134e44b8f6d384787a99ec22e2c088ee4edea8d8322814346b"
    ]
    source_manifest_file_sha256: Literal[
        "aa25517d49cb6c36b76e9b6a218a83e7419359f5a7c40626412797ca13f8d768"
    ]
    inventory_events_file_sha256: Literal[
        "0418e3bf2d1212c766328c94d3ed46342c731f1cdbde954acc2024aea5bdb39c"
    ]
    source_raw_value_multiset_sha256: Literal[
        "d2b8ca4dc666eec083c4b3d795c102ba2d10255cfaa68aaf32a018952b4b7242"
    ]
    producer_ack_sha256: R2DSha256
    transcript_sha256: R2DSha256
    lineage_sha256: R2DSha256
    capture_checkpoint_sha256: R2DSha256
    reconstructed_jsonl_sha256: R2DSha256
    code_sha256: R2DSha256
    artifact_set_sha256: R2DSha256
    content_sha256: R2DSha256

    def content_payload(self) -> dict[str, object]:
        """Return the complete evidence join except its self-address."""

        return {
            key: value
            for key, value in self.model_dump(mode="json").items()
            if key != "content_sha256"
        }

    @model_validator(mode="after")
    def checkpoint_is_content_addressed(self) -> Self:
        if self.content_sha256 != canonical_json_sha256(self.content_payload()):
            raise ValueError("R2D evidence checkpoint content SHA-256 mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        source_contract: R2DSourceContract,
        producer_ack_sha256: str,
        transcript_sha256: str,
        lineage_sha256: str,
        capture_checkpoint_sha256: str,
        reconstructed_jsonl_sha256: str,
        code_sha256: str,
        artifact_set_sha256: str,
    ) -> R2DEvidenceCheckpoint:
        """Create the immutable join from already validated component identities."""

        payload = {
            "contract_version": "r2d.a1.evidence-checkpoint.v1",
            "release": "R2D-A1",
            "source_manifest_sha256": source_contract.source_manifest_sha256,
            "source_manifest_file_sha256": source_contract.source_manifest_file_sha256,
            "inventory_events_file_sha256": source_contract.inventory_events_file_sha256,
            "source_raw_value_multiset_sha256": source_contract.raw_value_multiset_sha256,
            "producer_ack_sha256": producer_ack_sha256,
            "transcript_sha256": transcript_sha256,
            "lineage_sha256": lineage_sha256,
            "capture_checkpoint_sha256": capture_checkpoint_sha256,
            "reconstructed_jsonl_sha256": reconstructed_jsonl_sha256,
            "code_sha256": code_sha256,
            "artifact_set_sha256": artifact_set_sha256,
        }
        return cls.model_validate({**payload, "content_sha256": canonical_json_sha256(payload)})


def load_r2d_config(path: Path = DEFAULT_R2D_CONFIG_PATH) -> R2DConfig:
    """Load and strictly validate the frozen R2D-A1 YAML contract."""

    with path.open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError("R2D configuration root must be a mapping")
    return R2DConfig.model_validate(cast(dict[str, Any], payload))


def r2d_config_sha256(config: R2DConfig) -> str:
    """Hash canonical semantic configuration independently from YAML formatting."""

    encoded = json.dumps(
        config.model_dump(mode="json"),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_json_sha256(payload: object) -> str:
    """Return SHA-256 for deterministic JSON-compatible evidence."""

    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def r2d_lf_canonical_file_sha256(path: Path) -> str:
    """Hash text bytes after canonicalizing every line ending to LF."""

    canonical_bytes = path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return hashlib.sha256(canonical_bytes).hexdigest()


def r2d_schema_bundle_payload(config: R2DSchemaContract) -> dict[str, object]:
    """Return the documented canonical bundle of schema IDs and LF-canonical file hashes."""

    return {
        "contract_version": "r2d.schema-bundle.v1",
        "schemas": [
            {
                "contract_id": config.envelope_schema_id,
                "sha256": config.envelope_schema_sha256,
            },
            {
                "contract_id": config.payload_schema_id,
                "sha256": config.payload_schema_sha256,
            },
        ],
    }


def r2d_schema_bundle_sha256(config: R2DSchemaContract) -> str:
    """Derive the schema bundle identity from its canonical documented payload."""

    return canonical_json_sha256(r2d_schema_bundle_payload(config))


def r2d_sha256_multiset_sha256(values: tuple[str, ...]) -> str:
    """Hash a sorted SHA-256 Counter using the frozen raw-multiset representation."""

    if any(
        len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
        for value in values
    ):
        raise ValueError("SHA-256 multiset values must be lowercase 64-character hex")
    return canonical_json_sha256(tuple(sorted(Counter(values).items())))


def r2d_evidence_bindings_sha256(
    *,
    source_manifest_sha256: str,
    config_sha256: str,
    normalized_business_sha256: str,
    transcript_sha256: str,
    checkpoint_sha256: str,
    producer_ack_sha256: str,
    lineage_sha256: str,
    evidence_checkpoint_sha256: str,
    code_sha256: str,
    artifact_set_sha256: str,
    direct_provenance_sha256: str,
    captured_provenance_sha256: str,
    resource_evidence_sha256: str,
) -> str:
    """Bind every top-level evidence identity into one canonical receipt hash."""

    payload = {
        "contract_version": "r2d.a1.evidence-bindings.v1",
        "bindings": {
            "source_manifest_sha256": source_manifest_sha256,
            "config_sha256": config_sha256,
            "normalized_business_sha256": normalized_business_sha256,
            "transcript_sha256": transcript_sha256,
            "checkpoint_sha256": checkpoint_sha256,
            "producer_ack_sha256": producer_ack_sha256,
            "lineage_sha256": lineage_sha256,
            "evidence_checkpoint_sha256": evidence_checkpoint_sha256,
            "code_sha256": code_sha256,
            "artifact_set_sha256": artifact_set_sha256,
            "direct_provenance_sha256": direct_provenance_sha256,
            "captured_provenance_sha256": captured_provenance_sha256,
            "resource_evidence_sha256": resource_evidence_sha256,
        },
    }
    return canonical_json_sha256(payload)
