"""Strict contracts for the first real-service R2D-A2 checkpoint."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import NonNegativeInt, PositiveInt, field_validator, model_validator

from nexus.ingestion.r2d_contracts import (
    R2DContract,
    R2DSha256,
    canonical_json_sha256,
    r2d_lf_canonical_file_sha256,
)

DEFAULT_R2D_A2A_CONFIG_PATH = Path("configs/ingestion/r2d-a2a.yaml")
R2DA2aCheckName = Literal[
    "real_topic_topology",
    "receipted_fresh_empty_initialization",
    "real_broker_acks_all",
    "atomic_postgres_inbox_progress",
    "manual_commit_convergence",
    "exact_byte_readback",
]
R2D_A2A_PASS_CHECK_NAMES: tuple[R2DA2aCheckName, ...] = (
    "real_topic_topology",
    "receipted_fresh_empty_initialization",
    "real_broker_acks_all",
    "atomic_postgres_inbox_progress",
    "manual_commit_convergence",
    "exact_byte_readback",
)
R2D_A2A_CODE_FILES = (
    "src/nexus/r2d_a2a.py",
    "src/nexus/cli.py",
    "src/nexus/r2d.py",
    "src/nexus/simulation/__init__.py",
    "src/nexus/simulation/supply_network.py",
    "src/nexus/ingestion/r2d_service.py",
    "src/nexus/ingestion/r2d_service_contracts.py",
    "src/nexus/ingestion/r2d_capture.py",
    "src/nexus/ingestion/r2d_contracts.py",
    "infra/compose.yaml",
    ".github/workflows/ci.yml",
    "pyproject.toml",
    "uv.lock",
)


class R2DA2BrokerContract(R2DContract):
    """Broker behavior that must be observed by the A2a service gate."""

    topic: Literal["inventory.adjusted.v1"]
    partition_count: Literal[2]
    replication_factor: Literal[1]
    producer_acks: Literal["all"]
    producer_enable_idempotence: Literal[True]
    consumer_group_id: Literal["nexus-r2d-a2a-primary-v1"]
    consumer_auto_offset_commit: Literal[False]
    consumer_auto_offset_store: Literal[False]
    isolation_level: Literal["read_committed"]


class R2DA2PostgresContract(R2DContract):
    """Minimal PostgreSQL ledger contract; connection secrets stay outside YAML."""

    schema_name: Literal["r2d_capture"]
    migration_version: Literal["001_r2d_capture"]
    migration_path: Literal["infra/postgres/migrations/001_r2d_capture.sql"]
    lock_timeout_ms: PositiveInt
    statement_timeout_ms: PositiveInt


class R2DA2GateContract(R2DContract):
    """Exact bounded service-smoke limits."""

    maximum_records: PositiveInt
    service_start_timeout_seconds: PositiveInt
    poll_timeout_seconds: PositiveInt
    shutdown_timeout_seconds: PositiveInt
    require_fresh_empty_topic: Literal[True]


class R2DA2ServiceConfig(R2DContract):
    """Frozen non-secret service configuration for R2D-A2a."""

    release: Literal["R2D-A2a"]
    contract_version: Literal["1.0.0"]
    broker: R2DA2BrokerContract
    postgres: R2DA2PostgresContract
    gate: R2DA2GateContract


class R2DBrokerAcknowledgement(R2DContract):
    """One delivery callback accepted by a producer configured with ``acks=all``."""

    acknowledgement_kind: Literal["broker_acks_all"]
    real_broker_ack_claim: Literal[True]
    topic: Literal["inventory.adjusted.v1"]
    partition: NonNegativeInt
    offset: NonNegativeInt
    key_hex: str
    raw_value_sha256: R2DSha256
    broker_timestamp_ms: NonNegativeInt

    @model_validator(mode="after")
    def acknowledgement_is_in_scope(self) -> Self:
        if self.partition >= 2:
            raise ValueError("broker acknowledgement partition is outside the frozen topology")
        if not self.key_hex or len(self.key_hex) % 2:
            raise ValueError("broker acknowledgement key_hex must be nonempty even-length hex")
        try:
            bytes.fromhex(self.key_hex)
        except ValueError as exc:
            raise ValueError("broker acknowledgement key_hex is not hexadecimal") from exc
        return self


class R2DBrokerAckLedger(R2DContract):
    """Canonical real-broker acknowledgement evidence for the bounded fixture."""

    contract_version: Literal["r2d.a2a.broker-ack.v1"]
    release: Literal["R2D-A2a"]
    producer_acks: Literal["all"]
    real_broker_ack_claim: Literal[True]
    acknowledgements: tuple[R2DBrokerAcknowledgement, ...]
    acknowledged_record_count: NonNegativeInt
    content_sha256: R2DSha256

    @field_validator("acknowledgements", mode="before")
    @classmethod
    def acknowledgements_are_immutable(cls, value: object) -> tuple[object, ...] | object:
        """Accept canonical JSON arrays but freeze them inside the model."""

        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def ledger_is_ordered_unique_and_content_addressed(self) -> Self:
        identities = tuple(
            (item.topic, item.partition, item.offset) for item in self.acknowledgements
        )
        if identities != tuple(sorted(identities, key=lambda item: (item[1], item[2]))):
            raise ValueError("broker acknowledgements must be ordered by partition then offset")
        if len(identities) != len(set(identities)):
            raise ValueError("broker acknowledgements must have unique transport identities")
        if self.acknowledged_record_count != len(self.acknowledgements):
            raise ValueError("acknowledged_record_count differs from acknowledgement count")
        if self.content_sha256 != canonical_json_sha256(self.content_payload()):
            raise ValueError("broker acknowledgement ledger content SHA-256 mismatch")
        return self

    def content_payload(self) -> dict[str, object]:
        """Return every semantic field except the self-address."""

        return {
            "contract_version": self.contract_version,
            "release": self.release,
            "producer_acks": self.producer_acks,
            "real_broker_ack_claim": self.real_broker_ack_claim,
            "acknowledgements": [item.model_dump(mode="json") for item in self.acknowledgements],
            "acknowledged_record_count": self.acknowledged_record_count,
        }


class R2DA2aVerificationCheck(R2DContract):
    """One exact service-foundation PASS check."""

    name: R2DA2aCheckName
    passed: Literal[True]
    evidence_sha256: R2DSha256


class R2DA2aVerificationReceipt(R2DContract):
    """Bounded A2a receipt; crash recovery and downstream parity remain explicitly unrun."""

    contract_version: Literal["r2d.a2a.verification-receipt.v1"]
    release: Literal["R2D-A2a"]
    status: Literal["PASS"]
    checks: tuple[R2DA2aVerificationCheck, ...]
    topic: Literal["inventory.adjusted.v1"]
    partition_count: Literal[2]
    replication_factor: Literal[1]
    producer_acks: Literal["all"]
    record_count: NonNegativeInt
    starts: tuple[NonNegativeInt, ...]
    ends: tuple[NonNegativeInt, ...]
    next_offsets: tuple[NonNegativeInt, ...]
    data_config_sha256: R2DSha256
    service_config_sha256: R2DSha256
    migration_sha256: R2DSha256
    code_sha256: R2DSha256
    broker_ack_sha256: R2DSha256
    transcript_sha256: R2DSha256
    lineage_sha256: R2DSha256
    raw_value_multiset_sha256: R2DSha256
    service_foundation_status: Literal["PASS"]
    crash_recovery_status: Literal["NOT_RUN"]
    downstream_parity_status: Literal["NOT_RUN"]
    r2d_a2_status: Literal["IN_PROGRESS"]
    full_r2d_a_status: Literal["OPEN"]
    full_g2_status: Literal["OPEN"]
    exactly_once_claim: Literal[False]
    content_sha256: R2DSha256

    @field_validator("checks", "starts", "ends", "next_offsets", mode="before")
    @classmethod
    def sequences_are_immutable(cls, value: object) -> tuple[object, ...] | object:
        return tuple(value) if isinstance(value, list) else value

    def content_payload(self) -> dict[str, object]:
        """Return every semantic field except the self-address."""

        return self.model_dump(mode="json", exclude={"content_sha256"})

    @model_validator(mode="after")
    def receipt_is_complete_honest_and_content_addressed(self) -> Self:
        check_names = tuple(item.name for item in self.checks)
        if check_names != R2D_A2A_PASS_CHECK_NAMES:
            raise ValueError("A2a receipt checks are incomplete, duplicated, or reordered")
        if self.record_count <= 0 or self.record_count > 64:
            raise ValueError("A2a receipt record_count is outside the bounded scope")
        if len(self.starts) != 2 or len(self.ends) != 2 or len(self.next_offsets) != 2:
            raise ValueError("A2a receipt must bind exactly two partition offsets")
        if self.starts != (0, 0):
            raise ValueError("A2a receipt must prove receipted fresh-empty starts")
        if self.next_offsets != self.ends:
            raise ValueError("A2a receipt durable progress must equal the frozen end vector")
        if any(start > end for start, end in zip(self.starts, self.ends, strict=True)):
            raise ValueError("A2a receipt contains an inverted partition range")
        if self.content_sha256 != canonical_json_sha256(self.content_payload()):
            raise ValueError("A2a receipt content SHA-256 mismatch")
        return self


def load_r2d_a2a_config(path: Path = DEFAULT_R2D_A2A_CONFIG_PATH) -> R2DA2ServiceConfig:
    """Load the strict service configuration from YAML."""

    return R2DA2ServiceConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


def r2d_a2a_config_sha256(config: R2DA2ServiceConfig) -> str:
    """Hash the canonical non-secret service configuration."""

    return canonical_json_sha256(config.model_dump(mode="json"))


def migration_sha256(project_root: Path, config: R2DA2ServiceConfig) -> str:
    """Hash the PostgreSQL migration with checked-in text line endings canonicalized."""

    path = project_root.resolve() / config.postgres.migration_path
    return r2d_lf_canonical_file_sha256(path)


def r2d_a2a_code_sha256(project_root: Path) -> str:
    """Bind the exact checked-in code, Compose topology, and dependency lock used by A2a."""

    root = project_root.resolve()
    identities = {path: r2d_lf_canonical_file_sha256(root / path) for path in R2D_A2A_CODE_FILES}
    return canonical_json_sha256(identities)
