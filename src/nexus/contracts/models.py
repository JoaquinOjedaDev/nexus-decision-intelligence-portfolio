"""Versioned, storage-neutral contracts for the NEXUS decision lifecycle.

The recommendation manifest deliberately contains only decision-time facts. Approval,
execution, invalidation, and observed outcomes are separate append-only lifecycle events.
This makes a recommendation reproducible without rewriting its history.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    FiniteFloat,
    NonNegativeInt,
    PositiveInt,
    StringConstraints,
    field_validator,
    model_validator,
)

Identifier = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=160,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    ),
]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
EventTypeName = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=3,
        max_length=120,
        pattern=r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$",
    ),
]

_UNSET_FINGERPRINT = "0" * 64


def _utc(value: datetime) -> datetime:
    """Reject naive datetimes and normalize equivalent instants for canonical hashing."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include an explicit timezone offset")
    return value.astimezone(UTC)


def _canonical_json(value: object) -> bytes:
    """Encode JSON deterministically, including a single UTC datetime representation."""

    def normalize(item: object) -> object:
        if isinstance(item, datetime):
            return _utc(item).isoformat(timespec="microseconds").replace("+00:00", "Z")
        if isinstance(item, UUID):
            return str(item)
        if isinstance(item, StrEnum):
            return item.value
        if isinstance(item, BaseModel):
            return normalize(item.model_dump(mode="python"))
        if isinstance(item, dict):
            return {str(key): normalize(val) for key, val in item.items()}
        if isinstance(item, (tuple, list, set, frozenset)):
            return [normalize(element) for element in item]
        return item

    return json.dumps(
        normalize(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class ContractModel(BaseModel):
    """Default contract behavior: strict, immutable, and closed to unknown fields."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
    )


class ActorRole(StrEnum):
    PLANNER = "planner"
    APPROVER = "approver"
    AUDITOR = "auditor"
    SYSTEM = "system"


class EventEnvelope(ContractModel):
    """Transport-neutral envelope used by batch, webhook, CDC, and stream adapters."""

    contract_version: Annotated[str, StringConstraints(pattern=r"^1\.[0-9]+$")] = "1.0"
    event_id: UUID
    event_type: EventTypeName
    event_version: PositiveInt = 1
    source: Identifier
    subject_id: Identifier
    occurred_at: AwareDatetime
    ingested_at: AwareDatetime
    idempotency_key: Identifier
    correlation_id: Identifier | None = None
    trace_id: Identifier | None = None
    payload: dict[str, Any]

    _normalize_occurred_at = field_validator("occurred_at")(_utc)
    _normalize_ingested_at = field_validator("ingested_at")(_utc)

    @model_validator(mode="after")
    def observed_after_occurrence(self) -> Self:
        if self.ingested_at < self.occurred_at:
            raise ValueError("ingested_at cannot precede occurred_at")
        return self


class DataSnapshotRef(ContractModel):
    """Logical snapshot identity with no dependency on a storage engine or URI."""

    dataset: Identifier
    snapshot_id: Identifier
    schema_version: Identifier
    as_of: AwareDatetime
    content_sha256: Sha256
    row_count: NonNegativeInt
    partition_keys: tuple[Identifier, ...] = ()

    _normalize_as_of = field_validator("as_of")(_utc)


class ArtifactRef(ContractModel):
    """Content-addressed model, forecast, optimizer, or policy artifact."""

    name: Identifier
    version: Identifier
    content_sha256: Sha256


class RecommendationLine(ContractModel):
    """A single SKU/location replenishment recommendation."""

    sku_id: Identifier
    location_id: Identifier
    recommended_quantity: NonNegativeInt
    horizon_days: PositiveInt
    reason_code: Identifier


class RecommendationManifest(ContractModel):
    """Immutable, content-addressed record of everything known at decision time.

    ``manifest_id``, ``created_at``, and trace metadata are operational identity and are
    intentionally excluded from the fingerprint. All fields that can change the business
    decision are included. Sequence ordering for snapshots and recommendation lines is
    canonicalized because those collections are semantically sets.
    """

    contract_version: Annotated[str, StringConstraints(pattern=r"^1\.[0-9]+$")] = "1.0"
    manifest_id: UUID
    decision_type: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")] = (
        "weekly_replenishment"
    )
    decision_key: Identifier
    decision_time: AwareDatetime
    valid_until: AwareDatetime
    created_at: AwareDatetime
    created_by: Identifier
    input_snapshots: tuple[DataSnapshotRef, ...]
    forecast_artifact: ArtifactRef
    optimizer_artifact: ArtifactRef
    policy_artifact: ArtifactRef
    scenario_seed: NonNegativeInt
    recommendations: tuple[RecommendationLine, ...]
    correlation_id: Identifier | None = None
    trace_id: Identifier | None = None
    fingerprint: Sha256 = _UNSET_FINGERPRINT

    _normalize_decision_time = field_validator("decision_time")(_utc)
    _normalize_valid_until = field_validator("valid_until")(_utc)
    _normalize_created_at = field_validator("created_at")(_utc)

    @model_validator(mode="after")
    def validate_and_fingerprint(self) -> Self:
        if self.valid_until <= self.decision_time:
            raise ValueError("valid_until must be later than decision_time")
        if self.created_at < self.decision_time:
            raise ValueError("created_at cannot precede decision_time")
        if not self.input_snapshots:
            raise ValueError("at least one input snapshot is required")
        if not self.recommendations:
            raise ValueError("at least one recommendation is required")
        if any(snapshot.as_of > self.decision_time for snapshot in self.input_snapshots):
            raise ValueError("input snapshots cannot be later than decision_time")

        snapshot_keys = [(item.dataset, item.snapshot_id) for item in self.input_snapshots]
        if len(snapshot_keys) != len(set(snapshot_keys)):
            raise ValueError("input snapshots must be unique by dataset and snapshot_id")
        line_keys = [(item.sku_id, item.location_id) for item in self.recommendations]
        if len(line_keys) != len(set(line_keys)):
            raise ValueError("recommendations must be unique by sku_id and location_id")

        calculated = self.calculate_fingerprint()
        if self.fingerprint != _UNSET_FINGERPRINT and not hmac.compare_digest(
            self.fingerprint, calculated
        ):
            raise ValueError("fingerprint does not match canonical decision-time content")
        object.__setattr__(self, "fingerprint", calculated)
        return self

    def decision_time_content(self) -> dict[str, object]:
        """Return the exact content protected by the manifest fingerprint."""

        snapshots = sorted(
            self.input_snapshots,
            key=lambda item: (item.dataset, item.snapshot_id, item.content_sha256),
        )
        lines = sorted(
            self.recommendations,
            key=lambda item: (item.sku_id, item.location_id),
        )
        return {
            "contract_version": self.contract_version,
            "decision_type": self.decision_type,
            "decision_key": self.decision_key,
            "decision_time": self.decision_time,
            "valid_until": self.valid_until,
            "created_by": self.created_by,
            "input_snapshots": snapshots,
            "forecast_artifact": self.forecast_artifact,
            "optimizer_artifact": self.optimizer_artifact,
            "policy_artifact": self.policy_artifact,
            "scenario_seed": self.scenario_seed,
            "recommendations": lines,
        }

    def calculate_fingerprint(self) -> str:
        return hashlib.sha256(_canonical_json(self.decision_time_content())).hexdigest()


class LifecycleEventType(StrEnum):
    """Allowed append-only transitions after a recommendation has been created."""

    SUBMITTED_FOR_APPROVAL = "submitted_for_approval"
    APPROVAL_GRANTED = "approval_granted"
    APPROVAL_REJECTED = "approval_rejected"
    APPROVAL_EXPIRED = "approval_expired"
    INVALIDATED = "invalidated"
    EXECUTION_REQUESTED = "execution_requested"
    EXECUTION_SUCCEEDED = "execution_succeeded"
    EXECUTION_FAILED = "execution_failed"
    OUTCOME_OBSERVED = "outcome_observed"


class OutcomeMetric(ContractModel):
    name: Identifier
    value: FiniteFloat
    unit: Identifier


class LifecycleEvent(ContractModel):
    """One immutable event in the append-only recommendation lifecycle ledger."""

    contract_version: Annotated[str, StringConstraints(pattern=r"^1\.[0-9]+$")] = "1.0"
    event_id: UUID
    sequence: PositiveInt
    event_type: LifecycleEventType
    manifest_id: UUID
    manifest_fingerprint: Sha256
    occurred_at: AwareDatetime
    actor_id: Identifier
    actor_role: ActorRole
    correlation_id: Identifier
    reason: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
    approval_expires_at: AwareDatetime | None = None
    external_reference: Identifier | None = None
    outcome_snapshot: DataSnapshotRef | None = None
    outcome_metrics: tuple[OutcomeMetric, ...] = ()

    _normalize_occurred_at = field_validator("occurred_at")(_utc)

    @field_validator("approval_expires_at")
    @classmethod
    def normalize_optional_expiry(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _utc(value)

    @model_validator(mode="after")
    def validate_type_specific_fields(self) -> Self:
        if self.event_type is LifecycleEventType.APPROVAL_GRANTED:
            if self.approval_expires_at is None:
                raise ValueError("approval_granted requires approval_expires_at")
            if self.approval_expires_at <= self.occurred_at:
                raise ValueError("approval_expires_at must be later than occurred_at")
        elif self.approval_expires_at is not None:
            raise ValueError("approval_expires_at is only valid for approval_granted")

        if self.event_type is LifecycleEventType.OUTCOME_OBSERVED:
            if self.outcome_snapshot is None or not self.outcome_metrics:
                raise ValueError("outcome_observed requires a snapshot and at least one metric")
        elif self.outcome_snapshot is not None or self.outcome_metrics:
            raise ValueError("outcome fields are only valid for outcome_observed")

        execution_results = {
            LifecycleEventType.EXECUTION_SUCCEEDED,
            LifecycleEventType.EXECUTION_FAILED,
        }
        if self.event_type in execution_results and self.external_reference is None:
            raise ValueError("execution result events require external_reference")
        return self
