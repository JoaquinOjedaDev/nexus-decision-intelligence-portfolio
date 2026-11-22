"""Append-only lifecycle storage and deterministic state projection.

The ledger is deliberately storage-light for the R1 vertical slice. The in-memory
implementation is useful in tests and the JSONL implementation uses an atomic
same-directory replacement so a failed write leaves the last committed ledger intact.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from threading import RLock
from typing import Protocol
from uuid import UUID

from nexus.contracts import (
    ActorRole,
    LifecycleEvent,
    LifecycleEventType,
    RecommendationManifest,
)
from nexus.policies import (
    ApprovalPolicy,
    AuthorizationError,
    Permission,
    assert_permission,
)


class WorkflowError(ValueError):
    """Base class for lifecycle failures that must fail closed."""


class IllegalTransitionError(WorkflowError):
    """Raised when an event is not legal from the current projected state."""


class ManifestBindingError(WorkflowError):
    """Raised when an event does not bind to the exact immutable recommendation."""


class LedgerCorruptionError(WorkflowError):
    """Raised when persisted history cannot be parsed or replayed safely."""


class DecisionState(StrEnum):
    CREATED = "created"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    APPROVAL_EXPIRED = "approval_expired"
    INVALIDATED = "invalidated"
    EXECUTION_PENDING = "execution_pending"
    EXECUTED = "executed"
    EXECUTION_FAILED = "execution_failed"
    OUTCOME_OBSERVED = "outcome_observed"


_TRANSITIONS: dict[DecisionState, frozenset[LifecycleEventType]] = {
    DecisionState.CREATED: frozenset({LifecycleEventType.SUBMITTED_FOR_APPROVAL}),
    DecisionState.PENDING_APPROVAL: frozenset(
        {
            LifecycleEventType.APPROVAL_GRANTED,
            LifecycleEventType.APPROVAL_REJECTED,
            LifecycleEventType.APPROVAL_EXPIRED,
            LifecycleEventType.INVALIDATED,
        }
    ),
    DecisionState.APPROVED: frozenset(
        {
            LifecycleEventType.EXECUTION_REQUESTED,
            LifecycleEventType.APPROVAL_EXPIRED,
            LifecycleEventType.INVALIDATED,
        }
    ),
    DecisionState.REJECTED: frozenset(),
    DecisionState.APPROVAL_EXPIRED: frozenset(),
    DecisionState.INVALIDATED: frozenset(),
    DecisionState.EXECUTION_PENDING: frozenset(
        {
            LifecycleEventType.EXECUTION_SUCCEEDED,
            LifecycleEventType.EXECUTION_FAILED,
        }
    ),
    DecisionState.EXECUTED: frozenset({LifecycleEventType.OUTCOME_OBSERVED}),
    DecisionState.EXECUTION_FAILED: frozenset({LifecycleEventType.EXECUTION_REQUESTED}),
    DecisionState.OUTCOME_OBSERVED: frozenset(),
}

_EVENT_STATES: dict[LifecycleEventType, DecisionState] = {
    LifecycleEventType.SUBMITTED_FOR_APPROVAL: DecisionState.PENDING_APPROVAL,
    LifecycleEventType.APPROVAL_GRANTED: DecisionState.APPROVED,
    LifecycleEventType.APPROVAL_REJECTED: DecisionState.REJECTED,
    LifecycleEventType.APPROVAL_EXPIRED: DecisionState.APPROVAL_EXPIRED,
    LifecycleEventType.INVALIDATED: DecisionState.INVALIDATED,
    LifecycleEventType.EXECUTION_REQUESTED: DecisionState.EXECUTION_PENDING,
    LifecycleEventType.EXECUTION_SUCCEEDED: DecisionState.EXECUTED,
    LifecycleEventType.EXECUTION_FAILED: DecisionState.EXECUTION_FAILED,
    LifecycleEventType.OUTCOME_OBSERVED: DecisionState.OUTCOME_OBSERVED,
}


@dataclass(frozen=True, slots=True)
class LifecycleProjection:
    """State reconstructed solely from an immutable manifest and lifecycle events."""

    manifest_id: UUID
    manifest_fingerprint: str
    state: DecisionState
    last_sequence: int
    events: tuple[LifecycleEvent, ...]
    approval: LifecycleEvent | None = None
    execution_reference: str | None = None
    outcome: LifecycleEvent | None = None


class LifecycleLedger(Protocol):
    """Minimal ledger interface used by the workflow service."""

    def append(
        self,
        manifest: RecommendationManifest,
        event: LifecycleEvent,
        *,
        approval_policy: ApprovalPolicy | None = None,
    ) -> LifecycleProjection: ...

    def events_for(
        self,
        manifest: RecommendationManifest,
        *,
        approval_policy: ApprovalPolicy | None = None,
    ) -> tuple[LifecycleEvent, ...]: ...

    def project(
        self,
        manifest: RecommendationManifest,
        *,
        approval_policy: ApprovalPolicy | None = None,
    ) -> LifecycleProjection: ...


def replay_lifecycle(
    manifest: RecommendationManifest,
    events: Iterable[LifecycleEvent],
    *,
    approval_policy: ApprovalPolicy | None = None,
) -> LifecycleProjection:
    """Validate and replay history; any ambiguity or policy violation fails closed."""

    policy = approval_policy or ApprovalPolicy()
    ordered = tuple(events)
    state = DecisionState.CREATED
    approval: LifecycleEvent | None = None
    execution_reference: str | None = None
    outcome: LifecycleEvent | None = None
    seen_event_ids: set[UUID] = set()
    previous: LifecycleEvent | None = None

    for expected_sequence, event in enumerate(ordered, start=1):
        if event.event_id in seen_event_ids:
            raise LedgerCorruptionError(f"duplicate lifecycle event_id {event.event_id}")
        seen_event_ids.add(event.event_id)

        if event.sequence != expected_sequence:
            raise LedgerCorruptionError(
                f"expected sequence {expected_sequence}, received {event.sequence}"
            )
        _assert_manifest_binding(manifest, event)
        if previous is not None and event.occurred_at < previous.occurred_at:
            raise LedgerCorruptionError("lifecycle event timestamps must be nondecreasing")

        allowed = _TRANSITIONS[state]
        if event.event_type not in allowed:
            raise IllegalTransitionError(
                f"{event.event_type.value!r} is not legal from state {state.value!r}"
            )

        if event.event_type is LifecycleEventType.SUBMITTED_FOR_APPROVAL:
            assert_permission(event.actor_role, Permission.SUBMIT_FOR_APPROVAL)
            if event.actor_id != manifest.created_by:
                raise AuthorizationError("only the manifest maker can submit the recommendation")
            if event.occurred_at < manifest.created_at:
                raise AuthorizationError("submission cannot precede manifest creation")
        elif event.event_type is LifecycleEventType.APPROVAL_GRANTED:
            policy.assert_valid_approval(manifest, event)
            approval = event
        elif event.event_type is LifecycleEventType.APPROVAL_REJECTED:
            assert_permission(event.actor_role, Permission.REJECT)
            if event.actor_id == manifest.created_by:
                raise AuthorizationError("maker-checker violation: the maker cannot reject")
        elif event.event_type is LifecycleEventType.EXECUTION_REQUESTED:
            if approval is None:
                raise AuthorizationError("execution requires a recorded approval")
            policy.assert_execution_authorized(
                manifest,
                approval,
                event,
                now=event.occurred_at,
            )
        elif event.event_type in {
            LifecycleEventType.APPROVAL_EXPIRED,
            LifecycleEventType.INVALIDATED,
            LifecycleEventType.EXECUTION_SUCCEEDED,
            LifecycleEventType.EXECUTION_FAILED,
            LifecycleEventType.OUTCOME_OBSERVED,
        }:
            if event.actor_role is not ActorRole.SYSTEM:
                raise AuthorizationError(f"{event.event_type.value} must be recorded by the system")

        if event.event_type is LifecycleEventType.EXECUTION_SUCCEEDED:
            execution_reference = event.external_reference
        elif event.event_type is LifecycleEventType.OUTCOME_OBSERVED:
            if execution_reference is None:
                raise IllegalTransitionError("an outcome requires a successful execution")
            if event.external_reference != execution_reference:
                raise ManifestBindingError(
                    "outcome external_reference does not match the successful execution"
                )
            if previous is None or event.occurred_at <= previous.occurred_at:
                raise IllegalTransitionError("outcome must be observed after execution")
            if (
                event.outcome_snapshot is None
                or event.outcome_snapshot.as_of < previous.occurred_at
                or event.outcome_snapshot.as_of > event.occurred_at
            ):
                raise IllegalTransitionError(
                    "outcome snapshot must be between execution and outcome observation"
                )
            outcome = event

        state = _EVENT_STATES[event.event_type]
        previous = event

    return LifecycleProjection(
        manifest_id=manifest.manifest_id,
        manifest_fingerprint=manifest.fingerprint,
        state=state,
        last_sequence=len(ordered),
        events=ordered,
        approval=approval,
        execution_reference=execution_reference,
        outcome=outcome,
    )


def _assert_manifest_binding(
    manifest: RecommendationManifest,
    event: LifecycleEvent,
) -> None:
    if event.manifest_id != manifest.manifest_id:
        raise ManifestBindingError("event is bound to a different manifest_id")
    if event.manifest_fingerprint != manifest.fingerprint:
        raise ManifestBindingError("event fingerprint does not match the manifest")


class InMemoryLifecycleLedger:
    """Append-only process-local ledger."""

    def __init__(self) -> None:
        self._events: dict[UUID, list[LifecycleEvent]] = {}
        self._lock = RLock()

    def append(
        self,
        manifest: RecommendationManifest,
        event: LifecycleEvent,
        *,
        approval_policy: ApprovalPolicy | None = None,
    ) -> LifecycleProjection:
        with self._lock:
            current = self._events.get(manifest.manifest_id, [])
            projection = replay_lifecycle(
                manifest,
                (*current, event),
                approval_policy=approval_policy,
            )
            self._events.setdefault(manifest.manifest_id, []).append(event)
            return projection

    def events_for(
        self,
        manifest: RecommendationManifest,
        *,
        approval_policy: ApprovalPolicy | None = None,
    ) -> tuple[LifecycleEvent, ...]:
        with self._lock:
            events = tuple(self._events.get(manifest.manifest_id, ()))
        replay_lifecycle(manifest, events, approval_policy=approval_policy)
        return events

    def project(
        self,
        manifest: RecommendationManifest,
        *,
        approval_policy: ApprovalPolicy | None = None,
    ) -> LifecycleProjection:
        events = self.events_for(manifest, approval_policy=approval_policy)
        return replay_lifecycle(manifest, events, approval_policy=approval_policy)


class JsonlLifecycleLedger:
    """Portable JSONL ledger committed by an atomic ``os.replace`` operation.

    A write validates the full projected history, writes and fsyncs a temporary file
    in the ledger directory, then atomically replaces the committed path. A crash before
    replacement leaves the previous valid file available for recovery.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = RLock()

    def append(
        self,
        manifest: RecommendationManifest,
        event: LifecycleEvent,
        *,
        approval_policy: ApprovalPolicy | None = None,
    ) -> LifecycleProjection:
        with self._lock:
            current = self._read()
            projection = replay_lifecycle(
                manifest,
                (*current, event),
                approval_policy=approval_policy,
            )
            self._atomic_write((*current, event))
            return projection

    def events_for(
        self,
        manifest: RecommendationManifest,
        *,
        approval_policy: ApprovalPolicy | None = None,
    ) -> tuple[LifecycleEvent, ...]:
        with self._lock:
            events = self._read()
        replay_lifecycle(manifest, events, approval_policy=approval_policy)
        return events

    def project(
        self,
        manifest: RecommendationManifest,
        *,
        approval_policy: ApprovalPolicy | None = None,
    ) -> LifecycleProjection:
        events = self.events_for(manifest, approval_policy=approval_policy)
        return replay_lifecycle(manifest, events, approval_policy=approval_policy)

    def _read(self) -> tuple[LifecycleEvent, ...]:
        if not self.path.exists():
            return ()
        events: list[LifecycleEvent] = []
        try:
            with self.path.open("r", encoding="utf-8") as ledger_file:
                for line_number, line in enumerate(ledger_file, start=1):
                    if not line.strip():
                        raise LedgerCorruptionError(f"blank JSONL record at line {line_number}")
                    try:
                        events.append(LifecycleEvent.model_validate_json(line))
                    except ValueError as exc:
                        raise LedgerCorruptionError(
                            f"invalid lifecycle event at line {line_number}"
                        ) from exc
        except UnicodeError as exc:
            raise LedgerCorruptionError("ledger is not valid UTF-8") from exc
        return tuple(events)

    def _atomic_write(self, events: tuple[LifecycleEvent, ...]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
            )
            temporary_path = Path(temporary_name)
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as output:
                for event in events:
                    output.write(event.model_dump_json())
                    output.write("\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary_path, self.path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
