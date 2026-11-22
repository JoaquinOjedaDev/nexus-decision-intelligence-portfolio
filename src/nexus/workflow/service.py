"""High-level, deterministic lifecycle operations for one recommendation manifest."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from nexus.contracts import (
    ActorRole,
    DataSnapshotRef,
    LifecycleEvent,
    LifecycleEventType,
    OutcomeMetric,
    RecommendationManifest,
)
from nexus.policies import ApprovalPolicy
from nexus.workflow.executor import (
    MockErpEffect,
    MockErpExecutor,
    execution_idempotency_key,
)
from nexus.workflow.ledger import (
    DecisionState,
    IllegalTransitionError,
    LifecycleLedger,
    LifecycleProjection,
    ManifestBindingError,
)


class RecommendationWorkflow:
    """Drive a recommendation through approval, execution, and delayed outcome."""

    def __init__(
        self,
        manifest: RecommendationManifest,
        *,
        ledger: LifecycleLedger,
        executor: MockErpExecutor,
        approval_policy: ApprovalPolicy | None = None,
    ) -> None:
        self.manifest = manifest
        self.ledger = ledger
        self.executor = executor
        self.approval_policy = approval_policy or ApprovalPolicy()

    @property
    def projection(self) -> LifecycleProjection:
        return self.ledger.project(
            self.manifest,
            approval_policy=self.approval_policy,
        )

    def submit(
        self,
        *,
        occurred_at: datetime,
        actor_id: str,
        correlation_id: str,
        reason: str = "Recommendation submitted for independent review.",
    ) -> LifecycleProjection:
        return self._append(
            event_type=LifecycleEventType.SUBMITTED_FOR_APPROVAL,
            occurred_at=occurred_at,
            actor_id=actor_id,
            actor_role=ActorRole.PLANNER,
            correlation_id=correlation_id,
            reason=reason,
        )

    def approve(
        self,
        *,
        occurred_at: datetime,
        approval_expires_at: datetime,
        actor_id: str,
        correlation_id: str,
        actor_role: ActorRole = ActorRole.APPROVER,
        reason: str = "Recommendation approved within delegated authority.",
    ) -> LifecycleProjection:
        return self._append(
            event_type=LifecycleEventType.APPROVAL_GRANTED,
            occurred_at=occurred_at,
            actor_id=actor_id,
            actor_role=actor_role,
            correlation_id=correlation_id,
            reason=reason,
            approval_expires_at=approval_expires_at,
        )

    def execute(
        self,
        *,
        occurred_at: datetime,
        correlation_id: str,
        idempotency_key: str | None = None,
        actor_id: str = "mock-erp-adapter",
    ) -> MockErpEffect:
        """Create at most one mock ERP effect and record its successful execution.

        Repeating the call with the same key returns the original effect. If history
        already contains the successful execution, no duplicate lifecycle event is added.
        """

        stable_key = execution_idempotency_key(self.manifest)
        key = idempotency_key or stable_key
        if key != stable_key:
            raise ManifestBindingError(
                "execution idempotency_key must be the stable key for this manifest"
            )
        projection = self.projection

        if projection.state in {DecisionState.EXECUTED, DecisionState.OUTCOME_OBSERVED}:
            effect = self.executor.execute(self.manifest, idempotency_key=key)
            if effect.external_reference != projection.execution_reference:
                raise ManifestBindingError(
                    "idempotent ERP reference does not match recorded execution"
                )
            return effect

        if projection.state in {DecisionState.APPROVED, DecisionState.EXECUTION_FAILED}:
            projection = self._append(
                event_type=LifecycleEventType.EXECUTION_REQUESTED,
                occurred_at=occurred_at,
                actor_id=actor_id,
                actor_role=ActorRole.SYSTEM,
                correlation_id=correlation_id,
                reason=f"Execution requested with idempotency key {key}.",
                deterministic_salt=key,
            )

        if projection.state is not DecisionState.EXECUTION_PENDING:
            raise IllegalTransitionError(
                f"execution is not legal from state {projection.state.value!r}"
            )

        effect = self.executor.execute(self.manifest, idempotency_key=key)
        self._append(
            event_type=LifecycleEventType.EXECUTION_SUCCEEDED,
            occurred_at=occurred_at,
            actor_id=actor_id,
            actor_role=ActorRole.SYSTEM,
            correlation_id=correlation_id,
            reason="Mock ERP accepted the idempotent purchase-order request.",
            external_reference=effect.external_reference,
            deterministic_salt=key,
        )
        return effect

    def record_outcome(
        self,
        *,
        occurred_at: datetime,
        outcome_snapshot: DataSnapshotRef,
        outcome_metrics: tuple[OutcomeMetric, ...],
        correlation_id: str,
        actor_id: str = "outcome-observer",
        reason: str = "Delayed actuals linked to the executed recommendation.",
    ) -> LifecycleProjection:
        projection = self.projection
        if projection.execution_reference is None:
            raise IllegalTransitionError("cannot record an outcome before successful execution")
        return self._append(
            event_type=LifecycleEventType.OUTCOME_OBSERVED,
            occurred_at=occurred_at,
            actor_id=actor_id,
            actor_role=ActorRole.SYSTEM,
            correlation_id=correlation_id,
            reason=reason,
            external_reference=projection.execution_reference,
            outcome_snapshot=outcome_snapshot,
            outcome_metrics=outcome_metrics,
        )

    def _append(
        self,
        *,
        event_type: LifecycleEventType,
        occurred_at: datetime,
        actor_id: str,
        actor_role: ActorRole,
        correlation_id: str,
        reason: str,
        approval_expires_at: datetime | None = None,
        external_reference: str | None = None,
        outcome_snapshot: DataSnapshotRef | None = None,
        outcome_metrics: tuple[OutcomeMetric, ...] = (),
        deterministic_salt: str = "",
    ) -> LifecycleProjection:
        sequence = self.projection.last_sequence + 1
        event_id = _event_id(
            self.manifest,
            sequence=sequence,
            event_type=event_type,
            actor_id=actor_id,
            occurred_at=occurred_at,
            salt=deterministic_salt,
        )
        event = LifecycleEvent(
            event_id=event_id,
            sequence=sequence,
            event_type=event_type,
            manifest_id=self.manifest.manifest_id,
            manifest_fingerprint=self.manifest.fingerprint,
            occurred_at=occurred_at,
            actor_id=actor_id,
            actor_role=actor_role,
            correlation_id=correlation_id,
            reason=reason,
            approval_expires_at=approval_expires_at,
            external_reference=external_reference,
            outcome_snapshot=outcome_snapshot,
            outcome_metrics=outcome_metrics,
        )
        if event_type is LifecycleEventType.APPROVAL_GRANTED:
            self.approval_policy.assert_valid_approval(self.manifest, event)
        elif event_type is LifecycleEventType.EXECUTION_REQUESTED:
            approval = self.projection.approval
            if approval is None:
                raise IllegalTransitionError("execution requires a recorded approval")
            self.approval_policy.assert_execution_authorized(
                self.manifest,
                approval,
                event,
                now=occurred_at,
            )
        return self.ledger.append(
            self.manifest,
            event,
            approval_policy=self.approval_policy,
        )


def _event_id(
    manifest: RecommendationManifest,
    *,
    sequence: int,
    event_type: LifecycleEventType,
    actor_id: str,
    occurred_at: datetime,
    salt: str,
) -> UUID:
    identity = (
        f"nexus:lifecycle:v1:{manifest.manifest_id}:{manifest.fingerprint}:"
        f"{sequence}:{event_type.value}:{actor_id}:"
        f"{occurred_at.astimezone(UTC).isoformat()}:{salt}"
    )
    return uuid5(NAMESPACE_URL, identity)
