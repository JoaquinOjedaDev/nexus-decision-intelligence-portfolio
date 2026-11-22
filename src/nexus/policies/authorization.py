"""Human authorization and maker-checker rules for governed decisions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType

from pydantic import BaseModel, ConfigDict, PositiveInt

from nexus.contracts import (
    ActorRole,
    LifecycleEvent,
    LifecycleEventType,
    RecommendationManifest,
)


class Permission(StrEnum):
    CREATE_RECOMMENDATION = "create_recommendation"
    SUBMIT_FOR_APPROVAL = "submit_for_approval"
    APPROVE = "approve"
    REJECT = "reject"
    REQUEST_EXECUTION = "request_execution"
    EXECUTE = "execute"
    VIEW_AUDIT = "view_audit"


PERMISSION_MATRIX = MappingProxyType(
    {
        ActorRole.PLANNER: frozenset(
            {
                Permission.CREATE_RECOMMENDATION,
                Permission.SUBMIT_FOR_APPROVAL,
                Permission.VIEW_AUDIT,
            }
        ),
        ActorRole.APPROVER: frozenset(
            {
                Permission.APPROVE,
                Permission.REJECT,
                Permission.REQUEST_EXECUTION,
                Permission.VIEW_AUDIT,
            }
        ),
        ActorRole.AUDITOR: frozenset({Permission.VIEW_AUDIT}),
        ActorRole.SYSTEM: frozenset({Permission.EXECUTE}),
    }
)


class AuthorizationError(ValueError):
    """Raised when an action violates a role or approval invariant."""


def permissions_for(role: ActorRole) -> frozenset[Permission]:
    return PERMISSION_MATRIX[role]


def assert_permission(role: ActorRole, permission: Permission) -> None:
    if permission not in permissions_for(role):
        raise AuthorizationError(f"role {role.value!r} cannot {permission.value!r}")


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise AuthorizationError(f"{field_name} must include an explicit timezone offset")
    return value.astimezone(UTC)


class ApprovalPolicy(BaseModel):
    """Rules that bind a short-lived approval to one immutable manifest."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)

    maximum_ttl_seconds: PositiveInt = 14_400

    def assert_valid_approval(
        self,
        manifest: RecommendationManifest,
        approval: LifecycleEvent,
    ) -> None:
        if approval.event_type is not LifecycleEventType.APPROVAL_GRANTED:
            raise AuthorizationError("an approval_granted lifecycle event is required")
        assert_permission(approval.actor_role, Permission.APPROVE)
        if approval.actor_id == manifest.created_by:
            raise AuthorizationError("maker-checker violation: the maker cannot approve")
        self._assert_manifest_binding(manifest, approval)
        if approval.occurred_at < manifest.decision_time:
            raise AuthorizationError("approval cannot precede the decision time")
        if approval.occurred_at >= manifest.valid_until:
            raise AuthorizationError("approval was granted after the recommendation expired")
        if approval.approval_expires_at is None:  # guarded by the contract; defensive here
            raise AuthorizationError("approval is missing its expiry")
        maximum_expiry = min(
            approval.occurred_at + timedelta(seconds=self.maximum_ttl_seconds),
            manifest.valid_until,
        )
        if approval.approval_expires_at > maximum_expiry:
            raise AuthorizationError("approval expiry exceeds policy or manifest validity")

    def assert_execution_authorized(
        self,
        manifest: RecommendationManifest,
        approval: LifecycleEvent,
        execution: LifecycleEvent,
        *,
        now: datetime,
    ) -> None:
        """Require an exact, unexpired binding before a system execution."""

        self.assert_valid_approval(manifest, approval)
        instant = _aware_utc(now, "now")
        if execution.event_type is not LifecycleEventType.EXECUTION_REQUESTED:
            raise AuthorizationError("an execution_requested lifecycle event is required")
        assert_permission(execution.actor_role, Permission.EXECUTE)
        self._assert_manifest_binding(manifest, execution)
        if execution.sequence <= approval.sequence:
            raise AuthorizationError("execution must follow approval in the append-only ledger")
        if execution.occurred_at < approval.occurred_at:
            raise AuthorizationError("execution cannot precede approval")
        if approval.approval_expires_at is None or instant >= approval.approval_expires_at:
            raise AuthorizationError("approval has expired")
        if instant >= manifest.valid_until:
            raise AuthorizationError("recommendation has expired")
        if execution.occurred_at > instant:
            raise AuthorizationError("execution event cannot be in the future")

    @staticmethod
    def _assert_manifest_binding(
        manifest: RecommendationManifest,
        event: LifecycleEvent,
    ) -> None:
        if event.manifest_id != manifest.manifest_id:
            raise AuthorizationError("lifecycle event is bound to a different manifest_id")
        if event.manifest_fingerprint != manifest.fingerprint:
            raise AuthorizationError("lifecycle event fingerprint does not match the manifest")
