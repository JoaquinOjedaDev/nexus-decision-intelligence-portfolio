from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from nexus.contracts import (
    ActorRole,
    ArtifactRef,
    DataSnapshotRef,
    EventEnvelope,
    LifecycleEvent,
    LifecycleEventType,
    RecommendationLine,
    RecommendationManifest,
)
from nexus.policies import (
    ApprovalPolicy,
    AuthorizationError,
    Permission,
    ReplanDisposition,
    ReplanPolicy,
    permissions_for,
)
from nexus.policies.authorization import PERMISSION_MATRIX

DECISION_TIME = datetime(2026, 7, 18, 12, tzinfo=UTC)


def manifest() -> RecommendationManifest:
    return RecommendationManifest(
        manifest_id=UUID("22222222-2222-4222-8222-222222222222"),
        decision_key="ARG-2026-W29",
        decision_time=DECISION_TIME,
        valid_until=DECISION_TIME + timedelta(days=1),
        created_at=DECISION_TIME + timedelta(seconds=5),
        created_by="planner.alba",
        input_snapshots=(
            DataSnapshotRef(
                dataset="gold.inventory_position",
                snapshot_id="inventory-r1",
                schema_version="1.0.0",
                as_of=DECISION_TIME - timedelta(minutes=5),
                content_sha256="a" * 64,
                row_count=100,
            ),
        ),
        forecast_artifact=ArtifactRef(
            name="seasonal-naive", version="1.0.0", content_sha256="b" * 64
        ),
        optimizer_artifact=ArtifactRef(
            name="base-stock-policy", version="1.0.0", content_sha256="c" * 64
        ),
        policy_artifact=ArtifactRef(
            name="replenishment-guardrails", version="1.0.0", content_sha256="d" * 64
        ),
        scenario_seed=290718,
        recommendations=(
            RecommendationLine(
                sku_id="SKU-001",
                location_id="DC-ARG-01",
                recommended_quantity=120,
                horizon_days=7,
                reason_code="below_base_stock",
            ),
        ),
    )


def approval_event(
    recommendation: RecommendationManifest,
    *,
    actor_id: str = "approver.benicio",
    approved_at: datetime = DECISION_TIME + timedelta(minutes=15),
    expires_at: datetime = DECISION_TIME + timedelta(hours=4),
    fingerprint: str | None = None,
) -> LifecycleEvent:
    return LifecycleEvent(
        event_id=uuid4(),
        sequence=2,
        event_type=LifecycleEventType.APPROVAL_GRANTED,
        manifest_id=recommendation.manifest_id,
        manifest_fingerprint=fingerprint or recommendation.fingerprint,
        occurred_at=approved_at,
        actor_id=actor_id,
        actor_role=ActorRole.APPROVER,
        correlation_id="replan-2026w29",
        reason="Within delegated authority.",
        approval_expires_at=expires_at,
    )


def execution_event(
    recommendation: RecommendationManifest,
    *,
    occurred_at: datetime = DECISION_TIME + timedelta(minutes=16),
) -> LifecycleEvent:
    return LifecycleEvent(
        event_id=uuid4(),
        sequence=3,
        event_type=LifecycleEventType.EXECUTION_REQUESTED,
        manifest_id=recommendation.manifest_id,
        manifest_fingerprint=recommendation.fingerprint,
        occurred_at=occurred_at,
        actor_id="mock-erp-adapter",
        actor_role=ActorRole.SYSTEM,
        correlation_id="replan-2026w29",
        reason="Approved recommendation queued for idempotent execution.",
    )


def inventory_event(
    *, occurred_at: datetime, event_type: str = "inventory.adjusted"
) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type=event_type,
        source="wms.demo",
        subject_id="SKU-001:DC-ARG-01",
        occurred_at=occurred_at,
        ingested_at=occurred_at,
        idempotency_key=f"event-{uuid4()}",
        payload={"quantity_delta": -12},
    )


def test_permission_matrix_keeps_auditor_read_only() -> None:
    assert permissions_for(ActorRole.AUDITOR) == frozenset({Permission.VIEW_AUDIT})
    assert Permission.APPROVE in permissions_for(ActorRole.APPROVER)
    assert Permission.APPROVE not in permissions_for(ActorRole.PLANNER)
    assert next(iter(permissions_for(ActorRole.SYSTEM))) == Permission.EXECUTE


def test_ai_adapter_has_no_authoritative_actor_role() -> None:
    with pytest.raises(ValueError, match="ai"):
        ActorRole("ai")

    assert set(PERMISSION_MATRIX) == set(ActorRole)


def test_maker_checker_rejects_self_approval() -> None:
    recommendation = manifest()
    approval = approval_event(recommendation, actor_id=recommendation.created_by)

    with pytest.raises(AuthorizationError, match="maker-checker"):
        ApprovalPolicy().assert_valid_approval(recommendation, approval)


def test_approval_is_bound_to_exact_manifest_fingerprint() -> None:
    recommendation = manifest()
    approval = approval_event(recommendation, fingerprint="f" * 64)

    with pytest.raises(AuthorizationError, match="fingerprint"):
        ApprovalPolicy().assert_valid_approval(recommendation, approval)


def test_expired_approval_cannot_authorize_execution() -> None:
    recommendation = manifest()
    approval = approval_event(
        recommendation,
        expires_at=DECISION_TIME + timedelta(hours=1),
    )
    execution = execution_event(recommendation, occurred_at=DECISION_TIME + timedelta(minutes=30))

    with pytest.raises(AuthorizationError, match="approval has expired"):
        ApprovalPolicy().assert_execution_authorized(
            recommendation,
            approval,
            execution,
            now=DECISION_TIME + timedelta(hours=1),
        )


def test_valid_approval_authorizes_exactly_bound_execution() -> None:
    recommendation = manifest()
    approval = approval_event(recommendation)
    execution = execution_event(recommendation)

    ApprovalPolicy().assert_execution_authorized(
        recommendation,
        approval,
        execution,
        now=DECISION_TIME + timedelta(minutes=17),
    )


@given(elapsed_seconds=st.integers(min_value=0, max_value=1_799))
def test_material_replan_during_cooldown_is_deferred_and_invalidates_stale_advice(
    elapsed_seconds: int,
) -> None:
    policy = ReplanPolicy(cooldown_seconds=1_800, stale_after_seconds=21_600)
    recommendation = manifest()
    last_replan = DECISION_TIME
    now = last_replan + timedelta(seconds=elapsed_seconds)
    event = inventory_event(occurred_at=now)

    result = policy.evaluate(
        event,
        now=now,
        last_replan_at=last_replan,
        active_manifest=recommendation,
    )

    assert result.disposition is ReplanDisposition.DEFER_COOLDOWN
    assert result.eligible_at == DECISION_TIME + timedelta(seconds=1_800)
    assert result.invalidate_active_manifest is (elapsed_seconds > 0)


def test_material_replan_triggers_at_cooldown_boundary() -> None:
    policy = ReplanPolicy(cooldown_seconds=1_800)
    boundary = DECISION_TIME + timedelta(seconds=1_800)
    result = policy.evaluate(
        inventory_event(occurred_at=boundary),
        now=boundary,
        last_replan_at=DECISION_TIME,
        active_manifest=manifest(),
    )

    assert result.disposition is ReplanDisposition.TRIGGER_NOW
    assert result.invalidate_active_manifest is True


def test_non_material_event_does_not_trigger_replan() -> None:
    now = DECISION_TIME + timedelta(minutes=5)
    result = ReplanPolicy().evaluate(
        inventory_event(occurred_at=now, event_type="shipment.scanned"),
        now=now,
        active_manifest=manifest(),
    )

    assert result.disposition is ReplanDisposition.IGNORE
    assert result.material is False
    assert result.invalidate_active_manifest is False


def test_age_limit_invalidates_even_for_non_material_event() -> None:
    now = DECISION_TIME + timedelta(hours=7)
    result = ReplanPolicy(stale_after_seconds=21_600).evaluate(
        inventory_event(occurred_at=now, event_type="shipment.scanned"),
        now=now,
        active_manifest=manifest(),
    )

    assert result.disposition is ReplanDisposition.INVALIDATE_ONLY
    assert result.invalidate_active_manifest is True
