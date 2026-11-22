from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from nexus.contracts import (
    ActorRole,
    ArtifactRef,
    DataSnapshotRef,
    LifecycleEvent,
    LifecycleEventType,
    OutcomeMetric,
    RecommendationLine,
    RecommendationManifest,
)
from nexus.policies import AuthorizationError
from nexus.workflow import (
    DecisionState,
    IllegalTransitionError,
    InMemoryLifecycleLedger,
    JsonlLifecycleLedger,
    LedgerCorruptionError,
    ManifestBindingError,
    MockErpExecutor,
    MockErpStateError,
    RecommendationWorkflow,
    execution_idempotency_key,
    replay_lifecycle,
)

DECISION_TIME = datetime(2026, 7, 29, 12, tzinfo=UTC)
CORRELATION_ID = "r1-thin-slice"


def manifest(*, manifest_id: UUID | None = None) -> RecommendationManifest:
    return RecommendationManifest(
        manifest_id=manifest_id or UUID("22222222-2222-4222-8222-222222222222"),
        decision_key="ARG-2026-W31",
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
                row_count=2,
            ),
        ),
        forecast_artifact=ArtifactRef(
            name="seasonal-naive",
            version="1.0.0",
            content_sha256="b" * 64,
        ),
        optimizer_artifact=ArtifactRef(
            name="base-stock-policy",
            version="1.0.0",
            content_sha256="c" * 64,
        ),
        policy_artifact=ArtifactRef(
            name="replenishment-guardrails",
            version="1.0.0",
            content_sha256="d" * 64,
        ),
        scenario_seed=290729,
        recommendations=(
            RecommendationLine(
                sku_id="SKU-001",
                location_id="DC-ARG-01",
                recommended_quantity=24,
                horizon_days=7,
                reason_code="below_base_stock",
            ),
        ),
    )


def workflow(
    recommendation: RecommendationManifest,
    *,
    ledger: InMemoryLifecycleLedger | JsonlLifecycleLedger | None = None,
    executor: MockErpExecutor | None = None,
) -> RecommendationWorkflow:
    return RecommendationWorkflow(
        recommendation,
        ledger=ledger or InMemoryLifecycleLedger(),
        executor=executor or MockErpExecutor(),
    )


def submit_and_approve(flow: RecommendationWorkflow) -> None:
    submitted = flow.submit(
        occurred_at=DECISION_TIME + timedelta(minutes=1),
        actor_id="planner.alba",
        correlation_id=CORRELATION_ID,
    )
    assert submitted.state is DecisionState.PENDING_APPROVAL

    approved = flow.approve(
        occurred_at=DECISION_TIME + timedelta(minutes=2),
        approval_expires_at=DECISION_TIME + timedelta(hours=2),
        actor_id="approver.benicio",
        correlation_id=CORRELATION_ID,
    )
    assert approved.state is DecisionState.APPROVED


def test_illegal_transition_fails_closed_without_mutating_ledger() -> None:
    recommendation = manifest()
    ledger = InMemoryLifecycleLedger()
    flow = workflow(recommendation, ledger=ledger)

    with pytest.raises(IllegalTransitionError, match="not legal"):
        flow.approve(
            occurred_at=DECISION_TIME + timedelta(minutes=2),
            approval_expires_at=DECISION_TIME + timedelta(hours=2),
            actor_id="approver.benicio",
            correlation_id=CORRELATION_ID,
        )

    assert ledger.project(recommendation).state is DecisionState.CREATED
    assert ledger.events_for(recommendation) == ()


@pytest.mark.parametrize(
    ("actor_id", "actor_role", "message"),
    [
        ("planner.alba", ActorRole.APPROVER, "maker-checker"),
        ("auditor.cecilia", ActorRole.AUDITOR, "cannot"),
    ],
)
def test_self_or_wrong_role_approval_is_rejected(
    actor_id: str,
    actor_role: ActorRole,
    message: str,
) -> None:
    recommendation = manifest()
    flow = workflow(recommendation)
    flow.submit(
        occurred_at=DECISION_TIME + timedelta(minutes=1),
        actor_id="planner.alba",
        correlation_id=CORRELATION_ID,
    )

    with pytest.raises(AuthorizationError, match=message):
        flow.approve(
            occurred_at=DECISION_TIME + timedelta(minutes=2),
            approval_expires_at=DECISION_TIME + timedelta(hours=2),
            actor_id=actor_id,
            actor_role=actor_role,
            correlation_id=CORRELATION_ID,
        )

    assert flow.projection.state is DecisionState.PENDING_APPROVAL
    assert flow.projection.last_sequence == 1


def test_duplicate_execution_has_one_effect_and_stable_reference() -> None:
    recommendation = manifest()
    executor = MockErpExecutor()
    flow = workflow(recommendation, executor=executor)
    submit_and_approve(flow)
    key = execution_idempotency_key(recommendation)
    execution_time = DECISION_TIME + timedelta(minutes=3)

    first = flow.execute(
        occurred_at=execution_time,
        correlation_id=CORRELATION_ID,
        idempotency_key=key,
    )
    second = flow.execute(
        occurred_at=execution_time + timedelta(seconds=30),
        correlation_id=CORRELATION_ID,
        idempotency_key=key,
    )

    assert first == second
    assert first.external_reference.startswith("MOCK-PO-")
    assert first.total_quantity == 24
    assert executor.effect_count == 1
    assert flow.projection.state is DecisionState.EXECUTED
    assert flow.projection.last_sequence == 4
    assert [event.event_type for event in flow.projection.events] == [
        LifecycleEventType.SUBMITTED_FOR_APPROVAL,
        LifecycleEventType.APPROVAL_GRANTED,
        LifecycleEventType.EXECUTION_REQUESTED,
        LifecycleEventType.EXECUTION_SUCCEEDED,
    ]


def test_duplicate_execution_has_one_durable_effect_after_restart(
    tmp_path: Path,
) -> None:
    recommendation = manifest()
    ledger = JsonlLifecycleLedger(tmp_path / "lifecycle.jsonl")
    state_path = tmp_path / "mock-erp-effects.json"
    first_executor = MockErpExecutor(state_path)
    first_flow = workflow(
        recommendation,
        ledger=ledger,
        executor=first_executor,
    )
    submit_and_approve(first_flow)
    first_effect = first_flow.execute(
        occurred_at=DECISION_TIME + timedelta(minutes=3),
        correlation_id=CORRELATION_ID,
    )

    restarted_executor = MockErpExecutor(state_path)
    restarted_flow = workflow(
        recommendation,
        ledger=JsonlLifecycleLedger(tmp_path / "lifecycle.jsonl"),
        executor=restarted_executor,
    )
    retry_effect = restarted_flow.execute(
        occurred_at=DECISION_TIME + timedelta(minutes=4),
        correlation_id=CORRELATION_ID,
    )

    assert retry_effect == first_effect
    assert first_executor.effect_count == 1
    assert restarted_executor.effect_count == 1
    assert restarted_flow.projection.last_sequence == 4


def test_corrupt_durable_mock_erp_state_fails_closed(tmp_path: Path) -> None:
    state_path = tmp_path / "mock-erp-effects.json"
    state_path.write_text('{"version":"1.0","effects":[{}]}\n', encoding="utf-8")

    with pytest.raises(MockErpStateError, match="effect 1"):
        MockErpExecutor(state_path)


def test_tampered_durable_effect_fails_manifest_reconciliation(
    tmp_path: Path,
) -> None:
    recommendation = manifest()
    state_path = tmp_path / "mock-erp-effects.json"
    executor = MockErpExecutor(state_path)
    key = execution_idempotency_key(recommendation)
    executor.execute(recommendation, idempotency_key=key)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["effects"][0]["total_quantity"] += 1
    state_path.write_text(json.dumps(state), encoding="utf-8")
    restarted = MockErpExecutor(state_path)

    with pytest.raises(MockErpStateError, match="disagrees"):
        restarted.execute(recommendation, idempotency_key=key)


def test_nonstable_execution_key_fails_before_external_effect() -> None:
    recommendation = manifest()
    executor = MockErpExecutor()
    flow = workflow(recommendation, executor=executor)
    submit_and_approve(flow)

    with pytest.raises(ManifestBindingError, match="stable key"):
        flow.execute(
            occurred_at=DECISION_TIME + timedelta(minutes=3),
            correlation_id=CORRELATION_ID,
            idempotency_key="caller-chosen-key",
        )

    assert executor.effect_count == 0
    assert flow.projection.state is DecisionState.APPROVED
    assert flow.projection.last_sequence == 2


def test_jsonl_replay_reconstructs_identical_state(tmp_path: Path) -> None:
    recommendation = manifest()
    ledger_path = tmp_path / "workflow" / "lifecycle.jsonl"
    ledger = JsonlLifecycleLedger(ledger_path)
    flow = workflow(recommendation, ledger=ledger)
    submit_and_approve(flow)
    flow.execute(
        occurred_at=DECISION_TIME + timedelta(minutes=3),
        correlation_id=CORRELATION_ID,
    )
    original = flow.projection

    reopened = JsonlLifecycleLedger(ledger_path)
    replayed = reopened.project(recommendation)
    direct = replay_lifecycle(recommendation, reopened.events_for(recommendation))

    assert replayed == original
    assert direct == original
    assert len(ledger_path.read_text(encoding="utf-8").splitlines()) == 4
    assert list(ledger_path.parent.glob("*.tmp")) == []


def test_manifest_fingerprint_mismatch_is_rejected_before_append() -> None:
    recommendation = manifest()
    ledger = InMemoryLifecycleLedger()
    mismatched = LifecycleEvent(
        event_id=uuid4(),
        sequence=1,
        event_type=LifecycleEventType.SUBMITTED_FOR_APPROVAL,
        manifest_id=recommendation.manifest_id,
        manifest_fingerprint="f" * 64,
        occurred_at=DECISION_TIME + timedelta(minutes=1),
        actor_id="planner.alba",
        actor_role=ActorRole.PLANNER,
        correlation_id=CORRELATION_ID,
        reason="Attempted submission with a stale fingerprint.",
    )

    with pytest.raises(ManifestBindingError, match="fingerprint"):
        ledger.append(recommendation, mismatched)

    assert ledger.events_for(recommendation) == ()


def test_delayed_outcome_links_exact_decision_and_execution() -> None:
    recommendation = manifest()
    flow = workflow(recommendation)
    submit_and_approve(flow)
    effect = flow.execute(
        occurred_at=DECISION_TIME + timedelta(minutes=3),
        correlation_id=CORRELATION_ID,
    )
    observation_time = DECISION_TIME + timedelta(days=7)
    outcome_snapshot = DataSnapshotRef(
        dataset="gold.demand_actuals",
        snapshot_id="actuals-2026-w31",
        schema_version="1.0.0",
        as_of=observation_time,
        content_sha256="e" * 64,
        row_count=1,
    )

    projection = flow.record_outcome(
        occurred_at=observation_time,
        outcome_snapshot=outcome_snapshot,
        outcome_metrics=(
            OutcomeMetric(name="actual_demand", value=79.0, unit="units"),
            OutcomeMetric(name="forecast_demand", value=77.0, unit="units"),
        ),
        correlation_id=CORRELATION_ID,
    )

    assert projection.state is DecisionState.OUTCOME_OBSERVED
    assert projection.outcome is not None
    assert projection.outcome.manifest_id == recommendation.manifest_id
    assert projection.outcome.manifest_fingerprint == recommendation.fingerprint
    assert projection.outcome.external_reference == effect.external_reference
    assert projection.outcome.outcome_snapshot == outcome_snapshot
    assert projection.last_sequence == 5


def test_outcome_with_wrong_execution_reference_is_rejected() -> None:
    recommendation = manifest()
    ledger = InMemoryLifecycleLedger()
    flow = workflow(recommendation, ledger=ledger)
    submit_and_approve(flow)
    flow.execute(
        occurred_at=DECISION_TIME + timedelta(minutes=3),
        correlation_id=CORRELATION_ID,
    )
    wrong_outcome = LifecycleEvent(
        event_id=uuid4(),
        sequence=5,
        event_type=LifecycleEventType.OUTCOME_OBSERVED,
        manifest_id=recommendation.manifest_id,
        manifest_fingerprint=recommendation.fingerprint,
        occurred_at=DECISION_TIME + timedelta(days=7),
        actor_id="outcome-observer",
        actor_role=ActorRole.SYSTEM,
        correlation_id=CORRELATION_ID,
        reason="Outcome with a reference that cannot be reconciled.",
        external_reference="MOCK-PO-WRONG",
        outcome_snapshot=DataSnapshotRef(
            dataset="gold.demand_actuals",
            snapshot_id="actuals-2026-w31",
            schema_version="1.0.0",
            as_of=DECISION_TIME + timedelta(days=7),
            content_sha256="e" * 64,
            row_count=1,
        ),
        outcome_metrics=(OutcomeMetric(name="actual_demand", value=79.0, unit="units"),),
    )

    with pytest.raises(ManifestBindingError, match="external_reference"):
        ledger.append(recommendation, wrong_outcome)

    assert ledger.project(recommendation).state is DecisionState.EXECUTED
    assert ledger.project(recommendation).last_sequence == 4


def test_submission_cannot_predate_manifest_creation() -> None:
    recommendation = manifest()
    flow = workflow(recommendation)

    with pytest.raises(AuthorizationError, match="manifest creation"):
        flow.submit(
            occurred_at=DECISION_TIME + timedelta(seconds=1),
            actor_id="planner.alba",
            correlation_id=CORRELATION_ID,
        )

    assert flow.projection.state is DecisionState.CREATED


def test_outcome_snapshot_cannot_come_from_after_observation() -> None:
    recommendation = manifest()
    ledger = InMemoryLifecycleLedger()
    flow = workflow(recommendation, ledger=ledger)
    submit_and_approve(flow)
    effect = flow.execute(
        occurred_at=DECISION_TIME + timedelta(minutes=3),
        correlation_id=CORRELATION_ID,
    )
    observation_time = DECISION_TIME + timedelta(days=7)
    future_snapshot = DataSnapshotRef(
        dataset="gold.demand_actuals",
        snapshot_id="future-actuals",
        schema_version="1.0.0",
        as_of=observation_time + timedelta(microseconds=1),
        content_sha256="e" * 64,
        row_count=1,
    )
    event = LifecycleEvent(
        event_id=uuid4(),
        sequence=5,
        event_type=LifecycleEventType.OUTCOME_OBSERVED,
        manifest_id=recommendation.manifest_id,
        manifest_fingerprint=recommendation.fingerprint,
        occurred_at=observation_time,
        actor_id="outcome-observer",
        actor_role=ActorRole.SYSTEM,
        correlation_id=CORRELATION_ID,
        reason="Attempted outcome using a future snapshot.",
        external_reference=effect.external_reference,
        outcome_snapshot=future_snapshot,
        outcome_metrics=(OutcomeMetric(name="actual_demand", value=79.0, unit="units"),),
    )

    with pytest.raises(IllegalTransitionError, match="between execution"):
        ledger.append(recommendation, event)

    assert ledger.project(recommendation).state is DecisionState.EXECUTED


def test_corrupt_jsonl_fails_closed(tmp_path: Path) -> None:
    recommendation = manifest()
    ledger_path = tmp_path / "lifecycle.jsonl"
    ledger_path.write_text('{"not":"a lifecycle event"}\n', encoding="utf-8")

    with pytest.raises(LedgerCorruptionError, match="line 1"):
        JsonlLifecycleLedger(ledger_path).project(recommendation)


def test_idempotency_key_cannot_be_reused_for_another_manifest() -> None:
    first = manifest()
    second = manifest(manifest_id=UUID("33333333-3333-4333-8333-333333333333"))
    executor = MockErpExecutor()
    key = "shared-but-invalid-key"
    executor.execute(first, idempotency_key=key)

    with pytest.raises(ManifestBindingError, match="different manifest"):
        executor.execute(second, idempotency_key=key)

    assert executor.effect_count == 1


def test_equivalent_event_instants_produce_the_same_event_id() -> None:
    recommendation = manifest()
    utc_flow = workflow(recommendation)
    offset_flow = workflow(recommendation)
    instant = DECISION_TIME + timedelta(minutes=1)

    utc_event = utc_flow.submit(
        occurred_at=instant,
        actor_id="planner.alba",
        correlation_id=CORRELATION_ID,
    ).events[0]
    offset_event = offset_flow.submit(
        occurred_at=instant.astimezone(timezone(timedelta(hours=-3))),
        actor_id="planner.alba",
        correlation_id=CORRELATION_ID,
    ).events[0]

    assert offset_event.event_id == utc_event.event_id


def test_custom_approval_policy_is_used_during_ledger_replay() -> None:
    from nexus.policies import ApprovalPolicy

    recommendation = manifest()
    policy = ApprovalPolicy(maximum_ttl_seconds=28_800)
    flow = RecommendationWorkflow(
        recommendation,
        ledger=InMemoryLifecycleLedger(),
        executor=MockErpExecutor(),
        approval_policy=policy,
    )
    flow.submit(
        occurred_at=DECISION_TIME + timedelta(minutes=1),
        actor_id="planner.alba",
        correlation_id=CORRELATION_ID,
    )

    approved = flow.approve(
        occurred_at=DECISION_TIME + timedelta(minutes=2),
        approval_expires_at=DECISION_TIME + timedelta(hours=6),
        actor_id="approver.benicio",
        correlation_id=CORRELATION_ID,
    )

    assert approved.state is DecisionState.APPROVED
