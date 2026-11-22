"""Executable R1 thin slice from governed inputs to measured outcome."""

from __future__ import annotations

import hashlib
import json
import time as clock
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from nexus.contracts import (
    ArtifactRef,
    DataSnapshotRef,
    OutcomeMetric,
    RecommendationManifest,
)
from nexus.data import QuarantineReason, R1DataPathResult, run_r1_data_path
from nexus.decisioning import BaseStockRecommendation, recommend_base_stock
from nexus.modeling import (
    DailyDemandObservation,
    SeasonalNaiveForecast,
    forecast_seasonal_naive,
)
from nexus.workflow import (
    DecisionState,
    JsonlLifecycleLedger,
    LifecycleProjection,
    MockErpEffect,
    MockErpExecutor,
    RecommendationWorkflow,
)

ISSUE_TIME = datetime(2026, 7, 15, 9, tzinfo=UTC)
OUTCOME_TIME = ISSUE_TIME + timedelta(days=7)
SKU_ID = "SKU-001"
LOCATION_ID = "DC-ARG-01"
ACTUAL_DEMAND = 79
CORRELATION_ID = "r1-thin-slice"
_SCHEMA_VERSION = "1.0.0"
_EXPECTED_FORECAST = (11, 13, 9, 12, 14, 10, 8)


@dataclass(frozen=True, slots=True)
class R1DemoResult:
    """Typed result returned by the complete R1 demonstration."""

    data: R1DataPathResult
    forecast: SeasonalNaiveForecast
    recommendation: BaseStockRecommendation
    manifest: RecommendationManifest
    projection: LifecycleProjection
    effect: MockErpEffect
    actual_demand: int
    absolute_demand_error: int
    effect_count: int
    duration_seconds: float
    summary_path: Path


@dataclass(frozen=True, slots=True)
class R1VerificationResult:
    """One independently readable R1 gate result."""

    name: str
    passed: bool
    detail: str


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _select_decision_inputs(
    data: R1DataPathResult,
) -> tuple[tuple[DailyDemandObservation, ...], int, int, int, int]:
    products = [
        row
        for row in data.product_locations
        if row.active and row.sku_id == SKU_ID and row.location_id == LOCATION_ID
    ]
    inventories = [
        row
        for row in data.gold_inventory
        if row.sku_id == SKU_ID and row.location_id == LOCATION_ID
    ]
    if len(products) != 1 or len(inventories) != 1:
        raise ValueError("R1 requires exactly one active product and one Gold inventory position")

    observations = tuple(
        DailyDemandObservation(
            sku_id=row.sku_id,
            location_id=row.location_id,
            demand_date=row.demand_date,
            quantity=row.demand_qty,
            available_at=row.available_at,
        )
        for row in data.demand_for(SKU_ID, LOCATION_ID)
    )
    product = products[0]
    inventory = inventories[0]
    return (
        observations,
        product.lead_time_days,
        product.safety_stock_qty,
        inventory.on_hand_qty,
        inventory.on_order_qty,
    )


def _snapshot_ref(
    data: R1DataPathResult,
    *,
    dataset: str,
) -> DataSnapshotRef:
    artifact = data.artifact(dataset)
    return DataSnapshotRef(
        dataset=dataset,
        snapshot_id=f"sha256-{artifact.content_sha256[:20]}",
        schema_version=_SCHEMA_VERSION,
        as_of=data.as_of,
        content_sha256=artifact.content_sha256,
        row_count=artifact.row_count,
        partition_keys=("sku_id", "location_id"),
    )


def _build_manifest(
    data: R1DataPathResult,
    forecast: SeasonalNaiveForecast,
    recommendation: BaseStockRecommendation,
) -> RecommendationManifest:
    demand_snapshot = _snapshot_ref(data, dataset="gold.demand_history")
    inventory_snapshot = _snapshot_ref(data, dataset="gold.inventory_position")
    decision_key = f"{SKU_ID}:{LOCATION_ID}:{ISSUE_TIME.date().isoformat()}"
    manifest_identity = ":".join(
        (
            "nexus-r1",
            decision_key,
            demand_snapshot.content_sha256,
            inventory_snapshot.content_sha256,
            forecast.forecast_sha256,
            recommendation.decision_sha256,
        )
    )
    policy_sha256 = _canonical_sha256(
        {
            "approval": "human_required",
            "maker_checker": True,
            "maximum_approval_ttl_seconds": 14_400,
            "version": _SCHEMA_VERSION,
        }
    )
    return RecommendationManifest(
        manifest_id=uuid5(NAMESPACE_URL, manifest_identity),
        decision_key=decision_key,
        decision_time=ISSUE_TIME,
        valid_until=ISSUE_TIME + timedelta(hours=4),
        created_at=ISSUE_TIME,
        created_by="planner.alba",
        input_snapshots=(demand_snapshot, inventory_snapshot),
        forecast_artifact=ArtifactRef(
            name=forecast.model_name,
            version=forecast.model_version,
            content_sha256=forecast.forecast_sha256,
        ),
        optimizer_artifact=ArtifactRef(
            name=recommendation.method_name,
            version=recommendation.method_version,
            content_sha256=recommendation.decision_sha256,
        ),
        policy_artifact=ArtifactRef(
            name="r1-human-approval",
            version=_SCHEMA_VERSION,
            content_sha256=policy_sha256,
        ),
        scenario_seed=20260715,
        recommendations=(recommendation.to_recommendation_line(),),
        correlation_id=CORRELATION_ID,
        trace_id="trace-r1-demo-20260715",
    )


def _outcome_snapshot() -> DataSnapshotRef:
    actual_content = {
        "actual_demand": ACTUAL_DEMAND,
        "horizon_end": (ISSUE_TIME.date() + timedelta(days=6)).isoformat(),
        "horizon_start": ISSUE_TIME.date().isoformat(),
        "location_id": LOCATION_ID,
        "observed_at": OUTCOME_TIME.isoformat(),
        "sku_id": SKU_ID,
    }
    content_sha256 = _canonical_sha256(actual_content)
    return DataSnapshotRef(
        dataset="gold.demand_actuals",
        snapshot_id=f"sha256-{content_sha256[:20]}",
        schema_version=_SCHEMA_VERSION,
        as_of=OUTCOME_TIME,
        content_sha256=content_sha256,
        row_count=1,
        partition_keys=("sku_id", "location_id"),
    )


def _advance_workflow(
    manifest: RecommendationManifest,
    artifact_dir: Path,
    *,
    forecast_total: int,
) -> tuple[LifecycleProjection, MockErpEffect, int]:
    executor = MockErpExecutor(artifact_dir / "workflow" / "mock-erp-effects.json")
    workflow = RecommendationWorkflow(
        manifest,
        ledger=JsonlLifecycleLedger(artifact_dir / "workflow" / "lifecycle.jsonl"),
        executor=executor,
    )
    projection = workflow.projection

    if projection.state is DecisionState.CREATED:
        projection = workflow.submit(
            occurred_at=ISSUE_TIME + timedelta(minutes=1),
            actor_id=manifest.created_by,
            correlation_id=CORRELATION_ID,
        )
    if projection.state is DecisionState.PENDING_APPROVAL:
        projection = workflow.approve(
            occurred_at=ISSUE_TIME + timedelta(minutes=2),
            approval_expires_at=ISSUE_TIME + timedelta(hours=3),
            actor_id="approver.benicio",
            correlation_id=CORRELATION_ID,
        )

    executable_states = {
        DecisionState.APPROVED,
        DecisionState.EXECUTION_PENDING,
        DecisionState.EXECUTED,
        DecisionState.OUTCOME_OBSERVED,
    }
    if projection.state not in executable_states:
        raise ValueError(f"R1 workflow cannot continue from {projection.state.value}")
    effect = workflow.execute(
        occurred_at=ISSUE_TIME + timedelta(minutes=3),
        correlation_id=CORRELATION_ID,
    )
    retry_effect = workflow.execute(
        occurred_at=ISSUE_TIME + timedelta(minutes=4),
        correlation_id=CORRELATION_ID,
    )
    if retry_effect != effect:
        raise RuntimeError("idempotent execution retry returned a different ERP effect")

    projection = workflow.projection
    absolute_error = abs(ACTUAL_DEMAND - forecast_total)
    if projection.state is DecisionState.EXECUTED:
        projection = workflow.record_outcome(
            occurred_at=OUTCOME_TIME,
            outcome_snapshot=_outcome_snapshot(),
            outcome_metrics=(
                OutcomeMetric(name="actual_demand", value=float(ACTUAL_DEMAND), unit="units"),
                OutcomeMetric(name="forecast_demand", value=float(forecast_total), unit="units"),
                OutcomeMetric(
                    name="absolute_demand_error",
                    value=float(absolute_error),
                    unit="units",
                ),
            ),
            correlation_id=CORRELATION_ID,
        )
    if projection.state is not DecisionState.OUTCOME_OBSERVED:
        raise RuntimeError(f"R1 workflow ended in unexpected state {projection.state.value}")
    return projection, effect, executor.effect_count


def _summary(
    result: R1DemoResult,
    artifact_dir: Path,
) -> dict[str, Any]:
    quarantine_counts = Counter(row.reason_code.value for row in result.data.quarantined)
    return {
        "release": "R1",
        "status": "PASS",
        "decision": {
            "sku_id": SKU_ID,
            "location_id": LOCATION_ID,
            "issue_time": ISSUE_TIME.isoformat(),
            "forecast_values": [str(point.predicted_demand) for point in result.forecast.points],
            "forecast_total": str(result.forecast.total_expected_demand()),
            "on_hand": str(result.recommendation.on_hand),
            "on_order": str(result.recommendation.on_order),
            "safety_stock": str(result.recommendation.safety_stock),
            "recommended_quantity": result.recommendation.recommended_quantity,
            "reason_code": result.recommendation.reason_code.value,
        },
        "data": {
            "gold_demand_rows": len(result.data.gold_demand),
            "accepted_inventory_events": len(result.data.accepted_inventory_adjustments),
            "quarantine_counts": dict(sorted(quarantine_counts.items())),
            "artifacts": [
                {
                    "dataset": artifact.dataset,
                    "path": artifact.path.relative_to(artifact_dir).as_posix(),
                    "content_sha256": artifact.content_sha256,
                    "row_count": artifact.row_count,
                }
                for artifact in result.data.artifacts
            ],
        },
        "governance": {
            "manifest_id": str(result.manifest.manifest_id),
            "manifest_fingerprint": result.manifest.fingerprint,
            "lifecycle_state": result.projection.state.value,
            "lifecycle_events": [event.event_type.value for event in result.projection.events],
            "external_reference": result.effect.external_reference,
            "mock_erp_effect_count_after_retry": result.effect_count,
        },
        "feedback": {
            "actual_demand": result.actual_demand,
            "absolute_demand_error": result.absolute_demand_error,
        },
        "runtime": {
            "duration_seconds": round(result.duration_seconds, 6),
            "ceiling_seconds": 300,
        },
    }


def run_r1_demo(
    *,
    source_dir: Path,
    artifact_dir: Path,
) -> R1DemoResult:
    """Run or safely resume the deterministic R1 closed loop."""

    started = clock.perf_counter()
    source_dir = source_dir.resolve()
    artifact_dir = artifact_dir.resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    data = run_r1_data_path(
        source_dir=source_dir,
        output_dir=artifact_dir / "data",
        as_of=ISSUE_TIME,
    )
    observations, lead_time, safety_stock, on_hand, on_order = _select_decision_inputs(data)
    forecast = forecast_seasonal_naive(
        observations,
        sku_id=SKU_ID,
        location_id=LOCATION_ID,
        issue_time=ISSUE_TIME,
        history_cutoff=ISSUE_TIME,
        horizon_days=lead_time,
        season_length_days=7,
        cold_start_daily_demand=0,
    )
    recommendation = recommend_base_stock(
        forecast,
        lead_time_days=lead_time,
        safety_stock=safety_stock,
        on_hand=on_hand,
        on_order=on_order,
    )
    manifest = _build_manifest(data, forecast, recommendation)
    forecast_total = int(forecast.total_expected_demand())
    projection, effect, effect_count = _advance_workflow(
        manifest,
        artifact_dir,
        forecast_total=forecast_total,
    )

    _write_json(
        artifact_dir / "decision" / "forecast.json",
        {
            "forecast_sha256": forecast.forecast_sha256,
            "input_sha256": forecast.input_sha256,
            "model_name": forecast.model_name,
            "model_version": forecast.model_version,
            "points": [
                {
                    "predicted_demand": str(point.predicted_demand),
                    "source": point.source.value,
                    "source_date": (
                        None if point.source_date is None else point.source_date.isoformat()
                    ),
                    "target_date": point.target_date.isoformat(),
                }
                for point in forecast.points
            ],
        },
    )
    _write_json(
        artifact_dir / "decision" / "recommendation.json",
        {
            "decision_sha256": recommendation.decision_sha256,
            "expected_lead_time_demand": str(recommendation.expected_lead_time_demand),
            "method_name": recommendation.method_name,
            "method_version": recommendation.method_version,
            "on_hand": str(recommendation.on_hand),
            "on_order": str(recommendation.on_order),
            "reason_code": recommendation.reason_code.value,
            "recommended_quantity": recommendation.recommended_quantity,
            "safety_stock": str(recommendation.safety_stock),
        },
    )
    _write_json(
        artifact_dir / "decision" / "manifest.json",
        manifest.model_dump(mode="json"),
    )
    _write_json(
        artifact_dir / "feedback" / "delayed-actual.json",
        {
            "absolute_demand_error": abs(ACTUAL_DEMAND - forecast_total),
            "actual_demand": ACTUAL_DEMAND,
            "external_reference": effect.external_reference,
            "forecast_demand": forecast_total,
            "manifest_fingerprint": manifest.fingerprint,
            "observed_at": OUTCOME_TIME.isoformat(),
        },
    )

    duration_seconds = clock.perf_counter() - started
    summary_path = artifact_dir / "r1-demo-summary.json"
    result = R1DemoResult(
        data=data,
        forecast=forecast,
        recommendation=recommendation,
        manifest=manifest,
        projection=projection,
        effect=effect,
        actual_demand=ACTUAL_DEMAND,
        absolute_demand_error=abs(ACTUAL_DEMAND - forecast_total),
        effect_count=effect_count,
        duration_seconds=duration_seconds,
        summary_path=summary_path,
    )
    _write_json(summary_path, _summary(result, artifact_dir))
    return result


def verify_r1(
    *,
    source_dir: Path,
    artifact_dir: Path,
) -> list[R1VerificationResult]:
    """Run the R1 demo twice and evaluate its executable acceptance contract."""

    try:
        first = run_r1_demo(source_dir=source_dir, artifact_dir=artifact_dir / "run-a")
        second = run_r1_demo(source_dir=source_dir, artifact_dir=artifact_dir / "run-b")
    except Exception as exc:  # pragma: no cover - converted to release-gate evidence
        return [R1VerificationResult("closed-loop execution", False, str(exc))]

    forecast_values = tuple(int(point.predicted_demand) for point in first.forecast.points)
    reasons = Counter(row.reason_code for row in first.data.quarantined)
    results = [
        R1VerificationResult(
            "batch + event data path",
            (
                len(first.data.gold_demand) == 14
                and len(first.data.accepted_inventory_adjustments) == 1
                and first.recommendation.on_hand == 35
                and first.recommendation.on_order == 10
                and reasons[QuarantineReason.DUPLICATE_EVENT_ID] == 1
                and reasons[QuarantineReason.EVENT_CONTRACT_INVALID] == 1
            ),
            (
                f"demand={len(first.data.gold_demand)}, accepted_events="
                f"{len(first.data.accepted_inventory_adjustments)}, "
                f"on_hand={first.recommendation.on_hand}, quarantine={sum(reasons.values())}"
            ),
        ),
        R1VerificationResult(
            "forecast + recommendation",
            (
                forecast_values == _EXPECTED_FORECAST
                and first.forecast.total_expected_demand() == 77
                and first.recommendation.recommended_quantity == 47
            ),
            (
                f"forecast={forecast_values}, total={first.forecast.total_expected_demand()}, "
                f"quantity={first.recommendation.recommended_quantity}"
            ),
        ),
        R1VerificationResult(
            "deterministic fingerprint",
            (
                first.manifest.fingerprint == second.manifest.fingerprint
                and first.forecast.forecast_sha256 == second.forecast.forecast_sha256
                and first.recommendation.decision_sha256 == second.recommendation.decision_sha256
            ),
            f"fingerprint={first.manifest.fingerprint[:16]}",
        ),
        R1VerificationResult(
            "approval + idempotent execution",
            (
                first.projection.state is DecisionState.OUTCOME_OBSERVED
                and first.effect_count == 1
                and second.effect_count == 1
                and first.effect.external_reference == second.effect.external_reference
                and len(first.projection.events) == 5
            ),
            (
                f"events={len(first.projection.events)}, effects={first.effect_count}, "
                f"reference={first.effect.external_reference}"
            ),
        ),
        R1VerificationResult(
            "delayed feedback",
            (
                first.actual_demand == 79
                and first.absolute_demand_error == 2
                and first.projection.outcome is not None
            ),
            (f"actual={first.actual_demand}, absolute_error={first.absolute_demand_error}"),
        ),
        R1VerificationResult(
            "five-minute smoke ceiling",
            first.duration_seconds < 300 and second.duration_seconds < 300,
            (f"run_a={first.duration_seconds:.3f}s, run_b={second.duration_seconds:.3f}s"),
        ),
    ]
    _write_json(
        artifact_dir / "r1-verification-summary.json",
        {
            "checks": [
                {"detail": result.detail, "name": result.name, "passed": result.passed}
                for result in results
            ],
            "manifest_fingerprint": first.manifest.fingerprint,
            "release": "R1",
            "status": "PASS" if all(result.passed for result in results) else "FAIL",
        },
    )
    return results
