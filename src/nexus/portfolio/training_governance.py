"""Record actual learned artifacts and offline replay; never deploy or approve."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from nexus.contracts import ActorRole
from nexus.modeling.r3_artifacts import canonical_json_sha256, write_immutable_json_manifest
from nexus.modeling.r3_features import feature_artifact_rows
from nexus.portfolio.auth import Actor
from nexus.portfolio.learning import LearningBook, ModelArtifact, ShadowOutput
from nexus.portfolio.monitoring import MonitorActual, MonitorBook, MonitorForecast, MonitorInput
from nexus.portfolio.training_data import TrainingLabel
from nexus.portfolio.training_pipeline import LearningRun, sandbox_evaluation, verify_learning_run


def _shadow(case: str, identity: str, values: Any) -> ShadowOutput:
    return ShadowOutput(
        case,
        identity,
        cast(
            tuple[Decimal, Decimal, Decimal, Decimal, Decimal],
            tuple(Decimal(str(value)) for value in values),
        ),
    )


def record_learning_governance(
    run: LearningRun,
    output_dir: Path,
    *,
    proposer: Actor,
    registrar: Actor,
    expected_source_sha256: str,
) -> dict[str, Any]:
    """Persist real baseline telemetry, proposal, candidate and paired held-out replay.

    Planner/system identities are caller-owned. This bridge never manufactures a
    human approver, deploys a candidate, changes the scientific champion, or writes
    PortfolioStore. Monitoring uses raw seasonal forecasts; shadow comparison uses
    the separately calibrated baseline that was actually evaluated by the trainer.
    """
    verify_learning_run(run)
    if run.candidate is None or run.baseline_artifact is None:
        raise ValueError("abstained learning run has no candidate for governance")
    receipt, snapshot = run.receipt, run.receipt["snapshot"]
    source_hash = receipt["bindings"]["source_snapshot_sha256"]
    if source_hash != expected_source_sha256 or snapshot["source_snapshot_sha256"] != source_hash:
        raise ValueError("learning governance source binding mismatch")
    snapshot_hash = canonical_json_sha256(
        {key: value for key, value in snapshot.items() if key != "snapshot_sha256"}
    )
    if (
        snapshot_hash != snapshot["snapshot_sha256"]
        or snapshot_hash != receipt["bindings"]["snapshot_sha256"]
    ):
        raise ValueError("learning governance snapshot receipt mismatch")
    labels = {
        row.key: row
        for row in (
            TrainingLabel.model_validate_json(json.dumps(value)) for value in snapshot["labels"]
        )
    }
    if len(labels) != len(snapshot["labels"]):
        raise ValueError("duplicate finalized snapshot label")
    locations = tuple(sorted({row.location_id for row in labels.values()}))
    for actor, role in ((proposer, ActorRole.PLANNER), (registrar, ActorRole.SYSTEM)):
        if (
            not isinstance(actor, Actor)
            or actor.role is not role
            or not set(locations).issubset(actor.locations)
        ):
            raise PermissionError("governance requires a global planner and system registrar")
    instant = datetime.fromisoformat(receipt["plan"]["evaluation_cutoff"])
    raw_rows = [
        row for name in ("training", "calibration", "final") for row in receipt["split_rows"][name]
    ]
    raw_identity = canonical_json_sha256(
        [{"feature": row["feature"], "baseline": row["baseline"]} for row in raw_rows]
    )
    forecasts: list[MonitorForecast] = []
    actuals: list[MonitorActual] = []
    for row in raw_rows:
        feature = row["feature"]
        label = labels.get(
            (
                feature["sku_id"],
                feature["location_id"],
                datetime.fromisoformat(feature["target_date"]).date(),
            )
        )
        if label is None or row["label"] != {
            "quantity": label.quantity,
            "available_at": label.available_at.isoformat(),
            "correction_closed_at": label.correction_closed_at.isoformat(),
            "source_rows_sha256": label.source_rows_sha256,
        }:
            raise ValueError("split label differs from finalized source snapshot")
        forecasts.append(
            MonitorForecast(
                sku_id=label.sku_id,
                location_id=label.location_id,
                issue_time=datetime.fromisoformat(feature["issue_time"]),
                target_date=label.demand_date,
                **dict(
                    zip(
                        ("q025", "q10", "q50", "q90", "q975"),
                        (Decimal(str(v)) for v in row["baseline"]),
                        strict=True,
                    )
                ),
                model_id="seasonal_naive_empirical_intervals",
                artifact_sha256=raw_identity,
            )
        )
        actuals.append(
            MonitorActual(
                sku_id=label.sku_id,
                location_id=label.location_id,
                target_date=label.demand_date,
                quantity=Decimal(label.quantity),
                available_at=label.available_at,
                mature_after=label.correction_closed_at,
                correction_closed=True,
            )
        )
    monitor_input = MonitorInput(
        now=instant,
        source_watermark=max(row.available_at for row in labels.values()),
        forecasts=tuple(forecasts),
        actuals=tuple(actuals),
        source_id="offline_learning_replay",
        evidence_sha256=source_hash,
    )  # Contract rejects overlapping/duplicate outcome keys before any persistence.
    baseline, candidate = [], []
    features = feature_artifact_rows(tuple(row.feature for row in run.evaluation_rows))
    for feature, row, prediction, paired in zip(
        features, run.evaluation_rows, run.candidate_forecasts, receipt["evaluation"], strict=True
    ):
        key = [
            row.feature.sku_id,
            row.feature.location_id,
            row.feature.issue_time.isoformat(),
            row.feature.target_date.isoformat(),
        ]
        if paired["key"] != key:
            raise ValueError("held-out shadow key differs from evaluated case")
        identity = canonical_json_sha256(
            {"feature": feature, "source": source_hash, "plan": receipt["bindings"]["plan_sha256"]}
        )
        case = "offline-held-out-" + canonical_json_sha256(key)[:24]

        baseline.append(_shadow(case, identity, paired["baseline"]))
        candidate.append(_shadow(case, identity, prediction.values))
    directory = output_dir.resolve()
    write_immutable_json_manifest(
        directory,
        dataset="learning.raw_baseline_telemetry",
        manifest={
            "scope": "offline historical replay; raw baseline, not calibrated comparator",
            "source_snapshot_sha256": source_hash,
            "forecast_rows_sha256": raw_identity,
            "rows": raw_rows,
        },
        schema_identity={"contract": "nexus.learning-raw-telemetry.v1"},
    )
    monitored = MonitorBook(directory / "monitoring.sqlite3").evaluate(monitor_input)
    evaluation = sandbox_evaluation(run, monitor_evaluation_sha256=monitored["evaluation_sha256"])
    artifact = ModelArtifact.from_bytes(
        "runtime_daily_lightgbm_candidate", run.candidate.to_bytes()
    )
    if (
        evaluation.to_dict()["artifact_sha256"] != artifact.sha256
        or evaluation.to_dict()["baseline_artifact_sha256"] != run.baseline_artifact.sha256
    ):
        raise ValueError("learning governance artifact receipt mismatch")
    book = LearningBook(
        directory / "learning.sqlite3",
        initial_artifact=run.baseline_artifact,
        locations=locations,
        clock=lambda: instant,
    )
    proposal = book.propose_training(
        proposer,
        source_snapshot_sha256=source_hash,
        monitor_evaluation_sha256=monitored["evaluation_sha256"],
        reason="Register completed experiment; offline held-out replay, never live deployment",
    )
    candidate_id = book.register_candidate(
        registrar, proposal_id=proposal, artifact=artifact, evaluation=evaluation
    )
    shadow = book.record_shadow(
        registrar,
        candidate_id=candidate_id,
        baseline_artifact_sha256=run.baseline_artifact.sha256,
        candidate_artifact_sha256=artifact.sha256,
        baseline=tuple(baseline),
        candidate=tuple(candidate),
    )
    state = book.snapshot()
    return {
        "contract_version": "nexus.learning-governance-bridge.v1",
        "scope": "offline held-out replay; no live traffic or deployment",
        "source_watermark_semantics": "Historical source-label availability, not live ingestion",
        "source_snapshot_sha256": source_hash,
        "learning_receipt_sha256": run.receipt_sha256,
        "monitor_evaluation_sha256": monitored["evaluation_sha256"],
        "monitor": monitored,
        "raw_monitor_baseline_sha256": raw_identity,
        "calibrated_comparison_baseline_sha256": run.baseline_artifact.sha256,
        "proposal_id": proposal,
        "candidate_id": candidate_id,
        "candidate_sha256": artifact.sha256,
        "shadow_replay": {"scope": "offline held-out replay", **shadow},
        "trained_by_upstream_pipeline": True,
        "registry_performs_training": False,
        "deployment_applied": False,
        "scientific_promotion_applied": False,
        "human_approval_recorded": False,
        "state": state,
    }
