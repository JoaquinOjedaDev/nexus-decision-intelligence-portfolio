"""Finalized R2 Gold -> point-in-time candidate -> held-out paired evaluation.

This is a separate synthetic runtime experiment, not the frozen R3B experiment.
Neither a numerical win nor this runner can promote the scientific champion.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from importlib.metadata import version
from itertools import pairwise
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from nexus.modeling.r3_artifacts import write_immutable_json_manifest
from nexus.modeling.r3_baseline import forecast_probabilistic_seasonal_naive
from nexus.modeling.r3_contracts import R3RollingOrigin
from nexus.modeling.r3_features import (
    R3PointInTimeFeatureRow,
    build_features_for_origin,
    feature_artifact_rows,
)
from nexus.modeling.r3_metrics import (
    ForecastMetricRow,
    block_bootstrap_95,
    central_interval_coverage,
    mean_weighted_interval_score,
)
from nexus.modeling.r3b_contracts import load_r3b_config
from nexus.modeling.r3b_evaluation import decide_promotion_from_metrics
from nexus.modeling.r3b_models import (
    R3BQuantilePrediction,
    apply_quantile_calibration,
    fit_quantile_calibrator,
)
from nexus.modeling.seasonal_naive import DailyDemandObservation
from nexus.portfolio.learning import CandidateEvaluation, ModelArtifact
from nexus.portfolio.training_data import (
    TrainingLabel,
    TrainingSnapshot,
    pit_history,
    validate_training_snapshot,
)
from nexus.portfolio.training_models import (
    DailyQuantileCandidate,
    fit_daily_candidate,
    matrix_from_features,
)


class LearningPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    contract_version: Literal["nexus.runtime-learning.v1"]
    classification: Literal["synthetic"]
    training_start: date
    training_end: date
    fit_at: AwareDatetime
    calibration_start: date
    calibration_end: date
    calibrated_at: AwareDatetime
    evaluation_start: date
    evaluation_end: date
    evaluation_cutoff: AwareDatetime
    horizon_days: Literal[14]
    origin_stride_days: Literal[14]
    minimum_training_rows: int = Field(ge=500)
    minimum_calibration_support_per_horizon: Literal[20]
    minimum_evaluation_blocks: int = Field(ge=20)
    bootstrap_resamples: int = Field(ge=1000)
    bootstrap_seed: int = Field(ge=0)
    target: Literal["fully_fulfilled_daily_units"]
    missing_values: Literal["zero_with_history_count_signal"]
    automatic_promotion: Literal[False]
    maximum_coverage80_error: float = Field(ge=0.05, le=0.05)
    maximum_coverage95_error: float = Field(ge=0.05, le=0.05)
    evidence_scope: Literal["synthetic_engineering_benchmark_not_confirmatory"]

    @model_validator(mode="after")
    def chronology(self) -> Self:
        dates = (
            self.training_start,
            self.training_end,
            self.fit_at.date(),
            self.calibration_start,
            self.calibration_end,
            self.calibrated_at.date(),
            self.evaluation_start,
            self.evaluation_end,
            self.evaluation_cutoff.date(),
        )
        if any(left >= right for left, right in pairwise(dates)):
            raise ValueError("learning windows/cutoffs must be disjoint and strictly chronological")
        for timestamp in (self.fit_at, self.calibrated_at, self.evaluation_cutoff):
            if timestamp.utcoffset() != timedelta(0):
                raise ValueError("learning cutoffs must be UTC")
        return self


def load_learning_plan(path: Path) -> LearningPlan:
    return LearningPlan.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


def _encode(value: Any) -> bytes:
    return json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()


def _sha(value: Any) -> str:
    return hashlib.sha256(_encode(value)).hexdigest()


@dataclass(frozen=True)
class LearningRow:
    feature: R3PointInTimeFeatureRow
    label: TrainingLabel
    baseline: tuple[float, ...]

    @property
    def key(self) -> tuple[str, str, date]:
        return self.feature.sku_id, self.feature.location_id, self.feature.target_date


def build_learning_rows(
    snapshot: TrainingSnapshot,
    *,
    start: date,
    end: date,
    labels_at: datetime,
    diagnostics: dict[str, Any] | None = None,
) -> tuple[LearningRow, ...]:
    """Nonoverlapping whole 14-day origins; labels must be finalized by split cutoff.

    An incomplete key-origin is excluded as a whole, preventing selective easy
    horizons from inflating calibration support or changing comparison weights.
    """
    eligible = {
        (row.sku_id, row.location_id, row.demand_date): row
        for row in snapshot.labels
        if row.available_at <= labels_at
        and row.correction_closed_at <= labels_at
        and row.demand_date < labels_at.date()
    }
    output: list[LearningRow] = []
    known_keys = sorted({(row.sku_id, row.location_id) for row in snapshot.labels})
    excluded: list[dict[str, Any]] = []
    eligible_origins = 0
    issue_date = start
    while issue_date + timedelta(days=13) <= end:
        issue_time = datetime.combine(issue_date, datetime.min.time(), UTC)
        history = pit_history(snapshot, issue_time)
        history_keys = {(row.sku_id, row.location_id) for row in history}
        for sku_id, location_id in known_keys:
            if (sku_id, location_id) not in history_keys:
                excluded.append(
                    {
                        "sku_id": sku_id,
                        "location_id": location_id,
                        "issue_time": issue_time.isoformat(),
                        "reason": "no_finalized_history",
                    }
                )
        if history:
            origin = R3RollingOrigin(
                name=f"runtime-{issue_date.isoformat()}", issue_time=issue_time, role="diagnostic"
            )
            features = build_features_for_origin(history, origin=origin, horizon_days=14)
            keys = sorted({(row.sku_id, row.location_id) for row in features})
            for sku_id, location_id in keys:
                selected = tuple(
                    row
                    for row in features
                    if (row.sku_id, row.location_id) == (sku_id, location_id)
                )
                missing = [
                    row.target_date.isoformat()
                    for row in selected
                    if (sku_id, location_id, row.target_date) not in eligible
                ]
                if missing:
                    excluded.append(
                        {
                            "sku_id": sku_id,
                            "location_id": location_id,
                            "issue_time": issue_time.isoformat(),
                            "reason": "incomplete_finalized_uncensored_window",
                            "missing_target_dates": missing,
                        }
                    )
                    continue
                eligible_origins += 1
                baseline = forecast_probabilistic_seasonal_naive(
                    [
                        DailyDemandObservation(
                            sku_id=row.sku_id,
                            location_id=row.location_id,
                            demand_date=row.demand_date,
                            quantity=row.demand_qty,
                            available_at=row.latest_available_at,
                        )
                        for row in history
                        if (row.sku_id, row.location_id) == (sku_id, location_id)
                    ],
                    sku_id=sku_id,
                    location_id=location_id,
                    issue_time=issue_time,
                    horizon_days=14,
                )
                for feature, point in zip(selected, baseline.points, strict=True):
                    output.append(
                        LearningRow(
                            feature,
                            eligible[(sku_id, location_id, feature.target_date)],
                            tuple(
                                float(getattr(point, name))
                                for name in ("q025", "q10", "q50", "q90", "q975")
                            ),
                        )
                    )
        issue_date += timedelta(days=14)
    if len({row.key for row in output}) != len(output):
        raise ValueError("learning targets must be distinct within a split")
    if diagnostics is not None:
        diagnostics.update(
            eligible_key_origins=eligible_origins,
            excluded_key_origins=len(excluded),
            excluded_origins=excluded,
            trailing_days_outside_whole_horizon=max(0, (end - issue_date).days + 1),
            selection_boundary="Metrics condition on complete fully fulfilled/finalized windows",
        )
    return tuple(output)


def _row_payload(rows: tuple[LearningRow, ...]) -> list[dict[str, Any]]:
    features = feature_artifact_rows(tuple(row.feature for row in rows))
    return [
        {
            "feature": feature,
            "label": {
                "quantity": row.label.quantity,
                "available_at": row.label.available_at.isoformat(),
                "correction_closed_at": row.label.correction_closed_at.isoformat(),
                "source_rows_sha256": row.label.source_rows_sha256,
            },
            "baseline": row.baseline,
        }
        for row, feature in zip(rows, features, strict=True)
    ]


def _metric(row: LearningRow, values: tuple[float, ...]) -> ForecastMetricRow:
    return ForecastMetricRow(
        sku_id=row.feature.sku_id,
        location_id=row.feature.location_id,
        origin_time=row.feature.issue_time,
        target_date=row.feature.target_date,
        actual=Decimal(row.label.quantity),
        **{
            name: Decimal(str(value))
            for name, value in zip(("q025", "q10", "q50", "q90", "q975"), values, strict=True)
        },
    )


def _metric_key(row: ForecastMetricRow) -> tuple[str, str, datetime, date]:
    return row.sku_id, row.location_id, row.origin_time, row.target_date


@dataclass(frozen=True)
class LearningRun:
    receipt: dict[str, Any]
    candidate: DailyQuantileCandidate | None
    baseline_artifact: ModelArtifact | None
    evaluation_rows: tuple[LearningRow, ...] = ()
    candidate_forecasts: tuple[R3BQuantilePrediction, ...] = ()
    receipt_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "receipt_sha256", _sha(self.receipt))


def verify_learning_run(run: LearningRun) -> None:
    if _sha(run.receipt) != run.receipt_sha256:
        raise ValueError("learning receipt mutated after evaluation")
    if run.candidate is None:
        if run.receipt["training_performed"] is not False or run.baseline_artifact is not None:
            raise ValueError("abstention cannot contain a trained artifact")
        return
    if (
        run.candidate.sha256 != run.receipt["candidate_sha256"]
        or run.baseline_artifact is None
        or run.baseline_artifact.sha256 != run.receipt["baseline_artifact_sha256"]
    ):
        raise ValueError("learning artifact differs from the evaluated bytes")
    features = tuple(row.feature for row in run.evaluation_rows)
    reloaded = DailyQuantileCandidate.from_bytes(
        run.candidate.to_bytes(), expected_sha256=run.receipt["candidate_sha256"]
    )
    predictions = reloaded.predict(
        matrix_from_features(features), tuple(row.horizon_day for row in features)
    )
    if predictions != run.candidate_forecasts:
        raise ValueError("learning forecasts do not match the retained candidate")
    recorded = run.receipt["evaluation"]
    if len(recorded) != len(predictions):
        raise ValueError("learning evaluation rows were truncated")
    for row, prediction, record in zip(run.evaluation_rows, predictions, recorded, strict=True):
        actual = tuple(getattr(prediction, name) for name in ("q025", "q10", "q50", "q90", "q975"))
        if tuple(record["candidate"]) != actual or record["actual"] != row.label.quantity:
            raise ValueError("learning receipt prediction/label mismatch")


def run_learning_experiment(
    project_root: Path,
    snapshot: TrainingSnapshot,
    plan: LearningPlan,
) -> LearningRun:
    """Train once after sufficiency checks; never tune on final evaluation outcomes."""
    validate_training_snapshot(snapshot)
    if snapshot.as_of != plan.evaluation_cutoff:
        raise ValueError("snapshot as_of must exactly match the frozen evaluation cutoff")
    bindings = {
        "snapshot_sha256": snapshot.snapshot_sha256,
        "source_snapshot_sha256": snapshot.source_snapshot_sha256,
        "finalization_sha256": snapshot.finalization_sha256,
        "plan_sha256": _sha(plan.model_dump(mode="json")),
    }
    governed = [
        "src/nexus/portfolio/training_pipeline.py",
        "src/nexus/portfolio/training_data.py",
        "src/nexus/portfolio/training_models.py",
        "src/nexus/portfolio/training_fixture.py",
        "src/nexus/modeling/r3_features.py",
        "src/nexus/modeling/r3b_evaluation.py",
        "src/nexus/modeling/r3b_models.py",
        "src/nexus/modeling/r3_metrics.py",
        "src/nexus/modeling/r3_baseline.py",
        "configs/modeling/r3b-smoke.yaml",
        "uv.lock",
    ]
    code_files = {
        name: hashlib.sha256((project_root / name).read_bytes()).hexdigest() for name in governed
    }
    bindings["code_sha256"] = _sha(code_files)
    receipt: dict[str, Any] = {
        "contract_version": "nexus.runtime-learning-result.v1",
        "bindings": bindings,
        "plan": plan.model_dump(mode="json"),
        "training_performed": False,
        "scientific_champion_changed": False,
        "classification": "synthetic",
        "target_semantics": "Fully fulfilled observed daily units; not inferred latent lost demand",
        "snapshot": snapshot.to_dict(),
        "code_files": code_files,
    }
    if snapshot.status != "READY":
        receipt.update(status="ABSTAIN_INSUFFICIENT_EVIDENCE", reason="snapshot_not_ready")
        return LearningRun(receipt, None, None)
    selection: dict[str, dict[str, Any]] = {
        name: {} for name in ("training", "calibration", "final")
    }
    train = build_learning_rows(
        snapshot,
        start=plan.training_start,
        end=plan.training_end,
        labels_at=plan.fit_at,
        diagnostics=selection["training"],
    )
    calibration = build_learning_rows(
        snapshot,
        start=plan.calibration_start,
        end=plan.calibration_end,
        labels_at=plan.calibrated_at,
        diagnostics=selection["calibration"],
    )
    final = build_learning_rows(
        snapshot,
        start=plan.evaluation_start,
        end=plan.evaluation_end,
        labels_at=plan.evaluation_cutoff,
        diagnostics=selection["final"],
    )
    sets = [set(row.key for row in rows) for rows in (train, calibration, final)]
    if any(sets[a] & sets[b] for a, b in ((0, 1), (0, 2), (1, 2))):
        raise ValueError("learning split target leakage")
    support = Counter(row.feature.horizon_day for row in calibration)
    blocks = {
        (row.feature.sku_id, row.feature.location_id, row.feature.issue_time) for row in final
    }
    receipt["splits"] = {
        name: {
            "rows": len(rows),
            "sha256": _sha(_row_payload(rows)),
            "distinct_targets": len({row.key for row in rows}),
            "max_label_closed_at": (
                max(row.label.correction_closed_at for row in rows).astimezone(UTC).isoformat()
                if rows
                else None
            ),
        }
        for name, rows in (("training", train), ("calibration", calibration), ("final", final))
    }
    receipt["calibration_support"] = {str(h): support[h] for h in range(1, 15)}
    receipt["selection"] = selection
    receipt["split_rows"] = {
        name: _row_payload(rows)
        for name, rows in (("training", train), ("calibration", calibration), ("final", final))
    }
    if (
        len(train) < plan.minimum_training_rows
        or any(support[h] < 20 for h in range(1, 15))
        or len(blocks) < plan.minimum_evaluation_blocks
    ):
        receipt.update(status="ABSTAIN_INSUFFICIENT_EVIDENCE", reason="split_support_not_met")
        return LearningRun(receipt, None, None)

    def matrix(rows: tuple[LearningRow, ...]) -> tuple[tuple[float, ...], ...]:
        return matrix_from_features(tuple(row.feature for row in rows))

    candidate = fit_daily_candidate(
        matrix(train),
        tuple(row.label.quantity for row in train),
        tuple(row.feature.horizon_day for row in train),
        calibration_matrix=matrix(calibration),
        calibration_targets=tuple(row.label.quantity for row in calibration),
        calibration_horizons=tuple(row.feature.horizon_day for row in calibration),
        minimum_training_rows=plan.minimum_training_rows,
        minimum_calibration_support=20,
    )
    receipt["dependencies"] = {name: version(name) for name in ("lightgbm", "numpy", "pyarrow")}
    baseline_calibrators = {
        h: fit_quantile_calibrator(
            [row.label.quantity for row in calibration if row.feature.horizon_day == h],
            [row.baseline for row in calibration if row.feature.horizon_day == h],
            minimum_support=20,
        )
        for h in range(1, 15)
    }
    baseline_artifact = ModelArtifact.from_bytes(
        "runtime_calibrated_seasonal_baseline",
        _encode(
            {
                "contract": "nexus.runtime-learning-baseline.v1",
                "model": "seasonal_naive_empirical_intervals",
                "algorithm_version": "1.0.0",
                "season_days": 7,
                "horizon_days": 14,
                "history_policy": "available_and_correction_closed_at_each_issue",
                "implementation_binding": bindings["code_sha256"],
                "plan_sha256": bindings["plan_sha256"],
                "calibration_rows_sha256": receipt["splits"]["calibration"]["sha256"],
                "calibrators": {str(h): asdict(item) for h, item in baseline_calibrators.items()},
            }
        ),
    )
    predictions = candidate.predict(matrix(final), tuple(row.feature.horizon_day for row in final))
    reloaded = DailyQuantileCandidate.from_bytes(candidate.to_bytes())
    if (
        reloaded.predict(matrix(final), tuple(row.feature.horizon_day for row in final))
        != predictions
    ):
        raise ValueError("reloaded candidate prediction parity failed")
    baseline_predictions = tuple(
        apply_quantile_calibration(baseline_calibrators[row.feature.horizon_day], [row.baseline])[0]
        for row in final
    )

    def values(item: R3BQuantilePrediction) -> tuple[float, ...]:
        return tuple(getattr(item, name) for name in ("q025", "q10", "q50", "q90", "q975"))

    candidate_metrics = tuple(
        _metric(row, values(pred)) for row, pred in zip(final, predictions, strict=True)
    )
    baseline_metrics = tuple(
        _metric(row, values(pred)) for row, pred in zip(final, baseline_predictions, strict=True)
    )
    delta = {
        _metric_key(c): mean_weighted_interval_score((b,)) - mean_weighted_interval_score((c,))
        for c, b in zip(candidate_metrics, baseline_metrics, strict=True)
    }
    interval = block_bootstrap_95(
        candidate_metrics,
        lambda rows: (
            sum((delta[_metric_key(row)] for row in rows), Decimal(0)) / Decimal(len(rows))
        ),
        resamples=plan.bootstrap_resamples,
        seed=plan.bootstrap_seed,
    )
    cw, bw = (
        mean_weighted_interval_score(candidate_metrics),
        mean_weighted_interval_score(baseline_metrics),
    )
    improvement = (bw - cw) / bw if bw else None
    policy = load_r3b_config(project_root / "configs/modeling/r3b-smoke.yaml").promotion
    coverage80 = central_interval_coverage(
        [r.actual for r in candidate_metrics],
        [r.q10 for r in candidate_metrics],
        [r.q90 for r in candidate_metrics],
    )
    coverage95 = central_interval_coverage(
        [r.actual for r in candidate_metrics],
        [r.q025 for r in candidate_metrics],
        [r.q975 for r in candidate_metrics],
    )
    quality_checks = {
        "prediction_reload_parity": True,
        "coverage80_within_five_percentage_points": abs(coverage80 - Decimal("0.80"))
        <= Decimal("0.05"),
        "coverage95_within_five_percentage_points": abs(coverage95 - Decimal("0.95"))
        <= Decimal("0.05"),
    }
    decision = decide_promotion_from_metrics(
        relative_wis_improvement=improvement,
        paired_bootstrap_lower_bound=interval.lower_95,
        promotion=policy,
        evidence_sufficient=True,
        gate_checks_passed=all(quality_checks.values()),
    )
    segments = []
    for dimension in ("sku_id", "location_id"):
        for value in sorted({getattr(row, dimension) for row in candidate_metrics}):
            c = tuple(row for row in candidate_metrics if getattr(row, dimension) == value)
            b = tuple(row for row in baseline_metrics if getattr(row, dimension) == value)
            segments.append(
                {
                    "dimension": dimension,
                    "value": value,
                    "rows": len(c),
                    "candidate_wis": str(mean_weighted_interval_score(c)),
                    "baseline_wis": str(mean_weighted_interval_score(b)),
                }
            )
    receipt.update(
        status=decision.status,
        training_performed=True,
        candidate_sha256=candidate.sha256,
        baseline_artifact_sha256=baseline_artifact.sha256,
        scientific_decision=asdict(decision),
        quality_checks=quality_checks,
        evaluation_blocks=len(blocks),
        metrics={
            "candidate_coverage95": str(coverage95),
            "candidate_wis": str(cw),
            "baseline_wis": str(bw),
            "relative_wis_improvement": None if improvement is None else str(improvement),
            "paired_improvement_95": {
                "lower": str(interval.lower_95),
                "estimate": str(interval.estimate),
                "upper": str(interval.upper_95),
            },
            "candidate_coverage80": str(
                central_interval_coverage(
                    [r.actual for r in candidate_metrics],
                    [r.q10 for r in candidate_metrics],
                    [r.q90 for r in candidate_metrics],
                )
            ),
        },
        segments=segments,
        horizon_diagnostics=[
            {
                "horizon": h,
                "rows": sum(row.feature.horizon_day == h for row in final),
                "candidate_wis": str(
                    mean_weighted_interval_score(
                        tuple(
                            metric
                            for row, metric in zip(final, candidate_metrics, strict=True)
                            if row.feature.horizon_day == h
                        )
                    )
                ),
                "baseline_wis": str(
                    mean_weighted_interval_score(
                        tuple(
                            metric
                            for row, metric in zip(final, baseline_metrics, strict=True)
                            if row.feature.horizon_day == h
                        )
                    )
                ),
            }
            for h in range(1, 15)
        ],
        replay_prediction_parity=True,
        evaluation=[
            {
                "key": [
                    r.feature.sku_id,
                    r.feature.location_id,
                    r.feature.issue_time.isoformat(),
                    r.feature.target_date.isoformat(),
                ],
                "actual": r.label.quantity,
                "candidate": values(c),
                "baseline": values(b),
            }
            for r, c, b in zip(final, predictions, baseline_predictions, strict=True)
        ],
    )
    if any(
        hashlib.sha256((project_root / name).read_bytes()).hexdigest() != expected
        for name, expected in code_files.items()
    ):
        raise ValueError("governing learning files changed during the experiment")
    return LearningRun(receipt, candidate, baseline_artifact, final, predictions)


def persist_learning_run(output_dir: Path, run: LearningRun) -> Path:
    """Retain candidate native model strings and all input/evaluation bindings."""
    verify_learning_run(run)
    payload = {
        **run.receipt,
        "candidate_artifact": None
        if run.candidate is None
        else json.loads(run.candidate.to_bytes()),
        "baseline_artifact": None
        if run.baseline_artifact is None
        else json.loads(run.baseline_artifact.payload),
    }
    return write_immutable_json_manifest(
        output_dir,
        dataset="runtime.learning",
        manifest=payload,
        schema_identity={
            "contract": "nexus.runtime-learning-result.v1",
            "classification": "synthetic",
        },
    ).path


def sandbox_evaluation(run: LearningRun, *, monitor_evaluation_sha256: str) -> CandidateEvaluation:
    """Trusted local bridge; never accept this receipt from browser-supplied flags."""
    verify_learning_run(run)
    if run.candidate is None or run.baseline_artifact is None:
        raise ValueError("an abstained run has no candidate to register")
    payload = {
        "contract_version": "nexus.sandbox-evaluation.v1",
        "source_snapshot_sha256": run.receipt["bindings"]["source_snapshot_sha256"],
        "monitor_evaluation_sha256": monitor_evaluation_sha256,
        "artifact_sha256": run.candidate.sha256,
        "baseline_artifact_sha256": run.baseline_artifact.sha256,
        "config_sha256": run.receipt["bindings"]["plan_sha256"],
        "evaluation_cutoff": run.receipt["plan"]["evaluation_cutoff"],
        "evidence_sufficient": True,
        "scientific_decision": run.receipt["scientific_decision"],
        "runtime_learning_receipt_sha256": run.receipt_sha256,
        "training_performed": True,
        "verification_claim_boundary": (
            "Trusted internal runtime bridge; synthetic experiment, not frozen R3B"
        ),
    }
    content = _encode(payload)
    return CandidateEvaluation(content, hashlib.sha256(content).hexdigest())
