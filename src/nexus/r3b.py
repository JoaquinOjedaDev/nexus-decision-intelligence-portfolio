"""Executable R3B learned-intelligence and evidence gate.

The bounded gate trains deterministic global LightGBM quantile challengers for
demand and supplier lead time, calibrates demand forecasts on a frozen earlier
origin, evaluates only after forecasts are persisted, reconciles the hierarchy,
logs a local MLflow experiment, and emits a platform-normalized CI receipt.
Synthetic evidence can complete the engineering gate but can never promote the
champion automatically.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

from nexus.data.r2c_models import load_r2c_profile
from nexus.modeling.r3_artifacts import (
    ScientificArtifactRef,
    write_immutable_json_manifest,
)
from nexus.modeling.r3_baseline import (
    ProbabilisticBaselineForecast,
    forecast_probabilistic_seasonal_naive,
)
from nexus.modeling.r3_hierarchy import (
    BottomLevelQuantileForecast,
    HierarchicalQuantileForecast,
    aggregate_quantile_forecasts,
    validate_bottom_up_coherence,
)
from nexus.modeling.r3_metrics import weighted_interval_score
from nexus.modeling.r3b_contracts import (
    DEFAULT_R3B_CONFIG_PATH,
    R3BConfig,
    load_r3b_config,
    r3b_config_sha256,
)
from nexus.modeling.r3b_data import (
    R3BDataSnapshot,
    R3BEvaluationBundle,
    R3BPersistedForecast,
    R3BR2CRegistration,
    canonical_rows_sha256,
    get_evaluation_truth_after_persistence,
    register_r2c_package,
)
from nexus.modeling.r3b_evaluation import (
    R3BEvaluationResult,
    R3BPlatformReceipt,
    R3BPredictionRecord,
    R3BPromotionDecision,
    R3BTruthRecord,
    assert_platform_receipts_match,
    build_platform_receipt,
    decide_promotion,
    evaluate_candidate_against_baseline,
)
from nexus.modeling.r3b_features import (
    R3BDemandFeatureRow,
    R3BDemandTrainingDataset,
    build_demand_forecast_rows,
    build_demand_training_examples,
    build_lead_time_training_examples,
)
from nexus.modeling.r3b_models import (
    R3BEmpiricalQuantileBaseline,
    R3BQuantileCalibrator,
    R3BQuantileEnsemble,
    R3BQuantilePrediction,
    apply_quantile_calibration,
    fit_empirical_quantile_baseline,
    fit_quantile_calibrator,
    fit_quantile_ensemble,
    predict_empirical_quantiles,
    predict_quantiles,
)
from nexus.modeling.r3b_resources import R3BResourceEvidence, R3BResourceMonitor
from nexus.modeling.r3b_tracking import R3BTrackingReceipt, log_r3b_challenger
from nexus.modeling.seasonal_naive import DailyDemandObservation
from nexus.simulation import generate_r2c_profile
from nexus.simulation.representative_supply_network import iter_r2c_demand_snapshot
from nexus.storage import file_sha256

_R2C_SMOKE_CONFIG = Path(__file__).resolve().parents[2] / "configs/profiles/smoke.yaml"
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_QUANTILE_NAMES = ("q025", "q10", "q50", "q90", "q975")
_QUANTILE_VALUES = tuple(Decimal(value) for value in ("0.025", "0.10", "0.50", "0.90", "0.975"))


class R3BGateError(RuntimeError):
    """The R3B run cannot produce trustworthy learned-intelligence evidence."""


def _canonical_text_sha256(path: Path) -> str:
    normalized = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _source_bundle_sha256() -> str:
    source_root = _PROJECT_ROOT / "src" / "nexus"
    return canonical_rows_sha256(
        [
            {
                "content_sha256": _canonical_text_sha256(path),
                "path": path.relative_to(_PROJECT_ROOT).as_posix(),
            }
            for path in sorted(source_root.rglob("*.py"))
        ]
    )


def _execution_revision_sha256(source_bundle_sha256: str) -> str:
    github_sha = os.environ.get("GITHUB_SHA", "").strip().lower()
    revision = github_sha if re.fullmatch(r"[a-f0-9]{40}", github_sha) else source_bundle_sha256
    return hashlib.sha256(revision.encode("ascii")).hexdigest()


def _configure_bounded_native_runtime() -> None:
    """Bound numerical and Arrow worker pools before either model family executes."""

    os.environ["OMP_NUM_THREADS"] = "2"
    os.environ["OMP_DYNAMIC"] = "FALSE"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["NUMEXPR_NUM_THREADS"] = "1"
    import pyarrow as pa

    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)


@dataclass(frozen=True, slots=True)
class R3BDemandOriginRun:
    """Model inputs and persisted artifacts for one frozen rolling origin."""

    name: str
    issue_time: datetime
    training: R3BDemandTrainingDataset
    forecast_features: tuple[R3BDemandFeatureRow, ...]
    model: R3BQuantileEnsemble
    raw_predictions: tuple[R3BPredictionRecord, ...]
    baseline_predictions: tuple[R3BPredictionRecord, ...]
    feature_artifact: ScientificArtifactRef
    model_artifact: ScientificArtifactRef


@dataclass(frozen=True, slots=True)
class R3BLeadTimeEvaluation:
    """Frozen temporal holdout comparison for the lead-time challenger."""

    training_label_count: int
    holdout_label_count: int
    right_censored_excluded_at_final: int
    candidate_wis: Decimal
    baseline_wis: Decimal
    relative_wis_improvement: Decimal | None
    baseline_status: str
    candidate_training_identity: str
    model_frozen_at: datetime
    holdout_feature_sha256: str
    forecast_artifact_sha256: str
    forecast_artifact_file_sha256: str
    rows: tuple[R3BLeadTimeEvaluationRow, ...]


@dataclass(frozen=True, slots=True)
class R3BLeadTimeEvaluationRow:
    """One event-time-safe lead-time forecast joined to its later-matured label."""

    order_id: str
    supplier_id: str
    prediction_as_of: datetime
    label_available_at: datetime
    actual_lead_time_days: Decimal
    candidate: R3BQuantilePrediction
    baseline: R3BQuantilePrediction
    candidate_wis: Decimal
    baseline_wis: Decimal

    def __post_init__(self) -> None:
        prediction_as_of = self.prediction_as_of
        label_available_at = self.label_available_at
        if prediction_as_of.tzinfo is None or prediction_as_of.utcoffset() is None:
            raise R3BGateError("lead-time prediction_as_of must be timezone-aware")
        if label_available_at.tzinfo is None or label_available_at.utcoffset() is None:
            raise R3BGateError("lead-time label_available_at must be timezone-aware")
        prediction_as_of = prediction_as_of.astimezone(UTC)
        label_available_at = label_available_at.astimezone(UTC)
        if prediction_as_of >= label_available_at:
            raise R3BGateError("lead-time forecasts must predate label availability")
        actual = Decimal(self.actual_lead_time_days)
        if not actual.is_finite() or actual < 0:
            raise R3BGateError("lead-time actual must be finite and nonnegative")
        object.__setattr__(self, "prediction_as_of", prediction_as_of)
        object.__setattr__(self, "label_available_at", label_available_at)
        object.__setattr__(self, "actual_lead_time_days", actual)


@dataclass(frozen=True, slots=True)
class R3BRunResult:
    """One materialized but not yet replay-verified R3B scientific run."""

    config: R3BConfig
    output_dir: Path
    registration: R3BR2CRegistration
    diagnostic_evaluation: R3BEvaluationResult
    calibration_evaluation: R3BEvaluationResult
    final_evaluation: R3BEvaluationResult
    calibrators: Mapping[int, R3BQuantileCalibrator]
    lead_time_evaluation: R3BLeadTimeEvaluation
    promotion: R3BPromotionDecision
    hierarchy_rows: tuple[HierarchicalQuantileForecast, ...]
    tracking: R3BTrackingReceipt
    platform_receipt: R3BPlatformReceipt
    artifacts: tuple[ScientificArtifactRef, ...]
    semantic_manifest: ScientificArtifactRef
    summary_path: Path


@dataclass(frozen=True, slots=True)
class R3BVerificationCheck:
    """One independently readable verification assertion."""

    name: str
    passed: bool
    detail: str


@dataclass(frozen=True, slots=True)
class R3BVerificationResult:
    """Final local R3B gate result and normalized receipt location."""

    passed: bool
    status: str
    checks: tuple[R3BVerificationCheck, ...]
    first_run: R3BRunResult | None
    second_run: R3BRunResult | None
    resource_evidence: R3BResourceEvidence | None
    final_promotion: R3BPromotionDecision | None
    platform_receipt_path: Path | None
    gate_manifest: ScientificArtifactRef
    summary_path: Path


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, default=str, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _artifact_payload(reference: ScientificArtifactRef, root: Path) -> dict[str, object]:
    return {
        "content_sha256": reference.content_sha256,
        "dataset": reference.dataset,
        "file_sha256": reference.file_sha256,
        "format": reference.format,
        "relative_path": reference.path.resolve().relative_to(root.resolve()).as_posix(),
        "row_count": reference.row_count,
        "schema_sha256": reference.schema_sha256,
    }


def _matrix(
    rows: Sequence[Sequence[float | int | None]],
) -> tuple[tuple[float, ...], ...]:
    """Apply the frozen missing-value policy: zero plus an explicit history-count signal."""

    return tuple(tuple(0.0 if value is None else float(value) for value in row) for row in rows)


def _prediction_records(
    features: Sequence[R3BDemandFeatureRow],
    predictions: Sequence[R3BQuantilePrediction],
) -> tuple[R3BPredictionRecord, ...]:
    if len(features) != len(predictions):
        raise R3BGateError("forecast feature and prediction counts differ")
    return tuple(
        R3BPredictionRecord.from_quantile_prediction(
            sku_id=feature.sku_id,
            location_id=feature.location_id,
            target_date=feature.target_date,
            horizon_days=feature.horizon_days,
            prediction=prediction,
            cold_start=feature.cold_start,
        )
        for feature, prediction in zip(features, predictions, strict=True)
    )


def _persisted_prediction_records(
    rows: Sequence[R3BPersistedForecast],
) -> tuple[R3BPredictionRecord, ...]:
    return tuple(
        R3BPredictionRecord(
            sku_id=row.sku_id,
            location_id=row.location_id,
            target_date=row.target_date,
            horizon_days=row.horizon_days,
            q025=row.q025,
            q10=row.q10,
            q50=row.q50,
            q90=row.q90,
            q975=row.q975,
            cold_start=row.cold_start,
        )
        for row in rows
    )


def _visible_observations(
    snapshot: R3BDataSnapshot,
    issue_time: datetime,
) -> dict[tuple[str, str], tuple[DailyDemandObservation, ...]]:
    grouped: dict[tuple[str, str], list[DailyDemandObservation]] = defaultdict(list)
    rows = iter_r2c_demand_snapshot(
        snapshot.output_dir,
        planning_cutoff=issue_time,
        verified_manifest=snapshot.manifest,
    )
    for row in rows:
        if not cast(bool, row["is_active"]) or cast(bool, row["stockout_censored"]):
            continue
        sku_id = cast(str, row["sku_id"])
        location_id = cast(str, row["location_id"])
        grouped[(sku_id, location_id)].append(
            DailyDemandObservation(
                sku_id=sku_id,
                location_id=location_id,
                demand_date=cast(date, row["demand_date"]),
                quantity=cast(int, row["observed_sales_qty"]),
                available_at=cast(datetime, row["available_at"]),
            )
        )
    return {
        key: tuple(sorted(values, key=lambda item: item.demand_date))
        for key, values in grouped.items()
    }


def _seasonal_baseline_predictions(
    snapshot: R3BDataSnapshot,
    features: Sequence[R3BDemandFeatureRow],
    issue_time: datetime,
) -> tuple[R3BPredictionRecord, ...]:
    history = _visible_observations(snapshot, issue_time)
    full_forecasts: dict[tuple[str, str], ProbabilisticBaselineForecast] = {}
    records: list[R3BPredictionRecord] = []
    for feature in features:
        key = feature.sku_id, feature.location_id
        forecast = full_forecasts.get(key)
        if forecast is None:
            forecast = forecast_probabilistic_seasonal_naive(
                history.get(key, ()),
                sku_id=feature.sku_id,
                location_id=feature.location_id,
                issue_time=issue_time,
                horizon_days=28,
                season_length_days=7,
                cold_start_daily_demand=0,
            )
            full_forecasts[key] = forecast
        point = forecast.points[feature.horizon_days - 1]
        prediction = R3BQuantilePrediction.from_values(point.quantiles)
        records.append(
            R3BPredictionRecord.from_quantile_prediction(
                sku_id=feature.sku_id,
                location_id=feature.location_id,
                target_date=feature.target_date,
                horizon_days=feature.horizon_days,
                prediction=prediction,
                cold_start=feature.cold_start,
            )
        )
    return tuple(records)


def _fit_demand_origin(
    snapshot: R3BDataSnapshot,
    *,
    config: R3BConfig,
    name: str,
    issue_time: datetime,
    output_dir: Path,
) -> R3BDemandOriginRun:
    training = build_demand_training_examples(
        snapshot,
        config=config,
        outer_fit_origin=issue_time,
    )
    if not training.examples:
        raise R3BGateError(f"{name} has no mature demand training examples")
    forecast_features = build_demand_forecast_rows(
        snapshot,
        config=config,
        issue_time=issue_time,
    )
    if not forecast_features:
        raise R3BGateError(f"{name} has no active forecast rows")
    feature_artifact = write_immutable_json_manifest(
        output_dir,
        dataset=f"r3b.features.{name}",
        manifest={
            "contract": "nexus.r3b.point-in-time-features.v1",
            "feature_columns": training.feature_columns,
            "forecast_rows": [row.artifact_row() for row in forecast_features],
            "missing_value_policy": "zero_with_history_count_signal",
            "origin": issue_time,
            "training_dataset_sha256": training.dataset_sha256,
            "training_rows": [example.identity_row() for example in training.examples],
        },
    )
    model = fit_quantile_ensemble(
        _matrix(training.feature_matrix()),
        training.targets(),
        training.feature_columns,
        config,
    )
    model_artifact = write_immutable_json_manifest(
        output_dir,
        dataset=f"r3b.models.demand.{name}",
        manifest={
            "cold_start_fallback": config.forecast_contract.cold_start_fallback,
            "contract": "nexus.r3b.lightgbm-quantile-ensemble.v1",
            "model": model.to_dict(),
        },
    )
    predictions = predict_quantiles(
        model,
        _matrix(tuple(row.feature_vector() for row in forecast_features)),
    )
    learned_predictions = _prediction_records(forecast_features, predictions)
    baseline_predictions = _seasonal_baseline_predictions(
        snapshot,
        forecast_features,
        issue_time,
    )
    candidate_predictions = tuple(
        baseline if feature.cold_start else learned
        for feature, learned, baseline in zip(
            forecast_features,
            learned_predictions,
            baseline_predictions,
            strict=True,
        )
    )
    return R3BDemandOriginRun(
        name=name,
        issue_time=issue_time,
        training=training,
        forecast_features=forecast_features,
        model=model,
        raw_predictions=candidate_predictions,
        baseline_predictions=baseline_predictions,
        feature_artifact=feature_artifact,
        model_artifact=model_artifact,
    )


def _persist_and_evaluate_origin(
    snapshot: R3BDataSnapshot,
    origin: R3BDemandOriginRun,
    *,
    candidate: Sequence[R3BPredictionRecord],
    config: R3BConfig,
    output_dir: Path,
    evaluation_cutoff: datetime,
) -> tuple[R3BEvaluationResult, ScientificArtifactRef, R3BEvaluationBundle]:
    forecast_artifact = write_immutable_json_manifest(
        output_dir,
        dataset=f"r3b.forecasts.{origin.name}",
        manifest={
            "baseline": [asdict(row) for row in origin.baseline_predictions],
            "candidate": [asdict(row) for row in candidate],
            "contract": "nexus.r3b.persisted-forecast-bundle.v1",
            "origin": origin.issue_time,
        },
    )
    if not forecast_artifact.path.is_file():
        raise R3BGateError("forecast artifact must exist before truth access")
    truth_bundle = get_evaluation_truth_after_persistence(
        snapshot,
        forecast_artifact_path=forecast_artifact.path,
        evaluation_cutoff=evaluation_cutoff,
    )
    if (
        truth_bundle.forecast_artifact_sha256 != forecast_artifact.content_sha256
        or truth_bundle.forecast_artifact_file_sha256 != forecast_artifact.file_sha256
        or truth_bundle.forecast_artifact_path != forecast_artifact.path.resolve()
    ):
        raise R3BGateError("truth access receipt does not match the persisted forecast artifact")
    persisted_candidate = _persisted_prediction_records(truth_bundle.candidate)
    persisted_baseline = _persisted_prediction_records(truth_bundle.baseline)
    eligible_truth = tuple(
        R3BTruthRecord.from_evaluation_truth(row)
        for row in truth_bundle.rows
        if row.is_active and not row.stockout_censored
    )
    eligible_keys = {row.forecast_key for row in eligible_truth}
    eligible_candidate = tuple(
        row for row in persisted_candidate if row.forecast_key in eligible_keys
    )
    eligible_baseline = tuple(
        row for row in persisted_baseline if row.forecast_key in eligible_keys
    )
    evaluation = evaluate_candidate_against_baseline(
        eligible_candidate,
        eligible_baseline,
        eligible_truth,
        config=config,
    )
    return evaluation, forecast_artifact, truth_bundle


def _fit_calibrators(
    evaluation: R3BEvaluationResult,
    config: R3BConfig,
) -> dict[int, R3BQuantileCalibrator]:
    calibrators: dict[int, R3BQuantileCalibrator] = {}
    for horizon in config.forecast_contract.horizons_days:
        rows = tuple(
            row
            for row in evaluation.rows
            if row.truth.horizon_days == horizon and not row.candidate.cold_start
        )
        calibrators[horizon] = fit_quantile_calibrator(
            tuple(float(row.truth.actual) for row in rows),
            tuple(R3BQuantilePrediction.from_values(row.candidate.quantiles) for row in rows),
            config.calibration.minimum_support_per_horizon,
        )
    return calibrators


def _apply_calibrators(
    origin: R3BDemandOriginRun,
    calibrators: Mapping[int, R3BQuantileCalibrator],
) -> tuple[R3BPredictionRecord, ...]:
    calibrated: list[R3BPredictionRecord] = []
    for feature, raw, baseline in zip(
        origin.forecast_features,
        origin.raw_predictions,
        origin.baseline_predictions,
        strict=True,
    ):
        calibrator = calibrators[feature.horizon_days]
        raw_prediction = R3BQuantilePrediction.from_values(raw.quantiles)
        if feature.cold_start:
            prediction = R3BQuantilePrediction.from_values(baseline.quantiles)
        else:
            prediction = (
                apply_quantile_calibration(calibrator, (raw_prediction,))[0]
                if calibrator.assessed
                else raw_prediction
            )
        calibrated.append(
            R3BPredictionRecord.from_quantile_prediction(
                sku_id=feature.sku_id,
                location_id=feature.location_id,
                target_date=feature.target_date,
                horizon_days=feature.horizon_days,
                prediction=prediction,
                cold_start=feature.cold_start,
            )
        )
    return tuple(calibrated)


def _lead_time_wis(actual: float | Decimal, prediction: R3BQuantilePrediction) -> Decimal:
    return weighted_interval_score(
        actual,
        median=prediction.q50,
        lower_80=prediction.q10,
        upper_80=prediction.q90,
        lower_95=prediction.q025,
        upper_95=prediction.q975,
    )


def _persisted_utc(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise R3BGateError(f"{field} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise R3BGateError(f"{field} must be a valid ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise R3BGateError(f"{field} must include an explicit timezone")
    return parsed.astimezone(UTC)


def _lead_time_gate(
    snapshot: R3BDataSnapshot,
    *,
    config: R3BConfig,
    calibration_origin: datetime,
    final_origin: datetime,
    output_dir: Path,
) -> tuple[
    R3BLeadTimeEvaluation,
    R3BQuantileEnsemble,
    ScientificArtifactRef,
    ScientificArtifactRef,
]:
    training = build_lead_time_training_examples(
        snapshot,
        config=config,
        origin_time=calibration_origin,
    )
    final = build_lead_time_training_examples(
        snapshot,
        config=config,
        origin_time=final_origin,
    )
    if len(training.examples) < config.lead_time_model.minimum_training_labels:
        raise R3BGateError("lead-time training support is below the frozen minimum")
    training_ids = {row.features.order_id for row in training.examples}
    holdout = tuple(row for row in final.examples if row.features.order_id not in training_ids)
    if not holdout:
        raise R3BGateError("lead-time temporal holdout contains no newly mature labels")
    model = fit_quantile_ensemble(
        _matrix(training.feature_matrix()),
        training.targets(),
        training.feature_columns,
        config,
    )
    candidate = predict_quantiles(
        model,
        _matrix(tuple(row.features.feature_vector() for row in holdout)),
    )
    baseline: R3BEmpiricalQuantileBaseline = fit_empirical_quantile_baseline(
        training.targets(),
        config.forecast_contract.quantiles,
        config.lead_time_model.minimum_training_labels,
    )
    baseline_predictions = predict_empirical_quantiles(baseline, len(holdout))
    holdout_feature_rows = tuple(
        {
            "feature_values": dict(
                zip(training.feature_columns, row.features.feature_vector(), strict=True)
            ),
            "order_id": row.features.order_id,
            "ordered_at": row.features.ordered_at,
            "ordered_available_at": row.features.ordered_available_at,
            "supplier_id": row.features.supplier_id,
        }
        for row in holdout
    )
    holdout_feature_sha256 = canonical_rows_sha256(holdout_feature_rows)
    forecast_rows = tuple(
        {
            "baseline": dict(zip(_QUANTILE_NAMES, baseline_prediction.values, strict=True)),
            "candidate": dict(zip(_QUANTILE_NAMES, candidate_prediction.values, strict=True)),
            **feature_row,
            "prediction_as_of": max(
                calibration_origin,
                row.features.ordered_available_at,
            ),
        }
        for row, feature_row, candidate_prediction, baseline_prediction in zip(
            holdout,
            holdout_feature_rows,
            candidate,
            baseline_predictions,
            strict=True,
        )
    )
    forecast_artifact = write_immutable_json_manifest(
        output_dir,
        dataset="r3b.forecasts.lead_time_temporal_holdout",
        manifest={
            "contract": "nexus.r3b.lead-time-forecast-bundle.v1",
            "feature_columns": training.feature_columns,
            "holdout_feature_sha256": holdout_feature_sha256,
            "model_frozen_at": calibration_origin,
            "model_training_identity": model.training_identity,
            "rows": forecast_rows,
            "training_dataset_sha256": training.dataset_sha256,
        },
    )
    if (
        not forecast_artifact.path.is_file()
        or file_sha256(forecast_artifact.path) != forecast_artifact.file_sha256
    ):
        raise R3BGateError("lead-time forecast artifact was not durably persisted")
    persisted_payload = json.loads(forecast_artifact.path.read_text(encoding="utf-8"))
    expected_root_fields = {
        "contract",
        "feature_columns",
        "holdout_feature_sha256",
        "model_frozen_at",
        "model_training_identity",
        "rows",
        "training_dataset_sha256",
    }
    if not isinstance(persisted_payload, dict) or set(persisted_payload) != expected_root_fields:
        raise R3BGateError("lead-time forecast artifact violates its frozen contract")
    try:
        persisted_model_frozen_at = _persisted_utc(
            persisted_payload["model_frozen_at"], "model_frozen_at"
        )
        persisted_feature_columns = tuple(cast(list[str], persisted_payload["feature_columns"]))
    except (AttributeError, TypeError, ValueError) as error:
        raise R3BGateError("lead-time persisted model lineage is invalid") from error
    if (
        persisted_payload["contract"] != "nexus.r3b.lead-time-forecast-bundle.v1"
        or persisted_feature_columns != training.feature_columns
        or persisted_payload["holdout_feature_sha256"] != holdout_feature_sha256
        or persisted_model_frozen_at != calibration_origin.astimezone(UTC)
        or persisted_payload["model_training_identity"] != model.training_identity
        or persisted_payload["training_dataset_sha256"] != training.dataset_sha256
        or not isinstance(persisted_payload["rows"], list)
    ):
        raise R3BGateError("lead-time forecast lineage differs from its fitted inputs")
    persisted_rows = cast(list[dict[str, object]], persisted_payload["rows"])
    if len(persisted_rows) != len(holdout):
        raise R3BGateError("lead-time persisted forecast count differs from the holdout")
    expected_row_fields = {
        "baseline",
        "candidate",
        "feature_values",
        "order_id",
        "ordered_at",
        "ordered_available_at",
        "prediction_as_of",
        "supplier_id",
    }

    evaluated_rows: list[R3BLeadTimeEvaluationRow] = []
    persisted_feature_rows: list[dict[str, object]] = []
    for example, persisted in zip(holdout, persisted_rows, strict=True):
        features = example.features
        if set(persisted) != expected_row_fields or (
            persisted.get("order_id") != features.order_id
            or persisted.get("supplier_id") != features.supplier_id
        ):
            raise R3BGateError("lead-time persisted forecast keys differ from the holdout")
        try:
            prediction_as_of = _persisted_utc(persisted["prediction_as_of"], "prediction_as_of")
            ordered_at = _persisted_utc(persisted["ordered_at"], "ordered_at")
            ordered_available_at = _persisted_utc(
                persisted["ordered_available_at"], "ordered_available_at"
            )
            candidate_values = cast(Mapping[str, Decimal | float | int], persisted["candidate"])
            baseline_values = cast(Mapping[str, Decimal | float | int], persisted["baseline"])
            persisted_feature_values = cast(
                Mapping[str, Decimal | float | int | None], persisted["feature_values"]
            )
            persisted_candidate = R3BQuantilePrediction.from_values(
                candidate_values[name] for name in _QUANTILE_NAMES
            )
            persisted_baseline = R3BQuantilePrediction.from_values(
                baseline_values[name] for name in _QUANTILE_NAMES
            )
        except (KeyError, TypeError, ValueError) as error:
            raise R3BGateError("lead-time persisted forecast row is invalid") from error
        expected_feature_values = dict(
            zip(training.feature_columns, features.feature_vector(), strict=True)
        )
        if (
            set(candidate_values) != set(_QUANTILE_NAMES)
            or set(baseline_values) != set(_QUANTILE_NAMES)
            or set(persisted_feature_values) != set(training.feature_columns)
            or persisted_feature_values != expected_feature_values
            or ordered_at != features.ordered_at
            or ordered_available_at != features.ordered_available_at
            or prediction_as_of != max(calibration_origin, features.ordered_available_at)
        ):
            raise R3BGateError("lead-time persisted row differs from point-in-time features")
        persisted_feature_rows.append(
            {
                "feature_values": persisted["feature_values"],
                "order_id": persisted["order_id"],
                "ordered_at": persisted["ordered_at"],
                "ordered_available_at": persisted["ordered_available_at"],
                "supplier_id": persisted["supplier_id"],
            }
        )
        actual = Decimal(str(example.target_actual_lead_time_days))
        evaluated_rows.append(
            R3BLeadTimeEvaluationRow(
                order_id=features.order_id,
                supplier_id=features.supplier_id,
                prediction_as_of=prediction_as_of,
                label_available_at=example.label_available_at,
                actual_lead_time_days=actual,
                candidate=persisted_candidate,
                baseline=persisted_baseline,
                candidate_wis=_lead_time_wis(actual, persisted_candidate),
                baseline_wis=_lead_time_wis(actual, persisted_baseline),
            )
        )
    if canonical_rows_sha256(persisted_feature_rows) != holdout_feature_sha256:
        raise R3BGateError("lead-time persisted feature hash failed reconstruction")
    rows = tuple(evaluated_rows)
    candidate_wis = sum((row.candidate_wis for row in rows), Decimal(0)) / Decimal(len(rows))
    baseline_wis = sum((row.baseline_wis for row in rows), Decimal(0)) / Decimal(len(rows))
    relative = None if baseline_wis == 0 else (baseline_wis - candidate_wis) / baseline_wis
    evaluation = R3BLeadTimeEvaluation(
        training_label_count=len(training.examples),
        holdout_label_count=len(holdout),
        right_censored_excluded_at_final=final.excluded_right_censored_count,
        candidate_wis=candidate_wis,
        baseline_wis=baseline_wis,
        relative_wis_improvement=relative,
        baseline_status=baseline.status,
        candidate_training_identity=model.training_identity,
        model_frozen_at=calibration_origin,
        holdout_feature_sha256=holdout_feature_sha256,
        forecast_artifact_sha256=forecast_artifact.content_sha256,
        forecast_artifact_file_sha256=forecast_artifact.file_sha256,
        rows=rows,
    )
    evaluation_artifact = write_immutable_json_manifest(
        output_dir,
        dataset="r3b.evaluation.lead_time",
        manifest={
            "contract": "nexus.r3b.lead-time-temporal-holdout.v1",
            "evaluation": asdict(evaluation),
            "forecast_artifact": _artifact_payload(forecast_artifact, output_dir),
            "model": model.to_dict(),
            "training_dataset_sha256": training.dataset_sha256,
        },
    )
    return evaluation, model, forecast_artifact, evaluation_artifact


def _hierarchy(
    predictions: Sequence[R3BPredictionRecord],
    issue_time: datetime,
) -> tuple[HierarchicalQuantileForecast, ...]:
    bottom = tuple(
        BottomLevelQuantileForecast(
            issue_time=issue_time,
            target_date=row.target_date,
            sku_id=row.sku_id,
            location_id=row.location_id,
            quantile=quantile,
            predicted_quantity=value,
        )
        for row in predictions
        for quantile, value in zip(_QUANTILE_VALUES, row.quantiles, strict=True)
    )
    result = aggregate_quantile_forecasts(bottom)
    validate_bottom_up_coherence(result)
    return result


def _platform_name() -> str:
    system_name = platform.system().lower()
    runner_os = os.environ.get("RUNNER_OS", "").strip().lower()
    if runner_os and runner_os != system_name:
        raise R3BGateError("RUNNER_OS does not match the observed runtime platform")
    return (
        f"{system_name}-{platform.machine().lower()}-"
        f"python-{sys.version_info.major}.{sys.version_info.minor}"
    )


def run_r3b_once(
    *,
    output_dir: Path,
    config_path: Path = DEFAULT_R3B_CONFIG_PATH,
) -> R3BRunResult:
    """Materialize one isolated R3B run with status ``MATERIALIZED_UNVERIFIED``."""

    _configure_bounded_native_runtime()
    root = output_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    config = load_r3b_config(config_path)
    source = generate_r2c_profile(
        root / "source",
        config=load_r2c_profile(_R2C_SMOKE_CONFIG),
    )
    snapshot = register_r2c_package(source, config=config)
    origins = {origin.role: origin for origin in config.evaluation.rolling_origins}
    diagnostic_origin = origins["diagnostic"].issue_time
    calibration_origin = origins["calibration"].issue_time
    final_origin = origins["final_stress"].issue_time

    diagnostic = _fit_demand_origin(
        snapshot,
        config=config,
        name="diagnostic",
        issue_time=diagnostic_origin,
        output_dir=root,
    )
    calibration = _fit_demand_origin(
        snapshot,
        config=config,
        name="calibration",
        issue_time=calibration_origin,
        output_dir=root,
    )
    final = _fit_demand_origin(
        snapshot,
        config=config,
        name="final_stress",
        issue_time=final_origin,
        output_dir=root,
    )
    (
        diagnostic_evaluation,
        diagnostic_forecast_artifact,
        diagnostic_truth_bundle,
    ) = _persist_and_evaluate_origin(
        snapshot,
        diagnostic,
        candidate=diagnostic.raw_predictions,
        config=config,
        output_dir=root,
        evaluation_cutoff=final_origin,
    )
    (
        calibration_evaluation,
        calibration_forecast_artifact,
        calibration_truth_bundle,
    ) = _persist_and_evaluate_origin(
        snapshot,
        calibration,
        candidate=calibration.raw_predictions,
        config=config,
        output_dir=root,
        evaluation_cutoff=final_origin,
    )
    calibrators = _fit_calibrators(calibration_evaluation, config)
    final_predictions = _apply_calibrators(final, calibrators)
    final_evaluation, final_forecast_artifact, final_truth_bundle = _persist_and_evaluate_origin(
        snapshot,
        final,
        candidate=final_predictions,
        config=config,
        output_dir=root,
        evaluation_cutoff=snapshot.manifest.generated_through,
    )
    persisted_final_predictions = _persisted_prediction_records(final_truth_bundle.candidate)
    hierarchy_rows = _hierarchy(persisted_final_predictions, final_origin)
    hierarchy_artifact = write_immutable_json_manifest(
        root,
        dataset="r3b.forecasts.final_stress_hierarchy",
        manifest={
            "contract": "nexus.r3b.bottom-up-marginal-quantiles.v1",
            "joint_distribution_calibration_claimed": False,
            "rows": [asdict(row) for row in hierarchy_rows],
        },
    )
    (
        lead_evaluation,
        lead_model,
        lead_forecast_artifact,
        lead_evaluation_artifact,
    ) = _lead_time_gate(
        snapshot,
        config=config,
        calibration_origin=calibration_origin,
        final_origin=final_origin,
        output_dir=root,
    )
    calibration_sufficient = all(item.assessed for item in calibrators.values())
    promotion = decide_promotion(
        final_evaluation,
        config=config,
        evidence_sufficient=calibration_sufficient,
        gate_checks_passed=True,
    )
    evaluation_artifact = write_immutable_json_manifest(
        root,
        dataset="r3b.evaluation.demand",
        manifest={
            "calibration": asdict(calibration_evaluation),
            "calibrators": {
                str(horizon): asdict(calibrator)
                for horizon, calibrator in sorted(calibrators.items())
            },
            "contract": "nexus.r3b.rolling-origin-evaluation.v1",
            "diagnostic": asdict(diagnostic_evaluation),
            "final_stress": asdict(final_evaluation),
            "forecast_lineage": {
                "calibration": {
                    "content_sha256": calibration_truth_bundle.forecast_artifact_sha256,
                    "evaluation_cutoff": calibration_truth_bundle.evaluation_cutoff,
                    "file_sha256": calibration_truth_bundle.forecast_artifact_file_sha256,
                },
                "diagnostic": {
                    "content_sha256": diagnostic_truth_bundle.forecast_artifact_sha256,
                    "evaluation_cutoff": diagnostic_truth_bundle.evaluation_cutoff,
                    "file_sha256": diagnostic_truth_bundle.forecast_artifact_file_sha256,
                },
                "final_stress": {
                    "content_sha256": final_truth_bundle.forecast_artifact_sha256,
                    "evaluation_cutoff": final_truth_bundle.evaluation_cutoff,
                    "file_sha256": final_truth_bundle.forecast_artifact_file_sha256,
                },
            },
        },
    )
    promotion_artifact = write_immutable_json_manifest(
        root,
        dataset="r3b.promotion.decision",
        manifest={
            "champion_after_gate": config.promotion.current_champion,
            "champion_before_gate": config.promotion.current_champion,
            "decision": asdict(promotion),
        },
    )
    key_hash = canonical_rows_sha256(
        [
            {
                "horizon_days": row.horizon_days,
                "location_id": row.location_id,
                "sku_id": row.sku_id,
                "target_date": row.target_date,
            }
            for row in persisted_final_predictions
        ]
    )
    source_bundle_sha256 = _source_bundle_sha256()
    semantic_hashes = {
        "calibration_training": calibration.model.training_identity,
        "ci_workflow": _canonical_text_sha256(_PROJECT_ROOT / ".github" / "workflows" / "ci.yml"),
        "config": r3b_config_sha256(config),
        "dependency_lock": _canonical_text_sha256(_PROJECT_ROOT / "uv.lock"),
        "diagnostic_training": diagnostic.model.training_identity,
        "execution_revision": _execution_revision_sha256(source_bundle_sha256),
        "final_training": final.model.training_identity,
        "forecast_keys": key_hash,
        "implementation_source_bundle": source_bundle_sha256,
        "lead_time_holdout_features": lead_evaluation.holdout_feature_sha256,
        "lead_time_training": lead_model.training_identity,
        "lead_time_holdout_keys": canonical_rows_sha256(
            [
                {
                    "order_id": row.order_id,
                    "prediction_as_of": row.prediction_as_of,
                    "supplier_id": row.supplier_id,
                }
                for row in lead_evaluation.rows
            ]
        ),
        "r2c_registration": snapshot.registration.registration_sha256,
    }
    tracking = log_r3b_challenger(
        root / "tracking",
        experiment_name="nexus-r3b-synthetic",
        run_name="r3b-final-stress",
        registered_model_name="nexus-r3b-learned-challengers",
        params={
            "classification": "synthetic",
            "demand_feature_count": len(final.training.feature_columns),
            "demand_training_rows": len(final.training.examples),
            "imputation": "zero_with_history_count_signal",
            "lead_time_feature_count": len(config.feature_contract.allowed_lead_time_features),
            "lead_time_training_rows": lead_evaluation.training_label_count,
            "seed": config.seed,
        },
        metrics={
            "demand_baseline_wis": float(final_evaluation.baseline_wis),
            "demand_candidate_wis": float(final_evaluation.candidate_wis),
            "lead_time_baseline_wis": float(lead_evaluation.baseline_wis),
            "lead_time_candidate_wis": float(lead_evaluation.candidate_wis),
        },
        tags={
            "classification": "synthetic",
            "gate": "R3B",
            "promotion_status": promotion.status,
        },
        semantic_hashes=semantic_hashes,
        model_json=json.dumps(
            {"demand": final.model.to_dict(), "lead_time": lead_model.to_dict()},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
        evidence_paths=(
            evaluation_artifact.path,
            lead_forecast_artifact.path,
            lead_evaluation_artifact.path,
            promotion_artifact.path,
        ),
    )
    tracking_artifact = write_immutable_json_manifest(
        root,
        dataset="r3b.tracking.receipt",
        manifest=tracking.audit_payload(),
    )
    level_counts: dict[str, int] = defaultdict(int)
    for hierarchy_row in hierarchy_rows:
        level_counts[hierarchy_row.level.value] += 1
    lead_descriptors = {
        "lead_time.baseline_status": lead_evaluation.baseline_status,
        "lead_time.event_time_rule": "max_model_freeze_and_order_availability_before_label",
        "lead_time.model_frozen_at": lead_evaluation.model_frozen_at.isoformat().replace(
            "+00:00", "Z"
        ),
    }
    lead_numbers: dict[str, Decimal | int | float] = {
        "lead_time.baseline_wis": lead_evaluation.baseline_wis,
        "lead_time.candidate_wis": lead_evaluation.candidate_wis,
        "lead_time.holdout_labels": lead_evaluation.holdout_label_count,
        "lead_time.training_labels": lead_evaluation.training_label_count,
    }
    for index, lead_row in enumerate(lead_evaluation.rows):
        prefix = f"lead_time.rows.{index:06d}"
        lead_descriptors.update(
            {
                f"{prefix}.label_available_at": lead_row.label_available_at.isoformat().replace(
                    "+00:00", "Z"
                ),
                f"{prefix}.order_id": lead_row.order_id,
                f"{prefix}.prediction_as_of": lead_row.prediction_as_of.isoformat().replace(
                    "+00:00", "Z"
                ),
                f"{prefix}.supplier_id": lead_row.supplier_id,
            }
        )
        lead_numbers[f"{prefix}.actual_lead_time_days"] = lead_row.actual_lead_time_days
        lead_numbers[f"{prefix}.candidate_wis"] = lead_row.candidate_wis
        lead_numbers[f"{prefix}.baseline_wis"] = lead_row.baseline_wis
        for role, prediction in (
            ("candidate", lead_row.candidate),
            ("baseline", lead_row.baseline),
        ):
            for quantile_name, value in zip(
                _QUANTILE_NAMES,
                prediction.values,
                strict=True,
            ):
                lead_numbers[f"{prefix}.{role}.{quantile_name}"] = value
    calibration_descriptors = {
        f"calibration.horizon_{horizon:02d}.status": calibrator.status
        for horizon, calibrator in sorted(calibrators.items())
    }
    calibration_numbers = {
        f"calibration.horizon_{horizon:02d}.offset.{quantile_name}": offset
        for horizon, calibrator in sorted(calibrators.items())
        for quantile_name, offset in zip(_QUANTILE_NAMES, calibrator.offsets, strict=True)
    }
    platform_receipt = build_platform_receipt(
        platform=_platform_name(),
        semantic_hashes=semantic_hashes,
        evaluation=final_evaluation,
        promotion=promotion,
        extra_descriptors={
            "demand.cold_start_fallback": config.forecast_contract.cold_start_fallback,
            "hierarchy.joint_distribution_calibration_claimed": "false",
            "hierarchy.method": config.reconciliation.method,
            "runtime.runner_os_attested": (
                "true" if os.environ.get("RUNNER_OS", "").strip() else "local_unattested"
            ),
            "tracking.registered": "true",
            **calibration_descriptors,
            **lead_descriptors,
        },
        extra_numeric_values={
            "calibration.horizon_07_support": calibrators[7].support,
            "calibration.horizon_14_support": calibrators[14].support,
            "calibration.horizon_28_support": calibrators[28].support,
            "diagnostic.baseline_wis": diagnostic_evaluation.baseline_wis,
            "diagnostic.candidate_wis": diagnostic_evaluation.candidate_wis,
            "hierarchy.network_rows": level_counts["network"],
            "hierarchy.sku_location_rows": level_counts["sku_location"],
            "hierarchy.sku_rows": level_counts["sku"],
            **calibration_numbers,
            **lead_numbers,
        },
    )
    receipt_artifact = write_immutable_json_manifest(
        root,
        dataset="r3b.receipts.platform",
        manifest=platform_receipt.to_dict(),
    )
    artifacts = (
        diagnostic.feature_artifact,
        diagnostic.model_artifact,
        calibration.feature_artifact,
        calibration.model_artifact,
        final.feature_artifact,
        final.model_artifact,
        diagnostic_forecast_artifact,
        calibration_forecast_artifact,
        final_forecast_artifact,
        hierarchy_artifact,
        lead_forecast_artifact,
        lead_evaluation_artifact,
        evaluation_artifact,
        promotion_artifact,
        tracking_artifact,
        receipt_artifact,
    )
    semantic_manifest = write_immutable_json_manifest(
        root,
        dataset="r3b.semantic_manifest",
        manifest={
            "artifacts": [
                _artifact_payload(item, root) for item in sorted(artifacts, key=lambda x: x.dataset)
            ],
            "classification": "synthetic",
            "config_sha256": r3b_config_sha256(config),
            "contract": "nexus.r3b.semantic-manifest.v1",
            "promotion_status": promotion.status,
            "r2c_registration_sha256": snapshot.registration.registration_sha256,
            "status": "MATERIALIZED_UNVERIFIED",
        },
    )
    summary_path = root / "r3b-run-summary.json"
    _atomic_json(
        summary_path,
        {
            "classification": "synthetic",
            "final_baseline_wis": str(final_evaluation.baseline_wis),
            "final_candidate_wis": str(final_evaluation.candidate_wis),
            "mlflow_run_id": tracking.run_id,
            "platform_receipt_sha256": receipt_artifact.content_sha256,
            "promotion_status": promotion.status,
            "semantic_manifest": semantic_manifest.path.as_posix(),
            "status": "MATERIALIZED_UNVERIFIED",
        },
    )
    return R3BRunResult(
        config=config,
        output_dir=root,
        registration=snapshot.registration,
        diagnostic_evaluation=diagnostic_evaluation,
        calibration_evaluation=calibration_evaluation,
        final_evaluation=final_evaluation,
        calibrators=calibrators,
        lead_time_evaluation=lead_evaluation,
        promotion=promotion,
        hierarchy_rows=hierarchy_rows,
        tracking=tracking,
        platform_receipt=platform_receipt,
        artifacts=artifacts,
        semantic_manifest=semantic_manifest,
        summary_path=summary_path,
    )


def _failure_manifest(root: Path, detail: str) -> ScientificArtifactRef:
    return write_immutable_json_manifest(
        root,
        dataset="r3b.gate",
        manifest={
            "contract": "nexus.r3b.gate.v1",
            "detail": detail,
            "status": "FAIL",
        },
    )


def verify_r3b(
    *,
    artifact_root: Path,
    config_path: Path = DEFAULT_R3B_CONFIG_PATH,
) -> R3BVerificationResult:
    """Run two fresh scientific replays and emit a normalized local CI receipt."""

    root = artifact_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    first: R3BRunResult | None = None
    second: R3BRunResult | None = None
    resource_evidence: R3BResourceEvidence | None = None
    final_promotion: R3BPromotionDecision | None = None
    checks: list[R3BVerificationCheck] = []
    receipt_path: Path | None = None
    try:
        config = load_r3b_config(config_path)
        with R3BResourceMonitor(
            config,
            governed_artifact_roots=(root,),
        ) as resource_monitor:
            first = run_r3b_once(output_dir=root / "run-a", config_path=config_path)
            second = run_r3b_once(output_dir=root / "run-b", config_path=config_path)
            exact_receipt = first.platform_receipt.to_json() == second.platform_receipt.to_json()
            checks.append(
                R3BVerificationCheck(
                    "same_platform_exact_replay",
                    exact_receipt,
                    "normalized scientific receipts are byte-identical",
                )
            )
            comparison = assert_platform_receipts_match(
                first.platform_receipt,
                second.platform_receipt,
                config=first.config,
            )
            checks.append(
                R3BVerificationCheck(
                    "numeric_tolerance_comparator",
                    comparison.passed,
                    f"compared={comparison.compared_numeric_values}; "
                    f"max_abs_difference={comparison.max_absolute_difference}",
                )
            )
            registration_equal = (
                first.registration.registration_sha256 == second.registration.registration_sha256
            )
            checks.append(
                R3BVerificationCheck(
                    "registered_r2c_identity",
                    registration_equal,
                    first.registration.registration_sha256,
                )
            )
            tracking_complete = (
                first.tracking.tracking_store.is_file()
                and first.tracking.registered_model_version >= 1
                and first.tracking.readback_verified
                and file_sha256(first.tracking.tracking_store)
                == first.tracking.tracking_store_sha256
                and second.tracking.tracking_store.is_file()
                and second.tracking.readback_verified
                and file_sha256(second.tracking.tracking_store)
                == second.tracking.tracking_store_sha256
            )
            checks.append(
                R3BVerificationCheck(
                    "local_mlflow_tracking_and_registry",
                    tracking_complete,
                    f"model_version={first.tracking.registered_model_version}",
                )
            )
            receipt_path = root / "r3b-platform-receipt.json"
            _atomic_json(receipt_path, first.platform_receipt.to_dict())
        resource_evidence = resource_monitor.evidence
        checks.append(
            R3BVerificationCheck(
                "complete_process_tree_resource_budget",
                resource_evidence.passed,
                (
                    f"wall={resource_evidence.wall_time_seconds:.3f}s; "
                    f"rss={resource_evidence.peak_process_tree_rss_mib:.1f}MiB; "
                    f"threads={resource_evidence.peak_process_tree_native_threads}; "
                    f"artifacts={resource_evidence.governed_artifact_bytes} bytes"
                ),
            )
        )
        technical_checks_passed = all(check.passed for check in checks)
        evidence_sufficient = all(item.assessed for item in first.calibrators.values()) and all(
            item.assessed for item in second.calibrators.values()
        )
        final_promotion = decide_promotion(
            first.final_evaluation,
            config=first.config,
            evidence_sufficient=evidence_sufficient,
            gate_checks_passed=technical_checks_passed,
        )
        checks.append(
            R3BVerificationCheck(
                "engineering_complete_without_auto_promotion",
                final_promotion.engineering_complete and not final_promotion.promotion_applied,
                final_promotion.status,
            )
        )
        passed = all(check.passed for check in checks)
        status = "R3B_ENGINEERING_PASS_SYNTHETIC_PROMOTION_BLOCKED" if passed else "R3B_GATE_FAIL"
        gate_manifest = write_immutable_json_manifest(
            root,
            dataset="r3b.gate",
            manifest={
                "checks": [asdict(check) for check in checks],
                "classification": "synthetic",
                "contract": "nexus.r3b.gate.v1",
                "full_g3_status": "OPEN",
                "promotion_applied": False,
                "promotion_status": final_promotion.status,
                "resources": resource_evidence.to_dict(),
                "status": status,
            },
        )
    except Exception as error:
        passed = False
        status = "R3B_GATE_FAIL"
        checks.append(R3BVerificationCheck("setup_or_execution", False, str(error)))
        gate_manifest = _failure_manifest(root, str(error))
    summary_path = root / "r3b-verification-summary.json"
    _atomic_json(
        summary_path,
        {
            "checks": [asdict(check) for check in checks],
            "classification": "synthetic",
            "full_g3_status": "OPEN",
            "gate_manifest": gate_manifest.path.as_posix(),
            "passed": passed,
            "platform_receipt": None if receipt_path is None else receipt_path.as_posix(),
            "promotion_status": None if final_promotion is None else final_promotion.status,
            "resources": (None if resource_evidence is None else resource_evidence.to_dict()),
            "status": status,
        },
    )
    return R3BVerificationResult(
        passed=passed,
        status=status,
        checks=tuple(checks),
        first_run=first,
        second_run=second,
        resource_evidence=resource_evidence,
        final_promotion=final_promotion,
        platform_receipt_path=receipt_path,
        gate_manifest=gate_manifest,
        summary_path=summary_path,
    )


def compare_platform_receipt_files(
    left_path: Path,
    right_path: Path,
    *,
    config_path: Path = DEFAULT_R3B_CONFIG_PATH,
) -> dict[str, object]:
    """Strictly compare normalized Linux/Windows receipts for the CI join job."""

    left = R3BPlatformReceipt.from_json(left_path.read_text(encoding="utf-8"))
    right = R3BPlatformReceipt.from_json(right_path.read_text(encoding="utf-8"))
    platform_families = {
        "linux"
        if item.platform.lower().startswith("linux-")
        else "windows"
        if item.platform.lower().startswith("windows-")
        else "unsupported"
        for item in (left, right)
    }
    if platform_families != {"linux", "windows"}:
        raise R3BGateError(
            "cross-platform CI comparison requires exactly one Linux and one Windows receipt"
        )
    comparison = assert_platform_receipts_match(
        left,
        right,
        config=load_r3b_config(config_path),
    )
    return asdict(comparison)


__all__ = [
    "R3BDemandOriginRun",
    "R3BGateError",
    "R3BLeadTimeEvaluation",
    "R3BLeadTimeEvaluationRow",
    "R3BRunResult",
    "R3BVerificationCheck",
    "R3BVerificationResult",
    "compare_platform_receipt_files",
    "run_r3b_once",
    "verify_r3b",
]
