"""Executable R3A point-in-time probabilistic-intelligence foundation."""

from __future__ import annotations

import ctypes
import json
import os
import platform
import sys
import threading
import time as clock
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, getcontext
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa

from nexus.data.r2_models import R2RebuildResult
from nexus.modeling.gold_adapter import (
    R3DemandObservation,
    R3DemandSnapshot,
    adapt_r2_demand_history,
)
from nexus.modeling.r3_artifacts import (
    EvaluationArtifactRow,
    ScientificArtifactRef,
    canonical_json_sha256,
    write_evaluation_rows,
    write_forecast_rows,
    write_immutable_json_manifest,
)
from nexus.modeling.r3_baseline import (
    ProbabilisticBaselineForecast,
    forecast_probabilistic_seasonal_naive,
    join_matured_labels,
    rolling_probabilistic_seasonal_naive,
)
from nexus.modeling.r3_contracts import R3AConfig, load_r3a_config
from nexus.modeling.r3_features import (
    FEATURE_ARTIFACT_SCHEMA,
    R3PointInTimeFeatureRow,
    build_point_in_time_features,
    feature_artifact_rows,
)
from nexus.modeling.r3_hierarchy import (
    BottomLevelActual,
    BottomLevelQuantileForecast,
    HierarchicalActual,
    HierarchicalQuantileForecast,
    HierarchyLevel,
    aggregate_actuals,
    aggregate_quantile_forecasts,
    validate_bottom_up_coherence,
)
from nexus.modeling.r3_metrics import (
    BootstrapInterval,
    ForecastMetricRow,
    ProbabilisticMetricSummary,
    block_bootstrap_95,
    mean_weighted_interval_score,
    summarize_probabilistic_metrics,
)
from nexus.modeling.seasonal_naive import DailyDemandObservation
from nexus.r2 import run_r2_rebuild
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import ContentAddressedArtifact, file_sha256, write_immutable_parquet

_QUANTILES = (
    Decimal("0.025"),
    Decimal("0.10"),
    Decimal("0.50"),
    Decimal("0.90"),
    Decimal("0.975"),
)
_MODEL_NAME = "seasonal_naive_empirical_intervals"
_MODEL_VERSION = "1.0.0"

LABEL_ARTIFACT_SCHEMA = pa.schema(
    [
        pa.field("issue_time", pa.string(), nullable=False),
        pa.field("target_date", pa.string(), nullable=False),
        pa.field("sku_location_key", pa.string(), nullable=False),
        pa.field("sku_id", pa.string(), nullable=False),
        pa.field("location_id", pa.string(), nullable=False),
        pa.field("actual_quantity", pa.int64(), nullable=False),
        pa.field("label_cutoff", pa.string(), nullable=False),
    ],
    metadata={b"nexus.contract": b"r3.matured-evaluation-labels.v1"},
)

type MaseScaleKey = tuple[str, str, datetime]


@dataclass(frozen=True, slots=True)
class R3ARunResult:
    """Governed scientific artifacts and measurements from one R3A run."""

    config: R3AConfig
    snapshot: R3DemandSnapshot
    feature_rows: tuple[R3PointInTimeFeatureRow, ...]
    forecasts: tuple[ProbabilisticBaselineForecast, ...]
    metric_rows: tuple[ForecastMetricRow, ...]
    hierarchical_forecasts: tuple[HierarchicalQuantileForecast, ...]
    hierarchical_actuals: tuple[HierarchicalActual, ...]
    evaluation_rows: tuple[EvaluationArtifactRow, ...]
    overall_metrics: ProbabilisticMetricSummary
    stress_metrics: ProbabilisticMetricSummary
    overall_wis_bootstrap: BootstrapInterval
    stress_wis_bootstrap: BootstrapInterval
    artifacts: tuple[ScientificArtifactRef, ...]
    semantic_manifest: ScientificArtifactRef
    duration_seconds: float
    peak_rss_mib: float
    memory_measurement: str
    python_threads_before: int
    python_threads_after: int
    artifact_bytes: int
    summary_path: Path


@dataclass(frozen=True, slots=True)
class R3VerificationResult:
    """One independently readable bounded R3A gate result."""

    name: str
    passed: bool
    detail: str


def _iso(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include an explicit timezone offset")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    if not value.is_finite():
        raise ValueError("scientific manifests cannot contain non-finite decimals")
    return "0" if value == 0 else format(value.normalize(), "f")


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Path):
        return value.as_posix()
    raise TypeError(f"cannot encode {type(value).__name__} as JSON")


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            value,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            default=_json_default,
        )
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _peak_rss_mib() -> tuple[float, str]:
    """Return process high-water RSS on the Linux and Windows release platforms."""

    status_path = Path("/proc/self/status")
    if status_path.is_file():
        for line in status_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("VmHWM:"):
                kib = int(line.split()[1])
                return kib / 1024.0, "linux_proc_vmhwm"

    if sys.platform == "win32":

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.c_ulong),
                ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.GetCurrentProcess.argtypes = []
        kernel32.GetCurrentProcess.restype = ctypes.c_void_p
        psapi.GetProcessMemoryInfo.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ProcessMemoryCounters),
            ctypes.c_ulong,
        ]
        psapi.GetProcessMemoryInfo.restype = ctypes.c_bool
        handle = kernel32.GetCurrentProcess()
        succeeded = psapi.GetProcessMemoryInfo(
            handle,
            ctypes.byref(counters),
            counters.cb,
        )
        if not succeeded:
            raise OSError(ctypes.get_last_error(), "GetProcessMemoryInfo failed")
        return counters.PeakWorkingSetSize / (1024.0 * 1024.0), "windows_peak_working_set"

    raise RuntimeError("peak RSS measurement is unsupported on this platform")


def _artifact_payload(reference: ScientificArtifactRef) -> dict[str, object]:
    return {
        "content_sha256": reference.content_sha256,
        "dataset": reference.dataset,
        "format": reference.format,
        "row_count": reference.row_count,
        "schema_sha256": reference.schema_sha256,
    }


def _scientific_ref_from_parquet(
    artifact: ContentAddressedArtifact,
) -> ScientificArtifactRef:
    return ScientificArtifactRef(
        dataset=artifact.dataset,
        format="parquet",
        path=artifact.path,
        content_sha256=artifact.content_sha256,
        schema_sha256=artifact.schema_sha256,
        file_sha256=file_sha256(artifact.path),
        row_count=artifact.row_count,
    )


def _daily_observations(
    observations: Sequence[R3DemandObservation],
) -> tuple[DailyDemandObservation, ...]:
    return tuple(
        DailyDemandObservation(
            sku_id=row.sku_id,
            location_id=row.location_id,
            demand_date=row.demand_date,
            quantity=row.demand_qty,
            available_at=row.latest_available_at,
        )
        for row in observations
    )


def _forecast_all(
    observations: Sequence[DailyDemandObservation],
    config: R3AConfig,
) -> tuple[ProbabilisticBaselineForecast, ...]:
    grouped: dict[tuple[str, str], list[DailyDemandObservation]] = defaultdict(list)
    for row in observations:
        grouped[(row.sku_id, row.location_id)].append(row)
    issue_times = tuple(origin.issue_time for origin in config.evaluation.rolling_origins)
    forecasts = tuple(
        forecast
        for (sku_id, location_id), rows in sorted(grouped.items())
        for forecast in rolling_probabilistic_seasonal_naive(
            rows,
            sku_id=sku_id,
            location_id=location_id,
            issue_times=issue_times,
            horizon_days=config.forecast_contract.horizon_days,
            season_length_days=config.forecast_contract.season_length_days,
            cold_start_daily_demand=0,
        )
    )
    return tuple(
        sorted(
            forecasts,
            key=lambda forecast: (
                forecast.issue_time,
                forecast.sku_id,
                forecast.location_id,
            ),
        )
    )


def _bottom_forecasts(
    forecasts: Sequence[ProbabilisticBaselineForecast],
) -> tuple[BottomLevelQuantileForecast, ...]:
    return tuple(
        BottomLevelQuantileForecast(
            issue_time=forecast.issue_time,
            target_date=point.target_date,
            sku_id=forecast.sku_id,
            location_id=forecast.location_id,
            quantile=quantile,
            predicted_quantity=predicted,
        )
        for forecast in forecasts
        for point in forecast.points
        for quantile, predicted in zip(_QUANTILES, point.quantiles, strict=True)
    )


def _bottom_actuals(rows: Sequence[ForecastMetricRow]) -> tuple[BottomLevelActual, ...]:
    unique: dict[tuple[datetime, str, str, date], BottomLevelActual] = {}
    for row in rows:
        key = (row.origin_time, row.sku_id, row.location_id, row.target_date)
        actual = BottomLevelActual(
            issue_time=row.origin_time,
            target_date=row.target_date,
            sku_id=row.sku_id,
            location_id=row.location_id,
            actual_quantity=row.actual,
        )
        prior = unique.setdefault(key, actual)
        if prior != actual:
            raise ValueError("conflicting actual labels for one hierarchy key")
    return tuple(unique[key] for key in sorted(unique))


def _seasonal_scales(
    observations: Sequence[DailyDemandObservation],
    *,
    issue_time: datetime,
    season_length: int,
) -> dict[MaseScaleKey, Decimal]:
    grouped: dict[tuple[str, str], dict[date, int]] = defaultdict(dict)
    for row in observations:
        if row.demand_date < issue_time.date() and row.available_at <= issue_time:
            grouped[(row.sku_id, row.location_id)][row.demand_date] = row.quantity
    scales: dict[MaseScaleKey, Decimal] = {}
    for (sku_id, location_id), values in grouped.items():
        errors: list[Decimal] = []
        for demand_date, quantity in values.items():
            seasonal_date = demand_date - timedelta(days=season_length)
            if seasonal_date in values:
                errors.append(Decimal(abs(quantity - values[seasonal_date])))
        if errors:
            scales[(sku_id, location_id, issue_time)] = sum(errors, start=Decimal(0)) / Decimal(
                len(errors)
            )
    return scales


def _complete_mase_scales(
    rows: Sequence[ForecastMetricRow],
    observations: Sequence[DailyDemandObservation],
    *,
    season_length: int,
) -> dict[MaseScaleKey, Decimal] | None:
    scales: dict[MaseScaleKey, Decimal] = {}
    for issue_time in sorted({row.origin_time for row in rows}):
        scales.update(
            _seasonal_scales(
                observations,
                issue_time=issue_time,
                season_length=season_length,
            )
        )
    required = {row.block_key for row in rows}
    return scales if required.issubset(scales) else None


def _summary_row(
    rows: Sequence[ForecastMetricRow],
    *,
    segment_name: str,
    segment_value: str,
    horizon_days: int,
    mase_scales: dict[MaseScaleKey, Decimal] | None = None,
) -> EvaluationArtifactRow:
    summary = summarize_probabilistic_metrics(
        rows,
        mase_scales_by_block=mase_scales,
    )
    return EvaluationArtifactRow(
        model_name=_MODEL_NAME,
        model_version=_MODEL_VERSION,
        segment_name=segment_name,
        segment_value=segment_value,
        horizon_days=horizon_days,
        **asdict(summary),
    )


def _hierarchy_metric_rows(
    forecasts: Sequence[HierarchicalQuantileForecast],
    actuals: Sequence[HierarchicalActual],
) -> dict[HierarchyLevel, tuple[ForecastMetricRow, ...]]:
    actual_by_key = {
        (
            row.issue_time,
            row.target_date,
            row.level,
            row.sku_id,
            row.location_id,
        ): row.actual_quantity
        for row in actuals
    }
    grouped_forecasts: dict[
        tuple[datetime, date, HierarchyLevel, str | None, str | None],
        dict[Decimal, Decimal],
    ] = defaultdict(dict)
    for row in forecasts:
        grouped_forecasts[
            (row.issue_time, row.target_date, row.level, row.sku_id, row.location_id)
        ][row.quantile] = row.predicted_quantity

    grouped_rows: dict[HierarchyLevel, list[ForecastMetricRow]] = defaultdict(list)
    for key, values in sorted(grouped_forecasts.items(), key=lambda item: repr(item[0])):
        issue_time, target_date, level, sku_id, location_id = key
        if set(values) != set(_QUANTILES):
            raise ValueError("hierarchy row does not contain the frozen quantile grid")
        grouped_rows[level].append(
            ForecastMetricRow(
                sku_id=sku_id or "__network__",
                location_id=location_id or "__all__",
                origin_time=issue_time,
                target_date=target_date,
                actual=actual_by_key[key],
                q025=values[_QUANTILES[0]],
                q10=values[_QUANTILES[1]],
                q50=values[_QUANTILES[2]],
                q90=values[_QUANTILES[3]],
                q975=values[_QUANTILES[4]],
            )
        )
    return {level: tuple(rows) for level, rows in grouped_rows.items()}


def _evaluation_rows(
    metric_rows: Sequence[ForecastMetricRow],
    feature_rows: Sequence[R3PointInTimeFeatureRow],
    daily: Sequence[DailyDemandObservation],
    hierarchy_forecasts: Sequence[HierarchicalQuantileForecast],
    hierarchy_actuals: Sequence[HierarchicalActual],
    config: R3AConfig,
) -> tuple[EvaluationArtifactRow, ...]:
    rows: list[EvaluationArtifactRow] = [
        _summary_row(
            metric_rows,
            segment_name="overall",
            segment_value="all_matured",
            horizon_days=config.forecast_contract.horizon_days,
        )
    ]
    origin_by_time = {origin.issue_time: origin for origin in config.evaluation.rolling_origins}
    for issue_time, origin in origin_by_time.items():
        selected = tuple(row for row in metric_rows if row.origin_time == issue_time)
        rows.append(
            _summary_row(
                selected,
                segment_name="origin",
                segment_value=origin.name,
                horizon_days=config.forecast_contract.horizon_days,
                mase_scales=_complete_mase_scales(
                    selected,
                    daily,
                    season_length=config.forecast_contract.season_length_days,
                ),
            )
        )

    for horizon_day in range(1, config.forecast_contract.horizon_days + 1):
        selected = tuple(
            row
            for row in metric_rows
            if (row.target_date - row.origin_time.date()).days + 1 == horizon_day
        )
        rows.append(
            _summary_row(
                selected,
                segment_name="horizon",
                segment_value=f"day_{horizon_day}",
                horizon_days=horizon_day,
            )
        )

    for location_id in sorted({row.location_id for row in metric_rows}):
        selected = tuple(row for row in metric_rows if row.location_id == location_id)
        rows.append(
            _summary_row(
                selected,
                segment_name="location",
                segment_value=location_id,
                horizon_days=config.forecast_contract.horizon_days,
            )
        )

    feature_by_key = {
        (row.issue_time, row.sku_id, row.location_id, row.target_date): row for row in feature_rows
    }
    history_segments: dict[str, list[ForecastMetricRow]] = defaultdict(list)
    intermittency_segments: dict[str, list[ForecastMetricRow]] = defaultdict(list)
    velocity_segments: dict[str, list[ForecastMetricRow]] = defaultdict(list)
    for row in metric_rows:
        feature = feature_by_key[(row.origin_time, row.sku_id, row.location_id, row.target_date)]
        history_segment = (
            "cold_start"
            if feature.history_count <= config.forecast_contract.season_length_days
            else "established"
        )
        history_segments[history_segment].append(row)
        nonzero_rate = feature.rolling_nonzero_rate_14
        intermittency_segments[
            "unknown"
            if nonzero_rate is None
            else ("intermittent" if nonzero_rate < Decimal("0.5") else "dense")
        ].append(row)
        mean = feature.rolling_mean_14
        velocity_segments[
            "unknown" if mean is None else ("fast" if mean >= Decimal(5) else "slow")
        ].append(row)
    for name, groups in (
        ("history", history_segments),
        ("intermittency", intermittency_segments),
        ("velocity", velocity_segments),
    ):
        for value, segment_rows in sorted(groups.items()):
            rows.append(
                _summary_row(
                    segment_rows,
                    segment_name=name,
                    segment_value=value,
                    horizon_days=config.forecast_contract.horizon_days,
                )
            )

    for level, selected in sorted(
        _hierarchy_metric_rows(hierarchy_forecasts, hierarchy_actuals).items(),
        key=lambda item: item[0].value,
    ):
        rows.append(
            _summary_row(
                selected,
                segment_name="hierarchy",
                segment_value=level.value,
                horizon_days=config.forecast_contract.horizon_days,
            )
        )
    return tuple(
        sorted(
            rows,
            key=lambda row: (row.segment_name, row.segment_value, row.horizon_days),
        )
    )


def _label_artifact_rows(
    rows: Sequence[ForecastMetricRow],
    *,
    label_cutoff: datetime,
) -> list[dict[str, object]]:
    return [
        {
            "issue_time": _iso(row.origin_time),
            "target_date": row.target_date.isoformat(),
            "sku_location_key": f"{row.sku_id}::{row.location_id}",
            "sku_id": row.sku_id,
            "location_id": row.location_id,
            "actual_quantity": int(row.actual),
            "label_cutoff": _iso(label_cutoff),
        }
        for row in rows
    ]


def _forecast_provenance_manifest(
    forecasts: Sequence[ProbabilisticBaselineForecast],
) -> dict[str, object]:
    """Persist the calibration and input identities omitted from hierarchy rows."""

    return {
        "contract_version": "1.0.0",
        "forecast_count": len(forecasts),
        "forecasts": [
            {
                "eligible_observation_count": forecast.eligible_observation_count,
                "forecast_sha256": forecast.forecast_sha256,
                "input_sha256": forecast.input_sha256,
                "issue_time": _iso(forecast.issue_time),
                "location_id": forecast.location_id,
                "median_forecast_sha256": forecast.median_forecast_sha256,
                "model_name": forecast.model_name,
                "model_version": forecast.model_version,
                "parameters": dict(forecast.parameters),
                "quantile_method": forecast.quantile_method,
                "residual_count": forecast.residual_count,
                "residuals_sha256": canonical_json_sha256(
                    [_decimal_text(value) for value in forecast.residuals]
                ),
                "sku_id": forecast.sku_id,
            }
            for forecast in forecasts
        ],
    }


def _runtime_evidence() -> dict[str, object]:
    decimal_context = getcontext()
    return {
        "decimal": {
            "precision": decimal_context.prec,
            "rounding": decimal_context.rounding,
        },
        "machine": platform.machine(),
        "operating_system": platform.system(),
        "operating_system_release": platform.release(),
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "sys_platform": sys.platform,
    }


def _materialize_r3a(
    *,
    project_root: Path,
    rebuild: R2RebuildResult,
    artifact_dir: Path,
    config: R3AConfig,
) -> R3ARunResult:
    started = clock.perf_counter()
    starting_threads = threading.active_count()
    snapshot = adapt_r2_demand_history(rebuild)
    daily = _daily_observations(snapshot.observations)
    features = build_point_in_time_features(snapshot.observations, config=config)
    forecasts = _forecast_all(daily, config)
    metric_rows = join_matured_labels(
        forecasts,
        daily,
        label_cutoff=rebuild.planning_cutoff,
    )
    hierarchy_forecasts = aggregate_quantile_forecasts(_bottom_forecasts(forecasts))
    validate_bottom_up_coherence(hierarchy_forecasts)
    hierarchy_actuals = aggregate_actuals(_bottom_actuals(metric_rows))
    evaluations = _evaluation_rows(
        metric_rows,
        features,
        daily,
        hierarchy_forecasts,
        hierarchy_actuals,
        config,
    )
    overall = summarize_probabilistic_metrics(metric_rows)
    stress_origin = next(
        origin for origin in config.evaluation.rolling_origins if origin.role == "final_stress"
    )
    stress_rows = tuple(row for row in metric_rows if row.origin_time == stress_origin.issue_time)
    stress_scales = _complete_mase_scales(
        stress_rows,
        daily,
        season_length=config.forecast_contract.season_length_days,
    )
    if stress_scales is None:
        raise ValueError("final stress origin lacks a complete per-series seasonal MASE scale")
    stress = summarize_probabilistic_metrics(
        stress_rows,
        mase_scales_by_block=stress_scales,
    )
    bootstrap_config = config.evaluation.bootstrap
    overall_bootstrap = block_bootstrap_95(
        metric_rows,
        mean_weighted_interval_score,
        resamples=bootstrap_config.resamples,
        seed=bootstrap_config.seed,
    )
    stress_bootstrap = block_bootstrap_95(
        stress_rows,
        mean_weighted_interval_score,
        resamples=bootstrap_config.resamples,
        seed=bootstrap_config.seed,
    )

    scientific_root = artifact_dir / "scientific"
    snapshot_ref = write_immutable_json_manifest(
        scientific_root,
        dataset="snapshot.r3a_demand_registration",
        manifest=snapshot.contract.model_dump(mode="json"),
        schema_identity={"contract": "r3a.demand-snapshot-registration.v1"},
    )
    feature_artifact = write_immutable_parquet(
        scientific_root,
        dataset="features.r3a_point_in_time",
        rows=feature_artifact_rows(features),
        schema=FEATURE_ARTIFACT_SCHEMA,
    )
    feature_ref = _scientific_ref_from_parquet(feature_artifact)
    label_artifact = write_immutable_parquet(
        scientific_root,
        dataset="labels.r3a_matured",
        rows=_label_artifact_rows(metric_rows, label_cutoff=rebuild.planning_cutoff),
        schema=LABEL_ARTIFACT_SCHEMA,
    )
    label_ref = _scientific_ref_from_parquet(label_artifact)
    forecast_ref = write_forecast_rows(
        scientific_root,
        dataset="forecasts.r3a_seasonal_naive",
        rows=hierarchy_forecasts,
    )
    forecast_provenance_ref = write_immutable_json_manifest(
        scientific_root,
        dataset="manifests.r3a_forecast_provenance",
        manifest=_forecast_provenance_manifest(forecasts),
        schema_identity={"contract": "r3a.forecast-provenance.v1"},
    )
    evaluation_ref = write_evaluation_rows(
        scientific_root,
        dataset="evaluation.r3a_baseline",
        rows=evaluations,
    )
    governed_artifacts = (
        snapshot_ref,
        feature_ref,
        label_ref,
        forecast_ref,
        forecast_provenance_ref,
        evaluation_ref,
    )

    code_files = (
        "src/nexus/modeling/gold_adapter.py",
        "src/nexus/modeling/r3_artifacts.py",
        "src/nexus/modeling/r3_baseline.py",
        "src/nexus/modeling/r3_contracts.py",
        "src/nexus/modeling/r3_features.py",
        "src/nexus/modeling/r3_hierarchy.py",
        "src/nexus/modeling/r3_metrics.py",
        "src/nexus/modeling/seasonal_naive.py",
        "src/nexus/storage/artifacts.py",
        "src/nexus/r3.py",
    )
    semantic_payload: dict[str, object] = {
        "artifact_contract_version": "1.0.0",
        "artifacts": [_artifact_payload(item) for item in governed_artifacts],
        "bootstrap": {
            "overall_wis": asdict(overall_bootstrap),
            "stress_wis": asdict(stress_bootstrap),
        },
        "champion": {
            "automatic_promotion": config.promotion.automatic,
            "candidate_status": "not_run_insufficient_representative_data",
            "model_name": _MODEL_NAME,
            "model_version": _MODEL_VERSION,
            "synthetic_performance_claims_allowed": (
                config.promotion.synthetic_performance_claims_allowed
            ),
        },
        "code": {path: file_sha256(project_root / path) for path in code_files},
        "config_sha256": canonical_json_sha256(config.model_dump(mode="json")),
        "evaluation": {
            "overall": asdict(overall),
            "stress": asdict(stress),
        },
        "hierarchy": {
            "actual_rows": len(hierarchy_actuals),
            "forecast_rows": len(hierarchy_forecasts),
            "method": "bottom_up_sum_of_marginal_quantiles_contract_smoke",
            "joint_calibration_claim": False,
        },
        "release": "R3A",
        "rolling_origins": [
            origin.model_dump(mode="json") for origin in config.evaluation.rolling_origins
        ],
        "runtime": {
            package: version(package) for package in ("duckdb", "pyarrow", "pydantic", "pyyaml")
        },
        "snapshot": snapshot.contract.model_dump(mode="json"),
        "status": "MATERIALIZED_UNVERIFIED",
    }
    semantic_ref = write_immutable_json_manifest(
        scientific_root,
        dataset="manifests.r3a_semantic",
        manifest=semantic_payload,
        schema_identity={"contract": "r3a.semantic-manifest.v1"},
    )
    all_artifacts = (*governed_artifacts, semantic_ref)
    artifact_bytes = sum(reference.path.stat().st_size for reference in all_artifacts)
    peak_rss_mib, memory_method = _peak_rss_mib()
    duration_seconds = clock.perf_counter() - started
    ending_threads = threading.active_count()
    summary_path = artifact_dir / "r3a-execution-evidence.json"
    _write_json(
        summary_path,
        {
            "artifact_bytes": artifact_bytes,
            "budget": config.resource_budget.model_dump(mode="json"),
            "duration_seconds": duration_seconds,
            "memory_measurement": memory_method,
            "peak_rss_mib": peak_rss_mib,
            "python_threads_after": ending_threads,
            "python_threads_before": starting_threads,
            "release": "R3A",
            "runtime": _runtime_evidence(),
            "semantic_manifest_sha256": semantic_ref.content_sha256,
            "status": "MATERIALIZED_UNVERIFIED",
        },
    )
    return R3ARunResult(
        config=config,
        snapshot=snapshot,
        feature_rows=features,
        forecasts=forecasts,
        metric_rows=metric_rows,
        hierarchical_forecasts=hierarchy_forecasts,
        hierarchical_actuals=hierarchy_actuals,
        evaluation_rows=evaluations,
        overall_metrics=overall,
        stress_metrics=stress,
        overall_wis_bootstrap=overall_bootstrap,
        stress_wis_bootstrap=stress_bootstrap,
        artifacts=governed_artifacts,
        semantic_manifest=semantic_ref,
        duration_seconds=duration_seconds,
        peak_rss_mib=peak_rss_mib,
        memory_measurement=memory_method,
        python_threads_before=starting_threads,
        python_threads_after=ending_threads,
        artifact_bytes=artifact_bytes,
        summary_path=summary_path,
    )


def _build_upstream(
    *,
    project_root: Path,
    artifact_dir: Path,
    seed: int,
) -> R2RebuildResult:
    fixture = generate_r2_smoke_fixture(artifact_dir / "source", seed=seed)
    return run_r2_rebuild(
        project_root=project_root,
        source_dir=fixture.source_dir,
        artifact_dir=artifact_dir / "rebuild",
        planning_cutoff=fixture.manifest.planning_cutoff,
    )


def run_r3a_demo(
    *,
    project_root: Path,
    artifact_dir: Path,
    config_path: Path | None = None,
) -> R3ARunResult:
    """Rebuild the trusted upstream snapshot and materialize one R3A scientific run."""

    config = load_r3a_config() if config_path is None else load_r3a_config(config_path)
    rebuild = _build_upstream(
        project_root=project_root,
        artifact_dir=artifact_dir / "upstream",
        seed=config.seed,
    )
    return _materialize_r3a(
        project_root=project_root.resolve(),
        rebuild=rebuild,
        artifact_dir=artifact_dir / "run",
        config=config,
    )


def _artifact_hashes(result: R3ARunResult) -> dict[str, str]:
    return {
        reference.dataset: reference.content_sha256
        for reference in (*result.artifacts, result.semantic_manifest)
    }


def _adversarial_temporal_leakage_check(
    snapshot: R3DemandSnapshot,
    config: R3AConfig,
) -> tuple[bool, str]:
    """Prove future-dated and old-but-late facts cannot change an origin."""

    origin = config.evaluation.rolling_origins[0]
    first_series = min((row.sku_id, row.location_id) for row in snapshot.observations)
    clean_daily = tuple(
        row
        for row in _daily_observations(snapshot.observations)
        if (row.sku_id, row.location_id) == first_series
        and row.demand_date < origin.issue_time.date()
        and row.available_at <= origin.issue_time
    )
    sku_id, location_id = first_series
    adversarial_daily = (
        *clean_daily,
        DailyDemandObservation(
            sku_id=sku_id,
            location_id=location_id,
            demand_date=origin.issue_time.date() - timedelta(days=30),
            quantity=999_999,
            available_at=origin.issue_time + timedelta(days=1),
        ),
        DailyDemandObservation(
            sku_id=sku_id,
            location_id=location_id,
            demand_date=origin.issue_time.date(),
            quantity=999_999,
            available_at=origin.issue_time + timedelta(days=1),
        ),
    )
    clean_forecast = forecast_probabilistic_seasonal_naive(
        clean_daily,
        sku_id=sku_id,
        location_id=location_id,
        issue_time=origin.issue_time,
        horizon_days=config.forecast_contract.horizon_days,
        season_length_days=config.forecast_contract.season_length_days,
        cold_start_daily_demand=0,
    )
    adversarial_forecast = forecast_probabilistic_seasonal_naive(
        adversarial_daily,
        sku_id=sku_id,
        location_id=location_id,
        issue_time=origin.issue_time,
        horizon_days=config.forecast_contract.horizon_days,
        season_length_days=config.forecast_contract.season_length_days,
        cold_start_daily_demand=0,
    )

    def observation(
        demand_date: date,
        quantity: int,
        available_at: datetime,
    ) -> R3DemandObservation:
        synthetic_sku = "adversarial-sku"
        synthetic_location = "adversarial-location"
        sku_location_key = f"{synthetic_sku}::{synthetic_location}"
        return R3DemandObservation(
            demand_history_key=f"{demand_date.isoformat()}::{sku_location_key}",
            demand_date=demand_date,
            sku_location_key=sku_location_key,
            sku_id=synthetic_sku,
            location_id=synthetic_location,
            demand_qty=quantity,
            fulfillment_record_count=1,
            latest_available_at=available_at,
        )

    issue_date = origin.issue_time.date()
    clean_feature_observations = (
        observation(
            issue_date - timedelta(days=4),
            3,
            origin.issue_time - timedelta(days=3),
        ),
        observation(
            issue_date - timedelta(days=2),
            5,
            origin.issue_time - timedelta(days=1),
        ),
    )
    adversarial_feature_observations = (
        *clean_feature_observations,
        observation(
            issue_date - timedelta(days=1),
            999_999,
            origin.issue_time + timedelta(days=1),
        ),
        observation(
            issue_date,
            999_999,
            origin.issue_time + timedelta(days=1),
        ),
    )
    clean_features = tuple(
        row
        for row in build_point_in_time_features(
            clean_feature_observations,
            config=config,
        )
        if row.issue_time == origin.issue_time
    )
    adversarial_features = tuple(
        row
        for row in build_point_in_time_features(
            adversarial_feature_observations,
            config=config,
        )
        if row.issue_time == origin.issue_time
    )
    forecast_stable = clean_forecast.forecast_sha256 == adversarial_forecast.forecast_sha256
    features_stable = clean_features == adversarial_features
    return (
        forecast_stable and features_stable,
        (
            f"forecast_stable={str(forecast_stable).lower()}, "
            f"feature_rows_stable={str(features_stable).lower()}, "
            f"adversarial_values=2"
        ),
    )


def _publish_verification_gate(
    *,
    artifact_dir: Path,
    gate_payload: dict[str, object],
    summary_extra: dict[str, object] | None = None,
) -> ScientificArtifactRef:
    gate_ref = write_immutable_json_manifest(
        artifact_dir / "scientific",
        dataset="gates.r3a_verification",
        manifest=gate_payload,
        schema_identity={"contract": "r3a.verification-gate.v1"},
    )
    _write_json(
        artifact_dir / "r3a-verification-summary.json",
        {
            **gate_payload,
            "gate_manifest_path": gate_ref.path.as_posix(),
            "gate_manifest_sha256": gate_ref.content_sha256,
            **({} if summary_extra is None else summary_extra),
        },
    )
    return gate_ref


def verify_r3(
    *,
    project_root: Path,
    artifact_dir: Path,
) -> list[R3VerificationResult]:
    """Execute the bounded R3A registration, leakage, evaluation, and replay gate."""

    config: R3AConfig | None = None
    try:
        config = load_r3a_config()
        rebuild = _build_upstream(
            project_root=project_root,
            artifact_dir=artifact_dir / "upstream",
            seed=config.seed,
        )
        first = _materialize_r3a(
            project_root=project_root.resolve(),
            rebuild=rebuild,
            artifact_dir=artifact_dir / "run-a",
            config=config,
        )
        second = _materialize_r3a(
            project_root=project_root.resolve(),
            rebuild=rebuild,
            artifact_dir=artifact_dir / "run-b",
            config=config,
        )
        leakage_passed, leakage_detail = _adversarial_temporal_leakage_check(
            first.snapshot,
            config,
        )
    except Exception as exc:
        failure = R3VerificationResult("R3A executable scientific run", False, str(exc))
        failure_payload: dict[str, object] = {
            "artifact_hashes": {},
            "checks": [asdict(failure)],
            "config_sha256": (
                None if config is None else canonical_json_sha256(config.model_dump(mode="json"))
            ),
            "execution_evidence_file_sha256": {},
            "failure_phase": "setup_or_materialization",
            "release": "R3A",
            "semantic_manifest_sha256": None,
            "status": "FAIL",
        }
        try:
            _publish_verification_gate(
                artifact_dir=artifact_dir,
                gate_payload=failure_payload,
            )
        except Exception as evidence_exc:  # pragma: no cover - target itself may be unwritable
            failure = R3VerificationResult(
                failure.name,
                False,
                f"{failure.detail}; durable evidence failed: {evidence_exc}",
            )
        return [failure]

    stress_tolerance = Decimal(str(config.evaluation.contract_smoke_coverage_tolerance))
    max_artifact_bytes = config.resource_budget.promotable_artifacts_mib * 1024 * 1024
    results = [
        R3VerificationResult(
            "registered exact R2 Gold snapshot",
            (
                first.snapshot.contract.source_row_count == 112
                and first.snapshot.contract.source_content_sha256
                == rebuild.gold_artifact("gold.r2_demand_history").content_sha256
            ),
            (
                f"rows={first.snapshot.contract.source_row_count}, "
                f"registration={first.snapshot.contract.registration_sha256[:12]}"
            ),
        ),
        R3VerificationResult(
            "point-in-time feature boundary",
            (
                len(first.feature_rows) == 84
                and all(
                    row.max_feature_available_at is None
                    or row.max_feature_available_at <= row.issue_time
                    for row in first.feature_rows
                )
            ),
            f"rows={len(first.feature_rows)}, version={config.feature_contract.version}",
        ),
        R3VerificationResult(
            "adversarial temporal leakage rejection",
            leakage_passed,
            leakage_detail,
        ),
        R3VerificationResult(
            "nonnegative monotonic probabilistic baseline",
            (
                len(first.forecasts) == 12
                and all(
                    point.q025 <= point.q10 <= point.q50 <= point.q90 <= point.q975
                    for forecast in first.forecasts
                    for point in forecast.points
                )
            ),
            f"origins=3, series=4, forecast_points={len(first.metric_rows)}",
        ),
        R3VerificationResult(
            "matured labels + probabilistic metrics",
            (
                len(first.metric_rows) == 84
                and first.overall_metrics.observation_count == 84
                and first.stress_metrics.observation_count == 28
            ),
            (
                f"wis={_decimal_text(first.overall_metrics.weighted_interval_score)}, "
                f"stress_wis={_decimal_text(first.stress_metrics.weighted_interval_score)}"
            ),
        ),
        R3VerificationResult(
            "bottom-up hierarchy coherence",
            (len(first.hierarchical_forecasts) == 735 and len(first.hierarchical_actuals) == 147),
            (
                f"forecast_rows={len(first.hierarchical_forecasts)}, "
                f"actual_rows={len(first.hierarchical_actuals)}"
            ),
        ),
        R3VerificationResult(
            "reproducible registered-snapshot run",
            (
                first.semantic_manifest.content_sha256 == second.semantic_manifest.content_sha256
                and _artifact_hashes(first) == _artifact_hashes(second)
            ),
            f"semantic_manifest={first.semantic_manifest.content_sha256[:16]}",
        ),
        R3VerificationResult(
            "fail-closed promotion policy",
            (
                config.promotion.automatic is False
                and config.promotion.synthetic_performance_claims_allowed is False
                and config.promotion.current_champion == _MODEL_NAME
            ),
            "baseline=champion, candidate=not_run, automatic=false",
        ),
        R3VerificationResult(
            "coverage recorded against smoke tolerance",
            (
                abs(first.stress_metrics.interval_coverage_80 - Decimal("0.80")) <= stress_tolerance
                and abs(first.stress_metrics.interval_coverage_95 - Decimal("0.95"))
                <= stress_tolerance
            ),
            (
                f"stress_80={_decimal_text(first.stress_metrics.interval_coverage_80)}, "
                f"stress_95={_decimal_text(first.stress_metrics.interval_coverage_95)}, "
                f"tolerance={stress_tolerance}"
            ),
        ),
        R3VerificationResult(
            "bounded modeling resource profile",
            (
                first.duration_seconds < config.resource_budget.wall_time_seconds
                and second.duration_seconds < config.resource_budget.wall_time_seconds
                and first.peak_rss_mib < config.resource_budget.peak_rss_mib
                and second.peak_rss_mib < config.resource_budget.peak_rss_mib
                and first.artifact_bytes <= max_artifact_bytes
                and second.artifact_bytes <= max_artifact_bytes
                and first.python_threads_after <= config.resource_budget.model_threads
                and second.python_threads_after <= config.resource_budget.model_threads
            ),
            (
                f"run_a={first.duration_seconds:.2f}s/{first.peak_rss_mib:.1f}MiB, "
                f"run_b={second.duration_seconds:.2f}s/{second.peak_rss_mib:.1f}MiB, "
                f"artifacts={first.artifact_bytes}B, "
                f"threads={first.python_threads_after}/{second.python_threads_after}, "
                f"wall_scope={config.resource_budget.wall_time_scope}, "
                f"rss_scope={config.resource_budget.peak_rss_scope}"
            ),
        ),
    ]
    gate_status = "PASS" if all(result.passed for result in results) else "FAIL"
    gate_payload: dict[str, object] = {
        "artifact_hashes": _artifact_hashes(first),
        "checks": [asdict(result) for result in results],
        "config_sha256": canonical_json_sha256(config.model_dump(mode="json")),
        "execution_evidence_file_sha256": {
            "run_a": file_sha256(first.summary_path),
            "run_b": file_sha256(second.summary_path),
        },
        "release": "R3A",
        "semantic_manifest_sha256": first.semantic_manifest.content_sha256,
        "status": gate_status,
    }
    _publish_verification_gate(
        artifact_dir=artifact_dir,
        gate_payload=gate_payload,
        summary_extra={
            "overall_metrics": asdict(first.overall_metrics),
            "stress_metrics": asdict(first.stress_metrics),
        },
    )
    return results


__all__ = [
    "LABEL_ARTIFACT_SCHEMA",
    "R3ARunResult",
    "R3VerificationResult",
    "run_r3a_demo",
    "verify_r3",
]
