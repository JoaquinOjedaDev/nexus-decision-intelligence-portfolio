"""Strict, immutable configuration contracts for the bounded R3B model gate."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Annotated, Any, Literal, Self, cast

import yaml
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    StringConstraints,
    field_validator,
    model_validator,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_R3B_CONFIG_PATH = PROJECT_ROOT / "configs" / "modeling" / "r3b-smoke.yaml"

R3BIdentifier = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=160,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    ),
]
R3BFraction = Annotated[float, Field(gt=0.0, lt=1.0)]
R3BUtcTime = Annotated[
    str,
    StringConstraints(pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$"),
]
R3BDemandFeatureName = Literal[
    "lag_sales_1",
    "lag_sales_7",
    "lag_sales_14",
    "rolling_mean_sales_7",
    "rolling_mean_sales_14",
    "rolling_nonzero_rate_7",
    "rolling_nonzero_rate_14",
    "days_since_nonzero",
    "history_count",
    "target_day_of_week",
    "horizon_days",
]
R3BLeadTimeFeatureName = Literal[
    "ordered_qty",
    "promised_lead_time_days",
    "order_day_of_week",
    "order_month",
]
R3BForbiddenFieldName = Literal[
    "synthetic_cohort",
    "synthetic_latent_demand_qty",
    "observed_sales_qty",
    "fulfilled_qty",
    "lost_sales_qty",
    "stockout_censored",
    "is_active",
    "available_at",
    "supplier_id",
    "promotion_flag",
    "disruption_flag",
    "received_qty",
    "receipt_count",
    "completed_at",
    "label_available_at",
    "status",
    "actual_lead_time_days",
    "delay_days",
    "is_late",
    "right_censored",
    "lead_time_regime",
    "baseline_lead_time_days",
    "variability_days",
]
R3BPromotionStatus = Literal[
    "ELIGIBLE_BUT_BLOCKED_SYNTHETIC",
    "CHALLENGER_LOST",
    "ABSTAIN_INSUFFICIENT_EVIDENCE",
    "REJECTED_GATE_FAILURE",
]

R3B_HORIZONS_DAYS: tuple[int, ...] = (7, 14, 28)
R3B_QUANTILES: tuple[float, ...] = (0.025, 0.1, 0.5, 0.9, 0.975)
R3B_MINIMUM_LEARNED_HISTORY_OBSERVATIONS = 14
R3B_DEMAND_FEATURES: tuple[R3BDemandFeatureName, ...] = (
    "lag_sales_1",
    "lag_sales_7",
    "lag_sales_14",
    "rolling_mean_sales_7",
    "rolling_mean_sales_14",
    "rolling_nonzero_rate_7",
    "rolling_nonzero_rate_14",
    "days_since_nonzero",
    "history_count",
    "target_day_of_week",
    "horizon_days",
)
R3B_LEAD_TIME_FEATURES: tuple[R3BLeadTimeFeatureName, ...] = (
    "ordered_qty",
    "promised_lead_time_days",
    "order_day_of_week",
    "order_month",
)
R3B_FORBIDDEN_FIELDS: tuple[R3BForbiddenFieldName, ...] = (
    "synthetic_cohort",
    "synthetic_latent_demand_qty",
    "observed_sales_qty",
    "fulfilled_qty",
    "lost_sales_qty",
    "stockout_censored",
    "is_active",
    "available_at",
    "supplier_id",
    "promotion_flag",
    "disruption_flag",
    "received_qty",
    "receipt_count",
    "completed_at",
    "label_available_at",
    "status",
    "actual_lead_time_days",
    "delay_days",
    "is_late",
    "right_censored",
    "lead_time_regime",
    "baseline_lead_time_days",
    "variability_days",
)
R3B_PROMOTION_STATUSES: tuple[R3BPromotionStatus, ...] = (
    "ELIGIBLE_BUT_BLOCKED_SYNTHETIC",
    "CHALLENGER_LOST",
    "ABSTAIN_INSUFFICIENT_EVIDENCE",
    "REJECTED_GATE_FAILURE",
)


def _tuple_from_yaml(value: object) -> object:
    return tuple(value) if isinstance(value, list) else value


def _parse_datetime(value: object) -> object:
    if not isinstance(value, str):
        return value
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("issue_time must be a valid ISO-8601 timestamp") from error


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("issue_time must include an explicit timezone offset")
    return value.astimezone(UTC)


class R3BContract(BaseModel):
    """Closed, recursively immutable base for R3B contracts."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)


class R3BDataContract(R3BContract):
    """Exact R2C smoke inputs and leakage-safe demand-label semantics."""

    source_generator: Literal["nexus-r2c-representative-supply-network"]
    r2c_profile: Literal["smoke"]
    classification: Literal["synthetic"]
    demand_dataset: Literal["representative.demand_history"]
    product_location_dataset: Literal["representative.product_locations"]
    lead_time_dataset: Literal["representative.lead_time_history"]
    target: Literal["observed_sales_qty"]
    activity_policy: Literal["active_only"]
    censoring_policy: Literal["uncensored_only"]
    availability_policy: Literal["available_at_lte_issue_time"]
    latent_demand_field: Literal["synthetic_latent_demand_qty"]
    latent_demand_usage: Literal["post_persistence_evaluation_only"]
    cohort_field: Literal["synthetic_cohort"]
    cohort_usage: Literal["post_persistence_evaluation_only"]

    @property
    def source_profile(self) -> Literal["smoke"]:
        """Expose the R2C source profile under the adapter-facing name."""

        return self.r2c_profile

    @property
    def demand_target(self) -> Literal["observed_sales_qty"]:
        """Expose the target with an unambiguous demand-specific name."""

        return self.target


class R3BFeatureContract(R3BContract):
    """Closed point-in-time feature vocabulary for both R3B model families."""

    version: Literal["r3b-pit-v1"]
    lags_days: tuple[PositiveInt, ...]
    rolling_windows_days: tuple[PositiveInt, ...]
    allowed_demand_features: tuple[R3BDemandFeatureName, ...]
    allowed_lead_time_features: tuple[R3BLeadTimeFeatureName, ...]
    forbidden_fields: tuple[R3BForbiddenFieldName, ...]
    fit_scope: Literal["each_origin_only"]

    _freeze_sequences = field_validator(
        "lags_days",
        "rolling_windows_days",
        "allowed_demand_features",
        "allowed_lead_time_features",
        "forbidden_fields",
        mode="before",
    )(_tuple_from_yaml)

    @model_validator(mode="after")
    def vocabulary_is_exact_and_closed(self) -> Self:
        if self.lags_days != (1, 7, 14):
            raise ValueError("r3b-pit-v1 requires exact lags_days [1, 7, 14]")
        if self.rolling_windows_days != (7, 14):
            raise ValueError("r3b-pit-v1 requires exact rolling_windows_days [7, 14]")
        if self.allowed_demand_features != R3B_DEMAND_FEATURES:
            raise ValueError("r3b-pit-v1 requires the exact ordered demand feature set")
        if self.allowed_lead_time_features != R3B_LEAD_TIME_FEATURES:
            raise ValueError("r3b-pit-v1 requires the exact ordered lead-time feature set")
        if self.forbidden_fields != R3B_FORBIDDEN_FIELDS:
            raise ValueError("r3b-pit-v1 requires the exact ordered forbidden-field denylist")
        if set(self.allowed_demand_features) & set(self.forbidden_fields):
            raise ValueError("allowed demand features and forbidden fields must not overlap")
        if set(self.allowed_lead_time_features) & set(self.forbidden_fields):
            raise ValueError("allowed lead-time features and forbidden fields must not overlap")
        return self

    @property
    def forbidden_features(self) -> tuple[R3BForbiddenFieldName, ...]:
        """Expose the denylist using feature-adapter terminology."""

        return self.forbidden_fields


class R3BForecastContract(R3BContract):
    """Daily forecast support shared by the baseline and learned candidate."""

    grain: Literal["sku-location-day"]
    frequency: Literal["daily"]
    issue_time_utc: R3BUtcTime
    horizons_days: tuple[PositiveInt, ...]
    maximum_horizon_days: Literal[28]
    quantiles: tuple[R3BFraction, ...]
    cold_start_fallback: Literal["seasonal_naive_empirical_intervals"]
    minimum_history_observations_for_learned_candidate: Literal[14]

    _freeze_sequences = field_validator("horizons_days", "quantiles", mode="before")(
        _tuple_from_yaml
    )

    @model_validator(mode="after")
    def support_is_frozen(self) -> Self:
        if self.issue_time_utc != "04:00:00":
            raise ValueError("R3B issue_time_utc must be exactly 04:00:00")
        if self.horizons_days != R3B_HORIZONS_DAYS:
            raise ValueError("R3B horizons_days must be exactly 7, 14, 28")
        if max(self.horizons_days) != self.maximum_horizon_days:
            raise ValueError("maximum_horizon_days must equal the longest configured horizon")
        if self.quantiles != R3B_QUANTILES:
            raise ValueError("R3B quantiles must exactly match 0.025, 0.10, 0.50, 0.90, 0.975")
        if (
            self.minimum_history_observations_for_learned_candidate
            != R3B_MINIMUM_LEARNED_HISTORY_OBSERVATIONS
        ):
            raise ValueError("R3B learned candidate requires exactly 14 visible observations")
        return self


class R3BLightGBMContract(R3BContract):
    """Frozen deterministic LightGBM quantile candidate parameters."""

    model_name: Literal["global_lightgbm_quantile"]
    objective: Literal["quantile"]
    metric: Literal["quantile"]
    training_strategy: Literal["one_model_per_quantile"]
    device_type: Literal["cpu"]
    boosting_type: Literal["gbdt"]
    deterministic: Literal[True]
    force_col_wise: Literal[True]
    seed: Literal[20260718]
    data_random_seed: Literal[20260718]
    feature_fraction_seed: Literal[20260718]
    bagging_seed: Literal[20260718]
    num_threads: Literal[2]
    num_boost_round: Literal[64]
    early_stopping_rounds: Literal[0]


class R3BLeadTimeContract(R3BContract):
    """Leakage-safe supplier lead-time label and empirical-control policy."""

    target: Literal["actual_lead_time_days"]
    label_availability_field: Literal["label_available_at"]
    maturity_policy: Literal["label_available_at_lte_origin"]
    right_censoring_policy: Literal["exclude_right_censored"]
    allowed_features: tuple[R3BLeadTimeFeatureName, ...]
    baseline: Literal["empirical_mature_uncensored_quantiles"]
    minimum_training_labels: Literal[20]

    _freeze_features = field_validator("allowed_features", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def feature_vocabulary_is_exact(self) -> Self:
        if self.allowed_features != R3B_LEAD_TIME_FEATURES:
            raise ValueError("lead-time model requires the exact ordered feature set")
        return self


class R3BCalibrationContract(R3BContract):
    """As-of-safe calibration fitted before the untouched stress origin."""

    method: Literal["empirical_residual_quantile_offsets"]
    fit_origin_role: Literal["calibration"]
    application_origin_role: Literal["final_stress"]
    as_of_policy: Literal["label_available_at_lte_application_issue_time"]
    maturity_policy: Literal["complete_horizon_mature_only"]
    minimum_support_per_horizon: Literal[20]
    insufficient_support_policy: Literal["abstain_insufficient_evidence"]


class R3BReconciliationContract(R3BContract):
    """Arithmetic hierarchy coherence without a joint-distribution claim."""

    method: Literal["arithmetic_bottom_up"]
    hierarchy: tuple[Literal["sku_location", "sku", "network"], ...]
    quantile_aggregation: Literal["marginal_quantile_sums"]
    arithmetic_coherence_required: Literal[True]
    joint_distribution_calibration_claimed: Literal[False]

    _freeze_hierarchy = field_validator("hierarchy", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def hierarchy_is_complete_and_ordered(self) -> Self:
        if self.hierarchy != ("sku_location", "sku", "network"):
            raise ValueError("hierarchy must be ordered sku_location, sku, network")
        return self


class R3BRollingOrigin(R3BContract):
    """One ordered, point-in-time R3B evaluation boundary."""

    name: R3BIdentifier
    issue_time: AwareDatetime
    role: Literal["diagnostic", "calibration", "final_stress"]

    _parse_issue_time = field_validator("issue_time", mode="before")(_parse_datetime)
    _normalize_issue_time = field_validator("issue_time")(_normalize_utc)


class R3BBootstrapContract(R3BContract):
    """Deterministic block-bootstrap policy for candidate comparisons."""

    confidence_level: R3BFraction
    resamples: Literal[200]
    resampling_unit: Literal["sku_location_origin_block"]
    seed: Literal[20260718]

    @model_validator(mode="after")
    def confidence_is_frozen(self) -> Self:
        if self.confidence_level != 0.95:
            raise ValueError("R3B bootstrap confidence_level must be exactly 0.95")
        return self


class R3BEvaluationContract(R3BContract):
    """Predeclared origins, mature labels, metrics, and segmentation."""

    rolling_origins: tuple[R3BRollingOrigin, ...]
    primary_metric: Literal["weighted_interval_score"]
    equal_observation_weights: Literal[True]
    mature_labels_only: Literal[True]
    final_stress_tuning_allowed: Literal[False]
    segment_dimensions: tuple[
        Literal["horizon", "hierarchy", "synthetic_cohort", "cold_start"], ...
    ]
    bootstrap: R3BBootstrapContract

    _freeze_sequences = field_validator(
        "rolling_origins",
        "segment_dimensions",
        mode="before",
    )(_tuple_from_yaml)

    @model_validator(mode="after")
    def origins_and_segments_are_complete_and_ordered(self) -> Self:
        roles = tuple(origin.role for origin in self.rolling_origins)
        required_roles = ("diagnostic", "calibration", "final_stress")
        if roles != required_roles:
            raise ValueError(
                "rolling origins must contain exactly one ordered diagnostic, calibration, "
                "and final_stress role"
            )
        names = tuple(origin.name for origin in self.rolling_origins)
        if len(names) != len(set(names)):
            raise ValueError("rolling-origin names must be unique")
        issue_times = tuple(origin.issue_time for origin in self.rolling_origins)
        if any(left >= right for left, right in pairwise(issue_times)):
            raise ValueError("rolling-origin issue times must be strictly increasing")
        expected_segments = ("horizon", "hierarchy", "synthetic_cohort", "cold_start")
        if self.segment_dimensions != expected_segments:
            raise ValueError("R3B segment_dimensions must match the exact ordered set")
        return self


class R3BPromotionContract(R3BContract):
    """Manual-only promotion outcomes for synthetic engineering evidence."""

    automatic: Literal[False]
    current_champion: Literal["seasonal_naive_empirical_intervals"]
    minimum_wis_improvement: R3BFraction
    candidate_must_win: Literal[True]
    require_positive_paired_bootstrap_lower_bound: Literal[True]
    human_approval_required: Literal[True]
    synthetic_performance_claims_allowed: Literal[False]
    allowed_statuses: tuple[R3BPromotionStatus, ...]
    passing_candidate_status: Literal["ELIGIBLE_BUT_BLOCKED_SYNTHETIC"]
    losing_candidate_status: Literal["CHALLENGER_LOST"]
    insufficient_evidence_status: Literal["ABSTAIN_INSUFFICIENT_EVIDENCE"]
    gate_failure_status: Literal["REJECTED_GATE_FAILURE"]

    _freeze_statuses = field_validator("allowed_statuses", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def threshold_and_statuses_are_frozen(self) -> Self:
        if self.minimum_wis_improvement != 0.10:
            raise ValueError("R3B minimum_wis_improvement must be exactly 0.10")
        if self.allowed_statuses != R3B_PROMOTION_STATUSES:
            raise ValueError("R3B promotion statuses must match the exact ordered set")
        return self


class R3BResourceBudget(R3BContract):
    """Frozen process-tree envelope for the complete learned-model smoke gate."""

    wall_time_scope: Literal["r3b_complete_verification"]
    wall_time_seconds: Literal[300]
    peak_rss_scope: Literal["complete_process_tree_high_water"]
    peak_rss_mib: Literal[2048]
    thread_scope: Literal["complete_process_tree_native_threads_high_water"]
    maximum_process_tree_threads: Literal[96]
    artifact_scope: Literal["r3b_governed_artifacts_including_tracking_store"]
    artifact_mib: Literal[100]
    shutdown_scope: Literal["process_tree_descendants_after_cleanup"]
    shutdown_seconds: Literal[10]
    maximum_surviving_descendants: Literal[0]


class R3BNumericTolerance(R3BContract):
    """Cross-platform tolerance for deterministic scientific-value comparisons."""

    comparison_scope: Literal["cross_platform_forecast_and_metric_values"]
    absolute_tolerance: float = Field(ge=0.0)
    relative_tolerance: float = Field(ge=0.0)
    nan_policy: Literal["forbid"]
    infinity_policy: Literal["forbid"]

    @model_validator(mode="after")
    def tolerances_are_frozen(self) -> Self:
        if self.absolute_tolerance != 1e-7:
            raise ValueError("R3B absolute_tolerance must be exactly 1e-7")
        if self.relative_tolerance != 1e-6:
            raise ValueError("R3B relative_tolerance must be exactly 1e-6")
        return self


class R3BConfig(R3BContract):
    """Complete executable R3B learned-intelligence smoke contract."""

    release: Literal["R3B"]
    contract_version: Literal["1.0.0"]
    profile: Literal["r2c-synthetic-contract-smoke"]
    seed: Literal[20260718]
    data_contract: R3BDataContract
    forecast_contract: R3BForecastContract
    feature_contract: R3BFeatureContract
    demand_model: R3BLightGBMContract
    lead_time_model: R3BLeadTimeContract
    calibration: R3BCalibrationContract
    reconciliation: R3BReconciliationContract
    evaluation: R3BEvaluationContract
    promotion: R3BPromotionContract
    resource_budget: R3BResourceBudget
    numeric_tolerance: R3BNumericTolerance

    @model_validator(mode="after")
    def sections_are_aligned_and_leakage_safe(self) -> Self:
        expected_time = self.forecast_contract.issue_time_utc
        for origin in self.evaluation.rolling_origins:
            if origin.issue_time.tzinfo is not UTC:
                raise ValueError("rolling origins must be normalized to UTC")
            if origin.issue_time.strftime("%H:%M:%S") != expected_time:
                raise ValueError("rolling origins must use forecast_contract.issue_time_utc")
        if (
            self.feature_contract.allowed_lead_time_features
            != self.lead_time_model.allowed_features
        ):
            raise ValueError("shared and lead-time-model feature vocabularies must match")
        required_forbidden = {
            self.data_contract.target,
            self.data_contract.latent_demand_field,
            self.data_contract.cohort_field,
            self.lead_time_model.target,
            self.lead_time_model.label_availability_field,
        }
        if not required_forbidden.issubset(self.feature_contract.forbidden_fields):
            raise ValueError("targets and evaluation-only truth must be forbidden model features")
        if self.demand_model.num_threads > self.resource_budget.maximum_process_tree_threads:
            raise ValueError("model threads cannot exceed the process-tree thread budget")
        return self


def load_r3b_config(path: Path = DEFAULT_R3B_CONFIG_PATH) -> R3BConfig:
    """Load and strictly validate the frozen R3B YAML configuration."""

    with path.open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError("R3B configuration root must be a mapping")
    return R3BConfig.model_validate(cast(dict[str, Any], payload))


def r3b_config_sha256(config: R3BConfig) -> str:
    """Hash the canonical semantic R3B configuration independently of YAML formatting."""

    encoded = json.dumps(
        config.model_dump(mode="json"),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
