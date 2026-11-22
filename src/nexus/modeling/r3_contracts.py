"""Strict, immutable configuration contracts for the bounded R3A model gate."""

from __future__ import annotations

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
    NonNegativeInt,
    PositiveInt,
    StringConstraints,
    field_validator,
    model_validator,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_R3A_CONFIG_PATH = PROJECT_ROOT / "configs" / "modeling" / "r3a-smoke.yaml"

R3Identifier = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=160,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    ),
]
R3Sha256 = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
R3Fraction = Annotated[float, Field(gt=0.0, lt=1.0)]
R3UtcTime = Annotated[str, StringConstraints(pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$")]


def normalize_r3_utc(value: datetime) -> datetime:
    """Reject naive timestamps and normalize equivalent instants to UTC."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include an explicit timezone offset")
    return value.astimezone(UTC)


def _parse_datetime(value: object) -> object:
    """Parse the one explicit ISO-8601 wire representation accepted by YAML."""

    if not isinstance(value, str):
        return value
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("issue_time must be a valid ISO-8601 timestamp") from error


def _tuple_from_yaml(value: object) -> object:
    """Freeze YAML sequences before strict Pydantic validation."""

    return tuple(value) if isinstance(value, list) else value


class R3Contract(BaseModel):
    """Closed, deeply immutable-by-construction base for R3A contracts."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
    )


class R3DataContract(R3Contract):
    """R2 Gold input and target semantics allowed into R3A."""

    source_dataset: Literal["gold.r2_demand_history"]
    classification: Literal["synthetic"]
    target: Literal["latent_demand_qty"]
    stockout_policy: Literal["require_uncensored_demand"]
    master_history: Literal["current_snapshot_only"]


class R3FeatureContract(R3Contract):
    """Point-in-time feature recipe fitted independently at each origin."""

    version: Literal["r3a-pit-v1"]
    lags_days: tuple[PositiveInt, ...]
    rolling_windows_days: tuple[PositiveInt, ...]
    features: tuple[
        Literal[
            "lag_demand",
            "rolling_mean",
            "rolling_nonzero_rate",
            "days_since_nonzero",
            "history_count",
            "target_day_of_week",
        ],
        ...,
    ]
    fit_scope: Literal["each_origin_only"]

    _freeze_sequences = field_validator(
        "lags_days",
        "rolling_windows_days",
        "features",
        mode="before",
    )(_tuple_from_yaml)

    @model_validator(mode="after")
    def feature_sequences_are_nonempty_unique_and_ordered(self) -> Self:
        for name, values in (
            ("lags_days", self.lags_days),
            ("rolling_windows_days", self.rolling_windows_days),
        ):
            if not values:
                raise ValueError(f"{name} must not be empty")
            if tuple(sorted(set(values))) != values:
                raise ValueError(f"{name} must be unique and strictly increasing")
        if self.lags_days != (1, 7, 14):
            raise ValueError("r3a-pit-v1 requires lags_days [1, 7, 14]")
        if self.rolling_windows_days != (7, 14):
            raise ValueError("r3a-pit-v1 requires rolling_windows_days [7, 14]")
        if not self.features or len(set(self.features)) != len(self.features):
            raise ValueError("features must be nonempty and unique")
        expected_features = (
            "lag_demand",
            "rolling_mean",
            "rolling_nonzero_rate",
            "days_since_nonzero",
            "history_count",
            "target_day_of_week",
        )
        if self.features != expected_features:
            raise ValueError("r3a-pit-v1 requires the exact ordered feature set")
        return self


class R3ForecastContract(R3Contract):
    """Forecast grain, horizon, seasonality, and probability support."""

    grain: Literal["sku-location-day"]
    issue_time_utc: R3UtcTime
    horizon_days: PositiveInt
    season_length_days: PositiveInt
    quantiles: tuple[R3Fraction, ...]
    cold_start_fallback: Literal["zero"]
    hierarchy: tuple[Literal["sku_location", "sku", "network"], ...]

    _freeze_sequences = field_validator("quantiles", "hierarchy", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def probability_and_hierarchy_contracts_are_coherent(self) -> Self:
        if len(self.quantiles) < 3:
            raise ValueError("quantiles must contain lower, median, and upper support")
        if any(left >= right for left, right in pairwise(self.quantiles)):
            raise ValueError("quantiles must be strictly increasing and unique")
        if 0.5 not in self.quantiles:
            raise ValueError("quantiles must include the 0.5 median")
        expected_quantiles = (0.025, 0.1, 0.5, 0.9, 0.975)
        if self.quantiles != expected_quantiles:
            raise ValueError("R3A quantiles must exactly match 0.025, 0.10, 0.50, 0.90, 0.975")
        if self.hierarchy != ("sku_location", "sku", "network"):
            raise ValueError("hierarchy must be ordered sku_location, sku, network")
        return self


class R3RollingOrigin(R3Contract):
    """One immutable, explicitly purposed evaluation origin."""

    name: R3Identifier
    issue_time: AwareDatetime
    role: Literal["diagnostic", "calibration", "final_stress"]

    _parse_issue_time = field_validator("issue_time", mode="before")(_parse_datetime)
    _normalize_issue_time = field_validator("issue_time")(normalize_r3_utc)


class R3BootstrapContract(R3Contract):
    """Block-bootstrap uncertainty contract for paired model comparisons."""

    confidence_level: R3Fraction
    resamples: PositiveInt
    resampling_unit: Literal["sku_location_origin_block"]
    seed: NonNegativeInt

    @model_validator(mode="after")
    def confidence_matches_the_executed_interval(self) -> Self:
        if self.confidence_level != 0.95:
            raise ValueError("R3A bootstrap confidence_level must be exactly 0.95")
        return self


class R3EvaluationContract(R3Contract):
    """Ordered rolling-origin evaluation and uncertainty policy."""

    rolling_origins: tuple[R3RollingOrigin, ...]
    primary_metric: Literal["weighted_interval_score"]
    equal_observation_weights: Literal[True]
    bootstrap: R3BootstrapContract
    contract_smoke_coverage_tolerance: R3Fraction
    full_g3_coverage_tolerance: R3Fraction

    _freeze_origins = field_validator("rolling_origins", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def origins_are_unique_complete_and_ordered(self) -> Self:
        expected_roles = ("diagnostic", "calibration", "final_stress")
        observed_roles = tuple(origin.role for origin in self.rolling_origins)
        if observed_roles != expected_roles:
            raise ValueError(
                "rolling-origin roles must be unique and ordered "
                "diagnostic, calibration, final_stress"
            )
        names = tuple(origin.name for origin in self.rolling_origins)
        if len(names) != len(set(names)):
            raise ValueError("rolling-origin names must be unique")
        issue_times = tuple(origin.issue_time for origin in self.rolling_origins)
        if any(left >= right for left, right in pairwise(issue_times)):
            raise ValueError("rolling-origin issue times must be strictly increasing")
        if self.full_g3_coverage_tolerance >= self.contract_smoke_coverage_tolerance:
            raise ValueError("full G3 tolerance must be tighter than smoke tolerance")
        return self


class R3PromotionContract(R3Contract):
    """Fail-closed promotion policy for synthetic R3A evidence."""

    automatic: Literal[False]
    current_champion: Literal["seasonal_naive_empirical_intervals"]
    minimum_wis_improvement: R3Fraction
    candidate_must_win: Literal[True]
    synthetic_performance_claims_allowed: Literal[False]


class R3ResourceBudget(R3Contract):
    """Strictly positive bounded-compute envelope for the smoke gate."""

    wall_time_scope: Literal["r3a_modeling_materialization"]
    wall_time_seconds: PositiveInt
    peak_rss_scope: Literal["python_process_lifetime_high_water"]
    peak_rss_mib: PositiveInt
    thread_scope: Literal["python_interpreter_threads_after_materialization"]
    model_threads: PositiveInt
    artifact_scope: Literal["governed_scientific_artifacts"]
    promotable_artifacts_mib: PositiveInt


class R3AConfig(R3Contract):
    """Complete executable R3A smoke contract."""

    release: Literal["R3A"]
    contract_version: Literal["1.0.0"]
    profile: R3Identifier
    seed: NonNegativeInt
    data_contract: R3DataContract
    feature_contract: R3FeatureContract
    forecast_contract: R3ForecastContract
    evaluation: R3EvaluationContract
    promotion: R3PromotionContract
    resource_budget: R3ResourceBudget
    deferred_full_g3: tuple[R3Identifier, ...]

    _freeze_deferred = field_validator("deferred_full_g3", mode="before")(_tuple_from_yaml)

    @model_validator(mode="after")
    def top_level_contract_is_deterministic_and_utc_aligned(self) -> Self:
        if self.seed != self.evaluation.bootstrap.seed:
            raise ValueError("top-level and bootstrap seeds must match")
        if not self.deferred_full_g3 or len(set(self.deferred_full_g3)) != len(
            self.deferred_full_g3
        ):
            raise ValueError("deferred_full_g3 entries must be nonempty and unique")
        expected_time = self.forecast_contract.issue_time_utc
        for origin in self.evaluation.rolling_origins:
            if origin.issue_time.tzinfo is not UTC:
                raise ValueError("rolling origins must be normalized to UTC")
            if origin.issue_time.strftime("%H:%M:%S") != expected_time:
                raise ValueError("rolling origins must use forecast_contract.issue_time_utc")
        return self


def load_r3a_config(path: Path = DEFAULT_R3A_CONFIG_PATH) -> R3AConfig:
    """Load and strictly validate the R3A YAML configuration."""

    with path.open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError("R3A configuration root must be a mapping")
    return R3AConfig.model_validate(cast(dict[str, Any], payload))
