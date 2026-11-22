"""Strict contracts for the bounded R4A decision-optimization gate.

R4A deliberately models money as integer cents, service as basis points, and every
quantity as an integer.  Those choices keep the optimization boundary auditable and
avoid silently changing a hard constraint through floating-point rounding.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
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
DEFAULT_R4A_CONFIG_PATH = PROJECT_ROOT / "configs" / "decisioning" / "r4a-smoke.yaml"

R4Identifier = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=160,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    ),
]
BasisPoints = Annotated[int, Field(ge=0, le=10_000)]
R4Sha256 = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
ScenarioName = Literal["normal", "demand_surge", "supplier_delay"]
RawSolverStatus = Literal[
    "OPTIMAL",
    "FEASIBLE",
    "INFEASIBLE",
    "UNKNOWN",
    "MODEL_INVALID",
    "ERROR",
]
ReviewDisposition = Literal[
    "REVIEWABLE",
    "RECEIPT_ONLY_INFEASIBLE",
    "RECEIPT_ONLY_SOLVER_FAILURE",
]
R4A_PASS_CHECK_NAMES = (
    "two_run_replay",
    "solver_and_independent_feasibility",
    "human_approval_boundary",
    "unknown_safe_fallback",
    "error_fail_closed",
    "infeasible_and_model_invalid_fail_closed",
    "frozen_policy_evaluation",
    "immutable_artifacts",
    "recommendation_fingerprint_round_trip",
)


def _freeze(value: object) -> object:
    return tuple(value) if isinstance(value, list) else value


def _parse_datetime(value: object) -> object:
    if not isinstance(value, str):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include an explicit timezone offset")
    return value.astimezone(UTC)


class R4Contract(BaseModel):
    """Closed and immutable-by-construction R4A contract base."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)


class ForecastAuthority(R4Contract):
    selected_model: Literal["seasonal_naive_empirical_intervals"]
    current_champion: Literal["seasonal_naive_empirical_intervals"]
    challenger: Literal["global_lightgbm_quantile_candidate"]
    challenger_gate_status: Literal["CHALLENGER_LOST"]
    superiority_claims_allowed: Literal[False]
    adapter_path: R4Identifier
    adapter_file_sha256: R4Sha256
    source_contract: Literal["nexus.r3b.persisted-forecast-bundle.v1"]
    source_forecast_artifact_sha256: R4Sha256
    source_promotion_artifact_sha256: R4Sha256

    @model_validator(mode="after")
    def champion_remains_authoritative(self) -> Self:
        if self.selected_model != self.current_champion:
            raise ValueError("the losing R3B challenger must never be selected")
        return self


class PortfolioLine(R4Contract):
    sku_id: R4Identifier
    location_id: R4Identifier
    supplier_id: R4Identifier
    initial_inventory_units: NonNegativeInt
    regular_lead_time_days: PositiveInt
    order_multiple_units: PositiveInt
    minimum_order_units: NonNegativeInt
    maximum_order_units: PositiveInt
    unit_volume: PositiveInt
    unit_cost_cents: PositiveInt
    fixed_order_cost_cents: NonNegativeInt
    holding_cost_cents_per_unit_day: NonNegativeInt
    stockout_cost_cents_per_unit: PositiveInt
    expedite_cost_cents_per_unit: PositiveInt
    working_capital_cost_cents_per_unit: NonNegativeInt
    service_target_bps: BasisPoints

    @model_validator(mode="after")
    def order_rules_are_representable(self) -> Self:
        if self.minimum_order_units > self.maximum_order_units:
            raise ValueError("minimum_order_units cannot exceed maximum_order_units")
        if self.minimum_order_units % self.order_multiple_units != 0:
            raise ValueError("minimum_order_units must be an order multiple")
        if self.maximum_order_units % self.order_multiple_units != 0:
            raise ValueError("maximum_order_units must be an order multiple")
        return self

    @property
    def key(self) -> str:
        return f"{self.sku_id}:{self.location_id}"


class LocationCapacity(R4Contract):
    location_id: R4Identifier
    storage_capacity_volume: PositiveInt
    expedite_capacity_units_per_day: NonNegativeInt


class SupplierCapacity(R4Contract):
    supplier_id: R4Identifier
    regular_capacity_units: PositiveInt


class ScenarioPolicy(R4Contract):
    names: tuple[ScenarioName, ...]
    planning_seeds: tuple[NonNegativeInt, ...]
    evaluation_seeds: tuple[NonNegativeInt, ...]
    demand_quantile_weights_bps: tuple[PositiveInt, PositiveInt, PositiveInt]
    demand_surge_percent: PositiveInt
    supplier_delay_days: PositiveInt

    _freeze_sequences = field_validator(
        "names",
        "planning_seeds",
        "evaluation_seeds",
        "demand_quantile_weights_bps",
        mode="before",
    )(_freeze)

    @model_validator(mode="after")
    def scenario_boundary_is_frozen(self) -> Self:
        if self.names != ("normal", "demand_surge", "supplier_delay"):
            raise ValueError("R4A requires normal, demand_surge, supplier_delay in that order")
        if not self.planning_seeds or not self.evaluation_seeds:
            raise ValueError("planning and evaluation seeds must both be nonempty")
        if len(set(self.planning_seeds)) != len(self.planning_seeds):
            raise ValueError("planning seeds must be unique")
        if len(set(self.evaluation_seeds)) != len(self.evaluation_seeds):
            raise ValueError("evaluation seeds must be unique")
        if set(self.planning_seeds) & set(self.evaluation_seeds):
            raise ValueError("planning and evaluation seeds must be disjoint")
        if sum(self.demand_quantile_weights_bps) != 10_000:
            raise ValueError("demand quantile weights must total 10,000 basis points")
        if self.demand_surge_percent < 100:
            raise ValueError("demand_surge_percent must be at least 100")
        return self


class SolverPolicy(R4Contract):
    engine: Literal["ortools_cp_sat"]
    engine_version_range: Literal[">=9.15,<9.16"]
    workers: Literal[1]
    random_seed: NonNegativeInt
    max_deterministic_time: Annotated[float, Field(gt=0.0)]
    max_wall_time_seconds: Annotated[float, Field(gt=0.0)]
    deterministic_tie_break: Literal["lexicographic_order_quantity"]
    transfers: Literal["absent"]


class EvaluationPolicy(R4Contract):
    confidence_level_bps: BasisPoints
    bootstrap_resamples: PositiveInt
    bootstrap_seed: NonNegativeInt
    frozen_policy_tuning: Literal[True]
    superiority_claims_allowed: Literal[False]

    @model_validator(mode="after")
    def confidence_is_supported(self) -> Self:
        if self.confidence_level_bps != 9_500:
            raise ValueError("R4A executes exactly 95% deterministic intervals")
        return self


class R4AConfig(R4Contract):
    release: Literal["R4A"]
    contract_version: Literal["1.0.0"]
    profile: R4Identifier
    classification: Literal["synthetic"]
    decision_time: AwareDatetime
    horizon_days: Literal[14]
    budget_cents: PositiveInt
    forecast_authority: ForecastAuthority
    portfolio: tuple[PortfolioLine, ...]
    location_capacities: tuple[LocationCapacity, ...]
    supplier_capacities: tuple[SupplierCapacity, ...]
    scenarios: ScenarioPolicy
    solver: SolverPolicy
    evaluation: EvaluationPolicy

    _parse_decision_time = field_validator("decision_time", mode="before")(_parse_datetime)
    _normalize_decision_time = field_validator("decision_time")(_utc)
    _freeze_sequences = field_validator(
        "portfolio", "location_capacities", "supplier_capacities", mode="before"
    )(_freeze)

    @model_validator(mode="after")
    def portfolio_is_bounded_and_joinable(self) -> Self:
        if len(self.portfolio) != 4:
            raise ValueError("the bounded R4A smoke portfolio requires exactly four lines")
        keys = tuple(line.key for line in self.portfolio)
        if len(set(keys)) != len(keys):
            raise ValueError("portfolio sku-location keys must be unique")
        if tuple(sorted(keys)) != keys:
            raise ValueError("portfolio must be ordered by sku-location key")
        locations = {item.location_id for item in self.location_capacities}
        suppliers = {item.supplier_id for item in self.supplier_capacities}
        if len(locations) != len(self.location_capacities):
            raise ValueError("location capacities must be unique")
        if len(suppliers) != len(self.supplier_capacities):
            raise ValueError("supplier capacities must be unique")
        if {line.location_id for line in self.portfolio} - locations:
            raise ValueError("every portfolio location requires a capacity")
        if {line.supplier_id for line in self.portfolio} - suppliers:
            raise ValueError("every portfolio supplier requires a capacity")
        return self


class ChampionForecastLine(R4Contract):
    sku_id: R4Identifier
    location_id: R4Identifier
    daily_q10_demand: tuple[NonNegativeInt, ...]
    daily_median_demand: tuple[NonNegativeInt, ...]
    daily_q90_demand: tuple[NonNegativeInt, ...]

    _freeze_demand = field_validator(
        "daily_q10_demand", "daily_median_demand", "daily_q90_demand", mode="before"
    )(_freeze)

    @model_validator(mode="after")
    def quantiles_are_ordered(self) -> Self:
        if not (
            len(self.daily_q10_demand)
            == len(self.daily_median_demand)
            == len(self.daily_q90_demand)
            == 14
        ):
            raise ValueError("champion adapter lines must cover the exact 14-day horizon")
        if any(
            not lower <= median <= upper
            for lower, median, upper in zip(
                self.daily_q10_demand,
                self.daily_median_demand,
                self.daily_q90_demand,
                strict=True,
            )
        ):
            raise ValueError("champion demand quantiles must be ordered")
        return self

    @property
    def key(self) -> str:
        return f"{self.sku_id}:{self.location_id}"


class PlanningLine(ChampionForecastLine):
    supplier_id: R4Identifier
    initial_inventory_units: NonNegativeInt
    regular_lead_time_days: PositiveInt


class PlanningSnapshot(R4Contract):
    contract_version: Literal["r4a.planning.v1"] = "r4a.planning.v1"
    decision_time: AwareDatetime
    horizon_days: Literal[14]
    classification: Literal["synthetic"]
    forecast_model: Literal["seasonal_naive_empirical_intervals"]
    rejected_challenger_status: Literal["CHALLENGER_LOST"]
    lines: tuple[PlanningLine, ...]

    _normalize_decision_time = field_validator("decision_time")(_utc)
    _freeze_lines = field_validator("lines", mode="before")(_freeze)


class ChampionForecastAdapter(R4Contract):
    contract_version: Literal["r4a.champion-forecast-adapter.v1"]
    source_contract: Literal["nexus.r3b.persisted-forecast-bundle.v1"]
    source_forecast_artifact_sha256: R4Sha256
    source_promotion_artifact_sha256: R4Sha256
    source_origin: AwareDatetime
    selected_model: Literal["seasonal_naive_empirical_intervals"]
    challenger_gate_status: Literal["CHALLENGER_LOST"]
    adaptation: Literal["rounded_q10_q50_q90_repeated_to_14_daily_marginals"]
    joint_distribution_calibration_claimed: Literal[False]
    lines: tuple[ChampionForecastLine, ...]

    _parse_origin = field_validator("source_origin", mode="before")(_parse_datetime)
    _normalize_origin = field_validator("source_origin")(_utc)
    _freeze_lines = field_validator("lines", mode="before")(_freeze)

    @model_validator(mode="after")
    def lines_are_unique_and_ordered(self) -> Self:
        keys = tuple(line.key for line in self.lines)
        if len(set(keys)) != len(keys) or tuple(sorted(keys)) != keys:
            raise ValueError("champion adapter lines must be unique and key ordered")
        return self


class ScenarioDemandLine(R4Contract):
    sku_id: R4Identifier
    location_id: R4Identifier
    daily_demand_units: tuple[NonNegativeInt, ...]
    regular_delay_days: NonNegativeInt

    _freeze_demand = field_validator("daily_demand_units", mode="before")(_freeze)

    @property
    def key(self) -> str:
        return f"{self.sku_id}:{self.location_id}"


class DemandScenario(R4Contract):
    contract_version: Literal["r4a.scenario.v1"] = "r4a.scenario.v1"
    name: ScenarioName
    purpose: Literal["planning", "evaluation"]
    seed: NonNegativeInt
    probability_weight: PositiveInt
    lines: tuple[ScenarioDemandLine, ...]

    _freeze_lines = field_validator("lines", mode="before")(_freeze)


class OrderDecisionLine(R4Contract):
    sku_id: R4Identifier
    location_id: R4Identifier
    order_quantity_units: NonNegativeInt
    reason_code: Literal[
        "optimized_regular_order",
        "inventory_sufficient",
        "projected_heuristic_order",
        "safe_fallback_order",
    ]

    @property
    def key(self) -> str:
        return f"{self.sku_id}:{self.location_id}"


class ScenarioRecourseLine(R4Contract):
    scenario_name: ScenarioName
    scenario_seed: NonNegativeInt
    sku_id: R4Identifier
    location_id: R4Identifier
    end_inventory_units: tuple[NonNegativeInt, ...]
    lost_sales_units: tuple[NonNegativeInt, ...]
    expedite_units: tuple[NonNegativeInt, ...]

    _freeze_sequences = field_validator(
        "end_inventory_units", "lost_sales_units", "expedite_units", mode="before"
    )(_freeze)

    @property
    def key(self) -> str:
        return f"{self.sku_id}:{self.location_id}"


class FeasibilityIssue(R4Contract):
    code: R4Identifier
    scope: R4Identifier
    detail: Annotated[str, StringConstraints(min_length=1, max_length=500)]


class FeasibilityReport(R4Contract):
    contract_version: Literal["r4a.feasibility.v1"] = "r4a.feasibility.v1"
    independently_feasible: bool
    service_targets_met: bool
    issues: tuple[FeasibilityIssue, ...] = ()

    _freeze_issues = field_validator("issues", mode="before")(_freeze)


class EvaluationBaseline(R4Contract):
    """A validated comparison policy with no solver or lifecycle authority."""

    contract_version: Literal["r4a.evaluation-baseline.v1"] = "r4a.evaluation-baseline.v1"
    policy_name: Literal["projected_base_stock"] = "projected_base_stock"
    policy_version: Literal["1.0.0"] = "1.0.0"
    role: Literal["EVALUATION_BASELINE"] = "EVALUATION_BASELINE"
    solver_was_run: Literal[False] = False
    review_eligible: Literal[False] = False
    lines: tuple[OrderDecisionLine, ...]
    recourse: tuple[ScenarioRecourseLine, ...]
    feasibility: FeasibilityReport
    transfer_decisions: tuple[()] = ()

    _freeze_sequences = field_validator("lines", "recourse", mode="before")(_freeze)


class PolicyDecision(R4Contract):
    contract_version: Literal["r4a.decision.v1"] = "r4a.decision.v1"
    policy_name: Literal["constrained_cp_sat", "projected_base_stock"]
    policy_version: Literal["1.0.0"] = "1.0.0"
    raw_solver_status: RawSolverStatus
    solver_objective_cents: NonNegativeInt | None
    solver_best_bound_cents: NonNegativeInt | None
    optimality_gap_bps: BasisPoints | None
    deterministic_tie_break_applied: bool
    fallback_used: bool
    fallback_reason: R4Identifier | None
    lines: tuple[OrderDecisionLine, ...]
    recourse: tuple[ScenarioRecourseLine, ...]
    feasibility: FeasibilityReport
    review_disposition: ReviewDisposition
    transfer_decisions: tuple[()] = ()

    _freeze_sequences = field_validator("lines", "recourse", mode="before")(_freeze)

    @model_validator(mode="after")
    def disposition_is_fail_closed(self) -> Self:
        solver_succeeded = self.raw_solver_status in {"OPTIMAL", "FEASIBLE"}
        if solver_succeeded and (
            self.policy_name != "constrained_cp_sat"
            or self.fallback_used
            or self.fallback_reason is not None
            or not self.deterministic_tie_break_applied
        ):
            raise ValueError(
                "solver success requires the constrained policy, no fallback, "
                "and a deterministic tie-break"
            )
        if not solver_succeeded and (
            self.policy_name != "projected_base_stock"
            or not self.fallback_used
            or self.fallback_reason is None
            or self.deterministic_tie_break_applied
        ):
            raise ValueError("solver failure requires an explicit deterministic heuristic fallback")
        fully_feasible = (
            self.feasibility.independently_feasible and self.feasibility.service_targets_met
        )
        can_be_reviewable = (solver_succeeded or self.raw_solver_status == "UNKNOWN") and (
            fully_feasible
        )
        if self.review_disposition == "REVIEWABLE" and not can_be_reviewable:
            raise ValueError("only an independently feasible solver result may await approval")
        if solver_succeeded:
            expected_disposition: ReviewDisposition = (
                "REVIEWABLE" if fully_feasible else "RECEIPT_ONLY_INFEASIBLE"
            )
        elif self.raw_solver_status == "UNKNOWN":
            expected_disposition = "REVIEWABLE" if fully_feasible else "RECEIPT_ONLY_SOLVER_FAILURE"
        elif self.raw_solver_status == "INFEASIBLE":
            expected_disposition = "RECEIPT_ONLY_INFEASIBLE"
        else:
            expected_disposition = "RECEIPT_ONLY_SOLVER_FAILURE"
        if self.review_disposition != expected_disposition:
            raise ValueError("review disposition does not match solver and feasibility evidence")
        if solver_succeeded:
            if self.solver_objective_cents is None or self.solver_best_bound_cents is None:
                raise ValueError("solver success requires objective and best-bound evidence")
            if self.optimality_gap_bps is None:
                raise ValueError("solver success requires an optimality gap")
            if self.solver_best_bound_cents > self.solver_objective_cents:
                raise ValueError("solver best bound cannot exceed the incumbent objective")
            if self.raw_solver_status == "OPTIMAL" and (
                self.solver_best_bound_cents != self.solver_objective_cents
                or self.optimality_gap_bps != 0
            ):
                raise ValueError("optimal status requires an exact bound and zero gap")
        elif any(
            value is not None
            for value in (
                self.solver_objective_cents,
                self.solver_best_bound_cents,
                self.optimality_gap_bps,
            )
        ):
            raise ValueError("non-solver decisions cannot report objective evidence")
        return self


class PolicyMetrics(R4Contract):
    policy_name: Literal["constrained_cp_sat", "projected_base_stock"]
    observation_paths: PositiveInt
    fill_rate_bps: BasisPoints
    stockout_units: NonNegativeInt
    stockout_cost_cents: NonNegativeInt
    average_inventory_milliunits: NonNegativeInt
    holding_cost_cents: NonNegativeInt
    expedite_units: NonNegativeInt
    expedite_cost_cents: NonNegativeInt
    working_capital_cost_cents: NonNegativeInt
    order_cost_cents: NonNegativeInt
    total_cost_cents: NonNegativeInt
    regret_cents: NonNegativeInt


class CostInterval(R4Contract):
    metric: Literal["optimizer_minus_heuristic_mean_seed_cost_cents"]
    confidence_level_bps: Literal[9500]
    lower_cents: int
    point_cents: int
    upper_cents: int


class EvaluationReport(R4Contract):
    contract_version: Literal["r4a.evaluation.v1"] = "r4a.evaluation.v1"
    evaluation_seeds: tuple[NonNegativeInt, ...]
    paths_sha256: Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
    optimizer: PolicyMetrics
    heuristic: PolicyMetrics
    cost_difference_interval: CostInterval
    superiority_claim_allowed: Literal[False]
    claim: Literal["NO_SUPERIORITY_CLAIM"]

    _freeze_seeds = field_validator("evaluation_seeds", mode="before")(_freeze)


class GateReceipt(R4Contract):
    contract_version: Literal["r4a.gate.v1"] = "r4a.gate.v1"
    release: Literal["R4A"]
    status: Literal["PASS", "FAIL", "MATERIALIZED_UNVERIFIED"]
    full_g4_status: Literal["OPEN"]
    raw_solver_status: RawSolverStatus
    independently_feasible: bool
    review_disposition: ReviewDisposition
    manifest_created: bool
    lifecycle_state: Literal["pending_approval"] | None
    superiority_claim_allowed: Literal[False]
    checks: tuple[R4Identifier, ...]

    _freeze_checks = field_validator("checks", mode="before")(_freeze)


class VerificationRuntime(R4Contract):
    """Observed runtime provenance kept outside normalized scientific evidence."""

    os_family: Literal["linux", "windows", "macos", "other"]
    architecture: R4Identifier
    python_minor: Annotated[str, StringConstraints(pattern=r"^3\.(?:12|13)$")]
    github_runner_os_attested: bool


class VerificationCheck(R4Contract):
    name: R4Identifier
    passed: bool
    detail: Annotated[str, StringConstraints(min_length=1, max_length=500)]
    evidence_sha256: R4Sha256 | None = None


class VerificationReceipt(R4Contract):
    contract_version: Literal["r4a.verification.v2"] = "r4a.verification.v2"
    release: Literal["R4A"]
    status: Literal["PASS", "FAIL"]
    full_g4_status: Literal["OPEN"]
    runtime: VerificationRuntime
    checks: tuple[VerificationCheck, ...]
    replay_semantic_sha256: R4Sha256
    superiority_claim_allowed: Literal[False]

    _freeze_checks = field_validator("checks", mode="before")(_freeze)

    @model_validator(mode="after")
    def pass_requires_complete_consistent_evidence(self) -> Self:
        names = tuple(check.name for check in self.checks)
        if len(set(names)) != len(names):
            raise ValueError("verification check names must be unique")
        if self.status == "PASS":
            if names != R4A_PASS_CHECK_NAMES:
                raise ValueError("PASS receipt requires the exact ordered R4A verification set")
            if not all(check.passed for check in self.checks):
                raise ValueError("PASS receipt requires every verification check to pass")
            replay_check = self.checks[0]
            if replay_check.evidence_sha256 != self.replay_semantic_sha256:
                raise ValueError(
                    "PASS receipt must bind the replay check to replay_semantic_sha256"
                )
            if replay_check.detail != "two fresh replay semantic hashes match":
                raise ValueError("PASS receipt requires the canonical replay-check detail")
        elif not self.checks or all(check.passed for check in self.checks):
            raise ValueError("FAIL receipt requires at least one failed verification check")
        return self


def load_r4a_config(path: Path = DEFAULT_R4A_CONFIG_PATH) -> R4AConfig:
    """Load a YAML config through the closed, strict R4A schema."""

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("R4A config root must be a mapping")
    return R4AConfig.model_validate(cast(dict[str, Any], payload))


def load_champion_forecast_adapter(config: R4AConfig) -> ChampionForecastAdapter:
    """Read and checksum the frozen R3B champion-to-R4A adapter."""

    project_root = PROJECT_ROOT.resolve()
    path = (project_root / config.forecast_authority.adapter_path).resolve()
    if not path.is_relative_to(project_root):
        raise ValueError("champion forecast adapter must stay inside the project root")
    payload_bytes = path.read_bytes()
    actual_sha256 = hashlib.sha256(payload_bytes).hexdigest()
    if actual_sha256 != config.forecast_authority.adapter_file_sha256:
        raise ValueError("champion forecast adapter file checksum does not match policy")
    payload = json.loads(payload_bytes)
    adapter = ChampionForecastAdapter.model_validate(payload)
    authority = config.forecast_authority
    if (
        adapter.source_contract != authority.source_contract
        or adapter.source_forecast_artifact_sha256 != authority.source_forecast_artifact_sha256
        or adapter.source_promotion_artifact_sha256 != authority.source_promotion_artifact_sha256
        or adapter.selected_model != authority.selected_model
        or adapter.challenger_gate_status != authority.challenger_gate_status
    ):
        raise ValueError("champion forecast adapter provenance does not match policy")
    if tuple(line.key for line in adapter.lines) != tuple(line.key for line in config.portfolio):
        raise ValueError("champion forecast adapter does not match the planning portfolio")
    return adapter


__all__ = [
    "DEFAULT_R4A_CONFIG_PATH",
    "R4A_PASS_CHECK_NAMES",
    "ChampionForecastAdapter",
    "CostInterval",
    "DemandScenario",
    "EvaluationBaseline",
    "EvaluationReport",
    "FeasibilityIssue",
    "FeasibilityReport",
    "GateReceipt",
    "OrderDecisionLine",
    "PlanningLine",
    "PlanningSnapshot",
    "PolicyDecision",
    "PolicyMetrics",
    "PortfolioLine",
    "R4AConfig",
    "RawSolverStatus",
    "ReviewDisposition",
    "ScenarioDemandLine",
    "ScenarioRecourseLine",
    "VerificationCheck",
    "VerificationReceipt",
    "VerificationRuntime",
    "load_champion_forecast_adapter",
    "load_r4a_config",
]
