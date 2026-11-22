"""Fail-closed evaluation and cross-platform comparison primitives for R3B.

The learned candidate and transparent champion are evaluated on the same persisted
forecast keys.  This module deliberately has no access to feature construction or model
training: evaluation-only cohort truth enters here after forecast persistence, and every
comparison remains paired at the ``sku-location-target-horizon`` grain.
"""

from __future__ import annotations

import json
import random
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, Self, cast

from nexus.modeling.r3_metrics import pinball_loss, weighted_interval_score
from nexus.modeling.r3b_contracts import R3BConfig, R3BPromotionContract, R3BPromotionStatus
from nexus.modeling.r3b_data import R3BEvaluationTruth, R3BForecastKey
from nexus.modeling.r3b_models import R3BQuantilePrediction

type R3BEvaluationNumber = Decimal | float | int
type R3BSegmentDimension = Literal["horizon", "hierarchy", "synthetic_cohort", "cold_start"]

_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
_PATH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:\[\]-]*$")
_RECEIPT_FIELDS = {
    "schema_version",
    "release",
    "platform",
    "semantic_hashes",
    "descriptors",
    "numeric_values",
}


class R3BEvaluationError(ValueError):
    """R3B evidence is incomplete, non-finite, unpaired, or semantically different."""


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise R3BEvaluationError(f"{name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise R3BEvaluationError(f"{name} must not be blank")
    return normalized


def _decimal(value: object, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float)):
        raise R3BEvaluationError(f"{name} must be a Decimal, int, or float")
    normalized = Decimal(str(value)) if isinstance(value, float) else Decimal(value)
    if not normalized.is_finite():
        raise R3BEvaluationError(f"{name} must be finite")
    return normalized


def _receipt_decimal(value: object, name: str) -> Decimal:
    """Parse the canonical decimal string used on the JSON receipt boundary."""

    if isinstance(value, str):
        try:
            normalized = Decimal(value)
        except InvalidOperation as error:
            raise R3BEvaluationError(f"{name} must be a canonical decimal") from error
        if not normalized.is_finite():
            raise R3BEvaluationError(f"{name} must be finite")
        return normalized
    return _decimal(value, name)


def _nonnegative_decimal(value: object, name: str) -> Decimal:
    normalized = _decimal(value, name)
    if normalized < 0:
        raise R3BEvaluationError(f"{name} must be nonnegative")
    return normalized


def _aware_utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise R3BEvaluationError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise R3BEvaluationError(f"{name} must include an explicit timezone")
    return value.astimezone(UTC)


def _decimal_text(value: Decimal) -> str:
    if not value.is_finite():
        raise R3BEvaluationError("receipt numbers must be finite")
    return "0" if value == 0 else format(value.normalize(), "f")


def _exact_fields(payload: Mapping[str, object], expected: set[str], name: str) -> None:
    observed = set(payload)
    if observed != expected:
        missing = sorted(expected - observed)
        extra = sorted(observed - expected)
        raise R3BEvaluationError(f"{name} fields differ; missing={missing}, extra={extra}")


def _sequence(value: object, name: str) -> Sequence[object]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise R3BEvaluationError(f"{name} must be an array")
    return value


@dataclass(frozen=True, slots=True)
class R3BPredictionRecord:
    """One canonical, label-free five-quantile forecast at the governed key grain."""

    sku_id: str
    location_id: str
    target_date: date
    horizon_days: int
    q025: Decimal
    q10: Decimal
    q50: Decimal
    q90: Decimal
    q975: Decimal
    cold_start: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "sku_id", _identifier(self.sku_id, "sku_id"))
        object.__setattr__(self, "location_id", _identifier(self.location_id, "location_id"))
        if not isinstance(self.target_date, date):
            raise R3BEvaluationError("target_date must be a date")
        if isinstance(self.horizon_days, bool) or self.horizon_days not in {7, 14, 28}:
            raise R3BEvaluationError("horizon_days must be one of 7, 14, 28")
        if not isinstance(self.cold_start, bool):
            raise R3BEvaluationError("cold_start must be a boolean")
        quantile_names = ("q025", "q10", "q50", "q90", "q975")
        for name in quantile_names:
            object.__setattr__(self, name, _nonnegative_decimal(getattr(self, name), name))
        if not self.q025 <= self.q10 <= self.q50 <= self.q90 <= self.q975:
            raise R3BEvaluationError("prediction quantiles must be monotonic")

    @classmethod
    def from_quantile_prediction(
        cls,
        *,
        sku_id: str,
        location_id: str,
        target_date: date,
        horizon_days: int,
        prediction: R3BQuantilePrediction,
        cold_start: bool,
    ) -> Self:
        """Compose validated model output with its label-free forecast identity."""

        if not isinstance(prediction, R3BQuantilePrediction):
            raise R3BEvaluationError("prediction must be an R3BQuantilePrediction")
        return cls(
            sku_id=sku_id,
            location_id=location_id,
            target_date=target_date,
            horizon_days=horizon_days,
            q025=Decimal(str(prediction.q025)),
            q10=Decimal(str(prediction.q10)),
            q50=Decimal(str(prediction.q50)),
            q90=Decimal(str(prediction.q90)),
            q975=Decimal(str(prediction.q975)),
            cold_start=cold_start,
        )

    @property
    def forecast_key(self) -> R3BForecastKey:
        return self.sku_id, self.location_id, self.target_date, self.horizon_days

    @property
    def quantiles(self) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
        return self.q025, self.q10, self.q50, self.q90, self.q975


@dataclass(frozen=True, slots=True)
class R3BTruthRecord:
    """One mature, active, uncensored evaluation label plus segmentation-only cohort."""

    sku_id: str
    location_id: str
    target_date: date
    horizon_days: int
    actual: Decimal
    synthetic_cohort: str
    label_available_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "sku_id", _identifier(self.sku_id, "sku_id"))
        object.__setattr__(self, "location_id", _identifier(self.location_id, "location_id"))
        if not isinstance(self.target_date, date):
            raise R3BEvaluationError("target_date must be a date")
        if isinstance(self.horizon_days, bool) or self.horizon_days not in {7, 14, 28}:
            raise R3BEvaluationError("horizon_days must be one of 7, 14, 28")
        object.__setattr__(self, "actual", _nonnegative_decimal(self.actual, "actual"))
        object.__setattr__(
            self,
            "synthetic_cohort",
            _identifier(self.synthetic_cohort, "synthetic_cohort"),
        )
        object.__setattr__(
            self,
            "label_available_at",
            _aware_utc(self.label_available_at, "label_available_at"),
        )

    @classmethod
    def from_evaluation_truth(cls, truth: R3BEvaluationTruth) -> Self:
        """Expose the governed observed-sales target and reject censored label rows."""

        if not isinstance(truth, R3BEvaluationTruth):
            raise R3BEvaluationError("truth must be an R3BEvaluationTruth")
        if not truth.is_active or truth.stockout_censored:
            raise R3BEvaluationError("evaluation truth must be active and uncensored")
        return cls(
            sku_id=truth.sku_id,
            location_id=truth.location_id,
            target_date=truth.target_date,
            horizon_days=truth.horizon_days,
            actual=Decimal(truth.observed_sales_qty),
            synthetic_cohort=truth.synthetic_cohort,
            label_available_at=truth.label_available_at,
        )

    @property
    def forecast_key(self) -> R3BForecastKey:
        return self.sku_id, self.location_id, self.target_date, self.horizon_days


@dataclass(frozen=True, slots=True)
class R3BForecastScore:
    """Per-observation proper scores for all frozen quantiles and intervals."""

    pinball_q025: Decimal
    pinball_q10: Decimal
    pinball_q50: Decimal
    pinball_q90: Decimal
    pinball_q975: Decimal
    weighted_interval_score: Decimal


@dataclass(frozen=True, slots=True)
class R3BEvaluationRow:
    """One strictly paired candidate, baseline, and truth observation."""

    candidate: R3BPredictionRecord
    baseline: R3BPredictionRecord
    truth: R3BTruthRecord
    candidate_score: R3BForecastScore
    baseline_score: R3BForecastScore

    @property
    def forecast_key(self) -> R3BForecastKey:
        return self.truth.forecast_key

    @property
    def block_key(self) -> tuple[str, str]:
        """Return the frozen SKU-location bootstrap unit for one evaluated origin."""

        return self.truth.sku_id, self.truth.location_id


@dataclass(frozen=True, slots=True)
class R3BPairedBootstrapInterval:
    """Percentile interval over paired baseline-minus-candidate WIS."""

    estimate: Decimal
    lower: Decimal
    upper: Decimal
    confidence_level: Decimal
    resamples: int
    seed: int
    block_count: int


@dataclass(frozen=True, slots=True)
class R3BSegmentSummary:
    """Equal-observation WIS comparison for one predeclared segment value."""

    dimension: R3BSegmentDimension
    value: str
    observation_count: int
    candidate_wis: Decimal
    baseline_wis: Decimal
    absolute_wis_improvement: Decimal
    relative_wis_improvement: Decimal | None


@dataclass(frozen=True, slots=True)
class R3BEvaluationResult:
    """Complete deterministic paired evaluation result."""

    rows: tuple[R3BEvaluationRow, ...]
    candidate_wis: Decimal
    baseline_wis: Decimal
    absolute_wis_improvement: Decimal
    relative_wis_improvement: Decimal | None
    paired_wis_bootstrap: R3BPairedBootstrapInterval
    segment_summaries: tuple[R3BSegmentSummary, ...]


def score_prediction(
    actual: R3BEvaluationNumber, prediction: R3BPredictionRecord
) -> R3BForecastScore:
    """Score one validated forecast without observation weighting or rounding."""

    normalized_actual = _nonnegative_decimal(actual, "actual")
    return R3BForecastScore(
        pinball_q025=pinball_loss(normalized_actual, prediction.q025, Decimal("0.025")),
        pinball_q10=pinball_loss(normalized_actual, prediction.q10, Decimal("0.10")),
        pinball_q50=pinball_loss(normalized_actual, prediction.q50, Decimal("0.50")),
        pinball_q90=pinball_loss(normalized_actual, prediction.q90, Decimal("0.90")),
        pinball_q975=pinball_loss(normalized_actual, prediction.q975, Decimal("0.975")),
        weighted_interval_score=weighted_interval_score(
            normalized_actual,
            median=prediction.q50,
            lower_80=prediction.q10,
            upper_80=prediction.q90,
            lower_95=prediction.q025,
            upper_95=prediction.q975,
        ),
    )


def _indexed_records[T: R3BPredictionRecord | R3BTruthRecord](
    rows: Sequence[T], name: str
) -> dict[R3BForecastKey, T]:
    if not rows:
        raise R3BEvaluationError(f"{name} must not be empty")
    indexed: dict[R3BForecastKey, T] = {}
    for row in rows:
        key = row.forecast_key
        if key in indexed:
            raise R3BEvaluationError(f"{name} contains duplicate forecast key {key!r}")
        indexed[key] = row
    return indexed


def _assert_exact_keys(
    candidate: Mapping[R3BForecastKey, object],
    baseline: Mapping[R3BForecastKey, object],
    truth: Mapping[R3BForecastKey, object],
) -> tuple[R3BForecastKey, ...]:
    candidate_keys = set(candidate)
    baseline_keys = set(baseline)
    truth_keys = set(truth)
    if candidate_keys != baseline_keys or candidate_keys != truth_keys:
        raise R3BEvaluationError(
            "candidate, baseline, and truth keys must be exactly identical; "
            f"baseline_missing={len(candidate_keys - baseline_keys)}, "
            f"baseline_extra={len(baseline_keys - candidate_keys)}, "
            f"truth_missing={len(candidate_keys - truth_keys)}, "
            f"truth_extra={len(truth_keys - candidate_keys)}"
        )
    return tuple(sorted(candidate_keys))


def _mean(values: Sequence[Decimal]) -> Decimal:
    if not values:
        raise R3BEvaluationError("at least one value is required")
    return sum(values, start=Decimal(0)) / Decimal(len(values))


def _wis_pair(rows: Sequence[R3BEvaluationRow]) -> tuple[Decimal, Decimal]:
    return (
        _mean(tuple(row.candidate_score.weighted_interval_score for row in rows)),
        _mean(tuple(row.baseline_score.weighted_interval_score for row in rows)),
    )


def _relative_improvement(candidate_wis: Decimal, baseline_wis: Decimal) -> Decimal | None:
    if baseline_wis == 0:
        return None
    return (baseline_wis - candidate_wis) / baseline_wis


def _empirical_quantile(values: Sequence[Decimal], quantile: Decimal) -> Decimal:
    if not values:
        raise R3BEvaluationError("bootstrap values must not be empty")
    ordered = tuple(sorted(values))
    if len(ordered) == 1:
        return ordered[0]
    rank = Decimal(len(ordered) - 1) * quantile
    lower_index = int(rank)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = rank - Decimal(lower_index)
    return ordered[lower_index] + fraction * (ordered[upper_index] - ordered[lower_index])


def paired_wis_block_bootstrap(
    rows: Sequence[R3BEvaluationRow], *, config: R3BConfig
) -> R3BPairedBootstrapInterval:
    """Resample canonical SKU-location blocks and preserve each paired observation."""

    if not rows:
        raise R3BEvaluationError("evaluation rows must not be empty")
    canonical = tuple(sorted(rows, key=lambda item: item.forecast_key))
    grouped: dict[tuple[str, str], list[R3BEvaluationRow]] = defaultdict(list)
    for row in canonical:
        grouped[row.block_key].append(row)
    blocks = tuple(tuple(grouped[key]) for key in sorted(grouped))
    bootstrap = config.evaluation.bootstrap
    generator = random.Random(bootstrap.seed)
    estimates: list[Decimal] = []
    for _ in range(bootstrap.resamples):
        sampled = tuple(
            row
            for _block_index in range(len(blocks))
            for row in blocks[generator.randrange(len(blocks))]
        )
        candidate_wis, baseline_wis = _wis_pair(sampled)
        estimates.append(baseline_wis - candidate_wis)
    alpha = (Decimal(1) - Decimal(str(bootstrap.confidence_level))) / Decimal(2)
    candidate_wis, baseline_wis = _wis_pair(canonical)
    return R3BPairedBootstrapInterval(
        estimate=baseline_wis - candidate_wis,
        lower=_empirical_quantile(estimates, alpha),
        upper=_empirical_quantile(estimates, Decimal(1) - alpha),
        confidence_level=Decimal(str(bootstrap.confidence_level)),
        resamples=bootstrap.resamples,
        seed=bootstrap.seed,
        block_count=len(blocks),
    )


def _segment_value(row: R3BEvaluationRow, dimension: R3BSegmentDimension) -> str:
    if dimension == "horizon":
        return str(row.truth.horizon_days)
    if dimension == "hierarchy":
        return "sku_location"
    if dimension == "synthetic_cohort":
        return row.truth.synthetic_cohort
    return "true" if row.candidate.cold_start else "false"


def summarize_segments(
    rows: Sequence[R3BEvaluationRow], *, config: R3BConfig
) -> tuple[R3BSegmentSummary, ...]:
    """Summarize all frozen dimensions at this bottom-level evaluation boundary."""

    dimensions = config.evaluation.segment_dimensions
    summaries: list[R3BSegmentSummary] = []
    for dimension in dimensions:
        groups: dict[str, list[R3BEvaluationRow]] = defaultdict(list)
        for row in rows:
            groups[_segment_value(row, dimension)].append(row)
        for value in sorted(groups):
            segment_rows = tuple(groups[value])
            candidate_wis, baseline_wis = _wis_pair(segment_rows)
            summaries.append(
                R3BSegmentSummary(
                    dimension=dimension,
                    value=value,
                    observation_count=len(segment_rows),
                    candidate_wis=candidate_wis,
                    baseline_wis=baseline_wis,
                    absolute_wis_improvement=baseline_wis - candidate_wis,
                    relative_wis_improvement=_relative_improvement(candidate_wis, baseline_wis),
                )
            )
    return tuple(summaries)


def evaluate_candidate_against_baseline(
    candidate_predictions: Sequence[R3BPredictionRecord],
    baseline_predictions: Sequence[R3BPredictionRecord],
    truth_rows: Sequence[R3BTruthRecord],
    *,
    config: R3BConfig,
) -> R3BEvaluationResult:
    """Evaluate only after exact key parity and cold-start metadata parity are proven."""

    candidate = _indexed_records(candidate_predictions, "candidate predictions")
    baseline = _indexed_records(baseline_predictions, "baseline predictions")
    truth = _indexed_records(truth_rows, "truth rows")
    keys = _assert_exact_keys(candidate, baseline, truth)
    allowed_horizons = set(config.forecast_contract.horizons_days)
    if any(key[3] not in allowed_horizons for key in keys):
        raise R3BEvaluationError("evaluation contains a horizon outside the frozen contract")

    evaluated: list[R3BEvaluationRow] = []
    for key in keys:
        candidate_row = candidate[key]
        baseline_row = baseline[key]
        truth_row = truth[key]
        if candidate_row.cold_start is not baseline_row.cold_start:
            raise R3BEvaluationError(f"cold_start metadata differs for forecast key {key!r}")
        evaluated.append(
            R3BEvaluationRow(
                candidate=candidate_row,
                baseline=baseline_row,
                truth=truth_row,
                candidate_score=score_prediction(truth_row.actual, candidate_row),
                baseline_score=score_prediction(truth_row.actual, baseline_row),
            )
        )
    frozen_rows = tuple(evaluated)
    candidate_wis, baseline_wis = _wis_pair(frozen_rows)
    return R3BEvaluationResult(
        rows=frozen_rows,
        candidate_wis=candidate_wis,
        baseline_wis=baseline_wis,
        absolute_wis_improvement=baseline_wis - candidate_wis,
        relative_wis_improvement=_relative_improvement(candidate_wis, baseline_wis),
        paired_wis_bootstrap=paired_wis_block_bootstrap(frozen_rows, config=config),
        segment_summaries=summarize_segments(frozen_rows, config=config),
    )


@dataclass(frozen=True, slots=True)
class R3BPromotionDecision:
    """A promotion recommendation that can never mutate the synthetic gate's champion."""

    status: R3BPromotionStatus
    engineering_complete: bool
    candidate_won: bool | None
    promotion_applied: Literal[False]
    synthetic_evidence: Literal[True]
    human_approval_required: Literal[True]
    human_approval_recorded: bool
    blocking_reasons: tuple[str, ...]


def decide_promotion(
    evaluation: R3BEvaluationResult,
    *,
    config: R3BConfig,
    evidence_sufficient: bool,
    gate_checks_passed: bool,
    human_approval_recorded: bool = False,
) -> R3BPromotionDecision:
    """Apply the four-state R3B policy while keeping synthetic promotion impossible."""

    return decide_promotion_from_metrics(
        relative_wis_improvement=evaluation.relative_wis_improvement,
        paired_bootstrap_lower_bound=evaluation.paired_wis_bootstrap.lower,
        promotion=config.promotion,
        evidence_sufficient=evidence_sufficient,
        gate_checks_passed=gate_checks_passed,
        human_approval_recorded=human_approval_recorded,
    )


def decide_promotion_from_metrics(
    *,
    relative_wis_improvement: Decimal | None,
    paired_bootstrap_lower_bound: Decimal,
    promotion: R3BPromotionContract,
    evidence_sufficient: bool,
    gate_checks_passed: bool,
    human_approval_recorded: bool = False,
) -> R3BPromotionDecision:
    """Shared numerical policy, independent of any dataset's frozen evaluation shape."""

    if relative_wis_improvement is not None:
        relative_wis_improvement = _decimal(relative_wis_improvement, "relative improvement")
    paired_bootstrap_lower_bound = _decimal(paired_bootstrap_lower_bound, "paired lower bound")

    for value, name in (
        (evidence_sufficient, "evidence_sufficient"),
        (gate_checks_passed, "gate_checks_passed"),
        (human_approval_recorded, "human_approval_recorded"),
    ):
        if not isinstance(value, bool):
            raise R3BEvaluationError(f"{name} must be a boolean")
    if gate_checks_passed is False:
        return R3BPromotionDecision(
            status=promotion.gate_failure_status,
            engineering_complete=False,
            candidate_won=None,
            promotion_applied=False,
            synthetic_evidence=True,
            human_approval_required=True,
            human_approval_recorded=human_approval_recorded,
            blocking_reasons=("gate_check_failed",),
        )
    if evidence_sufficient is False:
        return R3BPromotionDecision(
            status=promotion.insufficient_evidence_status,
            engineering_complete=False,
            candidate_won=None,
            promotion_applied=False,
            synthetic_evidence=True,
            human_approval_required=True,
            human_approval_recorded=human_approval_recorded,
            blocking_reasons=("insufficient_evidence",),
        )

    improvement = relative_wis_improvement
    meets_threshold = improvement is not None and improvement >= Decimal(
        str(promotion.minimum_wis_improvement)
    )
    confidence_bound_is_positive = (
        promotion.require_positive_paired_bootstrap_lower_bound and paired_bootstrap_lower_bound > 0
    )
    candidate_won = meets_threshold and confidence_bound_is_positive
    if not candidate_won:
        reasons: list[str] = []
        if not meets_threshold:
            reasons.append("candidate_did_not_meet_frozen_wis_threshold")
        if not confidence_bound_is_positive:
            reasons.append("paired_bootstrap_lower_bound_not_positive")
        return R3BPromotionDecision(
            status=promotion.losing_candidate_status,
            engineering_complete=True,
            candidate_won=False,
            promotion_applied=False,
            synthetic_evidence=True,
            human_approval_required=True,
            human_approval_recorded=human_approval_recorded,
            blocking_reasons=tuple(reasons),
        )

    blocking_reasons = ["synthetic_evidence_cannot_promote_champion"]
    if not human_approval_recorded:
        blocking_reasons.append("human_approval_required")
    return R3BPromotionDecision(
        status=promotion.passing_candidate_status,
        engineering_complete=True,
        candidate_won=True,
        promotion_applied=False,
        synthetic_evidence=True,
        human_approval_required=True,
        human_approval_recorded=human_approval_recorded,
        blocking_reasons=tuple(blocking_reasons),
    )


@dataclass(frozen=True, slots=True)
class R3BReceiptSemanticHash:
    name: str
    sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _identifier(self.name, "semantic hash name"))
        if _SHA256_RE.fullmatch(self.sha256) is None:
            raise R3BEvaluationError("semantic hashes must be lowercase SHA-256 values")

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "sha256": self.sha256}

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> Self:
        _exact_fields(payload, {"name", "sha256"}, "semantic hash")
        return cls(name=cast(str, payload["name"]), sha256=cast(str, payload["sha256"]))


@dataclass(frozen=True, slots=True)
class R3BReceiptDescriptor:
    path: str
    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _identifier(self.path, "descriptor path"))
        if _PATH_RE.fullmatch(self.path) is None:
            raise R3BEvaluationError("descriptor path contains unsupported characters")
        object.__setattr__(self, "value", _identifier(self.value, "descriptor value"))

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "value": self.value}

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> Self:
        _exact_fields(payload, {"path", "value"}, "descriptor")
        return cls(path=cast(str, payload["path"]), value=cast(str, payload["value"]))


@dataclass(frozen=True, slots=True)
class R3BReceiptNumber:
    path: str
    value: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _identifier(self.path, "numeric path"))
        if _PATH_RE.fullmatch(self.path) is None:
            raise R3BEvaluationError("numeric path contains unsupported characters")
        object.__setattr__(self, "value", _decimal(self.value, "receipt numeric value"))

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "value": _decimal_text(self.value)}

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> Self:
        _exact_fields(payload, {"path", "value"}, "numeric value")
        return cls(
            path=cast(str, payload["path"]),
            value=_receipt_decimal(payload["value"], "value"),
        )


@dataclass(frozen=True, slots=True)
class R3BPlatformReceipt:
    """Normalized semantic evidence; ``platform`` is provenance, not compared content."""

    schema_version: Literal["1.0.0"]
    release: Literal["R3B"]
    platform: str
    semantic_hashes: tuple[R3BReceiptSemanticHash, ...]
    descriptors: tuple[R3BReceiptDescriptor, ...]
    numeric_values: tuple[R3BReceiptNumber, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "platform", _identifier(self.platform, "platform"))
        if self.schema_version != "1.0.0" or self.release != "R3B":
            raise R3BEvaluationError("receipt schema_version or release is unsupported")
        for keys, name in (
            (tuple(item.name for item in self.semantic_hashes), "semantic_hashes"),
            (tuple(item.path for item in self.descriptors), "descriptors"),
            (tuple(item.path for item in self.numeric_values), "numeric_values"),
        ):
            if not keys or keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
                raise R3BEvaluationError(f"{name} must be nonempty, unique, and sorted")
        descriptor_paths = {item.path for item in self.descriptors}
        numeric_paths = {item.path for item in self.numeric_values}
        if descriptor_paths & numeric_paths:
            raise R3BEvaluationError("descriptor and numeric receipt paths must not overlap")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "release": self.release,
            "platform": self.platform,
            "semantic_hashes": [item.to_dict() for item in self.semantic_hashes],
            "descriptors": [item.to_dict() for item in self.descriptors],
            "numeric_values": [item.to_dict() for item in self.numeric_values],
        }

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> Self:
        _exact_fields(payload, _RECEIPT_FIELDS, "platform receipt")
        hashes = tuple(
            R3BReceiptSemanticHash.from_mapping(cast(Mapping[str, object], item))
            for item in _sequence(payload["semantic_hashes"], "semantic_hashes")
        )
        descriptors = tuple(
            R3BReceiptDescriptor.from_mapping(cast(Mapping[str, object], item))
            for item in _sequence(payload["descriptors"], "descriptors")
        )
        numbers = tuple(
            R3BReceiptNumber.from_mapping(cast(Mapping[str, object], item))
            for item in _sequence(payload["numeric_values"], "numeric_values")
        )
        return cls(
            schema_version=cast(Literal["1.0.0"], payload["schema_version"]),
            release=cast(Literal["R3B"], payload["release"]),
            platform=cast(str, payload["platform"]),
            semantic_hashes=hashes,
            descriptors=descriptors,
            numeric_values=numbers,
        )

    @classmethod
    def from_json(cls, payload: str) -> Self:
        try:
            decoded = json.loads(payload, parse_float=Decimal)
        except (json.JSONDecodeError, TypeError) as error:
            raise R3BEvaluationError("platform receipt is not valid JSON") from error
        if not isinstance(decoded, Mapping):
            raise R3BEvaluationError("platform receipt JSON root must be an object")
        return cls.from_mapping(cast(Mapping[str, object], decoded))


def _row_prefix(index: int) -> str:
    return f"rows.{index:06d}"


def build_platform_receipt(
    *,
    platform: str,
    semantic_hashes: Mapping[str, str],
    evaluation: R3BEvaluationResult,
    promotion: R3BPromotionDecision,
    extra_descriptors: Mapping[str, str] | None = None,
    extra_numeric_values: Mapping[str, R3BEvaluationNumber] | None = None,
) -> R3BPlatformReceipt:
    """Flatten all scientific values into a strict, canonically ordered CI receipt."""

    descriptors = [
        R3BReceiptDescriptor("promotion.status", promotion.status),
        R3BReceiptDescriptor(
            "promotion.engineering_complete", str(promotion.engineering_complete).lower()
        ),
        R3BReceiptDescriptor(
            "promotion.candidate_won",
            "none" if promotion.candidate_won is None else str(promotion.candidate_won).lower(),
        ),
        R3BReceiptDescriptor(
            "promotion.human_approval_recorded",
            str(promotion.human_approval_recorded).lower(),
        ),
        R3BReceiptDescriptor(
            "promotion.blocking_reasons", ",".join(promotion.blocking_reasons) or "none"
        ),
    ]
    numbers = [
        R3BReceiptNumber("aggregate.observation_count", Decimal(len(evaluation.rows))),
        R3BReceiptNumber("aggregate.candidate_wis", evaluation.candidate_wis),
        R3BReceiptNumber("aggregate.baseline_wis", evaluation.baseline_wis),
        R3BReceiptNumber("aggregate.absolute_wis_improvement", evaluation.absolute_wis_improvement),
        R3BReceiptNumber("bootstrap.estimate", evaluation.paired_wis_bootstrap.estimate),
        R3BReceiptNumber("bootstrap.lower", evaluation.paired_wis_bootstrap.lower),
        R3BReceiptNumber("bootstrap.upper", evaluation.paired_wis_bootstrap.upper),
        R3BReceiptNumber(
            "bootstrap.confidence_level", evaluation.paired_wis_bootstrap.confidence_level
        ),
        R3BReceiptNumber("bootstrap.resamples", Decimal(evaluation.paired_wis_bootstrap.resamples)),
        R3BReceiptNumber("bootstrap.seed", Decimal(evaluation.paired_wis_bootstrap.seed)),
        R3BReceiptNumber(
            "bootstrap.block_count", Decimal(evaluation.paired_wis_bootstrap.block_count)
        ),
    ]
    if evaluation.relative_wis_improvement is None:
        descriptors.append(
            R3BReceiptDescriptor(
                "aggregate.relative_wis_improvement", "undefined_zero_baseline_wis"
            )
        )
    else:
        numbers.append(
            R3BReceiptNumber(
                "aggregate.relative_wis_improvement", evaluation.relative_wis_improvement
            )
        )

    for index, row in enumerate(evaluation.rows):
        prefix = _row_prefix(index)
        descriptors.extend(
            (
                R3BReceiptDescriptor(f"{prefix}.sku_id", row.truth.sku_id),
                R3BReceiptDescriptor(f"{prefix}.location_id", row.truth.location_id),
                R3BReceiptDescriptor(f"{prefix}.target_date", row.truth.target_date.isoformat()),
                R3BReceiptDescriptor(f"{prefix}.synthetic_cohort", row.truth.synthetic_cohort),
                R3BReceiptDescriptor(
                    f"{prefix}.label_available_at",
                    row.truth.label_available_at.isoformat().replace("+00:00", "Z"),
                ),
                R3BReceiptDescriptor(f"{prefix}.cold_start", str(row.candidate.cold_start).lower()),
            )
        )
        numbers.extend(
            (
                R3BReceiptNumber(f"{prefix}.horizon_days", Decimal(row.truth.horizon_days)),
                R3BReceiptNumber(f"{prefix}.actual", row.truth.actual),
            )
        )
        for role, prediction, score in (
            ("candidate", row.candidate, row.candidate_score),
            ("baseline", row.baseline, row.baseline_score),
        ):
            for quantile_name, quantile_value in zip(
                ("q025", "q10", "q50", "q90", "q975"),
                prediction.quantiles,
                strict=True,
            ):
                numbers.append(
                    R3BReceiptNumber(f"{prefix}.{role}.prediction.{quantile_name}", quantile_value)
                )
            for score_name in (
                "pinball_q025",
                "pinball_q10",
                "pinball_q50",
                "pinball_q90",
                "pinball_q975",
                "weighted_interval_score",
            ):
                numbers.append(
                    R3BReceiptNumber(
                        f"{prefix}.{role}.score.{score_name}",
                        cast(Decimal, getattr(score, score_name)),
                    )
                )

    for index, segment in enumerate(evaluation.segment_summaries):
        prefix = f"segments.{index:06d}"
        descriptors.extend(
            (
                R3BReceiptDescriptor(f"{prefix}.dimension", segment.dimension),
                R3BReceiptDescriptor(f"{prefix}.value", segment.value),
            )
        )
        numbers.extend(
            (
                R3BReceiptNumber(f"{prefix}.observation_count", Decimal(segment.observation_count)),
                R3BReceiptNumber(f"{prefix}.candidate_wis", segment.candidate_wis),
                R3BReceiptNumber(f"{prefix}.baseline_wis", segment.baseline_wis),
                R3BReceiptNumber(
                    f"{prefix}.absolute_wis_improvement",
                    segment.absolute_wis_improvement,
                ),
            )
        )
        relative_path = f"{prefix}.relative_wis_improvement"
        if segment.relative_wis_improvement is None:
            descriptors.append(R3BReceiptDescriptor(relative_path, "undefined_zero_baseline_wis"))
        else:
            numbers.append(R3BReceiptNumber(relative_path, segment.relative_wis_improvement))

    descriptors.extend(
        R3BReceiptDescriptor(path, value) for path, value in (extra_descriptors or {}).items()
    )
    numbers.extend(
        R3BReceiptNumber(path, _decimal(value, path))
        for path, value in (extra_numeric_values or {}).items()
    )

    hashes = tuple(
        sorted(
            (R3BReceiptSemanticHash(name, value) for name, value in semantic_hashes.items()),
            key=lambda item: item.name,
        )
    )
    return R3BPlatformReceipt(
        schema_version="1.0.0",
        release="R3B",
        platform=platform,
        semantic_hashes=hashes,
        descriptors=tuple(sorted(descriptors, key=lambda item: item.path)),
        numeric_values=tuple(sorted(numbers, key=lambda item: item.path)),
    )


@dataclass(frozen=True, slots=True)
class R3BReceiptComparison:
    passed: bool
    left_platform: str
    right_platform: str
    compared_numeric_values: int
    max_absolute_difference: Decimal
    mismatches: tuple[str, ...]


def compare_platform_receipts(
    left: R3BPlatformReceipt,
    right: R3BPlatformReceipt,
    *,
    config: R3BConfig,
) -> R3BReceiptComparison:
    """Compare normalized receipts with strict shape/hash and frozen numeric tolerance."""

    if not isinstance(left, R3BPlatformReceipt) or not isinstance(right, R3BPlatformReceipt):
        raise R3BEvaluationError("both receipts must be R3BPlatformReceipt instances")
    mismatches: list[str] = []
    if left.schema_version != right.schema_version or left.release != right.release:
        mismatches.append("receipt schema or release differs")

    left_hashes = {item.name: item.sha256 for item in left.semantic_hashes}
    right_hashes = {item.name: item.sha256 for item in right.semantic_hashes}
    if set(left_hashes) != set(right_hashes):
        mismatches.append("semantic hash fields are missing or extra")
    else:
        for name in sorted(left_hashes):
            if left_hashes[name] != right_hashes[name]:
                mismatches.append(f"semantic hash mismatch: {name}")

    left_descriptors = {item.path: item.value for item in left.descriptors}
    right_descriptors = {item.path: item.value for item in right.descriptors}
    if set(left_descriptors) != set(right_descriptors):
        mismatches.append("descriptor fields are missing or extra")
    else:
        for path in sorted(left_descriptors):
            if left_descriptors[path] != right_descriptors[path]:
                mismatches.append(f"descriptor mismatch: {path}")

    left_numbers = {item.path: item.value for item in left.numeric_values}
    right_numbers = {item.path: item.value for item in right.numeric_values}
    numeric_paths = set(left_numbers)
    if numeric_paths != set(right_numbers):
        mismatches.append("numeric fields are missing or extra")
        numeric_paths &= set(right_numbers)

    absolute_tolerance = _decimal(config.numeric_tolerance.absolute_tolerance, "absolute_tolerance")
    relative_tolerance = _decimal(config.numeric_tolerance.relative_tolerance, "relative_tolerance")
    max_difference = Decimal(0)
    for path in sorted(numeric_paths):
        left_value = _decimal(left_numbers[path], f"left {path}")
        right_value = _decimal(right_numbers[path], f"right {path}")
        difference = abs(left_value - right_value)
        max_difference = max(max_difference, difference)
        allowed = absolute_tolerance + relative_tolerance * max(abs(left_value), abs(right_value))
        if difference > allowed:
            mismatches.append(
                f"numeric value outside tolerance: {path}; "
                f"difference={_decimal_text(difference)}, allowed={_decimal_text(allowed)}"
            )
    return R3BReceiptComparison(
        passed=not mismatches,
        left_platform=left.platform,
        right_platform=right.platform,
        compared_numeric_values=len(numeric_paths),
        max_absolute_difference=max_difference,
        mismatches=tuple(mismatches),
    )


def assert_platform_receipts_match(
    left: R3BPlatformReceipt,
    right: R3BPlatformReceipt,
    *,
    config: R3BConfig,
) -> R3BReceiptComparison:
    """Return a passing comparison or reject the first semantic/numeric divergence."""

    comparison = compare_platform_receipts(left, right, config=config)
    if not comparison.passed:
        raise R3BEvaluationError("platform receipts differ: " + "; ".join(comparison.mismatches))
    return comparison


__all__ = [
    "R3BEvaluationError",
    "R3BEvaluationResult",
    "R3BEvaluationRow",
    "R3BForecastScore",
    "R3BPairedBootstrapInterval",
    "R3BPlatformReceipt",
    "R3BPredictionRecord",
    "R3BPromotionDecision",
    "R3BReceiptComparison",
    "R3BReceiptDescriptor",
    "R3BReceiptNumber",
    "R3BReceiptSemanticHash",
    "R3BSegmentSummary",
    "R3BTruthRecord",
    "assert_platform_receipts_match",
    "build_platform_receipt",
    "compare_platform_receipts",
    "decide_promotion",
    "decide_promotion_from_metrics",
    "evaluate_candidate_against_baseline",
    "paired_wis_block_bootstrap",
    "score_prediction",
    "summarize_segments",
]
