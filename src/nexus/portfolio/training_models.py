"""Daily-horizon candidate numerics, separate from the frozen R3B release gate.

The caller owns point-in-time splits, mature labels, unique outcome keys, and
artifact/source provenance. This matrix-only wrapper reuses public deterministic
LightGBM and empirical calibration primitives, not the R3B 7/14/28-day gate.
It performs no promotion, network I/O, pickle loading, or source ingestion.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from decimal import Decimal
from numbers import Integral, Real
from typing import Any

from nexus.modeling.r3_features import R3PointInTimeFeatureRow
from nexus.modeling.r3b_contracts import R3B_DEMAND_FEATURES, R3B_QUANTILES, R3BLightGBMContract
from nexus.modeling.r3b_models import (
    R3BQuantileCalibrator,
    R3BQuantileEnsemble,
    R3BQuantilePrediction,
    apply_quantile_calibration,
    fit_quantile_calibrator,
    fit_quantile_ensemble,
    predict_quantiles,
)

DAILY_FEATURE_NAMES: tuple[str, ...] = tuple(R3B_DEMAND_FEATURES)
MISSING_VALUE_POLICY = "zero_with_history_count_signal"
CONTRACT_VERSION = "nexus.portfolio.daily-lightgbm-candidate.v1"
type DailyNumber = Decimal | float | int
type DailyFeatureValue = DailyNumber | None
type DailyMatrix = tuple[tuple[float, ...], ...]


def _encode(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha(value: object) -> str:
    return hashlib.sha256(_encode(value)).hexdigest()


def _check_sha(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValueError("expected a lowercase SHA-256 digest")
    return value


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return int(value)


def _horizon(value: object) -> int:
    horizon = _positive_integer(value, "horizon")
    if horizon > 14:
        raise ValueError("daily horizon must be in 1..14, not the frozen R3B horizon contract")
    return horizon


def _number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (Decimal, Real)):
        raise ValueError(f"{name} must be a finite nonnegative number")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{name} must be a finite nonnegative number")
    return result


def _matrix(
    matrix: Iterable[Iterable[DailyFeatureValue]],
    horizons: Iterable[int],
    feature_names: Iterable[str],
    *,
    allow_empty: bool = False,
) -> tuple[DailyMatrix, tuple[int, ...]]:
    if tuple(feature_names) != DAILY_FEATURE_NAMES:
        raise ValueError(
            "feature_names must match the exact ordered daily/R3B demand feature names"
        )
    normalized_horizons = tuple(_horizon(value) for value in horizons)
    raw_rows = tuple(matrix)
    if len(raw_rows) != len(normalized_horizons) or (not raw_rows and not allow_empty):
        raise ValueError("matrix and horizons must have equal, nonempty row counts")
    rows = []
    for index, raw in enumerate(raw_rows):
        if isinstance(raw, (str, bytes, Mapping)):
            raise ValueError("feature rows must be ordered numeric sequences")
        values = tuple(raw)
        if len(values) != len(DAILY_FEATURE_NAMES):
            raise ValueError("each feature row must contain exactly 11 ordered values")
        normalized = []
        for column, value in enumerate(values):
            if value is None and column < 8:
                number = 0.0
            else:
                number = _number(value, DAILY_FEATURE_NAMES[column])
            if column in (5, 6) and number > 1:
                raise ValueError("rolling nonzero rates must be within [0, 1]")
            if column in (7, 8, 9, 10) and not number.is_integer():
                raise ValueError("days/history/weekday/horizon features must be integer-valued")
            if column == 9 and number > 6:
                raise ValueError("target_day_of_week must be within [0, 6]")
            normalized.append(number)
        if normalized[-1] != normalized_horizons[index]:
            raise ValueError("horizon vector does not match the last ordered feature")
        rows.append(tuple(normalized))
    return tuple(rows), normalized_horizons


def matrix_from_features(rows: Iterable[R3PointInTimeFeatureRow]) -> DailyMatrix:
    """Map public PIT features to the exact demand feature order and zero policy.

    ``seasonal_target_lag_7`` is deliberately excluded: it is not an R3B demand
    feature. The horizon feature is the daily ``horizon_day``, not a forged 7/14/28
    contract. The caller must still validate split boundaries and unique keys.
    """
    features = tuple(rows)
    if any(not isinstance(row, R3PointInTimeFeatureRow) for row in features):
        raise ValueError("rows must be validated R3PointInTimeFeatureRow objects")
    matrix = tuple(
        (
            row.lag_1,
            row.lag_7,
            row.lag_14,
            row.rolling_mean_7,
            row.rolling_mean_14,
            row.rolling_nonzero_rate_7,
            row.rolling_nonzero_rate_14,
            row.days_since_nonzero,
            row.history_count,
            row.target_day_of_week,
            row.horizon_day,
        )
        for row in features
    )
    return _matrix(
        matrix, (row.horizon_day for row in features), DAILY_FEATURE_NAMES, allow_empty=True
    )[0]


def _numerical_contract() -> R3BLightGBMContract:
    """Reuse only the validated numerical parameters, without loading a release config."""
    return R3BLightGBMContract(
        model_name="global_lightgbm_quantile",
        objective="quantile",
        metric="quantile",
        training_strategy="one_model_per_quantile",
        device_type="cpu",
        boosting_type="gbdt",
        deterministic=True,
        force_col_wise=True,
        seed=20260718,
        data_random_seed=20260718,
        feature_fraction_seed=20260718,
        bagging_seed=20260718,
        num_threads=2,
        num_boost_round=64,
        early_stopping_rounds=0,
    )


@dataclass(frozen=True, slots=True)
class DailyQuantileCandidate:
    """Reloadable five-booster candidate with per-daily-horizon calibration support."""

    ensemble: R3BQuantileEnsemble
    training_support: tuple[tuple[int, int], ...]
    calibrators: tuple[tuple[int, R3BQuantileCalibrator], ...]
    normalized_training_input_sha256: str
    normalized_calibration_input_sha256: str
    minimum_training_rows: int = 20
    minimum_calibration_support: int = 20

    def __post_init__(self) -> None:
        if not isinstance(self.ensemble, R3BQuantileEnsemble):
            raise ValueError("ensemble must be a validated numerical quantile ensemble")
        if (
            self.ensemble.feature_names != DAILY_FEATURE_NAMES
            or self.ensemble.quantiles != R3B_QUANTILES
        ):
            raise ValueError("ensemble features/quantiles violate the daily wrapper contract")
        _check_sha(self.normalized_training_input_sha256)
        _check_sha(self.normalized_calibration_input_sha256)
        _positive_integer(self.minimum_training_rows, "minimum_training_rows")
        _positive_integer(self.minimum_calibration_support, "minimum_calibration_support")
        if self.minimum_calibration_support < 20:
            raise ValueError("daily calibration requires at least 20 held-out rows per horizon")
        horizons = tuple(_horizon(horizon) for horizon, _ in self.training_support)
        if not horizons or tuple(sorted(set(horizons))) != horizons:
            raise ValueError("training horizon support must be unique, ordered, and nonempty")
        counts = tuple(
            _positive_integer(count, "training support") for _, count in self.training_support
        )
        if sum(counts) < self.minimum_training_rows:
            raise ValueError("training support is below the declared minimum")
        if tuple(horizon for horizon, _ in self.calibrators) != horizons:
            raise ValueError("calibrator keys must exactly match trained horizons")
        for _, calibrator in self.calibrators:
            if not isinstance(calibrator, R3BQuantileCalibrator):
                raise ValueError("calibration entries must be validated numerical calibrators")
            if calibrator.minimum_support != self.minimum_calibration_support:
                raise ValueError(
                    "calibrator support threshold does not match the artifact contract"
                )
            if calibrator.quantiles != R3B_QUANTILES:
                raise ValueError("calibrator quantiles do not match the daily contract")

    @property
    def supported_horizons(self) -> tuple[int, ...]:
        return tuple(horizon for horizon, _ in self.training_support)

    @property
    def calibration_diagnostics(self) -> dict[str, Any]:
        return {
            "minimum_support_per_horizon": self.minimum_calibration_support,
            "all_trained_horizons_assessed": all(item.assessed for _, item in self.calibrators),
            "horizons": {
                str(horizon): {
                    "training_rows": dict(self.training_support)[horizon],
                    "calibration_rows": item.support,
                    "assessed": item.assessed,
                    "status": item.status,
                }
                for horizon, item in self.calibrators
            },
            "coverage_guarantee_claimed": False,
        }

    def predict_raw(
        self,
        matrix: Iterable[Iterable[DailyFeatureValue]],
        horizons: Iterable[int],
        *,
        feature_names: Iterable[str] = DAILY_FEATURE_NAMES,
    ) -> tuple[R3BQuantilePrediction, ...]:
        """Explicit uncalibrated numerical predictions for diagnostics, not silent fallback."""
        rows, normalized_horizons = _matrix(matrix, horizons, feature_names)
        if not set(normalized_horizons).issubset(self.supported_horizons):
            raise ValueError("prediction includes an untrained horizon")
        # The public ensemble is frozen but its Mapping field may be caller-owned;
        # revalidate its self-hash before any native model strings are loaded.
        verified = R3BQuantileEnsemble.from_dict(self.ensemble.to_dict())
        return predict_quantiles(verified, rows)

    def predict(
        self,
        matrix: Iterable[Iterable[DailyFeatureValue]],
        horizons: Iterable[int],
        *,
        feature_names: Iterable[str] = DAILY_FEATURE_NAMES,
    ) -> tuple[R3BQuantilePrediction, ...]:
        """Calibrated predictions; unsupported horizons abstain instead of looking healthy."""
        rows, normalized_horizons = _matrix(matrix, horizons, feature_names)
        calibrators = dict(self.calibrators)
        insufficient = sorted(
            {
                horizon
                for horizon in normalized_horizons
                if horizon not in calibrators or not calibrators[horizon].assessed
            }
        )
        if insufficient:
            raise ValueError(f"insufficient calibration support for horizons {insufficient}")
        raw = self.predict_raw(rows, normalized_horizons)
        return tuple(
            apply_quantile_calibration(calibrators[horizon], (prediction,))[0]
            for horizon, prediction in zip(normalized_horizons, raw, strict=True)
        )

    def to_dict(self) -> dict[str, Any]:
        """Canonical JSON-safe envelope with both nested model and wrapper checksums."""
        R3BQuantileEnsemble.from_dict(self.ensemble.to_dict())
        body = {
            "contract_version": CONTRACT_VERSION,
            "feature_names": list(DAILY_FEATURE_NAMES),
            "missing_value_policy": MISSING_VALUE_POLICY,
            "supported_horizons": list(self.supported_horizons),
            "training_support": [list(pair) for pair in self.training_support],
            "minimum_training_rows": self.minimum_training_rows,
            "minimum_calibration_support": self.minimum_calibration_support,
            "normalized_training_input_sha256": self.normalized_training_input_sha256,
            "normalized_calibration_input_sha256": self.normalized_calibration_input_sha256,
            "ensemble": self.ensemble.to_dict(),
            "calibrators": [[horizon, asdict(item)] for horizon, item in self.calibrators],
            "frozen_r3b_gate_claimed": False,
            "scientific_champion_changed": False,
        }
        return {**body, "content_sha256": _sha(body)}

    def to_bytes(self) -> bytes:
        """Native LightGBM model strings in canonical UTF-8 JSON, never pickle."""
        return _encode(self.to_dict())

    @property
    def sha256(self) -> str:
        """File-byte hash directly suitable for learning.ModelArtifact."""
        return hashlib.sha256(self.to_bytes()).hexdigest()

    @classmethod
    def from_bytes(
        cls,
        payload: bytes,
        *,
        expected_sha256: str | None = None,
    ) -> DailyQuantileCandidate:
        """Validate wrapper/nested checksums and exact metadata before native inference."""
        if not isinstance(payload, bytes) or not 0 < len(payload) <= 32 * 1024 * 1024:
            raise ValueError("candidate artifact must be 1 byte to 32 MiB of JSON bytes")
        if expected_sha256 is not None and hashlib.sha256(payload).hexdigest() != _check_sha(
            expected_sha256
        ):
            raise ValueError("candidate artifact byte checksum mismatch")
        decoded = json.loads(payload)
        if not isinstance(decoded, dict) or "content_sha256" not in decoded:
            raise ValueError("candidate artifact requires a checksummed JSON object")
        body = {key: value for key, value in decoded.items() if key != "content_sha256"}
        if _sha(body) != decoded["content_sha256"] or _encode(decoded) != payload:
            raise ValueError("candidate content checksum/canonical encoding mismatch")
        try:
            if (
                body["contract_version"] != CONTRACT_VERSION
                or body["feature_names"] != list(DAILY_FEATURE_NAMES)
                or body["missing_value_policy"] != MISSING_VALUE_POLICY
                or body["frozen_r3b_gate_claimed"] is not False
                or body["scientific_champion_changed"] is not False
            ):
                raise ValueError("candidate metadata violates its daily-only contract")
            calibrators = tuple(
                (
                    horizon,
                    R3BQuantileCalibrator(
                        quantiles=tuple(item["quantiles"]),
                        offsets=tuple(item["offsets"]),
                        support=item["support"],
                        minimum_support=item["minimum_support"],
                        assessed=item["assessed"],
                        status=item["status"],
                    ),
                )
                for horizon, item in body["calibrators"]
            )
            candidate = cls(
                ensemble=R3BQuantileEnsemble.from_dict(body["ensemble"]),
                training_support=tuple(tuple(pair) for pair in body["training_support"]),
                calibrators=calibrators,
                normalized_training_input_sha256=body["normalized_training_input_sha256"],
                normalized_calibration_input_sha256=body["normalized_calibration_input_sha256"],
                minimum_training_rows=body["minimum_training_rows"],
                minimum_calibration_support=body["minimum_calibration_support"],
            )
            if list(candidate.supported_horizons) != body["supported_horizons"]:
                raise ValueError("supported horizons do not match training support")
            if candidate.to_bytes() != payload:
                raise ValueError("candidate artifact contains unknown or noncanonical fields")
            return candidate
        except (KeyError, TypeError, IndexError) as error:
            raise ValueError("candidate artifact is incomplete or malformed") from error


def fit_daily_candidate(
    training_matrix: Iterable[Iterable[DailyFeatureValue]],
    training_targets: Iterable[DailyNumber],
    training_horizons: Iterable[int],
    *,
    calibration_matrix: Iterable[Iterable[DailyFeatureValue]],
    calibration_targets: Iterable[DailyNumber],
    calibration_horizons: Iterable[int],
    feature_names: Iterable[str] = DAILY_FEATURE_NAMES,
    minimum_training_rows: int = 20,
    minimum_calibration_support: int = 20,
    model_contract: R3BLightGBMContract | None = None,
) -> DailyQuantileCandidate:
    """Fit a global direct daily model and independent held-out horizon offsets.

    The caller must supply held-out calibration rows; this numerical boundary
    cannot infer time, distinct outcome IDs, or label maturity from a matrix.
    Row order is retained in training and input hashes. Empty calibration is
    recorded as explicit insufficient support, never as successful calibration.
    """
    minimum_training_rows = _positive_integer(minimum_training_rows, "minimum_training_rows")
    minimum_calibration_support = _positive_integer(
        minimum_calibration_support, "minimum_calibration_support"
    )
    if minimum_calibration_support < 20:
        raise ValueError("daily calibration requires at least 20 held-out rows per horizon")
    names = tuple(feature_names)
    train, train_horizons = _matrix(training_matrix, training_horizons, names)
    calibration, cal_horizons = _matrix(
        calibration_matrix, calibration_horizons, names, allow_empty=True
    )
    targets = tuple(_number(value, "training target") for value in training_targets)
    cal_targets = tuple(_number(value, "calibration target") for value in calibration_targets)
    if len(train) != len(targets) or len(calibration) != len(cal_targets):
        raise ValueError("feature and target row counts must match in each split")
    if len(train) < minimum_training_rows:
        raise ValueError("insufficient training rows for the declared minimum")
    if not set(cal_horizons).issubset(set(train_horizons)):
        raise ValueError("calibration includes a horizon absent from training")
    contract = _numerical_contract() if model_contract is None else model_contract
    if not isinstance(contract, R3BLightGBMContract):
        raise ValueError(
            "provide only the numerical LightGBM contract, not a frozen release config"
        )
    ensemble = fit_quantile_ensemble(train, targets, names, contract)
    calibration_predictions = predict_quantiles(ensemble, calibration) if calibration else ()
    support = tuple(sorted(Counter(train_horizons).items()))
    calibrators = []
    for horizon, _ in support:
        indices = [index for index, value in enumerate(cal_horizons) if value == horizon]
        calibrators.append(
            (
                horizon,
                fit_quantile_calibrator(
                    (cal_targets[index] for index in indices),
                    tuple(calibration_predictions[index] for index in indices),
                    minimum_calibration_support,
                ),
            )
        )
    return DailyQuantileCandidate(
        ensemble=ensemble,
        training_support=support,
        calibrators=tuple(calibrators),
        normalized_training_input_sha256=_sha(
            {
                "matrix": train,
                "horizons": train_horizons,
                "targets": targets,
                "missing_value_policy": MISSING_VALUE_POLICY,
            }
        ),
        normalized_calibration_input_sha256=_sha(
            {
                "matrix": calibration,
                "horizons": cal_horizons,
                "targets": cal_targets,
                "missing_value_policy": MISSING_VALUE_POLICY,
            }
        ),
        minimum_training_rows=minimum_training_rows,
        minimum_calibration_support=minimum_calibration_support,
    )
