"""Deterministic matrix-based learned-model primitives for the R3B gate.

The orchestration layer owns point-in-time row construction.  This module deliberately
accepts only numeric matrices, so the learned model cannot reach back into registered
source rows or evaluation-only fields.  LightGBM is an optional modeling dependency and
is imported only after the OpenMP process policy has been fixed.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from numbers import Real
from typing import Any, ClassVar, cast

from nexus.modeling.r3b_contracts import (
    R3B_DEMAND_FEATURES,
    R3B_LEAD_TIME_FEATURES,
    R3B_QUANTILES,
    R3BConfig,
    R3BLightGBMContract,
)

type R3BNumeric = Decimal | float | int
type R3BResolvedParameter = bool | float | int | str

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ASSESSED = "ASSESSED"
_ABSTAIN = "ABSTAIN_INSUFFICIENT_EVIDENCE"


class R3BModelError(ValueError):
    """Raised when an R3B model input or serialized model fails closed."""


def _finite_float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (Decimal, Real)):
        raise R3BModelError(f"{name} must be a real number")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise R3BModelError(f"{name} must be finite")
    return normalized


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise R3BModelError(f"{name} must be a positive integer")
    return value


def _normalize_quantiles(
    quantiles: Iterable[R3BNumeric], *, sort_values: bool
) -> tuple[float, float, float, float, float]:
    try:
        values = tuple(_finite_float(value, "quantile") for value in quantiles)
    except TypeError as error:
        raise R3BModelError("quantiles must be an iterable of real numbers") from error
    if len(values) != 5:
        raise R3BModelError("R3B requires exactly five quantiles")
    if any(value <= 0.0 or value >= 1.0 for value in values):
        raise R3BModelError("quantiles must be strictly between zero and one")
    if len(set(values)) != len(values):
        raise R3BModelError("quantiles must be unique")
    normalized = tuple(sorted(values)) if sort_values else values
    if normalized != tuple(sorted(normalized)):
        raise R3BModelError("quantiles must be strictly increasing")
    return cast(tuple[float, float, float, float, float], normalized)


def _normalize_feature_names(feature_names: Iterable[str]) -> tuple[str, ...]:
    try:
        names = tuple(feature_names)
    except TypeError as error:
        raise R3BModelError("feature_names must be an iterable of strings") from error
    if not names:
        raise R3BModelError("feature_names must not be empty")
    if any(not isinstance(name, str) or not name for name in names):
        raise R3BModelError("feature_names must contain nonempty strings")
    if len(names) != len(set(names)):
        raise R3BModelError("feature_names must not contain duplicates")
    return names


def _normalize_matrix(
    matrix: Iterable[Iterable[R3BNumeric]], *, expected_columns: int
) -> tuple[tuple[float, ...], ...]:
    try:
        raw_rows = tuple(matrix)
    except TypeError as error:
        raise R3BModelError("matrix must be an iterable of numeric rows") from error
    if not raw_rows:
        raise R3BModelError("matrix must contain at least one row")

    rows: list[tuple[float, ...]] = []
    for row_index, raw_row in enumerate(raw_rows):
        if isinstance(raw_row, (str, bytes, Mapping)):
            raise R3BModelError(f"matrix row {row_index} must be a numeric sequence")
        try:
            raw_values = tuple(raw_row)
        except TypeError as error:
            raise R3BModelError(f"matrix row {row_index} must be a numeric sequence") from error
        if len(raw_values) != expected_columns:
            raise R3BModelError(
                f"matrix row {row_index} has {len(raw_values)} columns; expected {expected_columns}"
            )
        rows.append(
            tuple(
                _finite_float(value, f"matrix[{row_index}][{column_index}]")
                for column_index, value in enumerate(raw_values)
            )
        )
    return tuple(rows)


def _normalize_nonnegative_values(
    values: Iterable[R3BNumeric], *, name: str, allow_empty: bool = False
) -> tuple[float, ...]:
    try:
        normalized = tuple(
            _finite_float(value, f"{name}[{index}]") for index, value in enumerate(values)
        )
    except TypeError as error:
        raise R3BModelError(f"{name} must be an iterable of real numbers") from error
    if not normalized and not allow_empty:
        raise R3BModelError(f"{name} must not be empty")
    if any(value < 0.0 for value in normalized):
        raise R3BModelError(f"{name} must be nonnegative")
    return normalized


def _hash_payload(payload: object) -> str:
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_sha256(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise R3BModelError(f"{name} must be a lowercase SHA-256")
    return value


def _load_modeling_runtime() -> tuple[Any, Any]:
    """Load NumPy and LightGBM only after fixing the native thread policy."""

    os.environ["OMP_NUM_THREADS"] = "2"
    os.environ["OMP_DYNAMIC"] = "FALSE"
    try:
        import lightgbm as lgb
        import numpy as np
    except ImportError as error:  # pragma: no cover - exercised in base-only installations
        raise R3BModelError(
            "R3B learned models require the optional 'modeling' dependency group"
        ) from error
    return lgb, np


@dataclass(frozen=True, slots=True)
class R3BQuantilePrediction:
    """One validated nonnegative, monotonic five-quantile prediction."""

    q025: float
    q10: float
    q50: float
    q90: float
    q975: float

    _FIELD_NAMES: ClassVar[tuple[str, ...]] = ("q025", "q10", "q50", "q90", "q975")

    def __post_init__(self) -> None:
        values: list[float] = []
        for field_name in self._FIELD_NAMES:
            value = _finite_float(getattr(self, field_name), field_name)
            if value < 0.0:
                raise R3BModelError("quantile predictions must be nonnegative")
            object.__setattr__(self, field_name, value)
            values.append(value)
        if values != sorted(values):
            raise R3BModelError("quantile predictions must be monotonic")

    @classmethod
    def from_values(cls, values: Iterable[R3BNumeric]) -> R3BQuantilePrediction:
        """Construct a prediction from five values in increasing-quantile order."""

        try:
            normalized = tuple(
                _finite_float(value, f"prediction value {index}")
                for index, value in enumerate(values)
            )
        except TypeError as error:
            raise R3BModelError("prediction values must be iterable") from error
        if len(normalized) != 5:
            raise R3BModelError("a prediction requires exactly five values")
        return cls(*normalized)

    @property
    def values(self) -> tuple[float, float, float, float, float]:
        """Return values in q025, q10, q50, q90, q975 order."""

        return self.q025, self.q10, self.q50, self.q90, self.q975

    @property
    def quantile_values(self) -> tuple[float, float, float, float, float]:
        """Alias used by generic matrix orchestration."""

        return self.values

    def __iter__(self):  # type: ignore[no-untyped-def]
        return iter(self.values)

    def __len__(self) -> int:
        return 5

    def __getitem__(self, index: int) -> float:
        return self.values[index]


def _normalized_resolved_params(
    params: Mapping[str, R3BResolvedParameter],
) -> dict[str, R3BResolvedParameter]:
    normalized: dict[str, R3BResolvedParameter] = {}
    keys = tuple(params)
    if any(not isinstance(key, str) or not key for key in keys):
        raise R3BModelError("resolved parameter names must be nonempty strings")
    for key in sorted(keys):
        value = params[key]
        if isinstance(value, float) and not math.isfinite(value):
            raise R3BModelError("resolved floating-point parameters must be finite")
        if type(value) not in {bool, float, int, str}:
            raise R3BModelError("resolved parameters must be JSON scalar values")
        normalized[key] = value
    if not normalized:
        raise R3BModelError("resolved_params must not be empty")
    return normalized


def _model_identity_payload(
    *,
    feature_names: tuple[str, ...],
    quantiles: tuple[float, float, float, float, float],
    booster_model_strings: tuple[str, ...],
    resolved_params: Mapping[str, R3BResolvedParameter],
    training_identity: str,
) -> dict[str, object]:
    return {
        "booster_model_strings": booster_model_strings,
        "feature_names": feature_names,
        "quantiles": quantiles,
        "resolved_params": dict(resolved_params),
        "schema": "nexus.r3b.quantile-ensemble.v1",
        "training_identity": training_identity,
    }


@dataclass(frozen=True, slots=True)
class R3BQuantileEnsemble:
    """Portable ensemble of five serialized, independently trained boosters."""

    feature_names: tuple[str, ...]
    quantiles: tuple[float, float, float, float, float]
    booster_model_strings: tuple[str, ...]
    resolved_params: Mapping[str, R3BResolvedParameter]
    training_identity: str
    model_identity: str

    def __post_init__(self) -> None:
        feature_names = _normalize_feature_names(self.feature_names)
        quantiles = _normalize_quantiles(self.quantiles, sort_values=False)
        try:
            model_strings = tuple(self.booster_model_strings)
        except TypeError as error:
            raise R3BModelError("booster_model_strings must be iterable") from error
        if len(model_strings) != 5 or any(
            not isinstance(model, str) or not model.strip() for model in model_strings
        ):
            raise R3BModelError("exactly five nonempty booster model strings are required")
        params = _normalized_resolved_params(self.resolved_params)
        training_identity = _validate_sha256(self.training_identity, "training_identity")
        model_identity = _validate_sha256(self.model_identity, "model_identity")
        expected_identity = _hash_payload(
            _model_identity_payload(
                feature_names=feature_names,
                quantiles=quantiles,
                booster_model_strings=model_strings,
                resolved_params=params,
                training_identity=training_identity,
            )
        )
        if model_identity != expected_identity:
            raise R3BModelError("model_identity does not bind the serialized ensemble")
        object.__setattr__(self, "feature_names", feature_names)
        object.__setattr__(self, "quantiles", quantiles)
        object.__setattr__(self, "booster_model_strings", model_strings)
        object.__setattr__(self, "resolved_params", params)
        object.__setattr__(self, "training_identity", training_identity)
        object.__setattr__(self, "model_identity", model_identity)

    @property
    def model_strings(self) -> tuple[str, ...]:
        """Short alias for serialized booster strings."""

        return self.booster_model_strings

    @property
    def booster_strings(self) -> tuple[str, ...]:
        """Compatibility alias for serialized booster strings."""

        return self.booster_model_strings

    @property
    def parameters(self) -> Mapping[str, R3BResolvedParameter]:
        """Compatibility alias for the complete resolved training parameters."""

        return self.resolved_params

    @property
    def training_sha256(self) -> str:
        return self.training_identity

    @property
    def model_sha256(self) -> str:
        return self.model_identity

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible representation containing no live booster state."""

        return {
            "feature_names": list(self.feature_names),
            "quantiles": list(self.quantiles),
            "booster_model_strings": list(self.booster_model_strings),
            "resolved_params": dict(self.resolved_params),
            "training_identity": self.training_identity,
            "model_identity": self.model_identity,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> R3BQuantileEnsemble:
        """Validate and reload a representation returned by :meth:`to_dict`."""

        try:
            return cls(
                feature_names=tuple(cast(Iterable[str], payload["feature_names"])),
                quantiles=cast(
                    tuple[float, float, float, float, float],
                    tuple(cast(Iterable[float], payload["quantiles"])),
                ),
                booster_model_strings=tuple(cast(Iterable[str], payload["booster_model_strings"])),
                resolved_params=cast(
                    Mapping[str, R3BResolvedParameter], payload["resolved_params"]
                ),
                training_identity=cast(str, payload["training_identity"]),
                model_identity=cast(str, payload["model_identity"]),
            )
        except (KeyError, TypeError) as error:
            raise R3BModelError("serialized ensemble payload is incomplete or malformed") from error

    def to_json(self) -> str:
        """Serialize this ensemble with canonical JSON formatting."""

        return json.dumps(
            self.to_dict(),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, payload: str) -> R3BQuantileEnsemble:
        """Reload and self-verify an ensemble from JSON."""

        try:
            decoded = json.loads(payload)
        except (TypeError, json.JSONDecodeError) as error:
            raise R3BModelError("serialized ensemble must be valid JSON") from error
        if not isinstance(decoded, dict):
            raise R3BModelError("serialized ensemble JSON root must be an object")
        return cls.from_dict(cast(dict[str, object], decoded))


def _resolve_contract(
    contract: R3BConfig | R3BLightGBMContract,
) -> tuple[R3BLightGBMContract, tuple[tuple[str, ...], ...], dict[str, object]]:
    if isinstance(contract, R3BConfig):
        return (
            contract.demand_model,
            (
                tuple(contract.feature_contract.allowed_demand_features),
                tuple(contract.feature_contract.allowed_lead_time_features),
            ),
            contract.model_dump(mode="json"),
        )
    if isinstance(contract, R3BLightGBMContract):
        return (
            contract,
            (tuple(R3B_DEMAND_FEATURES), tuple(R3B_LEAD_TIME_FEATURES)),
            contract.model_dump(mode="json"),
        )
    raise R3BModelError("contract must be an R3BConfig or R3BLightGBMContract")


def _resolved_lightgbm_params(
    contract: R3BLightGBMContract,
) -> dict[str, R3BResolvedParameter]:
    seed = contract.seed
    return {
        "bagging_fraction": 1.0,
        "bagging_freq": 0,
        "bagging_seed": contract.bagging_seed,
        "boosting_type": contract.boosting_type,
        "data_random_seed": contract.data_random_seed,
        "deterministic": contract.deterministic,
        "device_type": contract.device_type,
        "drop_seed": seed,
        "early_stopping_rounds": contract.early_stopping_rounds,
        "extra_seed": seed,
        "feature_fraction": 1.0,
        "feature_fraction_seed": contract.feature_fraction_seed,
        "force_col_wise": contract.force_col_wise,
        "metric": contract.metric,
        "num_boost_round": contract.num_boost_round,
        "num_threads": contract.num_threads,
        "objective": contract.objective,
        "seed": seed,
        "verbosity": -1,
    }


def fit_quantile_ensemble(
    matrix: Iterable[Iterable[R3BNumeric]],
    targets: Iterable[R3BNumeric],
    feature_names: Iterable[str],
    contract: R3BConfig | R3BLightGBMContract,
    quantiles: Iterable[R3BNumeric] = R3B_QUANTILES,
) -> R3BQuantileEnsemble:
    """Fit one deterministic native LightGBM booster per requested quantile.

    Row order is intentionally preserved in both training and ``training_identity``.
    Feature order is likewise part of the identity and is never canonicalized.
    """

    names = _normalize_feature_names(feature_names)
    model_contract, allowed_feature_sets, contract_payload = _resolve_contract(contract)
    allowed_features = frozenset(
        name for feature_set in allowed_feature_sets for name in feature_set
    )
    unknown = tuple(name for name in names if name not in allowed_features)
    if unknown:
        raise R3BModelError(f"unknown or forbidden feature names: {unknown!r}")
    if names not in allowed_feature_sets:
        raise R3BModelError(
            "feature_names must match the exact ordered demand or lead-time feature contract"
        )
    rows = _normalize_matrix(matrix, expected_columns=len(names))
    normalized_targets = _normalize_nonnegative_values(targets, name="targets")
    if len(rows) != len(normalized_targets):
        raise R3BModelError("matrix and targets must contain the same number of rows")
    normalized_quantiles = _normalize_quantiles(quantiles, sort_values=True)
    if isinstance(contract, R3BConfig) and normalized_quantiles != tuple(
        contract.forecast_contract.quantiles
    ):
        raise R3BModelError("quantiles must match the R3B forecast contract")

    resolved_params = _resolved_lightgbm_params(model_contract)
    training_identity = _hash_payload(
        {
            "contract": contract_payload,
            "feature_names": names,
            "matrix": rows,
            "quantiles": normalized_quantiles,
            "resolved_params": resolved_params,
            "schema": "nexus.r3b.quantile-training.v1",
            "targets": normalized_targets,
        }
    )

    lgb, np = _load_modeling_runtime()
    data = np.asarray(rows, dtype=np.float64, order="C")
    labels = np.asarray(normalized_targets, dtype=np.float64)
    try:
        dataset = lgb.Dataset(
            data,
            label=labels,
            feature_name=list(names),
            free_raw_data=False,
        )
        training_params = {
            key: value
            for key, value in resolved_params.items()
            if key not in {"early_stopping_rounds", "num_boost_round"}
        }
        model_strings: list[str] = []
        for quantile in normalized_quantiles:
            params = dict(training_params)
            params["alpha"] = quantile
            booster = lgb.train(
                params,
                dataset,
                num_boost_round=model_contract.num_boost_round,
                callbacks=[],
            )
            model_strings.append(
                cast(str, booster.model_to_string(num_iteration=model_contract.num_boost_round))
            )
    except Exception as error:
        raise R3BModelError(f"LightGBM quantile training failed: {error}") from error

    frozen_model_strings = tuple(model_strings)
    model_identity = _hash_payload(
        _model_identity_payload(
            feature_names=names,
            quantiles=normalized_quantiles,
            booster_model_strings=frozen_model_strings,
            resolved_params=resolved_params,
            training_identity=training_identity,
        )
    )
    return R3BQuantileEnsemble(
        feature_names=names,
        quantiles=normalized_quantiles,
        booster_model_strings=frozen_model_strings,
        resolved_params=resolved_params,
        training_identity=training_identity,
        model_identity=model_identity,
    )


def predict_quantiles(
    ensemble: R3BQuantileEnsemble,
    matrix: Iterable[Iterable[R3BNumeric]],
) -> tuple[R3BQuantilePrediction, ...]:
    """Predict, clip at zero, and monotonically rearrange each five-quantile row."""

    if not isinstance(ensemble, R3BQuantileEnsemble):
        raise R3BModelError("ensemble must be an R3BQuantileEnsemble")
    rows = _normalize_matrix(matrix, expected_columns=len(ensemble.feature_names))
    lgb, np = _load_modeling_runtime()
    data = np.asarray(rows, dtype=np.float64, order="C")
    columns: list[tuple[float, ...]] = []
    try:
        for model_string in ensemble.booster_model_strings:
            booster = lgb.Booster(model_str=model_string)
            raw = booster.predict(data, num_iteration=booster.current_iteration(), num_threads=2)
            values = tuple(float(value) for value in raw)
            if len(values) != len(rows) or any(not math.isfinite(value) for value in values):
                raise R3BModelError("LightGBM returned invalid prediction values")
            columns.append(values)
    except R3BModelError:
        raise
    except Exception as error:
        raise R3BModelError(f"LightGBM quantile prediction failed: {error}") from error

    predictions: list[R3BQuantilePrediction] = []
    for row_index in range(len(rows)):
        repaired = sorted(max(0.0, column[row_index]) for column in columns)
        predictions.append(R3BQuantilePrediction.from_values(repaired))
    return tuple(predictions)


def _prediction_rows(
    predictions: object, *, allow_empty: bool = False
) -> tuple[R3BQuantilePrediction, ...]:
    if isinstance(predictions, R3BQuantilePrediction):
        return (predictions,)
    if isinstance(predictions, (str, bytes)):
        raise R3BModelError("predictions must be an iterable of prediction rows")
    try:
        raw_rows = tuple(cast(Iterable[object], predictions))
    except TypeError as error:
        raise R3BModelError("predictions must be an iterable of prediction rows") from error
    if not raw_rows and not allow_empty:
        raise R3BModelError("predictions must not be empty")
    normalized: list[R3BQuantilePrediction] = []
    for row in raw_rows:
        if isinstance(row, R3BQuantilePrediction):
            normalized.append(row)
            continue
        try:
            normalized.append(R3BQuantilePrediction.from_values(cast(Iterable[R3BNumeric], row)))
        except (TypeError, R3BModelError) as error:
            raise R3BModelError("each prediction row must contain five valid values") from error
    return tuple(normalized)


def _empirical_quantile(values: tuple[float, ...], quantile: float) -> float:
    ordered = tuple(sorted(values))
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * quantile
    lower_index = math.floor(rank)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = rank - lower_index
    return ordered[lower_index] + fraction * (ordered[upper_index] - ordered[lower_index])


@dataclass(frozen=True, slots=True)
class R3BQuantileCalibrator:
    """Empirical residual-quantile offsets and their explicit support decision."""

    quantiles: tuple[float, float, float, float, float]
    offsets: tuple[float, float, float, float, float]
    support: int
    minimum_support: int
    assessed: bool
    status: str

    def __post_init__(self) -> None:
        quantiles = _normalize_quantiles(self.quantiles, sort_values=False)
        try:
            offsets = tuple(_finite_float(value, "calibration offset") for value in self.offsets)
        except TypeError as error:
            raise R3BModelError("calibration offsets must be iterable") from error
        if len(offsets) != 5:
            raise R3BModelError("a calibrator requires exactly five offsets")
        minimum_support = _positive_integer(self.minimum_support, "minimum_support")
        if isinstance(self.support, bool) or not isinstance(self.support, int) or self.support < 0:
            raise R3BModelError("support must be a nonnegative integer")
        expected_assessed = self.support >= minimum_support
        expected_status = _ASSESSED if expected_assessed else _ABSTAIN
        if self.assessed is not expected_assessed or self.status != expected_status:
            raise R3BModelError("calibrator assessment status does not match its support")
        object.__setattr__(self, "quantiles", quantiles)
        object.__setattr__(self, "offsets", offsets)
        object.__setattr__(self, "minimum_support", minimum_support)

    @property
    def quantile_offsets(self) -> tuple[float, float, float, float, float]:
        return self.offsets

    @property
    def support_count(self) -> int:
        return self.support


def fit_quantile_calibrator(
    actuals: Iterable[R3BNumeric],
    predictions: object,
    minimum_support: int,
) -> R3BQuantileCalibrator:
    """Fit deterministic per-quantile residual offsets or explicitly abstain."""

    normalized_actuals = _normalize_nonnegative_values(actuals, name="actuals", allow_empty=True)
    normalized_predictions = _prediction_rows(predictions, allow_empty=True)
    if len(normalized_actuals) != len(normalized_predictions):
        raise R3BModelError("actuals and predictions must contain the same number of rows")
    threshold = _positive_integer(minimum_support, "minimum_support")
    support = len(normalized_actuals)
    if support < threshold:
        return R3BQuantileCalibrator(
            quantiles=cast(
                tuple[float, float, float, float, float], tuple(float(q) for q in R3B_QUANTILES)
            ),
            offsets=(0.0, 0.0, 0.0, 0.0, 0.0),
            support=support,
            minimum_support=threshold,
            assessed=False,
            status=_ABSTAIN,
        )

    quantiles = cast(
        tuple[float, float, float, float, float], tuple(float(q) for q in R3B_QUANTILES)
    )
    offsets = tuple(
        _empirical_quantile(
            tuple(
                actual - prediction.values[index]
                for actual, prediction in zip(
                    normalized_actuals, normalized_predictions, strict=True
                )
            ),
            quantile,
        )
        for index, quantile in enumerate(quantiles)
    )
    return R3BQuantileCalibrator(
        quantiles=quantiles,
        offsets=cast(tuple[float, float, float, float, float], offsets),
        support=support,
        minimum_support=threshold,
        assessed=True,
        status=_ASSESSED,
    )


def apply_quantile_calibration(
    calibrator: R3BQuantileCalibrator,
    predictions: object,
) -> tuple[R3BQuantilePrediction, ...]:
    """Apply an assessed calibrator, clipping and repairing any induced crossing."""

    if not isinstance(calibrator, R3BQuantileCalibrator):
        raise R3BModelError("calibrator must be an R3BQuantileCalibrator")
    if not calibrator.assessed:
        raise R3BModelError("cannot apply a calibrator that abstained for insufficient support")
    normalized_predictions = _prediction_rows(predictions)
    calibrated: list[R3BQuantilePrediction] = []
    for prediction in normalized_predictions:
        repaired = sorted(
            max(0.0, value + offset)
            for value, offset in zip(prediction.values, calibrator.offsets, strict=True)
        )
        calibrated.append(R3BQuantilePrediction.from_values(repaired))
    return tuple(calibrated)


@dataclass(frozen=True, slots=True)
class R3BEmpiricalQuantileBaseline:
    """A constant empirical distribution with an explicit support decision."""

    quantiles: tuple[float, float, float, float, float]
    quantile_values: tuple[float, float, float, float, float]
    support: int
    minimum_support: int
    assessed: bool
    status: str

    def __post_init__(self) -> None:
        quantiles = _normalize_quantiles(self.quantiles, sort_values=False)
        prediction = R3BQuantilePrediction.from_values(self.quantile_values)
        minimum_support = _positive_integer(self.minimum_support, "minimum_support")
        if isinstance(self.support, bool) or not isinstance(self.support, int) or self.support < 0:
            raise R3BModelError("support must be a nonnegative integer")
        expected_assessed = self.support >= minimum_support
        expected_status = _ASSESSED if expected_assessed else _ABSTAIN
        if self.assessed is not expected_assessed or self.status != expected_status:
            raise R3BModelError("baseline assessment status does not match its support")
        object.__setattr__(self, "quantiles", quantiles)
        object.__setattr__(self, "quantile_values", prediction.values)
        object.__setattr__(self, "minimum_support", minimum_support)

    @property
    def values(self) -> tuple[float, float, float, float, float]:
        return self.quantile_values

    @property
    def prediction(self) -> R3BQuantilePrediction:
        return R3BQuantilePrediction.from_values(self.quantile_values)

    @property
    def support_count(self) -> int:
        return self.support

    def predict(self, count: int = 1) -> tuple[R3BQuantilePrediction, ...]:
        return predict_empirical_quantiles(self, count)


def fit_empirical_quantile_baseline(
    values: Iterable[R3BNumeric],
    quantiles: Iterable[R3BNumeric] = R3B_QUANTILES,
    minimum_support: int = 20,
) -> R3BEmpiricalQuantileBaseline:
    """Fit deterministic type-7 empirical quantiles with a support gate."""

    normalized_values = _normalize_nonnegative_values(values, name="values")
    normalized_quantiles = _normalize_quantiles(quantiles, sort_values=True)
    threshold = _positive_integer(minimum_support, "minimum_support")
    quantile_values = tuple(
        _empirical_quantile(normalized_values, quantile) for quantile in normalized_quantiles
    )
    assessed = len(normalized_values) >= threshold
    return R3BEmpiricalQuantileBaseline(
        quantiles=normalized_quantiles,
        quantile_values=cast(
            tuple[float, float, float, float, float], tuple(sorted(quantile_values))
        ),
        support=len(normalized_values),
        minimum_support=threshold,
        assessed=assessed,
        status=_ASSESSED if assessed else _ABSTAIN,
    )


def predict_empirical_quantiles(
    baseline: R3BEmpiricalQuantileBaseline,
    count: int | None = None,
    *,
    row_count: int | None = None,
) -> tuple[R3BQuantilePrediction, ...]:
    """Repeat an assessed empirical prediction for a requested positive row count."""

    if not isinstance(baseline, R3BEmpiricalQuantileBaseline):
        raise R3BModelError("baseline must be an R3BEmpiricalQuantileBaseline")
    if not baseline.assessed:
        raise R3BModelError("cannot predict from a baseline with insufficient support")
    if count is not None and row_count is not None:
        raise R3BModelError("count and row_count are mutually exclusive")
    repetitions = _positive_integer(
        1 if count is None and row_count is None else (row_count if count is None else count),
        "count",
    )
    prediction = baseline.prediction
    return tuple(prediction for _ in range(repetitions))


def predict_empirical_quantile_baseline(
    baseline: R3BEmpiricalQuantileBaseline,
    count: int | None = None,
    *,
    row_count: int | None = None,
) -> tuple[R3BQuantilePrediction, ...]:
    """Compatibility alias for :func:`predict_empirical_quantiles`."""

    return predict_empirical_quantiles(baseline, count, row_count=row_count)


__all__ = [
    "R3BEmpiricalQuantileBaseline",
    "R3BModelError",
    "R3BQuantileCalibrator",
    "R3BQuantileEnsemble",
    "R3BQuantilePrediction",
    "apply_quantile_calibration",
    "fit_empirical_quantile_baseline",
    "fit_quantile_calibrator",
    "fit_quantile_ensemble",
    "predict_empirical_quantile_baseline",
    "predict_empirical_quantiles",
    "predict_quantiles",
]
