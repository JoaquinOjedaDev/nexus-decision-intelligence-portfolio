"""Daily candidate numerics: deterministic training, support gates and native reload."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from nexus.modeling.r3_features import R3PointInTimeFeatureRow
from nexus.modeling.r3b_contracts import R3B_DEMAND_FEATURES, R3B_HORIZONS_DAYS
from nexus.modeling.r3b_models import R3BQuantilePrediction
from nexus.portfolio.training_models import (
    CONTRACT_VERSION,
    DAILY_FEATURE_NAMES,
    MISSING_VALUE_POLICY,
    DailyQuantileCandidate,
    fit_daily_candidate,
    matrix_from_features,
)

HORIZONS = (1, 2, 7, 14)


def dataset(per_horizon: int, offset: int = 0) -> dict[str, tuple]:
    matrix, targets, horizons = [], [], []
    for horizon in HORIZONS:
        for index in range(per_horizon):
            lag = index % 10 + horizon
            matrix.append(
                (
                    lag,
                    lag + 2,
                    None if index % 7 == 0 else lag + 1,
                    lag + 0.5,
                    lag + 1.5,
                    0.7,
                    0.8,
                    1,
                    30,
                    index % 7,
                    horizon,
                )
            )
            targets.append(float(lag + (index % 3) + offset))
            horizons.append(horizon)
    return {"matrix": tuple(matrix), "targets": tuple(targets), "horizons": tuple(horizons)}


def train(**changes: Any) -> DailyQuantileCandidate:
    train_data, calibration = dataset(40), dataset(20, offset=2)
    values = dict(
        training_matrix=train_data["matrix"],
        training_targets=train_data["targets"],
        training_horizons=train_data["horizons"],
        calibration_matrix=calibration["matrix"],
        calibration_targets=calibration["targets"],
        calibration_horizons=calibration["horizons"],
    )
    values.update(changes)
    return fit_daily_candidate(**values)


@pytest.fixture(scope="module")
def candidates() -> tuple[DailyQuantileCandidate, DailyQuantileCandidate]:
    pytest.importorskip("lightgbm", reason="actual training needs the optional modeling group")
    return train(), train()


def test_real_daily_training_is_deterministic_without_changing_frozen_horizons(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
) -> None:
    first, second = candidates
    assert first.to_bytes() == second.to_bytes()
    assert first.sha256 == second.sha256
    assert first.ensemble.model_identity == second.ensemble.model_identity
    assert first.supported_horizons == HORIZONS
    assert R3B_HORIZONS_DAYS == (7, 14, 28)
    assert DAILY_FEATURE_NAMES == R3B_DEMAND_FEATURES
    payload = first.to_dict()
    assert payload["contract_version"] == CONTRACT_VERSION
    assert payload["missing_value_policy"] == "zero_with_history_count_signal"
    assert payload["frozen_r3b_gate_claimed"] is False
    assert payload["scientific_champion_changed"] is False
    assert len(first.ensemble.booster_model_strings) == 5
    assert all(model.startswith("tree") for model in first.ensemble.booster_model_strings)
    assert first.ensemble.resolved_params["num_threads"] == 2
    assert first.ensemble.resolved_params["num_boost_round"] == 64


def test_real_calibrated_prediction_and_reload_have_exact_parity(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
) -> None:
    model, _ = candidates
    sample = dataset(2)
    original = model.predict(sample["matrix"], sample["horizons"])
    reloaded = DailyQuantileCandidate.from_bytes(model.to_bytes(), expected_sha256=model.sha256)
    assert reloaded.to_bytes() == model.to_bytes()
    assert reloaded.sha256 == hashlib.sha256(model.to_bytes()).hexdigest()
    assert reloaded.predict(sample["matrix"], sample["horizons"]) == original
    assert all(isinstance(row, R3BQuantilePrediction) for row in original)
    assert all(tuple(sorted(row.values)) == row.values and row.q025 >= 0 for row in original)
    assert model.calibration_diagnostics["all_trained_horizons_assessed"] is True
    assert model.calibration_diagnostics["coverage_guarantee_claimed"] is False
    assert all(
        value["training_rows"] == 40 and value["calibration_rows"] == 20
        for value in model.calibration_diagnostics["horizons"].values()
    )


def test_insufficient_horizon_abstains_without_silent_raw_fallback(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
) -> None:
    calibration = dataset(19, offset=2)
    model = train(
        calibration_matrix=calibration["matrix"],
        calibration_targets=calibration["targets"],
        calibration_horizons=calibration["horizons"],
    )
    sample = dataset(1)
    assert model.calibration_diagnostics["all_trained_horizons_assessed"] is False
    assert all(
        value["calibration_rows"] == 19
        for value in model.calibration_diagnostics["horizons"].values()
    )
    with pytest.raises(ValueError, match="insufficient calibration support"):
        model.predict(sample["matrix"], sample["horizons"])
    assert len(model.predict_raw(sample["matrix"], sample["horizons"])) == len(HORIZONS)
    assert (
        DailyQuantileCandidate.from_bytes(model.to_bytes()).calibration_diagnostics
        == model.calibration_diagnostics
    )


def test_empty_calibration_persists_zero_support(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
) -> None:
    model = train(calibration_matrix=(), calibration_targets=(), calibration_horizons=())
    assert all(item.support == 0 and not item.assessed for _, item in model.calibrators)
    sample = dataset(1)
    with pytest.raises(ValueError, match="insufficient"):
        model.predict(sample["matrix"], sample["horizons"])


def test_calibration_is_per_horizon_and_preserves_requested_row_order(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
) -> None:
    model, _ = candidates
    sample = dataset(1)
    forward = model.predict(sample["matrix"], sample["horizons"])
    reverse = model.predict(sample["matrix"][::-1], sample["horizons"][::-1])
    assert reverse == forward[::-1]
    # Explicit horizon offsets are persisted, not pooled across 1/2/7/14 days.
    assert tuple(horizon for horizon, _ in model.calibrators) == HORIZONS
    assert all(item.minimum_support == 20 for _, item in model.calibrators)


def test_missing_values_match_declared_zero_policy(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
) -> None:
    model, _ = candidates
    sample = dataset(1)
    zeroed = tuple(
        tuple(0 if value is None else value for value in row) for row in sample["matrix"]
    )
    assert model.predict_raw(sample["matrix"], sample["horizons"]) == model.predict_raw(
        zeroed, sample["horizons"]
    )
    assert MISSING_VALUE_POLICY == "zero_with_history_count_signal"


def test_artifact_rejects_byte_wrapper_and_native_model_corruption(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
) -> None:
    model, _ = candidates
    with pytest.raises(ValueError, match="byte checksum"):
        DailyQuantileCandidate.from_bytes(model.to_bytes(), expected_sha256="c" * 64)
    corrupted = model.to_dict()
    corrupted["minimum_calibration_support"] = 1
    with pytest.raises(ValueError, match="checksum"):
        DailyQuantileCandidate.from_bytes(_json(corrupted))
    corrupted = model.to_dict()
    corrupted["ensemble"]["booster_model_strings"][0] += "\ncorrupted native model"
    _reseal(corrupted)
    with pytest.raises(ValueError, match="model_identity"):
        DailyQuantileCandidate.from_bytes(_json(corrupted))


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _reseal(envelope: dict[str, Any]) -> None:
    envelope["content_sha256"] = hashlib.sha256(
        _json({key: value for key, value in envelope.items() if key != "content_sha256"})
    ).hexdigest()


@pytest.mark.parametrize(
    "field,value",
    [
        ("contract_version", "nexus.r3b.forged-gate"),
        ("missing_value_policy", "invented_minus_one"),
        ("frozen_r3b_gate_claimed", True),
        ("scientific_champion_changed", True),
        ("supported_horizons", [1, 2, 7, 28]),
    ],
)
def test_resealed_artifact_cannot_override_runtime_contract(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
    field: str,
    value: object,
) -> None:
    envelope = candidates[0].to_dict()
    envelope[field] = value
    _reseal(envelope)
    with pytest.raises(ValueError):
        DailyQuantileCandidate.from_bytes(_json(envelope))


def test_artifact_rejects_unknown_fields_and_impossible_calibration_support(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
) -> None:
    envelope = candidates[0].to_dict()
    envelope["unexpected_field"] = True
    _reseal(envelope)
    with pytest.raises(ValueError, match="unknown"):
        DailyQuantileCandidate.from_bytes(_json(envelope))
    envelope = candidates[0].to_dict()
    envelope["calibrators"][0][1]["support"] = 19
    _reseal(envelope)
    with pytest.raises(ValueError, match="assessment"):
        DailyQuantileCandidate.from_bytes(_json(envelope))


def test_training_row_order_remains_in_identity(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
) -> None:
    rows = dataset(40)
    reordered = train(
        training_matrix=rows["matrix"][::-1],
        training_targets=rows["targets"][::-1],
        training_horizons=rows["horizons"][::-1],
    )
    assert (
        reordered.normalized_training_input_sha256 != candidates[0].normalized_training_input_sha256
    )
    assert reordered.ensemble.training_identity != candidates[0].ensemble.training_identity


@pytest.mark.parametrize(
    "change",
    [
        {"feature_names": DAILY_FEATURE_NAMES[::-1]},
        {"training_targets": (1,)},
        {"training_matrix": (), "training_targets": (), "training_horizons": ()},
        {"training_horizons": (1,)},
        {"minimum_training_rows": 10000},
        {"minimum_training_rows": True},
        {"minimum_calibration_support": 0},
        {"model_contract": {}},
    ],
)
def test_invalid_matrix_contract_fails_before_native_training(change: dict) -> None:
    with pytest.raises(ValueError):
        train(**change)


@pytest.mark.parametrize(
    "column,value",
    [
        (0, float("nan")),
        (1, float("inf")),
        (2, -1),
        (3, True),
        (5, 1.01),
        (6, 2),
        (7, 0.5),
        (8, None),
        (8, 2.5),
        (9, 7),
        (10, 15),
    ],
)
def test_invalid_feature_values_are_rejected(column: int, value: object) -> None:
    data = dataset(40)
    first = list(data["matrix"][0])
    first[column] = value
    with pytest.raises(ValueError):
        train(training_matrix=(tuple(first), *data["matrix"][1:]))


def test_explicit_horizons_must_match_features_and_training_support(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
) -> None:
    data = dataset(40)
    with pytest.raises(ValueError, match="does not match"):
        train(training_horizons=(2, *data["horizons"][1:]))
    sample = dataset(1)
    unknown = (*sample["matrix"][0][:-1], 3)
    with pytest.raises(ValueError, match="untrained"):
        candidates[0].predict_raw((unknown,), (3,))
    with pytest.raises(ValueError, match="absent from training"):
        train(calibration_matrix=(unknown,), calibration_targets=(1,), calibration_horizons=(3,))
    with pytest.raises(ValueError, match="positive integer"):
        train(training_horizons=(True, *data["horizons"][1:]))


def feature_row(horizon: int = 1) -> R3PointInTimeFeatureRow:
    issue = datetime(2026, 9, 1, 4, tzinfo=UTC)
    target = issue.date() + timedelta(days=horizon - 1)
    return R3PointInTimeFeatureRow(
        feature_version="daily-v1",
        origin_name="train",
        issue_time=issue,
        sku_id="SKU-1",
        location_id="LOC-1",
        horizon_day=horizon,
        target_date=target,
        history_count=30,
        max_feature_available_at=issue - timedelta(hours=1),
        lag_1=5,
        lag_7=6,
        lag_14=None,
        seasonal_target_lag_7=999,
        rolling_mean_7=Decimal("5.5"),
        rolling_mean_14=Decimal("6.5"),
        rolling_nonzero_rate_7=Decimal("0.7"),
        rolling_nonzero_rate_14=Decimal("0.8"),
        days_since_nonzero=None,
        target_day_of_week=target.weekday(),
    )


def test_public_feature_mapping_is_exact_and_does_not_add_seasonal_target() -> None:
    row = feature_row()
    assert matrix_from_features((row,)) == (
        (5.0, 6.0, 0.0, 5.5, 6.5, 0.7, 0.8, 0.0, 30.0, 1.0, 1.0),
    )
    assert matrix_from_features(()) == ()
    assert len(matrix_from_features((row,))[0]) == len(DAILY_FEATURE_NAMES)
    assert 999 not in matrix_from_features((row,))[0]
    with pytest.raises(ValueError, match="validated"):
        matrix_from_features(({},))
    with pytest.raises(ValueError, match=r"1\.\.14"):
        matrix_from_features((feature_row(28),))
    with pytest.raises(ValueError, match="finite"):
        matrix_from_features((replace(row, history_count=True),))


def test_artifact_malformed_bytes_fail_closed() -> None:
    for payload in (b"", b"[]", b"{}", b"not json"):
        with pytest.raises(ValueError):
            DailyQuantileCandidate.from_bytes(payload)


def test_calibration_floor_cannot_be_lowered_below_twenty() -> None:
    with pytest.raises(ValueError, match="at least 20"):
        train(minimum_calibration_support=19)


def test_all_daily_horizons_map_without_modifying_frozen_r3b_contract() -> None:
    rows = tuple(feature_row(horizon) for horizon in range(1, 15))
    matrix = matrix_from_features(rows)
    assert tuple(int(row[-1]) for row in matrix) == tuple(range(1, 15))
    assert R3B_HORIZONS_DAYS == (7, 14, 28)


def test_artifact_rejects_mismatched_support_and_thresholds(
    candidates: tuple[DailyQuantileCandidate, DailyQuantileCandidate],
) -> None:
    original, _ = candidates
    for change in (
        {"training_support": ((1, 0),)},
        {"training_support": ((1, 1),), "minimum_training_rows": 20},
        {"training_support": ((1, 20), (1, 20))},
        {"calibrators": original.calibrators[:-1]},
        {"minimum_calibration_support": 19},
        {"minimum_calibration_support": 21},
    ):
        with pytest.raises(ValueError):
            replace(original, **change)


def test_bad_feature_shape_and_untrusted_artifact_digests_fail_closed() -> None:
    inputs = dataset(40)
    with pytest.raises(ValueError, match="ordered numeric"):
        train(training_matrix=({"lag_sales_1": 2}, *inputs["matrix"][1:]))
    with pytest.raises(ValueError, match="exactly 11"):
        train(training_matrix=((1, 2), *inputs["matrix"][1:]))
    with pytest.raises(ValueError, match="lowercase SHA"):
        DailyQuantileCandidate.from_bytes(b"{}", expected_sha256="invalid")
