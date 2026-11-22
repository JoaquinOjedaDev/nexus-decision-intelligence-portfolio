from __future__ import annotations

import json
import os
from collections.abc import Iterator

import pytest

from nexus.modeling.r3b_contracts import (
    R3B_DEMAND_FEATURES,
    R3B_LEAD_TIME_FEATURES,
    R3B_QUANTILES,
    R3BConfig,
    load_r3b_config,
)
from nexus.modeling.r3b_models import (
    R3BEmpiricalQuantileBaseline,
    R3BModelError,
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

pytestmark = pytest.mark.r3b

FEATURE_NAMES = R3B_DEMAND_FEATURES
MATRIX = tuple(
    tuple(float((index + column) % 11) for column in range(len(FEATURE_NAMES)))
    for index in range(30)
)
TARGETS = tuple(float((index % 7) + 1) for index in range(30))


@pytest.fixture(scope="module")
def config() -> R3BConfig:
    return load_r3b_config()


@pytest.fixture(scope="module")
def trained_ensembles(
    config: R3BConfig,
) -> Iterator[tuple[R3BQuantileEnsemble, R3BQuantileEnsemble]]:
    first = fit_quantile_ensemble(MATRIX, TARGETS, FEATURE_NAMES, config)
    second = fit_quantile_ensemble(MATRIX, TARGETS, FEATURE_NAMES, config)
    yield first, second


def test_quantile_ensemble_is_deterministic_and_resolves_the_native_policy(
    trained_ensembles: tuple[R3BQuantileEnsemble, R3BQuantileEnsemble],
) -> None:
    first, second = trained_ensembles

    assert first == second
    assert first.training_identity == second.training_identity
    assert first.model_identity == second.model_identity
    assert first.booster_model_strings == second.booster_model_strings
    assert len(first.training_identity) == len(first.model_identity) == 64
    assert first.feature_names == FEATURE_NAMES
    assert first.quantiles == R3B_QUANTILES
    assert first.resolved_params == {
        "bagging_fraction": 1.0,
        "bagging_freq": 0,
        "bagging_seed": 20260718,
        "boosting_type": "gbdt",
        "data_random_seed": 20260718,
        "deterministic": True,
        "device_type": "cpu",
        "drop_seed": 20260718,
        "early_stopping_rounds": 0,
        "extra_seed": 20260718,
        "feature_fraction": 1.0,
        "feature_fraction_seed": 20260718,
        "force_col_wise": True,
        "metric": "quantile",
        "num_boost_round": 64,
        "num_threads": 2,
        "objective": "quantile",
        "seed": 20260718,
        "verbosity": -1,
    }
    assert os.environ["OMP_NUM_THREADS"] == "2"
    assert os.environ["OMP_DYNAMIC"] == "FALSE"


def test_training_identity_preserves_semantically_meaningful_row_order(
    config: R3BConfig,
    trained_ensembles: tuple[R3BQuantileEnsemble, R3BQuantileEnsemble],
) -> None:
    ordered, _ = trained_ensembles
    reversed_rows = fit_quantile_ensemble(
        tuple(reversed(MATRIX)),
        tuple(reversed(TARGETS)),
        FEATURE_NAMES,
        config,
    )

    assert reversed_rows.training_identity != ordered.training_identity
    assert reversed_rows.model_identity != ordered.model_identity


def test_model_inputs_fail_closed_before_training(config: R3BConfig) -> None:
    with pytest.raises(R3BModelError, match="duplicates"):
        fit_quantile_ensemble(MATRIX, TARGETS, ("lag_sales_1", "lag_sales_1"), config)
    with pytest.raises(R3BModelError, match="unknown or forbidden"):
        fit_quantile_ensemble(MATRIX, TARGETS, ("lag_sales_1", "observed_sales_qty"), config)
    with pytest.raises(R3BModelError, match="exact ordered"):
        fit_quantile_ensemble(MATRIX, TARGETS, FEATURE_NAMES[:-1], config)
    with pytest.raises(R3BModelError, match="columns"):
        fit_quantile_ensemble(((1.0,),), (1.0,), FEATURE_NAMES, config)
    with pytest.raises(R3BModelError, match="same number of rows"):
        fit_quantile_ensemble((MATRIX[0],), (1.0, 2.0), FEATURE_NAMES, config)
    with pytest.raises(R3BModelError, match="at least one row"):
        fit_quantile_ensemble((), (), FEATURE_NAMES, config)
    with pytest.raises(R3BModelError, match="finite"):
        fit_quantile_ensemble(
            ((float("nan"), *MATRIX[0][1:]),),
            (1.0,),
            FEATURE_NAMES,
            config,
        )
    with pytest.raises(R3BModelError, match="nonnegative"):
        fit_quantile_ensemble((MATRIX[0],), (-1.0,), FEATURE_NAMES, config)


def test_prediction_is_nonnegative_monotonic_and_validates_matrix_shape(
    trained_ensembles: tuple[R3BQuantileEnsemble, R3BQuantileEnsemble],
) -> None:
    ensemble, _ = trained_ensembles
    predictions = predict_quantiles(ensemble, MATRIX[:2])

    assert len(predictions) == 2
    assert all(prediction.values == tuple(sorted(prediction.values)) for prediction in predictions)
    assert all(value >= 0.0 for prediction in predictions for value in prediction)
    with pytest.raises(R3BModelError, match="monotonic"):
        R3BQuantilePrediction(1.0, 3.0, 2.0, 4.0, 5.0)
    with pytest.raises(R3BModelError, match="nonnegative"):
        R3BQuantilePrediction(-1.0, 1.0, 2.0, 3.0, 4.0)
    with pytest.raises(R3BModelError, match="columns"):
        predict_quantiles(ensemble, ((1.0,),))
    with pytest.raises(R3BModelError, match="finite"):
        predict_quantiles(ensemble, ((float("inf"), *MATRIX[0][1:]),))


def test_quantile_ensemble_accepts_the_exact_lead_time_feature_contract(
    config: R3BConfig,
) -> None:
    lead_matrix = tuple(
        tuple(float((index + column) % 9 + 1) for column in range(len(R3B_LEAD_TIME_FEATURES)))
        for index in range(24)
    )
    lead_targets = tuple(float(index % 5 + 1) for index in range(24))

    ensemble = fit_quantile_ensemble(
        lead_matrix,
        lead_targets,
        R3B_LEAD_TIME_FEATURES,
        config,
    )
    predictions = predict_quantiles(ensemble, lead_matrix[:2])

    assert ensemble.feature_names == R3B_LEAD_TIME_FEATURES
    assert len(predictions) == 2
    assert all(prediction.values == tuple(sorted(prediction.values)) for prediction in predictions)


def test_calibration_abstains_below_support_and_repairs_crossing() -> None:
    predictions = (
        R3BQuantilePrediction(0.0, 1.0, 2.0, 3.0, 4.0),
        R3BQuantilePrediction(1.0, 2.0, 3.0, 4.0, 5.0),
    )
    abstained = fit_quantile_calibrator((1.0, 2.0), predictions, minimum_support=3)

    assert abstained.assessed is False
    assert abstained.status == "ABSTAIN_INSUFFICIENT_EVIDENCE"
    assert abstained.support == 2
    with pytest.raises(R3BModelError, match="abstained"):
        apply_quantile_calibration(abstained, predictions)

    assessed = fit_quantile_calibrator((1.0, 2.0), predictions, minimum_support=2)
    assert assessed.assessed is True
    assert assessed.status == "ASSESSED"

    crossing_offsets = R3BQuantileCalibrator(
        quantiles=R3B_QUANTILES,
        offsets=(10.0, 0.0, -10.0, 1.0, -1.0),
        support=2,
        minimum_support=2,
        assessed=True,
        status="ASSESSED",
    )
    repaired = apply_quantile_calibration(
        crossing_offsets,
        (R3BQuantilePrediction(1.0, 2.0, 3.0, 4.0, 5.0),),
    )
    assert repaired[0].values == (0.0, 2.0, 4.0, 5.0, 11.0)


def test_calibration_abstains_on_paired_empty_support_and_rejects_one_sided_empty() -> None:
    abstained = fit_quantile_calibrator((), (), minimum_support=20)

    assert abstained.status == "ABSTAIN_INSUFFICIENT_EVIDENCE"
    assert abstained.assessed is False
    assert abstained.support == 0
    assert abstained.offsets == (0.0, 0.0, 0.0, 0.0, 0.0)

    one_prediction = (R3BQuantilePrediction(0.0, 1.0, 2.0, 3.0, 4.0),)
    with pytest.raises(R3BModelError, match="same number of rows"):
        fit_quantile_calibrator((), one_prediction, minimum_support=20)
    with pytest.raises(R3BModelError, match="same number of rows"):
        fit_quantile_calibrator((1.0,), (), minimum_support=20)


def test_empirical_baseline_uses_type_7_quantiles_and_support_gate() -> None:
    baseline = fit_empirical_quantile_baseline(
        (1.0, 2.0, 3.0, 4.0, 5.0),
        R3B_QUANTILES,
        minimum_support=5,
    )

    assert isinstance(baseline, R3BEmpiricalQuantileBaseline)
    assert baseline.assessed is True
    assert baseline.quantile_values == pytest.approx((1.1, 1.4, 3.0, 4.6, 4.9))
    assert predict_empirical_quantiles(baseline, row_count=2) == (
        baseline.prediction,
        baseline.prediction,
    )

    abstained = fit_empirical_quantile_baseline((1.0, 2.0), R3B_QUANTILES, 3)
    assert abstained.status == "ABSTAIN_INSUFFICIENT_EVIDENCE"
    with pytest.raises(R3BModelError, match="insufficient support"):
        predict_empirical_quantiles(abstained)


def test_serialized_boosters_reload_exactly_and_identity_detects_tampering(
    trained_ensembles: tuple[R3BQuantileEnsemble, R3BQuantileEnsemble],
) -> None:
    ensemble, _ = trained_ensembles
    reloaded = R3BQuantileEnsemble.from_json(ensemble.to_json())

    assert reloaded == ensemble
    assert predict_quantiles(reloaded, MATRIX[2:3]) == predict_quantiles(ensemble, MATRIX[2:3])

    tampered = json.loads(ensemble.to_json())
    tampered["booster_model_strings"][0] += "\n"
    with pytest.raises(R3BModelError, match="model_identity"):
        R3BQuantileEnsemble.from_dict(tampered)
