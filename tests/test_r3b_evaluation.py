from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from nexus.modeling.r3b_contracts import R3BConfig, load_r3b_config
from nexus.modeling.r3b_data import R3BEvaluationTruth
from nexus.modeling.r3b_evaluation import (
    R3BEvaluationError,
    R3BPlatformReceipt,
    R3BPredictionRecord,
    R3BReceiptNumber,
    R3BTruthRecord,
    assert_platform_receipts_match,
    build_platform_receipt,
    compare_platform_receipts,
    decide_promotion,
    evaluate_candidate_against_baseline,
    score_prediction,
)
from nexus.r3b import R3BGateError, compare_platform_receipt_files

pytestmark = pytest.mark.r3b


@pytest.fixture(scope="module")
def config() -> R3BConfig:
    return load_r3b_config()


def prediction(
    *,
    sku: str = "SKU-1",
    location: str = "DC-1",
    target: date = date(2026, 3, 3),
    horizon: int = 7,
    values: tuple[int, int, int, int, int] = (10, 10, 10, 10, 10),
    cold_start: bool = False,
) -> R3BPredictionRecord:
    return R3BPredictionRecord(
        sku_id=sku,
        location_id=location,
        target_date=target,
        horizon_days=horizon,
        q025=Decimal(values[0]),
        q10=Decimal(values[1]),
        q50=Decimal(values[2]),
        q90=Decimal(values[3]),
        q975=Decimal(values[4]),
        cold_start=cold_start,
    )


def truth(
    *,
    sku: str = "SKU-1",
    location: str = "DC-1",
    target: date = date(2026, 3, 3),
    horizon: int = 7,
    actual: int = 10,
    cohort: str = "regular",
    label_available_at: datetime = datetime(2026, 3, 4, 4, tzinfo=UTC),
) -> R3BTruthRecord:
    return R3BTruthRecord(
        sku_id=sku,
        location_id=location,
        target_date=target,
        horizon_days=horizon,
        actual=Decimal(actual),
        synthetic_cohort=cohort,
        label_available_at=label_available_at,
    )


def winning_rows() -> tuple[
    tuple[R3BPredictionRecord, ...],
    tuple[R3BPredictionRecord, ...],
    tuple[R3BTruthRecord, ...],
]:
    keys = (
        ("SKU-1", "DC-1", date(2026, 3, 3), 7, False, "regular", 10),
        ("SKU-1", "DC-1", date(2026, 3, 10), 14, False, "regular", 12),
        ("SKU-2", "DC-2", date(2026, 3, 24), 28, True, "cold_start", 8),
    )
    candidate = tuple(
        prediction(
            sku=sku,
            location=location,
            target=target,
            horizon=horizon,
            values=(actual, actual, actual, actual, actual),
            cold_start=cold,
        )
        for sku, location, target, horizon, cold, _cohort, actual in keys
    )
    baseline = tuple(
        prediction(
            sku=sku,
            location=location,
            target=target,
            horizon=horizon,
            values=(actual - 4, actual - 3, actual - 2, actual - 1, actual),
            cold_start=cold,
        )
        for sku, location, target, horizon, cold, _cohort, actual in keys
    )
    truths = tuple(
        truth(
            sku=sku,
            location=location,
            target=target,
            horizon=horizon,
            actual=actual,
            cohort=cohort,
        )
        for sku, location, target, horizon, _cold, cohort, actual in keys
    )
    return candidate, baseline, truths


def test_prediction_and_truth_contracts_reject_invalid_scientific_values() -> None:
    with pytest.raises(R3BEvaluationError, match="monotonic"):
        prediction(values=(0, 3, 2, 4, 5))
    with pytest.raises(R3BEvaluationError, match="nonnegative"):
        prediction(values=(-1, 0, 1, 2, 3))
    with pytest.raises(R3BEvaluationError, match="finite"):
        R3BPredictionRecord(
            "SKU",
            "DC",
            date(2026, 3, 3),
            7,
            Decimal("NaN"),
            Decimal(1),
            Decimal(2),
            Decimal(3),
            Decimal(4),
            False,
        )
    with pytest.raises(R3BEvaluationError, match="horizon"):
        truth(horizon=1)


def test_privileged_truth_adapter_rejects_inactive_or_censored_rows() -> None:
    source = R3BEvaluationTruth(
        sku_id="SKU-1",
        location_id="DC-1",
        target_date=date(2026, 3, 3),
        horizon_days=7,
        observed_sales_qty=5,
        synthetic_latent_demand_qty=8,
        synthetic_cohort="stockout",
        is_active=True,
        stockout_censored=True,
        label_available_at=datetime(2026, 3, 4, tzinfo=UTC),
    )

    with pytest.raises(R3BEvaluationError, match="active and uncensored"):
        R3BTruthRecord.from_evaluation_truth(source)

    adapted = R3BTruthRecord.from_evaluation_truth(replace(source, stockout_censored=False))
    assert adapted.label_available_at == source.label_available_at

    with pytest.raises(R3BEvaluationError, match="explicit timezone"):
        truth(label_available_at=datetime(2026, 3, 4, 4))


def test_evaluation_fails_closed_on_key_or_segmentation_mismatch(config: R3BConfig) -> None:
    candidate = (prediction(),)
    baseline = (prediction(values=(8, 8, 8, 8, 8)),)
    truths = (truth(),)

    with pytest.raises(R3BEvaluationError, match="exactly identical"):
        evaluate_candidate_against_baseline(
            candidate,
            (prediction(target=date(2026, 3, 10)),),
            truths,
            config=config,
        )
    with pytest.raises(R3BEvaluationError, match="duplicate"):
        evaluate_candidate_against_baseline(
            (candidate[0], candidate[0]), baseline, truths, config=config
        )
    with pytest.raises(R3BEvaluationError, match="cold_start metadata"):
        evaluate_candidate_against_baseline(
            candidate,
            (prediction(values=(8, 8, 8, 8, 8), cold_start=True),),
            truths,
            config=config,
        )


def test_per_row_scores_and_equal_weight_aggregate_match_frozen_formulas(
    config: R3BConfig,
) -> None:
    candidate, baseline, truths = winning_rows()
    result = evaluate_candidate_against_baseline(candidate, baseline, truths, config=config)

    assert result.candidate_wis == 0
    assert result.baseline_wis == sum(
        row.baseline_score.weighted_interval_score for row in result.rows
    ) / Decimal(3)
    assert result.absolute_wis_improvement == result.baseline_wis
    assert result.relative_wis_improvement == 1
    assert result.rows[0].candidate_score.pinball_q025 == 0
    expected_first_baseline = score_prediction(10, baseline[0])
    assert result.rows[0].baseline_score == expected_first_baseline

    segments = {(item.dimension, item.value): item for item in result.segment_summaries}
    assert segments[("horizon", "7")].observation_count == 1
    assert segments[("hierarchy", "sku_location")].observation_count == 3
    assert segments[("synthetic_cohort", "regular")].observation_count == 2
    assert segments[("cold_start", "true")].observation_count == 1
    observed_dimension_order = tuple(
        dict.fromkeys(item.dimension for item in result.segment_summaries)
    )
    assert observed_dimension_order == config.evaluation.segment_dimensions
    assert all(item.candidate_wis == 0 for item in result.segment_summaries)


def test_paired_bootstrap_is_seeded_blocked_and_input_permutation_invariant(
    config: R3BConfig,
) -> None:
    candidate, baseline, truths = winning_rows()
    first = evaluate_candidate_against_baseline(candidate, baseline, truths, config=config)
    reversed_result = evaluate_candidate_against_baseline(
        tuple(reversed(candidate)),
        tuple(reversed(baseline)),
        tuple(reversed(truths)),
        config=config,
    )

    assert first.paired_wis_bootstrap == reversed_result.paired_wis_bootstrap
    assert first.paired_wis_bootstrap.block_count == 2
    assert first.paired_wis_bootstrap.resamples == config.evaluation.bootstrap.resamples
    assert first.paired_wis_bootstrap.seed == config.evaluation.bootstrap.seed
    assert first.paired_wis_bootstrap.lower <= first.paired_wis_bootstrap.upper


def test_four_promotion_outcomes_preserve_synthetic_and_human_boundaries(
    config: R3BConfig,
) -> None:
    candidate, baseline, truths = winning_rows()
    winning = evaluate_candidate_against_baseline(candidate, baseline, truths, config=config)
    losing = evaluate_candidate_against_baseline(baseline, candidate, truths, config=config)

    eligible = decide_promotion(
        winning, config=config, evidence_sufficient=True, gate_checks_passed=True
    )
    lost = decide_promotion(
        losing, config=config, evidence_sufficient=True, gate_checks_passed=True
    )
    abstained = decide_promotion(
        winning, config=config, evidence_sufficient=False, gate_checks_passed=True
    )
    rejected = decide_promotion(
        winning, config=config, evidence_sufficient=True, gate_checks_passed=False
    )

    assert eligible.status == "ELIGIBLE_BUT_BLOCKED_SYNTHETIC"
    assert eligible.promotion_applied is False
    assert eligible.synthetic_evidence is True
    assert "human_approval_required" in eligible.blocking_reasons
    assert lost.status == "CHALLENGER_LOST"
    assert lost.engineering_complete is True
    assert abstained.status == "ABSTAIN_INSUFFICIENT_EVIDENCE"
    assert abstained.engineering_complete is False
    assert rejected.status == "REJECTED_GATE_FAILURE"
    assert rejected.engineering_complete is False


def test_promotion_requires_threshold_and_strictly_positive_bootstrap_lower_bound(
    config: R3BConfig,
) -> None:
    candidate, baseline, truths = winning_rows()
    winning = evaluate_candidate_against_baseline(candidate, baseline, truths, config=config)
    threshold = Decimal(str(config.promotion.minimum_wis_improvement))
    at_threshold = replace(winning, relative_wis_improvement=threshold)
    positive_bound = replace(
        at_threshold,
        paired_wis_bootstrap=replace(
            at_threshold.paired_wis_bootstrap, lower=Decimal("0.000000001")
        ),
    )
    zero_bound = replace(
        at_threshold,
        paired_wis_bootstrap=replace(at_threshold.paired_wis_bootstrap, lower=Decimal(0)),
    )

    eligible = decide_promotion(
        positive_bound,
        config=config,
        evidence_sufficient=True,
        gate_checks_passed=True,
    )
    not_confident = decide_promotion(
        zero_bound,
        config=config,
        evidence_sufficient=True,
        gate_checks_passed=True,
    )

    assert eligible.status == "ELIGIBLE_BUT_BLOCKED_SYNTHETIC"
    assert not_confident.status == "CHALLENGER_LOST"
    assert not_confident.engineering_complete is True
    assert not_confident.blocking_reasons == ("paired_bootstrap_lower_bound_not_positive",)


def _receipt(config: R3BConfig, platform: str = "linux-x86_64") -> R3BPlatformReceipt:
    candidate, baseline, truths = winning_rows()
    evaluation = evaluate_candidate_against_baseline(candidate, baseline, truths, config=config)
    promotion = decide_promotion(
        evaluation,
        config=config,
        evidence_sufficient=True,
        gate_checks_passed=True,
    )
    return build_platform_receipt(
        platform=platform,
        semantic_hashes={"config": "a" * 64, "data": "b" * 64},
        evaluation=evaluation,
        promotion=promotion,
        extra_descriptors={"hierarchy.coherence": "passed"},
        extra_numeric_values={"lead_time.support": 24},
    )


def _changed_number(
    receipt: R3BPlatformReceipt, *, path: str, value: Decimal, platform: str
) -> R3BPlatformReceipt:
    payload = deepcopy(receipt.to_dict())
    payload["platform"] = platform
    values = cast(list[dict[str, str]], payload["numeric_values"])
    for item in values:
        if item["path"] == path:
            item["value"] = str(value)
            break
    else:  # pragma: no cover - test helper assertion
        raise AssertionError(path)
    return R3BPlatformReceipt.from_mapping(cast(dict[str, object], payload))


def test_platform_receipt_round_trip_and_tolerance_comparison(config: R3BConfig) -> None:
    linux = _receipt(config)
    reloaded = R3BPlatformReceipt.from_json(linux.to_json())
    candidate_wis = next(
        item.value for item in linux.numeric_values if item.path == "aggregate.candidate_wis"
    )
    windows = _changed_number(
        linux,
        path="aggregate.candidate_wis",
        value=candidate_wis + Decimal("0.0000000005"),
        platform="windows-x86_64",
    )

    assert reloaded == linux
    assert any(item.path == "hierarchy.coherence" for item in linux.descriptors)
    assert any(item.path == "lead_time.support" for item in linux.numeric_values)
    assert any(item.path.endswith(".label_available_at") for item in linux.descriptors)
    comparison = assert_platform_receipts_match(linux, windows, config=config)
    assert comparison.passed is True
    assert comparison.compared_numeric_values == len(linux.numeric_values)
    assert comparison.max_absolute_difference == Decimal("0.0000000005")


def test_file_comparator_requires_one_linux_and_one_windows_receipt(
    config: R3BConfig,
    tmp_path: Path,
) -> None:
    linux = _receipt(config)
    windows_payload = deepcopy(linux.to_dict())
    windows_payload["platform"] = "windows-amd64-python-3.13"
    windows = R3BPlatformReceipt.from_mapping(cast(dict[str, object], windows_payload))
    linux_path = tmp_path / "linux.json"
    windows_path = tmp_path / "windows.json"
    linux_path.write_text(linux.to_json(), encoding="utf-8")
    windows_path.write_text(windows.to_json(), encoding="utf-8")

    assert compare_platform_receipt_files(linux_path, windows_path)["passed"] is True
    with pytest.raises(R3BGateError, match="one Linux and one Windows"):
        compare_platform_receipt_files(linux_path, linux_path)


def test_receipt_comparison_rejects_tolerance_shape_hash_and_nonfinite_values(
    config: R3BConfig,
) -> None:
    linux = _receipt(config)
    outside = _changed_number(
        linux,
        path="aggregate.candidate_wis",
        value=Decimal("0.001"),
        platform="windows-x86_64",
    )
    assert compare_platform_receipts(linux, outside, config=config).passed is False
    with pytest.raises(R3BEvaluationError, match="outside tolerance"):
        assert_platform_receipts_match(linux, outside, config=config)

    missing_payload = deepcopy(linux.to_dict())
    cast(list[object], missing_payload["numeric_values"]).pop()
    missing_payload["platform"] = "windows-x86_64"
    missing = R3BPlatformReceipt.from_mapping(cast(dict[str, object], missing_payload))
    assert (
        "numeric fields are missing or extra"
        in compare_platform_receipts(linux, missing, config=config).mismatches
    )

    hash_payload = deepcopy(linux.to_dict())
    hash_payload["platform"] = "windows-x86_64"
    cast(list[dict[str, str]], hash_payload["semantic_hashes"])[0]["sha256"] = "c" * 64
    hash_mismatch = R3BPlatformReceipt.from_mapping(cast(dict[str, object], hash_payload))
    assert any(
        "semantic hash mismatch" in item
        for item in compare_platform_receipts(linux, hash_mismatch, config=config).mismatches
    )

    unknown_payload = cast(dict[str, Any], deepcopy(linux.to_dict()))
    unknown_payload["unexpected"] = True
    with pytest.raises(R3BEvaluationError, match="extra"):
        R3BPlatformReceipt.from_mapping(unknown_payload)
    with pytest.raises(R3BEvaluationError, match="finite"):
        R3BReceiptNumber("metric", Decimal("Infinity"))
