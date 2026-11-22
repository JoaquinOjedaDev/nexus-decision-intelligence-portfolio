"""Actual daily training through the temporal pipeline, without dbt or paid APIs.

The snapshot below is a validated in-memory test input, not fabricated evidence
that the Gold/source bridge was executed. That bridge has separate dbt tests.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections import Counter
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from nexus.contracts import ActorRole
from nexus.modeling.r3_artifacts import canonical_json_sha256
from nexus.modeling.r3_metrics import BootstrapInterval
from nexus.modeling.r3b_contracts import R3B_HORIZONS_DAYS, load_r3b_config
from nexus.portfolio import training_pipeline as pipeline
from nexus.portfolio.auth import Actor
from nexus.portfolio.learning import SCIENTIFIC_CHAMPION, LearningBook, ModelArtifact
from nexus.portfolio.training_data import (
    TrainingDataError,
    TrainingLabel,
    TrainingSnapshot,
    pit_history,
    validate_training_snapshot,
)
from nexus.portfolio.training_models import DailyQuantileCandidate, matrix_from_features
from nexus.portfolio.training_pipeline import (
    LearningPlan,
    LearningRun,
    build_learning_rows,
    load_learning_plan,
    persist_learning_run,
    run_learning_experiment,
    sandbox_evaluation,
    verify_learning_run,
)

ROOT = Path(__file__).resolve().parents[1]
KEYS = tuple((f"SKU-{sku}", f"LOC-{loc}") for sku in (1, 2) for loc in (1, 2))
MONITOR_SHA = hashlib.sha256(b"unit-test monitor evidence").hexdigest()


def _rehash(snapshot: TrainingSnapshot, **changes: Any) -> TrainingSnapshot:
    updated = replace(snapshot, **changes)
    payload = updated.to_dict()
    payload.pop("snapshot_sha256")
    updated = replace(updated, snapshot_sha256=canonical_json_sha256(payload))
    validate_training_snapshot(updated)
    return updated


@pytest.fixture(scope="module")
def plan() -> LearningPlan:
    return load_learning_plan(ROOT / "configs/portfolio/runtime-learning.yaml")


@pytest.fixture(scope="module")
def snapshot(plan: LearningPlan) -> TrainingSnapshot:
    labels = []
    start = date(2025, 6, 1)
    for series, (sku, location) in enumerate(KEYS):
        for index in range(365):
            day = start + timedelta(days=index)
            # Stable seasonal signal plus bounded deterministic variation. This
            # benchmark deliberately makes no claim that the challenger wins.
            quantity = 15 + series * 8 + day.weekday() * 3 + (index * 17 + series) % 11
            labels.append(
                TrainingLabel(
                    sku_id=sku,
                    location_id=location,
                    demand_date=day,
                    quantity=quantity,
                    available_at=datetime.combine(
                        day + timedelta(days=1), datetime.min.time(), UTC
                    ),
                    correction_closed_at=datetime.combine(
                        day + timedelta(days=4), datetime.min.time(), UTC
                    ),
                    fulfillment_record_count=1,
                    source_rows_sha256=canonical_json_sha256([sku, location, str(day), quantity]),
                )
            )

    def unit_hash(name: str) -> str:
        return hashlib.sha256(f"in-memory test-only {name}".encode()).hexdigest()

    value = TrainingSnapshot(
        status="READY",
        labels=tuple(sorted(labels, key=lambda row: row.key)),
        exclusions=(),
        reason_codes=(),
        source_snapshot_sha256=unit_hash("source"),
        source_manifest_file_sha256=unit_hash("manifest"),
        gold_demand_sha256=unit_hash("demand"),
        gold_fulfillment_sha256=unit_hash("fulfillment"),
        finalization_sha256=unit_hash("explicit finalization"),
        as_of=plan.evaluation_cutoff,
        minimum_history_days=180,
        minimum_mature_labels=500,
        counts_by_series=tuple((f"{sku}:{location}", 365) for sku, location in KEYS),
        snapshot_sha256="0" * 64,
    )
    return _rehash(value)


@pytest.fixture(scope="module")
def runs(snapshot: TrainingSnapshot, plan: LearningPlan) -> tuple[LearningRun, LearningRun]:
    pytest.importorskip("lightgbm", reason="actual candidate fitting requires the modeling extra")
    first = run_learning_experiment(ROOT, snapshot, plan)
    second = run_learning_experiment(ROOT, snapshot, plan)
    return first, second


@pytest.mark.parametrize(
    "changes",
    [
        {"training_end": "2025-12-05"},
        {"calibration_start": "2025-11-30"},
        {"evaluation_start": "2026-02-27"},
        {"evaluation_cutoff": "2026-05-27T00:00:00Z"},
        {"fit_at": "2025-12-05T00:00:00+03:00"},
        {"horizon_days": 28},
        {"origin_stride_days": 7},
        {"minimum_training_rows": 499},
        {"minimum_calibration_support_per_horizon": 19},
        {"minimum_evaluation_blocks": 19},
        {"bootstrap_resamples": 999},
        {"automatic_promotion": True},
        {"maximum_coverage80_error": 0.10},
        {"maximum_coverage95_error": 0.10},
        {"classification": "production"},
    ],
)
def test_plan_rejects_temporal_overlap_and_relaxed_evidence(
    plan: LearningPlan, changes: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError):
        LearningPlan.model_validate({**plan.model_dump(mode="json"), **changes})


def test_snapshot_mutation_or_wrong_cutoff_is_rejected_before_fit(
    snapshot: TrainingSnapshot, plan: LearningPlan, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pipeline, "fit_daily_candidate", lambda *a, **k: pytest.fail("fit called"))
    with pytest.raises(TrainingDataError, match="checksum"):
        run_learning_experiment(ROOT, replace(snapshot, source_snapshot_sha256="a" * 64), plan)
    later = _rehash(snapshot, as_of=snapshot.as_of + timedelta(days=1))
    with pytest.raises(ValueError, match="exactly match"):
        run_learning_experiment(ROOT, later, plan)


def test_not_ready_snapshot_abstains_without_fitting(
    snapshot: TrainingSnapshot, plan: LearningPlan, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(pipeline, "fit_daily_candidate", lambda *a, **k: pytest.fail("fit called"))
    insufficient = _rehash(
        snapshot,
        minimum_mature_labels=2000,
        status="ABSTAIN_INSUFFICIENT_EVIDENCE",
        reason_codes=("insufficient_distinct_mature_labels",),
    )
    run = run_learning_experiment(ROOT, insufficient, plan)
    assert run.receipt["reason"] == "snapshot_not_ready"
    assert run.candidate is run.baseline_artifact is None
    assert run.receipt["training_performed"] is False
    assert json.loads(persist_learning_run(tmp_path, run).read_text())["candidate_artifact"] is None
    with pytest.raises(ValueError, match="abstained run"):
        sandbox_evaluation(run, monitor_evaluation_sha256=MONITOR_SHA)


@pytest.mark.parametrize("dimension", ["training", "calibration", "evaluation"])
def test_each_support_gate_abstains_before_candidate_fit(
    dimension: str, snapshot: TrainingSnapshot, plan: LearningPlan, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pipeline, "fit_daily_candidate", lambda *a, **k: pytest.fail("fit called"))
    if dimension == "calibration":
        # Delay one target from five separate key-origins. All 14 horizons of
        # each incomplete origin disappear, so every horizon has 19, not 24.
        excluded_dates = {plan.calibration_start + timedelta(days=14 * index) for index in range(5)}
        snapshot = _rehash(
            snapshot,
            labels=tuple(
                row.model_copy(
                    update={"correction_closed_at": plan.calibrated_at + timedelta(days=1)}
                )
                if (row.sku_id, row.location_id) == KEYS[0] and row.demand_date in excluded_dates
                else row
                for row in snapshot.labels
            ),
        )
    else:
        field = "minimum_training_rows" if dimension == "training" else "minimum_evaluation_blocks"
        plan = LearningPlan.model_validate({**plan.model_dump(), field: 10000})
    run = run_learning_experiment(ROOT, snapshot, plan)
    assert run.receipt["status"] == "ABSTAIN_INSUFFICIENT_EVIDENCE"
    assert run.receipt["reason"] == "split_support_not_met"
    assert run.receipt["training_performed"] is False
    if dimension == "calibration":
        assert set(run.receipt["calibration_support"].values()) == {19}
        assert run.receipt["selection"]["calibration"]["excluded_key_origins"] == 5


@pytest.mark.parametrize("delayed_field", ["available_at", "correction_closed_at"])
def test_future_arrival_or_closure_excludes_whole_key_origin_and_pit_history(
    delayed_field: str, snapshot: TrainingSnapshot, plan: LearningPlan
) -> None:
    delayed_day = plan.calibration_start + timedelta(days=6)
    delayed_at = plan.calibrated_at + timedelta(hours=1)
    altered = _rehash(
        snapshot,
        labels=tuple(
            row.model_copy(update={delayed_field: delayed_at})
            if row.key == (*KEYS[0], delayed_day)
            else row
            for row in snapshot.labels
        ),
    )
    diagnostics: dict[str, Any] = {}
    rows = build_learning_rows(
        altered,
        start=plan.calibration_start,
        end=plan.calibration_end,
        labels_at=plan.calibrated_at,
        diagnostics=diagnostics,
    )
    assert len(rows) == 23 * 14
    assert Counter(row.feature.horizon_day for row in rows) == dict.fromkeys(range(1, 15), 23)
    assert not any(
        (row.feature.sku_id, row.feature.location_id) == KEYS[0]
        and row.feature.issue_time.date() == plan.calibration_start
        for row in rows
    )
    assert diagnostics["excluded_key_origins"] == 1
    assert diagnostics["excluded_origins"][0]["missing_target_dates"] == [str(delayed_day)]
    before = pit_history(altered, plan.calibrated_at)
    after = pit_history(altered, delayed_at)
    assert not any(
        (row.sku_id, row.location_id, row.demand_date) == (*KEYS[0], delayed_day) for row in before
    )
    assert any(
        (row.sku_id, row.location_id, row.demand_date) == (*KEYS[0], delayed_day) for row in after
    )


def test_real_training_replay_is_byte_deterministic(runs: tuple[LearningRun, LearningRun]) -> None:
    first, second = runs
    assert first.candidate is not None and second.candidate is not None
    assert first.baseline_artifact is not None and second.baseline_artifact is not None
    assert first.candidate.to_bytes() == second.candidate.to_bytes()
    assert first.baseline_artifact.payload == second.baseline_artifact.payload
    assert first.receipt == second.receipt
    assert first.receipt_sha256 == second.receipt_sha256
    assert first.receipt["training_performed"] is True
    assert first.receipt["replay_prediction_parity"] is True
    assert first.candidate.supported_horizons == tuple(range(1, 15))
    assert R3B_HORIZONS_DAYS == (7, 14, 28)


def test_exactly_twenty_calibration_origins_per_horizon_can_fit(
    snapshot: TrainingSnapshot, plan: LearningPlan
) -> None:
    pytest.importorskip("lightgbm")
    delayed_dates = {plan.calibration_start + timedelta(days=14 * index) for index in range(4)}
    altered = _rehash(
        snapshot,
        labels=tuple(
            row.model_copy(update={"correction_closed_at": plan.calibrated_at + timedelta(days=1)})
            if (row.sku_id, row.location_id) == KEYS[0] and row.demand_date in delayed_dates
            else row
            for row in snapshot.labels
        ),
    )
    run = run_learning_experiment(ROOT, altered, plan)
    assert run.receipt["training_performed"] is True
    assert run.receipt["calibration_support"] == {str(h): 20 for h in range(1, 15)}
    assert run.candidate is not None
    assert all(
        calibrator.assessed and calibrator.support == 20
        for _, calibrator in run.candidate.calibrators
    )
    assert run.receipt["scientific_champion_changed"] is False


def test_training_calibration_final_targets_are_disjoint_and_point_in_time(
    runs: tuple[LearningRun, LearningRun], plan: LearningPlan
) -> None:
    receipt = runs[0].receipt
    key_sets = []
    for name, cutoff, count in (
        ("training", plan.fit_at, 560),
        ("calibration", plan.calibrated_at, 336),
        ("final", plan.evaluation_cutoff, 336),
    ):
        rows = receipt["split_rows"][name]
        assert len(rows) == count == receipt["splits"][name]["distinct_targets"]
        keys = set()
        for row in rows:
            feature, label = row["feature"], row["label"]
            keys.add((feature["sku_id"], feature["location_id"], feature["target_date"]))
            assert datetime.fromisoformat(label["available_at"]) <= cutoff
            assert datetime.fromisoformat(label["correction_closed_at"]) <= cutoff
            assert datetime.fromisoformat(
                feature["max_feature_available_at"]
            ) <= datetime.fromisoformat(feature["issue_time"])
        assert len(keys) == count
        key_sets.append(keys)
    assert not (key_sets[0] & key_sets[1] or key_sets[0] & key_sets[2] or key_sets[1] & key_sets[2])
    assert receipt["calibration_support"] == {str(h): 24 for h in range(1, 15)}
    assert receipt["evaluation_blocks"] == 24
    assert len(receipt["horizon_diagnostics"]) == 14
    assert {row["rows"] for row in receipt["horizon_diagnostics"]} == {24}
    assert "fully fulfilled/finalized" in receipt["selection"]["final"]["selection_boundary"]


def test_reload_predicts_actual_retained_evaluation_matrix(
    runs: tuple[LearningRun, LearningRun],
) -> None:
    run = runs[0]
    assert run.candidate is not None
    loaded = DailyQuantileCandidate.from_bytes(
        run.candidate.to_bytes(), expected_sha256=run.candidate.sha256
    )
    features = tuple(row.feature for row in run.evaluation_rows)
    predictions = loaded.predict(
        matrix_from_features(features), tuple(row.horizon_day for row in features)
    )
    assert predictions == run.candidate_forecasts
    verify_learning_run(run)


@pytest.mark.parametrize(
    "nominal,coverage,passed",
    [
        ("80", "0.749", False),
        ("80", "0.75", True),
        ("80", "0.85", True),
        ("80", "0.851", False),
        ("95", "0.899", False),
        ("95", "0.90", True),
        ("95", "0.999", True),
        ("95", "1.0", True),
    ],
)
def test_coverage_guard_blocks_even_controlled_positive_wis_and_confidence(
    nominal: str,
    coverage: str,
    passed: bool,
    snapshot: TrainingSnapshot,
    plan: LearningPlan,
    runs: tuple[LearningRun, LearningRun],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Controlled metric outputs isolate gate wiring; they are not a measured
    # benchmark result and are never retained or presented as training evidence.
    candidate = runs[0].candidate
    assert candidate is not None
    monkeypatch.setattr(pipeline, "fit_daily_candidate", lambda *a, **k: candidate)
    original_metric = pipeline.mean_weighted_interval_score
    aggregate_calls = 0

    def score(rows: Any) -> Decimal:
        nonlocal aggregate_calls
        if len(rows) == 336:
            aggregate_calls += 1
            return Decimal(1 if aggregate_calls == 1 else 2)
        return original_metric(rows)

    monkeypatch.setattr(pipeline, "mean_weighted_interval_score", score)
    predictions = runs[0].candidate_forecasts
    intervals = {
        level: (
            tuple(Decimal(str(getattr(row, low))) for row in predictions),
            tuple(Decimal(str(getattr(row, high))) for row in predictions),
        )
        for level, low, high in (("80", "q10", "q90"), ("95", "q025", "q975"))
    }
    assert intervals["80"] != intervals["95"]

    def coverage_for_bounds(actuals: Any, lower: Any, upper: Any) -> Decimal:
        assert len(actuals) == 336
        for level, bounds in intervals.items():
            if (tuple(lower), tuple(upper)) == bounds:
                return Decimal(coverage if nominal == level else "0." + level)
        pytest.fail("Coverage check did not use the candidate's declared interval endpoints")

    monkeypatch.setattr(pipeline, "central_interval_coverage", coverage_for_bounds)
    monkeypatch.setattr(
        pipeline,
        "block_bootstrap_95",
        lambda *a, **k: BootstrapInterval(
            Decimal(1), Decimal("0.5"), Decimal("1.5"), Decimal("0.95"), 1000, 20260718, 24
        ),
    )
    run = run_learning_experiment(ROOT, snapshot, plan)
    assert Decimal(run.receipt["metrics"]["relative_wis_improvement"]) > Decimal("0.1")
    assert Decimal(run.receipt["metrics"]["paired_improvement_95"]["lower"]) > 0
    assert (
        run.receipt["quality_checks"][f"coverage{nominal}_within_five_percentage_points"] is passed
    )
    other = "95" if nominal == "80" else "80"
    assert run.receipt["quality_checks"][f"coverage{other}_within_five_percentage_points"] is True
    assert run.receipt["metrics"][f"candidate_coverage{nominal}"] == coverage
    expected = "ELIGIBLE_BUT_BLOCKED_SYNTHETIC" if passed else "REJECTED_GATE_FAILURE"
    assert run.receipt["status"] == expected
    assert run.receipt["scientific_decision"]["promotion_applied"] is False


@pytest.mark.parametrize("writer", ["persist", "sandbox"])
@pytest.mark.parametrize("alteration", ["candidate", "baseline", "receipt", "predictions"])
def test_artifact_swap_or_mutated_receipt_cannot_cross_persistence_boundary(
    writer: str, alteration: str, runs: tuple[LearningRun, LearningRun], tmp_path: Path
) -> None:
    run = copy.deepcopy(runs[0])
    assert run.candidate is not None
    if alteration == "candidate":
        run = replace(
            run, candidate=replace(run.candidate, normalized_training_input_sha256="f" * 64)
        )
    elif alteration == "baseline":
        run = replace(
            run, baseline_artifact=ModelArtifact.from_bytes("different_baseline", b"not evaluated")
        )
    elif alteration == "receipt":
        run.receipt["metrics"]["relative_wis_improvement"] = "0.99"
    else:
        run = replace(run, candidate_forecasts=run.candidate_forecasts[:-1])
    with pytest.raises(ValueError, match="learning"):
        if writer == "persist":
            persist_learning_run(tmp_path, run)
        else:
            sandbox_evaluation(run, monitor_evaluation_sha256=MONITOR_SHA)
    assert not list(tmp_path.iterdir())


def test_persist_is_immutable_and_sandbox_receipt_binds_exact_artifacts(
    runs: tuple[LearningRun, LearningRun], tmp_path: Path
) -> None:
    run = runs[0]
    assert run.candidate is not None and run.baseline_artifact is not None
    first = persist_learning_run(tmp_path, run)
    assert persist_learning_run(tmp_path, run) == first
    stored = json.loads(first.read_text())
    restored_bytes = json.dumps(
        stored["candidate_artifact"], allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode()
    assert restored_bytes == run.candidate.to_bytes()
    assessment = sandbox_evaluation(run, monitor_evaluation_sha256=MONITOR_SHA).to_dict()
    assert assessment["artifact_sha256"] == run.candidate.sha256
    assert assessment["baseline_artifact_sha256"] == run.baseline_artifact.sha256
    assert assessment["runtime_learning_receipt_sha256"] == run.receipt_sha256
    assert assessment["monitor_evaluation_sha256"] == MONITOR_SHA
    assert assessment["training_performed"] is True
    assert assessment["scientific_decision"]["promotion_applied"] is False


def test_real_candidate_registration_never_changes_scientific_champion(
    runs: tuple[LearningRun, LearningRun], tmp_path: Path
) -> None:
    run = runs[0]
    assert run.candidate is not None and run.baseline_artifact is not None
    locations = ("LOC-1", "LOC-2")
    planner = Actor("planner", "Planner", ActorRole.PLANNER, locations)
    worker = Actor("worker", "Training worker", ActorRole.SYSTEM, locations)
    approver = Actor("approver", "Approver", ActorRole.APPROVER, locations)
    config_before = load_r3b_config(ROOT / "configs/modeling/r3b-smoke.yaml").model_dump(
        mode="json"
    )
    book = LearningBook(
        tmp_path / "learning.sqlite", initial_artifact=run.baseline_artifact, locations=locations
    )
    proposal = book.propose_training(
        planner,
        source_snapshot_sha256=run.receipt["bindings"]["source_snapshot_sha256"],
        monitor_evaluation_sha256=MONITOR_SHA,
        reason="Isolated integration-test training review",
    )
    registered = book.register_candidate(
        worker,
        proposal_id=proposal,
        artifact=ModelArtifact.from_bytes("runtime_daily_candidate", run.candidate.to_bytes()),
        evaluation=sandbox_evaluation(run, monitor_evaluation_sha256=MONITOR_SHA),
    )
    with pytest.raises(PermissionError, match="synthetic"):
        book.promote_scientific(approver, candidate_id=registered)
    assert book.snapshot()["authoritative_scientific_champion"] == SCIENTIFIC_CHAMPION
    assert run.receipt["scientific_champion_changed"] is False
    assert (
        load_r3b_config(ROOT / "configs/modeling/r3b-smoke.yaml").model_dump(mode="json")
        == config_before
    )
