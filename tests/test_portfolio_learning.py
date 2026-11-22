"""Governed sandbox deployment drills never mutate scientific champion authority."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from nexus.contracts import ActorRole
from nexus.modeling.r3b_contracts import R3BConfig, load_r3b_config
from nexus.modeling.r3b_evaluation import (
    R3BEvaluationResult,
    R3BPredictionRecord,
    R3BTruthRecord,
    evaluate_candidate_against_baseline,
)
from nexus.modeling.r3b_models import R3BQuantileCalibrator
from nexus.portfolio.auth import Actor
from nexus.portfolio.learning import (
    SCIENTIFIC_CHAMPION,
    CandidateEvaluation,
    LearningBook,
    ModelArtifact,
    ShadowOutput,
)

NOW = datetime(2026, 9, 12, 12, tzinfo=UTC)
SOURCE, MONITOR = "a" * 64, "b" * 64
LOCATIONS = ("LOC-1", "LOC-2")
PLANNER = Actor("planner", "Planner", ActorRole.PLANNER, LOCATIONS)
WORKER = Actor("training-worker", "Internal worker", ActorRole.SYSTEM, LOCATIONS)
APPROVER = Actor("approver", "Approver", ActorRole.APPROVER, LOCATIONS)
AUDITOR = Actor("auditor", "Auditor", ActorRole.AUDITOR, LOCATIONS)
BASELINE = ModelArtifact.from_bytes("baseline", b"test-only baseline artifact")
CANDIDATE = ModelArtifact.from_bytes("candidate", b"test-only candidate artifact")


@pytest.fixture(scope="module")
def config() -> R3BConfig:
    return load_r3b_config()


@pytest.fixture(scope="module")
def evaluation(config: R3BConfig) -> R3BEvaluationResult:
    final_origin = config.evaluation.rolling_origins[-1].issue_time
    candidate, baseline, truths = [], [], []
    for index in range(10):
        for horizon in config.forecast_contract.horizons_days:
            target = final_origin.date() + timedelta(days=horizon - 1)
            prediction = R3BPredictionRecord(
                sku_id=f"SKU-{index}",
                location_id="LOC-1",
                target_date=target,
                horizon_days=horizon,
                q025=Decimal(10),
                q10=Decimal(10),
                q50=Decimal(10),
                q90=Decimal(10),
                q975=Decimal(10),
                cold_start=False,
            )
            candidate.append(prediction)
            baseline.append(
                replace(
                    prediction,
                    q025=Decimal(20),
                    q10=Decimal(20),
                    q50=Decimal(20),
                    q90=Decimal(20),
                    q975=Decimal(20),
                )
            )
            truths.append(
                R3BTruthRecord(
                    sku_id=prediction.sku_id,
                    location_id="LOC-1",
                    target_date=target,
                    horizon_days=horizon,
                    actual=Decimal(10),
                    synthetic_cohort="smooth",
                    label_available_at=datetime.combine(
                        target + timedelta(days=1), datetime.min.time(), UTC
                    ),
                )
            )
    return evaluate_candidate_against_baseline(candidate, baseline, truths, config=config)


def _calibrators(config: R3BConfig, support: int = 20) -> dict[int, R3BQuantileCalibrator]:
    return {
        horizon: R3BQuantileCalibrator(
            quantiles=(0.025, 0.10, 0.50, 0.90, 0.975),
            offsets=(0.0, 0.0, 0.0, 0.0, 0.0),
            support=support,
            minimum_support=20,
            assessed=support >= 20,
            status="ASSESSED" if support >= 20 else "ABSTAIN_INSUFFICIENT_EVIDENCE",
        )
        for horizon in config.forecast_contract.horizons_days
    }


def _receipt(evaluation: R3BEvaluationResult, config: R3BConfig, **changes) -> CandidateEvaluation:
    values = dict(
        config=config,
        calibrators=_calibrators(config),
        verification_checks={"local_test_receipt": True},
        evaluation_cutoff=NOW,
        source_snapshot_sha256=SOURCE,
        monitor_evaluation_sha256=MONITOR,
        artifact_sha256=CANDIDATE.sha256,
        baseline_artifact_sha256=BASELINE.sha256,
    )
    values.update(changes)
    return CandidateEvaluation.from_r3b(evaluation, **values)


def _book(tmp_path: Path) -> LearningBook:
    return LearningBook(
        tmp_path / "learning.sqlite3",
        initial_artifact=BASELINE,
        locations=LOCATIONS,
        clock=lambda: NOW,
    )


def _register(book: LearningBook, receipt: CandidateEvaluation) -> str:
    proposal = book.propose_training(
        PLANNER,
        source_snapshot_sha256=SOURCE,
        monitor_evaluation_sha256=MONITOR,
        reason="measured review needed",
    )
    return book.register_candidate(
        WORKER, proposal_id=proposal, artifact=CANDIDATE, evaluation=receipt
    )


def _outputs(count: int = 10, median: int = 10) -> tuple[ShadowOutput, ...]:
    return tuple(
        ShadowOutput(
            f"case-{index}", hashlib.sha256(str(index).encode()).hexdigest(), (Decimal(median),) * 5
        )
        for index in range(count)
    )


def _shadow(book: LearningBook, candidate_id: str) -> dict:
    return book.record_shadow(
        WORKER,
        candidate_id=candidate_id,
        baseline_artifact_sha256=BASELINE.sha256,
        candidate_artifact_sha256=CANDIDATE.sha256,
        baseline=_outputs(),
        candidate=_outputs(median=11),
    )


def test_new_book_requires_artifact_scope_and_reopens_without_reset(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="initial artifact"):
        LearningBook(tmp_path / "empty.sqlite3")
    book = _book(tmp_path)
    state = book.snapshot()
    assert state["mode"] == "sandbox-deployment-rehearsal"
    assert state["training_performed"] is False
    assert state["authoritative_scientific_champion"] == SCIENTIFIC_CHAMPION
    assert state["sandbox_deployment"]["artifact_sha256"] == BASELINE.sha256
    assert LearningBook(book.path).snapshot() == state
    with pytest.raises(ValueError, match="cannot be replaced"):
        LearningBook(book.path, initial_artifact=CANDIDATE)
    with pytest.raises(ValueError, match="scope cannot change"):
        LearningBook(book.path, locations=("LOC-1",))


def test_proposal_is_hash_keyed_durable_and_idempotent(tmp_path: Path) -> None:
    book = _book(tmp_path)
    proposal = book.propose_training(
        PLANNER, source_snapshot_sha256=SOURCE, monitor_evaluation_sha256=MONITOR, reason="review"
    )
    assert (
        book.propose_training(
            WORKER,
            source_snapshot_sha256=SOURCE,
            monitor_evaluation_sha256=MONITOR,
            reason="same evidence",
        )
        == proposal
    )
    state = LearningBook(book.path).snapshot()
    assert len(state["proposals"]) == 1
    assert state["proposals"][proposal]["creator_id"] == PLANNER.actor_id
    assert state["proposals"][proposal]["status"] == "awaiting_trusted_training_bridge"
    assert len(state["audit"]) == 2


def test_actual_policy_adapter_blocks_synthetic_scientific_promotion(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path
) -> None:
    receipt = _receipt(evaluation, config)
    decision = receipt.to_dict()["scientific_decision"]
    assert decision["status"] == "ELIGIBLE_BUT_BLOCKED_SYNTHETIC"
    assert decision["synthetic_evidence"] is True
    assert decision["promotion_applied"] is False
    book = _book(tmp_path)
    candidate = _register(book, receipt)
    before = book.snapshot()
    with pytest.raises(PermissionError, match="synthetic_evidence_cannot_promote_champion"):
        book.promote_scientific(APPROVER, candidate_id=candidate)
    assert book.snapshot() == before


def test_adapter_recomputes_instead_of_trusting_forged_metric_summary(
    evaluation: R3BEvaluationResult, config: R3BConfig
) -> None:
    forged = replace(
        evaluation, relative_wis_improvement=Decimal(-100), candidate_wis=Decimal(99999)
    )
    assert _receipt(forged, config).sha256 == _receipt(evaluation, config).sha256


@pytest.mark.parametrize("failure", ["lost", "support", "horizon", "gates"])
def test_losses_insufficient_and_failed_gates_cannot_deploy(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path, failure: str
) -> None:
    values = {}
    if failure == "lost":
        evaluation = evaluate_candidate_against_baseline(
            tuple(row.baseline for row in evaluation.rows),
            tuple(row.candidate for row in evaluation.rows),
            tuple(row.truth for row in evaluation.rows),
            config=config,
        )
    elif failure == "support":
        values["calibrators"] = _calibrators(config, support=19)
    elif failure == "horizon":
        values["calibrators"] = {7: _calibrators(config)[7]}
    else:
        values["verification_checks"] = {"deterministic_replay": False}
    receipt = _receipt(evaluation, config, **values)
    assert receipt.to_dict()["scientific_decision"]["status"] != "ELIGIBLE_BUT_BLOCKED_SYNTHETIC"
    book = _book(tmp_path)
    candidate = _register(book, receipt)
    _shadow(book, candidate)
    before = book.snapshot()
    with pytest.raises(PermissionError, match="cannot deploy"):
        book.deploy_sandbox(
            APPROVER, candidate_id=candidate, expected_current_sha256=BASELINE.sha256
        )
    assert book.snapshot() == before


def test_adapter_rejects_immature_empty_or_duplicate_evaluation_rows(
    evaluation: R3BEvaluationResult, config: R3BConfig
) -> None:
    with pytest.raises(ValueError, match="complete and available"):
        _receipt(evaluation, config, evaluation_cutoff=datetime(2026, 3, 1, tzinfo=UTC))
    with pytest.raises(ValueError, match="nonempty"):
        _receipt(replace(evaluation, rows=()), config)
    with pytest.raises(ValueError, match="duplicate"):
        _receipt(replace(evaluation, rows=(*evaluation.rows, evaluation.rows[0])), config)
    with pytest.raises(ValueError, match="boolean"):
        _receipt(evaluation, config, verification_checks={"check": "yes"})


def test_evaluation_and_artifact_bindings_cannot_be_substituted(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path
) -> None:
    book = _book(tmp_path)
    proposal = book.propose_training(
        PLANNER, source_snapshot_sha256=SOURCE, monitor_evaluation_sha256=MONITOR, reason="review"
    )
    for changes in [
        {"source_snapshot_sha256": "c" * 64},
        {"monitor_evaluation_sha256": "c" * 64},
        {"artifact_sha256": BASELINE.sha256},
    ]:
        with pytest.raises(ValueError, match="binding mismatch"):
            book.register_candidate(
                WORKER,
                proposal_id=proposal,
                artifact=CANDIDATE,
                evaluation=_receipt(evaluation, config, **changes),
            )
    assert not book.snapshot()["candidates"]


def test_registration_is_idempotent_and_role_scoped(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path
) -> None:
    book = _book(tmp_path)
    receipt = _receipt(evaluation, config)
    candidate = _register(book, receipt)
    before = book.snapshot()
    assert _register(book, receipt) == candidate
    assert book.snapshot() == before
    proposal = before["candidates"][candidate]["proposal_id"]
    for actor in (PLANNER, APPROVER, AUDITOR, replace(WORKER, locations=("LOC-1",))):
        with pytest.raises(PermissionError):
            book.register_candidate(
                actor, proposal_id=proposal, artifact=CANDIDATE, evaluation=receipt
            )
    for actor in (APPROVER, AUDITOR, replace(PLANNER, locations=())):
        with pytest.raises(PermissionError):
            book.propose_training(
                actor,
                source_snapshot_sha256=SOURCE,
                monitor_evaluation_sha256=MONITOR,
                reason="bad role",
            )


def test_shadow_requires_same_keys_inputs_artifacts_and_minimum(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path
) -> None:
    book = _book(tmp_path)
    candidate = _register(book, _receipt(evaluation, config))
    args = dict(
        candidate_id=candidate,
        baseline_artifact_sha256=BASELINE.sha256,
        candidate_artifact_sha256=CANDIDATE.sha256,
        baseline=_outputs(),
        candidate=_outputs(median=11),
    )
    for changes in [
        {"candidate": _outputs(9)},
        {"candidate": ()},
        {"candidate": (*_outputs(), _outputs()[0])},
        {"candidate": (replace(_outputs()[0], input_sha256="c" * 64), *_outputs()[1:])},
        {"baseline_artifact_sha256": CANDIDATE.sha256},
        {"candidate_artifact_sha256": BASELINE.sha256},
    ]:
        with pytest.raises(ValueError):
            book.record_shadow(WORKER, **{**args, **changes})
    assert not book.snapshot()["shadows"]
    shadow = _shadow(book, candidate)
    assert shadow["case_count"] == 10
    assert shadow["mean_absolute_median_difference"] == "1"
    assert shadow["quality_superiority_inferred"] is False
    before = book.snapshot()
    assert _shadow(book, candidate) == shadow
    assert book.snapshot() == before


def test_deployment_requires_independent_global_approver_and_shadow(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path
) -> None:
    book = _book(tmp_path)
    candidate = _register(book, _receipt(evaluation, config))
    with pytest.raises(ValueError, match="completed shadow"):
        book.deploy_sandbox(
            APPROVER, candidate_id=candidate, expected_current_sha256=BASELINE.sha256
        )
    _shadow(book, candidate)
    before = book.snapshot()
    for actor in (
        PLANNER,
        WORKER,
        AUDITOR,
        replace(APPROVER, actor_id=PLANNER.actor_id),
        replace(APPROVER, actor_id=WORKER.actor_id),
        replace(APPROVER, locations=("LOC-1",)),
    ):
        with pytest.raises(PermissionError):
            book.deploy_sandbox(
                actor, candidate_id=candidate, expected_current_sha256=BASELINE.sha256
            )
    assert book.snapshot() == before


def test_switch_reopen_rollback_retains_scientific_champion(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path
) -> None:
    book = _book(tmp_path)
    candidate = _register(book, _receipt(evaluation, config))
    _shadow(book, candidate)
    deployed = book.deploy_sandbox(
        APPROVER, candidate_id=candidate, expected_current_sha256=BASELINE.sha256
    )
    assert deployed["sandbox_deployment"]["artifact_sha256"] == CANDIDATE.sha256
    assert deployed["authoritative_scientific_champion"] == SCIENTIFIC_CHAMPION
    assert deployed["scientific_promotion_applied"] is False
    reopened = LearningBook(book.path, clock=lambda: NOW)
    assert reopened.snapshot() == deployed
    with pytest.raises(ValueError, match="stale"):
        reopened.deploy_sandbox(
            APPROVER, candidate_id=candidate, expected_current_sha256=BASELINE.sha256
        )
    with pytest.raises(PermissionError):
        reopened.rollback_sandbox(
            WORKER, expected_current_sha256=CANDIDATE.sha256, reason="worker cannot approve"
        )
    rolled_back = reopened.rollback_sandbox(
        APPROVER, expected_current_sha256=CANDIDATE.sha256, reason="local recovery drill"
    )
    assert rolled_back["sandbox_deployment"]["artifact_sha256"] == BASELINE.sha256
    assert rolled_back["scientific_promotion_applied"] is False
    assert rolled_back["audit"][-1]["kind"] == "sandbox_rolled_back"
    with pytest.raises(ValueError, match="no predecessor"):
        reopened.rollback_sandbox(
            APPROVER, expected_current_sha256=BASELINE.sha256, reason="no predecessor"
        )


def test_concurrent_switches_cannot_double_apply(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path
) -> None:
    book = _book(tmp_path)
    candidate = _register(book, _receipt(evaluation, config))
    _shadow(book, candidate)

    def switch(_: int) -> str:
        try:
            book.deploy_sandbox(
                APPROVER, candidate_id=candidate, expected_current_sha256=BASELINE.sha256
            )
            return "applied"
        except ValueError as error:
            assert "stale" in str(error)
            return "stale"

    with ThreadPoolExecutor(max_workers=2) as workers:
        assert sorted(workers.map(switch, [1, 2])) == ["applied", "stale"]
    assert (
        len([event for event in book.snapshot()["audit"] if event["kind"] == "sandbox_deployed"])
        == 1
    )


@pytest.mark.parametrize("target", ["audit", "artifact"])
def test_persisted_tampering_fails_closed(tmp_path: Path, target: str) -> None:
    book = _book(tmp_path)
    with closing(sqlite3.connect(book.path)) as db, db:
        if target == "audit":
            event = json.loads(db.execute("SELECT payload FROM learning_events").fetchone()[0])
            event["actor_id"] = "tampered"
            db.execute("UPDATE learning_events SET payload=?", (json.dumps(event),))
        else:
            db.execute("UPDATE learning_artifacts SET payload=?", (b"corrupt",))
    with pytest.raises(ValueError, match="mismatch"):
        book.snapshot()


def test_model_artifact_and_shadow_validation() -> None:
    with pytest.raises(ValueError, match="checksum"):
        ModelArtifact("bad", b"artifact", "a" * 64)
    with pytest.raises(ValueError, match="1 byte"):
        ModelArtifact.from_bytes("empty", b"")
    with pytest.raises(ValueError, match="nonblank"):
        ModelArtifact.from_bytes("", b"artifact")
    with pytest.raises(ValueError, match="finite"):
        ShadowOutput("one", SOURCE, (Decimal("NaN"),) * 5)
    with pytest.raises(ValueError, match="monotonic"):
        ShadowOutput("one", SOURCE, (Decimal(1), Decimal(0), Decimal(2), Decimal(3), Decimal(4)))
    with pytest.raises(ValueError, match="five"):
        ShadowOutput("one", SOURCE, ())


def test_receipt_cannot_assert_scientific_promotion(
    evaluation: R3BEvaluationResult, config: R3BConfig
) -> None:
    original = _receipt(evaluation, config)
    with pytest.raises(ValueError, match="checksum"):
        CandidateEvaluation(original.payload, "c" * 64)
    receipt = original.to_dict()
    receipt["scientific_decision"]["promotion_applied"] = True
    payload = json.dumps(receipt).encode()
    with pytest.raises(ValueError, match="boundaries"):
        CandidateEvaluation(payload, hashlib.sha256(payload).hexdigest())


def test_learning_clock_cannot_rewrite_past(tmp_path: Path) -> None:
    book = _book(tmp_path)
    past = LearningBook(book.path, clock=lambda: NOW - timedelta(seconds=1))
    before = past.snapshot()
    with pytest.raises(ValueError, match="backwards"):
        past.propose_training(
            PLANNER,
            source_snapshot_sha256=SOURCE,
            monitor_evaluation_sha256=MONITOR,
            reason="late operation",
        )
    assert past.snapshot() == before


def test_future_evaluation_receipt_cannot_register(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path
) -> None:
    book = _book(tmp_path)
    receipt = _receipt(evaluation, config, evaluation_cutoff=NOW + timedelta(seconds=1))
    with pytest.raises(ValueError, match="future evaluation"):
        _register(book, receipt)
    assert not book.snapshot()["candidates"]


def test_candidate_must_be_evaluated_against_current_baseline(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path
) -> None:
    book = _book(tmp_path)
    receipt = _receipt(evaluation, config, baseline_artifact_sha256="c" * 64)
    candidate = _register(book, receipt)
    _shadow(book, candidate)
    before = book.snapshot()
    with pytest.raises(ValueError, match="current sandbox baseline"):
        book.deploy_sandbox(
            APPROVER, candidate_id=candidate, expected_current_sha256=BASELINE.sha256
        )
    assert book.snapshot() == before


def test_shadow_renamed_inputs_cannot_inflate_case_minimum(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path
) -> None:
    book = _book(tmp_path)
    candidate = _register(book, _receipt(evaluation, config))
    copied_inputs = tuple(replace(row, input_sha256=SOURCE) for row in _outputs())
    with pytest.raises(ValueError, match="distinct input cases"):
        book.record_shadow(
            WORKER,
            candidate_id=candidate,
            baseline_artifact_sha256=BASELINE.sha256,
            candidate_artifact_sha256=CANDIDATE.sha256,
            baseline=copied_inputs,
            candidate=copied_inputs,
        )


def test_rollback_maker_checker_and_stale_guard(
    evaluation: R3BEvaluationResult, config: R3BConfig, tmp_path: Path
) -> None:
    book = _book(tmp_path)
    candidate = _register(book, _receipt(evaluation, config))
    _shadow(book, candidate)
    book.deploy_sandbox(APPROVER, candidate_id=candidate, expected_current_sha256=BASELINE.sha256)
    before = book.snapshot()
    with pytest.raises(PermissionError, match="independent approver"):
        book.rollback_sandbox(
            replace(APPROVER, actor_id=PLANNER.actor_id),
            expected_current_sha256=CANDIDATE.sha256,
            reason="same creator in another role",
        )
    with pytest.raises(ValueError, match="stale"):
        book.rollback_sandbox(
            APPROVER, expected_current_sha256=BASELINE.sha256, reason="stale view"
        )
    assert book.snapshot() == before


def test_adapter_rejects_wrong_origin_and_impossible_label_availability(
    evaluation: R3BEvaluationResult, config: R3BConfig
) -> None:
    first = evaluation.rows[0]
    with pytest.raises(ValueError, match="untouched final-stress"):
        _receipt(
            replace(
                evaluation,
                rows=(
                    replace(
                        first,
                        truth=replace(
                            first.truth, target_date=first.truth.target_date - timedelta(days=1)
                        ),
                    ),
                    *evaluation.rows[1:],
                ),
            ),
            config,
        )
    with pytest.raises(ValueError, match="complete and available"):
        _receipt(
            replace(
                evaluation,
                rows=(
                    replace(
                        first,
                        truth=replace(
                            first.truth,
                            label_available_at=datetime.combine(
                                first.truth.target_date, datetime.min.time(), UTC
                            ),
                        ),
                    ),
                    *evaluation.rows[1:],
                ),
            ),
            config,
        )


@pytest.mark.parametrize("bad_calibrators", [{True: None}, {7: "not-a-calibrator"}])
def test_adapter_rejects_unvalidated_support_objects(
    evaluation: R3BEvaluationResult, config: R3BConfig, bad_calibrators: dict
) -> None:
    with pytest.raises(ValueError, match="calibrator"):
        _receipt(evaluation, config, calibrators=bad_calibrators)


@pytest.mark.parametrize("value", [0, -1, True])
def test_invalid_shadow_case_minimum(tmp_path: Path, value: int) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        LearningBook(tmp_path / "invalid.sqlite3", minimum_shadow_cases=value)
