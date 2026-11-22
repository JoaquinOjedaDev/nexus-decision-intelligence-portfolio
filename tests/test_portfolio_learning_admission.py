"""Admission journal tests; constructed inputs do not claim the Gold bridge ran."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import pytest
from test_portfolio_training_pipeline import plan as plan
from test_portfolio_training_pipeline import snapshot as snapshot

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.learning import (
    CandidateEvaluation,
    LearningBook,
    ModelArtifact,
    ShadowOutput,
    TrainingAdmission,
)
from nexus.portfolio.training_pipeline import (
    LearningRun,
    run_learning_experiment,
    sandbox_evaluation,
)

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 12, 12, tzinfo=UTC)
LOCATIONS = ("LOC-1", "LOC-2")
WORKER = Actor("admission-worker", "Trusted worker", ActorRole.SYSTEM, LOCATIONS)
PLANNER = Actor("planner", "Planner", ActorRole.PLANNER, LOCATIONS)
APPROVER = Actor("approver", "Independent approver", ActorRole.APPROVER, LOCATIONS)
BASELINE = ModelArtifact.from_bytes("baseline", b"constructed current baseline")


def _sha(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _book(tmp_path, clock=lambda: NOW):
    return LearningBook(
        tmp_path / "learning.sqlite3", initial_artifact=BASELINE, locations=LOCATIONS, clock=clock
    )


def _dependency_version(name):
    try:
        return version(name)
    except PackageNotFoundError:
        # Receipt-shape unit tests do not require the optional trainer installed.
        return "not-installed-constructed-unit-binding"


@pytest.fixture(scope="module")
def experiment(snapshot, plan):
    pytest.importorskip("lightgbm")
    return run_learning_experiment(ROOT, snapshot, plan)


@pytest.fixture
def binding(snapshot, plan):
    governed = [
        "src/nexus/portfolio/training_pipeline.py",
        "src/nexus/portfolio/training_data.py",
        "src/nexus/portfolio/training_models.py",
        "src/nexus/portfolio/training_fixture.py",
        "src/nexus/modeling/r3_features.py",
        "src/nexus/modeling/r3b_evaluation.py",
        "src/nexus/modeling/r3b_models.py",
        "src/nexus/modeling/r3_metrics.py",
        "src/nexus/modeling/r3_baseline.py",
        "configs/modeling/r3b-smoke.yaml",
        "uv.lock",
    ]
    return TrainingAdmission.from_dict(
        {
            "contract_version": "nexus.training-admission.v1",
            "feedback_receipt_id": _sha("feedback receipt"),
            "feedback_evaluation_sha256": _sha("feedback evaluation"),
            "feedback_evidence_sha256": _sha("feedback evidence"),
            "feedback_journal_head_sha256": _sha("feedback journal"),
            "feedback_source_snapshot_sha256": _sha("aggregate feedback, not R2"),
            "monitor_evaluation_sha256": _sha("monitor"),
            "training_snapshot": snapshot.to_dict(),
            "plan": plan.model_dump(mode="json"),
            "code_files": {
                name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in governed
            },
            "bridge_code_files": {
                "src/nexus/portfolio/learning.py": _sha("constructed bridge binding")
            },
            "dependencies": {
                name: _dependency_version(name) for name in ("lightgbm", "numpy", "pyarrow")
            },
            "revision_map_sha256": _sha("verified revision mapping"),
            "baseline_artifact_sha256": BASELINE.sha256,
            "locations": list(LOCATIONS),
        }
    )


def _propose(book, binding):
    data = binding.to_dict()
    return book.propose_training(
        PLANNER,
        source_snapshot_sha256=data["feedback_source_snapshot_sha256"],
        monitor_evaluation_sha256=data["monitor_evaluation_sha256"],
        reason="measured breach",
    )


def _admit(book, binding):
    return book.admit_training(WORKER, proposal_id=_propose(book, binding), binding=binding)


def _abstention(binding):
    data = binding.to_dict()
    snapshot = data["training_snapshot"]
    return LearningRun(
        {
            "contract_version": "nexus.runtime-learning-result.v1",
            "bindings": {
                "snapshot_sha256": snapshot["snapshot_sha256"],
                "source_snapshot_sha256": snapshot["source_snapshot_sha256"],
                "finalization_sha256": snapshot["finalization_sha256"],
                "plan_sha256": _sha(data["plan"]),
                "code_sha256": _sha(data["code_files"]),
            },
            "plan": data["plan"],
            "snapshot": snapshot,
            "code_files": data["code_files"],
            "training_performed": False,
            "scientific_champion_changed": False,
            "classification": "synthetic",
            "status": "ABSTAIN_INSUFFICIENT_EVIDENCE",
            "reason": "constructed unit-test abstention, not a real fit",
        },
        None,
        None,
    )


def test_binding_is_frozen_and_does_not_alias_input(binding):
    data = binding.to_dict()
    data["locations"].append("NEW")
    assert binding.to_dict()["locations"] == list(LOCATIONS)
    with pytest.raises(FrozenInstanceError):
        binding.sha256 = "0" * 64
    with pytest.raises(ValueError, match="checksum"):
        TrainingAdmission(binding.payload + b" ", binding.sha256)
    with pytest.raises(ValueError, match="canonical"):
        content = json.dumps(binding.to_dict(), indent=2).encode()
        TrainingAdmission(content, hashlib.sha256(content).hexdigest())


@pytest.mark.parametrize(
    "change",
    [
        {"extra": True},
        {"contract_version": "unsafe"},
        {"feedback_receipt_id": "bad"},
        {"locations": []},
        {"locations": ["LOC-2", "LOC-1"]},
        {"locations": ["LOC-1", "LOC-1"]},
        {"locations": [" LOC-1"]},
        {"code_files": {}},
        {"code_files": {"../escape": "a" * 64}},
        {"code_files": {"C:/escape": "a" * 64}},
        {"bridge_code_files": {"bridge.py": "bad"}},
        {"dependencies": {}},
    ],
)
def test_binding_rejects_noncontract_fields(binding, change):
    with pytest.raises(ValueError):
        TrainingAdmission.from_dict({**binding.to_dict(), **change})


@pytest.mark.parametrize(
    "field, value",
    [
        ("minimum_history_days", 1),
        ("minimum_mature_labels", 1),
        ("gold_demand_sha256", "a" * 64),
        ("finalization_sha256", None),
        ("extra", "not allowed"),
    ],
)
def test_snapshot_binding_rejects_tamper_and_weakened_thresholds(binding, field, value):
    data = binding.to_dict()
    data["training_snapshot"][field] = value
    # Even a correctly rehashed weak policy remains forbidden.
    if field in {"minimum_history_days", "minimum_mature_labels"}:
        raw = data["training_snapshot"]
        raw["snapshot_sha256"] = _sha(
            {key: val for key, val in raw.items() if key != "snapshot_sha256"}
        )
    with pytest.raises(ValueError):
        TrainingAdmission.from_dict(data)


def test_admission_and_restart_are_exactly_idempotent_but_never_rebound(tmp_path, binding):
    book = _book(tmp_path)
    before = book.snapshot()["audit"]
    proposal = _propose(book, binding)
    admission_id = book.admit_training(WORKER, proposal_id=proposal, binding=binding)
    expected = book.snapshot()
    restarted = LearningBook(book.path, clock=lambda: NOW)
    assert restarted.admit_training(WORKER, proposal_id=proposal, binding=binding) == admission_id
    assert restarted.snapshot() == expected
    assert expected["audit"][: len(before)] == before
    item = expected["training_admissions"][admission_id]
    assert item["admitted_at"] == NOW.isoformat()
    assert item["binding"] == binding.to_dict()
    assert item["started"] is item["completion"] is None
    changed = TrainingAdmission.from_dict({**binding.to_dict(), "revision_map_sha256": "a" * 64})
    with pytest.raises(ValueError, match="different immutable"):
        restarted.admit_training(WORKER, proposal_id=proposal, binding=changed)
    assert restarted.snapshot() == expected


@pytest.mark.parametrize(
    "actor",
    [
        PLANNER,
        APPROVER,
        replace(WORKER, locations=("LOC-1",)),
        replace(WORKER, role=ActorRole.AUDITOR),
    ],
)
def test_all_admission_operations_require_system_and_global_scope(tmp_path, binding, actor):
    book = _book(tmp_path)
    proposal = _propose(book, binding)
    before = book.snapshot()
    with pytest.raises(PermissionError):
        book.admit_training(actor, proposal_id=proposal, binding=binding)
    assert book.snapshot() == before
    admission_id = book.admit_training(WORKER, proposal_id=proposal, binding=binding)
    before = book.snapshot()
    with pytest.raises(PermissionError):
        book.start_training(actor, admission_id=admission_id)
    with pytest.raises(PermissionError):
        book.complete_training(actor, admission_id=admission_id, run=_abstention(binding))
    assert book.snapshot() == before


@pytest.mark.parametrize(
    "field",
    ["feedback_source_snapshot_sha256", "monitor_evaluation_sha256", "baseline_artifact_sha256"],
)
def test_admission_requires_actual_proposal_and_current_baseline(tmp_path, binding, field):
    book = _book(tmp_path)
    proposal = _propose(book, binding)
    before = book.snapshot()
    changed = TrainingAdmission.from_dict({**binding.to_dict(), field: "a" * 64})
    with pytest.raises(ValueError, match=r"binding mismatch|baseline"):
        book.admit_training(WORKER, proposal_id=proposal, binding=changed)
    assert book.snapshot() == before


def test_future_snapshot_and_backwards_start_clock_cannot_enter_journal(tmp_path, binding):
    instant = [
        datetime.fromisoformat(binding.to_dict()["plan"]["evaluation_cutoff"])
        - timedelta(seconds=1)
    ]
    book = _book(tmp_path, clock=lambda: instant[0])
    proposal = _propose(book, binding)
    with pytest.raises(ValueError, match="future"):
        book.admit_training(WORKER, proposal_id=proposal, binding=binding)
    instant[0] = NOW
    admission_id = book.admit_training(WORKER, proposal_id=proposal, binding=binding)
    instant[0] -= timedelta(seconds=1)
    before = book.snapshot()
    with pytest.raises(ValueError, match="backwards"):
        book.start_training(WORKER, admission_id=admission_id)
    assert book.snapshot() == before


def test_concurrent_start_has_one_winner_and_no_restart_refit(tmp_path, binding):
    book = _book(tmp_path)
    admission_id = _admit(book, binding)

    def claim(_):
        try:
            return book.start_training(WORKER, admission_id=admission_id)
        except ValueError as error:
            assert "already started" in str(error)
            return None

    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(claim, range(4)))
    assert sum(item is not None for item in outcomes) == 1
    restarted = LearningBook(book.path, clock=lambda: NOW)
    with pytest.raises(ValueError, match="explicit recovery"):
        restarted.start_training(WORKER, admission_id=admission_id)
    assert (
        len(
            [
                event
                for event in restarted.snapshot()["audit"]
                if event["kind"] == "training_started"
            ]
        )
        == 1
    )


def test_abstention_requires_start_and_completes_exactly_once(tmp_path, binding):
    book = _book(tmp_path)
    admission_id = _admit(book, binding)
    run = _abstention(binding)
    with pytest.raises(ValueError, match="prior durable"):
        book.complete_training(WORKER, admission_id=admission_id, run=run)
    book.start_training(WORKER, admission_id=admission_id)
    result = book.complete_training(WORKER, admission_id=admission_id, run=run)
    assert result["candidate_id"] is None and result["training_performed"] is False
    before = book.snapshot()
    restarted = LearningBook(book.path, clock=lambda: NOW + timedelta(seconds=1))
    assert restarted.complete_training(WORKER, admission_id=admission_id, run=run) == result
    assert restarted.snapshot() == before
    receipt = copy.deepcopy(run.receipt)
    receipt["reason"] = "different abstention"
    with pytest.raises(ValueError, match="different completion"):
        restarted.complete_training(
            WORKER, admission_id=admission_id, run=LearningRun(receipt, None, None)
        )


@pytest.mark.parametrize(
    "field",
    [
        "source_snapshot_sha256",
        "snapshot_sha256",
        "finalization_sha256",
        "plan_sha256",
        "code_sha256",
    ],
)
def test_completion_rejects_substituted_run_identity(tmp_path, binding, field):
    book = _book(tmp_path)
    admission_id = _admit(book, binding)
    book.start_training(WORKER, admission_id=admission_id)
    run = _abstention(binding)
    receipt = copy.deepcopy(run.receipt)
    receipt["bindings"][field] = "a" * 64
    before = book.snapshot()
    with pytest.raises(ValueError, match="admitted run bindings"):
        book.complete_training(
            WORKER, admission_id=admission_id, run=LearningRun(receipt, None, None)
        )
    assert book.snapshot() == before


def test_actual_candidate_is_bound_to_original_monitor_and_always_non_deployable(
    tmp_path, binding, experiment
):
    book = _book(tmp_path)
    admission_id = _admit(book, binding)
    started = book.start_training(WORKER, admission_id=admission_id)
    result = book.complete_training(WORKER, admission_id=admission_id, run=experiment)
    candidate = book.snapshot()["candidates"][result["candidate_id"]]
    assert candidate["admission_id"] == admission_id
    assert (
        candidate["evaluation"]["monitor_evaluation_sha256"]
        == binding.to_dict()["monitor_evaluation_sha256"]
    )
    assert (
        candidate["evaluation"]["source_snapshot_sha256"]
        == experiment.receipt["bindings"]["source_snapshot_sha256"]
    )
    assert result["logical_run_id"] == started["logical_run_id"]
    assert result["status"] == experiment.receipt["status"]
    with pytest.raises(PermissionError, match="cannot deploy"):
        book.deploy_sandbox(
            APPROVER, candidate_id=result["candidate_id"], expected_current_sha256=BASELINE.sha256
        )
    with pytest.raises(PermissionError, match="cannot_promote"):
        book.promote_scientific(APPROVER, candidate_id=result["candidate_id"])
    assert book.snapshot()["sandbox_deployment"]["artifact_sha256"] == BASELINE.sha256


def test_legacy_candidate_registration_still_rejects_distinct_feedback_source(
    tmp_path, binding, experiment
):
    book = _book(tmp_path)
    proposal = _propose(book, binding)
    artifact = ModelArtifact.from_bytes(
        "runtime_daily_lightgbm_candidate", experiment.candidate.to_bytes()
    )
    evaluation = sandbox_evaluation(
        experiment, monitor_evaluation_sha256=binding.to_dict()["monitor_evaluation_sha256"]
    )
    with pytest.raises(ValueError, match="artifact/source/monitor binding mismatch"):
        book.register_candidate(
            WORKER, proposal_id=proposal, artifact=artifact, evaluation=evaluation
        )


@pytest.mark.parametrize("target", ["event", "candidate", "comparator"])
def test_persisted_admission_or_artifact_tampering_fails_restart(
    tmp_path, binding, experiment, target
):
    book = _book(tmp_path)
    admission_id = _admit(book, binding)
    book.start_training(WORKER, admission_id=admission_id)
    book.complete_training(WORKER, admission_id=admission_id, run=experiment)
    with closing(sqlite3.connect(book.path)) as db:
        if target == "event":
            row = db.execute("SELECT payload FROM learning_events WHERE sequence=3").fetchone()
            changed = json.loads(row[0])
            changed["data"]["binding"]["revision_map_sha256"] = "a" * 64
            db.execute(
                "UPDATE learning_events SET payload=? WHERE sequence=3", (json.dumps(changed),)
            )
        else:
            digest = (
                experiment.candidate.sha256
                if target == "candidate"
                else experiment.baseline_artifact.sha256
            )
            db.execute(
                "UPDATE learning_artifacts SET payload=? WHERE sha256=?", (b"tampered", digest)
            )
        db.commit()
    with pytest.raises(ValueError, match=r"hash chain|artifact checksum"):
        LearningBook(book.path, clock=lambda: NOW)


def test_admission_rejects_mutated_typed_receipt_and_implicit_boolean_coercion(tmp_path, binding):
    raw = binding.to_dict()
    raw["plan"]["automatic_promotion"] = 0
    with pytest.raises(ValueError, match="snapshot/plan"):
        TrainingAdmission.from_dict(raw)
    book = _book(tmp_path)
    proposal = _propose(book, binding)
    before = book.snapshot()
    object.__setattr__(binding, "payload", binding.payload + b" ")
    with pytest.raises(ValueError, match="checksum"):
        book.admit_training(WORKER, proposal_id=proposal, binding=binding)
    assert book.snapshot() == before


@pytest.mark.parametrize("operation", ["admit", "start", "complete"])
def test_unknown_training_identity_does_not_append(tmp_path, binding, operation):
    book = _book(tmp_path)
    before = book.snapshot()
    with pytest.raises(ValueError, match=r"unknown|prior durable"):
        if operation == "admit":
            book.admit_training(WORKER, proposal_id="f" * 64, binding=binding)
        elif operation == "start":
            book.start_training(WORKER, admission_id="f" * 64)
        else:
            book.complete_training(WORKER, admission_id="f" * 64, run=_abstention(binding))
    assert book.snapshot() == before


def test_failed_run_check_leaves_started_without_implicit_retry(tmp_path, binding):
    instant = [NOW]
    book = _book(tmp_path, clock=lambda: instant[0])
    admission_id = _admit(book, binding)
    book.start_training(WORKER, admission_id=admission_id)
    before = book.snapshot()
    with pytest.raises(ValueError, match="LearningRun object"):
        book.complete_training(WORKER, admission_id=admission_id, run={})
    run = _abstention(binding)
    run.receipt["reason"] = "mutated after hashing"
    with pytest.raises(ValueError, match="mutated"):
        book.complete_training(WORKER, admission_id=admission_id, run=run)
    instant[0] -= timedelta(seconds=1)
    with pytest.raises(ValueError, match="backwards"):
        book.complete_training(WORKER, admission_id=admission_id, run=_abstention(binding))
    assert book.snapshot() == before
    assert before["training_admissions"][admission_id]["completion"] is None


@pytest.mark.parametrize("where", ["before_admission", "after_admission"])
def test_legacy_registered_candidates_cannot_gain_retroactive_training_provenance(
    tmp_path, binding, experiment, where
):
    raw = binding.to_dict()
    raw["feedback_source_snapshot_sha256"] = raw["training_snapshot"]["source_snapshot_sha256"]
    matching = TrainingAdmission.from_dict(raw)
    book = _book(tmp_path)
    proposal = _propose(book, matching)
    admission_id = None
    if where == "after_admission":
        admission_id = book.admit_training(WORKER, proposal_id=proposal, binding=matching)
    artifact = ModelArtifact.from_bytes(
        "runtime_daily_lightgbm_candidate", experiment.candidate.to_bytes()
    )
    candidate_id = book.register_candidate(
        WORKER,
        proposal_id=proposal,
        artifact=artifact,
        evaluation=sandbox_evaluation(
            experiment, monitor_evaluation_sha256=raw["monitor_evaluation_sha256"]
        ),
    )
    before = book.snapshot()
    if where == "before_admission":
        with pytest.raises(ValueError, match="retroactive admission"):
            book.admit_training(WORKER, proposal_id=proposal, binding=matching)
    else:
        with pytest.raises(ValueError, match="retrospective training start"):
            book.start_training(WORKER, admission_id=admission_id)
        with pytest.raises(PermissionError, match="cannot deploy"):
            book.deploy_sandbox(
                APPROVER, candidate_id=candidate_id, expected_current_sha256=BASELINE.sha256
            )
    assert book.snapshot() == before


@pytest.mark.parametrize("target", ["plan", "snapshot", "code", "dependencies"])
def test_trained_completion_requires_identical_admitted_provenance(
    tmp_path, binding, experiment, target
):
    book = _book(tmp_path)
    admission_id = _admit(book, binding)
    book.start_training(WORKER, admission_id=admission_id)
    receipt = copy.deepcopy(experiment.receipt)
    if target == "plan":
        receipt["plan"]["automatic_promotion"] = 0
    elif target == "snapshot":
        receipt["snapshot"]["evaluation_sufficiency_claimed"] = 0
    elif target == "code":
        receipt["code_files"]["uv.lock"] = "a" * 64
    else:
        receipt["dependencies"]["lightgbm"] = "changed"
    changed = LearningRun(
        receipt,
        experiment.candidate,
        experiment.baseline_artifact,
        experiment.evaluation_rows,
        experiment.candidate_forecasts,
    )
    before = book.snapshot()
    with pytest.raises(ValueError, match="admitted run bindings"):
        book.complete_training(WORKER, admission_id=admission_id, run=changed)
    assert book.snapshot() == before


@pytest.mark.parametrize(
    "target", ["admission_actor", "admission_source", "start_order", "completion_order"]
)
def test_semantically_invalid_rehashed_events_fail_replay(tmp_path, binding, target):
    book = _book(tmp_path)
    admission_id = _admit(book, binding)
    book.start_training(WORKER, admission_id=admission_id)
    book.complete_training(WORKER, admission_id=admission_id, run=_abstention(binding))
    with closing(sqlite3.connect(book.path)) as db:
        rows = list(db.execute("SELECT sequence, payload FROM learning_events ORDER BY sequence"))
        previous = "0" * 64
        for sequence, payload in rows:
            event = json.loads(payload)
            if target == "admission_actor" and sequence == 3:
                event["actor_role"] = ActorRole.PLANNER.value
            elif target == "admission_source" and sequence == 3:
                event["data"]["binding"]["feedback_source_snapshot_sha256"] = "a" * 64
            elif target == "start_order" and sequence == 4:
                event["at"] = (NOW - timedelta(seconds=1)).isoformat()
                event["data"]["started_at"] = event["at"]
            elif target == "completion_order" and sequence == 5:
                event["at"] = (NOW - timedelta(seconds=1)).isoformat()
                event["data"]["completed_at"] = event["at"]
            event["previous_sha256"] = previous
            previous = _sha(event)
            db.execute(
                "UPDATE learning_events SET payload=?, event_sha256=? WHERE sequence=?",
                (json.dumps(event), previous, sequence),
            )
        db.commit()
    with pytest.raises(ValueError, match=r"system actor|binding mismatch|must follow"):
        LearningBook(book.path, clock=lambda: NOW)


def _change_legacy_baseline(book):
    """Constructed trusted evaluation exercises the real legacy switch, not model quality."""
    artifact = ModelArtifact.from_bytes("other-model", b"different constructed baseline")
    source, monitor = _sha("separate legacy source"), _sha("separate legacy monitor")
    proposal = book.propose_training(
        PLANNER,
        source_snapshot_sha256=source,
        monitor_evaluation_sha256=monitor,
        reason="separate review",
    )
    receipt = {
        "contract_version": "nexus.sandbox-evaluation.v1",
        "source_snapshot_sha256": source,
        "monitor_evaluation_sha256": monitor,
        "artifact_sha256": artifact.sha256,
        "baseline_artifact_sha256": BASELINE.sha256,
        "config_sha256": _sha("constructed config"),
        "evaluation_cutoff": NOW.isoformat(),
        "evidence_sufficient": True,
        "scientific_decision": {
            "status": "ELIGIBLE_BUT_BLOCKED_SYNTHETIC",
            "candidate_won": True,
            "engineering_complete": True,
            "synthetic_evidence": True,
            "promotion_applied": False,
            "human_approval_required": True,
        },
    }
    content = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    candidate_id = book.register_candidate(
        WORKER,
        proposal_id=proposal,
        artifact=artifact,
        evaluation=CandidateEvaluation(content, hashlib.sha256(content).hexdigest()),
    )
    outputs = tuple(
        ShadowOutput(f"case-{index}", _sha(index), (Decimal(1),) * 5) for index in range(10)
    )
    book.record_shadow(
        WORKER,
        candidate_id=candidate_id,
        baseline_artifact_sha256=BASELINE.sha256,
        candidate_artifact_sha256=artifact.sha256,
        baseline=outputs,
        candidate=outputs,
    )
    book.deploy_sandbox(
        APPROVER, candidate_id=candidate_id, expected_current_sha256=BASELINE.sha256
    )


@pytest.mark.parametrize("completed_before_change", [False, True])
def test_completion_transaction_checks_current_baseline_but_exact_retry_is_stable(
    tmp_path, binding, completed_before_change
):
    book = _book(tmp_path)
    admission_id = _admit(book, binding)
    book.start_training(WORKER, admission_id=admission_id)
    run = _abstention(binding)
    original = None
    if completed_before_change:
        original = book.complete_training(WORKER, admission_id=admission_id, run=run)
    _change_legacy_baseline(book)
    before = book.snapshot()
    if completed_before_change:
        assert book.complete_training(WORKER, admission_id=admission_id, run=run) == original
    else:
        with pytest.raises(ValueError, match="baseline changed before training completion"):
            book.complete_training(WORKER, admission_id=admission_id, run=run)
        assert before["training_admissions"][admission_id]["completion"] is None
    assert book.snapshot() == before
