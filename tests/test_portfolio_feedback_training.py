"""Real journals and real trainer; R2/Gold is explicitly mocked in this unit suite.

The source package is generated and checksum checked. The typed in-memory label
snapshot is not evidence that dbt executed; the command acceptance test owns that.
"""

from __future__ import annotations

import csv
import json
import shutil
from collections import Counter
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from nexus.contracts import ActorRole
from nexus.modeling.r3_artifacts import canonical_json_sha256
from nexus.portfolio import feedback_training as bridge
from nexus.portfolio.auth import Actor
from nexus.portfolio.feedback import ActualRevision, FeedbackBook
from nexus.portfolio.learning import LearningBook, ModelArtifact
from nexus.portfolio.monitoring import MonitorBook
from nexus.portfolio.store import PortfolioStore, digest
from nexus.portfolio.training_data import (
    FinalizationManifest,
    TrainingLabel,
    TrainingSnapshot,
    validate_training_snapshot,
)
from nexus.portfolio.training_fixture import generate_training_fixture
from nexus.portfolio.training_pipeline import LearningPlan, load_learning_plan
from nexus.storage import file_sha256

ROOT = Path(__file__).resolve().parents[1]
START = datetime(2025, 5, 30, tzinfo=UTC)


@dataclass
class Inputs:
    root: Path
    source: Path
    finalization: Path
    snapshot: TrainingSnapshot
    plan: LearningPlan
    actor: Actor
    feedback: FeedbackBook
    learning: LearningBook
    receipt_id: str

    def kwargs(self, output: Path, *, fit: bool = False) -> dict[str, Any]:
        plan = self.plan if fit else self.plan.model_copy(update={"minimum_training_rows": 10000})
        return {
            "feedback_book": self.feedback,
            "feedback_receipt_id": self.receipt_id,
            "learning_book": self.learning,
            "actor": self.actor,
            "source_dir": self.source,
            "finalization_path": self.finalization,
            "plan": plan,
            "output_dir": output,
        }


@pytest.fixture(scope="module")
def original(tmp_path_factory: pytest.TempPathFactory) -> Inputs:
    root = tmp_path_factory.mktemp("feedback-training-unit")
    fixture = generate_training_fixture(root / "source")
    plan = load_learning_plan(ROOT / "configs/portfolio/runtime-learning.yaml")
    closure = FinalizationManifest.model_validate_json(fixture.finalization_path.read_bytes())
    closures = {row.key: row for row in closure.labels}
    labels = []
    with (fixture.source_dir / "fulfillment_outcomes.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        for row in csv.DictReader(handle):
            day = datetime.fromisoformat(row["demand_date"]).date()
            finality = closures[(row["sku_id"], row["location_id"], day)]
            assert int(row["demand_qty"]) == int(row["fulfilled_qty"])
            labels.append(
                TrainingLabel(
                    sku_id=row["sku_id"],
                    location_id=row["location_id"],
                    demand_date=day,
                    quantity=int(row["fulfilled_qty"]),
                    available_at=datetime.fromisoformat(row["available_at"]),
                    correction_closed_at=finality.correction_closed_at,
                    fulfillment_record_count=1,
                    source_rows_sha256=finality.source_rows_sha256,
                )
            )
    counts = Counter(f"{row.sku_id}:{row.location_id}" for row in labels)
    snapshot = TrainingSnapshot(
        status="READY",
        labels=tuple(sorted(labels, key=lambda row: row.key)),
        exclusions=(),
        reason_codes=(),
        source_snapshot_sha256=fixture.source_snapshot_sha256,
        source_manifest_file_sha256=file_sha256(fixture.source_dir / "source-manifest.json"),
        gold_demand_sha256=digest("in-memory unit demand, not dbt"),
        gold_fulfillment_sha256=digest("in-memory unit fulfillment, not dbt"),
        finalization_sha256=fixture.finalization_sha256,
        as_of=plan.evaluation_cutoff,
        minimum_history_days=180,
        minimum_mature_labels=500,
        counts_by_series=tuple(sorted(counts.items())),
        snapshot_sha256="0" * 64,
    )
    payload = snapshot.to_dict()
    payload.pop("snapshot_sha256")
    snapshot = replace(snapshot, snapshot_sha256=canonical_json_sha256(payload))
    validate_training_snapshot(snapshot)
    locations = tuple(sorted({row.location_id for row in labels}))
    actor = Actor("unit-trusted-worker", "Unit worker", ActorRole.SYSTEM, locations)
    planner = replace(actor, actor_id="unit-planner", role=ActorRole.PLANNER)
    clock = [START]
    store = PortfolioStore(root / "runtime", clock=lambda: clock[0].timestamp())
    # One constructed long-range forecast generation for testing the journal join;
    # these values are not presented as a production 14-day model forecast.
    chosen = snapshot.labels[:500]
    seed = {
        "metadata": {
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": "a" * 64,
        },
        "recommendations": [],
        "forecasts": [
            {
                "sku_id": row.sku_id,
                "location_id": row.location_id,
                "origin": START.isoformat(),
                "model": "constructed-unit-baseline",
                "forecast_sha256": digest(["test-only", row.sku_id, str(row.demand_date)]),
                "points": [
                    {
                        "date": row.demand_date.isoformat(),
                        "q025": 0,
                        "q10": 0,
                        "q50": 0,
                        "q90": 0,
                        "q975": 0,
                    }
                ],
            }
            for row in chosen
        ],
    }
    store.ensure_seed(seed)
    feedback = FeedbackBook(root / "feedback.sqlite3", clock=lambda: clock[0])
    feedback.register_current_runtime(store, actor=actor, expected_seed_sha256=digest(seed))
    clock[0] = plan.evaluation_cutoff
    for index, row in enumerate(chosen):
        feedback.append_actual(
            ActualRevision(
                event_id=f"unit-actual-{index}",
                source_id="generated-source-unit",
                source_snapshot_sha256=fixture.source_snapshot_sha256,
                sku_id=row.sku_id,
                location_id=row.location_id,
                target_date=row.demand_date,
                revision=1,
                quantity=Decimal(row.quantity),
                available_at=row.available_at,
                correction_closed=True,
                correction_closed_at=row.correction_closed_at,
            ),
            actor=actor,
        )
    learning = LearningBook(
        root / "learning.sqlite3",
        initial_artifact=ModelArtifact.from_bytes("unit-baseline", b"unit baseline"),
        locations=locations,
        clock=lambda: clock[0],
    )
    receipt = feedback.evaluate(
        MonitorBook(root / "monitor.sqlite3"),
        learning_book=learning,
        proposer=planner,
    )
    assert receipt["proposal_id"] is not None
    return Inputs(
        root,
        fixture.source_dir,
        fixture.finalization_path,
        snapshot,
        plan,
        actor,
        feedback,
        learning,
        receipt["receipt_id"],
    )


@pytest.fixture
def inputs(original: Inputs, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Inputs:
    source = tmp_path / "source"
    shutil.copytree(original.source, source)
    shutil.copyfile(original.feedback.path, tmp_path / "feedback.sqlite3")
    shutil.copyfile(original.learning.path, tmp_path / "learning.sqlite3")
    result = replace(
        original,
        root=tmp_path,
        source=source,
        finalization=source / original.finalization.name,
        feedback=FeedbackBook(
            tmp_path / "feedback.sqlite3", clock=lambda: original.plan.evaluation_cutoff
        ),
        learning=LearningBook(
            tmp_path / "learning.sqlite3", clock=lambda: original.plan.evaluation_cutoff
        ),
    )

    def rebuild(**kwargs: Any) -> Any:
        assert kwargs["source_dir"] == source
        assert kwargs["planning_cutoff"] == result.plan.evaluation_cutoff
        path = kwargs["artifact_dir"] / "explicit-unit-not-dbt.parquet"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"explicit in-memory R2 test double, not real Gold")
        return SimpleNamespace(
            gold_artifacts=(
                SimpleNamespace(
                    path=path,
                    dataset="gold.r2_demand_history",
                    content_sha256=result.snapshot.gold_demand_sha256,
                ),
                SimpleNamespace(
                    path=path,
                    dataset="gold.r2_fulfillment_outcomes",
                    content_sha256=result.snapshot.gold_fulfillment_sha256,
                ),
            )
        )

    def snapshot(*args: Any, **kwargs: Any) -> TrainingSnapshot:
        assert kwargs["minimum_history_days"] == 180
        assert kwargs["minimum_mature_labels"] == 500
        assert kwargs["as_of"] == result.plan.evaluation_cutoff
        assert kwargs["expected_finalization_sha256"] == file_sha256(result.finalization)
        return result.snapshot

    monkeypatch.setattr(bridge, "run_r2_rebuild", rebuild)
    monkeypatch.setattr(bridge, "build_training_snapshot", snapshot)
    return result


def test_real_trainer_after_durable_admission_and_verified_no_refit_retry(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("lightgbm")
    calls = []
    real = bridge.run_learning_experiment

    def train(*args: Any, **kwargs: Any):
        state = inputs.learning.snapshot()
        admission = next(iter(state["training_admissions"].values()))
        assert admission["started"] is not None and admission["completion"] is None
        calls.append(admission["logical_run_id"])
        return real(*args, **kwargs)

    monkeypatch.setattr(bridge, "run_learning_experiment", train)
    output = inputs.root / "real-trainer"
    result = bridge.run_feedback_training(ROOT, **inputs.kwargs(output, fit=True))
    assert result["training_performed"] is True
    assert result["completion"]["deployment_blocked"] is True
    assert result["completion"]["candidate_id"] is not None
    assert result["revision_map_counts"] == {"matched_verified_source_label": 500}
    retained = json.loads(Path(result["learning_receipt_path"]).read_bytes())
    assert retained["splits"]["training"]["rows"] >= 500
    assert retained["code_files"] == bridge._code_maps(ROOT)[0]
    assert retained["dependencies"] == bridge._dependencies()
    assert retained["candidate_artifact"] is not None
    assert result["scientific_promotion_applied"] is result["deployment_applied"] is False
    monkeypatch.setattr(bridge, "run_r2_rebuild", lambda **kw: pytest.fail("retry rebuilt Gold"))
    assert bridge.run_feedback_training(ROOT, **inputs.kwargs(output, fit=True)) == result
    assert len(calls) == 1
    assert inputs.learning.snapshot()["sandbox_deployment"]["model_id"] == "unit-baseline"


def test_abstention_is_completed_without_fabricated_artifact(inputs: Inputs) -> None:
    result = bridge.run_feedback_training(ROOT, **inputs.kwargs(inputs.root / "abstain"))
    assert result["status"] == "ABSTAIN_INSUFFICIENT_EVIDENCE"
    assert result["training_performed"] is False
    assert result["completion"]["candidate_id"] is None
    assert bridge.run_feedback_training(ROOT, **inputs.kwargs(inputs.root / "abstain")) == result


@pytest.mark.parametrize(
    "actor_change", [{"role": ActorRole.PLANNER}, {"locations": ()}, {"actor_id": " "}]
)
def test_role_and_scope_denied_before_any_output_or_gold(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch, actor_change: dict[str, Any]
) -> None:
    monkeypatch.setattr(
        bridge, "run_r2_rebuild", lambda **kw: pytest.fail("unauthorized Gold write")
    )
    args = inputs.kwargs(inputs.root / "denied")
    args["actor"] = replace(inputs.actor, **actor_change)
    with pytest.raises(PermissionError):
        bridge.run_feedback_training(ROOT, **args)
    assert not args["output_dir"].exists()
    assert not inputs.learning.snapshot()["training_admissions"]


def test_source_scope_checked_before_gold_or_output(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        bridge, "_source_scope", lambda source: {*inputs.actor.locations, "UNAUTHORIZED"}
    )
    with pytest.raises(PermissionError):
        bridge.run_feedback_training(ROOT, **inputs.kwargs(inputs.root / "scope-denied"))
    assert not (inputs.root / "scope-denied").exists()


def test_finalization_source_mismatch_denied_before_output(inputs: Inputs) -> None:
    payload = json.loads(inputs.finalization.read_bytes())
    payload["source_snapshot_sha256"] = "f" * 64
    inputs.finalization.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="finalization source"):
        bridge.run_feedback_training(ROOT, **inputs.kwargs(inputs.root / "closure-denied"))
    assert not (inputs.root / "closure-denied").exists()


@pytest.mark.parametrize(
    "change,match",
    [
        ({"quantity": "999999"}, "quantity"),
        ({"quantity": "0.5"}, "quantity"),
        ({"available_at": "2026-06-03T00:00:00Z"}, "availability"),
        ({"correction_closed_at": "2026-06-03T00:00:00Z"}, "finality"),
    ],
)
def test_overlap_value_or_temporal_mismatch_cannot_be_silently_excluded(
    inputs: Inputs, change: dict[str, Any], match: str
) -> None:
    evidence = inputs.feedback.export_training_evidence(inputs.receipt_id, actor=inputs.actor)
    evidence["actuals"][0]["revision"].update(change)
    with pytest.raises(ValueError, match=match):
        bridge._revision_map(evidence, inputs.snapshot, inputs.plan)


def test_actual_source_mismatch_is_explicit_and_all_unrelated_is_rejected(inputs: Inputs) -> None:
    evidence = inputs.feedback.export_training_evidence(inputs.receipt_id, actor=inputs.actor)
    evidence["actuals"][0]["revision"]["source_snapshot_sha256"] = "f" * 64
    mapping = bridge._revision_map(evidence, inputs.snapshot, inputs.plan)
    assert mapping["counts"] == {
        "excluded_actual_source_snapshot_mismatch": 1,
        "matched_verified_source_label": 499,
    }
    for row in evidence["actuals"]:
        row["revision"]["source_snapshot_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="no matching"):
        bridge._revision_map(evidence, inputs.snapshot, inputs.plan)


def test_absent_or_censored_snapshot_labels_are_reported_not_inserted(inputs: Inputs) -> None:
    evidence = inputs.feedback.export_training_evidence(inputs.receipt_id, actor=inputs.actor)
    removed = inputs.snapshot.labels[0]
    counts = dict(inputs.snapshot.counts_by_series)
    counts[f"{removed.sku_id}:{removed.location_id}"] -= 1
    snapshot = replace(
        inputs.snapshot,
        labels=inputs.snapshot.labels[1:],
        exclusions=(("unfulfilled_censored", 1),),
        counts_by_series=tuple(sorted(counts.items())),
    )
    payload = snapshot.to_dict()
    payload.pop("snapshot_sha256")
    snapshot = replace(snapshot, snapshot_sha256=canonical_json_sha256(payload))
    mapping = bridge._revision_map(evidence, snapshot, inputs.plan)
    assert mapping["counts"]["excluded_not_in_finalized_uncensored_snapshot"] == 1
    assert mapping["snapshot_exclusions"] == {"unfulfilled_censored": 1}
    assert mapping["entries"][0]["source_rows_sha256"] is None


def _append_revision(inputs: Inputs) -> None:
    evidence = inputs.feedback.export_training_evidence(inputs.receipt_id, actor=inputs.actor)
    actual = ActualRevision.model_validate_json(json.dumps(evidence["actuals"][0]["revision"]))
    inputs.feedback.append_actual(
        actual.model_copy(
            update={
                "event_id": "racing-correction",
                "revision": 2,
                "supersedes_event_id": actual.event_id,
            }
        ),
        actor=inputs.actor,
    )


def test_feedback_revision_after_fit_invalidates_completion_and_retains_failure(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = bridge.run_learning_experiment

    def race(*args: Any, **kwargs: Any):
        run = real(*args, **kwargs)
        _append_revision(inputs)
        return run

    monkeypatch.setattr(bridge, "run_learning_experiment", race)
    with pytest.raises(bridge.FeedbackTrainingError, match="stale") as error:
        bridge.run_feedback_training(ROOT, **inputs.kwargs(inputs.root / "raced"))
    assert error.value.failure_receipt_path is not None
    failure = json.loads(error.value.failure_receipt_path.read_bytes())
    assert failure["training_claimed"] is True
    assert failure["phase"] == "post_fit_provenance"
    assert Path(failure["retained_learning_receipt_path"]).is_file()
    admission = next(iter(inputs.learning.snapshot()["training_admissions"].values()))
    assert admission["started"] is not None and admission["completion"] is None


def test_source_change_after_fit_invalidates_completion(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = bridge.run_learning_experiment

    def race(*args: Any, **kwargs: Any):
        run = real(*args, **kwargs)
        path = inputs.source / "fulfillment_outcomes.csv"
        path.write_bytes(path.read_bytes() + b"\n")
        return run

    monkeypatch.setattr(bridge, "run_learning_experiment", race)
    with pytest.raises(bridge.FeedbackTrainingError, match="checksum"):
        bridge.run_feedback_training(ROOT, **inputs.kwargs(inputs.root / "source-raced"))
    assert (
        next(iter(inputs.learning.snapshot()["training_admissions"].values()))["completion"] is None
    )


def test_changed_code_before_admission_does_not_fit(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = bridge._code_maps
    calls = []

    def changing(root: Path):
        code, extra = real(root)
        if calls:
            extra["src/nexus/portfolio/feedback_training.py"] = "f" * 64
        calls.append(1)
        return code, extra

    monkeypatch.setattr(bridge, "_code_maps", changing)
    monkeypatch.setattr(
        bridge, "run_learning_experiment", lambda *a: pytest.fail("raced code fitted")
    )
    with pytest.raises(bridge.FeedbackTrainingError, match="code"):
        bridge.run_feedback_training(ROOT, **inputs.kwargs(inputs.root / "code-raced"))
    assert not inputs.learning.snapshot()["training_admissions"]


def test_crashed_fit_has_one_claim_and_no_implicit_retry(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def fail(*args: Any):
        calls.append(1)
        raise RuntimeError("unit fit interruption")

    monkeypatch.setattr(bridge, "run_learning_experiment", fail)
    output = inputs.root / "interrupted"
    with pytest.raises(bridge.FeedbackTrainingError, match="fit interruption"):
        bridge.run_feedback_training(ROOT, **inputs.kwargs(output))
    with pytest.raises(ValueError, match="explicit recovery"):
        bridge.run_feedback_training(ROOT, **inputs.kwargs(output))
    with pytest.raises(ValueError, match="already admitted"):
        bridge.run_feedback_training(ROOT, **inputs.kwargs(inputs.root / "second-fit"))
    assert len(calls) == 1
    assert not (inputs.root / "second-fit").exists()


@pytest.mark.parametrize(
    "artifact_key", ["learning_receipt_path", "revision_map_path", "feedback_evidence_path"]
)
def test_completed_retry_rejects_tampered_artifact_without_refit(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch, artifact_key: str
) -> None:
    output = inputs.root / "tampered"
    result = bridge.run_feedback_training(ROOT, **inputs.kwargs(output))
    path = Path(result[artifact_key])
    payload = json.loads(path.read_bytes())
    payload["tampered"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(
        bridge, "run_learning_experiment", lambda *a: pytest.fail("tamper caused refit")
    )
    with pytest.raises(ValueError, match="checksum"):
        bridge.run_feedback_training(ROOT, **inputs.kwargs(output))


def test_gold_failure_retains_failed_receipt_without_admission(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(**kwargs: Any):
        raise ValueError("unit dbt contract failed")

    monkeypatch.setattr(bridge, "run_r2_rebuild", fail)
    with pytest.raises(bridge.FeedbackTrainingError, match="dbt contract") as error:
        bridge.run_feedback_training(ROOT, **inputs.kwargs(inputs.root / "gold-failed"))
    assert error.value.failure_receipt_path is not None
    failure = json.loads(error.value.failure_receipt_path.read_bytes())
    assert failure["training_claimed"] is False and failure["admission_id"] is None
    assert not inputs.learning.snapshot()["training_admissions"]


@pytest.mark.parametrize(
    "change",
    [
        {"gold_files": []},
        {"status": "ELIGIBLE_FOR_REVIEW"},
        {"training_performed": True},
        {"feedback_receipt_id": "a" * 64},
        {"feedback_export_sha256": "b" * 64},
        {"revision_map_counts": {"matched_verified_source_label": 9999}},
        {"revision_map_sha256": "c" * 64},
        {"deployment_applied": True},
        {"unexpected_claim": True},
    ],
)
def test_completed_retry_rejects_self_rebound_result_claims(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch, change: dict[str, Any]
) -> None:
    output = inputs.root / "rebound"
    result = bridge.run_feedback_training(ROOT, **inputs.kwargs(output))
    old = Path(result["receipt_path"])
    payload = json.loads(old.read_bytes())
    payload.update(change)
    rebound = old.with_name(canonical_json_sha256(payload) + ".json")
    old.rename(rebound)
    rebound.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(
        bridge, "run_learning_experiment", lambda *a: pytest.fail("rebound caused refit")
    )
    with pytest.raises(ValueError, match="authoritative"):
        bridge.run_feedback_training(ROOT, **inputs.kwargs(output))


def test_completed_retry_rejects_modified_bound_gold_bytes(inputs: Inputs) -> None:
    output = inputs.root / "gold-tampered"
    result = bridge.run_feedback_training(ROOT, **inputs.kwargs(output))
    Path(result["gold_files"][0]["path"]).write_bytes(b"changed Gold")
    with pytest.raises(ValueError, match="Gold artifact"):
        bridge.run_feedback_training(ROOT, **inputs.kwargs(output))
