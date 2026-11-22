"""Actual CPU learner -> raw monitoring -> auditable held-out replay, without dbt."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest
from test_portfolio_training_data import _unit_gold

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.learning import LearningBook, ModelArtifact
from nexus.portfolio.training_data import build_training_snapshot
from nexus.portfolio.training_fixture import generate_training_fixture
from nexus.portfolio.training_governance import record_learning_governance
from nexus.portfolio.training_pipeline import (
    LearningRun,
    load_learning_plan,
    run_learning_experiment,
)

ROOT = Path(__file__).resolve().parents[1]
LOCATIONS = ("DC-ARG-01", "DC-ARG-02")
PLANNER = Actor("planner.test", "Planner", ActorRole.PLANNER, LOCATIONS)
WORKER = Actor("learner.test", "Training worker", ActorRole.SYSTEM, LOCATIONS)


@pytest.fixture(scope="module")
def trained(tmp_path_factory: pytest.TempPathFactory) -> LearningRun:
    directory = tmp_path_factory.mktemp("learning-governance")
    fixture = generate_training_fixture(directory / "source")
    snapshot = build_training_snapshot(
        _unit_gold(fixture.source_dir, directory / "explicit-unit-gold"),
        source_dir=fixture.source_dir,
        finalization_path=fixture.finalization_path,
        expected_finalization_sha256=fixture.finalization_sha256,
    )
    return run_learning_experiment(
        ROOT, snapshot, load_learning_plan(ROOT / "configs/portfolio/runtime-learning.yaml")
    )


def _record(run: LearningRun, directory: Path, **kwargs):
    return record_learning_governance(
        run,
        directory,
        **{
            "proposer": PLANNER,
            "registrar": WORKER,
            "expected_source_sha256": run.receipt["bindings"]["source_snapshot_sha256"],
            **kwargs,
        },
    )


def test_actual_learning_monitor_and_paired_replay_are_not_deployment(
    trained: LearningRun, tmp_path: Path
) -> None:
    result = _record(trained, tmp_path / "governance")
    monitor = result["monitor"]
    quality = next(row for row in monitor["checks"].values() if row["kind"] == "forecast_quality")
    freshness = next(row for row in monitor["checks"].values() if row["kind"] == "source_freshness")
    assert quality["model_id"] == "seasonal_naive_empirical_intervals"
    assert quality["distinct_mature_outcomes"] == quality["mature_forecast_rows"] == 1232
    assert quality["minimum_distinct_mature_outcomes"] == 500
    assert quality["status"] != "insufficient_evidence"
    assert freshness["status"] == "firing"
    assert freshness["watermark"].startswith("2026-06-01T03:00:00")
    assert "not live ingestion" in result["source_watermark_semantics"]
    assert monitor["evidence_sha256"] == result["source_snapshot_sha256"]
    assert trained.candidate is not None and trained.baseline_artifact is not None
    assert result["candidate_sha256"] == trained.candidate.sha256
    assert result["calibrated_comparison_baseline_sha256"] == trained.baseline_artifact.sha256
    assert result["raw_monitor_baseline_sha256"] != trained.baseline_artifact.sha256
    assert result["shadow_replay"]["case_count"] == 336
    assert result["shadow_replay"]["quality_superiority_inferred"] is False
    assert result["trained_by_upstream_pipeline"] is True
    assert result["registry_performs_training"] is False
    assert result["human_approval_recorded"] is result["deployment_applied"] is False
    state = result["state"]
    assert state["training_performed"] is False
    assert state["current_node"] == "initial"
    assert state["authoritative_scientific_champion"] == "seasonal_naive_empirical_intervals"
    assert state["scientific_promotion_applied"] is False
    stored = state["candidates"][result["candidate_id"]]
    assert stored["evaluation"]["runtime_learning_receipt_sha256"] == trained.receipt_sha256
    assert stored["evaluation"]["scientific_decision"] == json.loads(
        json.dumps(trained.receipt["scientific_decision"])
    )
    assert sorted(path.name for path in (tmp_path / "governance").glob("*.sqlite3")) == [
        "learning.sqlite3",
        "monitoring.sqlite3",
    ]


def test_actual_replay_reopens_without_duplicate_effect(trained: LearningRun, tmp_path: Path):
    first = _record(trained, tmp_path)
    second = _record(trained, tmp_path)
    assert first == second
    assert second["monitor"]["evaluation_count"] == 1
    assert len(second["state"]["candidates"]) == len(second["state"]["proposals"]) == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"proposer": WORKER},
        {"registrar": PLANNER},
        {"proposer": replace(PLANNER, locations=("DC-ARG-01",))},
        {"registrar": replace(WORKER, locations=("DC-ARG-02",))},
    ],
)
def test_wrong_role_or_partial_scope_cannot_write(
    trained: LearningRun, tmp_path: Path, kwargs: dict
) -> None:
    target = tmp_path / "denied"
    with pytest.raises(PermissionError, match="global planner"):
        _record(trained, target, **kwargs)
    assert not target.exists()


def test_changed_source_binding_cannot_write(trained: LearningRun, tmp_path: Path):
    with pytest.raises(ValueError, match="source binding"):
        _record(trained, tmp_path / "denied", expected_source_sha256="f" * 64)
    assert not (tmp_path / "denied").exists()


def test_mutated_receipt_or_wrong_baseline_artifact_cannot_write(
    trained: LearningRun, tmp_path: Path
) -> None:
    tampered = deepcopy(trained)
    tampered.receipt["evaluation"][0]["actual"] += 1
    with pytest.raises(ValueError, match="receipt mutated"):
        _record(tampered, tmp_path / "mutated")
    wrong_artifact = replace(
        trained, baseline_artifact=ModelArtifact.from_bytes("wrong", b"not the evaluated model")
    )
    with pytest.raises(ValueError, match="artifact differs"):
        _record(wrong_artifact, tmp_path / "artifact")
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("location", ["snapshot", "split_rows"])
def test_source_label_parity_is_checked_even_for_reencoded_receipt(
    trained: LearningRun, tmp_path: Path, location: str
) -> None:
    receipt = deepcopy(trained.receipt)
    if location == "snapshot":
        receipt["snapshot"]["labels"][0]["quantity"] += 1
    else:
        receipt["split_rows"]["training"][0]["label"]["quantity"] += 1
    reencoded = replace(trained, receipt=receipt)
    with pytest.raises(ValueError, match=r"snapshot receipt|split label"):
        _record(reencoded, tmp_path / "denied")
    assert not (tmp_path / "denied").exists()


def test_actual_failed_candidate_cannot_deploy_even_in_sandbox(
    trained: LearningRun, tmp_path: Path
) -> None:
    result = _record(trained, tmp_path)
    assert trained.receipt["scientific_decision"]["status"] == "REJECTED_GATE_FAILURE"
    before = result["state"]
    book = LearningBook(
        tmp_path / "learning.sqlite3",
        initial_artifact=trained.baseline_artifact,
        locations=LOCATIONS,
        clock=lambda: datetime.fromisoformat(trained.receipt["plan"]["evaluation_cutoff"]),
    )
    approver = Actor("approver.test", "Independent test approver", ActorRole.APPROVER, LOCATIONS)
    with pytest.raises(PermissionError, match="cannot deploy even in sandbox"):
        book.deploy_sandbox(
            approver,
            candidate_id=result["candidate_id"],
            expected_current_sha256=result["calibrated_comparison_baseline_sha256"],
        )
    assert book.snapshot() == before
