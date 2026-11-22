"""Read-only export of measured triggers; fixtures are not source/Gold evidence."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from test_portfolio_feedback import CLOSED, PLANNER, WORKER, _actual, _setup

from nexus.portfolio.feedback import FeedbackBook
from nexus.portfolio.learning import LearningBook, ModelArtifact
from nexus.portfolio.monitoring import MonitorBook
from nexus.portfolio.store import canonical, digest


@pytest.fixture(scope="module")
def trigger(tmp_path_factory: pytest.TempPathFactory) -> tuple[FeedbackBook, dict[str, Any]]:
    root = tmp_path_factory.mktemp("feedback-export")
    clock, _, book, _ = _setup(root, count=500)
    clock[0] = CLOSED
    for index in range(500):
        book.append_actual(_actual(index), actor=WORKER)
    learning = LearningBook(
        root / "learning.sqlite3",
        initial_artifact=ModelArtifact.from_bytes("test-only-baseline", b"test-only bytes"),
        locations=WORKER.locations,
        clock=lambda: clock[0],
    )
    receipt = book.evaluate(
        MonitorBook(root / "monitor.sqlite3"), learning_book=learning, proposer=PLANNER
    )
    return book, receipt


def _copy(book: FeedbackBook, target: Path) -> FeedbackBook:
    with closing(sqlite3.connect(book.path)) as source, closing(sqlite3.connect(target)) as copy:
        source.backup(copy)
    return FeedbackBook(target, clock=lambda: CLOSED + timedelta(hours=1))


def test_export_recomputes_trigger_and_does_not_write_or_invent_training_labels(trigger) -> None:
    book, receipt = trigger
    before = book.snapshot()
    exported = book.export_training_evidence(receipt["receipt_id"], actor=WORKER)
    assert exported["receipt"] == receipt
    assert len(exported["actuals"]) == len(exported["forecast_outcome_keys"]) == 500
    assert exported["training_snapshot_verified"] is False
    assert (
        digest({key: value for key, value in exported.items() if key != "export_sha256"})
        == (exported["export_sha256"])
    )
    assert all(item["revision"]["quantity"] == "20" for item in exported["actuals"])
    assert book.snapshot() == before
    exported["actuals"][0]["revision"]["quantity"] = "0"
    assert (
        book.export_training_evidence(receipt["receipt_id"], actor=WORKER)["actuals"][0][
            "revision"
        ]["quantity"]
        == "20"
    )


@pytest.mark.parametrize("actor", [PLANNER, replace(WORKER, locations=("another-location",))])
def test_export_denies_role_or_incomplete_scope_without_writes(trigger, actor) -> None:
    book, receipt = trigger
    before = book.snapshot()
    with pytest.raises(PermissionError):
        book.export_training_evidence(receipt["receipt_id"], actor=actor)
    assert book.snapshot() == before


def test_export_requires_exact_current_head_and_explicit_proposal(trigger, tmp_path: Path) -> None:
    original, receipt = trigger
    book = _copy(original, tmp_path / "feedback.sqlite3")
    with pytest.raises(ValueError, match="stale"):
        book.export_training_evidence(
            receipt["receipt_id"], actor=WORKER, expected_head_sha256="f" * 64
        )
    monitor_only = book.evaluate(MonitorBook(tmp_path / "monitor.sqlite3"))
    with pytest.raises(ValueError, match="explicit retained proposal"):
        book.export_training_evidence(monitor_only["receipt_id"], actor=WORKER)
    book.append_actual(
        _actual(
            0,
            event_id="reopened-0",
            revision=2,
            supersedes_event_id="observed-0",
            correction_closed=False,
            correction_closed_at=None,
        ),
        actor=WORKER,
    )
    with pytest.raises(ValueError, match="stale"):
        book.export_training_evidence(receipt["receipt_id"], actor=WORKER)


@pytest.mark.parametrize(
    "damage",
    [
        "selection",
        "measurement",
        "forecast_generations",
        "actual_versions",
        "forecast_rows",
        "eligible_forecasts",
    ],
)
def test_self_rebound_receipt_cannot_fabricate_selected_rows_or_metrics(
    trigger, tmp_path: Path, damage: str
) -> None:
    original, receipt = trigger
    book = _copy(original, tmp_path / "feedback.sqlite3")
    with closing(sqlite3.connect(book.path)) as db, db:
        value = json.loads(
            db.execute(
                "SELECT payload FROM feedback_receipts WHERE evaluation_sha256=?",
                (receipt["receipt_id"],),
            ).fetchone()[0]
        )
        if damage == "selection":
            value["selection"]["selected_actuals"].pop()
        elif damage != "measurement":
            value[damage] += 1
        else:
            quality = next(
                row
                for row in value["monitor"]["checks"].values()
                if row["kind"] == "forecast_quality"
            )
            quality["metrics"]["coverage_80"] = "0.8"
        db.execute(
            "UPDATE feedback_receipts SET payload=?,receipt_sha256=? WHERE evaluation_sha256=?",
            (canonical(value), digest(value), receipt["receipt_id"]),
        )
    with pytest.raises(ValueError, match=r"selection|measurements"):
        book.export_training_evidence(receipt["receipt_id"], actor=WORKER)


def test_export_rejects_unknown_receipt_or_future_evaluation(trigger, tmp_path: Path) -> None:
    original, receipt = trigger
    book = _copy(original, tmp_path / "feedback.sqlite3")
    with pytest.raises(ValueError, match="unknown feedback receipt"):
        book.export_training_evidence("f" * 64, actor=WORKER)
    book.clock = lambda: CLOSED - timedelta(seconds=1)
    with pytest.raises(ValueError, match="future"):
        book.export_training_evidence(receipt["receipt_id"], actor=WORKER)
