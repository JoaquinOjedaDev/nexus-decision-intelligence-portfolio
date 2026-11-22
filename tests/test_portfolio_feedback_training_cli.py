"""CLI boundaries only: source rebuild and training are verified in separate tests."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from typer.testing import CliRunner

from nexus.contracts import ActorRole
from nexus.portfolio import cli, feedback_training
from nexus.portfolio.feedback import FeedbackBook
from nexus.portfolio.learning import LearningBook, ModelArtifact


def arguments(root: Path) -> list[str]:
    return [
        "learn-feedback",
        str(root / "source"),
        "--feedback-receipt-id",
        "a" * 64,
        "--feedback-journal",
        str(root / "feedback.sqlite3"),
        "--learning-journal",
        str(root / "learning.sqlite3"),
        "--finalization-file",
        str(root / "closure.json"),
        "--output-dir",
        str(root / "output"),
    ]


def test_missing_journals_do_not_bootstrap_or_create_output(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli.app, arguments(tmp_path))
    assert result.exit_code != 0
    assert "must already exist" in result.output
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("content", ["unrelated_sqlite", "not_sqlite"])
def test_wrong_journal_is_rejected_without_initialization(tmp_path: Path, content: str) -> None:
    path = tmp_path / "feedback.sqlite3"
    if content == "unrelated_sqlite":
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("CREATE TABLE unrelated (value TEXT)")
    else:
        path.write_text("not a SQLite journal", encoding="utf-8")
    before = path.read_bytes()
    result = CliRunner().invoke(cli.app, arguments(tmp_path))
    assert result.exit_code != 0
    assert "wrong schema" in result.output or "not a readable SQLite" in result.output
    assert path.read_bytes() == before
    assert not (tmp_path / "learning.sqlite3").exists()
    assert not (tmp_path / "output").exists()


def test_cli_uses_existing_learning_scope_without_creating_proposal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    feedback = FeedbackBook(tmp_path / "feedback.sqlite3")
    learning = LearningBook(
        tmp_path / "learning.sqlite3",
        initial_artifact=ModelArtifact.from_bytes("test-only", b"test-only opaque baseline"),
        locations=("L1", "L2"),
    )
    prior = learning.snapshot(), feedback.snapshot()
    calls = []

    def bridge(root, **kwargs):
        calls.append((root, kwargs))
        assert kwargs["actor"].role is ActorRole.SYSTEM
        assert kwargs["actor"].locations == ("L1", "L2")
        assert kwargs["feedback_receipt_id"] == "a" * 64
        assert kwargs["source_dir"] == tmp_path / "source"
        assert kwargs["finalization_path"] == tmp_path / "closure.json"
        assert kwargs["output_dir"] == tmp_path / "output"
        return {"status": "TEST_ONLY_NO_TRAINING", "deployment_applied": False}

    monkeypatch.setattr(feedback_training, "run_feedback_training", bridge)
    result = CliRunner().invoke(cli.app, arguments(tmp_path))
    assert result.exit_code == 0, result.output
    assert "TEST_ONLY_NO_TRAINING" in result.output
    assert len(calls) == 1
    assert (learning.snapshot(), feedback.snapshot()) == prior
    assert not (tmp_path / "output").exists()
