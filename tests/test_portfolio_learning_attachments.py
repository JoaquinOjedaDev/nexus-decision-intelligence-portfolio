"""Portable, runtime-bound attachment history; no PostgreSQL server is needed."""

from __future__ import annotations

import shutil
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from nexus.portfolio.learning_attachments import _DDL, LearningAttachments
from nexus.portfolio.store import StoreError, digest


def summary(source: str = "a") -> dict[str, Any]:
    return {
        "contract_version": "nexus.learning-view.v1",
        "locations": ["NORTH", "SOUTH"],
        "bindings": {"source_snapshot_sha256": source * 64},
        "deployed": False,
        "human_approval": False,
        "evaluation_status": "REJECTED_GATE_FAILURE",
    }


def attach(book: LearningAttachments, source: str = "a", now: float = 123.0) -> dict[str, Any]:
    return book.attach(
        summary(source),
        seed_sha256=source * 64,
        registrar_id="trusted.registrar",
        registered_at=now,
    )


def test_absent_read_is_lazy_and_retry_retains_exact_original_record(tmp_path: Path) -> None:
    root = tmp_path / "not-created"
    book = LearningAttachments(root, "b" * 64)
    assert book.read() is None and not root.exists()
    first = attach(book)
    assert attach(book, now=150) == first
    assert LearningAttachments(root, "b" * 64).read() == first
    assert first["summary_sha256"] == digest(summary())
    assert first["registered_at"] == 123


def test_concurrent_idempotent_attachment_commits_one_event(tmp_path: Path) -> None:
    with ThreadPoolExecutor(max_workers=8) as workers:
        rows = list(
            workers.map(lambda _: attach(LearningAttachments(tmp_path, "b" * 64)), range(16))
        )
    assert len({digest(row) for row in rows}) == 1
    with closing(sqlite3.connect(tmp_path / "learning-attachments.sqlite3")) as db:
        assert db.execute("SELECT count(*) FROM attachment_events").fetchone() == (1,)


def test_history_is_retained_and_rebinding_does_not_overwrite_old_summary(tmp_path: Path) -> None:
    book = LearningAttachments(tmp_path, "b" * 64)
    first = attach(book)
    second = attach(book, "c", 124)
    returned = attach(book, "a", 125)
    assert [first["sequence"], second["sequence"], returned["sequence"]] == [1, 2, 3]
    assert second["previous_sha256"] == digest(first)
    assert returned["previous_sha256"] == digest(second)
    assert returned["report"] == first["report"]
    assert book.read() == returned


def test_copied_attachment_cannot_be_adopted_by_another_runtime(tmp_path: Path) -> None:
    source = LearningAttachments(tmp_path / "one", "a" * 64)
    attach(source)
    target = LearningAttachments(tmp_path / "two", "b" * 64)
    target.root.mkdir()
    shutil.copyfile(source.path, target.path)
    for operation in (target.read, lambda: attach(target)):
        with pytest.raises(StoreError) as refused:
            operation()
        assert refused.value.code == "integrity_error"
    assert source.read() is not None


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE attachment_events SET payload='{}'",
        "DELETE FROM attachment_events",
        "UPDATE attachment_identity SET runtime_identity='wrong'",
        "DELETE FROM attachment_identity",
    ],
)
def test_schema_enforces_append_only_history(tmp_path: Path, statement: str) -> None:
    book = LearningAttachments(tmp_path, "b" * 64)
    original = attach(book)
    with (
        closing(sqlite3.connect(book.path)) as db,
        pytest.raises(sqlite3.IntegrityError, match="immutable"),
    ):
        db.execute(statement)
    assert book.read() == original


@pytest.mark.parametrize("damage", ["body", "event_hash", "trigger", "extra_table"])
def test_tampered_history_or_schema_fails_closed(tmp_path: Path, damage: str) -> None:
    book = LearningAttachments(tmp_path, "b" * 64)
    attach(book)
    with closing(sqlite3.connect(book.path)) as db, db:
        if damage == "extra_table":
            db.execute("CREATE TABLE unrelated (id INTEGER)")
        else:
            db.execute("DROP TRIGGER attachment_events_no_update")
            if damage == "body":
                db.execute("UPDATE attachment_events SET payload='{}'")
            elif damage == "event_hash":
                db.execute("UPDATE attachment_events SET event_sha256=?", ("f" * 64,))
            if damage == "trigger":
                db.execute(
                    "CREATE TRIGGER attachment_events_no_update BEFORE UPDATE "
                    "ON attachment_events BEGIN SELECT 1; END"
                )
            else:
                db.execute(_DDL["attachment_events_no_update"])
    for operation in (book.read, lambda: attach(book)):
        with pytest.raises(StoreError) as refused:
            operation()
        assert refused.value.code == "integrity_error"


def test_foreign_database_is_not_initialized_or_overwritten(tmp_path: Path) -> None:
    book = LearningAttachments(tmp_path, "a" * 64)
    with closing(sqlite3.connect(book.path)) as db, db:
        db.execute("CREATE TABLE foreign_data (id INTEGER)")
        db.execute("INSERT INTO foreign_data VALUES (42)")
    with pytest.raises(StoreError):
        attach(book)
    with closing(sqlite3.connect(book.path)) as db:
        assert db.execute("SELECT * FROM foreign_data").fetchall() == [(42,)]
        assert db.execute("SELECT name FROM sqlite_schema WHERE type='table'").fetchall() == [
            ("foreign_data",)
        ]


@pytest.mark.parametrize("location", ["root", "file", "journal"])
def test_linked_sidecar_paths_are_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, location: str
) -> None:
    root = tmp_path / "runtime"
    target = (
        root
        if location == "root"
        else root / ("learning-attachments.sqlite3" + ("-journal" if location == "journal" else ""))
    )
    original = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda self: self == target or original(self))
    with pytest.raises(StoreError) as refused:
        LearningAttachments(root, "b" * 64)
    assert refused.value.code == "invalid_path" and not root.exists()


@pytest.mark.parametrize("now", [float("nan"), float("inf"), True])
def test_invalid_clock_is_refused_before_creation(tmp_path: Path, now: float) -> None:
    book = LearningAttachments(tmp_path / "runtime", "a" * 64)
    with pytest.raises(StoreError):
        attach(book, now=now)
    assert not book.root.exists()


def test_clock_cannot_reorder_existing_history(tmp_path: Path) -> None:
    book = LearningAttachments(tmp_path, "a" * 64)
    initial = attach(book)
    with pytest.raises(StoreError) as refused:
        attach(book, "c", 100)
    assert refused.value.code == "invalid_clock" and book.read() == initial


def test_failure_before_commit_retains_no_partial_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    book = LearningAttachments(tmp_path, "a" * 64)
    first = attach(book)
    original = book._read
    calls = 0

    def fail_after_insert(db: sqlite3.Connection) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("controlled publication failure")
        return original(db)

    monkeypatch.setattr(book, "_read", fail_after_insert)
    with pytest.raises(RuntimeError, match="controlled"):
        attach(book, "c", 124)
    monkeypatch.setattr(book, "_read", original)
    assert book.read() == first
