"""Runtime-bound append-only learning summaries outside the operational database.

This local sidecar never promotes a model. Callers authorize under the workflow
guard before constructing/accessing it, retaining workflow -> sidecar lock order.
Its hash chain detects corruption, not an adversarial administrator rewriting a
whole journal. Missing files are read as absent without creating a database.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any, NoReturn

from nexus.portfolio.store import StoreError, canonical, digest

_SHA = re.compile(r"[a-f0-9]{64}\Z")
_ZERO = "0" * 64
_TABLES = {"attachment_identity", "attachment_events"}
_DDL = {
    "attachment_identity": (
        "CREATE TABLE attachment_identity (id INTEGER PRIMARY KEY CHECK(id=1), "
        "runtime_identity TEXT NOT NULL)"
    ),
    "attachment_events": (
        "CREATE TABLE attachment_events (sequence INTEGER PRIMARY KEY, "
        "payload TEXT NOT NULL, event_sha256 TEXT NOT NULL UNIQUE)"
    ),
}
for _table in sorted(_TABLES):
    for _operation in ("UPDATE", "DELETE"):
        _name = f"{_table}_no_{_operation.lower()}"
        _DDL[_name] = (
            f"CREATE TRIGGER {_name} BEFORE {_operation} ON {_table} "
            "BEGIN SELECT RAISE(ABORT,'immutable attachment history'); END"
        )


def _refuse() -> NoReturn:
    raise StoreError("integrity_error", "Learning attachment evidence failed its integrity check")


class LearningAttachments:
    """A lazy, fixed-name journal bound to one verified runtime storage identity."""

    def __init__(self, root: Path, runtime_identity: str) -> None:
        if not isinstance(runtime_identity, str) or not _SHA.fullmatch(runtime_identity):
            raise StoreError("invalid_plan", "Learning attachments require a runtime identity")
        self.root, self.runtime_identity = root, runtime_identity
        self.path = root / "learning-attachments.sqlite3"
        self._safe_path()

    def _safe_path(self) -> None:
        if (
            not self.root.is_absolute()
            or self.root == Path(self.root.anchor)
            or self.root.anchor.startswith(("\\\\", "//"))
            or ".." in self.root.parts
        ):
            raise StoreError("invalid_path", "Learning attachments require a bounded local root")
        for ancestor in (*reversed(self.path.parents), self.path):
            if ancestor.is_symlink() or ancestor.is_junction():
                raise StoreError("invalid_path", "Linked learning attachment paths are forbidden")
        for suffix in ("-wal", "-shm", "-journal"):
            sibling = Path(str(self.path) + suffix)
            if sibling.is_symlink() or sibling.is_junction():
                raise StoreError(
                    "invalid_path", "Linked learning attachment journals are forbidden"
                )

    def _schema(self, db: sqlite3.Connection) -> None:
        objects = dict(
            db.execute("SELECT name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'")
        )
        if objects != _DDL:
            _refuse()
        if db.execute("SELECT * FROM attachment_identity").fetchall() != [
            (1, self.runtime_identity)
        ]:
            _refuse()

    def _read(self, db: sqlite3.Connection) -> list[dict[str, Any]]:
        self._schema(db)
        history: list[dict[str, Any]] = []
        previous = _ZERO
        try:
            for sequence, payload, recorded_hash in db.execute(
                "SELECT sequence,payload,event_sha256 FROM attachment_events ORDER BY sequence"
            ):
                event = json.loads(payload)
                summary = event["report"]
                if (
                    set(event)
                    != {
                        "sequence",
                        "runtime_identity",
                        "seed_sha256",
                        "summary_sha256",
                        "report",
                        "registered_at",
                        "registrar_id",
                        "previous_sha256",
                    }
                    or sequence != len(history) + 1
                    or event["sequence"] != sequence
                    or canonical(event) != payload
                    or digest(event) != recorded_hash
                    or event["runtime_identity"] != self.runtime_identity
                    or event["previous_sha256"] != previous
                    or not _SHA.fullmatch(event["seed_sha256"])
                    or digest(summary) != event["summary_sha256"]
                    or summary["contract_version"] != "nexus.learning-view.v1"
                    or not isinstance(event["registrar_id"], str)
                    or not event["registrar_id"]
                    or type(event["registered_at"]) not in (int, float)
                    or not math.isfinite(event["registered_at"])
                    or (history and event["registered_at"] < history[-1]["registered_at"])
                ):
                    _refuse()
                history.append(event)
                previous = recorded_hash
        except (TypeError, ValueError, KeyError, OverflowError):
            _refuse()
        return history

    def read(self) -> dict[str, Any] | None:
        self._safe_path()
        if not self.path.exists():
            return None
        try:
            with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as db:
                db.execute("BEGIN")
                history = self._read(db)
                return history[-1] if history else None
        except sqlite3.Error:
            _refuse()

    def attach(
        self, summary: dict[str, Any], *, seed_sha256: str, registrar_id: str, registered_at: float
    ) -> dict[str, Any]:
        self._safe_path()
        value = json.loads(canonical(summary))
        if (
            not isinstance(seed_sha256, str)
            or not _SHA.fullmatch(seed_sha256)
            or not isinstance(registrar_id, str)
            or not registrar_id
            or type(registered_at) not in (int, float)
            or not math.isfinite(registered_at)
            or value.get("contract_version") != "nexus.learning-view.v1"
        ):
            raise StoreError("invalid_plan", "Learning attachment binding is invalid")
        self.root.mkdir(parents=True, exist_ok=True)
        self._safe_path()
        try:
            with closing(sqlite3.connect(self.path, timeout=10)) as db:
                db.execute("PRAGMA synchronous=FULL")
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    objects = db.execute(
                        "SELECT name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
                    ).fetchall()
                    if not objects:
                        for statement in _DDL.values():
                            db.execute(statement)
                        db.execute(
                            "INSERT INTO attachment_identity VALUES (1,?)", (self.runtime_identity,)
                        )
                    history = self._read(db)
                    identity = digest(value)
                    if (
                        history
                        and history[-1]["summary_sha256"] == identity
                        and history[-1]["seed_sha256"] == seed_sha256
                    ):
                        return history[-1]
                    if history and registered_at < history[-1]["registered_at"]:
                        raise StoreError(
                            "invalid_clock", "Learning attachment time cannot move backwards"
                        )
                    event = {
                        "sequence": len(history) + 1,
                        "runtime_identity": self.runtime_identity,
                        "seed_sha256": seed_sha256,
                        "summary_sha256": identity,
                        "report": value,
                        "registered_at": registered_at,
                        "registrar_id": registrar_id,
                        "previous_sha256": digest(history[-1]) if history else _ZERO,
                    }
                    db.execute(
                        "INSERT INTO attachment_events VALUES (?,?,?)",
                        (event["sequence"], canonical(event), digest(event)),
                    )
                    self._read(db)
                    return event
        except sqlite3.Error:
            _refuse()
