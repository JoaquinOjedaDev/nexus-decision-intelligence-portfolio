"""Portable alert lifecycle: durable local delivery, not a remote paging claim."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class AlertBook:
    """A restart-safe, idempotent synthetic incident and notification journal."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY, status TEXT NOT NULL,
                    created_at TEXT NOT NULL, resolved_at TEXT, actor TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS notifications (
                    id INTEGER PRIMARY KEY, alert_id INTEGER NOT NULL,
                    transition TEXT NOT NULL, delivered_at TEXT NOT NULL,
                    UNIQUE(alert_id, transition)
                );
            """)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def transition(self, action: str, actor: str) -> dict[str, Any]:
        if action not in {"trigger", "resolve"}:
            raise ValueError("Unknown incident action")
        now = datetime.now(UTC).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            active = db.execute("SELECT id FROM alerts WHERE status='firing'").fetchone()
            if action == "trigger" and active is None:
                cursor = db.execute(
                    "INSERT INTO alerts(status,created_at,actor) VALUES ('firing',?,?)",
                    (now, actor),
                )
                db.execute(
                    "INSERT INTO notifications(alert_id,transition,delivered_at) VALUES (?,?,?)",
                    (cursor.lastrowid, "firing", now),
                )
            elif action == "resolve" and active is not None:
                db.execute(
                    "UPDATE alerts SET status='resolved',resolved_at=? WHERE id=?",
                    (now, active["id"]),
                )
                db.execute(
                    "INSERT INTO notifications(alert_id,transition,delivered_at) VALUES (?,?,?)",
                    (active["id"], "resolved", now),
                )
        return self.snapshot()

    def snapshot(self) -> dict[str, Any]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT 50").fetchall()
            delivered = db.execute("SELECT count(*) FROM notifications").fetchone()[0]
        return {
            "alerts": [
                {
                    **dict(row),
                    "severity": "warning",
                    "message": "Simulated source-freshness breach (training exercise)",
                    "delivery": "durable local notification journal",
                }
                for row in rows
            ],
            "firing": any(row["status"] == "firing" for row in rows),
            "notifications_delivered": delivered,
            "channel": "local-journal",
        }
