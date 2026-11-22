"""Local delayed-outcome integration, separate from the animated demo feedback.

Persisting a historical forecast now does not make it prospective. The journal
retains registration time and excludes forecasts registered after the first
actual availability. This adapter never closes a correction window by itself.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.feedback import ActualRevision, FeedbackBook
from nexus.portfolio.monitoring import MonitorBook
from nexus.portfolio.service_backend import (
    OperationalStore,
    RuntimeView,
    guarded_view,
    read_view,
    state_root,
)
from nexus.portfolio.store import StoreError


def _journal_path(store: OperationalStore, name: str) -> Path:
    path = state_root(store) / name
    for candidate in (path, *(Path(str(path) + suffix) for suffix in ("-wal", "-shm", "-journal"))):
        if candidate.is_symlink() or candidate.is_junction():
            raise StoreError("invalid_path", "Linked feedback journals are forbidden")
    return path


def feedback_book(store: OperationalStore) -> FeedbackBook:
    return FeedbackBook(
        _journal_path(store, "feedback.sqlite3"),
        clock=lambda: datetime.fromtimestamp(store.clock(), UTC),
    )


def retain_runtime_forecasts(
    store: OperationalStore,
    *,
    reader: Actor | None = None,
) -> dict[str, Any]:
    """Retain the currently verified workflow seed without synthesizing issue times."""
    with guarded_view(store, actor=reader) as current:
        if reader is not None:
            authorize_runtime_feedback_scope(store, reader, view=current)
        locations = tuple(sorted(current.retained_locations))
        return feedback_book(store).register_current_runtime(
            store,
            actor=Actor(
                "runtime.feedback", "Local forecast publisher", ActorRole.SYSTEM, locations
            ),
            expected_seed_sha256=current.seed_sha256,
            view=current,
        )


def monitor_runtime_feedback(
    store: OperationalStore,
    *,
    reader: Actor | None = None,
) -> dict[str, Any]:
    """Monitor retained generations and actual revisions; never auto-propose or fit."""
    try:
        retain_runtime_forecasts(store, reader=reader)
        return feedback_book(store).evaluate(
            MonitorBook(_journal_path(store, "feedback-monitoring.sqlite3")),
            reader=reader,
        )
    except PermissionError as error:
        if reader is None:
            raise
        raise StoreError(
            "scope_denied", "Feedback generation changed outside actor scope", 403
        ) from error


def ingest_runtime_actual(
    store: OperationalStore, revision: ActualRevision, *, actor: Actor
) -> dict[str, Any]:
    """Trusted local source-publisher boundary, not an unauthenticated HTTP upload."""
    if actor.role is not ActorRole.SYSTEM or revision.location_id not in actor.locations:
        raise PermissionError("actual publisher requires SYSTEM role and matching location")
    retain_runtime_forecasts(store)
    return feedback_book(store).append_actual(revision, actor=actor)


def runtime_feedback_summary(store: OperationalStore, actor: Actor) -> dict[str, Any]:
    """Global summary only; no raw outcomes, forecast rows, or producer event IDs."""
    authorize_runtime_feedback_scope(store, actor)
    result = monitor_runtime_feedback(store, reader=actor)
    if not set(result["locations"]).issubset(actor.locations):
        raise StoreError("scope_denied", "Feedback generation changed outside actor scope", 403)
    checks = result["monitor"]["checks"]
    quality = [row for row in checks.values() if row["kind"] == "forecast_quality"]
    current_quality_status = (
        "firing"
        if any(row["status"] == "firing" for row in quality)
        else "healthy"
        if quality and all(row["status"] == "healthy" for row in quality)
        else "unknown"
        if any(row["status"] == "unknown" for row in quality)
        else "insufficient_evidence"
    )
    active_forecast_alerts = [
        row
        for row in result["monitor"]["active_alerts"]
        if ":forecast:" in row["check_id"] and row["status"] == "firing"
    ]
    quality_status = "firing" if active_forecast_alerts else current_quality_status
    return {
        "contract_version": "nexus.runtime-feedback-summary.v1",
        "scope": "Persisted local forecasts and explicit source actual revisions",
        "quality_status": quality_status,
        "current_quality_status": current_quality_status,
        "active_forecast_alerts": len(active_forecast_alerts),
        "ledger_head_sha256": result["ledger_head_sha256"],
        "evaluation_sha256": result["evaluation_sha256"],
        "forecast_generations": result["forecast_generations"],
        "forecast_rows": result["forecast_rows"],
        "actual_versions": result["actual_versions"],
        "training_performed": False,
        "proposal_created": False,
        "external_live_feed": False,
        "posthoc_forecasts_excluded": True,
    }


def authorize_runtime_feedback_scope(
    store: OperationalStore, actor: Actor, *, view: RuntimeView | None = None
) -> None:
    """Read existing scopes before any journal constructor or global side effect."""
    if view is None:
        with read_view(store, actor=actor) as current:
            authorize_runtime_feedback_scope(store, actor, view=current)
        return
    from nexus.policies.authorization import Permission

    store._permission(actor, Permission.VIEW_AUDIT)
    scope = set(view.retained_locations)
    if not scope.issubset(actor.locations):
        raise StoreError("scope_denied", "Feedback evidence requires all retained locations", 403)
    path = _journal_path(store, "feedback.sqlite3")
    if path.exists():
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            scope.update(
                point["location_id"]
                for event in FeedbackBook._read(db)
                if event["kind"] == "forecast_generation"
                for point in event["data"]["forecasts"]
            )
    if not scope.issubset(actor.locations):
        raise StoreError("scope_denied", "Feedback evidence requires all retained locations", 403)
