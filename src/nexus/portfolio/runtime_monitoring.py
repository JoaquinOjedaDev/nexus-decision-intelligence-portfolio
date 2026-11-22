"""Bind live local-ingestion age and persisted runtime forecasts to measured checks.

Correction closure is never inferred from a demo's five-second outcome animation.
Without registered, correction-closed actuals, forecast quality remains insufficient.
"""

from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from nexus.portfolio.auth import Actor
from nexus.portfolio.monitoring import MonitorActual, MonitorBook, MonitorForecast, MonitorInput
from nexus.portfolio.service_backend import OperationalStore, RuntimeView, guarded_view, state_root
from nexus.portfolio.store import StoreError


def monitor_runtime(
    store: OperationalStore,
    *,
    actuals: tuple[MonitorActual, ...] = (),
    actor: Actor | None = None,
    view: RuntimeView | None = None,
) -> dict[str, Any]:
    """Measure local pipeline age, not freshness of an external business feed.

    Lock order is workflow store then measured journal. Retain the store's writer
    transaction through evaluation/commit so another process cannot activate a
    replacement generation between this snapshot and its monitor publication.
    Capture the clock under that same lock, before assembling forecast inputs;
    a delayed old-generation tick must not acquire a newer generation's time.
    Only bounded validation/metrics run here, never dbt or model training.
    """
    with nullcontext(view) if view is not None else guarded_view(store, actor=actor) as current:
        checked_at = datetime.fromtimestamp(current.checked_at, UTC)
        seed = current.seed
        if actor is not None:
            from nexus.policies.authorization import Permission

            store._permission(actor, Permission.VIEW_AUDIT)
            if not current.retained_locations.issubset(actor.locations):
                raise StoreError("scope_denied", "Monitoring requires all retained locations", 403)
        forecasts = tuple(
            MonitorForecast(
                sku_id=series["sku_id"],
                location_id=series["location_id"],
                issue_time=datetime.fromisoformat(series["origin"]),
                target_date=date.fromisoformat(point["date"]),
                q025=Decimal(str(point["q025"])),
                q10=Decimal(str(point["q10"])),
                q50=Decimal(str(point["q50"])),
                q90=Decimal(str(point["q90"])),
                q975=Decimal(str(point["q975"])),
                model_id=series["model"],
                artifact_sha256=series["forecast_sha256"],
            )
            for series in seed["forecasts"]
            for point in series["points"]
        )
        path = state_root(store) / "monitoring.sqlite3"
        for candidate in (
            path,
            *(Path(str(path) + suffix) for suffix in ("-wal", "-shm", "-journal")),
        ):
            if candidate.is_symlink() or candidate.is_junction():
                raise StoreError("invalid_path", "Linked monitoring journals are forbidden")
        result = MonitorBook(path).evaluate(
            MonitorInput(
                now=checked_at,
                source_watermark=(
                    datetime.fromisoformat(current.runtime_ingested_at)
                    if current.runtime_ingested_at
                    else None
                ),
                source_id="local_runtime_ingestion",
                evidence_sha256=seed["metadata"]["source_snapshot_sha256"],
                forecasts=forecasts,
                actuals=actuals,
            )
        )
    return {
        **result,
        "scope": "Local pipeline ingestion, not an external live feed",
        "historical_source_watermark": seed["metadata"]["source_watermark"],
        "correction_closed_actuals_supplied": len(actuals),
    }
