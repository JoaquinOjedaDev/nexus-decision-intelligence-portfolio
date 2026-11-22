"""Small authenticated control surface; all expensive planning stays in a worker."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field

from nexus.contracts import EventEnvelope
from nexus.portfolio.capture_health_ledger import CaptureHealthLedger
from nexus.portfolio.postgres_runtime import PostgresRuntimeControl
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.runtime_control import RuntimeControl
from nexus.portfolio.service_backend import OperationalStore, runtime_control, runtime_status
from nexus.portfolio.source_watch_runtime import SourceWatchRuntimeStore
from nexus.portfolio.store import StoreError

router = APIRouter()


class RuntimeEventRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    event_id: str = Field(pattern=r"^[a-fA-F0-9-]{36}$")
    event_type: str = Field(pattern=r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$", max_length=120)
    occurred_at: str = Field(max_length=40)
    source_snapshot_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


def _runtime_store(request: Request) -> OperationalStore:
    from nexus.portfolio.api import get_store

    if request.app.state.portfolio_pipeline != "runtime":
        raise StoreError(
            "runtime_disabled", "Open the separate connected runtime for this action", 404
        )
    return get_store(request)


def _runtime(request: Request) -> RuntimeControl | PostgresRuntimeControl:
    return runtime_control(_runtime_store(request))


@router.get("/api/runtime/status")
def status(request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import require_actor

    actor = require_actor(request)
    store = _runtime_store(request)
    result = runtime_status(store, actor)
    if isinstance(store, PublicationRuntimeStore):
        result["source_publication"] = store.publication_status(actor)
        if isinstance(store.transport, CaptureHealthLedger):
            result["capture_health"] = store.transport.health_status(store.transport_binding, actor)
    if isinstance(store, SourceWatchRuntimeStore):
        result["source_watch"] = store.source_watch_status(actor)
    return result


@router.post("/api/runtime/events")
def submit(body: RuntimeEventRequest, request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import authorize_mutation

    actor = authorize_mutation(request)
    control = _runtime(request)
    try:
        occurred = datetime.fromisoformat(body.occurred_at)
        event = EventEnvelope(
            event_id=UUID(body.event_id),
            event_type=body.event_type,
            source="local-planner",
            subject_id="portfolio",
            occurred_at=occurred,
            # Stable content across HTTP redelivery; receipt wall time is separately
            # recorded by the durable queue. Both timestamps are validated as UTC.
            ingested_at=occurred,
            idempotency_key=body.event_id,
            payload={"source_snapshot_sha256": body.source_snapshot_sha256},
        )
    except ValueError as error:
        raise StoreError(
            "invalid_event", "Use a valid UUID and timezone-aware timestamp", 422
        ) from error
    if occurred > datetime.fromtimestamp(control.store.clock(), UTC):
        raise StoreError("future_event", "Event time cannot be in the future", 422)
    return control.submit(event, actor)
