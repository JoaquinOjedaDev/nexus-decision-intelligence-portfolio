"""Authenticated health, incident, and read-only reference endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from nexus.portfolio.copilot import answer_question
from nexus.portfolio.local_copilot import LocalQuestion, answer_local
from nexus.portfolio.observability import AlertBook
from nexus.portfolio.service_backend import (
    backend_label,
    guarded_view,
    metrics,
    runtime_status,
    state_root,
)

router = APIRouter()


class Question(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    question: str = Field(min_length=3, max_length=1000)


class IncidentAction(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    action: Literal["trigger", "resolve"]


def _book(request: Request) -> AlertBook:
    from nexus.portfolio.api import get_store

    return AlertBook(state_root(get_store(request)) / "alerts.sqlite3")


@router.get("/api/health")
def health(request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import get_store, require_actor

    actor = require_actor(request)
    store = get_store(request)
    if request.app.state.portfolio_pipeline == "runtime":
        from nexus.portfolio.runtime_feedback import authorize_runtime_feedback_scope

        authorize_runtime_feedback_scope(store, actor)
    measured_metrics = metrics(store, actor)
    incidents = _book(request).snapshot()
    failed = measured_metrics["states"].get("execution_failed", 0)
    result: dict[str, Any] = {
        "status": "degraded" if incidents["firing"] or failed else "healthy",
        "mode": "local-demo",
        "checks": [
            {
                "name": "Mock execution delivery",
                "status": "degraded" if failed else "healthy",
                "value": failed,
                "detail": "Failed deliveries awaiting authorized retry; no side effect on failure.",
            },
            {
                "name": "Durable state",
                "status": "healthy",
                "value": backend_label(store),
                "detail": "Live database read succeeded; approvals and effects persist on restart.",
            },
            {
                "name": "Execution effects",
                "status": "healthy",
                "value": measured_metrics["effect_count"],
                "detail": "Unique committed mock ERP effects; no real purchases.",
            },
            {
                "name": "Synthetic freshness exercise",
                "status": "degraded" if incidents["firing"] else "healthy",
                "value": "firing" if incidents["firing"] else "clear",
                "detail": "Operator-triggered incident simulation, not a live upstream monitor.",
            },
            {
                "name": "Notification journal",
                "status": "healthy",
                "value": incidents["notifications_delivered"],
                "detail": "Persisted local firing/resolved notifications. No external paging.",
            },
        ],
        "alerts": incidents["alerts"],
        "metrics": {
            **measured_metrics,
            "notifications_delivered": incidents["notifications_delivered"],
        },
    }
    if request.app.state.portfolio_pipeline == "runtime":
        from nexus.portfolio.runtime_feedback import runtime_feedback_summary
        from nexus.portfolio.runtime_monitoring import monitor_runtime

        control = runtime_status(store, actor)
        measured = monitor_runtime(store, actor=actor)
        feedback = runtime_feedback_summary(store, actor)
        source_checks = {
            key: row for key, row in measured["checks"].items() if row["kind"] == "source_freshness"
        }
        source_status = (
            "firing"
            if any(row["status"] == "firing" for row in source_checks.values())
            or any(
                row["check_id"].endswith(":source_freshness") for row in measured["active_alerts"]
            )
            else "unknown"
            if not source_checks
            or any(row["status"] != "healthy" for row in source_checks.values())
            else "healthy"
        )
        operational = control["operational"]
        failed_jobs = operational["failed_unresolved"]
        pending_jobs = operational["queued"] + operational["running"]
        result["runtime"] = control
        result["measured_monitoring"] = measured
        result["delayed_feedback"] = feedback
        result["forecast_quality_authority"] = "delayed_feedback"
        result["checks"].append(
            {
                "name": "Delayed outcome evidence",
                "status": "degraded"
                if feedback["quality_status"] == "firing"
                else feedback["quality_status"],
                "value": feedback["actual_versions"],
                "detail": (
                    f"{feedback['forecast_generations']} retained forecast generations; "
                    "explicit correction closure required. Post-hoc forecasts are excluded."
                ),
            }
        )
        result["checks"].append(
            {
                "name": "Measured monitor worker",
                "status": "degraded" if request.app.state.runtime_monitor_error else "healthy",
                "value": "60 s cadence",
                "detail": (
                    "Runs while this local runtime is open; no monitoring when the PC is off."
                ),
            }
        )
        for check in source_checks.values():
            result["checks"].append(
                {
                    "name": "Local ingestion freshness",
                    "status": "degraded" if check["status"] == "firing" else check["status"],
                    "value": check["status"].replace("_", " "),
                    "detail": (
                        "Measured from last local pipeline completion; not an external live feed."
                    ),
                }
            )
        result["checks"].append(
            {
                "name": "Durable replanning",
                "status": "degraded" if failed_jobs else "healthy",
                "value": pending_jobs,
                "detail": (
                    f"{failed_jobs} failed jobs; stale advice is invalidated before computation."
                ),
            }
        )
        if (
            source_status == "firing"
            or failed_jobs
            or request.app.state.runtime_monitor_error
            or feedback["quality_status"] == "firing"
        ):
            result["status"] = "degraded"
        elif result["status"] == "healthy":
            result["status"] = (
                "unknown"
                if "unknown" in (source_status, feedback["quality_status"])
                else "healthy"
                if feedback["quality_status"] == "healthy"
                else "insufficient_evidence"
            )
    return result


@router.post("/api/incidents/demo")
def incident(body: IncidentAction, request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import authorize_mutation, get_store

    actor = authorize_mutation(request)
    if actor.role.value != "planner":
        raise HTTPException(
            status_code=403, detail="Only the demo planner can run incident exercises"
        )
    store = get_store(request)
    with guarded_view(store, actor=actor) as view:
        if request.app.state.portfolio_pipeline == "runtime":
            from nexus.portfolio.runtime_feedback import authorize_runtime_feedback_scope

            authorize_runtime_feedback_scope(store, actor, view=view)
        _book(request).transition(body.action, actor.actor_id)
    return health(request)


@router.post("/api/copilot")
def copilot(body: Question, request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import require_actor

    require_actor(request)
    return answer_question(body.question, request.app.state.portfolio_root)


@router.post("/api/copilot/local")
def local_copilot(body: LocalQuestion, request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import authorize_mutation, get_store
    from nexus.portfolio.store import StoreError

    # Read-only, but inference consumes local resources: require a same-session CSRF token.
    authorize_mutation(request)
    provider = request.app.state.local_model_provider
    if provider is None:
        raise StoreError("local_provider_disabled", "Local model inference is not enabled", 503)
    corpus = request.app.state.local_document_corpus
    return answer_local(
        body,
        get_store(request),
        lambda: authorize_mutation(request),
        provider,
        corpus=corpus,
        as_of=datetime.now(UTC).date() if corpus is not None else None,
    )
