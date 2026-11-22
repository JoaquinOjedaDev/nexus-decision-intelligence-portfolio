"""Loopback-only FastAPI portfolio application with persistent human decisions."""

from __future__ import annotations

import hmac
import ipaddress
import logging
import os
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from nexus.portfolio.auth import (
    COOKIE_NAME,
    JWT_AUDIENCE,
    JWT_ISSUER,
    PERSONA_NAMES,
    SESSION_TTL_SECONDS,
    Actor,
)
from nexus.portfolio.local_copilot_documents import DocumentCorpus
from nexus.portfolio.local_provider import LocalModelConfig, LocalModelProvider
from nexus.portfolio.postgres_backend import PostgresBackendError, PostgresConfig
from nexus.portfolio.service_backend import OperationalStore
from nexus.portfolio.store import PortfolioStore, StoreError
from nexus.portfolio.transport_runtime import TransportFenceConfig

LOGGER = logging.getLogger(__name__)
LOCAL_MODES = {"local-demo", "portfolio"}


class StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class LoginRequest(StrictRequest):
    persona: Literal["planner", "approver", "auditor"]


class DecisionRequest(StrictRequest):
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    note: str = Field(default="", max_length=1000)


class ExecuteRequest(StrictRequest):
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    simulate_failure: bool = False


def get_store(request: Request) -> OperationalStore:
    """Open the explicit backend; PostgreSQL HTTP never migrates or builds a seed."""

    store = cast(OperationalStore | None, request.app.state.portfolio_store)
    if store is not None:
        return store
    with request.app.state.portfolio_lock:
        store = cast(OperationalStore | None, request.app.state.portfolio_store)
        if store is None:
            if request.app.state.portfolio_postgres is not None:
                from nexus.portfolio.runtime_service import load_postgres_runtime

                store = load_postgres_runtime(
                    request.app.state.portfolio_root,
                    request.app.state.portfolio_state_dir,
                    request.app.state.portfolio_postgres,
                    transport_fence=request.app.state.portfolio_transport_fence,
                )
            elif request.app.state.portfolio_pipeline == "runtime":
                from nexus.portfolio.runtime_service import prepare_runtime

                store = prepare_runtime(
                    request.app.state.portfolio_root, request.app.state.portfolio_state_dir
                )
            else:
                store = PortfolioStore(request.app.state.portfolio_state_dir)
                from nexus.portfolio.engine import build_seed

                # Recheck artifact bytes and governing inputs even after restart.
                seed = build_seed(
                    request.app.state.portfolio_root,
                    request.app.state.portfolio_state_dir / "evidence",
                )
                store.ensure_seed(seed)
            request.app.state.portfolio_store = store
    return store


def _session(request: Request) -> tuple[Actor, str, str] | None:
    token = request.cookies.get(COOKIE_NAME)
    return get_store(request).session(token) if token else None


def require_actor(request: Request) -> Actor:
    session = _session(request)
    if session is None:
        raise StoreError("authentication_required", "Choose a demo persona to continue", 401)
    return session[0]


def authorize_mutation(request: Request) -> Actor:
    """Require a server session and its independent anti-CSRF token."""

    session = _session(request)
    if session is None:
        raise StoreError("authentication_required", "Choose a demo persona to continue", 401)
    supplied = request.headers.get("x-csrf-token", "")
    if not supplied or not hmac.compare_digest(supplied, session[1]):
        raise StoreError("csrf_failed", "Session verification failed; refresh and try again", 403)
    return session[0]


def _error(code: str, message: str, status: int) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": code, "message": message}, "detail": message}, status_code=status
    )


def _is_loopback(host: str) -> bool:
    if host in {"localhost", "testserver", "testclient"}:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def create_app(
    state_dir: Path,
    project_root: Path | None = None,
    *,
    mode: str | None = None,
    pipeline: Literal["frozen", "runtime"] = "frozen",
    postgres: PostgresConfig | None = None,
    local_model: LocalModelConfig | None = None,
    local_documents: DocumentCorpus | None = None,
    transport_fence: TransportFenceConfig | None = None,
) -> FastAPI:
    """Create an app without starting services, building models, or resetting data.

    The app and CLI enforce loopback access. Fixture personas are deliberately
    convenient for a local hiring demo and must not be exposed as production auth.
    """

    root = project_root or Path(__file__).resolve().parents[3]
    if transport_fence is not None and postgres is None:
        raise StoreError(
            "runtime_required", "Transport fencing requires explicit PostgreSQL runtime"
        )
    if postgres is not None and pipeline != "runtime":
        raise StoreError("runtime_required", "PostgreSQL serving requires the runtime pipeline")
    if postgres is not None:
        from nexus.portfolio.runtime_service import _postgres_store

        # Validate the original path before resolve() could hide a linked ancestor.
        # Construction performs no database, filesystem or source mutation.
        _postgres_store(state_dir, postgres, transport_fence=transport_fence)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        monitor = None
        if pipeline == "runtime":
            from nexus.portfolio.runtime_scheduler import RuntimeMonitor

            monitor = RuntimeMonitor(application)
            monitor.start()
        try:
            yield
        finally:
            if monitor is not None:
                monitor.close()

    app = FastAPI(title="NEXUS Decision Intelligence", version="1.0.0", lifespan=lifespan)
    app.state.portfolio_state_dir = state_dir.resolve()
    app.state.portfolio_root = root.resolve()
    app.state.portfolio_mode = mode or os.environ.get("NEXUS_PORTFOLIO_MODE", "local-demo")
    app.state.portfolio_store = None
    app.state.portfolio_pipeline = pipeline
    app.state.portfolio_postgres = postgres
    app.state.portfolio_transport_fence = transport_fence
    app.state.local_model_provider = LocalModelProvider(local_model) if local_model else None
    app.state.local_document_corpus = local_documents
    app.state.runtime_monitor_error = False
    app.state.portfolio_lock = threading.Lock()

    @app.middleware("http")
    async def local_boundary(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        host = request.url.hostname or ""
        peer = request.client.host if request.client else ""
        if not _is_loopback(host) or not _is_loopback(peer):
            return _error("local_only", "The portfolio demo accepts loopback connections only", 403)
        if request.app.state.portfolio_mode not in LOCAL_MODES and request.url.path.startswith(
            "/api/"
        ):
            return _error("fixture_mode_disabled", "Demo personas are disabled in this mode", 403)
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            origin = request.headers.get("origin")
            if origin is not None:
                parsed = urlsplit(origin)
                if (
                    parsed.scheme != request.url.scheme
                    or parsed.netloc.lower() != request.url.netloc.lower()
                    or parsed.path not in {"", "/"}
                    or parsed.query
                    or parsed.fragment
                ):
                    return _error("origin_denied", "Cross-origin changes are not permitted", 403)
            if (
                request.method != "DELETE"
                and request.headers.get("content-type", "").split(";", 1)[0] != "application/json"
            ):
                return _error("json_required", "Send an application/json request", 415)
        try:
            response = await call_next(request)
        except Exception:
            LOGGER.exception("Portfolio request failed")
            response = _error("internal_error", "The request could not be completed safely", 503)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; font-src 'self'; connect-src 'self'; "
            "object-src 'none'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
        )
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(StoreError)
    async def store_error(_request: Request, exc: StoreError) -> JSONResponse:
        return _error(exc.code, str(exc), exc.status)

    @app.exception_handler(PostgresBackendError)
    async def backend_error(_request: Request, exc: PostgresBackendError) -> JSONResponse:
        # The backend's controlled error contains no DSN or driver diagnostics.
        return _error(exc.code, str(exc), 503)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, _exc: RequestValidationError) -> JSONResponse:
        return _error("invalid_request", "Request fields are missing, invalid, or unexpected", 422)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok", "mode": app.state.portfolio_mode}

    @app.get("/readyz")
    def readyz(request: Request) -> dict[str, str]:
        get_store(request).seed()
        return {"status": "ready"}

    def session_metadata() -> dict[str, Any]:
        return {
            "mode": app.state.portfolio_mode,
            "local_copilot_enabled": app.state.local_model_provider is not None,
            "authentication": {
                "token_format": "JWT",
                "issuer": JWT_ISSUER,
                "audience": JWT_AUDIENCE,
                "identity_source": "local-demo-personas",
                "production_sso": False,
            },
            "personas": [
                {"id": name, "name": label, "role": name} for name, label in PERSONA_NAMES.items()
            ],
        }

    @app.get("/api/session")
    def session_info(request: Request) -> dict[str, Any]:
        session = _session(request)
        return {
            **session_metadata(),
            "user": session[0].public() if session else None,
            "csrf_token": session[1] if session else None,
        }

    @app.post("/api/session")
    def login(body: LoginRequest, request: Request, response: Response) -> dict[str, Any]:
        store = get_store(request)
        old = _session(request)
        if old is not None:
            store.revoke_session(old[2])
        token, result = store.create_session(body.persona)
        response.set_cookie(
            COOKIE_NAME,
            token,
            max_age=SESSION_TTL_SECONDS,
            httponly=True,
            secure=request.url.scheme == "https",
            samesite="strict",
            path="/",
        )
        return {**session_metadata(), **result}

    @app.delete("/api/session")
    def logout(request: Request, response: Response) -> dict[str, Any]:
        authorize_mutation(request)
        session = _session(request)
        assert session is not None
        get_store(request).revoke_session(session[2])
        response.delete_cookie(COOKIE_NAME, path="/", httponly=True, samesite="strict")
        return {**session_metadata(), "user": None, "csrf_token": None}

    @app.get("/api/overview")
    def overview(request: Request) -> dict[str, Any]:
        actor = require_actor(request)
        store = get_store(request)
        snapshot = store.overview_snapshot(actor)
        recommendations = snapshot["recommendations"]
        current = snapshot["current_recommendations"]
        counts = {
            state: sum(item["state"] == state for item in recommendations)
            for state in (
                "pending",
                "approved",
                "executed",
                "rejected",
                "revoked",
                "execution_failed",
                "outcome_observed",
                "invalidated",
            )
        }
        counts["total"] = len(recommendations)
        counts["current_generation"] = len(current)
        counts["historical"] = len(recommendations) - len(current)
        counts["current_recommended_units"] = sum(item["recommended_quantity"] for item in current)
        counts["current_order_value"] = sum(item.get("expected_cost", 0) for item in current)
        counts["pending"] = sum(
            item["state"] == "pending" and not item["expired"] for item in recommendations
        )
        counts["expired_review"] = sum(
            item["state"] == "pending" and item["expired"] for item in recommendations
        )
        counts["effect_count"] = sum(
            item["state"] in {"executed", "outcome_observed"} for item in recommendations
        )
        return {
            **snapshot,
            "summary": counts,
            "summary_scope": (
                "Workflow states cover retained history; "
                "planning metrics describe only the current generation."
            ),
        }

    @app.get("/api/recommendations")
    def recommendations(request: Request) -> dict[str, Any]:
        items = get_store(request).recommendations(require_actor(request))
        return {"items": items, "total": len(items)}

    @app.get("/api/recommendations/{recommendation_id}")
    def recommendation(recommendation_id: str, request: Request) -> dict[str, Any]:
        store = get_store(request)
        return store.recommendation_context(recommendation_id, require_actor(request))

    @app.post("/api/recommendations/{recommendation_id}/approve")
    def approve(recommendation_id: str, body: DecisionRequest, request: Request) -> dict[str, Any]:
        return get_store(request).decide(
            recommendation_id, authorize_mutation(request), "approve", body.fingerprint, body.note
        )

    @app.post("/api/recommendations/{recommendation_id}/reject")
    def reject(recommendation_id: str, body: DecisionRequest, request: Request) -> dict[str, Any]:
        return get_store(request).decide(
            recommendation_id, authorize_mutation(request), "reject", body.fingerprint, body.note
        )

    @app.post("/api/recommendations/{recommendation_id}/revoke")
    def revoke(recommendation_id: str, body: DecisionRequest, request: Request) -> dict[str, Any]:
        return get_store(request).decide(
            recommendation_id, authorize_mutation(request), "revoke", body.fingerprint, body.note
        )

    @app.post("/api/recommendations/{recommendation_id}/execute")
    def execute(recommendation_id: str, body: ExecuteRequest, request: Request) -> dict[str, Any]:
        return get_store(request).execute(
            recommendation_id,
            authorize_mutation(request),
            body.fingerprint,
            request.headers.get("idempotency-key", ""),
            simulate_failure=body.simulate_failure,
        )

    @app.post("/api/recommendations/{recommendation_id}/outcome")
    def observe(recommendation_id: str, body: DecisionRequest, request: Request) -> dict[str, Any]:
        return get_store(request).observe(
            recommendation_id, authorize_mutation(request), body.fingerprint, body.note
        )

    @app.get("/api/audit")
    def audit(request: Request) -> dict[str, Any]:
        items = get_store(request).audit(require_actor(request))
        return {"items": items, "total": len(items), "integrity": "verified"}

    from nexus.portfolio.insights import router

    app.include_router(router)
    from nexus.portfolio.runtime_routes import router as runtime_router

    app.include_router(runtime_router)
    from nexus.portfolio.recovery_routes import router as recovery_router

    app.include_router(recovery_router)
    from nexus.portfolio.learning_views import router as learning_router

    app.include_router(learning_router)
    static = Path(__file__).parent / "static"
    if static.is_dir():
        app.mount("/static", StaticFiles(directory=static), name="static")

        @app.get("/")
        def index() -> FileResponse:
            return FileResponse(static / "index.html")

    return app
