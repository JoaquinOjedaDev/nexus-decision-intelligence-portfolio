"""Opt-in commands over prepared, pinned service state; never automatic provisioning."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any

import typer

from nexus.config import PROJECT_ROOT
from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.auth import Actor
from nexus.portfolio.broker_runtime import BrokerAdmissionError
from nexus.portfolio.postgres_cli import _errors, _render
from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger
from nexus.portfolio.runtime_service import load_postgres_runtime, run_runtime_worker
from nexus.portfolio.service_config import ServiceConfig, read_service_config
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.source_publisher import SourceOutboxPublisher
from nexus.portfolio.source_watch_runtime import SourceWatchRuntimeStore
from nexus.portfolio.store import StoreError
from nexus.portfolio.transport_consumer import DurableBrokerConsumer
from nexus.portfolio.transport_ledger import TransportLedger

app = typer.Typer(no_args_is_help=True, help="Explicit pinned local service commands")


class Step(StrEnum):
    observe = "observe"
    publish = "publish"
    capture_operations = "capture-operations"
    capture_audit = "capture-audit"
    dispatch = "dispatch"
    plan = "plan"
    recovery_publish = "recovery-publish"


def open_service(config: ServiceConfig) -> SourceWatchRuntimeStore:
    store = load_postgres_runtime(
        PROJECT_ROOT, config.root, config.workflow, transport_fence=config.fence
    )
    if not isinstance(store, SourceWatchRuntimeStore):
        raise StoreError("configuration_error", "Explicit source-watched runtime required")
    return store


def run_step(config: ServiceConfig, action: Step) -> dict[str, Any]:
    """One bounded unit; supervisor health and short-lived credentials remain separate."""
    if action is Step.capture_audit:
        consumer = DurableBrokerConsumer(TransportLedger(config.audit), config.audit_binding)
        try:
            return {"step": action.value, "result": consumer.poll_batch()}
        finally:
            consumer.close()
    store = open_service(config)
    ledger = store.transport
    if not isinstance(ledger, RecoveryResolutionLedger):
        raise StoreError("configuration_error", "Recovery-enabled operations ledger required")
    result: Any
    if action is Step.observe:
        result = store.poll_source(
            Actor("source-observer", "Source observer", ActorRole.SYSTEM, store.locations())
        )
    elif action is Step.publish:
        result = SourceOutboxPublisher(store).publish_once()
    elif action is Step.capture_operations:
        consumer = DurableBrokerConsumer(ledger, config.fence.binding)
        try:
            result = consumer.poll_batch()
        finally:
            consumer.close()
    elif action is Step.dispatch:
        result = ledger.dispatch_once(config.fence.binding, store.control)
    elif action is Step.plan:
        result = run_runtime_worker(PROJECT_ROOT, store)
    elif action is Step.recovery_publish:
        result = ledger.publish_once(
            config.fence.binding,
            Actor("recovery-publisher", "Recovery publisher", ActorRole.SYSTEM, store.locations()),
        )
    else:
        raise StoreError("invalid_step", "Choose an explicit service step", 422)
    return {"step": action.value, "result": result}


@app.command()
def check(config_file: Annotated[Path, typer.Argument()]) -> None:
    """Validate configuration only; no database, broker or model connection."""
    with _errors():
        config = read_service_config(config_file)
        _render(
            {
                "status": "configuration_valid_not_service_ready",
                "operations": config.fence.binding.payload(),
                "audit": config.audit_binding.payload(),
            }
        )


@app.command("enqueue-source")
def enqueue_source(
    config_file: Annotated[Path, typer.Argument()], event_file: Annotated[Path, typer.Argument()]
) -> None:
    """Retain an explicit event and configured source bytes before broker publication."""
    with _errors():
        path = event_file.absolute()
        if (
            path.anchor.startswith(("\\\\", "//"))
            or ".." in path.parts
            or any(p.is_symlink() or p.is_junction() for p in (*path.parents, path))
            or not path.is_file()
            or path.stat().st_size > 65536
        ):
            raise StoreError("invalid_input", "Use a bounded unlinked local event file")
        with path.open("rb") as handle:
            raw = handle.read(65537)
        if len(raw) > 65536:
            raise StoreError("invalid_input", "Event exceeds the local envelope limit")
        event = EventEnvelope.model_validate_json(raw)
        config = read_service_config(config_file)
        policy = config.fence.source_watch
        assert policy is not None
        bundle = SourceBundle.capture(Path(policy.source_root))
        store = open_service(config)
        result = store.enqueue_source(
            bundle,
            event,
            Actor(
                "source-publisher",
                "Configured source publisher",
                ActorRole.SYSTEM,
                store.locations(),
            ),
        )
        _render(
            {
                "source_publication": result,
                "claim": "retained_intent_not_broker_or_business_completion",
            }
        )


@app.command()
def step(config_file: Annotated[Path, typer.Argument()], action: Step) -> None:
    """Run one explicit worker unit, preserving offsets and all retained evidence."""
    with _errors():
        config = read_service_config(config_file)
        try:
            result = run_step(config, action)
        except BrokerAdmissionError:
            raise StoreError(
                "broker_unavailable", "Broker step refused; reconcile retained state", 503
            ) from None
        _render(result)
        if isinstance(result["result"], dict) and result["result"].get("status") == "failed":
            raise typer.Exit(1)


@app.command()
def serve(
    config_file: Annotated[Path, typer.Argument()],
    port: Annotated[int, typer.Option(min=1024, max=65535)] = 8767,
) -> None:
    """Serve already prepared V8/V11 state with authenticated recovery controls."""
    import uvicorn

    from nexus.portfolio.api import create_app

    with _errors():
        config = read_service_config(config_file)
        store = open_service(config)
        application = create_app(
            config.root,
            PROJECT_ROOT,
            pipeline="runtime",
            postgres=config.workflow,
            transport_fence=config.fence,
        )
        application.state.portfolio_store = store
        uvicorn.run(application, host="127.0.0.1", port=port, proxy_headers=False)


@app.command()
def work(
    config_file: Annotated[Path, typer.Argument()],
    action: Step,
    interval: Annotated[float, typer.Option(min=0.05, max=30)] = 1.0,
    max_steps: Annotated[int, typer.Option(min=0)] = 0,
    manifest_sha256: Annotated[str | None, typer.Option()] = None,
    managed: Annotated[bool, typer.Option()] = False,
) -> None:
    """Run one persistent role; managed stdin EOF or STOP requests graceful shutdown."""
    import threading

    from nexus.portfolio.service_worker import run_worker, watch_parent

    with _errors():
        config = read_service_config(config_file, expected_sha256=manifest_sha256)
        stop = threading.Event()
        if managed:
            threading.Thread(target=watch_parent, args=(stop,), daemon=True).start()
        raise typer.Exit(
            run_worker(config, action, interval=interval, max_steps=max_steps, stop=stop)
        )


@app.command()
def supervise(
    config_file: Annotated[Path, typer.Argument()],
    duration: Annotated[float, typer.Option(min=1, max=86400)],
    interval: Annotated[float, typer.Option(min=0.05, max=30)] = 1.0,
    stale_seconds: Annotated[float, typer.Option(min=10, max=600)] = 120,
    shutdown_seconds: Annotated[float, typer.Option(min=1, max=120)] = 45,
) -> None:
    """Run seven independent roles for an explicit duration; no automatic restart."""
    from nexus.portfolio.service_supervisor import supervise as run_supervisor

    with _errors():
        raise typer.Exit(
            run_supervisor(
                config_file,
                duration=duration,
                interval=interval,
                stale_seconds=stale_seconds,
                shutdown_seconds=shutdown_seconds,
            )
        )
