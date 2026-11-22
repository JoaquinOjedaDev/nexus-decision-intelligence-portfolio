"""Explicit local PostgreSQL commands; no automatic migration or paid service."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated, Any

import typer

from nexus.config import PROJECT_ROOT
from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.postgres_backend import PostgresBackendError, PostgresConfig
from nexus.portfolio.runtime_service import load_postgres_runtime, prepare_postgres_runtime
from nexus.portfolio.service_backend import runtime_control, runtime_status
from nexus.portfolio.store import StoreError

app = typer.Typer(no_args_is_help=True, help="Explicit prepared local PostgreSQL runtime")


@contextmanager
def _errors() -> Iterator[None]:
    """Never render secret-bearing driver/parser locals in a rich traceback."""
    try:
        yield
    except (PostgresBackendError, StoreError) as error:
        typer.echo(f"{error.code}: {error}", err=True)
        raise typer.Exit(1) from None
    except (ValueError, OSError):
        typer.echo(
            "invalid_input: Runtime input or local evidence could not be read safely", err=True
        )
        raise typer.Exit(1) from None


def read_config(path: Path) -> tuple[Path, PostgresConfig]:
    """Read only application credentials; reject migration keys and implicit paths."""
    location = path.absolute()
    if (
        location.anchor.startswith(("\\\\", "//"))
        or ".." in location.parts
        or any(p.is_symlink() or p.is_junction() for p in (*location.parents, location))
        or not location.is_file()
        or location.stat().st_size > 16_384
    ):
        raise StoreError("configuration_error", "Use a bounded local private application config")
    try:
        values = json.loads(location.read_text(encoding="utf-8"))
        if (
            not isinstance(values, dict)
            or set(values) != {"dsn", "schema", "runtime_root"}
            or any(not isinstance(value, str) or not value for value in values.values())
        ):
            raise ValueError
        root = Path(values["runtime_root"])
        if not root.is_absolute():
            raise ValueError
        config = PostgresConfig(values["dsn"], values["schema"], root / "evidence" / "initial")
        from nexus.portfolio.runtime_service import _postgres_store

        _postgres_store(root, config)
        return root, config
    except (ValueError, TypeError, OSError):
        raise StoreError(
            "configuration_error", "Invalid private PostgreSQL application config"
        ) from None


def _render(result: dict[str, Any]) -> None:
    typer.echo(json.dumps(result, indent=2, default=str))


@app.command()
def prepare(
    config_file: Annotated[Path, typer.Argument(help="Private application-only configuration")],
    source_dir: Annotated[Path | None, typer.Option(help="Checksum-bound source package")] = None,
) -> None:
    """Build initial evidence explicitly; schema must already be migrated separately."""
    with _errors():
        root, config = read_config(config_file)
        store = prepare_postgres_runtime(PROJECT_ROOT, root, config, source_dir=source_dir)
        _render({"status": "ready", "database": "postgresql", "metadata": store.seed()["metadata"]})


@app.command()
def serve(
    config_file: Annotated[Path, typer.Argument(help="Private application-only configuration")],
    port: Annotated[int, typer.Option(min=1024, max=65535)] = 8767,
) -> None:
    """Serve prepared state only, with existing loopback/persona/CSRF boundaries."""
    import uvicorn

    from nexus.portfolio.api import create_app

    with _errors():
        root, config = read_config(config_file)
        store = load_postgres_runtime(PROJECT_ROOT, root, config)
        application = create_app(root, PROJECT_ROOT, pipeline="runtime", postgres=config)
        application.state.portfolio_store = store
        typer.echo(f"NEXUS local PostgreSQL runtime: http://127.0.0.1:{port}")
        uvicorn.run(application, host="127.0.0.1", port=port, proxy_headers=False)


@app.command()
def status(config_file: Annotated[Path, typer.Argument()]) -> None:
    """Read operational status through an explicit local auditor persona."""
    with _errors():
        root, config = read_config(config_file)
        store = load_postgres_runtime(PROJECT_ROOT, root, config)
        _render(runtime_status(store, resolve_persona("auditor", store.locations())))


@app.command()
def event(
    config_file: Annotated[Path, typer.Argument()],
    event_file: Annotated[Path, typer.Argument(help="Stable EventEnvelope JSON")],
) -> None:
    """Submit as a local Planner, not an automatic broker or approval identity."""
    with _errors():
        envelope = EventEnvelope.model_validate_json(event_file.read_text(encoding="utf-8"))
        root, config = read_config(config_file)
        store = load_postgres_runtime(PROJECT_ROOT, root, config)
        _render(
            runtime_control(store).submit(envelope, resolve_persona("planner", store.locations()))
        )


@app.command()
def worker(
    config_file: Annotated[Path, typer.Argument()],
    source_dir: Annotated[Path, typer.Argument(help="Source package matching the queued hash")],
) -> None:
    """Run one eligible job; a system worker cannot approve or execute orders."""
    from nexus.portfolio.runtime_service import run_runtime_worker

    with _errors():
        root, config = read_config(config_file)
        store = load_postgres_runtime(PROJECT_ROOT, root, config)
        result = run_runtime_worker(PROJECT_ROOT, store, source_dir=source_dir)
        _render(result)
        if result["status"] == "failed":
            raise typer.Exit(1)


@app.command()
def actual(
    config_file: Annotated[Path, typer.Argument()],
    actual_file: Annotated[Path, typer.Argument(help="Trusted ActualRevision JSON")],
) -> None:
    """Append explicit source actuals; do not invent maturity or initiate training."""
    from nexus.portfolio.feedback import ActualRevision
    from nexus.portfolio.runtime_feedback import ingest_runtime_actual

    with _errors():
        revision = ActualRevision.model_validate_json(actual_file.read_text(encoding="utf-8"))
        root, config = read_config(config_file)
        store = load_postgres_runtime(PROJECT_ROOT, root, config)
        actor = Actor(
            "runtime.actuals.cli", "Local actual publisher", ActorRole.SYSTEM, store.locations()
        )
        _render(ingest_runtime_actual(store, revision, actor=actor))
