"""One-command preparation, loopback serving, and isolated portfolio acceptance."""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import typer

from nexus.config import PROJECT_ROOT
from nexus.portfolio.postgres_cli import app as postgres_app
from nexus.portfolio.service_cli import app as service_app

if TYPE_CHECKING:
    from nexus.portfolio.local_copilot_documents import DocumentCorpus
    from nexus.portfolio.local_provider import LocalModelConfig
    from nexus.portfolio.store import PortfolioStore

app = typer.Typer(no_args_is_help=True, help="Local portfolio control tower")
app.add_typer(postgres_app, name="postgres", help="Opt-in local PostgreSQL runtime (experimental)")
app.add_typer(
    service_app, name="service", help="Opt-in pinned local service runtime (experimental)"
)
DEFAULT_STATE = PROJECT_ROOT / "artifacts" / "portfolio"
DEFAULT_RUNTIME_STATE = PROJECT_ROOT / "artifacts" / "portfolio-runtime"


def _demo_local_copilot() -> tuple[LocalModelConfig, DocumentCorpus]:
    """Opt-in, pinned local inference and current fictional demo evidence."""
    from nexus.portfolio.local_copilot_documents import load_corpus
    from nexus.portfolio.local_provider import LocalModelConfig

    return (
        LocalModelConfig(
            "http://127.0.0.1:11435",
            "qwen3.5:9b-q4_K_M",
            "6488c96fa5faab64bb65cbd30d4289e20e6130ef535a93ef9a49f42eda893ea7",
        ),
        load_corpus(PROJECT_ROOT / "fixtures/local_copilot/portfolio-demo-v1/documents.json"),
    )


@app.command("learning-fixture")
def prepare_learning_fixture(output_dir: Annotated[Path, typer.Argument()]) -> None:
    """Generate a separate synthetic year with explicit correction-finalization evidence."""
    from nexus.portfolio.training_fixture import generate_training_fixture

    result = generate_training_fixture(output_dir)
    typer.echo(
        json.dumps(
            {
                "source_dir": str(result.source_dir),
                "finalization_path": str(result.finalization_path),
                "source_snapshot_sha256": result.source_snapshot_sha256,
                "planning_cutoff": result.planning_cutoff.isoformat(),
            },
            indent=2,
        )
    )


@app.command("learn-runtime")
def train_connected_candidate(
    source_dir: Annotated[Path, typer.Argument()],
    finalization_file: Annotated[Path | None, typer.Option()] = None,
    output_dir: Annotated[
        Path | None, typer.Option(help="New empty isolated evidence directory")
    ] = None,
    record_review: Annotated[
        bool, typer.Option(help="Record trusted local review and offline shadow replay")
    ] = False,
    track_local: Annotated[
        bool, typer.Option(help="Log and verify this run in isolated local MLflow (tracking group)")
    ] = False,
    runtime_state: Annotated[
        Path | None,
        typer.Option(help="Separate runtime directory for the read-only learning panel"),
    ] = None,
) -> None:
    """Validate mature labels, train and evaluate locally; never change the deployed champion."""
    from nexus.portfolio.training_data import build_training_snapshot
    from nexus.portfolio.training_pipeline import (
        load_learning_plan,
        persist_learning_run,
        run_learning_experiment,
    )
    from nexus.r2 import run_r2_rebuild

    if output_dir is None:
        artifacts = PROJECT_ROOT / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        output_dir = Path(tempfile.mkdtemp(prefix="runtime-learning-", dir=artifacts))
    elif output_dir.exists() and any(output_dir.iterdir()):
        raise typer.BadParameter("learning evidence directory must be new or empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    plan = load_learning_plan(PROJECT_ROOT / "configs/portfolio/runtime-learning.yaml")
    if runtime_state is not None and runtime_state.resolve() == DEFAULT_STATE.resolve():
        raise typer.BadParameter("choose a separate runtime state; the frozen demo is preserved")
    rebuild = run_r2_rebuild(
        project_root=PROJECT_ROOT,
        source_dir=source_dir,
        artifact_dir=output_dir / "gold",
        planning_cutoff=plan.evaluation_cutoff,
    )
    snapshot = build_training_snapshot(
        rebuild,
        source_dir=source_dir,
        finalization_path=finalization_file,
        as_of=plan.evaluation_cutoff,
    )
    run = run_learning_experiment(PROJECT_ROOT, snapshot, plan)
    path = persist_learning_run(output_dir, run)
    tracking: dict[str, Any] | None = None
    if track_local:
        from nexus.portfolio.training_tracking import track_learning_run

        tracking = track_learning_run(run, output_dir / "tracking", project_root=PROJECT_ROOT)
    review: dict[str, Any] | None = None
    display: dict[str, Any] | None = None
    if record_review or runtime_state is not None:
        from nexus.contracts import ActorRole
        from nexus.portfolio.auth import Actor

        locations = tuple(sorted({label.location_id for label in snapshot.labels}))
        registrar = Actor(
            "learning.cli.registrar",
            "Local trusted learning registrar",
            ActorRole.SYSTEM,
            locations,
        )
        if record_review and run.candidate is not None:
            from nexus.modeling.r3_artifacts import write_immutable_json_manifest
            from nexus.portfolio.training_governance import record_learning_governance

            governance = record_learning_governance(
                run,
                output_dir / "review",
                proposer=Actor(
                    "learning.cli.planner", "Local demo planner", ActorRole.PLANNER, locations
                ),
                registrar=registrar,
                expected_source_sha256=snapshot.source_snapshot_sha256,
            )
            ref = write_immutable_json_manifest(
                output_dir,
                dataset="runtime.learning_governance",
                manifest=governance,
                schema_identity={"contract": "nexus.learning-governance-bridge.v1"},
            )
            review = {
                "candidate_id": governance["candidate_id"],
                "receipt": str(ref.path),
                "deployment_applied": False,
                "human_approval_recorded": False,
            }
        if runtime_state is not None:
            from nexus.portfolio.learning_views import attach_learning_report
            from nexus.portfolio.runtime_service import prepare_runtime

            store = prepare_runtime(
                PROJECT_ROOT,
                runtime_state,
                source_dir=source_dir,
                planning_cutoff=plan.evaluation_cutoff,
            )
            display_registrar = Actor(
                registrar.actor_id,
                registrar.name,
                ActorRole.SYSTEM,
                tuple(sorted({item["location_id"] for item in store.seed()["recommendations"]})),
            )
            display = attach_learning_report(store, run, actor=display_registrar)
    typer.echo(
        json.dumps(
            {
                "status": run.receipt["status"],
                "reason": run.receipt.get("reason"),
                "training_performed": run.receipt["training_performed"],
                "receipt": str(path),
                "receipt_sha256": run.receipt_sha256,
                "splits": run.receipt.get("splits", {}),
                "metrics": run.receipt.get("metrics", {}),
                "scientific_champion_changed": False,
                "tracking": tracking,
                "review": review,
                "display": display,
            },
            indent=2,
        )
    )


@app.command("learn-feedback")
def train_feedback_candidate(
    source_dir: Annotated[Path, typer.Argument(help="Verified R2 source package")],
    feedback_receipt_id: Annotated[str, typer.Option(help="Existing explicit proposal receipt")],
    feedback_journal: Annotated[Path, typer.Option(help="Existing feedback SQLite journal")],
    learning_journal: Annotated[Path, typer.Option(help="Existing initialized learning journal")],
    finalization_file: Annotated[Path, typer.Option(help="Explicit source-bound label closure")],
    output_dir: Annotated[Path, typer.Option(help="New isolated evidence directory")],
) -> None:
    """Admit a source-backed experiment from an existing trigger; never propose or deploy."""
    import sqlite3
    from contextlib import closing

    from nexus.contracts import ActorRole
    from nexus.portfolio.auth import Actor
    from nexus.portfolio.feedback import FeedbackBook
    from nexus.portfolio.feedback_training import run_feedback_training
    from nexus.portfolio.learning import LearningBook
    from nexus.portfolio.training_pipeline import load_learning_plan

    # Constructors may initialize new journals. Inspect supplied files read-only
    # first so a typo or unrelated database is not silently initialized here.
    for path, required in (
        (feedback_journal, {"feedback_events", "feedback_receipts"}),
        (learning_journal, {"learning_events", "learning_artifacts"}),
    ):
        if not path.is_file():
            raise typer.BadParameter("feedback and learning journals must already exist")
        try:
            with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
                tables = {
                    row[0]
                    for row in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")
                }
                if not required.issubset(tables):
                    raise typer.BadParameter("supplied journal has the wrong schema")
        except sqlite3.Error as error:
            raise typer.BadParameter(
                "supplied journal is not a readable SQLite database"
            ) from error
    learning = LearningBook(learning_journal)
    actor = Actor(
        "feedback.learning.cli",
        "Local trusted feedback training worker",
        ActorRole.SYSTEM,
        tuple(learning.snapshot()["locations"]),
    )
    result = run_feedback_training(
        PROJECT_ROOT,
        feedback_book=FeedbackBook(feedback_journal),
        feedback_receipt_id=feedback_receipt_id,
        learning_book=learning,
        actor=actor,
        source_dir=source_dir,
        finalization_path=finalization_file,
        plan=load_learning_plan(PROJECT_ROOT / "configs/portfolio/runtime-learning.yaml"),
        output_dir=output_dir,
    )
    typer.echo(json.dumps(result, indent=2, default=str))


@app.command("prepare-runtime")
def prepare_connected(
    state_dir: Annotated[
        Path, typer.Option(help="Separate persistent runtime directory")
    ] = DEFAULT_RUNTIME_STATE,
    source_dir: Annotated[
        Path | None, typer.Option(help="Checksum-bound R2 source package")
    ] = None,
) -> None:
    """Build one connected source/Gold/forecast/optimizer run; never reset frozen state."""
    from nexus.portfolio.runtime_service import prepare_runtime

    store = prepare_runtime(PROJECT_ROOT, state_dir, source_dir=source_dir)
    typer.echo(
        json.dumps(
            {
                "status": "ready",
                "state_dir": str(state_dir.resolve()),
                "metadata": store.seed()["metadata"],
            },
            indent=2,
        )
    )


@app.command("runtime-actual")
def import_runtime_actual(
    actual_file: Annotated[Path, typer.Argument(help="One trusted ActualRevision JSON event")],
    state_dir: Annotated[Path, typer.Option()] = DEFAULT_RUNTIME_STATE,
) -> None:
    """Record an explicitly finalized/corrected actual; never infer closure or train."""
    from nexus.contracts import ActorRole
    from nexus.portfolio.auth import Actor
    from nexus.portfolio.feedback import ActualRevision
    from nexus.portfolio.runtime_feedback import ingest_runtime_actual, monitor_runtime_feedback
    from nexus.portfolio.runtime_service import prepare_runtime

    revision = ActualRevision.model_validate_json(actual_file.read_text(encoding="utf-8"))
    store = prepare_runtime(PROJECT_ROOT, state_dir)
    actor = Actor(
        "runtime.actuals.cli", "Local trusted actual publisher", ActorRole.SYSTEM, store.locations()
    )
    result = ingest_runtime_actual(store, revision, actor=actor)
    typer.echo(
        json.dumps(
            {"ingestion": result, "feedback": monitor_runtime_feedback(store)},
            indent=2,
            default=str,
        )
    )


@app.command("verify-recovery")
def verify_local_recovery(
    output_dir: Annotated[Path, typer.Argument(help="New isolated recovery evidence directory")],
) -> None:
    """Crash and recover only a new synthetic child process; preserve existing demo state."""
    from nexus.portfolio.recovery import run_recovery_acceptance

    result = run_recovery_acceptance(output_dir, allow_mutation=True)
    typer.echo(json.dumps(result, indent=2, default=str))
    if result["status"] != "pass":
        raise typer.Exit(1)


@app.command("serve-runtime")
def serve_connected(
    state_dir: Annotated[
        Path, typer.Option(help="Separate persistent runtime directory")
    ] = DEFAULT_RUNTIME_STATE,
    port: Annotated[int, typer.Option(min=1024, max=65535)] = 8766,
    local_model: Annotated[
        bool, typer.Option(help="Enable pinned local model and fictional demo documents")
    ] = False,
) -> None:
    """Serve the connected pipeline on a separate loopback port."""
    import uvicorn

    from nexus.portfolio.api import create_app

    typer.echo(f"NEXUS connected local runtime: http://127.0.0.1:{port}")
    model, documents = _demo_local_copilot() if local_model else (None, None)
    uvicorn.run(
        create_app(
            state_dir,
            PROJECT_ROOT,
            pipeline="runtime",
            local_model=model,
            local_documents=documents,
        ),
        host="127.0.0.1",
        port=port,
        proxy_headers=False,
    )


@app.command("runtime-event")
def submit_runtime_event(
    event_file: Annotated[
        Path, typer.Argument(help="EventEnvelope JSON with expected source hash")
    ],
    state_dir: Annotated[Path, typer.Option()] = DEFAULT_RUNTIME_STATE,
) -> None:
    """Receipt a local planner event; keep its event ID/content unchanged for retries."""
    from nexus.contracts import EventEnvelope
    from nexus.portfolio.auth import resolve_persona
    from nexus.portfolio.runtime_control import RuntimeControl
    from nexus.portfolio.runtime_service import prepare_runtime

    store = prepare_runtime(PROJECT_ROOT, state_dir)
    event = EventEnvelope.model_validate_json(event_file.read_text(encoding="utf-8"))
    result = RuntimeControl(store).submit(event, resolve_persona("planner", store.locations()))
    typer.echo(json.dumps(result, indent=2))


@app.command("verify-runtime")
def verify_connected(
    state_dir: Annotated[
        Path | None,
        typer.Option(help="New empty acceptance directory; default retains a unique one"),
    ] = None,
) -> None:
    """Run the real connected HTTP workflow and retain its evidence without touching the demo."""
    from nexus.portfolio.runtime_acceptance import run_runtime_acceptance

    if state_dir is None:
        artifacts = PROJECT_ROOT / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        state_dir = Path(tempfile.mkdtemp(prefix="runtime-acceptance-", dir=artifacts))
    result = run_runtime_acceptance(PROJECT_ROOT, state_dir)
    typer.echo(json.dumps(result, indent=2))
    if result["status"] != "pass":
        raise typer.Exit(1)


@app.command("runtime-worker")
def run_connected_worker(
    source_dir: Annotated[Path, typer.Argument(help="Source package matching the queued hash")],
    state_dir: Annotated[Path, typer.Option()] = DEFAULT_RUNTIME_STATE,
) -> None:
    """Process at most one eligible job; the worker cannot approve or execute orders."""
    from nexus.portfolio.runtime_service import prepare_runtime, run_runtime_worker

    store = prepare_runtime(PROJECT_ROOT, state_dir)
    result = run_runtime_worker(PROJECT_ROOT, store, source_dir=source_dir)
    typer.echo(json.dumps(result, indent=2))
    if result["status"] == "failed":
        raise typer.Exit(1)


def _prepare_demo(state_dir: Path) -> PortfolioStore:
    """Validate evidence before opening workflow state or exposing HTTP routes."""
    from nexus.portfolio.engine import StalePortfolioEvidenceError, build_seed
    from nexus.portfolio.store import PortfolioStore

    try:
        seed = build_seed(PROJECT_ROOT, state_dir / "evidence")
    except StalePortfolioEvidenceError as error:
        fresh = state_dir.resolve().with_name(state_dir.name + "-fresh")
        suffix = 2
        while fresh.exists():
            fresh = state_dir.resolve().with_name(f"{state_dir.name}-fresh-{suffix}")
            suffix += 1
        typer.echo(
            "Saved demo evidence is stale. Existing evidence and decisions were preserved. "
            "Prepare and serve a new state directory with these matching commands:\n"
            f'uv run --group portfolio nexus portfolio prepare --state-dir "{fresh}"\n'
            f'uv run --group portfolio nexus portfolio serve --state-dir "{fresh}"',
            err=True,
        )
        raise typer.Exit(2) from error
    store = PortfolioStore(state_dir)
    store.ensure_seed(seed)
    return store


@app.command()
def prepare(
    state_dir: Annotated[Path, typer.Option(help="Persistent demo directory")] = DEFAULT_STATE,
) -> None:
    """Run source/dbt checks and real optimization, preserving previous decisions."""
    seed = _prepare_demo(state_dir).seed()
    typer.echo(
        json.dumps(
            {"status": "ready", "state_dir": str(state_dir.resolve()), "metrics": seed["metrics"]},
            indent=2,
        )
    )


@app.command()
def serve(
    state_dir: Annotated[Path, typer.Option(help="Persistent demo directory")] = DEFAULT_STATE,
    port: Annotated[int, typer.Option(min=1024, max=65535)] = 8765,
    local_model: Annotated[
        bool, typer.Option(help="Enable pinned local model and fictional demo documents")
    ] = False,
) -> None:
    """Serve only on 127.0.0.1; demo personas are not production authentication."""
    import uvicorn

    from nexus.portfolio.api import create_app

    store = _prepare_demo(state_dir)
    model, documents = _demo_local_copilot() if local_model else (None, None)
    application = create_app(state_dir, PROJECT_ROOT, local_model=model, local_documents=documents)
    application.state.portfolio_store = store
    typer.echo(f"NEXUS local demo: http://127.0.0.1:{port} (Ctrl+C to stop)")
    uvicorn.run(application, host="127.0.0.1", port=port, proxy_headers=False)


def run_acceptance(root: Path, state_dir: Path) -> dict[str, Any]:
    """Exercise real seed + public HTTP workflow, then reopen the durable database."""
    from fastapi.testclient import TestClient

    from nexus.portfolio.api import create_app
    from nexus.portfolio.engine import build_seed, load_seed
    from nexus.portfolio.store import PortfolioStore

    started = time.monotonic()
    seed = build_seed(root, state_dir / "evidence")
    now = [time.time()]
    store = PortfolioStore(state_dir, clock=lambda: now[0])
    store.ensure_seed(seed)
    application = create_app(state_dir, root)
    application.state.portfolio_store = store
    checks: list[str] = []

    def check(condition: bool, name: str) -> None:
        if not condition:
            raise ValueError(f"Portfolio acceptance failed: {name}")
        checks.append(name)

    with TestClient(application) as client:
        check(client.get("/").status_code == 200, "interface_served")
        check(client.get("/api/recommendations").status_code == 401, "session_required")
        session = client.post("/api/session", json={"persona": "planner"}).json()
        headers = {"X-CSRF-Token": session["csrf_token"]}
        items = client.get("/api/recommendations").json()["items"]
        check(len(items) > 0, "computed_recommendations_available")
        item = items[0]
        endpoint = f"/api/recommendations/{item['id']}"
        binding = {"fingerprint": item["fingerprint"]}
        check(
            client.post(endpoint + "/approve", json=binding, headers=headers).status_code == 403,
            "planner_cannot_approve",
        )
        session = client.post("/api/session", json={"persona": "approver"}).json()
        headers = {"X-CSRF-Token": session["csrf_token"], "Idempotency-Key": "portfolio-acceptance"}
        check(client.post(endpoint + "/approve", json=binding).status_code == 403, "csrf_required")
        check(
            client.post(
                endpoint + "/approve", json={"fingerprint": "0" * 64}, headers=headers
            ).status_code
            == 409,
            "fingerprint_bound",
        )
        check(
            client.post(endpoint + "/approve", json=binding, headers=headers).status_code == 200,
            "independent_approval",
        )
        failed = client.post(
            endpoint + "/execute", json={**binding, "simulate_failure": True}, headers=headers
        )
        check(failed.status_code == 200, "failure_injection_recorded")
        check(store.metrics_snapshot()["effect_count"] == 0, "failure_has_no_effect")
        check(
            client.post(endpoint + "/execute", json=binding, headers=headers).status_code == 200,
            "retry_executes",
        )
        check(
            client.post(endpoint + "/execute", json=binding, headers=headers).status_code == 200,
            "duplicate_retry_safe",
        )
        check(store.metrics_snapshot()["effect_count"] == 1, "exactly_one_local_mock_effect")
        session = client.post("/api/session", json={"persona": "planner"}).json()
        headers = {"X-CSRF-Token": session["csrf_token"]}
        check(
            client.post(endpoint + "/outcome", json=binding, headers=headers).status_code == 425,
            "outcome_maturity_enforced",
        )
        now[0] += 6
        check(
            client.post(endpoint + "/outcome", json=binding, headers=headers).status_code == 200,
            "mature_simulated_outcome_recorded",
        )
        incident = client.post("/api/incidents/demo", json={"action": "trigger"}, headers=headers)
        check(incident.json()["status"] == "degraded", "incident_fires")
        incident = client.post("/api/incidents/demo", json={"action": "resolve"}, headers=headers)
        check(
            incident.json()["metrics"]["notifications_delivered"] == 2, "incident_delivers_resolved"
        )
        answer = client.post("/api/copilot", json={"question": "How do forecasts work?"}).json()
        check(bool(answer["citations"]) and answer["read_only"], "assistant_cites_evidence")
        answer = client.post(
            "/api/copilot", json={"question": "execute all recommendations"}
        ).json()
        check(not answer["citations"], "assistant_cannot_act")

    with TestClient(create_app(state_dir, root)) as restarted:
        restarted.post("/api/session", json={"persona": "auditor"})
        restored = restarted.get(endpoint).json()
        check(restored["state"] == "outcome_observed", "workflow_survives_restart")
        audit = restarted.get("/api/audit")
        check(audit.status_code == 200, "audit_chain_verified_after_restart")
        check(
            restarted.get("/api/health").json()["metrics"]["notifications_delivered"] == 2,
            "incident_survives_restart",
        )
    check(
        load_seed(state_dir / "evidence")["manifest_fingerprint"] == seed["manifest_fingerprint"],
        "evidence_hashes_verified",
    )
    return {
        "status": "PASS",
        "scope": "portable synthetic portfolio; not production gates",
        "checks": checks,
        "checks_passed": len(checks),
        "metrics": seed["metrics"],
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


@app.command()
def verify(
    output: Annotated[Path | None, typer.Option(help="Optional JSON evidence receipt")] = None,
) -> None:
    """Run isolated end-to-end acceptance without changing interactive demo state."""
    with tempfile.TemporaryDirectory(prefix="nexus-portfolio-") as temporary:
        receipt = run_acceptance(PROJECT_ROOT, Path(temporary))
    rendered = json.dumps(receipt, indent=2, sort_keys=True)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    typer.echo(rendered)
