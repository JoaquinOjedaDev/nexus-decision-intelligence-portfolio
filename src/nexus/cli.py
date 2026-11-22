"""Cross-platform NEXUS command-line entrypoint."""

from __future__ import annotations

import importlib.metadata
import json
import platform
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer
from rich.console import Console
from rich.table import Table

from nexus import __version__
from nexus.config import PROJECT_ROOT, NexusSettings, load_project_config
from nexus.portfolio.cli import app as portfolio_app
from nexus.storage import run_storage_proof

if TYPE_CHECKING:
    from nexus.data.r2c_models import R2CProfileConfig

app = typer.Typer(no_args_is_help=True, help="NEXUS decision-intelligence platform")
r0_app = typer.Typer(no_args_is_help=True, help="R0 architecture-proof commands")
r1_app = typer.Typer(no_args_is_help=True, help="R1 closed-loop demonstration commands")
r2_app = typer.Typer(no_args_is_help=True, help="R2A trustworthy-data commands")
r2b_app = typer.Typer(no_args_is_help=True, help="R2B partition-replay commands")
r2c_app = typer.Typer(no_args_is_help=True, help="R2C representative-profile commands")
r2d_app = typer.Typer(no_args_is_help=True, help="R2D governed event-path commands")
r3_app = typer.Typer(no_args_is_help=True, help="R3A probabilistic-intelligence commands")
r3b_app = typer.Typer(no_args_is_help=True, help="R3B learned-intelligence commands")
r4a_app = typer.Typer(no_args_is_help=True, help="R4A constrained-decisioning commands")
storage_app = typer.Typer(no_args_is_help=True, help="Storage proof commands")
contracts_app = typer.Typer(no_args_is_help=True, help="Contract validation commands")
app.add_typer(r0_app, name="r0")
app.add_typer(r1_app, name="r1")
app.add_typer(r2_app, name="r2")
app.add_typer(r2b_app, name="r2b")
app.add_typer(r2c_app, name="r2c")
app.add_typer(r2d_app, name="r2d")
app.add_typer(r3_app, name="r3")
app.add_typer(r3b_app, name="r3b")
app.add_typer(r4a_app, name="r4a")
app.add_typer(portfolio_app, name="portfolio")
app.add_typer(storage_app, name="storage")
app.add_typer(contracts_app, name="contracts")
console = Console()


def _command_status(command: list[str]) -> tuple[bool, str]:
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, str(exc)
    detail = (result.stdout or result.stderr).strip().splitlines()
    return result.returncode == 0, detail[0] if detail else f"exit={result.returncode}"


@app.command()
def version() -> None:
    """Print the package version."""

    console.print(__version__)


@app.command()
def doctor() -> None:
    """Inspect the local R0 toolchain without changing external state."""

    settings = NexusSettings.from_environment()
    project = load_project_config()
    table = Table(title="NEXUS R0 doctor")
    table.add_column("Check")
    table.add_column("Value")
    table.add_row("NEXUS", __version__)
    table.add_row("Python", platform.python_version())
    table.add_row("Platform", platform.platform())
    table.add_row("Environment", settings.environment)
    table.add_row("Data profile", settings.data_profile)
    table.add_row("Decision", str(project["decision"]["grain"]))
    for package in (
        "pydantic",
        "duckdb",
        "pyarrow",
        "dbt-duckdb",
        "dagster",
        "psutil",
    ):
        try:
            value = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            value = "missing"
        table.add_row(package, value)
    uv_path = shutil.which("uv")
    table.add_row("uv", uv_path or "missing")
    docker_path = shutil.which("docker")
    if docker_path:
        ok, detail = _command_status([docker_path, "version", "--format", "{{.Server.Version}}"])
        table.add_row("Docker daemon", detail if ok else f"unavailable: {detail}")
    else:
        table.add_row("Docker", "missing")
    console.print(table)


@storage_app.command("prove")
def storage_prove(
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for generated proof evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r0" / "storage",
) -> None:
    """Run the deterministic Parquet + DuckDB architecture proof."""

    result = run_storage_proof(artifact_dir)
    console.print_json(json.dumps(result.__dict__))


@contracts_app.command("validate")
def contracts_validate() -> None:
    """Validate the versioned example contracts against the Pydantic models."""

    from nexus.contracts.validation import validate_contract_examples

    summary = validate_contract_examples(PROJECT_ROOT / "contracts" / "examples")
    console.print_json(json.dumps(summary))


@r0_app.command("verify")
def r0_verify(
    skip_dbt: bool = typer.Option(False, help="Skip the dbt build when debugging locally."),
    with_compose: bool = typer.Option(False, help="Also validate Docker Compose configuration."),
) -> None:
    """Execute the local R0 release gate."""

    from nexus.r0 import verify_r0

    results = verify_r0(project_root=PROJECT_ROOT, run_dbt=not skip_dbt, run_compose=with_compose)
    table = Table(title="NEXUS R0 verification")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Evidence")
    for result in results:
        table.add_row(result.name, "PASS" if result.passed else "FAIL", result.detail)
    console.print(table)
    if not all(result.passed for result in results):
        raise typer.Exit(code=1)


@r1_app.command("demo")
def r1_demo(
    source_dir: Annotated[
        Path,
        typer.Option(help="Directory containing the checked-in synthetic R1 sources."),
    ] = PROJECT_ROOT / "fixtures" / "r1",
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for the R1 decision, ledger, and feedback evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r1" / "demo",
) -> None:
    """Run the complete governed R1 decision lifecycle."""

    from nexus.r1 import run_r1_demo

    result = run_r1_demo(source_dir=source_dir, artifact_dir=artifact_dir)
    table = Table(title="NEXUS R1 closed loop")
    table.add_column("Lifecycle stage")
    table.add_column("Result")
    table.add_column("Evidence")
    table.add_row(
        "1. Sources",
        "CSV batch + JSONL event",
        (
            f"{len(result.data.gold_demand)} demand rows; "
            f"{len(result.data.accepted_inventory_adjustments)} accepted event"
        ),
    )
    table.add_row(
        "2. Data foundation",
        "Validated + point-in-time Gold",
        (f"on hand={result.recommendation.on_hand}; quarantined={len(result.data.quarantined)}"),
    )
    table.add_row(
        "3. Intelligence",
        "Seasonal-naive forecast",
        f"7-day demand={result.forecast.total_expected_demand()}",
    )
    table.add_row(
        "4. Decisioning",
        "Base-stock rule",
        f"recommended quantity={result.recommendation.recommended_quantity}",
    )
    table.add_row(
        "5. Human-approved action",
        result.effect.external_reference,
        (
            f"lifecycle events={len(result.projection.events)}; "
            f"ERP effects after retry={result.effect_count}"
        ),
    )
    table.add_row(
        "6. Feedback",
        f"actual demand={result.actual_demand}",
        f"absolute error={result.absolute_demand_error}",
    )
    console.print(table)
    console.print(f"Evidence: {result.summary_path}")


@r1_app.command("verify")
def r1_verify(
    source_dir: Annotated[
        Path,
        typer.Option(help="Directory containing the checked-in synthetic R1 sources."),
    ] = PROJECT_ROOT / "fixtures" / "r1",
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for repeat-run verification evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r1" / "verification",
) -> None:
    """Execute the deterministic R1 acceptance gate twice."""

    from nexus.r1 import verify_r1

    results = verify_r1(source_dir=source_dir, artifact_dir=artifact_dir)
    table = Table(title="NEXUS R1 verification")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Evidence")
    for result in results:
        table.add_row(result.name, "PASS" if result.passed else "FAIL", result.detail)
    console.print(table)
    if not all(result.passed for result in results):
        raise typer.Exit(code=1)


@r2_app.command("generate")
def r2_generate(
    output_dir: Annotated[
        Path,
        typer.Option(help="Directory for the deterministic synthetic source package."),
    ] = PROJECT_ROOT / "artifacts" / "r2" / "source",
    seed: Annotated[
        int,
        typer.Option(min=0, help="Seed recorded in the immutable source manifest."),
    ] = 20260718,
) -> None:
    """Generate the bounded synthetic R2A supply network."""

    from nexus.simulation import generate_r2_smoke_fixture

    fixture = generate_r2_smoke_fixture(output_dir, seed=seed)
    table = Table(title="NEXUS R2A source package")
    table.add_column("Control")
    table.add_column("Value")
    table.add_row("Profile", fixture.manifest.profile)
    table.add_row("Seed", str(fixture.manifest.seed))
    table.add_row("Days", str(fixture.manifest.days))
    table.add_row("Source files", str(len(fixture.manifest.sources)))
    table.add_row("Manifest content SHA-256", fixture.manifest_sha256)
    table.add_row("Manifest file SHA-256", fixture.manifest_file_sha256)
    table.add_row("Planning cutoff", fixture.manifest.planning_cutoff.isoformat())
    console.print(table)
    console.print(f"Manifest: {fixture.manifest_path}")


@r2_app.command("rebuild")
def r2_rebuild(
    source_dir: Annotated[
        Path,
        typer.Option(help="Directory containing source-manifest.json and bound sources."),
    ] = PROJECT_ROOT / "artifacts" / "r2" / "source",
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for immutable landing, DuckDB, Gold, and evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r2" / "rebuild",
    planning_cutoff: Annotated[
        str | None,
        typer.Option(help="ISO-8601 cutoff; defaults to the source manifest's planning cutoff."),
    ] = None,
) -> None:
    """Rebuild tested point-in-time Gold products from immutable sources."""

    from datetime import datetime

    from nexus.data import R2SourceManifest
    from nexus.r2 import run_r2_rebuild

    manifest_path = source_dir / "source-manifest.json"
    manifest = R2SourceManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    cutoff = (
        datetime.fromisoformat(planning_cutoff.replace("Z", "+00:00"))
        if planning_cutoff is not None
        else manifest.planning_cutoff
    )
    result = run_r2_rebuild(
        project_root=PROJECT_ROOT,
        source_dir=source_dir,
        artifact_dir=artifact_dir,
        planning_cutoff=cutoff,
    )
    table = Table(title="NEXUS R2A trustworthy-data rebuild")
    table.add_column("Layer")
    table.add_column("Result")
    table.add_column("Evidence")
    table.add_row(
        "1. Source-native landing",
        f"{len(result.landing.source_native_paths)} immutable files",
        result.source_manifest_sha256[:16],
    )
    table.add_row(
        "2. Typed validation",
        f"{len(result.landing.inventory_adjustments)} event revisions",
        f"quarantine={len(result.landing.quarantined)}",
    )
    table.add_row(
        "3. dbt validation",
        "PASS",
        f"Gold products={len(result.gold_artifacts)}",
    )
    table.add_row(
        "4. Point-in-time Gold",
        f"on hand={result.quality_counts['total_on_hand_qty']}",
        f"on order={result.quality_counts['total_on_order_qty']}",
    )
    table.add_row(
        "5. Replay manifest",
        result.semantic_manifest_sha256[:16],
        f"{result.duration_seconds:.2f}s",
    )
    console.print(table)
    console.print(f"Evidence: {result.summary_path}")


@r2_app.command("verify")
def r2_verify(
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for independent R2A gate runs."),
    ] = PROJECT_ROOT / "artifacts" / "r2" / "verification",
) -> None:
    """Execute deterministic generation, rebuild, cutoff, and replay checks."""

    from nexus.r2 import verify_r2

    results = verify_r2(project_root=PROJECT_ROOT, artifact_dir=artifact_dir)
    table = Table(title="NEXUS R2A verification")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Evidence")
    for result in results:
        table.add_row(result.name, "PASS" if result.passed else "FAIL", result.detail)
    console.print(table)
    if not all(result.passed for result in results):
        raise typer.Exit(code=1)


@r2b_app.command("verify")
def r2b_verify(
    project_root: Annotated[
        Path,
        typer.Option(help="Repository root containing the R2A and R2B contracts."),
    ] = PROJECT_ROOT,
    artifact_root: Annotated[
        Path,
        typer.Option(help="Directory for isolated R2B partition-replay and gate evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r2b" / "verification",
    config_path: Annotated[
        Path,
        typer.Option(help="Strict R2B cutoff partition-replay contract YAML."),
    ] = PROJECT_ROOT / "configs" / "orchestration" / "r2b-backfill.yaml",
) -> None:
    """Execute the bounded Dagster retry, parity, replay, and resource gate."""

    from nexus.r2b import verify_r2b

    result = verify_r2b(
        project_root=project_root,
        artifact_root=artifact_root,
        config_path=config_path,
    )
    console.print_json(json.dumps(result.to_summary(), default=str))
    if not result.passed:
        raise typer.Exit(code=1)


def _load_r2c_cli_profile(profile: str) -> R2CProfileConfig:
    from nexus.data.r2c_models import load_r2c_profile

    if profile not in {"smoke", "demo", "full"}:
        raise typer.BadParameter("profile must be one of: smoke, demo, full")
    return load_r2c_profile(PROJECT_ROOT / "configs" / "profiles" / f"{profile}.yaml")


@r2c_app.command("plan")
def r2c_plan(
    profile: Annotated[
        str,
        typer.Option(help="Profile to size without generating data: smoke, demo, or full."),
    ] = "smoke",
) -> None:
    """Show exact R2C cardinality and opt-in requirements without materialization."""

    from nexus.simulation import plan_r2c_profile

    config = _load_r2c_cli_profile(profile)
    plan = plan_r2c_profile(config)
    console.print_json(json.dumps(plan.model_dump(mode="json")))


@r2c_app.command("generate")
def r2c_generate(
    profile: Annotated[
        str,
        typer.Option(help="Profile to generate: smoke, demo, or full."),
    ] = "smoke",
    output_dir: Annotated[
        Path | None,
        typer.Option(help="Package directory; defaults under artifacts/r2c/<profile>/source."),
    ] = None,
    allow_scale: Annotated[
        bool,
        typer.Option(help="Explicitly authorize the protected full scale-plan materialization."),
    ] = False,
) -> None:
    """Generate one governed, partitioned representative synthetic profile."""

    from nexus.simulation import generate_r2c_profile

    config = _load_r2c_cli_profile(profile)
    target = output_dir or PROJECT_ROOT / "artifacts" / "r2c" / profile / "source"
    generated = generate_r2c_profile(target, config=config, allow_scale=allow_scale)
    console.print_json(
        json.dumps(
            {
                "classification": generated.manifest.classification,
                "dataset_semantic_sha256": generated.manifest.dataset_semantic_sha256,
                "demand_rows": generated.manifest.controls.demand_rows,
                "manifest": str(generated.manifest_path),
                "manifest_file_sha256": generated.manifest_file_sha256,
                "profile": generated.manifest.profile,
                "series": generated.manifest.controls.product_location_rows,
                "shards": len(generated.manifest.shards),
            }
        )
    )


@r2c_app.command("verify")
def r2c_verify(
    profile: Annotated[
        str,
        typer.Option(help="Profile gate to execute; smoke is the pull-request default."),
    ] = "smoke",
    artifact_root: Annotated[
        Path | None,
        typer.Option(help="Evidence root; defaults under artifacts/r2c/<profile>/verification."),
    ] = None,
    allow_scale: Annotated[
        bool,
        typer.Option(help="Explicitly authorize a protected full-profile gate."),
    ] = False,
) -> None:
    """Execute the deterministic R2C profile, PIT-label, integrity, and resource gate."""

    from nexus.r2c import verify_r2c

    config = _load_r2c_cli_profile(profile)
    target = artifact_root or PROJECT_ROOT / "artifacts" / "r2c" / profile / "verification"
    result = verify_r2c(artifact_root=target, config=config, allow_scale=allow_scale)
    console.print_json(json.dumps(result.model_dump(mode="json")))
    if not result.passed:
        raise typer.Exit(code=1)


@r2d_app.command("verify")
def r2d_verify(
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for two fresh portable R2D-A1 replays and evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r2d" / "a1-verification",
    config_path: Annotated[
        Path,
        typer.Option(help="Strict portable R2D-A1 ingestion contract YAML."),
    ] = PROJECT_ROOT / "configs" / "ingestion" / "r2d-a1.yaml",
) -> None:
    """Verify portable event capture and parity without claiming service recovery."""

    from nexus.r2d import verify_r2d_a1

    result = verify_r2d_a1(artifact_root=artifact_dir, config_path=config_path)
    receipt = result.receipt
    console.print_json(
        json.dumps(
            {
                "checks": [item.model_dump(mode="json") for item in receipt.checks],
                "full_g2_status": receipt.full_g2_status,
                "full_r2d_a_status": receipt.full_r2d_a_status,
                "operational_recovery_claim": receipt.operational_recovery_claim,
                "passed": result.passed,
                "receipt": str(result.receipt_path),
                "release": receipt.release,
                "runtime": receipt.runtime.model_dump(mode="json"),
                "service_backed_status": receipt.service_backed_status,
                "status": receipt.status,
            }
        )
    )
    if not result.passed:
        raise typer.Exit(code=1)


@r2d_app.command("compare-platforms")
def r2d_compare_platforms(
    left: Annotated[Path, typer.Option(help="Linux portable R2D-A1 receipt JSON.")],
    right: Annotated[Path, typer.Option(help="Windows portable R2D-A1 receipt JSON.")],
) -> None:
    """Exact-join Linux and Windows portable semantics, never service evidence."""

    from nexus.r2d import compare_r2d_a1_receipt_files

    result = compare_r2d_a1_receipt_files(left, right)
    console.print_json(json.dumps(result, default=str))


@r2d_app.command("verify-b1")
def r2d_verify_b1(
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for two portable R2D-B1 replays and evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r2d" / "b1-verification",
    config_path: Annotated[
        Path,
        typer.Option(help="Strict portable R2D-B1 governance contract YAML."),
    ] = PROJECT_ROOT / "configs" / "ingestion" / "r2d-b1.yaml",
) -> None:
    """Verify schema compatibility, poison isolation, and governed redrive."""

    from nexus.r2d_b1 import dump_r2d_b1_result, verify_r2d_b1

    result = verify_r2d_b1(
        artifact_root=artifact_dir,
        config_path=config_path,
        project_root=PROJECT_ROOT,
    )
    console.print_json(dump_r2d_b1_result(result))
    if not result.passed:
        raise typer.Exit(code=1)


@r2d_app.command("compare-b1-platforms")
def r2d_compare_b1_platforms(
    left: Annotated[Path, typer.Option(help="Attested Linux R2D-B1 receipt JSON.")],
    right: Annotated[Path, typer.Option(help="Attested Windows R2D-B1 receipt JSON.")],
) -> None:
    """Exact-join Linux and Windows portable B1 governance evidence."""

    from nexus.r2d_b1 import compare_r2d_b1_receipts

    console.print_json(json.dumps(compare_r2d_b1_receipts(left, right), default=str))


@r2d_app.command("verify-service-b2a")
def r2d_verify_service_b2a(
    bootstrap_servers: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_B2_BOOTSTRAP_SERVERS",
            help="Kafka-compatible bootstrap endpoint supplied by the isolated B2a gate.",
        ),
    ],
    service_image_evidence: Annotated[
        Path,
        typer.Option(help="Canonical observed Redpanda image identity from the running container."),
    ],
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Fresh directory for live DLQ and redrive evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r2d" / "b2a-verification",
    config_path: Annotated[
        Path,
        typer.Option(help="Strict R2D-B2a service governance contract YAML."),
    ] = PROJECT_ROOT / "configs" / "ingestion" / "r2d-b2a.yaml",
) -> None:
    """Verify live poison routing and one append-only manual redrive."""

    from nexus.r2d_b2a import dump_r2d_b2a_result, verify_r2d_b2a

    result = verify_r2d_b2a(
        artifact_root=artifact_dir,
        bootstrap_servers=bootstrap_servers,
        service_image_evidence_path=service_image_evidence,
        config_path=config_path,
        project_root=PROJECT_ROOT,
    )
    console.print_json(dump_r2d_b2a_result(result))
    if not result.passed:
        raise typer.Exit(code=1)


@r2d_app.command("verify-service-b2b")
def r2d_verify_service_b2b(
    postgres_dsn: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_B2B_POSTGRES_DSN",
            help="PostgreSQL DSN supplied by the isolated B2b gate; never persisted.",
            show_default=False,
        ),
    ],
    service_image_evidence: Annotated[
        Path,
        typer.Option(
            help="Canonical observed PostgreSQL image identity from the running container."
        ),
    ],
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Fresh directory for persistent Dagster and restart evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r2d" / "b2b-verification",
    config_path: Annotated[
        Path,
        typer.Option(help="Strict R2D-B2b persistent orchestration contract YAML."),
    ] = PROJECT_ROOT / "configs" / "orchestration" / "r2d-b2b.yaml",
) -> None:
    """Verify a PostgreSQL-backed backfill across two Dagster daemon epochs."""

    from nexus.r2d_b2b import dump_r2d_b2b_result, verify_r2d_b2b

    result = verify_r2d_b2b(
        artifact_root=artifact_dir,
        postgres_dsn=postgres_dsn,
        service_image_evidence_path=service_image_evidence,
        config_path=config_path,
        project_root=PROJECT_ROOT,
    )
    console.print_json(dump_r2d_b2b_result(result))
    if not result.passed:
        raise typer.Exit(code=1)


@r2d_app.command("capture-service-b2c-before")
def r2d_capture_service_b2c_before(
    bootstrap_servers: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_B2_BOOTSTRAP_SERVERS",
            help="Kafka-compatible endpoint for the isolated B2c gate.",
        ),
    ],
    postgres_dsn: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_B2B_POSTGRES_DSN",
            help="PostgreSQL DSN for the isolated B2c gate; never persisted.",
            show_default=False,
        ),
    ],
    dagster_home: Annotated[
        Path,
        typer.Option(help="DAGSTER_HOME created by the inherited B2b verifier."),
    ],
    b2a_receipt: Annotated[Path, typer.Option(help="Complete current B2a receipt JSON.")],
    b2b_receipt: Annotated[Path, typer.Option(help="Complete current B2b receipt JSON.")],
    output: Annotated[
        Path,
        typer.Option(help="Fresh canonical pre-restart checkpoint JSON path."),
    ],
    config_path: Annotated[
        Path,
        typer.Option(help="Strict R2D-B2c retained-restart contract YAML."),
    ] = PROJECT_ROOT / "configs" / "operations" / "r2d-b2c-retained-restart.yaml",
) -> None:
    """Capture the exact Redpanda and Dagster state before service stop."""

    from nexus.r2d_b2c import capture_r2d_b2c_pre_restart

    checkpoint = capture_r2d_b2c_pre_restart(
        output_path=output,
        bootstrap_servers=bootstrap_servers,
        postgres_dsn=postgres_dsn,
        dagster_home=dagster_home,
        b2a_receipt_path=b2a_receipt,
        b2b_receipt_path=b2b_receipt,
        config_path=config_path,
    )
    console.print_json(json.dumps(checkpoint.model_dump(mode="json"), sort_keys=True))


@r2d_app.command("cycle-service-b2c")
def r2d_cycle_service_b2c(
    compose_project_name: Annotated[
        str,
        typer.Option(
            envvar="COMPOSE_PROJECT_NAME",
            help="Isolated nexus-r2d-b2c-* Compose project to stop and start.",
        ),
    ],
    output: Annotated[
        Path,
        typer.Option(help="Fresh canonical lifecycle evidence JSON path."),
    ],
    config_path: Annotated[
        Path,
        typer.Option(help="Strict R2D-B2c retained-restart contract YAML."),
    ] = PROJECT_ROOT / "configs" / "operations" / "r2d-b2c-retained-restart.yaml",
) -> None:
    """Stop and restart Redpanda/PostgreSQL while retaining named volumes."""

    from nexus.r2d_b2c import cycle_r2d_b2c_services

    lifecycle = cycle_r2d_b2c_services(
        output_path=output,
        compose_project_name=compose_project_name,
        config_path=config_path,
        project_root=PROJECT_ROOT,
    )
    console.print_json(json.dumps(lifecycle.model_dump(mode="json"), sort_keys=True))


@r2d_app.command("verify-service-b2c")
def r2d_verify_service_b2c(
    bootstrap_servers: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_B2_BOOTSTRAP_SERVERS",
            help="Kafka-compatible endpoint for the restarted B2c gate.",
        ),
    ],
    postgres_dsn: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_B2B_POSTGRES_DSN",
            help="PostgreSQL DSN for the restarted B2c gate; never persisted.",
            show_default=False,
        ),
    ],
    dagster_home: Annotated[
        Path,
        typer.Option(help="DAGSTER_HOME created by the inherited B2b verifier."),
    ],
    b2a_receipt: Annotated[Path, typer.Option(help="Complete current B2a receipt JSON.")],
    b2b_receipt: Annotated[Path, typer.Option(help="Complete current B2b receipt JSON.")],
    pre_restart_checkpoint: Annotated[
        Path, typer.Option(help="Canonical checkpoint captured before the service stop.")
    ],
    lifecycle_evidence: Annotated[
        Path, typer.Option(help="Canonical Docker stop/start lifecycle evidence.")
    ],
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Fresh directory for the complete retained-restart package."),
    ] = PROJECT_ROOT / "artifacts" / "r2d" / "b2c-verification",
    config_path: Annotated[
        Path,
        typer.Option(help="Strict R2D-B2c retained-restart contract YAML."),
    ] = PROJECT_ROOT / "configs" / "operations" / "r2d-b2c-retained-restart.yaml",
) -> None:
    """Verify exact retained state and fresh reconnect I/O after service restart."""

    from nexus.r2d_b2c import dump_r2d_b2c_result, verify_r2d_b2c

    result = verify_r2d_b2c(
        artifact_root=artifact_dir,
        bootstrap_servers=bootstrap_servers,
        postgres_dsn=postgres_dsn,
        dagster_home=dagster_home,
        b2a_receipt_path=b2a_receipt,
        b2b_receipt_path=b2b_receipt,
        pre_restart_checkpoint_path=pre_restart_checkpoint,
        lifecycle_evidence_path=lifecycle_evidence,
        config_path=config_path,
        project_root=PROJECT_ROOT,
    )
    console.print_json(dump_r2d_b2c_result(result))
    if not result.passed:
        raise typer.Exit(code=1)


@r2d_app.command("verify-service-a2a")
def r2d_verify_service_a2a(
    bootstrap_servers: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_A2_BOOTSTRAP_SERVERS",
            help="Kafka-compatible bootstrap endpoint; supplied by the isolated gate.",
        ),
    ],
    postgres_dsn: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_A2_POSTGRES_DSN",
            help="PostgreSQL DSN from the environment; never written to evidence.",
            show_default=False,
        ),
    ],
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Fresh directory for the content-addressed A2a service receipt."),
    ] = PROJECT_ROOT / "artifacts" / "r2d" / "a2a-verification",
) -> None:
    """Verify real Redpanda-to-PostgreSQL capture without claiming crash recovery."""

    from nexus.r2d_a2a import verify_r2d_a2a

    result = verify_r2d_a2a(
        project_root=PROJECT_ROOT,
        artifact_root=artifact_dir,
        bootstrap_servers=bootstrap_servers,
        postgres_dsn=postgres_dsn,
    )
    receipt = result.receipt
    console.print_json(
        json.dumps(
            {
                "checks": [item.model_dump(mode="json") for item in receipt.checks],
                "crash_recovery_status": receipt.crash_recovery_status,
                "downstream_parity_status": receipt.downstream_parity_status,
                "full_g2_status": receipt.full_g2_status,
                "full_r2d_a_status": receipt.full_r2d_a_status,
                "r2d_a2_status": receipt.r2d_a2_status,
                "receipt": str(result.receipt_artifact.path),
                "service_foundation_status": receipt.service_foundation_status,
                "status": receipt.status,
            }
        )
    )


@r2d_app.command("verify-service-a2b1")
def r2d_verify_service_a2b1(
    bootstrap_servers: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_A2_BOOTSTRAP_SERVERS",
            help="Kafka-compatible bootstrap endpoint; supplied by the isolated gate.",
        ),
    ],
    postgres_dsn: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_A2_POSTGRES_DSN",
            help="PostgreSQL DSN from the environment; never written to evidence.",
            show_default=False,
        ),
    ],
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Fresh directory for the A2b-1 hard-crash receipt."),
    ] = PROJECT_ROOT / "artifacts" / "r2d" / "a2b1-verification",
) -> None:
    """Verify both process-crash windows and restart offset reconciliation."""

    from nexus.r2d_a2b import verify_r2d_a2b1

    result = verify_r2d_a2b1(
        project_root=PROJECT_ROOT,
        artifact_root=artifact_dir,
        bootstrap_servers=bootstrap_servers,
        postgres_dsn=postgres_dsn,
    )
    receipt = result.receipt
    console.print_json(
        json.dumps(
            {
                "checks": [item.model_dump(mode="json") for item in receipt.checks],
                "crash_recovery_status": receipt.crash_recovery_status,
                "downstream_parity_status": receipt.downstream_parity_status,
                "full_g2_status": receipt.full_g2_status,
                "full_r2d_a_status": receipt.full_r2d_a_status,
                "r2d_a2_status": receipt.r2d_a2_status,
                "receipt": str(result.receipt_artifact.path),
                "status": receipt.status,
                "worker_exit_codes": list(receipt.worker_exit_codes),
            }
        )
    )


@r2d_app.command("verify-service-a2b2")
def r2d_verify_service_a2b2(
    bootstrap_servers: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_A2_BOOTSTRAP_SERVERS",
            help="Kafka-compatible bootstrap endpoint; supplied by the isolated gate.",
        ),
    ],
    postgres_dsn: Annotated[
        str,
        typer.Option(
            envvar="NEXUS_R2D_A2_POSTGRES_DSN",
            help="PostgreSQL DSN from the environment; never written to evidence.",
            show_default=False,
        ),
    ],
    a2b1_receipt: Annotated[
        Path,
        typer.Option(help="Content-addressed passing A2b-1 receipt from the same commit."),
    ],
    service_image_evidence: Annotated[
        Path,
        typer.Option(help="Canonical observed IDs for the running PostgreSQL and Redpanda images."),
    ],
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Fresh directory for final A2 parity and recovery evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r2d" / "a2-verification",
) -> None:
    """Verify retained PostgreSQL rebuild, R2A parity, and final bounded R2D-A2."""

    from nexus.r2d_a2b2 import dump_a2_result, verify_r2d_a2b2

    result = verify_r2d_a2b2(
        project_root=PROJECT_ROOT,
        artifact_root=artifact_dir,
        bootstrap_servers=bootstrap_servers,
        postgres_dsn=postgres_dsn,
        a2b1_receipt_path=a2b1_receipt,
        service_image_evidence_path=service_image_evidence,
    )
    console.print_json(dump_a2_result(result))


@r3_app.command("demo")
def r3_demo(
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for registered R3A scientific evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r3" / "demo",
    config_path: Annotated[
        Path | None,
        typer.Option(help="Optional strict R3A modeling-contract YAML."),
    ] = None,
) -> None:
    """Run the bounded point-in-time probabilistic-intelligence baseline."""

    from nexus.r3 import run_r3a_demo

    result = run_r3a_demo(
        project_root=PROJECT_ROOT,
        artifact_dir=artifact_dir,
        config_path=config_path,
    )
    table = Table(title="NEXUS R3A probabilistic intelligence")
    table.add_column("Boundary")
    table.add_column("Result")
    table.add_column("Evidence")
    table.add_row(
        "1. Registered R2 Gold snapshot",
        f"{result.snapshot.contract.source_row_count} demand rows",
        result.snapshot.contract.registration_sha256[:16],
    )
    table.add_row(
        "2. Point-in-time features",
        f"{len(result.feature_rows)} feature rows",
        result.config.feature_contract.version,
    )
    table.add_row(
        "3. Probabilistic baseline",
        f"{len(result.forecasts)} series-origin forecasts",
        f"matured points={len(result.metric_rows)}",
    )
    table.add_row(
        "4. Rolling-origin evaluation",
        f"WIS={result.overall_metrics.weighted_interval_score}",
        f"stress WIS={result.stress_metrics.weighted_interval_score}",
    )
    table.add_row(
        "5. Bottom-up hierarchy",
        f"{len(result.hierarchical_forecasts)} quantile rows",
        f"actual rows={len(result.hierarchical_actuals)}",
    )
    table.add_row(
        "6. Fail-closed governance",
        "MATERIALIZED_UNVERIFIED",
        (
            f"automatic promotion={result.config.promotion.automatic}; "
            f"{result.semantic_manifest.content_sha256[:16]}"
        ),
    )
    table.add_row(
        "7. Bounded execution",
        f"{result.duration_seconds:.2f}s / {result.peak_rss_mib:.1f} MiB",
        f"artifacts={result.artifact_bytes} bytes",
    )
    console.print(table)
    console.print(f"Evidence: {result.summary_path}")


@r3_app.command("verify")
def r3_verify(
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for independent R3A gate runs."),
    ] = PROJECT_ROOT / "artifacts" / "r3" / "verification",
) -> None:
    """Execute registered-snapshot, leakage, evaluation, replay, and budget checks."""

    from nexus.r3 import verify_r3

    results = verify_r3(project_root=PROJECT_ROOT, artifact_dir=artifact_dir)
    table = Table(title="NEXUS R3A verification")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Evidence")
    for result in results:
        table.add_row(result.name, "PASS" if result.passed else "FAIL", result.detail)
    console.print(table)
    console.print(f"Evidence: {artifact_dir / 'r3a-verification-summary.json'}")
    if not all(result.passed for result in results):
        raise typer.Exit(code=1)


@r3b_app.command("run")
def r3b_run(
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for one materialized R3B scientific run."),
    ] = PROJECT_ROOT / "artifacts" / "r3b" / "run",
    config_path: Annotated[
        Path,
        typer.Option(help="Strict R3B learned-intelligence contract YAML."),
    ] = PROJECT_ROOT / "configs" / "modeling" / "r3b-smoke.yaml",
) -> None:
    """Train, evaluate, track, and materialize one unverified R3B run."""

    from nexus.r3b import run_r3b_once

    result = run_r3b_once(output_dir=artifact_dir, config_path=config_path)
    console.print_json(
        json.dumps(
            {
                "classification": "synthetic",
                "demand_baseline_wis": str(result.final_evaluation.baseline_wis),
                "demand_candidate_wis": str(result.final_evaluation.candidate_wis),
                "lead_time_baseline_wis": str(result.lead_time_evaluation.baseline_wis),
                "lead_time_candidate_wis": str(result.lead_time_evaluation.candidate_wis),
                "mlflow_run_id": result.tracking.run_id,
                "promotion_applied": result.promotion.promotion_applied,
                "promotion_status": result.promotion.status,
                "status": "MATERIALIZED_UNVERIFIED",
                "summary": str(result.summary_path),
            }
        )
    )


@r3b_app.command("verify")
def r3b_verify(
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for two fresh R3B replays and gate evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r3b" / "verification",
    config_path: Annotated[
        Path,
        typer.Option(help="Strict R3B learned-intelligence contract YAML."),
    ] = PROJECT_ROOT / "configs" / "modeling" / "r3b-smoke.yaml",
) -> None:
    """Execute the deterministic model, MLflow, replay, and resource gate."""

    from nexus.r3b import verify_r3b

    result = verify_r3b(artifact_root=artifact_dir, config_path=config_path)
    console.print_json(
        json.dumps(
            {
                "checks": [
                    {"name": item.name, "passed": item.passed, "detail": item.detail}
                    for item in result.checks
                ],
                "classification": "synthetic",
                "full_g3_status": "OPEN",
                "passed": result.passed,
                "platform_receipt": (
                    None
                    if result.platform_receipt_path is None
                    else str(result.platform_receipt_path)
                ),
                "promotion_status": (
                    None if result.final_promotion is None else result.final_promotion.status
                ),
                "status": result.status,
                "summary": str(result.summary_path),
            }
        )
    )
    if not result.passed:
        raise typer.Exit(code=1)


@r3b_app.command("compare-platforms")
def r3b_compare_platforms(
    left: Annotated[Path, typer.Option(help="First normalized platform receipt JSON.")],
    right: Annotated[Path, typer.Option(help="Second normalized platform receipt JSON.")],
    config_path: Annotated[
        Path,
        typer.Option(help="Strict R3B numeric-tolerance contract YAML."),
    ] = PROJECT_ROOT / "configs" / "modeling" / "r3b-smoke.yaml",
) -> None:
    """Join Linux and Windows evidence with strict semantic and tolerant numeric checks."""

    from nexus.r3b import compare_platform_receipt_files

    result = compare_platform_receipt_files(left, right, config_path=config_path)
    console.print_json(json.dumps(result, default=str))


@r4a_app.command("run")
def r4a_run(
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for one immutable R4A decision run."),
    ] = PROJECT_ROOT / "artifacts" / "r4a" / "run",
    config_path: Annotated[
        Path,
        typer.Option(help="Strict bounded R4A decisioning contract YAML."),
    ] = PROJECT_ROOT / "configs" / "decisioning" / "r4a-smoke.yaml",
) -> None:
    """Simulate, optimize, validate, and submit only for human approval."""

    from nexus.r4a import run_r4a_once

    result = run_r4a_once(output_dir=artifact_dir, config_path=config_path)
    console.print_json(
        json.dumps(
            {
                "classification": result.config.classification,
                "evaluation_claim": (
                    None if result.evaluation is None else result.evaluation.claim
                ),
                "full_g4_status": result.gate.full_g4_status,
                "independently_feasible": result.optimizer.feasibility.independently_feasible,
                "lifecycle_state": (
                    None if result.lifecycle_state is None else result.lifecycle_state.value
                ),
                "manifest_created": result.manifest is not None,
                "raw_solver_status": result.optimizer.raw_solver_status,
                "review_disposition": result.optimizer.review_disposition,
                "semantic_sha256": result.semantic_sha256,
                "status": result.gate.status,
            }
        )
    )


@r4a_app.command("verify")
def r4a_verify(
    artifact_dir: Annotated[
        Path,
        typer.Option(help="Directory for independent R4A replay and fail-closed evidence."),
    ] = PROJECT_ROOT / "artifacts" / "r4a" / "verification",
    config_path: Annotated[
        Path,
        typer.Option(help="Strict bounded R4A decisioning contract YAML."),
    ] = PROJECT_ROOT / "configs" / "decisioning" / "r4a-smoke.yaml",
) -> None:
    """Execute the deterministic optimization, fallback, approval, and replay gate."""

    from nexus.r4a import verify_r4a

    result = verify_r4a(artifact_root=artifact_dir, config_path=config_path)
    console.print_json(
        json.dumps(
            {
                "checks": [item.model_dump(mode="json") for item in result.receipt.checks],
                "full_g4_status": result.receipt.full_g4_status,
                "passed": result.passed,
                "runtime": result.receipt.runtime.model_dump(mode="json"),
                "status": result.receipt.status,
                "summary": str(result.artifact.path),
                "superiority_claim_allowed": result.receipt.superiority_claim_allowed,
            }
        )
    )
    if not result.passed:
        raise typer.Exit(code=1)


@r4a_app.command("compare-platforms")
def r4a_compare_platforms(
    left: Annotated[Path, typer.Option(help="First normalized R4A verification receipt JSON.")],
    right: Annotated[Path, typer.Option(help="Second normalized R4A verification receipt JSON.")],
) -> None:
    """Join Linux and Windows R4A evidence by exact semantic receipt equality."""

    from nexus.r4a import compare_r4a_verification_receipt_files

    result = compare_r4a_verification_receipt_files(left, right)
    console.print_json(json.dumps(result))


if __name__ == "__main__":
    app()
