"""Executable R2A trustworthy-data generation, rebuild, and replay gate."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import time as clock
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from nexus.data.r2_dbt_execution import current_dbt_executor
from nexus.data.r2_models import (
    R2LandingResult,
    R2RebuildResult,
    R2SourceFile,
    R2SourceManifest,
    normalize_r2_utc,
)
from nexus.data.r2_pipeline import land_r2_sources
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import (
    ContentAddressedArtifact,
    canonical_rows_sha256,
    file_sha256,
    write_immutable_parquet,
)

_EXPECTED_SOURCE_MANIFEST_SHA256 = (
    "03e32a3f197839134e44b8f6d384787a99ec22e2c088ee4edea8d8322814346b"
)
_EXPECTED_SOURCE_MANIFEST_FILE_SHA256 = (
    "aa25517d49cb6c36b76e9b6a218a83e7419359f5a7c40626412797ca13f8d768"
)
_EXPECTED_FINAL_GOLD_HASHES = {
    "gold.r2_demand_history": ("416699ec7a75bdab6bc9e4c5d7ff8b5d95d5a6225e102d4bcb3e3505b935379a"),
    "gold.r2_fulfillment_outcomes": (
        "1a0dfd9c52c9c88052483c7c72270249b81b0a354c3ca243c60a6dc3c1ff8717"
    ),
    "gold.r2_inventory_position": (
        "0f43999ef57db66ab185cf813847c7b4b1fcd1e3734a3a583b961aa611bf25bc"
    ),
    "gold.r2_open_orders": ("3c2905b27bba7f1d20d30a7c832a057f0a64522f7bbd092e5b3d4ea8569845de"),
    "gold.r2_supplier_performance": (
        "0989787e4b389fd78b581ef112d5c26af804973f49adc179fd8a9cac1f691219"
    ),
}
_EXPECTED_EARLY_GOLD_HASHES = {
    "gold.r2_demand_history": ("871467f42ccb5dae2f828daada1fd5d52e73ff10e5a701f9b79aea39422c1fe3"),
    "gold.r2_fulfillment_outcomes": (
        "2cefa7a87cbc8a9b15c2ee6b29136956db1c1966ebc95e1681c9d6f0a69e6659"
    ),
    "gold.r2_inventory_position": (
        "ed6d491f9e55d66115db6c9a301c31a08fe2d444e862be714cefc3652114a100"
    ),
    "gold.r2_open_orders": ("4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"),
    "gold.r2_supplier_performance": (
        "aeeaf9dd9a7c0cc63c098fb7a9c0bb5fb3025743c2f0fc09ad9237647c5d0728"
    ),
}
_GOLD_TABLES: dict[str, tuple[str, tuple[str, ...]]] = {
    "gold.r2_demand_history": (
        "gold_r2_demand_history",
        ("demand_date", "sku_location_key"),
    ),
    "gold.r2_fulfillment_outcomes": (
        "gold_r2_fulfillment_outcomes",
        ("fulfillment_id",),
    ),
    "gold.r2_inventory_position": (
        "gold_r2_inventory_position",
        ("sku_location_key",),
    ),
    "gold.r2_open_orders": ("gold_r2_open_orders", ("order_id",)),
    "gold.r2_supplier_performance": (
        "gold_r2_supplier_performance",
        ("supplier_id",),
    ),
}


class R2DbtBuildError(RuntimeError):
    """dbt could not produce a tested Gold snapshot."""


class R2ExpectedControlError(RuntimeError):
    """Observed R2A controls disagree with the bound source manifest."""


@dataclass(frozen=True, slots=True)
class R2VerificationResult:
    """One independently readable R2A gate result."""

    name: str
    passed: bool
    detail: str


def _iso(value: datetime) -> str:
    return normalize_r2_utc(value).isoformat().replace("+00:00", "Z")


def _json_value(value: object) -> object:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            value = value.replace(tzinfo=UTC)
        return _iso(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _dbt_code_sha256(project_root: Path) -> str:
    files = [
        project_root / "dbt" / "dbt_project.yml",
        project_root / "dbt" / "profiles.yml",
        *project_root.glob("dbt/models/r2/**/*"),
        *project_root.glob("dbt/tests/r2_*.sql"),
        *project_root.glob("dbt/tests/generic/r2_*.sql"),
    ]
    entries = [
        {
            "path": path.relative_to(project_root).as_posix(),
            "sha256": _normalized_text_sha256(path),
        }
        for path in sorted(files)
        if path.is_file()
    ]
    return canonical_rows_sha256(entries)


def _pipeline_code_sha256(project_root: Path) -> str:
    files = [
        project_root / "src" / "nexus" / "r2.py",
        project_root / "src" / "nexus" / "data" / "r2_models.py",
        project_root / "src" / "nexus" / "data" / "r2_pipeline.py",
        project_root / "src" / "nexus" / "data" / "r2_dbt_execution.py",
        project_root / "src" / "nexus" / "contracts" / "models.py",
        project_root / "src" / "nexus" / "simulation" / "supply_network.py",
        project_root / "src" / "nexus" / "storage" / "artifacts.py",
        project_root / "pyproject.toml",
        project_root / "uv.lock",
        project_root / "contracts" / "schemas" / "event-envelope.schema.json",
        project_root / "contracts" / "schemas" / "r2-source-manifest.schema.json",
        project_root / "contracts" / "schemas" / "r2-inventory-adjustment-payload.schema.json",
    ]
    entries = [
        {
            "path": path.relative_to(project_root).as_posix(),
            "sha256": _normalized_text_sha256(path),
        }
        for path in files
    ]
    return canonical_rows_sha256(entries)


def r2_dbt_code_sha256(project_root: Path) -> str:
    """Return the authoritative normalized identity of the public R2A dbt project."""

    return _dbt_code_sha256(project_root.resolve())


def r2_pipeline_code_sha256(project_root: Path) -> str:
    """Return the authoritative normalized identity of the public R2A Python pipeline."""

    return _pipeline_code_sha256(project_root.resolve())


def _normalized_text_sha256(path: Path) -> str:
    normalized = path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return hashlib.sha256(normalized).hexdigest()


def _run_dbt(
    *,
    project_root: Path,
    validated_files: Mapping[str, Path],
    database_path: Path,
    planning_cutoff: datetime,
) -> str:
    dbt = shutil.which("dbt")
    if dbt is None:
        raise R2DbtBuildError("dbt executable not found; run uv sync --locked --all-groups")
    database_path = database_path.resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    # Each attempt already owns its database. Keep compiled SQL, partial-parse
    # caches, run results and logs beside that database, never in shared dbt/target.
    dbt_artifacts = database_path.with_suffix(".dbt")
    environment = os.environ.copy()
    environment["NEXUS_DBT_PATH"] = str(database_path)
    environment["TZ"] = "UTC"
    validated_sql_files = {
        dataset: path.resolve().as_posix().replace("'", "''")
        for dataset, path in validated_files.items()
    }
    variables = json.dumps(
        {
            "enable_r2": True,
            "r2_planning_cutoff": planning_cutoff.isoformat(),
            "r2_validated_files": validated_sql_files,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    command = [
        dbt,
        "build",
        "--project-dir",
        str(project_root / "dbt"),
        "--profiles-dir",
        str(project_root / "dbt"),
        "--target",
        "ci",
        "--target-path",
        str(dbt_artifacts / "target"),
        "--log-path",
        str(dbt_artifacts / "logs"),
        "--select",
        "tag:r2",
        "--vars",
        variables,
    ]
    executor = current_dbt_executor()
    if executor is not None:
        try:
            return executor(command[1:], environment)
        except Exception:
            raise R2DbtBuildError("Owned planning dbt invocation failed") from None
    completed = subprocess.run(
        command,
        cwd=project_root,
        env=environment,
        # dbt is non-interactive. Never inherit a managed worker's STOP/EOF pipe:
        # child startup must not depend on the supervisor closing its control input.
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        text=True,
        timeout=180,
    )
    output = (completed.stdout or completed.stderr).strip()
    if completed.returncode != 0:
        tail = "\n".join(output.splitlines()[-30:])
        raise R2DbtBuildError(f"R2A dbt build failed:\n{tail}")
    for line in reversed(output.splitlines()):
        if "Done. PASS=" in line:
            return "Done. " + line.split("Done. ", maxsplit=1)[1]
    return "dbt build completed"


def _table_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table: str,
    order_by: Sequence[str],
) -> tuple[list[dict[str, object]], pa.Schema]:
    order_clause = ", ".join(order_by)
    arrow_table = connection.execute(
        f"select * from {table} order by {order_clause}"
    ).to_arrow_table()
    rows = [
        {key: _json_value(value) for key, value in row.items()} for row in arrow_table.to_pylist()
    ]
    json_schema = pa.schema(
        [
            pa.field(
                field.name,
                (
                    pa.string()
                    if (
                        pa.types.is_date(field.type)
                        or pa.types.is_decimal(field.type)
                        or pa.types.is_duration(field.type)
                        or pa.types.is_time(field.type)
                        or pa.types.is_timestamp(field.type)
                    )
                    else field.type
                ),
                nullable=field.nullable,
            )
            for field in arrow_table.schema
        ]
    )
    return rows, json_schema


def _scalar_int(connection: duckdb.DuckDBPyConnection, query: str) -> int:
    row = connection.execute(query).fetchone()
    if row is None:
        raise RuntimeError(f"quality query returned no row: {query}")
    return int(row[0])


def _write_exact_json(path: Path, value: object) -> None:
    text = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise RuntimeError(f"immutable build manifest conflict: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _require_expected_controls(
    *,
    expected: Mapping[str, int],
    observed: Mapping[str, int],
) -> None:
    mismatches = {
        name: {"expected": expected[name], "observed": observed[name]}
        for name in observed
        if observed[name] != expected[name]
    }
    if mismatches:
        raise R2ExpectedControlError(
            "source-manifest expected controls do not match observed data: "
            + json.dumps(mismatches, sort_keys=True)
        )


def materialize_r2_validation_checkpoint(
    *,
    source_dir: Path,
    artifact_dir: Path,
) -> R2LandingResult:
    """Land and validate source bytes exactly once for downstream checkpoint reuse."""

    landing = land_r2_sources(
        source_dir=source_dir.resolve(),
        output_dir=artifact_dir.resolve() / "lake",
        namespace_by_manifest=True,
    )
    expected_controls = landing.source_manifest.expected_controls.model_dump()
    _require_expected_controls(
        expected=expected_controls,
        observed={
            "accepted_inventory_adjustments": len(landing.inventory_adjustments),
            "fulfillment_rows": len(landing.fulfillment_outcomes),
            "opening_inventory_rows": len(landing.opening_inventory),
            "product_location_rows": len(landing.product_locations),
            "purchase_order_rows": len(landing.purchase_orders),
            "quarantine_rows": len(landing.quarantined),
            "receipt_rows": len(landing.receipts),
            "supplier_rows": len(landing.suppliers),
        },
    )
    return landing


def run_r2_rebuild(
    *,
    project_root: Path,
    source_dir: Path,
    artifact_dir: Path,
    planning_cutoff: datetime,
) -> R2RebuildResult:
    """Land, validate, transform, test, and export one point-in-time snapshot."""

    started = clock.perf_counter()
    landing = materialize_r2_validation_checkpoint(
        source_dir=source_dir,
        artifact_dir=artifact_dir,
    )
    return _run_r2_gold_from_checkpoint(
        project_root=project_root,
        artifact_dir=artifact_dir,
        planning_cutoff=planning_cutoff,
        checkpoint=landing,
        started=started,
    )


def run_r2_gold_from_checkpoint(
    *,
    project_root: Path,
    artifact_dir: Path,
    planning_cutoff: datetime,
    checkpoint: R2LandingResult,
) -> R2RebuildResult:
    """Transform one already-validated checkpoint without reparsing its source package."""

    return _run_r2_gold_from_checkpoint(
        project_root=project_root,
        artifact_dir=artifact_dir,
        planning_cutoff=planning_cutoff,
        checkpoint=checkpoint,
        started=clock.perf_counter(),
    )


def _run_r2_gold_from_checkpoint(
    *,
    project_root: Path,
    artifact_dir: Path,
    planning_cutoff: datetime,
    checkpoint: R2LandingResult,
    started: float,
) -> R2RebuildResult:
    project_root = project_root.resolve()
    artifact_dir = artifact_dir.resolve()
    planning_cutoff = normalize_r2_utc(planning_cutoff)
    landing = checkpoint
    expected_controls = landing.source_manifest.expected_controls.model_dump()
    validated_files = {
        artifact.dataset.removeprefix("validated."): artifact.path
        for artifact in landing.validated_artifacts
        if artifact.dataset.startswith("validated.")
    }
    pipeline_code_sha256 = _pipeline_code_sha256(project_root)
    dbt_code_sha256 = _dbt_code_sha256(project_root)
    build_input_sha256 = canonical_rows_sha256(
        [
            {
                "dbt_code_sha256": dbt_code_sha256,
                "pipeline_code_sha256": pipeline_code_sha256,
                "planning_cutoff": _iso(planning_cutoff),
                "source_manifest_file_sha256": landing.source_manifest_file_sha256,
            }
        ]
    )
    database_path = (
        artifact_dir
        / "warehouse"
        / f"{planning_cutoff:%Y%m%dT%H%M%SZ}-{build_input_sha256[:16]}.duckdb"
    )
    dbt_detail = _run_dbt(
        project_root=project_root,
        validated_files=validated_files,
        database_path=database_path,
        planning_cutoff=planning_cutoff,
    )

    exported: list[ContentAddressedArtifact] = []
    gold_row_counts: dict[str, int] = {}
    with duckdb.connect(str(database_path), read_only=True) as connection:
        for dataset, (table, order_by) in _GOLD_TABLES.items():
            rows, schema = _table_rows(connection, table=table, order_by=order_by)
            artifact = write_immutable_parquet(
                artifact_dir / "exports",
                dataset=dataset,
                rows=rows,
                schema=schema,
            )
            exported.append(artifact)
            gold_row_counts[dataset] = len(rows)

        effective_count = _scalar_int(
            connection,
            "select count(*) from int_r2_effective_inventory_adjustments",
        )
        deferred_count = _scalar_int(
            connection,
            "select count(*) from int_r2_deferred_inventory_adjustments",
        )
        chain_deferred_count = _scalar_int(
            connection,
            "select count(*) from int_r2_deferred_inventory_adjustments "
            "where deferred_reason = 'incomplete_visible_revision_chain'",
        )
        superseded_count = _scalar_int(
            connection,
            "select count(*) from int_r2_superseded_inventory_adjustments",
        )
        total_demand = _scalar_int(
            connection,
            "select coalesce(sum(demand_qty), 0) from gold_r2_demand_history",
        )
        total_on_hand = _scalar_int(
            connection,
            "select coalesce(sum(on_hand_qty), 0) from gold_r2_inventory_position",
        )
        total_on_order = _scalar_int(
            connection,
            "select coalesce(sum(open_qty), 0) from gold_r2_open_orders",
        )
        latest_fulfillment_available_at = connection.execute(
            "select max(available_at) from stg_r2_fulfillment_outcomes"
        ).fetchone()
        latest_adjustment_ingested_at = connection.execute(
            "select max(ingested_at) from stg_r2_inventory_adjustments "
            f"where occurred_at <= timestamptz '{planning_cutoff.isoformat()}' "
            f"and ingested_at <= timestamptz '{planning_cutoff.isoformat()}'"
        ).fetchone()

    fulfillment_watermark = (
        cast(datetime, latest_fulfillment_available_at[0])
        if latest_fulfillment_available_at is not None
        and latest_fulfillment_available_at[0] is not None
        else None
    )
    adjustment_watermark = (
        cast(datetime, latest_adjustment_ingested_at[0])
        if latest_adjustment_ingested_at is not None
        and latest_adjustment_ingested_at[0] is not None
        else None
    )
    if fulfillment_watermark is not None and fulfillment_watermark.tzinfo is None:
        fulfillment_watermark = fulfillment_watermark.replace(tzinfo=UTC)
    if adjustment_watermark is not None and adjustment_watermark.tzinfo is None:
        adjustment_watermark = adjustment_watermark.replace(tzinfo=UTC)

    quality_counts: dict[str, int | None] = {
        "chain_deferred_inventory_adjustments": chain_deferred_count,
        "dbt_gold_products": len(_GOLD_TABLES),
        "deferred_inventory_adjustments": deferred_count,
        "effective_inventory_adjustments": effective_count,
        "fulfillment_freshness_lag_seconds": (
            int((planning_cutoff - fulfillment_watermark).total_seconds())
            if fulfillment_watermark is not None
            else None
        ),
        "quarantined_records": len(landing.quarantined),
        "superseded_inventory_adjustments": superseded_count,
        "total_demand_qty": total_demand,
        "total_on_hand_qty": total_on_hand,
        "total_on_order_qty": total_on_order,
        "validated_inventory_adjustments": len(landing.inventory_adjustments),
    }
    if planning_cutoff == landing.source_manifest.planning_cutoff:
        _require_expected_controls(
            expected=expected_controls,
            observed={
                "deferred_inventory_adjustments": deferred_count,
                "superseded_inventory_adjustments": superseded_count,
            },
        )
    watermarks = {
        "latest_visible_adjustment_ingested_at": (
            _iso(adjustment_watermark) if adjustment_watermark is not None else None
        ),
        "latest_visible_fulfillment_available_at": (
            _iso(fulfillment_watermark) if fulfillment_watermark is not None else None
        ),
    }
    dbt_manifest: dict[str, object] = {
        "code_sha256": dbt_code_sha256,
        "engine": "dbt-duckdb",
        "status": "PASS",
    }
    semantic_manifest: dict[str, object] = {
        "artifact_contract_version": "1.0.0",
        "build_input_sha256": build_input_sha256,
        "dbt": dbt_manifest,
        "pipeline_code_sha256": pipeline_code_sha256,
        "gold": [
            {
                "content_sha256": artifact.content_sha256,
                "dataset": artifact.dataset,
                "row_count": artifact.row_count,
                "schema_sha256": artifact.schema_sha256,
            }
            for artifact in sorted(exported, key=lambda item: item.dataset)
        ],
        "planning_cutoff": _iso(planning_cutoff),
        "quality_counts": quality_counts,
        "release": "R2A",
        "runtime": {
            package: version(package)
            for package in (
                "dbt-core",
                "dbt-duckdb",
                "duckdb",
                "pyarrow",
                "pydantic",
            )
        },
        "source_manifest_sha256": landing.source_manifest_sha256,
        "source_manifest_file_sha256": landing.source_manifest_file_sha256,
        "watermarks": watermarks,
        "validated": [
            {
                "content_sha256": artifact.content_sha256,
                "dataset": artifact.dataset,
                "row_count": artifact.row_count,
                "schema_sha256": artifact.schema_sha256,
            }
            for artifact in sorted(
                landing.validated_artifacts,
                key=lambda item: item.dataset,
            )
        ],
    }
    semantic_manifest_sha256 = canonical_rows_sha256([{"manifest": semantic_manifest}])
    build_manifest = {
        **semantic_manifest,
        "dbt": {**dbt_manifest, "detail": dbt_detail},
        "semantic_manifest_sha256": semantic_manifest_sha256,
        "status": "PASS",
    }
    summary_path = artifact_dir / "manifests" / f"{semantic_manifest_sha256}.json"
    _write_exact_json(summary_path, build_manifest)
    return R2RebuildResult(
        planning_cutoff=planning_cutoff,
        source_manifest_sha256=landing.source_manifest_sha256,
        source_manifest_file_sha256=landing.source_manifest_file_sha256,
        semantic_manifest_sha256=semantic_manifest_sha256,
        landing=landing,
        gold_artifacts=tuple(exported),
        gold_row_counts=gold_row_counts,
        quality_counts=quality_counts,
        summary_path=summary_path,
        database_path=database_path,
        duration_seconds=clock.perf_counter() - started,
    )


def _permuted_source(source_dir: Path, target_dir: Path) -> Path:
    shutil.copytree(source_dir, target_dir, dirs_exist_ok=True)
    events_path = target_dir / "inventory_events.jsonl"
    lines = [line for line in events_path.read_text(encoding="utf-8").splitlines() if line]
    events_path.write_text("\n".join(reversed(lines)) + "\n", encoding="utf-8")
    manifest_path = target_dir / "source-manifest.json"
    manifest = R2SourceManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    sources: list[R2SourceFile] = []
    for source in manifest.sources:
        if source.file_name == events_path.name:
            sources.append(source.model_copy(update={"content_sha256": file_sha256(events_path)}))
        else:
            sources.append(source)
    updated = manifest.model_copy(update={"sources": tuple(sources)})
    manifest_path.write_bytes(
        (
            json.dumps(
                updated.model_dump(mode="json"),
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
            )
            + "\n"
        ).encode("utf-8")
    )
    return target_dir


def _gold_hashes(result: R2RebuildResult) -> dict[str, str]:
    return {artifact.dataset: artifact.content_sha256 for artifact in result.gold_artifacts}


def _inventory_rows(result: R2RebuildResult) -> list[dict[str, Any]]:
    artifact = result.gold_artifact("gold.r2_inventory_position")
    return cast(list[dict[str, Any]], pq.read_table(artifact.path).to_pylist())


def verify_r2(
    *,
    project_root: Path,
    artifact_dir: Path,
) -> list[R2VerificationResult]:
    """Run independent generation, rebuild, cutoff, and replay-equivalence checks."""

    try:
        first_fixture = generate_r2_smoke_fixture(artifact_dir / "run-a" / "source")
        second_fixture = generate_r2_smoke_fixture(artifact_dir / "run-b" / "source")
        final_first = run_r2_rebuild(
            project_root=project_root,
            source_dir=first_fixture.source_dir,
            artifact_dir=artifact_dir / "run-a" / "rebuild-final",
            planning_cutoff=first_fixture.manifest.planning_cutoff,
        )
        final_second = run_r2_rebuild(
            project_root=project_root,
            source_dir=second_fixture.source_dir,
            artifact_dir=artifact_dir / "run-b" / "rebuild-final",
            planning_cutoff=second_fixture.manifest.planning_cutoff,
        )
        early = run_r2_rebuild(
            project_root=project_root,
            source_dir=first_fixture.source_dir,
            artifact_dir=artifact_dir / "run-a" / "rebuild-early",
            planning_cutoff=first_fixture.manifest.early_replay_cutoff,
        )
        permuted_dir = _permuted_source(
            first_fixture.source_dir,
            artifact_dir / "run-c" / "source-permuted",
        )
        permuted = run_r2_rebuild(
            project_root=project_root,
            source_dir=permuted_dir,
            artifact_dir=artifact_dir / "run-c" / "rebuild-final",
            planning_cutoff=first_fixture.manifest.planning_cutoff,
        )
    except Exception as exc:  # pragma: no cover - converted to release-gate evidence
        return [R2VerificationResult("R2A executable rebuild", False, str(exc))]

    final_hashes = _gold_hashes(final_first)
    early_hashes = _gold_hashes(early)
    final_inventory = {row["sku_location_key"]: row for row in _inventory_rows(final_first)}
    early_inventory = {row["sku_location_key"]: row for row in _inventory_rows(early)}
    expected = first_fixture.manifest.expected_controls
    results = [
        R2VerificationResult(
            "deterministic synthetic source package",
            (
                first_fixture.manifest_sha256
                == second_fixture.manifest_sha256
                == _EXPECTED_SOURCE_MANIFEST_SHA256
                and first_fixture.manifest_file_sha256
                == second_fixture.manifest_file_sha256
                == _EXPECTED_SOURCE_MANIFEST_FILE_SHA256
            ),
            (
                f"manifest_content={first_fixture.manifest_sha256[:12]}, "
                f"manifest_file={first_fixture.manifest_file_sha256[:12]}, "
                f"seed={first_fixture.manifest.seed}"
            ),
        ),
        R2VerificationResult(
            "immutable landing + typed dispositions",
            (
                len(final_first.landing.product_locations) == expected.product_location_rows
                and len(final_first.landing.fulfillment_outcomes) == expected.fulfillment_rows
                and len(final_first.landing.inventory_adjustments)
                == expected.accepted_inventory_adjustments
                and len(final_first.landing.quarantined) == expected.quarantine_rows
            ),
            (
                f"products={len(final_first.landing.product_locations)}, "
                f"fulfillment={len(final_first.landing.fulfillment_outcomes)}, "
                f"events={len(final_first.landing.inventory_adjustments)}, "
                f"quarantine={len(final_first.landing.quarantined)}"
            ),
        ),
        R2VerificationResult(
            "dbt-tested Gold products",
            (
                len(final_first.gold_artifacts) == 5
                and all(count > 0 for count in final_first.gold_row_counts.values())
                and final_first.quality_counts["quarantined_records"] == expected.quarantine_rows
            ),
            (
                f"products={len(final_first.gold_artifacts)}, "
                f"rows={sum(final_first.gold_row_counts.values())}, "
                f"on_hand={final_first.quality_counts['total_on_hand_qty']}"
            ),
        ),
        R2VerificationResult(
            "reproducible clean rebuild",
            (
                final_first.semantic_manifest_sha256 == final_second.semantic_manifest_sha256
                and final_hashes == _gold_hashes(final_second)
                and (not _EXPECTED_FINAL_GOLD_HASHES or final_hashes == _EXPECTED_FINAL_GOLD_HASHES)
            ),
            f"semantic_manifest={final_first.semantic_manifest_sha256[:16]}",
        ),
        R2VerificationResult(
            "delivery-order-independent replay",
            final_hashes == _gold_hashes(permuted),
            (
                f"ordered_inventory={final_hashes['gold.r2_inventory_position'][:12]}, "
                f"permuted_inventory="
                f"{_gold_hashes(permuted)['gold.r2_inventory_position'][:12]}"
            ),
        ),
        R2VerificationResult(
            "bitemporal cutoff + replacement correction",
            (
                early_inventory["SKU-001::DC-ARG-01"]["adjustment_qty"] == -1
                and final_inventory["SKU-001::DC-ARG-01"]["adjustment_qty"] == 1
                and early.quality_counts["deferred_inventory_adjustments"] == 2
                and final_first.quality_counts["deferred_inventory_adjustments"]
                == expected.deferred_inventory_adjustments
                and final_first.quality_counts["superseded_inventory_adjustments"]
                == expected.superseded_inventory_adjustments
                and (not _EXPECTED_EARLY_GOLD_HASHES or early_hashes == _EXPECTED_EARLY_GOLD_HASHES)
            ),
            (
                "target_adjustment=-1->1, "
                f"deferred={early.quality_counts['deferred_inventory_adjustments']}->"
                f"{final_first.quality_counts['deferred_inventory_adjustments']}, "
                f"superseded="
                f"{final_first.quality_counts['superseded_inventory_adjustments']}"
            ),
        ),
        R2VerificationResult(
            "two-minute smoke ceiling",
            (
                final_first.duration_seconds < 120
                and final_second.duration_seconds < 120
                and early.duration_seconds < 120
                and permuted.duration_seconds < 120
            ),
            (
                f"final_a={final_first.duration_seconds:.2f}s, "
                f"final_b={final_second.duration_seconds:.2f}s, "
                f"early={early.duration_seconds:.2f}s, "
                f"permuted={permuted.duration_seconds:.2f}s"
            ),
        ),
    ]
    _write_json(
        artifact_dir / "r2a-verification-summary.json",
        {
            "checks": [
                {"detail": result.detail, "name": result.name, "passed": result.passed}
                for result in results
            ],
            "final_gold_hashes": final_hashes,
            "final_semantic_manifest_sha256": final_first.semantic_manifest_sha256,
            "release": "R2A",
            "status": "PASS" if all(result.passed for result in results) else "FAIL",
        },
    )
    return results
