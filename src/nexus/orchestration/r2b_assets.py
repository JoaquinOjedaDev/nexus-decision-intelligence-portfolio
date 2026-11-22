"""Dagster assets for bounded, deterministic R2B cutoff partition materialization."""

import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext

from nexus.data import R2GeneratedFixture, R2RebuildResult
from nexus.modeling.r3_artifacts import ScientificArtifactRef, write_immutable_json_manifest
from nexus.orchestration.r2b_checkpoint import (
    R2BValidationCheckpointRef,
    checkpoint_governed_files,
    checkpoint_ref_from_root,
    load_r2b_validation_checkpoint,
    reference_manifest,
    write_r2b_validation_checkpoint,
)
from nexus.orchestration.r2b_contracts import R2BConfig, r2b_config_sha256
from nexus.r2 import materialize_r2_validation_checkpoint, run_r2_gold_from_checkpoint
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import ArtifactConflictError, canonical_rows_sha256

_ASSET_GRAPH_VERSION = "r2b-assets-v1"


class R2BTransientError(RuntimeError):
    """The sole retryable R2B failure category."""


@dataclass(frozen=True, slots=True)
class R2BGoldSnapshot:
    """In-run Gold output kept separate from immutable semantic evidence."""

    partition_key: str
    partition_name: str
    checkpoint: R2BValidationCheckpointRef
    checkpoint_reused: bool
    attempt_count: int
    duration_seconds: float
    result: R2RebuildResult


@dataclass(frozen=True, slots=True)
class R2BPartitionMaterialization:
    """One successful orchestrated partition and its two evidence planes."""

    partition_key: str
    partition_name: str
    checkpoint: R2BValidationCheckpointRef
    gold: R2RebuildResult
    semantic_manifest: ScientificArtifactRef
    execution_path: Path
    dagster_run_id: str
    attempt_count: int

    @property
    def semantic_sha256(self) -> str:
        return self.semantic_manifest.content_sha256


@dataclass(frozen=True, slots=True)
class R2BAssetGraph:
    """Loadable definitions plus the paths and contracts needed by the verifier."""

    assets: tuple[dg.AssetsDefinition, ...]
    definitions: dg.Definitions
    partitions_def: dg.StaticPartitionsDefinition
    project_root: Path
    scenario_root: Path
    config: R2BConfig
    inject_transient_failure: bool


def _normalized_file_sha256(path: Path) -> str:
    normalized = path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return hashlib.sha256(normalized).hexdigest()


def _code_sha256(project_root: Path, relative_paths: tuple[str, ...]) -> str:
    entries = [
        {"path": path, "sha256": _normalized_file_sha256(project_root / path)}
        for path in relative_paths
    ]
    return canonical_rows_sha256(entries)


def r2_validation_code_sha256(project_root: Path) -> str:
    """Hash only the code and schemas that define source validation semantics."""

    return _code_sha256(
        project_root.resolve(),
        (
            "src/nexus/contracts/models.py",
            "src/nexus/data/r2_models.py",
            "src/nexus/data/r2_pipeline.py",
            "src/nexus/storage/artifacts.py",
            "contracts/schemas/event-envelope.schema.json",
            "contracts/schemas/r2-inventory-adjustment-payload.schema.json",
            "contracts/schemas/r2-source-manifest.schema.json",
        ),
    )


def r2b_orchestration_code_sha256(project_root: Path) -> str:
    """Hash the executable orchestration and durable-checkpoint implementation."""

    return _code_sha256(
        project_root.resolve(),
        (
            "src/nexus/orchestration/r2b_assets.py",
            "src/nexus/orchestration/r2b_checkpoint.py",
            "src/nexus/orchestration/r2b_contracts.py",
            "src/nexus/r2.py",
        ),
    )


def partition_storage_key(partition_key: str) -> str:
    """Keep Windows paths bounded while retaining the full key in evidence."""

    return hashlib.sha256(partition_key.encode("utf-8")).hexdigest()


def _file_state(paths: tuple[Path, ...]) -> dict[str, tuple[str, int]]:
    return {
        path.resolve().as_posix(): (
            hashlib.sha256(path.read_bytes()).hexdigest(),
            path.stat().st_mtime_ns,
        )
        for path in paths
    }


def _write_execution_json(path: Path, payload: dict[str, object]) -> None:
    encoded = (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    ).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{path.stem}.", suffix=".tmp", dir=path.parent, delete=False
        ) as temporary:
            temporary.write(encoded)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def build_r2b_asset_graph(
    *,
    project_root: Path,
    scenario_root: Path,
    config: R2BConfig,
    inject_transient_failure: bool,
) -> R2BAssetGraph:
    """Build a loadable asset graph without starting a daemon or external service."""

    project_root = project_root.resolve()
    scenario_root = scenario_root.resolve()
    checkpoint_root = scenario_root / "shared"
    validation_code_sha256 = r2_validation_code_sha256(project_root)
    orchestration_code_sha256 = r2b_orchestration_code_sha256(project_root)
    config_sha256 = r2b_config_sha256(config)
    partition_keys = tuple(
        config.partition_key(partition) for partition in config.partition_contract.partitions
    )
    partitions_def = dg.StaticPartitionsDefinition(partition_keys)
    backfill_policy = dg.BackfillPolicy.multi_run(max_partitions_per_run=1)

    @dg.asset(
        name="r2b_source_package",
        group_name="r2b_data_foundation",
        code_version=validation_code_sha256,
        kinds={"python", "synthetic"},
    )
    def source_package(context: AssetExecutionContext) -> R2GeneratedFixture:
        fixture = generate_r2_smoke_fixture(checkpoint_root / "source", seed=config.seed)
        if (
            fixture.manifest_sha256 != config.source_contract.source_manifest_sha256
            or fixture.manifest_file_sha256 != config.source_contract.source_manifest_file_sha256
        ):
            raise dg.Failure(
                description="generated source package disagrees with the frozen R2B contract",
                allow_retries=False,
            )
        context.add_output_metadata(
            {
                "profile": fixture.manifest.profile,
                "seed": fixture.manifest.seed,
                "source_manifest_sha256": fixture.manifest_sha256,
                "source_manifest_file_sha256": fixture.manifest_file_sha256,
            }
        )
        return fixture

    @dg.asset(
        name="r2b_validation_checkpoint",
        group_name="r2b_data_foundation",
        code_version=validation_code_sha256,
        kinds={"python", "parquet"},
    )
    def validation_checkpoint(
        context: AssetExecutionContext,
        r2b_source_package: R2GeneratedFixture,
    ) -> R2BValidationCheckpointRef:
        try:
            landing = materialize_r2_validation_checkpoint(
                source_dir=r2b_source_package.source_dir,
                artifact_dir=checkpoint_root / "rebuild",
            )
            reference = write_r2b_validation_checkpoint(
                checkpoint_root=checkpoint_root,
                validation_code_sha256=validation_code_sha256,
                landing=landing,
            )
            load_r2b_validation_checkpoint(
                reference,
                expected_validation_code_sha256=validation_code_sha256,
                expected_source_manifest_sha256=config.source_contract.source_manifest_sha256,
                expected_source_manifest_file_sha256=(
                    config.source_contract.source_manifest_file_sha256
                ),
            )
        except Exception as error:
            raise dg.Failure(
                description=f"validation checkpoint failed closed: {error}",
                allow_retries=False,
            ) from error
        context.add_output_metadata(
            {
                "checkpoint_sha256": reference.content_sha256,
                "validated_datasets": len(landing.validated_artifacts),
            }
        )
        return reference

    @dg.asset(
        name="r2b_gold_snapshot",
        deps=[dg.AssetKey("r2b_validation_checkpoint")],
        partitions_def=partitions_def,
        backfill_policy=backfill_policy,
        retry_policy=dg.RetryPolicy(
            max_retries=config.retry_contract.maximum_retries,
            delay=config.retry_contract.delay_seconds,
        ),
        group_name="r2b_data_foundation",
        code_version=orchestration_code_sha256,
        kinds={"dagster", "dbt", "duckdb"},
    )
    def gold_snapshot(context: AssetExecutionContext) -> R2BGoldSnapshot:
        try:
            return _materialize_gold_snapshot(context)
        except R2BTransientError:
            raise
        except dg.Failure as error:
            if error.allow_retries is False:
                raise
            raise dg.Failure(
                description=f"unexpected retry-enabled Gold failure: {error}",
                allow_retries=False,
            ) from error
        except Exception as error:
            raise dg.Failure(
                description=f"unexpected Gold orchestration failure: {error}",
                allow_retries=False,
            ) from error

    def _materialize_gold_snapshot(
        context: AssetExecutionContext,
    ) -> R2BGoldSnapshot:
        started = time.perf_counter()
        partition_key = context.partition_key
        try:
            partition = config.partition_for_key(partition_key)
            checkpoint = checkpoint_ref_from_root(checkpoint_root)
            before = _file_state(checkpoint_governed_files(checkpoint))
            landing = load_r2b_validation_checkpoint(
                checkpoint,
                expected_validation_code_sha256=validation_code_sha256,
                expected_source_manifest_sha256=config.source_contract.source_manifest_sha256,
                expected_source_manifest_file_sha256=(
                    config.source_contract.source_manifest_file_sha256
                ),
            )
        except Exception as error:
            raise dg.Failure(
                description=f"checkpoint integrity failure: {error}",
                allow_retries=False,
            ) from error

        failure = config.retry_contract.injected_failure
        if (
            inject_transient_failure
            and partition.name == failure.partition
            and context.retry_number < failure.failing_attempts
        ):
            raise R2BTransientError("deterministic post-validation failure injected before dbt")

        partition_root = scenario_root / "partitions" / partition_storage_key(partition_key)
        try:
            result = run_r2_gold_from_checkpoint(
                project_root=project_root,
                artifact_dir=partition_root / "rebuild",
                planning_cutoff=partition.planning_cutoff,
                checkpoint=landing,
            )
            after = _file_state(checkpoint_governed_files(checkpoint))
            if before != after:
                raise ArtifactConflictError(
                    "validated checkpoint changed during Gold materialization"
                )
        except Exception as error:
            raise dg.Failure(
                description=f"Gold build failed permanently: {error}",
                allow_retries=False,
            ) from error

        context.add_output_metadata(
            {
                "partition_key": partition_key,
                "planning_cutoff": partition.planning_cutoff.isoformat(),
                "checkpoint_sha256": checkpoint.content_sha256,
                "r2a_semantic_sha256": result.semantic_manifest_sha256,
                "attempt_count": context.retry_number + 1,
            }
        )
        return R2BGoldSnapshot(
            partition_key=partition_key,
            partition_name=partition.name,
            checkpoint=checkpoint,
            checkpoint_reused=True,
            attempt_count=context.retry_number + 1,
            duration_seconds=time.perf_counter() - started,
            result=result,
        )

    @dg.asset(
        name="r2b_semantic_lineage",
        partitions_def=partitions_def,
        backfill_policy=backfill_policy,
        group_name="r2b_data_foundation",
        code_version=orchestration_code_sha256,
        kinds={"dagster", "json"},
    )
    def semantic_lineage(
        context: AssetExecutionContext,
        r2b_gold_snapshot: R2BGoldSnapshot,
    ) -> dg.Output[R2BPartitionMaterialization]:
        partition = config.partition_for_key(r2b_gold_snapshot.partition_key)
        partition_root = (
            scenario_root / "partitions" / partition_storage_key(r2b_gold_snapshot.partition_key)
        )
        checkpoint_manifest = reference_manifest(r2b_gold_snapshot.checkpoint)
        r2a_manifest = json.loads(r2b_gold_snapshot.result.summary_path.read_text(encoding="utf-8"))
        semantic: dict[str, object] = {
            "artifact_contract_version": "1.0.0",
            "release": "R2B",
            "status": "MATERIALIZED_UNVERIFIED",
            "asset_graph": {
                "dagster_version": version("dagster"),
                "graph_version": _ASSET_GRAPH_VERSION,
                "orchestration_code_sha256": orchestration_code_sha256,
            },
            "config_sha256": config_sha256,
            "partition": {
                "key": r2b_gold_snapshot.partition_key,
                "name": partition.name,
                "planning_cutoff": partition.planning_cutoff,
                "role": partition.role,
            },
            "retry_policy": {
                "delay_seconds": config.retry_contract.delay_seconds,
                "maximum_retries": config.retry_contract.maximum_retries,
                "retryable_failure": "R2BTransientError",
            },
            "source": {
                "source_manifest_file_sha256": (config.source_contract.source_manifest_file_sha256),
                "source_manifest_sha256": config.source_contract.source_manifest_sha256,
            },
            "validation_checkpoint": {
                "content_sha256": r2b_gold_snapshot.checkpoint.content_sha256,
                "file_sha256": r2b_gold_snapshot.checkpoint.file_sha256,
                "schema_sha256": r2b_gold_snapshot.checkpoint.schema_sha256,
                "validated": [
                    item.model_dump(mode="json") for item in checkpoint_manifest.validated
                ],
            },
            "r2a_snapshot": {
                "gold": r2a_manifest["gold"],
                "quality_counts": r2b_gold_snapshot.result.quality_counts,
                "semantic_manifest_sha256": (r2b_gold_snapshot.result.semantic_manifest_sha256),
                "watermarks": r2a_manifest["watermarks"],
            },
        }
        manifest_ref = write_immutable_json_manifest(
            partition_root / "governed",
            dataset="r2b.semantic_lineage",
            manifest=semantic,
            schema_identity={"contract": "r2b.semantic-lineage", "version": "1.0.0"},
        )
        execution_path = partition_root / "execution" / f"{context.run_id}.json"
        _write_execution_json(
            execution_path,
            {
                "release": "R2B",
                "status": "SUCCESS",
                "dagster_run_id": context.run_id,
                "partition_key": r2b_gold_snapshot.partition_key,
                "semantic_manifest_sha256": manifest_ref.content_sha256,
                "checkpoint_sha256": r2b_gold_snapshot.checkpoint.content_sha256,
                "checkpoint_reused": r2b_gold_snapshot.checkpoint_reused,
                "attempt_count": r2b_gold_snapshot.attempt_count,
                "retry_count": r2b_gold_snapshot.attempt_count - 1,
                "gold_duration_seconds": round(r2b_gold_snapshot.duration_seconds, 6),
                "paths": {
                    "semantic_manifest": manifest_ref.path.resolve().as_posix(),
                    "r2a_manifest": r2b_gold_snapshot.result.summary_path.resolve().as_posix(),
                },
            },
        )
        materialization = R2BPartitionMaterialization(
            partition_key=r2b_gold_snapshot.partition_key,
            partition_name=partition.name,
            checkpoint=r2b_gold_snapshot.checkpoint,
            gold=r2b_gold_snapshot.result,
            semantic_manifest=manifest_ref,
            execution_path=execution_path,
            dagster_run_id=context.run_id,
            attempt_count=r2b_gold_snapshot.attempt_count,
        )
        return dg.Output(
            materialization,
            data_version=dg.DataVersion(manifest_ref.content_sha256),
            metadata={
                "semantic_manifest_sha256": manifest_ref.content_sha256,
                "partition_key": r2b_gold_snapshot.partition_key,
                "status": "MATERIALIZED_UNVERIFIED",
            },
        )

    assets = (source_package, validation_checkpoint, gold_snapshot, semantic_lineage)
    definitions = dg.Definitions(assets=list(assets))
    dg.Definitions.validate_loadable(definitions)
    return R2BAssetGraph(
        assets=assets,
        definitions=definitions,
        partitions_def=partitions_def,
        project_root=project_root,
        scenario_root=scenario_root,
        config=config,
        inject_transient_failure=inject_transient_failure,
    )
