"""Executable R2B orchestration, replay, parity, and resource gate."""

from __future__ import annotations

import json
import os
import platform
import shutil
import sys
import tempfile
import threading
import time
from contextlib import suppress
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path

import dagster as dg
import psutil

from nexus.data import R2RebuildResult
from nexus.modeling.r3_artifacts import ScientificArtifactRef, write_immutable_json_manifest
from nexus.orchestration.r2b_assets import (
    R2BAssetGraph,
    R2BPartitionMaterialization,
    build_r2b_asset_graph,
    partition_storage_key,
    r2_validation_code_sha256,
)
from nexus.orchestration.r2b_checkpoint import (
    checkpoint_governed_files,
    checkpoint_ref_from_root,
    load_r2b_validation_checkpoint,
    reference_manifest,
)
from nexus.orchestration.r2b_contracts import (
    DEFAULT_R2B_CONFIG_PATH,
    R2BConfig,
    load_r2b_config,
    r2b_config_sha256,
)
from nexus.r2 import run_r2_rebuild
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import ArtifactConflictError, file_sha256


@dataclass(frozen=True, slots=True)
class R2BVerificationCheck:
    """One independently readable R2B gate assertion."""

    name: str
    passed: bool
    detail: dict[str, object]

    def to_summary(self) -> dict[str, object]:
        return {"name": self.name, "passed": self.passed, "detail": self.detail}


@dataclass(frozen=True, slots=True)
class R2BVerificationResult:
    """Complete machine-readable R2B gate outcome."""

    checks: tuple[R2BVerificationCheck, ...]
    config_sha256: str | None
    checkpoint_sha256: str | None
    partitions: dict[str, dict[str, object]]
    dagster_run_ids: tuple[str, ...]
    resource_usage: dict[str, object]
    deferred_full_g2: tuple[str, ...]
    gate_manifest: ScientificArtifactRef | None
    summary_path: Path

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)

    def to_summary(self) -> dict[str, object]:
        return {
            "release": "R2B",
            "gate": "G2B",
            "status": "PASS" if self.passed else "FAIL",
            "full_g2_status": "IN_PROGRESS",
            "checks": [check.to_summary() for check in self.checks],
            "config_sha256": self.config_sha256,
            "checkpoint_sha256": self.checkpoint_sha256,
            "partitions": self.partitions,
            "execution": {"dagster_run_ids": list(self.dagster_run_ids)},
            "resource_usage": self.resource_usage,
            "deferred_full_g2": list(self.deferred_full_g2),
            "evidence": {
                "gate_manifest_path": (
                    self.gate_manifest.path.resolve().as_posix()
                    if self.gate_manifest is not None
                    else None
                ),
                "gate_manifest_sha256": (
                    self.gate_manifest.content_sha256 if self.gate_manifest is not None else None
                ),
                "summary_path": self.summary_path.resolve().as_posix(),
            },
        }


class _ProcessTreeSampler:
    """Sample the orchestrator and descendants while dbt subprocesses are alive."""

    def __init__(self) -> None:
        self._process = psutil.Process()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._sample_until_stopped,
            name="r2b-process-tree-sampler",
            daemon=True,
        )
        self.peak_rss_bytes = 0
        self.descendant_pids_seen: set[int] = set()

    def start(self) -> None:
        self._sample_once()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._sample_once()

    def _sample_until_stopped(self) -> None:
        while not self._stop.wait(0.02):
            self._sample_once()

    def _sample_once(self) -> None:
        try:
            descendants = self._process.children(recursive=True)
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            descendants = []
        rss = 0
        for process in (self._process, *descendants):
            try:
                rss += process.memory_info().rss
                if process.pid != self._process.pid:
                    self.descendant_pids_seen.add(process.pid)
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue
        self.peak_rss_bytes = max(self.peak_rss_bytes, rss)

    def surviving_descendant_pids(self) -> tuple[int, ...]:
        try:
            descendants = self._process.children(recursive=True)
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            return ()
        return tuple(sorted(process.pid for process in descendants if process.is_running()))


def _materialize_checkpoint(graph: R2BAssetGraph) -> tuple[str, int]:
    result = dg.materialize(
        graph.assets,
        selection=["r2b_source_package", "r2b_validation_checkpoint"],
        raise_on_error=True,
        tags={"nexus/release": "R2B", "nexus/run-kind": "validation-checkpoint"},
    )
    materializations = [
        event
        for event in result.get_asset_materialization_events()
        if event.asset_key == dg.AssetKey("r2b_validation_checkpoint")
    ]
    return result.run_id, len(materializations)


def _materialize_partition(
    graph: R2BAssetGraph,
    partition_key: str,
) -> tuple[R2BPartitionMaterialization, dg.ExecuteInProcessResult]:
    graph.config.partition_for_key(partition_key)
    result = dg.materialize(
        graph.assets,
        selection=["r2b_gold_snapshot", "r2b_semantic_lineage"],
        partition_key=partition_key,
        raise_on_error=True,
        tags={
            "nexus/release": "R2B",
            "nexus/run-kind": "cutoff-partition-materialization",
        },
    )
    output = result.output_for_node("r2b_semantic_lineage")
    if not isinstance(output, R2BPartitionMaterialization):
        raise TypeError("Dagster returned an unexpected R2B lineage output")
    return output, result


def _event_count(result: dg.ExecuteInProcessResult, attribute: str) -> int:
    return sum(
        bool(getattr(event, attribute, False)) and event.step_key == "r2b_gold_snapshot"
        for event in result.all_events
    )


def _artifact_identity(result: R2RebuildResult) -> dict[str, object]:
    return {
        "planning_cutoff": result.planning_cutoff.isoformat(),
        "source_manifest_sha256": result.source_manifest_sha256,
        "source_manifest_file_sha256": result.source_manifest_file_sha256,
        "semantic_manifest_sha256": result.semantic_manifest_sha256,
        "validated": [
            {
                "dataset": artifact.dataset,
                "content_sha256": artifact.content_sha256,
                "schema_sha256": artifact.schema_sha256,
                "row_count": artifact.row_count,
            }
            for artifact in sorted(
                result.landing.validated_artifacts, key=lambda item: item.dataset
            )
        ],
        "gold": [
            {
                "dataset": artifact.dataset,
                "content_sha256": artifact.content_sha256,
                "schema_sha256": artifact.schema_sha256,
                "row_count": artifact.row_count,
            }
            for artifact in sorted(result.gold_artifacts, key=lambda item: item.dataset)
        ],
        "gold_row_counts": dict(sorted(result.gold_row_counts.items())),
        "quality_counts": dict(sorted(result.quality_counts.items())),
    }


def _file_state(paths: tuple[Path, ...]) -> dict[str, dict[str, object]]:
    return {
        path.resolve().as_posix(): {
            "file_sha256": file_sha256(path),
            "mtime_ns": path.stat().st_mtime_ns,
            "size_bytes": path.stat().st_size,
        }
        for path in sorted(set(paths))
    }


def _partition_governed_files(
    materialization: R2BPartitionMaterialization,
) -> tuple[Path, ...]:
    return (
        *checkpoint_governed_files(materialization.checkpoint),
        *(
            artifact.path
            for artifact in sorted(
                materialization.gold.gold_artifacts, key=lambda item: item.dataset
            )
        ),
        materialization.gold.summary_path,
        materialization.semantic_manifest.path,
    )


def _semantic_has_execution_facts(value: object) -> bool:
    forbidden_keys = {
        "attempt_count",
        "dagster_run_id",
        "duration_seconds",
        "execution",
        "hostname",
        "path",
        "paths",
        "peak_rss_mib",
        "pid",
        "retry_count",
        "run_id",
    }
    if isinstance(value, dict):
        return any(
            key in forbidden_keys or _semantic_has_execution_facts(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_semantic_has_execution_facts(item) for item in value)
    return False


def _governed_artifact_bytes(artifact_root: Path) -> tuple[int, int]:
    excluded_parts = {"corruption-probe", "execution", "source", "warehouse"}
    files = [
        path
        for path in artifact_root.rglob("*")
        if path.is_file() and not excluded_parts.intersection(path.relative_to(artifact_root).parts)
    ]
    return sum(path.stat().st_size for path in files), len(files)


def _write_json_atomic(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
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


def _finalize_result(
    *,
    artifact_root: Path,
    checks: list[R2BVerificationCheck],
    config: R2BConfig | None,
    config_sha256: str | None,
    checkpoint_sha256: str | None,
    partitions: dict[str, dict[str, object]],
    dagster_run_ids: list[str],
    resource_usage: dict[str, object],
) -> R2BVerificationResult:
    summary_path = artifact_root / "r2b-verification-summary.json"
    gate_payload: dict[str, object] = {
        "artifact_contract_version": "1.0.0",
        "release": "R2B",
        "gate": "G2B",
        "status": "PASS" if all(check.passed for check in checks) else "FAIL",
        "full_g2_status": "IN_PROGRESS",
        "checks": [check.to_summary() for check in checks],
        "config_sha256": config_sha256,
        "checkpoint_sha256": checkpoint_sha256,
        "partitions": partitions,
        "execution": {"dagster_run_ids": dagster_run_ids},
        "resource_usage": resource_usage,
        "deferred_full_g2": list(config.deferred_full_g2) if config is not None else [],
    }
    gate_manifest: ScientificArtifactRef | None
    try:
        gate_manifest = write_immutable_json_manifest(
            artifact_root / "evidence",
            dataset="r2b.gate_evidence",
            manifest=gate_payload,
            schema_identity={"contract": "r2b.gate-evidence", "version": "1.0.0"},
        )
    except Exception as error:  # pragma: no cover - only unwritable/corrupt evidence roots
        checks.append(
            R2BVerificationCheck(
                name="immutable gate publication",
                passed=False,
                detail={"error": f"{type(error).__name__}: {error}"},
            )
        )
        gate_manifest = None
    result = R2BVerificationResult(
        checks=tuple(checks),
        config_sha256=config_sha256,
        checkpoint_sha256=checkpoint_sha256,
        partitions=partitions,
        dagster_run_ids=tuple(dagster_run_ids),
        resource_usage=resource_usage,
        deferred_full_g2=(config.deferred_full_g2 if config is not None else ()),
        gate_manifest=gate_manifest,
        summary_path=summary_path,
    )
    with suppress(OSError):  # gate result still returns if the mutable convenience file fails
        _write_json_atomic(summary_path, result.to_summary())
    return result


def verify_r2b(
    *,
    project_root: Path,
    artifact_root: Path,
    config_path: Path = DEFAULT_R2B_CONFIG_PATH,
) -> R2BVerificationResult:
    """Prove bounded retries, durable checkpoints, parity, replay, and idempotence."""

    started = time.perf_counter()
    sampler = _ProcessTreeSampler()
    sampler.start()
    project_root = project_root.resolve()
    artifact_root = artifact_root.resolve()
    config_path = config_path.resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)
    checks: list[R2BVerificationCheck] = []
    config: R2BConfig | None = None
    config_sha256: str | None = None
    checkpoint_sha256: str | None = None
    partitions_summary: dict[str, dict[str, object]] = {}
    dagster_run_ids: list[str] = []

    try:
        config = load_r2b_config(config_path)
        config_sha256 = r2b_config_sha256(config)
        exact_keys = tuple(
            config.partition_key(partition) for partition in config.partition_contract.partitions
        )

        forward_graph = build_r2b_asset_graph(
            project_root=project_root,
            scenario_root=artifact_root / "orchestrated-forward",
            config=config,
            inject_transient_failure=True,
        )
        observed_keys = tuple(forward_graph.partitions_def.get_partition_keys())
        checks.append(
            R2BVerificationCheck(
                name="loadable static-partition asset graph",
                passed=observed_keys == exact_keys and len(exact_keys) == 2,
                detail={
                    "partition_keys": list(observed_keys),
                    "asset_keys": sorted(
                        key.to_user_string() for asset in forward_graph.assets for key in asset.keys
                    ),
                    "backfill_max_partitions_per_run": 1,
                },
            )
        )

        checkpoint_run_id, checkpoint_materializations = _materialize_checkpoint(forward_graph)
        dagster_run_ids.append(checkpoint_run_id)
        checkpoint = checkpoint_ref_from_root(forward_graph.scenario_root / "shared")
        checkpoint_sha256 = checkpoint.content_sha256
        durable_landing = load_r2b_validation_checkpoint(
            checkpoint,
            expected_validation_code_sha256=r2_validation_code_sha256(project_root),
            expected_source_manifest_sha256=config.source_contract.source_manifest_sha256,
            expected_source_manifest_file_sha256=(
                config.source_contract.source_manifest_file_sha256
            ),
        )
        checkpoint_manifest = reference_manifest(checkpoint)
        checks.append(
            R2BVerificationCheck(
                name="durable content-addressed validation checkpoint",
                passed=(
                    checkpoint_materializations == 1
                    and durable_landing.source_manifest_file_sha256
                    == config.source_contract.source_manifest_file_sha256
                    and len(durable_landing.validated_artifacts) == 8
                    and not {
                        "config_sha256",
                        "partition_key",
                        "planning_cutoff",
                        "relative_path",
                    }.intersection(checkpoint_manifest.model_dump(mode="json"))
                ),
                detail={
                    "checkpoint_sha256": checkpoint.content_sha256,
                    "checkpoint_materializations": checkpoint_materializations,
                    "validated_datasets": len(durable_landing.validated_artifacts),
                    "identity_excludes_cutoffs_paths_and_execution": True,
                },
            )
        )
        del durable_landing

        forward: dict[str, R2BPartitionMaterialization] = {}
        forward_runs: dict[str, dg.ExecuteInProcessResult] = {}
        for partition in config.partition_contract.partitions:
            partition_key = config.partition_key(partition)
            materialization, run = _materialize_partition(forward_graph, partition_key)
            forward[partition.name] = materialization
            forward_runs[partition.name] = run
            dagster_run_ids.append(run.run_id)

        final_run = forward_runs["final"]
        retry_events = _event_count(final_run, "is_step_up_for_retry")
        restart_events = _event_count(final_run, "is_step_restarted")
        validation_events_in_partitions = sum(
            1
            for run in forward_runs.values()
            for event in run.get_asset_materialization_events()
            if event.asset_key == dg.AssetKey("r2b_validation_checkpoint")
        )
        checks.append(
            R2BVerificationCheck(
                name="bounded post-validation transient retry",
                passed=(
                    forward["early"].attempt_count == 1
                    and forward["final"].attempt_count == 2
                    and retry_events == 1
                    and restart_events == 1
                    and validation_events_in_partitions == 0
                    and all(
                        item.checkpoint.content_sha256 == checkpoint.content_sha256
                        for item in forward.values()
                    )
                ),
                detail={
                    "early_attempts": forward["early"].attempt_count,
                    "final_attempts": forward["final"].attempt_count,
                    "retry_events": retry_events,
                    "restart_events": restart_events,
                    "validation_rematerializations": validation_events_in_partitions,
                },
            )
        )

        fixture = generate_r2_smoke_fixture(artifact_root / "direct" / "source", seed=config.seed)
        if (
            fixture.manifest_sha256 != config.source_contract.source_manifest_sha256
            or fixture.manifest_file_sha256 != config.source_contract.source_manifest_file_sha256
        ):
            raise ValueError("direct source fixture disagrees with the R2B source contract")
        direct: dict[str, R2RebuildResult] = {}
        for partition in config.partition_contract.partitions:
            direct[partition.name] = run_r2_rebuild(
                project_root=project_root,
                source_dir=fixture.source_dir,
                artifact_dir=(
                    artifact_root
                    / "direct"
                    / "partitions"
                    / partition_storage_key(config.partition_key(partition))
                    / "rebuild"
                ),
                planning_cutoff=partition.planning_cutoff,
            )
        parity = {
            name: _artifact_identity(direct[name]) == _artifact_identity(forward[name].gold)
            for name in ("early", "final")
        }
        checks.append(
            R2BVerificationCheck(
                name="direct-versus-orchestrated Gold parity",
                passed=all(parity.values()),
                detail={
                    "partitions": parity,
                    "r2a_semantic_sha256": {
                        name: forward[name].gold.semantic_manifest_sha256
                        for name in ("early", "final")
                    },
                },
            )
        )

        expected_controls = fixture.manifest.expected_controls
        cutoff_visible = (
            forward["early"].gold.quality_counts["deferred_inventory_adjustments"] == 2
            and forward["final"].gold.quality_counts["deferred_inventory_adjustments"]
            == expected_controls.deferred_inventory_adjustments
            and forward["final"].gold.quality_counts["superseded_inventory_adjustments"]
            == expected_controls.superseded_inventory_adjustments
            and forward["early"].gold.semantic_manifest_sha256
            != forward["final"].gold.semantic_manifest_sha256
        )
        checks.append(
            R2BVerificationCheck(
                name="cutoff-specific correction visibility",
                passed=cutoff_visible,
                detail={
                    "early_deferred": forward["early"].gold.quality_counts[
                        "deferred_inventory_adjustments"
                    ],
                    "final_deferred": forward["final"].gold.quality_counts[
                        "deferred_inventory_adjustments"
                    ],
                    "final_superseded": forward["final"].gold.quality_counts[
                        "superseded_inventory_adjustments"
                    ],
                },
            )
        )

        reverse_graph = build_r2b_asset_graph(
            project_root=project_root,
            scenario_root=artifact_root / "orchestrated-reverse",
            config=config,
            inject_transient_failure=False,
        )
        reverse_checkpoint_run_id, reverse_checkpoint_materializations = _materialize_checkpoint(
            reverse_graph
        )
        dagster_run_ids.append(reverse_checkpoint_run_id)
        reverse: dict[str, R2BPartitionMaterialization] = {}
        for partition in reversed(config.partition_contract.partitions):
            materialization, run = _materialize_partition(
                reverse_graph, config.partition_key(partition)
            )
            reverse[partition.name] = materialization
            dagster_run_ids.append(run.run_id)
        reverse_equal = {
            name: forward[name].semantic_sha256 == reverse[name].semantic_sha256
            for name in ("early", "final")
        }
        checks.append(
            R2BVerificationCheck(
                name="reverse-order backfill equivalence",
                passed=(
                    reverse_checkpoint_materializations == 1
                    and all(reverse_equal.values())
                    and len(
                        {
                            item.checkpoint.content_sha256
                            for item in (*forward.values(), *reverse.values())
                        }
                    )
                    == 1
                ),
                detail={
                    "partitions": reverse_equal,
                    "shared_checkpoint_sha256": checkpoint.content_sha256,
                    "reverse_checkpoint_materializations": (reverse_checkpoint_materializations),
                },
            )
        )

        before_rerun = _file_state(_partition_governed_files(forward["early"]))
        rerun, rerun_result = _materialize_partition(
            forward_graph,
            config.partition_key(config.partition_contract.partitions[0]),
        )
        dagster_run_ids.append(rerun_result.run_id)
        after_rerun = _file_state(_partition_governed_files(rerun))
        checks.append(
            R2BVerificationCheck(
                name="idempotent partition rerun preserves governed artifacts",
                passed=(
                    rerun.semantic_sha256 == forward["early"].semantic_sha256
                    and before_rerun == after_rerun
                    and rerun.dagster_run_id != forward["early"].dagster_run_id
                ),
                detail={
                    "semantic_sha256": rerun.semantic_sha256,
                    "governed_file_count": len(after_rerun),
                    "execution_run_ids_distinct": (
                        rerun.dagster_run_id != forward["early"].dagster_run_id
                    ),
                },
            )
        )

        corruption_root = artifact_root / "corruption-probe" / checkpoint.content_sha256
        shutil.copytree(
            forward_graph.scenario_root / "shared",
            corruption_root,
            dirs_exist_ok=True,
        )
        corrupt_ref = checkpoint_ref_from_root(corruption_root)
        corrupt_parquet = next(
            path for path in checkpoint_governed_files(corrupt_ref) if path.suffix == ".parquet"
        )
        payload = corrupt_parquet.read_bytes()
        corrupt_parquet.write_bytes(payload[:-1] + bytes([payload[-1] ^ 0x01]))
        corruption_error: str | None = None
        try:
            load_r2b_validation_checkpoint(
                corrupt_ref,
                expected_validation_code_sha256=r2_validation_code_sha256(project_root),
                expected_source_manifest_sha256=config.source_contract.source_manifest_sha256,
                expected_source_manifest_file_sha256=(
                    config.source_contract.source_manifest_file_sha256
                ),
            )
        except ArtifactConflictError as error:
            corruption_error = f"{type(error).__name__}: {error}"
        checks.append(
            R2BVerificationCheck(
                name="checkpoint corruption fails closed before dbt",
                passed=corruption_error is not None,
                detail={"observed_error": corruption_error},
            )
        )

        semantic_execution_separation: dict[str, bool] = {}
        for name, materialization in forward.items():
            semantic = json.loads(
                materialization.semantic_manifest.path.read_text(encoding="utf-8")
            )
            execution = json.loads(materialization.execution_path.read_text(encoding="utf-8"))
            semantic_execution_separation[name] = (
                not _semantic_has_execution_facts(semantic)
                and execution["dagster_run_id"] == materialization.dagster_run_id
                and execution["semantic_manifest_sha256"] == materialization.semantic_sha256
                and execution["checkpoint_reused"] is True
                and semantic["status"] == "MATERIALIZED_UNVERIFIED"
            )
            partitions_summary[name] = {
                "partition_key": materialization.partition_key,
                "planning_cutoff": config.partition_for_key(
                    materialization.partition_key
                ).planning_cutoff.isoformat(),
                "checkpoint_sha256": materialization.checkpoint.content_sha256,
                "r2a_semantic_manifest_sha256": (materialization.gold.semantic_manifest_sha256),
                "r2b_semantic_manifest_sha256": materialization.semantic_sha256,
                "attempt_count": materialization.attempt_count,
            }
        checks.append(
            R2BVerificationCheck(
                name="semantic and execution evidence are separated",
                passed=all(semantic_execution_separation.values()),
                detail={"partitions": semantic_execution_separation},
            )
        )
    except Exception as error:
        checks.append(
            R2BVerificationCheck(
                name="R2B executable verification",
                passed=False,
                detail={"error": f"{type(error).__name__}: {error}"},
            )
        )

    sampler.stop()
    wall_time_seconds = time.perf_counter() - started
    artifact_bytes, artifact_file_count = _governed_artifact_bytes(artifact_root)
    surviving_descendants = sampler.surviving_descendant_pids()
    python_threads_after_run = threading.active_count()
    peak_rss_mib = sampler.peak_rss_bytes / (1024 * 1024)
    resource_usage: dict[str, object] = {
        "measurement": {
            "wall_time_scope": "r2b_complete_verification",
            "peak_rss_scope": "orchestrator_process_tree_high_water",
            "peak_rss_sample_interval_seconds": 0.02,
            "artifact_scope": "r2b_governed_manifests_and_r2_snapshots",
            "artifact_exclusions": [
                "corruption-probe",
                "execution metadata",
                "generated source inputs",
                "mutable DuckDB warehouses",
            ],
        },
        "observed": {
            "wall_time_seconds": round(wall_time_seconds, 6),
            "peak_rss_mib": round(peak_rss_mib, 3),
            "python_threads_after_run": python_threads_after_run,
            "artifact_bytes": artifact_bytes,
            "artifact_file_count": artifact_file_count,
            "descendant_processes_seen": len(sampler.descendant_pids_seen),
            "surviving_descendant_pids": list(surviving_descendants),
        },
        "runtime": {
            "dagster": version("dagster"),
            "dbt-core": version("dbt-core"),
            "psutil": version("psutil"),
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "executable": Path(sys.executable).name,
        },
    }
    if config is not None:
        budget = config.resource_budget
        resource_passed = (
            wall_time_seconds <= budget.wall_time_seconds
            and peak_rss_mib <= budget.peak_rss_mib
            and python_threads_after_run <= budget.maximum_python_threads
            and artifact_bytes <= budget.artifact_mib * 1024 * 1024
            and not surviving_descendants
        )
        resource_usage["budget"] = {
            "wall_time_seconds": budget.wall_time_seconds,
            "peak_rss_mib": budget.peak_rss_mib,
            "maximum_python_threads": budget.maximum_python_threads,
            "artifact_bytes": budget.artifact_mib * 1024 * 1024,
            "surviving_descendants": 0,
        }
        checks.append(
            R2BVerificationCheck(
                name="bounded complete-verification resource envelope",
                passed=resource_passed,
                detail={
                    "wall_time_seconds": round(wall_time_seconds, 3),
                    "peak_rss_mib": round(peak_rss_mib, 3),
                    "python_threads_after_run": python_threads_after_run,
                    "artifact_bytes": artifact_bytes,
                    "surviving_descendant_pids": list(surviving_descendants),
                },
            )
        )
    else:
        resource_usage["budget"] = None

    return _finalize_result(
        artifact_root=artifact_root,
        checks=checks,
        config=config,
        config_sha256=config_sha256,
        checkpoint_sha256=checkpoint_sha256,
        partitions=partitions_summary,
        dagster_run_ids=dagster_run_ids,
        resource_usage=resource_usage,
    )
