from __future__ import annotations

from datetime import datetime
from pathlib import Path

import dagster as dg
import pytest

from nexus.data import R2LandingResult, R2RebuildResult
from nexus.orchestration import r2b_assets
from nexus.orchestration.r2b_assets import (
    R2BAssetGraph,
    R2BGoldSnapshot,
    build_r2b_asset_graph,
)
from nexus.orchestration.r2b_checkpoint import (
    checkpoint_governed_files,
    checkpoint_ref_from_root,
)
from nexus.orchestration.r2b_contracts import load_r2b_config
from nexus.storage import ArtifactConflictError, file_sha256

ROOT = Path(__file__).resolve().parents[1]


def _graph(tmp_path: Path, *, inject_transient_failure: bool = False) -> R2BAssetGraph:
    return build_r2b_asset_graph(
        project_root=ROOT,
        scenario_root=tmp_path,
        config=load_r2b_config(),
        inject_transient_failure=inject_transient_failure,
    )


def _asset(graph: R2BAssetGraph, name: str) -> dg.AssetsDefinition:
    key = dg.AssetKey(name)
    return next(asset for asset in graph.assets if key in asset.keys)


def test_asset_definitions_are_loadable_with_exact_partition_topology(tmp_path: Path) -> None:
    graph = _graph(tmp_path)
    config = graph.config
    manifest_hash = config.source_contract.source_manifest_file_sha256
    expected_keys = (
        f"r2b-v1__{manifest_hash}__20260620T000000Z",
        f"r2b-v1__{manifest_hash}__20260630T000000Z",
    )

    dg.Definitions.validate_loadable(graph.definitions)
    assert graph.partitions_def.get_partition_keys() == expected_keys
    assert all(len(key.split("__")[1]) == 64 for key in expected_keys)

    source = _asset(graph, "r2b_source_package")
    checkpoint = _asset(graph, "r2b_validation_checkpoint")
    gold = _asset(graph, "r2b_gold_snapshot")
    lineage = _asset(graph, "r2b_semantic_lineage")

    assert source.partitions_def is None
    assert checkpoint.partitions_def is None
    assert gold.partitions_def is graph.partitions_def
    assert lineage.partitions_def is graph.partitions_def
    assert checkpoint.asset_deps[dg.AssetKey("r2b_validation_checkpoint")] == {
        dg.AssetKey("r2b_source_package")
    }
    assert gold.asset_deps[dg.AssetKey("r2b_gold_snapshot")] == {
        dg.AssetKey("r2b_validation_checkpoint")
    }
    assert lineage.asset_deps[dg.AssetKey("r2b_semantic_lineage")] == {
        dg.AssetKey("r2b_gold_snapshot")
    }


def test_retry_declared_backfill_and_process_tree_policies_are_bounded(tmp_path: Path) -> None:
    graph = _graph(tmp_path)
    gold = _asset(graph, "r2b_gold_snapshot")
    lineage = _asset(graph, "r2b_semantic_lineage")
    retry_policy = gold.op.retry_policy

    assert retry_policy is not None
    assert retry_policy.max_retries == 1
    assert retry_policy.delay == 0
    assert gold.backfill_policy is not None
    assert gold.backfill_policy.max_partitions_per_run == 1
    assert lineage.backfill_policy is not None
    assert lineage.backfill_policy.max_partitions_per_run == 1
    assert graph.config.resource_budget.peak_rss_scope == ("orchestrator_process_tree_high_water")
    assert graph.config.resource_budget.peak_rss_mib == 2048


def test_final_partition_retries_once_then_reuses_immutable_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = _graph(tmp_path, inject_transient_failure=True)
    instance = dg.DagsterInstance.ephemeral()
    try:
        checkpoint_run = dg.materialize(
            graph.assets,
            selection=["r2b_source_package", "r2b_validation_checkpoint"],
            instance=instance,
            raise_on_error=True,
        )
        assert checkpoint_run.success
        checkpoint = checkpoint_ref_from_root(tmp_path / "shared")
        before = {
            path: (file_sha256(path), path.stat().st_mtime_ns)
            for path in checkpoint_governed_files(checkpoint)
        }
        calls: list[datetime] = []

        def fake_gold(
            *,
            project_root: Path,
            artifact_dir: Path,
            planning_cutoff: datetime,
            checkpoint: R2LandingResult,
        ) -> R2RebuildResult:
            del project_root
            calls.append(planning_cutoff)
            artifact_dir.mkdir(parents=True, exist_ok=True)
            return R2RebuildResult(
                planning_cutoff=planning_cutoff,
                source_manifest_sha256=checkpoint.source_manifest_sha256,
                source_manifest_file_sha256=checkpoint.source_manifest_file_sha256,
                semantic_manifest_sha256="d" * 64,
                landing=checkpoint,
                gold_artifacts=(),
                gold_row_counts={},
                quality_counts={},
                summary_path=artifact_dir / "summary.json",
                database_path=artifact_dir / "warehouse.duckdb",
                duration_seconds=0.0,
            )

        monkeypatch.setattr(r2b_assets, "run_r2_gold_from_checkpoint", fake_gold)
        final = graph.config.partition_contract.partitions[1]
        partition_key = graph.config.partition_key(final)
        result = dg.materialize(
            graph.assets,
            selection=["r2b_gold_snapshot"],
            partition_key=partition_key,
            instance=instance,
            raise_on_error=True,
        )

        output = result.output_for_node("r2b_gold_snapshot")
        assert isinstance(output, R2BGoldSnapshot)
        assert result.success
        assert output.partition_name == "final"
        assert output.partition_key == partition_key
        assert output.attempt_count == 2
        assert output.checkpoint_reused is True
        assert output.checkpoint == checkpoint
        assert calls == [final.planning_cutoff]
        assert (
            sum(event.event_type_value == "STEP_UP_FOR_RETRY" for event in result.all_events) == 1
        )
        assert {
            path: (file_sha256(path), path.stat().st_mtime_ns)
            for path in checkpoint_governed_files(checkpoint)
        } == before
    finally:
        instance.dispose()


def test_checkpoint_integrity_failure_is_terminal_and_not_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = _graph(tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    try:
        checkpoint_run = dg.materialize(
            graph.assets,
            selection=["r2b_source_package", "r2b_validation_checkpoint"],
            instance=instance,
            raise_on_error=True,
        )
        assert checkpoint_run.success

        def reject_checkpoint(*args: object, **kwargs: object) -> R2LandingResult:
            del args, kwargs
            raise ArtifactConflictError("deliberate terminal integrity failure")

        monkeypatch.setattr(
            r2b_assets,
            "load_r2b_validation_checkpoint",
            reject_checkpoint,
        )
        final = graph.config.partition_contract.partitions[1]
        result = dg.materialize(
            graph.assets,
            selection=["r2b_gold_snapshot"],
            partition_key=graph.config.partition_key(final),
            instance=instance,
            raise_on_error=False,
        )

        assert result.success is False
        assert sum(event.is_step_failure for event in result.all_events) == 1
        assert sum(event.is_step_up_for_retry for event in result.all_events) == 0
        assert sum(event.is_step_restarted for event in result.all_events) == 0
    finally:
        instance.dispose()


@pytest.mark.parametrize(
    "unclassified_error",
    [
        RuntimeError("deliberate unclassified orchestration failure"),
        dg.RetryRequested(max_retries=1, seconds_to_wait=0),
        dg.Failure(
            description="deliberate retry-enabled Dagster failure",
            allow_retries=True,
        ),
    ],
    ids=("runtime-error", "retry-requested", "retry-enabled-failure"),
)
def test_unclassified_gold_failure_is_terminal_and_not_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unclassified_error: Exception,
) -> None:
    graph = _graph(tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    try:
        checkpoint_run = dg.materialize(
            graph.assets,
            selection=["r2b_source_package", "r2b_validation_checkpoint"],
            instance=instance,
            raise_on_error=True,
        )
        assert checkpoint_run.success

        calls = 0

        def reject_storage_key(partition_key: str) -> str:
            del partition_key
            nonlocal calls
            calls += 1
            raise unclassified_error

        monkeypatch.setattr(r2b_assets, "partition_storage_key", reject_storage_key)
        final = graph.config.partition_contract.partitions[1]
        result = dg.materialize(
            graph.assets,
            selection=["r2b_gold_snapshot"],
            partition_key=graph.config.partition_key(final),
            instance=instance,
            raise_on_error=False,
        )

        assert result.success is False
        assert calls == 1
        assert sum(event.is_step_failure for event in result.all_events) == 1
        assert sum(event.is_step_up_for_retry for event in result.all_events) == 0
        assert sum(event.is_step_restarted for event in result.all_events) == 0
    finally:
        instance.dispose()
