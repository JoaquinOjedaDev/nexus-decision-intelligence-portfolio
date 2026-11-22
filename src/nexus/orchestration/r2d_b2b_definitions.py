"""Loadable Dagster definitions for the persistent R2D-B2b backfill service gate."""

from __future__ import annotations

import os
from pathlib import Path

import dagster as dg

from nexus.orchestration.r2b_assets import build_r2b_asset_graph
from nexus.orchestration.r2b_contracts import load_r2b_config
from nexus.orchestration.r2d_b2b_contracts import PROJECT_ROOT


def build_r2d_b2b_definitions() -> dg.Definitions:
    """Build the existing R2B assets plus one actual two-partition backfill job."""

    project_root = Path(os.getenv("NEXUS_R2D_B2B_PROJECT_ROOT", str(PROJECT_ROOT))).resolve()
    scenario_root = Path(
        os.getenv(
            "NEXUS_R2D_B2B_SCENARIO_ROOT",
            str(project_root / "artifacts" / "r2d" / "b2b-definition-smoke"),
        )
    ).resolve()
    config_path = Path(
        os.getenv(
            "NEXUS_R2D_B2B_R2B_CONFIG_PATH",
            str(project_root / "configs" / "orchestration" / "r2b-backfill.yaml"),
        )
    ).resolve()
    config = load_r2b_config(config_path)
    graph = build_r2b_asset_graph(
        project_root=project_root,
        scenario_root=scenario_root,
        config=config,
        inject_transient_failure=False,
    )
    backfill_job = dg.define_asset_job(
        name="r2d_b2b_backfill_job",
        selection=dg.AssetSelection.assets("r2b_gold_snapshot", "r2b_semantic_lineage"),
        partitions_def=graph.partitions_def,
        tags={"nexus/release": "R2D-B2b", "nexus/operation": "bounded-backfill"},
        description="Actual bounded two-partition backfill using the governed R2B asset graph.",
    )
    definitions = dg.Definitions(assets=list(graph.assets), jobs=[backfill_job])
    dg.Definitions.validate_loadable(definitions)
    return definitions


defs = build_r2d_b2b_definitions()
