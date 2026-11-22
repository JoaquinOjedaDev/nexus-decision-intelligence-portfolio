from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nexus.data.r2_models import R2LandingResult, R2RebuildResult
from nexus.modeling.gold_adapter import (
    R3DemandSnapshotContract,
    R3GoldAdapterError,
    adapt_r2_demand_history,
)
from nexus.storage import ContentAddressedArtifact, write_immutable_parquet

DATASET = "gold.r2_demand_history"
CUTOFF = datetime(2026, 6, 30, tzinfo=UTC)


def _row(
    *,
    sku_id: str = "SKU-001",
    location_id: str = "DC-01",
    demand_date: str = "2026-06-01",
    demand_qty: int = 8,
    latest_available_at: str = "2026-06-02T03:00:00Z",
) -> dict[str, object]:
    sku_location_key = f"{sku_id}::{location_id}"
    return {
        "demand_history_key": f"{demand_date}::{sku_location_key}",
        "demand_date": demand_date,
        "sku_location_key": sku_location_key,
        "sku_id": sku_id,
        "location_id": location_id,
        "demand_qty": demand_qty,
        "fulfillment_record_count": 1,
        "latest_available_at": latest_available_at,
    }


def _rebuild(
    tmp_path: Path,
    rows: list[dict[str, object]],
    *,
    planning_cutoff: datetime = CUTOFF,
) -> R2RebuildResult:
    artifact = write_immutable_parquet(tmp_path / "exports", dataset=DATASET, rows=rows)
    irrelevant = write_immutable_parquet(
        tmp_path / "exports", dataset="gold.r2_inventory_position", rows=[]
    )
    return _rebuild_with_artifact(
        tmp_path,
        artifact,
        planning_cutoff=planning_cutoff,
        irrelevant=irrelevant,
    )


def _rebuild_with_artifact(
    tmp_path: Path,
    artifact: ContentAddressedArtifact,
    *,
    planning_cutoff: datetime = CUTOFF,
    irrelevant: ContentAddressedArtifact | None = None,
) -> R2RebuildResult:
    artifacts = (artifact,) if irrelevant is None else (irrelevant, artifact)
    return R2RebuildResult(
        planning_cutoff=planning_cutoff,
        source_manifest_sha256="b" * 64,
        source_manifest_file_sha256="c" * 64,
        semantic_manifest_sha256="a" * 64,
        landing=cast(R2LandingResult, object()),
        gold_artifacts=artifacts,
        gold_row_counts={DATASET: artifact.row_count},
        quality_counts={},
        summary_path=tmp_path / "manifest.json",
        database_path=tmp_path / "warehouse.duckdb",
        duration_seconds=0.1,
    )


def test_adapter_selects_exact_gold_reverifies_it_and_registers_stable_snapshot(
    tmp_path: Path,
) -> None:
    rebuild = _rebuild(
        tmp_path,
        [
            _row(
                sku_id="SKU-002",
                demand_date="2026-06-02",
                demand_qty=4,
                latest_available_at="2026-06-03T03:00:00Z",
            ),
            _row(sku_id="SKU-001", demand_date="2026-06-01", demand_qty=8),
        ],
    )

    snapshot = adapt_r2_demand_history(rebuild)
    source = rebuild.gold_artifact(DATASET)

    assert [row.sku_id for row in snapshot.observations] == ["SKU-001", "SKU-002"]
    assert snapshot.contract.source_dataset == DATASET
    assert snapshot.contract.semantic_manifest_sha256 == rebuild.semantic_manifest_sha256
    assert snapshot.contract.planning_cutoff == CUTOFF
    assert snapshot.contract.source_content_sha256 == source.content_sha256
    assert snapshot.contract.source_schema_sha256 == source.schema_sha256
    assert snapshot.contract.source_row_count == 2
    assert len(snapshot.contract.registration_sha256) == 64

    payload = snapshot.contract.model_dump()
    payload["source_row_count"] = 3
    with pytest.raises(ValueError, match="does not bind"):
        R3DemandSnapshotContract.model_validate(payload)


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([_row(demand_qty=-1)], "nonnegative integer"),
        ([_row(latest_available_at="2026-06-02T03:00:00")], "timezone-aware"),
        ([_row(latest_available_at="2026-07-01T03:00:00Z")], "late-arriving"),
        (
            [
                _row(
                    demand_date="2026-07-01",
                    latest_available_at="2026-07-02T03:00:00Z",
                )
            ],
            "late-arriving",
        ),
        ([_row(demand_date="2026-02-30")], "valid calendar date"),
        ([_row(), _row()], "duplicate sku-location-date"),
    ],
)
def test_malformed_future_late_and_duplicate_rows_fail_closed(
    tmp_path: Path,
    rows: list[dict[str, object]],
    message: str,
) -> None:
    rebuild = _rebuild(tmp_path, rows)

    with pytest.raises(R3GoldAdapterError, match=message):
        adapt_r2_demand_history(rebuild)


def test_corrupt_content_schema_and_row_count_fail_closed(tmp_path: Path) -> None:
    content_rebuild = _rebuild(tmp_path / "content", [_row()])
    content_artifact = content_rebuild.gold_artifact(DATASET)
    pq.write_table(pa.Table.from_pylist([_row(demand_qty=99)]), content_artifact.path)
    with pytest.raises(R3GoldAdapterError, match="content hash"):
        adapt_r2_demand_history(content_rebuild)

    schema_rebuild = _rebuild(tmp_path / "schema", [_row()])
    schema_artifact = schema_rebuild.gold_artifact(DATASET)
    changed = pq.read_table(schema_artifact.path).set_column(
        5,
        "demand_qty",
        pa.array([8], type=pa.int32()),
    )
    pq.write_table(changed, schema_artifact.path)
    with pytest.raises(R3GoldAdapterError, match="schema hash"):
        adapt_r2_demand_history(schema_rebuild)

    count_rebuild = _rebuild(tmp_path / "count", [_row()])
    artifact = count_rebuild.gold_artifact(DATASET)
    wrong_count = replace(artifact, row_count=2)
    count_rebuild = replace(
        count_rebuild,
        gold_artifacts=(wrong_count,),
        gold_row_counts={DATASET: 2},
    )
    with pytest.raises(R3GoldAdapterError, match="row count"):
        adapt_r2_demand_history(count_rebuild)


def test_naive_rebuild_cutoff_and_missing_exact_dataset_fail_closed(tmp_path: Path) -> None:
    rebuild = _rebuild(tmp_path / "naive", [_row()])
    naive = replace(rebuild, planning_cutoff=datetime(2026, 6, 30))
    with pytest.raises(R3GoldAdapterError, match="timezone-aware"):
        adapt_r2_demand_history(naive)

    artifact = rebuild.gold_artifact(DATASET)
    missing = replace(rebuild, gold_artifacts=(replace(artifact, dataset="gold.other"),))
    with pytest.raises(R3GoldAdapterError, match="missing required Gold dataset"):
        adapt_r2_demand_history(missing)
