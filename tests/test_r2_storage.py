from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import nexus.r2 as r2_module
from nexus.storage import ArtifactConflictError, write_immutable_parquet


def test_existing_content_address_rejects_a_different_declared_schema(
    tmp_path: Path,
) -> None:
    rows = [{"value": 1}]
    int32_schema = pa.schema([pa.field("value", pa.int32(), nullable=False)])
    int64_schema = pa.schema([pa.field("value", pa.int64(), nullable=False)])
    first = write_immutable_parquet(
        tmp_path,
        dataset="validated.schema_guard",
        rows=rows,
        schema=int32_schema,
    )
    initial_bytes = first.path.read_bytes()
    initial_mtime = first.path.stat().st_mtime_ns

    with pytest.raises(ArtifactConflictError, match="incompatible schema"):
        write_immutable_parquet(
            tmp_path,
            dataset="validated.schema_guard",
            rows=rows,
            schema=int64_schema,
        )

    assert first.path.read_bytes() == initial_bytes
    assert first.path.stat().st_mtime_ns == initial_mtime
    assert pq.read_schema(first.path).equals(int32_schema, check_metadata=True)


@pytest.mark.parametrize(
    "dataset",
    [
        "../escape",
        r"validated..\escape",
        "validated.r2/escape",
        ".hidden",
        "",
    ],
)
def test_artifact_dataset_rejects_path_traversal_and_invalid_segments(
    tmp_path: Path,
    dataset: str,
) -> None:
    output_dir = tmp_path / "lake"
    escaped = tmp_path / "escape"

    with pytest.raises(ValueError, match="invalid artifact dataset name"):
        write_immutable_parquet(
            output_dir,
            dataset=dataset,
            rows=[],
        )

    assert not escaped.exists()
    assert not list(tmp_path.rglob("*.parquet"))


def test_empty_content_addressed_artifact_preserves_declared_schema(
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [
            pa.field("sku_location_key", pa.string(), nullable=False),
            pa.field("on_hand_qty", pa.int64(), nullable=False),
        ]
    )
    first = write_immutable_parquet(
        tmp_path,
        dataset="gold.empty_inventory",
        rows=[],
        schema=schema,
    )
    initial_mtime = first.path.stat().st_mtime_ns
    second = write_immutable_parquet(
        tmp_path,
        dataset="gold.empty_inventory",
        rows=[],
        schema=schema,
    )

    table = pq.read_table(first.path)
    assert table.num_rows == 0
    assert table.schema.equals(schema, check_metadata=True)
    assert len(first.schema_sha256) == 64
    assert second.schema_sha256 == first.schema_sha256
    assert second.path == first.path
    assert second.path.stat().st_mtime_ns == initial_mtime


def test_empty_gold_query_schema_survives_json_normalization_and_export(
    tmp_path: Path,
) -> None:
    with duckdb.connect(":memory:") as connection:
        connection.execute(
            """
            create table empty_gold (
                sku_location_key varchar not null,
                planning_cutoff timestamptz not null,
                on_hand_qty bigint not null
            )
            """
        )
        rows, schema = r2_module._table_rows(
            connection,
            table="empty_gold",
            order_by=("sku_location_key",),
        )

    artifact = write_immutable_parquet(
        tmp_path,
        dataset="gold.empty_query",
        rows=rows,
        schema=schema,
    )
    exported = pq.read_table(artifact.path)

    assert rows == []
    assert schema.names == ["sku_location_key", "planning_cutoff", "on_hand_qty"]
    assert schema.field("planning_cutoff").type == pa.string()
    assert exported.num_rows == 0
    assert exported.schema.equals(schema, check_metadata=True)
