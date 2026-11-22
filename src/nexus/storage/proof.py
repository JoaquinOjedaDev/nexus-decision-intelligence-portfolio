"""Deterministic Parquet + DuckDB proof used by the R0 laptop gate."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True)
class StorageProofResult:
    """Evidence emitted by the storage proof."""

    engine: str
    raw_rows: int
    curated_rows: int
    raw_content_sha256: str
    curated_content_sha256: str
    database_path: str
    raw_path: str
    curated_path: str
    manifest_path: str


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _source_rows() -> list[dict[str, Any]]:
    return [
        {
            "event_id": "evt-0001",
            "sku_id": "sku-001",
            "location_id": "loc-01",
            "delta_qty": 100,
            "reason": "opening_balance",
            "event_time": "2026-01-01T09:00:00+00:00",
            "ingested_at": "2026-01-01T09:00:03+00:00",
        },
        {
            "event_id": "evt-0002",
            "sku_id": "sku-001",
            "location_id": "loc-01",
            "delta_qty": -4,
            "reason": "sale",
            "event_time": "2026-01-01T10:00:00+00:00",
            "ingested_at": "2026-01-01T10:00:02+00:00",
        },
        {
            "event_id": "evt-0003",
            "sku_id": "sku-001",
            "location_id": "loc-01",
            "delta_qty": 12,
            "reason": "receipt",
            "event_time": "2026-01-01T11:00:00+00:00",
            "ingested_at": "2026-01-01T11:00:04+00:00",
        },
        {
            "event_id": "evt-0004",
            "sku_id": "sku-002",
            "location_id": "loc-01",
            "delta_qty": 40,
            "reason": "opening_balance",
            "event_time": "2026-01-01T09:00:00+00:00",
            "ingested_at": "2026-01-01T09:00:03+00:00",
        },
        {
            "event_id": "evt-0005",
            "sku_id": "sku-002",
            "location_id": "loc-01",
            "delta_qty": -3,
            "reason": "sale",
            "event_time": "2026-01-01T10:30:00+00:00",
            "ingested_at": "2026-01-01T10:30:02+00:00",
        },
    ]


def run_storage_proof(artifact_root: Path) -> StorageProofResult:
    """Write raw Parquet, curate it with DuckDB, and emit content-addressed evidence."""

    artifact_root = artifact_root.resolve()
    raw_dir = artifact_root / "raw"
    curated_dir = artifact_root / "curated"
    raw_dir.mkdir(parents=True, exist_ok=True)
    curated_dir.mkdir(parents=True, exist_ok=True)

    raw_path = raw_dir / "inventory_adjustments.parquet"
    curated_path = curated_dir / "inventory_position.parquet"
    database_path = artifact_root / "storage_proof.duckdb"
    manifest_path = artifact_root / "storage-proof-manifest.json"

    source_rows = _source_rows()
    table = pa.Table.from_pylist(source_rows)
    pq.write_table(table, raw_path, compression="zstd", version="2.6", use_dictionary=True)

    raw_sql_path = raw_path.as_posix().replace("'", "''")
    curated_sql_path = curated_path.as_posix().replace("'", "''")
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE raw_inventory_adjustments AS
            SELECT
                event_id::VARCHAR AS event_id,
                sku_id::VARCHAR AS sku_id,
                location_id::VARCHAR AS location_id,
                delta_qty::BIGINT AS delta_qty,
                reason::VARCHAR AS reason,
                event_time::TIMESTAMPTZ AS event_time,
                ingested_at::TIMESTAMPTZ AS ingested_at
            FROM read_parquet('{raw_sql_path}')
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE TABLE gold_inventory_position AS
            SELECT
                sku_id,
                location_id,
                SUM(delta_qty)::BIGINT AS on_hand_qty,
                COUNT(*)::BIGINT AS event_count,
                MAX(event_time)::TIMESTAMPTZ AS as_of_event_time
            FROM raw_inventory_adjustments
            GROUP BY sku_id, location_id
            ORDER BY sku_id, location_id
            """
        )
        connection.execute(
            f"COPY gold_inventory_position TO '{curated_sql_path}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        curated_rows = connection.execute(
            """
            SELECT sku_id, location_id, on_hand_qty, event_count, as_of_event_time
            FROM gold_inventory_position
            ORDER BY sku_id, location_id
            """
        ).fetchall()
        engine_row = connection.execute("SELECT version()").fetchone()
        if engine_row is None:  # pragma: no cover - DuckDB always returns one row
            raise RuntimeError("DuckDB did not return an engine version")
        engine = engine_row[0]
    finally:
        connection.close()

    raw_hash = _canonical_sha256(source_rows)
    curated_hash = _canonical_sha256(curated_rows)
    result = StorageProofResult(
        engine=str(engine),
        raw_rows=len(source_rows),
        curated_rows=len(curated_rows),
        raw_content_sha256=raw_hash,
        curated_content_sha256=curated_hash,
        database_path=str(database_path),
        raw_path=str(raw_path),
        curated_path=str(curated_path),
        manifest_path=str(manifest_path),
    )

    evidence = {
        "proof_version": "1.0.0",
        "snapshot_type": "parquet-content-sha256",
        **asdict(result),
    }
    manifest_path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result
