import json
from pathlib import Path

import duckdb

from nexus.storage import run_storage_proof


def test_storage_proof_is_deterministic(tmp_path: Path) -> None:
    first = run_storage_proof(tmp_path / "proof")
    second = run_storage_proof(tmp_path / "proof")
    assert first.raw_rows == 5
    assert first.curated_rows == 2
    assert first.raw_content_sha256 == second.raw_content_sha256
    assert first.curated_content_sha256 == second.curated_content_sha256


def test_storage_proof_reconciles_inventory(tmp_path: Path) -> None:
    result = run_storage_proof(tmp_path / "proof")
    connection = duckdb.connect(result.database_path, read_only=True)
    try:
        quantities = dict(
            connection.execute(
                "SELECT sku_id, on_hand_qty FROM gold_inventory_position ORDER BY sku_id"
            ).fetchall()
        )
    finally:
        connection.close()
    assert quantities == {"sku-001": 108, "sku-002": 37}


def test_storage_manifest_is_content_addressed(tmp_path: Path) -> None:
    result = run_storage_proof(tmp_path / "proof")
    manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
    assert manifest["snapshot_type"] == "parquet-content-sha256"
    assert manifest["curated_content_sha256"] == result.curated_content_sha256
