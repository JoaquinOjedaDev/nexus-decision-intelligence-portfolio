from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
from pytest import MonkeyPatch
from typer.testing import CliRunner

import nexus.r2 as r2_module
from nexus.cli import app
from nexus.r2 import R2VerificationResult
from nexus.simulation import generate_r2_smoke_fixture

runner = CliRunner()
ROOT = Path(__file__).resolve().parents[1]


def _expected_r2() -> dict[str, object]:
    return json.loads((ROOT / "fixtures" / "r2" / "expected.json").read_text(encoding="utf-8"))


def test_checked_in_expected_fixture_matches_release_gate_hash_pins() -> None:
    expected = _expected_r2()

    assert expected["source_manifest_sha256"] == r2_module._EXPECTED_SOURCE_MANIFEST_SHA256
    assert (
        expected["source_manifest_file_sha256"] == r2_module._EXPECTED_SOURCE_MANIFEST_FILE_SHA256
    )
    assert expected["gold_hashes"]["early"] == r2_module._EXPECTED_EARLY_GOLD_HASHES
    assert expected["gold_hashes"]["final"] == r2_module._EXPECTED_FINAL_GOLD_HASHES


def test_r2_generate_command_writes_a_bound_source_package(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"

    result = runner.invoke(
        app,
        ["r2", "generate", "--output-dir", str(source_dir), "--seed", "20260718"],
    )

    assert result.exit_code == 0, result.stdout
    assert "NEXUS R2A source package" in result.stdout
    assert "03e32a3f19783913" in result.stdout
    manifest = json.loads((source_dir / "source-manifest.json").read_text(encoding="utf-8"))
    assert manifest["profile"] == "smoke"
    assert manifest["seed"] == 20260718
    assert len(manifest["sources"]) == 7


def test_r2_rebuild_command_executes_one_dbt_backed_point_in_time_build(
    tmp_path: Path,
) -> None:
    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    artifact_dir = tmp_path / "rebuild"
    expected = _expected_r2()
    command = [
        "r2",
        "rebuild",
        "--source-dir",
        str(fixture.source_dir),
        "--artifact-dir",
        str(artifact_dir),
    ]

    result = runner.invoke(app, command)

    assert result.exit_code == 0, result.stdout
    assert "NEXUS R2A trustworthy-data rebuild" in result.stdout
    assert "quarantine=4" in result.stdout
    summaries = list((artifact_dir / "manifests").glob("*.json"))
    assert len(summaries) == 1
    summary = json.loads(summaries[0].read_text(encoding="utf-8"))
    assert summary["status"] == "PASS"
    assert summary["release"] == "R2A"
    assert summary["planning_cutoff"] == "2026-06-30T00:00:00Z"
    assert (
        summary["source_manifest_sha256"]
        == fixture.manifest_sha256
        == expected["source_manifest_sha256"]
    )
    assert (
        summary["source_manifest_file_sha256"]
        == fixture.manifest_file_sha256
        == expected["source_manifest_file_sha256"]
    )
    assert summary["quality_counts"] == {
        "chain_deferred_inventory_adjustments": 0,
        "dbt_gold_products": 5,
        "deferred_inventory_adjustments": 1,
        "effective_inventory_adjustments": 3,
        "fulfillment_freshness_lag_seconds": 75600,
        "quarantined_records": 4,
        "superseded_inventory_adjustments": 1,
        "total_demand_qty": 1112,
        "total_on_hand_qty": 178,
        "total_on_order_qty": 240,
        "validated_inventory_adjustments": 5,
    }
    totals = expected["network_totals_final"]
    assert summary["quality_counts"]["total_demand_qty"] == totals["demand_qty"]
    assert summary["quality_counts"]["total_on_hand_qty"] == totals["on_hand_qty"]
    assert summary["quality_counts"]["total_on_order_qty"] == totals["on_order_qty"]
    assert summary["watermarks"] == {
        "latest_visible_adjustment_ingested_at": "2026-06-25T10:00:00Z",
        "latest_visible_fulfillment_available_at": "2026-06-29T03:00:00Z",
    }
    gold = {item["dataset"]: item for item in summary["gold"]}
    assert all(len(item["schema_sha256"]) == 64 for item in gold.values())
    assert {dataset: item["content_sha256"] for dataset, item in gold.items()} == expected[
        "gold_hashes"
    ]["final"]

    inventory_path = next(
        (artifact_dir / "exports" / "gold" / "r2_inventory_position").glob("*.parquet")
    )
    inventory = {row["sku_location_key"]: row for row in pq.read_table(inventory_path).to_pylist()}
    assert inventory["SKU-001::DC-ARG-01"]["adjustment_qty"] == 1
    assert inventory["SKU-001::DC-ARG-01"]["on_hand_qty"] >= 0

    summary_bytes = summaries[0].read_bytes()
    parquet_state = {
        path.relative_to(artifact_dir): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in artifact_dir.rglob("*.parquet")
    }
    repeated = runner.invoke(app, command)

    assert repeated.exit_code == 0, repeated.stdout
    assert list((artifact_dir / "manifests").glob("*.json")) == summaries
    assert summaries[0].read_bytes() == summary_bytes
    assert {
        path.relative_to(artifact_dir): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in artifact_dir.rglob("*.parquet")
    } == parquet_state

    permuted_source = r2_module._permuted_source(
        fixture.source_dir,
        tmp_path / "permuted-source",
    )
    permuted = r2_module.run_r2_rebuild(
        project_root=ROOT,
        source_dir=permuted_source,
        artifact_dir=artifact_dir,
        planning_cutoff=fixture.manifest.planning_cutoff,
    )
    assert {
        artifact.dataset: artifact.content_sha256 for artifact in permuted.gold_artifacts
    } == expected["gold_hashes"]["final"]
    assert (
        len(list((artifact_dir / "lake").rglob("validated/inventory_adjustments/*.parquet"))) == 2
    )


def test_r2_verify_command_reports_gate_results_without_repeating_four_builds(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    observed_artifact_dirs: list[Path] = []

    def passing_verify(*, project_root: Path, artifact_dir: Path) -> list[R2VerificationResult]:
        assert project_root.is_dir()
        observed_artifact_dirs.append(artifact_dir)
        return [R2VerificationResult("bounded replay", True, "hashes match")]

    monkeypatch.setattr("nexus.r2.verify_r2", passing_verify)
    artifact_dir = tmp_path / "verification"
    passed = runner.invoke(
        app,
        ["r2", "verify", "--artifact-dir", str(artifact_dir)],
    )

    assert passed.exit_code == 0
    assert "NEXUS R2A verification" in passed.stdout
    assert "bounded replay" in passed.stdout
    assert "PASS" in passed.stdout
    assert observed_artifact_dirs == [artifact_dir]

    def failing_verify(*, project_root: Path, artifact_dir: Path) -> list[R2VerificationResult]:
        del project_root, artifact_dir
        return [R2VerificationResult("bounded replay", False, "hash mismatch")]

    monkeypatch.setattr("nexus.r2.verify_r2", failing_verify)
    failed = runner.invoke(
        app,
        ["r2", "verify", "--artifact-dir", str(tmp_path / "failed")],
    )

    assert failed.exit_code == 1
    assert "FAIL" in failed.stdout
    assert "hash mismatch" in failed.stdout
