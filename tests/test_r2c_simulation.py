from __future__ import annotations

import json
import statistics
from collections import defaultdict
from datetime import date
from pathlib import Path

import pytest

import nexus.simulation.representative_supply_network as r2c_generator
from nexus.data import R2CProfileConfig, load_r2c_profile, r2c_profile_sha256
from nexus.data.r2c_models import canonical_r2c_sha256
from nexus.simulation import (
    R2CPackageIntegrityError,
    generate_r2c_profile,
    iter_r2c_dataset_rows,
    verify_r2c_package,
)

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = json.loads((ROOT / "fixtures" / "r2c" / "expected.json").read_text())


def _smoke() -> R2CProfileConfig:
    return load_r2c_profile(ROOT / "configs" / "profiles" / "smoke.yaml")


def test_generation_is_logically_invariant_to_batch_and_partition_execution_order(
    tmp_path: Path,
) -> None:
    config = _smoke()
    first = generate_r2c_profile(tmp_path / "first", config=config)
    second = generate_r2c_profile(
        tmp_path / "second",
        config=config,
        batch_rows_override=97,
        reverse_partition_order=True,
    )

    assert first.manifest.profile_contract_sha256 == EXPECTED["profile_contract_sha256"]
    assert first.manifest.dataset_semantic_sha256 == EXPECTED["dataset_semantic_sha256"]
    assert second.manifest.dataset_semantic_sha256 == first.manifest.dataset_semantic_sha256
    assert first.manifest.controls.model_dump(mode="json") == EXPECTED["controls"]
    assert second.manifest.controls == first.manifest.controls
    assert [(shard.dataset, shard.partition) for shard in first.manifest.shards] == sorted(
        (shard.dataset, shard.partition) for shard in first.manifest.shards
    )
    assert verify_r2c_package(first.output_dir, expected_config=config) == first.manifest


def test_generation_is_idempotent_and_a_seed_change_changes_semantics(tmp_path: Path) -> None:
    config = _smoke()
    first = generate_r2c_profile(tmp_path / "stable", config=config)
    mtimes = {
        path: path.stat().st_mtime_ns for path in first.output_dir.rglob("*") if path.is_file()
    }
    repeated = generate_r2c_profile(tmp_path / "stable", config=config)
    assert repeated.manifest == first.manifest
    assert {
        path: path.stat().st_mtime_ns for path in first.output_dir.rglob("*") if path.is_file()
    } == mtimes

    payload = config.model_dump(mode="python")
    payload["seed"] = config.seed + 1
    changed_config = R2CProfileConfig.model_validate(payload)
    changed = generate_r2c_profile(tmp_path / "changed", config=changed_config)
    assert r2c_profile_sha256(changed_config) != r2c_profile_sha256(config)
    assert changed.manifest.dataset_semantic_sha256 != first.manifest.dataset_semantic_sha256


def test_rectangular_panel_marks_prelaunch_rows_without_false_demand(tmp_path: Path) -> None:
    config = _smoke()
    generated = generate_r2c_profile(tmp_path / "profile", config=config)
    products = {
        (row["sku_id"], row["location_id"]): row
        for row in iter_r2c_dataset_rows(
            generated.output_dir,
            dataset="representative.product_locations",
        )
    }
    for product in products.values():
        activation_date = product["activation_date"]
        assert isinstance(activation_date, date)
        if product["synthetic_cohort"] == "cold_start":
            active_days = (config.end_date - activation_date).days + 1
            assert activation_date > config.start_date
            assert max(config.forecast_horizons_days) <= active_days
            assert active_days <= config.cold_start_history_days
        else:
            assert activation_date == config.start_date
    inactive_rows = 0
    active_positive_rows = 0
    for row in iter_r2c_dataset_rows(
        generated.output_dir,
        dataset="representative.demand_history",
    ):
        product = products[(row["sku_id"], row["location_id"])]
        activation_date = product["activation_date"]
        assert isinstance(activation_date, date)
        if row["demand_date"] < activation_date:
            inactive_rows += 1
            assert row["is_active"] is False
            assert row["synthetic_latent_demand_qty"] == 0
            assert row["observed_sales_qty"] == 0
        elif row["synthetic_latent_demand_qty"] > 0:
            active_positive_rows += 1
    assert inactive_rows > 0
    assert active_positive_rows > 0


def test_generation_rejects_a_nonpositive_batch_override(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="batch_rows_override must be positive"):
        generate_r2c_profile(tmp_path / "profile", config=_smoke(), batch_rows_override=0)


@pytest.mark.parametrize("precreate_empty", [False, True])
def test_failed_profile_generation_leaves_no_partial_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, precreate_empty: bool
) -> None:
    output = tmp_path / "profile"
    if precreate_empty:
        output.mkdir()
    original = r2c_generator._write_shard
    calls = 0

    def fail_after_first_shard(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected shard failure")
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(r2c_generator, "_write_shard", fail_after_first_shard)
        with pytest.raises(RuntimeError, match="injected shard failure"):
            generate_r2c_profile(output, config=_smoke())
    assert not output.exists() or not any(output.iterdir())
    assert not list(tmp_path.glob(".profile.stage-*"))
    assert verify_r2c_package(generate_r2c_profile(output, config=_smoke()).output_dir)


def test_demand_cohorts_match_the_canonical_adi_cv2_quadrants(tmp_path: Path) -> None:
    generated = generate_r2c_profile(tmp_path / "profile", config=_smoke())
    values: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    for row in iter_r2c_dataset_rows(
        generated.output_dir,
        dataset="representative.demand_history",
    ):
        if row["is_active"]:
            key = (row["synthetic_cohort"], row["sku_id"], row["location_id"])
            values[key].append(row["synthetic_latent_demand_qty"])

    for (cohort, _, _), series in values.items():
        if cohort == "cold_start":
            continue
        nonzero = [value for value in series if value > 0]
        adi = len(series) / len(nonzero)
        mean = statistics.fmean(nonzero)
        cv2 = statistics.pvariance(nonzero) / mean**2
        if cohort in {"smooth", "erratic"}:
            assert adi < 1.32
        else:
            assert adi >= 1.32
        if cohort in {"smooth", "intermittent"}:
            assert cv2 < 0.49
        else:
            assert cv2 >= 0.49


def test_package_verification_detects_corruption_and_orphan_shards(tmp_path: Path) -> None:
    first = generate_r2c_profile(tmp_path / "corrupt", config=_smoke())
    target = first.output_dir / first.manifest.shards[0].relative_path
    payload = bytearray(target.read_bytes())
    payload[len(payload) // 2] ^= 1
    target.write_bytes(payload)
    with pytest.raises(R2CPackageIntegrityError, match="physical shard hash mismatch"):
        verify_r2c_package(first.output_dir)

    clean = generate_r2c_profile(tmp_path / "orphan", config=_smoke())
    orphan = clean.output_dir / "undeclared.parquet"
    orphan.write_bytes(b"not parquet")
    with pytest.raises(R2CPackageIntegrityError, match="undeclared or missing"):
        verify_r2c_package(clean.output_dir)


def test_package_verification_recomputes_controls_and_file_sizes(tmp_path: Path) -> None:
    controls_package = generate_r2c_profile(tmp_path / "controls", config=_smoke())
    controls_manifest = json.loads(controls_package.manifest_path.read_text(encoding="utf-8"))
    controls_manifest["controls"]["demand_rows"] += 1
    controls_package.manifest_path.write_text(
        json.dumps(controls_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(R2CPackageIntegrityError, match="controls do not match rows"):
        verify_r2c_package(controls_package.output_dir)

    size_package = generate_r2c_profile(tmp_path / "size", config=_smoke())
    size_manifest = json.loads(size_package.manifest_path.read_text(encoding="utf-8"))
    size_manifest["shards"][0]["file_bytes"] += 1
    size_package.manifest_path.write_text(
        json.dumps(size_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(R2CPackageIntegrityError, match="physical shard hash mismatch"):
        verify_r2c_package(size_package.output_dir)


def test_package_verification_requires_every_declared_month_partition(tmp_path: Path) -> None:
    generated = generate_r2c_profile(tmp_path / "missing", config=_smoke())
    payload = json.loads(generated.manifest_path.read_text(encoding="utf-8"))
    removed = next(
        shard for shard in payload["shards"] if shard["dataset"] == "representative.demand_history"
    )
    payload["shards"].remove(removed)
    (generated.output_dir / removed["relative_path"]).unlink()
    identities = [
        {
            "dataset": shard["dataset"],
            "partition": shard["partition"],
            "row_count": shard["row_count"],
            "schema_sha256": shard["schema_sha256"],
            "semantic_sha256": shard["semantic_sha256"],
        }
        for shard in payload["shards"]
    ]
    payload["dataset_semantic_sha256"] = canonical_r2c_sha256(identities)
    generated.manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(R2CPackageIntegrityError, match="exact required dataset partitions"):
        verify_r2c_package(generated.output_dir)
