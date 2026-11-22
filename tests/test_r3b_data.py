from __future__ import annotations

import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from nexus.data import R2CGeneratedProfile, load_r2c_profile
from nexus.modeling.r3_artifacts import write_immutable_json_manifest
from nexus.modeling.r3b_contracts import R3BConfig, load_r3b_config, r3b_config_sha256
from nexus.modeling.r3b_data import (
    R3BDataError,
    R3BDataSnapshot,
    canonical_rows_sha256,
    get_evaluation_truth_after_persistence,
    register_r2c_package,
)
from nexus.simulation import generate_r2c_profile, iter_r2c_dataset_rows
from nexus.storage import file_sha256

ROOT = Path(__file__).resolve().parents[1]
R2C_CONFIG_PATH = ROOT / "configs" / "profiles" / "smoke.yaml"
R3B_CONFIG_PATH = ROOT / "configs" / "modeling" / "r3b-smoke.yaml"
pytestmark = pytest.mark.r3b


@pytest.fixture(scope="module")
def r3b_config() -> R3BConfig:
    return load_r3b_config(R3B_CONFIG_PATH)


@pytest.fixture(scope="module")
def generated_package(tmp_path_factory: pytest.TempPathFactory) -> R2CGeneratedProfile:
    config = load_r2c_profile(R2C_CONFIG_PATH)
    return generate_r2c_profile(tmp_path_factory.mktemp("r3b-data") / "r2c", config=config)


@pytest.fixture(scope="module")
def snapshot(generated_package: R2CGeneratedProfile, r3b_config: R3BConfig) -> R3BDataSnapshot:
    return register_r2c_package(generated_package, config=r3b_config)


def _persist_forecast(
    output_dir: Path,
    keys: list[tuple[str, str, date, int]],
) -> Path:
    rows = [
        {
            "cold_start": False,
            "horizon_days": horizon,
            "location_id": location_id,
            "q025": 0,
            "q10": 1,
            "q50": 2,
            "q90": 3,
            "q975": 4,
            "sku_id": sku_id,
            "target_date": target_date,
        }
        for sku_id, location_id, target_date, horizon in keys
    ]
    return write_immutable_json_manifest(
        output_dir,
        dataset="r3b.test.forecast",
        manifest={
            "baseline": rows,
            "candidate": rows,
            "contract": "nexus.r3b.persisted-forecast-bundle.v1",
            "origin": datetime(2026, 1, 1, 4, tzinfo=UTC),
        },
    ).path


def test_registration_binds_the_exact_verified_r2c_package(
    generated_package: R2CGeneratedProfile,
    snapshot: R3BDataSnapshot,
    r3b_config: R3BConfig,
) -> None:
    registration = snapshot.registration
    manifest = generated_package.manifest

    assert snapshot.output_dir == generated_package.output_dir.resolve()
    assert snapshot.manifest == manifest
    assert snapshot.contract is registration
    assert registration.source_profile == "smoke"
    assert registration.r3b_config_sha256 == r3b_config_sha256(r3b_config)
    assert registration.profile_contract_sha256 == manifest.profile_contract_sha256
    assert registration.dataset_semantic_sha256 == manifest.dataset_semantic_sha256
    assert registration.manifest_file_sha256 == generated_package.manifest_file_sha256
    assert registration.manifest_file_sha256 == file_sha256(generated_package.manifest_path)
    assert registration.generator_code_sha256 == file_sha256(
        ROOT / "src" / "nexus" / "simulation" / "representative_supply_network.py"
    )
    assert registration.supplier_row_count == manifest.controls.supplier_rows
    assert registration.product_location_row_count == manifest.controls.product_location_rows
    assert registration.demand_row_count == manifest.controls.demand_rows
    assert registration.lead_time_row_count == manifest.controls.lead_time_rows
    assert tuple(shard.identity_row() for shard in registration.shards) == tuple(
        shard.model_dump(mode="json") for shard in manifest.shards
    )

    repeated = register_r2c_package(generated_package.output_dir, config=r3b_config)
    assert repeated.registration == registration
    identity = registration.model_dump(mode="python", exclude={"registration_sha256"})
    assert registration.registration_sha256 == canonical_rows_sha256(
        [registration._identity_row(identity)]
    )


def test_registration_fails_closed_when_a_declared_shard_is_tampered(
    generated_package: R2CGeneratedProfile,
    r3b_config: R3BConfig,
    tmp_path: Path,
) -> None:
    copied_package = tmp_path / "tampered-r2c"
    shutil.copytree(generated_package.output_dir, copied_package)
    shard_path = copied_package / generated_package.manifest.shards[0].relative_path
    payload = bytearray(shard_path.read_bytes())
    payload[len(payload) // 2] ^= 1
    shard_path.write_bytes(payload)

    with pytest.raises(R3BDataError, match="R2C package verification failed"):
        register_r2c_package(copied_package, config=r3b_config)


def test_truth_access_requires_a_valid_persistence_receipt_and_exact_unique_keys(
    snapshot: R3BDataSnapshot,
    tmp_path: Path,
) -> None:
    raw = next(
        row
        for row in iter_r2c_dataset_rows(
            snapshot.output_dir,
            dataset="representative.demand_history",
            verified_manifest=snapshot.manifest,
        )
        if cast(datetime, row["available_at"]) <= snapshot.manifest.generated_through
    )
    key = (
        cast(str, raw["sku_id"]),
        cast(str, raw["location_id"]),
        cast(date, raw["demand_date"]),
        7,
    )

    with pytest.raises(R3BDataError, match="persisted regular file"):
        get_evaluation_truth_after_persistence(
            snapshot,
            forecast_artifact_path=tmp_path / "missing.json",
        )
    with pytest.raises(R3BDataError, match="nonempty"):
        get_evaluation_truth_after_persistence(
            snapshot,
            forecast_artifact_path=_persist_forecast(tmp_path / "empty", []),
        )
    with pytest.raises(R3BDataError, match="unique"):
        get_evaluation_truth_after_persistence(
            snapshot,
            forecast_artifact_path=_persist_forecast(tmp_path / "duplicates", [key, key]),
        )
    with pytest.raises(R3BDataError, match="undeclared horizon"):
        get_evaluation_truth_after_persistence(
            snapshot,
            forecast_artifact_path=_persist_forecast(tmp_path / "horizon", [(*key[:3], 21)]),
        )

    artifact_path = _persist_forecast(tmp_path / "valid", [key])
    bundle = get_evaluation_truth_after_persistence(
        snapshot,
        forecast_artifact_path=artifact_path,
    )
    assert bundle.forecast_artifact_sha256 == artifact_path.stem
    assert bundle.forecast_artifact_file_sha256 == file_sha256(artifact_path)
    assert bundle.forecast_artifact_path == artifact_path.resolve()
    assert bundle.baseline == bundle.candidate
    assert bundle.candidate[0].forecast_key == key
    assert len(bundle) == 1
    assert bundle[0].forecast_key == key
    assert bundle[0].observed_sales_qty == raw["observed_sales_qty"]
    assert bundle[0].synthetic_latent_demand_qty == raw["synthetic_latent_demand_qty"]
    assert bundle[0].synthetic_cohort == raw["synthetic_cohort"]
    assert bundle[0].label_available_at == raw["available_at"]


def test_truth_access_fails_when_any_persisted_key_is_missing_or_not_yet_mature(
    snapshot: R3BDataSnapshot,
    tmp_path: Path,
) -> None:
    raw = next(
        row
        for row in iter_r2c_dataset_rows(
            snapshot.output_dir,
            dataset="representative.demand_history",
            verified_manifest=snapshot.manifest,
        )
        if cast(datetime, row["available_at"])
        > datetime.combine(cast(date, row["demand_date"]), datetime.min.time(), tzinfo=UTC)
    )
    key = (
        cast(str, raw["sku_id"]),
        cast(str, raw["location_id"]),
        cast(date, raw["demand_date"]),
        14,
    )
    available_at = cast(datetime, raw["available_at"])
    valid_path = _persist_forecast(tmp_path / "valid", [key])

    with pytest.raises(R3BDataError, match="complete mature evaluation truth"):
        get_evaluation_truth_after_persistence(
            snapshot,
            forecast_artifact_path=valid_path,
            evaluation_cutoff=available_at - timedelta(microseconds=1),
        )
    with pytest.raises(R3BDataError, match="complete mature evaluation truth"):
        get_evaluation_truth_after_persistence(
            snapshot,
            forecast_artifact_path=_persist_forecast(
                tmp_path / "missing",
                [
                    (
                        key[0],
                        key[1],
                        snapshot.manifest.end_date + timedelta(days=1),
                        key[3],
                    )
                ],
            ),
        )
    with pytest.raises(R3BDataError, match="timezone-aware"):
        get_evaluation_truth_after_persistence(
            snapshot,
            forecast_artifact_path=valid_path,
            evaluation_cutoff=datetime(2026, 1, 1),
        )

    at_boundary = get_evaluation_truth_after_persistence(
        snapshot,
        forecast_artifact_path=valid_path,
        evaluation_cutoff=available_at,
    )
    assert at_boundary[0].label_available_at == available_at


def test_truth_access_rejects_tampered_or_unpaired_forecast_bytes(
    snapshot: R3BDataSnapshot,
    tmp_path: Path,
) -> None:
    raw = next(
        row
        for row in iter_r2c_dataset_rows(
            snapshot.output_dir,
            dataset="representative.demand_history",
            verified_manifest=snapshot.manifest,
        )
        if cast(datetime, row["available_at"]) <= snapshot.manifest.generated_through
    )
    key = (
        cast(str, raw["sku_id"]),
        cast(str, raw["location_id"]),
        cast(date, raw["demand_date"]),
        7,
    )
    tampered = _persist_forecast(tmp_path / "tampered", [key])
    tampered_payload = tampered.read_text(encoding="utf-8").replace('"q50":2', '"q50":2.5', 1)
    tampered.write_text(tampered_payload, encoding="utf-8")
    with pytest.raises(R3BDataError, match="content hash"):
        get_evaluation_truth_after_persistence(snapshot, forecast_artifact_path=tampered)

    row = {
        "cold_start": False,
        "horizon_days": key[3],
        "location_id": key[1],
        "q025": 0,
        "q10": 1,
        "q50": 2,
        "q90": 3,
        "q975": 4,
        "sku_id": key[0],
        "target_date": key[2],
    }
    unpaired = write_immutable_json_manifest(
        tmp_path / "unpaired",
        dataset="r3b.test.forecast",
        manifest={
            "baseline": [],
            "candidate": [row],
            "contract": "nexus.r3b.persisted-forecast-bundle.v1",
            "origin": datetime(2026, 1, 1, 4, tzinfo=UTC),
        },
    ).path
    with pytest.raises(R3BDataError, match="keys must be paired"):
        get_evaluation_truth_after_persistence(snapshot, forecast_artifact_path=unpaired)
