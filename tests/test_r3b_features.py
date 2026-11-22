from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import cast

import pytest

from nexus.data import R2CGeneratedProfile, load_r2c_profile
from nexus.modeling.r3b_contracts import R3BConfig, load_r3b_config
from nexus.modeling.r3b_data import R3BDataSnapshot, register_r2c_package
from nexus.modeling.r3b_features import (
    DEMAND_FEATURE_COLUMNS,
    FORBIDDEN_FEATURE_COLUMNS,
    LEAD_TIME_FEATURE_COLUMNS,
    R3BFeatureError,
    assert_allowed_feature_columns,
    build_demand_forecast_rows,
    build_demand_training_examples,
    build_lead_time_training_examples,
)
from nexus.simulation import (
    generate_r2c_profile,
    iter_r2c_dataset_rows,
    iter_r2c_demand_snapshot,
    iter_r2c_lead_time_label_snapshot,
)

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
    return generate_r2c_profile(tmp_path_factory.mktemp("r3b-features") / "r2c", config=config)


@pytest.fixture(scope="module")
def snapshot(generated_package: R2CGeneratedProfile, r3b_config: R3BConfig) -> R3BDataSnapshot:
    return register_r2c_package(generated_package, config=r3b_config)


def _origin(config: R3BConfig, role: str) -> datetime:
    return next(item.issue_time for item in config.evaluation.rolling_origins if item.role == role)


def test_feature_column_contract_is_exact_ordered_and_denylisted(r3b_config: R3BConfig) -> None:
    assert assert_allowed_feature_columns(
        DEMAND_FEATURE_COLUMNS,
        config=r3b_config,
        model_kind="demand",
    ) == tuple(r3b_config.feature_contract.allowed_demand_features)
    assert assert_allowed_feature_columns(
        LEAD_TIME_FEATURE_COLUMNS,
        config=r3b_config,
        model_kind="lead_time",
    ) == tuple(r3b_config.feature_contract.allowed_lead_time_features)
    assert not set(DEMAND_FEATURE_COLUMNS) & set(FORBIDDEN_FEATURE_COLUMNS)
    assert not set(LEAD_TIME_FEATURE_COLUMNS) & set(FORBIDDEN_FEATURE_COLUMNS)

    with pytest.raises(R3BFeatureError, match="frozen ordered contract"):
        assert_allowed_feature_columns(
            tuple(reversed(DEMAND_FEATURE_COLUMNS)),
            config=r3b_config,
            model_kind="demand",
        )
    with pytest.raises(R3BFeatureError, match="frozen ordered contract"):
        assert_allowed_feature_columns(
            (*DEMAND_FEATURE_COLUMNS[:-1], "synthetic_latent_demand_qty"),
            config=r3b_config,
            model_kind="demand",
        )
    with pytest.raises(R3BFeatureError, match="unique"):
        assert_allowed_feature_columns(
            (*DEMAND_FEATURE_COLUMNS[:-1], DEMAND_FEATURE_COLUMNS[0]),
            config=r3b_config,
            model_kind="demand",
        )


def test_demand_training_is_deterministic_point_in_time_and_label_separated(
    snapshot: R3BDataSnapshot,
    r3b_config: R3BConfig,
) -> None:
    final_origin = _origin(r3b_config, "final_stress")
    dataset = build_demand_training_examples(
        snapshot,
        config=r3b_config,
        outer_fit_origin=final_origin,
    )
    repeated = build_demand_training_examples(
        snapshot,
        config=r3b_config,
        outer_fit_origin=final_origin,
    )
    visible_rows = list(
        iter_r2c_demand_snapshot(
            snapshot.output_dir,
            planning_cutoff=final_origin,
            verified_manifest=snapshot.manifest,
        )
    )
    source = {
        (
            cast(str, row["sku_id"]),
            cast(str, row["location_id"]),
            cast(date, row["demand_date"]),
        ): row
        for row in visible_rows
    }
    eligible = [
        row
        for row in visible_rows
        if cast(date, row["demand_date"]) < final_origin.date()
        and cast(bool, row["is_active"])
        and not cast(bool, row["stockout_censored"])
    ]
    expected_examples = sum(
        cast(date, row["demand_date"]) - timedelta(days=horizon - 1) >= snapshot.manifest.start_date
        for row in eligible
        for horizon in r3b_config.forecast_contract.horizons_days
    )

    assert dataset.feature_columns == DEMAND_FEATURE_COLUMNS
    assert dataset.examples
    assert dataset == repeated
    assert dataset.dataset_sha256 == repeated.dataset_sha256
    assert len(dataset.examples) == expected_examples
    assert dataset.excluded_inactive_labels == sum(
        cast(date, row["demand_date"]) < final_origin.date() and not cast(bool, row["is_active"])
        for row in visible_rows
    )
    assert dataset.excluded_censored_labels == sum(
        cast(date, row["demand_date"]) < final_origin.date()
        and cast(bool, row["is_active"])
        and cast(bool, row["stockout_censored"])
        for row in visible_rows
    )
    assert dataset.excluded_inactive_labels > 0
    assert dataset.excluded_censored_labels > 0

    for example in dataset.examples:
        features = example.features
        raw = source[(features.sku_id, features.location_id, features.target_date)]
        feature_artifact = features.artifact_row()
        feature_names = set(features.feature_columns)

        assert raw["is_active"] is True
        assert raw["stockout_censored"] is False
        assert example.target_observed_sales_qty == raw["observed_sales_qty"]
        assert example.label_available_at == raw["available_at"]
        assert example.label_available_at <= final_origin
        assert features.max_feature_available_at is None or (
            features.max_feature_available_at <= features.issue_time
        )
        assert features.target_date == features.issue_time.date() + timedelta(
            days=features.horizon_days - 1
        )
        assert len(features.feature_vector()) == len(features.feature_columns)
        assert feature_names == set(DEMAND_FEATURE_COLUMNS)
        assert not feature_names & set(FORBIDDEN_FEATURE_COLUMNS)
        assert not set(feature_artifact) & set(FORBIDDEN_FEATURE_COLUMNS)
        assert "target_observed_sales_qty" not in feature_artifact


def test_forecast_grid_excludes_preactivation_and_contains_no_label_fields(
    snapshot: R3BDataSnapshot,
    r3b_config: R3BConfig,
) -> None:
    issue_time = _origin(r3b_config, "diagnostic")
    rows = build_demand_forecast_rows(snapshot, config=r3b_config, issue_time=issue_time)
    product_locations = list(
        iter_r2c_dataset_rows(
            snapshot.output_dir,
            dataset="representative.product_locations",
            verified_manifest=snapshot.manifest,
        )
    )
    eligible = {
        (cast(str, row["sku_id"]), cast(str, row["location_id"]))
        for row in product_locations
        if cast(datetime, row["available_at"]) <= issue_time
        and cast(date, row["activation_date"]) <= issue_time.date()
    }

    assert eligible
    assert len(eligible) < len(product_locations)
    assert len(rows) == len(eligible) * len(r3b_config.forecast_contract.horizons_days)
    assert {(row.sku_id, row.location_id) for row in rows} == eligible
    assert (
        tuple(sorted(rows, key=lambda row: (row.sku_id, row.location_id, row.horizon_days))) == rows
    )
    for row in rows:
        artifact = row.artifact_row()
        assert not set(artifact) & set(FORBIDDEN_FEATURE_COLUMNS)
        assert "target_observed_sales_qty" not in artifact
        assert "synthetic_latent_demand_qty" not in artifact
        assert "synthetic_cohort" not in artifact


def test_late_arriving_demand_enters_features_only_at_its_availability_boundary(
    snapshot: R3BDataSnapshot,
    r3b_config: R3BConfig,
) -> None:
    raw_rows = list(
        iter_r2c_dataset_rows(
            snapshot.output_dir,
            dataset="representative.demand_history",
            verified_manifest=snapshot.manifest,
        )
    )
    products = {
        (cast(str, row["sku_id"]), cast(str, row["location_id"])): row
        for row in iter_r2c_dataset_rows(
            snapshot.output_dir,
            dataset="representative.product_locations",
            verified_manifest=snapshot.manifest,
        )
    }
    late = next(
        row
        for row in raw_rows
        if cast(bool, row["is_active"])
        and not cast(bool, row["stockout_censored"])
        and cast(datetime, row["available_at"]).date() > cast(date, row["demand_date"])
        and cast(datetime, row["available_at"]).time() != datetime.min.time()
    )
    series_key = cast(str, late["sku_id"]), cast(str, late["location_id"])
    available_at = cast(datetime, late["available_at"])
    at_boundary = datetime.combine(available_at.date(), time(hour=4), tzinfo=UTC)
    before = at_boundary - timedelta(days=1)
    product = products[series_key]
    assert cast(datetime, product["available_at"]) <= before
    assert cast(date, product["activation_date"]) <= before.date()
    assert available_at > before
    assert available_at <= at_boundary

    before_row = next(
        row
        for row in build_demand_forecast_rows(
            snapshot,
            config=r3b_config,
            issue_time=before,
        )
        if (row.sku_id, row.location_id, row.horizon_days) == (*series_key, 7)
    )
    at_boundary_row = next(
        row
        for row in build_demand_forecast_rows(
            snapshot,
            config=r3b_config,
            issue_time=at_boundary,
        )
        if (row.sku_id, row.location_id, row.horizon_days) == (*series_key, 7)
    )

    def expected_history_count(cutoff: datetime) -> int:
        return sum(
            (cast(str, row["sku_id"]), cast(str, row["location_id"])) == series_key
            and cast(date, row["demand_date"]) < cutoff.date()
            and cast(datetime, row["available_at"]) <= cutoff
            and cast(bool, row["is_active"])
            and not cast(bool, row["stockout_censored"])
            for row in raw_rows
        )

    assert before_row.history_count == expected_history_count(before)
    assert at_boundary_row.history_count == expected_history_count(at_boundary)
    assert at_boundary_row.history_count > before_row.history_count
    assert before_row.max_feature_available_at is None or (
        before_row.max_feature_available_at < available_at
    )
    assert at_boundary_row.max_feature_available_at == available_at


def test_lead_time_training_contains_only_mature_uncensored_examples(
    snapshot: R3BDataSnapshot,
    r3b_config: R3BConfig,
) -> None:
    origin = _origin(r3b_config, "final_stress")
    dataset = build_lead_time_training_examples(
        snapshot,
        config=r3b_config,
        origin_time=origin,
    )
    repeated = build_lead_time_training_examples(
        snapshot,
        config=r3b_config,
        origin_time=origin,
    )
    source_rows = list(
        iter_r2c_lead_time_label_snapshot(
            snapshot.output_dir,
            planning_cutoff=origin,
            verified_manifest=snapshot.manifest,
        )
    )
    source = {cast(str, row["order_id"]): row for row in source_rows}

    assert dataset.feature_columns == LEAD_TIME_FEATURE_COLUMNS
    assert dataset.examples
    assert dataset == repeated
    assert dataset.dataset_sha256 == repeated.dataset_sha256
    assert dataset.known_order_count == len(source_rows)
    assert dataset.excluded_right_censored_count == sum(
        cast(bool, row["right_censored"]) for row in source_rows
    )
    assert dataset.excluded_right_censored_count > 0
    assert len(dataset.examples) + dataset.excluded_right_censored_count == len(source_rows)

    for example in dataset.examples:
        features = example.features
        raw = source[features.order_id]
        model_features = dict(zip(features.feature_columns, features.feature_vector(), strict=True))

        assert raw["right_censored"] is False
        assert raw["actual_lead_time_days"] is not None
        assert raw["label_available_at"] is not None
        assert cast(datetime, raw["label_available_at"]) <= origin
        assert example.target_actual_lead_time_days == raw["actual_lead_time_days"]
        assert example.label_available_at == raw["label_available_at"]
        assert set(model_features) == set(LEAD_TIME_FEATURE_COLUMNS)
        assert not set(model_features) & set(FORBIDDEN_FEATURE_COLUMNS)
        assert features.ordered_at <= features.origin_time
        assert features.ordered_at <= features.ordered_available_at <= features.origin_time
        assert features.ordered_available_at == raw["ordered_available_at"]
