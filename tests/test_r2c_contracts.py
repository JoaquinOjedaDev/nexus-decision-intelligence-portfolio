from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from nexus.data import R2CProfileConfig, R2CShard, load_r2c_profile
from nexus.r2c import verify_r2c
from nexus.simulation import (
    R2CInsufficientDiskError,
    R2CProfileOptInRequired,
    generate_r2c_profile,
    plan_r2c_profile,
    require_r2c_free_disk,
)

ROOT = Path(__file__).resolve().parents[1]


def _payload(profile: str = "smoke") -> dict[str, object]:
    return load_r2c_profile(ROOT / "configs" / "profiles" / f"{profile}.yaml").model_dump(
        mode="python"
    )


def test_profiles_have_explicit_cartesian_cardinality_and_safe_scale_preflight() -> None:
    smoke = load_r2c_profile(ROOT / "configs" / "profiles" / "smoke.yaml")
    demo = load_r2c_profile(ROOT / "configs" / "profiles" / "demo.yaml")
    full = load_r2c_profile(ROOT / "configs" / "profiles" / "full.yaml")

    assert (smoke.series_count, plan_r2c_profile(smoke).demand_rows) == (40, 3360)
    assert (demo.series_count, plan_r2c_profile(demo).demand_rows) == (500, 365000)
    full_plan = plan_r2c_profile(full)
    assert (full_plan.series_count, full_plan.demand_rows) == (30490, 59181090)
    assert full_plan.requires_explicit_opt_in is True
    assert full_plan.executable_by_default is False
    assert full_plan.minimum_free_disk_gib > full_plan.estimated_uncompressed_gib


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("series_count", 39, "series_count must equal"),
        ("forecast_horizons_days", (7, 14), "must be frozen"),
        ("cold_start_history_days", 27, "at least the longest"),
        ("stress_days", 29, "cannot exceed holdout"),
    ],
)
def test_profile_contract_rejects_ambiguous_dimensions_and_windows(
    field: str,
    value: object,
    message: str,
) -> None:
    payload = _payload()
    payload[field] = value
    with pytest.raises(ValidationError, match=message):
        R2CProfileConfig.model_validate(payload)


def test_profile_contract_rejects_inconsistent_cohorts_and_opt_in() -> None:
    payload = _payload()
    cohorts = dict(payload["cohort_counts"])  # type: ignore[arg-type]
    cohorts["smooth"] = 13
    payload["cohort_counts"] = cohorts
    with pytest.raises(ValidationError, match="cohort_counts must sum"):
        R2CProfileConfig.model_validate(payload)

    full_payload = _payload("full")
    full_payload["requires_explicit_opt_in"] = False
    with pytest.raises(ValidationError, match="must require explicit opt-in"):
        R2CProfileConfig.model_validate(full_payload)


def test_full_generation_fails_before_writing_without_explicit_opt_in(tmp_path: Path) -> None:
    full = load_r2c_profile(ROOT / "configs" / "profiles" / "full.yaml")
    with pytest.raises(R2CProfileOptInRequired, match="scale plan"):
        generate_r2c_profile(tmp_path / "full", config=full)
    assert not (tmp_path / "full").exists()

    with pytest.raises(R2CProfileOptInRequired, match="scale plan"):
        verify_r2c(artifact_root=tmp_path / "full-verification", config=full)
    assert not (tmp_path / "full-verification").exists()


def test_a_large_profile_cannot_bypass_opt_in_by_using_the_smoke_name() -> None:
    payload = _payload()
    payload.update(
        {
            "sku_count": 1000,
            "location_count": 1000,
            "series_count": 1_000_000,
        }
    )
    cohorts = dict(payload["cohort_counts"])  # type: ignore[arg-type]
    cohorts.update(
        {
            "smooth": 350000,
            "intermittent": 250000,
            "erratic": 200000,
            "lumpy": 100000,
            "cold_start": 100000,
        }
    )
    payload["cohort_counts"] = cohorts
    with pytest.raises(ValidationError, match="dimensions and cohorts do not match"):
        R2CProfileConfig.model_validate(payload)


def test_full_preflight_rejects_insufficient_disk_before_writing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    full = load_r2c_profile(ROOT / "configs" / "profiles" / "full.yaml")
    monkeypatch.setattr(
        "nexus.simulation.representative_supply_network.shutil.disk_usage",
        lambda _: SimpleNamespace(free=1024),
    )
    with pytest.raises(R2CInsufficientDiskError, match="insufficient free disk"):
        require_r2c_free_disk(tmp_path / "full", config=full, copies=2)

    generated_root = tmp_path / "generated-full"
    with pytest.raises(R2CInsufficientDiskError, match="insufficient free disk"):
        generate_r2c_profile(generated_root, config=full, allow_scale=True)
    assert not generated_root.exists()

    verification_root = tmp_path / "verified-full"
    with pytest.raises(R2CInsufficientDiskError, match="insufficient free disk"):
        verify_r2c(artifact_root=verification_root, config=full, allow_scale=True)
    assert not verification_root.exists()


def test_shard_contract_rejects_host_paths_and_traversal() -> None:
    base = {
        "dataset": "representative.suppliers",
        "partition": "all",
        "row_count": 1,
        "semantic_sha256": "a" * 64,
        "schema_sha256": "b" * 64,
        "file_sha256": "c" * 64,
        "file_bytes": 1,
    }
    for unsafe in ("../escape.parquet", "C:\\host\\file.parquet", "/root/file.parquet"):
        with pytest.raises(ValidationError, match="safe POSIX-relative"):
            R2CShard.model_validate({**base, "relative_path": unsafe})
