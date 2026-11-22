"""Deterministic synthetic worlds used by executable release gates."""

from nexus.simulation.representative_supply_network import (
    R2C_MANIFEST_NAME,
    R2CInsufficientDiskError,
    R2CPackageIntegrityError,
    R2CProfileOptInRequired,
    generate_r2c_profile,
    iter_r2c_dataset_rows,
    iter_r2c_demand_snapshot,
    iter_r2c_lead_time_label_snapshot,
    plan_r2c_profile,
    require_r2c_free_disk,
    verify_r2c_package,
)
from nexus.simulation.supply_network import (
    R2_SMOKE_DAYS,
    R2_SMOKE_EARLY_CUTOFF,
    R2_SMOKE_PLANNING_CUTOFF,
    R2_SMOKE_SEED,
    generate_r2_smoke_fixture,
)

__all__ = [
    "R2C_MANIFEST_NAME",
    "R2_SMOKE_DAYS",
    "R2_SMOKE_EARLY_CUTOFF",
    "R2_SMOKE_PLANNING_CUTOFF",
    "R2_SMOKE_SEED",
    "R2CInsufficientDiskError",
    "R2CPackageIntegrityError",
    "R2CProfileOptInRequired",
    "generate_r2_smoke_fixture",
    "generate_r2c_profile",
    "iter_r2c_dataset_rows",
    "iter_r2c_demand_snapshot",
    "iter_r2c_lead_time_label_snapshot",
    "plan_r2c_profile",
    "require_r2c_free_disk",
    "verify_r2c_package",
]
