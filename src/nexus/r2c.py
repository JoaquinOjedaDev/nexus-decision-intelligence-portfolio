"""Executable R2C representative-profile verification gate."""

from __future__ import annotations

import json
import tempfile
import threading
import time
from datetime import UTC, date, datetime, timedelta
from datetime import time as datetime_time
from pathlib import Path
from typing import cast

import psutil

from nexus.data.r2c_models import R2CManifest, R2CProfileConfig, R2CVerificationResult
from nexus.simulation.representative_supply_network import (
    R2CProfileOptInRequired,
    generate_r2c_profile,
    iter_r2c_dataset_rows,
    iter_r2c_demand_snapshot,
    iter_r2c_lead_time_label_snapshot,
    plan_r2c_profile,
    require_r2c_free_disk,
    verify_r2c_package,
)


class _ProcessTreeSampler:
    """Poll this verifier and its children for a portable RSS high-water mark."""

    def __init__(self) -> None:
        self._root = psutil.Process()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="r2c-rss-sampler", daemon=True)
        self.peak_bytes = 0

    def _capture(self) -> None:
        processes = [self._root, *self._root.children(recursive=True)]
        rss = 0
        for process in processes:
            try:
                rss += process.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        self.peak_bytes = max(self.peak_bytes, rss)

    def _run(self) -> None:
        while not self._stop.wait(0.05):
            self._capture()

    def __enter__(self) -> _ProcessTreeSampler:
        self._capture()
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._capture()


def _child_identities() -> set[tuple[int, float]]:
    """Identify live descendants so cleanup can exclude pre-existing children."""

    identities: set[tuple[int, float]] = set()
    for process in psutil.Process().children(recursive=True):
        try:
            identities.add((process.pid, process.create_time()))
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
    return identities


def _artifact_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _point_in_time_labels_are_safe(
    package_dir: Path,
    config: R2CProfileConfig,
    verified_manifest: R2CManifest,
) -> bool:
    cutoff_day = config.end_date - timedelta(days=config.holdout_days // 2)
    cutoff = datetime.combine(cutoff_day, datetime_time(23, 59), tzinfo=UTC)
    raw_rows = {
        cast(str, row["order_id"]): row
        for row in iter_r2c_dataset_rows(
            package_dir,
            dataset="representative.lead_time_history",
            verified_manifest=verified_manifest,
        )
        if cast(datetime, row["ordered_available_at"]) <= cutoff
    }
    snapshot_rows = {
        cast(str, row["order_id"]): row
        for row in iter_r2c_lead_time_label_snapshot(
            package_dir,
            planning_cutoff=cutoff,
            verified_manifest=verified_manifest,
        )
    }
    if raw_rows.keys() != snapshot_rows.keys():
        return False

    mature = 0
    immature = 0
    for order_id, raw in raw_rows.items():
        snapshot = snapshot_rows[order_id]
        label_available_at = cast(datetime | None, raw["label_available_at"])
        if label_available_at is not None and label_available_at <= cutoff:
            mature += 1
            if (
                snapshot["actual_lead_time_days"] != raw["actual_lead_time_days"]
                or snapshot["completed_at"] != raw["completed_at"]
                or snapshot["label_available_at"] != raw["label_available_at"]
                or snapshot["received_qty"] != raw["received_qty"]
                or snapshot["receipt_count"] != raw["receipt_count"]
                or snapshot["delay_days"] != raw["delay_days"]
                or snapshot["is_late"] != raw["is_late"]
                or snapshot["right_censored"] is not False
            ):
                return False
        else:
            immature += 1
            if (
                snapshot["actual_lead_time_days"] is not None
                or snapshot["completed_at"] is not None
                or snapshot["label_available_at"] is not None
                or snapshot["received_qty"] != 0
                or snapshot["receipt_count"] != 0
                or snapshot["delay_days"] is not None
                or snapshot["is_late"] is not None
                or snapshot["status"] != "right_censored"
                or snapshot["right_censored"] is not True
            ):
                return False
    return mature > 0 and immature > 0


def _point_in_time_demand_is_safe(
    package_dir: Path,
    config: R2CProfileConfig,
    verified_manifest: R2CManifest,
) -> bool:
    cutoff_day = config.end_date - timedelta(days=config.holdout_days // 2)
    cutoff = datetime.combine(cutoff_day, datetime_time(23, 59), tzinfo=UTC)
    raw_iterator = iter_r2c_dataset_rows(
        package_dir,
        dataset="representative.demand_history",
        verified_manifest=verified_manifest,
    )
    snapshot_iterator = iter_r2c_demand_snapshot(
        package_dir,
        planning_cutoff=cutoff,
        verified_manifest=verified_manifest,
    )
    snapshot_row = next(snapshot_iterator, None)
    included = 0
    late_excluded = 0
    for raw in raw_iterator:
        available_at = cast(datetime, raw["available_at"])
        demand_date = cast(date, raw["demand_date"])
        if available_at <= cutoff:
            if snapshot_row != raw:
                return False
            included += 1
            snapshot_row = next(snapshot_iterator, None)
        elif demand_date <= cutoff.date():
            late_excluded += 1
    return included > 0 and late_excluded > 0 and snapshot_row is None


def verify_r2c(
    *,
    artifact_root: Path,
    config: R2CProfileConfig,
    allow_scale: bool = False,
) -> R2CVerificationResult:
    """Prove deterministic partitioning, representative controls, PIT labels, and budgets."""

    if config.requires_explicit_opt_in and not allow_scale:
        raise R2CProfileOptInRequired(
            "the full profile is a scale plan; pass allow_scale only after reviewing its preflight"
        )
    if config.requires_explicit_opt_in:
        require_r2c_free_disk(artifact_root, config=config, copies=2)
    started = time.perf_counter()
    artifact_root = artifact_root.resolve()
    executions_root = artifact_root / "executions"
    executions_root.mkdir(parents=True, exist_ok=True)
    execution_root = Path(tempfile.mkdtemp(prefix=f"{config.name}-", dir=executions_root))
    baseline_children = _child_identities()
    with _ProcessTreeSampler() as sampler:
        first = generate_r2c_profile(
            execution_root / "run-a",
            config=config,
            allow_scale=allow_scale,
        )
        alternate_batch_rows = max(1, config.write_batch_rows // 3)
        second = generate_r2c_profile(
            execution_root / "run-b-reverse",
            config=config,
            allow_scale=allow_scale,
            batch_rows_override=alternate_batch_rows,
            reverse_partition_order=True,
        )
        verified_first = verify_r2c_package(first.output_dir, expected_config=config)
        verified_second = verify_r2c_package(second.output_dir, expected_config=config)
        plan = plan_r2c_profile(config)
        controls = verified_first.controls
        checks = {
            "profile_contract_bound": (
                first.manifest.profile_contract_sha256
                == second.manifest.profile_contract_sha256
                == verified_first.profile_contract_sha256
                == verified_second.profile_contract_sha256
            ),
            "batch_and_partition_order_independent": (
                first.manifest.dataset_semantic_sha256
                == second.manifest.dataset_semantic_sha256
                == verified_first.dataset_semantic_sha256
                == verified_second.dataset_semantic_sha256
            ),
            "exact_cardinality": (
                controls.supplier_rows == config.supplier_count
                and controls.product_location_rows == config.series_count
                and controls.demand_rows == plan.demand_rows
                and controls.lead_time_rows > 0
            ),
            "exact_cohort_allocation": controls.cohort_counts == config.cohort_counts,
            "representative_scenarios_present": all(
                value > 0
                for value in (
                    controls.promotion_rows,
                    controls.disruption_rows,
                    controls.stockout_censored_rows,
                    controls.completed_orders,
                    controls.partial_orders,
                    controls.open_orders,
                    controls.late_orders,
                    controls.late_arriving_demand_rows,
                )
            ),
            "point_in_time_lead_time_labels": _point_in_time_labels_are_safe(
                first.output_dir,
                config,
                verified_first,
            ),
            "point_in_time_demand_availability": _point_in_time_demand_is_safe(
                first.output_dir,
                config,
                verified_first,
            ),
            "package_integrity": verified_first == first.manifest
            and verified_second == second.manifest,
            "fresh_generation": first.output_dir.parent == execution_root
            and second.output_dir.parent == execution_root,
        }

    wall_time_seconds = time.perf_counter() - started
    peak_rss_mib = sampler.peak_bytes / 1024**2
    threads_after = threading.active_count()
    checks["no_surviving_descendants"] = not (_child_identities() - baseline_children)
    summary_path = execution_root / "r2c-verification-summary.json"
    package_bytes = _artifact_bytes(execution_root)
    artifact_bytes = package_bytes
    result: R2CVerificationResult
    summary_bytes = b""
    for _ in range(4):
        complete_checks = {
            **checks,
            "resource_budget": (
                wall_time_seconds <= config.resource_budget.maximum_runtime_seconds
                and peak_rss_mib <= config.resource_budget.peak_rss_mib
                and artifact_bytes <= config.resource_budget.maximum_artifact_mib * 1024**2
                and threads_after <= config.resource_budget.maximum_python_threads
            ),
        }
        result = R2CVerificationResult(
            passed=all(complete_checks.values()),
            profile=config.name,
            profile_contract_sha256=first.manifest.profile_contract_sha256,
            dataset_semantic_sha256=first.manifest.dataset_semantic_sha256,
            checks=complete_checks,
            controls=controls,
            wall_time_seconds=round(wall_time_seconds, 6),
            peak_rss_mib=round(peak_rss_mib, 3),
            artifact_bytes=artifact_bytes,
            python_threads_after_run=threads_after,
            summary_path=str(summary_path),
        )
        summary_bytes = (
            json.dumps(result.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        updated_bytes = package_bytes + len(summary_bytes)
        if updated_bytes == artifact_bytes:
            break
        artifact_bytes = updated_bytes
    summary_path.write_bytes(summary_bytes)
    return result
