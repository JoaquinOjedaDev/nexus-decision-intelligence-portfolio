"""Deterministic, partitioned representative synthetic data for the R2C gate."""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import tempfile
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from nexus.data.r2c_models import (
    R2CCohortCounts,
    R2CCohortName,
    R2CControls,
    R2CDatasetName,
    R2CGeneratedProfile,
    R2CManifest,
    R2CProfileConfig,
    R2CProfilePlan,
    R2CShard,
    canonical_r2c_sha256,
    r2c_profile_sha256,
)
from nexus.storage import canonical_schema_sha256, file_sha256

R2C_MANIFEST_NAME = "profile-manifest.json"

_SUPPLIER_SCHEMA = pa.schema(
    [
        pa.field("supplier_id", pa.string(), nullable=False),
        pa.field("lead_time_regime", pa.string(), nullable=False),
        pa.field("baseline_lead_time_days", pa.int16(), nullable=False),
        pa.field("variability_days", pa.int16(), nullable=False),
        pa.field("available_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)
_PRODUCT_LOCATION_SCHEMA = pa.schema(
    [
        pa.field("sku_id", pa.string(), nullable=False),
        pa.field("location_id", pa.string(), nullable=False),
        pa.field("supplier_id", pa.string(), nullable=False),
        pa.field("category_id", pa.string(), nullable=False),
        pa.field("synthetic_cohort", pa.string(), nullable=False),
        pa.field("activation_date", pa.date32(), nullable=False),
        pa.field("available_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)
_DEMAND_SCHEMA = pa.schema(
    [
        pa.field("demand_date", pa.date32(), nullable=False),
        pa.field("sku_id", pa.string(), nullable=False),
        pa.field("location_id", pa.string(), nullable=False),
        pa.field("supplier_id", pa.string(), nullable=False),
        pa.field("synthetic_cohort", pa.string(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("promotion_flag", pa.bool_(), nullable=False),
        pa.field("disruption_flag", pa.bool_(), nullable=False),
        pa.field("synthetic_latent_demand_qty", pa.int64(), nullable=False),
        pa.field("observed_sales_qty", pa.int64(), nullable=False),
        pa.field("fulfilled_qty", pa.int64(), nullable=False),
        pa.field("lost_sales_qty", pa.int64(), nullable=False),
        pa.field("stockout_censored", pa.bool_(), nullable=False),
        pa.field("available_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)
_LEAD_TIME_SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("sku_id", pa.string(), nullable=False),
        pa.field("location_id", pa.string(), nullable=False),
        pa.field("supplier_id", pa.string(), nullable=False),
        pa.field("ordered_qty", pa.int64(), nullable=False),
        pa.field("received_qty", pa.int64(), nullable=False),
        pa.field("receipt_count", pa.int16(), nullable=False),
        pa.field("ordered_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("expected_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ordered_available_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("completed_at", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("label_available_at", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("status", pa.string(), nullable=False),
        pa.field("actual_lead_time_days", pa.float64(), nullable=True),
        pa.field("delay_days", pa.float64(), nullable=True),
        pa.field("is_late", pa.bool_(), nullable=True),
        pa.field("right_censored", pa.bool_(), nullable=False),
        pa.field("disruption_flag", pa.bool_(), nullable=False),
    ]
)
R2C_SCHEMAS: Mapping[R2CDatasetName, pa.Schema] = {
    "representative.suppliers": _SUPPLIER_SCHEMA,
    "representative.product_locations": _PRODUCT_LOCATION_SCHEMA,
    "representative.demand_history": _DEMAND_SCHEMA,
    "representative.lead_time_history": _LEAD_TIME_SCHEMA,
}


class R2CProfileOptInRequired(PermissionError):
    """Raised before materializing an explicitly protected scale profile."""


class R2CPackageIntegrityError(ValueError):
    """Raised when a package no longer matches its manifest."""


class R2CInsufficientDiskError(OSError):
    """Raised before a protected profile can exhaust its target filesystem."""


@dataclass(frozen=True, slots=True)
class _Series:
    sku_id: str
    location_id: str
    supplier_id: str
    category_id: str
    cohort: R2CCohortName
    activation_date: date
    base_demand: int

    @property
    def identity(self) -> str:
        return f"{self.sku_id}:{self.location_id}"


@dataclass(slots=True)
class _ObservedControls:
    promotion_rows: int = 0
    disruption_rows: int = 0
    stockout_censored_rows: int = 0
    completed_orders: int = 0
    partial_orders: int = 0
    open_orders: int = 0
    late_orders: int = 0
    late_arriving_demand_rows: int = 0
    demand_rows: int = 0
    lead_time_rows: int = 0


def _stable_u64(seed: int, *parts: object) -> int:
    payload = "|".join((str(seed), *(str(part) for part in parts))).encode("utf-8")
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")


def _stable_unit(seed: int, *parts: object) -> float:
    return _stable_u64(seed, *parts) / 2**64


def _utc(day: date, hour: int = 0) -> datetime:
    return datetime.combine(day, time(hour=hour), tzinfo=UTC)


def _json_scalar(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _canonical_row_bytes(row: Mapping[str, object]) -> bytes:
    normalized = {key: _json_scalar(value) for key, value in row.items()}
    return (
        json.dumps(
            normalized,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _month_ranges(start: date, end: date) -> list[tuple[str, date, date]]:
    ranges: list[tuple[str, date, date]] = []
    cursor = start.replace(day=1)
    while cursor <= end:
        next_month = (
            date(cursor.year + 1, 1, 1)
            if cursor.month == 12
            else date(cursor.year, cursor.month + 1, 1)
        )
        lower = max(start, cursor)
        upper = min(end, next_month - timedelta(days=1))
        ranges.append((f"month={cursor:%Y-%m}", lower, upper))
        cursor = next_month
    return ranges


def _build_series(config: R2CProfileConfig) -> tuple[_Series, ...]:
    identities = [
        (f"SKU-{sku_index:05d}", f"LOC-{location_index:03d}")
        for sku_index in range(1, config.sku_count + 1)
        for location_index in range(1, config.location_count + 1)
    ]
    cohort_order = sorted(
        identities,
        key=lambda item: (_stable_u64(config.seed, "cohort", *item), item),
    )
    assigned: dict[tuple[str, str], R2CCohortName] = {}
    offset = 0
    for cohort, count in config.cohort_counts.as_dict().items():
        for identity in cohort_order[offset : offset + count]:
            assigned[identity] = cohort
        offset += count

    series: list[_Series] = []
    for sku_id, location_id in identities:
        identity = (sku_id, location_id)
        cohort = assigned[identity]
        supplier_number = (
            _stable_u64(config.seed, "supplier", *identity) % config.supplier_count + 1
        )
        activation_date = config.start_date
        if cohort == "cold_start":
            maximum_jitter = max(
                0,
                config.cold_start_history_days - max(config.forecast_horizons_days),
            )
            jitter = _stable_u64(config.seed, "activation", *identity) % (maximum_jitter + 1)
            active_days = config.cold_start_history_days - jitter
            activation_date = config.end_date - timedelta(days=active_days - 1)
        series.append(
            _Series(
                sku_id=sku_id,
                location_id=location_id,
                supplier_id=f"SUP-{supplier_number:04d}",
                category_id=(f"CAT-{_stable_u64(config.seed, 'category', sku_id) % 5 + 1:02d}"),
                cohort=cohort,
                activation_date=activation_date,
                base_demand=5 + _stable_u64(config.seed, "base-demand", *identity) % 16,
            )
        )
    return tuple(series)


def _supplier_rows(config: R2CProfileConfig) -> Iterator[dict[str, object]]:
    regimes = ("reliable", "variable", "disruption_prone")
    for number in range(1, config.supplier_count + 1):
        regime = regimes[(number - 1) % len(regimes)]
        baseline = 3 + _stable_u64(config.seed, "supplier-days", number) % 6
        variability = {"reliable": 1, "variable": 3, "disruption_prone": 5}[regime]
        yield {
            "supplier_id": f"SUP-{number:04d}",
            "lead_time_regime": regime,
            "baseline_lead_time_days": baseline,
            "variability_days": variability,
            "available_at": _utc(config.start_date),
        }


def _product_rows(series: Sequence[_Series]) -> Iterator[dict[str, object]]:
    for item in series:
        yield {
            "sku_id": item.sku_id,
            "location_id": item.location_id,
            "supplier_id": item.supplier_id,
            "category_id": item.category_id,
            "synthetic_cohort": item.cohort,
            "activation_date": item.activation_date,
            "available_at": _utc(item.activation_date),
        }


def _demand_quantity(config: R2CProfileConfig, item: _Series, day: date) -> int:
    if day < item.activation_date:
        return 0
    occurrence_probability = {
        "smooth": 0.96,
        "intermittent": 0.25,
        "erratic": 0.90,
        "lumpy": 0.28,
        "cold_start": 0.78,
    }[item.cohort]
    if _stable_unit(config.seed, "occurs", item.identity, day) >= occurrence_probability:
        return 0
    variation = _stable_unit(config.seed, "magnitude", item.identity, day)
    low, spread = {
        "smooth": (0.85, 0.30),
        "intermittent": (0.80, 0.45),
        "erratic": (0.15, 8.00),
        "lumpy": (0.15, 8.00),
        "cold_start": (0.65, 0.90),
    }[item.cohort]
    weekly_factor = (1.12, 1.00, 0.96, 1.02, 1.18, 0.84, 0.76)[day.weekday()]
    day_offset = (day - config.start_date).days
    promotion = (day_offset + _stable_u64(config.seed, "promotion", item.identity)) % 91 < 3
    promotion_factor = 1.75 if promotion else 1.0
    magnitude_factor = (
        low + spread * variation**5
        if item.cohort in {"erratic", "lumpy"}
        else low + spread * variation
    )
    quantity = item.base_demand * magnitude_factor * weekly_factor
    return max(1, round(quantity * promotion_factor))


def _is_disruption(config: R2CProfileConfig, item: _Series, day: date) -> bool:
    supplier_number = int(item.supplier_id.removeprefix("SUP-"))
    disruption_prone = (supplier_number - 1) % 3 == 2
    stress_start = config.end_date - timedelta(days=config.stress_days - 1)
    return day >= stress_start and disruption_prone


def _demand_rows(
    config: R2CProfileConfig,
    series: Sequence[_Series],
    lower: date,
    upper: date,
    controls: _ObservedControls,
) -> Iterator[dict[str, object]]:
    day = lower
    while day <= upper:
        day_offset = (day - config.start_date).days
        for item in series:
            active = day >= item.activation_date
            latent = _demand_quantity(config, item, day)
            promotion = (
                active
                and (day_offset + _stable_u64(config.seed, "promotion", item.identity)) % 91 < 3
            )
            disruption = active and _is_disruption(config, item, day)
            recurrent_shortage = (
                day_offset + _stable_u64(config.seed, "stock-cycle", item.identity)
            ) % 47 < 3
            stockout = latent > 0 and (
                recurrent_shortage
                or (
                    disruption
                    and _stable_unit(config.seed, "disruption-stock", item.identity, day) < 0.55
                )
            )
            shortage_fraction = 0.35 + 0.55 * _stable_unit(
                config.seed,
                "shortage",
                item.identity,
                day,
            )
            fulfilled = (
                max(0, latent - math.ceil(latent * shortage_fraction)) if stockout else latent
            )
            availability_delay = (
                3 if _stable_unit(config.seed, "arrival-delay", item.identity, day) < 0.04 else 1
            )
            available_at = max(
                _utc(day + timedelta(days=availability_delay), 3),
                _utc(item.activation_date),
            )
            controls.demand_rows += 1
            controls.promotion_rows += int(promotion)
            controls.disruption_rows += int(disruption)
            controls.stockout_censored_rows += int(stockout)
            controls.late_arriving_demand_rows += int(active and availability_delay > 1)
            yield {
                "demand_date": day,
                "sku_id": item.sku_id,
                "location_id": item.location_id,
                "supplier_id": item.supplier_id,
                "synthetic_cohort": item.cohort,
                "is_active": active,
                "promotion_flag": promotion,
                "disruption_flag": disruption,
                "synthetic_latent_demand_qty": latent,
                "observed_sales_qty": fulfilled,
                "fulfilled_qty": fulfilled,
                "lost_sales_qty": latent - fulfilled,
                "stockout_censored": stockout,
                "available_at": available_at,
            }
        day += timedelta(days=1)


def _supplier_parameters(config: R2CProfileConfig, supplier_id: str) -> tuple[int, int]:
    number = int(supplier_id.removeprefix("SUP-"))
    regime = ("reliable", "variable", "disruption_prone")[(number - 1) % 3]
    baseline = 3 + _stable_u64(config.seed, "supplier-days", number) % 6
    variability = {"reliable": 1, "variable": 3, "disruption_prone": 5}[regime]
    return baseline, variability


def _first_order_on_or_after(activation: date, lower: date, cycle_days: int) -> date:
    if lower <= activation:
        return activation
    elapsed = (lower - activation).days
    cycles = (elapsed + cycle_days - 1) // cycle_days
    return activation + timedelta(days=cycles * cycle_days)


def _lead_time_rows(
    config: R2CProfileConfig,
    series: Sequence[_Series],
    lower: date,
    upper: date,
    controls: _ObservedControls,
) -> Iterator[dict[str, object]]:
    observation_cutoff = _utc(config.end_date + timedelta(days=2), 23)
    for item in series:
        order_day = _first_order_on_or_after(item.activation_date, lower, config.order_cycle_days)
        while order_day <= upper:
            order_index = (order_day - item.activation_date).days // config.order_cycle_days
            ordered_at = _utc(order_day, 12)
            baseline, variability = _supplier_parameters(config, item.supplier_id)
            disruption = _is_disruption(config, item, order_day)
            variation_width = 2 * variability + 1
            variation = (
                _stable_u64(config.seed, "lead-variation", item.identity, order_index)
                % variation_width
            ) - variability
            actual_days = max(1, baseline + variation + (7 if disruption else 0))
            ordered_qty = max(1, round(item.base_demand * config.order_cycle_days * 1.15))
            state_draw = _stable_unit(config.seed, "order-state", item.identity, order_index)
            status = "complete"
            received_qty = ordered_qty
            receipt_count = (
                2
                if _stable_unit(config.seed, "multi-receipt", item.identity, order_index) < 0.25
                else 1
            )
            candidate_completed_at = ordered_at + timedelta(days=actual_days)
            label_delay = (
                3
                if _stable_unit(config.seed, "label-delay", item.identity, order_index) < 0.08
                else 1
            )
            candidate_label_available_at = candidate_completed_at + timedelta(days=label_delay)
            completed_at: datetime | None = candidate_completed_at
            label_available_at: datetime | None = candidate_label_available_at
            if state_draw < 0.08:
                status = "open"
                received_qty = 0
                receipt_count = 0
                completed_at = None
                label_available_at = None
            elif state_draw < 0.18:
                status = "partial"
                received_qty = max(1, ordered_qty // 2)
                receipt_count = 1
                completed_at = None
                label_available_at = None
            elif candidate_label_available_at > observation_cutoff:
                status = "open"
                received_qty = 0
                receipt_count = 0
                completed_at = None
                label_available_at = None

            complete = status == "complete"
            actual_label = float(actual_days) if complete else None
            delay_label = float(actual_days - baseline) if complete else None
            is_late = actual_days > baseline if complete else None
            controls.lead_time_rows += 1
            controls.completed_orders += int(complete)
            controls.partial_orders += int(status == "partial")
            controls.open_orders += int(status == "open")
            controls.late_orders += int(is_late is True)
            yield {
                "order_id": f"PO-{order_day:%Y%m%d}-{item.sku_id}-{item.location_id}",
                "sku_id": item.sku_id,
                "location_id": item.location_id,
                "supplier_id": item.supplier_id,
                "ordered_qty": ordered_qty,
                "received_qty": received_qty,
                "receipt_count": receipt_count,
                "ordered_at": ordered_at,
                "expected_at": ordered_at + timedelta(days=baseline),
                "ordered_available_at": ordered_at + timedelta(hours=1),
                "completed_at": completed_at,
                "label_available_at": label_available_at,
                "status": status,
                "actual_lead_time_days": actual_label,
                "delay_days": delay_label,
                "is_late": is_late,
                "right_censored": not complete,
                "disruption_flag": disruption,
            }
            order_day += timedelta(days=config.order_cycle_days)


def _dataset_dir(dataset: R2CDatasetName) -> str:
    return dataset.replace(".", "/")


def _write_shard(
    output_dir: Path,
    *,
    dataset: R2CDatasetName,
    partition: str,
    rows: Iterable[Mapping[str, object]],
    batch_rows: int,
) -> R2CShard:
    schema = R2C_SCHEMAS[dataset]
    partition_dir = output_dir / _dataset_dir(dataset) / partition
    partition_dir.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    digest = hashlib.sha256()
    row_count = 0
    buffer: list[dict[str, object]] = []
    try:
        with tempfile.NamedTemporaryFile(
            prefix=".r2c-",
            suffix=".parquet.tmp",
            dir=partition_dir,
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
        with pq.ParquetWriter(
            temporary_path,
            schema,
            compression="zstd",
            version="2.6",
            use_dictionary=True,
        ) as writer:
            for source_row in rows:
                row = dict(source_row)
                digest.update(_canonical_row_bytes(row))
                buffer.append(row)
                row_count += 1
                if len(buffer) >= batch_rows:
                    writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                    buffer.clear()
            if buffer:
                writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
        semantic_sha256 = digest.hexdigest()
        target = partition_dir / f"{semantic_sha256}.parquet"
        if target.exists():
            if file_sha256(target) != file_sha256(temporary_path):
                raise FileExistsError(f"refusing to replace immutable R2C shard: {target}")
            temporary_path.unlink()
        else:
            os.replace(temporary_path, target)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    relative_path = target.relative_to(output_dir).as_posix()
    return R2CShard(
        dataset=dataset,
        partition=partition,
        relative_path=relative_path,
        row_count=row_count,
        semantic_sha256=semantic_sha256,
        schema_sha256=canonical_schema_sha256(schema),
        file_sha256=file_sha256(target),
        file_bytes=target.stat().st_size,
    )


def _semantic_root(shards: Sequence[R2CShard]) -> str:
    identities = [
        {
            "dataset": shard.dataset,
            "partition": shard.partition,
            "row_count": shard.row_count,
            "schema_sha256": shard.schema_sha256,
            "semantic_sha256": shard.semantic_sha256,
        }
        for shard in sorted(shards, key=lambda item: (item.dataset, item.partition))
    ]
    return canonical_r2c_sha256(identities)


def plan_r2c_profile(config: R2CProfileConfig) -> R2CProfilePlan:
    """Calculate deterministic cardinality and a conservative storage estimate."""

    demand_rows = config.series_count * config.history_days
    regular_series = config.series_count - config.cohort_counts.cold_start
    maximum_order_rows = regular_series * math.ceil(config.history_days / config.order_cycle_days)
    maximum_order_rows += config.cohort_counts.cold_start * math.ceil(
        config.cold_start_history_days / config.order_cycle_days
    )
    estimated_bytes = (
        demand_rows * 150
        + maximum_order_rows * 240
        + config.series_count * 180
        + config.supplier_count * 120
    )
    estimated_gib = estimated_bytes / 1024**3
    return R2CProfilePlan(
        profile=config.name,
        series_count=config.series_count,
        demand_rows=demand_rows,
        maximum_order_rows=maximum_order_rows,
        estimated_uncompressed_gib=round(estimated_gib, 3),
        minimum_free_disk_gib=round(max(1.0, estimated_gib * 1.5), 3),
        requires_explicit_opt_in=config.requires_explicit_opt_in,
        executable_by_default=not config.requires_explicit_opt_in,
    )


def require_r2c_free_disk(
    output_dir: Path,
    *,
    config: R2CProfileConfig,
    copies: int = 1,
) -> None:
    """Fail before protected scale writes when conservative free space is unavailable."""

    if copies <= 0:
        raise ValueError("copies must be positive")
    probe = output_dir.resolve()
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    plan = plan_r2c_profile(config)
    required_bytes = math.ceil(plan.minimum_free_disk_gib * copies * 1024**3)
    free_bytes = shutil.disk_usage(probe).free
    if free_bytes < required_bytes:
        raise R2CInsufficientDiskError(
            "insufficient free disk for protected R2C profile: "
            f"required={required_bytes / 1024**3:.3f} GiB, "
            f"available={free_bytes / 1024**3:.3f} GiB"
        )


def _read_existing_profile(
    output_dir: Path,
    config: R2CProfileConfig,
) -> R2CGeneratedProfile | None:
    manifest_path = output_dir / R2C_MANIFEST_NAME
    if not manifest_path.exists():
        if output_dir.exists() and any(output_dir.iterdir()):
            raise FileExistsError(
                f"R2C output contains an incomplete or foreign package: {output_dir}"
            )
        return None
    manifest = R2CManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    if manifest.profile_contract_sha256 != r2c_profile_sha256(config):
        raise FileExistsError(
            "refusing to reinterpret an existing R2C package with another profile"
        )
    verify_r2c_package(output_dir, expected_config=config)
    return R2CGeneratedProfile(
        output_dir=output_dir,
        manifest_path=manifest_path,
        manifest=manifest,
        manifest_file_sha256=file_sha256(manifest_path),
    )


def _generate_r2c_profile_contents(
    output_dir: Path,
    *,
    config: R2CProfileConfig,
    allow_scale: bool = False,
    batch_rows_override: int | None = None,
    reverse_partition_order: bool = False,
) -> R2CGeneratedProfile:
    """Generate an immutable profile without holding high-volume facts in memory."""

    if config.requires_explicit_opt_in and not allow_scale:
        raise R2CProfileOptInRequired(
            "the full profile is a scale plan; pass allow_scale only after reviewing its preflight"
        )
    output_dir = output_dir.resolve()
    existing = _read_existing_profile(output_dir, config)
    if existing is not None:
        return existing
    if config.requires_explicit_opt_in:
        require_r2c_free_disk(output_dir, config=config)
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_rows = config.write_batch_rows if batch_rows_override is None else batch_rows_override
    if batch_rows <= 0:
        raise ValueError("batch_rows_override must be positive")

    series = _build_series(config)
    controls = _ObservedControls()
    shards = [
        _write_shard(
            output_dir,
            dataset="representative.suppliers",
            partition="all",
            rows=_supplier_rows(config),
            batch_rows=batch_rows,
        ),
        _write_shard(
            output_dir,
            dataset="representative.product_locations",
            partition="all",
            rows=_product_rows(series),
            batch_rows=batch_rows,
        ),
    ]
    month_ranges = _month_ranges(config.start_date, config.end_date)
    execution_ranges = list(reversed(month_ranges)) if reverse_partition_order else month_ranges
    for partition, lower, upper in execution_ranges:
        shards.append(
            _write_shard(
                output_dir,
                dataset="representative.demand_history",
                partition=partition,
                rows=_demand_rows(config, series, lower, upper, controls),
                batch_rows=batch_rows,
            )
        )
        shards.append(
            _write_shard(
                output_dir,
                dataset="representative.lead_time_history",
                partition=partition,
                rows=_lead_time_rows(config, series, lower, upper, controls),
                batch_rows=batch_rows,
            )
        )

    ordered_shards = tuple(sorted(shards, key=lambda item: (item.dataset, item.partition)))
    observed_cohorts = Counter(item.cohort for item in series)
    manifest = R2CManifest(
        schema_version="1.0.0",
        generator="nexus-r2c-representative-supply-network",
        classification="synthetic",
        profile=config.name,
        profile_contract=config,
        profile_contract_sha256=r2c_profile_sha256(config),
        dataset_semantic_sha256=_semantic_root(ordered_shards),
        start_date=config.start_date,
        end_date=config.end_date,
        generated_through=_utc(config.end_date + timedelta(days=3), 3),
        shards=ordered_shards,
        controls=R2CControls(
            supplier_rows=config.supplier_count,
            product_location_rows=config.series_count,
            demand_rows=controls.demand_rows,
            lead_time_rows=controls.lead_time_rows,
            promotion_rows=controls.promotion_rows,
            disruption_rows=controls.disruption_rows,
            stockout_censored_rows=controls.stockout_censored_rows,
            completed_orders=controls.completed_orders,
            partial_orders=controls.partial_orders,
            open_orders=controls.open_orders,
            late_orders=controls.late_orders,
            late_arriving_demand_rows=controls.late_arriving_demand_rows,
            cohort_counts=R2CCohortCounts.model_validate(dict(observed_cohorts)),
        ),
    )
    manifest_path = output_dir / R2C_MANIFEST_NAME
    manifest_text = (
        json.dumps(
            manifest.model_dump(mode="json"),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    temporary_manifest: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=".profile-manifest.",
            suffix=".tmp",
            dir=output_dir,
            delete=False,
            mode="w",
            encoding="utf-8",
            newline="\n",
        ) as temporary:
            temporary.write(manifest_text)
            temporary_manifest = Path(temporary.name)
        os.replace(temporary_manifest, manifest_path)
        temporary_manifest = None
    finally:
        if temporary_manifest is not None:
            temporary_manifest.unlink(missing_ok=True)
    return R2CGeneratedProfile(
        output_dir=output_dir,
        manifest_path=manifest_path,
        manifest=manifest,
        manifest_file_sha256=file_sha256(manifest_path),
    )


def generate_r2c_profile(
    output_dir: Path,
    *,
    config: R2CProfileConfig,
    allow_scale: bool = False,
    batch_rows_override: int | None = None,
    reverse_partition_order: bool = False,
) -> R2CGeneratedProfile:
    """Publish a complete verified profile, leaving the destination clean on failure."""

    if config.requires_explicit_opt_in and not allow_scale:
        raise R2CProfileOptInRequired(
            "the full profile is a scale plan; pass allow_scale only after reviewing its preflight"
        )
    output_dir = output_dir.resolve()
    existing = _read_existing_profile(output_dir, config)
    if existing is not None:
        return existing
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.stage-", dir=output_dir.parent)
    ).resolve()
    try:
        generated = _generate_r2c_profile_contents(
            staging,
            config=config,
            allow_scale=allow_scale,
            batch_rows_override=batch_rows_override,
            reverse_partition_order=reverse_partition_order,
        )
        verify_r2c_package(staging, expected_config=config)
        if output_dir.exists():
            output_dir.rmdir()  # Only an empty pre-existing destination is allowed.
        staging.rename(output_dir)
        manifest_path = output_dir / R2C_MANIFEST_NAME
        return R2CGeneratedProfile(
            output_dir=output_dir,
            manifest_path=manifest_path,
            manifest=generated.manifest,
            manifest_file_sha256=generated.manifest_file_sha256,
        )
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def _iter_shard_rows(path: Path) -> Iterator[dict[str, object]]:
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=4096):
        yield from cast(list[dict[str, object]], batch.to_pylist())


def _require_integrity(condition: bool, detail: str) -> None:
    if not condition:
        raise R2CPackageIntegrityError(detail)


def _expected_partition_identities(manifest: R2CManifest) -> set[tuple[str, str]]:
    months = {
        partition for partition, _, _ in _month_ranges(manifest.start_date, manifest.end_date)
    }
    return {
        ("representative.suppliers", "all"),
        ("representative.product_locations", "all"),
        *(("representative.demand_history", month) for month in months),
        *(("representative.lead_time_history", month) for month in months),
    }


def _shards_for_dataset(
    manifest: R2CManifest,
    dataset: R2CDatasetName,
) -> tuple[R2CShard, ...]:
    return tuple(shard for shard in manifest.shards if shard.dataset == dataset)


def _audit_r2c_logical_contract(
    output_dir: Path,
    manifest: R2CManifest,
    expected_config: R2CProfileConfig | None,
) -> None:
    identities = {(shard.dataset, shard.partition) for shard in manifest.shards}
    _require_integrity(
        identities == _expected_partition_identities(manifest),
        "manifest does not contain the exact required dataset partitions",
    )

    supplier_ids: set[str] = set()
    supplier_parameters: dict[str, tuple[int, int]] = {}
    for shard in _shards_for_dataset(manifest, "representative.suppliers"):
        for row in _iter_shard_rows(output_dir / Path(shard.relative_path)):
            supplier_id = cast(str, row["supplier_id"])
            regime = cast(str, row["lead_time_regime"])
            variability = cast(int, row["variability_days"])
            _require_integrity(supplier_id not in supplier_ids, "duplicate supplier_id")
            _require_integrity(cast(int, row["baseline_lead_time_days"]) > 0, "invalid lead time")
            _require_integrity(
                regime in {"reliable", "variable", "disruption_prone"}
                and variability
                == {"reliable": 1, "variable": 3, "disruption_prone": 5}.get(regime),
                "invalid supplier lead-time regime",
            )
            _require_integrity(
                row["available_at"] == _utc(manifest.start_date),
                "supplier availability must match the profile start",
            )
            supplier_ids.add(supplier_id)
            supplier_parameters[supplier_id] = (
                cast(int, row["baseline_lead_time_days"]),
                variability,
            )

    product_keys: list[tuple[str, str]] = []
    products: dict[tuple[str, str], dict[str, object]] = {}
    observed_cohorts: Counter[str] = Counter()
    for shard in _shards_for_dataset(manifest, "representative.product_locations"):
        for row in _iter_shard_rows(output_dir / Path(shard.relative_path)):
            key = (cast(str, row["sku_id"]), cast(str, row["location_id"]))
            _require_integrity(key not in products, "duplicate product-location key")
            supplier_id = cast(str, row["supplier_id"])
            activation_date = cast(date, row["activation_date"])
            available_at = cast(datetime, row["available_at"])
            cohort = cast(str, row["synthetic_cohort"])
            _require_integrity(supplier_id in supplier_ids, "unknown product-location supplier")
            _require_integrity(
                manifest.start_date <= activation_date <= manifest.end_date,
                "activation_date falls outside the profile",
            )
            _require_integrity(
                available_at.date() == activation_date,
                "product-location availability must match activation",
            )
            if cohort == "cold_start":
                active_days = (manifest.end_date - activation_date).days + 1
                _require_integrity(
                    activation_date > manifest.start_date
                    and max(manifest.profile_contract.forecast_horizons_days)
                    <= active_days
                    <= manifest.profile_contract.cold_start_history_days,
                    "cold-start activation falls outside its frozen history window",
                )
            else:
                _require_integrity(
                    activation_date == manifest.start_date,
                    "non-cold-start series must activate at the profile start",
                )
            product_keys.append(key)
            products[key] = row
            observed_cohorts[cast(str, row["synthetic_cohort"])] += 1
    _require_integrity(product_keys == sorted(product_keys), "product locations are not ordered")

    demand_rows = 0
    promotion_rows = 0
    disruption_rows = 0
    stockout_rows = 0
    late_demand_rows = 0
    expected_position = 0
    product_count = len(product_keys)
    for shard in _shards_for_dataset(manifest, "representative.demand_history"):
        for row in _iter_shard_rows(output_dir / Path(shard.relative_path)):
            _require_integrity(product_count > 0, "demand exists without product locations")
            expected_day = manifest.start_date + timedelta(days=expected_position // product_count)
            expected_key = product_keys[expected_position % product_count]
            key = (cast(str, row["sku_id"]), cast(str, row["location_id"]))
            demand_day = cast(date, row["demand_date"])
            product = products.get(key)
            _require_integrity(
                demand_day == expected_day and key == expected_key,
                "demand grain is missing, duplicated, or out of order",
            )
            _require_integrity(
                shard.partition == f"month={demand_day:%Y-%m}",
                "demand row is routed to the wrong month partition",
            )
            _require_integrity(product is not None, "unknown demand product-location")
            assert product is not None
            _require_integrity(
                row["supplier_id"] == product["supplier_id"]
                and row["synthetic_cohort"] == product["synthetic_cohort"],
                "demand dimension attributes do not match product-location",
            )
            activation_date = cast(date, product["activation_date"])
            active = demand_day >= activation_date
            latent = cast(int, row["synthetic_latent_demand_qty"])
            observed = cast(int, row["observed_sales_qty"])
            fulfilled = cast(int, row["fulfilled_qty"])
            lost = cast(int, row["lost_sales_qty"])
            stockout = cast(bool, row["stockout_censored"])
            promotion = cast(bool, row["promotion_flag"])
            disruption = cast(bool, row["disruption_flag"])
            available_at = cast(datetime, row["available_at"])
            _require_integrity(row["is_active"] is active, "incorrect demand activation state")
            _require_integrity(
                min(latent, observed, fulfilled, lost) >= 0,
                "negative demand quantity",
            )
            _require_integrity(
                observed == fulfilled <= latent and lost == latent - fulfilled,
                "demand quantity reconciliation failed",
            )
            _require_integrity(stockout is (lost > 0), "stockout censor flag is inconsistent")
            _require_integrity(
                available_at > _utc(demand_day),
                "demand availability must follow its business date",
            )
            _require_integrity(
                available_at >= cast(datetime, product["available_at"])
                and available_at <= manifest.generated_through,
                "demand availability violates the package observation boundary",
            )
            if not active:
                _require_integrity(
                    latent == observed == fulfilled == lost == 0
                    and not promotion
                    and not disruption
                    and not stockout,
                    "pre-activation rows must be explicit structural zeros",
                )
            demand_rows += 1
            promotion_rows += int(promotion)
            disruption_rows += int(disruption)
            stockout_rows += int(stockout)
            late_demand_rows += int(active and (available_at.date() - demand_day).days > 1)
            expected_position += 1
    expected_demand_rows = product_count * ((manifest.end_date - manifest.start_date).days + 1)
    _require_integrity(
        demand_rows == expected_demand_rows,
        "demand panel does not cover every declared series and day",
    )

    lead_time_rows = 0
    completed_orders = 0
    partial_orders = 0
    open_orders = 0
    late_orders = 0
    for shard in _shards_for_dataset(manifest, "representative.lead_time_history"):
        partition_range = next(
            (lower, upper)
            for partition, lower, upper in _month_ranges(manifest.start_date, manifest.end_date)
            if partition == shard.partition
        )
        expected_orders: list[tuple[tuple[str, str], date]] = []
        cycle_days = manifest.profile_contract.order_cycle_days
        lower, upper = partition_range
        for key in product_keys:
            activation = cast(date, products[key]["activation_date"])
            order_day = _first_order_on_or_after(activation, lower, cycle_days)
            while order_day <= upper:
                expected_orders.append((key, order_day))
                order_day += timedelta(days=cycle_days)
        _require_integrity(
            shard.row_count == len(expected_orders),
            "lead-time partition does not contain the expected order grid",
        )
        observed_partition_rows = 0
        for row, (expected_key, expected_day) in zip(
            _iter_shard_rows(output_dir / Path(shard.relative_path)),
            expected_orders,
            strict=False,
        ):
            observed_partition_rows += 1
            key = (cast(str, row["sku_id"]), cast(str, row["location_id"]))
            product = products.get(key)
            ordered_at = cast(datetime, row["ordered_at"])
            expected_at = cast(datetime, row["expected_at"])
            ordered_available_at = cast(datetime, row["ordered_available_at"])
            order_day = ordered_at.date()
            _require_integrity(
                key == expected_key and order_day == expected_day,
                "lead-time grain is missing, duplicated, or out of order",
            )
            _require_integrity(
                shard.partition == f"month={order_day:%Y-%m}",
                "lead-time row is routed to the wrong month partition",
            )
            _require_integrity(product is not None, "unknown lead-time product-location")
            assert product is not None
            supplier_id = cast(str, row["supplier_id"])
            _require_integrity(
                supplier_id == product["supplier_id"] and supplier_id in supplier_ids,
                "lead-time supplier relationship failed",
            )
            _require_integrity(
                row["order_id"] == f"PO-{order_day:%Y%m%d}-{row['sku_id']}-{row['location_id']}",
                "order_id does not match its business key",
            )
            ordered_qty = cast(int, row["ordered_qty"])
            received_qty = cast(int, row["received_qty"])
            receipt_count = cast(int, row["receipt_count"])
            status = cast(str, row["status"])
            completed_at = cast(datetime | None, row["completed_at"])
            label_available_at = cast(datetime | None, row["label_available_at"])
            actual_days = cast(float | None, row["actual_lead_time_days"])
            delay_days = cast(float | None, row["delay_days"])
            is_late = cast(bool | None, row["is_late"])
            right_censored = cast(bool, row["right_censored"])
            _require_integrity(
                ordered_qty > 0 and 0 <= received_qty <= ordered_qty and receipt_count >= 0,
                "invalid order or receipt quantities",
            )
            _require_integrity(
                expected_at > ordered_at and ordered_available_at >= ordered_at,
                "invalid order time ordering",
            )
            baseline_days = (expected_at - ordered_at).total_seconds() / 86400
            _require_integrity(
                baseline_days == supplier_parameters[supplier_id][0]
                and ordered_available_at <= manifest.generated_through,
                "order times do not match the supplier or observation boundary",
            )
            if status == "complete":
                _require_integrity(
                    completed_at is not None
                    and label_available_at is not None
                    and actual_days is not None
                    and delay_days is not None
                    and is_late is not None
                    and not right_censored
                    and received_qty == ordered_qty
                    and receipt_count > 0,
                    "completed order is missing its mature label contract",
                )
                assert completed_at is not None and label_available_at is not None
                assert actual_days is not None and delay_days is not None and is_late is not None
                measured_days = (completed_at - ordered_at).total_seconds() / 86400
                _require_integrity(
                    label_available_at >= completed_at
                    and label_available_at <= manifest.generated_through
                    and completed_at > ordered_at
                    and actual_days > 0
                    and actual_days == measured_days
                    and delay_days == actual_days - baseline_days
                    and is_late is (actual_days > baseline_days),
                    "completed lead-time label is inconsistent",
                )
                completed_orders += 1
                late_orders += int(is_late)
            else:
                _require_integrity(status in {"partial", "open"}, "unsupported order status")
                _require_integrity(
                    completed_at is None
                    and label_available_at is None
                    and actual_days is None
                    and delay_days is None
                    and is_late is None
                    and right_censored,
                    "censored order exposes a future completion label",
                )
                if status == "partial":
                    _require_integrity(
                        0 < received_qty < ordered_qty and receipt_count > 0,
                        "partial order must expose only partial receipts",
                    )
                    partial_orders += 1
                else:
                    _require_integrity(
                        received_qty == receipt_count == 0,
                        "open order cannot expose receipts",
                    )
                    open_orders += 1
            lead_time_rows += 1
        _require_integrity(
            observed_partition_rows == len(expected_orders),
            "lead-time partition does not contain the expected order grid",
        )

    observed_controls = R2CControls(
        supplier_rows=len(supplier_ids),
        product_location_rows=len(product_keys),
        demand_rows=demand_rows,
        lead_time_rows=lead_time_rows,
        promotion_rows=promotion_rows,
        disruption_rows=disruption_rows,
        stockout_censored_rows=stockout_rows,
        completed_orders=completed_orders,
        partial_orders=partial_orders,
        open_orders=open_orders,
        late_orders=late_orders,
        late_arriving_demand_rows=late_demand_rows,
        cohort_counts=R2CCohortCounts.model_validate(dict(observed_cohorts)),
    )
    _require_integrity(
        observed_controls == manifest.controls, "manifest controls do not match rows"
    )
    resolved_config = manifest.profile_contract
    _require_integrity(
        len(supplier_ids) == resolved_config.supplier_count
        and len(product_keys) == resolved_config.series_count
        and observed_cohorts == Counter(resolved_config.cohort_counts.as_dict()),
        "manifest dimensions do not match the resolved profile contract",
    )
    if expected_config is not None:
        _require_integrity(
            resolved_config == expected_config,
            "resolved profile does not match the expected profile contract",
        )


def verify_r2c_package(
    output_dir: Path,
    *,
    expected_config: R2CProfileConfig | None = None,
) -> R2CManifest:
    """Stream-verify every manifest-bound shard, schema, count, hash, and path."""

    output_dir = output_dir.resolve()
    manifest_path = output_dir / R2C_MANIFEST_NAME
    manifest = R2CManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    if expected_config is not None:
        if manifest.profile_contract_sha256 != r2c_profile_sha256(expected_config):
            raise R2CPackageIntegrityError("profile contract hash does not match")
        if manifest.profile != expected_config.name:
            raise R2CPackageIntegrityError("profile name does not match")

    declared_paths: set[Path] = set()
    for shard in manifest.shards:
        path = (output_dir / Path(shard.relative_path)).resolve()
        if not path.is_relative_to(output_dir):
            raise R2CPackageIntegrityError("manifest shard escapes the package root")
        declared_paths.add(path)
        if (
            not path.is_file()
            or path.stat().st_size != shard.file_bytes
            or file_sha256(path) != shard.file_sha256
        ):
            raise R2CPackageIntegrityError(f"physical shard hash mismatch: {shard.relative_path}")
        parquet = pq.ParquetFile(path)
        expected_schema = R2C_SCHEMAS[shard.dataset]
        if not parquet.schema_arrow.equals(expected_schema, check_metadata=True):
            raise R2CPackageIntegrityError(f"schema mismatch: {shard.relative_path}")
        if canonical_schema_sha256(parquet.schema_arrow) != shard.schema_sha256:
            raise R2CPackageIntegrityError(f"schema identity mismatch: {shard.relative_path}")
        digest = hashlib.sha256()
        rows = 0
        for row in _iter_shard_rows(path):
            digest.update(_canonical_row_bytes(row))
            rows += 1
        if rows != shard.row_count or digest.hexdigest() != shard.semantic_sha256:
            raise R2CPackageIntegrityError(f"logical shard mismatch: {shard.relative_path}")

    physical_paths = {path.resolve() for path in output_dir.rglob("*.parquet")}
    if physical_paths != declared_paths:
        raise R2CPackageIntegrityError("package contains undeclared or missing Parquet shards")
    if _semantic_root(manifest.shards) != manifest.dataset_semantic_sha256:
        raise R2CPackageIntegrityError("dataset semantic root mismatch")
    _audit_r2c_logical_contract(output_dir, manifest, expected_config)
    return manifest


def iter_r2c_dataset_rows(
    output_dir: Path,
    *,
    dataset: R2CDatasetName,
    verified_manifest: R2CManifest | None = None,
) -> Iterator[dict[str, object]]:
    """Yield one verified dataset without combining its partitions in memory."""

    manifest = verified_manifest or verify_r2c_package(output_dir)
    for shard in manifest.shards:
        if shard.dataset == dataset:
            yield from _iter_shard_rows(output_dir / Path(shard.relative_path))


def iter_r2c_demand_snapshot(
    output_dir: Path,
    *,
    planning_cutoff: datetime,
    verified_manifest: R2CManifest | None = None,
) -> Iterator[dict[str, object]]:
    """Yield demand facts only after they became available by a planning cutoff."""

    if planning_cutoff.tzinfo is None or planning_cutoff.utcoffset() is None:
        raise ValueError("planning_cutoff must include an explicit timezone")
    cutoff = planning_cutoff.astimezone(UTC)
    manifest = verified_manifest or verify_r2c_package(output_dir)
    for shard in manifest.shards:
        if shard.dataset != "representative.demand_history":
            continue
        for row in _iter_shard_rows(output_dir / Path(shard.relative_path)):
            if cast(datetime, row["available_at"]) <= cutoff:
                yield row


def iter_r2c_lead_time_label_snapshot(
    output_dir: Path,
    *,
    planning_cutoff: datetime,
    verified_manifest: R2CManifest | None = None,
) -> Iterator[dict[str, object]]:
    """Yield order features while conservatively masking immature completion labels."""

    if planning_cutoff.tzinfo is None or planning_cutoff.utcoffset() is None:
        raise ValueError("planning_cutoff must include an explicit timezone")
    cutoff = planning_cutoff.astimezone(UTC)
    manifest = verified_manifest or verify_r2c_package(output_dir)
    for shard in manifest.shards:
        if shard.dataset != "representative.lead_time_history":
            continue
        for source_row in _iter_shard_rows(output_dir / Path(shard.relative_path)):
            ordered_available_at = cast(datetime, source_row["ordered_available_at"])
            if ordered_available_at > cutoff:
                continue
            row = dict(source_row)
            label_available_at = cast(datetime | None, row["label_available_at"])
            if label_available_at is None or label_available_at > cutoff:
                row.update(
                    {
                        "received_qty": 0,
                        "receipt_count": 0,
                        "completed_at": None,
                        "label_available_at": None,
                        "status": "right_censored",
                        "actual_lead_time_days": None,
                        "delay_days": None,
                        "is_late": None,
                        "right_censored": True,
                    }
                )
            yield row
