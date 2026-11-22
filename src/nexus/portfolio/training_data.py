"""Checksum-bound, correction-closed labels from the actual R2 Gold products.

This is a runtime data bridge, not an R2C/R3B dataset impersonation. Finalization
is explicit supplied synthetic evidence, never inferred from arrival or elapsed
demo time. Source revisions produce a new binding and invalidate old evidence.
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Literal, Self, cast

import pyarrow.parquet as pq
from pydantic import AwareDatetime, NonNegativeInt, PositiveInt, model_validator

from nexus.data.r2_models import (
    R2FulfillmentOutcome,
    R2Identifier,
    R2RebuildResult,
    R2Record,
    R2Sha256,
    normalize_r2_utc,
)
from nexus.modeling.gold_adapter import R3DemandObservation, adapt_r2_demand_history
from nexus.modeling.r3_artifacts import canonical_json_sha256
from nexus.portfolio.runtime_pipeline import runtime_source_fingerprint
from nexus.storage import canonical_rows_sha256, canonical_schema_sha256, file_sha256


class TrainingDataError(ValueError):
    """The source, Gold grain, or closure evidence cannot cross the learning boundary."""


class FinalizationLabel(R2Record):
    sku_id: R2Identifier
    location_id: R2Identifier
    demand_date: date
    source_rows_sha256: R2Sha256
    correction_closed: bool
    correction_closed_at: AwareDatetime
    selected_revision: Literal[1] = 1

    @property
    def key(self) -> tuple[str, str, date]:
        return self.sku_id, self.location_id, self.demand_date


class FinalizationManifest(R2Record):
    contract_version: Literal["nexus.synthetic-label-finalization.v1"]
    classification: Literal["synthetic"]
    authority: Literal["nexus-r2-learning-supply-network"]
    source_snapshot_sha256: R2Sha256
    rule: Literal["explicit_synthetic_closure_72h_after_daily_completion"]
    real_world_finality_claimed: Literal[False] = False
    labels: tuple[FinalizationLabel, ...]

    @model_validator(mode="after")
    def unique_closed_keys(self) -> Self:
        keys = [item.key for item in self.labels]
        if keys != sorted(set(keys)):
            raise ValueError("finalization keys must be unique and ordered")
        for item in self.labels:
            earliest = datetime.combine(item.demand_date + timedelta(days=4), time.min, UTC)
            if item.correction_closed_at < earliest:
                raise ValueError("closure cannot precede the declared 72-hour correction window")
        return self


class TrainingLabel(R2Record):
    sku_id: R2Identifier
    location_id: R2Identifier
    demand_date: date
    quantity: NonNegativeInt
    available_at: AwareDatetime
    correction_closed_at: AwareDatetime
    fulfillment_record_count: PositiveInt
    source_rows_sha256: R2Sha256

    @model_validator(mode="after")
    def source_and_closure_follow_complete_day(self) -> Self:
        end = datetime.combine(self.demand_date + timedelta(days=1), time.min, UTC)
        if self.available_at < end or self.correction_closed_at < end + timedelta(hours=72):
            raise ValueError(
                "training label cannot precede daily completion and correction closure"
            )
        return self

    @property
    def key(self) -> tuple[str, str, date]:
        return self.sku_id, self.location_id, self.demand_date


@dataclass(frozen=True, slots=True)
class TrainingSnapshot:
    status: Literal["READY", "ABSTAIN_INSUFFICIENT_EVIDENCE"]
    labels: tuple[TrainingLabel, ...]
    exclusions: tuple[tuple[str, int], ...]
    reason_codes: tuple[str, ...]
    source_snapshot_sha256: str
    source_manifest_file_sha256: str
    gold_demand_sha256: str
    gold_fulfillment_sha256: str
    finalization_sha256: str | None
    as_of: datetime
    minimum_history_days: int
    minimum_mature_labels: int
    counts_by_series: tuple[tuple[str, int], ...]
    snapshot_sha256: str

    @property
    def excluded_counts(self) -> dict[str, int]:
        return dict(self.exclusions)

    def to_dict(self) -> dict[str, Any]:
        return {
            **_snapshot_payload(self),
            "snapshot_sha256": self.snapshot_sha256,
        }


def _snapshot_payload(snapshot: TrainingSnapshot) -> dict[str, Any]:
    return {
        "contract_version": "nexus.runtime-training-snapshot.v1",
        "classification": "synthetic",
        "target": "fulfilled_qty_on_fully_fulfilled_demand_days",
        "censoring_policy": "exclude_any_unfulfilled_quantity",
        "history_policy": "available_and_correction_closed_at_each_issue",
        "status": snapshot.status,
        "labels": [item.model_dump(mode="json") for item in snapshot.labels],
        "excluded_counts": snapshot.excluded_counts,
        "reason_codes": list(snapshot.reason_codes),
        "source_snapshot_sha256": snapshot.source_snapshot_sha256,
        "source_manifest_file_sha256": snapshot.source_manifest_file_sha256,
        "gold_demand_sha256": snapshot.gold_demand_sha256,
        "gold_fulfillment_sha256": snapshot.gold_fulfillment_sha256,
        "finalization_sha256": snapshot.finalization_sha256,
        "as_of": snapshot.as_of.isoformat(),
        "minimum_history_days": snapshot.minimum_history_days,
        "minimum_mature_labels": snapshot.minimum_mature_labels,
        "counts_by_series": dict(snapshot.counts_by_series),
        "ready_scope": "Enough eligible source history to attempt a separately gated fit",
        "evaluation_sufficiency_claimed": False,
        "scientific_promotion_allowed": False,
    }


def fulfillment_rows_sha256(rows: tuple[R2FulfillmentOutcome, ...]) -> str:
    """Canonical business-source identity independent of Gold-derived columns."""
    return canonical_json_sha256(
        [item.model_dump(mode="json") for item in sorted(rows, key=lambda row: row.fulfillment_id)]
    )


def validate_training_snapshot(snapshot: TrainingSnapshot) -> None:
    """Reject altered receipt fields or inconsistent maturity/support before fitting.

    This verifies the trusted bridge's receipt, not independent real-world truth.
    Source/Gold bytes are checked by build_training_snapshot at its construction.
    """
    if not isinstance(snapshot, TrainingSnapshot):
        raise TrainingDataError("expected a TrainingSnapshot")
    instant = normalize_r2_utc(snapshot.as_of)
    for value in (
        snapshot.source_snapshot_sha256,
        snapshot.source_manifest_file_sha256,
        snapshot.gold_demand_sha256,
        snapshot.gold_fulfillment_sha256,
        snapshot.snapshot_sha256,
    ):
        if not isinstance(value, str) or re.fullmatch(r"[a-f0-9]{64}", value) is None:
            raise TrainingDataError("training snapshot digest must be lowercase SHA-256")
    if (
        snapshot.finalization_sha256 is not None
        and re.fullmatch(r"[a-f0-9]{64}", snapshot.finalization_sha256) is None
    ):
        raise TrainingDataError("training finalization digest must be SHA-256")
    if any(
        type(value) is not int or value <= 0
        for value in (snapshot.minimum_history_days, snapshot.minimum_mature_labels)
    ):
        raise TrainingDataError("training evidence thresholds must be positive integers")
    if not isinstance(snapshot.labels, tuple) or any(
        not isinstance(row, TrainingLabel) for row in snapshot.labels
    ):
        raise TrainingDataError("training labels must be an immutable typed tuple")
    for row in snapshot.labels:
        try:
            TrainingLabel.model_validate_json(row.model_dump_json(warnings=False))
        except ValueError as error:
            raise TrainingDataError("training label fields violate their contract") from error
    keys = [row.key for row in snapshot.labels]
    if keys != sorted(set(keys)):
        raise TrainingDataError("training labels must have unique ordered keys")
    if any(
        row.available_at > instant
        or row.correction_closed_at > instant
        or row.demand_date >= instant.date()
        for row in snapshot.labels
    ):
        raise TrainingDataError("training labels are not mature at snapshot as_of")
    counts = dict(snapshot.counts_by_series)
    if (
        len(counts) != len(snapshot.counts_by_series)
        or any(type(value) is not int or value < 0 for value in counts.values())
        or list(counts) != sorted(counts)
    ):
        raise TrainingDataError("training per-series counts must be ordered and nonnegative")
    actual_counts = Counter(f"{row.sku_id}:{row.location_id}" for row in snapshot.labels)
    if set(actual_counts) - set(counts) or any(
        value != actual_counts.get(key, 0) for key, value in counts.items()
    ):
        raise TrainingDataError("training label counts disagree with the receipt")
    if len(dict(snapshot.exclusions)) != len(snapshot.exclusions) or any(
        not isinstance(key, str) or not key or type(value) is not int or value < 0
        for key, value in snapshot.exclusions
    ):
        raise TrainingDataError("training exclusion counts are invalid")
    reasons: list[str] = []
    if snapshot.finalization_sha256 is None:
        reasons.append("no_explicit_correction_closure")
        if snapshot.labels:
            raise TrainingDataError("labels require an explicit finalization binding")
    if len(snapshot.labels) < snapshot.minimum_mature_labels:
        reasons.append("insufficient_distinct_mature_labels")
    if not counts or min(counts.values()) < snapshot.minimum_history_days:
        reasons.append("insufficient_per_series_history")
    expected_status = "ABSTAIN_INSUFFICIENT_EVIDENCE" if reasons else "READY"
    if snapshot.status != expected_status or snapshot.reason_codes != tuple(reasons):
        raise TrainingDataError("training readiness does not match actual evidence support")
    if canonical_json_sha256(_snapshot_payload(snapshot)) != snapshot.snapshot_sha256:
        raise TrainingDataError("training snapshot checksum mismatch")


def _group_rows(
    rows: tuple[R2FulfillmentOutcome, ...],
) -> dict[tuple[str, str, date], tuple[R2FulfillmentOutcome, ...]]:
    identifiers = [row.fulfillment_id for row in rows]
    if len(identifiers) != len(set(identifiers)):
        raise TrainingDataError("duplicate fulfillment business identifier")
    grouped: dict[tuple[str, str, date], list[R2FulfillmentOutcome]] = defaultdict(list)
    for row in rows:
        grouped[(row.sku_id, row.location_id, row.demand_date)].append(row)
    return {key: tuple(values) for key, values in grouped.items()}


def build_training_snapshot(
    rebuild: R2RebuildResult,
    *,
    source_dir: Path,
    finalization_path: Path | None = None,
    expected_finalization_sha256: str | None = None,
    as_of: datetime | None = None,
    minimum_history_days: int = 180,
    minimum_mature_labels: int = 500,
) -> TrainingSnapshot:
    """Verify source/Gold/closure identities and count every excluded daily label.

    READY means the data may enter a fit, not that calibration/evaluation support
    or candidate superiority has passed. Their windows belong to the trainer.
    """
    for value in (minimum_history_days, minimum_mature_labels):
        if type(value) is not int or value <= 0:
            raise TrainingDataError("minimum evidence thresholds must be positive integers")
    instant = normalize_r2_utc(as_of or rebuild.planning_cutoff)
    if instant > rebuild.planning_cutoff:
        raise TrainingDataError("training as_of cannot exceed the verified Gold snapshot cutoff")
    source = source_dir.resolve()
    source_hash = runtime_source_fingerprint(source)
    manifest_file_hash = file_sha256(source / "source-manifest.json")
    if manifest_file_hash != rebuild.source_manifest_file_sha256:
        raise TrainingDataError("source package differs from the Gold source binding")
    demand = adapt_r2_demand_history(rebuild)
    reference = rebuild.gold_artifact("gold.r2_fulfillment_outcomes")
    table = pq.read_table(reference.path)
    physical_rows = cast(list[dict[str, Any]], table.to_pylist())
    if (
        len(physical_rows) != reference.row_count
        or canonical_rows_sha256(physical_rows) != reference.content_sha256
        or canonical_schema_sha256(table.schema) != reference.schema_sha256
    ):
        raise TrainingDataError("Gold fulfillment checksum or schema mismatch")
    fields = set(R2FulfillmentOutcome.model_fields)
    rows = tuple(
        R2FulfillmentOutcome.model_validate_json(
            json.dumps({name: row[name] for name in fields}, default=str)
        )
        for row in physical_rows
    )
    gold_groups = _group_rows(rows)
    demand_keys = {(row.sku_id, row.location_id, row.demand_date) for row in demand.observations}
    if set(gold_groups) != demand_keys:
        raise TrainingDataError("Gold demand and fulfillment daily grains differ")
    source_groups = _group_rows(rebuild.landing.fulfillment_outcomes)
    for key, grouped in gold_groups.items():
        if key not in source_groups or fulfillment_rows_sha256(grouped) != fulfillment_rows_sha256(
            source_groups[key]
        ):
            raise TrainingDataError("Gold fulfillment rows differ from the bound source records")
    closure_hash: str | None = None
    closures: dict[tuple[str, str, date], FinalizationLabel] = {}
    if finalization_path is not None:
        closure_hash = file_sha256(finalization_path)
        if (
            expected_finalization_sha256 is not None
            and closure_hash != expected_finalization_sha256
        ):
            raise TrainingDataError("finalization file checksum mismatch")
        finalization = FinalizationManifest.model_validate_json(
            finalization_path.read_text(encoding="utf-8")
        )
        if finalization.source_snapshot_sha256 != source_hash:
            raise TrainingDataError("finalization source snapshot mismatch")
        closures = {item.key: item for item in finalization.labels}
        for key, declared_closure in closures.items():
            if (
                key not in source_groups
                or declared_closure.source_rows_sha256
                != fulfillment_rows_sha256(source_groups[key])
            ):
                raise TrainingDataError("finalization daily label differs from source rows")
    elif expected_finalization_sha256 is not None:
        raise TrainingDataError("expected finalization hash requires a finalization file")
    exclusions: Counter[str] = Counter()
    labels: list[TrainingLabel] = []
    series_counts = {f"{row.sku_id}:{row.location_id}": 0 for row in demand.observations}
    for observation in demand.observations:
        key = (observation.sku_id, observation.location_id, observation.demand_date)
        records = gold_groups[key]
        available = max(item.available_at for item in records)
        if (
            sum(item.demand_qty for item in records) != observation.demand_qty
            or len(records) != observation.fulfillment_record_count
            or available != observation.latest_available_at
        ):
            raise TrainingDataError("Gold aggregate quantities or availability disagree")
        if observation.demand_date >= instant.date() or available > instant:
            exclusions["future_or_incomplete"] += 1
            continue
        if any(item.fulfilled_qty != item.demand_qty for item in records):
            exclusions["unfulfilled_censored"] += 1
            continue
        closure = closures.get(key)
        if closure is None:
            exclusions["missing_finalization"] += 1
            continue
        if not closure.correction_closed or closure.correction_closed_at > instant:
            exclusions["correction_window_open"] += 1
            continue
        labels.append(
            TrainingLabel(
                sku_id=observation.sku_id,
                location_id=observation.location_id,
                demand_date=observation.demand_date,
                quantity=sum(item.fulfilled_qty for item in records),
                available_at=available,
                correction_closed_at=closure.correction_closed_at,
                fulfillment_record_count=len(records),
                source_rows_sha256=closure.source_rows_sha256,
            )
        )
        series_counts[f"{observation.sku_id}:{observation.location_id}"] += 1
    reasons: list[str] = []
    if finalization_path is None:
        reasons.append("no_explicit_correction_closure")
    if len(labels) < minimum_mature_labels:
        reasons.append("insufficient_distinct_mature_labels")
    if not series_counts or min(series_counts.values()) < minimum_history_days:
        reasons.append("insufficient_per_series_history")
    values: dict[str, Any] = {
        "status": "ABSTAIN_INSUFFICIENT_EVIDENCE" if reasons else "READY",
        "labels": tuple(sorted(labels, key=lambda item: item.key)),
        "exclusions": tuple(sorted(exclusions.items())),
        "reason_codes": tuple(reasons),
        "source_snapshot_sha256": source_hash,
        "source_manifest_file_sha256": manifest_file_hash,
        "gold_demand_sha256": demand.contract.source_content_sha256,
        "gold_fulfillment_sha256": reference.content_sha256,
        "finalization_sha256": closure_hash,
        "as_of": instant,
        "minimum_history_days": minimum_history_days,
        "minimum_mature_labels": minimum_mature_labels,
        "counts_by_series": tuple(sorted(series_counts.items())),
        "snapshot_sha256": "",
    }
    temporary = TrainingSnapshot(**values)
    values["snapshot_sha256"] = canonical_json_sha256(_snapshot_payload(temporary))
    result = TrainingSnapshot(**values)
    validate_training_snapshot(result)
    return result


def pit_history(
    snapshot: TrainingSnapshot, issue_time: datetime
) -> tuple[R3DemandObservation, ...]:
    """Never expose labels whose availability OR correction closure is after issue."""
    issue = normalize_r2_utc(issue_time)
    if issue > snapshot.as_of:
        raise TrainingDataError("history issue exceeds the training snapshot as_of")
    validate_training_snapshot(snapshot)
    return tuple(
        R3DemandObservation(
            demand_history_key=f"{row.demand_date.isoformat()}::{row.sku_id}::{row.location_id}",
            demand_date=row.demand_date,
            sku_location_key=f"{row.sku_id}::{row.location_id}",
            sku_id=row.sku_id,
            location_id=row.location_id,
            demand_qty=row.quantity,
            fulfillment_record_count=row.fulfillment_record_count,
            latest_available_at=max(row.available_at, row.correction_closed_at),
        )
        for row in snapshot.labels
        if row.demand_date < issue.date()
        and row.available_at <= issue
        and row.correction_closed_at <= issue
    )
