"""Immutable scientific-artifact helpers for the bounded R3A foundation."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Literal, cast

import pyarrow as pa

from nexus.modeling.r3_hierarchy import HierarchicalQuantileForecast, HierarchyLevel
from nexus.storage import (
    ArtifactConflictError,
    canonical_rows_sha256,
    file_sha256,
    write_immutable_parquet,
)

FORECAST_ARTIFACT_SCHEMA = pa.schema(
    [
        pa.field("issue_time", pa.string(), nullable=False),
        pa.field("target_date", pa.string(), nullable=False),
        pa.field("hierarchy_level", pa.string(), nullable=False),
        pa.field("sku_id", pa.string(), nullable=True),
        pa.field("location_id", pa.string(), nullable=True),
        pa.field("quantile", pa.float64(), nullable=False),
        pa.field("predicted_quantity", pa.float64(), nullable=False),
    ],
    metadata={b"nexus.contract": b"r3.forecast.v1"},
)


EVALUATION_ARTIFACT_SCHEMA = pa.schema(
    [
        pa.field("model_name", pa.string(), nullable=False),
        pa.field("model_version", pa.string(), nullable=False),
        pa.field("segment_name", pa.string(), nullable=False),
        pa.field("segment_value", pa.string(), nullable=False),
        pa.field("horizon_days", pa.int32(), nullable=False),
        pa.field("observation_count", pa.int64(), nullable=False),
        pa.field("pinball_q025", pa.float64(), nullable=False),
        pa.field("pinball_q10", pa.float64(), nullable=False),
        pa.field("pinball_q50", pa.float64(), nullable=False),
        pa.field("pinball_q90", pa.float64(), nullable=False),
        pa.field("pinball_q975", pa.float64(), nullable=False),
        pa.field("interval_score_80", pa.float64(), nullable=False),
        pa.field("interval_coverage_80", pa.float64(), nullable=False),
        pa.field("interval_score_95", pa.float64(), nullable=False),
        pa.field("interval_coverage_95", pa.float64(), nullable=False),
        pa.field("weighted_interval_score", pa.float64(), nullable=False),
        pa.field("bias", pa.float64(), nullable=False),
        pa.field("mase", pa.float64(), nullable=True),
    ],
    metadata={b"nexus.contract": b"r3.evaluation-summary.v1"},
)


@dataclass(frozen=True, slots=True)
class ScientificArtifactRef:
    """Complete logical and physical identity of one immutable scientific artifact."""

    dataset: str
    format: Literal["json", "parquet"]
    path: Path
    content_sha256: str
    schema_sha256: str
    file_sha256: str
    row_count: int


@dataclass(frozen=True, slots=True)
class EvaluationArtifactRow:
    """One immutable segment/horizon metric summary suitable for promotion evidence."""

    model_name: str
    model_version: str
    segment_name: str
    segment_value: str
    horizon_days: int
    observation_count: int
    pinball_q025: Decimal
    pinball_q10: Decimal
    pinball_q50: Decimal
    pinball_q90: Decimal
    pinball_q975: Decimal
    interval_score_80: Decimal
    interval_coverage_80: Decimal
    interval_score_95: Decimal
    interval_coverage_95: Decimal
    weighted_interval_score: Decimal
    bias: Decimal
    mase: Decimal | None


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("canonical JSON cannot contain a non-finite Decimal")
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _normalize_json(value: object) -> object:
    """Convert supported scientific values into an unambiguous JSON value."""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON cannot contain a non-finite float")
        return value
    if isinstance(value, Decimal):
        return _canonical_decimal(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("canonical JSON datetimes must include a timezone offset")
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Enum):
        return _normalize_json(value.value)
    if is_dataclass(value) and not isinstance(value, type):
        return _normalize_json(asdict(cast(Any, value)))
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical JSON object keys must be strings")
            normalized[key] = _normalize_json(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize_json(item) for item in value]
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


def canonical_json_bytes(value: object) -> bytes:
    """Encode a supported value as canonical UTF-8 JSON without a trailing newline."""

    return json.dumps(
        _normalize_json(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def canonical_json_sha256(value: object) -> str:
    """Hash canonical logical JSON independently from its physical file framing."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _dataset_directory(output_dir: Path, dataset: str) -> Path:
    segments = dataset.split(".")
    if not segments or any(
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", segment) is None for segment in segments
    ):
        raise ValueError(f"invalid artifact dataset name: {dataset!r}")
    output_root = output_dir.resolve()
    target_dir = output_root.joinpath(*segments)
    if not target_dir.resolve().is_relative_to(output_root):
        raise ValueError(f"artifact dataset escapes output directory: {dataset!r}")
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir


def _publish_once(target: Path, payload: bytes) -> None:
    """Publish complete bytes without replacing an already-visible immutable path."""

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{target.stem}.", suffix=".tmp", dir=target.parent, delete=False
        ) as temporary:
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        with suppress(FileExistsError):
            os.link(temporary_path, target)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def write_immutable_json_manifest(
    output_dir: Path,
    *,
    dataset: str,
    manifest: object,
    schema_identity: object | None = None,
) -> ScientificArtifactRef:
    """Atomically write one canonical, content-addressed immutable JSON manifest."""

    canonical = canonical_json_bytes(manifest)
    content_sha256 = hashlib.sha256(canonical).hexdigest()
    payload = canonical + b"\n"
    target = _dataset_directory(output_dir, dataset) / f"{content_sha256}.json"
    _publish_once(target, payload)
    try:
        existing = target.read_bytes()
    except OSError as exc:
        raise ArtifactConflictError(f"cannot read immutable manifest: {target}") from exc
    if existing != payload:
        raise ArtifactConflictError(f"content-addressed manifest is corrupt: {target}")
    try:
        parsed = json.loads(existing)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactConflictError(
            f"content-addressed manifest is invalid JSON: {target}"
        ) from exc
    if canonical_json_sha256(parsed) != content_sha256:
        raise ArtifactConflictError(f"content-addressed manifest has conflicting content: {target}")

    schema = (
        schema_identity
        if schema_identity is not None
        else {"format": "canonical-json", "kind": "scientific-manifest", "version": "1.0.0"}
    )
    return ScientificArtifactRef(
        dataset=dataset,
        format="json",
        path=target,
        content_sha256=content_sha256,
        schema_sha256=canonical_json_sha256(schema),
        file_sha256=file_sha256(target),
        row_count=1,
    )


def _utc_text(value: object, field: str) -> str:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise TypeError(f"{field} must be a datetime or ISO-8601 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone offset")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _date_text(value: object, field: str) -> str:
    if isinstance(value, datetime):
        raise TypeError(f"{field} must be a date, not a datetime")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        return date.fromisoformat(value).isoformat()
    raise TypeError(f"{field} must be a date or ISO-8601 date string")


def _text(value: object, field: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonblank string")
    return value.strip()


def _finite_float(value: object, field: str, *, nullable: bool = False) -> float | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float)):
        raise TypeError(f"{field} must be numeric")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field} must be finite")
    return normalized


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
    return value


def _forecast_mapping(
    row: HierarchicalQuantileForecast | Mapping[str, object],
) -> dict[str, object]:
    if isinstance(row, HierarchicalQuantileForecast):
        source: Mapping[str, object] = {
            "issue_time": row.issue_time,
            "target_date": row.target_date,
            "hierarchy_level": row.level.value,
            "sku_id": row.sku_id,
            "location_id": row.location_id,
            "quantile": row.quantile,
            "predicted_quantity": row.predicted_quantity,
        }
    else:
        source = row
    level_value = source.get("hierarchy_level", source.get("level"))
    level = level_value.value if isinstance(level_value, HierarchyLevel) else level_value
    return {
        "issue_time": _utc_text(source.get("issue_time"), "issue_time"),
        "target_date": _date_text(source.get("target_date"), "target_date"),
        "hierarchy_level": _text(level, "hierarchy_level"),
        "sku_id": _text(source.get("sku_id"), "sku_id", nullable=True),
        "location_id": _text(source.get("location_id"), "location_id", nullable=True),
        "quantile": _finite_float(source.get("quantile"), "quantile"),
        "predicted_quantity": _finite_float(source.get("predicted_quantity"), "predicted_quantity"),
    }


def _evaluation_mapping(
    row: EvaluationArtifactRow | Mapping[str, object],
) -> dict[str, object]:
    source = cast(Mapping[str, object], asdict(row)) if is_dataclass(row) else row
    return {
        "model_name": _text(source.get("model_name"), "model_name"),
        "model_version": _text(source.get("model_version"), "model_version"),
        "segment_name": _text(source.get("segment_name"), "segment_name"),
        "segment_value": _text(source.get("segment_value"), "segment_value"),
        "horizon_days": _positive_int(source.get("horizon_days"), "horizon_days"),
        "observation_count": _nonnegative_int(source.get("observation_count"), "observation_count"),
        "pinball_q025": _finite_float(source.get("pinball_q025"), "pinball_q025"),
        "pinball_q10": _finite_float(source.get("pinball_q10"), "pinball_q10"),
        "pinball_q50": _finite_float(source.get("pinball_q50"), "pinball_q50"),
        "pinball_q90": _finite_float(source.get("pinball_q90"), "pinball_q90"),
        "pinball_q975": _finite_float(source.get("pinball_q975"), "pinball_q975"),
        "interval_score_80": _finite_float(source.get("interval_score_80"), "interval_score_80"),
        "interval_coverage_80": _finite_float(
            source.get("interval_coverage_80"), "interval_coverage_80"
        ),
        "interval_score_95": _finite_float(source.get("interval_score_95"), "interval_score_95"),
        "interval_coverage_95": _finite_float(
            source.get("interval_coverage_95"), "interval_coverage_95"
        ),
        "weighted_interval_score": _finite_float(
            source.get("weighted_interval_score"), "weighted_interval_score"
        ),
        "bias": _finite_float(source.get("bias"), "bias"),
        "mase": _finite_float(source.get("mase"), "mase", nullable=True),
    }


def _parquet_ref(
    output_dir: Path,
    *,
    dataset: str,
    rows: Sequence[Mapping[str, object]],
    schema: pa.Schema,
) -> ScientificArtifactRef:
    logical_rows = [dict(row) for row in rows]
    content_sha256 = canonical_rows_sha256(logical_rows)
    expected_target = _dataset_directory(output_dir, dataset) / f"{content_sha256}.parquet"
    target_existed = expected_target.exists()
    try:
        artifact = write_immutable_parquet(
            output_dir,
            dataset=dataset,
            rows=logical_rows,
            schema=schema,
        )
    except ArtifactConflictError:
        raise
    except Exception as exc:
        if target_existed or expected_target.exists():
            raise ArtifactConflictError(
                f"content-addressed Parquet artifact is corrupt: {expected_target}"
            ) from exc
        raise
    return ScientificArtifactRef(
        dataset=artifact.dataset,
        format="parquet",
        path=artifact.path,
        content_sha256=artifact.content_sha256,
        schema_sha256=artifact.schema_sha256,
        file_sha256=file_sha256(artifact.path),
        row_count=artifact.row_count,
    )


def write_forecast_rows(
    output_dir: Path,
    *,
    dataset: str,
    rows: Sequence[HierarchicalQuantileForecast | Mapping[str, object]],
) -> ScientificArtifactRef:
    """Write deterministic long-form hierarchical quantile forecasts to Parquet."""

    logical_rows = [_forecast_mapping(row) for row in rows]
    logical_rows.sort(
        key=lambda row: (
            cast(str, row["issue_time"]),
            cast(str, row["target_date"]),
            cast(str, row["hierarchy_level"]),
            cast(str | None, row["sku_id"]) or "",
            cast(str | None, row["location_id"]) or "",
            cast(float, row["quantile"]),
        )
    )
    return _parquet_ref(
        output_dir,
        dataset=dataset,
        rows=logical_rows,
        schema=FORECAST_ARTIFACT_SCHEMA,
    )


def write_evaluation_rows(
    output_dir: Path,
    *,
    dataset: str,
    rows: Sequence[EvaluationArtifactRow | Mapping[str, object]],
) -> ScientificArtifactRef:
    """Write deterministic segment/horizon evaluation summaries to Parquet."""

    logical_rows = [_evaluation_mapping(row) for row in rows]
    logical_rows.sort(
        key=lambda row: (
            cast(str, row["model_name"]),
            cast(str, row["model_version"]),
            cast(str, row["segment_name"]),
            cast(str, row["segment_value"]),
            cast(int, row["horizon_days"]),
        )
    )
    return _parquet_ref(
        output_dir,
        dataset=dataset,
        rows=logical_rows,
        schema=EVALUATION_ARTIFACT_SCHEMA,
    )


write_forecast_artifact = write_forecast_rows
write_evaluation_artifact = write_evaluation_rows


__all__ = [
    "EVALUATION_ARTIFACT_SCHEMA",
    "FORECAST_ARTIFACT_SCHEMA",
    "EvaluationArtifactRow",
    "ScientificArtifactRef",
    "canonical_json_bytes",
    "canonical_json_sha256",
    "write_evaluation_artifact",
    "write_evaluation_rows",
    "write_forecast_artifact",
    "write_forecast_rows",
    "write_immutable_json_manifest",
]
