"""Verified R2C registration and post-persistence truth access for R3B.

This module is deliberately the only R3B boundary that can expose simulator
truth.  Model features are built in :mod:`nexus.modeling.r3b_features`; latent
demand and cohort labels are available only through the evidence-bearing
post-persistence accessor below.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self, cast

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from nexus.data.r2c_models import R2CGeneratedProfile, R2CManifest
from nexus.modeling.r3b_contracts import R3BConfig, r3b_config_sha256
from nexus.simulation.representative_supply_network import (
    R2C_MANIFEST_NAME,
    iter_r2c_dataset_rows,
    verify_r2c_package,
)
from nexus.storage import file_sha256

_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
_GENERATOR_MODULE_PATH = Path(__file__).resolve().parents[1] / (
    "simulation/representative_supply_network.py"
)


class R3BDataError(RuntimeError):
    """An R2C package cannot safely cross the R3B modeling boundary."""


def _canonical_value(value: object) -> object:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("canonical timestamps must be timezone-aware")
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("canonical decimals must be finite")
        return "0" if value == 0 else format(value.normalize(), "f")
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical row keys must be strings")
            normalized[key] = _canonical_value(item)
        return normalized
    if isinstance(value, (tuple, list)):
        return [_canonical_value(item) for item in value]
    if value is None or type(value) in {str, int, float, bool}:
        return value
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_row_bytes(row: Mapping[str, object]) -> bytes:
    """Serialize one logical row with stable JSON, UTC, and newline rules."""

    normalized = cast(dict[str, object], _canonical_value(row))
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


def canonical_rows_bytes(rows: Sequence[Mapping[str, object]]) -> bytes:
    """Serialize an already canonically ordered logical row sequence."""

    return b"".join(canonical_row_bytes(row) for row in rows)


def canonical_rows_sha256(rows: Sequence[Mapping[str, object]]) -> str:
    """Hash canonical logical rows, including their declared sequence order."""

    return hashlib.sha256(canonical_rows_bytes(rows)).hexdigest()


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)


class R3BRegisteredShard(_FrozenModel):
    """One manifest-declared physical and logical shard identity."""

    dataset: str
    partition: str
    relative_path: str
    row_count: int
    semantic_sha256: str
    schema_sha256: str
    file_sha256: str
    file_bytes: int

    @model_validator(mode="after")
    def values_are_safe(self) -> Self:
        if self.row_count < 0 or self.file_bytes < 0:
            raise ValueError("registered shard counts must be nonnegative")
        for name in ("semantic_sha256", "schema_sha256", "file_sha256"):
            if _SHA256_RE.fullmatch(cast(str, getattr(self, name))) is None:
                raise ValueError(f"{name} must be a lowercase SHA-256")
        path = Path(self.relative_path)
        if path.is_absolute() or ".." in path.parts or "\\" in self.relative_path:
            raise ValueError("registered shard path must be safe and POSIX-relative")
        return self

    def identity_row(self) -> dict[str, object]:
        return self.model_dump(mode="json")


class R3BR2CRegistration(_FrozenModel):
    """Self-verifying identity over one exact, independently verified R2C package."""

    contract_version: Literal["1.0.0"]
    registered_name: Literal["r3b.registered.r2c"]
    r3b_config_sha256: str
    source_generator: Literal["nexus-r2c-representative-supply-network"]
    source_profile: Literal["smoke"]
    profile_contract_sha256: str
    dataset_semantic_sha256: str
    manifest_file_sha256: str
    generator_code_sha256: str
    generated_through: datetime
    supplier_row_count: int
    product_location_row_count: int
    demand_row_count: int
    lead_time_row_count: int
    shards: tuple[R3BRegisteredShard, ...]
    registration_sha256: str

    @field_validator("generated_through")
    @classmethod
    def normalize_generated_through(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("generated_through must be timezone-aware")
        return value.astimezone(UTC)

    @classmethod
    def register(
        cls,
        *,
        config_sha256: str,
        manifest: R2CManifest,
        manifest_file_sha256: str,
        generator_code_sha256: str,
    ) -> R3BR2CRegistration:
        shards = tuple(
            R3BRegisteredShard(**shard.model_dump(mode="python")) for shard in manifest.shards
        )
        counts = Counter[str]()
        for shard in shards:
            counts[shard.dataset] += shard.row_count
        values: dict[str, object] = {
            "contract_version": "1.0.0",
            "registered_name": "r3b.registered.r2c",
            "r3b_config_sha256": config_sha256,
            "source_generator": manifest.generator,
            "source_profile": manifest.profile,
            "profile_contract_sha256": manifest.profile_contract_sha256,
            "dataset_semantic_sha256": manifest.dataset_semantic_sha256,
            "manifest_file_sha256": manifest_file_sha256,
            "generator_code_sha256": generator_code_sha256,
            "generated_through": manifest.generated_through,
            "supplier_row_count": counts["representative.suppliers"],
            "product_location_row_count": counts["representative.product_locations"],
            "demand_row_count": counts["representative.demand_history"],
            "lead_time_row_count": counts["representative.lead_time_history"],
            "shards": shards,
        }
        identity = cls._identity_row(values)
        return cls.model_validate(
            {**values, "registration_sha256": canonical_rows_sha256([identity])},
            strict=True,
        )

    @staticmethod
    def _identity_row(values: Mapping[str, object]) -> dict[str, object]:
        raw_shards = cast(Sequence[object], values["shards"])
        shard_rows = [
            shard.identity_row()
            if isinstance(shard, R3BRegisteredShard)
            else cast(Mapping[str, object], shard)
            for shard in raw_shards
        ]
        return {
            "contract_version": values["contract_version"],
            "dataset_semantic_sha256": values["dataset_semantic_sha256"],
            "demand_row_count": values["demand_row_count"],
            "generated_through": values["generated_through"],
            "generator_code_sha256": values["generator_code_sha256"],
            "lead_time_row_count": values["lead_time_row_count"],
            "manifest_file_sha256": values["manifest_file_sha256"],
            "product_location_row_count": values["product_location_row_count"],
            "profile_contract_sha256": values["profile_contract_sha256"],
            "r3b_config_sha256": values["r3b_config_sha256"],
            "registered_name": values["registered_name"],
            "shards": shard_rows,
            "source_generator": values["source_generator"],
            "source_profile": values["source_profile"],
            "supplier_row_count": values["supplier_row_count"],
        }

    @model_validator(mode="after")
    def identity_is_complete_and_self_verifying(self) -> Self:
        hashes = (
            self.r3b_config_sha256,
            self.profile_contract_sha256,
            self.dataset_semantic_sha256,
            self.manifest_file_sha256,
            self.generator_code_sha256,
            self.registration_sha256,
        )
        if any(_SHA256_RE.fullmatch(value) is None for value in hashes):
            raise ValueError("registration identities must be lowercase SHA-256 values")
        identities = [(shard.dataset, shard.partition) for shard in self.shards]
        if (
            not self.shards
            or identities != sorted(identities)
            or len(identities) != len(set(identities))
        ):
            raise ValueError("registered shards must be nonempty, unique, and canonical")
        counts = Counter[str]()
        for shard in self.shards:
            counts[shard.dataset] += shard.row_count
        expected_counts = (
            counts["representative.suppliers"],
            counts["representative.product_locations"],
            counts["representative.demand_history"],
            counts["representative.lead_time_history"],
        )
        if expected_counts != (
            self.supplier_row_count,
            self.product_location_row_count,
            self.demand_row_count,
            self.lead_time_row_count,
        ):
            raise ValueError("registered row counts do not match declared shards")
        identity = self._identity_row(
            self.model_dump(mode="python", exclude={"registration_sha256"})
        )
        if self.registration_sha256 != canonical_rows_sha256([identity]):
            raise ValueError("registration_sha256 does not bind the complete R2C identity")
        return self


@dataclass(frozen=True, slots=True)
class R3BDataSnapshot:
    """One verified package handle; consumers traverse only its manifest entries."""

    output_dir: Path
    manifest: R2CManifest
    registration: R3BR2CRegistration

    @property
    def contract(self) -> R3BR2CRegistration:
        """R3A-compatible name for the snapshot registration."""

        return self.registration


def register_r2c_package(
    source: R2CGeneratedProfile | Path,
    *,
    config: R3BConfig,
    generator_code_path: Path | None = None,
) -> R3BDataSnapshot:
    """Audit and register one exact smoke package without filesystem shard discovery."""

    output_dir = (
        source.output_dir if isinstance(source, R2CGeneratedProfile) else source
    ).resolve()
    expected_profile = config.data_contract.source_profile
    if expected_profile != "smoke":
        raise R3BDataError("R3B Wave 1 registers only the verified R2C smoke profile")
    manifest_path = output_dir / R2C_MANIFEST_NAME
    try:
        manifest_hash = file_sha256(manifest_path)
        manifest = verify_r2c_package(output_dir)
    except Exception as error:
        raise R3BDataError(f"R2C package verification failed: {error}") from error
    if manifest.profile != expected_profile or manifest.classification != "synthetic":
        raise R3BDataError("R2C source profile or classification does not match R3B")
    if manifest.generator != "nexus-r2c-representative-supply-network":
        raise R3BDataError("R2C source generator is not approved for R3B")
    if isinstance(source, R2CGeneratedProfile):
        if source.manifest != manifest or source.manifest_path.resolve() != manifest_path.resolve():
            raise R3BDataError("generated-package handle does not match its verified manifest")
        if source.manifest_file_sha256 != manifest_hash:
            raise R3BDataError("generated-package manifest hash does not match physical bytes")
    code_path = (generator_code_path or _GENERATOR_MODULE_PATH).resolve()
    if not code_path.is_file():
        raise R3BDataError("R2C generator source code is unavailable for registration")
    registration = R3BR2CRegistration.register(
        config_sha256=r3b_config_sha256(config),
        manifest=manifest,
        manifest_file_sha256=manifest_hash,
        generator_code_sha256=file_sha256(code_path),
    )
    return R3BDataSnapshot(
        output_dir=output_dir,
        manifest=manifest,
        registration=registration,
    )


R3BForecastKey = tuple[str, str, date, int]


@dataclass(frozen=True, slots=True)
class R3BEvaluationTruth:
    """Privileged simulator truth joined only after a forecast artifact exists."""

    sku_id: str
    location_id: str
    target_date: date
    horizon_days: int
    observed_sales_qty: int
    synthetic_latent_demand_qty: int
    synthetic_cohort: str
    is_active: bool
    stockout_censored: bool
    label_available_at: datetime

    @property
    def forecast_key(self) -> R3BForecastKey:
        return (self.sku_id, self.location_id, self.target_date, self.horizon_days)


@dataclass(frozen=True, slots=True)
class R3BPersistedForecast:
    """One validated forecast row reconstructed from immutable artifact bytes."""

    sku_id: str
    location_id: str
    target_date: date
    horizon_days: int
    q025: Decimal
    q10: Decimal
    q50: Decimal
    q90: Decimal
    q975: Decimal
    cold_start: bool

    @property
    def forecast_key(self) -> R3BForecastKey:
        return (self.sku_id, self.location_id, self.target_date, self.horizon_days)

    @property
    def quantiles(self) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
        return (self.q025, self.q10, self.q50, self.q90, self.q975)


@dataclass(frozen=True, slots=True)
class R3BEvaluationBundle:
    """Truth rows bound to the already-persisted forecast artifact receipt."""

    forecast_artifact_sha256: str
    forecast_artifact_file_sha256: str
    forecast_artifact_path: Path
    evaluation_cutoff: datetime
    baseline: tuple[R3BPersistedForecast, ...]
    candidate: tuple[R3BPersistedForecast, ...]
    rows: tuple[R3BEvaluationTruth, ...]

    def __iter__(self):  # type: ignore[no-untyped-def]
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, index: int) -> R3BEvaluationTruth:
        return self.rows[index]


def _forecast_records_from_rows(payload: object, *, role: str) -> tuple[R3BPersistedForecast, ...]:
    if not isinstance(payload, list):
        raise R3BDataError(f"forecast artifact {role} payload must be an array")
    expected_fields = {
        "cold_start",
        "horizon_days",
        "location_id",
        "q025",
        "q10",
        "q50",
        "q90",
        "q975",
        "sku_id",
        "target_date",
    }
    records: list[R3BPersistedForecast] = []
    for item in payload:
        if not isinstance(item, dict) or set(item) != expected_fields:
            raise R3BDataError(f"forecast artifact {role} row violates the frozen schema")
        try:
            target_date = date.fromisoformat(cast(str, item["target_date"]))
        except (TypeError, ValueError) as error:
            raise R3BDataError("forecast artifact target_date is invalid") from error
        sku_id = item["sku_id"]
        location_id = item["location_id"]
        horizon = item["horizon_days"]
        if (
            not isinstance(sku_id, str)
            or not sku_id
            or not isinstance(location_id, str)
            or not location_id
            or isinstance(horizon, bool)
            or not isinstance(horizon, int)
            or not isinstance(item["cold_start"], bool)
        ):
            raise R3BDataError("forecast artifact key or cold-start fields are invalid")
        try:
            quantiles = tuple(
                Decimal(str(item[name])) for name in ("q025", "q10", "q50", "q90", "q975")
            )
        except Exception as error:
            raise R3BDataError("forecast artifact quantiles are invalid") from error
        if any(not value.is_finite() or value < 0 for value in quantiles):
            raise R3BDataError("forecast artifact quantiles must be finite and nonnegative")
        if quantiles != tuple(sorted(quantiles)):
            raise R3BDataError("forecast artifact quantiles must be monotone")
        records.append(
            R3BPersistedForecast(
                sku_id=sku_id,
                location_id=location_id,
                target_date=target_date,
                horizon_days=horizon,
                q025=quantiles[0],
                q10=quantiles[1],
                q50=quantiles[2],
                q90=quantiles[3],
                q975=quantiles[4],
                cold_start=item["cold_start"],
            )
        )
    return tuple(records)


def _persisted_forecast_receipt(
    path: Path,
) -> tuple[
    str,
    str,
    tuple[R3BPersistedForecast, ...],
    tuple[R3BPersistedForecast, ...],
]:
    if path.is_symlink():
        raise R3BDataError("forecast artifact must be a persisted regular file")
    resolved = path.resolve()
    if not resolved.is_file():
        raise R3BDataError("forecast artifact must be a persisted regular file")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise R3BDataError(f"forecast artifact is not readable canonical JSON: {error}") from error
    if not isinstance(payload, dict) or set(payload) != {
        "baseline",
        "candidate",
        "contract",
        "origin",
    }:
        raise R3BDataError("forecast artifact root does not match the frozen bundle contract")
    if payload["contract"] != "nexus.r3b.persisted-forecast-bundle.v1":
        raise R3BDataError("forecast artifact contract is not approved for truth access")
    try:
        canonical = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise R3BDataError("forecast artifact contains noncanonical values") from error
    content_sha256 = hashlib.sha256(canonical).hexdigest()
    if resolved.suffix != ".json" or resolved.stem != content_sha256:
        raise R3BDataError("forecast artifact path is not bound to its canonical content hash")
    try:
        origin = datetime.fromisoformat(cast(str, payload["origin"]).replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as error:
        raise R3BDataError("forecast artifact origin is invalid") from error
    if origin.tzinfo is None or origin.utcoffset() is None:
        raise R3BDataError("forecast artifact origin must be timezone-aware")
    baseline = _forecast_records_from_rows(payload["baseline"], role="baseline")
    candidate = _forecast_records_from_rows(payload["candidate"], role="candidate")
    baseline_keys = tuple(row.forecast_key for row in baseline)
    candidate_keys = tuple(row.forecast_key for row in candidate)
    if baseline_keys != candidate_keys:
        raise R3BDataError("forecast artifact baseline and candidate keys must be paired")
    return content_sha256, file_sha256(resolved), baseline, candidate


def get_evaluation_truth_after_persistence(
    snapshot: R3BDataSnapshot,
    *,
    forecast_artifact_path: Path,
    evaluation_cutoff: datetime | None = None,
) -> R3BEvaluationBundle:
    """Join truth to keys independently re-read from a content-addressed forecast artifact."""

    (
        forecast_artifact_sha256,
        artifact_file_sha256,
        baseline,
        candidate,
    ) = _persisted_forecast_receipt(forecast_artifact_path)
    keys = tuple(row.forecast_key for row in candidate)
    if not keys:
        raise R3BDataError("persisted forecast artifact candidate rows must be nonempty")
    if len(keys) != len(set(keys)):
        raise R3BDataError("persisted_prediction_keys must be unique")
    if any(horizon not in {7, 14, 28} for _, _, _, horizon in keys):
        raise R3BDataError("persisted prediction key contains an undeclared horizon")
    cutoff = evaluation_cutoff or snapshot.manifest.generated_through
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise R3BDataError("evaluation_cutoff must be timezone-aware")
    cutoff = cutoff.astimezone(UTC)

    by_grain: dict[tuple[str, str, date], dict[str, object]] = {}
    try:
        rows = iter_r2c_dataset_rows(
            snapshot.output_dir,
            dataset="representative.demand_history",
            verified_manifest=snapshot.manifest,
        )
        for row in rows:
            grain = (
                cast(str, row["sku_id"]),
                cast(str, row["location_id"]),
                cast(date, row["demand_date"]),
            )
            if grain in by_grain:
                raise R3BDataError("R2C demand truth contains a duplicate grain")
            by_grain[grain] = row
    except R3BDataError:
        raise
    except Exception as error:
        raise R3BDataError(f"cannot read registered R2C demand truth: {error}") from error

    truth: list[R3BEvaluationTruth] = []
    missing: list[R3BForecastKey] = []
    for sku_id, location_id, target_date, horizon in sorted(keys):
        truth_row = by_grain.get((sku_id, location_id, target_date))
        if truth_row is None:
            missing.append((sku_id, location_id, target_date, horizon))
            continue
        available_at = cast(datetime, truth_row["available_at"]).astimezone(UTC)
        if available_at > cutoff:
            missing.append((sku_id, location_id, target_date, horizon))
            continue
        truth.append(
            R3BEvaluationTruth(
                sku_id=sku_id,
                location_id=location_id,
                target_date=target_date,
                horizon_days=horizon,
                observed_sales_qty=cast(int, truth_row["observed_sales_qty"]),
                synthetic_latent_demand_qty=cast(int, truth_row["synthetic_latent_demand_qty"]),
                synthetic_cohort=cast(str, truth_row["synthetic_cohort"]),
                is_active=cast(bool, truth_row["is_active"]),
                stockout_censored=cast(bool, truth_row["stockout_censored"]),
                label_available_at=available_at,
            )
        )
    if missing:
        raise R3BDataError(
            "persisted forecast keys must have complete mature evaluation truth; "
            f"missing={len(missing)}"
        )
    return R3BEvaluationBundle(
        forecast_artifact_sha256=forecast_artifact_sha256,
        forecast_artifact_file_sha256=artifact_file_sha256,
        forecast_artifact_path=forecast_artifact_path.resolve(),
        evaluation_cutoff=cutoff,
        baseline=baseline,
        candidate=candidate,
        rows=tuple(truth),
    )


__all__ = [
    "R3BDataError",
    "R3BDataSnapshot",
    "R3BEvaluationBundle",
    "R3BEvaluationTruth",
    "R3BForecastKey",
    "R3BPersistedForecast",
    "R3BR2CRegistration",
    "R3BRegisteredShard",
    "canonical_row_bytes",
    "canonical_rows_bytes",
    "canonical_rows_sha256",
    "get_evaluation_truth_after_persistence",
    "register_r2c_package",
]
