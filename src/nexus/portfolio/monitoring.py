"""Measured, local forecast/source monitoring with a durable transition journal.

This is distinct from the manual incident exercise. Source freshness measures the
last successful ingestion, not the age of the newest historical demand date.
Forecast checks use only complete, arrived, correction-closed mature outcomes.
They report evidence, never retrain or promote a model. Hashes bind the supplied
inputs; they are not proof that an external caller supplied truthful provenance.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from nexus.modeling.r3_metrics import ForecastMetricRow, summarize_probabilistic_metrics


def _text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonblank string")
    return value.strip()


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(UTC)


def _day(value: date) -> date:
    if type(value) is not date:
        raise ValueError("target_date must be a date, not a datetime")
    return value


def _day_end(value: date) -> datetime:
    return datetime.combine(value + timedelta(days=1), time.min, UTC)


def _number(value: Decimal | int | float, name: str, *, nonnegative: bool = True) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float)):
        raise ValueError(f"{name} must be a finite number")
    result = Decimal(str(value))
    if not result.is_finite() or (nonnegative and result < 0):
        raise ValueError(f"{name} must be finite and nonnegative")
    return result


def _digest(value: str, name: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _json_default(value: object) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return "0" if value == 0 else format(value.normalize(), "f")
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def _json(value: object) -> str:
    return json.dumps(value, default=_json_default, sort_keys=True, separators=(",", ":"))


def _sha(value: object) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class MonitorForecast:
    """One point from a persisted five-quantile forecast artifact.

    ``issue_time`` must precede the completed daily outcome. Same-day forecasts
    are permitted, as in R3, but a forecast issued after actual availability is
    ineligible. Artifact loading/checksum verification belongs to the producer.
    """

    sku_id: str
    location_id: str
    issue_time: datetime
    target_date: date
    q025: Decimal
    q10: Decimal
    q50: Decimal
    q90: Decimal
    q975: Decimal
    model_id: str
    artifact_sha256: str

    def __post_init__(self) -> None:
        for name in ("sku_id", "location_id", "model_id"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        object.__setattr__(self, "issue_time", _utc(self.issue_time, "issue_time"))
        _day(self.target_date)
        if self.target_date < self.issue_time.date():
            raise ValueError("forecast target_date cannot precede its issue date")
        _digest(self.artifact_sha256, "artifact_sha256")
        for name in ("q025", "q10", "q50", "q90", "q975"):
            object.__setattr__(self, name, _number(getattr(self, name), name))
        if not self.q025 <= self.q10 <= self.q50 <= self.q90 <= self.q975:
            raise ValueError("forecast quantiles must be monotonic")

    @property
    def outcome_key(self) -> tuple[str, str, date]:
        return self.sku_id, self.location_id, self.target_date

    @property
    def key(self) -> tuple[str, str, datetime, date, str]:
        return self.sku_id, self.location_id, self.issue_time, self.target_date, self.model_id


@dataclass(frozen=True, slots=True)
class MonitorActual:
    """A version-selected daily outcome with explicit correction-window closure.

    A late arrival may occur after its maturity deadline; both timestamps must
    nevertheless be at or before evaluation. Producers must resolve revisions
    before supplying this contract, because duplicate outcome keys fail closed.
    """

    sku_id: str
    location_id: str
    target_date: date
    quantity: Decimal
    available_at: datetime
    mature_after: datetime
    correction_closed: bool

    def __post_init__(self) -> None:
        for name in ("sku_id", "location_id"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        _day(self.target_date)
        object.__setattr__(self, "quantity", _number(self.quantity, "quantity"))
        for name in ("available_at", "mature_after"):
            object.__setattr__(self, name, _utc(getattr(self, name), name))
            if getattr(self, name) < _day_end(self.target_date):
                raise ValueError(f"{name} cannot precede completion of the daily outcome")
        if not isinstance(self.correction_closed, bool):
            raise ValueError("correction_closed must be a boolean")

    @property
    def key(self) -> tuple[str, str, date]:
        return self.sku_id, self.location_id, self.target_date


@dataclass(frozen=True, slots=True)
class MonitorConfig:
    """Inclusive healthy boundaries; breach means strictly beyond a threshold.

    The roadmap defaults are 500 distinct mature outcomes, ingestion age beyond
    twice its expected cadence, and 80% coverage more than five percentage points
    from nominal. Bias/WAPE alert limits are opt-in, domain-specific thresholds.
    """

    min_mature_observations: int = 500
    expected_source_cadence_seconds: int = 86_400
    freshness_multiplier: Decimal = Decimal("2")
    nominal_coverage: Decimal = Decimal("0.80")
    coverage_tolerance: Decimal = Decimal("0.05")
    max_abs_bias_units: Decimal | None = None
    max_wape: Decimal | None = None

    def __post_init__(self) -> None:
        for name in ("min_mature_observations", "expected_source_cadence_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("freshness_multiplier", "nominal_coverage", "coverage_tolerance"):
            object.__setattr__(self, name, _number(getattr(self, name), name))
        if self.freshness_multiplier == 0:
            raise ValueError("freshness_multiplier must be positive")
        if self.nominal_coverage != Decimal("0.80"):
            raise ValueError("q10/q90 measure an 80% interval; nominal_coverage must be 0.80")
        if self.coverage_tolerance >= 1:
            raise ValueError("coverage_tolerance must be less than one")
        for name in ("max_abs_bias_units", "max_wape"):
            if getattr(self, name) is not None:
                object.__setattr__(self, name, _number(getattr(self, name), name))


@dataclass(frozen=True, slots=True)
class MonitorInput:
    """A point-in-time snapshot; unknown ingestion freshness is explicitly None."""

    now: datetime
    source_watermark: datetime | None
    forecasts: tuple[MonitorForecast, ...]
    actuals: tuple[MonitorActual, ...]
    source_id: str
    evidence_sha256: str
    expected_model_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "now", _utc(self.now, "now"))
        object.__setattr__(self, "source_id", _text(self.source_id, "source_id"))
        _digest(self.evidence_sha256, "evidence_sha256")
        if not isinstance(self.expected_model_ids, tuple):
            raise ValueError("expected_model_ids must be a tuple of unique model identifiers")
        expected = tuple(
            _text(model_id, "expected_model_id") for model_id in self.expected_model_ids
        )
        if len(expected) != len(set(expected)):
            raise ValueError("expected_model_ids must not contain duplicate model identifiers")
        object.__setattr__(self, "expected_model_ids", tuple(sorted(expected)))
        if self.source_watermark is not None:
            object.__setattr__(
                self, "source_watermark", _utc(self.source_watermark, "source_watermark")
            )
            if self.source_watermark > self.now:
                raise ValueError("source_watermark cannot be in the future")
        for name, row_type in (("forecasts", MonitorForecast), ("actuals", MonitorActual)):
            rows = getattr(self, name)
            if not isinstance(rows, tuple) or any(not isinstance(row, row_type) for row in rows):
                raise ValueError(f"{name} must be a tuple of {row_type.__name__} rows")
            keys = [row.key for row in rows]
            if len(keys) != len(set(keys)):
                raise ValueError(f"duplicate {name} keys are not permitted, even identical rows")


def _forecast_check(
    model_id: str, forecasts: list[MonitorForecast], data: MonitorInput, config: MonitorConfig
) -> dict[str, Any]:
    actuals = {row.key: row for row in data.actuals}
    exclusions: dict[str, int] = defaultdict(int)
    rows: list[ForecastMetricRow] = []
    outcome_keys: set[tuple[str, str, date]] = set()
    for forecast in forecasts:
        actual = actuals.get(forecast.outcome_key)
        if forecast.issue_time > data.now:
            exclusions["forecast_not_yet_issued"] += 1
        elif actual is None:
            exclusions["missing_actual"] += 1
        elif _day_end(actual.target_date) > data.now:
            exclusions["incomplete_outcome_day"] += 1
        elif actual.available_at > data.now:
            exclusions["actual_not_yet_available"] += 1
        elif actual.mature_after > data.now:
            exclusions["maturity_deadline_not_reached"] += 1
        elif not actual.correction_closed:
            exclusions["correction_window_open"] += 1
        elif forecast.issue_time >= actual.available_at:
            exclusions["forecast_not_before_outcome"] += 1
        else:
            outcome_keys.add(actual.key)
            rows.append(
                ForecastMetricRow(
                    sku_id=forecast.sku_id,
                    location_id=forecast.location_id,
                    origin_time=forecast.issue_time,
                    target_date=forecast.target_date,
                    actual=actual.quantity,
                    q025=forecast.q025,
                    q10=forecast.q10,
                    q50=forecast.q50,
                    q90=forecast.q90,
                    q975=forecast.q975,
                )
            )
    metrics: dict[str, Any] = {}
    breaches: list[str] = []
    if rows:
        summary = summarize_probabilistic_metrics(rows)
        total_actual = sum((row.actual for row in rows), start=Decimal(0))
        metrics = {
            "bias_units": summary.bias,
            "coverage_80": summary.interval_coverage_80,
            "wis": summary.weighted_interval_score,
            "wape": (
                sum((abs(row.q50 - row.actual) for row in rows), start=Decimal(0)) / total_actual
                if total_actual > 0
                else None
            ),
            "wape_defined": total_actual > 0,
        }
    status = "insufficient_evidence"
    if len(outcome_keys) >= config.min_mature_observations:
        if abs(metrics["coverage_80"] - config.nominal_coverage) > config.coverage_tolerance:
            breaches.append("coverage_80_outside_tolerance")
        if (
            config.max_abs_bias_units is not None
            and abs(metrics["bias_units"]) > config.max_abs_bias_units
        ):
            breaches.append("absolute_bias_above_limit")
        if (
            config.max_wape is not None
            and metrics["wape"] is not None
            and metrics["wape"] > config.max_wape
        ):
            breaches.append("wape_above_limit")
        status = "firing" if breaches else "healthy"
        if not breaches and config.max_wape is not None and metrics["wape"] is None:
            status = "unknown"
    return {
        "kind": "forecast_quality",
        "model_id": model_id,
        "status": status,
        "forecast_rows": len(forecasts),
        "mature_forecast_rows": len(rows),
        "distinct_mature_outcomes": len(outcome_keys),
        "minimum_distinct_mature_outcomes": config.min_mature_observations,
        "excluded_forecast_rows": dict(sorted(exclusions.items())),
        "mature_target_date_range": (
            [min(row.target_date for row in rows), max(row.target_date for row in rows)]
            if rows
            else None
        ),
        "window_selection": "Caller supplies the explicit forecast/outcome evaluation window.",
        "metrics": metrics,
        "breaches": breaches,
        "reason": (
            "Fewer distinct mature outcomes than the configured minimum; not a healthy verdict."
            if status == "insufficient_evidence"
            else "WAPE is undefined because the actual total is zero."
            if status == "unknown"
            else "Measured thresholds breached."
            if breaches
            else "All configured checks are within inclusive threshold boundaries."
        ),
    }


def _evaluate(data: MonitorInput, config: MonitorConfig) -> dict[str, Any]:
    forecasts = tuple(sorted(data.forecasts, key=lambda row: row.key))
    actuals = tuple(sorted(data.actuals, key=lambda row: row.key))
    input_hash = _sha(
        {
            "source_id": data.source_id,
            "source_watermark": data.source_watermark,
            "evidence_sha256": data.evidence_sha256,
            "forecasts": [asdict(row) for row in forecasts],
            "actuals": [asdict(row) for row in actuals],
            # Preserve legacy empty-scope input identities; explicit scope is bound.
            **({"expected_model_ids": data.expected_model_ids} if data.expected_model_ids else {}),
        }
    )
    age = (
        None
        if data.source_watermark is None
        else Decimal(str((data.now - data.source_watermark).total_seconds()))
    )
    limit = config.expected_source_cadence_seconds * config.freshness_multiplier
    available_dates = [
        row.target_date
        for row in actuals
        if row.available_at <= data.now and _day_end(row.target_date) <= data.now
    ]
    latest_date = max(available_dates) if available_dates else None
    freshness = {
        "kind": "source_freshness",
        "status": "unknown" if age is None else "firing" if age > limit else "healthy",
        "watermark": data.source_watermark,
        "ingestion_age_seconds": age,
        "threshold_seconds": limit,
        "latest_available_observation_date": latest_date,
        "observation_age_seconds": (
            None
            if latest_date is None
            else Decimal(str((data.now - _day_end(latest_date)).total_seconds()))
        ),
        "reason": (
            "Successful source-ingestion watermark is unavailable."
            if age is None
            else "Time since successful source ingestion exceeds the configured cadence policy."
            if age > limit
            else "Source ingestion age is within the configured limit; observation age is separate."
        ),
    }
    grouped: dict[str, list[MonitorForecast]] = defaultdict(list)
    for forecast in forecasts:
        grouped[forecast.model_id].append(forecast)
    for model_id in data.expected_model_ids:
        grouped.setdefault(model_id, [])
    checks: dict[str, dict[str, Any]] = {f"{data.source_id}:source_freshness": freshness}
    for model_id, model_forecasts in sorted(grouped.items()):
        checks[f"{data.source_id}:forecast:{model_id}"] = _forecast_check(
            model_id, model_forecasts, data, config
        )
    if not grouped:
        checks[f"{data.source_id}:forecast:none"] = {
            "kind": "forecast_quality",
            "status": "insufficient_evidence",
            "reason": "No persisted forecast rows supplied; not a healthy verdict.",
            "distinct_mature_outcomes": 0,
            "minimum_distinct_mature_outcomes": config.min_mature_observations,
        }
    result = {
        "contract_version": "nexus.portfolio.measured-monitor.v1",
        "evaluated_at": data.now.isoformat(),
        "source_id": data.source_id,
        "input_sha256": input_hash,
        "evidence_sha256": data.evidence_sha256,
        "config_sha256": _sha(asdict(config)),
        "config": asdict(config),
        "checks": checks,
        "metric_definitions": {
            "bias_units": "Mean(q50 - actual); positive means over-forecasting, in units/day.",
            "coverage_80": "Mean(q10 <= actual <= q90), inclusive, per eligible forecast row.",
            "wis": "R3 WIS: median plus central 80%/95% interval proper scores, equal row weights.",
            "wape": "Sum(abs(q50 - actual)) / sum(actual); null if total actual is zero.",
            "evidence_minimum": (
                "Distinct SKU-location-target-date outcomes per model, not duplicated origins. "
                "Repeated origins receive equal metric weights but do not inflate this minimum."
            ),
        },
        "model_promotion": "never automatic; monitoring is evidence only",
        "delivery": "durable local journal; no external paging",
    }
    result["evaluation_sha256"] = _sha(result)
    return result


class MonitorBook:
    """Restart-safe measured evidence; only a healthy verdict resolves an alert.

    Insufficient or unknown evidence cannot silently clear an existing breach.
    Ticks are chronological, transactions serialize concurrent evaluators, and
    identical evaluations/unchanged states cannot duplicate notifications.
    """

    def __init__(self, path: Path, config: MonitorConfig | None = None) -> None:
        self.path = path
        self.config = config if config is not None else MonitorConfig()
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS monitor_evaluations (
                    id INTEGER PRIMARY KEY, evaluated_at TEXT NOT NULL,
                    evaluation_sha256 TEXT NOT NULL UNIQUE, payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS monitor_alerts (
                    id INTEGER PRIMARY KEY, check_id TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('firing', 'resolved')),
                    fired_at TEXT NOT NULL, resolved_at TEXT,
                    firing_evaluation_sha256 TEXT NOT NULL, resolving_evaluation_sha256 TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_monitor_alert
                    ON monitor_alerts(check_id) WHERE status='firing';
                CREATE TABLE IF NOT EXISTS monitor_notifications (
                    id INTEGER PRIMARY KEY, alert_id INTEGER NOT NULL,
                    transition TEXT NOT NULL CHECK(transition IN ('firing', 'resolved')),
                    delivered_at TEXT NOT NULL, evaluation_sha256 TEXT NOT NULL,
                    input_sha256 TEXT NOT NULL, evidence_sha256 TEXT NOT NULL,
                    UNIQUE(alert_id, transition)
                );
            """)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def evaluate(self, data: MonitorInput) -> dict[str, Any]:
        """Validate, measure and persist one snapshot; return a JSON-friendly readout."""
        if not isinstance(data, MonitorInput):
            raise ValueError("data must be a validated MonitorInput")
        result = _evaluate(data, self.config)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            latest = db.execute(
                "SELECT evaluated_at FROM monitor_evaluations ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if latest is not None and data.now < datetime.fromisoformat(latest["evaluated_at"]):
                raise ValueError("monitor evaluation time cannot move backwards")
            inserted = db.execute(
                "INSERT OR IGNORE INTO monitor_evaluations"
                "(evaluated_at,evaluation_sha256,payload) VALUES (?,?,?)",
                (result["evaluated_at"], result["evaluation_sha256"], _json(result)),
            )
            if inserted.rowcount == 0:
                return self._snapshot(db)
            for check_id, check in result["checks"].items():
                active = db.execute(
                    "SELECT id FROM monitor_alerts WHERE check_id=? AND status='firing'",
                    (check_id,),
                ).fetchone()
                transition = None
                alert_id = None
                if check["status"] == "firing" and active is None:
                    cursor = db.execute(
                        "INSERT INTO monitor_alerts"
                        "(check_id,status,fired_at,firing_evaluation_sha256) "
                        "VALUES (?,'firing',?,?)",
                        (check_id, result["evaluated_at"], result["evaluation_sha256"]),
                    )
                    alert_id, transition = cursor.lastrowid, "firing"
                elif check["status"] == "healthy" and active is not None:
                    alert_id, transition = active["id"], "resolved"
                    db.execute(
                        "UPDATE monitor_alerts SET status='resolved',resolved_at=?,"
                        "resolving_evaluation_sha256=? WHERE id=?",
                        (result["evaluated_at"], result["evaluation_sha256"], alert_id),
                    )
                if transition is not None:
                    db.execute(
                        "INSERT INTO monitor_notifications(alert_id,transition,delivered_at,"
                        "evaluation_sha256,input_sha256,evidence_sha256) VALUES (?,?,?,?,?,?)",
                        (
                            alert_id,
                            transition,
                            result["evaluated_at"],
                            result["evaluation_sha256"],
                            result["input_sha256"],
                            result["evidence_sha256"],
                        ),
                    )
            return self._snapshot(db)

    @staticmethod
    def _snapshot(db: sqlite3.Connection) -> dict[str, Any]:
        latest = db.execute(
            "SELECT payload,evaluation_sha256 FROM monitor_evaluations ORDER BY id DESC LIMIT 1"
        ).fetchone()
        result: dict[str, Any] = json.loads(latest["payload"]) if latest is not None else {}
        if latest is not None:
            recorded_hash = result.get("evaluation_sha256")
            unsigned_result = {
                key: value for key, value in result.items() if key != "evaluation_sha256"
            }
            if (
                recorded_hash != latest["evaluation_sha256"]
                or _sha(unsigned_result) != recorded_hash
            ):
                raise ValueError("persisted monitor evaluation checksum mismatch")
        statuses = {check["status"] for check in result.get("checks", {}).values()}
        active = db.execute(
            "SELECT * FROM monitor_alerts WHERE status='firing' ORDER BY id DESC"
        ).fetchall()
        result.update(
            {
                "status": (
                    "firing"
                    if active or "firing" in statuses
                    else "unknown"
                    if not statuses or "unknown" in statuses
                    else "insufficient_evidence"
                    if "insufficient_evidence" in statuses
                    else "healthy"
                ),
                "active_alerts": [dict(row) for row in active],
                "notifications": [
                    dict(row)
                    for row in db.execute(
                        "SELECT * FROM monitor_notifications ORDER BY id DESC LIMIT 50"
                    ).fetchall()
                ],
                "notifications_delivered": db.execute(
                    "SELECT count(*) FROM monitor_notifications"
                ).fetchone()[0],
                "evaluation_count": db.execute(
                    "SELECT count(*) FROM monitor_evaluations"
                ).fetchone()[0],
                "channel": "measured-local-journal",
            }
        )
        return result

    def snapshot(self) -> dict[str, Any]:
        """Read the latest persisted evidence without changing any monitor state."""
        with self._connect() as db:
            db.execute("BEGIN")
            return self._snapshot(db)
