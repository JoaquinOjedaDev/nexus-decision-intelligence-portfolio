"""Append-only local forecast/actual feedback, measured monitoring, optional proposal.

Forecast-source and later actual-source hashes are separate provenance, not an
equality join. Trusted local ingestion attests the actual values and correction
closure; hashes cannot establish external business truth. A forecast must have
been registered before the outcome was known, not merely carry an old issue date.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import asdict
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Self, cast

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.learning import LearningBook
from nexus.portfolio.monitoring import (
    MonitorActual,
    MonitorBook,
    MonitorConfig,
    MonitorForecast,
    MonitorInput,
)
from nexus.portfolio.monitoring import _evaluate as _evaluate_monitor
from nexus.portfolio.monitoring import _json as _monitor_json
from nexus.portfolio.service_backend import OperationalStore, RuntimeView, guarded_view
from nexus.portfolio.store import canonical, digest

ZERO = "0" * 64


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("feedback clock must be timezone-aware")
    return value.astimezone(UTC)


def _hash(value: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("feedback provenance requires a lowercase SHA-256 digest")
    return value


def _json(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str, allow_nan=False))


def _actor(actor: Actor, role: ActorRole, locations: set[str]) -> None:
    if (
        not isinstance(actor, Actor)
        or actor.role is not role
        or not actor.actor_id.strip()
        or not locations.issubset(actor.locations)
    ):
        raise PermissionError("feedback operation requires the correct role and location scope")


class ActualRevision(BaseModel):
    """Explicit source revision; later corrections may reopen previously closed data."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    event_id: str = Field(min_length=1, max_length=200)
    source_id: str = Field(min_length=1, max_length=200)
    source_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    sku_id: str = Field(min_length=1)
    location_id: str = Field(min_length=1)
    target_date: date
    revision: int = Field(ge=1)
    supersedes_event_id: str | None = None
    quantity: Decimal = Field(ge=0, allow_inf_nan=False)
    available_at: AwareDatetime
    correction_closed: bool
    correction_closed_at: AwareDatetime | None = None
    quantity_semantics: Literal["observed_daily_demand"] = "observed_daily_demand"

    @model_validator(mode="after")
    def validate_revision(self) -> Self:
        for text in (self.event_id, self.source_id, self.sku_id, self.location_id):
            if text != text.strip() or not text:
                raise ValueError("feedback identifiers cannot be blank or padded")
        if (self.revision == 1) != (self.supersedes_event_id is None):
            raise ValueError("revision one has no predecessor; later revisions must name it")
        end = datetime.combine(self.target_date + timedelta(days=1), time.min, UTC)
        if self.available_at < end:
            raise ValueError("actual availability cannot precede daily outcome completion")
        if self.correction_closed != (self.correction_closed_at is not None):
            raise ValueError("closure timestamp is required exactly when correction_closed is true")
        if self.correction_closed_at is not None and self.correction_closed_at < end:
            raise ValueError("correction closure cannot precede daily outcome completion")
        return self

    @property
    def key(self) -> tuple[str, str, date]:
        return self.sku_id, self.location_id, self.target_date


def _forecast(row: dict[str, Any]) -> MonitorForecast:
    return MonitorForecast(
        **{
            **row,
            "issue_time": datetime.fromisoformat(row["issue_time"]),
            "target_date": date.fromisoformat(row["target_date"]),
            **{name: Decimal(row[name]) for name in ("q025", "q10", "q50", "q90", "q975")},
        }
    )


class FeedbackBook:
    """Single-host SQLite journal; callers hold runtime-store lock before this lock."""

    def __init__(
        self, path: Path, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    ) -> None:
        self.path, self.clock = path, clock
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS feedback_events (
                    sequence INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
                    payload TEXT NOT NULL, event_sha256 TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS feedback_receipts (
                    evaluation_sha256 TEXT PRIMARY KEY, payload TEXT NOT NULL,
                    receipt_sha256 TEXT NOT NULL);
            """)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _read(db: sqlite3.Connection) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        previous = ZERO
        for index, row in enumerate(
            db.execute("SELECT * FROM feedback_events ORDER BY sequence"), 1
        ):
            event = json.loads(row["payload"])
            if (
                row["sequence"] != index
                or event["sequence"] != index
                or event["event_id"] != row["event_id"]
                or event["previous_sha256"] != previous
                or digest(event) != row["event_sha256"]
                or event["kind"] not in {"forecast_generation", "actual_revision"}
            ):
                raise ValueError("feedback journal checksum or sequence mismatch")
            if events and event["recorded_at"] < events[-1]["recorded_at"]:
                raise ValueError("feedback journal clock moved backwards")
            previous = row["event_sha256"]
            events.append({**event, "event_sha256": previous})
        return events

    def _append(
        self,
        db: sqlite3.Connection,
        events: list[dict[str, Any]],
        *,
        event_id: str,
        kind: str,
        data: dict[str, Any],
        actor: Actor,
    ) -> dict[str, Any]:
        previous = next((row for row in events if row["event_id"] == event_id), None)
        if previous is not None:
            if previous["data"] != data or previous["kind"] != kind:
                raise ValueError("feedback event ID conflicts with immutable content")
            return previous
        now = _utc(self.clock()).isoformat()
        if events and now < events[-1]["recorded_at"]:
            raise ValueError("feedback clock cannot move backwards")
        event = {
            "sequence": len(events) + 1,
            "event_id": event_id,
            "kind": kind,
            "recorded_at": now,
            "actor_id": actor.actor_id,
            "actor_role": actor.role.value,
            "previous_sha256": events[-1]["event_sha256"] if events else ZERO,
            "data": data,
        }
        identity = digest(event)
        db.execute(
            "INSERT INTO feedback_events VALUES (?,?,?,?)",
            (
                event["sequence"],
                event_id,
                canonical(event),
                identity,
            ),
        )
        return {**event, "event_sha256": identity}

    def register_current_runtime(
        self,
        store: OperationalStore,
        *,
        actor: Actor,
        expected_seed_sha256: str,
        view: RuntimeView | None = None,
    ) -> dict[str, Any]:
        """Capture the actual active, checksum-verified runtime seed under its writer lock."""
        _hash(expected_seed_sha256)
        context = nullcontext(view) if view is not None else guarded_view(store)
        with context as current:
            seed = current.seed
            if digest(seed) != current.seed_sha256 or current.seed_sha256 != expected_seed_sha256:
                raise ValueError("feedback current runtime seed binding mismatch")
            if seed["metadata"]["pipeline_mode"] != "connected_runtime_baseline":
                raise ValueError("feedback requires a connected runtime forecast generation")
            _actor(actor, ActorRole.SYSTEM, set(current.retained_locations))
            rows = [
                asdict(
                    MonitorForecast(
                        sku_id=series["sku_id"],
                        location_id=series["location_id"],
                        issue_time=datetime.fromisoformat(series["origin"]),
                        target_date=date.fromisoformat(point["date"]),
                        **{
                            name: Decimal(str(point[name]))
                            for name in ("q025", "q10", "q50", "q90", "q975")
                        },
                        model_id=series["model"],
                        artifact_sha256=series["forecast_sha256"],
                    )
                )
                for series in seed["forecasts"]
                for point in series["points"]
            ]
            if not rows or len(
                {
                    tuple(
                        str(row[k])
                        for k in ("sku_id", "location_id", "issue_time", "target_date", "model_id")
                    )
                    for row in rows
                }
            ) != len(rows):
                raise ValueError("runtime forecast generation is empty or has duplicate keys")
            _actor(actor, ActorRole.SYSTEM, {row["location_id"] for row in rows})
            data = _json(
                {
                    "seed_sha256": expected_seed_sha256,
                    "source_snapshot_sha256": _hash(seed["metadata"]["source_snapshot_sha256"]),
                    "runtime_ingested_at": current.runtime_ingested_at,
                    "forecasts": rows,
                }
            )
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                return self._append(
                    db,
                    self._read(db),
                    event_id="forecast:" + expected_seed_sha256,
                    kind="forecast_generation",
                    data=data,
                    actor=actor,
                )

    def append_actual(self, actual: ActualRevision, *, actor: Actor) -> dict[str, Any]:
        """Persist the next exact revision; retries are idempotent, conflicts never overwrite."""
        actual = ActualRevision.model_validate_json(actual.model_dump_json())
        _actor(actor, ActorRole.SYSTEM, {actual.location_id})
        if actual.correction_closed_at is not None and actual.correction_closed_at > _utc(
            self.clock()
        ):
            raise ValueError("future correction closure cannot be attested as an observed fact")
        data = actual.model_dump(mode="json")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            events = self._read(db)
            if any(row["event_id"] == "actual:" + actual.event_id for row in events):
                return self._append(
                    db,
                    events,
                    event_id="actual:" + actual.event_id,
                    kind="actual_revision",
                    data=data,
                    actor=actor,
                )
            known_keys = {
                (f["sku_id"], f["location_id"])
                for row in events
                if row["kind"] == "forecast_generation"
                for f in row["data"]["forecasts"]
            }
            if (actual.sku_id, actual.location_id) not in known_keys:
                raise ValueError("actual SKU/location has no retained runtime generation")
            versions = [
                ActualRevision.model_validate_json(canonical(row["data"]))
                for row in events
                if row["kind"] == "actual_revision"
            ]
            previous = next((row for row in reversed(versions) if row.key == actual.key), None)
            if previous is None:
                if actual.revision != 1:
                    raise ValueError("actual revision chain must start at one")
            elif (
                actual.revision != previous.revision + 1
                or actual.supersedes_event_id != previous.event_id
                or actual.source_id != previous.source_id
                or actual.available_at < previous.available_at
            ):
                raise ValueError("actual revision must extend its source chain without backdating")
            return self._append(
                db,
                events,
                event_id="actual:" + actual.event_id,
                kind="actual_revision",
                data=data,
                actor=actor,
            )

    def _selection(
        self, events: list[dict[str, Any]], now: datetime
    ) -> tuple[MonitorInput, dict[str, Any]]:
        visible = [row for row in events if datetime.fromisoformat(row["recorded_at"]) <= now]
        generations = [row for row in visible if row["kind"] == "forecast_generation"]
        versions: dict[tuple[str, str, date], list[tuple[ActualRevision, dict[str, Any]]]] = {}
        for event in visible:
            if event["kind"] == "actual_revision":
                actual = ActualRevision.model_validate_json(canonical(event["data"]))
                versions.setdefault(actual.key, []).append((actual, event))
        selected, first_known, selected_ids = {}, {}, []
        for key, rows in versions.items():
            arrived = [(row, event) for row, event in rows if row.available_at <= now]
            if not arrived:
                continue
            actual, event = arrived[-1]
            first_known[key] = min(
                min(row.available_at, datetime.fromisoformat(entry["recorded_at"]))
                for row, entry in rows
            )
            selected[key] = MonitorActual(
                sku_id=actual.sku_id,
                location_id=actual.location_id,
                target_date=actual.target_date,
                quantity=actual.quantity,
                available_at=actual.available_at,
                mature_after=actual.correction_closed_at or actual.available_at,
                correction_closed=actual.correction_closed,
            )
            selected_ids.append(
                {
                    "event_id": actual.event_id,
                    "revision": actual.revision,
                    "source_snapshot_sha256": actual.source_snapshot_sha256,
                }
            )
        candidates: dict[
            tuple[str, str, str, date],
            list[tuple[datetime, datetime, str, MonitorForecast, dict[str, Any]]],
        ] = {}
        observations: list[dict[str, Any]] = []
        for generation in generations:
            registered = datetime.fromisoformat(generation["recorded_at"])
            for index, payload in enumerate(generation["data"]["forecasts"]):
                forecast = _forecast(payload)
                outcome = selected.get(forecast.outcome_key)
                reason = "eligible"
                if forecast.issue_time > now:
                    reason = "forecast_not_yet_issued"
                elif outcome is None:
                    reason = (
                        "actual_not_yet_available"
                        if forecast.outcome_key in versions
                        else "missing_actual"
                    )
                elif not outcome.correction_closed:
                    reason = "correction_window_open"
                elif outcome.mature_after > now:
                    reason = "maturity_deadline_not_reached"
                elif forecast.issue_time >= first_known[forecast.outcome_key]:
                    reason = "forecast_issued_after_outcome_known"
                elif registered >= first_known[forecast.outcome_key]:
                    reason = "forecast_registered_after_outcome_known"
                observation = {
                    "generation_sha256": generation["data"]["seed_sha256"],
                    "row_index": index,
                    "reason": reason,
                    "key": _json(forecast.key),
                }
                observations.append(observation)
                if reason == "eligible":
                    candidates.setdefault((forecast.model_id, *forecast.outcome_key), []).append(
                        (
                            forecast.issue_time,
                            registered,
                            generation["event_id"],
                            forecast,
                            observation,
                        )
                    )
        chosen = []
        for options in candidates.values():
            ordered = sorted(options, key=lambda row: row[:3], reverse=True)
            chosen.append(ordered[0][3])
            for row in ordered[1:]:
                row[4]["reason"] = "superseded_by_selected_forecast"
        # Explicit expected models retain zero-evidence checks without reintroducing
        # ineligible forecasts into the global actual join or fabricating rows.
        expected_models = tuple(
            sorted(
                {
                    row["model_id"]
                    for generation in generations
                    for row in generation["data"]["forecasts"]
                }
            )
        )
        identity = digest(
            {
                "events": [row["event_sha256"] for row in visible],
                "selection": observations,
                "selected_actuals": selected_ids,
            }
        )
        watermarks = [
            datetime.fromisoformat(row["data"].get("runtime_ingested_at") or row["recorded_at"])
            for row in visible
        ]
        watermarks = [stamp for stamp in watermarks if stamp <= now]
        data = MonitorInput(
            now=now,
            source_watermark=max(watermarks) if watermarks else None,
            forecasts=tuple(chosen),
            actuals=tuple(selected.values()),
            source_id="local_feedback_ingestion",
            evidence_sha256=identity,
            expected_model_ids=expected_models,
        )
        return data, {
            "locations": sorted(
                {
                    point["location_id"]
                    for event in events
                    if event["kind"] == "forecast_generation"
                    for point in event["data"]["forecasts"]
                }
            ),
            "ledger_head_sha256": visible[-1]["event_sha256"] if visible else ZERO,
            "source_snapshot_sha256": identity,
            "forecast_source_sha256s": sorted(
                {row["data"]["source_snapshot_sha256"] for row in generations}
            ),
            "actual_source_sha256s": sorted(
                {
                    row["data"]["source_snapshot_sha256"]
                    for row in visible
                    if row["kind"] == "actual_revision"
                }
            ),
            "counts": {
                "forecast_generations": len(generations),
                "actual_versions": sum(map(len, versions.values())),
                "forecast_rows": len(observations),
                "eligible_forecasts": len(chosen),
            },
            "selection": {
                "selected_actuals": selected_ids,
                "observations": observations,
                "exclusion_counts": dict(
                    Counter(row["reason"] for row in observations if row["reason"] != "eligible")
                ),
            },
        }

    def evaluate(
        self,
        monitor_book: MonitorBook,
        *,
        learning_book: LearningBook | None = None,
        proposer: Actor | None = None,
        reader: Actor | None = None,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        """Monitor by default; explicit planner proposals require measured quality breach."""
        now, clock = _utc(as_of or self.clock()), _utc(self.clock())
        if now > clock:
            raise ValueError("feedback evaluation cannot see a future clock")
        if monitor_book.config.min_mature_observations < 500:
            raise ValueError("feedback monitoring cannot lower the 500-outcome evidence minimum")
        if (learning_book is None) != (proposer is None):
            raise ValueError("a training proposal requires both explicit planner and learning book")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            events = self._read(db)
            data, selection = self._selection(events, now)
            if reader is not None:
                if not isinstance(reader, Actor) or reader.role not in {
                    ActorRole.PLANNER,
                    ActorRole.APPROVER,
                    ActorRole.AUDITOR,
                }:
                    raise PermissionError("feedback reader requires an authorized human role")
                _actor(reader, reader.role, set(selection["locations"]))
            if proposer is not None:
                locations = {
                    row["location_id"]
                    for event in events
                    if event["kind"] == "forecast_generation"
                    for row in event["data"]["forecasts"]
                }
                _actor(proposer, ActorRole.PLANNER, locations)
            monitored = monitor_book.evaluate(data)
            breaches = [
                row
                for row in monitored["checks"].values()
                if row["kind"] == "forecast_quality"
                and row["status"] == "firing"
                and row["distinct_mature_outcomes"] >= 500
                and row["breaches"]
            ]
            proposal = None
            if breaches and proposer is not None and learning_book is not None:
                proposal = learning_book.propose_training(
                    proposer,
                    source_snapshot_sha256=data.evidence_sha256,
                    monitor_evaluation_sha256=monitored["evaluation_sha256"],
                    reason="Measured finalized feedback breach: "
                    + "; ".join(
                        f"{row['model_id']}: {','.join(row['breaches'])}" for row in breaches
                    ),
                )
            result = {
                "contract_version": "nexus.feedback-evaluation.v1",
                **selection,
                **selection["counts"],
                "evaluation_sha256": monitored["evaluation_sha256"],
                "monitor": monitored,
                "proposal_id": proposal,
                "proposal_reason": "Measured forecast-quality breach"
                if breaches
                else "No eligible measured forecast-quality breach",
                "scope": (
                    "Local feedback ingestion and selected finalized observations; "
                    "not an external freshness attestation"
                ),
                "training_performed": False,
                "scientific_promotion_applied": False,
            }
            receipt_id = digest(
                {"evaluation": result["evaluation_sha256"], "proposal_id": proposal}
            )
            result["receipt_id"] = receipt_id
            encoded = canonical(result)
            existing = db.execute(
                "SELECT payload FROM feedback_receipts WHERE evaluation_sha256=?", (receipt_id,)
            ).fetchone()
            if existing is not None and existing["payload"] != encoded:
                raise ValueError(
                    "feedback receipt identity conflicts with existing immutable evidence"
                )
            db.execute(
                "INSERT OR IGNORE INTO feedback_receipts VALUES (?,?,?)",
                (receipt_id, encoded, digest(result)),
            )
            return result

    def receipt(self, receipt_id: str) -> dict[str, Any]:
        with self._connect() as db:
            self._read(db)
            row = db.execute(
                "SELECT * FROM feedback_receipts WHERE evaluation_sha256=?", (_hash(receipt_id),)
            ).fetchone()
            if row is None:
                raise ValueError("unknown feedback receipt")
            value = json.loads(row["payload"])
            if digest(value) != row["receipt_sha256"]:
                raise ValueError("feedback receipt checksum mismatch")
            return cast(dict[str, Any], value)

    def export_training_evidence(
        self,
        receipt_id: str,
        *,
        actor: Actor,
        expected_head_sha256: str | None = None,
    ) -> dict[str, Any]:
        """Export verified trigger observations, not a training-ready snapshot.

        The trusted training worker needs every retained location. Recompute the
        receipt's selection and measured breach within one read transaction; no
        monitor, proposal or journal is written. Any newer journal event requires
        an explicitly reevaluated trigger, even when the saved receipt is valid
        historical evidence. A later source/Gold bridge must establish training
        label semantics and point-in-time finality independently.
        """
        _actor(actor, ActorRole.SYSTEM, set())
        _hash(receipt_id)
        if expected_head_sha256 is not None:
            _hash(expected_head_sha256)
        with self._connect() as db:
            db.execute("BEGIN")
            events = self._read(db)
            locations = {
                point["location_id"]
                for event in events
                if event["kind"] == "forecast_generation"
                for point in event["data"]["forecasts"]
            }
            _actor(actor, ActorRole.SYSTEM, locations)
            row = db.execute(
                "SELECT payload,receipt_sha256 FROM feedback_receipts WHERE evaluation_sha256=?",
                (receipt_id,),
            ).fetchone()
            if row is None:
                raise ValueError("unknown feedback receipt")
            receipt = json.loads(row["payload"])
            if digest(receipt) != row["receipt_sha256"]:
                raise ValueError("feedback receipt checksum mismatch")
            head = events[-1]["event_sha256"] if events else ZERO
            if head != receipt["ledger_head_sha256"] or (
                expected_head_sha256 is not None and head != expected_head_sha256
            ):
                raise ValueError("feedback trigger is stale; reevaluate the current journal")
            if receipt.get("proposal_id") is None:
                raise ValueError("training export requires an explicit retained proposal")
            if receipt_id != receipt.get("receipt_id") or receipt_id != digest(
                {"evaluation": receipt["evaluation_sha256"], "proposal_id": receipt["proposal_id"]}
            ):
                raise ValueError("feedback receipt identity mismatch")
            evaluated_at = _utc(datetime.fromisoformat(receipt["monitor"]["evaluated_at"]))
            if evaluated_at > _utc(self.clock()):
                raise ValueError("feedback trigger evaluation is in the future")
            data, selection = self._selection(events, evaluated_at)
            if any(
                receipt.get(key) != value
                for key, value in {**selection, **selection["counts"]}.items()
            ):
                raise ValueError("feedback receipt selection differs from retained events")
            raw_config = receipt["monitor"]["config"]
            config_fields: dict[str, Any] = {
                key: value
                if key in {"min_mature_observations", "expected_source_cadence_seconds"}
                or value is None
                else Decimal(str(value))
                for key, value in raw_config.items()
            }
            config = MonitorConfig(**config_fields)
            if config.min_mature_observations < 500 or config.coverage_tolerance > Decimal("0.05"):
                raise ValueError("training trigger cannot weaken mature-outcome/coverage gates")
            measured = json.loads(_monitor_json(_evaluate_monitor(data, config)))
            if any(receipt["monitor"].get(key) != value for key, value in measured.items()):
                raise ValueError("feedback measurements differ from recomputed evidence")
            if receipt["evaluation_sha256"] != measured["evaluation_sha256"] or not any(
                check["kind"] == "forecast_quality"
                and check["status"] == "firing"
                and check["distinct_mature_outcomes"] >= 500
                and check["breaches"]
                for check in measured["checks"].values()
            ):
                raise ValueError("training trigger requires a measured 500-outcome quality breach")
            outcome_keys = {point.outcome_key for point in data.forecasts}
            selected_ids = {item["event_id"] for item in selection["selection"]["selected_actuals"]}
            actuals = []
            for event in events:
                if event["kind"] != "actual_revision":
                    continue
                revision = ActualRevision.model_validate_json(canonical(event["data"]))
                if revision.event_id in selected_ids and revision.key in outcome_keys:
                    actuals.append(
                        {
                            "revision": revision.model_dump(mode="json"),
                            "event_sha256": event["event_sha256"],
                            "recorded_at": event["recorded_at"],
                        }
                    )
            actuals.sort(
                key=lambda item: (
                    item["revision"]["sku_id"],
                    item["revision"]["location_id"],
                    item["revision"]["target_date"],
                )
            )
            exported = {
                "contract_version": "nexus.feedback-training-export.v1",
                "receipt": receipt,
                "actuals": actuals,
                "forecast_outcome_keys": _json(sorted(outcome_keys)),
                "ledger_head_sha256": head,
                "locations": sorted(locations),
                "training_snapshot_verified": False,
            }
            return {**exported, "export_sha256": digest(exported)}

    def snapshot(self) -> dict[str, Any]:
        with self._connect() as db:
            db.execute("BEGIN")
            events = self._read(db)
            return {
                "event_count": len(events),
                "locations": sorted(
                    {
                        point["location_id"]
                        for event in events
                        if event["kind"] == "forecast_generation"
                        for point in event["data"]["forecasts"]
                    }
                ),
                "counts": dict(Counter(row["kind"] for row in events)),
                "ledger_head_sha256": events[-1]["event_sha256"] if events else ZERO,
                "receipt_ids": [
                    row[0]
                    for row in db.execute(
                        "SELECT evaluation_sha256 FROM feedback_receipts ORDER BY rowid"
                    )
                ],
            }
