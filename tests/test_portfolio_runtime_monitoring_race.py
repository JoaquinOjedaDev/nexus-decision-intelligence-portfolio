"""A delayed runtime monitor cannot publish an old generation after a newer one."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio import runtime_monitoring as bridge
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.monitoring import MonitorBook, MonitorForecast
from nexus.portfolio.runtime_control import RuntimeControl
from nexus.portfolio.store import PortfolioStore, StoreError, iso

NOW = datetime(2026, 9, 12, 12, tzinfo=UTC).timestamp()
PLANNER = resolve_persona("planner", ("DC-ARG",))
WORKER = Actor("runtime-worker", "Runtime worker", ActorRole.SYSTEM, ("DC-ARG",))


class Clock:
    def __init__(self) -> None:
        self.value = NOW
        self.lock = threading.Lock()

    def __call__(self) -> float:
        with self.lock:
            return self.value

    def advance(self) -> None:
        with self.lock:
            self.value += 1


def seed(source: str) -> dict[str, Any]:
    return {
        "metadata": {
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": source * 64,
            "source_watermark": "2026-02-25T04:00:00+00:00",
        },
        "executable": True,
        "recommendations": [
            {
                "id": "rec-1",
                "sku_id": "SKU-001",
                "location_id": "DC-ARG",
                "recommended_quantity": 20,
                "expected_cost": 100,
                "created_by": "planner.r4a",
            }
        ],
        "forecasts": [
            {
                "sku_id": "SKU-001",
                "location_id": "DC-ARG",
                "origin": "2026-02-25T04:00:00+00:00",
                "model": "seasonal_naive_empirical_intervals",
                "forecast_sha256": source * 64,
                "points": [
                    {
                        "date": "2026-02-26",
                        "q025": 0,
                        "q10": 1,
                        "q50": 2,
                        "q90": 3,
                        "q975": 4,
                    }
                ],
            }
        ],
    }


def context(tmp_path: Path) -> tuple[PortfolioStore, RuntimeControl, Clock, dict[str, Any]]:
    clock = Clock()
    old_reader = PortfolioStore(tmp_path, clock=clock)
    old_reader.ensure_seed(seed("a"))
    # Half a second within the healthy boundary: advancing the evaluation time
    # after an old snapshot would manufacture a freshness alert.
    with old_reader.transaction() as db:
        db.execute("INSERT INTO metadata VALUES ('runtime_ingested_at',?)", (iso(NOW - 172_799.5),))
    # A separate PortfolioStore uses separate SQLite connections, as another
    # process would. The lock must be in the DB, not merely a Python thread lock.
    writer = PortfolioStore(tmp_path, clock=clock)
    control = RuntimeControl(writer)
    control.submit(
        EventEnvelope(
            event_id=uuid4(),
            event_type="inventory.adjusted",
            source="local-test-fixture",
            subject_id="portfolio",
            occurred_at=datetime.fromtimestamp(clock(), UTC),
            ingested_at=datetime.fromtimestamp(clock(), UTC),
            idempotency_key=uuid4().hex,
            payload={"source_snapshot_sha256": "b" * 64},
        ),
        PLANNER,
    )
    job = control.claim(WORKER)
    assert job is not None
    return old_reader, control, clock, job


def test_old_snapshot_holds_activation_until_monitor_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old_reader, control, clock, job = context(tmp_path)
    snapshot_captured = threading.Barrier(2)
    release_old_snapshot = threading.Event()
    activation_attempted = threading.Event()
    activation_completed = threading.Event()
    original_connection = control.store.connection

    @contextmanager
    def observe_writer_lock() -> Iterator[sqlite3.Connection]:
        with original_connection() as db:
            db.set_trace_callback(
                lambda sql: activation_attempted.set() if sql == "BEGIN IMMEDIATE" else None
            )
            yield db

    def paused_forecast(**values: Any) -> MonitorForecast:
        if values["artifact_sha256"] == "a" * 64:
            snapshot_captured.wait(timeout=3)
            assert release_old_snapshot.wait(timeout=3), "old snapshot was not released"
        return MonitorForecast(**values)

    def activate_and_monitor() -> dict[str, Any]:
        control.activate(job["id"], job["token"], seed("b"), WORKER)
        activation_completed.set()
        return bridge.monitor_runtime(control.store)

    monkeypatch.setattr(bridge, "MonitorForecast", paused_forecast)
    monkeypatch.setattr(control.store, "connection", observe_writer_lock)
    with ThreadPoolExecutor(max_workers=2) as workers:
        old = workers.submit(bridge.monitor_runtime, old_reader)
        snapshot_captured.wait(timeout=3)
        clock.advance()
        new = workers.submit(activate_and_monitor)
        try:
            assert activation_attempted.wait(timeout=3), "activation never attempted its DB lock"
            stayed_serialized = not activation_completed.wait(timeout=0.2)
        finally:
            release_old_snapshot.set()
        old_result = old.result(timeout=3)
        new_result = new.result(timeout=3)
    assert stayed_serialized, "activation overtook an uncommitted old-generation monitor snapshot"
    assert old_result["evaluated_at"] == iso(NOW)
    assert old_result["evidence_sha256"] == "a" * 64
    assert new_result["evaluated_at"] == iso(NOW + 1)
    assert new_result["evidence_sha256"] == "b" * 64
    journal = MonitorBook(tmp_path / "monitoring.sqlite3").snapshot()
    assert journal["evidence_sha256"] == "b" * 64
    assert journal["evaluated_at"] == iso(NOW + 1)
    assert journal["evaluation_count"] == 2
    assert journal["notifications_delivered"] == 0
    assert journal["active_alerts"] == []
    assert journal["status"] == "insufficient_evidence"
    assert journal["config"]["min_mature_observations"] == 500
    assert new_result["scope"] == "Local pipeline ingestion, not an external live feed"
    assert new_result["historical_source_watermark"] == "2026-02-25T04:00:00+00:00"
    assert control.snapshot()["archived_generations"] == 1


def test_repeated_concurrent_ticks_serialize_and_keep_latest_generation(tmp_path: Path) -> None:
    first, control, clock, job = context(tmp_path)

    def tick_wave() -> list[dict[str, Any]]:
        ready = threading.Barrier(8)

        def tick(index: int) -> dict[str, Any]:
            ready.wait(timeout=3)
            return bridge.monitor_runtime(first if index % 2 else control.store)

        with ThreadPoolExecutor(max_workers=8) as workers:
            return list(workers.map(tick, range(8)))

    initial = tick_wave()
    assert all(result["evidence_sha256"] == "a" * 64 for result in initial)
    assert all(result["evaluation_count"] == 1 for result in initial)
    clock.advance()
    control.activate(job["id"], job["token"], seed("b"), WORKER)
    updated = tick_wave()
    assert all(result["evidence_sha256"] == "b" * 64 for result in updated)
    assert all(result["evaluation_count"] == 2 for result in updated)
    persisted = MonitorBook(tmp_path / "monitoring.sqlite3").snapshot()
    assert persisted["evidence_sha256"] == "b" * 64
    assert persisted["notifications_delivered"] == 0


def test_failed_monitor_evaluation_releases_workflow_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, control, clock, job = context(tmp_path)

    def invalid_evaluation(*args: Any, **kwargs: Any) -> None:
        raise ValueError("injected invalid monitor evidence")

    with monkeypatch.context() as scoped:
        scoped.setattr(MonitorBook, "evaluate", invalid_evaluation)
        with pytest.raises(ValueError, match="injected invalid"):
            bridge.monitor_runtime(first)
    clock.advance()
    control.activate(job["id"], job["token"], seed("b"), WORKER)
    assert bridge.monitor_runtime(first)["evidence_sha256"] == "b" * 64


def test_uninitialized_monitor_fails_without_holding_workflow_lock(tmp_path: Path) -> None:
    store = PortfolioStore(tmp_path, clock=lambda: NOW)
    with pytest.raises(StoreError) as error:
        bridge.monitor_runtime(store)
    assert (error.value.code, error.value.status) == ("not_ready", 503)
    store.ensure_seed(seed("a"))
    assert bridge.monitor_runtime(store)["status"] == "unknown"
