"""Actual journal integration; no fabricated closure, promotion, or demo mutation."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.feedback import ActualRevision
from nexus.portfolio.runtime_feedback import (
    feedback_book,
    ingest_runtime_actual,
    monitor_runtime_feedback,
    retain_runtime_forecasts,
    runtime_feedback_summary,
)
from nexus.portfolio.store import PortfolioStore, StoreError, digest

ISSUE = datetime(2026, 1, 2, tzinfo=UTC)


def seed() -> dict[str, Any]:
    value: dict[str, Any] = {
        "metadata": {
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": "a" * 64,
        },
        "executable": True,
        "recommendations": [
            {
                "id": "rec-1",
                "sku_id": "SKU-001",
                "location_id": "DC-ARG",
                "recommended_quantity": 20,
                "expected_cost": 100,
            }
        ],
        "metrics": {},
        "forecasts": [
            {
                "sku_id": "SKU-001",
                "location_id": "DC-ARG",
                "model": "baseline",
                "forecast_sha256": "e" * 64,
                "origin": ISSUE.isoformat(),
                "points": [
                    {
                        "date": ISSUE.date().isoformat(),
                        "q025": 0,
                        "q10": 5,
                        "q50": 10,
                        "q90": 15,
                        "q975": 20,
                    }
                ],
            }
        ],
    }
    value["seed_hash"] = digest(value)
    return value


def make_store(tmp_path: Path) -> tuple[PortfolioStore, list[float]]:
    clock = [ISSUE.timestamp()]
    store = PortfolioStore(tmp_path / "state", clock=lambda: clock[0])
    store.ensure_seed(seed())
    return store, clock


def actual() -> ActualRevision:
    return ActualRevision.model_validate_json(
        json.dumps(
            {
                "event_id": "actual-1",
                "source_id": "trusted-local-fixture",
                "source_snapshot_sha256": "b" * 64,
                "sku_id": "SKU-001",
                "location_id": "DC-ARG",
                "target_date": "2026-01-02",
                "revision": 1,
                "supersedes_event_id": None,
                "quantity": "11",
                "available_at": "2026-01-03T03:00:00+00:00",
                "correction_closed": True,
                "correction_closed_at": "2026-01-06T00:00:00+00:00",
            }
        )
    )


def actor(role: ActorRole = ActorRole.SYSTEM, location: str = "DC-ARG") -> Actor:
    return Actor("test-publisher", "Test publisher", role, (location,))


def test_registration_and_actuals_survive_restart_without_changing_business_state(
    tmp_path: Path,
) -> None:
    store, clock = make_store(tmp_path)
    before = store.metrics_snapshot()
    retain_runtime_forecasts(store)
    clock[0] = (ISSUE + timedelta(days=5)).timestamp()
    ingest_runtime_actual(store, actual(), actor=actor())
    first = monitor_runtime_feedback(store)
    reopened = PortfolioStore(store.path.parent, clock=lambda: clock[0])
    second = monitor_runtime_feedback(reopened)
    assert first["evaluation_sha256"] == second["evaluation_sha256"]
    assert second["forecast_generations"] == 1
    assert second["actual_versions"] == 1
    assert second["proposal_id"] is None and not second["training_performed"]
    after = reopened.metrics_snapshot()
    assert {k: v for k, v in after.items() if k != "checked_at"} == {
        k: v for k, v in before.items() if k != "checked_at"
    }
    assert after["checked_at"] == datetime.fromtimestamp(clock[0], UTC).isoformat()
    report = runtime_feedback_summary(reopened, actor(ActorRole.AUDITOR))
    assert report["quality_status"] == "insufficient_evidence"
    assert report["posthoc_forecasts_excluded"] and not report["external_live_feed"]
    assert "actual-1" not in json.dumps(report)


@pytest.mark.parametrize("principal", [actor(ActorRole.PLANNER), actor(location="OTHER")])
def test_invalid_publisher_denied_before_creating_journal(tmp_path: Path, principal: Actor) -> None:
    store, _ = make_store(tmp_path)
    with pytest.raises(PermissionError):
        ingest_runtime_actual(store, actual(), actor=principal)
    assert not (store.path.parent / "feedback.sqlite3").exists()


def test_scoped_reader_denied(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path)
    retain_runtime_forecasts(store)
    with pytest.raises(StoreError, match="all retained locations"):
        runtime_feedback_summary(store, actor(ActorRole.AUDITOR, "OTHER"))


def test_simulated_order_outcome_is_not_a_forecast_actual(tmp_path: Path) -> None:
    store, _ = make_store(tmp_path)
    report = runtime_feedback_summary(store, actor(ActorRole.AUDITOR))
    assert report["actual_versions"] == 0
    assert report["quality_status"] == "insufficient_evidence"
    assert feedback_book(store).snapshot()["locations"] == ["DC-ARG"]


def test_real_500_outcome_breach_reopened_sticky_then_healthy_http_recovery(tmp_path: Path) -> None:
    """Measured controlled actuals, not a mocked monitor verdict or relaxed minimum."""
    from nexus.portfolio.monitoring import MonitorBook
    from nexus.portfolio.runtime_control import RuntimeControl

    clock = [ISSUE.timestamp()]
    store = PortfolioStore(tmp_path / "mature-runtime", clock=lambda: clock[0])
    sample = seed()
    forecast = sample["forecasts"][0]
    sample["forecasts"] = [
        {
            **forecast,
            "sku_id": f"SKU-{index:04d}",
            "points": [
                {**forecast["points"][0], "q025": 8, "q10": 9, "q50": 10, "q90": 11, "q975": 12}
            ],
        }
        for index in range(500)
    ]
    sample["metadata"]["source_watermark"] = ISSUE.isoformat()
    store.ensure_seed(sample)
    with store.transaction() as db:
        db.execute("INSERT INTO metadata VALUES ('runtime_ingested_at',?)", (ISSUE.isoformat(),))
    RuntimeControl(store)
    retain_runtime_forecasts(store)
    clock[0] = (ISSUE + timedelta(days=1, hours=1)).timestamp()
    book = feedback_book(store)
    revisions = []
    for index in range(500):
        payload = actual().model_dump(mode="json")
        payload.update(
            event_id=f"measured-{index}",
            sku_id=f"SKU-{index:04d}",
            quantity="10" if index < 374 else "12",
            available_at=(ISSUE + timedelta(days=1)).isoformat(),
            correction_closed_at=datetime.fromtimestamp(clock[0], UTC).isoformat(),
        )
        revision = ActualRevision.model_validate_json(json.dumps(payload))
        revisions.append(revision)
        book.append_actual(revision, actor=actor())
    initial = runtime_feedback_summary(store, actor(ActorRole.AUDITOR))
    assert initial["quality_status"] == "firing" and initial["active_forecast_alerts"] == 1
    clock[0] += 3600
    reopened = revisions[374].model_copy(
        update={
            "event_id": "reopen-374",
            "revision": 2,
            "supersedes_event_id": "measured-374",
            "available_at": datetime.fromtimestamp(clock[0], UTC),
            "correction_closed": False,
            "correction_closed_at": None,
        }
    )
    book.append_actual(reopened, actor=actor())
    insufficient = runtime_feedback_summary(store, actor(ActorRole.AUDITOR))
    assert insufficient["current_quality_status"] == "insufficient_evidence"
    assert insufficient["quality_status"] == "firing"
    assert insufficient["active_forecast_alerts"] == 1
    clock[0] += 3600
    closed_payload = reopened.model_dump(mode="json")
    closed_payload.update(
        event_id="reclose-374",
        revision=3,
        supersedes_event_id="reopen-374",
        quantity="10",
        available_at=datetime.fromtimestamp(clock[0], UTC).isoformat(),
        correction_closed=True,
        correction_closed_at=datetime.fromtimestamp(clock[0], UTC).isoformat(),
    )
    book.append_actual(
        ActualRevision.model_validate_json(json.dumps(closed_payload)), actor=actor()
    )
    healthy = runtime_feedback_summary(store, actor(ActorRole.AUDITOR))
    assert healthy["quality_status"] == healthy["current_quality_status"] == "healthy"
    assert healthy["active_forecast_alerts"] == 0
    measured = MonitorBook(store.path.parent / "feedback-monitoring.sqlite3").snapshot()
    quality = [row for row in measured["checks"].values() if row["kind"] == "forecast_quality"]
    assert quality[0]["distinct_mature_outcomes"] == 500
    assert quality[0]["metrics"]["coverage_80"] == "0.75"
    assert [row["transition"] for row in measured["notifications"]] == ["resolved", "firing"]
    testclient = pytest.importorskip("fastapi.testclient")
    pytest.importorskip("jwt")
    from nexus.portfolio.api import create_app

    app = create_app(store.path.parent, pipeline="runtime")
    app.state.portfolio_store = store
    with closing(testclient.TestClient(app)) as client:
        client.post("/api/session", json={"persona": "auditor"})
        response = client.get("/api/health")
        assert response.status_code == 200
        health = response.json()
        assert health["status"] == "healthy"
        assert health["forecast_quality_authority"] == "delayed_feedback"
        assert not any(row["name"] == "Measured forecast quality" for row in health["checks"])
        assert any(
            row["name"] == "Delayed outcome evidence" and row["status"] == "healthy"
            for row in health["checks"]
        )
        # Legacy current-generation evaluation has no actuals and cannot override retained quality.
        assert health["measured_monitoring"]["status"] == "insufficient_evidence"


@pytest.mark.parametrize("existing_journals", [False, True])
def test_denied_global_health_and_incident_do_not_create_or_mutate_journals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    existing_journals: bool,
) -> None:
    testclient = pytest.importorskip("fastapi.testclient")
    from nexus.portfolio import api
    from nexus.portfolio.monitoring import MonitorBook
    from nexus.portfolio.observability import AlertBook

    store, _ = make_store(tmp_path)
    paths = [
        store.path.parent / name
        for name in (
            "feedback.sqlite3",
            "monitoring.sqlite3",
            "feedback-monitoring.sqlite3",
            "alerts.sqlite3",
        )
    ]
    if existing_journals:
        retain_runtime_forecasts(store)
        MonitorBook(paths[1])
        MonitorBook(paths[2])
        AlertBook(paths[3]).transition("trigger", "fixture")

    def counts() -> dict[str, Any]:
        result = {}
        for path in paths:
            if path.exists():
                with closing(sqlite3.connect(path)) as db:
                    tables = [
                        row[0]
                        for row in db.execute(
                            "SELECT name FROM sqlite_schema WHERE type='table' "
                            "AND name NOT LIKE 'sqlite_%'"
                        )
                    ]
                    result[path.name] = {
                        name: db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
                        for name in tables
                    }
        return result

    before = counts()
    denied = actor(ActorRole.PLANNER, "OUT-OF-SCOPE")
    monkeypatch.setattr(api, "require_actor", lambda _: denied)
    monkeypatch.setattr(api, "authorize_mutation", lambda _: denied)
    app = api.create_app(store.path.parent, pipeline="runtime")
    app.state.portfolio_store = store
    with closing(testclient.TestClient(app)) as client:
        assert client.get("/api/health").status_code == 403
        assert client.post("/api/incidents/demo", json={"action": "resolve"}).status_code == 403
    assert counts() == before


def test_unknown_source_stays_unknown_and_actor_guard_precedes_monitor_write(
    tmp_path: Path,
) -> None:
    testclient = pytest.importorskip("fastapi.testclient")
    pytest.importorskip("jwt")
    from nexus.portfolio.api import create_app
    from nexus.portfolio.runtime_control import RuntimeControl
    from nexus.portfolio.runtime_monitoring import monitor_runtime

    store, _ = make_store(tmp_path)
    with store.transaction() as db:
        payload = store.seed()
        payload["metadata"]["source_watermark"] = ISSUE.isoformat()
        db.execute("UPDATE metadata SET value=? WHERE key='seed'", (json.dumps(payload),))
        db.execute("UPDATE metadata SET value=? WHERE key='seed_hash'", (digest(payload),))
    RuntimeControl(store)
    with pytest.raises(StoreError, match="all retained locations"):
        monitor_runtime(store, actor=actor(ActorRole.AUDITOR, "OTHER"))
    assert not (store.path.parent / "monitoring.sqlite3").exists()
    app = create_app(store.path.parent, pipeline="runtime")
    app.state.portfolio_store = store
    with closing(testclient.TestClient(app)) as client:
        client.post("/api/session", json={"persona": "auditor"})
        health = client.get("/api/health")
        assert health.status_code == 200
        assert health.json()["status"] == "unknown"
