"""Old decision evidence stays generation-owned across activation and restart."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from typing import Any
from uuid import uuid4

import pytest

from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.runtime_control import RuntimeControl
from nexus.portfolio.store import PortfolioStore, StoreError, canonical, digest


def seed(source: str, quantity: int) -> dict[str, Any]:
    return {
        "metadata": {
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": source * 64,
        },
        "executable": True,
        "recommendations": [
            {
                "id": "rec-1",
                "sku_id": "SKU-001",
                "location_id": "NORTH",
                "recommended_quantity": quantity,
                "expected_cost": quantity * 5,
                "created_by": "planner.r4a",
            }
        ],
        "scenarios": [{"name": f"scenario-{source}", "expected_cost": quantity * 8}],
        "lineage": [{"stage": "Decisioning", "trace_id": f"lineage-{source}"}],
        "metrics": {"order_value": quantity * 5},
    }


@pytest.fixture
def context(tmp_path: Path) -> tuple[PortfolioStore, RuntimeControl, Actor, Actor]:
    store = PortfolioStore(tmp_path, clock=lambda: 1_900_000_000.0)
    store.ensure_seed(seed("a", 10))
    control = RuntimeControl(store)
    planner = resolve_persona("planner", ("NORTH",))
    worker = Actor("runtime-worker", "Worker", ActorRole.SYSTEM, ("NORTH",))
    return store, control, planner, worker


def activate(context: tuple[PortfolioStore, RuntimeControl, Actor, Actor]) -> str:
    store, control, planner, worker = context
    now = datetime.fromtimestamp(store.clock(), UTC)
    control.submit(
        EventEnvelope(
            event_id=uuid4(),
            event_type="inventory.adjusted",
            source="fixture",
            subject_id="portfolio",
            occurred_at=now,
            ingested_at=now,
            idempotency_key=uuid4().hex,
            payload={"source_snapshot_sha256": "b" * 64},
        ),
        planner,
    )
    job = control.claim(worker)
    assert job is not None
    control.activate(job["id"], job["token"], seed("b", 20), worker)
    return f"{job['id']}-rec-1"


def test_old_and_current_decisions_retain_their_own_scenarios_on_restart(
    context: tuple[PortfolioStore, RuntimeControl, Actor, Actor],
) -> None:
    store, _, planner, _ = context
    new_id = activate(context)
    reopened = PortfolioStore(store.path.parent, clock=store.clock)
    historical = reopened.recommendation_context("rec-1", planner)
    current = reopened.recommendation_context(new_id, planner)
    assert historical["scenarios"] == seed("a", 10)["scenarios"]
    assert historical["lineage"] == seed("a", 10)["lineage"]
    assert historical["generation"]["seed_sha256"] == digest(seed("a", 10))
    assert historical["generation"]["is_current"] is False
    assert historical["state"] == "invalidated"
    assert len(historical["events"]) == 2
    assert current["scenarios"] == seed("b", 20)["scenarios"]
    assert current["generation"]["is_current"] is True
    assert current["state"] == "pending"


def test_generation_context_enforces_role_scope_and_exact_id(
    context: tuple[PortfolioStore, RuntimeControl, Actor, Actor],
) -> None:
    store, _, planner, worker = context
    activate(context)
    for identity, actor, code in (
        ("rec-1", worker, "role_denied"),
        ("rec-1", replace(planner, locations=("SOUTH",)), "scope_denied"),
        ("suffix-rec-1", planner, "not_found"),
    ):
        with pytest.raises(StoreError) as raised:
            store.recommendation_context(identity, actor)
        assert raised.value.code == code


@pytest.mark.parametrize("reseal", [False, True])
def test_archive_payload_tampering_fails_closed(
    context: tuple[PortfolioStore, RuntimeControl, Actor, Actor], reseal: bool
) -> None:
    store, control, planner, _ = context
    activate(context)
    with store.transaction() as db:
        payload = json.loads(db.execute("SELECT payload FROM runtime_generations").fetchone()[0])
        payload["scenarios"][0]["name"] = "changed"
        db.execute("UPDATE runtime_generations SET payload=?", (canonical(payload),))
        if reseal:
            control._seal(db)
    with pytest.raises(StoreError, match=r"changed|hash mismatch"):
        store.recommendation_context("rec-1", planner)


def test_same_snapshot_does_not_mix_current_seed_and_new_recommendations(
    context: tuple[PortfolioStore, RuntimeControl, Actor, Actor], monkeypatch: pytest.MonkeyPatch
) -> None:
    store, _, planner, _ = context
    captured, updated = Event(), Event()
    original = store._generation_catalog

    def pause(db: Any) -> Any:
        result = original(db)
        captured.set()
        assert updated.wait(timeout=5)
        return result

    monkeypatch.setattr(store, "_generation_catalog", pause)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(store.overview_snapshot, planner)
        try:
            assert captured.wait(timeout=5)
            activate(context)
        finally:
            updated.set()
        snapshot = pending.result(timeout=5)
    assert snapshot["metrics"]["order_value"] == 50
    assert [item["id"] for item in snapshot["recommendations"]] == ["rec-1"]
    assert snapshot["recommendations"][0]["state"] == "pending"
    assert len(snapshot["current_recommendations"]) == 1


def test_http_distinguishes_current_plan_totals_from_retained_history(
    context: tuple[PortfolioStore, RuntimeControl, Actor, Actor],
) -> None:
    TestClient = pytest.importorskip("fastapi.testclient").TestClient
    pytest.importorskip("jwt")
    create_app = pytest.importorskip("nexus.portfolio.api").create_app
    store, _, _, _ = context
    new_id = activate(context)
    app = create_app(store.path.parent, pipeline="runtime")
    app.state.portfolio_store = store
    with TestClient(app) as client:
        client.post("/api/session", json={"persona": "auditor"})
        overview = client.get("/api/overview").json()
        assert overview["summary"]["total"] == 2
        assert overview["summary"]["current_generation"] == 1
        assert overview["summary"]["historical"] == 1
        assert overview["summary"]["current_recommended_units"] == 20
        assert overview["summary"]["current_order_value"] == 100
        assert overview["metrics"]["order_value"] == 100
        assert len(overview["recommendations"]) == 2
        assert [item["id"] for item in overview["current_recommendations"]] == [new_id]
        assert client.get("/api/recommendations").json()["total"] == 2
        assert (
            client.get("/api/recommendations/rec-1").json()["scenarios"]
            == seed("a", 10)["scenarios"]
        )
