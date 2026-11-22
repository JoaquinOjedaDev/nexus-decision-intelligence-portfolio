"""Actual PostgreSQL + authenticated HTTP integration; constructed source fixtures.

These are not scientific pipeline, real-human, browser or Compose acceptance.
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import LOCATIONS, source_seed

from nexus.portfolio import runtime_service
from nexus.portfolio.api import create_app
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.runtime_scheduler import monitoring_tick
from nexus.portfolio.service_backend import bind_service_root

pytestmark = pytest.mark.portfolio_service


def rich_seed(root: Path, source: str = "a") -> dict[str, Any]:
    value = source_seed(root, source)
    value["metadata"].update(
        source_watermark="2026-06-30T00:00:00+00:00",
        scientific_decision_time="2026-06-30T00:00:00+00:00",
    )
    value["forecasts"] = [
        {
            "sku_id": f"SKU-{index:03}",
            "location_id": location,
            "model": "constructed-baseline",
            "forecast_sha256": source * 64,
            "origin": "2026-06-30T00:00:00+00:00",
            "points": [{"date": "2026-07-01", "q025": 0, "q10": 1, "q50": 2, "q90": 3, "q975": 4}],
        }
        for index, location in enumerate(LOCATIONS, 1)
    ]
    return value


@pytest.fixture
def application(cluster: Cluster, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    root = tmp_path / "runtime"
    now = [datetime(2026, 9, 13, tzinfo=UTC).timestamp()]
    initial = rich_seed(root / "evidence" / "initial")
    store = PostgresRuntimeStore(
        cluster.backend(root / "evidence" / "initial").config,
        runtime_root=root,
        clock=lambda: now[0],
    )
    cluster.migrate(store)
    store.ensure_seed(initial)
    bind_service_root(store, claim=True)
    receipts = {str(store.artifact_root.resolve()): initial}

    def build(
        project: Path, evidence: Path, source: Path | None = None, **_: Any
    ) -> dict[str, Any]:
        seed = rich_seed(evidence, "b")
        receipts[str(evidence.resolve())] = seed
        return copy.deepcopy(seed)

    monkeypatch.setattr(runtime_service, "build_runtime_seed", build)
    monkeypatch.setattr(runtime_service, "runtime_source_fingerprint", lambda path: "b" * 64)
    monkeypatch.setattr(
        runtime_service,
        "verify_runtime_current",
        lambda project, path: copy.deepcopy(receipts[str(path.resolve())]),
    )

    # Keep the controlled clock; the path/admission factory is covered separately.
    def constructed_store(directory: Path, config: Any, *, transport_fence: Any = None) -> Any:
        assert transport_fence is None
        return store

    monkeypatch.setattr(runtime_service, "_postgres_store", constructed_store)
    app = create_app(root, pipeline="runtime", postgres=store.config)
    app.state.test_clock = now
    app.state.test_store = store
    return app


def login(client: TestClient, persona: str = "planner") -> dict[str, str]:
    response = client.post("/api/session", json={"persona": persona})
    assert response.status_code == 200, response.text
    return {"X-CSRF-Token": response.json()["csrf_token"]}


def test_all_registered_readers_and_independent_workflow_use_postgres(application: Any) -> None:
    planner = TestClient(application)
    headers = login(planner)
    store = application.state.test_store
    assert application.state.portfolio_store is store
    assert not (store.runtime_root / "portfolio.sqlite3").exists()
    for path in (
        "/readyz",
        "/api/overview",
        "/api/recommendations",
        "/api/audit",
        "/api/runtime/status",
        "/api/health",
        "/api/learning",
    ):
        response = planner.get(path)
        assert response.status_code == 200, (path, response.text)
    assert planner.get("/api/learning").json()["status"] == "not_run"
    health = planner.get("/api/health").json()
    assert (
        next(row for row in health["checks"] if row["name"] == "Durable state")["value"]
        == "PostgreSQL"
    )
    assert (
        planner.post("/api/copilot", json={"question": "Which model is the champion?"}).status_code
        == 200
    )
    for action in ("trigger", "resolve"):
        response = planner.post("/api/incidents/demo", json={"action": action}, headers=headers)
        assert response.status_code == 200, response.text

    item = planner.get("/api/recommendations").json()["items"][0]
    endpoint = "/api/recommendations/" + item["id"]
    assert planner.get(endpoint).status_code == 200
    body = {"fingerprint": item["fingerprint"], "note": "Constructed integration approval"}
    assert planner.post(endpoint + "/approve", json=body, headers=headers).status_code == 403
    approver = TestClient(application)
    approval_headers = login(approver, "approver")
    assert (
        approver.post(endpoint + "/approve", json=body, headers=approval_headers).status_code == 200
    )
    for _ in range(2):
        executed = approver.post(
            endpoint + "/execute",
            json={"fingerprint": item["fingerprint"]},
            headers={**approval_headers, "Idempotency-Key": "postgres-http-unique-effect"},
        )
        assert executed.status_code == 200, executed.text
    application.state.test_clock[0] += 6
    assert planner.post(endpoint + "/outcome", json=body, headers=headers).status_code == 200
    assert planner.get("/api/overview").json()["summary"]["effect_count"] == 1
    assert planner.get("/api/audit").json()["integrity"] == "verified"
    assert planner.delete("/api/session", headers=headers).status_code == 200
    assert planner.get("/api/overview").status_code == 401


def test_http_event_replans_and_all_readers_retain_both_generations(application: Any) -> None:
    client = TestClient(application)
    headers = login(client)
    original = client.get("/api/recommendations").json()["items"][0]
    payload = {
        "event_id": str(uuid4()),
        "event_type": "inventory.adjusted",
        "occurred_at": datetime.fromtimestamp(application.state.test_clock[0], UTC).isoformat(),
        "source_snapshot_sha256": "b" * 64,
    }
    assert client.post("/api/runtime/events", json=payload).status_code == 403
    for _ in range(2):
        response = client.post("/api/runtime/events", json=payload, headers=headers)
        assert response.status_code == 200, response.text
    result = runtime_service.run_runtime_worker(
        application.state.portfolio_root,
        application.state.test_store,
        source_dir=Path("constructed"),
    )
    assert result["status"] == "completed", result
    monitoring_tick(application)
    assert application.state.runtime_monitor_error is False
    state = client.get("/api/runtime/status").json()
    assert state["event_count"] == 1 and state["operational"]["completed"] == 1
    assert state["archived_generations"] == 1
    overview = client.get("/api/overview").json()
    assert overview["summary"]["historical"] == 2
    assert overview["summary"]["current_generation"] == 2
    assert overview["summary"]["effect_count"] == 0
    retained = client.get("/api/recommendations/" + original["id"])
    assert retained.status_code == 200, retained.text
    health = client.get("/api/health")
    assert health.status_code == 200, health.text
    assert health.json()["delayed_feedback"]["forecast_generations"] == 2
    assert client.get("/api/learning").json()["status"] == "not_run"
    assert not (application.state.test_store.runtime_root / "portfolio.sqlite3").exists()
