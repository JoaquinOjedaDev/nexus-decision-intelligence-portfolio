"""Connected service orchestration tests, isolated from expensive scientific tests."""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from nexus.contracts import EventEnvelope
from nexus.portfolio import runtime_service as service
from nexus.portfolio.api import create_app
from nexus.portfolio.auth import resolve_persona
from nexus.portfolio.runtime_control import RuntimeControl
from nexus.portfolio.runtime_monitoring import monitor_runtime
from nexus.portfolio.store import PortfolioStore, StoreError, digest


def sample(source: str = "a") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "metadata": {
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": source * 64,
            "source_watermark": "2026-01-01T00:00:00+00:00",
            "scientific_decision_time": "2026-01-02T00:00:00+00:00",
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
        "metrics": {},
        "forecasts": [
            {
                "sku_id": "SKU-001",
                "location_id": "DC-ARG",
                "model": "seasonal-baseline",
                "forecast_sha256": "e" * 64,
                "origin": "2026-01-02T00:00:00+00:00",
                "points": [
                    {"date": "2026-01-02", "q025": 0, "q10": 5, "q50": 10, "q90": 15, "q975": 20}
                ],
            }
        ],
    }
    payload["seed_hash"] = digest(payload)
    return payload


@pytest.fixture
def setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[PortfolioStore, dict[str, Any]]:
    receipts: dict[str, Any] = {}

    def build(root: Path, evidence: Path, source: Path | None = None, **_: Any) -> dict[str, Any]:
        evidence.mkdir(parents=True, exist_ok=True)
        value = sample("b" if source else "a")
        receipts[str(evidence.resolve())] = value
        return json.loads(json.dumps(value))

    monkeypatch.setattr(service, "build_runtime_seed", build)
    monkeypatch.setattr(
        service,
        "verify_runtime_current",
        lambda root, path: json.loads(json.dumps(receipts[str(path.resolve())])),
    )
    monkeypatch.setattr(service, "runtime_source_fingerprint", lambda path: "b" * 64)
    store = service.prepare_runtime(tmp_path, tmp_path / "state")
    return store, receipts


def submit(store: PortfolioStore) -> None:
    now = datetime.fromtimestamp(store.clock(), UTC)
    RuntimeControl(store).submit(
        EventEnvelope(
            event_id=uuid4(),
            event_type="inventory.adjusted",
            source="test-source",
            subject_id="portfolio",
            occurred_at=now,
            ingested_at=now,
            idempotency_key=uuid4().hex,
            payload={"source_snapshot_sha256": "b" * 64},
        ),
        resolve_persona("planner", store.locations()),
    )


def test_prepare_restart_does_not_rebuild_or_reset(
    setup: tuple[PortfolioStore, dict[str, Any]],
) -> None:
    store, receipts = setup
    reopened = service.prepare_runtime(store.path.parent.parent, store.path.parent)
    assert reopened.seed() == store.seed() and len(receipts) == 1
    assert reopened.metrics_snapshot()["event_count"] == 1
    with pytest.raises(StoreError, match="Submit a material event"):
        service.prepare_runtime(
            store.path.parent.parent, store.path.parent, source_dir=Path("changed")
        )


def test_worker_activation_survives_verified_restart(
    setup: tuple[PortfolioStore, dict[str, Any]],
) -> None:
    store, _ = setup
    submit(store)
    result = service.run_runtime_worker(store.path.parent.parent, store, source_dir=Path("changed"))
    assert result["status"] == "completed" and result["human_approval_required"]
    assert store.seed()["metadata"]["runtime_artifact_seed_hash"] == sample("b")["seed_hash"]
    reopened = service.prepare_runtime(store.path.parent.parent, store.path.parent)
    assert service.verify_runtime_store(store.path.parent.parent, reopened) == store.seed()
    assert reopened.metrics_snapshot()["states"] == {"invalidated": 1, "pending": 1}
    assert (
        service.run_runtime_worker(store.path.parent.parent, reopened, source_dir=Path("changed"))[
            "status"
        ]
        == "idle"
    )


def test_wrong_source_fails_job_without_effects(
    setup: tuple[PortfolioStore, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, _ = setup
    submit(store)
    monkeypatch.setattr(service, "runtime_source_fingerprint", lambda path: "c" * 64)
    result = service.run_runtime_worker(store.path.parent.parent, store, source_dir=Path("wrong"))
    assert result["status"] == "failed" and result["reason"] == "source_mismatch"
    assert store.metrics_snapshot()["states"] == {"invalidated": 1}
    assert store.metrics_snapshot()["effect_count"] == 0


def test_artifact_mismatch_and_metadata_tamper_fail_closed(
    setup: tuple[PortfolioStore, dict[str, Any]],
) -> None:
    store, receipts = setup
    entry = next(iter(receipts))
    receipts[entry]["recommendations"][0]["recommended_quantity"] = 999
    with pytest.raises(StoreError, match="verified artifacts"):
        service.verify_runtime_store(store.path.parent.parent, store)
    with store.transaction() as db:
        db.execute("UPDATE metadata SET value='../escape' WHERE key='runtime_evidence_path'")
    with pytest.raises(StoreError, match="changed"):
        service.verify_runtime_store(store.path.parent.parent, store)


def test_local_freshness_not_historical_age_and_missing_labels_not_healthy(
    setup: tuple[PortfolioStore, dict[str, Any]],
) -> None:
    store, _ = setup
    now = [store.clock()]
    store.clock = lambda: now[0]
    first = monitor_runtime(store)
    assert first["status"] == "insufficient_evidence"
    assert first["checks"]["local_runtime_ingestion:source_freshness"]["status"] == "healthy"
    now[0] += 172_801
    second = monitor_runtime(store)
    assert second["status"] == "firing"
    assert second["notifications_delivered"] == 1
    assert monitor_runtime(store)["notifications_delivered"] == 1


def test_prepare_refuses_frozen_state_before_creating_runtime_journals(tmp_path: Path) -> None:
    store = PortfolioStore(tmp_path / "frozen")
    frozen = sample()
    frozen["metadata"]["pipeline_mode"] = "frozen_demo"
    store.ensure_seed(frozen)
    with store.connection() as db:
        before = list(db.execute("SELECT name FROM sqlite_schema ORDER BY name"))
    with pytest.raises(StoreError, match="frozen state is preserved"):
        service.prepare_runtime(tmp_path, store.path.parent)
    with store.connection() as db:
        after = list(db.execute("SELECT name FROM sqlite_schema ORDER BY name"))
    assert [tuple(row) for row in after] == [tuple(row) for row in before]
    assert not (store.path.parent / "feedback.sqlite3").exists()


def test_runtime_http_is_authenticated_csrf_scoped_and_idempotent(
    setup: tuple[PortfolioStore, dict[str, Any]],
) -> None:
    store, _ = setup
    app = create_app(store.path.parent, pipeline="runtime")
    app.state.portfolio_store = store
    with TestClient(app) as client:
        assert client.get("/api/runtime/status").status_code == 401
        login = client.post("/api/session", json={"persona": "approver"}).json()
        body = {
            "event_id": str(uuid4()),
            "event_type": "inventory.adjusted",
            "occurred_at": datetime.fromtimestamp(store.clock(), UTC).isoformat(),
            "source_snapshot_sha256": "b" * 64,
        }
        assert client.post("/api/runtime/events", json=body).status_code == 403
        assert (
            client.post(
                "/api/runtime/events", json=body, headers={"X-CSRF-Token": login["csrf_token"]}
            ).status_code
            == 403
        )
        login = client.post("/api/session", json={"persona": "planner"}).json()
        headers = {"X-CSRF-Token": login["csrf_token"]}
        first = client.post("/api/runtime/events", json=body, headers=headers)
        assert first.status_code == 200, first.text
        assert first.json()["invalidated"] == 1
        assert client.post("/api/runtime/events", json=body, headers=headers).json()["duplicate"]
        health = client.get("/api/health")
        assert health.status_code == 200, health.text
        assert health.json()["status"] == "insufficient_evidence"
        assert health.json()["runtime"]["event_count"] == 1
        assert client.get("/api/overview").json()["summary"]["invalidated"] == 1
        assert (
            client.post(
                "/api/runtime/events", json={**body, "occurred_at": "not a date"}, headers=headers
            ).status_code
            == 422
        )


def test_frozen_app_rejects_runtime_control(setup: tuple[PortfolioStore, dict[str, Any]]) -> None:
    store, _ = setup
    app = create_app(store.path.parent)
    app.state.portfolio_store = store
    with TestClient(app) as client:
        client.post("/api/session", json={"persona": "planner"})
        assert client.get("/api/runtime/status").status_code == 404


def test_reclaimed_worker_has_separate_writable_evidence_directory(
    setup: tuple[PortfolioStore, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, receipts = setup
    now = [store.clock()]
    store.clock = lambda: now[0]
    submit(store)
    started, release = threading.Event(), threading.Event()
    paths: list[Path] = []

    def build(root: Path, evidence: Path, source: Path, **kwargs: Any) -> dict[str, Any]:
        evidence.mkdir(parents=True, exist_ok=True)
        paths.append(evidence)
        receipts[str(evidence.resolve())] = sample("b")
        if len(paths) == 1:
            started.set()
            assert release.wait(timeout=5)
        return sample("b")

    monkeypatch.setattr(service, "build_runtime_seed", build)
    with ThreadPoolExecutor(max_workers=2) as pool:
        old = pool.submit(
            service.run_runtime_worker, store.path.parent.parent, store, source_dir=Path("source")
        )
        assert started.wait(timeout=5)
        now[0] += 301
        current = service.run_runtime_worker(
            store.path.parent.parent, store, source_dir=Path("source")
        )
        release.set()
        previous = old.result(timeout=5)
    assert current["status"] == "completed"
    assert previous["status"] == "failed" and previous["reason"] == "stale_worker"
    assert len(paths) == 2 and paths[0] != paths[1]
    assert paths[0].parent == paths[1].parent
    assert service.verify_runtime_store(store.path.parent.parent, store) == store.seed()


def test_captured_restart_snapshot_stays_consistent_during_activation(
    setup: tuple[PortfolioStore, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, receipts = setup
    submit(store)
    control = RuntimeControl(store)
    worker = resolve_persona("planner", store.locations())
    from nexus.contracts import ActorRole
    from nexus.portfolio.auth import Actor

    system = Actor("runtime-worker", "Worker", ActorRole.SYSTEM, worker.locations)
    job = control.claim(system)
    assert job
    artifact = store.path.parent / "evidence" / "jobs" / job["id"] / "verified"
    artifact.mkdir(parents=True)
    receipts[str(artifact.resolve())] = sample("b")
    captured, release = threading.Event(), threading.Event()

    def verify(root: Path, path: Path) -> dict[str, Any]:
        if path.name == "initial":
            captured.set()
            assert release.wait(timeout=5)
        return json.loads(json.dumps(receipts[str(path.resolve())]))

    monkeypatch.setattr(service, "verify_runtime_current", verify)
    with ThreadPoolExecutor(max_workers=1) as pool:
        old = pool.submit(service.verify_runtime_store, store.path.parent.parent, store)
        assert captured.wait(timeout=5)
        control.activate(job["id"], job["token"], sample("b"), system, evidence_dir=artifact)
        release.set()
        result = old.result(timeout=5)
    assert result["metadata"]["source_snapshot_sha256"] == "a" * 64
    assert (
        service.verify_runtime_store(store.path.parent.parent, store)["metadata"][
            "source_snapshot_sha256"
        ]
        == "b" * 64
    )


def test_initial_reference_crash_recovery_does_not_invent_ingestion_time(
    setup: tuple[PortfolioStore, dict[str, Any]],
) -> None:
    store, receipts = setup
    recovery_dir = store.path.parent.parent / "recovery"
    incomplete = PortfolioStore(recovery_dir)
    incomplete.ensure_seed(sample())
    (recovery_dir / "evidence" / "initial").mkdir(parents=True)
    receipts[str((recovery_dir / "evidence" / "initial").resolve())] = sample()
    recovered = service.prepare_runtime(recovery_dir.parent, recovery_dir)
    assert recovered.seed() == sample()
    assert monitor_runtime(recovered)["status"] == "unknown"
