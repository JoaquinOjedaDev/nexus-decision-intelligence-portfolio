"""HTTP session, cross-origin, request-boundary, and workflow smoke coverage."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from nexus.portfolio import store as portfolio_store
from nexus.portfolio.auth import COOKIE_NAME
from nexus.portfolio.store import APPROVAL_TTL_SECONDS, PortfolioStore

TestClient = pytest.importorskip("fastapi.testclient").TestClient
pytest.importorskip("jwt")
create_app = pytest.importorskip("nexus.portfolio.api").create_app


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    store = PortfolioStore(tmp_path)
    store.ensure_seed(
        {
            "metadata": {"synthetic": True},
            "recommendations": [
                {
                    "id": "rec-one",
                    "location_id": "WH-NORTH",
                    "sku": "SKU-001",
                    "recommended_quantity": 12,
                    "created_by": "planner.r4a",
                    "expected_cost": 25,
                }
            ],
            "metrics": {},
            "scenarios": [],
        }
    )
    app = create_app(tmp_path)
    app.state.portfolio_store = store
    return TestClient(app)


def login(client: TestClient, persona: str = "approver") -> dict[str, str]:
    response = client.post("/api/session", json={"persona": persona})
    assert response.status_code == 200, response.text
    return {"X-CSRF-Token": response.json()["csrf_token"]}


def binding(client: TestClient) -> str:
    return str(client.get("/api/recommendations").json()["items"][0]["fingerprint"])


@pytest.mark.parametrize("action", ["review", "resolve", "evidence"])
def test_recovery_requires_session_csrf_and_explicit_runtime(
    client: TestClient, action: str
) -> None:
    body: dict[str, Any] = {
        "request_sha256": "a" * 64,
        "decision_id": "00000000-0000-4000-8000-000000000001",
    }
    if action == "review":
        body.update(action="approve", reason="Independent review")
    url = f"/api/runtime/recovery/request-one/{action}"
    assert client.post(url, json=body).status_code == 401
    headers = login(client)
    assert client.post(url, json=body).status_code == 403
    assert client.post(url, json=body, headers=headers).status_code == 404
    assert client.post(url, json={**body, "actor": "admin"}, headers=headers).status_code == 422


def test_recovery_routes_forward_only_server_actor(client: TestClient, monkeypatch: Any) -> None:
    from nexus.portfolio import recovery_routes

    calls: list[tuple[str, str]] = []

    class Ledger:
        def review_status(self, binding: Any, request_id: str, actor: Any) -> dict[str, Any]:
            calls.append(("status", actor.actor_id))
            return {"request_id": request_id}

        def review_request(
            self, binding: Any, request_id: str, sha: str, actor: Any, **kwargs: Any
        ) -> dict[str, Any]:
            calls.append(("review", actor.actor_id))
            assert sha == "a" * 64
            assert kwargs["ttl_seconds"] == 900
            return {"request_id": request_id}

        def resolve(
            self, binding: Any, request_id: str, sha: str, decision: str, actor: Any
        ) -> dict[str, Any]:
            calls.append(("resolve", actor.actor_id))
            return {"request_id": request_id}

    monkeypatch.setattr(recovery_routes, "_recovery", lambda request: (Ledger(), object()))
    url = "/api/runtime/recovery/request-one"
    assert client.get(url + "/review").status_code == 401
    headers = login(client)
    body = {"request_sha256": "a" * 64, "decision_id": "00000000-0000-4000-8000-000000000001"}
    assert client.get(url + "/review").status_code == 200
    assert (
        client.post(
            url + "/review",
            headers=headers,
            json={**body, "action": "approve", "reason": "Reviewed"},
        ).status_code
        == 200
    )
    assert client.post(url + "/resolve", headers=headers, json=body).status_code == 200
    assert calls == [(name, "approver.portfolio") for name in ("status", "review", "resolve")]


def test_recovery_proposal_boundary(client: TestClient) -> None:
    url = "/api/runtime/recovery/proposals"
    body = {
        "partition": 0,
        "inbox_sha256": "a" * 64,
        "publication_id": "00000000-0000-4000-8000-000000000001",
        "reason": "Review",
    }
    assert client.post(url, json=body).status_code == 401
    headers = login(client, "planner")
    assert client.post(url, json=body).status_code == 403
    assert client.post(url, json=body, headers=headers).status_code == 404
    for field in ("source_path", "replacement_base64", "actor", "submitted_at"):
        assert (
            client.post(url, json={**body, field: "client-controlled"}, headers=headers).status_code
            == 422
        )
    for invalid in (True, -1, "0"):
        assert (
            client.post(url, json={**body, "partition": invalid}, headers=headers).status_code
            == 422
        )


def test_recovery_discovery_redacts_payload(client: TestClient, monkeypatch: Any) -> None:
    import json
    from types import SimpleNamespace

    from nexus.portfolio import recovery_routes

    url = "/api/runtime/recovery/pending/0"
    assert client.get(url).status_code == 401
    login(client, "auditor")
    assert client.get(url).status_code == 404
    evidence = {
        "dead_letter_id": "d",
        "binding_sha256": "b",
        "inbox_record_hash": "h",
        "original_value_sha256": "v",
        "classification": "invalid_event_envelope",
        "physical": {
            "partition": 0,
            "offset": 7,
            "value": "secret",
            "key": "secret",
            "headers": [["private", "secret"]],
        },
    }

    class Ledger:
        result: Any = SimpleNamespace(payload=json.dumps(evidence))

        def pending_dead_letter(self, binding: Any, partition: int, actor: Any) -> Any:
            assert actor.actor_id == "auditor.portfolio" and partition == 0
            return self.result

    ledger = Ledger()
    monkeypatch.setattr(recovery_routes, "_recovery", lambda request: (ledger, object()))
    result = client.get(url)
    assert result.status_code == 200 and "secret" not in result.text
    assert result.json()["pending"] == {
        **{k: v for k, v in evidence.items() if k != "physical"},
        "partition": 0,
        "offset": 7,
    }
    ledger.result = None
    assert client.get(url).json()["pending"] is None
    assert client.get("/api/runtime/recovery/pending/not-an-integer").status_code == 422


def test_app_factory_and_liveness_do_not_initialize_dataset(tmp_path: Path) -> None:
    app = create_app(tmp_path)
    client = TestClient(app)
    assert client.get("/healthz").json()["status"] == "ok"
    assert app.state.portfolio_store is None
    assert not (tmp_path / "portfolio.sqlite3").exists()
    assert client.get("/api/session").json()["user"] is None


def test_session_cookie_is_signed_http_only_and_logout_revokes_it(client: TestClient) -> None:
    response = client.post("/api/session", json={"persona": "approver"})
    cookie_header = response.headers["set-cookie"].lower()
    assert "httponly" in cookie_header
    assert "samesite=strict" in cookie_header
    assert "max-age=3600" in cookie_header
    token = client.cookies[COOKIE_NAME]
    assert client.get("/api/session").json()["user"]["role"] == "approver"
    assert client.delete("/api/session").status_code == 403
    assert (
        client.delete(
            "/api/session", headers={"X-CSRF-Token": response.json()["csrf_token"]}
        ).status_code
        == 200
    )
    client.cookies.set(COOKIE_NAME, token)
    assert client.get("/api/recommendations").status_code == 401


def test_session_persona_is_server_owned_and_rotation_invalidates_old_token(
    client: TestClient,
) -> None:
    assert (
        client.post("/api/session", json={"persona": "planner", "role": "approver"}).status_code
        == 422
    )
    assert client.post("/api/session", json={"persona": "system"}).status_code == 422
    login(client, "planner")
    previous = client.cookies[COOKIE_NAME]
    login(client, "approver")
    store = client.app.state.portfolio_store
    assert store.session(previous) is None


def test_unauthenticated_and_tampered_sessions_cannot_read_data(client: TestClient) -> None:
    assert client.get("/api/recommendations").status_code == 401
    login(client)
    token = client.cookies[COOKIE_NAME]
    client.cookies.clear()
    client.cookies.set(COOKIE_NAME, token[:-10] + "forged12345")
    assert client.get("/api/recommendations").status_code == 401


def test_csrf_origin_and_content_type_are_enforced(client: TestClient) -> None:
    headers = login(client)
    payload = {"fingerprint": binding(client)}
    url = "/api/recommendations/rec-one/approve"
    assert client.post(url, json=payload).status_code == 403
    assert client.post(url, json=payload, headers={"X-CSRF-Token": "bad"}).status_code == 403
    assert (
        client.post(
            url, json=payload, headers={**headers, "Origin": "https://evil.example"}
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/api/session", json={"persona": "approver"}, headers={"Origin": "null"}
        ).status_code
        == 403
    )
    assert client.post(url, data=payload, headers=headers).status_code == 415
    assert (
        client.post(
            url, json=payload, headers={**headers, "Origin": "http://testserver"}
        ).status_code
        == 200
    )


def test_fixture_login_refused_in_production_mode(tmp_path: Path) -> None:
    client = TestClient(create_app(tmp_path, mode="production"))
    response = client.post("/api/session", json={"persona": "approver"})
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "fixture_mode_disabled"
    assert not (tmp_path / "portfolio.sqlite3").exists()


def test_remote_peer_and_untrusted_host_cannot_access_fixture(tmp_path: Path) -> None:
    app = create_app(tmp_path)
    external = TestClient(app, client=("203.0.113.10", 1234))
    assert external.get("/healthz").status_code == 403
    dns_rebinding = TestClient(app, base_url="http://evil.example")
    assert dns_rebinding.get("/healthz").status_code == 403


def test_http_human_approval_execution_retry_and_audit(client: TestClient) -> None:
    headers = login(client, "planner")
    payload = {"fingerprint": binding(client)}
    base = "/api/recommendations/rec-one"
    assert client.post(base + "/approve", json=payload, headers=headers).status_code == 403
    headers = login(client, "approver")
    assert client.post(base + "/execute", json=payload, headers=headers).status_code == 422
    execution_headers = {**headers, "Idempotency-Key": "approval-demo-1"}
    assert (
        client.post(base + "/execute", json=payload, headers=execution_headers).status_code == 409
    )
    assert client.post(base + "/approve", json=payload, headers=headers).status_code == 200
    failed = client.post(
        base + "/execute",
        json={**payload, "simulate_failure": True},
        headers=execution_headers,
    )
    assert failed.json()["state"] == "execution_failed"
    executed = client.post(base + "/execute", json=payload, headers=execution_headers)
    replay = client.post(base + "/execute", json=payload, headers=execution_headers)
    assert executed.json()["execution_ref"] == replay.json()["execution_ref"]
    assert client.get("/api/overview").json()["summary"]["effect_count"] == 1
    audit = client.get("/api/audit").json()
    assert audit["integrity"] == "verified"
    assert sum(item["event_type"] == "executed" for item in audit["items"]) == 1
    assert client.post(base + "/outcome", json=payload, headers=headers).status_code == 403
    headers = login(client, "planner")
    assert client.post(base + "/outcome", json=payload, headers=headers).status_code == 425


def test_failed_mock_execution_requires_renewal_after_approval_expires(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = client.app.state.portfolio_store
    clock = [float(int(store.clock()) + 1)]
    store.clock = lambda: clock[0]
    headers = login(client)
    payload = {"fingerprint": binding(client)}
    base = "/api/recommendations/rec-one"
    execution_headers = {**headers, "Idempotency-Key": "renewed-approval"}
    approved = client.post(base + "/approve", json=payload, headers=headers)
    assert approved.status_code == 200
    failed = client.post(
        base + "/execute",
        json={**payload, "simulate_failure": True},
        headers=execution_headers,
    )
    assert failed.json()["state"] == "execution_failed"
    assert failed.json()["approval_renewable"] is False
    premature = client.post(base + "/approve", json=payload, headers=headers)
    assert premature.status_code == 409

    clock[0] += APPROVAL_TTL_SECONDS
    detail = client.get(base).json()
    assert detail["expired"] is False
    assert detail["approval_renewable"] is True
    refused = client.post(base + "/execute", json=payload, headers=execution_headers)
    assert refused.status_code == 409
    assert refused.json()["error"]["code"] == "approval_expired"
    assert store.metrics_snapshot()["effect_count"] == 0

    wrong_fingerprint = client.post(
        base + "/approve", json={"fingerprint": "0" * 64}, headers=headers
    )
    assert wrong_fingerprint.status_code == 409
    assert wrong_fingerprint.json()["error"]["code"] == "stale_fingerprint"

    resolve_persona = portfolio_store.resolve_persona
    with monkeypatch.context() as other_approver:
        other_approver.setattr(
            portfolio_store,
            "resolve_persona",
            lambda persona, locations: replace(
                resolve_persona(persona, locations), actor_id="approver.other"
            ),
        )
        other_headers = login(client)
        wrong_approver = client.post(base + "/approve", json=payload, headers=other_headers)
        assert wrong_approver.status_code == 409
        assert wrong_approver.json()["error"]["code"] == "illegal_transition"

    headers = login(client)
    execution_headers.update(headers)
    renewed = client.post(base + "/approve", json=payload, headers=headers)
    assert renewed.status_code == 200
    assert renewed.json()["state"] == "approved"
    assert renewed.json()["fingerprint"] == payload["fingerprint"]
    assert renewed.json()["approval_expires_at"] > approved.json()["approval_expires_at"]
    assert renewed.json()["approval_renewable"] is False
    executed = client.post(base + "/execute", json=payload, headers=execution_headers)
    replay = client.post(base + "/execute", json=payload, headers=execution_headers)
    assert executed.json()["state"] == "executed"
    assert replay.json()["execution_ref"] == executed.json()["execution_ref"]
    assert store.metrics_snapshot()["effect_count"] == 1

    audit = client.get("/api/audit").json()
    assert audit["integrity"] == "verified"
    assert [item["event_type"] for item in audit["items"]] == [
        "recommendation_created",
        "approved",
        "execution_requested",
        "execution_failed",
        "approved",
        "execution_requested",
        "executed",
    ]


@pytest.mark.parametrize(
    "extra",
    [
        {"quantity": 999},
        {"actor_role": "system"},
        {"approval_expires_at": "2099-01-01"},
        {"fingerprint": "not-a-sha256"},
    ],
)
def test_write_payload_cannot_change_authority_or_content(
    client: TestClient, extra: dict[str, Any]
) -> None:
    headers = login(client)
    payload = {"fingerprint": binding(client), **extra}
    assert (
        client.post(
            "/api/recommendations/rec-one/approve", json=payload, headers=headers
        ).status_code
        == 422
    )


def test_error_does_not_disclose_internal_exceptions(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    login(client)

    def failure(_actor: Any) -> dict[str, Any]:
        raise RuntimeError("database_password=very-secret")

    monkeypatch.setattr(client.app.state.portfolio_store, "overview_snapshot", failure)
    response = client.get("/api/overview")
    assert response.status_code == 503
    assert "very-secret" not in response.text
    assert response.json()["error"]["code"] == "internal_error"


def test_restart_reverifies_cached_evidence_before_serving_persisted_data(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nexus.portfolio import engine

    store = client.app.state.portfolio_store
    calls: list[tuple[Path, Path]] = []

    def verified(root: Path, artifacts: Path) -> dict[str, Any]:
        calls.append((root, artifacts))
        return store.seed()

    monkeypatch.setattr(engine, "build_seed", verified)
    restart = TestClient(create_app(store.path.parent))
    assert restart.get("/readyz").status_code == 200
    assert len(calls) == 1
    login(restart)
    assert restart.get("/api/recommendations").status_code == 200
    assert len(calls) == 1


def test_tampered_artifact_prevents_restart_but_does_not_destroy_workflow(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nexus.portfolio import engine

    store = client.app.state.portfolio_store

    def corrupted(_root: Path, _artifacts: Path) -> dict[str, Any]:
        raise ValueError("Artifact hash mismatch: private-local-path")

    monkeypatch.setattr(engine, "build_seed", corrupted)
    restart = TestClient(create_app(store.path.parent))
    result = restart.get("/readyz")
    assert result.status_code == 503
    assert "private-local-path" not in result.text
    assert store.has_seed()
    assert store.metrics_snapshot()["event_count"] == 1


def test_overview_separates_actionable_reviews_from_expired_history(client: TestClient) -> None:
    store = client.app.state.portfolio_store
    clock = [store.clock()]
    store.clock = lambda: clock[0]
    login(client)
    before = client.get("/api/overview").json()
    assert before["summary"]["pending"] == 1
    assert before["summary"]["expired_review"] == 0
    binding_value = before["recommendations"][0]["fingerprint"]
    # ISO timestamps round SQLite's sub-microsecond float; move just beyond the boundary.
    clock[0] = (
        datetime.fromisoformat(before["recommendations"][0]["valid_until"]).timestamp() + 0.001
    )
    headers = login(client)
    after = client.get("/api/overview").json()
    assert after["summary"]["pending"] == 0
    assert after["summary"]["expired_review"] == 1
    assert after["summary"]["total"] == 1
    assert after["recommendations"][0]["state"] == "pending"
    assert after["recommendations"][0]["expired"] is True
    assert datetime.fromisoformat(after["checked_at"]).timestamp() == pytest.approx(
        clock[0], abs=0.000001, rel=0
    )
    refused = client.post(
        "/api/recommendations/rec-one/approve",
        json={"fingerprint": binding_value},
        headers=headers,
    )
    assert refused.status_code == 409
    assert refused.json()["error"]["code"] == "recommendation_expired"
