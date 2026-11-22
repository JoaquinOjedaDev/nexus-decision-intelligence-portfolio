"""Real SQLite authorization and evidence; deterministic provider doubles, no model claims."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from pydantic import ValidationError
from test_portfolio_store import seed as seed

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.local_copilot import LocalQuestion, ToolSelection, answer_local, run_tool
from nexus.portfolio.local_provider import LocalProviderError, LocalReply
from nexus.portfolio.store import PortfolioStore, StoreError, digest

TOOLS = (
    "get_forecast",
    "get_inventory_risk",
    "explain_recommendation",
    "get_supplier_terms",
    "compare_scenarios",
    "draft_purchase_order",
    "abstain",
)
NOW = 1_800_000_000.0


@pytest.fixture
def store(tmp_path: Path, seed: dict[str, Any]) -> PortfolioStore:
    for row in seed["recommendations"]:
        row.update(
            supplier_id="SUP-001",
            lead_time_days=3,
            initial_inventory=15,
            risk="high",
            risk_definition="Interval width, not a failure probability.",
            reason="Bounded constrained optimization.",
        )
    seed["forecasts"] = [
        {
            "id": "rec-1",
            "sku_id": "SKU-001",
            "location_id": "WH-NORTH",
            "points": [{"date": "2027-01-16", "p10": 1.0, "p50": 3.0, "p90": 9.0}],
            "model": "fixture-seasonal",
            "origin": "2027-01-15T08:00:00Z",
        }
    ]
    seed["scenarios"] = [{"id": "normal", "fill_rate": 0.91, "cost": 84.0}]
    result = PortfolioStore(tmp_path, clock=lambda: NOW)
    result.ensure_seed(seed)
    return result


def actor(role: str = "planner") -> Actor:
    return resolve_persona(role, ("WH-NORTH", "WH-SOUTH"))


def request(store: PortfolioStore, **changes: Any) -> LocalQuestion:
    row = store.recommendation_context("rec-1", actor())
    return LocalQuestion(
        **{
            "question": "Explain this recommendation",
            "recommendation_id": "rec-1",
            "location_id": "WH-NORTH",
            "fingerprint": row["fingerprint"],
            "generation_sha256": row["generation"]["seed_sha256"],
            **changes,
        }
    )


@pytest.mark.parametrize("tool", TOOLS)
def test_tools_return_exact_bound_evidence_without_writes(store: PortfolioStore, tool: Any) -> None:
    before = store.audit(actor())
    result = run_tool(ToolSelection(tool=tool), request(store), store, actor())
    assert store.audit(actor()) == before
    assert store.metrics_snapshot()["effect_count"] == 0
    assert result["read_only"] is True
    assert result["evidence_sha256"] == digest(
        {key: value for key, value in result.items() if key != "evidence_sha256"}
    )
    if tool != "abstain":
        assert result["citations"][0]["payload_sha256"] == digest(result["payload"])
        assert result["citations"][0]["source_version"] == request(store).generation_sha256
    if tool == "get_forecast":
        assert result["payload"]["points"][0]["p50"] == 3.0
    elif tool == "get_inventory_risk":
        assert result["payload"]["initial_inventory"] == 15
        assert "not a failure probability" in result["payload"]["risk_definition"]
    elif tool == "get_supplier_terms":
        assert result["payload"]["lead_time_days"] == 3
        assert "not available" in result["payload"]["scope_note"]
    elif tool == "draft_purchase_order":
        assert result["payload"]["recommended_quantity"] == 24
        assert result["payload"]["submitted"] is False
        assert result["payload"]["requires_independent_approval"] is True


@pytest.mark.parametrize("tool", TOOLS)
@pytest.mark.parametrize(
    "principal",
    [
        Actor("worker", "Worker", ActorRole.SYSTEM, ("WH-NORTH", "WH-SOUTH")),
        resolve_persona("planner", ("WH-SOUTH",)),
    ],
)
def test_denied_before_service_read(store: PortfolioStore, tool: Any, principal: Actor) -> None:
    body = request(store)
    store.recommendation_context = Mock(side_effect=AssertionError("must not read"))  # type: ignore[method-assign]
    with pytest.raises(StoreError) as caught:
        run_tool(ToolSelection(tool=tool), body, store, principal)
    assert caught.value.status == 403
    store.recommendation_context.assert_not_called()


@pytest.mark.parametrize("role", ["auditor", "approver"])
def test_draft_role_denial_before_read(store: PortfolioStore, role: str) -> None:
    body = request(store)
    store.recommendation_context = Mock(side_effect=AssertionError("must not read"))  # type: ignore[method-assign]
    with pytest.raises(StoreError, match="Only a planner"):
        run_tool(ToolSelection(tool="draft_purchase_order"), body, store, actor(role))


def test_full_plan_scope_is_required_even_for_sqlite(store: PortfolioStore) -> None:
    with pytest.raises(StoreError, match="all retained locations"):
        run_tool(
            ToolSelection(tool="compare_scenarios"),
            request(store),
            store,
            resolve_persona("planner", ("WH-NORTH",)),
        )


@pytest.mark.parametrize(
    "case", ["missing_field", "no_forecast", "ambiguous_forecast", "no_scenarios"]
)
def test_missing_or_ambiguous_evidence_abstains_without_fabrication(
    store: PortfolioStore, tmp_path: Path, case: str
) -> None:
    seed = store.seed()
    if case == "missing_field":
        del seed["recommendations"][0]["supplier_id"]
        tool = "get_supplier_terms"
    elif case == "no_scenarios":
        seed["scenarios"] = []
        tool = "compare_scenarios"
    else:
        seed["forecasts"] = [] if case == "no_forecast" else seed["forecasts"] * 2
        tool = "get_forecast"
    alternate = PortfolioStore(tmp_path / "alternate", clock=lambda: NOW)
    alternate.ensure_seed(seed)
    with pytest.raises(StoreError) as caught:
        run_tool(
            ToolSelection.model_validate({"tool": tool}), request(alternate), alternate, actor()
        )
    assert caught.value.code == "evidence_unavailable"


@pytest.mark.parametrize(
    "changes",
    [
        {"generation_sha256": "f" * 64},
        {"fingerprint": "f" * 64},
        {"location_id": "WH-SOUTH"},
    ],
)
def test_stale_binding_and_wrong_location_denied(store: PortfolioStore, changes: Any) -> None:
    with pytest.raises(StoreError):
        run_tool(ToolSelection(tool="get_forecast"), request(store, **changes), store, actor())


@pytest.mark.parametrize("action", ["expire", "reject", "approve"])
def test_invalid_or_nonpending_draft_is_refused(store: PortfolioStore, action: str) -> None:
    body = request(store)
    if action == "expire":
        store.clock = lambda: NOW + 86400
    else:
        store.decide("rec-1", actor("approver"), action, body.fingerprint)
    with pytest.raises(StoreError):
        run_tool(ToolSelection(tool="draft_purchase_order"), body, store, actor())


@pytest.mark.parametrize(
    "payload",
    [
        {"tool": "approve"},
        {"tool": "execute"},
        {"tool": "delete"},
        {"tool": "draft_purchase_order", "quantity": 999},
        {"tool": "get_forecast", "location_id": "WH-SECRET"},
        {"tool": "get_forecast", "actor": "admin"},
    ],
)
def test_model_cannot_invent_action_or_arguments(payload: Any) -> None:
    with pytest.raises(ValidationError):
        ToolSelection.model_validate(payload)


def test_model_output_only_selects_tool_never_numbers(store: PortfolioStore) -> None:
    provider = Mock()
    provider.generate.return_value = LocalReply(
        ToolSelection(tool="get_forecast"), {"model": "double"}
    )
    result = answer_local(request(store), store, actor, provider)
    assert result["evidence"]["payload"]["points"][0]["p50"] == 3.0
    assert result["trace"]["provider"] == {"model": "double"}
    args = provider.generate.call_args
    assert args.args[0] == "Explain this recommendation"
    assert "SUP-001" not in str(args)


@pytest.mark.parametrize("failure", ["revoked", "changed", "expired_evidence", "provider"])
def test_post_inference_revalidation_and_failure(store: PortfolioStore, failure: str) -> None:
    provider = Mock()
    calls = 0

    def resolve() -> Actor:
        nonlocal calls
        calls += 1
        if calls > 1:
            if failure == "revoked":
                raise StoreError("authentication_required", "Session revoked", 401)
            if failure == "changed":
                return replace(actor(), actor_id="different")
        return actor()

    def generate(*args: Any, **kwargs: Any) -> LocalReply:
        if failure == "expired_evidence":
            store.clock = lambda: NOW + 86400
        if failure == "provider":
            raise LocalProviderError("sensitive details must not escape")
        return LocalReply(ToolSelection(tool="get_forecast"), {})

    provider.generate.side_effect = generate
    with pytest.raises(StoreError) as caught:
        answer_local(request(store), store, resolve, provider)
    assert "sensitive" not in str(caught.value)


def test_http_is_opt_in_authenticated_csrf_guarded_and_default_unchanged(
    store: PortfolioStore,
) -> None:
    from fastapi.testclient import TestClient

    from nexus.portfolio.api import create_app

    app = create_app(store.path.parent)
    app.state.portfolio_store = store
    client = TestClient(app)
    body = request(store).model_dump()
    assert client.post("/api/copilot/local", json=body).status_code == 401
    session = client.post("/api/session", json={"persona": "planner"}).json()
    assert client.post("/api/copilot/local", json=body).status_code == 403
    headers = {"X-CSRF-Token": session["csrf_token"]}
    assert client.post("/api/copilot/local", json=body, headers=headers).status_code == 503
    provider = Mock()
    provider.generate.return_value = LocalReply(ToolSelection(tool="explain_recommendation"), {})
    app.state.local_model_provider = provider
    response = client.post("/api/copilot/local", json=body, headers=headers)
    assert response.status_code == 200, response.text
    assert response.json()["evidence"]["payload"]["recommended_quantity"] == 24
    assert (
        client.post("/api/copilot", json={"question": "What are forecasts?"}).json()["mode"]
        == "deterministic-evidence"
    )


def test_http_revoked_during_inference_is_denied(store: PortfolioStore) -> None:
    from fastapi.testclient import TestClient

    from nexus.portfolio.api import create_app
    from nexus.portfolio.auth import COOKIE_NAME

    app = create_app(store.path.parent)
    app.state.portfolio_store = store
    client = TestClient(app)
    session = client.post("/api/session", json={"persona": "planner"}).json()
    provider = Mock()

    def generate(*args: Any, **kwargs: Any) -> LocalReply:
        current = store.session(client.cookies[COOKIE_NAME])
        assert current is not None
        store.revoke_session(current[2])
        return LocalReply(ToolSelection(tool="get_forecast"), {})

    provider.generate.side_effect = generate
    app.state.local_model_provider = provider
    response = client.post(
        "/api/copilot/local",
        json=request(store).model_dump(),
        headers={"X-CSRF-Token": session["csrf_token"]},
    )
    assert response.status_code == 401
