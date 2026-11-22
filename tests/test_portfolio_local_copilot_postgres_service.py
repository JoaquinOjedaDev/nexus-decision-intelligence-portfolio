"""Real isolated PostgreSQL tool reads; constructed evidence, not full G6 acceptance."""

from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import LOCATIONS, actor, event, source_seed

from nexus.portfolio.local_copilot import LocalQuestion, ToolSelection, answer_local, run_tool
from nexus.portfolio.local_provider import LocalReply
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.service_backend import bind_service_root
from nexus.portfolio.store import StoreError

pytestmark = pytest.mark.portfolio_service
NOW = 1_800_000_000.1234567


@pytest.fixture
def runtime(cluster: Cluster, tmp_path: Path) -> PostgresRuntimeStore:
    root = tmp_path / "runtime"
    seed = source_seed(root / "evidence" / "initial")
    for row in seed["recommendations"]:
        row.update(
            supplier_id="SUP-001",
            lead_time_days=3,
            initial_inventory=15,
            risk="high",
            risk_definition="Interval width, not failure probability.",
            reason="Constructed native tool fixture.",
        )
    seed["forecasts"].append(
        {
            "id": "rec-1",
            "sku_id": "SKU-001",
            "location_id": "WH-NORTH",
            "points": [{"date": "2027-01-16", "p50": 3.0}],
            "model": "fixture-seasonal",
            "origin": "2027-01-15T08:00:00Z",
        }
    )
    result = PostgresRuntimeStore(
        cluster.backend(root / "evidence" / "initial").config, runtime_root=root, clock=lambda: NOW
    )
    cluster.migrate(result)
    result.ensure_seed(seed)
    bind_service_root(result, claim=True)
    return result


def request(runtime: PostgresRuntimeStore) -> LocalQuestion:
    row = runtime.recommendation_context("rec-1", actor())
    return LocalQuestion(
        question="Show the forecast",
        recommendation_id="rec-1",
        location_id="WH-NORTH",
        fingerprint=row["fingerprint"],
        generation_sha256=row["generation"]["seed_sha256"],
    )


def test_all_six_tools_read_native_projection_without_business_writes(
    runtime: PostgresRuntimeStore,
) -> None:
    before = runtime.audit(actor())
    body = request(runtime)
    for tool in (
        "get_forecast",
        "get_inventory_risk",
        "explain_recommendation",
        "get_supplier_terms",
        "compare_scenarios",
        "draft_purchase_order",
        "abstain",
    ):
        selected = ToolSelection.model_validate({"tool": tool})
        result = run_tool(selected, body, runtime, actor())
        assert result["tool"] == tool
        assert result["generation_sha256"] == body.generation_sha256
        if tool == "get_forecast":
            assert result["payload"]["points"][0]["p50"] == 3.0
        if tool == "draft_purchase_order":
            assert result["payload"]["recommended_quantity"] == 25
            assert result["payload"]["submitted"] is False
    assert runtime.audit(actor()) == before


def test_forecast_only_scope_required_and_invalidation_refused(
    runtime: PostgresRuntimeStore,
) -> None:
    body = request(runtime)
    with pytest.raises(StoreError):
        run_tool(
            ToolSelection(tool="compare_scenarios"), body, runtime, actor(locations=LOCATIONS[:2])
        )
    runtime.control.submit(event(NOW), actor())
    with pytest.raises(StoreError, match="current, valid"):
        run_tool(ToolSelection(tool="get_forecast"), body, runtime, actor())


def test_material_event_during_inference_fences_native_tool(
    runtime: PostgresRuntimeStore,
) -> None:
    provider = Mock()

    def generate(*args: Any, **kwargs: Any) -> LocalReply:
        runtime.control.submit(event(NOW), actor())
        return LocalReply(ToolSelection(tool="draft_purchase_order"), {})

    provider.generate.side_effect = generate
    with pytest.raises(StoreError, match="current, valid"):
        answer_local(request(runtime), runtime, actor, provider)
    assert not any(row["event_type"] == "executed" for row in runtime.audit(actor()))
