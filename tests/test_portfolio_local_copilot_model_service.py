"""Explicit real-model + authenticated ASGI diagnostic, with synthetic SQLite evidence.

No source pipeline, browser, final G6 quality or production identity claim.
Every observation is saved before assertions, including failed selection cases.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from test_portfolio_local_copilot import actor, request
from test_portfolio_local_copilot import seed as seed
from test_portfolio_local_copilot import store as store

from nexus.portfolio.api import create_app
from nexus.portfolio.local_provider import LocalModelConfig
from nexus.portfolio.store import PortfolioStore, digest

pytestmark = pytest.mark.skipif(
    os.environ.get("NEXUS_LOCAL_COPILOT_ACCEPTANCE") != "1",
    reason="Real local inference diagnostic requires explicit opt-in",
)
ROOT = Path(__file__).resolve().parents[1]


def test_local_model_selects_governed_tools_through_authenticated_api(
    store: PortfolioStore, tmp_path: Path
) -> None:
    corpus_bytes = (ROOT / "fixtures/local_copilot/diagnostic-v1.json").read_bytes()
    corpus = json.loads(corpus_bytes)
    config = LocalModelConfig(
        "http://127.0.0.1:11435",
        "qwen3.5:9b-q4_K_M",
        "6488c96fa5faab64bb65cbd30d4289e20e6130ef535a93ef9a49f42eda893ea7",
    )
    app = create_app(store.path.parent, local_model=config)
    app.state.portfolio_store = store
    client = TestClient(app)
    session = client.post("/api/session", json={"persona": "planner"}).json()
    headers = {"X-CSRF-Token": session["csrf_token"]}
    initial_audit = digest(store.audit(actor()))
    observations = []
    for case in corpus["cases"]:
        started = time.perf_counter()
        response = client.post(
            "/api/copilot/local",
            json=request(store, question=case["question"]).model_dump(),
            headers=headers,
        )
        payload = response.json()
        evidence = payload.get("evidence", {})
        tool = evidence.get("tool")
        citations = evidence.get("citations", [])
        bound = tool == "abstain" or (
            bool(citations)
            and (
                citations[0]["payload_sha256"] == digest(evidence["payload"])
                and citations[0]["source_version"] == request(store).generation_sha256
            )
        )
        unchanged = digest(store.audit(actor())) == initial_audit
        observations.append(
            {
                "id": case["id"],
                "question_sha256": digest(case["question"]),
                "http_status": response.status_code,
                "selected_tool": tool,
                "expected_tool": case["expected_tool"],
                "selection_correct": response.status_code == 200 and tool == case["expected_tool"],
                "structured_evidence_binding_valid": bound,
                "workflow_unchanged": unchanged,
                "elapsed_seconds": time.perf_counter() - started,
                "trace": payload.get("trace"),
                "error": payload.get("error"),
            }
        )
    accuracy = sum(row["selection_correct"] for row in observations) / len(observations)
    violations = sum(not row["workflow_unchanged"] for row in observations)
    (tmp_path / "local-copilot-diagnostic.json").write_text(
        json.dumps(
            {
                "contract": corpus["contract"],
                "scope": corpus["scope"],
                "corpus_sha256": hashlib.sha256(corpus_bytes).hexdigest(),
                "model": config.model,
                "model_manifest_sha256": config.manifest_sha256,
                "selection_accuracy": accuracy,
                "workflow_boundary_violations": violations,
                "observations": observations,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    assert accuracy >= corpus["minimum_selection_accuracy"], observations
    assert violations == corpus["required_boundary_violations"], observations
    assert all(row["structured_evidence_binding_valid"] for row in observations), observations
    assert store.metrics_snapshot()["effect_count"] == 0
