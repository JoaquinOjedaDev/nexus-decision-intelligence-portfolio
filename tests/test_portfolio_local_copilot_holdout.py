"""Frozen real-model evaluation over an isolated prepared synthetic portfolio demo."""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from nexus.portfolio.api import create_app
from nexus.portfolio.auth import resolve_persona
from nexus.portfolio.cli import _demo_local_copilot
from nexus.portfolio.local_copilot_answers import render_verified_answer
from nexus.portfolio.local_copilot_documents import (
    PassageReference,
    passage_digest,
    retrieve_claim,
)
from nexus.portfolio.store import PortfolioStore, digest

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "fixtures/local_copilot/portfolio-holdout-v4.json"


def test_demo_documents_match_current_demo_identities() -> None:
    _, corpus = _demo_local_copilot()
    today = datetime.now(UTC).date()
    for supplier, location in (
        ("SUP-0001", "LOC-001"),
        ("SUP-0002", "LOC-002"),
        ("SUP-001", "DC-ARG-01"),
        ("SUP-002", "DC-ARG-02"),
    ):
        actor = resolve_persona("planner", (location,))
        key = f"supplier.{supplier.lower().replace('-', '_')}.payment"
        evidence = retrieve_claim(
            corpus,
            actor=actor,
            location_id=location,
            claim_key=key,
            subject_id=supplier,
            as_of=today,
        )
        assert evidence.status == "found"
        assert len(evidence.references) == 1


@pytest.mark.skipif(
    os.environ.get("NEXUS_LOCAL_COPILOT_HOLDOUT") != "1",
    reason="Real installed-model holdout requires explicit local opt-in",
)
def test_installed_model_on_frozen_portfolio_holdout() -> None:
    state_dir = Path(os.environ["NEXUS_PORTFOLIO_AI_STATE"])
    receipt_path = Path(os.environ["NEXUS_PORTFOLIO_AI_RECEIPT"])
    fixture_bytes = FIXTURE.read_bytes()
    fixture = json.loads(fixture_bytes)
    model, corpus = _demo_local_copilot()
    store = PortfolioStore(state_dir)
    app = create_app(state_dir, ROOT, local_model=model, local_documents=corpus)
    app.state.portfolio_store = store
    actor = resolve_persona("planner", store.locations())
    observations: list[dict[str, Any]] = []
    with TestClient(app) as client:
        session = client.post("/api/session", json={"persona": "planner"})
        assert session.status_code == 200
        assert session.json()["local_copilot_enabled"] is True
        headers = {"X-CSRF-Token": session.json()["csrf_token"]}
        initial_audit = digest(store.audit(actor))
        recommendations = client.get("/api/recommendations").json()["items"]
        for case in fixture["cases"]:
            supplier = case.get("supplier_id", "SUP-0001")
            item = next(
                row
                for row in recommendations
                if row["supplier_id"] == supplier and row["location_id"] == "LOC-001"
            )
            detail = client.get(f"/api/recommendations/{item['id']}").json()
            started = time.perf_counter()
            response = client.post(
                "/api/copilot/local",
                json={
                    "question": case["question"],
                    "recommendation_id": item["id"],
                    "location_id": item["location_id"],
                    "generation_sha256": detail["generation"]["seed_sha256"],
                    "fingerprint": detail["fingerprint"],
                },
                headers=headers,
            )
            body = response.json()
            evidence = body.get("evidence", {})
            tool = evidence.get("tool")
            raw_tool = body.get("trace", {}).get("selected_tool")
            documents = body.get("document_citations", [])
            expected_reference = case.get("expected_reference")
            citation_valid = all(
                citation.get("reference_id") == expected_reference
                and citation.get("subject_id") in (None, supplier)
                and citation.get("corpus_sha256") == corpus.corpus_sha256
                and citation.get("sha256")
                == passage_digest(
                    citation["claim_key"],
                    citation["claim_value"],
                    citation["source_excerpt_untrusted"],
                )
                for citation in documents
            )
            rendered = False
            if response.status_code == 200:
                reference = (
                    PassageReference.model_validate(
                        {
                            **{
                                key: value
                                for key, value in documents[0].items()
                                if key != "source_excerpt_untrusted"
                            },
                            "text": documents[0]["source_excerpt_untrusted"],
                        }
                    )
                    if documents
                    else None
                )
                rendered = body.get("answer") == render_verified_answer(evidence, reference)
            data_citations = evidence.get("citations", [])
            data_bound = tool == "abstain" or (
                len(data_citations) == 1
                and data_citations[0].get("payload_sha256") == digest(evidence.get("payload"))
                and data_citations[0].get("source_version") == detail["generation"]["seed_sha256"]
            )
            observations.append(
                {
                    "id": case["id"],
                    "question_sha256": digest(case["question"]),
                    "http_status": response.status_code,
                    "selected_tool": tool,
                    "raw_selected_tool": raw_tool,
                    "expected_tool": case["expected_tool"],
                    "model_selection_correct": response.status_code == 200
                    and raw_tool == case["expected_tool"],
                    "tool_response_correct": response.status_code == 200
                    and tool == case["expected_tool"],
                    "expected_reference": expected_reference,
                    "returned_references": [c["reference_id"] for c in documents],
                    "citation_valid": citation_valid,
                    "rendered_from_evidence": rendered,
                    "structured_data_bound": data_bound,
                    "workflow_unchanged": digest(store.audit(actor)) == initial_audit
                    and store.metrics_snapshot()["effect_count"] == 0,
                    "elapsed_seconds": time.perf_counter() - started,
                    "trace": body.get("trace"),
                    "error": body.get("error"),
                }
            )
    correct = sum(row["model_selection_correct"] for row in observations)
    response_correct = sum(row["tool_response_correct"] for row in observations)
    returned = sum(len(row["returned_references"]) for row in observations)
    relevant = sum(
        row["returned_references"] == [row["expected_reference"]] and row["citation_valid"]
        for row in observations
        if row["expected_reference"] is not None
    )
    expected = sum(row["expected_reference"] is not None for row in observations)
    violations = sum(not row["workflow_unchanged"] for row in observations)
    report = {
        "contract": fixture["contract"],
        "evaluated_at": datetime.now(UTC).isoformat(),
        "fixture_sha256": hashlib.sha256(fixture_bytes).hexdigest(),
        "corpus_sha256": corpus.corpus_sha256,
        "model": model.model,
        "manifest_sha256": model.manifest_sha256,
        "model_selection_accuracy": correct / len(observations),
        "tool_response_accuracy": response_correct / len(observations),
        "document_citation_precision": relevant / returned if returned else 0,
        "document_citation_recall": relevant / expected,
        "workflow_boundary_violations": violations,
        "observations": observations,
    }
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    assert report["model_selection_accuracy"] >= fixture["minimum_selection_accuracy"], observations
    assert (
        report["document_citation_precision"] >= fixture["minimum_document_citation_precision"]
    ), observations
    assert report["document_citation_recall"] >= fixture["minimum_document_citation_recall"], (
        observations
    )
    assert violations == fixture["required_workflow_boundary_violations"], observations
    assert all(row["rendered_from_evidence"] for row in observations), observations
    assert all(row["structured_data_bound"] for row in observations), observations
