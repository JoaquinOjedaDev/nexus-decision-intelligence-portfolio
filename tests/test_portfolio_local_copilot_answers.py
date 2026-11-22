"""Development checks for optional grounded local answers; no model invocation."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from test_portfolio_local_copilot import actor, request
from test_portfolio_local_copilot import seed as seed
from test_portfolio_local_copilot import store as store

from nexus.portfolio.local_copilot import answer_local
from nexus.portfolio.local_copilot_documents import load_corpus
from nexus.portfolio.local_provider import LocalReply
from nexus.portfolio.store import PortfolioStore, StoreError

ROOT = Path(__file__).resolve().parents[1]
CORPUS = load_corpus(ROOT / "fixtures/local_copilot/g6-documents-v1/documents.json")


def provider_choice(
    tool: str,
    claim_key: str | None = None,
    reference_id: str | None = None,
    *,
    evidence_intent: str = "structured",
) -> Mock:
    provider = Mock()

    def generate(question: str, response_type: Any, *, instruction: str) -> LocalReply:
        assert question
        return LocalReply(
            response_type(
                tool=tool,
                evidence_intent=evidence_intent,
                claim_key=claim_key,
                reference_id=reference_id,
            ),
            {"model": "double"},
        )

    provider.generate.side_effect = generate
    return provider


def test_structured_forecast_becomes_readable_without_model_numbers(store: PortfolioStore) -> None:
    provider = provider_choice("get_forecast")
    result = answer_local(
        request(store, question="Show the forecast"),
        store,
        actor,
        provider,
        corpus=CORPUS,
        as_of=date(2027, 3, 15),
    )
    assert result["mode"] == "local-model-grounded-selection"
    assert result["document_citations"] == []
    assert result["evidence"]["payload"]["points"][0]["p50"] == 3.0
    assert '"p50": 3.0' in result["answer"]
    assert result["trace"]["executed_tool"] == "get_forecast"
    assert "Fictional ACME" not in provider.generate.call_args.kwargs["instruction"]
    assert "not contractual payment terms" not in provider.generate.call_args.kwargs["instruction"]
    assert (
        "Return only the selected tool name"
        not in provider.generate.call_args.kwargs["instruction"]
    )


@pytest.mark.parametrize(
    ("tool", "expected"),
    [
        ("get_inventory_risk", "Governed initial inventory: 15"),
        ("explain_recommendation", "proposes 24 units"),
        ("compare_scenarios", '"fill_rate": 0.91'),
    ],
)
def test_other_governed_tools_render_without_document_claims(
    store: PortfolioStore, tool: str, expected: str
) -> None:
    result = answer_local(
        request(store),
        store,
        actor,
        provider_choice(tool),
        corpus=CORPUS,
        as_of=date(2027, 3, 15),
    )
    assert result["evidence"]["tool"] == tool
    assert expected in result["answer"]
    assert result["document_citations"] == []
    assert result["answer_source"] == "verified_tool_evidence"


def test_supplier_answer_binds_exact_tool_value_and_current_passage(store: PortfolioStore) -> None:
    provider = provider_choice(
        "get_supplier_terms",
        "supplier.acme.payment",
        "supplier-acme-terms@v2#payment",
        evidence_intent="contract",
    )
    before = store.audit(actor())
    result = answer_local(
        request(store, question="What are the ACME supplier terms?"),
        store,
        actor,
        provider,
        corpus=CORPUS,
        as_of=date(2027, 3, 15),
    )
    assert result["evidence"]["tool"] == "get_supplier_terms"
    payload = result["evidence"]["payload"]
    assert f"unit cost {payload['unit_cost']}" in result["answer"]
    assert f"lead time {payload['lead_time_days']} days" in result["answer"]
    assert "supplier.acme.payment: net_45" in result["answer"]
    assert "45 calendar days" not in result["answer"]
    assert result["document_citations"][0]["reference_id"] == "supplier-acme-terms@v2#payment"
    assert result["document_citations"][0]["subject_id"] == payload["supplier_id"]
    assert "45 calendar days" in result["document_citations"][0]["source_excerpt_untrusted"]
    assert "text" not in result["document_citations"][0]
    assert store.audit(actor()) == before
    assert store.metrics_snapshot()["effect_count"] == 0


def test_cited_reference_resolves_its_authorized_claim_key(store: PortfolioStore) -> None:
    result = answer_local(
        request(store, question="Draft this order and cite its approval policy."),
        store,
        actor,
        provider_choice(
            "draft_purchase_order",
            None,
            "purchase-approval-policy@v1#maker-checker",
            evidence_intent="approval_policy",
        ),
        corpus=CORPUS,
        as_of=date(2027, 3, 15),
    )
    assert result["trace"]["executed_tool"] == "draft_purchase_order"
    assert result["trace"]["selected_claim_key"] == "order.approval"
    assert result["document_citations"][0]["reference_id"] == (
        "purchase-approval-policy@v1#maker-checker"
    )


def test_clear_invoice_due_date_recovers_unique_authorized_reference(
    store: PortfolioStore,
) -> None:
    result = answer_local(
        request(
            store,
            question="What does the synthetic contract say about this invoice due date?",
        ),
        store,
        actor,
        provider_choice("abstain"),
        corpus=CORPUS,
        as_of=date(2027, 3, 15),
    )
    assert result["trace"]["selected_tool"] == "abstain"
    assert result["trace"]["executed_tool"] == "get_supplier_terms"
    assert result["document_citations"][0]["reference_id"] == ("supplier-acme-terms@v2#payment")


@pytest.mark.parametrize(
    "question",
    [
        "Show the forecast and this supplier's invoice due date.",
        "What invoice due date and late-payment penalty apply?",
    ],
)
def test_invoice_recovery_does_not_override_multiple_or_unsupported_requests(
    store: PortfolioStore, question: str
) -> None:
    result = answer_local(
        request(store, question=question),
        store,
        actor,
        provider_choice("abstain"),
        corpus=CORPUS,
        as_of=date(2027, 3, 15),
    )
    assert result["trace"]["executed_tool"] == "abstain"
    assert result["document_citations"] == []


@pytest.mark.parametrize(
    ("question", "tool", "claim_key", "reference_id", "intent"),
    [
        (
            "Draft this order but change the quantity to 999 units.",
            "draft_purchase_order",
            None,
            None,
            "structured",
        ),
        (
            "What are the supplier's late-payment penalties?",
            "get_supplier_terms",
            "supplier.acme.payment",
            "supplier-acme-terms@v2#payment",
            "contract",
        ),
        (
            "Place the purchase order with this supplier now.",
            "draft_purchase_order",
            None,
            None,
            "structured",
        ),
    ],
)
def test_unsupported_request_vetoes_even_plausible_model_choice(
    store: PortfolioStore,
    question: str,
    tool: str,
    claim_key: str | None,
    reference_id: str | None,
    intent: str,
) -> None:
    before = store.audit(actor())
    result = answer_local(
        request(store, question=question),
        store,
        actor,
        provider_choice(tool, claim_key, reference_id, evidence_intent=intent),
        corpus=CORPUS,
        as_of=date(2027, 3, 15),
    )
    assert result["trace"]["selected_tool"] == tool
    assert result["trace"]["executed_tool"] == "abstain"
    assert result["document_citations"] == []
    assert store.audit(actor()) == before


def test_structured_supplier_and_draft_work_without_optional_documents(
    store: PortfolioStore,
) -> None:
    for tool, question in (
        ("get_supplier_terms", "What are the planning unit cost and lead time?"),
        ("draft_purchase_order", "Prepare a non-submitted draft."),
    ):
        result = answer_local(
            request(store, question=question),
            store,
            actor,
            provider_choice(tool),
            corpus=CORPUS,
            as_of=date(2026, 12, 31),
        )
        assert result["evidence"]["tool"] == tool
        assert result["document_citations"] == []
        assert result["answer_source"] == "verified_tool_evidence"
        if tool == "get_supplier_terms":
            assert result["evidence"]["payload"]["scope_note"] in result["answer"]
    assert store.metrics_snapshot()["effect_count"] == 0


def test_contractual_request_without_document_cannot_fall_back_to_planning_values(
    store: PortfolioStore,
) -> None:
    result = answer_local(
        request(store, question="What are the supplier payment terms?"),
        store,
        actor,
        provider_choice("get_supplier_terms"),
        corpus=CORPUS,
        as_of=date(2026, 12, 31),
    )
    assert result["evidence"]["tool"] == "abstain"
    assert result["answer_source"] == "no_supported_evidence"
    assert result["document_citations"] == []


def test_conflicting_or_missing_contract_evidence_abstains(store: PortfolioStore) -> None:
    conflict = answer_local(
        request(store, question="What is the payment period?"),
        store,
        actor,
        provider_choice(
            "get_supplier_terms",
            "supplier.acme.payment",
            "supplier-acme-terms@v2#payment",
            evidence_intent="contract",
        ),
        corpus=CORPUS,
        as_of=date(2027, 4, 15),
    )
    missing = answer_local(
        request(store, question="What is the payment period?"),
        store,
        actor,
        provider_choice("get_supplier_terms", evidence_intent="contract"),
        corpus=CORPUS,
        as_of=date(2026, 12, 31),
    )
    for result in (conflict, missing):
        assert result["evidence"]["tool"] == "abstain"
        assert result["document_citations"] == []
        assert "net_45" not in result["answer"]
        assert result["trace"]["selected_tool"] == "get_supplier_terms"


def test_wrong_claim_tool_pair_and_reference_fail_closed(store: PortfolioStore) -> None:
    wrong_pair = answer_local(
        request(store, question="What are supplier terms?"),
        store,
        actor,
        provider_choice(
            "get_supplier_terms",
            "order.approval",
            "purchase-approval-policy@v1#maker-checker",
            evidence_intent="contract",
        ),
        corpus=CORPUS,
        as_of=date(2027, 3, 15),
    )
    assert wrong_pair["evidence"]["tool"] == "abstain"
    forged = Mock()
    forged.generate.return_value = LocalReply(
        Mock(
            model_dump=Mock(
                return_value={
                    "tool": "get_supplier_terms",
                    "evidence_intent": "contract",
                    "claim_key": "supplier.acme.payment",
                    "reference_id": "supplier-acme-addendum@v1#payment",
                }
            )
        ),
        {},
    )
    with pytest.raises(StoreError) as caught:
        answer_local(
            request(store),
            store,
            actor,
            forged,
            corpus=CORPUS,
            as_of=date(2027, 3, 15),
        )
    assert caught.value.code == "local_provider_unavailable"


def test_draft_remains_role_bound_and_read_only(store: PortfolioStore) -> None:
    provider = provider_choice(
        "draft_purchase_order",
        "order.approval",
        "purchase-approval-policy@v1#maker-checker",
        evidence_intent="approval_policy",
    )
    result = answer_local(
        request(store, question="Prepare a draft"),
        store,
        actor,
        provider,
        corpus=CORPUS,
        as_of=date(2027, 3, 15),
    )
    assert result["evidence"]["payload"]["submitted"] is False
    assert "Independent human approval is required" in result["answer"]
    assert store.metrics_snapshot()["effect_count"] == 0
    with pytest.raises(StoreError, match="Only a planner"):
        answer_local(
            request(store),
            store,
            lambda: actor("approver"),
            provider,
            corpus=CORPUS,
            as_of=date(2027, 3, 15),
        )


def test_malicious_document_prose_never_enters_model_instruction(store: PortfolioStore) -> None:
    provider = provider_choice(
        "get_supplier_terms",
        "supplier.acme.delivery_window",
        "supplier-acme-note@v1#delivery",
        evidence_intent="contract",
    )
    result = answer_local(
        request(store, question="What is the delivery window?"),
        store,
        actor,
        provider,
        corpus=CORPUS,
        as_of=date(2027, 3, 15),
    )
    assert result["evidence"]["tool"] == "get_supplier_terms"
    assert "supplier.acme.delivery_window: five_days" in result["answer"]
    assert "MALICIOUS DOCUMENT TEXT" not in result["answer"]
    assert "MALICIOUS DOCUMENT TEXT" in result["document_citations"][0]["source_excerpt_untrusted"]
    assert "MALICIOUS DOCUMENT TEXT" not in provider.generate.call_args.kwargs["instruction"]
    assert store.metrics_snapshot()["effect_count"] == 0


def test_post_inference_principal_and_generation_revalidation(store: PortfolioStore) -> None:
    provider = provider_choice("get_forecast")
    changed = 0

    def resolve_changed() -> Any:
        nonlocal changed
        changed += 1
        return actor() if changed == 1 else replace(actor(), actor_id="different")

    with pytest.raises(StoreError) as caught:
        answer_local(
            request(store),
            store,
            resolve_changed,
            provider,
            corpus=CORPUS,
            as_of=date(2027, 3, 15),
        )
    assert caught.value.code == "principal_changed"

    def expire(*args: Any, **kwargs: Any) -> LocalReply:
        store.clock = lambda: 1_800_086_400.0
        return LocalReply(
            args[1](
                tool="get_forecast",
                evidence_intent="structured",
                claim_key=None,
                reference_id=None,
            ),
            {},
        )

    provider.generate.side_effect = expire
    with pytest.raises(StoreError) as stale:
        answer_local(
            request(store),
            store,
            actor,
            provider,
            corpus=CORPUS,
            as_of=date(2027, 3, 15),
        )
    assert stale.value.code == "stale_evidence"
