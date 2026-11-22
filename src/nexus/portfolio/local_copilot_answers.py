"""Deterministic readable answers from verified tool and document evidence."""

from __future__ import annotations

import json
from typing import Any

from nexus.portfolio.local_copilot_documents import PassageReference


def render_verified_answer(
    tool_evidence: dict[str, Any], document_reference: PassageReference | None = None
) -> str:
    """Render only fields already checked by the governed tool/document layers."""
    tool = tool_evidence["tool"]
    payload = tool_evidence["payload"]
    if tool == "get_forecast":
        answer = (
            f"Governed forecast for {payload['sku_id']} at {payload['location_id']}: "
            f"model {payload['model']}, origin {payload['origin']}, "
            f"points {json.dumps(payload['points'], sort_keys=True)}."
        )
    elif tool == "get_inventory_risk":
        answer = (
            f"Governed initial inventory: {payload['initial_inventory']}. "
            f"Recorded risk: {payload['risk']}. Definition: {payload['risk_definition']}"
        )
    elif tool == "explain_recommendation":
        answer = (
            f"The current recommendation proposes {payload['recommended_quantity']} units "
            f"with expected cost {payload['expected_cost']}. Recorded reason: {payload['reason']}"
        )
    elif tool == "get_supplier_terms":
        answer = (
            f"Governed planning inputs for supplier {payload['supplier_id']}: "
            f"unit cost {payload['unit_cost']}, lead time {payload['lead_time_days']} days."
        )
        if document_reference is None:
            answer += f" {payload['scope_note']}"
    elif tool == "compare_scenarios":
        answer = (
            "Governed simulated scenarios (not realized savings): "
            f"{json.dumps(payload['scenarios'], sort_keys=True)}."
        )
    elif tool == "draft_purchase_order":
        answer = (
            f"Non-submitted draft for {payload['sku_id']} at {payload['location_id']}: "
            f"supplier {payload['supplier_id']}, {payload['recommended_quantity']} units, "
            f"expected cost {payload['expected_cost']}. Independent human approval is "
            f"required; review {payload['review_url']}."
        )
    else:
        return "No supported, authorized evidence is available for this request."
    if document_reference is not None:
        answer += (
            f" Fictional document claim {document_reference.claim_key}: "
            f"{document_reference.claim_value} [{document_reference.reference_id}]."
        )
    return answer
