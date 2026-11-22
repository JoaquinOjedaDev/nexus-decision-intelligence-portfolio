"""Explicit, session-bound recovery review; never a broker publisher endpoint."""

from __future__ import annotations

import json
from typing import Any, Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field

from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger
from nexus.portfolio.store import StoreError
from nexus.portfolio.transport_ledger import TransportBinding

router = APIRouter(prefix="/api/runtime/recovery")


class ResolutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    request_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    decision_id: str = Field(pattern=r"^[a-f0-9-]{36}$")


class ReviewRequest(ResolutionRequest):
    action: Literal["approve", "reject", "revoke"]
    reason: str = Field(min_length=1, max_length=500)
    ttl_seconds: int = Field(default=900, ge=1, le=900)


class ProposalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    partition: int = Field(ge=0)
    inbox_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    publication_id: str = Field(pattern=r"^[a-f0-9-]{36}$")
    reason: str = Field(min_length=1, max_length=500)


def _recovery(request: Request) -> tuple[RecoveryResolutionLedger, TransportBinding]:
    from nexus.portfolio.runtime_routes import _runtime_store

    store = _runtime_store(request)
    if (
        not isinstance(store, PublicationRuntimeStore)
        or not isinstance(store.transport, RecoveryResolutionLedger)
        or store.transport.publication is not store
    ):
        raise StoreError("recovery_disabled", "Explicit bound recovery runtime required", 404)
    return store.transport, store.transport_binding


@router.get("/pending/{partition}")
def pending(partition: int, request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import require_actor

    actor = require_actor(request)
    ledger, binding = _recovery(request)
    evidence = ledger.pending_dead_letter(binding, partition, actor)
    if evidence is None:
        return {"pending": None, "claim": "inspection_not_recovery_authorization"}
    value = json.loads(evidence.payload)
    # Explicit projection: never disclose keys, raw values, headers or decoded input.
    summary = {
        key: value[key]
        for key in (
            "dead_letter_id",
            "binding_sha256",
            "inbox_record_hash",
            "original_value_sha256",
            "classification",
        )
    }
    summary.update(partition=value["physical"]["partition"], offset=value["physical"]["offset"])
    return {"pending": summary, "claim": "inspection_not_recovery_authorization"}


@router.post("/proposals")
def propose(body: ProposalRequest, request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import authorize_mutation

    actor = authorize_mutation(request)
    ledger, binding = _recovery(request)
    return ledger.propose_publication(
        binding, body.partition, body.inbox_sha256, body.publication_id, body.reason, actor
    )


@router.get("/{request_id}/review")
def status(request_id: str, request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import require_actor

    actor = require_actor(request)
    ledger, binding = _recovery(request)
    return ledger.review_status(binding, request_id, actor)


@router.post("/{request_id}/review")
def review(request_id: str, body: ReviewRequest, request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import authorize_mutation

    actor = authorize_mutation(request)
    ledger, binding = _recovery(request)
    return ledger.review_request(
        binding,
        request_id,
        body.request_sha256,
        actor,
        decision_id=body.decision_id,
        action=body.action,
        reason=body.reason,
        ttl_seconds=body.ttl_seconds,
    )


@router.post("/{request_id}/resolve")
def resolve(request_id: str, body: ResolutionRequest, request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import authorize_mutation

    actor = authorize_mutation(request)
    ledger, binding = _recovery(request)
    return ledger.resolve(binding, request_id, body.request_sha256, body.decision_id, actor)


@router.post("/{request_id}/evidence")
def enqueue_evidence(request_id: str, body: ResolutionRequest, request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import authorize_mutation

    actor = authorize_mutation(request)
    ledger, binding = _recovery(request)
    intents = ledger.enqueue_evidence(
        binding, request_id, body.request_sha256, body.decision_id, actor
    )
    return {"intent_ids": intents, "claim": "retained_review_evidence_not_delivery_authorization"}
