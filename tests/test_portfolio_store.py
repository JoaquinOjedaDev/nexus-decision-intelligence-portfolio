"""Persistence, authorization, stale approval, and exactly-once mock effects."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor, SessionCodec, resolve_persona
from nexus.portfolio.store import (
    APPROVAL_TTL_SECONDS,
    DEMO_MATURITY_SECONDS,
    RECOMMENDATION_TTL_SECONDS,
    PortfolioStore,
    StoreError,
)


@pytest.fixture
def clock() -> list[float]:
    return [1_800_000_000.0]


@pytest.fixture
def seed() -> dict[str, Any]:
    return {
        "metadata": {"synthetic": True},
        "recommendations": [
            {
                "id": f"rec-{index}",
                "sku": "SKU-001",
                "sku_id": "SKU-001",
                "location_id": location,
                "recommended_quantity": 24,
                "unit_cost": 3.5,
                "expected_cost": 84.0,
                "created_by": "planner.r4a",
                "observed_demand": 20,
                "outcome_provenance": "synthetic_scenario_replay",
                "outcome_scenario_seed": 4201,
                "scientific_decision_time": "2025-01-01T00:00:00+00:00",
            }
            for index, location in enumerate(("WH-NORTH", "WH-SOUTH"), 1)
        ],
        "scenarios": [],
        "forecasts": [],
        "metrics": {},
        "sources": [],
        "lineage": [],
        "artifacts": [],
    }


@pytest.fixture
def store(tmp_path: Path, clock: list[float], seed: dict[str, Any]) -> PortfolioStore:
    result = PortfolioStore(tmp_path, clock=lambda: clock[0])
    result.ensure_seed(seed)
    return result


def actor(role: str = "approver") -> Actor:
    return resolve_persona(role, ("WH-NORTH", "WH-SOUTH"))


def fingerprint(store: PortfolioStore, recommendation_id: str = "rec-1") -> str:
    return str(store.recommendation(recommendation_id, actor())["fingerprint"])


def approved(store: PortfolioStore, recommendation_id: str = "rec-1") -> str:
    binding = fingerprint(store, recommendation_id)
    store.decide(recommendation_id, actor(), "approve", binding)
    return binding


def test_workflow_persists_and_only_records_mature_simulated_outcome(
    store: PortfolioStore, tmp_path: Path, clock: list[float], seed: dict[str, Any]
) -> None:
    binding = approved(store)
    result = store.execute("rec-1", actor(), binding, "request-1")
    assert result["state"] == "executed"
    assert result["execution_ref"].startswith("MOCK-PO-")
    with pytest.raises(StoreError, match="observation window"):
        store.observe("rec-1", actor("planner"), binding)
    restart = PortfolioStore(tmp_path, clock=lambda: clock[0])
    restart.ensure_seed(seed)
    assert restart.recommendation("rec-1", actor())["execution_ref"] == result["execution_ref"]
    assert len(restart.audit(actor())) == 5
    clock[0] += DEMO_MATURITY_SECONDS
    observed = restart.observe("rec-1", actor("planner"), binding)
    assert observed["state"] == "outcome_observed"
    assert observed["outcome"]["observed_demand"] == 20
    assert observed["outcome"]["simulated"] is True
    assert observed["outcome"]["causal_business_impact_claim"] is False
    assert restart.observe("rec-1", actor("planner"), binding) == observed
    assert restart.metrics_snapshot()["effect_count"] == 1


def test_concurrent_execution_from_independent_connections_creates_one_effect(
    store: PortfolioStore, tmp_path: Path, clock: list[float]
) -> None:
    binding = approved(store)

    def execute(index: int) -> str:
        independent = PortfolioStore(tmp_path, clock=lambda: clock[0])
        result = independent.execute("rec-1", actor(), binding, f"click-{index}")
        return str(result["execution_ref"])

    with ThreadPoolExecutor(max_workers=8) as pool:
        references = list(pool.map(execute, range(24)))
    assert len(set(references)) == 1
    assert store.metrics_snapshot()["effect_count"] == 1
    assert store.metrics_snapshot()["outbox"] == {"delivered": 1}
    assert [event["event_type"] for event in store.audit(actor())].count("executed") == 1
    assert [event["event_type"] for event in store.audit(actor())].count("execution_requested") == 1


def test_failed_execution_retries_after_restart_without_duplicate_effect(
    store: PortfolioStore, tmp_path: Path, clock: list[float]
) -> None:
    binding = approved(store)
    failed = store.execute("rec-1", actor(), binding, "stable-key", simulate_failure=True)
    assert failed["state"] == "execution_failed"
    assert store.metrics_snapshot()["effect_count"] == 0
    restarted = PortfolioStore(tmp_path, clock=lambda: clock[0])
    result = restarted.execute("rec-1", actor(), binding, "stable-key")
    replay = restarted.execute("rec-1", actor(), binding, "second-click")
    assert result["execution_ref"] == replay["execution_ref"]
    assert restarted.metrics_snapshot()["retry_count"] == 1
    assert restarted.metrics_snapshot()["effect_count"] == 1


def test_transaction_rolls_back_effect_if_audit_write_fails(
    store: PortfolioStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    binding = approved(store)
    original = store._event

    def fail_event(*args: Any, **kwargs: Any) -> None:
        if args[2] == "executed":
            raise RuntimeError("injected disk failure")
        original(*args, **kwargs)

    monkeypatch.setattr(store, "_event", fail_event)
    with pytest.raises(RuntimeError, match="disk failure"):
        store.execute("rec-1", actor(), binding, "rollback")
    assert store.metrics_snapshot()["effect_count"] == 0
    assert store.metrics_snapshot()["outbox"] == {}
    assert store.recommendation("rec-1", actor())["state"] == "approved"


@pytest.mark.parametrize("action", ["approve", "reject", "revoke", "execute"])
@pytest.mark.parametrize("role", ["planner", "auditor"])
def test_roles_cannot_escalate_actions(store: PortfolioStore, action: str, role: str) -> None:
    binding = fingerprint(store)
    with pytest.raises(StoreError) as caught:
        if action == "execute":
            store.execute("rec-1", actor(role), binding, "forbidden")
        else:
            store.decide("rec-1", actor(role), action, binding)
    assert caught.value.status == 403
    assert store.metrics_snapshot()["effect_count"] == 0


def test_scope_restricts_lists_detail_audit_and_mutation(store: PortfolioStore) -> None:
    scoped = replace(actor(), locations=("WH-NORTH",))
    assert [item["id"] for item in store.recommendations(scoped)] == ["rec-1"]
    assert {item["recommendation_id"] for item in store.audit(scoped)} == {"rec-1"}
    with pytest.raises(StoreError, match="outside your assigned scope"):
        store.recommendation("rec-2", scoped)
    with pytest.raises(StoreError, match="outside your assigned scope"):
        store.decide("rec-2", scoped, "approve", fingerprint(store, "rec-2"))


def test_maker_cannot_approve_even_with_approver_role(store: PortfolioStore) -> None:
    maker = Actor("planner.r4a", "Maker", ActorRole.APPROVER, ("WH-NORTH",))
    with pytest.raises(StoreError) as caught:
        store.decide("rec-1", maker, "approve", fingerprint(store))
    assert caught.value.code == "maker_checker"


def test_rejected_and_revoked_recommendations_cannot_execute(store: PortfolioStore) -> None:
    first = fingerprint(store)
    store.decide("rec-1", actor(), "reject", first)
    second = approved(store, "rec-2")
    store.execute("rec-2", actor(), second, "failure", simulate_failure=True)
    store.decide("rec-2", actor(), "revoke", second)
    for recommendation_id, binding in (("rec-1", first), ("rec-2", second)):
        with pytest.raises(StoreError, match="requires a current human approval"):
            store.execute(recommendation_id, actor(), binding, recommendation_id)
    assert store.metrics_snapshot()["effect_count"] == 0
    assert store.metrics_snapshot()["outbox"] == {"cancelled": 1}


def test_fingerprint_and_expiry_cannot_be_bypassed(
    store: PortfolioStore, clock: list[float]
) -> None:
    with pytest.raises(StoreError) as mismatch:
        store.decide("rec-1", actor(), "approve", "f" * 64)
    assert mismatch.value.code == "stale_fingerprint"
    binding = approved(store)
    clock[0] += APPROVAL_TTL_SECONDS
    with pytest.raises(StoreError) as expired:
        store.execute("rec-1", actor(), binding, "expired")
    assert expired.value.code == "approval_expired"
    second = fingerprint(store, "rec-2")
    clock[0] += RECOMMENDATION_TTL_SECONDS
    with pytest.raises(StoreError) as recommendation_expired:
        store.decide("rec-2", actor(), "approve", second)
    assert recommendation_expired.value.code == "recommendation_expired"


def test_same_approver_can_renew_expired_approval_before_recommendation_expiry(
    store: PortfolioStore, clock: list[float]
) -> None:
    binding = approved(store)
    original_expiry = store.recommendation("rec-1", actor())["approval_expires_at"]
    store.decide("rec-1", actor(), "approve", binding)
    assert [event["event_type"] for event in store.audit(actor())].count("approved") == 1

    clock[0] += APPROVAL_TTL_SECONDS
    renewed = store.decide("rec-1", actor(), "approve", binding)
    assert renewed["approval_expires_at"] > original_expiry
    assert [event["event_type"] for event in store.audit(actor())].count("approved") == 2
    assert store.execute("rec-1", actor(), binding, "renewed-approval")["state"] == "executed"


def test_idempotency_key_cannot_move_between_recommendations(store: PortfolioStore) -> None:
    first, second = approved(store), approved(store, "rec-2")
    store.execute("rec-1", actor(), first, "one-key")
    with pytest.raises(StoreError) as caught:
        store.execute("rec-2", actor(), second, "one-key")
    assert caught.value.code == "idempotency_conflict"


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE recommendations SET payload='{}' WHERE id='rec-1'",
        "UPDATE recommendations SET state='approved' WHERE id='rec-1'",
        "UPDATE events SET actor_id='forged' WHERE sequence=1",
        "DELETE FROM events WHERE sequence=(SELECT MAX(sequence) FROM events)",
    ],
)
def test_modified_or_truncated_state_fails_closed(store: PortfolioStore, sql: str) -> None:
    binding = fingerprint(store)
    with store.transaction() as db:
        db.execute(sql)
    with pytest.raises(StoreError) as caught:
        store.decide("rec-1", actor(), "approve", binding)
    assert caught.value.code == "integrity_error"
    with pytest.raises(StoreError):
        store.metrics_snapshot()
    with store.connection() as db:
        assert db.execute("SELECT COUNT(*) FROM effects").fetchone()[0] == 0


def test_reverted_revocation_cannot_resurrect_approval(store: PortfolioStore) -> None:
    binding = approved(store)
    store.decide("rec-1", actor(), "revoke", binding)
    with store.transaction() as db:
        db.execute("UPDATE recommendations SET state='approved' WHERE id='rec-1'")
    with pytest.raises(StoreError, match="audit history"):
        store.execute("rec-1", actor(), binding, "resurrection")


def test_modified_effect_is_not_returned_as_success(store: PortfolioStore) -> None:
    binding = approved(store)
    store.execute("rec-1", actor(), binding, "first")
    with store.transaction() as db:
        db.execute("UPDATE effects SET payload='{}'")
    with pytest.raises(StoreError, match="mock effect has changed"):
        store.execute("rec-1", actor(), binding, "second")


def test_signed_session_restart_revocation_and_expiry(
    store: PortfolioStore, tmp_path: Path, clock: list[float]
) -> None:
    pytest.importorskip("jwt")
    token, public = store.create_session("approver")
    assert public["user"]["role"] == "approver"
    restarted = PortfolioStore(tmp_path, clock=lambda: clock[0])
    session = restarted.session(token)
    assert session is not None
    assert restarted.session(token[:-8] + "tampered") is None
    restarted.revoke_session(session[2])
    assert store.session(token) is None
    second, _ = store.create_session("planner")
    clock[0] += 3600
    assert store.session(second) is None


@pytest.mark.parametrize("token", ["", "a.b.c", "a.!!!", "x" * 3000, "eyJ9.foo"])
def test_malformed_signed_tokens_are_rejected(token: str) -> None:
    pytest.importorskip("jwt")
    assert SessionCodec(b"a" * 32).verify(token, now=100) is None


def test_seed_scientific_time_is_separate_and_runtime_expiry_does_not_refresh(
    store: PortfolioStore, seed: dict[str, Any], clock: list[float]
) -> None:
    before = store.recommendation("rec-1", actor())
    clock[0] += 100
    store.ensure_seed(seed)
    after = store.recommendation("rec-1", actor())
    assert before["created_at"] == after["created_at"]
    assert before["valid_until"] == after["valid_until"]
    assert after["scientific_decision_time"] == "2025-01-01T00:00:00+00:00"


def test_reinitialization_cannot_mix_new_evidence_with_existing_workflow(
    store: PortfolioStore, seed: dict[str, Any]
) -> None:
    binding = approved(store)
    changed = {**seed, "metadata": {"synthetic": True, "new_revision": 2}}
    with pytest.raises(StoreError) as error:
        store.ensure_seed(changed)
    assert error.value.code == "seed_changed"
    assert store.recommendation("rec-1", actor())["state"] == "approved"
    assert fingerprint(store) == binding


def test_outcome_deadline_tamper_is_detected_before_observation(store: PortfolioStore) -> None:
    binding = approved(store)
    store.execute("rec-1", actor(), binding, "maturity-bound")
    with store.transaction() as db:
        db.execute("UPDATE recommendations SET outcome_due_at=0 WHERE id='rec-1'")
    with pytest.raises(StoreError) as error:
        store.observe("rec-1", actor("planner"), binding)
    assert error.value.code == "integrity_error"
