"""Real PostgreSQL initial-generation workflow, security and rollback evidence.

The shared fixture validates the exact task-owned cluster before any mutation.
Each test retains its own random schema. No test starts services or contacts the
default PostgreSQL port. Constructed workflow fixtures are not forecasting proof.
"""

from __future__ import annotations

import copy
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster

from nexus.contracts import ActorRole
from nexus.portfolio.auth import SESSION_TTL_SECONDS, Actor, resolve_persona
from nexus.portfolio.postgres_backend import PostgresBackend, PostgresBackendError
from nexus.portfolio.postgres_workflow import PostgresWorkflowStore
from nexus.portfolio.store import (
    APPROVAL_TTL_SECONDS,
    DEMO_MATURITY_SECONDS,
    RECOMMENDATION_TTL_SECONDS,
    PortfolioStore,
    StoreError,
    digest,
    iso,
)

pytestmark = pytest.mark.portfolio_service
LOCATIONS = ("WH-NORTH", "WH-SOUTH")


def actor(role: str = "approver") -> Actor:
    if role == "system":
        return Actor("worker.test", "Service worker", ActorRole.SYSTEM, LOCATIONS)
    return resolve_persona(role, LOCATIONS)


@pytest.fixture
def clock() -> list[float]:
    return [1_800_000_000.1234567]


@pytest.fixture
def seed() -> dict[str, Any]:
    return {
        "metadata": {"synthetic": True, "global_budget": "GLOBAL_BUDGET_SECRET"},
        "recommendations": [
            {
                "id": f"rec-{index}",
                "sku_id": f"SKU-{index:03}",
                "location_id": location,
                "recommended_quantity": 24 + index,
                "unit_cost": 3.5,
                "expected_cost": (24 + index) * 3.5,
                "created_by": "planner.r4a",
                "observed_demand": 20,
                "outcome_provenance": "synthetic_scenario_replay",
                "outcome_scenario_seed": 4201,
                "scientific_decision_time": "2025-01-01T00:00:00+00:00",
                "portfolio_constraints": {"scope": "ALL_SITES_SECRET", "budget": 983741.23},
            }
            for index, location in enumerate(LOCATIONS, 1)
        ],
        "scenarios": [{"name": "SOUTH_SCENARIO_SECRET", "location_id": "WH-SOUTH"}],
        "forecasts": [{"sku_id": "SOUTH_FORECAST_SECRET", "location_id": "WH-SOUTH"}],
        "metrics": {"global_budget": "GLOBAL_METRIC_SECRET"},
        "sources": [{"name": "GLOBAL_SOURCE_SECRET"}],
        "lineage": [{"name": "GLOBAL_LINEAGE_SECRET"}],
        "artifacts": [],
    }


@pytest.fixture
def workflow(
    cluster: Cluster, tmp_path: Path, clock: list[float], seed: dict[str, Any]
) -> PostgresWorkflowStore:
    config = cluster.backend(tmp_path / "artifacts").config
    store = PostgresWorkflowStore(config, clock=lambda: clock[0])
    store.migrate(migration_dsn=cluster.config["migration_dsn"])
    store.ensure_seed(seed)
    return store


def fingerprint(store: PostgresWorkflowStore, recommendation_id: str = "rec-1") -> str:
    return store.recommendation(recommendation_id, actor())["fingerprint"]


def approved(store: PostgresWorkflowStore, recommendation_id: str = "rec-1") -> str:
    binding = fingerprint(store, recommendation_id)
    store.decide(recommendation_id, actor(), "approve", binding)
    return binding


def metrics(store: PostgresWorkflowStore) -> dict[str, Any]:
    return store.metrics_snapshot(actor("auditor"))


def assert_chain(store: PostgresWorkflowStore) -> None:
    previous = "0" * 64
    raw = store.snapshot()["events"]
    public = store.audit(actor("auditor"))
    assert [event["event_hash"] for event in public] == [event["event_hash"] for event in raw]
    for sequence, event in enumerate(raw, 1):
        material = copy.deepcopy(event)
        expected = material.pop("event_hash")
        material["details"] = json.loads(material["details"])
        assert material["sequence"] == sequence and material["previous_hash"] == previous
        assert digest(material) == expected
        assert public[sequence - 1]["occurred_at"] == iso(material["occurred_at"])
        previous = expected


def test_upgrade_from_002_preserves_original_seed_and_immutable_migration_row(
    cluster: Cluster, tmp_path: Path, clock: list[float], seed: dict[str, Any]
) -> None:
    foundation = cluster.backend(tmp_path / "upgrade-artifacts")
    cluster.migrate(foundation)
    foundation.ensure_seed(seed)
    original = foundation.snapshot()
    old_ledger = cluster.query(
        foundation, "SELECT * FROM {schema}.schema_migrations WHERE version=2"
    )
    store = PostgresWorkflowStore(foundation.config, clock=lambda: clock[0])
    store.migrate(migration_dsn=cluster.config["migration_dsn"])
    assert (
        cluster.query(store, "SELECT * FROM {schema}.schema_migrations WHERE version=2")
        == old_ledger
    )
    assert cluster.query(
        store, "SELECT version FROM {schema}.schema_migrations ORDER BY version"
    ) == [(2,), (3,)]
    assert store.seed() == seed
    assert store.audit(actor())[0]["event_hash"] == original["events"][0]["event_hash"]
    ledger = cluster.query(store, "SELECT * FROM {schema}.schema_migrations ORDER BY version")
    store.migrate(migration_dsn=cluster.config["migration_dsn"])
    assert (
        cluster.query(store, "SELECT * FROM {schema}.schema_migrations ORDER BY version") == ledger
    )
    with pytest.raises(PostgresBackendError):
        PostgresBackend(foundation.config).snapshot()


def test_full_workflow_matches_sqlite_payloads_events_and_business_metrics(
    workflow: PostgresWorkflowStore, tmp_path: Path, clock: list[float], seed: dict[str, Any]
) -> None:
    sqlite = PortfolioStore(tmp_path / "sqlite-parity", clock=lambda: clock[0])
    sqlite.ensure_seed(seed)
    assert workflow.locations() == sqlite.locations() == LOCATIONS
    assert workflow.recommendations(actor()) == sqlite.recommendations(actor())
    assert workflow.overview_snapshot(actor()) == sqlite.overview_snapshot(actor())
    assert workflow.recommendation_context("rec-1", actor()) == sqlite.recommendation_context(
        "rec-1", actor()
    )
    binding = fingerprint(workflow)
    assert workflow.decide("rec-1", actor(), "approve", binding, "reviewed") == sqlite.decide(
        "rec-1", actor(), "approve", binding, "reviewed"
    )
    assert workflow.execute("rec-1", actor(), binding, "parity") == sqlite.execute(
        "rec-1", actor(), binding, "parity"
    )
    clock[0] += DEMO_MATURITY_SECONDS
    assert workflow.observe("rec-1", actor("planner"), binding, "observed") == sqlite.observe(
        "rec-1", actor("planner"), binding, "observed"
    )
    assert workflow.audit(actor()) == sqlite.audit(actor())
    actual, expected = metrics(workflow), sqlite.metrics_snapshot()
    for key in ("states", "outbox", "effect_count", "event_count", "retry_count", "checked_at"):
        assert actual[key] == expected[key]
    assert actual["database"].lower() == "postgresql"
    assert_chain(workflow)


def test_concurrent_duplicate_execution_uses_one_effect_across_independent_instances(
    cluster: Cluster, workflow: PostgresWorkflowStore, clock: list[float]
) -> None:
    binding = approved(workflow)
    barrier = threading.Barrier(8)

    def execute(index: int) -> str:
        store = PostgresWorkflowStore(workflow.config, clock=lambda: clock[0])
        barrier.wait(timeout=20)
        key = "shared-key" if index % 2 else f"key-{index}"
        return store.execute("rec-1", actor(), binding, key)["execution_ref"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        references = list(pool.map(execute, range(8)))
    assert len(set(references)) == 1
    assert metrics(workflow)["effect_count"] == 1
    assert cluster.query(workflow, "SELECT count(*) FROM {schema}.effects") == [(1,)]
    assert cluster.query(workflow, "SELECT count(*) FROM {schema}.idempotency") == [(5,)]
    events = [event["event_type"] for event in workflow.audit(actor())]
    assert events.count("execution_requested") == events.count("executed") == 1
    assert_chain(workflow)


def test_concurrent_cross_recommendation_key_conflict_is_atomic(
    workflow: PostgresWorkflowStore, clock: list[float]
) -> None:
    bindings = {key: approved(workflow, key) for key in ("rec-1", "rec-2")}
    barrier = threading.Barrier(2)

    def execute(recommendation_id: str) -> tuple[str, str]:
        store = PostgresWorkflowStore(workflow.config, clock=lambda: clock[0])
        barrier.wait(timeout=20)
        try:
            result = store.execute(
                recommendation_id, actor(), bindings[recommendation_id], "one-key"
            )
            return recommendation_id, result["state"]
        except StoreError as error:
            return recommendation_id, error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = dict(pool.map(execute, bindings))
    assert sorted(results.values()) == ["executed", "idempotency_conflict"]
    assert metrics(workflow)["effect_count"] == 1
    loser = next(key for key, value in results.items() if value == "idempotency_conflict")
    assert workflow.recommendation(loser, actor())["state"] == "approved"
    workflow.execute(loser, actor(), bindings[loser], "independent-valid-key")
    assert metrics(workflow)["effect_count"] == 2
    assert_chain(workflow)


@pytest.mark.parametrize(
    "stage",
    [
        "idempotency_recorded",
        "outbox_written",
        "effect_written",
        "projection_written",
        "audit_written",
    ],
)
def test_transaction_fault_at_each_execution_boundary_rolls_back_all_effects_and_audit(
    cluster: Cluster, workflow: PostgresWorkflowStore, clock: list[float], stage: str
) -> None:
    binding = approved(workflow)
    before = workflow.recommendation("rec-1", actor())
    before_audit = workflow.audit(actor())

    def fail(observed: str) -> None:
        if observed == stage:
            raise RuntimeError(f"injected {stage}")

    failing = PostgresWorkflowStore(workflow.config, clock=lambda: clock[0], _failure_hook=fail)
    with pytest.raises(RuntimeError, match="injected"):
        failing.execute("rec-1", actor(), binding, "atomic-key")
    assert workflow.recommendation("rec-1", actor()) == before
    assert workflow.audit(actor()) == before_audit
    for table in ("effects", "outbox", "idempotency"):
        assert cluster.query(workflow, f"SELECT count(*) FROM {{schema}}.{table}") == [(0,)]
    assert workflow.execute("rec-1", actor(), binding, "atomic-key")["state"] == "executed"
    assert metrics(workflow)["effect_count"] == 1
    assert_chain(workflow)


def test_failed_delivery_retains_no_effect_then_restart_retry_and_replay_are_idempotent(
    workflow: PostgresWorkflowStore, clock: list[float]
) -> None:
    binding = approved(workflow)
    failed = workflow.execute("rec-1", actor(), binding, "failure", simulate_failure=True)
    assert failed["state"] == "execution_failed"
    assert metrics(workflow)["effect_count"] == 0
    assert metrics(workflow)["outbox"] == {"failed": 1}
    restart = PostgresWorkflowStore(workflow.config, clock=lambda: clock[0])
    executed = restart.execute("rec-1", actor(), binding, "failure")
    assert (
        restart.execute("rec-1", actor(), binding, "new-click")["execution_ref"]
        == executed["execution_ref"]
    )
    assert metrics(restart)["retry_count"] == 1 and metrics(restart)["effect_count"] == 1
    assert_chain(restart)


def test_approval_expiring_during_write_rolls_back_before_effect_commit(
    cluster: Cluster, workflow: PostgresWorkflowStore, clock: list[float]
) -> None:
    binding = approved(workflow)
    before_audit = workflow.audit(actor())
    expiry = next(
        row["approval_expires_at"]
        for row in workflow.snapshot()["recommendations"]
        if row["id"] == "rec-1"
    )
    reached = []

    def expire_after_request(stage: str) -> None:
        if stage == "outbox_written":
            reached.append(stage)
            clock[0] = expiry

    slow = PostgresWorkflowStore(
        workflow.config, clock=lambda: clock[0], _failure_hook=expire_after_request
    )
    with pytest.raises(StoreError) as expired:
        slow.execute("rec-1", actor(), binding, "expiry-during-write")
    assert expired.value.code == "approval_expired"
    assert reached == ["outbox_written"]
    assert workflow.audit(actor()) == before_audit
    assert workflow.recommendation("rec-1", actor())["state"] == "approved"
    for table in ("effects", "outbox", "idempotency"):
        assert cluster.query(workflow, f"SELECT count(*) FROM {{schema}}.{table}") == [(0,)]
    assert_chain(workflow)


def test_revocation_after_failure_and_rejection_forbid_execution(
    workflow: PostgresWorkflowStore,
) -> None:
    first = approved(workflow)
    workflow.execute("rec-1", actor(), first, "will-revoke", simulate_failure=True)
    workflow.decide("rec-1", actor(), "revoke", first)
    second = fingerprint(workflow, "rec-2")
    workflow.decide("rec-2", actor(), "reject", second)
    for key, binding in (("rec-1", first), ("rec-2", second)):
        with pytest.raises(StoreError) as caught:
            workflow.execute(key, actor(), binding, f"forbidden-{key}")
        assert caught.value.code == "approval_required"
    assert metrics(workflow)["effect_count"] == 0
    assert metrics(workflow)["outbox"] == {"cancelled": 1}


def test_expiry_and_fingerprint_contracts_survive_current_clock_restart(
    workflow: PostgresWorkflowStore, clock: list[float], seed: dict[str, Any]
) -> None:
    before = workflow.recommendation("rec-1", actor())
    with pytest.raises(StoreError) as stale:
        workflow.decide("rec-1", actor(), "approve", "f" * 64)
    assert stale.value.code == "stale_fingerprint"
    binding = approved(workflow)
    assert APPROVAL_TTL_SECONDS == 900 and RECOMMENDATION_TTL_SECONDS == 86400
    clock[0] += APPROVAL_TTL_SECONDS
    restart = PostgresWorkflowStore(workflow.config, clock=lambda: clock[0])
    restart.ensure_seed(seed)
    with pytest.raises(StoreError) as expired:
        restart.execute("rec-1", actor(), binding, "expired")
    assert expired.value.code == "approval_expired"
    after = restart.recommendation("rec-1", actor())
    assert (
        after["created_at"] == before["created_at"]
        and after["valid_until"] == before["valid_until"]
    )
    clock[0] = datetime.fromisoformat(before["valid_until"]).timestamp()
    with pytest.raises(StoreError) as recommendation_expired:
        restart.decide("rec-2", actor(), "approve", fingerprint(restart, "rec-2"))
    assert recommendation_expired.value.code == "recommendation_expired"


def test_changed_seed_cannot_replace_an_approved_initial_generation(
    workflow: PostgresWorkflowStore, seed: dict[str, Any]
) -> None:
    binding = approved(workflow)
    before = workflow.recommendation("rec-1", actor())
    changed = copy.deepcopy(seed)
    changed["metadata"]["source_revision"] = "new-source"
    with pytest.raises((PostgresBackendError, StoreError)) as refused:
        workflow.ensure_seed(changed)
    assert refused.value.code == "seed_changed"
    assert workflow.recommendation("rec-1", actor()) == before
    assert fingerprint(workflow) == binding


def test_existing_effect_retry_after_expiry_does_not_create_a_new_effect(
    workflow: PostgresWorkflowStore, clock: list[float]
) -> None:
    binding = approved(workflow)
    first = workflow.execute("rec-1", actor(), binding, "first")
    clock[0] += RECOMMENDATION_TTL_SECONDS + 10
    replay = workflow.execute("rec-1", actor(), binding, "late-retry")
    assert replay["execution_ref"] == first["execution_ref"] and replay["expired"] is True
    assert metrics(workflow)["effect_count"] == 1


def test_repeated_approval_does_not_extend_lease_and_late_approval_is_capped(
    workflow: PostgresWorkflowStore, clock: list[float]
) -> None:
    binding = fingerprint(workflow)
    first = workflow.decide("rec-1", actor(), "approve", binding)
    audit = workflow.audit(actor())
    assert first["approval_expires_at"] == iso(clock[0] + 900)
    clock[0] += 10
    assert workflow.decide("rec-1", actor(), "approve", binding) == first
    assert workflow.audit(actor()) == audit
    second = workflow.recommendation("rec-2", actor())
    clock[0] = datetime.fromisoformat(second["valid_until"]).timestamp() - 50
    late = workflow.decide("rec-2", actor(), "approve", second["fingerprint"])
    assert late["approval_expires_at"] == second["valid_until"]


def test_outcome_requires_successful_execution_planner_and_exact_maturity(
    workflow: PostgresWorkflowStore, clock: list[float]
) -> None:
    binding = fingerprint(workflow)
    with pytest.raises(StoreError) as pending:
        workflow.observe("rec-1", actor("planner"), binding)
    assert pending.value.code == "execution_required"
    approved(workflow)
    workflow.execute("rec-1", actor(), binding, "outcome")
    for role in ("approver", "auditor", "system"):
        with pytest.raises(StoreError) as denied:
            workflow.observe("rec-1", actor(role), binding)
        assert denied.value.code == "role_denied"
    maturity = clock[0] + DEMO_MATURITY_SECONDS
    clock[0] = maturity - 0.000001
    with pytest.raises(StoreError) as immature:
        workflow.observe("rec-1", actor("planner"), binding)
    assert immature.value.code == "outcome_immature"
    clock[0] = maturity
    outcome = workflow.observe("rec-1", actor("planner"), binding, "human feedback")
    assert outcome["state"] == "outcome_observed"
    assert outcome["outcome"]["simulated"] is True
    assert outcome["outcome"]["causal_business_impact_claim"] is False
    assert workflow.observe("rec-1", actor("planner"), binding) == outcome


def test_maker_checker_and_illegal_transitions_refuse_without_new_audit(
    workflow: PostgresWorkflowStore,
) -> None:
    binding = fingerprint(workflow)
    maker = Actor("planner.r4a", "Maker with approver role", ActorRole.APPROVER, LOCATIONS)
    before = workflow.audit(actor())
    with pytest.raises(StoreError) as denied:
        workflow.decide("rec-1", maker, "approve", binding)
    assert denied.value.code == "maker_checker" and workflow.audit(actor()) == before
    workflow.decide("rec-1", actor(), "reject", binding)
    with pytest.raises(StoreError) as illegal:
        workflow.decide("rec-1", actor(), "approve", binding)
    assert illegal.value.code == "illegal_transition"


def test_scoped_reads_never_leak_shared_plan_or_other_location_context(
    workflow: PostgresWorkflowStore,
) -> None:
    north = replace(actor("auditor"), locations=("WH-NORTH",))
    rows = workflow.recommendations(north)
    assert [row["id"] for row in rows] == ["rec-1"]
    record = workflow.recommendation("rec-1", north)
    audit = workflow.audit(north)
    assert {event["recommendation_id"] for event in audit} == {"rec-1"}
    serialized = json.dumps([rows, record, audit])
    for forbidden in (
        "WH-SOUTH",
        "GLOBAL_BUDGET_SECRET",
        "ALL_SITES_SECRET",
        "983741.23",
        "SOUTH_SCENARIO_SECRET",
        "SOUTH_FORECAST_SECRET",
        "GLOBAL_METRIC_SECRET",
        "GLOBAL_SOURCE_SECRET",
        "GLOBAL_LINEAGE_SECRET",
    ):
        assert forbidden not in serialized
    for read in (
        lambda: workflow.overview_snapshot(north),
        lambda: workflow.recommendation_context("rec-1", north),
        lambda: workflow.metrics_snapshot(north),
        lambda: workflow.recommendation("rec-2", north),
    ):
        with pytest.raises(StoreError) as denied:
            read()
        assert denied.value.code == "scope_denied" and denied.value.status == 403


def test_out_of_scope_and_system_actions_cannot_mutate_workflow(
    workflow: PostgresWorkflowStore,
) -> None:
    north = replace(actor(), locations=("WH-NORTH",))
    binding = fingerprint(workflow, "rec-2")
    before = workflow.audit(actor())
    with pytest.raises(StoreError) as denied:
        workflow.decide("rec-2", north, "approve", binding)
    assert denied.value.code == "scope_denied"
    for action in ("approve", "reject", "revoke", "execute"):
        with pytest.raises(StoreError) as forbidden:
            if action == "execute":
                workflow.execute("rec-2", actor("system"), binding, "system")
            else:
                workflow.decide("rec-2", actor("system"), action, binding)
        assert forbidden.value.code == "role_denied"
    assert workflow.audit(actor()) == before


def test_sessions_verify_jwt_restart_expiry_revocation_and_do_not_leak_private_rows(
    cluster: Cluster, workflow: PostgresWorkflowStore, clock: list[float]
) -> None:
    token, public = workflow.create_session("approver")
    assert public["user"]["role"] == "approver"
    restart = PostgresWorkflowStore(workflow.config, clock=lambda: clock[0])
    validated = restart.session(token)
    assert validated is not None and validated[0].role is ActorRole.APPROVER
    csrf_bound = validated[1] == public["csrf_token"]
    assert csrf_bound, "The public CSRF value must match the authenticated session"
    invalid_token_rejected = restart.session(token[:-8] + "tampered") is None
    assert invalid_token_rejected, "A modified token must not resolve to a session"
    secret = cluster.query(workflow, "SELECT value FROM {schema}.metadata WHERE key='session_key'")[
        0
    ][0]
    views = json.dumps(
        [
            workflow.snapshot(),
            workflow.seed(),
            workflow.overview_snapshot(actor()),
            workflow.recommendation_context("rec-1", actor()),
            metrics(workflow),
            workflow.audit(actor()),
        ]
    )
    private_material_exposed = any(
        private in views for private in (secret, token, validated[2], public["csrf_token"])
    )
    assert not private_material_exposed, "A public workflow projection exposed session material"
    for private_key in ("session_key", "session_tokens", "sessions"):
        assert private_key not in views
    restart.revoke_session(validated[2])
    revoked_token_rejected = workflow.session(token) is None
    assert revoked_token_rejected, "Revoked session must not resolve"
    second, _ = workflow.create_session("planner")
    clock[0] += SESSION_TTL_SECONDS
    expired_token_rejected = restart.session(second) is None
    assert expired_token_rejected, "Expired session must not resolve"


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE {schema}.recommendations SET approval_actor='phantom' WHERE id='rec-1'",
        "UPDATE {schema}.recommendations SET approval_fingerprint=repeat('f',64) WHERE id='rec-1'",
        "UPDATE {schema}.recommendations SET approval_expires_at=123 WHERE id='rec-1'",
        "UPDATE {schema}.recommendations SET execution_ref='phantom' WHERE id='rec-1'",
        "UPDATE {schema}.recommendations SET executed_at=123 WHERE id='rec-1'",
        "UPDATE {schema}.recommendations SET outcome_due_at=123 WHERE id='rec-1'",
        "UPDATE {schema}.recommendations SET outcome_json='{{}}' WHERE id='rec-1'",
        "UPDATE {schema}.recommendations SET state='approved' WHERE id='rec-1'",
    ],
)
def test_phantom_projection_fields_without_matching_audit_fail_closed(
    cluster: Cluster, workflow: PostgresWorkflowStore, statement: str
) -> None:
    cluster.query(workflow, statement)
    for read in (lambda: workflow.recommendation("rec-1", actor()), lambda: metrics(workflow)):
        with pytest.raises((PostgresBackendError, StoreError)) as corrupt:
            read()
        assert corrupt.value.code == "integrity_error"


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE {schema}.outbox SET attempts=attempts+1",
        "UPDATE {schema}.outbox SET fingerprint=repeat('f',64)",
        "UPDATE {schema}.outbox SET operation_id=repeat('f',64)",
        "UPDATE {schema}.outbox SET state='failed'",
        "UPDATE {schema}.outbox SET last_error='invented error'",
        "DELETE FROM {schema}.outbox",
        "UPDATE {schema}.idempotency SET fingerprint=repeat('f',64)",
        "UPDATE {schema}.idempotency SET recommendation_id='rec-2'",
    ],
)
def test_execution_side_table_tamper_cannot_be_replayed_as_success(
    cluster: Cluster, workflow: PostgresWorkflowStore, statement: str
) -> None:
    binding = approved(workflow)
    workflow.execute("rec-1", actor(), binding, "original")
    if ".idempotency " in statement:
        cluster.query(workflow, "ALTER TABLE {schema}.idempotency DISABLE TRIGGER USER")
    cluster.query(workflow, statement)
    if ".idempotency " in statement:
        cluster.query(workflow, "ALTER TABLE {schema}.idempotency ENABLE TRIGGER USER")
    for action in (
        lambda: workflow.execute("rec-1", actor(), binding, "retry"),
        lambda: metrics(workflow),
    ):
        with pytest.raises((PostgresBackendError, StoreError)) as corrupt:
            action()
        assert corrupt.value.code == "integrity_error"


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE {schema}.effects SET payload='{{}}'",
        "UPDATE {schema}.effects SET external_reference='changed-reference'",
        "UPDATE {schema}.effects SET created_at=created_at+1",
        "DELETE FROM {schema}.effects",
    ],
)
def test_privileged_effect_tamper_is_detected_after_restoring_immutability_triggers(
    cluster: Cluster, workflow: PostgresWorkflowStore, statement: str
) -> None:
    binding = approved(workflow)
    workflow.execute("rec-1", actor(), binding, "original")
    cluster.query(workflow, "ALTER TABLE {schema}.effects DISABLE TRIGGER USER")
    cluster.query(workflow, statement)
    cluster.query(workflow, "ALTER TABLE {schema}.effects ENABLE TRIGGER USER")
    with pytest.raises((PostgresBackendError, StoreError)) as corrupt:
        workflow.execute("rec-1", actor(), binding, "retry")
    assert corrupt.value.code == "integrity_error"


def test_application_has_no_ddl_history_or_effect_rewrite_privilege(
    cluster: Cluster, workflow: PostgresWorkflowStore
) -> None:
    binding = approved(workflow)
    workflow.execute("rec-1", actor(), binding, "immutable")
    for statement in (
        "CREATE TABLE {schema}.forbidden_table (id INTEGER)",
        "UPDATE {schema}.schema_migrations SET migration_sha256=repeat('f',64)",
        "UPDATE {schema}.events SET actor_id='forged'",
        "DELETE FROM {schema}.events",
        "UPDATE {schema}.effects SET payload='{{}}'",
        "DELETE FROM {schema}.effects",
    ):
        with pytest.raises(cluster.driver.errors.InsufficientPrivilege):
            cluster.query(workflow, statement, role="application")
    assert metrics(workflow)["effect_count"] == 1


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE {schema}.sessions SET persona='approver'",
        "UPDATE {schema}.session_tokens SET subject='approver.portfolio'",
        "UPDATE {schema}.session_tokens SET token_id=repeat('f',64)",
        "UPDATE {schema}.sessions SET expires_at=expires_at+3600",
    ],
)
def test_retained_session_row_tamper_cannot_escalate_a_signed_planner_token(
    cluster: Cluster, workflow: PostgresWorkflowStore, statement: str
) -> None:
    token, _ = workflow.create_session("planner")
    cluster.query(workflow, "ALTER TABLE {schema}.session_tokens DISABLE TRIGGER USER")
    cluster.query(workflow, statement)
    cluster.query(workflow, "ALTER TABLE {schema}.session_tokens ENABLE TRIGGER USER")
    # Corrupt retained state is an integrity incident, not an ordinary bad login.
    with pytest.raises((PostgresBackendError, StoreError)) as corrupt:
        workflow.session(token)
    assert corrupt.value.code == "integrity_error"


def test_orphan_idempotency_binding_without_execution_request_fails_closed(
    cluster: Cluster, workflow: PostgresWorkflowStore
) -> None:
    second = fingerprint(workflow, "rec-2")
    cluster.query(
        workflow,
        "INSERT INTO {schema}.idempotency (key,recommendation_id,fingerprint) VALUES (%s,%s,%s)",
        ("orphan-with-valid-fingerprint", "rec-2", second),
    )
    with pytest.raises((PostgresBackendError, StoreError)) as corrupt:
        metrics(workflow)
    assert corrupt.value.code == "integrity_error"


def test_scoped_audit_verifies_indexed_location_ownership_before_filtering(
    cluster: Cluster, workflow: PostgresWorkflowStore
) -> None:
    north = replace(actor("auditor"), locations=("WH-NORTH",))
    cluster.query(
        workflow, "UPDATE {schema}.recommendations SET location_id='WH-NORTH' WHERE id='rec-2'"
    )
    with pytest.raises((PostgresBackendError, StoreError)) as corrupt:
        workflow.audit(north)
    assert corrupt.value.code == "integrity_error"
