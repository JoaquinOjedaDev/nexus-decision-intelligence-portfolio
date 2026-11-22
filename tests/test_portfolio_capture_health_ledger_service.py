"""Guarded native health policy/action tests; observations here are constructed."""

from dataclasses import replace
from uuid import uuid4

import pytest
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import actor
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime
from test_portfolio_transport_ledger_service import binding as binding
from test_portfolio_transport_ledger_service import ledger as ledger
from test_portfolio_transport_runtime_service import approve

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.capture_health import CaptureObservation
from nexus.portfolio.capture_health_ledger import CaptureHealthLedger
from nexus.portfolio.postgres_backend import PostgresBackendError
from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger
from nexus.portfolio.store import StoreError, digest
from nexus.portfolio.transport_runtime import TransportRuntimeStore

pytestmark = pytest.mark.portfolio_service


def writer(binding):
    return Actor(
        "capture-operations", "Operations capture", ActorRole.SYSTEM, binding.broker.locations
    )


@pytest.fixture
def healthy(cluster, runtime, ledger, binding, clock):
    health = CaptureHealthLedger(
        ledger.config, health_binding=binding, max_age_seconds=10, clock=lambda: clock[0]
    )
    assert cluster.migrate(health)["version"] == 12
    store = TransportRuntimeStore(
        runtime.config,
        transport=health,
        binding=binding,
        runtime_root=runtime.runtime_root,
        clock=lambda: clock[0],
    )
    cluster.migrate(store)
    return store


def sample(store, clock, *, instance=None, high=(0, 0)):
    ledger, binding = store.transport, store.transport_binding
    identity = instance or str(uuid4())
    ledger.begin_capture(binding, identity, writer(binding))
    clock[0] += 0.1
    return CaptureObservation(
        str(uuid4()),
        identity,
        digest(binding.payload()),
        clock[0],
        clock[0],
        (0, 0),
        high,
        (0, 0),
        (0, 0),
    )


def retain(store, observation):
    return store.transport.record_health(
        store.transport_binding, observation, writer(store.transport_binding)
    )


def test_missing_fresh_and_expired_are_enforced(healthy, clock):
    with pytest.raises(StoreError) as error:
        approve(healthy)
    assert error.value.code == "capture_observation_missing"
    fresh = sample(healthy, clock)
    retain(healthy, fresh)
    fingerprint = approve(healthy)
    clock[0] += 10
    before = healthy.snapshot()
    with pytest.raises(StoreError) as error:
        healthy.execute("rec-1", actor("approver"), fingerprint, "expired")
    assert error.value.code == "capture_observation_expired"
    assert healthy.snapshot() == before
    healthy.decide("rec-1", actor("approver"), "revoke", fingerprint)


def test_lagging_and_regressing_broker_heads_are_refused(healthy, clock):
    lag = sample(healthy, clock, high=(1, 0))
    retain(healthy, lag)
    with pytest.raises(StoreError) as error:
        approve(healthy)
    assert error.value.code == "capture_lagging"
    regressed = sample(healthy, clock, instance=lag.consumer_instance_id)
    with pytest.raises(StoreError) as error:
        retain(healthy, regressed)
    assert error.value.code == "capture_sample_superseded"


def test_expiry_before_commit_rolls_back_effect(healthy, clock):
    retain(healthy, sample(healthy, clock))
    fingerprint = approve(healthy)
    before = healthy.snapshot()

    def expire(stage):
        if stage == "effect_written":
            clock[0] += 11

    healthy._failure_hook = expire
    with pytest.raises(StoreError) as error:
        healthy.execute("rec-1", actor("approver"), fingerprint, "expires-during-write")
    assert error.value.code == "capture_observation_expired"
    assert healthy.snapshot() == before


def test_restart_supersedes_old_capture_generation(healthy, clock):
    first = sample(healthy, clock)
    retain(healthy, first)
    approve(healthy)
    replacement = sample(healthy, clock)
    with pytest.raises(StoreError) as error:
        approve(healthy, "rec-2")
    assert error.value.code == "capture_observation_missing"
    with pytest.raises(StoreError) as error:
        retain(
            healthy,
            replace(first, observation_id=str(uuid4()), started_at=clock[0], completed_at=clock[0]),
        )
    assert error.value.code == "capture_generation_superseded"
    retain(healthy, replacement)
    assert retain(healthy, replacement)["recorded"] is False
    approve(healthy, "rec-2")
    with pytest.raises(StoreError, match="Old capture generation"):
        healthy.transport.begin_capture(
            healthy.transport_binding, first.consumer_instance_id, writer(healthy.transport_binding)
        )
    proof = healthy.transport.verify_history(healthy.transport_binding)
    assert proof["capture_instances"] == proof["capture_observations"] == 2


def test_policy_persists_and_old_clients_refuse(healthy, cluster, clock):
    health = healthy.transport
    assert cluster.migrate(health)["applied"] is False
    old = RecoveryResolutionLedger(health.config)
    with pytest.raises(PostgresBackendError):
        old.capture_progress(healthy.transport_binding)
    wrong = CaptureHealthLedger(
        health.config,
        health_binding=healthy.transport_binding,
        max_age_seconds=20,
        clock=lambda: clock[0],
    )
    with pytest.raises(StoreError, match="Retained capture policy"):
        wrong.capture_progress(healthy.transport_binding)
    restored = CaptureHealthLedger(
        health.config,
        health_binding=healthy.transport_binding,
        max_age_seconds=10,
        clock=lambda: clock[0],
    )
    assert restored.capture_progress(healthy.transport_binding) == (0, 0)


@pytest.mark.parametrize("role", ["planner", "approver", "auditor"])
def test_humans_cannot_refresh_capture_health(healthy, clock, role):
    observed = sample(healthy, clock)
    with pytest.raises(StoreError) as error:
        healthy.transport.record_health(healthy.transport_binding, observed, actor(role))
    assert error.value.code == "role_denied"


def test_committed_idempotent_effect_is_retrievable_when_health_expires(healthy, clock):
    retain(healthy, sample(healthy, clock))
    fingerprint = approve(healthy)
    first = healthy.execute("rec-1", actor("approver"), fingerprint, "once")
    clock[0] += 11
    repeated = healthy.execute("rec-1", actor("approver"), fingerprint, "once")
    assert repeated == {**first, "outcome_ready": True}
    assert len(healthy.snapshot()["effects"]) == 1


def test_capture_writer_requires_exact_identity_scope_and_binding(healthy, clock):
    observed = sample(healthy, clock)
    binding = healthy.transport_binding
    for identity, code in (
        (replace(writer(binding), actor_id="source-observer"), "role_denied"),
        (replace(writer(binding), locations=()), "scope_denied"),
    ):
        with pytest.raises(StoreError) as error:
            healthy.transport.record_health(binding, observed, identity)
        assert error.value.code == code
    with pytest.raises(StoreError) as error:
        healthy.transport.record_health(
            replace(binding, cluster_id="other-cluster"), observed, writer(binding)
        )
    assert error.value.code == "capture_binding_changed"
    assert healthy.transport.verify_history(binding)["capture_observations"] == 0


def test_clock_regression_and_duplicate_sample_do_not_refresh_health(healthy, clock):
    observed = sample(healthy, clock)
    retain(healthy, observed)
    clock[0] -= 0.01
    with pytest.raises(StoreError) as error:
        approve(healthy)
    assert error.value.code == "capture_clock_regressed"
    clock[0] = observed.started_at + 10
    with pytest.raises(StoreError) as error:
        retain(healthy, observed)
    assert error.value.code == "capture_observation_expired"
    proof = healthy.transport.verify_history(healthy.transport_binding)
    assert proof["capture_observations"] == 1


@pytest.mark.parametrize(
    "table", ["capture_health_policy", "capture_instances", "capture_observations"]
)
def test_application_cannot_rewrite_capture_evidence(healthy, clock, cluster, table):
    retain(healthy, sample(healthy, clock))
    with pytest.raises(cluster.driver.errors.InsufficientPrivilege):
        cluster.query(
            healthy.transport,
            "DELETE FROM {schema}." + table,
            role="application",
        )
    assert healthy.transport.verify_history(healthy.transport_binding)["capture_observations"] == 1
