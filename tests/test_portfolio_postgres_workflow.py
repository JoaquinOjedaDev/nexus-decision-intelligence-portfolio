"""Portable PostgreSQL workflow import and pre-database authorization boundaries."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_backend import PostgresConfig
from nexus.portfolio.postgres_workflow import PostgresWorkflowStore
from nexus.portfolio.store import StoreError


@pytest.fixture
def lazy_workflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PostgresWorkflowStore:
    # No test may connect, even when optional psycopg is installed in this environment.
    monkeypatch.setitem(sys.modules, "psycopg", None)
    return PostgresWorkflowStore(
        PostgresConfig(
            dsn="postgresql://unused:do-not-print@127.0.0.1:55438/nexus_portfolio_test_never_connect",
            schema="nexus_test_portable_workflow",
            artifact_root=tmp_path / "not-created",
        )
    )


def test_workflow_constructor_is_lazy_without_sqlite_connection_shim(
    lazy_workflow: PostgresWorkflowStore,
) -> None:
    assert not lazy_workflow.config.artifact_root.exists()
    assert "do-not-print" not in repr(lazy_workflow)
    assert "do-not-print" not in repr(lazy_workflow.config)
    assert not hasattr(lazy_workflow, "connection")
    assert not hasattr(lazy_workflow, "transaction")


@pytest.mark.parametrize("role", [ActorRole.PLANNER, ActorRole.AUDITOR, ActorRole.SYSTEM])
@pytest.mark.parametrize("action", ["approve", "reject", "revoke", "execute"])
def test_disallowed_roles_cannot_reach_database_for_human_actions(
    lazy_workflow: PostgresWorkflowStore, role: ActorRole, action: str
) -> None:
    principal = Actor("portable-denied", "Denied", role, ("WH-NORTH",))
    with pytest.raises(StoreError) as caught:
        if action == "execute":
            lazy_workflow.execute("rec-1", principal, "a" * 64, "not-dispatched")
        else:
            lazy_workflow.decide("rec-1", principal, action, "a" * 64)
    assert caught.value.code == "role_denied" and caught.value.status == 403


@pytest.mark.parametrize("role", [ActorRole.APPROVER, ActorRole.AUDITOR, ActorRole.SYSTEM])
def test_only_planner_can_request_outcome_before_database_access(
    lazy_workflow: PostgresWorkflowStore, role: ActorRole
) -> None:
    principal = Actor("portable-denied", "Denied", role, ("WH-NORTH",))
    with pytest.raises(StoreError) as caught:
        lazy_workflow.observe("rec-1", principal, "a" * 64)
    assert caught.value.code == "role_denied" and caught.value.status == 403


@pytest.mark.parametrize("action", ["publish", "execute", "", None])
def test_unknown_decision_action_refuses_before_database_access(
    lazy_workflow: PostgresWorkflowStore, action: Any
) -> None:
    principal = Actor("approver.test", "Approver", ActorRole.APPROVER, ("WH-NORTH",))
    with pytest.raises(StoreError) as caught:
        lazy_workflow.decide("rec-1", principal, action, "a" * 64)
    assert caught.value.code == "invalid_action" and caught.value.status == 422


@pytest.mark.parametrize("key", ["", "x" * 129])
def test_unbounded_execution_keys_refuse_before_database_access(
    lazy_workflow: PostgresWorkflowStore, key: str
) -> None:
    principal = Actor("approver.test", "Approver", ActorRole.APPROVER, ("WH-NORTH",))
    with pytest.raises(StoreError) as caught:
        lazy_workflow.execute("rec-1", principal, "a" * 64, key)
    assert caught.value.code == "invalid_idempotency_key" and caught.value.status == 422
