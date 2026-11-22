"""Opt-in real PostgreSQL views plus local learning history, using constructed receipts.

The shared cluster fixture requires explicit isolated acceptance configuration.
These cases never start/stop services, build dbt/models, or claim scientific results.
"""

from __future__ import annotations

import shutil
import sqlite3
from contextlib import closing
from dataclasses import replace
from typing import Any

import pytest
from fastapi.testclient import TestClient
from test_portfolio_postgres_api_service import application as application
from test_portfolio_postgres_api_service import login, rich_seed
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import LOCATIONS, actor, event

from nexus.portfolio.learning_attachments import _DDL
from nexus.portfolio.learning_views import attach_learning_report, read_learning_report
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.runtime_feedback import feedback_book, retain_runtime_forecasts
from nexus.portfolio.service_backend import bind_service_root
from nexus.portfolio.store import StoreError
from nexus.portfolio.training_pipeline import LearningRun

pytestmark = pytest.mark.portfolio_service


def evaluated(source: str = "a") -> LearningRun:
    """A self-verified abstention fixture; no fitted-model or source-pipeline claim."""
    return LearningRun(
        {
            "contract_version": "nexus.runtime-learning-result.v1",
            "bindings": {
                "source_snapshot_sha256": source * 64,
                "snapshot_sha256": "b" * 64,
                "plan_sha256": "c" * 64,
                "code_sha256": "d" * 64,
                "finalization_sha256": None,
            },
            "status": "ABSTAIN_INSUFFICIENT_EVIDENCE",
            "training_performed": False,
            "classification": "synthetic",
            "reason": "split_support_not_met",
            "snapshot": {"labels": [{"private_source_row": "NOT_FOR_BROWSER"}]},
            "splits": {"training": {"rows": 20}, "calibration": {"rows": 4}, "final": {"rows": 2}},
        },
        None,
        None,
    )


def snapshot(store: PostgresRuntimeStore) -> dict[str, Any]:
    with store.read_snapshot() as unit:
        return unit.snapshot()


def test_learning_and_feedback_persist_across_real_activation_without_workflow_mutation(
    application: Any,
    cluster: Cluster,
) -> None:
    store = application.state.test_store
    reader, registrar = actor("auditor"), actor("worker")
    before = snapshot(store)
    assert read_learning_report(store, reader) == {"status": "not_run", "report": None}
    original = attach_learning_report(store, evaluated(), actor=registrar)
    assert attach_learning_report(store, evaluated(), actor=registrar) == original
    retain_runtime_forecasts(store, reader=reader)
    assert snapshot(store) == before
    assert original["report"]["locations"] == sorted(LOCATIONS)
    assert original["report"]["deployed"] is original["report"]["human_approval"] is False
    assert "NOT_FOR_BROWSER" not in str(original)

    client = TestClient(application)
    login(client, "auditor")
    assert client.get("/api/learning").json() == original
    assert client.post("/api/learning", json={"receipt": "untrusted"}).status_code == 405

    application.state.test_clock[0] += 1
    store.control.submit(event(store.clock(), "b"), actor())
    job = store.control.claim(registrar)
    assert job is not None
    evidence = store.control.attempt_evidence_dir(job["id"], job["token"], registrar)
    candidate = rich_seed(evidence, "b")
    # NORTH and FORECAST become archived-only locations, still part of global scope.
    candidate["recommendations"] = [candidate["recommendations"][1]]
    candidate["forecasts"] = [candidate["forecasts"][1]]
    store.control.activate(job["id"], job["token"], candidate, registrar, evidence_dir=evidence)
    assert read_learning_report(store, reader) == {**original, "status": "stale"}
    partial = replace(reader, locations=(LOCATIONS[1],))
    with pytest.raises(StoreError) as error:
        read_learning_report(store, partial)
    assert (error.value.code, error.value.status) == ("scope_denied", 403)
    with pytest.raises(StoreError, match="different source"):
        attach_learning_report(store, evaluated(), actor=registrar)

    activated = snapshot(store)
    current = attach_learning_report(store, evaluated("b"), actor=registrar)
    retain_runtime_forecasts(store, reader=reader)
    reopened = PostgresRuntimeStore(
        store.config, runtime_root=store.runtime_root, clock=store.clock
    )
    assert read_learning_report(reopened, reader) == current
    assert snapshot(reopened) == activated
    assert feedback_book(reopened).snapshot()["counts"]["forecast_generation"] == 2
    with closing(sqlite3.connect(store.runtime_root / "learning-attachments.sqlite3")) as db:
        assert db.execute("SELECT count(*) FROM attachment_events").fetchone() == (2,)
    assert (
        cluster.query(
            store, "SELECT key FROM {schema}.metadata WHERE key LIKE %s", ("learning_report%",)
        )
        == []
    )
    assert not (store.runtime_root / "portfolio.sqlite3").exists()


def test_native_learning_scope_tamper_and_cross_runtime_binding_fail_closed(
    application: Any,
    cluster: Cluster,
) -> None:
    store = application.state.test_store
    reader, registrar = actor("auditor"), actor("worker")
    sidecar = store.runtime_root / "learning-attachments.sqlite3"
    before = snapshot(store)
    partial = replace(reader, locations=LOCATIONS[:2])
    for operation in (
        lambda: read_learning_report(store, partial),
        lambda: attach_learning_report(
            store, evaluated(), actor=replace(registrar, locations=LOCATIONS[:2])
        ),
        lambda: attach_learning_report(store, evaluated(), actor=reader),
        lambda: attach_learning_report(store, evaluated("f"), actor=registrar),
    ):
        with pytest.raises(StoreError):
            operation()
        assert not sidecar.exists()
    attach_learning_report(store, evaluated(), actor=registrar)

    other_root = store.runtime_root.parent / "other-runtime"
    other_evidence = other_root / "evidence" / "initial"
    other_seed = rich_seed(other_evidence)
    other = PostgresRuntimeStore(
        cluster.backend(other_evidence).config, runtime_root=other_root, clock=store.clock
    )
    cluster.migrate(other)
    other.ensure_seed(other_seed)
    bind_service_root(other, claim=True)
    shutil.copyfile(sidecar, other_root / sidecar.name)
    for operation in (
        lambda: read_learning_report(other, reader),
        lambda: attach_learning_report(other, evaluated(), actor=registrar),
    ):
        with pytest.raises(StoreError) as error:
            operation()
        assert error.value.code == "integrity_error"

    with closing(sqlite3.connect(sidecar)) as db, db:
        db.execute("DROP TRIGGER attachment_events_no_update")
        db.execute("UPDATE attachment_events SET payload='{}'")
        db.execute(_DDL["attachment_events_no_update"])
    with pytest.raises(StoreError) as error:
        read_learning_report(store, reader)
    assert error.value.code == "integrity_error"
    assert snapshot(store) == before
    assert not (store.runtime_root / "portfolio.sqlite3").exists()
