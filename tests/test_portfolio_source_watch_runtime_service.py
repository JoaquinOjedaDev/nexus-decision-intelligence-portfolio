"""Real isolated PostgreSQL freshness fences; constructed plans, actual source reads."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Event, current_thread

import pytest
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import actor, source_seed
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_publication_runtime_service import envelope, publisher
from test_portfolio_publication_runtime_service import published as published
from test_portfolio_transport_ledger_service import binding as binding
from test_portfolio_transport_ledger_service import ledger as ledger
from test_portfolio_transport_runtime_service import approve
from test_portfolio_transport_runtime_service import fenced as fenced

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_backend import PostgresBackendError
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.source_freshness import SourceWatchPolicy
from nexus.portfolio.source_watch_runtime import SourceWatchRuntimeStore
from nexus.portfolio.store import StoreError
from nexus.simulation import generate_r2_smoke_fixture

pytestmark = pytest.mark.portfolio_service


@pytest.fixture
def runtime(cluster: Cluster, tmp_path: Path, clock: list[float]) -> PostgresRuntimeStore:
    root = tmp_path / "runtime"
    source = tmp_path / "source"
    generate_r2_smoke_fixture(source)
    seed = source_seed(root / "evidence" / "initial")
    seed["metadata"]["source_snapshot_sha256"] = SourceBundle.capture(source).source_sha256
    store = PostgresRuntimeStore(
        cluster.backend(root / "evidence" / "initial").config,
        runtime_root=root,
        clock=lambda: clock[0],
    )
    cluster.migrate(store)
    store.ensure_seed(seed)
    return store


@pytest.fixture
def watched(
    published: PublicationRuntimeStore, cluster: Cluster, tmp_path: Path
) -> SourceWatchRuntimeStore:
    result = SourceWatchRuntimeStore(
        published.config,
        transport=published.transport,
        binding=published.transport_binding,
        runtime_root=published.runtime_root,
        clock=published.clock,
        watch=SourceWatchPolicy("inventory", str((tmp_path / "source").resolve()), 30),
    )
    before = published.snapshot()
    assert cluster.migrate(result)["applied"]
    assert result.snapshot() == before
    return result


def observer(store: SourceWatchRuntimeStore) -> Actor:
    return Actor("source-observer", "Source observer", ActorRole.SYSTEM, store.locations())


def test_missing_blocks_then_real_probe_enables(watched: SourceWatchRuntimeStore) -> None:
    before = watched.snapshot()
    with pytest.raises(StoreError) as denied:
        approve(watched)
    assert denied.value.code == "source_observation_missing"
    assert watched.snapshot() == before
    assert watched.poll_source(observer(watched))["reason"] == "fresh"
    approve(watched)


def test_expired_blocks_new_effect_but_revocation_remains(
    watched: SourceWatchRuntimeStore, clock: list[float]
) -> None:
    watched.poll_source(observer(watched))
    fingerprint = approve(watched)
    clock[0] += 30
    with pytest.raises(StoreError) as denied:
        watched.execute("rec-1", actor("approver"), fingerprint, "expired")
    assert denied.value.code == "source_observation_expired"
    assert not watched.snapshot()["effects"]
    watched.decide("rec-1", actor("approver"), "revoke", fingerprint)
    watched.poll_source(observer(watched))
    assert watched.source_watch_status(actor())["reason"] == "fresh"
    assert watched.recommendation("rec-1", actor())["status"] != "approved"


def test_prior_effect_retry_is_available_after_expiry(
    watched: SourceWatchRuntimeStore, clock: list[float]
) -> None:
    watched.poll_source(observer(watched))
    fingerprint = approve(watched)
    effect = watched.execute("rec-1", actor("approver"), fingerprint, "once")
    clock[0] += 31
    again = watched.execute("rec-1", actor("approver"), fingerprint, "once")
    assert again["outcome_ready"] is True and effect["outcome_ready"] is False
    assert {k: v for k, v in again.items() if k != "outcome_ready"} == {
        k: v for k, v in effect.items() if k != "outcome_ready"
    }
    assert len(watched.snapshot()["effects"]) == 1


def test_read_failure_fences_even_at_same_clock(
    watched: SourceWatchRuntimeStore, tmp_path: Path
) -> None:
    watched.poll_source(observer(watched))
    root = Path(watched.watch.source_root)
    root.rename(tmp_path / "source-preserved")
    assert watched.poll_source(observer(watched))["reason"] == "source_read_failed"
    with pytest.raises(StoreError) as denied:
        approve(watched)
    assert denied.value.code == "source_read_failed"


def test_changed_unpublished_bytes_fence_empty_queues(
    watched: SourceWatchRuntimeStore, clock: list[float], tmp_path: Path
) -> None:
    watched.poll_source(observer(watched))
    clock[0] += 1
    Path(watched.watch.source_root).rename(tmp_path / "original-source-preserved")
    generate_r2_smoke_fixture(Path(watched.watch.source_root), seed=123)
    assert watched.poll_source(observer(watched))["reason"] == "source_changed"
    assert watched.publication_status(actor())["pending_admission"] == 0
    with pytest.raises(StoreError) as denied:
        approve(watched)
    assert denied.value.code == "source_changed"


def test_restart_and_v7_downgrade_refusal(
    watched: SourceWatchRuntimeStore,
    published: PublicationRuntimeStore,
    cluster: Cluster,
) -> None:
    watched.poll_source(observer(watched))
    assert cluster.migrate(watched)["applied"] is False
    restarted = SourceWatchRuntimeStore(
        watched.config,
        transport=watched.transport,
        binding=watched.transport_binding,
        runtime_root=watched.runtime_root,
        clock=watched.clock,
        watch=watched.watch,
    )
    assert restarted.source_watch_status(actor())["reason"] == "fresh"
    approve(restarted)
    with pytest.raises(PostgresBackendError):
        approve(published)
    with pytest.raises(PostgresBackendError):
        cluster.migrate(published)


def test_policy_cannot_be_relaxed_on_restart(watched: SourceWatchRuntimeStore) -> None:
    relaxed = SourceWatchRuntimeStore(
        watched.config,
        transport=watched.transport,
        binding=watched.transport_binding,
        runtime_root=watched.runtime_root,
        clock=watched.clock,
        watch=replace(watched.watch, max_age_seconds=3600),
    )
    with pytest.raises(PostgresBackendError) as denied:
        relaxed.snapshot()
    assert denied.value.code == "source_policy_mismatch"


@pytest.mark.parametrize("role", ["planner", "approver", "auditor", "worker"])
def test_humans_and_worker_cannot_assert_source_health(
    watched: SourceWatchRuntimeStore,
    role: str,
) -> None:
    with pytest.raises(StoreError) as denied:
        watched.poll_source(actor(role))
    assert denied.value.code == "role_denied"


def test_scope_cannot_be_omitted(watched: SourceWatchRuntimeStore) -> None:
    partial = replace(observer(watched), locations=watched.locations()[:1])
    with pytest.raises(StoreError) as denied:
        watched.poll_source(partial)
    assert denied.value.code == "scope_denied"


def test_recording_failure_rolls_back_receipt(
    watched: SourceWatchRuntimeStore, cluster: Cluster
) -> None:
    def fail(stage: str) -> None:
        if stage == "source_observation_recorded":
            raise RuntimeError("receipt rollback")

    watched._failure_hook = fail
    with pytest.raises(RuntimeError, match="rollback"):
        watched.poll_source(observer(watched))
    watched._failure_hook = None
    assert cluster.query(watched, "SELECT COUNT(*) FROM {schema}.runtime_source_observations") == [
        (0,)
    ]
    assert watched.source_watch_status(actor())["reason"] == "source_observation_missing"


def test_publication_racing_probe_does_not_mark_new_generation_fresh(
    watched: SourceWatchRuntimeStore,
    clock: list[float],
    tmp_path: Path,
) -> None:
    generate_r2_smoke_fixture(tmp_path / "changed", seed=123)
    changed = SourceBundle.capture(tmp_path / "changed")

    def enqueue(stage: str) -> None:
        if stage == "source_probe_completed":
            watched.enqueue_source(changed, envelope(changed, clock), publisher(watched))

    watched._failure_hook = enqueue
    result = watched.poll_source(observer(watched))
    watched._failure_hook = None
    assert result["reason"] == "source_observation_superseded"
    assert watched.source_watch_status(actor())["reason"] == "source_observation_superseded"


def test_clock_regression_does_not_make_retained_health_fresh(
    watched: SourceWatchRuntimeStore, clock: list[float]
) -> None:
    watched.poll_source(observer(watched))
    clock[0] -= 1
    with pytest.raises(StoreError) as denied:
        approve(watched)
    assert denied.value.code == "source_clock_regressed"


def test_application_cannot_rewrite_policy_or_observation(
    watched: SourceWatchRuntimeStore, cluster: Cluster
) -> None:
    watched.poll_source(observer(watched))
    for table in ("runtime_source_watch", "runtime_source_observations"):
        for statement in (
            "UPDATE {schema}." + table + " SET payload='tampered'",
            "DELETE FROM {schema}." + table,
        ):
            with pytest.raises(cluster.driver.errors.InsufficientPrivilege):
                cluster.query(watched, statement, role="application")
    assert watched.source_watch_status(actor())["reason"] == "fresh"


def test_status_does_not_expose_source_path(watched: SourceWatchRuntimeStore) -> None:
    watched.poll_source(observer(watched))
    status = watched.source_watch_status(actor())
    assert watched.watch.source_root not in str(status)
    assert status["claim"] == "bounded_local_source_observation_not_upstream_event_time"


def test_slow_old_success_cannot_overwrite_new_failure(
    watched: SourceWatchRuntimeStore, clock: list[float], tmp_path: Path
) -> None:
    completed, release = Event(), Event()

    def delay(stage: str) -> None:
        if stage == "source_probe_completed" and current_thread().name.startswith("old-probe"):
            completed.set()
            assert release.wait(15), "old probe was never released"

    watched._failure_hook = delay
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="old-probe") as pool:
        pending = pool.submit(watched.poll_source, observer(watched))
        try:
            assert completed.wait(15)
            clock[0] += 1
            Path(watched.watch.source_root).rename(tmp_path / "source-preserved")
            assert watched.poll_source(observer(watched))["reason"] == "source_read_failed"
        finally:
            release.set()
        assert pending.result(timeout=15) == {"recorded": False, "reason": "superseded_probe"}
    watched._failure_hook = None
    assert watched.source_watch_status(actor())["reason"] == "source_read_failed"
