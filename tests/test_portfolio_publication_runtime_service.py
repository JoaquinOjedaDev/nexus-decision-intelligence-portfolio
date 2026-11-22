"""Actual isolated PostgreSQL publication transaction and retained-source tests."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import actor, event
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime
from test_portfolio_transport_ledger_service import binding as binding
from test_portfolio_transport_ledger_service import ledger as ledger
from test_portfolio_transport_runtime_service import approve
from test_portfolio_transport_runtime_service import fenced as fenced

from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio import source_publisher
from nexus.portfolio.auth import Actor
from nexus.portfolio.broker_runtime import BrokerAdmissionError
from nexus.portfolio.postgres_backend import PostgresBackendError
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.runtime_pipeline import runtime_source_fingerprint
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.source_publisher import SourceOutboxPublisher
from nexus.portfolio.store import StoreError, canonical
from nexus.portfolio.transport_ledger import TransportRecord
from nexus.portfolio.transport_runtime import TransportRuntimeStore
from nexus.simulation import generate_r2_smoke_fixture

pytestmark = pytest.mark.portfolio_service


@pytest.fixture
def published(fenced: TransportRuntimeStore, cluster: Cluster) -> PublicationRuntimeStore:
    result = PublicationRuntimeStore(
        fenced.config,
        transport=fenced.transport,
        binding=fenced.transport_binding,
        runtime_root=fenced.runtime_root,
        clock=fenced.clock,
    )
    before = fenced.snapshot()
    assert cluster.migrate(result)["applied"] is True
    assert result.snapshot() == before
    return result


@pytest.fixture
def bundle(tmp_path: Path) -> SourceBundle:
    generate_r2_smoke_fixture(tmp_path / "producer")
    return SourceBundle.capture(tmp_path / "producer")


def publisher(store: PublicationRuntimeStore) -> Actor:
    return Actor("source-publisher", "Source publisher", ActorRole.SYSTEM, store.locations())


def broker_actor(store: PublicationRuntimeStore) -> Actor:
    return Actor("runtime-event-consumer", "Event consumer", ActorRole.SYSTEM, store.locations())


def envelope(bundle: SourceBundle, clock: list[float]) -> EventEnvelope:
    return event(clock[0]).model_copy(
        update={
            "source": "nexus.source-snapshot.v1",
            "payload": {"source_snapshot_sha256": bundle.source_sha256},
        }
    )


def test_migration_reapply_and_v6_bypass_refused(
    published: PublicationRuntimeStore, fenced: TransportRuntimeStore, cluster: Cluster
) -> None:
    assert cluster.migrate(published)["version"] == 7
    assert cluster.migrate(published)["applied"] is False
    with pytest.raises(PostgresBackendError):
        fenced.snapshot()
    assert cluster.query(
        published, "SELECT version FROM {schema}.schema_migrations ORDER BY version"
    ) == [(2,), (3,), (4,), (6,), (7,)]


def test_intent_blocks_actions_before_capture_without_mutating_workflow(
    published: PublicationRuntimeStore,
    bundle: SourceBundle,
    clock: list[float],
    cluster: Cluster,
) -> None:
    fingerprint = approve(published)
    before = published.snapshot()
    outgoing = envelope(bundle, clock)
    first = published.enqueue_source(bundle, outgoing, publisher(published))
    assert not first["duplicate"]
    assert published.enqueue_source(bundle, outgoing, publisher(published))["duplicate"]
    assert published.snapshot() == before
    assert published.transport.verify_history(published.transport_binding)["captured"] == [0, 0]
    with pytest.raises(StoreError) as denied:
        approve(published, "rec-2")
    assert denied.value.code == "publication_pending"
    with pytest.raises(StoreError) as denied:
        published.execute("rec-1", actor("approver"), fingerprint, "not-published")
    assert denied.value.code == "publication_pending"
    published.decide("rec-1", actor("approver"), "revoke", fingerprint)
    assert cluster.query(published, "SELECT COUNT(*) FROM {schema}.runtime_source_bundles") == [
        (1,)
    ]
    assert cluster.query(published, "SELECT COUNT(*) FROM {schema}.runtime_publications") == [(1,)]


def test_failed_intent_rolls_back_bundle_and_outbox(
    published: PublicationRuntimeStore, bundle: SourceBundle, clock: list[float], cluster: Cluster
) -> None:
    def fail(stage: str) -> None:
        if stage == "publication_enqueued":
            raise RuntimeError("injected intent failure")

    published._failure_hook = fail
    with pytest.raises(RuntimeError, match="intent failure"):
        published.enqueue_source(bundle, envelope(bundle, clock), publisher(published))
    published._failure_hook = None
    for table in ("runtime_source_bundles", "runtime_publications"):
        assert cluster.query(published, "SELECT COUNT(*) FROM {schema}." + table) == [(0,)]
    approve(published)


def test_concurrent_duplicate_enqueue_has_one_sequence(
    published: PublicationRuntimeStore, bundle: SourceBundle, clock: list[float], cluster: Cluster
) -> None:
    outgoing, principal = envelope(bundle, clock), publisher(published)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda _: published.enqueue_source(bundle, outgoing, principal), range(2))
        )
    assert sum(not r["duplicate"] for r in results) == 1
    assert {r["sequence"] for r in results} == {1}
    assert cluster.query(published, "SELECT COUNT(*) FROM {schema}.runtime_publications") == [(1,)]


@pytest.mark.parametrize("role", ["planner", "approver", "auditor", "worker"])
def test_only_explicit_publisher_identity_can_enqueue(
    published: PublicationRuntimeStore, bundle: SourceBundle, clock: list[float], role: str
) -> None:
    with pytest.raises(StoreError) as denied:
        published.enqueue_source(bundle, envelope(bundle, clock), actor(role))
    assert denied.value.code == "role_denied"


def test_conflicts_and_unregistered_admission_fail_closed(
    published: PublicationRuntimeStore, bundle: SourceBundle, clock: list[float]
) -> None:
    outgoing = envelope(bundle, clock)
    with pytest.raises(StoreError, match="requires retained source"):
        published.control.submit_broker(outgoing, broker_actor(published))
    published.enqueue_source(bundle, outgoing, publisher(published))
    changed = outgoing.model_copy(update={"event_type": "demand.spike"})
    with pytest.raises(StoreError, match="another publication"):
        published.enqueue_source(bundle, changed, publisher(published))
    with pytest.raises(StoreError, match="differs from its retained"):
        published.control.submit_broker(changed, broker_actor(published))
    with pytest.raises(StoreError, match="changed source bytes"):
        published.enqueue_source(bundle, envelope(bundle, clock), publisher(published))
    assert published.control.snapshot(actor())["event_count"] == 0


def test_publications_cannot_be_admitted_out_of_order_or_through_human_api(
    published: PublicationRuntimeStore,
    bundle: SourceBundle,
    clock: list[float],
    tmp_path: Path,
) -> None:
    first = envelope(bundle, clock)
    published.enqueue_source(bundle, first, publisher(published))
    generate_r2_smoke_fixture(tmp_path / "second-source", seed=123)
    second_bundle = SourceBundle.capture(tmp_path / "second-source")
    second = envelope(second_bundle, clock)
    published.enqueue_source(second_bundle, second, publisher(published))
    with pytest.raises(StoreError) as denied:
        published.control.submit(first, actor())
    assert denied.value.code == "publication_delivery_required"
    with pytest.raises(StoreError) as denied:
        published.control.submit_broker(second, broker_actor(published))
    assert denied.value.code == "publication_order_gap"
    assert published.publication_status(actor())["pending_admission"] == 2
    published.control.submit_broker(first, broker_actor(published))
    assert published.publication_status(actor())["pending_admission"] == 1
    published.control.submit_broker(second, broker_actor(published))
    assert published.publication_status(actor())["pending_admission"] == 0
    assert published.publication_status(actor())["broker_acknowledged"] == 0
    with pytest.raises(StoreError):
        published.publication_status(actor("worker"))


def test_retained_bytes_recover_and_dispatch_admits_exact_publication(
    published: PublicationRuntimeStore, bundle: SourceBundle, clock: list[float], tmp_path: Path
) -> None:
    outgoing = envelope(bundle, clock)
    published.enqueue_source(bundle, outgoing, publisher(published))
    (tmp_path / "producer/source-manifest.json").write_text("producer changed", encoding="utf-8")
    restarted = PublicationRuntimeStore(
        published.config,
        transport=published.transport,
        binding=published.transport_binding,
        runtime_root=published.runtime_root,
        clock=published.clock,
    )
    restored = restarted.materialize_source(bundle.source_sha256)
    assert runtime_source_fingerprint(restored) == bundle.source_sha256
    restarted.transport.capture(
        restarted.transport_binding,
        [TransportRecord(0, 0, b"portfolio", canonical(outgoing.model_dump(mode="json")).encode())],
    )
    first = restarted.transport.dispatch_once(restarted.transport_binding, restarted.control)
    assert first and first["admission"]["duplicate"] is False
    assert restarted.control.snapshot(actor())["event_count"] == 1
    assert not restarted.snapshot()["effects"]
    with pytest.raises(StoreError):
        approve(restarted)


def test_runtime_cannot_rewrite_source_intent_or_receipt(
    published: PublicationRuntimeStore, bundle: SourceBundle, clock: list[float], cluster: Cluster
) -> None:
    published.enqueue_source(bundle, envelope(bundle, clock), publisher(published))
    for table in ("runtime_source_bundles", "runtime_publications", "runtime_publication_receipts"):
        for statement in ("DELETE FROM {schema}.", "TRUNCATE {schema}."):
            with pytest.raises(cluster.driver.errors.InsufficientPrivilege):
                cluster.query(published, statement + table, role="application")


def test_scope_and_source_bytes_must_match(
    published: PublicationRuntimeStore, bundle: SourceBundle, clock: list[float]
) -> None:
    partial = Actor("source-publisher", "Publisher", ActorRole.SYSTEM, ("WH-NORTH",))
    with pytest.raises(StoreError) as denied:
        published.enqueue_source(bundle, envelope(bundle, clock), partial)
    assert denied.value.code == "scope_denied"
    wrong = envelope(bundle, clock).model_copy(
        update={"payload": {"source_snapshot_sha256": "f" * 64}}
    )
    with pytest.raises(StoreError, match="exact source bytes"):
        published.enqueue_source(bundle, wrong, publisher(published))


@pytest.mark.parametrize(
    "failure",
    [
        "missing",
        "double",
        "error",
        "unfinished",
        "topic",
        "partition",
        "offset",
        "identity_changed",
    ],
)
def test_unconfirmed_broker_ack_never_records_a_receipt(
    published: PublicationRuntimeStore,
    bundle: SourceBundle,
    clock: list[float],
    cluster: Cluster,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """Actual PostgreSQL with an explicit broker double; not live-broker failure evidence."""
    outgoing = envelope(bundle, clock)
    published.enqueue_source(bundle, outgoing, publisher(published))

    class ProducerDouble:
        def __init__(self, settings: dict[str, Any]) -> None:
            assert settings["acks"] == "all" and settings["enable.idempotence"] is True
            assert settings["allow.auto.create.topics"] is False

        def produce(self, topic: str, **kwargs: Any) -> None:
            assert topic == published.transport_binding.broker.topic
            assert kwargs["value"] == canonical(outgoing.model_dump(mode="json")).encode()
            message = SimpleNamespace(
                topic=lambda: "wrong-topic" if failure == "topic" else topic,
                partition=lambda: 1 if failure == "partition" else 0,
                offset=lambda: -1 if failure == "offset" else 0,
            )
            if failure != "missing":
                kwargs["on_delivery"]("broker error" if failure == "error" else None, message)
            if failure == "double":
                kwargs["on_delivery"](None, message)

        def flush(self, seconds: int) -> int:
            return 1 if failure == "unfinished" else 0

    monkeypatch.setattr(
        source_publisher,
        "importlib",
        SimpleNamespace(import_module=lambda _: SimpleNamespace(Producer=ProducerDouble)),
    )
    dispatch = SourceOutboxPublisher(published)
    observations = 0

    def inspect() -> None:
        nonlocal observations
        observations += 1
        if failure == "identity_changed" and observations > 1:
            raise BrokerAdmissionError("publication_broker_identity_differs")

    monkeypatch.setattr(dispatch, "_verify_broker", inspect)
    with pytest.raises(BrokerAdmissionError):
        dispatch.publish_once()
    with pytest.raises(BrokerAdmissionError, match="restart_required"):
        dispatch.publish_once()
    assert cluster.query(
        published, "SELECT COUNT(*) FROM {schema}.runtime_publication_receipts"
    ) == [(0,)]
    assert published.publication_status(actor())["pending_admission"] == 1
    assert not published.snapshot()["effects"]
