"""Two real independent Redpanda groups, separate PostgreSQL ledgers, one business path."""

import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from test_portfolio_broker_runtime_service import Broker
from test_portfolio_broker_runtime_service import broker as broker
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import actor, event
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime

from nexus.portfolio.broker_runtime import BrokerAdmissionError
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.transport_consumer import DurableBrokerConsumer, inspect_topic
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger, runtime_target

pytestmark = pytest.mark.portfolio_service


def setup(
    cluster: Cluster,
    broker: Broker,
    runtime: PostgresRuntimeStore,
    path: Path,
    *,
    audit: bool = False,
) -> tuple[TransportLedger, TransportBinding]:
    config = replace(
        broker.config, group_id=broker.config.group_id + ("-audit" if audit else "-ops")
    )
    identity = inspect_topic(config)
    assert identity["cluster_id"] == broker.cluster_id
    binding = TransportBinding(
        config,
        identity["cluster_id"],
        identity["topic_id"],
        2,
        "audit" if audit else "operations",
        None if audit else runtime_target(runtime.control),
    )
    ledger = TransportLedger(cluster.backend(path).config)
    cluster.migrate(ledger)
    ledger.register(binding)
    return ledger, binding


def capture(consumer: DurableBrokerConsumer, count: int) -> list[dict[str, Any]]:
    deadline, total = time.monotonic() + 20, 0
    receipts = []
    while total < count and time.monotonic() < deadline:
        receipt = consumer.poll_batch()
        if receipt is not None:
            receipts.append(receipt)
            total += receipt["inserted"] + receipt["duplicates"]
    assert total == count
    return receipts


def test_two_real_groups_capture_same_records_independently_then_dispatch_once(
    cluster: Cluster,
    broker: Broker,
    runtime: PostgresRuntimeStore,
    clock: list[float],
    tmp_path: Path,
) -> None:
    ops, ops_binding = setup(cluster, broker, runtime, tmp_path / "ops")
    audit, audit_binding = setup(cluster, broker, runtime, tmp_path / "audit", audit=True)
    one = DurableBrokerConsumer(ops, ops_binding)
    two = DurableBrokerConsumer(audit, audit_binding)
    try:
        payload = event(clock[0]).model_copy(update={"source": "nexus.source-snapshot.v1"})
        broker.produce(payload.model_dump_json().encode())
        broker.produce(payload.model_dump_json().encode())
        producer = broker.module.Producer(
            {
                "bootstrap.servers": broker.config.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        acknowledgements = []
        producer.produce(
            broker.config.topic,
            key=b"portfolio",
            value=payload.model_dump_json().encode(),
            partition=1,
            on_delivery=lambda error, message: acknowledgements.append(error),
        )
        assert producer.flush(10) == 0 and acknowledgements == [None]
        capture(one, 3)
        assert audit.verify_history(audit_binding)["captured"] == [0, 0]
        capture(two, 3)
        assert ops.verify_history(ops_binding)["captured"] == [2, 1]
        assert audit.verify_history(audit_binding)["captured"] == [2, 1]
        query = (
            "SELECT partition,broker_offset,payload_sha256 FROM {schema}.transport_inbox "
            "ORDER BY partition,broker_offset"
        )
        assert cluster.query(ops, query) == cluster.query(audit, query)
        assert runtime.control.snapshot(actor())["event_count"] == 0
        first = ops.dispatch_once(ops_binding, runtime.control)
        second = ops.dispatch_once(ops_binding, runtime.control)
        third = ops.dispatch_once(ops_binding, runtime.control)
        assert first is not None and second is not None and third is not None
        assert not first["admission"]["duplicate"] and second["admission"]["duplicate"]
        assert first["admission"]["job_id"] == second["admission"]["job_id"]
        assert third["admission"]["duplicate"] is True
        assert third["admission"]["job_id"] == first["admission"]["job_id"]
        assert ops.verify_history(ops_binding)["delivered"] == [2, 1]
        assert audit.verify_history(audit_binding)["delivered"] == []
        assert runtime.control.snapshot(actor())["event_count"] == 1
        for consumer in (one, two):
            offsets = consumer.consumer.committed(
                [broker.module.TopicPartition(broker.config.topic, p) for p in range(2)], timeout=5
            )
            assert [p.offset for p in offsets] == [2, 1]
    finally:
        one.close()
        two.close()


def test_restart_reconciles_database_commit_before_broker_ack(
    cluster: Cluster,
    broker: Broker,
    runtime: PostgresRuntimeStore,
    clock: list[float],
    tmp_path: Path,
) -> None:
    ledger, binding = setup(cluster, broker, runtime, tmp_path / "ops")
    consumer = DurableBrokerConsumer(ledger, binding)
    raw = consumer.consumer

    class FailedCommit:
        def consume(self, **kwargs: Any) -> Any:
            return raw.consume(**kwargs)

        def commit(self, **kwargs: Any) -> Any:
            raise RuntimeError("injected broker acknowledgement failure")

        def close(self) -> None:
            raw.close()

    consumer.consumer = FailedCommit()
    try:
        broker.produce(
            event(clock[0])
            .model_copy(update={"source": "nexus.source-snapshot.v1"})
            .model_dump_json()
            .encode()
        )
        with pytest.raises(RuntimeError, match="acknowledgement"):
            capture(consumer, 1)
        assert ledger.verify_history(binding)["captured"] == [1, 0]
        offsets = raw.committed([broker.module.TopicPartition(broker.config.topic, 0)], timeout=5)
        assert offsets[0].offset == 0
        with pytest.raises(BrokerAdmissionError, match="requires_reconciliation"):
            consumer.poll_batch()
    finally:
        consumer.close()
    restarted = DurableBrokerConsumer(ledger, binding)
    try:
        offsets = restarted.consumer.committed(
            [broker.module.TopicPartition(broker.config.topic, 0)], timeout=5
        )
        assert offsets[0].offset == 1
        assert restarted.poll_batch() is None
        assert ledger.dispatch_once(binding, runtime.control) is not None
    finally:
        restarted.close()


def test_topic_or_cluster_binding_mismatch_refuses_before_capture(
    cluster: Cluster,
    broker: Broker,
    runtime: PostgresRuntimeStore,
    tmp_path: Path,
) -> None:
    ledger, binding = setup(cluster, broker, runtime, tmp_path / "ops")
    for wrong in (
        replace(binding, topic_id="f" * 32),
        replace(binding, cluster_id="wrong-cluster"),
    ):
        with pytest.raises(BrokerAdmissionError, match="binding_changed"):
            DurableBrokerConsumer(ledger, wrong)
    assert ledger.verify_history(binding)["captured"] == [0, 0]
