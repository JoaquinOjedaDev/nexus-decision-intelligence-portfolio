"""Opt-in real Redpanda -> PostgreSQL admission and crash-window acceptance.

Requires an explicitly supplied broker cluster ID and the existing guarded native
PostgreSQL configuration. Creates unique topics/groups and schemas, retaining all
evidence. Constructed seeds establish storage/transport semantics, not a real replan.
"""

from __future__ import annotations

import importlib
import os
import time
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import pytest
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import LOCATIONS, event
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime

from nexus.portfolio.broker_runtime import RuntimeBrokerConfig, RuntimeBrokerConsumer
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore

pytestmark = pytest.mark.portfolio_service


@dataclass
class Broker:
    module: Any
    config: RuntimeBrokerConfig
    cluster_id: str

    def consumer(self) -> Any:
        consumer = self.module.Consumer(self.config.consumer_options())
        partitions = [self.module.TopicPartition(self.config.topic, i) for i in range(2)]
        committed = consumer.committed(partitions, timeout=5)
        assert all(p.error is None and p.offset >= 0 for p in committed)
        consumer.assign(committed)
        return consumer

    def produce(self, payload: bytes) -> None:
        producer = self.module.Producer(
            {
                "bootstrap.servers": self.config.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "message.timeout.ms": 10000,
            }
        )
        results: list[Any] = []
        producer.produce(
            self.config.topic,
            key=b"portfolio",
            value=payload,
            partition=0,
            on_delivery=lambda error, message: results.append((error, message.offset())),
        )
        assert producer.flush(12) == 0
        assert len(results) == 1 and results[0][0] is None and results[0][1] >= 0


@pytest.fixture
def broker() -> Broker:
    endpoint = os.environ.get("NEXUS_PORTFOLIO_BROKER_ENDPOINT")
    identity = os.environ.get("NEXUS_PORTFOLIO_BROKER_CLUSTER_ID")
    if not endpoint or not identity:
        pytest.skip("Explicit isolated broker endpoint and cluster identity are required")
    module = importlib.import_module("confluent_kafka")
    admin_module = importlib.import_module("confluent_kafka.admin")
    suffix = uuid4().hex
    config = RuntimeBrokerConfig(
        endpoint, f"nexus-runtime-test-{suffix}", f"nexus-runtime-plan-{suffix}", LOCATIONS
    )
    admin = admin_module.AdminClient({"bootstrap.servers": endpoint, "socket.timeout.ms": 5000})
    metadata = admin.list_topics(timeout=5)
    assert metadata.cluster_id == identity, "Refusing unrelated broker"
    assert config.topic not in metadata.topics
    created = admin.create_topics(
        [
            admin_module.NewTopic(
                config.topic,
                num_partitions=2,
                replication_factor=1,
                config={"cleanup.policy": "delete", "retention.ms": "604800000"},
            )
        ],
        request_timeout=10,
    )
    created[config.topic].result(12)
    consumer = module.Consumer(config.consumer_options())
    try:
        partitions = [module.TopicPartition(config.topic, i, 0) for i in range(2)]
        # Initialization is allowed only for this just-created, proven-empty topic.
        assert all(consumer.get_watermark_offsets(p, timeout=5) == (0, 0) for p in partitions)
        assert all(p.offset < 0 for p in consumer.committed(partitions, timeout=5))
        result = consumer.commit(offsets=partitions, asynchronous=False)
        assert all(p.error is None and p.offset == 0 for p in result)
    finally:
        consumer.close()
    return Broker(module, config, identity)


def poll(bridge: RuntimeBrokerConsumer) -> dict[str, Any]:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        result = bridge.poll_once()
        if result is not None:
            return result
    raise AssertionError("No broker admission within 15 seconds")


def test_real_broker_event_is_durable_before_offset_and_duplicate_is_idempotent(
    broker: Broker, runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    payload = event(clock[0]).model_copy(update={"source": "nexus.source-snapshot.v1"})
    broker.produce(payload.model_dump_json().encode())
    bridge = RuntimeBrokerConsumer(runtime.control, broker.config, broker.consumer())
    try:
        first = poll(bridge)
        assert first["next_offset"] == 1 and not first["admission"]["duplicate"]
        state = runtime.snapshot()
        broker.produce(payload.model_dump_json().encode())
        second = poll(bridge)
        assert second["next_offset"] == 2 and second["admission"]["duplicate"]
        assert second["admission"]["job_id"] == first["admission"]["job_id"]
        assert runtime.snapshot() == state
    finally:
        bridge.close()


def test_real_broker_crash_after_database_commit_replays_without_second_job(
    broker: Broker, runtime: PostgresRuntimeStore, clock: list[float]
) -> None:
    broker.produce(
        event(clock[0])
        .model_copy(update={"source": "nexus.source-snapshot.v1"})
        .model_dump_json()
        .encode()
    )
    underlying = broker.consumer()

    class InterruptedCommit:
        def poll(self, timeout: float) -> Any:
            return underlying.poll(timeout)

        def commit(self, **kwargs: Any) -> Any:
            raise RuntimeError("injected after PostgreSQL commit before Kafka commit")

        def close(self) -> None:
            underlying.close()

    failing = RuntimeBrokerConsumer(runtime.control, broker.config, InterruptedCommit())
    try:
        with pytest.raises(RuntimeError, match="injected after PostgreSQL"):
            poll(failing)
        durable = runtime.snapshot()
        offsets = underlying.committed(
            [broker.module.TopicPartition(broker.config.topic, 0)], timeout=5
        )
        assert offsets[0].offset == 0
    finally:
        failing.close()
    restarted = RuntimeBrokerConsumer(runtime.control, broker.config, broker.consumer())
    try:
        replayed = poll(restarted)
        assert replayed["admission"]["duplicate"] and replayed["next_offset"] == 1
        assert runtime.snapshot() == durable
    finally:
        restarted.close()
