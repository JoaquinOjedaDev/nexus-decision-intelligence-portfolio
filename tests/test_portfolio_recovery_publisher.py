"""Deterministic broker doubles; not live ACK or persistent redrive evidence."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from nexus.portfolio import recovery_publisher as module
from nexus.portfolio.broker_runtime import BrokerAdmissionError, RuntimeBrokerConfig
from nexus.portfolio.recovery_publisher import RecoveryEvidencePublisher, RecoveryTopic
from nexus.portfolio.transport_ledger import TransportBinding


@pytest.fixture
def topic() -> RecoveryTopic:
    source = TransportBinding(
        RuntimeBrokerConfig("127.0.0.1:19092", "nexus-runtime-source", "nexus-runtime-ops", ("A",)),
        "cluster-one",
        "1" * 32,
        2,
        "operations",
        "a" * 64,
    )
    return RecoveryTopic(
        source,
        replace(source.broker, topic="nexus-runtime-dlq", group_id="nexus-runtime-readback"),
        "2" * 32,
        "dead_letter",
    )


@pytest.fixture
def broker(monkeypatch: pytest.MonkeyPatch, topic: RecoveryTopic) -> SimpleNamespace:
    state = SimpleNamespace(
        mismatch=None,
        ack_error=None,
        outstanding=0,
        callbacks=1,
        closed=False,
        produced=0,
        retention=topic.payload()["retention"].copy(),
        identity_calls=0,
    )

    class Message:
        def topic(self):
            return "nexus-runtime-other" if state.mismatch == "topic" else topic.broker.topic

        def partition(self):
            return 1 if state.mismatch == "partition" else 0

        def offset(self):
            return -1 if state.mismatch == "offset" else 7

        def error(self):
            return "unavailable" if state.mismatch == "error" else None

        def key(self):
            return b"wrong" if state.mismatch == "key" else b"a" * 64

        def value(self):
            return b"wrong" if state.mismatch == "value" else b"exact evidence\x00\xff"

        def headers(self):
            return [("injected", b"x")] if state.mismatch == "headers" else None

    class Producer:
        def __init__(self, options):
            assert options["acks"] == "all" and options["enable.idempotence"]
            assert not options["allow.auto.create.topics"]

        def produce(self, name, **options):
            assert name == topic.broker.topic and options["partition"] == 0
            state.produced += 1
            for _ in range(state.callbacks):
                options["on_delivery"](state.ack_error, Message())

        def flush(self, timeout):
            return state.outstanding

    class Consumer:
        def __init__(self, options):
            assert not options["enable.auto.commit"] and not options["enable.auto.offset.store"]

        def assign(self, partitions):
            assert partitions == [(topic.broker.topic, 0, 7)]

        def poll(self, timeout):
            return Message()

        def close(self):
            state.closed = True

    class ConfigResource:
        Type = SimpleNamespace(TOPIC=2)

        def __init__(self, kind, name):
            self.name = name

    class Admin:
        def __init__(self, options):
            pass

        def describe_configs(self, resources, **options):
            return {
                r: SimpleNamespace(
                    result=lambda timeout: {
                        k: SimpleNamespace(value=v) for k, v in state.retention.items()
                    }
                )
                for r in resources
            }

    def inspect(config):
        state.identity_calls += 1
        return {
            "cluster_id": "changed" if state.mismatch == "identity" else topic.source.cluster_id,
            "topic_id": topic.topic_id,
            "partitions": [0],
        }

    monkeypatch.setattr(module, "inspect_topic", inspect)
    monkeypatch.setattr(
        module.importlib,
        "import_module",
        lambda name: (
            SimpleNamespace(AdminClient=Admin, ConfigResource=ConfigResource)
            if name.endswith(".admin")
            else SimpleNamespace(
                Producer=Producer, Consumer=Consumer, TopicPartition=lambda *args: args
            )
        ),
    )
    return state


def test_exact_readback_has_no_business_authority(topic, broker):
    result = RecoveryEvidencePublisher(topic).publish("a" * 64, b"exact evidence\x00\xff")
    assert result["offset"] == 7 and result["intent_id"] == "a" * 64
    assert result["claim"] == "broker_ack_and_exact_readback_not_recovery_authorization"
    assert broker.closed and broker.identity_calls == 2


@pytest.mark.parametrize(
    "mismatch", ["topic", "partition", "offset", "error", "key", "value", "headers", "identity"]
)
def test_changed_ack_or_readback_fail_stops(topic, broker, mismatch):
    broker.mismatch = mismatch
    publisher = RecoveryEvidencePublisher(topic)
    with pytest.raises(BrokerAdmissionError):
        publisher.publish("a" * 64, b"exact evidence\x00\xff")
    with pytest.raises(BrokerAdmissionError, match="restart_required"):
        publisher.publish("a" * 64, b"exact evidence\x00\xff")


@pytest.mark.parametrize(
    "option,value",
    [("ack_error", "failed"), ("outstanding", 1), ("callbacks", 0), ("callbacks", 2)],
)
def test_unconfirmed_ack_never_returns_receipt(topic, broker, option, value):
    setattr(broker, option, value)
    with pytest.raises(BrokerAdmissionError, match="ack_unconfirmed"):
        RecoveryEvidencePublisher(topic).publish("a" * 64, b"exact evidence\x00\xff")


@pytest.mark.parametrize("name", ["retention.ms", "retention.bytes", "cleanup.policy"])
def test_retention_refused_before_produce(topic, broker, name):
    broker.retention[name] = "wrong"
    with pytest.raises(BrokerAdmissionError, match="retention_differs"):
        RecoveryEvidencePublisher(topic).publish("a" * 64, b"exact evidence\x00\xff")
    assert broker.produced == 0


def test_source_topic_cannot_be_reused(topic):
    with pytest.raises(ValueError):
        replace(topic, broker=topic.source.broker)


@pytest.mark.parametrize(
    "intent,payload",
    [("bad", b"x"), ("a" * 64, b""), ("a" * 64, b"x" * (512 * 1024 + 1))],
    ids=["invalid-id", "empty-payload", "oversized-payload"],
)
def test_invalid_intent_is_rejected_without_broker_io(topic, intent, payload):
    with pytest.raises(ValueError):
        RecoveryEvidencePublisher(topic).publish(intent, payload)


def test_identity_changed_after_ack_does_not_return_receipt(topic, broker, monkeypatch):
    inspect = module.inspect_topic

    def changed(config):
        result = inspect(config)
        if broker.identity_calls == 2:
            result["topic_id"] = "3" * 32
        return result

    monkeypatch.setattr(module, "inspect_topic", changed)
    with pytest.raises(BrokerAdmissionError, match="identity_differs"):
        RecoveryEvidencePublisher(topic).publish("a" * 64, b"exact evidence\x00\xff")
    assert broker.closed and broker.produced == 1


def test_readback_timeout_closes_reader_and_fail_stops(topic, broker, monkeypatch):
    times = iter([0, 11])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(times))
    publisher = RecoveryEvidencePublisher(topic)
    with pytest.raises(BrokerAdmissionError, match="readback_unavailable"):
        publisher.publish("a" * 64, b"exact evidence\x00\xff")
    assert publisher.failed and broker.closed
