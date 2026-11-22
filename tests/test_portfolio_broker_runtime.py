"""Portable transport contract tests; mocks are not real-broker acceptance evidence."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_portfolio_postgres_runtime import event

from nexus.portfolio.broker_runtime import (
    BrokerAdmissionError,
    RuntimeBrokerConfig,
    RuntimeBrokerConsumer,
)


def configuration() -> RuntimeBrokerConfig:
    return RuntimeBrokerConfig(
        "127.0.0.1:19092", "nexus-runtime-snapshots-v1", "nexus-runtime-plan-v1", ("WH-NORTH",)
    )


def harness() -> tuple[RuntimeBrokerConsumer, Mock, Mock, Mock]:
    control, transport, message = Mock(), Mock(), Mock()
    config = configuration()
    message.error.return_value = None
    message.topic.return_value = config.topic
    message.partition.return_value = 0
    message.offset.return_value = 4
    message.key.return_value = b"portfolio"
    message.value.return_value = (
        event().model_copy(update={"source": "nexus.source-snapshot.v1"}).model_dump_json().encode()
    )
    transport.poll.return_value = message
    transport.commit.return_value = [
        SimpleNamespace(topic=config.topic, partition=0, offset=5, error=None)
    ]
    control.submit_broker.return_value = {"duplicate": False, "job_id": "job", "invalidated": 2}
    return RuntimeBrokerConsumer(control, config, transport), control, transport, message


def test_configuration_disables_implicit_ack_creation_and_offset_reset() -> None:
    config = configuration().consumer_options()
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["allow.auto.create.topics"] is False
    assert config["auto.offset.reset"] == "error"
    assert config["isolation.level"] == "read_committed"


@pytest.mark.parametrize(
    "endpoint",
    [
        "localhost:9092",
        "remote:9092",
        "127.0.0.1:0",
        "127.0.0.1:65536",
        "127.0.0.1:9092,remote:9092",
    ],
)
def test_configuration_refuses_nonlocal_or_invalid_endpoints(endpoint: str) -> None:
    with pytest.raises(ValueError):
        replace(configuration(), bootstrap_servers=endpoint)


@pytest.mark.parametrize(
    "field,value",
    [
        ("topic", "unrelated"),
        ("group_id", "unrelated"),
        ("locations", ()),
        ("locations", ("x", "x")),
        ("locations", (" ",)),
    ],
)
def test_configuration_requires_explicit_scoped_identity(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        replace(configuration(), **{field: value})


def test_durable_admission_precedes_synchronous_exact_offset_commit() -> None:
    bridge, control, transport, message = harness()
    order = Mock()
    order.attach_mock(control, "database")
    order.attach_mock(transport, "broker")
    receipt = bridge.poll_once()
    assert receipt is not None and receipt["next_offset"] == 5
    relevant = {"broker.poll", "database.submit_broker", "broker.commit"}
    calls = [item[0] for item in order.mock_calls if item[0] in relevant]
    assert calls == ["broker.poll", "database.submit_broker", "broker.commit"]
    transport.commit.assert_called_once_with(message=message, asynchronous=False)
    assert control.submit_broker.call_args.args[1].actor_id == "runtime-event-consumer"
    assert receipt["claim"] == "durable_runtime_admission_not_completed_replan"


@pytest.mark.parametrize("stage", ["poll", "admission", "commit"])
def test_exception_stops_consumer_and_never_skips_to_a_later_record(stage: str) -> None:
    bridge, control, transport, _ = harness()
    target = {
        "poll": transport.poll,
        "admission": control.submit_broker,
        "commit": transport.commit,
    }[stage]
    target.side_effect = RuntimeError("injected")
    with pytest.raises(RuntimeError, match="injected"):
        bridge.poll_once()
    if stage != "commit":
        transport.commit.assert_not_called()
    before = transport.poll.call_count
    with pytest.raises(BrokerAdmissionError, match="requires_reconciliation"):
        bridge.poll_once()
    assert transport.poll.call_count == before


@pytest.mark.parametrize(
    "method,value",
    [
        ("error", "failure"),
        ("topic", "unrelated"),
        ("partition", -1),
        ("offset", -1),
        ("value", None),
        ("value", b""),
        ("value", b"x" * 65537),
        ("value", b"not-json"),
        ("key", b"another-scope"),
    ],
    ids=["error", "topic", "partition", "offset", "null", "empty", "oversize", "json", "key"],
)
def test_invalid_message_is_never_admitted_or_committed(method: str, value: object) -> None:
    bridge, control, transport, message = harness()
    getattr(message, method).return_value = value
    with pytest.raises(BrokerAdmissionError):
        bridge.poll_once()
    control.submit_broker.assert_not_called()
    transport.commit.assert_not_called()
    assert bridge.failed


@pytest.mark.parametrize(
    "result",
    [
        None,
        [],
        [SimpleNamespace(topic="wrong", partition=0, offset=5, error=None)],
        [
            SimpleNamespace(
                topic="nexus-runtime-snapshots-v1", partition=0, offset=5, error="failure"
            )
        ],
    ],
)
def test_unconfirmed_offset_commit_requires_reconciliation(result: object) -> None:
    bridge, control, transport, _ = harness()
    transport.commit.return_value = result
    with pytest.raises(BrokerAdmissionError, match="commit_unconfirmed"):
        bridge.poll_once()
    control.submit_broker.assert_called_once()
    assert bridge.failed


def test_idle_poll_and_explicit_close_do_not_admit_or_commit() -> None:
    bridge, control, transport, _ = harness()
    transport.poll.return_value = None
    assert bridge.poll_once() is None
    bridge.close()
    control.submit_broker.assert_not_called()
    transport.commit.assert_not_called()
    transport.close.assert_called_once()
    assert bridge.failed
