"""Explicit transport doubles for refusal ordering; not broker/service evidence."""

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from test_portfolio_transport_ledger import binding

from nexus.portfolio import transport_consumer as adapter
from nexus.portfolio.broker_runtime import BrokerAdmissionError


@dataclass
class Partition:
    topic: str
    partition: int
    offset: int = 0
    error: object | None = None


@pytest.fixture
def doubles(monkeypatch: pytest.MonkeyPatch) -> tuple[Mock, Mock]:
    contract = binding()
    consumer, ledger = Mock(), Mock()
    consumer.committed.return_value = [Partition(contract.broker.topic, p) for p in range(2)]
    consumer.get_watermark_offsets.return_value = (0, 10)
    consumer.commit.side_effect = lambda **kwargs: kwargs["offsets"]
    consumer.consume.return_value = []
    ledger.verify_history.return_value = {"captured": [0, 0]}
    ledger.reconcile.return_value = [0, 0]
    module = SimpleNamespace(
        Consumer=lambda options: consumer, TopicPartition=Partition, OFFSET_INVALID=-1001
    )
    monkeypatch.setattr(adapter.importlib, "import_module", lambda name: module)
    monkeypatch.setattr(
        adapter,
        "inspect_topic",
        lambda config: {
            "cluster_id": contract.cluster_id,
            "topic_id": contract.topic_id,
            "partitions": [0, 1],
        },
    )
    return consumer, ledger


def message(**changes: Any) -> Mock:
    result = Mock()
    for key, value in {
        "error": None,
        "topic": binding().broker.topic,
        "partition": 0,
        "offset": 0,
        "key": b"portfolio",
        "value": b"physical-bytes",
        "headers": [("x", None), ("x", b"\xff")],
        "timestamp": (1, 1800000000000),
        **changes,
    }.items():
        getattr(result, key).return_value = value
    return result


def test_capture_happens_before_ack_and_all_physical_fields_preserved(doubles: Any) -> None:
    consumer, ledger = doubles
    bridge = adapter.DurableBrokerConsumer(ledger, binding())
    consumer.consume.return_value = [message()]
    ledger.capture.return_value = {"next_offsets": [1, 0], "inserted": 1, "duplicates": 0}

    def commit(**kwargs: Any) -> Any:
        ledger.capture.assert_called_once()
        physical = ledger.capture.call_args.args[1][0]
        assert physical.headers == (("x", None), ("x", b"\xff"))
        assert physical.timestamp_ms == 1800000000000
        return kwargs["offsets"]

    consumer.commit.side_effect = commit
    assert bridge.poll_batch()["next_offsets"] == [1, 0]


@pytest.mark.parametrize(
    "change",
    [
        {"error": "error"},
        {"topic": "other-topic"},
        {"partition": 32},
        {"offset": -1},
        {"value": "invalid-not-bytes"},
        {"timestamp": (0, 10)},
    ],
)
def test_bad_physical_records_stop_before_database_and_ack(doubles: Any, change: Any) -> None:
    consumer, ledger = doubles
    bridge = adapter.DurableBrokerConsumer(ledger, binding())
    consumer.commit.reset_mock()
    consumer.consume.return_value = [message(**change)]
    with pytest.raises((ValueError, BrokerAdmissionError)):
        bridge.poll_batch()
    ledger.capture.assert_not_called()
    consumer.commit.assert_not_called()
    with pytest.raises(BrokerAdmissionError, match="requires_reconciliation"):
        bridge.poll_batch()


@pytest.mark.parametrize("confirmation", [None, [], [Partition("wrong", 0, 1)]])
def test_unconfirmed_broker_ack_requires_reconciliation(doubles: Any, confirmation: Any) -> None:
    consumer, ledger = doubles
    bridge = adapter.DurableBrokerConsumer(ledger, binding())
    consumer.consume.return_value = [message()]
    ledger.capture.return_value = {"next_offsets": [1, 0], "inserted": 1, "duplicates": 0}
    consumer.commit.side_effect = None
    consumer.commit.return_value = confirmation
    with pytest.raises(BrokerAdmissionError, match="unconfirmed"):
        bridge.poll_batch()
    assert bridge.failed


@pytest.mark.parametrize(("captured", "watermark"), [([1, 0], (0, 2)), ([0, 0], (1, 2))])
def test_missing_progress_never_adopts_latest_or_skips_retention(
    doubles: Any, captured: Any, watermark: Any
) -> None:
    consumer, ledger = doubles
    consumer.committed.return_value[0].offset = -1001
    consumer.get_watermark_offsets.return_value = watermark
    ledger.verify_history.return_value = {"captured": captured}
    with pytest.raises(BrokerAdmissionError, match="requires_review"):
        adapter.DurableBrokerConsumer(ledger, binding())
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_new_explicit_zero_origin_group_may_replay_available_backlog(doubles: Any) -> None:
    consumer, ledger = doubles
    consumer.committed.return_value[0].offset = -1001
    adapter.DurableBrokerConsumer(ledger, binding())
    ledger.reconcile.assert_called_once_with(binding(), [0, 0], [(0, 10), (0, 10)])
    assert [p.offset for p in consumer.assign.call_args.args[0]] == [0, 0]


def test_database_failure_never_commits_broker_progress(doubles: Any) -> None:
    consumer, ledger = doubles
    bridge = adapter.DurableBrokerConsumer(ledger, binding())
    consumer.commit.reset_mock()
    consumer.consume.return_value = [message()]
    ledger.capture.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError):
        bridge.poll_batch()
    consumer.commit.assert_not_called()
    assert bridge.failed
