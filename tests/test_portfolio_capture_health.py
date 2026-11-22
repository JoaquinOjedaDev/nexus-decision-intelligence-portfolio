"""Capture-health contract/refusal tests; doubles do not prove service behavior."""

from dataclasses import replace
from uuid import uuid4

import pytest
from test_portfolio_transport_consumer import Partition
from test_portfolio_transport_consumer import doubles as doubles
from test_portfolio_transport_ledger import binding

from nexus.portfolio import transport_consumer as adapter
from nexus.portfolio.broker_runtime import BrokerAdmissionError
from nexus.portfolio.capture_health import CaptureObservation, capture_health_reason
from nexus.portfolio.store import digest


def observation(**changes):
    value = CaptureObservation(
        str(uuid4()),
        str(uuid4()),
        digest(binding().payload()),
        100.0,
        101.0,
        (0, 0),
        (2, 0),
        (2, 0),
        (2, 0),
    )
    return replace(value, **changes)


def test_lifetime_includes_io_and_does_not_renew_on_read():
    value = observation()
    assert (
        capture_health_reason(binding(), value, now=101, max_age_seconds=3)
        == "caught_up_at_observation"
    )
    assert (
        capture_health_reason(binding(), value, now=103, max_age_seconds=3)
        == "capture_observation_expired"
    )
    assert (
        capture_health_reason(binding(), value, now=100.5, max_age_seconds=3)
        == "capture_clock_regressed"
    )
    assert (
        capture_health_reason(binding(), None, now=101, max_age_seconds=3)
        == "capture_observation_missing"
    )
    assert (
        capture_health_reason(
            binding(), observation(binding_sha256="f" * 64), now=101, max_age_seconds=3
        )
        == "capture_binding_changed"
    )
    assert value.sha256 == digest(value.payload())


def test_ack_debt_and_broker_lag_are_not_healthy():
    value = observation(captured=(1, 0), committed=(1, 0))
    assert value.lag == (1, 0)
    assert capture_health_reason(binding(), value, now=101, max_age_seconds=3) == "capture_lagging"
    value = observation(committed=(1, 0))
    assert (
        capture_health_reason(binding(), value, now=101, max_age_seconds=3) == "capture_ack_pending"
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"low": (3, 0)},
        {"high": (1, 0)},
        {"committed": (3, 0)},
        {"captured": (True, 0)},
        {"committed": (-1, 0)},
        {"high": (2,)},
        {"low": [0, 0]},
        {"completed_at": 99.0},
        {"started_at": float("nan")},
        {"completed_at": float("inf")},
        {"started_at": True},
        {"contract": "other"},
        {"observation_id": "not-a-uuid"},
        {"consumer_instance_id": ""},
        {"binding_sha256": "F" * 64},
        {"high": (2**63, 0)},
        {"started_at": 10**400},
    ],
)
def test_invalid_or_impossible_measurements_are_refused(changes):
    with pytest.raises(ValueError):
        observation(**changes)


@pytest.mark.parametrize("age", [True, 0, 61, 1.0])
def test_policy_must_be_explicit_and_bounded(age):
    with pytest.raises(ValueError):
        capture_health_reason(binding(), observation(), now=101, max_age_seconds=age)


@pytest.mark.parametrize(
    "now",
    [float("nan"), float("inf"), True, -1, 10**400],
    ids=["nan", "infinity", "boolean", "negative", "overflow"],
)
def test_invalid_evaluation_clock_never_reports_health(now):
    with pytest.raises(ValueError):
        capture_health_reason(binding(), observation(), now=now, max_age_seconds=3)


def test_sample_proves_lag_without_consuming_or_committing(doubles):
    consumer, ledger = doubles
    bridge = adapter.DurableBrokerConsumer(ledger, binding())
    ledger.capture_progress.return_value = (0, 0)
    consumer.commit.reset_mock()
    value = bridge.sample_health(clock=iter([100.0, 101.0]).__next__)
    assert value.consumer_instance_id == bridge.instance_id and value.lag == (10, 10)
    assert value.captured == value.committed == (0, 0)
    ledger.capture.assert_not_called()
    consumer.consume.assert_not_called()
    consumer.assign.assert_called_once()  # Only constructor reconciliation.
    consumer.commit.assert_not_called()
    assert not bridge.failed


@pytest.mark.parametrize(
    "change",
    [
        "identity",
        "ahead",
        "missing",
        "topology",
        "retention",
        "database",
        "clock",
        "watermark_shape",
        "broker",
    ],
)
def test_sampling_fault_latches_consumer(doubles, monkeypatch, change):
    consumer, ledger = doubles
    bridge = adapter.DurableBrokerConsumer(ledger, binding())
    ledger.capture_progress.return_value = (0, 0)
    consumer.commit.reset_mock()
    clock = iter([100.0, 101.0]).__next__
    if change == "identity":
        real = adapter.inspect_topic
        calls = []

        def changed(config):
            calls.append(1)
            return real(config) if len(calls) == 1 else {**real(config), "topic_id": "f" * 32}

        monkeypatch.setattr(adapter, "inspect_topic", changed)
    elif change == "ahead":
        consumer.committed.return_value[0].offset = 1
    elif change == "missing":
        consumer.committed.return_value[0].offset = -1001
    elif change == "topology":
        consumer.committed.return_value = [Partition("wrong", 0), Partition("wrong", 1)]
    elif change == "retention":
        consumer.get_watermark_offsets.return_value = (1, 10)
    elif change == "database":
        ledger.capture_progress.side_effect = RuntimeError("unavailable")
    elif change == "broker":
        consumer.get_watermark_offsets.side_effect = RuntimeError("unavailable")
    elif change == "watermark_shape":
        consumer.get_watermark_offsets.return_value = (0, 10, 99)
    else:
        clock = iter([100.0, 99.0]).__next__
    with pytest.raises((ValueError, RuntimeError, BrokerAdmissionError)):
        bridge.sample_health(clock=clock)
    assert bridge.failed
    consumer.commit.assert_not_called()
    with pytest.raises(BrokerAdmissionError, match="requires_reconciliation"):
        bridge.poll_batch()
    with pytest.raises(BrokerAdmissionError, match="requires_reconciliation"):
        bridge.sample_health()


def test_closed_consumer_cannot_issue_new_health(doubles):
    _, ledger = doubles
    bridge = adapter.DurableBrokerConsumer(ledger, binding())
    bridge.close()
    with pytest.raises(BrokerAdmissionError, match="requires_reconciliation"):
        bridge.sample_health()
