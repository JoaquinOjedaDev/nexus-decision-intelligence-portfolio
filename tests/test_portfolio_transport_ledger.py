"""Portable physical-record and immutable binding contracts; no services used."""

from dataclasses import replace
from typing import Any

import pytest

from nexus.portfolio.broker_runtime import RuntimeBrokerConfig
from nexus.portfolio.postgres_backend import PostgresBackendError
from nexus.portfolio.transport_ledger import TransportBinding, TransportRecord


def binding() -> TransportBinding:
    return TransportBinding(
        RuntimeBrokerConfig(
            "127.0.0.1:19092", "nexus-runtime-topic", "nexus-runtime-group", ("LOC-1",)
        ),
        "cluster-one",
        "topic-one",
        2,
        "audit",
    )


@pytest.mark.parametrize(
    "change",
    [
        {"cluster_id": ""},
        {"topic_id": "../bad"},
        {"partition_count": True},
        {"partition_count": 0},
        {"partition_count": 33},
        {"purpose": "approve"},
        {"runtime_target_sha256": "a" * 64},
        {"purpose": "operations"},
        {"purpose": "operations", "runtime_target_sha256": "bad"},
    ],
)
def test_invalid_binding_refused(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(binding(), **change)


@pytest.mark.parametrize(
    "change",
    [
        {"partition": True},
        {"partition": -1},
        {"partition": 32},
        {"offset": False},
        {"offset": -1},
        {"offset": 2**63 - 1},
        {"value": "not bytes"},
        {"key": b"x" * 65537},
        {"value": b"x" * 65537},
        {"headers": []},
        {"headers": (("", b"a"),)},
        {"headers": (("secret", "not bytes"),)},
        {"headers": (("a", b"x" * 4097),)},
        {"headers": (("a", b"x"),) * 65},
        {"timestamp_type": True},
        {"timestamp_ms": 0},
        {"timestamp_type": 1},
        {"timestamp_type": 2, "timestamp_ms": 2**63},
    ],
)
def test_invalid_physical_record_refused(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(TransportRecord(0, 0, None, b"event"), **change)


def test_binary_tombstones_header_order_duplicates_and_timestamps_round_trip() -> None:
    for record in (
        TransportRecord(0, 0, None, None),
        TransportRecord(
            1,
            10,
            b"\xff\x00",
            b"\xfe\xff",
            (("key", None), ("key", b""), ("key", b"\xff")),
            2,
            1800000000000,
        ),
    ):
        assert TransportRecord.from_payload(record.payload()) == record


@pytest.mark.parametrize(
    "change",
    [
        {"key": "not!base64"},
        {"value": 3},
        {"unknown": "ignored?"},
        {"headers": [["key", "YQ==", "extra"]]},
        {"partition": True},
    ],
)
def test_noncanonical_or_extra_record_fields_refused(change: dict[str, Any]) -> None:
    with pytest.raises(PostgresBackendError, match="encoding differs"):
        TransportRecord.from_payload({**TransportRecord(0, 0, None, None).payload(), **change})
