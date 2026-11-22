"""Experimental local broker-to-runtime admission, with commit-after-durability.

This is a single operational consumer, not the complete two-group transport gate.
It never creates topics, resets offsets, skips poison records or performs approval.
Source files must already have been published as a checksum-bound snapshot; an
accepted message queues computation rather than asserting that computation passed.
"""

from __future__ import annotations

import importlib
import re
from dataclasses import dataclass
from typing import Any

from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_runtime import PostgresRuntimeControl


class BrokerAdmissionError(RuntimeError):
    """A bounded reason code; never includes raw payloads or connection secrets."""


def decode_runtime_event(value: bytes | None, key: bytes | None) -> EventEnvelope:
    if not isinstance(value, bytes) or not 0 < len(value) <= 65536:
        raise BrokerAdmissionError("invalid_payload_size")
    try:
        event = EventEnvelope.model_validate_json(value)
    except ValueError:
        raise BrokerAdmissionError("invalid_event_envelope") from None
    if (
        event.contract_version != "1.0"
        or event.event_version != 1
        or event.source != "nexus.source-snapshot.v1"
        or event.subject_id != "portfolio"
        or key != b"portfolio"
    ):
        raise BrokerAdmissionError("unexpected_event_contract")
    return event


@dataclass(frozen=True)
class RuntimeBrokerConfig:
    """Explicit loopback-only sandbox contract, with no arbitrary client overrides."""

    bootstrap_servers: str
    topic: str
    group_id: str
    locations: tuple[str, ...]

    def __post_init__(self) -> None:
        endpoint = re.fullmatch(r"127\.0\.0\.1:([0-9]{1,5})", self.bootstrap_servers)
        if endpoint is None or not 1024 < int(endpoint[1]) < 65536:
            raise ValueError("A non-default explicit loopback broker endpoint is required")
        for value in (self.topic, self.group_id):
            if not re.fullmatch(r"nexus-runtime-[a-z0-9][a-z0-9.-]{0,100}", value):
                raise ValueError("Topic and group must be bounded nexus-runtime names")
        if (
            not isinstance(self.locations, tuple)
            or not self.locations
            or any(not isinstance(item, str) or not item.strip() for item in self.locations)
            or len(set(self.locations)) != len(self.locations)
        ):
            raise ValueError("Declare unique nonempty locations explicitly")

    def consumer_options(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "error",
            "allow.auto.create.topics": False,
            "isolation.level": "read_committed",
            "socket.timeout.ms": 10000,
        }


class RuntimeBrokerConsumer:
    """Serial fail-stop delivery; a failure requires a new consumer and reconciliation.

    Kafka and PostgreSQL do not share a transaction. A crash after admission but
    before Kafka commit replays the immutable event ID through runtime deduplication.
    Offsets must be initialized and reconciled separately before starting; absence
    of offsets is an error, never an implicit jump to the latest record.
    """

    def __init__(
        self, control: PostgresRuntimeControl, config: RuntimeBrokerConfig, consumer: Any
    ) -> None:
        self.control = control
        self.config = config
        self.consumer = consumer
        self.actor = Actor(
            "runtime-event-consumer", "Runtime event consumer", ActorRole.SYSTEM, config.locations
        )
        self.failed = False

    @classmethod
    def connect(
        cls, control: PostgresRuntimeControl, config: RuntimeBrokerConfig
    ) -> RuntimeBrokerConsumer:
        """Load the optional transport only when explicitly connecting."""
        module = importlib.import_module("confluent_kafka")
        consumer = module.Consumer(config.consumer_options())
        try:
            consumer.subscribe([config.topic])
        except BaseException:
            consumer.close()
            raise
        return cls(control, config, consumer)

    def poll_once(self) -> dict[str, Any] | None:
        if self.failed:
            raise BrokerAdmissionError("consumer_requires_reconciliation")
        try:
            message = self.consumer.poll(1.0)
            if message is None:
                return None
            if message.error() is not None:
                raise BrokerAdmissionError("broker_poll_error")
            topic, partition, offset = message.topic(), message.partition(), message.offset()
            if (
                topic != self.config.topic
                or type(partition) is not int
                or partition < 0
                or type(offset) is not int
                or offset < 0
            ):
                raise BrokerAdmissionError("invalid_transport_identity")
            event = decode_runtime_event(message.value(), message.key())
            admitted = self.control.submit_broker(event, self.actor)
            # submit_broker has exited its transaction before acknowledging transport.
            committed = self.consumer.commit(message=message, asynchronous=False)
            if (
                not isinstance(committed, list)
                or len(committed) != 1
                or committed[0].error is not None
                or (committed[0].topic, committed[0].partition, committed[0].offset)
                != (topic, partition, offset + 1)
            ):
                raise BrokerAdmissionError("broker_commit_unconfirmed")
            return {
                "topic": topic,
                "partition": partition,
                "next_offset": offset + 1,
                "event_id": str(event.event_id),
                "admission": admitted,
                "claim": "durable_runtime_admission_not_completed_replan",
            }
        except BaseException:
            self.failed = True
            raise

    def close(self) -> None:
        self.failed = True
        self.consumer.close()
