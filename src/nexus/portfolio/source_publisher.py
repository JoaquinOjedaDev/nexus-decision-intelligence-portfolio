"""Fail-stop, at-least-once delivery from the retained source outbox to the pinned broker."""

from __future__ import annotations

import hashlib
import importlib
from collections.abc import Callable
from typing import Any

from nexus.portfolio.broker_runtime import BrokerAdmissionError
from nexus.portfolio.postgres_backend import _dsn
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.store import canonical, digest
from nexus.portfolio.transport_consumer import inspect_topic


class SourceOutboxPublisher:
    def __init__(self, store: PublicationRuntimeStore) -> None:
        self.store = store
        self.failed = False

    def _verify_broker(self) -> None:
        binding = self.store.transport_binding
        if inspect_topic(binding.broker) != {
            "cluster_id": binding.cluster_id,
            "topic_id": binding.topic_id,
            "partitions": list(range(binding.partition_count)),
        }:
            raise BrokerAdmissionError("publication_broker_identity_differs")

    def publish_once(self, *, failpoint: Callable[[], None] | None = None) -> dict[str, Any] | None:
        if self.failed:
            raise BrokerAdmissionError("publication_restart_required")
        store, binding = self.store, self.store.transport_binding
        app = _dsn(store.config.dsn)
        lock = int.from_bytes(
            hashlib.sha256((store.config.schema + ":source-publisher").encode()).digest()[:8],
            "big",
            signed=True,
        )
        try:
            with store._connection(app) as db, db.transaction():
                store._verify_schema(db, store._migration_bytes()[1], app["user"])
                store._check_application_role(db, app["user"])
                # Separate publisher lock, never the workflow guard during network I/O.
                db.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
                row = store._query(
                    db,
                    "SELECT p.* FROM {p} p LEFT JOIN {r} r ON r.id=p.id "
                    "WHERE r.id IS NULL ORDER BY p.sequence LIMIT 1",
                    p="runtime_publications",
                    r="runtime_publication_receipts",
                ).fetchone()
                if row is None:
                    return None
                event = store._publication(row)
                store._bundle(db, row["source_sha256"])
                self._verify_broker()
                module = importlib.import_module("confluent_kafka")
                producer = module.Producer(
                    {
                        "bootstrap.servers": binding.broker.bootstrap_servers,
                        "acks": "all",
                        "enable.idempotence": True,
                        "message.timeout.ms": 10000,
                        "allow.auto.create.topics": False,
                    }
                )
                results: list[tuple[Any, Any]] = []
                producer.produce(
                    binding.broker.topic,
                    partition=0,
                    key=b"portfolio",
                    value=canonical(event.model_dump(mode="json")).encode(),
                    on_delivery=lambda error, message: results.append((error, message)),
                )
                if producer.flush(12) != 0 or len(results) != 1 or results[0][0] is not None:
                    raise BrokerAdmissionError("publication_ack_unconfirmed")
                message = results[0][1]
                if (
                    message.topic() != binding.broker.topic
                    or message.partition() != 0
                    or message.offset() < 0
                ):
                    raise BrokerAdmissionError("publication_ack_identity_differs")
                self._verify_broker()
                if failpoint is not None:
                    failpoint()  # Broker may have committed: retry keeps the exact event identity.
                receipt = {
                    "contract": "nexus.source-publication-receipt.v1",
                    "id": row["id"],
                    "publication_sha256": row["payload_sha256"],
                    "event_sha256": row["event_sha256"],
                    "binding_sha256": digest(binding.payload()),
                    "partition": 0,
                    "offset": message.offset(),
                    "claim": "broker_ack_not_business_admission",
                }
                store._query(
                    db,
                    "INSERT INTO {t} VALUES (%s,%s,%s)",
                    (row["id"], canonical(receipt), digest(receipt)),
                    t="runtime_publication_receipts",
                )
                return receipt
        except BaseException:
            self.failed = True
            raise
