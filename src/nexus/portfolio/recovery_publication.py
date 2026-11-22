"""Durable source/review-bound recovery evidence outbox; no delivery advancement."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Callable
from dataclasses import replace
from typing import Any, ClassVar, cast

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_backend import _refuse
from nexus.portfolio.recovery_ledger import RecoveryTransportLedger
from nexus.portfolio.recovery_publisher import RecoveryEvidencePublisher, RecoveryTopic
from nexus.portfolio.store import StoreError, canonical, digest
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger


class RecoveryPublicationLedger(RecoveryTransportLedger):
    """Explicit transport [5,9,10] journal; network I/O never takes delivery locks."""

    _versions: ClassVar[tuple[int, ...]] = (5, 9, 10)
    _prior_ledger_type: ClassVar[type[TransportLedger]] = RecoveryTransportLedger
    _schema_columns: ClassVar[dict[str, dict[str, str]]] = {
        **RecoveryTransportLedger._schema_columns,
        **{
            name: {"id": "text", "payload": "text", "payload_sha256": "text"}
            for name in ("transport_recovery_topics", "transport_recovery_publication_receipts")
        },
        "transport_recovery_publications": {
            "id": "text",
            "request_id": "text",
            "decision_id": "text",
            "payload": "text",
            "payload_sha256": "text",
        },
    }
    _schema_tables: ClassVar[set[str]] = set(_schema_columns)
    _foreign_keys: ClassVar[dict[tuple[str, str], tuple[list[int], str, list[int]]]] = {
        **RecoveryTransportLedger._foreign_keys,
        ("transport_recovery_publications", "transport_recovery_publications_request_id_fkey"): (
            [2],
            "transport_recovery_requests",
            [1],
        ),
        ("transport_recovery_publications", "transport_recovery_publications_decision_id_fkey"): (
            [3],
            "transport_recovery_decisions",
            [1],
        ),
        (
            "transport_recovery_publication_receipts",
            "transport_recovery_publication_receipts_id_fkey",
        ): ([1], "transport_recovery_publications", [1]),
    }
    _history_triggers: ClassVar[dict[str, str]] = {
        **RecoveryTransportLedger._history_triggers,
        "transport_recovery_topics": "immutable_recovery_topics",
        "transport_recovery_publications": "immutable_recovery_publication",
        "transport_recovery_publication_receipts": "immutable_recovery_publication_receipt",
    }
    _forbidden_grants: ClassVar[dict[str, str]] = {
        **RecoveryTransportLedger._forbidden_grants,
        **{
            name: "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER"
            for name in (
                "transport_recovery_topics",
                "transport_recovery_publications",
                "transport_recovery_publication_receipts",
            )
        },
    }

    def enroll_topics(
        self,
        binding: TransportBinding,
        dead_letter: RecoveryTopic,
        redrive_audit: RecoveryTopic,
        actor: Actor,
    ) -> None:
        self._authorize(binding, actor, ActorRole.APPROVER)
        if (
            dead_letter.source != binding
            or redrive_audit.source != binding
            or dead_letter.kind != "dead_letter"
            or redrive_audit.kind != "redrive_audit"
            or dead_letter.broker.topic == redrive_audit.broker.topic
            or dead_letter.topic_id == redrive_audit.topic_id
        ):
            raise StoreError(
                "recovery_topics_conflict", "Pin two distinct bound evidence topics", 422
            )
        value = {"dead_letter": dead_letter.payload(), "redrive_audit": redrive_audit.payload()}
        with self._transport(binding, write=True) as db:
            self._progress(db, binding, delivery=True, lock=True)
            self._retain(db, "transport_recovery_topics", "topics", value)

    def _topics(self, db: Any, binding: TransportBinding) -> dict[str, RecoveryTopic]:
        rows = self._query(db, "SELECT * FROM {t}", t="transport_recovery_topics").fetchall()
        if len(rows) != 1 or rows[0]["id"] != "topics":
            _refuse("integrity_error", "Recovery evidence topics must be explicitly enrolled")
        value = self._json(rows[0])
        try:
            if set(value) != {"dead_letter", "redrive_audit"}:
                raise ValueError
            result = {}
            for kind in ("dead_letter", "redrive_audit"):
                item = value[kind]
                config = replace(
                    binding.broker,
                    topic=item["topic"],
                    group_id="nexus-runtime-recovery-reader-" + digest(item)[:16],
                )
                topic = RecoveryTopic(binding, config, item["topic_id"], cast(Any, kind))
                if item != topic.payload():
                    raise ValueError
                result[kind] = topic
            if (
                result["dead_letter"].broker.topic == result["redrive_audit"].broker.topic
                or result["dead_letter"].topic_id == result["redrive_audit"].topic_id
            ):
                raise ValueError
        except (ValueError, TypeError, KeyError):
            _refuse("integrity_error", "Recovery topic binding differs")
        return result

    def _evidence(
        self, db: Any, request: dict[str, Any], decision: dict[str, Any], kind: str
    ) -> str:
        case = self._json(
            self._query(
                db,
                "SELECT * FROM {t} WHERE id=%s",
                (request["case_id"],),
                t="transport_recovery_cases",
            ).fetchone()
        )
        if kind == "dead_letter":
            return canonical(case)
        return canonical(
            {
                "contract": "nexus.recovery-review-evidence.v1",
                "request": self._json(request),
                "decision": decision,
                "case_sha256": digest(case),
                "claim": "approved_proposal_at_enqueue_not_redrive_execution",
            }
        )

    def _intent(
        self, db: Any, binding: TransportBinding, row: dict[str, Any]
    ) -> tuple[dict[str, Any], RecoveryTopic]:
        value = self._json(row)
        request, original = self._load_request(db, binding, row["request_id"])
        reviews = self._reviews(db, binding, request, original)
        decision = next((r for r in reviews if r["decision_id"] == row["decision_id"]), None)
        try:
            if decision is None or decision["action"] != "approve":
                raise ValueError
            topic = self._topics(db, binding)[value["kind"]]
            created = value["created_at"]
            if (
                type(created) not in (int, float)
                or not math.isfinite(created)
                or not decision["created_at"] <= created < decision["expires_at"]
                or created > self._now().timestamp()
            ):
                raise ValueError
            evidence = self._evidence(db, request, decision, value["kind"])
            identity = {
                "contract": "nexus.recovery-publication-intent.v1",
                "request_id": row["request_id"],
                "request_sha256": request["payload_sha256"],
                "decision_id": row["decision_id"],
                "decision_sha256": digest(decision),
                "kind": value["kind"],
                "topic_binding_sha256": digest(topic.payload()),
                "evidence": evidence,
                "claim": "retained_review_evidence_not_delivery_authorization",
            }
            if value != {**identity, "id": digest(identity), "created_at": created} or row[
                "id"
            ] != digest(identity):
                raise ValueError
        except (ValueError, TypeError, KeyError):
            _refuse("integrity_error", "Recovery publication intent differs from retained review")
        return value, topic

    def enqueue_evidence(
        self,
        binding: TransportBinding,
        request_id: str,
        expected_request_hash: str,
        decision_id: str,
        actor: Actor,
        *,
        failpoint: Callable[[], None] | None = None,
    ) -> list[str]:
        self._authorize(binding, actor, ActorRole.APPROVER)
        with self._transport(binding, write=True) as db:
            self._progress(db, binding, delivery=True, lock=True)
            request, original = self._load_request(db, binding, request_id)
            reviews = self._reviews(db, binding, request, original)
            now = self._now().timestamp()
            if request["payload_sha256"] != expected_request_hash or not reviews:
                raise StoreError(
                    "recovery_review_required", "Review the exact retained request", 409
                )
            decision = reviews[-1]
            if (
                decision["decision_id"] != decision_id
                or decision["action"] != "approve"
                or now >= decision["expires_at"]
            ):
                raise StoreError(
                    "recovery_review_required", "Require the current unexpired approval", 409
                )
            pending, _ = self._pending(db, binding, original["partition"])
            if pending["id"] != original["id"]:
                raise StoreError("stale_recovery_target", "Review the current pending record", 409)
            topics = self._topics(db, binding)
            ids = []
            for kind, topic in topics.items():
                identity = {
                    "contract": "nexus.recovery-publication-intent.v1",
                    "request_id": request_id,
                    "request_sha256": expected_request_hash,
                    "decision_id": decision_id,
                    "decision_sha256": digest(decision),
                    "kind": kind,
                    "topic_binding_sha256": digest(topic.payload()),
                    "evidence": self._evidence(db, request, decision, kind),
                    "claim": "retained_review_evidence_not_delivery_authorization",
                }
                intent_id = digest(identity)
                existing = self._query(
                    db,
                    "SELECT * FROM {t} WHERE id=%s",
                    (intent_id,),
                    t="transport_recovery_publications",
                ).fetchone()
                if existing is None:
                    value = {**identity, "id": intent_id, "created_at": now}
                    self._query(
                        db,
                        "INSERT INTO {t} VALUES (%s,%s,%s,%s,%s)",
                        (intent_id, request_id, decision_id, canonical(value), digest(value)),
                        t="transport_recovery_publications",
                    )
                else:
                    self._intent(db, binding, existing)
                ids.append(intent_id)
            if failpoint is not None:
                failpoint()
            return ids

    def _observation(
        self, row: dict[str, Any], intent: dict[str, Any], topic: RecoveryTopic
    ) -> dict[str, Any]:
        value = self._json(row)
        expected = {
            "contract": "nexus.recovery-publication-observation.v1",
            "intent_id": intent["id"],
            "topic_binding_sha256": digest(topic.payload()),
            "payload_sha256": hashlib.sha256(intent["evidence"].encode()).hexdigest(),
            "key_sha256": hashlib.sha256(intent["id"].encode()).hexdigest(),
            "partition": 0,
            "offset": value.get("offset"),
            "claim": "broker_ack_and_exact_readback_not_recovery_authorization",
        }
        if (
            row["id"] != intent["id"]
            or value != expected
            or type(value.get("offset")) is not int
            or not 0 <= value["offset"] < 2**63
        ):
            _refuse("integrity_error", "Recovery publication observation differs")
        return value

    def publish_once(
        self,
        binding: TransportBinding,
        actor: Actor,
        *,
        failpoint: Callable[[], None] | None = None,
    ) -> dict[str, Any] | None:
        if (
            actor.role is not ActorRole.SYSTEM
            or actor.actor_id != "recovery-publisher"
            or set(actor.locations) != set(binding.broker.locations)
        ):
            raise StoreError("role_denied", "Use the configured scoped recovery publisher", 403)
        lock = int.from_bytes(
            hashlib.sha256((self.config.schema + ":recovery-publisher").encode()).digest()[:8],
            "big",
            signed=True,
        )
        with self._transport(binding, write=True) as db:
            # Serialize publishers only. Never hold workflow or delivery locks during I/O.
            db.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
            row = self._query(
                db,
                "SELECT p.* FROM {p} p LEFT JOIN {r} r ON r.id=p.id "
                "WHERE r.id IS NULL ORDER BY p.id LIMIT 1",
                p="transport_recovery_publications",
                r="transport_recovery_publication_receipts",
            ).fetchone()
            if row is None:
                return None
            pair = self._query(
                db,
                "SELECT * FROM {t} WHERE request_id=%s AND decision_id=%s",
                (row["request_id"], row["decision_id"]),
                t="transport_recovery_publications",
            ).fetchall()
            if len(pair) != 2:
                _refuse("integrity_error", "Recovery publication pair is incomplete")
            for item in pair:
                self._intent(db, binding, item)
            intent, topic = self._intent(db, binding, row)
            observed = RecoveryEvidencePublisher(topic).publish(
                intent["id"], intent["evidence"].encode()
            )
            receipt = {
                "id": intent["id"],
                "payload": canonical(observed),
                "payload_sha256": digest(observed),
            }
            self._observation(receipt, intent, topic)
            if failpoint is not None:
                failpoint()  # Broker may have committed. Retry exact intent bytes, never new IDs.
            self._retain(db, "transport_recovery_publication_receipts", intent["id"], observed)
            return observed

    def _verify_extension_history(self, db: Any, binding: TransportBinding) -> dict[str, Any]:
        result = super()._verify_extension_history(db, binding)
        enrolled = self._query(
            db, "SELECT COUNT(*) AS n FROM {t}", t="transport_recovery_topics"
        ).fetchone()["n"]
        if enrolled:
            self._topics(db, binding)
        if (
            self._query(
                db,
                "SELECT 1 FROM {t} GROUP BY request_id,decision_id HAVING COUNT(*)<>2 LIMIT 1",
                t="transport_recovery_publications",
            ).fetchone()
            is not None
        ):
            _refuse("integrity_error", "Recovery publication pair is incomplete")
        last = ""
        intents = receipts = 0
        while True:
            rows = self._query(
                db,
                "SELECT * FROM {t} WHERE id>%s ORDER BY id LIMIT 1000",
                (last,),
                t="transport_recovery_publications",
            ).fetchall()
            if not rows:
                break
            for row in rows:
                intent, topic = self._intent(db, binding, row)
                receipt = self._query(
                    db,
                    "SELECT * FROM {t} WHERE id=%s",
                    (row["id"],),
                    t="transport_recovery_publication_receipts",
                ).fetchone()
                if receipt is not None:
                    self._observation(receipt, intent, topic)
                    receipts += 1
                intents += 1
                last = row["id"]
        for table, expected in (
            ("transport_recovery_publications", intents),
            ("transport_recovery_publication_receipts", receipts),
        ):
            if (
                self._query(db, "SELECT COUNT(*) AS n FROM {t}", t=table).fetchone()["n"]
                != expected
            ):
                _refuse("integrity_error", "Unverified recovery publication history")
        result["recovery_publication"] = {
            "intents": intents,
            "receipts": receipts,
            "claim": "evidence_publication_not_delivery_resolution",
        }
        return result
