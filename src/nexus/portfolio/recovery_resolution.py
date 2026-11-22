"""Governed quarantine resolution only after exact replacement publication/capture.

Never substitutes quarantine for business admission: the source outbox and all
captured pending records continue fencing orders until normal dispatch/replanning.
"""

from __future__ import annotations

import base64
import math
from collections.abc import Callable
from typing import Any, ClassVar

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.postgres_backend import _dsn, _refuse
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.recovery_publication import RecoveryPublicationLedger
from nexus.portfolio.store import StoreError, canonical, digest
from nexus.portfolio.transport_ledger import (
    TransportBinding,
    TransportLedger,
    TransportRecord,
    runtime_target,
)
from nexus.portfolio.transport_recovery import RuntimeDeadLetter, RuntimeRedriveRequest


class RecoveryResolutionLedger(RecoveryPublicationLedger):
    """Explicit V11; provide the pinned source-publication runtime when resolving/replaying."""

    _versions: ClassVar[tuple[int, ...]] = (5, 9, 10, 11)
    _prior_ledger_type: ClassVar[type[TransportLedger]] = RecoveryPublicationLedger
    _schema_columns: ClassVar[dict[str, dict[str, str]]] = {
        **RecoveryPublicationLedger._schema_columns,
        "transport_recovery_resolutions": {
            "id": "text",
            "request_id": "text",
            "decision_id": "text",
            "payload": "text",
            "payload_sha256": "text",
        },
    }
    _schema_tables: ClassVar[set[str]] = set(_schema_columns)
    _foreign_keys: ClassVar[dict[tuple[str, str], tuple[list[int], str, list[int]]]] = {
        **RecoveryPublicationLedger._foreign_keys,
        ("transport_recovery_resolutions", "transport_recovery_resolutions_id_fkey"): (
            [1],
            "transport_inbox",
            [1],
        ),
        ("transport_recovery_resolutions", "transport_recovery_resolutions_request_id_fkey"): (
            [2],
            "transport_recovery_requests",
            [1],
        ),
        ("transport_recovery_resolutions", "transport_recovery_resolutions_decision_id_fkey"): (
            [3],
            "transport_recovery_decisions",
            [1],
        ),
    }
    _history_triggers: ClassVar[dict[str, str]] = {
        **RecoveryPublicationLedger._history_triggers,
        "transport_recovery_resolutions": "immutable_recovery_resolution",
    }
    _forbidden_grants: ClassVar[dict[str, str]] = {
        **RecoveryPublicationLedger._forbidden_grants,
        "transport_recovery_resolutions": "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER",
    }

    def __init__(
        self, *args: Any, publication: PublicationRuntimeStore | None = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.publication = publication

    def propose_publication(
        self,
        binding: TransportBinding,
        partition: int,
        expected_inbox_hash: str,
        publication_id: str,
        reason: str,
        actor: Actor,
    ) -> dict[str, Any]:
        """Construct a proposal from retained bytes, not client paths or envelopes.

        Repeated calls create separately reviewable proposals; submission is not an
        effect or an approval. Retention rechecks the pending pointer under lock.
        """
        self._authorize(binding, actor, ActorRole.PLANNER)
        store = self.publication
        if (
            not isinstance(store, PublicationRuntimeStore)
            or store.transport_binding != binding
            or store.transport.config.schema != self.config.schema
            or _dsn(store.transport.config.dsn) != _dsn(self.config.dsn)
            or runtime_target(store.control) != binding.runtime_target_sha256
        ):
            raise StoreError("recovery_runtime_required", "Use the bound publication runtime", 409)
        with store.read_snapshot() as unit:
            row = unit._query(
                "SELECT * FROM {t} WHERE id=%s", (publication_id,), t="runtime_publications"
            ).fetchone()
            if row is None:
                raise StoreError(
                    "recovery_publication_required", "Retain replacement source first", 409
                )
            event = store._publication(row)
            bundle = store._bundle(unit._db, row["source_sha256"])
        with self._transport(binding, write=True) as db:
            original, physical = self._pending(db, binding, partition)
            if original["record_hash"] != expected_inbox_hash:
                raise StoreError("stale_recovery_target", "Select the exact pending record", 409)
            dead = RuntimeDeadLetter.create(binding, physical, expected_inbox_hash)
            proposal = RuntimeRedriveRequest.create(
                dead,
                binding,
                physical,
                expected_inbox_hash,
                bundle,
                canonical(event.model_dump(mode="json")).encode(),
                operator=actor,
                reason=reason,
                submitted_at=self._now(),
            )
        return self.retain_request(binding, partition, expected_inbox_hash, bundle, proposal, actor)

    def _source_proof(
        self, db: Any, binding: TransportBinding, request: dict[str, Any], original: dict[str, Any]
    ) -> dict[str, Any]:
        store = self.publication
        if (
            not isinstance(store, PublicationRuntimeStore)
            or store.transport_binding != binding
            or store.transport.config.schema != self.config.schema
            or _dsn(store.transport.config.dsn) != _dsn(self.config.dsn)
            or runtime_target(store.control) != binding.runtime_target_sha256
        ):
            raise StoreError(
                "recovery_runtime_required", "Use the exact bound source-publication runtime", 409
            )
        proposal = self._json(request)
        replacement = base64.b64decode(proposal["replacement_base64"], validate=True)
        with store.read_snapshot() as unit:
            publication = unit._query(
                "SELECT * FROM {t} WHERE id=%s",
                (proposal["replacement_event_id"],),
                t="runtime_publications",
            ).fetchone()
            if publication is None:
                raise StoreError(
                    "recovery_publication_required",
                    "Retain the exact replacement source intent first",
                    409,
                )
            event = store._publication(publication)
            # The normal source publisher emits canonical bytes. Never silently normalize
            # differently reviewed replacement bytes; require a newly reviewed proposal.
            if canonical(event.model_dump(mode="json")).encode() != replacement:
                raise StoreError(
                    "recovery_replacement_differs",
                    "Published bytes must equal the reviewed replacement",
                    409,
                )
            bundle = store._bundle(unit._db, request["source_id"])
            if publication["source_sha256"] != bundle.source_sha256 or self._json(
                self._query(
                    db,
                    "SELECT * FROM {t} WHERE id=%s",
                    (request["source_id"],),
                    t="transport_recovery_sources",
                ).fetchone()
            ) != {"bundle": bundle.payload}:
                _refuse("integrity_error", "Replacement source bytes differ from reviewed source")
            row = unit._query(
                "SELECT * FROM {t} WHERE id=%s",
                (publication["id"],),
                t="runtime_publication_receipts",
            ).fetchone()
            if row is None:
                raise StoreError(
                    "recovery_publication_required",
                    "Replacement source needs broker acknowledgement",
                    409,
                )
            receipt = self._json(row)
            if (
                receipt
                != {
                    "contract": "nexus.source-publication-receipt.v1",
                    "id": publication["id"],
                    "publication_sha256": publication["payload_sha256"],
                    "event_sha256": publication["event_sha256"],
                    "binding_sha256": digest(binding.payload()),
                    "partition": 0,
                    "offset": receipt.get("offset"),
                    "claim": "broker_ack_not_business_admission",
                }
                or type(receipt.get("offset")) is not int
                or receipt["offset"] < 0
            ):
                _refuse("integrity_error", "Replacement publication receipt differs")
            captured = self._query(
                db,
                "SELECT * FROM {t} WHERE partition=0 AND broker_offset=%s",
                (receipt["offset"],),
                t="transport_inbox",
            ).fetchone()
            if captured is None:
                raise StoreError(
                    "recovery_capture_required",
                    "Capture the acknowledged replacement before resolution",
                    409,
                )
            physical = TransportRecord.from_payload(self._row_payload(binding, captured))
            if (
                physical.key != b"portfolio"
                or physical.value != replacement
                or (physical.partition, physical.offset) != (0, receipt["offset"])
                or (original["partition"] == 0 and physical.offset <= original["broker_offset"])
            ):
                _refuse("integrity_error", "Captured replacement bytes or order differ")
            return {
                "event_id": publication["id"],
                "source_sha256": bundle.source_sha256,
                "publication_sha256": publication["payload_sha256"],
                "receipt_sha256": row["payload_sha256"],
                "inbox_id": captured["id"],
                "inbox_record_hash": captured["record_hash"],
                "partition": 0,
                "offset": receipt["offset"],
            }

    def _evidence_receipts(
        self, db: Any, binding: TransportBinding, request_id: str, decision_id: str
    ) -> dict[str, Any]:
        rows = self._query(
            db,
            "SELECT * FROM {t} WHERE request_id=%s AND decision_id=%s ORDER BY id",
            (request_id, decision_id),
            t="transport_recovery_publications",
        ).fetchall()
        if len(rows) != 2:
            raise StoreError(
                "recovery_evidence_required", "Publish both retained recovery evidence intents", 409
            )
        evidence = {}
        for row in rows:
            intent, topic = self._intent(db, binding, row)
            receipt = self._query(
                db,
                "SELECT * FROM {t} WHERE id=%s",
                (row["id"],),
                t="transport_recovery_publication_receipts",
            ).fetchone()
            if receipt is None:
                raise StoreError(
                    "recovery_evidence_required",
                    "Both evidence publications need retained observations",
                    409,
                )
            self._observation(receipt, intent, topic)
            evidence[intent["kind"]] = {
                "intent_id": row["id"],
                "intent_sha256": row["payload_sha256"],
                "receipt_sha256": receipt["payload_sha256"],
            }
        if set(evidence) != {"dead_letter", "redrive_audit"}:
            _refuse("integrity_error", "Recovery evidence pair differs")
        return evidence

    def _resolution(
        self, db: Any, binding: TransportBinding, row: dict[str, Any]
    ) -> dict[str, Any]:
        value = self._json(row)
        request, original = self._load_request(db, binding, row["request_id"])
        reviews = self._reviews(db, binding, request, original)
        decision = next((r for r in reviews if r["decision_id"] == row["decision_id"]), None)
        try:
            actor = Actor(
                value["actor_id"],
                "Retained resolver",
                ActorRole(value["actor_role"]),
                tuple(value["locations"]),
            )
            self._authorize(binding, actor, ActorRole.APPROVER)
            resolved = value["resolved_at"]
            if (
                decision is None
                or decision["action"] != "approve"
                or type(resolved) not in (int, float)
                or not math.isfinite(resolved)
                or not decision["created_at"] <= resolved < decision["expires_at"]
                or resolved > self._now().timestamp()
            ):
                raise ValueError
            expected = {
                "contract": "nexus.recovery-resolution.v1",
                "id": original["id"],
                "request_id": row["request_id"],
                "request_sha256": request["payload_sha256"],
                "decision_id": row["decision_id"],
                "decision_sha256": digest(decision),
                "inbox_record_hash": original["record_hash"],
                "actor_id": actor.actor_id,
                "actor_role": "approver",
                "locations": sorted(binding.broker.locations),
                "resolved_at": resolved,
                "replacement": self._source_proof(db, binding, request, original),
                "evidence": self._evidence_receipts(
                    db, binding, row["request_id"], row["decision_id"]
                ),
                "claim": "original_quarantined_replacement_requires_normal_admission",
            }
            if row["id"] != original["id"] or value != expected:
                raise ValueError
        except (ValueError, KeyError, TypeError):
            _refuse("integrity_error", "Recovery resolution differs from retained evidence")
        receipt = self._query(
            db, "SELECT * FROM {t} WHERE id=%s", (row["id"],), t="transport_receipts"
        ).fetchone()
        if receipt is None or self._row_payload(binding, receipt) != {
            "inbox_record_hash": original["record_hash"],
            "recovery_resolution_sha256": row["payload_sha256"],
            "claim": "quarantine_resolution_not_business_admission",
        }:
            _refuse("integrity_error", "Recovery resolution delivery link differs")
        return value

    def resolve(
        self,
        binding: TransportBinding,
        request_id: str,
        expected_request_hash: str,
        decision_id: str,
        actor: Actor,
        *,
        failpoint: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        self._authorize(binding, actor, ActorRole.APPROVER)
        with self._transport(binding, write=True) as db:
            delivered = self._progress(db, binding, delivery=True, lock=True)
            request, original = self._load_request(db, binding, request_id)
            if request["payload_sha256"] != expected_request_hash:
                raise StoreError(
                    "stale_recovery_request", "Resolve the exact reviewed request", 409
                )
            prior = self._query(
                db,
                "SELECT * FROM {t} WHERE id=%s",
                (original["id"],),
                t="transport_recovery_resolutions",
            ).fetchone()
            if prior is not None:
                if prior["request_id"] != request_id or prior["decision_id"] != decision_id:
                    raise StoreError(
                        "recovery_resolution_conflict",
                        "Original record already has another resolution",
                        409,
                    )
                return {**self._resolution(db, binding, prior), "duplicate": True}
            pending, _ = self._pending(db, binding, original["partition"])
            if pending["id"] != original["id"]:
                raise StoreError(
                    "stale_recovery_target", "Resolve only the exact pending record", 409
                )
            reviews = self._reviews(db, binding, request, original)
            if (
                not reviews
                or reviews[-1]["decision_id"] != decision_id
                or reviews[-1]["action"] != "approve"
            ):
                raise StoreError(
                    "recovery_review_required", "Require the latest independent approval", 409
                )
            decision = reviews[-1]
            replacement = self._source_proof(db, binding, request, original)
            evidence = self._evidence_receipts(db, binding, request_id, decision_id)
            now = self._now().timestamp()
            if now < decision["created_at"]:
                raise StoreError("invalid_clock", "Resolution clock precedes approval", 409)
            if now >= decision["expires_at"]:
                raise StoreError(
                    "recovery_review_expired", "Review expired before resolution commit", 409
                )
            value = {
                "contract": "nexus.recovery-resolution.v1",
                "id": original["id"],
                "request_id": request_id,
                "request_sha256": expected_request_hash,
                "decision_id": decision_id,
                "decision_sha256": digest(decision),
                "inbox_record_hash": original["record_hash"],
                "actor_id": actor.actor_id,
                "actor_role": "approver",
                "locations": sorted(binding.broker.locations),
                "resolved_at": now,
                "replacement": replacement,
                "evidence": evidence,
                "claim": "original_quarantined_replacement_requires_normal_admission",
            }
            self._query(
                db,
                "INSERT INTO {t} VALUES (%s,%s,%s,%s,%s)",
                (original["id"], request_id, decision_id, canonical(value), digest(value)),
                t="transport_recovery_resolutions",
            )
            receipt = {
                "inbox_record_hash": original["record_hash"],
                "recovery_resolution_sha256": digest(value),
                "claim": "quarantine_resolution_not_business_admission",
            }
            pointer = delivered[original["partition"]]
            head = digest([original["id"], digest(receipt), pointer["head_hash"]])
            self._query(
                db,
                "INSERT INTO {t} VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    original["id"],
                    original["partition"],
                    original["broker_offset"],
                    canonical(receipt),
                    digest(receipt),
                    pointer["head_hash"],
                    head,
                ),
                t="transport_receipts",
            )
            self._query(
                db,
                "UPDATE {t} SET next_offset=%s,head_hash=%s WHERE partition=%s",
                (original["broker_offset"] + 1, head, original["partition"]),
                t="transport_delivery",
            )
            if failpoint is not None:
                failpoint()
            return {**value, "duplicate": False}

    def _verify_extension_history(self, db: Any, binding: TransportBinding) -> dict[str, Any]:
        result = super()._verify_extension_history(db, binding)
        last, count = "", 0
        while True:
            rows = self._query(
                db,
                "SELECT * FROM {t} WHERE id>%s ORDER BY id LIMIT 1000",
                (last,),
                t="transport_recovery_resolutions",
            ).fetchall()
            if not rows:
                break
            for row in rows:
                self._resolution(db, binding, row)
                last, count = row["id"], count + 1
        if (
            self._query(
                db, "SELECT COUNT(*) AS n FROM {t}", t="transport_recovery_resolutions"
            ).fetchone()["n"]
            != count
        ):
            _refuse("integrity_error", "Unverified recovery resolution history")
        # Every non-business delivery receipt must have its verified resolution.
        orphan = self._query(
            db,
            "SELECT 1 FROM {r} r LEFT JOIN {s} s ON s.id=r.id "
            "WHERE (r.payload::jsonb->>'claim') "
            "IS DISTINCT FROM 'business_admission_not_completed_replan' "
            "AND s.id IS NULL LIMIT 1",
            r="transport_receipts",
            s="transport_recovery_resolutions",
        ).fetchone()
        if orphan is not None:
            _refuse("integrity_error", "Unbound non-business transport receipt")
        result["recovery_resolutions"] = {
            "count": count,
            "claim": "quarantine_not_business_admission",
        }
        return result
