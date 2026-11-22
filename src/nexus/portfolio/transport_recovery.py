"""Byte-preserving poison evidence and governed source-bound redrive proposals.

These portable contracts do not advance a transport checkpoint or authorize a
business action. Persistence must join them back to the actual inbox and source
outbox before any authorized redrive can affect delivery.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from nexus.contracts import ActorRole
from nexus.policies.replanning import ReplanTrigger
from nexus.portfolio.auth import Actor
from nexus.portfolio.broker_runtime import BrokerAdmissionError, decode_runtime_event
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.store import StoreError, canonical, digest
from nexus.portfolio.transport_ledger import TransportBinding, TransportRecord

_SHA = re.compile(r"[a-f0-9]{64}")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,159}")
_DECODE_FAILURES = frozenset(
    {"invalid_payload_size", "invalid_event_envelope", "unexpected_event_contract"}
)


def _poison_payload(
    binding: TransportBinding, record: TransportRecord, inbox_record_hash: str
) -> dict[str, Any]:
    if (
        binding.purpose != "operations"
        or record.partition >= binding.partition_count
        or not isinstance(inbox_record_hash, str)
        or _SHA.fullmatch(inbox_record_hash) is None
    ):
        raise StoreError("invalid_dead_letter", "Use the bound operations inbox identity", 422)
    try:
        decode_runtime_event(record.value, record.key)
    except BrokerAdmissionError as error:
        reason = str(error)
        if reason not in _DECODE_FAILURES:
            raise StoreError("invalid_dead_letter", "Unknown poison classification", 422) from None
    else:
        # Business rejections, database faults and timeouts are not schema poison.
        raise StoreError("not_schema_poison", "A compatible envelope cannot be dead-lettered", 422)
    identity = {
        "contract": "nexus.runtime-dead-letter.v1",
        "binding_sha256": digest(binding.payload()),
        "inbox_record_hash": inbox_record_hash,
        "physical": record.payload(),
        "original_value_sha256": (
            None if record.value is None else hashlib.sha256(record.value).hexdigest()
        ),
        "classification": reason,
    }
    return {**identity, "dead_letter_id": digest(identity)}


@dataclass(frozen=True)
class RuntimeDeadLetter:
    payload: str = field(repr=False)

    @classmethod
    def create(
        cls, binding: TransportBinding, record: TransportRecord, inbox_record_hash: str
    ) -> RuntimeDeadLetter:
        return cls(canonical(_poison_payload(binding, record, inbox_record_hash)))

    def verify(
        self, binding: TransportBinding, record: TransportRecord, inbox_record_hash: str
    ) -> dict[str, Any]:
        expected = _poison_payload(binding, record, inbox_record_hash)
        if self.payload != canonical(expected):
            raise StoreError("dead_letter_conflict", "Dead-letter bytes or lineage differ", 409)
        return expected


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise StoreError("invalid_redrive", "Redrive requires an explicit aware timestamp", 422)
    return value.astimezone(UTC)


def _original_identifiers(record: TransportRecord) -> tuple[str | None, str | None]:
    try:
        parsed = json.loads(record.value) if record.value is not None else None
        if not isinstance(parsed, dict):
            return None, None
        raw_id = parsed.get("event_id")
        identity: str | None
        try:
            identity = str(UUID(raw_id))
        except (TypeError, ValueError, AttributeError):
            identity = raw_id if isinstance(raw_id, str) else None
        key = parsed.get("idempotency_key")
        return identity, key if isinstance(key, str) else None
    except (ValueError, UnicodeError, RecursionError):
        return None, None


def _request_payload(
    dead_letter: RuntimeDeadLetter,
    binding: TransportBinding,
    record: TransportRecord,
    inbox_record_hash: str,
    bundle: SourceBundle,
    replacement: bytes,
    operator: Actor,
    reason: str,
    submitted_at: datetime,
) -> dict[str, Any]:
    evidence = dead_letter.verify(binding, record, inbox_record_hash)
    if (
        operator.role is not ActorRole.PLANNER
        or not isinstance(operator.actor_id, str)
        or _IDENTIFIER.fullmatch(operator.actor_id) is None
        or operator.locations != binding.broker.locations
    ):
        raise StoreError(
            "redrive_denied", "Redrive proposal requires a scoped planner identity", 403
        )
    if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
        raise StoreError("invalid_redrive", "Supply a bounded nonempty operator reason", 422)
    instant = _utc(submitted_at)
    try:
        event = decode_runtime_event(replacement, b"portfolio")
    except BrokerAdmissionError:
        raise StoreError("invalid_redrive", "Replacement envelope is not compatible", 422) from None
    source = bundle.source_sha256  # Revalidates exact source bytes and manifest checksums.
    if (
        event.payload != {"source_snapshot_sha256": source}
        or event.event_type not in {trigger.value for trigger in ReplanTrigger}
        or max(event.occurred_at, event.ingested_at) > instant
    ):
        raise StoreError(
            "invalid_redrive", "Bind a material replacement to exact source bytes", 422
        )
    original_id, original_key = _original_identifiers(record)
    if str(event.event_id) == original_id or event.idempotency_key == original_key:
        raise StoreError("redrive_identity_reused", "Use a new event ID and idempotency key", 409)
    identity = {
        "contract": "nexus.runtime-redrive-request.v1",
        "dead_letter_id": evidence["dead_letter_id"],
        "dead_letter_sha256": digest(evidence),
        "inbox_record_hash": inbox_record_hash,
        "causation_reference": evidence["dead_letter_id"],
        "binding_sha256": digest(binding.payload()),
        "source_sha256": source,
        "source_bundle_sha256": digest(bundle.payload),
        "replacement_base64": base64.b64encode(replacement).decode("ascii"),
        "replacement_sha256": hashlib.sha256(replacement).hexdigest(),
        "replacement_event_id": str(event.event_id),
        "replacement_idempotency_key": event.idempotency_key,
        "operator_id": operator.actor_id,
        "operator_role": operator.role.value,
        "locations": list(operator.locations),
        "reason": reason.strip(),
        "submitted_at": instant.isoformat(),
        "claim": "proposal_not_delivery_or_business_approval",
    }
    return {**identity, "request_id": digest(identity)}


@dataclass(frozen=True)
class RuntimeRedriveRequest:
    payload: str = field(repr=False)

    @classmethod
    def create(
        cls,
        dead_letter: RuntimeDeadLetter,
        binding: TransportBinding,
        record: TransportRecord,
        inbox_record_hash: str,
        bundle: SourceBundle,
        replacement: bytes,
        *,
        operator: Actor,
        reason: str,
        submitted_at: datetime,
    ) -> RuntimeRedriveRequest:
        return cls(
            canonical(
                _request_payload(
                    dead_letter,
                    binding,
                    record,
                    inbox_record_hash,
                    bundle,
                    replacement,
                    operator,
                    reason,
                    submitted_at,
                )
            )
        )

    def verify(
        self,
        dead_letter: RuntimeDeadLetter,
        binding: TransportBinding,
        record: TransportRecord,
        inbox_record_hash: str,
        bundle: SourceBundle,
        *,
        operator: Actor,
        now: datetime,
    ) -> dict[str, Any]:
        """Recompute semantics against external authoritative evidence, not self-rebound hashes."""
        try:
            if not isinstance(self.payload, str) or len(self.payload) > 256 * 1024:
                raise ValueError
            value = json.loads(self.payload)
            replacement = base64.b64decode(value["replacement_base64"], validate=True)
            submitted = datetime.fromisoformat(value["submitted_at"])
            if _utc(submitted) > _utc(now):
                raise ValueError
            expected = _request_payload(
                dead_letter,
                binding,
                record,
                inbox_record_hash,
                bundle,
                replacement,
                operator,
                value["reason"],
                submitted,
            )
            if self.payload != canonical(expected):
                raise ValueError
            return expected
        except (ValueError, KeyError, TypeError, AttributeError, RecursionError):
            raise StoreError(
                "redrive_conflict", "Redrive proposal differs from its evidence", 409
            ) from None
