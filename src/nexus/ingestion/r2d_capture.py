"""Dependency-free capture, barrier, and reconstruction primitives for R2D-A1."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import cast

from nexus.ingestion.r2d_contracts import (
    R2DCaptureCheckpoint,
    R2DCapturedRecord,
    R2DCaptureTranscript,
    R2DConfig,
    R2DHeader,
    R2DParsedLineage,
    R2DPartitionCheckpoint,
    R2DPhysicalRecord,
    R2DProducerAckLedger,
    R2DProducerAcknowledgement,
    R2DTransportDecision,
    canonical_json_sha256,
    r2d_config_sha256,
    r2d_sha256_multiset_sha256,
)


class R2DCaptureError(ValueError):
    """Base error for portable capture evidence that cannot be trusted."""


class R2DPhysicalRecordConflict(R2DCaptureError):
    """A physical transport identity was observed with conflicting evidence."""


class R2DCheckpointError(R2DCaptureError):
    """A checkpoint cannot prove exact contiguous half-open completion."""


def _canonical_headers(headers: tuple[R2DHeader, ...]) -> tuple[dict[str, str | None], ...]:
    """Sort without collapsing duplicate or nullable header values."""

    ordered = sorted(
        headers,
        key=lambda item: (
            item.name.encode("utf-8"),
            item.value_bytes is not None,
            item.value_bytes or b"",
        ),
    )
    return tuple({"name": item.name, "value_hex": item.value_hex} for item in ordered)


def _physical_payload(
    record: R2DPhysicalRecord,
    decision: R2DTransportDecision,
) -> dict[str, object]:
    value = record.value_bytes
    return {
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "key_hex": record.key_hex,
        "headers": _canonical_headers(record.headers),
        "broker_timestamp_ms": record.broker_timestamp_ms,
        "broker_timestamp_type": record.broker_timestamp_type,
        "value_hex": record.value_hex,
        "raw_value_sha256": None if value is None else hashlib.sha256(value).hexdigest(),
        "transport_disposition": decision.disposition,
        "transport_reason": decision.reason,
    }


def physical_record_fingerprint(
    record: R2DPhysicalRecord,
    decision: R2DTransportDecision,
) -> str:
    """Hash physical evidence while deliberately excluding parsed lineage and tracing."""

    return canonical_json_sha256(_physical_payload(record, decision))


def partition_for_key(key: bytes, *, partition_count: int = 2) -> int:
    """Map exact key bytes using SHA-256 interpreted as one big-endian integer."""

    if not key:
        raise ValueError("partition key bytes must be nonempty")
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    return int.from_bytes(hashlib.sha256(key).digest(), byteorder="big") % partition_count


def _best_effort_json(value: bytes) -> dict[str, object] | None:
    try:
        parsed = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return cast(dict[str, object], parsed) if isinstance(parsed, dict) else None


def _string_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _int_or_none(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _parsed_lineage(value: bytes) -> R2DParsedLineage | None:
    parsed = _best_effort_json(value)
    if parsed is None:
        return None
    payload = parsed.get("payload")
    payload_mapping = payload if isinstance(payload, dict) else {}
    fields = {
        "event_id": _string_or_none(parsed.get("event_id")),
        "idempotency_key": _string_or_none(parsed.get("idempotency_key")),
        "source": _string_or_none(parsed.get("source")),
        "logical_event_id": _string_or_none(payload_mapping.get("logical_event_id")),
        "revision": _int_or_none(payload_mapping.get("revision")),
        "correlation_id": _string_or_none(parsed.get("correlation_id")),
        "trace_id": _string_or_none(parsed.get("trace_id")),
    }
    if all(item is None for item in fields.values()):
        return None
    return R2DParsedLineage.model_validate(fields)


def parsed_lineage_sha256(lineage: R2DParsedLineage | None) -> str:
    """Hash best-effort parsed lineage independently from physical evidence."""

    payload = None if lineage is None else lineage.model_dump(mode="json")
    return canonical_json_sha256(payload)


def routing_key_for_value(value: bytes, config: R2DConfig) -> bytes | None:
    """Resolve a provable routing key without performing business validation.

    Parsed source/logical identity is preferred. The only fallback is an exact
    raw-value hash explicitly frozen in the governed configuration.
    """

    parsed = _best_effort_json(value)
    if parsed is not None:
        payload = parsed.get("payload")
        if isinstance(payload, dict):
            source = parsed.get("source")
            logical_event_id = payload.get("logical_event_id")
            if (
                isinstance(source, str)
                and isinstance(logical_event_id, str)
                and source
                and logical_event_id
            ):
                return f"{source}:{logical_event_id}".encode()
    value_sha256 = hashlib.sha256(value).hexdigest()
    for override in config.topic_contract.routing_key_overrides:
        if override.raw_value_sha256 == value_sha256:
            return override.routing_key.encode("utf-8")
    return None


def _headers_match(
    record: R2DPhysicalRecord,
    *,
    expected_routing_key: bytes | None,
    config: R2DConfig,
) -> bool:
    contract = config.header_contract
    allowed = set(contract.allowed_names)
    if len(record.headers) > config.admission_contract.maximum_headers:
        return False
    if any(header.name not in allowed for header in record.headers):
        return False
    if any(
        len(header.name.encode("utf-8")) > config.admission_contract.maximum_header_name_bytes
        for header in record.headers
    ):
        return False
    if any(
        header.value_bytes is not None
        and len(header.value_bytes) > config.admission_contract.maximum_header_value_bytes
        for header in record.headers
    ):
        return False

    by_name: dict[str, list[bytes | None]] = {}
    for header in record.headers:
        by_name.setdefault(header.name, []).append(header.value_bytes)

    expected_static = {
        contract.contract_header_name: contract.contract_header_value.encode("utf-8"),
        contract.schema_header_name: contract.schema_header_value.encode("ascii"),
    }
    for name, expected in expected_static.items():
        if by_name.get(name) != [expected]:
            return False
    routing_values = by_name.get(contract.routing_header_name)
    if expected_routing_key is None:
        return routing_values is None or (
            len(routing_values) == 1 and routing_values[0] == record.key_bytes
        )
    return routing_values == [expected_routing_key]


def classify_transport_record(
    record: R2DPhysicalRecord,
    config: R2DConfig,
) -> R2DTransportDecision:
    """Apply only the frozen transport rules, in their exact fail-closed precedence."""

    value = record.value_bytes
    if value is None:
        return R2DTransportDecision(disposition="excluded", reason="tombstone")
    if not value:
        return R2DTransportDecision(disposition="excluded", reason="empty")
    if len(value) > config.admission_contract.maximum_value_bytes:
        return R2DTransportDecision(disposition="excluded", reason="oversized")
    try:
        decoded = value.decode("utf-8")
    except UnicodeDecodeError:
        return R2DTransportDecision(disposition="excluded", reason="non_utf8")
    if not decoded.strip():
        return R2DTransportDecision(disposition="excluded", reason="whitespace_only")
    if "\n" in decoded or "\r" in decoded:
        return R2DTransportDecision(disposition="excluded", reason="newline_unsafe")

    expected_key = routing_key_for_value(value, config)
    routing_key = expected_key if expected_key is not None else record.key_bytes
    if routing_key is None or not routing_key:
        return R2DTransportDecision(disposition="excluded", reason="key_mismatch")
    if expected_key is not None and record.key_bytes != expected_key:
        return R2DTransportDecision(disposition="excluded", reason="key_mismatch")
    if record.partition != partition_for_key(
        routing_key,
        partition_count=config.topic_contract.partition_count,
    ):
        return R2DTransportDecision(disposition="excluded", reason="key_mismatch")
    if (
        record.key_bytes is not None
        and len(record.key_bytes) > config.admission_contract.maximum_key_bytes
    ):
        return R2DTransportDecision(disposition="excluded", reason="key_mismatch")
    if not _headers_match(record, expected_routing_key=routing_key, config=config):
        return R2DTransportDecision(disposition="excluded", reason="header_mismatch")
    return R2DTransportDecision(disposition="admitted", reason="admitted")


def _captured_record(record: R2DPhysicalRecord, config: R2DConfig) -> R2DCapturedRecord:
    value = record.value_bytes
    record_with_lineage = record.model_copy(
        update={"lineage": None if value is None else _parsed_lineage(value)}
    )
    decision = classify_transport_record(record_with_lineage, config)
    fingerprint = physical_record_fingerprint(record_with_lineage, decision)
    return R2DCapturedRecord(
        record=record_with_lineage,
        decision=decision,
        raw_value_sha256=None if value is None else hashlib.sha256(value).hexdigest(),
        physical_fingerprint=fingerprint,
        lineage_sha256=parsed_lineage_sha256(record_with_lineage.lineage),
    )


def _transcript_payload(
    *,
    topic: str,
    partition_count: int,
    records: tuple[R2DCapturedRecord, ...],
) -> dict[str, object]:
    return {
        "topic": topic,
        "partition_count": partition_count,
        "records": [
            {
                "topic": item.record.topic,
                "partition": item.record.partition,
                "offset": item.record.offset,
                "physical_fingerprint": item.physical_fingerprint,
            }
            for item in records
        ],
    }


def _transcript_lineage_payload(
    records: tuple[R2DCapturedRecord, ...],
) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "topic": item.record.topic,
            "partition": item.record.partition,
            "offset": item.record.offset,
            "lineage_sha256": item.lineage_sha256,
        }
        for item in records
    )


def _validate_captured_record(item: R2DCapturedRecord, config: R2DConfig) -> None:
    value = item.record.value_bytes
    expected_raw = None if value is None else hashlib.sha256(value).hexdigest()
    if item.raw_value_sha256 != expected_raw:
        raise R2DCaptureError("captured raw-value SHA-256 mismatch")
    expected_decision = classify_transport_record(item.record, config)
    if item.decision != expected_decision:
        raise R2DCaptureError("captured transport decision differs from configured admission")

    expected_physical = physical_record_fingerprint(item.record, item.decision)
    if item.physical_fingerprint != expected_physical:
        raise R2DCaptureError("captured physical-record fingerprint mismatch")
    expected_lineage = None if value is None else _parsed_lineage(value)
    if item.record.lineage != expected_lineage:
        raise R2DCaptureError("captured parsed lineage differs from the raw value")
    if item.lineage_sha256 != parsed_lineage_sha256(expected_lineage):
        raise R2DCaptureError("captured parsed-lineage SHA-256 mismatch")


def validate_capture_transcript(
    transcript: R2DCaptureTranscript, config: R2DConfig
) -> R2DCaptureTranscript:
    """Recompute every nested identity and the transcript content address."""

    if transcript.topic != config.topic_contract.topic:
        raise R2DCaptureError("capture transcript topic differs from configured topic")
    if transcript.partition_count != config.topic_contract.partition_count:
        raise R2DCaptureError("capture transcript topology differs from configuration")
    if transcript.partition_count != 2:
        raise R2DCaptureError("R2D-A1 transcript must declare exactly two partitions")
    identities: set[tuple[str, int, int]] = set()
    expected_order = sorted(
        transcript.records,
        key=lambda item: (item.record.partition, item.record.offset),
    )
    if list(transcript.records) != expected_order:
        raise R2DCaptureError("captured records must be ordered by partition then offset")
    for item in transcript.records:
        _validate_captured_record(item, config)
        if item.record.topic != transcript.topic:
            raise R2DCaptureError("captured record topic differs from transcript topic")
        if item.record.partition >= transcript.partition_count:
            raise R2DCaptureError("captured record partition is outside the transcript topology")
        if item.transport_identity in identities:
            raise R2DCaptureError("capture transcript contains a duplicate transport identity")
        identities.add(item.transport_identity)
    expected_hash = canonical_json_sha256(
        _transcript_payload(
            topic=transcript.topic,
            partition_count=transcript.partition_count,
            records=transcript.records,
        )
    )
    if transcript.content_sha256 != expected_hash:
        raise R2DCaptureError("capture transcript content SHA-256 mismatch")
    expected_lineage_hash = canonical_json_sha256(_transcript_lineage_payload(transcript.records))
    if transcript.lineage_sha256 != expected_lineage_hash:
        raise R2DCaptureError("capture transcript lineage SHA-256 mismatch")
    return transcript


def capture_transcript(
    records: Iterable[R2DPhysicalRecord],
    config: R2DConfig,
) -> R2DCaptureTranscript:
    """Capture physical offsets once, verify redeliveries, and canonicalize poll order."""

    captured_by_identity: dict[tuple[str, int, int], R2DCapturedRecord] = {}
    for record in records:
        if record.topic != config.topic_contract.topic:
            raise R2DCaptureError("physical record topic differs from configured topic")
        if record.partition >= config.topic_contract.partition_count:
            raise R2DCaptureError("physical record partition is outside the configured topology")
        candidate = _captured_record(record, config)
        existing = captured_by_identity.get(record.transport_identity)
        if existing is None:
            captured_by_identity[record.transport_identity] = candidate
            continue
        if existing.physical_fingerprint != candidate.physical_fingerprint:
            raise R2DPhysicalRecordConflict(
                f"transport identity conflict for {record.topic}/{record.partition}/{record.offset}"
            )

    if len(captured_by_identity) > config.admission_contract.maximum_unique_records:
        raise R2DCaptureError("capture exceeds maximum_unique_records")
    captured = tuple(
        sorted(
            captured_by_identity.values(),
            key=lambda item: (item.record.partition, item.record.offset),
        )
    )
    payload = _transcript_payload(
        topic=config.topic_contract.topic,
        partition_count=config.topic_contract.partition_count,
        records=captured,
    )
    transcript = R2DCaptureTranscript(
        topic=config.topic_contract.topic,
        partition_count=config.topic_contract.partition_count,
        records=captured,
        content_sha256=canonical_json_sha256(payload),
        lineage_sha256=canonical_json_sha256(_transcript_lineage_payload(captured)),
    )
    return validate_capture_transcript(transcript, config)


def raw_value_multiset_sha256(values: Iterable[bytes]) -> str:
    """Hash the canonical sorted raw-value SHA Counter with duplicate multiplicity."""

    return r2d_sha256_multiset_sha256(tuple(hashlib.sha256(value).hexdigest() for value in values))


def _producer_ack_payload(
    *,
    config: R2DConfig,
    acknowledgements: tuple[R2DProducerAcknowledgement, ...],
    raw_multiset_sha256: str,
    frozen_source_match: bool,
) -> dict[str, object]:
    source = config.source_contract
    return {
        "contract_version": "r2d.a1.portable-producer-ack.v1",
        "release": "R2D-A1",
        "acknowledgement_kind": "simulated_portable",
        "real_broker_ack_claim": False,
        "source_manifest_sha256": source.source_manifest_sha256,
        "source_manifest_file_sha256": source.source_manifest_file_sha256,
        "inventory_events_file_sha256": source.inventory_events_file_sha256,
        "acknowledged_record_count": len(acknowledgements),
        "acknowledgements": [item.model_dump(mode="json") for item in acknowledgements],
        "raw_value_multiset_sha256": raw_multiset_sha256,
        "frozen_source_match": frozen_source_match,
    }


def build_portable_producer_ack_ledger(
    records: Iterable[R2DPhysicalRecord],
    config: R2DConfig,
) -> R2DProducerAckLedger:
    """Build deterministic simulated acknowledgements without a real-broker claim."""

    physical_records = tuple(records)
    identities = tuple(record.transport_identity for record in physical_records)
    if len(identities) != len(set(identities)):
        raise R2DCaptureError("producer acknowledgement input has duplicate transport identities")
    acknowledgements: list[R2DProducerAcknowledgement] = []
    raw_values: list[bytes] = []
    for record in physical_records:
        if record.topic != config.topic_contract.topic:
            raise R2DCaptureError("producer acknowledgement topic differs from config")
        if record.partition >= config.topic_contract.partition_count:
            raise R2DCaptureError("producer acknowledgement partition is outside config")
        key_bytes = record.key_bytes
        if not key_bytes:
            raise R2DCaptureError("producer acknowledgement requires nonempty exact key bytes")
        value = record.value_bytes
        if value is None:
            raise R2DCaptureError("producer acknowledgement requires exact raw value bytes")
        raw_values.append(value)
        decision = classify_transport_record(record, config)
        if decision.disposition != "admitted":
            raise R2DCaptureError(
                f"producer acknowledgement requires transport admission: {decision.reason}"
            )
        acknowledgements.append(
            R2DProducerAcknowledgement(
                acknowledgement_kind="simulated_portable",
                real_broker_ack_claim=False,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                key_hex=key_bytes.hex(),
                raw_value_sha256=hashlib.sha256(value).hexdigest(),
            )
        )
    ordered = tuple(sorted(acknowledgements, key=lambda item: (item.partition, item.offset)))
    multiset_sha256 = raw_value_multiset_sha256(raw_values)
    source = config.source_contract
    frozen_source_match = (
        len(ordered) == source.inventory_event_count
        and multiset_sha256 == source.raw_value_multiset_sha256
    )
    payload = _producer_ack_payload(
        config=config,
        acknowledgements=ordered,
        raw_multiset_sha256=multiset_sha256,
        frozen_source_match=frozen_source_match,
    )
    return R2DProducerAckLedger.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def validate_portable_producer_ack_ledger(
    ledger: R2DProducerAckLedger,
    records: Iterable[R2DPhysicalRecord],
    config: R2DConfig,
    *,
    require_frozen_source: bool = False,
) -> R2DProducerAckLedger:
    """Rebuild every acknowledgement identity and fail closed on any difference."""

    validated = R2DProducerAckLedger.model_validate(ledger.model_dump(mode="python"))
    expected = build_portable_producer_ack_ledger(records, config)
    if validated != expected:
        raise R2DCaptureError("producer acknowledgement ledger differs from physical records")
    if require_frozen_source and not validated.frozen_source_match:
        raise R2DCaptureError("producer acknowledgement ledger does not match frozen source")
    return validated


def _checkpoint_payload(
    *,
    topic: str,
    source_manifest_sha256: str,
    config_sha256: str,
    transcript_sha256: str,
    producer_ack_sha256: str,
    producer_ack_frozen_source_match: bool,
    lineage_sha256: str,
    partitions: tuple[R2DPartitionCheckpoint, ...],
) -> dict[str, object]:
    return {
        "topic": topic,
        "source_manifest_sha256": source_manifest_sha256,
        "config_sha256": config_sha256,
        "transcript_sha256": transcript_sha256,
        "producer_ack_sha256": producer_ack_sha256,
        "producer_ack_frozen_source_match": producer_ack_frozen_source_match,
        "lineage_sha256": lineage_sha256,
        "partitions": [item.model_dump(mode="json") for item in partitions],
    }


def build_capture_checkpoint(
    *,
    config: R2DConfig,
    transcript: R2DCaptureTranscript,
    starts: Mapping[int, int],
    ends: Mapping[int, int] | None = None,
    next_offsets: Mapping[int, int] | None = None,
    producer_ack_ledger: R2DProducerAckLedger | None = None,
    require_frozen_source: bool = True,
) -> R2DCaptureCheckpoint:
    """Construct a content-addressed checkpoint from explicit partition vectors."""

    validate_capture_transcript(transcript, config)
    physical_records = tuple(item.record for item in transcript.records)
    ack_ledger = (
        build_portable_producer_ack_ledger(physical_records, config)
        if producer_ack_ledger is None
        else validate_portable_producer_ack_ledger(
            producer_ack_ledger,
            physical_records,
            config,
            require_frozen_source=require_frozen_source,
        )
    )
    if require_frozen_source and not ack_ledger.frozen_source_match:
        raise R2DCheckpointError(
            "publishable checkpoint requires acknowledgements matching the frozen source"
        )
    expected_partitions = set(range(config.topic_contract.partition_count))
    if set(starts) != expected_partitions:
        raise R2DCheckpointError("starts must contain exactly partitions 0 and 1")
    if ends is not None and set(ends) != expected_partitions:
        raise R2DCheckpointError("ends must contain exactly partitions 0 and 1")
    resolved_ends: dict[int, int] = {}
    for partition in sorted(expected_partitions):
        partition_records = transcript.partition_records(partition)
        inferred_end = (
            max(item.record.offset for item in partition_records) + 1
            if partition_records
            else starts[partition]
        )
        resolved_ends[partition] = inferred_end if ends is None else ends[partition]
    resolved_next = resolved_ends if next_offsets is None else dict(next_offsets)
    if set(resolved_next) != expected_partitions:
        raise R2DCheckpointError("next_offsets must contain exactly partitions 0 and 1")

    partitions = tuple(
        R2DPartitionCheckpoint(
            partition=partition,
            start_offset=starts[partition],
            end_offset_exclusive=resolved_ends[partition],
            next_offset=resolved_next[partition],
            physical_fingerprints=tuple(
                item.physical_fingerprint for item in transcript.partition_records(partition)
            ),
        )
        for partition in sorted(expected_partitions)
    )
    config_hash = r2d_config_sha256(config)
    payload = _checkpoint_payload(
        topic=transcript.topic,
        source_manifest_sha256=config.source_contract.source_manifest_sha256,
        config_sha256=config_hash,
        transcript_sha256=transcript.content_sha256,
        producer_ack_sha256=ack_ledger.content_sha256,
        producer_ack_frozen_source_match=ack_ledger.frozen_source_match,
        lineage_sha256=transcript.lineage_sha256,
        partitions=partitions,
    )
    return R2DCaptureCheckpoint(
        topic=transcript.topic,
        source_manifest_sha256=config.source_contract.source_manifest_sha256,
        config_sha256=config_hash,
        transcript_sha256=transcript.content_sha256,
        producer_ack_sha256=ack_ledger.content_sha256,
        producer_ack_frozen_source_match=ack_ledger.frozen_source_match,
        lineage_sha256=transcript.lineage_sha256,
        partitions=partitions,
        content_sha256=canonical_json_sha256(payload),
    )


def verify_complete_checkpoint(
    checkpoint: R2DCaptureCheckpoint,
    transcript: R2DCaptureTranscript,
    config: R2DConfig,
    producer_ack_ledger: R2DProducerAckLedger | None = None,
    require_frozen_source: bool = True,
) -> R2DCaptureCheckpoint:
    """Fail closed unless every half-open partition range is exact and contiguous."""

    validate_capture_transcript(transcript, config)
    expected_checkpoint_hash = canonical_json_sha256(
        _checkpoint_payload(
            topic=checkpoint.topic,
            source_manifest_sha256=checkpoint.source_manifest_sha256,
            config_sha256=checkpoint.config_sha256,
            transcript_sha256=checkpoint.transcript_sha256,
            producer_ack_sha256=checkpoint.producer_ack_sha256,
            producer_ack_frozen_source_match=(checkpoint.producer_ack_frozen_source_match),
            lineage_sha256=checkpoint.lineage_sha256,
            partitions=checkpoint.partitions,
        )
    )
    if checkpoint.content_sha256 != expected_checkpoint_hash:
        raise R2DCheckpointError("checkpoint content SHA-256 mismatch")
    if checkpoint.topic != config.topic_contract.topic or checkpoint.topic != transcript.topic:
        raise R2DCheckpointError("checkpoint topic does not match config and transcript")
    if checkpoint.config_sha256 != r2d_config_sha256(config):
        raise R2DCheckpointError("checkpoint configuration SHA-256 mismatch")
    if checkpoint.source_manifest_sha256 != config.source_contract.source_manifest_sha256:
        raise R2DCheckpointError("checkpoint source-manifest SHA-256 mismatch")
    if checkpoint.transcript_sha256 != transcript.content_sha256:
        raise R2DCheckpointError("checkpoint transcript SHA-256 mismatch")
    if checkpoint.lineage_sha256 != transcript.lineage_sha256:
        raise R2DCheckpointError("checkpoint lineage SHA-256 mismatch")
    physical_records = tuple(item.record for item in transcript.records)
    ack_ledger = (
        build_portable_producer_ack_ledger(physical_records, config)
        if producer_ack_ledger is None
        else validate_portable_producer_ack_ledger(
            producer_ack_ledger,
            physical_records,
            config,
            require_frozen_source=require_frozen_source,
        )
    )
    if require_frozen_source and not ack_ledger.frozen_source_match:
        raise R2DCheckpointError(
            "publishable checkpoint requires acknowledgements matching the frozen source"
        )
    if checkpoint.producer_ack_sha256 != ack_ledger.content_sha256:
        raise R2DCheckpointError("checkpoint producer-ack SHA-256 mismatch")
    if checkpoint.producer_ack_frozen_source_match != ack_ledger.frozen_source_match:
        raise R2DCheckpointError("checkpoint producer-ack source-match state mismatch")

    expected_partitions = tuple(range(config.topic_contract.partition_count))
    if tuple(item.partition for item in checkpoint.partitions) != expected_partitions:
        raise R2DCheckpointError("checkpoint must contain ordered partitions 0 and 1 exactly once")
    for barrier in checkpoint.partitions:
        if barrier.next_offset != barrier.end_offset_exclusive:
            raise R2DCheckpointError(
                f"partition {barrier.partition} has not reached its exclusive end offset"
            )
        records = transcript.partition_records(barrier.partition)
        offsets = tuple(item.record.offset for item in records)
        expected_offsets = tuple(range(barrier.start_offset, barrier.end_offset_exclusive))
        if offsets != expected_offsets:
            raise R2DCheckpointError(
                f"partition {barrier.partition} is missing or has out-of-range offsets"
            )
        fingerprints = tuple(item.physical_fingerprint for item in records)
        if barrier.physical_fingerprints != fingerprints:
            raise R2DCheckpointError(
                f"partition {barrier.partition} fingerprint sequence does not match transcript"
            )
    return checkpoint


def raw_value_hash_counts(values: Iterable[bytes]) -> Counter[str]:
    """Count each exact record value hash, retaining duplicate multiplicity."""

    return Counter(hashlib.sha256(value).hexdigest() for value in values)


def reconstruct_jsonl_bytes(transcript: R2DCaptureTranscript, config: R2DConfig) -> bytes:
    """Reconstruct admitted bytes in deterministic partition/offset order."""

    validate_capture_transcript(transcript, config)
    values = tuple(
        item.record.value_bytes
        for item in transcript.records
        if item.decision.disposition == "admitted"
    )
    admitted = tuple(value for value in values if value is not None)
    return b"" if not admitted else b"\n".join(admitted) + b"\n"
