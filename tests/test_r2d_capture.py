from __future__ import annotations

import hashlib
import json
from collections import Counter
from copy import deepcopy
from typing import Any

import pytest

from nexus.ingestion import (
    R2DCaptureCheckpoint,
    R2DCaptureError,
    R2DCheckpointError,
    R2DEvidenceCheckpoint,
    R2DHeader,
    R2DParsedLineage,
    R2DPhysicalRecord,
    R2DPhysicalRecordConflict,
    R2DProducerAckLedger,
    R2DTransportDecision,
    build_capture_checkpoint,
    build_portable_producer_ack_ledger,
    capture_transcript,
    classify_transport_record,
    load_r2d_config,
    partition_for_key,
    physical_record_fingerprint,
    raw_value_hash_counts,
    raw_value_multiset_sha256,
    reconstruct_jsonl_bytes,
    validate_capture_transcript,
    validate_portable_producer_ack_ledger,
    verify_complete_checkpoint,
)
from nexus.ingestion.r2d_contracts import canonical_json_sha256


def _event_bytes(logical_event_id: str, *, delta_qty: int = 1) -> bytes:
    return json.dumps(
        {
            "contract_version": "1.0",
            "event_id": f"event-{logical_event_id}",
            "event_type": "inventory.adjusted",
            "event_version": 1,
            "source": "synthetic.wms",
            "subject_id": "SKU-001:DC-ARG-01",
            "occurred_at": "2026-06-01T00:00:00Z",
            "ingested_at": "2026-06-01T00:00:01Z",
            "idempotency_key": f"idem-{logical_event_id}",
            "payload": {
                "logical_event_id": logical_event_id,
                "revision": 1,
                "sku_id": "SKU-001",
                "location_id": "DC-ARG-01",
                "delta_qty": delta_qty,
                "reason_code": "test",
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _headers(
    key: bytes, *, extra: tuple[tuple[str, bytes | None], ...] = ()
) -> tuple[tuple[str, bytes | None], ...]:
    config = load_r2d_config()
    return (
        (config.header_contract.contract_header_name, b"inventory.adjusted.v1"),
        (
            config.header_contract.schema_header_name,
            config.schema_contract.schema_bundle_sha256.encode(),
        ),
        (config.header_contract.routing_header_name, key),
        *extra,
    )


def _record(
    *,
    logical_event_id: str = "ADJ-001",
    offset: int = 0,
    partition: int | None = None,
    value: bytes | None = None,
    key: bytes | None = None,
    headers: tuple[tuple[str, bytes | None], ...] | None = None,
    timestamp: int = 1_784_347_200_000,
) -> R2DPhysicalRecord:
    resolved_value = _event_bytes(logical_event_id) if value is None else value
    resolved_key = f"synthetic.wms:{logical_event_id}".encode() if key is None else key
    resolved_partition = partition_for_key(resolved_key) if partition is None else partition
    resolved_headers = _headers(resolved_key) if headers is None else headers
    return R2DPhysicalRecord.from_bytes(
        topic="inventory.adjusted.v1",
        partition=resolved_partition,
        offset=offset,
        key=resolved_key,
        headers=resolved_headers,
        broker_timestamp_ms=timestamp,
        broker_timestamp_type="create_time",
        value=resolved_value,
    )


@pytest.mark.parametrize(
    ("record", "reason"),
    [
        (
            R2DPhysicalRecord.from_bytes(
                topic="inventory.adjusted.v1",
                partition=0,
                offset=0,
                key=None,
                headers=(),
                broker_timestamp_ms=None,
                broker_timestamp_type="not_available",
                value=None,
            ),
            "tombstone",
        ),
        (_record(value=b"", key=b"wrong", partition=0, headers=()), "empty"),
        (_record(value=b"x" * 4097, key=b"wrong", partition=0, headers=()), "oversized"),
        (_record(value=b"\xff\n", key=b"wrong", partition=0, headers=()), "non_utf8"),
        (_record(value=b" \t\n", key=b"wrong", partition=0, headers=()), "whitespace_only"),
        (_record(value=b"{}\n", key=b"wrong", partition=0, headers=()), "newline_unsafe"),
        (_record(key=b"wrong", partition=0), "key_mismatch"),
        (_record(headers=()), "header_mismatch"),
        (_record(), "admitted"),
    ],
)
def test_every_transport_admission_class_and_precedence(
    record: R2DPhysicalRecord,
    reason: str,
) -> None:
    decision = classify_transport_record(record, load_r2d_config())

    assert decision.reason == reason
    assert decision.disposition == ("admitted" if reason == "admitted" else "excluded")


@pytest.mark.parametrize(
    ("value", "key"),
    [
        (b'{"contract_version":"99","unknown":true}', b"opaque-route"),
        (b'{"source":"synthetic.wms","payload":', b"opaque-route"),
        (b"not-json-at-all", b"opaque-route"),
        (_event_bytes("ADJ-ZERO", delta_qty=0), b"synthetic.wms:ADJ-ZERO"),
    ],
)
def test_safely_frameable_r2a_invalid_values_are_still_admitted(
    value: bytes,
    key: bytes,
) -> None:
    record = _record(
        value=value,
        key=key,
        partition=partition_for_key(key),
        headers=_headers(key),
    )

    assert classify_transport_record(record, load_r2d_config()).reason == "admitted"


def test_redelivery_deduplicates_only_exact_offset_and_conflict_fails_closed() -> None:
    config = load_r2d_config()
    original = _record()
    later_semantic_duplicate = original.model_copy(update={"offset": 1})

    transcript = capture_transcript(
        (original, original, later_semantic_duplicate),
        config,
    )

    assert len(transcript.records) == 2
    assert [item.record.offset for item in transcript.records] == [0, 1]

    conflicting = original.model_copy(update={"value_hex": b"changed".hex()})
    with pytest.raises(R2DPhysicalRecordConflict, match="transport identity conflict"):
        capture_transcript((original, conflicting), config)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("key_hex", b"other".hex()),
        ("headers", (R2DHeader.from_bytes("traceparent", b"changed"),)),
        ("broker_timestamp_ms", 1_784_347_200_001),
        ("value_hex", b"changed".hex()),
    ],
)
def test_every_physical_mutation_changes_fingerprint(field: str, replacement: Any) -> None:
    record = _record()
    decision = classify_transport_record(record, load_r2d_config())
    changed = record.model_copy(update={field: replacement})
    changed_decision = classify_transport_record(changed, load_r2d_config())

    assert physical_record_fingerprint(record, decision) != physical_record_fingerprint(
        changed, changed_decision
    )

    excluded = R2DTransportDecision(disposition="excluded", reason="header_mismatch")
    assert physical_record_fingerprint(record, decision) != physical_record_fingerprint(
        record, excluded
    )


def test_parsed_lineage_does_not_change_physical_fingerprint() -> None:
    record = _record()
    decision = classify_transport_record(record, load_r2d_config())
    changed = record.model_copy(
        update={"lineage": R2DParsedLineage(event_id="untrusted-parser-output")}
    )

    assert physical_record_fingerprint(record, decision) == physical_record_fingerprint(
        changed, decision
    )


def test_lineage_is_reparsed_from_raw_bytes_and_bound_separately() -> None:
    config = load_r2d_config()
    transcript = capture_transcript((_record(),), config)
    captured = transcript.records[0]
    assert captured.record.lineage is not None
    changed_record = captured.record.model_copy(
        update={"lineage": R2DParsedLineage(event_id="fabricated-lineage")}
    )
    changed_capture = captured.model_copy(update={"record": changed_record})
    tampered = transcript.model_copy(update={"records": (changed_capture,)})

    assert changed_capture.physical_fingerprint == captured.physical_fingerprint
    with pytest.raises(R2DCaptureError, match="lineage differs from the raw value"):
        validate_capture_transcript(tampered, config)

    changed_hash = transcript.model_copy(update={"lineage_sha256": "f" * 64})
    with pytest.raises(R2DCaptureError, match="transcript lineage SHA-256 mismatch"):
        validate_capture_transcript(changed_hash, config)


def _two_partition_records() -> tuple[R2DPhysicalRecord, ...]:
    by_partition: dict[int, list[str]] = {0: [], 1: []}
    candidate = 0
    while any(len(items) < 2 for items in by_partition.values()):
        logical_id = f"ADJ-{candidate:03d}"
        key = f"synthetic.wms:{logical_id}".encode()
        partition = partition_for_key(key)
        if len(by_partition[partition]) < 2:
            by_partition[partition].append(logical_id)
        candidate += 1
    records = [
        _record(logical_event_id=logical_id, partition=partition, offset=offset)
        for partition, logical_ids in by_partition.items()
        for offset, logical_id in enumerate(logical_ids, start=3 + partition * 5)
    ]
    return tuple(records)


def test_poll_order_does_not_change_transcript_or_reconstructed_jsonl() -> None:
    config = load_r2d_config()
    records = _two_partition_records()
    first = capture_transcript((records[3], records[0], records[2], records[1]), config)
    second = capture_transcript(reversed(records), config)

    assert first == second
    assert [(item.record.partition, item.record.offset) for item in first.records] == [
        (0, 3),
        (0, 4),
        (1, 8),
        (1, 9),
    ]
    expected_values = [item.record.value_bytes for item in first.records]
    assert reconstruct_jsonl_bytes(first, config) == b"\n".join(expected_values) + b"\n"  # type: ignore[arg-type]


def test_complete_checkpoint_supports_interleaving_and_an_empty_partition() -> None:
    config = load_r2d_config()
    one_partition = next(record for record in _two_partition_records() if record.partition == 0)
    one_partition = one_partition.model_copy(update={"offset": 5})
    transcript = capture_transcript((one_partition,), config)
    ledger = build_portable_producer_ack_ledger(
        (item.record for item in transcript.records),
        config,
    )
    checkpoint = build_capture_checkpoint(
        config=config,
        transcript=transcript,
        starts={0: 5, 1: 11},
        producer_ack_ledger=ledger,
        require_frozen_source=False,
    )

    assert (
        verify_complete_checkpoint(
            checkpoint,
            transcript,
            config,
            producer_ack_ledger=ledger,
            require_frozen_source=False,
        )
        is checkpoint
    )
    assert checkpoint.source_manifest_sha256 == config.source_contract.source_manifest_sha256
    assert checkpoint.producer_ack_sha256 == ledger.content_sha256
    assert checkpoint.lineage_sha256 == transcript.lineage_sha256
    assert checkpoint.partitions[0].end_offset_exclusive == 6
    assert checkpoint.partitions[1].start_offset == 11
    assert checkpoint.partitions[1].end_offset_exclusive == 11


def test_delayed_partition_gap_and_tamper_cannot_publish_complete_checkpoint() -> None:
    config = load_r2d_config()
    transcript = capture_transcript(_two_partition_records(), config)
    starts = {0: 3, 1: 8}

    delayed = build_capture_checkpoint(
        config=config,
        transcript=transcript,
        starts=starts,
        ends={0: 5, 1: 10},
        next_offsets={0: 5, 1: 9},
        require_frozen_source=False,
    )
    with pytest.raises(R2DCheckpointError, match="has not reached"):
        verify_complete_checkpoint(
            delayed,
            transcript,
            config,
            require_frozen_source=False,
        )

    gap_records = tuple(
        item for item in _two_partition_records() if not (item.partition == 0 and item.offset == 4)
    )
    gap = capture_transcript(gap_records, config)
    falsely_advanced = build_capture_checkpoint(
        config=config,
        transcript=gap,
        starts=starts,
        ends={0: 5, 1: 10},
        require_frozen_source=False,
    )
    with pytest.raises(R2DCheckpointError, match="missing or has out-of-range"):
        verify_complete_checkpoint(
            falsely_advanced,
            gap,
            config,
            require_frozen_source=False,
        )

    complete = build_capture_checkpoint(
        config=config,
        transcript=transcript,
        starts=starts,
        require_frozen_source=False,
    )
    tampered_payload = deepcopy(complete.model_dump())
    changed = list(tampered_payload["partitions"][0]["physical_fingerprints"])
    changed[0] = "f" * 64
    tampered_payload["partitions"][0]["physical_fingerprints"] = tuple(changed)
    tampered = R2DCaptureCheckpoint.model_validate(tampered_payload)
    with pytest.raises(R2DCheckpointError, match="content SHA-256 mismatch"):
        verify_complete_checkpoint(
            tampered,
            transcript,
            config,
            require_frozen_source=False,
        )


def test_transcript_content_address_detects_nested_tampering() -> None:
    config = load_r2d_config()
    transcript = capture_transcript((_record(),), config)
    captured = transcript.records[0]
    changed_record = captured.record.model_copy(update={"value_hex": b"changed".hex()})
    changed_capture = captured.model_copy(update={"record": changed_record})
    tampered = transcript.model_copy(update={"records": (changed_capture,)})

    with pytest.raises(R2DCaptureError, match="raw-value SHA-256 mismatch"):
        validate_capture_transcript(tampered, config)


def test_raw_hash_counts_retain_duplicate_multiplicity_and_exclude_jsonl_delimiter() -> None:
    values = (b'{"a":1}', b'{"a":1}', b'{"a":2}')
    counts = raw_value_hash_counts(values)

    assert counts == Counter(
        {
            "015abd7f5cc57a2dd94b7590f04ad8084273905ee33ec5cebeae62276a97f862": 2,
            "7e8059f495589fcd981232cc11d00b00da3802c01d688fa1cf1f6bed6e5bb33c": 1,
        }
    )
    assert raw_value_multiset_sha256(values) == (
        "b2cf36c8a5996a2c948a7b2ff521a4e94ddc17334f6ae93104c846d7854d1450"
    )


def test_portable_ack_ledger_binds_tpo_key_raw_multiset_and_never_claims_broker_ack() -> None:
    config = load_r2d_config()
    records = _two_partition_records()
    ledger = build_portable_producer_ack_ledger(records, config)

    assert ledger.acknowledgement_kind == "simulated_portable"
    assert ledger.real_broker_ack_claim is False
    assert ledger.acknowledged_record_count == len(records)
    assert ledger.frozen_source_match is False
    assert tuple(
        (item.topic, item.partition, item.offset, item.key_hex, item.raw_value_sha256)
        for item in ledger.acknowledgements
    ) == tuple(
        (
            record.topic,
            record.partition,
            record.offset,
            record.key_hex,
            hashlib.sha256(record.value_bytes or b"").hexdigest(),
        )
        for record in sorted(records, key=lambda item: (item.partition, item.offset))
    )
    assert validate_portable_producer_ack_ledger(ledger, records, config) == ledger
    with pytest.raises(R2DCaptureError, match="does not match frozen source"):
        validate_portable_producer_ack_ledger(
            ledger,
            records,
            config,
            require_frozen_source=True,
        )

    payload = ledger.model_dump()
    payload["raw_value_multiset_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="raw-value multiset SHA-256 mismatch"):
        R2DProducerAckLedger.model_validate(payload)


def test_evidence_checkpoint_is_content_addressed_and_source_bound() -> None:
    config = load_r2d_config()
    evidence = R2DEvidenceCheckpoint.create(
        source_contract=config.source_contract,
        producer_ack_sha256="1" * 64,
        transcript_sha256="2" * 64,
        lineage_sha256="3" * 64,
        capture_checkpoint_sha256="4" * 64,
        reconstructed_jsonl_sha256="5" * 64,
        code_sha256="6" * 64,
        artifact_set_sha256="7" * 64,
    )

    assert evidence.source_manifest_sha256 == config.source_contract.source_manifest_sha256
    payload = evidence.model_dump()
    payload["code_sha256"] = "8" * 64
    with pytest.raises(ValueError, match="evidence checkpoint content SHA-256 mismatch"):
        R2DEvidenceCheckpoint.model_validate(payload)


def test_transport_decision_is_recomputed_even_after_consistent_rehash() -> None:
    config = load_r2d_config()
    transcript = capture_transcript((_record(headers=()),), config)
    captured = transcript.records[0]
    assert captured.decision.reason == "header_mismatch"

    forged_decision = R2DTransportDecision(disposition="admitted", reason="admitted")
    forged_capture = captured.model_copy(
        update={
            "decision": forged_decision,
            "physical_fingerprint": physical_record_fingerprint(
                captured.record,
                forged_decision,
            ),
        }
    )
    transcript_payload = {
        "topic": transcript.topic,
        "partition_count": transcript.partition_count,
        "records": [
            {
                "topic": forged_capture.record.topic,
                "partition": forged_capture.record.partition,
                "offset": forged_capture.record.offset,
                "physical_fingerprint": forged_capture.physical_fingerprint,
            }
        ],
    }
    forged = transcript.model_copy(
        update={
            "records": (forged_capture,),
            "content_sha256": canonical_json_sha256(transcript_payload),
        }
    )

    with pytest.raises(R2DCaptureError, match="transport decision differs"):
        validate_capture_transcript(forged, config)


def test_portable_ack_rejects_empty_or_misrouted_key_header_and_partition() -> None:
    config = load_r2d_config()
    valid = _record()
    wrong_key = b"wrong-route"
    invalid_records = (
        _record(
            key=b"",
            partition=0,
            headers=_headers(b""),
        ),
        valid.model_copy(update={"partition": 1 - valid.partition}),
        _record(headers=_headers(b"wrong-route")),
        _record(
            key=wrong_key,
            partition=partition_for_key(wrong_key),
            headers=_headers(wrong_key),
        ),
    )

    for record in invalid_records:
        with pytest.raises(R2DCaptureError, match="producer acknowledgement"):
            build_portable_producer_ack_ledger((record,), config)
