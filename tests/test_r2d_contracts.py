from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from pydantic import ValidationError

import nexus.r2d as r2d_module
from nexus.ingestion import (
    R2D_CAPTURE_TRANSCRIPT_SHA256,
    R2D_CORRECTION_SEMANTICS_PROJECTION_SHA256,
    R2D_DIRECT_PROVENANCE_PROJECTION_SHA256,
    R2D_INVENTORY_EVENTS_FILE_SHA256,
    R2D_NORMALIZED_BUSINESS_SHA256,
    R2D_PORTABLE_RECOVERY_PROJECTION_SHA256,
    R2D_RAW_VALUE_MULTISET_SHA256,
    R2D_SCHEMA_BUNDLE_SHA256,
    R2D_SOURCE_MANIFEST_FILE_SHA256,
    R2D_SOURCE_MANIFEST_SHA256,
    R2DConfig,
    R2DHeader,
    R2DPhysicalRecord,
    load_r2d_config,
    partition_for_key,
    physical_record_fingerprint,
    r2d_config_sha256,
    r2d_lf_canonical_file_sha256,
    r2d_schema_bundle_payload,
    r2d_schema_bundle_sha256,
)
from nexus.ingestion.r2d_contracts import R2DTransportDecision

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "ingestion" / "r2d-a1.yaml"


def _payload() -> dict[str, Any]:
    return cast(dict[str, Any], yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")))


def _record(headers: tuple[R2DHeader, ...]) -> R2DPhysicalRecord:
    return R2DPhysicalRecord.from_bytes(
        topic="inventory.adjusted.v1",
        partition=0,
        offset=7,
        key=b"synthetic.wms:ADJ-001",
        headers=tuple((header.name, header.value_bytes) for header in headers),
        broker_timestamp_ms=1_784_347_200_000,
        broker_timestamp_type="create_time",
        value=b'{"source":"synthetic.wms","payload":{"logical_event_id":"ADJ-001"}}',
    )


def test_config_is_strict_frozen_bounded_and_explicitly_non_operational() -> None:
    config = load_r2d_config()

    assert config.release == "R2D-A1"
    assert config.source_contract.model_dump() == {
        "generator": "nexus-r2-supply-network",
        "source_manifest_sha256": R2D_SOURCE_MANIFEST_SHA256,
        "source_manifest_file_sha256": R2D_SOURCE_MANIFEST_FILE_SHA256,
        "inventory_events_file_sha256": R2D_INVENTORY_EVENTS_FILE_SHA256,
        "inventory_event_count": 9,
        "raw_value_multiset_sha256": R2D_RAW_VALUE_MULTISET_SHA256,
    }
    assert config.topic_contract.model_dump() == {
        "topic": "inventory.adjusted.v1",
        "partition_count": 2,
        "replication_factor": 1,
        "key_format": "source:logical_event_id",
        "partitioner": "sha256_modulo",
        "reconstruction_order": "partition_then_offset",
        "future_service_producer_acks": "all",
        "portable_acknowledgement_kind": "simulated_portable",
        "real_broker_ack_claim": False,
        "consumer_auto_offset_commit": False,
        "routing_key_overrides": (
            {
                "raw_value_sha256": (
                    "c3b9d488773f31ee841b256a2da6f6a6b1d9224a06268abe1d33000814eb38ec"
                ),
                "routing_key": "synthetic.wms:ADJ-MALFORMED-001",
            },
        ),
    }
    assert config.admission_contract.maximum_unique_records == 64
    assert config.admission_contract.nominal_value_bytes == 1024
    assert config.admission_contract.maximum_value_bytes == 4096
    assert tuple(
        cutoff.planning_cutoff for cutoff in config.snapshot_contract.planning_cutoffs
    ) == (
        datetime(2026, 6, 20, tzinfo=UTC),
        datetime(2026, 6, 30, tzinfo=UTC),
    )
    assert config.scope_contract.broker_dependency == "none"
    assert config.scope_contract.database_dependency == "none"
    assert config.scope_contract.operational_recovery_claim is False
    assert config.scope_contract.exactly_once_claim is False
    assert config.expected_evidence_contract.model_dump() == {
        "contract_version": "r2d.a1.expected-evidence.v1",
        "correction_projection": "without_file_evidence",
        "portable_recovery_projection": "without_checkpoint_evidence",
        "direct_provenance_projection": "without_file_evidence",
        "capture_transcript_sha256": R2D_CAPTURE_TRANSCRIPT_SHA256,
        "normalized_business_sha256": R2D_NORMALIZED_BUSINESS_SHA256,
        "correction_semantics_projection_sha256": (R2D_CORRECTION_SEMANTICS_PROJECTION_SHA256),
        "portable_recovery_projection_sha256": R2D_PORTABLE_RECOVERY_PROJECTION_SHA256,
        "direct_provenance_projection_sha256": R2D_DIRECT_PROVENANCE_PROJECTION_SHA256,
    }

    with pytest.raises(ValidationError, match="frozen"):
        config.topic_contract.partition_count = 3
    with pytest.raises(ValidationError, match="frozen"):
        config.expected_evidence_contract.correction_projection = "identity"  # type: ignore[assignment]


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda value: value.update({"unknown": True}), "Extra inputs are not permitted"),
        (
            lambda value: value["topic_contract"].update({"partition_count": 3}),
            "Input should be 2",
        ),
        (
            lambda value: value["admission_contract"].update({"maximum_value_bytes": 8192}),
            "Input should be 4096",
        ),
        (
            lambda value: value["snapshot_contract"]["planning_cutoffs"][0].update(
                {"planning_cutoff": "2026-06-21T00:00:00Z"}
            ),
            "exact frozen UTC pair",
        ),
        (
            lambda value: value["source_contract"].update({"inventory_event_count": 8}),
            "Input should be 9",
        ),
        (
            lambda value: value["expected_evidence_contract"].update(
                {"normalized_business_sha256": "0" * 64}
            ),
            "Input should be",
        ),
        (
            lambda value: value["expected_evidence_contract"].update(
                {"correction_projection": "identity"}
            ),
            "without_file_evidence",
        ),
        (
            lambda value: value["expected_evidence_contract"].update(
                {"portable_recovery_projection": "identity"}
            ),
            "without_checkpoint_evidence",
        ),
        (
            lambda value: value["expected_evidence_contract"].update(
                {"contract_version": "r2d.a1.expected-evidence.v2"}
            ),
            "r2d.a1.expected-evidence.v1",
        ),
        (
            lambda value: value["expected_evidence_contract"].update(
                {"correction_semantics_projection_sha256": "0" * 64}
            ),
            "Input should be",
        ),
        (
            lambda value: value["expected_evidence_contract"].update(
                {"portable_recovery_projection_sha256": "0" * 64}
            ),
            "Input should be",
        ),
        (
            lambda value: value["expected_evidence_contract"].update(
                {"direct_provenance_projection": "identity"}
            ),
            "without_file_evidence",
        ),
        (
            lambda value: value["expected_evidence_contract"].update(
                {"capture_transcript_sha256": "0" * 64}
            ),
            "Input should be",
        ),
        (
            lambda value: value["expected_evidence_contract"].update(
                {"direct_provenance_projection_sha256": "0" * 64}
            ),
            "Input should be",
        ),
        (
            lambda value: value["expected_evidence_contract"].update({"unknown": True}),
            "Extra inputs are not permitted",
        ),
    ],
)
def test_unknown_or_tampered_configuration_fails_closed(mutator: Any, message: str) -> None:
    payload = deepcopy(_payload())
    mutator(payload)

    with pytest.raises(ValidationError, match=message):
        R2DConfig.model_validate(payload)


def test_config_hash_is_semantic_and_schema_hashes_bind_checked_in_bytes(
    tmp_path: Path,
) -> None:
    config = load_r2d_config()
    reordered = _payload()
    reordered_path = tmp_path / "equivalent.yaml"
    reordered_path.write_text(yaml.safe_dump(reordered, sort_keys=True), encoding="utf-8")

    assert r2d_config_sha256(load_r2d_config(reordered_path)) == r2d_config_sha256(config)
    assert config.schema_contract.envelope_schema_sha256 == r2d_lf_canonical_file_sha256(
        ROOT / "contracts" / "schemas" / "event-envelope.schema.json"
    )
    assert config.schema_contract.payload_schema_sha256 == r2d_lf_canonical_file_sha256(
        ROOT / "contracts" / "schemas" / "r2-inventory-adjustment-payload.schema.json"
    )
    assert r2d_schema_bundle_payload(config.schema_contract) == {
        "contract_version": "r2d.schema-bundle.v1",
        "schemas": [
            {
                "contract_id": "https://nexus.local/contracts/event-envelope/1.0",
                "sha256": config.schema_contract.envelope_schema_sha256,
            },
            {
                "contract_id": (
                    "https://nexus.local/contracts/r2-inventory-adjustment-payload.schema.json"
                ),
                "sha256": config.schema_contract.payload_schema_sha256,
            },
        ],
    }
    assert r2d_schema_bundle_sha256(config.schema_contract) == R2D_SCHEMA_BUNDLE_SHA256
    assert config.schema_contract.schema_bundle_sha256 == R2D_SCHEMA_BUNDLE_SHA256
    assert config.header_contract.schema_header_value == R2D_SCHEMA_BUNDLE_SHA256


def test_file_hash_canonicalizes_crlf_and_bare_cr_to_lf(tmp_path: Path) -> None:
    lf_path = tmp_path / "lf.schema.json"
    crlf_path = tmp_path / "crlf.schema.json"
    cr_path = tmp_path / "cr.schema.json"
    lf_bytes = b'{\n  "type": "object"\n}\n'
    lf_path.write_bytes(lf_bytes)
    crlf_path.write_bytes(lf_bytes.replace(b"\n", b"\r\n"))
    cr_path.write_bytes(lf_bytes.replace(b"\n", b"\r"))

    expected = hashlib.sha256(lf_bytes).hexdigest()
    assert r2d_lf_canonical_file_sha256(lf_path) == expected
    assert r2d_lf_canonical_file_sha256(crlf_path) == expected
    assert r2d_lf_canonical_file_sha256(cr_path) == expected


def test_code_bundle_hash_is_stable_across_text_line_endings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code_path = tmp_path / "component.py"
    lf_bytes = b'print("stable")\n'
    monkeypatch.setattr(r2d_module, "_PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(r2d_module, "_CODE_BUNDLE_FILES", ("component.py",))
    monkeypatch.setattr(r2d_module, "r2_dbt_code_sha256", lambda _: "a" * 64)
    monkeypatch.setattr(r2d_module, "r2_pipeline_code_sha256", lambda _: "b" * 64)

    code_path.write_bytes(lf_bytes)
    expected = r2d_module._code_bundle_sha256()
    code_path.write_bytes(lf_bytes.replace(b"\n", b"\r\n"))
    assert r2d_module._code_bundle_sha256() == expected
    code_path.write_bytes(lf_bytes.replace(b"\n", b"\r"))
    assert r2d_module._code_bundle_sha256() == expected


@pytest.mark.parametrize("field", ["key_hex", "value_hex"])
@pytest.mark.parametrize("bad_hex", ["a", "AA", "0g"])
def test_hex_byte_contracts_reject_odd_uppercase_or_non_hex(
    field: str,
    bad_hex: str,
) -> None:
    payload = _record(()).model_dump()
    payload[field] = bad_hex

    with pytest.raises(ValidationError):
        R2DPhysicalRecord.model_validate(payload)


def test_sha256_modulo_partitioning_is_stable_and_colocates_equal_keys() -> None:
    key = b"synthetic.wms:ADJ-CYCLE-001"
    expected = int.from_bytes(hashlib.sha256(key).digest(), "big") % 2

    assert partition_for_key(key) == expected
    assert partition_for_key(key) == partition_for_key(bytes(key))
    assert partition_for_key(key, partition_count=7) == (
        int.from_bytes(hashlib.sha256(key).digest(), "big") % 7
    )
    with pytest.raises(ValueError, match="nonempty"):
        partition_for_key(b"")
    with pytest.raises(ValueError, match="positive"):
        partition_for_key(key, partition_count=0)


def test_header_fingerprint_is_order_independent_but_duplicate_preserving() -> None:
    first = R2DHeader.from_bytes("traceparent", b"trace-a")
    second = R2DHeader.from_bytes("traceparent", b"trace-b")
    decision = R2DTransportDecision(disposition="admitted", reason="admitted")

    ordered = _record((first, second))
    reversed_record = _record((second, first))
    dropped_duplicate = _record((first,))

    assert [item.value_bytes for item in ordered.headers] == [b"trace-a", b"trace-b"]
    assert physical_record_fingerprint(ordered, decision) == physical_record_fingerprint(
        reversed_record, decision
    )
    assert physical_record_fingerprint(ordered, decision) != physical_record_fingerprint(
        dropped_duplicate, decision
    )
