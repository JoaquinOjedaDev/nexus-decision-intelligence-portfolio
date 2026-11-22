from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from nexus.ingestion import (
    R2DPhysicalRecord,
    capture_transcript,
    load_r2d_config,
    partition_for_key,
)
from nexus.ingestion.r2d_b1 import (
    R2DB1GovernanceError,
    assess_backward_compatibility,
    build_dead_letter_record,
    classify_schema_value,
    create_redrive_request,
    validate_redrive_ledger,
)
from nexus.ingestion.r2d_b1_contracts import (
    R2DB1Config,
    R2DB1DeadLetterRecord,
    load_r2d_b1_config,
    r2d_b1_config_sha256,
)

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.r2d


def _event(
    *,
    event_id: str = "10000000-0000-4000-8000-000000000001",
    event_version: int = 1,
    idempotency_key: str = "idem-ADJ-B1-001",
    subject_id: str = "SKU-001:DC-ARG-01",
    logical_event_id: str = "ADJ-B1-001",
) -> dict[str, Any]:
    return {
        "contract_version": "1.0",
        "event_id": event_id,
        "event_type": "inventory.adjusted",
        "event_version": event_version,
        "source": "synthetic.wms",
        "subject_id": subject_id,
        "occurred_at": "2026-06-01T00:00:00Z",
        "ingested_at": "2026-06-01T00:00:01Z",
        "idempotency_key": idempotency_key,
        "payload": {
            "logical_event_id": logical_event_id,
            "revision": 1,
            "sku_id": "SKU-001",
            "location_id": "DC-ARG-01",
            "delta_qty": 1,
            "reason_code": "cycle_count",
        },
    }


def _bytes(event: dict[str, Any]) -> bytes:
    return json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _captured(raw_value: bytes, *, offset: int = 0):
    data_config = load_r2d_config()
    key = b"synthetic.wms:ADJ-B1-001"
    headers = (
        (
            data_config.header_contract.contract_header_name,
            data_config.header_contract.contract_header_value.encode("utf-8"),
        ),
        (
            data_config.header_contract.schema_header_name,
            data_config.header_contract.schema_header_value.encode("ascii"),
        ),
        (data_config.header_contract.routing_header_name, key),
    )
    record = R2DPhysicalRecord.from_bytes(
        topic=data_config.topic_contract.topic,
        partition=partition_for_key(key),
        offset=offset,
        key=key,
        headers=headers,
        broker_timestamp_ms=1_780_272_001_000,
        broker_timestamp_type="create_time",
        value=raw_value,
    )
    return capture_transcript((record,), data_config).records[0]


def _schema(name: str) -> dict[str, object]:
    return json.loads((ROOT / "contracts" / "schemas" / name).read_text(encoding="utf-8"))


def test_config_is_strict_frozen_and_binds_r2d_a() -> None:
    config = load_r2d_b1_config()

    assert config.release == "R2D-B1"
    assert config.schema_policy.business_validation_owner == "R2A"
    assert config.scope.service_dead_letter_topic_status == "NOT_RUN"
    assert config.scope.full_g2_status == "OPEN"
    assert r2d_b1_config_sha256(config) == (
        "61dacd3ef618138b2e91701c3bc032e91e978065c08bb916429e1a2f18323b3e"
    )
    with pytest.raises(ValidationError):
        config.release = "changed"  # type: ignore[misc]
    payload = config.model_dump(mode="json")
    payload["unexpected"] = True
    with pytest.raises(ValidationError):
        R2DB1Config.model_validate(payload)


@pytest.mark.parametrize(
    ("mutator", "reason"),
    [
        (lambda _: b"\xff", "invalid_utf8"),
        (lambda _: b'{"contract_version":', "invalid_json"),
        (lambda _: b"[]", "envelope_schema_invalid"),
        (
            lambda event: _bytes({**event, "contract_version": "2.0"}),
            "unsupported_contract_version",
        ),
        (
            lambda event: _bytes({**event, "event_type": "inventory.deleted"}),
            "unsupported_event_type",
        ),
        (
            lambda event: _bytes({**event, "event_version": 2}),
            "unsupported_event_version",
        ),
        (
            lambda event: _bytes({**event, "event_id": "not-a-uuid"}),
            "envelope_schema_invalid",
        ),
        (
            lambda event: _bytes(
                {
                    **event,
                    "payload": {**event["payload"], "unknown_field": "poison"},
                }
            ),
            "payload_schema_invalid",
        ),
    ],
)
def test_schema_poison_matrix_is_fail_closed(mutator: Any, reason: str) -> None:
    config = load_r2d_b1_config()
    raw = mutator(_event())

    decision = classify_schema_value(raw, config)

    assert decision.disposition == "dead_letter"
    assert decision.reason == reason


def test_defaults_and_business_boundary_are_preserved() -> None:
    config = load_r2d_b1_config()
    defaulted = _event(subject_id="WRONG:SUBJECT")
    del defaulted["contract_version"]
    del defaulted["event_version"]

    decision = classify_schema_value(_bytes(defaulted), config)

    assert decision.disposition == "forward"
    assert decision.reason == "compatible"
    assert decision.envelope_contract_version == "1.0"
    assert decision.event_version == 1
    # Subject/payload consistency is an R2A business rule, not a B1 schema rule.
    assert config.schema_policy.business_validation_owner == "R2A"


def test_conservative_compatibility_accepts_only_optional_additions() -> None:
    baseline = _schema("r2-inventory-adjustment-payload.schema.json")
    compatible = deepcopy(baseline)
    compatible["$id"] = "https://nexus.local/contracts/r2-inventory-adjustment-payload/1.1"
    compatible["properties"]["operator_note"] = {"type": ["string", "null"]}  # type: ignore[index]

    accepted = assess_backward_compatibility(baseline, compatible)

    assert accepted.compatible is True
    assert accepted.violations == ()
    assert accepted == assess_backward_compatibility(baseline, compatible)

    new_required = deepcopy(compatible)
    new_required["required"] = [*new_required["required"], "operator_note"]  # type: ignore[index]
    rejected = assess_backward_compatibility(baseline, new_required)
    assert rejected.compatible is False
    assert "new_required_property:operator_note" in rejected.violations


@pytest.mark.parametrize(
    "mutation",
    [
        "remove_property",
        "change_property",
        "change_additional_properties",
        "unsupported_keyword",
    ],
)
def test_conservative_compatibility_rejects_ambiguous_or_breaking_changes(
    mutation: str,
) -> None:
    baseline = _schema("r2-inventory-adjustment-payload.schema.json")
    candidate = deepcopy(baseline)
    if mutation == "remove_property":
        del candidate["properties"]["delta_qty"]  # type: ignore[index]
    elif mutation == "change_property":
        candidate["properties"]["delta_qty"] = {"type": "string"}  # type: ignore[index]
    elif mutation == "change_additional_properties":
        candidate["additionalProperties"] = True
    else:
        candidate["allOf"] = []

    assessment = assess_backward_compatibility(baseline, candidate)

    assert assessment.compatible is False
    assert assessment.violations


def test_dead_letter_preserves_exact_bytes_and_revalidates_transport() -> None:
    config = load_r2d_b1_config()
    data_config = load_r2d_config()
    poisoned = _bytes(_event(event_version=2))
    captured = _captured(poisoned)
    decision = classify_schema_value(poisoned, config)

    dead_letter = build_dead_letter_record(captured, decision, config, data_config)

    assert bytes.fromhex(dead_letter.original_raw_value_hex) == poisoned
    assert dead_letter.original_raw_value_sha256 == decision.raw_value_sha256
    assert dead_letter == build_dead_letter_record(captured, decision, config, data_config)
    tampered = dead_letter.model_dump(mode="json")
    tampered["original_raw_value_hex"] = b"changed".hex()
    with pytest.raises(ValidationError, match="raw-value SHA-256"):
        R2DB1DeadLetterRecord.model_validate_json(json.dumps(tampered))


def test_dead_letter_refuses_transport_excluded_input() -> None:
    config = load_r2d_b1_config()
    data_config = load_r2d_config()
    captured = _captured(b"\xff")
    decision = classify_schema_value(b"\xff", config)

    with pytest.raises(R2DB1GovernanceError, match="transport-admitted"):
        build_dead_letter_record(captured, decision, config, data_config)


def test_redrive_is_append_only_audited_and_uses_new_identity() -> None:
    config = load_r2d_b1_config()
    data_config = load_r2d_config()
    poisoned = _bytes(_event(event_version=2))
    captured = _captured(poisoned)
    dead_letter = build_dead_letter_record(
        captured,
        classify_schema_value(poisoned, config),
        config,
        data_config,
    )
    replacement = _bytes(
        _event(
            event_id="20000000-0000-4000-8000-000000000002",
            idempotency_key="idem-ADJ-B1-REDRIVE-001",
        )
    )

    request = create_redrive_request(
        dead_letter,
        replacement,
        operator_id="operator@example.com",
        reason="Correct unsupported event version after source-owner approval",
        submitted_at=datetime(2026, 8, 11, 20, 0, tzinfo=UTC),
        config=config,
    )

    assert request.causation_reference == dead_letter.dead_letter_id
    assert request.original_raw_value_sha256 == dead_letter.original_raw_value_sha256
    assert request.replacement_event_id == UUID("20000000-0000-4000-8000-000000000002")
    assert bytes.fromhex(dead_letter.original_raw_value_hex) == poisoned
    assert validate_redrive_ledger((request,), (dead_letter,), config) == (request,)


def test_redrive_rejects_reused_identity_invalid_replacement_and_detached_evidence() -> None:
    config = load_r2d_b1_config()
    data_config = load_r2d_config()
    original_event = _event(event_version=2)
    poisoned = _bytes(original_event)
    dead_letter = build_dead_letter_record(
        _captured(poisoned),
        classify_schema_value(poisoned, config),
        config,
        data_config,
    )
    timestamp = datetime(2026, 8, 11, 20, 0, tzinfo=UTC)

    with pytest.raises(R2DB1GovernanceError, match="new governed event identity"):
        create_redrive_request(
            dead_letter,
            _bytes(_event(event_version=1)),
            operator_id="operator@example.com",
            reason="attempted identity reuse",
            submitted_at=timestamp,
            config=config,
        )
    with pytest.raises(R2DB1GovernanceError, match="not schema-compatible"):
        create_redrive_request(
            dead_letter,
            b"not-json",
            operator_id="operator@example.com",
            reason="invalid replacement",
            submitted_at=timestamp,
            config=config,
        )

    replacement = _bytes(
        _event(
            event_id="30000000-0000-4000-8000-000000000003",
            idempotency_key="idem-ADJ-B1-REDRIVE-003",
        )
    )
    request = create_redrive_request(
        dead_letter,
        replacement,
        operator_id="operator@example.com",
        reason="approved correction",
        submitted_at=timestamp,
        config=config,
    )
    detached = request.model_copy(update={"dead_letter_content_sha256": "0" * 64})
    with pytest.raises((ValidationError, R2DB1GovernanceError)):
        validate_redrive_ledger((detached,), (dead_letter,), config)
