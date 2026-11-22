"""Portable recovery contracts: no automatic skip, persisted redrive or broker claim."""

import base64
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_portfolio_transport_ledger import binding as audit_binding

from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.auth import Actor
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.store import StoreError, canonical, digest
from nexus.portfolio.transport_ledger import TransportRecord
from nexus.portfolio.transport_recovery import RuntimeDeadLetter, RuntimeRedriveRequest
from nexus.simulation import generate_r2_smoke_fixture


@pytest.fixture
def case(tmp_path: Path) -> SimpleNamespace:
    binding = replace(audit_binding(), purpose="operations", runtime_target_sha256="a" * 64)
    root = tmp_path / "source"
    generate_r2_smoke_fixture(root)
    bundle = SourceBundle.capture(root)
    now = datetime(2026, 9, 25, tzinfo=UTC)
    event = EventEnvelope(
        event_id=uuid4(),
        event_type="inventory.adjusted",
        event_version=1,
        source="nexus.source-snapshot.v1",
        subject_id="portfolio",
        occurred_at=now,
        ingested_at=now,
        idempotency_key=uuid4().hex,
        payload={"source_snapshot_sha256": bundle.source_sha256},
    )
    record = TransportRecord(0, 10, b"portfolio", b"\xff\x00poison", (("x", None), ("x", b"")))
    original_hash = "b" * 64
    dead = RuntimeDeadLetter.create(binding, record, original_hash)
    return SimpleNamespace(
        binding=binding,
        bundle=bundle,
        now=now,
        event=event,
        record=record,
        original_hash=original_hash,
        dead=dead,
        operator=Actor(
            "planner.operations", "Planner", ActorRole.PLANNER, binding.broker.locations
        ),
    )


def proposal(case: SimpleNamespace, **kwargs: object) -> RuntimeRedriveRequest:
    options = dict(
        operator=case.operator, reason="Correct invalid source envelope", submitted_at=case.now
    )
    options.update(kwargs)
    return RuntimeRedriveRequest.create(
        case.dead,
        case.binding,
        case.record,
        case.original_hash,
        case.bundle,
        case.event.model_dump_json().encode(),
        **options,
    )


def verify(case: SimpleNamespace, request: RuntimeRedriveRequest) -> dict[str, object]:
    return request.verify(
        case.dead,
        case.binding,
        case.record,
        case.original_hash,
        case.bundle,
        operator=case.operator,
        now=case.now,
    )


def test_deterministic_byte_preserving_evidence(case: SimpleNamespace) -> None:
    expected = case.dead.verify(case.binding, case.record, case.original_hash)
    assert expected["classification"] == "invalid_event_envelope"
    assert TransportRecord.from_payload(expected["physical"]) == case.record
    assert RuntimeDeadLetter.create(case.binding, case.record, case.original_hash) == case.dead
    assert "poison" not in repr(case.dead)


@pytest.mark.parametrize("value", [None, b"", b"not json", b"[]", b"{}"])
def test_poison_matrix_and_distinct_tombstone_bytes(
    case: SimpleNamespace, value: bytes | None
) -> None:
    record = replace(case.record, value=value)
    dead = RuntimeDeadLetter.create(case.binding, record, case.original_hash)
    payload = dead.verify(case.binding, record, case.original_hash)
    assert TransportRecord.from_payload(payload["physical"]).value == value
    assert (payload["original_value_sha256"] is None) is (value is None)


def test_compatible_event_or_business_failure_is_not_schema_poison(case: SimpleNamespace) -> None:
    for event in (
        case.event,
        case.event.model_copy(update={"payload": {"bad_business_data": True}}),
    ):
        with pytest.raises(StoreError) as denied:
            RuntimeDeadLetter.create(
                case.binding,
                replace(case.record, value=event.model_dump_json().encode()),
                case.original_hash,
            )
        assert denied.value.code == "not_schema_poison"


@pytest.mark.parametrize("change", ["binding", "record", "hash", "content"])
def test_dead_letter_cannot_rebind_to_other_evidence(case: SimpleNamespace, change: str) -> None:
    binding, record, original_hash, dead = case.binding, case.record, case.original_hash, case.dead
    if change == "binding":
        binding = replace(binding, topic_id="different-topic")
    elif change == "record":
        record = replace(record, headers=(("x", b""), ("x", None)))
    elif change == "hash":
        original_hash = "c" * 64
    else:
        value = json.loads(dead.payload)
        value["classification"] = "unexpected_event_contract"
        value["dead_letter_id"] = digest({k: v for k, v in value.items() if k != "dead_letter_id"})
        dead = RuntimeDeadLetter(canonical(value))
    with pytest.raises(StoreError):
        dead.verify(binding, record, original_hash)


def test_redrive_binds_source_causation_operator_and_exact_replacement(
    case: SimpleNamespace,
) -> None:
    request = proposal(case)
    result = verify(case, request)
    assert result["causation_reference"] == json.loads(case.dead.payload)["dead_letter_id"]
    assert result["source_sha256"] == case.bundle.source_sha256
    assert result["replacement_event_id"] == str(case.event.event_id)
    assert base64.b64decode(result["replacement_base64"]) == case.event.model_dump_json().encode()
    assert result["operator_id"] == case.operator.actor_id
    assert result["claim"] == "proposal_not_delivery_or_business_approval"
    assert proposal(case) == request
    assert "replacement" not in repr(request)


@pytest.mark.parametrize("role", [ActorRole.SYSTEM, ActorRole.APPROVER, ActorRole.AUDITOR])
def test_only_scoped_planner_can_propose(case: SimpleNamespace, role: ActorRole) -> None:
    with pytest.raises(StoreError) as denied:
        proposal(case, operator=replace(case.operator, role=role))
    assert denied.value.code == "redrive_denied"


def test_no_partial_scope_or_blank_identity(case: SimpleNamespace) -> None:
    for actor in (replace(case.operator, locations=()), replace(case.operator, actor_id=" ")):
        with pytest.raises(StoreError):
            proposal(case, operator=actor)


@pytest.mark.parametrize("reason", ["", "  ", "x" * 501, None])
def test_reason_required_and_bounded(case: SimpleNamespace, reason: object) -> None:
    with pytest.raises(StoreError):
        proposal(case, reason=reason)


@pytest.mark.parametrize("reuse", ["event_id", "idempotency_key"])
def test_parseable_poison_requires_both_new_identities(case: SimpleNamespace, reuse: str) -> None:
    original = case.event.model_dump(mode="json")
    original["event_version"] = 999  # Parseable envelope, unsupported governed version.
    if reuse == "event_id":
        original["idempotency_key"] = "other-key"
        original["event_id"] = original["event_id"].upper()
    else:
        original["event_id"] = str(uuid4())
    case.record = replace(case.record, value=canonical(original).encode())
    case.dead = RuntimeDeadLetter.create(case.binding, case.record, case.original_hash)
    with pytest.raises(StoreError) as denied:
        proposal(case)
    assert denied.value.code == "redrive_identity_reused"


@pytest.mark.parametrize("change", ["source", "kind", "future", "key", "extra_payload"])
def test_replacement_must_be_material_and_source_bound(case: SimpleNamespace, change: str) -> None:
    if change == "source":
        case.event = case.event.model_copy(update={"payload": {"source_snapshot_sha256": "c" * 64}})
    elif change == "kind":
        case.event = case.event.model_copy(update={"event_type": "telemetry.observed"})
    elif change == "future":
        case.event = case.event.model_copy(update={"ingested_at": case.now + timedelta(seconds=1)})
    elif change == "key":
        case.event = case.event.model_copy(update={"source": "untrusted"})
    else:
        case.event = case.event.model_copy(
            update={"payload": {**case.event.payload, "approval": True}}
        )
    with pytest.raises(StoreError):
        proposal(case)


@pytest.mark.parametrize(
    "field,value",
    [
        ("operator_id", "planner.forged"),
        ("operator_role", "system"),
        ("locations", ["another-location"]),
        ("causation_reference", "a" * 64),
        ("source_sha256", "a" * 64),
        ("source_bundle_sha256", "a" * 64),
        ("inbox_record_hash", "a" * 64),
        ("binding_sha256", "a" * 64),
        ("replacement_event_id", "forged"),
        ("dead_letter_sha256", "a" * 64),
    ],
)
def test_self_rehashed_request_cannot_change_external_bindings(
    case: SimpleNamespace, field: str, value: object
) -> None:
    payload = json.loads(proposal(case).payload)
    payload[field] = value
    payload["request_id"] = digest({k: v for k, v in payload.items() if k != "request_id"})
    with pytest.raises(StoreError):
        verify(case, RuntimeRedriveRequest(canonical(payload)))


def test_future_naive_and_noncanonical_requests_refused(case: SimpleNamespace) -> None:
    with pytest.raises(StoreError):
        proposal(case, submitted_at=case.now.replace(tzinfo=None))
    future = proposal(case, submitted_at=case.now + timedelta(seconds=1))
    with pytest.raises(StoreError):
        verify(case, future)
    request = proposal(case)
    for invalid in (request.payload + " ", "{}", "[]", "null", "x" * (256 * 1024 + 1)):
        with pytest.raises(StoreError):
            verify(case, RuntimeRedriveRequest(invalid))


def test_external_bundle_corruption_refused(case: SimpleNamespace) -> None:
    request = proposal(case)
    case.bundle = SourceBundle("{}")
    with pytest.raises(StoreError):
        verify(case, request)


def test_deeply_nested_poison_can_be_repaired_without_parser_leak(case: SimpleNamespace) -> None:
    case.record = replace(case.record, value=b"[" * 2000 + b"0" + b"]" * 2000)
    case.dead = RuntimeDeadLetter.create(case.binding, case.record, case.original_hash)
    assert verify(case, proposal(case))["claim"] == "proposal_not_delivery_or_business_approval"
    with pytest.raises(StoreError) as denied:
        verify(case, RuntimeRedriveRequest("[" * 2000 + "0" + "]" * 2000))
    assert denied.value.code == "redrive_conflict"
