from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from nexus.contracts import (
    ArtifactRef,
    DataSnapshotRef,
    EventEnvelope,
    LifecycleEvent,
    RecommendationLine,
    RecommendationManifest,
)

ROOT = Path(__file__).resolve().parents[1]
SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64


def manifest_payload() -> dict[str, object]:
    decision_time = datetime(2026, 7, 18, 12, tzinfo=UTC)
    return {
        "manifest_id": UUID("22222222-2222-4222-8222-222222222222"),
        "decision_key": "ARG-2026-W29",
        "decision_time": decision_time,
        "valid_until": decision_time + timedelta(days=1),
        "created_at": decision_time + timedelta(seconds=5),
        "created_by": "planner.alba",
        "input_snapshots": (
            DataSnapshotRef(
                dataset="gold.inventory_position",
                snapshot_id="inventory-r1",
                schema_version="1.0.0",
                as_of=decision_time - timedelta(minutes=5),
                content_sha256=SHA_A,
                row_count=100,
                partition_keys=("week",),
            ),
        ),
        "forecast_artifact": ArtifactRef(
            name="seasonal-naive", version="1.0.0", content_sha256=SHA_B
        ),
        "optimizer_artifact": ArtifactRef(
            name="base-stock-policy", version="1.0.0", content_sha256=SHA_C
        ),
        "policy_artifact": ArtifactRef(
            name="replenishment-guardrails", version="1.0.0", content_sha256=SHA_D
        ),
        "scenario_seed": 290718,
        "recommendations": (
            RecommendationLine(
                sku_id="SKU-001",
                location_id="DC-ARG-01",
                recommended_quantity=120,
                horizon_days=7,
                reason_code="below_base_stock",
            ),
        ),
        "correlation_id": "replan-2026w29",
        "trace_id": "trace-manifest-0001",
    }


def test_event_envelope_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        EventEnvelope(
            event_id=uuid4(),
            event_type="inventory.adjusted",
            source="wms.demo",
            subject_id="SKU-001:DC-01",
            occurred_at=datetime(2026, 7, 18, 12),
            ingested_at=datetime(2026, 7, 18, 12, tzinfo=UTC),
            idempotency_key="event-1",
            payload={},
        )


def test_event_envelope_rejects_ingestion_before_occurrence() -> None:
    occurred = datetime(2026, 7, 18, 12, tzinfo=UTC)
    with pytest.raises(ValidationError, match="cannot precede"):
        EventEnvelope(
            event_id=uuid4(),
            event_type="inventory.adjusted",
            source="wms.demo",
            subject_id="SKU-001:DC-01",
            occurred_at=occurred,
            ingested_at=occurred - timedelta(seconds=1),
            idempotency_key="event-1",
            payload={},
        )


@given(
    replacement_id=st.uuids(version=4),
    created_delay=st.integers(min_value=0, max_value=3_600),
    metadata_suffix=st.text(
        alphabet="abcdefghijklmnopqrstuvwxyz0123456789", min_size=1, max_size=16
    ),
)
def test_fingerprint_excludes_operational_metadata(
    replacement_id: UUID,
    created_delay: int,
    metadata_suffix: str,
) -> None:
    original = RecommendationManifest(**manifest_payload())
    changed = manifest_payload()
    changed.update(
        {
            "manifest_id": replacement_id,
            "created_at": changed["decision_time"] + timedelta(seconds=created_delay),
            "correlation_id": f"correlation-{metadata_suffix}",
            "trace_id": f"trace-{metadata_suffix}",
        }
    )
    equivalent = RecommendationManifest(**changed)

    assert equivalent.fingerprint == original.fingerprint


def test_fingerprint_includes_business_decision_content() -> None:
    original = RecommendationManifest(**manifest_payload())
    changed = manifest_payload()
    changed["recommendations"] = (
        RecommendationLine(
            sku_id="SKU-001",
            location_id="DC-ARG-01",
            recommended_quantity=121,
            horizon_days=7,
            reason_code="below_base_stock",
        ),
    )

    assert RecommendationManifest(**changed).fingerprint != original.fingerprint


def test_fingerprint_is_order_independent_for_set_like_collections() -> None:
    base = manifest_payload()
    second_snapshot = DataSnapshotRef(
        dataset="gold.open_orders",
        snapshot_id="orders-r1",
        schema_version="1.0.0",
        as_of=base["decision_time"],
        content_sha256="e" * 64,
        row_count=20,
    )
    second_line = RecommendationLine(
        sku_id="SKU-002",
        location_id="DC-ARG-01",
        recommended_quantity=55,
        horizon_days=7,
        reason_code="below_base_stock",
    )
    forward = deepcopy(base)
    forward["input_snapshots"] = (*base["input_snapshots"], second_snapshot)
    forward["recommendations"] = (*base["recommendations"], second_line)
    reverse = deepcopy(base)
    reverse["input_snapshots"] = tuple(reversed(forward["input_snapshots"]))
    reverse["recommendations"] = tuple(reversed(forward["recommendations"]))

    assert (
        RecommendationManifest(**forward).fingerprint
        == RecommendationManifest(**reverse).fingerprint
    )


def test_equivalent_timezone_offsets_hash_to_same_fingerprint() -> None:
    utc_payload = manifest_payload()
    offset_payload = manifest_payload()
    offset = timezone(timedelta(hours=-3))
    for field in ("decision_time", "valid_until", "created_at"):
        offset_payload[field] = utc_payload[field].astimezone(offset)

    assert (
        RecommendationManifest(**utc_payload).fingerprint
        == RecommendationManifest(**offset_payload).fingerprint
    )


def test_supplied_fingerprint_detects_tampering() -> None:
    payload = manifest_payload()
    payload["fingerprint"] = "f" * 64
    with pytest.raises(ValidationError, match="fingerprint does not match"):
        RecommendationManifest(**payload)


def test_manifest_rejects_snapshot_from_after_decision_time() -> None:
    payload = manifest_payload()
    decision_time = payload["decision_time"]
    assert isinstance(decision_time, datetime)
    payload["input_snapshots"] = (
        DataSnapshotRef(
            dataset="gold.inventory_position",
            snapshot_id="future-inventory",
            schema_version="1.0.0",
            as_of=decision_time + timedelta(microseconds=1),
            content_sha256=SHA_A,
            row_count=100,
        ),
    )

    with pytest.raises(ValidationError, match="later than decision_time"):
        RecommendationManifest(**payload)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_outcome_metric_rejects_non_finite_values(value: float) -> None:
    from nexus.contracts import OutcomeMetric

    with pytest.raises(ValidationError, match="finite"):
        OutcomeMetric(name="absolute_error", value=value, unit="units")


@pytest.mark.parametrize(
    ("example_name", "schema_name", "model"),
    [
        ("event-envelope.json", "event-envelope.schema.json", EventEnvelope),
        ("data-snapshot-ref.json", "data-snapshot-ref.schema.json", DataSnapshotRef),
        (
            "recommendation-manifest.json",
            "recommendation-manifest.schema.json",
            RecommendationManifest,
        ),
        ("lifecycle-approval-granted.json", "lifecycle-event.schema.json", LifecycleEvent),
    ],
)
def test_checked_in_examples_match_json_schema_and_runtime_contract(
    example_name: str,
    schema_name: str,
    model: type[EventEnvelope | DataSnapshotRef | RecommendationManifest | LifecycleEvent],
) -> None:
    example_text = (ROOT / "contracts" / "examples" / example_name).read_text(encoding="utf-8")
    schema = json.loads((ROOT / "contracts" / "schemas" / schema_name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(json.loads(example_text))
    runtime_schema = model.model_json_schema()
    assert set(schema["properties"]) == set(runtime_schema["properties"])
    assert set(schema["required"]) == set(runtime_schema["required"])
    instance = model.model_validate_json(example_text)
    if isinstance(instance, RecommendationManifest):
        assert instance.fingerprint == json.loads(example_text)["fingerprint"]
