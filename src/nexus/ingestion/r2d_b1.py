"""Portable schema governance, dead-letter evidence, and manual redrive for R2D-B1."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from uuid import UUID

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError

from nexus.ingestion.r2d_b1_contracts import (
    PROJECT_ROOT,
    R2DB1CompatibilityAssessment,
    R2DB1Config,
    R2DB1DeadLetterRecord,
    R2DB1RedriveRequest,
    R2DB1SchemaDecision,
    R2DB1SchemaReason,
    r2d_b1_dead_letter_id,
    r2d_b1_redrive_id,
)
from nexus.ingestion.r2d_capture import (
    classify_transport_record,
    parsed_lineage_sha256,
    physical_record_fingerprint,
)
from nexus.ingestion.r2d_contracts import R2DCapturedRecord, R2DConfig
from nexus.modeling.r3_artifacts import canonical_json_sha256


class R2DB1GovernanceError(ValueError):
    """Fail-closed error raised when governed evidence cannot be trusted."""


def _load_json_object(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2DB1GovernanceError(f"cannot load governed JSON schema: {path}") from exc
    if not isinstance(payload, dict):
        raise R2DB1GovernanceError(f"governed JSON schema is not an object: {path}")
    return payload


def _schema_paths(config: R2DB1Config, project_root: Path) -> tuple[Path, Path]:
    return (
        project_root / config.schema_policy.envelope_schema_path,
        project_root / config.schema_policy.payload_schema_path,
    )


def _decision(
    *,
    disposition: str,
    reason: R2DB1SchemaReason,
    raw_value_sha256: str,
    config: R2DB1Config,
    parsed: Mapping[str, object] | None,
) -> R2DB1SchemaDecision:
    event_id: UUID | None = None
    raw_event_id = None if parsed is None else parsed.get("event_id")
    if isinstance(raw_event_id, str):
        try:
            event_id = UUID(raw_event_id)
        except ValueError:
            event_id = None
    contract_version = None if parsed is None else parsed.get("contract_version", "1.0")
    event_type = None if parsed is None else parsed.get("event_type")
    event_version = None if parsed is None else parsed.get("event_version", 1)
    idempotency_key = None if parsed is None else parsed.get("idempotency_key")
    payload: dict[str, object] = {
        "contract_version": "r2d.b1.schema-decision.v1",
        "disposition": disposition,
        "reason": reason,
        "raw_value_sha256": raw_value_sha256,
        "schema_bundle_sha256": config.schema_policy.schema_bundle_sha256,
        "envelope_contract_version": (
            contract_version if isinstance(contract_version, str) else None
        ),
        "event_type": event_type if isinstance(event_type, str) else None,
        "event_version": (
            event_version
            if isinstance(event_version, int) and not isinstance(event_version, bool)
            else None
        ),
        "event_id": None if event_id is None else str(event_id),
        "idempotency_key": idempotency_key if isinstance(idempotency_key, str) else None,
    }
    model_payload = {**payload, "event_id": event_id}
    return R2DB1SchemaDecision.model_validate(
        {**model_payload, "content_sha256": canonical_json_sha256(payload)}
    )


def classify_schema_value(
    raw_value: bytes,
    config: R2DB1Config,
    *,
    project_root: Path = PROJECT_ROOT,
) -> R2DB1SchemaDecision:
    """Apply only schema/version policy; never duplicate R2A business rules."""

    raw_sha256 = hashlib.sha256(raw_value).hexdigest()
    try:
        decoded = raw_value.decode("utf-8")
    except UnicodeDecodeError:
        return _decision(
            disposition="dead_letter",
            reason="invalid_utf8",
            raw_value_sha256=raw_sha256,
            config=config,
            parsed=None,
        )
    try:
        parsed_value = json.loads(decoded)
    except json.JSONDecodeError:
        return _decision(
            disposition="dead_letter",
            reason="invalid_json",
            raw_value_sha256=raw_sha256,
            config=config,
            parsed=None,
        )
    if not isinstance(parsed_value, dict):
        return _decision(
            disposition="dead_letter",
            reason="envelope_schema_invalid",
            raw_value_sha256=raw_sha256,
            config=config,
            parsed=None,
        )
    parsed: dict[str, object] = parsed_value
    contract_version = parsed.get("contract_version", "1.0")
    if (
        not isinstance(contract_version, str)
        or contract_version not in config.schema_policy.supported_contract_versions
    ):
        return _decision(
            disposition="dead_letter",
            reason="unsupported_contract_version",
            raw_value_sha256=raw_sha256,
            config=config,
            parsed=parsed,
        )
    event_type = parsed.get("event_type")
    if isinstance(event_type, str) and event_type != config.schema_policy.event_type:
        return _decision(
            disposition="dead_letter",
            reason="unsupported_event_type",
            raw_value_sha256=raw_sha256,
            config=config,
            parsed=parsed,
        )
    event_version = parsed.get("event_version", 1)
    if (
        isinstance(event_version, int)
        and not isinstance(event_version, bool)
        and event_version not in config.schema_policy.supported_event_versions
    ):
        return _decision(
            disposition="dead_letter",
            reason="unsupported_event_version",
            raw_value_sha256=raw_sha256,
            config=config,
            parsed=parsed,
        )

    envelope_path, payload_path = _schema_paths(config, project_root)
    envelope_schema = _load_json_object(envelope_path)
    payload_schema = _load_json_object(payload_path)
    envelope_validator = Draft202012Validator(envelope_schema, format_checker=FormatChecker())
    if next(envelope_validator.iter_errors(parsed), None) is not None:
        return _decision(
            disposition="dead_letter",
            reason="envelope_schema_invalid",
            raw_value_sha256=raw_sha256,
            config=config,
            parsed=parsed,
        )
    payload = parsed.get("payload")
    payload_validator = Draft202012Validator(payload_schema, format_checker=FormatChecker())
    if next(payload_validator.iter_errors(payload), None) is not None:
        return _decision(
            disposition="dead_letter",
            reason="payload_schema_invalid",
            raw_value_sha256=raw_sha256,
            config=config,
            parsed=parsed,
        )
    return _decision(
        disposition="forward",
        reason="compatible",
        raw_value_sha256=raw_sha256,
        config=config,
        parsed=parsed,
    )


def assess_backward_compatibility(
    baseline_schema: Mapping[str, object],
    candidate_schema: Mapping[str, object],
) -> R2DB1CompatibilityAssessment:
    """Conservatively accept optional additions while rejecting semantic mutation.

    This is intentionally narrower than a general JSON-Schema implication solver.
    Unsupported root keywords fail closed rather than being guessed compatible.
    """

    baseline = dict(baseline_schema)
    candidate = dict(candidate_schema)
    violations: set[str] = set()
    try:
        Draft202012Validator.check_schema(candidate)
    except SchemaError:
        violations.add("candidate_schema_invalid")

    allowed_root_keys = {
        "$schema",
        "$id",
        "title",
        "type",
        "additionalProperties",
        "required",
        "properties",
        "$defs",
    }
    for key in candidate.keys() - allowed_root_keys:
        violations.add(f"unsupported_root_keyword:{key}")
    if baseline.get("$schema") != candidate.get("$schema"):
        violations.add("dialect_changed")
    if baseline.get("type") != "object" or candidate.get("type") != "object":
        violations.add("object_type_required")
    if (
        baseline.get("additionalProperties") is False
        and candidate.get("additionalProperties") is not False
    ):
        violations.add("additional_properties_policy_changed")

    baseline_required = set(_string_sequence(baseline.get("required")))
    candidate_required = set(_string_sequence(candidate.get("required")))
    for name in candidate_required - baseline_required:
        violations.add(f"new_required_property:{name}")

    baseline_properties = _mapping(baseline.get("properties"))
    candidate_properties = _mapping(candidate.get("properties"))
    for name, definition in baseline_properties.items():
        if name not in candidate_properties:
            violations.add(f"property_removed:{name}")
        elif canonical_json_sha256(definition) != canonical_json_sha256(candidate_properties[name]):
            violations.add(f"property_changed:{name}")

    baseline_defs = _mapping(baseline.get("$defs"))
    candidate_defs = _mapping(candidate.get("$defs"))
    for name, definition in baseline_defs.items():
        if name not in candidate_defs:
            violations.add(f"definition_removed:{name}")
        elif canonical_json_sha256(definition) != canonical_json_sha256(candidate_defs[name]):
            violations.add(f"definition_changed:{name}")

    ordered = tuple(sorted(violations))
    payload: dict[str, object] = {
        "contract_version": "r2d.b1.compatibility-assessment.v1",
        "algorithm": "conservative_object_backward_v1",
        "baseline_schema_sha256": canonical_json_sha256(baseline),
        "candidate_schema_sha256": canonical_json_sha256(candidate),
        "compatible": not ordered,
        "violations": ordered,
    }
    return R2DB1CompatibilityAssessment.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def _string_sequence(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items()}


def build_dead_letter_record(
    captured: R2DCapturedRecord,
    decision: R2DB1SchemaDecision,
    config: R2DB1Config,
    data_config: R2DConfig,
) -> R2DB1DeadLetterRecord:
    """Create immutable DLQ evidence only from a revalidated admitted record."""

    record = captured.record
    raw_value = record.value_bytes
    expected_transport = classify_transport_record(record, data_config)
    if captured.decision != expected_transport or expected_transport.disposition != "admitted":
        raise R2DB1GovernanceError("dead-letter input is not a transport-admitted record")
    if raw_value is None:
        raise R2DB1GovernanceError("dead-letter evidence requires original raw bytes")
    if captured.raw_value_sha256 != hashlib.sha256(raw_value).hexdigest():
        raise R2DB1GovernanceError("captured raw-value identity changed before dead-lettering")
    if captured.physical_fingerprint != physical_record_fingerprint(record, captured.decision):
        raise R2DB1GovernanceError("captured physical fingerprint changed before dead-lettering")
    if captured.lineage_sha256 != parsed_lineage_sha256(record.lineage):
        raise R2DB1GovernanceError("captured lineage identity changed before dead-lettering")
    expected_schema = classify_schema_value(raw_value, config)
    if decision != expected_schema or decision.disposition != "dead_letter":
        raise R2DB1GovernanceError("dead-letter decision differs from current schema policy")

    dead_letter_id = r2d_b1_dead_letter_id(
        physical_fingerprint=captured.physical_fingerprint,
        decision_sha256=decision.content_sha256,
    )
    payload: dict[str, object] = {
        "contract_version": "r2d.b1.dead-letter.v1",
        "dead_letter_id": dead_letter_id,
        "original_topic": record.topic,
        "original_partition": record.partition,
        "original_offset": record.offset,
        "original_physical_fingerprint": captured.physical_fingerprint,
        "original_key_hex": record.key_hex,
        "original_headers": [header.model_dump(mode="json") for header in record.headers],
        "original_raw_value_hex": raw_value.hex(),
        "original_raw_value_sha256": captured.raw_value_sha256,
        "schema_decision": decision.model_dump(mode="json"),
    }
    model_payload = {**payload, "schema_decision": decision}
    return R2DB1DeadLetterRecord.model_validate(
        {**model_payload, "content_sha256": canonical_json_sha256(payload)}
    )


def create_redrive_request(
    dead_letter: R2DB1DeadLetterRecord,
    replacement_raw_value: bytes,
    *,
    operator_id: str,
    reason: str,
    submitted_at: datetime,
    config: R2DB1Config,
) -> R2DB1RedriveRequest:
    """Create a new governed event request without altering the rejected evidence."""

    dead_letter = R2DB1DeadLetterRecord.model_validate_json(dead_letter.model_dump_json())
    replacement_decision = classify_schema_value(replacement_raw_value, config)
    if replacement_decision.disposition != "forward":
        raise R2DB1GovernanceError("redrive replacement is not schema-compatible")
    try:
        replacement = json.loads(replacement_raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2DB1GovernanceError("redrive replacement is not valid JSON") from exc
    if not isinstance(replacement, dict):
        raise R2DB1GovernanceError("redrive replacement must be an event object")
    try:
        replacement_event_id = UUID(str(replacement["event_id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise R2DB1GovernanceError("redrive replacement requires a governed event_id") from exc
    replacement_idempotency_key = replacement.get("idempotency_key")
    if not isinstance(replacement_idempotency_key, str):
        raise R2DB1GovernanceError("redrive replacement requires an idempotency_key")

    try:
        original = json.loads(bytes.fromhex(dead_letter.original_raw_value_hex).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        original = None
    if isinstance(original, dict):
        if original.get("event_id") == str(replacement_event_id):
            raise R2DB1GovernanceError("redrive must use a new governed event identity")
        if original.get("idempotency_key") == replacement_idempotency_key:
            raise R2DB1GovernanceError("redrive must use a new idempotency key")

    identity_payload: dict[str, object] = {
        "contract_version": "r2d.b1.redrive-request.v1",
        "dead_letter_id": dead_letter.dead_letter_id,
        "dead_letter_content_sha256": dead_letter.content_sha256,
        "original_raw_value_sha256": dead_letter.original_raw_value_sha256,
        "causation_reference": dead_letter.dead_letter_id,
        "operator_id": operator_id,
        "reason": reason,
        "submitted_at": submitted_at,
        "replacement_raw_value_hex": replacement_raw_value.hex(),
        "replacement_raw_value_sha256": hashlib.sha256(replacement_raw_value).hexdigest(),
        "replacement_event_id": str(replacement_event_id),
        "replacement_idempotency_key": replacement_idempotency_key,
        "replacement_schema_decision_sha256": replacement_decision.content_sha256,
    }
    redrive_id = r2d_b1_redrive_id(identity_payload)
    content_payload = {**identity_payload, "redrive_id": redrive_id}
    model_payload = {
        **content_payload,
        "replacement_event_id": replacement_event_id,
    }
    return R2DB1RedriveRequest.model_validate(
        {**model_payload, "content_sha256": canonical_json_sha256(content_payload)}
    )


def validate_redrive_ledger(
    requests: Sequence[R2DB1RedriveRequest],
    dead_letters: Sequence[R2DB1DeadLetterRecord],
    config: R2DB1Config,
) -> tuple[R2DB1RedriveRequest, ...]:
    """Revalidate an append-only ledger and reject identity reuse or detached evidence."""

    dead_letter_by_id: dict[str, R2DB1DeadLetterRecord] = {}
    for dead_letter_item in dead_letters:
        validated = R2DB1DeadLetterRecord.model_validate_json(dead_letter_item.model_dump_json())
        if validated.dead_letter_id in dead_letter_by_id:
            raise R2DB1GovernanceError("dead-letter ledger contains a duplicate identity")
        dead_letter_by_id[validated.dead_letter_id] = validated

    seen_redrive_ids: set[str] = set()
    seen_event_ids: set[UUID] = set()
    seen_idempotency_keys: set[str] = set()
    validated_requests: list[R2DB1RedriveRequest] = []
    for request_item in requests:
        request = R2DB1RedriveRequest.model_validate_json(request_item.model_dump_json())
        dead_letter = dead_letter_by_id.get(request.dead_letter_id)
        if dead_letter is None:
            raise R2DB1GovernanceError("redrive request references an unknown dead-letter record")
        if (
            request.dead_letter_content_sha256 != dead_letter.content_sha256
            or request.original_raw_value_sha256 != dead_letter.original_raw_value_sha256
        ):
            raise R2DB1GovernanceError("redrive request is detached from original evidence")
        replacement = bytes.fromhex(request.replacement_raw_value_hex)
        decision = classify_schema_value(replacement, config)
        if (
            decision.disposition != "forward"
            or decision.content_sha256 != request.replacement_schema_decision_sha256
        ):
            raise R2DB1GovernanceError("redrive replacement no longer matches schema policy")
        if request.redrive_id in seen_redrive_ids:
            raise R2DB1GovernanceError("redrive ledger contains a duplicate request identity")
        if request.replacement_event_id in seen_event_ids:
            raise R2DB1GovernanceError("redrive ledger reuses a governed event identity")
        if request.replacement_idempotency_key in seen_idempotency_keys:
            raise R2DB1GovernanceError("redrive ledger reuses an idempotency key")
        seen_redrive_ids.add(request.redrive_id)
        seen_event_ids.add(request.replacement_event_id)
        seen_idempotency_keys.add(request.replacement_idempotency_key)
        validated_requests.append(request)
    return tuple(validated_requests)
