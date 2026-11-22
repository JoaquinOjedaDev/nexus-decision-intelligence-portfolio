"""Validation helpers for checked-in contract examples."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from pydantic import BaseModel

from nexus.contracts.models import (
    DataSnapshotRef,
    EventEnvelope,
    LifecycleEvent,
    RecommendationManifest,
)

_CONTRACTS: dict[str, tuple[str, type[BaseModel]]] = {
    "data-snapshot-ref.json": ("data-snapshot-ref.schema.json", DataSnapshotRef),
    "event-envelope.json": ("event-envelope.schema.json", EventEnvelope),
    "lifecycle-approval-granted.json": ("lifecycle-event.schema.json", LifecycleEvent),
    "recommendation-manifest.json": (
        "recommendation-manifest.schema.json",
        RecommendationManifest,
    ),
}
_R2_CONTRACTS = {
    "r2-inventory-adjustment-payload.json": ("r2-inventory-adjustment-payload.schema.json"),
    "r2-source-manifest.json": "r2-source-manifest.schema.json",
}


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path.name} must contain a JSON object")
    return value


def validate_contract_examples(examples_dir: Path) -> dict[str, int]:
    """Validate every required example with JSON Schema and its runtime model."""

    schema_dir = examples_dir.parent / "schemas"
    missing = [
        name for name in (*_CONTRACTS, *_R2_CONTRACTS) if not (examples_dir / name).is_file()
    ]
    if missing:
        raise FileNotFoundError(f"missing contract examples: {', '.join(sorted(missing))}")

    for example_name, (schema_name, model) in _CONTRACTS.items():
        example_path = examples_dir / example_name
        schema = _load_json(schema_dir / schema_name)
        instance = _load_json(example_path)
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(instance)
        # Strict Pydantic models intentionally accept datetime/tuple coercion only
        # through JSON-aware validation, not arbitrary Python object coercion.
        model.model_validate_json(example_path.read_text(encoding="utf-8"))

    for example_name, schema_name in _R2_CONTRACTS.items():
        example_path = examples_dir / example_name
        schema = _load_json(schema_dir / schema_name)
        instance = _load_json(example_path)
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(instance)

    # Imported lazily so the core contracts package remains independent of
    # the data-pipeline package during module initialization.
    from nexus.data.r2_models import R2InventoryAdjustmentPayload, R2SourceManifest

    R2InventoryAdjustmentPayload.model_validate_json(
        (examples_dir / "r2-inventory-adjustment-payload.json").read_text(encoding="utf-8")
    )
    R2SourceManifest.model_validate_json(
        (examples_dir / "r2-source-manifest.json").read_text(encoding="utf-8")
    )

    return {"validated": len(_CONTRACTS) + len(_R2_CONTRACTS)}
