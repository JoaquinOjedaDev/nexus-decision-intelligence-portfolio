"""Idempotent mock ERP adapter for the R1 vertical slice."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import cast
from uuid import UUID

from nexus.contracts import RecommendationManifest
from nexus.workflow.ledger import ManifestBindingError, WorkflowError


@dataclass(frozen=True, slots=True)
class MockErpEffect:
    """One externally observable mock purchase-order creation."""

    idempotency_key: str
    external_reference: str
    manifest_id: UUID
    manifest_fingerprint: str
    line_count: int
    total_quantity: int


class MockErpStateError(WorkflowError):
    """Raised when durable mock-ERP state is unreadable or internally inconsistent."""


def execution_idempotency_key(manifest: RecommendationManifest) -> str:
    """Return the stable key for exactly one manifest execution."""

    protected = f"nexus:mock-erp:v1:{manifest.manifest_id}:{manifest.fingerprint}"
    return hashlib.sha256(protected.encode("utf-8")).hexdigest()


class MockErpExecutor:
    """ERP simulator with idempotent create semantics.

    Supplying ``state_path`` makes effects durable across sequential process restarts.
    The R1 CLI always uses that mode. Omitting it provides a lightweight in-memory test
    double; neither mode claims multi-process concurrency.
    """

    def __init__(self, state_path: Path | None = None) -> None:
        self._state_path = state_path
        self._lock = RLock()
        self._effects = self._load_effects()

    def execute(
        self,
        manifest: RecommendationManifest,
        *,
        idempotency_key: str,
    ) -> MockErpEffect:
        if not idempotency_key:
            raise WorkflowError("execution idempotency_key cannot be empty")

        with self._lock:
            existing = self._effects.get(idempotency_key)
            if existing is not None:
                if (
                    existing.manifest_id != manifest.manifest_id
                    or existing.manifest_fingerprint != manifest.fingerprint
                ):
                    raise ManifestBindingError(
                        "idempotency key was already used for a different manifest"
                    )
                expected_total = sum(line.recommended_quantity for line in manifest.recommendations)
                if (
                    existing.line_count != len(manifest.recommendations)
                    or existing.total_quantity != expected_total
                ):
                    raise MockErpStateError(
                        "persisted mock ERP effect disagrees with its bound manifest"
                    )
                return existing

            key_digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
            effect = MockErpEffect(
                idempotency_key=idempotency_key,
                external_reference=f"MOCK-PO-{key_digest[:16].upper()}",
                manifest_id=manifest.manifest_id,
                manifest_fingerprint=manifest.fingerprint,
                line_count=len(manifest.recommendations),
                total_quantity=sum(line.recommended_quantity for line in manifest.recommendations),
            )
            updated = {**self._effects, idempotency_key: effect}
            self._persist_effects(updated)
            self._effects = updated
            return effect

    @property
    def effects(self) -> tuple[MockErpEffect, ...]:
        with self._lock:
            return tuple(self._effects.values())

    @property
    def effect_count(self) -> int:
        with self._lock:
            return len(self._effects)

    def _load_effects(self) -> dict[str, MockErpEffect]:
        if self._state_path is None or not self._state_path.exists():
            return {}
        try:
            decoded: object = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise MockErpStateError("mock ERP state is not valid UTF-8 JSON") from exc
        if not isinstance(decoded, dict):
            raise MockErpStateError("mock ERP state must be a JSON object")
        payload = cast(dict[str, object], decoded)
        if payload.get("version") != "1.0":
            raise MockErpStateError("mock ERP state has an unsupported version")
        raw_effects = payload.get("effects")
        if not isinstance(raw_effects, list):
            raise MockErpStateError("mock ERP state effects must be a list")

        effects: dict[str, MockErpEffect] = {}
        for index, raw_effect in enumerate(raw_effects, start=1):
            effect = _decode_effect(raw_effect, index=index)
            if effect.idempotency_key in effects:
                raise MockErpStateError(
                    f"duplicate idempotency key in mock ERP state at effect {index}"
                )
            effects[effect.idempotency_key] = effect
        return effects

    def _persist_effects(self, effects: dict[str, MockErpEffect]) -> None:
        if self._state_path is None:
            return
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "effects": [
                _encode_effect(effect)
                for effect in sorted(
                    effects.values(),
                    key=lambda item: item.idempotency_key,
                )
            ],
            "version": "1.0",
        }
        temporary_path: Path | None = None
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                dir=self._state_path.parent,
                prefix=f".{self._state_path.name}.",
                suffix=".tmp",
            )
            temporary_path = Path(temporary_name)
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as output:
                json.dump(
                    payload,
                    output,
                    ensure_ascii=False,
                    allow_nan=False,
                    indent=2,
                    sort_keys=True,
                )
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary_path, self._state_path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)


def _encode_effect(effect: MockErpEffect) -> dict[str, object]:
    return {
        "external_reference": effect.external_reference,
        "idempotency_key": effect.idempotency_key,
        "line_count": effect.line_count,
        "manifest_fingerprint": effect.manifest_fingerprint,
        "manifest_id": str(effect.manifest_id),
        "total_quantity": effect.total_quantity,
    }


def _required_string(payload: dict[str, object], field: str, *, index: int) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value:
        raise MockErpStateError(f"effect {index} has invalid {field}")
    return value


def _required_nonnegative_int(
    payload: dict[str, object],
    field: str,
    *,
    index: int,
) -> int:
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MockErpStateError(f"effect {index} has invalid {field}")
    return value


def _decode_effect(raw_effect: object, *, index: int) -> MockErpEffect:
    if not isinstance(raw_effect, dict):
        raise MockErpStateError(f"effect {index} must be a JSON object")
    payload = cast(dict[str, object], raw_effect)
    idempotency_key = _required_string(payload, "idempotency_key", index=index)
    external_reference = _required_string(payload, "external_reference", index=index)
    expected_reference = (
        f"MOCK-PO-{hashlib.sha256(idempotency_key.encode('utf-8')).hexdigest()[:16].upper()}"
    )
    if external_reference != expected_reference:
        raise MockErpStateError(f"effect {index} has an invalid external reference")
    fingerprint = _required_string(payload, "manifest_fingerprint", index=index)
    if len(fingerprint) != 64 or any(
        character not in "0123456789abcdef" for character in fingerprint
    ):
        raise MockErpStateError(f"effect {index} has an invalid manifest fingerprint")
    try:
        manifest_id = UUID(_required_string(payload, "manifest_id", index=index))
    except ValueError as exc:
        raise MockErpStateError(f"effect {index} has an invalid manifest_id") from exc
    return MockErpEffect(
        idempotency_key=idempotency_key,
        external_reference=external_reference,
        manifest_id=manifest_id,
        manifest_fingerprint=fingerprint,
        line_count=_required_nonnegative_int(payload, "line_count", index=index),
        total_quantity=_required_nonnegative_int(payload, "total_quantity", index=index),
    )
