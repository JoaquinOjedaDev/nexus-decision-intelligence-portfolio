"""Identity-bound capture measurements, not durable action authorization.

Freshness starts before I/O, not when the answer is received. An observation is
only a bounded snapshot; records may arrive immediately afterward. Publication
and transactional delivery fences remain necessary even for a fresh sample.
"""

from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from typing import Any
from uuid import UUID

from nexus.portfolio.store import digest
from nexus.portfolio.transport_ledger import TransportBinding


def valid_instant(value: object) -> bool:
    try:
        return (
            isinstance(value, (float, int))
            and not isinstance(value, bool)
            and math.isfinite(value)
            and value >= 0
        )
    except OverflowError:
        return False


@dataclass(frozen=True)
class CaptureObservation:
    observation_id: str
    consumer_instance_id: str
    binding_sha256: str
    started_at: float
    completed_at: float
    low: tuple[int, ...]
    high: tuple[int, ...]
    captured: tuple[int, ...]
    committed: tuple[int, ...]
    contract: str = "nexus.capture-observation.v1"

    def __post_init__(self) -> None:
        try:
            if any(str(UUID(v)) != v for v in (self.observation_id, self.consumer_instance_id)):
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise ValueError("Canonical observation and consumer identities required") from None
        vectors = (self.low, self.high, self.captured, self.committed)
        if (
            self.contract != "nexus.capture-observation.v1"
            or not isinstance(self.binding_sha256, str)
            or re.fullmatch(r"[a-f0-9]{64}", self.binding_sha256) is None
            or not valid_instant(self.started_at)
            or not valid_instant(self.completed_at)
            or self.completed_at < self.started_at
            or any(type(v) is not tuple for v in vectors)
            or not 1 <= len(self.low) <= 32
            or any(len(v) != len(self.low) for v in vectors)
            or any(type(n) is not int or not 0 <= n <= 2**63 - 1 for v in vectors for n in v)
            or any(
                not low <= committed <= captured <= high
                for low, high, captured, committed in zip(*vectors, strict=True)
            )
        ):
            raise ValueError("Invalid capture observation")

    @property
    def lag(self) -> tuple[int, ...]:
        return tuple(h - c for h, c in zip(self.high, self.captured, strict=True))

    def payload(self) -> dict[str, Any]:
        value = asdict(self)
        for key in ("low", "high", "captured", "committed"):
            value[key] = list(value[key])
        return value

    @property
    def sha256(self) -> str:
        return digest(self.payload())


def capture_health_reason(
    binding: TransportBinding,
    observation: CaptureObservation | None,
    *,
    now: float,
    max_age_seconds: int,
) -> str:
    """Zero-lag policy evaluation only; a durable transaction must enforce it later."""
    if not valid_instant(now) or type(max_age_seconds) is not int or not 1 <= max_age_seconds <= 60:
        raise ValueError("Finite clock and bounded explicit capture-health lifetime required")
    if observation is None:
        return "capture_observation_missing"
    if not isinstance(observation, CaptureObservation):
        raise ValueError("Typed capture observation required")
    if (
        observation.binding_sha256 != digest(binding.payload())
        or len(observation.low) != binding.partition_count
    ):
        return "capture_binding_changed"
    if now < observation.completed_at:
        return "capture_clock_regressed"
    if now - observation.started_at >= max_age_seconds:
        return "capture_observation_expired"
    if observation.committed != observation.captured:
        return "capture_ack_pending"
    if any(observation.lag):
        return "capture_lagging"
    return "caught_up_at_observation"
