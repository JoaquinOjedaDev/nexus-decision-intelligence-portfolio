"""Strict source-observation contracts for the upcoming persisted freshness fence.

These are bounded local-file observations, not proof of upstream event-time freshness.
The runtime must retain the policy and observation under its workflow guard before
using this decision. This module neither grants authorization nor changes a database.
"""

from __future__ import annotations

import math
import re
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

from nexus.portfolio.source_bundle import SourceBundle, _safe_directory
from nexus.portfolio.store import StoreError, digest


def _sha(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def _instant(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


@dataclass(frozen=True)
class SourceWatchPolicy:
    """Explicit source identity and bounded observation lifetime; no ambient discovery."""

    source_id: str
    source_root: str
    max_age_seconds: int
    contract: Literal["nexus.source-watch-policy.v1"] = "nexus.source-watch-policy.v1"

    def __post_init__(self) -> None:
        if (
            self.contract != "nexus.source-watch-policy.v1"
            or not isinstance(self.source_id, str)
            or re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", self.source_id) is None
            or not isinstance(self.source_root, str)
            or not self.source_root
            or type(self.max_age_seconds) is not int
            or not 1 <= self.max_age_seconds <= 3600
        ):
            raise ValueError("Invalid source observation policy")
        # Rechecked on every probe so a later linked directory is not trusted.
        root = _safe_directory(Path(self.source_root))
        if str(root) != self.source_root:
            raise ValueError("Source policy requires a canonical absolute local directory")

    def payload(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def sha256(self) -> str:
        return digest(self.payload())


@dataclass(frozen=True)
class SourceObservation:
    observation_id: str
    policy_sha256: str
    publication_sequence: int
    started_at: float
    completed_at: float
    outcome: Literal["healthy", "changed", "read_failed"]
    source_sha256: str | None
    contract: Literal["nexus.source-observation.v1"] = "nexus.source-observation.v1"

    def __post_init__(self) -> None:
        try:
            identity = str(UUID(self.observation_id))
        except (ValueError, TypeError, AttributeError):
            raise ValueError("Invalid source observation identity") from None
        if (
            self.contract != "nexus.source-observation.v1"
            or identity != self.observation_id
            or not _sha(self.policy_sha256)
            or type(self.publication_sequence) is not int
            or self.publication_sequence < 0
            or not _instant(self.started_at)
            or not _instant(self.completed_at)
            or self.completed_at < self.started_at
            or self.outcome not in {"healthy", "changed", "read_failed"}
            or (
                self.source_sha256 is not None
                if self.outcome == "read_failed"
                else not _sha(self.source_sha256)
            )
        ):
            raise ValueError("Invalid source observation contract")

    def payload(self) -> dict[str, Any]:
        return asdict(self)


def freshness_reason(
    policy: SourceWatchPolicy,
    observation: SourceObservation | None,
    *,
    source_sha256: str,
    publication_sequence: int,
    now: float,
) -> str:
    """Return fresh only for a recent successful observation of the exact current state.

    Evaluate *after* obtaining the action transaction's workflow guard. A successful
    return is not authorization, evidence of a completed replan, or a broker-health
    assertion. Exact expiration is stale; a slow read consumes its freshness budget.
    """
    if (
        not _instant(now)
        or not _sha(source_sha256)
        or type(publication_sequence) is not int
        or publication_sequence < 0
    ):
        raise ValueError("Invalid freshness evaluation context")
    if observation is None:
        return "source_observation_missing"
    if observation.policy_sha256 != policy.sha256:
        return "source_policy_mismatch"
    if now < observation.completed_at:
        return "source_clock_regressed"
    if observation.outcome != "healthy":
        return f"source_{observation.outcome}"
    if (
        observation.publication_sequence != publication_sequence
        or observation.source_sha256 != source_sha256
    ):
        return "source_observation_superseded"
    if now - observation.started_at >= policy.max_age_seconds:
        return "source_observation_expired"
    return "fresh"


def probe_source(
    policy: SourceWatchPolicy,
    *,
    source_sha256: str,
    publication_sequence: int,
    clock: Callable[[], float] = time.time,
) -> tuple[SourceObservation, SourceBundle | None]:
    """Read exact bytes from the pinned directory; return a receipt, never publish it.

    Caller captures the current publication identity before probing, then atomically
    rechecks it when persisting this result. A concurrent source publication must
    supersede the receipt. Only bounded failure codes leave this function, not raw
    file contents, filesystem paths from exceptions, or connection credentials.
    """
    started = clock()
    freshness_reason(
        policy,
        None,
        source_sha256=source_sha256,
        publication_sequence=publication_sequence,
        now=started,
    )
    bundle: SourceBundle | None = None
    actual: str | None = None
    outcome: Literal["healthy", "changed", "read_failed"] = "read_failed"
    try:
        bundle = SourceBundle.capture(Path(policy.source_root))
        actual = bundle.source_sha256
        outcome = "healthy" if actual == source_sha256 else "changed"
    except (OSError, StoreError):
        bundle = None
    observation = SourceObservation(
        observation_id=str(uuid4()),
        policy_sha256=policy.sha256,
        publication_sequence=publication_sequence,
        started_at=started,
        completed_at=clock(),
        outcome=outcome,
        source_sha256=actual,
    )
    return observation, bundle
