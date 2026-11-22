"""Portable observation contracts; these do not claim persisted action enforcement."""

from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from uuid import uuid4

import pytest

from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.source_freshness import (
    SourceObservation,
    SourceWatchPolicy,
    freshness_reason,
    probe_source,
)
from nexus.simulation import generate_r2_smoke_fixture


@pytest.fixture
def policy(tmp_path: Path) -> SourceWatchPolicy:
    return SourceWatchPolicy("inventory", str((tmp_path / "source").resolve()), 30)


def observation(policy: SourceWatchPolicy) -> SourceObservation:
    return SourceObservation(str(uuid4()), policy.sha256, 5, 100.0, 102.0, "healthy", "a" * 64)


def reason(policy: SourceWatchPolicy, observed: SourceObservation | None, now: float = 110) -> str:
    return freshness_reason(
        policy, observed, source_sha256="a" * 64, publication_sequence=5, now=now
    )


def test_exact_age_boundary_and_slow_read(policy: SourceWatchPolicy) -> None:
    observed = observation(policy)
    assert reason(policy, observed, 129.999) == "fresh"
    assert reason(policy, observed, 130) == "source_observation_expired"
    slow = replace(observed, completed_at=140)
    assert reason(policy, slow, 140) == "source_observation_expired"


def test_missing_regressed_and_reconfigured_fail_closed(policy: SourceWatchPolicy) -> None:
    assert reason(policy, None) == "source_observation_missing"
    observed = observation(policy)
    assert reason(policy, observed, 101) == "source_clock_regressed"
    assert reason(replace(policy, max_age_seconds=60), observed) == "source_policy_mismatch"
    assert reason(replace(policy, source_id="other"), observed) == "source_policy_mismatch"


@pytest.mark.parametrize("sequence,source", [(4, "a" * 64), (6, "a" * 64), (5, "b" * 64)])
def test_same_source_different_sequence_still_supersedes(
    policy: SourceWatchPolicy, sequence: int, source: str
) -> None:
    observed = replace(observation(policy), publication_sequence=sequence, source_sha256=source)
    assert reason(policy, observed) == "source_observation_superseded"


@pytest.mark.parametrize("outcome", ["changed", "read_failed"])
def test_failed_receipt_never_inherits_previous_success(
    policy: SourceWatchPolicy, outcome: str
) -> None:
    payload = observation(policy).payload()
    payload.update(outcome=outcome, source_sha256=None if outcome == "read_failed" else "a" * 64)
    assert reason(policy, SourceObservation(**payload)) == "source_" + outcome


@pytest.mark.parametrize(
    "changes",
    [
        {"publication_sequence": True},
        {"publication_sequence": -1},
        {"started_at": float("nan")},
        {"started_at": float("inf")},
        {"started_at": True},
        {"completed_at": 99},
        {"completed_at": -1},
        {"outcome": "unknown"},
        {"source_sha256": None},
        {"source_sha256": "A" * 64},
        {"policy_sha256": "invalid"},
        {"observation_id": "not-uuid"},
        {"observation_id": True},
        {"contract": "v2"},
        {"outcome": "read_failed"},
        {"started_at": "100"},
    ],
)
def test_invalid_observation_is_not_a_health_receipt(
    policy: SourceWatchPolicy, changes: dict[str, object]
) -> None:
    with pytest.raises(ValueError):
        SourceObservation(**{**observation(policy).payload(), **changes})


@pytest.mark.parametrize("age", [0, -1, 3601, 30.0, True, "30"])
def test_policy_age_is_explicit_bounded_integer(tmp_path: Path, age: object) -> None:
    with pytest.raises(ValueError):
        SourceWatchPolicy("inventory", str(tmp_path.resolve()), age)  # type: ignore[arg-type]


@pytest.mark.parametrize("name", ["", "Upper", "source/name", "a" * 65])
def test_source_identity_is_bounded(tmp_path: Path, name: str) -> None:
    with pytest.raises(ValueError):
        SourceWatchPolicy(name, str(tmp_path.resolve()), 30)


def test_contracts_round_trip_and_are_immutable(policy: SourceWatchPolicy) -> None:
    observed = observation(policy)
    assert SourceWatchPolicy(**policy.payload()) == policy
    assert SourceObservation(**observed.payload()) == observed
    with pytest.raises(FrozenInstanceError):
        policy.max_age_seconds = 3600  # type: ignore[misc]


@pytest.mark.parametrize("now", [float("nan"), float("inf"), -1, True, "110"])
def test_invalid_clock_never_looks_fresh(policy: SourceWatchPolicy, now: object) -> None:
    with pytest.raises(ValueError):
        reason(policy, observation(policy), now)  # type: ignore[arg-type]


def test_actual_probe_matches_exact_bytes_and_retains_duration(policy: SourceWatchPolicy) -> None:
    root = Path(policy.source_root)
    generate_r2_smoke_fixture(root)
    source = SourceBundle.capture(root).source_sha256
    ticks = iter([100.0, 105.0])
    observed, retained = probe_source(
        policy, source_sha256=source, publication_sequence=7, clock=lambda: next(ticks)
    )
    assert retained is not None and retained.source_sha256 == source
    assert observed.outcome == "healthy"
    assert (observed.started_at, observed.completed_at) == (100, 105)
    assert (
        freshness_reason(policy, observed, source_sha256=source, publication_sequence=7, now=110)
        == "fresh"
    )
    changed, _ = probe_source(policy, source_sha256="a" * 64, publication_sequence=7)
    assert changed.outcome == "changed"


def test_missing_source_returns_sanitized_failure(policy: SourceWatchPolicy) -> None:
    failed, bundle = probe_source(policy, source_sha256="a" * 64, publication_sequence=5)
    assert bundle is None and failed.source_sha256 is None and failed.outcome == "read_failed"
    assert policy.source_root not in str(failed.payload())
    assert "No such" not in str(failed.payload())


def test_changed_bytes_without_manifest_update_cannot_be_healthy(policy: SourceWatchPolicy) -> None:
    root = Path(policy.source_root)
    generate_r2_smoke_fixture(root)
    original = SourceBundle.capture(root)
    name = next(name for name in original.files() if name != "source-manifest.json")
    (root / name).write_bytes(b"invalid newly changed source")
    failed, bundle = probe_source(
        policy, source_sha256=original.source_sha256, publication_sequence=5
    )
    assert failed.outcome == "read_failed" and bundle is None


def test_probe_clock_regression_produces_no_success_receipt(policy: SourceWatchPolicy) -> None:
    generate_r2_smoke_fixture(Path(policy.source_root))
    ticks = iter([100.0, 99.0])
    with pytest.raises(ValueError):
        probe_source(
            policy, source_sha256="a" * 64, publication_sequence=5, clock=lambda: next(ticks)
        )
