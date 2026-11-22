"""Isolated real-journal/metric replay; controlled inputs are not claimed dbt forecasts."""

from __future__ import annotations

import json
import platform
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

import pytest

from nexus.contracts import ActorRole
from nexus.modeling.r3_artifacts import write_immutable_json_manifest
from nexus.portfolio.auth import Actor
from nexus.portfolio.feedback import ActualRevision, FeedbackBook
from nexus.portfolio.learning import LearningBook, ModelArtifact
from nexus.portfolio.monitoring import MonitorBook, MonitorConfig
from nexus.portfolio.store import PortfolioStore, digest

START = datetime(2026, 1, 1, tzinfo=UTC)
TARGET = date(2026, 1, 2)
ARRIVAL = datetime(2026, 1, 3, 1, tzinfo=UTC)
CLOSED = datetime(2026, 1, 4, tzinfo=UTC)
WORKER = Actor("feedback-worker", "Trusted test worker", ActorRole.SYSTEM, ("DC-ARG-01",))
PLANNER = Actor("planner", "Planner", ActorRole.PLANNER, ("DC-ARG-01",))


def _seed(count: int = 1) -> dict[str, Any]:
    return {
        "metadata": {
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": "a" * 64,
        },
        "recommendations": [],
        "forecasts": [
            {
                "sku_id": f"SKU-{index:04d}",
                "location_id": "DC-ARG-01",
                "origin": START.isoformat(),
                "model": "seasonal_naive_empirical_intervals",
                "forecast_sha256": digest(["controlled-fixture-not-dbt", index]),
                "points": [
                    {
                        "date": TARGET.isoformat(),
                        "q025": 8,
                        "q10": 9,
                        "q50": 10,
                        "q90": 11,
                        "q975": 12,
                    }
                ],
            }
            for index in range(count)
        ],
    }


def _setup(root: Path, count: int = 1):
    clock = [START]
    store = PortfolioStore(root / "runtime", clock=lambda: clock[0].timestamp())
    seed = _seed(count)
    store.ensure_seed(seed)
    book = FeedbackBook(root / "feedback.sqlite3", clock=lambda: clock[0])
    event = book.register_current_runtime(store, actor=WORKER, expected_seed_sha256=digest(seed))
    return clock, store, book, event


def _actual(index: int = 0, **changes: Any) -> ActualRevision:
    return ActualRevision(
        **{
            "event_id": f"observed-{index}",
            "source_id": "trusted-test-feed",
            "source_snapshot_sha256": "b" * 64,
            "sku_id": f"SKU-{index:04d}",
            "location_id": "DC-ARG-01",
            "target_date": TARGET,
            "revision": 1,
            "quantity": Decimal(20),
            "available_at": ARRIVAL,
            "correction_closed": True,
            "correction_closed_at": CLOSED,
            **changes,
        }
    )


def _quality(result: dict[str, Any]) -> dict[str, Any]:
    return next(
        row for row in result["monitor"]["checks"].values() if row["kind"] == "forecast_quality"
    )


def test_current_seed_is_immutable_and_atomic_retries_do_not_duplicate(tmp_path: Path) -> None:
    clock, store, book, event = _setup(tmp_path)
    assert (
        book.register_current_runtime(
            store, actor=WORKER, expected_seed_sha256=digest(store.seed())
        )
        == event
    )
    clock[0] = CLOSED
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(
                lambda _: FeedbackBook(book.path, clock=lambda: clock[0]).append_actual(
                    _actual(), actor=WORKER
                ),
                range(8),
            )
        )
    assert all(result == results[0] for result in results)
    reopened = FeedbackBook(book.path, clock=lambda: clock[0])
    assert reopened.snapshot()["event_count"] == 2
    assert reopened.snapshot()["locations"] == ["DC-ARG-01"]
    with pytest.raises(ValueError, match="ID conflicts"):
        reopened.append_actual(_actual(quantity=Decimal(99)), actor=WORKER)
    with pytest.raises(ValueError, match="seed binding"):
        reopened.register_current_runtime(store, actor=WORKER, expected_seed_sha256="f" * 64)
    assert reopened.snapshot()["event_count"] == 2


def test_explicit_correction_chain_reopens_and_recloses_without_erasing_history(tmp_path: Path):
    clock, _, book, _ = _setup(tmp_path)
    monitor = MonitorBook(tmp_path / "monitor.sqlite3")
    clock[0] = ARRIVAL
    with pytest.raises(ValueError, match="future correction closure"):
        book.append_actual(_actual(), actor=WORKER)
    assert book.snapshot()["event_count"] == 1
    book.append_actual(_actual(correction_closed=False, correction_closed_at=None), actor=WORKER)
    before = book.evaluate(monitor)
    assert before["counts"]["eligible_forecasts"] == 0
    assert before["selection"]["exclusion_counts"] == {"correction_window_open": 1}
    clock[0] = CLOSED
    assert book.evaluate(monitor)["counts"]["eligible_forecasts"] == 0
    book.append_actual(
        _actual(
            event_id="closure-2", revision=2, supersedes_event_id="observed-0", available_at=CLOSED
        ),
        actor=WORKER,
    )
    mature = book.evaluate(monitor)
    assert mature["counts"]["eligible_forecasts"] == 1
    assert _quality(mature)["status"] == "insufficient_evidence"
    corrected = _actual(
        event_id="correction-3",
        revision=3,
        supersedes_event_id="closure-2",
        quantity=Decimal(10),
        available_at=CLOSED,
        correction_closed=False,
        correction_closed_at=None,
        source_snapshot_sha256="c" * 64,
    )
    book.append_actual(corrected, actor=WORKER)
    reopened = book.evaluate(monitor)
    assert reopened["selection"]["exclusion_counts"] == {"correction_window_open": 1}
    clock[0] += timedelta(days=1)
    final = corrected.model_copy(
        update={
            "event_id": "closure-4",
            "revision": 4,
            "supersedes_event_id": "correction-3",
            "available_at": clock[0],
            "correction_closed": True,
            "correction_closed_at": clock[0],
        }
    )
    book.append_actual(final, actor=WORKER)
    latest = book.evaluate(monitor)
    assert latest["counts"]["eligible_forecasts"] == 1
    assert latest["selection"]["selected_actuals"][0]["revision"] == 4
    assert latest["actual_source_sha256s"] == ["b" * 64, "c" * 64]
    assert latest["forecast_source_sha256s"] == ["a" * 64]
    assert book.snapshot()["counts"]["actual_revision"] == 4


def test_future_arrivals_and_late_revisions_do_not_leak_into_past_snapshots(tmp_path: Path):
    clock, _, book, _ = _setup(tmp_path)
    clock[0] = ARRIVAL
    book.append_actual(
        _actual(
            available_at=CLOSED + timedelta(days=1),
            correction_closed=False,
            correction_closed_at=None,
        ),
        actor=WORKER,
    )
    now = book.evaluate(MonitorBook(tmp_path / "present.sqlite3"))
    assert now["selection"]["exclusion_counts"] == {"actual_not_yet_available": 1}
    assert _quality(now)["distinct_mature_outcomes"] == 0
    past = book.evaluate(MonitorBook(tmp_path / "past.sqlite3"), as_of=START + timedelta(hours=1))
    assert past["counts"]["actual_versions"] == 0
    assert past["selection"]["selected_actuals"] == []
    with pytest.raises(ValueError, match="future clock"):
        book.evaluate(MonitorBook(tmp_path / "future.sqlite3"), as_of=clock[0] + timedelta(days=1))


def test_backdated_forecast_registration_is_not_rescued_by_a_later_correction(tmp_path: Path):
    clock = [CLOSED]
    store = PortfolioStore(tmp_path / "runtime", clock=lambda: clock[0].timestamp())
    store.ensure_seed(_seed())
    book = FeedbackBook(tmp_path / "feedback.sqlite3", clock=lambda: clock[0])
    book.register_current_runtime(store, actor=WORKER, expected_seed_sha256=digest(store.seed()))
    book.append_actual(_actual(), actor=WORKER)
    clock[0] += timedelta(days=1)
    book.append_actual(
        _actual(
            event_id="late-correction",
            revision=2,
            supersedes_event_id="observed-0",
            available_at=clock[0],
            correction_closed_at=clock[0],
        ),
        actor=WORKER,
    )
    result = book.evaluate(MonitorBook(tmp_path / "monitor.sqlite3"))
    assert result["selection"]["exclusion_counts"] == {"forecast_registered_after_outcome_known": 1}
    assert _quality(result)["distinct_mature_outcomes"] == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"revision": 3, "supersedes_event_id": "observed-0", "event_id": "bad"},
        {"revision": 2, "supersedes_event_id": "other", "event_id": "bad"},
        {
            "revision": 2,
            "supersedes_event_id": "observed-0",
            "event_id": "bad",
            "source_id": "other-feed",
        },
        {
            "revision": 2,
            "supersedes_event_id": "observed-0",
            "event_id": "bad",
            "available_at": ARRIVAL - timedelta(minutes=1),
        },
    ],
)
def test_conflicting_revision_lineage_fails_closed(tmp_path: Path, changes: dict[str, Any]):
    clock, _, book, _ = _setup(tmp_path)
    clock[0] = CLOSED
    book.append_actual(_actual(), actor=WORKER)
    with pytest.raises(ValueError, match="extend its source chain"):
        book.append_actual(_actual(**changes), actor=WORKER)
    assert book.snapshot()["event_count"] == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"quantity": Decimal("NaN")},
        {"quantity": Decimal("-1")},
        {"correction_closed": True, "correction_closed_at": None},
        {"correction_closed": False},
        {"available_at": START},
        {"revision": True},
        {"source_snapshot_sha256": "not-a-hash"},
        {"event_id": "  "},
    ],
)
def test_invalid_actual_contract_is_rejected(changes: dict[str, Any]):
    with pytest.raises(ValueError):
        _actual(**changes)


def test_roles_unknown_keys_minimum_evidence_and_planner_opt_in_are_enforced(tmp_path: Path):
    clock, store, book, _ = _setup(tmp_path)
    with pytest.raises(PermissionError):
        book.register_current_runtime(
            store, actor=PLANNER, expected_seed_sha256=digest(store.seed())
        )
    clock[0] = CLOSED
    with pytest.raises(PermissionError):
        book.append_actual(_actual(), actor=PLANNER)
    with pytest.raises(ValueError, match="no retained runtime"):
        book.append_actual(_actual(sku_id="UNKNOWN"), actor=WORKER)
    with pytest.raises(ValueError, match="500-outcome"):
        book.evaluate(
            MonitorBook(
                tmp_path / "bad-monitor.sqlite3", MonitorConfig(min_mature_observations=499)
            )
        )
    with pytest.raises(ValueError, match="both explicit planner"):
        book.evaluate(MonitorBook(tmp_path / "monitor.sqlite3"), proposer=PLANNER)


@pytest.fixture(scope="module")
def mature_replay(tmp_path_factory: pytest.TempPathFactory):
    started = perf_counter()
    root = tmp_path_factory.mktemp("feedback-mature-replay")
    clock, store, book, _ = _setup(root, count=500)
    # A second pre-outcome generation must not double the independent evidence.
    clock[0] += timedelta(hours=1)
    replacement = deepcopy(store.seed())
    replacement["metadata"]["new_generation_tag"] = "second controlled test generation"
    second = PortfolioStore(root / "second-runtime", clock=lambda: clock[0].timestamp())
    second.ensure_seed(replacement)
    book.register_current_runtime(second, actor=WORKER, expected_seed_sha256=digest(replacement))
    clock[0] = CLOSED  # Observe the declared Jan4 closure; never attest it prematurely.
    for index in range(500):
        book.append_actual(_actual(index), actor=WORKER)
    clock[0] = CLOSED
    return root, clock, book, perf_counter() - started


def test_500_real_joined_measurements_create_only_explicit_pre_fit_proposal(mature_replay):
    root, clock, book, ingestion_seconds = mature_replay
    monitor = MonitorBook(root / "monitor.sqlite3")
    baseline = ModelArtifact.from_bytes("baseline-fixture", b"controlled unit baseline artifact")
    learning = LearningBook(
        root / "learning.sqlite3",
        initial_artifact=baseline,
        locations=WORKER.locations,
        clock=lambda: clock[0],
    )
    measured = book.evaluate(monitor)
    assert measured["proposal_id"] is None
    assert measured["counts"] == {
        "forecast_generations": 2,
        "actual_versions": 500,
        "forecast_rows": 1000,
        "eligible_forecasts": 500,
    }
    assert measured["selection"]["exclusion_counts"] == {"superseded_by_selected_forecast": 500}
    assert _quality(measured)["distinct_mature_outcomes"] == 500
    assert _quality(measured)["status"] == "firing"
    assert Decimal(_quality(measured)["metrics"]["coverage_80"]) == 0
    assert learning.snapshot()["proposals"] == {}
    proposed = book.evaluate(monitor, learning_book=learning, proposer=PLANNER)
    replay = book.evaluate(monitor, learning_book=learning, proposer=PLANNER)
    assert proposed == replay
    assert proposed["proposal_id"] is not None
    assert proposed["training_performed"] is proposed["scientific_promotion_applied"] is False
    state = learning.snapshot()
    assert len(state["proposals"]) == 1 and state["candidates"] == {}
    assert state["current_node"] == "initial"
    assert (
        state["proposals"][proposed["proposal_id"]]["source_snapshot_sha256"]
        == proposed["source_snapshot_sha256"]
    )
    assert (
        FeedbackBook(book.path, clock=lambda: clock[0]).receipt(proposed["receipt_id"]) == proposed
    )
    write_immutable_json_manifest(
        root / "receipt",
        dataset="feedback.isolated-replay",
        manifest={
            "scope": "Controlled input replay, real SQLite and measured metrics; no dbt or fit",
            "measurement_profile": "Single-host local replay, not production load acceptance",
            "ingestion_seconds": round(ingestion_seconds, 6),
            "python_version": platform.python_version(),
            "sqlite_version": sqlite3.sqlite_version,
            "result": proposed,
        },
        schema_identity={"contract": "nexus.feedback-replay-test.v1"},
    )


def test_insufficient_outcomes_or_freshness_alone_never_propose(tmp_path: Path):
    clock, _, book, _ = _setup(tmp_path)
    clock[0] = CLOSED + timedelta(days=5)
    monitor = MonitorBook(tmp_path / "monitor.sqlite3")
    learning = LearningBook(
        tmp_path / "learning.sqlite3",
        locations=WORKER.locations,
        clock=lambda: clock[0],
        initial_artifact=ModelArtifact.from_bytes("baseline-fixture", b"controlled test artifact"),
    )
    result = book.evaluate(monitor, learning_book=learning, proposer=PLANNER)
    assert result["monitor"]["status"] == "firing"  # Old local ingestion.
    assert _quality(result)["status"] == "insufficient_evidence"
    assert result["proposal_id"] is None and not learning.snapshot()["proposals"]


def test_posthoc_model_remains_insufficient_beside_500_eligible_other_model_rows(
    mature_replay,
    tmp_path: Path,
) -> None:
    _, _, original, _ = mature_replay
    with (
        closing(sqlite3.connect(original.path)) as source,
        closing(sqlite3.connect(tmp_path / "feedback.sqlite3")) as copied,
    ):
        source.backup(copied)
    clock = [CLOSED + timedelta(hours=1)]
    book = FeedbackBook(tmp_path / "feedback.sqlite3", clock=lambda: clock[0])
    late = _seed(500)
    for series in late["forecasts"]:
        series["model"] = "posthoc-model-b"
    store = PortfolioStore(tmp_path / "late-runtime", clock=lambda: clock[0].timestamp())
    store.ensure_seed(late)
    book.register_current_runtime(store, actor=WORKER, expected_seed_sha256=digest(late))
    result = book.evaluate(MonitorBook(tmp_path / "monitor.sqlite3"))
    checks = {
        row["model_id"]: row
        for row in result["monitor"]["checks"].values()
        if row["kind"] == "forecast_quality"
    }
    assert checks["seasonal_naive_empirical_intervals"]["distinct_mature_outcomes"] == 500
    absent = checks["posthoc-model-b"]
    assert absent["status"] == "insufficient_evidence"
    assert absent["forecast_rows"] == absent["distinct_mature_outcomes"] == 0
    assert absent["metrics"] == {}
    assert result["selection"]["exclusion_counts"]["forecast_registered_after_outcome_known"] == 500
    assert result["counts"]["eligible_forecasts"] == 500


def test_journal_and_retained_receipt_tampering_is_detected(tmp_path: Path):
    clock, _, book, _ = _setup(tmp_path)
    result = book.evaluate(MonitorBook(tmp_path / "monitor.sqlite3"))
    with closing(sqlite3.connect(book.path)) as db, db:
        db.execute("UPDATE feedback_receipts SET payload='{}'")
    with pytest.raises(ValueError, match="receipt checksum"):
        book.receipt(result["receipt_id"])
    with closing(sqlite3.connect(book.path)) as db, db:
        payload = json.loads(db.execute("SELECT payload FROM feedback_events").fetchone()[0])
        payload["data"]["forecasts"][0]["q50"] = "999"
        db.execute("UPDATE feedback_events SET payload=?", (json.dumps(payload),))
    with pytest.raises(ValueError, match="journal checksum"):
        FeedbackBook(book.path, clock=lambda: clock[0]).snapshot()


def test_reader_scope_is_checked_under_journal_lock_before_monitor_writes(tmp_path: Path):
    clock, _, book, _ = _setup(tmp_path)
    wider = Actor("full-worker", "Internal worker", ActorRole.SYSTEM, ("DC-ARG-01", "DC-ARG-02"))
    replacement = _seed()
    replacement["forecasts"][0]["location_id"] = "DC-ARG-02"
    other = PortfolioStore(tmp_path / "new-location", clock=lambda: clock[0].timestamp())
    other.ensure_seed(replacement)
    book.register_current_runtime(other, actor=wider, expected_seed_sha256=digest(replacement))
    monitor = MonitorBook(tmp_path / "monitor.sqlite3")
    with pytest.raises(PermissionError, match="location scope"):
        book.evaluate(monitor, reader=PLANNER)
    with pytest.raises(PermissionError, match="human role"):
        book.evaluate(monitor, reader=wider)
    assert monitor.snapshot()["evaluation_count"] == 0
    assert book.snapshot()["receipt_ids"] == []
    auditor = Actor("global-auditor", "Auditor", ActorRole.AUDITOR, wider.locations)
    result = book.evaluate(monitor, reader=auditor)
    assert result["locations"] == ["DC-ARG-01", "DC-ARG-02"]
    assert result["proposal_id"] is None
