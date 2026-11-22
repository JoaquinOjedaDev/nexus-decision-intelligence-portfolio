"""Portable shared-view contract checks; these do not claim a live PostgreSQL test."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from nexus.contracts import ActorRole
from nexus.portfolio import learning_views, runtime_feedback, runtime_monitoring
from nexus.portfolio.auth import Actor
from nexus.portfolio.feedback import FeedbackBook
from nexus.portfolio.learning_attachments import LearningAttachments
from nexus.portfolio.service_backend import OperationalStore, RuntimeView
from nexus.portfolio.store import PortfolioStore, StoreError, digest
from nexus.portfolio.training_pipeline import LearningRun

LOCATIONS = ("CURRENT", "FORECAST", "ARCHIVED")
HUMAN = Actor("audit.reader", "Auditor", ActorRole.AUDITOR, LOCATIONS)
REGISTRAR = Actor("learning.registrar", "Trusted registrar", ActorRole.SYSTEM, LOCATIONS)
NOW = datetime(2026, 1, 1, tzinfo=UTC)


def seed(source: str = "a") -> dict[str, Any]:
    return {
        "metadata": {
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": source * 64,
            "source_watermark": NOW.isoformat(),
        },
        "recommendations": [{"id": "rec", "location_id": "CURRENT"}],
        "forecasts": [
            {
                "sku_id": "SKU",
                "location_id": "FORECAST",
                "origin": NOW.isoformat(),
                "model": "controlled_fixture",
                "forecast_sha256": "f" * 64,
                "points": [
                    {"date": "2026-01-02", "q025": 1, "q10": 2, "q50": 3, "q90": 4, "q975": 5}
                ],
            }
        ],
    }


def evaluated(source: str = "a") -> LearningRun:
    return LearningRun(
        {
            "contract_version": "nexus.runtime-learning-result.v1",
            "bindings": {
                "source_snapshot_sha256": source * 64,
                "snapshot_sha256": "b" * 64,
                "plan_sha256": "c" * 64,
                "code_sha256": "d" * 64,
                "finalization_sha256": None,
            },
            "status": "ABSTAIN_INSUFFICIENT_EVIDENCE",
            "training_performed": False,
            "classification": "synthetic",
            "reason": "split_support_not_met",
            "snapshot": {"labels": [{"raw_secret": "NOT_FOR_BROWSER"}]},
            "splits": {"training": {"rows": 20}, "calibration": {"rows": 4}, "final": {"rows": 2}},
        },
        None,
        None,
    )


class ViewBoundary:
    """Typed-view double with deliberately no SQLite connection/transaction API."""

    _permission = staticmethod(PortfolioStore._permission)

    def __init__(self, root: Path) -> None:
        self.root = root
        self.active = False
        self.actors: list[Actor | None] = []
        initial = seed()
        self.view = RuntimeView(
            initial,
            digest(initial),
            frozenset(LOCATIONS),
            NOW.isoformat(),
            None,
            NOW.timestamp(),
            "e" * 64,
        )

    def clock(self) -> float:
        return self.view.checked_at

    @contextmanager
    def context(
        self,
        store: OperationalStore,
        *,
        actor: Actor | None = None,
        expected_seed_sha256: str | None = None,
    ) -> Iterator[RuntimeView]:
        assert store is self and not self.active, "nested workflow guard"
        assert expected_seed_sha256 in (None, self.view.seed_sha256)
        self.actors.append(actor)
        self.active = True
        try:
            yield self.view
        finally:
            self.active = False

    def state_root(self, store: OperationalStore) -> Path:
        assert store is self
        return self.root

    def change_source(self, source: str) -> None:
        assert not self.active
        replacement = seed(source)
        self.view = replace(
            self.view,
            seed=replacement,
            seed_sha256=digest(replacement),
            checked_at=self.view.checked_at + 1,
        )


@pytest.fixture
def boundary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ViewBoundary:
    item = ViewBoundary(tmp_path / "runtime")
    for module in (learning_views, runtime_feedback, runtime_monitoring):
        monkeypatch.setattr(module, "guarded_view", item.context)
        monkeypatch.setattr(module, "state_root", item.state_root)
        if hasattr(module, "read_view"):
            monkeypatch.setattr(module, "read_view", item.context)
    return item


def test_learning_uses_guarded_runtime_bound_sidecar_without_sql_emulation(
    boundary: ViewBoundary,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = LearningAttachments

    class GuardedAttachments(original):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            assert boundary.active
            super().__init__(*args, **kwargs)

        def attach(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
            assert boundary.active
            return super().attach(*args, **kwargs)

    monkeypatch.setattr(learning_views, "LearningAttachments", GuardedAttachments)
    store = cast(OperationalStore, boundary)
    assert learning_views.read_learning_report(store, HUMAN)["status"] == "not_run"
    assert not boundary.root.exists()
    result = learning_views.attach_learning_report(store, evaluated(), actor=REGISTRAR)
    assert learning_views.read_learning_report(store, HUMAN) == result
    assert learning_views.attach_learning_report(store, evaluated(), actor=REGISTRAR) == result
    assert result["report"]["locations"] == sorted(LOCATIONS)
    assert result["report"]["deployed"] is result["report"]["human_approval"] is False
    assert "NOT_FOR_BROWSER" not in str(result)
    assert boundary.actors == [HUMAN, None, HUMAN, None]


@pytest.mark.parametrize("missing", ["CURRENT", "FORECAST", "ARCHIVED"])
def test_learning_denial_happens_before_any_sidecar_constructor(
    boundary: ViewBoundary,
    monkeypatch: pytest.MonkeyPatch,
    missing: str,
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Unauthorized request constructed a sidecar")

    monkeypatch.setattr(learning_views, "LearningAttachments", forbidden)
    store = cast(OperationalStore, boundary)
    locations = tuple(location for location in LOCATIONS if location != missing)
    with pytest.raises(StoreError, match="every reported location"):
        learning_views.attach_learning_report(
            store, evaluated(), actor=replace(REGISTRAR, locations=locations)
        )
    with pytest.raises(StoreError, match="every reported location"):
        learning_views.read_learning_report(store, replace(HUMAN, locations=locations))
    assert not boundary.root.exists() and not boundary.active


def test_learning_retains_old_source_without_turning_it_into_current_authority(
    boundary: ViewBoundary,
) -> None:
    store = cast(OperationalStore, boundary)
    first = learning_views.attach_learning_report(store, evaluated(), actor=REGISTRAR)
    boundary.change_source("b")
    stale = learning_views.read_learning_report(store, HUMAN)
    assert stale == {**first, "status": "stale"}
    with pytest.raises(StoreError, match="different source"):
        learning_views.attach_learning_report(store, evaluated(), actor=REGISTRAR)
    current = learning_views.attach_learning_report(store, evaluated("b"), actor=REGISTRAR)
    assert current["status"] == "current" and current != first
    assert (
        LearningAttachments(boundary.root, boundary.view.runtime_identity).read()["sequence"] == 2
    )
    boundary.view = replace(boundary.view, runtime_identity="f" * 64)
    with pytest.raises(StoreError) as error:
        learning_views.read_learning_report(store, HUMAN)
    assert error.value.code == "integrity_error"


def test_learning_role_denial_precedes_workflow_or_filesystem_access(
    boundary: ViewBoundary,
) -> None:
    with pytest.raises(StoreError) as error:
        learning_views.attach_learning_report(
            cast(OperationalStore, boundary), evaluated(), actor=HUMAN
        )
    assert error.value.code == "role_denied"
    assert boundary.actors == [] and not boundary.root.exists()


def test_retention_and_monitor_share_existing_view_without_nested_workflow_guard(
    boundary: ViewBoundary,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Nested workflow guard during sidecar publication")

    monkeypatch.setattr("nexus.portfolio.feedback.guarded_view", forbidden)
    original = FeedbackBook._append

    def append(*args: Any, **kwargs: Any) -> dict[str, Any]:
        assert boundary.active
        return original(*args, **kwargs)

    monkeypatch.setattr(FeedbackBook, "_append", append)
    store = cast(OperationalStore, boundary)
    event = runtime_feedback.retain_runtime_forecasts(store, reader=HUMAN)
    assert event["data"]["seed_sha256"] == boundary.view.seed_sha256
    assert event["data"]["runtime_ingested_at"] == NOW.isoformat()
    assert runtime_feedback.retain_runtime_forecasts(store, reader=HUMAN) == event
    with boundary.context(store, actor=HUMAN) as view:
        runtime_feedback.authorize_runtime_feedback_scope(store, HUMAN, view=view)
        result = runtime_monitoring.monitor_runtime(store, actor=HUMAN, view=view)
    assert result["evidence_sha256"] == "a" * 64
    assert result["correction_closed_actuals_supplied"] == 0


@pytest.mark.parametrize("operation", ["retain", "monitor"])
def test_feedback_and_monitor_scope_denial_creates_no_journal(
    boundary: ViewBoundary,
    operation: str,
) -> None:
    store = cast(OperationalStore, boundary)
    partial = replace(HUMAN, locations=("CURRENT", "FORECAST"))
    with pytest.raises(StoreError) as error:
        if operation == "retain":
            runtime_feedback.retain_runtime_forecasts(store, reader=partial)
        else:
            runtime_monitoring.monitor_runtime(store, actor=partial)
    assert (error.value.code, error.value.status) == ("scope_denied", 403)
    assert not boundary.root.exists() and not boundary.active
