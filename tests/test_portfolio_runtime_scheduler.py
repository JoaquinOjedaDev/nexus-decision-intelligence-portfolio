"""Local monitor lifecycle: lazy, bounded, visible failures, automatic retry."""

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from nexus.portfolio import runtime_scheduler as scheduler


@pytest.fixture(autouse=True)
def feedback_monitor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduler, "monitor_runtime_feedback", lambda _: {"status": "unknown"})


def application(store: object | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(portfolio_store=store, runtime_monitor_error=False)
    )


def test_tick_does_not_initialize_an_unused_demo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduler, "monitor_runtime", lambda _: pytest.fail("must stay lazy"))
    app = application()
    scheduler.monitoring_tick(app)
    assert app.state.portfolio_store is None and not app.state.runtime_monitor_error


def test_failure_is_visible_and_next_tick_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    app = application(object())

    def failed(_: object) -> None:
        raise ValueError("invalid evidence")

    monkeypatch.setattr(scheduler, "monitor_runtime", failed)
    scheduler.monitoring_tick(app)
    assert app.state.runtime_monitor_error
    monkeypatch.setattr(scheduler, "monitor_runtime", lambda _: {"status": "unknown"})
    scheduler.monitoring_tick(app)
    assert not app.state.runtime_monitor_error


def test_monitor_runs_automatically_and_stops_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = threading.Event()
    monkeypatch.setattr(scheduler, "monitor_runtime", lambda _: observed.set())
    worker = scheduler.RuntimeMonitor(application(object()), interval_seconds=0.01)
    worker.start()
    try:
        assert observed.wait(timeout=2), "monitor did not evaluate within the test bound"
    finally:
        worker.close()
    assert not worker.thread.is_alive()


def test_feedback_failure_is_visible_and_retry_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    app = application(object())
    monkeypatch.setattr(scheduler, "monitor_runtime", lambda _: {"status": "unknown"})

    def failed(_: object) -> None:
        raise ValueError("feedback integrity failure")

    monkeypatch.setattr(scheduler, "monitor_runtime_feedback", failed)
    scheduler.monitoring_tick(app)
    assert app.state.runtime_monitor_error
    monkeypatch.setattr(scheduler, "monitor_runtime_feedback", lambda _: {"status": "unknown"})
    scheduler.monitoring_tick(app)
    assert not app.state.runtime_monitor_error


@pytest.mark.parametrize("interval", [0, -1, 4000, float("nan")])
def test_invalid_interval_rejected(interval: float) -> None:
    with pytest.raises(ValueError):
        scheduler.RuntimeMonitor(application(), interval_seconds=interval)
