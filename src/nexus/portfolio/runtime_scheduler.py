"""One local monitoring tick per minute while the connected app is running.

No external scheduler, cloud account, model call or automatic order execution.
An uninitialized app stays lazy. Failure is visible and retried on the next tick.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from nexus.portfolio.runtime_feedback import monitor_runtime_feedback
from nexus.portfolio.runtime_monitoring import monitor_runtime

LOGGER = logging.getLogger(__name__)


def monitoring_tick(application: Any) -> None:
    store = application.state.portfolio_store
    if store is None:
        return
    try:
        monitor_runtime(store)
        monitor_runtime_feedback(store)
    except Exception:
        LOGGER.exception("Local measured monitoring failed")
        application.state.runtime_monitor_error = True
    else:
        application.state.runtime_monitor_error = False


class RuntimeMonitor:
    """Owned by the app lifespan and stopped before that lifespan completes."""

    def __init__(self, application: Any, *, interval_seconds: float = 60) -> None:
        if not 0.01 <= interval_seconds <= 3600:
            raise ValueError("monitor interval must be between 0.01 and 3600 seconds")
        self.application = application
        self.interval_seconds = interval_seconds
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True, name="nexus-monitor")

    def _run(self) -> None:
        while not self.stop_event.wait(self.interval_seconds):
            monitoring_tick(self.application)

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=15)
        if self.thread.is_alive():
            LOGGER.error("Local monitor did not stop within its bounded shutdown window")
