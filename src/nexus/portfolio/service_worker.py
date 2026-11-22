"""Persistent local worker, bounded reports and fail-stop resource ownership."""

from __future__ import annotations

import json
import math
import os
import sys
import threading
import time
from collections.abc import Callable
from typing import Any

from nexus.config import PROJECT_ROOT
from nexus.contracts import ActorRole
from nexus.data.r2_dbt_execution import WorkerDbtRunner, use_dbt_executor
from nexus.portfolio.auth import Actor
from nexus.portfolio.broker_runtime import BrokerAdmissionError
from nexus.portfolio.capture_health_ledger import CaptureHealthLedger
from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger
from nexus.portfolio.runtime_service import run_runtime_worker
from nexus.portfolio.service_cli import Step, open_service
from nexus.portfolio.service_config import ServiceConfig
from nexus.portfolio.source_publisher import SourceOutboxPublisher
from nexus.portfolio.store import StoreError
from nexus.portfolio.transport_consumer import DurableBrokerConsumer
from nexus.portfolio.transport_ledger import TransportLedger


class ServiceWorker:
    """Retain one store/consumer per process; do not reconcile the full log each poll."""

    def __init__(self, config: ServiceConfig, action: Step):
        if not isinstance(action, Step):
            raise ValueError("Explicit worker role required")
        self.action = action
        self.binding = (
            config.audit_binding if action is Step.capture_audit else config.fence.binding
        )
        self.store = None if action is Step.capture_audit else open_service(config)
        self.ledger = TransportLedger(config.audit) if self.store is None else self.store.transport
        self.consumer = None
        self.publisher = None
        self.dbt_runner = WorkerDbtRunner() if action is Step.plan else None
        self.last_timings: dict[str, float] | None = None
        if self.store is not None and not isinstance(self.ledger, RecoveryResolutionLedger):
            raise StoreError("configuration_error", "Explicit recovery ledger required")
        if action in {Step.capture_operations, Step.capture_audit}:
            self.consumer = DurableBrokerConsumer(self.ledger, self.binding)
            if isinstance(self.ledger, CaptureHealthLedger):
                try:
                    self.ledger.begin_capture(
                        self.binding, self.consumer.instance_id, self._capture_actor()
                    )
                except BaseException:
                    self.consumer.close()
                    raise
        elif action is Step.publish:
            assert self.store is not None
            self.publisher = SourceOutboxPublisher(self.store)
        self.closed = False

    def _capture_actor(self) -> Actor:
        return Actor(
            "capture-operations",
            "Operations capture",
            ActorRole.SYSTEM,
            self.binding.broker.locations,
        )

    def tick(self) -> str:
        self.last_timings = None
        if self.closed:
            raise StoreError("worker_closed", "Worker requires a fresh reconciled start")
        result: Any
        if self.consumer is not None:
            result = self.consumer.poll_batch()
            if isinstance(self.ledger, CaptureHealthLedger):
                observed = self.consumer.sample_health()
                recorded = self.ledger.record_health(self.binding, observed, self._capture_actor())
                return "capture_" + str(recorded["reason"])
        elif self.publisher is not None:
            result = self.publisher.publish_once()
        else:
            assert self.store is not None
            if self.action is Step.observe:
                result = self.store.poll_source(
                    Actor(
                        "source-observer",
                        "Source observer",
                        ActorRole.SYSTEM,
                        self.store.locations(),
                    )
                )
                return "observed_" + str(result["reason"])
            if self.action is Step.dispatch:
                try:
                    result = self.ledger.dispatch_once(self.binding, self.store.control)
                except BrokerAdmissionError as error:
                    if str(error) in {
                        "invalid_payload_size",
                        "invalid_event_envelope",
                        "unexpected_event_contract",
                    }:
                        # Keep publication/capture alive so a reviewed correction can arrive.
                        # No delivery advance, retry skip or actor impersonation occurs.
                        return "blocked_schema_poison"
                    raise
            elif self.action is Step.plan:
                assert self.dbt_runner is not None
                with use_dbt_executor(self.dbt_runner):
                    result = run_runtime_worker(PROJECT_ROOT, self.store)
                if result["status"] == "failed":
                    raise StoreError(
                        "planning_failed", "Planning worker failed; inspect retained job"
                    )
                if result["status"] == "completed":
                    values = dict(result["timings"])
                    if self.dbt_runner.last_seconds is not None:
                        values["dbt"] = self.dbt_runner.last_seconds
                    self.last_timings = validated_planning_timings(values)
                return str(result["status"])
            elif self.action is Step.recovery_publish:
                assert isinstance(self.ledger, RecoveryResolutionLedger)
                result = self.ledger.publish_once(
                    self.binding,
                    Actor(
                        "recovery-publisher",
                        "Recovery publisher",
                        ActorRole.SYSTEM,
                        self.store.locations(),
                    ),
                )
            else:
                raise StoreError("invalid_step", "Unknown worker role")
        return "idle" if result is None else "progress"

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            if self.consumer is not None:
                self.consumer.close()


def json_report(value: dict[str, Any]) -> None:
    print(json.dumps(value, separators=(",", ":")), flush=True)


def validated_planning_timings(value: Any) -> dict[str, float]:
    required = {"prepare", "claim", "source", "build", "verify", "activate", "retain", "total"}
    if (
        not isinstance(value, dict)
        or not required <= set(value) <= required | {"dbt"}
        or any(
            type(v) not in (int, float) or not 0 <= v <= 86400 or not math.isfinite(v)
            for v in value.values()
        )
        or any(v > value["total"] for k, v in value.items() if k != "total")
        or value.get("dbt", 0) > value["build"]
    ):
        raise ValueError("Invalid bounded planning timings")
    return dict(value)


def run_worker(
    config: ServiceConfig,
    action: Step,
    *,
    interval: float = 1.0,
    max_steps: int = 0,
    stop: threading.Event | None = None,
    emit: Callable[[dict[str, Any]], None] = json_report,
    factory: Callable[..., ServiceWorker] = ServiceWorker,
) -> int:
    if (
        isinstance(interval, bool)
        or not math.isfinite(interval)
        or not 0.05 <= interval <= 30
        or type(max_steps) is not int
        or max_steps < 0
    ):
        raise ValueError("Use a bounded worker interval and step count")
    policy = config.fence.source_watch
    if action is Step.observe and (policy is None or interval >= policy.max_age_seconds / 2):
        raise ValueError("Observation interval must leave freshness headroom")
    stop = stop or threading.Event()
    sequence = 0

    def report(event: str, **fields: Any) -> None:
        nonlocal sequence
        sequence += 1
        emit(
            {
                "contract": "nexus.service-worker.v1",
                "pid": os.getpid(),
                "role": action.value,
                "sequence": sequence,
                "event": event,
                **fields,
            }
        )

    worker = None
    steps = 0
    failed = False
    report("starting")
    try:
        worker = factory(config, action)
        report("ready")
        while not stop.is_set() and (max_steps == 0 or steps < max_steps):
            report("step_started")
            started = time.monotonic()
            outcome = worker.tick()
            steps += 1
            timings = getattr(worker, "last_timings", None)
            report(
                "step_completed",
                outcome=outcome,
                elapsed_seconds=time.monotonic() - started,
                **({"timings": validated_planning_timings(timings)} if timings is not None else {}),
            )
            if max_steps and steps >= max_steps:
                break
            stop.wait(interval)
    except KeyboardInterrupt:
        stop.set()
    except Exception:
        # Never expose database exceptions, driver locals or raw payloads on worker pipes.
        report("failed", reason="worker_step_failed")
        failed = True
    finally:
        if worker is not None:
            try:
                worker.close()
            except Exception:
                report("failed", reason="worker_close_failed")
                failed = True
    if not failed:
        report("stopped", steps=steps)
    return 1 if failed else 0


def watch_parent(stop: threading.Event) -> None:
    """Managed workers stop on STOP or parent-pipe EOF; no orphan restart loop."""
    sys.stdin.readline(32)
    stop.set()
