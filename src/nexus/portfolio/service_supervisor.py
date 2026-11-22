"""Owned-process worker supervision, bounded health reports and explicit failure."""

from __future__ import annotations

import json
import math
import queue
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nexus.portfolio.service_cli import Step
from nexus.portfolio.service_config import read_service_config
from nexus.portfolio.service_worker import json_report, validated_planning_timings


@dataclass
class Child:
    process: subprocess.Popen[str]
    sequence: int
    last_seen: float
    phase: str = "spawned"
    outcome: str | None = None
    completed: int = 0
    worker_pid: int | None = None


def _owned_pid(process: subprocess.Popen[str], pid: Any) -> bool:
    """Windows venv redirectors may report a child interpreter, not the launcher PID."""
    if type(pid) is not int or pid <= 0 or process.poll() is not None:
        return False
    if pid == process.pid:
        return True
    import psutil

    try:
        return any(
            child.pid == pid for child in psutil.Process(process.pid).children(recursive=True)
        )
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


def _read(role: str, stream: Any, messages: queue.Queue[tuple[str, str | None]]) -> None:
    try:
        while line := stream.readline(4097):
            messages.put((role, line))
    finally:
        messages.put((role, None))


def _discard_errors(stream: Any) -> None:
    # Driver stderr can contain private connection details. Drain, never publish it.
    while stream.read(4096):
        pass


def _terminate_owned(process: subprocess.Popen[str]) -> None:
    """Last resort only for this live Popen and its identity-checked descendants."""
    if process.poll() is not None:
        return
    import psutil

    try:
        parent = psutil.Process(process.pid)
        children = [(child, child.create_time()) for child in parent.children(recursive=True)]
        for child, created in reversed(children):
            try:
                if child.is_running() and child.create_time() == created:
                    child.terminate()
            except psutil.NoSuchProcess:
                pass
        process.terminate()
        process.wait(timeout=5)
        for child, created in children:
            try:
                if child.is_running() and child.create_time() == created:
                    child.kill()
            except psutil.NoSuchProcess:
                pass
    except (psutil.NoSuchProcess, ProcessLookupError):
        return
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def supervise(
    path: Path,
    *,
    duration: float,
    interval: float = 1.0,
    stale_seconds: float = 120,
    shutdown_seconds: float = 45,
    stop: threading.Event | None = None,
    emit: Callable[[dict[str, Any]], None] = json_report,
) -> int:
    """Supervise seven independent roles, not the API; never auto-restart a failed worker."""
    if any(
        isinstance(v, bool) or not math.isfinite(v)
        for v in (duration, interval, stale_seconds, shutdown_seconds)
    ) or not (
        1 <= duration <= 86400
        and 0.05 <= interval <= 30
        and 10 <= stale_seconds <= 600
        and 1 <= shutdown_seconds <= 120
    ):
        raise ValueError("Use bounded supervision times")
    config = read_service_config(path)
    policy = config.fence.source_watch
    if policy is None or interval >= policy.max_age_seconds / 2:
        raise ValueError("Observation interval must leave freshness headroom")
    stop = stop or threading.Event()
    messages: queue.Queue[tuple[str, str | None]] = queue.Queue(maxsize=512)
    children: dict[str, Child] = {}
    readers: list[threading.Thread] = []
    failed = False
    started = time.monotonic()

    def report(event: str, **fields: Any) -> None:
        emit({"contract": "nexus.service-supervisor.v1", "event": event, **fields})

    try:
        for action in Step:
            role = action.value
            options: dict[str, Any] = {}
            if sys.platform == "win32":
                options["creationflags"] = (
                    subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP
                )
            else:
                options["start_new_session"] = True
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "nexus.cli",
                    "portfolio",
                    "service",
                    "work",
                    str(path.absolute()),
                    role,
                    "--interval",
                    str(interval),
                    "--managed",
                    "--manifest-sha256",
                    config.manifest_sha256,
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                **options,
            )
            children[role] = Child(process, 0, time.monotonic())
            for target, args in (
                (_read, (role, process.stdout, messages)),
                (_discard_errors, (process.stderr,)),
            ):
                thread = threading.Thread(target=target, args=args, daemon=True)
                thread.start()
                readers.append(thread)
            report("spawned", role=role, pid=process.pid)
        while not stop.is_set() and time.monotonic() - started < duration:
            try:
                role, line = messages.get(timeout=0.1)
            except queue.Empty:
                role, line = "", None
            if role:
                child = children[role]
                if line is None:
                    raise RuntimeError("worker_pipe_closed")
                if len(line) > 4096:
                    raise RuntimeError("worker_report_oversize")
                value = json.loads(line)
                if (
                    not isinstance(value, dict)
                    or value.get("contract") != "nexus.service-worker.v1"
                    or value.get("role") != role
                    or (
                        child.worker_pid is None and not _owned_pid(child.process, value.get("pid"))
                    )
                    or (child.worker_pid is not None and value.get("pid") != child.worker_pid)
                    or type(value.get("sequence")) is not int
                    or value["sequence"] != child.sequence + 1
                    or value.get("event")
                    not in {"starting", "ready", "step_started", "step_completed"}
                ):
                    raise RuntimeError("worker_report_invalid_or_failed")
                child.sequence = value["sequence"]
                child.worker_pid = value["pid"]
                child.last_seen = time.monotonic()
                permitted = {
                    "spawned": "starting",
                    "starting": "ready",
                    "ready": "step_started",
                    "step_started": "step_completed",
                    "step_completed": "step_started",
                }
                if permitted.get(child.phase) != value["event"]:
                    raise RuntimeError("worker_transition_invalid")
                child.phase = value["event"]
                if child.phase == "step_completed":
                    child.completed += 1
                    child.outcome = value.get("outcome")
                    if not isinstance(child.outcome, str) or len(child.outcome) > 100:
                        raise RuntimeError("worker_outcome_invalid")
                timings = value.get("timings")
                if timings is not None and (
                    role != Step.plan.value
                    or child.phase != "step_completed"
                    or child.outcome != "completed"
                ):
                    raise RuntimeError("planning_timings_wrong_phase")
                report(
                    "worker_progress",
                    role=role,
                    phase=child.phase,
                    outcome=child.outcome,
                    claim="process_progress_not_business_readiness",
                    **(
                        {"timings": validated_planning_timings(timings)}
                        if timings is not None
                        else {}
                    ),
                )
            for child in children.values():
                if (
                    child.process.poll() is not None
                    or time.monotonic() - child.last_seen > stale_seconds
                ):
                    raise RuntimeError("worker_exit_or_stale")
        if not stop.is_set() and any(child.completed == 0 for child in children.values()):
            raise RuntimeError("worker_never_completed")
    except KeyboardInterrupt:
        stop.set()
    except Exception as error:
        failed = True
        reasons = {
            "worker_pipe_closed",
            "worker_report_oversize",
            "worker_report_invalid_or_failed",
            "worker_transition_invalid",
            "worker_outcome_invalid",
            "worker_exit_or_stale",
            "worker_never_completed",
        }
        reason = str(error) if str(error) in reasons else "worker_start_or_protocol_failure"
        report("failed", reason=reason, error_type=type(error).__name__)
    finally:
        for child in children.values():
            try:
                assert child.process.stdin is not None
                child.process.stdin.write("STOP\n")
                child.process.stdin.flush()
                child.process.stdin.close()
            except (BrokenPipeError, OSError):
                pass
            finally:
                if child.process.stdin is not None:
                    with suppress(OSError):
                        child.process.stdin.close()
        deadline = time.monotonic() + shutdown_seconds
        while (
            any(c.process.poll() is None for c in children.values()) and time.monotonic() < deadline
        ):
            # Drain reports while stopping so a full stdout pipe cannot prevent shutdown.
            with suppress(queue.Empty):
                messages.get(timeout=0.05)
        for role, child in children.items():
            if child.process.poll() is None:
                failed = True
                report("forced_stop", role=role, pid=child.process.pid)
                try:
                    _terminate_owned(child.process)
                except Exception:
                    report("termination_failed", role=role, pid=child.process.pid)
            elif child.process.returncode != 0:
                failed = True
            report(
                "worker_exit",
                role=role,
                exit_code=child.process.poll(),
                completed_steps=child.completed,
            )
        for thread in readers:
            thread.join(timeout=1)
        if any(thread.is_alive() for thread in readers):
            failed = True
            report("reader_shutdown_failed")
        else:
            for child in children.values():
                for stream in (child.process.stdout, child.process.stderr):
                    if stream is not None:
                        with suppress(OSError):
                            stream.close()
        report(
            "stopped",
            successful=not failed,
            claim="bounded_supervision_not_full_service_or_soak_acceptance",
        )
    return 1 if failed else 0
