"""Opt-in sequential dbt invocation for an owned, supervised planning process.

Never use this runner inside an HTTP process. dbt changes process globals; its
owner must be a dedicated worker, with an external process watchdog for hangs.
The ordinary R2 CLI continues to use a bounded subprocess.
"""

from __future__ import annotations

import importlib
import os
import threading
from time import perf_counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from contextvars import ContextVar
from typing import Any

DbtExecutor = Callable[[list[str], dict[str, str]], str]
_EXECUTOR: ContextVar[DbtExecutor | None] = ContextVar("nexus_dbt_executor", default=None)
_INVOCATION_LOCK = threading.Lock()


def current_dbt_executor() -> DbtExecutor | None:
    return _EXECUTOR.get()


@contextmanager
def use_dbt_executor(executor: DbtExecutor) -> Iterator[None]:
    token = _EXECUTOR.set(executor)
    try:
        yield
    finally:
        _EXECUTOR.reset(token)


class WorkerDbtRunner:
    """Same dbt build command, fresh manifest, explicit cleanup after each call."""

    def __init__(self) -> None:
        self.owner = (os.getpid(), threading.get_ident())
        self.failed = False
        self.last_seconds: float | None = None
        self._runner: Any = None
        self._reset: Any = None
        self._connections: Any = None
        # Imports are warmed once before the worker advertises ready. Do not let
        # third-party output enter the supervisor's bounded JSON report stream.
        with open(os.devnull, "w") as sink, redirect_stdout(sink), redirect_stderr(sink):
            self._runner = importlib.import_module("dbt.cli.main").dbtRunner
            self._reset = importlib.import_module("dbt.adapters.factory").reset_adapters
            self._connections = importlib.import_module(
                "dbt.adapters.duckdb.connections"
            ).DuckDBConnectionManager

    def __call__(self, command: list[str], environment: dict[str, str]) -> str:
        if self.failed or self.owner != (os.getpid(), threading.get_ident()):
            raise RuntimeError("dbt requires a fresh runner in its owning planning thread")
        if not command or command[0] != "build":
            raise ValueError("Planning runner accepts only the R2 build command")
        if not _INVOCATION_LOCK.acquire(blocking=False):
            raise RuntimeError("Concurrent in-process dbt invocation refused")
        keys = ("NEXUS_DBT_PATH", "TZ", "DBT_SEND_ANONYMOUS_USAGE_STATS")
        previous = {key: os.environ.get(key) for key in keys}
        started = perf_counter()
        try:
            os.environ["NEXUS_DBT_PATH"] = environment["NEXUS_DBT_PATH"]
            os.environ["TZ"] = "UTC"
            os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
            with open(os.devnull, "w") as sink, redirect_stdout(sink), redirect_stderr(sink):
                try:
                    result = self._runner().invoke(
                        [*command, "--quiet", "--no-send-anonymous-usage-stats"]
                    )
                    if not result.success or result.exception is not None:
                        raise RuntimeError("dbt build failed; inspect attempt-local logs")
                    rows = getattr(result.result, "results", None)
                    if not rows or any(str(row.status) not in {"pass", "success"} for row in rows):
                        raise RuntimeError("dbt did not retain exclusively successful results")
                    count = len(rows)
                finally:
                    # dbt-duckdb's environment retains its root connection even
                    # after cursor cleanup; release it before read-only Gold export.
                    try:
                        self._reset()
                    finally:
                        self._connections.close_all_connections()
            return f"Done. PASS={count} WARN=0 ERROR=0 SKIP=0 TOTAL={count}"
        except BaseException:
            self.failed = True
            raise
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            _INVOCATION_LOCK.release()
            self.last_seconds = perf_counter() - started
