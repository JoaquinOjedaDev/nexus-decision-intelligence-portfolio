"""Owned sequential executor isolation, redaction and fail-stop boundaries."""

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from nexus.data import r2_dbt_execution as execution


def runner(statuses=("success", "pass"), failure=False):
    calls = []

    class Invoke:
        def invoke(self, command):
            calls.append(command)
            print("private backend output")
            return SimpleNamespace(
                success=not failure,
                exception=None,
                result=SimpleNamespace(results=[SimpleNamespace(status=s) for s in statuses]),
            )

    value = object.__new__(execution.WorkerDbtRunner)
    value.owner = (os.getpid(), threading.get_ident())
    value.failed = False
    value._runner = Invoke
    value._reset = lambda: calls.append("reset")
    value._connections = SimpleNamespace(close_all_connections=lambda: calls.append("close"))
    return value, calls


def test_context_restores_even_after_failure():
    def callback(*args):
        return "ok"

    assert execution.current_dbt_executor() is None
    with pytest.raises(ValueError), execution.use_dbt_executor(callback):
        assert execution.current_dbt_executor() is callback
        raise ValueError("fixture")
    assert execution.current_dbt_executor() is None


def test_success_cleans_and_restores_environment_without_stdout(monkeypatch, capsys):
    monkeypatch.setenv("NEXUS_DBT_PATH", "original")
    monkeypatch.delenv("TZ", raising=False)
    value, calls = runner()
    assert value(["build"], {"NEXUS_DBT_PATH": "owned"}) == (
        "Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2"
    )
    assert calls[-2:] == ["reset", "close"]
    assert calls[0][-2:] == ["--quiet", "--no-send-anonymous-usage-stats"]
    assert os.environ["NEXUS_DBT_PATH"] == "original" and "TZ" not in os.environ
    assert not capsys.readouterr().out
    assert not value.failed


@pytest.mark.parametrize("statuses", [(), ("warn",), ("error",), ("skipped",)])
def test_nonpassing_results_latch_failure_and_cleanup(statuses):
    value, calls = runner(statuses)
    with pytest.raises(RuntimeError):
        value(["build"], {"NEXUS_DBT_PATH": "owned"})
    assert calls[-2:] == ["reset", "close"] and value.failed
    with pytest.raises(RuntimeError, match="fresh runner"):
        value(["build"], {"NEXUS_DBT_PATH": "owned"})


def test_build_and_cleanup_failures_are_fail_stop():
    value, calls = runner(failure=True)
    with pytest.raises(RuntimeError, match="build failed"):
        value(["build"], {"NEXUS_DBT_PATH": "owned"})
    assert value.failed and calls[-2:] == ["reset", "close"]
    value, calls = runner()

    def fail():
        raise RuntimeError("cleanup fixture")

    value._reset = fail
    with pytest.raises(RuntimeError, match="cleanup fixture"):
        value(["build"], {"NEXUS_DBT_PATH": "owned"})
    assert value.failed and calls[-1] == "close"


def test_wrong_thread_and_concurrent_invocation_are_refused():
    value, calls = runner()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(value, ["build"], {"NEXUS_DBT_PATH": "owned"})
        with pytest.raises(RuntimeError, match="owning planning thread"):
            future.result()
    assert calls == []
    with execution._INVOCATION_LOCK, pytest.raises(RuntimeError, match="Concurrent"):
        value(["build"], {"NEXUS_DBT_PATH": "owned"})
    assert calls == []
