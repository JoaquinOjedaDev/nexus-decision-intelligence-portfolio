"""Persistent worker and real owned-process supervisor lifecycle checks."""

import io
import json
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest
from test_portfolio_service_cli import manifest

from nexus.portfolio import service_supervisor, service_worker
from nexus.portfolio.service_cli import Step
from nexus.portfolio.service_config import read_service_config
from nexus.portfolio.store import StoreError


@pytest.mark.parametrize(
    "bad", ["extra", "nan", "negative", "bool", "dbt_exceeds_build", "stage_exceeds_total"]
)
def test_planning_timing_report_rejects_unbounded_or_unknown_values(bad):
    values = dict.fromkeys(
        ("prepare", "claim", "source", "build", "verify", "activate", "retain", "total"), 1.0
    )
    assert service_worker.validated_planning_timings(values) == values
    if bad == "extra":
        values["secret"] = "DO_NOT_PRINT"
    elif bad == "nan":
        values["total"] = float("nan")
    elif bad == "negative":
        values["source"] = -1
    elif bad == "bool":
        values["source"] = True
    elif bad == "dbt_exceeds_build":
        values["total"] = 3
        values["dbt"] = 2
    else:
        values["source"] = 2
    with pytest.raises(ValueError, match="Invalid bounded planning timings"):
        service_worker.validated_planning_timings(values)


def test_worker_reuses_instance_and_emits_bounded_reports(tmp_path):
    path, _ = manifest(tmp_path)
    events, instances = [], []

    class Worker:
        def __init__(self, config, action):
            instances.append(self)
            self.ticks = 0
            self.closed = False

        def tick(self):
            self.ticks += 1
            return "idle"

        def close(self):
            self.closed = True

    assert (
        service_worker.run_worker(
            read_service_config(path),
            Step.publish,
            interval=0.05,
            max_steps=3,
            emit=events.append,
            factory=Worker,
        )
        == 0
    )
    assert len(instances) == 1 and instances[0].ticks == 3 and instances[0].closed
    assert [e["sequence"] for e in events] == list(range(1, len(events) + 1))
    assert events[-1]["event"] == "stopped" and events[-1]["steps"] == 3


@pytest.mark.parametrize("close_failure", [False, True])
def test_worker_failure_is_redacted_and_closed(tmp_path, close_failure):
    path, _ = manifest(tmp_path)
    events, closed = [], []

    class Worker:
        def __init__(self, config, action):
            pass

        def tick(self):
            raise RuntimeError("password=DO_NOT_PRINT")

        def close(self):
            closed.append(True)
            if close_failure:
                raise RuntimeError("password=DO_NOT_PRINT")

    assert (
        service_worker.run_worker(
            read_service_config(path), Step.publish, emit=events.append, factory=Worker
        )
        == 1
    )
    assert closed == [True] and events[-1]["event"] == "failed"
    assert "DO_NOT_PRINT" not in json.dumps(events)


def test_parent_eof_and_manifest_change_fail_closed(tmp_path, monkeypatch):
    stop = threading.Event()
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    service_worker.watch_parent(stop)
    assert stop.is_set()
    path, _ = manifest(tmp_path)
    config = read_service_config(path)
    path.write_text(path.read_text() + " ", encoding="utf-8")
    with pytest.raises(StoreError):
        read_service_config(path, expected_sha256=config.manifest_sha256)


def test_schema_poison_blocks_without_impersonating_human():
    worker = object.__new__(service_worker.ServiceWorker)
    worker.closed = False
    worker.action = Step.dispatch
    worker.consumer = worker.publisher = None
    worker.binding = object()
    worker.store = SimpleNamespace(control=object())

    def dispatch(*args):
        raise service_worker.BrokerAdmissionError("invalid_event_envelope")

    worker.ledger = SimpleNamespace(dispatch_once=dispatch)
    assert worker.tick() == "blocked_schema_poison"

    def broken(*args):
        raise service_worker.BrokerAdmissionError("broker_commit_unconfirmed")

    worker.ledger.dispatch_once = broken
    with pytest.raises(service_worker.BrokerAdmissionError):
        worker.tick()


CHILD_SCRIPT = r"""
import json,os,sys,time
role,mode=sys.argv[1:]
seq=0
def report(event,**fields):
    global seq
    seq+=1
    pid=1 if mode=='wrongpid' and role=='plan' else os.getpid()
    print(json.dumps(dict(contract='nexus.service-worker.v1',pid=pid,role=role,
                         sequence=seq,event=event,**fields)),flush=True)
if mode=='crash' and role=='plan':
    raise SystemExit(4)
if mode=='transition' and role=='plan':
    report('ready')
if mode=='oversize' and role=='plan':
    print('x'*5000,flush=True)
report('starting')
report('ready')
report('step_started')
report('step_completed',outcome='idle')
if mode=='bad' and role=='plan':
    print('secret=DO_NOT_PRINT',flush=True)
if mode=='hang' and role=='plan':
    time.sleep(30)
else:
    sys.stdin.readline()
report('stopped',steps=1)
"""


@pytest.mark.parametrize(
    "mode", ["healthy", "crash", "bad", "hang", "wrongpid", "transition", "oversize", "stale"]
)
def test_supervisor_real_process_shutdown_and_failures(tmp_path, monkeypatch, mode):
    path, _ = manifest(tmp_path)
    original = subprocess.Popen
    processes, events = [], []

    def launch(command, **kwargs):
        role = command[command.index("work") + 2]
        assert "--manifest-sha256" in command and "--managed" in command
        process = original([sys.executable, "-u", "-c", CHILD_SCRIPT, role, mode], **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(service_supervisor.subprocess, "Popen", launch)
    result = service_supervisor.supervise(
        path,
        duration=12 if mode == "stale" else 2,
        stale_seconds=10,
        shutdown_seconds=1,
        emit=events.append,
    )
    assert result == (0 if mode == "healthy" else 1), json.dumps(events)
    assert len(processes) == 7 and all(p.poll() is not None for p in processes)
    assert events[-1]["event"] == "stopped"
    assert "DO_NOT_PRINT" not in json.dumps(events)
    if mode == "hang":
        assert any(e["event"] == "forced_stop" for e in events)
    if mode == "stale":
        assert any(e.get("reason") == "worker_exit_or_stale" for e in events)
