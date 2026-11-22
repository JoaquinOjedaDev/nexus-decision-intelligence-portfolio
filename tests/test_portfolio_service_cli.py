"""Pinned service manifest and safe command boundaries without opening services."""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from nexus.portfolio import service_cli
from nexus.portfolio.broker_runtime import RuntimeBrokerConfig
from nexus.portfolio.postgres_backend import PostgresConfig
from nexus.portfolio.runtime_service import _postgres_store
from nexus.portfolio.service_config import read_service_config
from nexus.portfolio.store import StoreError
from nexus.portfolio.transport_ledger import TransportBinding, runtime_target


def manifest(tmp_path: Path):
    root = tmp_path / "runtime"
    source = tmp_path / "source"
    source.mkdir()
    dsn = "host=127.0.0.1 port=55439 dbname=nexus_test user=nexus_app password=DO_NOT_PRINT"
    config = PostgresConfig(dsn, "nexus_workflow", root / "evidence" / "initial")
    base = _postgres_store(root, config)

    def binding(group, purpose):
        return TransportBinding(
            RuntimeBrokerConfig(
                "127.0.0.1:19092", "nexus-runtime-events", "nexus-runtime-" + group, ("LOC-001",)
            ),
            "cluster-1",
            "a" * 32,
            2,
            purpose,
            runtime_target(base.control) if purpose == "operations" else None,
        ).payload()

    value = {
        "contract": "nexus.local-service.v1",
        "runtime_root": str(root),
        "workflow": {"dsn": dsn, "schema": config.schema},
        "operations": {
            "database": {"dsn": dsn, "schema": "nexus_operations"},
            "binding": binding("ops", "operations"),
        },
        "audit": {
            "database": {"dsn": dsn, "schema": "nexus_audit"},
            "binding": binding("audit", "audit"),
        },
        "source_watch": {
            "contract": "nexus.source-watch-policy.v1",
            "source_id": "inventory",
            "source_root": str(source),
            "max_age_seconds": 30,
        },
    }
    path = tmp_path / "private-service.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return path, value


def test_manifest_check_is_nonmutating_and_redacted(tmp_path):
    path, value = manifest(tmp_path)
    config = read_service_config(path)
    assert config.fence.capture_health_max_age_seconds is None
    assert config.fence.recovery_resolution and config.fence.source_watch
    assert "DO_NOT_PRINT" not in repr(config)
    result = CliRunner().invoke(service_cli.app, ["check", str(path)])
    assert result.exit_code == 0, result.output
    assert "configuration_valid_not_service_ready" in result.output
    assert "DO_NOT_PRINT" not in result.output
    assert not Path(value["runtime_root"]).exists()


def test_v2_manifest_requires_persisted_capture_health_without_opening_services(tmp_path):
    from nexus.portfolio.capture_health_ledger import CaptureHealthLedger

    path, value = manifest(tmp_path)
    value["contract"] = "nexus.local-service.v2"
    value["capture_health"] = {"max_age_seconds": 10}
    path.write_text(json.dumps(value), encoding="utf-8")
    config = read_service_config(path)
    assert config.fence.capture_health_max_age_seconds == 10
    store = _postgres_store(config.root, config.workflow, transport_fence=config.fence)
    assert isinstance(store.transport, CaptureHealthLedger)
    assert store.transport.max_age_seconds == 10
    assert not config.root.exists()


@pytest.mark.parametrize("ttl", [None, True, False, 0, -1, 61, 10.0, "10"])
def test_v2_manifest_rejects_invalid_capture_lifetime(tmp_path, ttl):
    path, value = manifest(tmp_path)
    value["contract"] = "nexus.local-service.v2"
    value["capture_health"] = {"max_age_seconds": ttl}
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(StoreError, match="Invalid private local service manifest"):
        read_service_config(path)
    assert not Path(value["runtime_root"]).exists()


@pytest.mark.parametrize("mutation", ["missing", "extra", "v1_extra"])
def test_capture_health_manifest_contract_cannot_silently_downgrade(tmp_path, mutation):
    path, value = manifest(tmp_path)
    value["contract"] = "nexus.local-service.v2"
    value["capture_health"] = {"max_age_seconds": 10}
    if mutation == "missing":
        del value["capture_health"]
    elif mutation == "extra":
        value["capture_health"]["enabled"] = False
    else:
        value["contract"] = "nexus.local-service.v1"
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(StoreError, match="Invalid private local service manifest"):
        read_service_config(path)


@pytest.mark.parametrize(
    "mutation",
    [
        "extra",
        "duplicate",
        "schema",
        "group",
        "topic",
        "cluster",
        "scope",
        "target",
        "bool_offset",
        "relative",
        "remote",
        "ttl",
        "oversize",
    ],
)
def test_manifest_rejects_unsafe_or_mixed_bindings(tmp_path, mutation):
    path, value = manifest(tmp_path)
    if mutation == "extra":
        value["migration_dsn"] = "DO_NOT_PRINT"
    elif mutation == "schema":
        value["audit"]["database"]["schema"] = value["workflow"]["schema"]
    elif mutation == "group":
        value["audit"]["binding"]["group"] = value["operations"]["binding"]["group"]
    elif mutation in {"topic", "cluster"}:
        field = "cluster_id" if mutation == "cluster" else "topic"
        value["audit"]["binding"][field] = "nexus-runtime-other"
    elif mutation == "scope":
        value["audit"]["binding"]["locations"] = ["OTHER"]
    elif mutation == "target":
        value["operations"]["binding"]["runtime_target_sha256"] = "0" * 64
    elif mutation == "bool_offset":
        value["audit"]["binding"]["initial_offset"] = False
    elif mutation == "relative":
        value["runtime_root"] = "relative"
    elif mutation == "remote":
        value["audit"]["binding"]["endpoint"] = "example.com:9092"
    elif mutation == "ttl":
        value["source_watch"]["max_age_seconds"] = True
    raw = json.dumps(value)
    if mutation == "duplicate":
        raw = raw[:-1] + ',"contract":"nexus.local-service.v1"}'
    if mutation == "oversize":
        raw += " " * 32769
    path.write_text(raw, encoding="utf-8")
    result = CliRunner().invoke(service_cli.app, ["check", str(path)])
    assert result.exit_code == 1
    assert "configuration_error" in result.output and "DO_NOT_PRINT" not in result.output


def test_failed_step_does_not_echo_backend_details(tmp_path, monkeypatch):
    path, _ = manifest(tmp_path)

    def fail(*args):
        raise service_cli.BrokerAdmissionError("DO_NOT_PRINT")

    monkeypatch.setattr(service_cli, "run_step", fail)
    result = CliRunner().invoke(service_cli.app, ["step", str(path), "publish"])
    assert result.exit_code == 1 and "broker_unavailable" in result.output
    assert "DO_NOT_PRINT" not in result.output


def test_unknown_step_cannot_touch_services(tmp_path, monkeypatch):
    path, _ = manifest(tmp_path)
    monkeypatch.setattr(service_cli, "run_step", lambda *args: pytest.fail("must not run"))
    result = CliRunner().invoke(service_cli.app, ["step", str(path), "approve"])
    assert result.exit_code != 0


def test_bad_manifest_is_safe_exception(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text('{"password":"DO_NOT_PRINT"}', encoding="utf-8")
    with pytest.raises(StoreError) as error:
        read_service_config(path)
    assert "DO_NOT_PRINT" not in str(error.value)


@pytest.mark.parametrize(
    "payload", ['{"secret":"DO_NOT_PRINT"}', "x" * 65537], ids=["malformed", "oversized"]
)
def test_source_command_rejects_invalid_event_before_opening_services(
    tmp_path, monkeypatch, payload
):
    path, _ = manifest(tmp_path)
    event = tmp_path / "event.json"
    event.write_text(payload, encoding="utf-8")
    monkeypatch.setattr(service_cli, "open_service", lambda *args: pytest.fail("must not open"))
    result = CliRunner().invoke(service_cli.app, ["enqueue-source", str(path), str(event)])
    assert result.exit_code == 1 and "DO_NOT_PRINT" not in result.output


def test_capture_closes_on_failure_without_loading_workflow(tmp_path, monkeypatch):
    path, _ = manifest(tmp_path)
    config = read_service_config(path)
    calls = []

    class Consumer:
        def __init__(self, ledger, binding):
            assert binding.purpose == "audit"

        def poll_batch(self):
            raise service_cli.BrokerAdmissionError("test failure")

        def close(self):
            calls.append("closed")

    monkeypatch.setattr(service_cli, "DurableBrokerConsumer", Consumer)
    monkeypatch.setattr(
        service_cli, "open_service", lambda *args: pytest.fail("audit must be independent")
    )
    with pytest.raises(service_cli.BrokerAdmissionError):
        service_cli.run_step(config, service_cli.Step.capture_audit)
    assert calls == ["closed"]


def test_serve_passes_exact_config_to_loopback_app(tmp_path, monkeypatch):
    import uvicorn

    path, _ = manifest(tmp_path)
    supplied = object()
    monkeypatch.setattr(service_cli, "open_service", lambda config: supplied)
    observed = []

    def run(app, **kwargs):
        observed.append(kwargs)
        assert app.state.portfolio_store is supplied
        assert app.state.portfolio_transport_fence.recovery_resolution
        assert app.state.portfolio_pipeline == "runtime"

    monkeypatch.setattr(uvicorn, "run", run)
    result = CliRunner().invoke(service_cli.app, ["serve", str(path), "--port", "8877"])
    assert result.exit_code == 0, result.output
    assert observed == [{"host": "127.0.0.1", "port": 8877, "proxy_headers": False}]


@pytest.mark.parametrize("action", list(service_cli.Step))
def test_steps_route_only_to_declared_worker_and_close_capture(tmp_path, monkeypatch, action):
    from types import SimpleNamespace

    from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger

    path, _ = manifest(tmp_path)
    config = read_service_config(path)
    calls = []

    class Ledger(RecoveryResolutionLedger):
        def __init__(self):
            pass

        def dispatch_once(self, binding, control):
            calls.append("dispatch")
            return {"ok": True}

        def publish_once(self, binding, actor):
            assert actor.actor_id == "recovery-publisher" and actor.role.value == "system"
            calls.append("recovery-publish")
            return {"ok": True}

    def observe(actor):
        assert actor.actor_id == "source-observer" and actor.role.value == "system"
        calls.append("observe")
        return {"ok": True}

    store = SimpleNamespace(
        transport=Ledger(), control=object(), locations=lambda: ("LOC-001",), poll_source=observe
    )

    def opened(config):
        calls.append("open-workflow")
        return store

    monkeypatch.setattr(service_cli, "open_service", opened)

    class Consumer:
        def __init__(self, ledger, binding):
            calls.append("capture-" + binding.purpose)

        def poll_batch(self):
            return {"ok": True}

        def close(self):
            calls.append("close")

    monkeypatch.setattr(service_cli, "DurableBrokerConsumer", Consumer)

    def published():
        calls.append("publish")
        return {"ok": True}

    monkeypatch.setattr(
        service_cli, "SourceOutboxPublisher", lambda store: SimpleNamespace(publish_once=published)
    )

    def planned(root, store):
        calls.append("plan")
        return {"ok": True}

    monkeypatch.setattr(service_cli, "run_runtime_worker", planned)
    result = service_cli.run_step(config, action)
    assert result == {"step": action.value, "result": {"ok": True}}
    expected = [] if action is service_cli.Step.capture_audit else ["open-workflow"]
    expected.append(action.value)
    if action in {service_cli.Step.capture_audit, service_cli.Step.capture_operations}:
        expected.append("close")
    assert calls == expected
