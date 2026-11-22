"""Portable refusal and redaction checks; no Docker, PostgreSQL or broker I/O."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from scripts import bootstrap_ops_service as bootstrap


def local_mounts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, Path]:
    state, private = tmp_path / "state", tmp_path / "private"
    state.mkdir()
    private.mkdir()
    secret = tmp_path / "postgres-bootstrap"
    monkeypatch.setattr(bootstrap.sys, "platform", "linux")
    monkeypatch.setattr(bootstrap.os, "getuid", lambda: 10001, raising=False)
    return state, private, secret


def test_preflight_accepts_exact_hex_secret_with_lf_or_crlf(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, private, secret = local_mounts(tmp_path, monkeypatch)
    for ending in (b"\n", b"\r\n", b""):
        secret.write_bytes(b"a" * 64 + ending)
        assert bootstrap.preflight(state, private, secret, "cluster-1") == "a" * 64
    secret.write_bytes(b"a" * 64 + b" \n")
    with pytest.raises(bootstrap.BootstrapRefusal, match="invalid_bootstrap_secret"):
        bootstrap.preflight(state, private, secret, "cluster-1")


def test_preflight_refuses_retained_state_before_any_provisioning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, private, secret = local_mounts(tmp_path, monkeypatch)
    secret.write_bytes(b"b" * 64 + b"\n")
    (state / "existing-evidence").write_text("retained")
    with pytest.raises(bootstrap.BootstrapRefusal, match="fresh_empty_mounts_required"):
        bootstrap.preflight(state, private, secret, "cluster-1")
    assert (state / "existing-evidence").read_text() == "retained"
    (state / "existing-evidence").unlink()
    (private / "prior-failure.json").write_text("retained")
    with pytest.raises(bootstrap.BootstrapRefusal, match="fresh_empty_mounts_required"):
        bootstrap.preflight(state, private, secret, "cluster-1")


def test_main_redacts_driver_failure_and_retains_private_failure_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    state, private, secret = local_mounts(tmp_path, monkeypatch)
    password = "c" * 64
    secret.write_bytes((password + "\n").encode())
    monkeypatch.setattr(bootstrap, "STATE", state)
    monkeypatch.setattr(bootstrap, "PRIVATE", private)
    monkeypatch.setattr(bootstrap, "ADMIN_SECRET", secret)
    monkeypatch.setattr(
        bootstrap,
        "provision",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("dsn password=" + password)),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "bootstrap_ops_service.py",
            "--expected-cluster-id",
            "cluster-1",
            "--expected-pgdata",
            "/var/lib/postgresql/data/cluster-v1",
        ],
    )
    assert bootstrap.main() == 1
    output = capsys.readouterr()
    receipt = (private / "bootstrap-failure.json").read_text(encoding="utf-8")
    assert password not in output.err + output.out + receipt
    assert json.loads(receipt) == {
        "contract": "nexus.ops-bootstrap-failure.v1",
        "error_type": "RuntimeError",
        "stage": "provision",
        "state_retained": True,
        "status": "failed",
    }
    assert not any(state.iterdir())
    with pytest.raises(bootstrap.BootstrapRefusal, match="fresh_empty_mounts_required"):
        bootstrap.preflight(state, private, secret, "cluster-1")


def test_failure_receipt_keeps_only_structured_kafka_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    state, private, secret = local_mounts(tmp_path, monkeypatch)
    secret.write_bytes(b"e" * 64 + b"\n")
    monkeypatch.setattr(bootstrap, "STATE", state)
    monkeypatch.setattr(bootstrap, "PRIVATE", private)
    monkeypatch.setattr(bootstrap, "ADMIN_SECRET", secret)
    failure = bootstrap.KafkaBootstrapError(KafkaError(KafkaError.COORDINATOR_NOT_AVAILABLE))
    monkeypatch.setattr(
        bootstrap,
        "provision",
        lambda *args, **kwargs: (_ for _ in ()).throw(failure),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "bootstrap_ops_service.py",
            "--expected-cluster-id",
            "cluster-1",
            "--expected-pgdata",
            "/var/lib/postgresql/data/cluster-v1",
        ],
    )
    assert bootstrap.main() == 1
    receipt = json.loads((private / "bootstrap-failure.json").read_text())
    assert receipt["kafka_error_code"] == 15
    assert receipt["kafka_error_name"] == "COORDINATOR_NOT_AVAILABLE"
    assert receipt["stage"] == "provision"
    assert "e" * 64 not in json.dumps(receipt) + capsys.readouterr().err


def test_private_atomic_writer_does_not_replace_existing_receipt(tmp_path: Path) -> None:
    path = tmp_path / "receipt.json"
    bootstrap._atomic_json(path, {"status": "first"})
    with pytest.raises(FileExistsError):
        bootstrap._atomic_json(path, {"status": "second"})
    assert json.loads(path.read_text(encoding="utf-8")) == {"status": "first"}
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600


def test_role_passwords_are_retained_before_first_database_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, private, _ = local_mounts(tmp_path, monkeypatch)
    monkeypatch.setattr(bootstrap, "_database_preflight", lambda *args: None)
    monkeypatch.setattr(bootstrap, "_broker_preflight", lambda *args: object())
    observed: list[str] = []

    def stop_before_roles(admin: str, application: str, migration: str) -> None:
        assert admin == "d" * 64
        assert (private / "application-password.txt").read_text().strip() == application
        assert (private / "migration-password.txt").read_text().strip() == migration
        assert application != migration
        observed.append("credentials_retained")
        raise bootstrap.BootstrapRefusal("injected_role_failure")

    monkeypatch.setattr(bootstrap, "_create_roles", stop_before_roles)
    progress: dict[str, str] = {}
    with pytest.raises(bootstrap.BootstrapRefusal, match="injected_role_failure"):
        bootstrap.provision(
            state,
            private,
            "d" * 64,
            "cluster-1",
            expected_data_dir="/var/lib/postgresql/data/cluster-v1",
            progress=progress,
        )
    assert observed == ["credentials_retained"]
    assert progress["stage"] == "roles"


def test_zero_origin_refuses_existing_group_progress_before_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    committed: list[Any] = []

    class Partition:
        def __init__(self, topic: str, partition: int, offset: int = 0):
            self.topic, self.partition, self.offset, self.error = topic, partition, offset, None

    class Consumer:
        def __init__(self, options: dict[str, Any]):
            assert options["enable.auto.commit"] is False

        def get_watermark_offsets(self, part: Partition, timeout: int) -> tuple[int, int]:
            return (0, 0)

        def committed(self, parts: list[Partition], timeout: int) -> list[Partition]:
            return [Partition(part.topic, part.partition, 1) for part in parts]

        def commit(self, **kwargs: Any) -> None:
            committed.append(kwargs)

        def close(self) -> None:
            pass

    module = SimpleNamespace(Consumer=Consumer, TopicPartition=Partition)
    real_import = bootstrap.importlib.import_module
    monkeypatch.setattr(
        bootstrap.importlib,
        "import_module",
        lambda name: module if name == "confluent_kafka" else real_import(name),
    )
    monkeypatch.setattr(bootstrap, "inspect_topic", lambda config: {"cluster_id": "cluster-1"})
    config = bootstrap.RuntimeBrokerConfig(
        bootstrap.BROKER, bootstrap.SOURCE_TOPIC, "nexus-runtime-ops", ("LOC-001",)
    )
    with pytest.raises(bootstrap.BootstrapRefusal, match="consumer_group_already_initialized"):
        bootstrap._commit_zero(config, "cluster-1")
    assert committed == []


@pytest.mark.parametrize(
    "mode",
    ["lookup_transient", "lookup_permanent", "commit_transient"],
    ids=["cold-coordinator", "permanent-error", "cold-commit"],
)
def test_zero_origin_retries_only_transient_group_operations(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    calls = {"lookup": 0, "commit": 0, "watermark": 0}

    class Partition:
        def __init__(self, topic: str, partition: int, offset: int = 0):
            self.topic, self.partition, self.offset, self.error = topic, partition, offset, None

    class Consumer:
        def __init__(self, options: dict[str, Any]):
            assert options["enable.auto.commit"] is False

        def get_watermark_offsets(self, part: Partition, timeout: int) -> tuple[int, int]:
            calls["watermark"] += 1
            return (0, 0)

        def committed(self, parts: list[Partition], timeout: float) -> list[Partition]:
            calls["lookup"] += 1
            if calls["lookup"] == 1 and mode.startswith("lookup_"):
                code = (
                    KafkaError.COORDINATOR_NOT_AVAILABLE
                    if mode == "lookup_transient"
                    else KafkaError.INVALID_GROUP_ID
                )
                raise KafkaException(KafkaError(code))
            return [Partition(part.topic, part.partition, -1001) for part in parts]

        def commit(self, **kwargs: Any) -> list[Partition]:
            calls["commit"] += 1
            if mode == "commit_transient" and calls["commit"] == 1:
                raise KafkaException(KafkaError(KafkaError.REQUEST_TIMED_OUT))
            return kwargs["offsets"]

        def close(self) -> None:
            pass

    module = SimpleNamespace(Consumer=Consumer, TopicPartition=Partition)
    real_import = bootstrap.importlib.import_module
    monkeypatch.setattr(
        bootstrap.importlib,
        "import_module",
        lambda name: module if name == "confluent_kafka" else real_import(name),
    )
    monkeypatch.setattr(bootstrap, "inspect_topic", lambda config: {"cluster_id": "cluster-1"})
    monkeypatch.setattr(bootstrap.time, "sleep", lambda seconds: None)
    config = bootstrap.RuntimeBrokerConfig(
        bootstrap.BROKER, bootstrap.SOURCE_TOPIC, "nexus-runtime-ops", ("LOC-001",)
    )
    if mode != "lookup_permanent":
        bootstrap._commit_zero(config, "cluster-1")
        assert calls == {
            "lookup": 2,
            "commit": 2 if mode == "commit_transient" else 1,
            "watermark": 4,
        }
    else:
        with pytest.raises(bootstrap.KafkaBootstrapError) as failure:
            bootstrap._commit_zero(config, "cluster-1")
        assert failure.value.code == KafkaError.INVALID_GROUP_ID
        assert failure.value.name == "INVALID_GROUP_ID"
        assert calls == {"lookup": 1, "commit": 0, "watermark": 2}
