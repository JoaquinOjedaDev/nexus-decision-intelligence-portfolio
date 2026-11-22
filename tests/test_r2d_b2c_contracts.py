from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from pydantic import ValidationError
from typer.main import get_command
from typer.testing import CliRunner

from nexus.cli import app
from nexus.ingestion.r2d_contracts import R2DPhysicalRecord, canonical_json_sha256
from nexus.modeling.r3_artifacts import canonical_json_bytes
from nexus.orchestration.r2d_b2b_contracts import R2D_B2B_PARTITION_KEYS
from nexus.orchestration.r2d_b2c_contracts import (
    R2D_B2C_EVIDENCE_DATASETS,
    R2D_B2C_PASS_CHECK_NAMES,
    R2DB2CArtifactRef,
    R2DB2CBrokerProbe,
    R2DB2CDagsterSnapshot,
    R2DB2CLifecycleEvidence,
    R2DB2CPreRestartCheckpoint,
    R2DB2CTopicSnapshot,
    R2DB2CVerificationReceipt,
    load_canonical_b2c_model,
    load_r2d_b2c_config,
    r2d_b2c_config_sha256,
)

pytestmark = pytest.mark.r2d
runner = CliRunner()


def _record(topic: str, partition: int, offset: int) -> R2DPhysicalRecord:
    return R2DPhysicalRecord.from_bytes(
        topic=topic,
        partition=partition,
        offset=offset,
        key=f"key-{partition}-{offset}".encode(),
        headers=(),
        broker_timestamp_ms=1_781_524_800_000 + offset,
        broker_timestamp_type="create_time",
        value=f"value-{partition}-{offset}".encode(),
    )


def _topic(topic: str, highs: tuple[int, int]) -> R2DB2CTopicSnapshot:
    records = tuple(
        _record(topic, partition, offset)
        for partition, high in enumerate(highs)
        for offset in range(high)
    )
    rows = [item.model_dump(mode="json") for item in records]
    return R2DB2CTopicSnapshot(
        contract_version="r2d.b2c.topic-snapshot.v1",
        topic=topic,  # type: ignore[arg-type]
        low_watermarks=(0, 0),
        high_watermarks=highs,
        records=records,
        records_sha256=canonical_json_sha256(rows),
    )


def _dagster() -> R2DB2CDagsterSnapshot:
    runs = [
        {
            "run_id": f"run-{index}",
            "job_name": "r2d_b2b_backfill_job",
            "partition_key": key,
            "status": "SUCCESS",
            "event_count": 8,
            "asset_materialization_events": 2,
            "run_success_events": 1,
        }
        for index, key in enumerate(R2D_B2B_PARTITION_KEYS)
    ]
    payload = {
        "contract_version": "r2d.b2c.dagster-snapshot.v1",
        "backfill_id": "backfill-1",
        "backfill_status": "COMPLETED",
        "partition_names": list(R2D_B2B_PARTITION_KEYS),
        "runs": runs,
    }
    return R2DB2CDagsterSnapshot.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def _pre() -> R2DB2CPreRestartCheckpoint:
    topics = (
        _topic("inventory.adjusted.v1", (1, 2)),
        _topic("inventory.adjusted.dlq.v1", (0, 1)),
        _topic("inventory.adjusted.redrive.v1", (0, 1)),
    )
    topic_rows = [item.model_dump(mode="json") for item in topics]
    counts = {"runs": 2, "event_logs": 16}
    payload = {
        "contract_version": "r2d.b2c.pre-restart-checkpoint.v1",
        "b2a_receipt_content_sha256": "1" * 64,
        "b2a_receipt_file_sha256": "2" * 64,
        "b2b_receipt_content_sha256": "3" * 64,
        "b2b_receipt_file_sha256": "4" * 64,
        "topics": topic_rows,
        "broker_snapshot_sha256": canonical_json_sha256(topic_rows),
        "dagster": _dagster().model_dump(mode="json"),
        "postgres_table_counts": counts,
        "postgres_table_counts_sha256": canonical_json_sha256(counts),
    }
    return R2DB2CPreRestartCheckpoint.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def _lifecycle() -> R2DB2CLifecycleEvidence:
    project = "nexus-r2d-b2c-test-1"
    services = []
    for service, image, volume, destination in (
        ("redpanda", "a", "nexus-redpanda", "/var/lib/redpanda/data"),
        ("postgres", "b", "nexus-postgres", "/var/lib/postgresql/data"),
    ):
        services.append(
            {
                "service": service,
                "container_id_before": image * 64,
                "container_id_stopped": image * 64,
                "container_id_after": image * 64,
                "image_id_before": f"sha256:{image * 64}",
                "image_id_after": f"sha256:{image * 64}",
                "volume_name_before": f"{project}_{volume}",
                "volume_name_stopped": f"{project}_{volume}",
                "volume_name_after": f"{project}_{volume}",
                "volume_destination": destination,
                "before_started_at": "2026-06-15T12:00:00Z",
                "stopped_finished_at": "2026-06-15T12:01:00Z",
                "after_started_at": "2026-06-15T12:02:00Z",
                "before_status": "running",
                "stopped_status": "exited",
                "after_status": "running",
                "before_health": "healthy",
                "after_health": "healthy",
                "stopped_exit_code": 0,
            }
        )
    volumes = [
        {
            "name": f"{project}_{name}",
            "driver": "local",
            "compose_project": project,
            "compose_volume": name,
            "created_at": "2026-06-15T11:59:00Z",
        }
        for name in ("nexus-redpanda", "nexus-postgres")
    ]
    payload = {
        "contract_version": "r2d.b2c.service-lifecycle.v1",
        "compose_project_name": project,
        "compose_file": "infra/compose.yaml",
        "stop_command": ["docker", "compose", "stop"],
        "start_command": ["docker", "compose", "start", "--wait"],
        "stop_elapsed_seconds": 1.0,
        "start_elapsed_seconds": 2.0,
        "services": services,
        "volumes_before": volumes,
        "volumes_stopped": volumes,
        "volumes_after": volumes,
    }
    return R2DB2CLifecycleEvidence.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def test_b2c_config_is_frozen_and_inherits_both_service_gates() -> None:
    config = load_r2d_b2c_config()
    assert config.release == "R2D-B2c"
    assert config.broker.retained_topics == (
        "inventory.adjusted.v1",
        "inventory.adjusted.dlq.v1",
        "inventory.adjusted.redrive.v1",
    )
    assert config.scope.retained_multiservice_restart_status == "PASS"
    assert config.scope.operational_alert_delivery_status == "NOT_RUN"
    assert config.scope.full_g2_status == "OPEN"
    assert len(r2d_b2c_config_sha256(config)) == 64
    with pytest.raises(ValidationError):
        config.release = "changed"  # type: ignore[misc]


def test_pre_checkpoint_rejects_missing_or_reordered_broker_records() -> None:
    checkpoint = _pre()
    assert checkpoint.dagster.backfill_status == "COMPLETED"
    tampered = checkpoint.model_dump(mode="json")
    tampered["topics"][0]["records"] = tampered["topics"][0]["records"][:-1]
    tampered["topics"][0]["records_sha256"] = canonical_json_sha256(
        tampered["topics"][0]["records"]
    )
    with pytest.raises(ValidationError, match="incomplete"):
        R2DB2CPreRestartCheckpoint.model_validate(tampered)


def test_lifecycle_requires_same_containers_images_and_volumes() -> None:
    lifecycle = _lifecycle()
    assert tuple(item.service for item in lifecycle.services) == ("redpanda", "postgres")
    changed = lifecycle.model_dump(mode="json")
    changed["services"][0]["container_id_after"] = "c" * 64
    changed["content_sha256"] = canonical_json_sha256(
        {key: value for key, value in changed.items() if key != "content_sha256"}
    )
    with pytest.raises(ValidationError, match="container identity"):
        R2DB2CLifecycleEvidence.model_validate(changed)


@pytest.mark.r2d_service
def test_cycle_records_actual_stopped_state_and_stopped_volumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nexus.r2d_b2c import cycle_r2d_b2c_services

    project = "nexus-r2d-b2c-test-1"
    ids = {"redpanda": "a" * 64, "postgres": "b" * 64}
    destinations = {
        "redpanda": "/var/lib/redpanda/data",
        "postgres": "/var/lib/postgresql/data",
    }
    volumes = {
        "redpanda": f"{project}_nexus-redpanda",
        "postgres": f"{project}_nexus-postgres",
    }
    phase = {"value": "before"}
    commands: list[list[str]] = []

    def fake_run(command: list[str], project_root: Path) -> tuple[str, float]:
        commands.append(command)
        if command[:2] == ["docker", "inspect"]:
            service = next(name for name, identity in ids.items() if identity == command[2])
            current = phase["value"]
            status = "exited" if current == "stopped" else "running"
            started = {
                "before": "2026-06-15T12:00:00Z",
                "stopped": "2026-06-15T12:00:00Z",
                "after": "2026-06-15T12:02:00Z",
            }[current]
            payload = [
                {
                    "Image": f"sha256:{ids[service]}",
                    "Mounts": [
                        {
                            "Destination": destinations[service],
                            "Type": "volume",
                            "Name": volumes[service],
                        }
                    ],
                    "State": {
                        "Status": status,
                        "ExitCode": 0,
                        "StartedAt": started,
                        "FinishedAt": "2026-06-15T12:01:00Z",
                        "Health": {"Status": "healthy"},
                    },
                }
            ]
            return json.dumps(payload), 0.01
        if command[:3] == ["docker", "volume", "inspect"]:
            name = command[3]
            compose_volume = next(key for key, value in volumes.items() if value == name)
            payload = [
                {
                    "Name": name,
                    "Driver": "local",
                    "CreatedAt": "2026-06-15T11:59:00Z",
                    "Labels": {
                        "com.docker.compose.project": project,
                        "com.docker.compose.volume": f"nexus-{compose_volume}",
                    },
                }
            ]
            return json.dumps(payload), 0.01
        if "ps" in command:
            assert "--all" in command
            return ids[command[-1]], 0.01
        if "stop" in command:
            phase["value"] = "stopped"
            return "", 1.0
        if "start" in command:
            phase["value"] = "after"
            return "", 2.0
        raise AssertionError(command)

    monkeypatch.setattr("nexus.r2d_b2c._run_command", fake_run)
    result = cycle_r2d_b2c_services(
        output_path=tmp_path / "lifecycle.json",
        compose_project_name=project,
    )
    assert result.volumes_before == result.volumes_stopped == result.volumes_after
    assert all(item.stopped_status == "exited" for item in result.services)
    assert any("stop" in command for command in commands)
    assert any("start" in command for command in commands)


def test_probe_requires_exact_consumed_bytes_and_commit() -> None:
    record = _record("nexus.r2d.b2c.reconnect.v1", 0, 0)
    payload = {
        "contract_version": "r2d.b2c.broker-reconnect-probe.v1",
        "topic": "nexus.r2d.b2c.reconnect.v1",
        "partition": 0,
        "offset": 0,
        "committed_offset": 1,
        "key_hex": record.key_hex,
        "value_hex": record.value_hex,
        "value_sha256": hashlib.sha256(record.value_bytes or b"").hexdigest(),
        "broker_timestamp_ms": record.broker_timestamp_ms,
        "consumed_record": record.model_dump(mode="json"),
    }
    probe = R2DB2CBrokerProbe.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )
    changed = probe.model_dump(mode="json")
    changed["committed_offset"] = 2
    changed["content_sha256"] = canonical_json_sha256(
        {key: value for key, value in changed.items() if key != "content_sha256"}
    )
    with pytest.raises(ValidationError, match="next offset"):
        R2DB2CBrokerProbe.model_validate(changed)


@pytest.mark.r2d_service
def test_post_restart_readback_waits_for_bounded_broker_convergence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nexus.r2d_b2c import _await_retained_topics

    expected = _pre().topics
    calls = {"count": 0}

    def fake_snapshot(*_: object, **__: object) -> R2DB2CTopicSnapshot:
        index = calls["count"] % 3
        attempt = calls["count"] // 3
        calls["count"] += 1
        if attempt == 0 and index == 0:
            return _topic("inventory.adjusted.v1", (0, 2))
        return expected[index]

    monkeypatch.setattr("nexus.r2d_b2c._topic_snapshot", fake_snapshot)
    monkeypatch.setattr("nexus.r2d_b2c.time.sleep", lambda _: None)
    observed = _await_retained_topics(
        service=object(),  # type: ignore[arg-type]
        config=load_r2d_b2c_config(),
        expected=expected,
    )
    assert observed == expected
    assert calls["count"] == 6


def test_canonical_pre_checkpoint_loader_rejects_pretty_json(tmp_path: Path) -> None:
    checkpoint = _pre()
    path = tmp_path / "pre.json"
    path.write_bytes(canonical_json_bytes(checkpoint.model_dump(mode="json")) + b"\n")
    assert isinstance(
        load_canonical_b2c_model(path, R2DB2CPreRestartCheckpoint),
        R2DB2CPreRestartCheckpoint,
    )
    path.write_text(json.dumps(checkpoint.model_dump(mode="json"), indent=2), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical"):
        load_canonical_b2c_model(path, R2DB2CPreRestartCheckpoint)


def test_receipt_requires_exact_checks_evidence_and_nonclaims() -> None:
    refs = tuple(
        R2DB2CArtifactRef(
            dataset=dataset,
            relative_path=f"r2d/b2c/{index}.json",
            content_sha256=hashlib_sha(dataset + "content"),
            schema_sha256=hashlib_sha(dataset + "schema"),
            file_sha256=hashlib_sha(dataset + "file"),
            row_count=1,
        )
        for index, dataset in enumerate(R2D_B2C_EVIDENCE_DATASETS)
    )
    artifact_set = canonical_json_sha256([item.model_dump(mode="json") for item in refs])
    hashes = {name: hashlib_sha(name) for name in R2D_B2C_PASS_CHECK_NAMES}
    fields = {
        "inherited_b2a_authority": "b2a_receipt_content_sha256",
        "inherited_b2b_authority": "b2b_receipt_content_sha256",
        "pre_restart_checkpoint_bound": "pre_restart_checkpoint_sha256",
        "actual_multiservice_stop": "lifecycle_sha256",
        "redpanda_zero_loss_readback": "redpanda_readback_sha256",
        "fresh_broker_reconnect_io": "reconnect_probe_sha256",
        "postgres_dagster_state_retained": "postgres_readback_sha256",
        "complete_checkpoint": "complete_checkpoint_sha256",
        "honest_scope": "scope_sha256",
    }
    hashes["named_volumes_retained"] = hashes["actual_multiservice_stop"]
    hashes["exact_container_restart"] = hashes["actual_multiservice_stop"]
    hashes["immutable_artifact_set"] = artifact_set
    identity_fields = {field: hashes[name] for name, field in fields.items()}
    payload = {
        "contract_version": "r2d.b2c.verification-receipt.v1",
        "release": "R2D-B2c",
        "status": "PASS",
        "checks": [
            {"name": name, "passed": True, "evidence_sha256": hashes[name]}
            for name in R2D_B2C_PASS_CHECK_NAMES
        ],
        "evidence_refs": [item.model_dump(mode="json") for item in refs],
        "config_sha256": "1" * 64,
        "code_sha256": "2" * 64,
        **identity_fields,
        "b2a_receipt_file_sha256": "3" * 64,
        "b2b_receipt_file_sha256": "4" * 64,
        "artifact_set_sha256": artifact_set,
        "compose_project_name": "nexus-r2d-b2c-test-1",
        "backfill_id": "backfill-1",
        "dagster_run_ids": ["run-1", "run-2"],
        "retained_topic_count": 3,
        "retained_record_count": 4,
        "runtime": {
            "operating_system": "Linux",
            "machine": "x86_64",
            "python_version": "3.13.0",
            "github_runner_os_attested": True,
        },
        "inherited_b2a_status": "PASS",
        "inherited_b2b_status": "PASS",
        "retained_multiservice_restart_status": "PASS",
        "operational_alert_delivery_status": "NOT_RUN",
        "high_availability_status": "NOT_RUN",
        "backup_restore_status": "NOT_RUN",
        "full_r2d_b_status": "IN_PROGRESS",
        "full_g2_status": "OPEN",
        "exactly_once_claim": False,
        "high_availability_claim": False,
        "backup_restore_claim": False,
        "multi_node_claim": False,
        "production_scale_claim": False,
    }
    receipt = R2DB2CVerificationReceipt.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )
    assert receipt.retained_multiservice_restart_status == "PASS"
    missing = receipt.model_dump(mode="json")
    missing["checks"] = missing["checks"][:-1]
    missing["content_sha256"] = canonical_json_sha256(
        {key: value for key, value in missing.items() if key != "content_sha256"}
    )
    with pytest.raises(ValidationError, match="checks"):
        R2DB2CVerificationReceipt.model_validate(missing)


def hashlib_sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


@pytest.mark.r2d_service
def test_cli_exposes_all_three_b2c_phases(monkeypatch: pytest.MonkeyPatch) -> None:
    root = get_command(app)
    commands = root.commands["r2d"].commands
    for name in ("capture-service-b2c-before", "cycle-service-b2c", "verify-service-b2c"):
        result = runner.invoke(app, ["r2d", name, "--help"])
        assert result.exit_code == 0
        assert name in commands
    captured: dict[str, object] = {}

    def fake_cycle(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(model_dump=lambda **_: {"status": "PASS"})

    monkeypatch.setattr("nexus.r2d_b2c.cycle_r2d_b2c_services", fake_cycle)
    result = runner.invoke(
        app,
        [
            "r2d",
            "cycle-service-b2c",
            "--compose-project-name",
            "nexus-r2d-b2c-test-1",
            "--output",
            "artifacts/b2c-lifecycle.json",
        ],
    )
    assert result.exit_code == 0
    assert captured["compose_project_name"] == "nexus-r2d-b2c-test-1"
    assert captured["output_path"] == Path("artifacts/b2c-lifecycle.json")


def test_ci_has_isolated_b2c_retained_restart_gate() -> None:
    workflow = yaml.safe_load(Path(".github/workflows/ci.yml").read_text(encoding="utf-8"))
    job = workflow["jobs"]["r2d-service-b2c"]
    assert job["needs"] == "r2d-service-b2b"
    steps = "\n".join(str(step) for step in job["steps"])
    assert "up -d --wait --wait-timeout 120 redpanda postgres" in steps
    assert "verify-service-b2a" in steps
    assert "verify-service-b2b" in steps
    assert "capture-service-b2c-before" in steps
    assert "cycle-service-b2c" in steps
    assert "verify-service-b2c" in steps
    assert "down -v --remove-orphans" in steps
