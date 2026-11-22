"""Retained Redpanda/PostgreSQL stop-start and reconnect gate for R2D-B2c."""

# mypy: disable-error-code="attr-defined"

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg import sql

from nexus.ingestion.r2d_b2_contracts import R2DB2AVerificationReceipt, load_r2d_b2a_config
from nexus.ingestion.r2d_b2_service import R2DB2ARuntime, R2DB2AService
from nexus.ingestion.r2d_contracts import canonical_json_sha256
from nexus.ingestion.r2d_service import physical_record_from_message
from nexus.modeling.r3_artifacts import (
    ScientificArtifactRef,
    canonical_json_bytes,
    write_immutable_json_manifest,
)
from nexus.orchestration.r2d_b2b_contracts import R2DB2BVerificationReceipt, load_r2d_b2b_config
from nexus.orchestration.r2d_b2c_contracts import (
    DEFAULT_R2D_B2C_CONFIG_PATH,
    PROJECT_ROOT,
    R2D_B2C_EVIDENCE_DATASETS,
    R2D_B2C_PASS_CHECK_NAMES,
    R2DB2CArtifactRef,
    R2DB2CBrokerProbe,
    R2DB2CConfig,
    R2DB2CDagsterRun,
    R2DB2CDagsterSnapshot,
    R2DB2CEvidenceDataset,
    R2DB2CLifecycleEvidence,
    R2DB2CPreRestartCheckpoint,
    R2DB2CTopic,
    R2DB2CTopicSnapshot,
    R2DB2CVerificationReceipt,
    load_canonical_b2c_model,
    load_r2d_b2c_config,
    r2d_b2c_code_sha256,
    r2d_b2c_config_sha256,
)
from nexus.r2d_b2a import load_r2d_b2a_receipt
from nexus.r2d_b2b import load_r2d_b2b_receipt, read_r2d_b2b_persistent_state
from nexus.storage import file_sha256


class R2DB2CGovernanceError(ValueError):
    """Fail-closed B2c service, lifecycle, or evidence error."""


@dataclass(frozen=True, slots=True)
class R2DB2CVerificationResult:
    receipt: R2DB2CVerificationReceipt
    receipt_artifact: ScientificArtifactRef

    @property
    def passed(self) -> bool:
        return self.receipt.status == "PASS"


def _write_canonical_once(path: Path, payload: dict[str, object]) -> None:
    if path.exists() or path.is_symlink():
        raise R2DB2CGovernanceError(f"B2c output must not already exist: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(payload) + b"\n")


def _safe_fresh_root(path: Path, project_root: Path) -> Path:
    output = path.resolve()
    root = project_root.resolve()
    if not output.is_relative_to(root):
        raise R2DB2CGovernanceError("B2c artifact root must stay inside the project")
    if output.exists() or output.is_symlink():
        raise R2DB2CGovernanceError("B2c artifact root must be fresh")
    output.mkdir(parents=True)
    return output


def _runtime() -> dict[str, object]:
    return {
        "operating_system": platform.system(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "github_runner_os_attested": os.getenv("RUNNER_OS") == "Linux",
    }


def _artifact_ref(root: Path, artifact: ScientificArtifactRef) -> R2DB2CArtifactRef:
    return R2DB2CArtifactRef(
        dataset=cast(R2DB2CEvidenceDataset, artifact.dataset),
        relative_path=artifact.path.resolve().relative_to(root.resolve()).as_posix(),
        content_sha256=artifact.content_sha256,
        schema_sha256=artifact.schema_sha256,
        file_sha256=artifact.file_sha256,
        row_count=1,
    )


def _canonical_payload(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2DB2CGovernanceError(f"B2c cannot read governed JSON: {path}") from exc
    if not isinstance(payload, dict) or raw != canonical_json_bytes(payload) + b"\n":
        raise R2DB2CGovernanceError(f"B2c governed JSON is not a canonical object: {path}")
    return payload


def _upstream_payload(
    receipt_path: Path,
    receipt: R2DB2AVerificationReceipt | R2DB2BVerificationReceipt,
    dataset: str,
) -> dict[str, object]:
    reference = next((item for item in receipt.evidence_refs if item.dataset == dataset), None)
    if reference is None:
        raise R2DB2CGovernanceError(f"B2c upstream evidence is missing: {dataset}")
    root = receipt_path.resolve().parents[3]
    path = (root / reference.relative_path).resolve()
    if not path.is_relative_to(root) or file_sha256(path) != reference.file_sha256:
        raise R2DB2CGovernanceError(f"B2c upstream evidence identity changed: {dataset}")
    return _canonical_payload(path)


def _topic_snapshot(service: R2DB2AService, topic: R2DB2CTopic, phase: str) -> R2DB2CTopicSnapshot:
    lows, highs = service.topic_watermarks(topic)
    records = service.consume_range(
        topic=topic,
        group_id=f"nexus-r2d-b2c-{phase}-{topic}",
        starts=lows,
        ends=highs,
    )
    rows = [item.model_dump(mode="json") for item in records]
    return R2DB2CTopicSnapshot(
        contract_version="r2d.b2c.topic-snapshot.v1",
        topic=topic,
        low_watermarks=cast(tuple[int, int], lows),
        high_watermarks=cast(tuple[int, int], highs),
        records=records,
        records_sha256=canonical_json_sha256(rows),
    )


def _await_retained_topics(
    *,
    service: R2DB2AService,
    config: R2DB2CConfig,
    expected: tuple[R2DB2CTopicSnapshot, ...],
) -> tuple[R2DB2CTopicSnapshot, ...]:
    """Wait for bounded post-health broker recovery, then fail with exact differences."""

    deadline = time.monotonic() + config.broker.poll_timeout_seconds
    while True:
        observed = tuple(
            _topic_snapshot(service, topic, "after") for topic in config.broker.retained_topics
        )
        if observed == expected:
            return observed
        if time.monotonic() >= deadline:
            observed_rows = [item.model_dump(mode="json") for item in observed]
            differences = [
                {
                    "topic": actual.topic,
                    "pre_low": list(wanted.low_watermarks),
                    "post_low": list(actual.low_watermarks),
                    "pre_high": list(wanted.high_watermarks),
                    "post_high": list(actual.high_watermarks),
                    "pre_records_sha256": wanted.records_sha256,
                    "post_records_sha256": actual.records_sha256,
                }
                for wanted, actual in zip(expected, observed, strict=True)
                if wanted != actual
            ]
            expected_rows = [item.model_dump(mode="json") for item in expected]
            raise R2DB2CGovernanceError(
                "B2c Redpanda retained state did not converge after restart: "
                f"pre={canonical_json_sha256(expected_rows)}, "
                f"post={canonical_json_sha256(observed_rows)}, "
                f"differences={json.dumps(differences, sort_keys=True)}"
            )
        time.sleep(1.0)


def _validate_b2a_live_state(
    *,
    receipt_path: Path,
    receipt: R2DB2AVerificationReceipt,
    snapshots: tuple[R2DB2CTopicSnapshot, ...],
) -> None:
    topology = _upstream_payload(receipt_path, receipt, "r2d.b2a.topic_topology")
    expected_offsets = cast(dict[str, list[int]], topology.get("final_end_offsets"))
    if {item.topic: list(item.high_watermarks) for item in snapshots} != expected_offsets:
        raise R2DB2CGovernanceError("B2c live broker watermarks differ from the B2a receipt")
    records = {item.transport_identity: item for snapshot in snapshots for item in snapshot.records}
    for dataset in (
        "r2d.b2a.source_ack",
        "r2d.b2a.dead_letter_ack",
        "r2d.b2a.redrive_ack",
        "r2d.b2a.replacement_ack",
    ):
        payload = _upstream_payload(receipt_path, receipt, dataset)
        for acknowledgement in cast(list[dict[str, object]], payload.get("acknowledgements", [])):
            identity = (
                cast(str, acknowledgement["topic"]),
                cast(int, acknowledgement["partition"]),
                cast(int, acknowledgement["offset"]),
            )
            record = records.get(identity)
            if record is None:
                raise R2DB2CGovernanceError("B2c retained broker record is missing")
            value_sha256 = hashlib.sha256(record.value_bytes or b"").hexdigest()
            if (
                record.key_hex != acknowledgement["key_hex"]
                or value_sha256 != acknowledgement["value_sha256"]
                or record.broker_timestamp_ms != acknowledgement["broker_timestamp_ms"]
            ):
                raise R2DB2CGovernanceError("B2c retained broker bytes differ from B2a acks")


def _postgres_counts(postgres_dsn: str) -> dict[str, int]:
    config = load_r2d_b2b_config()
    counts: dict[str, int] = {}
    with psycopg.connect(postgres_dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_is_in_recovery()")
        recovery = cursor.fetchone()
        if recovery is None or bool(recovery[0]):
            raise R2DB2CGovernanceError("B2c PostgreSQL is unavailable or unexpectedly a replica")
        for table in config.postgres.required_tables:
            cursor.execute(
                sql.SQL("SELECT count(*) FROM {}.{}").format(
                    sql.Identifier(config.postgres.schema_name), sql.Identifier(table)
                )
            )
            row = cursor.fetchone()
            if row is None:
                raise R2DB2CGovernanceError(f"B2c PostgreSQL table is unreadable: {table}")
            counts[table] = int(row[0])
    return counts


def _dagster_snapshot(
    *, postgres_dsn: str, dagster_home: Path, receipt: R2DB2BVerificationReceipt
) -> R2DB2CDagsterSnapshot:
    environment = dict(os.environ)
    environment.update(
        {
            "DAGSTER_HOME": str(dagster_home.resolve()),
            "NEXUS_R2D_B2B_POSTGRES_DSN": postgres_dsn,
        }
    )
    observed = read_r2d_b2b_persistent_state(
        environment=environment, backfill_id=receipt.backfill_id
    )
    raw_runs = cast(list[dict[str, object]], observed["runs"])
    runs = tuple(
        R2DB2CDagsterRun(
            run_id=cast(str, row["run_id"]),
            job_name=cast(Any, row["job_name"]),
            partition_key=cast(str, row["partition_key"]),
            status=cast(Any, row["status"]),
            event_count=cast(int, row["event_count"]),
            asset_materialization_events=cast(Any, row["asset_materialization_events"]),
            run_success_events=cast(Any, row["run_success_events"]),
        )
        for row in raw_runs
    )
    payload: dict[str, object] = {
        "contract_version": "r2d.b2c.dagster-snapshot.v1",
        "backfill_id": observed["backfill_id"],
        "backfill_status": observed["backfill_status"],
        "partition_names": observed["partition_names"],
        "runs": [item.model_dump(mode="json") for item in runs],
    }
    snapshot = R2DB2CDagsterSnapshot.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )
    if (
        snapshot.backfill_id != receipt.backfill_id
        or snapshot.partition_names != receipt.partition_keys
        or {item.run_id for item in snapshot.runs} != set(receipt.dagster_run_ids)
    ):
        raise R2DB2CGovernanceError("B2c live Dagster state differs from the B2b receipt")
    return snapshot


def capture_r2d_b2c_pre_restart(
    *,
    output_path: Path,
    bootstrap_servers: str,
    postgres_dsn: str,
    dagster_home: Path,
    b2a_receipt_path: Path,
    b2b_receipt_path: Path,
    config_path: Path = DEFAULT_R2D_B2C_CONFIG_PATH,
) -> R2DB2CPreRestartCheckpoint:
    """Capture the exact live state that must survive the service stop/start boundary."""

    config = load_r2d_b2c_config(config_path)
    b2a = load_r2d_b2a_receipt(b2a_receipt_path)
    b2b = load_r2d_b2b_receipt(b2b_receipt_path)
    if (
        b2a.config_sha256 != config.inherited.b2a_config_sha256
        or b2b.config_sha256 != config.inherited.b2b_config_sha256
    ):
        raise R2DB2CGovernanceError("B2c upstream receipts differ from inherited configs")
    service = R2DB2AService(
        config=load_r2d_b2a_config(),
        runtime=R2DB2ARuntime(bootstrap_servers=bootstrap_servers),
    )
    snapshots = tuple(
        _topic_snapshot(service, topic, "before") for topic in config.broker.retained_topics
    )
    _validate_b2a_live_state(receipt_path=b2a_receipt_path, receipt=b2a, snapshots=snapshots)
    dagster = _dagster_snapshot(postgres_dsn=postgres_dsn, dagster_home=dagster_home, receipt=b2b)
    counts = _postgres_counts(postgres_dsn)
    topic_rows = [item.model_dump(mode="json") for item in snapshots]
    payload: dict[str, object] = {
        "contract_version": "r2d.b2c.pre-restart-checkpoint.v1",
        "b2a_receipt_content_sha256": b2a.content_sha256,
        "b2a_receipt_file_sha256": file_sha256(b2a_receipt_path),
        "b2b_receipt_content_sha256": b2b.content_sha256,
        "b2b_receipt_file_sha256": file_sha256(b2b_receipt_path),
        "topics": topic_rows,
        "broker_snapshot_sha256": canonical_json_sha256(topic_rows),
        "dagster": dagster.model_dump(mode="json"),
        "postgres_table_counts": counts,
        "postgres_table_counts_sha256": canonical_json_sha256(counts),
    }
    checkpoint = R2DB2CPreRestartCheckpoint.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )
    _write_canonical_once(output_path, checkpoint.model_dump(mode="json"))
    return checkpoint


def _run_command(command: list[str], project_root: Path) -> tuple[str, float]:
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    elapsed = time.perf_counter() - started
    if completed.returncode != 0:
        raise R2DB2CGovernanceError(
            f"B2c command failed ({completed.returncode}): {' '.join(command)}: "
            f"{completed.stderr.strip()}"
        )
    return completed.stdout.strip(), elapsed


def _compose_prefix(config: R2DB2CConfig, project_name: str, project_root: Path) -> list[str]:
    compose_file = (project_root / config.services.compose_file).resolve()
    if not compose_file.is_relative_to(project_root.resolve()) or not compose_file.is_file():
        raise R2DB2CGovernanceError("B2c Compose file is missing or escapes the project")
    return [
        "docker",
        "compose",
        "--file",
        config.services.compose_file,
        "--project-name",
        project_name,
        "--profile",
        "ops",
    ]


def _container_id(prefix: list[str], service: str, project_root: Path) -> str:
    stdout, _ = _run_command([*prefix, "ps", "--all", "--quiet", service], project_root)
    ids = [item for item in stdout.splitlines() if item]
    if len(ids) != 1 or re.fullmatch(r"[a-f0-9]{64}", ids[0]) is None:
        raise R2DB2CGovernanceError(f"B2c expected one full container ID for {service}")
    return ids[0]


def _inspect_container(container_id: str, project_root: Path) -> dict[str, object]:
    stdout, _ = _run_command(["docker", "inspect", container_id], project_root)
    payload = json.loads(stdout)
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise R2DB2CGovernanceError("B2c Docker container inspection is malformed")
    return cast(dict[str, object], payload[0])


def _mount_for(inspect: dict[str, object], destination: str) -> str:
    mounts = cast(list[dict[str, object]], inspect.get("Mounts", []))
    matches = [item for item in mounts if item.get("Destination") == destination]
    if len(matches) != 1 or matches[0].get("Type") != "volume":
        raise R2DB2CGovernanceError("B2c service is not attached to its exact named volume")
    return cast(str, matches[0]["Name"])


def _health(inspect: dict[str, object]) -> str:
    state = cast(dict[str, object], inspect.get("State", {}))
    health = cast(dict[str, object], state.get("Health", {}))
    return cast(str, health.get("Status", ""))


def _volume_evidence(name: str, project_name: str, project_root: Path) -> dict[str, object]:
    stdout, _ = _run_command(["docker", "volume", "inspect", name], project_root)
    payload = json.loads(stdout)
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise R2DB2CGovernanceError("B2c Docker volume inspection is malformed")
    volume = cast(dict[str, object], payload[0])
    labels = cast(dict[str, str], volume.get("Labels", {}))
    if labels.get("com.docker.compose.project") != project_name:
        raise R2DB2CGovernanceError("B2c named volume belongs to another Compose project")
    return {
        "name": name,
        "driver": volume.get("Driver"),
        "compose_project": project_name,
        "compose_volume": labels.get("com.docker.compose.volume"),
        "created_at": volume.get("CreatedAt"),
    }


def cycle_r2d_b2c_services(
    *,
    output_path: Path,
    compose_project_name: str,
    config_path: Path = DEFAULT_R2D_B2C_CONFIG_PATH,
    project_root: Path = PROJECT_ROOT,
) -> R2DB2CLifecycleEvidence:
    """Stop and start both retained-volume services, recording canonical Docker evidence."""

    if re.fullmatch(r"nexus-r2d-b2c-[A-Za-z0-9_.-]+", compose_project_name) is None:
        raise R2DB2CGovernanceError("B2c requires an isolated nexus-r2d-b2c-* Compose project")
    config = load_r2d_b2c_config(config_path)
    prefix = _compose_prefix(config, compose_project_name, project_root)
    specs = (
        (config.services.redpanda_service, config.services.redpanda_volume_destination),
        (config.services.postgres_service, config.services.postgres_volume_destination),
    )
    before: dict[str, tuple[str, dict[str, object], str]] = {}
    for service, destination in specs:
        container_id = _container_id(prefix, service, project_root)
        inspect = _inspect_container(container_id, project_root)
        before[service] = (container_id, inspect, _mount_for(inspect, destination))
    volume_names = tuple(before[service][2] for service, _ in specs)
    volumes_before = tuple(
        _volume_evidence(name, compose_project_name, project_root) for name in volume_names
    )
    stop_command = [
        *prefix,
        "stop",
        "--timeout",
        str(config.services.stop_timeout_seconds),
        config.services.redpanda_service,
        config.services.postgres_service,
    ]
    _, stop_elapsed = _run_command(stop_command, project_root)
    stopped: dict[str, tuple[str, dict[str, object]]] = {}
    for service, _ in specs:
        container_id = _container_id(prefix, service, project_root)
        stopped[service] = (container_id, _inspect_container(container_id, project_root))
    volumes_stopped = tuple(
        _volume_evidence(name, compose_project_name, project_root) for name in volume_names
    )
    start_command = [
        *prefix,
        "start",
        "--wait",
        "--wait-timeout",
        str(config.services.start_wait_timeout_seconds),
        config.services.redpanda_service,
        config.services.postgres_service,
    ]
    _, start_elapsed = _run_command(start_command, project_root)
    after: dict[str, tuple[str, dict[str, object], str]] = {}
    for service, destination in specs:
        container_id = _container_id(prefix, service, project_root)
        inspect = _inspect_container(container_id, project_root)
        after[service] = (container_id, inspect, _mount_for(inspect, destination))
    volumes_after = tuple(
        _volume_evidence(name, compose_project_name, project_root) for name in volume_names
    )
    lifecycles: list[dict[str, object]] = []
    for service, destination in specs:
        before_id, before_inspect, before_volume = before[service]
        stopped_id, stopped_inspect = stopped[service]
        after_id, after_inspect, after_volume = after[service]
        before_state = cast(dict[str, object], before_inspect["State"])
        stopped_state = cast(dict[str, object], stopped_inspect["State"])
        after_state = cast(dict[str, object], after_inspect["State"])
        lifecycles.append(
            {
                "service": service,
                "container_id_before": before_id,
                "container_id_stopped": stopped_id,
                "container_id_after": after_id,
                "image_id_before": before_inspect["Image"],
                "image_id_after": after_inspect["Image"],
                "volume_name_before": before_volume,
                "volume_name_stopped": _mount_for(stopped_inspect, destination),
                "volume_name_after": after_volume,
                "volume_destination": destination,
                "before_started_at": before_state["StartedAt"],
                "stopped_finished_at": stopped_state["FinishedAt"],
                "after_started_at": after_state["StartedAt"],
                "before_status": before_state["Status"],
                "stopped_status": stopped_state["Status"],
                "after_status": after_state["Status"],
                "before_health": _health(before_inspect),
                "after_health": _health(after_inspect),
                "stopped_exit_code": stopped_state["ExitCode"],
            }
        )
    payload: dict[str, object] = {
        "contract_version": "r2d.b2c.service-lifecycle.v1",
        "compose_project_name": compose_project_name,
        "compose_file": config.services.compose_file,
        "stop_command": stop_command,
        "start_command": start_command,
        "stop_elapsed_seconds": round(stop_elapsed, 6),
        "start_elapsed_seconds": round(start_elapsed, 6),
        "services": lifecycles,
        "volumes_before": list(volumes_before),
        "volumes_stopped": list(volumes_stopped),
        "volumes_after": list(volumes_after),
    }
    evidence = R2DB2CLifecycleEvidence.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )
    _write_canonical_once(output_path, evidence.model_dump(mode="json"))
    return evidence


def _probe_broker(config: R2DB2CConfig, bootstrap_servers: str) -> R2DB2CBrokerProbe:
    topic = config.broker.reconnect_probe_topic
    admin = AdminClient(
        {
            "bootstrap.servers": bootstrap_servers,
            "broker.address.family": "v4",
            "client.id": "nexus-r2d-b2c-probe-admin",
        }
    )
    future = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=config.broker.reconnect_probe_partition_count,
                replication_factor=config.broker.reconnect_probe_replication_factor,
            )
        ],
        operation_timeout=30,
    )[topic]
    try:
        future.result(timeout=30)
    except KafkaException as exc:
        error = exc.args[0] if exc.args else None
        if isinstance(error, KafkaError) and error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
            raise R2DB2CGovernanceError("B2c reconnect probe topic must be fresh") from exc
        raise R2DB2CGovernanceError("B2c reconnect probe topic creation failed") from exc
    key = b"nexus-r2d-b2c-reconnect"
    value = canonical_json_bytes(
        {
            "contract_version": "r2d.b2c.reconnect-message.v1",
            "probe_id": "retained-service-reconnect-001",
        }
    )
    timestamp_ms = 1781524800400
    delivered: list[tuple[int, int, int]] = []
    errors: list[str] = []

    def callback(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            errors.append(str(error))
            return
        _, observed_timestamp = message.timestamp()
        partition = message.partition()
        offset = message.offset()
        if partition is None or offset is None:
            errors.append("B2c reconnect acknowledgement lacks transport identity")
            return
        delivered.append((partition, offset, observed_timestamp))

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "broker.address.family": "v4",
            "client.id": "nexus-r2d-b2c-probe-producer",
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    producer.produce(
        topic=topic,
        partition=0,
        key=key,
        value=value,
        timestamp=timestamp_ms,
        on_delivery=callback,
    )
    remaining = producer.flush(30)
    if remaining or errors or len(delivered) != 1:
        raise R2DB2CGovernanceError("B2c reconnect probe was not acknowledged exactly once")
    partition, offset, observed_timestamp = delivered[0]
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "broker.address.family": "v4",
            "group.id": config.broker.reconnect_probe_consumer_group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "error",
            "isolation.level": "read_committed",
        }
    )
    try:
        consumer.assign([TopicPartition(topic, 0, offset)])
        deadline = time.monotonic() + config.broker.poll_timeout_seconds
        message: Message | None = None
        while time.monotonic() < deadline:
            candidate = consumer.poll(1.0)
            if candidate is None:
                continue
            if candidate.error() is not None:
                raise R2DB2CGovernanceError(f"B2c reconnect probe poll failed: {candidate.error()}")
            message = candidate
            break
        if message is None:
            raise R2DB2CGovernanceError("B2c reconnect probe was not readable after restart")
        record = physical_record_from_message(message)
        consumer.commit(offsets=[TopicPartition(topic, 0, offset + 1)], asynchronous=False)
        committed = consumer.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
    finally:
        consumer.close()
    payload: dict[str, object] = {
        "contract_version": "r2d.b2c.broker-reconnect-probe.v1",
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "committed_offset": committed,
        "key_hex": key.hex(),
        "value_hex": value.hex(),
        "value_sha256": hashlib.sha256(value).hexdigest(),
        "broker_timestamp_ms": observed_timestamp,
        "consumed_record": record.model_dump(mode="json"),
    }
    return R2DB2CBrokerProbe.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def _copy_upstream_bundle(receipt_path: Path, output: Path, name: str) -> Path:
    source_root = receipt_path.resolve().parents[3]
    if any(path.is_symlink() for path in source_root.rglob("*")):
        raise R2DB2CGovernanceError("B2c upstream bundle may not contain symlinks")
    destination = output / "upstream" / name
    shutil.copytree(source_root, destination)
    copied = destination / receipt_path.resolve().relative_to(source_root)
    if not copied.is_file() or file_sha256(copied) != file_sha256(receipt_path):
        raise R2DB2CGovernanceError("B2c upstream receipt copy changed bytes")
    return copied


def _evidence_artifacts(
    *,
    output: Path,
    b2a: R2DB2AVerificationReceipt,
    b2b: R2DB2BVerificationReceipt,
    copied_b2a: Path,
    copied_b2b: Path,
    pre: R2DB2CPreRestartCheckpoint,
    lifecycle: R2DB2CLifecycleEvidence,
    redpanda_payload: dict[str, object],
    probe: R2DB2CBrokerProbe,
    postgres_payload: dict[str, object],
) -> tuple[list[ScientificArtifactRef], ScientificArtifactRef, ScientificArtifactRef]:
    evidence = [
        write_immutable_json_manifest(
            output,
            dataset="r2d.b2c.inherited_b2a",
            manifest={
                "contract_version": "r2d.b2c.inherited-b2a.v1",
                "receipt_content_sha256": b2a.content_sha256,
                "receipt_file_sha256": file_sha256(copied_b2a),
                "receipt_relative_path": copied_b2a.relative_to(output).as_posix(),
                "config_sha256": b2a.config_sha256,
                "code_sha256": b2a.code_sha256,
                "artifact_set_sha256": b2a.artifact_set_sha256,
            },
            schema_identity={"contract": "r2d.b2c.inherited-b2a.v1"},
        ),
        write_immutable_json_manifest(
            output,
            dataset="r2d.b2c.inherited_b2b",
            manifest={
                "contract_version": "r2d.b2c.inherited-b2b.v1",
                "receipt_content_sha256": b2b.content_sha256,
                "receipt_file_sha256": file_sha256(copied_b2b),
                "receipt_relative_path": copied_b2b.relative_to(output).as_posix(),
                "config_sha256": b2b.config_sha256,
                "code_sha256": b2b.code_sha256,
                "artifact_set_sha256": b2b.artifact_set_sha256,
            },
            schema_identity={"contract": "r2d.b2c.inherited-b2b.v1"},
        ),
    ]
    for dataset, manifest in (
        ("r2d.b2c.pre_restart_checkpoint", pre.model_dump(mode="json")),
        ("r2d.b2c.service_lifecycle", lifecycle.model_dump(mode="json")),
        ("r2d.b2c.redpanda_retained_readback", redpanda_payload),
        ("r2d.b2c.redpanda_reconnect_probe", probe.model_dump(mode="json")),
        ("r2d.b2c.postgres_dagster_readback", postgres_payload),
    ):
        evidence.append(
            write_immutable_json_manifest(
                output,
                dataset=dataset,
                manifest=manifest,
                schema_identity={"contract": manifest["contract_version"]},
            )
        )
    complete_payload: dict[str, object] = {
        "contract_version": "r2d.b2c.complete-checkpoint.v1",
        "b2a_receipt_content_sha256": b2a.content_sha256,
        "b2b_receipt_content_sha256": b2b.content_sha256,
        "pre_restart_checkpoint_sha256": pre.content_sha256,
        "lifecycle_sha256": lifecycle.content_sha256,
        "redpanda_readback_sha256": evidence[4].content_sha256,
        "reconnect_probe_sha256": probe.content_sha256,
        "postgres_readback_sha256": evidence[6].content_sha256,
        "retained_multiservice_restart_status": "PASS",
        "complete_checkpoint_status": "PASS",
    }
    complete = write_immutable_json_manifest(
        output,
        dataset="r2d.b2c.complete_checkpoint",
        manifest=complete_payload,
        schema_identity={"contract": complete_payload["contract_version"]},
    )
    evidence.append(complete)
    config = load_r2d_b2c_config()
    scope_payload: dict[str, object] = {
        "contract_version": "r2d.b2c.scope.v1",
        **config.scope.model_dump(mode="json"),
        "exactly_once_claim": False,
        "high_availability_claim": False,
        "backup_restore_claim": False,
        "multi_node_claim": False,
        "production_scale_claim": False,
    }
    scope = write_immutable_json_manifest(
        output,
        dataset="r2d.b2c.scope",
        manifest=scope_payload,
        schema_identity={"contract": scope_payload["contract_version"]},
    )
    evidence.append(scope)
    return evidence, complete, scope


def verify_r2d_b2c(
    *,
    artifact_root: Path,
    bootstrap_servers: str,
    postgres_dsn: str,
    dagster_home: Path,
    b2a_receipt_path: Path,
    b2b_receipt_path: Path,
    pre_restart_checkpoint_path: Path,
    lifecycle_evidence_path: Path,
    config_path: Path = DEFAULT_R2D_B2C_CONFIG_PATH,
    project_root: Path = PROJECT_ROOT,
) -> R2DB2CVerificationResult:
    """Verify retained state and reconnect I/O after the bounded service restart."""

    config = load_r2d_b2c_config(config_path)
    output = _safe_fresh_root(artifact_root, project_root)
    pre = cast(
        R2DB2CPreRestartCheckpoint,
        load_canonical_b2c_model(pre_restart_checkpoint_path, R2DB2CPreRestartCheckpoint),
    )
    lifecycle = cast(
        R2DB2CLifecycleEvidence,
        load_canonical_b2c_model(lifecycle_evidence_path, R2DB2CLifecycleEvidence),
    )
    b2a = load_r2d_b2a_receipt(b2a_receipt_path)
    b2b = load_r2d_b2b_receipt(b2b_receipt_path)
    if (
        pre.b2a_receipt_content_sha256 != b2a.content_sha256
        or pre.b2a_receipt_file_sha256 != file_sha256(b2a_receipt_path)
        or pre.b2b_receipt_content_sha256 != b2b.content_sha256
        or pre.b2b_receipt_file_sha256 != file_sha256(b2b_receipt_path)
    ):
        raise R2DB2CGovernanceError("B2c pre-restart checkpoint is detached from upstream receipts")
    redpanda_image = _upstream_payload(b2a_receipt_path, b2a, "r2d.b2a.service_image")
    postgres_image = _upstream_payload(b2b_receipt_path, b2b, "r2d.b2b.service_image")
    lifecycle_images = {item.service: item.image_id_after for item in lifecycle.services}
    if lifecycle_images != {
        "redpanda": redpanda_image["observed_image_id"],
        "postgres": postgres_image["observed_image_id"],
    }:
        raise R2DB2CGovernanceError("B2c lifecycle service images differ from upstream evidence")
    service = R2DB2AService(
        config=load_r2d_b2a_config(),
        runtime=R2DB2ARuntime(bootstrap_servers=bootstrap_servers),
    )
    service.require_exact_topic_configs()
    post_topics = _await_retained_topics(
        service=service,
        config=config,
        expected=pre.topics,
    )
    post_topic_rows = [item.model_dump(mode="json") for item in post_topics]
    redpanda_payload: dict[str, object] = {
        "contract_version": "r2d.b2c.redpanda-retained-readback.v1",
        "pre_restart_broker_snapshot_sha256": pre.broker_snapshot_sha256,
        "post_restart_topics": post_topic_rows,
        "post_restart_broker_snapshot_sha256": canonical_json_sha256(post_topic_rows),
        "exact_match": True,
        "retained_record_count": sum(len(item.records) for item in post_topics),
    }
    probe = _probe_broker(config, bootstrap_servers)
    post_dagster = _dagster_snapshot(
        postgres_dsn=postgres_dsn, dagster_home=dagster_home, receipt=b2b
    )
    post_counts = _postgres_counts(postgres_dsn)
    if post_dagster != pre.dagster or post_counts != pre.postgres_table_counts:
        raise R2DB2CGovernanceError("B2c PostgreSQL/Dagster state changed across stop/start")
    postgres_payload: dict[str, object] = {
        "contract_version": "r2d.b2c.postgres-dagster-readback.v1",
        "pre_restart_dagster_sha256": pre.dagster.content_sha256,
        "post_restart_dagster": post_dagster.model_dump(mode="json"),
        "pre_restart_table_counts_sha256": pre.postgres_table_counts_sha256,
        "post_restart_table_counts": post_counts,
        "post_restart_table_counts_sha256": canonical_json_sha256(post_counts),
        "exact_match": True,
    }
    copied_b2a = _copy_upstream_bundle(b2a_receipt_path, output, "b2a")
    copied_b2b = _copy_upstream_bundle(b2b_receipt_path, output, "b2b")
    evidence, complete_artifact, scope_artifact = _evidence_artifacts(
        output=output,
        b2a=b2a,
        b2b=b2b,
        copied_b2a=copied_b2a,
        copied_b2b=copied_b2b,
        pre=pre,
        lifecycle=lifecycle,
        redpanda_payload=redpanda_payload,
        probe=probe,
        postgres_payload=postgres_payload,
    )
    refs = tuple(_artifact_ref(output, item) for item in evidence)
    if tuple(item.dataset for item in refs) != R2D_B2C_EVIDENCE_DATASETS:
        raise R2DB2CGovernanceError("B2c implementation emitted the wrong evidence topology")
    artifact_set_sha256 = canonical_json_sha256([item.model_dump(mode="json") for item in refs])
    evidence_hashes = {
        "inherited_b2a_authority": b2a.content_sha256,
        "inherited_b2b_authority": b2b.content_sha256,
        "pre_restart_checkpoint_bound": pre.content_sha256,
        "actual_multiservice_stop": lifecycle.content_sha256,
        "named_volumes_retained": lifecycle.content_sha256,
        "exact_container_restart": lifecycle.content_sha256,
        "redpanda_zero_loss_readback": evidence[4].content_sha256,
        "fresh_broker_reconnect_io": probe.content_sha256,
        "postgres_dagster_state_retained": evidence[6].content_sha256,
        "complete_checkpoint": complete_artifact.content_sha256,
        "immutable_artifact_set": artifact_set_sha256,
        "honest_scope": scope_artifact.content_sha256,
    }
    receipt_payload: dict[str, object] = {
        "contract_version": "r2d.b2c.verification-receipt.v1",
        "release": "R2D-B2c",
        "status": "PASS",
        "checks": [
            {"name": name, "passed": True, "evidence_sha256": evidence_hashes[name]}
            for name in R2D_B2C_PASS_CHECK_NAMES
        ],
        "evidence_refs": [item.model_dump(mode="json") for item in refs],
        "config_sha256": r2d_b2c_config_sha256(config),
        "code_sha256": r2d_b2c_code_sha256(project_root),
        "b2a_receipt_content_sha256": b2a.content_sha256,
        "b2a_receipt_file_sha256": file_sha256(copied_b2a),
        "b2b_receipt_content_sha256": b2b.content_sha256,
        "b2b_receipt_file_sha256": file_sha256(copied_b2b),
        "pre_restart_checkpoint_sha256": pre.content_sha256,
        "lifecycle_sha256": lifecycle.content_sha256,
        "redpanda_readback_sha256": evidence[4].content_sha256,
        "reconnect_probe_sha256": probe.content_sha256,
        "postgres_readback_sha256": evidence[6].content_sha256,
        "complete_checkpoint_sha256": complete_artifact.content_sha256,
        "scope_sha256": scope_artifact.content_sha256,
        "artifact_set_sha256": artifact_set_sha256,
        "compose_project_name": lifecycle.compose_project_name,
        "backfill_id": b2b.backfill_id,
        "dagster_run_ids": list(b2b.dagster_run_ids),
        "retained_topic_count": len(post_topics),
        "retained_record_count": redpanda_payload["retained_record_count"],
        "runtime": _runtime(),
        **config.scope.model_dump(mode="json"),
        "exactly_once_claim": False,
        "high_availability_claim": False,
        "backup_restore_claim": False,
        "multi_node_claim": False,
        "production_scale_claim": False,
    }
    receipt = R2DB2CVerificationReceipt.model_validate(
        {**receipt_payload, "content_sha256": canonical_json_sha256(receipt_payload)}
    )
    receipt_artifact = write_immutable_json_manifest(
        output,
        dataset="r2d.b2c.verification_receipt",
        manifest=receipt.model_dump(mode="json"),
        schema_identity={"contract": receipt.contract_version},
    )
    load_r2d_b2c_receipt(receipt_artifact.path)
    return R2DB2CVerificationResult(receipt=receipt, receipt_artifact=receipt_artifact)


def load_r2d_b2c_receipt(path: Path) -> R2DB2CVerificationReceipt:
    """Strictly reload the complete B2c package without live services or Docker."""

    try:
        raw = path.read_bytes()
        parsed = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2DB2CGovernanceError(f"cannot load B2c receipt: {path}") from exc
    if raw != canonical_json_bytes(parsed) + b"\n":
        raise R2DB2CGovernanceError("B2c receipt is not canonical JSON")
    receipt = R2DB2CVerificationReceipt.model_validate(parsed)
    root = path.resolve().parents[3]
    payloads: dict[str, dict[str, object]] = {}
    for reference in receipt.evidence_refs:
        artifact = (root / reference.relative_path).resolve()
        if not artifact.is_relative_to(root) or not artifact.is_file() or artifact.is_symlink():
            raise R2DB2CGovernanceError("B2c evidence artifact is missing or escapes its root")
        payload = _canonical_payload(artifact)
        if (
            canonical_json_sha256(payload) != reference.content_sha256
            or file_sha256(artifact) != reference.file_sha256
            or canonical_json_sha256({"contract": payload.get("contract_version")})
            != reference.schema_sha256
        ):
            raise R2DB2CGovernanceError("B2c evidence artifact identity mismatch")
        payloads[reference.dataset] = payload
    config = load_r2d_b2c_config()
    if receipt.config_sha256 != r2d_b2c_config_sha256(config):
        raise R2DB2CGovernanceError("B2c receipt differs from the current config")
    if receipt.code_sha256 != r2d_b2c_code_sha256(PROJECT_ROOT):
        raise R2DB2CGovernanceError("B2c receipt differs from the current code")
    inherited_a = payloads["r2d.b2c.inherited_b2a"]
    inherited_b = payloads["r2d.b2c.inherited_b2b"]
    copied_a = (root / cast(str, inherited_a["receipt_relative_path"])).resolve()
    copied_b = (root / cast(str, inherited_b["receipt_relative_path"])).resolve()
    if not copied_a.is_relative_to(root) or not copied_b.is_relative_to(root):
        raise R2DB2CGovernanceError("B2c copied upstream receipt escapes its package")
    b2a = load_r2d_b2a_receipt(copied_a)
    b2b = load_r2d_b2b_receipt(copied_b)
    if (
        b2a.content_sha256 != receipt.b2a_receipt_content_sha256
        or file_sha256(copied_a) != receipt.b2a_receipt_file_sha256
        or b2b.content_sha256 != receipt.b2b_receipt_content_sha256
        or file_sha256(copied_b) != receipt.b2b_receipt_file_sha256
    ):
        raise R2DB2CGovernanceError("B2c copied upstream authority is detached")
    if b2b.backfill_id != receipt.backfill_id or b2b.dagster_run_ids != receipt.dagster_run_ids:
        raise R2DB2CGovernanceError("B2c copied Dagster authority is detached")
    expected_inherited_a = {
        "contract_version": "r2d.b2c.inherited-b2a.v1",
        "receipt_content_sha256": b2a.content_sha256,
        "receipt_file_sha256": file_sha256(copied_a),
        "receipt_relative_path": copied_a.relative_to(root).as_posix(),
        "config_sha256": b2a.config_sha256,
        "code_sha256": b2a.code_sha256,
        "artifact_set_sha256": b2a.artifact_set_sha256,
    }
    expected_inherited_b = {
        "contract_version": "r2d.b2c.inherited-b2b.v1",
        "receipt_content_sha256": b2b.content_sha256,
        "receipt_file_sha256": file_sha256(copied_b),
        "receipt_relative_path": copied_b.relative_to(root).as_posix(),
        "config_sha256": b2b.config_sha256,
        "code_sha256": b2b.code_sha256,
        "artifact_set_sha256": b2b.artifact_set_sha256,
    }
    if inherited_a != expected_inherited_a or inherited_b != expected_inherited_b:
        raise R2DB2CGovernanceError("B2c inherited evidence is not exact")
    pre = R2DB2CPreRestartCheckpoint.model_validate(payloads["r2d.b2c.pre_restart_checkpoint"])
    lifecycle = R2DB2CLifecycleEvidence.model_validate(payloads["r2d.b2c.service_lifecycle"])
    probe = R2DB2CBrokerProbe.model_validate(payloads["r2d.b2c.redpanda_reconnect_probe"])
    if (
        pre.content_sha256 != receipt.pre_restart_checkpoint_sha256
        or lifecycle.content_sha256 != receipt.lifecycle_sha256
        or probe.content_sha256 != receipt.reconnect_probe_sha256
    ):
        raise R2DB2CGovernanceError("B2c strict evidence is detached from the receipt")
    if (
        pre.b2a_receipt_content_sha256 != b2a.content_sha256
        or pre.b2a_receipt_file_sha256 != file_sha256(copied_a)
        or pre.b2b_receipt_content_sha256 != b2b.content_sha256
        or pre.b2b_receipt_file_sha256 != file_sha256(copied_b)
    ):
        raise R2DB2CGovernanceError("B2c pre-restart checkpoint is detached from upstream")
    if lifecycle.compose_project_name != receipt.compose_project_name:
        raise R2DB2CGovernanceError("B2c lifecycle belongs to another Compose project")
    redpanda_image = _upstream_payload(copied_a, b2a, "r2d.b2a.service_image")
    postgres_image = _upstream_payload(copied_b, b2b, "r2d.b2b.service_image")
    lifecycle_images = {item.service: item.image_id_after for item in lifecycle.services}
    if lifecycle_images != {
        "redpanda": redpanda_image["observed_image_id"],
        "postgres": postgres_image["observed_image_id"],
    }:
        raise R2DB2CGovernanceError("B2c lifecycle images differ from copied upstream evidence")
    by_dataset: dict[str, R2DB2CArtifactRef] = {
        item.dataset: item for item in receipt.evidence_refs
    }
    redpanda = payloads["r2d.b2c.redpanda_retained_readback"]
    post_topics = tuple(
        R2DB2CTopicSnapshot.model_validate(item)
        for item in cast(list[object], redpanda.get("post_restart_topics"))
    )
    post_topic_rows = [item.model_dump(mode="json") for item in post_topics]
    expected_redpanda = {
        "contract_version": "r2d.b2c.redpanda-retained-readback.v1",
        "pre_restart_broker_snapshot_sha256": pre.broker_snapshot_sha256,
        "post_restart_topics": post_topic_rows,
        "post_restart_broker_snapshot_sha256": canonical_json_sha256(post_topic_rows),
        "exact_match": True,
        "retained_record_count": sum(len(item.records) for item in post_topics),
    }
    if (
        post_topics != pre.topics
        or expected_redpanda["post_restart_broker_snapshot_sha256"] != pre.broker_snapshot_sha256
        or len(post_topics) != receipt.retained_topic_count
        or expected_redpanda["retained_record_count"] != receipt.retained_record_count
        or redpanda != expected_redpanda
    ):
        raise R2DB2CGovernanceError("B2c retained broker readback is not exact")
    postgres = payloads["r2d.b2c.postgres_dagster_readback"]
    post_dagster = R2DB2CDagsterSnapshot.model_validate(postgres.get("post_restart_dagster"))
    post_counts = cast(dict[str, int], postgres.get("post_restart_table_counts"))
    expected_postgres = {
        "contract_version": "r2d.b2c.postgres-dagster-readback.v1",
        "pre_restart_dagster_sha256": pre.dagster.content_sha256,
        "post_restart_dagster": post_dagster.model_dump(mode="json"),
        "pre_restart_table_counts_sha256": pre.postgres_table_counts_sha256,
        "post_restart_table_counts": post_counts,
        "post_restart_table_counts_sha256": canonical_json_sha256(post_counts),
        "exact_match": True,
    }
    if (
        post_dagster != pre.dagster
        or post_counts != pre.postgres_table_counts
        or expected_postgres["post_restart_table_counts_sha256"] != pre.postgres_table_counts_sha256
        or postgres != expected_postgres
    ):
        raise R2DB2CGovernanceError("B2c retained PostgreSQL/Dagster readback is not exact")
    complete = payloads["r2d.b2c.complete_checkpoint"]
    expected_complete = {
        "contract_version": "r2d.b2c.complete-checkpoint.v1",
        "b2a_receipt_content_sha256": b2a.content_sha256,
        "b2b_receipt_content_sha256": b2b.content_sha256,
        "pre_restart_checkpoint_sha256": pre.content_sha256,
        "lifecycle_sha256": lifecycle.content_sha256,
        "redpanda_readback_sha256": by_dataset["r2d.b2c.redpanda_retained_readback"].content_sha256,
        "reconnect_probe_sha256": probe.content_sha256,
        "postgres_readback_sha256": by_dataset["r2d.b2c.postgres_dagster_readback"].content_sha256,
        "retained_multiservice_restart_status": "PASS",
        "complete_checkpoint_status": "PASS",
    }
    if complete != expected_complete:
        raise R2DB2CGovernanceError("B2c complete checkpoint is detached or incomplete")
    scope = payloads["r2d.b2c.scope"]
    expected_scope = {
        "contract_version": "r2d.b2c.scope.v1",
        **config.scope.model_dump(mode="json"),
        "exactly_once_claim": False,
        "high_availability_claim": False,
        "backup_restore_claim": False,
        "multi_node_claim": False,
        "production_scale_claim": False,
    }
    if scope != expected_scope:
        raise R2DB2CGovernanceError("B2c scope evidence overclaims the bounded gate")
    expected_top = {
        "r2d.b2c.redpanda_retained_readback": receipt.redpanda_readback_sha256,
        "r2d.b2c.postgres_dagster_readback": receipt.postgres_readback_sha256,
        "r2d.b2c.complete_checkpoint": receipt.complete_checkpoint_sha256,
        "r2d.b2c.scope": receipt.scope_sha256,
    }
    if any(by_dataset[dataset].content_sha256 != value for dataset, value in expected_top.items()):
        raise R2DB2CGovernanceError("B2c receipt is detached from governed evidence")
    return receipt


def dump_r2d_b2c_result(result: R2DB2CVerificationResult) -> str:
    return json.dumps(
        {
            "checks": [item.model_dump(mode="json") for item in result.receipt.checks],
            "full_g2_status": result.receipt.full_g2_status,
            "full_r2d_b_status": result.receipt.full_r2d_b_status,
            "receipt": str(result.receipt_artifact.path),
            "retained_multiservice_restart_status": (
                result.receipt.retained_multiservice_restart_status
            ),
            "status": result.receipt.status,
        },
        sort_keys=True,
    )
