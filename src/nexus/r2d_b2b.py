"""Persistent Dagster, queued backfill, and daemon-restart gate for R2D-B2b."""

from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import BinaryIO, cast

import dagster as dg
import psutil
from dagster._core.run_coordinator import QueuedRunCoordinator
from dagster._core.storage.tags import BACKFILL_ID_TAG, PARTITION_NAME_TAG

from nexus.ingestion.r2d_contracts import canonical_json_sha256
from nexus.modeling.r3_artifacts import (
    ScientificArtifactRef,
    canonical_json_bytes,
    write_immutable_json_manifest,
)
from nexus.orchestration.r2b_assets import (
    build_r2b_asset_graph,
    partition_storage_key,
    r2_validation_code_sha256,
    r2b_orchestration_code_sha256,
)
from nexus.orchestration.r2b_checkpoint import (
    checkpoint_ref_from_root,
    load_r2b_validation_checkpoint,
)
from nexus.orchestration.r2b_contracts import load_r2b_config, r2b_config_sha256
from nexus.orchestration.r2d_b2b_contracts import (
    DEFAULT_R2D_B2B_CONFIG_PATH,
    DEFAULT_R2D_B2B_DAGSTER_CONFIG_PATH,
    DEFAULT_R2D_B2B_WORKSPACE_PATH,
    PROJECT_ROOT,
    R2D_B2B_EVIDENCE_DATASETS,
    R2D_B2B_PASS_CHECK_NAMES,
    R2DB2BArtifactRef,
    R2DB2BConfig,
    R2DB2BEvidenceDataset,
    R2DB2BServiceImageEvidence,
    R2DB2BVerificationReceipt,
    load_r2d_b2b_config,
    load_r2d_b2b_service_image,
    r2d_b2b_code_sha256,
    r2d_b2b_config_sha256,
)
from nexus.r2 import run_r2_rebuild
from nexus.storage import file_sha256


class R2DB2BGovernanceError(ValueError):
    """Fail-closed B2b service or evidence error."""


@dataclass(frozen=True, slots=True)
class R2DB2BVerificationResult:
    receipt: R2DB2BVerificationReceipt
    receipt_artifact: ScientificArtifactRef

    @property
    def passed(self) -> bool:
        return self.receipt.status == "PASS"


@dataclass(frozen=True, slots=True)
class _ProcessIdentity:
    pid: int
    create_time: float


class _ProcessTreeSampler:
    def __init__(self, interval_seconds: float = 0.05) -> None:
        self._interval = interval_seconds
        self._root = psutil.Process()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._started_at: float | None = None
        self._peak_rss = 0
        self._peak_threads = 0
        self._max_descendants = 0
        self._seen: dict[int, _ProcessIdentity] = {}
        self._errors: set[str] = set()

    def start(self) -> None:
        if self._started_at is not None:
            raise RuntimeError("B2b resource sampler already started")
        self._started_at = time.perf_counter()
        self._sample()
        self._thread = threading.Thread(target=self._run, name="r2d-b2b-resource", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            self._sample()

    def _sample(self) -> None:
        try:
            descendants = self._root.children(recursive=True)
        except (psutil.AccessDenied, psutil.NoSuchProcess) as exc:
            with self._lock:
                self._errors.add(f"process enumeration failed: {type(exc).__name__}: {exc}")
            return
        rss = 0
        native_threads = 0
        identities: list[_ProcessIdentity] = []
        for process in (self._root, *descendants):
            try:
                with process.oneshot():
                    rss += process.memory_info().rss
                    native_threads += process.num_threads()
                    if process.pid != self._root.pid:
                        identities.append(
                            _ProcessIdentity(pid=process.pid, create_time=process.create_time())
                        )
            except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
                continue
        with self._lock:
            self._peak_rss = max(self._peak_rss, rss)
            self._peak_threads = max(self._peak_threads, native_threads)
            self._max_descendants = max(self._max_descendants, len(descendants))
            for identity in identities:
                self._seen[identity.pid] = identity

    def stop_sampling(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._sample()

    def cleanup_seen(self, timeout_seconds: float) -> tuple[int, ...]:
        deadline = time.monotonic() + timeout_seconds
        survivors: list[psutil.Process] = []
        with self._lock:
            identities = tuple(self._seen.values())
        for identity in identities:
            try:
                process = psutil.Process(identity.pid)
                if abs(process.create_time() - identity.create_time) > 0.001:
                    continue
                if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                    survivors.append(process)
            except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
                continue
        for process in survivors:
            with suppress(psutil.AccessDenied, psutil.NoSuchProcess):
                process.terminate()
        remaining = max(0.0, deadline - time.monotonic())
        _, alive = psutil.wait_procs(survivors, timeout=remaining)
        for process in alive:
            with suppress(psutil.AccessDenied, psutil.NoSuchProcess):
                process.kill()
        remaining = max(0.0, deadline - time.monotonic())
        _, alive = psutil.wait_procs(alive, timeout=remaining)
        return tuple(sorted(process.pid for process in alive if process.is_running()))

    def evidence(
        self,
        *,
        output_root: Path,
        budget: Mapping[str, int],
        cleanup_seconds: float,
        survivors: tuple[int, ...],
    ) -> dict[str, object]:
        if self._started_at is None:
            raise RuntimeError("B2b resource sampler was not started")
        artifact_bytes = 0
        artifact_files = 0
        for candidate in output_root.rglob("*"):
            if candidate.is_file():
                artifact_bytes += candidate.stat().st_size
                artifact_files += 1
        with self._lock:
            peak_rss = self._peak_rss
            peak_threads = self._peak_threads
            max_descendants = self._max_descendants
            errors = sorted(self._errors)
        wall_time = time.perf_counter() - self._started_at
        passed = (
            wall_time <= budget["wall_time_seconds"]
            and peak_rss <= budget["peak_rss_mib"] * 1024 * 1024
            and peak_threads <= budget["maximum_native_threads"]
            and artifact_bytes <= budget["artifact_mib"] * 1024 * 1024
            and cleanup_seconds <= budget["shutdown_seconds"]
            and max_descendants <= budget["maximum_descendant_processes"]
            and not survivors
            and not errors
        )
        return {
            "contract_version": "r2d.b2b.resource.v1",
            "measurement_scope": "verifier_process_tree_and_pre_receipt_artifact_root",
            "budget": dict(budget),
            "observed": {
                "wall_time_seconds": round(wall_time, 6),
                "peak_rss_bytes": peak_rss,
                "peak_native_threads": peak_threads,
                "maximum_descendant_processes": max_descendants,
                "artifact_bytes": artifact_bytes,
                "artifact_files": artifact_files,
                "shutdown_seconds": round(cleanup_seconds, 6),
                "surviving_process_ids": list(survivors),
                "measurement_errors": errors,
            },
            "passed": passed,
        }


@dataclass(slots=True)
class _DaemonProcess:
    process: subprocess.Popen[bytes]
    stdout_handle: BinaryIO
    stderr_handle: BinaryIO
    stdout_path: Path
    stderr_path: Path
    started_at: float


def _qualified_name(value: object) -> str:
    cls = type(value)
    return f"{cls.__module__}.{cls.__name__}"


@contextmanager
def _temporary_environment(values: Mapping[str, str]) -> Iterator[None]:
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextmanager
def _persistent_instance(environment: Mapping[str, str]) -> Iterator[dg.DagsterInstance]:
    with _temporary_environment(environment):
        instance = dg.DagsterInstance.get()
        try:
            yield instance
        finally:
            instance.dispose()


def _safe_fresh_root(path: Path) -> Path:
    resolved = path.resolve()
    if path.is_symlink() or (resolved.exists() and any(resolved.iterdir())):
        raise R2DB2BGovernanceError("B2b artifact root must be fresh and cannot be a symlink")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _write_exact(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def _artifact_ref(root: Path, artifact: ScientificArtifactRef) -> R2DB2BArtifactRef:
    return R2DB2BArtifactRef(
        dataset=cast(R2DB2BEvidenceDataset, artifact.dataset),
        relative_path=artifact.path.resolve().relative_to(root.resolve()).as_posix(),
        content_sha256=artifact.content_sha256,
        schema_sha256=artifact.schema_sha256,
        file_sha256=artifact.file_sha256,
        row_count=1,
    )


def _runtime() -> dict[str, object]:
    runner_os = os.getenv("RUNNER_OS")
    return {
        "operating_system": runner_os or platform.system(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "dagster_version": version("dagster"),
        "dagster_postgres_version": version("dagster-postgres"),
        "github_runner_os_attested": os.getenv("GITHUB_ACTIONS") == "true" and bool(runner_os),
    }


def _dagster_environment(
    *,
    dagster_home: Path,
    postgres_dsn: str,
    project_root: Path,
    scenario_root: Path,
) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "DAGSTER_HOME": str(dagster_home.resolve()),
            "NEXUS_R2D_B2B_POSTGRES_DSN": postgres_dsn,
            "NEXUS_R2D_B2B_PROJECT_ROOT": str(project_root.resolve()),
            "NEXUS_R2D_B2B_SCENARIO_ROOT": str(scenario_root.resolve()),
            "NEXUS_R2D_B2B_R2B_CONFIG_PATH": str(
                (project_root / "configs" / "orchestration" / "r2b-backfill.yaml").resolve()
            ),
        }
    )
    return environment


def _prepare_dagster_home(output: Path, project_root: Path) -> Path:
    dagster_home = output / "dagster-home"
    dagster_home.mkdir(parents=True, exist_ok=True)
    template = project_root / DEFAULT_R2D_B2B_DAGSTER_CONFIG_PATH.relative_to(PROJECT_ROOT)
    _write_exact(dagster_home / "dagster.yaml", template.read_bytes())
    return dagster_home


def _prepare_checkpoint(project_root: Path, scenario_root: Path) -> dict[str, object]:
    r2b_config = load_r2b_config(project_root / "configs" / "orchestration" / "r2b-backfill.yaml")
    graph = build_r2b_asset_graph(
        project_root=project_root,
        scenario_root=scenario_root,
        config=r2b_config,
        inject_transient_failure=False,
    )
    result = dg.materialize(
        graph.assets,
        selection=["r2b_source_package", "r2b_validation_checkpoint"],
        raise_on_error=True,
        tags={"nexus/release": "R2D-B2b", "nexus/run-kind": "validation-checkpoint"},
    )
    materializations = [
        event
        for event in result.get_asset_materialization_events()
        if event.asset_key == dg.AssetKey("r2b_validation_checkpoint")
    ]
    if not result.success or len(materializations) != 1:
        raise R2DB2BGovernanceError("B2b inherited validation checkpoint did not materialize once")
    checkpoint = checkpoint_ref_from_root(scenario_root / "shared")
    load_r2b_validation_checkpoint(
        checkpoint,
        expected_validation_code_sha256=r2_validation_code_sha256(project_root),
        expected_source_manifest_sha256=r2b_config.source_contract.source_manifest_sha256,
        expected_source_manifest_file_sha256=r2b_config.source_contract.source_manifest_file_sha256,
    )
    return {
        "checkpoint_sha256": checkpoint.content_sha256,
        "checkpoint_file_sha256": checkpoint.file_sha256,
        "preparation_run_id": result.run_id,
        "materialization_count": len(materializations),
    }


def _postgres_instance_evidence(
    *,
    config: R2DB2BConfig,
    environment: Mapping[str, str],
    postgres_dsn: str,
) -> dict[str, object]:
    import psycopg

    with _persistent_instance(environment) as instance:
        run_class = _qualified_name(instance.run_storage)
        event_class = _qualified_name(instance.event_log_storage)
        schedule_class = _qualified_name(instance.schedule_storage)
        coordinator = instance.run_coordinator
        if not isinstance(coordinator, QueuedRunCoordinator):
            raise R2DB2BGovernanceError("B2b Dagster instance has no persistent run queue")
        queue_config = coordinator.get_run_queue_config()
        required_daemons = sorted(instance.get_required_daemon_types())
    with psycopg.connect(postgres_dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name",
            (config.postgres.schema_name,),
        )
        observed_tables = tuple(row[0] for row in cursor.fetchall())
    required_tables = config.postgres.required_tables
    if (
        run_class != config.postgres.run_storage_class
        or event_class != config.postgres.event_log_storage_class
        or schedule_class != config.postgres.schedule_storage_class
        or queue_config.max_concurrent_runs != config.dagster.maximum_concurrent_runs
        or not set(required_tables).issubset(observed_tables)
        or "QUEUED_RUN_COORDINATOR" not in required_daemons
    ):
        raise R2DB2BGovernanceError("B2b Dagster instance is not the frozen PostgreSQL queue")
    return {
        "contract_version": "r2d.b2b.instance.v1",
        "storage_classes": {
            "run": run_class,
            "event_log": event_class,
            "schedule": schedule_class,
        },
        "postgres_schema": config.postgres.schema_name,
        "required_tables": list(required_tables),
        "observed_tables": list(observed_tables),
        "run_queue": {
            "class": _qualified_name(coordinator),
            "maximum_concurrent_runs": queue_config.max_concurrent_runs,
            "dequeue_interval_seconds": config.dagster.dequeue_interval_seconds,
        },
        "run_launcher_class": _qualified_name(instance.run_launcher),
        "required_daemon_types": required_daemons,
        "dsn_persisted": False,
    }


def _start_daemon(
    *,
    output: Path,
    label: str,
    workspace_path: Path,
    project_root: Path,
    environment: Mapping[str, str],
) -> _DaemonProcess:
    stdout_path = output / "execution" / f"{label}.stdout.log"
    stderr_path = output / "execution" / f"{label}.stderr.log"
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_handle = stdout_path.open("wb")
    stderr_handle = stderr_path.open("wb")
    command = [
        sys.executable,
        "-c",
        "from dagster._daemon.cli import main; main()",
        "run",
        "-w",
        str(workspace_path),
    ]
    process = subprocess.Popen(
        command,
        cwd=project_root,
        env=dict(environment),
        stdout=stdout_handle,
        stderr=stderr_handle,
    )
    return _DaemonProcess(
        process=process,
        stdout_handle=stdout_handle,
        stderr_handle=stderr_handle,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        started_at=time.time(),
    )


def _stop_daemon(daemon: _DaemonProcess, timeout_seconds: float) -> dict[str, object]:
    process_tree: list[psutil.Process] = []
    with suppress(psutil.NoSuchProcess):
        root = psutil.Process(daemon.process.pid)
        process_tree.extend(root.children(recursive=True))
        process_tree.append(root)
    for process in reversed(process_tree):
        with suppress(psutil.AccessDenied, psutil.NoSuchProcess):
            process.terminate()
    _, alive = psutil.wait_procs(process_tree, timeout=timeout_seconds)
    for process in alive:
        with suppress(psutil.AccessDenied, psutil.NoSuchProcess):
            process.kill()
    _, alive = psutil.wait_procs(alive, timeout=2)
    with suppress(subprocess.TimeoutExpired):
        daemon.process.wait(timeout=1)
    daemon.stdout_handle.close()
    daemon.stderr_handle.close()
    return {
        "controller_pid": daemon.process.pid,
        "controller_exit_code": daemon.process.poll(),
        "started_at_epoch_seconds": daemon.started_at,
        "stdout_relative_path": daemon.stdout_path.name,
        "stdout_file_sha256": file_sha256(daemon.stdout_path),
        "stderr_relative_path": daemon.stderr_path.name,
        "stderr_file_sha256": file_sha256(daemon.stderr_path),
        "surviving_epoch_process_ids": sorted(process.pid for process in alive),
    }


def _stop_daemon_controller(
    daemon: _DaemonProcess, timeout_seconds: float
) -> tuple[dict[str, object], tuple[_ProcessIdentity, ...]]:
    """Stop only the daemon controller while allowing its launched run to finish."""

    children: list[_ProcessIdentity] = []
    root_process: psutil.Process | None = None
    with suppress(psutil.NoSuchProcess):
        root_process = psutil.Process(daemon.process.pid)
        for child in root_process.children(recursive=True):
            with suppress(psutil.AccessDenied, psutil.NoSuchProcess):
                children.append(_ProcessIdentity(child.pid, child.create_time()))
    if root_process is not None:
        with suppress(psutil.AccessDenied, psutil.NoSuchProcess):
            root_process.terminate()
        _, alive = psutil.wait_procs([root_process], timeout=timeout_seconds)
        for process in alive:
            with suppress(psutil.AccessDenied, psutil.NoSuchProcess):
                process.kill()
        psutil.wait_procs(alive, timeout=2)
    with suppress(subprocess.TimeoutExpired):
        daemon.process.wait(timeout=1)
    daemon.stdout_handle.close()
    daemon.stderr_handle.close()
    return (
        {
            "controller_pid": daemon.process.pid,
            "controller_exit_code": daemon.process.poll(),
            "started_at_epoch_seconds": daemon.started_at,
            "stdout_relative_path": daemon.stdout_path.name,
            "stdout_file_sha256": file_sha256(daemon.stdout_path),
            "stderr_relative_path": daemon.stderr_path.name,
            "stderr_file_sha256": file_sha256(daemon.stderr_path),
            "controller_stopped_before_first_run_completed": True,
        },
        tuple(children),
    )


def _cleanup_epoch_children(
    identities: tuple[_ProcessIdentity, ...], timeout_seconds: float
) -> tuple[int, ...]:
    processes: list[psutil.Process] = []
    for identity in identities:
        try:
            process = psutil.Process(identity.pid)
            if (
                abs(process.create_time() - identity.create_time) <= 0.001
                and process.is_running()
                and process.status() != psutil.STATUS_ZOMBIE
            ):
                processes.append(process)
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
    for process in reversed(processes):
        with suppress(psutil.AccessDenied, psutil.NoSuchProcess):
            process.terminate()
    _, alive = psutil.wait_procs(processes, timeout=timeout_seconds)
    for process in alive:
        with suppress(psutil.AccessDenied, psutil.NoSuchProcess):
            process.kill()
    _, alive = psutil.wait_procs(alive, timeout=2)
    return tuple(sorted(process.pid for process in alive if process.is_running()))


def _wait_for_daemon_heartbeat(
    *,
    environment: Mapping[str, str],
    started_at: float,
    timeout_seconds: int,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with _persistent_instance(environment) as instance:
            heartbeats = instance.get_daemon_heartbeats()
        queued = heartbeats.get("QUEUED_RUN_COORDINATOR")
        if queued is not None and queued.timestamp >= started_at - 1 and not queued.errors:
            daemon_ids = {heartbeat.daemon_id for heartbeat in heartbeats.values()}
            if len(daemon_ids) == 1:
                return {
                    "contract_version": "r2d.b2b.daemon-epoch.v1",
                    "daemon_id": queued.daemon_id,
                    "heartbeat_types": sorted(heartbeats),
                    "heartbeat_error_counts": {
                        name: len(heartbeat.errors or [])
                        for name, heartbeat in sorted(heartbeats.items())
                    },
                    "queued_run_coordinator_timestamp": queued.timestamp,
                }
        time.sleep(0.1)
    raise R2DB2BGovernanceError("B2b queued-run daemon heartbeat did not become healthy")


def _run_rows(instance: dg.DagsterInstance, backfill_id: str) -> list[dict[str, object]]:
    records = instance.get_run_records(
        filters=dg.RunsFilter(tags={BACKFILL_ID_TAG: backfill_id}), ascending=True
    )
    rows: list[dict[str, object]] = []
    for record in records:
        run = record.dagster_run
        partition_key = run.tags.get(PARTITION_NAME_TAG)
        if partition_key is None:
            raise R2DB2BGovernanceError("B2b backfill run is missing its partition tag")
        event_types = [
            entry.dagster_event.event_type_value
            for entry in instance.all_logs(run.run_id)
            if entry.dagster_event is not None
        ]
        rows.append(
            {
                "run_id": run.run_id,
                "job_name": run.job_name,
                "partition_key": partition_key,
                "status": run.status.value,
                "create_timestamp": record.create_timestamp.isoformat(),
                "start_time": record.start_time,
                "end_time": record.end_time,
                "event_count": len(event_types),
                "asset_materialization_events": event_types.count("ASSET_MATERIALIZATION"),
                "run_success_events": event_types.count("PIPELINE_SUCCESS"),
            }
        )
    return sorted(rows, key=lambda row: cast(str, row["partition_key"]))


def _wait_for_run_state(
    *,
    environment: Mapping[str, str],
    backfill_id: str,
    timeout_seconds: int,
    predicate: Callable[[list[dict[str, object]]], bool],
) -> list[dict[str, object]]:
    deadline = time.monotonic() + timeout_seconds
    last: list[dict[str, object]] = []
    while time.monotonic() < deadline:
        with _persistent_instance(environment) as instance:
            last = _run_rows(instance, backfill_id)
        if predicate(last):
            return last
        if any(row["status"] in {"FAILURE", "CANCELED"} for row in last):
            raise R2DB2BGovernanceError(f"B2b backfill run failed: {last}")
        time.sleep(0.05)
    raise R2DB2BGovernanceError(f"B2b backfill run-state timeout: {last}")


def _submit_backfill(
    *,
    config: R2DB2BConfig,
    output: Path,
    workspace_path: Path,
    project_root: Path,
    environment: Mapping[str, str],
) -> tuple[str, dict[str, object]]:
    with _persistent_instance(environment) as instance:
        before = {backfill.backfill_id for backfill in instance.get_backfills()}
    command = [
        sys.executable,
        "-c",
        "from dagster._cli import main; main()",
        "job",
        "backfill",
        "-w",
        str(workspace_path),
        "-j",
        config.dagster.job_name,
        "--partitions",
        ",".join(config.dagster.partition_keys),
        "--noprompt",
        "--tags",
        json.dumps(
            {"nexus/release": "R2D-B2b", "nexus/operation": "bounded-backfill"},
            separators=(",", ":"),
            sort_keys=True,
        ),
    ]
    completed = subprocess.run(
        command,
        cwd=project_root,
        env=dict(environment),
        check=False,
        capture_output=True,
        text=True,
        timeout=config.dagster.run_timeout_seconds,
    )
    stdout_path = output / "execution" / "backfill-submit.stdout.log"
    stderr_path = output / "execution" / "backfill-submit.stderr.log"
    _write_exact(stdout_path, completed.stdout.encode("utf-8"))
    _write_exact(stderr_path, completed.stderr.encode("utf-8"))
    if completed.returncode != 0:
        raise R2DB2BGovernanceError(
            f"B2b actual backfill submission failed with exit {completed.returncode}"
        )
    with _persistent_instance(environment) as instance:
        after = {backfill.backfill_id: backfill for backfill in instance.get_backfills()}
        new_ids = sorted(set(after) - before)
        if len(new_ids) != 1:
            raise R2DB2BGovernanceError("B2b submission did not create exactly one backfill record")
        backfill = after[new_ids[0]]
        rows = _run_rows(instance, backfill.backfill_id)
    if (
        backfill.status.value != "COMPLETED"
        or tuple(backfill.partition_names or ()) != config.dagster.partition_keys
        or len(rows) != config.dagster.expected_run_count
    ):
        raise R2DB2BGovernanceError("B2b persisted backfill submission differs from contract")
    payload = {
        "contract_version": "r2d.b2b.backfill-submission.v1",
        "backfill_id": backfill.backfill_id,
        "backfill_status": backfill.status.value,
        "job_name": config.dagster.job_name,
        "partition_keys": list(config.dagster.partition_keys),
        "run_ids": sorted(cast(str, row["run_id"]) for row in rows),
        "run_statuses_after_submission": [
            {"partition_key": row["partition_key"], "status": row["status"]} for row in rows
        ],
        "cli_return_code": completed.returncode,
        "stdout_file_sha256": file_sha256(stdout_path),
        "stderr_file_sha256": file_sha256(stderr_path),
    }
    return backfill.backfill_id, payload


def _database_counts(*, config: R2DB2BConfig, postgres_dsn: str) -> dict[str, int]:
    import psycopg
    from psycopg import sql

    counts: dict[str, int] = {}
    with psycopg.connect(postgres_dsn) as connection, connection.cursor() as cursor:
        for table in config.postgres.required_tables:
            cursor.execute(
                sql.SQL("SELECT count(*) FROM {}.{}").format(
                    sql.Identifier(config.postgres.schema_name), sql.Identifier(table)
                )
            )
            row = cursor.fetchone()
            if row is None:
                raise R2DB2BGovernanceError(f"B2b PostgreSQL table is unreadable: {table}")
            counts[table] = int(row[0])
    return counts


def _backfill_storage_payload(
    *,
    config: R2DB2BConfig,
    environment: Mapping[str, str],
    postgres_dsn: str,
    backfill_id: str,
    after_submission: list[dict[str, object]],
    after_daemon_dispatch: list[dict[str, object]],
    after_first_dispatch: list[dict[str, object]],
    while_daemon_stopped: list[dict[str, object]],
    final_rows: list[dict[str, object]],
) -> dict[str, object]:
    with _persistent_instance(environment) as instance:
        backfill = instance.get_backfill(backfill_id)
    if backfill is None or backfill.status.value != "COMPLETED":
        raise R2DB2BGovernanceError("B2b persisted backfill record is absent or incomplete")
    if tuple(backfill.partition_names or ()) != config.dagster.partition_keys:
        raise R2DB2BGovernanceError("B2b persisted backfill partitions changed")
    if len(final_rows) != 2 or any(row["status"] != "SUCCESS" for row in final_rows):
        raise R2DB2BGovernanceError("B2b did not persist exactly two successful partition runs")
    if (
        tuple(cast(str, row["partition_key"]) for row in final_rows)
        != config.dagster.partition_keys
    ):
        raise R2DB2BGovernanceError("B2b successful runs differ from the frozen partitions")
    if any(cast(int, row["asset_materialization_events"]) != 2 for row in final_rows):
        raise R2DB2BGovernanceError("B2b partition run did not materialize exactly two assets")
    if any(cast(int, row["run_success_events"]) != 1 for row in final_rows):
        raise R2DB2BGovernanceError("B2b partition run lacks one persisted success event")
    return {
        "contract_version": "r2d.b2b.run-storage.v1",
        "backfill_id": backfill_id,
        "backfill_status": backfill.status.value,
        "partition_names": list(backfill.partition_names or ()),
        "after_submission": after_submission,
        "after_first_dispatch": after_first_dispatch,
        "after_daemon_dispatch": after_daemon_dispatch,
        "while_daemon_stopped": while_daemon_stopped,
        "final_runs": final_rows,
        "postgres_table_row_counts": _database_counts(config=config, postgres_dsn=postgres_dsn),
    }


def _governed_file_rows(root: Path, paths: list[Path]) -> list[dict[str, object]]:
    resolved_root = root.resolve()
    rows = []
    for path in sorted({item.resolve() for item in paths}, key=lambda item: item.as_posix()):
        if not path.is_relative_to(resolved_root) or not path.is_file() or path.is_symlink():
            raise R2DB2BGovernanceError("B2b governed file is missing or escapes its root")
        rows.append(
            {
                "relative_path": path.relative_to(resolved_root).as_posix(),
                "file_sha256": file_sha256(path),
                "size_bytes": path.stat().st_size,
            }
        )
    return rows


def _summary_projection(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_bytes())
    if not isinstance(payload, dict) or payload.get("status") != "PASS":
        raise R2DB2BGovernanceError("B2b R2A summary is not a PASS object")
    required = (
        "gold",
        "planning_cutoff",
        "quality_counts",
        "semantic_manifest_sha256",
        "source_manifest_file_sha256",
        "source_manifest_sha256",
        "validated",
        "watermarks",
    )
    if any(key not in payload for key in required):
        raise R2DB2BGovernanceError("B2b R2A summary is incomplete")
    return {key: payload[key] for key in required}


def _exact_files(root: Path, pattern: str, expected: int) -> list[Path]:
    files = sorted(
        (path for path in root.glob(pattern) if path.is_file()), key=lambda p: p.as_posix()
    )
    if len(files) != expected:
        raise R2DB2BGovernanceError(
            f"B2b governed topology expected {expected} files for {pattern}, observed {len(files)}"
        )
    return files


def _semantic_parity_payload(
    *,
    config: R2DB2BConfig,
    project_root: Path,
    scenario_root: Path,
    final_rows: list[dict[str, object]],
) -> dict[str, object]:
    r2b_config = load_r2b_config(project_root / config.inherited_r2b.config_path)
    source_dir = scenario_root / "shared" / "source"
    partitions = []
    for row in final_rows:
        partition_key = cast(str, row["partition_key"])
        run_id = cast(str, row["run_id"])
        partition = r2b_config.partition_for_key(partition_key)
        partition_root = scenario_root / "partitions" / partition_storage_key(partition_key)
        execution_path = partition_root / "execution" / f"{run_id}.json"
        execution = json.loads(execution_path.read_bytes())
        if (
            execution.get("status") != "SUCCESS"
            or execution.get("partition_key") != partition_key
            or execution.get("dagster_run_id") != run_id
        ):
            raise R2DB2BGovernanceError("B2b partition execution evidence is inconsistent")
        lineage_paths = _exact_files(
            partition_root / "governed" / "r2b" / "semantic_lineage", "*.json", 1
        )
        backfill_summary_paths = _exact_files(partition_root / "rebuild" / "manifests", "*.json", 1)
        backfill_summary_path = backfill_summary_paths[0]
        lineage = json.loads(lineage_paths[0].read_bytes())
        backfill_projection = _summary_projection(backfill_summary_path)
        if (
            lineage.get("partition", {}).get("key") != partition_key
            or lineage.get("r2a_snapshot", {}).get("semantic_manifest_sha256")
            != backfill_projection["semantic_manifest_sha256"]
            or execution.get("semantic_manifest_sha256") != canonical_json_sha256(lineage)
        ):
            raise R2DB2BGovernanceError("B2b lineage is detached from its persisted run")

        direct = run_r2_rebuild(
            project_root=project_root,
            source_dir=source_dir,
            artifact_dir=scenario_root / "direct" / partition_storage_key(partition_key),
            planning_cutoff=partition.planning_cutoff,
        )
        direct_projection = _summary_projection(direct.summary_path)
        if direct_projection != backfill_projection:
            raise R2DB2BGovernanceError("B2b backfill differs from direct governed R2A semantics")

        backfill_files = [
            execution_path,
            lineage_paths[0],
            backfill_summary_path,
            *_exact_files(partition_root / "rebuild" / "exports", "**/*.parquet", 5),
            *_exact_files(partition_root / "rebuild" / "warehouse", "*.duckdb", 1),
        ]
        direct_files = [
            direct.summary_path,
            direct.database_path,
            *(artifact.path for artifact in direct.gold_artifacts),
        ]
        partitions.append(
            {
                "partition_key": partition_key,
                "partition_name": partition.name,
                "planning_cutoff": partition.planning_cutoff.isoformat(),
                "dagster_run_id": run_id,
                "semantic_projection": backfill_projection,
                "backfill_governed_files": _governed_file_rows(
                    scenario_root.parent, backfill_files
                ),
                "direct_governed_files": _governed_file_rows(scenario_root.parent, direct_files),
            }
        )
    if tuple(item["partition_key"] for item in partitions) != config.dagster.partition_keys:
        raise R2DB2BGovernanceError("B2b semantic evidence is incomplete or reordered")
    return {
        "contract_version": "r2d.b2b.semantic-parity.v1",
        "partition_count": len(partitions),
        "partitions": partitions,
        "direct_backfill_exact_match": True,
    }


def read_r2d_b2b_persistent_state(
    *, environment: Mapping[str, str], backfill_id: str
) -> dict[str, object]:
    """Read back B2b state from a newly constructed PostgreSQL Dagster instance."""

    with _persistent_instance(environment) as instance:
        backfill = instance.get_backfill(backfill_id)
        rows = _run_rows(instance, backfill_id)
    if backfill is None:
        raise R2DB2BGovernanceError("B2b backfill is absent during persistent readback")
    return {
        "backfill_id": backfill.backfill_id,
        "backfill_status": backfill.status.value,
        "partition_names": list(backfill.partition_names or ()),
        "runs": rows,
    }


def _cross_process_readback(
    *,
    environment: Mapping[str, str],
    project_root: Path,
    output: Path,
    backfill_id: str,
) -> dict[str, object]:
    command = [
        sys.executable,
        "-c",
        (
            "import json,os; from nexus.r2d_b2b import read_r2d_b2b_persistent_state; "
            "print(json.dumps(read_r2d_b2b_persistent_state("
            "environment=os.environ,backfill_id=os.environ['NEXUS_R2D_B2B_BACKFILL_ID']),"
            "sort_keys=True))"
        ),
    ]
    process_environment = dict(environment)
    process_environment["NEXUS_R2D_B2B_BACKFILL_ID"] = backfill_id
    completed = subprocess.run(
        command,
        cwd=project_root,
        env=process_environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    _write_exact(output / "execution" / "restart-readback.stderr.log", completed.stderr.encode())
    if completed.returncode != 0:
        raise R2DB2BGovernanceError("B2b fresh-process PostgreSQL readback failed")
    payload = json.loads(completed.stdout)
    if (
        payload.get("backfill_status") != "COMPLETED"
        or len(payload.get("runs", [])) != 2
        or any(row.get("status") != "SUCCESS" for row in payload["runs"])
    ):
        raise R2DB2BGovernanceError("B2b fresh-process readback changed persisted state")
    return {"contract_version": "r2d.b2b.restart-readback.v1", **payload}


def _publish_evidence(
    root: Path, payloads: dict[str, dict[str, object]]
) -> tuple[R2DB2BArtifactRef, ...]:
    if tuple(f"r2d.b2b.{name}" for name in payloads) != R2D_B2B_EVIDENCE_DATASETS:
        raise R2DB2BGovernanceError("B2b evidence payload topology differs from contract")
    return tuple(
        _artifact_ref(
            root,
            write_immutable_json_manifest(
                root,
                dataset=f"r2d.b2b.{name}",
                manifest=payload,
                schema_identity={"contract": payload["contract_version"]},
            ),
        )
        for name, payload in payloads.items()
    )


def _status_vector(rows: list[dict[str, object]]) -> list[str]:
    return sorted(cast(str, row["status"]) for row in rows)


def _require_no_secret(root: Path, secret: str) -> None:
    needle = secret.encode("utf-8")
    for path in root.rglob("*"):
        if path.is_file() and needle in path.read_bytes():
            raise R2DB2BGovernanceError("B2b artifact tree contains the PostgreSQL DSN")


def verify_r2d_b2b(
    *,
    artifact_root: Path,
    postgres_dsn: str,
    service_image_evidence_path: Path,
    config_path: Path = DEFAULT_R2D_B2B_CONFIG_PATH,
    project_root: Path = PROJECT_ROOT,
) -> R2DB2BVerificationResult:
    """Prove PostgreSQL-backed queued backfill continuity across daemon epochs."""

    output = _safe_fresh_root(artifact_root)
    config = load_r2d_b2b_config(config_path)
    service_image = load_r2d_b2b_service_image(service_image_evidence_path)
    service_image_file_sha256 = file_sha256(service_image_evidence_path)
    if service_image.configured_reference != config.postgres.configured_reference:
        raise R2DB2BGovernanceError("B2b observed PostgreSQL image differs from Compose authority")

    project_root = project_root.resolve()
    scenario_root = output / "scenario"
    dagster_home = _prepare_dagster_home(output, project_root)
    workspace_path = project_root / DEFAULT_R2D_B2B_WORKSPACE_PATH.relative_to(PROJECT_ROOT)
    environment = _dagster_environment(
        dagster_home=dagster_home,
        postgres_dsn=postgres_dsn,
        project_root=project_root,
        scenario_root=scenario_root,
    )
    sampler = _ProcessTreeSampler()
    daemon_one: _DaemonProcess | None = None
    daemon_two: _DaemonProcess | None = None
    sampler.start()
    try:
        checkpoint_payload = _prepare_checkpoint(project_root, scenario_root)
        instance_payload = _postgres_instance_evidence(
            config=config, environment=environment, postgres_dsn=postgres_dsn
        )
        backfill_id, submission_payload = _submit_backfill(
            config=config,
            output=output,
            workspace_path=workspace_path,
            project_root=project_root,
            environment=environment,
        )
        with _persistent_instance(environment) as instance:
            after_submission = _run_rows(instance, backfill_id)
        if _status_vector(after_submission) != ["QUEUED", "QUEUED"]:
            raise R2DB2BGovernanceError(
                "B2b actual backfill was not fully queued before daemon epoch one"
            )

        daemon_one = _start_daemon(
            output=output,
            label="daemon-epoch-one",
            workspace_path=workspace_path,
            project_root=project_root,
            environment=environment,
        )
        epoch_one_heartbeat = _wait_for_daemon_heartbeat(
            environment=environment,
            started_at=daemon_one.started_at,
            timeout_seconds=config.dagster.daemon_heartbeat_timeout_seconds,
        )
        daemon_dispatch_rows = _wait_for_run_state(
            environment=environment,
            backfill_id=backfill_id,
            timeout_seconds=config.dagster.run_timeout_seconds,
            predicate=lambda rows: len(rows) == 2 and _status_vector(rows) == ["QUEUED", "STARTED"],
        )
        daemon_one_stop, epoch_one_children = _stop_daemon_controller(
            daemon_one, timeout_seconds=config.resource_budget.shutdown_seconds
        )
        first_dispatch_rows = _wait_for_run_state(
            environment=environment,
            backfill_id=backfill_id,
            timeout_seconds=config.dagster.run_timeout_seconds,
            predicate=lambda rows: len(rows) == 2 and _status_vector(rows) == ["QUEUED", "SUCCESS"],
        )
        epoch_one_survivors = _cleanup_epoch_children(
            epoch_one_children, config.resource_budget.shutdown_seconds
        )
        daemon_one_stop["surviving_epoch_process_ids"] = list(epoch_one_survivors)
        daemon_one_stop["stdout_file_sha256"] = file_sha256(daemon_one.stdout_path)
        daemon_one_stop["stderr_file_sha256"] = file_sha256(daemon_one.stderr_path)
        daemon_one = None
        if epoch_one_survivors:
            raise R2DB2BGovernanceError("B2b first daemon epoch left surviving processes")
        time.sleep(config.dagster.queued_pause_observation_seconds)
        with _persistent_instance(environment) as instance:
            while_daemon_stopped = _run_rows(instance, backfill_id)
        if _status_vector(while_daemon_stopped) != ["QUEUED", "SUCCESS"]:
            raise R2DB2BGovernanceError("B2b queued run changed while the daemon was stopped")

        daemon_two = _start_daemon(
            output=output,
            label="daemon-epoch-two",
            workspace_path=workspace_path,
            project_root=project_root,
            environment=environment,
        )
        epoch_two_heartbeat = _wait_for_daemon_heartbeat(
            environment=environment,
            started_at=daemon_two.started_at,
            timeout_seconds=config.dagster.daemon_heartbeat_timeout_seconds,
        )
        if epoch_two_heartbeat["daemon_id"] == epoch_one_heartbeat["daemon_id"]:
            raise R2DB2BGovernanceError("B2b second daemon epoch reused the first daemon identity")
        final_rows = _wait_for_run_state(
            environment=environment,
            backfill_id=backfill_id,
            timeout_seconds=config.dagster.run_timeout_seconds,
            predicate=lambda rows: (
                len(rows) == 2 and _status_vector(rows) == ["SUCCESS", "SUCCESS"]
            ),
        )
        daemon_two_stop = _stop_daemon(
            daemon_two, timeout_seconds=config.resource_budget.shutdown_seconds
        )
        daemon_two = None
        if daemon_two_stop["surviving_epoch_process_ids"]:
            raise R2DB2BGovernanceError("B2b second daemon epoch left surviving processes")

        run_storage_payload = _backfill_storage_payload(
            config=config,
            environment=environment,
            postgres_dsn=postgres_dsn,
            backfill_id=backfill_id,
            after_submission=after_submission,
            after_daemon_dispatch=daemon_dispatch_rows,
            after_first_dispatch=first_dispatch_rows,
            while_daemon_stopped=while_daemon_stopped,
            final_rows=final_rows,
        )
        semantic_payload = _semantic_parity_payload(
            config=config,
            project_root=project_root,
            scenario_root=scenario_root,
            final_rows=final_rows,
        )
        readback_payload = _cross_process_readback(
            environment=environment,
            project_root=project_root,
            output=output,
            backfill_id=backfill_id,
        )
    finally:
        if daemon_one is not None:
            _stop_daemon(daemon_one, timeout_seconds=config.resource_budget.shutdown_seconds)
        if daemon_two is not None:
            _stop_daemon(daemon_two, timeout_seconds=config.resource_budget.shutdown_seconds)
        sampler.stop_sampling()

    cleanup_started = time.perf_counter()
    survivors = sampler.cleanup_seen(config.resource_budget.shutdown_seconds)
    cleanup_seconds = time.perf_counter() - cleanup_started
    resource_payload = sampler.evidence(
        output_root=output,
        budget=config.resource_budget.model_dump(mode="json"),
        cleanup_seconds=cleanup_seconds,
        survivors=survivors,
    )
    if not resource_payload["passed"]:
        raise R2DB2BGovernanceError(
            f"B2b resource or process-cleanup contract failed: {resource_payload}"
        )

    inherited_payload = {
        "contract_version": "r2d.b2b.inherited-r2b.v1",
        **config.inherited_r2b.model_dump(mode="json"),
        "observed_config_sha256": r2b_config_sha256(
            load_r2b_config(project_root / config.inherited_r2b.config_path)
        ),
        "observed_orchestration_code_sha256": r2b_orchestration_code_sha256(project_root),
        "validation_checkpoint": checkpoint_payload,
    }
    daemon_one_payload = {
        **epoch_one_heartbeat,
        "contract_version": "r2d.b2b.daemon-epoch.v1",
        "stopped_before_second_partition_dispatch": True,
        "stop_evidence": daemon_one_stop,
    }
    daemon_two_payload = {
        **epoch_two_heartbeat,
        "contract_version": "r2d.b2b.daemon-epoch.v1",
        "different_from_epoch_one": True,
        "stop_evidence": daemon_two_stop,
    }
    scope_payload = {
        "contract_version": "r2d.b2b.scope.v1",
        **config.scope.model_dump(mode="json"),
        "high_availability_claim": False,
        "automatic_scheduling_claim": False,
        "retained_multiservice_restart_claim": False,
    }
    _require_no_secret(output, postgres_dsn)
    evidence_refs = _publish_evidence(
        output,
        {
            "inherited_r2b": inherited_payload,
            "service_image": service_image.model_dump(mode="json"),
            "instance": instance_payload,
            "daemon_epoch_one": daemon_one_payload,
            "backfill_submission": submission_payload,
            "daemon_epoch_two": daemon_two_payload,
            "run_storage": run_storage_payload,
            "semantic_parity": semantic_payload,
            "restart_readback": readback_payload,
            "resource": resource_payload,
            "scope": scope_payload,
        },
    )
    evidence_sha = {
        reference.dataset.removeprefix("r2d.b2b."): reference.content_sha256
        for reference in evidence_refs
    }
    artifact_set_sha256 = canonical_json_sha256(
        [reference.model_dump(mode="json") for reference in evidence_refs]
    )
    check_sha = {
        "inherited_r2b_authority": config.inherited_r2b.config_sha256,
        "observed_postgres_image": service_image.content_sha256,
        "postgres_backed_dagster_storage": evidence_sha["instance"],
        "daemon_epoch_one_heartbeat": evidence_sha["daemon_epoch_one"],
        "actual_bounded_backfill_submission": evidence_sha["backfill_submission"],
        "first_partition_daemon_dispatch": evidence_sha["run_storage"],
        "daemon_restart_continuity": evidence_sha["daemon_epoch_two"],
        "exact_partition_run_success": evidence_sha["run_storage"],
        "backfill_semantic_parity": evidence_sha["semantic_parity"],
        "cross_process_persistent_readback": evidence_sha["restart_readback"],
        "immutable_artifact_set": artifact_set_sha256,
        "bounded_resource_cleanup": evidence_sha["resource"],
        "honest_scope": evidence_sha["scope"],
    }
    receipt_payload: dict[str, object] = {
        "contract_version": "r2d.b2b.verification-receipt.v1",
        "release": config.release,
        "status": "PASS",
        "checks": [
            {"name": name, "passed": True, "evidence_sha256": check_sha[name]}
            for name in R2D_B2B_PASS_CHECK_NAMES
        ],
        "evidence_refs": [reference.model_dump(mode="json") for reference in evidence_refs],
        "config_sha256": r2d_b2b_config_sha256(config),
        "code_sha256": r2d_b2b_code_sha256(project_root),
        "inherited_r2b_config_sha256": config.inherited_r2b.config_sha256,
        "inherited_r2b_orchestration_code_sha256": (config.inherited_r2b.orchestration_code_sha256),
        "service_image_sha256": service_image.content_sha256,
        "service_image_file_sha256": service_image_file_sha256,
        "instance_sha256": evidence_sha["instance"],
        "daemon_epoch_one_sha256": evidence_sha["daemon_epoch_one"],
        "backfill_submission_sha256": evidence_sha["backfill_submission"],
        "daemon_epoch_two_sha256": evidence_sha["daemon_epoch_two"],
        "run_storage_sha256": evidence_sha["run_storage"],
        "semantic_parity_sha256": evidence_sha["semantic_parity"],
        "restart_readback_sha256": evidence_sha["restart_readback"],
        "resource_sha256": evidence_sha["resource"],
        "artifact_set_sha256": artifact_set_sha256,
        "scope_sha256": evidence_sha["scope"],
        "backfill_id": backfill_id,
        "dagster_run_ids": sorted(cast(str, row["run_id"]) for row in final_rows),
        "partition_keys": list(config.dagster.partition_keys),
        "runtime": _runtime(),
        **config.scope.model_dump(mode="json"),
        "high_availability_claim": False,
        "automatic_scheduling_claim": False,
        "retained_multiservice_restart_claim": False,
    }
    receipt = R2DB2BVerificationReceipt.model_validate(
        {**receipt_payload, "content_sha256": canonical_json_sha256(receipt_payload)}
    )
    receipt_artifact = write_immutable_json_manifest(
        output,
        dataset="r2d.b2b.verification_receipt",
        manifest=receipt.model_dump(mode="json"),
        schema_identity={"contract": receipt.contract_version},
    )
    load_r2d_b2b_receipt(receipt_artifact.path)
    return R2DB2BVerificationResult(receipt=receipt, receipt_artifact=receipt_artifact)


def load_r2d_b2b_receipt(path: Path) -> R2DB2BVerificationReceipt:
    """Strictly reload a complete B2b package without a live PostgreSQL service."""

    try:
        raw = path.read_bytes()
        parsed = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2DB2BGovernanceError(f"cannot load B2b receipt: {path}") from exc
    if raw != canonical_json_bytes(parsed) + b"\n":
        raise R2DB2BGovernanceError("B2b receipt is not canonical JSON")
    receipt = R2DB2BVerificationReceipt.model_validate(parsed)
    root = path.resolve().parents[3]
    payloads: dict[str, dict[str, object]] = {}
    for reference in receipt.evidence_refs:
        artifact = (root / reference.relative_path).resolve()
        if not artifact.is_relative_to(root) or not artifact.is_file() or artifact.is_symlink():
            raise R2DB2BGovernanceError("B2b evidence artifact is missing or escapes its root")
        artifact_raw = artifact.read_bytes()
        artifact_payload = json.loads(artifact_raw)
        if not isinstance(artifact_payload, dict):
            raise R2DB2BGovernanceError("B2b evidence artifact must be a JSON object")
        if artifact_raw != canonical_json_bytes(artifact_payload) + b"\n":
            raise R2DB2BGovernanceError("B2b evidence artifact is not canonical JSON")
        if canonical_json_sha256(artifact_payload) != reference.content_sha256:
            raise R2DB2BGovernanceError("B2b evidence content identity mismatch")
        if file_sha256(artifact) != reference.file_sha256:
            raise R2DB2BGovernanceError("B2b evidence file identity mismatch")
        if canonical_json_sha256({"contract": artifact_payload.get("contract_version")}) != (
            reference.schema_sha256
        ):
            raise R2DB2BGovernanceError("B2b evidence schema identity mismatch")
        payloads[reference.dataset] = artifact_payload

    config = load_r2d_b2b_config()
    if receipt.config_sha256 != r2d_b2b_config_sha256(config):
        raise R2DB2BGovernanceError("B2b receipt differs from the current config")
    if receipt.code_sha256 != r2d_b2b_code_sha256(PROJECT_ROOT):
        raise R2DB2BGovernanceError("B2b receipt differs from the current code")
    service_image = R2DB2BServiceImageEvidence.model_validate(payloads["r2d.b2b.service_image"])
    if (
        service_image.content_sha256 != receipt.service_image_sha256
        or service_image.configured_reference != config.postgres.configured_reference
    ):
        raise R2DB2BGovernanceError("B2b service-image evidence is detached")

    by_dataset: dict[str, R2DB2BArtifactRef] = {
        reference.dataset: reference for reference in receipt.evidence_refs
    }
    top_level = {
        "r2d.b2b.instance": receipt.instance_sha256,
        "r2d.b2b.daemon_epoch_one": receipt.daemon_epoch_one_sha256,
        "r2d.b2b.backfill_submission": receipt.backfill_submission_sha256,
        "r2d.b2b.daemon_epoch_two": receipt.daemon_epoch_two_sha256,
        "r2d.b2b.run_storage": receipt.run_storage_sha256,
        "r2d.b2b.semantic_parity": receipt.semantic_parity_sha256,
        "r2d.b2b.restart_readback": receipt.restart_readback_sha256,
        "r2d.b2b.resource": receipt.resource_sha256,
        "r2d.b2b.scope": receipt.scope_sha256,
    }
    if any(
        by_dataset[dataset].content_sha256 != expected for dataset, expected in top_level.items()
    ):
        raise R2DB2BGovernanceError("B2b receipt is detached from governed evidence")
    inherited = payloads["r2d.b2b.inherited_r2b"]
    if (
        inherited.get("observed_config_sha256") != config.inherited_r2b.config_sha256
        or inherited.get("observed_orchestration_code_sha256")
        != config.inherited_r2b.orchestration_code_sha256
    ):
        raise R2DB2BGovernanceError("B2b inherited R2B authority is detached")
    instance = payloads["r2d.b2b.instance"]
    if instance.get("dsn_persisted") is not False or instance.get("storage_classes") != {
        "run": config.postgres.run_storage_class,
        "event_log": config.postgres.event_log_storage_class,
        "schedule": config.postgres.schedule_storage_class,
    }:
        raise R2DB2BGovernanceError("B2b PostgreSQL storage evidence is invalid")
    epoch_one = payloads["r2d.b2b.daemon_epoch_one"]
    epoch_one_errors = cast(dict[str, object], epoch_one.get("heartbeat_error_counts", {}))
    epoch_two = payloads["r2d.b2b.daemon_epoch_two"]
    epoch_two_errors = cast(dict[str, object], epoch_two.get("heartbeat_error_counts", {}))
    if (
        epoch_one.get("daemon_id") == epoch_two.get("daemon_id")
        or epoch_two.get("different_from_epoch_one") is not True
        or any(epoch_one_errors.values())
        or any(epoch_two_errors.values())
    ):
        raise R2DB2BGovernanceError("B2b daemon epoch continuity evidence is invalid")
    run_storage = payloads["r2d.b2b.run_storage"]
    if (
        _status_vector(cast(list[dict[str, object]], run_storage.get("after_daemon_dispatch")))
        != ["QUEUED", "STARTED"]
        or _status_vector(cast(list[dict[str, object]], run_storage.get("after_first_dispatch")))
        != ["QUEUED", "SUCCESS"]
        or _status_vector(cast(list[dict[str, object]], run_storage.get("while_daemon_stopped")))
        != ["QUEUED", "SUCCESS"]
        or _status_vector(cast(list[dict[str, object]], run_storage.get("final_runs")))
        != ["SUCCESS", "SUCCESS"]
    ):
        raise R2DB2BGovernanceError("B2b persisted run-state transitions are invalid")
    semantic = payloads["r2d.b2b.semantic_parity"]
    if semantic.get("direct_backfill_exact_match") is not True:
        raise R2DB2BGovernanceError("B2b semantic parity claim is false")
    for partition in cast(list[dict[str, object]], semantic.get("partitions")):
        for collection in ("backfill_governed_files", "direct_governed_files"):
            for file_row in cast(list[dict[str, object]], partition.get(collection)):
                governed = (root / cast(str, file_row["relative_path"])).resolve()
                if (
                    not governed.is_relative_to(root)
                    or not governed.is_file()
                    or file_sha256(governed) != file_row["file_sha256"]
                    or governed.stat().st_size != file_row["size_bytes"]
                ):
                    raise R2DB2BGovernanceError("B2b governed semantic file is invalid")
    readback = payloads["r2d.b2b.restart_readback"]
    if readback.get("backfill_id") != receipt.backfill_id or _status_vector(
        cast(list[dict[str, object]], readback.get("runs"))
    ) != ["SUCCESS", "SUCCESS"]:
        raise R2DB2BGovernanceError("B2b fresh-process readback is detached")
    resource = payloads["r2d.b2b.resource"]
    resource_observed = cast(dict[str, object], resource.get("observed", {}))
    if resource.get("passed") is not True or resource_observed.get("surviving_process_ids") != []:
        raise R2DB2BGovernanceError("B2b resource evidence is not a clean PASS")
    scope = payloads["r2d.b2b.scope"]
    if any(scope.get(key) != value for key, value in config.scope.model_dump(mode="json").items()):
        raise R2DB2BGovernanceError("B2b receipt overclaims its bounded scope")
    return receipt


def dump_r2d_b2b_result(result: R2DB2BVerificationResult) -> str:
    return json.dumps(
        {
            "actual_backfill_status": result.receipt.actual_backfill_status,
            "checks": [item.model_dump(mode="json") for item in result.receipt.checks],
            "dagster_process_restart_status": result.receipt.dagster_process_restart_status,
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
