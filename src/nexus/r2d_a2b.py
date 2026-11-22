"""Executable hard-crash and restart-reconciliation gate for R2D-A2b-1."""

from __future__ import annotations

import multiprocessing
import os
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn, cast

from psycopg import Connection

from nexus.ingestion.r2d_a2b_contracts import (
    R2D_A2B1_PASS_CHECK_NAMES,
    R2DA2b1CrashWindowEvidence,
    R2DA2b1VerificationCheck,
    R2DA2b1VerificationReceipt,
    r2d_a2b1_code_sha256,
)
from nexus.ingestion.r2d_capture import raw_value_multiset_sha256
from nexus.ingestion.r2d_contracts import (
    R2DPhysicalRecord,
    canonical_json_sha256,
    load_r2d_config,
    r2d_config_sha256,
)
from nexus.ingestion.r2d_service import (
    R2DA2Runtime,
    R2DA2Service,
    R2DBrokerAheadError,
    R2DInboxConflictError,
    R2DServiceFailpoint,
)
from nexus.ingestion.r2d_service_contracts import (
    load_r2d_a2a_config,
    r2d_a2a_config_sha256,
)
from nexus.modeling.r3_artifacts import ScientificArtifactRef, write_immutable_json_manifest
from nexus.r2d import split_lf_jsonl_values
from nexus.simulation import generate_r2_smoke_fixture

_BEFORE_DB_EXIT_CODE = 86
_AFTER_DB_EXIT_CODE = 87


@dataclass(frozen=True)
class R2DA2b1VerificationResult:
    """Passing hard-crash receipt and its immutable publication."""

    receipt: R2DA2b1VerificationReceipt
    receipt_artifact: ScientificArtifactRef


def _preflight_artifact_root(project_root: Path, artifact_root: Path) -> Path:
    resolved_project = project_root.resolve()
    resolved = artifact_root.resolve()
    if resolved == resolved_project or not resolved.is_relative_to(resolved_project):
        raise ValueError("A2b-1 artifact root must be a dedicated directory inside the project")
    current = resolved_project
    for part in resolved.relative_to(resolved_project).parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise ValueError("A2b-1 artifact root may not traverse a symlink")
    if resolved.exists() and any(resolved.iterdir()):
        raise ValueError("A2b-1 verification requires a fresh empty artifact root")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _hard_crash_worker(
    project_root: str,
    bootstrap_servers: str,
    postgres_dsn: str,
    partition: int,
    offset: int,
    failpoint: R2DServiceFailpoint,
    exit_code: int,
) -> None:
    """Open real clients and terminate the process at the selected transaction boundary."""

    root = Path(project_root)
    service = R2DA2Service(
        project_root=root,
        service_config=load_r2d_a2a_config(root / "configs" / "ingestion" / "r2d-a2a.yaml"),
        data_config=load_r2d_config(root / "configs" / "ingestion" / "r2d-a1.yaml"),
        runtime=R2DA2Runtime(
            bootstrap_servers=bootstrap_servers,
            postgres_dsn=postgres_dsn,
        ),
    )

    def terminate_process(_: R2DServiceFailpoint) -> NoReturn:
        os._exit(exit_code)

    record = service.read_record_at(partition=partition, offset=offset)
    connection = service.connect_postgres()
    service.persist_record(
        connection,
        record,
        failpoint=failpoint,
        crash_handler=terminate_process,
    )
    os._exit(95)


def _run_hard_crash(
    *,
    project_root: Path,
    runtime: R2DA2Runtime,
    partition: int,
    offset: int,
    failpoint: R2DServiceFailpoint,
    expected_exit_code: int,
    timeout_seconds: int,
) -> int:
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=_hard_crash_worker,
        args=(
            str(project_root),
            runtime.bootstrap_servers,
            runtime.postgres_dsn,
            partition,
            offset,
            failpoint,
            expected_exit_code,
        ),
        name=f"nexus-r2d-{failpoint}",
    )
    process.start()
    process.join(timeout_seconds)
    if process.is_alive():
        process.terminate()
        process.join(5)
        raise ValueError(f"hard-crash worker timed out at {failpoint}")
    observed_exit_code = process.exitcode
    process.close()
    if observed_exit_code != expected_exit_code:
        raise ValueError(
            f"hard-crash worker exited {observed_exit_code}, expected {expected_exit_code}"
        )
    return expected_exit_code


def _progress(
    connection: Connection[tuple[object, ...]],
    *,
    topic: str,
) -> tuple[int, ...]:
    rows = connection.execute(
        """
        SELECT partition_id, next_offset
        FROM r2d_capture.partition_progress
        WHERE topic = %s
        ORDER BY partition_id
        """,
        (topic,),
    ).fetchall()
    if tuple(row[0] for row in rows) != (0, 1):
        raise ValueError("PostgreSQL progress lost the exact two-partition topology")
    return tuple(cast(int, row[1]) for row in rows)


def _inbox_rows(
    connection: Connection[tuple[object, ...]],
    *,
    topic: str,
    partition: int,
    offset: int,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM r2d_capture.inbox
        WHERE topic = %s AND partition_id = %s AND broker_offset = %s
        """,
        (topic, partition, offset),
    ).fetchone()
    if row is None:
        raise ValueError("PostgreSQL inbox count is unavailable")
    return cast(int, row[0])


def _crash_evidence(
    *,
    failpoint: R2DServiceFailpoint,
    partition: int,
    offset: int,
    expected_exit_code: int,
    observed_exit_code: int,
    db_next_before: int,
    db_next_after: int,
    broker_next_before: int,
    broker_next_after: int,
    inbox_rows_before: int,
    inbox_rows_after: int,
) -> R2DA2b1CrashWindowEvidence:
    payload: dict[str, object] = {
        "contract_version": "r2d.a2b1.crash-window.v1",
        "failpoint": failpoint,
        "partition": partition,
        "offset": offset,
        "expected_exit_code": expected_exit_code,
        "observed_exit_code": observed_exit_code,
        "db_next_before": db_next_before,
        "db_next_after": db_next_after,
        "broker_next_before": broker_next_before,
        "broker_next_after": broker_next_after,
        "inbox_rows_before": inbox_rows_before,
        "inbox_rows_after": inbox_rows_after,
    }
    return R2DA2b1CrashWindowEvidence.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def verify_r2d_a2b1(
    *,
    project_root: Path,
    artifact_root: Path,
    bootstrap_servers: str,
    postgres_dsn: str,
) -> R2DA2b1VerificationResult:
    """Execute both hard crash windows and every bounded restart relation."""

    root = project_root.resolve()
    output = _preflight_artifact_root(root, artifact_root)
    data_config = load_r2d_config(root / "configs" / "ingestion" / "r2d-a1.yaml")
    service_config = load_r2d_a2a_config(root / "configs" / "ingestion" / "r2d-a2a.yaml")
    runtime = R2DA2Runtime(bootstrap_servers=bootstrap_servers, postgres_dsn=postgres_dsn)
    fixture = generate_r2_smoke_fixture(output / "source")
    values = split_lf_jsonl_values((fixture.source_dir / "inventory_events.jsonl").read_bytes())
    service = R2DA2Service(
        project_root=root,
        service_config=service_config,
        data_config=data_config,
        runtime=runtime,
    )
    service.provision_topic()

    with service.connect_postgres() as connection:
        migration_identity = service.apply_migration(connection)
        initial_starts = service.initialize_fresh_empty(connection)
        broker_ack_ledger = service.produce(values)
        first_by_partition = {
            acknowledgement.partition: acknowledgement
            for acknowledgement in broker_ack_ledger.acknowledgements
            if acknowledgement.offset == 0
        }
        if tuple(sorted(first_by_partition)) != (0, 1):
            raise ValueError("hard-crash fixture requires offset zero in both partitions")

        before_partition = 0
        before_offset = first_by_partition[before_partition].offset
        before_db = _progress(connection, topic=service.topic)
        before_broker = service.broker_committed_offsets()
        before_rows = _inbox_rows(
            connection,
            topic=service.topic,
            partition=before_partition,
            offset=before_offset,
        )
        before_exit = _run_hard_crash(
            project_root=root,
            runtime=runtime,
            partition=before_partition,
            offset=before_offset,
            failpoint="before_db_commit",
            expected_exit_code=_BEFORE_DB_EXIT_CODE,
            timeout_seconds=service_config.gate.poll_timeout_seconds,
        )
        after_before_db = _progress(connection, topic=service.topic)
        after_before_broker = service.broker_committed_offsets()
        after_before_rows = _inbox_rows(
            connection,
            topic=service.topic,
            partition=before_partition,
            offset=before_offset,
        )
        before_evidence = _crash_evidence(
            failpoint="before_db_commit",
            partition=before_partition,
            offset=before_offset,
            expected_exit_code=_BEFORE_DB_EXIT_CODE,
            observed_exit_code=before_exit,
            db_next_before=before_db[before_partition],
            db_next_after=after_before_db[before_partition],
            broker_next_before=before_broker[before_partition],
            broker_next_after=after_before_broker[before_partition],
            inbox_rows_before=before_rows,
            inbox_rows_after=after_before_rows,
        )

        recovered_before_record = service.read_record_at(
            partition=before_partition,
            offset=before_offset,
        )
        if not service.persist_record(connection, recovered_before_record):
            raise ValueError("pre-commit crash recovery did not create the first durable row")
        service.commit_broker_offsets((1, 0))

        after_partition = 1
        after_offset = first_by_partition[after_partition].offset
        post_db_before = _progress(connection, topic=service.topic)
        post_broker_before = service.broker_committed_offsets()
        post_rows_before = _inbox_rows(
            connection,
            topic=service.topic,
            partition=after_partition,
            offset=after_offset,
        )
        after_exit = _run_hard_crash(
            project_root=root,
            runtime=runtime,
            partition=after_partition,
            offset=after_offset,
            failpoint="after_db_commit_before_broker_commit",
            expected_exit_code=_AFTER_DB_EXIT_CODE,
            timeout_seconds=service_config.gate.poll_timeout_seconds,
        )
        post_db_after = _progress(connection, topic=service.topic)
        post_broker_after = service.broker_committed_offsets()
        post_rows_after = _inbox_rows(
            connection,
            topic=service.topic,
            partition=after_partition,
            offset=after_offset,
        )
        after_evidence = _crash_evidence(
            failpoint="after_db_commit_before_broker_commit",
            partition=after_partition,
            offset=after_offset,
            expected_exit_code=_AFTER_DB_EXIT_CODE,
            observed_exit_code=after_exit,
            db_next_before=post_db_before[after_partition],
            db_next_after=post_db_after[after_partition],
            broker_next_before=post_broker_before[after_partition],
            broker_next_after=post_broker_after[after_partition],
            inbox_rows_before=post_rows_before,
            inbox_rows_after=post_rows_after,
        )

        redelivered = service.read_record_at(partition=after_partition, offset=after_offset)
        if service.persist_record(connection, redelivered):
            raise ValueError("post-commit redelivery created a duplicate inbox row")
        count_after_redelivery = _inbox_rows(
            connection,
            topic=service.topic,
            partition=after_partition,
            offset=after_offset,
        )
        if count_after_redelivery != 1:
            raise ValueError("post-commit redelivery did not preserve one inbox row")

        caught_up = service.reconcile_current_broker_offsets(connection)
        if caught_up != (1, 1) or service.broker_committed_offsets() != caught_up:
            raise ValueError("broker-below-PostgreSQL restart did not catch up")
        equal = service.reconcile_current_broker_offsets(connection)
        if equal != caught_up or service.broker_committed_offsets() != equal:
            raise ValueError("equal broker/PostgreSQL restart did not continue unchanged")

        service.commit_broker_offsets((2, 1))
        broker_ahead_rejected = False
        try:
            service.reconcile_current_broker_offsets(connection)
        except R2DBrokerAheadError:
            broker_ahead_rejected = True
        finally:
            service.commit_broker_offsets((1, 1))
        if not broker_ahead_rejected:
            raise ValueError("broker-ahead restart did not fail closed")

        progress_before_conflict = _progress(connection, topic=service.topic)
        rows_before_conflict = _inbox_rows(
            connection,
            topic=service.topic,
            partition=after_partition,
            offset=after_offset,
        )
        timestamp = redelivered.broker_timestamp_ms
        if timestamp is None:
            raise ValueError("frozen crash record has no broker timestamp")
        conflicting_record: R2DPhysicalRecord = redelivered.model_copy(
            update={"broker_timestamp_ms": timestamp + 1}
        )
        conflict_rejected = False
        try:
            service.persist_record(connection, conflicting_record)
        except R2DInboxConflictError:
            conflict_rejected = True
        if not conflict_rejected:
            raise ValueError("conflicting physical evidence did not fail closed")
        if (
            _progress(connection, topic=service.topic) != progress_before_conflict
            or _inbox_rows(
                connection,
                topic=service.topic,
                partition=after_partition,
                offset=after_offset,
            )
            != rows_before_conflict
        ):
            raise ValueError("physical conflict changed durable evidence or progress")

        capture = service.capture_barrier(
            connection,
            broker_ack_ledger,
            evidence_starts=initial_starts,
        )
        total_inbox = connection.execute("SELECT COUNT(*) FROM r2d_capture.inbox").fetchone()

    if total_inbox != (len(values),):
        raise ValueError("final inbox count differs from the acknowledged fixture")
    if capture.starts != (1, 1) or capture.next_offsets != capture.ends:
        raise ValueError("restart barrier did not converge from the expected durable offsets")
    captured_values = tuple(
        item.record.value_bytes
        for item in capture.transcript.records
        if item.record.value_bytes is not None
    )
    raw_multiset_sha256 = raw_value_multiset_sha256(captured_values)
    if raw_multiset_sha256 != data_config.source_contract.raw_value_multiset_sha256:
        raise ValueError("hard-crash capture raw values differ from the frozen source")

    restart_evidence_sha256 = canonical_json_sha256(
        {
            "restart_starts": list(capture.starts),
            "ends": list(capture.ends),
            "next_offsets": list(capture.next_offsets),
        }
    )
    redelivery_sha256 = canonical_json_sha256(
        {
            "partition": after_partition,
            "offset": after_offset,
            "inserted": False,
            "inbox_rows": count_after_redelivery,
        }
    )
    broker_relation_sha256 = canonical_json_sha256({"database": [1, 1], "broker": [1, 1]})
    broker_ahead_sha256 = canonical_json_sha256(
        {"attempted_broker": [2, 1], "database": [1, 1], "rejected": True}
    )
    conflict_sha256 = canonical_json_sha256(
        {
            "progress": list(progress_before_conflict),
            "inbox_rows": rows_before_conflict,
            "rejected": True,
        }
    )
    survivor_sha256 = canonical_json_sha256(
        {"exit_codes": [_BEFORE_DB_EXIT_CODE, _AFTER_DB_EXIT_CODE], "survivors": 0}
    )
    evidence_by_check = {
        "hard_crash_before_db_commit": before_evidence.content_sha256,
        "hard_crash_after_db_commit_before_broker_commit": after_evidence.content_sha256,
        "restart_redelivery_is_idempotent": redelivery_sha256,
        "broker_below_db_catches_up": restart_evidence_sha256,
        "broker_equal_db_continues": broker_relation_sha256,
        "broker_ahead_db_fails_closed": broker_ahead_sha256,
        "physical_conflict_fails_closed": conflict_sha256,
        "full_barrier_converges": capture.transcript.content_sha256,
        "crash_workers_leave_no_survivors": survivor_sha256,
    }
    checks = tuple(
        R2DA2b1VerificationCheck(
            name=name,
            passed=True,
            evidence_sha256=evidence_by_check[name],
        )
        for name in R2D_A2B1_PASS_CHECK_NAMES
    )
    payload: dict[str, object] = {
        "contract_version": "r2d.a2b1.verification-receipt.v1",
        "release": "R2D-A2b-1",
        "status": "PASS",
        "checks": [check.model_dump(mode="json") for check in checks],
        "topic": service_config.broker.topic,
        "partition_count": service_config.broker.partition_count,
        "record_count": len(capture.transcript.records),
        "initial_starts": list(initial_starts),
        "restart_starts": list(capture.starts),
        "ends": list(capture.ends),
        "next_offsets": list(capture.next_offsets),
        "worker_exit_codes": [_BEFORE_DB_EXIT_CODE, _AFTER_DB_EXIT_CODE],
        "worker_survivor_count": 0,
        "data_config_sha256": r2d_config_sha256(data_config),
        "service_config_sha256": r2d_a2a_config_sha256(service_config),
        "migration_sha256": migration_identity,
        "code_sha256": r2d_a2b1_code_sha256(root),
        "broker_ack_sha256": capture.broker_ack_ledger.content_sha256,
        "transcript_sha256": capture.transcript.content_sha256,
        "lineage_sha256": capture.transcript.lineage_sha256,
        "raw_value_multiset_sha256": raw_multiset_sha256,
        "before_db_commit_evidence_sha256": before_evidence.content_sha256,
        "after_db_commit_evidence_sha256": after_evidence.content_sha256,
        "crash_recovery_status": "PASS",
        "downstream_parity_status": "NOT_RUN",
        "r2d_a2_status": "IN_PROGRESS",
        "full_r2d_a_status": "OPEN",
        "full_g2_status": "OPEN",
        "exactly_once_claim": False,
    }
    receipt = R2DA2b1VerificationReceipt.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )
    receipt_artifact = write_immutable_json_manifest(
        output,
        dataset="r2d.a2b1.verification_receipt",
        manifest=receipt.model_dump(mode="json"),
        schema_identity={
            "contract_version": receipt.contract_version,
            "release": receipt.release,
        },
    )
    return R2DA2b1VerificationResult(receipt=receipt, receipt_artifact=receipt_artifact)
