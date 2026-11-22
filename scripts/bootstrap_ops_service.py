"""Fresh-only, Linux-local bootstrap for the isolated flagship Compose namespace.

Run in the application image with /state and /bootstrap-private writable by UID
10001, the PostgreSQL bootstrap secret mounted read-only, and the Redpanda network
namespace shared. Never use this script against an existing service state.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import secrets
import shutil
import sys
import time
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import Any, Literal

from confluent_kafka import KafkaError, KafkaException

from nexus.config import PROJECT_ROOT
from nexus.portfolio.auth import resolve_persona
from nexus.portfolio.broker_runtime import RuntimeBrokerConfig
from nexus.portfolio.capture_health_ledger import CaptureHealthLedger
from nexus.portfolio.postgres_backend import PostgresConfig
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.recovery_publisher import RecoveryTopic
from nexus.portfolio.runtime_service import prepare_postgres_runtime
from nexus.portfolio.service_config import read_service_config
from nexus.portfolio.source_freshness import SourceWatchPolicy
from nexus.portfolio.source_watch_runtime import SourceWatchRuntimeStore
from nexus.portfolio.transport_consumer import inspect_topic
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger, runtime_target

STATE = Path("/state")
PRIVATE = Path("/bootstrap-private")
ADMIN_SECRET = Path("/run/secrets/postgres_bootstrap")
DB = "nexus_portfolio_test_ops"
PORT = 55439
BROKER = "127.0.0.1:19092"
APP_ROLE = "nexus_ops_application"
MIGRATION_ROLE = "nexus_ops_migration"
WORKFLOW_SCHEMA = "nexus_ops_workflow"
OPERATIONS_SCHEMA = "nexus_ops_operations"
AUDIT_SCHEMA = "nexus_ops_audit"
SOURCE_TOPIC = "nexus-runtime-ops-source"
DEAD_LETTER_TOPIC = "nexus-runtime-ops-dead-letter"
REDRIVE_TOPIC = "nexus-runtime-ops-redrive-audit"
CLUSTER_ID = re.compile(r"[A-Za-z0-9_.-]{1,100}\Z")


class BootstrapRefusal(RuntimeError):
    """Controlled refusal; details are never rendered by the command entrypoint."""


class KafkaBootstrapError(BootstrapRefusal):
    """Only structured Kafka identity is retained; never driver exception text."""

    def __init__(self, error: KafkaError):
        self.code = error.code()
        name = error.name()
        self.name = name if re.fullmatch(r"[A-Z0-9_]{1,80}", name) else "UNKNOWN"
        super().__init__("kafka_bootstrap_failed")


_COORDINATOR_RETRY_CODES = {
    KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
    KafkaError.COORDINATOR_NOT_AVAILABLE,
    KafkaError.NOT_COORDINATOR,
    KafkaError.REQUEST_TIMED_OUT,
    KafkaError._TIMED_OUT,
    KafkaError._WAIT_COORD,
}


def _plain_path(path: Path, *, directory: bool) -> None:
    if not path.is_absolute() or any(parent.is_symlink() for parent in (path, *path.parents)):
        raise BootstrapRefusal("untrusted_path")
    if not (path.is_dir() if directory else path.is_file()):
        raise BootstrapRefusal("missing_mount")


def preflight(state: Path, private: Path, admin_secret: Path, expected_cluster_id: str) -> str:
    """Refuse reused state before any database or broker mutation."""
    if sys.platform != "linux" or os.getuid() != 10001:
        raise BootstrapRefusal("linux_nonroot_image_required")
    if not isinstance(expected_cluster_id, str) or not CLUSTER_ID.fullmatch(expected_cluster_id):
        raise BootstrapRefusal("expected_cluster_identity_required")
    _plain_path(state, directory=True)
    _plain_path(private, directory=True)
    _plain_path(admin_secret, directory=False)
    if any(state.iterdir()) or any(private.iterdir()):
        raise BootstrapRefusal("fresh_empty_mounts_required")
    if not os.access(state, os.W_OK | os.X_OK) or not os.access(private, os.W_OK | os.X_OK):
        raise BootstrapRefusal("owned_writable_mounts_required")
    raw = admin_secret.read_bytes()
    if len(raw) not in (64, 65, 66):
        raise BootstrapRefusal("invalid_bootstrap_secret")
    password = raw.removesuffix(b"\r\n").removesuffix(b"\n").decode("ascii")
    if not re.fullmatch(r"[0-9a-f]{64}", password):
        raise BootstrapRefusal("invalid_bootstrap_secret")
    return password


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    """Private mode, durable bytes, and no replacement of an existing receipt."""
    temporary = path.with_name(path.name + ".tmp-" + secrets.token_hex(8))
    data = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8")
    handle = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path, follow_symlinks=False)
        if os.name != "nt":
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _private_password(path: Path, value: str) -> None:
    handle = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(handle, "w", encoding="ascii") as stream:
        stream.write(value + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _dsn(user: str, password: str) -> str:
    return f"host=127.0.0.1 port={PORT} dbname={DB} user={user} password={password}"


def _database_preflight(admin_password: str, expected_data_dir: str) -> None:
    psycopg = importlib.import_module("psycopg")
    rows = importlib.import_module("psycopg.rows")
    with psycopg.connect(
        host="127.0.0.1",
        port=PORT,
        dbname=DB,
        user="nexus_bootstrap",
        password=admin_password,
        connect_timeout=5,
        row_factory=rows.dict_row,
    ) as db:
        identity = db.execute(
            "SELECT current_database() AS db, current_user AS role, "
            "inet_server_port() AS port, current_setting('data_directory') AS data_dir"
        ).fetchone()
        if identity != {
            "db": DB,
            "role": "nexus_bootstrap",
            "port": PORT,
            "data_dir": expected_data_dir,
        }:
            raise BootstrapRefusal("database_identity_differs")
        roles = db.execute(
            "SELECT rolname FROM pg_roles WHERE rolname IN (%s,%s)",
            (APP_ROLE, MIGRATION_ROLE),
        ).fetchall()
        schemas = db.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname IN (%s,%s,%s)",
            (WORKFLOW_SCHEMA, OPERATIONS_SCHEMA, AUDIT_SCHEMA),
        ).fetchall()
        if roles or schemas:
            raise BootstrapRefusal("database_state_exists")


def _broker_preflight(expected_cluster_id: str) -> Any:
    admin_module = importlib.import_module("confluent_kafka.admin")
    admin = admin_module.AdminClient({"bootstrap.servers": BROKER, "socket.timeout.ms": 5000})
    metadata = admin.list_topics(timeout=5)
    if metadata.cluster_id != expected_cluster_id or any(
        topic in metadata.topics for topic in (SOURCE_TOPIC, DEAD_LETTER_TOPIC, REDRIVE_TOPIC)
    ):
        raise BootstrapRefusal("broker_identity_or_state_differs")
    return admin


def _create_roles(admin_password: str, application_password: str, migration_password: str) -> None:
    psycopg = importlib.import_module("psycopg")
    sql = psycopg.sql
    with psycopg.connect(
        host="127.0.0.1",
        port=PORT,
        dbname=DB,
        user="nexus_bootstrap",
        password=admin_password,
        connect_timeout=5,
    ) as db:
        for role, password in (
            (MIGRATION_ROLE, migration_password),
            (APP_ROLE, application_password),
        ):
            db.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE NOREPLICATION"
                ).format(sql.Identifier(role), sql.Literal(password))
            )
        db.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}, {}").format(
                sql.Identifier(DB), sql.Identifier(MIGRATION_ROLE), sql.Identifier(APP_ROLE)
            )
        )
        db.execute(
            sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
                sql.Identifier(DB), sql.Identifier(MIGRATION_ROLE)
            )
        )


def _create_topic(
    admin: Any,
    name: str,
    *,
    partitions: int,
    recovery: bool,
    expected_cluster_id: str,
    locations: tuple[str, ...],
) -> str:
    admin_module = importlib.import_module("confluent_kafka.admin")
    policy = {"cleanup.policy": "delete", "retention.ms": "-1" if recovery else "604800000"}
    if recovery:
        policy["retention.bytes"] = "-1"
    result = admin.create_topics(
        [
            admin_module.NewTopic(
                name, num_partitions=partitions, replication_factor=1, config=policy
            )
        ],
        request_timeout=10,
    )
    result[name].result(12)
    config = RuntimeBrokerConfig(BROKER, name, "nexus-runtime-bootstrap-readback", locations)
    identity = inspect_topic(config)
    topic_id = identity.get("topic_id")
    if (
        identity
        != {
            "cluster_id": expected_cluster_id,
            "topic_id": topic_id,
            "partitions": list(range(partitions)),
        }
        or not isinstance(topic_id, str)
        or not re.fullmatch(r"[0-9a-f]{32}", topic_id)
    ):
        raise BootstrapRefusal("new_topic_identity_differs")
    resource = admin_module.ConfigResource(admin_module.ConfigResource.Type.TOPIC, name)
    observed = admin.describe_configs([resource], request_timeout=5)[resource].result(6)
    if any(key not in observed or observed[key].value != value for key, value in policy.items()):
        raise BootstrapRefusal("new_topic_policy_differs")
    return topic_id


def _commit_zero(config: RuntimeBrokerConfig, expected_cluster_id: str) -> None:
    """Retry only a cold coordinator, always rechecking empty topic/group origin."""
    module = importlib.import_module("confluent_kafka")
    options = {**config.consumer_options(), "socket.timeout.ms": 5000}
    consumer = module.Consumer(options)
    deadline = time.monotonic() + 30

    def retry(error: KafkaError) -> None:
        if not (error.retriable() or error.code() in _COORDINATOR_RETRY_CODES):
            raise KafkaBootstrapError(error) from None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise KafkaBootstrapError(error) from None
        time.sleep(min(0.5, remaining))

    try:
        while True:
            if inspect_topic(config)["cluster_id"] != expected_cluster_id:
                raise BootstrapRefusal("broker_identity_differs")
            parts = [module.TopicPartition(config.topic, index, 0) for index in range(2)]
            if any(consumer.get_watermark_offsets(part, timeout=5) != (0, 0) for part in parts):
                raise BootstrapRefusal("source_topic_not_empty")
            try:
                prior = consumer.committed(
                    parts, timeout=min(5, max(0.1, deadline - time.monotonic()))
                )
            except KafkaException as exc:
                if not exc.args or not isinstance(exc.args[0], KafkaError):
                    raise BootstrapRefusal("kafka_error_unavailable") from None
                retry(exc.args[0])
                continue
            if len(prior) != 2 or any(part.offset >= 0 for part in prior):
                raise BootstrapRefusal("consumer_group_already_initialized")
            lookup_errors = [part.error for part in prior if part.error is not None]
            if lookup_errors:
                if any(not isinstance(error, KafkaError) for error in lookup_errors):
                    raise BootstrapRefusal("committed_lookup_unconfirmed")
                first_permanent = next(
                    (
                        error
                        for error in lookup_errors
                        if not (error.retriable() or error.code() in _COORDINATOR_RETRY_CODES)
                    ),
                    None,
                )
                retry(first_permanent or lookup_errors[0])
                continue
            try:
                result = consumer.commit(offsets=parts, asynchronous=False)
            except KafkaException as exc:
                if not exc.args or not isinstance(exc.args[0], KafkaError):
                    raise BootstrapRefusal("kafka_error_unavailable") from None
                retry(exc.args[0])
                continue
            if not isinstance(result, list) or len(result) != 2:
                raise BootstrapRefusal("zero_origin_commit_unconfirmed")
            errors = [part.error for part in result if part.error is not None]
            if errors:
                if any(not isinstance(error, KafkaError) for error in errors):
                    raise BootstrapRefusal("zero_origin_commit_unconfirmed")
                first_permanent = next(
                    (
                        error
                        for error in errors
                        if not (error.retriable() or error.code() in _COORDINATOR_RETRY_CODES)
                    ),
                    None,
                )
                retry(first_permanent or errors[0])
                continue
            if any(part.offset != 0 for part in result):
                raise BootstrapRefusal("zero_origin_commit_unconfirmed")
            return
    finally:
        consumer.close()


def provision(
    state: Path,
    private: Path,
    admin_password: str,
    expected_cluster_id: str,
    *,
    expected_data_dir: str,
    project_root: Path = PROJECT_ROOT,
    progress: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Retain each successful boundary; a failed run is never reset or reused."""
    progress = progress if progress is not None else {}
    progress["stage"] = "database_preflight"
    _database_preflight(admin_password, expected_data_dir)
    progress["stage"] = "broker_preflight"
    admin = _broker_preflight(expected_cluster_id)
    progress["stage"] = "credentials"
    app_password, migration_password = secrets.token_hex(32), secrets.token_hex(32)
    _private_password(private / "application-password.txt", app_password)
    _private_password(private / "migration-password.txt", migration_password)
    progress["stage"] = "roles"
    _create_roles(admin_password, app_password, migration_password)
    app_dsn = _dsn(APP_ROLE, app_password)
    migration_dsn = _dsn(MIGRATION_ROLE, migration_password)
    root = state / "application"
    workflow = PostgresConfig(app_dsn, WORKFLOW_SCHEMA, root / "evidence" / "initial")
    base = PostgresRuntimeStore(workflow, runtime_root=root)
    progress["stage"] = "workflow_migration"
    base.migrate(migration_dsn=migration_dsn)
    progress["stage"] = "initial_seed"
    initial = prepare_postgres_runtime(project_root, root, workflow)
    locations = tuple(initial.locations())
    initial_source_sha256 = initial.seed()["metadata"]["source_snapshot_sha256"]
    initial_runtime_target = runtime_target(initial.control)
    progress["stage"] = "source_watch_files"
    watched = state / "watched-source"
    shutil.copytree(root / "evidence" / "initial" / "source", watched, symlinks=False)
    watch = SourceWatchPolicy("inventory", str(watched.resolve()), 30)

    progress["stage"] = "source_topic"
    source_id = _create_topic(
        admin,
        SOURCE_TOPIC,
        partitions=2,
        recovery=False,
        expected_cluster_id=expected_cluster_id,
        locations=locations,
    )
    ops_broker = RuntimeBrokerConfig(BROKER, SOURCE_TOPIC, "nexus-runtime-ops", locations)
    audit_broker = replace(ops_broker, group_id="nexus-runtime-audit")
    ops_binding = TransportBinding(
        ops_broker, expected_cluster_id, source_id, 2, "operations", initial_runtime_target
    )
    audit_binding = TransportBinding(audit_broker, expected_cluster_id, source_id, 2, "audit")
    # Later migrations replace V4's exact-schema contract. Do not read this store again.
    del initial
    progress["stage"] = "consumer_groups"
    for broker in (ops_broker, audit_broker):
        _commit_zero(broker, expected_cluster_id)
    ops_db = PostgresConfig(app_dsn, OPERATIONS_SCHEMA, root / "transport" / "operations")
    audit_db = PostgresConfig(app_dsn, AUDIT_SCHEMA, root / "transport" / "audit")
    progress["stage"] = "transport_migrations"
    operations_base = TransportLedger(ops_db)
    operations_base.migrate(migration_dsn=migration_dsn)
    operations_base.register(ops_binding)
    audit = TransportLedger(audit_db)
    audit.migrate(migration_dsn=migration_dsn)
    audit.register(audit_binding)
    operations = CaptureHealthLedger(ops_db, health_binding=ops_binding, max_age_seconds=10)
    progress["stage"] = "capture_health_migration"
    operations.migrate(migration_dsn=migration_dsn)
    publication = PublicationRuntimeStore(
        workflow, runtime_root=root, transport=operations, binding=ops_binding
    )
    progress["stage"] = "source_publication_migration"
    publication.migrate(migration_dsn=migration_dsn)
    source = SourceWatchRuntimeStore(
        workflow, runtime_root=root, transport=operations, binding=ops_binding, watch=watch
    )
    progress["stage"] = "source_watch_migration"
    source.migrate(migration_dsn=migration_dsn)
    operations.publication = source

    progress["stage"] = "recovery_topics"
    evidence_topics = []
    topic_specs: tuple[tuple[str, Literal["dead_letter", "redrive_audit"]], ...] = (
        (DEAD_LETTER_TOPIC, "dead_letter"),
        (REDRIVE_TOPIC, "redrive_audit"),
    )
    for name, kind in topic_specs:
        topic_id = _create_topic(
            admin,
            name,
            partitions=1,
            recovery=True,
            expected_cluster_id=expected_cluster_id,
            locations=locations,
        )
        broker = replace(
            ops_broker,
            topic=name,
            group_id=f"nexus-runtime-recovery-reader-{kind.replace('_', '-')}",
        )
        evidence_topics.append(RecoveryTopic(ops_binding, broker, topic_id, kind))
    progress["stage"] = "recovery_enrollment"
    operations.enroll_topics(
        ops_binding, evidence_topics[0], evidence_topics[1], resolve_persona("approver", locations)
    )
    manifest = {
        "contract": "nexus.local-service.v2",
        "runtime_root": str(root),
        "workflow": {"dsn": app_dsn, "schema": WORKFLOW_SCHEMA},
        "operations": {
            "database": {"dsn": app_dsn, "schema": OPERATIONS_SCHEMA},
            "binding": ops_binding.payload(),
        },
        "audit": {
            "database": {"dsn": app_dsn, "schema": AUDIT_SCHEMA},
            "binding": audit_binding.payload(),
        },
        "source_watch": watch.payload(),
        "capture_health": {"max_age_seconds": 10},
    }
    manifest_path = state / "private-service.json"
    progress["stage"] = "manifest"
    _atomic_json(manifest_path, manifest)
    read_service_config(manifest_path)
    progress["stage"] = "receipt_construction"
    receipt = {
        "contract": "nexus.ops-bootstrap.v1",
        "status": "prepared_not_service_ready",
        "bootstrap_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "database": DB,
        "schemas": [WORKFLOW_SCHEMA, OPERATIONS_SCHEMA, AUDIT_SCHEMA],
        "source_sha256": initial_source_sha256,
        "cluster_id": expected_cluster_id,
        "source_topic_id": source_id,
        "operations_binding": ops_binding.payload(),
        "audit_binding": audit_binding.payload(),
        "recovery_topics": [topic.payload() for topic in evidence_topics],
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "manifest_path": str(manifest_path),
        "application_credential_path": str(private / "application-password.txt"),
        "migration_credential_path": str(private / "migration-password.txt"),
    }
    progress["stage"] = "receipt"
    _atomic_json(private / "bootstrap-receipt.json", receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-cluster-id", required=True)
    parser.add_argument("--expected-pgdata", required=True)
    args = parser.parse_args()
    progress = {"stage": "preflight"}
    try:
        if (
            not args.expected_pgdata.startswith("/var/lib/postgresql/data/")
            or ".." in Path(args.expected_pgdata).parts
        ):
            raise BootstrapRefusal("expected_pgdata_required")
        admin_password = preflight(STATE, PRIVATE, ADMIN_SECRET, args.expected_cluster_id)
        progress["stage"] = "provision"
        provision(
            STATE,
            PRIVATE,
            admin_password,
            args.expected_cluster_id,
            expected_data_dir=args.expected_pgdata,
            progress=progress,
        )
    except Exception as error:
        # Never emit exception text or a traceback: psycopg errors can contain DSNs.
        if PRIVATE.is_dir() and not (PRIVATE / "bootstrap-failure.json").exists():
            with suppress(OSError):
                failure = {
                    "contract": "nexus.ops-bootstrap-failure.v1",
                    "status": "failed",
                    "stage": progress["stage"],
                    "error_type": type(error).__name__,
                    "state_retained": True,
                }
                if isinstance(error, KafkaBootstrapError):
                    failure.update(kafka_error_code=error.code, kafka_error_name=error.name)
                _atomic_json(
                    PRIVATE / "bootstrap-failure.json",
                    failure,
                )
        print(
            "bootstrap_failed; inspect private failure receipt and retained state",
            file=sys.stderr,
        )
        return 1
    print("bootstrap_prepared_not_service_ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
