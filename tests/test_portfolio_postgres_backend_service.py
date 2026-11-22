"""Opt-in, real PostgreSQL foundation evidence; never the default database.

Set NEXUS_PORTFOLIO_PG_TEST_CONFIG to a task-owned JSON configuration prepared by
the local acceptance coordinator. Before any mutation the fixture checks explicit
loopback DSNs, database/role/port identity, the server data directory, and a unique
acceptance marker. Every case retains a random nexus_test_* schema for inspection.
No test starts services, creates databases, drops schemas, or contacts a broker.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from nexus.portfolio.postgres_backend import (
    PostgresBackend,
    PostgresBackendError,
    PostgresConfig,
)
from nexus.portfolio.store import PortfolioStore, canonical, digest

pytestmark = pytest.mark.portfolio_service
ROOT = Path(__file__).resolve().parents[1]
CLOCK = 1_800_000_000.1234567


@dataclass(frozen=True)
class Cluster:
    driver: Any = field(repr=False)
    config: dict[str, Any] = field(repr=False)

    def connect(self, role: str = "migration") -> Any:
        return self.driver.connect(self.config[f"{role}_dsn"], connect_timeout=5)

    def backend(self, root: Path, *, schema: str | None = None) -> PostgresBackend:
        return PostgresBackend(
            PostgresConfig(
                dsn=self.config["application_dsn"],
                schema=schema or f"nexus_test_{uuid4().hex}",
                artifact_root=root.resolve(),
            ),
            clock=lambda: CLOCK,
        )

    def migrate(self, backend: PostgresBackend, **kwargs: Any) -> dict[str, Any]:
        return backend.migrate(migration_dsn=self.config["migration_dsn"], **kwargs)

    def query(
        self,
        backend: PostgresBackend,
        statement: str,
        parameters: tuple[Any, ...] = (),
        *,
        role: str = "migration",
    ) -> list[Any]:
        query = self.driver.sql.SQL(statement).format(
            schema=self.driver.sql.Identifier(backend.config.schema)
        )
        with self.connect(role) as connection:
            cursor = connection.execute(query, parameters)
            return cursor.fetchall() if cursor.description is not None else []


@pytest.fixture(scope="module")
def cluster() -> Cluster:
    location = os.environ.get("NEXUS_PORTFOLIO_PG_TEST_CONFIG")
    if not location:
        pytest.skip("No explicitly isolated portfolio PostgreSQL acceptance configuration")
    config_path = Path(location)
    if not config_path.is_absolute() or not config_path.is_file():
        pytest.fail(
            "Portfolio PostgreSQL acceptance configuration must be an existing absolute file"
        )
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    database = config["expected_database"]
    port = int(config["expected_port"])
    assert database.startswith("nexus_portfolio_test_"), "Refusing a non-acceptance database"
    assert 1024 < port < 65536 and port != 5432, "Refusing the default PostgreSQL endpoint"
    try:
        driver = importlib.import_module("psycopg")
        conninfo_to_dict = importlib.import_module("psycopg.conninfo").conninfo_to_dict
    except ImportError:
        pytest.fail("The opted-in PostgreSQL lane requires the local optional psycopg dependency")
    users = []
    for role in ("application", "migration", "admin"):
        parsed = conninfo_to_dict(config[f"{role}_dsn"])
        required = {"host", "port", "dbname", "user", "password"}
        assert required <= set(parsed) <= required | {"connect_timeout"}, "Unsafe DSN overrides"
        assert parsed["host"] == "127.0.0.1", "Acceptance requires an explicit loopback host"
        assert int(parsed["port"]) == port, "Acceptance DSN port mismatch"
        assert parsed["dbname"] == database, "Acceptance DSN database mismatch"
        assert parsed["user"] == config[f"expected_{role}_user"], "Acceptance DSN user mismatch"
        assert parsed.get("connect_timeout", "5") == "5", "Acceptance timeout mismatch"
        assert bool(parsed["password"]), "Acceptance must not fall back to implicit credentials"
        users.append(parsed["user"])
    assert len(set(users)) == 3, "Application, migration and admin identities must differ"
    assert isinstance(config["cluster_id"], str) and len(config["cluster_id"]) >= 16
    expected_data = Path(config["data_directory"]).resolve()
    assert expected_data.is_absolute() and expected_data.is_dir()
    result = Cluster(driver=driver, config=config)
    # These checks are read-only and occur before any generated schema is created.
    with result.connect("admin") as connection:
        row = connection.execute(
            "SELECT current_database(), current_user, inet_server_port(), "
            "current_setting('data_directory')"
        ).fetchone()
        assert row[:3] == (database, config["expected_admin_user"], port)
        assert Path(row[3]).resolve() == expected_data, "Server data directory is not task-owned"
        identities = connection.execute(
            "SELECT cluster_id FROM public.nexus_acceptance_identity"
        ).fetchall()
        assert identities == [(config["cluster_id"],)], "Acceptance cluster identity mismatch"
    return result


@pytest.fixture
def backend(cluster: Cluster, tmp_path: Path) -> PostgresBackend:
    result = cluster.backend(tmp_path / "explicit-artifacts")
    cluster.migrate(result)
    return result


def seed() -> dict[str, Any]:
    """A constructed workflow fixture, not a source-backed forecast claim."""

    return {
        "metadata": {"synthetic": True, "label": "ámbito / 東京", "fraction": 1.25e-12},
        "recommendations": [
            {
                "id": "rec-z",
                "sku_id": "SKU-001",
                "location_id": "WH-NORTH",
                "recommended_quantity": 24,
                "unit_cost": 3.5,
                "expected_cost": 84.0,
                "created_by": "planner.custom",
                "state": "ignored-original-state",
                "status": "ignored-original-status",
                "negative_zero": -0.0,
            },
            {
                "id": "rec-a",
                "sku_id": "SKU-002",
                "location_id": "WH-SOUTH",
                "recommended_quantity": 17,
                "unit_cost": 2.75,
                "expected_cost": 46.75,
            },
        ],
        "scenarios": [],
        "forecasts": [],
        "metrics": {},
        "sources": [],
        "lineage": [],
        "artifacts": [],
    }


def append(session: Any, *, label: str = "test") -> Any:
    return session.append_event(
        "rec-z", "foundation.probe", "foundation.test", "system", {"label": label}
    )


def test_migration_reapply_is_idempotent_and_empty_state_is_explicit(
    cluster: Cluster, backend: PostgresBackend
) -> None:
    first = cluster.query(
        backend,
        "SELECT version,migration_sha256,applied_at,application_role "
        "FROM {schema}.schema_migrations ORDER BY version",
    )
    cluster.migrate(backend)
    assert (
        cluster.query(
            backend,
            "SELECT version,migration_sha256,applied_at,application_role "
            "FROM {schema}.schema_migrations ORDER BY version",
        )
        == first
    )
    assert len(first) == 1 and first[0][0] == 2 and len(first[0][1]) == 64
    assert first[0][3] == cluster.config["expected_application_user"]
    assert backend.has_seed() is False
    with pytest.raises(PostgresBackendError) as caught:
        backend.seed()
    assert caught.value.code == "not_ready"


def test_application_cannot_create_schema_or_rewrite_migration_history(
    cluster: Cluster, backend: PostgresBackend
) -> None:
    operations = [
        "CREATE TABLE {schema}.forbidden_table (id INTEGER)",
        "ALTER TABLE {schema}.schema_migrations ADD COLUMN forbidden TEXT",
        "UPDATE {schema}.schema_migrations SET migration_sha256 = repeat('f',64)",
        "DELETE FROM {schema}.schema_migrations",
        "TRUNCATE {schema}.schema_migrations",
        "INSERT INTO {schema}.schema_migrations VALUES (99,repeat('a',64),0,'intruder')",
    ]
    for statement in operations:
        with pytest.raises(cluster.driver.errors.InsufficientPrivilege):
            cluster.query(backend, statement, role="application")
    with cluster.connect("application") as connection:
        role = connection.execute(
            "SELECT rolsuper,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=current_user"
        ).fetchone()
        assert role == (False, False, False)
        can_create = connection.execute(
            "SELECT has_database_privilege(current_user,current_database(),'CREATE')"
        ).fetchone()[0]
        assert can_create is False
    assert backend.has_seed() is False


def test_changed_migration_bytes_and_owner_history_tampering_fail_closed(
    cluster: Cluster, backend: PostgresBackend, tmp_path: Path
) -> None:
    migration = ROOT / "infra/postgres/migrations/002_portfolio_ops.sql"
    changed = tmp_path / "002_portfolio_ops.sql"
    changed.write_bytes(migration.read_bytes() + b"\n-- deliberate checksum mismatch\n")
    with pytest.raises(PostgresBackendError) as caught:
        cluster.migrate(backend, migration_path=changed)
    assert caught.value.code == "migration_conflict"
    # The privileged negative fixture changes only this test's owned schema.
    cluster.query(backend, "ALTER TABLE {schema}.schema_migrations DISABLE TRIGGER USER")
    cluster.query(backend, "UPDATE {schema}.schema_migrations SET migration_sha256=repeat('f',64)")
    cluster.query(backend, "ALTER TABLE {schema}.schema_migrations ENABLE TRIGGER USER")
    for action in (backend.has_seed, lambda: cluster.migrate(backend)):
        with pytest.raises(PostgresBackendError) as tampered:
            action()
        assert tampered.value.code in {"migration_conflict", "integrity_error"}


def test_unknown_existing_schema_is_not_adopted_or_overwritten(
    cluster: Cluster, tmp_path: Path
) -> None:
    backend = cluster.backend(tmp_path / "unknown-schema")
    cluster.query(backend, "CREATE SCHEMA {schema}")
    cluster.query(backend, "CREATE TABLE {schema}.foreign_table (id INTEGER)")
    cluster.query(backend, "INSERT INTO {schema}.foreign_table VALUES (73)")
    with pytest.raises(PostgresBackendError) as caught:
        cluster.migrate(backend)
    assert caught.value.code == "migration_conflict"
    assert cluster.query(backend, "SELECT id FROM {schema}.foreign_table") == [(73,)]


def test_concurrent_first_migration_retains_one_checksum_ledger(
    cluster: Cluster, tmp_path: Path
) -> None:
    backend = cluster.backend(tmp_path / "concurrent-migration")
    barrier = threading.Barrier(6)

    def initialize(_: int) -> None:
        barrier.wait(timeout=15)
        cluster.migrate(backend)

    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(initialize, range(6)))
    assert cluster.query(backend, "SELECT count(*) FROM {schema}.schema_migrations") == [(1,)]
    assert backend.has_seed() is False


def test_seed_payload_float_timestamp_and_event_hash_match_sqlite_exactly(
    backend: PostgresBackend, tmp_path: Path
) -> None:
    initial = seed()
    sqlite = PortfolioStore(tmp_path / "sqlite-parity", clock=lambda: CLOCK)
    sqlite.ensure_seed(initial)
    backend.ensure_seed(initial)
    snapshot = backend.snapshot()
    with sqlite.connection() as connection:
        recommendations = [
            dict(row) for row in connection.execute("SELECT * FROM recommendations ORDER BY id")
        ]
        events = [dict(row) for row in connection.execute("SELECT * FROM events ORDER BY sequence")]
        audit_head = connection.execute(
            "SELECT value FROM metadata WHERE key='audit_head'"
        ).fetchone()[0]
    assert snapshot["seed"] == initial == sqlite.seed()
    assert snapshot["seed_sha256"] == digest(initial)
    assert snapshot["recommendations"] == recommendations
    assert snapshot["events"] == events
    assert snapshot["audit_head"] == audit_head
    assert snapshot["event_count"] == 2
    assert [event["recommendation_id"] for event in snapshot["events"]] == ["rec-z", "rec-a"]
    for row in snapshot["recommendations"]:
        assert row["created_at"] == CLOCK
        assert row["fingerprint"] == digest(json.loads(row["payload"]))
        assert row["payload"] == canonical(json.loads(row["payload"]))
        assert "status" not in json.loads(row["payload"])
        assert "state" not in json.loads(row["payload"])
    assert snapshot["events"][0]["actor_id"] == "planner.custom"
    assert snapshot["events"][1]["actor_id"] == "planner.r4a"


def test_concurrent_bootstrap_has_one_seed_and_retry_does_not_extend_expiry(
    cluster: Cluster, backend: PostgresBackend
) -> None:
    initial = seed()
    barrier = threading.Barrier(8)

    def initialize(index: int) -> None:
        independent = PostgresBackend(backend.config, clock=lambda: CLOCK + index)
        barrier.wait(timeout=15)
        independent.ensure_seed(initial)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(initialize, range(8)))
    before = backend.snapshot()
    assert before["event_count"] == 2 and len(before["recommendations"]) == 2
    assert cluster.query(backend, "SELECT count(*) FROM {schema}.metadata WHERE key='seed'") == [
        (1,)
    ]
    PostgresBackend(backend.config, clock=lambda: CLOCK + 9999).ensure_seed(initial)
    assert backend.snapshot() == before
    changed = seed()
    changed["metadata"]["new_revision"] = 2
    with pytest.raises(PostgresBackendError) as caught:
        backend.ensure_seed(changed)
    assert caught.value.code == "seed_changed"
    assert backend.snapshot() == before


def test_rollback_releases_guard_and_leaves_no_audit_gap(backend: PostgresBackend) -> None:
    backend.ensure_seed(seed())
    initial = backend.snapshot()
    with (
        pytest.raises(RuntimeError, match="injected rollback"),
        backend.write_transaction() as session,
    ):
        append(session, label="must-roll-back")
        assert session.snapshot()["event_count"] == 3
        raise RuntimeError("injected rollback")
    assert backend.snapshot() == initial
    with backend.write_transaction() as session:
        append(session, label="committed-after-rollback")
    after = backend.snapshot()
    assert [row["sequence"] for row in after["events"]] == [1, 2, 3]
    assert after["events"][-1]["previous_hash"] == initial["audit_head"]
    assert after["audit_head"] == after["events"][-1]["event_hash"]
    assert json.loads(after["events"][-1]["details"]) == {"label": "committed-after-rollback"}


def test_concurrent_writers_retain_gapless_single_chain(backend: PostgresBackend) -> None:
    backend.ensure_seed(seed())
    barrier = threading.Barrier(8)

    def write(index: int) -> None:
        barrier.wait(timeout=15)
        with backend.write_transaction() as session:
            append(session, label=f"writer-{index}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(8)))
    snapshot = backend.snapshot()
    assert snapshot["event_count"] == 10
    assert [event["sequence"] for event in snapshot["events"]] == list(range(1, 11))
    previous = "0" * 64
    for event in snapshot["events"]:
        assert event["previous_hash"] == previous
        material = {key: value for key, value in event.items() if key != "event_hash"}
        material["details"] = json.loads(material["details"])
        assert digest(material) == event["event_hash"]
        previous = event["event_hash"]
    assert previous == snapshot["audit_head"]


def test_repeatable_read_snapshot_does_not_mix_committed_generations(
    backend: PostgresBackend,
) -> None:
    backend.ensure_seed(seed())
    with backend.read_snapshot() as reader:
        before = reader.snapshot()
        with backend.write_transaction() as writer:
            append(writer, label="committed-during-reader")
        assert reader.snapshot() == before
    assert backend.snapshot()["event_count"] == before["event_count"] + 1


def test_read_session_is_read_only_and_cannot_escape_its_lifetime(
    backend: PostgresBackend,
) -> None:
    backend.ensure_seed(seed())
    with backend.read_snapshot() as reader:
        assert not hasattr(reader, "append_event")
        assert not hasattr(reader, "execute")
        assert reader.has_seed() is True
        assert reader.seed() == seed()
        # Inspect the actual database settings, not just the Python interface.
        assert reader._db.execute("SHOW transaction_read_only").fetchone() == {
            "transaction_read_only": "on"
        }
        assert reader._db.execute("SHOW transaction_isolation").fetchone() == {
            "transaction_isolation": "repeatable read"
        }
    with pytest.raises(PostgresBackendError) as caught:
        reader.snapshot()
    assert caught.value.code == "session_closed"


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE {schema}.metadata SET value=repeat('f',64) WHERE key='seed_hash'",
        "UPDATE {schema}.events SET details='{{}}' WHERE sequence=1",
        "UPDATE {schema}.recommendations SET fingerprint=repeat('f',64) WHERE id='rec-z'",
        "UPDATE {schema}.workflow_guard SET event_count=event_count+1 WHERE singleton=1",
    ],
)
def test_retained_integrity_tamper_is_rejected_even_on_identical_seed_retry(
    cluster: Cluster, backend: PostgresBackend, statement: str
) -> None:
    backend.ensure_seed(seed())
    if ".events " in statement:
        cluster.query(backend, "ALTER TABLE {schema}.events DISABLE TRIGGER USER")
    cluster.query(backend, statement)
    if ".events " in statement:
        cluster.query(backend, "ALTER TABLE {schema}.events ENABLE TRIGGER USER")
    for read in (backend.snapshot, lambda: backend.ensure_seed(seed())):
        with pytest.raises(PostgresBackendError) as caught:
            read()
        assert caught.value.code == "integrity_error"


def test_failed_first_seed_is_atomic_and_the_next_seed_can_succeed(
    backend: PostgresBackend,
) -> None:
    broken = seed()
    broken["recommendations"][1]["id"] = "rec-z"
    with pytest.raises(PostgresBackendError):
        backend.ensure_seed(broken)
    assert backend.has_seed() is False
    backend.ensure_seed(seed())
    assert backend.snapshot()["event_count"] == 2
    assert [event["sequence"] for event in backend.snapshot()["events"]] == [1, 2]


def test_explicit_artifact_roots_and_schemas_are_isolated(
    cluster: Cluster, backend: PostgresBackend, tmp_path: Path
) -> None:
    other = cluster.backend(tmp_path / "other-artifacts")
    cluster.migrate(other)
    backend.ensure_seed(seed())
    assert backend.has_seed() is True
    assert other.has_seed() is False
    assert backend.config.artifact_root != other.config.artifact_root
    assert not backend.config.artifact_root.exists()
    assert not other.config.artifact_root.exists()


def test_empty_seed_is_valid_without_invented_audit_events(backend: PostgresBackend) -> None:
    empty = seed()
    empty["recommendations"] = []
    backend.ensure_seed(empty)
    snapshot = backend.snapshot()
    assert snapshot["seed"] == empty
    assert snapshot["events"] == [] and snapshot["recommendations"] == []
    assert snapshot["event_count"] == 0 and snapshot["audit_head"] == "0" * 64
    backend.ensure_seed(empty)
    assert backend.snapshot() == snapshot


def test_foundation_probe_cannot_masquerade_as_a_business_approval(
    backend: PostgresBackend,
) -> None:
    backend.ensure_seed(seed())
    before = backend.snapshot()
    with pytest.raises(PostgresBackendError), backend.write_transaction() as session:
        session.append_event("rec-z", "approved", "fake.approver", "approver", {})
    assert backend.snapshot() == before


def test_seed_artifacts_are_bound_to_explicit_root_and_rechecked_on_retry(
    backend: PostgresBackend,
) -> None:
    root = backend.config.artifact_root
    root.mkdir()
    source = root / "evidence.json"
    original = b'{"fixture":"constructed foundation evidence"}\n'
    source.write_bytes(original)
    initial = seed()
    initial["artifacts"] = [
        {"path": "evidence.json", "file_sha256": hashlib.sha256(source.read_bytes()).hexdigest()}
    ]
    backend.ensure_seed(initial)
    before = backend.snapshot()
    source.write_bytes(b'{"fixture":"tampered evidence"}\n')
    for action in (backend.snapshot, lambda: backend.ensure_seed(initial)):
        with pytest.raises(PostgresBackendError) as caught:
            action()
        assert caught.value.code == "integrity_error"
    source.write_bytes(original)
    assert backend.snapshot() == before


@pytest.mark.parametrize(
    "statement",
    [
        "GRANT CREATE ON SCHEMA {schema} TO {application_role}",
        "GRANT UPDATE ON {schema}.schema_migrations TO {application_role}",
    ],
)
def test_runtime_refuses_newly_excessive_application_grants(
    cluster: Cluster, backend: PostgresBackend, statement: str
) -> None:
    query = cluster.driver.sql.SQL(statement).format(
        schema=cluster.driver.sql.Identifier(backend.config.schema),
        application_role=cluster.driver.sql.Identifier(cluster.config["expected_application_user"]),
    )
    with cluster.connect() as connection:
        connection.execute(query)
    with pytest.raises(PostgresBackendError) as caught:
        backend.has_seed()
    assert caught.value.code == "privilege_denied"


@pytest.mark.parametrize(
    "statement",
    [
        "ALTER TABLE {schema}.events DROP CONSTRAINT events_recommendation_id_fkey",
        "ALTER TABLE {schema}.workflow_guard DROP CONSTRAINT workflow_guard_event_count_check",
        "CREATE OR REPLACE TRIGGER immutable_event_history BEFORE DELETE ON {schema}.events "
        "FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation()",
    ],
)
def test_schema_constraint_and_trigger_drift_cannot_hide_behind_unchanged_ledger(
    cluster: Cluster, backend: PostgresBackend, statement: str
) -> None:
    # Only the independently generated test schema is altered; no user schema is touched.
    cluster.query(backend, statement)
    for action in (backend.has_seed, lambda: cluster.migrate(backend)):
        with pytest.raises(PostgresBackendError) as caught:
            action()
        assert caught.value.code == "migration_conflict"
