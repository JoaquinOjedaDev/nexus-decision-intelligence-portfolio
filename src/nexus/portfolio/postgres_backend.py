"""Explicit PostgreSQL operational foundation; not a PortfolioStore replacement.

The optional driver is loaded only at an explicit database operation. Bootstrap
uses separate credentials, while runtime connections cannot migrate. The small
unit of work intentionally exposes no SQLite-compatible ``execute`` surface.
Initial seed persistence is trusted infrastructure, not an approval operation or
independent verification of upstream scientific/source authority.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import math
import re
import shlex
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, cast
from urllib.parse import parse_qsl, unquote, urlsplit

from nexus.portfolio.store import RECOMMENDATION_TTL_SECONDS, canonical, digest, iso

MIGRATION_VERSION = 2
ZERO_HASH = "0" * 64
_NAME = re.compile(r"nexus_[a-z0-9_]{1,57}\Z")
_TABLES = {"schema_migrations", "metadata", "recommendations", "events", "workflow_guard"}
_MIGRATION = Path(__file__).resolve().parents[3] / "infra/postgres/migrations/002_portfolio_ops.sql"
_ALLOWED_DSN_KEYS = {
    "host",
    "hostaddr",
    "port",
    "dbname",
    "user",
    "password",
    "sslmode",
    "connect_timeout",
}
_COLUMNS = {
    "schema_migrations": {
        "version": "integer",
        "migration_sha256": "text",
        "applied_at": "double precision",
        "application_role": "text",
    },
    "metadata": {"key": "text", "value": "text"},
    "workflow_guard": {"singleton": "integer", "event_count": "bigint", "audit_head": "text"},
    "events": {
        "sequence": "bigint",
        "recommendation_id": "text",
        "event_type": "text",
        "actor_id": "text",
        "actor_role": "text",
        "occurred_at": "double precision",
        "details": "text",
        "previous_hash": "text",
        "event_hash": "text",
    },
    "recommendations": {
        "id": "text",
        "payload": "text",
        "fingerprint": "text",
        "location_id": "text",
        "creator": "text",
        "state": "text",
        "created_at": "double precision",
        "valid_until": "double precision",
        "approval_actor": "text",
        "approval_fingerprint": "text",
        "approval_expires_at": "double precision",
        "execution_ref": "text",
        "executed_at": "double precision",
        "outcome_due_at": "double precision",
        "outcome_json": "text",
    },
}
_NULLABLE = {
    "approval_actor",
    "approval_fingerprint",
    "approval_expires_at",
    "execution_ref",
    "executed_at",
    "outcome_due_at",
    "outcome_json",
}


class PostgresBackendError(ValueError):
    """Safe error without connection strings, passwords or driver diagnostics."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _refuse(code: str, message: str) -> None:
    raise PostgresBackendError(code, message)


def _dsn(value: str) -> dict[str, str]:
    """Parse a deliberately bounded local connection contract without importing psycopg.

    Requiring every routing field prevents environment/PGSERVICE fallbacks from
    accidentally reaching a user's existing database. libpq receives parsed
    keyword arguments, never an unvalidated string with options or multi-hosts.
    """
    try:
        if not isinstance(value, str) or not value or len(value) > 8192:
            raise ValueError
        if value.startswith(("postgresql://", "postgres://")):
            parsed = urlsplit(value)
            result = {
                "host": parsed.hostname or "",
                "port": str(parsed.port or ""),
                "dbname": unquote(parsed.path.lstrip("/")),
                "user": unquote(parsed.username or ""),
            }
            if parsed.password is not None:
                result["password"] = unquote(parsed.password)
            for key, item in parse_qsl(parsed.query, keep_blank_values=True):
                if key in result:
                    raise ValueError
                result[key] = item
            if parsed.fragment:
                raise ValueError
        else:
            result = {}
            for token in shlex.split(value):
                key, separator, item = token.partition("=")
                if not separator or key in result:
                    raise ValueError
                result[key] = item
        if set(result) - _ALLOWED_DSN_KEYS or any(
            not result.get(key) for key in ("host", "port", "dbname", "user")
        ):
            raise ValueError
        if result["host"] not in {"127.0.0.1", "::1", "localhost"}:
            raise ValueError
        if result.get("hostaddr", result["host"]) not in {"127.0.0.1", "::1", "localhost"}:
            raise ValueError
        if not result["port"].isdigit() or not 1 <= int(result["port"]) <= 65535:
            raise ValueError
        if any("\x00" in item for item in result.values()):
            raise ValueError
        if "connect_timeout" in result and (
            not result["connect_timeout"].isdigit() or not 1 <= int(result["connect_timeout"]) <= 30
        ):
            raise ValueError
        result["hostaddr"] = result.get("hostaddr", result["host"]).replace(
            "localhost", "127.0.0.1"
        )
        return result
    except (ValueError, TypeError, AttributeError):
        _refuse("configuration_error", "An explicit local PostgreSQL connection is required")
    raise AssertionError("unreachable")


def _driver() -> Any:
    try:
        return importlib.import_module("psycopg")
    except ImportError:
        raise PostgresBackendError(
            "dependency_missing", "PostgreSQL requires the optional psycopg service dependency"
        ) from None


@dataclass(frozen=True)
class PostgresConfig:
    dsn: str = field(repr=False)
    schema: str
    artifact_root: Path
    connect_timeout_seconds: int = 5

    def __post_init__(self) -> None:
        _dsn(self.dsn)
        if not isinstance(self.schema, str) or not _NAME.fullmatch(self.schema):
            _refuse("configuration_error", "Use a task-owned nexus_ PostgreSQL schema name")
        if (
            not isinstance(self.artifact_root, Path)
            or not self.artifact_root.is_absolute()
            or self.artifact_root == Path(self.artifact_root.anchor)
            or self.artifact_root.anchor.startswith(("\\\\", "//"))
        ):
            _refuse("configuration_error", "An explicit absolute artifact directory is required")
        if (
            type(self.connect_timeout_seconds) is not int
            or not 1 <= self.connect_timeout_seconds <= 30
        ):
            _refuse("configuration_error", "Connection timeout must be between 1 and 30 seconds")


class PostgresBackend:
    """Task-owned local PostgreSQL foundation with explicit transaction boundaries."""

    _schema_tables: ClassVar[set[str]] = _TABLES
    _migration_version: ClassVar[int] = MIGRATION_VERSION
    _schema_columns: ClassVar[dict[str, dict[str, str]]] = _COLUMNS
    _nullable_columns: ClassVar[set[tuple[str, str]]] = {
        ("recommendations", name) for name in _NULLABLE
    }
    _uniques: ClassVar[dict[tuple[str, str], list[int]]] = {}
    _foreign_keys: ClassVar[dict[tuple[str, str], tuple[list[int], str, list[int]]]] = {
        ("events", "events_recommendation_id_fkey"): ([2], "recommendations", [1])
    }
    _history_triggers: ClassVar[dict[str, str]] = {
        "schema_migrations": "immutable_migration_history",
        "events": "immutable_event_history",
    }
    _forbidden_grants: ClassVar[dict[str, str]] = {
        "events": "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER",
        "recommendations": "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER",
        "workflow_guard": "INSERT,DELETE,TRUNCATE,REFERENCES,TRIGGER",
        "metadata": "DELETE,TRUNCATE,REFERENCES,TRIGGER",
    }

    def __init__(self, config: PostgresConfig, *, clock: Callable[[], float] = time.time) -> None:
        self.config = config
        self.artifact_root = config.artifact_root
        self.clock = clock

    def _verify_artifacts(self, seed: dict[str, Any]) -> None:
        """Check declared file bytes only; upstream source/scientific authority is separate."""
        root = self.artifact_root
        if root.anchor.startswith(("\\\\", "//")):
            raise ValueError("Network artifact roots are not supported")
        for ancestor in (*reversed(root.parents), root):
            if ancestor.is_symlink() or ancestor.is_junction():
                raise ValueError("Linked artifact roots are not supported")
        resolved_root = root.resolve()
        seen: set[str] = set()
        for artifact in seed.get("artifacts", []):
            relative = artifact["path"]
            candidate = Path(relative)
            if (
                not relative
                or relative in seen
                or candidate.is_absolute()
                or candidate.drive
                or ".." in candidate.parts
            ):
                raise ValueError("Artifact path is not a unique relative local path")
            path = root
            for component in candidate.parts:
                path = path / component
                if path.is_symlink() or path.is_junction():
                    raise ValueError("Linked artifacts are not supported")
            if (
                not path.resolve().is_relative_to(resolved_root)
                or not path.is_file()
                or hashlib.sha256(path.read_bytes()).hexdigest() != artifact["file_sha256"]
            ):
                raise ValueError("Artifact checksum mismatch")
            seen.add(relative)

    @contextmanager
    def _connection(self, parameters: dict[str, str]) -> Iterator[Any]:
        driver = _driver()
        try:
            with driver.connect(
                **{key: value for key, value in parameters.items() if key != "connect_timeout"},
                connect_timeout=self.config.connect_timeout_seconds,
                application_name="nexus-portfolio-foundation",
                # Override PGOPTIONS; caller/environment search paths or SET ROLE
                # must not influence the explicitly qualified foundation.
                options="-c search_path=pg_catalog",
                autocommit=True,
                row_factory=importlib.import_module("psycopg.rows").dict_row,
            ) as db:
                identity = db.execute(
                    "SELECT current_user,current_database(),inet_server_port() AS port"
                ).fetchone()
                if (
                    identity["current_user"] != parameters["user"]
                    or identity["current_database"] != parameters["dbname"]
                    or identity["port"] != int(parameters["port"])
                ):
                    _refuse("configuration_error", "PostgreSQL endpoint identity differs")
                yield db
        except driver.Error:
            raise PostgresBackendError(
                "database_unavailable", "PostgreSQL operation failed; inspect local service health"
            ) from None

    def _table(self, name: str) -> Any:
        return _driver().sql.Identifier(self.config.schema, name)

    def _query(self, db: Any, query: str, parameters: Any = None, **names: str) -> Any:
        sql = _driver().sql
        statement = sql.SQL(query).format(
            **{key: self._table(value) for key, value in names.items()}
        )
        return db.execute(statement, parameters)

    @staticmethod
    def _migration_bytes(path: Path | None = None) -> tuple[str, str]:
        try:
            if path is None:
                from nexus.portfolio.postgres_migrations import read_migration_sql

                raw = read_migration_sql(2)
            else:
                raw = path.read_bytes()
            return raw.decode("utf-8"), hashlib.sha256(raw).hexdigest()
        except (OSError, UnicodeError, ValueError):
            raise PostgresBackendError(
                "migration_conflict", "The explicit portfolio migration artifact is unavailable"
            ) from None

    def _verify_schema(self, db: Any, expected_hash: str, application_role: str) -> None:
        relations = db.execute(
            "SELECT c.relname,c.relkind FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s",
            (self.config.schema,),
        ).fetchall()
        expected = {(name, "r") for name in self._schema_tables}
        expected |= {(name + "_pkey", "i") for name in self._schema_tables}
        expected |= {(name, "i") for _, name in self._uniques}
        if {(row["relname"], row["relkind"]) for row in relations} != expected:
            _refuse("migration_conflict", "Portfolio schema contains unknown or missing relations")
        functions = db.execute(
            "SELECT p.proname,p.prosrc,p.prosecdef,p.proconfig,l.lanname FROM pg_catalog.pg_proc p "
            "JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace "
            "JOIN pg_catalog.pg_language l ON l.oid=p.prolang WHERE n.nspname=%s",
            (self.config.schema,),
        ).fetchall()
        if [row["proname"] for row in functions] != ["reject_history_mutation"]:
            _refuse("migration_conflict", "Portfolio schema function inventory differs")
        function = functions[0]
        expected_source = (
            "BEGIN RAISE EXCEPTION 'Immutable portfolio history cannot be rewritten'; END;"
        )
        if (
            " ".join(function["prosrc"].split()) != expected_source
            or function["prosecdef"]
            or function["lanname"] != "plpgsql"
            or function["proconfig"] != ["search_path=pg_catalog"]
        ):
            _refuse("migration_conflict", "Immutable history function differs from its migration")
        columns = db.execute(
            "SELECT c.relname,a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod) AS type,"
            "a.attnotnull FROM pg_catalog.pg_attribute a "
            "JOIN pg_catalog.pg_class c ON c.oid=a.attrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped",
            (self.config.schema,),
        ).fetchall()
        expected_columns = {
            (table, name, kind, (table, name) not in self._nullable_columns)
            for table, specification in self._schema_columns.items()
            for name, kind in specification.items()
        }
        if {
            (row["relname"], row["attname"], row["type"], row["attnotnull"]) for row in columns
        } != expected_columns:
            _refuse("migration_conflict", "Portfolio schema column contract differs")
        triggers = db.execute(
            "SELECT c.relname,t.tgname,t.tgenabled,p.proname,pn.nspname AS function_schema, "
            "t.tgtype,t.tgnargs,t.tgqual,t.tgconstraint FROM pg_catalog.pg_trigger t "
            "JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid "
            "JOIN pg_catalog.pg_namespace pn ON pn.oid=p.pronamespace "
            "WHERE n.nspname=%s AND NOT t.tgisinternal",
            (self.config.schema,),
        ).fetchall()
        if {
            (row["relname"], row["tgname"], row["tgenabled"], row["proname"]) for row in triggers
        } != {
            (table, name, "O", "reject_history_mutation")
            for table, name in self._history_triggers.items()
        }:
            _refuse("migration_conflict", "Immutable history protections differ from the migration")
        if any(
            row["function_schema"] != self.config.schema
            or row["tgtype"] != 58
            or row["tgnargs"] != 0
            or row["tgqual"] is not None
            or row["tgconstraint"] != 0
            for row in triggers
        ):
            _refuse("migration_conflict", "Immutable history trigger definitions differ")
        self._verify_constraints(db)
        self._verify_migration_history(db, expected_hash, application_role)

    def _verify_migration_history(self, db: Any, expected_hash: str, application_role: str) -> None:
        rows = self._query(db, "SELECT * FROM {table}", table="schema_migrations").fetchall()
        if (
            len(rows) != 1
            or rows[0]["version"] != self._migration_version
            or rows[0]["migration_sha256"] != expected_hash
            or rows[0]["application_role"] != application_role
            or not math.isfinite(rows[0]["applied_at"])
        ):
            _refuse("migration_conflict", "Portfolio migration history conflicts with its artifact")

    def _verify_constraints(self, db: Any) -> None:
        constraints = db.execute(
            "SELECT c.relname,k.conname,k.contype,k.convalidated,k.condeferrable, "
            "k.conkey,k.confkey,k.confupdtype,k.confdeltype,k.confmatchtype, "
            "r.relname AS reference_table,rn.nspname AS reference_schema, "
            "pg_catalog.pg_get_expr(k.conbin,k.conrelid) AS expression "
            "FROM pg_catalog.pg_constraint k JOIN pg_catalog.pg_class c ON c.oid=k.conrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "LEFT JOIN pg_catalog.pg_class r ON r.oid=k.confrelid "
            "LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid=r.relnamespace "
            # PostgreSQL 18 also catalogs NOT NULL; attnotnull is checked above
            # for both PostgreSQL 17 and 18, independent of generated names.
            "WHERE n.nspname=%s AND k.contype <> 'n'",
            (self.config.schema,),
        ).fetchall()
        checks = self._constraint_checks()
        expected = {(table, table + "_pkey", "p") for table in self._schema_tables}
        expected |= {(table, name, "c") for table, name in checks}
        expected |= {(table, name, "u") for table, name in self._uniques}
        expected |= {(table, name, "f") for table, name in self._foreign_keys}
        if {(row["relname"], row["conname"], row["contype"]) for row in constraints} != expected:
            _refuse("migration_conflict", "Portfolio constraint inventory differs")
        for row in constraints:
            valid = row["convalidated"] and not row["condeferrable"]
            if row["contype"] == "p":
                valid = valid and row["conkey"] == [1]
            elif row["contype"] == "u":
                valid = valid and row["conkey"] == self._uniques[(row["relname"], row["conname"])]
            elif row["contype"] == "c":
                column, expression = checks[(row["relname"], row["conname"])]
                normalized = row["expression"].translate(str.maketrans("", "", " ()"))
                valid = valid and row["conkey"] == [column] and normalized == expression
            else:
                keys, table, references = self._foreign_keys[(row["relname"], row["conname"])]
                valid = (
                    valid
                    and row["conkey"] == keys
                    and row["confkey"] == references
                    and row["reference_table"] == table
                    and row["reference_schema"] == self.config.schema
                    and row["confupdtype"] == "a"
                    and row["confdeltype"] == "a"
                    and row["confmatchtype"] == "s"
                )
            if not valid:
                _refuse("migration_conflict", "Portfolio constraint definition differs")

    def _constraint_checks(self) -> dict[tuple[str, str], tuple[int, str]]:
        return {
            ("schema_migrations", "schema_migrations_migration_sha256_check"): (
                2,
                "migration_sha256~'^[0-9a-f]{64}$'::text",
            ),
            ("events", "events_sequence_check"): (1, "sequence>0"),
            ("workflow_guard", "workflow_guard_singleton_check"): (1, "singleton=1"),
            ("workflow_guard", "workflow_guard_event_count_check"): (2, "event_count>=0"),
            ("workflow_guard", "workflow_guard_audit_head_check"): (
                3,
                "audit_head~'^[0-9a-f]{64}$'::text",
            ),
        }

    def _check_application_role(self, db: Any, role: str) -> None:
        row = db.execute(
            "SELECT rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls "
            "FROM pg_catalog.pg_roles WHERE rolname=%s",
            (role,),
        ).fetchone()
        if row is None or any(row.values()):
            _refuse("privilege_denied", "The application identity must be least privileged")
        ownership = db.execute(
            "SELECT pg_has_role(%s,nspowner,'MEMBER') AS can_become_owner "
            "FROM pg_catalog.pg_namespace WHERE nspname=%s",
            (role, self.config.schema),
        ).fetchone()
        if ownership["can_become_owner"]:
            _refuse("privilege_denied", "The application identity cannot assume the schema owner")
        privileges = db.execute(
            "SELECT has_database_privilege(%s,current_database(),'CREATE') AS database_create, "
            "has_schema_privilege(%s,%s,'CREATE') AS schema_create",
            (role, role, self.config.schema),
        ).fetchone()
        if any(privileges.values()):
            _refuse("privilege_denied", "The application identity cannot own or create schema")
        mutable = db.execute(
            "SELECT has_table_privilege(%s,%s,"
            "'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AS edit",
            (role, self._table("schema_migrations").as_string(db)),
        ).fetchone()
        if mutable["edit"]:
            _refuse("privilege_denied", "The application identity cannot alter migration history")
        for table, forbidden in self._forbidden_grants.items():
            excessive = db.execute(
                "SELECT has_table_privilege(%s,%s,%s) AS edit",
                (role, self._table(table).as_string(db), forbidden),
            ).fetchone()
            if excessive["edit"]:
                _refuse("privilege_denied", "Application grants exceed the foundation contract")

    def migrate(self, *, migration_dsn: str, migration_path: Path | None = None) -> dict[str, Any]:
        """Apply once under a transaction advisory lock; never create users/databases."""
        application = _dsn(self.config.dsn)
        migration = _dsn(migration_dsn)
        if (
            any(application[key] != migration[key] for key in ("hostaddr", "port", "dbname"))
            or application["user"] == migration["user"]
        ):
            _refuse(
                "configuration_error",
                "Separate bootstrap identity on the same database is required",
            )
        template, checksum = self._migration_bytes(migration_path)
        sql = _driver().sql
        lock = int.from_bytes(
            hashlib.sha256(self.config.schema.encode()).digest()[:8], "big", signed=True
        )
        with self._connection(migration) as db, db.transaction():
            db.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
            owner = db.execute(
                "SELECT pg_get_userbyid(nspowner) AS owner "
                "FROM pg_catalog.pg_namespace WHERE nspname=%s",
                (self.config.schema,),
            ).fetchone()
            created = owner is None
            if created:
                db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.config.schema)))
                db.execute(
                    sql.SQL(template).format(
                        schema=sql.Identifier(self.config.schema),
                        application_role=sql.Identifier(application["user"]),
                    )
                )
                self._query(
                    db,
                    "INSERT INTO {table} VALUES (%s,%s,%s,%s)",
                    (self._migration_version, checksum, float(self.clock()), application["user"]),
                    table="schema_migrations",
                )
            elif owner["owner"] != migration["user"]:
                _refuse(
                    "migration_conflict", "Existing schema is not owned by this bootstrap identity"
                )
            self._verify_schema(db, checksum, application["user"])
            self._check_application_role(db, application["user"])
            version = db.execute("SHOW server_version").fetchone()["server_version"]
            return {
                "version": self._migration_version,
                "migration_sha256": checksum,
                "schema": self.config.schema,
                "applied": created,
                "server_version": version,
            }

    @contextmanager
    def _session(self, *, write: bool) -> Iterator[PostgresReadSession]:
        application = _dsn(self.config.dsn)
        _, checksum = self._migration_bytes()
        with self._connection(application) as db, db.transaction():
            db.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                if not write
                else "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
            )
            if not write:
                db.execute("SET TRANSACTION READ ONLY")
            self._verify_schema(db, checksum, application["user"])
            self._check_application_role(db, application["user"])
            if write:
                guard = self._query(
                    db, "SELECT * FROM {table} WHERE singleton=1 FOR UPDATE", table="workflow_guard"
                ).fetchone()
                if guard is None:
                    _refuse("integrity_error", "Workflow serialization guard is missing")
            session = self._new_session(db, write=write)
            try:
                # Verify before any mutable read; a write retains the guard until commit.
                session.snapshot()
                yield session
                if write:
                    self._finish_write(session)
            finally:
                session._active = False

    def _new_session(self, db: Any, *, write: bool) -> PostgresReadSession:
        return PostgresWriteSession(self, db) if write else PostgresReadSession(self, db)

    def _finish_write(self, session: PostgresReadSession) -> None:
        session.snapshot()

    @contextmanager
    def read_snapshot(self) -> Iterator[PostgresReadSession]:
        with self._session(write=False) as session:
            yield session

    @contextmanager
    def write_transaction(self) -> Iterator[PostgresWriteSession]:
        with self._session(write=True) as session:
            yield cast(PostgresWriteSession, session)

    def has_seed(self) -> bool:
        with self.read_snapshot() as session:
            return session.has_seed()

    def seed(self) -> dict[str, Any]:
        with self.read_snapshot() as session:
            return session.seed()

    def ensure_seed(self, seed: dict[str, Any]) -> None:
        with self.write_transaction() as session:
            session.ensure_seed(seed)

    def snapshot(self) -> dict[str, Any]:
        with self.read_snapshot() as session:
            return session.snapshot()


class PostgresReadSession:
    """Consistent unscoped infrastructure snapshot, not an authenticated API reader."""

    def __init__(self, backend: PostgresBackend, db: Any) -> None:
        self._backend = backend
        self._db = db
        self._active = True

    def _query(self, query: str, parameters: Any = None, **names: str) -> Any:
        if not self._active:
            _refuse("session_closed", "The PostgreSQL unit of work is closed")
        return self._backend._query(self._db, query, parameters, **names)

    def has_seed(self) -> bool:
        return self.snapshot()["seed"] is not None

    def seed(self) -> dict[str, Any]:
        seed = self.snapshot()["seed"]
        if seed is None:
            _refuse("not_ready", "Portfolio seed has not been initialized")
        return cast(dict[str, Any], seed)

    def snapshot(self) -> dict[str, Any]:
        try:
            return self._snapshot()
        except (KeyError, TypeError, ValueError, OverflowError, OSError) as exc:
            if isinstance(exc, PostgresBackendError):
                raise
            raise PostgresBackendError(
                "integrity_error", "Persisted portfolio evidence is invalid"
            ) from None

    def _snapshot(self) -> dict[str, Any]:
        metadata = {
            row["key"]: row["value"]
            for row in self._query("SELECT * FROM {table}", table="metadata").fetchall()
        }
        recommendations = self._query(
            "SELECT * FROM {table} ORDER BY id", table="recommendations"
        ).fetchall()
        events = self._query("SELECT * FROM {table} ORDER BY sequence", table="events").fetchall()
        guards = self._query("SELECT * FROM {table}", table="workflow_guard").fetchall()
        if len(guards) != 1 or guards[0]["singleton"] != 1:
            _refuse("integrity_error", "Workflow serialization guard is invalid")
        previous = ZERO_HASH
        creations: dict[str, dict[str, Any]] = {}
        for index, row in enumerate(events, 1):
            value = dict(row)
            expected = value.pop("event_hash")
            value["details"] = json.loads(value["details"])
            if (
                value["sequence"] != index
                or value["previous_hash"] != previous
                or canonical(value["details"]) != row["details"]
                or digest(value) != expected
            ):
                _refuse("integrity_error", "Audit history failed its integrity check")
            if row["event_type"] == "recommendation_created":
                if row["recommendation_id"] in creations:
                    _refuse("integrity_error", "Duplicate recommendation creation event")
                creations[row["recommendation_id"]] = value
            elif not row["event_type"].startswith("foundation."):
                _refuse(
                    "integrity_error", "A workflow transition is not supported by this foundation"
                )
            previous = expected
        if guards[0]["event_count"] != len(events) or guards[0]["audit_head"] != previous:
            _refuse("integrity_error", "Audit counter or head disagrees with history")
        if not metadata:
            if recommendations or events:
                _refuse("integrity_error", "Unseeded workflow contains business state")
            return {
                "seed": None,
                "seed_sha256": None,
                "recommendations": [],
                "events": [],
                "audit_head": ZERO_HASH,
                "event_count": 0,
            }
        if set(metadata) != {"seed", "seed_hash", "audit_head"}:
            _refuse("integrity_error", "Seed metadata is incomplete or unexpected")
        seed = json.loads(metadata["seed"])
        if canonical(seed) != metadata["seed"] or digest(seed) != metadata["seed_hash"]:
            _refuse("integrity_error", "Stored seed failed its integrity check")
        self._backend._verify_artifacts(seed)
        if metadata["audit_head"] != previous:
            _refuse("integrity_error", "Audit metadata head disagrees with history")
        owners = {item["id"]: item for item in seed["recommendations"]}
        if (
            len(owners) != len(seed["recommendations"])
            or set(owners) != set(creations)
            or set(owners) != {row["id"] for row in recommendations}
        ):
            _refuse("integrity_error", "Seed recommendation ownership is incomplete")
        for row in recommendations:
            original = dict(owners[row["id"]])
            original.pop("state", None)
            original.pop("status", None)
            original.update(created_at=iso(row["created_at"]), valid_until=iso(row["valid_until"]))
            creator = str(original.get("created_by", "planner.r4a"))
            source = (
                "connected runtime baseline"
                if seed.get("metadata", {}).get("pipeline_mode") == "connected_runtime_baseline"
                else "frozen synthetic experiment"
            )
            creation = creations[row["id"]]
            if (
                canonical(original) != row["payload"]
                or digest(original) != row["fingerprint"]
                or row["location_id"] != original["location_id"]
                or row["creator"] != creator
                or row["valid_until"] != row["created_at"] + RECOMMENDATION_TTL_SECONDS
                or row["state"] != "pending"
                or creation["actor_id"] != creator
                or creation["actor_role"] != "planner"
                or creation["details"] != {"fingerprint": row["fingerprint"], "source": source}
                or any(
                    row[key] is not None
                    for key in (
                        "approval_actor",
                        "approval_fingerprint",
                        "approval_expires_at",
                        "execution_ref",
                        "executed_at",
                        "outcome_due_at",
                        "outcome_json",
                    )
                )
            ):
                _refuse(
                    "integrity_error", "Recommendation projection differs from its seed and history"
                )
        return {
            "seed": seed,
            "seed_sha256": metadata["seed_hash"],
            "recommendations": recommendations,
            "events": events,
            "audit_head": previous,
            "event_count": len(events),
        }


class PostgresWriteSession(PostgresReadSession):
    """Guard-serialized trusted initialization and diagnostic audit writes only."""

    def ensure_seed(self, seed: dict[str, Any]) -> None:
        current = self.snapshot()
        try:
            encoded = canonical(seed)
            stable = json.loads(encoded)
            items = stable["recommendations"]
            if not isinstance(items, list) or len({item["id"] for item in items}) != len(items):
                raise ValueError
            for item in items:
                if any(
                    not isinstance(item[key], str) or not item[key] for key in ("id", "location_id")
                ):
                    raise ValueError
                if "created_by" in item and (
                    not isinstance(item["created_by"], str) or not item["created_by"]
                ):
                    raise ValueError
            self._backend._verify_artifacts(stable)
        except (KeyError, TypeError, ValueError, OSError):
            raise PostgresBackendError(
                "invalid_seed", "Seed structure or artifact integrity is invalid"
            ) from None
        if current["seed"] is not None:
            if digest(stable) != current["seed_sha256"]:
                _refuse("seed_changed", "Evidence differs from the persisted workflow")
            return
        now = float(self._backend.clock())
        if not math.isfinite(now):
            _refuse("invalid_seed", "Initialization requires a finite clock")
        for key, value in (
            ("seed", encoded),
            ("seed_hash", digest(stable)),
            ("audit_head", ZERO_HASH),
        ):
            self._query("INSERT INTO {table} VALUES (%s,%s)", (key, value), table="metadata")
        for item in items:
            payload = dict(item)
            payload.pop("status", None)
            payload.pop("state", None)
            payload.update(created_at=iso(now), valid_until=iso(now + RECOMMENDATION_TTL_SECONDS))
            fingerprint = digest(payload)
            creator = str(payload.get("created_by", "planner.r4a"))
            self._query(
                "INSERT INTO {table} "
                "(id,payload,fingerprint,location_id,creator,state,created_at,valid_until) "
                "VALUES (%s,%s,%s,%s,%s,'pending',%s,%s)",
                (
                    payload["id"],
                    canonical(payload),
                    fingerprint,
                    payload["location_id"],
                    creator,
                    now,
                    now + RECOMMENDATION_TTL_SECONDS,
                ),
                table="recommendations",
            )
            self._append_event(
                payload["id"],
                "recommendation_created",
                creator,
                "planner",
                {
                    "fingerprint": fingerprint,
                    "source": (
                        "connected runtime baseline"
                        if stable.get("metadata", {}).get("pipeline_mode")
                        == "connected_runtime_baseline"
                        else "frozen synthetic experiment"
                    ),
                },
            )

    def append_event(
        self,
        recommendation_id: str,
        event_type: str,
        actor_id: str,
        actor_role: str,
        details: dict[str, Any],
    ) -> dict[str, Any]:
        """Append a non-workflow foundation probe; this grants no approval authority."""
        if not isinstance(event_type, str) or not event_type.startswith("foundation."):
            _refuse("invalid_event", "Only foundation diagnostic events are supported")
        self.seed()
        return self._append_event(recommendation_id, event_type, actor_id, actor_role, details)

    def _append_event(
        self,
        recommendation_id: str,
        event_type: str,
        actor_id: str,
        actor_role: str,
        details: dict[str, Any],
    ) -> dict[str, Any]:
        guard = self._query(
            "SELECT * FROM {table} WHERE singleton=1", table="workflow_guard"
        ).fetchone()
        instant = float(self._backend.clock())
        data = {
            "sequence": guard["event_count"] + 1,
            "recommendation_id": recommendation_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "actor_role": actor_role,
            "occurred_at": instant,
            "details": details,
            "previous_hash": guard["audit_head"],
        }
        try:
            event_hash = digest(data)
        except (ValueError, TypeError):
            raise PostgresBackendError(
                "invalid_event", "Event must contain finite canonical JSON"
            ) from None
        self._query(
            "INSERT INTO {table} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                data["sequence"],
                recommendation_id,
                event_type,
                actor_id,
                actor_role,
                instant,
                canonical(details),
                data["previous_hash"],
                event_hash,
            ),
            table="events",
        )
        self._query(
            "UPDATE {table} SET event_count=%s,audit_head=%s WHERE singleton=1",
            (data["sequence"], event_hash),
            table="workflow_guard",
        )
        self._query(
            "UPDATE {table} SET value=%s WHERE key='audit_head'", (event_hash,), table="metadata"
        )
        return {**data, "event_hash": event_hash}
