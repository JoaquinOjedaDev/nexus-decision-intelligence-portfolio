"""Portable PostgreSQL configuration and optional-dependency boundary checks.

These checks never connect to PostgreSQL. Real transaction, privilege and parity
evidence belongs to the separately opted-in portfolio_service test module.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from nexus.portfolio.postgres_backend import (
    PostgresBackend,
    PostgresBackendError,
    PostgresConfig,
)

ROOT = Path(__file__).resolve().parents[1]
DSN = "postgresql://nexus_test_app:portable-only-secret@127.0.0.1:55438/nexus_portfolio_test_unit"


def test_constructor_is_lazy_and_credentials_are_not_in_repr(tmp_path: Path) -> None:
    artifacts = tmp_path / "not-created"
    config = PostgresConfig(dsn=DSN, schema="nexus_test_portable", artifact_root=artifacts)
    backend = PostgresBackend(config)
    assert "portable-only-secret" not in repr(config)
    assert "portable-only-secret" not in repr(backend)
    assert not artifacts.exists()


@pytest.mark.parametrize(
    "schema",
    [
        "",
        "public",
        "nexus_",
        "NEXUS_test",
        "nexus_a.b",
        "nexus_a;DROP TABLE x",
        "nexus_" + "a" * 64,
    ],
)
def test_schema_must_be_an_explicit_owned_identifier(tmp_path: Path, schema: str) -> None:
    with pytest.raises(PostgresBackendError) as caught:
        PostgresConfig(dsn=DSN, schema=schema, artifact_root=tmp_path)
    assert caught.value.code == "configuration_error"
    assert "portable-only-secret" not in str(caught.value)


@pytest.mark.parametrize(
    "dsn",
    [
        "",
        "postgresql:///nexus_portfolio_test_unit",
        "postgresql://127.0.0.1:55438/nexus_portfolio_test_unit",
        "postgresql://nexus_test_app@127.0.0.1/nexus_portfolio_test_unit",
        "postgresql://nexus_test_app@127.0.0.1:55438/",
        "postgresql://nexus_test_app@database.example:55438/nexus_portfolio_test_unit",
        "postgresql://nexus_test_app@0.0.0.0:55438/nexus_portfolio_test_unit",
    ],
)
def test_incomplete_or_nonlocal_dsn_cannot_fall_back_to_environment(
    tmp_path: Path, dsn: str
) -> None:
    with pytest.raises(PostgresBackendError) as caught:
        PostgresConfig(dsn=dsn, schema="nexus_test_portable", artifact_root=tmp_path)
    assert caught.value.code == "configuration_error"


def test_artifact_root_cannot_be_implicit_or_relative() -> None:
    with pytest.raises(PostgresBackendError) as caught:
        PostgresConfig(dsn=DSN, schema="nexus_test_portable", artifact_root=Path("relative"))
    assert caught.value.code == "configuration_error"


@pytest.mark.parametrize("timeout", [0, -1, 31, True])
def test_connect_timeout_is_bounded(tmp_path: Path, timeout: int) -> None:
    with pytest.raises(PostgresBackendError) as caught:
        PostgresConfig(
            dsn=DSN,
            schema="nexus_test_portable",
            artifact_root=tmp_path,
            connect_timeout_seconds=timeout,
        )
    assert caught.value.code == "configuration_error"


@pytest.mark.parametrize(
    "migration_dsn",
    [
        DSN,
        "postgresql://nexus_migrator@127.0.0.1:55439/nexus_portfolio_test_unit",
        "postgresql://nexus_migrator@127.0.0.1:55438/nexus_portfolio_test_other",
    ],
)
def test_migration_requires_distinct_identity_on_identical_endpoint_before_driver_use(
    tmp_path: Path, migration_dsn: str
) -> None:
    backend = PostgresBackend(
        PostgresConfig(dsn=DSN, schema="nexus_test_portable", artifact_root=tmp_path)
    )
    with pytest.raises(PostgresBackendError) as caught:
        backend.migrate(migration_dsn=migration_dsn)
    assert caught.value.code == "configuration_error"


def test_missing_optional_driver_preserves_import_and_sqlite_demo(tmp_path: Path) -> None:
    """Block the driver even on machines where another extra installed it."""

    script = r"""
import importlib.abc
import pathlib
import sys
sys.path.insert(0, sys.argv[1])
class WithoutDriver(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "psycopg" or fullname.startswith("psycopg."):
            raise ModuleNotFoundError("deliberate optional-driver absence", name="psycopg")
sys.meta_path.insert(0, WithoutDriver())
from nexus.portfolio.postgres_backend import PostgresConfig, PostgresBackend, PostgresBackendError
from nexus.portfolio.store import PortfolioStore
root = pathlib.Path(sys.argv[2])
config = PostgresConfig(
    dsn="postgresql://nexus_test_app:do-not-print@127.0.0.1:55438/nexus_portfolio_test_unit",
    schema="nexus_test_optional",
    artifact_root=root / "postgres-artifacts",
)
backend = PostgresBackend(config)
assert not config.artifact_root.exists()
try:
    backend.has_seed()
except PostgresBackendError as exc:
    assert exc.code == "dependency_missing", exc.code
    assert "do-not-print" not in str(exc)
else:
    raise AssertionError("missing driver should be actionable, not silently fall back")
assert not config.artifact_root.exists()
sqlite = PortfolioStore(root / "sqlite")
assert not sqlite.has_seed()
print("optional-driver boundary and SQLite initialization passed")
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(ROOT / "src"), str(tmp_path)],
        cwd=ROOT,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "optional-driver boundary and SQLite initialization passed" in result.stdout
