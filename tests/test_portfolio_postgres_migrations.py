"""Portable migration-resource checks; no driver, build, or database is required."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from nexus.portfolio import postgres_migrations as migrations

ROOT = Path(__file__).resolve().parents[1]


def test_editable_checkout_reads_exact_canonical_migration() -> None:
    assert (
        migrations.read_migration_sql(2)
        == (ROOT / "infra/postgres/migrations/002_portfolio_ops.sql").read_bytes()
    )


@pytest.mark.parametrize("version", [0, 1, 13, -1, True, "2", None])
def test_unsupported_versions_cannot_select_arbitrary_files(version: object) -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        migrations.read_migration_sql(version)  # type: ignore[arg-type]


def resources(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    package = tmp_path / "installed-package"
    sql = package / "sql_migrations"
    sql.mkdir(parents=True)
    monkeypatch.setattr(migrations, "files", lambda _: package)
    return sql


def test_bundled_bytes_have_priority_and_are_not_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sql = resources(tmp_path, monkeypatch)
    path = sql / migrations.MIGRATION_FILES[2]
    path.write_bytes(b"-- first bundled fixture\n")
    assert migrations.read_migration_sql(2) == b"-- first bundled fixture\n"
    path.write_bytes(b"-- changed bundled fixture\n")
    assert migrations.read_migration_sql(2) == b"-- changed bundled fixture\n"


def test_missing_bundled_file_never_falls_back_to_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    resources(tmp_path, monkeypatch)
    with pytest.raises(FileNotFoundError):
        migrations.read_migration_sql(2)


def test_missing_installed_resource_directory_never_searches_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = tmp_path / "venv/Lib/site-packages/nexus/portfolio"
    package.mkdir(parents=True)
    monkeypatch.setattr(migrations, "files", lambda _: package)
    monkeypatch.setattr(migrations, "__file__", str(package / "postgres_migrations.py"))
    monkeypatch.chdir(ROOT)
    with pytest.raises(FileNotFoundError, match="Installed portfolio SQL resources"):
        migrations.read_migration_sql(2)


@pytest.mark.parametrize(
    "content",
    [b"", b"-- x\x00", b"\xff", b"x" * (256 * 1024 + 1)],
    ids=["empty", "nul", "invalid-utf8", "oversize"],
)
def test_invalid_resource_bytes_are_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, content: bytes
) -> None:
    sql = resources(tmp_path, monkeypatch)
    (sql / migrations.MIGRATION_FILES[2]).write_bytes(content)
    with pytest.raises((ValueError, UnicodeError)):
        migrations.read_migration_sql(2)


@pytest.mark.parametrize(
    ("version", "filename"),
    [
        (3, "003_portfolio_workflow.sql"),
        (4, "004_portfolio_runtime.sql"),
        (5, "005_portfolio_transport.sql"),
        (6, "006_portfolio_transport_fence.sql"),
        (7, "007_portfolio_publication.sql"),
        (8, "008_portfolio_source_watch.sql"),
        (9, "009_portfolio_recovery.sql"),
        (10, "010_portfolio_recovery_publication.sql"),
        (11, "011_portfolio_recovery_resolution.sql"),
        (12, "012_portfolio_capture_health.sql"),
    ],
)
def test_workflow_migration_has_an_explicit_resource_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, version: int, filename: str
) -> None:
    sql = resources(tmp_path, monkeypatch)
    (sql / filename).write_bytes(b"-- workflow fixture\n")
    assert migrations.read_migration_sql(version) == b"-- workflow fixture\n"


def test_wheel_includes_canonical_infra_migrations_without_a_maintained_copy() -> None:
    configuration = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    forced = configuration["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]
    assert forced["infra/postgres/migrations"] == "nexus/portfolio/sql_migrations"
    assert not (ROOT / "src/nexus/portfolio/sql_migrations").exists()
