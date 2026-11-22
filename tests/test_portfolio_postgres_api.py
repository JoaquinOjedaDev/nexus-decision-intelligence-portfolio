"""Portable PostgreSQL HTTP admission checks; no database or source builds."""

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from nexus.portfolio import postgres_backend, runtime_service
from nexus.portfolio.api import create_app
from nexus.portfolio.postgres_backend import PostgresBackendError, PostgresConfig
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.store import StoreError


def config(root: Path) -> PostgresConfig:
    return PostgresConfig(
        "host=127.0.0.1 port=55438 dbname=nexus_test user=nexus_app password=DO_NOT_PRINT",
        "nexus_http_test",
        root / "evidence" / "initial",
    )


def forbidden(*args: Any, **kwargs: Any) -> None:
    pytest.fail("HTTP must not migrate, compute sources or fall back to SQLite")


def test_postgres_factory_and_liveness_are_lazy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "runtime"
    monkeypatch.setattr(postgres_backend, "_driver", forbidden)
    monkeypatch.setattr(runtime_service, "build_runtime_seed", forbidden)
    monkeypatch.setattr(runtime_service, "prepare_runtime", forbidden)
    monkeypatch.setattr(PostgresRuntimeStore, "migrate", forbidden)
    app = create_app(root, pipeline="runtime", postgres=config(root))
    client = TestClient(app)
    assert client.get("/healthz").status_code == 200
    assert client.get("/api/session").json()["user"] is None
    assert app.state.portfolio_store is None
    assert not root.exists()


def test_missing_driver_is_controlled_and_never_falls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "runtime"

    def absent() -> None:
        raise PostgresBackendError("dependency_missing", "Optional PostgreSQL driver is missing")

    monkeypatch.setattr(postgres_backend, "_driver", absent)
    monkeypatch.setattr(runtime_service, "prepare_runtime", forbidden)
    monkeypatch.setattr(runtime_service, "build_runtime_seed", forbidden)
    client = TestClient(create_app(root, pipeline="runtime", postgres=config(root)))
    response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "dependency_missing"
    assert "DO_NOT_PRINT" not in response.text and "dsn" not in response.text.lower()
    assert not root.exists()


def test_empty_migrated_schema_requires_explicit_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "runtime"
    monkeypatch.setattr(PostgresRuntimeStore, "has_seed", lambda self: False)
    monkeypatch.setattr(PostgresRuntimeStore, "migrate", forbidden)
    monkeypatch.setattr(runtime_service, "prepare_runtime", forbidden)
    monkeypatch.setattr(runtime_service, "build_runtime_seed", forbidden)
    client = TestClient(create_app(root, pipeline="runtime", postgres=config(root)))
    response = client.post("/api/session", json={"persona": "planner"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "not_ready"
    assert not root.exists()


def test_frozen_pipeline_cannot_select_postgres(tmp_path: Path) -> None:
    with pytest.raises(StoreError) as caught:
        create_app(tmp_path, postgres=config(tmp_path))
    assert caught.value.code == "runtime_required"


def test_existing_sqlite_file_is_preserved(tmp_path: Path) -> None:
    path = tmp_path / "portfolio.sqlite3"
    path.write_bytes(b"existing-user-state")
    with pytest.raises(StoreError) as caught:
        create_app(tmp_path, pipeline="runtime", postgres=config(tmp_path))
    assert caught.value.code == "backend_conflict"
    assert path.read_bytes() == b"existing-user-state"


def test_mismatched_initial_evidence_root_fails_before_io(tmp_path: Path) -> None:
    with pytest.raises(StoreError) as caught:
        create_app(tmp_path / "runtime", pipeline="runtime", postgres=config(tmp_path))
    assert caught.value.code == "configuration_error"
    assert not (tmp_path / "runtime").exists()


def test_partial_preparation_is_not_overwritten_or_relabelled_fresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = config(tmp_path)
    settings.artifact_root.mkdir(parents=True)
    evidence = settings.artifact_root / "retained.txt"
    evidence.write_text("incomplete-source-run", encoding="utf-8")
    monkeypatch.setattr(PostgresRuntimeStore, "has_seed", lambda self: False)
    monkeypatch.setattr(runtime_service, "build_runtime_seed", forbidden)
    with pytest.raises(StoreError) as caught:
        runtime_service.prepare_postgres_runtime(tmp_path, tmp_path, settings)
    assert caught.value.code == "incomplete_preparation"
    assert evidence.read_text(encoding="utf-8") == "incomplete-source-run"
