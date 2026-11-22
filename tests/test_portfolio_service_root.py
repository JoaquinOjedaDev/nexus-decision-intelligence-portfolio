"""Filesystem-only root ownership tests: no credentials used and no database calls."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from nexus.portfolio import service_backend
from nexus.portfolio.postgres_backend import PostgresBackendError, PostgresConfig
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.service_backend import bind_service_root, state_root
from nexus.portfolio.store import PortfolioStore, StoreError


def store(root: Path, schema: str = "nexus_first") -> PostgresRuntimeStore:
    return PostgresRuntimeStore(
        PostgresConfig(
            "host=127.0.0.1 port=55438 dbname=nexus_fixture user=nexus_app password=TEST_SECRET",
            schema,
            root / "evidence" / "initial",
        ),
        runtime_root=root,
    )


def test_read_requires_explicit_claim_and_does_not_create_state(tmp_path: Path) -> None:
    backend = store(tmp_path / "new")
    with pytest.raises(StoreError) as caught:
        state_root(backend)
    assert caught.value.code == "service_root_unbound"
    assert not backend.runtime_root.exists()


def test_other_schema_cannot_share_any_sidecar(tmp_path: Path) -> None:
    first, second = store(tmp_path), store(tmp_path, "nexus_second")
    bind_service_root(first, claim=True)
    marker = tmp_path / ".nexus-service-root.json"
    original = marker.read_bytes()
    journal = tmp_path / "feedback.sqlite3"
    journal.write_bytes(b"first-runtime-data")
    for operation in (lambda: state_root(second), lambda: bind_service_root(second, claim=True)):
        with pytest.raises(StoreError) as caught:
            operation()
        assert caught.value.code == "service_root_conflict"
    assert marker.read_bytes() == original
    assert journal.read_bytes() == b"first-runtime-data"
    assert "TEST_SECRET" not in original.decode()
    assert state_root(first) == tmp_path


@pytest.mark.parametrize(
    "name",
    [
        "portfolio.sqlite3",
        "alerts.sqlite3",
        "feedback.sqlite3",
        "feedback-monitoring.sqlite3",
        "monitoring.sqlite3",
        "learning-attachments.sqlite3",
        "feedback.sqlite3-wal",
        "alerts.sqlite3-shm",
        "monitoring.sqlite3-journal",
    ],
)
def test_unbound_journals_are_not_silently_adopted(tmp_path: Path, name: str) -> None:
    path = tmp_path / name
    path.write_bytes(b"retained")
    with pytest.raises(StoreError) as caught:
        bind_service_root(store(tmp_path), claim=True)
    assert caught.value.code == "service_root_unbound"
    assert not (tmp_path / ".nexus-service-root.json").exists()
    assert path.read_bytes() == b"retained"


@pytest.mark.parametrize("content", [b"", b"{}", b"partial", b"x" * 1000])
def test_partial_or_tampered_claim_is_preserved(tmp_path: Path, content: bytes) -> None:
    marker = tmp_path / ".nexus-service-root.json"
    marker.write_bytes(content)
    with pytest.raises(StoreError) as caught:
        bind_service_root(store(tmp_path), claim=True)
    assert caught.value.code == "service_root_conflict"
    assert marker.read_bytes() == content


def test_password_rotation_does_not_change_ownership(tmp_path: Path) -> None:
    first = store(tmp_path)
    bind_service_root(first, claim=True)
    second = PostgresRuntimeStore(
        replace(first.config, dsn=first.config.dsn.replace("TEST_SECRET", "ROTATED")),
        runtime_root=tmp_path,
    )
    bind_service_root(second, claim=True)
    assert state_root(second) == tmp_path


def test_concurrent_different_owners_never_both_claim(tmp_path: Path) -> None:
    candidates = [store(tmp_path, "nexus_first"), store(tmp_path, "nexus_second")]

    def claim(index: int) -> bool:
        try:
            bind_service_root(candidates[index], claim=True)
        except StoreError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(claim, range(2)))
    assert sum(outcomes) == 1
    assert state_root(candidates[outcomes.index(True)]) == tmp_path


def test_sqlite_does_not_gain_an_owner_file(tmp_path: Path) -> None:
    legacy = PortfolioStore(tmp_path)
    bind_service_root(legacy, claim=True)
    assert state_root(legacy) == tmp_path
    assert not (tmp_path / ".nexus-service-root.json").exists()


@pytest.mark.parametrize("stage", ["mkdir", "open", "fsync"])
def test_claim_io_failures_are_redacted_and_retained(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    backend = store(tmp_path / "new")
    marker = backend.runtime_root / ".nexus-service-root.json"

    def fail(*args: object, **kwargs: object) -> None:
        raise PermissionError("PRIVATE_PATH TEST_SECRET")

    with monkeypatch.context() as fault:
        if stage == "mkdir":
            fault.setattr(Path, "mkdir", fail)
        else:
            fault.setattr(service_backend.os, stage, fail)
        with pytest.raises(StoreError) as caught:
            bind_service_root(backend, claim=True)
    assert caught.value.code == "service_root_unavailable"
    assert "PRIVATE_PATH" not in str(caught.value)
    assert "TEST_SECRET" not in str(caught.value)
    if stage == "fsync":
        # A failed durability acknowledgement must not erase the ownership file.
        assert marker.is_file()
    else:
        assert not marker.exists()


@pytest.mark.parametrize(
    ("old", "new"),
    [("55438", "55439"), ("nexus_fixture", "other_fixture")],
)
def test_database_route_change_cannot_reuse_owned_root(tmp_path: Path, old: str, new: str) -> None:
    backend = store(tmp_path)
    bind_service_root(backend, claim=True)
    original = (tmp_path / ".nexus-service-root.json").read_bytes()
    backend.config = replace(backend.config, dsn=backend.config.dsn.replace(old, new))
    with pytest.raises(StoreError) as caught:
        state_root(backend)
    assert caught.value.code == "service_root_conflict"
    assert (tmp_path / ".nexus-service-root.json").read_bytes() == original


def test_nonpermitted_host_is_rejected_before_root_access(tmp_path: Path) -> None:
    backend = store(tmp_path)
    with pytest.raises(PostgresBackendError) as caught:
        replace(backend.config, dsn=backend.config.dsn.replace("127.0.0.1", "127.0.0.2"))
    assert caught.value.code == "configuration_error"
    assert not (tmp_path / ".nexus-service-root.json").exists()
