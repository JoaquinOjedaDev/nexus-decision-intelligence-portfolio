"""Portable explicit-route/factory checks; never contact PostgreSQL or the broker."""

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from nexus.portfolio import postgres_backend
from nexus.portfolio.api import create_app
from nexus.portfolio.broker_runtime import RuntimeBrokerConfig
from nexus.portfolio.postgres_backend import PostgresBackendError, PostgresConfig
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.recovery_ledger import RecoveryTransportLedger
from nexus.portfolio.runtime_service import _postgres_store
from nexus.portfolio.source_freshness import SourceWatchPolicy
from nexus.portfolio.source_watch_runtime import SourceWatchRuntimeStore
from nexus.portfolio.store import StoreError
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger, runtime_target
from nexus.portfolio.transport_runtime import TransportFenceConfig, TransportRuntimeStore


@pytest.fixture
def settings(tmp_path: Path) -> tuple[PostgresConfig, TransportFenceConfig]:
    config = PostgresConfig(
        "host=127.0.0.1 port=55439 dbname=nexus_test user=nexus_app password=PRIVATE",
        "nexus_runtime_test",
        tmp_path / "evidence/initial",
    )
    binding = TransportBinding(
        RuntimeBrokerConfig(
            "127.0.0.1:19092", "nexus-runtime-test", "nexus-runtime-test-ops", ("LOC-1",)
        ),
        "test-cluster",
        "test-topic",
        2,
        "operations",
        runtime_target(PostgresRuntimeStore(config, runtime_root=tmp_path).control),
    )
    return config, TransportFenceConfig(replace(config, schema="nexus_capture_test"), binding)


@pytest.mark.parametrize("publication", [False, True])
def test_explicit_fenced_factory_is_lazy(
    settings: tuple[PostgresConfig, TransportFenceConfig],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    publication: bool,
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Factory must not connect, migrate, or capture")

    monkeypatch.setattr(postgres_backend, "_driver", forbidden)
    config, fence = settings
    fence = replace(fence, require_source_publication=publication)
    app = create_app(tmp_path, pipeline="runtime", postgres=config, transport_fence=fence)
    assert app.state.portfolio_store is None
    assert TestClient(app).get("/healthz").status_code == 200
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("change", ["host", "port", "database", "schema", "target", "audit"])
def test_cross_route_or_nonoperations_fence_is_refused(
    settings: tuple[PostgresConfig, TransportFenceConfig],
    tmp_path: Path,
    change: str,
) -> None:
    config, fence = settings
    capture, binding = fence.ledger, fence.binding
    if change in {"host", "port", "database"}:
        old, new = {
            "host": ("127.0.0.1", "::1"),
            "port": ("55439", "55440"),
            "database": ("nexus_test", "nexus_other"),
        }[change]
        capture = replace(capture, dsn=capture.dsn.replace(old, new))
    elif change == "schema":
        capture = replace(capture, schema=config.schema)
    elif change == "target":
        binding = replace(binding, runtime_target_sha256="f" * 64)
    else:
        binding = replace(binding, purpose="audit", runtime_target_sha256=None)
    with pytest.raises(PostgresBackendError) as failure:
        TransportRuntimeStore(
            config, runtime_root=tmp_path, transport=TransportLedger(capture), binding=binding
        )
    assert failure.value.code == "configuration_error"
    assert "PRIVATE" not in str(failure.value)


def test_fence_never_selects_sqlite(
    settings: tuple[PostgresConfig, TransportFenceConfig], tmp_path: Path
) -> None:
    with pytest.raises(StoreError) as failure:
        create_app(tmp_path, transport_fence=settings[1])
    assert failure.value.code == "runtime_required"


def test_custom_migration_cannot_replace_fence(tmp_path: Path) -> None:
    with pytest.raises(PostgresBackendError, match="canonical packaged"):
        TransportRuntimeStore._migration_bytes(tmp_path / "untrusted.sql")


def test_source_watch_requires_publication_and_selects_lazy_v8(
    settings: tuple[PostgresConfig, TransportFenceConfig],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, fence = settings
    policy = SourceWatchPolicy("inventory", str((tmp_path / "input").resolve()), 30)
    with pytest.raises(ValueError, match="publication mode"):
        replace(fence, source_watch=policy)

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Factory must remain lazy")

    monkeypatch.setattr(postgres_backend, "_driver", forbidden)
    watched = replace(fence, require_source_publication=True, source_watch=policy)
    store = _postgres_store(tmp_path, config, transport_fence=watched)
    assert isinstance(store, SourceWatchRuntimeStore) and store.watch == policy
    with pytest.raises(ValueError, match="publication mode"):
        replace(fence, recovery_journal=True)
    recovery_store = _postgres_store(
        tmp_path, config, transport_fence=replace(watched, recovery_journal=True)
    )
    assert isinstance(recovery_store, SourceWatchRuntimeStore)
    assert isinstance(recovery_store.transport, RecoveryTransportLedger)
    from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger

    with pytest.raises(ValueError, match="explicit recovery journal"):
        replace(watched, recovery_resolution=True)
    for policy in (None, watched.source_watch):
        resolved = _postgres_store(
            tmp_path,
            config,
            transport_fence=replace(
                watched, recovery_journal=True, recovery_resolution=True, source_watch=policy
            ),
        )
        assert isinstance(resolved.transport, RecoveryResolutionLedger)
        assert resolved.transport.publication is resolved
    assert list(tmp_path.iterdir()) == []
