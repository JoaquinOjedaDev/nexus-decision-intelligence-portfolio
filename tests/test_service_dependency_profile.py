"""Clean operational installs must declare every service runtime dependency."""

import tomllib
from pathlib import Path

import pytest

from scripts import service_image_smoke

ROOT = Path(__file__).resolve().parents[1]


def test_service_group_includes_broker_portfolio_and_process_ownership() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    group = project["dependency-groups"]["service"]
    assert {row["include-group"] for row in group if isinstance(row, dict)} == {
        "portfolio",
        "r2d-service",
    }
    assert "psutil>=7.2,<8" in group
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    package = next(p for p in lock["package"] if p["name"] == "nexus-decision-intelligence")
    resolved = {row["name"] for row in package["dev-dependencies"]["service"]}
    assert {"psutil", "psycopg", "confluent-kafka", "dbt-duckdb", "ortools", "uvicorn"} <= resolved


def test_service_image_uses_pinned_base_offline_hashes_and_nonroot_default() -> None:
    dockerfile = (ROOT / "infra/flagship/Dockerfile").read_text()
    assert "FROM python@sha256:" in dockerfile
    assert "--no-index --find-links=/wheels" in dockerfile
    assert "--require-hashes" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert "trusted-host" not in dockerfile
    ignore = (ROOT / "infra/flagship/Dockerfile.dockerignore").read_text()
    assert "\n**\n" in ignore
    assert "!artifacts/" not in ignore and "!.venv/" not in ignore
    assert "**/*secrets*" in ignore and "**/*.duckdb*" in ignore


def test_image_smoke_refuses_host_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(service_image_smoke.platform, "system", lambda: "Windows")
    with pytest.raises(ValueError, match="non-root service image"):
        service_image_smoke.main()


def test_image_smoke_refuses_root_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(service_image_smoke.platform, "system", lambda: "Linux")
    monkeypatch.setattr(service_image_smoke.os, "getuid", lambda: 0, raising=False)
    with pytest.raises(ValueError, match="non-root service image"):
        service_image_smoke.main()
