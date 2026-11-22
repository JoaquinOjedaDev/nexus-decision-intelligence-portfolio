"""CLI orchestration and isolated HTTP acceptance use deterministic test evidence."""

from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from nexus.config import PROJECT_ROOT
from nexus.portfolio import cli, engine


@pytest.fixture
def seed(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    value = {
        "manifest_fingerprint": "a" * 64,
        "metadata": {"classification": "synthetic"},
        "metrics": {"recommendations": 1},
        "recommendations": [
            {
                "id": "test-recommendation",
                "sku_id": "SKU-01",
                "location_id": "LOC-01",
                "created_by": "planner.r4a",
                "recommended_quantity": 20,
                "observed_demand": 25,
                "on_hand": 10,
                "unit_cost": 5,
                "expected_cost": 100,
            }
        ],
    }
    monkeypatch.setattr(engine, "build_seed", lambda *_args, **_kwargs: value)
    monkeypatch.setattr(engine, "load_seed", lambda *_args, **_kwargs: value)
    return value


def test_real_http_acceptance_with_fixture_seed(seed: dict[str, Any], tmp_path: Path) -> None:
    result = cli.run_acceptance(PROJECT_ROOT, tmp_path)
    assert result["status"] == "PASS"
    assert result["checks_passed"] >= 20
    assert result["metrics"] == seed["metrics"]


def test_prepare_preserves_state(seed: dict[str, Any], tmp_path: Path) -> None:
    runner = CliRunner()
    for _ in range(2):
        result = runner.invoke(cli.app, ["prepare", "--state-dir", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert '"status": "ready"' in result.output


def test_verify_writes_receipt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "run_acceptance", lambda *_args: {"status": "PASS"})
    output = tmp_path / "receipt.json"
    result = CliRunner().invoke(cli.app, ["verify", "--output", str(output)])
    assert result.exit_code == 0
    assert '"PASS"' in output.read_text()


def test_serve_never_binds_publicly(
    seed: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import uvicorn

    recorded: dict[str, Any] = {}

    def observe(application: Any, **kwargs: Any) -> None:
        assert (
            application.state.portfolio_store.seed()["manifest_fingerprint"]
            == seed["manifest_fingerprint"]
        )
        recorded.update(kwargs)

    monkeypatch.setattr(uvicorn, "run", observe)
    result = CliRunner().invoke(cli.app, ["serve", "--state-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert recorded["host"] == "127.0.0.1"
    assert recorded["proxy_headers"] is False
    assert CliRunner().invoke(cli.app, ["serve", "--port", "22"]).exit_code != 0


@pytest.mark.parametrize("command", ["prepare", "serve"])
def test_stale_demo_refuses_before_database_or_server_and_preserves_evidence(
    command: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import uvicorn

    state = tmp_path / "existing"
    evidence = state / "evidence"
    evidence.mkdir(parents=True)
    retained = evidence / "retained.json"
    retained.write_bytes(b'{"historical":true}\n')
    (tmp_path / "existing-fresh").mkdir()

    def stale(*_args: Any, **_kwargs: Any) -> Any:
        raise engine.StalePortfolioEvidenceError("stale")

    monkeypatch.setattr(engine, "build_seed", stale)
    monkeypatch.setattr(uvicorn, "run", lambda *_args, **_kwargs: pytest.fail("server started"))
    result = CliRunner().invoke(cli.app, [command, "--state-dir", str(state)], color=False)
    assert result.exit_code != 0
    assert "Saved demo evidence is stale" in result.output
    assert "NEXUS local demo:" not in result.output
    assert "prepare --state-dir" in result.output
    assert "serve --state-dir" in result.output
    assert "existing-fresh-2" in result.output
    assert retained.read_bytes() == b'{"historical":true}\n'
    assert not (state / "portfolio.sqlite3").exists()
    assert not (tmp_path / "existing-fresh-2").exists()
