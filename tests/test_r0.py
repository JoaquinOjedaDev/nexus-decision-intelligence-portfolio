from __future__ import annotations

import shutil
import sys
from pathlib import Path

from nexus.r0 import _prepare_dbt_output_path, _run, verify_r0

ROOT = Path(__file__).resolve().parents[1]


def test_run_captures_success_and_failure(tmp_path: Path) -> None:
    success = _run([sys.executable, "-c", "print('ok')"], cwd=tmp_path)
    failure = _run(
        [sys.executable, "-c", "import sys; print('expected failure'); sys.exit(4)"],
        cwd=tmp_path,
    )

    assert success.passed is True
    assert success.detail == "ok"
    assert failure.passed is False
    assert failure.detail == "expected failure"


def test_prepare_dbt_output_path_on_clean_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NEXUS_DBT_PATH", "generated/dbt/clean-runner.duckdb")

    database_path = _prepare_dbt_output_path(tmp_path)

    assert database_path == tmp_path / "generated" / "dbt" / "clean-runner.duckdb"
    assert database_path.parent.is_dir()
    assert not database_path.exists()


def test_verify_r0_core_path() -> None:
    results = verify_r0(ROOT, run_dbt=False, run_compose=False)

    assert results
    assert all(result.passed for result in results)
    assert {result.name for result in results} == {
        "required R0 files",
        "contract examples",
        "storage proof",
    }


def test_verify_r0_reports_missing_optional_executables(monkeypatch) -> None:
    monkeypatch.setattr(shutil, "which", lambda _name: None)

    results = verify_r0(ROOT, run_dbt=True, run_compose=True)

    failures = {result.name: result.detail for result in results if not result.passed}
    assert failures == {
        "dbt build": "dbt executable not found",
        "compose config": "docker not found",
    }


def test_verify_r0_reports_missing_repository_files(tmp_path: Path) -> None:
    results = verify_r0(tmp_path, run_dbt=False, run_compose=False)

    by_name = {result.name: result for result in results}
    assert by_name["required R0 files"].passed is False
    assert "pyproject.toml" in by_name["required R0 files"].detail
    assert by_name["contract examples"].passed is False
    assert by_name["storage proof"].passed is True
