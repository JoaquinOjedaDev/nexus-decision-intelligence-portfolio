"""Non-interactive dbt must not inherit a supervisor control channel."""

import subprocess
from datetime import UTC, datetime
from types import SimpleNamespace

from nexus import r2


def test_dbt_subprocess_has_closed_stdin(tmp_path, monkeypatch):
    monkeypatch.setattr(r2.shutil, "which", lambda name: "dbt-test.exe")
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout="Done. PASS=189", stderr="")

    monkeypatch.setattr(r2.subprocess, "run", run)
    assert (
        r2._run_dbt(
            project_root=tmp_path,
            validated_files={"inventory": tmp_path / "inventory.parquet"},
            database_path=tmp_path / "db" / "test.duckdb",
            planning_cutoff=datetime(2026, 6, 30, tzinfo=UTC),
        )
        == "Done. PASS=189"
    )
    command, options = calls[0]
    assert command[:2] == ["dbt-test.exe", "build"]
    assert options["stdin"] == subprocess.DEVNULL
    assert options["capture_output"] and options["timeout"] == 180
    artifacts = (tmp_path / "db" / "test.dbt").resolve()
    assert command[command.index("--target-path") + 1] == str(artifacts / "target")
    assert command[command.index("--log-path") + 1] == str(artifacts / "logs")
    assert options["env"]["NEXUS_DBT_PATH"] == str((tmp_path / "db" / "test.duckdb").resolve())
