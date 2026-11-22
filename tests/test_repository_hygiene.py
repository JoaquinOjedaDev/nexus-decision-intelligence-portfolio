from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _tracked_files() -> set[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return {path for path in result.stdout.split("\0") if path}


def test_secret_file_patterns_are_ignored_and_not_tracked() -> None:
    ignore_rules = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    forbidden = {
        path
        for path in _tracked_files()
        if path == ".env" or Path(path).suffix.lower() in {".pem", ".key"}
    }

    assert {".env", "*.pem", "*.key"}.issubset(ignore_rules)
    assert not forbidden


def test_external_data_is_not_tracked() -> None:
    tracked_data = {path for path in _tracked_files() if path.startswith("data/")}

    assert (ROOT / "data" / ".gitkeep").is_file()
    assert tracked_data <= {"data/.gitkeep"}
