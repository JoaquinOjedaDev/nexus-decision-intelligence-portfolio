from __future__ import annotations

import json
from pathlib import Path

from nexus.data import load_r2c_profile
from nexus.r2c import verify_r2c

ROOT = Path(__file__).resolve().parents[1]


def test_bounded_r2c_gate_emits_machine_readable_passing_evidence(tmp_path: Path) -> None:
    config = load_r2c_profile(ROOT / "configs" / "profiles" / "smoke.yaml")
    result = verify_r2c(artifact_root=tmp_path / "verification", config=config)

    assert result.passed is True, result.checks
    assert result.checks and all(result.checks.values())
    assert result.controls.demand_rows == 3360
    assert result.python_threads_after_run <= config.resource_budget.maximum_python_threads
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert summary["passed"] is True
    assert summary["dataset_semantic_sha256"] == result.dataset_semantic_sha256
    execution_root = Path(result.summary_path).parent
    assert result.artifact_bytes == sum(
        path.stat().st_size for path in execution_root.rglob("*") if path.is_file()
    )

    repeated = verify_r2c(artifact_root=tmp_path / "verification", config=config)
    assert repeated.passed is True
    assert Path(repeated.summary_path).parent != Path(result.summary_path).parent
    assert repeated.checks["fresh_generation"] is True
