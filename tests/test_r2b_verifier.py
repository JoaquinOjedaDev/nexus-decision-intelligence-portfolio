from __future__ import annotations

import json
from pathlib import Path

from nexus.r2b import verify_r2b

ROOT = Path(__file__).resolve().parents[1]


def test_setup_failure_persists_immutable_fail_gate(tmp_path: Path) -> None:
    artifact_root = tmp_path / "verification"

    result = verify_r2b(
        project_root=ROOT,
        artifact_root=artifact_root,
        config_path=tmp_path / "missing-r2b.yaml",
    )

    assert result.passed is False
    assert result.gate_manifest is not None
    assert result.gate_manifest.path.is_file()
    assert result.summary_path.is_file()
    assert result.to_summary()["full_g2_status"] == "IN_PROGRESS"
    assert any(check.name == "R2B executable verification" for check in result.checks)

    immutable = json.loads(result.gate_manifest.path.read_text(encoding="utf-8"))
    mutable = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert immutable["status"] == "FAIL"
    assert mutable["status"] == "FAIL"
    assert immutable["gate"] == mutable["gate"] == "G2B"
