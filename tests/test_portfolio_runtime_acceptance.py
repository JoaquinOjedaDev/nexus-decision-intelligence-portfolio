"""Actual local end-to-end acceptance; one two-build run shared by assertions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from nexus.modeling.r3_artifacts import canonical_json_sha256
from nexus.portfolio.runtime_acceptance import RuntimeAcceptanceError, run_runtime_acceptance

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def acceptance(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict[str, Any]]:
    root = tmp_path_factory.mktemp("runtime-acceptance")
    result = run_runtime_acceptance(ROOT, root)
    return root, result


def test_real_connected_http_worker_restart_acceptance_passes(
    acceptance: tuple[Path, dict[str, Any]],
) -> None:
    _, result = acceptance
    assert result["status"] == "pass", result.get("failure")
    assert all(check["passed"] for check in result["checks"])
    assert result["final_metrics"]["effect_count"] == 1
    assert result["final_runtime"]["archived_generations"] == 1
    assert sorted(job["state"] for job in result["final_runtime"]["jobs"]) == [
        "completed",
        "failed",
    ]
    assert len(result["scientific_bindings"]) == 2
    assert result["measurements_seconds"]["initial_real_build"] <= 60
    assert result["measurements_seconds"]["replacement_real_build"] <= 60


def test_acceptance_receipt_is_honest_and_bound_to_predeclared_plan(
    acceptance: tuple[Path, dict[str, Any]],
) -> None:
    directory, result = acceptance
    plan = json.loads((directory / "acceptance-plan.json").read_text(encoding="utf-8"))
    receipt = json.loads((directory / "acceptance-receipt.json").read_text(encoding="utf-8"))
    assert receipt == result
    assert result["plan_sha256"] == canonical_json_sha256(plan)
    assert result["receipt_sha256"] == canonical_json_sha256(
        {key: value for key, value in result.items() if key != "receipt_sha256"}
    )
    assert plan["profile"]["real_builds"] == 2
    assert plan["profile"]["repetitions"] == 1
    assert plan["profile"]["original_full_load_gate_claimed"] is False
    assert result["scope"]["original_full_gates_closed"] is False
    assert result["scope"]["real_business_savings_claimed"] is False
    checks = {item["name"]: item for item in result["checks"]}
    assert checks["future_available_actual_is_excluded"]["observed"]["metrics"] == {}
    assert checks["missing_mature_labels_is_not_healthy"]["observed"] == "insufficient_evidence"
    assert "csrf" not in json.dumps(result).lower()


def test_acceptance_refuses_to_reset_existing_state(tmp_path: Path) -> None:
    marker = tmp_path / "existing-user-state.txt"
    marker.write_text("preserve", encoding="utf-8")
    with pytest.raises(RuntimeAcceptanceError, match="new or empty"):
        run_runtime_acceptance(ROOT, tmp_path)
    assert marker.read_text(encoding="utf-8") == "preserve"
