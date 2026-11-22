from pathlib import Path

import yaml

WORKFLOW_PATH = Path(".github/workflows/ci.yml")
FULL_VALIDATION = (
    "${{ github.event_name == 'workflow_dispatch' && inputs.validation_tier == 'full' }}"
)
FULL_VALIDATION_PY313 = (
    "${{ github.event_name == 'workflow_dispatch' && inputs.validation_tier == 'full' "
    "&& matrix.python-version == '3.13' }}"
)


def _workflow() -> dict[object, object]:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def test_ci_defaults_to_portable_quality_and_requires_opt_in_for_expensive_jobs() -> None:
    workflow = _workflow()
    triggers = workflow.get("on", workflow.get(True))
    assert isinstance(triggers, dict)
    assert "pull_request" in triggers
    assert triggers["push"]["branches"] == ["main"]
    dispatch = triggers["workflow_dispatch"]
    tier = dispatch["inputs"]["validation_tier"]
    assert tier["required"] is True
    assert tier["type"] == "choice"
    assert tier["default"] == "lean"
    assert tier["options"] == ["lean", "full"]

    jobs = workflow["jobs"]
    assert "if" not in jobs["quality"]
    for name, job in jobs.items():
        if name != "quality":
            assert job["if"] == FULL_VALIDATION, name


def test_expensive_executable_gates_are_manual_full_only() -> None:
    quality = _workflow()["jobs"]["quality"]
    steps = {step.get("name"): step for step in quality["steps"]}
    for name in (
        "R0 executable gate",
        "R1 executable gate",
        "R2A trustworthy-data gate",
        "R3A probabilistic-baseline gate",
    ):
        assert steps[name]["if"] == FULL_VALIDATION
    for name in (
        "R2B partition-materialization gate",
        "R2C representative-profile gate",
    ):
        assert steps[name]["if"] == FULL_VALIDATION_PY313

    portable_tests = steps["Tests"]["run"]
    assert "not r2d_service" in portable_tests
    assert "not r3b" in portable_tests
    assert "not r4a" in portable_tests


def test_portable_coverage_excludes_service_only_b2c_entrypoint() -> None:
    coverage_config = Path("configs/coverage/portable-ci.toml").read_text(encoding="utf-8")
    assert '"src/nexus/r2d_b2c.py"' in coverage_config


def test_quality_includes_guarded_native_coverage_without_weakening_threshold() -> None:
    quality = _workflow()["jobs"]["quality"]
    assert quality["runs-on"] == "ubuntu-24.04"
    assert quality["env"]["NEXUS_PORTFOLIO_PG_TEST_CONFIG"] == (
        "${{ github.workspace }}/artifacts/ci-postgres/connection-secrets.json"
    )
    steps = {step.get("name"): step for step in quality["steps"]}
    assert "--group r2d-service" in steps["Sync locked dependencies"]["run"]
    init = steps["Initialize isolated portfolio test cluster"]["run"]
    assert "native_postgres_test_cluster.py init" in init
    assert "--root artifacts/ci-postgres" in init
    assert "--bin-dir /usr/lib/postgresql/16/bin --port 55438" in init
    names = list(steps)
    assert names.index("Initialize isolated portfolio test cluster") < names.index("Tests")
    assert "not portfolio_service" not in steps["Tests"]["run"]
    assert "--cov-fail-under" not in steps["Tests"]["run"]
    stop = steps["Stop isolated portfolio test cluster"]
    assert stop["if"] == "${{ always() }}"
    assert "-f artifacts/ci-postgres/data/postmaster.pid" in stop["run"]
    assert "native_postgres_test_cluster.py stop --root artifacts/ci-postgres" in stop["run"]
    assert "fail_under = 85" in Path("configs/coverage/portable-ci.toml").read_text()
