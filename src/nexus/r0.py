"""Executable R0 release-gate checks."""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from nexus.storage import run_storage_proof


@dataclass(frozen=True)
class VerificationResult:
    name: str
    passed: bool
    detail: str


def _prepare_dbt_output_path(project_root: Path) -> Path:
    """Create the parent directory for dbt's DuckDB output on a clean workspace."""

    configured = Path(os.getenv("NEXUS_DBT_PATH", "artifacts/dbt/nexus_ci.duckdb"))
    database_path = configured if configured.is_absolute() else project_root / configured
    database_path.parent.mkdir(parents=True, exist_ok=True)
    return database_path


def _run(
    command: list[str],
    cwd: Path,
    timeout: int = 120,
    *,
    name: str | None = None,
) -> VerificationResult:
    result_name = name or " ".join((Path(command[0]).name, command[1]))
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return VerificationResult(result_name, False, str(exc))
    output = (completed.stdout or completed.stderr).strip().splitlines()
    detail = output[-1] if output else f"exit={completed.returncode}"
    return VerificationResult(result_name, completed.returncode == 0, detail)


def verify_r0(
    project_root: Path, run_dbt: bool = True, run_compose: bool = False
) -> list[VerificationResult]:
    """Run deterministic checks that constitute the executable portion of G0."""

    required = [
        "pyproject.toml",
        "uv.lock",
        "configs/project.yaml",
        "contracts/schemas/event-envelope.schema.json",
        "docs/evidence/R0_GATE.md",
        "dbt/dbt_project.yml",
        "infra/compose.yaml",
    ]
    missing = [path for path in required if not (project_root / path).exists()]
    results = [
        VerificationResult(
            "required R0 files",
            not missing,
            "all present" if not missing else f"missing: {', '.join(missing)}",
        )
    ]

    try:
        from nexus.contracts.validation import validate_contract_examples

        summary = validate_contract_examples(project_root / "contracts" / "examples")
        results.append(
            VerificationResult("contract examples", True, f"validated={summary['validated']}")
        )
    except Exception as exc:  # pragma: no cover - surfaced as release-gate evidence
        results.append(VerificationResult("contract examples", False, str(exc)))

    try:
        proof = run_storage_proof(project_root / "artifacts" / "r0" / "storage")
        results.append(
            VerificationResult(
                "storage proof",
                proof.raw_rows == 5 and proof.curated_rows == 2,
                (
                    f"raw={proof.raw_rows}, curated={proof.curated_rows}, "
                    f"hash={proof.curated_content_sha256[:12]}"
                ),
            )
        )
    except Exception as exc:  # pragma: no cover - surfaced as release-gate evidence
        results.append(VerificationResult("storage proof", False, str(exc)))

    if run_dbt:
        dbt = shutil.which("dbt")
        if dbt is None:
            results.append(VerificationResult("dbt build", False, "dbt executable not found"))
        else:
            _prepare_dbt_output_path(project_root)
            results.append(
                _run(
                    [
                        dbt,
                        "build",
                        "--project-dir",
                        "dbt",
                        "--profiles-dir",
                        "dbt",
                        "--target",
                        "ci",
                    ],
                    project_root,
                    timeout=180,
                    name="dbt build",
                )
            )

    if run_compose:
        docker = shutil.which("docker")
        if docker is None:
            results.append(VerificationResult("compose config", False, "docker not found"))
        else:
            results.append(
                _run(
                    [docker, "compose", "-f", "infra/compose.yaml", "config", "--quiet"],
                    project_root,
                    name="compose config",
                )
            )
    return results
