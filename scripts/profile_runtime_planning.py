"""Diagnostic fresh-build wall timings, never the asynchronous service p95 gate.

Run sequentially from the checkout with dbt on PATH. No database services, model
providers or demo state are contacted. Every run owns fresh artifact directories.
"""

from __future__ import annotations

import argparse
import functools
import hashlib
import importlib.metadata
import json
import platform
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from nexus import r2
from nexus.portfolio import runtime_pipeline as pipeline

PROJECT = Path(__file__).resolve().parents[1]
STAGES = (
    (r2, "materialize_r2_validation_checkpoint"),
    (r2, "_run_dbt"),
    (r2, "_table_rows"),
    (pipeline, "run_r2_rebuild"),
    (pipeline, "materialize_runtime_forecast"),
    (pipeline, "solve_constrained_policy"),
    (pipeline, "verify_runtime_current"),
)


@contextmanager
def timed_stages(rows: list[dict[str, Any]]) -> Iterator[None]:
    """Process-local diagnostic wrappers; nested times must not be summed."""
    originals = []

    def wrapper(original: Any, name: str) -> Any:
        @functools.wraps(original)
        def measured(*args: Any, **kwargs: Any) -> Any:
            started = time.perf_counter()
            passed = False
            try:
                result = original(*args, **kwargs)
                passed = True
                return result
            finally:
                rows.append(
                    {"stage": name, "seconds": time.perf_counter() - started, "passed": passed}
                )

        return measured

    try:
        for module, name in STAGES:
            original = getattr(module, name)
            originals.append((module, name, original))
            setattr(module, name, wrapper(original, name))
        yield
    finally:
        for module, name, original in reversed(originals):
            setattr(module, name, original)


def fresh_output(project: Path, output: Path) -> Path:
    candidate = output.absolute()
    artifacts = (project / "artifacts").resolve()
    if (
        ".." in candidate.parts
        or candidate.anchor.startswith(("\\\\", "//"))
        or any(p.is_symlink() or p.is_junction() for p in (*candidate.parents, candidate))
        or not candidate.resolve().is_relative_to(artifacts)
        or candidate.resolve() == artifacts
        or candidate.exists()
    ):
        raise ValueError("Use a fresh, unlinked directory strictly inside project artifacts")
    candidate.mkdir(parents=True, exist_ok=False)
    return candidate


def profile(project: Path, output: Path, iterations: int) -> dict[str, Any]:
    if type(iterations) is not int or not 1 <= iterations <= 20:
        raise ValueError("Choose 1 through 20 fresh diagnostic builds")
    root = fresh_output(project, output)
    receipt: dict[str, Any] = {
        "contract": "nexus.planning-stage-profile.v1",
        "claim": "isolated_fresh_build_diagnostic_not_async_service_p95",
        "nested_stage_times_are_not_additive": True,
        "synthetic": True,
        "python": platform.python_version(),
        "platform": platform.system(),
        "dependencies": {
            name: importlib.metadata.version(name)
            for name in ("dbt-core", "dbt-duckdb", "duckdb", "ortools")
        },
        "governing_code_sha256": pipeline.runtime_code_fingerprint(project),
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "runs": [],
    }
    for index in range(iterations):
        stages: list[dict[str, Any]] = []
        with timed_stages(stages):
            started = time.perf_counter()
            seed = pipeline.build_runtime_seed(project, root / f"run-{index + 1}")
            built = time.perf_counter()
            verified = pipeline.verify_runtime_current(project, root / f"run-{index + 1}")
            finished = time.perf_counter()
        if seed != verified:
            raise ValueError("Fresh producer differs from independently verified evidence")
        receipt["runs"].append(
            {
                "iteration": index + 1,
                "build_seconds": built - started,
                "verify_seconds": finished - built,
                "seed_hash": seed["seed_hash"],
                "stages": stages,
            }
        )
    if pipeline.runtime_code_fingerprint(project) != receipt["governing_code_sha256"]:
        raise ValueError("Governing code changed during diagnostic run")
    with (root / "profile.json").open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=3)
    args = parser.parse_args()
    result = profile(PROJECT, args.output, args.iterations)
    print(
        json.dumps(
            {
                "claim": result["claim"],
                "build_seconds": [r["build_seconds"] for r in result["runs"]],
            }
        )
    )


if __name__ == "__main__":
    main()
