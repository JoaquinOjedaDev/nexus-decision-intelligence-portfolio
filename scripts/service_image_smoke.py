"""Network-free image smoke; run as UID 10001 with fresh /evidence mounted.

This proves a fresh Linux producer/verifier, not the service or performance gates.
The launcher must enforce --network none, --read-only and bounded resources.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import platform
from pathlib import Path
from time import perf_counter

from nexus.portfolio.runtime_pipeline import (
    build_runtime_seed,
    runtime_code_fingerprint,
    verify_runtime_current,
)
from nexus.storage import file_sha256


def main() -> None:
    if platform.system() != "Linux" or os.getuid() != 10001:
        raise ValueError("Run only in the non-root service image")
    project = Path("/app")
    evidence = Path("/evidence")
    runtime = evidence / "runtime"
    receipt_path = evidence / "receipt.json"
    if (
        not evidence.is_dir()
        or evidence.resolve() != evidence
        or runtime.exists()
        or receipt_path.exists()
    ):
        raise ValueError("Mount a fresh, unlinked evidence directory")
    if os.access(project / "src/nexus/config.py", os.W_OK):
        raise ValueError("Application source must not be writable")
    if (project / "artifacts").exists() or (project / ".git").exists():
        raise ValueError("Host evidence or git state entered the application image")
    started = perf_counter()
    seed = build_runtime_seed(project, runtime)
    built = perf_counter()
    if verify_runtime_current(project, runtime) != seed:
        raise ValueError("Independent verification differs from the producer")
    results = list(runtime.rglob("run_results.json"))
    if len(results) != 1:
        raise ValueError("Expected exactly one dbt invocation")
    run = json.loads(results[0].read_text())
    if len(run["results"]) != 189 or any(
        row["status"] not in {"success", "pass"} for row in run["results"]
    ):
        raise ValueError("The unchanged dbt result set did not pass")
    receipt = {
        "contract": "nexus.service-image-smoke.v1",
        "status": "passed",
        "full_service_gate_claimed": False,
        "uid": os.getuid(),
        "python": platform.python_version(),
        "platform": platform.system(),
        "build_seconds": built - started,
        "total_seconds": perf_counter() - started,
        "dbt_results": 189,
        "seed_hash": seed["seed_hash"],
        "governing_code_sha256": runtime_code_fingerprint(project),
        "source_read_only": True,
        "host_artifacts_absent": True,
        "lock_sha256": file_sha256(project / "uv.lock"),
        "harness_sha256": file_sha256(Path(__file__)),
        "dependencies": {
            name: importlib.metadata.version(name)
            for name in (
                "psutil",
                "psycopg",
                "confluent-kafka",
                "dbt-core",
                "dbt-duckdb",
                "ortools",
            )
        },
    }
    with receipt_path.open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=2, allow_nan=False)
        handle.write("\n")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
