"""Real sequential dbt reuse with frozen data parity and released DuckDB handles."""

import json
import time
from datetime import UTC, datetime
from pathlib import Path

from nexus.data.r2_dbt_execution import WorkerDbtRunner, current_dbt_executor, use_dbt_executor
from nexus.r2 import run_r2_rebuild
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import file_sha256

ROOT = Path(__file__).resolve().parents[1]


def test_owned_runner_reuses_imports_not_manifests_or_database_handles(tmp_path):
    source = generate_r2_smoke_fixture(tmp_path / "source")
    expected = json.loads((ROOT / "fixtures/r2/expected.json").read_text())
    runner = WorkerDbtRunner()
    evidence = []
    for index, day in enumerate((20, 30, 20)):
        cutoff = datetime(2026, 6, day, tzinfo=UTC)
        started = time.perf_counter()
        with use_dbt_executor(runner):
            result = run_r2_rebuild(
                project_root=ROOT,
                source_dir=source.source_dir,
                artifact_dir=tmp_path / f"attempt-{index}",
                planning_cutoff=cutoff,
            )
        assert current_dbt_executor() is None
        elapsed = time.perf_counter() - started
        artifacts = result.database_path.with_suffix(".dbt")
        run = json.loads((artifacts / "target/run_results.json").read_text())
        assert len(run["results"]) == 189
        assert all(row["status"] in {"pass", "success"} for row in run["results"])
        hashes = {artifact.dataset: artifact.content_sha256 for artifact in result.gold_artifacts}
        assert hashes == expected["gold_hashes"]["early" if day == 20 else "final"]
        evidence.append(
            {
                "cutoff": cutoff.isoformat(),
                "elapsed_seconds": elapsed,
                "dbt_results": len(run["results"]),
                "invocation_id": run["metadata"]["invocation_id"],
                "gold_hashes_match_frozen_fixture": True,
                "run_results_sha256": file_sha256(artifacts / "target/run_results.json"),
            }
        )
    assert len({row["invocation_id"] for row in evidence}) == 3
    assert not runner.failed
    (tmp_path / "owned-dbt-receipt.json").write_text(
        json.dumps(
            {
                "contract": "nexus.owned-dbt-acceptance.v1",
                "status": "passed",
                "asynchronous_service_p95_claimed": False,
                "runs": evidence,
                "code_sha256": {
                    name: file_sha256(ROOT / name)
                    for name in (
                        "src/nexus/r2.py",
                        "src/nexus/data/r2_dbt_execution.py",
                        "src/nexus/portfolio/service_worker.py",
                        "tests/test_r2_owned_dbt_service.py",
                    )
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
