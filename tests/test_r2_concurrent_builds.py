"""Independent real dbt attempts must not share compiled state or logs."""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier

from nexus import r2
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import file_sha256

ROOT = Path(__file__).resolve().parents[1]


def test_parallel_cutoffs_retain_independent_dbt_evidence(tmp_path, monkeypatch):
    source = generate_r2_smoke_fixture(tmp_path / "source")
    expected = json.loads((ROOT / "fixtures/r2/expected.json").read_text())
    before = {
        path: file_sha256(path)
        for folder in (ROOT / "dbt/target", ROOT / "dbt/logs")
        if folder.exists()
        for path in folder.rglob("*")
        if path.is_file()
    }
    original = r2._run_dbt
    barrier = Barrier(2, timeout=30)

    def synchronized(**kwargs):
        barrier.wait()
        return original(**kwargs)

    monkeypatch.setattr(r2, "_run_dbt", synchronized)
    cutoffs = [datetime(2026, 6, 20, tzinfo=UTC), datetime(2026, 6, 30, tzinfo=UTC)]

    def build(index):
        return r2.run_r2_rebuild(
            project_root=ROOT,
            source_dir=source.source_dir,
            artifact_dir=tmp_path / f"attempt-{index}",
            planning_cutoff=cutoffs[index],
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(build, (0, 1)))
    invocations = []
    evidence = []
    for index, result in enumerate(results):
        artifacts = result.database_path.with_suffix(".dbt")
        run = json.loads((artifacts / "target/run_results.json").read_text())
        manifest = json.loads((artifacts / "target/manifest.json").read_text())
        assert len(run["results"]) == 189
        assert all(item["status"] in {"pass", "success"} for item in run["results"])
        assert run["metadata"]["invocation_id"] == manifest["metadata"]["invocation_id"]
        invocations.append(run["metadata"]["invocation_id"])
        assert Path(run["args"]["target_path"]).resolve() == artifacts / "target"
        assert run["args"]["vars"]["r2_planning_cutoff"] == cutoffs[index].isoformat()
        assert (artifacts / "logs/dbt.log").is_file()
        for path in run["args"]["vars"]["r2_validated_files"].values():
            assert Path(path).resolve().is_relative_to(tmp_path / f"attempt-{index}")
        summary = json.loads(result.summary_path.read_text())
        assert summary["status"] == "PASS"
        hashes = {artifact.dataset: artifact.content_sha256 for artifact in result.gold_artifacts}
        assert hashes == expected["gold_hashes"]["early" if index == 0 else "final"]
        evidence.append(
            {
                "cutoff": cutoffs[index].isoformat(),
                "invocation_id": invocations[-1],
                "dbt_results": len(run["results"]),
                "dbt_elapsed_seconds": run["elapsed_time"],
                "gold_hashes_match_frozen_fixture": True,
                "gold_hashes": hashes,
                "run_results_sha256": file_sha256(artifacts / "target/run_results.json"),
                "dbt_manifest_sha256": file_sha256(artifacts / "target/manifest.json"),
            }
        )
    assert len(set(invocations)) == 2
    assert results[0].semantic_manifest_sha256 != results[1].semantic_manifest_sha256
    after = {
        path: file_sha256(path)
        for folder in (ROOT / "dbt/target", ROOT / "dbt/logs")
        if folder.exists()
        for path in folder.rglob("*")
        if path.is_file()
    }
    assert after == before
    (tmp_path / "dbt-isolation-receipt.json").write_text(
        json.dumps(
            {
                "contract": "nexus.dbt-attempt-isolation.v1",
                "status": "passed",
                "production_sha256": file_sha256(ROOT / "src/nexus/r2.py"),
                "test_sha256": file_sha256(Path(__file__)),
                "concurrent_subprocesses": 2,
                "shared_project_artifacts_unchanged": True,
                "performance_gate_claimed": False,
                "attempts": evidence,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
