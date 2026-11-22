from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

import nexus.ingestion.r2d_resources as resources
from nexus.ingestion import R2DResourceMonitor, load_r2d_config


def test_cleanup_counts_only_children_started_during_monitoring(tmp_path: Path) -> None:
    baseline = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(5)"])
    monitor = R2DResourceMonitor(load_r2d_config(), governed_artifact_roots=(tmp_path,))
    started: subprocess.Popen[bytes] | None = None
    try:
        monitor.start()
        started = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(5)"])
        survivors, complete = monitor._surviving_descendants_once()
        assert complete
        assert baseline.pid not in survivors
        assert started.pid in survivors
    finally:
        if started is not None:
            started.terminate()
            started.wait(timeout=5)
        baseline.terminate()
        baseline.wait(timeout=5)
        monitor.stop()


def test_real_context_measures_process_tree_artifacts_and_clean_shutdown(
    tmp_path: Path,
) -> None:
    (tmp_path / "receipt.json").write_bytes(b"receipt")
    nested = tmp_path / "source"
    nested.mkdir()
    (nested / "inventory_events.jsonl").write_bytes(b"event-one\nevent-two\n")

    monitor = R2DResourceMonitor(
        load_r2d_config(),
        governed_artifact_roots=(tmp_path,),
        sample_interval_seconds=0.005,
    )
    with monitor:
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(0.05)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        assert child.wait(timeout=5) == 0
        payload = bytearray(1024 * 1024)
        payload[0] = 1

    evidence = monitor.evidence
    assert payload[0] == 1
    assert evidence.measurement_errors == ()
    assert evidence.peak_process_tree_rss_bytes > 0
    assert evidence.peak_process_tree_native_threads >= 1
    assert evidence.maximum_descendant_count >= 1
    assert evidence.surviving_descendant_pids == ()
    assert evidence.governed_artifact_bytes == len(b"receipt") + len(b"event-one\nevent-two\n")
    assert evidence.governed_artifact_file_count == 2
    assert evidence.passed


def test_evidence_serializes_exact_budgets_and_hashes_exact_observations(tmp_path: Path) -> None:
    artifact = tmp_path / "checkpoint.json"
    artifact.write_bytes(b"checkpoint")

    with R2DResourceMonitor(
        load_r2d_config(),
        governed_artifact_roots=(tmp_path, artifact),
    ) as monitor:
        pass

    evidence = monitor.evidence
    payload = evidence.to_dict()
    assert payload["budget"] == {
        "wall_time_seconds": 300,
        "peak_process_tree_rss_mib": 4096,
        "maximum_process_tree_threads": 256,
        "artifact_mib": 1024,
        "shutdown_seconds": 10,
        "maximum_surviving_descendants": 0,
    }
    assert payload["observed"]["governed_artifact_bytes"] == len(b"checkpoint")
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    assert evidence.semantic_sha256 == hashlib.sha256(encoded).hexdigest()
    assert evidence.semantic_sha256 == evidence.semantic_sha256


def test_context_preserves_workload_exception_and_still_finalizes(tmp_path: Path) -> None:
    monitor = R2DResourceMonitor(load_r2d_config(), governed_artifact_roots=(tmp_path,))

    with pytest.raises(RuntimeError, match="workload failed"), monitor:
        raise RuntimeError("workload failed")

    assert monitor.evidence.wall_time_seconds > 0
    assert monitor.evidence.surviving_descendant_pids == ()


def test_artifact_measurement_failure_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        resources,
        "_measure_governed_artifacts",
        lambda roots: resources._ArtifactUsage(
            bytes=0,
            file_count=0,
            errors=(f"cannot measure {roots[0]}",),
        ),
    )

    with R2DResourceMonitor(
        load_r2d_config(),
        governed_artifact_roots=(tmp_path,),
    ) as monitor:
        pass

    checks = {check.name: check for check in monitor.evidence.checks}
    assert not monitor.evidence.passed
    assert checks["measurement_integrity"].passed is False
    assert checks["governed_artifact_bytes"].passed is False
    assert monitor.evidence.measurement_errors


def test_sampler_failure_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_sampler(root: object) -> resources._ProcessSample:
        del root
        raise RuntimeError("injected sampler failure")

    monkeypatch.setattr(resources, "_sample_process_tree", fail_sampler)

    with R2DResourceMonitor(
        load_r2d_config(),
        governed_artifact_roots=(tmp_path,),
        sample_interval_seconds=0.005,
    ) as monitor:
        pass

    assert not monitor.evidence.passed
    assert any("injected sampler failure" in item for item in monitor.evidence.measurement_errors)
    assert (
        next(
            check for check in monitor.evidence.checks if check.name == "measurement_integrity"
        ).passed
        is False
    )


@pytest.mark.parametrize("interval", [0.0, 0.0009, 1.0001])
def test_monitor_rejects_unbounded_sampler_intervals(tmp_path: Path, interval: float) -> None:
    with pytest.raises(ValueError, match=r"between 0\.001 and 1\.0"):
        R2DResourceMonitor(
            load_r2d_config(),
            governed_artifact_roots=(tmp_path,),
            sample_interval_seconds=interval,
        )


def test_monitor_requires_a_governed_artifact_scope() -> None:
    with pytest.raises(ValueError, match="at least one governed artifact root"):
        R2DResourceMonitor(load_r2d_config(), governed_artifact_roots=())
