from __future__ import annotations

from pathlib import Path

import pytest

import nexus.modeling.r3b_resources as resources
from nexus.modeling.r3b_contracts import load_r3b_config
from nexus.modeling.r3b_resources import (
    ARTIFACT_METHOD,
    NATIVE_THREAD_METHOD,
    RSS_METHOD,
    R3BResourceMonitor,
)

pytestmark = pytest.mark.r3b


def test_real_process_tree_measurement_evaluates_the_frozen_smoke_budget(tmp_path: Path) -> None:
    (tmp_path / "tracking.sqlite").write_bytes(b"tracking")
    model_directory = tmp_path / "models"
    model_directory.mkdir()
    (model_directory / "candidate.txt").write_bytes(b"candidate-model")

    monitor = R3BResourceMonitor(
        load_r3b_config(),
        governed_artifact_roots=(tmp_path,),
        sample_interval_seconds=0.005,
    )
    with monitor:
        # Allocate observable in-process work without starting an unmanaged child process.
        payload = bytearray(1024 * 1024)
        payload[0] = 1

    evidence = monitor.evidence
    assert payload[0] == 1
    assert evidence.measurement_errors == ()
    assert evidence.peak_process_tree_rss_bytes > 0
    assert evidence.peak_process_tree_native_threads >= 1
    assert evidence.maximum_descendant_count >= 0
    assert evidence.surviving_descendant_pids == ()
    assert evidence.governed_artifact_bytes == len(b"tracking") + len(b"candidate-model")
    assert evidence.governed_artifact_file_count == 2
    assert evidence.passed is all(check.passed for check in evidence.checks)


def test_receipt_has_explicit_methods_and_individual_pass_fail_evidence(tmp_path: Path) -> None:
    artifact = tmp_path / "forecast.parquet"
    artifact.write_bytes(b"12345")

    with R3BResourceMonitor(
        load_r3b_config(), governed_artifact_roots=(tmp_path, artifact)
    ) as monitor:
        pass

    receipt = monitor.evidence.to_dict()
    checks = {str(check["name"]): check for check in receipt["checks"]}
    methods = receipt["measurement"]

    expected_status = "PASS" if all(check["passed"] for check in receipt["checks"]) else "FAIL"
    assert receipt["status"] == expected_status
    assert receipt["budget"]["wall_time_seconds"] == 300
    assert receipt["budget"]["maximum_process_tree_threads"] == (
        load_r3b_config().resource_budget.maximum_process_tree_threads
    )
    assert receipt["observed"]["governed_artifact_bytes"] == 5
    assert checks["wall_time_seconds"]["status"] == "PASS"
    assert checks["peak_process_tree_rss_bytes"]["measurement_method"] == RSS_METHOD
    assert checks["peak_process_tree_native_threads"]["measurement_method"] == NATIVE_THREAD_METHOD
    assert checks["governed_artifact_bytes"]["measurement_method"] == ARTIFACT_METHOD
    assert methods["sample_interval_seconds"] == 0.02


def test_context_does_not_suppress_workload_exceptions_and_still_finalizes(tmp_path: Path) -> None:
    monitor = R3BResourceMonitor(load_r3b_config(), governed_artifact_roots=(tmp_path,))

    with pytest.raises(RuntimeError, match="workload failed"), monitor:
        raise RuntimeError("workload failed")

    assert monitor.evidence.wall_time_seconds > 0
    assert monitor.evidence.surviving_descendant_pids == ()


def test_evidence_is_unavailable_until_stop_and_stop_is_idempotent(tmp_path: Path) -> None:
    monitor = R3BResourceMonitor(load_r3b_config(), governed_artifact_roots=(tmp_path,))
    with pytest.raises(RuntimeError, match="only after"):
        _ = monitor.evidence

    monitor.start()
    first = monitor.stop()
    second = monitor.stop()

    assert first is second
    assert first.passed is all(check.passed for check in first.checks)


@pytest.mark.parametrize("interval", [0.0, 0.0009, 1.0001])
def test_sampler_interval_is_strictly_bounded(tmp_path: Path, interval: float) -> None:
    with pytest.raises(ValueError, match=r"between 0\.001 and 1\.0"):
        R3BResourceMonitor(
            load_r3b_config(),
            governed_artifact_roots=(tmp_path,),
            sample_interval_seconds=interval,
        )


def test_empty_governed_scope_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least one governed artifact root"):
        R3BResourceMonitor(load_r3b_config(), governed_artifact_roots=())


def test_artifact_measurement_error_fails_the_gate_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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

    with R3BResourceMonitor(load_r3b_config(), governed_artifact_roots=(tmp_path,)) as monitor:
        pass

    checks = {check.name: check for check in monitor.evidence.checks}
    assert not monitor.evidence.passed
    assert checks["measurement_integrity"].passed is False
    assert checks["governed_artifact_bytes"].passed is False
    assert monitor.evidence.measurement_errors
