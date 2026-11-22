"""Real local MLflow persistence, optional dependency and fail-closed boundaries."""

from __future__ import annotations

import builtins
import copy
import json
import os
import socket
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit
from urllib.request import url2pathname

import pytest
from test_portfolio_training_governance import trained as trained

from nexus.portfolio import training_tracking as tracking
from nexus.portfolio.learning import SCIENTIFIC_CHAMPION
from nexus.portfolio.training_pipeline import LearningRun
from nexus.portfolio.training_tracking import EXPERIMENT, TrainingTrackingError, track_learning_run
from nexus.storage import file_sha256


def _directory(project: Path) -> Path:
    return project / "artifacts/local-tracking"


def _artifact_directory(receipt: dict[str, Any]) -> Path:
    return Path(url2pathname(urlsplit(receipt["artifact_uri"]).path))


@pytest.fixture
def sdk(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv("MLFLOW_DISABLE_TELEMETRY", "true")
    monkeypatch.setenv("MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING", "false")

    def no_network(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Local MLflow provenance attempted a network connection")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    module = pytest.importorskip(
        "mlflow", reason="actual tracking requires optional tracking group"
    )
    return module


@pytest.mark.parametrize(
    "uri",
    [
        "https://mlflow.example.test",
        "http://localhost:5000",
        "databricks",
        "postgresql://localhost/tracking",
        "file:///tmp/tracking",
        "sqlite:///relative.sqlite",
        "sqlite:////server/share/remote.sqlite",
        "sqlite:///C:/unrelated.sqlite",
    ],
)
def test_remote_or_unbound_uri_denied_before_sdk_import(
    uri: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "mlflow" or name.startswith("mlflow."):
            pytest.fail("MLflow import occurred before destination validation")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    with pytest.raises(TrainingTrackingError, match="local SQLite"):
        track_learning_run(
            cast(LearningRun, None), _directory(tmp_path), project_root=tmp_path, tracking_uri=uri
        )
    assert not _directory(tmp_path).exists()


@pytest.mark.parametrize("location", ["project", "artifacts", "outside", "unc"])
def test_non_scoped_output_rejected_before_learning_or_sdk(
    location: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    choices = {
        "project": tmp_path,
        "artifacts": tmp_path / "artifacts",
        "outside": tmp_path.parent / "outside-tracking",
        "unc": Path("//server/share/tracking"),
    }
    monkeypatch.setattr(tracking, "verify_learning_run", lambda _: pytest.fail("run inspected"))
    with pytest.raises(TrainingTrackingError):
        track_learning_run(cast(LearningRun, None), choices[location], project_root=tmp_path)


def test_optional_sdk_missing_has_actionable_error_and_restores_environment(
    trained: LearningRun, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_import = builtins.__import__
    monkeypatch.setenv("MLFLOW_DISABLE_TELEMETRY", "caller-value")

    def unavailable(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "mlflow":
            raise ImportError("intentionally unavailable optional dependency")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", unavailable)
    with pytest.raises(TrainingTrackingError, match="optional 'tracking'"):
        track_learning_run(trained, _directory(tmp_path), project_root=tmp_path)
    assert os.environ["MLFLOW_DISABLE_TELEMETRY"] == "caller-value"


def test_real_local_artifacts_readback_idempotency_and_no_registry_promotion(
    sdk: Any, trained: LearningRun, tmp_path: Path
) -> None:
    before_uri = sdk.get_tracking_uri()
    first = track_learning_run(trained, _directory(tmp_path), project_root=tmp_path)
    second = track_learning_run(
        trained, _directory(tmp_path), project_root=tmp_path, tracking_uri=first["tracking_uri"]
    )
    assert first == second
    assert sdk.get_tracking_uri() == before_uri
    client = sdk.MlflowClient(
        tracking_uri=first["tracking_uri"], registry_uri=first["tracking_uri"]
    )
    runs = client.search_runs([first["experiment_id"]])
    assert len(runs) == 1 and runs[0].info.status == "FINISHED"
    assert client.search_registered_models() == []
    assert first["readback_verified"] is True
    assert first["tracking_environment"]["mlflow"] == "3.17.2"
    assert first["tracking_implementation_sha256"] == file_sha256(Path(tracking.__file__))
    assert first["scientific_champion"] == SCIENTIFIC_CHAMPION
    assert (
        first["model_registration_performed"]
        is first["model_promotion_performed"]
        is first["deployment_applied"]
        is False
    )
    directory = _artifact_directory(first)
    assert len(first["artifact_sha256"]) == 9
    assert {name: file_sha256(directory / name) for name in first["artifact_sha256"]} == first[
        "artifact_sha256"
    ]
    assert trained.candidate is not None and trained.baseline_artifact is not None
    assert (directory / "model/candidate.json").read_bytes() == trained.candidate.to_bytes()
    assert (
        directory / "model/calibrated-baseline.json"
    ).read_bytes() == trained.baseline_artifact.payload
    assert json.loads((directory / "evidence/learning-receipt.json").read_text()) == json.loads(
        json.dumps(trained.receipt)
    )
    assert first["bindings"] == trained.receipt["bindings"]
    assert len(client.get_metric_history(first["run_id"], "candidate_wis")) == 1


@pytest.mark.parametrize(
    "uri",
    [
        "http://127.0.0.1:5000",
        "https://tracking.example.test",
        "databricks",
        "file:///tmp/tracking",
        "sqlite://remote/tracking",
        "sqlite:///:memory:",
        "sqlite:///tracking.db?gateway=true",
    ],
)
def test_sdk_rejects_server_or_gateway_configuration_before_import(
    uri: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_import = builtins.__import__

    def observe_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "mlflow" or name.startswith("mlflow."):
            pytest.fail("MLflow imported before rejecting unsupported tracking profile")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", observe_import)
    with (
        pytest.raises(TrainingTrackingError, match="local SQLite SDK"),
        tracking._local_sdk(uri),
    ):
        pytest.fail("unsupported SDK client created")


@pytest.mark.parametrize(
    "artifact",
    [
        "model/candidate.json",
        "model/calibrated-baseline.json",
        "evidence/snapshot.json",
        "evidence/evaluation.json",
        "evidence/bindings.json",
    ],
)
def test_real_retained_artifact_tamper_is_detected_not_repaired(
    artifact: str, sdk: Any, trained: LearningRun, tmp_path: Path
) -> None:
    receipt = track_learning_run(trained, _directory(tmp_path), project_root=tmp_path)
    path = _artifact_directory(receipt) / artifact
    path.write_bytes(b"tampered artifact bytes")
    with pytest.raises(TrainingTrackingError, match="artifact bytes"):
        track_learning_run(trained, _directory(tmp_path), project_root=tmp_path)
    assert path.read_bytes() == b"tampered artifact bytes"


def test_finished_run_missing_artifact_is_not_silently_completed(
    sdk: Any, trained: LearningRun, tmp_path: Path
) -> None:
    receipt = track_learning_run(trained, _directory(tmp_path), project_root=tmp_path)
    path = _artifact_directory(receipt) / "evidence/plan.json"
    path.rename(path.with_name("unexpected-plan.json"))
    with pytest.raises(TrainingTrackingError, match=r"artifact bytes|missing artifacts"):
        track_learning_run(trained, _directory(tmp_path), project_root=tmp_path)
    assert not path.exists()


def test_real_partial_run_retry_finishes_once_without_duplicate_metrics(
    sdk: Any, trained: LearningRun, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = sdk.MlflowClient.log_artifact
    calls = 0

    def interrupted(self: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("injected interruption during artifact upload")
        return original(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(sdk.MlflowClient, "log_artifact", interrupted)
        with pytest.raises(RuntimeError, match="injected interruption"):
            track_learning_run(trained, _directory(tmp_path), project_root=tmp_path)
    receipt = track_learning_run(trained, _directory(tmp_path), project_root=tmp_path)
    client = sdk.MlflowClient(
        tracking_uri=receipt["tracking_uri"], registry_uri=receipt["tracking_uri"]
    )
    assert len(client.search_runs([receipt["experiment_id"]])) == 1
    assert len(client.get_metric_history(receipt["run_id"], "candidate_wis")) == 1
    assert receipt["readback_verified"] is True


def test_concurrent_actual_local_tracking_is_one_logical_run(
    sdk: Any, trained: LearningRun, tmp_path: Path
) -> None:
    with ThreadPoolExecutor(max_workers=4) as workers:
        receipts = list(
            workers.map(
                lambda _: track_learning_run(trained, _directory(tmp_path), project_root=tmp_path),
                range(4),
            )
        )
    assert all(receipt == receipts[0] for receipt in receipts)
    client = sdk.MlflowClient(
        tracking_uri=receipts[0]["tracking_uri"], registry_uri=receipts[0]["tracking_uri"]
    )
    assert len(client.search_runs([receipts[0]["experiment_id"]])) == 1


@pytest.mark.parametrize("alteration", ["metric", "tag", "deleted", "duplicate", "index"])
def test_mutated_persisted_identity_or_metadata_is_denied(
    alteration: str, sdk: Any, trained: LearningRun, tmp_path: Path
) -> None:
    root = _directory(tmp_path)
    receipt = track_learning_run(trained, root, project_root=tmp_path)
    client = sdk.MlflowClient(
        tracking_uri=receipt["tracking_uri"], registry_uri=receipt["tracking_uri"]
    )
    if alteration == "metric":
        client.log_metric(receipt["run_id"], "candidate_wis", 99, synchronous=True)
    elif alteration == "tag":
        client.set_tag(receipt["run_id"], "model_promotion", "promoted")
    elif alteration == "deleted":
        client.delete_run(receipt["run_id"])
    elif alteration == "duplicate":
        client.create_run(
            receipt["experiment_id"], tags={"logical_identity": receipt["logical_identity"]}
        )
    else:
        with closing(sqlite3.connect(root / "provenance-index.sqlite3")) as connection:
            connection.execute("UPDATE runs SET receipt='{}'")
            connection.commit()
    with pytest.raises(TrainingTrackingError):
        track_learning_run(trained, root, project_root=tmp_path)


def test_remote_experiment_artifact_location_is_rejected_before_artifact_access(
    sdk: Any, trained: LearningRun, tmp_path: Path
) -> None:
    root = _directory(tmp_path)
    root.mkdir(parents=True)
    uri = f"sqlite:///{(root / 'mlflow.sqlite3').as_posix()}"
    client = sdk.MlflowClient(tracking_uri=uri, registry_uri=uri)
    client.create_experiment(EXPERIMENT, artifact_location="https://remote.example.test/artifacts")
    with pytest.raises(TrainingTrackingError, match="local file"):
        track_learning_run(trained, root, project_root=tmp_path)


@pytest.mark.parametrize("alteration", ["receipt", "candidate"])
def test_changed_learning_input_is_denied_before_tracking(
    alteration: str, trained: LearningRun, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = copy.deepcopy(trained)
    if alteration == "receipt":
        run.receipt["metrics"]["candidate_wis"] = "0.01"
    else:
        assert run.candidate is not None
        run = replace(
            run, candidate=replace(run.candidate, normalized_training_input_sha256="e" * 64)
        )
    monkeypatch.setattr(tracking, "_local_sdk", lambda _: pytest.fail("tracking SDK called"))
    with pytest.raises(ValueError, match="learning"):
        track_learning_run(run, _directory(tmp_path), project_root=tmp_path)
    assert not _directory(tmp_path).exists()


@pytest.mark.parametrize("alteration", ["source", "snapshot", "closure", "plan", "code", "split"])
def test_reencoded_but_internally_inconsistent_provenance_is_rejected(
    alteration: str, trained: LearningRun, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    receipt = copy.deepcopy(trained.receipt)
    if alteration == "source":
        receipt["bindings"]["source_snapshot_sha256"] = "a" * 64
    elif alteration == "snapshot":
        receipt["snapshot"]["labels"][0]["quantity"] += 1
    elif alteration == "closure":
        receipt["bindings"]["finalization_sha256"] = "b" * 64
    elif alteration == "plan":
        receipt["plan"]["bootstrap_seed"] += 1
    elif alteration == "code":
        receipt["code_files"]["uv.lock"] = "c" * 64
    else:
        receipt["split_rows"]["training"][0]["label"]["quantity"] += 1
    run = replace(trained, receipt=receipt)
    monkeypatch.setattr(tracking, "_local_sdk", lambda _: pytest.fail("tracking SDK called"))
    with pytest.raises(TrainingTrackingError, match=r"provenance|split rows"):
        track_learning_run(run, _directory(tmp_path), project_root=tmp_path)
    assert not _directory(tmp_path).exists()
