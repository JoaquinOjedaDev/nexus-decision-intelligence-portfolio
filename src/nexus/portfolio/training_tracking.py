"""Optional local MLflow provenance for runtime learning; no deployment authority.

The older R3B logger is deliberately not called: it creates a new registered
model on each invocation and changes the process-global tracking destination.
This adapter uses an explicit local client and verifies retained artifact bytes.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import re
import sqlite3
import tempfile
import threading
from collections.abc import Iterator
from contextlib import closing, contextmanager
from importlib.metadata import version
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import url2pathname

from nexus.modeling.r3_artifacts import canonical_json_sha256
from nexus.portfolio.learning import SCIENTIFIC_CHAMPION
from nexus.portfolio.training_pipeline import LearningRun, verify_learning_run
from nexus.storage import file_sha256

CONTRACT = "nexus.runtime-learning-mlflow.v1"
EXPERIMENT = "nexus-runtime-learning-local-v1"
_SDK_LOCK = threading.RLock()


class TrainingTrackingError(ValueError):
    """A tracking destination or retained provenance failed its local contract."""


def _encode(value: Any) -> bytes:
    return json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _local_directory(output_dir: Path, project_root: Path) -> Path:
    if not isinstance(output_dir, Path) or not isinstance(project_root, Path):
        raise TrainingTrackingError("tracking directories must be explicit local Path objects")
    if any(str(path).startswith(("\\\\", "//")) for path in (output_dir, project_root)):
        raise TrainingTrackingError("remote/UNC tracking directories are forbidden")
    project = project_root.resolve()
    base = (project / "artifacts").resolve()
    root = output_dir.resolve()
    if not base.is_relative_to(project) or root == base or not root.is_relative_to(base):
        raise TrainingTrackingError("tracking output must be inside project_root/artifacts")
    if str(root).startswith(("\\\\", "//")):
        raise TrainingTrackingError("remote/UNC tracking directories are forbidden")
    if root.exists() and not root.is_dir():
        raise TrainingTrackingError("tracking output is not a directory")
    return root


def _file_uri_path(uri: str, expected: Path, allowed_root: Path) -> Path:
    parsed = urlsplit(uri)
    if parsed.scheme != "file" or parsed.netloc or parsed.query or parsed.fragment:
        raise TrainingTrackingError("MLflow artifact URI is not the exact local file destination")
    actual = Path(url2pathname(parsed.path)).resolve()
    if actual != expected.resolve() or not actual.is_relative_to(allowed_root.resolve()):
        raise TrainingTrackingError("MLflow artifact destination escaped its local binding")
    return actual


@contextmanager
def _local_sdk(uri: str) -> Iterator[Any]:
    _validate_local_tracking_uri(uri)
    # MLflow reads telemetry flags dynamically. Serialize our temporary process
    # environment override, restore caller settings, and never change fluent URIs.
    with _SDK_LOCK:
        settings = {
            "MLFLOW_DISABLE_TELEMETRY": "true",
            "MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING": "false",
            "MLFLOW_ENABLE_ASYNC_LOGGING": "false",
        }
        previous = {name: os.environ.get(name) for name in settings}
        os.environ.update(settings)
        try:
            try:
                from mlflow import MlflowClient
            except ImportError as error:
                raise TrainingTrackingError(
                    "Local MLflow provenance requires the optional 'tracking' dependency group"
                ) from error
            yield MlflowClient(tracking_uri=uri, registry_uri=uri)
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def _validate_local_tracking_uri(uri: str) -> None:
    """The supported provenance profile never instantiates an HTTP/Gateway client."""
    parsed = urlsplit(uri)
    if (
        parsed.scheme != "sqlite"
        or not uri.startswith("sqlite:///")
        or parsed.netloc
        or parsed.query
        or parsed.fragment
        or not parsed.path
        or parsed.path == "/:memory:"
    ):
        raise TrainingTrackingError("only a local SQLite SDK tracking URI is allowed")


def _payloads(run: LearningRun) -> tuple[str, dict[str, bytes], dict[str, str], dict[str, float]]:
    verify_learning_run(run)
    if run.candidate is None or run.baseline_artifact is None:
        raise TrainingTrackingError("An abstained run has no learned artifact to track")
    receipt = json.loads(_encode(run.receipt))
    bindings, snapshot = receipt["bindings"], receipt["snapshot"]
    snapshot_hash = canonical_json_sha256(
        {name: value for name, value in snapshot.items() if name != "snapshot_sha256"}
    )
    if (
        snapshot_hash != snapshot["snapshot_sha256"]
        or snapshot_hash != bindings["snapshot_sha256"]
        or snapshot["source_snapshot_sha256"] != bindings["source_snapshot_sha256"]
        or snapshot["finalization_sha256"] != bindings["finalization_sha256"]
        or _sha(_encode(receipt["plan"])) != bindings["plan_sha256"]
        or _sha(_encode(receipt["code_files"])) != bindings["code_sha256"]
    ):
        raise TrainingTrackingError("learning source/snapshot/plan/code provenance is inconsistent")
    for name in ("training", "calibration", "final"):
        rows, declared = receipt["split_rows"][name], receipt["splits"][name]
        if len(rows) != declared["rows"] or _sha(_encode(rows)) != declared["sha256"]:
            raise TrainingTrackingError("learning split rows differ from their bound evidence")
    if (
        receipt.get("classification") != "synthetic"
        or receipt.get("scientific_champion_changed") is not False
    ):
        raise TrainingTrackingError("tracking cannot attest a scientific champion change")
    decision = receipt["scientific_decision"]
    if (
        decision.get("promotion_applied") is not False
        or decision.get("synthetic_evidence") is not True
    ):
        raise TrainingTrackingError("synthetic model promotion remains blocked")
    logical_id = _sha(
        _encode(
            {
                "contract": CONTRACT,
                "receipt_sha256": run.receipt_sha256,
                "candidate_sha256": run.candidate.sha256,
                "baseline_sha256": run.baseline_artifact.sha256,
            }
        )
    )
    payloads = {
        "model/candidate.json": run.candidate.to_bytes(),
        "model/calibrated-baseline.json": run.baseline_artifact.payload,
        "evidence/learning-receipt.json": _encode(receipt),
        "evidence/snapshot.json": _encode(receipt["snapshot"]),
        "evidence/plan.json": _encode(receipt["plan"]),
        "evidence/bindings.json": _encode(
            {name: receipt[name] for name in ("bindings", "code_files", "dependencies")}
        ),
        "evidence/evaluation.json": _encode(
            {
                name: receipt.get(name)
                for name in (
                    "metrics",
                    "quality_checks",
                    "segments",
                    "horizon_diagnostics",
                    "evaluation",
                )
            }
        ),
        "evidence/splits.json": _encode(
            {
                name: receipt.get(name)
                for name in ("splits", "split_rows", "selection", "calibration_support")
            }
        ),
    }
    payloads["evidence/artifact-index.json"] = _encode(
        {
            "contract": CONTRACT,
            "logical_identity": logical_id,
            "receipt_sha256": run.receipt_sha256,
            "files": {name: _sha(content) for name, content in sorted(payloads.items())},
            "scientific_champion": SCIENTIFIC_CHAMPION,
            "model_registration_performed": False,
            "model_promotion_performed": False,
        }
    )
    params = {
        "contract": CONTRACT,
        "receipt_sha256": run.receipt_sha256,
        "candidate_sha256": run.candidate.sha256,
        "baseline_sha256": run.baseline_artifact.sha256,
        "source_snapshot_sha256": receipt["bindings"]["source_snapshot_sha256"],
        "training_snapshot_sha256": receipt["bindings"]["snapshot_sha256"],
        "plan_sha256": receipt["bindings"]["plan_sha256"],
        "code_sha256": receipt["bindings"]["code_sha256"],
        "finalization_sha256": receipt["bindings"]["finalization_sha256"],
        "evaluation_status": receipt["status"],
    }
    metrics = {
        name: float(value)
        for name, value in receipt["metrics"].items()
        if name
        in {
            "candidate_wis",
            "baseline_wis",
            "relative_wis_improvement",
            "candidate_coverage80",
            "candidate_coverage95",
        }
        and value is not None
    }
    if any(not math.isfinite(value) for value in metrics.values()):
        raise TrainingTrackingError("MLflow metrics must be finite")
    if any(not isinstance(value, str) or not value for value in params.values()):
        raise TrainingTrackingError("MLflow provenance parameters require nonempty strings")
    verify_learning_run(run)
    return logical_id, payloads, params, metrics


def _retained_files(client: Any, run_id: str, directory: Path) -> dict[str, str]:
    output: dict[str, str] = {}
    pending = [""]
    while pending:
        for item in client.list_artifacts(run_id, pending.pop()):
            path = item.path
            if not isinstance(path, str) or not path or "\\" in path:
                raise TrainingTrackingError("MLflow artifact path is invalid")
            target = directory / path
            if target.resolve() == directory or not target.resolve().is_relative_to(directory):
                raise TrainingTrackingError("MLflow artifact path escaped the local run")
            if item.is_dir:
                if path not in {"model", "evidence"}:
                    raise TrainingTrackingError("unexpected MLflow artifact directory")
                pending.append(path)
            elif not target.is_file():
                raise TrainingTrackingError("MLflow listed an absent artifact file")
            else:
                output[path] = file_sha256(target)
    return output


def _verify_metadata(
    record: Any,
    params: dict[str, str],
    metrics: dict[str, float],
    tags: dict[str, str],
    *,
    complete: bool,
) -> None:
    for expected, actual, label in (
        (params, record.data.params, "parameters"),
        (metrics, record.data.metrics, "metrics"),
        (tags, record.data.tags, "tags"),
    ):
        if any(key in actual and actual[key] != value for key, value in expected.items()):
            raise TrainingTrackingError(f"MLflow retained {label} differ from the evaluated run")
        if complete and any(actual.get(key) != value for key, value in expected.items()):
            raise TrainingTrackingError(f"MLflow retained {label} are incomplete")


def track_learning_run(
    run: LearningRun,
    output_dir: Path,
    *,
    project_root: Path,
    tracking_uri: str | None = None,
) -> dict[str, Any]:
    """Log/reopen one exact local run, rejecting changed or remote retained evidence.

    Retries share a logical run identity. A local SQLite writer lock serializes
    cooperating processes; the MLflow logical-identity tag recovers a run created
    before an interrupted index transaction. Missing artifacts on a RUNNING run
    may be completed, but existing bytes/metadata are never silently overwritten.
    No registered model, stage, alias, deployment or approval is created.
    """
    root = _local_directory(output_dir, project_root)
    store = root / "mlflow.sqlite3"
    uri = f"sqlite:///{store.as_posix()}"
    if tracking_uri is not None and tracking_uri != uri:
        raise TrainingTrackingError("only the generated local SQLite tracking URI is allowed")
    logical_id, payloads, params, metrics = _payloads(run)
    expected_hashes = {name: _sha(content) for name, content in sorted(payloads.items())}
    tags = {
        "logical_identity": logical_id,
        "mlflow.runName": "runtime-learning-" + logical_id[:16],
        "classification": "synthetic",
        "scientific_champion": SCIENTIFIC_CHAMPION,
        "model_promotion": "blocked",
        "authority": "provenance_only_not_deployment",
    }
    root.mkdir(parents=True, exist_ok=True)
    artifact_base = root / "artifacts"
    for destination in (store, root / "provenance-index.sqlite3", artifact_base):
        if not destination.resolve().is_relative_to(root):
            raise TrainingTrackingError("local tracking file escaped the output directory")
    artifact_base.mkdir(exist_ok=True)
    if not artifact_base.resolve().is_relative_to(root):
        raise TrainingTrackingError("local artifact root escaped the tracking directory")
    with closing(sqlite3.connect(root / "provenance-index.sqlite3", timeout=60)) as index:
        index.execute(
            "CREATE TABLE IF NOT EXISTS runs (identity TEXT PRIMARY KEY, receipt TEXT NOT NULL)"
        )
        index.execute("BEGIN IMMEDIATE")
        indexed = index.execute(
            "SELECT receipt FROM runs WHERE identity=?", (logical_id,)
        ).fetchone()
        with _local_sdk(uri) as client:
            experiment = client.get_experiment_by_name(EXPERIMENT)
            if experiment is None:
                experiment_id = client.create_experiment(
                    EXPERIMENT, artifact_location=artifact_base.as_uri()
                )
                experiment = client.get_experiment(experiment_id)
            else:
                experiment_id = experiment.experiment_id
            _file_uri_path(experiment.artifact_location, artifact_base, root)
            found = client.search_runs(
                [experiment_id],
                filter_string=f"tags.logical_identity = '{logical_id}'",
                run_view_type=3,
                max_results=2,
            )
            if len(found) > 1:
                raise TrainingTrackingError("multiple MLflow runs have the same logical identity")
            if indexed is not None and not found:
                raise TrainingTrackingError("indexed MLflow run is missing; refusing a duplicate")
            record = found[0] if found else client.create_run(experiment_id, tags=tags)
            run_id = record.info.run_id
            if re.fullmatch(r"[a-f0-9]{32}", run_id) is None:
                raise TrainingTrackingError("MLflow run ID is not a canonical local identifier")
            if record.info.lifecycle_stage != "active" or record.info.status not in {
                "RUNNING",
                "FINISHED",
            }:
                raise TrainingTrackingError("MLflow run has an unexpected terminal state")
            directory = _file_uri_path(
                record.info.artifact_uri, artifact_base / run_id / "artifacts", root
            )
            finished = record.info.status == "FINISHED"
            _verify_metadata(record, params, metrics, tags, complete=finished)
            observed = _retained_files(client, run_id, directory)
            if any(
                name not in expected_hashes or expected_hashes[name] != value
                for name, value in observed.items()
            ):
                raise TrainingTrackingError("MLflow artifact bytes differ from evaluated evidence")
            if finished and observed != expected_hashes:
                raise TrainingTrackingError("finished MLflow run has missing artifacts")
            if not finished:
                for name, value in params.items():
                    if name not in record.data.params:
                        client.log_param(run_id, name, value, synchronous=True)
                for name, metric_value in metrics.items():
                    if name not in record.data.metrics:
                        client.log_metric(run_id, name, metric_value, synchronous=True)
                with tempfile.TemporaryDirectory(prefix="staging-", dir=root) as temporary:
                    staging = Path(temporary)
                    for name, content in sorted(payloads.items()):
                        if name in observed:
                            continue
                        path = staging / name
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(content)
                        client.log_artifact(run_id, str(path), artifact_path=path.parent.name)
                client.set_terminated(run_id, status="FINISHED")
            retained = client.get_run(run_id)
            _file_uri_path(retained.info.artifact_uri, directory, root)
            _verify_metadata(retained, params, metrics, tags, complete=True)
            if (
                retained.info.status != "FINISHED"
                or _retained_files(client, run_id, directory) != expected_hashes
            ):
                raise TrainingTrackingError("MLflow artifact readback verification failed")
            result = {
                "contract_version": CONTRACT,
                "logical_identity": logical_id,
                "run_id": run_id,
                "experiment_id": str(experiment_id),
                "tracking_uri": uri,
                "artifact_uri": retained.info.artifact_uri,
                "receipt_sha256": run.receipt_sha256,
                "bindings": json.loads(_encode(run.receipt["bindings"])),
                "code_files": json.loads(_encode(run.receipt["code_files"])),
                "dependencies": json.loads(_encode(run.receipt["dependencies"])),
                "tracking_environment": {
                    "mlflow": version("mlflow"),
                    "python": platform.python_version(),
                    "sqlite": sqlite3.sqlite_version,
                },
                "tracking_implementation_sha256": file_sha256(Path(__file__)),
                "artifact_sha256": expected_hashes,
                "params": params,
                "metrics": metrics,
                "tags": tags,
                "readback_verified": True,
                "scientific_champion": SCIENTIFIC_CHAMPION,
                "model_registration_performed": False,
                "model_promotion_performed": False,
                "deployment_applied": False,
            }
            encoded = _encode(result).decode()
            previous = index.execute(
                "SELECT receipt FROM runs WHERE identity=?", (logical_id,)
            ).fetchone()
            if previous is not None and previous[0] != encoded:
                raise TrainingTrackingError("local provenance index differs from MLflow readback")
            index.execute(
                "INSERT OR IGNORE INTO runs(identity, receipt) VALUES (?,?)", (logical_id, encoded)
            )
            index.commit()
            return result
