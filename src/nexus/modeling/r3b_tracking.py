"""Local, evidence-bearing MLflow tracking for the bounded R3B gate.

R3B uses MLflow as an experiment ledger, not as an online service.  A fresh
SQLite store and local artifact directory make each verification run isolated,
portable, and safe for CI.  Mutable MLflow identifiers remain in the audit
receipt but are deliberately excluded from its normalized cross-platform view.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from nexus.storage import file_sha256

type R3BTrackingScalar = bool | float | int | str

_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


class R3BTrackingError(RuntimeError):
    """The local experiment ledger could not be validated or materialized."""


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalized_scalars(
    values: Mapping[str, R3BTrackingScalar],
    *,
    kind: str,
) -> dict[str, R3BTrackingScalar]:
    normalized: dict[str, R3BTrackingScalar] = {}
    for key in sorted(values):
        if not isinstance(key, str) or not key.strip():
            raise R3BTrackingError(f"{kind} names must be nonblank strings")
        value = values[key]
        if type(value) not in {bool, float, int, str}:
            raise R3BTrackingError(f"{kind} values must be JSON scalars")
        if isinstance(value, float) and not math.isfinite(value):
            raise R3BTrackingError(f"{kind} values must be finite")
        normalized[key] = value
    return normalized


def _sqlite_uri(path: Path) -> str:
    resolved = path.resolve()
    return f"sqlite:///{resolved.as_posix()}"


@dataclass(frozen=True, slots=True)
class R3BTrackingReceipt:
    """Audit identity for one manually logged challenger run."""

    experiment_name: str
    run_name: str
    run_id: str
    experiment_id: str
    tracking_store: Path
    tracking_store_sha256: str
    artifact_root: Path
    registered_model_name: str
    registered_model_version: int
    registered_model_source: str
    registered_model_status: str
    model_artifact_sha256: str
    evidence_file_sha256: Mapping[str, str]
    readback_verified: Literal[True]
    params: Mapping[str, R3BTrackingScalar]
    metrics: Mapping[str, float]
    tags: Mapping[str, str]
    semantic_hashes: Mapping[str, str]

    def normalized_payload(self) -> dict[str, object]:
        """Return stable scientific content without paths or mutable MLflow IDs."""

        return {
            "contract": "nexus.r3b.mlflow-receipt.v1",
            "experiment_name": self.experiment_name,
            "metrics": dict(sorted(self.metrics.items())),
            "params": dict(sorted(self.params.items())),
            "registered_model_name": self.registered_model_name,
            "run_name": self.run_name,
            "semantic_hashes": dict(sorted(self.semantic_hashes.items())),
            "tags": dict(sorted(self.tags.items())),
        }

    @property
    def normalized_sha256(self) -> str:
        return _canonical_sha256(self.normalized_payload())

    def audit_payload(self) -> dict[str, object]:
        """Return the normalized content plus local lineage needed for inspection."""

        return {
            **self.normalized_payload(),
            "artifact_root": self.artifact_root.as_posix(),
            "experiment_id": self.experiment_id,
            "normalized_sha256": self.normalized_sha256,
            "registered_model_version": self.registered_model_version,
            "registered_model_source": self.registered_model_source,
            "registered_model_status": self.registered_model_status,
            "model_artifact_sha256": self.model_artifact_sha256,
            "evidence_file_sha256": dict(sorted(self.evidence_file_sha256.items())),
            "readback_verified": self.readback_verified,
            "run_id": self.run_id,
            "tracking_store": self.tracking_store.as_posix(),
            "tracking_store_sha256": self.tracking_store_sha256,
        }


def log_r3b_challenger(
    output_dir: Path,
    *,
    experiment_name: str,
    run_name: str,
    registered_model_name: str,
    params: Mapping[str, R3BTrackingScalar],
    metrics: Mapping[str, float],
    tags: Mapping[str, str],
    semantic_hashes: Mapping[str, str],
    model_json: str,
    evidence_paths: Sequence[Path] = (),
) -> R3BTrackingReceipt:
    """Log one local MLflow run and register its serialized challenger artifact."""

    names = (experiment_name, run_name, registered_model_name)
    if any(not isinstance(value, str) or not value.strip() for value in names):
        raise R3BTrackingError("MLflow experiment, run, and model names must be nonblank")
    normalized_params = _normalized_scalars(params, kind="parameter")
    raw_metrics = _normalized_scalars(metrics, kind="metric")
    normalized_metrics = {key: float(value) for key, value in raw_metrics.items()}
    normalized_tags = _normalized_scalars(tags, kind="tag")
    if any(not isinstance(value, str) for value in normalized_tags.values()):
        raise R3BTrackingError("MLflow tag values must be strings")
    normalized_hashes = dict(sorted(semantic_hashes.items()))
    if not normalized_hashes or any(
        not isinstance(key, str) or not key.strip() or _SHA256_RE.fullmatch(value) is None
        for key, value in normalized_hashes.items()
    ):
        raise R3BTrackingError("semantic_hashes must contain named lowercase SHA-256 values")
    if not isinstance(model_json, str) or not model_json.strip():
        raise R3BTrackingError("model_json must contain the serialized challenger")
    resolved_evidence = tuple(path.resolve() for path in evidence_paths)
    if any(not path.is_file() for path in resolved_evidence):
        raise R3BTrackingError("every evidence path must be an existing file")
    evidence_names = tuple(path.name for path in resolved_evidence)
    if len(evidence_names) != len(set(evidence_names)):
        raise R3BTrackingError("evidence artifact basenames must be unique")
    expected_evidence_hashes = {path.name: file_sha256(path) for path in resolved_evidence}
    expected_model_sha256 = hashlib.sha256(model_json.encode("utf-8")).hexdigest()

    root = output_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    store = root / "mlflow.sqlite"
    artifacts = root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    os.environ["MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING"] = "false"
    try:
        import mlflow
        from mlflow import MlflowClient
        from mlflow.exceptions import MlflowException
    except ImportError as error:  # pragma: no cover - base-only installations
        raise R3BTrackingError("R3B tracking requires the optional 'tracking' group") from error

    tracking_uri = _sqlite_uri(store)
    try:
        mlflow.set_tracking_uri(tracking_uri)
        client = MlflowClient(tracking_uri=tracking_uri)
        experiment = client.get_experiment_by_name(experiment_name)
        experiment_id = (
            client.create_experiment(experiment_name, artifact_location=artifacts.as_uri())
            if experiment is None
            else experiment.experiment_id
        )
        with mlflow.start_run(
            experiment_id=experiment_id,
            run_name=run_name,
            tags=dict(normalized_tags),
        ) as active_run:
            mlflow.log_params(normalized_params)
            mlflow.log_metrics(normalized_metrics)
            for key, value in normalized_hashes.items():
                mlflow.set_tag(f"semantic.{key}", value)
            mlflow.log_text(model_json, "model/ensemble.json")
            mlflow.log_dict(
                {
                    "contract": "nexus.r3b.serialized-challenger.v1",
                    "semantic_hashes": normalized_hashes,
                },
                "model/model-summary.json",
            )
            for evidence_path in resolved_evidence:
                mlflow.log_artifact(str(evidence_path), artifact_path="evidence")
            run_id = active_run.info.run_id
        try:
            client.create_registered_model(registered_model_name)
        except MlflowException as error:
            if "already exists" not in str(error).lower():
                raise
        version = client.create_model_version(
            name=registered_model_name,
            source=f"runs:/{run_id}/model",
            run_id=run_id,
            tags={"classification": "synthetic", "promotion": "blocked"},
        )
        persisted_run = client.get_run(run_id)
        expected_params = {key: str(value) for key, value in normalized_params.items()}
        if any(
            persisted_run.data.params.get(key) != value for key, value in expected_params.items()
        ):
            raise R3BTrackingError("MLflow parameter read-back differs from logged values")
        if any(
            persisted_run.data.metrics.get(key) != value
            for key, value in normalized_metrics.items()
        ):
            raise R3BTrackingError("MLflow metric read-back differs from logged values")
        expected_tags = {
            **cast(dict[str, str], normalized_tags),
            **{f"semantic.{key}": value for key, value in normalized_hashes.items()},
        }
        if any(persisted_run.data.tags.get(key) != value for key, value in expected_tags.items()):
            raise R3BTrackingError("MLflow tag read-back differs from logged values")
        persisted_version = client.get_model_version(
            registered_model_name,
            str(version.version),
        )
        expected_model_source = f"runs:/{run_id}/model"
        if (
            persisted_version.run_id != run_id
            or persisted_version.source != expected_model_source
            or str(persisted_version.status) != "READY"
        ):
            raise R3BTrackingError("registered model version read-back is not ready or run-bound")
        persisted_model = Path(client.download_artifacts(run_id, "model/ensemble.json"))
        if file_sha256(persisted_model) != expected_model_sha256:
            raise R3BTrackingError("serialized model read-back hash differs from the input")
        persisted_summary = Path(client.download_artifacts(run_id, "model/model-summary.json"))
        summary_payload = json.loads(persisted_summary.read_text(encoding="utf-8"))
        if summary_payload != {
            "contract": "nexus.r3b.serialized-challenger.v1",
            "semantic_hashes": normalized_hashes,
        }:
            raise R3BTrackingError("serialized model summary read-back is invalid")
        observed_evidence_hashes = {
            name: file_sha256(Path(client.download_artifacts(run_id, f"evidence/{name}")))
            for name in evidence_names
        }
        if observed_evidence_hashes != expected_evidence_hashes:
            raise R3BTrackingError("MLflow evidence read-back hashes differ from the inputs")
    except Exception as error:
        raise R3BTrackingError(f"local MLflow logging failed: {error}") from error
    if not store.is_file():
        raise R3BTrackingError("MLflow did not materialize its SQLite tracking store")
    return R3BTrackingReceipt(
        experiment_name=experiment_name,
        run_name=run_name,
        run_id=run_id,
        experiment_id=str(experiment_id),
        tracking_store=store,
        tracking_store_sha256=file_sha256(store),
        artifact_root=artifacts,
        registered_model_name=registered_model_name,
        registered_model_version=int(version.version),
        registered_model_source=expected_model_source,
        registered_model_status="READY",
        model_artifact_sha256=expected_model_sha256,
        evidence_file_sha256=expected_evidence_hashes,
        readback_verified=True,
        params=normalized_params,
        metrics=normalized_metrics,
        tags=cast(dict[str, str], normalized_tags),
        semantic_hashes=normalized_hashes,
    )


__all__ = [
    "R3BTrackingError",
    "R3BTrackingReceipt",
    "R3BTrackingScalar",
    "log_r3b_challenger",
]
