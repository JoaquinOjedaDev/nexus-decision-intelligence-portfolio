"""Measured feedback -> independently rebuilt Gold -> one admitted local fit.

Feedback is a reason to investigate, not a replacement training dataset. Every
label still crosses R2, explicit correction closure, and the existing temporal
trainer. This local bridge never invents a planner, approves, promotes or deploys.
The two journals are not a distributed transaction: provenance is checked before
and after fitting; a detected race leaves the claimed admission incomplete.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import date
from importlib.metadata import version
from pathlib import Path
from typing import Any

from nexus.contracts import ActorRole
from nexus.data.r2_models import R2SourceManifest
from nexus.modeling.r3_artifacts import canonical_json_sha256, write_immutable_json_manifest
from nexus.portfolio.auth import Actor
from nexus.portfolio.feedback import ActualRevision, FeedbackBook
from nexus.portfolio.learning import LearningBook, TrainingAdmission
from nexus.portfolio.runtime_pipeline import runtime_source_fingerprint
from nexus.portfolio.store import canonical, digest
from nexus.portfolio.training_data import (
    FinalizationManifest,
    TrainingSnapshot,
    build_training_snapshot,
)
from nexus.portfolio.training_pipeline import (
    LearningPlan,
    build_learning_rows,
    persist_learning_run,
    run_learning_experiment,
)
from nexus.r2 import run_r2_rebuild
from nexus.storage import file_sha256

# Kept identical to the experiment's code_files map; the bridge has its own map
# rather than changing the historical experiment's scientific binding.
EXPERIMENT_FILES = (
    "src/nexus/portfolio/training_pipeline.py",
    "src/nexus/portfolio/training_data.py",
    "src/nexus/portfolio/training_models.py",
    "src/nexus/portfolio/training_fixture.py",
    "src/nexus/modeling/r3_features.py",
    "src/nexus/modeling/r3b_evaluation.py",
    "src/nexus/modeling/r3b_models.py",
    "src/nexus/modeling/r3_metrics.py",
    "src/nexus/modeling/r3_baseline.py",
    "configs/modeling/r3b-smoke.yaml",
    "uv.lock",
)


class FeedbackTrainingError(ValueError):
    """A failed, possibly already claimed attempt; inspect its retained evidence."""

    def __init__(self, message: str, *, failure_receipt_path: Path | None = None) -> None:
        super().__init__(message)
        self.failure_receipt_path = failure_receipt_path


def _authorize(actor: Actor, locations: set[str]) -> None:
    if (
        not isinstance(actor, Actor)
        or actor.role is not ActorRole.SYSTEM
        or not actor.actor_id.strip()
        or not locations.issubset(actor.locations)
    ):
        raise PermissionError("feedback training requires a scoped trusted SYSTEM actor")


def _source_scope(source: Path) -> set[str]:
    """Read-only conservative scope scan, including rows R2 may quarantine later."""
    manifest = R2SourceManifest.model_validate_json(
        (source / "source-manifest.json").read_text(encoding="utf-8")
    )
    locations: set[str] = set()

    def inspect(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if key == "location_id":
                    if not isinstance(item, str) or not item.strip():
                        raise ValueError("source location scope cannot be established")
                    locations.add(item.strip())
                else:
                    inspect(item)
        elif isinstance(value, list):
            for item in value:
                inspect(item)

    for item in manifest.sources:
        path = source / item.file_name
        if path.suffix == ".csv":
            with path.open(encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    inspect(row)
        elif path.suffix == ".jsonl":
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        inspect(json.loads(line))
        else:
            raise ValueError("unsupported source format for authorization preflight")
    if not locations:
        raise ValueError("source package has no identifiable location scope")
    return locations


def _code_maps(root: Path) -> tuple[dict[str, str], dict[str, str]]:
    experiment = {name: file_sha256(root / name) for name in EXPERIMENT_FILES}
    bridge = {
        "src/nexus/portfolio/feedback_training.py",
        "src/nexus/portfolio/feedback.py",
        "src/nexus/portfolio/learning.py",
        "src/nexus/portfolio/monitoring.py",
        "src/nexus/portfolio/auth.py",
        "src/nexus/portfolio/runtime_pipeline.py",
        "src/nexus/modeling/gold_adapter.py",
        "src/nexus/modeling/r3_artifacts.py",
        "src/nexus/storage/artifacts.py",
        "src/nexus/r2.py",
    }
    bridge.update(
        path.relative_to(root).as_posix() for path in (root / "src/nexus/data").glob("r2*.py")
    )
    bridge.update(
        path.relative_to(root).as_posix()
        for path in (root / "dbt").rglob("*")
        if path.is_file()
        and path.suffix in {".sql", ".yaml", ".yml"}
        and not {"target", "logs", "dbt_packages"}.intersection(
            path.relative_to(root / "dbt").parts
        )
    )
    return experiment, {name: file_sha256(root / name) for name in sorted(bridge)}


def _dependencies() -> dict[str, str]:
    return {name: version(name) for name in ("lightgbm", "numpy", "pyarrow")}


def _new_output(output_dir: Path, protected: tuple[Path, ...]) -> Path:
    raw = output_dir.absolute()
    if str(raw).startswith(("\\\\", "//")):
        raise ValueError("feedback training output must be local")
    if any(item.is_symlink() or item.is_junction() for item in (raw, *raw.parents)):
        raise ValueError("feedback training output cannot traverse a linked directory")
    output = raw.resolve()
    if any(output == path or path.is_relative_to(output) for path in protected):
        raise ValueError("feedback training output cannot contain protected project or state")
    if output.is_relative_to(protected[1]):
        raise ValueError("feedback training output cannot modify the source package")
    return output


def _gold_inventory(rebuild: Any, output: Path, snapshot: TrainingSnapshot) -> list[dict[str, Any]]:
    result = [
        {
            "dataset": item.dataset,
            "path": str(item.path.resolve()),
            "file_sha256": file_sha256(item.path),
            "content_sha256": item.content_sha256,
        }
        for item in rebuild.gold_artifacts
    ]
    _verify_gold_inventory(result, output, snapshot.to_dict())
    return sorted(result, key=lambda item: item["dataset"])


def _verify_gold_inventory(items: Any, output: Path, snapshot: dict[str, Any]) -> None:
    if not isinstance(items, list) or not items:
        raise ValueError("retained Gold inventory cannot be empty")
    datasets = {item["dataset"]: item for item in items}
    if len(datasets) != len(items):
        raise ValueError("retained Gold inventory has duplicate datasets")
    for dataset, field in (
        ("gold.r2_demand_history", "gold_demand_sha256"),
        ("gold.r2_fulfillment_outcomes", "gold_fulfillment_sha256"),
    ):
        if dataset not in datasets or datasets[dataset]["content_sha256"] != snapshot[field]:
            raise ValueError("retained Gold inventory differs from admitted training snapshot")
    for item in items:
        path = Path(item["path"])
        if (
            set(item) != {"dataset", "path", "file_sha256", "content_sha256"}
            or not path.resolve().is_relative_to(output)
            or any(part.is_symlink() or part.is_junction() for part in (path, *path.parents))
            or file_sha256(path) != item["file_sha256"]
        ):
            raise ValueError("retained Gold artifact checksum/path mismatch")


def _result_payload(
    *,
    feedback_id: str,
    evidence: dict[str, Any],
    evidence_path: Path,
    admission_id: str,
    start: dict[str, Any],
    completion: dict[str, Any],
    revisions: dict[str, Any],
    revision_path: Path,
    learned_path: Path,
) -> dict[str, Any]:
    return {
        "contract_version": "nexus.feedback-training-result.v1",
        "classification": "synthetic",
        "status": completion["status"],
        "training_performed": completion["training_performed"],
        "feedback_receipt_id": feedback_id,
        "feedback_export_sha256": evidence["export_sha256"],
        "feedback_evidence_path": str(evidence_path),
        "admission_id": admission_id,
        "start": start,
        "completion": completion,
        "revision_map_sha256": digest(revisions),
        "revision_map_path": str(revision_path),
        "revision_map_counts": revisions["counts"],
        "learning_receipt_sha256": completion["run_receipt_sha256"],
        "learning_receipt_path": str(learned_path),
        "gold_files": revisions["gold_files"],
        "scientific_promotion_applied": False,
        "deployment_applied": False,
        "automatic_human_approval": False,
        "pre_and_post_provenance_checked": True,
        "distributed_atomicity_claimed": False,
        "scope": (
            "One trusted local fit after a measured feedback proposal; "
            "no production or prospective evaluation claim"
        ),
    }


def _read_retained(output: Path, path: Path) -> dict[str, Any]:
    if not path.resolve().is_relative_to(output) or any(
        item.is_symlink() or item.is_junction() for item in (path, *path.parents)
    ):
        raise ValueError("retained training artifact escaped its isolated output")
    value: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if canonical_json_sha256(value) != path.stem:
        raise ValueError("retained training artifact checksum mismatch")
    return value


def _reuse_completed(
    output: Path,
    *,
    state: dict[str, Any],
    proposal_id: str,
    evidence: dict[str, Any],
    source_hash: str,
    finalization_hash: str,
    plan: LearningPlan,
    code: dict[str, str],
    bridge_code: dict[str, str],
    dependencies: dict[str, str],
) -> dict[str, Any]:
    paths = list((output / "feedback/training/result").glob("*.json"))
    if len(paths) != 1:
        raise ValueError("existing training output requires explicit recovery, not implicit refit")
    result = _read_retained(output, paths[0])
    admission = state["training_admissions"].get(result["admission_id"])
    if admission is None or admission["proposal_id"] != proposal_id:
        raise ValueError("retained bridge result has no matching admission")
    binding, completion = admission["binding"], admission["completion"]
    if (
        completion is None
        or result["completion"] != completion
        or result["start"] != admission["started"]
        or result["learning_receipt_sha256"] != completion["run_receipt_sha256"]
        or binding["feedback_evidence_sha256"] != evidence["export_sha256"]
        or binding["feedback_journal_head_sha256"] != evidence["ledger_head_sha256"]
        or binding["training_snapshot"]["source_snapshot_sha256"] != source_hash
        or binding["training_snapshot"]["finalization_sha256"] != finalization_hash
        or binding["plan"] != plan.model_dump(mode="json")
        or binding["code_files"] != code
        or binding["bridge_code_files"] != bridge_code
        or binding["dependencies"] != dependencies
        or binding["baseline_artifact_sha256"] != state["sandbox_deployment"]["artifact_sha256"]
    ):
        raise ValueError("completed training provenance no longer matches current inputs")
    if _read_retained(output, Path(result["feedback_evidence_path"])) != evidence:
        raise ValueError("retained feedback export differs from the verified journal")
    revisions = _read_retained(output, Path(result["revision_map_path"]))
    if digest(revisions) != binding["revision_map_sha256"]:
        raise ValueError("retained revision map differs from the admission")
    _verify_gold_inventory(revisions["gold_files"], output, binding["training_snapshot"])
    learned = _read_retained(output, Path(result["learning_receipt_path"]))
    candidate, baseline = learned.pop("candidate_artifact"), learned.pop("baseline_artifact")
    if learned != completion["run_receipt"]:
        raise ValueError("retained experiment receipt differs from the admitted completion")
    if learned["training_performed"]:
        if (
            digest(candidate) != learned["candidate_sha256"]
            or digest(baseline) != learned["baseline_artifact_sha256"]
        ):
            raise ValueError("retained trained artifact differs from evaluated bytes")
    elif candidate is not None or baseline is not None:
        raise ValueError("abstention cannot contain a trained artifact")
    expected = _result_payload(
        feedback_id=evidence["receipt"]["receipt_id"],
        evidence=evidence,
        evidence_path=Path(result["feedback_evidence_path"]),
        admission_id=admission["admission_id"],
        start=admission["started"],
        completion=completion,
        revisions=revisions,
        revision_path=Path(result["revision_map_path"]),
        learned_path=Path(result["learning_receipt_path"]),
    )
    if result != expected:
        raise ValueError("retained bridge result differs from authoritative completion evidence")
    return {**result, "receipt_sha256": digest(result), "receipt_path": str(paths[0])}


def _revision_map(
    evidence: dict[str, Any], snapshot: TrainingSnapshot, plan: LearningPlan
) -> dict[str, Any]:
    labels = {row.key: row for row in snapshot.labels}
    eligible = {
        (key[0], key[1], date.fromisoformat(key[2])) for key in evidence["forecast_outcome_keys"]
    }
    split_keys = {
        name: {
            row.key for row in build_learning_rows(snapshot, start=start, end=end, labels_at=cutoff)
        }
        for name, start, end, cutoff in (
            ("training", plan.training_start, plan.training_end, plan.fit_at),
            ("calibration", plan.calibration_start, plan.calibration_end, plan.calibrated_at),
            ("final", plan.evaluation_start, plan.evaluation_end, plan.evaluation_cutoff),
        )
    }
    mapped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, date]] = set()
    for entry in evidence["actuals"]:
        actual = ActualRevision.model_validate_json(canonical(entry["revision"]))
        if actual.key in seen:
            raise ValueError("feedback export contains duplicate selected outcome keys")
        seen.add(actual.key)
        label = labels.get(actual.key)
        item: dict[str, Any] = {
            "key": [actual.sku_id, actual.location_id, actual.target_date.isoformat()],
            "actual_event_id": actual.event_id,
            "actual_event_sha256": entry["event_sha256"],
            "actual_recorded_at": entry["recorded_at"],
            "actual_revision": actual.revision,
            "actual_source_snapshot_sha256": actual.source_snapshot_sha256,
            "actual_quantity": str(actual.quantity),
            "actual_available_at": actual.available_at.isoformat(),
            "actual_correction_closed_at": (
                actual.correction_closed_at.isoformat() if actual.correction_closed_at else None
            ),
            "source_rows_sha256": label.source_rows_sha256 if label else None,
            "split_roles": [],
        }
        # A changed value for an overlapping key is an inconsistency, not an
        # invitation to hide the row under a different source-version exclusion.
        if (
            label is not None
            and actual.key in eligible
            and (
                actual.quantity != actual.quantity.to_integral_value()
                or actual.quantity != label.quantity
            )
        ):
            raise ValueError("same-key feedback quantity differs from verified training label")
        if actual.key not in eligible:
            item["status"] = "excluded_not_monitor_eligible"
        elif actual.source_snapshot_sha256 != snapshot.source_snapshot_sha256:
            item["status"] = "excluded_actual_source_snapshot_mismatch"
        elif label is None:
            item["status"] = "excluded_not_in_finalized_uncensored_snapshot"
        elif not actual.correction_closed or actual.correction_closed_at is None:
            raise ValueError("monitor-eligible feedback actual has no correction closure")
        elif (
            label.available_at < actual.available_at
            or label.correction_closed_at < actual.correction_closed_at
        ):
            raise ValueError("training label availability or finality precedes feedback revision")
        else:
            item.update(
                status="matched_verified_source_label",
                training_quantity=label.quantity,
                training_available_at=label.available_at.isoformat(),
                training_correction_closed_at=label.correction_closed_at.isoformat(),
                split_roles=[name for name, keys in split_keys.items() if actual.key in keys],
            )
        mapped.append(item)
    if eligible - seen:
        raise ValueError("feedback export omitted an eligible actual")
    counts = Counter(item["status"] for item in mapped)
    if counts["matched_verified_source_label"] < 1:
        raise ValueError("feedback breach has no matching verified source labels")
    return {
        "contract_version": "nexus.feedback-training-revision-map.v1",
        "training_snapshot_sha256": snapshot.snapshot_sha256,
        "feedback_export_sha256": evidence["export_sha256"],
        "entries": sorted(mapped, key=lambda item: item["key"]),
        "counts": dict(sorted(counts.items())),
        "snapshot_exclusions": snapshot.excluded_counts,
        "training_uses_feedback_rows_directly": False,
        "boundary": (
            "A 500-outcome monitoring breach is a trigger, not 500 training labels. "
            "Matched source labels may be history, calibration, final evaluation or excluded "
            "from whole-window splits. Only the frozen temporal trainer selects fit rows; "
            "final evaluation rows never enter fitting or calibration."
        ),
    }


def _write_receipt(output: Path, payload: dict[str, Any], dataset: str) -> Path:
    return write_immutable_json_manifest(
        output,
        dataset=dataset,
        manifest=payload,
        schema_identity={"contract": payload["contract_version"], "classification": "synthetic"},
    ).path


def run_feedback_training(
    project_root: Path,
    *,
    feedback_book: FeedbackBook,
    feedback_receipt_id: str,
    learning_book: LearningBook,
    actor: Actor,
    source_dir: Path,
    finalization_path: Path,
    plan: LearningPlan,
    output_dir: Path,
) -> dict[str, Any]:
    """Rebuild and fit once for an existing explicit proposal; never auto-promote.

    A failed started admission is deliberately non-retryable. A new operator
    decision/evidence is needed, not a hidden second fit under the same identity.
    This is a retrospective synthetic experiment, not prospective preregistration.
    """
    _authorize(actor, set())
    root, source, finalization = (
        project_root.resolve(),
        source_dir.resolve(),
        finalization_path.resolve(),
    )
    evidence = feedback_book.export_training_evidence(feedback_receipt_id, actor=actor)
    feedback = evidence["receipt"]
    state = learning_book.snapshot()
    locations = set(state["locations"]) | set(feedback["locations"])
    _authorize(actor, locations)
    proposal_id = feedback.get("proposal_id")
    proposal = state["proposals"].get(proposal_id)
    if proposal is None:
        raise ValueError("feedback training requires an existing explicit training proposal")
    if (
        proposal["source_snapshot_sha256"] != feedback["source_snapshot_sha256"]
        or proposal["monitor_evaluation_sha256"] != feedback["evaluation_sha256"]
    ):
        raise ValueError("feedback proposal source/monitor binding mismatch")
    source_hash = runtime_source_fingerprint(source)
    source_locations = _source_scope(source)
    finalization_hash = file_sha256(finalization)
    closure = FinalizationManifest.model_validate_json(finalization.read_text(encoding="utf-8"))
    _authorize(actor, source_locations | {row.location_id for row in closure.labels})
    if closure.source_snapshot_sha256 != source_hash:
        raise ValueError("finalization source snapshot mismatch")
    if not set(feedback["locations"]).issubset(source_locations):
        raise ValueError("training source does not cover the feedback location scope")
    if not (source_locations | set(feedback["locations"])).issubset(state["locations"]):
        raise ValueError("training cannot implicitly expand the learning model location scope")
    code, bridge_code = _code_maps(root)
    dependencies = _dependencies()
    output = _new_output(
        output_dir, (root, source, learning_book.path.resolve(), feedback_book.path.resolve())
    )
    head = evidence["ledger_head_sha256"]
    baseline = state["sandbox_deployment"]["artifact_sha256"]
    if output.exists():
        return _reuse_completed(
            output,
            state=state,
            proposal_id=proposal_id,
            evidence=evidence,
            source_hash=source_hash,
            finalization_hash=finalization_hash,
            plan=plan,
            code=code,
            bridge_code=bridge_code,
            dependencies=dependencies,
        )
    if any(item["proposal_id"] == proposal_id for item in state["training_admissions"].values()):
        raise ValueError("training already admitted; use its retained output or explicit recovery")
    admission_id: str | None = None
    started = False
    retained_run_path: Path | None = None
    phase = "source_rebuild"
    output.mkdir(parents=True, exist_ok=False)
    try:
        rebuild = run_r2_rebuild(
            project_root=root,
            source_dir=source,
            artifact_dir=output / "gold",
            planning_cutoff=plan.evaluation_cutoff,
        )

        def validated_snapshot() -> TrainingSnapshot:
            return build_training_snapshot(
                rebuild,
                source_dir=source,
                finalization_path=finalization,
                expected_finalization_sha256=finalization_hash,
                as_of=plan.evaluation_cutoff,
                minimum_history_days=180,
                minimum_mature_labels=500,
            )

        snapshot = validated_snapshot()
        if not set(feedback["locations"]).issubset({row.location_id for row in snapshot.labels}):
            raise ValueError("finalized training labels do not cover feedback locations")
        revision_map = _revision_map(evidence, snapshot, plan)
        revision_map["gold_files"] = _gold_inventory(rebuild, output, snapshot)
        revision_map_path = _write_receipt(output, revision_map, "feedback.training.revisions")
        evidence_path = _write_receipt(output, evidence, "feedback.training.evidence")

        def recheck() -> None:
            current = feedback_book.export_training_evidence(
                feedback_receipt_id, actor=actor, expected_head_sha256=head
            )
            if current != evidence:
                raise ValueError("feedback evidence changed during the training attempt")
            if (
                runtime_source_fingerprint(source) != source_hash
                or file_sha256(finalization) != finalization_hash
                or _code_maps(root) != (code, bridge_code)
                or _dependencies() != dependencies
                or validated_snapshot().snapshot_sha256 != snapshot.snapshot_sha256
            ):
                raise ValueError(
                    "source, Gold, closure, code or dependencies changed during training"
                )
            if learning_book.snapshot()["sandbox_deployment"]["artifact_sha256"] != baseline:
                raise ValueError("deployed baseline changed during training")
            _verify_gold_inventory(revision_map["gold_files"], output, snapshot.to_dict())

        recheck()
        phase = "admission"
        binding = TrainingAdmission.from_dict(
            {
                "contract_version": "nexus.training-admission.v1",
                "feedback_receipt_id": feedback_receipt_id,
                "feedback_evaluation_sha256": feedback["evaluation_sha256"],
                "feedback_evidence_sha256": evidence["export_sha256"],
                "feedback_journal_head_sha256": head,
                "feedback_source_snapshot_sha256": feedback["source_snapshot_sha256"],
                "monitor_evaluation_sha256": feedback["evaluation_sha256"],
                "training_snapshot": snapshot.to_dict(),
                "plan": plan.model_dump(mode="json"),
                "code_files": code,
                "bridge_code_files": bridge_code,
                "dependencies": dependencies,
                "revision_map_sha256": digest(revision_map),
                "baseline_artifact_sha256": baseline,
                "locations": state["locations"],
            }
        )
        admission_id = learning_book.admit_training(actor, proposal_id=proposal_id, binding=binding)
        recheck()
        start = learning_book.start_training(actor, admission_id=admission_id)
        started = True
        phase = "fitting"
        recheck()
        run = run_learning_experiment(root, snapshot, plan)
        retained_run_path = persist_learning_run(output, run)
        phase = "post_fit_provenance"
        recheck()
        completion = learning_book.complete_training(actor, admission_id=admission_id, run=run)
        result = _result_payload(
            feedback_id=feedback_receipt_id,
            evidence=evidence,
            evidence_path=evidence_path,
            admission_id=admission_id,
            start=start,
            completion=completion,
            revisions=revision_map,
            revision_path=revision_map_path,
            learned_path=retained_run_path,
        )
        path = _write_receipt(output, result, "feedback.training.result")
        return {**result, "receipt_sha256": digest(result), "receipt_path": str(path)}
    except Exception as error:
        failure = {
            "contract_version": "nexus.feedback-training-failure.v1",
            "status": "FAILED_CLOSED",
            "phase": phase,
            "feedback_receipt_id": feedback_receipt_id,
            "feedback_export_sha256": evidence["export_sha256"],
            "admission_id": admission_id,
            "training_claimed": started,
            "implicit_refit_allowed": False,
            "retained_learning_receipt_path": str(retained_run_path) if retained_run_path else None,
            "error_type": type(error).__name__,
            "error": str(error)[:1000],
            "scientific_promotion_applied": False,
            "deployment_applied": False,
        }
        path = _write_receipt(output, failure, "feedback.training.failure")
        raise FeedbackTrainingError(str(error), failure_receipt_path=path) from error
