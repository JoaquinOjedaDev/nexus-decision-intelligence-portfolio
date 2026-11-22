"""Real local dbt/runtime -> historical feedback -> admitted training acceptance.

This is an explicitly retrospective synthetic replay, not a four-hour soak,
prospective model evaluation, real human approval, or production deployment.
Every forecast is the unchanged result of an actual historical runtime build.
Run sequentially: dbt's project target directory is shared.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

from nexus.config import PROJECT_ROOT
from nexus.contracts import ActorRole
from nexus.data.r2_models import R2FulfillmentOutcome
from nexus.modeling.r3_artifacts import write_immutable_json_manifest
from nexus.portfolio.auth import Actor
from nexus.portfolio.feedback import ActualRevision, FeedbackBook
from nexus.portfolio.learning import LearningBook, ModelArtifact
from nexus.portfolio.monitoring import MonitorBook
from nexus.portfolio.runtime_pipeline import (
    build_runtime_seed,
    runtime_code_fingerprint,
    runtime_source_fingerprint,
)
from nexus.portfolio.store import PortfolioStore, canonical, digest
from nexus.portfolio.training_data import FinalizationManifest, fulfillment_rows_sha256
from nexus.portfolio.training_pipeline import load_learning_plan
from nexus.storage import file_sha256

ORIGINS = tuple(datetime(2025, 7, 1, tzinfo=UTC) + timedelta(days=14 * i) for i in range(9))


def retain(root: Path, dataset: str, payload: dict[str, Any]) -> Path:
    return write_immutable_json_manifest(
        root,
        dataset=dataset,
        manifest=payload,
        schema_identity={"contract": payload["contract_version"], "classification": "synthetic"},
    ).path


def source_actuals(source: Path, keys: set[tuple[str, str, date]]) -> list[ActualRevision]:
    """Validate explicit source demand/closure, never replace demand with censored sales."""
    source_hash = runtime_source_fingerprint(source)
    closure = FinalizationManifest.model_validate_json(
        (source / "outcome-finalization.json").read_text(encoding="utf-8")
    )
    if closure.source_snapshot_sha256 != source_hash:
        raise ValueError("source finalization binding mismatch")
    finality = {row.key: row for row in closure.labels}
    groups: dict[tuple[str, str, date], list[R2FulfillmentOutcome]] = defaultdict(list)
    with (source / "fulfillment_outcomes.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            item = R2FulfillmentOutcome.model_validate(
                {
                    **row,
                    "demand_date": date.fromisoformat(row["demand_date"]),
                    "available_at": datetime.fromisoformat(row["available_at"]),
                    "demand_qty": int(row["demand_qty"]),
                    "fulfilled_qty": int(row["fulfilled_qty"]),
                }
            )
            key = item.sku_id, item.location_id, item.demand_date
            if key in keys:
                groups[key].append(item)
    if set(groups) != keys:
        raise ValueError("source is missing a forecast outcome")
    result = []
    for key, rows in sorted(groups.items()):
        proof = finality[key]
        if not proof.correction_closed or proof.source_rows_sha256 != fulfillment_rows_sha256(
            tuple(rows)
        ):
            raise ValueError("actual correction closure/source row binding mismatch")
        if any(row.demand_qty != row.fulfilled_qty for row in rows):
            raise ValueError("this replay requires uncensored observed demand")
        result.append(
            ActualRevision(
                event_id="runtime-replay:" + digest([str(value) for value in key]),
                source_id="verified-learning-fixture-v2",
                source_snapshot_sha256=source_hash,
                sku_id=key[0],
                location_id=key[1],
                target_date=key[2],
                revision=1,
                quantity=Decimal(sum(row.demand_qty for row in rows)),
                available_at=max(row.available_at for row in rows),
                correction_closed=True,
                correction_closed_at=proof.correction_closed_at,
            )
        )
    return result


def run(
    source: Path, output: Path, *, build_only: bool = False, runtime_dir: Path | None = None
) -> dict[str, Any]:
    source, output = source.resolve(), output.resolve()
    if (
        not output.is_relative_to(PROJECT_ROOT / "artifacts")
        or output == PROJECT_ROOT / "artifacts"
    ):
        raise ValueError("acceptance output must be a separate project artifacts subdirectory")
    if output.is_relative_to(source) or source.is_relative_to(output):
        raise ValueError("acceptance cannot overwrite the source package")
    runtime_root = (runtime_dir or output / "runtime").resolve()
    if (
        not runtime_root.is_relative_to(PROJECT_ROOT / "artifacts")
        or runtime_root == PROJECT_ROOT / "artifacts"
        or runtime_root.is_relative_to(source)
        or source.is_relative_to(runtime_root)
    ):
        raise ValueError("runtime evidence must use a separate project artifacts directory")
    output.mkdir(parents=True, exist_ok=True)
    specification = {
        "contract_version": "nexus.feedback-training-acceptance-spec.v1",
        "origins": [origin.isoformat() for origin in ORIGINS],
        "source_snapshot_sha256": runtime_source_fingerprint(source),
        "finalization_sha256": file_sha256(source / "outcome-finalization.json"),
        "runtime_code_sha256": runtime_code_fingerprint(PROJECT_ROOT),
        "acceptance_code_sha256": file_sha256(Path(__file__)),
        "runtime_evidence_root": str(runtime_root),
        "expected_forecasts": 504,
        "historical_replay": True,
        "prospective_evaluation": False,
        "soak_test": False,
        "selection_note": (
            "First nine existing training-window origins selected before these runtime builds. "
            "Historical fixture and preflight coverage were already inspected; "
            "this is engineering verification, not confirmatory scientific evaluation."
        ),
    }
    spec_path = retain(output, "acceptance.specification", specification)

    def recheck_specification() -> None:
        if (
            runtime_source_fingerprint(source) != specification["source_snapshot_sha256"]
            or file_sha256(source / "outcome-finalization.json")
            != specification["finalization_sha256"]
            or runtime_code_fingerprint(PROJECT_ROOT) != specification["runtime_code_sha256"]
            or file_sha256(Path(__file__)) != specification["acceptance_code_sha256"]
        ):
            raise ValueError("acceptance source, finalization, runtime code or script changed")

    seeds = []
    builds = []
    for origin in ORIGINS:
        before = perf_counter()
        evidence = runtime_root / origin.date().isoformat()
        seed = build_runtime_seed(PROJECT_ROOT, evidence, source, planning_cutoff=origin)
        recheck_specification()
        if seed["metadata"]["source_snapshot_sha256"] != specification["source_snapshot_sha256"]:
            raise ValueError("runtime source changed between historical origins")
        if not seed["executable"] or seed["metrics"]["dbt_status"] != "PASS":
            raise ValueError(f"historical runtime is not executable at {origin.isoformat()}")
        seeds.append((origin, seed))
        builds.append(
            {
                "origin": origin.isoformat(),
                "seed_sha256": digest(seed),
                "evidence_dir": str(evidence),
                "solver_status": seed["metrics"]["solver_status"],
                "dbt_summary": seed["metrics"]["dbt_summary"],
                "seconds_this_invocation_including_cache_verification": perf_counter() - before,
            }
        )
        print(json.dumps({"runtime_origin": origin.isoformat(), "status": "VERIFIED"}), flush=True)
    if build_only:
        return {
            "status": "BUILDS_VERIFIED_ONLY",
            "builds": builds,
            "specification_path": str(spec_path),
        }
    state_dir = output / "replay"
    if state_dir.exists():
        raise ValueError("replay already exists; preserve it and use a new acceptance output")
    clock = [ORIGINS[0]]
    feedback = FeedbackBook(state_dir / "feedback.sqlite3", clock=lambda: clock[0])
    locations = tuple(
        sorted({line["location_id"] for _, seed in seeds for line in seed["forecasts"]})
    )
    worker = Actor("acceptance.worker", "Synthetic acceptance worker", ActorRole.SYSTEM, locations)
    keys = {
        (line["sku_id"], line["location_id"], date.fromisoformat(point["date"]))
        for _, seed in seeds
        for line in seed["forecasts"]
        for point in line["points"]
    }
    actuals = source_actuals(source, keys)
    if len(keys) != specification["expected_forecasts"]:
        raise ValueError("actual runtime forecast count differs from fixed replay specification")
    events: list[tuple[datetime, int, Any]] = [(instant, 0, seed) for instant, seed in seeds]
    for actual in actuals:
        assert actual.correction_closed_at is not None
        events.append((max(actual.available_at, actual.correction_closed_at), 1, actual))
    for instant, kind, value in sorted(events, key=lambda item: (item[0], item[1])):
        clock[0] = instant
        if kind == 0:
            store = PortfolioStore(
                state_dir / "origins" / instant.date().isoformat(),
                clock=lambda: clock[0].timestamp(),
            )
            store.ensure_seed(value)
            feedback.register_current_runtime(
                store, actor=worker, expected_seed_sha256=digest(value)
            )
        else:
            feedback.append_actual(value, actor=worker)
    # Historical issuance/arrival replay ends here. Admission/start/completion use actual UTC.
    feedback.clock = lambda: datetime.now(UTC)
    baseline = ModelArtifact.from_bytes(
        "seasonal-runtime-code-policy-binding-not-executable",
        canonical(
            {
                "contract_version": "nexus.acceptance-baseline-binding.v1",
                "runtime_code_sha256": runtime_code_fingerprint(PROJECT_ROOT),
                "policy_file_sha256": file_sha256(
                    PROJECT_ROOT / "configs/portfolio/runtime-baseline.yaml"
                ),
                "executable_artifact": False,
                "scope": (
                    "Identity of the source-runtime baseline for this isolated governance test"
                ),
            }
        ).encode(),
    )
    learning = LearningBook(
        state_dir / "learning.sqlite3", initial_artifact=baseline, locations=locations
    )
    planner = Actor(
        "acceptance.planner",
        "Synthetic test planner, not user approval",
        ActorRole.PLANNER,
        locations,
    )
    receipt = feedback.evaluate(
        MonitorBook(state_dir / "monitor.sqlite3"), learning_book=learning, proposer=planner
    )
    if receipt["proposal_id"] is None or receipt["eligible_forecasts"] != 504:
        raise ValueError("actual retained runtime outcomes did not establish the expected trigger")
    before = learning.snapshot()
    from nexus.portfolio.feedback_training import run_feedback_training

    arguments = {
        "feedback_book": feedback,
        "feedback_receipt_id": receipt["receipt_id"],
        "learning_book": learning,
        "actor": worker,
        "source_dir": source,
        "finalization_path": source / "outcome-finalization.json",
        "plan": load_learning_plan(PROJECT_ROOT / "configs/portfolio/runtime-learning.yaml"),
        "output_dir": output / "training",
    }
    trained = run_feedback_training(PROJECT_ROOT, **arguments)
    if trained["training_performed"] is not True or trained["completion"]["candidate_id"] is None:
        raise ValueError("acceptance requires an actual fit, not a valid abstention")
    after = learning.snapshot()
    retried = run_feedback_training(PROJECT_ROOT, **arguments)
    if trained != retried or learning.snapshot() != after:
        raise ValueError("completed retry changed evidence or journal state")
    if after["sandbox_deployment"] != before["sandbox_deployment"]:
        raise ValueError("acceptance must never deploy a model")
    admission = after["training_admissions"][trained["admission_id"]]
    if (
        not admission["admitted_at"]
        <= admission["started"]["started_at"]
        <= admission["completion"]["completed_at"]
    ):
        raise ValueError("actual admission/start/completion order was not preserved")
    recheck_specification()
    result = {
        "contract_version": "nexus.feedback-training-acceptance.v1",
        "status": "PASS",
        "specification_path": str(spec_path),
        "specification_sha256": digest(specification),
        "builds": builds,
        "forecast_generations": 9,
        "forecast_rows": len(keys),
        "source_actuals": len(actuals),
        "monitor": receipt["monitor"],
        "training_receipt_path": trained["receipt_path"],
        "training_receipt_sha256": trained["receipt_sha256"],
        "training_status": trained["status"],
        "revision_map_counts": trained["revision_map_counts"],
        "completed_retry_no_refit": True,
        "admission_precedes_actual_fit": True,
        "baseline_unchanged": True,
        "synthetic_planner_only": True,
        "human_approval_claimed": False,
        "deployment_applied": False,
        "full_g5_closed": False,
        "scope": (
            "Nine genuine source/dbt/forecast/planning builds, "
            "chronological historical replay, one admitted fit"
        ),
    }
    path = retain(output, "acceptance.result", result)
    return {**result, "receipt_path": str(path), "receipt_sha256": digest(result)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument("--runtime-dir", type=Path, help="Optional existing verified runtime cache")
    args = parser.parse_args()
    print(
        json.dumps(
            run(args.source, args.output, build_only=args.build_only, runtime_dir=args.runtime_dir),
            indent=2,
        )
    )
