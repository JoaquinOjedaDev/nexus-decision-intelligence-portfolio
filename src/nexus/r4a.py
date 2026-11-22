"""Executable R4A scenario simulation and constrained-decision gate."""

from __future__ import annotations

import json
import os
import platform
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Literal, cast
from uuid import NAMESPACE_URL, uuid5

from nexus.contracts import (
    ArtifactRef,
    DataSnapshotRef,
    RecommendationLine,
    RecommendationManifest,
)
from nexus.decisioning.r4a_contracts import (
    DEFAULT_R4A_CONFIG_PATH,
    DemandScenario,
    EvaluationBaseline,
    EvaluationReport,
    GateReceipt,
    PlanningSnapshot,
    PolicyDecision,
    R4AConfig,
    VerificationCheck,
    VerificationReceipt,
    VerificationRuntime,
    load_champion_forecast_adapter,
    load_r4a_config,
)
from nexus.decisioning.r4a_evaluation import evaluate_policies
from nexus.decisioning.r4a_optimizer import (
    InjectedFailure,
    build_heuristic_policy,
    solve_constrained_policy,
    validate_decision,
)
from nexus.decisioning.r4a_scenarios import build_planning_snapshot, build_scenarios
from nexus.modeling.r3_artifacts import (
    ScientificArtifactRef,
    canonical_json_sha256,
    write_immutable_json_manifest,
)
from nexus.storage import file_sha256
from nexus.workflow import InMemoryLifecycleLedger, MockErpExecutor, RecommendationWorkflow
from nexus.workflow.ledger import DecisionState


@dataclass(frozen=True, slots=True)
class R4ARunResult:
    config: R4AConfig
    planning: PlanningSnapshot
    optimizer: PolicyDecision
    heuristic: EvaluationBaseline
    evaluation: EvaluationReport | None
    manifest: RecommendationManifest | None
    lifecycle_state: DecisionState | None
    artifacts: tuple[ScientificArtifactRef, ...]
    semantic_sha256: str
    gate: GateReceipt

    @property
    def artifact(self) -> dict[str, ScientificArtifactRef]:
        return {item.dataset: item for item in self.artifacts}


@dataclass(frozen=True, slots=True)
class R4AVerificationResult:
    receipt: VerificationReceipt
    artifact: ScientificArtifactRef

    @property
    def passed(self) -> bool:
        return self.receipt.status == "PASS"


def _verification_runtime() -> VerificationRuntime:
    """Record observed OS/Python provenance and validate GitHub's runner attestation."""

    observed_system = platform.system().strip().lower()
    os_family = cast(
        Literal["linux", "windows", "macos", "other"],
        {
            "darwin": "macos",
            "linux": "linux",
            "windows": "windows",
        }.get(observed_system, "other"),
    )
    runner_os = os.environ.get("RUNNER_OS", "").strip().lower()
    runner_family = {
        "linux": "linux",
        "macos": "macos",
        "windows": "windows",
    }.get(runner_os)
    if runner_os and runner_family is None:
        raise ValueError("RUNNER_OS is not a recognized GitHub runner platform")
    if runner_family is not None and runner_family != os_family:
        raise ValueError("RUNNER_OS does not match the observed R4A runtime platform")
    return VerificationRuntime(
        os_family=os_family,
        architecture=platform.machine().strip().lower() or "unknown",
        python_minor=f"{sys.version_info.major}.{sys.version_info.minor}",
        github_runner_os_attested=runner_family is not None,
    )


def _write_contract(
    output_dir: Path,
    *,
    dataset: str,
    value: object,
    schema: str,
) -> ScientificArtifactRef:
    return write_immutable_json_manifest(
        output_dir,
        dataset=dataset,
        manifest=value,
        schema_identity={"contract": schema, "format": "canonical-json"},
    )


def _artifact_integrity_ok(artifact: ScientificArtifactRef) -> bool:
    if not artifact.path.is_file() or file_sha256(artifact.path) != artifact.file_sha256:
        return False
    try:
        value = json.loads(artifact.path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return canonical_json_sha256(value) == artifact.content_sha256


def _revalidate_persisted_policies(
    *,
    policy_ref: ScientificArtifactRef,
    scenarios_ref: ScientificArtifactRef,
    decision_ref: ScientificArtifactRef,
    baseline_ref: ScientificArtifactRef,
) -> None:
    """Reparse persisted contracts and independently reconstruct both feasibility reports."""

    persisted_config = R4AConfig.model_validate_json(policy_ref.path.read_text(encoding="utf-8"))
    scenario_payload = json.loads(scenarios_ref.path.read_text(encoding="utf-8"))
    if not isinstance(scenario_payload, list):
        raise ValueError("persisted R4A scenario set must be a list")
    persisted_scenarios = tuple(DemandScenario.model_validate(item) for item in scenario_payload)
    persisted_decision = PolicyDecision.model_validate_json(
        decision_ref.path.read_text(encoding="utf-8")
    )
    persisted_baseline = EvaluationBaseline.model_validate_json(
        baseline_ref.path.read_text(encoding="utf-8")
    )
    decision_feasibility = validate_decision(
        persisted_config,
        persisted_scenarios,
        persisted_decision.lines,
        persisted_decision.recourse,
    )
    baseline_feasibility = validate_decision(
        persisted_config,
        persisted_scenarios,
        persisted_baseline.lines,
        persisted_baseline.recourse,
    )
    if decision_feasibility != persisted_decision.feasibility:
        raise ValueError("persisted decision feasibility does not match independent reconstruction")
    if baseline_feasibility != persisted_baseline.feasibility:
        raise ValueError("persisted baseline feasibility does not match independent reconstruction")


def _build_manifest(
    config: R4AConfig,
    decision: PolicyDecision,
    *,
    planning_ref: ScientificArtifactRef,
    forecast_ref: ScientificArtifactRef,
    scenarios_ref: ScientificArtifactRef,
    policy_ref: ScientificArtifactRef,
    decision_ref: ScientificArtifactRef,
) -> RecommendationManifest:
    identity = ":".join(
        (
            "nexus-r4a",
            config.decision_time.isoformat(),
            planning_ref.content_sha256,
            scenarios_ref.content_sha256,
            policy_ref.content_sha256,
            decision_ref.content_sha256,
        )
    )
    return RecommendationManifest(
        manifest_id=uuid5(NAMESPACE_URL, identity),
        decision_key=f"portfolio:{config.decision_time.date().isoformat()}:r4a",
        decision_time=config.decision_time,
        valid_until=config.decision_time + timedelta(hours=4),
        created_at=config.decision_time,
        created_by="planner.r4a",
        input_snapshots=(
            DataSnapshotRef(
                dataset="r4a.planning_input",
                snapshot_id=f"sha256-{planning_ref.content_sha256[:20]}",
                schema_version="r4a-planning-v1",
                as_of=config.decision_time,
                content_sha256=planning_ref.content_sha256,
                row_count=len(config.portfolio),
                partition_keys=("sku_id", "location_id"),
            ),
            DataSnapshotRef(
                dataset="r4a.planning_scenarios",
                snapshot_id=f"sha256-{scenarios_ref.content_sha256[:20]}",
                schema_version="r4a-scenario-v1",
                as_of=config.decision_time,
                content_sha256=scenarios_ref.content_sha256,
                row_count=len(config.scenarios.planning_seeds) * len(config.scenarios.names),
                partition_keys=("scenario_seed", "scenario_name"),
            ),
        ),
        forecast_artifact=ArtifactRef(
            name=config.forecast_authority.selected_model,
            version="r3b-champion-adapter-v1",
            content_sha256=forecast_ref.content_sha256,
        ),
        optimizer_artifact=ArtifactRef(
            name="ortools_cp_sat",
            version="r4a-1.0.0",
            content_sha256=decision_ref.content_sha256,
        ),
        policy_artifact=ArtifactRef(
            name="r4a-constrained-replenishment",
            version="1.0.0",
            content_sha256=policy_ref.content_sha256,
        ),
        scenario_seed=config.scenarios.planning_seeds[0],
        recommendations=tuple(
            RecommendationLine(
                sku_id=line.sku_id,
                location_id=line.location_id,
                recommended_quantity=line.order_quantity_units,
                horizon_days=config.horizon_days,
                reason_code=line.reason_code,
            )
            for line in decision.lines
        ),
        correlation_id="r4a-bounded-smoke",
        trace_id=f"trace-r4a-{config.decision_time.date().isoformat()}",
    )


def run_r4a_once(
    *,
    output_dir: Path,
    config_path: Path = DEFAULT_R4A_CONFIG_PATH,
    inject_failure: InjectedFailure | None = None,
) -> R4ARunResult:
    """Materialize one immutable run, stopping at pending human approval when eligible."""

    config = load_r4a_config(config_path)
    forecast_adapter = load_champion_forecast_adapter(config)
    planning = build_planning_snapshot(config)
    planning_scenarios = build_scenarios(config, purpose="planning")
    evaluation_scenarios = build_scenarios(config, purpose="evaluation")
    planning_ref = _write_contract(
        output_dir,
        dataset="r4a.planning",
        value=planning.model_dump(mode="json"),
        schema="r4a.planning.v1",
    )
    forecast_ref = _write_contract(
        output_dir,
        dataset="r4a.champion_forecast_adapter",
        value=forecast_adapter.model_dump(mode="json"),
        schema="r4a.champion-forecast-adapter.v1",
    )
    planning_scenarios_ref = _write_contract(
        output_dir,
        dataset="r4a.scenarios.planning",
        value=[item.model_dump(mode="json") for item in planning_scenarios],
        schema="r4a.scenario-set.v1",
    )
    evaluation_scenarios_ref = _write_contract(
        output_dir,
        dataset="r4a.scenarios.evaluation",
        value=[item.model_dump(mode="json") for item in evaluation_scenarios],
        schema="r4a.scenario-set.v1",
    )
    policy_ref = _write_contract(
        output_dir,
        dataset="r4a.policy",
        value=config.model_dump(mode="json"),
        schema="r4a.policy.v1",
    )

    heuristic = build_heuristic_policy(config, planning_scenarios)
    optimizer = solve_constrained_policy(
        config,
        planning_scenarios,
        inject_failure=inject_failure,
    )
    decision_ref = _write_contract(
        output_dir,
        dataset="r4a.decision",
        value=optimizer.model_dump(mode="json"),
        schema="r4a.decision.v1",
    )
    heuristic_ref = _write_contract(
        output_dir,
        dataset="r4a.heuristic",
        value=heuristic.model_dump(mode="json"),
        schema="r4a.evaluation-baseline.v1",
    )
    _revalidate_persisted_policies(
        policy_ref=policy_ref,
        scenarios_ref=planning_scenarios_ref,
        decision_ref=decision_ref,
        baseline_ref=heuristic_ref,
    )
    evaluation: EvaluationReport | None = None
    evaluation_ref: ScientificArtifactRef | None = None
    if optimizer.policy_name == "constrained_cp_sat" and optimizer.raw_solver_status in {
        "OPTIMAL",
        "FEASIBLE",
    }:
        evaluation = evaluate_policies(
            config,
            evaluation_scenarios,
            optimizer_lines=optimizer.lines,
            heuristic_lines=heuristic.lines,
        )
        evaluation_ref = _write_contract(
            output_dir,
            dataset="r4a.evaluation",
            value=evaluation.model_dump(mode="json"),
            schema="r4a.evaluation.v1",
        )

    manifest: RecommendationManifest | None = None
    lifecycle_state: DecisionState | None = None
    lifecycle_ref: ScientificArtifactRef | None = None
    manifest_ref: ScientificArtifactRef | None = None
    if optimizer.review_disposition == "REVIEWABLE":
        manifest = _build_manifest(
            config,
            optimizer,
            planning_ref=planning_ref,
            forecast_ref=forecast_ref,
            scenarios_ref=planning_scenarios_ref,
            policy_ref=policy_ref,
            decision_ref=decision_ref,
        )
        manifest_ref = _write_contract(
            output_dir,
            dataset="r4a.recommendation_manifest",
            value=manifest.model_dump(mode="json"),
            schema="nexus.recommendation-manifest.v1",
        )
        workflow = RecommendationWorkflow(
            manifest,
            ledger=InMemoryLifecycleLedger(),
            executor=MockErpExecutor(),
        )
        projection = workflow.submit(
            occurred_at=config.decision_time + timedelta(minutes=1),
            actor_id=manifest.created_by,
            correlation_id=manifest.correlation_id or "r4a-bounded-smoke",
            reason="Feasible R4A recommendation submitted for independent human review.",
        )
        lifecycle_state = projection.state
        lifecycle_ref = _write_contract(
            output_dir,
            dataset="r4a.lifecycle_submission",
            value={
                "events": [event.model_dump(mode="json") for event in projection.events],
                "state": projection.state.value,
            },
            schema="nexus.lifecycle-submission.v1",
        )

    checks = [
        "champion_authority_preserved",
        "planning_evaluation_seeds_disjoint",
        "independent_feasibility_completed",
        "persisted_policy_revalidation_completed",
        "superiority_claim_blocked",
        "transfer_scope_explicitly_absent",
    ]
    if manifest is not None:
        checks.extend(("manifest_created", "stopped_at_pending_approval"))
    else:
        checks.extend(("non_actionable_receipt_created", "manifest_creation_blocked"))
    gate = GateReceipt(
        release="R4A",
        status="MATERIALIZED_UNVERIFIED",
        full_g4_status="OPEN",
        raw_solver_status=optimizer.raw_solver_status,
        independently_feasible=optimizer.feasibility.independently_feasible,
        review_disposition=optimizer.review_disposition,
        manifest_created=manifest is not None,
        lifecycle_state=(
            "pending_approval" if lifecycle_state is DecisionState.PENDING_APPROVAL else None
        ),
        superiority_claim_allowed=False,
        checks=tuple(checks),
    )
    gate_ref = _write_contract(
        output_dir,
        dataset="r4a.gate",
        value=gate.model_dump(mode="json"),
        schema="r4a.gate.v1",
    )
    artifacts = (
        planning_ref,
        forecast_ref,
        planning_scenarios_ref,
        evaluation_scenarios_ref,
        policy_ref,
        heuristic_ref,
        decision_ref,
        *((evaluation_ref,) if evaluation_ref is not None else ()),
        *((manifest_ref,) if manifest_ref is not None else ()),
        *((lifecycle_ref,) if lifecycle_ref is not None else ()),
        gate_ref,
    )
    semantic_sha256 = canonical_json_sha256(
        {
            "decision": optimizer.model_dump(mode="json"),
            "evaluation": None if evaluation is None else evaluation.model_dump(mode="json"),
            "heuristic": heuristic.model_dump(mode="json"),
            "gate": gate.model_dump(mode="json"),
            "manifest_fingerprint": None if manifest is None else manifest.fingerprint,
            "planning": planning.model_dump(mode="json"),
            "forecast_adapter": forecast_adapter.model_dump(mode="json"),
            "planning_scenarios": [item.model_dump(mode="json") for item in planning_scenarios],
        }
    )
    return R4ARunResult(
        config=config,
        planning=planning,
        optimizer=optimizer,
        heuristic=heuristic,
        evaluation=evaluation,
        manifest=manifest,
        lifecycle_state=lifecycle_state,
        artifacts=artifacts,
        semantic_sha256=semantic_sha256,
        gate=gate,
    )


def verify_r4a(
    *,
    artifact_root: Path,
    config_path: Path = DEFAULT_R4A_CONFIG_PATH,
) -> R4AVerificationResult:
    """Run two clean replays plus deterministic UNKNOWN/error fail-closed probes."""

    runtime = _verification_runtime()
    try:
        first = run_r4a_once(output_dir=artifact_root / "run-a", config_path=config_path)
        second = run_r4a_once(output_dir=artifact_root / "run-b", config_path=config_path)
        unknown = run_r4a_once(
            output_dir=artifact_root / "injected-unknown",
            config_path=config_path,
            inject_failure="unknown",
        )
        solver_error = run_r4a_once(
            output_dir=artifact_root / "injected-error",
            config_path=config_path,
            inject_failure="error",
        )
        infeasible = run_r4a_once(
            output_dir=artifact_root / "injected-infeasible",
            config_path=config_path,
            inject_failure="infeasible",
        )
        model_invalid = run_r4a_once(
            output_dir=artifact_root / "injected-model-invalid",
            config_path=config_path,
            inject_failure="model_invalid",
        )
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"[:500]
        failure_sha256 = canonical_json_sha256(
            {"contract": "r4a.verification-failure.v2", "detail": detail}
        )
        receipt = VerificationReceipt(
            release="R4A",
            status="FAIL",
            full_g4_status="OPEN",
            runtime=runtime,
            checks=(
                VerificationCheck(
                    name="setup_or_execution",
                    passed=False,
                    detail=detail,
                ),
            ),
            replay_semantic_sha256=failure_sha256,
            superiority_claim_allowed=False,
        )
        artifact = _write_contract(
            artifact_root,
            dataset="r4a.verification",
            value=receipt.model_dump(mode="json"),
            schema="r4a.verification.v2",
        )
        return R4AVerificationResult(receipt=receipt, artifact=artifact)
    checks = (
        VerificationCheck(
            name="two_run_replay",
            passed=first.semantic_sha256 == second.semantic_sha256,
            detail=(
                "two fresh replay semantic hashes match"
                if first.semantic_sha256 == second.semantic_sha256
                else "two fresh replay semantic hashes differ"
            ),
            evidence_sha256=first.semantic_sha256,
        ),
        VerificationCheck(
            name="solver_and_independent_feasibility",
            passed=(
                first.optimizer.raw_solver_status in {"OPTIMAL", "FEASIBLE"}
                and first.optimizer.feasibility.independently_feasible
                and first.optimizer.feasibility.service_targets_met
                and first.optimizer.deterministic_tie_break_applied
                and first.optimizer.solver_best_bound_cents is not None
                and first.optimizer.optimality_gap_bps is not None
            ),
            detail=(
                f"status={first.optimizer.raw_solver_status}, "
                f"issues={len(first.optimizer.feasibility.issues)}"
            ),
        ),
        VerificationCheck(
            name="human_approval_boundary",
            passed=(
                first.manifest is not None
                and first.lifecycle_state is DecisionState.PENDING_APPROVAL
            ),
            detail=f"state={first.lifecycle_state}",
        ),
        VerificationCheck(
            name="unknown_safe_fallback",
            passed=(
                unknown.manifest is not None
                and unknown.optimizer.fallback_used
                and unknown.optimizer.feasibility.independently_feasible
                and unknown.optimizer.feasibility.service_targets_met
                and unknown.optimizer.review_disposition == "REVIEWABLE"
                and unknown.lifecycle_state is DecisionState.PENDING_APPROVAL
            ),
            detail=(
                f"status={unknown.optimizer.raw_solver_status}, "
                f"manifest={unknown.manifest is not None}"
            ),
        ),
        VerificationCheck(
            name="error_fail_closed",
            passed=(
                solver_error.manifest is None
                and solver_error.optimizer.fallback_used
                and solver_error.optimizer.feasibility.independently_feasible
                and solver_error.optimizer.review_disposition == "RECEIPT_ONLY_SOLVER_FAILURE"
            ),
            detail=(
                f"status={solver_error.optimizer.raw_solver_status}, "
                f"manifest={solver_error.manifest is not None}"
            ),
        ),
        VerificationCheck(
            name="infeasible_and_model_invalid_fail_closed",
            passed=(
                infeasible.manifest is None
                and infeasible.optimizer.review_disposition == "RECEIPT_ONLY_INFEASIBLE"
                and model_invalid.manifest is None
                and model_invalid.optimizer.review_disposition == "RECEIPT_ONLY_SOLVER_FAILURE"
            ),
            detail=(
                f"infeasible_manifest={infeasible.manifest is not None}, "
                f"model_invalid_manifest={model_invalid.manifest is not None}"
            ),
        ),
        VerificationCheck(
            name="frozen_policy_evaluation",
            passed=(
                first.evaluation is not None
                and first.evaluation.evaluation_seeds == first.config.scenarios.evaluation_seeds
                and not first.evaluation.superiority_claim_allowed
                and first.evaluation.claim == "NO_SUPERIORITY_CLAIM"
            ),
            detail=(
                "evaluation="
                + (
                    "missing"
                    if first.evaluation is None
                    else (
                        f"paths={first.evaluation.optimizer.observation_paths}, "
                        f"claim={first.evaluation.claim}"
                    )
                )
            ),
        ),
        VerificationCheck(
            name="immutable_artifacts",
            passed=all(
                _artifact_integrity_ok(item)
                for run in (
                    first,
                    second,
                    unknown,
                    solver_error,
                    infeasible,
                    model_invalid,
                )
                for item in run.artifacts
            ),
            detail=f"run_a_artifacts={len(first.artifacts)}; content_and_file_hashes=verified",
        ),
        VerificationCheck(
            name="recommendation_fingerprint_round_trip",
            passed=(
                first.manifest is not None
                and RecommendationManifest.model_validate(
                    first.manifest.model_dump(mode="python")
                ).fingerprint
                == first.manifest.fingerprint
            ),
            detail=(
                "manifest=missing"
                if first.manifest is None
                else f"fingerprint={first.manifest.fingerprint}"
            ),
        ),
    )
    passed = all(check.passed for check in checks)
    receipt = VerificationReceipt(
        release="R4A",
        status="PASS" if passed else "FAIL",
        full_g4_status="OPEN",
        runtime=runtime,
        checks=checks,
        replay_semantic_sha256=first.semantic_sha256,
        superiority_claim_allowed=False,
    )
    artifact = _write_contract(
        artifact_root,
        dataset="r4a.verification",
        value=receipt.model_dump(mode="json"),
        schema="r4a.verification.v2",
    )
    return R4AVerificationResult(receipt=receipt, artifact=artifact)


def compare_r4a_verification_receipt_files(left_path: Path, right_path: Path) -> dict[str, object]:
    """Require byte-independent Linux/Windows semantic verification agreement."""

    if left_path.resolve() == right_path.resolve():
        raise ValueError("R4A cross-platform comparison requires two distinct receipt files")
    left = VerificationReceipt.model_validate_json(left_path.read_text(encoding="utf-8"))
    right = VerificationReceipt.model_validate_json(right_path.read_text(encoding="utf-8"))
    if left.status != "PASS" or right.status != "PASS":
        raise ValueError("both R4A platform receipts must pass before comparison")
    if not left.runtime.github_runner_os_attested or not right.runtime.github_runner_os_attested:
        raise ValueError("both R4A receipts require matching GitHub runner OS attestation")
    platforms = {left.runtime.os_family, right.runtime.os_family}
    if platforms != {"linux", "windows"}:
        raise ValueError("R4A cross-platform comparison requires one Linux and one Windows receipt")
    if left.runtime.python_minor != right.runtime.python_minor:
        raise ValueError("R4A platform receipts require the same Python minor version")
    left_semantics = left.model_dump(mode="json", exclude={"runtime"})
    right_semantics = right.model_dump(mode="json", exclude={"runtime"})
    if left_semantics != right_semantics:
        raise ValueError("R4A platform receipts disagree on normalized semantic evidence")
    return {
        "full_g4_status": left.full_g4_status,
        "passed": True,
        "platforms": sorted(platforms),
        "python_minor": left.runtime.python_minor,
        "replay_semantic_sha256": left.replay_semantic_sha256,
        "status": "CROSS_PLATFORM_PASS",
        "superiority_claim_allowed": left.superiority_claim_allowed,
    }


__all__ = [
    "R4ARunResult",
    "R4AVerificationResult",
    "compare_r4a_verification_receipt_files",
    "run_r4a_once",
    "verify_r4a",
]
