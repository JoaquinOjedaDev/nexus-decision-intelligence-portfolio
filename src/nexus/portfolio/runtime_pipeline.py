"""One connected, fresh R2 Gold -> seasonal baseline -> constrained decision run.

This bridge reuses existing public data/scientific/solver implementations. It does
not relabel a new baseline as the frozen R3B champion artifact, perform learned
model training, or claim that engineered audit features are baseline predictors.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Literal, cast
from uuid import NAMESPACE_URL, uuid4, uuid5

import pyarrow.parquet as pq

from nexus.contracts import ArtifactRef, DataSnapshotRef, RecommendationLine, RecommendationManifest
from nexus.data.r2_models import R2RebuildResult, R2SourceManifest, normalize_r2_utc
from nexus.decisioning.r4a_contracts import (
    DemandScenario,
    PolicyDecision,
    PortfolioLine,
    R4AConfig,
    ScenarioDemandLine,
)
from nexus.decisioning.r4a_evaluation import simulate_order_policy
from nexus.decisioning.r4a_optimizer import (
    InjectedFailure,
    build_heuristic_policy,
    solve_constrained_policy,
    validate_decision,
)
from nexus.decisioning.r4a_scenarios import stable_keyed_int
from nexus.modeling.gold_adapter import R3DemandSnapshot, adapt_r2_demand_history
from nexus.modeling.r3_artifacts import (
    ScientificArtifactRef,
    canonical_json_bytes,
    canonical_json_sha256,
    write_immutable_json_manifest,
)
from nexus.modeling.r3_baseline import forecast_probabilistic_seasonal_naive
from nexus.modeling.r3_contracts import R3AConfig, load_r3a_config
from nexus.modeling.r3_features import build_point_in_time_features, feature_artifact_rows
from nexus.modeling.seasonal_naive import DailyDemandObservation
from nexus.portfolio.runtime_contracts import (
    RuntimeForecastAdapter,
    RuntimeForecastLine,
    RuntimeForecastPoint,
    RuntimePlanningConfig,
    RuntimePolicy,
    load_runtime_policy,
)
from nexus.r2 import r2_dbt_code_sha256, r2_pipeline_code_sha256, run_r2_rebuild
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import canonical_rows_sha256, canonical_schema_sha256, file_sha256

RUNTIME_SEED_FILENAME = "runtime-seed.json"


class RuntimePipelineError(ValueError):
    """A runtime source, lineage or decision cannot cross the executable boundary."""


@dataclass(frozen=True, slots=True)
class RuntimeForecastArtifacts:
    snapshot: R3DemandSnapshot
    adapter: RuntimeForecastAdapter
    inventory: tuple[dict[str, Any], ...]
    references: tuple[ScientificArtifactRef, ...]


def _json(path: Path) -> Any:
    def reject(value: str) -> None:
        raise RuntimePipelineError(f"non-finite JSON constant: {value}")

    return json.loads(path.read_text(encoding="utf-8"), parse_constant=reject)


def _publish_runtime_seed(root: Path, envelope: dict[str, Any]) -> None:
    """Durably stage complete bytes, then atomically link without overwriting.

    A process interruption before the link leaves the canonical cache absent.
    The same-filesystem hard link publishes only a fully flushed file; an
    existing different canonical artifact is never replaced. A crash may leave
    an unreferenced uniquely named staging file, which readers safely ignore.
    """
    content = (json.dumps(envelope, allow_nan=False, sort_keys=True, indent=2) + "\n").encode(
        "utf-8"
    )
    destination = root / RUNTIME_SEED_FILENAME
    temporary = root / f".runtime-seed-{uuid4().hex}.tmp"
    owned = False
    try:
        with temporary.open("xb") as handle:
            owned = True
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if destination.read_bytes() != content:
                raise RuntimePipelineError(
                    "concurrent runtime seed publication differs; existing bytes preserved"
                ) from None
        # POSIX directory fsync also persists the new directory entry. Windows
        # has no portable directory-fsync API; the staged file itself is flushed.
        if hasattr(os, "O_DIRECTORY"):
            directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if owned:
            temporary.unlink(missing_ok=True)


def _persist(root: Path, dataset: str, value: Any) -> ScientificArtifactRef:
    return write_immutable_json_manifest(
        root,
        dataset=dataset,
        manifest=value,
        schema_identity={"contract": dataset + ".v1", "classification": "synthetic"},
    )


def _reference(root: Path, path: Path, dataset: str) -> dict[str, Any]:
    actual = path.resolve()
    if not actual.is_relative_to(root):
        raise RuntimePipelineError("runtime artifact escapes its evidence directory")
    return {
        "dataset": dataset,
        "path": actual.relative_to(root).as_posix(),
        "file_sha256": file_sha256(actual),
        "bytes": actual.stat().st_size,
    }


def _source_manifest(source_dir: Path) -> R2SourceManifest:
    manifest = R2SourceManifest.model_validate_json(
        (source_dir / "source-manifest.json").read_text(encoding="utf-8")
    )
    for item in manifest.sources:
        path = (source_dir / item.file_name).resolve()
        if (
            not path.is_relative_to(source_dir)
            or not path.is_file()
            or file_sha256(path) != item.content_sha256
        ):
            raise RuntimePipelineError(f"source checksum or path mismatch: {item.file_name}")
    return manifest


def runtime_source_fingerprint(source_dir: Path) -> str:
    """Bind validated source bytes, independent of their absolute directory."""
    root = source_dir.resolve()
    manifest = _source_manifest(root)
    return canonical_json_sha256(
        {
            "manifest_file_sha256": file_sha256(root / "source-manifest.json"),
            "files": [
                {"name": item.file_name, "sha256": file_sha256(root / item.file_name)}
                for item in sorted(manifest.sources, key=lambda item: item.file_name)
            ],
        }
    )


def runtime_code_fingerprint(project_root: Path) -> str:
    """Current normalized code/feature/dbt identity, independent of source location."""
    project_root = project_root.resolve()
    files = [
        project_root / "src/nexus/r2.py",
        *project_root.glob("src/nexus/data/r2*.py"),
        *project_root.glob("src/nexus/modeling/r3*.py"),
        project_root / "src/nexus/modeling/gold_adapter.py",
        project_root / "src/nexus/modeling/seasonal_naive.py",
        *project_root.glob("src/nexus/decisioning/r4a*.py"),
        project_root / "src/nexus/portfolio/runtime_pipeline.py",
        project_root / "src/nexus/portfolio/runtime_contracts.py",
        project_root / "configs/modeling/r3a-smoke.yaml",
        *project_root.glob("dbt/models/r2/**/*.sql"),
        *project_root.glob("dbt/tests/r2*.sql"),
        *project_root.glob("dbt/tests/generic/r2*.sql"),
        project_root / "dbt/dbt_project.yml",
        project_root / "dbt/profiles.yml",
    ]
    return canonical_json_sha256(
        {
            "r2_pipeline_sha256": r2_pipeline_code_sha256(project_root),
            "r2_dbt_sha256": r2_dbt_code_sha256(project_root),
            "runtime_scientific_files": [
                {
                    "path": path.relative_to(project_root).as_posix(),
                    "sha256": canonical_json_sha256(path.read_text(encoding="utf-8")),
                }
                for path in sorted(set(files))
                if path.is_file()
            ],
        }
    )


def _request_fingerprint(
    project_root: Path,
    source_dir: Path,
    policy: RuntimePolicy,
    cutoff: datetime,
    inject_failure: InjectedFailure | None,
) -> str:
    return canonical_json_sha256(
        {
            "source_manifest_file_sha256": file_sha256(source_dir / "source-manifest.json"),
            "policy": policy.model_dump(mode="json"),
            "cutoff": cutoff,
            "failure_injection": inject_failure,
            "code_sha256": runtime_code_fingerprint(project_root),
        }
    )


def materialize_runtime_forecast(
    rebuild: R2RebuildResult, project_root: Path, output_dir: Path
) -> RuntimeForecastArtifacts:
    """Checksum Gold, validate temporal features, and freshly forecast its exact keys."""
    snapshot = adapt_r2_demand_history(rebuild)
    inventory_ref = rebuild.gold_artifact("gold.r2_inventory_position")
    table = pq.read_table(inventory_ref.path)
    inventory = cast(list[dict[str, Any]], table.to_pylist())
    if (
        len(inventory) != inventory_ref.row_count
        or canonical_rows_sha256(inventory) != inventory_ref.content_sha256
        or canonical_schema_sha256(table.schema) != inventory_ref.schema_sha256
    ):
        raise RuntimePipelineError("Gold inventory checksum/schema mismatch")
    demand_keys = sorted({f"{row.sku_id}:{row.location_id}" for row in snapshot.observations})
    inventory_keys = [f"{row['sku_id']}:{row['location_id']}" for row in inventory]
    if sorted(inventory_keys) != demand_keys or len(set(inventory_keys)) != len(inventory_keys):
        raise RuntimePipelineError("Gold demand and inventory keys must match exactly")
    cutoff = rebuild.planning_cutoff
    for row in inventory:
        if datetime.fromisoformat(row["planning_cutoff"].replace("Z", "+00:00")) != cutoff:
            raise RuntimePipelineError("Gold inventory cutoff differs from forecast issue time")
        if type(row["on_hand_qty"]) is not int or row["on_hand_qty"] < 0:
            raise RuntimePipelineError("Gold on-hand inventory must be nonnegative")

    # All three feature origins remain valid under the public R3A contract. Only
    # the final origin corresponds to this runtime planning action; no labels join.
    feature_payload = load_r3a_config(project_root / "configs/modeling/r3a-smoke.yaml").model_dump(
        mode="python"
    )
    feature_payload["forecast_contract"].update(
        horizon_days=14, issue_time_utc=cutoff.strftime("%H:%M:%S")
    )
    feature_payload["evaluation"]["rolling_origins"] = tuple(
        {"name": name, "issue_time": cutoff - timedelta(days=days), "role": role}
        for name, days, role in (
            ("runtime_diagnostic", 14, "diagnostic"),
            ("runtime_calibration", 7, "calibration"),
            ("runtime_planning", 0, "final_stress"),
        )
    )
    feature_config = R3AConfig.model_validate(feature_payload)
    features = build_point_in_time_features(snapshot.observations, config=feature_config)
    registration_ref = _persist(
        output_dir, "runtime.registration", snapshot.contract.model_dump(mode="json")
    )
    feature_ref = _persist(output_dir, "runtime.features", feature_artifact_rows(features))
    feature_config_ref = _persist(
        output_dir, "runtime.feature_config", feature_config.model_dump(mode="json")
    )
    lines: list[RuntimeForecastLine] = []
    baseline_payload: list[dict[str, Any]] = []
    for key in demand_keys:
        selected = [
            row for row in snapshot.observations if f"{row.sku_id}:{row.location_id}" == key
        ]
        first = selected[0]
        baseline = forecast_probabilistic_seasonal_naive(
            [
                DailyDemandObservation(
                    sku_id=row.sku_id,
                    location_id=row.location_id,
                    demand_date=row.demand_date,
                    quantity=row.demand_qty,
                    available_at=row.latest_available_at,
                )
                for row in selected
            ],
            sku_id=first.sku_id,
            location_id=first.location_id,
            issue_time=cutoff,
            horizon_days=14,
        )
        baseline_payload.append(cast(dict[str, Any], json.loads(canonical_json_bytes(baseline))))
        lines.append(
            RuntimeForecastLine(
                sku_id=first.sku_id,
                location_id=first.location_id,
                eligible_observation_count=baseline.eligible_observation_count,
                eligible_input_sha256=baseline.input_sha256,
                baseline_forecast_sha256=baseline.forecast_sha256,
                residual_count=baseline.residual_count,
                points=tuple(
                    RuntimeForecastPoint(
                        target_date=point.target_date,
                        q025=float(point.q025),
                        q10=float(point.q10),
                        q50=float(point.q50),
                        q90=float(point.q90),
                        q975=float(point.q975),
                    )
                    for point in baseline.points
                ),
            )
        )
    baseline_ref = _persist(output_dir, "runtime.baselines", baseline_payload)
    adapter = RuntimeForecastAdapter(
        issue_time=cutoff,
        source_manifest_sha256=rebuild.source_manifest_sha256,
        gold_demand_sha256=snapshot.contract.source_content_sha256,
        gold_inventory_sha256=inventory_ref.content_sha256,
        registration_sha256=snapshot.contract.registration_sha256,
        feature_sha256=feature_ref.content_sha256,
        lines=tuple(lines),
    )
    adapter_ref = _persist(output_dir, "runtime.forecast_adapter", adapter.model_dump(mode="json"))
    return RuntimeForecastArtifacts(
        snapshot,
        adapter,
        tuple(inventory),
        (registration_ref, feature_ref, feature_config_ref, baseline_ref, adapter_ref),
    )


def runtime_planning_config(
    forecasts: RuntimeForecastArtifacts, policy: RuntimePolicy
) -> RuntimePlanningConfig:
    """Join by the actual source keys, preserving on-hand stock from the same Gold."""
    inventory = {f"{row['sku_id']}:{row['location_id']}": row for row in forecasts.inventory}
    keys = [line.key for line in policy.lines]
    if set(keys) != set(inventory) or keys != [line.key for line in forecasts.adapter.lines]:
        raise RuntimePipelineError("runtime policy, forecast and inventory keys must match exactly")
    locations = {item.location_id for item in policy.location_capacities}
    suppliers = {item.supplier_id for item in policy.supplier_capacities}
    portfolio: list[PortfolioLine] = []
    for line in policy.lines:
        row = inventory[line.key]
        if line.location_id not in locations or row["supplier_id"] not in suppliers:
            raise RuntimePipelineError("source supplier/location capacity is missing")
        portfolio.append(
            PortfolioLine(
                **line.model_dump(mode="python"),
                supplier_id=row["supplier_id"],
                initial_inventory_units=row["on_hand_qty"],
            )
        )
    return RuntimePlanningConfig(
        decision_time=forecasts.adapter.issue_time,
        horizon_days=14,
        budget_cents=policy.budget_cents,
        portfolio=tuple(portfolio),
        location_capacities=policy.location_capacities,
        supplier_capacities=policy.supplier_capacities,
        scenarios=policy.scenarios,
        solver=policy.solver,
        evaluation=policy.evaluation,
    )


def runtime_scenarios(
    adapter: RuntimeForecastAdapter,
    policy: RuntimePolicy,
    *,
    purpose: Literal["planning", "evaluation"],
) -> tuple[DemandScenario, ...]:
    """Adapt daily baseline marginals to reproducible, purpose-separated stress paths."""
    seeds = (
        policy.scenarios.planning_seeds
        if purpose == "planning"
        else policy.scenarios.evaluation_seeds
    )
    scenarios: list[DemandScenario] = []
    for seed in seeds:
        for name in policy.scenarios.names:
            lines: list[ScenarioDemandLine] = []
            for line in adapter.lines:
                values: list[int] = []
                for index, point in enumerate(line.points):
                    draw = stable_keyed_int(
                        seed, "connected-runtime-v1", purpose, line.key, index, modulus=10000
                    )
                    support = (point.q10, point.q50, point.q90)
                    weights = policy.scenarios.demand_quantile_weights_bps
                    selected = (
                        support[0]
                        if draw < weights[0]
                        else support[1]
                        if draw < sum(weights[:2])
                        else support[2]
                    )
                    multiplier = (
                        Decimal(policy.scenarios.demand_surge_percent) / 100
                        if name == "demand_surge"
                        else Decimal(1)
                    )
                    values.append(
                        int(
                            (Decimal(str(selected)) * multiplier).quantize(
                                Decimal(1), rounding=ROUND_HALF_UP
                            )
                        )
                    )
                lines.append(
                    ScenarioDemandLine(
                        sku_id=line.sku_id,
                        location_id=line.location_id,
                        daily_demand_units=tuple(values),
                        regular_delay_days=policy.scenarios.supplier_delay_days
                        if name == "supplier_delay"
                        else 0,
                    )
                )
            scenarios.append(
                DemandScenario(
                    name=name, purpose=purpose, seed=seed, probability_weight=1, lines=tuple(lines)
                )
            )
    return tuple(scenarios)


def _shared_config(config: RuntimePlanningConfig) -> R4AConfig:
    """Adapt the structural algorithm input without constructing R4A provenance.

    Shared solver/validator/simulator functions currently annotate their structural
    inputs with R4AConfig. They use only portfolio/capacity/solver fields. This single
    typing cast preserves their implementation and does not call any frozen forecast
    loader, alter an R4A contract, or serialize a runtime config as an R4A release.
    """
    return cast(R4AConfig, config)


def _manifest(
    config: RuntimePlanningConfig, decision: PolicyDecision, lineage: dict[str, Any]
) -> RecommendationManifest:
    identity = canonical_json_sha256(lineage)
    return RecommendationManifest(
        manifest_id=uuid5(NAMESPACE_URL, "nexus-connected-runtime:" + identity),
        decision_key="connected-runtime:" + config.decision_time.strftime("%Y%m%dT%H%M%SZ"),
        decision_time=config.decision_time,
        valid_until=config.decision_time + timedelta(hours=4),
        created_at=config.decision_time,
        created_by="planner.r4a",
        input_snapshots=tuple(
            DataSnapshotRef(
                dataset=dataset,
                snapshot_id="sha256-" + lineage[key][:20],
                schema_version="runtime-v1",
                as_of=config.decision_time,
                content_sha256=lineage[key],
                row_count=lineage[count],
                partition_keys=("sku_id", "location_id"),
            )
            for dataset, key, count in (
                ("gold.r2_demand_history", "gold_demand_sha256", "demand_rows"),
                ("gold.r2_inventory_position", "gold_inventory_sha256", "inventory_rows"),
                ("runtime.lineage", "lineage_inputs_sha256", "inventory_rows"),
            )
        ),
        forecast_artifact=ArtifactRef(
            name="seasonal_naive_empirical_intervals",
            version="connected-runtime-v1",
            content_sha256=lineage["forecast_sha256"],
        ),
        optimizer_artifact=ArtifactRef(
            name="ortools_cp_sat",
            version="shared-r4a-v1",
            content_sha256=lineage["decision_sha256"],
        ),
        policy_artifact=ArtifactRef(
            name="synthetic-connected-runtime",
            version="1.0.0",
            content_sha256=lineage["policy_sha256"],
        ),
        scenario_seed=config.scenarios.planning_seeds[0],
        recommendations=tuple(
            RecommendationLine(
                sku_id=line.sku_id,
                location_id=line.location_id,
                recommended_quantity=line.order_quantity_units,
                horizon_days=14,
                reason_code=line.reason_code,
            )
            for line in decision.lines
        ),
        correlation_id="connected-runtime",
        trace_id="runtime-" + identity[:24],
    )


def load_runtime_seed(
    evidence_dir: Path, *, expected_fingerprint: str | None = None
) -> dict[str, Any]:
    """Verify every immutable runtime evidence file before returning a cached seed."""
    root = evidence_dir.resolve()
    envelope = _json(root / RUNTIME_SEED_FILENAME)
    payload = envelope["payload"]
    if canonical_json_sha256(payload) != envelope["payload_sha256"]:
        raise RuntimePipelineError("runtime seed checksum mismatch")
    if (
        canonical_json_sha256({key: value for key, value in payload.items() if key != "seed_hash"})
        != payload["seed_hash"]
    ):
        raise RuntimePipelineError("runtime seed identity mismatch")
    if payload.get("metadata", {}).get("pipeline_mode") != "connected_runtime_baseline":
        raise RuntimePipelineError("runtime seed has the wrong provenance contract")
    if expected_fingerprint is not None and payload["input_fingerprint"] != expected_fingerprint:
        raise RuntimePipelineError(
            "runtime inputs changed; use a fresh evidence directory and invalidate old approvals"
        )
    references = {item["dataset"]: item for item in payload["artifacts"]}
    seen: set[str] = set()
    for item in payload["artifacts"]:
        path = (root / item["path"]).resolve()
        if (
            not path.is_relative_to(root)
            or item["path"] in seen
            or not path.is_file()
            or file_sha256(path) != item["file_sha256"]
        ):
            raise RuntimePipelineError("runtime artifact checksum or path mismatch")
        seen.add(item["path"])
    adapter_path = root / references["runtime.forecast_adapter"]["path"]
    adapter = RuntimeForecastAdapter.model_validate_json(adapter_path.read_text(encoding="utf-8"))
    lineage = payload["runtime_lineage"]
    if canonical_json_sha256(adapter.model_dump(mode="json")) != lineage["forecast_sha256"]:
        raise RuntimePipelineError("runtime forecast lineage mismatch")
    if (
        adapter.gold_demand_sha256 != lineage["gold_demand_sha256"]
        or adapter.gold_inventory_sha256 != lineage["gold_inventory_sha256"]
    ):
        raise RuntimePipelineError("runtime Gold lineage mismatch")
    if payload["executable"]:
        manifest = RecommendationManifest.model_validate_json(
            json.dumps(payload["original_manifest"])
        )
        if manifest.fingerprint != payload["manifest_fingerprint"]:
            raise RuntimePipelineError("runtime recommendation manifest mismatch")
        expected = {
            (line.sku_id, line.location_id): line.recommended_quantity
            for line in manifest.recommendations
        }
        actual = {
            (line["sku_id"], line["location_id"]): line["recommended_quantity"]
            for line in payload["recommendations"]
        }
        if len(actual) != len(payload["recommendations"]) or actual != expected:
            raise RuntimePipelineError("runtime recommendation quantities differ from the manifest")
        if not payload["metrics"]["independently_feasible"] or payload["metrics"][
            "solver_status"
        ] not in ("OPTIMAL", "FEASIBLE"):
            raise RuntimePipelineError("non-executable runtime solver outcome")
    elif payload["recommendations"] or payload["original_manifest"] is not None:
        raise RuntimePipelineError("failed runtime cannot contain executable recommendations")
    return cast(dict[str, Any], payload)


def verify_runtime_current(
    project_root: Path, evidence_dir: Path, *, config_path: Path | None = None
) -> dict[str, Any]:
    """Verify durable evidence against current code/policy without source I/O or dbt."""
    seed = load_runtime_seed(evidence_dir)
    policy = load_runtime_policy(
        config_path or project_root / "configs/portfolio/runtime-baseline.yaml"
    )
    if seed["metadata"]["governing_code_sha256"] != runtime_code_fingerprint(project_root):
        raise RuntimePipelineError(
            "runtime governing code changed; rebuild and invalidate approvals"
        )
    if seed["runtime_lineage"]["policy_sha256"] != canonical_json_sha256(
        policy.model_dump(mode="json")
    ):
        raise RuntimePipelineError("runtime policy changed; rebuild and invalidate approvals")
    return seed


def runtime_decision_executable(decision: PolicyDecision) -> bool:
    """Even feasible heuristic fallback cannot become an executable runtime action."""
    return (
        decision.raw_solver_status in {"OPTIMAL", "FEASIBLE"}
        and not decision.fallback_used
        and decision.review_disposition == "REVIEWABLE"
        and decision.feasibility.independently_feasible
        and decision.feasibility.service_targets_met
    )


def build_runtime_seed(
    project_root: Path,
    evidence_dir: Path,
    source_dir: Path | None = None,
    *,
    planning_cutoff: datetime | None = None,
    config_path: Path | None = None,
    inject_failure: InjectedFailure | None = None,
) -> dict[str, Any]:
    """Build a connected fresh baseline seed, or fail closed on changed cached inputs."""
    project_root, root = project_root.resolve(), evidence_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if source_dir is None:
        source_dir = root / "source"
        generate_r2_smoke_fixture(source_dir)
    source_dir = source_dir.resolve()
    source = _source_manifest(source_dir)
    source_fingerprint = runtime_source_fingerprint(source_dir)
    cutoff = normalize_r2_utc(planning_cutoff or source.planning_cutoff)
    policy = load_runtime_policy(
        config_path or project_root / "configs/portfolio/runtime-baseline.yaml"
    )
    request_fingerprint = _request_fingerprint(
        project_root, source_dir, policy, cutoff, inject_failure
    )
    if (root / RUNTIME_SEED_FILENAME).exists():
        return load_runtime_seed(root, expected_fingerprint=request_fingerprint)
    rebuild = run_r2_rebuild(
        project_root=project_root,
        source_dir=source_dir,
        artifact_dir=root / "foundation",
        planning_cutoff=cutoff,
    )
    forecasts = materialize_runtime_forecast(rebuild, project_root, root)
    config = runtime_planning_config(forecasts, policy)
    planning = runtime_scenarios(forecasts.adapter, policy, purpose="planning")
    evaluation = runtime_scenarios(forecasts.adapter, policy, purpose="evaluation")
    shared = _shared_config(config)
    heuristic = build_heuristic_policy(shared, planning)
    decision = solve_constrained_policy(shared, planning, inject_failure=inject_failure)
    independently_checked = validate_decision(shared, planning, decision.lines, decision.recourse)
    if independently_checked != decision.feasibility:
        raise RuntimePipelineError(
            "shared solver result differs from independent feasibility validation"
        )
    executable = runtime_decision_executable(decision)
    references = list(forecasts.references)
    policy_ref = _persist(root, "runtime.policy", policy.model_dump(mode="json"))
    planning_ref = _persist(root, "runtime.planning", config.model_dump(mode="json"))
    scenarios_ref = _persist(
        root,
        "runtime.scenarios",
        {
            "planning": [item.model_dump(mode="json") for item in planning],
            "evaluation": [item.model_dump(mode="json") for item in evaluation],
        },
    )
    decision_ref = _persist(root, "runtime.decision", decision.model_dump(mode="json"))
    heuristic_ref = _persist(root, "runtime.heuristic", heuristic.model_dump(mode="json"))
    references.extend((policy_ref, planning_ref, scenarios_ref, decision_ref, heuristic_ref))
    adapter_ref = next(item for item in references if item.dataset == "runtime.forecast_adapter")
    lineage: dict[str, Any] = {
        "contract_version": "nexus.runtime-lineage.v1",
        "cutoff": cutoff.isoformat(),
        "source_manifest_sha256": rebuild.source_manifest_sha256,
        "source_manifest_file_sha256": rebuild.source_manifest_file_sha256,
        "gold_snapshot_sha256": rebuild.semantic_manifest_sha256,
        "gold_demand_sha256": forecasts.adapter.gold_demand_sha256,
        "gold_inventory_sha256": forecasts.adapter.gold_inventory_sha256,
        "registration_sha256": forecasts.adapter.registration_sha256,
        "feature_sha256": forecasts.adapter.feature_sha256,
        "forecast_sha256": adapter_ref.content_sha256,
        "policy_sha256": policy_ref.content_sha256,
        "planning_sha256": planning_ref.content_sha256,
        "scenario_sha256": scenarios_ref.content_sha256,
        "decision_sha256": decision_ref.content_sha256,
        "demand_rows": len(forecasts.snapshot.observations),
        "inventory_rows": len(forecasts.inventory),
        "baseline_uses_engineered_features": False,
        "input_fingerprint": request_fingerprint,
    }
    lineage["lineage_inputs_sha256"] = canonical_json_sha256(lineage)
    manifest = _manifest(config, decision, lineage) if executable else None
    references.append(_persist(root, "runtime.lineage", lineage))
    references.append(
        _persist(
            root,
            "runtime.gate",
            {
                "executable": executable,
                "raw_solver_status": decision.raw_solver_status,
                "independent_feasibility": independently_checked.model_dump(mode="json"),
                "no_fallback_execution": True,
                "full_g3_closed": False,
            },
        )
    )
    if manifest:
        references.append(_persist(root, "runtime.manifest", manifest.model_dump(mode="json")))
    paths = simulate_order_policy(shared, evaluation, decision.lines)
    baseline_paths = simulate_order_policy(shared, evaluation, heuristic.lines)
    references.append(
        _persist(
            root,
            "runtime.evaluation_paths",
            {
                "optimizer": [asdict(item) for item in paths],
                "heuristic": [asdict(item) for item in baseline_paths],
            },
        )
    )
    scenarios: list[dict[str, Any]] = []
    for name in policy.scenarios.names:
        selected = [item for item in paths if item.path_key.endswith(":" + name)]
        compared = [item for item in baseline_paths if item.path_key.endswith(":" + name)]
        demand = sum(item.demand_units for item in selected)
        scenarios.append(
            {
                "id": name,
                "name": name.replace("_", " ").title(),
                "evaluation_paths": len(selected),
                "demand_units": demand / len(selected),
                "expected_cost": sum(item.total_cost_cents for item in selected)
                / len(selected)
                / 100,
                "baseline_cost": sum(item.total_cost_cents for item in compared)
                / len(compared)
                / 100,
                "fill_rate": sum(item.served_units for item in selected) / demand
                if demand
                else 1.0,
                "stockout_units": sum(item.stockout_units for item in selected) / len(selected),
                "expedite_units": sum(item.expedite_units for item in selected) / len(selected),
                "delay_days": policy.scenarios.supplier_delay_days
                if name == "supplier_delay"
                else 0,
                "demand_multiplier": policy.scenarios.demand_surge_percent / 100
                if name == "demand_surge"
                else 1.0,
                "interpretation": (
                    "Held-out synthetic paths under frozen order quantities; no superiority claim."
                ),
            }
        )
    inventory = {f"{row['sku_id']}:{row['location_id']}": row for row in forecasts.inventory}
    orders = {line.key: line for line in decision.lines}
    config_lines = {line.key: line for line in config.portfolio}
    outcome = {line.key: line for line in evaluation[0].lines}
    recommendations: list[dict[str, Any]] = []
    public_forecasts: list[dict[str, Any]] = []
    for line in forecasts.adapter.lines:
        points = [
            {
                "date": point.target_date.isoformat(),
                "q025": point.q025,
                "q10": point.q10,
                "q50": point.q50,
                "q90": point.q90,
                "q975": point.q975,
            }
            for point in line.points
        ]
        history = [
            {
                "date": row.demand_date.isoformat(),
                "actual": row.demand_qty,
                "available_at": row.latest_available_at.isoformat(),
            }
            for row in forecasts.snapshot.observations
            if row.sku_id == line.sku_id
            and row.location_id == line.location_id
            and row.demand_date < cutoff.date()
            and row.latest_available_at <= cutoff
        ]
        identifier = (
            "runtime-rec-"
            + canonical_json_sha256([lineage["lineage_inputs_sha256"], line.key])[:12]
        )
        public_forecasts.append(
            {
                "id": identifier,
                "sku_id": line.sku_id,
                "location_id": line.location_id,
                "history": history,
                "points": points,
                "model": forecasts.adapter.model_name,
                "origin": cutoff.isoformat(),
                "input_sha256": line.eligible_input_sha256,
                "forecast_sha256": line.baseline_forecast_sha256,
                "residual_count": line.residual_count,
                "adaptation": (
                    "Fresh seasonal baseline; daily quantiles rounded only for integer scenarios."
                ),
                "history_scope": (
                    "Same checksum-verified R2 Gold snapshot; facts available before issue time."
                ),
            }
        )
        if manifest is None:
            continue
        item, order = config_lines[line.key], orders[line.key]
        quantity = order.order_quantity_units
        spread = sum(point.q90 - point.q10 for point in line.points) / max(
            1.0, sum(point.q50 for point in line.points)
        )
        recommendations.append(
            {
                "id": identifier,
                "sku": line.sku_id,
                "sku_id": line.sku_id,
                "location_id": line.location_id,
                "supplier_id": item.supplier_id,
                "status": "pending",
                "state": "pending",
                "created_by": "planner.r4a",
                "recommended_quantity": quantity,
                "unit_cost": item.unit_cost_cents / 100,
                "expected_cost": (
                    quantity * item.unit_cost_cents
                    + (item.fixed_order_cost_cents if quantity else 0)
                )
                / 100,
                "initial_inventory": item.initial_inventory_units,
                "on_order_excluded": inventory[line.key]["on_order_qty"],
                "risk": "high" if spread >= 1 else "medium" if spread >= 0.5 else "low",
                "risk_definition": (
                    "Relative empirical demand interval width, not failure probability."
                ),
                "reason": (
                    "Fresh source/Gold/baseline constrained plan. Open orders excluded from usable "
                    "stock; review before action."
                ),
                "confidence_label": "Fresh empirical P10-P90 interval",
                "forecast_points": points,
                "history": history,
                "lead_time_days": item.regular_lead_time_days,
                "manifest_fingerprint": manifest.fingerprint,
                "evidence_sha256": decision_ref.content_sha256,
                "runtime_lineage_sha256": lineage["lineage_inputs_sha256"],
                "scientific_decision_time": cutoff.isoformat(),
                "observed_demand": sum(outcome[line.key].daily_demand_units),
                "outcome_observation_date": points[-1]["date"],
                "outcome_provenance": "synthetic_runtime_scenario_replay",
                "outcome_scenario_seed": evaluation[0].seed,
            }
        )
    artifact_records = [_reference(root, item.path, item.dataset) for item in references]
    artifact_records.extend(
        _reference(root, item.path, item.dataset)
        for item in (*rebuild.gold_artifacts, *rebuild.landing.validated_artifacts)
    )
    artifact_records.extend(
        _reference(root, path, "runtime.source_native")
        for path in rebuild.landing.source_native_paths
    )
    artifact_records.append(_reference(root, rebuild.summary_path, "runtime.dbt_manifest"))
    source_rows = sum(item.record_count for item in source.sources)
    accepted_rows = sum(
        item.row_count
        for item in rebuild.landing.validated_artifacts
        if item.dataset.startswith("validated.")
    )
    value = sum(item["expected_cost"] for item in recommendations)
    watermark = max(row.latest_available_at for row in forecasts.snapshot.observations)
    payload: dict[str, Any] = {
        "contract_version": "nexus.portfolio.runtime-seed.v1",
        "executable": executable,
        "input_fingerprint": request_fingerprint,
        "manifest_fingerprint": manifest.fingerprint if manifest else None,
        "original_manifest": manifest.model_dump(mode="json") if manifest else None,
        "runtime_lineage": lineage,
        "metadata": {
            "title": "NEXUS Connected Runtime",
            "pipeline_mode": "connected_runtime_baseline",
            "source_snapshot_sha256": source_fingerprint,
            "governing_code_sha256": runtime_code_fingerprint(project_root),
            "classification": "synthetic",
            "scope": "Connected baseline bridge; human review and mock ERP only.",
            "model_champion": forecasts.adapter.model_name,
            "challenger_status": "CHALLENGER_LOST",
            "historical_challenger_status": "CHALLENGER_LOST",
            "superiority_claim_allowed": False,
            "learned_model_training_claimed": False,
            "currency": "USD",
            "horizon_days": 14,
            "scientific_decision_time": cutoff.isoformat(),
            "foundation_snapshot_time": cutoff.isoformat(),
            "source_watermark": watermark.isoformat(),
            "forecast_mode": (
                "Fresh seasonal probabilistic baseline from this exact R2 Gold snapshot; "
                "no learned-model training."
            ),
            "data_scope_note": (
                "All runtime stages share source keys, cutoff and hashes. "
                "Frozen R3B CHALLENGER_LOST remains historical only."
            ),
            "forecast_interval_note": (
                "Five fresh empirical daily quantiles; no joint calibration claim."
            ),
            "inventory_policy": policy.inventory_policy,
            "inventory_limitation": (
                "Actual Gold on-hand is usable stock. Existing open orders remain visible but "
                "excluded; scheduled receipts are not modeled."
            ),
            "policy_costs": (
                "Prices, lead times and capacities are explicit synthetic assumptions."
            ),
            "approval_clock_note": (
                "Review timestamps remain separate from historical synthetic scientific dates."
            ),
        },
        "metrics": {
            "recommendations": len(recommendations),
            "pending_approvals": len(recommendations),
            "order_value": value,
            "budget": policy.budget_cents / 100,
            "budget_utilization": value / (policy.budget_cents / 100),
            "source_rows": source_rows,
            "accepted_rows": accepted_rows,
            "quarantined_rows": len(rebuild.landing.quarantined),
            "gold_rows": sum(rebuild.gold_row_counts.values()),
            "gold_products": len(rebuild.gold_artifacts),
            "dbt_status": "PASS",
            "dbt_summary": _json(rebuild.summary_path)["dbt"]["detail"],
            "source_validity_rate": accepted_rows / source_rows if source_rows else 1.0,
            "solver_status": decision.raw_solver_status,
            "independently_feasible": independently_checked.independently_feasible,
            "fill_rate": scenarios[0]["fill_rate"],
            "quality_counts": rebuild.quality_counts,
            "source_freshness_lag_seconds": int((cutoff - watermark).total_seconds()),
            "quarantine_reasons": dict(
                Counter(row.reason_code.value for row in rebuild.landing.quarantined)
            ),
        },
        "sources": [
            {
                "name": item.file_name,
                "dataset": item.dataset,
                "rows": item.record_count,
                "format": Path(item.file_name).suffix.removeprefix("."),
                "sha256": item.content_sha256,
                "transport": "Checksum-bound synthetic CSV/JSONL source package",
            }
            for item in source.sources
        ],
        "recommendations": recommendations,
        "forecasts": public_forecasts,
        "scenarios": scenarios,
        "lineage": [
            {
                "id": "foundation",
                "stage": "Data foundation",
                "trace_id": "runtime-source-" + rebuild.source_manifest_sha256[:16],
                "source_snapshot": rebuild.source_manifest_sha256,
                "snapshot_sha256": rebuild.semantic_manifest_sha256,
                "validation": "Pydantic + dbt + verified Gold checksums",
                "scope": "Connected runtime source",
            },
            {
                "id": "intelligence",
                "stage": "Intelligence",
                "trace_id": "runtime-forecast-" + adapter_ref.content_sha256[:16],
                "source_snapshot": forecasts.adapter.gold_demand_sha256,
                "forecast_sha256": adapter_ref.content_sha256,
                "model": forecasts.adapter.model_name,
                "scope": "Fresh history-based baseline; feature matrix retained as PIT evidence",
            },
            {
                "id": "decision",
                "stage": "Decisioning",
                "trace_id": manifest.trace_id if manifest else "runtime-failed",
                "policy_sha256": policy_ref.content_sha256,
                "decision_sha256": decision_ref.content_sha256,
                "manifest_fingerprint": manifest.fingerprint if manifest else None,
                "policy": "shared ortools_cp_sat",
                "scope": "Independently validated current-source plan"
                if executable
                else "Non-executable failure receipt",
            },
        ],
        "artifacts": sorted(artifact_records, key=lambda item: item["path"]),
    }
    if runtime_source_fingerprint(source_dir) != source_fingerprint:
        raise RuntimePipelineError("source changed while building the runtime evidence")
    payload["seed_hash"] = canonical_json_sha256(payload)
    envelope = {"payload_sha256": canonical_json_sha256(payload), "payload": payload}
    _publish_runtime_seed(root, envelope)
    return load_runtime_seed(root, expected_fingerprint=request_fingerprint)
