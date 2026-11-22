"""Inspectable portfolio adapter over the existing R2, R3B and R4A authorities.

The R2/dbt example and R3B representative source are deliberately separate source
packages.  The frozen, checksum-bound R3B champion supplies the R4A forecasts; this
module does not claim to retrain it or silently promote its losing challenger.
"""

from __future__ import annotations

import importlib.util
import json
from collections import Counter
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from nexus.contracts import RecommendationManifest
from nexus.data.r2c_models import load_r2c_profile
from nexus.decisioning.r4a_contracts import load_champion_forecast_adapter, load_r4a_config
from nexus.decisioning.r4a_evaluation import simulate_order_policy
from nexus.decisioning.r4a_scenarios import build_scenarios
from nexus.modeling.r3_artifacts import canonical_json_sha256, write_immutable_json_manifest
from nexus.r2 import run_r2_rebuild
from nexus.r4a import run_r4a_once
from nexus.simulation import generate_r2_smoke_fixture, generate_r2c_profile
from nexus.simulation.representative_supply_network import (
    iter_r2c_dataset_rows,
    verify_r2c_package,
)
from nexus.storage import file_sha256

CONTRACT_VERSION = "nexus.portfolio.seed.v1"
SEED_FILENAME = "portfolio-seed.json"


class StalePortfolioEvidenceError(ValueError):
    """Saved evidence belongs to different governing inputs or implementation."""


def _read_json(path: Path) -> Any:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON value: {value}")

    return json.loads(path.read_text(encoding="utf-8"), parse_constant=reject_constant)


def _input_fingerprint(project_root: Path) -> str:
    """Invalidate cached evidence if a governing input or implementation changes."""

    paths = [
        project_root / "configs/decisioning/r4a-smoke.yaml",
        project_root / "configs/profiles/smoke.yaml",
        project_root / "fixtures/r4a/champion-forecast-adapter.json",
        project_root / "src/nexus/r2.py",
        project_root / "src/nexus/r4a.py",
        project_root / "src/nexus/portfolio/engine.py",
        *project_root.glob("src/nexus/data/r2*.py"),
        *project_root.glob("src/nexus/decisioning/r4a*.py"),
        *project_root.glob("src/nexus/simulation/*.py"),
        *project_root.glob("dbt/**/*.sql"),
        *project_root.glob("dbt/models/**/*.yml"),
        *project_root.glob("dbt/models/**/*.yaml"),
        *project_root.glob("dbt/seeds/**/*.csv"),
        project_root / "dbt/dbt_project.yml",
        project_root / "dbt/profiles.yml",
    ]
    entries = [
        {
            "path": path.relative_to(project_root).as_posix(),
            "sha256": canonical_json_sha256(path.read_text(encoding="utf-8")),
        }
        for path in sorted(set(paths))
        if path.is_file() and "target" not in path.parts
    ]
    return canonical_json_sha256(entries)


def _file_record(root: Path, path: Path, *, dataset: str) -> dict[str, Any]:
    resolved = path.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("portfolio artifact escapes evidence directory")
    return {
        "dataset": dataset,
        "path": resolved.relative_to(root).as_posix(),
        "file_sha256": file_sha256(resolved),
        "bytes": resolved.stat().st_size,
    }


def validate_seed(payload: dict[str, Any]) -> None:
    """Reject malformed or internally contradictory data before it reaches the UI."""

    canonical_json_sha256(payload)  # Includes rejection of every nested NaN/Infinity.
    if payload.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("unsupported portfolio seed contract")
    manifest = RecommendationManifest.model_validate_json(json.dumps(payload["original_manifest"]))
    if manifest.fingerprint != payload["manifest_fingerprint"]:
        raise ValueError("portfolio manifest fingerprint mismatch")
    metadata = payload["metadata"]
    if metadata["classification"] != "synthetic" or metadata["superiority_claim_allowed"]:
        raise ValueError("portfolio must retain its synthetic, non-promotional scope")
    if metadata["model_champion"] != "seasonal_naive_empirical_intervals":
        raise ValueError("portfolio cannot replace the governed champion")
    if metadata["challenger_status"] != "CHALLENGER_LOST":
        raise ValueError("portfolio cannot promote the losing challenger")
    manifest_lines = {
        (line.sku_id, line.location_id): line.recommended_quantity
        for line in manifest.recommendations
    }
    recommendations = payload["recommendations"]
    ids = [item["id"] for item in recommendations]
    if len(ids) != len(set(ids)) or len(recommendations) != len(manifest_lines):
        raise ValueError("recommendation identities must be complete and unique")
    for item in recommendations:
        key = (item["sku_id"], item["location_id"])
        quantity = item["recommended_quantity"]
        if type(quantity) is not int or quantity != manifest_lines.get(key):
            raise ValueError("recommendation quantity differs from governed manifest")
        for field in ("unit_cost", "expected_cost", "observed_demand"):
            if type(item[field]) not in (int, float) or item[field] < 0:
                raise ValueError(f"invalid nonnegative recommendation value: {field}")
        for point in item["forecast_points"]:
            if not 0 <= point["q10"] <= point["q50"] <= point["q90"]:
                raise ValueError("forecast quantiles must be ordered and nonnegative")
        if item["manifest_fingerprint"] != manifest.fingerprint:
            raise ValueError("recommendation is bound to a different manifest")
    scenarios = payload["scenarios"]
    if [item["id"] for item in scenarios] != ["normal", "demand_surge", "supplier_delay"]:
        raise ValueError("the three governed scenario families are required")
    for scenario in scenarios:
        if not 0 <= scenario["fill_rate"] <= 1 or scenario["expected_cost"] < 0:
            raise ValueError("invalid scenario metric")


def load_seed(
    artifact_dir: Path, *, expected_input_fingerprint: str | None = None
) -> dict[str, Any]:
    """Reload a local evidence bundle, checking every referenced file before use.

    Hashes detect changes and corruption; they are integrity checks, not signatures
    or protection against an attacker who controls the whole local directory.
    """

    root = artifact_dir.resolve()
    envelope = _read_json(root / SEED_FILENAME)
    payload = envelope["payload"]
    if canonical_json_sha256(payload) != envelope["payload_sha256"]:
        raise ValueError("portfolio seed checksum mismatch")
    validate_seed(payload)
    if (
        expected_input_fingerprint is not None
        and payload["input_fingerprint"] != expected_input_fingerprint
    ):
        raise StalePortfolioEvidenceError(
            "portfolio evidence is stale; use a fresh artifact directory"
        )
    seen: set[str] = set()
    for artifact in payload["artifacts"]:
        relative = artifact["path"]
        path = (root / relative).resolve()
        if relative in seen or not path.is_relative_to(root):
            raise ValueError("duplicate or unsafe portfolio artifact path")
        seen.add(relative)
        if not path.is_file() or file_sha256(path) != artifact["file_sha256"]:
            raise ValueError(f"portfolio artifact checksum mismatch: {relative}")
    return cast(dict[str, Any], payload)


def build_seed(project_root: Path, artifact_dir: Path) -> dict[str, Any]:
    """Build once or integrity-check and reuse a deterministic, offline demo seed.

    Requires the ``data`` and ``r4a`` dependency groups and dbt on PATH. Historical
    scientific timestamps remain untouched. The API adds separate runtime review
    timestamps, so a fresh demonstration can safely exercise human approval.
    """

    project_root = project_root.resolve()
    root = artifact_dir.resolve()
    fingerprint = _input_fingerprint(project_root)
    if (root / SEED_FILENAME).exists():
        return load_seed(root, expected_input_fingerprint=fingerprint)
    if importlib.util.find_spec("ortools") is None:
        raise RuntimeError("portfolio requires the data and r4a dependency groups; install them")
    root.mkdir(parents=True, exist_ok=True)
    config_path = project_root / "configs/decisioning/r4a-smoke.yaml"
    config = load_r4a_config(config_path)
    adapter = load_champion_forecast_adapter(config)

    # This independent source example demonstrates actual CSV/JSONL -> dbt -> Gold.
    source = generate_r2_smoke_fixture(root / "foundation/source")
    foundation = run_r2_rebuild(
        project_root=project_root,
        source_dir=source.source_dir,
        artifact_dir=root / "foundation/rebuild",
        planning_cutoff=source.manifest.planning_cutoff,
    )

    # Recreate only the frozen model's synthetic source profile for honest history.
    # No refit, model selection, or promotion is performed by this presentation layer.
    representative_config = load_r2c_profile(project_root / "configs/profiles/smoke.yaml")
    representative = generate_r2c_profile(root / "representative", config=representative_config)
    representative_manifest = verify_r2c_package(
        representative.output_dir, expected_config=representative_config
    )
    history: dict[str, list[dict[str, Any]]] = {line.key: [] for line in config.portfolio}
    for row in iter_r2c_dataset_rows(
        representative.output_dir,
        dataset="representative.demand_history",
        verified_manifest=representative_manifest,
    ):
        key = f"{row['sku_id']}:{row['location_id']}"
        observed_day = cast(date, row["demand_date"])
        if (
            key in history
            and observed_day < adapter.source_origin.date()
            and cast(datetime, row["available_at"]) <= adapter.source_origin
            and row["is_active"]
            and not row["stockout_censored"]
        ):
            history[key].append(
                {"date": observed_day.isoformat(), "actual": row["observed_sales_qty"]}
            )
    for key, values in history.items():
        history[key] = sorted(values, key=lambda row: row["date"])[-28:]

    result = run_r4a_once(output_dir=root / "decision", config_path=config_path)
    if result.manifest is None or not result.optimizer.feasibility.independently_feasible:
        raise RuntimeError(
            "portfolio decision is not reviewable: "
            f"{result.optimizer.raw_solver_status}/{result.optimizer.fallback_reason}"
        )
    manifest = result.manifest
    evaluation_scenarios = build_scenarios(config, purpose="evaluation")
    optimizer_paths = simulate_order_policy(config, evaluation_scenarios, result.optimizer.lines)
    baseline_paths = simulate_order_policy(config, evaluation_scenarios, result.heuristic.lines)
    path_ref = write_immutable_json_manifest(
        root,
        dataset="portfolio.scenario_paths",
        manifest={
            "optimizer": [asdict(path) for path in optimizer_paths],
            "baseline": [asdict(path) for path in baseline_paths],
        },
        schema_identity={"contract": "portfolio.scenario-paths.v1"},
    )
    scenarios: list[dict[str, Any]] = []
    labels = {
        "normal": "Normal demand",
        "demand_surge": "Demand surge",
        "supplier_delay": "Supplier delay",
    }
    for name in config.scenarios.names:
        paths = [path for path in optimizer_paths if path.path_key.endswith(":" + name)]
        baselines = [path for path in baseline_paths if path.path_key.endswith(":" + name)]
        demand = sum(path.demand_units for path in paths)
        served = sum(path.served_units for path in paths)
        scenarios.append(
            {
                "id": name,
                "name": labels[name],
                "evaluation_paths": len(paths),
                "demand_units": demand / len(paths),
                "expected_cost": sum(path.total_cost_cents for path in paths) / len(paths) / 100,
                "baseline_cost": sum(path.total_cost_cents for path in baselines)
                / len(baselines)
                / 100,
                "fill_rate": served / demand if demand else 1.0,
                "stockout_units": sum(path.stockout_units for path in paths) / len(paths),
                "expedite_units": sum(path.expedite_units for path in paths) / len(paths),
                "delay_days": config.scenarios.supplier_delay_days
                if name == "supplier_delay"
                else 0,
                "demand_multiplier": config.scenarios.demand_surge_percent / 100
                if name == "demand_surge"
                else 1.0,
                "interpretation": (
                    "Mean of held-out synthetic paths; order quantities are frozen, "
                    "not re-optimized."
                ),
                "evidence_sha256": path_ref.content_sha256,
            }
        )
    portfolio = {line.key: line for line in config.portfolio}
    forecast_lines = {line.key: line for line in adapter.lines}
    outcome_lines = {line.key: line for line in evaluation_scenarios[0].lines}
    recommendations: list[dict[str, Any]] = []
    forecasts: list[dict[str, Any]] = []
    for decision in result.optimizer.lines:
        line = portfolio[decision.key]
        forecast = forecast_lines[decision.key]
        points = [
            {
                "date": (config.decision_time.date() + timedelta(days=index + 1)).isoformat(),
                "q10": lower,
                "q50": median,
                "q90": upper,
            }
            for index, (lower, median, upper) in enumerate(
                zip(
                    forecast.daily_q10_demand,
                    forecast.daily_median_demand,
                    forecast.daily_q90_demand,
                    strict=True,
                )
            )
        ]
        quantity = decision.order_quantity_units
        cost_cents = quantity * line.unit_cost_cents + (
            line.fixed_order_cost_cents if quantity else 0
        )
        spread_ratio = (sum(forecast.daily_q90_demand) - sum(forecast.daily_q10_demand)) / max(
            1, sum(forecast.daily_median_demand)
        )
        identifier = "rec-" + canonical_json_sha256([manifest.fingerprint, decision.key])[:12]
        recommendations.append(
            {
                "id": identifier,
                "sku": line.sku_id,
                "sku_id": line.sku_id,
                "location_id": line.location_id,
                "supplier_id": line.supplier_id,
                "status": "pending",
                "state": "pending",
                "created_by": "planner.r4a",
                "recommended_quantity": quantity,
                "unit_cost": line.unit_cost_cents / 100,
                "expected_cost": cost_cents / 100,
                "initial_inventory": line.initial_inventory_units,
                "risk": "high" if spread_ratio >= 1 else "medium" if spread_ratio >= 0.5 else "low",
                "risk_definition": (
                    "Relative width of the empirical demand interval; not a failure probability."
                ),
                "reason": (
                    "Constrained plan respects budget, supplier capacity, storage, "
                    f"and order multiples. {decision.reason_code.replace('_', ' ')}."
                ),
                "confidence_label": "Empirical P10-P90 interval",
                "forecast_points": points,
                "history": history[decision.key],
                "lead_time_days": line.regular_lead_time_days,
                "manifest_fingerprint": manifest.fingerprint,
                "evidence_sha256": result.artifact["r4a.decision"].content_sha256,
                "scientific_decision_time": config.decision_time.isoformat(),
                "observed_demand": sum(outcome_lines[decision.key].daily_demand_units),
                "outcome_observation_date": points[-1]["date"],
                "outcome_provenance": "synthetic_scenario_replay",
                "outcome_scenario_seed": evaluation_scenarios[0].seed,
            }
        )
        forecasts.append(
            {
                "id": identifier,
                "sku_id": line.sku_id,
                "location_id": line.location_id,
                "history": history[decision.key],
                "points": points,
                "model": adapter.selected_model,
                "origin": adapter.source_origin.isoformat(),
                "adaptation": adapter.adaptation,
                "history_scope": (
                    "Recreated R2C smoke observations available at origin; "
                    "active and uncensored only."
                ),
            }
        )

    artifacts = [
        _file_record(root, reference.path, dataset=reference.dataset)
        for reference in result.artifacts
    ]
    artifacts.append(_file_record(root, path_ref.path, dataset=path_ref.dataset))
    artifacts.extend(
        _file_record(root, reference.path, dataset=reference.dataset)
        for reference in (*foundation.gold_artifacts, *foundation.landing.validated_artifacts)
    )
    artifacts.extend(
        _file_record(root, path, dataset="r2.source_native")
        for path in foundation.landing.source_native_paths
    )
    artifacts.append(_file_record(root, source.manifest_path, dataset="r2.source_manifest"))
    artifacts.append(_file_record(root, foundation.summary_path, dataset="r2.dbt_build_manifest"))
    artifacts.append(
        _file_record(root, representative.manifest_path, dataset="r2c.source_manifest")
    )
    artifacts.extend(
        _file_record(root, representative.output_dir / shard.relative_path, dataset=shard.dataset)
        for shard in representative_manifest.shards
    )
    source_rows = sum(item.record_count for item in source.manifest.sources)
    accepted_rows = sum(
        item.row_count
        for item in foundation.landing.validated_artifacts
        if item.dataset.startswith("validated.")
    )
    quarantined = len(foundation.landing.quarantined)
    total_order_cost = sum(item["expected_cost"] for item in recommendations)
    payload: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "input_fingerprint": fingerprint,
        "manifest_fingerprint": manifest.fingerprint,
        "original_manifest": manifest.model_dump(mode="json"),
        "metadata": {
            "title": "NEXUS Decision Intelligence",
            "classification": "synthetic",
            "scope": "Local portfolio demonstration; human review and mock ERP only.",
            "model_champion": adapter.selected_model,
            "challenger_status": adapter.challenger_gate_status,
            "superiority_claim_allowed": False,
            "currency": "USD",
            "horizon_days": config.horizon_days,
            "scientific_decision_time": config.decision_time.isoformat(),
            "foundation_snapshot_time": foundation.planning_cutoff.isoformat(),
            "forecast_mode": (
                "Frozen checksum-verified R3B champion adapter; no retraining in the demo."
            ),
            "data_scope_note": (
                "R2 CSV/JSONL/dbt quality example is independent of the R2C/R3B "
                "forecasting fixture. No fabricated join connects them."
            ),
            "forecast_interval_note": (
                "Rounded empirical daily marginal P10/P50/P90 repeated over 14 days; "
                "joint calibration is not claimed."
            ),
            "approval_clock_note": (
                "Runtime review timestamps are separate from the historical "
                "synthetic scientific dates."
            ),
        },
        "metrics": {
            "recommendations": len(recommendations),
            "pending_approvals": len(recommendations),
            "order_value": total_order_cost,
            "budget": config.budget_cents / 100,
            "budget_utilization": total_order_cost / (config.budget_cents / 100),
            "source_rows": source_rows,
            "accepted_rows": accepted_rows,
            "quarantined_rows": quarantined,
            "gold_rows": sum(foundation.gold_row_counts.values()),
            "gold_products": len(foundation.gold_artifacts),
            "dbt_status": "PASS",
            "dbt_summary": _read_json(foundation.summary_path)["dbt"]["detail"],
            "source_validity_rate": accepted_rows / source_rows if source_rows else 1.0,
            "solver_status": result.optimizer.raw_solver_status,
            "independently_feasible": result.optimizer.feasibility.independently_feasible,
            "fill_rate": scenarios[0]["fill_rate"],
            "quality_counts": foundation.quality_counts,
            "quarantine_reasons": dict(
                Counter(row.reason_code.value for row in foundation.landing.quarantined)
            ),
        },
        "recommendations": recommendations,
        "forecasts": forecasts,
        "scenarios": scenarios,
        "sources": [
            {
                "name": item.file_name,
                "dataset": item.dataset,
                "rows": item.record_count,
                "format": Path(item.file_name).suffix.removeprefix("."),
                "sha256": item.content_sha256,
                "transport": "Local synthetic fixture; no live API connection",
            }
            for item in source.manifest.sources
        ],
        "lineage": [
            {
                "id": "foundation",
                "stage": "Data foundation",
                "trace_id": "r2-" + foundation.semantic_manifest_sha256[:16],
                "source_snapshot": source.manifest_sha256,
                "snapshot_sha256": foundation.semantic_manifest_sha256,
                "validation": "Pydantic source contracts + dbt build/tests",
                "scope": "Independent R2 data-quality example",
            },
            {
                "id": "intelligence",
                "stage": "Intelligence",
                "trace_id": "r3b-" + adapter.source_forecast_artifact_sha256[:16],
                "source_snapshot": representative_manifest.dataset_semantic_sha256,
                "forecast_sha256": adapter.source_forecast_artifact_sha256,
                "promotion_sha256": adapter.source_promotion_artifact_sha256,
                "model": adapter.selected_model,
                "scope": "Frozen R3B champion authority + recreated representative history",
            },
            {
                "id": "decision",
                "stage": "Decisioning",
                "trace_id": manifest.trace_id,
                "policy_sha256": result.artifact["r4a.policy"].content_sha256,
                "decision_sha256": result.artifact["r4a.decision"].content_sha256,
                "manifest_fingerprint": manifest.fingerprint,
                "policy": "ortools_cp_sat",
                "scope": "Fresh constrained optimization; pending independent human approval",
            },
        ],
        "artifacts": sorted(artifacts, key=lambda item: item["path"]),
    }
    validate_seed(payload)
    envelope = {"payload_sha256": canonical_json_sha256(payload), "payload": payload}
    # Exclusive creation prevents a second process from replacing the first evidence.
    with (root / SEED_FILENAME).open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(envelope, handle, ensure_ascii=False, allow_nan=False, sort_keys=True, indent=2)
        handle.write("\n")
    return load_seed(root, expected_input_fingerprint=fingerprint)
