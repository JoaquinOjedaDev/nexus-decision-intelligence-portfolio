"""Measured local acceptance of the connected pipeline and durable human workflow.

Two real dbt/solver builds run in a new isolated directory. Only the workflow
clock is advanced for the explicitly compressed outcome/cooldown exercise; the
historical scientific cutoff and measured wall-clock timings are never changed.
This is a bounded local smoke profile, not the original production/load gate.
"""

from __future__ import annotations

import csv
import json
import os
import platform
import shutil
import sys
import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from fastapi.testclient import TestClient

from nexus.modeling.r3_artifacts import canonical_json_sha256
from nexus.portfolio.api import create_app
from nexus.portfolio.monitoring import MonitorActual
from nexus.portfolio.runtime_monitoring import monitor_runtime
from nexus.portfolio.runtime_pipeline import runtime_source_fingerprint
from nexus.portfolio.runtime_service import run_runtime_worker, verify_runtime_store
from nexus.portfolio.store import DEMO_MATURITY_SECONDS, PortfolioStore
from nexus.storage import file_sha256

ACCEPTANCE_PROFILE = {
    "name": "connected-runtime-local-smoke-v1",
    "real_builds": 2,
    "maximum_build_seconds": 60,
    "maximum_total_seconds": 180,
    "timing_enforcement": "Observed post-run acceptance thresholds; not a process watchdog",
    "repetitions": 1,
    "load_scope": "Four SKU-location synthetic fixture, one local HTTP client, one local worker",
    "original_full_load_gate_claimed": False,
    "paid_services": False,
    "hosted_ci": False,
}


class RuntimeAcceptanceError(ValueError):
    """A named acceptance boundary failed; its partial receipt is retained."""


def _write_new(path: Path, payload: dict[str, Any]) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")


def _environment() -> dict[str, Any]:
    dependencies: dict[str, str | None] = {}
    for name in ("dbt-core", "dbt-duckdb", "duckdb", "ortools", "fastapi", "httpx"):
        try:
            dependencies[name] = version(name)
        except PackageNotFoundError:
            dependencies[name] = None
    return {
        "python": platform.python_version(),
        "os": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor() or "not_reported",
        "logical_cpu_count": os.cpu_count(),
        "memory_bytes": None,
        "memory_note": "Not measured; this profile makes no memory-capacity claim",
        "dependencies": dependencies,
    }


def _code_binding(root: Path) -> dict[str, Any]:
    paths = [
        *root.glob("src/nexus/portfolio/runtime*.py"),
        *(
            root / relative
            for relative in (
                "src/nexus/portfolio/api.py",
                "src/nexus/portfolio/store.py",
                "src/nexus/portfolio/auth.py",
                "src/nexus/portfolio/monitoring.py",
                "src/nexus/policies/replanning.py",
                "configs/portfolio/runtime-baseline.yaml",
                "pyproject.toml",
                "uv.lock",
            )
        ),
    ]
    files = {path.relative_to(root).as_posix(): file_sha256(path) for path in sorted(set(paths))}
    return {"files": files, "sha256": canonical_json_sha256(files)}


def _changed_source(source: Path, target: Path) -> Path:
    """Create a separate declared synthetic correction; never mutate input evidence."""
    shutil.copytree(source, target)
    path = target / "fulfillment_outcomes.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames
        rows = list(reader)
    if not fields:
        raise RuntimeAcceptanceError("Synthetic demand source has no columns")
    changed = 0
    for row in rows:
        if (row["sku_id"], row["location_id"], row["demand_date"]) == (
            "SKU-001",
            "DC-ARG-01",
            "2026-06-23",
        ):
            row["demand_qty"] = str(int(row["demand_qty"]) + 3)
            changed += 1
    if changed != 1:
        raise RuntimeAcceptanceError("Synthetic correction must address exactly one demand row")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    manifest_path = target / "source-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for item in manifest["sources"]:
        if item["file_name"] == path.name:
            item["content_sha256"] = file_sha256(path)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return target


def _login(client: TestClient, persona: str) -> dict[str, str]:
    response = client.post("/api/session", json={"persona": persona})
    if response.status_code != 200:
        raise RuntimeAcceptanceError(f"HTTP login failed: {response.status_code}")
    return {"X-CSRF-Token": str(response.json()["csrf_token"])}


def run_runtime_acceptance(project_root: Path, state_dir: Path) -> dict[str, Any]:
    """Run two real builds plus durable HTTP checks; retain a hash-bound receipt.

    ``state_dir`` must be new or empty. An existing user/demo state is never reset.
    Returned ``status`` is pass/fail; failures retain completed checks and evidence.
    Session tokens/CSRF secrets are deliberately absent from the receipt.
    """
    root, directory = project_root.resolve(), state_dir.resolve()
    if directory.exists() and (not directory.is_dir() or any(directory.iterdir())):
        raise RuntimeAcceptanceError("Acceptance requires a new or empty isolated directory")
    directory.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    code = _code_binding(root)
    plan = {
        "contract_version": "nexus.runtime-acceptance-plan.v1",
        "declared_at": datetime.now(UTC).isoformat(),
        "profile": ACCEPTANCE_PROFILE,
        "environment": _environment(),
        "code_binding": code,
    }
    _write_new(directory / "acceptance-plan.json", plan)
    result: dict[str, Any] = {
        "contract_version": "nexus.runtime-acceptance.v1",
        "status": "running",
        "plan_sha256": canonical_json_sha256(plan),
        "profile": ACCEPTANCE_PROFILE,
        "environment": plan["environment"],
        "code_binding": code,
        "checks": [],
        "measurements_seconds": {},
        "scientific_bindings": {},
        "scope": {
            "synthetic": True,
            "mock_effects_only": True,
            "real_business_savings_claimed": False,
            "fresh_learned_model_superiority_claimed": False,
            "original_full_gates_closed": False,
            "clock_note": "Operational clock advances only; scientific dates stay historical",
        },
    }

    def check(name: str, passed: bool, observed: Any) -> None:
        result["checks"].append({"name": name, "passed": bool(passed), "observed": observed})
        if not passed:
            raise RuntimeAcceptanceError(name)

    previous_path = os.environ.get("PATH", "")
    os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + previous_path
    try:
        app = create_app(directory / "application", root, pipeline="runtime")
        with TestClient(app) as client:
            check(
                "startup_is_separate_lazy_connected_runtime",
                app.state.portfolio_pipeline == "runtime"
                and app.state.portfolio_store is None
                and client.get("/healthz").status_code == 200,
                {"pipeline": app.state.portfolio_pipeline, "initialized_before_ready": False},
            )
            build_started = time.perf_counter()
            ready = client.get("/readyz")
            elapsed = time.perf_counter() - build_started
            result["measurements_seconds"]["initial_real_build"] = elapsed
            check("initial_real_pipeline_ready", ready.status_code == 200, ready.json())
            check("initial_build_within_local_budget", elapsed <= 60, elapsed)
            store = cast(PortfolioStore, app.state.portfolio_store)
            clock = [store.clock()]
            store.clock = lambda: clock[0]
            original = store.seed()
            result["scientific_bindings"]["initial"] = {
                "source_snapshot_sha256": original["metadata"]["source_snapshot_sha256"],
                "governing_code_sha256": original["metadata"]["governing_code_sha256"],
                "policy_sha256": original["runtime_lineage"]["policy_sha256"],
                "manifest_fingerprint": original["manifest_fingerprint"],
                "scientific_cutoff": original["metadata"]["scientific_decision_time"],
            }
            headers = _login(client, "approver")
            old = client.get("/api/recommendations").json()["items"][0]
            old_url = "/api/recommendations/" + old["id"]
            body = {"fingerprint": old["fingerprint"]}
            approved = client.post(old_url + "/approve", json=body, headers=headers)
            check("old_plan_human_approved", approved.status_code == 200, approved.json()["state"])
            initial_source = directory / "application/evidence/initial/source"
            changed_source = _changed_source(initial_source, directory / "changed-source")
            changed_hash = runtime_source_fingerprint(changed_source)
            headers = _login(client, "planner")
            event = {
                "event_id": str(uuid4()),
                "event_type": "demand.spike",
                # Bound synthetic event time to whole seconds to avoid rounding a
                # sub-microsecond float clock fraction into a future timestamp.
                "occurred_at": datetime.fromtimestamp(int(clock[0]), UTC).isoformat(),
                "source_snapshot_sha256": changed_hash,
            }
            queued = client.post("/api/runtime/events", json=event, headers=headers)
            check("material_event_queued", queued.status_code == 200, queued.json())
            old_state = client.get(old_url).json()["state"]
            check(
                "queued_event_immediately_invalidates_approval",
                old_state == "invalidated",
                old_state,
            )
            duplicate = client.post("/api/runtime/events", json=event, headers=headers)
            check(
                "event_redelivery_is_idempotent",
                duplicate.json()["duplicate"] is True,
                duplicate.json(),
            )
            headers = _login(client, "approver")
            denied = client.post(
                old_url + "/execute",
                json=body,
                headers={**headers, "Idempotency-Key": "acceptance-old-plan"},
            )
            check(
                "invalidated_plan_cannot_execute",
                denied.status_code == 409 and store.metrics_snapshot()["effect_count"] == 0,
                {"http_status": denied.status_code, "body": denied.json()},
            )
            build_started = time.perf_counter()
            worker = run_runtime_worker(root, store, source_dir=changed_source)
            elapsed = time.perf_counter() - build_started
            result["measurements_seconds"]["replacement_real_build"] = elapsed
            check("worker_activates_fresh_plan", worker["status"] == "completed", worker)
            check("replacement_build_within_local_budget", elapsed <= 60, elapsed)
            replacement = store.seed()
            old_q50 = original["forecasts"][0]["points"][0]["q50"]
            new_q50 = replacement["forecasts"][0]["points"][0]["q50"]
            check(
                "visible_demand_changes_fresh_forecast",
                new_q50 == old_q50 + 3,
                {"old": old_q50, "new": new_q50},
            )
            check(
                "replacement_requires_new_approval",
                replacement["manifest_fingerprint"] != original["manifest_fingerprint"],
                {
                    "old": original["manifest_fingerprint"],
                    "new": replacement["manifest_fingerprint"],
                },
            )
            result["scientific_bindings"]["replacement"] = {
                "source_snapshot_sha256": replacement["metadata"]["source_snapshot_sha256"],
                "governing_code_sha256": replacement["metadata"]["governing_code_sha256"],
                "policy_sha256": replacement["runtime_lineage"]["policy_sha256"],
                "manifest_fingerprint": replacement["manifest_fingerprint"],
                "scientific_cutoff": replacement["metadata"]["scientific_decision_time"],
            }
            current = next(
                item
                for item in client.get("/api/recommendations").json()["items"]
                if item["state"] == "pending"
            )
            url = "/api/recommendations/" + current["id"]
            body = {"fingerprint": current["fingerprint"]}
            execution_headers = {**headers, "Idempotency-Key": "acceptance-new-plan"}
            denied_new = client.post(url + "/execute", json=body, headers=execution_headers)
            check(
                "fresh_plan_cannot_inherit_old_approval",
                denied_new.status_code == 409,
                denied_new.json(),
            )
            approved_new = client.post(url + "/approve", json=body, headers=headers)
            check(
                "replacement_human_approved",
                approved_new.status_code == 200,
                approved_new.json()["state"],
            )
            executed = client.post(url + "/execute", json=body, headers=execution_headers)
            check(
                "approved_plan_creates_one_mock_effect",
                executed.status_code == 200 and store.metrics_snapshot()["effect_count"] == 1,
                executed.json()["state"],
            )
            execution_ref = executed.json()["execution_ref"]
            headers = _login(client, "planner")
            immature = client.post(url + "/outcome", json=body, headers=headers)
            check("outcome_requires_server_maturity", immature.status_code == 425, immature.json())
            clock[0] += DEMO_MATURITY_SECONDS + 1
            observed = client.post(url + "/outcome", json=body, headers=headers)
            check(
                "mature_mock_outcome_records_no_causal_claim",
                observed.status_code == 200
                and observed.json()["outcome"]["causal_business_impact_claim"] is False,
                observed.json()["outcome"],
            )
            initial_health = client.get("/api/health").json()
            check(
                "missing_mature_labels_is_not_healthy",
                initial_health["status"] == "insufficient_evidence",
                initial_health["status"],
            )
            series = replacement["forecasts"][0]
            future = datetime.fromtimestamp(clock[0], UTC) + timedelta(days=1)
            future_actual = MonitorActual(
                sku_id=series["sku_id"],
                location_id=series["location_id"],
                target_date=date.fromisoformat(series["points"][0]["date"]),
                quantity=Decimal("999999"),
                available_at=future,
                mature_after=future,
                correction_closed=True,
            )
            measured = monitor_runtime(store, actuals=(future_actual,))
            quality = next(
                value
                for value in measured["checks"].values()
                if value["kind"] == "forecast_quality"
            )
            check(
                "future_available_actual_is_excluded",
                quality["distinct_mature_outcomes"] == 0
                and quality["metrics"] == {}
                and quality["excluded_forecast_rows"].get("actual_not_yet_available") == 1,
                quality,
            )
            check(
                "all_forecast_history_is_point_in_time",
                all(
                    datetime.fromisoformat(row["available_at"])
                    <= datetime.fromisoformat(series["origin"])
                    and row["date"] < series["origin"][:10]
                    for series in replacement["forecasts"]
                    for row in series["history"]
                ),
                {"scientific_cutoff": replacement["metadata"]["scientific_decision_time"]},
            )
            before_restart = store.metrics_snapshot()

        restart_started = time.perf_counter()
        restarted = create_app(directory / "application", root, pipeline="runtime")
        with TestClient(restarted) as client:
            # Real lazy initialization calls prepare_runtime -> verify_runtime_store.
            ready = client.get("/readyz")
            check(
                "restart_verifies_active_retained_artifacts", ready.status_code == 200, ready.json()
            )
            reopened = cast(PortfolioStore, restarted.state.portfolio_store)
            reopened.clock = lambda: clock[0]
            check(
                "restart_preserves_all_workflow_state",
                reopened.metrics_snapshot() == before_restart,
                reopened.metrics_snapshot(),
            )
            verify_runtime_store(root, reopened)
            headers = _login(client, "approver")
            replay = client.post(
                url + "/execute",
                json=body,
                headers={**headers, "Idempotency-Key": "acceptance-new-plan"},
            )
            audit = client.get("/api/audit").json()
            executed_events = sum(item["event_type"] == "executed" for item in audit["items"])
            check(
                "restart_execution_replay_has_one_effect",
                replay.status_code == 200
                and replay.json()["execution_ref"] == execution_ref
                and reopened.metrics_snapshot()["effect_count"] == executed_events == 1,
                {
                    "effect_count": reopened.metrics_snapshot()["effect_count"],
                    "executed_events": executed_events,
                    "execution_ref": replay.json().get("execution_ref"),
                },
            )
            result["measurements_seconds"]["verified_restart_and_replay"] = (
                time.perf_counter() - restart_started
            )
            clock[0] += 1801  # Advance beyond the real declared 1800-second replan cooldown.
            headers = _login(client, "planner")
            unmatched_source = _changed_source(changed_source, directory / "unmatched-source")
            wrong_event = {
                **event,
                "event_id": str(uuid4()),
                "occurred_at": datetime.fromtimestamp(int(clock[0]), UTC).isoformat(),
                "source_snapshot_sha256": runtime_source_fingerprint(unmatched_source),
            }
            check(
                "unmatched_source_event_queued",
                client.post("/api/runtime/events", json=wrong_event, headers=headers).status_code
                == 200,
                {"expected_source_sha256": wrong_event["source_snapshot_sha256"]},
            )
            failed = run_runtime_worker(root, reopened, source_dir=initial_source)
            status = client.get("/api/runtime/status").json()
            failed_job = next(item for item in status["jobs"] if item["id"] == failed.get("job_id"))
            check(
                "worker_source_mismatch_is_durable_and_fail_closed",
                failed["status"] == "failed"
                and failed["reason"] == "source_mismatch"
                and failed_job["state"] == "failed"
                and reopened.metrics_snapshot()["effect_count"] == 1,
                {
                    "worker": failed,
                    "job": failed_job,
                    "effect_count": reopened.metrics_snapshot()["effect_count"],
                },
            )
            result["final_runtime"] = status
            result["final_metrics"] = reopened.metrics_snapshot()
            result["scope"]["simulated_clock_advance_seconds"] = DEMO_MATURITY_SECONDS + 1 + 1801
        check(
            "acceptance_governing_files_unchanged_during_run",
            code == _code_binding(root),
            code["sha256"],
        )
        total = time.perf_counter() - started
        result["measurements_seconds"]["total"] = total
        check("whole_smoke_within_predeclared_local_budget", total <= 180, total)
        result["status"] = "pass"
    except Exception as error:
        result["status"] = "fail"
        result["failure"] = {"type": type(error).__name__, "message": str(error)}
    finally:
        os.environ["PATH"] = previous_path
        result["measurements_seconds"]["total"] = time.perf_counter() - started
        result["finished_at"] = datetime.now(UTC).isoformat()
        result["receipt_sha256"] = canonical_json_sha256(result)
        _write_new(directory / "acceptance-receipt.json", result)
    return result
