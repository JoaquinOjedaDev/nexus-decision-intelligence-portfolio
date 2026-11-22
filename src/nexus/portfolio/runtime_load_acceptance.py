"""Predeclared, paid-service-free four-key API smoke profile, not a G7 gate."""

from __future__ import annotations

import hashlib
import math
import os
import platform
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any
from uuid import uuid4

from nexus.portfolio.auth import COOKIE_NAME, resolve_persona
from nexus.portfolio.store import PortfolioStore, digest

PROFILE = {
    "id": "nexus.local.runtime-api-smoke.v1",
    "current_product_location_keys": 4,
    "minimum_repetitions": 3,
    "warmup_reads": 3,
    "concurrent_clients": 6,
    "reads_per_repetition": 24,
    "duplicate_executions_per_repetition": 24,
    "p95_api_limit_ms": 500,
    "percentile_method": "nearest-rank, ceil(0.95*n), on all measured requests",
    "original_G7_gate_closed": False,
}


def _p95(values: list[float]) -> float:
    if not values:
        raise ValueError("Percentile requires at least one measurement")
    return sorted(values)[math.ceil(0.95 * len(values)) - 1]


def _code_binding(root: Path) -> dict[str, str]:
    names = ["api.py", "store.py", "auth.py", "runtime_control.py", "runtime_load_acceptance.py"]
    files = [root / "src/nexus/portfolio" / name for name in names]
    files += [root / "pyproject.toml", root / "uv.lock"]
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in files
    }


def _packages() -> dict[str, str]:
    return {name: version(name) for name in ("fastapi", "starlette", "httpx", "pyjwt")}


def _measure(
    client: Any, operation: str, path: str, body: dict[str, Any], headers: dict[str, str]
) -> dict[str, Any]:
    started = time.perf_counter()
    status, error = 0, None
    try:
        response = (
            client.post(path, json=body, headers=headers)
            if operation == "execute"
            else client.get(path)
        )
        status = response.status_code
        if status != 200:
            error = response.json().get("error", {}).get("code", "http_error")
    except Exception as exc:
        error = type(exc).__name__
    return {
        "operation": operation,
        "latency_ms": (time.perf_counter() - started) * 1000,
        "status_code": status,
        "error": error,
    }


def run_runtime_load_acceptance(
    project_root: Path,
    store: PortfolioStore,
    *,
    allow_mutation: bool = False,
    repetitions: int = 3,
) -> dict[str, Any]:
    """Mutate only an explicitly caller-owned prepared isolation state; never build data.

    TestClient runs in-process ASGI, without sockets or app background-monitor lifespan.
    Login/approval/warmup are excluded. Request timings include session validation and
    SQLite work. Round elapsed includes client/executor overhead. Memory is Python
    traced allocations only, not RSS, native DuckDB/model buffers, or child processes.
    """
    if allow_mutation is not True:
        raise ValueError("Explicit allow_mutation=True is required for isolated acceptance state")
    if type(repetitions) is not int or not 3 <= repetitions <= 10:
        raise ValueError("Use 3 through 10 predeclared-profile repetitions")
    if tracemalloc.is_tracing():
        raise ValueError("Another Python allocation trace is active; measurement was not changed")
    from fastapi.testclient import TestClient

    from nexus.portfolio.api import create_app

    observer = resolve_persona("auditor", store.locations())
    snapshot = store.overview_snapshot(observer)
    current = snapshot["current_recommendations"]
    if (
        snapshot.get("metadata", {}).get("pipeline_mode") != "connected_runtime_baseline"
        or len(current) != 4
        or len({(item["sku_id"], item["location_id"]) for item in current}) != 4
    ):
        raise ValueError("The predeclared profile requires exactly four connected runtime keys")
    pending = [item for item in current if item["state"] == "pending" and not item["expired"]]
    if not pending:
        raise ValueError("An unexpired pending recommendation is required; history is retained")
    root = project_root.resolve()
    code = _code_binding(root)
    before = store.metrics_snapshot()
    item = pending[0]
    endpoint = f"/api/recommendations/{item['id']}"
    execution = {"fingerprint": item["fingerprint"]}
    app = create_app(store.path.parent, root, pipeline="runtime")
    app.state.portfolio_store = store
    with closing(TestClient(app, follow_redirects=False)) as setup:
        # closing() avoids TestClient lifespan; no scheduler contaminates the load profile.
        login = setup.post("/api/session", json={"persona": "approver"})
        if login.status_code != 200:
            raise ValueError("Acceptance persona initialization failed")
        token = setup.cookies[COOKIE_NAME]
        headers = {
            "X-CSRF-Token": login.json()["csrf_token"],
            "Idempotency-Key": f"load-acceptance-{uuid4().hex}",
        }
        approved = setup.post(
            endpoint + "/approve",
            json={**execution, "note": "Independent local load acceptance."},
            headers=headers,
        )
        if approved.status_code != 200:
            raise ValueError("Acceptance approval failed; no latency verdict was produced")
        warmup = [_measure(setup, "overview", "/api/overview", {}, {}) for _ in range(3)]
        reads = [
            ("overview", "/api/overview") if index % 2 == 0 else ("detail", endpoint)
            for index in range(24)
        ]
        tasks = [entry for read in reads for entry in (read, ("execute", endpoint + "/execute"))]

        def chunk(entries: list[tuple[str, str]]) -> list[dict[str, Any]]:
            client = TestClient(app, follow_redirects=False)
            try:
                client.cookies.set(COOKIE_NAME, token)
                return [_measure(client, op, path, execution, headers) for op, path in entries]
            finally:
                client.close()

        rounds: list[dict[str, Any]] = []
        traced_peak = 0
        tracemalloc.start()
        try:
            load_started = time.perf_counter()
            for repetition in range(repetitions):
                started = time.perf_counter()
                with ThreadPoolExecutor(max_workers=6) as workers:
                    batches = list(workers.map(chunk, [tasks[index::6] for index in range(6)]))
                samples = [sample for batch in batches for sample in batch]
                rounds.append(
                    {
                        "repetition": repetition + 1,
                        "samples": samples,
                        "elapsed_ms": (time.perf_counter() - started) * 1000,
                        "p95_api_ms": _p95([sample["latency_ms"] for sample in samples]),
                    }
                )
            load_elapsed_ms = (time.perf_counter() - load_started) * 1000
            traced_peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
    after = store.metrics_snapshot()
    restarted_at = time.perf_counter()
    reopened = PortfolioStore(store.path.parent, clock=store.clock)
    recovered = reopened.recommendation_context(item["id"], observer)
    recovered_metrics = reopened.metrics_snapshot()
    restarted_app = create_app(store.path.parent, root, pipeline="runtime")
    restarted_app.state.portfolio_store = reopened
    with closing(TestClient(restarted_app, follow_redirects=False)) as restart_client:
        restart_client.cookies.set(COOKIE_NAME, token)
        recovered_response = restart_client.get(endpoint)
    restart_ok = (
        digest(reopened.seed()) == snapshot["generation_seed_sha256"]
        and recovered["state"] == "executed"
        and recovered["fingerprint"] == item["fingerprint"]
        and recovered_metrics["effect_count"] == before["effect_count"] + 1
        and recovered_metrics["event_count"] == after["event_count"]
        and recovered_response.status_code == 200
        and recovered_response.json().get("state") == "executed"
    )
    restart_ms = (time.perf_counter() - restarted_at) * 1000
    samples = [sample for run in rounds for sample in run["samples"]]
    p95 = _p95([sample["latency_ms"] for sample in samples])
    checks = {
        "warmup_ok": all(sample["status_code"] == 200 for sample in warmup),
        "requests_ok": all(sample["status_code"] == 200 for sample in samples),
        "p95_local_profile": p95 <= 500 and all(run["p95_api_ms"] <= 500 for run in rounds),
        "one_additional_mock_effect": after["effect_count"] == before["effect_count"] + 1,
        "logical_restart_verified": restart_ok,
        "code_unchanged_during_measurement": _code_binding(root) == code,
    }
    return {
        "schema": "nexus.portfolio.runtime-load-acceptance.v1",
        "status": "passed" if all(checks.values()) else "failed",
        "profile": dict(PROFILE),
        "checks": checks,
        "recorded_at": datetime.now(UTC).isoformat(),
        "environment": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "logical_cpus": os.cpu_count(),
            "python": platform.python_version(),
            "packages": _packages(),
        },
        "binding": {
            "code_files": code,
            "code_sha256": digest(code),
            "seed_sha256": snapshot["generation_seed_sha256"],
            "source_sha256": snapshot["metadata"].get("source_snapshot_sha256"),
            "governing_code_sha256": snapshot["metadata"].get("governing_code_sha256"),
            "runtime_lineage": snapshot.get("runtime_lineage", {}),
            "retained_recommendations": len(snapshot["recommendations"]),
            "recommendation_id": item["id"],
            "fingerprint": item["fingerprint"],
        },
        "warmup": warmup,
        "repetitions": rounds,
        "p95_api_ms": p95,
        "p95_by_operation_ms": {
            op: _p95([s["latency_ms"] for s in samples if s["operation"] == op])
            for op in ("overview", "detail", "execute")
        },
        "load_elapsed_ms": load_elapsed_ms,
        "memory": {
            "python_traced_peak_bytes": traced_peak,
            "scope": "Load rounds only; Python allocations, not process RSS/native/children",
        },
        "recovery": {
            "logical_reopen_verified": restart_ok,
            "elapsed_ms": restart_ms,
            "scope": "New PortfolioStore in same process; not a process crash or RTO claim",
            "backup_restore": "not_exercised_no_established_store_interface",
        },
        "scope": {
            "transport": "in_process_ASGI_TestClient_no_network",
            "includes": "HTTP middleware, JWT validation, SQLite, tracing overhead",
            "excludes": "data/dbt/model preparation, login, approval, warmup from timed samples",
            "threshold": "Predeclared local four-key smoke only; not original G7 acceptance",
            "paid_services": False,
            "original_G7_gate_closed": False,
        },
    }
