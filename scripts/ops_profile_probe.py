"""Bounded, read-only HTTP observations for an already running local ops API.

Only POST /api/session creates an auditor session. Every business probe is a GET.
This is a component diagnostic, not an ops-profile or soak acceptance runner.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

CONTRACT = "nexus.ops-profile-probe.v1"
DEFAULT_BASE_URL = "http://127.0.0.1:8767"
UNVERIFIED = (
    "seven_worker_process_supervision",
    "scoped_service_credentials",
    "upstream_event_time_freshness",
    "broker_ack_loss_and_recovery",
    "cross_service_trace",
    "load_and_resource_envelope",
    "three_four_hour_soaks",
)
_SHA = re.compile(r"[a-f0-9]{64}\Z")
_SOURCE_REASONS = frozenset(
    {
        "fresh",
        "source_observation_missing",
        "source_observation_expired",
        "source_observation_superseded",
        "source_read_failed",
        "source_changed",
        "source_publication_pending",
    }
)
_CAPTURE_REASONS = frozenset(
    {
        "caught_up_at_observation",
        "capture_observation_missing",
        "capture_observation_expired",
        "capture_lagging",
        "capture_ack_pending",
        "capture_sample_superseded",
        "capture_clock_regressed",
    }
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _milliseconds(start: float) -> float:
    return round((time.monotonic() - start) * 1000, 3)


def _count(value: Any) -> bool:
    return type(value) is int and value >= 0


def _failure(
    reason: str, *, status: int | None = None, latency_ms: float | None = None
) -> dict[str, Any]:
    result: dict[str, Any] = {"ok": False, "reason": reason}
    if status is not None:
        result["http_status"] = status
    if latency_ms is not None:
        result["latency_ms"] = latency_ms
    return result


def _json_response(
    client: httpx.Client, method: str, path: str, **kwargs: Any
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    start = time.monotonic()
    try:
        response = client.request(method, path, **kwargs)
    except httpx.TimeoutException:
        return None, _failure("timeout", latency_ms=_milliseconds(start))
    except httpx.RequestError:
        return None, _failure("transport_error", latency_ms=_milliseconds(start))
    latency = _milliseconds(start)
    if response.status_code != 200:
        return None, _failure(
            f"http_{response.status_code}", status=response.status_code, latency_ms=latency
        )
    try:
        body = response.json()
    except ValueError:
        return None, _failure("malformed_json", status=200, latency_ms=latency)
    if not isinstance(body, dict):
        return None, _failure("invalid_payload", status=200, latency_ms=latency)
    return body, {"ok": True, "reason": "ok", "http_status": 200, "latency_ms": latency}


def _login(client: httpx.Client) -> dict[str, Any]:
    body, request = _json_response(client, "POST", "/api/session", json={"persona": "auditor"})
    if body is None:
        return request
    user = body.get("user")
    if (
        not isinstance(user, dict)
        or user.get("role") != "auditor"
        or not isinstance(body.get("csrf_token"), str)
        or not body["csrf_token"]
        or not client.cookies.get("nexus_portfolio_session")
    ):
        return {**request, "ok": False, "reason": "invalid_session"}
    return request


def _session(client: httpx.Client) -> dict[str, Any]:
    body, request = _json_response(client, "GET", "/api/session")
    if body is None:
        return request
    user = body.get("user")
    if not isinstance(user, dict) or user.get("role") != "auditor":
        return {**request, "ok": False, "reason": "unauthorized_session"}
    return request


def _seed(client: httpx.Client) -> dict[str, Any]:
    body, request = _json_response(client, "GET", "/readyz")
    if body is None:
        return request
    if body.get("status") != "ready":
        return {**request, "ok": False, "reason": "seed_unavailable"}
    return request


def _runtime(client: httpx.Client) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    body, request = _json_response(client, "GET", "/api/runtime/status")
    if body is None:
        return None, request
    operational = body.get("operational")
    if not isinstance(operational, dict) or any(
        not _count(operational.get(name)) for name in ("lease_expired", "failed_unresolved")
    ):
        return None, {**request, "ok": False, "reason": "invalid_runtime_status"}
    checked_at = body.get("checked_at")
    if not isinstance(checked_at, str):
        return None, {**request, "ok": False, "reason": "invalid_runtime_status"}
    try:
        checked = datetime.fromisoformat(checked_at)
    except ValueError:
        return None, {**request, "ok": False, "reason": "invalid_runtime_status"}
    if checked.tzinfo is None:
        return None, {**request, "ok": False, "reason": "invalid_runtime_status"}
    if body.get("adapter") != "postgresql":
        return body, {**request, "ok": False, "reason": "unexpected_runtime_adapter"}
    if operational["lease_expired"] or operational["failed_unresolved"]:
        return body, {
            **request,
            "ok": False,
            "reason": "runtime_unresolved_work",
            "lease_expired": operational["lease_expired"],
            "failed_unresolved": operational["failed_unresolved"],
        }
    return body, request


def _source_watch(body: dict[str, Any] | None) -> dict[str, Any]:
    if body is None:
        return _failure("runtime_unavailable")
    value = body.get("source_watch")
    if not isinstance(value, dict) or value.get("reason") not in _SOURCE_REASONS:
        return _failure("source_watch_missing_or_invalid")
    state = value["reason"]
    return {
        "ok": state == "fresh",
        "reason": "ok" if state == "fresh" else "source_not_fresh",
        "state": state,
    }


def _capture_health(body: dict[str, Any] | None) -> dict[str, Any]:
    if body is None:
        return _failure("runtime_unavailable")
    value = body.get("capture_health")
    if not isinstance(value, dict) or value.get("reason") not in _CAPTURE_REASONS:
        return _failure("capture_health_missing_or_invalid")
    state = value["reason"]
    return {
        "ok": state == "caught_up_at_observation",
        "reason": "ok" if state == "caught_up_at_observation" else "capture_not_current",
        "state": state,
    }


def _publication(body: dict[str, Any] | None) -> dict[str, Any]:
    if body is None:
        return _failure("runtime_unavailable")
    value = body.get("source_publication")
    if not isinstance(value, dict):
        return _failure("publication_missing_or_invalid")
    names = ("accepted", "broker_acknowledged", "business_admitted", "pending_admission")
    if any(not _count(value.get(name)) for name in names):
        return _failure("publication_missing_or_invalid")
    accepted, acknowledged, admitted, pending = (value[name] for name in names)
    if not (0 <= admitted <= acknowledged <= accepted and pending == accepted - admitted):
        return _failure("publication_inconsistent")
    ack_lag = accepted - acknowledged
    admission_lag = acknowledged - admitted
    return {
        "ok": pending == 0,
        "reason": "ok" if pending == 0 else "publication_pending",
        "pending_admission": pending,
        "broker_ack_lag": ack_lag,
        "business_admission_lag": admission_lag,
    }


def _overview(client: httpx.Client) -> dict[str, Any]:
    body, request = _json_response(client, "GET", "/api/overview")
    if body is None:
        return request
    summary = body.get("summary")
    generation = body.get("generation_seed_sha256")
    if (
        not isinstance(summary, dict)
        or not _count(summary.get("total"))
        or not isinstance(generation, str)
        or not _SHA.fullmatch(generation)
    ):
        return {**request, "ok": False, "reason": "invalid_overview"}
    if summary["total"] == 0:
        return {**request, "ok": False, "reason": "overview_empty", "recommendation_count": 0}
    return {**request, "recommendation_count": summary["total"]}


def _recommendations(client: httpx.Client) -> dict[str, Any]:
    body, request = _json_response(client, "GET", "/api/recommendations")
    if body is None:
        return request
    items = body.get("items")
    total = body.get("total")
    if not isinstance(items, list) or not _count(total) or total != len(items):
        return {**request, "ok": False, "reason": "invalid_recommendations"}
    if total == 0:
        return {
            **request,
            "ok": False,
            "reason": "recommendations_empty",
            "recommendation_count": 0,
        }
    return {**request, "recommendation_count": total}


def probe_once(client: httpx.Client) -> dict[str, Any]:
    """Take one complete sample; endpoint failures do not suppress other GETs."""
    started = time.monotonic()
    started_at = _utc_now()
    checks: dict[str, dict[str, Any]] = {
        "session": _session(client),
        "seed": _seed(client),
    }
    status, checks["runtime"] = _runtime(client)
    checks["source_watch"] = _source_watch(status)
    checks["capture_health"] = _capture_health(status)
    checks["source_publication"] = _publication(status)
    checks["overview"] = _overview(client)
    checks["recommendations"] = _recommendations(client)
    return {
        "started_at": started_at,
        "elapsed_ms": _milliseconds(started),
        "ready": all(check["ok"] for check in checks.values()),
        "checks": checks,
    }


def collect(
    client: httpx.Client,
    *,
    base_url: str,
    samples: int | None,
    duration_seconds: float | None,
    interval_seconds: float,
    timeout_seconds: float,
) -> dict[str, Any]:
    if samples is None and duration_seconds is None:
        samples = 1
    if samples is not None and not 1 <= samples <= 100_000:
        raise ValueError("samples must be between 1 and 100000")
    if duration_seconds is not None and (
        not math.isfinite(duration_seconds) or not 0 < duration_seconds <= 86_400
    ):
        raise ValueError("duration must be finite and at most 86400 seconds")
    if not math.isfinite(interval_seconds) or not 0.1 <= interval_seconds <= 3600:
        raise ValueError("interval must be between 0.1 and 3600 seconds")
    if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 60:
        raise ValueError("timeout must be finite and at most 60 seconds")
    start = time.monotonic()
    began = _utc_now()
    login = _login(client)
    observed: list[dict[str, Any]] = []
    while (samples is None or len(observed) < samples) and (
        duration_seconds is None or not observed or time.monotonic() - start < duration_seconds
    ):
        item = probe_once(client)
        item["index"] = len(observed) + 1
        item["ready"] = bool(login["ok"] and item["ready"])
        observed.append(item)
        if samples is not None and len(observed) >= samples:
            break
        remaining = (
            None if duration_seconds is None else duration_seconds - (time.monotonic() - start)
        )
        if remaining is not None and remaining <= 0:
            break
        time.sleep(interval_seconds if remaining is None else min(interval_seconds, remaining))
    reasons = Counter(
        check["reason"] for item in observed for check in item["checks"].values() if not check["ok"]
    )
    if not login["ok"]:
        reasons["login_" + login["reason"]] += 1
    ready_count = sum(item["ready"] for item in observed)
    return {
        "contract": CONTRACT,
        "target": base_url,
        "started_at": began,
        "finished_at": _utc_now(),
        "elapsed_seconds": round(time.monotonic() - start, 3),
        "configuration": {
            "requested_samples": samples,
            "duration_seconds": duration_seconds,
            "interval_seconds": interval_seconds,
            "timeout_seconds": timeout_seconds,
        },
        "login": login,
        "summary": {
            "sample_count": len(observed),
            "ready_samples": ready_count,
            "failed_samples": len(observed) - ready_count,
            "reason_counts": dict(sorted(reasons.items())),
            "component_ready": bool(observed and ready_count == len(observed)),
        },
        "samples": observed,
        "unverified_dimensions": list(UNVERIFIED),
        "claim": "bounded_api_component_observation_not_full_ops_readiness",
    }


def _base_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or not parsed.port
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("base URL must be http://127.0.0.1:<port> with no credentials or path")
    return f"http://127.0.0.1:{parsed.port}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--samples", type=int)
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--output", type=Path, required=True, help="new JSON receipt path")
    args = parser.parse_args(argv)
    try:
        base_url = _base_url(args.base_url)
        if args.output.exists():
            raise ValueError("output already exists")
        with httpx.Client(
            base_url=base_url, timeout=args.timeout_seconds, trust_env=False, follow_redirects=False
        ) as client:
            receipt = collect(
                client,
                base_url=base_url,
                samples=args.samples,
                duration_seconds=args.duration_seconds,
                interval_seconds=args.interval_seconds,
                timeout_seconds=args.timeout_seconds,
            )
        with args.output.open("x", encoding="utf-8") as output:
            json.dump(receipt, output, indent=2, sort_keys=True, allow_nan=False)
            output.write("\n")
    except (OSError, ValueError) as error:
        parser.error(str(error))
    ready = receipt["summary"]["ready_samples"]
    total = receipt["summary"]["sample_count"]
    print(f"{ready}/{total} API component samples ready; receipt: {args.output}")
    return 0 if receipt["summary"]["component_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
