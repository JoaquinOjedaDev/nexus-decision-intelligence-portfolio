"""Mock-HTTP coverage for the bounded local ops component probe."""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from scripts.ops_profile_probe import _base_url, collect

BASE = "http://127.0.0.1:8767"


def status() -> dict[str, Any]:
    return {
        "operational": {"lease_expired": 0, "failed_unresolved": 0},
        "adapter": "postgresql",
        "checked_at": datetime.now(UTC).isoformat(),
        "source_watch": {"reason": "fresh"},
        "capture_health": {"reason": "caught_up_at_observation"},
        "source_publication": {
            "accepted": 2,
            "broker_acknowledged": 2,
            "business_admitted": 2,
            "pending_admission": 0,
        },
    }


def client_for(
    *,
    runtime: dict[str, Any] | None = None,
    overrides: Mapping[str, httpx.Response | Exception] | None = None,
) -> tuple[httpx.Client, list[tuple[str, str]]]:
    calls: list[tuple[str, str]] = []
    payload = copy.deepcopy(runtime if runtime is not None else status())
    changes = overrides or {}

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        path = request.url.path
        if path == "/api/session" and request.method == "POST":
            assert json.loads(request.content) == {"persona": "auditor"}
            return httpx.Response(
                200,
                json={"user": {"role": "auditor"}, "csrf_token": "private-csrf"},
                headers={"set-cookie": "nexus_portfolio_session=private-cookie; HttpOnly; Path=/"},
            )
        if path in changes:
            changed = changes[path]
            if isinstance(changed, Exception):
                raise changed
            return changed
        if (
            path != "/readyz"
            and "nexus_portfolio_session=private-cookie" not in request.headers.get("cookie", "")
        ):
            return httpx.Response(401, json={"error": {"code": "authentication_required"}})
        if path == "/api/session":
            return httpx.Response(
                200, json={"user": {"role": "auditor"}, "csrf_token": "private-csrf"}
            )
        if path == "/readyz":
            return httpx.Response(200, json={"status": "ready"})
        if path == "/api/runtime/status":
            return httpx.Response(200, json=payload)
        if path == "/api/overview":
            return httpx.Response(
                200, json={"summary": {"total": 1}, "generation_seed_sha256": "a" * 64}
            )
        if path == "/api/recommendations":
            return httpx.Response(200, json={"items": [{"id": "redacted-in-receipt"}], "total": 1})
        raise AssertionError(f"Unexpected endpoint: {path}")

    return httpx.Client(
        base_url=BASE, transport=httpx.MockTransport(handler), trust_env=False
    ), calls


def run(client: httpx.Client, *, samples: int = 1) -> dict[str, Any]:
    with client:
        return collect(
            client,
            base_url=BASE,
            samples=samples,
            duration_seconds=None,
            interval_seconds=0.1,
            timeout_seconds=1,
        )


def test_ready_sample_uses_only_session_post_and_scoped_gets() -> None:
    client, calls = client_for()
    receipt = run(client)
    assert receipt["summary"]["component_ready"] is True
    assert receipt["summary"]["sample_count"] == 1
    assert receipt["summary"]["ready_samples"] == 1
    assert calls == [
        ("POST", "/api/session"),
        ("GET", "/api/session"),
        ("GET", "/readyz"),
        ("GET", "/api/runtime/status"),
        ("GET", "/api/overview"),
        ("GET", "/api/recommendations"),
    ]
    encoded = json.dumps(receipt)
    assert "private-cookie" not in encoded
    assert "private-csrf" not in encoded
    assert "redacted-in-receipt" not in encoded
    assert "seven_worker_process_supervision" in receipt["unverified_dimensions"]


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"source_watch": None}, "source_watch_missing_or_invalid"),
        ({"source_watch": {"reason": "source_observation_expired"}}, "source_not_fresh"),
        ({"capture_health": None}, "capture_health_missing_or_invalid"),
        ({"capture_health": {"reason": "capture_lagging"}}, "capture_not_current"),
        (
            {
                "source_publication": {
                    "accepted": 3,
                    "broker_acknowledged": 2,
                    "business_admitted": 1,
                    "pending_admission": 2,
                }
            },
            "publication_pending",
        ),
        ({"operational": {"lease_expired": 1, "failed_unresolved": 0}}, "runtime_unresolved_work"),
    ],
)
def test_missing_stale_or_lagging_status_fails_closed(
    change: dict[str, Any], expected: str
) -> None:
    runtime = status()
    runtime.update(change)
    client, _ = client_for(runtime=runtime)
    receipt = run(client)
    assert receipt["summary"]["failed_samples"] == 1
    assert expected in receipt["summary"]["reason_counts"]
    assert receipt["summary"]["component_ready"] is False


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"/api/runtime/status": httpx.Response(200, text="not json")}, "malformed_json"),
        ({"/api/runtime/status": httpx.Response(401, json={"detail": "secret"})}, "http_401"),
        ({"/api/overview": httpx.Response(200, json={"summary": "bad"})}, "invalid_overview"),
        ({"/api/recommendations": httpx.Response(403, json={"detail": "secret"})}, "http_403"),
    ],
)
def test_malformed_and_unauthorized_responses_are_counted_without_body_leak(
    overrides: dict[str, httpx.Response], expected: str
) -> None:
    client, _ = client_for(overrides=overrides)
    receipt = run(client)
    assert receipt["summary"]["failed_samples"] == 1
    assert expected in receipt["summary"]["reason_counts"]
    assert "secret" not in json.dumps(receipt)


def test_timeout_is_counted_and_other_reads_continue() -> None:
    client, calls = client_for(
        overrides={"/api/runtime/status": httpx.ReadTimeout("private timeout detail")}
    )
    receipt = run(client)
    assert receipt["summary"]["reason_counts"]["timeout"] == 1
    assert receipt["summary"]["failed_samples"] == 1
    assert ("GET", "/api/recommendations") in calls
    assert "private timeout detail" not in json.dumps(receipt)


def test_two_samples_retain_a_failed_later_observation() -> None:
    calls: list[tuple[str, str]] = []
    count = 0
    good, _ = client_for()
    original = good._transport

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal count
        calls.append((request.method, request.url.path))
        if request.url.path == "/api/runtime/status":
            count += 1
            if count == 2:
                return httpx.Response(503, json={"detail": "private server detail"})
        return original.handle_request(request)

    client = httpx.Client(base_url=BASE, transport=httpx.MockTransport(handler), trust_env=False)
    receipt = run(client, samples=2)
    good.close()
    assert receipt["summary"]["sample_count"] == 2
    assert receipt["summary"]["ready_samples"] == 1
    assert receipt["summary"]["failed_samples"] == 1
    assert receipt["samples"][1]["checks"]["runtime"]["reason"] == "http_503"
    assert "private server detail" not in json.dumps(receipt)
    assert len([call for call in calls if call == ("GET", "/api/runtime/status")]) == 2


def test_remote_or_credentialed_target_is_rejected() -> None:
    assert _base_url(BASE) == BASE
    for value in (
        "https://127.0.0.1:8767",
        "http://example.com:8767",
        "http://user:pass@127.0.0.1:8767",
        "http://127.0.0.1:8767/api",
    ):
        with pytest.raises(ValueError):
            _base_url(value)
