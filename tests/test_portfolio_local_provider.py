"""Provider contract tests use an HTTP double, never cloud or local model inference."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Literal

import httpx
import pytest
from pydantic import BaseModel, ConfigDict

from nexus.portfolio.local_provider import LocalModelConfig, LocalModelProvider, LocalProviderError


class Answer(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    status: Literal["ready"]
    can_approve: Literal[False]


def config() -> LocalModelConfig:
    return LocalModelConfig("http://127.0.0.1:11435", "qwen3.5:9b-q4_K_M", "a" * 64)


def install_http(
    monkeypatch: pytest.MonkeyPatch,
    *,
    change: dict | None = None,
    status: int = 200,
    changed_tag: bool = False,
) -> list[httpx.Request]:
    original = httpx.Client
    calls: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/api/tags":
            digest = "b" * 64 if changed_tag and len(calls) > 1 else "a" * 64
            return httpx.Response(
                200, json={"models": [{"name": config().model, "digest": digest, "size": 1234}]}
            )
        result = {
            "model": config().model,
            "done": True,
            "done_reason": "stop",
            "prompt_eval_count": 100,
            "eval_count": 20,
            "message": {"role": "assistant", "content": '{"status":"ready","can_approve":false}'},
        }
        result.update(change or {})
        return httpx.Response(status, json=result)

    def client(**kwargs: object) -> httpx.Client:
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        return original(transport=httpx.MockTransport(respond), **kwargs)

    monkeypatch.setattr("nexus.portfolio.local_provider.httpx.Client", client)
    return calls


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://cloud.example",
        "http://localhost:11434",
        "http://127.0.0.1:11435/path",
        "http://user:password@127.0.0.1:11435",
        "http://127.0.0.1:11435?proxy=x",
        "http://127.0.0.1:80",
    ],
)
def test_only_explicit_loopback_endpoint_is_accepted(endpoint: str) -> None:
    with pytest.raises(ValueError):
        replace(config(), endpoint=endpoint)


@pytest.mark.parametrize(
    "field,value",
    [
        ("model", "qwen3.5:cloud"),
        ("model", "remote/model:tag"),
        ("model", "qwen3.5"),
        ("manifest_sha256", "bad"),
    ],
)
def test_no_cloud_tag_implicit_tag_or_unpinned_model(field: str, value: str) -> None:
    with pytest.raises(ValueError):
        replace(config(), **{field: value})


def test_valid_structured_output_has_bound_provenance_and_no_tool_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_http(monkeypatch)
    result = LocalModelProvider(config()).generate(
        "synthetic-private-question", Answer, instruction="Return a diagnostic response."
    )
    assert result.value == Answer(status="ready", can_approve=False)
    assert [r.url.path for r in calls] == ["/api/tags", "/api/chat", "/api/tags"]
    payload = json.loads(calls[1].content)
    assert payload["keep_alive"] == 0 and payload["stream"] is False
    assert payload["options"]["num_predict"] == 256
    assert "tools" not in payload
    assert "synthetic-private-question" not in repr(result.provenance)
    assert result.provenance["manifest_sha256"] == "a" * 64


@pytest.mark.parametrize(
    "change",
    [
        {"model": "wrong"},
        {"done": False},
        {"done_reason": "length"},
        {"eval_count": 257},
        {"prompt_eval_count": True},
        {"message": None},
        {"message": {"role": "assistant", "content": '{"status":"ready","can_approve":true}'}},
        {"message": {"role": "assistant", "content": '{"status":"ready","message":"extra field"}'}},
        {"message": {"role": "assistant", "content": "malformed"}},
    ],
)
def test_untrusted_malformed_or_overbudget_output_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    change: dict,
) -> None:
    install_http(monkeypatch, change=change)
    with pytest.raises(LocalProviderError):
        LocalModelProvider(config()).generate("diagnostic", Answer, instruction="Diagnostic only.")


@pytest.mark.parametrize("status", [302, 401, 500, 503])
def test_provider_failure_does_not_fall_back_or_leak_content(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    calls = install_http(monkeypatch, status=status)
    with pytest.raises(LocalProviderError, match=r"^provider_unavailable$"):
        LocalModelProvider(config()).generate("private-question", Answer, instruction="Diagnostic.")
    assert len(calls) == 2


def test_model_identity_change_during_response_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    install_http(monkeypatch, changed_tag=True)
    with pytest.raises(LocalProviderError, match="identity_mismatch"):
        LocalModelProvider(config()).generate("diagnostic", Answer, instruction="Diagnostic.")


def test_prompt_budget_refuses_before_http(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = install_http(monkeypatch)
    with pytest.raises(LocalProviderError, match="input_budget"):
        LocalModelProvider(config()).generate("x" * 3073, Answer, instruction="Diagnostic.")
    assert calls == []
