"""Optional local-only inference boundary; no tools, writes, pulls or cloud fallback."""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from typing import Any, TypeVar
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)


class LocalProviderError(RuntimeError):
    """Safe failure code, without prompts, model output or transport diagnostics."""


@dataclass(frozen=True)
class LocalModelConfig:
    endpoint: str
    model: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        parsed = urlsplit(self.endpoint)
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.port is None
            or not 1024 < parsed.port < 65536
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("An explicit loopback-only model endpoint is required")
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}:[a-zA-Z0-9._-]{1,64}", self.model)
            or "cloud" in self.model.lower()
            or not re.fullmatch(r"[0-9a-f]{64}", self.manifest_sha256)
        ):
            raise ValueError("Pin an installed local model tag and manifest SHA-256")


@dataclass(frozen=True)
class LocalReply:
    value: BaseModel
    provenance: dict[str, Any]


class LocalModelProvider:
    """Fail-closed structured response; callers must still authorize every tool.

    Uses the installed model only. Server startup is a separate explicit operation
    with cloud disabled. A model is never pulled, changed, or substituted here.
    No raw prompt or generated content is included in provenance or errors.
    """

    def __init__(self, config: LocalModelConfig) -> None:
        self.config = config

    @staticmethod
    def _json(client: httpx.Client, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        with client.stream(method, path, **kwargs) as response:
            response.raise_for_status()
            content = bytearray()
            for chunk in response.iter_bytes():
                content.extend(chunk)
                if len(content) > 131072:
                    raise LocalProviderError("provider_response_too_large")
        value = json.loads(content)
        if not isinstance(value, dict):
            raise LocalProviderError("provider_response_invalid")
        return value

    def _verify_installed(self, client: httpx.Client) -> None:
        models = self._json(client, "GET", "/api/tags").get("models")
        if not isinstance(models, list):
            raise LocalProviderError("provider_inventory_invalid")
        matches = [m for m in models if isinstance(m, dict) and m.get("name") == self.config.model]
        if len(matches) != 1 or matches[0].get("digest") != self.config.manifest_sha256:
            raise LocalProviderError("provider_model_identity_mismatch")
        if type(matches[0].get("size")) is not int or matches[0]["size"] <= 0:
            raise LocalProviderError("provider_model_not_local")

    def generate(self, question: str, response_type: type[T], *, instruction: str) -> LocalReply:
        schema = response_type.model_json_schema()
        if response_type.model_config.get("extra") != "forbid":
            raise ValueError("Response contracts must forbid extra fields")
        schema_text = json.dumps(schema, sort_keys=True, separators=(",", ":"))
        system = (
            instruction + "\nReturn only JSON conforming exactly to this schema:\n" + schema_text
        )
        # Conservative byte envelope leaves room for tokenizer/template overhead and output.
        if (
            not isinstance(question, str)
            or not question.strip()
            or len((system + question).encode()) > 3072
        ):
            raise LocalProviderError("provider_input_budget_exceeded")
        payload = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": question},
            ],
            "format": schema,
            "stream": False,
            "think": False,
            "keep_alive": 0,
            "options": {"temperature": 0, "num_ctx": 4096, "num_predict": 256},
        }
        started = time.perf_counter()
        try:
            with httpx.Client(
                base_url=self.config.endpoint,
                trust_env=False,
                follow_redirects=False,
                timeout=httpx.Timeout(120, connect=2),
            ) as client:
                self._verify_installed(client)
                result = self._json(client, "POST", "/api/chat", json=payload)
                self._verify_installed(client)
            if (
                result.get("model") != self.config.model
                or result.get("done") is not True
                or result.get("done_reason") != "stop"
            ):
                raise LocalProviderError("provider_incomplete_response")
            for name, maximum in (("prompt_eval_count", 3840), ("eval_count", 256)):
                if type(result.get(name)) is not int or not 0 < result[name] <= maximum:
                    raise LocalProviderError("provider_token_budget_invalid")
            message = result.get("message")
            if not isinstance(message, dict) or message.get("role") != "assistant":
                raise LocalProviderError("provider_message_invalid")
            content = message.get("content")
            if not isinstance(content, str) or len(content.encode()) > 8192:
                raise LocalProviderError("provider_content_invalid")
            value = response_type.model_validate_json(content, strict=True)
        except (httpx.HTTPError, OSError):
            raise LocalProviderError("provider_unavailable") from None
        except (ValueError, TypeError, ValidationError):
            raise LocalProviderError("provider_schema_invalid") from None
        return LocalReply(
            value,
            {
                "provider": "ollama-local",
                "model": self.config.model,
                "manifest_sha256": self.config.manifest_sha256,
                "schema_sha256": hashlib.sha256(schema_text.encode()).hexdigest(),
                "prompt_sha256": hashlib.sha256(
                    json.dumps(payload, sort_keys=True).encode()
                ).hexdigest(),
                "elapsed_seconds": time.perf_counter() - started,
                "prompt_tokens": result["prompt_eval_count"],
                "output_tokens": result["eval_count"],
                "new_paid_provider_usage": False,
                "claim": "validated_structured_inference_not_authorization",
            },
        )
