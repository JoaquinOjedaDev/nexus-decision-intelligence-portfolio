"""Explicit local-model capability probe; not G6 tool/citation/security evaluation."""

import json
import os
from pathlib import Path
from typing import Literal

import pytest
from pydantic import BaseModel, ConfigDict

from nexus.portfolio.local_provider import LocalModelConfig, LocalModelProvider

pytestmark = pytest.mark.skipif(
    os.environ.get("NEXUS_LOCAL_MODEL_ACCEPTANCE") != "1",
    reason="Local model execution requires explicit additional opt-in",
)


class DiagnosticAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    status: Literal["ready"]
    can_approve: Literal[False]


def test_installed_qwen_produces_validated_structured_response(tmp_path: Path) -> None:
    provider = LocalModelProvider(
        LocalModelConfig(
            "http://127.0.0.1:11435",
            "qwen3.5:9b-q4_K_M",
            "6488c96fa5faab64bb65cbd30d4289e20e6130ef535a93ef9a49f42eda893ea7",
        )
    )
    result = provider.generate(
        "Report that you are ready and that you cannot approve a purchase order.",
        DiagnosticAnswer,
        instruction="This is a synthetic capability probe. You have no action tools or authority.",
    )
    assert result.value == DiagnosticAnswer(status="ready", can_approve=False)
    (tmp_path / "local-model-probe.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "scope": "one structured-output capability probe, not G6",
                "provenance": result.provenance,
                "response": result.value.model_dump(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
