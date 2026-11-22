"""Configuration loading with repository-safe defaults."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class NexusSettings(BaseModel):
    """Runtime settings derived from environment variables."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    environment: str = Field(default="dev", pattern=r"^(dev|ci|demo|release)$")
    data_profile: str = Field(default="smoke", pattern=r"^(smoke|demo|full)$")
    log_level: str = Field(default="INFO", pattern=r"^(DEBUG|INFO|WARNING|ERROR)$")
    artifact_dir: Path = Path("artifacts")

    @classmethod
    def from_environment(cls) -> NexusSettings:
        """Load only documented NEXUS variables; secrets stay outside this model."""

        return cls(
            environment=os.getenv("NEXUS_ENV", "dev").lower(),
            data_profile=os.getenv("NEXUS_DATA_PROFILE", "smoke").lower(),
            log_level=os.getenv("NEXUS_LOG_LEVEL", "INFO").upper(),
            artifact_dir=Path(os.getenv("NEXUS_ARTIFACT_DIR", "artifacts")),
        )


def load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML mapping and reject empty or non-mapping documents."""

    with path.open(encoding="utf-8") as handle:
        value = yaml.safe_load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return value


def load_project_config(root: Path = PROJECT_ROOT) -> dict[str, Any]:
    """Load the versioned project decision contract."""

    return load_yaml(root / "configs" / "project.yaml")
