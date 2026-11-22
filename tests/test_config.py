from pathlib import Path

import pytest

from nexus.config import NexusSettings, load_project_config, load_yaml


def test_project_contract_has_weekly_sku_location_grain() -> None:
    config = load_project_config()
    assert config["decision"]["grain"] == "sku-location-week"
    assert config["decision"]["maker_checker_required"] is True


def test_settings_use_documented_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("NEXUS_ENV", "NEXUS_DATA_PROFILE", "NEXUS_LOG_LEVEL"):
        monkeypatch.delenv(key, raising=False)
    settings = NexusSettings.from_environment()
    assert settings.environment == "dev"
    assert settings.data_profile == "smoke"


def test_load_yaml_rejects_non_mapping(tmp_path: Path) -> None:
    path = tmp_path / "invalid.yaml"
    path.write_text("- item\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Expected a YAML mapping"):
        load_yaml(path)
