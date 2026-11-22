from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_dbt_project_has_staging_gold_and_reconciliation() -> None:
    expected = [
        ROOT / "dbt" / "models" / "staging" / "stg_inventory_adjustments.sql",
        ROOT / "dbt" / "models" / "gold" / "fct_inventory_position.sql",
        ROOT / "dbt" / "tests" / "assert_inventory_reconciliation.sql",
        ROOT / "dbt" / "tests" / "assert_inventory_nonnegative.sql",
    ]
    assert all(path.exists() for path in expected)


def test_compose_uses_profiles_pins_and_local_ports() -> None:
    compose = yaml.safe_load((ROOT / "infra" / "compose.yaml").read_text(encoding="utf-8"))
    services = compose["services"]
    assert {"postgres", "redpanda", "prometheus", "grafana"} <= services.keys()
    for service in services.values():
        assert service["profiles"]
        assert not service["image"].endswith(":latest")
        for port in service.get("ports", []):
            assert str(port).startswith("127.0.0.1:")


def test_archived_minio_is_not_a_runtime_dependency() -> None:
    compose_text = (ROOT / "infra" / "compose.yaml").read_text(encoding="utf-8").lower()
    assert "minio" not in compose_text
