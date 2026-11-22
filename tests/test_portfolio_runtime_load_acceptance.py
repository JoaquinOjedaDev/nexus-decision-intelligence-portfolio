"""Local load coordinator correctness; artificial timings are not performance evidence."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jwt")
from nexus.portfolio import runtime_load_acceptance as load
from nexus.portfolio.runtime_control import RuntimeControl
from nexus.portfolio.store import PortfolioStore


@pytest.fixture
def prepared(tmp_path: Path) -> PortfolioStore:
    store = PortfolioStore(tmp_path, clock=lambda: 1_900_000_000.0)
    store.ensure_seed(
        {
            "metadata": {
                "pipeline_mode": "connected_runtime_baseline",
                "source_snapshot_sha256": "a" * 64,
            },
            "executable": True,
            "recommendations": [
                {
                    "id": f"rec-{index}",
                    "sku_id": f"SKU-{index}",
                    "location_id": "NORTH",
                    "created_by": "planner.r4a",
                    "recommended_quantity": 10,
                    "expected_cost": 50,
                }
                for index in range(4)
            ],
            "metrics": {},
            "scenarios": [],
            "lineage": [],
        }
    )
    RuntimeControl(store)
    return store


def run(
    prepared: PortfolioStore, monkeypatch: pytest.MonkeyPatch, latency: float
) -> dict[str, Any]:
    actual = load._measure

    def measure(*args: Any, **kwargs: Any) -> dict[str, Any]:
        sample = actual(*args, **kwargs)
        sample["latency_ms"] = latency
        return sample

    monkeypatch.setattr(load, "_measure", measure)
    return load.run_runtime_load_acceptance(
        Path(__file__).resolve().parents[1], prepared, allow_mutation=True
    )


def test_explicit_isolation_mutation_authority_is_required(prepared: PortfolioStore) -> None:
    before = prepared.metrics_snapshot()
    with pytest.raises(ValueError, match="allow_mutation=True"):
        load.run_runtime_load_acceptance(Path("."), prepared)
    assert prepared.metrics_snapshot()["event_count"] == before["event_count"]
    assert prepared.metrics_snapshot()["effect_count"] == 0


@pytest.mark.parametrize("repetitions", [1, 2, True, 11])
def test_cannot_relax_predeclared_repetition_minimum(
    prepared: PortfolioStore, repetitions: int
) -> None:
    with pytest.raises(ValueError, match="3 through 10"):
        load.run_runtime_load_acceptance(
            Path("."), prepared, allow_mutation=True, repetitions=repetitions
        )
    assert prepared.metrics_snapshot()["effect_count"] == 0


def test_profile_preserves_raws_and_one_effect_across_clients_and_reopen(
    prepared: PortfolioStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    receipt = run(prepared, monkeypatch, 10)
    assert receipt["status"] == "passed", receipt["checks"]
    assert len(receipt["warmup"]) == 3
    assert len(receipt["repetitions"]) == 3
    assert all(len(result["samples"]) == 48 for result in receipt["repetitions"])
    assert all(
        sum(sample["operation"] == "execute" for sample in result["samples"]) == 24
        for result in receipt["repetitions"]
    )
    assert receipt["p95_api_ms"] == 10
    assert receipt["profile"]["p95_api_limit_ms"] == 500
    assert receipt["memory"]["python_traced_peak_bytes"] > 0
    assert receipt["recovery"]["logical_reopen_verified"]
    assert receipt["recovery"]["backup_restore"] == "not_exercised_no_established_store_interface"
    assert receipt["binding"]["source_sha256"] == "a" * 64
    assert len(receipt["binding"]["code_files"]) == 7
    assert prepared.metrics_snapshot()["effect_count"] == 1
    assert receipt["scope"]["original_G7_gate_closed"] is False
    assert receipt["scope"]["paid_services"] is False


def test_slow_measurements_fail_without_relaxing_threshold(
    prepared: PortfolioStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    receipt = run(prepared, monkeypatch, 501)
    assert receipt["status"] == "failed"
    assert receipt["checks"]["p95_local_profile"] is False
    assert receipt["checks"]["requests_ok"]
    assert receipt["checks"]["one_additional_mock_effect"]
    assert receipt["p95_api_ms"] == 501


def test_nearest_rank_tail_is_not_mean_latency() -> None:
    assert load._p95([1] * 18 + [900, 1000]) == 900
    with pytest.raises(ValueError):
        load._p95([])


def test_profile_does_not_accept_wrong_pipeline_or_disturb_existing_trace(
    prepared: PortfolioStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(load.tracemalloc, "is_tracing", lambda: True)
    with pytest.raises(ValueError, match="allocation trace"):
        load.run_runtime_load_acceptance(Path("."), prepared, allow_mutation=True)
    monkeypatch.setattr(load.tracemalloc, "is_tracing", lambda: False)
    original = prepared.overview_snapshot

    def wrong_pipeline(actor: Any) -> dict[str, Any]:
        snapshot = original(actor)
        snapshot["metadata"]["pipeline_mode"] = "frozen"
        return snapshot

    monkeypatch.setattr(prepared, "overview_snapshot", wrong_pipeline)
    with pytest.raises(ValueError, match="four connected runtime keys"):
        load.run_runtime_load_acceptance(Path("."), prepared, allow_mutation=True)
    assert prepared.metrics_snapshot()["effect_count"] == 0
