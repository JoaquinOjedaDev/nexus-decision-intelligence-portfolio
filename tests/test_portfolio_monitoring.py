"""Measured monitor contracts, mature-only statistics and durable state transitions."""

from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from nexus.portfolio.monitoring import (
    MonitorActual,
    MonitorBook,
    MonitorConfig,
    MonitorForecast,
    MonitorInput,
)

NOW = datetime(2026, 9, 12, 12, tzinfo=UTC)
DAY = date(2026, 9, 9)
ISSUED = datetime(2026, 9, 8, 6, tzinfo=UTC)
ARRIVED = datetime(2026, 9, 10, 1, tzinfo=UTC)
MATURE = datetime(2026, 9, 12, 0, tzinfo=UTC)
ARTIFACT = "a" * 64
EVIDENCE = "b" * 64


def _input(count: int = 500, covered: int = 400) -> MonitorInput:
    forecasts = tuple(
        MonitorForecast(
            sku_id=f"SKU-{index:05d}",
            location_id="LOC-1",
            issue_time=ISSUED,
            target_date=DAY,
            q025=Decimal(6),
            q10=Decimal(8),
            q50=Decimal(10),
            q90=Decimal(12),
            q975=Decimal(16),
            model_id="champion-v1",
            artifact_sha256=ARTIFACT,
        )
        for index in range(count)
    )
    actuals = tuple(
        MonitorActual(
            sku_id=forecast.sku_id,
            location_id=forecast.location_id,
            target_date=DAY,
            quantity=Decimal(10 if index < covered else 14),
            available_at=ARRIVED,
            mature_after=MATURE,
            correction_closed=True,
        )
        for index, forecast in enumerate(forecasts)
    )
    return MonitorInput(
        now=NOW,
        source_watermark=NOW - timedelta(hours=1),
        forecasts=forecasts,
        actuals=actuals,
        source_id="gold-demand",
        evidence_sha256=EVIDENCE,
    )


def _model(result: dict, model_id: str = "champion-v1") -> dict:
    return result["checks"][f"gold-demand:forecast:{model_id}"]


def _source(result: dict) -> dict:
    return result["checks"]["gold-demand:source_freshness"]


def test_expected_model_with_no_rows_is_insufficient_beside_healthy_model(tmp_path: Path) -> None:
    result = MonitorBook(tmp_path / "scoped.sqlite3").evaluate(
        replace(_input(), expected_model_ids=("champion-v1", "missing-model"))
    )
    assert _model(result)["status"] == "healthy"
    absent = _model(result, "missing-model")
    assert absent["status"] == "insufficient_evidence"
    assert absent["forecast_rows"] == absent["distinct_mature_outcomes"] == 0
    assert absent["metrics"] == {}
    assert result["status"] == "insufficient_evidence"
    legacy = MonitorBook(tmp_path / "legacy.sqlite3").evaluate(_input())
    empty_scope = MonitorBook(tmp_path / "empty-scope.sqlite3").evaluate(
        replace(_input(), expected_model_ids=())
    )
    assert legacy == empty_scope
    assert result["input_sha256"] != legacy["input_sha256"]


@pytest.mark.parametrize("model_ids", [("",), ("a", "a"), ["model"], (True,)])
def test_expected_model_scope_rejects_invalid_or_duplicate_ids(model_ids) -> None:
    with pytest.raises(ValueError):
        replace(_input(0), expected_model_ids=model_ids)


def test_empty_book_and_no_inputs_are_not_healthy(tmp_path: Path) -> None:
    book = MonitorBook(tmp_path / "measured.sqlite3")
    assert book.snapshot()["status"] == "unknown"
    result = book.evaluate(replace(_input(0, 0), source_watermark=None))
    assert result["status"] == "unknown"
    assert _source(result)["status"] == "unknown"
    assert result["checks"]["gold-demand:forecast:none"]["status"] == "insufficient_evidence"
    assert result["notifications_delivered"] == 0


def test_exactly_500_distinct_mature_outcomes_enables_metrics(tmp_path: Path) -> None:
    result = MonitorBook(tmp_path / "measured.sqlite3").evaluate(_input())
    assert result["status"] == "healthy"
    quality = _model(result)
    assert quality["distinct_mature_outcomes"] == 500
    assert quality["mature_forecast_rows"] == 500
    assert quality["metrics"]["coverage_80"] == "0.8"
    assert quality["metrics"]["bias_units"] == "-0.8"
    assert Decimal(quality["metrics"]["wape"]) == Decimal(400) / Decimal(5400)
    assert Decimal(quality["metrics"]["wis"]) > 0
    assert result["model_promotion"] == "never automatic; monitoring is evidence only"
    json.dumps(result, allow_nan=False)


def test_499_outcomes_are_insufficient_even_if_calibrated(tmp_path: Path) -> None:
    result = MonitorBook(tmp_path / "measured.sqlite3").evaluate(_input(499, 399))
    assert result["status"] == "insufficient_evidence"
    assert _model(result)["status"] == "insufficient_evidence"
    assert result["notifications_delivered"] == 0


@pytest.mark.parametrize(
    ("covered", "status"), [(374, "firing"), (375, "healthy"), (425, "healthy"), (426, "firing")]
)
def test_coverage_threshold_is_inclusive(tmp_path: Path, covered: int, status: str) -> None:
    result = MonitorBook(tmp_path / "measured.sqlite3").evaluate(_input(500, covered))
    assert _model(result)["status"] == status


def test_source_age_boundary_and_observation_age_are_separate(tmp_path: Path) -> None:
    book = MonitorBook(tmp_path / "measured.sqlite3")
    data = replace(_input(), source_watermark=NOW - timedelta(days=2))
    result = book.evaluate(data)
    assert _source(result)["status"] == "healthy"
    assert _source(result)["ingestion_age_seconds"] == "172800"
    assert _source(result)["latest_available_observation_date"] == "2026-09-09"
    assert _source(result)["observation_age_seconds"] == "216000"
    later = book.evaluate(replace(data, now=NOW + timedelta(microseconds=1)))
    assert _source(later)["status"] == "firing"
    assert later["notifications_delivered"] == 1


def test_missing_watermark_does_not_report_freshness_from_old_actuals(tmp_path: Path) -> None:
    result = MonitorBook(tmp_path / "measured.sqlite3").evaluate(
        replace(_input(), source_watermark=None)
    )
    assert result["status"] == "unknown"
    assert _model(result)["status"] == "healthy"
    assert _source(result)["ingestion_age_seconds"] is None
    assert _source(result)["observation_age_seconds"] is not None


def test_future_watermark_and_naive_evaluation_time_fail_closed() -> None:
    with pytest.raises(ValueError, match="future"):
        replace(_input(1), source_watermark=NOW + timedelta(microseconds=1))
    with pytest.raises(ValueError, match="timezone-aware"):
        replace(_input(1), now=NOW.replace(tzinfo=None))


def test_immature_late_open_and_missing_outcomes_are_excluded(tmp_path: Path) -> None:
    data = _input(504, 400)
    actuals = list(data.actuals)
    actuals[500] = replace(actuals[500], available_at=NOW + timedelta(seconds=1))
    actuals[501] = replace(actuals[501], mature_after=NOW + timedelta(seconds=1))
    actuals[502] = replace(actuals[502], correction_closed=False)
    actuals.pop()
    result = MonitorBook(tmp_path / "measured.sqlite3").evaluate(
        replace(data, actuals=tuple(actuals))
    )
    quality = _model(result)
    assert quality["status"] == "healthy"
    assert quality["distinct_mature_outcomes"] == 500
    assert quality["excluded_forecast_rows"] == {
        "actual_not_yet_available": 1,
        "maturity_deadline_not_reached": 1,
        "correction_window_open": 1,
        "missing_actual": 1,
    }


def test_maturity_and_availability_are_inclusive_at_cutoff(tmp_path: Path) -> None:
    data = _input()
    data = replace(
        data,
        actuals=tuple(replace(row, available_at=NOW, mature_after=NOW) for row in data.actuals),
    )
    assert MonitorBook(tmp_path / "measured.sqlite3").evaluate(data)["status"] == "healthy"


def test_outcome_day_must_be_complete_and_forecast_must_exist_at_cutoff(tmp_path: Path) -> None:
    data = _input(2)
    forecasts = (
        replace(
            data.forecasts[0], issue_time=NOW + timedelta(days=1), target_date=date(2026, 9, 14)
        ),
        replace(data.forecasts[1], target_date=NOW.date()),
    )
    actuals = (
        replace(
            data.actuals[0],
            target_date=date(2026, 9, 14),
            available_at=NOW + timedelta(days=4),
            mature_after=NOW + timedelta(days=5),
        ),
        replace(
            data.actuals[1],
            target_date=NOW.date(),
            available_at=NOW + timedelta(days=1),
            mature_after=NOW + timedelta(days=2),
        ),
    )
    result = MonitorBook(tmp_path / "measured.sqlite3").evaluate(
        replace(data, forecasts=forecasts, actuals=actuals)
    )
    assert _model(result)["excluded_forecast_rows"] == {
        "forecast_not_yet_issued": 1,
        "incomplete_outcome_day": 1,
    }
    assert _model(result)["metrics"] == {}


@pytest.mark.parametrize("conflicting", [False, True])
def test_duplicate_forecasts_and_actuals_fail_closed(conflicting: bool) -> None:
    data = _input(1)
    forecast = replace(data.forecasts[0], q50=Decimal(11)) if conflicting else data.forecasts[0]
    actual = replace(data.actuals[0], quantity=Decimal(100)) if conflicting else data.actuals[0]
    with pytest.raises(ValueError, match="duplicate forecasts"):
        replace(data, forecasts=(*data.forecasts, forecast))
    with pytest.raises(ValueError, match="duplicate actuals"):
        replace(data, actuals=(*data.actuals, actual))


def test_repeated_origins_do_not_inflate_evidence_minimum(tmp_path: Path) -> None:
    data = _input(499, 399)
    forecasts = (*data.forecasts, replace(data.forecasts[0], issue_time=ISSUED - timedelta(days=1)))
    result = MonitorBook(tmp_path / "measured.sqlite3").evaluate(replace(data, forecasts=forecasts))
    assert _model(result)["mature_forecast_rows"] == 500
    assert _model(result)["distinct_mature_outcomes"] == 499
    assert _model(result)["status"] == "insufficient_evidence"


def test_model_versions_are_evaluated_separately(tmp_path: Path) -> None:
    data = _input()
    challenger = tuple(
        replace(row, model_id="challenger-v2", q90=Decimal(15)) for row in data.forecasts
    )
    result = MonitorBook(tmp_path / "measured.sqlite3").evaluate(
        replace(data, forecasts=(*data.forecasts, *challenger))
    )
    assert _model(result)["status"] == "healthy"
    assert _model(result, "challenger-v2")["status"] == "firing"
    assert _model(result, "challenger-v2")["metrics"]["coverage_80"] == "1"
    assert result["notifications_delivered"] == 1


def test_journal_deduplicates_tick_reopens_and_resolves(tmp_path: Path) -> None:
    path = tmp_path / "measured.sqlite3"
    book = MonitorBook(path)
    breach = _input(500, 300)
    first = book.evaluate(breach)
    assert first["status"] == "firing"
    assert first["evaluation_count"] == 1
    assert first["notifications_delivered"] == 1
    assert book.evaluate(breach) == first
    later = book.evaluate(replace(breach, now=NOW + timedelta(seconds=1)))
    assert later["evaluation_count"] == 2
    assert later["notifications_delivered"] == 1
    reopened = MonitorBook(path)
    assert reopened.snapshot() == later
    recovered = reopened.evaluate(replace(_input(), now=NOW + timedelta(seconds=2)))
    assert recovered["status"] == "healthy"
    assert recovered["active_alerts"] == []
    assert recovered["notifications_delivered"] == 2
    assert [row["transition"] for row in recovered["notifications"]] == ["resolved", "firing"]
    assert all(len(row["input_sha256"]) == 64 for row in recovered["notifications"])
    assert all(row["evidence_sha256"] == EVIDENCE for row in recovered["notifications"])
    again = reopened.evaluate(replace(breach, now=NOW + timedelta(seconds=3)))
    assert again["notifications_delivered"] == 3
    assert len(again["active_alerts"]) == 1


def test_unknown_or_insufficient_evidence_cannot_resolve_an_alert(tmp_path: Path) -> None:
    book = MonitorBook(tmp_path / "measured.sqlite3")
    breach = replace(_input(500, 300), source_watermark=NOW - timedelta(days=3))
    assert book.evaluate(breach)["notifications_delivered"] == 2
    unclear = replace(_input(1), now=NOW + timedelta(seconds=1), source_watermark=None)
    result = book.evaluate(unclear)
    assert _source(result)["status"] == "unknown"
    assert _model(result)["status"] == "insufficient_evidence"
    assert result["status"] == "firing"
    assert len(result["active_alerts"]) == 2
    assert result["notifications_delivered"] == 2


def test_replaying_old_same_timestamp_evaluation_cannot_reopen_resolved_alert(
    tmp_path: Path,
) -> None:
    book = MonitorBook(tmp_path / "measured.sqlite3")
    breach = _input(500, 300)
    book.evaluate(breach)
    recovered = book.evaluate(_input())
    assert recovered["status"] == "healthy"
    assert book.evaluate(breach) == recovered


def test_concurrent_identical_ticks_commit_one_evaluation_and_notification(tmp_path: Path) -> None:
    book = MonitorBook(tmp_path / "measured.sqlite3")
    data = _input(500, 300)
    with ThreadPoolExecutor(max_workers=4) as workers:
        results = list(workers.map(book.evaluate, [data] * 4))
    assert all(result["evaluation_count"] == 1 for result in results)
    assert all(result["notifications_delivered"] == 1 for result in results)
    assert book.snapshot()["status"] == "firing"


def test_corrupted_persisted_evidence_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "measured.sqlite3"
    book = MonitorBook(path)
    book.evaluate(_input())
    with closing(sqlite3.connect(path)) as db, db:
        original = json.loads(db.execute("SELECT payload FROM monitor_evaluations").fetchone()[0])
        original["evidence_sha256"] = "c" * 64
        db.execute("UPDATE monitor_evaluations SET payload=?", (json.dumps(original),))
    with pytest.raises(ValueError, match="checksum mismatch"):
        book.snapshot()


def test_input_hash_is_order_independent_but_binds_evidence_and_values(tmp_path: Path) -> None:
    data = _input()
    book = MonitorBook(tmp_path / "measured.sqlite3")
    first = book.evaluate(data)
    reversed_data = replace(data, forecasts=data.forecasts[::-1], actuals=data.actuals[::-1])
    assert book.evaluate(reversed_data) == first
    new_evidence = book.evaluate(replace(data, evidence_sha256="c" * 64))
    assert new_evidence["input_sha256"] != first["input_sha256"]
    changed = book.evaluate(
        replace(data, actuals=(replace(data.actuals[0], quantity=Decimal(11)), *data.actuals[1:]))
    )
    assert changed["input_sha256"] != first["input_sha256"]


def test_chronological_ticks_reject_past_without_changes(tmp_path: Path) -> None:
    book = MonitorBook(tmp_path / "measured.sqlite3")
    before = book.evaluate(_input())
    with pytest.raises(ValueError, match="backwards"):
        book.evaluate(replace(_input(), now=NOW - timedelta(seconds=1)))
    assert book.snapshot() == before


def test_optional_bias_and_wape_thresholds_are_strict(tmp_path: Path) -> None:
    config = MonitorConfig(max_abs_bias_units=Decimal("0.8"), max_wape=Decimal(400) / Decimal(5400))
    book = MonitorBook(tmp_path / "measured.sqlite3", config)
    assert book.evaluate(_input())["status"] == "healthy"
    tighter = MonitorBook(
        tmp_path / "strict.sqlite3",
        replace(config, max_abs_bias_units=Decimal("0.79"), max_wape=Decimal("0.07")),
    )
    breaches = _model(tighter.evaluate(_input()))["breaches"]
    assert breaches == ["absolute_bias_above_limit", "wape_above_limit"]


def test_zero_total_wape_is_null_not_nan_or_a_healthy_wape_verdict(tmp_path: Path) -> None:
    data = _input()
    forecasts = tuple(
        replace(row, q025=Decimal(0), q10=Decimal(0 if index < 400 else 1), q50=Decimal(1))
        for index, row in enumerate(data.forecasts)
    )
    data = replace(
        data,
        forecasts=forecasts,
        actuals=tuple(replace(row, quantity=Decimal(0)) for row in data.actuals),
    )
    book = MonitorBook(tmp_path / "measured.sqlite3", MonitorConfig(max_wape=Decimal("0.2")))
    result = book.evaluate(data)
    assert _model(result)["status"] == "unknown"
    assert _model(result)["metrics"]["wape"] is None
    assert _model(result)["metrics"]["wape_defined"] is False
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), Decimal("-1"), True])
def test_nonfinite_negative_and_boolean_values_rejected(bad: Decimal | float | bool) -> None:
    data = _input(1)
    with pytest.raises(ValueError):
        replace(data.actuals[0], quantity=bad)
    with pytest.raises(ValueError):
        replace(data.forecasts[0], q50=bad)


def test_contract_validation_covers_keys_times_quantiles_and_hashes() -> None:
    data = _input(1)
    with pytest.raises(ValueError, match="nonblank"):
        replace(data.forecasts[0], sku_id=" ")
    with pytest.raises(ValueError, match="monotonic"):
        replace(data.forecasts[0], q50=Decimal(20))
    with pytest.raises(ValueError, match="SHA-256"):
        replace(data.forecasts[0], artifact_sha256="not-a-digest")
    with pytest.raises(ValueError, match="precede its issue"):
        replace(data.forecasts[0], target_date=date(2026, 9, 1))
    with pytest.raises(ValueError, match="completion"):
        replace(data.actuals[0], available_at=ISSUED)
    with pytest.raises(ValueError, match="completion"):
        replace(data.actuals[0], mature_after=ISSUED)
    with pytest.raises(ValueError, match="boolean"):
        replace(data.actuals[0], correction_closed=1)
    with pytest.raises(ValueError, match="must be a date"):
        replace(data.actuals[0], target_date=NOW)
    with pytest.raises(ValueError, match="tuple"):
        replace(data, forecasts=list(data.forecasts))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"min_mature_observations": 0},
        {"min_mature_observations": True},
        {"expected_source_cadence_seconds": -1},
        {"freshness_multiplier": Decimal(0)},
        {"nominal_coverage": Decimal("0.95")},
        {"coverage_tolerance": Decimal(1)},
        {"max_wape": Decimal("NaN")},
        {"max_abs_bias_units": Decimal(-1)},
    ],
)
def test_invalid_monitor_config_is_rejected(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        MonitorConfig(**kwargs)
