"""Source/guard helper tests only: no runtime builds, dbt, fitting, or soak.

Loading the acceptance script with runpy does not execute its __main__. These
tests deliberately make no claim that the full acceptance experiment passed.
"""

from __future__ import annotations

import csv
import json
import runpy
import shutil
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from nexus.data.r2_models import R2FulfillmentOutcome
from nexus.portfolio.runtime_pipeline import runtime_source_fingerprint
from nexus.portfolio.training_data import fulfillment_rows_sha256
from nexus.portfolio.training_fixture import TrainingFixture, generate_training_fixture
from nexus.storage import file_sha256

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def helpers() -> dict[str, Any]:
    return runpy.run_path(str(ROOT / "scripts/feedback_training_acceptance.py"))


@pytest.fixture(scope="module")
def fixture(tmp_path_factory: pytest.TempPathFactory) -> TrainingFixture:
    return generate_training_fixture(tmp_path_factory.mktemp("feedback-source-helpers") / "source")


@pytest.fixture
def source(fixture: TrainingFixture, tmp_path: Path) -> Path:
    destination = tmp_path / "source"
    shutil.copytree(fixture.source_dir, destination)
    return destination


def _rows(source: Path) -> list[dict[str, str]]:
    with (source / "fulfillment_outcomes.csv").open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _key(row: dict[str, str]) -> tuple[str, str, date]:
    return row["sku_id"], row["location_id"], date.fromisoformat(row["demand_date"])


def _typed(row: dict[str, str]) -> R2FulfillmentOutcome:
    return R2FulfillmentOutcome.model_validate(
        {
            **row,
            "demand_date": date.fromisoformat(row["demand_date"]),
            "available_at": datetime.fromisoformat(row["available_at"]),
            "demand_qty": int(row["demand_qty"]),
            "fulfilled_qty": int(row["fulfilled_qty"]),
        }
    )


def _replace_rows(
    source: Path, rows: list[dict[str, str]], *, refresh_row_proofs: bool = False
) -> None:
    """Rebind a test source package so semantic tests reach past the byte checks."""
    path = source / "fulfillment_outcomes.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    manifest_path = source / "source-manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    entry = next(item for item in manifest["sources"] if item["file_name"] == path.name)
    entry["content_sha256"] = file_sha256(path)
    entry["record_count"] = len(rows)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    finalization_path = source / "outcome-finalization.json"
    finalization = json.loads(finalization_path.read_bytes())
    finalization["source_snapshot_sha256"] = runtime_source_fingerprint(source)
    if refresh_row_proofs:
        typed = [_typed(row) for row in rows]
        for proof in finalization["labels"]:
            key = (proof["sku_id"], proof["location_id"], date.fromisoformat(proof["demand_date"]))
            matching = tuple(
                row for row in typed if (row.sku_id, row.location_id, row.demand_date) == key
            )
            proof["source_rows_sha256"] = fulfillment_rows_sha256(matching)
    finalization_path.write_text(json.dumps(finalization), encoding="utf-8")


def test_source_actuals_are_sorted_deterministic_closed_and_byte_bound(
    helpers: dict[str, Any], source: Path
) -> None:
    rows = _rows(source)
    chosen = [rows[index] for index in (0, 17, 450, 1001)]
    keys = {_key(row) for row in chosen}
    actuals = helpers["source_actuals"](source, keys)
    assert [row.key for row in actuals] == sorted(keys)
    assert helpers["source_actuals"](source, keys) == actuals
    expected = {_key(row): row for row in chosen}
    for actual in actuals:
        row = expected[actual.key]
        assert actual.quantity == Decimal(row["demand_qty"])
        assert actual.quantity_semantics == "observed_daily_demand"
        assert actual.source_snapshot_sha256 == runtime_source_fingerprint(source)
        assert actual.available_at == datetime.fromisoformat(row["available_at"])
        assert actual.correction_closed is True
        assert actual.correction_closed_at is not None
        earliest = datetime.combine(
            actual.target_date + timedelta(days=4), datetime.min.time(), UTC
        )
        assert actual.correction_closed_at >= earliest
        assert actual.revision == 1 and actual.supersedes_event_id is None


def test_modified_source_bytes_are_rejected_before_actuals(
    helpers: dict[str, Any], source: Path
) -> None:
    row = _rows(source)[0]
    path = source / "fulfillment_outcomes.csv"
    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="checksum"):
        helpers["source_actuals"](source, {_key(row)})


def test_forecast_outcome_missing_from_source_is_rejected(
    helpers: dict[str, Any], source: Path
) -> None:
    row = _rows(source)[0]
    with pytest.raises(ValueError, match="missing a forecast outcome"):
        helpers["source_actuals"](
            source, {_key(row), ("SKU-MISSING", row["location_id"], date(2025, 7, 1))}
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"demand_qty": "not-an-integer"},
        {"demand_qty": "1.5"},
        {"demand_qty": "-1"},
        {"fulfilled_qty": "-1"},
        {"demand_date": "2025-99-99"},
        {"available_at": "2025-06-02T00:00:00"},
        {"unexpected_field": "not allowed"},
    ],
)
def test_rebound_source_still_requires_strict_typed_rows(
    helpers: dict[str, Any], source: Path, changes: dict[str, str]
) -> None:
    rows = _rows(source)
    key = _key(rows[0])
    rows[0].update(changes)
    if "unexpected_field" in changes:
        for row in rows[1:]:
            row["unexpected_field"] = ""
    _replace_rows(source, rows)
    with pytest.raises(ValueError):
        helpers["source_actuals"](source, {key})


@pytest.mark.parametrize(
    "change,match",
    [
        ({"source_snapshot_sha256": "f" * 64}, "source finalization binding"),
        ({"classification": "production"}, "classification"),
    ],
)
def test_bad_finalization_manifest_is_rejected(
    helpers: dict[str, Any], source: Path, change: dict[str, Any], match: str
) -> None:
    key = _key(_rows(source)[0])
    path = source / "outcome-finalization.json"
    payload = json.loads(path.read_bytes())
    payload.update(change)
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match=match):
        helpers["source_actuals"](source, {key})


@pytest.mark.parametrize(
    "change",
    [
        {"correction_closed": False},
        {"source_rows_sha256": "f" * 64},
    ],
)
def test_open_or_wrong_row_finalization_is_rejected(
    helpers: dict[str, Any], source: Path, change: dict[str, Any]
) -> None:
    path = source / "outcome-finalization.json"
    payload = json.loads(path.read_bytes())
    proof = payload["labels"][0]
    key = (proof["sku_id"], proof["location_id"], date.fromisoformat(proof["demand_date"]))
    proof.update(change)
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="correction closure/source row"):
        helpers["source_actuals"](source, {key})


def test_premature_finalization_is_rejected(helpers: dict[str, Any], source: Path) -> None:
    path = source / "outcome-finalization.json"
    payload = json.loads(path.read_bytes())
    proof = payload["labels"][0]
    day = date.fromisoformat(proof["demand_date"])
    proof["correction_closed_at"] = datetime.combine(
        day + timedelta(days=1), datetime.min.time(), UTC
    ).isoformat()
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="72-hour"):
        helpers["source_actuals"](source, {(proof["sku_id"], proof["location_id"], day)})


def test_rebound_censored_sales_do_not_replace_observed_demand(
    helpers: dict[str, Any], source: Path
) -> None:
    rows = _rows(source)
    row = rows[0]
    assert int(row["demand_qty"]) > 0
    row["fulfilled_qty"] = str(int(row["demand_qty"]) - 1)
    _replace_rows(source, rows, refresh_row_proofs=True)
    with pytest.raises(ValueError, match="uncensored observed demand"):
        helpers["source_actuals"](source, {_key(row)})


@pytest.mark.parametrize(
    "case", ["outside_artifacts", "artifacts_root", "inside_source", "contains_source"]
)
def test_output_guards_fail_before_runtime_build_or_output_creation(
    helpers: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    run = helpers["run"]
    project = tmp_path / "project"
    namespace = run.__globals__
    monkeypatch.setitem(namespace, "PROJECT_ROOT", project)
    monkeypatch.setitem(
        namespace,
        "build_runtime_seed",
        lambda *args, **kwargs: pytest.fail("guard test launched a runtime/dbt build"),
    )
    source = project / "artifacts/source"
    outputs = {
        "outside_artifacts": tmp_path / "unrelated",
        "artifacts_root": project / "artifacts",
        "inside_source": source / "nested-output",
        "contains_source": project / "artifacts/source-parent",
    }
    output = outputs[case]
    if case == "contains_source":
        source = output / "source"
    with pytest.raises(ValueError, match=r"artifacts subdirectory|overwrite the source"):
        run(source, output, build_only=True)
    assert not output.exists()
    assert not project.exists()


@pytest.mark.parametrize(
    "case", ["outside_artifacts", "artifacts_root", "inside_source", "contains_source"]
)
def test_runtime_reuse_guard_fails_before_output_or_build(
    helpers: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    run = helpers["run"]
    project = tmp_path / "project"
    namespace = run.__globals__
    monkeypatch.setitem(namespace, "PROJECT_ROOT", project)
    monkeypatch.setitem(
        namespace,
        "build_runtime_seed",
        lambda *args, **kwargs: pytest.fail("guard test launched runtime/dbt"),
    )
    source = project / "artifacts/source"
    output = project / "artifacts/acceptance"
    candidates = {
        "outside_artifacts": tmp_path / "unrelated",
        "artifacts_root": project / "artifacts",
        "inside_source": source / "reused-runtime",
        "contains_source": project / "artifacts/source-parent",
    }
    runtime = candidates[case]
    if case == "contains_source":
        source = runtime / "source"
    with pytest.raises(ValueError, match="runtime evidence must use"):
        run(source, output, build_only=True, runtime_dir=runtime)
    assert not output.exists()
    assert not project.exists()
