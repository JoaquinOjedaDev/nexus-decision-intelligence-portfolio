from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from nexus.modeling.r3_artifacts import (
    EVALUATION_ARTIFACT_SCHEMA,
    FORECAST_ARTIFACT_SCHEMA,
    EvaluationArtifactRow,
    canonical_json_bytes,
    canonical_json_sha256,
    write_evaluation_rows,
    write_forecast_rows,
    write_immutable_json_manifest,
)
from nexus.modeling.r3_hierarchy import (
    BottomLevelQuantileForecast,
    HierarchicalQuantileForecast,
    aggregate_quantile_forecasts,
)
from nexus.storage import ArtifactConflictError, file_sha256

ISSUE = datetime(2026, 7, 1, 9, tzinfo=UTC)
TARGET = date(2026, 7, 2)


def _hierarchical_rows() -> tuple[HierarchicalQuantileForecast, ...]:
    bottom = (
        BottomLevelQuantileForecast(ISSUE, TARGET, "SKU-A", "LOC-1", Decimal("0.1"), Decimal("1")),
        BottomLevelQuantileForecast(ISSUE, TARGET, "SKU-A", "LOC-1", Decimal("0.5"), Decimal("2")),
        BottomLevelQuantileForecast(ISSUE, TARGET, "SKU-A", "LOC-1", Decimal("0.9"), Decimal("3")),
    )
    return aggregate_quantile_forecasts(bottom)


def _evaluation_row(model_name: str = "seasonal_naive") -> EvaluationArtifactRow:
    return EvaluationArtifactRow(
        model_name=model_name,
        model_version="1.0.0",
        segment_name="all",
        segment_value="all",
        horizon_days=7,
        observation_count=12,
        pinball_q025=Decimal("0.3"),
        pinball_q10=Decimal("0.4"),
        pinball_q50=Decimal("0.5"),
        pinball_q90=Decimal("0.6"),
        pinball_q975=Decimal("0.7"),
        interval_score_80=Decimal("1.2"),
        interval_coverage_80=Decimal("0.8"),
        interval_score_95=Decimal("1.5"),
        interval_coverage_95=Decimal("0.95"),
        weighted_interval_score=Decimal("0.75"),
        bias=Decimal("-0.1"),
        mase=None,
    )


def test_canonical_json_has_exact_bytes_and_hash() -> None:
    payload = {
        "z": Decimal("1.2300"),
        "a": [2, 1],
        "when": datetime(2026, 7, 1, 6, tzinfo=UTC),
    }
    assert canonical_json_bytes(payload) == (
        b'{"a":[2,1],"when":"2026-07-01T06:00:00Z","z":"1.23"}'
    )
    assert canonical_json_sha256(payload) == (
        "c290867e0e4f5037d3c35d8b5fa34767530ce2753b1bac012da888d96ccaae01"
    )
    assert canonical_json_sha256(dict(reversed(tuple(payload.items())))) == canonical_json_sha256(
        payload
    )


def test_json_manifest_is_immutable_and_detects_corruption(tmp_path: Path) -> None:
    manifest = {"schema_version": "1.0.0", "rows": 3, "metric": Decimal("0.75")}
    first = write_immutable_json_manifest(
        tmp_path,
        dataset="r3.manifest",
        manifest=manifest,
        schema_identity={"name": "r3-manifest", "version": "1.0.0"},
    )
    initial_mtime = first.path.stat().st_mtime_ns
    second = write_immutable_json_manifest(
        tmp_path,
        dataset="r3.manifest",
        manifest={"metric": Decimal("0.750"), "rows": 3, "schema_version": "1.0.0"},
        schema_identity={"version": "1.0.0", "name": "r3-manifest"},
    )

    assert first == second
    assert first.path.name == f"{first.content_sha256}.json"
    assert first.path.stat().st_mtime_ns == initial_mtime
    assert first.file_sha256 == file_sha256(first.path)
    assert first.row_count == 1
    assert first.content_sha256 == (
        "1ad481186996832be4d9016c869058659799ec297dead4f9149eeb4acadffee5"
    )
    assert first.schema_sha256 == (
        "3b9bf9448958d0128584c7926f64ebc44013f1cac184e8d0a5a88ca2d779318d"
    )
    assert first.file_sha256 == ("d19851675422c9c7eb0c4f3b938555e3634de421cabf3de8a3038caa7675fa0a")

    first.path.write_bytes(b"{}\n")
    with pytest.raises(ArtifactConflictError, match="manifest is corrupt"):
        write_immutable_json_manifest(
            tmp_path,
            dataset="r3.manifest",
            manifest=manifest,
            schema_identity={"name": "r3-manifest", "version": "1.0.0"},
        )


def test_forecast_parquet_is_order_invariant_and_detects_corruption(tmp_path: Path) -> None:
    rows = _hierarchical_rows()
    first = write_forecast_rows(tmp_path / "first", dataset="r3.forecast", rows=rows)
    second = write_forecast_rows(
        tmp_path / "second", dataset="r3.forecast", rows=tuple(reversed(rows))
    )

    assert (
        first.content_sha256
        == second.content_sha256
        == ("9e7298c6981b7c30a6d23617df66f56f296b084346fec5f8df08448cb2fad868")
    )
    assert first.schema_sha256 == second.schema_sha256
    assert first.file_sha256 == second.file_sha256
    assert first.row_count == len(rows)
    assert first.path.name == f"{first.content_sha256}.parquet"

    first.path.write_bytes(b"not parquet")
    with pytest.raises(ArtifactConflictError, match="Parquet artifact is corrupt"):
        write_forecast_rows(tmp_path / "first", dataset="r3.forecast", rows=rows)


def test_empty_parquet_outputs_preserve_explicit_schemas(tmp_path: Path) -> None:
    forecast = write_forecast_rows(tmp_path, dataset="r3.empty_forecast", rows=())
    evaluation = write_evaluation_rows(tmp_path, dataset="r3.empty_evaluation", rows=())

    assert forecast.row_count == evaluation.row_count == 0
    assert (
        forecast.content_sha256
        == evaluation.content_sha256
        == ("4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945")
    )
    assert forecast.schema_sha256 != evaluation.schema_sha256
    assert pq.read_schema(forecast.path).equals(FORECAST_ARTIFACT_SCHEMA, check_metadata=True)
    assert pq.read_schema(evaluation.path).equals(EVALUATION_ARTIFACT_SCHEMA, check_metadata=True)


def test_evaluation_rows_are_order_invariant(tmp_path: Path) -> None:
    rows = (_evaluation_row("seasonal_naive"), _evaluation_row("intermittent_baseline"))
    first = write_evaluation_rows(tmp_path / "a", dataset="r3.evaluation", rows=rows)
    second = write_evaluation_rows(
        tmp_path / "b", dataset="r3.evaluation", rows=tuple(reversed(rows))
    )
    assert first.content_sha256 == second.content_sha256
    assert first.schema_sha256 == second.schema_sha256
    assert first.file_sha256 == second.file_sha256
    assert first.row_count == 2
