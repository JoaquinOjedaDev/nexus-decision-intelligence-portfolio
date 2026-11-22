"""Offline acceptance-validator tests; no API, broker, database or Docker contact."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from nexus.portfolio.broker_runtime import RuntimeBrokerConfig
from nexus.portfolio.transport_ledger import TransportBinding
from scripts import ops_business_acceptance as acceptance


def replacement_case() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    source = "a" * 64
    old = {"id": "old-recommendation", "fingerprint": "d" * 64}
    initial = {"archived_generations": 0, "event_count": 0}
    current = {
        "archived_generations": 1,
        "event_count": 1,
        "jobs": [{"id": "job-1", "state": "completed", "source_sha256": source}],
    }
    overview = {
        "metadata": {"runtime_job_id": "job-1", "source_snapshot_sha256": source},
        "generation_seed_sha256": "b" * 64,
        "recommendations": [
            {
                "id": old["id"],
                "state": "invalidated",
                "current_generation": False,
                "generation_seed_sha256": "c" * 64,
            }
        ],
        "current_recommendations": [
            {
                "id": "job-1-new-recommendation",
                "state": "pending",
                "fingerprint": "e" * 64,
                "current_generation": True,
                "generation_seed_sha256": "b" * 64,
            }
        ],
    }
    return initial, current, overview, old


def validate_case(
    initial: dict[str, Any],
    current: dict[str, Any],
    overview: dict[str, Any],
    old: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    return acceptance.replacement(initial, current, overview, old, "c" * 64, "a" * 64)


def test_replacement_requires_exact_source_job_generation_and_old_invalidation() -> None:
    initial, current, overview, old = replacement_case()
    row, job_id = validate_case(initial, current, overview, old)
    assert row["id"] == "job-1-new-recommendation"
    assert job_id == "job-1"

    mutations: tuple[Callable[[dict[str, Any], dict[str, Any]], None], ...] = (
        lambda c, o: c["jobs"][0].update(source_sha256="f" * 64),
        lambda c, o: o["metadata"].update(source_snapshot_sha256="f" * 64),
        lambda c, o: o["metadata"].update(runtime_job_id="unrelated-job"),
        lambda c, o: o["recommendations"][0].update(state="approved"),
        lambda c, o: o["current_recommendations"][0].update(generation_seed_sha256="c" * 64),
        lambda c, o: o["current_recommendations"][0].update(id="unrelated-recommendation"),
    )
    for mutation in mutations:
        changed_current, changed_overview = deepcopy(current), deepcopy(overview)
        mutation(changed_current, changed_overview)
        with pytest.raises(RuntimeError):
            validate_case(initial, changed_current, changed_overview, old)


def test_safe_refusal_requires_exact_error_code() -> None:
    correct = httpx.Response(409, json={"error": {"code": "approval_required"}})
    acceptance.refused(correct, status=409, code="approval_required")
    for result in (
        httpx.Response(409, json={"error": {"code": "transport_pending"}}),
        httpx.Response(403, json={"error": {"code": "approval_required"}}),
        httpx.Response(409, text="not-json"),
    ):
        with pytest.raises(RuntimeError):
            acceptance.refused(result, status=409, code="approval_required")


def binding() -> TransportBinding:
    broker = RuntimeBrokerConfig(
        "127.0.0.1:19092", "nexus-runtime-ops-source", "nexus-runtime-ops", ("LOC-001",)
    )
    return TransportBinding(broker, "cluster-1", "1" * 32, 2, "operations", "a" * 64)


def test_broker_observation_reads_only_and_checks_pinned_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = binding()
    calls: list[str] = []

    class Partition:
        def __init__(self, topic: str, partition: int, offset: int = 0):
            self.topic, self.partition, self.offset, self.error = topic, partition, offset, None

    class Consumer:
        def __init__(self, options: dict[str, Any]):
            assert options["enable.auto.commit"] is False
            calls.append("open")

        def committed(self, parts: list[Partition], timeout: int) -> list[Partition]:
            calls.append("committed")
            return [Partition(parts[0].topic, 0, 1), Partition(parts[1].topic, 1, 0)]

        def get_watermark_offsets(
            self, part: Partition, timeout: int, cached: bool
        ) -> tuple[int, int]:
            assert cached is False
            calls.append("watermark")
            return (0, 1 if part.partition == 0 else 0)

        def close(self) -> None:
            calls.append("close")

    module = SimpleNamespace(Consumer=Consumer, TopicPartition=Partition)
    original_import = importlib.import_module
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name: module if name == "confluent_kafka" else original_import(name),
    )
    monkeypatch.setattr(
        acceptance,
        "inspect_topic",
        lambda _: {"cluster_id": "cluster-1", "topic_id": "1" * 32, "partitions": [0, 1]},
    )
    observed = acceptance.broker_progress(target)
    assert observed["committed"] == [1, 0]
    assert observed["watermarks"] == [[0, 1], [0, 0]]
    assert calls == ["open", "committed", "watermark", "watermark", "close"]

    monkeypatch.setattr(
        acceptance,
        "inspect_topic",
        lambda _: {"cluster_id": "different", "topic_id": "1" * 32, "partitions": [0, 1]},
    )
    with pytest.raises(RuntimeError, match="identity differs"):
        acceptance.broker_progress(target)
    assert calls == ["open", "committed", "watermark", "watermark", "close"]


def test_both_group_commits_must_match_retained_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations = binding()
    audit_broker = RuntimeBrokerConfig(
        "127.0.0.1:19092", "nexus-runtime-ops-source", "nexus-runtime-audit", ("LOC-001",)
    )
    audit = TransportBinding(audit_broker, "cluster-1", "1" * 32, 2, "audit")
    values = {
        "nexus-runtime-ops": {
            "committed": [1, 0],
            "watermarks": [[0, 1], [0, 0]],
        },
        "nexus-runtime-audit": {
            "committed": [0, 0],
            "watermarks": [[0, 1], [0, 0]],
        },
    }
    monkeypatch.setattr(acceptance, "broker_progress", lambda value: values[value.broker.group_id])
    assert acceptance.settled_broker_progress(operations, audit, [1, 0]) is None
    values["nexus-runtime-audit"]["committed"] = [1, 0]
    assert acceptance.settled_broker_progress(operations, audit, [1, 0]) is not None
    values["nexus-runtime-audit"]["watermarks"] = [[0, 2], [0, 0]]
    with pytest.raises(RuntimeError, match="exceed retained history"):
        acceptance.settled_broker_progress(operations, audit, [1, 0])
