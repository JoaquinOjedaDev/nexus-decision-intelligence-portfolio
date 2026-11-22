"""One synthetic business loop against the fresh isolated Docker service profile."""

from __future__ import annotations

import csv
import importlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx

from nexus.contracts import EventEnvelope
from nexus.portfolio.service_cli import open_service
from nexus.portfolio.service_config import read_service_config
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.store import DEMO_MATURITY_SECONDS
from nexus.portfolio.transport_consumer import inspect_topic
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger
from nexus.storage import file_sha256

ROOT = Path("/state")
MANIFEST = ROOT / "private-service.json"
OUTPUT = ROOT / "business-acceptance"
URL = "http://127.0.0.1:8767"


def response(value: httpx.Response, expected: int = 200) -> Any:
    if value.status_code != expected:
        raise RuntimeError(f"HTTP status {value.status_code}; expected {expected}")
    return value.json()


def refused(value: httpx.Response, *, status: int, code: str) -> None:
    """A generic conflict is not proof of the intended approval boundary."""
    if value.status_code != status:
        raise RuntimeError("Expected safe refusal status differs")
    try:
        body = value.json()
    except ValueError:
        raise RuntimeError("Safe refusal payload is malformed") from None
    if (
        not isinstance(body, dict)
        or not isinstance(body.get("error"), dict)
        or body["error"].get("code") != code
    ):
        raise RuntimeError("Safe refusal code differs")


def replacement(
    initial: dict[str, Any],
    current: dict[str, Any],
    overview: dict[str, Any],
    old: dict[str, Any],
    initial_generation: str,
    source_sha256: str,
) -> tuple[dict[str, Any], str]:
    """Bind one completed material job to the active source and selected advice."""
    jobs = current.get("jobs")
    metadata = overview.get("metadata")
    generation = overview.get("generation_seed_sha256")
    if (
        current.get("archived_generations") != initial.get("archived_generations", -1) + 1
        or current.get("event_count") != initial.get("event_count", -1) + 1
        or not isinstance(jobs, list)
        or len(jobs) != 1
        or not isinstance(jobs[0], dict)
        or jobs[0].get("state") != "completed"
        or jobs[0].get("source_sha256") != source_sha256
        or not isinstance(jobs[0].get("id"), str)
        or not isinstance(metadata, dict)
        or metadata.get("source_snapshot_sha256") != source_sha256
        or metadata.get("runtime_job_id") != jobs[0]["id"]
        or not isinstance(generation, str)
        or generation == initial_generation
    ):
        raise RuntimeError("Replacement is not bound to the submitted material source and job")
    old_rows = [
        row
        for row in overview.get("recommendations", [])
        if isinstance(row, dict) and row.get("id") == old["id"]
    ]
    if (
        len(old_rows) != 1
        or old_rows[0].get("state") != "invalidated"
        or old_rows[0].get("current_generation") is not False
        or old_rows[0].get("generation_seed_sha256") != initial_generation
    ):
        raise RuntimeError("Old approved recommendation was not invalidated")
    current_rows = overview.get("current_recommendations")
    if not isinstance(current_rows, list):
        raise RuntimeError("Current recommendation projection is missing")
    selected = [
        row for row in current_rows if isinstance(row, dict) and row.get("state") == "pending"
    ]
    if not selected:
        raise RuntimeError("No newly pending recommendation exists")
    row = selected[0]
    if (
        row.get("current_generation") is not True
        or row.get("generation_seed_sha256") != generation
        or not isinstance(row.get("id"), str)
        or not row["id"].startswith(jobs[0]["id"] + "-")
        or row.get("fingerprint") == old.get("fingerprint")
    ):
        raise RuntimeError("Selected recommendation lacks the replacement generation binding")
    return row, jobs[0]["id"]


def broker_progress(binding: TransportBinding) -> dict[str, Any]:
    """Metadata and committed offsets only: no assign, subscribe, poll or commit."""
    actual = inspect_topic(binding.broker)
    if actual != {
        "cluster_id": binding.cluster_id,
        "topic_id": binding.topic_id,
        "partitions": list(range(binding.partition_count)),
    }:
        raise RuntimeError("Broker or topic identity differs from the retained binding")
    module = importlib.import_module("confluent_kafka")
    consumer = module.Consumer(binding.broker.consumer_options())
    try:
        parts = [
            module.TopicPartition(binding.broker.topic, index)
            for index in range(binding.partition_count)
        ]
        committed = consumer.committed(parts, timeout=5)
        if len(committed) != binding.partition_count or any(
            part.error is not None
            or part.topic != binding.broker.topic
            or part.partition != index
            or type(part.offset) is not int
            or part.offset < 0
            for index, part in enumerate(committed)
        ):
            raise RuntimeError("Broker committed offsets are unavailable")
        watermarks = [
            consumer.get_watermark_offsets(part, timeout=5, cached=False) for part in parts
        ]
        if any(
            len(mark) != 2
            or any(type(value) is not int or value < 0 for value in mark)
            or mark[0] > mark[1]
            for mark in watermarks
        ):
            raise RuntimeError("Broker watermarks are unavailable")
        return {
            "cluster_id": actual["cluster_id"],
            "topic_id": actual["topic_id"],
            "committed": [part.offset for part in committed],
            "watermarks": [list(mark) for mark in watermarks],
        }
    finally:
        consumer.close()


def settled_broker_progress(
    operations: TransportBinding, audit: TransportBinding, durable: list[int]
) -> dict[str, Any] | None:
    """Require both physical group commits and topic ends to match retained capture."""
    observed_ops = broker_progress(operations)
    observed_audit = broker_progress(audit)
    for observed in (observed_ops, observed_audit):
        committed, watermarks = observed["committed"], observed["watermarks"]
        if len(committed) != len(durable) or len(watermarks) != len(durable):
            raise RuntimeError("Broker partition vector differs from retained history")
        if any(
            low != 0 or high != retained or offset > retained
            for (low, high), offset, retained in zip(watermarks, committed, durable, strict=True)
        ):
            raise RuntimeError("Broker watermarks or committed progress exceed retained history")
    if observed_ops["committed"] != durable or observed_audit["committed"] != durable:
        return None
    return {"operations": observed_ops, "audit": observed_audit}


def login(client: httpx.Client, persona: str) -> dict[str, str]:
    value = response(client.post("/api/session", json={"persona": persona}))
    return {"X-CSRF-Token": value["csrf_token"]}


def wait_for(check: Any, label: str, seconds: float = 150) -> Any:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.5)
    raise RuntimeError(f"Timed out at {label}")


def correct_inventory(source: Path) -> str:
    """Retain original fixture bytes and apply one explicit historical correction."""
    if source != ROOT / "watched-source" or source.is_symlink():
        raise ValueError("Unexpected watched source")
    target = OUTPUT / "corrected-source"
    shutil.copytree(source, target)
    inventory = target / "opening_inventory.csv"
    with inventory.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        fields, rows = reader.fieldnames, list(reader)
    if not fields or rows[0]["sku_id"] != "SKU-001":
        raise ValueError("Unexpected synthetic source fixture")
    rows[0]["on_hand_qty"] = str(int(rows[0]["on_hand_qty"]) + 7)
    with inventory.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    manifest_path = target / "source-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    matches = [row for row in manifest["sources"] if row["file_name"] == inventory.name]
    if len(matches) != 1:
        raise ValueError("Unexpected source manifest")
    matches[0]["content_sha256"] = file_sha256(inventory)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    source.rename(OUTPUT / "original-source-preserved")
    shutil.copytree(target, source)
    return SourceBundle.capture(source).source_sha256


def exercise(receipt: dict[str, Any]) -> None:
    config = read_service_config(MANIFEST)
    receipt["manifest_sha256"] = config.manifest_sha256
    with (
        httpx.Client(base_url=URL, timeout=15, trust_env=False) as planner,
        httpx.Client(base_url=URL, timeout=15, trust_env=False) as approver,
    ):
        planner_headers, approver_headers = login(planner, "planner"), login(approver, "approver")

        def status() -> dict[str, Any]:
            value = response(planner.get("/api/runtime/status"))
            if not isinstance(value, dict):
                raise ValueError("Invalid runtime status")
            return value

        def healthy() -> bool:
            current = status()
            return bool(
                current["source_watch"]["reason"] == "fresh"
                and current["capture_health"]["reason"] == "caught_up_at_observation"
                and current["source_publication"]["pending_admission"] == 0
            )

        receipt["stage"] = "initial_readiness"
        wait_for(healthy, "initial_readiness")
        initial = status()
        overview = response(planner.get("/api/overview"))
        if initial["event_count"] != 0 or overview["summary"]["effect_count"] != 0:
            raise ValueError("Business acceptance requires fresh prepared state")
        initial_generation = overview["generation_seed_sha256"]
        old = response(planner.get("/api/recommendations"))["items"][0]
        old_url = "/api/recommendations/" + old["id"]
        old_body = {"fingerprint": old["fingerprint"]}
        response(approver.post(old_url + "/approve", json=old_body, headers=approver_headers))

        receipt["stage"] = "source_publication"
        if config.fence.source_watch is None:
            raise ValueError("Explicit source-watch configuration required")
        source_hash = correct_inventory(Path(config.fence.source_watch.source_root))
        event_id = uuid4()
        instant = datetime.now(UTC)
        event = EventEnvelope(
            event_id=event_id,
            event_type="inventory.adjusted",
            source="nexus.source-snapshot.v1",
            subject_id="portfolio",
            occurred_at=instant,
            ingested_at=instant,
            idempotency_key=str(event_id),
            payload={"source_snapshot_sha256": source_hash},
        )
        event_path = OUTPUT / "event.json"
        event_path.write_text(event.model_dump_json(), encoding="utf-8")
        queued = subprocess.run(
            [
                sys.executable,
                "-m",
                "nexus.cli",
                "portfolio",
                "service",
                "enqueue-source",
                str(MANIFEST),
                str(event_path),
            ],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if queued.returncode:
            raise RuntimeError("Source enqueue failed; retained state requires inspection")
        queued_receipt = json.loads(queued.stdout)
        publication = queued_receipt["source_publication"]
        if (
            publication.get("id") != str(event_id)
            or publication.get("source_sha256") != source_hash
            or publication.get("duplicate") is not False
        ):
            raise RuntimeError("Accepted source intent differs from the submitted event")
        receipt["source_intent"] = {
            "id": publication["id"],
            "source_sha256": publication["source_sha256"],
            "sequence": publication["sequence"],
        }
        receipt["source_sha256"] = source_hash
        receipt["stage"] = "replan"
        wait_for(
            lambda: status()["archived_generations"] == initial["archived_generations"] + 1,
            "material_replan",
        )
        wait_for(healthy, "post_replan_readiness")
        active_status = status()
        active_overview = response(planner.get("/api/overview"))
        current, job_id = replacement(
            initial, active_status, active_overview, old, initial_generation, source_hash
        )
        refused(
            approver.post(
                old_url + "/execute",
                json=old_body,
                headers={**approver_headers, "Idempotency-Key": "stale-ops-order"},
            ),
            status=409,
            code="approval_required",
        )
        if response(planner.get("/api/overview"))["summary"]["effect_count"] != 0:
            raise RuntimeError("Old recommendation refusal still created an effect")
        endpoint = "/api/recommendations/" + current["id"]
        context = response(planner.get(endpoint))
        if (
            context.get("fingerprint") != current["fingerprint"]
            or context.get("state") != "pending"
            or context.get("generation", {}).get("seed_sha256")
            != active_overview["generation_seed_sha256"]
            or context.get("generation", {}).get("is_current") is not True
            or context.get("generation", {}).get("metadata", {}).get("runtime_job_id") != job_id
            or context.get("generation", {}).get("metadata", {}).get("source_snapshot_sha256")
            != source_hash
        ):
            raise RuntimeError("Recommendation detail lost the source/job/generation binding")
        body = {"fingerprint": current["fingerprint"]}
        execution_headers = {**approver_headers, "Idempotency-Key": "isolated-ops-order"}
        receipt["stage"] = "human_approval_and_execution"
        refused(
            approver.post(endpoint + "/execute", json=body, headers=execution_headers),
            status=409,
            code="approval_required",
        )
        approved = response(
            approver.post(endpoint + "/approve", json=body, headers=approver_headers)
        )
        if (
            approved.get("state") != "approved"
            or approved.get("fingerprint") != current["fingerprint"]
        ):
            raise RuntimeError("Human approval did not bind the selected recommendation")
        executions = []
        for _ in range(2):
            executions.append(
                response(approver.post(endpoint + "/execute", json=body, headers=execution_headers))
            )
        references = {item.get("execution_ref") for item in executions}
        if (
            any(
                item.get("state") != "executed" or item.get("fingerprint") != current["fingerprint"]
                for item in executions
            )
            or len(references) != 1
            or not next(iter(references))
        ):
            raise RuntimeError("Repeated execution did not retain one bound mock effect")
        execution_ref = next(iter(references))
        receipt["stage"] = "outcome"
        refused(
            planner.post(endpoint + "/outcome", json=body, headers=planner_headers),
            status=425,
            code="outcome_immature",
        )
        time.sleep(DEMO_MATURITY_SECONDS + 0.1)
        outcome = response(planner.post(endpoint + "/outcome", json=body, headers=planner_headers))
        if (
            outcome.get("state") != "outcome_observed"
            or outcome.get("fingerprint") != current["fingerprint"]
            or outcome.get("execution_ref") != execution_ref
            or not isinstance(outcome.get("outcome"), dict)
            or outcome["outcome"].get("execution_ref") != execution_ref
            or outcome["outcome"].get("causal_business_impact_claim") is not False
        ):
            raise RuntimeError("Observed outcome differs from the approved mock effect")
        final = status()
        effects = response(planner.get("/api/overview"))["summary"]["effect_count"]
        ops = open_service(config).transport.verify_history(config.fence.binding)
        audit = TransportLedger(config.audit).verify_history(config.audit_binding)
        if (
            final["event_count"] != 1
            or effects != 1
            or final["source_publication"]["pending_admission"] != 0
            or sum(ops["delivered"]) != 1
            or ops["captured"] != ops["delivered"]
            or audit["captured"] != ops["delivered"]
        ):
            raise RuntimeError("Final event/effect/two-group evidence differs")
        expected_offsets = ops["captured"]
        broker = wait_for(
            lambda: settled_broker_progress(
                config.fence.binding, config.audit_binding, expected_offsets
            ),
            "committed_broker_progress",
            seconds=30,
        )
        receipt.update(
            status="passed",
            stage="complete",
            runtime_status={
                "adapter": final["adapter"],
                "event_count": final["event_count"],
                "archived_generations": final["archived_generations"],
                "source_watch_reason": final["source_watch"]["reason"],
                "capture_health_reason": final["capture_health"]["reason"],
                "pending_admission": final["source_publication"]["pending_admission"],
                "replacement_job_id": job_id,
            },
            effect_count=effects,
            operations_delivered=ops["delivered"],
            audit_captured=audit["captured"],
            broker_progress=broker,
            recommendation_id=current["id"],
            execution_ref=execution_ref,
        )


def main() -> int:
    if sys.platform != "linux" or os.getuid() != 10001 or ROOT.is_symlink():
        raise RuntimeError("Use the owned non-root Linux service namespace")
    OUTPUT.mkdir(exist_ok=False)
    started = time.monotonic()
    receipt: dict[str, Any] = {
        "contract": "nexus.ops-business-acceptance.v1",
        "status": "running",
        "stage": "configuration",
        "synthetic_data": True,
        "mock_erp": True,
        "load_or_soak_claimed": False,
        "harness_sha256": file_sha256(Path(__file__)),
    }
    try:
        exercise(receipt)
    except Exception as error:
        receipt.update(status="failed", error_type=type(error).__name__)
    finally:
        receipt["elapsed_seconds"] = time.monotonic() - started
        with (OUTPUT / "receipt.json").open("x", encoding="utf-8") as stream:
            json.dump(receipt, stream, indent=2)
    print(json.dumps({"status": receipt["status"], "stage": receipt["stage"]}))
    return 0 if receipt["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
