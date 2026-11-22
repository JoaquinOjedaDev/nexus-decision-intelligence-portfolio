"""Opt-in material traffic through seven real, concurrently supervised workers."""

from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from test_portfolio_broker_runtime_service import Broker
from test_portfolio_broker_runtime_service import broker as broker
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_source_http_service import ROOT, login, response, serving
from test_portfolio_recovery_publisher_service import create_evidence_topic
from test_portfolio_transport_consumer_service import setup as durable_setup

from nexus.contracts import EventEnvelope
from nexus.policies.replanning import ReplanPolicy
from nexus.portfolio.auth import resolve_persona
from nexus.portfolio.capture_health_ledger import CaptureHealthLedger
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger
from nexus.portfolio.runtime_service import prepare_postgres_runtime
from nexus.portfolio.service_config import read_service_config
from nexus.portfolio.service_supervisor import supervise
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.source_freshness import SourceWatchPolicy
from nexus.portfolio.source_watch_runtime import SourceWatchRuntimeStore
from nexus.portfolio.store import DEMO_MATURITY_SECONDS
from nexus.portfolio.transport_runtime import TransportFenceConfig
from nexus.storage import file_sha256

pytestmark = [
    pytest.mark.portfolio_service,
    pytest.mark.skipif(
        os.environ.get("NEXUS_PORTFOLIO_SOURCE_HTTP_ACCEPTANCE") != "1",
        reason="Real active workers and source rebuild require explicit opt-in",
    ),
]


def inventory_correction(source: Path, target: Path) -> None:
    """Synthetic historical correction, not newly observed customer inventory."""
    shutil.copytree(source, target)
    path = target / "opening_inventory.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fields, rows = reader.fieldnames, list(reader)
    assert fields and rows[0]["sku_id"] == "SKU-001"
    rows[0]["on_hand_qty"] = str(int(rows[0]["on_hand_qty"]) + 7)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    manifest_path = target / "source-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    matches = [item for item in manifest["sources"] if item["file_name"] == path.name]
    assert len(matches) == 1
    matches[0]["content_sha256"] = file_sha256(path)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


@pytest.mark.parametrize("capture_health", [False, True], ids=["v1", "v2"])
def test_material_event_and_reviewed_recovery_under_supervision(
    cluster: Cluster,
    broker: Broker,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capture_health: bool,
) -> None:
    repeated_replans = int(os.environ.get("NEXUS_PORTFOLIO_REPEAT_REPLANS", "0"))
    if not 0 <= repeated_replans <= 10 or (repeated_replans and not capture_health):
        pytest.fail("Repeated material replans require V2 and a count from 0 through 10")
    cooldown = ReplanPolicy().cooldown_seconds
    root = tmp_path / "application"
    config = cluster.backend(root / "evidence/initial").config
    cluster.migrate(PostgresRuntimeStore(config, runtime_root=root))
    initial = prepare_postgres_runtime(ROOT, root, config)
    scoped = replace(broker, config=replace(broker.config, locations=initial.locations()))
    ops_base, binding = durable_setup(cluster, scoped, initial, tmp_path / "operations")
    audit, audit_binding = durable_setup(cluster, scoped, initial, tmp_path / "audit", audit=True)
    ops = (
        CaptureHealthLedger(ops_base.config, health_binding=binding, max_age_seconds=10)
        if capture_health
        else RecoveryResolutionLedger(ops_base.config)
    )
    cluster.migrate(ops)
    store = PublicationRuntimeStore(config, runtime_root=root, transport=ops, binding=binding)
    cluster.migrate(store)
    watched = tmp_path / "watched-source"
    shutil.copytree(root / "evidence/initial/source", watched)
    policy = SourceWatchPolicy("inventory", str(watched.resolve()), 30)
    store = SourceWatchRuntimeStore(
        config, runtime_root=root, transport=ops, binding=binding, watch=policy
    )
    cluster.migrate(store)
    ops.publication = store
    fence = TransportFenceConfig(
        ops.config, binding, True, policy, True, True, 10 if capture_health else None
    )
    ops.enroll_topics(
        binding,
        replace(create_evidence_topic(scoped), source=binding),
        replace(create_evidence_topic(scoped), source=binding, kind="redrive_audit"),
        resolve_persona("approver", store.locations()),
    )
    manifest = tmp_path / "private-service.json"
    manifest.write_text(
        json.dumps(
            {
                "contract": "nexus.local-service.v2"
                if capture_health
                else "nexus.local-service.v1",
                **({"capture_health": {"max_age_seconds": 10}} if capture_health else {}),
                "runtime_root": str(root),
                "workflow": {"dsn": config.dsn, "schema": config.schema},
                "operations": {
                    "database": {"dsn": ops.config.dsn, "schema": ops.config.schema},
                    "binding": binding.payload(),
                },
                "audit": {
                    "database": {"dsn": audit.config.dsn, "schema": audit.config.schema},
                    "binding": audit_binding.payload(),
                },
                "source_watch": policy.payload(),
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(sys.path))
    read_service_config(manifest)
    reports: list[dict[str, Any]] = []
    exits: list[int] = []
    stop = threading.Event()
    thread = threading.Thread(
        target=lambda: exits.append(
            supervise(
                manifest,
                duration=300 + (cooldown + 120) * repeated_replans,
                interval=3,
                stale_seconds=120,
                stop=stop,
                emit=reports.append,
            )
        ),
        daemon=True,
    )
    receipt: dict[str, Any] = {
        "contract": "nexus.active-service-acceptance.v1",
        "status": "running",
        "harness_sha256": file_sha256(Path(__file__)),
        "synthetic_historical_inventory_correction": True,
        "mock_erp": True,
        "real_worker_processes": True,
        "http_server": "real_uvicorn_test_thread",
        "soak_or_performance_claimed": False,
        "additional_material_replans": repeated_replans,
        "unchanged_replan_cooldown_seconds": cooldown,
        "planner_session_renewals": 0,
        "repeated_source_intents": [],
        "worker_interval_seconds": 3,
        "worker_stale_seconds": 120,
        "default_120_second_watchdog_pass_claimed": True,
        "operations_binding": binding.payload(),
        "audit_binding": audit_binding.payload(),
        "code_sha256": {
            p.relative_to(ROOT).as_posix(): file_sha256(p)
            for p in sorted((ROOT / "src/nexus/portfolio").glob("*.py"))
        },
    }

    def until(check: Any, label: str, seconds: float = 180) -> Any:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            assert thread.is_alive() and not exits, (label, reports[-10:])
            assert not any(r["event"] in {"failed", "forced_stop"} for r in reports)
            result = check()
            if result:
                return result
            time.sleep(1)
        pytest.fail("Timed out: " + label)

    started = time.monotonic()
    thread.start()
    try:
        with (
            serving(root, config, fence) as url,
            httpx.Client(base_url=url, timeout=60, trust_env=False) as planner,
            httpx.Client(base_url=url, timeout=60, trust_env=False) as approver,
        ):
            ph, ah = login(planner, "planner"), login(approver, "approver")
            planner_login_at = time.monotonic()

            def status() -> dict[str, Any]:
                nonlocal planner_login_at
                if time.monotonic() - planner_login_at >= 300:
                    ph.clear()
                    ph.update(login(planner, "planner"))
                    planner_login_at = time.monotonic()
                    receipt["planner_session_renewals"] += 1
                return response(planner.get("/api/runtime/status"))

            until(lambda: status()["source_watch"]["reason"] == "fresh", "initial observation")
            if capture_health:
                until(
                    lambda: status()["capture_health"]["reason"] == "caught_up_at_observation",
                    "initial capture observation",
                )
            old = response(planner.get("/api/recommendations"))["items"][0]
            old_url = "/api/recommendations/" + old["id"]
            old_body = {"fingerprint": old["fingerprint"]}
            response(approver.post(old_url + "/approve", json=old_body, headers=ah))
            scoped.produce(b"malformed synthetic inventory envelope\x00\xff")
            until(
                lambda: any(r.get("outcome") == "blocked_schema_poison" for r in reports),
                "dispatcher safely blocked on poison",
            )
            inspected = response(planner.get("/api/runtime/recovery/pending/0"))["pending"]
            assert inspected and inspected["offset"] == 0
            corrected = tmp_path / "corrected-source"
            inventory_correction(watched, corrected)
            watched.rename(tmp_path / "initial-source-preserved")
            shutil.copytree(corrected, watched)
            bundle = SourceBundle.capture(watched)
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
                payload={"source_snapshot_sha256": bundle.source_sha256},
            )
            event_path = tmp_path / "inventory-event.json"
            event_path.write_text(event.model_dump_json(), encoding="utf-8")
            queued = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "nexus.cli",
                    "portfolio",
                    "service",
                    "enqueue-source",
                    str(manifest),
                    str(event_path),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            assert queued.returncode == 0, "Source-intent CLI failed (private stderr withheld)"
            receipt["source_intent"] = json.loads(queued.stdout)
            until(
                lambda: (
                    ops.verify_history(binding)["captured"] == [2, 0]
                    and audit.verify_history(audit_binding)["captured"] == [2, 0]
                ),
                "both groups captured poison and replacement",
            )
            response(
                approver.post(
                    old_url + "/execute",
                    json=old_body,
                    headers={**ah, "Idempotency-Key": "old-blocked"},
                ),
                409,
            )
            assert response(planner.get("/api/overview"))["summary"]["effect_count"] == 0
            retained = response(
                planner.post(
                    "/api/runtime/recovery/proposals",
                    headers=ph,
                    json={
                        "partition": 0,
                        "inbox_sha256": inspected["inbox_record_hash"],
                        "publication_id": str(event_id),
                        "reason": "Synthetic inventory correction",
                    },
                )
            )
            recovery_url = "/api/runtime/recovery/" + retained["request_id"]
            review = {
                "request_sha256": retained["request_sha256"],
                "decision_id": str(uuid4()),
                "action": "approve",
                "reason": "Independent correction review",
                "ttl_seconds": 900,
            }
            response(planner.post(recovery_url + "/review", json=review, headers=ph), 403)
            response(approver.post(recovery_url + "/review", json=review, headers=ah))
            resolution = {k: review[k] for k in ("request_sha256", "decision_id")}
            denied = response(
                approver.post(recovery_url + "/resolve", json=resolution, headers=ah), 409
            )
            assert denied["error"]["code"] == "recovery_evidence_required"
            response(planner.post(recovery_url + "/evidence", json=resolution, headers=ph), 403)
            response(approver.post(recovery_url + "/evidence", json=resolution), 403)
            evidence = response(
                approver.post(recovery_url + "/evidence", json=resolution, headers=ah)
            )
            assert len(evidence["intent_ids"]) == 2
            assert (
                response(approver.post(recovery_url + "/evidence", json=resolution, headers=ah))
                == evidence
            )

            def resolve_when_delivered() -> Any:
                result = approver.post(recovery_url + "/resolve", json=resolution, headers=ah)
                if result.status_code == 409:
                    assert result.json()["error"]["code"] == "recovery_evidence_required"
                    return None
                return response(result)

            receipt["resolution"] = until(
                resolve_when_delivered, "real recovery evidence publication"
            )
            until(lambda: status()["archived_generations"] == 1, "worker rebuilt corrected source")
            until(lambda: status()["source_watch"]["reason"] == "fresh", "replacement observation")
            if capture_health:
                until(
                    lambda: status()["capture_health"]["reason"] == "caught_up_at_observation",
                    "replacement capture observation",
                )
            for repetition in range(repeated_replans):
                previous_generation = status()["archived_generations"]
                corrected = tmp_path / f"repeated-correction-{repetition}"
                inventory_correction(watched, corrected)
                watched.rename(tmp_path / f"preserved-source-{repetition}")
                shutil.copytree(corrected, watched)
                bundle = SourceBundle.capture(watched)
                event_id = uuid4()
                instant = datetime.now(UTC)
                repeated_event = EventEnvelope(
                    event_id=event_id,
                    event_type="inventory.adjusted",
                    source="nexus.source-snapshot.v1",
                    subject_id="portfolio",
                    occurred_at=instant,
                    ingested_at=instant,
                    idempotency_key=str(event_id),
                    payload={"source_snapshot_sha256": bundle.source_sha256},
                )
                event_path = tmp_path / f"repeated-event-{repetition}.json"
                event_path.write_text(repeated_event.model_dump_json(), encoding="utf-8")
                queued = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "nexus.cli",
                        "portfolio",
                        "service",
                        "enqueue-source",
                        str(manifest),
                        str(event_path),
                    ],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                )
                assert queued.returncode == 0, "Repeated source-intent CLI failed"
                receipt["repeated_source_intents"].append(json.loads(queued.stdout))
                until(
                    lambda generation=previous_generation: (
                        status()["archived_generations"] == generation + 1
                    ),
                    "successive material source rebuild",
                    seconds=cooldown + 120,
                )
                until(
                    lambda: status()["capture_health"]["reason"] == "caught_up_at_observation",
                    "successive capture observation",
                )
                until(
                    lambda: status()["source_watch"]["reason"] == "fresh",
                    "successive source observation",
                )
                expected_offsets = [3 + repetition, 0]
                until(
                    lambda offsets=expected_offsets: (
                        ops.verify_history(binding)["delivered"] == offsets
                        and audit.verify_history(audit_binding)["captured"] == offsets
                    ),
                    "both groups retained successive correction",
                )
                print(f"Completed successive source correction {repetition + 1}", flush=True)
            if repeated_replans:
                ah = login(approver, "approver")
            current = next(
                item
                for item in response(planner.get("/api/recommendations"))["items"]
                if item["state"] == "pending"
            )
            assert current["fingerprint"] != old["fingerprint"]
            endpoint = "/api/recommendations/" + current["id"]
            body = {"fingerprint": current["fingerprint"]}
            eh = {**ah, "Idempotency-Key": "active-workers-order"}
            response(approver.post(endpoint + "/execute", json=body, headers=eh), 409)
            response(approver.post(endpoint + "/approve", json=body, headers=ah))
            for _ in range(2):
                response(approver.post(endpoint + "/execute", json=body, headers=eh))
            response(planner.post(endpoint + "/outcome", json=body, headers=ph), 425)
            time.sleep(DEMO_MATURITY_SECONDS + 0.1)
            observed = response(planner.post(endpoint + "/outcome", json=body, headers=ph))
            assert not observed["outcome"]["causal_business_impact_claim"]
            final = status()
            assert (
                final["event_count"] == 1 + repeated_replans
                and final["source_publication"]["pending_admission"] == 0
            )
            assert response(planner.get("/api/overview"))["summary"]["effect_count"] == 1
            assert ops.verify_history(binding)["delivered"] == [2 + repeated_replans, 0]
            receipt.update(
                status="passed",
                runtime_status=final,
                source_sha256=bundle.source_sha256,
                effect_count=1,
                evidence_intents=evidence,
                elapsed_seconds=time.monotonic() - started,
            )
            if capture_health:
                another = next(
                    item
                    for item in response(planner.get("/api/recommendations"))["items"]
                    if item["state"] == "pending"
                )
                another_url = "/api/recommendations/" + another["id"]
                another_body = {"fingerprint": another["fingerprint"]}
                response(approver.post(another_url + "/approve", json=another_body, headers=ah))
                stop.set()
                thread.join(timeout=90)
                assert exits == [0] and not thread.is_alive()
                time.sleep(10.1)
                stopped = status()["capture_health"]
                assert stopped["reason"] == "capture_observation_expired"
                refused = response(
                    approver.post(
                        another_url + "/execute",
                        json=another_body,
                        headers={**ah, "Idempotency-Key": "stopped-workers"},
                    ),
                    409,
                )
                assert refused["error"]["code"] == "capture_observation_expired"
                assert response(planner.get("/api/overview"))["summary"]["effect_count"] == 1
                receipt["stopped_capture_action_refused"] = {
                    "health": stopped,
                    "execution": refused,
                }
    finally:
        stop.set()
        thread.join(timeout=90)
        receipt["supervisor_exits"] = exits
        receipt["supervisor_stopped"] = not thread.is_alive()
        receipt["planning_stage_timings"] = [
            item["timings"] for item in reports if item.get("role") == "plan" and "timings" in item
        ]
        (tmp_path / "active-supervisor-reports.json").write_text(
            json.dumps(reports, indent=2), encoding="utf-8"
        )
        (tmp_path / "active-service-receipt.json").write_text(
            json.dumps(receipt, indent=2), encoding="utf-8"
        )
        assert not thread.is_alive(), "Test-owned supervisor did not stop"
    assert exits == [0], reports[-10:]
    assert len(receipt["planning_stage_timings"]) == 1 + repeated_replans
    assert all(row["dbt"] > 0 for row in receipt["planning_stage_timings"])
    assert len([r for r in reports if r["event"] == "worker_exit" and r["exit_code"] == 0]) == 7
