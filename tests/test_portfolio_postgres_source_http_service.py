"""Opt-in real pipeline -> PostgreSQL -> authenticated loopback HTTP acceptance.

No mocked pipeline/store/clock/transport. Synthetic personas and mock ERP remain.
Optional broker variant proves real delivery; not browser, process-crash, load or soak proof.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import uvicorn
from test_portfolio_broker_runtime_service import Broker, poll
from test_portfolio_broker_runtime_service import broker as broker
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_recovery_publisher_service import create_evidence_topic
from test_portfolio_transport_consumer_service import capture as durable_capture
from test_portfolio_transport_consumer_service import setup as durable_setup

from nexus.contracts import ActorRole, EventEnvelope
from nexus.portfolio.api import create_app
from nexus.portfolio.auth import Actor, resolve_persona
from nexus.portfolio.broker_runtime import RuntimeBrokerConsumer
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger
from nexus.portfolio.runtime_acceptance import _changed_source
from nexus.portfolio.runtime_pipeline import runtime_source_fingerprint
from nexus.portfolio.runtime_service import prepare_postgres_runtime, run_runtime_worker
from nexus.portfolio.service_cli import Step, run_step
from nexus.portfolio.service_config import read_service_config
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.source_freshness import SourceWatchPolicy
from nexus.portfolio.source_publisher import SourceOutboxPublisher
from nexus.portfolio.source_watch_runtime import SourceWatchRuntimeStore
from nexus.portfolio.store import DEMO_MATURITY_SECONDS, digest
from nexus.portfolio.transport_consumer import DurableBrokerConsumer
from nexus.portfolio.transport_runtime import TransportFenceConfig, TransportRuntimeStore
from nexus.storage import file_sha256

pytestmark = [
    pytest.mark.portfolio_service,
    pytest.mark.skipif(
        os.environ.get("NEXUS_PORTFOLIO_SOURCE_HTTP_ACCEPTANCE") != "1",
        reason="Real two-build HTTP acceptance requires explicit additional opt-in",
    ),
]
ROOT = Path(__file__).resolve().parents[1]


@contextmanager
def serving(root: Path, config: Any, fence: TransportFenceConfig | None = None) -> Iterator[str]:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        app = create_app(root, ROOT, pipeline="runtime", postgres=config, transport_fence=fence)
        server = uvicorn.Server(
            uvicorn.Config(
                app, log_level="warning", proxy_headers=False, timeout_graceful_shutdown=5
            )
        )
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 15
            while not server.started:
                assert thread.is_alive() and time.monotonic() < deadline, "HTTP startup failed"
                time.sleep(0.05)
            yield f"http://127.0.0.1:{port}"
        finally:
            server.should_exit = True
            thread.join(timeout=15)
            assert not thread.is_alive(), "Test-owned HTTP server did not stop"


def response(value: httpx.Response, status: int = 200) -> Any:
    assert value.status_code == status, value.text
    return value.json()


def login(client: httpx.Client, persona: str) -> dict[str, str]:
    return {
        "X-CSRF-Token": response(client.post("/api/session", json={"persona": persona}))[
            "csrf_token"
        ]
    }


@pytest.mark.parametrize(
    "transport",
    ["http", "broker", "durable_broker", "published_broker", "watched_broker", "recovered_broker"],
)
def test_real_sources_replan_through_authenticated_postgres_http(
    cluster: Cluster, tmp_path: Path, transport: str, request: pytest.FixtureRequest
) -> None:
    recovering = transport == "recovered_broker"
    publication = transport in {"published_broker", "watched_broker", "recovered_broker"}
    durable = transport in {
        "durable_broker",
        "published_broker",
        "watched_broker",
        "recovered_broker",
    }
    selected_broker: Broker | None = (
        request.getfixturevalue("broker") if transport == "broker" or durable else None
    )
    root = tmp_path / "application"
    config = cluster.backend(root / "evidence" / "initial").config
    cluster.migrate(PostgresRuntimeStore(config, runtime_root=root))
    started = time.perf_counter()
    receipt: dict[str, Any] = {
        "contract_version": "nexus.postgres-source-http-acceptance.v1",
        "status": "running",
        "started_at": datetime.now(UTC).isoformat(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "schema": config.schema,
        "synthetic": True,
        "real_http": True,
        "mock_erp": True,
        "real_broker": selected_broker is not None,
        "soak_claimed": False,
        "event_transport": transport,
        "code_sha256": {
            p.relative_to(ROOT).as_posix(): file_sha256(p)
            for p in sorted((ROOT / "src/nexus/portfolio").glob("*.py"))
        },
    }
    try:
        build_started = time.perf_counter()
        store = prepare_postgres_runtime(ROOT, root, config)
        receipt["initial_build_seconds"] = time.perf_counter() - build_started
        initial = store.seed()
        receipt["initial_seed_sha256"] = digest(initial)
        fence = None
        if durable:
            assert selected_broker is not None
            scoped_broker = replace(
                selected_broker, config=replace(selected_broker.config, locations=store.locations())
            )
            ops, ops_binding = durable_setup(cluster, scoped_broker, store, tmp_path / "ops-ledger")
            audit, audit_binding = durable_setup(
                cluster, scoped_broker, store, tmp_path / "audit-ledger", audit=True
            )
            fence = TransportFenceConfig(ops.config, ops_binding, publication)
            if recovering:
                ops = RecoveryResolutionLedger(ops.config)
                receipt["recovery_migration"] = cluster.migrate(ops)
                fence = replace(fence, recovery_journal=True, recovery_resolution=True)
            store_type = PublicationRuntimeStore if publication else TransportRuntimeStore
            store = store_type(config, runtime_root=root, transport=ops, binding=ops_binding)
            receipt["transport_fence_migration"] = cluster.migrate(store)
            if transport in {"watched_broker", "recovered_broker"}:
                watched_source = tmp_path / "watched-source"
                shutil.copytree(root / "evidence/initial/source", watched_source)
                policy = SourceWatchPolicy("inventory", str(watched_source.resolve()), 30)
                fence = replace(fence, source_watch=policy)
                store = SourceWatchRuntimeStore(
                    config, runtime_root=root, transport=ops, binding=ops_binding, watch=policy
                )
                receipt["source_watch_migration"] = cluster.migrate(store)
                source_observer = Actor(
                    "source-observer", "Source observer", ActorRole.SYSTEM, store.locations()
                )
            if recovering:
                assert isinstance(ops, RecoveryResolutionLedger)
                assert isinstance(store, PublicationRuntimeStore)
                ops.publication = store
                service_path = tmp_path / "private-service.json"
                service_path.write_text(
                    json.dumps(
                        {
                            "contract": "nexus.local-service.v1",
                            "runtime_root": str(root),
                            "workflow": {"dsn": config.dsn, "schema": config.schema},
                            "operations": {
                                "database": {"dsn": ops.config.dsn, "schema": ops.config.schema},
                                "binding": ops_binding.payload(),
                            },
                            "audit": {
                                "database": {
                                    "dsn": audit.config.dsn,
                                    "schema": audit.config.schema,
                                },
                                "binding": audit_binding.payload(),
                            },
                            "source_watch": policy.payload(),
                        }
                    ),
                    encoding="utf-8",
                )
                service = read_service_config(service_path)

                def service_command(*args: str) -> dict[str, Any]:
                    script = (
                        "import sys;sys.path=" + repr(sys.path) + ";"
                        "from nexus.portfolio.service_cli import app;app()"
                    )
                    completed = subprocess.run(
                        [sys.executable, "-c", script, *args],
                        cwd=ROOT,
                        capture_output=True,
                        text=True,
                        timeout=120,
                        check=False,
                        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
                    )
                    if args[0] == "supervise":
                        reports = [json.loads(line) for line in completed.stdout.splitlines()]
                        (tmp_path / "supervisor-reports.json").write_text(
                            json.dumps(reports, indent=2), encoding="utf-8"
                        )
                        assert completed.returncode == 0, reports[-10:]
                        return {
                            "status": reports[-1],
                            "spawned": [r for r in reports if r["event"] == "spawned"],
                            "exits": [r for r in reports if r["event"] == "worker_exit"],
                            "report_count": len(reports),
                            "reports_sha256": file_sha256(tmp_path / "supervisor-reports.json"),
                        }
                    assert completed.returncode == 0, "Service subprocess failed"
                    return json.loads(completed.stdout)

                receipt["service_commands"] = {
                    "check": service_command("check", str(service_path)),
                    "claim": "local_pinned_commands_not_full_supervisor_or_compose",
                }
        with (
            serving(root, config, fence) as url,
            httpx.Client(base_url=url, timeout=60, trust_env=False) as planner,
            httpx.Client(base_url=url, timeout=60, trust_env=False) as approver,
        ):
            response(planner.get("/api/overview"), 401)
            ph, ah = login(planner, "planner"), login(approver, "approver")
            for path in (
                "/readyz",
                "/api/overview",
                "/api/recommendations",
                "/api/audit",
                "/api/runtime/status",
                "/api/health",
                "/api/learning",
            ):
                response(planner.get(path))
            original = response(planner.get("/api/recommendations"))["items"][0]
            old_url = "/api/recommendations/" + original["id"]
            body = {"fingerprint": original["fingerprint"]}
            response(planner.post(old_url + "/approve", json=body, headers=ph), 403)
            if isinstance(store, SourceWatchRuntimeStore):
                missing = response(approver.post(old_url + "/approve", json=body, headers=ah), 409)
                assert missing["error"]["code"] == "source_observation_missing"
                receipt["source_watch"] = {"initial_missing": missing}
                assert store.poll_source(source_observer)["reason"] == "fresh"
            response(approver.post(old_url + "/approve", json=body, headers=ah))
            changed = _changed_source(root / "evidence/initial/source", tmp_path / "changed-source")
            if isinstance(store, SourceWatchRuntimeStore):
                watched_source.rename(tmp_path / "watched-initial-preserved")
                shutil.copytree(changed, watched_source)
                assert store.poll_source(source_observer)["reason"] == "source_changed"
                changed_refusal = response(
                    approver.post(old_url + "/approve", json=body, headers=ah), 409
                )
                assert changed_refusal["error"]["code"] == "source_changed"
                receipt["source_watch"]["changed_before_publication"] = changed_refusal
            event = {
                "event_id": str(uuid4()),
                "event_type": "demand.spike",
                "occurred_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
                "source_snapshot_sha256": runtime_source_fingerprint(changed),
            }
            response(planner.post("/api/runtime/events", json=event), 403)
            if selected_broker is None:
                response(planner.post("/api/runtime/events", json=event, headers=ph))
                assert (
                    response(planner.post("/api/runtime/events", json=event, headers=ph))[
                        "duplicate"
                    ]
                    is True
                )
            else:
                broker_config = replace(selected_broker.config, locations=store.locations())
                instant = datetime.fromisoformat(event["occurred_at"])
                envelope = EventEnvelope(
                    event_id=UUID(event["event_id"]),
                    event_type=event["event_type"],
                    source="nexus.source-snapshot.v1",
                    subject_id="portfolio",
                    occurred_at=instant,
                    ingested_at=instant,
                    idempotency_key=event["event_id"],
                    payload={"source_snapshot_sha256": event["source_snapshot_sha256"]},
                )
                if durable:
                    capture_ops = DurableBrokerConsumer(ops, ops_binding)
                    capture_audit = DurableBrokerConsumer(audit, audit_binding)
                    try:
                        if publication:
                            assert isinstance(store, PublicationRuntimeStore)
                            publisher_actor = Actor(
                                "source-publisher",
                                "Source publisher",
                                ActorRole.SYSTEM,
                                store.locations(),
                            )
                            bundle = SourceBundle.capture(changed)
                            queued = store.enqueue_source(bundle, envelope, publisher_actor)
                            publication_status = response(planner.get("/api/runtime/status"))[
                                "source_publication"
                            ]
                            assert (
                                publication_status["accepted"] == 1
                                and publication_status["pending_admission"] == 1
                            )
                            assert publication_status["broker_acknowledged"] == 0
                            assert (
                                store.enqueue_source(bundle, envelope, publisher_actor)["duplicate"]
                                is True
                            )
                            rejected = response(
                                approver.post(old_url + "/approve", json=body, headers=ah), 409
                            )
                            assert rejected["error"]["code"] == "publication_pending"
                            rejected_execution = response(
                                approver.post(
                                    old_url + "/execute",
                                    json=body,
                                    headers={**ah, "Idempotency-Key": "publication-pending"},
                                ),
                                409,
                            )
                            assert rejected_execution["error"]["code"] == "publication_pending"
                            # Preserve test-owned input but make its original path unavailable.
                            changed.rename(tmp_path / "producer-source-preserved")
                            assert not changed.exists()

                            if recovering:
                                assert isinstance(ops, RecoveryResolutionLedger)
                                selected_broker.produce(b"malformed real source envelope\x00\xff")
                                durable_capture(capture_ops, 1)
                                durable_capture(capture_audit, 1)
                                inspected = response(planner.get("/api/runtime/recovery/pending/0"))
                                original_hash = inspected["pending"]["inbox_record_hash"]
                                assert inspected["pending"]["offset"] == 0
                                assert "physical" not in inspected["pending"]
                                assert "base64" not in json.dumps(inspected)
                                reviewer = resolve_persona("approver", store.locations())
                                proposal_body = {
                                    "partition": 0,
                                    "inbox_sha256": original_hash,
                                    "publication_id": str(envelope.event_id),
                                    "reason": "Correct envelope using retained changed source",
                                }
                                response(
                                    planner.post(
                                        "/api/runtime/recovery/proposals", json=proposal_body
                                    ),
                                    403,
                                )
                                response(
                                    approver.post(
                                        "/api/runtime/recovery/proposals",
                                        json=proposal_body,
                                        headers=ah,
                                    ),
                                    403,
                                )
                                response(
                                    planner.post(
                                        "/api/runtime/recovery/proposals",
                                        json={**proposal_body, "source_path": str(changed)},
                                        headers=ph,
                                    ),
                                    422,
                                )
                                retained = response(
                                    planner.post(
                                        "/api/runtime/recovery/proposals",
                                        json=proposal_body,
                                        headers=ph,
                                    )
                                )
                                recovery_url = "/api/runtime/recovery/" + retained["request_id"]
                                review_body = {
                                    "request_sha256": retained["request_sha256"],
                                    "decision_id": str(uuid4()),
                                    "action": "approve",
                                    "reason": "Independent canonical replacement review",
                                    "ttl_seconds": 900,
                                }
                                response(
                                    approver.post(recovery_url + "/review", json=review_body), 403
                                )
                                response(
                                    planner.post(
                                        recovery_url + "/review", json=review_body, headers=ph
                                    ),
                                    403,
                                )
                                decision = response(
                                    approver.post(
                                        recovery_url + "/review", json=review_body, headers=ah
                                    )
                                )
                                assert response(approver.get(recovery_url + "/review"))[
                                    "review_valid"
                                ]
                                dlq_topic = replace(
                                    create_evidence_topic(scoped_broker), source=ops_binding
                                )
                                audit_topic = replace(
                                    create_evidence_topic(scoped_broker),
                                    source=ops_binding,
                                    kind="redrive_audit",
                                )
                                ops.enroll_topics(ops_binding, dlq_topic, audit_topic, reviewer)
                                ops.enqueue_evidence(
                                    ops_binding,
                                    retained["request_id"],
                                    retained["request_sha256"],
                                    decision["decision_id"],
                                    reviewer,
                                )
                                recovery_observations = [
                                    run_step(service, Step.recovery_publish)["result"],
                                    run_step(service, Step.recovery_publish)["result"],
                                ]
                                assert all(recovery_observations)
                                receipt["governed_recovery"] = {
                                    "operator_inspection": inspected,
                                    "request": retained,
                                    "decision": decision,
                                    "dead_letter_topic": dlq_topic.payload(),
                                    "audit_topic": audit_topic.payload(),
                                    "observations": recovery_observations,
                                    "original_hash": original_hash,
                                    "recovery_review_boundary": (
                                        "authenticated_HTTP_proposal_review_resolution_synthetic_personas;"
                                        "publisher_trusted_service_calls;no_review_UI"
                                    ),
                                }

                            def crash_after_ack() -> None:
                                raise RuntimeError("ack before receipt crash")

                            with pytest.raises(RuntimeError, match="ack before receipt crash"):
                                SourceOutboxPublisher(store).publish_once(failpoint=crash_after_ack)
                            acknowledged = SourceOutboxPublisher(store).publish_once()
                            assert acknowledged is not None
                            publication_status = response(planner.get("/api/runtime/status"))[
                                "source_publication"
                            ]
                            assert (
                                publication_status["broker_acknowledged"] == 1
                                and publication_status["pending_admission"] == 1
                            )
                            assert SourceOutboxPublisher(store).publish_once() is None
                            assert response(
                                approver.post(old_url + "/approve", json=body, headers=ah), 409
                            )["error"]["code"] == (
                                "transport_pending" if recovering else "publication_pending"
                            )
                            receipt["source_publication"] = {
                                "intent": queued,
                                "ack": acknowledged,
                                "pre_capture_approval": rejected,
                                "pre_capture_execution": rejected_execution,
                                "original_source_path_unavailable": True,
                                "post_ack_receipt_failure_retried": True,
                            }
                        else:
                            selected_broker.produce(envelope.model_dump_json().encode())
                            selected_broker.produce(envelope.model_dump_json().encode())
                        durable_capture(capture_ops, 2)
                        durable_capture(capture_audit, 2)
                        blocked_approval = response(
                            approver.post(old_url + "/approve", json=body, headers=ah), 409
                        )
                        blocked_execution = response(
                            approver.post(
                                old_url + "/execute",
                                json=body,
                                headers={**ah, "Idempotency-Key": "transport-pending"},
                            ),
                            409,
                        )
                        assert blocked_approval["error"]["code"] == "transport_pending"
                        assert blocked_execution["error"]["code"] == "transport_pending"
                        assert response(planner.get(old_url))["state"] == "approved"
                        assert (
                            response(planner.get("/api/overview"))["summary"]["effect_count"] == 0
                        )
                        receipt["transport_fence"] = {
                            "pending_approval": blocked_approval,
                            "pending_execution": blocked_execution,
                            "workflow_unchanged_until_admission": True,
                        }
                        if recovering:
                            resolution_body = {
                                "request_sha256": retained["request_sha256"],
                                "decision_id": decision["decision_id"],
                            }
                            response(
                                planner.post(
                                    recovery_url + "/resolve", json=resolution_body, headers=ph
                                ),
                                403,
                            )
                            resolution = response(
                                approver.post(
                                    recovery_url + "/resolve", json=resolution_body, headers=ah
                                )
                            )
                            assert ops.verify_history(ops_binding)["delivered"] == [1, 0]
                            assert response(planner.get("/api/runtime/status"))["event_count"] == 0
                            still_blocked = response(
                                approver.post(old_url + "/approve", json=body, headers=ah), 409
                            )
                            assert still_blocked["error"]["code"] == "transport_pending"
                            receipt["governed_recovery"]["resolution"] = resolution
                            receipt["governed_recovery"]["post_quarantine_approval_denied"] = (
                                still_blocked
                            )
                        first_admission = (
                            run_step(service, Step.dispatch)["result"]
                            if recovering
                            else ops.dispatch_once(ops_binding, store.control)
                        )
                        replay_admission = (
                            run_step(service, Step.dispatch)["result"]
                            if recovering
                            else ops.dispatch_once(ops_binding, store.control)
                        )
                        assert first_admission is not None and replay_admission is not None
                        assert first_admission["admission"]["duplicate"] is False
                        assert replay_admission["admission"]["duplicate"] is True
                        receipt["durable_broker"] = {
                            "operations_binding": ops_binding.payload(),
                            "audit_binding": audit_binding.payload(),
                            "operations_state": ops.verify_history(ops_binding),
                            "audit_state": audit.verify_history(audit_binding),
                            "first_admission": first_admission,
                            "duplicate_admission": replay_admission,
                        }
                    finally:
                        capture_ops.close()
                        capture_audit.close()
                else:
                    bridge = RuntimeBrokerConsumer(
                        store.control, broker_config, selected_broker.consumer()
                    )
                    try:
                        selected_broker.produce(envelope.model_dump_json().encode())
                        first_delivery = poll(bridge)
                        assert first_delivery["admission"]["duplicate"] is False
                        selected_broker.produce(envelope.model_dump_json().encode())
                        replay_delivery = poll(bridge)
                        assert replay_delivery["admission"]["duplicate"] is True
                        receipt["broker"] = {
                            "cluster_id": selected_broker.cluster_id,
                            "topic": broker_config.topic,
                            "group": broker_config.group_id,
                            "locations": list(broker_config.locations),
                            "delivery": first_delivery,
                            "duplicate_delivery": replay_delivery,
                        }
                    finally:
                        bridge.close()
            assert response(planner.get(old_url))["state"] == "invalidated"
            response(
                approver.post(
                    old_url + "/execute",
                    json=body,
                    headers={**ah, "Idempotency-Key": "source-http-old"},
                ),
                409,
            )
            build_started = time.perf_counter()
            result = (
                service_command("step", str(service_path), "plan")["result"]
                if recovering
                else run_runtime_worker(ROOT, store, source_dir=None if publication else changed)
            )
            if recovering:
                receipt["service_commands"]["plan"] = result
            assert result["status"] == "completed", result
            receipt["replacement_build_seconds"] = time.perf_counter() - build_started
            replacement = store.seed()
            receipt["replacement_seed_sha256"] = digest(replacement)
            assert (
                replacement["forecasts"][0]["points"][0]["q50"]
                == initial["forecasts"][0]["points"][0]["q50"] + 3
            )
            current = next(
                item
                for item in response(planner.get("/api/recommendations"))["items"]
                if item["state"] == "pending"
            )
            endpoint = "/api/recommendations/" + current["id"]
            body = {"fingerprint": current["fingerprint"]}
            eh = {**ah, "Idempotency-Key": "source-http-new"}
            response(approver.post(endpoint + "/execute", json=body, headers=eh), 409)
            if isinstance(store, SourceWatchRuntimeStore):
                watched_source.rename(tmp_path / "watched-current-preserved")
                assert store.poll_source(source_observer)["reason"] == "source_read_failed"
                outage = response(approver.post(endpoint + "/approve", json=body, headers=ah), 409)
                assert outage["error"]["code"] == "source_read_failed"
                (tmp_path / "watched-current-preserved").rename(watched_source)
                assert store.poll_source(source_observer)["reason"] == "fresh"
                receipt["source_watch"]["unavailable_source"] = outage
                receipt["source_watch"]["recovered_status"] = response(
                    planner.get("/api/runtime/status")
                )["source_watch"]
            response(approver.post(endpoint + "/approve", json=body, headers=ah))
            for _ in range(2):
                response(approver.post(endpoint + "/execute", json=body, headers=eh))
            assert response(planner.get("/api/overview"))["summary"]["effect_count"] == 1
            response(planner.post(endpoint + "/outcome", json=body, headers=ph), 425)
            time.sleep(DEMO_MATURITY_SECONDS + 0.1)
            observed = response(planner.post(endpoint + "/outcome", json=body, headers=ph))
            assert observed["outcome"]["causal_business_impact_claim"] is False
            for action in ("trigger", "resolve"):
                response(planner.post("/api/incidents/demo", json={"action": action}, headers=ph))
            response(
                planner.post("/api/copilot", json={"question": "Which model is the champion?"})
            )
            assert response(planner.get("/api/audit"))["integrity"] == "verified"
            cookies = dict(planner.cookies)
        if isinstance(store, SourceWatchRuntimeStore):
            with store.read_snapshot() as unit:
                last_probe, last_sequence = store._latest_observation(unit)
            assert last_probe is not None
            # Real elapsed time, no clock patch: restart must not renew an expired receipt.
            time.sleep(
                max(0, last_probe.started_at + store.watch.max_age_seconds + 0.1 - time.time())
            )
        # Graceful ASGI restart, not an OS process crash; persisted credentials reused.
        with (
            serving(root, config, fence) as url,
            httpx.Client(base_url=url, cookies=cookies, timeout=60, trust_env=False) as restored,
        ):
            assert response(restored.get("/api/overview"))["summary"]["effect_count"] == 1
            assert response(restored.get(endpoint))["outcome"] is not None
            status = response(restored.get("/api/runtime/status"))
            assert status["archived_generations"] == 1 and status["event_count"] == 1
            if publication:
                assert status["source_publication"]["accepted"] == 1
                assert status["source_publication"]["business_admitted"] == 1
                assert status["source_publication"]["pending_admission"] == 0
                receipt["source_publication"]["restored_status"] = status["source_publication"]
            if isinstance(store, SourceWatchRuntimeStore):
                assert status["source_watch"]["reason"] == "source_observation_expired"
                assert status["source_watch"]["observation_sequence"] == last_sequence
                receipt["source_watch"]["restored_status"] = status["source_watch"]
                assert store.poll_source(source_observer)["reason"] == "fresh"
                refreshed = response(restored.get("/api/runtime/status"))["source_watch"]
                assert refreshed["reason"] == "fresh"
                assert refreshed["observation_sequence"] == last_sequence + 1
                receipt["source_watch"]["fresh_after_new_probe"] = refreshed
            assert response(restored.get("/api/audit"))["integrity"] == "verified"
            if recovering:
                assert isinstance(ops, RecoveryResolutionLedger)
                receipt["service_commands"]["audit_idle"] = service_command(
                    "step", str(service_path), "capture-audit"
                )
                assert receipt["service_commands"]["audit_idle"]["result"] is None
                reopened = RecoveryResolutionLedger(ops.config, publication=store)
                assert reopened.resolve(
                    ops_binding,
                    retained["request_id"],
                    retained["request_sha256"],
                    decision["decision_id"],
                    reviewer,
                )["duplicate"]
                for durable_ledger, binding in ((reopened, ops_binding), (audit, audit_binding)):
                    restarted_capture = DurableBrokerConsumer(durable_ledger, binding)
                    try:
                        assert restarted_capture.poll_batch() is None
                    finally:
                        restarted_capture.close()
                receipt["governed_recovery"]["restored_operations"] = reopened.verify_history(
                    ops_binding
                )
                receipt["governed_recovery"]["restored_audit"] = audit.verify_history(audit_binding)
                assert receipt["governed_recovery"]["restored_operations"]["delivered"] == [3, 0]
                assert receipt["governed_recovery"]["restored_audit"]["captured"] == [3, 0]
                supervision = service_command(
                    "supervise",
                    str(service_path),
                    "--duration",
                    "25",
                    "--interval",
                    "1",
                    "--shutdown-seconds",
                    "30",
                )
                assert supervision["status"]["successful"]
                assert len(supervision["spawned"]) == len(supervision["exits"]) == 7
                assert all(
                    r["exit_code"] == 0 and r["completed_steps"] > 0 for r in supervision["exits"]
                )
                receipt["service_supervision"] = supervision
                assert response(restored.get("/api/overview"))["summary"]["effect_count"] == 1
                assert ops.verify_history(ops_binding)["delivered"] == [3, 0]
                assert audit.verify_history(audit_binding)["captured"] == [3, 0]
        assert not (root / "portfolio.sqlite3").exists()
        receipt["status"] = "pass"
    finally:
        if receipt["status"] != "pass":
            receipt["status"] = "fail"
        receipt["elapsed_seconds"] = time.perf_counter() - started
        (tmp_path / "source-http-receipt.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8"
        )
