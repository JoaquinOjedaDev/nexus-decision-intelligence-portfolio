"""Guarded native resolution tests with constructed broker evidence, not live recovery."""

import base64
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import actor
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime
from test_portfolio_publication_runtime_service import publisher as source_actor
from test_portfolio_recovery_ledger_service import pending as pending
from test_portfolio_recovery_ledger_service import retain, review
from test_portfolio_recovery_publication_service import enqueue, worker
from test_portfolio_recovery_publication_service import publisher as publisher
from test_portfolio_transport_ledger_service import binding as binding
from test_portfolio_transport_ledger_service import ledger as ledger

from nexus.contracts import EventEnvelope
from nexus.portfolio.postgres_backend import PostgresBackendError
from nexus.portfolio.publication_runtime import PublicationRuntimeStore
from nexus.portfolio.recovery_publisher import RecoveryTopic
from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger
from nexus.portfolio.store import StoreError, canonical, digest
from nexus.portfolio.transport_ledger import TransportRecord
from nexus.portfolio.transport_recovery import RuntimeRedriveRequest

pytestmark = pytest.mark.portfolio_service


@pytest.fixture
def recovery(ledger, runtime, cluster, binding, clock):
    preliminary = RecoveryResolutionLedger(ledger.config, clock=lambda: clock[0])
    assert cluster.migrate(preliminary)["version"] == 11
    store = PublicationRuntimeStore(
        runtime.config,
        transport=preliminary,
        binding=binding,
        runtime_root=runtime.runtime_root,
        clock=lambda: clock[0],
    )
    cluster.migrate(store)
    return RecoveryResolutionLedger(ledger.config, publication=store, clock=lambda: clock[0])


@pytest.fixture
def prepared(recovery, binding, pending, clock, request):
    raw = base64.b64decode(json.loads(pending.request.payload)["replacement_base64"])
    replacement = canonical(json.loads(raw)).encode() if getattr(request, "param", True) else raw
    pending.request = RuntimeRedriveRequest.create(
        pending.dead,
        binding,
        pending.physical,
        pending.original_hash,
        pending.bundle,
        replacement,
        operator=actor(),
        reason="Review exact canonical source replacement",
        submitted_at=datetime.fromtimestamp(clock[0], UTC),
    )
    topics = [
        RecoveryTopic(
            binding,
            replace(binding.broker, topic="nexus-runtime-" + name.replace("_", "-")),
            str(index) * 32,
            name,
        )
        for index, name in enumerate(("dead_letter", "redrive_audit"), start=3)
    ]
    recovery.enroll_topics(binding, *topics, actor("approver"))
    retained = retain(recovery, binding, pending)
    decision = review(recovery, binding, retained)
    return SimpleNamespace(
        retained=retained, decision=decision, replacement=replacement, bundle=pending.bundle
    )


def stage(recovery, binding, prepared, cluster, *, ack=True, capture=True, evidence=True):
    store = recovery.publication
    event = EventEnvelope.model_validate_json(prepared.replacement)
    store.enqueue_source(prepared.bundle, event, source_actor(store))
    if ack:
        identity, sha, event_sha = cluster.query(
            store, "SELECT id,payload_sha256,event_sha256 FROM {schema}.runtime_publications"
        )[0]
        receipt = {
            "contract": "nexus.source-publication-receipt.v1",
            "id": identity,
            "publication_sha256": sha,
            "event_sha256": event_sha,
            "binding_sha256": digest(binding.payload()),
            "partition": 0,
            "offset": 1,
            "claim": "broker_ack_not_business_admission",
        }
        cluster.query(
            store,
            "INSERT INTO {schema}.runtime_publication_receipts VALUES (%s,%s,%s)",
            (identity, canonical(receipt), digest(receipt)),
            role="application",
        )
    if capture:
        recovery.capture(binding, [TransportRecord(0, 1, b"portfolio", prepared.replacement)])
    if evidence:
        enqueue(recovery, binding, prepared)
        recovery.publish_once(binding, worker(binding))
        recovery.publish_once(binding, worker(binding))


def resolve(recovery, binding, prepared, **kwargs):
    return recovery.resolve(
        binding,
        prepared.retained["request_id"],
        prepared.retained["request_sha256"],
        prepared.decision["decision_id"],
        actor("approver"),
        **kwargs,
    )


def test_propose_retained_publication(recovery, binding, prepared, cluster):
    stage(recovery, binding, prepared, cluster, ack=False, capture=False, evidence=False)
    event = EventEnvelope.model_validate_json(prepared.replacement)
    original_hash = cluster.query(
        recovery, "SELECT record_hash FROM {schema}.transport_inbox WHERE broker_offset=0"
    )[0][0]
    result = recovery.propose_publication(
        binding, 0, original_hash, str(event.event_id), "Server-side proposal", actor()
    )
    assert result["claim"] == "retained_proposal_not_delivery"
    assert (
        recovery.review_status(binding, result["request_id"], actor())["reason"]
        == "awaiting_review"
    )
    assert recovery.verify_history(binding)["delivered"] == [0, 0]
    for who in (actor("approver"), actor("auditor")):
        with pytest.raises(StoreError, match="Recovery role"):
            recovery.propose_publication(binding, 0, original_hash, str(event.event_id), "No", who)
    with pytest.raises(StoreError) as stale:
        recovery.propose_publication(binding, 0, "0" * 64, str(event.event_id), "No", actor())
    assert stale.value.code == "stale_recovery_target"
    with pytest.raises(StoreError) as missing:
        recovery.propose_publication(
            binding, 0, original_hash, "00000000-0000-4000-8000-000000000001", "No", actor()
        )
    assert missing.value.code == "recovery_publication_required"


def test_resolution_preserves_action_fence_and_requires_normal_admission(
    recovery, binding, prepared, cluster, publisher, clock
):
    stage(recovery, binding, prepared, cluster)
    result = resolve(recovery, binding, prepared)
    assert not result["duplicate"]
    history = recovery.verify_history(binding)
    assert history["captured"] == [2, 0] and history["delivered"] == [1, 0]
    assert recovery.publication.control.snapshot(actor())["event_count"] == 0
    fingerprint = recovery.publication.recommendation("rec-1", actor())["fingerprint"]
    with pytest.raises(StoreError) as denied:
        recovery.publication.decide("rec-1", actor("approver"), "approve", fingerprint)
    assert denied.value.code == "transport_pending"
    recovery.dispatch_once(binding, recovery.publication.control)
    assert recovery.publication.control.snapshot(actor())["event_count"] == 1
    assert recovery.verify_history(binding)["delivered"] == [2, 0]
    reopened = RecoveryResolutionLedger(
        recovery.config, publication=recovery.publication, clock=lambda: clock[0]
    )
    assert resolve(reopened, binding, prepared)["duplicate"]
    assert reopened.verify_history(binding)["recovery_resolutions"]["count"] == 1
    assert not recovery.publication.snapshot()["effects"]


@pytest.mark.parametrize("missing", ["ack", "capture", "evidence"])
def test_each_publication_requirement_is_mandatory(
    recovery, binding, prepared, cluster, publisher, missing
):
    stage(recovery, binding, prepared, cluster, **{missing: False})
    with pytest.raises(StoreError):
        resolve(recovery, binding, prepared)
    assert recovery.verify_history(binding)["delivered"] == [0, 0]


def test_missing_source_intent_cannot_resolve(recovery, binding, prepared):
    with pytest.raises(StoreError, match="source intent"):
        resolve(recovery, binding, prepared)


@pytest.mark.parametrize("invalid", ["expire", "revoke", "hash", "decision"])
def test_review_rechecked_at_resolution(
    recovery, binding, prepared, cluster, publisher, clock, invalid
):
    stage(recovery, binding, prepared, cluster)
    if invalid == "expire":
        clock[0] += 60
    elif invalid == "revoke":
        review(recovery, binding, prepared.retained, action="revoke")
    elif invalid == "hash":
        prepared.retained["request_sha256"] = "0" * 64
    else:
        prepared.decision["decision_id"] = "other"
    with pytest.raises(StoreError):
        resolve(recovery, binding, prepared)
    assert recovery.verify_history(binding)["delivered"] == [0, 0]


def test_resolution_transaction_rolls_back_all_three_writes(
    recovery, binding, prepared, cluster, publisher
):
    stage(recovery, binding, prepared, cluster)

    def fail():
        raise RuntimeError("before commit")

    with pytest.raises(RuntimeError):
        resolve(recovery, binding, prepared, failpoint=fail)
    assert recovery.verify_history(binding)["delivered"] == [0, 0]
    assert cluster.query(
        recovery, "SELECT COUNT(*) FROM {schema}.transport_recovery_resolutions"
    ) == [(0,)]
    assert cluster.query(recovery, "SELECT COUNT(*) FROM {schema}.transport_receipts") == [(0,)]
    assert not resolve(recovery, binding, prepared)["duplicate"]


def test_concurrent_resolution_is_one_checkpoint_change(
    recovery, binding, prepared, cluster, publisher
):
    stage(recovery, binding, prepared, cluster)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: resolve(recovery, binding, prepared), range(2)))
    assert sum(not r["duplicate"] for r in results) == 1
    assert recovery.verify_history(binding)["delivered"] == [1, 0]


def test_restart_without_bound_runtime_fails_closed(
    recovery, binding, prepared, cluster, publisher, clock
):
    stage(recovery, binding, prepared, cluster)
    resolve(recovery, binding, prepared)
    reopened = RecoveryResolutionLedger(recovery.config, clock=lambda: clock[0])
    with pytest.raises((PostgresBackendError, StoreError)):
        reopened.verify_history(binding)


@pytest.mark.parametrize("prepared", [False], indirect=True)
def test_equivalent_json_is_not_permission_to_change_reviewed_bytes(
    recovery, binding, prepared, cluster, publisher
):
    stage(recovery, binding, prepared, cluster)
    with pytest.raises(StoreError, match="Published bytes must equal"):
        resolve(recovery, binding, prepared)
    assert recovery.verify_history(binding)["delivered"] == [0, 0]


def test_expiry_after_source_proof_still_prevents_commit(
    recovery, binding, prepared, cluster, publisher, clock, monkeypatch
):
    stage(recovery, binding, prepared, cluster)
    original = recovery._source_proof

    def delayed(*args):
        result = original(*args)
        clock[0] += 60
        return result

    monkeypatch.setattr(recovery, "_source_proof", delayed)
    with pytest.raises(StoreError, match="expired before resolution"):
        resolve(recovery, binding, prepared)
    assert recovery.verify_history(binding)["delivered"] == [0, 0]


def test_capture_of_other_bytes_cannot_support_resolution(
    recovery, binding, prepared, cluster, publisher
):
    stage(recovery, binding, prepared, cluster, capture=False)
    recovery.capture(binding, [TransportRecord(0, 1, b"portfolio", b"different captured bytes")])
    with pytest.raises(PostgresBackendError, match="Captured replacement bytes"):
        resolve(recovery, binding, prepared)
    assert recovery.verify_history(binding)["delivered"] == [0, 0]


def test_resolution_requires_approver_and_complete_scope(
    recovery, binding, prepared, cluster, publisher
):
    stage(recovery, binding, prepared, cluster)
    for denied in (actor(), actor("auditor"), replace(actor("approver"), locations=())):
        with pytest.raises(StoreError):
            recovery.resolve(
                binding,
                prepared.retained["request_id"],
                prepared.retained["request_sha256"],
                prepared.decision["decision_id"],
                denied,
            )
    assert recovery.verify_history(binding)["delivered"] == [0, 0]


def test_resolution_is_immutable_and_replay_links_remain_exact(
    recovery, binding, prepared, cluster, publisher
):
    stage(recovery, binding, prepared, cluster)
    resolve(recovery, binding, prepared)
    with pytest.raises(cluster.driver.errors.InsufficientPrivilege):
        cluster.query(
            recovery, "DELETE FROM {schema}.transport_recovery_resolutions", role="application"
        )
    assert recovery.verify_history(binding)["recovery_resolutions"]["count"] == 1
