"""Read-only, source-bound learning evidence; never a model-promotion interface."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request

from nexus.contracts import ActorRole
from nexus.policies.authorization import Permission
from nexus.portfolio.auth import Actor
from nexus.portfolio.learning_attachments import LearningAttachments
from nexus.portfolio.service_backend import OperationalStore, guarded_view, read_view, state_root
from nexus.portfolio.store import PortfolioStore, StoreError, canonical, digest

if TYPE_CHECKING:
    from nexus.portfolio.training_pipeline import LearningRun

router = APIRouter()
_ACTIVE = "learning_report_active"
_PREFIX = "learning_report:"


def _global_scope(actor: Actor, locations: list[str]) -> None:
    if not set(locations).issubset(actor.locations):
        raise StoreError("scope_denied", "Learning evidence requires every reported location", 403)


def _locations(seed: dict[str, Any]) -> list[str]:
    return sorted({item["location_id"] for item in seed["recommendations"]})


def _summary(receipt: dict[str, Any], receipt_hash: str, locations: list[str]) -> dict[str, Any]:
    """Explicit projection: no source rows, forecasts, artifacts, or report paths."""
    bindings = {
        name: receipt["bindings"].get(name)
        for name in (
            "source_snapshot_sha256",
            "snapshot_sha256",
            "finalization_sha256",
            "plan_sha256",
            "code_sha256",
        )
    }
    if any(
        value is not None and not re.fullmatch(r"[a-f0-9]{64}", str(value))
        for value in bindings.values()
    ):
        raise ValueError("Invalid learning evidence binding")
    return {
        "contract_version": "nexus.learning-view.v1",
        "receipt_sha256": receipt_hash,
        "locations": locations,
        "evaluation_status": receipt["status"],
        "trained": receipt["training_performed"],
        "reason": receipt.get("reason"),
        "classification": receipt["classification"],
        "splits": {
            name: receipt.get("splits", {}).get(name, {}).get("rows")
            for name in ("training", "calibration", "final")
        },
        "metrics": {
            name: receipt.get("metrics", {}).get(name)
            for name in (
                "candidate_wis",
                "baseline_wis",
                "candidate_coverage80",
                "candidate_coverage95",
                "relative_wis_improvement",
            )
        },
        "quality_checks": {
            name: receipt.get("quality_checks", {}).get(name)
            for name in (
                "prediction_reload_parity",
                "coverage80_within_five_percentage_points",
                "coverage95_within_five_percentage_points",
            )
        },
        "bindings": bindings,
        "deployed": False,
        "human_approval": False,
        "currentness_scope": "source_snapshot_only",
    }


def attach_learning_report(
    store: OperationalStore,
    run: LearningRun,
    *,
    actor: Actor,
) -> dict[str, Any]:
    """Trusted local registrar only. Attach evaluated evidence without changing decisions."""
    if actor.role is not ActorRole.SYSTEM:
        raise StoreError("role_denied", "Only the trusted local registrar can attach evidence", 403)
    from nexus.portfolio.training_pipeline import verify_learning_run

    verify_learning_run(run)
    receipt = json.loads(canonical(run.receipt))
    if digest(receipt) != run.receipt_sha256:
        raise ValueError("Learning receipt changed during verification")
    if not isinstance(store, PortfolioStore):
        with guarded_view(store) as current:
            _global_scope(actor, sorted(current.retained_locations))
            seed = current.seed
            if seed.get("metadata", {}).get("pipeline_mode") != "connected_runtime_baseline":
                raise StoreError(
                    "runtime_required", "Learning evidence requires a connected runtime"
                )
            if receipt["bindings"]["source_snapshot_sha256"] != seed["metadata"].get(
                "source_snapshot_sha256"
            ):
                raise StoreError(
                    "stale_learning_report", "Learning evidence has a different source"
                )
            summary = _summary(receipt, run.receipt_sha256, sorted(current.retained_locations))
            retained = LearningAttachments(state_root(store), current.runtime_identity).attach(
                summary,
                seed_sha256=current.seed_sha256,
                registrar_id=actor.actor_id,
                registered_at=current.checked_at,
            )
            return {
                "status": "current",
                "summary_sha256": retained["summary_sha256"],
                "report": retained["report"],
            }
    with store.transaction() as db:
        store._verify_events(db)
        seed, _, _ = store._generation_catalog(db)
        if seed.get("metadata", {}).get("pipeline_mode") != "connected_runtime_baseline":
            raise StoreError("runtime_required", "Learning evidence requires a connected runtime")
        locations = _locations(seed)
        _global_scope(actor, locations)
        if receipt["bindings"]["source_snapshot_sha256"] != seed["metadata"].get(
            "source_snapshot_sha256"
        ):
            raise StoreError("stale_learning_report", "Learning evidence has a different source")
        summary = _summary(receipt, run.receipt_sha256, locations)
        identity = digest(summary)
        encoded = canonical(summary)
        row = db.execute("SELECT value FROM metadata WHERE key=?", (_PREFIX + identity,)).fetchone()
        if row is not None and row[0] != encoded:
            raise StoreError("integrity_error", "Stored learning summary was changed")
        db.execute(
            "INSERT OR IGNORE INTO metadata(key,value) VALUES (?,?)", (_PREFIX + identity, encoded)
        )
        db.execute(
            "INSERT INTO metadata(key,value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (_ACTIVE, identity),
        )
    return {"status": "current", "summary_sha256": identity, "report": summary}


def read_learning_report(store: OperationalStore, actor: Actor) -> dict[str, Any]:
    """One atomic read snapshot; source currentness is not a code/revision revalidation."""
    store._permission(actor, Permission.VIEW_AUDIT)
    if not isinstance(store, PortfolioStore):
        with read_view(store, actor=actor) as current:
            _global_scope(actor, sorted(current.retained_locations))
            attached = LearningAttachments(state_root(store), current.runtime_identity).read()
            if attached is None:
                return {"status": "not_run", "report": None}
            summary = attached["report"]
            _global_scope(actor, summary["locations"])
            matched = (
                current.seed["metadata"].get("source_snapshot_sha256")
                == summary["bindings"]["source_snapshot_sha256"]
            )
            return {
                "status": "current" if matched else "stale",
                "summary_sha256": attached["summary_sha256"],
                "report": summary,
            }
    with store.connection() as db:
        db.execute("BEGIN")
        store._verify_events(db)
        seed, _, _ = store._generation_catalog(db)
        _global_scope(actor, _locations(seed))
        active = db.execute("SELECT value FROM metadata WHERE key=?", (_ACTIVE,)).fetchone()
        if active is None:
            return {"status": "not_run", "report": None}
        identity = active[0]
        row = db.execute("SELECT value FROM metadata WHERE key=?", (_PREFIX + identity,)).fetchone()
        try:
            if not re.fullmatch(r"[a-f0-9]{64}", identity) or row is None:
                raise ValueError("Missing learning summary")
            summary = json.loads(row[0])
            if (
                digest(summary) != identity
                or summary["contract_version"] != "nexus.learning-view.v1"
            ):
                raise ValueError("Learning summary hash mismatch")
            _global_scope(actor, summary["locations"])
            source = seed["metadata"].get("source_snapshot_sha256")
            is_current = source == summary["bindings"]["source_snapshot_sha256"]
        except (ValueError, TypeError, KeyError) as error:
            if isinstance(error, StoreError):
                raise
            raise StoreError(
                "integrity_error", "Learning evidence failed its integrity check"
            ) from error
        return {
            "status": "current" if is_current else "stale",
            "summary_sha256": identity,
            "report": summary,
        }


@router.get("/api/learning")
def learning(request: Request) -> dict[str, Any]:
    from nexus.portfolio.api import get_store, require_actor

    actor = require_actor(request)
    return read_learning_report(get_store(request), actor)
