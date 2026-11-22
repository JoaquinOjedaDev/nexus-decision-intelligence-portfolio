"""Governed local deployment rehearsal, deliberately not an automated learning loop.

Only trusted internal execution may register artifacts/evaluations or shadow
outputs. No browser-supplied eligibility flags are accepted. The scientific R3B
champion is immutable: synthetic evidence may support a sandbox deployment drill
but can never promote the authoritative scientific champion. This module does
not train a model, call providers, execute orders, or prove external gate claims.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from nexus.contracts import ActorRole
from nexus.modeling.r3b_contracts import R3BConfig, r3b_config_sha256
from nexus.modeling.r3b_evaluation import (
    R3BEvaluationResult,
    decide_promotion,
    evaluate_candidate_against_baseline,
)
from nexus.modeling.r3b_models import R3BQuantileCalibrator
from nexus.portfolio.auth import Actor

if TYPE_CHECKING:
    from nexus.portfolio.training_pipeline import LearningRun

SCIENTIFIC_CHAMPION = "seasonal_naive_empirical_intervals"
_ZERO_HASH = "0" * 64
_MAX_ARTIFACT_BYTES = 16 * 1024 * 1024


def _text(value: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise ValueError("identifier/reason must be a nonblank string of at most 256 characters")
    return value.strip()


def _hash(value: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise ValueError("expected a lowercase SHA-256 digest")
    return value


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _encode(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha(value: object) -> str:
    return hashlib.sha256(_encode(value)).hexdigest()


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class ModelArtifact:
    """Opaque artifact bytes; persistence verifies identity but does not run the model."""

    model_id: str
    payload: bytes
    sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "model_id", _text(self.model_id))
        _hash(self.sha256)
        if not isinstance(self.payload, bytes) or not 0 < len(self.payload) <= _MAX_ARTIFACT_BYTES:
            raise ValueError("artifact must contain 1 byte to 16 MiB of immutable bytes")
        if hashlib.sha256(self.payload).hexdigest() != self.sha256:
            raise ValueError("artifact checksum mismatch")

    @classmethod
    def from_bytes(cls, model_id: str, payload: bytes) -> ModelArtifact:
        return cls(model_id, payload, hashlib.sha256(payload).hexdigest())


@dataclass(frozen=True, slots=True)
class CandidateEvaluation:
    """Checksum-bound receipt created by a trusted, artifact-verified R3B bridge.

    This receipt binds a computed decision and its inputs, not an independent
    attestation of the bridge's named technical checks. Do not expose its raw
    constructor, calibration support, or verification checks as HTTP inputs.
    """

    payload: bytes
    sha256: str

    def __post_init__(self) -> None:
        _hash(self.sha256)
        if (
            not isinstance(self.payload, bytes)
            or hashlib.sha256(self.payload).hexdigest() != self.sha256
        ):
            raise ValueError("evaluation receipt checksum mismatch")
        value = json.loads(self.payload)
        if (
            not isinstance(value, dict)
            or value.get("contract_version") != "nexus.sandbox-evaluation.v1"
        ):
            raise ValueError("unsupported sandbox evaluation receipt")
        for field in (
            "source_snapshot_sha256",
            "monitor_evaluation_sha256",
            "artifact_sha256",
            "baseline_artifact_sha256",
            "config_sha256",
        ):
            _hash(value[field])
        decision = value["scientific_decision"]
        if (
            decision.get("synthetic_evidence") is not True
            or decision.get("promotion_applied") is not False
            or decision.get("human_approval_required") is not True
        ):
            raise ValueError("scientific promotion boundaries cannot be changed")
        if self.payload != _encode(value):
            raise ValueError("evaluation receipt must use its canonical encoding")

    def to_dict(self) -> dict[str, Any]:
        return dict(json.loads(self.payload))

    @classmethod
    def from_r3b(
        cls,
        evaluation: R3BEvaluationResult,
        *,
        config: R3BConfig,
        calibrators: Mapping[int, R3BQuantileCalibrator],
        verification_checks: Mapping[str, bool],
        evaluation_cutoff: datetime,
        source_snapshot_sha256: str,
        monitor_evaluation_sha256: str,
        artifact_sha256: str,
        baseline_artifact_sha256: str,
    ) -> CandidateEvaluation:
        """Recompute paired statistics and derive support before applying real R3B policy."""
        cutoff = _utc(evaluation_cutoff)
        for digest in (
            source_snapshot_sha256,
            monitor_evaluation_sha256,
            artifact_sha256,
            baseline_artifact_sha256,
        ):
            _hash(digest)
        if not evaluation.rows:
            raise ValueError("evaluation needs nonempty paired forecast/truth rows")
        if any(
            row.truth.label_available_at > cutoff
            or row.truth.target_date >= cutoff.date()
            or row.truth.label_available_at.date() <= row.truth.target_date
            for row in evaluation.rows
        ):
            raise ValueError("evaluation rows must be complete and available at the cutoff")
        final_origin = config.evaluation.rolling_origins[-1].issue_time
        if any(
            row.truth.target_date
            != final_origin.date() + timedelta(days=row.truth.horizon_days - 1)
            for row in evaluation.rows
        ):
            raise ValueError("evaluation rows must match the frozen untouched final-stress origin")
        if not verification_checks or any(
            not isinstance(name, str) or not name.strip() or not isinstance(passed, bool)
            for name, passed in verification_checks.items()
        ):
            raise ValueError("nonempty named boolean verification checks are required")
        horizons = set(config.forecast_contract.horizons_days)
        if any(type(horizon) is not int for horizon in calibrators):
            raise ValueError("calibrator horizon keys must be integers")
        if any(not isinstance(item, R3BQuantileCalibrator) for item in calibrators.values()):
            raise ValueError("calibrators must be validated R3BQuantileCalibrator objects")
        sufficient = (
            set(calibrators) == horizons
            and {row.truth.horizon_days for row in evaluation.rows} == horizons
            and all(
                item.minimum_support == config.calibration.minimum_support_per_horizon
                and item.support >= config.calibration.minimum_support_per_horizon
                and item.assessed
                for item in calibrators.values()
            )
        )
        recomputed = evaluate_candidate_against_baseline(
            tuple(row.candidate for row in evaluation.rows),
            tuple(row.baseline for row in evaluation.rows),
            tuple(row.truth for row in evaluation.rows),
            config=config,
        )
        decision = decide_promotion(
            recomputed,
            config=config,
            evidence_sufficient=sufficient,
            gate_checks_passed=all(verification_checks.values()),
        )
        receipt = {
            "contract_version": "nexus.sandbox-evaluation.v1",
            "source_snapshot_sha256": source_snapshot_sha256,
            "monitor_evaluation_sha256": monitor_evaluation_sha256,
            "artifact_sha256": artifact_sha256,
            "baseline_artifact_sha256": baseline_artifact_sha256,
            "issue_time": final_origin.isoformat(),
            "config_sha256": r3b_config_sha256(config),
            "evaluation_cutoff": cutoff.isoformat(),
            "evaluation_rows": len(recomputed.rows),
            "evaluation_rows_sha256": _sha(
                [
                    {
                        "key": [str(value) for value in row.forecast_key],
                        "actual": str(row.truth.actual),
                        "available_at": row.truth.label_available_at.isoformat(),
                        "candidate": [str(value) for value in row.candidate.quantiles],
                        "baseline": [str(value) for value in row.baseline.quantiles],
                    }
                    for row in recomputed.rows
                ]
            ),
            "candidate_wis": str(recomputed.candidate_wis),
            "baseline_wis": str(recomputed.baseline_wis),
            "relative_wis_improvement": (
                None
                if recomputed.relative_wis_improvement is None
                else str(recomputed.relative_wis_improvement)
            ),
            "paired_bootstrap_lower": str(recomputed.paired_wis_bootstrap.lower),
            "calibration_support": {
                str(horizon): {"support": item.support, "minimum": item.minimum_support}
                for horizon, item in sorted(calibrators.items())
            },
            "evidence_sufficient": sufficient,
            "verification_checks": dict(sorted(verification_checks.items())),
            "verification_claim_boundary": (
                "Trusted bridge receipt; not independent execution proof."
            ),
            "scientific_decision": asdict(decision),
        }
        payload = _encode(receipt)
        return cls(payload, hashlib.sha256(payload).hexdigest())


@dataclass(frozen=True, slots=True)
class ShadowOutput:
    """One checked forecast output; paired hashes must identify identical input features."""

    case_id: str
    input_sha256: str
    quantiles: tuple[Decimal, Decimal, Decimal, Decimal, Decimal]

    def __post_init__(self) -> None:
        object.__setattr__(self, "case_id", _text(self.case_id))
        _hash(self.input_sha256)
        if not isinstance(self.quantiles, tuple) or len(self.quantiles) != 5:
            raise ValueError("shadow outputs require exactly five quantiles")
        if any(
            not isinstance(value, Decimal) or not value.is_finite() or value < 0
            for value in self.quantiles
        ):
            raise ValueError("shadow quantiles must be finite nonnegative Decimals")
        if tuple(sorted(self.quantiles)) != self.quantiles:
            raise ValueError("shadow quantiles must be monotonic")


@dataclass(frozen=True, slots=True)
class TrainingAdmission:
    """Immutable trusted bridge binding, not independent proof of source truth.

    Feedback evidence and the R2 training package deliberately have different
    source identities. Only this explicit binding can join them; it grants no
    deployment authority and does not relax legacy candidate registration.
    """

    payload: bytes
    sha256: str

    def __post_init__(self) -> None:
        _hash(self.sha256)
        if (
            not isinstance(self.payload, bytes)
            or not 0 < len(self.payload) <= _MAX_ARTIFACT_BYTES
            or hashlib.sha256(self.payload).hexdigest() != self.sha256
        ):
            raise ValueError("training admission checksum/size mismatch")
        value = json.loads(self.payload)
        fields = {
            "contract_version",
            "feedback_receipt_id",
            "feedback_evaluation_sha256",
            "feedback_evidence_sha256",
            "feedback_journal_head_sha256",
            "feedback_source_snapshot_sha256",
            "monitor_evaluation_sha256",
            "training_snapshot",
            "plan",
            "code_files",
            "bridge_code_files",
            "dependencies",
            "revision_map_sha256",
            "baseline_artifact_sha256",
            "locations",
        }
        if (
            not isinstance(value, dict)
            or set(value) != fields
            or value["contract_version"] != "nexus.training-admission.v1"
            or self.payload != _encode(value)
        ):
            raise ValueError("training admission must use its strict canonical contract")
        for name in fields - {
            "contract_version",
            "training_snapshot",
            "plan",
            "code_files",
            "bridge_code_files",
            "dependencies",
            "locations",
        }:
            _hash(value[name])
        locations = value["locations"]
        if (
            not isinstance(locations, list)
            or not locations
            or any(not isinstance(item, str) or _text(item) != item for item in locations)
            or locations != sorted(set(locations))
        ):
            raise ValueError("admission locations must be nonempty, unique, and ordered")
        for name in ("code_files", "bridge_code_files"):
            mapping = value[name]
            if not isinstance(mapping, dict) or not mapping:
                raise ValueError("admission requires experiment and bridge code bindings")
            for path, digest in mapping.items():
                if (
                    not isinstance(path, str)
                    or not path
                    or len(path) > 512
                    or path.startswith(("/", "\\"))
                    or ":" in path
                    or "\\" in path
                    or any(part in {"", ".", ".."} for part in path.split("/"))
                ):
                    raise ValueError("admission code paths must be normalized relative paths")
                _hash(digest)
        dependencies = value["dependencies"]
        if (
            not isinstance(dependencies, dict)
            or set(dependencies) != {"lightgbm", "numpy", "pyarrow"}
            or any(
                not isinstance(item, str) or _text(item) != item for item in dependencies.values()
            )
        ):
            raise ValueError("admission must bind the three runtime modeling dependencies")
        self._validate_snapshot_plan(value)

    @staticmethod
    def _validate_snapshot_plan(value: dict[str, Any]) -> None:
        # Local imports avoid the existing training_pipeline -> learning cycle.
        from nexus.portfolio.training_data import (
            TrainingLabel,
            TrainingSnapshot,
            validate_training_snapshot,
        )
        from nexus.portfolio.training_pipeline import LearningPlan

        raw = value["training_snapshot"]
        if not isinstance(raw, dict):
            raise ValueError("admission requires a complete training snapshot")
        try:
            snapshot = TrainingSnapshot(
                status=raw["status"],
                labels=tuple(
                    TrainingLabel.model_validate_json(_encode(row)) for row in raw["labels"]
                ),
                exclusions=tuple(sorted(raw["excluded_counts"].items())),
                reason_codes=tuple(raw["reason_codes"]),
                source_snapshot_sha256=raw["source_snapshot_sha256"],
                source_manifest_file_sha256=raw["source_manifest_file_sha256"],
                gold_demand_sha256=raw["gold_demand_sha256"],
                gold_fulfillment_sha256=raw["gold_fulfillment_sha256"],
                finalization_sha256=raw["finalization_sha256"],
                as_of=_utc(datetime.fromisoformat(raw["as_of"])),
                minimum_history_days=raw["minimum_history_days"],
                minimum_mature_labels=raw["minimum_mature_labels"],
                counts_by_series=tuple(sorted(raw["counts_by_series"].items())),
                snapshot_sha256=raw["snapshot_sha256"],
            )
            validate_training_snapshot(snapshot)
            plan = LearningPlan.model_validate_json(_encode(value["plan"]))
        except (KeyError, TypeError, AttributeError) as error:
            raise ValueError("training admission snapshot/plan is malformed") from error
        if (
            _encode(snapshot.to_dict()) != _encode(raw)
            or _encode(plan.model_dump(mode="json")) != _encode(value["plan"])
            or snapshot.minimum_history_days < 180
            or snapshot.minimum_mature_labels < 500
            or snapshot.as_of != plan.evaluation_cutoff
            or not {row.location_id for row in snapshot.labels}.issubset(value["locations"])
        ):
            raise ValueError("admission snapshot/plan/scope or minimum evidence policy mismatch")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> TrainingAdmission:
        payload = _encode(dict(value))
        return cls(payload, hashlib.sha256(payload).hexdigest())

    def to_dict(self) -> dict[str, Any]:
        return dict(json.loads(self.payload))


def _admission_run_binding(binding: dict[str, Any], receipt: dict[str, Any]) -> None:
    snapshot = binding["training_snapshot"]
    expected = {
        "snapshot_sha256": snapshot["snapshot_sha256"],
        "source_snapshot_sha256": snapshot["source_snapshot_sha256"],
        "finalization_sha256": snapshot["finalization_sha256"],
        "plan_sha256": _sha(binding["plan"]),
        "code_sha256": _sha(binding["code_files"]),
    }
    trained = receipt.get("training_performed")
    if (
        receipt.get("contract_version") != "nexus.runtime-learning-result.v1"
        or receipt.get("bindings") != expected
        or _encode(receipt.get("snapshot")) != _encode(snapshot)
        or _encode(receipt.get("plan")) != _encode(binding["plan"])
        or _encode(receipt.get("code_files")) != _encode(binding["code_files"])
        or receipt.get("classification") != "synthetic"
        or receipt.get("scientific_champion_changed") is not False
        or type(trained) is not bool
        or (trained and receipt.get("dependencies") != binding["dependencies"])
        or (not trained and receipt.get("status") != "ABSTAIN_INSUFFICIENT_EVIDENCE")
        or (not trained and not isinstance(receipt.get("reason"), str))
    ):
        raise ValueError("training completion differs from its admitted run bindings")
    if trained:
        for name in ("candidate_sha256", "baseline_artifact_sha256"):
            _hash(receipt[name])
        decision = receipt.get("scientific_decision", {})
        if (
            decision.get("promotion_applied") is not False
            or decision.get("synthetic_evidence") is not True
        ):
            raise ValueError("training completion cannot promote the scientific champion")


class LearningBook:
    """Atomic sandbox model switches, manual approvals, rollback and audited proposals."""

    def __init__(
        self,
        path: Path,
        *,
        initial_artifact: ModelArtifact | None = None,
        locations: tuple[str, ...] | None = None,
        minimum_shadow_cases: int = 10,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        if type(minimum_shadow_cases) is not int or minimum_shadow_cases <= 0:
            raise ValueError("minimum_shadow_cases must be a positive integer")
        self.path, self.clock = path, clock
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS learning_artifacts (
                    sha256 TEXT PRIMARY KEY, model_id TEXT NOT NULL, payload BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS learning_events (
                    sequence INTEGER PRIMARY KEY, payload TEXT NOT NULL,
                    event_sha256 TEXT NOT NULL UNIQUE
                );
            """)
            db.execute("BEGIN IMMEDIATE")
            state = self._read(db)
            if not state:
                if initial_artifact is None or not locations:
                    raise ValueError(
                        "new learning book requires an initial artifact and global locations"
                    )
                normalized_locations = tuple(sorted({_text(location) for location in locations}))
                self._artifact(db, initial_artifact)
                self._append(
                    db,
                    "initialized",
                    {
                        "artifact_sha256": initial_artifact.sha256,
                        "model_id": initial_artifact.model_id,
                        "locations": normalized_locations,
                        "minimum_shadow_cases": minimum_shadow_cases,
                        "authoritative_scientific_champion": SCIENTIFIC_CHAMPION,
                    },
                    None,
                )
            else:
                if initial_artifact is not None and (
                    state["initial_artifact_sha256"] != initial_artifact.sha256
                    or state["initial_model_id"] != initial_artifact.model_id
                ):
                    raise ValueError("initial deployment cannot be replaced during restart")
                if locations is not None and sorted(set(locations)) != state["locations"]:
                    raise ValueError("learning location scope cannot change during restart")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _artifact(db: sqlite3.Connection, artifact: ModelArtifact) -> None:
        existing = db.execute(
            "SELECT model_id,payload FROM learning_artifacts WHERE sha256=?", (artifact.sha256,)
        ).fetchone()
        if existing is not None and (
            existing["model_id"] != artifact.model_id
            or bytes(existing["payload"]) != artifact.payload
        ):
            raise ValueError("artifact identity collision")
        db.execute(
            "INSERT OR IGNORE INTO learning_artifacts(sha256,model_id,payload) VALUES (?,?,?)",
            (artifact.sha256, artifact.model_id, artifact.payload),
        )

    def _append(
        self,
        db: sqlite3.Connection,
        kind: str,
        payload: dict[str, Any],
        actor: Actor | None,
        *,
        recorded_at: datetime | None = None,
    ) -> None:
        latest = db.execute(
            "SELECT sequence,payload,event_sha256 FROM learning_events "
            "ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        now = _utc(self.clock()) if recorded_at is None else _utc(recorded_at)
        if latest is not None and now < datetime.fromisoformat(json.loads(latest["payload"])["at"]):
            raise ValueError("learning clock cannot move backwards")
        event = {
            "sequence": 1 if latest is None else latest["sequence"] + 1,
            "previous_sha256": _ZERO_HASH if latest is None else latest["event_sha256"],
            "kind": kind,
            "at": now.isoformat(),
            "actor_id": "local-bootstrap" if actor is None else actor.actor_id,
            "actor_role": "system" if actor is None else actor.role.value,
            "data": payload,
        }
        db.execute(
            "INSERT INTO learning_events(sequence,payload,event_sha256) VALUES (?,?,?)",
            (event["sequence"], _encode(event).decode(), _sha(event)),
        )

    @staticmethod
    def _authorize(actor: Actor, state: dict[str, Any], roles: set[ActorRole]) -> None:
        if not isinstance(actor, Actor) or actor.role not in roles:
            raise PermissionError("actor role is not authorized for this learning operation")
        _text(actor.actor_id)
        if not set(state["locations"]).issubset(set(actor.locations)):
            raise PermissionError("actor must cover every location in the global model scope")

    @staticmethod
    def _read(db: sqlite3.Connection) -> dict[str, Any]:
        state: dict[str, Any] = {}
        previous = _ZERO_HASH
        events = []
        for expected_sequence, row in enumerate(
            db.execute(
                "SELECT sequence,payload,event_sha256 FROM learning_events ORDER BY sequence"
            ),
            1,
        ):
            event = json.loads(row["payload"])
            if (
                row["sequence"] != expected_sequence
                or event["sequence"] != expected_sequence
                or event["previous_sha256"] != previous
                or _sha(event) != row["event_sha256"]
            ):
                raise ValueError("learning audit hash chain mismatch")
            previous = row["event_sha256"]
            kind, data = event["kind"], event["data"]
            events.append({**event, "event_sha256": previous})
            if kind == "initialized" and not state:
                if data["authoritative_scientific_champion"] != SCIENTIFIC_CHAMPION:
                    raise ValueError("scientific champion is immutable")
                state = {
                    "authoritative_scientific_champion": SCIENTIFIC_CHAMPION,
                    "scientific_promotion_applied": False,
                    "mode": "sandbox-deployment-rehearsal",
                    "training_performed": False,
                    "locations": data["locations"],
                    "minimum_shadow_cases": data["minimum_shadow_cases"],
                    "initial_artifact_sha256": data["artifact_sha256"],
                    "initial_model_id": data["model_id"],
                    "proposals": {},
                    "training_admissions": {},
                    "candidates": {},
                    "shadows": {},
                    "deployment_nodes": {"initial": {**data, "parent": None}},
                    "current_node": "initial",
                }
            elif kind == "training_proposed":
                state["proposals"][data["proposal_id"]] = data
            elif kind == "candidate_registered":
                state["candidates"][data["candidate_id"]] = data
            elif kind in {"training_admitted", "training_started", "training_completed"}:
                LearningBook._read_training_event(state, event)
            elif kind == "shadow_recorded":
                state["shadows"][data["candidate_id"]] = data
            elif kind == "sandbox_deployed":
                if data["parent"] != state["current_node"]:
                    raise ValueError("sandbox deployment projection mismatch")
                state["deployment_nodes"][data["node_id"]] = data
                state["current_node"] = data["node_id"]
            elif kind == "sandbox_rolled_back":
                if data["from_node"] != state["current_node"]:
                    raise ValueError("sandbox rollback projection mismatch")
                state["current_node"] = data["to_node"]
            else:
                raise ValueError("unknown or invalid learning event")
        if state:
            artifacts = {
                row["sha256"]: (row["model_id"], bytes(row["payload"]))
                for row in db.execute("SELECT sha256,model_id,payload FROM learning_artifacts")
            }
            required = {(state["initial_artifact_sha256"], state["initial_model_id"])}
            required.update(
                (item["artifact_sha256"], item["model_id"]) for item in state["candidates"].values()
            )
            required.update(
                (
                    item["completion"]["run_receipt"]["baseline_artifact_sha256"],
                    item["completion"]["baseline_model_id"],
                )
                for item in state["training_admissions"].values()
                if item["completion"] is not None and item["completion"]["training_performed"]
            )
            for digest, model_id in required:
                artifact = artifacts.get(digest)
                if (
                    artifact is None
                    or artifact[0] != model_id
                    or hashlib.sha256(artifact[1]).hexdigest() != digest
                ):
                    raise ValueError("persisted learning artifact checksum mismatch")
            state["sandbox_deployment"] = state["deployment_nodes"][state["current_node"]]
            state["audit"] = events
            state["audit_head_sha256"] = previous
        return state

    @staticmethod
    def _read_training_event(state: dict[str, Any], event: dict[str, Any]) -> None:
        data, kind = event["data"], event["kind"]
        if event["actor_role"] != ActorRole.SYSTEM.value:
            raise ValueError("training admission events require a trusted system actor")
        admission_id = _hash(data["admission_id"])
        if kind == "training_admitted":
            binding = TrainingAdmission.from_dict(data["binding"])
            proposal = state["proposals"].get(data["proposal_id"])
            if (
                proposal is None
                or data["binding_sha256"] != binding.sha256
                or admission_id != _sha([data["proposal_id"], binding.sha256])
                or data["logical_run_id"] != _sha(["admitted-training", admission_id])
                or data["admitted_at"] != event["at"]
                or data["admitted_by"] != event["actor_id"]
                or data["binding"]["locations"] != state["locations"]
                or data["binding"]["baseline_artifact_sha256"]
                != state["deployment_nodes"][state["current_node"]]["artifact_sha256"]
                or _utc(datetime.fromisoformat(data["binding"]["plan"]["evaluation_cutoff"]))
                > _utc(datetime.fromisoformat(event["at"]))
                or proposal["source_snapshot_sha256"]
                != data["binding"]["feedback_source_snapshot_sha256"]
                or proposal["monitor_evaluation_sha256"]
                != data["binding"]["monitor_evaluation_sha256"]
                or any(
                    item["proposal_id"] == data["proposal_id"]
                    for item in state["training_admissions"].values()
                )
            ):
                raise ValueError("persisted training admission binding mismatch")
            state["training_admissions"][admission_id] = {
                **data,
                "started": None,
                "completion": None,
            }
            return
        admission = state["training_admissions"].get(admission_id)
        if admission is None or data["logical_run_id"] != admission["logical_run_id"]:
            raise ValueError("training run has no matching prior admission")
        if kind == "training_started":
            if (
                admission["started"] is not None
                or data["started_at"] != event["at"]
                or data["started_by"] != event["actor_id"]
                or datetime.fromisoformat(data["started_at"])
                < datetime.fromisoformat(admission["admitted_at"])
            ):
                raise ValueError("training start must follow one immutable admission")
            admission["started"] = data
            return
        started = admission["started"]
        if (
            started is None
            or admission["completion"] is not None
            or data["completed_at"] != event["at"]
            or data["completed_by"] != event["actor_id"]
            or datetime.fromisoformat(data["completed_at"])
            < datetime.fromisoformat(started["started_at"])
            or data["run_receipt_sha256"] != _sha(data["run_receipt"])
            or data["deployment_blocked"] is not True
            or data["status"] != data["run_receipt"]["status"]
            or data["training_performed"] is not data["run_receipt"]["training_performed"]
        ):
            raise ValueError("training completion must follow its unique durable start")
        _admission_run_binding(admission["binding"], data["run_receipt"])
        candidate = data["candidate"]
        if data["training_performed"]:
            if not isinstance(candidate, dict):
                raise ValueError("trained completion requires its admitted candidate")
            evaluation = candidate["evaluation"]
            encoded = _encode(evaluation)
            CandidateEvaluation(encoded, candidate["evaluation_sha256"])
            if (
                candidate["candidate_id"]
                != _sha(
                    [admission_id, candidate["artifact_sha256"], candidate["evaluation_sha256"]]
                )
                or candidate["candidate_id"] != data["candidate_id"]
                or candidate["admission_id"] != admission_id
                or candidate["proposal_id"] != admission["proposal_id"]
                or candidate["artifact_sha256"] != data["run_receipt"]["candidate_sha256"]
                or evaluation["source_snapshot_sha256"]
                != admission["binding"]["training_snapshot"]["source_snapshot_sha256"]
                or evaluation["monitor_evaluation_sha256"]
                != admission["binding"]["monitor_evaluation_sha256"]
                or evaluation["runtime_learning_receipt_sha256"] != data["run_receipt_sha256"]
                or evaluation["artifact_sha256"] != candidate["artifact_sha256"]
                or evaluation["baseline_artifact_sha256"]
                != data["run_receipt"]["baseline_artifact_sha256"]
                or evaluation["scientific_decision"] != data["run_receipt"]["scientific_decision"]
                or evaluation["config_sha256"] != _sha(admission["binding"]["plan"])
                or evaluation["evaluation_cutoff"]
                != admission["binding"]["plan"]["evaluation_cutoff"]
                or candidate["registered_by"] != event["actor_id"]
                or data["baseline_model_id"] != "runtime_calibrated_seasonal_baseline"
                or candidate["deployment_blocked"] is not True
            ):
                raise ValueError("admitted candidate evaluation binding mismatch")
            state["candidates"][candidate["candidate_id"]] = candidate
            state["training_performed"] = True
        elif (
            candidate is not None
            or data["candidate_id"] is not None
            or data["baseline_model_id"] is not None
        ):
            raise ValueError("abstention must not register a candidate")
        admission["completion"] = data

    def admit_training(self, actor: Actor, *, proposal_id: str, binding: TrainingAdmission) -> str:
        """Bind one explicit proposal before fitting, without authorizing deployment."""
        if not isinstance(binding, TrainingAdmission):
            raise ValueError("training admission requires an immutable typed binding")
        TrainingAdmission(binding.payload, binding.sha256)
        value = binding.to_dict()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            state = self._read(db)
            self._authorize(actor, state, {ActorRole.SYSTEM})
            proposal = state["proposals"].get(proposal_id)
            if proposal is None:
                raise ValueError("unknown training proposal")
            if (
                value["locations"] != state["locations"]
                or value["feedback_source_snapshot_sha256"] != proposal["source_snapshot_sha256"]
                or value["monitor_evaluation_sha256"] != proposal["monitor_evaluation_sha256"]
            ):
                raise ValueError("admission feedback/source/monitor/location binding mismatch")
            admission_id = _sha([proposal_id, binding.sha256])
            for previous in state["training_admissions"].values():
                if previous["proposal_id"] == proposal_id:
                    if previous["admission_id"] != admission_id:
                        raise ValueError(
                            "training proposal already has a different immutable admission"
                        )
                    return admission_id
            if any(item["proposal_id"] == proposal_id for item in state["candidates"].values()):
                raise ValueError(
                    "an already registered candidate cannot receive retroactive admission"
                )
            now = _utc(self.clock())
            if (
                any(
                    _utc(datetime.fromisoformat(value["plan"][name])) > now
                    for name in ("fit_at", "calibrated_at", "evaluation_cutoff")
                )
                or _utc(datetime.fromisoformat(value["training_snapshot"]["as_of"])) > now
            ):
                raise ValueError("future data or evaluation cutoffs cannot be admitted")
            if value["baseline_artifact_sha256"] != state["sandbox_deployment"]["artifact_sha256"]:
                raise ValueError("admission baseline is not the current learning baseline")
            self._append(
                db,
                "training_admitted",
                {
                    "admission_id": admission_id,
                    "proposal_id": proposal_id,
                    "binding_sha256": binding.sha256,
                    "binding": value,
                    "admitted_at": now.isoformat(),
                    "admitted_by": actor.actor_id,
                    "logical_run_id": _sha(["admitted-training", admission_id]),
                },
                actor,
                recorded_at=now,
            )
        return admission_id

    def start_training(self, actor: Actor, *, admission_id: str) -> dict[str, Any]:
        """Claim the admitted logical run once; interrupted work needs explicit recovery."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            state = self._read(db)
            self._authorize(actor, state, {ActorRole.SYSTEM})
            admission = state["training_admissions"].get(admission_id)
            if admission is None:
                raise ValueError("unknown training admission")
            if admission["started"] is not None:
                raise ValueError(
                    "training already started; explicit recovery required, no implicit refit"
                )
            if any(
                item["proposal_id"] == admission["proposal_id"]
                for item in state["candidates"].values()
            ):
                raise ValueError(
                    "proposal already has a candidate; no retrospective training start"
                )
            if (
                admission["binding"]["baseline_artifact_sha256"]
                != state["sandbox_deployment"]["artifact_sha256"]
            ):
                raise ValueError("learning baseline changed after admission")
            now = _utc(self.clock())
            payload = {
                "admission_id": admission_id,
                "logical_run_id": admission["logical_run_id"],
                "started_at": now.isoformat(),
                "started_by": actor.actor_id,
            }
            self._append(db, "training_started", payload, actor, recorded_at=now)
            return payload

    def complete_training(
        self, actor: Actor, *, admission_id: str, run: LearningRun
    ) -> dict[str, Any]:
        """Record verified trained losses/wins or abstentions; admitted models cannot deploy."""
        from nexus.portfolio.training_pipeline import (
            LearningRun,
            sandbox_evaluation,
            verify_learning_run,
        )

        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            state = self._read(db)
            self._authorize(actor, state, {ActorRole.SYSTEM})
            admission = state["training_admissions"].get(admission_id)
            if admission is None or admission["started"] is None:
                raise ValueError(
                    "training completion requires its prior durable admission and start"
                )
            if not isinstance(run, LearningRun):
                raise ValueError("training completion requires a verified LearningRun object")
            verify_learning_run(run)
            receipt = json.loads(_encode(run.receipt))
            _admission_run_binding(admission["binding"], receipt)
            previous = admission["completion"]
            if previous is not None:
                if previous["run_receipt_sha256"] != run.receipt_sha256:
                    raise ValueError("training admission already has a different completion")
                return dict(previous)
            if (
                admission["binding"]["baseline_artifact_sha256"]
                != state["sandbox_deployment"]["artifact_sha256"]
            ):
                raise ValueError("learning baseline changed before training completion")
            now = _utc(self.clock())
            candidate = None
            if run.candidate is not None:
                evaluation = sandbox_evaluation(
                    run, monitor_evaluation_sha256=admission["binding"]["monitor_evaluation_sha256"]
                )
                artifact = ModelArtifact.from_bytes(
                    "runtime_daily_lightgbm_candidate", run.candidate.to_bytes()
                )
                candidate_id = _sha([admission_id, artifact.sha256, evaluation.sha256])
                candidate = {
                    "candidate_id": candidate_id,
                    "proposal_id": admission["proposal_id"],
                    "admission_id": admission_id,
                    "artifact_sha256": artifact.sha256,
                    "model_id": artifact.model_id,
                    "evaluation_sha256": evaluation.sha256,
                    "evaluation": evaluation.to_dict(),
                    "registered_by": actor.actor_id,
                    "deployment_blocked": True,
                    "deployment_block_reason": (
                        "executable_baseline_shadow_segment_business_gates_not_complete"
                    ),
                }
                self._artifact(db, artifact)
                if run.baseline_artifact is None:
                    raise ValueError("trained completion requires its retained comparator")
                self._artifact(db, run.baseline_artifact)
            payload = {
                "admission_id": admission_id,
                "logical_run_id": admission["logical_run_id"],
                "completed_at": now.isoformat(),
                "completed_by": actor.actor_id,
                "run_receipt_sha256": run.receipt_sha256,
                "run_receipt": receipt,
                "status": receipt["status"],
                "training_performed": receipt["training_performed"],
                "candidate_id": None if candidate is None else candidate["candidate_id"],
                "candidate": candidate,
                "baseline_model_id": None
                if run.baseline_artifact is None
                else run.baseline_artifact.model_id,
                "deployment_blocked": True,
            }
            if len(_encode(payload)) > _MAX_ARTIFACT_BYTES:
                raise ValueError("training completion exceeds the bounded receipt size")
            self._append(db, "training_completed", payload, actor, recorded_at=now)
            self._read(db)
            return payload

    def propose_training(
        self,
        actor: Actor,
        *,
        source_snapshot_sha256: str,
        monitor_evaluation_sha256: str,
        reason: str,
    ) -> str:
        """Create a durable proposal; this does not launch or fabricate a training run."""
        _hash(source_snapshot_sha256)
        _hash(monitor_evaluation_sha256)
        reason = _text(reason)
        proposal_id = _sha([source_snapshot_sha256, monitor_evaluation_sha256])
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            state = self._read(db)
            self._authorize(actor, state, {ActorRole.PLANNER, ActorRole.SYSTEM})
            if proposal_id not in state["proposals"]:
                self._append(
                    db,
                    "training_proposed",
                    {
                        "proposal_id": proposal_id,
                        "source_snapshot_sha256": source_snapshot_sha256,
                        "monitor_evaluation_sha256": monitor_evaluation_sha256,
                        "creator_id": actor.actor_id,
                        "reason": reason,
                        "status": "awaiting_trusted_training_bridge",
                    },
                    actor,
                )
        return proposal_id

    def register_candidate(
        self,
        actor: Actor,
        *,
        proposal_id: str,
        artifact: ModelArtifact,
        evaluation: CandidateEvaluation,
    ) -> str:
        """Persist trusted bridge output and its review outcome, including losses/abstentions."""
        receipt = evaluation.to_dict()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            state = self._read(db)
            self._authorize(actor, state, {ActorRole.SYSTEM})
            proposal = state["proposals"].get(proposal_id)
            if proposal is None:
                raise ValueError("unknown training proposal")
            if _utc(datetime.fromisoformat(receipt["evaluation_cutoff"])) > _utc(self.clock()):
                raise ValueError("future evaluation receipt cannot be registered")
            if (
                any(
                    receipt[field] != proposal[field]
                    for field in ("source_snapshot_sha256", "monitor_evaluation_sha256")
                )
                or receipt["artifact_sha256"] != artifact.sha256
            ):
                raise ValueError("evaluation artifact/source/monitor binding mismatch")
            candidate_id = _sha([proposal_id, artifact.sha256, evaluation.sha256])
            if candidate_id not in state["candidates"]:
                self._artifact(db, artifact)
                self._append(
                    db,
                    "candidate_registered",
                    {
                        "candidate_id": candidate_id,
                        "proposal_id": proposal_id,
                        "artifact_sha256": artifact.sha256,
                        "model_id": artifact.model_id,
                        "evaluation_sha256": evaluation.sha256,
                        "evaluation": receipt,
                        "registered_by": actor.actor_id,
                    },
                    actor,
                )
        return candidate_id

    def record_shadow(
        self,
        actor: Actor,
        *,
        candidate_id: str,
        baseline_artifact_sha256: str,
        candidate_artifact_sha256: str,
        baseline: tuple[ShadowOutput, ...],
        candidate: tuple[ShadowOutput, ...],
    ) -> dict[str, Any]:
        """Record paired same-input outputs; key parity is not a quality-superiority claim."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            state = self._read(db)
            self._authorize(actor, state, {ActorRole.SYSTEM})
            registration = state["candidates"].get(candidate_id)
            if registration is None:
                raise ValueError("unknown candidate")
            if (
                baseline_artifact_sha256 != state["sandbox_deployment"]["artifact_sha256"]
                or candidate_artifact_sha256 != registration["artifact_sha256"]
            ):
                raise ValueError(
                    "shadow artifact binding does not match current baseline/candidate"
                )
            indexed = []
            for outputs in (baseline, candidate):
                if not isinstance(outputs, tuple) or any(
                    not isinstance(row, ShadowOutput) for row in outputs
                ):
                    raise ValueError("shadow outputs must be tuples of validated ShadowOutput rows")
                by_key = {row.case_id: row for row in outputs}
                if len(by_key) != len(outputs):
                    raise ValueError("duplicate shadow case IDs")
                indexed.append(by_key)
            left, right = indexed
            if set(left) != set(right) or len(left) < state["minimum_shadow_cases"]:
                raise ValueError("shadow cases must match exactly and meet the nonempty minimum")
            if any(left[key].input_sha256 != right[key].input_sha256 for key in left):
                raise ValueError("shadow pairs must use identical input hashes")
            if len({row.input_sha256 for row in left.values()}) < state["minimum_shadow_cases"]:
                raise ValueError("shadow minimum requires distinct input cases, not renamed copies")
            pairs = [
                {
                    "case_id": key,
                    "input_sha256": left[key].input_sha256,
                    "baseline": [str(value) for value in left[key].quantiles],
                    "candidate": [str(value) for value in right[key].quantiles],
                }
                for key in sorted(left)
            ]
            payload = {
                "candidate_id": candidate_id,
                "baseline_artifact_sha256": baseline_artifact_sha256,
                "candidate_artifact_sha256": candidate_artifact_sha256,
                "case_count": len(pairs),
                "pairs": pairs,
                "paired_outputs_sha256": _sha(pairs),
                "mean_absolute_median_difference": str(
                    sum(
                        (abs(left[key].quantiles[2] - right[key].quantiles[2]) for key in left),
                        Decimal(0),
                    )
                    / Decimal(len(left))
                ),
                "quality_superiority_inferred": False,
                "recorded_by": actor.actor_id,
            }
            if state["shadows"].get(candidate_id) != payload:
                self._append(db, "shadow_recorded", payload, actor)
            return payload

    def deploy_sandbox(
        self, actor: Actor, *, candidate_id: str, expected_current_sha256: str
    ) -> dict[str, Any]:
        """Manual sandbox switch only; the authoritative scientific champion never changes."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            state = self._read(db)
            self._authorize(actor, state, {ActorRole.APPROVER})
            current = state["sandbox_deployment"]
            if current["artifact_sha256"] != expected_current_sha256:
                raise ValueError("stale sandbox deployment; refresh before approving")
            item = state["candidates"].get(candidate_id)
            if item is None:
                raise ValueError("unknown candidate")
            if item.get("admission_id") is not None or any(
                admission["proposal_id"] == item["proposal_id"]
                for admission in state["training_admissions"].values()
            ):
                raise PermissionError(
                    "admitted training cannot deploy until executable baseline, shadow, segment, "
                    "and business gates are implemented and verified"
                )
            proposal = state["proposals"][item["proposal_id"]]
            if actor.actor_id in {proposal["creator_id"], item["registered_by"]}:
                raise PermissionError(
                    "independent approver must differ from proposal creator and registrar"
                )
            decision = item["evaluation"]["scientific_decision"]
            if (
                decision["status"] != "ELIGIBLE_BUT_BLOCKED_SYNTHETIC"
                or decision["candidate_won"] is not True
                or decision["engineering_complete"] is not True
                or item["evaluation"]["evidence_sufficient"] is not True
            ):
                raise PermissionError(
                    "losing, insufficient, or failed evaluations cannot deploy even in sandbox"
                )
            if item["evaluation"]["baseline_artifact_sha256"] != expected_current_sha256:
                raise ValueError("evaluation was not against the current sandbox baseline")
            shadow = state["shadows"].get(candidate_id)
            if shadow is None or shadow["baseline_artifact_sha256"] != expected_current_sha256:
                raise ValueError(
                    "completed shadow evidence against the current baseline is required"
                )
            if actor.actor_id == shadow["recorded_by"]:
                raise PermissionError("shadow recorder cannot approve their own sandbox deployment")
            if item["artifact_sha256"] == expected_current_sha256:
                raise ValueError("candidate is already the current sandbox artifact")
            node_id = _sha([candidate_id, state["current_node"], state["audit_head_sha256"]])
            self._append(
                db,
                "sandbox_deployed",
                {
                    "node_id": node_id,
                    "parent": state["current_node"],
                    "candidate_id": candidate_id,
                    "artifact_sha256": item["artifact_sha256"],
                    "model_id": item["model_id"],
                    "approved_by": actor.actor_id,
                    "shadow_sha256": shadow["paired_outputs_sha256"],
                    "scientific_promotion_applied": False,
                },
                actor,
            )
            return self._read(db)

    def rollback_sandbox(
        self, actor: Actor, *, expected_current_sha256: str, reason: str
    ) -> dict[str, Any]:
        """Atomically restore the previous deployment node and its retained artifact bytes."""
        reason = _text(reason)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            state = self._read(db)
            self._authorize(actor, state, {ActorRole.APPROVER})
            current = state["sandbox_deployment"]
            if current["artifact_sha256"] != expected_current_sha256:
                raise ValueError("stale sandbox deployment; refresh before rollback")
            if current["parent"] is None:
                raise ValueError("initial sandbox deployment has no predecessor")
            registration = state["candidates"][current["candidate_id"]]
            proposal = state["proposals"][registration["proposal_id"]]
            if actor.actor_id in {proposal["creator_id"], registration["registered_by"]}:
                raise PermissionError("rollback also requires an independent approver")
            self._append(
                db,
                "sandbox_rolled_back",
                {
                    "from_node": state["current_node"],
                    "to_node": current["parent"],
                    "reason": reason,
                    "approved_by": actor.actor_id,
                    "scientific_promotion_applied": False,
                },
                actor,
            )
            return self._read(db)

    def promote_scientific(self, actor: Actor, *, candidate_id: str) -> None:
        """Explicit denial boundary, including an otherwise eligible synthetic candidate."""
        with self._connect() as db:
            db.execute("BEGIN")
            state = self._read(db)
            self._authorize(actor, state, {ActorRole.APPROVER})
            if candidate_id not in state["candidates"]:
                raise ValueError("unknown candidate")
        raise PermissionError("synthetic_evidence_cannot_promote_champion; sandbox rehearsal only")

    def snapshot(self) -> dict[str, Any]:
        """Read a checksum-verified state projection, with no writes or model inference."""
        with self._connect() as db:
            db.execute("BEGIN")
            return self._read(db)
