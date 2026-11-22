"""Persisted, generation-bound capture health for explicit operations-ledger V12."""

from __future__ import annotations

import json
from typing import Any, ClassVar, cast
from uuid import UUID

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor
from nexus.portfolio.capture_health import CaptureObservation, capture_health_reason, valid_instant
from nexus.portfolio.recovery_resolution import RecoveryResolutionLedger
from nexus.portfolio.store import StoreError, canonical, digest
from nexus.portfolio.transport_ledger import TransportBinding, TransportLedger


class CaptureHealthLedger(RecoveryResolutionLedger):
    """Older ledger clients refuse this schema; policy cannot be changed on reopen."""

    _versions: ClassVar[tuple[int, ...]] = (5, 9, 10, 11, 12)
    _prior_ledger_type: ClassVar[type[TransportLedger]] = RecoveryResolutionLedger
    _schema_columns: ClassVar[dict[str, dict[str, str]]] = {
        **RecoveryResolutionLedger._schema_columns,
        "capture_health_policy": {
            "singleton": "integer",
            "payload": "text",
            "payload_sha256": "text",
        },
        "capture_instances": {
            "id": "text",
            "sequence": "bigint",
            "payload": "text",
            "payload_sha256": "text",
        },
        "capture_observations": {
            "id": "text",
            "sequence": "bigint",
            "instance_id": "text",
            "payload": "text",
            "payload_sha256": "text",
        },
    }
    _schema_tables: ClassVar[set[str]] = set(_schema_columns)
    _uniques: ClassVar[dict[tuple[str, str], list[int]]] = {
        **RecoveryResolutionLedger._uniques,
        ("capture_instances", "capture_instances_sequence_key"): [2],
        ("capture_observations", "capture_observations_sequence_key"): [2],
    }
    _foreign_keys: ClassVar[dict[tuple[str, str], tuple[list[int], str, list[int]]]] = {
        **RecoveryResolutionLedger._foreign_keys,
        ("capture_observations", "capture_observations_instance_id_fkey"): (
            [3],
            "capture_instances",
            [1],
        ),
    }
    _history_triggers: ClassVar[dict[str, str]] = {
        **RecoveryResolutionLedger._history_triggers,
        "capture_health_policy": "immutable_capture_health_policy",
        "capture_instances": "immutable_capture_instance",
        "capture_observations": "immutable_capture_observation",
    }
    _forbidden_grants: ClassVar[dict[str, str]] = {
        **RecoveryResolutionLedger._forbidden_grants,
        "capture_health_policy": "INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER",
        "capture_instances": "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER",
        "capture_observations": "UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER",
    }

    def __init__(
        self, *args: Any, health_binding: TransportBinding, max_age_seconds: int, **kwargs: Any
    ):
        super().__init__(*args, **kwargs)
        if (
            not isinstance(health_binding, TransportBinding)
            or health_binding.purpose != "operations"
        ):
            raise ValueError("Explicit operations health binding required")
        capture_health_reason(health_binding, None, now=0.0, max_age_seconds=max_age_seconds)
        self.health_binding, self.max_age_seconds = health_binding, max_age_seconds

    def _policy(self) -> dict[str, Any]:
        return {
            "contract": "nexus.capture-health-policy.v1",
            "binding_sha256": digest(self.health_binding.payload()),
            "max_age_seconds": self.max_age_seconds,
            "maximum_lag": 0,
        }

    def _constraint_checks(self) -> dict[tuple[str, str], tuple[int, str]]:
        return {
            **super()._constraint_checks(),
            ("capture_health_policy", "capture_health_policy_singleton_check"): (1, "singleton=1"),
        }

    def _initialize_extension(self, db: Any) -> None:
        value = self._policy()
        expected = self.health_binding.payload()
        rows = self._query(db, "SELECT * FROM {t}", t="transport_binding").fetchall()
        if rows != [
            {"singleton": 1, "payload": canonical(expected), "payload_sha256": digest(expected)}
        ]:
            raise StoreError(
                "capture_binding_changed", "Initialize the exact bound ledger first", 409
            )
        self._query(
            db,
            "INSERT INTO {t} VALUES (1,%s,%s)",
            (canonical(value), digest(value)),
            t="capture_health_policy",
        )

    def _verify_migration_history(self, db: Any, expected_hash: str, application_role: str) -> None:
        super()._verify_migration_history(db, expected_hash, application_role)
        value = self._policy()
        if self._query(db, "SELECT * FROM {t}", t="capture_health_policy").fetchall() != [
            {"singleton": 1, "payload": canonical(value), "payload_sha256": digest(value)}
        ]:
            raise StoreError("capture_policy_changed", "Retained capture policy differs", 409)

    def _writer(self, binding: TransportBinding, actor: Actor) -> None:
        if binding != self.health_binding:
            raise StoreError("capture_binding_changed", "Exact health binding required", 409)
        if actor.role is not ActorRole.SYSTEM or actor.actor_id != "capture-operations":
            raise StoreError("role_denied", "Owning capture service identity required", 403)
        if set(actor.locations) != set(binding.broker.locations):
            raise StoreError("scope_denied", "Complete capture scope required", 403)

    def _latest(self, db: Any, table: str) -> dict[str, Any] | None:
        return cast(
            dict[str, Any] | None,
            self._query(db, "SELECT * FROM {t} ORDER BY sequence DESC LIMIT 1", t=table).fetchone(),
        )

    @staticmethod
    def _value(row: dict[str, Any]) -> dict[str, Any]:
        value = json.loads(row["payload"])
        if (
            not isinstance(value, dict)
            or canonical(value) != row["payload"]
            or digest(value) != row["payload_sha256"]
        ):
            raise StoreError("integrity_error", "Capture history digest differs", 409)
        return cast(dict[str, Any], value)

    def _instance(self, row: dict[str, Any]) -> dict[str, Any]:
        value = self._value(row)
        if (
            set(value) != {"id", "started_at", "binding_sha256"}
            or value["id"] != row["id"]
            or str(UUID(value["id"])) != value["id"]
            or not valid_instant(value["started_at"])
            or value["binding_sha256"] != digest(self.health_binding.payload())
            or type(row["sequence"]) is not int
            or row["sequence"] < 1
        ):
            raise StoreError("integrity_error", "Capture instance history differs", 409)
        return value

    def _health_observation(self, row: dict[str, Any]) -> CaptureObservation:
        value = self._value(row)
        try:
            for key in ("low", "high", "captured", "committed"):
                if type(value[key]) is not list:
                    raise ValueError
                value[key] = tuple(value[key])
            observed = CaptureObservation(**value)
        except (TypeError, ValueError, KeyError):
            raise StoreError(
                "integrity_error", "Capture observation contract differs", 409
            ) from None
        if (
            observed.observation_id != row["id"]
            or observed.consumer_instance_id != row["instance_id"]
            or observed.binding_sha256 != digest(self.health_binding.payload())
            or len(observed.captured) != self.health_binding.partition_count
        ):
            raise StoreError("integrity_error", "Capture observation identity differs", 409)
        return observed

    def begin_capture(
        self, binding: TransportBinding, instance_id: str, actor: Actor
    ) -> dict[str, Any]:
        self._writer(binding, actor)
        if str(UUID(instance_id)) != instance_id:
            raise ValueError("Canonical capture instance required")
        with self._transport(binding, write=True) as db:
            self._progress(db, binding, delivery=False, lock=True)
            latest = self._latest(db, "capture_instances")
            if latest is not None and latest["id"] == instance_id:
                return self._instance(latest)
            if self._query(
                db, "SELECT id FROM {t} WHERE id=%s", (instance_id,), t="capture_instances"
            ).fetchone():
                raise StoreError(
                    "capture_generation_superseded", "Old capture generation cannot resume", 409
                )
            now = self._now().timestamp()
            previous = self._latest(db, "capture_observations")
            if (latest and now < self._instance(latest)["started_at"]) or (
                previous and now < self._health_observation(previous).completed_at
            ):
                raise StoreError("capture_clock_regressed", "Capture clock regressed", 409)
            value = {
                "id": instance_id,
                "started_at": now,
                "binding_sha256": digest(binding.payload()),
            }
            self._query(
                db,
                "INSERT INTO {t} VALUES (%s,%s,%s,%s)",
                (
                    instance_id,
                    1 if latest is None else latest["sequence"] + 1,
                    canonical(value),
                    digest(value),
                ),
                t="capture_instances",
            )
            return value

    def record_health(
        self, binding: TransportBinding, observed: CaptureObservation, actor: Actor
    ) -> dict[str, Any]:
        self._writer(binding, actor)
        with self._transport(binding, write=True) as db:
            progress = self._progress(db, binding, delivery=False, lock=True)
            instance = self._latest(db, "capture_instances")
            if instance is None or instance["id"] != observed.consumer_instance_id:
                raise StoreError(
                    "capture_generation_superseded", "Current capture generation required", 409
                )
            now = self._now().timestamp()
            reason = capture_health_reason(
                binding, observed, now=now, max_age_seconds=self.max_age_seconds
            )
            if reason not in {"caught_up_at_observation", "capture_lagging", "capture_ack_pending"}:
                raise StoreError(reason, "Capture sample is not current and bound", 409)
            if observed.started_at < self._instance(instance)[
                "started_at"
            ] or observed.captured != tuple(r["next_offset"] for r in progress):
                raise StoreError(
                    "capture_sample_superseded", "Re-probe current durable capture", 409
                )
            latest = self._latest(db, "capture_observations")
            if latest:
                previous = self._health_observation(latest)
                if previous.observation_id == observed.observation_id and previous == observed:
                    return {"recorded": False, "reason": reason, "sequence": latest["sequence"]}
                if (
                    observed.started_at <= previous.started_at
                    or observed.completed_at < previous.completed_at
                    or any(
                        a < b
                        for current, old in (
                            (observed.low, previous.low),
                            (observed.high, previous.high),
                            (observed.captured, previous.captured),
                            (observed.committed, previous.committed),
                        )
                        for a, b in zip(current, old, strict=True)
                    )
                ):
                    raise StoreError(
                        "capture_sample_superseded", "Capture sample order regressed", 409
                    )
            sequence = 1 if latest is None else latest["sequence"] + 1
            self._query(
                db,
                "INSERT INTO {t} VALUES (%s,%s,%s,%s,%s)",
                (
                    observed.observation_id,
                    sequence,
                    observed.consumer_instance_id,
                    canonical(observed.payload()),
                    observed.sha256,
                ),
                t="capture_observations",
            )
            return {"recorded": True, "reason": reason, "sequence": sequence}

    def capture_action_bounds(
        self, db: Any, binding: TransportBinding, now: float
    ) -> tuple[float, float]:
        if binding != self.health_binding:
            raise StoreError(
                "capture_binding_changed", "Exact capture health binding required", 409
            )
        instance = self._latest(db, "capture_instances")
        row = self._latest(db, "capture_observations")
        observed = None if row is None else self._health_observation(row)
        if (
            instance is None
            or observed is None
            or self._instance(instance)["id"] != observed.consumer_instance_id
        ):
            raise StoreError(
                "capture_observation_missing", "Current capture generation has no observation", 409
            )
        reason = capture_health_reason(
            binding, observed, now=now, max_age_seconds=self.max_age_seconds
        )
        if reason != "caught_up_at_observation":
            raise StoreError(reason, "Capture health fences new actions", 409)
        current = tuple(r["next_offset"] for r in self._progress(db, binding, delivery=False))
        if observed.captured != current:
            raise StoreError(
                "capture_sample_superseded", "Capture progressed; a new sample is required", 409
            )
        return observed.completed_at, observed.started_at + self.max_age_seconds

    def _verify_extension_history(self, db: Any, binding: TransportBinding) -> dict[str, Any]:
        result = super()._verify_extension_history(db, binding)
        previous_time = -1.0
        instance_count = 0
        observation_count = 0
        for table in ("capture_instances", "capture_observations"):
            sequence = 0
            previous_started, previous_completed, previous_generation = -1.0, -1.0, 0
            previous_observed: CaptureObservation | None = None
            while True:
                rows = self._query(
                    db,
                    "SELECT * FROM {t} WHERE sequence>%s ORDER BY sequence LIMIT 1000",
                    (sequence,),
                    t=table,
                ).fetchall()
                if not rows:
                    break
                for row in rows:
                    if row["sequence"] != sequence + 1:
                        raise StoreError("integrity_error", "Capture history sequence gap", 409)
                    sequence += 1
                    if table == "capture_instances":
                        value = self._instance(row)
                        if value["started_at"] < previous_time:
                            raise StoreError(
                                "integrity_error", "Capture generation time regressed", 409
                            )
                        previous_time = value["started_at"]
                        instance_count += 1
                        continue
                    observed = self._health_observation(row)
                    instance = self._query(
                        db,
                        "SELECT * FROM {t} WHERE id=%s",
                        (observed.consumer_instance_id,),
                        t="capture_instances",
                    ).fetchone()
                    if instance is None:
                        raise StoreError(
                            "integrity_error", "Capture observation has no generation", 409
                        )
                    epoch = self._instance(instance)
                    next_epoch = self._query(
                        db,
                        "SELECT * FROM {t} WHERE sequence=%s",
                        (instance["sequence"] + 1,),
                        t="capture_instances",
                    ).fetchone()
                    if (
                        observed.started_at < epoch["started_at"]
                        or observed.started_at <= previous_started
                        or observed.completed_at < previous_completed
                        or instance["sequence"] < previous_generation
                        or (
                            previous_observed is not None
                            and any(
                                a < b
                                for current, old in (
                                    (observed.low, previous_observed.low),
                                    (observed.high, previous_observed.high),
                                    (observed.captured, previous_observed.captured),
                                    (observed.committed, previous_observed.committed),
                                )
                                for a, b in zip(current, old, strict=True)
                            )
                        )
                        or (
                            next_epoch is not None
                            and observed.completed_at > self._instance(next_epoch)["started_at"]
                        )
                    ):
                        raise StoreError(
                            "integrity_error", "Capture observation history order differs", 409
                        )
                    previous_started, previous_completed = (
                        observed.started_at,
                        observed.completed_at,
                    )
                    previous_generation = instance["sequence"]
                    previous_observed = observed
                    observation_count += 1
        return {
            **result,
            "capture_instances": instance_count,
            "capture_observations": observation_count,
        }

    def health_status(self, binding: TransportBinding, actor: Actor) -> dict[str, Any]:
        self._authorize(binding, actor)
        with self._transport(binding, write=False) as db:
            try:
                self.capture_action_bounds(db, binding, self._now().timestamp())
                reason = "caught_up_at_observation"
            except StoreError as error:
                if error.code not in {
                    "capture_observation_missing",
                    "capture_observation_expired",
                    "capture_lagging",
                    "capture_ack_pending",
                    "capture_sample_superseded",
                    "capture_clock_regressed",
                }:
                    raise
                reason = error.code
            row = self._latest(db, "capture_observations")
            return {
                "reason": reason,
                "max_age_seconds": self.max_age_seconds,
                "policy_sha256": digest(self._policy()),
                "observation_sequence": 0 if row is None else row["sequence"],
                "claim": "status_snapshot_not_action_authorization",
            }
