"""Deterministic out-of-cycle replanning and stale-recommendation invalidation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, PositiveInt

from nexus.contracts import EventEnvelope, RecommendationManifest


class ReplanTrigger(StrEnum):
    INVENTORY_ADJUSTED = "inventory.adjusted"
    DEMAND_SPIKE = "demand.spike"
    SUPPLIER_DELAYED = "supplier.delayed"
    SHIPMENT_DISRUPTED = "shipment.disrupted"
    ORDER_CANCELLED = "order.cancelled"
    FORECAST_DRIFTED = "forecast.drifted"
    POLICY_CHANGED = "policy.changed"


class ReplanDisposition(StrEnum):
    IGNORE = "ignore"
    TRIGGER_NOW = "trigger_now"
    DEFER_COOLDOWN = "defer_cooldown"
    INVALIDATE_ONLY = "invalidate_only"


class ReplanEvaluation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)

    disposition: ReplanDisposition
    material: bool
    invalidate_active_manifest: bool
    eligible_at: datetime | None = None
    reason: str


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone offset")
    return value.astimezone(UTC)


class ReplanPolicy(BaseModel):
    """Evaluate material events without causing replan storms.

    A material event can invalidate the active recommendation immediately, while the
    expensive replan still respects a cooldown. The two decisions are intentionally
    separate so stale advice is never presented merely because compute is throttled.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)

    material_event_types: frozenset[str] = Field(
        default_factory=lambda: frozenset(item.value for item in ReplanTrigger)
    )
    invalidating_event_types: frozenset[str] = Field(
        default_factory=lambda: frozenset(
            {
                ReplanTrigger.INVENTORY_ADJUSTED.value,
                ReplanTrigger.DEMAND_SPIKE.value,
                ReplanTrigger.SUPPLIER_DELAYED.value,
                ReplanTrigger.SHIPMENT_DISRUPTED.value,
                ReplanTrigger.ORDER_CANCELLED.value,
                ReplanTrigger.POLICY_CHANGED.value,
            }
        )
    )
    cooldown_seconds: PositiveInt = 1_800
    stale_after_seconds: PositiveInt = 21_600

    def evaluate(
        self,
        event: EventEnvelope,
        *,
        now: datetime,
        last_replan_at: datetime | None = None,
        active_manifest: RecommendationManifest | None = None,
    ) -> ReplanEvaluation:
        instant = _aware_utc(now, "now")
        if event.occurred_at > instant:
            raise ValueError("event.occurred_at cannot be in the future")
        last_replan = (
            None if last_replan_at is None else _aware_utc(last_replan_at, "last_replan_at")
        )
        if last_replan is not None and last_replan > instant:
            raise ValueError("last_replan_at cannot be in the future")

        material = event.event_type in self.material_event_types
        invalid, invalidation_reason = self._invalidation(active_manifest, event, instant)

        if not material:
            return ReplanEvaluation(
                disposition=(
                    ReplanDisposition.INVALIDATE_ONLY if invalid else ReplanDisposition.IGNORE
                ),
                material=False,
                invalidate_active_manifest=invalid,
                reason=invalidation_reason or "event type is not a material replan trigger",
            )

        eligible_at = (
            None if last_replan is None else last_replan + timedelta(seconds=self.cooldown_seconds)
        )
        if eligible_at is not None and instant < eligible_at:
            return ReplanEvaluation(
                disposition=ReplanDisposition.DEFER_COOLDOWN,
                material=True,
                invalidate_active_manifest=invalid,
                eligible_at=eligible_at,
                reason=(
                    "material event invalidated active advice; replan deferred by cooldown"
                    if invalid
                    else "material event replan deferred by cooldown"
                ),
            )

        return ReplanEvaluation(
            disposition=ReplanDisposition.TRIGGER_NOW,
            material=True,
            invalidate_active_manifest=invalid,
            eligible_at=instant,
            reason=invalidation_reason or "material event is eligible for an immediate replan",
        )

    def _invalidation(
        self,
        manifest: RecommendationManifest | None,
        event: EventEnvelope,
        now: datetime,
    ) -> tuple[bool, str | None]:
        if manifest is None:
            return False, None
        if now >= manifest.valid_until:
            return True, "active recommendation passed valid_until"
        if now >= manifest.decision_time + timedelta(seconds=self.stale_after_seconds):
            return True, "active recommendation exceeded the maximum decision age"
        if (
            event.event_type in self.invalidating_event_types
            and event.occurred_at > manifest.decision_time
        ):
            return True, "material post-decision fact invalidated the active recommendation"
        return False, None
