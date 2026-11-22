"""Strict private local-service manifest; parsing never opens or migrates services."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nexus.portfolio.broker_runtime import RuntimeBrokerConfig
from nexus.portfolio.postgres_backend import PostgresConfig, _dsn
from nexus.portfolio.runtime_service import _postgres_store
from nexus.portfolio.source_freshness import SourceWatchPolicy
from nexus.portfolio.store import StoreError
from nexus.portfolio.transport_ledger import TransportBinding, runtime_target
from nexus.portfolio.transport_runtime import TransportFenceConfig


def _object(value: Any, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError("Unexpected manifest fields")
    return value


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("Duplicate manifest field")
        result[key] = value
    return result


def _database(value: Any, artifact_root: Path) -> PostgresConfig:
    value = _object(value, {"dsn", "schema"})
    if any(not isinstance(v, str) or not v for v in value.values()):
        raise ValueError("Explicit database required")
    return PostgresConfig(value["dsn"], value["schema"], artifact_root)


def _binding(value: Any) -> TransportBinding:
    value = _object(
        value,
        {
            "contract",
            "endpoint",
            "topic",
            "group",
            "locations",
            "cluster_id",
            "topic_id",
            "partition_count",
            "purpose",
            "runtime_target_sha256",
            "initial_offset",
        },
    )
    if (
        not isinstance(value["locations"], list)
        or type(value["initial_offset"]) is not int
        or value["initial_offset"] != 0
    ):
        raise ValueError("Explicit scope and zero origin required")
    result = TransportBinding(
        RuntimeBrokerConfig(
            value["endpoint"], value["topic"], value["group"], tuple(value["locations"])
        ),
        value["cluster_id"],
        value["topic_id"],
        value["partition_count"],
        value["purpose"],
        value["runtime_target_sha256"],
    )
    if result.payload() != value:
        raise ValueError("Noncanonical binding")
    return result


@dataclass(frozen=True)
class ServiceConfig:
    root: Path
    workflow: PostgresConfig
    fence: TransportFenceConfig
    audit: PostgresConfig
    audit_binding: TransportBinding
    manifest_sha256: str


def read_service_config(path: Path, *, expected_sha256: str | None = None) -> ServiceConfig:
    """No arbitrary client options, migration credentials, resets or ambient discovery."""
    try:
        location = path.absolute()
        if (
            location.anchor.startswith(("\\\\", "//"))
            or ".." in location.parts
            or any(p.is_symlink() or p.is_junction() for p in (*location.parents, location))
            or not location.is_file()
            or location.stat().st_size > 32768
        ):
            raise ValueError("Use bounded unlinked local manifest")
        raw = location.read_bytes()
        fingerprint = hashlib.sha256(raw).hexdigest()
        if len(raw) > 32768 or (expected_sha256 is not None and fingerprint != expected_sha256):
            raise ValueError("Manifest changed after selection")
        decoded = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs)
        health_enabled = (
            isinstance(decoded, dict) and decoded.get("contract") == "nexus.local-service.v2"
        )
        value = _object(
            decoded,
            {
                "contract",
                "runtime_root",
                "workflow",
                "operations",
                "audit",
                "source_watch",
            }
            | ({"capture_health"} if health_enabled else set()),
        )
        if value["contract"] not in {
            "nexus.local-service.v1",
            "nexus.local-service.v2",
        } or not isinstance(value["runtime_root"], str):
            raise ValueError("Unknown manifest contract")
        root = Path(value["runtime_root"])
        if not root.is_absolute():
            raise ValueError("Explicit root required")
        workflow = _database(value["workflow"], root / "evidence" / "initial")
        base = _postgres_store(root, workflow)
        operations = _object(value["operations"], {"database", "binding"})
        audit = _object(value["audit"], {"database", "binding"})
        ops_config = _database(operations["database"], root / "transport" / "operations")
        audit_config = _database(audit["database"], root / "transport" / "audit")
        ops_binding, audit_binding = _binding(operations["binding"]), _binding(audit["binding"])
        if (
            len({workflow.schema, ops_config.schema, audit_config.schema}) != 3
            or _dsn(workflow.dsn) != _dsn(ops_config.dsn)
            or ops_binding.purpose != "operations"
            or audit_binding.purpose != "audit"
            or ops_binding.runtime_target_sha256 != runtime_target(base.control)
            or ops_binding.broker.group_id == audit_binding.broker.group_id
        ):
            raise ValueError("Require independent groups and schemas bound to this workflow")
        left, right = ops_binding.payload(), audit_binding.payload()
        if any(
            left[k] != right[k]
            for k in (
                "endpoint",
                "topic",
                "locations",
                "cluster_id",
                "topic_id",
                "partition_count",
            )
        ):
            raise ValueError("Both groups must capture the same pinned stream")
        watch = _object(
            value["source_watch"], {"source_id", "source_root", "max_age_seconds", "contract"}
        )
        policy = SourceWatchPolicy(**watch)
        health_age = None
        if health_enabled:
            health_age = _object(value["capture_health"], {"max_age_seconds"})["max_age_seconds"]
            if type(health_age) is not int:
                raise ValueError("Explicit integer capture-health lifetime required")
        fence = TransportFenceConfig(ops_config, ops_binding, True, policy, True, True, health_age)
        _postgres_store(root, workflow, transport_fence=fence)
        return ServiceConfig(root, workflow, fence, audit_config, audit_binding, fingerprint)
    except (ValueError, TypeError, KeyError, OSError, StoreError):
        raise StoreError("configuration_error", "Invalid private local service manifest") from None
