from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from nexus.ingestion.r2d_contracts import canonical_json_sha256
from nexus.ingestion.r2d_service_contracts import (
    R2D_A2A_PASS_CHECK_NAMES,
    R2DA2aVerificationCheck,
    R2DA2aVerificationReceipt,
    R2DBrokerAckLedger,
    R2DBrokerAcknowledgement,
    load_r2d_a2a_config,
    migration_sha256,
    r2d_a2a_code_sha256,
    r2d_a2a_config_sha256,
)

pytestmark = pytest.mark.r2d_service

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "ingestion" / "r2d-a2a.yaml"


def _acknowledgement(offset: int = 0) -> R2DBrokerAcknowledgement:
    return R2DBrokerAcknowledgement(
        acknowledgement_kind="broker_acks_all",
        real_broker_ack_claim=True,
        topic="inventory.adjusted.v1",
        partition=0,
        offset=offset,
        key_hex=b"synthetic.wms:ADJ-001".hex(),
        raw_value_sha256="a" * 64,
        broker_timestamp_ms=1_780_000_000_000 + offset,
    )


def _ledger(*acks: R2DBrokerAcknowledgement) -> R2DBrokerAckLedger:
    payload: dict[str, object] = {
        "contract_version": "r2d.a2a.broker-ack.v1",
        "release": "R2D-A2a",
        "producer_acks": "all",
        "real_broker_ack_claim": True,
        "acknowledgements": [item.model_dump(mode="json") for item in acks],
        "acknowledged_record_count": len(acks),
    }
    return R2DBrokerAckLedger.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )


def test_service_config_is_strict_frozen_and_secret_free() -> None:
    config = load_r2d_a2a_config(CONFIG_PATH)

    assert config.release == "R2D-A2a"
    assert config.broker.partition_count == 2
    assert config.broker.producer_acks == "all"
    assert config.broker.producer_enable_idempotence is True
    assert config.broker.consumer_auto_offset_commit is False
    assert config.postgres.schema_name == "r2d_capture"
    assert len(r2d_a2a_config_sha256(config)) == 64
    assert len(r2d_a2a_code_sha256(ROOT)) == 64
    assert "password" not in CONFIG_PATH.read_text(encoding="utf-8").lower()

    with pytest.raises(ValidationError):
        config.broker.partition_count = 3  # type: ignore[misc]
    with pytest.raises(ValidationError):
        config.model_copy(update={"unexpected": True}).model_validate(
            {**config.model_dump(mode="python"), "unexpected": True}
        )


def test_real_broker_ack_ledger_is_exact_unique_and_content_addressed() -> None:
    first = _acknowledgement(0)
    second = _acknowledgement(1)
    ledger = _ledger(first, second)

    assert ledger.real_broker_ack_claim is True
    assert ledger.producer_acks == "all"
    assert ledger.acknowledged_record_count == 2

    with pytest.raises(ValidationError, match="unique transport identities"):
        _ledger(first, first)
    with pytest.raises(ValidationError, match="content SHA-256 mismatch"):
        ledger.model_copy(update={"content_sha256": "f" * 64}).model_validate(
            {**ledger.model_dump(mode="python"), "content_sha256": "f" * 64}
        )


def test_migration_is_versioned_and_defines_atomic_inbox_progress() -> None:
    config = load_r2d_a2a_config(CONFIG_PATH)
    migration_path = ROOT / config.postgres.migration_path
    sql = migration_path.read_text(encoding="utf-8")

    assert len(migration_sha256(ROOT, config)) == 64
    assert "CREATE TABLE IF NOT EXISTS r2d_capture.inbox" in sql
    assert "PRIMARY KEY (topic, partition_id, broker_offset)" in sql
    assert "CREATE TABLE IF NOT EXISTS r2d_capture.partition_progress" in sql
    assert "next_offset BIGINT NOT NULL" in sql
    assert "raw_value BYTEA" in sql
    assert "password" not in sql.lower()


def test_migration_hash_is_stable_across_text_line_endings(tmp_path: Path) -> None:
    config = load_r2d_a2a_config(CONFIG_PATH)
    relative_path = Path(config.postgres.migration_path)
    hashes: set[str] = set()

    for name, content in {
        "lf": b"SELECT 1;\nSELECT 2;\n",
        "crlf": b"SELECT 1;\r\nSELECT 2;\r\n",
        "cr": b"SELECT 1;\rSELECT 2;\r",
    }.items():
        migration_path = tmp_path / name / relative_path
        migration_path.parent.mkdir(parents=True)
        migration_path.write_bytes(content)
        hashes.add(migration_sha256(tmp_path / name, config))

    assert len(hashes) == 1


def test_service_receipt_rejects_reordered_checks_and_tampering() -> None:
    checks = [
        R2DA2aVerificationCheck(name=name, passed=True, evidence_sha256="a" * 64)
        for name in R2D_A2A_PASS_CHECK_NAMES
    ]
    payload: dict[str, object] = {
        "contract_version": "r2d.a2a.verification-receipt.v1",
        "release": "R2D-A2a",
        "status": "PASS",
        "checks": [item.model_dump(mode="json") for item in checks],
        "topic": "inventory.adjusted.v1",
        "partition_count": 2,
        "replication_factor": 1,
        "producer_acks": "all",
        "record_count": 9,
        "starts": [0, 0],
        "ends": [3, 6],
        "next_offsets": [3, 6],
        "data_config_sha256": "b" * 64,
        "service_config_sha256": "c" * 64,
        "migration_sha256": "d" * 64,
        "code_sha256": "e" * 64,
        "broker_ack_sha256": "f" * 64,
        "transcript_sha256": "1" * 64,
        "lineage_sha256": "2" * 64,
        "raw_value_multiset_sha256": "3" * 64,
        "service_foundation_status": "PASS",
        "crash_recovery_status": "NOT_RUN",
        "downstream_parity_status": "NOT_RUN",
        "r2d_a2_status": "IN_PROGRESS",
        "full_r2d_a_status": "OPEN",
        "full_g2_status": "OPEN",
        "exactly_once_claim": False,
    }
    receipt = R2DA2aVerificationReceipt.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )

    assert receipt.code_sha256 == "e" * 64
    with pytest.raises(ValidationError, match="incomplete, duplicated, or reordered"):
        R2DA2aVerificationReceipt.model_validate(
            {**receipt.model_dump(mode="python"), "checks": list(reversed(receipt.checks))}
        )
    with pytest.raises(ValidationError, match="content SHA-256 mismatch"):
        R2DA2aVerificationReceipt.model_validate(
            {**receipt.model_dump(mode="python"), "code_sha256": "9" * 64}
        )


def test_ci_service_job_is_isolated_receipted_and_always_torn_down() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "r2d-service-a2a:" in workflow
    assert "--group r2d-service" in workflow
    assert "verify-service-a2a" in workflow
    assert "r2d-a2a-service-receipt-linux-py313" in workflow
    assert "COMPOSE_PROJECT_NAME: nexus-r2d-a2a-${{ github.run_id }}" in workflow
    assert "if: always()" in workflow
    assert "down -v --remove-orphans" in workflow
    assert "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}" in workflow
