from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from nexus.ingestion.r2d_capture import raw_value_multiset_sha256
from nexus.ingestion.r2d_contracts import load_r2d_config
from nexus.ingestion.r2d_service_contracts import load_r2d_a2a_config
from nexus.r2d import split_lf_jsonl_values
from nexus.simulation import generate_r2_smoke_fixture

if TYPE_CHECKING:
    from nexus.ingestion.r2d_service import R2DA2Runtime

pytestmark = pytest.mark.r2d_service

ROOT = Path(__file__).resolve().parents[1]


def _runtime() -> R2DA2Runtime:
    bootstrap_servers = os.environ.get("NEXUS_R2D_A2_BOOTSTRAP_SERVERS")
    postgres_dsn = os.environ.get("NEXUS_R2D_A2_POSTGRES_DSN")
    if not bootstrap_servers or not postgres_dsn:
        pytest.skip("R2D-A2a service endpoints are not configured")

    from nexus.ingestion.r2d_service import R2DA2Runtime

    return R2DA2Runtime(
        bootstrap_servers=bootstrap_servers,
        postgres_dsn=postgres_dsn,
    )


def test_real_broker_to_postgres_capture_is_exact_and_contiguous(tmp_path: Path) -> None:
    runtime = _runtime()

    from nexus.ingestion.r2d_service import R2DA2Service, dump_service_result

    fixture = generate_r2_smoke_fixture(tmp_path / "source")
    values = split_lf_jsonl_values((fixture.source_dir / "inventory_events.jsonl").read_bytes())
    data_config = load_r2d_config(ROOT / "configs" / "ingestion" / "r2d-a1.yaml")
    service = R2DA2Service(
        project_root=ROOT,
        service_config=load_r2d_a2a_config(ROOT / "configs" / "ingestion" / "r2d-a2a.yaml"),
        data_config=data_config,
        runtime=runtime,
    )

    service.provision_topic()
    with service.connect_postgres() as connection:
        migration_identity = service.apply_migration(connection)
        assert len(migration_identity) == 64
        assert service.initialize_fresh_empty(connection) == (0, 0)

        broker_ack_ledger = service.produce(values)
        result = service.capture_barrier(connection, broker_ack_ledger)

        assert result.starts == (0, 0)
        assert result.next_offsets == result.ends
        assert result.ends[0] > 0 and result.ends[1] > 0
        assert result.broker_ack_ledger.real_broker_ack_claim is True
        assert result.broker_ack_ledger.acknowledged_record_count == len(values) == 9
        assert len(result.transcript.records) == 9
        assert all(item.decision.disposition == "admitted" for item in result.transcript.records)
        captured_values = tuple(
            item.record.value_bytes
            for item in result.transcript.records
            if item.record.value_bytes is not None
        )
        assert raw_value_multiset_sha256(captured_values) == raw_value_multiset_sha256(values)

        inbox_count = connection.execute("SELECT COUNT(*) FROM r2d_capture.inbox").fetchone()
        assert inbox_count is not None and inbox_count[0] == 9

        first = result.transcript.records[0].record
        assert service.persist_record(connection, first) is False
        assert connection.execute("SELECT COUNT(*) FROM r2d_capture.inbox").fetchone() == (9,)

        summary = dump_service_result(result)
        assert data_config.source_contract.raw_value_multiset_sha256 == raw_value_multiset_sha256(
            values
        )
        assert hashlib.sha256(summary.encode("utf-8")).hexdigest()
        assert "password" not in summary.lower()
