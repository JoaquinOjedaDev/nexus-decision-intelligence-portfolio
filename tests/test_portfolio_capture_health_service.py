"""Real two-group lag observations; not durable authorization or a load test."""

import json
import time
from pathlib import Path

import pytest
from test_portfolio_broker_runtime_service import Broker
from test_portfolio_broker_runtime_service import broker as broker
from test_portfolio_postgres_backend_service import Cluster
from test_portfolio_postgres_backend_service import cluster as cluster
from test_portfolio_postgres_runtime_service import clock as clock
from test_portfolio_postgres_runtime_service import runtime as runtime
from test_portfolio_transport_consumer_service import capture, setup

from nexus.portfolio.broker_runtime import BrokerAdmissionError
from nexus.portfolio.capture_health import capture_health_reason
from nexus.portfolio.postgres_runtime import PostgresRuntimeStore
from nexus.portfolio.transport_consumer import DurableBrokerConsumer
from nexus.storage import file_sha256

pytestmark = pytest.mark.portfolio_service


def test_real_backlog_independent_groups_idle_expiry_and_restart(
    cluster: Cluster, broker: Broker, runtime: PostgresRuntimeStore, tmp_path: Path
) -> None:
    ops, binding = setup(cluster, broker, runtime, tmp_path / "operations")
    audit, audit_binding = setup(cluster, broker, runtime, tmp_path / "audit", audit=True)
    one = DurableBrokerConsumer(ops, binding)
    two = DurableBrokerConsumer(audit, audit_binding)
    receipt = {
        "contract": "nexus.capture-health-acceptance.v1",
        "claim": "real_measurements_not_durable_action_fence_or_load_gate",
        "operations_binding": binding.payload(),
        "audit_binding": audit_binding.payload(),
        "code_sha256": {
            name: file_sha256(Path(name))
            for name in (
                "src/nexus/portfolio/capture_health.py",
                "src/nexus/portfolio/transport_consumer.py",
                "src/nexus/portfolio/transport_ledger.py",
            )
        },
    }
    try:
        empty = one.sample_health()
        assert (
            capture_health_reason(binding, empty, now=time.time(), max_age_seconds=30)
            == "caught_up_at_observation"
        )
        broker.produce(b"synthetic capture-health record")
        lag = one.sample_health()
        assert lag.captured == lag.committed == (0, 0) and lag.high == (1, 0)
        assert (
            capture_health_reason(binding, lag, now=time.time(), max_age_seconds=30)
            == "capture_lagging"
        )
        assert ops.capture_progress(binding) == (0, 0)
        capture(one, 1)
        caught = one.sample_health()
        other_lag = two.sample_health()
        assert caught.captured == caught.committed == caught.high == (1, 0)
        assert other_lag.captured == (0, 0) and other_lag.high == (1, 0)
        capture(two, 1)
        audit_caught = two.sample_health()
        assert audit_caught.captured == audit_caught.committed == audit_caught.high == (1, 0)
        one.close()
        with pytest.raises(BrokerAdmissionError, match="requires_reconciliation"):
            one.sample_health()
        time.sleep(max(0, caught.started_at + 1.05 - time.time()))
        assert (
            capture_health_reason(binding, caught, now=time.time(), max_age_seconds=1)
            == "capture_observation_expired"
        )
        reopened = DurableBrokerConsumer(ops, binding)
        try:
            restarted = reopened.sample_health()
            assert restarted.consumer_instance_id != caught.consumer_instance_id
            assert restarted.captured == restarted.committed == restarted.high == (1, 0)
            assert ops.verify_history(binding)["delivered"] == [0, 0]
        finally:
            reopened.close()
        receipt.update(
            status="passed",
            empty=empty.payload(),
            lag=lag.payload(),
            caught=caught.payload(),
            audit_lag=other_lag.payload(),
            audit_caught=audit_caught.payload(),
            restarted=restarted.payload(),
        )
    finally:
        one.close()
        two.close()
        (tmp_path / "capture-health-receipt.json").write_text(
            json.dumps(receipt, indent=2), encoding="utf-8"
        )
