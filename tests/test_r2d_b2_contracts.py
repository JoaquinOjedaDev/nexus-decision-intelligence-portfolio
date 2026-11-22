from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from pydantic import ValidationError
from typer.main import get_command
from typer.testing import CliRunner

from nexus.cli import app
from nexus.ingestion.r2d_b2_contracts import (
    R2D_B2A_EVIDENCE_DATASETS,
    R2D_B2A_PASS_CHECK_NAMES,
    R2DB2AAckLedger,
    R2DB2AArtifactRef,
    R2DB2AVerificationReceipt,
    load_r2d_b2a_config,
    load_r2d_b2a_service_image,
    r2d_b2a_code_sha256,
    r2d_b2a_config_sha256,
)
from nexus.modeling.r3_artifacts import (
    canonical_json_bytes,
    canonical_json_sha256,
    write_immutable_json_manifest,
)
from nexus.r2d_b2a import R2DB2AGovernanceError, load_r2d_b2a_receipt

pytestmark = pytest.mark.r2d
runner = CliRunner()


def _ack_payload(
    *,
    phase: str = "source_fixture",
    topic: str = "inventory.adjusted.v1",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "contract_version": "r2d.b2a.broker-ack.v1",
        "phase": phase,
        "topic": topic,
        "producer_acks": "all",
        "acknowledgements": [
            {
                "acknowledgement_kind": "broker_acks_all",
                "topic": topic,
                "partition": 0,
                "offset": 0,
                "key_hex": "61",
                "value_sha256": "1" * 64,
                "broker_timestamp_ms": 1,
            }
        ],
    }
    return {**payload, "content_sha256": canonical_json_sha256(payload)}


def _receipt_bundle(tmp_path: Path) -> tuple[Path, R2DB2AVerificationReceipt]:
    inherited = "61dacd3ef618138b2e91701c3bc032e91e978065c08bb916429e1a2f18323b3e"
    references: list[R2DB2AArtifactRef] = []
    hashes: dict[str, str] = {}
    service_image_sha256 = ""
    for dataset in R2D_B2A_EVIDENCE_DATASETS:
        name = dataset.rsplit(".", maxsplit=1)[-1]
        if name == "service_image":
            image_payload = {
                "contract_version": "r2d.b2a.service-image.v1",
                "service": "redpanda",
                "configured_reference": "docker.redpanda.com/redpandadata/redpanda:v26.1.9",
                "observed_image_id": f"sha256:{'a' * 64}",
                "operating_system": "linux",
                "architecture": "amd64",
            }
            service_image_sha256 = canonical_json_sha256(image_payload)
            payload = {**image_payload, "content_sha256": service_image_sha256}
        elif name == "inherited_b1":
            payload = {
                "contract_version": "r2d.b2a.inherited-b1.v1",
                "config_path": "configs/ingestion/r2d-b1.yaml",
                "config_sha256": inherited,
                "business_validation_owner": "R2A",
            }
        elif name in {"source_ack", "dead_letter_ack", "redrive_ack"}:
            phase, topic = {
                "source_ack": ("source_fixture", "inventory.adjusted.v1"),
                "dead_letter_ack": ("dead_letter", "inventory.adjusted.dlq.v1"),
                "redrive_ack": ("redrive_audit", "inventory.adjusted.redrive.v1"),
            }[name]
            payload = _ack_payload(phase=phase, topic=topic)
        else:
            payload = {"contract_version": f"r2d.b2a.{name}.test.v1", "dataset": dataset}
        artifact = write_immutable_json_manifest(
            tmp_path,
            dataset=dataset,
            manifest=payload,
            schema_identity={"contract": payload["contract_version"]},
        )
        hashes[name] = (
            str(payload["content_sha256"])
            if name in {"source_ack", "dead_letter_ack", "redrive_ack"}
            else artifact.content_sha256
        )
        references.append(
            R2DB2AArtifactRef(
                dataset=dataset,
                relative_path=artifact.path.relative_to(tmp_path).as_posix(),
                content_sha256=artifact.content_sha256,
                schema_sha256=artifact.schema_sha256,
                file_sha256=artifact.file_sha256,
                row_count=1,
            )
        )
    artifact_set = canonical_json_sha256([item.model_dump(mode="json") for item in references])
    check_evidence = {
        "inherited_b1_authority": inherited,
        "observed_service_image": service_image_sha256,
        "exact_service_topic_topology": hashes["topic_topology"],
        "real_source_acks_all": hashes["source_ack"],
        "live_poison_isolation": hashes["route"],
        "byte_preserving_dlq_round_trip": hashes["dead_letter_round_trip"],
        "append_only_operator_redrive": hashes["redrive"],
        "replacement_returns_to_source": hashes["replacement_ack"],
        "original_dlq_immutability": hashes["dead_letter_round_trip"],
        "manual_commit_convergence": hashes["manual_commit"],
        "immutable_artifact_set": artifact_set,
        "honest_scope": hashes["scope"],
    }
    payload: dict[str, object] = {
        "contract_version": "r2d.b2a.verification-receipt.v1",
        "release": "R2D-B2a",
        "status": "PASS",
        "checks": [
            {"name": name, "passed": True, "evidence_sha256": check_evidence[name]}
            for name in R2D_B2A_PASS_CHECK_NAMES
        ],
        "evidence_refs": [item.model_dump(mode="json") for item in references],
        "config_sha256": r2d_b2a_config_sha256(load_r2d_b2a_config()),
        "code_sha256": r2d_b2a_code_sha256(),
        "inherited_b1_config_sha256": inherited,
        "service_image_sha256": service_image_sha256,
        "service_image_file_sha256": references[1].file_sha256,
        "topology_sha256": hashes["topic_topology"],
        "source_ack_sha256": hashes["source_ack"],
        "route_evidence_sha256": hashes["route"],
        "dead_letter_ack_sha256": hashes["dead_letter_ack"],
        "dead_letter_round_trip_sha256": hashes["dead_letter_round_trip"],
        "redrive_ack_sha256": hashes["redrive_ack"],
        "redrive_evidence_sha256": hashes["redrive"],
        "replacement_ack_sha256": hashes["replacement_ack"],
        "manual_commit_sha256": hashes["manual_commit"],
        "artifact_set_sha256": artifact_set,
        "scope_sha256": hashes["scope"],
        "source_record_count": 2,
        "forward_record_count": 1,
        "dead_letter_record_count": 1,
        "redrive_request_count": 1,
        "replacement_record_count": 1,
        "runtime": {
            "operating_system": "Linux",
            "machine": "x86_64",
            "python_version": "3.13.0",
            "github_runner_os_attested": True,
        },
        "portable_b1_status": "PASS",
        "service_dead_letter_topic_status": "PASS",
        "persistent_dagster_status": "NOT_RUN",
        "retained_service_restart_status": "NOT_RUN",
        "operational_alert_delivery_status": "NOT_RUN",
        "full_r2d_b_status": "IN_PROGRESS",
        "full_g2_status": "OPEN",
        "exactly_once_claim": False,
    }
    receipt = R2DB2AVerificationReceipt.model_validate(
        {**payload, "content_sha256": canonical_json_sha256(payload)}
    )
    receipt_artifact = write_immutable_json_manifest(
        tmp_path,
        dataset="r2d.b2a.verification_receipt",
        manifest=receipt.model_dump(mode="json"),
        schema_identity={"contract": receipt.contract_version},
    )
    return receipt_artifact.path, receipt


def test_b2a_config_is_strict_frozen_and_inherits_b1() -> None:
    config = load_r2d_b2a_config()
    assert config.broker.topics == (
        "inventory.adjusted.v1",
        "inventory.adjusted.dlq.v1",
        "inventory.adjusted.redrive.v1",
    )
    assert config.broker.retention_ms == -1
    assert config.scope.service_dead_letter_topic_status == "PASS"
    assert config.scope.persistent_dagster_status == "NOT_RUN"
    assert config.scope.full_g2_status == "OPEN"
    assert r2d_b2a_config_sha256(config) == (
        "48df248406fe9d7efad755418cd43f8410763984833ecdb7ceee98c584bfd9e5"
    )
    with pytest.raises(ValidationError):
        config.release = "changed"  # type: ignore[misc]


@pytest.mark.r2d_service
def test_topic_provisioning_sets_and_audits_non_expiring_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nexus.ingestion.r2d_b2_service import R2DB2ARuntime, R2DB2AService

    created: list[object] = []

    class Future:
        def __init__(self, value: object) -> None:
            self.value = value

        def result(self, **_: object) -> object:
            return self.value

    class Admin:
        def __init__(self, _: object) -> None:
            pass

        def create_topics(self, topics: list[object], **_: object) -> dict[str, Future]:
            created.extend(topics)
            return {topic.topic: Future(None) for topic in topics}  # type: ignore[attr-defined]

        def list_topics(self, **_: object) -> SimpleNamespace:
            partition = SimpleNamespace(replicas=[0])
            topic = SimpleNamespace(error=None, partitions={0: partition, 1: partition})
            return SimpleNamespace(
                topics={name: topic for name in load_r2d_b2a_config().broker.topics}
            )

        def describe_configs(self, resources: list[object]) -> dict[object, Future]:
            value = {"retention.ms": SimpleNamespace(value="-1")}
            return {resource: Future(value) for resource in resources}

    monkeypatch.setattr("nexus.ingestion.r2d_b2_service.AdminClient", Admin)
    service = R2DB2AService(
        config=load_r2d_b2a_config(),
        runtime=R2DB2ARuntime(bootstrap_servers="broker:9092"),
    )
    service.provision_topics()
    assert len(created) == 3
    assert all(
        topic.config == {"retention.ms": "-1"}
        for topic in created  # type: ignore[attr-defined]
    )


def test_ack_ledger_rejects_duplication_and_tamper() -> None:
    baseline = _ack_payload()
    assert R2DB2AAckLedger.model_validate(baseline).content_sha256 == baseline["content_sha256"]

    duplicate = json.loads(json.dumps(baseline))
    duplicate["acknowledgements"].append(duplicate["acknowledgements"][0])
    duplicate["content_sha256"] = canonical_json_sha256(
        {key: value for key, value in duplicate.items() if key != "content_sha256"}
    )
    with pytest.raises(ValidationError, match="unique"):
        R2DB2AAckLedger.model_validate(duplicate)

    tampered = json.loads(json.dumps(baseline))
    tampered["acknowledgements"][0]["value_sha256"] = "9" * 64
    with pytest.raises(ValidationError, match="content SHA-256"):
        R2DB2AAckLedger.model_validate(tampered)


def test_service_image_loader_is_canonical_strict_and_content_addressed(tmp_path: Path) -> None:
    payload = {
        "contract_version": "r2d.b2a.service-image.v1",
        "service": "redpanda",
        "configured_reference": "docker.redpanda.com/redpandadata/redpanda:v26.1.9",
        "observed_image_id": f"sha256:{'a' * 64}",
        "operating_system": "linux",
        "architecture": "amd64",
    }
    governed = {**payload, "content_sha256": canonical_json_sha256(payload)}
    path = tmp_path / "image.json"
    path.write_bytes(canonical_json_bytes(governed) + b"\n")
    assert load_r2d_b2a_service_image(path).observed_image_id == f"sha256:{'a' * 64}"

    path.write_text(json.dumps(governed, indent=2), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical"):
        load_r2d_b2a_service_image(path)

    wrong_arch = {**payload, "architecture": "arm64"}
    wrong = {**wrong_arch, "content_sha256": canonical_json_sha256(wrong_arch)}
    path.write_bytes(canonical_json_bytes(wrong) + b"\n")
    with pytest.raises(ValidationError):
        load_r2d_b2a_service_image(path)


def test_receipt_requires_exact_evidence_and_nonclaims(tmp_path: Path) -> None:
    path, receipt = _receipt_bundle(tmp_path)
    assert tuple(item.name for item in receipt.checks) == R2D_B2A_PASS_CHECK_NAMES
    assert receipt.service_dead_letter_topic_status == "PASS"
    assert receipt.persistent_dagster_status == "NOT_RUN"
    assert receipt.retained_service_restart_status == "NOT_RUN"
    assert receipt.full_g2_status == "OPEN"
    assert load_r2d_b2a_receipt(path) == receipt

    missing = receipt.model_dump(mode="json")
    missing["checks"] = missing["checks"][:-1]
    missing["content_sha256"] = canonical_json_sha256(
        {key: value for key, value in missing.items() if key != "content_sha256"}
    )
    with pytest.raises(ValidationError, match="checks"):
        R2DB2AVerificationReceipt.model_validate(missing)


def test_bundle_loader_rejects_stale_code_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, _ = _receipt_bundle(tmp_path)
    monkeypatch.setattr("nexus.r2d_b2a.r2d_b2a_code_sha256", lambda *_: "0" * 64)
    with pytest.raises(R2DB2AGovernanceError, match="current code"):
        load_r2d_b2a_receipt(path)


def test_bundle_loader_rejects_evidence_mutation(tmp_path: Path) -> None:
    path, receipt = _receipt_bundle(tmp_path)
    evidence = tmp_path / receipt.evidence_refs[0].relative_path
    evidence.write_text("{}\n", encoding="utf-8")
    with pytest.raises(R2DB2AGovernanceError, match=r"canonical|identity"):
        load_r2d_b2a_receipt(path)


def test_cli_exposes_b2a_and_forwards_explicit_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    help_result = runner.invoke(app, ["r2d", "verify-service-b2a", "--help"])
    assert help_result.exit_code == 0
    root_command = get_command(app)
    r2d_command = root_command.commands["r2d"]
    b2a_command = r2d_command.commands["verify-service-b2a"]
    option_names = {
        option for parameter in b2a_command.params for option in getattr(parameter, "opts", ())
    }
    assert {"--bootstrap-servers", "--artifact-dir", "--service-image-evidence"} <= option_names
    captured: dict[str, object] = {}

    def fake_verify(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(passed=True)

    monkeypatch.setattr("nexus.r2d_b2a.verify_r2d_b2a", fake_verify)
    monkeypatch.setattr("nexus.r2d_b2a.dump_r2d_b2a_result", lambda _: '{"status":"PASS"}')
    result = runner.invoke(
        app,
        [
            "r2d",
            "verify-service-b2a",
            "--bootstrap-servers",
            "broker:9092",
            "--artifact-dir",
            "artifacts/custom-b2a",
            "--service-image-evidence",
            "artifacts/service-image.json",
            "--config-path",
            "configs/ingestion/r2d-b2a.yaml",
        ],
    )
    assert result.exit_code == 0
    assert captured["bootstrap_servers"] == "broker:9092"
    assert captured["artifact_root"] == Path("artifacts/custom-b2a")
    assert captured["service_image_evidence_path"] == Path("artifacts/service-image.json")


def test_ci_runs_isolated_service_gate_after_cross_platform_b1() -> None:
    workflow = yaml.safe_load(Path(".github/workflows/ci.yml").read_text(encoding="utf-8"))
    job = workflow["jobs"]["r2d-service-b2a"]
    assert job["needs"] == "r2d-b1-cross-platform-receipt"
    steps = "\n".join(str(step) for step in job["steps"])
    assert "up -d --wait --wait-timeout 120 redpanda" in steps
    assert "b2a-service-image.json" in steps
    assert "verify-service-b2a" in steps
    assert "--service-image-evidence" in steps
    assert "down -v --remove-orphans" in steps
    start_step = next(
        str(step) for step in job["steps"] if step.get("name") == "Start fresh Redpanda"
    )
    assert "postgres" not in start_step
