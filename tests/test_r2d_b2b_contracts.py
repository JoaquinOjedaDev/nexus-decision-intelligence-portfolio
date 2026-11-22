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
from nexus.ingestion.r2d_contracts import canonical_json_sha256
from nexus.modeling.r3_artifacts import canonical_json_bytes, write_immutable_json_manifest
from nexus.orchestration.r2d_b2b_contracts import (
    R2D_B2B_EVIDENCE_DATASETS,
    R2D_B2B_PARTITION_KEYS,
    R2D_B2B_PASS_CHECK_NAMES,
    R2DB2BArtifactRef,
    R2DB2BVerificationReceipt,
    load_r2d_b2b_config,
    load_r2d_b2b_service_image,
    r2d_b2b_code_sha256,
    r2d_b2b_config_sha256,
)
from nexus.orchestration.r2d_b2b_definitions import defs
from nexus.r2d_b2b import R2DB2BGovernanceError, load_r2d_b2b_receipt

pytestmark = pytest.mark.r2d
runner = CliRunner()


def _image_payload() -> dict[str, object]:
    payload: dict[str, object] = {
        "contract_version": "r2d.b2b.service-image.v1",
        "service": "postgres",
        "configured_reference": "postgres:17.6-alpine",
        "observed_image_id": f"sha256:{'a' * 64}",
        "operating_system": "linux",
        "architecture": "amd64",
    }
    return {**payload, "content_sha256": canonical_json_sha256(payload)}


def _receipt_bundle(tmp_path: Path) -> tuple[Path, R2DB2BVerificationReceipt]:
    config = load_r2d_b2b_config()
    references: list[R2DB2BArtifactRef] = []
    hashes: dict[str, str] = {}
    for dataset in R2D_B2B_EVIDENCE_DATASETS:
        name = dataset.removeprefix("r2d.b2b.")
        if name == "service_image":
            payload = _image_payload()
        elif name == "inherited_r2b":
            payload = {
                "contract_version": "r2d.b2b.inherited-r2b.v1",
                "observed_config_sha256": config.inherited_r2b.config_sha256,
                "observed_orchestration_code_sha256": (
                    config.inherited_r2b.orchestration_code_sha256
                ),
            }
        else:
            payload = {"contract_version": f"r2d.b2b.{name}.test.v1", "dataset": dataset}
        artifact = write_immutable_json_manifest(
            tmp_path,
            dataset=dataset,
            manifest=payload,
            schema_identity={"contract": payload["contract_version"]},
        )
        references.append(
            R2DB2BArtifactRef(
                dataset=dataset,
                relative_path=artifact.path.relative_to(tmp_path).as_posix(),
                content_sha256=artifact.content_sha256,
                schema_sha256=artifact.schema_sha256,
                file_sha256=artifact.file_sha256,
                row_count=1,
            )
        )
        hashes[name] = artifact.content_sha256
    artifact_set = canonical_json_sha256([item.model_dump(mode="json") for item in references])
    check_sha = {
        "inherited_r2b_authority": config.inherited_r2b.config_sha256,
        "observed_postgres_image": hashes["service_image"],
        "postgres_backed_dagster_storage": hashes["instance"],
        "daemon_epoch_one_heartbeat": hashes["daemon_epoch_one"],
        "actual_bounded_backfill_submission": hashes["backfill_submission"],
        "first_partition_daemon_dispatch": hashes["run_storage"],
        "daemon_restart_continuity": hashes["daemon_epoch_two"],
        "exact_partition_run_success": hashes["run_storage"],
        "backfill_semantic_parity": hashes["semantic_parity"],
        "cross_process_persistent_readback": hashes["restart_readback"],
        "immutable_artifact_set": artifact_set,
        "bounded_resource_cleanup": hashes["resource"],
        "honest_scope": hashes["scope"],
    }
    receipt_payload: dict[str, object] = {
        "contract_version": "r2d.b2b.verification-receipt.v1",
        "release": "R2D-B2b",
        "status": "PASS",
        "checks": [
            {"name": name, "passed": True, "evidence_sha256": check_sha[name]}
            for name in R2D_B2B_PASS_CHECK_NAMES
        ],
        "evidence_refs": [item.model_dump(mode="json") for item in references],
        "config_sha256": r2d_b2b_config_sha256(config),
        "code_sha256": r2d_b2b_code_sha256(),
        "inherited_r2b_config_sha256": config.inherited_r2b.config_sha256,
        "inherited_r2b_orchestration_code_sha256": config.inherited_r2b.orchestration_code_sha256,
        "service_image_sha256": hashes["service_image"],
        "service_image_file_sha256": references[1].file_sha256,
        "instance_sha256": hashes["instance"],
        "daemon_epoch_one_sha256": hashes["daemon_epoch_one"],
        "backfill_submission_sha256": hashes["backfill_submission"],
        "daemon_epoch_two_sha256": hashes["daemon_epoch_two"],
        "run_storage_sha256": hashes["run_storage"],
        "semantic_parity_sha256": hashes["semantic_parity"],
        "restart_readback_sha256": hashes["restart_readback"],
        "resource_sha256": hashes["resource"],
        "artifact_set_sha256": artifact_set,
        "scope_sha256": hashes["scope"],
        "backfill_id": "test-backfill",
        "dagster_run_ids": ["run-a", "run-b"],
        "partition_keys": list(R2D_B2B_PARTITION_KEYS),
        "runtime": {
            "operating_system": "Linux",
            "machine": "x86_64",
            "python_version": "3.13.0",
            "dagster_version": "1.13.16",
            "dagster_postgres_version": "0.29.16",
            "github_runner_os_attested": True,
        },
        **config.scope.model_dump(mode="json"),
        "high_availability_claim": False,
        "automatic_scheduling_claim": False,
        "retained_multiservice_restart_claim": False,
    }
    receipt = R2DB2BVerificationReceipt.model_validate(
        {**receipt_payload, "content_sha256": canonical_json_sha256(receipt_payload)}
    )
    artifact = write_immutable_json_manifest(
        tmp_path,
        dataset="r2d.b2b.verification_receipt",
        manifest=receipt.model_dump(mode="json"),
        schema_identity={"contract": receipt.contract_version},
    )
    return artifact.path, receipt


def test_b2b_config_and_definitions_are_strict_and_exact() -> None:
    config = load_r2d_b2b_config()
    assert config.dagster.partition_keys == R2D_B2B_PARTITION_KEYS
    assert config.dagster.maximum_concurrent_runs == 1
    assert config.scope.dagster_process_restart_status == "PASS"
    assert config.scope.retained_multiservice_restart_status == "NOT_RUN"
    assert config.scope.full_g2_status == "OPEN"
    assert len(r2d_b2b_config_sha256(config)) == 64
    job = defs.get_job_def("r2d_b2b_backfill_job")
    assert tuple(job.partitions_def.get_partition_keys()) == R2D_B2B_PARTITION_KEYS
    assert job.backfill_policy is not None
    assert job.backfill_policy.max_partitions_per_run == 1
    with pytest.raises(ValidationError):
        config.release = "changed"  # type: ignore[misc]


def test_service_image_is_canonical_content_addressed_and_exact(tmp_path: Path) -> None:
    governed = _image_payload()
    path = tmp_path / "image.json"
    path.write_bytes(canonical_json_bytes(governed) + b"\n")
    assert load_r2d_b2b_service_image(path).service == "postgres"

    path.write_text(json.dumps(governed, indent=2), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical"):
        load_r2d_b2b_service_image(path)

    changed = {**governed, "observed_image_id": f"sha256:{'b' * 64}"}
    path.write_bytes(canonical_json_bytes(changed) + b"\n")
    with pytest.raises(ValidationError, match="content SHA-256"):
        load_r2d_b2b_service_image(path)


def test_receipt_requires_exact_checks_evidence_and_nonclaims(tmp_path: Path) -> None:
    _, receipt = _receipt_bundle(tmp_path)
    assert tuple(check.name for check in receipt.checks) == R2D_B2B_PASS_CHECK_NAMES
    assert tuple(reference.dataset for reference in receipt.evidence_refs) == (
        R2D_B2B_EVIDENCE_DATASETS
    )
    assert receipt.dagster_process_restart_status == "PASS"
    assert receipt.retained_multiservice_restart_status == "NOT_RUN"
    assert receipt.full_r2d_b_status == "IN_PROGRESS"
    assert receipt.full_g2_status == "OPEN"

    missing = receipt.model_dump(mode="json")
    missing["checks"] = missing["checks"][:-1]
    missing["content_sha256"] = canonical_json_sha256(
        {key: value for key, value in missing.items() if key != "content_sha256"}
    )
    with pytest.raises(ValidationError, match="checks"):
        R2DB2BVerificationReceipt.model_validate(missing)


def test_offline_loader_rejects_stale_code_before_claiming_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, _ = _receipt_bundle(tmp_path)
    monkeypatch.setattr("nexus.r2d_b2b.r2d_b2b_code_sha256", lambda *_: "0" * 64)
    with pytest.raises(R2DB2BGovernanceError, match="current code"):
        load_r2d_b2b_receipt(path)


def test_cli_exposes_b2b_and_forwards_secret_without_printing_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    help_result = runner.invoke(app, ["r2d", "verify-service-b2b", "--help"])
    assert help_result.exit_code == 0
    root_command = get_command(app)
    command = root_command.commands["r2d"].commands["verify-service-b2b"]
    option_names = {option for parameter in command.params for option in parameter.opts}
    assert {"--postgres-dsn", "--artifact-dir", "--service-image-evidence"} <= option_names
    captured: dict[str, object] = {}

    def fake_verify(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(passed=True)

    monkeypatch.setattr("nexus.r2d_b2b.verify_r2d_b2b", fake_verify)
    monkeypatch.setattr("nexus.r2d_b2b.dump_r2d_b2b_result", lambda _: '{"status":"PASS"}')
    secret = "postgresql://user:secret@postgres/nexus"
    result = runner.invoke(
        app,
        [
            "r2d",
            "verify-service-b2b",
            "--postgres-dsn",
            secret,
            "--service-image-evidence",
            "artifacts/b2b-image.json",
            "--artifact-dir",
            "artifacts/custom-b2b",
        ],
    )
    assert result.exit_code == 0
    assert captured["postgres_dsn"] == secret
    assert secret not in result.stdout
    assert captured["artifact_root"] == Path("artifacts/custom-b2b")


def test_ci_has_isolated_postgres_b2b_gate() -> None:
    workflow = yaml.safe_load(Path(".github/workflows/ci.yml").read_text(encoding="utf-8"))
    job = workflow["jobs"]["r2d-service-b2b"]
    assert job["needs"] == "r2d-service-b2a"
    upload = next(
        step for step in job["steps"] if step.get("name") == "Upload complete R2D-B2b evidence"
    )
    assert upload["uses"] == "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
    steps = "\n".join(str(step) for step in job["steps"])
    assert "up -d --wait --wait-timeout 120 postgres" in steps
    assert "b2b-service-image.json" in steps
    assert "verify-service-b2b" in steps
    assert "down -v --remove-orphans" in steps
    start = next(str(step) for step in job["steps"] if step.get("name") == "Start fresh PostgreSQL")
    assert "redpanda" not in start
