"""Strict contracts for the bounded R2D-B2c retained-service restart gate."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal, Self

import yaml
from pydantic import (
    NonNegativeFloat,
    NonNegativeInt,
    StringConstraints,
    field_validator,
    model_validator,
)

from nexus.ingestion.r2d_b2_contracts import load_r2d_b2a_config, r2d_b2a_config_sha256
from nexus.ingestion.r2d_contracts import (
    R2DContract,
    R2DPhysicalRecord,
    R2DSha256,
    canonical_json_sha256,
    r2d_lf_canonical_file_sha256,
)
from nexus.modeling.r3_artifacts import canonical_json_bytes
from nexus.orchestration.r2d_b2b_contracts import load_r2d_b2b_config, r2d_b2b_config_sha256

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_R2D_B2C_CONFIG_PATH = (
    PROJECT_ROOT / "configs" / "operations" / "r2d-b2c-retained-restart.yaml"
)

R2DB2CTopic = Literal[
    "inventory.adjusted.v1",
    "inventory.adjusted.dlq.v1",
    "inventory.adjusted.redrive.v1",
]
R2DB2CIdentifier = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/@-]*$", min_length=1, max_length=200),
]
R2DB2CContainerId = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
R2DB2CImageId = Annotated[str, StringConstraints(pattern=r"^sha256:[a-f0-9]{64}$")]
R2DB2CCheckName = Literal[
    "inherited_b2a_authority",
    "inherited_b2b_authority",
    "pre_restart_checkpoint_bound",
    "actual_multiservice_stop",
    "named_volumes_retained",
    "exact_container_restart",
    "redpanda_zero_loss_readback",
    "fresh_broker_reconnect_io",
    "postgres_dagster_state_retained",
    "complete_checkpoint",
    "immutable_artifact_set",
    "honest_scope",
]
R2D_B2C_PASS_CHECK_NAMES: tuple[R2DB2CCheckName, ...] = (
    "inherited_b2a_authority",
    "inherited_b2b_authority",
    "pre_restart_checkpoint_bound",
    "actual_multiservice_stop",
    "named_volumes_retained",
    "exact_container_restart",
    "redpanda_zero_loss_readback",
    "fresh_broker_reconnect_io",
    "postgres_dagster_state_retained",
    "complete_checkpoint",
    "immutable_artifact_set",
    "honest_scope",
)
R2DB2CEvidenceDataset = Literal[
    "r2d.b2c.inherited_b2a",
    "r2d.b2c.inherited_b2b",
    "r2d.b2c.pre_restart_checkpoint",
    "r2d.b2c.service_lifecycle",
    "r2d.b2c.redpanda_retained_readback",
    "r2d.b2c.redpanda_reconnect_probe",
    "r2d.b2c.postgres_dagster_readback",
    "r2d.b2c.complete_checkpoint",
    "r2d.b2c.scope",
]
R2D_B2C_EVIDENCE_DATASETS: tuple[R2DB2CEvidenceDataset, ...] = (
    "r2d.b2c.inherited_b2a",
    "r2d.b2c.inherited_b2b",
    "r2d.b2c.pre_restart_checkpoint",
    "r2d.b2c.service_lifecycle",
    "r2d.b2c.redpanda_retained_readback",
    "r2d.b2c.redpanda_reconnect_probe",
    "r2d.b2c.postgres_dagster_readback",
    "r2d.b2c.complete_checkpoint",
    "r2d.b2c.scope",
)


class R2DB2CInheritedContract(R2DContract):
    b2a_config_path: Literal["configs/ingestion/r2d-b2a.yaml"]
    b2a_config_sha256: Literal["48df248406fe9d7efad755418cd43f8410763984833ecdb7ceee98c584bfd9e5"]
    b2b_config_path: Literal["configs/orchestration/r2d-b2b.yaml"]
    b2b_config_sha256: Literal["70b2114d22617b1950c7a3519c033634f1c3fcf289c4308c3369fe4fdcbbc48d"]


class R2DB2CServicesContract(R2DContract):
    compose_file: Literal["infra/compose.yaml"]
    redpanda_service: Literal["redpanda"]
    postgres_service: Literal["postgres"]
    redpanda_volume_destination: Literal["/var/lib/redpanda/data"]
    postgres_volume_destination: Literal["/var/lib/postgresql/data"]
    stop_timeout_seconds: Literal[30]
    start_wait_timeout_seconds: Literal[120]
    require_same_container_ids: Literal[True]
    require_same_named_volumes: Literal[True]
    require_healthy_before_and_after: Literal[True]


class R2DB2CBrokerContract(R2DContract):
    retained_topics: tuple[R2DB2CTopic, ...]
    partition_count: Literal[2]
    reconnect_probe_topic: Literal["nexus.r2d.b2c.reconnect.v1"]
    reconnect_probe_partition_count: Literal[1]
    reconnect_probe_replication_factor: Literal[1]
    reconnect_probe_consumer_group_id: Literal["nexus-r2d-b2c-reconnect-v1"]
    poll_timeout_seconds: Literal[30]

    @field_validator("retained_topics", mode="before")
    @classmethod
    def freeze_topics(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def topics_are_exact(self) -> Self:
        expected = (
            "inventory.adjusted.v1",
            "inventory.adjusted.dlq.v1",
            "inventory.adjusted.redrive.v1",
        )
        if self.retained_topics != expected:
            raise ValueError("B2c retained topic order differs from B2a")
        return self


class R2DB2CPostgresContract(R2DContract):
    expected_backfill_status: Literal["COMPLETED"]
    expected_run_status: Literal["SUCCESS"]
    expected_run_count: Literal[2]
    require_exact_table_counts: Literal[True]


class R2DB2CScopeContract(R2DContract):
    inherited_b2a_status: Literal["PASS"]
    inherited_b2b_status: Literal["PASS"]
    retained_multiservice_restart_status: Literal["PASS"]
    operational_alert_delivery_status: Literal["NOT_RUN"]
    high_availability_status: Literal["NOT_RUN"]
    backup_restore_status: Literal["NOT_RUN"]
    full_r2d_b_status: Literal["IN_PROGRESS"]
    full_g2_status: Literal["OPEN"]


class R2DB2CConfig(R2DContract):
    release: Literal["R2D-B2c"]
    contract_version: Literal["1.0.0"]
    inherited: R2DB2CInheritedContract
    services: R2DB2CServicesContract
    broker: R2DB2CBrokerContract
    postgres: R2DB2CPostgresContract
    scope: R2DB2CScopeContract


class R2DB2CTopicSnapshot(R2DContract):
    contract_version: Literal["r2d.b2c.topic-snapshot.v1"]
    topic: R2DB2CTopic
    low_watermarks: tuple[NonNegativeInt, NonNegativeInt]
    high_watermarks: tuple[NonNegativeInt, NonNegativeInt]
    records: tuple[R2DPhysicalRecord, ...]
    records_sha256: R2DSha256

    @field_validator("low_watermarks", "high_watermarks", mode="before")
    @classmethod
    def freeze_watermarks(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("records", mode="before")
    @classmethod
    def freeze_records(cls, value: object) -> object:
        if not isinstance(value, list):
            return value
        return tuple(
            {**item, "headers": tuple(item.get("headers", ()))} if isinstance(item, dict) else item
            for item in value
        )

    @model_validator(mode="after")
    def snapshot_is_complete(self) -> Self:
        if any(
            low > high for low, high in zip(self.low_watermarks, self.high_watermarks, strict=True)
        ):
            raise ValueError("B2c topic snapshot has inverted watermarks")
        expected = tuple(
            (self.topic, partition, offset)
            for partition, (low, high) in enumerate(
                zip(self.low_watermarks, self.high_watermarks, strict=True)
            )
            for offset in range(low, high)
        )
        observed = tuple(record.transport_identity for record in self.records)
        if observed != expected:
            raise ValueError("B2c topic snapshot is incomplete, duplicated, or reordered")
        payload = [record.model_dump(mode="json") for record in self.records]
        if canonical_json_sha256(payload) != self.records_sha256:
            raise ValueError("B2c topic snapshot record identity mismatch")
        return self


class R2DB2CDagsterRun(R2DContract):
    run_id: R2DB2CIdentifier
    job_name: Literal["r2d_b2b_backfill_job"]
    partition_key: R2DB2CIdentifier
    status: Literal["SUCCESS"]
    event_count: NonNegativeInt
    asset_materialization_events: Literal[2]
    run_success_events: Literal[1]


class R2DB2CDagsterSnapshot(R2DContract):
    contract_version: Literal["r2d.b2c.dagster-snapshot.v1"]
    backfill_id: R2DB2CIdentifier
    backfill_status: Literal["COMPLETED"]
    partition_names: tuple[R2DB2CIdentifier, ...]
    runs: tuple[R2DB2CDagsterRun, ...]
    content_sha256: R2DSha256

    @field_validator("partition_names", "runs", mode="before")
    @classmethod
    def freeze_sequences(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def dagster_state_is_exact(self) -> Self:
        if len(self.partition_names) != 2 or len(set(self.partition_names)) != 2:
            raise ValueError("B2c Dagster snapshot requires two distinct partitions")
        if len(self.runs) != 2 or len({item.run_id for item in self.runs}) != 2:
            raise ValueError("B2c Dagster snapshot requires two distinct runs")
        if tuple(item.partition_key for item in self.runs) != self.partition_names:
            raise ValueError("B2c Dagster runs differ from the persisted partition order")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("B2c Dagster snapshot content identity mismatch")
        return self


class R2DB2CPreRestartCheckpoint(R2DContract):
    contract_version: Literal["r2d.b2c.pre-restart-checkpoint.v1"]
    b2a_receipt_content_sha256: R2DSha256
    b2a_receipt_file_sha256: R2DSha256
    b2b_receipt_content_sha256: R2DSha256
    b2b_receipt_file_sha256: R2DSha256
    topics: tuple[R2DB2CTopicSnapshot, ...]
    broker_snapshot_sha256: R2DSha256
    dagster: R2DB2CDagsterSnapshot
    postgres_table_counts: dict[str, NonNegativeInt]
    postgres_table_counts_sha256: R2DSha256
    content_sha256: R2DSha256

    @field_validator("topics", mode="before")
    @classmethod
    def freeze_topics(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def checkpoint_is_bound(self) -> Self:
        expected_topics = (
            "inventory.adjusted.v1",
            "inventory.adjusted.dlq.v1",
            "inventory.adjusted.redrive.v1",
        )
        if tuple(item.topic for item in self.topics) != expected_topics:
            raise ValueError("B2c pre-restart checkpoint has incomplete broker topology")
        topic_rows = [item.model_dump(mode="json") for item in self.topics]
        if canonical_json_sha256(topic_rows) != self.broker_snapshot_sha256:
            raise ValueError("B2c pre-restart broker snapshot identity mismatch")
        if canonical_json_sha256(self.postgres_table_counts) != self.postgres_table_counts_sha256:
            raise ValueError("B2c pre-restart PostgreSQL counts identity mismatch")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("B2c pre-restart checkpoint content identity mismatch")
        return self


class R2DB2CServiceLifecycle(R2DContract):
    service: Literal["redpanda", "postgres"]
    container_id_before: R2DB2CContainerId
    container_id_stopped: R2DB2CContainerId
    container_id_after: R2DB2CContainerId
    image_id_before: R2DB2CImageId
    image_id_after: R2DB2CImageId
    volume_name_before: R2DB2CIdentifier
    volume_name_stopped: R2DB2CIdentifier
    volume_name_after: R2DB2CIdentifier
    volume_destination: Literal["/var/lib/redpanda/data", "/var/lib/postgresql/data"]
    before_started_at: str
    stopped_finished_at: str
    after_started_at: str
    before_status: Literal["running"]
    stopped_status: Literal["exited"]
    after_status: Literal["running"]
    before_health: Literal["healthy"]
    after_health: Literal["healthy"]
    stopped_exit_code: Literal[0]

    @model_validator(mode="after")
    def lifecycle_is_one_retained_container(self) -> Self:
        if len({self.container_id_before, self.container_id_stopped, self.container_id_after}) != 1:
            raise ValueError("B2c service container identity changed across stop/start")
        if self.image_id_before != self.image_id_after:
            raise ValueError("B2c service image identity changed across stop/start")
        if len({self.volume_name_before, self.volume_name_stopped, self.volume_name_after}) != 1:
            raise ValueError("B2c named volume changed across stop/start")
        try:
            before = datetime.fromisoformat(self.before_started_at.replace("Z", "+00:00"))
            stopped = datetime.fromisoformat(self.stopped_finished_at.replace("Z", "+00:00"))
            after = datetime.fromisoformat(self.after_started_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("B2c lifecycle timestamps must be ISO-8601") from exc
        if before.tzinfo is None or stopped.tzinfo is None or after.tzinfo is None:
            raise ValueError("B2c lifecycle timestamps must include offsets")
        if not before < stopped < after:
            raise ValueError("B2c lifecycle timestamps do not prove a stop/start boundary")
        return self


class R2DB2CVolumeEvidence(R2DContract):
    name: R2DB2CIdentifier
    driver: Literal["local"]
    compose_project: R2DB2CIdentifier
    compose_volume: Literal["nexus-redpanda", "nexus-postgres"]
    created_at: str


class R2DB2CLifecycleEvidence(R2DContract):
    contract_version: Literal["r2d.b2c.service-lifecycle.v1"]
    compose_project_name: R2DB2CIdentifier
    compose_file: Literal["infra/compose.yaml"]
    stop_command: tuple[str, ...]
    start_command: tuple[str, ...]
    stop_elapsed_seconds: NonNegativeFloat
    start_elapsed_seconds: NonNegativeFloat
    services: tuple[R2DB2CServiceLifecycle, ...]
    volumes_before: tuple[R2DB2CVolumeEvidence, ...]
    volumes_stopped: tuple[R2DB2CVolumeEvidence, ...]
    volumes_after: tuple[R2DB2CVolumeEvidence, ...]
    content_sha256: R2DSha256

    @field_validator(
        "stop_command",
        "start_command",
        "services",
        "volumes_before",
        "volumes_stopped",
        "volumes_after",
        mode="before",
    )
    @classmethod
    def freeze_sequences(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def lifecycle_is_complete(self) -> Self:
        if tuple(item.service for item in self.services) != ("redpanda", "postgres"):
            raise ValueError("B2c lifecycle must bind Redpanda then PostgreSQL")
        if (
            self.volumes_before != self.volumes_stopped
            or self.volumes_stopped != self.volumes_after
            or len(self.volumes_before) != 2
        ):
            raise ValueError("B2c named-volume evidence changed across stop/start")
        if tuple(item.compose_volume for item in self.volumes_before) != (
            "nexus-redpanda",
            "nexus-postgres",
        ):
            raise ValueError("B2c lifecycle does not bind the exact retained volumes")
        if any(item.compose_project != self.compose_project_name for item in self.volumes_before):
            raise ValueError("B2c volume evidence belongs to another Compose project")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("B2c lifecycle evidence content identity mismatch")
        return self


class R2DB2CBrokerProbe(R2DContract):
    contract_version: Literal["r2d.b2c.broker-reconnect-probe.v1"]
    topic: Literal["nexus.r2d.b2c.reconnect.v1"]
    partition: Literal[0]
    offset: NonNegativeInt
    committed_offset: NonNegativeInt
    key_hex: str
    value_hex: str
    value_sha256: R2DSha256
    broker_timestamp_ms: NonNegativeInt
    consumed_record: R2DPhysicalRecord
    content_sha256: R2DSha256

    @field_validator("consumed_record", mode="before")
    @classmethod
    def freeze_consumed_record_headers(cls, value: object) -> object:
        if isinstance(value, dict) and isinstance(value.get("headers"), list):
            return {**value, "headers": tuple(value["headers"])}
        return value

    @model_validator(mode="after")
    def probe_is_exact(self) -> Self:
        if self.committed_offset != self.offset + 1:
            raise ValueError("B2c reconnect probe did not commit the next offset")
        if self.consumed_record.transport_identity != (self.topic, 0, self.offset):
            raise ValueError("B2c reconnect probe consumed another transport identity")
        try:
            value = bytes.fromhex(self.value_hex)
            bytes.fromhex(self.key_hex)
        except ValueError as exc:
            raise ValueError("B2c reconnect probe key/value must be even hexadecimal") from exc
        if hashlib.sha256(value).hexdigest() != self.value_sha256:
            raise ValueError("B2c reconnect probe value identity mismatch")
        if (
            self.consumed_record.key_hex != self.key_hex
            or self.consumed_record.value_hex != self.value_hex
        ):
            raise ValueError("B2c reconnect probe changed exact broker bytes")
        if self.consumed_record.broker_timestamp_ms != self.broker_timestamp_ms:
            raise ValueError("B2c reconnect probe timestamp changed")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("B2c reconnect probe content identity mismatch")
        return self


class R2DB2CArtifactRef(R2DContract):
    dataset: R2DB2CEvidenceDataset
    relative_path: str
    content_sha256: R2DSha256
    schema_sha256: R2DSha256
    file_sha256: R2DSha256
    row_count: Literal[1]


class R2DB2CVerificationCheck(R2DContract):
    name: R2DB2CCheckName
    passed: Literal[True]
    evidence_sha256: R2DSha256


class R2DB2CRuntime(R2DContract):
    operating_system: str
    machine: str
    python_version: str
    github_runner_os_attested: bool


class R2DB2CVerificationReceipt(R2DContract):
    contract_version: Literal["r2d.b2c.verification-receipt.v1"]
    release: Literal["R2D-B2c"]
    status: Literal["PASS"]
    checks: tuple[R2DB2CVerificationCheck, ...]
    evidence_refs: tuple[R2DB2CArtifactRef, ...]
    config_sha256: R2DSha256
    code_sha256: R2DSha256
    b2a_receipt_content_sha256: R2DSha256
    b2a_receipt_file_sha256: R2DSha256
    b2b_receipt_content_sha256: R2DSha256
    b2b_receipt_file_sha256: R2DSha256
    pre_restart_checkpoint_sha256: R2DSha256
    lifecycle_sha256: R2DSha256
    redpanda_readback_sha256: R2DSha256
    reconnect_probe_sha256: R2DSha256
    postgres_readback_sha256: R2DSha256
    complete_checkpoint_sha256: R2DSha256
    scope_sha256: R2DSha256
    artifact_set_sha256: R2DSha256
    compose_project_name: R2DB2CIdentifier
    backfill_id: R2DB2CIdentifier
    dagster_run_ids: tuple[R2DB2CIdentifier, ...]
    retained_topic_count: Literal[3]
    retained_record_count: NonNegativeInt
    runtime: R2DB2CRuntime
    inherited_b2a_status: Literal["PASS"]
    inherited_b2b_status: Literal["PASS"]
    retained_multiservice_restart_status: Literal["PASS"]
    operational_alert_delivery_status: Literal["NOT_RUN"]
    high_availability_status: Literal["NOT_RUN"]
    backup_restore_status: Literal["NOT_RUN"]
    full_r2d_b_status: Literal["IN_PROGRESS"]
    full_g2_status: Literal["OPEN"]
    exactly_once_claim: Literal[False]
    high_availability_claim: Literal[False]
    backup_restore_claim: Literal[False]
    multi_node_claim: Literal[False]
    production_scale_claim: Literal[False]
    content_sha256: R2DSha256

    @field_validator("checks", "evidence_refs", "dagster_run_ids", mode="before")
    @classmethod
    def freeze_sequences(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def receipt_is_honest_complete_and_bound(self) -> Self:
        if tuple(item.name for item in self.checks) != R2D_B2C_PASS_CHECK_NAMES:
            raise ValueError("B2c verification checks are incomplete, duplicated, or reordered")
        if tuple(item.dataset for item in self.evidence_refs) != R2D_B2C_EVIDENCE_DATASETS:
            raise ValueError("B2c evidence topology is incomplete, duplicated, or reordered")
        expected: dict[R2DB2CCheckName, str] = {
            "inherited_b2a_authority": self.b2a_receipt_content_sha256,
            "inherited_b2b_authority": self.b2b_receipt_content_sha256,
            "pre_restart_checkpoint_bound": self.pre_restart_checkpoint_sha256,
            "actual_multiservice_stop": self.lifecycle_sha256,
            "named_volumes_retained": self.lifecycle_sha256,
            "exact_container_restart": self.lifecycle_sha256,
            "redpanda_zero_loss_readback": self.redpanda_readback_sha256,
            "fresh_broker_reconnect_io": self.reconnect_probe_sha256,
            "postgres_dagster_state_retained": self.postgres_readback_sha256,
            "complete_checkpoint": self.complete_checkpoint_sha256,
            "immutable_artifact_set": self.artifact_set_sha256,
            "honest_scope": self.scope_sha256,
        }
        if any(item.evidence_sha256 != expected[item.name] for item in self.checks):
            raise ValueError("B2c verification check is detached from its evidence")
        refs = [item.model_dump(mode="json") for item in self.evidence_refs]
        if canonical_json_sha256(refs) != self.artifact_set_sha256:
            raise ValueError("B2c artifact-set identity mismatch")
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if canonical_json_sha256(payload) != self.content_sha256:
            raise ValueError("B2c verification receipt content identity mismatch")
        return self


R2D_B2C_CODE_FILES = (
    "configs/operations/r2d-b2c-retained-restart.yaml",
    "configs/ingestion/r2d-b2a.yaml",
    "configs/orchestration/r2d-b2b.yaml",
    "configs/orchestration/r2d-b2b-dagster.yaml",
    "src/nexus/orchestration/r2d_b2c_contracts.py",
    "src/nexus/ingestion/r2d_b2_contracts.py",
    "src/nexus/ingestion/r2d_b2_service.py",
    "src/nexus/orchestration/r2d_b2b_contracts.py",
    "src/nexus/r2d_b2a.py",
    "src/nexus/r2d_b2b.py",
    "src/nexus/r2d_b2c.py",
    "src/nexus/cli.py",
    "tests/test_r2d_b2c_contracts.py",
    "infra/compose.yaml",
    ".github/workflows/ci.yml",
    "pyproject.toml",
    "uv.lock",
)


def load_r2d_b2c_config(path: Path = DEFAULT_R2D_B2C_CONFIG_PATH) -> R2DB2CConfig:
    config = R2DB2CConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
    b2a = load_r2d_b2a_config(PROJECT_ROOT / config.inherited.b2a_config_path)
    b2b = load_r2d_b2b_config(PROJECT_ROOT / config.inherited.b2b_config_path)
    if r2d_b2a_config_sha256(b2a) != config.inherited.b2a_config_sha256:
        raise ValueError("B2c inherited B2a config identity mismatch")
    if r2d_b2b_config_sha256(b2b) != config.inherited.b2b_config_sha256:
        raise ValueError("B2c inherited B2b config identity mismatch")
    return config


def r2d_b2c_config_sha256(config: R2DB2CConfig) -> str:
    return canonical_json_sha256(config.model_dump(mode="json"))


def r2d_b2c_code_sha256(project_root: Path = PROJECT_ROOT) -> str:
    root = project_root.resolve()
    identities = {path: r2d_lf_canonical_file_sha256(root / path) for path in R2D_B2C_CODE_FILES}
    return canonical_json_sha256(identities)


def load_canonical_b2c_model(path: Path, model: type[R2DContract]) -> R2DContract:
    try:
        raw = path.read_bytes()
        parsed = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot load canonical B2c evidence: {path}") from exc
    if raw != canonical_json_bytes(parsed) + b"\n":
        raise ValueError("B2c evidence must be canonical JSON")
    return model.model_validate(parsed)
