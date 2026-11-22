"""Portable R2D-A1 bridge from captured event bytes to the public R2A rebuild.

R2D owns transport evidence only.  This module deliberately reconstructs an R2A
source package and then calls :func:`nexus.r2.run_r2_rebuild`; it does not copy
event validation, correction, cutoff, or dbt semantics from R2A.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import stat
import sys
import tempfile
from collections import Counter
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Literal, cast

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from nexus.data import (
    R2GeneratedFixture,
    R2QuarantineReason,
    R2RebuildResult,
    R2SourceManifest,
    land_r2_sources,
)
from nexus.ingestion import (
    DEFAULT_R2D_CONFIG_PATH,
    R2D_A1_PASS_CHECK_NAMES,
    R2DCaptureCheckpoint,
    R2DCaptureTranscript,
    R2DConfig,
    R2DPhysicalRecord,
    R2DResourceEvidence,
    R2DResourceMonitor,
    R2DVerificationCheck,
    R2DVerificationReceipt,
    R2DVerificationRuntime,
    build_capture_checkpoint,
    capture_transcript,
    load_r2d_config,
    partition_for_key,
    r2d_config_sha256,
    reconstruct_jsonl_bytes,
    routing_key_for_value,
    validate_capture_transcript,
    verify_complete_checkpoint,
)
from nexus.ingestion import (
    raw_value_hash_counts as ingestion_raw_value_hash_counts,
)
from nexus.ingestion.r2d_capture import (
    build_portable_producer_ack_ledger,
    raw_value_multiset_sha256,
    validate_portable_producer_ack_ledger,
)
from nexus.ingestion.r2d_contracts import (
    R2D_SOURCE_MANIFEST_SHA256,
    R2DEvidenceCheckpoint,
    R2DProducerAckLedger,
    r2d_evidence_bindings_sha256,
    r2d_lf_canonical_file_sha256,
    r2d_schema_bundle_payload,
    r2d_schema_bundle_sha256,
)
from nexus.ingestion.r2d_resources import (
    ARTIFACT_METHOD,
    INTEGRITY_METHOD,
    MIB,
    NATIVE_THREAD_METHOD,
    RSS_METHOD,
    SHUTDOWN_METHOD,
    WALL_TIME_METHOD,
    R2DResourceCheck,
)
from nexus.modeling.r3_artifacts import (
    ScientificArtifactRef,
    canonical_json_bytes,
    canonical_json_sha256,
    write_immutable_json_manifest,
)
from nexus.r2 import (
    _GOLD_TABLES as R2_GOLD_TABLES,
)
from nexus.r2 import (
    _table_rows as r2_table_rows,
)
from nexus.r2 import (
    r2_dbt_code_sha256,
    r2_pipeline_code_sha256,
    run_r2_rebuild,
)
from nexus.simulation import generate_r2_smoke_fixture
from nexus.storage import (
    ContentAddressedArtifact,
    canonical_rows_sha256,
    canonical_schema_sha256,
    file_sha256,
)

_INVENTORY_DATASET = "source.inventory_adjustments"
_INVENTORY_FILE = "inventory_events.jsonl"
_BROKER_TIMESTAMP_BASE_MS = 1_788_134_400_000
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_RECEIPT_FILE = "r2d-a1-verification-receipt.json"
_PORTABLE_BUNDLE_FILE = "r2d-a1-portable-bundle.json"
_ARTIFACT_SET_DEFINITION = (
    "80 canonical independently reverified core pre-checkpoint artifacts; retained correction "
    "and recovery evidence trees are separately live-rederived and verified; excludes the "
    "evidence checkpoint, artifact-index publication, resource evidence, portable bundle, and "
    "receipt"
)
_CODE_BUNDLE_FILES = (
    "contracts/schemas/event-envelope.schema.json",
    "contracts/schemas/r2-inventory-adjustment-payload.schema.json",
    "src/nexus/data/r2_pipeline.py",
    "src/nexus/data/r2_models.py",
    "src/nexus/ingestion/__init__.py",
    "src/nexus/ingestion/r2d_capture.py",
    "src/nexus/ingestion/r2d_contracts.py",
    "src/nexus/ingestion/r2d_resources.py",
    "src/nexus/modeling/r3_artifacts.py",
    "src/nexus/r2.py",
    "src/nexus/r2d.py",
    "src/nexus/storage/artifacts.py",
)
_EXPECTED_GOLD_DATASETS = (
    "gold.r2_demand_history",
    "gold.r2_fulfillment_outcomes",
    "gold.r2_inventory_position",
    "gold.r2_open_orders",
    "gold.r2_supplier_performance",
)
_EXPECTED_SOURCE_FILES = (
    ("source.fulfillment_outcomes", "fulfillment_outcomes.csv"),
    ("source.inventory_adjustments", "inventory_events.jsonl"),
    ("source.opening_inventory", "opening_inventory.csv"),
    ("source.product_locations", "product_locations.csv"),
    ("source.purchase_orders", "purchase_orders.csv"),
    ("source.receipts", "receipts.csv"),
    ("source.suppliers", "suppliers.csv"),
)
_EXPECTED_VALIDATED_DATASETS = (
    "quarantine.r2",
    "validated.fulfillment_outcomes",
    "validated.inventory_adjustments",
    "validated.opening_inventory",
    "validated.product_locations",
    "validated.purchase_orders",
    "validated.receipts",
    "validated.suppliers",
)
_EXPECTED_PRECHECKPOINT_MANIFESTS = (
    "r2d.a1.capture_checkpoint",
    "r2d.a1.capture_transcript",
    "r2d.a1.captured_provenance",
    "r2d.a1.correction_semantics",
    "r2d.a1.direct_provenance",
    "r2d.a1.normalized_business_outcome",
    "r2d.a1.portable_producer_ack",
    "r2d.a1.portable_recovery",
)
_R2D_MANIFEST_CONTRACTS = {
    "r2d.a1.capture_checkpoint": "r2d.a1.capture-checkpoint.v1",
    "r2d.a1.capture_transcript": "r2d.a1.capture-transcript.v1",
    "r2d.a1.captured_provenance": "r2d.a1.route-provenance.v1",
    "r2d.a1.correction_semantics": "r2d.a1.correction-semantics.v1",
    "r2d.a1.direct_provenance": "r2d.a1.route-provenance.v1",
    "r2d.a1.normalized_business_outcome": "r2d.a1.normalized-business-outcome.v1",
    "r2d.a1.portable_producer_ack": "r2d.a1.portable-producer-ack.v1",
    "r2d.a1.portable_recovery": "r2d.a1.portable-recovery.v1",
}


class R2DPackageConflictError(RuntimeError):
    """A reconstructed package path already contains different bytes."""


class R2DParityError(RuntimeError):
    """Direct and captured routes disagree on normalized business evidence."""


@dataclass(frozen=True, slots=True)
class R2DReconstructedPackage:
    """One strict seven-file R2A package reconstructed from captured values."""

    source_dir: Path
    manifest_path: Path
    manifest: R2SourceManifest
    manifest_file_sha256: str
    inventory_jsonl_sha256: str
    inventory_record_count: int


@dataclass(frozen=True, slots=True)
class R2DPortableCapture:
    """Verified portable capture and the exact R2A package reconstructed from it."""

    raw_values: tuple[bytes, ...]
    producer_ack_ledger: R2DProducerAckLedger
    transcript: R2DCaptureTranscript
    checkpoint: R2DCaptureCheckpoint
    package: R2DReconstructedPackage


@dataclass(frozen=True, slots=True)
class R2DGoldEvidence:
    """Route-neutral identity of one R2A Gold product."""

    dataset: str
    row_count: int
    content_sha256: str
    schema_sha256: str


@dataclass(frozen=True, slots=True)
class R2DMaterializedArtifactEvidence:
    """An independently re-read Parquet identity, including physical file evidence."""

    cutoff: str
    stage: Literal["validated", "gold"]
    dataset: str
    row_count: int
    content_sha256: str
    schema_sha256: str
    file_sha256: str


@dataclass(frozen=True, slots=True)
class R2DSourceFileEvidence:
    """An independently re-read source-package file bound to its strict manifest."""

    dataset: str
    file_name: str
    record_count: int
    file_sha256: str


@dataclass(frozen=True, slots=True)
class R2DSummaryEvidence:
    """An independently re-read public R2A summary and its semantic reference."""

    cutoff: str
    semantic_manifest_sha256: str
    file_sha256: str


@dataclass(frozen=True, slots=True)
class R2DCutoffOutcome:
    """Normalized business evidence for one governed planning cutoff."""

    planning_cutoff: str
    watermarks: tuple[tuple[str, str | None], ...]
    quality_counts: tuple[tuple[str, int | None], ...]
    quarantine_reason_counts: tuple[tuple[str, int], ...]
    gold: tuple[R2DGoldEvidence, ...]

    def __post_init__(self) -> None:
        datasets = tuple(item.dataset for item in self.gold)
        if datasets != _EXPECTED_GOLD_DATASETS:
            raise ValueError(
                "normalized R2D outcome must contain exactly five sorted R2A Gold datasets; "
                f"received {datasets!r}"
            )


@dataclass(frozen=True, slots=True)
class R2DNormalizedBusinessOutcome:
    """Exact cross-route comparator input with execution provenance removed."""

    raw_value_sha256_counts: tuple[tuple[str, int], ...]
    cutoffs: tuple[R2DCutoffOutcome, ...]

    def __post_init__(self) -> None:
        ordered = tuple(sorted(self.cutoffs, key=lambda item: item.planning_cutoff))
        if self.cutoffs != ordered:
            raise ValueError("normalized R2D cutoffs must be ordered")
        if len({item.planning_cutoff for item in self.cutoffs}) != len(self.cutoffs):
            raise ValueError("normalized R2D cutoffs must be unique")


@dataclass(frozen=True, slots=True)
class R2DRouteProvenance:
    """Route-specific evidence intentionally excluded from business parity."""

    route: Literal["direct", "captured"]
    source_manifest_sha256: str
    source_manifest_file_sha256: str
    semantic_manifests: tuple[tuple[str, str], ...]
    summary_file_sha256: tuple[tuple[str, str], ...]
    source_files: tuple[R2DSourceFileEvidence, ...]
    validated_artifacts: tuple[R2DMaterializedArtifactEvidence, ...]
    gold_artifacts: tuple[R2DMaterializedArtifactEvidence, ...]
    summaries: tuple[R2DSummaryEvidence, ...]
    checkpoint_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class R2DArtifactIndex:
    """Non-circular canonical set of every independently reverified pre-checkpoint artifact."""

    contract_version: Literal["r2d.a1.artifact-index.v1"]
    release: Literal["R2D-A1"]
    definition: str
    config_sha256: str
    schema_bundle_sha256: str
    code_sha256: str
    entries: tuple[dict[str, object], ...]
    artifact_set_sha256: str


@dataclass(frozen=True, slots=True)
class R2DPortableBundleRef:
    """Fixed-path portable bundle publication."""

    path: Path
    content_sha256: str
    file_sha256: str


@dataclass(frozen=True, slots=True)
class R2DRouteRebuilds:
    """The early and final R2A rebuilds for one transport route."""

    early: R2RebuildResult
    final: R2RebuildResult

    def ordered(self) -> tuple[R2RebuildResult, R2RebuildResult]:
        if self.early.planning_cutoff <= self.final.planning_cutoff:
            return self.early, self.final
        return self.final, self.early


@dataclass(frozen=True, slots=True)
class R2DA1RunResult:
    """One complete portable route-parity run and its immutable evidence."""

    config: R2DConfig
    config_sha256: str
    fixture: R2GeneratedFixture
    capture: R2DPortableCapture
    direct_rebuilds: R2DRouteRebuilds
    captured_rebuilds: R2DRouteRebuilds
    normalized_outcome: R2DNormalizedBusinessOutcome
    direct_provenance: R2DRouteProvenance
    captured_provenance: R2DRouteProvenance
    code_sha256: str
    artifact_index: R2DArtifactIndex
    evidence_checkpoint: R2DEvidenceCheckpoint
    correction_semantics: dict[str, object]
    recovery_evidence: dict[str, object]
    producer_ack_artifact: ScientificArtifactRef
    correction_semantics_artifact: ScientificArtifactRef
    recovery_evidence_artifact: ScientificArtifactRef
    transcript_artifact: ScientificArtifactRef
    checkpoint_artifact: ScientificArtifactRef
    normalized_artifact: ScientificArtifactRef
    direct_provenance_artifact: ScientificArtifactRef
    captured_provenance_artifact: ScientificArtifactRef
    evidence_checkpoint_artifact: ScientificArtifactRef
    artifact_index_artifact: ScientificArtifactRef

    @property
    def artifacts(self) -> tuple[ScientificArtifactRef, ...]:
        """Return all ten governed run manifests used before receipt publication."""

        return (
            self.producer_ack_artifact,
            self.correction_semantics_artifact,
            self.recovery_evidence_artifact,
            self.transcript_artifact,
            self.checkpoint_artifact,
            self.normalized_artifact,
            self.direct_provenance_artifact,
            self.captured_provenance_artifact,
            self.evidence_checkpoint_artifact,
            self.artifact_index_artifact,
        )

    @property
    def semantic_sha256(self) -> str:
        """Return the cross-run normalized business identity."""

        return self.normalized_artifact.content_sha256


@dataclass(frozen=True, slots=True)
class R2DA1VerificationResult:
    """Two-replay portable verification and its fixed-path strict receipt."""

    receipt: R2DVerificationReceipt
    receipt_path: Path
    run_a: R2DA1RunResult | None
    run_b: R2DA1RunResult | None
    resource_evidence: R2DResourceEvidence | None
    resource_artifact: ScientificArtifactRef | None
    portable_bundle: R2DPortableBundleRef | None

    @property
    def passed(self) -> bool:
        return self.receipt.status == "PASS"


def split_lf_jsonl_values(raw_jsonl: bytes) -> tuple[bytes, ...]:
    """Split canonical LF-framed JSONL without parsing or reserializing values."""

    if not raw_jsonl:
        return ()
    if not raw_jsonl.endswith(b"\n"):
        raise ValueError("R2D source JSONL must end with one LF delimiter")
    values = tuple(raw_jsonl[:-1].split(b"\n"))
    if any(not value or not value.strip() for value in values):
        raise ValueError("R2D source JSONL contains an empty or whitespace-only value")
    return values


def raw_value_sha256_counts(values: Sequence[bytes]) -> tuple[tuple[str, int], ...]:
    """Return the route-neutral raw-value multiset, excluding JSONL delimiters."""

    counts = ingestion_raw_value_hash_counts(values)
    return tuple(sorted(counts.items()))


def _validate_frozen_source_fixture(
    fixture: R2GeneratedFixture,
    config: R2DConfig,
) -> tuple[bytes, ...]:
    """Re-read the generated fixture and require every frozen source-contract identity."""

    source_contract = config.source_contract
    manifest_bytes = fixture.manifest_path.read_bytes()
    strict_manifest = R2SourceManifest.model_validate_json(manifest_bytes)
    manifest_semantic_sha256 = canonical_json_sha256(strict_manifest.model_dump(mode="json"))
    inventory_path = fixture.source_dir / _INVENTORY_FILE
    inventory_bytes = inventory_path.read_bytes()
    raw_values = split_lf_jsonl_values(inventory_bytes)
    inventory_source = next(
        item for item in strict_manifest.sources if item.dataset == _INVENTORY_DATASET
    )
    observed = {
        "generator": strict_manifest.generator,
        "source_manifest_sha256": manifest_semantic_sha256,
        "source_manifest_file_sha256": file_sha256(fixture.manifest_path),
        "inventory_events_file_sha256": file_sha256(inventory_path),
        "inventory_event_count": len(raw_values),
        "raw_value_multiset_sha256": raw_value_multiset_sha256(raw_values),
    }
    expected = source_contract.model_dump(mode="json")
    if observed != expected:
        raise ValueError(
            f"generated R2 fixture differs from frozen R2D source_contract: observed={observed!r}"
        )
    if (
        strict_manifest != fixture.manifest
        or fixture.manifest_sha256 != manifest_semantic_sha256
        or fixture.manifest_file_sha256 != observed["source_manifest_file_sha256"]
        or inventory_source.content_sha256 != observed["inventory_events_file_sha256"]
        or inventory_source.record_count != len(raw_values)
    ):
        raise ValueError("generated R2 fixture object does not match its re-read source package")
    return raw_values


def _required_headers(config: R2DConfig, routing_key: bytes) -> tuple[tuple[str, bytes], ...]:
    contract = config.header_contract
    return (
        (contract.contract_header_name, contract.contract_header_value.encode("utf-8")),
        (contract.routing_header_name, routing_key),
        (contract.schema_header_name, contract.schema_header_value.encode("ascii")),
    )


def _fixture_physical_records(
    values: Sequence[bytes],
    config: R2DConfig,
) -> tuple[R2DPhysicalRecord, ...]:
    """Adapt exact fixture values to deterministic two-partition physical records."""

    next_offsets = {partition: 0 for partition in range(config.topic_contract.partition_count)}
    records: list[R2DPhysicalRecord] = []
    for source_position, value in enumerate(values):
        routing_key = routing_key_for_value(value, config)
        if routing_key is None:
            raw_sha256 = hashlib.sha256(value).hexdigest()
            raise ValueError(
                f"fixture value has no parsed or config-authorized routing key: sha256={raw_sha256}"
            )
        partition = partition_for_key(
            routing_key,
            partition_count=config.topic_contract.partition_count,
        )
        offset = next_offsets[partition]
        next_offsets[partition] += 1
        records.append(
            R2DPhysicalRecord.from_bytes(
                topic=config.topic_contract.topic,
                partition=partition,
                offset=offset,
                key=routing_key,
                headers=_required_headers(config, routing_key),
                broker_timestamp_ms=_BROKER_TIMESTAMP_BASE_MS + source_position,
                broker_timestamp_type="create_time",
                value=value,
            )
        )
    return tuple(records)


def build_portable_captured_route(
    *,
    reference_source_dir: Path,
    output_dir: Path,
    config: R2DConfig | None = None,
    poll_order: Sequence[int] | None = None,
) -> R2DPortableCapture:
    """Capture the canonical fixture, verify its barrier, and reconstruct an R2A package."""

    resolved_config = load_r2d_config() if config is None else config
    raw_values = split_lf_jsonl_values(
        (reference_source_dir.resolve() / _INVENTORY_FILE).read_bytes()
    )
    physical_records = _fixture_physical_records(raw_values, resolved_config)
    producer_ack_ledger = build_portable_producer_ack_ledger(
        physical_records,
        resolved_config,
    )
    producer_ack_ledger = validate_portable_producer_ack_ledger(
        producer_ack_ledger,
        physical_records,
        resolved_config,
        require_frozen_source=True,
    )
    positions = tuple(range(len(physical_records))) if poll_order is None else tuple(poll_order)
    if tuple(sorted(positions)) != tuple(range(len(physical_records))):
        raise ValueError("poll_order must contain every physical-record position exactly once")
    transcript = capture_transcript(
        (physical_records[position] for position in positions),
        resolved_config,
    )
    excluded = tuple(item for item in transcript.records if item.decision.disposition != "admitted")
    if excluded:
        reasons = Counter(item.decision.reason for item in excluded)
        raise ValueError(f"positive R2D fixture contains transport exclusions: {dict(reasons)}")
    if any(
        not transcript.partition_records(partition)
        for partition in range(resolved_config.topic_contract.partition_count)
    ):
        raise ValueError("positive R2D fixture must exercise both configured partitions")

    starts = {partition: 0 for partition in range(resolved_config.topic_contract.partition_count)}
    checkpoint = build_capture_checkpoint(
        config=resolved_config,
        transcript=transcript,
        starts=starts,
        producer_ack_ledger=producer_ack_ledger,
    )
    checkpoint = verify_complete_checkpoint(
        checkpoint,
        transcript,
        resolved_config,
        producer_ack_ledger,
    )
    captured_values = tuple(
        value
        for item in transcript.records
        if item.decision.disposition == "admitted"
        for value in (item.record.value_bytes,)
        if value is not None
    )
    direct_counts = ingestion_raw_value_hash_counts(raw_values)
    acknowledged_counts = Counter(
        item.raw_value_sha256 for item in producer_ack_ledger.acknowledgements
    )
    captured_counts = ingestion_raw_value_hash_counts(captured_values)
    if direct_counts != acknowledged_counts or direct_counts != captured_counts:
        raise ValueError("direct, producer-ack, and captured raw-value multisets differ")

    reconstructed_jsonl = reconstruct_jsonl_bytes(transcript, resolved_config)
    if direct_counts != ingestion_raw_value_hash_counts(split_lf_jsonl_values(reconstructed_jsonl)):
        raise ValueError(
            "direct, producer-ack, capture, and reconstructed raw-value multisets differ"
        )
    package = reconstruct_r2_source_package(
        reference_source_dir=reference_source_dir,
        output_dir=output_dir,
        inventory_jsonl=reconstructed_jsonl,
    )
    return R2DPortableCapture(
        raw_values=raw_values,
        producer_ack_ledger=producer_ack_ledger,
        transcript=transcript,
        checkpoint=checkpoint,
        package=package,
    )


def _write_exact_bytes(path: Path, value: bytes, *, root: Path) -> None:
    """Publish exact bytes once without following any package-internal symlink."""

    root = root.absolute()
    target = path.absolute()
    try:
        relative = target.relative_to(root)
    except ValueError as error:
        raise R2DPackageConflictError(
            f"publication target escapes governed root: {target}"
        ) from error
    if not relative.parts:
        raise R2DPackageConflictError("publication target must be a file below the governed root")
    existing_chain = (*tuple(reversed(root.parents)), root)
    for ancestor in existing_chain:
        if not os.path.lexists(ancestor):
            continue
        stat_result = os.lstat(ancestor)
        is_reparse_point = bool(
            getattr(stat_result, "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        )
        is_junction = bool(getattr(ancestor, "is_junction", lambda: False)())
        if ancestor.is_symlink() or is_junction or is_reparse_point:
            raise R2DPackageConflictError(
                f"symbolic-link or reparse publication ancestor refused: {ancestor}"
            )
    root.mkdir(parents=True, exist_ok=True)
    if not root.is_dir() or root.is_symlink():
        raise R2DPackageConflictError(f"governed publication root is not a safe directory: {root}")

    current = root
    for segment in relative.parts[:-1]:
        current /= segment
        if current.is_symlink() or bool(getattr(current, "is_junction", lambda: False)()):
            raise R2DPackageConflictError(f"link-like publication ancestor refused: {current}")
        if os.path.lexists(current):
            if not current.is_dir():
                raise R2DPackageConflictError(f"publication ancestor is not a directory: {current}")
        else:
            current.mkdir()
        if current.is_symlink():
            raise R2DPackageConflictError(f"symbolic-link publication ancestor refused: {current}")

    if target.is_symlink():
        raise R2DPackageConflictError(f"symbolic-link publication target refused: {target}")
    if os.path.lexists(target):
        if not target.is_file():
            raise R2DPackageConflictError(f"publication target is not a regular file: {target}")
        if target.read_bytes() != value:
            raise R2DPackageConflictError(
                f"refusing to overwrite reconstructed package file: {target}"
            )
        return

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=target.parent,
            delete=False,
        ) as temporary:
            temporary.write(value)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        try:
            os.link(temporary_path, target)
        except FileExistsError:
            if target.is_symlink() or target.read_bytes() != value:
                raise R2DPackageConflictError(
                    f"conflicting concurrent publication at reconstructed package file: {target}"
                ) from None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _preflight_governed_directory(path: Path) -> Path:
    """Create one governed directory without traversing links or reparse points."""

    candidate = path.absolute()
    if candidate == Path(candidate.anchor):
        raise R2DPackageConflictError("filesystem roots cannot be governed artifact roots")
    chain = (*tuple(reversed(candidate.parents)), candidate)
    for component in chain:
        if not os.path.lexists(component):
            with suppress(FileExistsError):
                component.mkdir()
        stat_result = os.lstat(component)
        is_reparse_point = bool(
            getattr(stat_result, "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        )
        is_junction = bool(getattr(component, "is_junction", lambda: False)())
        if component.is_symlink() or is_junction or is_reparse_point:
            raise R2DPackageConflictError(
                f"link-like governed directory component refused: {component}"
            )
        if not stat.S_ISDIR(stat_result.st_mode):
            raise R2DPackageConflictError(
                f"governed directory component is not a directory: {component}"
            )
    return candidate


def reconstruct_r2_source_package(
    *,
    reference_source_dir: Path,
    output_dir: Path,
    inventory_jsonl: bytes,
) -> R2DReconstructedPackage:
    """Copy batch bytes and bind reconstructed inventory JSONL into an R2A package."""

    reference_source_dir = reference_source_dir.resolve()
    output_dir = output_dir.absolute()
    if output_dir == reference_source_dir:
        raise ValueError("captured package output must differ from its direct reference package")
    inventory_values = split_lf_jsonl_values(inventory_jsonl)

    reference_manifest_path = reference_source_dir / "source-manifest.json"
    manifest = R2SourceManifest.model_validate_json(reference_manifest_path.read_bytes())
    inventory_entries = [
        source for source in manifest.sources if source.dataset == _INVENTORY_DATASET
    ]
    if len(inventory_entries) != 1 or inventory_entries[0].file_name != _INVENTORY_FILE:
        raise ValueError("reference package has no unique inventory_events.jsonl binding")

    inventory_sha256 = hashlib.sha256(inventory_jsonl).hexdigest()
    sources = tuple(
        source.model_copy(
            update={
                "content_sha256": inventory_sha256,
                "record_count": len(inventory_values),
            }
        )
        if source.dataset == _INVENTORY_DATASET
        else source
        for source in manifest.sources
    )
    reconstructed_manifest = manifest.model_copy(update={"sources": sources})

    for source in manifest.sources:
        if source.dataset == _INVENTORY_DATASET:
            continue
        source_path = reference_source_dir / source.file_name
        source_bytes = source_path.read_bytes()
        if hashlib.sha256(source_bytes).hexdigest() != source.content_sha256:
            raise ValueError(f"reference package source hash mismatch: {source.file_name}")
        _write_exact_bytes(output_dir / source.file_name, source_bytes, root=output_dir)

    _write_exact_bytes(output_dir / _INVENTORY_FILE, inventory_jsonl, root=output_dir)
    manifest_bytes = (
        json.dumps(
            reconstructed_manifest.model_dump(mode="json"),
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")
    manifest_path = output_dir / "source-manifest.json"
    _write_exact_bytes(manifest_path, manifest_bytes, root=output_dir)
    rebound = R2SourceManifest.model_validate_json(manifest_path.read_bytes())
    return R2DReconstructedPackage(
        source_dir=output_dir,
        manifest_path=manifest_path,
        manifest=rebound,
        manifest_file_sha256=file_sha256(manifest_path),
        inventory_jsonl_sha256=inventory_sha256,
        inventory_record_count=len(inventory_values),
    )


def run_route_rebuilds(
    *,
    project_root: Path,
    source_dir: Path,
    artifact_dir: Path,
    early_cutoff: datetime,
    final_cutoff: datetime,
) -> R2DRouteRebuilds:
    """Run the unchanged public R2A entry point for both governed cutoffs."""

    early = run_r2_rebuild(
        project_root=project_root,
        source_dir=source_dir,
        artifact_dir=artifact_dir / "early",
        planning_cutoff=early_cutoff,
    )
    final = run_r2_rebuild(
        project_root=project_root,
        source_dir=source_dir,
        artifact_dir=artifact_dir / "final",
        planning_cutoff=final_cutoff,
    )
    return R2DRouteRebuilds(early=early, final=final)


def _summary_watermarks(result: R2RebuildResult) -> tuple[tuple[str, str | None], ...]:
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    if summary.get("release") != "R2A" or summary.get("status") != "PASS":
        raise ValueError(f"R2D requires a passing R2A summary: {result.summary_path}")
    value = summary.get("watermarks")
    if not isinstance(value, dict):
        raise ValueError(f"R2A summary has no watermarks object: {result.summary_path}")
    watermarks: list[tuple[str, str | None]] = []
    for name, watermark in sorted(value.items()):
        if watermark is not None and not isinstance(watermark, str):
            raise ValueError(f"R2A watermark {name!r} is not a string or null")
        watermarks.append((str(name), watermark))
    return tuple(watermarks)


def normalize_cutoff_outcome(result: R2RebuildResult) -> R2DCutoffOutcome:
    """Remove route provenance while preserving all bounded business evidence."""

    reasons = Counter(record.reason_code.value for record in result.landing.quarantined)
    gold = tuple(
        R2DGoldEvidence(
            dataset=artifact.dataset,
            row_count=artifact.row_count,
            content_sha256=artifact.content_sha256,
            schema_sha256=artifact.schema_sha256,
        )
        for artifact in sorted(result.gold_artifacts, key=lambda item: item.dataset)
    )
    return R2DCutoffOutcome(
        planning_cutoff=result.planning_cutoff.isoformat().replace("+00:00", "Z"),
        watermarks=_summary_watermarks(result),
        quality_counts=tuple(sorted(result.quality_counts.items())),
        quarantine_reason_counts=tuple(sorted(reasons.items())),
        gold=gold,
    )


def normalize_business_outcome(
    *,
    raw_values: Sequence[bytes],
    rebuilds: R2DRouteRebuilds,
) -> R2DNormalizedBusinessOutcome:
    """Build one exact-comparable direct or captured business outcome."""

    return R2DNormalizedBusinessOutcome(
        raw_value_sha256_counts=raw_value_sha256_counts(raw_values),
        cutoffs=tuple(normalize_cutoff_outcome(result) for result in rebuilds.ordered()),
    )


def _cutoff_text(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _verified_materialized_artifact(
    *,
    cutoff: str,
    stage: Literal["validated", "gold"],
    artifact: ContentAddressedArtifact,
) -> R2DMaterializedArtifactEvidence:
    """Re-read one Parquet and require every identity advertised by R2A."""

    dataset = artifact.dataset
    path = artifact.path
    expected_content = artifact.content_sha256
    expected_schema = artifact.schema_sha256
    expected_rows = artifact.row_count
    table = pq.read_table(path)
    observed_content = canonical_rows_sha256(table.to_pylist())
    observed_schema = canonical_schema_sha256(table.schema)
    observed_rows = table.num_rows
    if (
        observed_content != expected_content
        or observed_schema != expected_schema
        or observed_rows != expected_rows
    ):
        raise ValueError(f"R2A {stage} artifact failed independent verification: {dataset}")
    return R2DMaterializedArtifactEvidence(
        cutoff=cutoff,
        stage=stage,
        dataset=dataset,
        row_count=observed_rows,
        content_sha256=observed_content,
        schema_sha256=observed_schema,
        file_sha256=file_sha256(path),
    )


def _source_record_count(path: Path) -> int:
    if path.suffix == ".csv":
        lines = path.read_bytes().splitlines()
        return max(0, len(lines) - 1)
    return len(split_lf_jsonl_values(path.read_bytes()))


def _verified_source_files(result: R2RebuildResult) -> tuple[R2DSourceFileEvidence, ...]:
    """Re-read a landing package, including its strict manifest, for provenance."""

    native_by_dataset = {path.parent.name: path for path in result.landing.source_native_paths}
    manifest_path = native_by_dataset.get("source_manifest")
    if manifest_path is None:
        raise ValueError("R2A landing did not retain source-manifest.json")
    manifest_bytes = manifest_path.read_bytes()
    manifest = R2SourceManifest.model_validate_json(manifest_bytes)
    if (
        canonical_json_sha256(manifest.model_dump(mode="json")) != result.source_manifest_sha256
        or file_sha256(manifest_path) != result.source_manifest_file_sha256
        or manifest != result.landing.source_manifest
    ):
        raise ValueError("R2A landing source manifest failed independent verification")
    evidence: list[R2DSourceFileEvidence] = []
    for source in sorted(manifest.sources, key=lambda item: item.dataset):
        path = native_by_dataset.get(source.dataset.replace(".", "_"))
        if path is None:
            raise ValueError(f"R2A landing omitted manifest source {source.file_name}")
        observed_hash = file_sha256(path)
        observed_count = _source_record_count(path)
        if observed_hash != source.content_sha256 or observed_count != source.record_count:
            raise ValueError(
                f"R2A landing source failed independent verification: {source.dataset}"
            )
        evidence.append(
            R2DSourceFileEvidence(
                dataset=source.dataset,
                file_name=source.file_name,
                record_count=observed_count,
                file_sha256=observed_hash,
            )
        )
    return tuple(evidence)


def _verified_summary(result: R2RebuildResult) -> R2DSummaryEvidence:
    cutoff = _cutoff_text(result.planning_cutoff)
    summary_bytes = result.summary_path.read_bytes()
    summary = json.loads(summary_bytes)
    if not isinstance(summary, dict):
        raise ValueError("R2A summary root must be an object")
    semantic_payload = dict(summary)
    stored_semantic_sha256 = semantic_payload.pop("semantic_manifest_sha256", None)
    semantic_payload.pop("status", None)
    dbt_payload = semantic_payload.get("dbt")
    if not isinstance(dbt_payload, dict):
        raise ValueError("R2A summary dbt evidence must be an object")
    semantic_payload["dbt"] = {key: value for key, value in dbt_payload.items() if key != "detail"}
    recomputed_semantic_sha256 = canonical_rows_sha256([{"manifest": semantic_payload}])
    if (
        summary.get("release") != "R2A"
        or summary.get("status") != "PASS"
        or summary.get("planning_cutoff") != cutoff
        or summary.get("source_manifest_sha256") != result.source_manifest_sha256
        or summary.get("source_manifest_file_sha256") != result.source_manifest_file_sha256
        or stored_semantic_sha256 != result.semantic_manifest_sha256
        or recomputed_semantic_sha256 != result.semantic_manifest_sha256
    ):
        raise ValueError("R2A summary fields differ from the public rebuild result")
    return R2DSummaryEvidence(
        cutoff=cutoff,
        semantic_manifest_sha256=result.semantic_manifest_sha256,
        file_sha256=hashlib.sha256(summary_bytes).hexdigest(),
    )


def route_provenance(
    *,
    route: Literal["direct", "captured"],
    rebuilds: R2DRouteRebuilds,
    checkpoint_sha256: str | None = None,
) -> R2DRouteProvenance:
    """Collect separately verified route identities without normalizing them away."""

    ordered = rebuilds.ordered()
    source_content = {result.source_manifest_sha256 for result in ordered}
    source_files = {result.source_manifest_file_sha256 for result in ordered}
    if len(source_content) != 1 or len(source_files) != 1:
        raise ValueError("one route's cutoff rebuilds must use the same source package")
    semantic = tuple(
        (
            _cutoff_text(result.planning_cutoff),
            result.semantic_manifest_sha256,
        )
        for result in ordered
    )
    summaries = tuple(
        (
            _cutoff_text(result.planning_cutoff),
            file_sha256(result.summary_path),
        )
        for result in ordered
    )
    source_evidence = tuple(_verified_source_files(result) for result in ordered)
    if any(items != source_evidence[0] for items in source_evidence[1:]):
        raise ValueError("one route's cutoff rebuilds produced different source-file evidence")
    validated = tuple(
        _verified_materialized_artifact(
            cutoff=_cutoff_text(result.planning_cutoff),
            stage="validated",
            artifact=artifact,
        )
        for result in ordered
        for artifact in sorted(result.landing.validated_artifacts, key=lambda item: item.dataset)
    )
    gold = tuple(
        _verified_materialized_artifact(
            cutoff=_cutoff_text(result.planning_cutoff),
            stage="gold",
            artifact=artifact,
        )
        for result in ordered
        for artifact in sorted(result.gold_artifacts, key=lambda item: item.dataset)
    )
    summary_evidence = tuple(_verified_summary(result) for result in ordered)
    return R2DRouteProvenance(
        route=route,
        source_manifest_sha256=next(iter(source_content)),
        source_manifest_file_sha256=next(iter(source_files)),
        semantic_manifests=semantic,
        summary_file_sha256=summaries,
        source_files=source_evidence[0],
        validated_artifacts=validated,
        gold_artifacts=gold,
        summaries=summary_evidence,
        checkpoint_sha256=checkpoint_sha256,
    )


def require_business_parity(
    direct: R2DNormalizedBusinessOutcome,
    captured: R2DNormalizedBusinessOutcome,
) -> None:
    """Fail closed unless normalized direct and captured outcomes exact-match."""

    if direct != captured:
        raise R2DParityError("direct and captured routes disagree on normalized business evidence")


def _write_r2d_manifest(
    output_dir: Path,
    *,
    dataset: str,
    value: object,
    contract: str,
) -> ScientificArtifactRef:
    return write_immutable_json_manifest(
        output_dir,
        dataset=dataset,
        manifest=value,
        schema_identity={"contract": contract},
    )


def _normalized_payload(outcome: R2DNormalizedBusinessOutcome) -> dict[str, object]:
    return cast(dict[str, object], asdict(outcome))


def _provenance_payload(provenance: R2DRouteProvenance) -> dict[str, object]:
    return cast(
        dict[str, object],
        json.loads(canonical_json_bytes(asdict(provenance))),
    )


def _special_event(
    template: dict[str, object],
    *,
    revision: int,
    event_id: str,
    idempotency_key: str,
    ingested_at: str,
) -> dict[str, object]:
    event = cast(dict[str, object], json.loads(json.dumps(template)))
    payload = cast(dict[str, object], event["payload"])
    payload.update(
        {
            "delta_qty": -revision,
            "location_id": "DC-ARG-01",
            "logical_event_id": "ADJ-R2D-GAP-001",
            "revision": revision,
            "sku_id": "SKU-001",
        }
    )
    event.update(
        {
            "event_id": event_id,
            "idempotency_key": idempotency_key,
            "ingested_at": ingested_at,
            "occurred_at": "2026-06-02T09:00:00Z",
            "subject_id": "SKU-001:DC-ARG-01",
        }
    )
    return event


def _captured_special_jsonl(
    events: Sequence[dict[str, object]],
    config: R2DConfig,
) -> tuple[bytes, R2DCaptureTranscript]:
    values = tuple(canonical_json_bytes(event) for event in events)
    transcript = capture_transcript(_fixture_physical_records(values, config), config)
    if not all(item.decision.disposition == "admitted" for item in transcript.records):
        raise ValueError("special correction capture contains an excluded transport record")
    reconstructed = reconstruct_jsonl_bytes(transcript, config)
    if ingestion_raw_value_hash_counts(values) != ingestion_raw_value_hash_counts(
        split_lf_jsonl_values(reconstructed)
    ):
        raise ValueError("special correction capture changed raw-value multiplicity")
    return reconstructed, transcript


def _write_special_source_package(
    *,
    fixture: R2GeneratedFixture,
    output_dir: Path,
    inventory_jsonl: bytes,
    accepted_count: int,
    quarantine_count: int,
) -> R2DReconstructedPackage:
    manifest = fixture.manifest
    values = split_lf_jsonl_values(inventory_jsonl)
    inventory_sha256 = hashlib.sha256(inventory_jsonl).hexdigest()
    sources = tuple(
        source.model_copy(
            update={
                "content_sha256": inventory_sha256,
                "record_count": len(values),
            }
        )
        if source.dataset == _INVENTORY_DATASET
        else source
        for source in manifest.sources
    )
    controls = manifest.expected_controls.model_copy(
        update={
            "accepted_inventory_adjustments": accepted_count,
            "quarantine_rows": quarantine_count,
        }
    )
    rebound = manifest.model_copy(update={"sources": sources, "expected_controls": controls})
    for source in manifest.sources:
        if source.dataset == _INVENTORY_DATASET:
            continue
        _write_exact_bytes(
            output_dir / source.file_name,
            (fixture.source_dir / source.file_name).read_bytes(),
            root=output_dir,
        )
    _write_exact_bytes(output_dir / _INVENTORY_FILE, inventory_jsonl, root=output_dir)
    manifest_bytes = (
        json.dumps(
            rebound.model_dump(mode="json"),
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")
    manifest_path = output_dir / "source-manifest.json"
    _write_exact_bytes(manifest_path, manifest_bytes, root=output_dir)
    return R2DReconstructedPackage(
        source_dir=output_dir,
        manifest_path=manifest_path,
        manifest=R2SourceManifest.model_validate_json(manifest_bytes),
        manifest_file_sha256=file_sha256(manifest_path),
        inventory_jsonl_sha256=inventory_sha256,
        inventory_record_count=len(values),
    )


def _correction_semantics_evidence(
    *,
    output_dir: Path,
    fixture: R2GeneratedFixture,
    config: R2DConfig,
) -> dict[str, object]:
    """Exercise captured missing-prefix, recovery, and cutoff-visible-gap semantics."""

    template = json.loads((fixture.source_dir / _INVENTORY_FILE).read_bytes().splitlines()[0])
    revision_two = _special_event(
        template,
        revision=2,
        event_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        idempotency_key="KEY-R2D-GAP-2",
        ingested_at="2026-06-08T10:00:00Z",
    )
    revision_one = _special_event(
        template,
        revision=1,
        event_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        idempotency_key="KEY-R2D-GAP-1",
        ingested_at="2026-06-15T10:00:00Z",
    )
    rev2_jsonl, rev2_transcript = _captured_special_jsonl((revision_two,), config)
    complete_jsonl, complete_transcript = _captured_special_jsonl(
        (revision_two, revision_one),
        config,
    )
    rev2_package = _write_special_source_package(
        fixture=fixture,
        output_dir=output_dir / "rev2-only-source",
        inventory_jsonl=rev2_jsonl,
        accepted_count=0,
        quarantine_count=1,
    )
    complete_package = _write_special_source_package(
        fixture=fixture,
        output_dir=output_dir / "complete-source",
        inventory_jsonl=complete_jsonl,
        accepted_count=2,
        quarantine_count=0,
    )
    rev2_landing = land_r2_sources(
        source_dir=rev2_package.source_dir,
        output_dir=output_dir / "rev2-only-landing",
    )
    complete_landing = land_r2_sources(
        source_dir=complete_package.source_dir,
        output_dir=output_dir / "complete-landing",
    )
    gap_cutoff = datetime.fromisoformat("2026-06-10T12:00:00+00:00")
    gap_rebuild = run_r2_rebuild(
        project_root=_PROJECT_ROOT,
        source_dir=complete_package.source_dir,
        artifact_dir=output_dir / "gap-rebuild",
        planning_cutoff=gap_cutoff,
    )
    with duckdb.connect(str(gap_rebuild.database_path), read_only=True) as connection:
        deferred_reasons = tuple(
            row[0]
            for row in connection.execute(
                "select distinct deferred_reason "
                "from int_r2_deferred_inventory_adjustments order by deferred_reason"
            ).fetchall()
        )
    rev2_reason_counts = Counter(item.reason_code.value for item in rev2_landing.quarantined)
    complete_revisions = tuple(
        item.revision
        for item in complete_landing.inventory_adjustments
        if item.logical_event_id == "ADJ-R2D-GAP-001"
    )
    if (
        rev2_reason_counts != Counter({R2QuarantineReason.CORRECTION_CHAIN_INVALID.value: 1})
        or complete_revisions != (1, 2)
        or gap_rebuild.quality_counts["chain_deferred_inventory_adjustments"] != 1
        or "incomplete_visible_revision_chain" not in deferred_reasons
    ):
        raise ValueError("public R2A correction semantics differ from the R2D-A1 contract")
    return {
        "contract_version": "r2d.a1.correction-semantics.v1",
        "release": "R2D-A1",
        "real_service_claim": False,
        "captured_append_revisions": [2, 1],
        "rev2_only_transcript_sha256": rev2_transcript.content_sha256,
        "rev2_only_lineage_sha256": rev2_transcript.lineage_sha256,
        "rev2_only_source_manifest_sha256": rev2_landing.source_manifest_sha256,
        "rev2_only_quarantine_reason_counts": dict(sorted(rev2_reason_counts.items())),
        "complete_transcript_sha256": complete_transcript.content_sha256,
        "complete_lineage_sha256": complete_transcript.lineage_sha256,
        "complete_source_manifest_sha256": complete_landing.source_manifest_sha256,
        "complete_validated_revisions": list(complete_revisions),
        "later_complete_replay_corrects": True,
        "gap_planning_cutoff": _cutoff_text(gap_cutoff),
        "gap_semantic_manifest_sha256": gap_rebuild.semantic_manifest_sha256,
        "gap_summary_file_sha256": file_sha256(gap_rebuild.summary_path),
        "gap_chain_deferred_count": gap_rebuild.quality_counts[
            "chain_deferred_inventory_adjustments"
        ],
        "gap_deferred_reasons": list(deferred_reasons),
    }


def _portable_recovery_evidence(
    *,
    output_dir: Path,
    fixture: R2GeneratedFixture,
    capture: R2DPortableCapture,
    config: R2DConfig,
    expected_outcome: R2DNormalizedBusinessOutcome,
    expected_provenance: R2DRouteProvenance,
) -> dict[str, object]:
    """Rebuild derived state from retained capture/checkpoint and six static files only."""

    verify_complete_checkpoint(
        capture.checkpoint,
        capture.transcript,
        config,
        capture.producer_ack_ledger,
    )
    recovered_jsonl = reconstruct_jsonl_bytes(capture.transcript, config)
    package = reconstruct_r2_source_package(
        reference_source_dir=fixture.source_dir,
        output_dir=output_dir / "reconstructed-source",
        inventory_jsonl=recovered_jsonl,
    )
    early_cutoff, final_cutoff = (
        item.planning_cutoff for item in config.snapshot_contract.planning_cutoffs
    )
    rebuilds = run_route_rebuilds(
        project_root=_PROJECT_ROOT,
        source_dir=package.source_dir,
        artifact_dir=output_dir / "derived",
        early_cutoff=early_cutoff,
        final_cutoff=final_cutoff,
    )
    recovered_outcome = normalize_business_outcome(
        raw_values=split_lf_jsonl_values(recovered_jsonl),
        rebuilds=rebuilds,
    )
    require_business_parity(expected_outcome, recovered_outcome)
    recovered_provenance = route_provenance(
        route="captured",
        rebuilds=rebuilds,
        checkpoint_sha256=capture.checkpoint.content_sha256,
    )
    if _without_file_evidence(asdict(recovered_provenance)) != _without_file_evidence(
        asdict(expected_provenance)
    ):
        raise ValueError("portable recovery provenance semantics differ from captured route")
    return {
        "contract_version": "r2d.a1.portable-recovery.v1",
        "release": "R2D-A1",
        "operational_recovery_claim": False,
        "retained_evidence": [
            "capture_transcript",
            "complete_capture_checkpoint",
            "producer_ack_ledger",
            "six_static_source_files",
        ],
        "regenerated_derived_state": [
            "inventory_events.jsonl",
            "validated_lake",
            "dbt_duckdb_warehouse",
            "five_gold_products_per_cutoff",
        ],
        "checkpoint_sha256": capture.checkpoint.content_sha256,
        "reconstructed_jsonl_sha256": file_sha256(package.source_dir / _INVENTORY_FILE),
        "normalized_business_sha256": canonical_json_sha256(_normalized_payload(recovered_outcome)),
        "semantic_manifests": [list(item) for item in recovered_provenance.semantic_manifests],
        "gold": [
            {
                "cutoff": item.cutoff,
                "dataset": item.dataset,
                "row_count": item.row_count,
                "content_sha256": item.content_sha256,
                "schema_sha256": item.schema_sha256,
            }
            for item in recovered_provenance.gold_artifacts
        ],
        "exact_business_and_provenance_semantics_match": True,
    }


def _code_bundle_sha256() -> str:
    files = tuple(
        {"path": relative, "file_sha256": r2d_lf_canonical_file_sha256(_PROJECT_ROOT / relative)}
        for relative in _CODE_BUNDLE_FILES
    )
    return canonical_json_sha256(
        {
            "contract_version": "r2d.a1.code-bundle.v1",
            "files": files,
            "r2_dbt_code_sha256": r2_dbt_code_sha256(_PROJECT_ROOT),
            "r2_pipeline_code_sha256": r2_pipeline_code_sha256(_PROJECT_ROOT),
        }
    )


def _require_schema_bundle(config: R2DConfig) -> str:
    envelope_path = _PROJECT_ROOT / "contracts" / "schemas" / "event-envelope.schema.json"
    payload_path = (
        _PROJECT_ROOT / "contracts" / "schemas" / "r2-inventory-adjustment-payload.schema.json"
    )
    envelope = json.loads(envelope_path.read_bytes())
    payload = json.loads(payload_path.read_bytes())
    schema = config.schema_contract
    expected_payload = {
        "contract_version": "r2d.schema-bundle.v1",
        "schemas": [
            {
                "contract_id": envelope["$id"],
                "sha256": r2d_lf_canonical_file_sha256(envelope_path),
            },
            {
                "contract_id": payload["$id"],
                "sha256": r2d_lf_canonical_file_sha256(payload_path),
            },
        ],
    }
    if (
        expected_payload != r2d_schema_bundle_payload(schema)
        or canonical_json_sha256(expected_payload) != r2d_schema_bundle_sha256(schema)
        or r2d_schema_bundle_sha256(schema) != schema.schema_bundle_sha256
        or config.header_contract.schema_header_value != schema.schema_bundle_sha256
    ):
        raise ValueError("checked-in schemas differ from the canonical R2D schema bundle")
    return schema.schema_bundle_sha256


def _relative_bundle_path(root: Path, path: Path) -> str:
    root = root.absolute()
    path = path.absolute()
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise ValueError(f"governed artifact is outside the run root: {path}") from error
    if not relative.parts or ".." in relative.parts:
        raise ValueError(f"invalid governed artifact relative path: {relative}")
    current = root
    for segment in relative.parts:
        current /= segment
        if current.is_symlink() or bool(getattr(current, "is_junction", lambda: False)()):
            raise ValueError(f"governed artifact path contains a link-like component: {current}")
    return relative.as_posix()


def _file_entry(
    root: Path,
    path: Path,
    *,
    kind: str,
    **metadata: object,
) -> dict[str, object]:
    return {
        "kind": kind,
        "path": _relative_bundle_path(root, path),
        "file_sha256": file_sha256(path),
        **metadata,
    }


def _pre_checkpoint_artifact_index(
    *,
    output_dir: Path,
    config_sha256: str,
    schema_bundle_sha256: str,
    code_sha256: str,
    fixture: R2GeneratedFixture,
    capture: R2DPortableCapture,
    direct_rebuilds: R2DRouteRebuilds,
    captured_rebuilds: R2DRouteRebuilds,
    manifest_artifacts: Sequence[ScientificArtifactRef],
) -> R2DArtifactIndex:
    """Build the explicitly non-circular, independently reverified artifact set."""

    entries: list[dict[str, object]] = [
        _file_entry(
            output_dir,
            fixture.manifest_path,
            kind="source_manifest",
            route="direct",
            content_sha256=fixture.manifest_sha256,
        ),
        _file_entry(
            output_dir,
            capture.package.manifest_path,
            kind="source_manifest",
            route="captured",
            content_sha256=canonical_json_sha256(capture.package.manifest.model_dump(mode="json")),
        ),
    ]
    for route, manifest, source_dir in (
        ("direct", fixture.manifest, fixture.source_dir),
        ("captured", capture.package.manifest, capture.package.source_dir),
    ):
        for source in sorted(manifest.sources, key=lambda item: item.dataset):
            path = source_dir / source.file_name
            observed_hash = file_sha256(path)
            observed_count = _source_record_count(path)
            if observed_hash != source.content_sha256 or observed_count != source.record_count:
                raise ValueError(f"{route} source package changed before evidence checkpoint")
            entries.append(
                _file_entry(
                    output_dir,
                    path,
                    kind="source_file",
                    route=route,
                    dataset=source.dataset,
                    row_count=observed_count,
                    content_sha256=observed_hash,
                )
            )
    for reference in manifest_artifacts:
        if not _artifact_reload_matches(reference, json.loads(reference.path.read_bytes())):
            raise ValueError(
                f"R2D manifest changed before evidence checkpoint: {reference.dataset}"
            )
        entries.append(
            _file_entry(
                output_dir,
                reference.path,
                kind="r2d_manifest",
                dataset=reference.dataset,
                content_sha256=reference.content_sha256,
                schema_sha256=reference.schema_sha256,
                row_count=reference.row_count,
            )
        )
    for route, rebuilds in (
        ("direct", direct_rebuilds),
        ("captured", captured_rebuilds),
    ):
        for result in rebuilds.ordered():
            cutoff = _cutoff_text(result.planning_cutoff)
            summary = _verified_summary(result)
            entries.append(
                _file_entry(
                    output_dir,
                    result.summary_path,
                    kind="r2a_summary",
                    route=route,
                    cutoff=cutoff,
                    semantic_manifest_sha256=summary.semantic_manifest_sha256,
                )
            )
            for stage, artifacts in (
                ("validated", result.landing.validated_artifacts),
                ("gold", result.gold_artifacts),
            ):
                for artifact in sorted(artifacts, key=lambda item: item.dataset):
                    verified = _verified_materialized_artifact(
                        cutoff=cutoff,
                        stage=cast(Literal["validated", "gold"], stage),
                        artifact=artifact,
                    )
                    entries.append(
                        _file_entry(
                            output_dir,
                            artifact.path,
                            kind="parquet",
                            route=route,
                            cutoff=cutoff,
                            stage=stage,
                            dataset=verified.dataset,
                            row_count=verified.row_count,
                            content_sha256=verified.content_sha256,
                            schema_sha256=verified.schema_sha256,
                        )
                    )
    ordered_entries = tuple(
        sorted(entries, key=lambda item: (str(item["path"]), str(item["kind"])))
    )
    payload = {
        "contract_version": "r2d.a1.artifact-index.v1",
        "release": "R2D-A1",
        "definition": _ARTIFACT_SET_DEFINITION,
        "config_sha256": config_sha256,
        "schema_bundle_sha256": schema_bundle_sha256,
        "code_sha256": code_sha256,
        "entries": ordered_entries,
    }
    return R2DArtifactIndex(
        contract_version="r2d.a1.artifact-index.v1",
        release="R2D-A1",
        definition=_ARTIFACT_SET_DEFINITION,
        config_sha256=config_sha256,
        schema_bundle_sha256=schema_bundle_sha256,
        code_sha256=code_sha256,
        entries=ordered_entries,
        artifact_set_sha256=canonical_json_sha256(payload),
    )


def run_r2d_a1_once(
    output_dir: Path,
    config_path: Path = DEFAULT_R2D_CONFIG_PATH,
) -> R2DA1RunResult:
    """Run one complete portable captured-route parity proof."""

    output_dir = _preflight_governed_directory(output_dir)
    config = load_r2d_config(config_path)
    fixture = generate_r2_smoke_fixture(output_dir / "direct-source", seed=config.seed)
    _validate_frozen_source_fixture(fixture, config)
    capture = build_portable_captured_route(
        reference_source_dir=fixture.source_dir,
        output_dir=output_dir / "captured-source",
        config=config,
    )
    early_cutoff, final_cutoff = (
        item.planning_cutoff for item in config.snapshot_contract.planning_cutoffs
    )
    direct_rebuilds = run_route_rebuilds(
        project_root=_PROJECT_ROOT,
        source_dir=fixture.source_dir,
        artifact_dir=output_dir / "r2a" / "direct",
        early_cutoff=early_cutoff,
        final_cutoff=final_cutoff,
    )
    captured_rebuilds = run_route_rebuilds(
        project_root=_PROJECT_ROOT,
        source_dir=capture.package.source_dir,
        artifact_dir=output_dir / "r2a" / "captured",
        early_cutoff=early_cutoff,
        final_cutoff=final_cutoff,
    )
    reconstructed_values = split_lf_jsonl_values(
        (capture.package.source_dir / _INVENTORY_FILE).read_bytes()
    )
    direct_outcome = normalize_business_outcome(
        raw_values=capture.raw_values,
        rebuilds=direct_rebuilds,
    )
    captured_outcome = normalize_business_outcome(
        raw_values=reconstructed_values,
        rebuilds=captured_rebuilds,
    )
    require_business_parity(direct_outcome, captured_outcome)
    direct_lineage = route_provenance(route="direct", rebuilds=direct_rebuilds)
    captured_lineage = route_provenance(
        route="captured",
        rebuilds=captured_rebuilds,
        checkpoint_sha256=capture.checkpoint.content_sha256,
    )
    recovery_evidence = _portable_recovery_evidence(
        output_dir=output_dir / "portable-recovery",
        fixture=fixture,
        capture=capture,
        config=config,
        expected_outcome=captured_outcome,
        expected_provenance=captured_lineage,
    )

    evidence_dir = output_dir / "evidence"
    producer_ack_artifact = _write_r2d_manifest(
        evidence_dir,
        dataset="r2d.a1.portable_producer_ack",
        value=capture.producer_ack_ledger.model_dump(mode="json"),
        contract="r2d.a1.portable-producer-ack.v1",
    )
    transcript_artifact = _write_r2d_manifest(
        evidence_dir,
        dataset="r2d.a1.capture_transcript",
        value=capture.transcript.model_dump(mode="json"),
        contract="r2d.a1.capture-transcript.v1",
    )
    checkpoint_artifact = _write_r2d_manifest(
        evidence_dir,
        dataset="r2d.a1.capture_checkpoint",
        value=capture.checkpoint.model_dump(mode="json"),
        contract="r2d.a1.capture-checkpoint.v1",
    )
    normalized_artifact = _write_r2d_manifest(
        evidence_dir,
        dataset="r2d.a1.normalized_business_outcome",
        value=_normalized_payload(direct_outcome),
        contract="r2d.a1.normalized-business-outcome.v1",
    )
    direct_provenance_artifact = _write_r2d_manifest(
        evidence_dir,
        dataset="r2d.a1.direct_provenance",
        value=_provenance_payload(direct_lineage),
        contract="r2d.a1.route-provenance.v1",
    )
    captured_provenance_artifact = _write_r2d_manifest(
        evidence_dir,
        dataset="r2d.a1.captured_provenance",
        value=_provenance_payload(captured_lineage),
        contract="r2d.a1.route-provenance.v1",
    )
    correction_semantics = _correction_semantics_evidence(
        output_dir=output_dir / "correction-semantics",
        fixture=fixture,
        config=config,
    )
    correction_semantics_artifact = _write_r2d_manifest(
        evidence_dir,
        dataset="r2d.a1.correction_semantics",
        value=correction_semantics,
        contract="r2d.a1.correction-semantics.v1",
    )
    recovery_evidence_artifact = _write_r2d_manifest(
        evidence_dir,
        dataset="r2d.a1.portable_recovery",
        value=recovery_evidence,
        contract="r2d.a1.portable-recovery.v1",
    )
    config_sha256 = r2d_config_sha256(config)
    schema_bundle_sha256 = _require_schema_bundle(config)
    code_sha256 = _code_bundle_sha256()
    pre_checkpoint_artifacts = (
        producer_ack_artifact,
        transcript_artifact,
        checkpoint_artifact,
        normalized_artifact,
        direct_provenance_artifact,
        captured_provenance_artifact,
        correction_semantics_artifact,
        recovery_evidence_artifact,
    )
    artifact_index = _pre_checkpoint_artifact_index(
        output_dir=output_dir,
        config_sha256=config_sha256,
        schema_bundle_sha256=schema_bundle_sha256,
        code_sha256=code_sha256,
        fixture=fixture,
        capture=capture,
        direct_rebuilds=direct_rebuilds,
        captured_rebuilds=captured_rebuilds,
        manifest_artifacts=pre_checkpoint_artifacts,
    )
    reconstructed_jsonl_path = capture.package.source_dir / _INVENTORY_FILE
    evidence_checkpoint = R2DEvidenceCheckpoint.create(
        source_contract=config.source_contract,
        producer_ack_sha256=capture.producer_ack_ledger.content_sha256,
        transcript_sha256=capture.transcript.content_sha256,
        lineage_sha256=capture.transcript.lineage_sha256,
        capture_checkpoint_sha256=capture.checkpoint.content_sha256,
        reconstructed_jsonl_sha256=file_sha256(reconstructed_jsonl_path),
        code_sha256=code_sha256,
        artifact_set_sha256=artifact_index.artifact_set_sha256,
    )
    evidence_checkpoint_artifact = _write_r2d_manifest(
        evidence_dir,
        dataset="r2d.a1.evidence_checkpoint",
        value=evidence_checkpoint.model_dump(mode="json"),
        contract="r2d.a1.evidence-checkpoint.v1",
    )
    artifact_index_artifact = _write_r2d_manifest(
        evidence_dir,
        dataset="r2d.a1.artifact_index",
        value=asdict(artifact_index),
        contract="r2d.a1.artifact-index.v1",
    )
    result = R2DA1RunResult(
        config=config,
        config_sha256=config_sha256,
        fixture=fixture,
        capture=capture,
        direct_rebuilds=direct_rebuilds,
        captured_rebuilds=captured_rebuilds,
        normalized_outcome=direct_outcome,
        direct_provenance=direct_lineage,
        captured_provenance=captured_lineage,
        code_sha256=code_sha256,
        artifact_index=artifact_index,
        evidence_checkpoint=evidence_checkpoint,
        correction_semantics=correction_semantics,
        recovery_evidence=recovery_evidence,
        producer_ack_artifact=producer_ack_artifact,
        correction_semantics_artifact=correction_semantics_artifact,
        recovery_evidence_artifact=recovery_evidence_artifact,
        transcript_artifact=transcript_artifact,
        checkpoint_artifact=checkpoint_artifact,
        normalized_artifact=normalized_artifact,
        direct_provenance_artifact=direct_provenance_artifact,
        captured_provenance_artifact=captured_provenance_artifact,
        evidence_checkpoint_artifact=evidence_checkpoint_artifact,
        artifact_index_artifact=artifact_index_artifact,
    )
    if not _run_artifacts_reload(result):
        raise ValueError("fresh R2D-A1 immutable artifacts failed nested reload verification")
    return result


def _artifact_reload_matches(reference: ScientificArtifactRef, expected: object) -> bool:
    try:
        payload = reference.path.read_bytes()
        parsed = json.loads(payload)
        normalized_expected = json.loads(canonical_json_bytes(expected))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return (
        payload == canonical_json_bytes(parsed) + b"\n"
        and file_sha256(reference.path) == reference.file_sha256
        and canonical_json_sha256(parsed) == reference.content_sha256
        and parsed == normalized_expected
        and reference.row_count == 1
    )


def _artifact_index_payload(index: R2DArtifactIndex) -> dict[str, object]:
    return {key: value for key, value in asdict(index).items() if key != "artifact_set_sha256"}


def _safe_bundle_artifact_path(root: Path, relative_text: str) -> Path:
    relative = PurePosixPath(relative_text)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError(f"portable bundle contains unsafe artifact path: {relative_text!r}")
    if any("\\" in segment or ":" in segment for segment in relative.parts):
        raise ValueError(f"portable bundle path is not relative POSIX: {relative_text!r}")
    root = root.absolute()
    candidate = root.joinpath(*relative.parts)
    current = root
    for segment in relative.parts:
        current /= segment
        if current.is_symlink() or bool(getattr(current, "is_junction", lambda: False)()):
            raise ValueError(f"portable bundle artifact path contains a link: {current}")
    if not candidate.is_file():
        raise ValueError(f"portable bundle artifact is missing: {candidate}")
    return candidate


def _artifact_entry_identity(entry: dict[str, object]) -> tuple[object, ...]:
    kind = entry.get("kind")
    if kind == "source_manifest":
        return kind, entry.get("route")
    if kind == "source_file":
        return kind, entry.get("route"), entry.get("dataset")
    if kind == "r2d_manifest":
        return kind, entry.get("dataset")
    if kind == "r2a_summary":
        return kind, entry.get("route"), entry.get("cutoff")
    if kind == "parquet":
        return (
            kind,
            entry.get("route"),
            entry.get("cutoff"),
            entry.get("stage"),
            entry.get("dataset"),
        )
    return kind, entry.get("path")


def _required_artifact_index_identities(config: R2DConfig) -> tuple[tuple[object, ...], ...]:
    cutoffs = tuple(
        _cutoff_text(item.planning_cutoff) for item in config.snapshot_contract.planning_cutoffs
    )
    identities: list[tuple[object, ...]] = []
    for route in ("direct", "captured"):
        identities.append(("source_manifest", route))
        identities.extend(("source_file", route, dataset) for dataset, _ in _EXPECTED_SOURCE_FILES)
    identities.extend(("r2d_manifest", dataset) for dataset in _EXPECTED_PRECHECKPOINT_MANIFESTS)
    for route in ("direct", "captured"):
        for cutoff in cutoffs:
            identities.append(("r2a_summary", route, cutoff))
            identities.extend(
                ("parquet", route, cutoff, "validated", dataset)
                for dataset in _EXPECTED_VALIDATED_DATASETS
            )
            identities.extend(
                ("parquet", route, cutoff, "gold", dataset) for dataset in _EXPECTED_GOLD_DATASETS
            )
    return tuple(sorted(identities, key=repr))


def _artifact_entry_path_matches_topology(
    entry: dict[str, object],
    config: R2DConfig,
) -> bool:
    path = PurePosixPath(cast(str, entry["path"]))
    kind = entry["kind"]
    route = cast(str | None, entry.get("route"))
    if kind == "source_manifest":
        return path == PurePosixPath(f"{route}-source/source-manifest.json")
    if kind == "source_file":
        expected_files = dict(_EXPECTED_SOURCE_FILES)
        return path == PurePosixPath(
            f"{route}-source/{expected_files.get(cast(str, entry.get('dataset')), '')}"
        )
    if kind == "r2d_manifest":
        dataset = cast(str, entry["dataset"])
        expected = PurePosixPath("evidence", *dataset.split("."), f"{entry['content_sha256']}.json")
        return path == expected
    cutoff_names = {
        _cutoff_text(item.planning_cutoff): item.name
        for item in config.snapshot_contract.planning_cutoffs
    }
    cutoff_name = cutoff_names.get(cast(str, entry.get("cutoff")))
    if cutoff_name is None:
        return False
    if kind == "r2a_summary":
        return (
            path.parent == PurePosixPath("r2a", cast(str, route), cutoff_name, "manifests")
            and path.name == f"{entry['semantic_manifest_sha256']}.json"
        )
    if kind == "parquet":
        stage = entry.get("stage")
        dataset = cast(str, entry.get("dataset"))
        prefix = PurePosixPath("r2a", cast(str, route), cutoff_name)
        if not path.is_relative_to(prefix) or path.name != f"{entry['content_sha256']}.parquet":
            return False
        segments = dataset.split(".")
        if stage == "gold":
            return path.parent == prefix.joinpath("exports", *segments)
        return (
            stage == "validated"
            and len(path.parts) >= len(prefix.parts) + len(segments) + 2
            and tuple(path.parent.parts[-len(segments) :]) == tuple(segments)
            and path.parts[len(prefix.parts)] == "lake"
        )
    return False


def _provenance_index_expectations(
    payload: object,
    *,
    route: Literal["direct", "captured"],
    config: R2DConfig,
    checkpoint_sha256: str,
) -> dict[tuple[object, ...], dict[str, object]]:
    if not isinstance(payload, dict) or set(payload) != {
        "route",
        "source_manifest_sha256",
        "source_manifest_file_sha256",
        "semantic_manifests",
        "summary_file_sha256",
        "source_files",
        "validated_artifacts",
        "gold_artifacts",
        "summaries",
        "checkpoint_sha256",
    }:
        raise ValueError(f"{route} provenance has an unexpected shape")
    expected_cutoffs = tuple(
        _cutoff_text(item.planning_cutoff) for item in config.snapshot_contract.planning_cutoffs
    )
    if payload.get("route") != route or payload.get("checkpoint_sha256") != (
        None if route == "direct" else checkpoint_sha256
    ):
        raise ValueError(f"{route} provenance route/checkpoint binding differs")
    source_files = payload.get("source_files")
    summaries = payload.get("summaries")
    validated = payload.get("validated_artifacts")
    gold = payload.get("gold_artifacts")
    if not all(isinstance(value, list) for value in (source_files, summaries, validated, gold)):
        raise ValueError(f"{route} provenance evidence collections must be arrays")
    source_files = cast(list[object], source_files)
    summaries = cast(list[object], summaries)
    validated = cast(list[object], validated)
    gold = cast(list[object], gold)
    if (
        len(source_files) != len(_EXPECTED_SOURCE_FILES)
        or len(summaries) != len(expected_cutoffs)
        or len(validated) != len(expected_cutoffs) * len(_EXPECTED_VALIDATED_DATASETS)
        or len(gold) != len(expected_cutoffs) * len(_EXPECTED_GOLD_DATASETS)
    ):
        raise ValueError(f"{route} provenance evidence topology differs")

    expectations: dict[tuple[object, ...], dict[str, object]] = {
        ("source_manifest", route): {
            "content_sha256": payload.get("source_manifest_sha256"),
            "file_sha256": payload.get("source_manifest_file_sha256"),
        }
    }
    expected_source_files = dict(_EXPECTED_SOURCE_FILES)
    for item in source_files:
        if not isinstance(item, dict) or set(item) != {
            "dataset",
            "file_name",
            "record_count",
            "file_sha256",
        }:
            raise ValueError(f"{route} source provenance shape differs")
        dataset = cast(str, item.get("dataset"))
        if item.get("file_name") != expected_source_files.get(dataset):
            raise ValueError(f"{route} source provenance dataset/file topology differs")
        expectations[("source_file", route, dataset)] = {
            "file_sha256": item.get("file_sha256"),
            "content_sha256": item.get("file_sha256"),
            "row_count": item.get("record_count"),
        }

    summary_pairs: list[tuple[object, object]] = []
    summary_file_pairs: list[tuple[object, object]] = []
    for item in summaries:
        if not isinstance(item, dict) or set(item) != {
            "cutoff",
            "semantic_manifest_sha256",
            "file_sha256",
        }:
            raise ValueError(f"{route} summary provenance shape differs")
        cutoff = item.get("cutoff")
        summary_pairs.append((cutoff, item.get("semantic_manifest_sha256")))
        summary_file_pairs.append((cutoff, item.get("file_sha256")))
        expectations[("r2a_summary", route, cutoff)] = {
            "semantic_manifest_sha256": item.get("semantic_manifest_sha256"),
            "file_sha256": item.get("file_sha256"),
        }
    if (
        tuple(item[0] for item in summary_pairs) != expected_cutoffs
        or payload.get("semantic_manifests") != [list(item) for item in summary_pairs]
        or payload.get("summary_file_sha256") != [list(item) for item in summary_file_pairs]
    ):
        raise ValueError(f"{route} summary provenance bindings differ")

    for stage, values, expected_datasets in (
        ("validated", validated, _EXPECTED_VALIDATED_DATASETS),
        ("gold", gold, _EXPECTED_GOLD_DATASETS),
    ):
        for item in values:
            if not isinstance(item, dict) or set(item) != {
                "cutoff",
                "stage",
                "dataset",
                "row_count",
                "content_sha256",
                "schema_sha256",
                "file_sha256",
            }:
                raise ValueError(f"{route} {stage} provenance shape differs")
            cutoff = item.get("cutoff")
            artifact_dataset = item.get("dataset")
            if (
                item.get("stage") != stage
                or cutoff not in expected_cutoffs
                or artifact_dataset not in expected_datasets
            ):
                raise ValueError(f"{route} {stage} provenance topology differs")
            expectations[("parquet", route, cutoff, stage, artifact_dataset)] = {
                "row_count": item.get("row_count"),
                "content_sha256": item.get("content_sha256"),
                "schema_sha256": item.get("schema_sha256"),
                "file_sha256": item.get("file_sha256"),
            }
    return expectations


def _verify_artifact_index(
    index: R2DArtifactIndex,
    root: Path,
    config: R2DConfig,
    *,
    direct_provenance: object,
    captured_provenance: object,
    manifest_content_sha256: dict[str, str],
    checkpoint_sha256: str,
) -> bool:
    if (
        index.contract_version != "r2d.a1.artifact-index.v1"
        or index.release != "R2D-A1"
        or index.definition != _ARTIFACT_SET_DEFINITION
        or canonical_json_sha256(_artifact_index_payload(index)) != index.artifact_set_sha256
    ):
        return False
    try:
        identities = tuple(_artifact_entry_identity(entry) for entry in index.entries)
        required_identities = _required_artifact_index_identities(config)
        if (
            tuple(sorted(identities, key=repr)) != required_identities
            or len(identities) != len(set(identities))
            or len({entry.get("path") for entry in index.entries}) != len(index.entries)
            or index.entries
            != tuple(
                sorted(
                    index.entries,
                    key=lambda item: (str(item["path"]), str(item["kind"])),
                )
            )
            or set(manifest_content_sha256) != set(_EXPECTED_PRECHECKPOINT_MANIFESTS)
            or any(
                not _artifact_entry_path_matches_topology(entry, config) for entry in index.entries
            )
        ):
            return False
        expectations = _provenance_index_expectations(
            direct_provenance,
            route="direct",
            config=config,
            checkpoint_sha256=checkpoint_sha256,
        )
        expectations.update(
            _provenance_index_expectations(
                captured_provenance,
                route="captured",
                config=config,
                checkpoint_sha256=checkpoint_sha256,
            )
        )
        for dataset, content_sha256 in manifest_content_sha256.items():
            expectations[("r2d_manifest", dataset)] = {
                "content_sha256": content_sha256,
                "schema_sha256": canonical_json_sha256(
                    {"contract": _R2D_MANIFEST_CONTRACTS[dataset]}
                ),
                "row_count": 1,
            }
        if set(expectations) != set(required_identities):
            return False

        entries_by_identity = {_artifact_entry_identity(entry): entry for entry in index.entries}
        for entry in index.entries:
            expected = expectations[_artifact_entry_identity(entry)]
            if any(entry.get(name) != value for name, value in expected.items()):
                return False
            path = _safe_bundle_artifact_path(root, cast(str, entry["path"]))
            if file_sha256(path) != entry["file_sha256"]:
                return False
            kind = entry["kind"]
            common = {"kind", "path", "file_sha256"}
            expected_keys = {
                "source_manifest": common | {"route", "content_sha256"},
                "source_file": common | {"route", "dataset", "row_count", "content_sha256"},
                "r2d_manifest": common
                | {"dataset", "content_sha256", "schema_sha256", "row_count"},
                "r2a_summary": common | {"route", "cutoff", "semantic_manifest_sha256"},
                "parquet": common
                | {
                    "route",
                    "cutoff",
                    "stage",
                    "dataset",
                    "row_count",
                    "content_sha256",
                    "schema_sha256",
                },
            }.get(cast(str, kind))
            if expected_keys is None or set(entry) != expected_keys:
                return False
            if kind in {"source_manifest", "r2d_manifest"}:
                parsed = json.loads(path.read_bytes())
                if canonical_json_sha256(parsed) != entry["content_sha256"]:
                    return False
                if kind == "source_manifest":
                    manifest = R2SourceManifest.model_validate_json(path.read_bytes())
                    route = cast(str, entry["route"])
                    source_by_dataset = {source.dataset: source for source in manifest.sources}
                    if set(source_by_dataset) != {item[0] for item in _EXPECTED_SOURCE_FILES}:
                        return False
                    for dataset, file_name in _EXPECTED_SOURCE_FILES:
                        source = source_by_dataset[dataset]
                        expected_source = expectations[("source_file", route, dataset)]
                        if (
                            source.file_name != file_name
                            or source.content_sha256 != expected_source["content_sha256"]
                            or source.record_count != expected_source["row_count"]
                        ):
                            return False
                    if route == "direct" and (
                        entry["content_sha256"] != config.source_contract.source_manifest_sha256
                        or entry["file_sha256"]
                        != config.source_contract.source_manifest_file_sha256
                    ):
                        return False
            elif kind == "source_file":
                if entry["content_sha256"] != entry["file_sha256"]:
                    return False
                if _source_record_count(path) != entry["row_count"]:
                    return False
            elif kind == "r2a_summary":
                route = cast(str, entry["route"])
                source_expectation = expectations[("source_manifest", route)]
                cutoff = cast(str, entry["cutoff"])
                parsed = _verified_r2a_summary_payload(
                    path,
                    cutoff=cutoff,
                    semantic_sha256=cast(str, entry["semantic_manifest_sha256"]),
                    source_manifest_sha256=source_expectation["content_sha256"],
                    source_manifest_file_sha256=source_expectation["file_sha256"],
                )
                for summary_field, stage, expected_datasets in (
                    ("validated", "validated", _EXPECTED_VALIDATED_DATASETS),
                    ("gold", "gold", _EXPECTED_GOLD_DATASETS),
                ):
                    summary_artifacts = parsed.get(summary_field)
                    if not isinstance(summary_artifacts, list):
                        return False
                    expected_summary_artifacts: list[dict[str, object]] = []
                    for dataset in expected_datasets:
                        identity = ("parquet", route, cutoff, stage, dataset)
                        provenance_artifact = expectations.get(identity)
                        indexed_artifact = entries_by_identity.get(identity)
                        if provenance_artifact is None or indexed_artifact is None:
                            return False
                        expected_summary_artifact = {
                            "content_sha256": provenance_artifact.get("content_sha256"),
                            "dataset": dataset,
                            "row_count": provenance_artifact.get("row_count"),
                            "schema_sha256": provenance_artifact.get("schema_sha256"),
                        }
                        indexed_summary_artifact = {
                            "content_sha256": indexed_artifact.get("content_sha256"),
                            "dataset": indexed_artifact.get("dataset"),
                            "row_count": indexed_artifact.get("row_count"),
                            "schema_sha256": indexed_artifact.get("schema_sha256"),
                        }
                        if indexed_summary_artifact != expected_summary_artifact:
                            return False
                        expected_summary_artifacts.append(expected_summary_artifact)
                    if summary_artifacts != expected_summary_artifacts:
                        return False

            elif kind == "parquet":
                table = pq.read_table(path)
                if (
                    canonical_rows_sha256(table.to_pylist()) != entry["content_sha256"]
                    or canonical_schema_sha256(table.schema) != entry["schema_sha256"]
                    or table.num_rows != entry["row_count"]
                ):
                    return False
            else:
                return False
    except (KeyError, OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return False
    return True


def _artifact_index_entry(
    index: R2DArtifactIndex,
    identity: tuple[object, ...],
) -> dict[str, object]:
    matches = tuple(entry for entry in index.entries if _artifact_entry_identity(entry) == identity)
    if len(matches) != 1:
        raise ValueError(f"artifact index identity is not unique: {identity!r}")
    return matches[0]


def _indexed_artifact_path(
    index: R2DArtifactIndex,
    root: Path,
    identity: tuple[object, ...],
) -> tuple[dict[str, object], Path]:
    entry = _artifact_index_entry(index, identity)
    return entry, _safe_bundle_artifact_path(root, cast(str, entry["path"]))


def _safe_bundle_directory_path(root: Path, relative_text: str) -> Path:
    relative = PurePosixPath(relative_text)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError(f"portable bundle contains unsafe directory path: {relative_text!r}")
    if any("\\" in segment or ":" in segment for segment in relative.parts):
        raise ValueError(f"portable bundle directory is not relative POSIX: {relative_text!r}")
    root = root.absolute()
    candidate = root.joinpath(*relative.parts)
    current = root
    for segment in relative.parts:
        current /= segment
        if current.is_symlink() or bool(getattr(current, "is_junction", lambda: False)()):
            raise ValueError(f"portable bundle directory path contains a link: {current}")
    if not candidate.is_dir():
        raise ValueError(f"portable bundle directory is missing: {candidate}")
    return candidate


def _safe_bundle_single_file(root: Path, directory_text: str, suffix: str) -> Path:
    directory = _safe_bundle_directory_path(root, directory_text)
    entries = tuple(directory.iterdir())
    if len(entries) != 1 or entries[0].suffix != suffix:
        raise ValueError(
            f"portable evidence directory must contain exactly one {suffix} file: {directory}"
        )
    relative = entries[0].absolute().relative_to(root.absolute()).as_posix()
    return _safe_bundle_artifact_path(root, relative)


def _safe_bundle_files_with_suffix(
    root: Path,
    directory_text: str,
    suffix: str,
) -> tuple[str, ...]:
    """Return an exact relative file topology without traversing link-like entries."""

    directory = _safe_bundle_directory_path(root, directory_text)
    pending = [directory]
    observed: list[str] = []
    while pending:
        current = pending.pop()
        for entry in current.iterdir():
            stat_result = entry.lstat()
            is_reparse_point = bool(
                getattr(stat_result, "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
            )
            if (
                entry.is_symlink()
                or bool(getattr(entry, "is_junction", lambda: False)())
                or is_reparse_point
            ):
                raise ValueError(f"portable evidence tree contains a link: {entry}")
            if entry.is_dir():
                pending.append(entry)
            elif entry.is_file() and entry.suffix == suffix:
                observed.append(entry.absolute().relative_to(root.absolute()).as_posix())
    return tuple(sorted(observed))


def _verified_content_addressed_parquet(path: Path) -> pa.Table:
    table = pq.read_table(path)
    if (
        path.suffix != ".parquet"
        or path.stem != canonical_rows_sha256(table.to_pylist())
        or not path.stem
    ):
        raise ValueError(f"retained Parquet is not content-addressed by canonical rows: {path}")
    return table


def _verified_r2a_summary_payload(
    path: Path,
    *,
    cutoff: str,
    semantic_sha256: str,
    source_manifest_sha256: object,
    source_manifest_file_sha256: object,
) -> dict[str, object]:
    parsed = json.loads(path.read_bytes())
    expected_keys = {
        "artifact_contract_version",
        "build_input_sha256",
        "dbt",
        "gold",
        "pipeline_code_sha256",
        "planning_cutoff",
        "quality_counts",
        "release",
        "runtime",
        "semantic_manifest_sha256",
        "source_manifest_file_sha256",
        "source_manifest_sha256",
        "status",
        "validated",
        "watermarks",
    }
    if not isinstance(parsed, dict) or set(parsed) != expected_keys:
        raise ValueError(f"R2A summary has an unexpected shape: {path}")
    dbt_payload = parsed.get("dbt")
    if (
        not isinstance(dbt_payload, dict)
        or set(dbt_payload) != {"code_sha256", "detail", "engine", "status"}
        or dbt_payload.get("code_sha256") != r2_dbt_code_sha256(_PROJECT_ROOT)
        or dbt_payload.get("engine") != "dbt-duckdb"
        or dbt_payload.get("status") != "PASS"
        or not isinstance(dbt_payload.get("detail"), str)
        or parsed.get("pipeline_code_sha256") != r2_pipeline_code_sha256(_PROJECT_ROOT)
    ):
        raise ValueError(f"R2A summary code identity differs from checked-in R2A: {path}")
    semantic_payload = dict(parsed)
    stored_semantic_sha256 = semantic_payload.pop("semantic_manifest_sha256", None)
    semantic_payload.pop("status", None)
    semantic_payload["dbt"] = {
        name: value for name, value in dbt_payload.items() if name != "detail"
    }
    if (
        parsed.get("release") != "R2A"
        or parsed.get("status") != "PASS"
        or parsed.get("planning_cutoff") != cutoff
        or parsed.get("source_manifest_sha256") != source_manifest_sha256
        or parsed.get("source_manifest_file_sha256") != source_manifest_file_sha256
        or stored_semantic_sha256 != semantic_sha256
        or path.stem != semantic_sha256
        or canonical_rows_sha256([{"manifest": semantic_payload}]) != semantic_sha256
    ):
        raise ValueError(f"R2A summary semantic identity differs: {path}")
    return cast(dict[str, object], parsed)


def _strict_summary_watermarks(value: object) -> list[list[object]]:
    if not isinstance(value, dict) or any(not isinstance(name, str) for name in value):
        raise ValueError("R2A summary watermarks must be a string-keyed object")
    pairs: list[list[object]] = []
    for name, watermark in sorted(value.items()):
        if watermark is not None and not isinstance(watermark, str):
            raise ValueError("R2A summary watermark must be a string or null")
        pairs.append([name, watermark])
    return pairs


def _strict_summary_quality_counts(value: object) -> list[tuple[str, int | None]]:
    if not isinstance(value, dict) or any(not isinstance(name, str) for name in value):
        raise ValueError("R2A summary quality_counts must be a string-keyed object")
    pairs: list[tuple[str, int | None]] = []
    for name, count in sorted(value.items()):
        if count is not None and (not isinstance(count, int) or isinstance(count, bool)):
            raise ValueError("R2A summary quality count must be an integer or null")
        pairs.append((str(name), count))
    return pairs


def _normalized_business_from_route(
    *,
    route: Literal["direct", "captured"],
    transcript: R2DCaptureTranscript,
    index: R2DArtifactIndex,
    root: Path,
    config: R2DConfig,
) -> dict[str, object]:
    admitted_values = tuple(
        value
        for record in transcript.records
        if record.decision.disposition == "admitted"
        for value in (record.record.value_bytes,)
        if value is not None
    )
    if (
        not admitted_values
        or raw_value_multiset_sha256(admitted_values)
        != config.source_contract.raw_value_multiset_sha256
    ):
        raise ValueError("admitted transcript raw-value multiset differs from frozen source")
    raw_counts = raw_value_sha256_counts(admitted_values)
    _, route_inventory_path = _indexed_artifact_path(
        index,
        root,
        ("source_file", route, _INVENTORY_DATASET),
    )
    if raw_counts != raw_value_sha256_counts(
        split_lf_jsonl_values(route_inventory_path.read_bytes())
    ):
        raise ValueError(f"{route} source inventory differs from admitted transcript multiset")

    source_manifest = _artifact_index_entry(index, ("source_manifest", route))
    cutoffs: list[dict[str, object]] = []
    for cutoff_contract in config.snapshot_contract.planning_cutoffs:
        cutoff = _cutoff_text(cutoff_contract.planning_cutoff)
        summary_entry, summary_path = _indexed_artifact_path(
            index,
            root,
            ("r2a_summary", route, cutoff),
        )
        summary = _verified_r2a_summary_payload(
            summary_path,
            cutoff=cutoff,
            semantic_sha256=cast(str, summary_entry["semantic_manifest_sha256"]),
            source_manifest_sha256=source_manifest["content_sha256"],
            source_manifest_file_sha256=source_manifest["file_sha256"],
        )
        expected_gold = [
            {
                "content_sha256": artifact["content_sha256"],
                "dataset": dataset,
                "row_count": artifact["row_count"],
                "schema_sha256": artifact["schema_sha256"],
            }
            for dataset in _EXPECTED_GOLD_DATASETS
            for artifact in (
                _artifact_index_entry(index, ("parquet", route, cutoff, "gold", dataset)),
            )
        ]
        if canonical_json_bytes(summary.get("gold")) != canonical_json_bytes(expected_gold):
            raise ValueError(f"{route} R2A summary Gold identities differ at {cutoff}")

        quarantine_entry, quarantine_path = _indexed_artifact_path(
            index,
            root,
            ("parquet", route, cutoff, "validated", "quarantine.r2"),
        )
        quarantine = _verified_content_addressed_parquet(quarantine_path)
        expected_quarantine_columns = (
            "dataset",
            "source_file",
            "source_row",
            "reason_code",
            "detail",
            "record_reference",
        )
        quarantine_rows = quarantine.to_pylist()
        if (
            tuple(quarantine.schema.names) != expected_quarantine_columns
            or quarantine.num_rows != quarantine_entry["row_count"]
            or any(set(row) != set(expected_quarantine_columns) for row in quarantine_rows)
            or any(not isinstance(row["reason_code"], str) for row in quarantine_rows)
        ):
            raise ValueError(f"{route} quarantine evidence has an unexpected schema at {cutoff}")
        reason_counts = Counter(cast(str, row["reason_code"]) for row in quarantine_rows)
        quality_counts = _strict_summary_quality_counts(summary.get("quality_counts"))
        quality_by_name = dict(quality_counts)
        if quality_by_name.get("quarantined_records") != quarantine.num_rows:
            raise ValueError(f"{route} summary quarantine count differs at {cutoff}")
        cutoffs.append(
            {
                "planning_cutoff": cutoff,
                "watermarks": _strict_summary_watermarks(summary.get("watermarks")),
                "quality_counts": quality_counts,
                "quarantine_reason_counts": [
                    [reason, count] for reason, count in sorted(reason_counts.items())
                ],
                "gold": expected_gold,
            }
        )
    return {
        "raw_value_sha256_counts": [list(item) for item in raw_counts],
        "cutoffs": cutoffs,
    }


def _verified_normalized_business(
    payload: object,
    *,
    transcript: R2DCaptureTranscript,
    index: R2DArtifactIndex,
    root: Path,
    config: R2DConfig,
) -> dict[str, object]:
    direct = _normalized_business_from_route(
        route="direct",
        transcript=transcript,
        index=index,
        root=root,
        config=config,
    )
    captured = _normalized_business_from_route(
        route="captured",
        transcript=transcript,
        index=index,
        root=root,
        config=config,
    )
    if canonical_json_bytes(direct) != canonical_json_bytes(captured) or canonical_json_bytes(
        payload
    ) != canonical_json_bytes(direct):
        raise ValueError(
            "embedded normalized business is not the exact direct/captured evidence projection"
        )
    return direct


def _verified_captured_source_derivation(
    *,
    transcript: R2DCaptureTranscript,
    index: R2DArtifactIndex,
    root: Path,
    config: R2DConfig,
) -> str:
    reconstructed_jsonl = reconstruct_jsonl_bytes(transcript, config)
    captured_inventory_entry, captured_inventory_path = _indexed_artifact_path(
        index,
        root,
        ("source_file", "captured", _INVENTORY_DATASET),
    )
    captured_inventory = captured_inventory_path.read_bytes()
    if captured_inventory != reconstructed_jsonl:
        raise ValueError("captured inventory is not the exact ordered transcript reconstruction")
    captured_values = split_lf_jsonl_values(captured_inventory)
    captured_inventory_sha256 = hashlib.sha256(captured_inventory).hexdigest()
    if (
        captured_inventory_entry["content_sha256"] != captured_inventory_sha256
        or captured_inventory_entry["file_sha256"] != captured_inventory_sha256
        or captured_inventory_entry["row_count"] != len(captured_values)
    ):
        raise ValueError("captured inventory identity differs from reconstructed bytes")

    _, direct_manifest_path = _indexed_artifact_path(
        index,
        root,
        ("source_manifest", "direct"),
    )
    captured_manifest_entry, captured_manifest_path = _indexed_artifact_path(
        index,
        root,
        ("source_manifest", "captured"),
    )
    direct_manifest = R2SourceManifest.model_validate_json(direct_manifest_path.read_bytes())
    captured_manifest = R2SourceManifest.model_validate_json(captured_manifest_path.read_bytes())
    expected_sources = tuple(
        source.model_copy(
            update={
                "content_sha256": captured_inventory_sha256,
                "record_count": len(captured_values),
            }
        )
        if source.dataset == _INVENTORY_DATASET
        else source
        for source in direct_manifest.sources
    )
    expected_manifest = direct_manifest.model_copy(update={"sources": expected_sources})
    expected_manifest_bytes = (
        json.dumps(
            expected_manifest.model_dump(mode="json"),
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")
    if (
        captured_manifest != expected_manifest
        or captured_manifest_path.read_bytes() != expected_manifest_bytes
        or captured_manifest_entry["content_sha256"]
        != canonical_json_sha256(expected_manifest.model_dump(mode="json"))
        or captured_manifest_entry["file_sha256"] != file_sha256(captured_manifest_path)
    ):
        raise ValueError(
            "captured source manifest is not the direct manifest rebound only to "
            "the exact reconstructed inventory"
        )

    for dataset, _ in _EXPECTED_SOURCE_FILES:
        if dataset == _INVENTORY_DATASET:
            continue
        direct_entry, direct_path = _indexed_artifact_path(
            index,
            root,
            ("source_file", "direct", dataset),
        )
        captured_entry, captured_path = _indexed_artifact_path(
            index,
            root,
            ("source_file", "captured", dataset),
        )
        if (
            captured_path.read_bytes() != direct_path.read_bytes()
            or captured_entry["content_sha256"] != direct_entry["content_sha256"]
            or captured_entry["file_sha256"] != direct_entry["file_sha256"]
            or captured_entry["row_count"] != direct_entry["row_count"]
        ):
            raise ValueError(f"captured static source differs from frozen direct source: {dataset}")
    return captured_inventory_sha256


def _expected_special_source_manifest(
    direct_manifest: R2SourceManifest,
    inventory_jsonl: bytes,
    *,
    accepted_count: int,
    quarantine_count: int,
) -> R2SourceManifest:
    values = split_lf_jsonl_values(inventory_jsonl)
    inventory_sha256 = hashlib.sha256(inventory_jsonl).hexdigest()
    sources = tuple(
        source.model_copy(
            update={
                "content_sha256": inventory_sha256,
                "record_count": len(values),
            }
        )
        if source.dataset == _INVENTORY_DATASET
        else source
        for source in direct_manifest.sources
    )
    controls = direct_manifest.expected_controls.model_copy(
        update={
            "accepted_inventory_adjustments": accepted_count,
            "quarantine_rows": quarantine_count,
        }
    )
    return direct_manifest.model_copy(update={"sources": sources, "expected_controls": controls})


def _verified_retained_special_source(
    *,
    root: Path,
    relative_root: str,
    expected_jsonl: bytes,
    expected_manifest: R2SourceManifest,
    index: R2DArtifactIndex,
) -> tuple[R2SourceManifest, str]:
    inventory_path = _safe_bundle_artifact_path(
        root,
        f"{relative_root}/{_INVENTORY_FILE}",
    )
    if inventory_path.read_bytes() != expected_jsonl:
        raise ValueError(f"retained correction inventory differs: {relative_root}")
    manifest_path = _safe_bundle_artifact_path(
        root,
        f"{relative_root}/source-manifest.json",
    )
    manifest = R2SourceManifest.model_validate_json(manifest_path.read_bytes())
    if manifest != expected_manifest:
        raise ValueError(f"retained correction source manifest differs: {relative_root}")
    for dataset, file_name in _EXPECTED_SOURCE_FILES:
        if dataset == _INVENTORY_DATASET:
            continue
        _, governed_path = _indexed_artifact_path(
            index,
            root,
            ("source_file", "direct", dataset),
        )
        retained_path = _safe_bundle_artifact_path(root, f"{relative_root}/{file_name}")
        if retained_path.read_bytes() != governed_path.read_bytes():
            raise ValueError(f"retained correction static source differs: {file_name}")
    return manifest, file_sha256(manifest_path)


def _strict_summary_artifact_identities(
    value: object,
    expected_datasets: tuple[str, ...],
) -> dict[str, dict[str, object]]:
    """Return an exact, typed R2A summary artifact section."""

    if not isinstance(value, list) or len(value) != len(expected_datasets):
        raise ValueError("R2A summary artifact identities have an unexpected shape")
    identities: dict[str, dict[str, object]] = {}
    for expected_dataset, item in zip(expected_datasets, value, strict=True):
        if (
            not isinstance(item, dict)
            or set(item) != {"content_sha256", "dataset", "row_count", "schema_sha256"}
            or item.get("dataset") != expected_dataset
            or not isinstance(item.get("row_count"), int)
            or isinstance(item.get("row_count"), bool)
            or cast(int, item["row_count"]) < 0
        ):
            raise ValueError("R2A summary artifact identities differ from expected topology")
        for field in ("content_sha256", "schema_sha256"):
            digest = item.get(field)
            if (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError(f"R2A summary artifact {field} is not a SHA-256")
        identities[expected_dataset] = cast(dict[str, object], item)
    return identities


def _verified_gap_warehouse_view_sql(
    warehouse_path: Path,
    *,
    cutoff: str,
    validated: dict[str, dict[str, object]],
) -> tuple[str, str, str]:
    """Verify dbt topology without resolving the warehouse's absolute source paths."""

    staging_datasets = tuple(
        dataset for dataset in _EXPECTED_VALIDATED_DATASETS if dataset.startswith("validated.")
    )
    staging_views = {
        f"stg_r2_{dataset.removeprefix('validated.')}": dataset for dataset in staging_datasets
    }
    intermediate_views = {
        "int_r2_deferred_inventory_adjustments",
        "int_r2_effective_inventory_adjustments",
        "int_r2_superseded_inventory_adjustments",
    }
    expected_relations = tuple(
        sorted(
            [("main", table_name, "BASE TABLE") for table_name, _ in R2_GOLD_TABLES.values()]
            + [("main", view_name, "VIEW") for view_name in staging_views]
            + [("main", view_name, "VIEW") for view_name in intermediate_views]
        )
    )
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        observed_relations = tuple(
            (cast(str, row[0]), cast(str, row[1]), cast(str, row[2]))
            for row in connection.execute(
                "select table_schema, table_name, table_type "
                "from information_schema.tables "
                "where table_schema not in ('information_schema', 'pg_catalog') "
                "order by table_schema, table_name"
            ).fetchall()
        )
        if observed_relations != expected_relations:
            raise ValueError("retained gap warehouse relation topology differs")
        view_rows = connection.execute(
            "select schema_name, view_name, sql from duckdb_views() "
            "where internal = false order by schema_name, view_name"
        ).fetchall()

    expected_view_names = set(staging_views) | intermediate_views
    if (
        len(view_rows) != len(expected_view_names)
        or any(row[0] != "main" or not isinstance(row[1], str) for row in view_rows)
        or {cast(str, row[1]) for row in view_rows} != expected_view_names
        or any(not isinstance(row[2], str) for row in view_rows)
    ):
        raise ValueError("retained gap warehouse view topology differs")
    view_sql = {cast(str, row[1]): cast(str, row[2]) for row in view_rows}
    source_paths: dict[str, str] = {}
    for view_name, dataset in staging_views.items():
        sql = view_sql[view_name]
        matches = re.findall(r"read_parquet\('((?:''|[^'])*)'\)", sql)
        if len(matches) != 1:
            raise ValueError(f"retained gap warehouse source binding differs: {view_name}")
        source_path = matches[0].replace("''", "'").replace("\\", "/")
        parsed_source_path = PurePosixPath(source_path)
        is_drive_absolute = re.match(r"^[A-Za-z]:/", source_path) is not None
        if (
            not (parsed_source_path.is_absolute() or is_drive_absolute)
            or ".." in parsed_source_path.parts
        ):
            raise ValueError(f"retained gap warehouse source path is not absolute: {view_name}")
        artifact = validated[dataset]
        expected_suffix = (
            f"/validated/{dataset.removeprefix('validated.')}/{artifact['content_sha256']}.parquet"
        )
        if not source_path.endswith(expected_suffix):
            raise ValueError(f"retained gap warehouse source binding differs: {view_name}")
        source_paths[view_name] = matches[0]

    rendered_cutoff = datetime.fromisoformat(cutoff.replace("Z", "+00:00")).isoformat()
    required_fragments = {
        "int_r2_deferred_inventory_adjustments": (
            "stg_r2_inventory_adjustments",
            "row_number()",
            "future_business_and_arrival",
            "future_business_time",
            "late_arrival",
            "incomplete_visible_revision_chain",
            rendered_cutoff,
        ),
        "int_r2_effective_inventory_adjustments": (
            "stg_r2_inventory_adjustments",
            "row_number()",
            rendered_cutoff,
        ),
        "int_r2_superseded_inventory_adjustments": (
            "stg_r2_inventory_adjustments",
            "int_r2_effective_inventory_adjustments",
            "row_number()",
            rendered_cutoff,
        ),
    }
    for view_name, fragments in required_fragments.items():
        sql = view_sql[view_name].lower()
        if "read_parquet(" in sql or any(fragment.lower() not in sql for fragment in fragments):
            raise ValueError(f"retained gap warehouse intermediate view differs: {view_name}")

    catalog = f'"{warehouse_path.stem}".main.'
    required_catalog_bindings = {
        "int_r2_deferred_inventory_adjustments": ("stg_r2_inventory_adjustments",),
        "int_r2_effective_inventory_adjustments": ("stg_r2_inventory_adjustments",),
        "int_r2_superseded_inventory_adjustments": (
            "stg_r2_inventory_adjustments",
            "int_r2_effective_inventory_adjustments",
        ),
    }
    for view_name, relation_names in required_catalog_bindings.items():
        sql = view_sql[view_name]
        if any(sql.count(f"{catalog}{relation_name}") != 1 for relation_name in relation_names):
            raise ValueError(f"retained gap warehouse catalog binding differs: {view_name}")

    inventory_view = "stg_r2_inventory_adjustments"
    return (
        view_sql[inventory_view],
        source_paths[inventory_view],
        view_sql["int_r2_deferred_inventory_adjustments"],
    )


def _derived_gap_deferred_rows(
    inventory_path: Path,
    *,
    staging_sql: str,
    stored_source_path: str,
    deferred_sql: str,
    catalog_name: str,
) -> tuple[tuple[str, int, str, str], ...]:
    """Replay the retained dbt views against the verified relocated Parquet."""

    escaped_inventory_path = inventory_path.as_posix().replace("'", "''")
    stored_binding = f"read_parquet('{stored_source_path}')"
    if staging_sql.count(stored_binding) != 1:
        raise ValueError("retained gap inventory view source binding is ambiguous")
    rebound_staging_sql = staging_sql.replace(
        stored_binding,
        f"read_parquet('{escaped_inventory_path}')",
        1,
    )
    catalog_identifier = f'"{catalog_name.replace(chr(34), chr(34) * 2)}"'
    with duckdb.connect(":memory:") as connection:
        connection.execute(f"attach ':memory:' as {catalog_identifier}")
        connection.execute(f"use {catalog_identifier}")
        connection.execute(rebound_staging_sql)
        connection.execute(deferred_sql)
        rows = connection.execute(
            "select event_id, revision, idempotency_key, deferred_reason "
            "from int_r2_deferred_inventory_adjustments order by revision"
        ).fetchall()
    return tuple(
        (cast(str, row[0]), cast(int, row[1]), cast(str, row[2]), cast(str, row[3])) for row in rows
    )


def _verified_correction_semantics(
    payload: object,
    *,
    index: R2DArtifactIndex,
    root: Path,
    config: R2DConfig,
) -> dict[str, object]:
    _, direct_manifest_path = _indexed_artifact_path(
        index,
        root,
        ("source_manifest", "direct"),
    )
    direct_manifest = R2SourceManifest.model_validate_json(direct_manifest_path.read_bytes())
    _, direct_inventory_path = _indexed_artifact_path(
        index,
        root,
        ("source_file", "direct", _INVENTORY_DATASET),
    )
    template = json.loads(split_lf_jsonl_values(direct_inventory_path.read_bytes())[0])
    if not isinstance(template, dict):
        raise ValueError("frozen correction template is not an event object")
    revision_two = _special_event(
        template,
        revision=2,
        event_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        idempotency_key="KEY-R2D-GAP-2",
        ingested_at="2026-06-08T10:00:00Z",
    )
    revision_one = _special_event(
        template,
        revision=1,
        event_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        idempotency_key="KEY-R2D-GAP-1",
        ingested_at="2026-06-15T10:00:00Z",
    )
    rev2_jsonl, rev2_transcript = _captured_special_jsonl((revision_two,), config)
    complete_jsonl, complete_transcript = _captured_special_jsonl(
        (revision_two, revision_one),
        config,
    )
    rev2_expected_manifest = _expected_special_source_manifest(
        direct_manifest,
        rev2_jsonl,
        accepted_count=0,
        quarantine_count=1,
    )
    complete_expected_manifest = _expected_special_source_manifest(
        direct_manifest,
        complete_jsonl,
        accepted_count=2,
        quarantine_count=0,
    )
    rev2_manifest, _ = _verified_retained_special_source(
        root=root,
        relative_root="correction-semantics/rev2-only-source",
        expected_jsonl=rev2_jsonl,
        expected_manifest=rev2_expected_manifest,
        index=index,
    )
    complete_manifest, complete_manifest_file_sha256 = _verified_retained_special_source(
        root=root,
        relative_root="correction-semantics/complete-source",
        expected_jsonl=complete_jsonl,
        expected_manifest=complete_expected_manifest,
        index=index,
    )

    rev2_quarantine_path = _safe_bundle_single_file(
        root,
        "correction-semantics/rev2-only-landing/quarantine/r2",
        ".parquet",
    )
    rev2_quarantine = _verified_content_addressed_parquet(rev2_quarantine_path)
    quarantine_columns = {
        "dataset",
        "source_file",
        "source_row",
        "reason_code",
        "detail",
        "record_reference",
    }
    rev2_rows = rev2_quarantine.to_pylist()
    if (
        len(rev2_rows) != 1
        or set(rev2_rows[0]) != quarantine_columns
        or rev2_rows[0].get("dataset") != _INVENTORY_DATASET
        or rev2_rows[0].get("source_file") != _INVENTORY_FILE
        or rev2_rows[0].get("source_row") != 1
        or rev2_rows[0].get("reason_code") != R2QuarantineReason.CORRECTION_CHAIN_INVALID.value
        or rev2_rows[0].get("record_reference") != revision_two["event_id"]
        or not isinstance(rev2_rows[0].get("detail"), str)
    ):
        raise ValueError("retained revision-two quarantine evidence differs")
    rev2_reason_counts = Counter(cast(str, row["reason_code"]) for row in rev2_rows)

    complete_inventory_path = _safe_bundle_single_file(
        root,
        "correction-semantics/complete-landing/validated/inventory_adjustments",
        ".parquet",
    )
    complete_inventory = _verified_content_addressed_parquet(complete_inventory_path)
    complete_rows = sorted(complete_inventory.to_pylist(), key=lambda row: row["revision"])
    validated_columns = {
        "contract_version",
        "event_id",
        "event_version",
        "source",
        "logical_event_id",
        "revision",
        "idempotency_key",
        "occurred_at",
        "ingested_at",
        "sku_id",
        "location_id",
        "delta_qty",
        "reason_code",
    }
    expected_validated_rows: list[dict[str, object]] = []
    for event in (revision_one, revision_two):
        event_payload = cast(dict[str, object], event["payload"])
        expected_validated_rows.append(
            {
                "contract_version": event["contract_version"],
                "event_id": event["event_id"],
                "event_version": event["event_version"],
                "source": event["source"],
                "logical_event_id": event_payload["logical_event_id"],
                "revision": event_payload["revision"],
                "idempotency_key": event["idempotency_key"],
                "occurred_at": event["occurred_at"],
                "ingested_at": event["ingested_at"],
                "sku_id": event_payload["sku_id"],
                "location_id": event_payload["location_id"],
                "delta_qty": event_payload["delta_qty"],
                "reason_code": event_payload["reason_code"],
            }
        )
    if (
        any(set(row) != validated_columns for row in complete_rows)
        or complete_rows != expected_validated_rows
    ):
        raise ValueError("retained complete correction revisions differ")
    complete_revisions = [cast(int, row["revision"]) for row in complete_rows]

    gap_summary_path = _safe_bundle_single_file(
        root,
        "correction-semantics/gap-rebuild/manifests",
        ".json",
    )
    gap_cutoff = "2026-06-10T12:00:00Z"
    complete_manifest_sha256 = canonical_json_sha256(complete_manifest.model_dump(mode="json"))
    gap_summary = _verified_r2a_summary_payload(
        gap_summary_path,
        cutoff=gap_cutoff,
        semantic_sha256=gap_summary_path.stem,
        source_manifest_sha256=complete_manifest_sha256,
        source_manifest_file_sha256=complete_manifest_file_sha256,
    )
    gap_quality_counts = dict(_strict_summary_quality_counts(gap_summary.get("quality_counts")))
    validated = _strict_summary_artifact_identities(
        gap_summary.get("validated"),
        _EXPECTED_VALIDATED_DATASETS,
    )
    inventory_artifact = validated["validated.inventory_adjustments"]
    gap_lake_directory = (
        f"correction-semantics/gap-rebuild/lake/{complete_manifest_file_sha256[:16]}"
    )
    gap_inventory_relative_path = (
        f"{gap_lake_directory}/validated/inventory_adjustments/"
        f"{inventory_artifact['content_sha256']}.parquet"
    )
    gap_inventory_path = _safe_bundle_artifact_path(root, gap_inventory_relative_path)
    if _safe_bundle_files_with_suffix(
        root,
        f"{gap_lake_directory}/validated/inventory_adjustments",
        ".parquet",
    ) != (gap_inventory_relative_path,):
        raise ValueError("retained gap inventory Parquet topology differs")
    gap_inventory = _verified_content_addressed_parquet(gap_inventory_path)
    if (
        gap_inventory.num_rows != inventory_artifact["row_count"]
        or canonical_rows_sha256(gap_inventory.to_pylist()) != inventory_artifact["content_sha256"]
        or canonical_schema_sha256(gap_inventory.schema) != inventory_artifact["schema_sha256"]
    ):
        raise ValueError("retained gap inventory Parquet differs from R2A summary")
    gap_warehouse_path = _safe_bundle_single_file(
        root,
        "correction-semantics/gap-rebuild/warehouse",
        ".duckdb",
    )
    build_input_sha256 = gap_summary.get("build_input_sha256")
    cutoff_stamp = gap_cutoff.replace("-", "").replace(":", "")
    if (
        not isinstance(build_input_sha256, str)
        or gap_warehouse_path.name != f"{cutoff_stamp}-{build_input_sha256[:16]}.duckdb"
    ):
        raise ValueError("retained gap warehouse filename differs")
    staging_sql, stored_source_path, deferred_sql = _verified_gap_warehouse_view_sql(
        gap_warehouse_path,
        cutoff=gap_cutoff,
        validated=validated,
    )
    deferred_rows = _derived_gap_deferred_rows(
        gap_inventory_path,
        staging_sql=staging_sql,
        stored_source_path=stored_source_path,
        deferred_sql=deferred_sql,
        catalog_name=gap_warehouse_path.stem,
    )
    expected_deferred_rows = (
        (
            revision_one["event_id"],
            1,
            revision_one["idempotency_key"],
            "late_arrival",
        ),
        (
            revision_two["event_id"],
            2,
            revision_two["idempotency_key"],
            "incomplete_visible_revision_chain",
        ),
    )
    if deferred_rows != expected_deferred_rows:
        raise ValueError("retained gap deferred-reason rows differ")
    deferred_reasons = sorted({row[3] for row in deferred_rows})
    gap_chain_deferred_count = sum(
        row[3] == "incomplete_visible_revision_chain" for row in deferred_rows
    )
    if (
        gap_chain_deferred_count != 1
        or gap_quality_counts.get("chain_deferred_inventory_adjustments")
        != gap_chain_deferred_count
        or deferred_reasons != ["incomplete_visible_revision_chain", "late_arrival"]
    ):
        raise ValueError("retained gap correction semantics differ")

    expected = {
        "contract_version": "r2d.a1.correction-semantics.v1",
        "release": "R2D-A1",
        "real_service_claim": False,
        "captured_append_revisions": [2, 1],
        "rev2_only_transcript_sha256": rev2_transcript.content_sha256,
        "rev2_only_lineage_sha256": rev2_transcript.lineage_sha256,
        "rev2_only_source_manifest_sha256": canonical_json_sha256(
            rev2_manifest.model_dump(mode="json")
        ),
        "rev2_only_quarantine_reason_counts": dict(sorted(rev2_reason_counts.items())),
        "complete_transcript_sha256": complete_transcript.content_sha256,
        "complete_lineage_sha256": complete_transcript.lineage_sha256,
        "complete_source_manifest_sha256": complete_manifest_sha256,
        "complete_validated_revisions": complete_revisions,
        "later_complete_replay_corrects": True,
        "gap_planning_cutoff": gap_cutoff,
        "gap_semantic_manifest_sha256": gap_summary_path.stem,
        "gap_summary_file_sha256": file_sha256(gap_summary_path),
        "gap_chain_deferred_count": gap_chain_deferred_count,
        "gap_deferred_reasons": deferred_reasons,
    }
    if canonical_json_bytes(payload) != canonical_json_bytes(expected):
        raise ValueError("embedded correction semantics is not the retained evidence projection")
    return expected


def _verified_portable_recovery(
    payload: object,
    *,
    index: R2DArtifactIndex,
    root: Path,
    config: R2DConfig,
    checkpoint: R2DCaptureCheckpoint,
    normalized_business: dict[str, object],
) -> dict[str, object]:
    if tuple(sorted(R2_GOLD_TABLES)) != _EXPECTED_GOLD_DATASETS:
        raise RuntimeError("R2D recovery Gold datasets differ from authoritative R2A")
    captured_manifest_entry, captured_manifest_path = _indexed_artifact_path(
        index,
        root,
        ("source_manifest", "captured"),
    )
    recovered_manifest_path = _safe_bundle_artifact_path(
        root,
        "portable-recovery/reconstructed-source/source-manifest.json",
    )
    if recovered_manifest_path.read_bytes() != captured_manifest_path.read_bytes():
        raise ValueError("portable recovery source manifest differs from captured source")
    for dataset, file_name in _EXPECTED_SOURCE_FILES:
        _, captured_path = _indexed_artifact_path(
            index,
            root,
            ("source_file", "captured", dataset),
        )
        recovered_path = _safe_bundle_artifact_path(
            root,
            f"portable-recovery/reconstructed-source/{file_name}",
        )
        if recovered_path.read_bytes() != captured_path.read_bytes():
            raise ValueError(f"portable recovery source differs from captured source: {dataset}")

    semantic_manifests: list[list[object]] = []
    gold: list[dict[str, object]] = []
    cutoff_names = {
        _cutoff_text(item.planning_cutoff): item.name
        for item in config.snapshot_contract.planning_cutoffs
    }
    for cutoff_contract in config.snapshot_contract.planning_cutoffs:
        cutoff = _cutoff_text(cutoff_contract.planning_cutoff)
        cutoff_name = cutoff_names[cutoff]
        summary_entry = _artifact_index_entry(
            index,
            ("r2a_summary", "captured", cutoff),
        )
        semantic_sha256 = cast(str, summary_entry["semantic_manifest_sha256"])
        semantic_manifests.append([cutoff, semantic_sha256])
        expected_gold = [
            {
                "content_sha256": artifact["content_sha256"],
                "dataset": dataset,
                "row_count": artifact["row_count"],
                "schema_sha256": artifact["schema_sha256"],
            }
            for dataset in _EXPECTED_GOLD_DATASETS
            for artifact in (
                _artifact_index_entry(
                    index,
                    ("parquet", "captured", cutoff, "gold", dataset),
                ),
            )
        ]
        expected_validated = [
            {
                "content_sha256": artifact["content_sha256"],
                "dataset": dataset,
                "row_count": artifact["row_count"],
                "schema_sha256": artifact["schema_sha256"],
            }
            for dataset in _EXPECTED_VALIDATED_DATASETS
            for artifact in (
                _artifact_index_entry(index, ("parquet", "captured", cutoff, "validated", dataset)),
            )
        ]
        recovery_summary_path = _safe_bundle_artifact_path(
            root,
            f"portable-recovery/derived/{cutoff_name}/manifests/{semantic_sha256}.json",
        )
        recovery_summary = _verified_r2a_summary_payload(
            recovery_summary_path,
            cutoff=cutoff,
            semantic_sha256=semantic_sha256,
            source_manifest_sha256=captured_manifest_entry["content_sha256"],
            source_manifest_file_sha256=captured_manifest_entry["file_sha256"],
        )
        if canonical_json_bytes(recovery_summary.get("validated")) != canonical_json_bytes(
            expected_validated
        ) or canonical_json_bytes(recovery_summary.get("gold")) != canonical_json_bytes(
            expected_gold
        ):
            raise ValueError(f"portable recovery summary artifact identities differ at {cutoff}")

        captured_manifest_file_sha256 = cast(str, captured_manifest_entry["file_sha256"])
        lake_directory = (
            f"portable-recovery/derived/{cutoff_name}/lake/{captured_manifest_file_sha256[:16]}"
        )
        expected_lake_paths: list[str] = []
        for artifact in expected_validated:
            dataset = cast(str, artifact["dataset"])
            relative_path = (
                f"{lake_directory}/{'/'.join(dataset.split('.'))}/"
                f"{artifact['content_sha256']}.parquet"
            )
            expected_lake_paths.append(relative_path)
            recovery_validated = _verified_content_addressed_parquet(
                _safe_bundle_artifact_path(root, relative_path)
            )
            if (
                recovery_validated.num_rows != artifact["row_count"]
                or canonical_rows_sha256(recovery_validated.to_pylist())
                != artifact["content_sha256"]
                or canonical_schema_sha256(recovery_validated.schema) != artifact["schema_sha256"]
            ):
                raise ValueError(f"portable recovery validated artifact differs: {dataset}")
        if _safe_bundle_files_with_suffix(root, lake_directory, ".parquet") != tuple(
            sorted(expected_lake_paths)
        ):
            raise ValueError(f"portable recovery validated Parquet topology differs at {cutoff}")

        recovery_gold_by_dataset: dict[str, pa.Table] = {}
        expected_export_paths: list[str] = []
        for artifact in expected_gold:
            dataset = cast(str, artifact["dataset"])
            relative_path = (
                f"portable-recovery/derived/{cutoff_name}/exports/"
                f"{'/'.join(dataset.split('.'))}/{artifact['content_sha256']}.parquet"
            )
            expected_export_paths.append(relative_path)
            recovery_gold_path = _safe_bundle_artifact_path(root, relative_path)
            recovery_gold = _verified_content_addressed_parquet(recovery_gold_path)
            if (
                recovery_gold.num_rows != artifact["row_count"]
                or canonical_rows_sha256(recovery_gold.to_pylist()) != artifact["content_sha256"]
                or canonical_schema_sha256(recovery_gold.schema) != artifact["schema_sha256"]
            ):
                raise ValueError(f"portable recovery Gold artifact differs: {dataset}")
            recovery_gold_by_dataset[dataset] = recovery_gold
            gold.append({"cutoff": cutoff, **artifact})

        exports_directory = f"portable-recovery/derived/{cutoff_name}/exports"
        if _safe_bundle_files_with_suffix(root, exports_directory, ".parquet") != tuple(
            sorted(expected_export_paths)
        ):
            raise ValueError(f"portable recovery Gold Parquet topology differs at {cutoff}")

        build_input_sha256 = recovery_summary.get("build_input_sha256")
        if (
            not isinstance(build_input_sha256, str)
            or len(build_input_sha256) != 64
            or any(character not in "0123456789abcdef" for character in build_input_sha256)
        ):
            raise ValueError(f"portable recovery build input is not a SHA-256 at {cutoff}")
        warehouse_directory = f"portable-recovery/derived/{cutoff_name}/warehouse"
        warehouse_path = _safe_bundle_single_file(root, warehouse_directory, ".duckdb")
        cutoff_stamp = cutoff.replace("-", "").replace(":", "")
        expected_warehouse_name = f"{cutoff_stamp}-{build_input_sha256[:16]}.duckdb"
        if warehouse_path.name != expected_warehouse_name:
            raise ValueError(f"portable recovery warehouse filename differs at {cutoff}")

        expected_table_identities = tuple(
            sorted(("main", table_name) for table_name, _ in R2_GOLD_TABLES.values())
        )
        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            observed_table_identities = tuple(
                (row[0], row[1])
                for row in connection.execute(
                    "select table_schema, table_name from information_schema.tables "
                    "where table_type = 'BASE TABLE' order by table_schema, table_name"
                ).fetchall()
            )
            if observed_table_identities != expected_table_identities:
                raise ValueError(f"portable recovery warehouse table topology differs at {cutoff}")
            for artifact in expected_gold:
                dataset = cast(str, artifact["dataset"])
                table_name, order_by = R2_GOLD_TABLES[dataset]
                rows, schema = r2_table_rows(
                    connection,
                    table=table_name,
                    order_by=order_by,
                )
                recovery_gold = recovery_gold_by_dataset[dataset]
                if (
                    len(rows) != artifact["row_count"]
                    or canonical_rows_sha256(rows) != artifact["content_sha256"]
                    or canonical_schema_sha256(schema) != artifact["schema_sha256"]
                    or canonical_rows_sha256(rows)
                    != canonical_rows_sha256(recovery_gold.to_pylist())
                    or canonical_schema_sha256(schema)
                    != canonical_schema_sha256(recovery_gold.schema)
                ):
                    raise ValueError(f"portable recovery warehouse Gold differs: {dataset}")

    _, captured_inventory_path = _indexed_artifact_path(
        index,
        root,
        ("source_file", "captured", _INVENTORY_DATASET),
    )
    expected = {
        "contract_version": "r2d.a1.portable-recovery.v1",
        "release": "R2D-A1",
        "operational_recovery_claim": False,
        "retained_evidence": [
            "capture_transcript",
            "complete_capture_checkpoint",
            "producer_ack_ledger",
            "six_static_source_files",
        ],
        "regenerated_derived_state": [
            "inventory_events.jsonl",
            "validated_lake",
            "dbt_duckdb_warehouse",
            "five_gold_products_per_cutoff",
        ],
        "checkpoint_sha256": checkpoint.content_sha256,
        "reconstructed_jsonl_sha256": file_sha256(captured_inventory_path),
        "normalized_business_sha256": canonical_json_sha256(normalized_business),
        "semantic_manifests": semantic_manifests,
        "gold": gold,
        "exact_business_and_provenance_semantics_match": True,
    }
    if canonical_json_bytes(payload) != canonical_json_bytes(expected):
        raise ValueError("embedded portable recovery is not the retained evidence projection")
    return expected


def _run_artifacts_reload(run: R2DA1RunResult) -> bool:
    try:
        transcript = R2DCaptureTranscript.model_validate_json(
            run.transcript_artifact.path.read_bytes()
        )
        checkpoint = R2DCaptureCheckpoint.model_validate_json(
            run.checkpoint_artifact.path.read_bytes()
        )
        producer_ack = R2DProducerAckLedger.model_validate_json(
            run.producer_ack_artifact.path.read_bytes()
        )
        evidence_checkpoint = R2DEvidenceCheckpoint.model_validate_json(
            run.evidence_checkpoint_artifact.path.read_bytes()
        )
        validate_capture_transcript(transcript, run.config)
        physical_records = tuple(item.record for item in transcript.records)
        validate_portable_producer_ack_ledger(
            producer_ack,
            physical_records,
            run.config,
            require_frozen_source=True,
        )
        verify_complete_checkpoint(checkpoint, transcript, run.config, producer_ack)
        root = run.fixture.source_dir.parent
        _verified_captured_source_derivation(
            transcript=transcript,
            index=run.artifact_index,
            root=root,
            config=run.config,
        )
        normalized = _verified_normalized_business(
            _normalized_payload(run.normalized_outcome),
            transcript=transcript,
            index=run.artifact_index,
            root=root,
            config=run.config,
        )
        correction_semantics = _verified_correction_semantics(
            run.correction_semantics,
            index=run.artifact_index,
            root=root,
            config=run.config,
        )
        portable_recovery = _verified_portable_recovery(
            run.recovery_evidence,
            index=run.artifact_index,
            root=root,
            config=run.config,
            checkpoint=checkpoint,
            normalized_business=normalized,
        )
        expected_evidence = run.config.expected_evidence_contract
        if transcript.content_sha256 != expected_evidence.capture_transcript_sha256:
            raise ValueError("capture transcript differs from the frozen expected evidence")
        if (
            canonical_json_sha256(
                _without_file_evidence(_provenance_payload(run.direct_provenance))
            )
            != expected_evidence.direct_provenance_projection_sha256
        ):
            raise ValueError("direct provenance differs from the frozen expected projection")
        if canonical_json_sha256(normalized) != expected_evidence.normalized_business_sha256:
            raise ValueError("normalized business differs from the frozen expected evidence")
        if (
            canonical_json_sha256(_without_file_evidence(correction_semantics))
            != expected_evidence.correction_semantics_projection_sha256
        ):
            raise ValueError(
                "correction semantics differs from the frozen expected evidence projection"
            )
        if (
            canonical_json_sha256(_portable_recovery_projection(portable_recovery))
            != expected_evidence.portable_recovery_projection_sha256
        ):
            raise ValueError("portable recovery differs from the frozen expected evidence")
    except Exception:
        return False
    return (
        transcript == run.capture.transcript
        and checkpoint == run.capture.checkpoint
        and producer_ack == run.capture.producer_ack_ledger
        and evidence_checkpoint == run.evidence_checkpoint
        and _verify_artifact_index(
            run.artifact_index,
            run.fixture.source_dir.parent,
            run.config,
            direct_provenance=_provenance_payload(run.direct_provenance),
            captured_provenance=_provenance_payload(run.captured_provenance),
            manifest_content_sha256={
                item.dataset: item.content_sha256
                for item in run.artifacts
                if item.dataset in _EXPECTED_PRECHECKPOINT_MANIFESTS
            },
            checkpoint_sha256=run.capture.checkpoint.content_sha256,
        )
        and _artifact_reload_matches(
            run.producer_ack_artifact,
            run.capture.producer_ack_ledger.model_dump(mode="json"),
        )
        and _artifact_reload_matches(
            run.transcript_artifact,
            run.capture.transcript.model_dump(mode="json"),
        )
        and _artifact_reload_matches(
            run.checkpoint_artifact,
            run.capture.checkpoint.model_dump(mode="json"),
        )
        and _artifact_reload_matches(
            run.normalized_artifact,
            _normalized_payload(run.normalized_outcome),
        )
        and _artifact_reload_matches(
            run.direct_provenance_artifact,
            _provenance_payload(run.direct_provenance),
        )
        and _artifact_reload_matches(
            run.captured_provenance_artifact,
            _provenance_payload(run.captured_provenance),
        )
        and _artifact_reload_matches(
            run.correction_semantics_artifact,
            run.correction_semantics,
        )
        and _artifact_reload_matches(
            run.recovery_evidence_artifact,
            run.recovery_evidence,
        )
        and _artifact_reload_matches(
            run.evidence_checkpoint_artifact,
            run.evidence_checkpoint.model_dump(mode="json"),
        )
        and _artifact_reload_matches(
            run.artifact_index_artifact,
            asdict(run.artifact_index),
        )
    )


def _runtime(*, require_valid_attestation: bool = True) -> R2DVerificationRuntime:
    observed = platform.system().strip().lower()
    os_family = cast(
        Literal["linux", "windows", "macos", "other"],
        {"darwin": "macos", "linux": "linux", "windows": "windows"}.get(observed, "other"),
    )
    runner_value = os.environ.get("RUNNER_OS", "").strip().lower()
    runner_family = {
        "linux": "linux",
        "macos": "macos",
        "windows": "windows",
    }.get(runner_value)
    if require_valid_attestation and runner_value and runner_family is None:
        raise ValueError("RUNNER_OS is not a recognized GitHub runner platform")
    if require_valid_attestation and runner_family is not None and runner_family != os_family:
        raise ValueError("RUNNER_OS does not match the observed R2D runtime platform")
    return R2DVerificationRuntime(
        os_family=os_family,
        architecture=platform.machine().strip().lower() or "unknown",
        python_minor=f"{sys.version_info.major}.{sys.version_info.minor}",
        github_runner_os_attested=runner_family == os_family,
    )


def _schemas_match(config: R2DConfig) -> bool:
    try:
        return _require_schema_bundle(config) == config.schema_contract.schema_bundle_sha256
    except Exception:
        return False


def _transcript_semantics_ok(run: R2DA1RunResult) -> bool:
    transcript = run.capture.transcript
    used = {item.record.partition for item in transcript.records}
    correction = tuple(
        item
        for item in transcript.records
        if item.record.lineage is not None
        and item.record.lineage.logical_event_id == "ADJ-CYCLE-001"
    )
    return (
        transcript.partition_count == 2
        and transcript.content_sha256
        == run.config.expected_evidence_contract.capture_transcript_sha256
        and used == {0, 1}
        and len(correction) == 2
        and tuple(
            item.record.lineage.revision
            for item in sorted(correction, key=lambda value: value.record.offset)
            if item.record.lineage is not None
        )
        == (1, 2)
        and len({item.record.partition for item in correction}) == 1
        and len({item.record.key_hex for item in correction}) == 1
    )


def _transport_identity_ok(run: R2DA1RunResult) -> bool:
    records = run.capture.transcript.records
    try:
        validate_portable_producer_ack_ledger(
            run.capture.producer_ack_ledger,
            tuple(item.record for item in records),
            run.config,
            require_frozen_source=True,
        )
    except Exception:
        return False
    return (
        len(records) == len(run.capture.raw_values) == 9
        and all(item.decision.disposition == "admitted" for item in records)
        and len({item.transport_identity for item in records}) == len(records)
        and len({item.physical_fingerprint for item in records}) == len(records)
        and run.capture.producer_ack_ledger.acknowledged_record_count == len(records)
        and not run.capture.producer_ack_ledger.real_broker_ack_claim
    )


def _checkpoint_ok(run: R2DA1RunResult) -> bool:
    try:
        verified = verify_complete_checkpoint(
            run.capture.checkpoint,
            run.capture.transcript,
            run.config,
            run.capture.producer_ack_ledger,
        )
    except Exception:
        return False
    return (
        verified == run.capture.checkpoint
        and verified.producer_ack_frozen_source_match
        and all(
            item.start_offset == 0
            and item.next_offset == item.end_offset_exclusive
            and len(item.physical_fingerprints) == item.end_offset_exclusive - item.start_offset
            for item in verified.partitions
        )
    )


def _raw_conservation_ok(run: R2DA1RunResult) -> bool:
    admitted = tuple(
        value
        for item in run.capture.transcript.records
        for value in (item.record.value_bytes,)
        if item.decision.disposition == "admitted" and value is not None
    )
    reconstructed = split_lf_jsonl_values(
        (run.capture.package.source_dir / _INVENTORY_FILE).read_bytes()
    )
    expected = ingestion_raw_value_hash_counts(run.capture.raw_values)
    acknowledged = Counter(
        item.raw_value_sha256 for item in run.capture.producer_ack_ledger.acknowledgements
    )
    return (
        expected == acknowledged
        and expected == ingestion_raw_value_hash_counts(admitted)
        and expected == ingestion_raw_value_hash_counts(reconstructed)
        and expected
        == ingestion_raw_value_hash_counts(
            split_lf_jsonl_values(reconstruct_jsonl_bytes(run.capture.transcript, run.config))
        )
    )


def _source_package_ok(run: R2DA1RunResult) -> bool:
    direct = {item.dataset: item for item in run.fixture.manifest.sources}
    captured = {item.dataset: item for item in run.capture.package.manifest.sources}
    if len(direct) != len(captured) or len(direct) != 7 or set(direct) != set(captured):
        return False
    for dataset, source in direct.items():
        captured_source = captured[dataset]
        captured_path = run.capture.package.source_dir / captured_source.file_name
        if file_sha256(captured_path) != captured_source.content_sha256:
            return False
        if dataset != _INVENTORY_DATASET and (
            captured_source != source
            or captured_path.read_bytes()
            != (run.fixture.source_dir / source.file_name).read_bytes()
        ):
            return False
    try:
        _validate_frozen_source_fixture(run.fixture, run.config)
    except Exception:
        return False
    return file_sha256(
        run.capture.package.manifest_path
    ) == run.capture.package.manifest_file_sha256 and _verify_artifact_index(
        run.artifact_index,
        run.fixture.source_dir.parent,
        run.config,
        direct_provenance=_provenance_payload(run.direct_provenance),
        captured_provenance=_provenance_payload(run.captured_provenance),
        manifest_content_sha256={
            item.dataset: item.content_sha256
            for item in run.artifacts
            if item.dataset in _EXPECTED_PRECHECKPOINT_MANIFESTS
        },
        checkpoint_sha256=run.capture.checkpoint.content_sha256,
    )


def _r2a_summaries_pass(run: R2DA1RunResult) -> bool:
    for rebuilds in (run.direct_rebuilds, run.captured_rebuilds):
        for result in rebuilds.ordered():
            try:
                _verified_summary(result)
                for artifact in result.landing.validated_artifacts:
                    _verified_materialized_artifact(
                        cutoff=_cutoff_text(result.planning_cutoff),
                        stage="validated",
                        artifact=artifact,
                    )
                for artifact in result.gold_artifacts:
                    _verified_materialized_artifact(
                        cutoff=_cutoff_text(result.planning_cutoff),
                        stage="gold",
                        artifact=artifact,
                    )
            except Exception:
                return False
    return True


def _parity_ok(run: R2DA1RunResult) -> bool:
    reconstructed = split_lf_jsonl_values(
        (run.capture.package.source_dir / _INVENTORY_FILE).read_bytes()
    )
    captured = normalize_business_outcome(
        raw_values=reconstructed,
        rebuilds=run.captured_rebuilds,
    )
    return (
        run.normalized_outcome == captured
        and len(captured.cutoffs) == 2
        and all(len(item.gold) == 5 for item in captured.cutoffs)
    )


def _adjustment_qty(result: R2RebuildResult) -> int:
    rows = pq.read_table(result.gold_artifact("gold.r2_inventory_position").path).to_pylist()
    row = next(item for item in rows if item["sku_location_key"] == "SKU-001::DC-ARG-01")
    return int(row["adjustment_qty"])


def _point_in_time_ok(run: R2DA1RunResult) -> bool:
    for rebuilds in (run.direct_rebuilds, run.captured_rebuilds):
        if not (
            _adjustment_qty(rebuilds.early) == -1
            and _adjustment_qty(rebuilds.final) == 1
            and rebuilds.early.quality_counts["deferred_inventory_adjustments"] == 2
            and rebuilds.final.quality_counts["deferred_inventory_adjustments"] == 1
            and rebuilds.final.quality_counts["superseded_inventory_adjustments"] == 1
        ):
            return False
    correction = run.correction_semantics
    return (
        correction.get("captured_append_revisions") == [2, 1]
        and correction.get("rev2_only_quarantine_reason_counts") == {"correction_chain_invalid": 1}
        and correction.get("complete_validated_revisions") == [1, 2]
        and correction.get("later_complete_replay_corrects") is True
        and correction.get("gap_chain_deferred_count") == 1
        and "incomplete_visible_revision_chain"
        in cast(list[object], correction.get("gap_deferred_reasons", []))
        and _artifact_reload_matches(
            run.correction_semantics_artifact,
            correction,
        )
    )


def _provenance_ok(run: R2DA1RunResult) -> bool:
    return (
        canonical_json_sha256(_without_file_evidence(_provenance_payload(run.direct_provenance)))
        == run.config.expected_evidence_contract.direct_provenance_projection_sha256
        and run.direct_provenance != run.captured_provenance
        and run.direct_provenance.checkpoint_sha256 is None
        and run.captured_provenance.checkpoint_sha256 == run.capture.checkpoint.content_sha256
        and bool(run.direct_provenance.validated_artifacts)
        and bool(run.captured_provenance.validated_artifacts)
        and run.direct_provenance.source_manifest_sha256
        != run.captured_provenance.source_manifest_sha256
        and _artifact_reload_matches(
            run.direct_provenance_artifact,
            _provenance_payload(run.direct_provenance),
        )
        and _artifact_reload_matches(
            run.captured_provenance_artifact,
            _provenance_payload(run.captured_provenance),
        )
    )


def _verification_checks(
    first: R2DA1RunResult,
    second: R2DA1RunResult,
    resource: R2DResourceEvidence,
    resource_artifact: ScientificArtifactRef,
) -> tuple[R2DVerificationCheck, ...]:
    checks = (
        R2DVerificationCheck(
            name="two_fresh_replay",
            passed=first.semantic_sha256 == second.semantic_sha256,
            detail="two fresh replay semantic hashes match",
            evidence_sha256=first.semantic_sha256,
        ),
        R2DVerificationCheck(
            name="config_and_schema_identity",
            passed=(
                first.config_sha256 == second.config_sha256
                and _schemas_match(first.config)
                and _schemas_match(second.config)
            ),
            detail="strict config and checked-in schema bytes verified",
            evidence_sha256=first.config_sha256,
        ),
        R2DVerificationCheck(
            name="deterministic_two_partition_transcript",
            passed=(
                first.capture.transcript.content_sha256 == second.capture.transcript.content_sha256
                and _transcript_semantics_ok(first)
                and _transcript_semantics_ok(second)
            ),
            detail="two partitions used and correction revisions co-located",
            evidence_sha256=first.capture.transcript.content_sha256,
        ),
        R2DVerificationCheck(
            name="transport_admission_and_physical_identity",
            passed=_transport_identity_ok(first) and _transport_identity_ok(second),
            detail="all nine values admitted with unique physical identities",
            evidence_sha256=first.capture.transcript.content_sha256,
        ),
        R2DVerificationCheck(
            name="complete_half_open_checkpoint",
            passed=(
                first.capture.checkpoint.content_sha256 == second.capture.checkpoint.content_sha256
                and _checkpoint_ok(first)
                and _checkpoint_ok(second)
            ),
            detail="both contiguous half-open partition barriers completed",
            evidence_sha256=first.capture.checkpoint.content_sha256,
        ),
        R2DVerificationCheck(
            name="raw_value_conservation",
            passed=_raw_conservation_ok(first) and _raw_conservation_ok(second),
            detail=(
                "direct, simulated producer-ack, admitted capture, and reconstructed "
                "raw-value multisets match"
            ),
            evidence_sha256=first.capture.producer_ack_ledger.content_sha256,
        ),
        R2DVerificationCheck(
            name="manifest_bound_source_reconstruction",
            passed=_source_package_ok(first) and _source_package_ok(second),
            detail="seven-file packages and six static source files verified",
            evidence_sha256=first.evidence_checkpoint.content_sha256,
        ),
        R2DVerificationCheck(
            name="public_r2a_semantic_authority",
            passed=_r2a_summaries_pass(first) and _r2a_summaries_pass(second),
            detail="public R2A rebuild produced four passing route-cutoff summaries",
            evidence_sha256=first.evidence_checkpoint.content_sha256,
        ),
        R2DVerificationCheck(
            name="direct_captured_business_parity",
            passed=_parity_ok(first) and _parity_ok(second),
            detail="two cutoffs and exactly five sorted Gold products match",
            evidence_sha256=first.semantic_sha256,
        ),
        R2DVerificationCheck(
            name="point_in_time_correction_semantics",
            passed=_point_in_time_ok(first) and _point_in_time_ok(second),
            detail="early and final correction deferred and superseded behavior verified",
            evidence_sha256=first.evidence_checkpoint.content_sha256,
        ),
        R2DVerificationCheck(
            name="route_provenance_integrity",
            passed=(
                _provenance_ok(first)
                and _provenance_ok(second)
                and first.direct_provenance_artifact.content_sha256
                == second.direct_provenance_artifact.content_sha256
                and first.captured_provenance_artifact.content_sha256
                == second.captured_provenance_artifact.content_sha256
            ),
            detail="distinct route provenance verified and captured route binds checkpoint",
            evidence_sha256=first.evidence_checkpoint.content_sha256,
        ),
        R2DVerificationCheck(
            name="bounded_portable_resource_envelope",
            passed=resource.passed,
            detail=(
                "measured two fresh R2D-A1 executions and all pre-receipt governed "
                "run artifacts; final evidence publication is outside measured scope"
            ),
            evidence_sha256=resource.semantic_sha256,
        ),
        R2DVerificationCheck(
            name="immutable_artifacts",
            passed=(
                _run_artifacts_reload(first)
                and _run_artifacts_reload(second)
                and _artifact_reload_matches(resource_artifact, resource.to_dict())
            ),
            detail=(
                "80 core indexed artifacts reload exactly; retained correction and recovery "
                "trees are separately live-rederived and verified"
            ),
            evidence_sha256=first.artifact_index.artifact_set_sha256,
        ),
    )
    if tuple(item.name for item in checks) != R2D_A1_PASS_CHECK_NAMES:
        raise RuntimeError("R2D-A1 verifier checklist differs from the strict receipt contract")
    return checks


def _write_receipt_once(path: Path, receipt: R2DVerificationReceipt) -> R2DVerificationReceipt:
    payload = canonical_json_bytes(receipt.model_dump(mode="json")) + b"\n"
    _write_exact_bytes(path, payload, root=path.parent)
    existing = path.read_bytes()
    loaded = R2DVerificationReceipt.model_validate_json(existing)
    if existing != canonical_json_bytes(loaded.model_dump(mode="json")) + b"\n":
        raise R2DPackageConflictError(f"R2D-A1 receipt is not canonical: {path}")
    return loaded


def _failure_receipt(
    runtime: R2DVerificationRuntime,
    error: Exception,
) -> R2DVerificationReceipt:
    detail = f"{type(error).__name__}: {error}"[:500]
    failure_sha256 = canonical_json_sha256(
        {"contract": "r2d.a1.verification-failure.v1", "detail": detail}
    )
    identities = {
        "source_manifest_sha256": R2D_SOURCE_MANIFEST_SHA256,
        "config_sha256": failure_sha256,
        "normalized_business_sha256": failure_sha256,
        "transcript_sha256": failure_sha256,
        "checkpoint_sha256": failure_sha256,
        "producer_ack_sha256": failure_sha256,
        "lineage_sha256": failure_sha256,
        "evidence_checkpoint_sha256": failure_sha256,
        "code_sha256": failure_sha256,
        "artifact_set_sha256": failure_sha256,
        "direct_provenance_sha256": failure_sha256,
        "captured_provenance_sha256": failure_sha256,
        "resource_evidence_sha256": failure_sha256,
    }
    return R2DVerificationReceipt(
        contract_version="r2d.a1.verification.v1",
        release="R2D-A1",
        status="FAIL",
        full_r2d_a_status="OPEN",
        full_g2_status="OPEN",
        service_backed_status="NOT_RUN",
        operational_recovery_claim=False,
        runtime=runtime,
        checks=tuple(
            R2DVerificationCheck(
                name=name,
                passed=False,
                detail=detail,
                evidence_sha256=None,
            )
            for name in R2D_A1_PASS_CHECK_NAMES
        ),
        source_manifest_sha256=cast(
            Literal["03e32a3f197839134e44b8f6d384787a99ec22e2c088ee4edea8d8322814346b"],
            R2D_SOURCE_MANIFEST_SHA256,
        ),
        config_sha256=failure_sha256,
        normalized_business_sha256=failure_sha256,
        transcript_sha256=failure_sha256,
        checkpoint_sha256=failure_sha256,
        producer_ack_sha256=failure_sha256,
        lineage_sha256=failure_sha256,
        evidence_checkpoint_sha256=failure_sha256,
        code_sha256=failure_sha256,
        artifact_set_sha256=failure_sha256,
        direct_provenance_sha256=failure_sha256,
        captured_provenance_sha256=failure_sha256,
        resource_evidence_sha256=failure_sha256,
        evidence_bindings_sha256=r2d_evidence_bindings_sha256(**identities),
    )


def _portable_bundle_payload(
    *,
    run: R2DA1RunResult,
    resource: R2DResourceEvidence,
    resource_artifact: ScientificArtifactRef,
    artifact_root: Path,
) -> dict[str, object]:
    run_root = run.fixture.source_dir.parent
    receipt_binding = {
        "source_manifest_sha256": run.config.source_contract.source_manifest_sha256,
        "config_sha256": run.config_sha256,
        "normalized_business_sha256": run.semantic_sha256,
        "transcript_sha256": run.capture.transcript.content_sha256,
        "checkpoint_sha256": run.capture.checkpoint.content_sha256,
        "producer_ack_sha256": run.capture.producer_ack_ledger.content_sha256,
        "lineage_sha256": run.capture.transcript.lineage_sha256,
        "evidence_checkpoint_sha256": run.evidence_checkpoint.content_sha256,
        "code_sha256": run.code_sha256,
        "artifact_set_sha256": run.artifact_index.artifact_set_sha256,
        "direct_provenance_sha256": run.direct_provenance_artifact.content_sha256,
        "captured_provenance_sha256": run.captured_provenance_artifact.content_sha256,
        "resource_evidence_sha256": resource.semantic_sha256,
    }
    return {
        "contract_version": "r2d.a1.portable-bundle.v1",
        "release": "R2D-A1",
        "full_r2d_a_status": "OPEN",
        "full_g2_status": "OPEN",
        "service_backed_status": "NOT_RUN",
        "operational_recovery_claim": False,
        "run_root": _relative_bundle_path(artifact_root, run_root),
        "config": run.config.model_dump(mode="json"),
        "producer_ack_ledger": run.capture.producer_ack_ledger.model_dump(mode="json"),
        "transcript": run.capture.transcript.model_dump(mode="json"),
        "capture_checkpoint": run.capture.checkpoint.model_dump(mode="json"),
        "normalized_business": _normalized_payload(run.normalized_outcome),
        "direct_provenance": _provenance_payload(run.direct_provenance),
        "captured_provenance": _provenance_payload(run.captured_provenance),
        "correction_semantics": run.correction_semantics,
        "portable_recovery": run.recovery_evidence,
        "evidence_checkpoint": run.evidence_checkpoint.model_dump(mode="json"),
        "resource_evidence": resource.to_dict(),
        "artifact_index": asdict(run.artifact_index),
        "publications": [
            {
                "kind": kind,
                "path": _relative_bundle_path(artifact_root, reference.path),
                "content_sha256": reference.content_sha256,
                "file_sha256": reference.file_sha256,
            }
            for kind, reference in (
                ("evidence_checkpoint", run.evidence_checkpoint_artifact),
                ("artifact_index", run.artifact_index_artifact),
                ("resource_evidence", resource_artifact),
            )
        ],
        "receipt_binding": {
            **receipt_binding,
            "evidence_bindings_sha256": r2d_evidence_bindings_sha256(**receipt_binding),
        },
    }


def _write_portable_bundle_once(
    path: Path,
    payload: dict[str, object],
) -> R2DPortableBundleRef:
    encoded = canonical_json_bytes(payload) + b"\n"
    _write_exact_bytes(path, encoded, root=path.parent)
    observed = path.read_bytes()
    parsed = json.loads(observed)
    normalized_payload = json.loads(canonical_json_bytes(payload))
    if observed != canonical_json_bytes(parsed) + b"\n" or parsed != normalized_payload:
        raise R2DPackageConflictError(f"R2D-A1 portable bundle is not canonical: {path}")
    return R2DPortableBundleRef(
        path=path,
        content_sha256=canonical_json_sha256(parsed),
        file_sha256=file_sha256(path),
    )


def verify_r2d_a1(
    artifact_root: Path,
    config_path: Path = DEFAULT_R2D_CONFIG_PATH,
) -> R2DA1VerificationResult:
    """Execute two fresh portable A1 runs and persist one strict fixed-path receipt."""

    artifact_root = _preflight_governed_directory(artifact_root)
    run_a = _preflight_governed_directory(artifact_root / "run-a")
    run_b = _preflight_governed_directory(artifact_root / "run-b")
    receipt_path = artifact_root / _RECEIPT_FILE
    first: R2DA1RunResult | None = None
    second: R2DA1RunResult | None = None
    resource: R2DResourceEvidence | None = None
    resource_artifact: ScientificArtifactRef | None = None
    portable_bundle: R2DPortableBundleRef | None = None
    runtime = _runtime(require_valid_attestation=False)
    try:
        runtime = _runtime()
        config = load_r2d_config(config_path)
        with R2DResourceMonitor(
            config,
            governed_artifact_roots=(run_a, run_b),
        ) as monitor:
            first = run_r2d_a1_once(run_a, config_path=config_path)
            second = run_r2d_a1_once(run_b, config_path=config_path)
        resource = monitor.evidence
        resource_artifact = _write_r2d_manifest(
            artifact_root / "evidence",
            dataset="r2d.a1.resource_evidence",
            value=resource.to_dict(),
            contract="r2d.a1.resource-evidence.v1",
        )
        checks = _verification_checks(first, second, resource, resource_artifact)
        passed = all(item.passed for item in checks)
        bundle_payload = _portable_bundle_payload(
            run=first,
            resource=resource,
            resource_artifact=resource_artifact,
            artifact_root=artifact_root,
        )
        portable_bundle = _write_portable_bundle_once(
            artifact_root / _PORTABLE_BUNDLE_FILE,
            bundle_payload,
        )
        receipt_identities = {
            "source_manifest_sha256": first.config.source_contract.source_manifest_sha256,
            "config_sha256": first.config_sha256,
            "normalized_business_sha256": first.semantic_sha256,
            "transcript_sha256": first.capture.transcript.content_sha256,
            "checkpoint_sha256": first.capture.checkpoint.content_sha256,
            "producer_ack_sha256": first.capture.producer_ack_ledger.content_sha256,
            "lineage_sha256": first.capture.transcript.lineage_sha256,
            "evidence_checkpoint_sha256": first.evidence_checkpoint.content_sha256,
            "code_sha256": first.code_sha256,
            "artifact_set_sha256": first.artifact_index.artifact_set_sha256,
            "direct_provenance_sha256": first.direct_provenance_artifact.content_sha256,
            "captured_provenance_sha256": first.captured_provenance_artifact.content_sha256,
            "resource_evidence_sha256": resource.semantic_sha256,
        }
        receipt = R2DVerificationReceipt(
            contract_version="r2d.a1.verification.v1",
            release="R2D-A1",
            status="PASS" if passed else "FAIL",
            full_r2d_a_status="OPEN",
            full_g2_status="OPEN",
            service_backed_status="NOT_RUN",
            operational_recovery_claim=False,
            runtime=runtime,
            checks=checks,
            source_manifest_sha256=first.config.source_contract.source_manifest_sha256,
            config_sha256=first.config_sha256,
            normalized_business_sha256=first.semantic_sha256,
            transcript_sha256=first.capture.transcript.content_sha256,
            checkpoint_sha256=first.capture.checkpoint.content_sha256,
            producer_ack_sha256=first.capture.producer_ack_ledger.content_sha256,
            lineage_sha256=first.capture.transcript.lineage_sha256,
            evidence_checkpoint_sha256=first.evidence_checkpoint.content_sha256,
            code_sha256=first.code_sha256,
            artifact_set_sha256=first.artifact_index.artifact_set_sha256,
            direct_provenance_sha256=first.direct_provenance_artifact.content_sha256,
            captured_provenance_sha256=first.captured_provenance_artifact.content_sha256,
            resource_evidence_sha256=resource.semantic_sha256,
            evidence_bindings_sha256=r2d_evidence_bindings_sha256(**receipt_identities),
        )
    except Exception as error:
        receipt = _failure_receipt(runtime, error)
    loaded = _write_receipt_once(receipt_path, receipt)
    return R2DA1VerificationResult(
        receipt=loaded,
        receipt_path=receipt_path,
        run_a=first,
        run_b=second,
        resource_evidence=resource,
        resource_artifact=resource_artifact,
        portable_bundle=portable_bundle,
    )


def _load_canonical_receipt(path: Path) -> R2DVerificationReceipt:
    payload = path.read_bytes()
    receipt = R2DVerificationReceipt.model_validate_json(payload)
    if payload != canonical_json_bytes(receipt.model_dump(mode="json")) + b"\n":
        raise ValueError(f"R2D-A1 receipt is not canonical: {path}")
    return receipt


def _artifact_index_from_payload(payload: object) -> R2DArtifactIndex:
    if not isinstance(payload, dict):
        raise ValueError("portable bundle artifact_index must be an object")
    required = {
        "contract_version",
        "release",
        "definition",
        "config_sha256",
        "schema_bundle_sha256",
        "code_sha256",
        "entries",
        "artifact_set_sha256",
    }
    if set(payload) != required or not isinstance(payload["entries"], list):
        raise ValueError("portable bundle artifact_index has an unexpected shape")
    entries = tuple(
        cast(dict[str, object], item) for item in payload["entries"] if isinstance(item, dict)
    )
    if len(entries) != len(payload["entries"]):
        raise ValueError("portable bundle artifact_index entries must be objects")
    return R2DArtifactIndex(
        contract_version=cast(Literal["r2d.a1.artifact-index.v1"], payload["contract_version"]),
        release=cast(Literal["R2D-A1"], payload["release"]),
        definition=cast(str, payload["definition"]),
        config_sha256=cast(str, payload["config_sha256"]),
        schema_bundle_sha256=cast(str, payload["schema_bundle_sha256"]),
        code_sha256=cast(str, payload["code_sha256"]),
        entries=entries,
        artifact_set_sha256=cast(str, payload["artifact_set_sha256"]),
    )


def _resource_evidence_from_payload(
    payload: object,
    config: R2DConfig,
) -> R2DResourceEvidence:
    """Rebuild resource evidence from observations and the frozen budget."""

    if not isinstance(payload, dict) or set(payload) != {
        "status",
        "passed",
        "measurement",
        "budget",
        "observed",
        "checks",
        "measurement_errors",
    }:
        raise ValueError("portable resource evidence has an unexpected shape")
    measurement = payload.get("measurement")
    observed = payload.get("observed")
    checks_payload = payload.get("checks")
    errors_payload = payload.get("measurement_errors")
    if not isinstance(measurement, dict) or set(measurement) != {
        "wall_time",
        "peak_process_tree_rss",
        "peak_process_tree_native_threads",
        "shutdown",
        "governed_artifacts",
        "integrity",
        "sample_interval_seconds",
    }:
        raise ValueError("portable resource measurement contract differs")
    if not isinstance(observed, dict) or set(observed) != {
        "wall_time_seconds",
        "peak_process_tree_rss_bytes",
        "peak_process_tree_rss_mib",
        "peak_process_tree_native_threads",
        "maximum_descendant_count",
        "shutdown_time_seconds",
        "surviving_descendant_pids",
        "governed_artifact_bytes",
        "governed_artifact_file_count",
    }:
        raise ValueError("portable resource observations have an unexpected shape")
    if not isinstance(checks_payload, list) or not isinstance(errors_payload, list):
        raise ValueError("portable resource checks and errors must be arrays")
    if any(not isinstance(item, str) for item in errors_payload):
        raise ValueError("portable resource measurement errors must be strings")

    def required_number(name: str) -> float:
        value = observed[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"portable resource observation {name} must be numeric")
        return float(value)

    def required_int(name: str) -> int:
        value = observed[name]
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"portable resource observation {name} must be an integer")
        return cast(int, value)

    survivor_payload = observed["surviving_descendant_pids"]
    if not isinstance(survivor_payload, list) or any(
        isinstance(item, bool) or not isinstance(item, int) for item in survivor_payload
    ):
        raise ValueError("portable resource survivor PIDs must be integer arrays")
    sample_interval = measurement["sample_interval_seconds"]
    if isinstance(sample_interval, bool) or not isinstance(sample_interval, (int, float)):
        raise ValueError("portable resource sample interval must be numeric")

    wall_time = required_number("wall_time_seconds")
    peak_rss = required_int("peak_process_tree_rss_bytes")
    peak_threads = required_int("peak_process_tree_native_threads")
    maximum_descendants = required_int("maximum_descendant_count")
    shutdown_time = required_number("shutdown_time_seconds")
    artifact_bytes = required_int("governed_artifact_bytes")
    artifact_files = required_int("governed_artifact_file_count")
    survivors = tuple(cast(list[int], survivor_payload))
    errors = tuple(cast(list[str], errors_payload))
    if (
        wall_time < 0
        or peak_rss < 0
        or peak_threads < 0
        or maximum_descendants < 0
        or shutdown_time < 0
        or artifact_bytes < 0
        or artifact_files < 0
        or any(pid <= 0 for pid in survivors)
        or not 0.001 <= float(sample_interval) <= 1.0
    ):
        raise ValueError("portable resource observations are outside physical bounds")
    budget = config.resource_contract
    expected_checks = (
        R2DResourceCheck(
            "measurement_integrity",
            not errors,
            len(errors),
            0,
            "==",
            INTEGRITY_METHOD,
        ),
        R2DResourceCheck(
            "wall_time_seconds",
            wall_time <= budget.wall_time_seconds,
            wall_time,
            budget.wall_time_seconds,
            "<=",
            WALL_TIME_METHOD,
        ),
        R2DResourceCheck(
            "peak_process_tree_rss_bytes",
            peak_rss <= budget.peak_process_tree_rss_mib * MIB,
            peak_rss,
            budget.peak_process_tree_rss_mib * MIB,
            "<=",
            RSS_METHOD,
        ),
        R2DResourceCheck(
            "peak_process_tree_native_threads",
            peak_threads <= budget.maximum_process_tree_threads,
            peak_threads,
            budget.maximum_process_tree_threads,
            "<=",
            NATIVE_THREAD_METHOD,
        ),
        R2DResourceCheck(
            "governed_artifact_bytes",
            not errors and artifact_bytes <= budget.artifact_mib * MIB,
            artifact_bytes,
            budget.artifact_mib * MIB,
            "<=",
            ARTIFACT_METHOD,
        ),
        R2DResourceCheck(
            "shutdown_time_seconds",
            shutdown_time <= budget.shutdown_seconds,
            shutdown_time,
            budget.shutdown_seconds,
            "<=",
            SHUTDOWN_METHOD,
        ),
        R2DResourceCheck(
            "surviving_descendant_count",
            not errors and len(survivors) <= budget.maximum_surviving_descendants,
            len(survivors),
            budget.maximum_surviving_descendants,
            "<=",
            SHUTDOWN_METHOD,
        ),
    )
    evidence = R2DResourceEvidence(
        wall_time_seconds=wall_time,
        peak_process_tree_rss_bytes=peak_rss,
        peak_process_tree_native_threads=peak_threads,
        maximum_descendant_count=maximum_descendants,
        shutdown_time_seconds=shutdown_time,
        surviving_descendant_pids=survivors,
        governed_artifact_bytes=artifact_bytes,
        governed_artifact_file_count=artifact_files,
        sample_interval_seconds=float(sample_interval),
        budget=budget,
        checks=expected_checks,
        measurement_errors=errors,
    )
    expected_measurement = {
        "wall_time": WALL_TIME_METHOD,
        "peak_process_tree_rss": RSS_METHOD,
        "peak_process_tree_native_threads": NATIVE_THREAD_METHOD,
        "shutdown": SHUTDOWN_METHOD,
        "governed_artifacts": ARTIFACT_METHOD,
        "integrity": INTEGRITY_METHOD,
        "sample_interval_seconds": evidence.sample_interval_seconds,
    }
    if (
        measurement != expected_measurement
        or payload.get("budget") != budget.model_dump(mode="json")
        or checks_payload != [item.to_dict() for item in expected_checks]
        or payload != evidence.to_dict()
        or not evidence.passed
    ):
        raise ValueError("portable resource evidence is not a passing frozen envelope")
    return evidence


def _safe_bundle_run_root(receipt_root: Path, relative_text: str) -> Path:
    relative = PurePosixPath(relative_text)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError("portable bundle run_root must be a safe relative POSIX path")
    if any("\\" in segment or ":" in segment for segment in relative.parts):
        raise ValueError("portable bundle run_root must be a relative POSIX path")
    candidate = receipt_root.absolute().joinpath(*relative.parts)
    current = receipt_root.absolute()
    for segment in relative.parts:
        current /= segment
        if current.is_symlink() or bool(getattr(current, "is_junction", lambda: False)()):
            raise ValueError(f"portable bundle run_root contains a link: {current}")
    if not candidate.is_dir():
        raise ValueError(f"portable bundle run_root is missing: {candidate}")
    return candidate


def _verify_portable_bundle(
    receipt_path: Path,
    receipt: R2DVerificationReceipt,
) -> dict[str, object]:
    bundle_path = receipt_path.with_name(_PORTABLE_BUNDLE_FILE)
    encoded = bundle_path.read_bytes()
    bundle = json.loads(encoded)
    if not isinstance(bundle, dict) or encoded != canonical_json_bytes(bundle) + b"\n":
        raise ValueError(f"R2D-A1 portable bundle is not canonical: {bundle_path}")
    expected_top_level = {
        "contract_version",
        "release",
        "full_r2d_a_status",
        "full_g2_status",
        "service_backed_status",
        "operational_recovery_claim",
        "run_root",
        "config",
        "producer_ack_ledger",
        "transcript",
        "capture_checkpoint",
        "normalized_business",
        "direct_provenance",
        "captured_provenance",
        "correction_semantics",
        "portable_recovery",
        "evidence_checkpoint",
        "resource_evidence",
        "artifact_index",
        "publications",
        "receipt_binding",
    }
    if set(bundle) != expected_top_level or (
        bundle.get("contract_version") != "r2d.a1.portable-bundle.v1"
        or bundle.get("release") != "R2D-A1"
        or bundle.get("full_r2d_a_status") != "OPEN"
        or bundle.get("full_g2_status") != "OPEN"
        or bundle.get("service_backed_status") != "NOT_RUN"
        or bundle.get("operational_recovery_claim") is not False
    ):
        raise ValueError("R2D-A1 portable bundle shape or non-claim constants differ")
    binding = bundle.get("receipt_binding")
    if not isinstance(binding, dict):
        raise ValueError("R2D-A1 portable bundle has no receipt binding")
    bound_fields = (
        "source_manifest_sha256",
        "config_sha256",
        "normalized_business_sha256",
        "transcript_sha256",
        "checkpoint_sha256",
        "producer_ack_sha256",
        "lineage_sha256",
        "evidence_checkpoint_sha256",
        "code_sha256",
        "artifact_set_sha256",
        "direct_provenance_sha256",
        "captured_provenance_sha256",
        "resource_evidence_sha256",
        "evidence_bindings_sha256",
    )
    if set(binding) != set(bound_fields) or any(
        binding[name] != getattr(receipt, name) for name in bound_fields
    ):
        raise ValueError("R2D-A1 portable bundle receipt binding mismatch")

    config = R2DConfig.model_validate(bundle.get("config"))
    frozen_config = load_r2d_config(DEFAULT_R2D_CONFIG_PATH)
    if config != frozen_config or canonical_json_bytes(
        bundle.get("config")
    ) != canonical_json_bytes(frozen_config.model_dump(mode="json")):
        raise ValueError("embedded R2D config differs from the checked-in frozen config")
    transcript = R2DCaptureTranscript.model_validate_json(
        canonical_json_bytes(bundle.get("transcript"))
    )
    producer_ack = R2DProducerAckLedger.model_validate_json(
        canonical_json_bytes(bundle.get("producer_ack_ledger"))
    )
    checkpoint = R2DCaptureCheckpoint.model_validate_json(
        canonical_json_bytes(bundle.get("capture_checkpoint"))
    )
    evidence_checkpoint = R2DEvidenceCheckpoint.model_validate_json(
        canonical_json_bytes(bundle.get("evidence_checkpoint"))
    )
    validate_capture_transcript(transcript, config)
    physical_records = tuple(item.record for item in transcript.records)
    validate_portable_producer_ack_ledger(
        producer_ack,
        physical_records,
        config,
        require_frozen_source=True,
    )
    verify_complete_checkpoint(checkpoint, transcript, config, producer_ack)
    artifact_index = _artifact_index_from_payload(bundle.get("artifact_index"))
    run_root = _safe_bundle_run_root(
        receipt_path.parent,
        cast(str, bundle.get("run_root")),
    )
    embedded_manifest_hashes = {
        "r2d.a1.portable_producer_ack": canonical_json_sha256(producer_ack.model_dump(mode="json")),
        "r2d.a1.capture_transcript": canonical_json_sha256(transcript.model_dump(mode="json")),
        "r2d.a1.capture_checkpoint": canonical_json_sha256(checkpoint.model_dump(mode="json")),
        "r2d.a1.normalized_business_outcome": canonical_json_sha256(
            bundle.get("normalized_business")
        ),
        "r2d.a1.direct_provenance": canonical_json_sha256(bundle.get("direct_provenance")),
        "r2d.a1.captured_provenance": canonical_json_sha256(bundle.get("captured_provenance")),
        "r2d.a1.correction_semantics": canonical_json_sha256(bundle.get("correction_semantics")),
        "r2d.a1.portable_recovery": canonical_json_sha256(bundle.get("portable_recovery")),
    }
    if not _verify_artifact_index(
        artifact_index,
        run_root,
        config,
        direct_provenance=bundle.get("direct_provenance"),
        captured_provenance=bundle.get("captured_provenance"),
        manifest_content_sha256=embedded_manifest_hashes,
        checkpoint_sha256=checkpoint.content_sha256,
    ):
        raise ValueError("R2D-A1 portable bundle artifact index failed independent reload")
    _verified_captured_source_derivation(
        transcript=transcript,
        index=artifact_index,
        root=run_root,
        config=config,
    )
    for dataset, expected_hash in embedded_manifest_hashes.items():
        matches = tuple(
            entry
            for entry in artifact_index.entries
            if entry.get("kind") == "r2d_manifest" and entry.get("dataset") == dataset
        )
        if len(matches) != 1 or matches[0].get("content_sha256") != expected_hash:
            raise ValueError(f"portable bundle embedded evidence is not indexed: {dataset}")
    normalized = _verified_normalized_business(
        bundle.get("normalized_business"),
        transcript=transcript,
        index=artifact_index,
        root=run_root,
        config=config,
    )
    correction_semantics = _verified_correction_semantics(
        bundle.get("correction_semantics"),
        index=artifact_index,
        root=run_root,
        config=config,
    )
    portable_recovery = _verified_portable_recovery(
        bundle.get("portable_recovery"),
        index=artifact_index,
        root=run_root,
        config=config,
        checkpoint=checkpoint,
        normalized_business=normalized,
    )
    direct_provenance = bundle.get("direct_provenance")
    captured_provenance = bundle.get("captured_provenance")
    expected_evidence = config.expected_evidence_contract
    if transcript.content_sha256 != expected_evidence.capture_transcript_sha256:
        raise ValueError("capture transcript differs from the frozen expected evidence")
    if (
        canonical_json_sha256(_without_file_evidence(direct_provenance))
        != expected_evidence.direct_provenance_projection_sha256
    ):
        raise ValueError("direct provenance differs from the frozen expected projection")
    if canonical_json_sha256(normalized) != expected_evidence.normalized_business_sha256:
        raise ValueError("normalized business differs from the frozen expected evidence")
    if (
        canonical_json_sha256(_without_file_evidence(correction_semantics))
        != expected_evidence.correction_semantics_projection_sha256
    ):
        raise ValueError(
            "correction semantics differs from the frozen expected evidence projection"
        )
    if (
        canonical_json_sha256(_portable_recovery_projection(portable_recovery))
        != expected_evidence.portable_recovery_projection_sha256
    ):
        raise ValueError("portable recovery differs from the frozen expected evidence")
    resource = _resource_evidence_from_payload(bundle.get("resource_evidence"), config)
    resource_check = next(
        (item for item in receipt.checks if item.name == "bounded_portable_resource_envelope"),
        None,
    )
    captured_inventory_entries = tuple(
        entry
        for entry in artifact_index.entries
        if _artifact_entry_identity(entry) == ("source_file", "captured", _INVENTORY_DATASET)
    )
    if len(captured_inventory_entries) != 1:
        raise ValueError("captured reconstructed JSONL is not uniquely indexed")
    reconstructed_jsonl_sha256 = captured_inventory_entries[0].get("file_sha256")
    if (
        r2d_config_sha256(config) != receipt.config_sha256
        or _require_schema_bundle(config) != artifact_index.schema_bundle_sha256
        or _code_bundle_sha256() != receipt.code_sha256
        or artifact_index.code_sha256 != receipt.code_sha256
        or artifact_index.config_sha256 != receipt.config_sha256
        or artifact_index.artifact_set_sha256 != receipt.artifact_set_sha256
        or canonical_json_sha256(normalized) != receipt.normalized_business_sha256
        or canonical_json_sha256(direct_provenance) != receipt.direct_provenance_sha256
        or canonical_json_sha256(captured_provenance) != receipt.captured_provenance_sha256
        or resource.semantic_sha256 != receipt.resource_evidence_sha256
        or resource_check is None
        or not resource_check.passed
        or resource_check.evidence_sha256 != resource.semantic_sha256
        or producer_ack.content_sha256 != receipt.producer_ack_sha256
        or transcript.content_sha256 != receipt.transcript_sha256
        or transcript.lineage_sha256 != receipt.lineage_sha256
        or checkpoint.content_sha256 != receipt.checkpoint_sha256
        or evidence_checkpoint.content_sha256 != receipt.evidence_checkpoint_sha256
        or config.source_contract.source_manifest_sha256 != receipt.source_manifest_sha256
        or evidence_checkpoint.source_manifest_sha256
        != config.source_contract.source_manifest_sha256
        or evidence_checkpoint.source_manifest_file_sha256
        != config.source_contract.source_manifest_file_sha256
        or evidence_checkpoint.inventory_events_file_sha256
        != config.source_contract.inventory_events_file_sha256
        or evidence_checkpoint.source_raw_value_multiset_sha256
        != config.source_contract.raw_value_multiset_sha256
        or evidence_checkpoint.producer_ack_sha256 != producer_ack.content_sha256
        or evidence_checkpoint.transcript_sha256 != transcript.content_sha256
        or evidence_checkpoint.lineage_sha256 != transcript.lineage_sha256
        or evidence_checkpoint.capture_checkpoint_sha256 != checkpoint.content_sha256
        or evidence_checkpoint.reconstructed_jsonl_sha256 != reconstructed_jsonl_sha256
        or evidence_checkpoint.artifact_set_sha256 != receipt.artifact_set_sha256
        or evidence_checkpoint.code_sha256 != receipt.code_sha256
        or receipt.evidence_bindings_sha256
        != r2d_evidence_bindings_sha256(
            **{
                name: getattr(receipt, name)
                for name in bound_fields
                if name != "evidence_bindings_sha256"
            }
        )
    ):
        raise ValueError("R2D-A1 portable bundle semantic or content binding mismatch")
    publications = bundle.get("publications")
    if not isinstance(publications, list) or len(publications) != 3:
        raise ValueError("R2D-A1 portable bundle must bind three final publications")
    expected_publication_content = {
        "evidence_checkpoint": canonical_json_sha256(bundle["evidence_checkpoint"]),
        "artifact_index": canonical_json_sha256(bundle["artifact_index"]),
        "resource_evidence": receipt.resource_evidence_sha256,
    }
    if {
        publication.get("kind") for publication in publications if isinstance(publication, dict)
    } != set(expected_publication_content):
        raise ValueError("R2D-A1 portable publication kinds must be exact and unique")
    for publication in publications:
        if not isinstance(publication, dict):
            raise ValueError("R2D-A1 portable publication must be an object")
        if set(publication) != {"kind", "path", "content_sha256", "file_sha256"}:
            raise ValueError("R2D-A1 portable publication shape must be exact")
        path = _safe_bundle_artifact_path(
            receipt_path.parent,
            cast(str, publication.get("path")),
        )
        parsed = json.loads(path.read_bytes())
        if (
            file_sha256(path) != publication.get("file_sha256")
            or canonical_json_sha256(parsed) != publication.get("content_sha256")
            or path.stem != publication.get("content_sha256")
            or publication.get("content_sha256")
            != expected_publication_content[cast(str, publication.get("kind"))]
        ):
            raise ValueError("R2D-A1 portable publication hash mismatch")
    return cast(dict[str, object], bundle)


def _without_file_evidence(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _without_file_evidence(item)
            for key, item in value.items()
            if key not in {"file_sha256", "summary_file_sha256", "gap_summary_file_sha256"}
        }
    if isinstance(value, list):
        return [_without_file_evidence(item) for item in value]
    return value


def _portable_recovery_projection(value: object) -> dict[str, object]:
    """Remove the config-bound checkpoint identity from frozen recovery semantics."""

    projected = _without_file_evidence(value)
    if not isinstance(projected, dict):
        raise ValueError("portable recovery projection must be an object")
    return {key: item for key, item in projected.items() if key != "checkpoint_sha256"}


def _stable_bundle_projection(
    receipt: R2DVerificationReceipt,
    bundle: dict[str, object],
) -> dict[str, object]:
    return {
        "release": receipt.release,
        "status": receipt.status,
        "full_r2d_a_status": receipt.full_r2d_a_status,
        "full_g2_status": receipt.full_g2_status,
        "service_backed_status": receipt.service_backed_status,
        "operational_recovery_claim": receipt.operational_recovery_claim,
        "source_manifest_sha256": receipt.source_manifest_sha256,
        "config_sha256": receipt.config_sha256,
        "normalized_business_sha256": receipt.normalized_business_sha256,
        "transcript_sha256": receipt.transcript_sha256,
        "checkpoint_sha256": receipt.checkpoint_sha256,
        "producer_ack_sha256": receipt.producer_ack_sha256,
        "lineage_sha256": receipt.lineage_sha256,
        "code_sha256": receipt.code_sha256,
        "schema_bundle_sha256": cast(dict[str, object], bundle["artifact_index"])[
            "schema_bundle_sha256"
        ],
        "config": bundle["config"],
        "producer_ack_ledger": bundle["producer_ack_ledger"],
        "transcript": bundle["transcript"],
        "capture_checkpoint": bundle["capture_checkpoint"],
        "normalized_business": bundle["normalized_business"],
        "direct_provenance": _without_file_evidence(bundle["direct_provenance"]),
        "captured_provenance": _without_file_evidence(bundle["captured_provenance"]),
        "correction_semantics": _without_file_evidence(bundle["correction_semantics"]),
        "portable_recovery": _without_file_evidence(bundle["portable_recovery"]),
        "check_outcomes": [
            (
                {"name": item.name, "passed": item.passed}
                if item.name == "bounded_portable_resource_envelope"
                else {"name": item.name, "passed": item.passed, "detail": item.detail}
            )
            for item in receipt.checks
        ],
    }


def compare_r2d_a1_receipt_files(left_path: Path, right_path: Path) -> dict[str, object]:
    """Exact-join attested Linux/Windows portable semantics, excluding measurements."""

    if left_path.resolve() == right_path.resolve():
        raise ValueError("R2D-A1 cross-platform comparison requires two distinct receipt files")
    left = _load_canonical_receipt(left_path)
    right = _load_canonical_receipt(right_path)
    left_bundle = _verify_portable_bundle(left_path, left)
    right_bundle = _verify_portable_bundle(right_path, right)
    if left.status != "PASS" or right.status != "PASS":
        raise ValueError("both R2D-A1 platform receipts must pass before comparison")
    if not left.runtime.github_runner_os_attested or not right.runtime.github_runner_os_attested:
        raise ValueError("both R2D-A1 receipts require GitHub runner OS attestation")
    platforms = {left.runtime.os_family, right.runtime.os_family}
    if platforms != {"linux", "windows"}:
        raise ValueError("R2D-A1 comparison requires one Linux and one Windows receipt")
    if left.runtime.python_minor != right.runtime.python_minor:
        raise ValueError("R2D-A1 platform receipts require the same Python minor version")
    if _stable_bundle_projection(left, left_bundle) != _stable_bundle_projection(
        right,
        right_bundle,
    ):
        raise ValueError("R2D-A1 platform receipts disagree on normalized semantic evidence")
    return {
        "status": "R2D_A1_CROSS_PLATFORM_PASS",
        "passed": True,
        "platforms": ["linux", "windows"],
        "python_minor": left.runtime.python_minor,
        "release": "R2D-A1",
        "full_r2d_a_status": "OPEN",
        "full_g2_status": "OPEN",
        "service_backed_status": "NOT_RUN",
        "operational_recovery_claim": False,
        "normalized_business_sha256": left.normalized_business_sha256,
    }
