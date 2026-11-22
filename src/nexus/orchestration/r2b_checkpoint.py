"""Durable, fail-closed validation checkpoints for R2B orchestration."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal, cast

import pyarrow.parquet as pq
from pydantic import Field, model_validator

from nexus.data.r2_models import (
    R2FulfillmentOutcome,
    R2InventoryAdjustment,
    R2LandingResult,
    R2OpeningInventory,
    R2ProductLocation,
    R2PurchaseOrder,
    R2QuarantineRecord,
    R2Receipt,
    R2SourceManifest,
    R2Supplier,
)
from nexus.modeling.r3_artifacts import (
    ScientificArtifactRef,
    canonical_json_sha256,
    write_immutable_json_manifest,
)
from nexus.orchestration.r2b_contracts import R2BContract, R2BIdentifier, R2BSha256
from nexus.storage import (
    ArtifactConflictError,
    ContentAddressedArtifact,
    canonical_rows_sha256,
    canonical_schema_sha256,
    file_sha256,
)

_CHECKPOINT_DATASET = "r2b.validation_checkpoint"
_CHECKPOINT_SCHEMA = {"contract": "r2b.validation-checkpoint", "version": "1.0.0"}
_VALIDATED_DATASETS = {
    "quarantine.r2",
    "validated.fulfillment_outcomes",
    "validated.inventory_adjustments",
    "validated.opening_inventory",
    "validated.product_locations",
    "validated.purchase_orders",
    "validated.receipts",
    "validated.suppliers",
}


class R2BCheckpointSourceFile(R2BContract):
    """Logical identity of one source-native byte object."""

    dataset: R2BIdentifier
    file_sha256: R2BSha256


class R2BCheckpointParquet(R2BContract):
    """Logical and schema identity of one validated Parquet object."""

    dataset: R2BIdentifier
    content_sha256: R2BSha256
    schema_sha256: R2BSha256
    row_count: Annotated[int, Field(ge=0)]


class R2BValidationCheckpointManifest(R2BContract):
    """Path-, cutoff-, and execution-independent validation identity."""

    artifact_contract_version: Literal["1.0.0"]
    release: Literal["R2B"]
    kind: Literal["validated_checkpoint"]
    status: Literal["VALIDATED"]
    validation_code_sha256: R2BSha256
    source_manifest_sha256: R2BSha256
    source_manifest_file_sha256: R2BSha256
    source_native: tuple[R2BCheckpointSourceFile, ...]
    validated: tuple[R2BCheckpointParquet, ...]

    @model_validator(mode="after")
    def datasets_are_complete_and_unique(self) -> R2BValidationCheckpointManifest:
        source_datasets = tuple(item.dataset for item in self.source_native)
        validated_datasets = tuple(item.dataset for item in self.validated)
        if len(set(source_datasets)) != len(source_datasets):
            raise ValueError("source-native checkpoint datasets must be unique")
        if len(set(validated_datasets)) != len(validated_datasets):
            raise ValueError("validated checkpoint datasets must be unique")
        if "source.manifest" not in source_datasets:
            raise ValueError("checkpoint must bind the source manifest bytes")
        if set(validated_datasets) != _VALIDATED_DATASETS:
            raise ValueError("checkpoint must bind the complete R2A validated dataset set")
        return self


@dataclass(frozen=True, slots=True)
class R2BValidationCheckpointRef:
    """Physical locator and identity of one immutable checkpoint manifest."""

    checkpoint_root: Path
    manifest_path: Path
    content_sha256: str
    schema_sha256: str
    file_sha256: str


def _canonical_object_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _source_dataset(path: Path, landing: R2LandingResult) -> str:
    if path.stem == landing.source_manifest_file_sha256:
        return "source.manifest"
    matching = [
        source.dataset
        for source in landing.source_manifest.sources
        if source.content_sha256 == path.stem
    ]
    if len(matching) != 1:
        raise ValueError(f"cannot bind source-native checkpoint artifact: {path}")
    return matching[0]


def write_r2b_validation_checkpoint(
    *,
    checkpoint_root: Path,
    validation_code_sha256: str,
    landing: R2LandingResult,
) -> R2BValidationCheckpointRef:
    """Persist a canonical checkpoint that can be reconstructed after a restart."""

    checkpoint_root = checkpoint_root.resolve()
    manifest = R2BValidationCheckpointManifest(
        artifact_contract_version="1.0.0",
        release="R2B",
        kind="validated_checkpoint",
        status="VALIDATED",
        validation_code_sha256=validation_code_sha256,
        source_manifest_sha256=landing.source_manifest_sha256,
        source_manifest_file_sha256=landing.source_manifest_file_sha256,
        source_native=tuple(
            sorted(
                (
                    R2BCheckpointSourceFile(
                        dataset=_source_dataset(path, landing),
                        file_sha256=file_sha256(path),
                    )
                    for path in landing.source_native_paths
                ),
                key=lambda item: item.dataset,
            )
        ),
        validated=tuple(
            R2BCheckpointParquet(
                dataset=artifact.dataset,
                content_sha256=artifact.content_sha256,
                schema_sha256=artifact.schema_sha256,
                row_count=artifact.row_count,
            )
            for artifact in sorted(landing.validated_artifacts, key=lambda item: item.dataset)
        ),
    )
    artifact = write_immutable_json_manifest(
        checkpoint_root / "governed",
        dataset=_CHECKPOINT_DATASET,
        manifest=manifest.model_dump(mode="json"),
        schema_identity=_CHECKPOINT_SCHEMA,
    )
    return _checkpoint_ref(checkpoint_root, artifact)


def _checkpoint_ref(
    checkpoint_root: Path,
    artifact: ScientificArtifactRef,
) -> R2BValidationCheckpointRef:
    return R2BValidationCheckpointRef(
        checkpoint_root=checkpoint_root.resolve(),
        manifest_path=artifact.path.resolve(),
        content_sha256=artifact.content_sha256,
        schema_sha256=artifact.schema_sha256,
        file_sha256=artifact.file_sha256,
    )


def checkpoint_ref_from_path(
    *,
    checkpoint_root: Path,
    manifest_path: Path,
) -> R2BValidationCheckpointRef:
    """Construct and verify the physical identity of a checkpoint manifest path."""

    checkpoint_root = checkpoint_root.resolve()
    manifest_path = manifest_path.resolve()
    if not manifest_path.is_relative_to(checkpoint_root):
        raise ArtifactConflictError("checkpoint manifest escapes its checkpoint root")
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ArtifactConflictError("checkpoint manifest is unreadable or invalid JSON") from error
    content_sha256 = canonical_json_sha256(payload)
    if manifest_path.stem != content_sha256:
        raise ArtifactConflictError("checkpoint manifest filename does not match its content")
    return R2BValidationCheckpointRef(
        checkpoint_root=checkpoint_root,
        manifest_path=manifest_path,
        content_sha256=content_sha256,
        schema_sha256=canonical_json_sha256(_CHECKPOINT_SCHEMA),
        file_sha256=file_sha256(manifest_path),
    )


def checkpoint_ref_from_root(checkpoint_root: Path) -> R2BValidationCheckpointRef:
    """Discover the single immutable checkpoint beneath a configured root."""

    manifests = sorted(
        (checkpoint_root.resolve() / "governed" / "r2b" / "validation_checkpoint").glob("*.json")
    )
    if len(manifests) != 1:
        raise ArtifactConflictError(
            f"expected exactly one validation checkpoint, found {len(manifests)}"
        )
    return checkpoint_ref_from_path(
        checkpoint_root=checkpoint_root,
        manifest_path=manifests[0],
    )


def _lake_namespace(reference: R2BValidationCheckpointRef) -> Path:
    return (
        reference.checkpoint_root
        / "rebuild"
        / "lake"
        / reference_manifest(reference).source_manifest_file_sha256[:16]
    )


def _within_checkpoint_root(
    reference: R2BValidationCheckpointRef,
    path: Path,
) -> Path:
    resolved = path.resolve()
    if not resolved.is_relative_to(reference.checkpoint_root.resolve()):
        raise ArtifactConflictError("derived checkpoint artifact escapes its root")
    return resolved


def reference_manifest(
    reference: R2BValidationCheckpointRef,
) -> R2BValidationCheckpointManifest:
    """Read the strict checkpoint manifest after validating its physical identity."""

    actual_reference = checkpoint_ref_from_path(
        checkpoint_root=reference.checkpoint_root,
        manifest_path=reference.manifest_path,
    )
    if actual_reference != reference:
        raise ArtifactConflictError("checkpoint physical identity changed after publication")
    return R2BValidationCheckpointManifest.model_validate_json(reference.manifest_path.read_bytes())


def _source_path(
    *,
    lake_namespace: Path,
    dataset: str,
    file_sha256_value: str,
    source_manifest: R2SourceManifest | None,
) -> Path:
    if dataset == "source.manifest":
        suffix = ".json"
    else:
        if source_manifest is None:
            raise ArtifactConflictError("source manifest must be loaded before bound sources")
        matches = [source for source in source_manifest.sources if source.dataset == dataset]
        if len(matches) != 1:
            raise ArtifactConflictError(f"checkpoint contains unknown source dataset: {dataset}")
        suffix = Path(matches[0].file_name).suffix
    return lake_namespace / "raw" / dataset.replace(".", "_") / f"{file_sha256_value}{suffix}"


def _validated_path(
    *,
    lake_namespace: Path,
    item: R2BCheckpointParquet,
) -> Path:
    return lake_namespace.joinpath(*item.dataset.split("."), f"{item.content_sha256}.parquet")


def _validated_rows(
    *,
    path: Path,
    item: R2BCheckpointParquet,
) -> tuple[list[dict[str, Any]], ContentAddressedArtifact]:
    try:
        table = pq.read_table(path)
    except Exception as error:
        raise ArtifactConflictError(f"cannot read checkpoint Parquet: {path}") from error
    rows = cast(list[dict[str, Any]], table.to_pylist())
    if canonical_rows_sha256(rows) != item.content_sha256 or path.stem != item.content_sha256:
        raise ArtifactConflictError(f"checkpoint Parquet content hash mismatch: {path}")
    if canonical_schema_sha256(table.schema) != item.schema_sha256:
        raise ArtifactConflictError(f"checkpoint Parquet schema hash mismatch: {path}")
    if len(rows) != item.row_count:
        raise ArtifactConflictError(f"checkpoint Parquet row-count mismatch: {path}")
    return rows, ContentAddressedArtifact(
        dataset=item.dataset,
        path=path,
        content_sha256=item.content_sha256,
        schema_sha256=item.schema_sha256,
        row_count=item.row_count,
    )


def load_r2b_validation_checkpoint(
    reference: R2BValidationCheckpointRef,
    *,
    expected_validation_code_sha256: str,
    expected_source_manifest_sha256: str,
    expected_source_manifest_file_sha256: str,
) -> R2LandingResult:
    """Re-read, re-hash, and reconstruct a checkpoint without source reparsing."""

    manifest = reference_manifest(reference)
    if manifest.validation_code_sha256 != expected_validation_code_sha256:
        raise ArtifactConflictError("checkpoint validation-code hash mismatch")
    if manifest.source_manifest_sha256 != expected_source_manifest_sha256:
        raise ArtifactConflictError("checkpoint source-manifest semantic hash mismatch")
    if manifest.source_manifest_file_sha256 != expected_source_manifest_file_sha256:
        raise ArtifactConflictError("checkpoint source-manifest file hash mismatch")

    lake_namespace = _lake_namespace(reference)
    manifest_item = next(
        item for item in manifest.source_native if item.dataset == "source.manifest"
    )
    manifest_path = _within_checkpoint_root(
        reference,
        _source_path(
            lake_namespace=lake_namespace,
            dataset=manifest_item.dataset,
            file_sha256_value=manifest_item.file_sha256,
            source_manifest=None,
        ),
    )
    if file_sha256(manifest_path) != manifest_item.file_sha256:
        raise ArtifactConflictError("checkpoint source-manifest bytes changed")
    source_manifest = R2SourceManifest.model_validate_json(manifest_path.read_bytes())
    if _canonical_object_sha256(source_manifest.model_dump(mode="json")) != (
        manifest.source_manifest_sha256
    ):
        raise ArtifactConflictError("checkpoint source-manifest semantic hash mismatch")

    source_paths = [manifest_path]
    expected_sources = {source.dataset: source for source in source_manifest.sources}
    for item in manifest.source_native:
        if item.dataset == "source.manifest":
            continue
        path = _within_checkpoint_root(
            reference,
            _source_path(
                lake_namespace=lake_namespace,
                dataset=item.dataset,
                file_sha256_value=item.file_sha256,
                source_manifest=source_manifest,
            ),
        )
        if file_sha256(path) != item.file_sha256 or path.stem != item.file_sha256:
            raise ArtifactConflictError(f"checkpoint source-native hash mismatch: {path}")
        if expected_sources[item.dataset].content_sha256 != item.file_sha256:
            raise ArtifactConflictError(f"checkpoint source identity mismatch: {item.dataset}")
        source_paths.append(path)
    if set(expected_sources) != {
        item.dataset for item in manifest.source_native if item.dataset != "source.manifest"
    }:
        raise ArtifactConflictError("checkpoint source-native dataset set mismatch")

    rows_by_dataset: dict[str, list[dict[str, Any]]] = {}
    artifacts: list[ContentAddressedArtifact] = []
    for validated_item in manifest.validated:
        path = _within_checkpoint_root(
            reference,
            _validated_path(lake_namespace=lake_namespace, item=validated_item),
        )
        rows, artifact = _validated_rows(path=path, item=validated_item)
        rows_by_dataset[validated_item.dataset] = rows
        artifacts.append(artifact)

    def records(dataset: str, model: type[Any]) -> tuple[Any, ...]:
        return tuple(
            model.model_validate_json(json.dumps(row, separators=(",", ":"), sort_keys=True))
            for row in rows_by_dataset[dataset]
        )

    return R2LandingResult(
        source_manifest=source_manifest,
        source_manifest_sha256=manifest.source_manifest_sha256,
        source_manifest_file_sha256=manifest.source_manifest_file_sha256,
        product_locations=records("validated.product_locations", R2ProductLocation),
        suppliers=records("validated.suppliers", R2Supplier),
        opening_inventory=records("validated.opening_inventory", R2OpeningInventory),
        fulfillment_outcomes=records("validated.fulfillment_outcomes", R2FulfillmentOutcome),
        purchase_orders=records("validated.purchase_orders", R2PurchaseOrder),
        receipts=records("validated.receipts", R2Receipt),
        inventory_adjustments=records("validated.inventory_adjustments", R2InventoryAdjustment),
        quarantined=records("quarantine.r2", R2QuarantineRecord),
        validated_artifacts=tuple(artifacts),
        source_native_paths=tuple(sorted(source_paths)),
    )


def checkpoint_governed_files(reference: R2BValidationCheckpointRef) -> tuple[Path, ...]:
    """Return immutable files whose mtimes must survive retries and reruns."""

    manifest = reference_manifest(reference)
    lake_namespace = _lake_namespace(reference)
    manifest_item = next(
        item for item in manifest.source_native if item.dataset == "source.manifest"
    )
    source_manifest_path = _within_checkpoint_root(
        reference,
        _source_path(
            lake_namespace=lake_namespace,
            dataset="source.manifest",
            file_sha256_value=manifest_item.file_sha256,
            source_manifest=None,
        ),
    )
    source_manifest = R2SourceManifest.model_validate_json(source_manifest_path.read_bytes())
    source_files = [
        _within_checkpoint_root(
            reference,
            _source_path(
                lake_namespace=lake_namespace,
                dataset=item.dataset,
                file_sha256_value=item.file_sha256,
                source_manifest=source_manifest,
            ),
        )
        for item in manifest.source_native
    ]
    validated_files = [
        _within_checkpoint_root(
            reference,
            _validated_path(lake_namespace=lake_namespace, item=item),
        )
        for item in manifest.validated
    ]
    return (
        reference.manifest_path,
        _within_checkpoint_root(reference, lake_namespace / "source-package.sha256"),
        *sorted(source_files),
        *sorted(validated_files),
    )
