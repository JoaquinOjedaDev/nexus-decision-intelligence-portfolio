"""Content-addressed artifacts shared by governed data pipelines."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq


class ArtifactConflictError(RuntimeError):
    """An existing hash-named artifact does not contain the named content."""


@dataclass(frozen=True, slots=True)
class ContentAddressedArtifact:
    """Reference to one immutable Parquet artifact."""

    dataset: str
    path: Path
    content_sha256: str
    schema_sha256: str
    row_count: int


def canonical_rows_sha256(rows: Sequence[Mapping[str, object]]) -> str:
    """Hash logical rows without platform-specific Parquet metadata."""

    encoded = json.dumps(
        rows,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def file_sha256(path: Path) -> str:
    """Hash source-native bytes without loading a complete file into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_schema_sha256(schema: pa.Schema) -> str:
    """Hash the complete physical Arrow schema independently from row content."""

    def metadata(value: Mapping[bytes, bytes] | None) -> dict[str, str]:
        return (
            {
                key.hex(): item.hex()
                for key, item in sorted(value.items(), key=lambda entry: entry[0])
            }
            if value
            else {}
        )

    schema_identity: dict[str, object] = {
        "fields": [
            {
                "metadata": metadata(field.metadata),
                "name": field.name,
                "nullable": field.nullable,
                "type": str(field.type),
            }
            for field in schema
        ],
        "metadata": metadata(schema.metadata),
    }
    return canonical_rows_sha256([schema_identity])


def write_immutable_parquet(
    output_dir: Path,
    *,
    dataset: str,
    rows: Sequence[Mapping[str, object]],
    schema: pa.Schema | None = None,
) -> ContentAddressedArtifact:
    """Atomically write canonical rows to a content-addressed Parquet path."""

    logical_rows = [dict(row) for row in rows]
    content_sha256 = canonical_rows_sha256(logical_rows)
    segments = dataset.split(".")
    if not segments or any(
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", segment) is None for segment in segments
    ):
        raise ValueError(f"invalid artifact dataset name: {dataset!r}")
    output_root = output_dir.resolve()
    target_dir = output_root.joinpath(*segments)
    if not target_dir.resolve().is_relative_to(output_root):
        raise ValueError(f"artifact dataset escapes output directory: {dataset!r}")
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{content_sha256}.parquet"
    table = pa.Table.from_pylist(logical_rows, schema=schema)

    if target.exists():
        existing_table = pq.read_table(target)
        existing_rows = cast(list[dict[str, object]], existing_table.to_pylist())
        if canonical_rows_sha256(existing_rows) != content_sha256:
            raise ArtifactConflictError(f"content-addressed artifact is corrupt: {target}")
        if not existing_table.schema.equals(table.schema, check_metadata=True):
            raise ArtifactConflictError(
                f"content-addressed artifact has an incompatible schema: {target}"
            )
    else:
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                prefix=f".{content_sha256}.",
                suffix=".tmp",
                dir=target_dir,
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
            pq.write_table(
                table,
                temporary_path,
                compression="zstd",
                version="2.6",
                use_dictionary=True,
            )
            os.replace(temporary_path, target)
            temporary_path = None
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    return ContentAddressedArtifact(
        dataset=dataset,
        path=target,
        content_sha256=content_sha256,
        schema_sha256=canonical_schema_sha256(table.schema),
        row_count=len(logical_rows),
    )
