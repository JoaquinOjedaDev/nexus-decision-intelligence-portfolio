"""Bounded immutable R2 source bytes for a recoverable publication outbox.

Database retention is authoritative. Materialized files are a verified cache;
recovery can recreate them without relying on the original producer directory.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

from nexus.data.r2_models import R2SourceManifest
from nexus.modeling.r3_artifacts import canonical_json_sha256
from nexus.portfolio.store import StoreError, canonical

MAX_SOURCE_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 256 * 1024


def _safe_directory(root: Path) -> Path:
    if (
        not root.is_absolute()
        or root == Path(root.anchor)
        or ".." in root.parts
        or root.anchor.startswith(("\\\\", "//"))
        or any(p.is_symlink() or p.is_junction() for p in (*root.parents, root))
    ):
        raise StoreError("invalid_path", "Use an explicit unlinked local source directory", 422)
    return root.resolve()


def _read(path: Path, limit: int) -> bytes:
    if path.is_symlink() or path.is_junction() or not path.is_file():
        raise StoreError("invalid_source", "Source files must be regular unlinked files", 422)
    with path.open("rb") as handle:
        value = handle.read(limit + 1)
    if len(value) > limit:
        raise StoreError(
            "source_too_large", "Source bundle exceeds its declared local byte budget", 422
        )
    return value


@dataclass(frozen=True)
class SourceBundle:
    payload: str = field(repr=False)

    def files(self) -> dict[str, bytes]:
        try:
            if not isinstance(self.payload, str) or len(self.payload) > 2 * MAX_SOURCE_BYTES:
                raise ValueError
            value = json.loads(self.payload)
            if (
                not isinstance(value, dict)
                or set(value) != {"contract", "files"}
                or value["contract"] != "nexus.source-bundle.v1"
            ):
                raise ValueError
            encoded = value["files"]
            if not isinstance(encoded, dict) or not 2 <= len(encoded) <= 65:
                raise ValueError
            files = {k: base64.b64decode(v, validate=True) for k, v in encoded.items()}
            manifest_bytes = files["source-manifest.json"]
            if (
                len(manifest_bytes) > MAX_MANIFEST_BYTES
                or sum(map(len, files.values())) > MAX_SOURCE_BYTES
            ):
                raise ValueError
            manifest = R2SourceManifest.model_validate_json(manifest_bytes)
            if set(files) != {"source-manifest.json", *(s.file_name for s in manifest.sources)}:
                raise ValueError
            if any(
                hashlib.sha256(files[s.file_name]).hexdigest() != s.content_sha256
                for s in manifest.sources
            ):
                raise ValueError
            if canonical(value) != self.payload or any(
                base64.b64encode(v).decode("ascii") != encoded[k] for k, v in files.items()
            ):
                raise ValueError
            return files
        except (ValueError, KeyError, TypeError, AttributeError):
            raise StoreError(
                "invalid_source", "Source bundle bytes or manifest differ", 422
            ) from None

    @property
    def source_sha256(self) -> str:
        files = self.files()
        return canonical_json_sha256(
            {
                "manifest_file_sha256": hashlib.sha256(files["source-manifest.json"]).hexdigest(),
                "files": [
                    {"name": name, "sha256": hashlib.sha256(files[name]).hexdigest()}
                    for name in sorted(files)
                    if name != "source-manifest.json"
                ],
            }
        )

    @classmethod
    def capture(cls, root: Path) -> SourceBundle:
        root = _safe_directory(root)
        manifest_bytes = _read(root / "source-manifest.json", MAX_MANIFEST_BYTES)
        manifest = R2SourceManifest.model_validate_json(manifest_bytes)
        if len(manifest.sources) > 64:
            raise StoreError("source_too_large", "Source bundle has too many files", 422)
        values = {"source-manifest.json": manifest_bytes}
        remaining = MAX_SOURCE_BYTES - len(manifest_bytes)
        for item in manifest.sources:
            if item.file_name == "source-manifest.json":
                raise StoreError("invalid_source", "Manifest cannot replace itself", 422)
            values[item.file_name] = _read(root / item.file_name, remaining)
            remaining -= len(values[item.file_name])
        result = cls(
            canonical(
                {
                    "contract": "nexus.source-bundle.v1",
                    "files": {
                        name: base64.b64encode(value).decode("ascii")
                        for name, value in values.items()
                    },
                }
            )
        )
        result.files()
        return result

    def materialize(self, root: Path) -> Path:
        root = _safe_directory(root)
        files = self.files()
        root.mkdir(parents=True, exist_ok=True)
        # Publish the manifest last. A partial directory is not admitted by the pipeline.
        for name in sorted(files, key=lambda n: (n == "source-manifest.json", n)):
            destination = root / name
            if destination.exists() or destination.is_symlink():
                if _read(destination, MAX_SOURCE_BYTES) != files[name]:
                    raise StoreError(
                        "source_conflict", "Retained source cache differs; preserved existing bytes"
                    )
                continue
            temporary = root / (".publication-" + uuid4().hex + ".tmp")
            try:
                with temporary.open("xb") as handle:
                    handle.write(files[name])
                    handle.flush()
                    os.fsync(handle.fileno())
                try:
                    os.link(temporary, destination)
                except FileExistsError:
                    if _read(destination, MAX_SOURCE_BYTES) != files[name]:
                        raise StoreError(
                            "source_conflict", "Concurrent source cache differs"
                        ) from None
            finally:
                temporary.unlink(missing_ok=True)
        if hasattr(os, "O_DIRECTORY"):
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        return root
