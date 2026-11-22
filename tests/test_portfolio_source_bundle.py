"""Exact source retention/reconstruction and bounded untrusted-byte admission."""

import json
from pathlib import Path

import pytest

from nexus.portfolio import source_bundle
from nexus.portfolio.runtime_pipeline import runtime_source_fingerprint
from nexus.portfolio.source_bundle import SourceBundle
from nexus.portfolio.store import StoreError, canonical
from nexus.simulation import generate_r2_smoke_fixture


@pytest.fixture
def source(tmp_path: Path) -> Path:
    root = tmp_path / "producer"
    generate_r2_smoke_fixture(root)
    return root


def test_bundle_matches_runtime_identity_and_reconstructs_exact_bytes(
    source: Path, tmp_path: Path
) -> None:
    bundle = SourceBundle.capture(source)
    expected = runtime_source_fingerprint(source)
    assert bundle.source_sha256 == expected
    restored = bundle.materialize(tmp_path / "retained")
    assert bundle.materialize(restored) == restored
    assert runtime_source_fingerprint(restored) == expected
    for name, content in bundle.files().items():
        assert (source / name).read_bytes() == (restored / name).read_bytes() == content
    (source / "source-manifest.json").write_text("changed after capture", encoding="utf-8")
    assert runtime_source_fingerprint(bundle.materialize(tmp_path / "recovered")) == expected
    assert repr(bundle) == "SourceBundle()"


def test_existing_different_cache_is_never_overwritten(source: Path, tmp_path: Path) -> None:
    bundle = SourceBundle.capture(source)
    restored = bundle.materialize(tmp_path / "cache")
    target = restored / "source-manifest.json"
    target.write_bytes(b"preserve these bytes")
    with pytest.raises(StoreError, match="preserved existing bytes"):
        bundle.materialize(restored)
    assert target.read_bytes() == b"preserve these bytes"


@pytest.mark.parametrize(
    "kind", ["manifest", "file", "extra", "missing", "encoding", "shape", "contract", "canonical"]
)
def test_corrupt_bundle_is_rejected(source: Path, kind: str) -> None:
    value = json.loads(SourceBundle.capture(source).payload)
    filename = next(name for name in value["files"] if name != "source-manifest.json")
    if kind == "manifest":
        value["files"]["source-manifest.json"] = "e30="
    elif kind == "file":
        value["files"][filename] = "Y2hhbmdlZA=="
    elif kind == "extra":
        value["files"]["../outside"] = ""
    elif kind == "missing":
        del value["files"][filename]
    elif kind == "encoding":
        value["files"][filename] = "%%%"
    elif kind == "shape":
        value["files"] = []
    elif kind == "contract":
        value["contract"] = "other"
    payload = canonical(value) + (" " if kind == "canonical" else "")
    with pytest.raises(StoreError, match="bytes or manifest differ"):
        SourceBundle(payload).files()


def test_changed_input_and_size_budget_fail_before_publication(
    source: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = json.loads((source / "source-manifest.json").read_text())
    filename = manifest["sources"][0]["file_name"]
    with (source / filename).open("ab") as handle:
        handle.write(b"unexpected bytes")
    with pytest.raises(StoreError, match="bytes or manifest differ"):
        SourceBundle.capture(source)
    monkeypatch.setattr(source_bundle, "MAX_MANIFEST_BYTES", 16)
    with pytest.raises(StoreError, match="byte budget"):
        SourceBundle.capture(source)


@pytest.mark.parametrize("path", [Path("relative"), Path("C:/"), Path("C:/some/../path")])
def test_unsafe_source_roots_fail_before_reading(path: Path) -> None:
    with pytest.raises(StoreError, match="unlinked local"):
        SourceBundle.capture(path)
