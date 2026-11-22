"""The diagnostic harness must not overwrite state or change production functions."""

from types import SimpleNamespace

import pytest

from scripts import profile_runtime_planning as profile


def test_fresh_output_preserves_existing_files_and_rejects_outside(tmp_path):
    owned = tmp_path / "artifacts" / "profile"
    assert profile.fresh_output(tmp_path, owned) == owned
    marker = owned / "keep.txt"
    marker.write_text("keep", encoding="utf-8")
    with pytest.raises(ValueError):
        profile.fresh_output(tmp_path, owned)
    assert marker.read_text() == "keep"
    for denied in (tmp_path, tmp_path / "outside", tmp_path / "artifacts"):
        with pytest.raises(ValueError):
            profile.fresh_output(tmp_path, denied)


@pytest.mark.parametrize("fail", [False, True])
def test_stage_wrappers_restore_original_even_on_error(monkeypatch, fail):
    def original():
        if fail:
            raise ValueError("fixture failure")
        return 42

    module = SimpleNamespace(operation=original)
    monkeypatch.setattr(profile, "STAGES", ((module, "operation"),))
    rows = []
    if fail:
        with pytest.raises(ValueError, match="fixture failure"), profile.timed_stages(rows):
            module.operation()
    else:
        with profile.timed_stages(rows):
            assert module.operation() == 42
    assert module.operation is original
    assert len(rows) == 1 and rows[0]["passed"] is not fail
    assert rows[0]["seconds"] >= 0


@pytest.mark.parametrize("iterations", [True, 0, 21, -1, 1.5])
def test_invalid_count_does_not_create_output(tmp_path, iterations):
    output = tmp_path / "artifacts" / "profile"
    with pytest.raises(ValueError):
        profile.profile(tmp_path, output, iterations)
    assert not output.exists()
