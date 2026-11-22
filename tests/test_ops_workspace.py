"""Fresh-stack preparation refuses reuse before creating private credentials."""

import json
from pathlib import Path

import pytest

from scripts import prepare_ops_workspace as workspace

IMAGE = "sha256:" + "a" * 64


def environment(monkeypatch: pytest.MonkeyPatch, existing: str = "") -> list[list[str]]:
    calls: list[list[str]] = []

    def command(args: list[str]) -> str:
        calls.append(args)
        if args[1:3] == ["image", "inspect"]:
            return json.dumps(
                [
                    {
                        "Id": IMAGE,
                        "Os": "linux",
                        "Architecture": "amd64",
                        "Config": {"User": "10001:10001"},
                    }
                ]
            )
        return existing

    monkeypatch.setattr(workspace, "command", command)
    monkeypatch.setattr(workspace, "protect", lambda path: None)
    return calls


def test_preparation_is_fresh_private_and_does_not_start_services(tmp_path, monkeypatch):
    calls = environment(monkeypatch)
    result = workspace.prepare(tmp_path / "artifacts" / "new", IMAGE, project=tmp_path)
    secret = Path(result["bootstrap_password_file"]).read_text().strip()
    assert len(secret) == 64 and int(secret, 16) >= 0
    assert Path(result["bootstrap_password_file"]).read_bytes() == secret.encode() + b"\n"
    assert secret not in json.dumps(result)
    assert result["services_started"] is False
    assert len(calls) == 4
    assert all("up" not in call and "run" not in call for call in calls)
    assert calls[0][3] == "--all"
    assert calls[1][-1] == "{{.Name}}"
    with pytest.raises(ValueError, match="fresh"):
        workspace.prepare(tmp_path / "artifacts" / "new", IMAGE, project=tmp_path)
    assert Path(result["bootstrap_password_file"]).read_text().strip() == secret


def test_existing_stack_refused_without_writing(tmp_path, monkeypatch):
    environment(monkeypatch, "existing-owned-resource")
    output = tmp_path / "artifacts" / "new"
    with pytest.raises(ValueError, match="already exist"):
        workspace.prepare(output, IMAGE, project=tmp_path)
    assert not output.exists()


@pytest.mark.parametrize("image", ["latest", "repository:tag", "sha256:" + "A" * 64, ""])
def test_mutable_or_noncanonical_images_refused(tmp_path, monkeypatch, image):
    calls = environment(monkeypatch)
    with pytest.raises(ValueError, match="exact locally verified"):
        workspace.prepare(tmp_path / "artifacts" / "new", image, project=tmp_path)
    assert not calls


def test_outside_workspace_and_artifact_root_refused(tmp_path, monkeypatch):
    calls = environment(monkeypatch)
    for path in (tmp_path / "outside", tmp_path / "artifacts"):
        with pytest.raises(ValueError, match="strictly inside"):
            workspace.prepare(path, IMAGE, project=tmp_path)
    assert not calls


def test_acl_failure_prevents_secret_creation(tmp_path, monkeypatch):
    environment(monkeypatch)

    def fail(path):
        raise RuntimeError("ACL failed")

    monkeypatch.setattr(workspace, "protect", fail)
    output = tmp_path / "artifacts" / "new"
    with pytest.raises(RuntimeError, match="ACL failed"):
        workspace.prepare(output, IMAGE, project=tmp_path)
    assert not list(output.iterdir())


def test_fresh_named_attempt_checks_only_its_own_resources(tmp_path, monkeypatch):
    calls = environment(monkeypatch)
    name = "nexus-flagship-ops-20260927-b"
    result = workspace.prepare(tmp_path / "artifacts" / "new", IMAGE, project=tmp_path, stack=name)
    assert result["project"] == name
    assert all(f"label=com.docker.compose.project={name}" in call for call in calls[:3])


@pytest.mark.parametrize("name", ["default", "other-project", "nexus-flagship-ops-../escape"])
def test_unrelated_or_invalid_project_name_refused(tmp_path, monkeypatch, name):
    calls = environment(monkeypatch)
    with pytest.raises(ValueError, match="isolated ops project"):
        workspace.prepare(tmp_path / "artifacts" / "new", IMAGE, project=tmp_path, stack=name)
    assert not calls
