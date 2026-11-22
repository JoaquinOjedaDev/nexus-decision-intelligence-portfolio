"""Prepare fresh private local Docker-test credentials; never start or reset services."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import secrets
import subprocess
from pathlib import Path
from typing import Any

PROJECT = Path(__file__).resolve().parents[1]
STACK = "nexus-flagship-ops-20260925"


def command(arguments: list[str]) -> str:
    result = subprocess.run(arguments, capture_output=True, text=True, timeout=30, check=False)
    if result.returncode:
        raise RuntimeError("Local prerequisite command failed; no services were started")
    return result.stdout.strip()


def protect(path: Path) -> None:
    if os.name != "nt":
        path.chmod(0o700)
        return
    identity = next(csv.reader(io.StringIO(command(["whoami.exe", "/user", "/fo", "csv", "/nh"]))))
    sid = identity[1]
    if not re.fullmatch(r"S-1-[0-9-]+", sid):
        raise ValueError("Cannot establish private workspace ownership")
    command(
        [
            "icacls.exe",
            str(path),
            "/inheritance:r",
            "/grant:r",
            f"*{sid}:(OI)(CI)F",
            "*S-1-5-18:(OI)(CI)F",
        ]
    )


def write_bootstrap_password(path: Path) -> None:
    """Create a fresh generated secret with Linux-compatible bytes on every host."""
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(secrets.token_hex(32) + "\n")


def prepare(
    path: Path, image: str, *, project: Path = PROJECT, stack: str = STACK
) -> dict[str, Any]:
    if not re.fullmatch(r"nexus-flagship-ops-[a-z0-9][a-z0-9-]{0,31}", stack):
        raise ValueError("Use an explicit NEXUS isolated ops project name")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image):
        raise ValueError("Use the exact locally verified image ID, not a tag")
    candidate = path.absolute()
    artifacts = (project / "artifacts").resolve()
    if (
        ".." in candidate.parts
        or candidate.anchor.startswith(("\\\\", "//"))
        or any(p.is_symlink() or p.is_junction() for p in (*candidate.parents, candidate))
        or not candidate.resolve().is_relative_to(artifacts)
        or candidate.resolve() == artifacts
        or candidate.exists()
    ):
        raise ValueError("Use a fresh unlinked directory strictly inside project artifacts")
    for kind in ("container", "volume", "network"):
        field = "{{.Name}}" if kind == "volume" else "{{.ID}}"
        existing = command(
            [
                "docker",
                kind,
                "ls",
                "--filter",
                f"label=com.docker.compose.project={stack}",
                "--format",
                field,
            ]
            if kind != "container"
            else [
                "docker",
                "container",
                "ls",
                "--all",
                "--filter",
                f"label=com.docker.compose.project={stack}",
                "--format",
                field,
            ]
        )
        if existing:
            raise ValueError("Owned stack resources already exist; explicit recovery is required")
    inspected = json.loads(command(["docker", "image", "inspect", image]))
    if (
        len(inspected) != 1
        or inspected[0]["Id"] != image
        or inspected[0]["Os"] != "linux"
        or inspected[0]["Architecture"] != "amd64"
        or inspected[0]["Config"]["User"] != "10001:10001"
    ):
        raise ValueError("Candidate image identity or runtime user differs")
    candidate.mkdir(parents=True, exist_ok=False)
    protect(candidate)
    password_path = candidate / "postgres-bootstrap-password.txt"
    write_bootstrap_password(password_path)
    receipt = {
        "contract": "nexus.isolated-ops-workspace.v1",
        "project": stack,
        "image": image,
        "database": "nexus_portfolio_test_ops",
        "bootstrap_password_file": str(password_path),
        "services_started": False,
    }
    with (candidate / "workspace.json").open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=2)
        handle.write("\n")
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--stack", default=STACK)
    args = parser.parse_args()
    print(json.dumps(prepare(args.output, args.image, stack=args.stack)))


if __name__ == "__main__":
    main()
