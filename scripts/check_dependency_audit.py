"""Evaluate raw pip-audit findings against explicit, expiring profile assessments."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

DEFAULT_POLICY = Path(__file__).resolve().parents[1] / "configs/security/dependency-policy.json"


def evaluate(
    report: dict[str, Any], policy: dict[str, Any], *, profile: str, today: date
) -> dict[str, Any]:
    if policy.get("contract") != "nexus.dependency-assessment.v1":
        raise ValueError("unsupported dependency assessment policy")
    dependencies = report.get("dependencies")
    if not isinstance(dependencies, list) or not dependencies:
        raise ValueError("dependency audit must contain audited packages")
    assessments = policy.get("assessments")
    if not isinstance(assessments, list):
        raise ValueError("dependency policy assessments are missing")
    unresolved: list[dict[str, str]] = []
    assessed: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for package in dependencies:
        if package.get("skip_reason") or "version" not in package or "vulns" not in package:
            raise ValueError("audit contains an unaudited package")
        for finding in package["vulns"]:
            name, version, advisory = package["name"], package["version"], finding["id"]
            key = (name, version, advisory)
            if key in seen:
                continue
            seen.add(key)
            identifiers = {advisory, *finding.get("aliases", [])}
            matching = [
                item
                for item in assessments
                if item["package"] == name
                and version in item["versions"]
                and item["profile"] == profile
                and item["status"] == "not_affected"
                and identifiers.intersection(item["advisories"])
                and today <= date.fromisoformat(item["review_by"])
                and item.get("reason")
                and item.get("evidence")
            ]
            result = {"package": name, "version": version, "advisory": advisory}
            (assessed if len(matching) == 1 else unresolved).append(result)
    return {
        "contract": "nexus.dependency-audit-result.v1",
        "profile": profile,
        "evaluated_on": today.isoformat(),
        "audited_packages": len(dependencies),
        "assessed_not_affected": assessed,
        "unresolved": unresolved,
        "status": "PASS" if not unresolved else "FAIL",
        "raw_scanner_clean": not seen,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = evaluate(
        json.loads(args.input.read_text(encoding="utf-8")),
        json.loads(args.policy.read_text(encoding="utf-8")),
        profile=args.profile,
        today=datetime.now(UTC).date(),
    )
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        with args.output.open("x", encoding="utf-8") as handle:
            handle.write(rendered)
    print(rendered, end="")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
