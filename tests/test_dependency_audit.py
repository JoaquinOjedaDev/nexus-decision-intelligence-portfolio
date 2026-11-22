"""A profile assessment cannot hide unrelated, expired, or unscanned findings."""

import copy
import json
from datetime import date

import pytest

from scripts.check_dependency_audit import DEFAULT_POLICY, evaluate


def _report() -> dict:
    return {
        "dependencies": [
            {
                "name": "mlflow",
                "version": "3.17.2",
                "vulns": [{"id": "PYSEC-2026-3865", "aliases": ["CVE-2026-71211"]}],
            }
        ]
    }


def _scoped_policy() -> dict:
    # Exercise assessment handling without installing a release-policy exemption.
    policy = json.loads(DEFAULT_POLICY.read_text())
    policy["assessments"] = [
        {
            "package": "mlflow",
            "versions": ["3.17.2"],
            "advisories": ["PYSEC-2026-3865"],
            "profile": "local-sqlite-sdk",
            "status": "not_affected",
            "review_by": "2026-11-22",
            "reason": "Test-only assessment for an unreachable server feature",
            "evidence": ["test-only"],
        }
    ]
    return policy


def test_current_assessment_retains_raw_finding_and_has_explicit_scope() -> None:
    result = evaluate(
        _report(),
        _scoped_policy(),
        profile="local-sqlite-sdk",
        today=date(2026, 10, 23),
    )
    assert result["status"] == "PASS"
    assert result["raw_scanner_clean"] is False
    assert len(result["assessed_not_affected"]) == 1


@pytest.mark.parametrize("change", ["profile", "version", "advisory", "expired"])
def test_scope_changes_fail_closed(change: str) -> None:
    report = _report()
    profile = "local-sqlite-sdk"
    today = date(2026, 10, 23)
    if change == "profile":
        profile = "http-gateway"
    elif change == "version":
        report["dependencies"][0]["version"] = "3.15.0"
    elif change == "advisory":
        report["dependencies"][0]["vulns"] = [{"id": "UNREVIEWED"}]
    else:
        today = date(2026, 11, 23)
    result = evaluate(report, _scoped_policy(), profile=profile, today=today)
    assert result["status"] == "FAIL"
    assert len(result["unresolved"]) == 1


def test_duplicate_records_are_counted_once_and_new_findings_are_not_hidden() -> None:
    report = _report()
    report["dependencies"][0]["vulns"] *= 2
    report["dependencies"].append(
        {"name": "another", "version": "1", "vulns": [{"id": "UNREVIEWED"}]}
    )
    result = evaluate(
        report,
        _scoped_policy(),
        profile="local-sqlite-sdk",
        today=date(2026, 10, 23),
    )
    assert len(result["assessed_not_affected"]) == len(result["unresolved"]) == 1


def test_empty_or_skipped_package_audits_are_refused() -> None:
    policy = json.loads(DEFAULT_POLICY.read_text())
    for report in ({"dependencies": []}, copy.deepcopy(_report())):
        if report["dependencies"]:
            report["dependencies"][0]["skip_reason"] = "unavailable metadata"
        with pytest.raises(ValueError):
            evaluate(report, policy, profile="local-sqlite-sdk", today=date(2026, 10, 23))


def test_release_policy_has_no_suppression_and_rejects_any_known_finding() -> None:
    policy = json.loads(DEFAULT_POLICY.read_text())
    assert policy["assessments"] == []
    result = evaluate(_report(), policy, profile="local-sqlite-sdk", today=date(2026, 10, 23))
    assert result["status"] == "FAIL"
    assert result["assessed_not_affected"] == []
