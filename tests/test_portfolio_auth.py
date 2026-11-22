"""Local JWT boundaries and durable identity binding, without an external issuer."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from nexus.portfolio.auth import (
    JWT_ALGORITHM,
    JWT_AUDIENCE,
    JWT_ISSUER,
    JWT_REQUIRED_CLAIMS,
    SESSION_TTL_SECONDS,
    SessionCodec,
)
from nexus.portfolio.store import PortfolioStore, StoreError

jwt = pytest.importorskip("jwt")

KEY = b"jwt-local-security-fixture-key-32bytes"
NOW = 1_800_000_000


def claims(**changes: Any) -> dict[str, Any]:
    return {
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "sub": "approver.portfolio",
        "sid": "a" * 64,
        "jti": "b" * 64,
        "iat": NOW,
        "nbf": NOW,
        "exp": NOW + SESSION_TTL_SECONDS,
        **changes,
    }


def token(payload: dict[str, Any], **kwargs: Any) -> str:
    return jwt.encode(payload, KEY, algorithm=JWT_ALGORITHM, **kwargs)


def test_issued_session_is_an_interoperable_jwt_with_all_identity_claims() -> None:
    encoded = SessionCodec(KEY).sign(
        "a" * 64,
        NOW + SESSION_TTL_SECONDS,
        subject="approver.portfolio",
        token_id="b" * 64,
        issued_at=NOW,
    )
    assert len(encoded.split(".")) == 3
    decoded = jwt.decode(
        encoded,
        KEY,
        algorithms=[JWT_ALGORITHM],
        issuer=JWT_ISSUER,
        audience=JWT_AUDIENCE,
        options={"verify_iat": False, "verify_nbf": False, "verify_exp": False},
    )
    assert decoded == claims()
    checked = SessionCodec(KEY).verify(encoded, now=NOW)
    assert checked is not None
    assert checked.subject == "approver.portfolio"
    assert checked.token_id == "b" * 64


@pytest.mark.parametrize("algorithm", ["none", "HS384", "HS512"])
def test_algorithm_is_pinned_not_selected_from_token(algorithm: str) -> None:
    # A longer wrong-algorithm key avoids irrelevant weak-key warnings.
    signing_key = None if algorithm == "none" else b"x" * 64
    encoded = jwt.encode(claims(), signing_key, algorithm=algorithm)
    assert SessionCodec(KEY).verify(encoded, now=NOW) is None


def test_wrong_signature_is_rejected() -> None:
    encoded = jwt.encode(claims(), b"z" * 32, algorithm=JWT_ALGORITHM)
    assert SessionCodec(KEY).verify(encoded, now=NOW) is None


@pytest.mark.parametrize(
    "changes",
    [
        {"iss": "urn:another:issuer"},
        {"aud": "urn:another:service"},
        {"aud": [JWT_AUDIENCE]},
        {"sub": ""},
        {"sub": 123},
        {"sid": "invalid-session"},
        {"jti": "invalid-token"},
        {"sid": "A" * 64},
        {"iat": True},
        {"nbf": str(NOW)},
        {"exp": float(NOW + SESSION_TTL_SECONDS)},
        {"iat": NOW + 1, "nbf": NOW + 1, "exp": NOW + 1 + SESSION_TTL_SECONDS},
        {"nbf": NOW + 1},
        {"iat": NOW - 1, "nbf": NOW - 1, "exp": NOW + SESSION_TTL_SECONDS},
        {"role": "system"},
        {"locations": ["WH-UNAUTHORIZED"]},
    ],
)
def test_wrong_identity_time_types_and_client_privileges_are_rejected(
    changes: dict[str, Any],
) -> None:
    assert SessionCodec(KEY).verify(token(claims(**changes)), now=NOW) is None


@pytest.mark.parametrize("missing", JWT_REQUIRED_CLAIMS)
def test_every_registered_binding_claim_is_required(missing: str) -> None:
    payload = claims()
    payload.pop(missing)
    assert SessionCodec(KEY).verify(token(payload), now=NOW) is None


@pytest.mark.parametrize("header", [{"typ": "at+jwt"}, {"kid": "untrusted"}, {"crit": ["x"]}])
def test_unexpected_jose_headers_are_rejected(header: dict[str, Any]) -> None:
    assert SessionCodec(KEY).verify(token(claims(), headers=header), now=NOW) is None


def test_expiry_boundary_and_nonfinite_clock_fail_closed() -> None:
    encoded = token(claims())
    codec = SessionCodec(KEY)
    assert codec.verify(encoded, now=NOW + SESSION_TTL_SECONDS - 0.01) is not None
    assert codec.verify(encoded, now=NOW + SESSION_TTL_SECONDS) is None
    assert codec.verify(encoded, now=NOW - 0.01) is None
    assert codec.verify(encoded, now=float("nan")) is None
    assert codec.verify(encoded, now=float("inf")) is None


@pytest.fixture
def store(tmp_path: Path) -> PortfolioStore:
    value = PortfolioStore(tmp_path, clock=lambda: float(NOW))
    value.ensure_seed(
        {
            "recommendations": [
                {
                    "id": f"rec-{index}",
                    "location_id": location,
                    "recommended_quantity": 10,
                    "created_by": "planner.r4a",
                }
                for index, location in enumerate(("WH-NORTH", "WH-SOUTH"), 1)
            ]
        }
    )
    return value


def _stored_claims(store: PortfolioStore, encoded: str) -> tuple[dict[str, Any], bytes]:
    with store.connection() as db:
        secret = bytes.fromhex(
            db.execute("SELECT value FROM metadata WHERE key='session_key'").fetchone()[0]
        )
    payload = jwt.decode(
        encoded,
        secret,
        algorithms=[JWT_ALGORITHM],
        issuer=JWT_ISSUER,
        audience=JWT_AUDIENCE,
        options={"verify_iat": False, "verify_nbf": False, "verify_exp": False},
    )
    return payload, secret


@pytest.mark.parametrize(
    "changes",
    [
        {"sub": "planner.r4a"},
        {"jti": "c" * 64},
        {"sid": "d" * 64},
        {"iat": NOW - 1, "nbf": NOW - 1, "exp": NOW - 1 + SESSION_TTL_SECONDS},
    ],
)
def test_valid_signature_is_insufficient_without_exact_persisted_binding(
    store: PortfolioStore, changes: dict[str, Any]
) -> None:
    encoded, _ = store.create_session("approver")
    payload, secret = _stored_claims(store, encoded)
    altered = jwt.encode({**payload, **changes}, secret, algorithm=JWT_ALGORITHM)
    assert SessionCodec(secret).verify(altered, now=NOW) is not None
    assert store.session(altered) is None


def test_database_persona_cannot_change_subject_of_existing_token(store: PortfolioStore) -> None:
    encoded, _ = store.create_session("approver")
    with store.transaction() as db:
        db.execute("UPDATE sessions SET persona='planner'")
    assert store.session(encoded) is None


def test_unknown_database_persona_is_denied(store: PortfolioStore) -> None:
    encoded, _ = store.create_session("approver")
    with store.transaction() as db:
        db.execute("UPDATE sessions SET persona='invented-admin'")
    assert store.session(encoded) is None


def test_revocation_survives_restart_and_new_login_does_not_revive_old_token(
    store: PortfolioStore, tmp_path: Path
) -> None:
    encoded, _ = store.create_session("approver")
    session = store.session(encoded)
    assert session is not None
    store.revoke_session(session[2])
    restarted = PortfolioStore(tmp_path, clock=lambda: float(NOW))
    fresh, _ = restarted.create_session("approver")
    assert restarted.session(encoded) is None
    assert restarted.session(fresh) is not None
    assert restarted.metrics_snapshot()["event_count"] == 2


def test_jwt_actor_uses_server_location_scope_and_denies_cross_location(
    store: PortfolioStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    encoded, _ = store.create_session("approver")
    monkeypatch.setattr(store, "locations", lambda: ("WH-NORTH",))
    session = store.session(encoded)
    assert session is not None
    authenticated = session[0]
    assert authenticated.locations == ("WH-NORTH",)
    assert [item["id"] for item in store.recommendations(authenticated)] == ["rec-1"]
    with pytest.raises(StoreError) as denied:
        store.recommendation("rec-2", authenticated)
    assert denied.value.status == 403


def test_legacy_cookie_is_rejected_without_removing_old_sessions_or_workflow(
    store: PortfolioStore, tmp_path: Path
) -> None:
    session_id = "e" * 64
    with store.transaction() as db:
        db.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,0)",
            (session_id, "approver", "legacy-csrf", NOW + SESSION_TTL_SECONDS),
        )
        secret = bytes.fromhex(
            db.execute("SELECT value FROM metadata WHERE key='session_key'").fetchone()[0]
        )
        # Model the pre-JWT database schema. Only the new empty migration table is removed.
        db.execute("DROP TABLE session_tokens")
    payload = (
        base64.urlsafe_b64encode(
            json.dumps({"sid": session_id, "exp": NOW + SESSION_TTL_SECONDS, "v": 1}).encode()
        )
        .decode()
        .rstrip("=")
    )
    signature = (
        base64.urlsafe_b64encode(hmac.new(secret, payload.encode(), hashlib.sha256).digest())
        .decode()
        .rstrip("=")
    )
    restarted = PortfolioStore(tmp_path, clock=lambda: float(NOW))
    assert restarted.session(f"{payload}.{signature}") is None
    new_token, _ = restarted.create_session("approver")
    assert restarted.session(new_token) is not None
    assert restarted.metrics_snapshot()["event_count"] == 2
    with restarted.connection() as db:
        assert db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 2


def test_base_cli_help_and_doctor_do_not_import_portfolio_dependencies() -> None:
    script = """
import importlib.abc
import sys

class BlockPortfolioDependencies(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'jwt', 'fastapi', 'uvicorn', 'ortools'}:
            raise ModuleNotFoundError('Blocked optional dependency: ' + fullname)
        return None

sys.meta_path.insert(0, BlockPortfolioDependencies())
from typer.testing import CliRunner
from nexus import cli
cli._command_status = lambda command: (False, 'optional diagnostic disabled in regression')
for arguments in (['--help'], ['doctor']):
    result = CliRunner().invoke(cli.app, arguments)
    assert result.exit_code == 0, str(result.exception) + result.output
"""
    result = subprocess.run(
        [sys.executable, "-c", script], check=False, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stdout + result.stderr
