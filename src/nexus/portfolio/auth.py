"""Expiring, signed sessions for an explicitly local educational sandbox.

Personas are owned by the server. This is not production identity management;
remote deployments must replace the fixture login with a real identity provider.
"""

from __future__ import annotations

import math
import re
import secrets
from dataclasses import dataclass
from typing import Any

from nexus.contracts import ActorRole

COOKIE_NAME = "nexus_portfolio_session"
SESSION_TTL_SECONDS = 3600
JWT_ALGORITHM = "HS256"
JWT_ISSUER = "urn:nexus:portfolio:local-test-issuer"
JWT_AUDIENCE = "urn:nexus:portfolio:local-api"
JWT_REQUIRED_CLAIMS = ("iss", "aud", "sub", "sid", "jti", "iat", "nbf", "exp")
TOKEN_ID_PATTERN = re.compile(r"[0-9a-f]{64}")
PERSONA_NAMES = {
    "planner": "Alex Morgan",
    "approver": "Sam Rivera",
    "auditor": "Jordan Lee",
}


@dataclass(frozen=True)
class Actor:
    """Authenticated identity and warehouse scope resolved on the server."""

    actor_id: str
    name: str
    role: ActorRole
    locations: tuple[str, ...]

    def public(self) -> dict[str, Any]:
        return {
            "id": self.actor_id,
            "name": self.name,
            "role": self.role.value,
            "locations": list(self.locations),
        }


def resolve_persona(persona: str, locations: tuple[str, ...]) -> Actor:
    """Map a fixture name to privileges; never accept a client-supplied role."""

    if persona not in PERSONA_NAMES:
        raise ValueError("Unknown demo persona")
    return Actor(
        actor_id="planner.r4a" if persona == "planner" else f"{persona}.portfolio",
        name=PERSONA_NAMES[persona],
        role=ActorRole(persona),
        locations=locations,
    )


@dataclass(frozen=True)
class SessionClaims:
    """Verified local JWT identity; privileges still come from server state."""

    subject: str
    session_id: str
    token_id: str
    issued_at: int
    not_before: int
    expires_at: int


class SessionCodec:
    """Standards-compliant JWTs from a local test issuer, not a full OIDC provider.

    PyJWT validates the signature, pinned algorithm, issuer, audience, and claim
    presence. NumericDate checks use the same trusted clock as the workflow so
    deterministic maturity/expiry tests never depend on the machine's wall clock.
    No token can provide its own verification key, algorithm, roles, or scopes.
    """

    def __init__(self, secret: bytes) -> None:
        if len(secret) < 32:
            raise ValueError("A session key must have at least 256 bits")
        self.secret = secret

    def sign(
        self,
        session_id: str,
        expires_at: int,
        *,
        subject: str,
        token_id: str,
        issued_at: int,
    ) -> str:
        import jwt

        return jwt.encode(
            {
                "iss": JWT_ISSUER,
                "aud": JWT_AUDIENCE,
                "sub": subject,
                "sid": session_id,
                "jti": token_id,
                "iat": issued_at,
                "nbf": issued_at,
                "exp": expires_at,
            },
            self.secret,
            algorithm=JWT_ALGORITHM,
            headers={"typ": "JWT"},
        )

    def verify(self, token: str, *, now: float) -> SessionClaims | None:
        if len(token) > 4096 or not math.isfinite(now):
            return None
        import jwt

        try:
            decoded = jwt.decode_complete(
                token,
                self.secret,
                algorithms=[JWT_ALGORITHM],
                issuer=JWT_ISSUER,
                audience=JWT_AUDIENCE,
                options={
                    "require": list(JWT_REQUIRED_CLAIMS),
                    "strict_aud": True,
                    # Enforced strictly below against the trusted injected clock.
                    "verify_iat": False,
                    "verify_nbf": False,
                    "verify_exp": False,
                },
            )
            if decoded["header"] != {"alg": JWT_ALGORITHM, "typ": "JWT"}:
                return None
            claims = decoded["payload"]
            if set(claims) != set(JWT_REQUIRED_CLAIMS):
                return None
            if (
                not isinstance(claims["sub"], str)
                or not 1 <= len(claims["sub"]) <= 128
                or any(
                    not isinstance(claims[name], str)
                    or TOKEN_ID_PATTERN.fullmatch(claims[name]) is None
                    for name in ("sid", "jti")
                )
                or any(type(claims[name]) is not int for name in ("iat", "nbf", "exp"))
            ):
                return None
            if (
                claims["iat"] < 0
                or claims["nbf"] != claims["iat"]
                or claims["exp"] - claims["iat"] != SESSION_TTL_SECONDS
                or not claims["iat"] <= now < claims["exp"]
            ):
                return None
            return SessionClaims(
                subject=claims["sub"],
                session_id=claims["sid"],
                token_id=claims["jti"],
                issued_at=claims["iat"],
                not_before=claims["nbf"],
                expires_at=claims["exp"],
            )
        except (jwt.InvalidTokenError, ValueError, TypeError, UnicodeError):
            return None


def random_token() -> str:
    return secrets.token_hex(32)
