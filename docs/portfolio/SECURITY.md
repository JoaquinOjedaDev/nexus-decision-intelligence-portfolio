# Portfolio security and deployment boundaries

The control tower is a loopback-only local demo. Demo persona switching selects server-defined planner, approver, and auditor identities; it is not authentication against a real identity provider. Do not expose this application on a public interface, tunnel, shared host, or internet deployment with demo sessions enabled.

The local test issuer signs one-hour JWT sessions with PyJWT and a server-held HS256 key. Validation
pins the algorithm, issuer and audience; requires subject, session/token identifiers and validity
times; and binds those claims to the persisted server session. Revocation takes effect through that
server-side lookup. Tokens cannot supply roles, locations, verification keys or a different issuer.
This is a local JWT test issuer, not an OIDC/OAuth login service or proof of a real user's identity.

The API uses HttpOnly SameSite cookies and validates a separate CSRF token on mutating requests.
The browser never decides authorization. An approval is tied to the exact recommendation fingerprint
and a bounded validity window. The maker and checker must be different identities. An auditor may
inspect evidence but cannot approve or execute recommendations.

The JWT update adds session-binding records without replacing existing recommendations, approvals,
events or effects. Old two-part demo cookies are rejected: select a persona again after upgrading.
The signing key remains in the local database; an attacker who controls that database or the machine
is outside this sandbox's protection. Key rotation, external identity verification and secure remote
deployment remain open work. Never expose the persona-login endpoint publicly.

The execution outbox and unique effect records protect against duplicate local mock actions. Repeated requests and process restarts must not create duplicate effects. These guarantees apply within the SQLite sandbox, not to a real remote ERP that lacks a compatible idempotency contract.

The append-only audit chain detects ordinary event tampering, but a privileged database administrator could rewrite an entire chain. External immutable audit anchoring, backup policy, key rotation, and operational monitoring are production requirements, not features implied by a hash chain.

No production data, passwords, API keys, access tokens, or provider credentials are required for the portfolio demo. Generated state remains under ignored artifacts directories. The assistant indexes only a fixed list of project documentation and cannot fetch arbitrary paths or execute actions.

Before production deployment, replace demo personas with OIDC and tenant/location authorization, require HTTPS and secure cookies, introduce managed secrets and migrations, move state to the production database, validate distributed idempotency, test backup/restore, and perform a threat review. The broader roadmap owns these controls.
