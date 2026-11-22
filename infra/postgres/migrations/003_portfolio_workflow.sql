-- Explicit expansion of migration 002; no mutation of its immutable history.
CREATE TABLE {schema}.outbox (
    recommendation_id TEXT PRIMARY KEY REFERENCES {schema}.recommendations(id),
    operation_id TEXT NOT NULL UNIQUE, state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, fingerprint TEXT NOT NULL
);
CREATE TABLE {schema}.effects (
    recommendation_id TEXT PRIMARY KEY REFERENCES {schema}.recommendations(id),
    external_reference TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL,
    payload TEXT NOT NULL, created_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE {schema}.idempotency (
    key TEXT PRIMARY KEY,
    recommendation_id TEXT NOT NULL REFERENCES {schema}.recommendations(id),
    fingerprint TEXT NOT NULL
);
CREATE TABLE {schema}.sessions (
    id TEXT PRIMARY KEY, persona TEXT NOT NULL, csrf TEXT NOT NULL,
    expires_at DOUBLE PRECISION NOT NULL, revoked INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE {schema}.session_tokens (
    session_id TEXT PRIMARY KEY REFERENCES {schema}.sessions(id),
    subject TEXT NOT NULL, token_id TEXT NOT NULL UNIQUE,
    issued_at BIGINT NOT NULL, not_before BIGINT NOT NULL
);
CREATE TRIGGER immutable_effect_history BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.effects FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_idempotency_history BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.idempotency FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_session_token_history BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.session_tokens FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
REVOKE ALL ON {schema}.outbox,{schema}.effects,{schema}.idempotency,
    {schema}.sessions,{schema}.session_tokens FROM PUBLIC;
GRANT UPDATE ON {schema}.recommendations TO {application_role};
GRANT SELECT,INSERT,UPDATE ON {schema}.outbox TO {application_role};
GRANT SELECT,INSERT ON {schema}.effects,{schema}.idempotency,
    {schema}.sessions,{schema}.session_tokens TO {application_role};
GRANT UPDATE (revoked) ON {schema}.sessions TO {application_role};
