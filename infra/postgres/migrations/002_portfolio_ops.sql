-- Trusted template; identifiers are composed with psycopg.sql.Identifier.
-- This is the initial portfolio foundation, not the R2D capture schema.
CREATE TABLE {schema}.schema_migrations (
    version INTEGER PRIMARY KEY,
    migration_sha256 TEXT NOT NULL CHECK (migration_sha256 ~ '^[0-9a-f]{{64}}$'),
    applied_at DOUBLE PRECISION NOT NULL,
    application_role TEXT NOT NULL
);

CREATE TABLE {schema}.metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE {schema}.recommendations (
    id TEXT PRIMARY KEY, payload TEXT NOT NULL, fingerprint TEXT NOT NULL,
    location_id TEXT NOT NULL, creator TEXT NOT NULL, state TEXT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL, valid_until DOUBLE PRECISION NOT NULL,
    approval_actor TEXT, approval_fingerprint TEXT, approval_expires_at DOUBLE PRECISION,
    execution_ref TEXT, executed_at DOUBLE PRECISION, outcome_due_at DOUBLE PRECISION,
    outcome_json TEXT
);
CREATE TABLE {schema}.events (
    sequence BIGINT PRIMARY KEY CHECK (sequence > 0),
    recommendation_id TEXT NOT NULL REFERENCES {schema}.recommendations(id),
    event_type TEXT NOT NULL, actor_id TEXT NOT NULL, actor_role TEXT NOT NULL,
    occurred_at DOUBLE PRECISION NOT NULL, details TEXT NOT NULL,
    previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL
);
CREATE TABLE {schema}.workflow_guard (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    event_count BIGINT NOT NULL CHECK (event_count >= 0),
    audit_head TEXT NOT NULL CHECK (audit_head ~ '^[0-9a-f]{{64}}$')
);
INSERT INTO {schema}.workflow_guard VALUES (1, 0, repeat('0', 64));

-- Defence in depth: runtime privileges also deny these operations. A database
-- owner can alter triggers, so read-side checksum verification is still required.
CREATE FUNCTION {schema}.reject_history_mutation() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'Immutable portfolio history cannot be rewritten';
END;
$$;
CREATE TRIGGER immutable_migration_history
    BEFORE UPDATE OR DELETE OR TRUNCATE ON {schema}.schema_migrations
    FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_event_history
    BEFORE UPDATE OR DELETE OR TRUNCATE ON {schema}.events
    FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();

REVOKE ALL ON SCHEMA {schema} FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA {schema} FROM PUBLIC;
GRANT USAGE ON SCHEMA {schema} TO {application_role};
GRANT SELECT ON {schema}.schema_migrations TO {application_role};
GRANT SELECT, INSERT, UPDATE ON {schema}.metadata TO {application_role};
GRANT SELECT, INSERT ON {schema}.recommendations, {schema}.events TO {application_role};
GRANT SELECT, UPDATE ON {schema}.workflow_guard TO {application_role};
