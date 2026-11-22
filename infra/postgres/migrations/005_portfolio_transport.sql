-- Standalone per-consumer transport schema, NOT an upgrade of workflow schema 004.
-- Only a separate migration identity may create these objects.
CREATE TABLE {schema}.schema_migrations (
    version INTEGER PRIMARY KEY,
    migration_sha256 TEXT NOT NULL CHECK (migration_sha256 ~ '^[0-9a-f]{{64}}$'),
    applied_at DOUBLE PRECISION NOT NULL,
    application_role TEXT NOT NULL
);
CREATE TABLE {schema}.transport_binding (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    payload TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL
);
CREATE TABLE {schema}.transport_progress (
    partition INTEGER PRIMARY KEY CHECK (partition >= 0),
    next_offset BIGINT NOT NULL CHECK (next_offset >= 0),
    head_hash TEXT NOT NULL
);
CREATE TABLE {schema}.transport_inbox (
    id TEXT PRIMARY KEY,
    partition INTEGER NOT NULL REFERENCES {schema}.transport_progress(partition),
    broker_offset BIGINT NOT NULL CHECK (broker_offset >= 0),
    payload TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    UNIQUE (partition, broker_offset)
);
CREATE TABLE {schema}.transport_delivery (
    partition INTEGER PRIMARY KEY REFERENCES {schema}.transport_progress(partition),
    next_offset BIGINT NOT NULL CHECK (next_offset >= 0),
    head_hash TEXT NOT NULL
);
CREATE TABLE {schema}.transport_receipts (
    id TEXT PRIMARY KEY REFERENCES {schema}.transport_inbox(id),
    partition INTEGER NOT NULL REFERENCES {schema}.transport_delivery(partition),
    broker_offset BIGINT NOT NULL CHECK (broker_offset >= 0),
    payload TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    UNIQUE (partition, broker_offset)
);
CREATE FUNCTION {schema}.reject_history_mutation() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'Immutable portfolio history cannot be rewritten';
END;
$$;
CREATE TRIGGER immutable_migration_history
    BEFORE UPDATE OR DELETE OR TRUNCATE ON {schema}.schema_migrations
    FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_transport_binding
    BEFORE UPDATE OR DELETE OR TRUNCATE ON {schema}.transport_binding
    FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_transport_inbox
    BEFORE UPDATE OR DELETE OR TRUNCATE ON {schema}.transport_inbox
    FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_transport_receipts
    BEFORE UPDATE OR DELETE OR TRUNCATE ON {schema}.transport_receipts
    FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
REVOKE ALL ON SCHEMA {schema} FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA {schema} FROM PUBLIC;
GRANT USAGE ON SCHEMA {schema} TO {application_role};
GRANT SELECT ON {schema}.schema_migrations TO {application_role};
GRANT SELECT, INSERT ON {schema}.transport_binding, {schema}.transport_inbox,
    {schema}.transport_receipts TO {application_role};
GRANT SELECT, INSERT, UPDATE ON {schema}.transport_progress,
    {schema}.transport_delivery TO {application_role};
