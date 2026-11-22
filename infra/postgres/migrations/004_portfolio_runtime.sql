-- Explicit multi-generation runtime expansion; migrations 002/003 stay immutable.
CREATE TABLE {schema}.runtime_jobs (
    id TEXT PRIMARY KEY, source_sha256 TEXT NOT NULL, state TEXT NOT NULL,
    eligible_at DOUBLE PRECISION NOT NULL, created_at DOUBLE PRECISION NOT NULL,
    base_seed_hash TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
    token TEXT, lease_until DOUBLE PRECISION, completed_at DOUBLE PRECISION,
    result_sha256 TEXT, error TEXT
);
CREATE TABLE {schema}.runtime_events (
    sequence BIGINT PRIMARY KEY, id TEXT NOT NULL UNIQUE,
    event_hash TEXT NOT NULL, payload TEXT NOT NULL,
    job_id TEXT REFERENCES {schema}.runtime_jobs(id), received_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE {schema}.runtime_generations (
    seed_hash TEXT PRIMARY KEY, payload TEXT NOT NULL, archived_at DOUBLE PRECISION NOT NULL,
    replacement_job_id TEXT NOT NULL REFERENCES {schema}.runtime_jobs(id),
    evidence_path TEXT NOT NULL
);
CREATE TABLE {schema}.runtime_journal (
    sequence BIGINT PRIMARY KEY, operation TEXT NOT NULL, actor_id TEXT NOT NULL,
    actor_role TEXT NOT NULL, occurred_at DOUBLE PRECISION NOT NULL, details TEXT NOT NULL,
    previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL
);
CREATE TABLE {schema}.session_scopes (
    session_id TEXT PRIMARY KEY REFERENCES {schema}.sessions(id),
    locations TEXT NOT NULL, generation_seed_hash TEXT NOT NULL, scope_version INTEGER NOT NULL
);
CREATE TRIGGER immutable_runtime_event_history BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.runtime_events FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_runtime_generation_history BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.runtime_generations FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_runtime_journal_history BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.runtime_journal FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_session_scope_history BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.session_scopes FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
REVOKE ALL ON {schema}.runtime_jobs,{schema}.runtime_events,{schema}.runtime_generations,
    {schema}.runtime_journal,{schema}.session_scopes FROM PUBLIC;
GRANT SELECT,INSERT,UPDATE ON {schema}.runtime_jobs TO {application_role};
GRANT SELECT,INSERT ON {schema}.runtime_events,{schema}.runtime_generations,
    {schema}.runtime_journal,{schema}.session_scopes TO {application_role};
