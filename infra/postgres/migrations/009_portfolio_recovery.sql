-- Standalone transport [5] -> [5,9]. This is not a workflow-schema migration.
CREATE TABLE {schema}.transport_recovery_cases (
    id TEXT PRIMARY KEY REFERENCES {schema}.transport_inbox(id),
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TABLE {schema}.transport_recovery_sources (
    id TEXT PRIMARY KEY, payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TABLE {schema}.transport_recovery_requests (
    id TEXT PRIMARY KEY,
    case_id TEXT NOT NULL REFERENCES {schema}.transport_recovery_cases(id),
    source_id TEXT NOT NULL REFERENCES {schema}.transport_recovery_sources(id),
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TABLE {schema}.transport_recovery_decisions (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL REFERENCES {schema}.transport_recovery_requests(id),
    sequence BIGINT NOT NULL UNIQUE,
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TRIGGER immutable_recovery_case BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.transport_recovery_cases FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_recovery_source BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.transport_recovery_sources FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_recovery_request BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.transport_recovery_requests FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_recovery_decision BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.transport_recovery_decisions FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
REVOKE ALL ON {schema}.transport_recovery_cases,{schema}.transport_recovery_sources,
    {schema}.transport_recovery_requests,{schema}.transport_recovery_decisions FROM PUBLIC;
GRANT SELECT,INSERT ON {schema}.transport_recovery_cases,{schema}.transport_recovery_sources,
    {schema}.transport_recovery_requests,{schema}.transport_recovery_decisions TO {application_role};
