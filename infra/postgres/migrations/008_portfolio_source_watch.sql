-- Explicit v7 -> v8 extension: fail-closed bounded source observation.
CREATE TABLE {schema}.runtime_source_watch (
    singleton INTEGER PRIMARY KEY, payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TABLE {schema}.runtime_source_observations (
    id TEXT PRIMARY KEY, sequence BIGINT NOT NULL UNIQUE,
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TRIGGER immutable_source_watch BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.runtime_source_watch FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_source_observation BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.runtime_source_observations FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
REVOKE ALL ON {schema}.runtime_source_watch,{schema}.runtime_source_observations FROM PUBLIC;
GRANT SELECT ON {schema}.runtime_source_watch TO {application_role};
GRANT SELECT,INSERT ON {schema}.runtime_source_observations TO {application_role};
