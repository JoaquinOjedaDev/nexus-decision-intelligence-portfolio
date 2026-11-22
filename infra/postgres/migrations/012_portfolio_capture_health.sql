-- Opt-in operations ledger [5,9,10,11] -> [5,9,10,11,12].
CREATE TABLE {schema}.capture_health_policy (
    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TABLE {schema}.capture_instances (
    id TEXT PRIMARY KEY, sequence BIGINT NOT NULL UNIQUE,
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TABLE {schema}.capture_observations (
    id TEXT PRIMARY KEY, sequence BIGINT NOT NULL UNIQUE,
    instance_id TEXT NOT NULL REFERENCES {schema}.capture_instances(id),
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TRIGGER immutable_capture_health_policy BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.capture_health_policy FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_capture_instance BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.capture_instances FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_capture_observation BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.capture_observations FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
REVOKE ALL ON {schema}.capture_health_policy,{schema}.capture_instances,{schema}.capture_observations FROM PUBLIC;
GRANT SELECT ON {schema}.capture_health_policy TO {application_role};
GRANT SELECT,INSERT ON {schema}.capture_instances,{schema}.capture_observations TO {application_role};
