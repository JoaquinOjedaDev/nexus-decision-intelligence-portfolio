-- Standalone transport [5,9,10] -> [5,9,10,11]. Original source bytes stay immutable.
CREATE TABLE {schema}.transport_recovery_resolutions (
    id TEXT PRIMARY KEY REFERENCES {schema}.transport_inbox(id),
    request_id TEXT NOT NULL REFERENCES {schema}.transport_recovery_requests(id),
    decision_id TEXT NOT NULL REFERENCES {schema}.transport_recovery_decisions(id),
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TRIGGER immutable_recovery_resolution BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.transport_recovery_resolutions FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
REVOKE ALL ON {schema}.transport_recovery_resolutions FROM PUBLIC;
GRANT SELECT,INSERT ON {schema}.transport_recovery_resolutions TO {application_role};
