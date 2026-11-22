-- Standalone transport [5,9] -> [5,9,10]; never alters workflow history.
CREATE TABLE {schema}.transport_recovery_topics (
    id TEXT PRIMARY KEY, payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TABLE {schema}.transport_recovery_publications (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL REFERENCES {schema}.transport_recovery_requests(id),
    decision_id TEXT NOT NULL REFERENCES {schema}.transport_recovery_decisions(id),
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TABLE {schema}.transport_recovery_publication_receipts (
    id TEXT PRIMARY KEY REFERENCES {schema}.transport_recovery_publications(id),
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TRIGGER immutable_recovery_topics BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.transport_recovery_topics FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_recovery_publication BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.transport_recovery_publications FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_recovery_publication_receipt BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.transport_recovery_publication_receipts FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
REVOKE ALL ON {schema}.transport_recovery_topics,{schema}.transport_recovery_publications,
    {schema}.transport_recovery_publication_receipts FROM PUBLIC;
GRANT SELECT,INSERT ON {schema}.transport_recovery_topics,{schema}.transport_recovery_publications,
    {schema}.transport_recovery_publication_receipts TO {application_role};
