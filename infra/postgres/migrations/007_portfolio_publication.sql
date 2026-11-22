-- Explicit v6 -> v7 extension: source bytes and intent commit before broker I/O.
CREATE TABLE {schema}.runtime_source_bundles (
    source_sha256 TEXT PRIMARY KEY, payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TABLE {schema}.runtime_publications (
    id TEXT PRIMARY KEY, sequence BIGINT NOT NULL UNIQUE,
    source_sha256 TEXT NOT NULL REFERENCES {schema}.runtime_source_bundles(source_sha256),
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
    event_sha256 TEXT NOT NULL, created_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE {schema}.runtime_publication_receipts (
    id TEXT PRIMARY KEY REFERENCES {schema}.runtime_publications(id),
    payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE TRIGGER immutable_source_bundle BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.runtime_source_bundles FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_source_publication BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.runtime_publications FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
CREATE TRIGGER immutable_publication_receipt BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.runtime_publication_receipts FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_history_mutation();
REVOKE ALL ON {schema}.runtime_source_bundles,{schema}.runtime_publications,{schema}.runtime_publication_receipts FROM PUBLIC;
GRANT SELECT,INSERT ON {schema}.runtime_source_bundles,{schema}.runtime_publications,{schema}.runtime_publication_receipts TO {application_role};
