-- Explicit v4 -> v6 workflow extension. Transport v5 stays in its own schema.
-- Bootstrap alone pins the binding; runtime cannot disable or rewrite the fence.
CREATE TABLE {schema}.runtime_transport_fence (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    payload TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL
);
CREATE TRIGGER immutable_runtime_transport_fence BEFORE UPDATE OR DELETE OR TRUNCATE
    ON {schema}.runtime_transport_fence FOR EACH STATEMENT
    EXECUTE FUNCTION {schema}.reject_history_mutation();
REVOKE ALL ON {schema}.runtime_transport_fence FROM PUBLIC;
GRANT SELECT ON {schema}.runtime_transport_fence TO {application_role};
