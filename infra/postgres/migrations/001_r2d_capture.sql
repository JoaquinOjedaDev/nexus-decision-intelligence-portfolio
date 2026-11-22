CREATE SCHEMA IF NOT EXISTS r2d_capture;

CREATE TABLE IF NOT EXISTS r2d_capture.schema_migrations (
    version TEXT PRIMARY KEY,
    migration_sha256 TEXT NOT NULL CHECK (migration_sha256 ~ '^[0-9a-f]{64}$'),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS r2d_capture.partition_progress (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    next_offset BIGINT NOT NULL CHECK (next_offset >= 0),
    initialization_kind TEXT NOT NULL CHECK (
        initialization_kind IN ('receipted_fresh_empty', 'verified_checkpoint')
    ),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (topic, partition_id)
);

CREATE TABLE IF NOT EXISTS r2d_capture.inbox (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    broker_offset BIGINT NOT NULL CHECK (broker_offset >= 0),
    physical_fingerprint TEXT NOT NULL CHECK (physical_fingerprint ~ '^[0-9a-f]{64}$'),
    key_bytes BYTEA,
    headers_json JSONB NOT NULL,
    broker_timestamp_ms BIGINT CHECK (broker_timestamp_ms >= 0),
    broker_timestamp_type TEXT NOT NULL CHECK (
        broker_timestamp_type IN ('create_time', 'log_append_time', 'not_available')
    ),
    raw_value BYTEA,
    raw_value_sha256 TEXT CHECK (raw_value_sha256 IS NULL OR raw_value_sha256 ~ '^[0-9a-f]{64}$'),
    transport_disposition TEXT NOT NULL CHECK (
        transport_disposition IN ('admitted', 'excluded')
    ),
    transport_reason TEXT NOT NULL,
    lineage_json JSONB,
    lineage_sha256 TEXT NOT NULL CHECK (lineage_sha256 ~ '^[0-9a-f]{64}$'),
    config_sha256 TEXT NOT NULL CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (topic, partition_id, broker_offset),
    CHECK (
        (broker_timestamp_type = 'not_available' AND broker_timestamp_ms IS NULL)
        OR
        (broker_timestamp_type <> 'not_available' AND broker_timestamp_ms IS NOT NULL)
    ),
    CHECK (
        (raw_value IS NULL AND raw_value_sha256 IS NULL)
        OR
        (raw_value IS NOT NULL AND raw_value_sha256 IS NOT NULL)
    ),
    CHECK (
        (transport_disposition = 'admitted' AND transport_reason = 'admitted')
        OR
        (transport_disposition = 'excluded' AND transport_reason <> 'admitted')
    )
);

CREATE INDEX IF NOT EXISTS inbox_raw_value_sha256_idx
    ON r2d_capture.inbox (raw_value_sha256);
