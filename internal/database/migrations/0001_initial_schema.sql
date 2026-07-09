-- 0001_initial_schema.sql

CREATE TABLE IF NOT EXISTS schema_version (
    version          VARCHAR(64) PRIMARY KEY,
    migration_time   TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);
