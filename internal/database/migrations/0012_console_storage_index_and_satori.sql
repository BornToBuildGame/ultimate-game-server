-- Migration 0012: Console storage index & Satori schema persistence

CREATE TABLE IF NOT EXISTS console_storage_index (
    name VARCHAR(128) NOT NULL PRIMARY KEY,
    collection VARCHAR(128) NOT NULL,
    key VARCHAR(128) NOT NULL,
    fields TEXT[] NOT NULL DEFAULT '{}',
    sortable_fields TEXT[] NOT NULL DEFAULT '{}',
    max_entries INT NOT NULL DEFAULT 10000,
    index_only BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS satori_event (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    identity_id VARCHAR(128) NOT NULL,
    name VARCHAR(128) NOT NULL,
    value VARCHAR(1024) NOT NULL DEFAULT '',
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_satori_event_identity ON satori_event(identity_id, timestamp DESC);
