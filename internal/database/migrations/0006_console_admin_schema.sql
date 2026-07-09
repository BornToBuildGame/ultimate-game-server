-- Migration: 0006_console_admin_schema
-- Description: Create console admin tables and indices.

CREATE TABLE IF NOT EXISTS console_user (
    id UUID PRIMARY KEY,
    username VARCHAR(128) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password BYTEA CHECK (length(password) < 32000),
    acl JSONB DEFAULT '{"admin":false}'::jsonb NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb NOT NULL,
    create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    disable_time TIMESTAMPTZ DEFAULT '1970-01-01 00:00:00 UTC' NOT NULL,
    mfa_secret BYTEA DEFAULT NULL,
    mfa_recovery_codes BYTEA DEFAULT NULL,
    mfa_required BOOLEAN DEFAULT FALSE NOT NULL
);

CREATE TABLE IF NOT EXISTS console_audit_log (
    id UUID UNIQUE NOT NULL,
    console_user_id UUID NOT NULL,
    console_username TEXT NOT NULL,
    email TEXT NOT NULL,
    action TEXT NOT NULL,
    resource TEXT NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb NOT NULL,
    create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    PRIMARY KEY (create_time, console_username, action, resource, id)
);

CREATE TABLE IF NOT EXISTS console_acl_template (
    id UUID PRIMARY KEY,
    name VARCHAR(64) UNIQUE NOT NULL CHECK (length(name) > 0),
    description VARCHAR(64) DEFAULT '' NOT NULL,
    acl JSONB DEFAULT '{}'::jsonb NOT NULL,
    create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS setting (
    name VARCHAR(64) PRIMARY KEY CHECK (length(name) > 0) CONSTRAINT setting_name_uniq UNIQUE,
    value JSONB DEFAULT '{}'::jsonb NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS users_notes (
    id UUID UNIQUE NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    note TEXT NOT NULL,
    create_id UUID DEFAULT NULL,
    update_id UUID DEFAULT NULL,
    PRIMARY KEY (user_id, create_time, id)
);

CREATE INDEX IF NOT EXISTS idx_users_notes_user_id ON users_notes (user_id);
