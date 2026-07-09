-- Migration: 0004_chat_notification_schema
-- Description: Create message and notification tables and indices.

CREATE TABLE IF NOT EXISTS message (
    id UUID UNIQUE NOT NULL,
    code SMALLINT DEFAULT 0 NOT NULL,
    sender_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    username VARCHAR(128) NOT NULL,
    stream_mode SMALLINT NOT NULL,
    stream_subject UUID NOT NULL,
    stream_descriptor UUID NOT NULL,
    stream_label VARCHAR(128) NOT NULL,
    content JSONB DEFAULT '{}'::jsonb NOT NULL,
    create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    PRIMARY KEY (stream_mode, stream_subject, stream_descriptor, stream_label, create_time, id),
    UNIQUE (sender_id, id)
);

CREATE INDEX IF NOT EXISTS idx_message_sender ON message (sender_id);

CREATE TABLE IF NOT EXISTS notification (
    id UUID UNIQUE NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    subject VARCHAR(255) NOT NULL,
    content JSONB DEFAULT '{}'::jsonb NOT NULL,
    code SMALLINT NOT NULL,
    sender_id UUID NOT NULL,
    create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    PRIMARY KEY (user_id, create_time, id)
);

CREATE INDEX IF NOT EXISTS idx_notification_user_id ON notification (user_id);
