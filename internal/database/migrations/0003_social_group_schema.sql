-- Migration: 0003_social_group_schema
-- Description: Create user_edge, groups, and group_edge tables and indices.

CREATE TABLE IF NOT EXISTS user_edge (
    source_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE CHECK (source_id <> '00000000-0000-0000-0000-000000000000'),
    position BIGINT NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    destination_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE CHECK (destination_id <> '00000000-0000-0000-0000-000000000000'),
    state SMALLINT DEFAULT 0 NOT NULL, -- 0=friend, 1=invite_sent, 2=invite_received, 3=blocked
    metadata JSONB DEFAULT '{}'::jsonb NOT NULL,
    PRIMARY KEY (source_id, state, position),
    UNIQUE (source_id, destination_id)
);

CREATE INDEX IF NOT EXISTS idx_user_edge_fk_destination_id ON user_edge (destination_id);

CREATE TABLE IF NOT EXISTS groups (
    id UUID UNIQUE NOT NULL,
    creator_id UUID NOT NULL,
    name VARCHAR(255) UNIQUE NOT NULL,
    description VARCHAR(255),
    avatar_url VARCHAR(512),
    lang_tag VARCHAR(18) DEFAULT 'en' NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb NOT NULL,
    state SMALLINT DEFAULT 0 NOT NULL CHECK (state >= 0), -- 0=open, 1=closed
    edge_count INT DEFAULT 0 NOT NULL,
    max_count INT DEFAULT 100 NOT NULL CHECK (max_count >= 1),
    create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    disable_time TIMESTAMPTZ DEFAULT '1970-01-01 00:00:00 UTC' NOT NULL,
    PRIMARY KEY (disable_time, lang_tag, edge_count, id)
);

CREATE INDEX IF NOT EXISTS idx_groups_edge_count_update_time_id ON groups (disable_time, edge_count, update_time, id);
CREATE INDEX IF NOT EXISTS idx_groups_update_time_edge_count_id ON groups (disable_time, update_time, edge_count, id);
CREATE INDEX IF NOT EXISTS idx_groups_name_active ON groups (name) WHERE disable_time = '1970-01-01 00:00:00 UTC';

CREATE TABLE IF NOT EXISTS group_edge (
    source_id UUID NOT NULL CHECK (source_id <> '00000000-0000-0000-0000-000000000000'),
    position BIGINT NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    destination_id UUID NOT NULL CHECK (destination_id <> '00000000-0000-0000-0000-000000000000'),
    state SMALLINT DEFAULT 0 NOT NULL, -- 0=superadmin, 1=admin, 2=member, 3=join_request, 4=banned
    PRIMARY KEY (source_id, state, position),
    UNIQUE (source_id, destination_id)
);
