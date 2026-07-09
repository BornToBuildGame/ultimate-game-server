-- 0002_user_auth_schema.sql

-- Enable pg_trgm extension for trigram search on display name
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Users Table
CREATE TABLE IF NOT EXISTS users (
    id                        UUID PRIMARY KEY,
    username                  VARCHAR(128) NOT NULL CONSTRAINT users_username_key UNIQUE,
    display_name              VARCHAR(255),
    avatar_url                VARCHAR(512),
    lang_tag                  VARCHAR(18) NOT NULL DEFAULT 'en',
    location                  VARCHAR(255),
    timezone                  VARCHAR(255),
    metadata                  JSONB NOT NULL DEFAULT '{}',
    wallet                    JSONB NOT NULL DEFAULT '{}',
    email                     VARCHAR(255) UNIQUE,
    password                  BYTEA CHECK (length(password) < 32000),
    facebook_id               VARCHAR(128) UNIQUE,
    google_id                 VARCHAR(128) UNIQUE,
    gamecenter_id             VARCHAR(128) UNIQUE,
    steam_id                  VARCHAR(128) UNIQUE,
    custom_id                 VARCHAR(128) UNIQUE,
    apple_id                  VARCHAR(128) UNIQUE,
    facebook_instant_game_id  VARCHAR(128) UNIQUE,
    edge_count                INT NOT NULL DEFAULT 0 CHECK (edge_count >= 0),
    create_time               TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time               TIMESTAMPTZ NOT NULL DEFAULT now(),
    verify_time               TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00 UTC',
    disable_time              TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00 UTC'
);

-- User Device Table
CREATE TABLE IF NOT EXISTS user_device (
    PRIMARY KEY (id),
    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,

    id                  VARCHAR(128) NOT NULL,
    user_id             UUID NOT NULL,
    preferences         JSONB NOT NULL DEFAULT '{}',
    push_token_amazon   VARCHAR(512) NOT NULL DEFAULT '',
    push_token_android  VARCHAR(512) NOT NULL DEFAULT '',
    push_token_huawei   VARCHAR(512) NOT NULL DEFAULT '',
    push_token_ios      VARCHAR(512) NOT NULL DEFAULT '',
    push_token_web      VARCHAR(512) NOT NULL DEFAULT '',

    UNIQUE (user_id, id)
);

-- User Tombstone Table
CREATE TABLE IF NOT EXISTS user_tombstone (
    PRIMARY KEY (create_time, user_id),

    user_id        UUID NOT NULL UNIQUE,
    create_time    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Table Indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email) WHERE email IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_apple_id ON users(apple_id) WHERE apple_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_id ON users(google_id) WHERE google_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_facebook_id ON users(facebook_id) WHERE facebook_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_gamecenter_id ON users(gamecenter_id) WHERE gamecenter_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_steam_id ON users(steam_id) WHERE steam_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_custom_id ON users(custom_id) WHERE custom_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_facebook_instant_game_id ON users(facebook_instant_game_id) WHERE facebook_instant_game_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_users_display_name_trgm ON users USING GIN (display_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_user_device_user_id ON user_device(user_id);
