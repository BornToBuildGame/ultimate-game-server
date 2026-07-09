-- Migration: 0005_storage_economy_schema
-- Description: Create storage, wallet_ledger, purchase, and subscription tables and indices.

CREATE TABLE IF NOT EXISTS storage (
    collection VARCHAR(128) NOT NULL,
    key VARCHAR(128) NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    value JSONB DEFAULT '{}'::jsonb NOT NULL,
    version VARCHAR(32) NOT NULL,
    read SMALLINT DEFAULT 1 NOT NULL CHECK (read >= 0),
    write SMALLINT DEFAULT 1 NOT NULL CHECK (write >= 0),
    create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    PRIMARY KEY (collection, key, user_id)
);

CREATE INDEX IF NOT EXISTS idx_storage_collection_read_user_id_key ON storage (collection, read, user_id, key);
CREATE INDEX IF NOT EXISTS idx_storage_collection_read_key_user_id ON storage (collection, read, key, user_id);
CREATE INDEX IF NOT EXISTS idx_storage_collection_user_id_read_key ON storage (collection, user_id, read, key);
CREATE INDEX IF NOT EXISTS idx_storage_auto_index_fk_user_id ON storage (user_id);
CREATE INDEX IF NOT EXISTS idx_storage_value_gin ON storage USING gin(value jsonb_path_ops);

CREATE TABLE IF NOT EXISTS wallet_ledger (
    id UUID UNIQUE NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    changeset JSONB NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb NOT NULL,
    create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    PRIMARY KEY (user_id, create_time, id)
);

CREATE INDEX IF NOT EXISTS idx_wallet_ledger_user_history ON wallet_ledger(user_id, create_time DESC);

CREATE TABLE IF NOT EXISTS purchase (
    transaction_id VARCHAR(512) PRIMARY KEY CHECK (length(transaction_id) > 0),
    user_id UUID DEFAULT '00000000-0000-0000-0000-000000000000' NOT NULL REFERENCES users(id) ON DELETE SET DEFAULT,
    product_id VARCHAR(512) NOT NULL,
    store SMALLINT DEFAULT 0 NOT NULL, -- 0=AppleAppStore, 1=GooglePlay, 2=Huawei
    raw_response JSONB DEFAULT '{}'::jsonb NOT NULL,
    purchase_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    refund_time TIMESTAMPTZ DEFAULT '1970-01-01 00:00:00 UTC' NOT NULL,
    environment SMALLINT DEFAULT 0 NOT NULL, -- 0=Unknown, 1=Sandbox, 2=Production
    create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_purchase_time_user_id_transaction_id ON purchase (purchase_time DESC, user_id DESC, transaction_id DESC);

CREATE TABLE IF NOT EXISTS subscription (
    original_transaction_id VARCHAR(512) PRIMARY KEY CHECK (length(original_transaction_id) > 0),
    user_id UUID DEFAULT '00000000-0000-0000-0000-000000000000' NOT NULL REFERENCES users(id) ON DELETE SET DEFAULT,
    product_id VARCHAR(512) NOT NULL,
    store SMALLINT DEFAULT 0 NOT NULL, -- 0=AppleAppStore, 1=GooglePlay, 2=Huawei
    raw_response JSONB DEFAULT '{}'::jsonb NOT NULL,
    raw_notification JSONB DEFAULT '{}'::jsonb NOT NULL,
    purchase_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    expire_time TIMESTAMPTZ NOT NULL,
    refund_time TIMESTAMPTZ DEFAULT '1970-01-01 00:00:00 UTC' NOT NULL,
    environment SMALLINT DEFAULT 0 NOT NULL, -- 0=Unknown, 1=Sandbox, 2=Production
    create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
    update_time TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_subscription_time_user_id_transaction_id ON subscription (purchase_time DESC, user_id DESC, original_transaction_id DESC);
