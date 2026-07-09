-- Migration: 0008_seed_system_user
-- Description: Seed system user record to prevent foreign key violations for global storage and purchase/subscription fallbacks.

INSERT INTO users (id, username, display_name)
VALUES ('00000000-0000-0000-0000-000000000000', 'system', 'System User')
ON CONFLICT (id) DO NOTHING;
