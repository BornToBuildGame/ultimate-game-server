-- Migration: 0010_friends_indexes_score_audit
-- Description: TDD-07 user_edge lookup indexes + leaderboard score audit trail.

CREATE INDEX IF NOT EXISTS idx_user_edge_source_lookup
    ON user_edge (source_id, state, position DESC);

CREATE INDEX IF NOT EXISTS idx_user_edge_dest_lookup
    ON user_edge (destination_id, state);

CREATE TABLE IF NOT EXISTS leaderboard_score_audit (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    leaderboard_id VARCHAR(128) NOT NULL,
    owner_id       UUID NOT NULL,
    old_score      BIGINT NOT NULL DEFAULT 0,
    new_score      BIGINT NOT NULL DEFAULT 0,
    old_subscore   BIGINT NOT NULL DEFAULT 0,
    new_subscore   BIGINT NOT NULL DEFAULT 0,
    operator       SMALLINT NOT NULL DEFAULT 0,
    create_time    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_leaderboard_score_audit_board_time
    ON leaderboard_score_audit (leaderboard_id, create_time DESC);

CREATE INDEX IF NOT EXISTS idx_leaderboard_score_audit_owner_time
    ON leaderboard_score_audit (owner_id, create_time DESC);
