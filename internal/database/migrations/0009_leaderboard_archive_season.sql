CREATE TABLE IF NOT EXISTS leaderboard_record_archive (
    id              BIGSERIAL PRIMARY KEY,
    leaderboard_id  VARCHAR(128)  NOT NULL,
    owner_id        UUID          NOT NULL,
    username        VARCHAR(128),
    score           BIGINT        NOT NULL DEFAULT 0,
    subscore        BIGINT        NOT NULL DEFAULT 0,
    num_score       INT           NOT NULL DEFAULT 1,
    max_num_score   INT           NOT NULL DEFAULT 1000000,
    metadata        JSONB         NOT NULL DEFAULT '{}',
    create_time     TIMESTAMPTZ   NOT NULL,
    update_time     TIMESTAMPTZ   NOT NULL,
    expiry_time     TIMESTAMPTZ   NOT NULL,
    archived_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    season_key      VARCHAR(64)   NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_lb_archive_board_season ON leaderboard_record_archive (leaderboard_id, season_key, score DESC);
CREATE INDEX IF NOT EXISTS idx_lb_archive_owner ON leaderboard_record_archive (owner_id, leaderboard_id);

CREATE TABLE IF NOT EXISTS tournament_season_stats (
    tournament_id      VARCHAR(128) NOT NULL,
    season_key         VARCHAR(64)  NOT NULL,
    owner_id           UUID         NOT NULL,
    participations     INT          NOT NULL DEFAULT 0,
    best_score         BIGINT       NOT NULL DEFAULT 0,
    best_subscore      BIGINT       NOT NULL DEFAULT 0,
    total_rewards      BIGINT       NOT NULL DEFAULT 0,
    update_time        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (tournament_id, season_key, owner_id)
);

CREATE INDEX IF NOT EXISTS idx_tournament_season_owner ON tournament_season_stats (owner_id, tournament_id);
