CREATE TABLE IF NOT EXISTS leaderboard (
    PRIMARY KEY (id),
    id             VARCHAR(128) NOT NULL,
    authoritative  BOOLEAN      NOT NULL DEFAULT FALSE,
    sort_order     SMALLINT     NOT NULL DEFAULT 1, -- asc(0), desc(1)
    operator       SMALLINT     NOT NULL DEFAULT 0, -- best(0), set(1), increment(2), decrement(3)
    reset_schedule VARCHAR(64), -- cron format
    metadata       JSONB        NOT NULL DEFAULT '{}',
    create_time    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    category       SMALLINT     NOT NULL DEFAULT 0 CHECK (category >= 0),
    description    VARCHAR(255) NOT NULL DEFAULT '',
    duration       INT          NOT NULL DEFAULT 0 CHECK (duration >= 0),
    end_time       TIMESTAMPTZ  NOT NULL DEFAULT '1970-01-01 00:00:00 UTC',
    join_required  BOOLEAN      NOT NULL DEFAULT FALSE,
    max_size       INT          NOT NULL DEFAULT 100000000 CHECK (max_size > 0),
    max_num_score  INT          NOT NULL DEFAULT 1000000 CHECK (max_num_score > 0),
    title          VARCHAR(255) NOT NULL DEFAULT '',
    size           INT          NOT NULL DEFAULT 0,
    start_time     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    enable_ranks   BOOLEAN      NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS leaderboard_record (
    PRIMARY KEY (leaderboard_id, expiry_time, score, subscore, owner_id),
    FOREIGN KEY (leaderboard_id) REFERENCES leaderboard (id) ON DELETE CASCADE,
    leaderboard_id VARCHAR(128)  NOT NULL,
    owner_id       UUID          NOT NULL,
    username       VARCHAR(128),
    score          BIGINT        NOT NULL DEFAULT 0 CHECK (score >= 0),
    subscore       BIGINT        NOT NULL DEFAULT 0 CHECK (subscore >= 0),
    num_score      INT           NOT NULL DEFAULT 1 CHECK (num_score >= 0),
    max_num_score  INT           NOT NULL DEFAULT 1000000 CHECK (max_num_score > 0),
    metadata       JSONB         NOT NULL DEFAULT '{}',
    create_time    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    update_time    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    expiry_time    TIMESTAMPTZ   NOT NULL DEFAULT '1970-01-01 00:00:00 UTC',
    UNIQUE (owner_id, leaderboard_id, expiry_time)
);

CREATE INDEX IF NOT EXISTS leaderboard_create_time_id_idx ON leaderboard (create_time ASC, id ASC);
CREATE INDEX IF NOT EXISTS duration_start_time_end_time_category_idx ON leaderboard (duration, start_time, end_time DESC, category);
CREATE INDEX IF NOT EXISTS idx_leaderboard_record_ranking_desc ON leaderboard_record(leaderboard_id, score DESC, subscore DESC, update_time ASC) INCLUDE (expiry_time);
CREATE INDEX IF NOT EXISTS idx_leaderboard_record_ranking_asc ON leaderboard_record(leaderboard_id, score ASC, subscore ASC, update_time ASC) INCLUDE (expiry_time);
CREATE INDEX IF NOT EXISTS owner_id_expiry_time_leaderboard_id_idx ON leaderboard_record (owner_id, expiry_time, leaderboard_id);
