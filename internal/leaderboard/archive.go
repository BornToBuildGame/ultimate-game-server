package leaderboard

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ArchiveRecordsForExpiry copies active records for an expiry partition into the archive table.
func ArchiveRecordsForExpiry(ctx context.Context, pool *pgxpool.Pool, leaderboardID string, expiryTime time.Time, seasonKey string) (int64, error) {
	if seasonKey == "" {
		seasonKey = strconv.FormatInt(expiryTime.Unix(), 10)
	}
	tag, err := pool.Exec(ctx, `
		INSERT INTO leaderboard_record_archive (
			leaderboard_id, owner_id, username, score, subscore, num_score, max_num_score,
			metadata, create_time, update_time, expiry_time, season_key
		)
		SELECT leaderboard_id, owner_id, username, score, subscore, num_score, max_num_score,
		       metadata, create_time, update_time, expiry_time, $3
		FROM leaderboard_record
		WHERE leaderboard_id = $1 AND expiry_time = $2`,
		leaderboardID, expiryTime, seasonKey,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ManualReset archives the current expiry partition (optional), clears those records, and fires cache eviction.
func ManualReset(ctx context.Context, pool *pgxpool.Pool, leaderboardID string, archive bool) (int64, error) {
	lb, err := GetLeaderboard(ctx, pool, leaderboardID)
	if err != nil {
		return 0, err
	}
	expiryUnix, ok := CalculateExpiry(lb, 0, time.Now().UTC())
	if !ok {
		return 0, ErrNoRecordsPossible
	}
	expiryTime := ResolveExpiryTime(expiryUnix)
	seasonKey := strconv.FormatInt(expiryUnix, 10)

	var archived int64
	if archive {
		archived, err = ArchiveRecordsForExpiry(ctx, pool, leaderboardID, expiryTime, seasonKey)
		if err != nil {
			return 0, err
		}
	}

	// For tournaments, roll season stats from current records.
	if lb.IsTournament() {
		if _, err := pool.Exec(ctx, `
			INSERT INTO tournament_season_stats (tournament_id, season_key, owner_id, participations, best_score, best_subscore, update_time)
			SELECT leaderboard_id, $2, owner_id, 1, score, subscore, now()
			FROM leaderboard_record
			WHERE leaderboard_id = $1 AND expiry_time = $3
			ON CONFLICT (tournament_id, season_key, owner_id) DO UPDATE SET
				participations = tournament_season_stats.participations + 1,
				best_score = GREATEST(tournament_season_stats.best_score, EXCLUDED.best_score),
				best_subscore = CASE
					WHEN EXCLUDED.best_score > tournament_season_stats.best_score THEN EXCLUDED.best_subscore
					ELSE tournament_season_stats.best_subscore END,
				update_time = now()`,
			leaderboardID, seasonKey, expiryTime,
		); err != nil {
			return archived, fmt.Errorf("season stats: %w", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE leaderboard SET size = 0 WHERE id = $1`, leaderboardID); err != nil {
			return archived, err
		}
	}

	if _, err := pool.Exec(ctx, `
		DELETE FROM leaderboard_record WHERE leaderboard_id = $1 AND expiry_time = $2`,
		leaderboardID, expiryTime,
	); err != nil {
		return archived, err
	}
	SharedRankCache.EvictPartition(leaderboardID, expiryUnix)
	localCache.mu.Lock()
	delete(localCache.cache, fmt.Sprintf("%s:%d", leaderboardID, expiryUnix))
	localCache.mu.Unlock()
	return archived, nil
}

// ListArchivedRecords returns archived records for a leaderboard season.
func ListArchivedRecords(ctx context.Context, pool *pgxpool.Pool, leaderboardID, seasonKey string, limit int) ([]*LeaderboardRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > MaxPageSize {
		limit = MaxPageSize
	}
	rows, err := pool.Query(ctx, `
		SELECT leaderboard_id, owner_id, username, score, subscore, num_score, max_num_score,
		       metadata, create_time, update_time, expiry_time
		FROM leaderboard_record_archive
		WHERE leaderboard_id = $1 AND ($2 = '' OR season_key = $2)
		ORDER BY score DESC, subscore DESC
		LIMIT $3`, leaderboardID, seasonKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*LeaderboardRecord
	for rows.Next() {
		r := &LeaderboardRecord{}
		if err := rows.Scan(&r.LeaderboardID, &r.OwnerID, &r.Username, &r.Score, &r.Subscore,
			&r.NumScore, &r.MaxNumScore, &r.Metadata, &r.CreateTime, &r.UpdateTime, &r.ExpiryTime); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SeasonStat is per-player season aggregate for a tournament.
type SeasonStat struct {
	TournamentID   string    `json:"tournament_id"`
	SeasonKey      string    `json:"season_key"`
	OwnerID        string    `json:"owner_id"`
	Participations int       `json:"participations"`
	BestScore      int64     `json:"best_score"`
	BestSubscore   int64     `json:"best_subscore"`
	TotalRewards   int64     `json:"total_rewards"`
	UpdateTime     time.Time `json:"update_time"`
}

// ListSeasonStats returns season stats for a tournament.
func ListSeasonStats(ctx context.Context, pool *pgxpool.Pool, tournamentID, seasonKey string, limit int) ([]*SeasonStat, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := pool.Query(ctx, `
		SELECT tournament_id, season_key, owner_id, participations, best_score, best_subscore, total_rewards, update_time
		FROM tournament_season_stats
		WHERE tournament_id = $1 AND ($2 = '' OR season_key = $2)
		ORDER BY best_score DESC
		LIMIT $3`, tournamentID, seasonKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*SeasonStat
	for rows.Next() {
		s := &SeasonStat{}
		if err := rows.Scan(&s.TournamentID, &s.SeasonKey, &s.OwnerID, &s.Participations,
			&s.BestScore, &s.BestSubscore, &s.TotalRewards, &s.UpdateTime); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
