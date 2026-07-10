package tournament

import (
	"context"
	"fmt"
	"sync"
	"time"

	"ultimate-game-server/internal/leaderboard"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

// RewardHook represents the callback triggered when a tournament ends.
type RewardHook func(ctx context.Context, pool *pgxpool.Pool, tournamentID string, expiryTime time.Time, topRecords []*leaderboard.LeaderboardRecord) error

// TournamentScheduler manages active occurrences and reward processing.
type TournamentScheduler struct {
	pool            *pgxpool.Pool
	rdb             *redis.Client
	logger          *zap.Logger
	cronParser      cron.Parser
	rewardHook      RewardHook
	localRewarded   map[string]bool
	localRewardedMu sync.Mutex
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

// NewTournamentScheduler creates a new scheduler.
func NewTournamentScheduler(pool *pgxpool.Pool, rdb *redis.Client, logger *zap.Logger, hook RewardHook) *TournamentScheduler {
	return &TournamentScheduler{
		pool:          pool,
		rdb:           rdb,
		logger:        logger,
		cronParser:    cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
		rewardHook:    hook,
		localRewarded: make(map[string]bool),
		stopChan:      make(chan struct{}),
	}
}

// Start runs the background evaluation loop every tick (default 30 seconds).
func (ts *TournamentScheduler) Start(tickInterval time.Duration) {
	ts.wg.Add(1)
	go func() {
		defer ts.wg.Done()
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()

		ts.logger.Info("Starting tournament scheduler daemon...")
		for {
			select {
			case <-ts.stopChan:
				ts.logger.Info("Stopping tournament scheduler daemon...")
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
				if err := ts.evaluateTournaments(ctx); err != nil {
					ts.logger.Error("Failed evaluating tournaments", zap.Error(err))
				}
				cancel()
			}
		}
	}()
}

// Stop halts the scheduler loop.
func (ts *TournamentScheduler) Stop() {
	close(ts.stopChan)
	ts.wg.Wait()
}

func (ts *TournamentScheduler) evaluateTournaments(ctx context.Context) error {
	if ts.pool == nil {
		return nil
	}

	// 1. Fetch all configured tournaments (duration > 0 and reset_schedule != "")
	query := `
		SELECT id, reset_schedule, duration, start_time, end_time 
		FROM leaderboard 
		WHERE duration > 0 AND reset_schedule != ''
	`
	rows, err := ts.pool.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	now := time.Now().UTC()

	type tourneyItem struct {
		id            string
		resetSchedule string
		duration      int
		startTime     time.Time
		endTime       time.Time
	}

	var items []tourneyItem
	for rows.Next() {
		var item tourneyItem
		err = rows.Scan(&item.id, &item.resetSchedule, &item.duration, &item.startTime, &item.endTime)
		if err != nil {
			return err
		}
		items = append(items, item)
	}

	for _, item := range items {
		sched, err := ts.cronParser.Parse(item.resetSchedule)
		if err != nil {
			ts.logger.Warn("Failed parsing reset schedule cron for tournament", zap.String("id", item.id), zap.Error(err))
			continue
		}

		// Calculate the previous fire time relative to now
		prevFire, nextFire := ts.getPrevAndNextFireTime(sched, now, item.startTime)

		// Calculate occurrence expiry
		occurrenceExpiry := prevFire.Add(time.Duration(item.duration) * time.Second)

		// Check if the current occurrence has ended
		if now.After(occurrenceExpiry) {
			// This occurrence has expired! Trigger reward if not already processed.
			rewardKey := fmt.Sprintf("tournament:rewarded:%s:%d", item.id, occurrenceExpiry.Unix())

			if ts.isAlreadyRewarded(ctx, rewardKey) {
				continue
			}

			// Lock reward check
			if !ts.tryAcquireRewardLock(ctx, rewardKey) {
				continue
			}

			ts.logger.Info("Tournament occurrence expired, processing rewards", zap.String("id", item.id), zap.Time("expiry", occurrenceExpiry))

			// Fetch top records for this expired occurrence partition
			topRecords, _, err := leaderboard.GetLeaderboardRecords(ctx, ts.pool, ts.rdb, item.id, 100, "", occurrenceExpiry)
			if err != nil {
				ts.logger.Error("Failed to fetch top leaderboard records for tournament reward", zap.String("id", item.id), zap.Error(err))
				continue
			}

			if ts.rewardHook != nil {
				err = ts.rewardHook(ctx, ts.pool, item.id, occurrenceExpiry, topRecords)
				if err != nil {
					ts.logger.Error("Reward hook execution failed", zap.String("id", item.id), zap.Error(err))
					continue
				}
			}

			ts.logger.Info("Successfully rewarded tournament occurrence", zap.String("id", item.id), zap.Time("expiry", occurrenceExpiry))
		} else {
			ts.logger.Debug("Tournament occurrence is active", zap.String("id", item.id), zap.Time("ends_at", occurrenceExpiry), zap.Time("next_reset", nextFire))
		}
	}

	return nil
}

func (ts *TournamentScheduler) getPrevAndNextFireTime(sched cron.Schedule, now time.Time, startTime time.Time) (time.Time, time.Time) {
	// Look back in time starting 7 days before now to find the last fire time
	t := now.Add(-7 * 24 * time.Hour)
	if t.Before(startTime) {
		t = startTime
	}

	prev := startTime
	next := sched.Next(t)
	for {
		if next.After(now) {
			break
		}
		prev = next
		next = sched.Next(next)
	}

	return prev, next
}

func (ts *TournamentScheduler) isAlreadyRewarded(ctx context.Context, key string) bool {
	if ts.rdb != nil {
		exists, err := ts.rdb.Exists(ctx, key).Result()
		if err == nil && exists > 0 {
			return true
		}
	}
	ts.localRewardedMu.Lock()
	defer ts.localRewardedMu.Unlock()
	return ts.localRewarded[key]
}

func (ts *TournamentScheduler) tryAcquireRewardLock(ctx context.Context, key string) bool {
	if ts.rdb != nil {
		// Set dynamic Redis key with 24 hour expiration to prevent duplicate runs
		success, err := ts.rdb.SetNX(ctx, key, "1", 24*time.Hour).Result()
		if err == nil && success {
			return true
		}
		return false
	}

	ts.localRewardedMu.Lock()
	defer ts.localRewardedMu.Unlock()
	if ts.localRewarded[key] {
		return false
	}
	ts.localRewarded[key] = true
	return true
}

// JoinTournament registers a player's intent to participate in a tournament.
// It inserts an idempotent placeholder record with score=0, subscore=0, and num_score=0.
func JoinTournament(ctx context.Context, pool *pgxpool.Pool, tournamentID, ownerID, username string) error {
	var joinRequired bool
	var endTime time.Time
	query := `SELECT join_required, end_time FROM leaderboard WHERE id = $1`
	err := pool.QueryRow(ctx, query, tournamentID).Scan(&joinRequired, &endTime)
	if err != nil {
		return err
	}

	if !endTime.IsZero() && endTime.Unix() > 0 && time.Now().After(endTime) {
		return fmt.Errorf("tournament has already ended")
	}

	insertQuery := `
		INSERT INTO leaderboard_record (
			leaderboard_id, owner_id, username, score, subscore, num_score, max_num_score, metadata, create_time, update_time, expiry_time
		) VALUES ($1, $2, $3, 0, 0, 0, 1000000, '{}', now(), now(), $4)
		ON CONFLICT (owner_id, leaderboard_id, expiry_time) DO NOTHING
	`
	expiryTime := time.Unix(0, 0).UTC()
	_, err = pool.Exec(ctx, insertQuery, tournamentID, ownerID, username, expiryTime)
	return err
}
