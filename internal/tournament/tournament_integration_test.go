//go:build integration

package tournament

import (
	"context"
	"testing"
	"time"

	"ultimate-game-server/internal/database"
	"ultimate-game-server/internal/leaderboard"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestTournament_Integration(t *testing.T) {
	ctx := context.Background()

	// 1. Spin up PostgreSQL container
	postgresContainer, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("ultimate_game_db"),
		postgres.WithUsername("game_admin"),
		postgres.WithPassword("game_password"),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Errorf("failed to terminate postgres container: %v", err)
		}
	}()

	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get container DSN: %v", err)
	}

	logger := zap.NewNop()
	dbCfg := database.Config{
		DSN:          dsn,
		MaxOpenConns: 10,
		MaxRetries:   10,
		RetryDelay:   500 * time.Millisecond,
	}

	pool, err := database.ConnectWithBackoff(ctx, logger, dbCfg)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Run migrations
	err = database.RunMigrations(ctx, logger, pool)
	if err != nil {
		t.Fatalf("failed to run database migrations: %v", err)
	}

	// 2. Create a Tournament Leaderboard Config with static, past dates
	// This makes the test robust and independent of the execution time of the day.
	startTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lb := &leaderboard.Leaderboard{
		ID:            "weekly_cup",
		Authoritative: false,
		SortOrder:     leaderboard.SortOrderDescending,
		Operator:      leaderboard.OperatorBest,
		ResetSchedule: "0 0 1 1 *", // resets yearly on Jan 1st
		Metadata:      `{"tier": "gold"}`,
		Category:      2,
		Description:   "Weekly Cup",
		Duration:      3600, // 1 hour duration
		MaxNumScore:   3,
		Title:         "Weekly PvP Cup",
		StartTime:     startTime,
	}

	err = leaderboard.CreateLeaderboard(ctx, pool, lb)
	if err != nil {
		t.Fatalf("failed to create leaderboard: %v", err)
	}

	// Expiry time is exactly start_time + duration
	expiryTime := startTime.Add(time.Duration(lb.Duration) * time.Second)

	// 3. Submit Scores to this expired occurrence
	userA := uuid.New().String()
	userB := uuid.New().String()

	_, err = pool.Exec(ctx, `
		INSERT INTO leaderboard_record (leaderboard_id, owner_id, username, score, subscore, num_score, max_num_score, metadata, create_time, update_time, expiry_time)
		VALUES ($1, $2, $3, 100, 10, 1, 3, '{}', now(), now(), $4),
		       ($1, $5, $6, 120, 15, 1, 3, '{}', now(), now(), $4)
	`, lb.ID, userA, "player_a", expiryTime, userB, "player_b")
	if err != nil {
		t.Fatalf("failed to insert mock leaderboard records: %v", err)
	}

	// 4. Setup scheduler and reward hooks
	hookCalled := 0
	var rewardedRecords []*leaderboard.LeaderboardRecord
	var rewardedExpiry time.Time

	rewardHook := func(ctx context.Context, p *pgxpool.Pool, id string, expTime time.Time, top []*leaderboard.LeaderboardRecord) error {
		hookCalled++
		rewardedRecords = top
		rewardedExpiry = expTime
		return nil
	}

	ts := NewTournamentScheduler(pool, nil, logger, rewardHook)

	// 5. Evaluate
	err = ts.evaluateTournaments(ctx)
	if err != nil {
		t.Fatalf("evaluation failed: %v", err)
	}

	if hookCalled != 1 {
		t.Errorf("expected reward hook to be called once, got: %d", hookCalled)
	}
	if !rewardedExpiry.Equal(expiryTime) {
		t.Errorf("expected rewarded expiry to be %v, got %v", expiryTime, rewardedExpiry)
	}
	if len(rewardedRecords) != 2 {
		t.Fatalf("expected 2 records, got: %d", len(rewardedRecords))
	}
	// Sorted order should be player_b (120) first, then player_a (100)
	if rewardedRecords[0].OwnerID != userB || rewardedRecords[1].OwnerID != userA {
		t.Errorf("records not sorted correctly or mismatch")
	}

	// 6. Test Idempotency (Evaluating again should not run the hook)
	err = ts.evaluateTournaments(ctx)
	if err != nil {
		t.Fatalf("second evaluation failed: %v", err)
	}

	if hookCalled != 1 {
		t.Errorf("expected hook to still be called exactly once, but count is: %d", hookCalled)
	}
}

func TestTournamentScheduler_StartStop(t *testing.T) {
	ts := NewTournamentScheduler(nil, nil, zap.NewNop(), nil)
	ts.Start(10 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	ts.Stop()
}
