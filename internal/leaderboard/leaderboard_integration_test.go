//go:build integration

package leaderboard

import (
	"context"
	"testing"
	"time"

	"ultimate-game-server/internal/database"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestLeaderboard_Integration(t *testing.T) {
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

	// 2. Create Leaderboard Config
	lb := &Leaderboard{
		ID:            "ranked_matches",
		Authoritative: false,
		SortOrder:     SortOrderDescending,
		Operator:      OperatorBest,
		ResetSchedule: "",
		Metadata:      `{"season": 1}`,
		Category:      1,
		Description:   "Ranked PvP Matches",
		Duration:      0,
		MaxNumScore:   5,
		MaxSize:       10000,
		Title:         "Ranked Season 1",
		EnableRanks:   true,
	}

	err = CreateLeaderboard(ctx, pool, lb)
	if err != nil {
		t.Fatalf("failed to create leaderboard: %v", err)
	}

	// Double-creation should fail or we get details on fetch
	fetchedLb, err := GetLeaderboard(ctx, pool, "ranked_matches")
	if err != nil {
		t.Fatalf("failed to fetch leaderboard: %v", err)
	}
	if fetchedLb.Title != lb.Title {
		t.Errorf("expected title %q, got %q", lb.Title, fetchedLb.Title)
	}

	// 3. Submit Scores
	userA := uuid.New().String()
	userB := uuid.New().String()
	userC := uuid.New().String()

	// Submit scores (OperatorBest, Descending)
	_, err = SubmitScore(ctx, pool, nil, "ranked_matches", userA, "user_a", 100, 10, "{}", false)
	if err != nil {
		t.Fatalf("failed to submit score: %v", err)
	}

	// Submit lower score for userA, should keep 100 (OperatorBest)
	_, err = SubmitScore(ctx, pool, nil, "ranked_matches", userA, "user_a", 80, 5, "{}", false)
	if err != nil {
		t.Fatalf("failed to submit score: %v", err)
	}

	// Submit higher score for userB
	_, err = SubmitScore(ctx, pool, nil, "ranked_matches", userB, "user_b", 150, 15, "{}", false)
	if err != nil {
		t.Fatalf("failed to submit score: %v", err)
	}

	// Submit same score but higher subscore for userC
	_, err = SubmitScore(ctx, pool, nil, "ranked_matches", userC, "user_c", 100, 20, "{}", false)
	if err != nil {
		t.Fatalf("failed to submit score: %v", err)
	}

	// 4. Query Leaderboard Records
	expiryTime := time.Unix(0, 0).UTC()
	records, nextCursor, err := GetLeaderboardRecords(ctx, pool, nil, "ranked_matches", 10, "", expiryTime)
	if err != nil {
		t.Fatalf("failed to list leaderboard records: %v", err)
	}

	// Expected order:
	// 1. userB: 150 (subscore 15) -> Rank 1
	// 2. userC: 100 (subscore 20) -> Rank 2
	// 3. userA: 100 (subscore 10) -> Rank 3
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if nextCursor != "" {
		t.Errorf("expected empty nextCursor, got %q", nextCursor)
	}

	if records[0].OwnerID != userB || records[0].Rank != 1 {
		t.Errorf("expected Rank 1 to be userB, got: %s (Rank %d, Score %d)", records[0].Username, records[0].Rank, records[0].Score)
	}
	if records[1].OwnerID != userC || records[1].Rank != 2 {
		t.Errorf("expected Rank 2 to be userC, got: %s (Rank %d, Score %d)", records[1].Username, records[1].Rank, records[1].Score)
	}
	if records[2].OwnerID != userA || records[2].Rank != 3 {
		t.Errorf("expected Rank 3 to be userA, got: %s (Rank %d, Score %d)", records[2].Username, records[2].Rank, records[2].Score)
	}

	// 5. Query Around Player
	around, err := GetLeaderboardRecordsAroundPlayer(ctx, pool, nil, "ranked_matches", userC, 1, expiryTime)
	if err != nil {
		t.Fatalf("failed to query records around player: %v", err)
	}

	// Should return userB, userC, userA
	if len(around) != 3 {
		t.Fatalf("expected 3 records around userC, got %d", len(around))
	}
	if around[1].OwnerID != userC {
		t.Errorf("expected middle element to be userC, got %s", around[1].Username)
	}

	// 6. Test Max Attempts (MaxNumScore = 5)
	// We have submitted 2 scores for UserA so far. Let's submit 3 more, then the 4th should fail.
	for i := 0; i < 3; i++ {
		_, err = SubmitScore(ctx, pool, nil, "ranked_matches", userA, "user_a", 120, 10, "{}", false)
		if err != nil {
			t.Fatalf("failed to submit score on attempt %d: %v", i+3, err)
		}
	}

	_, err = SubmitScore(ctx, pool, nil, "ranked_matches", userA, "user_a", 130, 10, "{}", false)
	if err != ErrMaxAttemptsReached {
		t.Errorf("expected ErrMaxAttemptsReached, got: %v", err)
	}
}
