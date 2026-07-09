//go:build integration

package database

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestDatabase_Integration(t *testing.T) {
	ctx := context.Background()

	// Spin up PostgreSQL container
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

	// Get DSN
	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get postgres container connection string: %v", err)
	}

	logger := zap.NewNop()
	cfg := Config{
		DSN:             dsn,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		MaxConnLifetime: 5 * time.Minute,
		MaxRetries:      5,
		RetryDelay:      500 * time.Millisecond,
	}

	// Connect to database
	pool, err := ConnectWithBackoff(ctx, logger, cfg)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Run migrations
	err = RunMigrations(ctx, logger, pool)
	if err != nil {
		t.Fatalf("failed to run database migrations: %v", err)
	}

	// Verify that migrations were applied by checking getAppliedMigrations
	applied, err := getAppliedMigrations(ctx, pool)
	if err != nil {
		t.Fatalf("failed to get applied migrations: %v", err)
	}

	expectedVersion := "0001_initial_schema"
	if !applied[expectedVersion] {
		t.Errorf("expected migration %q to be marked as applied, but it was not", expectedVersion)
	}

	// Verify that schema_version table exists and can be queried directly
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema_version WHERE version = $1", expectedVersion).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query schema_version table: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 migration record in DB, got: %d", count)
	}
}
