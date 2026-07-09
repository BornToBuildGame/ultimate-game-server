//go:build integration

package notification

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"ultimate-game-server/internal/database"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestNotification_Integration(t *testing.T) {
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
		DSN:             dsn,
		MaxOpenConns:    5,
		MaxRetries:      5,
		RetryDelay:      500 * time.Millisecond,
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

	// 2. Insert test user
	userID := uuid.New().String()
	insertUser := `INSERT INTO users (id, username, email, password, display_name) VALUES ($1, $2, $3, $4, $5)`
	_, err = pool.Exec(ctx, insertUser, userID, "notify_user", "notify@test.com", []byte("hash"), "Notify User")
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	// Reset push counter
	atomic.StoreInt64(&ExternalPushCount, 0)

	// 3. Create Notification
	notif := &Notification{
		UserID:   userID,
		Subject:  "Daily Reward Available!",
		Content:  `{"gems": 100}`,
		Code:     10,
		SenderID: uuid.New().String(),
	}

	err = CreateNotification(ctx, pool, notif)
	if err != nil {
		t.Fatalf("failed to create notification: %v", err)
	}

	// Wait brief moment for goroutine external trigger mock to execute
	time.Sleep(50 * time.Millisecond)

	if count := atomic.LoadInt64(&ExternalPushCount); count != 1 {
		t.Errorf("expected ExternalPushCount to be 1, got: %d", count)
	}

	// 4. List Notifications and verify
	list, err := ListNotifications(ctx, pool, userID, 10)
	if err != nil {
		t.Fatalf("failed to list notifications: %v", err)
	}

	if len(list) != 1 {
		t.Fatalf("expected 1 notification record, got: %d", len(list))
	}

	n := list[0]
	if n.Subject != "Daily Reward Available!" {
		t.Errorf("expected subject 'Daily Reward Available!', got: %s", n.Subject)
	}
	if n.Content != `{"gems": 100}` {
		t.Errorf("expected content match, got: %s", n.Content)
	}
}
