//go:build integration

package chat

import (
	"context"
	"testing"
	"time"

	"ultimate-game-server/internal/database"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestChat_Integration(t *testing.T) {
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
	_, err = pool.Exec(ctx, insertUser, userID, "chat_user", "chat@test.com", []byte("hash"), "Chat User")
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	// 3. Save Message
	subjectID := uuid.New().String()
	descriptorID := uuid.New().String()

	msg := &Message{
		SenderID:         userID,
		Username:         "chat_user",
		StreamMode:       StreamModeRoom,
		StreamSubject:    subjectID,
		StreamDescriptor: descriptorID,
		StreamLabel:      "global_lobby",
		Content:          `{"text": "Hello world!"}`,
	}

	err = SaveMessage(ctx, pool, msg)
	if err != nil {
		t.Fatalf("failed to save message: %v", err)
	}

	// 4. List Messages and verify
	list, err := ListMessages(ctx, pool, StreamModeRoom, subjectID, descriptorID, "global_lobby", 10)
	if err != nil {
		t.Fatalf("failed to list messages: %v", err)
	}

	if len(list) != 1 {
		t.Fatalf("expected 1 message in history, got: %d", len(list))
	}

	m := list[0]
	if m.SenderID != userID {
		t.Errorf("expected sender %s, got %s", userID, m.SenderID)
	}
	if m.Content != `{"text": "Hello world!"}` {
		t.Errorf("expected message content match, got: %s", m.Content)
	}
}
