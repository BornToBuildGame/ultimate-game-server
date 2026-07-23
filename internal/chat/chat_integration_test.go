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
	pool, err := database.ConnectWithBackoff(ctx, logger, database.Config{
		DSN: dsn, MaxOpenConns: 5, MaxRetries: 5, RetryDelay: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	if err = database.RunMigrations(ctx, logger, pool); err != nil {
		t.Fatalf("failed to run database migrations: %v", err)
	}

	userID := uuid.New().String()
	_, err = pool.Exec(ctx, `INSERT INTO users (id, username, email, password, display_name) VALUES ($1, $2, $3, $4, $5)`,
		userID, "chat_user", "chat@test.com", []byte("hash"), "Chat User")
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	// Room channel with uuid.Nil subject/descriptor
	channelID, stream, err := BuildChannelId(ctx, pool, userID, "global_lobby", ChannelJoinRoom)
	if err != nil {
		t.Fatal(err)
	}
	if channelID != "2...global_lobby" {
		t.Fatalf("channelID=%q", channelID)
	}

	ack, msg, err := ChannelMessageSend(ctx, pool, stream, channelID, `{"text":"Hello world!"}`, userID, "chat_user", true)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if !ack.Persistent || msg.MessageID == "" {
		t.Fatalf("ack=%+v", ack)
	}

	list, err := ChannelMessagesList(ctx, pool, "", stream, channelID, 10, true, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(list.Messages))
	}
	if list.Messages[0].Content != `{"text":"Hello world!"}` {
		t.Errorf("content=%s", list.Messages[0].Content)
	}

	_, _, err = ChannelMessageUpdate(ctx, pool, stream, channelID, msg.MessageID, `{"text":"Edited"}`, userID, "chat_user", true)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	list2, err := ChannelMessagesList(ctx, pool, "", stream, channelID, 10, true, "")
	if err != nil {
		t.Fatal(err)
	}
	if list2.Messages[0].Content != `{"text":"Edited"}` {
		t.Errorf("after update content=%s", list2.Messages[0].Content)
	}

	_, _, err = ChannelMessageRemove(ctx, pool, stream, channelID, msg.MessageID, userID, "chat_user", true)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	list3, err := ChannelMessagesList(ctx, pool, "", stream, channelID, 10, true, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(list3.Messages) != 0 {
		t.Fatalf("expected 0 after remove, got %d", len(list3.Messages))
	}

	// Persist=false should not write
	_, _, err = ChannelMessageSend(ctx, pool, stream, channelID, `{"text":"ephemeral"}`, userID, "chat_user", false)
	if err != nil {
		t.Fatal(err)
	}
	list4, _ := ChannelMessagesList(ctx, pool, "", stream, channelID, 10, true, "")
	if len(list4.Messages) != 0 {
		t.Fatalf("persist=false should leave DB empty, got %d", len(list4.Messages))
	}
}
