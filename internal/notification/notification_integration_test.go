//go:build integration

package notification

import (
	"context"
	"testing"
	"time"

	"ultimate-game-server/internal/database"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestNotification_Integration(t *testing.T) {
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
		userID, "notify_user", "notify@test.com", []byte("hash"), "Notify User")
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	d := &mockDeliverer{byUser: map[string][]*Notification{}}

	// Non-persistent: deliver only, no DB row.
	nonPersist := &Notification{
		UserID: userID, Subject: "ephemeral", Content: `{"x":1}`, Code: 11, Persistent: false,
	}
	if err := NotificationSend(ctx, pool, d, map[string][]*Notification{userID: {nonPersist}}); err != nil {
		t.Fatalf("non-persist send: %v", err)
	}
	list0, err := NotificationList(ctx, pool, userID, 100, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list0.Notifications) != 0 {
		t.Fatalf("expected 0 persisted, got %d", len(list0.Notifications))
	}

	// Persistent: save + deliver.
	persist := &Notification{
		UserID: userID, Subject: "Daily Reward Available!", Content: `{"gems": 100}`,
		Code: 10, SenderID: uuid.New().String(), Persistent: true,
	}
	if err := NotificationSend(ctx, pool, d, map[string][]*Notification{userID: {persist}}); err != nil {
		t.Fatalf("persist send: %v", err)
	}

	list1, err := NotificationList(ctx, pool, userID, 1, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list1.Notifications) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(list1.Notifications))
	}
	n := list1.Notifications[0]
	if n.Subject != "Daily Reward Available!" {
		t.Errorf("subject: %s", n.Subject)
	}
	if list1.CacheableCursor == "" {
		t.Fatal("expected cacheable_cursor")
	}

	// Second page via cursor.
	persist2 := &Notification{
		UserID: userID, Subject: "Second", Content: `{}`, Code: 12, Persistent: true,
	}
	if err := NotificationSend(ctx, pool, d, map[string][]*Notification{userID: {persist2}}); err != nil {
		t.Fatalf("second send: %v", err)
	}
	list2, err := NotificationList(ctx, pool, userID, 1, list1.CacheableCursor)
	if err != nil {
		t.Fatalf("list page2: %v", err)
	}
	if len(list2.Notifications) != 1 || list2.Notifications[0].Subject != "Second" {
		t.Fatalf("unexpected page2: %+v", list2.Notifications)
	}

	// Delete clears inbox entry.
	if err := NotificationDelete(ctx, pool, userID, []string{n.ID, list2.Notifications[0].ID}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	list3, err := NotificationList(ctx, pool, userID, 100, "")
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	if len(list3.Notifications) != 0 {
		t.Fatalf("expected empty after delete, got %d", len(list3.Notifications))
	}

	d.mu.Lock()
	delivered := len(d.byUser[userID])
	d.mu.Unlock()
	if delivered < 3 {
		t.Fatalf("expected at least 3 deliveries, got %d", delivered)
	}
}
