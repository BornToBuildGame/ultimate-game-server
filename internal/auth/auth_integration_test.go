//go:build integration

package auth

import (
	"context"
	"strings"
	"testing"
	"time"

	"ultimate-game-server/internal/database"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestAuth_Integration(t *testing.T) {
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

	// Run migrations (which includes 0002_user_auth_schema.sql)
	err = database.RunMigrations(ctx, logger, pool)
	if err != nil {
		t.Fatalf("failed to run database migrations: %v", err)
	}

	// 2. Test User Registration
	email := "player@example.com"
	username := "player_one"
	password := "SecureP4ssword"
	displayName := "Player One"

	user, err := RegisterEmail(ctx, pool, username, email, password, displayName)
	if err != nil {
		t.Fatalf("failed to register user: %v", err)
	}
	if user.Username != username {
		t.Errorf("expected username %q, got %q", username, user.Username)
	}

	// Test password strength validation (e.g. no numbers)
	_, err = RegisterEmail(ctx, pool, "weak_p", "weak@example.com", "weakpass", "Weak")
	if err == nil {
		t.Error("expected registration to fail due to weak password, but it succeeded")
	}

	// Test unique username constraint
	_, err = RegisterEmail(ctx, pool, username, "player2@example.com", password, "Player Two")
	if err == nil || err.Error() != "username already taken" {
		t.Errorf("expected duplicate username error, got: %v", err)
	}

	// Test unique email constraint
	_, err = RegisterEmail(ctx, pool, "player_two", email, password, "Player Two")
	if err == nil || err.Error() != "email already taken" {
		t.Errorf("expected duplicate email error, got: %v", err)
	}

	// 3. Test Email Authentication
	authenticatedUser, err := AuthenticateEmail(ctx, pool, email, password)
	if err != nil {
		t.Fatalf("failed to authenticate: %v", err)
	}
	if authenticatedUser.ID != user.ID {
		t.Errorf("expected authenticated user ID %v, got %v", user.ID, authenticatedUser.ID)
	}

	// Test invalid credentials
	_, err = AuthenticateEmail(ctx, pool, email, "wrongPassword")
	if err == nil || err.Error() != "invalid credentials" {
		t.Errorf("expected invalid credentials error, got: %v", err)
	}

	// 4. Test Custom ID (device sign-in)
	customID := "device_macbook_pro_12345"
	customUser, err := AuthenticateCustom(ctx, pool, customID)
	if err != nil {
		t.Fatalf("failed to authenticate custom ID: %v", err)
	}
	if !strings.HasPrefix(customUser.Username, "user_") {
		t.Errorf("expected auto-generated username starting with 'user_', got %q", customUser.Username)
	}

	// Login again with same custom ID should return same user profile
	customUser2, err := AuthenticateCustom(ctx, pool, customID)
	if err != nil {
		t.Fatalf("failed to authenticate custom ID second time: %v", err)
	}
	if customUser2.ID != customUser.ID {
		t.Errorf("expected same user ID %v, got %v", customUser.ID, customUser2.ID)
	}

	// 5. Test Link Provider
	err = LinkProvider(ctx, pool, customUser.ID, "google", "google_provider_id_999")
	if err != nil {
		t.Fatalf("failed to link google provider: %v", err)
	}

	// Log in via google provider should now succeed and return the customUser ID
	googleUser, err := AuthenticateSocial(ctx, pool, "google", "google_provider_id_999")
	if err != nil {
		t.Fatalf("failed to authenticate social provider: %v", err)
	}
	if googleUser.ID != customUser.ID {
		t.Errorf("expected user ID %v, got %v", customUser.ID, googleUser.ID)
	}

	// 6. Test Add Device
	pushTokens := map[string]string{
		"android": "android_token_val",
		"ios":     "ios_token_val",
	}
	err = AddDevice(ctx, pool, user.ID, "device_iphone_15", `{"sound": true}`, pushTokens)
	if err != nil {
		t.Fatalf("failed to add device: %v", err)
	}

	// 7. Test Soft Delete User
	err = SoftDeleteUser(ctx, pool, user.ID)
	if err != nil {
		t.Fatalf("failed to soft delete user: %v", err)
	}

	// Attempting to log in as soft deleted user should return account disabled
	_, err = AuthenticateEmail(ctx, pool, email, password)
	if err == nil || err.Error() != "account disabled" {
		t.Errorf("expected account disabled error, got: %v", err)
	}

	// Verify user_tombstone record was created
	var exists bool
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM user_tombstone WHERE user_id = $1)", user.ID).Scan(&exists)
	if err != nil {
		t.Fatalf("failed to query user_tombstone: %v", err)
	}
	if !exists {
		t.Error("expected user tombstone record to exist, but it was not found")
	}
}
