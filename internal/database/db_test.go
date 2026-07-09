package database

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestConnectWithBackoff_RetryFailure(t *testing.T) {
	// Parseable DSN pointing to a non-existent port to force connection failure
	cfg := Config{
		DSN:        "postgres://game_admin:game_password@127.0.0.1:59999/ultimate_game_db?sslmode=disable&connect_timeout=1",
		MaxRetries: 3,
		RetryDelay: 10 * time.Millisecond,
	}

	logger := zap.NewNop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	pool, err := ConnectWithBackoff(ctx, logger, cfg)
	elapsed := time.Since(start)

	if err == nil {
		if pool != nil {
			pool.Close()
		}
		t.Fatal("Expected connection to fail, but it succeeded")
	}

	// It should fail with "failed to connect to PostgreSQL database after 3 attempts"
	expectedErrSubstring := "failed to connect to PostgreSQL database after 3 attempts"
	if !strings.Contains(err.Error(), expectedErrSubstring) {
		t.Errorf("Expected error to contain %q, got: %v", expectedErrSubstring, err)
	}

	// With RetryDelay = 10ms:
	// Attempt 1: fail, wait 10ms
	// Attempt 2: fail, wait 20ms
	// Attempt 3: fail, exit loop
	// Total wait time should be at least 30ms.
	expectedMinWait := 30 * time.Millisecond
	if elapsed < expectedMinWait {
		t.Errorf("Expected backoff retry to take at least %v, took %v", expectedMinWait, elapsed)
	}
}

func TestConnectWithBackoff_ContextCancellation(t *testing.T) {
	cfg := Config{
		DSN:        "postgres://game_admin:game_password@127.0.0.1:59999/ultimate_game_db?sslmode=disable&connect_timeout=1",
		MaxRetries: 5,
		RetryDelay: 500 * time.Millisecond,
	}

	logger := zap.NewNop()
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after 50ms (during the first retry delay)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	pool, err := ConnectWithBackoff(ctx, logger, cfg)
	if err == nil {
		if pool != nil {
			pool.Close()
		}
		t.Fatal("Expected connection to fail due to context cancellation")
	}

	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Errorf("Expected error to contain context cancellation error, got: %v", err)
	}
}
