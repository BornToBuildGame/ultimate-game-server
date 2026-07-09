//go:build integration

package socket

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"ultimate-game-server/internal/auth"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

func TestSocket_Integration(t *testing.T) {
	// Initialize token manager
	secret := []byte("super_secret_signing_key_at_least_32_bytes_long_1234567")
	tm, err := auth.NewTokenManager(secret, 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to create token manager: %v", err)
	}

	userID := "user-integration-123"
	username := "socket_player"

	// Create valid JWT token
	token, _, err := tm.GenerateSession(userID, username)
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	reg := NewConnectionRegistry()
	reg.GracePeriod = 200 * time.Millisecond // Use short grace period for fast integration test

	var disconnectWg sync.WaitGroup
	disconnectWg.Add(1)

	var onDisconnectCalled bool
	var mu sync.Mutex

	handler := NewGatewayHandler(
		zapLoggerMock(),
		tm,
		reg,
		func(s *Session) {},
		func(sessionID string) {
			mu.Lock()
			onDisconnectCalled = true
			mu.Unlock()
			disconnectWg.Done()
		},
	)

	server := httptest.NewServer(http.HandlerFunc(handler.Upgrade))
	defer server.Close()

	// Convert http URL to ws URL
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server url: %v", err)
	}
	u.Scheme = "ws"
	u.RawQuery = fmt.Sprintf("token=%s", token)

	// 1. First connection
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}

	// Verify we can receive the ping message or close it
	// Close the connection immediately to trigger grace period
	conn.Close()

	// Wait 50ms: less than 200ms grace period. Session should still exist.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if onDisconnectCalled {
		t.Error("onDisconnect was called too early during grace period")
	}
	mu.Unlock()

	// 2. Reconnect with the same session ID to recover session
	// The registry should contain the session ID we want to recover.
	// Since we closed the connection, let's find the session ID from the registry
	var activeSessionID string
	reg.mu.RLock()
	for sessID := range reg.sessions {
		activeSessionID = sessID
		break
	}
	reg.mu.RUnlock()

	if activeSessionID == "" {
		t.Fatal("expected session to exist in registry during grace period")
	}

	u.RawQuery = fmt.Sprintf("token=%s&session_id=%s", token, activeSessionID)
	conn2, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("failed to reconnect websocket: %v", err)
	}
	defer conn2.Close()

	// Wait 250ms: since we reconnected, the grace period timer for activeSessionID should have been cancelled,
	// and onDisconnect should NOT be called.
	time.Sleep(250 * time.Millisecond)

	mu.Lock()
	if onDisconnectCalled {
		t.Error("onDisconnect was called despite successful session recovery")
	}
	mu.Unlock()

	// 3. Close the second connection and let it expire
	conn2.Close()

	// Wait for grace period expiration (should trigger wg.Done())
	c := make(chan struct{})
	go func() {
		disconnectWg.Wait()
		close(c)
	}()

	select {
	case <-c:
		// Success: onDisconnect was called after grace period expired
	case <-time.After(1 * time.Second):
		t.Error("timed out waiting for onDisconnect to be called after grace period expired")
	}
}

// Helper mock logger to avoid dependency on main zap setup in testing
func zapLoggerMock() *zap.Logger {
	return zap.NewNop()
}
