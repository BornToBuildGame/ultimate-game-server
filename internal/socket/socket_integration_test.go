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
	reg.GracePeriod = 200 * time.Millisecond // Optional legacy grace for session registry recovery

	var disconnectCount int
	var mu sync.Mutex

	handler := NewGatewayHandler(
		zapLoggerMock(),
		tm,
		reg,
		func(s *Session) {},
		func(sessionID, userID, username string) {
			mu.Lock()
			disconnectCount++
			mu.Unlock()
		},
		nil,
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

	// Close the connection — presence untrack fires immediately; session stays in registry during grace.
	conn.Close()
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	if disconnectCount != 1 {
		t.Errorf("expected onDisconnect once immediately, got %d", disconnectCount)
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

	// Wait briefly — grace timer cancelled by reconnect; no extra onDisconnect.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if disconnectCount != 1 {
		t.Errorf("expected onDisconnect still 1 after reconnect, got %d", disconnectCount)
	}
	mu.Unlock()

	// Close the second connection; onDisconnect fires immediately again.
	conn2.Close()
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	if disconnectCount != 2 {
		t.Errorf("expected onDisconnect twice after second close, got %d", disconnectCount)
	}
	mu.Unlock()

	// After grace expires the session is evicted from the registry.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		reg.mu.RLock()
		_, exists := reg.sessions[activeSessionID]
		reg.mu.RUnlock()
		if !exists {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("timed out waiting for session eviction after grace period")
}

// Helper mock logger to avoid dependency on main zap setup in testing
func zapLoggerMock() *zap.Logger {
	return zap.NewNop()
}
