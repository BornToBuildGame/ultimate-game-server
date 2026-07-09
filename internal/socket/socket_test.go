package socket

import (
	"sync"
	"testing"
	"time"
)

func TestConnectionRegistry_ConcurrencyAndGracePeriod(t *testing.T) {
	reg := NewConnectionRegistry()

	userID := "user-999"
	sessionID := "session-999"

	session := &Session{
		ID:       sessionID,
		UserID:   userID,
		Username: "player_999",
		IsActive: true,
	}

	// 1. Add session
	reg.Add(session)

	s, ok := reg.GetBySession(sessionID)
	if !ok || s.ID != sessionID {
		t.Fatal("failed to retrieve registered session")
	}

	// 2. Start grace period
	cleanedUp := false
	var wg sync.WaitGroup
	wg.Add(1)

	// Inject custom cleanup function
	reg.StartGracePeriod(sessionID, func() {
		cleanedUp = true
		wg.Done()
	})

	// Verify session is marked inactive immediately
	s, ok = reg.GetBySession(sessionID)
	if !ok || s.IsActive {
		t.Error("expected session to be marked inactive during grace period")
	}

	// Wait 100ms: timer is for 30s, so it should NOT be cleaned up yet
	time.Sleep(100 * time.Millisecond)
	if cleanedUp {
		t.Error("unexpected early cleanup of session during grace period")
	}

	// 3. Simulate Reconnection: Adding the session again cancels grace timer
	newSession := &Session{
		ID:       sessionID,
		UserID:   userID,
		Username: "player_999",
		IsActive: true,
	}
	reg.Add(newSession)

	// Since we can't inspect the timer directly without reflection, we verify that
	// s is active again
	s, ok = reg.GetBySession(sessionID)
	if !ok || !s.IsActive {
		t.Error("expected re-added session to be marked active")
	}
}
