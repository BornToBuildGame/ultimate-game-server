package matchmaker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMatchmaker_SubmitAndCancel(t *testing.T) {
	mm := NewMatchmaker(nil)
	defer mm.Stop()

	ticket := &Ticket{
		ID:          "ticket-1",
		UserID:      "user-1",
		Username:    "player-1",
		SkillRating: 1500,
		Region:      "us-east",
		CreatedAt:   time.Now(),
	}

	mm.Submit(ticket)

	mm.mu.Lock()
	_, exists := mm.tickets["ticket-1"]
	mm.mu.Unlock()

	if !exists {
		t.Fatal("expected ticket to be registered in matchmaker")
	}

	mm.Cancel("ticket-1")

	mm.mu.Lock()
	_, exists = mm.tickets["ticket-1"]
	mm.mu.Unlock()

	if exists {
		t.Fatal("expected ticket to be removed after cancellation")
	}
}

func TestMatchmaker_MMRExpansionCurve(t *testing.T) {
	var mu sync.Mutex
	var matchedResults []MatchResult

	mm := NewMatchmaker(func(res MatchResult) {
		mu.Lock()
		matchedResults = append(matchedResults, res)
		mu.Unlock()
	})
	defer mm.Stop()

	// 1. Submit Player 1 (MMR: 1500, us-east)
	t1 := &Ticket{
		ID:          "t-1",
		UserID:      "u-1",
		Username:    "player-1",
		SkillRating: 1500,
		Region:      "us-east",
		CreatedAt:   time.Now(),
	}
	mm.Submit(t1)

	// 2. Submit Player 2 (MMR: 1600, us-east) -> Delta is 100 MMR
	// This delta (100 MMR) is greater than the initial allowed range (50 MMR) for new tickets.
	// So they should NOT match immediately.
	t2 := &Ticket{
		ID:          "t-2",
		UserID:      "u-2",
		Username:    "player-2",
		SkillRating: 1600,
		Region:      "us-east",
		CreatedAt:   time.Now(),
	}
	mm.Submit(t2)

	// Trigger matchmaking tick immediately
	mm.Tick()

	mu.Lock()
	if len(matchedResults) != 0 {
		t.Errorf("unexpected match formed immediately with delta 100 MMR: %v", matchedResults)
	}
	mu.Unlock()

	// 3. Simulate wait time expansion
	// We update Player 1's CreatedAt time to be 6 seconds ago.
	// Allowed range is: 50 + wait_time * 10 => 50 + 60 = 110 MMR.
	// Now 100 MMR delta should fit within the allowed range!
	mm.mu.Lock()
	if ticket, ok := mm.tickets["t-1"]; ok {
		ticket.CreatedAt = time.Now().Add(-6 * time.Second)
	}
	mm.mu.Unlock()

	// Trigger matchmaking tick again
	mm.Tick()

	time.Sleep(100 * time.Millisecond) // wait for callback go-routine

	mu.Lock()
	if len(matchedResults) != 1 {
		t.Fatalf("expected exactly 1 match result, got: %d", len(matchedResults))
	}
	res := matchedResults[0]
	mu.Unlock()

	if res.MatchID == "" {
		t.Error("expected non-empty MatchID")
	}
	hasU1 := false
	hasU2 := false
	for _, pid := range res.PlayerIDs {
		if pid == "u-1" {
			hasU1 = true
		}
		if pid == "u-2" {
			hasU2 = true
		}
	}
	if len(res.PlayerIDs) != 2 || !hasU1 || !hasU2 {
		t.Errorf("expected players u-1 and u-2 to be matched, got: %v", res.PlayerIDs)
	}
}

func TestMatchmaker_StartStop(t *testing.T) {
	mm := NewMatchmaker(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mm.Start(ctx, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	mm.Stop()
}
