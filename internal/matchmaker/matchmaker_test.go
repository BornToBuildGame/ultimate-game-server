package matchmaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestMatchmaker_SubmitAndCancel(t *testing.T) {
	logger := zap.NewNop()
	mm := NewMatchmaker(logger, nil, nil, nil, nil, nil)
	defer mm.Stop()

	ticket := &Ticket{
		ID:          "ticket-1",
		UserID:      "user-1",
		Username:    "player-1",
		SessionID:   "sess-1",
		SkillRating: 1500,
		Region:      "us-east",
		CreatedAt:   time.Now(),
		MinCount:    2,
		MaxCount:    2,
	}

	ctx := context.Background()
	err := mm.Submit(ctx, ticket)
	if err != nil {
		t.Fatalf("failed to submit ticket: %v", err)
	}

	mm.mu.Lock()
	_, exists := mm.tickets["ticket-1"]
	mm.mu.Unlock()

	if !exists {
		t.Fatal("expected ticket to be registered in matchmaker")
	}

	err = mm.Cancel(ctx, "ticket-1")
	if err != nil {
		t.Fatalf("failed to cancel ticket: %v", err)
	}

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

	logger := zap.NewNop()
	mm := NewMatchmaker(logger, nil, nil, nil, nil, func(res MatchResult) {
		mu.Lock()
		matchedResults = append(matchedResults, res)
		mu.Unlock()
	})
	defer mm.Stop()

	mm.Configure(&MatchmakerConfig{
		ProcessingIntervalMs: 1000,
		TicketExpirySec:      300,
		MaxTickets:           3,
		MaxIntervals:         2,
		TokenTTLSec:          30,
		Queues: map[string]QueueConfig{
			"default": {
				Name:          "default",
				MinPlayers:    2,
				MaxPlayers:    2,
				CountMultiple: 1,
				SkillMatch: SkillMatchConfig{
					Enabled:              true,
					InitialRange:         50,
					MaxRange:             500,
					ExpansionIntervalSec: 5,
					ExpansionStep:        25,
				},
			},
		},
	})

	ctx := context.Background()

	t1 := &Ticket{
		ID:          "t-1",
		UserID:      "u-1",
		Username:    "player-1",
		SessionID:   "s-1",
		SkillRating: 1500,
		Region:      "us-east",
		CreatedAt:   time.Now(),
	}
	_ = mm.Submit(ctx, t1)

	t2 := &Ticket{
		ID:          "t-2",
		UserID:      "u-2",
		Username:    "player-2",
		SessionID:   "s-2",
		SkillRating: 1600,
		Region:      "us-east",
		CreatedAt:   time.Now(),
	}
	_ = mm.Submit(ctx, t2)

	mm.Tick(ctx)

	mu.Lock()
	if len(matchedResults) != 0 {
		t.Errorf("unexpected match formed immediately with delta 100 MMR: %v", matchedResults)
	}
	mu.Unlock()

	mm.mu.Lock()
	if ticket, ok := mm.tickets["t-1"]; ok {
		ticket.CreatedAt = time.Now().Add(-11 * time.Second)
	}
	if ticket, ok := mm.tickets["t-2"]; ok {
		ticket.CreatedAt = time.Now().Add(-11 * time.Second)
	}
	mm.mu.Unlock()

	mm.Tick(ctx)
	time.Sleep(100 * time.Millisecond)

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
	logger := zap.NewNop()
	mm := NewMatchmaker(logger, nil, nil, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mm.Start(ctx, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	mm.Stop()
	mm.Stop() // idempotent
}

func TestMatchmaker_PropertiesBleveQueryMatching(t *testing.T) {
	var mu sync.Mutex
	var matchedResults []MatchResult

	logger := zap.NewNop()
	mm := NewMatchmaker(logger, nil, nil, nil, nil, func(res MatchResult) {
		mu.Lock()
		matchedResults = append(matchedResults, res)
		mu.Unlock()
	})
	defer mm.Stop()

	mm.Configure(&MatchmakerConfig{
		ProcessingIntervalMs: 1000,
		TicketExpirySec:      300,
		MaxTickets:           3,
		MaxIntervals:         2,
		TokenTTLSec:          30,
		Queues: map[string]QueueConfig{
			"ranked_5v5": {
				Name:          "ranked_5v5",
				MinPlayers:    2,
				MaxPlayers:    2,
				CountMultiple: 1,
				SkillMatch: SkillMatchConfig{
					Enabled: false,
				},
				RegionMatch: RegionMatchConfig{
					Strict: true,
				},
				ReversePrecision: ReversePrecisionConfig{
					Enabled: false,
				},
			},
		},
	})

	ctx := context.Background()

	t1 := &Ticket{
		ID:          "ticket-p1",
		UserID:      "u-p1",
		Username:    "player-1",
		SessionID:   "sess-p1",
		SkillRating: 1500,
		Region:      "us-east",
		CreatedAt:   time.Now(),
		QueueName:   "ranked_5v5",
		Query:       "+properties.mode:casual",
		StringProperties: map[string]string{
			"region": "us-east",
			"mode":   "ranked",
		},
	}

	t2 := &Ticket{
		ID:          "ticket-p2",
		UserID:      "u-p2",
		Username:    "player-2",
		SessionID:   "sess-p2",
		SkillRating: 1510,
		Region:      "us-east",
		CreatedAt:   time.Now(),
		QueueName:   "ranked_5v5",
		Query:       "+properties.mode:ranked",
		StringProperties: map[string]string{
			"region": "us-east",
			"mode":   "casual",
		},
	}

	_ = mm.Submit(ctx, t1)
	_ = mm.Submit(ctx, t2)

	mm.Tick(ctx)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(matchedResults) != 1 {
		t.Fatalf("expected exactly 1 match formed, got: %d", len(matchedResults))
	}
	mu.Unlock()
}

func TestPerTicketMinMax(t *testing.T) {
	var mu sync.Mutex
	var matched []MatchResult
	mm := NewMatchmaker(zap.NewNop(), nil, nil, nil, nil, func(res MatchResult) {
		mu.Lock()
		matched = append(matched, res)
		mu.Unlock()
	})
	defer mm.Stop()

	mm.Configure(&MatchmakerConfig{
		MaxTickets:   3,
		MaxIntervals: 2,
		TokenTTLSec:  30,
		Queues: map[string]QueueConfig{
			"default": {
				Name:          "default",
				MinPlayers:    2,
				MaxPlayers:    4,
				CountMultiple: 1,
			},
		},
	})

	ctx := context.Background()
	_ = mm.Submit(ctx, &Ticket{
		ID: "a", UserID: "u1", Username: "p1", SessionID: "s1",
		MinCount: 2, MaxCount: 4, CreatedAt: time.Now(),
	})
	_ = mm.Submit(ctx, &Ticket{
		ID: "b", UserID: "u2", Username: "p2", SessionID: "s2",
		MinCount: 2, MaxCount: 4, CreatedAt: time.Now(),
	})

	// First tick: Intervals=1 < MaxIntervals=2 → softfill requires MaxCount=4
	mm.Tick(ctx)
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	if len(matched) != 0 {
		t.Fatalf("expected no softfill match on first tick, got %d", len(matched))
	}
	mu.Unlock()

	// Second tick: Intervals=2 → allow [MinCount, MaxCount]
	mm.Tick(ctx)
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	if len(matched) != 1 {
		t.Fatalf("expected softfill match on second tick, got %d", len(matched))
	}
	mu.Unlock()
}

func TestPartyExcludeSameParty(t *testing.T) {
	var mu sync.Mutex
	var matched []MatchResult
	mm := NewMatchmaker(zap.NewNop(), nil, nil, nil, nil, func(res MatchResult) {
		mu.Lock()
		matched = append(matched, res)
		mu.Unlock()
	})
	defer mm.Stop()

	mm.Configure(&MatchmakerConfig{
		MaxTickets:   3,
		MaxIntervals: 1,
		TokenTTLSec:  30,
		Queues: map[string]QueueConfig{
			"default": {Name: "default", MinPlayers: 2, MaxPlayers: 2, CountMultiple: 1},
		},
	})

	ctx := context.Background()
	partyID := "party-shared"
	_ = mm.Submit(ctx, &Ticket{
		ID: "t1", UserID: "u1", Username: "p1", SessionID: "s1",
		PartyID: partyID, MinCount: 2, MaxCount: 2, CreatedAt: time.Now(),
	})
	_ = mm.Submit(ctx, &Ticket{
		ID: "t2", UserID: "u2", Username: "p2", SessionID: "s2",
		PartyID: partyID, MinCount: 2, MaxCount: 2, CreatedAt: time.Now(),
	})

	mm.Tick(ctx)
	mm.Tick(ctx)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(matched) != 0 {
		t.Fatalf("expected same party tickets to be excluded, got match: %+v", matched)
	}
}

func TestMaxTickets(t *testing.T) {
	mm := NewMatchmaker(zap.NewNop(), nil, nil, nil, nil, nil)
	defer mm.Stop()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		err := mm.Submit(ctx, &Ticket{
			UserID: "u1", Username: "p1", SessionID: "sess-cap",
			MinCount: 2, MaxCount: 2,
		})
		if err != nil {
			t.Fatalf("ticket %d submit failed: %v", i+1, err)
		}
	}
	err := mm.Submit(ctx, &Ticket{
		UserID: "u1", Username: "p1", SessionID: "sess-cap",
		MinCount: 2, MaxCount: 2,
	})
	if !errors.Is(err, ErrTooManyTickets) {
		t.Fatalf("expected ErrTooManyTickets, got %v", err)
	}
}

func TestRemoveSessionAll(t *testing.T) {
	mm := NewMatchmaker(zap.NewNop(), nil, nil, nil, nil, nil)
	defer mm.Stop()
	ctx := context.Background()

	_ = mm.Submit(ctx, &Ticket{ID: "x1", UserID: "u1", SessionID: "sess-x", MinCount: 2, MaxCount: 2})
	_ = mm.Submit(ctx, &Ticket{ID: "x2", UserID: "u1", SessionID: "sess-x", MinCount: 2, MaxCount: 2})
	_ = mm.Submit(ctx, &Ticket{ID: "y1", UserID: "u2", SessionID: "sess-y", MinCount: 2, MaxCount: 2})

	if err := mm.RemoveSessionAll(ctx, "sess-x"); err != nil {
		t.Fatalf("RemoveSessionAll: %v", err)
	}

	mm.mu.Lock()
	defer mm.mu.Unlock()
	if _, ok := mm.tickets["x1"]; ok {
		t.Fatal("expected x1 removed")
	}
	if _, ok := mm.tickets["x2"]; ok {
		t.Fatal("expected x2 removed")
	}
	if _, ok := mm.tickets["y1"]; !ok {
		t.Fatal("expected y1 still present")
	}
}

func TestMatchTokenConsume(t *testing.T) {
	var mu sync.Mutex
	var matched []MatchResult
	mm := NewMatchmaker(zap.NewNop(), nil, nil, nil, nil, func(res MatchResult) {
		mu.Lock()
		matched = append(matched, res)
		mu.Unlock()
	})
	defer mm.Stop()

	mm.Configure(&MatchmakerConfig{
		MaxTickets:   3,
		MaxIntervals: 1,
		TokenTTLSec:  30,
		Queues: map[string]QueueConfig{
			"default": {Name: "default", MinPlayers: 2, MaxPlayers: 2, CountMultiple: 1},
		},
	})

	ctx := context.Background()
	_ = mm.Submit(ctx, &Ticket{ID: "m1", UserID: "u1", Username: "a", SessionID: "s1", MinCount: 2, MaxCount: 2})
	_ = mm.Submit(ctx, &Ticket{ID: "m2", UserID: "u2", Username: "b", SessionID: "s2", MinCount: 2, MaxCount: 2})
	mm.Tick(ctx)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(matched) != 1 {
		mu.Unlock()
		t.Fatalf("expected match, got %d", len(matched))
	}
	token := matched[0].MatchToken
	mu.Unlock()

	if token == "" {
		t.Fatal("expected non-empty match token for non-authoritative match")
	}

	res, err := mm.ConsumeMatchToken(ctx, token)
	if err != nil {
		t.Fatalf("first consume failed: %v", err)
	}
	if res.MatchID == "" {
		t.Fatal("expected match id from token")
	}

	_, err = mm.ConsumeMatchToken(ctx, token)
	if !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("expected ErrTokenInvalid on second consume, got %v", err)
	}
}
