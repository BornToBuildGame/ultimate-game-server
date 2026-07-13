package matchmaker

import (
	"context"
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
		SkillRating: 1500,
		Region:      "us-east",
		CreatedAt:   time.Now(),
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

	ctx := context.Background()

	// 1. Submit Player 1 (MMR: 1500, us-east)
	t1 := &Ticket{
		ID:          "t-1",
		UserID:      "u-1",
		Username:    "player-1",
		SkillRating: 1500,
		Region:      "us-east",
		CreatedAt:   time.Now(),
	}
	_ = mm.Submit(ctx, t1)

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
	_ = mm.Submit(ctx, t2)

	// Trigger matchmaking tick immediately
	mm.Tick(ctx)

	mu.Lock()
	if len(matchedResults) != 0 {
		t.Errorf("unexpected match formed immediately with delta 100 MMR: %v", matchedResults)
	}
	mu.Unlock()

	// 3. Simulate wait time expansion
	// Under the progressive MMR expansion table:
	// - Wait time < 10s: ±75 MMR allowed (100 MMR delta won't match)
	// - Wait time < 20s: ±125 MMR allowed (100 MMR delta matches!)
	// So we set the wait time to 11 seconds.
	mm.mu.Lock()
	if ticket, ok := mm.tickets["t-1"]; ok {
		ticket.CreatedAt = time.Now().Add(-11 * time.Second)
	}
	mm.mu.Unlock()

	// Trigger matchmaking tick again
	mm.Tick(ctx)

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
	logger := zap.NewNop()
	mm := NewMatchmaker(logger, nil, nil, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mm.Start(ctx, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	mm.Stop()
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

	// Configure a queue with skill match enabled
	mm.Configure(&MatchmakerConfig{
		ProcessingIntervalMs: 1000,
		TicketExpirySec:      300,
		Queues: map[string]QueueConfig{
			"ranked_5v5": {
				Name:          "ranked_5v5",
				MinPlayers:    2,
				MaxPlayers:    2,
				CountMultiple: 1,
				SkillMatch: SkillMatchConfig{
					Enabled: true,
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

	// Player 1: region us-east, level 15, looking for casual mode
	t1 := &Ticket{
		ID:          "ticket-p1",
		UserID:      "u-p1",
		Username:    "player-1",
		SkillRating: 1500,
		Region:      "us-east",
		CreatedAt:   time.Now(),
		QueueName:   "ranked_5v5",
		Query:       "+properties.mode:casual",
		StringProperties: map[string]string{
			"region": "us-east",
			"mode":   "ranked", // this is t1's mode (ranked)
		},
	}

	// Player 2: region us-east, level 10, looking for ranked mode
	t2 := &Ticket{
		ID:          "ticket-p2",
		UserID:      "u-p2",
		Username:    "player-2",
		SkillRating: 1510,
		Region:      "us-east",
		CreatedAt:   time.Now(),
		QueueName:   "ranked_5v5",
		Query:       "+properties.mode:ranked",
		StringProperties: map[string]string{
			"region": "us-east",
			"mode":   "casual", // this is t2's mode (casual)
		},
	}

	_ = mm.Submit(ctx, t1)
	_ = mm.Submit(ctx, t2)

	// Tick matchmaking
	mm.Tick(ctx)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(matchedResults) != 1 {
		t.Fatalf("expected exactly 1 match formed, got: %d", len(matchedResults))
	}
	mu.Unlock()
}
