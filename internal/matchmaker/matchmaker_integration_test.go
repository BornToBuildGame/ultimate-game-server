//go:build integration

package matchmaker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"go.uber.org/zap"
)

func TestMatchmaker_RedisIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 1. Spin up Redis Testcontainer
	redisContainer, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}
	defer func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Errorf("failed to terminate redis container: %v", err)
		}
	}()

	connStr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get redis connection string: %v", err)
	}

	opt, err := redis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("failed to parse redis connection string: %v", err)
	}
	rdb := redis.NewClient(opt)

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to ping redis container: %v", err)
	}

	// 2. Setup Matchmaker using Redis Client
	var matchedResults []MatchResult
	logger := zap.NewNop()
	mm := NewMatchmaker(logger, rdb, nil, nil, nil, func(res MatchResult) {
		matchedResults = append(matchedResults, res)
	})
	defer mm.Stop()

	// Configure queue
	queueName := "battle_royale"
	mm.Configure(&MatchmakerConfig{
		ProcessingIntervalMs: 1000,
		TicketExpirySec:      300,
		Queues: map[string]QueueConfig{
			queueName: {
				Name:          queueName,
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

	// Submit Player 1
	p1 := &Ticket{
		ID:          "p1_ticket",
		UserID:      "user_p1",
		Username:    "player_1",
		SkillRating: 1500,
		Region:      "us-west",
		CreatedAt:   time.Now(),
		QueueName:   queueName,
		StringProperties: map[string]string{
			"region": "us-west",
		},
	}
	err = mm.Submit(ctx, p1)
	if err != nil {
		t.Fatalf("failed to submit p1: %v", err)
	}

	// Submit Player 2
	p2 := &Ticket{
		ID:          "p2_ticket",
		UserID:      "user_p2",
		Username:    "player_2",
		SkillRating: 1510,
		Region:      "us-west",
		CreatedAt:   time.Now(),
		QueueName:   queueName,
		StringProperties: map[string]string{
			"region": "us-west",
		},
	}
	err = mm.Submit(ctx, p2)
	if err != nil {
		t.Fatalf("failed to submit p2: %v", err)
	}

	// 3. Verify Redis State Schema
	// Check queue sorted set key
	queueKey := "matchmaker:queue:" + queueName
	ticketCount, err := rdb.ZCard(ctx, queueKey).Result()
	if err != nil {
		t.Fatalf("failed to read queue sorted set: %v", err)
	}
	if ticketCount != 2 {
		t.Errorf("expected 2 tickets in redis queue sorted set, got: %d", ticketCount)
	}

	// Check ticket payload hash key
	ticketKey := "matchmaker:ticket:p1_ticket"
	payload, err := rdb.HGet(ctx, ticketKey, "payload").Result()
	if err != nil {
		t.Fatalf("failed to read ticket hash payload: %v", err)
	}
	var storedTicket Ticket
	if err := json.Unmarshal([]byte(payload), &storedTicket); err != nil {
		t.Fatalf("failed to unmarshal ticket: %v", err)
	}
	if storedTicket.Username != "player_1" {
		t.Errorf("expected stored ticket username to be player_1, got: %s", storedTicket.Username)
	}

	// Check user ticket mapping key
	userMapKey := "matchmaker:user:user_p1"
	storedTicketID, err := rdb.Get(ctx, userMapKey).Result()
	if err != nil {
		t.Fatalf("failed to read user mapping: %v", err)
	}
	if storedTicketID != "p1_ticket" {
		t.Errorf("expected mapped ticket ID to be p1_ticket, got: %s", storedTicketID)
	}

	// 4. Verify Redlock coordination
	// Acquire lock to simulate lock held by another process
	lockKey := "lock:matchmaker:" + queueName
	acquired, err := rdb.SetNX(ctx, lockKey, "lock_holder", 5*time.Second).Result()
	if err != nil {
		t.Fatalf("failed to set lock: %v", err)
	}
	if !acquired {
		t.Fatal("expected to acquire lock")
	}

	// Trigger matchmaking tick - should fail to acquire lock and not match
	mm.Tick(ctx)
	time.Sleep(100 * time.Millisecond)

	if len(matchedResults) != 0 {
		t.Errorf("expected no match to form while lock was held, but got: %v", matchedResults)
	}

	// Release lock
	_, err = rdb.Del(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("failed to release lock: %v", err)
	}

	// Trigger matchmaking tick again - should succeed
	mm.Tick(ctx)
	time.Sleep(100 * time.Millisecond)

	if len(matchedResults) != 1 {
		t.Fatalf("expected 1 match to form, got: %d", len(matchedResults))
	}
	res := matchedResults[0]
	if len(res.PlayerIDs) != 2 {
		t.Errorf("expected 2 players in match, got: %d", len(res.PlayerIDs))
	}

	// 5. Verify match status updates
	statusKey := "matchmaker:status:p1_ticket"
	statusStr, err := rdb.Get(ctx, statusKey).Result()
	if err != nil {
		t.Fatalf("failed to retrieve status: %v", err)
	}
	var ticketStatus struct {
		Status  string `json:"status"`
		MatchID string `json:"match_id"`
	}
	if err := json.Unmarshal([]byte(statusStr), &ticketStatus); err != nil {
		t.Fatalf("failed to unmarshal ticket status: %v", err)
	}
	if ticketStatus.Status != "matched" || ticketStatus.MatchID != res.MatchID {
		t.Errorf("expected status 'matched' and match ID %s, got: status=%s, matchID=%s", res.MatchID, ticketStatus.Status, ticketStatus.MatchID)
	}
}
