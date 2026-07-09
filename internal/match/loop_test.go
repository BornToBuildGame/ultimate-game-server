package match

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestMatchLoop_Execution(t *testing.T) {
	playerIDs := []string{"p-1", "p-2"}
	logger := zap.NewNop()

	var mu sync.Mutex
	var broadcastStates [][]byte
	var terminated bool
	var finalState MatchState

	onBroadcast := func(matchID string, stateJson []byte) {
		mu.Lock()
		broadcastStates = append(broadcastStates, stateJson)
		mu.Unlock()
	}

	onEnd := func(matchID string, state MatchState) {
		mu.Lock()
		terminated = true
		finalState = state
		mu.Unlock()
	}

	// Create match loop with high tick rate (e.g. 100 TPS) for fast test execution
	ml := NewMatchLoop("match-1", playerIDs, 100, logger, onBroadcast, onEnd)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start match loop in background
	go ml.Start(ctx)

	// Wait for loop to boot and run a few ticks
	time.Sleep(50 * time.Millisecond)

	// 1. Submit movement input
	ml.SubmitInput(MatchInput{
		UserID:  "p-1",
		Action:  "move",
		Payload: "10,20",
	})

	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	if len(broadcastStates) == 0 {
		t.Fatal("expected match state broadcasts to be emitted")
	}

	// Verify player 1's position is updated in latest state broadcast
	var lastState MatchState
	err := json.Unmarshal(broadcastStates[len(broadcastStates)-1], &lastState)
	if err != nil {
		t.Fatalf("failed to unmarshal state: %v", err)
	}
	if lastState.Positions["p-1"] != "10,20" {
		t.Errorf("expected player 1 position '10,20', got: %s", lastState.Positions["p-1"])
	}
	mu.Unlock()

	// 2. Submit scoring inputs to trigger termination
	// We score 10 times for p-1
	for i := 0; i < 10; i++ {
		ml.SubmitInput(MatchInput{
			UserID: "p-1",
			Action: "score",
		})
	}

	// Loop should detect p-1 score >= 10 and terminate automatically within a few ticks
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if !terminated {
		t.Fatal("expected match loop to terminate automatically after score limit reached")
	}
	if finalState.Score["p-1"] < 10 {
		t.Errorf("expected final score of p-1 to be >= 10, got: %d", finalState.Score["p-1"])
	}
	if !finalState.IsFinished {
		t.Error("expected final state to mark IsFinished as true")
	}
	mu.Unlock()
}

func TestRouter_ForwardInput(t *testing.T) {
	router := NewRouter()
	playerIDs := []string{"p-1"}
	logger := zap.NewNop()

	ml := NewMatchLoop("m-1", playerIDs, 50, logger, nil, nil)
	router.Register("m-1", ml)

	// Forward input to registered match
	input := MatchInput{
		UserID:  "p-1",
		Action:  "move",
		Payload: "5,5",
	}
	err := router.ForwardInput(context.Background(), "m-1", input)
	if err != nil {
		t.Fatalf("expected forward input to succeed: %v", err)
	}

	// Verify input is in match queue buffer
	select {
	case in := <-ml.inputBuffer:
		if in.UserID != "p-1" || in.Action != "move" || in.Payload != "5,5" {
			t.Errorf("unexpected input in queue: %v", in)
		}
	default:
		t.Fatal("expected input to be submitted to queue")
	}

	// Forwarding to unregistered match should fail
	err = router.ForwardInput(context.Background(), "m-nonexistent", input)
	if err == nil {
		t.Error("expected error forwarding to nonexistent match, got nil")
	}

	router.Unregister("m-1")
}
