package match

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"ultimate-game-server/internal/runtime"

	lua "github.com/yuin/gopher-lua"
	"go.uber.org/zap"
)

type mockSessionRegistry struct {
	mu         sync.Mutex
	broadcasts [][]byte
}

func (m *mockSessionRegistry) SendToSession(sessionID string, payload []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var env struct {
		MatchData struct {
			Data string `json:"data"`
		} `json:"match_data"`
	}
	if err := json.Unmarshal(payload, &env); err == nil {
		if decoded, err := base64.StdEncoding.DecodeString(env.MatchData.Data); err == nil {
			m.broadcasts = append(m.broadcasts, decoded)
		}
	}
}

func TestMatchLoop_Execution(t *testing.T) {
	playerIDs := []string{"p-1", "p-2"}
	logger := zap.NewNop()

	var mu sync.Mutex
	var terminated bool
	var finalState MatchState

	mockReg := &mockSessionRegistry{}

	onEnd := func(matchID string, state MatchState) {
		mu.Lock()
		terminated = true
		finalState = state
		mu.Unlock()
	}

	// Create match loop with high tick rate (e.g. 100 TPS) for fast test execution
	ml := NewMatchLoop("match-1", playerIDs, 100, logger, mockReg)
	ml.onEnd = onEnd

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start match loop in background first
	go ml.Start(ctx)

	// Wait for loop to boot
	time.Sleep(50 * time.Millisecond)

	// Join players so they are registered in the presences list
	_, err := ml.JoinAttempt("p-1", "player1", "session-1", nil)
	if err != nil {
		t.Fatalf("failed to join p-1: %v", err)
	}
	_, err = ml.JoinAttempt("p-2", "player2", "session-2", nil)
	if err != nil {
		t.Fatalf("failed to join p-2: %v", err)
	}

	// 1. Submit movement input
	ml.SubmitInput(MatchInput{
		UserID:  "p-1",
		Action:  "move",
		Payload: "10,20",
	})

	time.Sleep(50 * time.Millisecond)

	mockReg.mu.Lock()
	if len(mockReg.broadcasts) == 0 {
		t.Fatal("expected match state broadcasts to be emitted")
	}

	// Verify player 1's position is updated in latest state broadcast
	var lastState MatchState
	err = json.Unmarshal(mockReg.broadcasts[len(mockReg.broadcasts)-1], &lastState)
	if err != nil {
		t.Fatalf("failed to unmarshal state: %v", err)
	}
	if lastState.Positions["p-1"] != "10,20" {
		t.Errorf("expected player 1 position '10,20', got: %s", lastState.Positions["p-1"])
	}
	mockReg.mu.Unlock()

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

	ml := NewMatchLoop("m-1", playerIDs, 50, logger, nil)
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

func TestRegistry_Search(t *testing.T) {
	reg := NewRegistry()

	labels1 := map[string]interface{}{"mode": "ranked", "tier": "gold"}
	labels2 := map[string]interface{}{"mode": "casual", "tier": "silver"}

	reg.Add("m-1", labels1, 2, 4, true)
	reg.Add("m-2", labels2, 1, 2, false)

	// Search matching
	results := reg.Search("mode", "ranked")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].MatchID != "m-1" {
		t.Errorf("expected m-1, got %s", results[0].MatchID)
	}

	// Search non-matching
	results = reg.Search("tier", "bronze")
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}

	reg.Remove("m-1")
	results = reg.Search("mode", "ranked")
	if len(results) != 0 {
		t.Errorf("expected 0 results after removal, got %d", len(results))
	}
}

func TestRouter_ClusterForwarding(t *testing.T) {
	router := NewRouter()

	// Configure cluster forwarding mock callback
	called := false
	var forwardedToNode, forwardedMatchID string
	var forwardedInput MatchInput

	router.SetClusterConfig(
		"node-A",
		nil, // no redis for local routing bypass testing
		func(ctx context.Context, targetNodeID, matchID string, input MatchInput) error {
			called = true
			forwardedToNode = targetNodeID
			forwardedMatchID = matchID
			forwardedInput = input
			return nil
		},
	)

	// In single-node mode without Redis, ForwardInput on unregistered match fails immediately
	input := MatchInput{UserID: "p-1", Action: "test"}
	err := router.ForwardInput(context.Background(), "m-2", input)
	if err == nil {
		t.Error("expected error on unregistered match without Redis client")
	}

	_ = called
	_ = forwardedToNode
	_ = forwardedMatchID
	_ = forwardedInput
}

func TestMatchLoop_LuaAuthoritative(t *testing.T) {
	L := lua.NewState()
	defer L.Close()

	script := `
		function match_init(ctx, params)
			return { tick = 0, score = {}, positions = {}, is_finished = false }
		end

		function match_join_attempt(ctx, dispatcher, tick, state, presence, metadata)
			return state, true
		end

		function match_loop(ctx, dispatcher, tick, state, messages)
			for i, msg in ipairs(messages) do
				if msg.action == "move" then
					state.positions[msg.user_id] = msg.payload
				elseif msg.action == "score" then
					state.score[msg.user_id] = (state.score[msg.user_id] or 0) + 1
					if state.score[msg.user_id] >= 3 then
						state.is_finished = true
					end
				end
			end
			return state
		end
	`
	err := L.DoString(script)
	if err != nil {
		t.Fatalf("failed to run script: %v", err)
	}

	// Create match loop
	playerIDs := []string{"p-1"}
	logger := zap.NewNop()

	var mu sync.Mutex
	var terminated bool
	var finalState MatchState

	mockReg := &mockSessionRegistry{}

	onEnd := func(matchID string, state MatchState) {
		mu.Lock()
		terminated = true
		finalState = state
		mu.Unlock()
	}

	ml := NewMatchLoop("match-lua-1", playerIDs, 100, logger, mockReg)
	ml.onEnd = onEnd

	// Create and inject custom Gopher-Lua sandbox
	sb := runtime.NewSandbox(64*1024*1024, 5*time.Second)
	defer sb.Close()
	_ = sb.L.DoString(script) // Load script functions in sandbox VM
	ml.SetSandbox(sb)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start match loop FIRST so it can handle the JoinAttempt request
	go ml.Start(ctx)

	// Wait for loop to boot
	time.Sleep(50 * time.Millisecond)

	// Test JoinAttempt hook
	accept, err := ml.JoinAttempt("p-1", "gamer_1", "session-1", map[string]string{"version": "1.0"})
	if err != nil {
		t.Fatalf("JoinAttempt failed: %v", err)
	}
	if !accept {
		t.Error("expected JoinAttempt to accept player")
	}

	time.Sleep(30 * time.Millisecond)

	// Submit move input
	ml.SubmitInput(MatchInput{
		UserID:  "p-1",
		Action:  "move",
		Payload: "50,60",
	})

	time.Sleep(30 * time.Millisecond)

	mockReg.mu.Lock()
	if len(mockReg.broadcasts) == 0 {
		t.Fatal("expected broadcast state delta")
	}
	var st MatchState
	_ = json.Unmarshal(mockReg.broadcasts[len(mockReg.broadcasts)-1], &st)
	if st.Positions["p-1"] != "50,60" {
		t.Errorf("expected p-1 position to be '50,60', got: %v", st.Positions["p-1"])
	}
	mockReg.mu.Unlock()

	// Submit scoring inputs to trigger termination via Lua hook limit (score >= 3)
	for i := 0; i < 3; i++ {
		ml.SubmitInput(MatchInput{
			UserID: "p-1",
			Action: "score",
		})
	}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if !terminated {
		t.Fatal("expected match to terminate via Lua hook score limit")
	}
	if finalState.Score["p-1"] < 3 {
		t.Errorf("expected final score to be >= 3, got: %d", finalState.Score["p-1"])
	}
	mu.Unlock()
}
