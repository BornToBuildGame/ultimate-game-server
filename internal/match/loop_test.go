package match

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/runtime"

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

	// Lua path no longer auto-broadcasts full state; verify via loop state.
	ml.mu.RLock()
	pos := ml.state.Positions["p-1"]
	ml.mu.RUnlock()
	if pos != "50,60" {
		t.Errorf("expected p-1 position to be '50,60', got: %v", pos)
	}

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

type terminateTrackingMatch struct {
	mu             sync.Mutex
	terminateCalls int
	joinAttempts   int
	loopReturnsNil bool
	deferredData   []byte
}

func (m *terminateTrackingMatch) MatchInit(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, params map[string]interface{}) (interface{}, int, string) {
	return map[string]interface{}{"ok": true}, 30, "{}"
}

func (m *terminateTrackingMatch) MatchJoinAttempt(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presence runtime.Presence, metadata map[string]string) (interface{}, bool, string) {
	m.mu.Lock()
	m.joinAttempts++
	m.mu.Unlock()
	return state, true, ""
}

func (m *terminateTrackingMatch) MatchJoin(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []runtime.Presence) interface{} {
	return state
}

func (m *terminateTrackingMatch) MatchLeave(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []runtime.Presence) interface{} {
	return state
}

func (m *terminateTrackingMatch) MatchLoop(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, messages []runtime.MatchData) interface{} {
	if m.deferredData != nil {
		if d, ok := dispatcher.(interface {
			BroadcastMessageDeferred(opCode int64, data []byte, presences []runtime.Presence, sender runtime.Presence, reliable bool) error
		}); ok {
			_ = d.BroadcastMessageDeferred(7, m.deferredData, nil, nil, true)
		}
	}
	m.mu.Lock()
	end := m.loopReturnsNil && m.joinAttempts > 0
	m.mu.Unlock()
	if end {
		return nil
	}
	return state
}

func (m *terminateTrackingMatch) MatchTerminate(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, graceSeconds int) interface{} {
	m.mu.Lock()
	m.terminateCalls++
	m.mu.Unlock()
	return state
}

func (m *terminateTrackingMatch) MatchSignal(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, data string) (interface{}, string) {
	return state, data
}

func TestMatchTerminateInvoked(t *testing.T) {
	logger := zap.NewNop()
	mockMatch := &terminateTrackingMatch{loopReturnsNil: true}
	ml := NewMatchLoop("m-term", nil, 50, logger, nil)
	ml.SetGoMatch(mockMatch, map[string]interface{}{"v": 1}, nil, nil, nil)
	ml.graceSeconds = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ml.Start(ctx)

	time.Sleep(20 * time.Millisecond)
	accept, err := ml.JoinAttempt("u1", "user1", "s1", nil)
	if err != nil || !accept {
		t.Fatalf("join failed: accept=%v err=%v", accept, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mockMatch.mu.Lock()
		calls := mockMatch.terminateCalls
		mockMatch.mu.Unlock()
		if calls > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	mockMatch.mu.Lock()
	calls := mockMatch.terminateCalls
	mockMatch.mu.Unlock()
	t.Fatalf("expected MatchTerminate to be called, got %d calls", calls)
}

func TestStateQuotaEndsMatch(t *testing.T) {
	logger := zap.NewNop()
	ml := NewMatchLoop("m-quota", nil, 30, logger, nil)
	// Force oversized goState past maxMatchStateBytes (1 MiB)
	big := strings.Repeat("x", maxMatchStateBytes+1024)
	ml.SetGoMatch(&terminateTrackingMatch{}, map[string]interface{}{"blob": big}, nil, nil, nil)

	finished := ml.tick()
	if !finished {
		t.Fatal("expected match to finish when state exceeds quota")
	}
	ml.mu.RLock()
	finishedFlag := ml.state.IsFinished
	ml.mu.RUnlock()
	if !finishedFlag {
		t.Fatal("expected IsFinished after state quota breach")
	}
}

func TestDeferredBroadcastFlushed(t *testing.T) {
	logger := zap.NewNop()
	reg := &mockSessionRegistry{}
	mockMatch := &terminateTrackingMatch{deferredData: []byte(`{"hello":"deferred"}`)}
	ml := NewMatchLoop("m-def", nil, 30, logger, reg)
	ml.SetGoMatch(mockMatch, map[string]interface{}{"ok": true}, nil, nil, nil)
	ml.presences["s1"] = &PresenceImpl{UserID: "u1", SessionID: "s1", Username: "user1"}

	_ = ml.tick()

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if len(reg.broadcasts) == 0 {
		t.Fatal("expected deferred broadcast flushed to session registry")
	}
	found := false
	for _, b := range reg.broadcasts {
		if strings.Contains(string(b), "deferred") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected deferred payload in broadcasts, got: %v", reg.broadcasts)
	}
}

func TestAuthoritativeMatchIDHasNodeSuffix(t *testing.T) {
	id := NewAuthoritativeMatchID()
	if !strings.Contains(id, ".") {
		t.Fatalf("expected uuid.node format, got %s", id)
	}
	parts := strings.SplitN(id, ".", 2)
	if parts[0] == "" || parts[1] == "" {
		t.Fatalf("expected non-empty uuid and node, got %s", id)
	}
	if parts[1] != ResolveNodeID() {
		t.Fatalf("expected node suffix %s, got %s", ResolveNodeID(), parts[1])
	}
}

func TestJoinAttemptAlreadyMember(t *testing.T) {
	logger := zap.NewNop()
	mockMatch := &terminateTrackingMatch{}
	ml := NewMatchLoop("m-member", nil, 50, logger, nil)
	ml.SetGoMatch(mockMatch, map[string]interface{}{"ok": true}, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ml.Start(ctx)
	time.Sleep(20 * time.Millisecond)

	accept, err := ml.JoinAttempt("u1", "user1", "s1", nil)
	if err != nil || !accept {
		t.Fatalf("first join failed: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	mockMatch.mu.Lock()
	firstAttempts := mockMatch.joinAttempts
	mockMatch.mu.Unlock()

	accept, err = ml.JoinAttempt("u1", "user1", "s1", nil)
	if err != nil || !accept {
		t.Fatalf("second join (already member) failed: %v", err)
	}
	mockMatch.mu.Lock()
	secondAttempts := mockMatch.joinAttempts
	mockMatch.mu.Unlock()
	if secondAttempts != firstAttempts {
		t.Fatalf("expected already-member to skip JoinAttempt handler, attempts %d -> %d", firstAttempts, secondAttempts)
	}
}

func TestCreateAndRegisterMatch_LuaModule(t *testing.T) {
	r := NewRouter()
	hr := runtime.NewHookRegistry()
	r.SetDependencies(hr, nil, zap.NewNop(), nil, nil)
	r.RegisterLuaMatchSource("echo_match", `
		function match_init(ctx, params)
			local label = "{}"
			if params and params.label then label = params.label end
			return { tick = 0, score = {}, positions = {}, is_finished = false }, 15, label
		end
		function match_join_attempt(ctx, dispatcher, tick, state, presence, metadata)
			if presence.user_id == "banned" then
				return state, false, "not allowed"
			end
			assert(ctx.user_id == presence.user_id)
			assert(ctx.session_id == presence.session_id)
			return state, true, nil
		end
		function match_loop(ctx, dispatcher, tick, state, messages)
			return state
		end
	`)

	matchID := "lua-match-1"
	err := r.CreateAndRegisterMatch(context.Background(), matchID, "echo_match", map[string]interface{}{"label": `{"map":"a"}`})
	if err != nil {
		t.Fatalf("CreateAndRegisterMatch lua: %v", err)
	}
	loop, ok := r.GetMatchLoop(matchID)
	if !ok || loop == nil {
		t.Fatal("expected match loop registered")
	}
	if loop.tickRate != 15 {
		t.Fatalf("tick rate want 15 got %d", loop.tickRate)
	}
	if loop.label != `{"map":"a"}` {
		t.Fatalf("label mismatch: %q", loop.label)
	}

	accept, err := loop.JoinAttempt("u1", "alice", "sess-1", nil)
	if err != nil || !accept {
		t.Fatalf("join expected accept: %v %v", accept, err)
	}
	accept, err = loop.JoinAttempt("banned", "bad", "sess-2", nil)
	if err == nil && accept {
		t.Fatal("expected reject for banned user")
	}
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("expected reject reason, got err=%v accept=%v", err, accept)
	}

	r.Unregister(matchID)
}

func TestLuaBroadcastPresenceFilter(t *testing.T) {
	script := `
		function match_init(ctx, params)
			return { tick = 0, score = {}, positions = {}, is_finished = false }, 10, "{}"
		end
		function match_join_attempt(ctx, dispatcher, tick, state, presence, metadata)
			return state, true, nil
		end
		function match_loop(ctx, dispatcher, tick, state, messages)
			for _, msg in ipairs(messages) do
				if msg.action == "ping" then
					dispatcher.broadcast_message(1, "hi", {{
						user_id = "u2", session_id = "s2", username = "bob"
					}}, {
						user_id = msg.user_id, session_id = "s1", username = "alice"
					}, true)
				end
			end
			return state
		end
	`
	reg := &recordingRegistry{}
	ml := NewMatchLoop("m-bcast", nil, 20, zap.NewNop(), reg)
	sb := runtime.NewSandbox(64*1024*1024, 5*time.Second)
	defer sb.Close()
	_ = sb.L.DoString(script)
	ml.SetSandbox(sb)
	_, _, err := ml.callLuaMatchInit(nil)
	if err != nil {
		t.Fatal(err)
	}
	ml.presences["s1"] = &PresenceImpl{UserID: "u1", SessionID: "s1", Username: "alice"}
	ml.presences["s2"] = &PresenceImpl{UserID: "u2", SessionID: "s2", Username: "bob"}
	ml.presences["s3"] = &PresenceImpl{UserID: "u3", SessionID: "s3", Username: "carol"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ml.Start(ctx)
	time.Sleep(20 * time.Millisecond)
	ml.SubmitInput(MatchInput{UserID: "u1", Action: "ping", Payload: "x"})
	time.Sleep(50 * time.Millisecond)

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if len(reg.sent) == 0 {
		t.Fatal("expected broadcast")
	}
	for sid := range reg.sent {
		if sid != "s2" {
			t.Fatalf("expected only s2 targeted, got %v", reg.sent)
		}
	}
}

type recordingRegistry struct {
	mu   sync.Mutex
	sent map[string][]byte
}

func (r *recordingRegistry) SendToSession(sessionID string, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sent == nil {
		r.sent = map[string][]byte{}
	}
	r.sent[sessionID] = append([]byte(nil), data...)
}
