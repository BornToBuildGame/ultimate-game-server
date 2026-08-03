package taixiu

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/pkg/runtime"
)

// mockLogger implements runtime.Logger for unit tests.
type mockLogger struct {
	t *testing.T
}

func (m *mockLogger) Debug(format string, args ...interface{}) { m.t.Logf("[DEBUG] "+format, args...) }
func (m *mockLogger) Info(format string, args ...interface{})  { m.t.Logf("[INFO] "+format, args...) }
func (m *mockLogger) Warn(format string, args ...interface{})  { m.t.Logf("[WARN] "+format, args...) }
func (m *mockLogger) Error(format string, args ...interface{}) { m.t.Logf("[ERROR] "+format, args...) }

// mockDispatcher records broadcasts sent during match loop.
type mockDispatcher struct {
	mu         sync.Mutex
	broadcasts []broadcastRecord
}

type broadcastRecord struct {
	OpCode    int64
	Data      []byte
	Presences []runtime.Presence
}

func (d *mockDispatcher) BroadcastMessage(opCode int64, data []byte, presences []runtime.Presence, sender runtime.Presence, reliable bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.broadcasts = append(d.broadcasts, broadcastRecord{
		OpCode:    opCode,
		Data:      data,
		Presences: presences,
	})
	return nil
}

func (d *mockDispatcher) findLastBroadcast(opCode int64) *broadcastRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := len(d.broadcasts) - 1; i >= 0; i-- {
		if d.broadcasts[i].OpCode == opCode {
			rec := d.broadcasts[i]
			return &rec
		}
	}
	return nil
}

// mockPresence implements runtime.Presence for tests.
type mockPresence struct {
	userID    string
	sessionID string
	username  string
}

func (p *mockPresence) GetUserId() string    { return p.userID }
func (p *mockPresence) GetSessionId() string { return p.sessionID }
func (p *mockPresence) GetNodeId() string    { return "node-1" }
func (p *mockPresence) GetUsername() string  { return p.username }

// mockMatchData implements runtime.MatchData for tests.
type mockMatchData struct {
	mockPresence
	opCode int64
	data   []byte
}

func (m *mockMatchData) GetOpCode() int64      { return m.opCode }
func (m *mockMatchData) GetData() []byte       { return m.data }
func (m *mockMatchData) GetReliable() bool     { return true }
func (m *mockMatchData) GetReceiveTime() int64 { return time.Now().UnixMilli() }

// mockRuntimeModule simulates wallet updates in unit test mode.
type mockRuntimeModule struct {
	runtime.RuntimeModule
	mu      sync.Mutex
	wallets map[string]map[string]int64
}

func newMockRuntimeModule() *mockRuntimeModule {
	return &mockRuntimeModule{
		wallets: make(map[string]map[string]int64),
	}
}

func (m *mockRuntimeModule) WalletUpdate(ctx context.Context, userID string, changeset map[string]int64, metadata map[string]interface{}, updateLedger bool) (map[string]int64, map[string]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w, exists := m.wallets[userID]
	if !exists {
		w = map[string]int64{"coins": 1000}
		m.wallets[userID] = w
	}

	prev := map[string]int64{"coins": w["coins"]}
	if change, ok := changeset["coins"]; ok {
		if w["coins"]+change < 0 {
			return nil, nil, fmt.Errorf("insufficient funds: current %d, required %d", w["coins"], -change)
		}
		w["coins"] += change
	}

	updated := map[string]int64{"coins": w["coins"]}
	return updated, prev, nil
}


func TestTaiXiuMatch_FullGameLoop(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{t: t}
	nk := newMockRuntimeModule()
	disp := &mockDispatcher{}

	match := NewTaiXiuMatch()

	// 1. Initialize Match Room
	params := map[string]interface{}{"betting_ticks": 2}
	state, tickRate, label := match.MatchInit(ctx, logger, nil, nk, params)
	if tickRate != 1 {
		t.Fatalf("expected tickRate 1, got %d", tickRate)
	}
	if label == "" {
		t.Fatal("expected non-empty label")
	}

	// 2. Players Join Match
	p1 := &mockPresence{userID: "user-1", sessionID: "sess-1", username: "player1"}
	p2 := &mockPresence{userID: "user-2", sessionID: "sess-2", username: "player2"}

	state = match.MatchJoin(ctx, logger, nil, nk, disp, 1, state, []runtime.Presence{p1, p2})

	// Setup initial wallets
	nk.wallets["user-1"] = map[string]int64{"coins": 1000}
	nk.wallets["user-2"] = map[string]int64{"coins": 1000}

	// 3. Tick 1 (Phase: BETTING) - Players Place Bets
	bet1Payload, _ := json.Marshal(BetRequest{Choice: ChoiceTai, Amount: 100})
	msg1 := &mockMatchData{mockPresence: *p1, opCode: OpCodeBet, data: bet1Payload}

	bet2Payload, _ := json.Marshal(BetRequest{Choice: ChoiceXiu, Amount: 200})
	msg2 := &mockMatchData{mockPresence: *p2, opCode: OpCodeBet, data: bet2Payload}

	state = match.MatchLoop(ctx, logger, nil, nk, disp, 1, state, []runtime.MatchData{msg1, msg2})

	s := state.(*TaiXiuState)
	if s.TotalPoolTai != 100 {
		t.Errorf("expected total pool TAI 100, got %d", s.TotalPoolTai)
	}
	if s.TotalPoolXiu != 200 {
		t.Errorf("expected total pool XIU 200, got %d", s.TotalPoolXiu)
	}

	// Verify wallet balance after bet placement
	if nk.wallets["user-1"]["coins"] != 900 {
		t.Errorf("expected user-1 wallet balance 900 after bet, got %d", nk.wallets["user-1"]["coins"])
	}
	if nk.wallets["user-2"]["coins"] != 800 {
		t.Errorf("expected user-2 wallet balance 800 after bet, got %d", nk.wallets["user-2"]["coins"])
	}

	// 4. Tick 2 (Transition from BETTING -> ROLLING)
	state = match.MatchLoop(ctx, logger, nil, nk, disp, 2, state, nil)
	if s.Phase != PhaseRolling {
		t.Errorf("expected phase ROLLING, got %s", s.Phase)
	}

	// 5. Tick 3 & 4 (Transition from ROLLING -> RESULT)
	state = match.MatchLoop(ctx, logger, nil, nk, disp, 3, state, nil)
	state = match.MatchLoop(ctx, logger, nil, nk, disp, 4, state, nil)

	if s.Phase != PhaseResult {
		t.Errorf("expected phase RESULT, got %s", s.Phase)
	}

	// Verify Round Outcome
	if s.LastSum < 3 || s.LastSum > 18 {
		t.Errorf("expected valid dice sum 3-18, got %d", s.LastSum)
	}
	t.Logf("🎲 Dice Result: %v (Sum: %d, Outcome: %s)", s.LastDice, s.LastSum, s.LastWinning)

	// Check Payouts
	for _, payout := range s.LastPayouts {
		if payout.Choice == s.LastWinning {
			expectedNet := payout.BetAmount
			if payout.NetProfit != expectedNet {
				t.Errorf("expected winner net profit %d, got %d", expectedNet, payout.NetProfit)
			}
			t.Logf("🏆 Winner %s won payout %d (Net: +%d, New Balance: %d)", payout.UserID, payout.Payout, payout.NetProfit, payout.NewBalance)
		} else {
			if payout.Payout != 0 {
				t.Errorf("expected loser payout 0, got %d", payout.Payout)
			}
			t.Logf("❌ Loser %s lost bet %d (Net: %d, New Balance: %d)", payout.UserID, payout.BetAmount, payout.NetProfit, payout.NewBalance)
		}
	}

	// 6. Advance through RESULT -> INTERMISSION -> Round 2 BETTING
	state = match.MatchLoop(ctx, logger, nil, nk, disp, 5, state, nil)
	state = match.MatchLoop(ctx, logger, nil, nk, disp, 6, state, nil)
	state = match.MatchLoop(ctx, logger, nil, nk, disp, 7, state, nil)
	state = match.MatchLoop(ctx, logger, nil, nk, disp, 8, state, nil)
	state = match.MatchLoop(ctx, logger, nil, nk, disp, 9, state, nil)

	if s.RoundNumber != 2 {
		t.Errorf("expected round number 2, got %d", s.RoundNumber)
	}
	if s.Phase != PhaseBetting {
		t.Errorf("expected phase BETTING for round 2, got %s", s.Phase)
	}
	if s.TotalPoolTai != 0 || s.TotalPoolXiu != 0 {
		t.Errorf("expected empty pools for round 2, got TAI=%d, XIU=%d", s.TotalPoolTai, s.TotalPoolXiu)
	}
}

func TestTaiXiuMatch_ValidationErrors(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{t: t}
	nk := newMockRuntimeModule()
	disp := &mockDispatcher{}

	match := NewTaiXiuMatch()
	state, _, _ := match.MatchInit(ctx, logger, nil, nk, map[string]interface{}{"betting_ticks": 10})

	p1 := &mockPresence{userID: "user-1", sessionID: "sess-1", username: "player1"}
	state = match.MatchJoin(ctx, logger, nil, nk, disp, 1, state, []runtime.Presence{p1})
	nk.wallets["user-1"] = map[string]int64{"coins": 50}

	// 1. Invalid Choice
	badChoicePayload, _ := json.Marshal(BetRequest{Choice: "INVALID", Amount: 10})
	msg := &mockMatchData{mockPresence: *p1, opCode: OpCodeBet, data: badChoicePayload}
	match.MatchLoop(ctx, logger, nil, nk, disp, 1, state, []runtime.MatchData{msg})

	ackBroadcast := disp.findLastBroadcast(OpCodeBetAck)
	if ackBroadcast == nil {
		t.Fatalf("expected OpCodeBetAck response")
	}
	var ack BetAckResponse
	_ = json.Unmarshal(ackBroadcast.Data, &ack)
	if ack.Success {
		t.Errorf("expected failure for invalid choice")
	}

	// 2. Insufficient Balance
	overBetPayload, _ := json.Marshal(BetRequest{Choice: ChoiceTai, Amount: 100})
	msgOver := &mockMatchData{mockPresence: *p1, opCode: OpCodeBet, data: overBetPayload}
	match.MatchLoop(ctx, logger, nil, nk, disp, 2, state, []runtime.MatchData{msgOver})

	ackBroadcast = disp.findLastBroadcast(OpCodeBetAck)
	if ackBroadcast == nil {
		t.Fatalf("expected OpCodeBetAck response")
	}
	_ = json.Unmarshal(ackBroadcast.Data, &ack)
	if ack.Success {
		t.Errorf("expected failure for insufficient wallet balance")
	}
}
