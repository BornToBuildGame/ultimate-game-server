package taixiu

import (
	"context"
	"database/sql"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"ultimate-game-server/internal/runtime"
)

// matchDispatcher describes the dispatcher interface provided by the server runtime.
type matchDispatcher interface {
	BroadcastMessage(opCode int64, data []byte, presences []runtime.Presence, sender runtime.Presence, reliable bool) error
}

// PlayerBet stores an individual player's bet for the current round.
type PlayerBet struct {
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`
	Choice    string `json:"choice"` // TAI or XIU
	Amount    int64  `json:"amount"`
}

// TaiXiuState holds the authoritative state of a Tài-Xỉu match room.
type TaiXiuState struct {
	sync.RWMutex

	RoundNumber       int64                       `json:"round_number"`
	Phase             string                      `json:"phase"`
	PhaseTicks        int                         `json:"phase_ticks"`
	BettingTicks      int                         `json:"betting_ticks"`
	RollingTicks      int                         `json:"rolling_ticks"`
	ResultTicks       int                         `json:"result_ticks"`
	IntermissionTicks int                         `json:"intermission_ticks"`
	Presences         map[string]runtime.Presence `json:"-"`
	Bets              map[string]*PlayerBet       `json:"bets"` // key: userID
	TotalPoolTai      int64                       `json:"total_pool_tai"`
	TotalPoolXiu      int64                       `json:"total_pool_xiu"`

	LastDice    [3]int         `json:"last_dice"`
	LastSum     int            `json:"last_sum"`
	LastWinning string         `json:"last_winning"`
	LastPayouts []PlayerPayout `json:"last_payouts"`

	rng *rand.Rand
}

// TaiXiuMatch implements runtime.Match for the Tài-Xỉu game room.
type TaiXiuMatch struct{}

// NewTaiXiuMatch creates a new instance of the match handler.
func NewTaiXiuMatch() runtime.Match {
	return &TaiXiuMatch{}
}

// MatchInit initializes match state when the room is created.
func (m *TaiXiuMatch) MatchInit(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, params map[string]interface{}) (interface{}, int, string) {
	bettingTicks := 10
	if val, ok := params["betting_ticks"]; ok {
		switch v := val.(type) {
		case int:
			bettingTicks = v
		case int64:
			bettingTicks = int(v)
		case float64:
			bettingTicks = int(v)
		}
	}

	state := &TaiXiuState{
		RoundNumber:       1,
		Phase:             PhaseBetting,
		PhaseTicks:        0,
		BettingTicks:      bettingTicks,
		RollingTicks:      2,
		ResultTicks:       3,
		IntermissionTicks: 2,
		Presences:         make(map[string]runtime.Presence),
		Bets:              make(map[string]*PlayerBet),
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	tickRate := 1 // 1 tick per second
	label := `{"game": "tai_xiu", "mode": "multiplayer"}`
	logger.Info("TaiXiuMatch initialized (Round %d)", state.RoundNumber)
	return state, tickRate, label
}

// MatchJoinAttempt approves or denies player join requests.
func (m *TaiXiuMatch) MatchJoinAttempt(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presence runtime.Presence, metadata map[string]string) (interface{}, bool, string) {
	s := state.(*TaiXiuState)
	s.Lock()
	defer s.Unlock()

	// Accept player join
	return s, true, ""
}

// MatchJoin records newly joined presences.
func (m *TaiXiuMatch) MatchJoin(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []runtime.Presence) interface{} {
	s := state.(*TaiXiuState)
	s.Lock()
	defer s.Unlock()

	for _, p := range presences {
		s.Presences[p.GetSessionId()] = p
		logger.Info("Player %s joined match (Session %s)", p.GetUserId(), p.GetSessionId())
	}
	return s
}

// MatchLeave handles presences departing from the match room.
func (m *TaiXiuMatch) MatchLeave(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []runtime.Presence) interface{} {
	s := state.(*TaiXiuState)
	s.Lock()
	defer s.Unlock()

	for _, p := range presences {
		delete(s.Presences, p.GetSessionId())
		logger.Info("Player %s left match", p.GetUserId())
	}
	return s
}

// MatchLoop is the tick loop driving the game state machine.
func (m *TaiXiuMatch) MatchLoop(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, messages []runtime.MatchData) interface{} {
	s := state.(*TaiXiuState)
	s.Lock()
	defer s.Unlock()

	disp, _ := dispatcher.(matchDispatcher)
	s.PhaseTicks++

	// 1. Process Incoming Client Messages
	for _, msg := range messages {
		switch msg.GetOpCode() {
		case OpCodeBet:
			m.handleBet(ctx, logger, nk, disp, s, msg)
		}
	}

	// 2. Drive State Machine Progression
	switch s.Phase {
	case PhaseBetting:
		remaining := s.BettingTicks - s.PhaseTicks
		if remaining < 0 {
			remaining = 0
		}

		// Broadcast state tick update
		if disp != nil {
			updatePayload, _ := json.Marshal(StateUpdateBroadcast{
				Phase:          PhaseBetting,
				RoundNumber:    s.RoundNumber,
				RemainingTicks: remaining,
				TotalPoolTai:   s.TotalPoolTai,
				TotalPoolXiu:   s.TotalPoolXiu,
				ActivePlayers:  len(s.Presences),
			})
			_ = disp.BroadcastMessage(OpCodeStateUpdate, updatePayload, nil, nil, true)
		}

		if s.PhaseTicks >= s.BettingTicks {
			s.Phase = PhaseRolling
			s.PhaseTicks = 0
			logger.Info("Round %d entering ROLLING phase", s.RoundNumber)
		}

	case PhaseRolling:
		if disp != nil {
			updatePayload, _ := json.Marshal(StateUpdateBroadcast{
				Phase:          PhaseRolling,
				RoundNumber:    s.RoundNumber,
				RemainingTicks: s.RollingTicks - s.PhaseTicks,
				TotalPoolTai:   s.TotalPoolTai,
				TotalPoolXiu:   s.TotalPoolXiu,
				ActivePlayers:  len(s.Presences),
			})
			_ = disp.BroadcastMessage(OpCodeStateUpdate, updatePayload, nil, nil, true)
		}

		if s.PhaseTicks >= s.RollingTicks {
			// Roll 3 dice
			d1 := s.rng.Intn(6) + 1
			d2 := s.rng.Intn(6) + 1
			d3 := s.rng.Intn(6) + 1
			sum := d1 + d2 + d3

			winningSide := ChoiceXiu
			if d1 == d2 && d2 == d3 {
				winningSide = ChoiceTriple
			} else if sum >= 11 && sum <= 17 {
				winningSide = ChoiceTai
			} else if sum >= 4 && sum <= 10 {
				winningSide = ChoiceXiu
			}

			s.LastDice = [3]int{d1, d2, d3}
			s.LastSum = sum
			s.LastWinning = winningSide
			s.LastPayouts = m.calculateAndDistributePayouts(ctx, logger, nk, s, winningSide)

			s.Phase = PhaseResult
			s.PhaseTicks = 0
			logger.Info("Round %d dice rolled: [%d, %d, %d] sum=%d outcome=%s", s.RoundNumber, d1, d2, d3, sum, winningSide)
		}

	case PhaseResult:
		if disp != nil {
			resultPayload, _ := json.Marshal(RoundResultBroadcast{
				RoundNumber: s.RoundNumber,
				Dice:        s.LastDice,
				Sum:         s.LastSum,
				WinningSide: s.LastWinning,
				Payouts:     s.LastPayouts,
			})
			_ = disp.BroadcastMessage(OpCodeRoundResult, resultPayload, nil, nil, true)
		}

		if s.PhaseTicks >= s.ResultTicks {
			s.Phase = PhaseIntermission
			s.PhaseTicks = 0
		}

	case PhaseIntermission:
		if s.PhaseTicks >= s.IntermissionTicks {
			// Reset round state for next round
			s.RoundNumber++
			s.Phase = PhaseBetting
			s.PhaseTicks = 0
			s.Bets = make(map[string]*PlayerBet)
			s.TotalPoolTai = 0
			s.TotalPoolXiu = 0
			s.LastPayouts = nil
			logger.Info("Starting Round %d", s.RoundNumber)
		}
	}

	return s
}

// handleBet processes an incoming bet placement attempt.
func (m *TaiXiuMatch) handleBet(ctx context.Context, logger runtime.Logger, nk runtime.RuntimeModule, disp matchDispatcher, s *TaiXiuState, msg runtime.MatchData) {
	userID := msg.GetUserId()
	senderPresence := msg

	if s.Phase != PhaseBetting {
		m.sendBetAck(disp, senderPresence, false, "Betting phase is closed", "", 0, 0)
		return
	}

	var req BetRequest
	if err := json.Unmarshal(msg.GetData(), &req); err != nil {
		m.sendBetAck(disp, senderPresence, false, "Invalid bet format", "", 0, 0)
		return
	}

	if req.Choice != ChoiceTai && req.Choice != ChoiceXiu {
		m.sendBetAck(disp, senderPresence, false, "Choice must be TAI or XIU", "", 0, 0)
		return
	}

	if req.Amount <= 0 {
		m.sendBetAck(disp, senderPresence, false, "Bet amount must be greater than 0", "", 0, 0)
		return
	}

	if existing, exists := s.Bets[userID]; exists {
		if existing.Choice != req.Choice {
			m.sendBetAck(disp, senderPresence, false, "Cannot bet on both TAI and XIU in the same round", "", 0, 0)
			return
		}
	}

	// Attempt to deduct bet amount from user's wallet via RuntimeModule
	changeset := map[string]int64{"coins": -req.Amount}
	updatedWallet, _, err := nk.WalletUpdate(ctx, userID, changeset, map[string]interface{}{
		"action":       "place_bet",
		"round_number": s.RoundNumber,
		"choice":       req.Choice,
	}, true)

	if err != nil {
		logger.Error("Failed to update wallet for bet (user %s): %v", userID, err)
		m.sendBetAck(disp, senderPresence, false, "Insufficient wallet balance or wallet error", "", 0, 0)
		return
	}

	newBalance := updatedWallet["coins"]

	// Record bet in match state
	if existing, exists := s.Bets[userID]; exists {
		existing.Amount += req.Amount
	} else {
		s.Bets[userID] = &PlayerBet{
			UserID:    userID,
			SessionID: msg.GetSessionId(),
			Choice:    req.Choice,
			Amount:    req.Amount,
		}
	}

	if req.Choice == ChoiceTai {
		s.TotalPoolTai += req.Amount
	} else {
		s.TotalPoolXiu += req.Amount
	}

	totalBet := s.Bets[userID].Amount
	m.sendBetAck(disp, senderPresence, true, "", req.Choice, totalBet, newBalance)
	logger.Info("User %s placed bet of %d on %s (New balance: %d)", userID, req.Amount, req.Choice, newBalance)
}

// sendBetAck responds to the individual betting player.
func (m *TaiXiuMatch) sendBetAck(disp matchDispatcher, sender runtime.Presence, success bool, errMsg string, choice string, amount int64, balance int64) {
	if disp == nil || sender == nil {
		return
	}
	resp := BetAckResponse{
		Success: success,
		Error:   errMsg,
		Choice:  choice,
		Amount:  amount,
		Balance: balance,
	}
	data, _ := json.Marshal(resp)
	_ = disp.BroadcastMessage(OpCodeBetAck, data, []runtime.Presence{sender}, nil, true)
}

// calculateAndDistributePayouts computes winning bets and credits winner wallets.
func (m *TaiXiuMatch) calculateAndDistributePayouts(ctx context.Context, logger runtime.Logger, nk runtime.RuntimeModule, s *TaiXiuState, winningSide string) []PlayerPayout {
	payouts := make([]PlayerPayout, 0, len(s.Bets))

	for userID, bet := range s.Bets {
		payout := int64(0)
		netProfit := -bet.Amount
		newBalance := int64(0)

		if bet.Choice == winningSide {
			// Winning payout 1:1 (return original bet + 1x win)
			payout = bet.Amount * 2
			netProfit = bet.Amount

			updatedWallet, _, err := nk.WalletUpdate(ctx, userID, map[string]int64{"coins": payout}, map[string]interface{}{
				"action":       "win_payout",
				"round_number": s.RoundNumber,
				"winning_side": winningSide,
			}, true)

			if err != nil {
				logger.Error("Failed to credit winning payout for user %s: %v", userID, err)
			} else if updatedWallet != nil {
				newBalance = updatedWallet["coins"]
			}
		} else {
			// Fetch current balance for display if lost (query balance with 0 change)
			updatedWallet, _, err := nk.WalletUpdate(ctx, userID, map[string]int64{"coins": 0}, nil, false)
			if err == nil && updatedWallet != nil {
				newBalance = updatedWallet["coins"]
			}
		}

		payouts = append(payouts, PlayerPayout{
			UserID:     userID,
			Choice:     bet.Choice,
			BetAmount:  bet.Amount,
			Payout:     payout,
			NetProfit:  netProfit,
			NewBalance: newBalance,
		})
	}

	return payouts
}

// MatchTerminate cleans up the room.
func (m *TaiXiuMatch) MatchTerminate(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, graceSeconds int) interface{} {
	return state
}

// MatchSignal handles external control signals.
func (m *TaiXiuMatch) MatchSignal(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, data string) (interface{}, string) {
	return state, "ok"
}
