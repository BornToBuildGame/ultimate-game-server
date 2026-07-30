package taixiu

// OpCodes for WebSocket / MatchData messages
const (
	OpCodeBet         int64 = 1 // Client -> Server: Place a bet
	OpCodeStateUpdate int64 = 2 // Server -> Client: Match state update / timer tick broadcast
	OpCodeBetAck      int64 = 3 // Server -> Client: Bet acknowledgement / status response
	OpCodeRoundResult int64 = 4 // Server -> Client: Round dice result & win/loss payout notification
)

// Bet Choice Constants
const (
	ChoiceTai    = "TAI"    // Sum 11 - 17 (Over)
	ChoiceXiu    = "XIU"    // Sum 4 - 10 (Under)
	ChoiceTriple = "TRIPLE" // 3 of a kind (1-1-1 or 6-6-6)
)

// Phase Constants
const (
	PhaseBetting      = "BETTING"
	PhaseRolling      = "ROLLING"
	PhaseResult       = "RESULT"
	PhaseIntermission = "INTERMISSION"
)

// BetRequest payload sent from client in OpCodeBet
type BetRequest struct {
	Choice string `json:"choice"` // "TAI" or "XIU"
	Amount int64  `json:"amount"` // Bet amount in coins/chips
}

// BetAckResponse payload sent to client in OpCodeBetAck
type BetAckResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Choice  string `json:"choice,omitempty"`
	Amount  int64  `json:"amount,omitempty"`
	Balance int64  `json:"balance,omitempty"`
}

// StateUpdateBroadcast payload sent to all players in OpCodeStateUpdate
type StateUpdateBroadcast struct {
	Phase          string `json:"phase"`
	RoundNumber    int64  `json:"round_number"`
	RemainingTicks int    `json:"remaining_ticks"`
	TotalPoolTai   int64  `json:"total_pool_tai"`
	TotalPoolXiu   int64  `json:"total_pool_xiu"`
	ActivePlayers  int    `json:"active_players"`
}

// PlayerPayout summary included in round result
type PlayerPayout struct {
	UserID     string `json:"user_id"`
	Choice     string `json:"choice"`
	BetAmount  int64  `json:"bet_amount"`
	Payout     int64  `json:"payout"`     // Total payout credited (0 if lost)
	NetProfit  int64  `json:"net_profit"` // Payout - BetAmount
	NewBalance int64  `json:"new_balance"`
}

// RoundResultBroadcast payload sent to all players in OpCodeRoundResult
type RoundResultBroadcast struct {
	RoundNumber int64          `json:"round_number"`
	Dice        [3]int         `json:"dice"`
	Sum         int            `json:"sum"`
	WinningSide string         `json:"winning_side"` // "TAI", "XIU", or "TRIPLE"
	Payouts     []PlayerPayout `json:"payouts"`
}
