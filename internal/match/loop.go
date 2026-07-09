package match

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"ultimate-game-server/internal/runtime"

	"go.uber.org/zap"
)

// MatchInput represents a player action sent during a match.
type MatchInput struct {
	UserID  string `json:"user_id"`
	Action  string `json:"action"`
	Payload string `json:"payload"`
}

// MatchState represents the match state inside the server loop.
type MatchState struct {
	Tick       int64             `json:"tick"`
	Score      map[string]int    `json:"score"`
	Positions  map[string]string `json:"positions"`
	IsFinished bool              `json:"is_finished"`
}

// MatchLoop handles the authoritative tick loop of a multiplayer match.
type MatchLoop struct {
	mu           sync.RWMutex
	MatchID      string
	PlayerIDs    []string
	inputBuffer  chan MatchInput
	tickDuration time.Duration
	state        MatchState
	logger       *zap.Logger

	sandbox     *runtime.Sandbox
	onBroadcast func(matchID string, stateJson []byte)
	onEnd       func(matchID string, finalState MatchState)
}

// NewMatchLoop creates a new MatchLoop.
func NewMatchLoop(
	matchID string,
	playerIDs []string,
	tickRate int, // e.g., 30 for 30Hz
	logger *zap.Logger,
	onBroadcast func(matchID string, stateJson []byte),
	onEnd func(matchID string, finalState MatchState),
) *MatchLoop {
	if tickRate <= 0 {
		tickRate = 30
	}
	tickDuration := time.Second / time.Duration(tickRate)

	scoreMap := make(map[string]int)
	posMap := make(map[string]string)
	for _, p := range playerIDs {
		scoreMap[p] = 0
		posMap[p] = "0,0"
	}

	// Initialize basic local state
	state := MatchState{
		Tick:       0,
		Score:      scoreMap,
		Positions:  posMap,
		IsFinished: false,
	}

	return &MatchLoop{
		MatchID:      matchID,
		PlayerIDs:    playerIDs,
		inputBuffer:  make(chan MatchInput, 512),
		tickDuration: tickDuration,
		state:        state,
		logger:       logger,
		onBroadcast:  onBroadcast,
		onEnd:        onEnd,
	}
}

// SetSandbox injects an extensibility runtime sandbox for custom script execution.
func (ml *MatchLoop) SetSandbox(sb *runtime.Sandbox) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.sandbox = sb
}

// SubmitInput queues an input action from a player.
func (ml *MatchLoop) SubmitInput(input MatchInput) {
	select {
	case ml.inputBuffer <- input:
	default:
		ml.logger.Warn("Match input queue full, dropping packet", zap.String("match_id", ml.MatchID), zap.String("user_id", input.UserID))
	}
}

// Start initiates the authoritative tick loop.
func (ml *MatchLoop) Start(ctx context.Context) {
	ml.logger.Info("Spawning authoritative match loop", zap.String("match_id", ml.MatchID))
	ticker := time.NewTicker(ml.tickDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			ml.terminate()
			return
		case <-ticker.C:
			if ml.tick() {
				ml.terminate()
				return
			}
		}
	}
}

func (ml *MatchLoop) tick() bool {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	ml.state.Tick++

	// 1. Drain input buffer
	var inputs []MatchInput
	drain := true
	for drain {
		select {
		case in := <-ml.inputBuffer:
			inputs = append(inputs, in)
		default:
			drain = false
		}
	}

	// 2. Process inputs
	for _, in := range inputs {
		switch in.Action {
		case "move":
			ml.state.Positions[in.UserID] = in.Payload
		case "score":
			ml.state.Score[in.UserID] += 1
			if ml.state.Score[in.UserID] >= 10 { // End match when someone scores 10 points
				ml.state.IsFinished = true
			}
		}
	}

	// 3. Serialize and broadcast state delta
	stateBytes, err := json.Marshal(ml.state)
	if err != nil {
		ml.logger.Error("Failed to marshal match state", zap.Error(err))
	} else if ml.onBroadcast != nil {
		ml.onBroadcast(ml.MatchID, stateBytes)
	}

	return ml.state.IsFinished
}

func (ml *MatchLoop) terminate() {
	ml.logger.Info("Terminating match loop", zap.String("match_id", ml.MatchID))
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	if ml.onEnd != nil {
		ml.onEnd(ml.MatchID, ml.state)
	}
}
