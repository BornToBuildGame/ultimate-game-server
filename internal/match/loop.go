package match

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"ultimate-game-server/internal/runtime"

	"github.com/google/uuid"
	lua "github.com/yuin/gopher-lua"
	"go.uber.org/zap"
)

const (
	maxLabelBytes          = 2048
	maxConcurrentMatches   = 500
	maxConsecutiveOverruns = 10
	inputBufferSize        = 128
	signalQueueSize        = 10
	deferredQueueSize      = 128
)

// ErrDeferredBroadcastFull is returned when deferredCh is at capacity.
var ErrDeferredBroadcastFull = errors.New("deferred broadcast queue full")

// SessionRegistry abstracts WebSocket session message delivery to prevent circular dependencies.
type SessionRegistry interface {
	SendToSession(sessionID string, payload []byte)
}

// PresenceImpl implements runtime.Presence.
type PresenceImpl struct {
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`
	Username  string `json:"username"`
	NodeID    string `json:"node_id"`
}

func (p *PresenceImpl) GetUserId() string    { return p.UserID }
func (p *PresenceImpl) GetSessionId() string { return p.SessionID }
func (p *PresenceImpl) GetNodeId() string    { return p.NodeID }
func (p *PresenceImpl) GetUsername() string  { return p.Username }

// MatchDataImpl implements runtime.MatchData.
type MatchDataImpl struct {
	PresenceImpl
	OpCode      int64  `json:"op_code"`
	Data        []byte `json:"data"`
	Reliable    bool   `json:"reliable"`
	ReceiveTime int64  `json:"receive_time"`
}

func (m *MatchDataImpl) GetOpCode() int64      { return m.OpCode }
func (m *MatchDataImpl) GetData() []byte       { return m.Data }
func (m *MatchDataImpl) GetReliable() bool     { return m.Reliable }
func (m *MatchDataImpl) GetReceiveTime() int64 { return m.ReceiveTime }

type matchJoinRequest struct {
	userID    string
	username  string
	metadata  map[string]string
	sessionID string
	respChan  chan joinResponse
}

type joinResponse struct {
	accept bool
	error  error
	state  interface{}
}

type matchLeaveRequest struct {
	userID    string
	sessionID string
}

type matchSignalRequest struct {
	data     string
	respChan chan signalResponse
}

type signalResponse struct {
	state  interface{}
	result string
}

type deferredBroadcast struct {
	opCode         int64
	data           []byte
	presenceFilter []string // session IDs; empty = all
	senderJSON     map[string]interface{}
	reliable       bool
}

// GoMatchDispatcher implements the reference engine's MatchDispatcher methods.
type GoMatchDispatcher struct {
	loop *MatchLoop
}

func (d *GoMatchDispatcher) BroadcastMessage(opCode int64, data []byte, presences []runtime.Presence, sender runtime.Presence, reliable bool) error {
	targets := d.resolveTargets(presences)
	senderJSON := map[string]interface{}{}
	if sender != nil {
		senderJSON = map[string]interface{}{
			"user_id":    sender.GetUserId(),
			"username":   sender.GetUsername(),
			"session_id": sender.GetSessionId(),
		}
	}
	return d.sendBroadcast(opCode, data, targets, senderJSON, reliable)
}

func (d *GoMatchDispatcher) BroadcastMessageDeferred(opCode int64, data []byte, presences []runtime.Presence, sender runtime.Presence, reliable bool) error {
	var filter []string
	if len(presences) > 0 {
		filter = make([]string, len(presences))
		for i, p := range presences {
			filter[i] = p.GetSessionId()
		}
	}
	senderJSON := map[string]interface{}{}
	if sender != nil {
		senderJSON = map[string]interface{}{
			"user_id":    sender.GetUserId(),
			"username":   sender.GetUsername(),
			"session_id": sender.GetSessionId(),
		}
	}
	msg := deferredBroadcast{
		opCode:         opCode,
		data:           append([]byte(nil), data...),
		presenceFilter: filter,
		senderJSON:     senderJSON,
		reliable:       reliable,
	}
	select {
	case d.loop.deferredCh <- msg:
		return nil
	default:
		return ErrDeferredBroadcastFull
	}
}

func (d *GoMatchDispatcher) MatchKick(presences []runtime.Presence) error {
	for _, p := range presences {
		d.loop.KickPlayer(p.GetSessionId())
	}
	return nil
}

func (d *GoMatchDispatcher) MatchLabelUpdate(label string) error {
	if len(label) > maxLabelBytes {
		return fmt.Errorf("match label exceeds %d bytes", maxLabelBytes)
	}
	// Tick already holds the write lock; avoid re-entrant Lock.
	if d.loop.mu.TryLock() {
		defer d.loop.mu.Unlock()
		return d.loop.setLabelLocked(label)
	}
	return d.loop.setLabelLocked(label)
}

func (d *GoMatchDispatcher) resolveTargets(presences []runtime.Presence) []string {
	if len(presences) > 0 {
		targets := make([]string, len(presences))
		for i, p := range presences {
			targets[i] = p.GetSessionId()
		}
		return targets
	}
	// Tick holds the write lock; TryRLock fails in that case — read unlocked safely.
	if d.loop.mu.TryRLock() {
		defer d.loop.mu.RUnlock()
		targets := make([]string, 0, len(d.loop.presences))
		for _, p := range d.loop.presences {
			targets = append(targets, p.SessionID)
		}
		return targets
	}
	targets := make([]string, 0, len(d.loop.presences))
	for _, p := range d.loop.presences {
		targets = append(targets, p.SessionID)
	}
	return targets
}

func (d *GoMatchDispatcher) sendBroadcast(opCode int64, data []byte, targets []string, senderJSON map[string]interface{}, reliable bool) error {
	if senderJSON == nil {
		senderJSON = map[string]interface{}{}
	}
	payload := map[string]interface{}{
		"cid": "",
		"match_data": map[string]interface{}{
			"match_id": d.loop.MatchID,
			"presence": senderJSON,
			"op_code":  opCode,
			"data":     base64.StdEncoding.EncodeToString(data),
			"reliable": reliable,
		},
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if d.loop.registry != nil {
		for _, sessionID := range targets {
			d.loop.registry.SendToSession(sessionID, payloadBytes)
		}
	}
	return nil
}

// MatchInput represents a player action sent during a match.
type MatchInput struct {
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`
	Username  string `json:"username"`
	OpCode    int64  `json:"op_code"`
	Action    string `json:"action"` // legacy demo fallback
	Payload   string `json:"payload"`
	Reliable  bool   `json:"reliable"`
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
	tickRate     int
	state        MatchState
	logger       *zap.Logger

	sandbox             *runtime.Sandbox
	luaMatchInit        *lua.LFunction
	luaMatchJoinAttempt *lua.LFunction
	luaMatchJoin        *lua.LFunction
	luaMatchLeave       *lua.LFunction
	luaMatchLoop        *lua.LFunction
	luaMatchTerminate   *lua.LFunction
	luaMatchSignal      *lua.LFunction

	goMatch  runtime.Match
	goState  interface{}
	goLogger runtime.Logger
	goDB     *sql.DB
	goNK     runtime.RuntimeModule

	registry             SessionRegistry
	presences            map[string]*PresenceImpl // key: sessionID
	joinRequests         chan matchJoinRequest
	leaveRequests        chan matchLeaveRequest
	signalRequests       chan matchSignalRequest
	deferredCh           chan deferredBroadcast
	label                string
	onEnd                func(matchID string, finalState MatchState)
	onMetadataUpdate     func(matchID string, label string, playerCount int)
	maxEmptySec          int
	emptyTicks           int
	consecutiveOverruns  int
	graceSeconds         int
}

// NewAuthoritativeMatchID returns `{uuid}.{node}` for authoritative matches.
func NewAuthoritativeMatchID() string {
	return uuid.New().String() + "." + ResolveNodeID()
}

// NewMatchLoop creates a new MatchLoop.
func NewMatchLoop(
	matchID string,
	playerIDs []string,
	tickRate int, // e.g., 30 for 30Hz
	logger *zap.Logger,
	registry SessionRegistry,
) *MatchLoop {
	if tickRate < 1 {
		tickRate = 1
	} else if tickRate > 60 {
		tickRate = 60
	}
	tickDuration := time.Second / time.Duration(tickRate)

	scoreMap := make(map[string]int)
	posMap := make(map[string]string)
	for _, p := range playerIDs {
		scoreMap[p] = 0
		posMap[p] = "0,0"
	}

	state := MatchState{
		Tick:       0,
		Score:      scoreMap,
		Positions:  posMap,
		IsFinished: false,
	}

	ml := &MatchLoop{
		MatchID:        matchID,
		PlayerIDs:      playerIDs,
		inputBuffer:    make(chan MatchInput, inputBufferSize),
		tickDuration:   tickDuration,
		tickRate:       tickRate,
		state:          state,
		logger:         logger,
		registry:       registry,
		presences:      make(map[string]*PresenceImpl),
		joinRequests:   make(chan matchJoinRequest, 128),
		leaveRequests:  make(chan matchLeaveRequest, 128),
		signalRequests: make(chan matchSignalRequest, signalQueueSize),
		deferredCh:     make(chan deferredBroadcast, deferredQueueSize),
	}
	if v := os.Getenv("UGE_MATCH_MAX_EMPTY_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			ml.maxEmptySec = n
		}
	}
	return ml
}

// SetSandbox injects an extensibility runtime sandbox for custom script execution.
func (ml *MatchLoop) SetSandbox(sb *runtime.Sandbox) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.sandbox = sb
	if sb != nil && sb.L != nil {
		if fn := sb.L.GetGlobal("match_init"); fn.Type() == lua.LTFunction {
			ml.luaMatchInit = fn.(*lua.LFunction)
		}
		if fn := sb.L.GetGlobal("match_join_attempt"); fn.Type() == lua.LTFunction {
			ml.luaMatchJoinAttempt = fn.(*lua.LFunction)
		}
		if fn := sb.L.GetGlobal("match_join"); fn.Type() == lua.LTFunction {
			ml.luaMatchJoin = fn.(*lua.LFunction)
		}
		if fn := sb.L.GetGlobal("match_leave"); fn.Type() == lua.LTFunction {
			ml.luaMatchLeave = fn.(*lua.LFunction)
		}
		if fn := sb.L.GetGlobal("match_loop"); fn.Type() == lua.LTFunction {
			ml.luaMatchLoop = fn.(*lua.LFunction)
		}
		if fn := sb.L.GetGlobal("match_terminate"); fn.Type() == lua.LTFunction {
			ml.luaMatchTerminate = fn.(*lua.LFunction)
		}
		if fn := sb.L.GetGlobal("match_signal"); fn.Type() == lua.LTFunction {
			ml.luaMatchSignal = fn.(*lua.LFunction)
		}
	}
}

// SetGoMatch configures the loop to run a Go native match instead of a Lua one.
func (ml *MatchLoop) SetGoMatch(goMatch runtime.Match, initialGoState interface{}, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.goMatch = goMatch
	ml.goState = initialGoState
	ml.goLogger = logger
	ml.goDB = db
	ml.goNK = nk
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
			start := time.Now()
			finished := ml.tick()
			if elapsed := time.Since(start); elapsed > defaultTickSoftTimeout {
				ml.consecutiveOverruns++
				ml.logger.Warn("match tick exceeded soft timeout",
					zap.String("match_id", ml.MatchID),
					zap.Duration("elapsed", elapsed),
					zap.Duration("limit", defaultTickSoftTimeout),
					zap.Int("consecutive_overruns", ml.consecutiveOverruns))
				if ml.consecutiveOverruns >= maxConsecutiveOverruns {
					ml.mu.Lock()
					ml.state.IsFinished = true
					ml.mu.Unlock()
					finished = true
				}
			} else {
				ml.consecutiveOverruns = 0
			}
			if finished {
				ml.terminate()
				return
			}
		}
	}
}

func (ml *MatchLoop) tick() (finished bool) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			ml.logger.Error("match tick panic recovered",
				zap.String("match_id", ml.MatchID),
				zap.Any("panic", r))
			ml.state.IsFinished = true
			finished = true
		}
		ml.flushDeferred()
	}()

	ml.state.Tick++
	dispatcher := &GoMatchDispatcher{loop: ml}

	hadActivity := false

	// 1. Process join requests (MatchJoinAttempt)
	var joinedPresences []runtime.Presence
	var joinedPresencesImpl []interface{}
	drainJoins := true
	for drainJoins {
		select {
		case req := <-ml.joinRequests:
			hadActivity = true
			// Already-member: accept without calling JoinAttempt handler.
			if _, exists := ml.presences[req.sessionID]; exists {
				req.respChan <- joinResponse{accept: true, state: ml.goState}
				continue
			}

			presence := &PresenceImpl{
				UserID:    req.userID,
				SessionID: req.sessionID,
				Username:  req.username,
				NodeID:    ResolveNodeID(),
			}

			accept := true
			var rejectReason string
			var err error

			if ml.goMatch != nil {
				var res interface{}
				res, accept, rejectReason = ml.goMatch.MatchJoinAttempt(context.Background(), ml.goLogger, ml.goDB, ml.goNK, dispatcher, ml.state.Tick, ml.goState, presence, req.metadata)
				if accept {
					ml.goState = res
				}
			} else if ml.luaMatchJoinAttempt != nil && ml.sandbox != nil {
				var reason string
				accept, reason, err = ml.luaJoinAttemptCall(presence, req.metadata)
				if rejectReason == "" {
					rejectReason = reason
				}
			}

			if accept && err == nil {
				ml.presences[req.sessionID] = presence
				joinedPresences = append(joinedPresences, presence)
				joinedPresencesImpl = append(joinedPresencesImpl, presence)
				req.respChan <- joinResponse{accept: true, state: ml.goState}
			} else {
				if rejectReason == "" && err != nil {
					rejectReason = err.Error()
				}
				req.respChan <- joinResponse{accept: false, error: fmt.Errorf("%s", rejectReason)}
			}
		default:
			drainJoins = false
		}
	}

	// 2. Process leave requests
	var leftPresences []runtime.Presence
	var leftPresencesImpl []interface{}
	drainLeaves := true
	for drainLeaves {
		select {
		case req := <-ml.leaveRequests:
			hadActivity = true
			if p, ok := ml.presences[req.sessionID]; ok {
				delete(ml.presences, req.sessionID)
				leftPresences = append(leftPresences, p)
				leftPresencesImpl = append(leftPresencesImpl, p)
			}
		default:
			drainLeaves = false
		}
	}

	// 3. Process signals
	drainSignals := true
	for drainSignals {
		select {
		case req := <-ml.signalRequests:
			hadActivity = true
			var res interface{}
			var result string
			if ml.goMatch != nil {
				res, result = ml.goMatch.MatchSignal(context.Background(), ml.goLogger, ml.goDB, ml.goNK, dispatcher, ml.state.Tick, ml.goState, req.data)
				ml.goState = res
			} else if ml.luaMatchSignal != nil && ml.sandbox != nil {
				result = ml.luaSignalCall(req.data)
			}
			if req.respChan != nil {
				req.respChan <- signalResponse{state: res, result: result}
			}
		default:
			drainSignals = false
		}
	}

	// 4. Call MatchJoin if any joined
	if len(joinedPresences) > 0 {
		if ml.goMatch != nil {
			ml.goState = ml.goMatch.MatchJoin(context.Background(), ml.goLogger, ml.goDB, ml.goNK, dispatcher, ml.state.Tick, ml.goState, joinedPresences)
		} else if ml.luaMatchJoin != nil && ml.sandbox != nil {
			ml.luaJoinCall(joinedPresencesImpl)
		}
	}

	// 5. Call MatchLeave if any left
	if len(leftPresences) > 0 {
		if ml.goMatch != nil {
			ml.goState = ml.goMatch.MatchLeave(context.Background(), ml.goLogger, ml.goDB, ml.goNK, dispatcher, ml.state.Tick, ml.goState, leftPresences)
		} else if ml.luaMatchLeave != nil && ml.sandbox != nil {
			ml.luaLeaveCall(leftPresencesImpl)
		}
	}

	// 6. Drain inputs
	var inputs []MatchInput
	drainInputs := true
	for drainInputs {
		select {
		case in := <-ml.inputBuffer:
			inputs = append(inputs, in)
			hadActivity = true
		default:
			drainInputs = false
		}
	}

	// 7. Process inputs via MatchLoop tick
	if ml.goMatch != nil {
		msgs := make([]runtime.MatchData, len(inputs))
		for i, in := range inputs {
			sessionID := in.SessionID
			if sessionID == "" {
				sessionID = "session-local"
			}
			username := in.Username
			if username == "" {
				username = "player"
			}
			opCode := in.OpCode
			if opCode == 0 && in.Action != "" {
				if parsed, err := strconv.ParseInt(in.Action, 10, 64); err == nil {
					opCode = parsed
				} else {
					opCode = 1
				}
			}
			msgs[i] = &MatchDataImpl{
				PresenceImpl: PresenceImpl{
					UserID:    in.UserID,
					SessionID: sessionID,
					Username:  username,
					NodeID:    ResolveNodeID(),
				},
				OpCode:      opCode,
				Data:        []byte(in.Payload),
				Reliable:    in.Reliable,
				ReceiveTime: time.Now().UnixMilli(),
			}
		}
		resState := ml.goMatch.MatchLoop(context.Background(), ml.goLogger, ml.goDB, ml.goNK, dispatcher, ml.state.Tick, ml.goState, msgs)
		if resState == nil {
			ml.state.IsFinished = true
		} else {
			ml.goState = resState
			if bytes, err := json.Marshal(resState); err == nil {
				_ = json.Unmarshal(bytes, &ml.state)
			}
		}
	} else if ml.luaMatchLoop != nil && ml.sandbox != nil {
		ml.luaLoopCall(inputs)
	} else {
		// Demo move/score path only when no Go/Lua handler is bound.
		for _, in := range inputs {
			switch in.Action {
			case "move":
				ml.state.Positions[in.UserID] = in.Payload
			case "score":
				ml.state.Score[in.UserID] += 1
				if ml.state.Score[in.UserID] >= 10 {
					ml.state.IsFinished = true
				}
			}
		}
	}

	// 8. Broadcast full state (op 0) only for demo (no Go/Lua handler).
	if ml.registry != nil && ml.goMatch == nil && ml.luaMatchLoop == nil {
		stateBytes, _ := json.Marshal(ml.state)
		payload := map[string]interface{}{
			"cid": "",
			"match_data": map[string]interface{}{
				"match_id": ml.MatchID,
				"presence": map[string]interface{}{
					"user_id":    "server",
					"username":   "server",
					"session_id": "server",
				},
				"op_code": 0,
				"data":    base64.StdEncoding.EncodeToString(stateBytes),
			},
		}
		payloadBytes, _ := json.Marshal(payload)
		for _, p := range ml.presences {
			ml.registry.SendToSession(p.SessionID, payloadBytes)
		}
	}

	if n := estimateStateSize(ml.goState); n > maxMatchStateBytes {
		ml.logger.Warn("match state exceeded quota; ending match",
			zap.String("match_id", ml.MatchID), zap.Int("bytes", n), zap.Int("limit", maxMatchStateBytes))
		ml.state.IsFinished = true
	}

	// Empty-match idle quota
	if ml.maxEmptySec > 0 {
		if len(ml.presences) == 0 && !hadActivity {
			ml.emptyTicks++
			threshold := ml.tickRate * ml.maxEmptySec
			if threshold > 0 && ml.emptyTicks > threshold {
				ml.logger.Info("match ended due to empty idle quota",
					zap.String("match_id", ml.MatchID),
					zap.Int("empty_ticks", ml.emptyTicks))
				ml.state.IsFinished = true
			}
		} else {
			ml.emptyTicks = 0
		}
	}

	return ml.state.IsFinished
}

func (ml *MatchLoop) flushDeferred() {
	n := len(ml.deferredCh)
	for i := 0; i < n; i++ {
		select {
		case msg := <-ml.deferredCh:
			targets := msg.presenceFilter
			if len(targets) == 0 {
				targets = make([]string, 0, len(ml.presences))
				for _, p := range ml.presences {
					targets = append(targets, p.SessionID)
				}
			}
			d := &GoMatchDispatcher{loop: ml}
			_ = d.sendBroadcast(msg.opCode, msg.data, targets, msg.senderJSON, msg.reliable)
		default:
			return
		}
	}
}

func (ml *MatchLoop) terminate() {
	ml.logger.Info("Terminating match loop", zap.String("match_id", ml.MatchID))

	grace := ml.graceSeconds
	if grace > 0 {
		time.Sleep(time.Duration(grace) * time.Second)
	}

	ml.mu.Lock()
	defer ml.mu.Unlock()

	dispatcher := &GoMatchDispatcher{loop: ml}

	if ml.goMatch != nil && ml.goState != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		ml.goState = ml.goMatch.MatchTerminate(ctx, ml.goLogger, ml.goDB, ml.goNK, dispatcher, ml.state.Tick, ml.goState, grace)
	}
	if ml.luaMatchTerminate != nil && ml.sandbox != nil {
		L := ml.sandbox.L
		ctxTbl := ml.luaMatchCtx(L, "", "")
		luaDispatcher := ml.pushLuaDispatcher(L, dispatcher)

		stateBytes, _ := json.Marshal(ml.state)
		var stateRaw interface{}
		_ = json.Unmarshal(stateBytes, &stateRaw)
		luaState := runtime.ToLuaValue(L, stateRaw)

		if err := L.CallByParam(lua.P{
			Fn:      ml.luaMatchTerminate,
			NRet:    1,
			Protect: true,
		}, ctxTbl, luaDispatcher, lua.LNumber(ml.state.Tick), luaState, lua.LNumber(grace)); err != nil {
			ml.logger.Warn("match_terminate lua error", zap.Error(err), zap.String("match_id", ml.MatchID))
		} else {
			retState := L.Get(-1)
			L.Pop(1)
			if tbl, ok := retState.(*lua.LTable); ok {
				goStateVal := runtime.ToGoValue(tbl)
				if bytes, err := json.Marshal(goStateVal); err == nil {
					_ = json.Unmarshal(bytes, &ml.state)
				}
			}
		}
	}

	ml.flushDeferred()

	if n := estimateStateSize(ml.goState); n > maxMatchStateBytes {
		ml.logger.Warn("match state exceeded quota at terminate",
			zap.String("match_id", ml.MatchID), zap.Int("bytes", n), zap.Int("limit", maxMatchStateBytes))
	}

	if ml.onEnd != nil {
		ml.onEnd(ml.MatchID, ml.state)
	}
}

func estimateStateSize(v interface{}) int {
	if v == nil {
		return 0
	}
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(b)
}

// JoinAttempt queues a join request onto the match loop.
func (ml *MatchLoop) JoinAttempt(userID, username, sessionID string, metadata map[string]string) (bool, error) {
	ml.mu.RLock()
	if _, exists := ml.presences[sessionID]; exists {
		ml.mu.RUnlock()
		return true, nil
	}
	ml.mu.RUnlock()

	respChan := make(chan joinResponse, 1)
	req := matchJoinRequest{
		userID:    userID,
		username:  username,
		metadata:  metadata,
		sessionID: sessionID,
		respChan:  respChan,
	}

	select {
	case ml.joinRequests <- req:
	default:
		return false, fmt.Errorf("match join queue full")
	}

	select {
	case res := <-respChan:
		return res.accept, res.error
	case <-time.After(5 * time.Second):
		return false, fmt.Errorf("match join timeout")
	}
}

// SubmitLeave queues a leave request onto the match loop.
func (ml *MatchLoop) SubmitLeave(userID, sessionID string) {
	select {
	case ml.leaveRequests <- matchLeaveRequest{userID: userID, sessionID: sessionID}:
	default:
	}
}

// SubmitSignal queues a signal request onto the match loop.
func (ml *MatchLoop) SubmitSignal(data string) (string, error) {
	respChan := make(chan signalResponse, 1)
	req := matchSignalRequest{
		data:     data,
		respChan: respChan,
	}

	select {
	case ml.signalRequests <- req:
	default:
		return "", fmt.Errorf("match signal queue full")
	}

	select {
	case res := <-respChan:
		return res.result, nil
	case <-time.After(5 * time.Second):
		return "", fmt.Errorf("match signal timeout")
	}
}

// EnqueueSignal queues a signal without waiting for the match-loop response (cluster fan-in).
func (ml *MatchLoop) EnqueueSignal(data string) {
	select {
	case ml.signalRequests <- matchSignalRequest{data: data}:
	default:
		ml.logger.Warn("Match signal queue full, dropping cluster signal", zap.String("match_id", ml.MatchID))
	}
}

func (ml *MatchLoop) KickPlayer(sessionID string) {
	select {
	case ml.leaveRequests <- matchLeaveRequest{sessionID: sessionID}:
	default:
	}
}

func (ml *MatchLoop) UpdateLabel(label string) error {
	if len(label) > maxLabelBytes {
		return fmt.Errorf("match label exceeds %d bytes", maxLabelBytes)
	}
	ml.mu.Lock()
	defer ml.mu.Unlock()
	return ml.setLabelLocked(label)
}

func (ml *MatchLoop) setLabelLocked(label string) error {
	ml.label = label
	if ml.onMetadataUpdate != nil {
		ml.onMetadataUpdate(ml.MatchID, label, len(ml.presences))
	}
	return nil
}

// GetPresences returns all active presences in the match.
func (ml *MatchLoop) GetPresences() []*PresenceImpl {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	res := make([]*PresenceImpl, 0, len(ml.presences))
	for _, p := range ml.presences {
		res = append(res, p)
	}
	return res
}

func (ml *MatchLoop) pushLuaDispatcher(L *lua.LState, dispatcher *GoMatchDispatcher) *lua.LTable {
	tbl := L.NewTable()

	parsePresences := func(arg lua.LValue) []runtime.Presence {
		pt, ok := arg.(*lua.LTable)
		if !ok || pt == nil {
			return nil
		}
		var out []runtime.Presence
		pt.ForEach(func(_, v lua.LValue) {
			row, ok := v.(*lua.LTable)
			if !ok {
				return
			}
			out = append(out, &PresenceImpl{
				UserID:    lua.LVAsString(L.GetField(row, "user_id")),
				SessionID: lua.LVAsString(L.GetField(row, "session_id")),
				Username:  lua.LVAsString(L.GetField(row, "username")),
			})
		})
		return out
	}
	parseSender := func(arg lua.LValue) runtime.Presence {
		row, ok := arg.(*lua.LTable)
		if !ok || row == nil {
			return nil
		}
		return &PresenceImpl{
			UserID:    lua.LVAsString(L.GetField(row, "user_id")),
			SessionID: lua.LVAsString(L.GetField(row, "session_id")),
			Username:  lua.LVAsString(L.GetField(row, "username")),
		}
	}

	L.SetField(tbl, "broadcast_message", L.NewFunction(func(L *lua.LState) int {
		opCode := int64(L.CheckNumber(1))
		data := L.CheckString(2)
		presences := parsePresences(L.Get(3))
		sender := parseSender(L.Get(4))
		reliable := true
		if L.GetTop() >= 5 {
			reliable = L.OptBool(5, true)
		}
		_ = dispatcher.BroadcastMessage(opCode, []byte(data), presences, sender, reliable)
		return 0
	}))

	L.SetField(tbl, "broadcast_message_deferred", L.NewFunction(func(L *lua.LState) int {
		opCode := int64(L.CheckNumber(1))
		data := L.CheckString(2)
		presences := parsePresences(L.Get(3))
		sender := parseSender(L.Get(4))
		reliable := true
		if L.GetTop() >= 5 {
			reliable = L.OptBool(5, true)
		}
		_ = dispatcher.BroadcastMessageDeferred(opCode, []byte(data), presences, sender, reliable)
		return 0
	}))

	L.SetField(tbl, "match_kick", L.NewFunction(func(L *lua.LState) int {
		_ = dispatcher.MatchKick(parsePresences(L.Get(1)))
		return 0
	}))

	L.SetField(tbl, "match_label_update", L.NewFunction(func(L *lua.LState) int {
		label := L.CheckString(1)
		_ = dispatcher.MatchLabelUpdate(label)
		return 0
	}))

	return tbl
}

func (ml *MatchLoop) luaMatchCtx(L *lua.LState, userID, sessionID string) *lua.LTable {
	ctxTbl := L.NewTable()
	L.SetField(ctxTbl, "match_id", lua.LString(ml.MatchID))
	if userID != "" {
		L.SetField(ctxTbl, "user_id", lua.LString(userID))
	}
	if sessionID != "" {
		L.SetField(ctxTbl, "session_id", lua.LString(sessionID))
	}
	return ctxTbl
}

// callLuaMatchInit runs match_init(ctx, params) -> state, tick_rate, label.
func (ml *MatchLoop) callLuaMatchInit(params map[string]interface{}) (int, string, error) {
	if ml.luaMatchInit == nil || ml.sandbox == nil || ml.sandbox.L == nil {
		return 0, "", fmt.Errorf("match_init not defined")
	}
	L := ml.sandbox.L
	ctxTbl := ml.luaMatchCtx(L, "", "")
	paramsVal := runtime.ToLuaValue(L, params)
	if params == nil {
		paramsVal = lua.LNil
	}

	err := L.CallByParam(lua.P{
		Fn: ml.luaMatchInit, NRet: 3, Protect: true,
	}, ctxTbl, paramsVal)
	if err != nil {
		return 0, "", err
	}
	labelVal := L.Get(-1)
	rateVal := L.Get(-2)
	stateVal := L.Get(-3)
	L.Pop(3)

	if stateVal.Type() == lua.LTNil {
		return 0, "", fmt.Errorf("match_init returned nil state")
	}
	rate := int(lua.LVAsNumber(rateVal))
	if rate < 1 || rate > 60 {
		return 0, "", fmt.Errorf("match_init tick rate must be 1-60, got %d", rate)
	}
	label := lua.LVAsString(labelVal)
	if len(label) > maxLabelBytes {
		return 0, "", fmt.Errorf("match_init label exceeds %d bytes", maxLabelBytes)
	}
	if tbl, ok := stateVal.(*lua.LTable); ok {
		goStateVal := runtime.ToGoValue(tbl)
		if bytes, err := json.Marshal(goStateVal); err == nil {
			_ = json.Unmarshal(bytes, &ml.state)
		}
	}
	return rate, label, nil
}

func (ml *MatchLoop) luaJoinAttemptCall(presence *PresenceImpl, metadata map[string]string) (bool, string, error) {
	L := ml.sandbox.L
	ctxTbl := ml.luaMatchCtx(L, presence.UserID, presence.SessionID)
	dispatcher := ml.pushLuaDispatcher(L, &GoMatchDispatcher{loop: ml})

	presenceTbl := L.NewTable()
	L.SetField(presenceTbl, "user_id", lua.LString(presence.UserID))
	L.SetField(presenceTbl, "username", lua.LString(presence.Username))
	L.SetField(presenceTbl, "session_id", lua.LString(presence.SessionID))
	L.SetField(presenceTbl, "node", lua.LString(presence.NodeID))

	metaTbl := L.NewTable()
	for k, v := range metadata {
		L.SetField(metaTbl, k, lua.LString(v))
	}

	stateBytes, _ := json.Marshal(ml.state)
	var stateRaw interface{}
	_ = json.Unmarshal(stateBytes, &stateRaw)
	luaState := runtime.ToLuaValue(L, stateRaw)

	err := L.CallByParam(lua.P{
		Fn:      ml.luaMatchJoinAttempt,
		NRet:    3,
		Protect: true,
	}, ctxTbl, dispatcher, lua.LNumber(ml.state.Tick), luaState, presenceTbl, metaTbl)

	if err != nil {
		return false, "", err
	}

	retReason := L.Get(-1)
	retAccept := L.Get(-2)
	retState := L.Get(-3)
	L.Pop(3)

	if tbl, ok := retState.(*lua.LTable); ok {
		goStateVal := runtime.ToGoValue(tbl)
		if bytes, err := json.Marshal(goStateVal); err == nil {
			_ = json.Unmarshal(bytes, &ml.state)
		}
	}

	reason := ""
	if retReason.Type() != lua.LTNil {
		reason = retReason.String()
	}
	return lua.LVAsBool(retAccept), reason, nil
}

func (ml *MatchLoop) luaJoinCall(presences []interface{}) {
	L := ml.sandbox.L
	ctxTbl := ml.luaMatchCtx(L, "", "")
	dispatcher := ml.pushLuaDispatcher(L, &GoMatchDispatcher{loop: ml})

	presencesTbl := L.NewTable()
	for _, p := range presences {
		pImpl := p.(*PresenceImpl)
		pObj := L.NewTable()
		L.SetField(pObj, "user_id", lua.LString(pImpl.UserID))
		L.SetField(pObj, "username", lua.LString(pImpl.Username))
		L.SetField(pObj, "session_id", lua.LString(pImpl.SessionID))
		L.SetField(pObj, "node", lua.LString(pImpl.NodeID))
		presencesTbl.Append(pObj)
	}

	stateBytes, _ := json.Marshal(ml.state)
	var stateRaw interface{}
	_ = json.Unmarshal(stateBytes, &stateRaw)
	luaState := runtime.ToLuaValue(L, stateRaw)

	err := L.CallByParam(lua.P{
		Fn:      ml.luaMatchJoin,
		NRet:    1,
		Protect: true,
	}, ctxTbl, dispatcher, lua.LNumber(ml.state.Tick), luaState, presencesTbl)

	if err == nil {
		retState := L.Get(-1)
		L.Pop(1)
		if tbl, ok := retState.(*lua.LTable); ok {
			goStateVal := runtime.ToGoValue(tbl)
			if bytes, err := json.Marshal(goStateVal); err == nil {
				_ = json.Unmarshal(bytes, &ml.state)
			}
		}
	}
}

func (ml *MatchLoop) luaLeaveCall(presences []interface{}) {
	L := ml.sandbox.L
	ctxTbl := ml.luaMatchCtx(L, "", "")
	dispatcher := ml.pushLuaDispatcher(L, &GoMatchDispatcher{loop: ml})

	presencesTbl := L.NewTable()
	for _, p := range presences {
		pImpl := p.(*PresenceImpl)
		pObj := L.NewTable()
		L.SetField(pObj, "user_id", lua.LString(pImpl.UserID))
		L.SetField(pObj, "username", lua.LString(pImpl.Username))
		L.SetField(pObj, "session_id", lua.LString(pImpl.SessionID))
		L.SetField(pObj, "node", lua.LString(pImpl.NodeID))
		presencesTbl.Append(pObj)
	}

	stateBytes, _ := json.Marshal(ml.state)
	var stateRaw interface{}
	_ = json.Unmarshal(stateBytes, &stateRaw)
	luaState := runtime.ToLuaValue(L, stateRaw)

	err := L.CallByParam(lua.P{
		Fn:      ml.luaMatchLeave,
		NRet:    1,
		Protect: true,
	}, ctxTbl, dispatcher, lua.LNumber(ml.state.Tick), luaState, presencesTbl)

	if err == nil {
		retState := L.Get(-1)
		L.Pop(1)
		if tbl, ok := retState.(*lua.LTable); ok {
			goStateVal := runtime.ToGoValue(tbl)
			if bytes, err := json.Marshal(goStateVal); err == nil {
				_ = json.Unmarshal(bytes, &ml.state)
			}
		}
	}
}

func (ml *MatchLoop) luaSignalCall(data string) string {
	L := ml.sandbox.L
	ctxTbl := ml.luaMatchCtx(L, "", "")
	dispatcher := ml.pushLuaDispatcher(L, &GoMatchDispatcher{loop: ml})

	stateBytes, _ := json.Marshal(ml.state)
	var stateRaw interface{}
	_ = json.Unmarshal(stateBytes, &stateRaw)
	luaState := runtime.ToLuaValue(L, stateRaw)

	err := L.CallByParam(lua.P{
		Fn:      ml.luaMatchSignal,
		NRet:    2,
		Protect: true,
	}, ctxTbl, dispatcher, lua.LNumber(ml.state.Tick), luaState, lua.LString(data))

	if err != nil {
		ml.logger.Warn("match_signal lua error", zap.Error(err), zap.String("match_id", ml.MatchID))
		return ""
	}

	retState := L.Get(-2)
	retResult := L.Get(-1)
	L.Pop(2)

	if tbl, ok := retState.(*lua.LTable); ok {
		goStateVal := runtime.ToGoValue(tbl)
		if bytes, err := json.Marshal(goStateVal); err == nil {
			_ = json.Unmarshal(bytes, &ml.state)
		}
	}
	return lua.LVAsString(retResult)
}

func (ml *MatchLoop) luaLoopCall(inputs []MatchInput) {
	L := ml.sandbox.L
	dispatcher := ml.pushLuaDispatcher(L, &GoMatchDispatcher{loop: ml})

	msgsTbl := L.NewTable()
	for _, in := range inputs {
		msgObj := L.NewTable()
		L.SetField(msgObj, "user_id", lua.LString(in.UserID))
		L.SetField(msgObj, "session_id", lua.LString(in.SessionID))
		L.SetField(msgObj, "username", lua.LString(in.Username))
		L.SetField(msgObj, "op_code", lua.LNumber(in.OpCode))
		L.SetField(msgObj, "action", lua.LString(in.Action))
		L.SetField(msgObj, "payload", lua.LString(in.Payload))
		L.SetField(msgObj, "data", lua.LString(in.Payload))
		L.SetField(msgObj, "reliable", lua.LBool(in.Reliable))
		msgsTbl.Append(msgObj)
	}

	stateBytes, _ := json.Marshal(ml.state)
	var stateRaw interface{}
	_ = json.Unmarshal(stateBytes, &stateRaw)
	luaState := runtime.ToLuaValue(L, stateRaw)

	ctxTbl := ml.luaMatchCtx(L, "", "")

	err := L.CallByParam(lua.P{
		Fn:      ml.luaMatchLoop,
		NRet:    1,
		Protect: true,
	}, ctxTbl, dispatcher, lua.LNumber(ml.state.Tick), luaState, msgsTbl)

	if err != nil {
		ml.logger.Error("Lua match_loop execution failed", zap.Error(err))
	} else {
		retState := L.Get(-1)
		L.Pop(1)

		if retState == lua.LNil {
			ml.state.IsFinished = true
		} else if tbl, ok := retState.(*lua.LTable); ok {
			goStateVal := runtime.ToGoValue(tbl)
			if bytes, err := json.Marshal(goStateVal); err == nil {
				_ = json.Unmarshal(bytes, &ml.state)
			}
		}
	}
}
