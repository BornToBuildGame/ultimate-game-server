package match

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"ultimate-game-server/internal/runtime"

	lua "github.com/yuin/gopher-lua"
	"go.uber.org/zap"
)

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

// GoMatchDispatcher implements the reference engine's MatchDispatcher methods.
type GoMatchDispatcher struct {
	loop *MatchLoop
}

func (d *GoMatchDispatcher) BroadcastMessage(opCode int64, data []byte, presences []runtime.Presence, sender runtime.Presence, reliable bool) error {
	d.loop.mu.RLock()
	var targets []string
	if len(presences) == 0 {
		for _, p := range d.loop.presences {
			targets = append(targets, p.SessionID)
		}
	} else {
		for _, p := range presences {
			targets = append(targets, p.GetSessionId())
		}
	}
	d.loop.mu.RUnlock()

	senderJSON := map[string]interface{}{}
	if sender != nil {
		senderJSON = map[string]interface{}{
			"user_id":    sender.GetUserId(),
			"username":   sender.GetUsername(),
			"session_id": sender.GetSessionId(),
		}
	}

	payload := map[string]interface{}{
		"cid": "",
		"match_data": map[string]interface{}{
			"match_id": d.loop.MatchID,
			"presence": senderJSON,
			"op_code":  opCode,
			"data":     base64.StdEncoding.EncodeToString(data),
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

func (d *GoMatchDispatcher) BroadcastMessageDeferred(opCode int64, data []byte, presences []runtime.Presence, sender runtime.Presence, reliable bool) error {
	return d.BroadcastMessage(opCode, data, presences, sender, reliable)
}

func (d *GoMatchDispatcher) MatchKick(presences []runtime.Presence) error {
	for _, p := range presences {
		d.loop.KickPlayer(p.GetSessionId())
	}
	return nil
}

func (d *GoMatchDispatcher) MatchLabelUpdate(label string) error {
	d.loop.UpdateLabel(label)
	return nil
}

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

	sandbox             *runtime.Sandbox
	luaMatchInit        *lua.LFunction
	luaMatchJoinAttempt *lua.LFunction
	luaMatchJoin        *lua.LFunction
	luaMatchLeave       *lua.LFunction
	luaMatchLoop        *lua.LFunction
	luaMatchTerminate   *lua.LFunction

	goMatch             runtime.Match
	goState             interface{}
	goLogger            runtime.Logger
	goDB                *sql.DB
	goNK                runtime.RuntimeModule

	// Gaps additions:
	registry         SessionRegistry
	presences        map[string]*PresenceImpl // key: sessionID
	joinRequests     chan matchJoinRequest
	leaveRequests    chan matchLeaveRequest
	signalRequests   chan matchSignalRequest
	label            string
	onEnd            func(matchID string, finalState MatchState)
	onMetadataUpdate func(matchID string, label string, playerCount int)
}

// NewMatchLoop creates a new MatchLoop.
func NewMatchLoop(
	matchID string,
	playerIDs []string,
	tickRate int, // e.g., 30 for 30Hz
	logger *zap.Logger,
	registry SessionRegistry,
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
		MatchID:        matchID,
		PlayerIDs:      playerIDs,
		inputBuffer:    make(chan MatchInput, 512),
		tickDuration:   tickDuration,
		state:          state,
		logger:         logger,
		registry:       registry,
		presences:      make(map[string]*PresenceImpl),
		joinRequests:   make(chan matchJoinRequest, 128),
		leaveRequests:  make(chan matchLeaveRequest, 128),
		signalRequests: make(chan matchSignalRequest, 32),
	}
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

	// 1. Process join requests (MatchJoinAttempt)
	var joinedPresences []runtime.Presence
	var joinedPresencesImpl []interface{}
	drainJoins := true
	for drainJoins {
		select {
		case req := <-ml.joinRequests:
			presence := &PresenceImpl{
				UserID:    req.userID,
				SessionID: req.sessionID,
				Username:  req.username,
				NodeID:    "node-local",
			}

			accept := true
			var rejectReason string
			var err error

			if ml.goMatch != nil {
				dispatcher := &GoMatchDispatcher{loop: ml}
				var res interface{}
				res, accept, rejectReason = ml.goMatch.MatchJoinAttempt(context.Background(), ml.goLogger, ml.goDB, ml.goNK, dispatcher, ml.state.Tick, ml.goState, presence, req.metadata)
				if accept {
					ml.goState = res
				}
			} else if ml.luaMatchJoinAttempt != nil && ml.sandbox != nil {
				accept, err = ml.luaJoinAttemptCall(presence, req.metadata)
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
			var res interface{}
			var result string
			if ml.goMatch != nil {
				dispatcher := &GoMatchDispatcher{loop: ml}
				res, result = ml.goMatch.MatchSignal(context.Background(), ml.goLogger, ml.goDB, ml.goNK, dispatcher, ml.state.Tick, ml.goState, req.data)
				ml.goState = res
			}
			req.respChan <- signalResponse{state: res, result: result}
		default:
			drainSignals = false
		}
	}

	// 4. Call MatchJoin if any joined
	if len(joinedPresences) > 0 {
		if ml.goMatch != nil {
			dispatcher := &GoMatchDispatcher{loop: ml}
			ml.goState = ml.goMatch.MatchJoin(context.Background(), ml.goLogger, ml.goDB, ml.goNK, dispatcher, ml.state.Tick, ml.goState, joinedPresences)
		} else if ml.luaMatchJoin != nil && ml.sandbox != nil {
			ml.luaJoinCall(joinedPresencesImpl)
		}
	}

	// 5. Call MatchLeave if any left
	if len(leftPresences) > 0 {
		if ml.goMatch != nil {
			dispatcher := &GoMatchDispatcher{loop: ml}
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
		default:
			drainInputs = false
		}
	}

	// 7. Process inputs via MatchLoop tick
	if ml.goMatch != nil {
		msgs := make([]runtime.MatchData, len(inputs))
		for i, in := range inputs {
			msgs[i] = &MatchDataImpl{
				PresenceImpl: PresenceImpl{
					UserID:    in.UserID,
					SessionID: "session-local",
					Username:  "player",
				},
				OpCode:      1,
				Data:        []byte(in.Payload),
				Reliable:    true,
				ReceiveTime: time.Now().UnixMilli(),
			}
		}
		dispatcher := &GoMatchDispatcher{loop: ml}
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

	// 8. Broadcast state updates back to participants
	if ml.registry != nil {
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
				"op_code": 0, // Server state tick
				"data":     base64.StdEncoding.EncodeToString(stateBytes),
			},
		}
		payloadBytes, _ := json.Marshal(payload)
		for _, p := range ml.presences {
			ml.registry.SendToSession(p.SessionID, payloadBytes)
		}
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

// JoinAttempt queues a join request onto the match loop.
func (ml *MatchLoop) JoinAttempt(userID, username, sessionID string, metadata map[string]string) (bool, error) {
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

func (ml *MatchLoop) KickPlayer(sessionID string) {
	select {
	case ml.leaveRequests <- matchLeaveRequest{sessionID: sessionID}:
	default:
	}
}

func (ml *MatchLoop) UpdateLabel(label string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.label = label
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


func (ml *MatchLoop) luaJoinAttemptCall(presence *PresenceImpl, metadata map[string]string) (bool, error) {
	L := ml.sandbox.L
	ctxTbl := L.NewTable()
	L.SetField(ctxTbl, "match_id", lua.LString(ml.MatchID))

	presenceTbl := L.NewTable()
	L.SetField(presenceTbl, "user_id", lua.LString(presence.UserID))
	L.SetField(presenceTbl, "username", lua.LString(presence.Username))
	L.SetField(presenceTbl, "session_id", lua.LString(presence.SessionID))

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
		NRet:    2,
		Protect: true,
	}, ctxTbl, lua.LNil, lua.LNumber(ml.state.Tick), luaState, presenceTbl, metaTbl)

	if err != nil {
		return false, err
	}

	retState := L.Get(-2)
	retAccept := L.Get(-1)
	L.Pop(2)

	if tbl, ok := retState.(*lua.LTable); ok {
		goStateVal := runtime.ToGoValue(tbl)
		if bytes, err := json.Marshal(goStateVal); err == nil {
			_ = json.Unmarshal(bytes, &ml.state)
		}
	}

	return lua.LVAsBool(retAccept), nil
}

func (ml *MatchLoop) luaJoinCall(presences []interface{}) {
	L := ml.sandbox.L
	ctxTbl := L.NewTable()
	L.SetField(ctxTbl, "match_id", lua.LString(ml.MatchID))

	presencesTbl := L.NewTable()
	for _, p := range presences {
		pImpl := p.(*PresenceImpl)
		pObj := L.NewTable()
		L.SetField(pObj, "user_id", lua.LString(pImpl.UserID))
		L.SetField(pObj, "username", lua.LString(pImpl.Username))
		L.SetField(pObj, "session_id", lua.LString(pImpl.SessionID))
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
	}, ctxTbl, lua.LNil, lua.LNumber(ml.state.Tick), luaState, presencesTbl)

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
	ctxTbl := L.NewTable()
	L.SetField(ctxTbl, "match_id", lua.LString(ml.MatchID))

	presencesTbl := L.NewTable()
	for _, p := range presences {
		pImpl := p.(*PresenceImpl)
		pObj := L.NewTable()
		L.SetField(pObj, "user_id", lua.LString(pImpl.UserID))
		L.SetField(pObj, "username", lua.LString(pImpl.Username))
		L.SetField(pObj, "session_id", lua.LString(pImpl.SessionID))
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
	}, ctxTbl, lua.LNil, lua.LNumber(ml.state.Tick), luaState, presencesTbl)

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

func (ml *MatchLoop) luaLoopCall(inputs []MatchInput) {
	L := ml.sandbox.L
	msgsTbl := L.NewTable()
	for _, in := range inputs {
		msgObj := L.NewTable()
		L.SetField(msgObj, "user_id", lua.LString(in.UserID))
		L.SetField(msgObj, "action", lua.LString(in.Action))
		L.SetField(msgObj, "payload", lua.LString(in.Payload))
		msgsTbl.Append(msgObj)
	}

	stateBytes, _ := json.Marshal(ml.state)
	var stateRaw interface{}
	_ = json.Unmarshal(stateBytes, &stateRaw)
	luaState := runtime.ToLuaValue(L, stateRaw)

	ctxTbl := L.NewTable()
	L.SetField(ctxTbl, "match_id", lua.LString(ml.MatchID))

	err := L.CallByParam(lua.P{
		Fn:      ml.luaMatchLoop,
		NRet:    1,
		Protect: true,
	}, ctxTbl, lua.LNil, lua.LNumber(ml.state.Tick), luaState, msgsTbl)

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

