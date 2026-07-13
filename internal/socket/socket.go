package socket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/match"
	"ultimate-game-server/internal/matchmaker"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for CORS matching Gateway rule
	},
}

// Session wraps a live WebSocket client connection.
type Session struct {
	mu        sync.RWMutex
	ID        string
	UserID    string
	Username  string
	Conn      *websocket.Conn
	Send      chan []byte
	CloseOnce sync.Once
	IsActive  bool
	matchIDs  map[string]bool // key: matchID
}

// ConnectionRegistry maintains active and recovering user WebSocket sessions.
type ConnectionRegistry struct {
	mu           sync.RWMutex
	sessions     map[string]*Session          // key: sessionID -> Session
	userSessions map[string]map[string]*Session // key: userID -> set of sessionIDs -> Session
	graceTimers  map[string]*time.Timer       // key: sessionID -> Recovery Timer
	GracePeriod  time.Duration                // Reconnection grace period
}

// NewConnectionRegistry creates a new ConnectionRegistry.
func NewConnectionRegistry() *ConnectionRegistry {
	return &ConnectionRegistry{
		sessions:     make(map[string]*Session),
		userSessions: make(map[string]map[string]*Session),
		graceTimers:  make(map[string]*time.Timer),
		GracePeriod:  30 * time.Second, // Default grace period
	}
}

// Add registers a new session, cancelling any active grace timers for reconnection.
func (cr *ConnectionRegistry) Add(s *Session) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	// Cancel grace timer if it exists (reconnection)
	if timer, exists := cr.graceTimers[s.ID]; exists {
		timer.Stop()
		delete(cr.graceTimers, s.ID)
	}

	cr.sessions[s.ID] = s

	userMap, exists := cr.userSessions[s.UserID]
	if !exists {
		userMap = make(map[string]*Session)
		cr.userSessions[s.UserID] = userMap
	}
	userMap[s.ID] = s
}

// StartGracePeriod kicks off the connection recovery window.
func (cr *ConnectionRegistry) StartGracePeriod(sessionID string, cleanupFn func()) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	s, exists := cr.sessions[sessionID]
	if !exists {
		return
	}

	s.IsActive = false

	// Cancel old timer if exists
	if oldTimer, ok := cr.graceTimers[sessionID]; ok {
		oldTimer.Stop()
	}

	gracePeriod := cr.GracePeriod
	if gracePeriod <= 0 {
		gracePeriod = 30 * time.Second
	}

	// Start recovery timer
	timer := time.AfterFunc(gracePeriod, func() {
		cr.mu.Lock()
		defer cr.mu.Unlock()

		// Verify session is still inactive (no reconnect occurred)
		if sess, ok := cr.sessions[sessionID]; ok && !sess.IsActive {
			delete(cr.sessions, sessionID)
			if userMap, ok := cr.userSessions[sess.UserID]; ok {
				delete(userMap, sessionID)
				if len(userMap) == 0 {
					delete(cr.userSessions, sess.UserID)
				}
			}
			delete(cr.graceTimers, sessionID)
			cleanupFn()
		}
	})

	cr.graceTimers[sessionID] = timer
}

// GetBySession retrieves a session by ID.
func (cr *ConnectionRegistry) GetBySession(sessionID string) (*Session, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	s, ok := cr.sessions[sessionID]
	return s, ok
}

// GetUserSessionIDs retrieves active session IDs for a user.
func (cr *ConnectionRegistry) GetUserSessionIDs(userID string) []string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	userMap, exists := cr.userSessions[userID]
	if !exists {
		return nil
	}
	var sessionIDs []string
	for _, sess := range userMap {
		if sess.IsActive {
			sessionIDs = append(sessionIDs, sess.ID)
		}
	}
	return sessionIDs
}

// SendToSession sends a message to a specific active session.
func (cr *ConnectionRegistry) SendToSession(sessionID string, payload []byte) {
	cr.mu.RLock()
	s, exists := cr.sessions[sessionID]
	cr.mu.RUnlock()
	if exists && s.IsActive {
		select {
		case s.Send <- payload:
		default:
			// dropped packet
		}
	}
}

// SendToUser sends a message to all active sessions of a user.
func (cr *ConnectionRegistry) SendToUser(userID string, payload []byte) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	userMap, exists := cr.userSessions[userID]
	if !exists {
		return
	}
	for _, sess := range userMap {
		if sess.IsActive {
			select {
			case sess.Send <- payload:
			default:
				// Channel full, skip
			}
		}
	}
}

// RelayedMatch represents a client-relayed room session.
type RelayedMatch struct {
	mu       sync.RWMutex
	MatchID  string
	Sessions map[string]*Session
}

// GatewayHandler upgrades connections and starts connection lifecycle loops.
type GatewayHandler struct {
	mu             sync.RWMutex
	logger         *zap.Logger
	tokenMgr       *auth.TokenManager
	registry       *ConnectionRegistry
	onConnect      func(s *Session)
	onDisconnect   func(sessionID string)
	Router         *match.Router
	Matchmaker     *matchmaker.Matchmaker
	relayedMatches map[string]*RelayedMatch
	rdb            *redis.Client
}

// SetMatchmaker configures the Matchmaker instance for ticket routing.
func (gh *GatewayHandler) SetMatchmaker(mm *matchmaker.Matchmaker) {
	gh.Matchmaker = mm
}

// SetRedisClient configures the Redis client instance.
func (gh *GatewayHandler) SetRedisClient(rdb *redis.Client) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	gh.rdb = rdb
}

// NewGatewayHandler creates a new GatewayHandler.
func NewGatewayHandler(
	logger *zap.Logger,
	tm *auth.TokenManager,
	reg *ConnectionRegistry,
	onConnect func(s *Session),
	onDisconnect func(sessionID string),
	router *match.Router,
) *GatewayHandler {
	return &GatewayHandler{
		logger:         logger,
		tokenMgr:       tm,
		registry:       reg,
		onConnect:      onConnect,
		onDisconnect:   onDisconnect,
		Router:         router,
		relayedMatches: make(map[string]*RelayedMatch),
	}
}

// CleanupRelayedMatch deletes the relayed match state.
func (gh *GatewayHandler) CleanupRelayedMatch(matchID string) {
	gh.mu.Lock()
	delete(gh.relayedMatches, matchID)
	gh.mu.Unlock()

	gh.mu.RLock()
	rdb := gh.rdb
	gh.mu.RUnlock()

	if rdb != nil {
		ctx := context.Background()
		pipe := rdb.Pipeline()
		pipe.Del(ctx, "match:node:"+matchID)
		pipe.SRem(ctx, "match:ids", matchID)
		pipe.Del(ctx, "match:metadata:"+matchID)
		_, _ = pipe.Exec(ctx)
	}
}

// Upgrade upgrades HTTP requests to WebSocket connection stream.
func (gh *GatewayHandler) Upgrade(w http.ResponseWriter, r *http.Request) {
	// 1. Authenticate via token in query params (or Header)
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		http.Error(w, "missing token", http.StatusUnauthorized)
		return
	}

	claims, err := gh.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)
		return
	}

	// 2. Perform WebSocket Upgrade
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		gh.logger.Error("Failed to upgrade websocket connection", zap.Error(err))
		return
	}

	// Enforce 4KB max payload read limits
	conn.SetReadLimit(4096)

	sessionID := r.URL.Query().Get("session_id")
	isReconnect := sessionID != ""

	// If no session_id supplied, generate new one
	if !isReconnect {
		sessionID = uuid.New().String()
	}

	session := &Session{
		ID:        sessionID,
		UserID:    claims.UserID,
		Username:  claims.Username,
		Conn:      conn,
		Send:      make(chan []byte, 256),
		IsActive:  true,
		matchIDs:  make(map[string]bool),
	}

	gh.registry.Add(session)

	if gh.onConnect != nil {
		gh.onConnect(session)
	}

	// Spawn reader and writer loops
	go gh.writePump(session)
	go gh.readPump(session)
}

func (gh *GatewayHandler) readPump(s *Session) {
	defer func() {
		gh.handleDisconnect(s)
	}()

	s.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	s.Conn.SetPongHandler(func(string) error {
		s.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		return nil
	})

	for {
		msgType, payload, err := s.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				gh.logger.Warn("WebSocket closed unexpectedly", zap.String("session_id", s.ID), zap.Error(err))
			}
			break
		}
		// Reset deadline on successful message read
		s.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		if msgType == websocket.TextMessage {
			gh.RouteMessage(s, payload)
		}
	}
}

func (gh *GatewayHandler) writePump(s *Session) {
	ticker := time.NewTicker(15 * time.Second)
	defer func() {
		ticker.Stop()
		s.Conn.Close()
	}()

	for {
		select {
		case msg, ok := <-s.Send:
			s.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if !ok {
				s.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := s.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			s.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := s.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (gh *GatewayHandler) handleDisconnect(s *Session) {
	s.CloseOnce.Do(func() {
		close(s.Send)
		s.Conn.Close()
	})

	// Start 30-second connection recovery grace period
	gh.registry.StartGracePeriod(s.ID, func() {
		if gh.onDisconnect != nil {
			gh.onDisconnect(s.ID)
		}

		s.mu.RLock()
		joinedMatches := make([]string, 0, len(s.matchIDs))
		for mID := range s.matchIDs {
			joinedMatches = append(joinedMatches, mID)
		}
		s.mu.RUnlock()

		for _, matchID := range joinedMatches {
			authoritative := false
			if gh.Router != nil {
				if loop, ok := gh.Router.GetMatchLoop(matchID); ok {
					authoritative = true
					loop.SubmitLeave(s.UserID, s.ID)
				}
			}

			if !authoritative {
				gh.mu.Lock()
				if rm, ok := gh.relayedMatches[matchID]; ok {
					rm.mu.Lock()
					delete(rm.Sessions, s.ID)
					sessionCount := len(rm.Sessions)
					rm.mu.Unlock()

					rdb := gh.rdb

					if sessionCount == 0 {
						gh.mu.Unlock()
						gh.CleanupRelayedMatch(matchID)
					} else {
						if rdb != nil {
							rdb.HSet(context.Background(), "match:metadata:"+matchID, "size", sessionCount)
						}

						// Broadcast leave presence notification
						notification := map[string]interface{}{
							"match_presence_event": map[string]interface{}{
								"match_id": matchID,
								"joins":    []interface{}{},
								"leaves": []map[string]interface{}{
									{
										"user_id":    s.UserID,
										"username":   s.Username,
										"session_id": s.ID,
									},
								},
							},
						}
						notificationBytes, _ := json.Marshal(notification)

						rm.mu.RLock()
						for _, sess := range rm.Sessions {
							select {
							case sess.Send <- notificationBytes:
							default:
							}
						}
						rm.mu.RUnlock()
						gh.mu.Unlock()
					}
				} else {
					gh.mu.Unlock()
				}
			}
		}
	})
}

// Envelope defines client-to-server and server-to-client WS structures.
type Envelope struct {
	Cid              string                   `json:"cid,omitempty"`
	MatchCreate      *MatchCreatePayload      `json:"match_create,omitempty"`
	MatchJoin        *MatchJoinPayload        `json:"match_join,omitempty"`
	MatchLeave       *MatchLeavePayload       `json:"match_leave,omitempty"`
	MatchDataSend    *MatchDataSendPayload    `json:"match_data_send,omitempty"`
	MatchmakerAdd    *MatchmakerAddPayload    `json:"matchmaker_add,omitempty"`
	MatchmakerRemove *MatchmakerRemovePayload `json:"matchmaker_remove,omitempty"`
}

type MatchCreatePayload struct{}

type MatchJoinPayload struct {
	MatchID  string            `json:"match_id"`
	Token    string            `json:"token,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type MatchLeavePayload struct {
	MatchID string `json:"match_id"`
}

type MatchDataSendPayload struct {
	MatchID  string `json:"match_id"`
	OpCode   int64  `json:"op_code"`
	Data     string `json:"data"` // Base64 or raw action payload
	Reliable bool   `json:"reliable"`
}

type MatchmakerAddPayload struct {
	QueueName         string             `json:"queue_name"`
	MinCount          int                `json:"min_count"`
	MaxCount          int                `json:"max_count"`
	StringProperties  map[string]string  `json:"string_properties"`
	NumericProperties map[string]float64 `json:"numeric_properties"`
	CountMultiple     int                `json:"count_multiple"`
	ReversePrecision  bool               `json:"reverse_precision"`
	Query             string             `json:"query"`
}

type MatchmakerRemovePayload struct {
	TicketID string `json:"ticket_id"`
}

func (gh *GatewayHandler) RouteMessage(s *Session, payload []byte) {
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		gh.logger.Warn("Failed to unmarshal websocket envelope", zap.Error(err))
		return
	}

	if env.MatchCreate != nil {
		matchID := uuid.New().String()
		gh.mu.Lock()
		gh.relayedMatches[matchID] = &RelayedMatch{
			MatchID:  matchID,
			Sessions: make(map[string]*Session),
		}
		gh.mu.Unlock()

		gh.mu.RLock()
		rdb := gh.rdb
		gh.mu.RUnlock()

		if rdb != nil {
			ctx := context.Background()
			pipe := rdb.Pipeline()
			pipe.Set(ctx, "match:node:"+matchID, "node-local", 0)
			pipe.SAdd(ctx, "match:ids", matchID)
			metadata := map[string]interface{}{
				"match_id":      matchID,
				"authoritative": "false",
				"label":         "{}",
				"size":          0,
				"max_size":      100,
				"node":          "node-local",
			}
			pipe.HMSet(ctx, "match:metadata:"+matchID, metadata)
			_, _ = pipe.Exec(ctx)
		}

		res := map[string]interface{}{
			"cid": env.Cid,
			"match_create": map[string]interface{}{
				"match_id": matchID,
			},
		}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes

	} else if env.MatchJoin != nil {
		matchID := env.MatchJoin.MatchID
		var authoritative bool = false

		if gh.Router != nil {
			loop, ok := gh.Router.GetMatchLoop(matchID)
			if ok {
				authoritative = true
				accept, err := loop.JoinAttempt(s.UserID, s.Username, s.ID, env.MatchJoin.Metadata)
				if err != nil || !accept {
					res := map[string]interface{}{
						"cid":   env.Cid,
						"error": "Match join rejected",
					}
					resBytes, _ := json.Marshal(res)
					s.Send <- resBytes
					return
				}
				s.mu.Lock()
				s.matchIDs[matchID] = true
				s.mu.Unlock()
			}
		}

		if !authoritative {
			gh.mu.Lock()
			rm, ok := gh.relayedMatches[matchID]
			if !ok {
				rm = &RelayedMatch{
					MatchID:  matchID,
					Sessions: make(map[string]*Session),
				}
				gh.relayedMatches[matchID] = rm
			}
			rm.mu.Lock()
			rm.Sessions[s.ID] = s
			sessionCount := len(rm.Sessions)
			rm.mu.Unlock()

			rdb := gh.rdb

			gh.mu.Unlock()

			s.mu.Lock()
			s.matchIDs[matchID] = true
			s.mu.Unlock()

			if rdb != nil {
				rdb.HSet(context.Background(), "match:metadata:"+matchID, "size", sessionCount)
			}
		}

		var presences []map[string]interface{}
		if authoritative {
			if loop, ok := gh.Router.GetMatchLoop(matchID); ok {
				for _, p := range loop.GetPresences() {
					presences = append(presences, map[string]interface{}{
						"user_id":    p.UserID,
						"username":   p.Username,
						"session_id": p.SessionID,
					})
				}
			}
		} else {
			gh.mu.Lock()
			if rm, ok := gh.relayedMatches[matchID]; ok {
				rm.mu.RLock()
				for _, sess := range rm.Sessions {
					presences = append(presences, map[string]interface{}{
						"user_id":    sess.UserID,
						"username":   sess.Username,
						"session_id": sess.ID,
					})
				}
				rm.mu.RUnlock()
			}
			gh.mu.Unlock()
		}

		res := map[string]interface{}{
			"cid": env.Cid,
			"match_join": map[string]interface{}{
				"match_id":  matchID,
				"presences": presences,
			},
		}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes

		if !authoritative {
			notification := map[string]interface{}{
				"match_presence_event": map[string]interface{}{
					"match_id": matchID,
					"joins": []map[string]interface{}{
						{
							"user_id":    s.UserID,
							"username":   s.Username,
							"session_id": s.ID,
						},
					},
					"leaves": []interface{}{},
				},
			}
			notificationBytes, _ := json.Marshal(notification)
			gh.mu.Lock()
			if rm, ok := gh.relayedMatches[matchID]; ok {
				rm.mu.RLock()
				for _, sess := range rm.Sessions {
					if sess.ID != s.ID {
						select {
						case sess.Send <- notificationBytes:
						default:
						}
					}
				}
				rm.mu.RUnlock()
			}
			gh.mu.Unlock()
		}

	} else if env.MatchLeave != nil {
		matchID := env.MatchLeave.MatchID
		authoritative := false

		if gh.Router != nil {
			if loop, ok := gh.Router.GetMatchLoop(matchID); ok {
				authoritative = true
				loop.SubmitLeave(s.UserID, s.ID)
			}
		}

		s.mu.Lock()
		delete(s.matchIDs, matchID)
		s.mu.Unlock()

		if !authoritative {
			gh.mu.Lock()
			if rm, ok := gh.relayedMatches[matchID]; ok {
				rm.mu.Lock()
				delete(rm.Sessions, s.ID)
				sessionCount := len(rm.Sessions)
				rm.mu.Unlock()

				rdb := gh.rdb

				if sessionCount == 0 {
					gh.mu.Unlock()
					gh.CleanupRelayedMatch(matchID)
				} else {
					if rdb != nil {
						rdb.HSet(context.Background(), "match:metadata:"+matchID, "size", sessionCount)
					}

					notification := map[string]interface{}{
						"match_presence_event": map[string]interface{}{
							"match_id": matchID,
							"joins":    []interface{}{},
							"leaves": []map[string]interface{}{
								{
									"user_id":    s.UserID,
									"username":   s.Username,
									"session_id": s.ID,
								},
							},
						},
					}
					notificationBytes, _ := json.Marshal(notification)
					
					rm.mu.RLock()
					for _, sess := range rm.Sessions {
						select {
						case sess.Send <- notificationBytes:
						default:
						}
					}
					rm.mu.RUnlock()
					gh.mu.Unlock()
				}
			} else {
				gh.mu.Unlock()
			}
		}

		res := map[string]interface{}{
			"cid": env.Cid,
			"match_leave": map[string]interface{}{
				"match_id": matchID,
			},
		}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes

	} else if env.MatchDataSend != nil {
		matchID := env.MatchDataSend.MatchID
		authoritative := false

		if gh.Router != nil {
			if _, ok := gh.Router.GetMatchLoop(matchID); ok {
				authoritative = true
				input := match.MatchInput{
					UserID:  s.UserID,
					Action:  fmt.Sprintf("%d", env.MatchDataSend.OpCode),
					Payload: env.MatchDataSend.Data,
				}
				_ = gh.Router.ForwardInput(context.Background(), matchID, input)
			}
		}

		if !authoritative {
			gh.mu.Lock()
			rm, ok := gh.relayedMatches[matchID]
			if ok {
				rm.mu.RLock()
				payload := map[string]interface{}{
					"cid": "",
					"match_data": map[string]interface{}{
						"match_id": matchID,
						"presence": map[string]interface{}{
							"user_id":    s.UserID,
							"username":   s.Username,
							"session_id": s.ID,
						},
						"op_code": env.MatchDataSend.OpCode,
						"data":     env.MatchDataSend.Data,
					},
				}
				payloadBytes, _ := json.Marshal(payload)
				for _, sess := range rm.Sessions {
					if sess.ID != s.ID {
						select {
						case sess.Send <- payloadBytes:
						default:
						}
					}
				}
				rm.mu.RUnlock()
			}
			gh.mu.Unlock()
		}

	} else if env.MatchmakerAdd != nil {
		if gh.Matchmaker == nil {
			gh.logger.Warn("Matchmaker not configured in WebSocket gateway")
			return
		}

		queueName := env.MatchmakerAdd.QueueName
		if queueName == "" {
			queueName = "default"
		}

		t := &matchmaker.Ticket{
			ID:                uuid.New().String(),
			UserID:            s.UserID,
			Username:          s.Username,
			Region:            env.MatchmakerAdd.StringProperties["region"],
			CreatedAt:         time.Now(),
			Query:             env.MatchmakerAdd.Query,
			MinCount:          env.MatchmakerAdd.MinCount,
			MaxCount:          env.MatchmakerAdd.MaxCount,
			StringProperties:  env.MatchmakerAdd.StringProperties,
			NumericProperties: env.MatchmakerAdd.NumericProperties,
			CountMultiple:     env.MatchmakerAdd.CountMultiple,
			ReversePrecision:  env.MatchmakerAdd.ReversePrecision,
			QueueName:         queueName,
		}

		if skillVal, ok := env.MatchmakerAdd.NumericProperties["skill"]; ok {
			t.SkillRating = int(skillVal)
		} else {
			t.SkillRating = 1000
		}

		err := gh.Matchmaker.Submit(context.Background(), t)
		if err != nil {
			gh.logger.Error("Failed to submit ticket via WebSocket", zap.Error(err))
			res := map[string]interface{}{
				"cid":   env.Cid,
				"error": "Failed to submit ticket: " + err.Error(),
			}
			resBytes, _ := json.Marshal(res)
			s.Send <- resBytes
			return
		}

		res := map[string]interface{}{
			"cid": env.Cid,
			"matchmaker_ticket": map[string]interface{}{
				"ticket_id":  t.ID,
				"queue_name": t.QueueName,
				"status":     "queued",
			},
		}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes

	} else if env.MatchmakerRemove != nil {
		if gh.Matchmaker == nil {
			gh.logger.Warn("Matchmaker not configured in WebSocket gateway")
			return
		}

		err := gh.Matchmaker.Cancel(context.Background(), env.MatchmakerRemove.TicketID)
		if err != nil {
			gh.logger.Error("Failed to cancel ticket via WebSocket", zap.Error(err))
			res := map[string]interface{}{
				"cid":   env.Cid,
				"error": "Failed to cancel ticket: " + err.Error(),
			}
			resBytes, _ := json.Marshal(res)
			s.Send <- resBytes
			return
		}

		res := map[string]interface{}{
			"cid": env.Cid,
			"matchmaker_remove": map[string]interface{}{
				"ticket_id": env.MatchmakerRemove.TicketID,
			},
		}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes
	}
}
