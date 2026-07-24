package socket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/match"
	"ultimate-game-server/internal/matchmaker"
	"ultimate-game-server/internal/party"
	"ultimate-game-server/internal/presence"
	"ultimate-game-server/internal/runtime"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const defaultSendBufferSize = 64

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     checkOrigin,
}

func checkOrigin(r *http.Request) bool {
	allowed := strings.TrimSpace(os.Getenv("SOCKET_ALLOWED_ORIGINS"))
	if allowed == "" || allowed == "*" {
		return true
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	for _, o := range strings.Split(allowed, ",") {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		if o == "*" || o == origin {
			return true
		}
		if strings.EqualFold(o, "localhost") &&
			(strings.Contains(origin, "localhost") || strings.Contains(origin, "127.0.0.1")) {
			return true
		}
	}
	return false
}

// Session wraps a live WebSocket client connection.
type Session struct {
	mu          sync.RWMutex
	ID          string
	UserID      string
	Username    string
	Conn        *websocket.Conn
	Send        chan []byte
	CloseOnce   sync.Once
	IsActive    bool
	TrackStatus bool
	matchIDs    map[string]bool // key: matchID
}

// TrySend enqueues a payload if the session is active; closes the connection under backpressure.
func (s *Session) TrySend(payload []byte) {
	if s == nil || !s.IsActive || s.Send == nil {
		return
	}
	select {
	case s.Send <- payload:
	default:
		s.CloseConn()
	}
}

// CloseConn closes the underlying WebSocket connection (nil-safe).
func (s *Session) CloseConn() {
	if s == nil {
		return
	}
	s.IsActive = false
	if s.Conn != nil {
		_ = s.Conn.Close()
	}
}

// MatchIDs returns a copy of joined match IDs.
func (s *Session) MatchIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.matchIDs))
	for id := range s.matchIDs {
		ids = append(ids, id)
	}
	return ids
}

// ConnectionRegistry maintains active and recovering user WebSocket sessions.
type ConnectionRegistry struct {
	mu           sync.RWMutex
	sessions     map[string]*Session            // key: sessionID -> Session
	userSessions map[string]map[string]*Session // key: userID -> set of sessionIDs -> Session
	graceTimers  map[string]*time.Timer         // key: sessionID -> Recovery Timer
	GracePeriod  time.Duration                  // Reconnection grace period
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

	s.IsActive = true
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

// GetUserSessions returns active sessions for a user.
func (cr *ConnectionRegistry) GetUserSessions(userID string) []*Session {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	userMap, exists := cr.userSessions[userID]
	if !exists {
		return nil
	}
	out := make([]*Session, 0, len(userMap))
	for _, sess := range userMap {
		if sess != nil && sess.IsActive {
			out = append(out, sess)
		}
	}
	return out
}

// AllSessions returns a snapshot of all active sessions.
func (cr *ConnectionRegistry) AllSessions() []*Session {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	out := make([]*Session, 0, len(cr.sessions))
	for _, sess := range cr.sessions {
		if sess != nil && sess.IsActive {
			out = append(out, sess)
		}
	}
	return out
}

// SendToSession sends a message to a specific active session.
// When the send buffer is full the session is closed (backpressure).
func (cr *ConnectionRegistry) SendToSession(sessionID string, payload []byte) {
	cr.mu.RLock()
	s, exists := cr.sessions[sessionID]
	cr.mu.RUnlock()
	if exists {
		s.TrySend(payload)
	}
}

// SendToUser sends a message to all active sessions of a user.
// When a session send buffer is full that session is closed (backpressure).
func (cr *ConnectionRegistry) SendToUser(userID string, payload []byte) {
	cr.mu.RLock()
	userMap, exists := cr.userSessions[userID]
	var sessions []*Session
	if exists {
		sessions = make([]*Session, 0, len(userMap))
		for _, sess := range userMap {
			sessions = append(sessions, sess)
		}
	}
	cr.mu.RUnlock()
	for _, sess := range sessions {
		sess.TrySend(payload)
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
	mu              sync.RWMutex
	logger          *zap.Logger
	tokenMgr        *auth.TokenManager
	registry        *ConnectionRegistry
	onConnect       func(s *Session)
	onDisconnect    func(sessionID, userID, username string)
	Router          *match.Router
	Matchmaker      *matchmaker.Matchmaker
	PartyRegistry   *party.Registry
	HookRegistry    *runtime.HookRegistry
	PresenceTracker *presence.PresenceTracker
	StreamTracker   *presence.StreamTracker
	StatusRegistry  *presence.StatusRegistry
	MessageRouter   *presence.LocalMessageRouter
	MaxStatusBytes  int
	MaxSubscriptions int
	dbPool          *pgxpool.Pool
	sendBufSize     int
	relayedMatches  map[string]*RelayedMatch
	channels        map[string]map[string]*Session // channelID -> sessionID -> Session
	channelMeta     map[string]map[string]channelMemberMeta
	rdb             *redis.Client
	nodeID          string
	relayCancel     context.CancelFunc
	rpcInvoker      func(ctx context.Context, userID, username, id, payload string) (result string, code int, err error)
}

// SetRPCInvoker configures WebSocket custom RPC dispatch.
func (gh *GatewayHandler) SetRPCInvoker(fn func(ctx context.Context, userID, username, id, payload string) (string, int, error)) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	gh.rpcInvoker = fn
}

// SetMatchmaker configures the Matchmaker instance for ticket routing.
func (gh *GatewayHandler) SetMatchmaker(mm *matchmaker.Matchmaker) {
	gh.Matchmaker = mm
}

// SetPartyRegistry configures the party registry for party matchmaker add/remove.
func (gh *GatewayHandler) SetPartyRegistry(reg *party.Registry) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	gh.PartyRegistry = reg
}

// SetHookRegistry configures runtime hooks for realtime interceptors.
func (gh *GatewayHandler) SetHookRegistry(hr *runtime.HookRegistry) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	gh.HookRegistry = hr
}

// SetRedisClient configures the Redis client instance.
func (gh *GatewayHandler) SetRedisClient(rdb *redis.Client) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	gh.rdb = rdb
}

// SetNodeID configures the cluster node label used for match metadata and Pub/Sub.
func (gh *GatewayHandler) SetNodeID(nodeID string) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	if nodeID == "" {
		nodeID = match.ResolveNodeID()
	}
	gh.nodeID = nodeID
}

// StartRelayFanoutListener subscribes to cross-node relayed match envelopes and delivers locally.
func (gh *GatewayHandler) StartRelayFanoutListener(parent context.Context) {
	gh.mu.Lock()
	rdb := gh.rdb
	nodeID := gh.nodeID
	if gh.relayCancel != nil {
		gh.relayCancel()
		gh.relayCancel = nil
	}
	gh.mu.Unlock()
	if rdb == nil {
		return
	}
	if nodeID == "" {
		nodeID = match.ResolveNodeID()
		gh.SetNodeID(nodeID)
	}
	ctx, cancel := context.WithCancel(parent)
	gh.mu.Lock()
	gh.relayCancel = cancel
	gh.mu.Unlock()

	pubsub := rdb.Subscribe(ctx, match.RelayFanoutChannel)
	go func() {
		defer pubsub.Close()
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var fanout match.RelayFanoutMessage
				if err := json.Unmarshal([]byte(msg.Payload), &fanout); err != nil {
					continue
				}
				if fanout.SourceNode == nodeID {
					continue
				}
				gh.deliverRelayFanout(fanout)
			}
		}
	}()
	gh.logger.Info("relay fan-out listener started", zap.String("node", nodeID))
}

func (gh *GatewayHandler) deliverRelayFanout(fanout match.RelayFanoutMessage) {
	gh.mu.RLock()
	rm, ok := gh.relayedMatches[fanout.MatchID]
	gh.mu.RUnlock()
	if !ok {
		return
	}
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	for _, sess := range rm.Sessions {
		if sess != nil && sess.IsActive {
			sess.TrySend(fanout.Payload)
		}
	}
}

func (gh *GatewayHandler) publishRelayFanout(matchID, kind string, envelope []byte) {
	gh.mu.RLock()
	rdb := gh.rdb
	nodeID := gh.nodeID
	gh.mu.RUnlock()
	if rdb == nil {
		return
	}
	if nodeID == "" {
		nodeID = match.ResolveNodeID()
	}
	_ = match.PublishRelayFanout(context.Background(), rdb, nodeID, matchID, kind, envelope)
}

// SetPresenceTracker configures the online index for friends∩online (legacy field name).
func (gh *GatewayHandler) SetPresenceTracker(pt *presence.PresenceTracker) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	gh.PresenceTracker = pt
}

// SetStreamTracker configures the Local-first stream tracker (party/channel/match/status modes).
func (gh *GatewayHandler) SetStreamTracker(st *presence.StreamTracker) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	gh.StreamTracker = st
}

// SetStatusRegistry configures the status follow graph and fan-out.
func (gh *GatewayHandler) SetStatusRegistry(sr *presence.StatusRegistry) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	gh.StatusRegistry = sr
}

// SetMessageRouter configures the local message router for status delivery.
func (gh *GatewayHandler) SetMessageRouter(r *presence.LocalMessageRouter) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	gh.MessageRouter = r
}

// SetPresenceLimits configures status string and follow caps.
func (gh *GatewayHandler) SetPresenceLimits(maxStatusBytes, maxSubscriptions int) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	if maxStatusBytes > 0 {
		gh.MaxStatusBytes = maxStatusBytes
	}
	if maxSubscriptions > 0 {
		gh.MaxSubscriptions = maxSubscriptions
	}
}

// SetDBPool configures an optional Postgres pool for chat persistence.
func (gh *GatewayHandler) SetDBPool(pool *pgxpool.Pool) {
	gh.mu.Lock()
	defer gh.mu.Unlock()
	gh.dbPool = pool
}

// NewGatewayHandler creates a new GatewayHandler.
func NewGatewayHandler(
	logger *zap.Logger,
	tm *auth.TokenManager,
	reg *ConnectionRegistry,
	onConnect func(s *Session),
	onDisconnect func(sessionID, userID, username string),
	router *match.Router,
) *GatewayHandler {
	return &GatewayHandler{
		logger:         logger,
		tokenMgr:       tm,
		registry:       reg,
		onConnect:      onConnect,
		onDisconnect:   onDisconnect,
		Router:         router,
		sendBufSize:    defaultSendBufferSize,
		relayedMatches: make(map[string]*RelayedMatch),
		channels:       make(map[string]map[string]*Session),
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

	trackStatus := r.URL.Query().Get("status") == "true"

	bufSize := gh.sendBufSize
	if bufSize <= 0 {
		bufSize = defaultSendBufferSize
	}

	matchIDs := make(map[string]bool)
	if isReconnect {
		if old, ok := gh.registry.GetBySession(sessionID); ok {
			old.mu.RLock()
			for id := range old.matchIDs {
				matchIDs[id] = true
			}
			old.mu.RUnlock()
		}
	}

	session := &Session{
		ID:          sessionID,
		UserID:      claims.UserID,
		Username:    claims.Username,
		Conn:        conn,
		Send:        make(chan []byte, bufSize),
		IsActive:    true,
		TrackStatus: trackStatus,
		matchIDs:    matchIDs,
	}

	gh.registry.Add(session)
	gh.trackNotifications(session)

	// Re-attach reconnecting session to live relayed matches.
	if len(matchIDs) > 0 {
		gh.mu.Lock()
		for matchID := range matchIDs {
			if rm, ok := gh.relayedMatches[matchID]; ok {
				rm.mu.Lock()
				rm.Sessions[session.ID] = session
				rm.mu.Unlock()
			}
		}
		gh.mu.Unlock()
	}

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
		s.IsActive = false
		close(s.Send)
		if s.Conn != nil {
			s.Conn.Close()
		}
	})

	gh.leaveAllChannels(s)
	gh.leaveAllParties(s)

	// Start 30-second connection recovery grace period
	gh.registry.StartGracePeriod(s.ID, func() {
		if gh.Matchmaker != nil {
			_ = gh.Matchmaker.RemoveSessionAll(context.Background(), s.ID)
		}
		if gh.onDisconnect != nil {
			gh.onDisconnect(s.ID, s.UserID, s.Username)
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
							sess.TrySend(notificationBytes)
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
	Cid                   string                        `json:"cid,omitempty"`
	MatchCreate           *MatchCreatePayload           `json:"match_create,omitempty"`
	MatchJoin             *MatchJoinPayload             `json:"match_join,omitempty"`
	MatchLeave            *MatchLeavePayload            `json:"match_leave,omitempty"`
	MatchDataSend         *MatchDataSendPayload         `json:"match_data_send,omitempty"`
	MatchmakerAdd         *MatchmakerAddPayload         `json:"matchmaker_add,omitempty"`
	MatchmakerRemove      *MatchmakerRemovePayload      `json:"matchmaker_remove,omitempty"`
	PartyMatchmakerAdd    *PartyMatchmakerAddPayload    `json:"party_matchmaker_add,omitempty"`
	PartyMatchmakerRemove *PartyMatchmakerRemovePayload `json:"party_matchmaker_remove,omitempty"`
	PartyCreate            *PartyCreatePayload            `json:"party_create,omitempty"`
	PartyJoin              *PartyJoinPayload              `json:"party_join,omitempty"`
	PartyLeave             *PartyLeavePayload             `json:"party_leave,omitempty"`
	PartyPromote           *PartyPromotePayload           `json:"party_promote,omitempty"`
	PartyAccept            *PartyAcceptPayload            `json:"party_accept,omitempty"`
	PartyRemove            *PartyRemovePayload            `json:"party_remove,omitempty"`
	PartyClose             *PartyClosePayload             `json:"party_close,omitempty"`
	PartyUpdate            *PartyUpdatePayload            `json:"party_update,omitempty"`
	PartyJoinRequestList   *PartyJoinRequestListPayload   `json:"party_join_request_list,omitempty"`
	PartyDataSend          *PartyDataSendPayload          `json:"party_data_send,omitempty"`
	StatusFollow           *StatusFollowPayload           `json:"status_follow,omitempty"`
	StatusUnfollow         *StatusUnfollowPayload         `json:"status_unfollow,omitempty"`
	StatusUpdate           *StatusUpdatePayload           `json:"status_update,omitempty"`
	Ping                   *PingPayload                   `json:"ping,omitempty"`
	ChannelJoin            *ChannelJoinPayload            `json:"channel_join,omitempty"`
	ChannelLeave           *ChannelLeavePayload           `json:"channel_leave,omitempty"`
	ChannelMessageSend     *ChannelMessageSendPayload     `json:"channel_message_send,omitempty"`
	ChannelMessageUpdate   *ChannelMessageUpdatePayload   `json:"channel_message_update,omitempty"`
	ChannelMessageRemove   *ChannelMessageRemovePayload   `json:"channel_message_remove,omitempty"`
	Rpc                    *RpcPayload                    `json:"rpc,omitempty"`
}

type RpcPayload struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
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
	MatchID   string   `json:"match_id"`
	OpCode    int64    `json:"op_code"`
	Data      string   `json:"data"` // Base64 or raw action payload
	Reliable  bool     `json:"reliable"`
	Presences []string `json:"presences,omitempty"` // session IDs
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

type PartyMatchmakerAddPayload struct {
	PartyID           string             `json:"party_id"`
	QueueName         string             `json:"queue_name"`
	MinCount          int                `json:"min_count"`
	MaxCount          int                `json:"max_count"`
	StringProperties  map[string]string  `json:"string_properties"`
	NumericProperties map[string]float64 `json:"numeric_properties"`
	CountMultiple     int                `json:"count_multiple"`
	Query             string             `json:"query"`
}

type PartyMatchmakerRemovePayload struct {
	PartyID  string `json:"party_id"`
	TicketID string `json:"ticket_id"`
}

type PartyCreatePayload struct {
	Open    bool   `json:"open"`
	Hidden  bool   `json:"hidden"`
	MaxSize int    `json:"max_size"`
	Label   string `json:"label"`
}

type PartyJoinPayload struct {
	PartyID string `json:"party_id"`
}

type PartyLeavePayload struct {
	PartyID string `json:"party_id"`
}

type PartyPresencePayload struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	SessionID string `json:"session_id"`
}

type PartyPromotePayload struct {
	PartyID  string               `json:"party_id"`
	Presence PartyPresencePayload `json:"presence"`
}

type PartyAcceptPayload struct {
	PartyID  string               `json:"party_id"`
	Presence PartyPresencePayload `json:"presence"`
}

type PartyRemovePayload struct {
	PartyID  string               `json:"party_id"`
	Presence PartyPresencePayload `json:"presence"`
}

type PartyClosePayload struct {
	PartyID string `json:"party_id"`
}

type PartyUpdatePayload struct {
	PartyID string `json:"party_id"`
	Open    bool   `json:"open"`
	Hidden  bool   `json:"hidden"`
	Label   string `json:"label"`
}

type PartyJoinRequestListPayload struct {
	PartyID string `json:"party_id"`
}

type PartyDataSendPayload struct {
	PartyID string `json:"party_id"`
	OpCode  int64  `json:"op_code"`
	Data    string `json:"data"`
}

type StatusFollowPayload struct {
	UserIDs    []string `json:"user_ids"`
	Usernames  []string `json:"usernames"`
}

type StatusUnfollowPayload struct {
	UserIDs []string `json:"user_ids"`
}

// StatusUpdatePayload distinguishes JSON null (offline) from empty string (online blank).
type StatusUpdatePayload struct {
	Status *string `json:"status"`
}

type PingPayload struct{}

type ChannelJoinPayload struct {
	Target      string `json:"target"`
	Type        int    `json:"type"` // 1=ROOM, 2=DM, 3=GROUP
	Persistence *bool  `json:"persistence,omitempty"`
	Hidden      *bool  `json:"hidden,omitempty"`
}

type ChannelLeavePayload struct {
	ChannelID string `json:"channel_id"`
}

type ChannelMessageSendPayload struct {
	ChannelID string          `json:"channel_id"`
	Content   json.RawMessage `json:"content"`
}

type ChannelMessageUpdatePayload struct {
	ChannelID string          `json:"channel_id"`
	MessageID string          `json:"message_id"`
	Content   json.RawMessage `json:"content"`
}

type ChannelMessageRemovePayload struct {
	ChannelID string `json:"channel_id"`
	MessageID string `json:"message_id"`
}

func (gh *GatewayHandler) RouteMessage(s *Session, payload []byte) {
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		gh.logger.Warn("Failed to unmarshal websocket envelope", zap.Error(err))
		return
	}

	if env.Rpc != nil {
		gh.mu.RLock()
		invoker := gh.rpcInvoker
		gh.mu.RUnlock()
		res := map[string]interface{}{"cid": env.Cid}
		if invoker == nil {
			res["error"] = map[string]interface{}{"message": "RPC not available", "code": 14}
		} else {
			result, code, err := invoker(context.Background(), s.UserID, s.Username, env.Rpc.ID, env.Rpc.Payload)
			if err != nil {
				res["error"] = map[string]interface{}{"message": err.Error(), "code": code}
			} else {
				res["rpc"] = map[string]interface{}{"id": env.Rpc.ID, "payload": result}
			}
		}
		resBytes, _ := json.Marshal(res)
		s.TrySend(resBytes)
		return
	}

	if env.MatchCreate != nil {
		matchID := uuid.New().String() + "."
		gh.mu.Lock()
		gh.relayedMatches[matchID] = &RelayedMatch{
			MatchID:  matchID,
			Sessions: make(map[string]*Session),
		}
		gh.mu.Unlock()

		gh.mu.RLock()
		rdb := gh.rdb
		nodeID := gh.nodeID
		gh.mu.RUnlock()
		if nodeID == "" {
			nodeID = match.ResolveNodeID()
		}

		if rdb != nil {
			ctx := context.Background()
			pipe := rdb.Pipeline()
			pipe.Set(ctx, "match:node:"+matchID, nodeID, 0)
			pipe.SAdd(ctx, "match:ids", matchID)
			metadata := map[string]interface{}{
				"match_id":      matchID,
				"authoritative": "false",
				"label":         "{}",
				"size":          0,
				"max_size":      100,
				"node":          nodeID,
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
		s.TrySend(resBytes)

	} else if env.MatchJoin != nil {
		matchID := env.MatchJoin.MatchID
		if env.MatchJoin.Token != "" && gh.Matchmaker != nil {
			if result, err := gh.Matchmaker.ConsumeMatchToken(context.Background(), env.MatchJoin.Token); err == nil && result != nil {
				matchID = result.MatchID
			} else if err != nil {
				res := map[string]interface{}{
					"cid":   env.Cid,
					"error": "invalid match token",
				}
				resBytes, _ := json.Marshal(res)
				s.TrySend(resBytes)
				return
			}
		}

		if gh.Router != nil {
			if loop, ok := gh.Router.GetMatchLoop(matchID); ok {
				// Authoritative: async JoinAttempt off the read path
				go func(cid, mid string, sess *Session, meta map[string]string) {
					accept, err := loop.JoinAttempt(sess.UserID, sess.Username, sess.ID, meta)
					if err != nil || !accept {
						res := map[string]interface{}{
							"cid":   cid,
							"error": "Match join rejected",
						}
						resBytes, _ := json.Marshal(res)
						sess.TrySend(resBytes)
						return
					}
					sess.mu.Lock()
					sess.matchIDs[mid] = true
					sess.mu.Unlock()

					var presences []map[string]interface{}
					if l, ok := gh.Router.GetMatchLoop(mid); ok {
						for _, p := range l.GetPresences() {
							presences = append(presences, map[string]interface{}{
								"user_id":    p.UserID,
								"username":   p.Username,
								"session_id": p.SessionID,
							})
						}
					}
					res := map[string]interface{}{
						"cid": cid,
						"match_join": map[string]interface{}{
							"match_id":  mid,
							"presences": presences,
						},
					}
					resBytes, _ := json.Marshal(res)
					sess.TrySend(resBytes)
				}(env.Cid, matchID, s, env.MatchJoin.Metadata)
				return
			}
		}

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

		var presences []map[string]interface{}
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

		res := map[string]interface{}{
			"cid": env.Cid,
			"match_join": map[string]interface{}{
				"match_id":  matchID,
				"presences": presences,
			},
		}
		resBytes, _ := json.Marshal(res)
		s.TrySend(resBytes)

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
					sess.TrySend(notificationBytes)
				}
			}
			rm.mu.RUnlock()
		}
		gh.mu.Unlock()
		gh.publishRelayFanout(matchID, "match_presence", notificationBytes)

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
						sess.TrySend(notificationBytes)
					}
					rm.mu.RUnlock()
					gh.mu.Unlock()
					gh.publishRelayFanout(matchID, "match_presence", notificationBytes)
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
		s.TrySend(resBytes)

	} else if env.MatchDataSend != nil {
		matchID := env.MatchDataSend.MatchID
		authoritative := false

		if gh.Router != nil {
			if _, ok := gh.Router.GetMatchLoop(matchID); ok {
				authoritative = true
				input := match.MatchInput{
					UserID:    s.UserID,
					SessionID: s.ID,
					Username:  s.Username,
					OpCode:    env.MatchDataSend.OpCode,
					Payload:   env.MatchDataSend.Data,
					Reliable:  env.MatchDataSend.Reliable,
					Action:    fmt.Sprintf("%d", env.MatchDataSend.OpCode),
				}
				_ = gh.Router.ForwardInput(context.Background(), matchID, input)
			}
		}

		if !authoritative {
			gh.mu.Lock()
			rm, ok := gh.relayedMatches[matchID]
			if ok {
				filter := make(map[string]bool, len(env.MatchDataSend.Presences))
				for _, sid := range env.MatchDataSend.Presences {
					filter[sid] = true
				}
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
						"op_code":  env.MatchDataSend.OpCode,
						"data":     env.MatchDataSend.Data,
						"reliable": env.MatchDataSend.Reliable,
					},
				}
				payloadBytes, _ := json.Marshal(payload)
				for _, sess := range rm.Sessions {
					if len(filter) > 0 {
						if !filter[sess.ID] {
							continue
						}
					} else if sess.ID == s.ID {
						continue // no echo unless listed in presences
					}
					sess.TrySend(payloadBytes)
				}
				rm.mu.RUnlock()
				gh.mu.Unlock()
				gh.publishRelayFanout(matchID, "match_data", payloadBytes)
			} else {
				gh.mu.Unlock()
			}
		}

	} else if env.MatchmakerAdd != nil {
		if gh.Matchmaker == nil {
			gh.logger.Warn("Matchmaker not configured in WebSocket gateway")
			return
		}

		payload := interface{}(env.MatchmakerAdd)
		if gh.HookRegistry != nil {
			if before, ok := gh.HookRegistry.GetBefore("matchmakeradd"); ok {
				out, err := before(context.Background(), &socketRuntimeLogger{gh.logger}, nil, nil, payload)
				if err != nil {
					res := map[string]interface{}{"cid": env.Cid, "error": err.Error()}
					resBytes, _ := json.Marshal(res)
					s.Send <- resBytes
					return
				}
				if out != nil {
					payload = out
				}
			}
		}
		addReq, _ := payload.(*MatchmakerAddPayload)
		if addReq == nil {
			addReq = env.MatchmakerAdd
		}

		queueName := addReq.QueueName
		if queueName == "" {
			queueName = "default"
		}

		t := &matchmaker.Ticket{
			ID:                uuid.New().String(),
			UserID:            s.UserID,
			Username:          s.Username,
			SessionID:         s.ID,
			Region:            addReq.StringProperties["region"],
			CreatedAt:         time.Now(),
			Query:             addReq.Query,
			MinCount:          addReq.MinCount,
			MaxCount:          addReq.MaxCount,
			StringProperties:  addReq.StringProperties,
			NumericProperties: addReq.NumericProperties,
			CountMultiple:     addReq.CountMultiple,
			ReversePrecision:  addReq.ReversePrecision,
			QueueName:         queueName,
		}

		if skillVal, ok := addReq.NumericProperties["skill"]; ok {
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

		if gh.HookRegistry != nil {
			if after, ok := gh.HookRegistry.GetAfter("matchmakeradd"); ok {
				_ = after(context.Background(), &socketRuntimeLogger{gh.logger}, nil, nil, map[string]interface{}{
					"ticket_id": t.ID,
				}, addReq)
			}
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

		payload := interface{}(env.MatchmakerRemove)
		if gh.HookRegistry != nil {
			if before, ok := gh.HookRegistry.GetBefore("matchmakerremove"); ok {
				out, err := before(context.Background(), &socketRuntimeLogger{gh.logger}, nil, nil, payload)
				if err != nil {
					res := map[string]interface{}{"cid": env.Cid, "error": err.Error()}
					resBytes, _ := json.Marshal(res)
					s.Send <- resBytes
					return
				}
				if out != nil {
					payload = out
				}
			}
		}
		remReq, _ := payload.(*MatchmakerRemovePayload)
		if remReq == nil {
			remReq = env.MatchmakerRemove
		}

		err := gh.Matchmaker.Cancel(context.Background(), remReq.TicketID)
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

		if gh.HookRegistry != nil {
			if after, ok := gh.HookRegistry.GetAfter("matchmakerremove"); ok {
				_ = after(context.Background(), &socketRuntimeLogger{gh.logger}, nil, nil, map[string]interface{}{
					"ticket_id": remReq.TicketID,
				}, remReq)
			}
		}

		res := map[string]interface{}{
			"cid": env.Cid,
			"matchmaker_remove": map[string]interface{}{
				"ticket_id": remReq.TicketID,
			},
		}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes

	} else if env.PartyMatchmakerAdd != nil {
		gh.handlePartyMatchmakerAdd(s, env.Cid, env.PartyMatchmakerAdd)
	} else if env.PartyMatchmakerRemove != nil {
		gh.handlePartyMatchmakerRemove(s, env.Cid, env.PartyMatchmakerRemove)
	} else if env.PartyCreate != nil {
		gh.handlePartyCreate(s, env.Cid, env.PartyCreate)
	} else if env.PartyJoin != nil {
		gh.handlePartyJoin(s, env.Cid, env.PartyJoin)
	} else if env.PartyLeave != nil {
		gh.handlePartyLeave(s, env.Cid, env.PartyLeave)
	} else if env.PartyPromote != nil {
		gh.handlePartyPromote(s, env.Cid, env.PartyPromote)
	} else if env.PartyAccept != nil {
		gh.handlePartyAccept(s, env.Cid, env.PartyAccept)
	} else if env.PartyRemove != nil {
		gh.handlePartyRemove(s, env.Cid, env.PartyRemove)
	} else if env.PartyClose != nil {
		gh.handlePartyClose(s, env.Cid, env.PartyClose)
	} else if env.PartyUpdate != nil {
		gh.handlePartyUpdate(s, env.Cid, env.PartyUpdate)
	} else if env.PartyJoinRequestList != nil {
		gh.handlePartyJoinRequestList(s, env.Cid, env.PartyJoinRequestList)
	} else if env.PartyDataSend != nil {
		gh.handlePartyDataSend(s, env.Cid, env.PartyDataSend)
	} else if env.StatusFollow != nil {
		gh.handleStatusFollow(s, env.Cid, env.StatusFollow)
	} else if env.StatusUnfollow != nil {
		gh.handleStatusUnfollow(s, env.Cid, env.StatusUnfollow)
	} else if env.StatusUpdate != nil {
		gh.handleStatusUpdate(s, env.Cid, env.StatusUpdate)
	} else if env.Ping != nil {
		gh.handlePing(s, env.Cid)
	} else if env.ChannelJoin != nil {
		gh.handleChannelJoin(s, env.Cid, env.ChannelJoin)
	} else if env.ChannelLeave != nil {
		gh.handleChannelLeave(s, env.Cid, env.ChannelLeave)
	} else if env.ChannelMessageSend != nil {
		gh.handleChannelMessageSend(s, env.Cid, env.ChannelMessageSend)
	} else if env.ChannelMessageUpdate != nil {
		gh.handleChannelMessageUpdate(s, env.Cid, env.ChannelMessageUpdate)
	} else if env.ChannelMessageRemove != nil {
		gh.handleChannelMessageRemove(s, env.Cid, env.ChannelMessageRemove)
	}
}

func (gh *GatewayHandler) handlePartyMatchmakerAdd(s *Session, cid string, req *PartyMatchmakerAddPayload) {
	if gh.Matchmaker == nil || gh.PartyRegistry == nil {
		res := map[string]interface{}{"cid": cid, "error": "party matchmaker not configured"}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes
		return
	}
	p, err := gh.PartyRegistry.GetParty(req.PartyID)
	if err != nil {
		res := map[string]interface{}{"cid": cid, "error": err.Error()}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes
		return
	}
	if p.LeaderID != s.UserID {
		res := map[string]interface{}{"cid": cid, "error": "only party leader can matchmake"}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes
		return
	}

	leader := &matchmaker.Presence{UserID: s.UserID, Username: s.Username, SessionID: s.ID}
	members := make([]*matchmaker.Presence, 0, len(p.Members))
	for _, m := range p.Members {
		if m.UserID == s.UserID {
			continue
		}
		members = append(members, &matchmaker.Presence{
			UserID:    m.UserID,
			Username:  m.Username,
			SessionID: m.SessionID,
		})
	}

	queueName := req.QueueName
	if queueName == "" {
		queueName = "default"
	}
	ticket := &matchmaker.Ticket{
		PartyID:           req.PartyID,
		QueueName:         queueName,
		MinCount:          req.MinCount,
		MaxCount:          req.MaxCount,
		StringProperties:  req.StringProperties,
		NumericProperties: req.NumericProperties,
		CountMultiple:     req.CountMultiple,
		Query:             req.Query,
		CreatedAt:         time.Now(),
	}
	if err := gh.Matchmaker.SubmitParty(context.Background(), leader, members, ticket); err != nil {
		res := map[string]interface{}{"cid": cid, "error": err.Error()}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes
		return
	}
	res := map[string]interface{}{
		"cid": cid,
		"party_matchmaker_ticket": map[string]interface{}{
			"ticket_id": ticket.ID,
			"party_id":  req.PartyID,
			"status":    "queued",
		},
	}
	resBytes, _ := json.Marshal(res)
	s.Send <- resBytes
}

func (gh *GatewayHandler) handlePartyMatchmakerRemove(s *Session, cid string, req *PartyMatchmakerRemovePayload) {
	if gh.Matchmaker == nil {
		res := map[string]interface{}{"cid": cid, "error": "matchmaker not configured"}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes
		return
	}
	if err := gh.Matchmaker.Cancel(context.Background(), req.TicketID); err != nil {
		res := map[string]interface{}{"cid": cid, "error": err.Error()}
		resBytes, _ := json.Marshal(res)
		s.Send <- resBytes
		return
	}
	res := map[string]interface{}{
		"cid": cid,
		"party_matchmaker_remove": map[string]interface{}{
			"ticket_id": req.TicketID,
			"party_id":  req.PartyID,
		},
	}
	resBytes, _ := json.Marshal(res)
	s.Send <- resBytes
}

type socketRuntimeLogger struct {
	zapLogger *zap.Logger
}

func (l *socketRuntimeLogger) Debug(format string, args ...interface{}) {
	l.zapLogger.Debug(fmt.Sprintf(format, args...))
}
func (l *socketRuntimeLogger) Info(format string, args ...interface{}) {
	l.zapLogger.Info(fmt.Sprintf(format, args...))
}
func (l *socketRuntimeLogger) Warn(format string, args ...interface{}) {
	l.zapLogger.Warn(fmt.Sprintf(format, args...))
}
func (l *socketRuntimeLogger) Error(format string, args ...interface{}) {
	l.zapLogger.Error(fmt.Sprintf(format, args...))
}
