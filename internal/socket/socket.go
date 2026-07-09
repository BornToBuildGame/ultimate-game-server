package socket

import (
	"net/http"
	"sync"
	"time"

	"ultimate-game-server/internal/auth"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
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
	ID        string
	UserID    string
	Username  string
	Conn      *websocket.Conn
	Send      chan []byte
	CloseOnce sync.Once
	IsActive  bool
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

// GatewayHandler upgrades connections and starts connection lifecycle loops.
type GatewayHandler struct {
	logger      *zap.Logger
	tokenMgr    *auth.TokenManager
	registry    *ConnectionRegistry
	onConnect   func(s *Session)
	onDisconnect func(sessionID string)
}

// NewGatewayHandler creates a new GatewayHandler.
func NewGatewayHandler(
	logger *zap.Logger,
	tm *auth.TokenManager,
	reg *ConnectionRegistry,
	onConnect func(s *Session),
	onDisconnect func(sessionID string),
) *GatewayHandler {
	return &GatewayHandler{
		logger:       logger,
		tokenMgr:     tm,
		registry:     reg,
		onConnect:    onConnect,
		onDisconnect: onDisconnect,
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
		_, _, err := s.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				gh.logger.Warn("WebSocket closed unexpectedly", zap.String("session_id", s.ID), zap.Error(err))
			}
			break
		}
		// Reset deadline on successful message read
		s.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
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
	})
}
