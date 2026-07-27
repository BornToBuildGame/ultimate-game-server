package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"ultimate-game-server/internal/console/acl"
	"ultimate-game-server/internal/console/consolepb"
	"ultimate-game-server/internal/satori"

	"github.com/blevesearch/bleve/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/soheilhy/cmux"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
)

// ConsoleClaims defines the JWT claims for authenticated console operators.
type ConsoleClaims struct {
	UserID     string                 `json:"user_id"`
	Username   string                 `json:"username"`
	Email      string                 `json:"email"`
	ACL        map[string]interface{} `json:"acl"`         // legacy JSON flags (compat)
	ACLBitmap  string                 `json:"acl_bitmap"`  // reference-aligned bitmap
	jwt.RegisteredClaims
}

func (c *ConsoleClaims) Permission() acl.Permission {
	if c == nil {
		return acl.None()
	}
	if c.ACLBitmap != "" {
		if p, err := acl.Parse(c.ACLBitmap); err == nil {
			return p
		}
	}
	b, _ := json.Marshal(c.ACL)
	return acl.FromDBACL(b)
}

// Server encapsulates the Console Admin HTTP server.
type Server struct {
	logger         Logger
	pool           *pgxpool.Pool
	jwtSecret      []byte
	listener       net.Listener
	httpServer     *http.Server
	grpcServer     *grpc.Server
	bleveIndex     bleve.Index
	auditChan      chan *AuditLogEntry
	wg             sync.WaitGroup
	mu             sync.Mutex
	sessionRevoker SessionRevoker
	startTime      time.Time

	// Optional deps for Phase 2c parity (set via setters).
	matchLister    MatchLister
	matchStater    MatchStater
	rpcDispatcher  RPCDispatcher
	statusProvider StatusProvider
	satoriClient   *satori.Client

	revokedTokens sync.Map // token string -> struct{}
}

// SessionRevoker invalidates player sessions (e.g. on ban).
type SessionRevoker interface {
	RevokeAllSessions(userID string)
}

// MatchLister lists active matches for console status/ops.
type MatchLister interface {
	ListMatches() []map[string]interface{}
}

// MatchStater returns match state JSON for a match id.
type MatchStater interface {
	GetMatchState(matchID string) (map[string]interface{}, error)
}

// RPCDispatcher invokes a runtime RPC as an admin.
type RPCDispatcher interface {
	DispatchRPC(ctx context.Context, id, payload string, userID, username string) (string, error)
}

// StatusProvider supplies node status metrics.
type StatusProvider interface {
	ConsoleStatus() map[string]interface{}
}

// SetSessionRevoker configures session revocation on ban/delete.
func (s *Server) SetSessionRevoker(r SessionRevoker) { s.sessionRevoker = r }

// SetMatchDeps wires match list/state helpers.
func (s *Server) SetMatchDeps(lister MatchLister, stater MatchStater) {
	s.matchLister = lister
	s.matchStater = stater
}

// SetRPCDispatcher wires CallRpc.
func (s *Server) SetRPCDispatcher(d RPCDispatcher) { s.rpcDispatcher = d }

// SetStatusProvider wires GET /console/api/status.
func (s *Server) SetStatusProvider(p StatusProvider) { s.statusProvider = p }

// SetSatoriClient wires Satori console RPCs.
func (s *Server) SetSatoriClient(c *satori.Client) { s.satoriClient = c }

// Logger interface matching our requirements.
type Logger interface {
	Info(msg string, fields ...any)
	Error(msg string, fields ...any)
}

// AuditLogEntry is the queue payload for console_audit_log.
type AuditLogEntry struct {
	ID              string
	ConsoleUserID   string
	ConsoleUsername string
	Email           string
	Action          string
	Resource        string
	Message         string
	Metadata        map[string]any
	CreateTime      time.Time
}

// NewServer initializes the Console Admin server.
func NewServer(logger Logger, pool *pgxpool.Pool, jwtSecret []byte) (*Server, error) {
	mapping := bleve.NewIndexMapping()
	index, err := bleve.NewMemOnly(mapping)
	if err != nil {
		return nil, fmt.Errorf("failed to create mem-only bleve index: %w", err)
	}

	s := &Server{
		logger:     logger,
		pool:       pool,
		jwtSecret:  jwtSecret,
		bleveIndex: index,
		auditChan:  make(chan *AuditLogEntry, 1000),
		startTime:  time.Now().UTC(),
	}

	s.wg.Add(1)
	go s.auditWorker()

	return s, nil
}

// Start opens the network listener and starts HTTP (/v2/console + legacy /console) and gRPC Console.
func (s *Server) Start(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = l

	m := cmux.New(l)
	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.Any())

	mux := http.NewServeMux()
	mux.HandleFunc("POST /console/authenticate", s.handleAuthenticate)
	mux.HandleFunc("POST /console/logout", s.handleLogout)
	mux.HandleFunc("POST /console/api/users/ban", s.handleBanUser)
	mux.HandleFunc("GET /console/api/search", s.handleSearch)
	s.registerOperatorRoutes(mux)
	s.registerPlayerRoutes(mux)
	s.registerParityRoutes(mux)
	s.registerLeaderboardRoutes(mux)
	s.registerFriendsRoutes(mux)
	s.registerDeferredRoutes(mux)
	s.registerV2ConsoleRoutes(mux)

	s.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	s.grpcServer = grpc.NewServer()
	consolepb.RegisterConsoleServer(s.grpcServer, NewGRPCConsole(s))

	s.logger.Info("Console Admin server listening (HTTP /v2/console + gRPC)", "address", addr)
	go func() {
		if err := s.grpcServer.Serve(grpcL); err != nil {
			s.logger.Error("Console gRPC closed", "err", err)
		}
	}()
	go func() {
		if err := s.httpServer.Serve(httpL); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("Console Admin HTTP closed with error", "err", err)
		}
	}()
	go func() {
		if err := m.Serve(); err != nil {
			s.logger.Error("Console cmux closed", "err", err)
		}
	}()

	return nil
}

// Close gracefully terminates HTTP/gRPC servers and async workers.
func (s *Server) Close() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(ctx)
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	close(s.auditChan)
	s.wg.Wait()
	_ = s.bleveIndex.Close()
}

func parseACL(dbAclBytes []byte) map[string]interface{} {
	aclMap := make(map[string]interface{})
	if len(dbAclBytes) > 0 {
		_ = json.Unmarshal(dbAclBytes, &aclMap)
	}
	return aclMap
}

// hasACL returns true if claims grant the legacy flag via bitmap (or admin).
func hasACL(claims *ConsoleClaims, flag string) bool {
	if claims == nil {
		return false
	}
	return acl.LegacyFlagAccess(claims.Permission(), flag)
}

func (s *Server) requireACL(w http.ResponseWriter, claims *ConsoleClaims, flag string) bool {
	if hasACL(claims, flag) {
		return true
	}
	http.Error(w, "Forbidden", http.StatusForbidden)
	return false
}

func (s *Server) requirePerm(w http.ResponseWriter, claims *ConsoleClaims, res acl.Resource, level acl.PermissionLevel) bool {
	if claims != nil && claims.Permission().HasAccess(acl.NewPermission(res, level)) {
		return true
	}
	http.Error(w, "Forbidden", http.StatusForbidden)
	return false
}

func (s *Server) queueAudit(claims *ConsoleClaims, action, resource, message string, meta map[string]any) {
	if claims == nil {
		return
	}
	if meta == nil {
		meta = map[string]any{}
	}
	audit := &AuditLogEntry{
		ID:              uuid.New().String(),
		ConsoleUserID:   claims.UserID,
		ConsoleUsername: claims.Username,
		Email:           claims.Email,
		Action:          action,
		Resource:        resource,
		Message:         message,
		Metadata:        meta,
		CreateTime:      time.Now().UTC(),
	}
	select {
	case s.auditChan <- audit:
	default:
		s.logger.Error("audit channel full; dropping entry", "action", action)
	}
	s.mu.Lock()
	_ = s.bleveIndex.Index(audit.ID, audit)
	s.mu.Unlock()
}

func (s *Server) handleAuthenticate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	username := r.FormValue("username")
	password := r.FormValue("password")
	totpCode := r.FormValue("totp")
	if username == "" || password == "" {
		http.Error(w, "Unauthorized: invalid credentials", http.StatusUnauthorized)
		return
	}

	var dbID, dbEmail, dbUsername string
	var dbPassword, dbMfaSecret []byte
	var dbAclBytes []byte
	var dbMfaRequired bool
	var dbDisableTime time.Time

	query := `SELECT id, username, email, password, acl, disable_time, mfa_secret, mfa_required 
	          FROM console_user WHERE username = $1`
	err := s.pool.QueryRow(r.Context(), query, username).Scan(
		&dbID, &dbUsername, &dbEmail, &dbPassword, &dbAclBytes, &dbDisableTime, &dbMfaSecret, &dbMfaRequired,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Unauthorized: invalid credentials", http.StatusUnauthorized)
			return
		}
		http.Error(w, "Internal Database Error", http.StatusInternalServerError)
		return
	}

	if dbDisableTime.After(time.Unix(0, 0)) {
		http.Error(w, "Unauthorized: account disabled", http.StatusUnauthorized)
		return
	}

	if err = bcrypt.CompareHashAndPassword(dbPassword, []byte(password)); err != nil {
		http.Error(w, "Unauthorized: invalid credentials", http.StatusUnauthorized)
		return
	}

	if dbMfaRequired {
		if len(dbMfaSecret) == 0 || !ValidateTOTP(dbMfaSecret, totpCode) {
			http.Error(w, "Unauthorized: invalid MFA code", http.StatusUnauthorized)
			return
		}
	}

	aclMap := parseACL(dbAclBytes)
	perm := acl.FromDBACL(dbAclBytes)
	jti := uuid.New().String()
	claims := &ConsoleClaims{
		UserID:    dbID,
		Username:  dbUsername,
		Email:     dbEmail,
		ACL:       aclMap,
		ACLBitmap: perm.String(),
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        jti,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(12 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(s.jwtSecret)
	if err != nil {
		http.Error(w, "Internal Token Error", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    tokenString,
		Path:     "/",
		Expires:  time.Now().Add(12 * time.Hour),
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": true, "username": dbUsername, "acl": aclMap, "acl_bitmap": perm.String(),
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie("session_token"); err == nil && cookie.Value != "" {
		s.revokedTokens.Store(cookie.Value, struct{}{})
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func (s *Server) handleBanUser(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.requireACL(w, claims, "write_players") {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	targetUserID := r.FormValue("user_id")
	if targetUserID == "" {
		targetUserID = r.URL.Query().Get("user_id")
	}
	if _, err := uuid.Parse(targetUserID); err != nil {
		http.Error(w, "Invalid user_id", http.StatusBadRequest)
		return
	}

	_, err = s.pool.Exec(r.Context(), `UPDATE users SET disable_time = now(), update_time = now() WHERE id = $1`, targetUserID)
	if err != nil {
		http.Error(w, "Failed to ban player", http.StatusInternalServerError)
		return
	}
	if s.sessionRevoker != nil {
		s.sessionRevoker.RevokeAllSessions(targetUserID)
	}
	s.queueAudit(claims, "ban_player", "users/"+targetUserID, "Banned user "+targetUserID, map[string]any{"target_user_id": targetUserID})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "banned", "user_id": targetUserID})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasACL(claims, "admin") && !hasACL(claims, "write_players") && !hasACL(claims, "read_players") {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	queryStr := r.URL.Query().Get("q")
	if len(queryStr) < 3 {
		http.Error(w, "Search query must be at least 3 characters", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	query := bleve.NewMatchQuery(queryStr)
	searchRequest := bleve.NewSearchRequest(query)
	searchRequest.Size = 50

	resultsChan := make(chan *bleve.SearchResult, 1)
	errChan := make(chan error, 1)
	go func() {
		s.mu.Lock()
		res, err := s.bleveIndex.Search(searchRequest)
		s.mu.Unlock()
		if err != nil {
			errChan <- err
			return
		}
		resultsChan <- res
	}()

	select {
	case res := <-resultsChan:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"hits": res.Total, "note": "audit_index"})
	case err := <-errChan:
		s.logger.Error("Search failure", "err", err)
		http.Error(w, "Search failed", http.StatusInternalServerError)
	case <-ctx.Done():
		http.Error(w, "Search timed out", http.StatusGatewayTimeout)
	}
}

func (s *Server) authenticateRequest(r *http.Request) (*ConsoleClaims, error) {
	cookie, err := r.Cookie("session_token")
	if err != nil {
		return nil, err
	}
	if _, revoked := s.revokedTokens.Load(cookie.Value); revoked {
		return nil, errors.New("token revoked")
	}

	token, err := jwt.ParseWithClaims(cookie.Value, &ConsoleClaims{}, func(token *jwt.Token) (interface{}, error) {
		return s.jwtSecret, nil
	})
	if err != nil || !token.Valid {
		return nil, errors.New("invalid token")
	}

	claims, ok := token.Claims.(*ConsoleClaims)
	if !ok {
		return nil, errors.New("invalid claims type")
	}
	return claims, nil
}

func (s *Server) auditWorker() {
	defer s.wg.Done()

	for entry := range s.auditChan {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		meta, _ := json.Marshal(entry.Metadata)
		query := `INSERT INTO console_audit_log (id, console_user_id, console_username, email, action, resource, message, metadata, create_time)
		          VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)`
		_, err := s.pool.Exec(ctx, query,
			entry.ID,
			entry.ConsoleUserID,
			entry.ConsoleUsername,
			entry.Email,
			entry.Action,
			entry.Resource,
			entry.Message,
			string(meta),
			entry.CreateTime,
		)
		cancel()
		if err != nil {
			s.logger.Error("failed to write console audit log", "err", err)
		}
	}
}

// writeJSON helper.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
