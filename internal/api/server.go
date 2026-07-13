package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/api/storagepb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/match"
	"ultimate-game-server/internal/matchmaker"
	"ultimate-game-server/internal/runtime"
	"ultimate-game-server/internal/socket"
	"ultimate-game-server/internal/storage"

	"github.com/dop251/goja"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/yuin/gopher-lua"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Config defines the configuration options for the API server.
type Config struct {
	HTTPAddr        string        `json:"http_addr" yaml:"http_addr"`
	GRPCAddr        string        `json:"grpc_addr" yaml:"grpc_addr"`
	JWTSecret       []byte        `json:"jwt_secret" yaml:"jwt_secret"`
	JWTExpiry       time.Duration `json:"jwt_expiry" yaml:"jwt_expiry"`
	RateLimitMax    float64       `json:"rate_limit_max" yaml:"rate_limit_max"`
	RateLimitRefill float64       `json:"rate_limit_refill" yaml:"rate_limit_refill"`
}

// Server handles HTTP and gRPC network interfaces.
type Server struct {
	logger         *zap.Logger
	cfg            Config
	dbPool         *pgxpool.Pool
	tokenMgr       *auth.TokenManager
	sessReg        *auth.SessionRegistry
	rateLimiter    *IPTokenBucketRateLimiter
	SocketRegistry *socket.ConnectionRegistry
	SocketGateway  *socket.GatewayHandler
	MatchRouter    *match.Router
	Matchmaker     *matchmaker.Matchmaker
	rdb            *redis.Client

	httpServer *http.Server
	gRPCServer *grpc.Server

	RuntimeManager *runtime.GoRuntimeManager
	LuaVM          *lua.LState
	JSVM           *goja.Runtime
}

// SetRuntimeManager configures the runtime manager for hook interceptors.
func (s *Server) SetRuntimeManager(rm *runtime.GoRuntimeManager) {
	s.RuntimeManager = rm
	if rm != nil {
		if s.MatchRouter != nil {
			s.MatchRouter.SetDependencies(rm.Registry(), rm.Logger(), s.logger, rm.DB(), rm.NK())
		}
		if grm, ok := rm.NK().(*runtime.GoRuntimeModule); ok && s.MatchRouter != nil {
			grm.SetMatchRegistry(s.MatchRouter)
		}
		if s.Matchmaker != nil {
			s.Matchmaker.SetDependencies(rm.DB(), rm.NK(), rm.Registry())
		}
	}
}

// SetVMs configures the Lua and JavaScript VM instances.
func (s *Server) SetVMs(luaVM *lua.LState, jsVM *goja.Runtime) {
	s.LuaVM = luaVM
	s.JSVM = jsVM
}

// NewServer creates a new API Server instance.
func NewServer(logger *zap.Logger, cfg Config, dbPool *pgxpool.Pool) (*Server, error) {
	tm, err := auth.NewTokenManager(cfg.JWTSecret, cfg.JWTExpiry)
	if err != nil {
		return nil, fmt.Errorf("failed to create token manager: %w", err)
	}

	if cfg.RateLimitMax <= 0 {
		cfg.RateLimitMax = 100
	}
	if cfg.RateLimitRefill <= 0 {
		cfg.RateLimitRefill = 10
	}

	matchRouter := match.NewRouter()
	sockRegistry := socket.NewConnectionRegistry()
	sockGateway := socket.NewGatewayHandler(logger, tm, sockRegistry, nil, nil, matchRouter)

	// Initialize Redis connection for Matchmaker
	var rdb *redis.Client
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		logger.Warn("Redis is not available, falling back to local in-memory matchmaking", zap.Error(err))
		rdb = nil
	} else {
		logger.Info("Connected to Redis successfully, enabling distributed matchmaking", zap.String("addr", redisAddr))
	}
	pingCancel()

	matchRouter.SetSessionRegistry(sockRegistry)
	if rdb != nil {
		matchRouter.SetClusterConfig("node-local", rdb, nil)
		sockGateway.SetRedisClient(rdb)
	}

	s := &Server{
		logger:         logger,
		cfg:            cfg,
		dbPool:         dbPool,
		tokenMgr:       tm,
		sessReg:        auth.NewSessionRegistry(),
		rateLimiter:    NewIPRateLimiter(cfg.RateLimitMax, cfg.RateLimitRefill),
		SocketRegistry: sockRegistry,
		SocketGateway:  sockGateway,
		MatchRouter:    matchRouter,
		rdb:            rdb,
	}

	// Matchmaker callbacks (notifying matched players over WebSockets)
	onMatched := func(result matchmaker.MatchResult) {
		type WSPresence struct {
			UserID    string `json:"user_id"`
			Username  string `json:"username"`
			SessionID string `json:"session_id"`
		}

		presences := make([]WSPresence, len(result.PlayerIDs))
		for idx, pid := range result.PlayerIDs {
			sessIDs := s.SocketRegistry.GetUserSessionIDs(pid)
			sessID := ""
			if len(sessIDs) > 0 {
				sessID = sessIDs[0]
			}
			presences[idx] = WSPresence{
				UserID:    pid,
				Username:  result.Usernames[idx],
				SessionID: sessID,
			}
		}

		for idx, pid := range result.PlayerIDs {
			selfPresence := WSPresence{
				UserID:    pid,
				Username:  result.Usernames[idx],
				SessionID: presences[idx].SessionID,
			}

			notification := map[string]interface{}{
				"cid": "",
				"matchmaker_matched": map[string]interface{}{
					"ticket_id":   "",
					"match_id":    result.MatchID,
					"match_token": result.MatchToken,
					"presences":   presences,
					"self":        selfPresence,
				},
			}

			msgBytes, _ := json.Marshal(notification)
			s.SocketRegistry.SendToUser(pid, msgBytes)
		}
	}

	s.Matchmaker = matchmaker.NewMatchmaker(logger, rdb, nil, nil, nil, onMatched)
	s.SocketGateway.SetMatchmaker(s.Matchmaker)

	return s, nil
}

// Start boots the HTTP and gRPC listeners.
func (s *Server) Start(ctx context.Context) error {
	// 1. Setup HTTP Server
	mux := http.NewServeMux()
	s.registerRoutes(mux)

	// Wrap handlers in global middlewares
	var handler http.Handler = mux
	handler = RateLimitMiddleware(s.rateLimiter)(handler)
	handler = BodyLimitMiddleware(4096)(handler) // limit request size to 4KB
	handler = CORSMiddleware(handler)
	handler = SecurityHeadersMiddleware(handler)

	if s.RuntimeManager != nil {
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			HTTPHookMiddleware(s.RuntimeManager, s.LuaVM, s.JSVM, mux.ServeHTTP)(w, r)
		})
	}

	s.httpServer = &http.Server{
		Addr:    s.cfg.HTTPAddr,
		Handler: handler,
	}

	// 2. Setup gRPC Server
	var opts []grpc.ServerOption
	if s.RuntimeManager != nil {
		opts = append(opts, grpc.UnaryInterceptor(GRPCHookUnaryInterceptor(s.RuntimeManager, s.LuaVM, s.JSVM)))
	}
	s.gRPCServer = grpc.NewServer(opts...)
	storagepb.RegisterStorageServiceServer(s.gRPCServer, NewStorageServer(s.dbPool, s.tokenMgr))
	apipb.RegisterLeaderboardServiceServer(s.gRPCServer, NewLeaderboardServer(s.dbPool, nil, s.tokenMgr))
	apipb.RegisterTournamentServiceServer(s.gRPCServer, NewTournamentServer(s.dbPool, nil, s.tokenMgr))
	apipb.RegisterFriendsServiceServer(s.gRPCServer, NewFriendsServer(s.dbPool, s.tokenMgr))
	apipb.RegisterGroupServiceServer(s.gRPCServer, NewGroupServer(s.dbPool, s.tokenMgr))
	apipb.RegisterMatchmakerServiceServer(s.gRPCServer, NewMatchmakerServer(s.Matchmaker, s.tokenMgr))
	apipb.RegisterRealtimeServiceServer(s.gRPCServer, NewRealtimeServer(s.logger, s.MatchRouter, s.rdb, s.tokenMgr))

	// Start Matchmaker Tick Loop (Ticks every 1 second)
	s.Matchmaker.Start(ctx, 1000*time.Millisecond)

	// 3. Listen HTTP
	httpListener, err := net.Listen("tcp", s.cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("failed to bind HTTP port %s: %w", s.cfg.HTTPAddr, err)
	}

	go func() {
		s.logger.Info("Starting HTTP API Server", zap.String("addr", s.cfg.HTTPAddr))
		if err := s.httpServer.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("HTTP Server error", zap.Error(err))
		}
	}()

	// 4. Listen gRPC
	grpcListener, err := net.Listen("tcp", s.cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("failed to bind gRPC port %s: %w", s.cfg.GRPCAddr, err)
	}

	go func() {
		s.logger.Info("Starting gRPC API Server", zap.String("addr", s.cfg.GRPCAddr))
		if err := s.gRPCServer.Serve(grpcListener); err != nil {
			s.logger.Error("gRPC Server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP and gRPC servers.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Shutting down API servers...")

	s.Matchmaker.Stop()
	s.gRPCServer.GracefulStop()

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown HTTP server: %w", err)
		}
	}

	return nil
}

type authEmailRequest struct {
	Email       string `json:"email"`
	Password    string `json:"password"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	Register    bool   `json:"register"`
}

type authCustomRequest struct {
	CustomID string `json:"custom_id"`
}

type authSocialRequest struct {
	Account struct {
		Token string `json:"token"`
	} `json:"account"`
	Username string `json:"username"`
	Create   *bool  `json:"create"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type authResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	UserID       string `json:"user_id"`
	Username     string `json:"username"`
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /v2/account/authenticate/email", s.handleAuthenticateEmail)
	mux.HandleFunc("POST /v2/account/authenticate/custom", s.handleAuthenticateCustom)
	mux.HandleFunc("POST /v2/account/session/refresh", s.handleSessionRefresh)
	mux.HandleFunc("POST /v2/storage", s.handleWriteStorageObjects)
	mux.HandleFunc("POST /v2/storage/read", s.handleReadStorageObjects)
	mux.HandleFunc("POST /v2/storage/delete", s.handleDeleteStorageObjects)
	mux.HandleFunc("GET /v2/storage/{collection}", s.handleListStorageObjects)
	mux.HandleFunc("GET /ws", s.SocketGateway.Upgrade)
	mux.HandleFunc("POST /v2/account/authenticate/apple", s.handleAuthenticateApple)
	mux.HandleFunc("POST /v2/account/authenticate/google", s.handleAuthenticateGoogle)
	mux.HandleFunc("POST /v2/account/authenticate/facebook", s.handleAuthenticateFacebook)

	// Leaderboard Routes
	mux.HandleFunc("POST /v2/leaderboard", s.handleCreateLeaderboard)
	mux.HandleFunc("DELETE /v2/leaderboard/{id}", s.handleDeleteLeaderboard)
	mux.HandleFunc("POST /v2/leaderboard/{id}", s.handleSubmitScore)
	mux.HandleFunc("GET /v2/leaderboard/{id}", s.handleListLeaderboardRecords)
	mux.HandleFunc("GET /v2/leaderboard/{id}/owner/{owner_id}", s.handleGetOwnerRecord)
	mux.HandleFunc("GET /v2/leaderboard/{id}/around/{owner_id}", s.handleAroundPlayerLookup)
	mux.HandleFunc("DELETE /v2/leaderboard/{id}/owner/{owner_id}", s.handleDeleteRecord)

	// Tournament Routes
	mux.HandleFunc("POST /v2/tournament", s.handleCreateTournament)
	mux.HandleFunc("DELETE /v2/tournament/{id}", s.handleDeleteTournament)
	mux.HandleFunc("POST /v2/tournament/{id}/join", s.handleJoinTournament)
	mux.HandleFunc("GET /v2/tournament", s.handleListTournaments)

	// Friends Routes
	mux.HandleFunc("POST /v2/friend", s.handleAddFriends)
	mux.HandleFunc("GET /v2/friend", s.handleListFriends)
	mux.HandleFunc("DELETE /v2/friend", s.handleDeleteFriends)
	mux.HandleFunc("POST /v2/friend/block/{user_id}", s.handleBlockFriend)

	// Group Routes
	mux.HandleFunc("POST /v2/group", s.handleCreateGroup)
	mux.HandleFunc("PUT /v2/group/{id}", s.handleUpdateGroup)
	mux.HandleFunc("DELETE /v2/group/{id}", s.handleDeleteGroup)
	mux.HandleFunc("GET /v2/group", s.handleListGroups)
	mux.HandleFunc("POST /v2/group/{id}/join", s.handleJoinGroup)
	mux.HandleFunc("POST /v2/group/{id}/leave", s.handleLeaveGroup)
	mux.HandleFunc("POST /v2/group/{id}/kick", s.handleKickGroupUsers)
	mux.HandleFunc("POST /v2/group/{id}/promote", s.handlePromoteGroupUsers)
	mux.HandleFunc("POST /v2/group/{id}/demote", s.handleDemoteGroupUsers)
	mux.HandleFunc("GET /v2/group/{id}/user", s.handleListGroupMembers)

	// Realtime / Match Routes
	mux.HandleFunc("POST /v2/match", s.handleCreateMatch)
	mux.HandleFunc("GET /v2/match", s.handleListMatches)
	mux.HandleFunc("GET /v2/match/{match_id}", s.handleGetMatch)
	mux.HandleFunc("POST /v2/match/{match_id}/signal", s.handleMatchSignal)

	// Matchmaker Routes
	mux.HandleFunc("POST /v2/matchmaker/ticket", s.handleSubmitMatchmakerTicket)
	mux.HandleFunc("DELETE /v2/matchmaker/ticket/{ticket_id}", s.handleCancelMatchmakerTicket)
	mux.HandleFunc("GET /v2/matchmaker/ticket/{ticket_id}", s.handleGetMatchmakerTicket)
	mux.HandleFunc("GET /v2/matchmaker/stats/{queue_name}", s.handleGetQueueStats)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *Server) handleAuthenticateEmail(w http.ResponseWriter, r *http.Request) {
	var req authEmailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Execute Before Hook if registered
	if s.RuntimeManager != nil && s.RuntimeManager.HasBeforeHook("AuthenticateEmail") {
		res, err := s.RuntimeManager.InvokeBeforeHook(r.Context(), "AuthenticateEmail", &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if casted, ok := res.(*authEmailRequest); ok {
			req = *casted
		}
	}

	var user *auth.User
	var err error

	if req.Register {
		user, err = auth.RegisterEmail(r.Context(), s.dbPool, req.Username, req.Email, req.Password, req.DisplayName)
	} else {
		user, err = auth.AuthenticateEmail(r.Context(), s.dbPool, req.Email, req.Password)
	}

	if err != nil {
		status := http.StatusUnauthorized
		if req.Register {
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return
	}

	accessToken, refreshToken, err := s.tokenMgr.GenerateSession(user.ID.String(), user.Username)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")

	resp := authResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		UserID:       user.ID.String(),
		Username:     user.Username,
	}

	// Execute After Hook if registered
	if s.RuntimeManager != nil && s.RuntimeManager.HasAfterHook("AuthenticateEmail") {
		go func() {
			hookErr := s.RuntimeManager.InvokeAfterHook(context.Background(), "AuthenticateEmail", &resp, &req)
			if hookErr != nil {
				s.logger.Error("After hook failed", zap.Error(hookErr))
			}
		}()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleAuthenticateCustom(w http.ResponseWriter, r *http.Request) {
	var req authCustomRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	user, err := auth.AuthenticateCustom(r.Context(), s.dbPool, req.CustomID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	accessToken, refreshToken, err := s.tokenMgr.GenerateSession(user.ID.String(), user.Username)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")

	resp := authResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		UserID:       user.ID.String(),
		Username:     user.Username,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleAuthenticateApple(w http.ResponseWriter, r *http.Request) {
	var req authSocialRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	providerID, err := auth.VerifyAppleToken(r.Context(), req.Account.Token)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	user, err := auth.AuthenticateSocial(r.Context(), s.dbPool, "apple", providerID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	accessToken, refreshToken, err := s.tokenMgr.GenerateSession(user.ID.String(), user.Username)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")

	resp := authResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		UserID:       user.ID.String(),
		Username:     user.Username,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleAuthenticateGoogle(w http.ResponseWriter, r *http.Request) {
	var req authSocialRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	providerID, err := auth.VerifyGoogleToken(r.Context(), req.Account.Token)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	user, err := auth.AuthenticateSocial(r.Context(), s.dbPool, "google", providerID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	accessToken, refreshToken, err := s.tokenMgr.GenerateSession(user.ID.String(), user.Username)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")

	resp := authResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		UserID:       user.ID.String(),
		Username:     user.Username,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleAuthenticateFacebook(w http.ResponseWriter, r *http.Request) {
	var req authSocialRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	providerID, err := auth.VerifyFacebookToken(r.Context(), req.Account.Token)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	user, err := auth.AuthenticateSocial(r.Context(), s.dbPool, "facebook", providerID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	accessToken, refreshToken, err := s.tokenMgr.GenerateSession(user.ID.String(), user.Username)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")

	resp := authResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		UserID:       user.ID.String(),
		Username:     user.Username,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleSessionRefresh(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	userID, detected, err := s.sessReg.ValidateAndRotateSession(req.RefreshToken)
	if err != nil {
		if detected {
			// Theft detected! Entire token family revoked.
			s.logger.Warn("Session token reuse/theft detected! Revoking family.", zap.String("user_id", userID))
			http.Error(w, "compromised token", http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// We need to fetch username to generate access token claims
	var username string
	query := "SELECT username FROM users WHERE id = $1"
	err = s.dbPool.QueryRow(r.Context(), query, userID).Scan(&username)
	if err != nil {
		http.Error(w, "user not found", http.StatusUnauthorized)
		return
	}

	accessToken, newRefreshToken, err := s.tokenMgr.GenerateSession(userID, username)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	s.sessReg.RegisterSession(userID, newRefreshToken, req.RefreshToken)

	resp := authResponse{
		AccessToken:  accessToken,
		RefreshToken: newRefreshToken,
		UserID:       userID,
		Username:     username,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) authenticateREST(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
		return "", errors.New("missing or invalid authorization header")
	}
	tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return "", fmt.Errorf("invalid token: %w", err)
	}
	return claims.UserID, nil
}

func (s *Server) authenticateRESTWithUsername(r *http.Request) (string, string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
		return "", "", errors.New("missing or invalid authorization header")
	}
	tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return "", "", fmt.Errorf("invalid token: %w", err)
	}
	return claims.UserID, claims.Username, nil
}


func (s *Server) handleWriteStorageObjects(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		Objects []struct {
			Collection      string      `json:"collection"`
			Key             string      `json:"key"`
			Value           interface{} `json:"value"`
			Version         string      `json:"version"`
			PermissionRead  int16       `json:"permission_read"`
			PermissionWrite int16       `json:"permission_write"`
		} `json:"objects"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	objs := make([]*storage.StorageObject, len(req.Objects))
	for i, obj := range req.Objects {
		valBytes, err := json.Marshal(obj.Value)
		if err != nil {
			http.Error(w, "invalid json value", http.StatusBadRequest)
			return
		}
		objs[i] = &storage.StorageObject{
			Collection: obj.Collection,
			Key:        obj.Key,
			UserID:     userID,
			Value:      string(valBytes),
			Version:    obj.Version,
			Read:       obj.PermissionRead,
			Write:      obj.PermissionWrite,
		}
	}

	err = storage.WriteStorageObjects(r.Context(), s.dbPool, objs)
	if err != nil {
		if errors.Is(err, storage.ErrOCCConflict) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	acks := make([]map[string]interface{}, len(objs))
	for i, o := range objs {
		acks[i] = map[string]interface{}{
			"collection":  o.Collection,
			"key":         o.Key,
			"user_id":     o.UserID,
			"version":     o.Version,
			"create_time": time.Now().Format(time.RFC3339),
			"update_time": time.Now().Format(time.RFC3339),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"acks": acks})
}

func (s *Server) handleReadStorageObjects(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateREST(r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		ObjectIDs []struct {
			Collection string `json:"collection"`
			Key        string `json:"key"`
			UserID     string `json:"user_id"`
		} `json:"object_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	reqs := make([]storage.ReadRequest, len(req.ObjectIDs))
	for i, obj := range req.ObjectIDs {
		reqs[i] = storage.ReadRequest{
			Collection: obj.Collection,
			Key:        obj.Key,
			UserID:     obj.UserID,
		}
	}

	objs, err := storage.ReadStorageObjects(r.Context(), s.dbPool, reqs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := make([]map[string]interface{}, len(objs))
	for i, o := range objs {
		var valRaw interface{}
		_ = json.Unmarshal([]byte(o.Value), &valRaw)
		res[i] = map[string]interface{}{
			"collection":       o.Collection,
			"key":              o.Key,
			"user_id":          o.UserID,
			"value":            valRaw,
			"version":          o.Version,
			"permission_read":  o.Read,
			"permission_write": o.Write,
			"create_time":      time.Now().Format(time.RFC3339),
			"update_time":      time.Now().Format(time.RFC3339),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"objects": res})
}

func (s *Server) handleDeleteStorageObjects(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		ObjectIDs []struct {
			Collection string `json:"collection"`
			Key        string `json:"key"`
			Version    string `json:"version"`
		} `json:"object_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	reqs := make([]storage.DeleteRequest, len(req.ObjectIDs))
	for i, obj := range req.ObjectIDs {
		reqs[i] = storage.DeleteRequest{
			Collection: obj.Collection,
			Key:        obj.Key,
			UserID:     userID,
			Version:    obj.Version,
		}
	}

	err = storage.DeleteStorageObjects(r.Context(), s.dbPool, reqs)
	if err != nil {
		if errors.Is(err, storage.ErrOCCConflict) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListStorageObjects(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateREST(r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	collection := r.PathValue("collection")
	userID := r.URL.Query().Get("user_id")
	limitStr := r.URL.Query().Get("limit")
	cursor := r.URL.Query().Get("cursor")

	limit := 20
	if limitStr != "" {
		var l int
		if _, err := fmt.Sscanf(limitStr, "%d", &l); err == nil && l > 0 {
			limit = l
		}
	}

	objs, nextCursor, err := storage.ListStorageObjects(r.Context(), s.dbPool, userID, collection, limit, cursor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := make([]map[string]interface{}, len(objs))
	for i, o := range objs {
		var valRaw interface{}
		_ = json.Unmarshal([]byte(o.Value), &valRaw)
		res[i] = map[string]interface{}{
			"collection":       o.Collection,
			"key":              o.Key,
			"user_id":          o.UserID,
			"value":            valRaw,
			"version":          o.Version,
			"permission_read":  o.Read,
			"permission_write": o.Write,
			"create_time":      time.Now().Format(time.RFC3339),
			"update_time":      time.Now().Format(time.RFC3339),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"objects":     res,
		"next_cursor": nextCursor,
	})
}

func (s *Server) handleSubmitMatchmakerTicket(w http.ResponseWriter, r *http.Request) {
	userID, username, err := s.authenticateRESTWithUsername(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var req struct {
		QueueName         string             `json:"queue_name"`
		MinCount          int                `json:"min_count"`
		MaxCount          int                `json:"max_count"`
		StringProperties  map[string]string  `json:"string_properties"`
		NumericProperties map[string]float64 `json:"numeric_properties"`
		CountMultiple     int                `json:"count_multiple"`
		ReversePrecision  bool               `json:"reverse_precision"`
		Query             string             `json:"query"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	queueName := req.QueueName
	if queueName == "" {
		queueName = "default"
	}

	t := &matchmaker.Ticket{
		ID:                uuid.New().String(),
		UserID:            userID,
		Username:          username,
		Region:            req.StringProperties["region"],
		CreatedAt:         time.Now(),
		Query:             req.Query,
		MinCount:          req.MinCount,
		MaxCount:          req.MaxCount,
		StringProperties:  req.StringProperties,
		NumericProperties: req.NumericProperties,
		CountMultiple:     req.CountMultiple,
		ReversePrecision:  req.ReversePrecision,
		QueueName:         queueName,
	}

	if skillVal, ok := req.NumericProperties["skill"]; ok {
		t.SkillRating = int(skillVal)
	} else {
		t.SkillRating = 1000 // default fallback
	}

	err = s.Matchmaker.Submit(r.Context(), t)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ticket_id":  t.ID,
		"queue_name": t.QueueName,
		"created_at": t.CreatedAt.Format(time.RFC3339),
	})
}

func (s *Server) handleCancelMatchmakerTicket(w http.ResponseWriter, r *http.Request) {
	_, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	ticketID := r.PathValue("ticket_id")
	if ticketID == "" {
		http.Error(w, "missing ticket_id", http.StatusBadRequest)
		return
	}

	err = s.Matchmaker.Cancel(r.Context(), ticketID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetMatchmakerTicket(w http.ResponseWriter, r *http.Request) {
	_, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	ticketID := r.PathValue("ticket_id")
	if ticketID == "" {
		http.Error(w, "missing ticket_id", http.StatusBadRequest)
		return
	}

	t, err := s.Matchmaker.GetTicket(r.Context(), ticketID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ticket_id":   t.ID,
		"queue_name":  t.QueueName,
		"status":      t.Status,
		"created_at":  t.CreatedAt.Format(time.RFC3339),
		"match_id":    t.MatchID,
		"match_token": t.MatchToken,
	})
}

func (s *Server) handleGetQueueStats(w http.ResponseWriter, r *http.Request) {
	_, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	queueName := r.PathValue("queue_name")
	if queueName == "" {
		queueName = "default"
	}

	count, _, err := s.Matchmaker.GetQueueStats(r.Context(), queueName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"queue_name":       queueName,
		"ticket_count":     count,
		"average_wait_sec": 0,
		"active_matches":   0,
	})
}

