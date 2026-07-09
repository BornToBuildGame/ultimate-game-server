package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
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
	logger      *zap.Logger
	cfg         Config
	dbPool      *pgxpool.Pool
	tokenMgr    *auth.TokenManager
	sessReg     *auth.SessionRegistry
	rateLimiter *IPTokenBucketRateLimiter

	httpServer *http.Server
	gRPCServer *grpc.Server

	RuntimeManager *runtime.GoRuntimeManager
}

// SetRuntimeManager configures the runtime manager for hook interceptors.
func (s *Server) SetRuntimeManager(rm *runtime.GoRuntimeManager) {
	s.RuntimeManager = rm
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

	return &Server{
		logger:      logger,
		cfg:         cfg,
		dbPool:      dbPool,
		tokenMgr:    tm,
		sessReg:     auth.NewSessionRegistry(),
		rateLimiter: NewIPRateLimiter(cfg.RateLimitMax, cfg.RateLimitRefill),
	}, nil
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

	s.httpServer = &http.Server{
		Addr:    s.cfg.HTTPAddr,
		Handler: handler,
	}

	// 2. Setup gRPC Server
	s.gRPCServer = grpc.NewServer()

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
