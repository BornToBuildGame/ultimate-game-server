package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/api/storagepb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/chat"
	"ultimate-game-server/internal/economy"
	"ultimate-game-server/internal/leaderboard"
	"ultimate-game-server/internal/match"
	"ultimate-game-server/internal/matchmaker"
	"ultimate-game-server/internal/notification"
	"ultimate-game-server/internal/party"
	"ultimate-game-server/internal/presence"
	"ultimate-game-server/internal/runtime"
	"ultimate-game-server/internal/social"
	"ultimate-game-server/internal/socket"
	"ultimate-game-server/internal/storage"
	"ultimate-game-server/internal/tournament"

	"github.com/dop251/goja"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/yuin/gopher-lua"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Config defines the configuration options for the API server.
type Config struct {
	HTTPAddr        string        `json:"http_addr" yaml:"http_addr"`
	GRPCAddr        string        `json:"grpc_addr" yaml:"grpc_addr"`
	JWTSecret       []byte        `json:"jwt_secret" yaml:"jwt_secret"`
	JWTExpiry       time.Duration `json:"jwt_expiry" yaml:"jwt_expiry"`
	RateLimitMax    float64       `json:"rate_limit_max" yaml:"rate_limit_max"`
	RateLimitRefill float64       `json:"rate_limit_refill" yaml:"rate_limit_refill"`
	RPCHTTPKey      string        `json:"rpc_http_key" yaml:"rpc_http_key"`
	RPCTimeoutMs    int           `json:"rpc_execution_timeout_ms" yaml:"rpc_execution_timeout_ms"`
	RPCMaxPayload   int           `json:"rpc_max_payload_size_bytes" yaml:"rpc_max_payload_size_bytes"`
	RuntimePath     string        `json:"runtime_path" yaml:"runtime_path"`
	PresenceMaxSubscriptions int  `json:"presence_max_subscriptions_per_user" yaml:"presence_max_subscriptions_per_user"`
	PresenceMaxStatusBytes   int  `json:"presence_max_status_bytes" yaml:"presence_max_status_bytes"`
}

// Server handles HTTP and gRPC network interfaces.
type Server struct {
	logger         *zap.Logger
	cfg            Config
	dbPool         *pgxpool.Pool
	tokenMgr       *auth.TokenManager
	sessReg        *auth.SessionRegistry
	rateLimiter    *IPTokenBucketRateLimiter
	authRateLimit  *IPTokenBucketRateLimiter
	loginLockout   *auth.LoginLockout
	SocketRegistry *socket.ConnectionRegistry
	SocketGateway  *socket.GatewayHandler
	MatchRouter    *match.Router
	Matchmaker     *matchmaker.Matchmaker
	PartyRegistry    *party.Registry
	presenceTracker  *presence.PresenceTracker
	streamManager    *runtime.LocalStreamManager
	rdb              *redis.Client

	httpServer *http.Server
	gRPCServer *grpc.Server

	RuntimeManager *runtime.GoRuntimeManager
	LuaVM          *lua.LState
	JSVM           *goja.Runtime

	TournamentScheduler *tournament.TournamentScheduler
	lifecycleCancel     context.CancelFunc
	rankTrimStop        chan struct{}

	notificationServer *NotificationServer
	economyServer      *EconomyServer
	iapServer          *IAPServer
	rpcServer          *RpcServer
	rpcCfg             runtime.RPCConfig
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
		if grm, ok := rm.NK().(*runtime.GoRuntimeModule); ok && s.PartyRegistry != nil {
			grm.SetPartyLister(&partyListAdapter{reg: s.PartyRegistry})
		}
		if grm, ok := rm.NK().(*runtime.GoRuntimeModule); ok && s.SocketGateway != nil {
			grm.SetStatusFollower(s.SocketGateway)
		}
		if grm, ok := rm.NK().(*runtime.GoRuntimeModule); ok && s.streamManager != nil {
			grm.SetStreamManager(s.streamManager)
		}
		if s.Matchmaker != nil {
			s.Matchmaker.SetDependencies(rm.DB(), rm.NK(), rm.Registry())
		}
		if s.SocketGateway != nil {
			s.SocketGateway.SetHookRegistry(rm.Registry())
			ex := runtime.NewRtHookExecutor(rm.Registry(), rm.Logger(), rm.DB(), rm.NK(), s.LuaVM, s.JSVM, &luaVMMutex, &jsVMMutex)
			s.SocketGateway.SetRtHookExecutor(ex)
			s.SocketGateway.SetEventDispatcher(func(name, userID, sessionID, username string) {
				rm.Registry().DispatchEvent(context.Background(), rm.Logger(), &runtime.Event{
					Name: name,
					Properties: map[string]string{
						"user_id":    userID,
						"session_id": sessionID,
						"username":   username,
					},
					Timestamp: time.Now().Unix(),
				})
			})
		}
		if s.notificationServer != nil {
			s.notificationServer.SetHooks(rm.Registry())
		}
		if s.economyServer != nil {
			s.economyServer.SetHooks(rm.Registry())
		}
		if s.iapServer != nil {
			s.iapServer.SetHooks(rm.Registry())
		}
		if s.rpcServer != nil {
			s.rpcServer.SetRuntime(rm)
		}
		if grm, ok := rm.NK().(*runtime.GoRuntimeModule); ok {
			grm.SetRPCDispatcher(func(ctx context.Context, id, payload string, opts runtime.RPCDispatchOpts) (string, codes.Code, error) {
				return rm.DispatchRPC(ctx, id, payload, opts, s.LuaVM, s.JSVM, s.rpcCfg)
			})
		}
		if s.SocketGateway != nil {
			s.SocketGateway.SetRPCInvoker(func(ctx context.Context, userID, username, id, payload string) (string, int, error) {
				res, code, err := rm.DispatchRPC(ctx, id, payload, runtime.RPCDispatchOpts{
					UserID: userID, Username: username, ExecutionMode: "websocket",
				}, s.LuaVM, s.JSVM, s.rpcCfg)
				return res, int(code), err
			})
		}
		if s.TournamentScheduler != nil {
			s.wireSchedulerHooks()
		}
	}
}

// SetVMs configures the Lua and JavaScript VM instances.
func (s *Server) SetVMs(luaVM *lua.LState, jsVM *goja.Runtime) {
	s.LuaVM = luaVM
	s.JSVM = jsVM
	if s.rpcServer != nil {
		s.rpcServer.SetVMs(luaVM, jsVM)
	}
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
	rpcCfg := runtime.DefaultRPCConfig()
	if cfg.RPCHTTPKey != "" {
		rpcCfg.HTTPKey = cfg.RPCHTTPKey
	}
	if cfg.RPCTimeoutMs > 0 {
		rpcCfg.ExecutionTimeoutMs = cfg.RPCTimeoutMs
	}
	if cfg.RPCMaxPayload > 0 {
		rpcCfg.MaxPayloadBytes = cfg.RPCMaxPayload
	}
	if v := os.Getenv("UGE_RPC_HTTP_KEY"); v != "" {
		rpcCfg.HTTPKey = v
	}

	matchRouter := match.NewRouter()
	sockRegistry := socket.NewConnectionRegistry()
	onlineIndex := presence.NewOnlineIndex()
	streamTracker := presence.NewLocalTracker()
	msgRouter := presence.NewLocalMessageRouter(sockRegistry)
	statusRegistry := presence.NewStatusRegistry(msgRouter, onlineIndex, 1024)

	maxSubs := cfg.PresenceMaxSubscriptions
	if maxSubs <= 0 {
		maxSubs = 1000
	}
	maxStatus := cfg.PresenceMaxStatusBytes
	if maxStatus <= 0 {
		maxStatus = 2048
	}

	var sockGateway *socket.GatewayHandler
	onConnect := func(s *socket.Session) {
		if sockGateway != nil {
			sockGateway.TrackStatusOnConnect(s)
			sockGateway.EmitSessionEvent("session_start", s.UserID, s.ID, s.Username)
		}
	}
	onDisconnect := func(sessionID, userID, username string) {
		if sockGateway != nil {
			sockGateway.EmitSessionEvent("session_end", userID, sessionID, username)
			sockGateway.UntrackStatusOnDisconnect(sessionID, userID, username)
		} else {
			_ = streamTracker.UntrackAll(sessionID)
			statusRegistry.UnfollowAll(sessionID)
		}
	}
	sockGateway = socket.NewGatewayHandler(logger, tm, sockRegistry, onConnect, onDisconnect, matchRouter)
	sockGateway.SetPresenceTracker(onlineIndex)
	sockGateway.SetStreamTracker(streamTracker)
	sockGateway.SetStatusRegistry(statusRegistry)
	sockGateway.SetMessageRouter(msgRouter)
	sockGateway.SetPresenceLimits(maxStatus, maxSubs)
	streamMgr := &runtime.LocalStreamManager{
		Tracker:  streamTracker,
		Router:   msgRouter,
		Registry: sockGateway,
	}
	if dbPool != nil {
		sockGateway.SetDBPool(dbPool)
	}
	social.DefaultChannelBroadcaster = sockGateway
	chat.DefaultRouter = sockGateway
	notification.DefaultDeliverer = sockGateway

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
	nodeID := match.ResolveNodeID()
	sockGateway.SetNodeID(nodeID)
	if rdb != nil {
		forwarder := match.NewPubSubForwarder(rdb, nodeID, logger)
		matchRouter.SetClusterConfig(nodeID, rdb, forwarder)
		matchRouter.StartClusterInputListener(context.Background())
		matchRouter.StartClusterSignalListener(context.Background())
		sockGateway.SetRedisClient(rdb)
		sockGateway.StartRelayFanoutListener(context.Background())
	}

	sessStore := auth.NewSessionStoreFromRedis(rdb)
	s := &Server{
		logger:          logger,
		cfg:             cfg,
		dbPool:          dbPool,
		tokenMgr:        tm,
		sessReg:         auth.NewSessionRegistryWithStore(sessStore),
		rateLimiter:     NewIPRateLimiter(cfg.RateLimitMax, cfg.RateLimitRefill),
		authRateLimit:   NewIPRateLimiter(10, 10.0/60.0), // 10 auth req/min/IP
		loginLockout:    auth.NewLoginLockout(),
		SocketRegistry:  sockRegistry,
		SocketGateway:   sockGateway,
		MatchRouter:     matchRouter,
		presenceTracker: onlineIndex,
		streamManager:   streamMgr,
		rdb:             rdb,
		rpcCfg:          rpcCfg,
	}

	// Matchmaker callbacks (notifying matched players over WebSockets)
	onMatched := func(result matchmaker.MatchResult) {
		type WSPresence struct {
			UserID    string `json:"user_id"`
			Username  string `json:"username"`
			SessionID string `json:"session_id"`
		}

		users := make([]WSPresence, 0, len(result.Users))
		if len(result.Users) > 0 {
			for _, u := range result.Users {
				sessID := u.SessionID
				if sessID == "" {
					sessIDs := s.SocketRegistry.GetUserSessionIDs(u.UserID)
					if len(sessIDs) > 0 {
						sessID = sessIDs[0]
					}
				}
				users = append(users, WSPresence{
					UserID:    u.UserID,
					Username:  u.Username,
					SessionID: sessID,
				})
			}
		} else {
			for idx, pid := range result.PlayerIDs {
				sessIDs := s.SocketRegistry.GetUserSessionIDs(pid)
				sessID := ""
				if len(sessIDs) > 0 {
					sessID = sessIDs[0]
				}
				uname := ""
				if idx < len(result.Usernames) {
					uname = result.Usernames[idx]
				}
				users = append(users, WSPresence{UserID: pid, Username: uname, SessionID: sessID})
			}
		}

		for _, u := range users {
			ticketID := ""
			if result.TicketIDs != nil {
				ticketID = result.TicketIDs[u.UserID]
			}
			payload := map[string]interface{}{
				"ticket_id": ticketID,
				"match_id":  result.MatchID,
				"users":     users,
				"self":      u,
			}
			if !result.Authoritative && result.MatchToken != "" {
				payload["token"] = result.MatchToken
				payload["match_token"] = result.MatchToken
			}

			notification := map[string]interface{}{
				"cid":                "",
				"matchmaker_matched": payload,
			}
			msgBytes, _ := json.Marshal(notification)
			s.SocketRegistry.SendToUser(u.UserID, msgBytes)
		}
	}

	s.Matchmaker = matchmaker.NewMatchmaker(logger, rdb, nil, nil, nil, onMatched)
	s.Matchmaker.SetSpawnMatch(func(result matchmaker.MatchResult) {
		if s.MatchRouter == nil || !result.Authoritative {
			return
		}
		module := result.Module
		if module == "" {
			module = os.Getenv("UGE_DEFAULT_MATCH_HANDLER")
		}
		if module == "" {
			logger.Warn("authoritative matchmaker result missing module; skip spawn")
			return
		}
		matchID := result.MatchID
		if matchID == "" || !strings.Contains(matchID, ".") {
			matchID = match.NewAuthoritativeMatchID()
		}
		params := map[string]interface{}{}
		_ = s.MatchRouter.CreateAndRegisterMatch(context.Background(), matchID, module, params)
	})
	s.SocketGateway.SetMatchmaker(s.Matchmaker)
	s.PartyRegistry = party.NewRegistryWithConfig(party.Config{
		Node:            nodeID,
		SingleParty:     true,
		DefaultMaxSize:  party.DefaultMaxSize,
		AbsoluteMaxSize: party.AbsoluteMaxSize,
		IdleCheckMs:     30000,
	})
	s.PartyRegistry.SetMembershipChangeHook(func(partyID string) {
		if s.Matchmaker != nil {
			_ = s.Matchmaker.RemovePartyAll(context.Background(), partyID)
		}
	})
	s.SocketGateway.SetPartyRegistry(s.PartyRegistry)

	// Leaderboard/tournament background scheduler (hooks connected via SetRuntimeManager / StartLifecycle).
	s.TournamentScheduler = tournament.NewTournamentScheduler(dbPool, rdb, logger, nil)
	s.rankTrimStop = make(chan struct{})

	return s, nil
}

// Redis returns the optional Redis client.
func (s *Server) Redis() *redis.Client {
	return s.rdb
}

// StartLifecycle starts leaderboard invalidation listener, rank trimmer, and tournament scheduler.
func (s *Server) StartLifecycle(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	s.lifecycleCancel = cancel
	leaderboard.StartInvalidationListener(ctx, s.rdb)
	leaderboard.StartRankCacheTrimmer(s.rankTrimStop)

	if s.RuntimeManager != nil {
		s.wireSchedulerHooks()
	}
	s.TournamentScheduler.Start(30 * time.Second)
}

func (s *Server) wireSchedulerHooks() {
	reg := s.RuntimeManager.Registry()
	if reg == nil {
		return
	}
	nk := s.RuntimeManager.NK()
	s.TournamentScheduler.SetRewardHook(func(ctx context.Context, pool *pgxpool.Pool, tournamentID string, expiryTime time.Time, topRecords []*leaderboard.LeaderboardRecord) error {
		if fn := reg.GetTournamentEnd(); fn != nil {
			return fn(ctx, s.RuntimeManager.Logger(), nil, nk, tournamentID, expiryTime.Unix(), expiryTime.Unix())
		}
		return nil
	})
	s.TournamentScheduler.SetResetHook(func(ctx context.Context, pool *pgxpool.Pool, tournamentID string, endActive, nextReset int64) error {
		if fn := reg.GetTournamentReset(); fn != nil {
			return fn(ctx, s.RuntimeManager.Logger(), nil, nk, tournamentID, endActive, nextReset)
		}
		return nil
	})
	s.TournamentScheduler.SetLeaderboardResetHook(func(ctx context.Context, pool *pgxpool.Pool, leaderboardID string, resetUnix int64) error {
		if fn := reg.GetLeaderboardReset(); fn != nil {
			return fn(ctx, s.RuntimeManager.Logger(), nil, nk, leaderboardID, resetUnix)
		}
		return nil
	})
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
	apipb.RegisterLeaderboardServiceServer(s.gRPCServer, NewLeaderboardServer(s.dbPool, s.rdb, s.tokenMgr))
	apipb.RegisterTournamentServiceServer(s.gRPCServer, NewTournamentServer(s.dbPool, s.rdb, s.tokenMgr))
	apipb.RegisterFriendsServiceServer(s.gRPCServer, NewFriendsServer(s.dbPool, s.tokenMgr, s.presenceTracker))
	apipb.RegisterGroupServiceServer(s.gRPCServer, NewGroupServer(s.dbPool, s.tokenMgr))
	apipb.RegisterMatchmakerServiceServer(s.gRPCServer, NewMatchmakerServer(s.Matchmaker, s.tokenMgr))
	apipb.RegisterRealtimeServiceServer(s.gRPCServer, NewRealtimeServer(s.logger, s.MatchRouter, s.rdb, s.tokenMgr))
	apipb.RegisterPartyServiceServer(s.gRPCServer, NewPartyServer(s.PartyRegistry, s.tokenMgr))
	apipb.RegisterChatServiceServer(s.gRPCServer, NewChannelServer(s.dbPool, s.tokenMgr, nil))
	s.notificationServer = NewNotificationServer(s.dbPool, s.tokenMgr, nil)
	s.economyServer = NewEconomyServer(s.dbPool, s.tokenMgr, nil)
	s.iapServer = NewIAPServer(s.dbPool, s.tokenMgr, nil, economy.DefaultIAPConfig)
	if s.RuntimeManager != nil {
		s.notificationServer.SetHooks(s.RuntimeManager.Registry())
		s.economyServer.SetHooks(s.RuntimeManager.Registry())
		s.iapServer.SetHooks(s.RuntimeManager.Registry())
	}
	apipb.RegisterNotificationServiceServer(s.gRPCServer, s.notificationServer)
	apipb.RegisterEconomyServiceServer(s.gRPCServer, s.economyServer)
	apipb.RegisterIAPServiceServer(s.gRPCServer, s.iapServer)
	s.rpcServer = NewRpcServer(s.dbPool, s.tokenMgr, s.RuntimeManager, s.rpcCfg)
	s.rpcServer.SetVMs(s.LuaVM, s.JSVM)
	apipb.RegisterRpcServiceServer(s.gRPCServer, s.rpcServer)
	if s.RuntimeManager != nil {
		if grm, ok := s.RuntimeManager.NK().(*runtime.GoRuntimeModule); ok {
			rm := s.RuntimeManager
			grm.SetRPCDispatcher(func(ctx context.Context, id, payload string, opts runtime.RPCDispatchOpts) (string, codes.Code, error) {
				return rm.DispatchRPC(ctx, id, payload, opts, s.LuaVM, s.JSVM, s.rpcCfg)
			})
		}
		if s.SocketGateway != nil {
			rm := s.RuntimeManager
			s.SocketGateway.SetRPCInvoker(func(ctx context.Context, userID, username, id, payload string) (string, int, error) {
				res, code, err := rm.DispatchRPC(ctx, id, payload, runtime.RPCDispatchOpts{
					UserID: userID, Username: username, ExecutionMode: "websocket",
				}, s.LuaVM, s.JSVM, s.rpcCfg)
				return res, int(code), err
			})
		}
	}
	apipb.RegisterAuthenticationServiceServer(s.gRPCServer, NewAuthServer(s))

	// Start Matchmaker Tick Loop (Ticks every 1 second)
	s.Matchmaker.Start(ctx, 1000*time.Millisecond)

	// Start leaderboard/tournament lifecycle (scheduler + redis invalidation + rank trim)
	s.StartLifecycle(ctx)

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

	if s.lifecycleCancel != nil {
		s.lifecycleCancel()
	}
	if s.rankTrimStop != nil {
		close(s.rankTrimStop)
	}
	if s.TournamentScheduler != nil {
		s.TournamentScheduler.Stop()
	}

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
	Email       string            `json:"email"`
	Password    string            `json:"password"`
	Username    string            `json:"username"`
	DisplayName string            `json:"display_name"`
	Register    bool              `json:"register"`
	Create      *bool             `json:"create"`
	Vars        map[string]string `json:"vars"`
}

type authCustomRequest struct {
	CustomID string            `json:"custom_id"`
	ID       string            `json:"id"`
	Create   *bool             `json:"create"`
	Username string            `json:"username"`
	Vars     map[string]string `json:"vars"`
}

type authSocialRequest struct {
	Account struct {
		Token string `json:"token"`
	} `json:"account"`
	Username string            `json:"username"`
	Create   *bool             `json:"create"`
	Vars     map[string]string `json:"vars"`
}

type refreshRequest struct {
	RefreshToken string            `json:"refresh_token"`
	Token        string            `json:"token"`
	Vars         map[string]string `json:"vars"`
}

type authResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	UserID       string `json:"user_id"`
	Username     string `json:"username"`
	Created      bool   `json:"created,omitempty"`
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /v2/account/authenticate/email", s.handleAuthenticateEmail)
	mux.HandleFunc("POST /v2/account/authenticate/custom", s.handleAuthenticateCustom)
	mux.HandleFunc("POST /v2/account/authenticate/device", s.handleAuthenticateDevice)
	mux.HandleFunc("POST /v2/account/authenticate/apple", s.handleAuthenticateApple)
	mux.HandleFunc("POST /v2/account/authenticate/google", s.handleAuthenticateGoogle)
	mux.HandleFunc("POST /v2/account/authenticate/facebook", s.handleAuthenticateFacebook)
	mux.HandleFunc("POST /v2/account/authenticate/steam", s.handleAuthenticateSteam)
	mux.HandleFunc("POST /v2/account/authenticate/gamecenter", s.handleAuthenticateGameCenter)
	mux.HandleFunc("POST /v2/account/session/refresh", s.handleSessionRefresh)
	mux.HandleFunc("POST /v2/account/session/logout", s.handleSessionLogout)
	mux.HandleFunc("GET /v2/account", s.handleGetAccount)
	mux.HandleFunc("PUT /v2/account", s.handleUpdateAccount)
	mux.HandleFunc("DELETE /v2/account", s.handleDeleteAccount)
	mux.HandleFunc("POST /v2/account/link/email", s.handleLinkEmail)
	mux.HandleFunc("POST /v2/account/link/device", s.handleLinkDevice)
	mux.HandleFunc("POST /v2/account/link/apple", s.handleLinkApple)
	mux.HandleFunc("POST /v2/account/link/google", s.handleLinkGoogle)
	mux.HandleFunc("POST /v2/account/link/facebook", s.handleLinkFacebook)
	mux.HandleFunc("POST /v2/account/link/steam", s.handleLinkSteam)
	mux.HandleFunc("POST /v2/account/link/custom", s.handleLinkCustom)
	mux.HandleFunc("POST /v2/account/unlink/email", s.handleUnlinkProvider("email"))
	mux.HandleFunc("POST /v2/account/unlink/device", s.handleUnlinkDevice)
	mux.HandleFunc("POST /v2/account/unlink/apple", s.handleUnlinkProvider("apple"))
	mux.HandleFunc("POST /v2/account/unlink/google", s.handleUnlinkProvider("google"))
	mux.HandleFunc("POST /v2/account/unlink/facebook", s.handleUnlinkProvider("facebook"))
	mux.HandleFunc("POST /v2/account/unlink/steam", s.handleUnlinkProvider("steam"))
	mux.HandleFunc("POST /v2/account/unlink/custom", s.handleUnlinkProvider("custom"))
	mux.HandleFunc("PUT /v2/storage", s.handleWriteStorageObjects)
	mux.HandleFunc("POST /v2/storage", s.handleReadStorageObjects)
	mux.HandleFunc("POST /v2/storage/read", s.handleReadStorageObjects)
	mux.HandleFunc("PUT /v2/storage/delete", s.handleDeleteStorageObjects)
	mux.HandleFunc("GET /v2/storage/{collection}/{user_id}", s.handleListStorageObjects)
	mux.HandleFunc("GET /v2/storage/{collection}", s.handleListStorageObjects)
	mux.HandleFunc("GET /ws", s.SocketGateway.Upgrade)
	mux.HandleFunc("GET /v2/stream", s.SocketGateway.Upgrade)

	// Leaderboard Routes
	mux.HandleFunc("GET /v2/leaderboard", s.handleListLeaderboards)
	mux.HandleFunc("POST /v2/leaderboard", s.handleCreateLeaderboard)
	mux.HandleFunc("DELETE /v2/leaderboard/{id}", s.handleDeleteLeaderboard)
	mux.HandleFunc("POST /v2/leaderboard/{id}", s.handleSubmitScore)
	mux.HandleFunc("GET /v2/leaderboard/{id}", s.handleListLeaderboardRecords)
	mux.HandleFunc("GET /v2/leaderboard/{id}/owner/{owner_id}", s.handleGetOwnerRecord)
	mux.HandleFunc("GET /v2/leaderboard/{id}/around/{owner_id}", s.handleAroundPlayerLookup)
	mux.HandleFunc("DELETE /v2/leaderboard/{id}/owner/{owner_id}", s.handleDeleteRecord)
	mux.HandleFunc("POST /v2/leaderboard/{id}/reset", s.handleManualLeaderboardReset)
	mux.HandleFunc("GET /v2/leaderboard/{id}/archive", s.handleListLeaderboardArchive)

	// Tournament Routes
	mux.HandleFunc("POST /v2/tournament", s.handleCreateTournament)
	mux.HandleFunc("DELETE /v2/tournament/{id}", s.handleDeleteTournament)
	mux.HandleFunc("POST /v2/tournament/{id}/join", s.handleJoinTournament)
	mux.HandleFunc("GET /v2/tournament", s.handleListTournaments)
	mux.HandleFunc("POST /v2/tournament/{id}", s.handleSubmitTournamentScore)
	mux.HandleFunc("GET /v2/tournament/{id}", s.handleListTournamentRecords)
	mux.HandleFunc("GET /v2/tournament/{id}/around/{owner_id}", s.handleTournamentAroundPlayer)
	mux.HandleFunc("DELETE /v2/tournament/{id}/owner/{owner_id}", s.handleDeleteTournamentRecord)
	mux.HandleFunc("GET /v2/tournament/{id}/season", s.handleListTournamentSeasons)

	// Friends Routes
	mux.HandleFunc("POST /v2/friend", s.handleAddFriends)
	mux.HandleFunc("GET /v2/friend", s.handleListFriends)
	mux.HandleFunc("GET /v2/friend/friends", s.handleListFriendsOfFriends)
	mux.HandleFunc("DELETE /v2/friend", s.handleDeleteFriends)
	mux.HandleFunc("POST /v2/friend/block/{user_id}", s.handleBlockFriend)
	mux.HandleFunc("DELETE /v2/friend/block/{user_id}", s.handleUnblockFriend)
	mux.HandleFunc("POST /v2/friend/facebook", s.handleImportFacebookFriends)
	mux.HandleFunc("POST /v2/friend/steam", s.handleImportSteamFriends)

	// Group Routes
	mux.HandleFunc("POST /v2/group", s.handleCreateGroup)
	mux.HandleFunc("PUT /v2/group/{id}", s.handleUpdateGroup)
	mux.HandleFunc("DELETE /v2/group/{id}", s.handleDeleteGroup)
	mux.HandleFunc("GET /v2/group", s.handleListGroups)
	mux.HandleFunc("POST /v2/group/{id}/join", s.handleJoinGroup)
	mux.HandleFunc("POST /v2/group/{id}/leave", s.handleLeaveGroup)
	mux.HandleFunc("POST /v2/group/{id}/add", s.handleAddGroupUsers)
	mux.HandleFunc("POST /v2/group/{id}/kick", s.handleKickGroupUsers)
	mux.HandleFunc("POST /v2/group/{id}/ban", s.handleBanGroupUsers)
	mux.HandleFunc("POST /v2/group/{id}/promote", s.handlePromoteGroupUsers)
	mux.HandleFunc("POST /v2/group/{id}/demote", s.handleDemoteGroupUsers)
	mux.HandleFunc("GET /v2/group/{id}/user", s.handleListGroupMembers)
	mux.HandleFunc("GET /v2/user/{user_id}/group", s.handleListUserGroups)

	// Realtime / Match Routes
	mux.HandleFunc("POST /v2/match", s.handleCreateMatch)
	mux.HandleFunc("GET /v2/match", s.handleListMatches)
	mux.HandleFunc("GET /v2/party", s.handleListParties)
	mux.HandleFunc("GET /v2/channel/{channel_id}", s.handleListChannelMessages)
	mux.HandleFunc("GET /v2/notification", s.handleListNotifications)
	mux.HandleFunc("DELETE /v2/notification", s.handleDeleteNotifications)
	mux.HandleFunc("GET /v2/rpc/{id}", s.handleRPC)
	mux.HandleFunc("POST /v2/rpc/{id}", s.handleRPC)
	mux.HandleFunc("GET /v2/wallet", s.handleGetWallet)
	mux.HandleFunc("GET /v2/wallet/ledger", s.handleListWalletLedger)
	mux.HandleFunc("POST /v2/iap/purchase/apple", s.handleValidatePurchaseApple)
	mux.HandleFunc("POST /v2/iap/purchase/google", s.handleValidatePurchaseGoogle)
	mux.HandleFunc("POST /v2/iap/purchase/huawei", s.handleValidatePurchaseHuawei)
	mux.HandleFunc("POST /v2/iap/purchase/facebookinstant", s.handleValidatePurchaseFacebookInstant)
	mux.HandleFunc("POST /v2/iap/purchase/samsung", s.handleValidatePurchaseSamsung)
	mux.HandleFunc("POST /v2/iap/subscription/apple", s.handleValidateSubscriptionApple)
	mux.HandleFunc("POST /v2/iap/subscription/google", s.handleValidateSubscriptionGoogle)
	mux.HandleFunc("POST /v2/iap/subscription", s.handleListSubscriptions)
	mux.HandleFunc("GET /v2/iap/subscription/{product_id}", s.handleGetSubscription)
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
	if !s.authRateLimit.Allow(s.clientIP(r)) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	var req authEmailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	in, err := s.invokeBefore(r.Context(), "AuthenticateEmail", &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if casted, ok := in.(*authEmailRequest); ok {
		req = *casted
	}

	ip := s.clientIP(r)
	if !req.Register {
		if err := s.loginLockout.Check(req.Email, ip); err != nil {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
			return
		}
	}

	var user *auth.User
	var created bool
	if req.Register {
		user, err = auth.RegisterEmail(r.Context(), s.dbPool, req.Username, req.Email, req.Password, req.DisplayName)
		created = true
	} else {
		user, err = auth.AuthenticateEmail(r.Context(), s.dbPool, req.Email, req.Password)
	}

	if err != nil {
		if !req.Register {
			s.loginLockout.RecordFailure(req.Email, ip)
		}
		status := http.StatusUnauthorized
		if req.Register {
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return
	}
	s.loginLockout.ClearSuccess(req.Email, ip)

	accessToken, refreshToken, err := s.tokenMgr.GenerateSessionWithVars(user.ID.String(), user.Username, req.Vars)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")
	resp := authResponse{
		AccessToken: accessToken, RefreshToken: refreshToken,
		UserID: user.ID.String(), Username: user.Username, Created: created,
	}
	s.invokeAfter("AuthenticateEmail", &resp, &req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleAuthenticateCustom(w http.ResponseWriter, r *http.Request) {
	if !s.authRateLimit.Allow(s.clientIP(r)) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	var req authCustomRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	in, err := s.invokeBefore(r.Context(), "AuthenticateCustom", &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if casted, ok := in.(*authCustomRequest); ok {
		req = *casted
	}
	customID := req.CustomID
	if customID == "" {
		customID = req.ID
	}
	opts := auth.AuthOptions{Create: createFlag(req.Create), Username: req.Username, Vars: req.Vars}
	user, created, err := auth.AuthenticateCustomWithOpts(r.Context(), s.dbPool, customID, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	accessToken, refreshToken, err := s.tokenMgr.GenerateSessionWithVars(user.ID.String(), user.Username, req.Vars)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")
	resp := authResponse{
		AccessToken: accessToken, RefreshToken: refreshToken,
		UserID: user.ID.String(), Username: user.Username, Created: created,
	}
	s.invokeAfter("AuthenticateCustom", &resp, &req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleSocialAuth(w http.ResponseWriter, r *http.Request, provider string, verify func(context.Context, string) (string, error), hook string) {
	if !s.authRateLimit.Allow(s.clientIP(r)) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	var req authSocialRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	in, err := s.invokeBefore(r.Context(), hook, &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if casted, ok := in.(*authSocialRequest); ok {
		req = *casted
	}
	providerID, err := verify(r.Context(), req.Account.Token)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	opts := auth.AuthOptions{Create: createFlag(req.Create), Username: req.Username, Vars: req.Vars}
	user, created, err := auth.AuthenticateSocialWithOpts(r.Context(), s.dbPool, provider, providerID, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	accessToken, refreshToken, err := s.tokenMgr.GenerateSessionWithVars(user.ID.String(), user.Username, req.Vars)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")
	resp := authResponse{
		AccessToken: accessToken, RefreshToken: refreshToken,
		UserID: user.ID.String(), Username: user.Username, Created: created,
	}
	s.invokeAfter(hook, &resp, &req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleAuthenticateApple(w http.ResponseWriter, r *http.Request) {
	s.handleSocialAuth(w, r, "apple", auth.VerifyAppleToken, "AuthenticateApple")
}
func (s *Server) handleAuthenticateGoogle(w http.ResponseWriter, r *http.Request) {
	s.handleSocialAuth(w, r, "google", auth.VerifyGoogleToken, "AuthenticateGoogle")
}
func (s *Server) handleAuthenticateFacebook(w http.ResponseWriter, r *http.Request) {
	s.handleSocialAuth(w, r, "facebook", auth.VerifyFacebookToken, "AuthenticateFacebook")
}

func (s *Server) handleSessionRefresh(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.RefreshToken == "" {
		req.RefreshToken = req.Token
	}
	in, err := s.invokeBefore(r.Context(), "SessionRefresh", &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if casted, ok := in.(*refreshRequest); ok {
		req = *casted
	}

	userID, detected, err := s.sessReg.ValidateAndRotateSession(req.RefreshToken)
	if err != nil {
		if detected {
			s.logger.Warn("Session token reuse/theft detected! Revoking family.", zap.String("user_id", userID))
			http.Error(w, "compromised token", http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var username string
	var disableTime time.Time
	err = s.dbPool.QueryRow(r.Context(), `SELECT username, disable_time FROM users WHERE id = $1`, userID).Scan(&username, &disableTime)
	if err != nil {
		http.Error(w, "user not found", http.StatusUnauthorized)
		return
	}
	if disableTime.After(time.Unix(0, 0)) {
		s.sessReg.RevokeAllSessions(userID)
		http.Error(w, "account disabled", http.StatusUnauthorized)
		return
	}

	accessToken, newRefreshToken, err := s.tokenMgr.GenerateSessionWithVars(userID, username, req.Vars)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.sessReg.RegisterSession(userID, newRefreshToken, req.RefreshToken)
	resp := authResponse{AccessToken: accessToken, RefreshToken: newRefreshToken, UserID: userID, Username: username}
	s.invokeAfter("SessionRefresh", &resp, &req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
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
	if claims.ID != "" && s.sessReg != nil {
		if store := s.sessReg.Store(); store != nil {
			denied, err := store.IsAccessJTIBlacklisted(r.Context(), claims.ID)
			if err == nil && denied {
				return "", errors.New("token revoked")
			}
		}
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
	if claims.ID != "" && s.sessReg != nil {
		if store := s.sessReg.Store(); store != nil {
			denied, err := store.IsAccessJTIBlacklisted(r.Context(), claims.ID)
			if err == nil && denied {
				return "", "", errors.New("token revoked")
			}
		}
	}
	return claims.UserID, claims.Username, nil
}

// SessionRegistry exposes the session registry for console ban revocation.
func (s *Server) SessionRegistry() *auth.SessionRegistry {
	return s.sessReg
}

func (s *Server) handleWriteStorageObjects(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		Objects []struct {
			Collection      string          `json:"collection"`
			Key             string          `json:"key"`
			Value           json.RawMessage `json:"value"`
			Version         string          `json:"version"`
			PermissionRead  *int16          `json:"permission_read"`
			PermissionWrite *int16          `json:"permission_write"`
		} `json:"objects"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	objs := make([]*storage.StorageObject, 0, len(req.Objects))
	for _, obj := range req.Objects {
		if obj.Collection == "" || obj.Key == "" {
			http.Error(w, "collection and key required", http.StatusBadRequest)
			return
		}
		trim := bytes.TrimSpace(obj.Value)
		if len(trim) == 0 || trim[0] != '{' || !json.Valid(trim) {
			http.Error(w, "value must be a JSON-encoded object", http.StatusBadRequest)
			return
		}
		read, write := int16(1), int16(1)
		if obj.PermissionRead != nil {
			read = *obj.PermissionRead
		}
		if obj.PermissionWrite != nil {
			write = *obj.PermissionWrite
		}
		if read < 0 || read > 2 || write < 0 || write > 1 {
			http.Error(w, "invalid permission", http.StatusBadRequest)
			return
		}
		objs = append(objs, &storage.StorageObject{
			Collection: obj.Collection,
			Key:        obj.Key,
			UserID:     userID,
			Value:      string(trim),
			Version:    obj.Version,
			Read:       read,
			Write:      write,
		})
	}

	acks, err := storage.WriteStorageObjects(r.Context(), s.dbPool, false, objs)
	if err != nil {
		if errors.Is(err, storage.ErrStorageRejectedVersion) || errors.Is(err, storage.ErrStorageRejectedPermission) {
			http.Error(w, "Storage write rejected.", http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	out := make([]map[string]interface{}, len(acks))
	for i, a := range acks {
		out[i] = map[string]interface{}{
			"collection":  a.Collection,
			"key":         a.Key,
			"user_id":     a.UserID,
			"version":     a.Version,
			"create_time": a.CreateTime.UTC().Format(time.RFC3339Nano),
			"update_time": a.UpdateTime.UTC().Format(time.RFC3339Nano),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"acks": out})
}

func (s *Server) handleReadStorageObjects(w http.ResponseWriter, r *http.Request) {
	callerStr, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	caller, err := uuid.Parse(callerStr)
	if err != nil {
		http.Error(w, "invalid user", http.StatusUnauthorized)
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

	reqs := make([]storage.ReadRequest, 0, len(req.ObjectIDs))
	for _, obj := range req.ObjectIDs {
		if obj.Collection == "" || obj.Key == "" {
			http.Error(w, "collection and key required", http.StatusBadRequest)
			return
		}
		if obj.UserID != "" {
			parsed, err := uuid.Parse(obj.UserID)
			if err != nil || parsed == uuid.Nil {
				http.Error(w, "invalid user_id", http.StatusBadRequest)
				return
			}
		}
		reqs = append(reqs, storage.ReadRequest{Collection: obj.Collection, Key: obj.Key, UserID: obj.UserID})
	}

	objs, err := storage.ReadStorageObjects(r.Context(), s.dbPool, caller, reqs)
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
			"create_time":      o.CreateTime.UTC().Format(time.RFC3339Nano),
			"update_time":      o.UpdateTime.UTC().Format(time.RFC3339Nano),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"objects": res})
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

	err = storage.DeleteStorageObjects(r.Context(), s.dbPool, false, reqs)
	if err != nil {
		if errors.Is(err, storage.ErrStorageRejectedVersion) || errors.Is(err, storage.ErrStorageRejectedPermission) {
			http.Error(w, "Storage write rejected.", http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListStorageObjects(w http.ResponseWriter, r *http.Request) {
	callerStr, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	caller, err := uuid.Parse(callerStr)
	if err != nil {
		http.Error(w, "invalid user", http.StatusUnauthorized)
		return
	}
	collection := r.PathValue("collection")
	userID := r.PathValue("user_id")
	if userID == "" {
		userID = r.URL.Query().Get("user_id")
	}
	limitStr := r.URL.Query().Get("limit")
	cursor := r.URL.Query().Get("cursor")

	limit := 1
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	var owner *uuid.UUID
	if userID != "" {
		parsed, err := uuid.Parse(userID)
		if err != nil {
			http.Error(w, "invalid user_id", http.StatusBadRequest)
			return
		}
		owner = &parsed
	}

	list, err := storage.ListStorageObjects(r.Context(), s.dbPool, caller, owner, collection, limit, cursor)
	if err != nil {
		if errors.Is(err, storage.ErrListCursorInvalid) {
			http.Error(w, "cursor is invalid", http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := make([]map[string]interface{}, len(list.Objects))
	for i, o := range list.Objects {
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
			"create_time":      o.CreateTime.UTC().Format(time.RFC3339Nano),
			"update_time":      o.UpdateTime.UTC().Format(time.RFC3339Nano),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"objects":     res,
		"next_cursor": list.Cursor,
		"cursor":      list.Cursor,
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
		SessionID:         userID,
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
		if errors.Is(err, matchmaker.ErrTooManyTickets) {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
			return
		}
		if errors.Is(err, matchmaker.ErrRateLimited) {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
			return
		}
		if errors.Is(err, matchmaker.ErrInvalidTicket) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
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

	stats := s.Matchmaker.GetStats(r.Context())
	oldest := ""
	if !stats.OldestTicketCreateTime.IsZero() {
		oldest = stats.OldestTicketCreateTime.Format(time.RFC3339)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ticket_count":              stats.TicketCount,
		"oldest_ticket_create_time": oldest,
		"completions":               stats.Completions,
	})
}
