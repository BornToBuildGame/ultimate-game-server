package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/api"
	"github.com/BornToBuildGame/ultimate-game-server/internal/console"
	"github.com/BornToBuildGame/ultimate-game-server/internal/database"
	"github.com/BornToBuildGame/ultimate-game-server/internal/economy"
	"github.com/BornToBuildGame/ultimate-game-server/internal/fleet"
	"github.com/BornToBuildGame/ultimate-game-server/internal/match"
	"github.com/BornToBuildGame/ultimate-game-server/internal/satori"
	"github.com/BornToBuildGame/ultimate-game-server/internal/storage"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/config"
	pkgruntime "github.com/BornToBuildGame/ultimate-game-server/pkg/runtime"

	"github.com/dop251/goja"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/yuin/gopher-lua"
	"go.uber.org/zap"
)

type zapRuntimeLogger struct{ z *zap.Logger }

func (l *zapRuntimeLogger) Debug(format string, args ...interface{}) {
	l.z.Sugar().Debugf(format, args...)
}
func (l *zapRuntimeLogger) Info(format string, args ...interface{}) {
	l.z.Sugar().Infof(format, args...)
}
func (l *zapRuntimeLogger) Warn(format string, args ...interface{}) {
	l.z.Sugar().Warnf(format, args...)
}
func (l *zapRuntimeLogger) Error(format string, args ...interface{}) {
	l.z.Sugar().Errorf(format, args...)
}

// Server is the programmatically embeddable engine instance for Ultimate Game Engine.
type Server struct {
	Logger         *zap.Logger
	Config         config.Config
	DBPool         *pgxpool.Pool
	SQLDB          *sql.DB
	RuntimeManager *pkgruntime.GoRuntimeManager
	RuntimeModule  pkgruntime.RuntimeModule
	CronScheduler  *pkgruntime.CronScheduler
	LuaVM          *lua.LState
	JSVM           *goja.Runtime

	apiServer     *api.Server
	consoleServer *console.Server
	modules       []Module
	initFuncs     []pkgruntime.InitModuleFunc
}

// NewServer initializes a new Ultimate Game Engine server instance from configuration.
func NewServer(cfg config.Config) (*Server, error) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	dbCfg := database.Config{
		DSN:             cfg.GetDatabase().DSN,
		ReadDSN:         cfg.GetDatabase().ReadDSN,
		MaxOpenConns:    int32(cfg.GetDatabase().MaxOpenConns),
		MaxIdleConns:    int32(cfg.GetDatabase().MaxIdleConns),
		MaxConnLifetime: cfg.GetDatabase().MaxConnLifetime,
		MaxConnIdleTime: cfg.GetDatabase().MaxConnIdleTime,
		MaxRetries:      5,
		RetryDelay:      1 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	dbPool, err := database.ConnectWithBackoff(ctx, logger, dbCfg)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("database connection failed: %w", err)
	}

	if cfg.GetDatabase().Migration {
		ctx = context.Background()
		if err := database.RunMigrations(ctx, logger, dbPool); err != nil {
			dbPool.Close()
			return nil, fmt.Errorf("database migrations failed: %w", err)
		}
	}

	iapCfg := economy.IAPConfig{
		AppleSharedPassword:           cfg.GetIAP().Apple.SharedPassword,
		AppleNotificationsEndpointID:  cfg.GetIAP().Apple.NotificationsEndpointID,
		GoogleClientEmail:             cfg.GetIAP().Google.ClientEmail,
		GooglePrivateKey:              cfg.GetIAP().Google.PrivateKey,
		GooglePackageName:             cfg.GetIAP().Google.PackageName,
		GoogleNotificationsEndpointID: cfg.GetIAP().Google.NotificationsEndpointID,
		HuaweiPublicKey:               cfg.GetIAP().Huawei.PublicKey,
		HuaweiClientID:                cfg.GetIAP().Huawei.ClientID,
		HuaweiClientSecret:            cfg.GetIAP().Huawei.ClientSecret,
		FacebookAppSecret:             cfg.GetIAP().FacebookInstant.AppSecret,
		SamsungPackageName:            cfg.GetIAP().Samsung.PackageName,
	}
	economy.DefaultIAPConfig = iapCfg

	multiInstCfg := api.MultiInstanceConfig{}
	if cfg.GetMultiInstance() != nil {
		multiInstCfg = api.MultiInstanceConfig{
			Enabled:   cfg.GetMultiInstance().Enabled,
			RedisAddr: cfg.GetMultiInstance().RedisAddr,
		}
	}

	serverCfg := api.Config{
		HTTPAddr:        cfg.GetSocket().HTTPAddr,
		GRPCAddr:        cfg.GetSocket().GRPCAddr,
		JWTSecret:       []byte(cfg.GetSession().EncryptionKey),
		JWTExpiry:       24 * time.Hour,
		RateLimitMax:    100,
		RateLimitRefill: 10,
		RPCHTTPKey:      cfg.GetRuntime().HTTPKey,
		RuntimePath:     cfg.GetRuntime().Path,
		IAP:             iapCfg,
		MultiInstance:   multiInstCfg,
	}

	apiSrv, err := api.NewServer(logger, serverCfg, dbPool)
	if err != nil {
		dbPool.Close()
		return nil, fmt.Errorf("failed to create api server: %w", err)
	}

	rtLogger := &zapRuntimeLogger{z: logger}
	var sqlDB *sql.DB
	if dbPool != nil {
		sqlDB = stdlib.OpenDBFromPool(dbPool)
	}
	nk := pkgruntime.NewGoRuntimeModule(dbPool, rtLogger)
	nk.SetStorageIndex(storage.NewBlugeStorageIndex())
	if os.Getenv("SATORI_URL") != "" {
		nk.SetSatoriClient(satori.NewClient(satori.Config{
			URL:        os.Getenv("SATORI_URL"),
			APIKeyName: os.Getenv("SATORI_API_KEY_NAME"),
			APIKey:     os.Getenv("SATORI_API_KEY"),
		}))
	}
	nk.SetFleetManager(fleet.NewLocalStub())
	if fm, ok := nk.GetFleetManager().(fleet.Initializer); ok {
		_ = fm.Init(nk, fleet.NewLocalFmCallbackHandler())
	}

	rm := pkgruntime.NewGoRuntimeManager(rtLogger, sqlDB, nk)
	apiSrv.SetRuntimeManager(rm)

	cronSched := pkgruntime.NewCronScheduler(rm.Registry(), rtLogger, sqlDB, nk)
	if apiSrv.Redis() != nil {
		cronSched.SetClusterLock(pkgruntime.NewCronClusterLock(apiSrv.Redis(), match.ResolveNodeID()))
	}

	return &Server{
		Logger:         logger,
		Config:         cfg,
		DBPool:         dbPool,
		SQLDB:          sqlDB,
		apiServer:      apiSrv,
		RuntimeManager: rm,
		RuntimeModule:  nk,
		CronScheduler:  cronSched,
		modules:        make([]Module, 0),
		initFuncs:      make([]pkgruntime.InitModuleFunc, 0),
	}, nil
}

// RegisterInit registers a native Go initialization function directly.
func (s *Server) RegisterInit(fn pkgruntime.InitModuleFunc) {
	if fn != nil {
		s.initFuncs = append(s.initFuncs, fn)
	}
}

// Use adds a pluggable game feature module to the server.
func (s *Server) Use(mod Module) {
	if mod != nil {
		s.modules = append(s.modules, mod)
	}
}

// Start launches the engine listeners, modules, and runtime schedulers.
func (s *Server) Start(ctx context.Context) error {
	rtLogger := &zapRuntimeLogger{z: s.Logger}

	// 1. Initialize registered modules
	for _, mod := range s.modules {
		s.Logger.Info("Initializing game module...", zap.String("module", mod.Name()))
		if err := mod.Init(ctx, s); err != nil {
			return fmt.Errorf("module %s init failed: %w", mod.Name(), err)
		}
	}

	// 2. Invoke registered Go init functions
	initCtx := context.Background()
	initializer := s.RuntimeManager.NewInitializer()
	for _, fn := range s.initFuncs {
		if err := fn(initCtx, rtLogger, s.SQLDB, s.RuntimeModule, initializer); err != nil {
			return fmt.Errorf("registered Go init module failed: %w", err)
		}
	}

	// 3. Load dynamic plugins if configured
	rtPath := s.Config.GetRuntime().Path
	if rtPath != "" {
		_ = s.RuntimeManager.LoadPlugins(ctx, rtPath)
	}

	// 4. Setup VM sandboxes (Lua/JS)
	luaVM := lua.NewState()
	jsVM := goja.New()
	pkgruntime.MapLuaNK(luaVM, s.RuntimeModule, s.RuntimeManager.Registry())
	pkgruntime.MapJSNK(jsVM, s.RuntimeModule, 5*time.Second, s.RuntimeManager.Registry())
	if rtPath != "" {
		_ = pkgruntime.LoadLuaModules(ctx, rtPath, luaVM, rtLogger)
		_ = pkgruntime.LoadJSModules(ctx, rtPath, jsVM, rtLogger)
	}
	s.apiServer.SetVMs(luaVM, jsVM)
	s.LuaVM = luaVM
	s.JSVM = jsVM

	// 5. Start Cron Scheduler
	s.CronScheduler.Start(ctx)

	// 6. Console Admin server if configured
	consoleListen := strings.TrimSpace(s.Config.GetConsole().Address)
	if consoleListen != "" {
		cs, err := console.NewServer(&consoleZapLogger{z: s.Logger}, s.DBPool, []byte(s.Config.GetSession().EncryptionKey))
		if err != nil {
			return fmt.Errorf("console init failed: %w", err)
		}
		cs.SetSessionRevoker(s.apiServer.SessionRegistry())
		matchAdapt := &matchConsoleAdapter{router: s.apiServer.MatchRouter}
		cs.SetMatchDeps(matchAdapt, matchAdapt)
		cs.SetStatusProvider(&statusConsoleAdapter{server: s.apiServer, match: matchAdapt})
		cs.SetRPCDispatcher(&rpcConsoleAdapter{
			rm: s.RuntimeManager, luaVM: luaVM, jsVM: jsVM,
		})
		cs.SetRuntimeRegistry(s.RuntimeManager.Registry())
		if err := cs.Start(consoleListen); err != nil {
			return fmt.Errorf("failed to start console admin: %w", err)
		}
		s.consoleServer = cs
	}

	// 7. Start API Server
	s.Logger.Info("Starting UGE API server...", zap.String("http", s.Config.GetSocket().HTTPAddr), zap.String("grpc", s.Config.GetSocket().GRPCAddr))
	return s.apiServer.Start(ctx)
}

// Stop gracefully shuts down all server listeners and components.
func (s *Server) Stop(ctx context.Context) error {
	rtLogger := &zapRuntimeLogger{z: s.Logger}
	s.RuntimeManager.Registry().InvokeShutdown(ctx, rtLogger, s.SQLDB, s.RuntimeModule)

	if s.consoleServer != nil {
		s.consoleServer.Close()
	}
	if s.CronScheduler != nil {
		s.CronScheduler.Stop()
	}
	if s.LuaVM != nil {
		s.LuaVM.Close()
	}
	if s.apiServer != nil {
		_ = s.apiServer.Stop(ctx)
	}
	if s.DBPool != nil {
		s.DBPool.Close()
	}
	return nil
}
