package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"ultimate-game-server/internal/api"
	"ultimate-game-server/internal/config"
	"ultimate-game-server/internal/console"
	"ultimate-game-server/internal/database"
	"ultimate-game-server/internal/economy"
	"ultimate-game-server/internal/fleet"
	"ultimate-game-server/internal/match"
	"ultimate-game-server/internal/runtime"
	"ultimate-game-server/internal/satori"
	"ultimate-game-server/internal/storage"

	"github.com/dop251/goja"
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

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	logger.Info("Starting Ultimate Game Engine server bootstrap...")

	cfg, err := config.Parse(os.Args)
	if err != nil {
		logger.Fatal("Failed to parse configurations", zap.Error(err))
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

	runMigrations := cfg.GetDatabase().Migration

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	dbPool, err := database.ConnectWithBackoff(ctx, logger, dbCfg)
	cancel()
	if err != nil {
		logger.Fatal("Database connection failed", zap.Error(err))
	}
	defer dbPool.Close()

	logger.Info("Database connection established successfully.")

	ctx = context.Background()
	if runMigrations {
		err = database.RunMigrations(ctx, logger, dbPool)
		if err != nil {
			logger.Fatal("Database migrations failed", zap.Error(err))
		}
		logger.Info("Database migrations completed successfully.")
	} else {
		logger.Info("Database migrations skipped (database_migration=false)")
	}

	rpcKey := cfg.GetRuntime().HTTPKey
	rtPath := cfg.GetRuntime().Path

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

	serverCfg := api.Config{
		HTTPAddr:        cfg.GetSocket().HTTPAddr,
		GRPCAddr:        cfg.GetSocket().GRPCAddr,
		JWTSecret:       []byte(cfg.GetSession().EncryptionKey),
		JWTExpiry:       24 * time.Hour,
		RateLimitMax:    100,
		RateLimitRefill: 10,
		RPCHTTPKey:      rpcKey,
		RuntimePath:     rtPath,
		IAP:             iapCfg,
	}

	server, err := api.NewServer(logger, serverCfg, dbPool)
	if err != nil {
		logger.Fatal("Failed to initialize server instance", zap.Error(err))
	}

	// Runtime: Go plugins + Lua/JS VMs
	rtLogger := &zapRuntimeLogger{z: logger}
	var sqlDB *sql.DB
	if dbPool != nil {
		sqlDB = stdlib.OpenDBFromPool(dbPool)
	}
	nk := runtime.NewGoRuntimeModule(dbPool, rtLogger)
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
	rm := runtime.NewGoRuntimeManager(rtLogger, sqlDB, nk)
	if err := rm.LoadPlugins(ctx, rtPath); err != nil {
		logger.Warn("LoadPlugins completed with errors", zap.Error(err))
	}
	server.SetRuntimeManager(rm)

	luaVM := lua.NewState()
	jsVM := goja.New()
	runtime.MapLuaNK(luaVM, nk, rm.Registry())
	runtime.MapJSNK(jsVM, nk, 5*time.Second, rm.Registry())
	if err := runtime.LoadLuaModules(ctx, rtPath, luaVM, rtLogger); err != nil {
		logger.Warn("LoadLuaModules completed with errors", zap.Error(err))
	}
	if err := runtime.LoadJSModules(ctx, rtPath, jsVM, rtLogger); err != nil {
		logger.Warn("LoadJSModules completed with errors", zap.Error(err))
	}
	server.SetVMs(luaVM, jsVM)

	cronSched := runtime.NewCronScheduler(rm.Registry(), rtLogger, sqlDB, nk)
	if server.Redis() != nil {
		cronSched.SetClusterLock(runtime.NewCronClusterLock(server.Redis(), match.ResolveNodeID()))
	}
	cronSched.Start(ctx)

	consoleListen := strings.TrimSpace(cfg.GetConsole().Address)
	var consoleServer *console.Server
	if consoleListen != "" {
		cs, err := console.NewServer(&consoleZapLogger{z: logger}, dbPool, []byte(cfg.GetSession().EncryptionKey))
		if err != nil {
			logger.Fatal("Failed to initialize console admin", zap.Error(err))
		}
		cs.SetSessionRevoker(server.SessionRegistry())
		matchAdapt := &matchConsoleAdapter{router: server.MatchRouter}
		cs.SetMatchDeps(matchAdapt, matchAdapt)
		cs.SetStatusProvider(&statusConsoleAdapter{server: server, match: matchAdapt})
		cs.SetRPCDispatcher(&rpcConsoleAdapter{
			rm: rm, luaVM: luaVM, jsVM: jsVM, cfg: runtime.DefaultRPCConfig(),
		})
		iapCfg := economy.DefaultIAPConfig
		cs.SetIAPNotificationDeps(console.IAPNotificationDeps{
			AppleEndpointID:  iapCfg.AppleNotificationsEndpointID,
			GoogleEndpointID: iapCfg.GoogleNotificationsEndpointID,
			Registry:         rm.Registry(),
			NK:               nk,
			Logger:           rtLogger,
		})
		if os.Getenv("SATORI_URL") != "" {
			cs.SetSatoriClient(satori.NewClient(satori.Config{
				URL:        os.Getenv("SATORI_URL"),
				APIKeyName: os.Getenv("SATORI_API_KEY_NAME"),
				APIKey:     os.Getenv("SATORI_API_KEY"),
			}))
		}
		cs.SetRuntimeRegistry(rm.Registry())
		if err := cs.Start(consoleListen); err != nil {
			logger.Fatal("Failed to start console admin", zap.Error(err), zap.String("addr", consoleListen))
		}
		consoleServer = cs
		logger.Info("Console admin started", zap.String("addr", consoleListen))
	} else {
		logger.Info("Console admin disabled (CONSOLE_ADDR empty)")
	}

	go func() {
		logger.Info("Booting API server instances...", zap.String("http", cfg.GetSocket().HTTPAddr), zap.String("grpc", cfg.GetSocket().GRPCAddr))
		if err := server.Start(ctx); err != nil {
			logger.Fatal("Server runtime crash", zap.Error(err))
		}
	}()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	sig := <-shutdownChan
	logger.Warn("Shutdown signal received, initiating graceful teardown...", zap.String("signal", sig.String()))

	teardownCtx, teardownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer teardownCancel()

	rm.Registry().InvokeShutdown(teardownCtx, rtLogger, sqlDB, nk)

	if consoleServer != nil {
		consoleServer.Close()
	}
	cronSched.Stop()
	luaVM.Close()
	if err := server.Stop(teardownCtx); err != nil {
		logger.Error("Failed to shutdown server cleanly", zap.Error(err))
	} else {
		logger.Info("Teardown completed cleanly. Goodbye.")
	}
}

func envIntOr(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envDurationOr(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func envBoolOr(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}
