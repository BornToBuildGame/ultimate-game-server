package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ultimate-game-server/internal/api"
	"ultimate-game-server/internal/database"

	"go.uber.org/zap"
)

func main() {
	// Parse CLI Flags
	httpAddr := flag.String("http_addr", "0.0.0.0:7350", "HTTP server address")
	grpcAddr := flag.String("grpc_addr", "0.0.0.0:7349", "gRPC server address")
	dsn := flag.String("dsn", "", "Database DSN (overrides environment variable)")
	jwtSecret := flag.String("jwt_secret", "super_secret_signing_key_at_least_32_bytes_long_1234567", "JWT secret key")
	flag.Parse()

	// Initialize Logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	logger.Info("Starting Ultimate Game Engine server bootstrap...")

	// 1. Resolve DB DSN (environment variable takes priority if dsn flag is default/empty)
	dbDsn := *dsn
	if dbDsn == "" {
		dbDsn = os.Getenv("DATABASE_URL")
		if dbDsn == "" {
			dbDsn = database.DefaultConfig().DSN
		}
	}

	dbCfg := database.DefaultConfig()
	dbCfg.DSN = dbDsn

	// 2. Connect to Database with backoff
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	dbPool, err := database.ConnectWithBackoff(ctx, logger, dbCfg)
	cancel()
	if err != nil {
		logger.Fatal("Database connection failed", zap.Error(err))
	}
	defer dbPool.Close()

	logger.Info("Database connection established successfully.")

	// 3. Run Database Migrations
	ctx = context.Background()
	err = database.RunMigrations(ctx, logger, dbPool)
	if err != nil {
		logger.Fatal("Database migrations failed", zap.Error(err))
	}
	logger.Info("Database migrations completed successfully.")

	// 4. Initialize API Server
	serverCfg := api.Config{
		HTTPAddr:        *httpAddr,
		GRPCAddr:        *grpcAddr,
		JWTSecret:       []byte(*jwtSecret),
		JWTExpiry:       24 * time.Hour,
		RateLimitMax:    100,
		RateLimitRefill: 10,
	}

	server, err := api.NewServer(logger, serverCfg, dbPool)
	if err != nil {
		logger.Fatal("Failed to initialize server instance", zap.Error(err))
	}

	// 5. Start Server
	go func() {
		logger.Info("Booting API server instances...", zap.String("http", *httpAddr), zap.String("grpc", *grpcAddr))
		if err := server.Start(ctx); err != nil {
			logger.Fatal("Server runtime crash", zap.Error(err))
		}
	}()

	// 6. Graceful Shutdown Setup
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	sig := <-shutdownChan
	logger.Warn("Shutdown signal received, initiating graceful teardown...", zap.String("signal", sig.String()))

	teardownCtx, teardownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer teardownCancel()

	if err := server.Stop(teardownCtx); err != nil {
		logger.Error("Failed to shutdown server cleanly", zap.Error(err))
	} else {
		logger.Info("Teardown completed cleanly. Goodbye.")
	}
}
