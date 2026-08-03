package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/config"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/engine"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/auth"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/economy"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/leaderboard"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/matchmaker"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/social"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/storage"

	"go.uber.org/zap"
)

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

	// Initialize modular server engine
	srv, err := engine.NewServer(cfg)
	if err != nil {
		logger.Fatal("Failed to initialize server engine instance", zap.Error(err))
	}

	// Register standard feature modules
	srv.Use(auth.NewModule())
	srv.Use(storage.NewModule())
	srv.Use(matchmaker.NewModule())
	srv.Use(economy.NewModule())
	srv.Use(social.NewModule())
	srv.Use(leaderboard.NewModule())

	ctx := context.Background()
	go func() {
		if err := srv.Start(ctx); err != nil {
			logger.Fatal("Server runtime crash", zap.Error(err))
		}
	}()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	sig := <-shutdownChan
	logger.Warn("Shutdown signal received, initiating graceful teardown...", zap.String("signal", sig.String()))

	teardownCtx, teardownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer teardownCancel()

	if err := srv.Stop(teardownCtx); err != nil {
		logger.Error("Failed to shutdown server cleanly", zap.Error(err))
	} else {
		logger.Info("Teardown completed cleanly. Goodbye.")
	}
}
