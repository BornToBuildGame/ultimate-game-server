package leaderboard

import (
	"context"

	"github.com/BornToBuildGame/ultimate-game-server/pkg/engine"
)

type LeaderboardModule struct{}

func NewModule() *LeaderboardModule {
	return &LeaderboardModule{}
}

func (m *LeaderboardModule) Name() string {
	return "leaderboard"
}

func (m *LeaderboardModule) Init(ctx context.Context, srv *engine.Server) error {
	srv.Logger.Info("Leaderboard feature module initialized")
	return nil
}
