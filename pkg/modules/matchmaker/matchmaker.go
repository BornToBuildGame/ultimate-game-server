package matchmaker

import (
	"context"

	"github.com/BornToBuildGame/ultimate-game-server/pkg/engine"
)

type MatchmakerModule struct{}

func NewModule() *MatchmakerModule {
	return &MatchmakerModule{}
}

func (m *MatchmakerModule) Name() string {
	return "matchmaker"
}

func (m *MatchmakerModule) Init(ctx context.Context, srv *engine.Server) error {
	srv.Logger.Info("Matchmaker feature module initialized")
	return nil
}
