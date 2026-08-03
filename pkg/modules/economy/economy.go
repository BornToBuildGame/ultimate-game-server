package economy

import (
	"context"

	"github.com/BornToBuildGame/ultimate-game-server/pkg/engine"
)

type EconomyModule struct{}

func NewModule() *EconomyModule {
	return &EconomyModule{}
}

func (m *EconomyModule) Name() string {
	return "economy"
}

func (m *EconomyModule) Init(ctx context.Context, srv *engine.Server) error {
	srv.Logger.Info("Economy feature module initialized")
	return nil
}
