package social

import (
	"context"

	"github.com/BornToBuildGame/ultimate-game-server/pkg/engine"
)

type SocialModule struct{}

func NewModule() *SocialModule {
	return &SocialModule{}
}

func (m *SocialModule) Name() string {
	return "social"
}

func (m *SocialModule) Init(ctx context.Context, srv *engine.Server) error {
	srv.Logger.Info("Social feature module initialized")
	return nil
}
