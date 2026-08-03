package auth

import (
	"context"

	"github.com/BornToBuildGame/ultimate-game-server/pkg/engine"
)

type AuthModule struct{}

func NewModule() *AuthModule {
	return &AuthModule{}
}

func (m *AuthModule) Name() string {
	return "auth"
}

func (m *AuthModule) Init(ctx context.Context, srv *engine.Server) error {
	srv.Logger.Info("Auth feature module initialized")
	return nil
}
