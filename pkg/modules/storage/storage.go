package storage

import (
	"context"

	"github.com/BornToBuildGame/ultimate-game-server/pkg/engine"
)

type StorageModule struct{}

func NewModule() *StorageModule {
	return &StorageModule{}
}

func (m *StorageModule) Name() string {
	return "storage"
}

func (m *StorageModule) Init(ctx context.Context, srv *engine.Server) error {
	srv.Logger.Info("Storage feature module initialized")
	return nil
}
