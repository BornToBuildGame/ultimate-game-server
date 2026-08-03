package engine_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/BornToBuildGame/ultimate-game-server/pkg/engine"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/auth"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/economy"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/leaderboard"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/matchmaker"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/social"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/storage"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/runtime"

	"github.com/stretchr/testify/assert"
)

func TestModularEngineStructure(t *testing.T) {
	// Verify modular feature packages instantiate correctly and implement engine.Module interface
	var _ engine.Module = auth.NewModule()
	var _ engine.Module = storage.NewModule()
	var _ engine.Module = matchmaker.NewModule()
	var _ engine.Module = economy.NewModule()
	var _ engine.Module = social.NewModule()
	var _ engine.Module = leaderboard.NewModule()

	authMod := auth.NewModule()
	assert.Equal(t, "auth", authMod.Name())

	storageMod := storage.NewModule()
	assert.Equal(t, "storage", storageMod.Name())

	matchMod := matchmaker.NewModule()
	assert.Equal(t, "matchmaker", matchMod.Name())

	econMod := economy.NewModule()
	assert.Equal(t, "economy", econMod.Name())

	socialMod := social.NewModule()
	assert.Equal(t, "social", socialMod.Name())

	leaderboardMod := leaderboard.NewModule()
	assert.Equal(t, "leaderboard", leaderboardMod.Name())
}

func TestRegisterInitSignature(t *testing.T) {
	initFn := func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, initializer runtime.Initializer) error {
		return nil
	}
	var fn runtime.InitModuleFunc = initFn
	assert.NotNil(t, fn)
}
