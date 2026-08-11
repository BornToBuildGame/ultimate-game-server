package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/BornToBuildGame/ultimate-game-server/pkg/config"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/engine"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/auth"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/runtime"
)

// InitModule registers typed after-auth hooks using only public pkg/runtime types.
func InitModule(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, initializer runtime.Initializer) error {
	err := initializer.RegisterAfterAuthenticateDevice(func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, out *runtime.Session, in *runtime.AuthenticateDeviceRequest) error {
		userID, _ := ctx.Value(runtime.CtxUserID).(string)
		logger.Info("device auth after-hook user=%s device=%s session_user=%s", userID, in.ID, out.UserID)
		return nil
	})
	if err != nil {
		return fmt.Errorf("register after authenticate device: %w", err)
	}

	err = initializer.RegisterBeforeAuthenticateEmail(func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, in *runtime.AuthenticateEmailRequest) (*runtime.AuthenticateEmailRequest, error) {
		if in.Email == "" {
			return nil, runtime.ErrBadRequest("email cannot be empty")
		}
		return in, nil
	})
	if err != nil {
		return fmt.Errorf("register before authenticate email: %w", err)
	}
	return nil
}

func main() {
	// Compile-time / smoke: public config + engine + typed InitModule surface.
	var _ runtime.InitModuleFunc = InitModule
	cfg := config.NewConfig()
	if cfg == nil {
		log.Fatal("config.NewConfig returned nil")
	}

	// NewServer is nameable from an external module (parameter type is pkg/config.Config).
	var newServer = engine.NewServer
	_ = newServer
	_ = auth.NewModule()

	log.Println("embed-external smoke: public pkg surface compiles")
}
