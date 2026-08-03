package economy

import (
	"context"
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/auth"
	"github.com/BornToBuildGame/ultimate-game-server/internal/storage"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)


func TestMultiUpdate_AccountStorageWallet(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	pg, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("pass"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Terminate(ctx) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	var pingErr error
	for i := 0; i < 30; i++ {
		pingErr = pool.Ping(ctx)
		if pingErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, pingErr)


	_, err = pool.Exec(ctx, `
		CREATE TABLE users (
			id UUID PRIMARY KEY,
			username TEXT NOT NULL UNIQUE,
			display_name TEXT,
			avatar_url TEXT,
			lang_tag TEXT,
			location TEXT,
			timezone TEXT,
			metadata JSONB DEFAULT '{}',
			wallet JSONB DEFAULT '{}',
			create_time TIMESTAMPTZ DEFAULT now(),
			update_time TIMESTAMPTZ DEFAULT now()
		);
		CREATE TABLE storage (
			collection TEXT NOT NULL,
			key TEXT NOT NULL,
			user_id UUID NOT NULL,
			value JSONB NOT NULL,
			version TEXT NOT NULL,
			read SMALLINT NOT NULL DEFAULT 1,
			write SMALLINT NOT NULL DEFAULT 1,
			create_time TIMESTAMPTZ DEFAULT now(),
			update_time TIMESTAMPTZ DEFAULT now(),
			PRIMARY KEY (collection, key, user_id)
		);
	`)
	require.NoError(t, err)

	userID := uuid.New().String()
	_, err = pool.Exec(ctx, `INSERT INTO users (id, username, wallet) VALUES ($1, 'player1', '{"coins":100}')`, userID)
	require.NoError(t, err)

	display := "Hero"
	acks, wallets, err := MultiUpdate(ctx, pool, nil, MultiUpdateParams{
		AccountUpdates: []auth.AccountUpdateParams{{UserID: userID, DisplayName: &display}},
		StorageWrites: []*storage.StorageObject{{
			Collection: "player", Key: "profile", UserID: userID,
			Value: `{"level":1}`, Version: "*", Read: 2, Write: 1,
		}},
		WalletUpdates: []WalletUpdate{{UserID: userID, Changeset: map[string]int64{"coins": -10}}},
		UpdateLedger:  false,
	})
	require.NoError(t, err)
	require.Len(t, acks, 1)
	require.Len(t, wallets, 1)
	require.Equal(t, int64(90), wallets[0].Updated["coins"])
}
