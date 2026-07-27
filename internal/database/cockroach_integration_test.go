//go:build integration

package database

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/cockroachdb"
)

func TestCockroachDB_WireCompatPing(t *testing.T) {
	ctx := context.Background()
	crdb, err := cockroachdb.Run(ctx, "cockroachdb/cockroach:latest-v24.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = crdb.Terminate(ctx) })

	dsn, err := crdb.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, pool.Ping(ctx))

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id UUID PRIMARY KEY,
			username TEXT NOT NULL UNIQUE,
			display_name TEXT,
			metadata JSONB NOT NULL DEFAULT '{}',
			wallet JSONB NOT NULL DEFAULT '{}',
			create_time TIMESTAMPTZ DEFAULT now(),
			update_time TIMESTAMPTZ DEFAULT now()
		)`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS storage (
			collection VARCHAR(128) NOT NULL,
			key VARCHAR(128) NOT NULL,
			user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			value JSONB DEFAULT '{}'::jsonb NOT NULL,
			version VARCHAR(32) NOT NULL,
			read SMALLINT DEFAULT 1 NOT NULL,
			write SMALLINT DEFAULT 1 NOT NULL,
			create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
			update_time TIMESTAMPTZ DEFAULT now() NOT NULL,
			PRIMARY KEY (collection, key, user_id)
		)`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS wallet_ledger (
			id UUID UNIQUE NOT NULL,
			user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			changeset JSONB NOT NULL,
			metadata JSONB DEFAULT '{}'::jsonb NOT NULL,
			create_time TIMESTAMPTZ DEFAULT now() NOT NULL,
			update_time TIMESTAMPTZ DEFAULT now() NOT NULL,
			PRIMARY KEY (user_id, create_time, id)
		)`)
	require.NoError(t, err)

	userID := uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO users (id, username, display_name, wallet)
		VALUES ($1, $2, $3, $4::jsonb)`,
		userID, "crdb_user", "CRDB", `{"gold":100}`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO storage (collection, key, user_id, value, version)
		VALUES ('inv', 'sword', $1, '{"dmg":10}'::jsonb, 'v1')`, userID)
	require.NoError(t, err)

	ledgerID := uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO wallet_ledger (id, user_id, changeset, metadata)
		VALUES ($1, $2, '{"gold":10}'::jsonb, '{}'::jsonb)`, ledgerID, userID)
	require.NoError(t, err)

	var gold int64
	err = pool.QueryRow(ctx, `SELECT (wallet->>'gold')::bigint FROM users WHERE id = $1`, userID).Scan(&gold)
	require.NoError(t, err)
	require.Equal(t, int64(100), gold)

	var ver string
	err = pool.QueryRow(ctx, `SELECT version FROM storage WHERE user_id = $1 AND collection = 'inv' AND key = 'sword'`, userID).Scan(&ver)
	require.NoError(t, err)
	require.Equal(t, "v1", ver)

	var n int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger WHERE user_id = $1`, userID).Scan(&n)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}
