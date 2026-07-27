//go:build integration

package api

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestRedisIPRateLimiter_MultiNodeWindow(t *testing.T) {
	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	addr, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	opt, err := redis.ParseURL(addr)
	require.NoError(t, err)
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })

	lim := NewRedisIPRateLimiter(rdb, 2, 1, time.Minute)
	ok1, _, _ := lim.AllowWithInfo("10.0.0.1")
	ok2, _, _ := lim.AllowWithInfo("10.0.0.1")
	ok3, _, _ := lim.AllowWithInfo("10.0.0.1")
	require.True(t, ok1)
	require.True(t, ok2)
	require.False(t, ok3)
}
