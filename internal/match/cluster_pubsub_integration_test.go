//go:build integration

package match

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"go.uber.org/zap"
)

func TestPubSubForwarder_TestcontainersRedis(t *testing.T) {
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

	router := NewRouter()
	logger := zap.NewNop()
	router.SetClusterConfig("node-B", rdb, NewPubSubForwarder(rdb, "node-A", logger))

	loop := NewMatchLoop("m-remote", nil, 10, logger, nil)
	router.Register("m-remote", loop)

	listenCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	router.StartClusterInputListener(listenCtx)
	time.Sleep(100 * time.Millisecond)

	forwarder := NewPubSubForwarder(rdb, "node-A", logger)
	err = forwarder(ctx, "node-B", "m-remote", MatchInput{
		UserID:  "u1",
		Action:  "42",
		Payload: "hello",
	})
	require.NoError(t, err)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case in := <-loop.inputBuffer:
			require.Equal(t, "u1", in.UserID)
			require.Equal(t, "hello", in.Payload)
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Fatal("timed out waiting for cluster input via Testcontainers Redis")
}
