//go:build integration

package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"go.uber.org/zap"
)

func TestMesh_ChatCrossNodeFanout(t *testing.T) {
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

	var got ChatMessage
	var wg sync.WaitGroup
	wg.Add(1)

	meshB := NewMesh(rdb, "node-B", zap.NewNop())
	meshB.SetChatHandler(func(msg ChatMessage) {
		got = msg
		wg.Done()
	})
	meshB.Start(ctx)
	t.Cleanup(meshB.Stop)

	meshA := NewMesh(rdb, "node-A", zap.NewNop())
	meshA.Start(ctx)
	t.Cleanup(meshA.Stop)

	time.Sleep(100 * time.Millisecond) // allow subscribe

	err = meshA.PublishChat(ctx, ChatMessage{
		ChannelID: "room-1",
		Payload:   []byte(`{"channel_message":{"channel_id":"room-1","content":"hi"}}`),
	})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cross-node chat")
	}

	require.Equal(t, "room-1", got.ChannelID)
	require.Equal(t, "node-A", got.SourceNode)
	require.Contains(t, string(got.Payload), "hi")
}

func TestMesh_PresenceCrossNodeFanout(t *testing.T) {
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

	var got PresenceEventMessage
	var wg sync.WaitGroup
	wg.Add(1)

	meshB := NewMesh(rdb, "node-B", zap.NewNop())
	meshB.SetPresenceHandler(func(msg PresenceEventMessage) {
		got = msg
		wg.Done()
	})
	meshB.Start(ctx)
	t.Cleanup(meshB.Stop)

	meshA := NewMesh(rdb, "node-A", zap.NewNop())
	meshA.Start(ctx)
	t.Cleanup(meshA.Stop)
	time.Sleep(100 * time.Millisecond)

	err = meshA.PublishPresence(ctx, PresenceEventMessage{
		Kind:    "update",
		Payload: []byte(`{"user_id":"u1","presence":{"user_id":"u1","status":"online"}}`),
	})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cross-node presence")
	}
	require.Equal(t, "update", got.Kind)
	require.Equal(t, "node-A", got.SourceNode)
}
