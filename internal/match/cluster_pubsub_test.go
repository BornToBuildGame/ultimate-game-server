package match

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func TestPubSubForwarder_PublishAndListen(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	router := NewRouter()
	logger := zap.NewNop()
	router.SetClusterConfig("node-B", rdb, NewPubSubForwarder(rdb, "node-A", logger))

	loop := NewMatchLoop("m-remote", nil, 10, logger, nil)
	router.Register("m-remote", loop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	router.StartClusterInputListener(ctx)

	// Allow subscribe to propagate in miniredis
	time.Sleep(20 * time.Millisecond)

	forwarder := NewPubSubForwarder(rdb, "node-A", logger)
	err = forwarder(context.Background(), "node-B", "m-remote", MatchInput{
		UserID:  "u1",
		Action:  "42",
		Payload: "hello",
	})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case in := <-loop.inputBuffer:
			if in.UserID != "u1" || in.Payload != "hello" {
				t.Fatalf("unexpected input: %+v", in)
			}
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Fatal("timed out waiting for cluster input delivery")
}

func TestPubSubSignalForwarder_PublishAndListen(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	router := NewRouter()
	logger := zap.NewNop()
	router.SetClusterConfig("node-B", rdb, NewPubSubForwarder(rdb, "node-A", logger))
	router.ZapLogger = logger

	loop := NewMatchLoop("m-sig", nil, 10, logger, nil)
	router.Register("m-sig", loop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	router.StartClusterSignalListener(ctx)
	time.Sleep(20 * time.Millisecond)

	forwarder := NewPubSubSignalForwarder(rdb, "node-A", logger)
	if err := forwarder(context.Background(), "node-B", "m-sig", "ping-payload"); err != nil {
		t.Fatalf("signal forward: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case req := <-loop.signalRequests:
			if req.data != "ping-payload" {
				t.Fatalf("unexpected signal: %+v", req)
			}
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Fatal("timed out waiting for cluster signal delivery")
}

func TestPublishRelayFanout_RoundTrip(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got RelayFanoutMessage
	var mu sync.Mutex
	done := make(chan struct{})

	pubsub := rdb.Subscribe(ctx, RelayFanoutChannel)
	go func() {
		defer pubsub.Close()
		for msg := range pubsub.Channel() {
			var fanout RelayFanoutMessage
			if err := json.Unmarshal([]byte(msg.Payload), &fanout); err != nil {
				continue
			}
			mu.Lock()
			got = fanout
			mu.Unlock()
			close(done)
			return
		}
	}()
	time.Sleep(20 * time.Millisecond)

	payload := []byte(`{"match_data":{"match_id":"m1."}}`)
	if err := PublishRelayFanout(ctx, rdb, "node-A", "m1.", "match_data", payload); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	mu.Lock()
	defer mu.Unlock()
	if got.SourceNode != "node-A" || got.MatchID != "m1." || got.Kind != "match_data" {
		t.Fatalf("unexpected fanout: %+v", got)
	}
	if string(got.Payload) != string(payload) {
		t.Fatalf("payload mismatch: %s", got.Payload)
	}
}
