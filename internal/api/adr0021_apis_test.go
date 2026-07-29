package api

import (
	"context"
	"testing"
	"time"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/runtime"

	"google.golang.org/protobuf/types/known/emptypb"
)

type nopLogger struct{}

func (nopLogger) Debug(string, ...interface{}) {}
func (nopLogger) Info(string, ...interface{})  {}
func (nopLogger) Warn(string, ...interface{})  {}
func (nopLogger) Error(string, ...interface{}) {}

func TestSystemHealthcheckRPC(t *testing.T) {
	s := NewSystemServer()
	out, err := s.Healthcheck(context.Background(), &emptypb.Empty{})
	if err != nil || out == nil {
		t.Fatalf("err=%v out=%v", err, out)
	}
}

func TestEventServerDispatches(t *testing.T) {
	reg := runtime.NewHookRegistry()
	done := make(chan struct{}, 1)
	reg.RegisterEvent(func(ctx context.Context, logger runtime.Logger, evt *runtime.Event) {
		if evt.Name == "client_event" && evt.Properties["external"] == "true" {
			done <- struct{}{}
		}
	})
	es := NewEventServer(nil, reg, nopLogger{})
	es.dispatch("u1", "alice", "client_event", map[string]string{"k": "v"}, 0)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("event not dispatched")
	}
}

func TestDeleteFriendsRequestShape(t *testing.T) {
	req := &apipb.DeleteFriendsRequest{Ids: []string{"a"}, Usernames: []string{"b"}}
	if len(req.GetIds()) != 1 || len(req.GetUsernames()) != 1 {
		t.Fatal(req)
	}
}
