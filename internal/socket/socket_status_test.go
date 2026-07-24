package socket

import (
	"encoding/json"
	"testing"
	"time"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/presence"

	"go.uber.org/zap"
)

func TestGatewayHandler_StatusFollowAndUpdate(t *testing.T) {
	logger := zap.NewNop()
	secret := []byte("super_secret_signing_key_at_least_32_bytes_long_1234567")
	tm, _ := auth.NewTokenManager(secret, 10*time.Minute)

	reg := NewConnectionRegistry()
	gh := NewGatewayHandler(logger, tm, reg, nil, nil, nil)

	online := presence.NewOnlineIndex()
	tracker := presence.NewLocalTracker()
	router := presence.NewLocalMessageRouter(reg)
	sr := presence.NewStatusRegistry(router, online, 64)
	defer sr.Stop()

	gh.SetPresenceTracker(online)
	gh.SetStreamTracker(tracker)
	gh.SetStatusRegistry(sr)
	gh.SetMessageRouter(router)
	gh.SetPresenceLimits(2048, 1000)

	target := &Session{
		ID: "sess-target", UserID: "user-target", Username: "target",
		Send: make(chan []byte, 10), IsActive: true, matchIDs: make(map[string]bool),
		TrackStatus: true,
	}
	follower := &Session{
		ID: "sess-follower", UserID: "user-follower", Username: "follower",
		Send: make(chan []byte, 10), IsActive: true, matchIDs: make(map[string]bool),
	}
	reg.Add(target)
	reg.Add(follower)

	gh.TrackStatusOnConnect(target)

	gh.RouteMessage(follower, []byte(`{"cid":"1","status_follow":{"user_ids":["user-target"]}}`))
	select {
	case msg := <-follower.Send:
		var resp struct {
			Cid    string `json:"cid"`
			Status struct {
				Presences []struct {
					UserID string `json:"user_id"`
				} `json:"presences"`
			} `json:"status"`
		}
		if err := json.Unmarshal(msg, &resp); err != nil {
			t.Fatalf("unmarshal status: %v", err)
		}
		if resp.Cid != "1" || len(resp.Status.Presences) != 1 || resp.Status.Presences[0].UserID != "user-target" {
			t.Fatalf("unexpected status snapshot: %s", string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("expected status follow snapshot")
	}

	gh.RouteMessage(target, []byte(`{"cid":"2","status_update":{"status":"{\"state\":\"ready\"}"}}`))
	select {
	case <-target.Send:
	case <-time.After(time.Second):
		t.Fatal("expected status_update ack")
	}
	select {
	case msg := <-follower.Send:
		var evt struct {
			StatusPresenceEvent struct {
				Joins []struct {
					UserID string `json:"user_id"`
					Status string `json:"status"`
				} `json:"joins"`
			} `json:"status_presence_event"`
		}
		if err := json.Unmarshal(msg, &evt); err != nil {
			t.Fatalf("unmarshal presence event: %v", err)
		}
		if len(evt.StatusPresenceEvent.Joins) != 1 || evt.StatusPresenceEvent.Joins[0].UserID != "user-target" {
			t.Fatalf("unexpected presence event: %s", string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("expected status_presence_event on follower")
	}

	// Empty string keeps online; null goes offline.
	gh.RouteMessage(target, []byte(`{"cid":"2b","status_update":{"status":""}}`))
	select {
	case <-target.Send:
	case <-time.After(time.Second):
		t.Fatal("expected empty status ack")
	}
	select {
	case <-follower.Send: // join with empty status
	case <-time.After(time.Second):
		t.Fatal("expected empty-status join event")
	}

	gh.RouteMessage(target, []byte(`{"cid":"2c","status_update":{"status":null}}`))
	select {
	case <-target.Send:
	case <-time.After(time.Second):
		t.Fatal("expected null status ack")
	}
	select {
	case msg := <-follower.Send:
		var evt struct {
			StatusPresenceEvent struct {
				Leaves []struct {
					UserID string `json:"user_id"`
				} `json:"leaves"`
			} `json:"status_presence_event"`
		}
		if err := json.Unmarshal(msg, &evt); err != nil {
			t.Fatalf("unmarshal leave: %v", err)
		}
		if len(evt.StatusPresenceEvent.Leaves) != 1 {
			t.Fatalf("expected leave, got %s", string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("expected leave on null status")
	}

	gh.RouteMessage(follower, []byte(`{"cid":"3","ping":{}}`))
	select {
	case msg := <-follower.Send:
		var resp struct {
			Cid  string                 `json:"cid"`
			Pong map[string]interface{} `json:"pong"`
		}
		if err := json.Unmarshal(msg, &resp); err != nil {
			t.Fatalf("unmarshal pong: %v", err)
		}
		if resp.Cid != "3" || resp.Pong == nil {
			t.Fatalf("expected pong with cid 3, got %s", string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("expected pong")
	}
}

func TestGatewayHandler_StatusUpdateTooLong(t *testing.T) {
	logger := zap.NewNop()
	secret := []byte("super_secret_signing_key_at_least_32_bytes_long_1234567")
	tm, _ := auth.NewTokenManager(secret, 10*time.Minute)
	reg := NewConnectionRegistry()
	gh := NewGatewayHandler(logger, tm, reg, nil, nil, nil)
	tracker := presence.NewLocalTracker()
	online := presence.NewOnlineIndex()
	router := presence.NewLocalMessageRouter(reg)
	sr := presence.NewStatusRegistry(router, online, 16)
	defer sr.Stop()
	gh.SetStreamTracker(tracker)
	gh.SetStatusRegistry(sr)
	gh.SetPresenceLimits(8, 1000)

	s := &Session{ID: "s1", UserID: "u1", Username: "u", Send: make(chan []byte, 4), IsActive: true, matchIDs: map[string]bool{}}
	reg.Add(s)
	gh.RouteMessage(s, []byte(`{"cid":"1","status_update":{"status":"abcdefghijk"}}`))
	select {
	case msg := <-s.Send:
		var resp map[string]interface{}
		_ = json.Unmarshal(msg, &resp)
		if resp["error"] == nil {
			t.Fatalf("expected error for long status: %s", string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("expected error response")
	}
}
