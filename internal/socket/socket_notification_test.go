package socket

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/notification"
	"github.com/BornToBuildGame/ultimate-game-server/internal/presence"

	"github.com/google/uuid"
)

func TestBuildNotificationsEnvelope(t *testing.T) {
	n := &notification.Notification{
		ID: uuid.New().String(), Subject: "s", Content: "{}", Code: -2,
		SenderID: uuid.New().String(), CreateTime: time.Now().UTC(), Persistent: true,
	}
	b := buildNotificationsEnvelope([]*notification.Notification{n})
	if b == nil {
		t.Fatal("nil payload")
	}
	var env map[string]interface{}
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("json: %v", err)
	}
	arr, ok := env["notifications"].([]interface{})
	if !ok || len(arr) != 1 {
		t.Fatalf("envelope: %+v", env)
	}
}

func TestNotificationsStreamKey(t *testing.T) {
	uid := uuid.New().String()
	k := NotificationsStream(uid)
	if k.Mode != presence.StreamModeNotifications || k.Subject != uid {
		t.Fatalf("stream key: %+v", k)
	}
}

func TestTrackNotifications(t *testing.T) {
	st := presence.NewStreamTracker()
	reg := NewConnectionRegistry()
	gh := &GatewayHandler{StreamTracker: st, registry: reg}
	uid := uuid.New().String()
	sid := uuid.New().String()
	sess := &Session{ID: sid, UserID: uid}
	gh.trackNotifications(sess)
	sessions := st.Sessions(NotificationsStream(uid))
	if len(sessions) != 1 || sessions[0] != sid {
		t.Fatalf("expected tracked session, got %v", sessions)
	}
}
