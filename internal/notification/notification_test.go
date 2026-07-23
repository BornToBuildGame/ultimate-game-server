package notification

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type mockDeliverer struct {
	mu    sync.Mutex
	byUser map[string][]*Notification
	all    []*Notification
}

func (m *mockDeliverer) SendNotifications(userID string, notifs []*Notification) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byUser[userID] = append(m.byUser[userID], notifs...)
}

func (m *mockDeliverer) SendNotificationsToAll(notifs []*Notification) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.all = append(m.all, notifs...)
}

func TestValidateRuntimeCode(t *testing.T) {
	if err := ValidateRuntimeCode(1); err != nil {
		t.Fatalf("positive code: %v", err)
	}
	if err := ValidateRuntimeCode(-1001); err != nil {
		t.Fatalf("custom negative: %v", err)
	}
	if err := ValidateRuntimeCode(-2); err == nil {
		t.Fatal("reserved system code should be rejected")
	}
	if err := ValidateRuntimeCode(0); err == nil {
		t.Fatal("zero should be rejected")
	}
}

func TestCursorRoundTrip(t *testing.T) {
	in := &notificationCacheableCursor{
		NotificationID: uuid.New().String(),
		CreateTime:     time.Now().UTC().UnixNano(),
	}
	enc, err := encodeCursor(in)
	if err != nil || enc == "" {
		t.Fatalf("encode: %v %q", err, enc)
	}
	out, err := decodeCursor(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.NotificationID != in.NotificationID || out.CreateTime != in.CreateTime {
		t.Fatalf("mismatch: %+v vs %+v", out, in)
	}
	if _, err := decodeCursor("not-a-cursor"); !errors.Is(err, ErrNotificationCursorInvalid) {
		t.Fatalf("expected invalid cursor, got %v", err)
	}
}

func TestNotificationSend_NonPersistent_NoDB(t *testing.T) {
	d := &mockDeliverer{byUser: map[string][]*Notification{}}
	userID := uuid.New().String()
	n := &Notification{
		UserID: userID, Subject: "hi", Content: `{"a":1}`, Code: 42, Persistent: false,
	}
	if err := NotificationSend(context.Background(), nil, d, map[string][]*Notification{userID: {n}}); err != nil {
		t.Fatalf("send: %v", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.byUser[userID]) != 1 {
		t.Fatalf("expected delivery, got %d", len(d.byUser[userID]))
	}
	if d.byUser[userID][0].ID == "" {
		t.Fatal("expected id assigned")
	}
}

func TestNotificationSendAll_NonPersistent(t *testing.T) {
	d := &mockDeliverer{byUser: map[string][]*Notification{}}
	n := &Notification{Subject: "broadcast", Content: "{}", Code: 7, Persistent: false}
	if err := NotificationSendAll(context.Background(), nil, d, n); err != nil {
		t.Fatalf("send all: %v", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.all) != 1 {
		t.Fatalf("expected broadcast delivery, got %d", len(d.all))
	}
}

func TestReservedCodes(t *testing.T) {
	if CodeFriendImport != -1001 {
		t.Fatalf("FriendImport want -1001 got %d", CodeFriendImport)
	}
	if CodeFriendJoinGame != -6 {
		t.Fatalf("FriendJoinGame want -6 got %d", CodeFriendJoinGame)
	}
	if CodeFriendRemove != -9 {
		t.Fatalf("FriendRemove want -9 got %d", CodeFriendRemove)
	}
}
