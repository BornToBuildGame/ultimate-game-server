package presence

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

type mockSender struct {
	mu   sync.Mutex
	msgs map[string][][]byte
}

func (m *mockSender) SendToSession(sessionID string, payload []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.msgs == nil {
		m.msgs = make(map[string][][]byte)
	}
	cp := append([]byte(nil), payload...)
	m.msgs[sessionID] = append(m.msgs[sessionID], cp)
}

func (m *mockSender) waitMsg(sessionID string, timeout time.Duration) []byte {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		if len(m.msgs[sessionID]) > 0 {
			msg := m.msgs[sessionID][0]
			m.msgs[sessionID] = m.msgs[sessionID][1:]
			m.mu.Unlock()
			return msg
		}
		m.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

func TestStatusRegistry_FollowFanOut(t *testing.T) {
	sender := &mockSender{}
	router := NewLocalMessageRouter(sender)
	online := NewOnlineIndex()
	sr := NewStatusRegistry(router, online, 64)
	defer sr.Stop()

	sr.Follow("sess-b", []string{"user-a"})
	sr.QueueJoin("user-a", StatusPresence{
		UserID: "user-a", SessionID: "sess-a", Username: "alice", Status: "ready",
	})

	msg := sender.waitMsg("sess-b", time.Second)
	if msg == nil {
		t.Fatal("expected status_presence_event")
	}
	var evt struct {
		StatusPresenceEvent struct {
			Joins []StatusPresence `json:"joins"`
		} `json:"status_presence_event"`
	}
	if err := json.Unmarshal(msg, &evt); err != nil {
		t.Fatal(err)
	}
	if len(evt.StatusPresenceEvent.Joins) != 1 || evt.StatusPresenceEvent.Joins[0].Status != "ready" {
		t.Fatalf("unexpected: %+v", evt)
	}
	if !online.IsOnline("user-a") {
		t.Fatal("expected online index set")
	}

	sr.QueueLeave("user-a", StatusPresence{
		UserID: "user-a", SessionID: "sess-a", Username: "alice",
	})
	msg = sender.waitMsg("sess-b", time.Second)
	if msg == nil {
		t.Fatal("expected leave event")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && online.IsOnline("user-a") {
		time.Sleep(5 * time.Millisecond)
	}
	if online.IsOnline("user-a") {
		t.Fatal("expected offline after leave")
	}
}
