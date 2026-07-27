package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ultimate-game-server/internal/presence"

	lua "github.com/yuin/gopher-lua"
)

func TestEnvelopeHookIDFromJSON(t *testing.T) {
	cases := []struct {
		m    map[string]interface{}
		want string
	}{
		{map[string]interface{}{"match_join": map[string]interface{}{}}, "match_join"},
		{map[string]interface{}{"rpc": map[string]interface{}{"id": "x"}}, "rpc"},
		{map[string]interface{}{"status_update": map[string]interface{}{"status": "hi"}}, "status_update"},
		{map[string]interface{}{"matchmaker_add": map[string]interface{}{}}, "matchmaker_add"},
		{map[string]interface{}{}, ""},
	}
	for _, tc := range cases {
		if got := EnvelopeHookIDFromJSON(tc.m); got != tc.want {
			t.Fatalf("EnvelopeHookIDFromJSON(%v)=%q want %q", tc.m, got, tc.want)
		}
	}
}

func TestRunBeforeRtRejectAndModify(t *testing.T) {
	reg := NewHookRegistry()
	reg.RegisterBefore("status_update", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		return nil, errors.New("blocked")
	})
	ex := NewRtHookExecutor(reg, &testLogger{t: t}, nil, nil, nil, nil, nil, nil)
	_, err := ex.RunBeforeRt(context.Background(), "status_update", map[string]interface{}{
		"status_update": map[string]interface{}{"status": "x"},
	})
	if err == nil || err.Error() != "blocked" {
		t.Fatalf("expected blocked, got %v", err)
	}

	reg2 := NewHookRegistry()
	reg2.RegisterBefore("match_join", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		m := in.(map[string]interface{})
		payload := m["match_join"].(map[string]interface{})
		payload["match_id"] = "rewritten"
		return m, nil
	})
	ex2 := NewRtHookExecutor(reg2, &testLogger{t: t}, nil, nil, nil, nil, nil, nil)
	out, err := ex2.RunBeforeRt(context.Background(), "match_join", map[string]interface{}{
		"match_join": map[string]interface{}{"match_id": "orig"},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := out["match_join"].(map[string]interface{})["match_id"]
	if got != "rewritten" {
		t.Fatalf("match_id=%v", got)
	}

	// rpc before hooks run
	reg3 := NewHookRegistry()
	reg3.RegisterBefore("rpc", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		return nil, errors.New("should not run")
	})
	ex3 := NewRtHookExecutor(reg3, &testLogger{t: t}, nil, nil, nil, nil, nil, nil)
	env := map[string]interface{}{"rpc": map[string]interface{}{"id": "x"}}
	_, err = ex3.RunBeforeRt(context.Background(), "rpc", env)
	if err == nil {
		t.Fatal("expected rpc before hook error")
	}
}

func TestLuaRegisterRtBefore(t *testing.T) {
	reg := NewHookRegistry()
	L := lua.NewState()
	defer L.Close()
	MapLuaNK(L, &mockRuntimeModule{}, reg)
	if err := L.DoString(`
		nk.register_rt_before(function(ctx, envelope)
			envelope.status_update = { status = "from_lua" }
			return envelope
		end, "status_update")
	`); err != nil {
		t.Fatal(err)
	}
	_, rt, fn, ok := reg.GetBeforeHook("status_update")
	if !ok || rt != "lua" || fn == "" {
		t.Fatalf("lua before not registered: ok=%v rt=%s fn=%s", ok, rt, fn)
	}
	ex := NewRtHookExecutor(reg, &testLogger{t: t}, nil, &mockRuntimeModule{}, L, nil, &sync.Mutex{}, nil)
	out, err := ex.RunBeforeRt(context.Background(), "status_update", map[string]interface{}{
		"status_update": map[string]interface{}{"status": "orig"},
	})
	if err != nil {
		t.Fatal(err)
	}
	su, _ := out["status_update"].(map[string]interface{})
	if su == nil {
		t.Fatalf("missing status_update in %#v", out)
	}
	if fmt.Sprint(su["status"]) != "from_lua" {
		t.Fatalf("status=%v", su["status"])
	}
}

func TestDispatchSessionEvents(t *testing.T) {
	reg := NewHookRegistry()
	var sawStart, sawEnd atomic.Bool
	done := make(chan struct{}, 2)
	reg.RegisterEvent(func(ctx context.Context, logger Logger, evt *Event) {
		switch evt.Name {
		case "session_start":
			sawStart.Store(true)
			done <- struct{}{}
		case "session_end":
			sawEnd.Store(true)
			done <- struct{}{}
		}
	})
	reg.DispatchEvent(context.Background(), &testLogger{t: t}, &Event{
		Name: "session_start",
		Properties: map[string]string{
			"user_id": "u1", "session_id": "s1", "username": "alice",
		},
		Timestamp: time.Now().Unix(),
	})
	reg.DispatchEvent(context.Background(), &testLogger{t: t}, &Event{
		Name:       "session_end",
		Properties: map[string]string{"user_id": "u1", "session_id": "s1"},
		Timestamp:  time.Now().Unix(),
	})
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for session events")
		}
	}
	if !sawStart.Load() || !sawEnd.Load() {
		t.Fatalf("start=%v end=%v", sawStart.Load(), sawEnd.Load())
	}
}

type memSender struct {
	mu   sync.Mutex
	msgs map[string][][]byte
}

func (m *memSender) SendToSession(sessionID string, payload []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.msgs == nil {
		m.msgs = make(map[string][][]byte)
	}
	cp := append([]byte(nil), payload...)
	m.msgs[sessionID] = append(m.msgs[sessionID], cp)
}

type memDisconnecter struct {
	closed string
}

func (d *memDisconnecter) DisconnectSession(sessionID string) error {
	d.closed = sessionID
	return nil
}

func TestStreamUserJoinSendDisconnect(t *testing.T) {
	tracker := presence.NewLocalTracker()
	sender := &memSender{}
	router := presence.NewLocalMessageRouter(sender)
	disc := &memDisconnecter{}
	sm := &LocalStreamManager{Tracker: tracker, Router: router, Registry: disc}

	ok, err := sm.StreamUserJoin(1, "subj", "", "", "user-1", "sess-1", false, false, "online")
	if err != nil || !ok {
		t.Fatalf("join: ok=%v err=%v", ok, err)
	}
	n, err := sm.StreamCount(1, "subj", "", "")
	if err != nil || n != 1 {
		t.Fatalf("count=%d err=%v", n, err)
	}
	list, err := sm.StreamUserList(1, "subj", "", "", true, true)
	if err != nil || len(list) != 1 || list[0].SessionID != "sess-1" {
		t.Fatalf("list=%v err=%v", list, err)
	}
	if err := sm.StreamSend(1, "subj", "", "", `{"hello":1}`, nil, true); err != nil {
		t.Fatal(err)
	}
	sender.mu.Lock()
	got := sender.msgs["sess-1"]
	sender.mu.Unlock()
	if len(got) != 1 || string(got[0]) != `{"hello":1}` {
		t.Fatalf("msgs=%v", got)
	}
	if err := sm.SessionDisconnect("sess-1"); err != nil {
		t.Fatal(err)
	}
	if disc.closed != "sess-1" {
		t.Fatalf("closed=%q", disc.closed)
	}
}

func TestNotificationHookIDsPascalCase(t *testing.T) {
	reg := NewHookRegistry()
	reg.RegisterBefore("ListNotifications", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		return in, nil
	})
	reg.RegisterBefore("DeleteNotifications", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		return in, nil
	})
	if _, ok := reg.GetBefore("ListNotifications"); !ok {
		t.Fatal("ListNotifications missing")
	}
	if _, ok := reg.GetBefore("DeleteNotifications"); !ok {
		t.Fatal("DeleteNotifications missing")
	}
	if _, ok := reg.GetBefore("listnotifications"); ok {
		t.Fatal("lowercase listnotifications should not be registered")
	}
}

func TestLuaRegisterReqBefore(t *testing.T) {
	reg := NewHookRegistry()
	L := lua.NewState()
	defer L.Close()
	MapLuaNK(L, &mockRuntimeModule{}, reg)
	if err := L.DoString(`
		nk.register_req_before(function(ctx, req) return req end, "AuthenticateEmail")
		nk.register_matchmaker_matched(function(entries) return "match-1" end)
	`); err != nil {
		t.Fatal(err)
	}
	_, rt, _, ok := reg.GetBeforeHook("AuthenticateEmail")
	if !ok || rt != "lua" {
		t.Fatalf("req before: ok=%v rt=%s", ok, rt)
	}
	fn := reg.GetMatchmakerMatched()
	if fn == nil {
		t.Fatal("matchmaker matched not registered")
	}
}
