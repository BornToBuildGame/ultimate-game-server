package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/presence"

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

	const customMode int16 = 100 // outside domain modes 0–7
	ok, err := sm.StreamUserJoin(customMode, "subj", "", "", "user-1", "sess-1", false, false, "online")
	if err != nil || !ok {
		t.Fatalf("join: ok=%v err=%v", ok, err)
	}
	n, err := sm.StreamCount(customMode, "subj", "", "")
	if err != nil || n != 1 {
		t.Fatalf("count=%d err=%v", n, err)
	}
	list, err := sm.StreamUserList(customMode, "subj", "", "", true, true)
	if err != nil || len(list) != 1 || list[0].SessionID != "sess-1" {
		t.Fatalf("list=%v err=%v", list, err)
	}
	if err := sm.StreamSend(customMode, "subj", "", "", `{"hello":1}`, nil, true); err != nil {
		t.Fatal(err)
	}
	sender.mu.Lock()
	got := sender.msgs["sess-1"]
	sender.mu.Unlock()
	// join emits stream_presence_event; send emits stream_data
	if len(got) < 2 {
		t.Fatalf("msgs=%v", got)
	}
	var foundData bool
	for _, raw := range got {
		var env map[string]interface{}
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatal(err)
		}
		if sd, ok := env["stream_data"].(map[string]interface{}); ok {
			foundData = true
			if sd["data"] != `{"hello":1}` {
				t.Fatalf("data=%v", sd["data"])
			}
			streamMeta, _ := sd["stream"].(map[string]interface{})
			if streamMeta == nil || streamMeta["subject"] != "subj" {
				t.Fatalf("stream meta=%v", streamMeta)
			}
		}
	}
	if !foundData {
		t.Fatalf("missing stream_data in %v", got)
	}
	if err := sm.SessionDisconnect("sess-1"); err != nil {
		t.Fatal(err)
	}
	if disc.closed != "sess-1" {
		t.Fatalf("closed=%q", disc.closed)
	}
}

func TestStreamPresenceEventOnJoinLeave(t *testing.T) {
	tracker := presence.NewLocalTracker()
	sender := &memSender{}
	router := presence.NewLocalMessageRouter(sender)
	sm := &LocalStreamManager{Tracker: tracker, Router: router}

	const customMode int16 = 100
	ok, err := sm.StreamUserJoin(customMode, "lobby", "", "room-a", "user-1", "sess-1", false, false, "")
	if err != nil || !ok {
		t.Fatalf("join1: %v %v", ok, err)
	}
	ok, err = sm.StreamUserJoin(customMode, "lobby", "", "room-a", "user-2", "sess-2", false, false, "")
	if err != nil || !ok {
		t.Fatalf("join2: %v %v", ok, err)
	}

	sender.mu.Lock()
	peerMsgs := sender.msgs["sess-1"]
	sender.mu.Unlock()
	foundJoin := false
	for _, raw := range peerMsgs {
		var env map[string]interface{}
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatal(err)
		}
		spe, ok := env["stream_presence_event"].(map[string]interface{})
		if !ok {
			continue
		}
		joins, _ := spe["joins"].([]interface{})
		if len(joins) == 1 {
			j := joins[0].(map[string]interface{})
			if j["session_id"] == "sess-2" && j["user_id"] == "user-2" {
				foundJoin = true
			}
		}
	}
	if !foundJoin {
		t.Fatalf("peer did not receive join presence: %v", peerMsgs)
	}

	if err := sm.StreamUserLeave(customMode, "lobby", "", "room-a", "user-2", "sess-2"); err != nil {
		t.Fatal(err)
	}
	sender.mu.Lock()
	peerMsgs = sender.msgs["sess-1"]
	sender.mu.Unlock()
	foundLeave := false
	for _, raw := range peerMsgs {
		var env map[string]interface{}
		_ = json.Unmarshal(raw, &env)
		spe, ok := env["stream_presence_event"].(map[string]interface{})
		if !ok {
			continue
		}
		leaves, _ := spe["leaves"].([]interface{})
		if len(leaves) == 1 {
			l := leaves[0].(map[string]interface{})
			if l["session_id"] == "sess-2" {
				foundLeave = true
			}
		}
	}
	if !foundLeave {
		t.Fatalf("peer did not receive leave presence: %v", peerMsgs)
	}

	// Domain modes must not emit stream_presence_event.
	sender.msgs = nil
	_, _ = sm.StreamUserJoin(presence.StreamModeStatus, "user-3", "", "", "user-3", "sess-3", false, false, "")
	sender.mu.Lock()
	defer sender.mu.Unlock()
	for _, raws := range sender.msgs {
		for _, raw := range raws {
			var env map[string]interface{}
			_ = json.Unmarshal(raw, &env)
			if _, ok := env["stream_presence_event"]; ok {
				t.Fatal("status mode should not emit stream_presence_event")
			}
		}
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

func TestRunBeforeReqRpcFuncRejectAndModify(t *testing.T) {
	reg := NewHookRegistry()
	reg.RegisterBefore(RpcFuncHookID, func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		return nil, errors.New("rpc blocked")
	})
	ex := NewRtHookExecutor(reg, &testLogger{t: t}, nil, nil, nil, nil, nil, nil)
	_, err := ex.RunBeforeReq(context.Background(), RpcFuncHookID, map[string]interface{}{
		"id": "echo", "payload": "{}",
	})
	if err == nil || err.Error() != "rpc blocked" {
		t.Fatalf("expected rpc blocked, got %v", err)
	}

	reg2 := NewHookRegistry()
	reg2.RegisterBefore(RpcFuncHookID, func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		m := in.(map[string]interface{})
		m["payload"] = `{"rewritten":true}`
		m["id"] = "other"
		return m, nil
	})
	ex2 := NewRtHookExecutor(reg2, &testLogger{t: t}, nil, nil, nil, nil, nil, nil)
	out, err := ex2.RunBeforeReq(context.Background(), RpcFuncHookID, map[string]interface{}{
		"id": "echo", "payload": "{}",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := ApplyRpcFuncBeforeResult(nil, out)
	if req["id"] != "other" || req["payload"] != `{"rewritten":true}` {
		t.Fatalf("req=%v", req)
	}

	var afterCalled atomic.Bool
	done := make(chan struct{}, 1)
	reg2.RegisterAfter(RpcFuncHookID, func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out, in interface{}) error {
		afterCalled.Store(true)
		done <- struct{}{}
		return nil
	})
	if err := ex2.RunAfterReq(context.Background(), RpcFuncHookID, map[string]interface{}{"payload": "ok"}, req); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for after RpcFunc")
	}
	if !afterCalled.Load() {
		t.Fatal("after RpcFunc not called")
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

func TestRegisterBeforeCreatePartyDualRT(t *testing.T) {
	reg := NewHookRegistry()
	init := &goInitializer{registry: reg}
	called := false
	if err := init.RegisterBeforeCreateParty(func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *CreatePartyRequest) (*CreatePartyRequest, error) {
		called = true
		if in.MaxSize != 8 {
			t.Fatalf("max_size=%d", in.MaxSize)
		}
		in.Open = true
		return in, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.GetBefore("CreateParty"); !ok {
		t.Fatal("CreateParty not registered")
	}
	if _, ok := reg.GetBefore("party_create"); !ok {
		t.Fatal("party_create not registered")
	}
	ex := NewRtHookExecutor(reg, &testLogger{t: t}, nil, nil, nil, nil, nil, nil)
	out, err := ex.RunBeforeRt(context.Background(), "party_create", map[string]interface{}{
		"party_create": map[string]interface{}{"open": false, "max_size": float64(8)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("typed before not called via RT id")
	}
	pc := out["party_create"].(map[string]interface{})
	if pc["open"] != true {
		t.Fatalf("open=%v", pc["open"])
	}
}

func TestLifecycleSessionShutdownHttp(t *testing.T) {
	reg := NewHookRegistry()
	init := &goInitializer{registry: reg}

	var sawStart, sawEnd atomic.Bool
	done := make(chan struct{}, 2)
	_ = init.RegisterEventSessionStart(func(ctx context.Context, logger Logger, evt *Event) {
		sawStart.Store(true)
		done <- struct{}{}
	})
	_ = init.RegisterEventSessionEnd(func(ctx context.Context, logger Logger, evt *Event) {
		sawEnd.Store(true)
		done <- struct{}{}
	})

	var shut atomic.Bool
	_ = init.RegisterShutdown(func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule) {
		shut.Store(true)
	})
	_ = init.RegisterHttp("/v2/custom/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("pong"))
	}, "GET")
	_ = init.RegisterConsoleHttp("/v2/console/custom", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	reg.DispatchEvent(context.Background(), &testLogger{t: t}, &Event{Name: "session_start"})
	reg.DispatchEvent(context.Background(), &testLogger{t: t}, &Event{Name: "session_end"})
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for session lifecycle events")
		}
	}
	if !sawStart.Load() || !sawEnd.Load() {
		t.Fatalf("start=%v end=%v", sawStart.Load(), sawEnd.Load())
	}

	reg.InvokeShutdown(context.Background(), &testLogger{t: t}, nil, nil)
	if !shut.Load() {
		t.Fatal("shutdown handler not invoked")
	}
	if len(reg.HTTPHandlers()) != 1 || len(reg.ConsoleHTTPHandlers()) != 1 {
		t.Fatalf("http=%d console=%d", len(reg.HTTPHandlers()), len(reg.ConsoleHTTPHandlers()))
	}
}

func TestLuaRegisterMatchmakerOverrideAndShutdown(t *testing.T) {
	reg := NewHookRegistry()
	L := lua.NewState()
	defer L.Close()
	MapLuaNK(L, &mockRuntimeModule{}, reg)
	if err := L.DoString(`
		nk.register_matchmaker_override(function(matches) return matches end)
		nk.register_matchmaker_processor(function(tickets) return {tickets} end)
		nk.register_shutdown(function() end)
	`); err != nil {
		t.Fatal(err)
	}
	if reg.GetMatchmakerOverride() == nil {
		t.Fatal("override not registered")
	}
	if reg.GetMatchmakerProcessor() == nil {
		t.Fatal("processor not registered")
	}
	if len(reg.ShutdownHandlers()) != 1 {
		t.Fatal("shutdown not registered")
	}
}
