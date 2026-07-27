package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"sync"

	"github.com/dop251/goja"
	"github.com/yuin/gopher-lua"
)

// RtHookExecutor runs before/after realtime hooks with Go → Lua → JS precedence.
type RtHookExecutor struct {
	Registry *HookRegistry
	Logger   Logger
	DB       *sql.DB
	NK       RuntimeModule
	LuaVM    *lua.LState
	JSVM     *goja.Runtime
	luaMu    *sync.Mutex
	jsMu     *sync.Mutex
}

// NewRtHookExecutor builds an executor. luaMu/jsMu may be shared with HTTP interceptors.
func NewRtHookExecutor(reg *HookRegistry, logger Logger, db *sql.DB, nk RuntimeModule, luaVM *lua.LState, jsVM *goja.Runtime, luaMu, jsMu *sync.Mutex) *RtHookExecutor {
	if luaMu == nil {
		luaMu = &sync.Mutex{}
	}
	if jsMu == nil {
		jsMu = &sync.Mutex{}
	}
	return &RtHookExecutor{
		Registry: reg,
		Logger:   logger,
		DB:       db,
		NK:       nk,
		LuaVM:    luaVM,
		JSVM:     jsVM,
		luaMu:    luaMu,
		jsMu:     jsMu,
	}
}

// rtProtobufHookAliases maps protobuf type names to JSON envelope hook keys.
var rtProtobufHookAliases = map[string]string{
	"StatusUpdate":       "status_update",
	"StatusFollow":       "status_follow",
	"StatusUnfollow":     "status_unfollow",
	"MatchCreate":        "match_create",
	"MatchJoin":          "match_join",
	"MatchLeave":         "match_leave",
	"MatchDataSend":      "match_data_send",
	"MatchmakerAdd":      "matchmaker_add",
	"MatchmakerRemove":   "matchmaker_remove",
	"ChannelJoin":        "channel_join",
	"ChannelLeave":       "channel_leave",
	"ChannelMessageSend": "channel_message_send",
	"PartyCreate":        "party_create",
	"PartyJoin":          "party_join",
	"PartyLeave":         "party_leave",
	"Rpc":                "rpc",
}

// ResolveRtHookID normalizes protobuf hook IDs to runtime registry keys.
func ResolveRtHookID(hookID string) string {
	if hookID == "" {
		return ""
	}
	if v, ok := rtProtobufHookAliases[hookID]; ok {
		return v
	}
	if strings.Contains(hookID, "_") {
		return hookID
	}
	var b strings.Builder
	for i, r := range hookID {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteByte(byte(r + ('a' - 'A')))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// RunBeforeRt invokes a before realtime hook. Returns modified envelope map (or original) and error.
func (e *RtHookExecutor) RunBeforeRt(ctx context.Context, hookID string, envelope map[string]interface{}) (map[string]interface{}, error) {
	hookID = ResolveRtHookID(hookID)
	if e == nil || e.Registry == nil || hookID == "" {
		return envelope, nil
	}
	gHook, runtimeType, fnName, found := e.Registry.GetBeforeHook(hookID)
	if !found {
		return envelope, nil
	}
	switch runtimeType {
	case "go":
		res, err := gHook(ctx, e.Logger, e.DB, e.NK, envelope)
		if err != nil {
			return nil, err
		}
		if res == nil {
			return envelope, nil
		}
		if m, ok := res.(map[string]interface{}); ok {
			return m, nil
		}
		if m, ok := res.(*map[string]interface{}); ok && m != nil {
			return *m, nil
		}
		return envelope, nil
	case "lua":
		if e.LuaVM == nil {
			return envelope, nil
		}
		e.luaMu.Lock()
		defer e.luaMu.Unlock()
		res, err := ExecuteLuaBeforeHook(e.LuaVM, fnName, ctx, envelope)
		if err != nil {
			return nil, err
		}
		if m, ok := res.(map[string]interface{}); ok {
			return m, nil
		}
		if m, ok := res.(*map[string]interface{}); ok && m != nil {
			return *m, nil
		}
		return envelope, nil
	case "js":
		if e.JSVM == nil {
			return envelope, nil
		}
		e.jsMu.Lock()
		defer e.jsMu.Unlock()
		res, err := ExecuteJSBeforeHook(e.JSVM, fnName, ctx, envelope)
		if err != nil {
			return nil, err
		}
		if m, ok := res.(map[string]interface{}); ok {
			return m, nil
		}
		if m, ok := res.(*map[string]interface{}); ok && m != nil {
			return *m, nil
		}
		return envelope, nil
	}
	return envelope, nil
}

// RunAfterRt invokes an after realtime hook (caller should run async).
func (e *RtHookExecutor) RunAfterRt(ctx context.Context, hookID string, out, in map[string]interface{}) error {
	hookID = ResolveRtHookID(hookID)
	if e == nil || e.Registry == nil || hookID == "" {
		return nil
	}
	aHook, runtimeType, fnName, found := e.Registry.GetAfterHook(hookID)
	if !found {
		return nil
	}
	switch runtimeType {
	case "go":
		return aHook(ctx, e.Logger, e.DB, e.NK, out, in)
	case "lua":
		if e.LuaVM == nil {
			return nil
		}
		e.luaMu.Lock()
		defer e.luaMu.Unlock()
		return ExecuteLuaAfterHook(e.LuaVM, fnName, ctx, out, in)
	case "js":
		if e.JSVM == nil {
			return nil
		}
		e.jsMu.Lock()
		defer e.jsMu.Unlock()
		return ExecuteJSAfterHook(e.JSVM, fnName, ctx, out, in)
	}
	return nil
}

// EnvelopeHookIDFromJSON returns the first known RT payload key in a JSON envelope map.
func EnvelopeHookIDFromJSON(m map[string]interface{}) string {
	keys := []string{
		"match_create", "match_join", "match_leave", "match_data_send",
		"matchmaker_add", "matchmaker_remove",
		"party_matchmaker_add", "party_matchmaker_remove",
		"party_create", "party_join", "party_leave", "party_promote", "party_accept",
		"party_remove", "party_close", "party_update", "party_join_request_list", "party_data_send",
		"status_follow", "status_unfollow", "status_update",
		"ping",
		"channel_join", "channel_leave", "channel_message_send", "channel_message_update", "channel_message_remove",
		"rpc",
	}
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil {
			return k
		}
	}
	return ""
}

// ParseEnvelopeMap unmarshals raw JSON into a generic map.
func ParseEnvelopeMap(payload []byte) (map[string]interface{}, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	return m, nil
}
