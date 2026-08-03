package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/BornToBuildGame/ultimate-game-server/internal/api"
	"github.com/BornToBuildGame/ultimate-game-server/internal/match"
	"github.com/BornToBuildGame/ultimate-game-server/internal/runtime"

	"github.com/dop251/goja"
	"github.com/yuin/gopher-lua"
	"go.uber.org/zap"
)

type consoleZapLogger struct{ z *zap.Logger }

func (l *consoleZapLogger) Info(msg string, fields ...any) {
	l.z.Sugar().Infow(msg, fields...)
}
func (l *consoleZapLogger) Error(msg string, fields ...any) {
	l.z.Sugar().Errorw(msg, fields...)
}

type matchConsoleAdapter struct{ router *match.Router }

func (a *matchConsoleAdapter) ListMatches() []map[string]interface{} {
	if a == nil || a.router == nil {
		return nil
	}
	local := a.router.GetLocalMatches()
	out := make([]map[string]interface{}, 0, len(local))
	for _, m := range local {
		out = append(out, map[string]interface{}{
			"match_id": m.MatchID, "label": m.Label,
			"size": m.PlayerCount, "max_size": m.MaxSize, "authoritative": m.Authoritative,
		})
	}
	return out
}

func (a *matchConsoleAdapter) GetMatchState(matchID string) (map[string]interface{}, error) {
	if a == nil || a.router == nil {
		return nil, errors.New("not found")
	}
	m, presences, ok := a.router.GetLocalMatch(matchID)
	if !ok {
		return nil, errors.New("not found")
	}
	ps := make([]map[string]interface{}, 0, len(presences))
	for _, p := range presences {
		ps = append(ps, map[string]interface{}{
			"user_id": p.UserID, "session_id": p.SessionID, "username": p.Username,
		})
	}
	return map[string]interface{}{
		"match_id": m.MatchID, "label": m.Label, "size": m.PlayerCount,
		"presences": ps, "authoritative": m.Authoritative,
	}, nil
}

type statusConsoleAdapter struct {
	server *api.Server
	match  *matchConsoleAdapter
}

func (a *statusConsoleAdapter) ConsoleStatus() map[string]interface{} {
	out := map[string]interface{}{}
	if a.match != nil {
		out["match_count"] = len(a.match.ListMatches())
	}
	if a.server != nil && a.server.SocketRegistry != nil {
		out["note"] = "console_status"
	}
	return out
}

type rpcConsoleAdapter struct {
	rm    *runtime.GoRuntimeManager
	luaVM *lua.LState
	jsVM  *goja.Runtime
	cfg   runtime.RPCConfig
}

func (a *rpcConsoleAdapter) DispatchRPC(ctx context.Context, id, payload, userID, username string) (string, error) {
	if a == nil || a.rm == nil {
		return "", errors.New("rpc unavailable")
	}
	res, code, err := a.rm.DispatchRPC(ctx, id, payload, runtime.RPCDispatchOpts{
		UserID: userID, Username: username, ExecutionMode: "console",
	}, a.luaVM, a.jsVM, a.cfg)
	if err != nil {
		return "", fmt.Errorf("%v (code=%v)", err, code)
	}
	return res, nil
}
