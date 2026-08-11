package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dop251/goja"
	"github.com/yuin/gopher-lua"
	"google.golang.org/grpc/codes"
)

// Context keys for RPC / hook execution.
type ctxKey string

const (
	CtxUserID        ctxKey = "user_id"
	CtxUsername      ctxKey = "username"
	CtxVars          ctxKey = "vars"
	CtxClientIP      ctxKey = "client_ip"
	CtxEnv           ctxKey = "env"
	CtxQueryParams   ctxKey = "query_params"
	CtxHeaders       ctxKey = "headers"
	CtxExecutionMode ctxKey = "execution_mode"
)

// Error is a typed runtime error with a gRPC status code (1–16).
type Error struct {
	Message string
	Code    codes.Code
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// NewError creates a typed error. Invalid codes are coerced to Internal (13).
func NewError(msg string, code int) *Error {
	c := codes.Code(code)
	if code < 1 || code > 16 {
		c = codes.Internal
	}
	return &Error{Message: msg, Code: c}
}

// ErrBadRequest returns a typed InvalidArgument (3) runtime error.
func ErrBadRequest(msg string) error {
	return NewError(msg, int(codes.InvalidArgument))
}

// CodeFromError extracts a gRPC code from err (default Internal).
func CodeFromError(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	var re *Error
	if errors.As(err, &re) && re != nil {
		return re.Code
	}
	return codes.Internal
}

// RPCConfig holds custom RPC limits and auth.
type RPCConfig struct {
	HTTPKey            string
	ExecutionTimeoutMs int
	MaxPayloadBytes    int
}

// DefaultRPCConfig returns reference-aligned defaults.
func DefaultRPCConfig() RPCConfig {
	return RPCConfig{
		HTTPKey:            "defaulthttpkey",
		ExecutionTimeoutMs: 10000,
		MaxPayloadBytes:    256 * 1024,
	}
}

// RPCDispatchOpts carries per-request RPC context.
type RPCDispatchOpts struct {
	UserID        string
	Username      string
	Vars          map[string]string
	ClientIP      string
	Env           string
	QueryParams   map[string][]string
	Headers       map[string][]string
	ExecutionMode string // "rpc", "http", "grpc", "websocket"
}

// DispatchRPC invokes a registered RPC (Go > Lua > JS) with timeout and size checks.
func (m *GoRuntimeManager) DispatchRPC(ctx context.Context, id, payload string, opts RPCDispatchOpts, luaVM *lua.LState, jsVM *goja.Runtime, cfg RPCConfig) (string, codes.Code, error) {
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" {
		return "", codes.InvalidArgument, NewError("RPC ID must be set", int(codes.InvalidArgument))
	}
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = DefaultRPCConfig().MaxPayloadBytes
	}
	if len(payload) > cfg.MaxPayloadBytes {
		return "", codes.InvalidArgument, NewError("http: request body too large", int(codes.InvalidArgument))
	}

	handler, runtimeKind, fnName, ok := m.registry.GetRPCHook(id)
	if !ok {
		return "", codes.NotFound, NewError("RPC function not found", int(codes.NotFound))
	}

	timeout := time.Duration(cfg.ExecutionTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ctx = context.WithValue(ctx, CtxUserID, opts.UserID)
	ctx = context.WithValue(ctx, CtxUsername, opts.Username)
	ctx = context.WithValue(ctx, CtxVars, opts.Vars)
	ctx = context.WithValue(ctx, CtxClientIP, opts.ClientIP)
	ctx = context.WithValue(ctx, CtxEnv, opts.Env)
	ctx = context.WithValue(ctx, CtxQueryParams, opts.QueryParams)
	ctx = context.WithValue(ctx, CtxHeaders, opts.Headers)
	mode := opts.ExecutionMode
	if mode == "" {
		mode = "rpc"
	}
	ctx = context.WithValue(ctx, CtxExecutionMode, mode)
	// Also set string keys for Lua ExecuteLuaRPC compatibility.
	ctx = context.WithValue(ctx, "user_id", opts.UserID)

	var (
		result string
		err    error
	)
	switch runtimeKind {
	case "go":
		err = m.safeCall(func() error {
			var rpcErr error
			result, rpcErr = handler(ctx, m.logger, m.db, m.nk, payload)
			return rpcErr
		})
	case "lua":
		if luaVM == nil {
			return "", codes.Internal, NewError("Lua VM not configured", int(codes.Internal))
		}
		result, err = ExecuteLuaRPC(luaVM, fnName, ctx, payload)
	case "js":
		if jsVM == nil {
			return "", codes.Internal, NewError("JS VM not configured", int(codes.Internal))
		}
		result, err = ExecuteJSRPC(jsVM, fnName, ctx, payload)
	default:
		return "", codes.Internal, fmt.Errorf("unknown runtime %q", runtimeKind)
	}

	if err != nil {
		code := CodeFromError(err)
		return "", code, err
	}
	if ctx.Err() == context.DeadlineExceeded {
		return "", codes.DeadlineExceeded, NewError("RPC execution timeout", int(codes.DeadlineExceeded))
	}
	return result, codes.OK, nil
}

// RpcCall invokes another RPC from the runtime (server-to-server style).
func (m *GoRuntimeModule) RpcCall(ctx context.Context, id, payload string) (string, error) {
	if m.rpcDispatcher == nil {
		return "", errors.New("RPC dispatcher not configured")
	}
	result, _, err := m.rpcDispatcher(ctx, id, payload, RPCDispatchOpts{ExecutionMode: "rpc"})
	return result, err
}

// RPCDispatcherFunc is set on GoRuntimeModule to avoid import cycles with VMs.
type RPCDispatcherFunc func(ctx context.Context, id, payload string, opts RPCDispatchOpts) (string, codes.Code, error)

// Ensure sql import used when building plugin-compatible signatures.
var _ *sql.DB
