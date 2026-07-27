package runtime

import (
	"context"
)

// RpcFuncHookID is the request-hook id for custom RPC (REST /v2/rpc/*, gRPC RpcFunc, WS rpc envelopes).
const RpcFuncHookID = "RpcFunc"

// RunBeforeReq invokes a before request hook by id with Go → Lua → JS precedence.
// Returns the (possibly modified) request value and an error if the hook rejects.
func (e *RtHookExecutor) RunBeforeReq(ctx context.Context, hookID string, in interface{}) (interface{}, error) {
	if e == nil || e.Registry == nil || hookID == "" {
		return in, nil
	}
	gHook, runtimeType, fnName, found := e.Registry.GetBeforeHook(hookID)
	if !found {
		return in, nil
	}
	switch runtimeType {
	case "go":
		res, err := gHook(ctx, e.Logger, e.DB, e.NK, in)
		if err != nil {
			return nil, err
		}
		if res == nil {
			return in, nil
		}
		return res, nil
	case "lua":
		if e.LuaVM == nil {
			return in, nil
		}
		e.luaMu.Lock()
		defer e.luaMu.Unlock()
		return ExecuteLuaBeforeHook(e.LuaVM, fnName, ctx, in)
	case "js":
		if e.JSVM == nil {
			return in, nil
		}
		e.jsMu.Lock()
		defer e.jsMu.Unlock()
		return ExecuteJSBeforeHook(e.JSVM, fnName, ctx, in)
	}
	return in, nil
}

// RunAfterReq invokes an after request hook (caller should typically run async).
func (e *RtHookExecutor) RunAfterReq(ctx context.Context, hookID string, out, in interface{}) error {
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

// ApplyRpcFuncBeforeResult merges a before-hook result map into id/payload fields.
func ApplyRpcFuncBeforeResult(req map[string]interface{}, result interface{}) map[string]interface{} {
	if result == nil {
		return req
	}
	if m, ok := result.(map[string]interface{}); ok {
		return m
	}
	if m, ok := result.(*map[string]interface{}); ok && m != nil {
		return *m
	}
	return req
}
