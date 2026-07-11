package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"

	"ultimate-game-server/internal/runtime"

	"github.com/dop251/goja"
	"github.com/yuin/gopher-lua"
	"google.golang.org/grpc"
)

var (
	luaVMMutex sync.Mutex
	jsVMMutex  sync.Mutex
)

// HTTPHookMiddleware wraps a REST handler to intercept requests and responses with before/after hooks.
func HTTPHookMiddleware(rm *runtime.GoRuntimeManager, luaVM *lua.LState, jsVM *goja.Runtime, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		endpoint := r.URL.Path

		var hookID string
		switch {
		case endpoint == "/v2/storage":
			hookID = "WriteStorageObjects"
		case endpoint == "/v2/storage/read":
			hookID = "ReadStorageObjects"
		case endpoint == "/v2/storage/delete":
			hookID = "DeleteStorageObjects"
		case strings.HasPrefix(endpoint, "/v2/storage/"):
			hookID = "ListStorageObjects"
		default:
			next(w, r)
			return
		}

		// Retrieve Before Hook from registry
		gHook, runtimeType, fnName, found := rm.Registry().GetBeforeHook(hookID)
		if found {
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusBadRequest)
				return
			}
			r.Body.Close()

			var reqVal map[string]interface{}
			_ = json.Unmarshal(bodyBytes, &reqVal)

			switch runtimeType {
			case "go":
				res, err := gHook(r.Context(), rm.Logger(), rm.DB(), rm.NK(), &reqVal)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				bodyBytes, _ = json.Marshal(res)
			case "lua":
				if luaVM != nil {
					luaVMMutex.Lock()
					res, err := runtime.ExecuteLuaBeforeHook(luaVM, fnName, r.Context(), &reqVal)
					luaVMMutex.Unlock()
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					bodyBytes, _ = json.Marshal(res)
				}
			case "js":
				if jsVM != nil {
					jsVMMutex.Lock()
					res, err := runtime.ExecuteJSBeforeHook(jsVM, fnName, r.Context(), &reqVal)
					jsVMMutex.Unlock()
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					bodyBytes, _ = json.Marshal(res)
				}
			}
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}

		// Core execution: wrap response writer to capture output for After Hook
		rec := &responseRecorder{ResponseWriter: w, body: bytes.NewBuffer(nil)}
		next(rec, r)

		// Retrieve After Hook from registry
		aHook, aRuntimeType, aFnName, aFound := rm.Registry().GetAfterHook(hookID)
		if aFound {
			var respVal interface{}
			_ = json.Unmarshal(rec.body.Bytes(), &respVal)

			var reqVal interface{}
			if r.Body != nil {
				reqBytes, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(reqBytes, &reqVal)
			}

			switch aRuntimeType {
			case "go":
				_ = aHook(r.Context(), rm.Logger(), rm.DB(), rm.NK(), &respVal, &reqVal)
			case "lua":
				if luaVM != nil {
					luaVMMutex.Lock()
					_ = runtime.ExecuteLuaAfterHook(luaVM, aFnName, r.Context(), respVal, reqVal)
					luaVMMutex.Unlock()
				}
			case "js":
				if jsVM != nil {
					jsVMMutex.Lock()
					_ = runtime.ExecuteJSAfterHook(jsVM, aFnName, r.Context(), respVal, reqVal)
					jsVMMutex.Unlock()
				}
			}
		}

		if rec.status != 0 {
			w.WriteHeader(rec.status)
		}
		w.Write(rec.body.Bytes())
	}
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	body   *bytes.Buffer
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	return r.body.Write(b)
}

// GRPCHookUnaryInterceptor returns a unary interceptor executing before/after hooks.
func GRPCHookUnaryInterceptor(rm *runtime.GoRuntimeManager, luaVM *lua.LState, jsVM *goja.Runtime) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		method := info.FullMethod
		parts := strings.Split(method, "/")
		var hookID string
		if len(parts) > 0 {
			hookID = parts[len(parts)-1]
		}

		gHook, runtimeType, fnName, found := rm.Registry().GetBeforeHook(hookID)
		if found {
			switch runtimeType {
			case "go":
				var err error
				req, err = gHook(ctx, rm.Logger(), rm.DB(), rm.NK(), req)
				if err != nil {
					return nil, err
				}
			case "lua":
				if luaVM != nil {
					luaVMMutex.Lock()
					var err error
					req, err = runtime.ExecuteLuaBeforeHook(luaVM, fnName, ctx, req)
					luaVMMutex.Unlock()
					if err != nil {
						return nil, err
					}
				}
			case "js":
				if jsVM != nil {
					jsVMMutex.Lock()
					var err error
					req, err = runtime.ExecuteJSBeforeHook(jsVM, fnName, ctx, req)
					jsVMMutex.Unlock()
					if err != nil {
						return nil, err
					}
				}
			}
		}

		resp, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}

		aHook, aRuntimeType, aFnName, aFound := rm.Registry().GetAfterHook(hookID)
		if aFound {
			switch aRuntimeType {
			case "go":
				_ = aHook(ctx, rm.Logger(), rm.DB(), rm.NK(), resp, req)
			case "lua":
				if luaVM != nil {
					luaVMMutex.Lock()
					_ = runtime.ExecuteLuaAfterHook(luaVM, aFnName, ctx, resp, req)
					luaVMMutex.Unlock()
				}
			case "js":
				if jsVM != nil {
					jsVMMutex.Lock()
					_ = runtime.ExecuteJSAfterHook(jsVM, aFnName, ctx, resp, req)
					jsVMMutex.Unlock()
				}
			}
		}

		return resp, nil
	}
}
