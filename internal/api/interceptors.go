package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"ultimate-game-server/internal/runtime"

	"github.com/dop251/goja"
	"github.com/yuin/gopher-lua"
	"google.golang.org/grpc"
)

// HTTPHookMiddleware wraps a REST handler to intercept requests and responses with before/after hooks.
func HTTPHookMiddleware(registry *runtime.HookRegistry, luaVM *lua.LState, jsVM *goja.Runtime, next http.HandlerFunc) http.HandlerFunc {
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
		gHook, runtimeType, fnName, found := registry.GetBeforeHook(hookID)
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
				res, err := gHook(r.Context(), nil, nil, nil, reqVal)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				bodyBytes, _ = json.Marshal(res)
			case "lua":
				if luaVM != nil {
					res, err := runtime.ExecuteLuaBeforeHook(luaVM, fnName, r.Context(), &reqVal)
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					bodyBytes, _ = json.Marshal(res)
				}
			case "js":
				if jsVM != nil {
					res, err := runtime.ExecuteJSBeforeHook(jsVM, fnName, r.Context(), &reqVal)
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
		aHook, aRuntimeType, aFnName, aFound := registry.GetAfterHook(hookID)
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
				_ = aHook(r.Context(), nil, nil, nil, respVal, reqVal)
			case "lua":
				if luaVM != nil {
					_ = runtime.ExecuteLuaAfterHook(luaVM, aFnName, r.Context(), respVal, reqVal)
				}
			case "js":
				if jsVM != nil {
					_ = runtime.ExecuteJSAfterHook(jsVM, aFnName, r.Context(), respVal, reqVal)
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
func GRPCHookUnaryInterceptor(registry *runtime.HookRegistry, luaVM *lua.LState, jsVM *goja.Runtime) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		method := info.FullMethod
		parts := strings.Split(method, "/")
		var hookID string
		if len(parts) > 0 {
			hookID = parts[len(parts)-1]
		}

		gHook, runtimeType, fnName, found := registry.GetBeforeHook(hookID)
		if found {
			switch runtimeType {
			case "go":
				var err error
				req, err = gHook(ctx, nil, nil, nil, req)
				if err != nil {
					return nil, err
				}
			case "lua":
				if luaVM != nil {
					var err error
					req, err = runtime.ExecuteLuaBeforeHook(luaVM, fnName, ctx, req)
					if err != nil {
						return nil, err
					}
				}
			case "js":
				if jsVM != nil {
					var err error
					req, err = runtime.ExecuteJSBeforeHook(jsVM, fnName, ctx, req)
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

		aHook, aRuntimeType, aFnName, aFound := registry.GetAfterHook(hookID)
		if aFound {
			switch aRuntimeType {
			case "go":
				_ = aHook(ctx, nil, nil, nil, resp, req)
			case "lua":
				if luaVM != nil {
					_ = runtime.ExecuteLuaAfterHook(luaVM, aFnName, ctx, resp, req)
				}
			case "js":
				if jsVM != nil {
					_ = runtime.ExecuteJSAfterHook(jsVM, aFnName, ctx, resp, req)
				}
			}
		}

		return resp, nil
	}
}
