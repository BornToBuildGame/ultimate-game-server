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

// resolveHTTPHookID maps REST paths/methods to runtime before/after hook IDs.
// Hook IDs match gRPC method names for leaderboard/tournament parity.
func resolveHTTPHookID(method, path string) string {
	switch {
	case path == "/v2/storage":
		return "WriteStorageObjects"
	case path == "/v2/storage/read":
		return "ReadStorageObjects"
	case path == "/v2/storage/delete":
		return "DeleteStorageObjects"
	case strings.HasPrefix(path, "/v2/storage/"):
		return "ListStorageObjects"
	case path == "/v2/user" && method == http.MethodGet:
		return "GetUsers"
	case path == "/v2/event" && method == http.MethodPost:
		return "Event"
	case path == "/v2/session/logout" && method == http.MethodPost:
		return "SessionLogout"
	case path == "/healthcheck" || path == "/health":
		return "Healthcheck"
	case path == "/ready":
		return "Ready"
	}

	if strings.HasPrefix(path, "/v2/leaderboard") {
		parts := strings.Split(strings.Trim(path, "/"), "/")
		// parts: ["v2","leaderboard", ...]
		switch {
		case method == http.MethodPost && len(parts) == 3:
			return "WriteLeaderboardRecord"
		case method == http.MethodGet && len(parts) == 3:
			return "ListLeaderboardRecords"
		case method == http.MethodGet && len(parts) == 5 && parts[3] == "around":
			return "ListLeaderboardRecordsAroundOwner"
		case method == http.MethodDelete && len(parts) == 5 && parts[3] == "owner":
			return "DeleteLeaderboardRecord"
		}
	}

	if strings.HasPrefix(path, "/v2/tournament") {
		parts := strings.Split(strings.Trim(path, "/"), "/")
		switch {
		case method == http.MethodGet && len(parts) == 2:
			return "ListTournaments"
		case method == http.MethodPost && len(parts) == 4 && parts[3] == "join":
			return "JoinTournament"
		case (method == http.MethodPost || method == http.MethodPut) && len(parts) == 3:
			return "WriteTournamentRecord"
		case method == http.MethodGet && len(parts) == 3:
			return "ListTournamentRecords"
		case method == http.MethodGet && len(parts) == 5 && parts[3] == "around":
			return "ListTournamentRecordsAroundOwner"
		case method == http.MethodDelete && len(parts) == 5 && parts[3] == "owner":
			return "DeleteTournamentRecord"
		}
	}

	if strings.HasPrefix(path, "/v2/friend") {
		parts := strings.Split(strings.Trim(path, "/"), "/")
		// parts: ["v2","friend", ...]
		switch {
		case method == http.MethodPost && len(parts) == 2:
			return "AddFriends"
		case method == http.MethodGet && len(parts) == 2:
			return "ListFriends"
		case method == http.MethodGet && len(parts) == 3 && parts[2] == "friends":
			return "ListFriendsOfFriends"
		case method == http.MethodDelete && len(parts) == 2:
			return "DeleteFriends"
		case method == http.MethodPost && len(parts) == 3 && parts[2] == "block":
			return "BlockFriends"
		case method == http.MethodPost && len(parts) == 4 && parts[2] == "block":
			return "BlockFriends"
		case method == http.MethodDelete && len(parts) == 3 && parts[2] == "block":
			return "UnblockFriends"
		case method == http.MethodDelete && len(parts) == 4 && parts[2] == "block":
			return "UnblockFriends"
		case method == http.MethodPost && len(parts) == 3 && parts[2] == "unblock":
			return "UnblockFriends"
		case method == http.MethodPost && len(parts) == 3 && parts[2] == "facebook":
			return "ImportFacebookFriends"
		case method == http.MethodPost && len(parts) == 3 && parts[2] == "steam":
			return "ImportSteamFriends"
		}
	}

	if strings.HasPrefix(path, "/v2/matchmaker/stats") {
		if method == http.MethodGet {
			return "GetMatchmakerStats"
		}
	}

	if strings.HasPrefix(path, "/v2/iap/") {
		parts := strings.Split(strings.Trim(path, "/"), "/")
		// parts: ["v2","iap", "purchase"|"subscription", ...]
		switch {
		case method == http.MethodPost && len(parts) == 4 && parts[2] == "purchase" && parts[3] == "apple":
			return "ValidatePurchaseApple"
		case method == http.MethodPost && len(parts) == 4 && parts[2] == "purchase" && parts[3] == "google":
			return "ValidatePurchaseGoogle"
		case method == http.MethodPost && len(parts) == 4 && parts[2] == "purchase" && parts[3] == "huawei":
			return "ValidatePurchaseHuawei"
		case method == http.MethodPost && len(parts) == 4 && parts[2] == "purchase" && parts[3] == "facebookinstant":
			return "ValidatePurchaseFacebookInstant"
		case method == http.MethodPost && len(parts) == 4 && parts[2] == "purchase" && parts[3] == "samsung":
			return "ValidatePurchaseSamsung"
		case method == http.MethodPost && len(parts) == 4 && parts[2] == "subscription" && parts[3] == "apple":
			return "ValidateSubscriptionApple"
		case method == http.MethodPost && len(parts) == 4 && parts[2] == "subscription" && parts[3] == "google":
			return "ValidateSubscriptionGoogle"
		case method == http.MethodPost && len(parts) == 3 && parts[2] == "subscription":
			return "ListSubscriptions"
		case method == http.MethodGet && len(parts) == 4 && parts[2] == "subscription":
			return "GetSubscription"
		}
	}
	return ""
}

func buildHTTPHookRequest(r *http.Request, bodyBytes []byte) map[string]interface{} {
	reqVal := map[string]interface{}{}
	if len(bodyBytes) > 0 {
		_ = json.Unmarshal(bodyBytes, &reqVal)
	}
	// Enrich with path/query context for GET/DELETE hooks.
	if id := r.PathValue("id"); id != "" {
		if strings.HasPrefix(r.URL.Path, "/v2/tournament") {
			if _, ok := reqVal["tournament_id"]; !ok {
				reqVal["tournament_id"] = id
			}
		} else if _, ok := reqVal["leaderboard_id"]; !ok {
			reqVal["leaderboard_id"] = id
		}
	}
	if ownerID := r.PathValue("owner_id"); ownerID != "" {
		reqVal["owner_id"] = ownerID
	}
	if userID := r.PathValue("user_id"); userID != "" {
		reqVal["user_id"] = userID
		if ids, ok := reqVal["ids"]; !ok || ids == nil {
			reqVal["ids"] = []string{userID}
		}
	}
	if productID := r.PathValue("product_id"); productID != "" {
		reqVal["product_id"] = productID
	}
	for k, vals := range r.URL.Query() {
		if len(vals) == 1 {
			reqVal[k] = vals[0]
		} else if len(vals) > 1 {
			reqVal[k] = vals
		}
	}
	return reqVal
}

// HTTPHookMiddleware wraps a REST handler to intercept requests and responses with before/after hooks.
func HTTPHookMiddleware(rm *runtime.GoRuntimeManager, luaVM *lua.LState, jsVM *goja.Runtime, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if rm == nil {
			next(w, r)
			return
		}
		hookID := resolveHTTPHookID(r.Method, r.URL.Path)
		if hookID == "" {
			next(w, r)
			return
		}

		var bodyBytes []byte
		var reqVal map[string]interface{}

		// Retrieve Before Hook from registry
		gHook, runtimeType, fnName, found := rm.Registry().GetBeforeHook(hookID)
		if found {
			if r.Body != nil && r.Method != http.MethodGet && r.Method != http.MethodDelete {
				var err error
				bodyBytes, err = io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, "failed to read body", http.StatusBadRequest)
					return
				}
				r.Body.Close()
			}
			reqVal = buildHTTPHookRequest(r, bodyBytes)

			switch runtimeType {
			case "go":
				res, err := gHook(r.Context(), rm.Logger(), rm.DB(), rm.NK(), &reqVal)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				if casted, ok := res.(*map[string]interface{}); ok {
					reqVal = *casted
				} else if casted, ok := res.(map[string]interface{}); ok {
					reqVal = casted
				}
				bodyBytes, _ = json.Marshal(reqVal)
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
					if m, ok := res.(*map[string]interface{}); ok {
						reqVal = *m
					} else if m, ok := res.(map[string]interface{}); ok {
						reqVal = m
					}
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
					if m, ok := res.(*map[string]interface{}); ok {
						reqVal = *m
					} else if m, ok := res.(map[string]interface{}); ok {
						reqVal = m
					}
				}
			}
			if r.Method != http.MethodGet && r.Method != http.MethodDelete {
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}
		} else if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
			bodyBytes, _ = io.ReadAll(r.Body)
			r.Body.Close()
			reqVal = buildHTTPHookRequest(r, bodyBytes)
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		} else {
			reqVal = buildHTTPHookRequest(r, nil)
		}

		// Core execution: wrap response writer to capture output for After Hook
		rec := &responseRecorder{ResponseWriter: w, body: bytes.NewBuffer(nil)}
		next(rec, r)

		// Retrieve After Hook from registry
		aHook, aRuntimeType, aFnName, aFound := rm.Registry().GetAfterHook(hookID)
		if aFound {
			var respVal interface{}
			_ = json.Unmarshal(rec.body.Bytes(), &respVal)
			go func(ctx context.Context, respVal, reqVal interface{}, aHook runtime.AfterHook, aRuntimeType, aFnName string) {
				switch aRuntimeType {
				case "go":
					_ = aHook(ctx, rm.Logger(), rm.DB(), rm.NK(), &respVal, &reqVal)
				case "lua":
					if luaVM != nil {
						luaVMMutex.Lock()
						_ = runtime.ExecuteLuaAfterHook(luaVM, aFnName, ctx, respVal, reqVal)
						luaVMMutex.Unlock()
					}
				case "js":
					if jsVM != nil {
						jsVMMutex.Lock()
						_ = runtime.ExecuteJSAfterHook(jsVM, aFnName, ctx, respVal, reqVal)
						jsVMMutex.Unlock()
					}
				}
			}(context.WithoutCancel(r.Context()), respVal, reqVal, aHook, aRuntimeType, aFnName)
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
			go func(ctx context.Context, resp, req interface{}, aHook runtime.AfterHook, aRuntimeType, aFnName string) {
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
			}(context.WithoutCancel(ctx), resp, req, aHook, aRuntimeType, aFnName)
		}

		return resp, nil
	}
}
