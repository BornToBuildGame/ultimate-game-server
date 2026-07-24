package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/runtime"

	"github.com/dop251/goja"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yuin/gopher-lua"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// RpcServer implements apipb.RpcServiceServer.
type RpcServer struct {
	apipb.UnimplementedRpcServiceServer
	dbPool   *pgxpool.Pool
	tokenMgr *auth.TokenManager
	runtime  *runtime.GoRuntimeManager
	rpcCfg   runtime.RPCConfig
	luaVM    *lua.LState
	jsVM     *goja.Runtime
}

func NewRpcServer(pool *pgxpool.Pool, tm *auth.TokenManager, rm *runtime.GoRuntimeManager, cfg runtime.RPCConfig) *RpcServer {
	return &RpcServer{dbPool: pool, tokenMgr: tm, runtime: rm, rpcCfg: cfg}
}

func (s *RpcServer) SetVMs(luaVM *lua.LState, jsVM *goja.Runtime) {
	s.luaVM = luaVM
	s.jsVM = jsVM
}

func (s *RpcServer) SetRuntime(rm *runtime.GoRuntimeManager) { s.runtime = rm }

func (s *RpcServer) authenticateRPC(ctx context.Context, httpKeyField string) (userID, username string, vars map[string]string, err error) {
	if httpKeyField != "" {
		if httpKeyField != s.rpcCfg.HTTPKey {
			return "", "", nil, status.Error(codes.Unauthenticated, "HTTP key invalid")
		}
		return "", "", nil, nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", nil, status.Error(codes.Unauthenticated, "Auth token or HTTP key required")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return "", "", nil, status.Error(codes.Unauthenticated, "Auth token or HTTP key required")
	}
	raw := authHeaders[0]
	if strings.HasPrefix(strings.ToLower(raw), "basic ") {
		decoded, decErr := base64.StdEncoding.DecodeString(strings.TrimSpace(raw[6:]))
		if decErr != nil {
			return "", "", nil, status.Error(codes.Unauthenticated, "HTTP key invalid")
		}
		parts := strings.SplitN(string(decoded), ":", 2)
		key := parts[0]
		if key != s.rpcCfg.HTTPKey {
			return "", "", nil, status.Error(codes.Unauthenticated, "HTTP key invalid")
		}
		return "", "", nil, nil
	}
	tokenStr := strings.TrimPrefix(raw, "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return "", "", nil, status.Error(codes.Unauthenticated, "Auth token invalid")
	}
	return claims.UserID, claims.Username, claims.Vars, nil
}

func (s *RpcServer) RpcFunc(ctx context.Context, req *apipb.Rpc) (*apipb.Rpc, error) {
	if s.runtime == nil {
		return nil, status.Error(codes.Unavailable, "runtime not configured")
	}
	userID, username, vars, err := s.authenticateRPC(ctx, req.GetHttpKey())
	if err != nil {
		return nil, err
	}
	result, code, err := s.runtime.DispatchRPC(ctx, req.GetId(), req.GetPayload(), runtime.RPCDispatchOpts{
		UserID: userID, Username: username, Vars: vars, ExecutionMode: "grpc",
	}, s.luaVM, s.jsVM, s.rpcCfg)
	if err != nil {
		return nil, status.Error(code, err.Error())
	}
	return &apipb.Rpc{Id: strings.ToLower(req.GetId()), Payload: result}, nil
}

// --- REST ---

func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeRPCError(w, http.StatusBadRequest, 3, "RPC ID must be set")
		return
	}
	if s.RuntimeManager == nil {
		writeRPCError(w, http.StatusServiceUnavailable, 14, "runtime not configured")
		return
	}

	userID, username, vars, ok := s.authenticateRPCHTTP(w, r)
	if !ok {
		return
	}

	var payload string
	unwrap := r.URL.Query().Has("unwrap")
	if r.Method == http.MethodPost {
		body, err := io.ReadAll(io.LimitReader(r.Body, int64(s.rpcCfg.MaxPayloadBytes)+1))
		if err != nil {
			writeRPCError(w, http.StatusBadRequest, 3, "bad request body")
			return
		}
		if len(body) > s.rpcCfg.MaxPayloadBytes {
			writeRPCError(w, http.StatusBadRequest, 3, "http: request body too large")
			return
		}
		if unwrap {
			payload = string(body)
		} else if len(body) > 0 {
			if err := json.Unmarshal(body, &payload); err != nil {
				writeRPCError(w, http.StatusBadRequest, 3, `json: cannot unmarshal object into Go value of type string`)
				return
			}
		}
	}

	result, code, err := s.RuntimeManager.DispatchRPC(r.Context(), id, payload, runtime.RPCDispatchOpts{
		UserID: userID, Username: username, Vars: vars,
		ClientIP: clientIP(r), QueryParams: r.URL.Query(), Headers: r.Header,
		ExecutionMode: "http",
	}, s.LuaVM, s.JSVM, s.rpcCfg)
	if err != nil {
		httpStatus := grpcHTTPStatus(code)
		writeRPCError(w, httpStatus, int(code), err.Error())
		return
	}

	if unwrap {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if ct := r.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		_, _ = w.Write([]byte(result))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"payload": result})
}

func (s *Server) authenticateRPCHTTP(w http.ResponseWriter, r *http.Request) (userID, username string, vars map[string]string, ok bool) {
	if key := r.URL.Query().Get("http_key"); key != "" {
		if key != s.rpcCfg.HTTPKey {
			writeRPCError(w, http.StatusUnauthorized, 16, "HTTP key invalid")
			return "", "", nil, false
		}
		return "", "", nil, true
	}
	authz := r.Header.Get("Authorization")
	if authz == "" {
		writeRPCError(w, http.StatusUnauthorized, 16, "Auth token or HTTP key required")
		return "", "", nil, false
	}
	if strings.HasPrefix(strings.ToLower(authz), "basic ") {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(authz[6:]))
		if err != nil {
			writeRPCError(w, http.StatusUnauthorized, 16, "HTTP key invalid")
			return "", "", nil, false
		}
		parts := strings.SplitN(string(decoded), ":", 2)
		if parts[0] != s.rpcCfg.HTTPKey {
			writeRPCError(w, http.StatusUnauthorized, 16, "HTTP key invalid")
			return "", "", nil, false
		}
		return "", "", nil, true
	}
	tokenStr := strings.TrimPrefix(authz, "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		writeRPCError(w, http.StatusUnauthorized, 16, "Auth token invalid")
		return "", "", nil, false
	}
	return claims.UserID, claims.Username, claims.Vars, true
}

func writeRPCError(w http.ResponseWriter, httpStatus, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error": msg, "message": msg, "code": code,
	})
}

func grpcHTTPStatus(c codes.Code) int {
	switch c {
	case codes.OK:
		return http.StatusOK
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i >= 0 {
		return host[:i]
	}
	return host
}