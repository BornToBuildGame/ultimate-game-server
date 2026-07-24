package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/runtime"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// EventServer implements apipb.EventServiceServer.
type EventServer struct {
	apipb.UnimplementedEventServiceServer
	tokenMgr *auth.TokenManager
	hooks    *runtime.HookRegistry
	logger   runtime.Logger
}

// NewEventServer creates an EventServer.
func NewEventServer(tm *auth.TokenManager, hooks *runtime.HookRegistry, logger runtime.Logger) *EventServer {
	return &EventServer{tokenMgr: tm, hooks: hooks, logger: logger}
}

// SetHooks updates the hook registry.
func (s *EventServer) SetHooks(hooks *runtime.HookRegistry) {
	s.hooks = hooks
}

func (s *EventServer) authenticate(ctx context.Context) (*auth.Claims, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing token")
	}
	tokenStr := strings.TrimPrefix(authHeaders[0], "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}
	return claims, nil
}

// Event accepts client telemetry and dispatches to runtime event handlers.
func (s *EventServer) Event(ctx context.Context, req *apipb.ClientEvent) (*emptypb.Empty, error) {
	claims, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	s.dispatch(claims.UserID, claims.Username, req.GetName(), req.GetProperties(), req.GetTimestamp())
	return &emptypb.Empty{}, nil
}

func (s *EventServer) dispatch(userID, username, name string, props map[string]string, ts int64) {
	if s.hooks == nil {
		return
	}
	if props == nil {
		props = map[string]string{}
	}
	props["user_id"] = userID
	props["username"] = username
	props["external"] = "true"
	if ts == 0 {
		ts = time.Now().Unix()
	}
	s.hooks.DispatchEvent(context.Background(), s.logger, &runtime.Event{
		Name:       name,
		Properties: props,
		Timestamp:  ts,
	})
}

func (s *Server) handleClientEvent(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		Name       string            `json:"name"`
		Properties map[string]string `json:"properties"`
		Timestamp  int64             `json:"timestamp"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}
	if _, err := s.invokeBefore(r.Context(), "Event", &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	username := ""
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		if claims, err := s.tokenMgr.VerifyToken(strings.TrimPrefix(h, "Bearer ")); err == nil && claims != nil {
			username = claims.Username
		}
	}
	if s.RuntimeManager != nil {
		props := req.Properties
		if props == nil {
			props = map[string]string{}
		}
		props["user_id"] = userID
		props["username"] = username
		props["external"] = "true"
		ts := req.Timestamp
		if ts == 0 {
			ts = time.Now().Unix()
		}
		s.RuntimeManager.Registry().DispatchEvent(r.Context(), s.RuntimeManager.Logger(), &runtime.Event{
			Name: req.Name, Properties: props, Timestamp: ts,
		})
	}
	s.invokeAfter("Event", nil, &req)
	w.WriteHeader(http.StatusOK)
}
