package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/party"
	"ultimate-game-server/internal/runtime"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// PartyServer implements apipb.PartyServiceServer.
type PartyServer struct {
	apipb.UnimplementedPartyServiceServer
	registry *party.Registry
	tokenMgr *auth.TokenManager
}

// NewPartyServer creates a PartyServer.
func NewPartyServer(reg *party.Registry, tm *auth.TokenManager) *PartyServer {
	return &PartyServer{registry: reg, tokenMgr: tm}
}

func (s *PartyServer) authenticate(ctx context.Context) (*auth.Claims, error) {
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

// ListParties lists discoverable parties.
func (s *PartyServer) ListParties(ctx context.Context, req *apipb.ListPartiesRequest) (*apipb.PartyList, error) {
	if _, err := s.authenticate(ctx); err != nil {
		return nil, err
	}
	if s.registry == nil {
		return &apipb.PartyList{}, nil
	}
	limit := int(req.GetLimit())
	var open *bool
	if req.Open != nil {
		v := req.GetOpen()
		open = &v
	}
	entries, cursor, err := s.registry.List(limit, open, false, req.GetQuery(), req.GetCursor())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list parties: %v", err)
	}
	out := &apipb.PartyList{Cursor: cursor, Parties: make([]*apipb.Party, 0, len(entries))}
	for _, e := range entries {
		out.Parties = append(out.Parties, &apipb.Party{
			Id:      e.ID,
			Open:    e.Open,
			Hidden:  e.Hidden,
			MaxSize: int32(e.MaxSize),
			Label:   e.Label,
		})
	}
	return out, nil
}

func (s *Server) handleListParties(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateREST(r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if s.PartyRegistry == nil {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"parties": []interface{}{}, "cursor": ""})
		return
	}
	limit := 10
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	var open *bool
	if v := r.URL.Query().Get("open"); v != "" {
		b := v == "true" || v == "1"
		open = &b
	}
	query := r.URL.Query().Get("query")
	cursor := r.URL.Query().Get("cursor")
	entries, next, err := s.PartyRegistry.List(limit, open, false, query, cursor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	parties := make([]map[string]interface{}, 0, len(entries))
	for _, e := range entries {
		parties = append(parties, map[string]interface{}{
			"id":       e.ID,
			"open":     e.Open,
			"hidden":   e.Hidden,
			"max_size": e.MaxSize,
			"label":    e.Label,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"parties": parties, "cursor": next})
}

// partyListAdapter adapts party.Registry to runtime.PartyLister.
type partyListAdapter struct {
	reg *party.Registry
}

func (a *partyListAdapter) List(limit int, open *bool, showHidden bool, query, cursor string) ([]*runtime.PartyListEntry, string, error) {
	if a == nil || a.reg == nil {
		return []*runtime.PartyListEntry{}, "", nil
	}
	entries, next, err := a.reg.List(limit, open, showHidden, query, cursor)
	if err != nil {
		return nil, "", err
	}
	out := make([]*runtime.PartyListEntry, len(entries))
	for i, e := range entries {
		out[i] = &runtime.PartyListEntry{
			ID: e.ID, Open: e.Open, Hidden: e.Hidden, MaxSize: e.MaxSize, Label: e.Label,
		}
	}
	return out, next, nil
}
