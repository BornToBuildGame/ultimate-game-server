package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// UserServer implements apipb.UserServiceServer.
type UserServer struct {
	apipb.UnimplementedUserServiceServer
	dbPool   *pgxpool.Pool
	tokenMgr *auth.TokenManager
}

// NewUserServer creates a UserServer.
func NewUserServer(pool *pgxpool.Pool, tm *auth.TokenManager) *UserServer {
	return &UserServer{dbPool: pool, tokenMgr: tm}
}

func (s *UserServer) authenticate(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return status.Error(codes.Unauthenticated, "missing token")
	}
	tokenStr := strings.TrimPrefix(authHeaders[0], "Bearer ")
	if _, err := s.tokenMgr.VerifyToken(tokenStr); err != nil {
		return status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}
	return nil
}

// GetUsers returns public user profiles by id, username, or facebook id.
func (s *UserServer) GetUsers(ctx context.Context, req *apipb.GetUsersRequest) (*apipb.Users, error) {
	if err := s.authenticate(ctx); err != nil {
		return nil, err
	}
	users, err := auth.GetUsersPublic(ctx, s.dbPool, req.GetIds(), req.GetUsernames(), req.GetFacebookIds())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get users: %v", err)
	}
	out := &apipb.Users{Users: make([]*apipb.User, 0, len(users))}
	for _, u := range users {
		out.Users = append(out.Users, toProtoPublicUser(u))
	}
	return out, nil
}

func toProtoPublicUser(u *auth.User) *apipb.User {
	if u == nil {
		return nil
	}
	return &apipb.User{
		Id:          u.ID.String(),
		Username:    u.Username,
		DisplayName: u.DisplayName,
		AvatarUrl:   u.AvatarURL,
		LangTag:     u.LangTag,
		Location:    u.Location,
		Timezone:    u.Timezone,
		Metadata:    u.Metadata,
		CreateTime:  timestamppb.New(u.CreateTime),
		UpdateTime:  timestamppb.New(u.UpdateTime),
	}
}

func (s *Server) handleGetUsers(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateREST(r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	ids := splitCSV(r.URL.Query().Get("ids"))
	usernames := splitCSV(r.URL.Query().Get("usernames"))
	facebookIDs := splitCSV(r.URL.Query().Get("facebook_ids"))
	if len(ids) == 0 && len(usernames) == 0 && len(facebookIDs) == 0 {
		http.Error(w, "ids, usernames, or facebook_ids required", http.StatusBadRequest)
		return
	}
	in, err := s.invokeBefore(r.Context(), "GetUsers", map[string]interface{}{
		"ids": ids, "usernames": usernames, "facebook_ids": facebookIDs,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if m, ok := in.(map[string]interface{}); ok {
		if v, ok := m["ids"].([]string); ok {
			ids = v
		}
		if v, ok := m["usernames"].([]string); ok {
			usernames = v
		}
		if v, ok := m["facebook_ids"].([]string); ok {
			facebookIDs = v
		}
	}
	users, err := auth.GetUsersPublic(r.Context(), s.dbPool, ids, usernames, facebookIDs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	items := make([]map[string]interface{}, 0, len(users))
	for _, u := range users {
		items = append(items, map[string]interface{}{
			"id":           u.ID.String(),
			"username":     u.Username,
			"display_name": u.DisplayName,
			"avatar_url":   u.AvatarURL,
			"lang_tag":     u.LangTag,
			"location":     u.Location,
			"timezone":     u.Timezone,
			"metadata":     u.Metadata,
			"create_time":  u.CreateTime.UTC().Format("2006-01-02T15:04:05Z07:00"),
			"update_time":  u.UpdateTime.UTC().Format("2006-01-02T15:04:05Z07:00"),
		})
	}
	resp := map[string]interface{}{"users": items}
	s.invokeAfter("GetUsers", resp, map[string]interface{}{"ids": ids, "usernames": usernames})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
