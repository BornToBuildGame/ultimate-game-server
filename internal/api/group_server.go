package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/social"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GroupServer struct {
	apipb.UnimplementedGroupServiceServer
	dbPool   *pgxpool.Pool
	tokenMgr *auth.TokenManager
	notifier social.FriendNotifier
}

func NewGroupServer(dbPool *pgxpool.Pool, tokenMgr *auth.TokenManager) *GroupServer {
	return &GroupServer{
		dbPool:   dbPool,
		tokenMgr: tokenMgr,
		notifier: social.NoopNotifier{},
	}
}

// SetNotifier configures group notification delivery.
func (s *GroupServer) SetNotifier(n social.FriendNotifier) {
	if n == nil {
		n = social.NoopNotifier{}
	}
	s.notifier = n
}

func (s *GroupServer) authenticate(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing token")
	}
	tokenStr := strings.TrimPrefix(authHeaders[0], "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return "", status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}
	return claims.UserID, nil
}

func (s *GroupServer) CreateGroup(ctx context.Context, req *apipb.CreateGroupRequest) (*apipb.Group, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	meta := req.GetMetadata()
	if meta == "" {
		meta = "{}"
	}
	g, err := social.CreateGroupWithParams(ctx, s.dbPool, userID, social.CreateGroupParams{
		Name: req.GetName(), Description: req.GetDescription(), AvatarURL: req.GetAvatarUrl(),
		LangTag: req.GetLangTag(), Metadata: meta, Open: req.GetOpen(), MaxCount: int(req.GetMaxCount()),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create group: %v", err)
	}
	return toProtoGroup(g), nil
}

func (s *GroupServer) UpdateGroup(ctx context.Context, req *apipb.UpdateGroupRequest) (*emptypb.Empty, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	meta := req.GetMetadata()
	if meta == "" {
		meta = "{}"
	}
	err = social.UpdateGroup(ctx, s.dbPool, userID, req.GetId(), req.GetName(), req.GetDescription(), req.GetAvatarUrl(), req.GetLangTag(), req.GetOpen(), meta)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "failed to update group: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *GroupServer) DeleteGroup(ctx context.Context, req *apipb.DeleteGroupRequest) (*emptypb.Empty, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	err = social.DeleteGroup(ctx, s.dbPool, userID, req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete group: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *GroupServer) ListGroups(ctx context.Context, req *apipb.ListGroupsRequest) (*apipb.GroupList, error) {
	var open *bool
	if req.Open != nil {
		v := req.GetOpen()
		open = &v
	}
	list, nextCursor, err := social.ListGroups(ctx, s.dbPool, req.GetName(), req.GetLangTag(), open, int(req.GetMembers()), int(req.GetLimit()), req.GetCursor())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list groups: %v", err)
	}
	protoGroups := make([]*apipb.Group, len(list))
	for i, g := range list {
		protoGroups[i] = toProtoGroup(g)
	}
	return &apipb.GroupList{Groups: protoGroups, NextCursor: nextCursor}, nil
}

func (s *GroupServer) JoinGroup(ctx context.Context, req *apipb.JoinGroupRequest) (*emptypb.Empty, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	err = social.JoinGroup(ctx, s.dbPool, userID, req.GetId(), s.notifier)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to join group: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *GroupServer) LeaveGroup(ctx context.Context, req *apipb.LeaveGroupRequest) (*emptypb.Empty, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	err = social.LeaveGroup(ctx, s.dbPool, userID, req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to leave group: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *GroupServer) AddGroupUsers(ctx context.Context, req *apipb.AddGroupUsersRequest) (*emptypb.Empty, error) {
	callerID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	err = social.AddGroupUsers(ctx, s.dbPool, callerID, req.GetGroupId(), req.GetUserIds(), s.notifier)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to add group users: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *GroupServer) KickGroupUsers(ctx context.Context, req *apipb.KickGroupUsersRequest) (*emptypb.Empty, error) {
	kickerID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	for _, uid := range req.GetUserIds() {
		if err = social.KickMember(ctx, s.dbPool, kickerID, uid, req.GetGroupId()); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "failed to kick group user %s: %v", uid, err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *GroupServer) PromoteGroupUsers(ctx context.Context, req *apipb.PromoteGroupUsersRequest) (*emptypb.Empty, error) {
	kickerID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	for _, uid := range req.GetUserIds() {
		if err = social.PromoteMember(ctx, s.dbPool, kickerID, uid, req.GetGroupId()); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "failed to promote group user %s: %v", uid, err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *GroupServer) DemoteGroupUsers(ctx context.Context, req *apipb.DemoteGroupUsersRequest) (*emptypb.Empty, error) {
	kickerID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	for _, uid := range req.GetUserIds() {
		if err = social.DemoteMember(ctx, s.dbPool, kickerID, uid, req.GetGroupId()); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "failed to demote group user %s: %v", uid, err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *GroupServer) BanGroupUsers(ctx context.Context, req *apipb.BanGroupUsersRequest) (*emptypb.Empty, error) {
	kickerID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	err = social.BanGroupUsers(ctx, s.dbPool, kickerID, req.GetGroupId(), req.GetUserIds())
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to ban group users: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *GroupServer) ListGroupUsers(ctx context.Context, req *apipb.ListGroupUsersRequest) (*apipb.GroupUserList, error) {
	list, nextCursor, err := social.ListGroupMembers(ctx, s.dbPool, req.GetGroupId(), int(req.GetLimit()), req.GetCursor())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list group users: %v", err)
	}
	protoMembers := make([]*apipb.GroupUser, len(list))
	for i, m := range list {
		protoMembers[i] = &apipb.GroupUser{
			User:  &apipb.User{Id: m.UserID, Username: m.Username},
			State: int32(m.Role),
		}
	}
	return &apipb.GroupUserList{GroupUsers: protoMembers, NextCursor: nextCursor}, nil
}

func (s *GroupServer) ListUserGroups(ctx context.Context, req *apipb.ListUserGroupsRequest) (*apipb.UserGroupList, error) {
	if _, err := s.authenticate(ctx); err != nil {
		return nil, err
	}
	list, nextCursor, err := social.ListUserGroups(ctx, s.dbPool, req.GetUserId(), int(req.GetLimit()), req.GetCursor())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list user groups: %v", err)
	}
	protoGroups := make([]*apipb.UserGroup, len(list))
	for i, r := range list {
		protoGroups[i] = &apipb.UserGroup{Group: toProtoGroup(r.Group), State: int32(r.Role)}
	}
	return &apipb.UserGroupList{UserGroups: protoGroups, NextCursor: nextCursor}, nil
}

func toProtoGroup(g *social.Group) *apipb.Group {
	open := g.State == social.GroupStateOpen
	pg := &apipb.Group{
		Id: g.ID, CreatorId: g.CreatorID, Name: g.Name, Description: g.Description,
		AvatarUrl: g.AvatarURL, LangTag: g.LangTag, Open: open,
		EdgeCount: int32(g.EdgeCount), MaxCount: int32(g.MaxCount), Metadata: g.Metadata,
	}
	if !g.CreateTime.IsZero() {
		pg.CreateTime = timestamppb.New(g.CreateTime)
	}
	if !g.UpdateTime.IsZero() {
		pg.UpdateTime = timestamppb.New(g.UpdateTime)
	}
	return pg
}

// --- REST ---

type restCreateGroupRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	AvatarURL   string `json:"avatar_url"`
	LangTag     string `json:"lang_tag"`
	Open        *bool  `json:"open"`
	MaxCount    int    `json:"max_count"`
	Metadata    string `json:"metadata"`
}

func (s *Server) handleCreateGroup(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req restCreateGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	open := true
	if req.Open != nil {
		open = *req.Open
	}
	meta := req.Metadata
	if meta == "" {
		meta = "{}"
	}
	g, err := social.CreateGroupWithParams(r.Context(), s.dbPool, userID, social.CreateGroupParams{
		Name: req.Name, Description: req.Description, AvatarURL: req.AvatarURL, LangTag: req.LangTag,
		Open: open, MaxCount: req.MaxCount, Metadata: meta,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(g)
}

type restUpdateGroupRequest struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	AvatarURL   string                 `json:"avatar_url"`
	LangTag     string                 `json:"lang_tag"`
	Open        bool                   `json:"open"`
	Metadata    map[string]interface{} `json:"metadata"`
}

func (s *Server) handleUpdateGroup(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	id := r.PathValue("id")
	var req restUpdateGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	metaBytes, _ := json.Marshal(req.Metadata)
	if req.Metadata == nil {
		metaBytes = []byte("{}")
	}
	err = social.UpdateGroup(r.Context(), s.dbPool, userID, id, req.Name, req.Description, req.AvatarURL, req.LangTag, req.Open, string(metaBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteGroup(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if err = social.DeleteGroup(r.Context(), s.dbPool, userID, r.PathValue("id")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListGroups(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var open *bool
	if openStr := q.Get("open"); openStr != "" {
		if parsed, err := strconv.ParseBool(openStr); err == nil {
			open = &parsed
		}
	}
	limitVal := 10
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}
	members := 0
	if m := q.Get("members"); m != "" {
		if parsed, err := strconv.Atoi(m); err == nil {
			members = parsed
		}
	}
	list, nextCursor, err := social.ListGroups(r.Context(), s.dbPool, q.Get("name"), q.Get("lang_tag"), open, members, limitVal, q.Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"groups": list, "next_cursor": nextCursor})
}

func (s *Server) handleJoinGroup(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if err = social.JoinGroup(r.Context(), s.dbPool, userID, r.PathValue("id"), social.NoopNotifier{}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleLeaveGroup(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if err = social.LeaveGroup(r.Context(), s.dbPool, userID, r.PathValue("id")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type restGroupUsersRequest struct {
	UserIDs []string `json:"user_ids"`
}

func (s *Server) handleAddGroupUsers(w http.ResponseWriter, r *http.Request) {
	callerID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req restGroupUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err = social.AddGroupUsers(r.Context(), s.dbPool, callerID, r.PathValue("id"), req.UserIDs, social.NoopNotifier{}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleBanGroupUsers(w http.ResponseWriter, r *http.Request) {
	callerID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req restGroupUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err = social.BanGroupUsers(r.Context(), s.dbPool, callerID, r.PathValue("id"), req.UserIDs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleKickGroupUsers(w http.ResponseWriter, r *http.Request) {
	kickerID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req restGroupUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	for _, uid := range req.UserIDs {
		if err := social.KickMember(r.Context(), s.dbPool, kickerID, uid, r.PathValue("id")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handlePromoteGroupUsers(w http.ResponseWriter, r *http.Request) {
	kickerID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req restGroupUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	for _, uid := range req.UserIDs {
		if err := social.PromoteMember(r.Context(), s.dbPool, kickerID, uid, r.PathValue("id")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDemoteGroupUsers(w http.ResponseWriter, r *http.Request) {
	kickerID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req restGroupUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	for _, uid := range req.UserIDs {
		if err := social.DemoteMember(r.Context(), s.dbPool, kickerID, uid, r.PathValue("id")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListGroupMembers(w http.ResponseWriter, r *http.Request) {
	limitVal := 10
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}
	list, nextCursor, err := social.ListGroupMembers(r.Context(), s.dbPool, r.PathValue("id"), limitVal, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"group_users": list, "next_cursor": nextCursor})
}

func (s *Server) handleListUserGroups(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateREST(r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	userID := r.PathValue("user_id")
	limitVal := 10
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}
	list, nextCursor, err := social.ListUserGroups(r.Context(), s.dbPool, userID, limitVal, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"user_groups": list, "next_cursor": nextCursor})
}
