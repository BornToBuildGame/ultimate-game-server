package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/social"
	"ultimate-game-server/internal/api/apipb"

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
}

func NewGroupServer(dbPool *pgxpool.Pool, tokenMgr *auth.TokenManager) *GroupServer {
	return &GroupServer{
		dbPool:   dbPool,
		tokenMgr: tokenMgr,
	}
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

	g, err := social.CreateGroup(ctx, s.dbPool, userID, req.GetName(), req.GetDescription(), req.GetAvatarUrl(), req.GetLangTag())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create group: %v", err)
	}

	return toProtoGroup(g), nil
}

func (s *GroupServer) UpdateGroup(ctx context.Context, req *apipb.UpdateGroupRequest) (*emptypb.Empty, error) {
	err := social.UpdateGroup(ctx, s.dbPool, req.GetId(), req.GetName(), req.GetDescription(), req.GetAvatarUrl(), req.GetLangTag(), req.GetOpen(), req.GetMetadata())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update group: %v", err)
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
	if req.GetOpen() {
		o := true
		open = &o
	} else {
		o := false
		open = &o
	}

	list, nextCursor, err := social.ListGroups(ctx, s.dbPool, req.GetName(), req.GetLangTag(), open, int(req.GetLimit()), req.GetCursor())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list groups: %v", err)
	}

	protoGroups := make([]*apipb.Group, len(list))
	for i, g := range list {
		protoGroups[i] = toProtoGroup(g)
	}

	return &apipb.GroupList{
		Groups:     protoGroups,
		NextCursor: nextCursor,
	}, nil
}

func (s *GroupServer) JoinGroup(ctx context.Context, req *apipb.JoinGroupRequest) (*emptypb.Empty, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	err = social.JoinGroup(ctx, s.dbPool, userID, req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to join group: %v", err)
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
	for _, uid := range req.GetUserIds() {
		err := social.JoinGroup(ctx, s.dbPool, uid, req.GetGroupId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to add group user %s: %v", uid, err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *GroupServer) KickGroupUsers(ctx context.Context, req *apipb.KickGroupUsersRequest) (*emptypb.Empty, error) {
	kickerID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	for _, uid := range req.GetUserIds() {
		err = social.KickMember(ctx, s.dbPool, kickerID, uid, req.GetGroupId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to kick group user %s: %v", uid, err)
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
		err = social.PromoteMember(ctx, s.dbPool, kickerID, uid, req.GetGroupId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to promote group user %s: %v", uid, err)
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
		err = social.DemoteMember(ctx, s.dbPool, kickerID, uid, req.GetGroupId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to demote group user %s: %v", uid, err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *GroupServer) BanGroupUsers(ctx context.Context, req *apipb.BanGroupUsersRequest) (*emptypb.Empty, error) {
	kickerID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	for _, uid := range req.GetUserIds() {
		_ = social.KickMember(ctx, s.dbPool, kickerID, uid, req.GetGroupId())
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
			User: &apipb.User{
				Id:       m.UserID,
				Username: m.Username,
			},
			State: int32(m.Role),
		}
	}

	return &apipb.GroupUserList{
		GroupUsers: protoMembers,
		NextCursor: nextCursor,
	}, nil
}

func (s *GroupServer) ListUserGroups(ctx context.Context, req *apipb.ListUserGroupsRequest) (*apipb.UserGroupList, error) {
	list, nextCursor, err := social.ListUserGroups(ctx, s.dbPool, req.GetUserId(), int(req.GetLimit()), req.GetCursor())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list user groups: %v", err)
	}

	protoGroups := make([]*apipb.UserGroup, len(list))
	for i, r := range list {
		protoGroups[i] = &apipb.UserGroup{
			Group: toProtoGroup(r.Group),
			State: int32(r.Role),
		}
	}

	return &apipb.UserGroupList{
		UserGroups: protoGroups,
		NextCursor: nextCursor,
	}, nil
}

func toProtoGroup(g *social.Group) *apipb.Group {
	open := true
	if g.State == social.GroupStateClosed {
		open = false
	}
	return &apipb.Group{
		Id:          g.ID,
		CreatorId:   g.CreatorID,
		Name:        g.Name,
		Description: g.Description,
		AvatarUrl:   g.AvatarURL,
		LangTag:     g.LangTag,
		Open:        open,
		EdgeCount:   int32(g.EdgeCount),
		MaxCount:    int32(g.MaxCount),
		CreateTime:  timestamppb.Now(),
		UpdateTime:  timestamppb.Now(),
	}
}

// REST Handlers on Server

type restCreateGroupRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	AvatarURL   string `json:"avatar_url"`
	LangTag     string `json:"lang_tag"`
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

	g, err := social.CreateGroup(r.Context(), s.dbPool, userID, req.Name, req.Description, req.AvatarURL, req.LangTag)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(g)
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
	id := r.PathValue("id")

	var req restUpdateGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	metaBytes, _ := json.Marshal(req.Metadata)

	err := social.UpdateGroup(r.Context(), s.dbPool, id, req.Name, req.Description, req.AvatarURL, req.LangTag, req.Open, string(metaBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
	id := r.PathValue("id")

	err = social.DeleteGroup(r.Context(), s.dbPool, userID, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListGroups(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	name := q.Get("name")
	langTag := q.Get("lang_tag")
	
	var open *bool
	if openStr := q.Get("open"); openStr != "" {
		parsed, err := strconv.ParseBool(openStr)
		if err == nil {
			open = &parsed
		}
	}

	limitVal := 10
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}

	cursor := q.Get("cursor")

	list, nextCursor, err := social.ListGroups(r.Context(), s.dbPool, name, langTag, open, limitVal, cursor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"groups":      list,
		"next_cursor": nextCursor,
	})
}

func (s *Server) handleJoinGroup(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	id := r.PathValue("id")

	err = social.JoinGroup(r.Context(), s.dbPool, userID, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
	id := r.PathValue("id")

	err = social.LeaveGroup(r.Context(), s.dbPool, userID, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

type restGroupUsersRequest struct {
	UserIDs []string `json:"user_ids"`
}

func (s *Server) handleKickGroupUsers(w http.ResponseWriter, r *http.Request) {
	kickerID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	id := r.PathValue("id")

	var req restGroupUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	for _, uid := range req.UserIDs {
		err := social.KickMember(r.Context(), s.dbPool, kickerID, uid, id)
		if err != nil {
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
	id := r.PathValue("id")

	var req restGroupUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	for _, uid := range req.UserIDs {
		err := social.PromoteMember(r.Context(), s.dbPool, kickerID, uid, id)
		if err != nil {
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
	id := r.PathValue("id")

	var req restGroupUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	for _, uid := range req.UserIDs {
		err := social.DemoteMember(r.Context(), s.dbPool, kickerID, uid, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListGroupMembers(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := r.URL.Query()

	limitVal := 10
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}

	cursor := q.Get("cursor")

	list, nextCursor, err := social.ListGroupMembers(r.Context(), s.dbPool, id, limitVal, cursor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"group_users": list,
		"next_cursor": nextCursor,
	})
}
