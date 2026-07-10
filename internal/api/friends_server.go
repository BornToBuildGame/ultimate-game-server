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

type FriendsServer struct {
	apipb.UnimplementedFriendsServiceServer
	dbPool   *pgxpool.Pool
	tokenMgr *auth.TokenManager
}

func NewFriendsServer(dbPool *pgxpool.Pool, tokenMgr *auth.TokenManager) *FriendsServer {
	return &FriendsServer{
		dbPool:   dbPool,
		tokenMgr: tokenMgr,
	}
}

func (s *FriendsServer) authenticate(ctx context.Context) (string, error) {
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

func (s *FriendsServer) AddFriends(ctx context.Context, req *apipb.AddFriendsRequest) (*emptypb.Empty, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resolvedIDs := make([]string, len(req.GetIds()))
	copy(resolvedIDs, req.GetIds())

	for _, username := range req.GetUsernames() {
		var uid string
		err := s.dbPool.QueryRow(ctx, "SELECT id FROM users WHERE username = $1", username).Scan(&uid)
		if err == nil {
			resolvedIDs = append(resolvedIDs, uid)
		}
	}

	for _, destID := range resolvedIDs {
		err := social.AddFriend(ctx, s.dbPool, userID, destID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to add friend: %v", err)
		}
	}

	return &emptypb.Empty{}, nil
}

func (s *FriendsServer) ListFriends(ctx context.Context, req *apipb.ListFriendsRequest) (*apipb.FriendList, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	list, nextCursor, err := social.ListFriends(ctx, s.dbPool, userID, int(req.GetState()), int(req.GetLimit()), req.GetCursor())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list friends: %v", err)
	}

	protoFriends := make([]*apipb.Friend, len(list))
	for i, f := range list {
		protoFriends[i] = &apipb.Friend{
			User: &apipb.User{
				Id:       f.UserID,
				Username: f.Username,
			},
			State:      int32(f.State),
			UpdateTime: timestamppb.Now(),
		}
	}

	return &apipb.FriendList{
		Friends:    protoFriends,
		NextCursor: nextCursor,
	}, nil
}

func (s *FriendsServer) DeleteFriends(ctx context.Context, req *apipb.DeleteFriendsRequest) (*emptypb.Empty, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resolvedIDs := make([]string, len(req.GetIds()))
	copy(resolvedIDs, req.GetIds())

	for _, username := range req.GetUsernames() {
		var uid string
		err := s.dbPool.QueryRow(ctx, "SELECT id FROM users WHERE username = $1", username).Scan(&uid)
		if err == nil {
			resolvedIDs = append(resolvedIDs, uid)
		}
	}

	for _, destID := range resolvedIDs {
		err := social.DeleteFriend(ctx, s.dbPool, userID, destID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to delete friend: %v", err)
		}
	}

	return &emptypb.Empty{}, nil
}

func (s *FriendsServer) BlockFriends(ctx context.Context, req *apipb.BlockFriendsRequest) (*emptypb.Empty, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	resolvedIDs := make([]string, len(req.GetIds()))
	copy(resolvedIDs, req.GetIds())

	for _, username := range req.GetUsernames() {
		var uid string
		err := s.dbPool.QueryRow(ctx, "SELECT id FROM users WHERE username = $1", username).Scan(&uid)
		if err == nil {
			resolvedIDs = append(resolvedIDs, uid)
		}
	}

	for _, destID := range resolvedIDs {
		err := social.BlockUser(ctx, s.dbPool, userID, destID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to block friend: %v", err)
		}
	}

	return &emptypb.Empty{}, nil
}

func (s *FriendsServer) ImportFacebookFriends(ctx context.Context, req *apipb.ImportFacebookFriendsRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *FriendsServer) ImportSteamFriends(ctx context.Context, req *apipb.ImportSteamFriendsRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// REST Handlers on Server

type restAddFriendsRequest struct {
	IDs       []string `json:"ids"`
	Usernames []string `json:"usernames"`
}

func (s *Server) handleAddFriends(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var req restAddFriendsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	resolvedIDs := make([]string, len(req.IDs))
	copy(resolvedIDs, req.IDs)

	for _, username := range req.Usernames {
		var uid string
		err := s.dbPool.QueryRow(r.Context(), "SELECT id FROM users WHERE username = $1", username).Scan(&uid)
		if err == nil {
			resolvedIDs = append(resolvedIDs, uid)
		}
	}

	for _, destID := range resolvedIDs {
		err := social.AddFriend(r.Context(), s.dbPool, userID, destID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListFriends(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	q := r.URL.Query()

	limitVal := 10
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}

	stateVal := 0
	if stateStr := q.Get("state"); stateStr != "" {
		if parsed, err := strconv.Atoi(stateStr); err == nil {
			stateVal = parsed
		}
	}

	cursor := q.Get("cursor")

	list, nextCursor, err := social.ListFriends(r.Context(), s.dbPool, userID, stateVal, limitVal, cursor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"friends":     list,
		"next_cursor": nextCursor,
	})
}

func (s *Server) handleDeleteFriends(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var req restAddFriendsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	resolvedIDs := make([]string, len(req.IDs))
	copy(resolvedIDs, req.IDs)

	for _, username := range req.Usernames {
		var uid string
		err := s.dbPool.QueryRow(r.Context(), "SELECT id FROM users WHERE username = $1", username).Scan(&uid)
		if err == nil {
			resolvedIDs = append(resolvedIDs, uid)
		}
	}

	for _, destID := range resolvedIDs {
		err := social.DeleteFriend(r.Context(), s.dbPool, userID, destID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleBlockFriend(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	destID := r.PathValue("user_id")

	if destID == "" {
		http.Error(w, "missing user_id to block", http.StatusBadRequest)
		return
	}

	err = social.BlockUser(r.Context(), s.dbPool, userID, destID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
