package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/BornToBuildGame/ultimate-game-server/internal/api/apipb"
	"github.com/BornToBuildGame/ultimate-game-server/internal/auth"
	"github.com/BornToBuildGame/ultimate-game-server/internal/notification"
	"github.com/BornToBuildGame/ultimate-game-server/internal/presence"
	"github.com/BornToBuildGame/ultimate-game-server/internal/social"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type FriendsServer struct {
	dbPool    *pgxpool.Pool
	tokenMgr  *auth.TokenManager
	presence  *presence.PresenceTracker
	notifier  social.FriendNotifier
	friendCfg social.Config
}

// DBNotifier persists friend notifications via the notification package.
type DBNotifier struct {
	Pool *pgxpool.Pool
}

func (n DBNotifier) Notify(ctx context.Context, userID, subject, content string, code int16, senderID string) error {
	return notification.CreateNotification(ctx, n.Pool, &notification.Notification{
		UserID:     userID,
		Subject:    subject,
		Content:    content,
		Code:       code,
		SenderID:   senderID,
		Persistent: true,
	})
}

func NewFriendsServer(dbPool *pgxpool.Pool, tokenMgr *auth.TokenManager, pt *presence.PresenceTracker) *FriendsServer {
	return &FriendsServer{
		dbPool:    dbPool,
		tokenMgr:  tokenMgr,
		presence:  pt,
		notifier:  DBNotifier{Pool: dbPool},
		friendCfg: social.DefaultConfig(),
	}
}

func (s *FriendsServer) authenticate(ctx context.Context) (string, string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return "", "", status.Error(codes.Unauthenticated, "missing token")
	}
	tokenStr := strings.TrimPrefix(authHeaders[0], "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return "", "", status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}
	return claims.UserID, claims.Username, nil
}

func friendUserToProto(u social.FriendUser, online bool) *apipb.User {
	return &apipb.User{
		Id:          u.ID,
		Username:    u.Username,
		DisplayName: u.DisplayName,
		AvatarUrl:   u.AvatarURL,
		LangTag:     u.LangTag,
		Location:    u.Location,
		Timezone:    u.Timezone,
		Metadata:    u.Metadata,
		CreateTime:  timestamppb.New(u.CreateTime),
		UpdateTime:  timestamppb.New(u.UpdateTime),
		Online:      online,
		EdgeCount:   u.EdgeCount,
	}
}

func (s *FriendsServer) onlineSet(userIDs []string) map[string]bool {
	out := make(map[string]bool, len(userIDs))
	if s.presence == nil || len(userIDs) == 0 {
		return out
	}
	for _, id := range s.presence.GetOnlineFriends(userIDs) {
		out[id] = true
	}
	return out
}

func (s *FriendsServer) AddFriends(ctx context.Context, req *apipb.AddFriendsRequest) (*emptypb.Empty, error) {
	userID, _, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	resolvedIDs, err := social.ResolveUserIDs(ctx, s.dbPool, req.GetIds(), req.GetUsernames())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resolve users: %v", err)
	}
	for _, destID := range resolvedIDs {
		if err := social.AddFriendWithOpts(ctx, s.dbPool, userID, destID, "{}", s.friendCfg, s.notifier); err != nil {
			return nil, mapFriendError(err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *FriendsServer) ListFriends(ctx context.Context, req *apipb.ListFriendsRequest) (*apipb.FriendList, error) {
	userID, _, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	state := 0
	if req.GetState() != nil {
		state = int(req.GetState().GetValue())
	}
	limit := 0
	if req.GetLimit() != nil {
		limit = int(req.GetLimit().GetValue())
	}
	list, nextCursor, err := social.ListFriends(ctx, s.dbPool, userID, state, limit, req.GetCursor())
	if err != nil {
		return nil, mapFriendError(err)
	}
	ids := make([]string, len(list))
	for i, f := range list {
		ids[i] = f.User.ID
	}
	online := s.onlineSet(ids)
	protoFriends := make([]*apipb.Friend, len(list))
	for i, f := range list {
		protoFriends[i] = &apipb.Friend{
			User:       friendUserToProto(f.User, online[f.User.ID]),
			State:      wrapperspb.Int32(int32(f.State)),
			UpdateTime: timestamppb.New(f.UpdateTime),
			Metadata:   f.Metadata,
		}
	}
	return &apipb.FriendList{Friends: protoFriends, Cursor: nextCursor}, nil
}

func (s *FriendsServer) ListFriendsOfFriends(ctx context.Context, req *apipb.ListFriendsOfFriendsRequest) (*apipb.FriendsOfFriendsList, error) {
	userID, _, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	limit := 10
	if req.GetLimit() != nil {
		limit = int(req.GetLimit().GetValue())
	}
	list, next, err := social.ListFriendsOfFriends(ctx, s.dbPool, userID, limit, req.GetCursor())
	if err != nil {
		return nil, mapFriendError(err)
	}
	ids := make([]string, len(list))
	for i, f := range list {
		ids[i] = f.User.ID
	}
	online := s.onlineSet(ids)
	out := make([]*apipb.FriendsOfFriendsList_FriendOfFriend, len(list))
	for i, f := range list {
		out[i] = &apipb.FriendsOfFriendsList_FriendOfFriend{
			Referrer: f.Referrer,
			User:     friendUserToProto(f.User, online[f.User.ID]),
		}
	}
	return &apipb.FriendsOfFriendsList{FriendsOfFriends: out, Cursor: next}, nil
}

func (s *FriendsServer) DeleteFriends(ctx context.Context, req *apipb.DeleteFriendsRequest) (*emptypb.Empty, error) {
	userID, _, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	resolvedIDs, err := social.ResolveUserIDs(ctx, s.dbPool, req.GetIds(), req.GetUsernames())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resolve users: %v", err)
	}
	for _, destID := range resolvedIDs {
		if err := social.DeleteFriend(ctx, s.dbPool, userID, destID, s.notifier); err != nil {
			return nil, mapFriendError(err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *FriendsServer) BlockFriends(ctx context.Context, req *apipb.BlockFriendsRequest) (*emptypb.Empty, error) {
	userID, _, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	resolvedIDs, err := social.ResolveUserIDs(ctx, s.dbPool, req.GetIds(), req.GetUsernames())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resolve users: %v", err)
	}
	for _, destID := range resolvedIDs {
		if err := social.BlockUser(ctx, s.dbPool, userID, destID); err != nil {
			return nil, mapFriendError(err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *FriendsServer) ImportFacebookFriends(ctx context.Context, req *apipb.ImportFacebookFriendsRequest) (*emptypb.Empty, error) {
	userID, username, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	token := ""
	if req.GetAccount() != nil {
		token = req.GetAccount().GetToken()
	}
	if token == "" {
		return nil, status.Error(codes.InvalidArgument, "Facebook token is required")
	}
	reset := false
	if req.GetReset_() != nil {
		reset = req.GetReset_().GetValue()
	}
	if err := social.ImportFacebookFriends(ctx, s.dbPool, userID, username, token, reset, s.notifier); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to import Facebook friends: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *FriendsServer) ImportSteamFriends(ctx context.Context, req *apipb.ImportSteamFriendsRequest) (*emptypb.Empty, error) {
	userID, username, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	token := ""
	if req.GetAccount() != nil {
		token = req.GetAccount().GetToken()
	}
	if token == "" {
		return nil, status.Error(codes.InvalidArgument, "Steam token is required")
	}
	reset := false
	if req.GetReset_() != nil {
		reset = req.GetReset_().GetValue()
	}
	if err := social.ImportSteamFriends(ctx, s.dbPool, userID, username, token, reset, s.notifier); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to import Steam friends: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func mapFriendError(err error) error {
	switch err {
	case social.ErrCannotAddSelf, social.ErrCannotBlockSelf:
		return status.Error(codes.InvalidArgument, err.Error())
	case social.ErrFriendLimitReached, social.ErrPendingLimitReached:
		return status.Error(codes.ResourceExhausted, err.Error())
	case social.ErrInvalidCursor:
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}

type restAddFriendsRequest struct {
	IDs       []string `json:"ids"`
	Usernames []string `json:"usernames"`
}

type restImportRequest struct {
	Token string `json:"token"`
	Reset bool   `json:"reset"`
}

func (s *Server) friendNotifier() social.FriendNotifier {
	return DBNotifier{Pool: s.dbPool}
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
	resolvedIDs, err := social.ResolveUserIDs(r.Context(), s.dbPool, req.IDs, req.Usernames)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, destID := range resolvedIDs {
		if err := social.AddFriendWithOpts(r.Context(), s.dbPool, userID, destID, "{}", social.DefaultConfig(), s.friendNotifier()); err != nil {
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
	limitVal := 100
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
	list, nextCursor, err := social.ListFriends(r.Context(), s.dbPool, userID, stateVal, limitVal, q.Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ids := make([]string, len(list))
	for i, f := range list {
		ids[i] = f.User.ID
	}
	online := map[string]bool{}
	if s.presenceTracker != nil {
		for _, id := range s.presenceTracker.GetOnlineFriends(ids) {
			online[id] = true
		}
	}
	friendsOut := make([]map[string]interface{}, len(list))
	for i, f := range list {
		friendsOut[i] = map[string]interface{}{
			"user": map[string]interface{}{
				"id": f.User.ID, "username": f.User.Username, "display_name": f.User.DisplayName,
				"avatar_url": f.User.AvatarURL, "lang_tag": f.User.LangTag, "location": f.User.Location,
				"timezone": f.User.Timezone, "metadata": f.User.Metadata, "online": online[f.User.ID],
				"edge_count": f.User.EdgeCount,
			},
			"state":       f.State,
			"update_time": f.UpdateTime,
			"metadata":    f.Metadata,
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"friends":     friendsOut,
		"next_cursor": nextCursor,
	})
}

func (s *Server) handleListFriendsOfFriends(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	q := r.URL.Query()
	limitVal := 100
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}
	list, next, err := social.ListFriendsOfFriends(r.Context(), s.dbPool, userID, limitVal, q.Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ids := make([]string, len(list))
	for i, f := range list {
		ids[i] = f.User.ID
	}
	online := map[string]bool{}
	if s.presenceTracker != nil {
		for _, id := range s.presenceTracker.GetOnlineFriends(ids) {
			online[id] = true
		}
	}
	out := make([]map[string]interface{}, len(list))
	for i, f := range list {
		out[i] = map[string]interface{}{
			"referrer": f.Referrer,
			"user": map[string]interface{}{
				"id": f.User.ID, "username": f.User.Username, "display_name": f.User.DisplayName,
				"online": online[f.User.ID],
			},
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"friends_of_friends": out,
		"cursor":             next,
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
	resolvedIDs, err := social.ResolveUserIDs(r.Context(), s.dbPool, req.IDs, req.Usernames)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, destID := range resolvedIDs {
		if err := social.DeleteFriend(r.Context(), s.dbPool, userID, destID, s.friendNotifier()); err != nil {
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
	if err := social.BlockUser(r.Context(), s.dbPool, userID, destID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleBlockFriendsBody(w http.ResponseWriter, r *http.Request) {
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
	resolvedIDs, err := social.ResolveUserIDs(r.Context(), s.dbPool, req.IDs, req.Usernames)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, destID := range resolvedIDs {
		if err := social.BlockUser(r.Context(), s.dbPool, userID, destID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleUnblockFriend(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	destID := r.PathValue("user_id")
	if destID == "" {
		http.Error(w, "missing user_id to unblock", http.StatusBadRequest)
		return
	}
	if err := social.UnblockUser(r.Context(), s.dbPool, userID, destID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleUnblockFriendsBody(w http.ResponseWriter, r *http.Request) {
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
	resolvedIDs, err := social.ResolveUserIDs(r.Context(), s.dbPool, req.IDs, req.Usernames)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, destID := range resolvedIDs {
		if err := social.UnblockUser(r.Context(), s.dbPool, userID, destID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleImportFacebookFriends(w http.ResponseWriter, r *http.Request) {
	userID, username, err := s.authenticateRESTWithUsername(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req restImportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.Token == "" {
		http.Error(w, "Facebook token is required", http.StatusBadRequest)
		return
	}
	if err := social.ImportFacebookFriends(r.Context(), s.dbPool, userID, username, req.Token, req.Reset, s.friendNotifier()); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleImportSteamFriends(w http.ResponseWriter, r *http.Request) {
	userID, username, err := s.authenticateRESTWithUsername(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req restImportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.Token == "" {
		http.Error(w, "Steam token is required", http.StatusBadRequest)
		return
	}
	if err := social.ImportSteamFriends(r.Context(), s.dbPool, userID, username, req.Token, req.Reset, s.friendNotifier()); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}
