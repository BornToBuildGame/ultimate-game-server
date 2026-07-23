package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/chat"
	"ultimate-game-server/internal/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ChannelServer implements apipb.ChatServiceServer.
type ChannelServer struct {
	apipb.UnimplementedChatServiceServer
	dbPool   *pgxpool.Pool
	tokenMgr *auth.TokenManager
	hooks    *runtime.HookRegistry
}

// NewChannelServer creates a ChannelServer.
func NewChannelServer(pool *pgxpool.Pool, tm *auth.TokenManager, hooks *runtime.HookRegistry) *ChannelServer {
	return &ChannelServer{dbPool: pool, tokenMgr: tm, hooks: hooks}
}

func (s *ChannelServer) authenticate(ctx context.Context) (*auth.Claims, error) {
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

func toProtoChannelMessage(m chat.ChannelMessage) *apipb.ChannelMessage {
	return &apipb.ChannelMessage{
		ChannelId:  m.ChannelID,
		MessageId:  m.MessageID,
		Code:       int32(m.Code),
		SenderId:   m.SenderID,
		Username:   m.Username,
		Content:    m.Content,
		CreateTime: timestamppb.New(m.CreateTime),
		UpdateTime: timestamppb.New(m.UpdateTime),
		Persistent: m.Persistent,
		RoomName:   m.RoomName,
		GroupId:    m.GroupID,
		UserIdOne:  m.UserIDOne,
		UserIdTwo:  m.UserIDTwo,
	}
}

// ListChannelMessages lists persistent channel history.
func (s *ChannelServer) ListChannelMessages(ctx context.Context, req *apipb.ListChannelMessagesRequest) (*apipb.ChannelMessageList, error) {
	claims, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetChannelId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid channel ID")
	}

	limit := int(req.GetLimit())
	if limit == 0 {
		limit = 20
	}
	if limit < 1 || limit > 100 {
		return nil, status.Error(codes.InvalidArgument, "invalid limit - limit must be between 1 and 100")
	}
	forward := true
	if req.Forward != nil {
		forward = req.GetForward()
	}

	stream, err := chat.ChannelIdToStream(req.GetChannelId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid channel ID")
	}

	list, err := chat.ChannelMessagesList(ctx, s.dbPool, claims.UserID, stream, req.GetChannelId(), limit, forward, req.GetCursor())
	if err != nil {
		if errors.Is(err, chat.ErrChannelCursorInvalid) {
			return nil, status.Error(codes.InvalidArgument, "cursor is invalid or expired")
		}
		if errors.Is(err, chat.ErrChannelGroupNotFound) || errors.Is(err, chat.ErrChannelIDInvalid) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "list channel messages: %v", err)
	}

	out := &apipb.ChannelMessageList{
		NextCursor:      list.NextCursor,
		PrevCursor:      list.PrevCursor,
		CacheableCursor: list.CacheableCursor,
		Messages:        make([]*apipb.ChannelMessage, 0, len(list.Messages)),
	}
	for _, m := range list.Messages {
		out.Messages = append(out.Messages, toProtoChannelMessage(m))
	}
	return out, nil
}

func (s *Server) handleListChannelMessages(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	channelID := r.PathValue("channel_id")
	if channelID == "" {
		http.Error(w, "invalid channel ID", http.StatusBadRequest)
		return
	}
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	if limit < 1 || limit > 100 {
		http.Error(w, "invalid limit", http.StatusBadRequest)
		return
	}
	forward := true
	if v := r.URL.Query().Get("forward"); v != "" {
		forward = v == "true" || v == "1"
	}
	cursor := r.URL.Query().Get("cursor")

	stream, err := chat.ChannelIdToStream(channelID)
	if err != nil {
		http.Error(w, "invalid channel ID", http.StatusBadRequest)
		return
	}
	list, err := chat.ChannelMessagesList(r.Context(), s.dbPool, userID, stream, channelID, limit, forward, cursor)
	if err != nil {
		if errors.Is(err, chat.ErrChannelCursorInvalid) {
			http.Error(w, "cursor is invalid or expired", http.StatusBadRequest)
			return
		}
		if errors.Is(err, chat.ErrChannelGroupNotFound) || errors.Is(err, chat.ErrChannelIDInvalid) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	messages := make([]map[string]interface{}, 0, len(list.Messages))
	for _, m := range list.Messages {
		item := map[string]interface{}{
			"channel_id":  m.ChannelID,
			"message_id":  m.MessageID,
			"code":        m.Code,
			"sender_id":   m.SenderID,
			"username":    m.Username,
			"content":     m.Content,
			"create_time": m.CreateTime.UTC().Format(time.RFC3339Nano),
			"update_time": m.UpdateTime.UTC().Format(time.RFC3339Nano),
			"persistent":  m.Persistent,
		}
		if m.RoomName != "" {
			item["room_name"] = m.RoomName
		}
		if m.GroupID != "" {
			item["group_id"] = m.GroupID
		}
		if m.UserIDOne != "" {
			item["user_id_one"] = m.UserIDOne
			item["user_id_two"] = m.UserIDTwo
		}
		messages = append(messages, item)
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"messages":         messages,
		"next_cursor":      list.NextCursor,
		"prev_cursor":      list.PrevCursor,
		"cacheable_cursor": list.CacheableCursor,
	})
}
