package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/api/apipb"
	"github.com/BornToBuildGame/ultimate-game-server/internal/auth"
	"github.com/BornToBuildGame/ultimate-game-server/internal/notification"
	"github.com/BornToBuildGame/ultimate-game-server/internal/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotificationServer implements apipb.NotificationServiceServer.
type NotificationServer struct {
	dbPool   *pgxpool.Pool
	tokenMgr *auth.TokenManager
	hooks    *runtime.HookRegistry
}

// NewNotificationServer creates a NotificationServer.
func NewNotificationServer(pool *pgxpool.Pool, tm *auth.TokenManager, hooks *runtime.HookRegistry) *NotificationServer {
	return &NotificationServer{dbPool: pool, tokenMgr: tm, hooks: hooks}
}

// SetHooks attaches Before/After List/Delete hook registry.
func (s *NotificationServer) SetHooks(hooks *runtime.HookRegistry) {
	s.hooks = hooks
}

func (s *NotificationServer) authenticate(ctx context.Context) (*auth.Claims, error) {
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

func toProtoNotification(n *notification.Notification) *apipb.Notification {
	if n == nil {
		return nil
	}
	return &apipb.Notification{
		Id:         n.ID,
		Subject:    n.Subject,
		Content:    n.Content,
		Code:       int32(n.Code),
		SenderId:   n.SenderID,
		CreateTime: timestamppb.New(n.CreateTime),
		Persistent: n.Persistent,
	}
}

func (s *NotificationServer) ListNotifications(ctx context.Context, req *apipb.ListNotificationsRequest) (*apipb.NotificationList, error) {
	claims, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if s.hooks != nil {
		if before, ok := s.hooks.GetBefore("ListNotifications"); ok && before != nil {
			out, herr := before(ctx, nil, nil, nil, req)
			if herr != nil {
				return nil, status.Errorf(codes.Internal, "%v", herr)
			}
			if out == nil {
				return nil, status.Error(codes.NotFound, "Requested resource was not found.")
			}
			if r, ok := out.(*apipb.ListNotificationsRequest); ok {
				req = r
			}
		}
	}
	limit := int(req.GetLimit().GetValue())
	list, err := notification.NotificationList(ctx, s.dbPool, claims.UserID, limit, req.GetCacheableCursor())
	if err != nil {
		if errors.Is(err, notification.ErrNotificationCursorInvalid) {
			return nil, status.Error(codes.InvalidArgument, "cursor is invalid or expired")
		}
		return nil, status.Errorf(codes.Internal, "list notifications: %v", err)
	}
	out := &apipb.NotificationList{
		CacheableCursor: list.CacheableCursor,
		Notifications:   make([]*apipb.Notification, 0, len(list.Notifications)),
	}
	for _, n := range list.Notifications {
		out.Notifications = append(out.Notifications, toProtoNotification(n))
	}
	if s.hooks != nil {
		if after, ok := s.hooks.GetAfter("ListNotifications"); ok && after != nil {
			_ = after(ctx, nil, nil, nil, out, req)
		}
	}
	return out, nil
}

func (s *NotificationServer) DeleteNotifications(ctx context.Context, req *apipb.DeleteNotificationsRequest) (*emptypb.Empty, error) {
	claims, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if s.hooks != nil {
		if before, ok := s.hooks.GetBefore("DeleteNotifications"); ok && before != nil {
			out, herr := before(ctx, nil, nil, nil, req)
			if herr != nil {
				return nil, status.Errorf(codes.Internal, "%v", herr)
			}
			if out == nil {
				return nil, status.Error(codes.NotFound, "Requested resource was not found.")
			}
			if r, ok := out.(*apipb.DeleteNotificationsRequest); ok {
				req = r
			}
		}
	}
	if err := notification.NotificationDelete(ctx, s.dbPool, claims.UserID, req.GetIds()); err != nil {
		return nil, status.Errorf(codes.Internal, "delete notifications: %v", err)
	}
	if s.hooks != nil {
		if after, ok := s.hooks.GetAfter("DeleteNotifications"); ok && after != nil {
			_ = after(ctx, nil, nil, nil, &emptypb.Empty{}, req)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) handleListNotifications(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	limit := 1
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	cursor := r.URL.Query().Get("cacheable_cursor")
	list, err := notification.NotificationList(r.Context(), s.dbPool, userID, limit, cursor)
	if err != nil {
		if errors.Is(err, notification.ErrNotificationCursorInvalid) {
			http.Error(w, "cursor is invalid or expired", http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	items := make([]map[string]interface{}, 0, len(list.Notifications))
	for _, n := range list.Notifications {
		items = append(items, map[string]interface{}{
			"id":          n.ID,
			"subject":     n.Subject,
			"content":     n.Content,
			"code":        n.Code,
			"sender_id":   n.SenderID,
			"create_time": n.CreateTime.UTC().Format(time.RFC3339Nano),
			"persistent":  true,
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"notifications":    items,
		"cacheable_cursor": list.CacheableCursor,
	})
}

func (s *Server) handleDeleteNotifications(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	ids := r.URL.Query()["ids"]
	if len(ids) == 1 && strings.Contains(ids[0], ",") {
		ids = strings.Split(ids[0], ",")
	}
	cleaned := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			cleaned = append(cleaned, id)
		}
	}
	if err := notification.NotificationDelete(r.Context(), s.dbPool, userID, cleaned); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
