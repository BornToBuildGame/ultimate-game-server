package socket

import (
	"encoding/json"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/notification"
	"github.com/BornToBuildGame/ultimate-game-server/internal/presence"
)

// NotificationsStream returns the stream key for a user's notification inbox.
func NotificationsStream(userID string) presence.StreamKey {
	return presence.StreamKey{Mode: presence.StreamModeNotifications, Subject: userID}
}

func (gh *GatewayHandler) trackNotifications(s *Session) {
	if gh.StreamTracker == nil || s == nil {
		return
	}
	gh.StreamTracker.TrackMembership(s.ID, NotificationsStream(s.UserID))
}

// SendNotifications implements notification.Deliverer for a single user.
func (gh *GatewayHandler) SendNotifications(userID string, notifs []*notification.Notification) {
	if gh == nil || len(notifs) == 0 || userID == "" {
		return
	}
	payload := buildNotificationsEnvelope(notifs)
	if payload == nil {
		return
	}
	if gh.StreamTracker != nil {
		for _, sid := range gh.StreamTracker.Sessions(NotificationsStream(userID)) {
			if sess, ok := gh.registry.GetBySession(sid); ok && sess != nil {
				sess.TrySend(payload)
			}
		}
		return
	}
	// Fallback: deliver to all sessions for the user.
	for _, sess := range gh.registry.GetUserSessions(userID) {
		sess.TrySend(payload)
	}
}

// SendNotificationsToAll implements notification.Deliverer for connected clients.
func (gh *GatewayHandler) SendNotificationsToAll(notifs []*notification.Notification) {
	if gh == nil || len(notifs) == 0 {
		return
	}
	payload := buildNotificationsEnvelope(notifs)
	if payload == nil {
		return
	}
	for _, sess := range gh.registry.AllSessions() {
		sess.TrySend(payload)
	}
}

func buildNotificationsEnvelope(notifs []*notification.Notification) []byte {
	items := make([]map[string]interface{}, 0, len(notifs))
	for _, n := range notifs {
		if n == nil {
			continue
		}
		items = append(items, map[string]interface{}{
			"id":          n.ID,
			"subject":     n.Subject,
			"content":     n.Content,
			"code":        n.Code,
			"sender_id":   n.SenderID,
			"create_time": n.CreateTime.UTC().Format(time.RFC3339Nano),
			"persistent":  n.Persistent,
		})
	}
	if len(items) == 0 {
		return nil
	}
	b, _ := json.Marshal(map[string]interface{}{"notifications": items})
	return b
}
