package runtime

import (
	"fmt"

	"ultimate-game-server/internal/presence"
)

// StreamPresenceView is a runtime-facing presence on a typed stream.
type StreamPresenceView struct {
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`
	Username  string `json:"username"`
	Status    string `json:"status,omitempty"`
	Hidden    bool   `json:"hidden,omitempty"`
}

// StreamManager backs RuntimeModule stream_* APIs (ADR-0020 / ADR-0019).
type StreamManager interface {
	StreamUserList(mode int16, subject, subcontext, label string, includeHidden, includeNotHidden bool) ([]StreamPresenceView, error)
	StreamUserJoin(mode int16, subject, subcontext, label, userID, sessionID string, hidden, persistence bool, status string) (bool, error)
	StreamUserLeave(mode int16, subject, subcontext, label, userID, sessionID string) error
	StreamCount(mode int16, subject, subcontext, label string) (int, error)
	StreamSend(mode int16, subject, subcontext, label, data string, sessionIDs []string, reliable bool) error
	SessionDisconnect(sessionID string) error
}

// LocalStreamManager implements StreamManager with LocalTracker + MessageRouter.
type LocalStreamManager struct {
	Tracker  *presence.LocalTracker
	Router   presence.MessageRouter
	Registry SessionDisconnecter
}

// SessionDisconnecter closes a live WebSocket session by ID.
type SessionDisconnecter interface {
	DisconnectSession(sessionID string) error
}

func streamKey(mode int16, subject, subcontext, label string) presence.StreamKey {
	return presence.StreamKey{Mode: mode, Subject: subject, Subcontext: subcontext, Label: label}
}

func (m *LocalStreamManager) StreamUserList(mode int16, subject, subcontext, label string, includeHidden, includeNotHidden bool) ([]StreamPresenceView, error) {
	if m == nil || m.Tracker == nil {
		return nil, fmt.Errorf("stream tracker not configured")
	}
	key := streamKey(mode, subject, subcontext, label)
	list := m.Tracker.ListByStream(key)
	out := make([]StreamPresenceView, 0, len(list))
	for _, p := range list {
		hidden := p.Meta.Hidden
		if hidden && !includeHidden {
			continue
		}
		if !hidden && !includeNotHidden {
			continue
		}
		out = append(out, StreamPresenceView{
			UserID:    p.UserID,
			SessionID: p.SessionID,
			Username:  p.Meta.Username,
			Status:    p.Meta.Status,
			Hidden:    hidden,
		})
	}
	return out, nil
}

func (m *LocalStreamManager) StreamUserJoin(mode int16, subject, subcontext, label, userID, sessionID string, hidden, persistence bool, status string) (bool, error) {
	if m == nil || m.Tracker == nil {
		return false, fmt.Errorf("stream tracker not configured")
	}
	_ = persistence // reserved for future durable stream membership
	key := streamKey(mode, subject, subcontext, label)
	isNew := m.Tracker.Track(sessionID, key, userID, presence.PresenceMeta{
		Username: "",
		Status:   status,
		Hidden:   hidden,
	})
	return isNew, nil
}

func (m *LocalStreamManager) StreamUserLeave(mode int16, subject, subcontext, label, userID, sessionID string) error {
	if m == nil || m.Tracker == nil {
		return fmt.Errorf("stream tracker not configured")
	}
	_ = userID
	key := streamKey(mode, subject, subcontext, label)
	_, _ = m.Tracker.Untrack(sessionID, key)
	return nil
}

func (m *LocalStreamManager) StreamCount(mode int16, subject, subcontext, label string) (int, error) {
	if m == nil || m.Tracker == nil {
		return 0, fmt.Errorf("stream tracker not configured")
	}
	return m.Tracker.Count(streamKey(mode, subject, subcontext, label)), nil
}

func (m *LocalStreamManager) StreamSend(mode int16, subject, subcontext, label, data string, sessionIDs []string, reliable bool) error {
	if m == nil || m.Tracker == nil {
		return fmt.Errorf("stream tracker not configured")
	}
	_ = reliable
	key := streamKey(mode, subject, subcontext, label)
	targets := sessionIDs
	if len(targets) == 0 {
		targets = m.Tracker.Sessions(key)
	}
	if m.Router == nil || len(targets) == 0 {
		return nil
	}
	m.Router.SendToSessionIDs(targets, []byte(data))
	return nil
}

func (m *LocalStreamManager) SessionDisconnect(sessionID string) error {
	if m == nil || m.Registry == nil {
		return fmt.Errorf("session registry not configured")
	}
	return m.Registry.DisconnectSession(sessionID)
}
