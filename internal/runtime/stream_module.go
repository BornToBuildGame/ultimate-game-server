package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"ultimate-game-server/internal/cluster"
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
	StreamUserGet(mode int16, subject, subcontext, label, userID, sessionID string) (*StreamPresenceView, error)
	StreamUserJoin(mode int16, subject, subcontext, label, userID, sessionID string, hidden, persistence bool, status string) (bool, error)
	StreamUserLeave(mode int16, subject, subcontext, label, userID, sessionID string) error
	StreamClose(mode int16, subject, subcontext, label string) error
	StreamCount(mode int16, subject, subcontext, label string) (int, error)
	StreamSend(mode int16, subject, subcontext, label, data string, sessionIDs []string, reliable bool) error
	SessionDisconnect(sessionID string) error
}

// LocalStreamManager implements StreamManager with LocalTracker + MessageRouter.
type LocalStreamManager struct {
	Tracker  *presence.LocalTracker
	Router   presence.MessageRouter
	Registry SessionDisconnecter
	mesh     *cluster.Mesh
	nodeID   string
}

// SetClusterMesh enables cross-node StreamSend fan-out.
func (m *LocalStreamManager) SetClusterMesh(mesh *cluster.Mesh, nodeID string) {
	if m == nil {
		return
	}
	m.mesh = mesh
	m.nodeID = nodeID
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

func (m *LocalStreamManager) StreamUserGet(mode int16, subject, subcontext, label, userID, sessionID string) (*StreamPresenceView, error) {
	list, err := m.StreamUserList(mode, subject, subcontext, label, true, true)
	if err != nil {
		return nil, err
	}
	for i := range list {
		p := &list[i]
		if userID != "" && p.UserID != userID {
			continue
		}
		if sessionID != "" && p.SessionID != sessionID {
			continue
		}
		cp := *p
		return &cp, nil
	}
	return nil, nil
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

func (m *LocalStreamManager) StreamClose(mode int16, subject, subcontext, label string) error {
	list, err := m.StreamUserList(mode, subject, subcontext, label, true, true)
	if err != nil {
		return err
	}
	for _, p := range list {
		_ = m.StreamUserLeave(mode, subject, subcontext, label, p.UserID, p.SessionID)
	}
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
	m.streamSendLocal(mode, subject, subcontext, label, data, sessionIDs)
	if m.mesh != nil {
		_ = m.mesh.PublishStreamBroadcast(context.Background(), cluster.StreamSendMessage{
			SourceNode: m.nodeID,
			Mode: mode, Subject: subject, Subcontext: subcontext, Label: label,
			Data: data, SessionIDs: sessionIDs,
		})
	}
	return nil
}

// StreamSendLocal delivers to local sessions only (used by peer mesh handlers to avoid rebroadcast).
func (m *LocalStreamManager) StreamSendLocal(mode int16, subject, subcontext, label, data string, sessionIDs []string) error {
	if m == nil || m.Tracker == nil {
		return fmt.Errorf("stream tracker not configured")
	}
	m.streamSendLocal(mode, subject, subcontext, label, data, sessionIDs)
	return nil
}

func (m *LocalStreamManager) streamSendLocal(mode int16, subject, subcontext, label, data string, sessionIDs []string) {
	if m.Router == nil {
		return
	}
	key := streamKey(mode, subject, subcontext, label)
	targets := sessionIDs
	if len(targets) == 0 {
		targets = m.Tracker.Sessions(key)
	}
	if len(targets) == 0 {
		return
	}
	// Reference-shaped stream_data envelope so clients can decode custom streams.
	env, err := json.Marshal(map[string]interface{}{
		"stream_data": map[string]interface{}{
			"stream": map[string]interface{}{
				"mode":       mode,
				"subject":    subject,
				"subcontext": subcontext,
				"label":      label,
			},
			"data":     data,
			"reliable": true,
		},
	})
	if err != nil {
		return
	}
	m.Router.SendToSessionIDs(targets, env)
}

func (m *LocalStreamManager) SessionDisconnect(sessionID string) error {
	if m == nil || m.Registry == nil {
		return fmt.Errorf("session registry not configured")
	}
	return m.Registry.DisconnectSession(sessionID)
}
