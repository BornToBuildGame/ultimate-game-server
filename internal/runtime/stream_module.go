package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/BornToBuildGame/ultimate-game-server/internal/cluster"
	"github.com/BornToBuildGame/ultimate-game-server/internal/presence"
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
	StreamUserUpdate(mode int16, subject, subcontext, label, userID, sessionID string, hidden, persistence bool, status string) error
	StreamUserKick(mode int16, subject, subcontext, label string, presence StreamPresenceView) error
	StreamClose(mode int16, subject, subcontext, label string) error
	StreamCount(mode int16, subject, subcontext, label string) (int, error)
	StreamSend(mode int16, subject, subcontext, label, data string, sessionIDs []string, reliable bool) error
	StreamSendRaw(mode int16, subject, subcontext, label string, data []byte, sessionIDs []string, reliable bool) error
	SessionDisconnect(sessionID string) error
	UntrackSession(sessionID string)
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
	if isNew && !hidden {
		m.emitStreamPresenceEvent(mode, subject, subcontext, label,
			[]StreamPresenceView{{UserID: userID, SessionID: sessionID, Status: status}},
			nil,
		)
	}
	return isNew, nil
}

func (m *LocalStreamManager) StreamUserLeave(mode int16, subject, subcontext, label, userID, sessionID string) error {
	if m == nil || m.Tracker == nil {
		return fmt.Errorf("stream tracker not configured")
	}
	key := streamKey(mode, subject, subcontext, label)
	removed, found := m.Tracker.Untrack(sessionID, key)
	if !found {
		return nil
	}
	uid := userID
	if uid == "" {
		uid = removed.UserID
	}
	if !removed.Meta.Hidden {
		m.emitStreamPresenceEvent(mode, subject, subcontext, label, nil, []StreamPresenceView{{
			UserID:    uid,
			SessionID: sessionID,
			Username:  removed.Meta.Username,
			Status:    removed.Meta.Status,
		}})
	}
	return nil
}

func (m *LocalStreamManager) StreamUserUpdate(mode int16, subject, subcontext, label, userID, sessionID string, hidden, persistence bool, status string) error {
	if m == nil || m.Tracker == nil {
		return fmt.Errorf("stream tracker not configured")
	}
	_ = persistence
	key := streamKey(mode, subject, subcontext, label)
	ok := m.Tracker.Update(sessionID, key, userID, presence.PresenceMeta{
		Status: status,
		Hidden: hidden,
	})
	if !ok {
		return fmt.Errorf("presence not found on stream")
	}
	return nil
}

func (m *LocalStreamManager) StreamUserKick(mode int16, subject, subcontext, label string, presenceView StreamPresenceView) error {
	if err := m.StreamUserLeave(mode, subject, subcontext, label, presenceView.UserID, presenceView.SessionID); err != nil {
		return err
	}
	if presenceView.SessionID != "" && m.Registry != nil {
		_ = m.Registry.DisconnectSession(presenceView.SessionID)
	}
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

func (m *LocalStreamManager) StreamSendRaw(mode int16, subject, subcontext, label string, data []byte, sessionIDs []string, reliable bool) error {
	// Lua/JS receive string payloads; encode raw bytes as base64 for the stream_data envelope.
	return m.StreamSend(mode, subject, subcontext, label, base64.StdEncoding.EncodeToString(data), sessionIDs, reliable)
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

// UntrackSession removes a session from all streams and emits stream_presence_event for custom modes.
func (m *LocalStreamManager) UntrackSession(sessionID string) {
	if m == nil || m.Tracker == nil {
		return
	}
	removals := m.Tracker.UntrackAllDetailed(sessionID)
	for _, r := range removals {
		if presence.IsDomainStreamMode(r.Key.Mode) || r.Presence.Meta.Hidden {
			continue
		}
		m.emitStreamPresenceEvent(r.Key.Mode, r.Key.Subject, r.Key.Subcontext, r.Key.Label, nil, []StreamPresenceView{{
			UserID:    r.Presence.UserID,
			SessionID: r.Presence.SessionID,
			Username:  r.Presence.Meta.Username,
			Status:    r.Presence.Meta.Status,
		}})
	}
}

// EmitStreamPresenceRemovals fans out leave events for custom streams after UntrackAllDetailed.
// Used by the socket gateway when it owns the tracker untrack path.
func EmitStreamPresenceRemovals(router presence.MessageRouter, tracker *presence.LocalTracker, removals []presence.StreamPresenceRemoval) {
	if router == nil || tracker == nil {
		return
	}
	sm := &LocalStreamManager{Tracker: tracker, Router: router}
	for _, r := range removals {
		if presence.IsDomainStreamMode(r.Key.Mode) || r.Presence.Meta.Hidden {
			continue
		}
		sm.emitStreamPresenceEvent(r.Key.Mode, r.Key.Subject, r.Key.Subcontext, r.Key.Label, nil, []StreamPresenceView{{
			UserID:    r.Presence.UserID,
			SessionID: r.Presence.SessionID,
			Username:  r.Presence.Meta.Username,
			Status:    r.Presence.Meta.Status,
		}})
	}
}

func (m *LocalStreamManager) emitStreamPresenceEvent(mode int16, subject, subcontext, label string, joins, leaves []StreamPresenceView) {
	if m == nil || m.Router == nil || m.Tracker == nil {
		return
	}
	if presence.IsDomainStreamMode(mode) {
		return
	}
	if len(joins) == 0 && len(leaves) == 0 {
		return
	}
	key := streamKey(mode, subject, subcontext, label)
	targets := m.Tracker.Sessions(key)
	// Also notify leavers so they see their own leave if still connected (join path includes new member).
	seen := make(map[string]struct{}, len(targets))
	for _, sid := range targets {
		seen[sid] = struct{}{}
	}
	for _, j := range joins {
		if _, ok := seen[j.SessionID]; !ok && j.SessionID != "" {
			targets = append(targets, j.SessionID)
			seen[j.SessionID] = struct{}{}
		}
	}
	for _, l := range leaves {
		if _, ok := seen[l.SessionID]; !ok && l.SessionID != "" {
			targets = append(targets, l.SessionID)
			seen[l.SessionID] = struct{}{}
		}
	}
	if len(targets) == 0 {
		return
	}
	joinMaps := make([]map[string]interface{}, 0, len(joins))
	for _, j := range joins {
		joinMaps = append(joinMaps, map[string]interface{}{
			"user_id":    j.UserID,
			"session_id": j.SessionID,
			"username":   j.Username,
		})
	}
	leaveMaps := make([]map[string]interface{}, 0, len(leaves))
	for _, l := range leaves {
		leaveMaps = append(leaveMaps, map[string]interface{}{
			"user_id":    l.UserID,
			"session_id": l.SessionID,
			"username":   l.Username,
		})
	}
	env, err := json.Marshal(map[string]interface{}{
		"stream_presence_event": map[string]interface{}{
			"stream": map[string]interface{}{
				"mode":       mode,
				"subject":    subject,
				"subcontext": subcontext,
				"label":      label,
			},
			"joins":  joinMaps,
			"leaves": leaveMaps,
		},
	})
	if err != nil {
		return
	}
	m.Router.SendToSessionIDs(targets, env)
}
