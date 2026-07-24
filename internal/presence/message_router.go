package presence

// MessageRouter delivers realtime payloads to sessions.
type MessageRouter interface {
	SendToSessionIDs(sessionIDs []string, payload []byte)
}

// SessionSender sends a payload to a single session if connected.
type SessionSender interface {
	SendToSession(sessionID string, payload []byte)
}

// LocalMessageRouter fans out to in-process sessions via SessionSender.
type LocalMessageRouter struct {
	sender SessionSender
}

// NewLocalMessageRouter wraps a SessionSender (typically ConnectionRegistry).
func NewLocalMessageRouter(sender SessionSender) *LocalMessageRouter {
	return &LocalMessageRouter{sender: sender}
}

// SendToSessionIDs delivers payload to each session ID (best-effort).
func (r *LocalMessageRouter) SendToSessionIDs(sessionIDs []string, payload []byte) {
	if r == nil || r.sender == nil || len(payload) == 0 {
		return
	}
	for _, sid := range sessionIDs {
		r.sender.SendToSession(sid, payload)
	}
}
