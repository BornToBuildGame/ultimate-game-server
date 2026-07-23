package presence

import "sync"

// Stream modes for Local-first Tracker (ADR-0009).
const (
	StreamModeNotifications = 0
	StreamModeStatus        = 1
	StreamModeChannel       = 2
	StreamModeGroup         = 3
	StreamModeDM            = 4
	StreamModeMatch         = 5
	StreamModeParty         = 6
)

// StreamKey identifies a realtime stream.
type StreamKey struct {
	Mode  int16
	Label string // e.g. party ID
}

// StreamTracker tracks session membership on named streams (Local-first).
type StreamTracker struct {
	mu      sync.RWMutex
	byStream map[StreamKey]map[string]struct{} // stream -> sessionIDs
	bySess   map[string]map[StreamKey]struct{} // sessionID -> streams
}

// NewStreamTracker creates an empty Local stream tracker.
func NewStreamTracker() *StreamTracker {
	return &StreamTracker{
		byStream: make(map[StreamKey]map[string]struct{}),
		bySess:   make(map[string]map[StreamKey]struct{}),
	}
}

// Track adds a session to a stream. Returns true if newly tracked.
func (t *StreamTracker) Track(sessionID string, key StreamKey) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	set, ok := t.byStream[key]
	if !ok {
		set = make(map[string]struct{})
		t.byStream[key] = set
	}
	if _, exists := set[sessionID]; exists {
		return false
	}
	set[sessionID] = struct{}{}
	sess, ok := t.bySess[sessionID]
	if !ok {
		sess = make(map[StreamKey]struct{})
		t.bySess[sessionID] = sess
	}
	sess[key] = struct{}{}
	return true
}

// Untrack removes a session from a stream.
func (t *StreamTracker) Untrack(sessionID string, key StreamKey) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if set, ok := t.byStream[key]; ok {
		delete(set, sessionID)
		if len(set) == 0 {
			delete(t.byStream, key)
		}
	}
	if sess, ok := t.bySess[sessionID]; ok {
		delete(sess, key)
		if len(sess) == 0 {
			delete(t.bySess, sessionID)
		}
	}
}

// UntrackAll removes a session from every stream.
func (t *StreamTracker) UntrackAll(sessionID string) []StreamKey {
	t.mu.Lock()
	defer t.mu.Unlock()
	sess, ok := t.bySess[sessionID]
	if !ok {
		return nil
	}
	keys := make([]StreamKey, 0, len(sess))
	for key := range sess {
		keys = append(keys, key)
		if set, ok := t.byStream[key]; ok {
			delete(set, sessionID)
			if len(set) == 0 {
				delete(t.byStream, key)
			}
		}
	}
	delete(t.bySess, sessionID)
	return keys
}

// UntrackByMode removes the session from all streams of the given mode.
func (t *StreamTracker) UntrackByMode(sessionID string, mode int16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	sess, ok := t.bySess[sessionID]
	if !ok {
		return
	}
	for key := range sess {
		if key.Mode != mode {
			continue
		}
		delete(sess, key)
		if set, ok := t.byStream[key]; ok {
			delete(set, sessionID)
			if len(set) == 0 {
				delete(t.byStream, key)
			}
		}
	}
	if len(sess) == 0 {
		delete(t.bySess, sessionID)
	}
}

// Sessions returns session IDs on a stream.
func (t *StreamTracker) Sessions(key StreamKey) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	set := t.byStream[key]
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

// Count returns member count on a stream.
func (t *StreamTracker) Count(key StreamKey) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.byStream[key])
}

// PartyStream returns the stream key for a party ID.
func PartyStream(partyID string) StreamKey {
	return StreamKey{Mode: StreamModeParty, Label: partyID}
}
