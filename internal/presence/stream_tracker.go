package presence

import "sync"

// Stream modes for Local-first Tracker (ADR-0009 / ADR-0019).
const (
	StreamModeNotifications      = 0
	StreamModeStatus             = 1
	StreamModeChannel            = 2
	StreamModeGroup              = 3
	StreamModeDM                 = 4
	StreamModeMatchRelayed       = 5
	StreamModeMatchAuthoritative = 6
	StreamModeParty              = 7

	// StreamModeMatch is an alias for relayed match rooms (legacy callers).
	StreamModeMatch = StreamModeMatchRelayed
)

// StreamKey identifies a realtime stream.
type StreamKey struct {
	Mode       int16
	Subject    string // user ID (status), group ID, DM user A, match UUID
	Subcontext string // DM user B
	Label      string // room name, party ID, or authoritative node
}

// PresenceMeta is compact metadata stored with a tracked presence.
type PresenceMeta struct {
	Username string
	Status   string
	Hidden   bool
}

// Presence is a session's membership on a stream with meta.
type Presence struct {
	SessionID string
	UserID    string
	Meta      PresenceMeta
}

// PresenceID identifies a session for MessageRouter delivery.
type PresenceID struct {
	SessionID string
	UserID    string
}

type streamPresence struct {
	UserID string
	Meta   PresenceMeta
}

// LocalTracker tracks session membership on named streams with optional PresenceMeta (ADR-0019).
type LocalTracker struct {
	mu       sync.RWMutex
	byStream map[StreamKey]map[string]*streamPresence // stream -> sessionID -> presence
	bySess   map[string]map[StreamKey]struct{}        // sessionID -> streams
}

// StreamTracker is an alias for LocalTracker (backward-compatible name).
type StreamTracker = LocalTracker

// NewLocalTracker creates an empty Local tracker.
func NewLocalTracker() *LocalTracker {
	return &LocalTracker{
		byStream: make(map[StreamKey]map[string]*streamPresence),
		bySess:   make(map[string]map[StreamKey]struct{}),
	}
}

// NewStreamTracker creates an empty Local stream tracker (alias of NewLocalTracker).
func NewStreamTracker() *LocalTracker {
	return NewLocalTracker()
}

// Track adds a session to a stream with meta. Returns true if newly tracked.
func (t *LocalTracker) Track(sessionID string, key StreamKey, userID string, meta PresenceMeta) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	set, ok := t.byStream[key]
	if !ok {
		set = make(map[string]*streamPresence)
		t.byStream[key] = set
	}
	if _, exists := set[sessionID]; exists {
		set[sessionID] = &streamPresence{UserID: userID, Meta: meta}
		return false
	}
	set[sessionID] = &streamPresence{UserID: userID, Meta: meta}
	sess, ok := t.bySess[sessionID]
	if !ok {
		sess = make(map[StreamKey]struct{})
		t.bySess[sessionID] = sess
	}
	sess[key] = struct{}{}
	return true
}

// TrackMembership adds a session to a stream without user meta (party/channel/notifications).
func (t *LocalTracker) TrackMembership(sessionID string, key StreamKey) bool {
	return t.Track(sessionID, key, "", PresenceMeta{})
}

// Update replaces meta for an existing presence. Returns false if not tracked.
func (t *LocalTracker) Update(sessionID string, key StreamKey, userID string, meta PresenceMeta) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	set, ok := t.byStream[key]
	if !ok {
		return false
	}
	sp, ok := set[sessionID]
	if !ok {
		return false
	}
	sp.UserID = userID
	sp.Meta = meta
	return true
}

// Untrack removes a session from a stream. Returns the removed presence if any.
func (t *LocalTracker) Untrack(sessionID string, key StreamKey) (Presence, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var removed Presence
	found := false
	if set, ok := t.byStream[key]; ok {
		if sp, ok := set[sessionID]; ok {
			removed = Presence{SessionID: sessionID, UserID: sp.UserID, Meta: sp.Meta}
			found = true
			delete(set, sessionID)
		}
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
	return removed, found
}

// UntrackAll removes a session from every stream. Returns removed presence entries.
func (t *LocalTracker) UntrackAll(sessionID string) []Presence {
	t.mu.Lock()
	defer t.mu.Unlock()
	sess, ok := t.bySess[sessionID]
	if !ok {
		return nil
	}
	out := make([]Presence, 0, len(sess))
	for key := range sess {
		if set, ok := t.byStream[key]; ok {
			if sp, ok := set[sessionID]; ok {
				out = append(out, Presence{SessionID: sessionID, UserID: sp.UserID, Meta: sp.Meta})
				delete(set, sessionID)
			}
			if len(set) == 0 {
				delete(t.byStream, key)
			}
		}
	}
	delete(t.bySess, sessionID)
	return out
}

// UntrackAllKeys removes a session from every stream and returns the stream keys (legacy helper).
func (t *LocalTracker) UntrackAllKeys(sessionID string) []StreamKey {
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
func (t *LocalTracker) UntrackByMode(sessionID string, mode int16) {
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

// ListByStream returns presences on a stream.
func (t *LocalTracker) ListByStream(key StreamKey) []Presence {
	t.mu.RLock()
	defer t.mu.RUnlock()
	set := t.byStream[key]
	out := make([]Presence, 0, len(set))
	for sid, sp := range set {
		out = append(out, Presence{SessionID: sid, UserID: sp.UserID, Meta: sp.Meta})
	}
	return out
}

// GetPresence returns a single presence on a stream.
func (t *LocalTracker) GetPresence(sessionID string, key StreamKey) (Presence, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	set := t.byStream[key]
	if set == nil {
		return Presence{}, false
	}
	sp, ok := set[sessionID]
	if !ok {
		return Presence{}, false
	}
	return Presence{SessionID: sessionID, UserID: sp.UserID, Meta: sp.Meta}, true
}

// Sessions returns session IDs on a stream.
func (t *LocalTracker) Sessions(key StreamKey) []string {
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
func (t *LocalTracker) Count(key StreamKey) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.byStream[key])
}

// StatusStream returns the stream key for a user's status presence.
func StatusStream(userID string) StreamKey {
	return StreamKey{Mode: StreamModeStatus, Subject: userID}
}

// NotificationsStream returns the stream key for a user's notification delivery.
func NotificationsStream(userID string) StreamKey {
	return StreamKey{Mode: StreamModeNotifications, Subject: userID}
}

// PartyStream returns the stream key for a party ID.
func PartyStream(partyID string) StreamKey {
	return StreamKey{Mode: StreamModeParty, Label: partyID}
}

// ChannelStream returns the stream key for a room channel label.
func ChannelStream(label string) StreamKey {
	return StreamKey{Mode: StreamModeChannel, Label: label}
}

// GroupStream returns the stream key for a group channel.
func GroupStream(groupID string) StreamKey {
	return StreamKey{Mode: StreamModeGroup, Subject: groupID}
}

// DMStream returns the stream key for a DM channel (subject/subcontext already sorted).
func DMStream(subject, subcontext string) StreamKey {
	return StreamKey{Mode: StreamModeDM, Subject: subject, Subcontext: subcontext}
}

// MatchRelayedStream returns the stream key for a client-relayed match.
func MatchRelayedStream(matchUUID string) StreamKey {
	return StreamKey{Mode: StreamModeMatchRelayed, Subject: matchUUID}
}

// MatchAuthoritativeStream returns the stream key for an authoritative match on a node.
func MatchAuthoritativeStream(matchUUID, node string) StreamKey {
	return StreamKey{Mode: StreamModeMatchAuthoritative, Subject: matchUUID, Label: node}
}
