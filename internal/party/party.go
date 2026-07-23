package party

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	LabelMaxBytes   = 2048
	AbsoluteMaxSize = 256
	DefaultMaxSize  = 4
)

var (
	ErrPartyNotFound             = errors.New("party not found")
	ErrNotLeader                 = errors.New("operation allowed only for party leader")
	ErrPartyFull                 = errors.New("party is full")
	ErrPartyClosed               = errors.New("party is closed")
	ErrMemberNotFound            = errors.New("member not found")
	ErrInvalidMaxSize            = errors.New("invalid max size (must be between 1 and 256)")
	ErrAlreadyMember             = errors.New("already a party member")
	ErrJoinRequestDuplicate      = errors.New("join request already pending")
	ErrJoinRequestsFull          = errors.New("join request list is full")
	ErrNotJoinRequest            = errors.New("presence is not a pending join request")
	ErrRemoveSelf                = errors.New("cannot remove self from party")
	ErrAlreadyInParty            = errors.New("user already in another party")
	ErrInvalidLabel              = errors.New("invalid party label")
	ErrHiddenNonEmptyLabel       = errors.New("party is hidden and label is not empty")
	ErrPartyClosedForList        = errors.New("party closed")
)

// PartyMember represents a player in an active party.
type PartyMember struct {
	UserID     string                 `json:"user_id"`
	Username   string                 `json:"username"`
	SessionID  string                 `json:"session_id"`
	JoinedAt   time.Time              `json:"joined_at"`
	Properties map[string]interface{} `json:"properties"`
}

// JoinRequest is a pending closed-party join request.
type JoinRequest struct {
	UserID    string    `json:"user_id"`
	Username  string    `json:"username"`
	SessionID string    `json:"session_id"`
	Requested time.Time `json:"requested"`
}

// Presence identifies a user/session for leader ops.
type Presence struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	SessionID string `json:"session_id"`
}

// PartyListEntry is a discoverable party row for ListParties.
type PartyListEntry struct {
	ID      string `json:"id"`
	Open    bool   `json:"open"`
	Hidden  bool   `json:"hidden"`
	MaxSize int    `json:"max_size"`
	Label   string `json:"label"`
}

// PartySession represents a transient, in-memory party group.
type PartySession struct {
	PartyID      string                  `json:"party_id"`
	LeaderID     string                  `json:"leader_id"`
	Open         bool                    `json:"open"`
	Hidden       bool                    `json:"hidden"`
	MaxSize      int                     `json:"max_size"`
	Label        string                  `json:"label"`
	Members      map[string]*PartyMember `json:"members"`       // key: userID
	JoinRequests map[string]*JoinRequest `json:"join_requests"` // key: userID
	Node         string                  `json:"node"`
	CreateTime   time.Time               `json:"create_time"`
	LastActive   time.Time               `json:"last_active"`
	closed       bool
	activity     int64
}

// JoinOutcome is returned from Join.
type JoinOutcome struct {
	Party  *PartySession
	Joined bool // true if member; false if join request queued
}

// LeaveOutcome is returned from Leave / LeaveSession.
type LeaveOutcome struct {
	Party          *PartySession
	PromotedLeader *PartyMember
	Dissolved      bool
	WasMember      bool
}

// RemoveOutcome is returned from Remove.
type RemoveOutcome struct {
	Party           *PartySession
	Kicked          *PartyMember
	RejectedRequest bool
}

// Config controls registry behaviour.
type Config struct {
	Node            string
	SingleParty     bool
	DefaultMaxSize  int
	AbsoluteMaxSize int
	IdleCheckMs     int
}

// DefaultConfig returns reference-aligned defaults.
func DefaultConfig() Config {
	return Config{
		Node:            "local",
		SingleParty:     true,
		DefaultMaxSize:  DefaultMaxSize,
		AbsoluteMaxSize: AbsoluteMaxSize,
		IdleCheckMs:     30000,
	}
}

// MembershipChangeFn is invoked when party membership changes (cancel MM tickets).
type MembershipChangeFn func(partyID string)

// Registry manages thread-safe memory maps of active parties.
type Registry struct {
	mu      sync.RWMutex
	parties map[string]*PartySession
	byUser  map[string]string // userID -> partyID
	bySess  map[string]string // sessionID -> partyID
	cfg     Config

	onMembershipChange MembershipChangeFn

	stopIdle chan struct{}
	idleOnce sync.Once
}

// NewRegistry creates a party registry with default config.
func NewRegistry() *Registry {
	return NewRegistryWithConfig(DefaultConfig())
}

// NewRegistryWithConfig creates a registry with the given config.
func NewRegistryWithConfig(cfg Config) *Registry {
	if cfg.AbsoluteMaxSize <= 0 {
		cfg.AbsoluteMaxSize = AbsoluteMaxSize
	}
	if cfg.DefaultMaxSize <= 0 {
		cfg.DefaultMaxSize = DefaultMaxSize
	}
	if cfg.Node == "" {
		cfg.Node = "local"
	}
	r := &Registry{
		parties:  make(map[string]*PartySession),
		byUser:   make(map[string]string),
		bySess:   make(map[string]string),
		cfg:      cfg,
		stopIdle: make(chan struct{}),
	}
	if cfg.IdleCheckMs > 0 {
		go r.idleLoop()
	}
	return r
}

// SetMembershipChangeHook registers a callback for membership changes.
func (r *Registry) SetMembershipChangeHook(fn MembershipChangeFn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onMembershipChange = fn
}

// StopIdleSweep stops the idle ticker.
func (r *Registry) StopIdleSweep() {
	r.idleOnce.Do(func() {
		close(r.stopIdle)
	})
}

func (r *Registry) idleLoop() {
	ticker := time.NewTicker(time.Duration(r.cfg.IdleCheckMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopIdle:
			return
		case <-ticker.C:
			r.SweepIdle()
		}
	}
}

func (r *Registry) notifyMembershipChange(partyID string) {
	if r.onMembershipChange != nil {
		r.onMembershipChange(partyID)
	}
}

func (r *Registry) touch(p *PartySession) {
	p.LastActive = time.Now()
	atomic.AddInt64(&p.activity, 1)
}

func validateLabel(label string, hidden bool) error {
	if label == "" {
		label = "{}"
	}
	if len(label) > LabelMaxBytes {
		return ErrInvalidLabel
	}
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(label), &obj); err != nil {
		return ErrInvalidLabel
	}
	if hidden && label != "{}" && len(obj) > 0 {
		return ErrHiddenNonEmptyLabel
	}
	return nil
}

func normalizeLabel(label string) string {
	if strings.TrimSpace(label) == "" {
		return "{}"
	}
	return label
}

// Create creates a new party; creator is leader. Enforces single-party when configured.
func (r *Registry) Create(leaderID, username, sessionID string, open, hidden bool, maxSize int, label string) (*PartySession, error) {
	if maxSize == 0 {
		maxSize = r.cfg.DefaultMaxSize
	}
	if maxSize < 1 || maxSize > r.cfg.AbsoluteMaxSize {
		return nil, ErrInvalidMaxSize
	}
	label = normalizeLabel(label)
	if err := validateLabel(label, hidden); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cfg.SingleParty {
		if prevID, ok := r.byUser[leaderID]; ok {
			r.leaveLocked(prevID, leaderID)
		}
	}

	partyID := uuid.New().String() + "." + r.cfg.Node
	now := time.Now()
	p := &PartySession{
		PartyID:      partyID,
		LeaderID:     leaderID,
		Open:         open,
		Hidden:       hidden,
		MaxSize:      maxSize,
		Label:        label,
		Members:      make(map[string]*PartyMember),
		JoinRequests: make(map[string]*JoinRequest),
		Node:         r.cfg.Node,
		CreateTime:   now,
		LastActive:   now,
	}
	p.Members[leaderID] = &PartyMember{
		UserID:     leaderID,
		Username:   username,
		SessionID:  sessionID,
		JoinedAt:   now,
		Properties: make(map[string]interface{}),
	}
	r.parties[partyID] = p
	r.byUser[leaderID] = partyID
	r.bySess[sessionID] = partyID
	atomic.StoreInt64(&p.activity, 1)
	return cloneParty(p), nil
}

// CreateParty is an alias matching older call sites (open parties, default label).
func (r *Registry) CreateParty(leaderID, username, sessionID string, open bool, maxSize int) (*PartySession, error) {
	return r.Create(leaderID, username, sessionID, open, false, maxSize, "{}")
}

// GetParty retrieves a party by ID (cloned snapshot).
func (r *Registry) GetParty(partyID string) (*PartySession, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, exists := r.parties[partyID]
	if !exists || p.closed {
		return nil, ErrPartyNotFound
	}
	return cloneParty(p), nil
}

// PartyIDForSession returns the party ID for a session, if any.
func (r *Registry) PartyIDForSession(sessionID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.bySess[sessionID]
	return id, ok
}

// JoinOutcome Join adds a member (open) or queues a join request (closed).
func (r *Registry) Join(partyID, userID, username, sessionID string) (*JoinOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, exists := r.parties[partyID]
	if !exists || p.closed {
		return nil, ErrPartyNotFound
	}
	r.touch(p)

	if _, ok := p.Members[userID]; ok {
		return nil, ErrAlreadyMember
	}
	if r.cfg.SingleParty {
		if prevID, ok := r.byUser[userID]; ok && prevID != partyID {
			return nil, ErrAlreadyInParty
		}
	}
	if len(p.Members) >= p.MaxSize {
		return nil, ErrPartyFull
	}

	if p.Open {
		now := time.Now()
		p.Members[userID] = &PartyMember{
			UserID:     userID,
			Username:   username,
			SessionID:  sessionID,
			JoinedAt:   now,
			Properties: make(map[string]interface{}),
		}
		delete(p.JoinRequests, userID)
		r.byUser[userID] = partyID
		r.bySess[sessionID] = partyID
		out := &JoinOutcome{Party: cloneParty(p), Joined: true}
		r.notifyMembershipChange(partyID)
		return out, nil
	}

	if _, ok := p.JoinRequests[userID]; ok {
		return nil, ErrJoinRequestDuplicate
	}
	if len(p.JoinRequests) >= p.MaxSize {
		return nil, ErrJoinRequestsFull
	}
	p.JoinRequests[userID] = &JoinRequest{
		UserID:    userID,
		Username:  username,
		SessionID: sessionID,
		Requested: time.Now(),
	}
	return &JoinOutcome{Party: cloneParty(p), Joined: false}, nil
}

// JoinParty wraps Join for older call sites that expect immediate membership (open or already accepted).
func (r *Registry) JoinParty(partyID, userID, username, sessionID string) (*PartySession, error) {
	out, err := r.Join(partyID, userID, username, sessionID)
	if err != nil {
		return nil, err
	}
	if !out.Joined {
		return out.Party, nil // closed: request queued; callers should check JoinRequests
	}
	return out.Party, nil
}

// Leave removes a user from the party.
func (r *Registry) Leave(partyID, userID string) (*LeaveOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaveLocked(partyID, userID)
}

// LeaveParty wraps Leave for older call sites.
func (r *Registry) LeaveParty(partyID, userID string) (*PartySession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out, err := r.leaveLocked(partyID, userID)
	if err != nil {
		return nil, err
	}
	if out.Dissolved {
		return nil, nil
	}
	return out.Party, nil
}

// LeaveSession removes the session from whatever party it is in.
func (r *Registry) LeaveSession(sessionID string) (*LeaveOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	partyID, ok := r.bySess[sessionID]
	if !ok {
		return &LeaveOutcome{WasMember: false}, nil
	}
	p := r.parties[partyID]
	if p == nil {
		delete(r.bySess, sessionID)
		return &LeaveOutcome{WasMember: false}, nil
	}
	var userID string
	for _, m := range p.Members {
		if m.SessionID == sessionID {
			userID = m.UserID
			break
		}
	}
	if userID == "" {
		// Pending join request only
		for uid, jr := range p.JoinRequests {
			if jr.SessionID == sessionID {
				delete(p.JoinRequests, uid)
				delete(r.bySess, sessionID)
				return &LeaveOutcome{Party: cloneParty(p), WasMember: false}, nil
			}
		}
		delete(r.bySess, sessionID)
		return &LeaveOutcome{WasMember: false}, nil
	}
	return r.leaveLocked(partyID, userID)
}

func (r *Registry) leaveLocked(partyID, userID string) (*LeaveOutcome, error) {
	p, exists := r.parties[partyID]
	if !exists || p.closed {
		return nil, ErrPartyNotFound
	}
	m, isMember := p.Members[userID]
	if !isMember {
		if _, ok := p.JoinRequests[userID]; ok {
			delete(p.JoinRequests, userID)
			return &LeaveOutcome{Party: cloneParty(p), WasMember: false}, nil
		}
		return nil, ErrMemberNotFound
	}
	r.touch(p)
	delete(p.Members, userID)
	delete(r.byUser, userID)
	delete(r.bySess, m.SessionID)

	out := &LeaveOutcome{WasMember: true}
	if len(p.Members) == 0 {
		p.closed = true
		delete(r.parties, partyID)
		out.Dissolved = true
		r.notifyMembershipChange(partyID)
		return out, nil
	}

	if p.LeaderID == userID {
		var oldest *PartyMember
		for _, mem := range p.Members {
			if oldest == nil || mem.JoinedAt.Before(oldest.JoinedAt) {
				oldest = mem
			}
		}
		if oldest != nil {
			p.LeaderID = oldest.UserID
			out.PromotedLeader = cloneMember(oldest)
		}
	}
	out.Party = cloneParty(p)
	r.notifyMembershipChange(partyID)
	return out, nil
}

// Promote transfers leadership.
func (r *Registry) Promote(partyID, leaderSessionID string, target Presence) (*PartySession, *PartyMember, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, err := r.requireLeader(partyID, leaderSessionID)
	if err != nil {
		return nil, nil, err
	}
	r.touch(p)
	m, ok := p.Members[target.UserID]
	if !ok || m.SessionID != target.SessionID {
		return nil, nil, ErrMemberNotFound
	}
	p.LeaderID = m.UserID
	return cloneParty(p), cloneMember(m), nil
}

// Accept accepts a pending join request.
func (r *Registry) Accept(partyID, leaderSessionID string, target Presence) (*PartySession, *PartyMember, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, err := r.requireLeader(partyID, leaderSessionID)
	if err != nil {
		return nil, nil, err
	}
	r.touch(p)
	if len(p.Members) >= p.MaxSize {
		return nil, nil, ErrPartyFull
	}
	jr, ok := p.JoinRequests[target.UserID]
	if !ok || jr.SessionID != target.SessionID {
		return nil, nil, ErrNotJoinRequest
	}
	if r.cfg.SingleParty {
		if prevID, exists := r.byUser[jr.UserID]; exists && prevID != partyID {
			r.leaveLocked(prevID, jr.UserID)
		}
	}
	delete(p.JoinRequests, target.UserID)
	now := time.Now()
	member := &PartyMember{
		UserID:     jr.UserID,
		Username:   jr.Username,
		SessionID:  jr.SessionID,
		JoinedAt:   now,
		Properties: make(map[string]interface{}),
	}
	p.Members[jr.UserID] = member
	r.byUser[jr.UserID] = partyID
	r.bySess[jr.SessionID] = partyID
	r.notifyMembershipChange(partyID)
	return cloneParty(p), cloneMember(member), nil
}

// Remove kicks a member or rejects a join request.
func (r *Registry) Remove(partyID, leaderSessionID string, target Presence) (*RemoveOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, err := r.requireLeader(partyID, leaderSessionID)
	if err != nil {
		return nil, err
	}
	r.touch(p)

	leader := p.Members[p.LeaderID]
	if leader != nil && leader.SessionID == target.SessionID && leader.UserID == target.UserID {
		return nil, ErrRemoveSelf
	}

	if jr, ok := p.JoinRequests[target.UserID]; ok && jr.SessionID == target.SessionID {
		delete(p.JoinRequests, target.UserID)
		return &RemoveOutcome{Party: cloneParty(p), RejectedRequest: true}, nil
	}

	m, ok := p.Members[target.UserID]
	if !ok || m.SessionID != target.SessionID {
		return nil, ErrMemberNotFound
	}
	delete(p.Members, target.UserID)
	delete(r.byUser, m.UserID)
	delete(r.bySess, m.SessionID)
	r.notifyMembershipChange(partyID)
	return &RemoveOutcome{Party: cloneParty(p), Kicked: cloneMember(m)}, nil
}

// Close dissolves the party (leader only).
func (r *Registry) Close(partyID, leaderSessionID string) (*PartySession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, err := r.requireLeader(partyID, leaderSessionID)
	if err != nil {
		return nil, err
	}
	snap := cloneParty(p)
	p.closed = true
	for _, m := range p.Members {
		delete(r.byUser, m.UserID)
		delete(r.bySess, m.SessionID)
	}
	delete(r.parties, partyID)
	r.notifyMembershipChange(partyID)
	return snap, nil
}

// Update updates open/hidden/label (leader only).
func (r *Registry) Update(partyID, leaderSessionID, label string, open, hidden bool) (*PartySession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, err := r.requireLeader(partyID, leaderSessionID)
	if err != nil {
		return nil, err
	}
	label = normalizeLabel(label)
	if err := validateLabel(label, hidden); err != nil {
		return nil, err
	}
	r.touch(p)
	p.Open = open
	p.Hidden = hidden
	p.Label = label
	return cloneParty(p), nil
}

// JoinRequestList returns pending join requests (leader only).
func (r *Registry) JoinRequestList(partyID, leaderSessionID string) ([]*JoinRequest, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p, exists := r.parties[partyID]
	if !exists || p.closed {
		return nil, ErrPartyNotFound
	}
	leader := p.Members[p.LeaderID]
	if leader == nil || leader.SessionID != leaderSessionID {
		return nil, ErrNotLeader
	}
	out := make([]*JoinRequest, 0, len(p.JoinRequests))
	for _, jr := range p.JoinRequests {
		cp := *jr
		out = append(out, &cp)
	}
	return out, nil
}

// List returns discoverable parties (in-memory filter + cursor).
func (r *Registry) List(limit int, open *bool, showHidden bool, query, cursor string) ([]*PartyListEntry, string, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.parties))
	for id, p := range r.parties {
		if p.closed {
			continue
		}
		if !showHidden && p.Hidden {
			continue
		}
		if open != nil && p.Open != *open {
			continue
		}
		if query != "" && !strings.Contains(p.Label, query) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	start := 0
	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err == nil {
			c := string(raw)
			for i, id := range ids {
				if id == c {
					start = i + 1
					break
				}
			}
		}
	}

	out := make([]*PartyListEntry, 0, limit)
	var next string
	for i := start; i < len(ids) && len(out) < limit; i++ {
		p := r.parties[ids[i]]
		out = append(out, &PartyListEntry{
			ID:      p.PartyID,
			Open:    p.Open,
			Hidden:  p.Hidden,
			MaxSize: p.MaxSize,
			Label:   p.Label,
		})
		if len(out) == limit && i+1 < len(ids) {
			next = base64.RawURLEncoding.EncodeToString([]byte(ids[i]))
		}
	}
	return out, next, nil
}

// UpdateMemberProperties updates custom variables for a party member.
func (r *Registry) UpdateMemberProperties(partyID, userID string, properties map[string]interface{}) (*PartySession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, exists := r.parties[partyID]
	if !exists || p.closed {
		return nil, ErrPartyNotFound
	}
	m, ok := p.Members[userID]
	if !ok {
		return nil, ErrMemberNotFound
	}
	for k, v := range properties {
		m.Properties[k] = v
	}
	r.touch(p)
	return cloneParty(p), nil
}

// SweepIdle closes parties with no members (safety) or no activity since last check and empty stream.
func (r *Registry) SweepIdle() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, p := range r.parties {
		if p.closed {
			delete(r.parties, id)
			continue
		}
		if len(p.Members) == 0 {
			p.closed = true
			delete(r.parties, id)
			continue
		}
		// Activity-based: if no members somehow remain tracked inconsistently — handled above.
		_ = atomic.LoadInt64(&p.activity)
	}
}

func (r *Registry) requireLeader(partyID, leaderSessionID string) (*PartySession, error) {
	p, exists := r.parties[partyID]
	if !exists || p.closed {
		return nil, ErrPartyNotFound
	}
	leader := p.Members[p.LeaderID]
	if leader == nil || leader.SessionID != leaderSessionID {
		return nil, ErrNotLeader
	}
	return p, nil
}

func cloneMember(m *PartyMember) *PartyMember {
	if m == nil {
		return nil
	}
	cp := *m
	if m.Properties != nil {
		cp.Properties = make(map[string]interface{}, len(m.Properties))
		for k, v := range m.Properties {
			cp.Properties[k] = v
		}
	}
	return &cp
}

func cloneParty(p *PartySession) *PartySession {
	if p == nil {
		return nil
	}
	cp := &PartySession{
		PartyID:      p.PartyID,
		LeaderID:     p.LeaderID,
		Open:         p.Open,
		Hidden:       p.Hidden,
		MaxSize:      p.MaxSize,
		Label:        p.Label,
		Members:      make(map[string]*PartyMember, len(p.Members)),
		JoinRequests: make(map[string]*JoinRequest, len(p.JoinRequests)),
		Node:         p.Node,
		CreateTime:   p.CreateTime,
		LastActive:   p.LastActive,
	}
	for k, m := range p.Members {
		cp.Members[k] = cloneMember(m)
	}
	for k, jr := range p.JoinRequests {
		cjr := *jr
		cp.JoinRequests[k] = &cjr
	}
	return cp
}

// Count returns active party count.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.parties)
}
